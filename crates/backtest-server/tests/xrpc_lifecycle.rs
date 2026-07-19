use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use xrpc::{
    JsonCodec, MessageChannelAdapter, RpcClient, RpcServer, SharedMemoryConfig,
    SharedMemoryFrameTransport,
};

const CHILD_TEST_NAME: &str = "shm_close_child";
const PROCESS_EXIT_BOUND: Duration = Duration::from_secs(8);

#[test]
#[ignore = "subprocess helper for the parent lifecycle regression"]
fn shm_close_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Tokio runtime should start");

    runtime.block_on(async {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after Unix epoch")
            .as_nanos();
        let name = format!("qs-xrpc03-close-{}-{unique}", std::process::id());
        let config = SharedMemoryConfig::default()
            .with_read_timeout(Duration::from_secs(300))
            .with_write_timeout(Duration::from_secs(30));

        let server = SharedMemoryFrameTransport::create_server(&name, config.clone())
            .expect("server SHM mapping should be created");
        let transport = SharedMemoryFrameTransport::connect_client_with_config(&name, config)
            .expect("client should attach to the SHM mapping");
        let channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(transport);
        let client = RpcClient::with_codec(channel, JsonCodec);
        let handle = client.try_start().expect("fresh client should start");

        assert!(
            client.try_start().is_err(),
            "xrpc 0.3 must reject duplicate client startup"
        );

        // Allow the receive loop to enter the 300-second idle SHM read.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let close_started = Instant::now();
        client
            .close()
            .await
            .expect("graceful client close should succeed");
        handle
            .join()
            .await
            .expect("receive task should terminate cleanly");
        assert!(
            close_started.elapsed() < Duration::from_secs(3),
            "close and join took {:?} with a 300-second read timeout",
            close_started.elapsed()
        );

        drop(client);
        drop(server);
    });

    // The 0.2 regression survived until Tokio runtime drop. Reaching the end of
    // this child process promptly is therefore part of the parent assertion.
    drop(runtime);
}

#[test]
fn shm_close_with_long_read_timeout_exits_process_promptly() {
    let current_test_binary = std::env::current_exe().expect("test executable should be known");
    let mut child = Command::new(current_test_binary)
        .args(["--ignored", "--exact", CHILD_TEST_NAME, "--nocapture"])
        .env("RUST_TEST_THREADS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("lifecycle test subprocess should start");

    let deadline = Instant::now() + PROCESS_EXIT_BOUND;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let output = child
                    .wait_with_output()
                    .expect("finished lifecycle subprocess output should be readable");
                assert!(
                    status.success(),
                    "lifecycle subprocess failed\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(20));
            }
            Ok(None) => {
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("timed-out lifecycle subprocess output should be readable");
                panic!(
                    "lifecycle subprocess did not exit within {:?}\nstdout:\n{}\nstderr:\n{}",
                    PROCESS_EXIT_BOUND,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("failed to inspect lifecycle subprocess: {error}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_server_stream_survives_connected_shm_read_timeouts() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after Unix epoch")
        .as_nanos();
    let name = format!("qs-xrpc031-stream-{}-{unique}", std::process::id());
    let config = SharedMemoryConfig::default()
        .with_read_timeout(Duration::from_millis(20))
        .with_write_timeout(Duration::from_secs(1))
        .with_max_retries(1);

    let server_transport = SharedMemoryFrameTransport::create_server(&name, config.clone())
        .expect("server SHM mapping should be created");
    let client_transport = SharedMemoryFrameTransport::connect_client_with_config(&name, config)
        .expect("client should attach to the SHM mapping");
    let server_channel = Arc::new(MessageChannelAdapter::<_, JsonCodec>::with_codec(
        server_transport,
    ));
    let client_channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(client_transport);

    let server = RpcServer::with_codec(JsonCodec);
    server.register_stream("delayed", |_req: ()| {
        futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(120)).await;
            Ok::<_, xrpc::RpcError>(7_u64)
        })
    });
    let server_task = tokio::spawn({
        let server_channel = server_channel.clone();
        async move { server.serve(server_channel).await }
    });

    let client = RpcClient::with_codec(client_channel, JsonCodec);
    let client_handle = client.try_start().expect("fresh client should start");
    let mut stream = client
        .call_server_stream::<_, u64>("delayed", &())
        .await
        .expect("stream call should commit");
    let value = tokio::time::timeout(Duration::from_secs(2), stream.recv())
        .await
        .expect("delayed stream item should arrive")
        .expect("stream should produce an item")
        .expect("stream item should decode");
    assert_eq!(value, 7);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), stream.recv())
            .await
            .expect("normal stream end should arrive")
            .is_none()
    );

    client.close().await.expect("client close should succeed");
    client_handle
        .join()
        .await
        .expect("client receive task should join");
    let _ = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server session should end after client close")
        .expect("server task should not panic");
}
