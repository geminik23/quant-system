use std::sync::Arc;
use std::time::{Duration, Instant};

use qs_service::ServiceEndpoint;
use qs_service_xrpc::{
    ConnectionContext, JsonCodec, XrpcClientSession, XrpcTransportConfig, channel_pair, connect,
    serve_host, serve_transport,
};
use tokio::sync::watch;
use xrpc::RpcServer;

fn registrar() -> Arc<impl qs_service_xrpc::XrpcServiceRegistrar<JsonCodec>> {
    Arc::new(
        |server: &RpcServer<JsonCodec>, _context: &ConnectionContext| {
            server.register_typed("echo", |request: String| async move { Ok(request) });
            server.register_stream("numbers", |count: u64| {
                futures::stream::iter((0..count).map(Ok::<_, xrpc::RpcError>))
            });
        },
    )
}

async fn assert_contract(session: XrpcClientSession<JsonCodec>) {
    let response: String = session.call("echo", &"hello".to_string()).await.unwrap();
    assert_eq!(response, "hello");

    let mut stream = session
        .call_server_stream::<_, u64>("numbers", &3_u64)
        .await
        .unwrap();
    for expected in 0..3 {
        assert_eq!(stream.recv().await.unwrap().unwrap(), expected);
    }
    assert!(stream.recv().await.is_none());

    let started = Instant::now();
    tokio::time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("close must wake an idle 300-second receive")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
}

async fn connect_eventually(endpoint: &ServiceEndpoint) -> XrpcClientSession<JsonCodec> {
    let mut last_error = None;
    for _ in 0..40 {
        match connect(
            endpoint,
            "contract-client",
            &XrpcTransportConfig::default(),
            JsonCodec,
        )
        .await
        {
            Ok(session) => return session,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("failed to connect to {endpoint}: {}", last_error.unwrap());
}

async fn assert_host_contract(endpoint: ServiceEndpoint) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let host_endpoint = endpoint.clone();
    let host = tokio::spawn(async move {
        serve_host(
            host_endpoint,
            XrpcTransportConfig::default(),
            JsonCodec,
            registrar(),
            shutdown_rx,
        )
        .await
    });

    let session = connect_eventually(&endpoint).await;
    assert_contract(session).await;
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), host)
        .await
        .expect("host shutdown timed out")
        .expect("host task panicked")
        .expect("host returned an error");
}

#[tokio::test]
async fn channel_unary_stream_and_shutdown_contract() {
    let endpoint: ServiceEndpoint = "channel://contract".parse().unwrap();
    let (session, server_transport) =
        channel_pair(&endpoint, &XrpcTransportConfig::default(), JsonCodec).unwrap();
    let context = ConnectionContext {
        client_id: 1,
        client_name: Some("contract-client".into()),
        endpoint,
    };
    let server = tokio::spawn(serve_transport(
        server_transport,
        JsonCodec,
        registrar(),
        context,
    ));
    assert_contract(session).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("channel server did not stop after client close");
}

#[tokio::test]
async fn shared_memory_unary_stream_and_shutdown_contract() {
    let endpoint = format!("shm://qs-contract-{}", nanoid::nanoid!())
        .parse()
        .unwrap();
    assert_host_contract(endpoint).await;
}

#[cfg(unix)]
#[tokio::test]
async fn unix_unary_stream_and_shutdown_contract() {
    let endpoint = format!("unix:///tmp/qs-contract-{}.sock", nanoid::nanoid!())
        .parse()
        .unwrap();
    assert_host_contract(endpoint).await;
}

#[tokio::test]
async fn tcp_loopback_unary_stream_and_shutdown_contract() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);
    let endpoint = format!("tcp://{address}").parse().unwrap();
    assert_host_contract(endpoint).await;
}

#[tokio::test]
async fn tcp_non_loopback_requires_explicit_trust_boundary() {
    let endpoint: ServiceEndpoint = "tcp://0.0.0.0:41001".parse().unwrap();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let error = serve_host(
        endpoint,
        XrpcTransportConfig::default(),
        JsonCodec,
        registrar(),
        shutdown_rx,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("allow_insecure_non_loopback"));
}
