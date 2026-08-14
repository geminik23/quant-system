//! Backtest server binary — acceptor loop with per-client SHM slots.
//!
//! Uses the same acceptor pattern as `qs-market-data`: clients connect to a
//! well-known SHM endpoint, receive a dedicated per-client slot, then use
//! that slot for all subsequent RPC calls.

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use clap::Parser;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{ConnectionContext, JsonCodec, XrpcTransportConfig, serve_host};
use tokio::task::JoinHandle;

use backtest_server::artifact_store::ArtifactStore;
use backtest_server::config::load_config;
use backtest_server::handlers::{
    ServerState, cancel_active_jobs, cleanup_expired_jobs, handle_add_profile,
    handle_cancel_backtest, handle_delete_result_artifact, handle_get_backtest_result,
    handle_get_backtest_status, handle_get_result_artifact_chunk, handle_list_profiles,
    handle_list_symbols, handle_ping, handle_reload_profiles, handle_remove_profile,
    handle_run_backtest, handle_run_backtest_multi, handle_submit_backtest, run_job_and_store,
    watch_backtest_stream,
};
use qs_backtest_api::*;

use data_preprocess::ParquetStore;
use qs_backtest::profile::ProfileRegistry;
use qs_symbols::SymbolRegistry;

use xrpc::RpcServer;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name = "backtest_server",
    about = "Backtest RPC server over shared memory"
)]
struct Cli {
    /// Path to the server TOML config file.
    #[arg(short, long)]
    config: String,

    /// Override the configured service endpoint (shm://, unix://, or tcp://).
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,

    /// Enable debug-level logging (overrides config).
    #[arg(long, default_value_t = false)]
    debug: bool,
}

// ── Per-Client Handler ──────────────────────────────────────────────────────

type BlockingJobHandles = Arc<Mutex<Vec<JoinHandle<()>>>>;

fn track_blocking_job(handles: &BlockingJobHandles, handle: JoinHandle<()>) {
    let mut handles = handles.lock().unwrap();
    handles.retain(|existing| !existing.is_finished());
    handles.push(handle);
}

async fn run_blocking_rpc<T, F>(label: &'static str, task: F) -> Result<T, xrpc::RpcError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| xrpc::RpcError::ServerError(format!("{label} task failed: {error}")))
}

async fn run_job_cleanup_loop(state: Arc<ServerState>, retention: Duration, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let removed = cleanup_expired_jobs(&state, retention);
        if removed > 0 {
            tracing::info!("Cleaned up {} expired backtest job(s)", removed);
        }
    }
}

async fn run_artifact_cleanup_loop(state: Arc<ServerState>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        match state.artifact_store.cleanup_expired() {
            Ok(removed) if removed > 0 => {
                tracing::info!("Cleaned up {} expired result artifact(s)", removed);
            }
            Ok(_) => {}
            Err(error) => tracing::warn!("Result artifact cleanup failed: {error}"),
        }
    }
}

/// Register the logical backtest methods for one provider connection.
fn register_client_handlers(
    server: &RpcServer<JsonCodec>,
    context: &ConnectionContext,
    state: Arc<ServerState>,
    blocking_jobs: BlockingJobHandles,
) {
    tracing::info!(
        client_id = context.client_id,
        client_name = ?context.client_name,
        endpoint = %context.endpoint.redacted(),
        "Registering backtest service connection"
    );

    // ── Register: ping ──
    {
        let state = state.clone();
        server.register_typed("ping", move |_req: ()| {
            let state = state.clone();
            async move {
                let resp = handle_ping(&state);
                Ok(resp)
            }
        });
    }

    // ── Register: list_profiles ──
    {
        let state = state.clone();
        server.register_typed("list_profiles", move |_req: ()| {
            let state = state.clone();
            async move {
                let resp = handle_list_profiles(&state);
                Ok(resp)
            }
        });
    }

    // ── Register: list_symbols ──
    {
        let state = state.clone();
        server.register_typed("list_symbols", move |req: ListSymbolsRequest| {
            let state = state.clone();
            async move { handle_list_symbols(&state, &req).map_err(xrpc::RpcError::ServerError) }
        });
    }

    // ── Register: run_backtest ──
    {
        let state = state.clone();
        server.register_typed("run_backtest", move |req: RunBacktestRequest| {
            let state = state.clone();
            async move {
                let resp = tokio::task::spawn_blocking(move || handle_run_backtest(&state, &req))
                    .await
                    .map_err(|e| {
                        xrpc::RpcError::ServerError(format!("backtest task failed: {e}"))
                    })?;
                Ok(resp)
            }
        });
    }

    // ── Register: run_backtest_multi ──
    {
        let state = state.clone();
        server.register_typed("run_backtest_multi", move |req: RunBacktestMultiRequest| {
            let state = state.clone();
            async move {
                run_blocking_rpc("multi-profile backtest", move || {
                    handle_run_backtest_multi(&state, &req)
                })
                .await
            }
        });
    }

    // ── Register: add_profile ──
    {
        let state = state.clone();
        server.register_typed("add_profile", move |req: AddProfileRequest| {
            let state = state.clone();
            async move {
                let resp = handle_add_profile(&state, &req);
                Ok(resp)
            }
        });
    }

    // ── Register: remove_profile ──
    {
        let state = state.clone();
        server.register_typed("remove_profile", move |req: RemoveProfileRequest| {
            let state = state.clone();
            async move {
                let resp = handle_remove_profile(&state, &req);
                Ok(resp)
            }
        });
    }

    // ── Register: reload_profiles ──
    {
        let state = state.clone();
        server.register_typed("reload_profiles", move |_req: ()| {
            let state = state.clone();
            async move {
                let resp = handle_reload_profiles(&state);
                Ok(resp)
            }
        });
    }

    // ── Register: submit_backtest ──
    {
        let state = state.clone();
        let blocking_jobs = blocking_jobs.clone();
        server.register_typed("submit_backtest", move |req: SubmitBacktestRequest| {
            let state = state.clone();
            let blocking_jobs = blocking_jobs.clone();
            async move {
                let submitted = handle_submit_backtest(&state, &req);
                if let Some(ref id) = submitted.job_id {
                    let id = id.clone();
                    let inner_req = req.request.clone();
                    let st = state.clone();
                    let handle = tokio::task::spawn_blocking(move || {
                        run_job_and_store(st, id, inner_req);
                    });
                    track_blocking_job(&blocking_jobs, handle);
                }
                Ok(submitted)
            }
        });
    }

    // ── Register: get_backtest_status (Issue 2) ──
    {
        let state = state.clone();
        server.register_typed(
            "get_backtest_status",
            move |req: GetBacktestStatusRequest| {
                let state = state.clone();
                async move { Ok(handle_get_backtest_status(&state, &req)) }
            },
        );
    }

    // ── Register: watch_backtest ──
    {
        let state = state.clone();
        server.register_stream("watch_backtest", move |req: WatchBacktestRequest| {
            watch_backtest_stream(state.clone(), req, Duration::from_secs(15))
        });
    }

    // ── Register: get_backtest_result (Issue 2) ──
    {
        let state = state.clone();
        server.register_typed(
            "get_backtest_result",
            move |req: GetBacktestResultRequest| {
                let state = state.clone();
                async move { Ok(handle_get_backtest_result(&state, &req)) }
            },
        );
    }

    {
        let state = state.clone();
        server.register_typed(
            "get_result_artifact_chunk",
            move |req: GetResultArtifactChunkRequest| {
                let state = state.clone();
                async move {
                    run_blocking_rpc("result artifact chunk", move || {
                        handle_get_result_artifact_chunk(&state, &req)
                    })
                    .await
                }
            },
        );
    }

    {
        let state = state.clone();
        server.register_typed(
            "delete_result_artifact",
            move |req: DeleteResultArtifactRequest| {
                let state = state.clone();
                async move {
                    run_blocking_rpc("result artifact delete", move || {
                        handle_delete_result_artifact(&state, &req)
                    })
                    .await
                }
            },
        );
    }

    // ── Register: cancel_backtest (Issue 2) ──
    {
        let state = state.clone();
        server.register_typed("cancel_backtest", move |req: CancelBacktestRequest| {
            let state = state.clone();
            async move { Ok(handle_cancel_backtest(&state, &req)) }
        });
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // 1. Load config.
    let cfg = load_config(&cli.config)?;

    // 2. Initialize tracing.
    unsafe {
        if cli.debug {
            std::env::set_var("RUST_LOG", "debug");
        } else {
            std::env::set_var("RUST_LOG", &cfg.logging.level);
        }
    }
    tracing_subscriber::fmt::init();

    // 3. Load the symbol registry.
    let symbol_registry = SymbolRegistry::load(&cfg.symbols.registry_path).map_err(|e| {
        format!(
            "Failed to load symbol registry '{}': {}",
            cfg.symbols.registry_path, e
        )
    })?;
    tracing::info!(
        "Loaded {} symbols from {}",
        symbol_registry.len(),
        cfg.symbols.registry_path
    );

    let instrument_domain =
        backtest_server::instrument_domain_from_config(&cfg.instruments, &symbol_registry)?;
    tracing::info!(
        "Loaded instrument catalog {}",
        instrument_domain.snapshot().id().version
    );

    // 4. Load the management profile registry.
    let profile_registry = ProfileRegistry::load(&cfg.profiles.profiles_path).map_err(|e| {
        format!(
            "Failed to load profiles '{}': {}",
            cfg.profiles.profiles_path, e
        )
    })?;
    tracing::info!(
        "Loaded {} profiles from {}",
        profile_registry.len(),
        cfg.profiles.profiles_path
    );

    // 5. Verify Parquet data store is accessible.
    let _store = ParquetStore::open(&cfg.database.data_dir).map_err(|e| {
        format!(
            "Failed to open data store '{}': {}",
            cfg.database.data_dir, e
        )
    })?;
    tracing::info!("Data store verified: {}", cfg.database.data_dir);

    // 6. Build shared state.
    let artifact_response_budget = cfg.server.shm_buffer_size.saturating_sub(64 * 1024);
    let encoded_chunk_size = cfg.artifacts.chunk_size.div_ceil(3).saturating_mul(4);
    if cfg.artifacts.inline_limit_bytes > artifact_response_budget {
        return Err(format!(
            "artifact inline_limit_bytes {} exceeds the SHM response budget {}",
            cfg.artifacts.inline_limit_bytes, artifact_response_budget
        )
        .into());
    }
    if encoded_chunk_size > artifact_response_budget {
        return Err(format!(
            "base64-encoded artifact chunk size {} exceeds the SHM response budget {}",
            encoded_chunk_size, artifact_response_budget
        )
        .into());
    }
    let artifact_store = ArtifactStore::new(
        &cfg.artifacts.directory,
        cfg.artifacts.inline_limit_bytes,
        cfg.artifacts.chunk_size,
        Duration::from_secs(cfg.artifacts.retention_secs),
        cfg.artifacts.max_total_bytes,
    )?;
    tracing::info!(
        "Result artifact store ready: {}, inline_limit={} bytes, chunk_size={} bytes",
        cfg.artifacts.directory,
        artifact_store.inline_limit_bytes(),
        artifact_store.chunk_size()
    );
    let profiles_path = cfg.profiles.profiles_path.clone();
    let state = Arc::new(ServerState {
        symbol_registry,
        instrument_domain,
        profile_registry: RwLock::new(profile_registry),
        data_dir: cfg.database.data_dir.clone(),
        profiles_path,
        start_time: std::time::Instant::now(),
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        max_retained_jobs: cfg.jobs.max_retained_jobs,
        artifact_store,
    });
    let cleanup_state = state.clone();
    let job_retention = Duration::from_secs(cfg.jobs.retention_secs);
    let cleanup_interval = Duration::from_secs(cfg.jobs.cleanup_interval_secs.max(1));
    let cleanup_handle = tokio::spawn(run_job_cleanup_loop(
        cleanup_state,
        job_retention,
        cleanup_interval,
    ));
    let artifact_cleanup_interval =
        Duration::from_secs((cfg.artifacts.retention_secs / 2).clamp(1, 60));
    let artifact_cleanup_handle = tokio::spawn(run_artifact_cleanup_loop(
        state.clone(),
        artifact_cleanup_interval,
    ));

    // 7. Resolve provider-neutral endpoint and shared xrpc host settings.
    let endpoint = match cli.endpoint {
        Some(endpoint) => endpoint,
        None => cfg.server.resolved_endpoint()?,
    };
    let transport_config = XrpcTransportConfig {
        buffer_bytes: cfg.server.shm_buffer_size,
        maximum_message_bytes: cfg.server.shm_buffer_size,
        maximum_connections: cfg.server.max_connections,
        allow_insecure_non_loopback: cfg.server.allow_insecure_non_loopback,
        ..XrpcTransportConfig::default()
    };
    tracing::info!(endpoint = %endpoint.redacted(), "Starting backtest service");

    // 8. Track blocking async-job workers. Connection tasks are owned by the host.
    let blocking_jobs: BlockingJobHandles = Arc::new(Mutex::new(Vec::new()));
    let registrar_state = state.clone();
    let registrar_jobs = blocking_jobs.clone();
    let registrar = Arc::new(
        move |server: &RpcServer<JsonCodec>, context: &ConnectionContext| {
            register_client_handlers(
                server,
                context,
                registrar_state.clone(),
                registrar_jobs.clone(),
            );
        },
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut host = tokio::spawn(serve_host(
        endpoint,
        transport_config,
        JsonCodec,
        registrar,
        shutdown_rx,
    ));
    tokio::select! {
        result = &mut host => {
            result??;
        }
        result = tokio::signal::ctrl_c() => {
            result?;
            tracing::info!("Shutdown signal received (Ctrl-C)");
            let _ = shutdown_tx.send(true);
            host.await??;
        }
    }

    // ── Graceful Shutdown ──

    tracing::info!("Shutting down...");
    cleanup_handle.abort();
    let _ = cleanup_handle.await;
    artifact_cleanup_handle.abort();
    let _ = artifact_cleanup_handle.await;

    let cancelled = cancel_active_jobs(&state);
    if cancelled > 0 {
        tracing::info!("Cancelled {} active backtest job(s)", cancelled);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let blocking_handles = {
        let mut handles = blocking_jobs.lock().unwrap();
        std::mem::take(&mut *handles)
    };
    if !blocking_handles.is_empty() {
        tracing::info!(
            "Waiting for {} blocking backtest worker(s)",
            blocking_handles.len()
        );
        for handle in blocking_handles {
            let _ = handle.await;
        }
    }

    tracing::info!("Server shut down cleanly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_canonical_backtest_execution_methods_are_registered() {
        let source = include_str!("backtest_server.rs");
        for method in [
            "run_backtest_v2",
            "run_backtest_multi_v2",
            "submit_backtest_v2",
        ] {
            assert!(
                !source.contains(&format!("register_typed(\"{method}\"")),
                "version-suffixed method `{method}` must not be registered"
            );
        }
        for method in ["run_backtest", "run_backtest_multi", "submit_backtest"] {
            assert!(
                source.contains(&format!("\"{method}\"")),
                "current method `{method}` must remain registered"
            );
        }
        assert!(
            source.contains("register_stream(\"watch_backtest\""),
            "retained backtest status streaming must remain registered"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn scheduled_cleanup_invokes_retention_helper() {
        use backtest_server::handlers::{BacktestJob, JobCancellationToken, JobStatus};

        let state = Arc::new(ServerState {
            symbol_registry: SymbolRegistry::empty(),
            instrument_domain: backtest_server::InstrumentDomain::compatibility(
                &SymbolRegistry::empty(),
            )
            .unwrap(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: String::new(),
            profiles_path: String::new(),
            start_time: std::time::Instant::now(),
            jobs: Mutex::new(std::collections::HashMap::from([(
                "expired".into(),
                BacktestJob {
                    status: JobStatus::Cancelled,
                    submitted_at: std::time::Instant::now(),
                    completed_at: Some(std::time::Instant::now()),
                    progress: BacktestProgress::default(),
                    result: None,
                    artifact: None,
                    inline_complete: true,
                    artifact_consumed: false,
                    error: None,
                    cancellation: JobCancellationToken::default(),
                    worker_active: false,
                    updates: tokio::sync::watch::channel(BacktestStatusResponse {
                        success: true,
                        job_id: "expired".into(),
                        status: "Cancelled".into(),
                        error: None,
                        elapsed_ms: Some(0),
                        progress: BacktestProgress::default(),
                    })
                    .0,
                },
            )])),
            max_retained_jobs: 10,
            artifact_store: ArtifactStore::new(
                std::env::temp_dir().join(format!(
                    "qs_backtest_server_binary_artifacts_{}",
                    std::process::id()
                )),
                12 * 1024 * 1024,
                1024 * 1024,
                Duration::from_secs(3_600),
                1024 * 1024 * 1024,
            )
            .unwrap(),
        });
        let cleanup = tokio::spawn(run_job_cleanup_loop(
            state.clone(),
            Duration::ZERO,
            Duration::from_millis(5),
        ));
        tokio::time::sleep(Duration::from_millis(25)).await;
        cleanup.abort();
        let _ = cleanup.await;

        assert!(state.jobs.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_rpc_helper_keeps_async_worker_responsive() {
        let work = run_blocking_rpc("test", || {
            std::thread::sleep(Duration::from_millis(100));
            7
        });
        tokio::pin!(work);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            result = &mut work => panic!("blocking work completed on async worker: {result:?}"),
        }

        assert_eq!(work.await.unwrap(), 7);
    }
}
