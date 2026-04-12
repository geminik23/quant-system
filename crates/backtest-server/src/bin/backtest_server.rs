//! Backtest server binary — acceptor loop with per-client SHM slots.
//!
//! Uses the same acceptor pattern as `qs-market-data`: clients connect to a
//! well-known SHM endpoint, receive a dedicated per-client slot, then use
//! that slot for all subsequent RPC calls.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use clap::Parser;
use tokio::task::JoinHandle;

use backtest_server::config::load_config;
use backtest_server::handlers::{
    ServerState, handle_add_profile, handle_list_profiles, handle_list_symbols, handle_ping,
    handle_reload_profiles, handle_remove_profile, handle_run_backtest, handle_run_backtest_multi,
};
use backtest_server::rpc_types::*;

use data_preprocess::ParquetStore;
use qs_backtest::profile::ProfileRegistry;
use qs_symbols::SymbolRegistry;

use xrpc::{MessageChannelAdapter, RpcServer, SharedMemoryConfig, SharedMemoryFrameTransport};

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

    /// Enable debug-level logging (overrides config).
    #[arg(long, default_value_t = false)]
    debug: bool,
}

// ── SHM Cleanup ─────────────────────────────────────────────────────────────

/// Remove stale shared memory files matching the given base name.
fn cleanup_shm(base_name: &str) {
    let shm_dir = std::path::Path::new("/dev/shm");
    if !shm_dir.exists() {
        return;
    }
    let entries = match std::fs::read_dir(shm_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();
        if name_str.starts_with(base_name) {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => tracing::info!("Cleaned up stale SHM file: {}", name_str),
                Err(e) => tracing::warn!("Failed to clean up SHM file {}: {}", name_str, e),
            }
        }
    }
}

// ── Per-Client Handler ──────────────────────────────────────────────────────

/// Spawn an RPC handler task for a single connected client.
fn spawn_client_handler(
    client_id: usize,
    slot_name: String,
    shm_config: SharedMemoryConfig,
    state: Arc<ServerState>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let transport = match SharedMemoryFrameTransport::create_server(&slot_name, shm_config) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(
                    "[client-{}] Failed to create SHM slot {}: {:?}",
                    client_id,
                    slot_name,
                    e
                );
                return;
            }
        };

        let channel = MessageChannelAdapter::new(transport);
        let server = RpcServer::new();

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
                async move {
                    handle_list_symbols(&state, &req).map_err(|e| xrpc::RpcError::ServerError(e))
                }
            });
        }

        // ── Register: run_backtest ──
        {
            let state = state.clone();
            server.register_typed("run_backtest", move |req: RunBacktestRequest| {
                let state = state.clone();
                async move {
                    let resp = handle_run_backtest(&state, &req);
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
                    let resp = handle_run_backtest_multi(&state, &req);
                    Ok(resp)
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

        tracing::info!(
            "[client-{}] Handler ready on shm://{}",
            client_id,
            slot_name
        );

        let channel = Arc::new(channel);
        if let Err(e) = server.serve(channel).await {
            tracing::debug!("[client-{}] Session ended: {:?}", client_id, e);
        }

        tracing::info!("[client-{}] Disconnected", client_id);
    })
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

    // 3. Load symbol registry (F06).
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

    // 4. Load profile registry (F09).
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
    let profiles_path = cfg.profiles.profiles_path.clone();
    let state = Arc::new(ServerState {
        symbol_registry,
        profile_registry: RwLock::new(profile_registry),
        data_dir: cfg.database.data_dir.clone(),
        profiles_path,
        start_time: std::time::Instant::now(),
    });

    // 7. SHM configuration.
    let shm_base = &cfg.server.shm_name;
    cleanup_shm(shm_base);

    let shm_config = SharedMemoryConfig::new()
        .with_buffer_size(cfg.server.shm_buffer_size)
        .with_read_timeout(Duration::from_secs(300))
        .with_write_timeout(Duration::from_secs(30));

    let acceptor_shm_config = SharedMemoryConfig::new()
        .with_buffer_size(64 * 1024)
        .with_read_timeout(Duration::from_secs(2))
        .with_write_timeout(Duration::from_secs(30));

    let accept_name = format!("{}-accept", shm_base);
    let client_seq = Arc::new(AtomicUsize::new(0));

    tracing::info!(
        "Starting backtest server (acceptor: shm://{}), buffer={}MB",
        accept_name,
        cfg.server.shm_buffer_size / (1024 * 1024)
    );

    // 8. Track client handler tasks.
    let mut client_handles: Vec<JoinHandle<()>> = Vec::new();

    // 9. Acceptor loop.
    loop {
        // Prune finished client handles.
        client_handles.retain(|h| !h.is_finished());

        let acceptor_transport = match SharedMemoryFrameTransport::create_server(
            &accept_name,
            acceptor_shm_config.clone(),
        ) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to create acceptor SHM: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let acceptor_channel = MessageChannelAdapter::new(acceptor_transport);
        let acceptor_server = RpcServer::new();

        let state_clone = state.clone();
        let shm_config_clone = shm_config.clone();
        let shm_name = cfg.server.shm_name.clone();
        let client_seq_clone = client_seq.clone();

        let spawned: Arc<tokio::sync::Mutex<Vec<JoinHandle<()>>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let spawned_inner = spawned.clone();

        acceptor_server.register_typed("connect", move |req: ConnectRequest| {
            let state = state_clone.clone();
            let shm_config = shm_config_clone.clone();
            let shm_name = shm_name.clone();
            let client_seq = client_seq_clone.clone();
            let spawned = spawned_inner.clone();
            async move {
                let client_id = client_seq.fetch_add(1, Ordering::SeqCst) + 1;
                let slot_name = format!("{}-client-{}", shm_name, client_id);

                tracing::info!(
                    "Client '{}' connecting, assigned id={} slot={}",
                    req.client_name,
                    client_id,
                    slot_name
                );

                let handle = spawn_client_handler(client_id, slot_name.clone(), shm_config, state);
                spawned.lock().await.push(handle);

                // Small delay to let the server-side SHM be created before client connects.
                tokio::time::sleep(Duration::from_millis(10)).await;

                Ok(ConnectResponse {
                    client_id,
                    slot_name,
                })
            }
        });

        let acceptor_channel = Arc::new(acceptor_channel);
        tokio::select! {
            result = acceptor_server.serve(acceptor_channel) => {
                if let Err(e) = result {
                    tracing::debug!("Acceptor session ended: {:?}", e);
                }
                // Collect any JoinHandles spawned during this accept cycle.
                let mut new_handles = spawned.lock().await;
                client_handles.append(&mut new_handles);
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Shutdown signal received (Ctrl-C)");
                break;
            }
        }
    }

    // ── Graceful Shutdown ──

    tracing::info!("Shutting down...");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let active = client_handles.iter().filter(|h| !h.is_finished()).count();
    if active > 0 {
        tracing::info!("Aborting {} remaining client handler(s)", active);
        for handle in &client_handles {
            handle.abort();
        }
        for handle in client_handles {
            let _ = handle.await;
        }
    }

    cleanup_shm(shm_base);

    tracing::info!("Server shut down cleanly");
    Ok(())
}
