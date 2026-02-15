//! xrpc market data server with shared memory transport.
//!
//! Uses an acceptor pattern: clients connect to a well-known shm endpoint,
//! receive a dedicated per-client shm slot, then use that for all RPC calls.

use chrono::Utc;
use clap::Parser;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::RwLock;

use market_data::Result;
use market_data::core::AlertSet;
use market_data::core::ctrader_type::CTraderFixConfig;
use market_data::market_data::{
    ConnectionState, MarketManagerHandles, market_handler::MarketHandler,
    market_manager::MarketManager,
};
use market_data::rpc_types::*;
use market_data::utils::load_config;
use market_data::xrpc_state::XrpcState;

use xrpc::{
    MessageChannelAdapter, RpcServer, ServerStreamSender, SharedMemoryConfig,
    SharedMemoryFrameTransport,
};

// ── Config ──

#[derive(serde::Deserialize, Debug, Clone)]
struct MarketDataSection {
    shm_name: String,
    shm_buffer_size: Option<usize>,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct LoggingSection {
    level: Option<String>,
}

#[derive(serde::Deserialize, Debug, Clone)]
struct ConfigRoot {
    ctrader: CTraderFixConfig,
    market_data: MarketDataSection,
    logging: Option<LoggingSection>,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "xrpc Market Data & Alerts Server")]
struct Cli {
    #[arg(short, long, value_name = "CONFIG FILE")]
    config: std::path::PathBuf,
    #[arg(long)]
    debug: bool,
}

// ── Per-Client Handler ──

fn spawn_client_handler(
    client_id: usize,
    slot_name: String,
    shm_config: SharedMemoryConfig,
    state: Arc<XrpcState>,
    handles: MarketManagerHandles,
) {
    tokio::spawn(async move {
        let transport = match SharedMemoryFrameTransport::create_server(&slot_name, shm_config) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to create shm slot {}: {:?}", slot_name, e);
                return;
            }
        };

        let channel = MessageChannelAdapter::new(transport);
        let server = RpcServer::new();

        // Per-client subscription filter: None = not subscribed, Some(empty) = all, Some(set) = filtered
        let filter: Arc<RwLock<Option<HashSet<String>>>> = Arc::new(RwLock::new(None));

        let handler = handles.market_handler.clone();

        // ── Register: get_price ──
        {
            let handler = handler.clone();
            server.register_typed("get_price", move |req: GetPriceRequest| {
                let handler = handler.clone();
                async move {
                    let (bid, ask, found) = match handler.get_last_bid_ask(&req.symbol).await {
                        Some((b, a)) => (b, a, true),
                        None => (0.0, 0.0, false),
                    };
                    Ok(GetPriceResponse {
                        symbol: req.symbol,
                        bid,
                        ask,
                        ts_ms: Utc::now().timestamp_millis(),
                        found,
                    })
                }
            });
        }

        // ── Register: get_prices ──
        {
            let handler = handler.clone();
            server.register_typed("get_prices", move |req: GetPricesRequest| {
                let handler = handler.clone();
                async move {
                    let mut prices = Vec::with_capacity(req.symbols.len());
                    for sym in req.symbols {
                        let (bid, ask, found) = match handler.get_last_bid_ask(&sym).await {
                            Some((b, a)) => (b, a, true),
                            None => (0.0, 0.0, false),
                        };
                        prices.push(PriceSnapshot {
                            symbol: sym,
                            bid,
                            ask,
                            ts_ms: Utc::now().timestamp_millis(),
                            found,
                        });
                    }
                    Ok(GetPricesResponse { prices })
                }
            });
        }

        // ── Register: get_symbols ──
        {
            let handler = handler.clone();
            server.register_typed("get_symbols", move |_req: ()| {
                let handler = handler.clone();
                async move {
                    let symbols = handler.get_all_symbols().await;
                    Ok(GetSymbolListResponse { symbols })
                }
            });
        }

        // ── Register: get_state ──
        {
            let handles = handles.clone();
            server.register_typed("get_state", move |_req: ()| {
                let handles = handles.clone();
                async move {
                    let state_val = handles.get_connection_state().await;
                    let state_str = match state_val {
                        ConnectionState::Connected => "CONNECTED",
                        ConnectionState::Disconnected => "DISCONNECTED",
                        ConnectionState::Connecting => "CONNECTING",
                        ConnectionState::Logon => "LOGON",
                    };
                    Ok(GetStateResponse {
                        state: state_str.to_string(),
                        ts_ms: Utc::now().timestamp_millis(),
                    })
                }
            });
        }

        // ── Register: subscribe ──
        {
            let filter = filter.clone();
            server.register_typed("subscribe", move |req: SubscribePricesRequest| {
                let filter = filter.clone();
                async move {
                    let mut w = filter.write().await;
                    let reference = if req.symbols.is_empty() {
                        *w = Some(HashSet::new());
                        "all symbols".to_string()
                    } else {
                        let set = w.get_or_insert_with(HashSet::new);
                        for s in req.symbols {
                            set.insert(s);
                        }
                        format!("symbols={}", set.len())
                    };
                    Ok(CommandAck::ok("SUBSCRIBED", reference))
                }
            });
        }

        // ── Register: unsubscribe ──
        {
            let filter = filter.clone();
            server.register_typed("unsubscribe", move |req: UnsubscribePricesRequest| {
                let filter = filter.clone();
                async move {
                    let mut w = filter.write().await;
                    let reference = if let Some(set) = w.as_mut() {
                        for s in req.symbols {
                            set.remove(&s);
                        }
                        format!("remaining={}", set.len())
                    } else {
                        "no active subscription".to_string()
                    };
                    Ok(CommandAck::ok("UNSUBSCRIBED", reference))
                }
            });
        }

        // ── Register: clear_subscription ──
        {
            let filter = filter.clone();
            server.register_typed("clear_subscription", move |_req: ()| {
                let filter = filter.clone();
                async move {
                    let mut w = filter.write().await;
                    *w = None;
                    Ok(CommandAck::ok("CLEARED", "all subscriptions cleared"))
                }
            });
        }

        // ── Register: set_alert ──
        {
            let handler = handler.clone();
            let state = state.clone();
            server.register_typed("set_alert", move |req: SetAlertRequest| {
                let handler = handler.clone();
                let state = state.clone();
                async move {
                    let set = match req.kind.as_str() {
                        "ABOVE" => AlertSet::High(req.price),
                        "BELOW" => AlertSet::Low(req.price),
                        _ => return Ok(CommandAck::error("invalid kind")),
                    };
                    let alert_id = if req.alert_id.is_empty() {
                        format!("c{client_id}-{}", nanoid::nanoid!())
                    } else {
                        req.alert_id
                    };
                    handler
                        .set_price_alert(req.symbol.clone(), set, Some(alert_id.clone()))
                        .await;
                    state.own_alert(&alert_id, client_id).await;
                    state
                        .set_alert_meta(&alert_id, &req.symbol, req.price)
                        .await;
                    Ok(CommandAck::ok("ALERT_SET", alert_id))
                }
            });
        }

        // ── Register: remove_alert ──
        {
            let handler = handler.clone();
            let state = state.clone();
            server.register_typed("remove_alert", move |req: RemoveAlertRequest| {
                let handler = handler.clone();
                let state = state.clone();
                async move {
                    if let Some(owner) = state.owner_of(&req.alert_id).await {
                        if owner != client_id {
                            return Ok(CommandAck::error("not owner"));
                        }
                    }
                    let removed = handler
                        .remove_price_alert(req.alert_id.clone())
                        .await
                        .is_some();
                    if removed {
                        state.release_alert(&req.alert_id).await;
                        Ok(CommandAck::ok("ALERT_REMOVED", req.alert_id))
                    } else {
                        Ok(CommandAck::error("alert not found"))
                    }
                }
            });
        }

        // ── Register: ping ──
        server.register_typed("ping", |_req: ()| async move {
            Ok(CommandAck::ok(
                "PONG",
                Utc::now().timestamp_millis().to_string(),
            ))
        });

        // ── Register: stream_prices (server streaming) ──
        {
            let handles = handles.clone();
            let filter = filter.clone();
            server.register_stream_fn(
                "stream_prices",
                move |_msg, sender: ServerStreamSender<_>| {
                    let handles = handles.clone();
                    let filter = filter.clone();
                    async move {
                        let mut price_rx = handles.subscribe_price_ticks();
                        while let Ok(tick) = price_rx.recv().await {
                            let f = filter.read().await;
                            let should_send = match &*f {
                                None => false,
                                Some(set) if set.is_empty() => true,
                                Some(set) => set.contains(&tick.symbol),
                            };
                            drop(f);
                            if should_send {
                                if sender
                                    .send(PriceTick {
                                        symbol: tick.symbol,
                                        bid: tick.bid,
                                        ask: tick.ask,
                                        ts_ms: tick.ts_ms,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                        let _ = sender.end();
                        Ok(())
                    }
                },
            );
        }

        // ── Register: stream_alerts (server streaming) ──
        {
            let handles = handles.clone();
            let state = state.clone();
            server.register_stream_fn(
                "stream_alerts",
                move |_msg, sender: ServerStreamSender<_>| {
                    let handles = handles.clone();
                    let state = state.clone();
                    async move {
                        let mut alert_rx = handles.subscribe_alerts();
                        while let Ok(event) = alert_rx.recv().await {
                            if state.owner_of(&event.alert_id).await == Some(client_id) {
                                let (symbol, ref_price) = state
                                    .take_alert_meta(&event.alert_id)
                                    .await
                                    .unwrap_or_default();
                                if sender
                                    .send(AlertResult {
                                        alert_id: event.alert_id,
                                        status: "TRIGGERED".into(),
                                        symbol,
                                        ref_price,
                                        ts_ms: event.ts_ms,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                        let _ = sender.end();
                        Ok(())
                    }
                },
            );
        }

        tracing::info!("Client {} connected on slot {}", client_id, slot_name);

        // Serve until client disconnects
        let channel = Arc::new(channel);
        if let Err(e) = server.serve(channel).await {
            tracing::warn!("Client {} session ended: {:?}", client_id, e);
        }

        // Cleanup: release owned alerts from both state and MarketHandler
        let released = state.release_alerts_of(client_id).await;
        if !released.is_empty() {
            for alert_id in &released {
                handler.remove_price_alert(alert_id.clone()).await;
            }
            tracing::info!(
                "Client {} disconnected, cleaned up {} alerts",
                client_id,
                released.len()
            );
        } else {
            tracing::info!("Client {} disconnected", client_id);
        }
    });
}

// ── Main ──

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = load_config::<ConfigRoot>(&cli.config).expect("config load failed");
    unsafe {
        if cli.debug {
            std::env::set_var("RUST_LOG", "debug");
        } else if let Some(log) = &cfg.logging {
            if let Some(level) = &log.level {
                std::env::set_var("RUST_LOG", level);
            }
        } else {
            std::env::set_var("RUST_LOG", "info");
        }
    }
    market_data::utils::setup();

    // Build shared memory config for per-client slots (long read timeout for idle clients)
    let buffer_size = cfg.market_data.shm_buffer_size.unwrap_or(4 * 1024 * 1024);
    let shm_config = SharedMemoryConfig::new()
        .with_buffer_size(buffer_size)
        .with_read_timeout(Duration::from_secs(300))
        .with_write_timeout(Duration::from_secs(30));

    // Smaller config for the acceptor endpoint (only handles a single connect RPC)
    let acceptor_shm_config = SharedMemoryConfig::new()
        .with_buffer_size(64 * 1024)
        .with_read_timeout(Duration::from_secs(300))
        .with_write_timeout(Duration::from_secs(30));

    // Initialize market subsystem
    let market_handler = Arc::new(MarketHandler::new());
    let mut market_manager = MarketManager::new(cfg.ctrader.clone(), market_handler);

    // Extract shared handles *before* run_forever() takes ownership of the event loop.
    // Client handlers use these directly — no Mutex needed.
    let handles = market_manager.shared_handles();

    // Spawn MarketManager in background (owns the FIX connection + reconnect loop)
    tokio::spawn(async move {
        if let Err(e) = market_manager.run_forever().await {
            tracing::error!("MarketManager run_forever error: {:?}", e);
        }
    });

    let state = Arc::new(XrpcState::new());
    let accept_name = format!("{}-accept", cfg.market_data.shm_name);

    tracing::info!(
        "Starting xrpc market data server (acceptor: shm://{})",
        accept_name
    );

    // Spawn a Ctrl-C watcher
    let shutdown = tokio::spawn(async {
        let _ = signal::ctrl_c().await;
        tracing::info!("Shutdown signal received");
    });

    // Acceptor loop: wait for client connections
    loop {
        // Check if shutdown was requested
        if shutdown.is_finished() {
            tracing::info!("Shutting down acceptor loop");
            break;
        }

        // Create the acceptor shm endpoint
        let acceptor_transport = match SharedMemoryFrameTransport::create_server(
            &accept_name,
            acceptor_shm_config.clone(),
        ) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to create acceptor shm: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let acceptor_channel = MessageChannelAdapter::new(acceptor_transport);
        let acceptor_server = RpcServer::new();

        let state_clone = state.clone();
        let handles_clone = handles.clone();
        let shm_config_clone = shm_config.clone();
        let shm_name = cfg.market_data.shm_name.clone();

        acceptor_server.register_typed("connect", move |req: ConnectRequest| {
            let state = state_clone.clone();
            let handles = handles_clone.clone();
            let shm_config = shm_config_clone.clone();
            let shm_name = shm_name.clone();
            async move {
                let client_id = state.next_client_id().await;
                let slot_name = format!("{}-client-{}", shm_name, client_id);

                tracing::info!(
                    "Client '{}' connecting, assigned id={} slot={}",
                    req.client_name,
                    client_id,
                    slot_name
                );

                spawn_client_handler(client_id, slot_name.clone(), shm_config, state, handles);

                // Small delay to let the server-side shm be created before client connects
                tokio::time::sleep(Duration::from_millis(10)).await;

                Ok(ConnectResponse {
                    client_id,
                    slot_name,
                })
            }
        });

        // Serve this single acceptor connection, then loop
        let acceptor_channel = Arc::new(acceptor_channel);
        if let Err(e) = acceptor_server.serve(acceptor_channel).await {
            // Client disconnected from acceptor — this is expected
            tracing::debug!("Acceptor session ended: {:?}", e);
        }
    }

    Ok(())
}
