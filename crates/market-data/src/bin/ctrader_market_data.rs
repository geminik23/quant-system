//! Real-time market-data service hosted through the configured RPC transport.

use chrono::Utc;
use clap::Parser;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

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
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{BincodeCodec, ConnectionContext, XrpcTransportConfig, serve_host};

use xrpc::{RpcServer, ServerStreamSender};

// ── Config ──

#[derive(serde::Deserialize, Debug, Clone)]
struct MarketDataSection {
    #[serde(default)]
    endpoint: Option<ServiceEndpoint>,
    #[serde(default = "default_shm_name")]
    shm_name: String,
    shm_buffer_size: Option<usize>,
    #[serde(default = "default_max_connections")]
    max_connections: usize,
    #[serde(default)]
    allow_insecure_non_loopback: bool,
}

fn default_shm_name() -> String {
    "market-data".to_string()
}

fn default_max_connections() -> usize {
    256
}

impl MarketDataSection {
    fn resolved_endpoint(&self) -> std::result::Result<ServiceEndpoint, String> {
        if let Some(endpoint) = &self.endpoint {
            if self.shm_name != default_shm_name() {
                return Err("configure either market_data.endpoint or legacy market_data.shm_name, not both".into());
            }
            return Ok(endpoint.clone());
        }
        format!("shm://{}", self.shm_name)
            .parse()
            .map_err(|error| format!("invalid market_data.shm_name: {error}"))
    }
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
    /// Override market_data.endpoint from the configuration file.
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,
}

struct ClientCleanup {
    client_id: usize,
    state: Arc<XrpcState>,
    handler: Arc<MarketHandler>,
}

impl Drop for ClientCleanup {
    fn drop(&mut self) {
        let client_id = self.client_id;
        let state = Arc::clone(&self.state);
        let handler = Arc::clone(&self.handler);
        tokio::spawn(async move {
            let released = state.release_alerts_of(client_id).await;
            for alert_id in &released {
                handler.remove_price_alert(alert_id.clone()).await;
            }
            tracing::info!(
                client_id,
                released_alerts = released.len(),
                "market-data client disconnected"
            );
        });
    }
}

// ── Per-Client RPC Registration ──

fn register_client_handlers(
    server: &RpcServer<BincodeCodec>,
    context: &ConnectionContext,
    state: Arc<XrpcState>,
    handles: MarketManagerHandles,
) {
    let client_id = context.client_id;

    // Per-client subscription filter: None = not subscribed, Some(empty) = all, Some(set) = filtered
    let filter: Arc<RwLock<Option<HashSet<String>>>> = Arc::new(RwLock::new(None));

    let handler = handles.market_handler.clone();
    let cleanup = Arc::new(ClientCleanup {
        client_id,
        state: Arc::clone(&state),
        handler: Arc::clone(&handler),
    });

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
                    .set_alert_meta(&alert_id, &req.symbol, req.price, &req.kind)
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
                if let Some(owner) = state.owner_of(&req.alert_id).await
                    && owner != client_id
                {
                    return Ok(CommandAck::error("not owner"));
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

    // ── Register: get_alerts ──
    {
        let state = state.clone();
        server.register_typed("get_alerts", move |_req: ()| {
            let state = state.clone();
            async move {
                let entries = state.alerts_of(client_id).await;
                let alerts = entries
                    .into_iter()
                    .map(|(alert_id, symbol, price, kind)| AlertInfo {
                        alert_id,
                        symbol,
                        price,
                        kind,
                    })
                    .collect();
                Ok(GetAlertsResponse { alerts })
            }
        });
    }

    // ── Register: ping ──
    {
        let cleanup = Arc::clone(&cleanup);
        server.register_typed("ping", move |_req: ()| {
            let _cleanup = Arc::clone(&cleanup);
            async move {
                Ok(CommandAck::ok(
                    "PONG",
                    Utc::now().timestamp_millis().to_string(),
                ))
            }
        });
    }

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
                        if should_send
                            && sender
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
                            let (symbol, ref_price, _kind) = state
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

    // ── Register: stream_events (server streaming — prices + state changes) ──
    {
        let handles = handles.clone();
        let filter = filter.clone();
        server.register_stream_fn(
            "stream_events",
            move |_msg, sender: ServerStreamSender<_>| {
                let handles = handles.clone();
                let filter = filter.clone();
                async move {
                    let mut price_rx = handles.subscribe_price_ticks();
                    let mut state_rx = handles.subscribe_state_changes();

                    loop {
                        tokio::select! {
                            result = price_rx.recv() => {
                                let Ok(tick) = result else { break };
                                let f = filter.read().await;
                                let should_send = match &*f {
                                    None => false,
                                    Some(set) if set.is_empty() => true,
                                    Some(set) => set.contains(&tick.symbol),
                                };
                                drop(f);
                                if should_send {
                                    let event = StreamEvent {
                                        event_type: "PRICE".into(),
                                        symbol: Some(tick.symbol),
                                        bid: Some(tick.bid),
                                        ask: Some(tick.ask),
                                        state: None,
                                        ts_ms: tick.ts_ms,
                                    };
                                    if sender.send(event).is_err() { break; }
                                }
                            }
                            result = state_rx.recv() => {
                                let Ok(new_state) = result else { break };
                                let state_str = match new_state {
                                    ConnectionState::Connected => "CONNECTED",
                                    ConnectionState::Disconnected => "DISCONNECTED",
                                    ConnectionState::Connecting => "CONNECTING",
                                    ConnectionState::Logon => "LOGON",
                                };
                                let event = StreamEvent {
                                    event_type: "STATE".into(),
                                    symbol: None,
                                    bid: None,
                                    ask: None,
                                    state: Some(state_str.to_string()),
                                    ts_ms: Utc::now().timestamp_millis(),
                                };
                                if sender.send(event).is_err() { break; }
                            }
                        }
                    }
                    let _ = sender.end();
                    Ok(())
                }
            },
        );
    }

    tracing::info!(client_id, endpoint = %context.endpoint, "market-data client registered");
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

    let endpoint = match cli.endpoint {
        Some(endpoint) => endpoint,
        None => cfg
            .market_data
            .resolved_endpoint()
            .map_err(market_data::QuantError::Other)?,
    };
    let buffer_size = cfg.market_data.shm_buffer_size.unwrap_or(4 * 1024 * 1024);

    // Initialize market subsystem
    let market_handler = Arc::new(MarketHandler::new());
    let mut market_manager = MarketManager::new(cfg.ctrader.clone(), market_handler);

    // Extract shared handles *before* run_forever() takes ownership of the event loop.
    // Client handlers use these directly — no Mutex needed.
    let handles = market_manager.shared_handles();

    // Spawn MarketManager in background (owns the FIX connection + reconnect loop)
    let mm_handle = tokio::spawn(async move {
        if let Err(e) = market_manager.run_forever().await {
            tracing::error!("MarketManager run_forever error: {:?}", e);
        }
    });

    let state = Arc::new(XrpcState::new());
    tracing::info!(endpoint = %endpoint, "starting market-data service");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let registrar_state = Arc::clone(&state);
    let registrar_handles = handles.clone();
    let registrar = Arc::new(
        move |server: &RpcServer<BincodeCodec>, context: &ConnectionContext| {
            register_client_handlers(
                server,
                context,
                Arc::clone(&registrar_state),
                registrar_handles.clone(),
            );
        },
    );
    let transport_config = XrpcTransportConfig {
        buffer_bytes: buffer_size,
        maximum_message_bytes: buffer_size,
        maximum_connections: cfg.market_data.max_connections,
        allow_insecure_non_loopback: cfg.market_data.allow_insecure_non_loopback,
        ..XrpcTransportConfig::default()
    };
    let mut host = tokio::spawn(serve_host(
        endpoint,
        transport_config,
        BincodeCodec,
        registrar,
        shutdown_rx,
    ));

    tokio::select! {
        result = &mut host => {
            result
                .map_err(|error| market_data::QuantError::Other(format!("market-data host task failed: {error}")))?
                .map_err(|error| market_data::QuantError::Other(format!("market-data host failed: {error}")))?;
        }
        signal = tokio::signal::ctrl_c() => {
            signal?;
            tracing::info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
            host.await
                .map_err(|error| market_data::QuantError::Other(format!("market-data host task failed: {error}")))?
                .map_err(|error| market_data::QuantError::Other(format!("market-data host failed: {error}")))?;
        }
    }

    // ── Graceful Shutdown ──

    tracing::info!("Shutting down...");

    let _ = shutdown_tx.send(true);

    // Abort the MarketManager background task (this also aborts its
    // internal message handler task, preventing a task leak on shutdown)
    mm_handle.abort();
    let _ = mm_handle.await;

    tracing::info!("Server shut down cleanly");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_resolution_supports_current_legacy_and_conflict_diagnostics() {
        let current = MarketDataSection {
            endpoint: Some("unix:///tmp/market-data.sock".parse().unwrap()),
            shm_name: default_shm_name(),
            shm_buffer_size: None,
            max_connections: default_max_connections(),
            allow_insecure_non_loopback: false,
        };
        assert_eq!(
            current.resolved_endpoint().unwrap().to_string(),
            "unix:///tmp/market-data.sock"
        );

        let legacy = MarketDataSection {
            endpoint: None,
            shm_name: "legacy-market-data".into(),
            shm_buffer_size: None,
            max_connections: default_max_connections(),
            allow_insecure_non_loopback: false,
        };
        assert_eq!(
            legacy.resolved_endpoint().unwrap().to_string(),
            "shm://legacy-market-data"
        );

        let conflicting = MarketDataSection {
            endpoint: Some("tcp://127.0.0.1:42001".parse().unwrap()),
            shm_name: "legacy-market-data".into(),
            shm_buffer_size: None,
            max_connections: default_max_connections(),
            allow_insecure_non_loopback: false,
        };
        assert!(
            conflicting
                .resolved_endpoint()
                .unwrap_err()
                .contains("either")
        );
    }
}
