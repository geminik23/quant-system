//! gRPC market data server with bidirectional streaming support.
//!
//! Provides real-time price streaming, price alerts, and unary price queries.
//! Uses MarketManager's broadcast channels for event distribution.

use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};
use tokio::signal;
use tokio::sync::{RwLock, broadcast, mpsc};
use tonic::{Request, Response, Status, transport::Server};

use quant::utils::load_config;
use quant::{
    core::ctrader_type::CTraderFixConfig, market_data::market_handler::MarketHandler,
    market_data::market_manager::MarketManager,
};

use quant::Result;

pub mod grpc {
    include!("../grpc/quant.rs");
}

use grpc::{
    AlertResult, CommandAck, GetPriceRequest, GetPriceResponse, GetPricesRequest,
    GetPricesResponse, GetStateRequest, GetStateResponse, GetSymbolListRequest,
    GetSymbolListResponse, PriceSnapshot, PriceTick, StreamCommand, StreamEvent,
    market_stream_server::{MarketStream, MarketStreamServer},
    stream_command, stream_event,
};

/// gRPC-specific state for managing client connections and alert ownership
struct GrpcState {
    price_tx: broadcast::Sender<PriceTick>,
    alert_tx: broadcast::Sender<AlertResult>,
    alerts_owner: Arc<RwLock<HashMap<String, usize>>>,
    client_seq: Arc<RwLock<usize>>,
    alert_meta: Arc<RwLock<HashMap<String, (String, f64)>>>,
}

impl GrpcState {
    fn new(
        price_tx: broadcast::Sender<PriceTick>,
        alert_tx: broadcast::Sender<AlertResult>,
    ) -> Self {
        Self {
            price_tx,
            alert_tx,
            alerts_owner: Arc::new(RwLock::new(HashMap::new())),
            client_seq: Arc::new(RwLock::new(0)),
            alert_meta: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    async fn next_client_id(&self) -> usize {
        let mut w = self.client_seq.write().await;
        *w += 1;
        *w
    }
    async fn own_alert(&self, alert_id: &str, client_id: usize) {
        self.alerts_owner
            .write()
            .await
            .insert(alert_id.to_string(), client_id);
    }
    async fn release_alerts_of(&self, client_id: usize) {
        let mut owners = self.alerts_owner.write().await;
        let mut meta = self.alert_meta.write().await;
        let ids: Vec<String> = owners
            .iter()
            .filter_map(|(id, owner)| {
                if *owner == client_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for id in &ids {
            owners.remove(id);
            meta.remove(id);
        }
    }
    async fn owner_of(&self, alert_id: &str) -> Option<usize> {
        self.alerts_owner.read().await.get(alert_id).copied()
    }
    async fn set_alert_meta(&self, alert_id: &str, symbol: &str, ref_price: f64) {
        self.alert_meta
            .write()
            .await
            .insert(alert_id.to_string(), (symbol.to_string(), ref_price));
    }
    async fn take_alert_meta(&self, alert_id: &str) -> Option<(String, f64)> {
        self.alert_meta.write().await.remove(alert_id)
    }
    async fn release_alert(&self, alert_id: &str) {
        self.alerts_owner.write().await.remove(alert_id);
        self.alert_meta.write().await.remove(alert_id);
    }
}

pub struct MarketStreamSvc {
    state: Arc<GrpcState>,
    market_manager: Arc<tokio::sync::Mutex<MarketManager>>,
}

#[tonic::async_trait]
impl MarketStream for MarketStreamSvc {
    type StreamStream =
        Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<StreamEvent, Status>> + Send>>;

    async fn get_price(
        &self,
        request: Request<GetPriceRequest>,
    ) -> std::result::Result<Response<GetPriceResponse>, Status> {
        let symbol = request.into_inner().symbol;
        let mm = self.market_manager.lock().await;
        let handler = mm.market_handler();
        let (bid, ask, found) = match handler.get_last_bid_ask(&symbol).await {
            Some((b, a)) => (b, a, true),
            None => (0.0, 0.0, false),
        };
        let resp = GetPriceResponse {
            symbol,
            bid,
            ask,
            ts_ms: Utc::now().timestamp_millis(),
            found,
        };
        Ok(Response::new(resp))
    }

    async fn get_prices(
        &self,
        request: Request<GetPricesRequest>,
    ) -> std::result::Result<Response<GetPricesResponse>, Status> {
        let symbols = request.into_inner().symbols;
        let mm = self.market_manager.lock().await;
        let handler = mm.market_handler();
        let mut snapshots = Vec::with_capacity(symbols.len());
        for sym in symbols.into_iter() {
            let (bid, ask, found) = match handler.get_last_bid_ask(&sym).await {
                Some((b, a)) => (b, a, true),
                None => (0.0, 0.0, false),
            };
            snapshots.push(PriceSnapshot {
                symbol: sym,
                bid,
                ask,
                ts_ms: Utc::now().timestamp_millis(),
                found,
            });
        }
        Ok(Response::new(GetPricesResponse { prices: snapshots }))
    }

    async fn get_symbol_list(
        &self,
        _request: Request<GetSymbolListRequest>,
    ) -> std::result::Result<Response<GetSymbolListResponse>, Status> {
        let mm = self.market_manager.lock().await;
        let handler = mm.market_handler();
        let symbols = handler.get_all_symbols().await;
        Ok(Response::new(GetSymbolListResponse { symbols }))
    }

    async fn get_state(
        &self,
        _request: Request<GetStateRequest>,
    ) -> std::result::Result<Response<GetStateResponse>, Status> {
        let mm = self.market_manager.lock().await;
        let state = mm.get_connection_state().await;
        let state_str = match state {
            quant::market_data::ConnectionState::Connected => "CONNECTED",
            quant::market_data::ConnectionState::Disconnected => "DISCONNECTED",
            quant::market_data::ConnectionState::Connecting => "CONNECTING",
            quant::market_data::ConnectionState::Logon => "LOGON",
        };
        Ok(Response::new(GetStateResponse {
            state: state_str.to_string(),
            ts_ms: Utc::now().timestamp_millis(),
        }))
    }

    async fn stream(
        &self,
        request: Request<tonic::Streaming<StreamCommand>>,
    ) -> std::result::Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let mut price_rx = self.state.price_tx.subscribe();
        let mut alert_rx = self.state.alert_tx.subscribe();
        let client_id = self.state.next_client_id().await;

        // Use Option to distinguish: None = no subscription, Some(empty) = subscribe all
        let filter_symbols = Arc::new(RwLock::new(Option::<HashSet<String>>::None));
        let filter_clone_prices = filter_symbols.clone();

        let (out_tx, out_rx) = mpsc::channel::<StreamEvent>(1024);
        let out_tx_prices = out_tx.clone();
        let out_tx_alerts = out_tx.clone();
        let out_tx_cmd = out_tx.clone();

        tokio::spawn(async move {
            while let Ok(t) = price_rx.recv().await {
                let filter = filter_clone_prices.read().await;
                let should_send = match &*filter {
                    None => false,                        // No subscription yet
                    Some(set) if set.is_empty() => true,  // Subscribe to all
                    Some(set) => set.contains(&t.symbol), // Filter by symbols
                };
                if should_send {
                    let _ = out_tx_prices
                        .send(StreamEvent {
                            evt: Some(stream_event::Evt::PriceTick(t)),
                        })
                        .await;
                }
            }
        });

        let state_for_alert_route = self.state.clone();
        tokio::spawn(async move {
            while let Ok(a) = alert_rx.recv().await {
                if let Some(owner) = state_for_alert_route.owner_of(&a.alert_id).await {
                    if owner == client_id {
                        let _ = out_tx_alerts
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Alert(a)),
                            })
                            .await;
                    }
                }
            }
        });

        let market_manager = self.market_manager.clone();
        let state_for_cmd = self.state.clone();
        tokio::spawn(async move {
            while let Some(Ok(cmd)) = inbound.next().await {
                match cmd.cmd {
                    Some(stream_command::Cmd::SubscribePrices(sp)) => {
                        let mut w = filter_symbols.write().await;
                        let reference = if sp.symbols.is_empty() {
                            // Empty list means subscribe to all symbols
                            *w = Some(HashSet::new());
                            "all symbols".to_string()
                        } else {
                            // Add new symbols to subscription
                            let set = w.get_or_insert_with(HashSet::new);
                            for s in sp.symbols {
                                set.insert(s);
                            }
                            format!("symbols={}", set.len())
                        };
                        let _ = out_tx_cmd
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Ack(CommandAck {
                                    kind: "SUBSCRIBED".into(),
                                    reference,
                                })),
                            })
                            .await;
                    }
                    Some(stream_command::Cmd::UnsubscribePrices(usp)) => {
                        let mut w = filter_symbols.write().await;
                        let reference = if let Some(set) = w.as_mut() {
                            for s in usp.symbols {
                                set.remove(&s);
                            }
                            format!("remaining={}", set.len())
                        } else {
                            "no active subscription".to_string()
                        };
                        let _ = out_tx_cmd
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Ack(CommandAck {
                                    kind: "UNSUBSCRIBED".into(),
                                    reference,
                                })),
                            })
                            .await;
                    }
                    Some(stream_command::Cmd::ClearSubscription(_)) => {
                        let mut w = filter_symbols.write().await;
                        *w = None; // Set to None = no subscription
                        let _ = out_tx_cmd
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Ack(CommandAck {
                                    kind: "CLEARED".into(),
                                    reference: "all subscriptions cleared".into(),
                                })),
                            })
                            .await;
                    }
                    Some(stream_command::Cmd::SetAlert(sa)) => {
                        let set = match sa.kind.as_str() {
                            "ABOVE" => Some(quant::core::AlertSet::High(sa.price)),
                            "BELOW" => Some(quant::core::AlertSet::Low(sa.price)),
                            _ => None,
                        };
                        if set.is_none() {
                            let _ = out_tx_cmd
                                .send(StreamEvent {
                                    evt: Some(stream_event::Evt::Ack(CommandAck {
                                        kind: "ERROR".into(),
                                        reference: "invalid kind".into(),
                                    })),
                                })
                                .await;
                            continue;
                        }
                        let alert_id = if sa.alert_id.is_empty() {
                            format!("c{client_id}-{}", nanoid::nanoid!())
                        } else {
                            sa.alert_id.clone()
                        };
                        {
                            let mm = market_manager.lock().await;
                            mm.market_handler()
                                .set_price_alert(
                                    sa.symbol.clone(),
                                    set.unwrap(),
                                    Some(alert_id.clone()),
                                )
                                .await;
                        }
                        state_for_cmd.own_alert(&alert_id, client_id).await;
                        state_for_cmd
                            .set_alert_meta(&alert_id, &sa.symbol, sa.price)
                            .await;
                        let _ = out_tx_cmd
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Ack(CommandAck {
                                    kind: "ALERT_SET".into(),
                                    reference: alert_id,
                                })),
                            })
                            .await;
                    }
                    Some(stream_command::Cmd::RemoveAlert(ra)) => {
                        if let Some(owner) = state_for_cmd.owner_of(&ra.alert_id).await {
                            if owner != client_id {
                                let _ = out_tx_cmd
                                    .send(StreamEvent {
                                        evt: Some(stream_event::Evt::Ack(CommandAck {
                                            kind: "ERROR".into(),
                                            reference: "not owner".into(),
                                        })),
                                    })
                                    .await;
                                continue;
                            }
                        }
                        let removed = {
                            let mm = market_manager.lock().await;
                            mm.market_handler()
                                .remove_price_alert(ra.alert_id.clone())
                                .await
                                .is_some()
                        };
                        if removed {
                            let _ = out_tx_cmd
                                .send(StreamEvent {
                                    evt: Some(stream_event::Evt::Ack(CommandAck {
                                        kind: "ALERT_REMOVED".into(),
                                        reference: ra.alert_id.clone(),
                                    })),
                                })
                                .await;
                            state_for_cmd.release_alert(&ra.alert_id).await;
                        } else {
                            let _ = out_tx_cmd
                                .send(StreamEvent {
                                    evt: Some(stream_event::Evt::Ack(CommandAck {
                                        kind: "ERROR".into(),
                                        reference: "alert not found".into(),
                                    })),
                                })
                                .await;
                        }
                    }
                    Some(stream_command::Cmd::Ping(_)) => {
                        let _ = out_tx_cmd
                            .send(StreamEvent {
                                evt: Some(stream_event::Evt::Ack(CommandAck {
                                    kind: "PONG".into(),
                                    reference: Utc::now().timestamp_millis().to_string(),
                                })),
                            })
                            .await;
                    }
                    None => {}
                }
            }
            state_for_cmd.release_alerts_of(client_id).await;
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx).map(|e| Ok(e));
        Ok(Response::new(Box::pin(out_stream) as Self::StreamStream))
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
struct MarketDataSection {
    host: String,
    port: u16,
}
#[derive(serde::Deserialize, Debug, Clone)]
struct LoggingSection {
    level: Option<String>,
}
#[derive(serde::Deserialize, Debug, Clone)]
struct GrpcConfigRoot {
    ctrader: CTraderFixConfig,
    market_data: MarketDataSection,
    logging: Option<LoggingSection>,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "gRPC Market Data & Alerts Server")]
struct Cli {
    #[arg(short, long, value_name = "CONFIG FILE")]
    config: std::path::PathBuf,
    #[arg(long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = load_config::<GrpcConfigRoot>(&cli.config).expect("config load failed");
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
    quant::utils::setup();

    let market_handler = Arc::new(MarketHandler::new());
    let market_manager = MarketManager::new(cfg.ctrader.clone(), market_handler.clone());
    let mm_arc = Arc::new(tokio::sync::Mutex::new(market_manager));

    let (price_tx, alert_tx) = {
        let mm = mm_arc.lock().await;
        let price_tx = mm.subscribe_price_ticks();
        let alert_tx = mm.subscribe_alerts();
        (price_tx, alert_tx)
    };

    let (grpc_price_tx, _) = broadcast::channel::<PriceTick>(2048);
    let (grpc_alert_tx, _) = broadcast::channel::<AlertResult>(1024);
    let state = Arc::new(GrpcState::new(grpc_price_tx.clone(), grpc_alert_tx.clone()));

    {
        let state_clone = state.clone();
        let grpc_price_sender = grpc_price_tx.clone();
        let grpc_alert_sender = grpc_alert_tx.clone();
        let mut price_rx = price_tx;
        let mut alert_rx = alert_tx;

        tokio::spawn(async move {
            while let Ok(tick) = price_rx.recv().await {
                let _ = grpc_price_sender.send(PriceTick {
                    symbol: tick.symbol,
                    bid: tick.bid,
                    ask: tick.ask,
                    ts_ms: tick.ts_ms,
                });
            }
        });

        tokio::spawn(async move {
            while let Ok(alert_event) = alert_rx.recv().await {
                let (symbol, ref_price) =
                    match state_clone.take_alert_meta(&alert_event.alert_id).await {
                        Some((sym, price)) => (sym, price),
                        None => (String::new(), 0.0),
                    };
                let _ = grpc_alert_sender.send(AlertResult {
                    alert_id: alert_event.alert_id,
                    status: "TRIGGERED".into(),
                    symbol,
                    ref_price,
                    ts_ms: alert_event.ts_ms,
                });
            }
        });
    }

    {
        let mm_clone = mm_arc.clone();
        tokio::spawn(async move {
            let mut mm = mm_clone.lock().await;
            if let Err(e) = mm.run_forever().await {
                log::error!("MarketManager run_forever error: {:?}", e);
            }
        });
    }

    let svc = MarketStreamSvc {
        state: state.clone(),
        market_manager: mm_arc.clone(),
    };
    let addr = format!("{}:{}", cfg.market_data.host, cfg.market_data.port)
        .parse()
        .unwrap();
    log::info!(
        "Starting gRPC MarketStream server on {} (graceful shutdown enabled)",
        addr
    );
    let shutdown = async {
        let _ = signal::ctrl_c().await;
        log::info!("Shutdown signal received - terminating gRPC server");
    };
    Server::builder()
        .add_service(MarketStreamServer::new(svc))
        .serve_with_shutdown(addr, shutdown)
        .await
        .map_err(|e| quant::QuantError::Other(format!("gRPC server error: {e}")))?;
    Ok(())
}
