use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use crate::{
    Result,
    commands::AlertResultCommand,
    core::ctrader_type::CTraderFixConfig,
    market_data::{
        ctrader_market::CTraderMarket,
        market_handler::{MarketHandler, MarketMessage},
    },
};

enum ReconnectSignal {
    Reconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Logon,
    Connected,
}

/// Price tick event that can be broadcast to multiple subscribers
#[derive(Debug, Clone)]
pub struct PriceTickEvent {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub ts_ms: i64,
}

/// Alert triggered event that can be broadcast to multiple subscribers
#[derive(Debug, Clone)]
pub struct AlertTriggeredEvent {
    pub alert_id: String,
    pub ts_ms: i64,
}

pub struct MarketManager {
    config: CTraderFixConfig,
    market_handler: Arc<MarketHandler>,
    ctrader_market: Option<CTraderMarket>,
    reconnect_tx: mpsc::Sender<ReconnectSignal>,
    reconnect_rx: mpsc::Receiver<ReconnectSignal>,
    alert_result_tx: Option<mpsc::UnboundedSender<AlertResultCommand>>,
    // Broadcast channels for external consumers (gRPC, WebSocket, etc.)
    price_broadcast_tx: broadcast::Sender<PriceTickEvent>,
    alert_broadcast_tx: broadcast::Sender<AlertTriggeredEvent>,
    connection_state: Arc<tokio::sync::RwLock<ConnectionState>>,
}

impl MarketManager {
    pub fn new(config: CTraderFixConfig, market_handler: Arc<MarketHandler>) -> Self {
        let (reconnect_tx, reconnect_rx) = mpsc::channel(10);
        let (price_broadcast_tx, _) = broadcast::channel(2048);
        let (alert_broadcast_tx, _) = broadcast::channel(1024);
        let connection_state = Arc::new(tokio::sync::RwLock::new(ConnectionState::Disconnected));

        Self {
            config,
            market_handler,
            ctrader_market: None,
            reconnect_tx,
            reconnect_rx,
            alert_result_tx: None,
            price_broadcast_tx,
            alert_broadcast_tx,
            connection_state,
        }
    }

    pub fn set_alert_result_sender(&mut self, tx: mpsc::UnboundedSender<AlertResultCommand>) {
        self.alert_result_tx = Some(tx);
    }

    pub fn market_handler(&self) -> Arc<MarketHandler> {
        self.market_handler.clone()
    }

    /// Subscribe to price tick broadcasts
    pub fn subscribe_price_ticks(&self) -> broadcast::Receiver<PriceTickEvent> {
        self.price_broadcast_tx.subscribe()
    }

    /// Subscribe to alert triggered broadcasts
    pub fn subscribe_alerts(&self) -> broadcast::Receiver<AlertTriggeredEvent> {
        self.alert_broadcast_tx.subscribe()
    }

    /// Get current connection state
    pub async fn get_connection_state(&self) -> ConnectionState {
        *self.connection_state.read().await
    }

    async fn initialize(&mut self) -> Result<()> {
        *self.connection_state.write().await = ConnectionState::Connecting;
        let mut ctrader_market = CTraderMarket::new(self.config.clone());
        ctrader_market
            .client
            .register_market_handler_arc(self.market_handler.clone());

        log::info!("Initializing CTrader market connection...");
        ctrader_market.initialize(false).await?;

        *self.connection_state.write().await = ConnectionState::Logon;
        log::info!("Successfully connected to CTrader market!");

        // Get symbol mappings and set them in the market handler
        let symbol_map = ctrader_market.get_symbol_str2id().await;
        self.market_handler.set_symbol2id(symbol_map).await;

        self.ctrader_market = Some(ctrader_market);
        *self.connection_state.write().await = ConnectionState::Connected;
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<()> {
        log::info!("Attempting to reconnect...");

        let mut new_ctrader_market = CTraderMarket::new(self.config.clone());
        new_ctrader_market
            .client
            .register_market_handler_arc(self.market_handler.clone());

        // Try to reinitialize the connection
        new_ctrader_market.initialize(false).await?;

        log::info!("Successfully reconnected to CTrader market!");

        // Update symbol mappings
        let symbol_map = new_ctrader_market.get_symbol_str2id().await;
        self.market_handler.set_symbol2id(symbol_map).await;

        // Replace the old market client with the new one
        self.ctrader_market = Some(new_ctrader_market);

        log::info!("Market reconnection completed!");
        Ok(())
    }

    fn start_message_handler(&self) {
        let mut receiver = self.market_handler.alert_receiver();
        let reconnect_tx = self.reconnect_tx.clone();
        let alert_result_tx = self.alert_result_tx.clone();
        let market_handler = self.market_handler.clone();
        let price_broadcast_tx = self.price_broadcast_tx.clone();
        let alert_broadcast_tx = self.alert_broadcast_tx.clone();
        let connection_state = self.connection_state.clone();

        tokio::spawn(async move {
            while let Ok(message) = receiver.recv().await {
                match message {
                    MarketMessage::OnPriceAlert(alert_id) => {
                        log::info!("Price alert triggered! Alert ID: {}", alert_id);

                        // Send to legacy alert result channel if set
                        let alert_result = AlertResultCommand::AlertTriggered {
                            alert_id: crate::core::Id(alert_id.clone()),
                        };
                        if let Some(tx) = &alert_result_tx {
                            let _ = tx.send(alert_result);
                        }

                        // Broadcast to all subscribers
                        let ts_ms = chrono::Utc::now().timestamp_millis();
                        let _ = alert_broadcast_tx.send(AlertTriggeredEvent { alert_id, ts_ms });
                    }
                    MarketMessage::PriceTick {
                        symbol_id,
                        bid,
                        ask,
                    } => {
                        // Broadcast price tick to all subscribers
                        if let Some(symbol) = market_handler.get_symbol_by_id(symbol_id).await {
                            let ts_ms = chrono::Utc::now().timestamp_millis();
                            let _ = price_broadcast_tx.send(PriceTickEvent {
                                symbol,
                                bid,
                                ask,
                                ts_ms,
                            });
                        }
                    }
                    MarketMessage::MarketConnected => {
                        log::info!("Market connected!");
                        *connection_state.write().await = ConnectionState::Connected;
                    }
                    MarketMessage::MarketDisconnected => {
                        log::warn!("Market disconnected! Sending reconnect signal...");
                        *connection_state.write().await = ConnectionState::Disconnected;
                        if let Err(e) = reconnect_tx.send(ReconnectSignal::Reconnect).await {
                            log::error!("Failed to send reconnect signal: {:?}", e);
                        }
                    }
                    MarketMessage::MarketLogon => {
                        log::info!("Market logged on!");
                        *connection_state.write().await = ConnectionState::Logon;
                    }
                    MarketMessage::RejectedSpot(symbol_id, error) => {
                        log::warn!(
                            "Spot subscription rejected for symbol {}: {}",
                            symbol_id,
                            error
                        );
                    }
                }
            }
        });
    }

    async fn run(&mut self) -> Result<()> {
        // Start message handler
        self.start_message_handler();

        // Handle reconnection signals
        while let Some(signal) = self.reconnect_rx.recv().await {
            match signal {
                ReconnectSignal::Reconnect => {
                    if let Err(e) = self.reconnect().await {
                        log::info!("Failed to reconnect: {:?}", e);
                        log::info!("Will retry on next disconnection event...");
                    }
                }
            }
        }

        Ok(())
    }

    /// Initialize the connection and run the market manager forever in the background
    pub async fn run_forever(&mut self) -> Result<()> {
        // Try to initialize the market connection
        if let Err(e) = self.initialize().await {
            log::info!("Failed to initialize market connection: {:?}", e);
            log::info!("Market manager will still run and attempt reconnection when possible");
        }

        // Run the market manager (this will handle reconnections automatically)
        log::info!("Starting market manager with automatic reconnection...");
        self.run().await
    }
}
