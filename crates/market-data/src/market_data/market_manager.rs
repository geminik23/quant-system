use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::{
    Result,
    commands::AlertResultCommand,
    core::ctrader_type::CTraderFixConfig,
    market_data::{
        ctrader_market::CTraderMarket,
        market_handler::{MarketHandler, MarketMessage},
    },
    rpc_types::DataQualityEvent,
};

enum ReconnectSignal {
    Reconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Logon,
    Connected,
}

impl ConnectionState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "CONNECTED",
            Self::Disconnected => "DISCONNECTED",
            Self::Connecting => "CONNECTING",
            Self::Logon => "LOGON",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceStateSnapshot {
    pub state: ConnectionState,
    pub changed_at_ms: i64,
}

/// Price tick event that can be broadcast to multiple subscribers
#[derive(Debug, Clone, PartialEq)]
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

/// Shared handles extracted from MarketManager for lock-free access by client handlers.
#[derive(Clone)]
pub struct MarketManagerHandles {
    pub market_handler: Arc<MarketHandler>,
    pub price_broadcast_tx: broadcast::Sender<PriceTickEvent>,
    pub alert_broadcast_tx: broadcast::Sender<AlertTriggeredEvent>,
    pub state_broadcast_tx: broadcast::Sender<SourceStateSnapshot>,
    pub quality_broadcast_tx: broadcast::Sender<DataQualityEvent>,
    pub source_state: Arc<tokio::sync::RwLock<SourceStateSnapshot>>,
}

impl MarketManagerHandles {
    pub fn subscribe_price_ticks(&self) -> broadcast::Receiver<PriceTickEvent> {
        self.price_broadcast_tx.subscribe()
    }

    pub fn subscribe_alerts(&self) -> broadcast::Receiver<AlertTriggeredEvent> {
        self.alert_broadcast_tx.subscribe()
    }

    pub fn subscribe_state_changes(&self) -> broadcast::Receiver<SourceStateSnapshot> {
        self.state_broadcast_tx.subscribe()
    }

    pub fn subscribe_quality_events(&self) -> broadcast::Receiver<DataQualityEvent> {
        self.quality_broadcast_tx.subscribe()
    }

    pub async fn get_source_state(&self) -> SourceStateSnapshot {
        *self.source_state.read().await
    }
}

async fn commit_source_state_at(
    market_handler: &MarketHandler,
    source_state: &tokio::sync::RwLock<SourceStateSnapshot>,
    state_broadcast_tx: &broadcast::Sender<SourceStateSnapshot>,
    next_state: ConnectionState,
    changed_at_ms: i64,
) -> SourceStateSnapshot {
    let mut current = source_state.write().await;
    if matches!(
        next_state,
        ConnectionState::Connecting | ConnectionState::Disconnected
    ) {
        market_handler.clear_observed_quotes().await;
    }
    if current.state == next_state {
        return *current;
    }

    let snapshot = SourceStateSnapshot {
        state: next_state,
        changed_at_ms,
    };
    *current = snapshot;
    drop(current);
    let _ = state_broadcast_tx.send(snapshot);
    snapshot
}

fn data_quality_event(reason: impl Into<String>, dropped: Option<u64>) -> DataQualityEvent {
    DataQualityEvent::new(reason, dropped, chrono::Utc::now().timestamp_millis())
}

pub struct MarketManager {
    config: CTraderFixConfig,
    market_handler: Arc<MarketHandler>,
    ctrader_market: Option<CTraderMarket>,
    reconnect_tx: mpsc::Sender<ReconnectSignal>,
    reconnect_rx: mpsc::Receiver<ReconnectSignal>,
    alert_result_tx: Option<mpsc::UnboundedSender<AlertResultCommand>>,
    // Broadcast channels for external consumers
    price_broadcast_tx: broadcast::Sender<PriceTickEvent>,
    alert_broadcast_tx: broadcast::Sender<AlertTriggeredEvent>,
    state_broadcast_tx: broadcast::Sender<SourceStateSnapshot>,
    quality_broadcast_tx: broadcast::Sender<DataQualityEvent>,
    source_state: Arc<tokio::sync::RwLock<SourceStateSnapshot>>,
}

impl MarketManager {
    pub fn new(config: CTraderFixConfig, market_handler: Arc<MarketHandler>) -> Self {
        let (reconnect_tx, reconnect_rx) = mpsc::channel(10);
        let (price_broadcast_tx, _) = broadcast::channel(2048);
        let (alert_broadcast_tx, _) = broadcast::channel(1024);
        let (state_broadcast_tx, _) = broadcast::channel(64);
        let (quality_broadcast_tx, _) = broadcast::channel(256);
        let source_state = Arc::new(tokio::sync::RwLock::new(SourceStateSnapshot {
            state: ConnectionState::Disconnected,
            changed_at_ms: chrono::Utc::now().timestamp_millis(),
        }));

        Self {
            config,
            market_handler,
            ctrader_market: None,
            reconnect_tx,
            reconnect_rx,
            alert_result_tx: None,
            price_broadcast_tx,
            alert_broadcast_tx,
            state_broadcast_tx,
            quality_broadcast_tx,
            source_state,
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

    /// Subscribe to connection state change broadcasts
    pub fn subscribe_state_changes(&self) -> broadcast::Receiver<SourceStateSnapshot> {
        self.state_broadcast_tx.subscribe()
    }

    /// Get current connection state
    pub async fn get_source_state(&self) -> SourceStateSnapshot {
        *self.source_state.read().await
    }

    /// Get the shared handles that client handlers need - no mutex required.
    /// Call this *before* `run_forever()` so clients don't need to lock the manager.
    pub fn shared_handles(&self) -> MarketManagerHandles {
        MarketManagerHandles {
            market_handler: self.market_handler.clone(),
            price_broadcast_tx: self.price_broadcast_tx.clone(),
            alert_broadcast_tx: self.alert_broadcast_tx.clone(),
            state_broadcast_tx: self.state_broadcast_tx.clone(),
            quality_broadcast_tx: self.quality_broadcast_tx.clone(),
            source_state: self.source_state.clone(),
        }
    }

    async fn transition_source_state(&self, next_state: ConnectionState) -> SourceStateSnapshot {
        commit_source_state_at(
            &self.market_handler,
            &self.source_state,
            &self.state_broadcast_tx,
            next_state,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
    }

    /// First-time connection: create a fresh CTraderMarket, connect, fetch symbols, subscribe.
    async fn initialize(&mut self) -> Result<()> {
        self.transition_source_state(ConnectionState::Connecting)
            .await;

        let mut ctrader_market = CTraderMarket::new(self.config.clone());
        ctrader_market
            .client
            .register_market_handler_arc(self.market_handler.clone());
        ctrader_market
            .client
            .register_connection_handler_arc(self.market_handler.clone());

        tracing::info!("Initializing CTrader market connection...");
        ctrader_market.initialize(false).await?;

        self.transition_source_state(ConnectionState::Logon).await;
        tracing::info!("Successfully connected to CTrader market!");

        // Get symbol mappings and set them in the market handler
        let symbol_map = ctrader_market.get_symbol_str2id().await;
        self.market_handler.set_symbol2id(symbol_map).await;

        self.ctrader_market = Some(ctrader_market);
        self.transition_source_state(ConnectionState::Connected)
            .await;
        Ok(())
    }

    /// Full re-initialization: drop the old connection entirely, create a fresh one.
    /// Based on real CTrader behavior where reconnecting on the same instance
    /// sometimes doesn't work - a clean re-init is more reliable.
    async fn reinitialize(&mut self) -> Result<()> {
        self.transition_source_state(ConnectionState::Connecting)
            .await;

        // Drop the old CTrader connection entirely
        if let Some(mut old) = self.ctrader_market.take() {
            let _ = old.client.disconnect().await;
            tracing::info!("Dropped old CTrader connection");
        }

        // Create a completely fresh instance
        let mut ctrader_market = CTraderMarket::new(self.config.clone());
        ctrader_market
            .client
            .register_market_handler_arc(self.market_handler.clone());
        ctrader_market
            .client
            .register_connection_handler_arc(self.market_handler.clone());

        tracing::info!("Re-initializing CTrader market connection...");
        ctrader_market.initialize(false).await?;

        // Update symbol mappings
        let symbol_map = ctrader_market.get_symbol_str2id().await;
        self.market_handler.set_symbol2id(symbol_map).await;

        self.ctrader_market = Some(ctrader_market);
        self.transition_source_state(ConnectionState::Connected)
            .await;

        tracing::info!("CTrader re-initialization completed");
        Ok(())
    }

    /// Retry loop with exponential backoff. Calls `reinitialize()` until it succeeds
    /// or max attempts are exhausted. Used for both initial connection failure and
    /// mid-session disconnects.
    async fn retry_connect(&mut self) {
        let base_delay = self.config.retry_base_delay_secs.unwrap_or(2);
        let max_delay = self.config.retry_max_delay_secs.unwrap_or(60);
        let max_attempts = self.config.retry_max_attempts.unwrap_or(0); // 0 = infinite

        let mut attempt: u32 = 0;

        loop {
            attempt += 1;

            if max_attempts > 0 && attempt > max_attempts {
                tracing::error!(
                    "Exceeded max reconnection attempts ({}). Giving up.",
                    max_attempts
                );
                self.transition_source_state(ConnectionState::Disconnected)
                    .await;
                return;
            }

            // Exponential backoff: base * 2^(attempt-1), capped at max
            let delay_secs =
                (base_delay * 2u64.saturating_pow(attempt.saturating_sub(1))).min(max_delay);

            tracing::info!(
                "Reconnection attempt {}{} in {}s...",
                attempt,
                if max_attempts > 0 {
                    format!("/{max_attempts}")
                } else {
                    String::new()
                },
                delay_secs
            );

            tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;

            match self.reinitialize().await {
                Ok(()) => {
                    tracing::info!("Reconnection succeeded on attempt {}", attempt);
                    return;
                }
                Err(e) => {
                    tracing::warn!("Reconnection attempt {} failed: {:?}", attempt, e);
                    self.transition_source_state(ConnectionState::Disconnected)
                        .await;
                    // Loop continues to next attempt
                }
            }
        }
    }

    fn start_message_handler(&self) -> JoinHandle<()> {
        let mut receiver = self.market_handler.alert_receiver();
        let reconnect_tx = self.reconnect_tx.clone();
        let alert_result_tx = self.alert_result_tx.clone();
        let market_handler = self.market_handler.clone();
        let price_broadcast_tx = self.price_broadcast_tx.clone();
        let alert_broadcast_tx = self.alert_broadcast_tx.clone();
        let state_broadcast_tx = self.state_broadcast_tx.clone();
        let quality_broadcast_tx = self.quality_broadcast_tx.clone();
        let source_state = self.source_state.clone();

        tokio::spawn(async move {
            loop {
                let message = match receiver.recv().await {
                    Ok(message) => message,
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        tracing::warn!(dropped, "internal market message receiver lagged");
                        let _ = quality_broadcast_tx.send(data_quality_event(
                            "internal market message receiver lagged",
                            Some(dropped),
                        ));
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };

                match message {
                    MarketMessage::OnPriceAlert(alert_id) => {
                        tracing::info!("Price alert triggered! Alert ID: {}", alert_id);

                        let alert_result = AlertResultCommand::AlertTriggered {
                            alert_id: crate::core::Id(alert_id.clone()),
                        };
                        if let Some(tx) = &alert_result_tx {
                            let _ = tx.send(alert_result);
                        }

                        let ts_ms = chrono::Utc::now().timestamp_millis();
                        let _ = alert_broadcast_tx.send(AlertTriggeredEvent { alert_id, ts_ms });
                    }
                    MarketMessage::PriceTick { symbol_id, quote } => {
                        if let Some(symbol) = market_handler.get_symbol_by_id(symbol_id).await {
                            let _ = price_broadcast_tx.send(PriceTickEvent {
                                symbol,
                                bid: quote.bid,
                                ask: quote.ask,
                                ts_ms: quote.observed_at_ms,
                            });
                        }
                    }
                    MarketMessage::MarketConnected => {
                        tracing::info!("CTrader transport connected");
                    }
                    MarketMessage::MarketDisconnected => {
                        let current = *source_state.read().await;
                        if current.state == ConnectionState::Connecting {
                            market_handler.clear_observed_quotes().await;
                            tracing::info!(
                                "Ignoring disconnect from connection replaced during reinitialization"
                            );
                            continue;
                        }

                        tracing::warn!("Market disconnected! Sending reconnect signal...");
                        commit_source_state_at(
                            &market_handler,
                            &source_state,
                            &state_broadcast_tx,
                            ConnectionState::Disconnected,
                            chrono::Utc::now().timestamp_millis(),
                        )
                        .await;
                        if let Err(e) = reconnect_tx.send(ReconnectSignal::Reconnect).await {
                            tracing::error!("Failed to send reconnect signal: {:?}", e);
                        }
                    }
                    MarketMessage::MarketLogon => {
                        let current = *source_state.read().await;
                        if current.state == ConnectionState::Connecting {
                            tracing::info!("CTrader FIX logon completed");
                            commit_source_state_at(
                                &market_handler,
                                &source_state,
                                &state_broadcast_tx,
                                ConnectionState::Logon,
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .await;
                        } else {
                            tracing::debug!(
                                state = current.state.as_str(),
                                "Ignoring late CTrader logon callback"
                            );
                        }
                    }
                    MarketMessage::RejectedSpot(symbol_id, error) => {
                        let reason =
                            format!("spot subscription rejected for symbol {symbol_id}: {error}");
                        tracing::warn!("{reason}");
                        let _ = quality_broadcast_tx.send(data_quality_event(reason, None));
                    }
                }
            }
        })
    }

    /// Initialize the connection and run the market manager forever in the background.
    /// Handles both initial connection failure and mid-session disconnects with
    /// exponential-backoff retry. Always does a full re-initialization (fresh
    /// CTraderMarket instance) rather than attempting to reuse old connections.
    pub async fn run_forever(&mut self) -> Result<()> {
        // Start the message handler (processes MarketMessage → broadcast events)
        let msg_handler = self.start_message_handler();

        // Initial connection attempt with retry on failure
        if let Err(e) = self.initialize().await {
            tracing::warn!("Initial connection failed: {:?}. Starting retry loop...", e);
            self.transition_source_state(ConnectionState::Disconnected)
                .await;
            self.retry_connect().await;
        }

        // Main loop: wait for disconnect signals, then retry
        tracing::info!("Market manager running with automatic reconnection");
        while let Some(signal) = self.reconnect_rx.recv().await {
            match signal {
                ReconnectSignal::Reconnect => {
                    // Drain any duplicate signals that arrived while we were processing
                    while self.reconnect_rx.try_recv().is_ok() {}

                    tracing::warn!("Disconnect detected. Starting reconnection retry...");
                    self.retry_connect().await;
                }
            }
        }

        // Abort the message handler task so it doesn't leak on shutdown.
        // This task holds Arc clones of MarketHandler (which owns the broadcast
        // Sender), so it would block forever on recv() if left running.
        msg_handler.abort();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cfix::types::{ConnectionHandler, MarketDataHandler, SpotPrice};
    use std::collections::HashMap;

    fn test_config() -> CTraderFixConfig {
        CTraderFixConfig {
            username: "user".into(),
            password: "password".into(),
            server: "localhost:0".into(),
            sendercompid: "sender".into(),
            ssl: false,
            retry_max_attempts: Some(1),
            retry_base_delay_secs: Some(0),
            retry_max_delay_secs: Some(0),
        }
    }

    #[tokio::test]
    async fn source_transition_timestamp_is_broadcast_and_cache_is_invalidated() {
        let handler = Arc::new(MarketHandler::new());
        handler
            .set_symbol2id(HashMap::from([("eurusd".to_string(), 7)]))
            .await;
        handler
            .on_price_of(
                7,
                SpotPrice {
                    bid: 1.1234,
                    ask: 1.1236,
                },
            )
            .await;
        assert!(handler.get_observed_quote("EURUSD").await.is_some());

        let source_state = tokio::sync::RwLock::new(SourceStateSnapshot {
            state: ConnectionState::Connected,
            changed_at_ms: 100,
        });
        let (state_tx, mut state_rx) = broadcast::channel(4);
        let snapshot = commit_source_state_at(
            &handler,
            &source_state,
            &state_tx,
            ConnectionState::Connecting,
            1_700_000_000_456,
        )
        .await;

        assert_eq!(snapshot.changed_at_ms, 1_700_000_000_456);
        assert_eq!(*source_state.read().await, snapshot);
        assert_eq!(state_rx.recv().await.unwrap(), snapshot);
        assert!(handler.get_observed_quote("EURUSD").await.is_none());

        handler
            .on_price_of(
                7,
                SpotPrice {
                    bid: 1.1734,
                    ask: 1.1736,
                },
            )
            .await;
        let unchanged = commit_source_state_at(
            &handler,
            &source_state,
            &state_tx,
            ConnectionState::Connecting,
            1_700_000_000_500,
        )
        .await;
        assert_eq!(unchanged, snapshot);
        assert!(handler.get_observed_quote("EURUSD").await.is_none());

        handler
            .on_price_of(
                7,
                SpotPrice {
                    bid: 1.2234,
                    ask: 1.2236,
                },
            )
            .await;
        assert!(handler.get_observed_quote("EURUSD").await.is_some());
        let disconnected = commit_source_state_at(
            &handler,
            &source_state,
            &state_tx,
            ConnectionState::Disconnected,
            1_700_000_000_789,
        )
        .await;

        assert_eq!(disconnected.changed_at_ms, 1_700_000_000_789);
        assert_eq!(state_rx.recv().await.unwrap(), disconnected);
        assert!(handler.get_observed_quote("EURUSD").await.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn internal_lag_reports_quality_and_processing_continues() {
        let handler = Arc::new(MarketHandler::new());
        let manager = MarketManager::new(test_config(), handler.clone());
        let mut quality_rx = manager.quality_broadcast_tx.subscribe();
        let message_task = manager.start_message_handler();

        for _ in 0..4_097 {
            handler.on_connect().await;
        }

        let quality = quality_rx.recv().await.unwrap();
        assert_eq!(quality.reason, "internal market message receiver lagged");
        assert_eq!(quality.dropped, Some(1));

        handler
            .on_rejected_spot_subscription(7, "not available".into())
            .await;
        let quality = quality_rx.recv().await.unwrap();
        assert_eq!(
            quality.reason,
            "spot subscription rejected for symbol 7: not available"
        );
        assert_eq!(quality.dropped, None);

        message_task.abort();
    }

    #[tokio::test]
    async fn late_connection_callbacks_do_not_regress_connected_state() {
        let handler = Arc::new(MarketHandler::new());
        let manager = MarketManager::new(test_config(), handler.clone());
        manager
            .transition_source_state(ConnectionState::Connected)
            .await;
        let message_task = manager.start_message_handler();

        handler.on_connect().await;
        handler.on_logon().await;
        tokio::task::yield_now().await;

        assert_eq!(
            manager.get_source_state().await.state,
            ConnectionState::Connected
        );

        message_task.abort();
    }

    #[tokio::test]
    async fn deliberate_disconnect_while_connecting_does_not_queue_reconnect() {
        let handler = Arc::new(MarketHandler::new());
        let mut manager = MarketManager::new(test_config(), handler.clone());
        let message_task = manager.start_message_handler();
        manager
            .transition_source_state(ConnectionState::Connecting)
            .await;

        handler.on_disconnect().await;
        tokio::task::yield_now().await;

        assert_eq!(
            manager.get_source_state().await.state,
            ConnectionState::Connecting
        );
        assert!(manager.reconnect_rx.try_recv().is_err());

        message_task.abort();
    }
}
