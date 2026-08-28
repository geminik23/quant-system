use cfix::types::{
    ConnectionHandler, DepthPrice, IncrementalRefresh, MarketDataHandler, SpotPrice,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use std::collections::HashMap;
use tokio::sync::{
    RwLock,
    broadcast::{Receiver, Sender, channel},
};

use crate::core::AlertSet;

use super::{price_alert::PriceAlert, utils::convert_symbol};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ObservedQuote {
    pub bid: f64,
    pub ask: f64,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone)]
pub enum MarketMessage {
    RejectedSpot(u32, String),
    OnPriceAlert(String),
    MarketDisconnected,
    MarketConnected,
    MarketLogon,
    PriceTick {
        symbol_id: u32,
        quote: ObservedQuote,
    },
}

pub struct MarketHandler {
    price_alert: RwLock<PriceAlert>,

    sender: Sender<MarketMessage>,
    recv: Receiver<MarketMessage>,

    symbol_str2id: RwLock<HashMap<String, u32>>,
    last_price: RwLock<HashMap<u32, ObservedQuote>>,

    last_ts: RwLock<DateTime<Utc>>,
}

impl MarketHandler {
    pub fn new() -> Self {
        let (tx, rx) = channel(4096);
        Self {
            sender: tx,
            recv: rx,
            price_alert: RwLock::new(PriceAlert::new()),
            symbol_str2id: RwLock::new(HashMap::new()),
            last_price: RwLock::new(HashMap::new()),
            last_ts: RwLock::new(Utc::now()),
        }
    }

    pub async fn get_last_active_time(&self) -> NaiveDateTime {
        self.last_ts.read().await.naive_utc()
    }
    pub async fn remove_price_alert(&self, alert_id: String) -> Option<AlertSet> {
        self.price_alert.write().await.remove(alert_id)
    }
    pub async fn modify_price(&self, alert_id: String, price: f64) -> Option<AlertSet> {
        self.price_alert.write().await.modify_price(alert_id, price)
    }

    pub async fn set_symbol2id(&self, data: HashMap<String, u32>) {
        let mut symbol_str2id_lock = self.symbol_str2id.write().await;
        symbol_str2id_lock.clear();
        symbol_str2id_lock.extend(data);
    }

    pub async fn get_symbol_id(&self, symbol: &str) -> Option<u32> {
        let symbol = convert_symbol(symbol);
        self.symbol_str2id.read().await.get(&symbol).copied()
    }

    pub async fn get_symbol_by_id(&self, symbol_id: u32) -> Option<String> {
        // Linear scan; symbol count expected small. Optimize with reverse map if needed.
        for (k, v) in self.symbol_str2id.read().await.iter() {
            if *v == symbol_id {
                return Some(k.clone());
            }
        }
        None
    }

    pub async fn get_price_of(&self, symbol: &str) -> Option<f64> {
        tracing::info!("{:?}", self.symbol_str2id.read().await);
        match self.get_symbol_id(symbol).await {
            Some(symbol_id) => self.price_alert.read().await.get_price(symbol_id),
            None => None,
        }
    }

    pub async fn get_observed_quote(&self, symbol: &str) -> Option<ObservedQuote> {
        match self.get_symbol_id(symbol).await {
            Some(id) => self.last_price.read().await.get(&id).copied(),
            None => None,
        }
    }

    pub async fn clear_observed_quotes(&self) {
        self.last_price.write().await.clear();
    }

    pub async fn get_all_symbols(&self) -> Vec<String> {
        self.symbol_str2id.read().await.keys().cloned().collect()
    }

    pub async fn set_price_alert_id(
        &self,
        symbol_id: u32,
        set: AlertSet,
        alert_id: Option<String>,
    ) -> String {
        self.price_alert
            .write()
            .await
            .set_alert(symbol_id, set, alert_id)
    }
    pub async fn set_price_alert(
        &self,
        symbol: String,
        set: AlertSet,
        alert_id: Option<String>,
    ) -> Option<String> {
        match self.get_symbol_id(symbol.as_str()).await {
            Some(sym_id) => Some(self.set_price_alert_id(sym_id, set, alert_id).await),
            None => {
                tracing::error!("Cannot find the symbol id - {symbol:?}");
                None
            }
        }
    }

    pub fn alert_receiver(&self) -> Receiver<MarketMessage> {
        self.recv.resubscribe()
    }

    async fn observe_price_at(&self, symbol_id: u32, price: SpotPrice, observed_at: DateTime<Utc>) {
        *self.last_ts.write().await = observed_at;
        let quote = ObservedQuote {
            bid: price.bid,
            ask: price.ask,
            observed_at_ms: observed_at.timestamp_millis(),
        };

        {
            let mut last_price = self.last_price.write().await;
            last_price.insert(symbol_id, quote);
        }

        self.sender
            .send(MarketMessage::PriceTick { symbol_id, quote })
            .ok();

        if let Some(result) = self
            .price_alert
            .write()
            .await
            .on_price(symbol_id, (price.bid, price.ask))
        {
            for id in result {
                self.sender.send(MarketMessage::OnPriceAlert(id)).ok();
            }
        }
    }
}

impl Default for MarketHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ConnectionHandler for MarketHandler {
    async fn on_connect(&self) {
        self.sender.send(MarketMessage::MarketConnected).ok();
        // self.notify_message(PAMessage::Connected).await;
    }

    async fn on_logon(&self) {
        self.sender.send(MarketMessage::MarketLogon).ok();
        // self.notify_message(PAMessage::LoggedOn).await;
    }

    async fn on_disconnect(&self) {
        self.sender.send(MarketMessage::MarketDisconnected).ok();
        // self.notify_message(PAMessage::Disconnected).await;
    }
}

#[async_trait::async_trait]
impl MarketDataHandler for MarketHandler {
    async fn on_price_of(&self, symbol_id: u32, price: SpotPrice) {
        self.observe_price_at(symbol_id, price, Utc::now()).await;
    }

    async fn on_rejected_spot_subscription(&self, symbol_id: u32, err_msg: String) {
        self.sender
            .send(MarketMessage::RejectedSpot(symbol_id, err_msg))
            .ok();
    }
    async fn on_accpeted_spot_subscription(&self, _symbol_id: u32) {}

    async fn on_market_depth_full_refresh(
        &self,
        _symbol_id: u32,
        _full_depth: HashMap<String, DepthPrice>,
    ) {
    }
    async fn on_market_depth_incremental_refresh(&self, _refresh: Vec<IncrementalRefresh>) {}
    async fn on_accpeted_depth_subscription(&self, _symbol_id: u32) {}
    async fn on_rejected_depth_subscription(&self, _symbol_id: u32, _err_msg: String) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn quote_is_cached_before_matching_tick_is_published() {
        let handler = MarketHandler::new();
        let mut receiver = handler.alert_receiver();
        let observed_at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        handler
            .set_symbol2id(HashMap::from([("eurusd".to_string(), 7)]))
            .await;

        handler
            .observe_price_at(
                7,
                SpotPrice {
                    bid: 1.1234,
                    ask: 1.1236,
                },
                observed_at,
            )
            .await;

        let MarketMessage::PriceTick { symbol_id, quote } = receiver.recv().await.unwrap() else {
            panic!("expected price tick");
        };
        assert_eq!(symbol_id, 7);
        assert_eq!(quote.observed_at_ms, 1_700_000_000_123);
        assert_eq!(handler.get_observed_quote("EURUSD").await, Some(quote));
    }

    #[tokio::test]
    async fn clearing_observed_quotes_invalidates_cached_prices() {
        let handler = MarketHandler::new();
        let observed_at = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        handler
            .set_symbol2id(HashMap::from([("eurusd".to_string(), 7)]))
            .await;
        handler
            .observe_price_at(
                7,
                SpotPrice {
                    bid: 1.1234,
                    ask: 1.1236,
                },
                observed_at,
            )
            .await;

        handler.clear_observed_quotes().await;

        assert_eq!(handler.get_observed_quote("EURUSD").await, None);
    }
}
