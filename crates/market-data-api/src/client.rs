use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use qs_service::TransportFailure;
use thiserror::Error;

use crate::{
    AlertResult, CommandAck, GetAlertsResponse, GetPriceRequest, GetPriceResponse,
    GetPricesRequest, GetPricesResponse, GetStateResponse, RemoveAlertRequest, SetAlertRequest,
    StreamEvent, SubscribePricesRequest, UnsubscribePricesRequest,
};

pub type PriceEventStream =
    Pin<Box<dyn Stream<Item = Result<StreamEvent, MarketDataClientError>> + Send>>;
pub type AlertEventStream =
    Pin<Box<dyn Stream<Item = Result<AlertResult, MarketDataClientError>> + Send>>;

/// Provider-neutral client port for snapshots, subscriptions, alerts, and events.
#[async_trait]
pub trait MarketDataClient: Send + Sync {
    async fn get_price(
        &self,
        request: GetPriceRequest,
    ) -> Result<GetPriceResponse, MarketDataClientError>;
    async fn get_prices(
        &self,
        request: GetPricesRequest,
    ) -> Result<GetPricesResponse, MarketDataClientError>;
    async fn state(&self) -> Result<GetStateResponse, MarketDataClientError>;
    async fn subscribe(
        &self,
        request: SubscribePricesRequest,
    ) -> Result<CommandAck, MarketDataClientError>;
    async fn unsubscribe(
        &self,
        request: UnsubscribePricesRequest,
    ) -> Result<CommandAck, MarketDataClientError>;
    async fn clear_subscription(&self) -> Result<CommandAck, MarketDataClientError>;
    async fn set_alert(
        &self,
        request: SetAlertRequest,
    ) -> Result<CommandAck, MarketDataClientError>;
    async fn remove_alert(
        &self,
        request: RemoveAlertRequest,
    ) -> Result<CommandAck, MarketDataClientError>;
    async fn alerts(&self) -> Result<GetAlertsResponse, MarketDataClientError>;
    async fn events(&self) -> Result<PriceEventStream, MarketDataClientError>;
    async fn alert_events(&self) -> Result<AlertEventStream, MarketDataClientError>;
}

#[derive(Debug, Clone, Error)]
pub enum MarketDataClientError {
    #[error("market-data service rejected the request: {0}")]
    Service(String),
    #[error(transparent)]
    Transport(#[from] TransportFailure),
    #[error(transparent)]
    Protocol(#[from] MarketDataServiceProtocolError),
}

#[derive(Debug, Clone, Error)]
#[error("market-data service protocol failure: {detail}")]
pub struct MarketDataServiceProtocolError {
    pub detail: String,
}
