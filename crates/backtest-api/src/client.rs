use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use qs_service::TransportFailure;
use thiserror::Error;

use crate::{
    BacktestEvent, BacktestStatusResponse, CancelBacktestResponse, GetBacktestResultResponse,
    PingResponse, SubmitBacktestRequest, SubmitBacktestResponse,
};

pub type BacktestEventStream =
    Pin<Box<dyn Stream<Item = Result<BacktestEvent, BacktestClientError>> + Send>>;

/// Provider-neutral client port for the retained backtest-job workflow.
#[async_trait]
pub trait BacktestClient: Send + Sync {
    async fn ping(&self) -> Result<PingResponse, BacktestClientError>;
    async fn submit(
        &self,
        request: SubmitBacktestRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError>;
    async fn status(&self, job_id: &str) -> Result<BacktestStatusResponse, BacktestClientError>;
    async fn watch(&self, job_id: &str) -> Result<BacktestEventStream, BacktestClientError>;
    async fn result(&self, job_id: &str) -> Result<GetBacktestResultResponse, BacktestClientError>;
    async fn cancel(&self, job_id: &str) -> Result<CancelBacktestResponse, BacktestClientError>;
}

#[derive(Debug, Clone, Error)]
pub enum BacktestClientError {
    #[error("backtest service rejected the request: {0}")]
    Service(String),
    #[error(transparent)]
    Transport(#[from] TransportFailure),
    #[error(transparent)]
    Protocol(#[from] BacktestServiceProtocolError),
}

#[derive(Debug, Clone, Error)]
#[error("backtest service protocol failure: {detail}")]
pub struct BacktestServiceProtocolError {
    pub detail: String,
}
