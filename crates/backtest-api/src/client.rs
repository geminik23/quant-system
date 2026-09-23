use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use qs_service::TransportFailure;
use thiserror::Error;

use crate::{
    AddProfileRequest, AddProfileResponse, BacktestEvent, BacktestStatusResponse,
    CancelBacktestResponse, DeleteResultArtifactRequest, DeleteResultArtifactResponse,
    GetBacktestResultResponse, GetResultArtifactChunkRequest, GetResultArtifactChunkResponse,
    GetSearchResultResponse, ListProfilesResponse, ListSymbolsRequest, ListSymbolsResponse,
    PingResponse, ReloadProfilesResponse, RemoveProfileRequest, RemoveProfileResponse,
    RunBacktestMultiRequest, RunBacktestMultiResponse, RunBacktestRequest, RunBacktestResponse,
    RunConfiguredStrategyRequest, SubmitBacktestRequest, SubmitBacktestResponse,
    SubmitConfiguredStrategyRequest, SubmitSearchRequest,
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
    async fn get_result_artifact_chunk(
        &self,
        request: GetResultArtifactChunkRequest,
    ) -> Result<GetResultArtifactChunkResponse, BacktestClientError>;
    async fn delete_result_artifact(
        &self,
        request: DeleteResultArtifactRequest,
    ) -> Result<DeleteResultArtifactResponse, BacktestClientError>;
    async fn cancel(&self, job_id: &str) -> Result<CancelBacktestResponse, BacktestClientError>;
}

/// Provider-neutral client port for finite synchronous backtest execution.
#[async_trait]
pub trait BacktestSyncClient: Send + Sync {
    async fn run_backtest(
        &self,
        request: RunBacktestRequest,
    ) -> Result<RunBacktestResponse, BacktestClientError>;
    async fn run_backtest_multi(
        &self,
        request: RunBacktestMultiRequest,
    ) -> Result<RunBacktestMultiResponse, BacktestClientError>;
}

/// Provider-neutral client port for profile and market-data discovery.
#[async_trait]
pub trait BacktestDiscoveryClient: Send + Sync {
    async fn list_profiles(&self) -> Result<ListProfilesResponse, BacktestClientError>;
    async fn list_symbols(
        &self,
        request: ListSymbolsRequest,
    ) -> Result<ListSymbolsResponse, BacktestClientError>;
}

/// Provider-neutral client port for configured strategy runs and server-side parameter searches.
///
/// Submitted jobs share the retained-job workflow: status, watch, cancel, results, and artifacts go through [`BacktestClient`] with the returned job ID.
#[async_trait]
pub trait BacktestStrategyClient: Send + Sync {
    async fn run_configured_strategy(
        &self,
        request: RunConfiguredStrategyRequest,
    ) -> Result<RunBacktestResponse, BacktestClientError>;
    async fn submit_configured_strategy(
        &self,
        request: SubmitConfiguredStrategyRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError>;
    async fn submit_search(
        &self,
        request: SubmitSearchRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError>;
    async fn search_result(
        &self,
        job_id: &str,
    ) -> Result<GetSearchResultResponse, BacktestClientError>;
}

/// Provider-neutral client port for runtime profile administration.
#[async_trait]
pub trait BacktestAdminClient: Send + Sync {
    async fn add_profile(
        &self,
        request: AddProfileRequest,
    ) -> Result<AddProfileResponse, BacktestClientError>;
    async fn remove_profile(
        &self,
        request: RemoveProfileRequest,
    ) -> Result<RemoveProfileResponse, BacktestClientError>;
    async fn reload_profiles(&self) -> Result<ReloadProfilesResponse, BacktestClientError>;
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
