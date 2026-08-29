use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use qs_backtest_api::{
    BacktestClient, BacktestClientError, BacktestDiscoveryClient, BacktestEventStream,
    BacktestStatusResponse, CancelBacktestResponse, DeleteResultArtifactRequest,
    DeleteResultArtifactResponse, GetBacktestResultResponse, GetResultArtifactChunkRequest,
    GetResultArtifactChunkResponse, ListProfilesResponse, ListSymbolsRequest, ListSymbolsResponse,
    PingResponse, SubmitBacktestRequest, SubmitBacktestResponse,
};

use crate::BacktestCatalogConnector;

/// Calls recorded by the deterministic catalog connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedCall {
    Connect,
    Ping,
    Profiles,
    Symbols,
    Close,
    Submit,
    Status,
    Watch,
    Result,
    ArtifactChunk,
    DeleteArtifact,
    Cancel,
}

#[derive(Clone)]
pub struct ScriptedCatalogConnector {
    endpoint_display: String,
    client: ScriptedCatalogClient,
    connect_error: Option<BacktestClientError>,
}

impl ScriptedCatalogConnector {
    pub fn success(
        endpoint_display: impl Into<String>,
        ping: PingResponse,
        profiles: ListProfilesResponse,
        symbols: ListSymbolsResponse,
    ) -> Self {
        Self {
            endpoint_display: endpoint_display.into(),
            client: ScriptedCatalogClient {
                calls: Arc::new(Mutex::new(Vec::new())),
                ping: Ok(ping),
                profiles: Ok(profiles),
                symbols: Ok(symbols),
            },
            connect_error: None,
        }
    }

    pub fn failing_connect(error: BacktestClientError) -> Self {
        Self {
            endpoint_display: "tcp://127.0.0.1:41001".into(),
            client: ScriptedCatalogClient {
                calls: Arc::new(Mutex::new(Vec::new())),
                ping: Err(unsupported("ping")),
                profiles: Err(unsupported("profiles")),
                symbols: Err(unsupported("symbols")),
            },
            connect_error: Some(error),
        }
    }

    pub fn calls(&self) -> Vec<ScriptedCall> {
        self.client.calls()
    }

    pub fn client_mut(&mut self) -> &mut ScriptedCatalogClient {
        &mut self.client
    }
}

#[async_trait]
impl BacktestCatalogConnector for ScriptedCatalogConnector {
    type Client = ScriptedCatalogClient;

    fn endpoint_display(&self) -> String {
        self.endpoint_display.clone()
    }

    async fn connect(&self) -> Result<Self::Client, BacktestClientError> {
        self.client.record(ScriptedCall::Connect);
        match &self.connect_error {
            Some(error) => Err(error.clone()),
            None => Ok(self.client.clone()),
        }
    }

    async fn close(&self, client: Self::Client) -> Result<(), BacktestClientError> {
        client.record(ScriptedCall::Close);
        Ok(())
    }
}

#[derive(Clone)]
pub struct ScriptedCatalogClient {
    calls: Arc<Mutex<Vec<ScriptedCall>>>,
    ping: Result<PingResponse, BacktestClientError>,
    profiles: Result<ListProfilesResponse, BacktestClientError>,
    symbols: Result<ListSymbolsResponse, BacktestClientError>,
}

impl ScriptedCatalogClient {
    pub fn fail_ping(&mut self, error: BacktestClientError) {
        self.ping = Err(error);
    }

    pub fn fail_profiles(&mut self, error: BacktestClientError) {
        self.profiles = Err(error);
    }

    pub fn fail_symbols(&mut self, error: BacktestClientError) {
        self.symbols = Err(error);
    }

    pub fn calls(&self) -> Vec<ScriptedCall> {
        self.calls
            .lock()
            .expect("call log lock is not poisoned")
            .clone()
    }

    fn record(&self, call: ScriptedCall) {
        self.calls
            .lock()
            .expect("call log lock is not poisoned")
            .push(call);
    }
}

#[async_trait]
impl BacktestClient for ScriptedCatalogClient {
    async fn ping(&self) -> Result<PingResponse, BacktestClientError> {
        self.record(ScriptedCall::Ping);
        self.ping.clone()
    }

    async fn submit(
        &self,
        _request: SubmitBacktestRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError> {
        self.record(ScriptedCall::Submit);
        Err(unsupported("submit"))
    }

    async fn status(&self, _job_id: &str) -> Result<BacktestStatusResponse, BacktestClientError> {
        self.record(ScriptedCall::Status);
        Err(unsupported("status"))
    }

    async fn watch(&self, _job_id: &str) -> Result<BacktestEventStream, BacktestClientError> {
        self.record(ScriptedCall::Watch);
        Ok(Box::pin(stream::empty()))
    }

    async fn result(
        &self,
        _job_id: &str,
    ) -> Result<GetBacktestResultResponse, BacktestClientError> {
        self.record(ScriptedCall::Result);
        Err(unsupported("result"))
    }

    async fn get_result_artifact_chunk(
        &self,
        _request: GetResultArtifactChunkRequest,
    ) -> Result<GetResultArtifactChunkResponse, BacktestClientError> {
        self.record(ScriptedCall::ArtifactChunk);
        Err(unsupported("artifact chunk"))
    }

    async fn delete_result_artifact(
        &self,
        _request: DeleteResultArtifactRequest,
    ) -> Result<DeleteResultArtifactResponse, BacktestClientError> {
        self.record(ScriptedCall::DeleteArtifact);
        Err(unsupported("delete artifact"))
    }

    async fn cancel(&self, _job_id: &str) -> Result<CancelBacktestResponse, BacktestClientError> {
        self.record(ScriptedCall::Cancel);
        Err(unsupported("cancel"))
    }
}

#[async_trait]
impl BacktestDiscoveryClient for ScriptedCatalogClient {
    async fn list_profiles(&self) -> Result<ListProfilesResponse, BacktestClientError> {
        self.record(ScriptedCall::Profiles);
        self.profiles.clone()
    }

    async fn list_symbols(
        &self,
        _request: ListSymbolsRequest,
    ) -> Result<ListSymbolsResponse, BacktestClientError> {
        self.record(ScriptedCall::Symbols);
        self.symbols.clone()
    }
}

fn unsupported(operation: &str) -> BacktestClientError {
    BacktestClientError::Service(format!("scripted {operation} response is not configured"))
}
