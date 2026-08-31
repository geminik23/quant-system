use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream;
use qs_backtest_api::{
    BacktestClient, BacktestClientError, BacktestDiscoveryClient, BacktestEvent,
    BacktestEventStream, BacktestStatusResponse, CancelBacktestResponse,
    DeleteResultArtifactRequest, DeleteResultArtifactResponse, GetBacktestResultResponse,
    GetResultArtifactChunkRequest, GetResultArtifactChunkResponse, ListProfilesResponse,
    ListSymbolsRequest, ListSymbolsResponse, PingResponse, SubmitBacktestRequest,
    SubmitBacktestResponse,
};

use crate::{BacktestCatalogConnector, BacktestConnector, ManagedBacktestClient, WorkflowSleeper};

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
    close_error: Option<BacktestClientError>,
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
            close_error: None,
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
            close_error: None,
        }
    }

    pub fn calls(&self) -> Vec<ScriptedCall> {
        self.client.calls()
    }

    pub fn client_mut(&mut self) -> &mut ScriptedCatalogClient {
        &mut self.client
    }

    pub fn fail_close(&mut self, error: BacktestClientError) {
        self.close_error = Some(error);
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
        match &self.close_error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
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

#[derive(Debug, Clone, Default)]
pub struct ScriptedWorkflowSleeper {
    requested_delays: Arc<Mutex<Vec<Duration>>>,
}

impl ScriptedWorkflowSleeper {
    pub fn requested_delays(&self) -> Vec<Duration> {
        self.requested_delays
            .lock()
            .expect("scripted sleeper lock is not poisoned")
            .clone()
    }
}

#[async_trait]
impl WorkflowSleeper for ScriptedWorkflowSleeper {
    async fn sleep(&self, duration: Duration) {
        self.requested_delays
            .lock()
            .expect("scripted sleeper lock is not poisoned")
            .push(duration);
    }
}

/// Identity-bearing calls recorded by the retained-workflow scripted client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedRunCall {
    Connect {
        connection_id: Option<String>,
    },
    Ping {
        connection_id: String,
    },
    Submit {
        connection_id: String,
        signal_count: usize,
        serialized_request_bytes: usize,
    },
    Status {
        connection_id: String,
        job_id: String,
    },
    Watch {
        connection_id: String,
        job_id: String,
    },
    Result {
        connection_id: String,
        job_id: String,
    },
    ArtifactChunk {
        connection_id: String,
        artifact_id: String,
        offset: u64,
    },
    DeleteArtifact {
        connection_id: String,
        artifact_id: String,
    },
    Cancel {
        connection_id: String,
        job_id: String,
    },
    Close {
        connection_id: String,
    },
}

pub enum ScriptedConnectionStep {
    ConnectError(BacktestClientError),
    Client(Box<ScriptedConnectionScript>),
}

impl ScriptedConnectionStep {
    pub fn client(script: ScriptedConnectionScript) -> Self {
        Self::Client(Box::new(script))
    }

    pub fn connect_error(error: BacktestClientError) -> Self {
        Self::ConnectError(error)
    }
}

#[derive(Clone, Default)]
pub struct ScriptedSubmitGate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl ScriptedSubmitGate {
    pub async fn wait_started(&self) {
        self.started.notified().await;
    }

    pub fn release(&self) {
        self.release.notify_one();
    }
}

pub struct ScriptedConnectionScript {
    connection_id: String,
    submit_gate: Option<ScriptedSubmitGate>,
    ping: VecDeque<Result<PingResponse, BacktestClientError>>,
    submit: VecDeque<Result<SubmitBacktestResponse, BacktestClientError>>,
    status: VecDeque<Result<BacktestStatusResponse, BacktestClientError>>,
    watch: VecDeque<Result<Vec<Result<BacktestEvent, BacktestClientError>>, BacktestClientError>>,
    result: VecDeque<Result<GetBacktestResultResponse, BacktestClientError>>,
    artifact: VecDeque<Result<GetResultArtifactChunkResponse, BacktestClientError>>,
    delete_artifact: VecDeque<Result<DeleteResultArtifactResponse, BacktestClientError>>,
    cancel: VecDeque<Result<CancelBacktestResponse, BacktestClientError>>,
    close: Result<(), BacktestClientError>,
}

impl ScriptedConnectionScript {
    pub fn new(connection_id: impl Into<String>) -> Self {
        Self {
            connection_id: connection_id.into(),
            submit_gate: None,
            ping: VecDeque::new(),
            submit: VecDeque::new(),
            status: VecDeque::new(),
            watch: VecDeque::new(),
            result: VecDeque::new(),
            artifact: VecDeque::new(),
            delete_artifact: VecDeque::new(),
            cancel: VecDeque::new(),
            close: Ok(()),
        }
    }

    pub fn push_ping(&mut self, response: Result<PingResponse, BacktestClientError>) {
        self.ping.push_back(response);
    }

    pub fn push_submit(&mut self, response: Result<SubmitBacktestResponse, BacktestClientError>) {
        self.submit.push_back(response);
    }

    pub fn set_submit_gate(&mut self, gate: ScriptedSubmitGate) {
        self.submit_gate = Some(gate);
    }

    pub fn push_status(&mut self, response: Result<BacktestStatusResponse, BacktestClientError>) {
        self.status.push_back(response);
    }

    pub fn push_watch(
        &mut self,
        response: Result<Vec<Result<BacktestEvent, BacktestClientError>>, BacktestClientError>,
    ) {
        self.watch.push_back(response);
    }

    pub fn push_result(
        &mut self,
        response: Result<GetBacktestResultResponse, BacktestClientError>,
    ) {
        self.result.push_back(response);
    }

    pub fn push_artifact_chunk(
        &mut self,
        response: Result<GetResultArtifactChunkResponse, BacktestClientError>,
    ) {
        self.artifact.push_back(response);
    }

    pub fn push_delete_artifact(
        &mut self,
        response: Result<DeleteResultArtifactResponse, BacktestClientError>,
    ) {
        self.delete_artifact.push_back(response);
    }

    pub fn push_cancel(&mut self, response: Result<CancelBacktestResponse, BacktestClientError>) {
        self.cancel.push_back(response);
    }

    pub fn set_close(&mut self, response: Result<(), BacktestClientError>) {
        self.close = response;
    }
}

struct ScriptedConnectorState {
    connections: VecDeque<ScriptedConnectionStep>,
    calls: Vec<ScriptedRunCall>,
}

#[derive(Clone)]
pub struct ScriptedBacktestConnector {
    endpoint_display: String,
    state: Arc<Mutex<ScriptedConnectorState>>,
}

impl ScriptedBacktestConnector {
    pub fn new(
        endpoint_display: impl Into<String>,
        connections: impl IntoIterator<Item = ScriptedConnectionStep>,
    ) -> Self {
        Self {
            endpoint_display: endpoint_display.into(),
            state: Arc::new(Mutex::new(ScriptedConnectorState {
                connections: connections.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    pub fn calls(&self) -> Vec<ScriptedRunCall> {
        self.state
            .lock()
            .expect("scripted connector lock is not poisoned")
            .calls
            .clone()
    }
}

#[async_trait]
impl BacktestConnector for ScriptedBacktestConnector {
    type Client = ScriptedBacktestClient;

    fn endpoint_display(&self) -> String {
        self.endpoint_display.clone()
    }

    async fn connect(&self) -> Result<Self::Client, BacktestClientError> {
        let mut state = self
            .state
            .lock()
            .expect("scripted connector lock is not poisoned");
        let step = state
            .connections
            .pop_front()
            .ok_or_else(|| unsupported("connection"))?;
        match step {
            ScriptedConnectionStep::ConnectError(error) => {
                state.calls.push(ScriptedRunCall::Connect {
                    connection_id: None,
                });
                Err(error)
            }
            ScriptedConnectionStep::Client(script) => {
                let script = *script;
                let connection_id = script.connection_id.clone();
                state.calls.push(ScriptedRunCall::Connect {
                    connection_id: Some(connection_id.clone()),
                });
                Ok(ScriptedBacktestClient {
                    connection_id,
                    script: Arc::new(Mutex::new(script)),
                    state: Arc::clone(&self.state),
                })
            }
        }
    }
}

pub struct ScriptedBacktestClient {
    connection_id: String,
    script: Arc<Mutex<ScriptedConnectionScript>>,
    state: Arc<Mutex<ScriptedConnectorState>>,
}

impl ScriptedBacktestClient {
    fn record(&self, call: ScriptedRunCall) {
        self.state
            .lock()
            .expect("scripted connector lock is not poisoned")
            .calls
            .push(call);
    }

    fn pop<T>(
        queue: &mut VecDeque<Result<T, BacktestClientError>>,
        operation: &str,
    ) -> Result<T, BacktestClientError> {
        queue
            .pop_front()
            .unwrap_or_else(|| Err(unsupported(operation)))
    }
}

#[async_trait]
impl ManagedBacktestClient for ScriptedBacktestClient {
    async fn close(self) -> Result<(), BacktestClientError> {
        self.record(ScriptedRunCall::Close {
            connection_id: self.connection_id.clone(),
        });
        self.script
            .lock()
            .expect("scripted connection lock is not poisoned")
            .close
            .clone()
    }
}

#[async_trait]
impl BacktestClient for ScriptedBacktestClient {
    async fn ping(&self) -> Result<PingResponse, BacktestClientError> {
        self.record(ScriptedRunCall::Ping {
            connection_id: self.connection_id.clone(),
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .ping,
            "ping",
        )
    }

    async fn submit(
        &self,
        request: SubmitBacktestRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError> {
        let gate = self
            .script
            .lock()
            .expect("scripted connection lock is not poisoned")
            .submit_gate
            .clone();
        if let Some(gate) = gate {
            gate.started.notify_one();
            gate.release.notified().await;
        }
        let serialized_request_bytes = serde_json::to_vec(&request).map_or(0, |value| value.len());
        self.record(ScriptedRunCall::Submit {
            connection_id: self.connection_id.clone(),
            signal_count: request.request.request.raw_signals.len(),
            serialized_request_bytes,
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .submit,
            "submit",
        )
    }

    async fn status(&self, job_id: &str) -> Result<BacktestStatusResponse, BacktestClientError> {
        self.record(ScriptedRunCall::Status {
            connection_id: self.connection_id.clone(),
            job_id: job_id.into(),
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .status,
            "status",
        )
    }

    async fn watch(&self, job_id: &str) -> Result<BacktestEventStream, BacktestClientError> {
        self.record(ScriptedRunCall::Watch {
            connection_id: self.connection_id.clone(),
            job_id: job_id.into(),
        });
        let items = Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .watch,
            "watch",
        )?;
        Ok(Box::pin(stream::iter(items)))
    }

    async fn result(&self, job_id: &str) -> Result<GetBacktestResultResponse, BacktestClientError> {
        self.record(ScriptedRunCall::Result {
            connection_id: self.connection_id.clone(),
            job_id: job_id.into(),
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .result,
            "result",
        )
    }

    async fn get_result_artifact_chunk(
        &self,
        request: GetResultArtifactChunkRequest,
    ) -> Result<GetResultArtifactChunkResponse, BacktestClientError> {
        self.record(ScriptedRunCall::ArtifactChunk {
            connection_id: self.connection_id.clone(),
            artifact_id: request.artifact_id,
            offset: request.offset,
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .artifact,
            "artifact chunk",
        )
    }

    async fn delete_result_artifact(
        &self,
        request: DeleteResultArtifactRequest,
    ) -> Result<DeleteResultArtifactResponse, BacktestClientError> {
        self.record(ScriptedRunCall::DeleteArtifact {
            connection_id: self.connection_id.clone(),
            artifact_id: request.artifact_id,
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .delete_artifact,
            "delete artifact",
        )
    }

    async fn cancel(&self, job_id: &str) -> Result<CancelBacktestResponse, BacktestClientError> {
        self.record(ScriptedRunCall::Cancel {
            connection_id: self.connection_id.clone(),
            job_id: job_id.into(),
        });
        Self::pop(
            &mut self
                .script
                .lock()
                .expect("scripted connection lock is not poisoned")
                .cancel,
            "cancel",
        )
    }
}

fn unsupported(operation: &str) -> BacktestClientError {
    BacktestClientError::Service(format!("scripted {operation} response is not configured"))
}
