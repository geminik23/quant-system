use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt;
use nanoid::nanoid;
use qs_backtest_api::{
    BacktestClient, BacktestClientError, BacktestEvent, BacktestStatusResponse,
    DeleteResultArtifactRequest, GetBacktestResultResponse, ResultArtifactRefMsg,
};
use qs_service::{RetryDisposition, TransportFailureKind};
use tokio::sync::mpsc;

use crate::workflow::{WorkflowActorChannels, WorkflowFrontendChannels, workflow_channel_pair};
use crate::{
    AnalysisDatasetState, ArtifactDownload, BacktestConnector, BacktestResultDocument,
    BacktestRunSnapshot, BacktestWorkflowEvent, BacktestWorkflowEventKind, ClientJobStatus,
    CommittedOutput, CompletedBacktest, LocalCommitState, ManagedBacktestClient, OpenedResultFile,
    OutputCommit, OutputIntent, OutputIntentSummary, OutputTarget, PersistedExecutionDatasetState,
    PreparedBacktest, RESUME_RECORD_FORMAT_VERSION, ReconnectPolicy, ResultFileFormat,
    ResultIoLimits, ResultOutput, ResumeRecord, RunTransitionStore, TokioWorkflowSleeper,
    WorkflowChannelConfig, WorkflowChannelError, WorkflowCommand, WorkflowCompletion,
    WorkflowError, WorkflowSleeper, WorkflowState, open_result_path, stage_output,
};

pub type BacktestWorkflowEventStream = mpsc::Receiver<BacktestWorkflowEvent>;

pub struct BacktestRunHandle {
    local_run_id: String,
    channels: WorkflowFrontendChannels,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl BacktestRunHandle {
    pub fn local_run_id(&self) -> &str {
        &self.local_run_id
    }

    pub fn snapshot(&self) -> BacktestRunSnapshot {
        self.channels.snapshot()
    }

    pub fn take_events(&mut self) -> Option<BacktestWorkflowEventStream> {
        self.channels.take_events()
    }

    pub async fn cancel(&self) -> Result<(), WorkflowChannelError> {
        self.channels.send_command(WorkflowCommand::Cancel).await
    }

    pub async fn detach(&self) -> Result<(), WorkflowChannelError> {
        self.channels.send_command(WorkflowCommand::Detach).await
    }

    pub async fn save_as(&self, output: OutputTarget) -> Result<(), WorkflowChannelError> {
        self.channels
            .send_command(WorkflowCommand::SaveAs(output))
            .await
    }

    pub async fn join(mut self) -> Result<WorkflowCompletion, WorkflowChannelError> {
        let completion = self.channels.join().await?;
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(completion)
    }
}

impl Drop for BacktestRunHandle {
    fn drop(&mut self) {
        self.channels.try_send_shutdown();
    }
}

pub struct BacktestWorkflow<C, S, Sl = TokioWorkflowSleeper> {
    connector: Arc<C>,
    store: Arc<S>,
    sleeper: Arc<Sl>,
    channels: WorkflowChannelConfig,
    reconnect: ReconnectPolicy,
    result_limits: ResultIoLimits,
}

impl<C, S> BacktestWorkflow<C, S, TokioWorkflowSleeper>
where
    C: BacktestConnector,
    S: RunTransitionStore,
{
    pub fn new(connector: C, store: S) -> Self {
        Self::with_sleeper(connector, store, TokioWorkflowSleeper)
    }
}

impl<C, S, Sl> BacktestWorkflow<C, S, Sl>
where
    C: BacktestConnector,
    S: RunTransitionStore,
    Sl: WorkflowSleeper,
{
    pub fn with_sleeper(connector: C, store: S, sleeper: Sl) -> Self {
        Self {
            connector: Arc::new(connector),
            store: Arc::new(store),
            sleeper: Arc::new(sleeper),
            channels: WorkflowChannelConfig::default(),
            reconnect: ReconnectPolicy,
            result_limits: ResultIoLimits::default(),
        }
    }

    pub fn with_channel_config(mut self, channels: WorkflowChannelConfig) -> Self {
        self.channels = channels;
        self
    }

    pub fn with_result_limits(mut self, limits: ResultIoLimits) -> Result<Self, WorkflowError> {
        self.result_limits =
            limits
                .validate()
                .map_err(|error| WorkflowError::InvalidConfiguration {
                    field: "result I/O limits",
                    detail: error.to_string(),
                })?;
        Ok(self)
    }

    pub fn start(
        &self,
        local_run_id: impl Into<String>,
        prepared: PreparedBacktest,
        output: OutputIntent,
    ) -> BacktestRunHandle {
        self.spawn(
            local_run_id.into(),
            ActorStart::Submit {
                prepared: Box::new(prepared),
                output,
            },
        )
    }

    pub fn resume(
        &self,
        local_run_id: impl Into<String>,
        record: ResumeRecord,
    ) -> BacktestRunHandle {
        self.spawn(local_run_id.into(), ActorStart::Resume { record })
    }

    pub fn recover_output(
        &self,
        local_run_id: impl Into<String>,
        record: ResumeRecord,
        output: OutputTarget,
    ) -> BacktestRunHandle {
        self.spawn(local_run_id.into(), ActorStart::Recover { record, output })
    }

    fn spawn(&self, local_run_id: String, start: ActorStart) -> BacktestRunHandle {
        let output = match &start {
            ActorStart::Submit { output, .. } => Some(OutputIntentSummary::from(output)),
            ActorStart::Resume { record } => Some(record.output.clone()),
            ActorStart::Recover { output, .. } => Some(OutputIntentSummary::from(
                &OutputIntent::Persist(output.clone()),
            )),
        };
        let initial = BacktestRunSnapshot {
            output,
            ..BacktestRunSnapshot::default()
        };
        let (frontend, actor_channels) = workflow_channel_pair(self.channels, initial);
        let actor = WorkflowActor {
            connector: Arc::clone(&self.connector),
            store: Arc::clone(&self.store),
            sleeper: Arc::clone(&self.sleeper),
            reconnect: self.reconnect,
            result_limits: self.result_limits,
            local_run_id: local_run_id.clone(),
            channels: actor_channels,
        };
        let task = tokio::spawn(async move {
            actor.run(start).await;
        });
        BacktestRunHandle {
            local_run_id,
            channels: frontend,
            task: Some(task),
        }
    }
}

enum ActorStart {
    Submit {
        prepared: Box<PreparedBacktest>,
        output: OutputIntent,
    },
    Resume {
        record: ResumeRecord,
    },
    Recover {
        record: ResumeRecord,
        output: OutputTarget,
    },
}

struct WorkflowActor<C, S, Sl>
where
    C: BacktestConnector,
    S: RunTransitionStore,
    Sl: WorkflowSleeper,
{
    connector: Arc<C>,
    store: Arc<S>,
    sleeper: Arc<Sl>,
    reconnect: ReconnectPolicy,
    result_limits: ResultIoLimits,
    local_run_id: String,
    channels: WorkflowActorChannels,
}

impl<C, S, Sl> WorkflowActor<C, S, Sl>
where
    C: BacktestConnector,
    S: RunTransitionStore,
    Sl: WorkflowSleeper,
{
    async fn run(mut self, start: ActorStart) {
        let outcome = self.run_inner(start).await;
        match outcome {
            Ok(completion) => self.channels.complete(completion),
            Err(error) => {
                self.channels.update_snapshot(|snapshot| {
                    snapshot.state = match error {
                        WorkflowError::SubmissionUncertain { .. } => {
                            WorkflowState::SubmissionUncertain
                        }
                        _ => WorkflowState::Failed,
                    };
                    snapshot.current_error = Some(error.to_string());
                });
                let completion = if matches!(error, WorkflowError::SubmissionUncertain { .. }) {
                    WorkflowCompletion::SubmissionUncertain
                } else {
                    WorkflowCompletion::Failed(error.to_string())
                };
                self.channels.complete(completion);
            }
        }
    }

    async fn run_inner(&mut self, start: ActorStart) -> Result<WorkflowCompletion, WorkflowError> {
        match start {
            ActorStart::Submit { prepared, output } => self.submit_run(*prepared, output).await,
            ActorStart::Resume { record } => self.resume_run(record, None).await,
            ActorStart::Recover { record, output } => self.resume_run(record, Some(output)).await,
        }
    }

    async fn submit_run(
        &mut self,
        prepared: PreparedBacktest,
        output: OutputIntent,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        let (request, input, request_summary, _) = prepared.into_workflow_parts();
        self.state(WorkflowState::Connecting);
        let client = self
            .connector
            .connect()
            .await
            .map_err(|error| WorkflowError::Connection {
                detail: error.to_string(),
            })?;
        if let Some(command) = self.channels.try_recv_command() {
            match command {
                WorkflowCommand::Cancel => {
                    let _ = client.close().await;
                    self.state(WorkflowState::Cancelled);
                    return Ok(WorkflowCompletion::Cancelled);
                }
                WorkflowCommand::Detach => {
                    let _ = client.close().await;
                    self.state(WorkflowState::Detached);
                    return Ok(WorkflowCompletion::Detached(None));
                }
                WorkflowCommand::Shutdown => {
                    let _ = client.close().await;
                    self.state(WorkflowState::Detached);
                    return Ok(WorkflowCompletion::Detached(None));
                }
                WorkflowCommand::SaveAs(_) => {}
            }
        }

        self.state(WorkflowState::Submitting);
        let mut submit = Box::pin(client.submit(request));
        let response = loop {
            tokio::select! {
                response = &mut submit => break response,
                command = self.channels.recv_command() => match command {
                    Some(WorkflowCommand::SaveAs(_)) => continue,
                    Some(WorkflowCommand::Cancel | WorkflowCommand::Detach | WorkflowCommand::Shutdown)
                    | None => {
                        drop(submit);
                        let _ = client.close().await;
                        return Err(WorkflowError::SubmissionUncertain {
                            detail: "submission observation stopped before a job ID was confirmed".into(),
                        });
                    }
                }
            }
        };
        drop(submit);
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                let _ = client.close().await;
                return Err(WorkflowError::SubmissionUncertain {
                    detail: error.to_string(),
                });
            }
        };
        if !response.success {
            let detail = response
                .error
                .unwrap_or_else(|| "service rejected the submission".into());
            let _ = client.close().await;
            return Err(WorkflowError::Submission { detail });
        }
        let Some(job_id) = response.job_id.filter(|value| !value.trim().is_empty()) else {
            let _ = client.close().await;
            return Err(WorkflowError::Submission {
                detail: "successful submission omitted its job ID".into(),
            });
        };
        let mut record = ResumeRecord {
            format_version: RESUME_RECORD_FORMAT_VERSION,
            state_sequence: 1,
            endpoint_display: self.connector.endpoint_display(),
            job_id: job_id.clone(),
            output: OutputIntentSummary::from(&output),
            input,
            request: request_summary.clone(),
            result_delivery: request_summary.result_delivery,
            submitted_at: Utc::now(),
            last_known_state: ClientJobStatus::Queued,
            local_commit: LocalCommitState::NotStarted,
            commit_intent_id: None,

            pending_artifact_id: None,
            artifact_released: false,
        };
        if let Err(error) = self
            .store
            .compare_and_swap(&self.local_run_id, 0, record.clone())
            .await
        {
            let _ = client.close().await;
            return Err(store_error(error));
        }
        self.channels.update_snapshot(|snapshot| {
            snapshot.job_id = Some(job_id.clone());
            snapshot.state = WorkflowState::Submitted;
            snapshot.resume_record = Some(record.clone());
        });
        self.channels
            .publish_event(BacktestWorkflowEventKind::ResumeRecordChanged);
        self.observe_and_deliver(client, &mut record, output, false)
            .await
    }

    async fn resume_run(
        &mut self,
        mut record: ResumeRecord,
        output_override: Option<OutputTarget>,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        validate_resume_record(&record)?;
        if let Some(output) = output_override {
            update_record(&self.store, &self.local_run_id, &mut record, |next| {
                next.output = OutputIntentSummary::from(&OutputIntent::Persist(output.clone()));
                next.local_commit = LocalCommitState::NotStarted;
                next.commit_intent_id = None;
            })
            .await?;
        }
        self.channels.update_snapshot(|snapshot| {
            snapshot.job_id = Some(record.job_id.clone());
            snapshot.output = Some(record.output.clone());
            snapshot.local_commit = record.local_commit;
            snapshot.resume_record = Some(record.clone());
        });

        if record.local_commit == LocalCommitState::Committed {
            return self.complete_from_local_with_release(&mut record).await;
        }
        if record.local_commit == LocalCommitState::CommitPrepared
            && self.try_adopt_committed(&mut record).await?
        {
            return self.complete_from_local_with_release(&mut record).await;
        }

        self.state(WorkflowState::Connecting);
        let output = output_intent_from_summary(&record.output)?;
        match self.connect_resume(&mut record).await? {
            ReconnectOutcome::Completed(client) => {
                self.deliver_result(client, &mut record, output).await
            }
            ReconnectOutcome::Active(client, cancel_sent) => {
                self.observe_and_deliver(client, &mut record, output, cancel_sent)
                    .await
            }
            ReconnectOutcome::Cancelled => {
                self.state(WorkflowState::Cancelled);
                Ok(WorkflowCompletion::Cancelled)
            }
            ReconnectOutcome::Detached => {
                self.state(WorkflowState::Detached);
                Ok(WorkflowCompletion::Detached(Some(record)))
            }
        }
    }

    async fn connect_resume(
        &mut self,
        record: &mut ResumeRecord,
    ) -> Result<ReconnectOutcome<C::Client>, WorkflowError> {
        let mut attempt = 0_u32;
        let mut cancel_requested = false;
        loop {
            let client = match self.connector.connect().await {
                Ok(client) => client,
                Err(error) if read_is_retryable(&error) => {
                    attempt = attempt.saturating_add(1);
                    let delay = self.reconnect.delay_for_attempt(attempt);
                    self.publish_reconnecting(attempt, delay);
                    tokio::select! {
                        _ = self.sleeper.sleep(delay) => {}
                        command = self.channels.recv_command() => match command {
                            Some(WorkflowCommand::Detach) if !cancel_requested => {
                                return Ok(ReconnectOutcome::Detached);
                            }
                            Some(WorkflowCommand::Cancel) => cancel_requested = true,
                            Some(WorkflowCommand::Shutdown) | None => {
                                return Ok(ReconnectOutcome::Detached);
                            }
                            Some(WorkflowCommand::Detach | WorkflowCommand::SaveAs(_)) => {}
                        }
                    }
                    continue;
                }
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            };
            let status = match client.status(&record.job_id).await {
                Ok(status) => status,
                Err(error) if read_is_retryable(&error) => {
                    let _ = client.close().await;
                    attempt = attempt.saturating_add(1);
                    let delay = self.reconnect.delay_for_attempt(attempt);
                    self.publish_reconnecting(attempt, delay);
                    tokio::select! {
                        _ = self.sleeper.sleep(delay) => {}
                        command = self.channels.recv_command() => match command {
                            Some(WorkflowCommand::Detach) if !cancel_requested => {
                                return Ok(ReconnectOutcome::Detached);
                            }
                            Some(WorkflowCommand::Cancel) => cancel_requested = true,
                            Some(WorkflowCommand::Shutdown) | None => {
                                return Ok(ReconnectOutcome::Detached);
                            }
                            Some(WorkflowCommand::Detach | WorkflowCommand::SaveAs(_)) => {}
                        }
                    }
                    continue;
                }
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            };
            let kind = validate_status(&record.job_id, &status)?;
            self.apply_status(record, &status, kind.clone()).await?;
            if cancel_requested
                && !matches!(
                    kind,
                    ClientJobStatus::Completed
                        | ClientJobStatus::Failed
                        | ClientJobStatus::Cancelled
                        | ClientJobStatus::NotFound
                )
            {
                self.state(WorkflowState::CancelRequested);
                let response =
                    client
                        .cancel(&record.job_id)
                        .await
                        .map_err(|error| WorkflowError::Job {
                            detail: format!("cancellation outcome is uncertain: {error}"),
                        })?;
                if !response.success || response.job_id != record.job_id {
                    return Err(WorkflowError::Job {
                        detail: response
                            .error
                            .unwrap_or_else(|| "cancellation request was rejected".into()),
                    });
                }
            }
            return match kind {
                ClientJobStatus::Completed => Ok(ReconnectOutcome::Completed(client)),
                ClientJobStatus::Cancelled => {
                    let _ = client.close().await;
                    Ok(ReconnectOutcome::Cancelled)
                }
                ClientJobStatus::Failed => Err(WorkflowError::Job {
                    detail: status.error.unwrap_or_else(|| "backtest job failed".into()),
                }),
                ClientJobStatus::NotFound => Err(WorkflowError::RetainedJobLost {
                    job_id: record.job_id.clone(),
                }),
                _ => Ok(ReconnectOutcome::Active(client, cancel_requested)),
            };
        }
    }

    async fn observe_and_deliver(
        &mut self,
        mut client: C::Client,
        record: &mut ResumeRecord,
        output: OutputIntent,
        mut cancel_sent: bool,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        let job_id = record.job_id.clone();
        let mut reconnect_attempt = 0_u32;
        loop {
            self.state(WorkflowState::Watching);
            let stream = client.watch(&job_id).await;
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(error) if read_is_retryable(&error) => {
                    let outcome = self
                        .reconnect(client, record, &mut reconnect_attempt, &mut cancel_sent)
                        .await?;
                    match outcome {
                        ReconnectOutcome::Active(next, sent) => {
                            client = next;
                            cancel_sent = sent;
                            continue;
                        }
                        ReconnectOutcome::Completed(next) => {
                            return self.deliver_result(next, record, output).await;
                        }
                        ReconnectOutcome::Cancelled => {
                            self.state(WorkflowState::Cancelled);
                            return Ok(WorkflowCompletion::Cancelled);
                        }
                        ReconnectOutcome::Detached => {
                            self.state(WorkflowState::Detached);
                            return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                        }
                    }
                }
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            };

            loop {
                tokio::select! {
                    command = self.channels.recv_command() => {
                        match command {
                            Some(WorkflowCommand::Detach) if !cancel_sent => {
                                let _ = client.close().await;
                                self.state(WorkflowState::Detached);
                                return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                            }
                            Some(WorkflowCommand::Cancel) if !cancel_sent => {
                                cancel_sent = true;
                                self.state(WorkflowState::CancelRequested);
                                let response = client.cancel(&job_id).await.map_err(|error| WorkflowError::Job {
                                    detail: format!("cancellation outcome is uncertain: {error}"),
                                })?;
                                if !response.success || response.job_id != job_id {
                                    return Err(WorkflowError::Job {
                                        detail: response.error.unwrap_or_else(|| "cancellation request was rejected".into()),
                                    });
                                }
                            }
                            Some(WorkflowCommand::Shutdown) | None => {
                                let _ = client.close().await;
                                self.state(WorkflowState::Detached);
                                return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                            }
                            Some(
                                WorkflowCommand::Cancel
                                | WorkflowCommand::Detach
                                | WorkflowCommand::SaveAs(_),
                            ) => {}
                        }
                    }
                    item = stream.next() => {
                        match item {
                            Some(Ok(BacktestEvent::Heartbeat { job_id: event_job_id, elapsed_ms })) => {
                                if event_job_id != job_id {
                                    return Err(WorkflowError::Job { detail: "heartbeat job ID mismatch".into() });
                                }
                                self.channels.update_snapshot(|snapshot| {
                                    snapshot.last_heartbeat_elapsed_ms = Some(elapsed_ms);
                                    snapshot.elapsed_ms = Some(elapsed_ms);
                                });
                            }
                            Some(Ok(BacktestEvent::Snapshot { status })) => {
                                let status_kind = validate_status(&job_id, &status)?;
                                self.apply_status(record, &status, status_kind.clone()).await?;
                                reconnect_attempt = 0;
                                self.channels.update_snapshot(|snapshot| {
                                    snapshot.reconnect_attempt = 0;
                                });
                                match status_kind {
                                    ClientJobStatus::Completed => return self.deliver_result(client, record, output).await,
                                    ClientJobStatus::Failed => return Err(WorkflowError::Job {
                                        detail: status.error.unwrap_or_else(|| "backtest job failed".into()),
                                    }),
                                    ClientJobStatus::Cancelled => {
                                        let _ = client.close().await;
                                        self.state(WorkflowState::Cancelled);
                                        return Ok(WorkflowCompletion::Cancelled);
                                    }
                                    ClientJobStatus::NotFound => return Err(WorkflowError::RetainedJobLost { job_id }),
                                    _ => {}
                                }
                            }
                            Some(Err(error)) if read_is_retryable(&error) => break,
                            None => break,
                            Some(Err(error)) => return Err(WorkflowError::Connection { detail: error.to_string() }),
                        }
                    }
                }
            }
            let outcome = self
                .reconnect(client, record, &mut reconnect_attempt, &mut cancel_sent)
                .await?;
            match outcome {
                ReconnectOutcome::Active(next, sent) => {
                    client = next;
                    cancel_sent = sent;
                }
                ReconnectOutcome::Completed(next) => {
                    return self.deliver_result(next, record, output).await;
                }
                ReconnectOutcome::Cancelled => {
                    self.state(WorkflowState::Cancelled);
                    return Ok(WorkflowCompletion::Cancelled);
                }
                ReconnectOutcome::Detached => {
                    self.state(WorkflowState::Detached);
                    return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                }
            }
        }
    }

    async fn reconnect(
        &mut self,
        client: C::Client,
        record: &mut ResumeRecord,
        attempt: &mut u32,
        cancel_sent: &mut bool,
    ) -> Result<ReconnectOutcome<C::Client>, WorkflowError> {
        let _ = client.close().await;
        let mut cancel_requested = false;
        loop {
            *attempt = attempt.saturating_add(1);
            let delay = self.reconnect.delay_for_attempt(*attempt);
            self.publish_reconnecting(*attempt, delay);
            tokio::select! {
                _ = self.sleeper.sleep(delay) => {}
                command = self.channels.recv_command() => {
                    match command {
                        Some(WorkflowCommand::Detach) if !cancel_requested && !*cancel_sent => {
                            return Ok(ReconnectOutcome::Detached);
                        }
                        Some(WorkflowCommand::Cancel) if !*cancel_sent => {
                            cancel_requested = true;
                        }
                        Some(WorkflowCommand::Shutdown) | None => {
                            return Ok(ReconnectOutcome::Detached);
                        }
                        Some(
                            WorkflowCommand::Cancel
                            | WorkflowCommand::Detach
                            | WorkflowCommand::SaveAs(_),
                        ) => {}
                    }
                }
            }
            let next = match self.connector.connect().await {
                Ok(client) => client,
                Err(error) if read_is_retryable(&error) => continue,
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            };
            let status = match next.status(&record.job_id).await {
                Ok(status) => status,
                Err(error) if read_is_retryable(&error) => {
                    let _ = next.close().await;
                    continue;
                }
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            };
            let status_kind = validate_status(&record.job_id, &status)?;
            self.apply_status(record, &status, status_kind.clone())
                .await?;
            if cancel_requested
                && !*cancel_sent
                && !matches!(
                    status_kind,
                    ClientJobStatus::Completed
                        | ClientJobStatus::Failed
                        | ClientJobStatus::Cancelled
                        | ClientJobStatus::NotFound
                )
            {
                self.state(WorkflowState::CancelRequested);
                let response =
                    next.cancel(&record.job_id)
                        .await
                        .map_err(|error| WorkflowError::Job {
                            detail: format!("cancellation outcome is uncertain: {error}"),
                        })?;
                if !response.success || response.job_id != record.job_id {
                    return Err(WorkflowError::Job {
                        detail: response
                            .error
                            .unwrap_or_else(|| "cancellation request was rejected".into()),
                    });
                }
                *cancel_sent = true;
            }
            match status_kind {
                ClientJobStatus::Completed => return Ok(ReconnectOutcome::Completed(next)),
                ClientJobStatus::Cancelled => {
                    let _ = next.close().await;
                    return Ok(ReconnectOutcome::Cancelled);
                }
                ClientJobStatus::Failed => {
                    return Err(WorkflowError::Job {
                        detail: status.error.unwrap_or_else(|| "backtest job failed".into()),
                    });
                }
                ClientJobStatus::NotFound => {
                    return Err(WorkflowError::RetainedJobLost {
                        job_id: record.job_id.clone(),
                    });
                }
                _ => return Ok(ReconnectOutcome::Active(next, *cancel_sent)),
            }
        }
    }

    async fn deliver_result(
        &mut self,
        mut client: C::Client,
        record: &mut ResumeRecord,
        output: OutputIntent,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        self.state(WorkflowState::FetchingResult);
        let mut result_retry_attempt = 0_u32;
        let mut cancel_sent = false;
        let response = loop {
            let result = tokio::select! {
                result = client.result(&record.job_id) => result,
                command = self.channels.recv_command() => {
                    if matches!(
                        command,
                        Some(WorkflowCommand::Detach | WorkflowCommand::Shutdown) | None
                    ) {
                        let _ = client.close().await;
                        self.state(WorkflowState::Detached);
                        return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                    }
                    continue;
                }
            };
            match result {
                Ok(response) => break response,
                Err(error) if read_is_retryable(&error) => {
                    match self
                        .reconnect(client, record, &mut result_retry_attempt, &mut cancel_sent)
                        .await?
                    {
                        ReconnectOutcome::Completed(next) => client = next,
                        ReconnectOutcome::Active(next, sent) => {
                            return Box::pin(self.observe_and_deliver(next, record, output, sent))
                                .await;
                        }
                        ReconnectOutcome::Cancelled => {
                            self.state(WorkflowState::Cancelled);
                            return Ok(WorkflowCompletion::Cancelled);
                        }
                        ReconnectOutcome::Detached => {
                            self.state(WorkflowState::Detached);
                            return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                        }
                    }
                }
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            }
        };
        validate_result_response(&record.job_id, &response)?;
        match output {
            OutputIntent::SummaryOnly => {
                if response.artifact.is_some() && response.result.is_none() {
                    let _ = client.close().await;
                    self.publish_record(record, WorkflowState::CompletedAwaitingOutput);
                    return Ok(WorkflowCompletion::OutputRequired(record.clone()));
                }
                let result = response.result.ok_or_else(|| WorkflowError::Output {
                    detail: "successful result omitted its inline payload".into(),
                })?;
                if response.artifact.is_some() && response.inline_complete {
                    return Err(WorkflowError::ResultFormat {
                        detail: "artifact response cannot be marked inline complete".into(),
                    });
                }
                validate_inline_result(&result, self.result_limits)?;
                let _ = client.close().await;
                self.state(WorkflowState::CompletedSummaryOnly);
                Ok(WorkflowCompletion::CompletedSummaryOnly(
                    CompletedBacktest {
                        job_id: record.job_id.clone(),
                        result,
                        output: None,
                        warning: response
                            .artifact
                            .map(|_| "complete result remains in server artifact retention".into()),
                    },
                ))
            }
            OutputIntent::Persist(target) => {
                let (result, artifact) = if let Some(reference) = response.artifact {
                    if response.inline_complete {
                        return Err(WorkflowError::ResultFormat {
                            detail: "artifact response cannot be marked inline complete".into(),
                        });
                    }
                    update_record(&self.store, &self.local_run_id, record, |next| {
                        next.pending_artifact_id = Some(reference.artifact_id.clone());
                        next.artifact_released = false;
                    })
                    .await?;
                    self.publish_record(record, WorkflowState::DownloadingArtifact);
                    let Some(result) = self
                        .download_artifact(&mut client, record, reference.clone(), &target)
                        .await?
                    else {
                        let _ = client.close().await;
                        self.state(WorkflowState::Detached);
                        return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                    };
                    (result, Some(reference))
                } else {
                    if !response.inline_complete {
                        return Err(WorkflowError::ResultFormat {
                            detail: "incomplete result omitted its artifact reference".into(),
                        });
                    }
                    let result = response.result.ok_or_else(|| WorkflowError::ResultFormat {
                        detail: "successful inline result omitted its payload".into(),
                    })?;
                    validate_inline_result(&result, self.result_limits)?;
                    (result, None)
                };
                let completion = self
                    .persist_result(&mut client, record, target, result, artifact.as_ref())
                    .await?;
                let _ = client.close().await;
                Ok(completion)
            }
        }
    }

    async fn download_artifact(
        &mut self,
        client: &mut C::Client,
        record: &mut ResumeRecord,
        reference: ResultArtifactRefMsg,
        target: &OutputTarget,
    ) -> Result<Option<qs_backtest_api::BacktestResultMsg>, WorkflowError> {
        let directory = output_parent(&target.path);
        let mut download = ArtifactDownload::start(reference, directory, self.result_limits)
            .map_err(artifact_error)?;
        let mut retry_attempt = 0_u32;
        loop {
            let request = download.next_request();
            let response = tokio::select! {
                response = client.get_result_artifact_chunk(request) => response,
                command = self.channels.recv_command() => {
                    if matches!(
                        command,
                        Some(WorkflowCommand::Detach | WorkflowCommand::Shutdown) | None
                    ) {
                        return Ok(None);
                    }
                    continue;
                }
            };
            match response {
                Ok(response) => {
                    let done = download.accept(response).map_err(artifact_error)?;
                    retry_attempt = 0;
                    if done {
                        break;
                    }
                }
                Err(error) if read_is_retryable(&error) => loop {
                    retry_attempt = retry_attempt.saturating_add(1);
                    let delay = self.reconnect.delay_for_attempt(retry_attempt);
                    tokio::select! {
                        _ = self.sleeper.sleep(delay) => {}
                        command = self.channels.recv_command() => {
                            if matches!(
                                command,
                                Some(WorkflowCommand::Detach | WorkflowCommand::Shutdown) | None
                            ) {
                                return Ok(None);
                            }
                        }
                    }
                    let replacement = match self.connector.connect().await {
                        Ok(replacement) => replacement,
                        Err(error) if read_is_retryable(&error) => continue,
                        Err(error) => {
                            return Err(WorkflowError::Connection {
                                detail: error.to_string(),
                            });
                        }
                    };
                    let status = match replacement.status(&record.job_id).await {
                        Ok(status) => status,
                        Err(error) if read_is_retryable(&error) => {
                            let _ = replacement.close().await;
                            continue;
                        }
                        Err(error) => {
                            return Err(WorkflowError::Connection {
                                detail: error.to_string(),
                            });
                        }
                    };
                    let kind = validate_status(&record.job_id, &status)?;
                    if kind != ClientJobStatus::Completed {
                        return Err(WorkflowError::Job {
                            detail: "artifact retry found a non-completed retained job".into(),
                        });
                    }
                    let previous = std::mem::replace(client, replacement);
                    let _ = previous.close().await;
                    break;
                },
                Err(error) => {
                    return Err(WorkflowError::Connection {
                        detail: error.to_string(),
                    });
                }
            }
        }
        download
            .finish(self.result_limits)
            .map(|payload| Some(payload.result))
            .map_err(artifact_error)
    }

    async fn persist_result(
        &mut self,
        client: &mut C::Client,
        record: &mut ResumeRecord,
        target: OutputTarget,
        result: qs_backtest_api::BacktestResultMsg,
        artifact: Option<&ResultArtifactRefMsg>,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        self.state(WorkflowState::ValidatingOutput);
        let (analysis, execution) = project_or_unavailable(&result, &record.request.evaluation)?;
        let document = BacktestResultDocument::new(
            Utc::now(),
            Some(record.job_id.clone()),
            record.input.clone(),
            record.request.clone(),
            result.clone(),
            analysis,
            execution,
        )
        .map_err(|error| WorkflowError::ResultFormat {
            detail: error.to_string(),
        })?;
        let output = match target.format {
            ResultFileFormat::DocumentV1 => ResultOutput::Document(&document),
            ResultFileFormat::LegacyBareResult => ResultOutput::Legacy(&result),
        };
        let mut staged =
            stage_output(target.clone(), output, self.result_limits).map_err(output_error)?;
        update_record(&self.store, &self.local_run_id, record, |next| {
            next.output = OutputIntentSummary::from(&OutputIntent::Persist(target.clone()));
            next.local_commit = LocalCommitState::CommitPrepared;
            next.commit_intent_id = Some(nanoid!(24));
        })
        .await?;
        self.publish_record(record, WorkflowState::CommittingOutput);

        loop {
            match staged.commit(self.result_limits).map_err(output_error)? {
                OutputCommit::Committed(committed) => {
                    update_record(&self.store, &self.local_run_id, record, |next| {
                        next.local_commit = LocalCommitState::Committed;
                    })
                    .await?;
                    self.publish_record(record, WorkflowState::CompletedPersisted);
                    let warning = if let Some(reference) = artifact {
                        match client
                            .delete_result_artifact(DeleteResultArtifactRequest {
                                artifact_id: reference.artifact_id.clone(),
                            })
                            .await
                        {
                            Ok(response)
                                if response.success
                                    && response.artifact_id == reference.artifact_id =>
                            {
                                match update_record(
                                    &self.store,
                                    &self.local_run_id,
                                    record,
                                    |next| {
                                        next.pending_artifact_id = None;
                                        next.artifact_released = true;
                                    },
                                )
                                .await
                                {
                                    Ok(()) => None,
                                    Err(error) => Some(format!(
                                        "artifact released but local release state was not recorded: {error}"
                                    )),
                                }
                            }
                            Ok(response) => Some(response.error.unwrap_or_else(|| {
                                "server artifact release was not accepted".into()
                            })),
                            Err(error) => Some(format!("server artifact release failed: {error}")),
                        }
                    } else {
                        None
                    };
                    if let Some(warning) = warning.as_ref() {
                        self.channels.update_snapshot(|snapshot| {
                            snapshot.current_warning = Some(warning.clone());
                        });
                        self.channels
                            .publish_event(BacktestWorkflowEventKind::WarningChanged);
                    }
                    return Ok(WorkflowCompletion::CompletedPersisted(CompletedBacktest {
                        job_id: record.job_id.clone(),
                        result,
                        output: Some(committed),
                        warning,
                    }));
                }
                OutputCommit::Conflict(conflict) => {
                    staged = conflict;
                    self.publish_record(record, WorkflowState::CompletedAwaitingOutput);
                    match self.channels.recv_command().await {
                        Some(WorkflowCommand::SaveAs(next_target)) => {
                            staged = staged
                                .retarget(next_target.clone(), self.result_limits)
                                .map_err(output_error)?;
                            update_record(&self.store, &self.local_run_id, record, |next| {
                                next.output = OutputIntentSummary::from(&OutputIntent::Persist(
                                    next_target.clone(),
                                ));
                            })
                            .await?;
                            self.publish_record(record, WorkflowState::CommittingOutput);
                        }
                        Some(
                            WorkflowCommand::Detach
                            | WorkflowCommand::Cancel
                            | WorkflowCommand::Shutdown,
                        )
                        | None => {
                            self.state(WorkflowState::Detached);
                            return Ok(WorkflowCompletion::Detached(Some(record.clone())));
                        }
                    }
                }
            }
        }
    }

    async fn apply_status(
        &mut self,
        record: &mut ResumeRecord,
        status: &BacktestStatusResponse,
        kind: ClientJobStatus,
    ) -> Result<(), WorkflowError> {
        if record.last_known_state != kind {
            update_record(&self.store, &self.local_run_id, record, |next| {
                next.last_known_state = kind.clone();
            })
            .await?;
        }
        self.channels.update_snapshot(|snapshot| {
            snapshot.elapsed_ms = status.elapsed_ms;
            snapshot.progress.stage.clone_from(&status.progress.stage);
            snapshot.progress.processed_events = snapshot
                .progress
                .processed_events
                .max(status.progress.processed_events);
            snapshot.progress.total_events = snapshot
                .progress
                .total_events
                .max(status.progress.total_events);
            snapshot.progress.processed_signals = snapshot
                .progress
                .processed_signals
                .max(status.progress.processed_signals);
            snapshot.progress.total_signals = snapshot
                .progress
                .total_signals
                .max(status.progress.total_signals);
            snapshot.progress.processed_symbols = snapshot
                .progress
                .processed_symbols
                .max(status.progress.processed_symbols);
            snapshot.progress.total_symbols = snapshot
                .progress
                .total_symbols
                .max(status.progress.total_symbols);
            snapshot.resume_record = Some(record.clone());
        });
        Ok(())
    }

    async fn try_adopt_committed(
        &mut self,
        record: &mut ResumeRecord,
    ) -> Result<bool, WorkflowError> {
        let OutputIntentSummary::Persist { path, format, .. } = &record.output else {
            return Ok(false);
        };
        if *format == ResultFileFormat::LegacyBareResult {
            return Ok(false);
        }
        if !path.exists() {
            return Ok(false);
        }
        if self.verify_local_result(record).is_err() {
            return Ok(false);
        }
        update_record(&self.store, &self.local_run_id, record, |next| {
            next.local_commit = LocalCommitState::Committed;
        })
        .await?;
        Ok(true)
    }

    async fn complete_from_local_with_release(
        &mut self,
        record: &mut ResumeRecord,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        let mut completion = self.complete_from_local(record)?;
        let Some(artifact_id) = record.pending_artifact_id.clone() else {
            return Ok(completion);
        };
        let warning = match self.connector.connect().await {
            Ok(client) => {
                let release = client
                    .delete_result_artifact(DeleteResultArtifactRequest {
                        artifact_id: artifact_id.clone(),
                    })
                    .await;
                let _ = client.close().await;
                match release {
                    Ok(response) if response.success && response.artifact_id == artifact_id => {
                        match update_record(&self.store, &self.local_run_id, record, |next| {
                            next.pending_artifact_id = None;
                            next.artifact_released = true;
                        })
                        .await
                        {
                            Ok(()) => None,
                            Err(error) => Some(format!(
                                "artifact released but local release state was not recorded: {error}"
                            )),
                        }
                    }
                    Ok(response) => Some(response.error.unwrap_or_else(|| {
                        "server artifact release was not accepted after recovery".into()
                    })),
                    Err(error) => Some(format!(
                        "server artifact release failed after recovery: {error}"
                    )),
                }
            }
            Err(error) => Some(format!(
                "server artifact release connection failed after recovery: {error}"
            )),
        };
        if let WorkflowCompletion::CompletedPersisted(completed) = &mut completion {
            completed.warning = warning.clone();
        }
        if let Some(warning) = warning {
            self.channels.update_snapshot(|snapshot| {
                snapshot.current_warning = Some(warning);
            });
        }
        Ok(completion)
    }

    fn complete_from_local(
        &mut self,
        record: &ResumeRecord,
    ) -> Result<WorkflowCompletion, WorkflowError> {
        let OutputIntentSummary::Persist { path, .. } = &record.output else {
            return Err(WorkflowError::Output {
                detail: "committed record has no persisted output".into(),
            });
        };
        let (result, byte_len) = self.verify_local_result(record)?;
        self.state(WorkflowState::CompletedPersisted);
        Ok(WorkflowCompletion::CompletedPersisted(CompletedBacktest {
            job_id: record.job_id.clone(),
            result,
            output: Some(CommittedOutput {
                path: path.clone(),
                byte_len,
            }),
            warning: None,
        }))
    }

    fn verify_local_result(
        &self,
        record: &ResumeRecord,
    ) -> Result<(qs_backtest_api::BacktestResultMsg, u64), WorkflowError> {
        let OutputIntentSummary::Persist { path, format, .. } = &record.output else {
            return Err(WorkflowError::Output {
                detail: "local result verification requires a persisted output".into(),
            });
        };
        let opened = open_result_path(path, self.result_limits).map_err(output_error)?;
        let identity_matches = match (&opened, format) {
            (OpenedResultFile::Document(document), ResultFileFormat::DocumentV1) => {
                document.job_id.as_deref() == Some(record.job_id.as_str())
                    && document.input == record.input
                    && document.request == record.request
            }
            (OpenedResultFile::Legacy(_), ResultFileFormat::LegacyBareResult) => true,
            _ => false,
        };
        if !identity_matches {
            return Err(WorkflowError::Output {
                detail: "local result format or identity does not match the resume record".into(),
            });
        }

        let byte_len = std::fs::metadata(path)
            .map_err(|error| WorkflowError::Output {
                detail: error.to_string(),
            })?
            .len();
        Ok((opened.result().clone(), byte_len))
    }

    fn publish_record(&mut self, record: &ResumeRecord, state: WorkflowState) {
        self.channels.update_snapshot(|snapshot| {
            snapshot.state = state;
            snapshot.local_commit = record.local_commit;
            snapshot.output = Some(record.output.clone());
            snapshot.resume_record = Some(record.clone());
        });
        self.channels
            .publish_event(BacktestWorkflowEventKind::ResumeRecordChanged);
    }

    fn publish_reconnecting(&mut self, attempt: u32, delay: std::time::Duration) {
        self.channels.update_snapshot(|snapshot| {
            snapshot.state = WorkflowState::Reconnecting;
            snapshot.reconnect_attempt = attempt;
        });
        self.channels
            .publish_event(BacktestWorkflowEventKind::Reconnecting { attempt, delay });
    }

    fn state(&mut self, state: WorkflowState) {
        self.channels.update_snapshot(|snapshot| {
            snapshot.state = state.clone();
        });
        self.channels
            .publish_event(BacktestWorkflowEventKind::StateChanged(state));
    }
}

enum ReconnectOutcome<C> {
    Active(C, bool),
    Completed(C),
    Cancelled,
    Detached,
}

async fn update_record<S: RunTransitionStore>(
    store: &Arc<S>,
    local_run_id: &str,
    record: &mut ResumeRecord,
    update: impl FnOnce(&mut ResumeRecord),
) -> Result<(), WorkflowError> {
    let expected = record.state_sequence;
    let mut next = record.clone();
    update(&mut next);
    next.state_sequence = expected.saturating_add(1);
    store
        .compare_and_swap(local_run_id, expected, next.clone())
        .await
        .map_err(store_error)?;
    *record = next;
    Ok(())
}

fn validate_resume_record(record: &ResumeRecord) -> Result<(), WorkflowError> {
    if record.format_version != RESUME_RECORD_FORMAT_VERSION {
        return Err(WorkflowError::Job {
            detail: format!(
                "unsupported resume record version {}",
                record.format_version
            ),
        });
    }
    if record.job_id.trim().is_empty() {
        return Err(WorkflowError::Job {
            detail: "resume record has an empty job ID".into(),
        });
    }
    Ok(())
}

fn validate_status(
    expected_job_id: &str,
    status: &BacktestStatusResponse,
) -> Result<ClientJobStatus, WorkflowError> {
    if status.job_id != expected_job_id {
        return Err(WorkflowError::Job {
            detail: "status response job ID mismatch".into(),
        });
    }
    let kind = match status.status.as_str() {
        "Queued" => ClientJobStatus::Queued,
        "LoadingData" | "LoadingPrimaryData" | "LoadingConversionData" => {
            ClientJobStatus::LoadingData
        }
        "Running" => ClientJobStatus::Running,
        "Completed" => ClientJobStatus::Completed,
        "Failed" => ClientJobStatus::Failed,
        "Cancelled" => ClientJobStatus::Cancelled,
        "NotFound" => ClientJobStatus::NotFound,
        other => ClientJobStatus::Unknown(other.into()),
    };
    if !status.success && kind != ClientJobStatus::NotFound {
        return Err(WorkflowError::Job {
            detail: status
                .error
                .clone()
                .unwrap_or_else(|| "status request failed".into()),
        });
    }
    Ok(kind)
}

fn validate_result_response(
    expected_job_id: &str,
    response: &GetBacktestResultResponse,
) -> Result<(), WorkflowError> {
    if response.job_id != expected_job_id {
        return Err(WorkflowError::ResultFormat {
            detail: "result response job ID mismatch".into(),
        });
    }
    if response.artifact_consumed {
        return Err(WorkflowError::Output {
            detail: "server result artifact was already consumed".into(),
        });
    }
    if !response.success {
        return Err(WorkflowError::Job {
            detail: response
                .error
                .clone()
                .unwrap_or_else(|| "result request failed".into()),
        });
    }
    Ok(())
}

fn validate_inline_result(
    result: &qs_backtest_api::BacktestResultMsg,
    limits: ResultIoLimits,
) -> Result<(), WorkflowError> {
    struct BoundedWriter {
        count: u64,
        maximum: u64,
    }

    impl std::io::Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self.count.saturating_add(bytes.len() as u64);
            if next > self.maximum {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "inline result exceeds configured payload limit",
                ));
            }
            self.count = next;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = BoundedWriter {
        count: 0,
        maximum: limits.maximum_decoded_payload_bytes,
    };
    serde_json::to_writer(&mut writer, result).map_err(|error| WorkflowError::ResultFormat {
        detail: error.to_string(),
    })?;
    crate::validate_backtest_result(result).map_err(|error| WorkflowError::ResultFormat {
        detail: error.to_string(),
    })
}

fn output_intent_from_summary(
    summary: &OutputIntentSummary,
) -> Result<OutputIntent, WorkflowError> {
    Ok(match summary {
        OutputIntentSummary::Persist {
            path,
            format,
            conflict,
        } => OutputIntent::Persist(OutputTarget {
            path: path.clone(),
            format: *format,
            conflict: *conflict,
        }),
        OutputIntentSummary::SummaryOnly => OutputIntent::SummaryOnly,
    })
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn read_is_retryable(error: &BacktestClientError) -> bool {
    match error {
        BacktestClientError::Transport(failure) => {
            failure.retry != RetryDisposition::Never
                && matches!(
                    failure.kind,
                    TransportFailureKind::Unavailable
                        | TransportFailureKind::ConnectTimeout
                        | TransportFailureKind::ReadTimeout
                        | TransportFailureKind::WriteTimeout
                        | TransportFailureKind::ConnectionClosed
                )
        }
        BacktestClientError::Service(_) | BacktestClientError::Protocol(_) => false,
    }
}

fn store_error(error: crate::RunStoreError) -> WorkflowError {
    WorkflowError::TransitionStore {
        detail: error.to_string(),
    }
}

fn artifact_error(error: crate::ArtifactError) -> WorkflowError {
    WorkflowError::ArtifactIntegrity {
        detail: error.to_string(),
    }
}

fn output_error(error: crate::OutputError) -> WorkflowError {
    WorkflowError::Output {
        detail: error.to_string(),
    }
}

#[cfg(feature = "analysis")]
fn project_or_unavailable(
    result: &qs_backtest_api::BacktestResultMsg,
    options: &qs_backtest_api::ProviderEvaluationOptionsMsg,
) -> Result<(AnalysisDatasetState, PersistedExecutionDatasetState), WorkflowError> {
    crate::project_result_datasets(result, options).map_err(|error| WorkflowError::Analysis {
        detail: error.to_string(),
    })
}

#[cfg(not(feature = "analysis"))]
fn project_or_unavailable(
    _result: &qs_backtest_api::BacktestResultMsg,
    _options: &qs_backtest_api::ProviderEvaluationOptionsMsg,
) -> Result<(AnalysisDatasetState, PersistedExecutionDatasetState), WorkflowError> {
    Ok((
        AnalysisDatasetState::Unavailable {
            reason: crate::AnalysisUnavailableReason::AnalysisFeatureDisabled,
        },
        PersistedExecutionDatasetState::Unavailable {
            reason: crate::ExecutionDatasetUnavailableReason::AnalysisFeatureDisabled,
        },
    ))
}
