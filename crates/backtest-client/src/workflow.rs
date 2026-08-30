use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};

pub const DEFAULT_COMMAND_CAPACITY: usize = 8;
pub const MAX_COMMAND_CAPACITY: usize = 64;
pub const DEFAULT_EVENT_CAPACITY: usize = 64;
pub const MAX_EVENT_CAPACITY: usize = 4_096;

/// Bounded command and semantic-event channel configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowChannelConfig {
    command_capacity: usize,
    event_capacity: usize,
}

impl WorkflowChannelConfig {
    pub fn new(command_capacity: usize, event_capacity: usize) -> Result<Self, ChannelConfigError> {
        if !(1..=MAX_COMMAND_CAPACITY).contains(&command_capacity) {
            return Err(ChannelConfigError::CommandCapacity {
                actual: command_capacity,
            });
        }
        if !(1..=MAX_EVENT_CAPACITY).contains(&event_capacity) {
            return Err(ChannelConfigError::EventCapacity {
                actual: event_capacity,
            });
        }
        Ok(Self {
            command_capacity,
            event_capacity,
        })
    }

    pub fn command_capacity(self) -> usize {
        self.command_capacity
    }

    pub fn event_capacity(self) -> usize {
        self.event_capacity
    }
}

impl Default for WorkflowChannelConfig {
    fn default() -> Self {
        Self {
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            event_capacity: DEFAULT_EVENT_CAPACITY,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChannelConfigError {
    #[error("command capacity must be between 1 and {MAX_COMMAND_CAPACITY}, got {actual}")]
    CommandCapacity { actual: usize },
    #[error("event capacity must be between 1 and {MAX_EVENT_CAPACITY}, got {actual}")]
    EventCapacity { actual: usize },
}

/// Fixed retained-job reconnect policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconnectPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectObservation {
    ConnectorConnected,
    ActiveStatus,
    ValidWatchSnapshot,
    ReadSucceeded,
    Detach,
    Cancel,
    Shutdown,
    TerminalStatus,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectBackoffAction {
    Keep,
    Reset,
    Stop,
}

impl ReconnectPolicy {
    /// Return the delay for a one-based reconnect attempt.
    pub fn delay_for_attempt(self, attempt: u32) -> Duration {
        match attempt {
            0 | 1 => Duration::from_millis(500),
            2 => Duration::from_secs(1),
            3 => Duration::from_secs(2),
            4 => Duration::from_secs(5),
            _ => Duration::from_secs(10),
        }
    }

    pub fn has_jitter(self) -> bool {
        false
    }

    pub fn maximum_attempts(self) -> Option<u32> {
        None
    }

    pub fn maximum_elapsed(self) -> Option<Duration> {
        None
    }

    pub fn retries_initial_connect(self) -> bool {
        false
    }

    pub fn action_after(self, observation: ReconnectObservation) -> ReconnectBackoffAction {
        match observation {
            ReconnectObservation::ConnectorConnected | ReconnectObservation::ActiveStatus => {
                ReconnectBackoffAction::Keep
            }
            ReconnectObservation::ValidWatchSnapshot | ReconnectObservation::ReadSucceeded => {
                ReconnectBackoffAction::Reset
            }
            ReconnectObservation::Detach
            | ReconnectObservation::Cancel
            | ReconnectObservation::Shutdown
            | ReconnectObservation::TerminalStatus
            | ReconnectObservation::NotFound => ReconnectBackoffAction::Stop,
        }
    }
}

#[async_trait]
pub trait WorkflowSleeper: Send + Sync + 'static {
    async fn sleep(&self, duration: Duration);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TokioWorkflowSleeper;

#[async_trait]
impl WorkflowSleeper for TokioWorkflowSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowCommand {
    Cancel,
    Detach,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Created,
    Connecting,
    Submitting,
    Submitted,
    Watching,
    Reconnecting,
    FetchingResult,
    DownloadingArtifact,
    ValidatingOutput,
    CommittingOutput,
    CancelRequested,
    CompletedPersisted,
    CompletedSummaryOnly,
    SubmissionUncertain,
    Detached,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestRunSnapshot {
    pub state_sequence: u64,
    pub dropped_event_count: u64,
    pub state: WorkflowState,
    pub job_id: Option<String>,
    pub reconnect_attempt: u32,
    pub current_warning: Option<String>,
    pub current_error: Option<String>,
}

impl Default for BacktestRunSnapshot {
    fn default() -> Self {
        Self {
            state_sequence: 0,
            dropped_event_count: 0,
            state: WorkflowState::Created,
            job_id: None,
            reconnect_attempt: 0,
            current_warning: None,
            current_error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BacktestWorkflowEventKind {
    StateChanged(WorkflowState),
    Reconnecting { attempt: u32, delay: Duration },
    WarningChanged,
    ResumeRecordChanged,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestWorkflowEvent {
    pub state_sequence: u64,
    pub kind: BacktestWorkflowEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowCompletion {
    CompletedPersisted,
    CompletedSummaryOnly,
    SubmissionUncertain,
    Detached,
    Cancelled,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkflowChannelError {
    #[error("workflow command receiver is closed")]
    CommandClosed,
    #[error("workflow completion sender was dropped")]
    CompletionClosed,
    #[error("workflow completion receiver was already consumed")]
    CompletionAlreadyTaken,
}

/// Caller-owned channels for commands and authoritative observation.
#[allow(dead_code)]
pub struct WorkflowFrontendChannels {
    command_tx: mpsc::Sender<WorkflowCommand>,
    snapshot_rx: watch::Receiver<BacktestRunSnapshot>,
    event_rx: Option<mpsc::Receiver<BacktestWorkflowEvent>>,
    completion_rx: Option<oneshot::Receiver<WorkflowCompletion>>,
}

#[allow(dead_code)]
impl WorkflowFrontendChannels {
    pub async fn send_command(&self, command: WorkflowCommand) -> Result<(), WorkflowChannelError> {
        self.command_tx
            .send(command)
            .await
            .map_err(|_| WorkflowChannelError::CommandClosed)
    }

    pub fn snapshot(&self) -> BacktestRunSnapshot {
        self.snapshot_rx.borrow().clone()
    }

    pub fn subscribe_snapshot(&self) -> watch::Receiver<BacktestRunSnapshot> {
        self.snapshot_rx.clone()
    }

    pub fn take_events(&mut self) -> Option<mpsc::Receiver<BacktestWorkflowEvent>> {
        self.event_rx.take()
    }

    pub async fn join(&mut self) -> Result<WorkflowCompletion, WorkflowChannelError> {
        let receiver = self
            .completion_rx
            .take()
            .ok_or(WorkflowChannelError::CompletionAlreadyTaken)?;
        receiver
            .await
            .map_err(|_| WorkflowChannelError::CompletionClosed)
    }
}

/// Actor-owned channels. Semantic event publication never waits on the consumer.
#[allow(dead_code)]
pub struct WorkflowActorChannels {
    command_rx: mpsc::Receiver<WorkflowCommand>,
    snapshot_tx: watch::Sender<BacktestRunSnapshot>,
    event_tx: mpsc::Sender<BacktestWorkflowEvent>,
    completion_tx: Option<oneshot::Sender<WorkflowCompletion>>,
    snapshot: BacktestRunSnapshot,
}

#[allow(dead_code)]
impl WorkflowActorChannels {
    pub async fn recv_command(&mut self) -> Option<WorkflowCommand> {
        self.command_rx.recv().await
    }

    pub fn snapshot(&self) -> &BacktestRunSnapshot {
        &self.snapshot
    }

    pub fn update_snapshot(&mut self, update: impl FnOnce(&mut BacktestRunSnapshot)) {
        update(&mut self.snapshot);
        self.snapshot.state_sequence = self.snapshot.state_sequence.saturating_add(1);
        self.snapshot_tx.send_replace(self.snapshot.clone());
    }

    /// Publish a best-effort transition notification after authoritative state is updated.
    pub fn publish_event(&mut self, kind: BacktestWorkflowEventKind) -> bool {
        let event = BacktestWorkflowEvent {
            state_sequence: self.snapshot.state_sequence,
            kind,
        };
        match self.event_tx.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.snapshot.dropped_event_count =
                    self.snapshot.dropped_event_count.saturating_add(1);
                self.snapshot.state_sequence = self.snapshot.state_sequence.saturating_add(1);
                self.snapshot_tx.send_replace(self.snapshot.clone());
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    pub fn complete(&mut self, completion: WorkflowCompletion) {
        if let Some(sender) = self.completion_tx.take() {
            let _ = sender.send(completion);
        }
        let _ = self.publish_event(BacktestWorkflowEventKind::Completed);
    }
}

#[allow(dead_code)]
pub fn workflow_channel_pair(
    config: WorkflowChannelConfig,
    initial_snapshot: BacktestRunSnapshot,
) -> (WorkflowFrontendChannels, WorkflowActorChannels) {
    let (command_tx, command_rx) = mpsc::channel(config.command_capacity());
    let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot.clone());
    let (event_tx, event_rx) = mpsc::channel(config.event_capacity());
    let (completion_tx, completion_rx) = oneshot::channel();
    (
        WorkflowFrontendChannels {
            command_tx,
            snapshot_rx,
            event_rx: Some(event_rx),
            completion_rx: Some(completion_rx),
        },
        WorkflowActorChannels {
            command_rx,
            snapshot_tx,
            event_tx,
            completion_tx: Some(completion_tx),
            snapshot: initial_snapshot,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn command_backpressure_and_event_snapshot_recovery_are_bounded() {
        let config = WorkflowChannelConfig::new(1, 1).unwrap();
        let (mut frontend, mut actor) =
            workflow_channel_pair(config, BacktestRunSnapshot::default());

        frontend
            .send_command(WorkflowCommand::Cancel)
            .await
            .unwrap();
        {
            let second_send = frontend.send_command(WorkflowCommand::Detach);
            tokio::pin!(second_send);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut second_send)
                    .await
                    .is_err()
            );
            assert_eq!(actor.recv_command().await, Some(WorkflowCommand::Cancel));
            second_send.await.unwrap();
            assert_eq!(actor.recv_command().await, Some(WorkflowCommand::Detach));
        }

        actor.update_snapshot(|snapshot| snapshot.state = WorkflowState::Connecting);
        assert!(actor.publish_event(BacktestWorkflowEventKind::StateChanged(
            WorkflowState::Connecting
        )));
        actor.update_snapshot(|snapshot| snapshot.state = WorkflowState::Submitting);
        assert!(
            !actor.publish_event(BacktestWorkflowEventKind::StateChanged(
                WorkflowState::Submitting
            ))
        );
        assert_eq!(frontend.snapshot().state, WorkflowState::Submitting);
        assert_eq!(frontend.snapshot().dropped_event_count, 1);
        assert_eq!(actor.snapshot().dropped_event_count, 1);

        let mut snapshot_rx = frontend.subscribe_snapshot();
        snapshot_rx.mark_changed();
        let mut events = frontend.take_events().unwrap();
        let first = events.recv().await.unwrap();
        assert!(matches!(
            first.kind,
            BacktestWorkflowEventKind::StateChanged(WorkflowState::Connecting)
        ));

        actor.update_snapshot(|snapshot| snapshot.state = WorkflowState::CompletedSummaryOnly);
        actor.complete(WorkflowCompletion::CompletedSummaryOnly);
        assert_eq!(
            frontend.join().await.unwrap(),
            WorkflowCompletion::CompletedSummaryOnly
        );
    }
}
