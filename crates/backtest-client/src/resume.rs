use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::BacktestRequestSummary;

pub const RESUME_RECORD_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultFileFormat {
    DocumentV1,
    LegacyBareResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputConflictPolicy {
    FailIfExists,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputTarget {
    pub path: PathBuf,
    pub format: ResultFileFormat,
    pub conflict: OutputConflictPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputIntent {
    Persist(OutputTarget),
    SummaryOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "intent", rename_all = "snake_case")]
pub enum OutputIntentSummary {
    Persist {
        path: PathBuf,
        format: ResultFileFormat,
        conflict: OutputConflictPolicy,
    },
    SummaryOnly,
}

impl From<&OutputIntent> for OutputIntentSummary {
    fn from(intent: &OutputIntent) -> Self {
        match intent {
            OutputIntent::Persist(target) => Self::Persist {
                path: target.path.clone(),
                format: target.format,
                conflict: target.conflict,
            },
            OutputIntent::SummaryOnly => Self::SummaryOnly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultInputMetadata {
    pub display_name: String,
    pub byte_len: u64,
    pub sha256: String,
    pub signal_count: u64,
    pub retained_signal_count: u64,
    pub entry_count: u64,
    pub symbols: Vec<String>,
    pub minimum_timestamp: Option<String>,
    pub maximum_timestamp: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultDeliverySummary {
    Auto,
    Inline,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientJobStatus {
    Queued,
    LoadingData,
    Running,
    Completed,
    Failed,
    Cancelled,
    NotFound,
    Unknown(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalCommitState {
    NotStarted,
    CommitPrepared,
    Committed,
}

/// Restart metadata that deliberately excludes raw signals and credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeRecord {
    pub format_version: u32,
    pub state_sequence: u64,
    pub endpoint_display: String,
    pub job_id: String,
    pub output: OutputIntentSummary,
    pub input: ResultInputMetadata,
    pub request: BacktestRequestSummary,
    pub result_delivery: ResultDeliverySummary,
    pub submitted_at: DateTime<Utc>,
    pub last_known_state: ClientJobStatus,
    pub local_commit: LocalCommitState,
    pub commit_intent_id: Option<String>,
    pub expected_document_sha256: Option<String>,
    pub committed_document_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RunStoreError {
    #[error("local run ID must not be empty")]
    InvalidLocalRunId,
    #[error("run transition conflict: expected sequence {expected}, actual sequence {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("next run transition sequence must be {expected}, got {actual}")]
    InvalidNextSequence { expected: u64, actual: u64 },
    #[error("run transition store lock is poisoned")]
    Poisoned,
}

#[async_trait]
pub trait RunTransitionStore: Send + Sync + 'static {
    async fn load(&self, local_run_id: &str) -> Result<Option<ResumeRecord>, RunStoreError>;

    async fn compare_and_swap(
        &self,
        local_run_id: &str,
        expected_sequence: u64,
        next: ResumeRecord,
    ) -> Result<(), RunStoreError>;
}

/// Process-lifetime transition store used by CLI composition and tests.
#[derive(Debug, Clone, Default)]
pub struct MemoryRunTransitionStore {
    records: Arc<Mutex<HashMap<String, ResumeRecord>>>,
}

#[async_trait]
impl RunTransitionStore for MemoryRunTransitionStore {
    async fn load(&self, local_run_id: &str) -> Result<Option<ResumeRecord>, RunStoreError> {
        validate_local_run_id(local_run_id)?;
        self.records
            .lock()
            .map_err(|_| RunStoreError::Poisoned)
            .map(|records| records.get(local_run_id).cloned())
    }

    async fn compare_and_swap(
        &self,
        local_run_id: &str,
        expected_sequence: u64,
        next: ResumeRecord,
    ) -> Result<(), RunStoreError> {
        validate_local_run_id(local_run_id)?;
        let mut records = self.records.lock().map_err(|_| RunStoreError::Poisoned)?;
        let actual_sequence = records
            .get(local_run_id)
            .map(|record| record.state_sequence)
            .unwrap_or(0);
        if actual_sequence != expected_sequence {
            return Err(RunStoreError::Conflict {
                expected: expected_sequence,
                actual: actual_sequence,
            });
        }
        let next_sequence = expected_sequence.saturating_add(1);
        if next.state_sequence != next_sequence {
            return Err(RunStoreError::InvalidNextSequence {
                expected: next_sequence,
                actual: next.state_sequence,
            });
        }
        records.insert(local_run_id.to_owned(), next);
        Ok(())
    }
}

fn validate_local_run_id(local_run_id: &str) -> Result<(), RunStoreError> {
    if local_run_id.trim().is_empty() {
        Err(RunStoreError::InvalidLocalRunId)
    } else {
        Ok(())
    }
}
