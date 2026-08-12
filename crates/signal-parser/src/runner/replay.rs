//! Deterministic causal replay of immutable source-event receipts.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::ingestion::SourceRevision;
use crate::runner::manifest::{ManifestError, ResolvedRunnerManifest};
use crate::runner::{IngestionService, IngestionServiceError, IngestionSubmissionOutcome};
use crate::state::{
    AdmittedExecutionIdentity, AdmittedSourceAdapter, SourceStateError, SourceStateStore,
};

/// Replays recorded source receipts into an independently configured ingestion service.
pub struct ReplayIngestionRunner {
    authoritative_state: Arc<dyn SourceStateStore>,
    target_service: Arc<IngestionService>,
    expected_source_adapter: AdmittedSourceAdapter,
    expected_execution_identity: AdmittedExecutionIdentity,
    maximum_receipts: usize,
}

impl ReplayIngestionRunner {
    pub fn new(
        authoritative_state: Arc<dyn SourceStateStore>,
        target_service: Arc<IngestionService>,
        manifest: &ResolvedRunnerManifest,
        maximum_receipts: usize,
    ) -> Result<Self, ReplayIngestionError> {
        if maximum_receipts == 0 {
            return Err(ReplayIngestionError::InvalidReceiptLimit);
        }
        let expected_source_adapter =
            AdmittedSourceAdapter::from(&manifest.source_adapter_identity()?);
        let target_source_adapter =
            AdmittedSourceAdapter::from(target_service.source_adapter_identity());
        if target_source_adapter != expected_source_adapter {
            return Err(ReplayIngestionError::TargetAdapterUnavailable);
        }
        let expected_execution_identity = manifest.execution_identity.clone();
        if target_service.execution_identity() != Some(&expected_execution_identity) {
            return Err(ReplayIngestionError::TargetExecutionUnavailable);
        }
        Ok(Self {
            authoritative_state,
            target_service,
            expected_source_adapter,
            expected_execution_identity,
            maximum_receipts,
        })
    }

    pub fn run(&self) -> Result<ReplayIngestionReport, ReplayIngestionError> {
        let mut receipts = self.authoritative_state.recorded_receipts()?;
        validate_intake_indexes(&receipts)?;
        receipts.sort_by_key(|receipt| (receipt.available_at, receipt.intake_index));
        validate_causal_revisions(&receipts)?;
        validate_recorded_adapter_identities(&receipts, &self.expected_source_adapter)?;
        validate_recorded_execution_identities(&receipts, &self.expected_execution_identity)?;

        let available_receipts = receipts.len();
        let replayed_receipts = available_receipts.min(self.maximum_receipts);
        let mut report = ReplayIngestionReport {
            available_receipts,
            replayed_receipts,
            committed_receipts: 0,
            existing_receipts: 0,
            retry_required_receipts: 0,
            truncated_receipts: available_receipts.saturating_sub(replayed_receipts),
        };

        for receipt in receipts.into_iter().take(self.maximum_receipts) {
            match self
                .target_service
                .submit(receipt.event, receipt.delivery_identity)?
            {
                IngestionSubmissionOutcome::Committed { .. } => report.committed_receipts += 1,
                IngestionSubmissionOutcome::Existing { .. } => report.existing_receipts += 1,
                IngestionSubmissionOutcome::RetryRequired => report.retry_required_receipts += 1,
            }
        }
        Ok(report)
    }
}

/// Bounded accounting for one immutable receipt replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayIngestionReport {
    pub available_receipts: usize,
    pub replayed_receipts: usize,
    pub committed_receipts: usize,
    pub existing_receipts: usize,
    pub retry_required_receipts: usize,
    pub truncated_receipts: usize,
}

/// Replay failures that leave the authoritative receipt store unchanged.
#[derive(Debug, thiserror::Error)]
pub enum ReplayIngestionError {
    #[error("replay receipt limit must be greater than zero")]
    InvalidReceiptLimit,
    #[error("recorded receipt intake indexes must be contiguous starting at one")]
    InvalidIntakeIndexes,
    #[error(
        "recorded receipt order regresses a monotonic source revision from intake {previous_intake_index} to {current_intake_index}"
    )]
    InvalidReceiptOrder {
        previous_intake_index: u64,
        current_intake_index: u64,
    },
    #[error("the resolved replay adapter is unavailable in the target runner")]
    TargetAdapterUnavailable,
    #[error(
        "recorded receipt at intake {intake_index} uses an adapter identity unavailable to replay"
    )]
    RecordedAdapterUnavailable { intake_index: u64 },
    #[error(
        "the resolved replay graph, pipeline, decoder, or finalizer is unavailable in the target runner"
    )]
    TargetExecutionUnavailable,
    #[error(
        "recorded receipt at intake {intake_index} uses a graph, pipeline, decoder, or finalizer identity unavailable to replay"
    )]
    RecordedExecutionUnavailable { intake_index: u64 },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    State(#[from] SourceStateError),
    #[error(transparent)]
    Service(#[from] IngestionServiceError),
}

fn validate_intake_indexes(
    receipts: &[crate::state::RecordedReceipt],
) -> Result<(), ReplayIngestionError> {
    let mut indexes = receipts
        .iter()
        .map(|receipt| receipt.intake_index)
        .collect::<Vec<_>>();
    indexes.sort_unstable();
    if indexes
        .iter()
        .enumerate()
        .any(|(position, index)| *index != position as u64 + 1)
    {
        return Err(ReplayIngestionError::InvalidIntakeIndexes);
    }
    Ok(())
}

fn validate_recorded_adapter_identities(
    receipts: &[crate::state::RecordedReceipt],
    expected_source_adapter: &AdmittedSourceAdapter,
) -> Result<(), ReplayIngestionError> {
    for receipt in receipts {
        if receipt.source_adapter != *expected_source_adapter {
            return Err(ReplayIngestionError::RecordedAdapterUnavailable {
                intake_index: receipt.intake_index,
            });
        }
    }
    Ok(())
}

fn validate_recorded_execution_identities(
    receipts: &[crate::state::RecordedReceipt],
    expected_execution_identity: &AdmittedExecutionIdentity,
) -> Result<(), ReplayIngestionError> {
    for receipt in receipts {
        if receipt.execution_identity.as_ref() != Some(expected_execution_identity) {
            return Err(ReplayIngestionError::RecordedExecutionUnavailable {
                intake_index: receipt.intake_index,
            });
        }
    }
    Ok(())
}

fn validate_causal_revisions(
    receipts: &[crate::state::RecordedReceipt],
) -> Result<(), ReplayIngestionError> {
    let mut latest = BTreeMap::new();
    for receipt in receipts {
        let SourceRevision::Monotonic(revision) = receipt.event.revision() else {
            continue;
        };
        let key = format!(
            "{}:{}",
            receipt.event.key().source().as_str(),
            receipt.event.key().external_id().as_str()
        );
        if let Some((previous_revision, previous_intake_index)) = latest.get(&key)
            && revision < previous_revision
        {
            return Err(ReplayIngestionError::InvalidReceiptOrder {
                previous_intake_index: *previous_intake_index,
                current_intake_index: receipt.intake_index,
            });
        }
        latest.insert(key, (*revision, receipt.intake_index));
    }
    Ok(())
}
