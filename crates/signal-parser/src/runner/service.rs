//! Provider-neutral logical service contract for durable source-event ingestion.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::ingestion::{SourceEvent, SourceEventRef, SourceId};
use crate::normalization::AuthorClass;
use crate::runner::{
    IngestionService as RunnerIngestionService, IngestionServiceError, IngestionSubmissionOutcome,
};
use crate::state::{
    CommittedBatchId, CommittedNormalizationOutcome, DurableDeliveryIdentity, SourceStateError,
    SourceStateStore,
};

const MAX_ADMISSION_IDENTITY_BYTES: usize = 256;
const OUTCOME_REFERENCE_PREFIX: &str = "committed-batch:";
const ADMISSION_REFERENCE_PREFIX: &str = "admission:";

/// Stable provider admission identity used to reconcile a submitted source event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AdmissionIdentity(String);

impl AdmissionIdentity {
    pub fn try_new(value: impl Into<String>) -> Result<Self, AdmissionIdentityError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_ADMISSION_IDENTITY_BYTES
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(AdmissionIdentityError);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn durable_delivery_identity(&self) -> DurableDeliveryIdentity {
        DurableDeliveryIdentity::Stable(self.0.clone())
    }

    /// Derives a stable service identity without losing an offline artifact position.
    pub fn from_delivery_identity(
        identity: &DurableDeliveryIdentity,
    ) -> Result<Self, AdmissionIdentityError> {
        match identity {
            DurableDeliveryIdentity::Stable(value) => Self::try_new(value.clone()),
            DurableDeliveryIdentity::OfflinePosition { artifact, ordinal } => {
                Self::try_new(format!("offline-admission:v1:{artifact}:{ordinal}"))
            }
            DurableDeliveryIdentity::StoreReceipt(receipt) => {
                Self::try_new(format!("store-receipt-admission:v1:{receipt}"))
            }
        }
    }
}

/// Returned when an admission identity cannot safely identify a durable request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("admission identity is invalid")]
pub struct AdmissionIdentityError;

/// Provider-neutral request for durable source-event application.
#[derive(Debug, Clone)]
pub struct SourceSubmission {
    pub admission_identity: AdmissionIdentity,
    pub delivery_identity: DurableDeliveryIdentity,
    pub authenticated_context: AuthenticatedSourceContext,
    pub event: SourceEvent,
}

/// Bounded provider-neutral source facts established before durable admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedSourceContext {
    pub source_id: SourceId,
    pub source_binding_id: String,
    pub author_class: Option<AuthorClass>,
}

impl AuthenticatedSourceContext {
    pub fn try_new(
        source_id: SourceId,
        source_binding_id: impl Into<String>,
        author_class: Option<AuthorClass>,
    ) -> Result<Self, AuthenticatedSourceContextError> {
        let source_binding_id = source_binding_id.into();
        if source_binding_id.is_empty()
            || source_binding_id.len() > 128
            || !source_binding_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(AuthenticatedSourceContextError::InvalidBindingId);
        }
        Ok(Self {
            source_id,
            source_binding_id,
            author_class,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthenticatedSourceContextError {
    #[error("authenticated source binding ID is invalid")]
    InvalidBindingId,
}

/// Stable reference to an immutable committed normalization outcome.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutcomeReference(String);

impl OutcomeReference {
    pub fn from_batch_id(batch_id: CommittedBatchId) -> Self {
        Self(format!(
            "{OUTCOME_REFERENCE_PREFIX}{}",
            batch_id.to_string_id()
        ))
    }

    pub fn from_admission_identity(identity: &AdmissionIdentity) -> Self {
        Self(format!("{ADMISSION_REFERENCE_PREFIX}{}", identity.as_str()))
    }

    pub fn try_new(value: impl Into<String>) -> Result<Self, OutcomeReferenceError> {
        let value = value.into();
        if let Some(batch_id) = value.strip_prefix(OUTCOME_REFERENCE_PREFIX) {
            CommittedBatchId::from_string_id(batch_id).map_err(|_| OutcomeReferenceError)?;
            return Ok(Self(value));
        }
        if let Some(identity) = value.strip_prefix(ADMISSION_REFERENCE_PREFIX) {
            AdmissionIdentity::try_new(identity.to_string()).map_err(|_| OutcomeReferenceError)?;
            return Ok(Self(value));
        }
        Err(OutcomeReferenceError)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn admission_identity(&self) -> Option<AdmissionIdentity> {
        self.0
            .strip_prefix(ADMISSION_REFERENCE_PREFIX)
            .and_then(|value| AdmissionIdentity::try_new(value.to_string()).ok())
    }

    pub fn committed_batch_id(&self) -> Result<CommittedBatchId, OutcomeReferenceError> {
        let batch_id = self
            .0
            .strip_prefix(OUTCOME_REFERENCE_PREFIX)
            .ok_or(OutcomeReferenceError)?;
        CommittedBatchId::from_string_id(batch_id).map_err(|_| OutcomeReferenceError)
    }
}

/// Returned when an outcome reference does not identify a committed batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("outcome reference is invalid")]
pub struct OutcomeReferenceError;

/// Durable submission result independent of any provider acknowledgement format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSubmissionResponse {
    pub admission_identity: AdmissionIdentity,
    pub source: SourceEventRef,
    pub disposition: SourceSubmissionDisposition,
    pub outcome_reference: OutcomeReference,
}

/// Whether the submission created a committed batch or reconciled an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSubmissionDisposition {
    Accepted,
    Committed,
    Existing,
}

/// Immutable committed outcome available after durable application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceSubmissionOutcome {
    Pending {
        reference: OutcomeReference,
        source: SourceEventRef,
    },
    Committed {
        reference: OutcomeReference,
        source: SourceEventRef,
        outcome: CommittedNormalizationOutcome,
    },
}

/// Provider-neutral application error vocabulary.
#[derive(Debug, thiserror::Error)]
pub enum LogicalIngestionServiceError {
    #[error("authenticated source does not match the submitted event")]
    SourceMismatch,
    #[error("durable application requires retry")]
    RetryRequired,
    #[error("durable ingestion state is unavailable")]
    Unavailable,
    #[error(transparent)]
    InvalidOutcomeReference(#[from] OutcomeReferenceError),
}

/// Object-safe async logical ingestion service contract.
pub trait IngestionService: Send + Sync {
    fn submit<'a>(
        &'a self,
        submission: SourceSubmission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SourceSubmissionResponse, LogicalIngestionServiceError>>
                + Send
                + 'a,
        >,
    >;

    fn outcome<'a>(
        &'a self,
        reference: OutcomeReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<SourceSubmissionOutcome>, LogicalIngestionServiceError>,
                > + Send
                + 'a,
        >,
    >;
}

/// Provider-neutral service facade over the durable ingestion runner.
pub struct DurableIngestionService {
    runner: Arc<RunnerIngestionService>,
    state: Arc<dyn SourceStateStore>,
}

impl DurableIngestionService {
    pub fn new(runner: Arc<RunnerIngestionService>, state: Arc<dyn SourceStateStore>) -> Self {
        Self { runner, state }
    }
}

impl IngestionService for DurableIngestionService {
    fn submit<'a>(
        &'a self,
        submission: SourceSubmission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SourceSubmissionResponse, LogicalIngestionServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if submission.authenticated_context.source_id != *submission.event.key().source() {
                return Err(LogicalIngestionServiceError::SourceMismatch);
            }
            let source = SourceEventRef::from(&submission.event);
            match self
                .runner
                .submit_with_author_class(
                    submission.event,
                    submission.delivery_identity,
                    submission.authenticated_context.author_class,
                )
                .map_err(map_runner_error)?
            {
                IngestionSubmissionOutcome::Committed { batch_id } => {
                    Ok(SourceSubmissionResponse {
                        admission_identity: submission.admission_identity,
                        source,
                        disposition: SourceSubmissionDisposition::Committed,
                        outcome_reference: OutcomeReference::from_batch_id(batch_id),
                    })
                }
                IngestionSubmissionOutcome::Existing { batch_id } => Ok(SourceSubmissionResponse {
                    admission_identity: submission.admission_identity,
                    source,
                    disposition: SourceSubmissionDisposition::Existing,
                    outcome_reference: OutcomeReference::from_batch_id(batch_id),
                }),
                IngestionSubmissionOutcome::RetryRequired => {
                    Err(LogicalIngestionServiceError::RetryRequired)
                }
            }
        })
    }

    fn outcome<'a>(
        &'a self,
        reference: OutcomeReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<SourceSubmissionOutcome>, LogicalIngestionServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let batch_id = reference.committed_batch_id()?;
            self.state
                .committed_batch(batch_id)
                .map_err(map_state_error)
                .map(|batch| {
                    batch.map(|batch| SourceSubmissionOutcome::Committed {
                        reference,
                        source: batch.source,
                        outcome: batch.outcome,
                    })
                })
        })
    }
}

fn map_runner_error(error: IngestionServiceError) -> LogicalIngestionServiceError {
    match error {
        IngestionServiceError::State(SourceStateError::CompareConflict)
        | IngestionServiceError::State(SourceStateError::FenceLost) => {
            LogicalIngestionServiceError::RetryRequired
        }
        IngestionServiceError::State(error) => map_state_error(error),
        IngestionServiceError::InvalidOutcomeReference => LogicalIngestionServiceError::Unavailable,
    }
}

fn map_state_error(_error: SourceStateError) -> LogicalIngestionServiceError {
    LogicalIngestionServiceError::Unavailable
}
