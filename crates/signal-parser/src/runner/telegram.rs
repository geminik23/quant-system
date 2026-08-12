//! Telegram provider binding for the logical ingestion service.

use std::sync::Arc;

use crate::adapters::telegram::{
    TelegramAdaptationOutcome, TelegramAdapterError, TelegramBatchPosition,
    TelegramBatchSourceAdapter, TelegramIgnoreReason, TelegramRelayInput,
    TelegramRelaySourceAdapter, TelegramSourceEvidenceV1,
};
use crate::ingestion::DateTimeUtc;
use crate::runner::service::{
    AdmissionIdentity, AdmissionIdentityError, AuthenticatedSourceContext, IngestionService,
    LogicalIngestionServiceError, SourceSubmission, SourceSubmissionResponse,
};

use crate::types::RawTgMessage;

/// Result of adapting one Telegram update for logical ingestion.
#[derive(Debug)]
pub enum TelegramIngestionOutcome {
    Submitted {
        evidence: TelegramSourceEvidenceV1,
        response: SourceSubmissionResponse,
    },
    Ignored {
        evidence: TelegramSourceEvidenceV1,
        reason: TelegramIgnoreReason,
    },
    Rejected {
        evidence: TelegramSourceEvidenceV1,
        diagnostic: crate::normalization::Diagnostic,
    },
}

/// Failure while adapting or submitting a Telegram update.
#[derive(Debug, thiserror::Error)]
pub enum TelegramIngestionBindingError {
    #[error(transparent)]
    Adapter(#[from] TelegramAdapterError),
    #[error(transparent)]
    AdmissionIdentity(#[from] AdmissionIdentityError),
    #[error(transparent)]
    Service(#[from] LogicalIngestionServiceError),
}

/// Adapts Telegram batch records and submits accepted events to a logical service.
pub struct TelegramBatchIngestionBinding {
    adapter: TelegramBatchSourceAdapter,
    service: Arc<dyn IngestionService>,
}

impl TelegramBatchIngestionBinding {
    pub fn new(adapter: TelegramBatchSourceAdapter, service: Arc<dyn IngestionService>) -> Self {
        Self { adapter, service }
    }

    pub async fn submit(
        &self,
        message: &RawTgMessage,
        received_at: DateTimeUtc,
        position: TelegramBatchPosition,
    ) -> Result<TelegramIngestionOutcome, TelegramIngestionBindingError> {
        submit_adaptation(
            self.service.as_ref(),
            self.adapter.adapt(message, received_at, position)?,
        )
        .await
    }
}

/// Adapts Telegram relay updates and submits each accepted event to a logical service.
pub struct TelegramRelayIngestionBinding {
    adapter: TelegramRelaySourceAdapter,
    service: Arc<dyn IngestionService>,
}

impl TelegramRelayIngestionBinding {
    pub fn new(adapter: TelegramRelaySourceAdapter, service: Arc<dyn IngestionService>) -> Self {
        Self { adapter, service }
    }

    pub async fn submit(
        &self,
        input: TelegramRelayInput,
        received_at: DateTimeUtc,
    ) -> Result<Vec<TelegramIngestionOutcome>, TelegramIngestionBindingError> {
        let adaptations = self.adapter.adapt(input, received_at)?;
        let mut outcomes = Vec::with_capacity(adaptations.len());
        for adaptation in adaptations {
            outcomes.push(submit_adaptation(self.service.as_ref(), adaptation).await?);
        }
        Ok(outcomes)
    }
}

async fn submit_adaptation(
    service: &dyn IngestionService,
    adaptation: TelegramAdaptationOutcome,
) -> Result<TelegramIngestionOutcome, TelegramIngestionBindingError> {
    match adaptation {
        TelegramAdaptationOutcome::Accepted {
            event,
            evidence,
            delivery_identity,
        } => {
            let admission_identity = AdmissionIdentity::from_delivery_identity(&delivery_identity)?;
            let authenticated_context =
                AuthenticatedSourceContext::try_new(event.key().source().clone(), "telegram", None)
                    .map_err(|_| AdmissionIdentityError)?;
            let response = service
                .submit(SourceSubmission {
                    admission_identity,
                    delivery_identity,
                    authenticated_context,
                    event: *event,
                })
                .await?;
            Ok(TelegramIngestionOutcome::Submitted { evidence, response })
        }
        TelegramAdaptationOutcome::Ignored { evidence, reason } => {
            Ok(TelegramIngestionOutcome::Ignored { evidence, reason })
        }
        TelegramAdaptationOutcome::Rejected {
            evidence,
            diagnostic,
        } => Ok(TelegramIngestionOutcome::Rejected {
            evidence,
            diagnostic,
        }),
    }
}
