use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::raw_signals_v1_schema;
use signal_parser::runner::service::{
    AdmissionIdentity, DurableIngestionService, IngestionService as _, OutcomeReference,
    SourceSubmission, SourceSubmissionDisposition,
};
use signal_parser::runner::{IngestionServiceConfig, structured_jsonl_service};
use signal_parser::state::{CommittedBatchId, MemorySourceStateStore, SourceStateStore};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("logical ingestion service must complete synchronously"),
    }
}

fn source_event() -> SourceEvent {
    let timestamp = DateTimeUtc::parse("2026-08-10T00:00:00Z").unwrap();
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("service:contract-test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(timestamp, SourceTimestampQuality::SourceProvided),
        timestamp,
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "CloseAll",
                        "ts": "2026-08-10T00:00:00Z"
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )),
    )
}

#[test]
fn logical_service_durably_submits_reconciles_and_looks_up_outcomes() {
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let runner = Arc::new(
        structured_jsonl_service(
            SourceId::new("service:contract-test").unwrap(),
            state.clone(),
            IngestionServiceConfig::default(),
        )
        .unwrap(),
    );
    let service = DurableIngestionService::new(runner, state);
    let event = source_event();

    let committed = block_on(
        service.submit(SourceSubmission {
            admission_identity: AdmissionIdentity::try_new("provider-admission-1").unwrap(),
            delivery_identity: signal_parser::state::DurableDeliveryIdentity::Stable(
                "provider-delivery-1".to_string(),
            ),
            authenticated_context:
                signal_parser::runner::service::AuthenticatedSourceContext::try_new(
                    SourceId::new("service:contract-test").unwrap(),
                    "service-test",
                    None,
                )
                .unwrap(),
            event: event.clone(),
        }),
    )
    .unwrap();
    assert_eq!(
        committed.disposition,
        SourceSubmissionDisposition::Committed
    );
    assert_eq!(
        committed.admission_identity.as_str(),
        "provider-admission-1"
    );
    let batch_id = CommittedBatchId::from_string_id(
        committed
            .outcome_reference
            .as_str()
            .strip_prefix("committed-batch:")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        OutcomeReference::from_batch_id(batch_id),
        committed.outcome_reference
    );

    let existing = block_on(
        service.submit(SourceSubmission {
            admission_identity: AdmissionIdentity::try_new("provider-admission-1").unwrap(),
            delivery_identity: signal_parser::state::DurableDeliveryIdentity::Stable(
                "provider-delivery-1".to_string(),
            ),
            authenticated_context:
                signal_parser::runner::service::AuthenticatedSourceContext::try_new(
                    SourceId::new("service:contract-test").unwrap(),
                    "service-test",
                    None,
                )
                .unwrap(),
            event,
        }),
    )
    .unwrap();
    assert_eq!(existing.disposition, SourceSubmissionDisposition::Existing);
    assert_eq!(existing.outcome_reference, committed.outcome_reference);

    let outcome = block_on(service.outcome(committed.outcome_reference))
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome,
        signal_parser::runner::service::SourceSubmissionOutcome::Committed {
            outcome: signal_parser::state::CommittedNormalizationOutcome::Accepted { .. },
            ..
        }
    ));
}
