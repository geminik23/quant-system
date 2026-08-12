use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::Duration;
use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::raw_signals_v1_schema;
use signal_parser::runner::{IngestionServiceConfig, structured_jsonl_service};
use signal_parser::state::{DurableDeliveryIdentity, MemorySourceStateStore, SourceStateStore};

use signal_parser::runner::{
    CommittedBatchSink, PublicationSinkError,
    publication::{
        DeliveryAcknowledgementPolicy, PublicationDeliveryReceipt, PublicationOrchestrator,
        PublicationRetryPolicy,
    },
};

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn event(external_id: &str) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("publication:contract-test").unwrap(),
            ExternalEventId::new(external_id).unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-08T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-08T00:00:01Z"),
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "CloseAll",
                        "ts": "2026-08-08T00:00:00Z"
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )),
    )
}

enum SinkResult {
    RetryableFailure,
    Success,
    SlowSuccess,
    InconsistentReceipt,
}

struct ScriptedSink {
    results: Mutex<VecDeque<SinkResult>>,
    published: Mutex<usize>,
}

impl ScriptedSink {
    fn new(results: impl IntoIterator<Item = SinkResult>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            published: Mutex::new(0),
        }
    }
}

impl CommittedBatchSink for ScriptedSink {
    fn acknowledgement_policy(&self) -> DeliveryAcknowledgementPolicy {
        DeliveryAcknowledgementPolicy::IdempotentByDeliveryId
    }

    fn publish(
        &self,
        delivery: signal_parser::runner::publication::CommittedDelivery<'_>,
    ) -> Result<PublicationDeliveryReceipt, PublicationSinkError> {
        match self.results.lock().unwrap().pop_front().unwrap() {
            SinkResult::RetryableFailure => Err(PublicationSinkError::Io(std::io::Error::other(
                "unavailable",
            ))),
            SinkResult::Success => {
                *self.published.lock().unwrap() += 1;
                Ok(PublicationDeliveryReceipt {
                    delivery_id: delivery.delivery_id,
                    batch_id: delivery.batch.batch_id,
                })
            }
            SinkResult::SlowSuccess => {
                std::thread::sleep(std::time::Duration::from_millis(5));
                Ok(PublicationDeliveryReceipt {
                    delivery_id: delivery.delivery_id,
                    batch_id: delivery.batch.batch_id,
                })
            }
            SinkResult::InconsistentReceipt => Ok(PublicationDeliveryReceipt {
                delivery_id: delivery.delivery_id,
                batch_id: signal_parser::state::CommittedBatchId::from_bytes([0; 32]),
            }),
        }
    }
}

#[test]
fn publication_retries_then_dead_letters_and_acknowledges_successes() {
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let service = structured_jsonl_service(
        SourceId::new("publication:contract-test").unwrap(),
        state.clone(),
        IngestionServiceConfig {
            publication_sink: Some("contract-sink".to_string()),
            ..IngestionServiceConfig::default()
        },
    )
    .unwrap();
    for external_id in ["event-1", "event-2"] {
        service
            .submit(
                event(external_id),
                DurableDeliveryIdentity::Stable(format!("delivery-{external_id}")),
            )
            .unwrap();
    }

    let sink = Arc::new(ScriptedSink::new([
        SinkResult::RetryableFailure,
        SinkResult::Success,
        SinkResult::RetryableFailure,
    ]));
    let runner = PublicationOrchestrator::new(
        state.clone(),
        sink.clone(),
        PublicationRetryPolicy::new(2, Duration::minutes(1)).unwrap(),
    );

    let first = runner.run_at(8, timestamp("2026-08-08T00:01:00Z")).unwrap();
    assert_eq!(first.leased, 2);
    assert_eq!(first.acknowledged, 1);
    assert_eq!(first.retries_scheduled, 1);
    assert_eq!(first.dead_lettered, 0);

    let second = runner.run_at(8, timestamp("2026-08-08T00:01:01Z")).unwrap();
    assert_eq!(second.leased, 1);
    assert_eq!(second.acknowledged, 0);
    assert_eq!(second.retries_scheduled, 0);
    assert_eq!(second.dead_lettered, 1);
    assert_eq!(*sink.published.lock().unwrap(), 1);
    assert!(
        state
            .lease_publications(
                8,
                timestamp("2026-08-08T00:05:00Z"),
                timestamp("2026-08-08T00:06:00Z"),
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn retry_backoff_is_exponential_and_capped() {
    let policy = PublicationRetryPolicy::with_operational_limits(
        4,
        Duration::minutes(1),
        Duration::seconds(1),
        Duration::milliseconds(10),
        Duration::milliseconds(25),
        Duration::minutes(5),
    )
    .unwrap();

    assert_eq!(policy.backoff_for_attempt(1), Duration::milliseconds(10));
    assert_eq!(policy.backoff_for_attempt(2), Duration::milliseconds(20));
    assert_eq!(policy.backoff_for_attempt(3), Duration::milliseconds(25));
    assert_eq!(
        policy.backoff_for_attempt(u32::MAX),
        Duration::milliseconds(25)
    );
}

#[test]
fn deadline_overrun_is_unknown_and_receipt_mismatch_is_not_acknowledged() {
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let service = structured_jsonl_service(
        SourceId::new("publication:contract-test").unwrap(),
        state.clone(),
        IngestionServiceConfig {
            publication_sink: Some("contract-sink".to_string()),
            ..IngestionServiceConfig::default()
        },
    )
    .unwrap();
    service
        .submit(
            event("deadline"),
            DurableDeliveryIdentity::Stable("delivery-deadline".to_string()),
        )
        .unwrap();
    service
        .submit(
            event("receipt"),
            DurableDeliveryIdentity::Stable("delivery-receipt".to_string()),
        )
        .unwrap();

    let runner = PublicationOrchestrator::new(
        state.clone(),
        Arc::new(ScriptedSink::new([
            SinkResult::SlowSuccess,
            SinkResult::InconsistentReceipt,
        ])),
        PublicationRetryPolicy::with_operational_limits(
            2,
            Duration::minutes(1),
            Duration::milliseconds(1),
            Duration::seconds(1),
            Duration::seconds(1),
            Duration::minutes(5),
        )
        .unwrap(),
    );

    let report = runner.run_at(8, timestamp("2026-08-08T00:01:00Z")).unwrap();
    assert_eq!(report.unknown_outcomes, 1);
    assert_eq!(report.retries_scheduled, 1);
    assert_eq!(report.acknowledged, 0);
    assert_eq!(report.dead_lettered, 1);
    assert!(
        state
            .lease_publications(
                8,
                timestamp("2026-08-08T00:01:00Z"),
                timestamp("2026-08-08T00:02:00Z"),
            )
            .unwrap()
            .is_empty()
    );
}
