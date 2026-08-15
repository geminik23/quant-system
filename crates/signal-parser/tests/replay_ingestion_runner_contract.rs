use std::sync::Arc;

use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::{
    ComponentId, SemanticVersion, SourceAdapterIdentity, raw_signals_v1_schema,
};
use signal_parser::runner::manifest::{
    BUILTIN_CANONICAL_RAW_SIGNALS_DECODER, BUILTIN_COMMITTED_JSONL_SINK,
    BUILTIN_SOURCE_EVENT_JSONL_ADAPTER, BUILTIN_SQLITE_STATE, BUILTIN_STANDARD_SIGNAL_FINALIZER,
    COMMITTED_NORMALIZATION_JSONL_CODEC, ResolvedRunnerManifest, RunnerManifest,
    SOURCE_EVENT_JSONL_CODEC,
};
use signal_parser::runner::replay::{ReplayIngestionError, ReplayIngestionRunner};
use signal_parser::runner::{IngestionServiceConfig, structured_jsonl_service};
use signal_parser::state::{
    AdmittedExecutionIdentity, DurableDeliveryIdentity, MemorySourceStateStore, SourceStateStore,
};

fn event(external_id: &str, revision: u64, received_at: &str) -> SourceEvent {
    let received_at = DateTimeUtc::parse(received_at).unwrap();
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("replay:contract-test").unwrap(),
            ExternalEventId::new(external_id).unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(revision),
        SourceTimestamp::new(received_at, SourceTimestampQuality::SourceProvided),
        received_at,
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "CloseAll",
                        "ts": "2026-08-11T00:00:00Z"
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )),
    )
}

fn service(state: Arc<dyn SourceStateStore>) -> Arc<signal_parser::runner::IngestionService> {
    Arc::new(
        structured_jsonl_service(
            SourceId::new("replay:contract-test").unwrap(),
            state,
            IngestionServiceConfig::default(),
        )
        .unwrap(),
    )
}

fn manifest() -> ResolvedRunnerManifest {
    RunnerManifest::from_toml_str(&format!(
        r#"schema_version = 1
id = "replay-contract"
version = "1.0.0"
malformed_records = "stop"

[[sources]]
id = "source"
adapter = "{BUILTIN_SOURCE_EVENT_JSONL_ADAPTER}"
codec = "{SOURCE_EVENT_JSONL_CODEC}"
path = "fixtures/source-events.jsonl"
source_id = "replay:contract-test"
pipeline = "strict-json"
revision_policy = "monotonic"

[pipelines.strict-json]
kind = "structured"
decoder = "{BUILTIN_CANONICAL_RAW_SIGNALS_DECODER}"
draft_validation = "none"
finalizer = "{BUILTIN_STANDARD_SIGNAL_FINALIZER}"

[state]
backend = "{BUILTIN_SQLITE_STATE}"
path = "target/replay-contract.sqlite"

[[sinks]]
id = "sink"
component = "{BUILTIN_COMMITTED_JSONL_SINK}"
codec = "{COMMITTED_NORMALIZATION_JSONL_CODEC}"
path = "target/replay-contract.jsonl"
acknowledgement = "duplicate_tolerant"

[limits]
admission_queue_depth = 1
publication_queue_depth = 1
maximum_payload_bytes = 1
admission_deadline_ms = 1
event_deadline_ms = 1
stage_deadline_ms = 1
compare_commit_retries = 0
reservation_ttl_seconds = 1
maximum_active_outputs = 1
replacement_policy = "patch"

[publication]
lease_ttl_seconds = 1
batch_size = 1
attempt_deadline_ms = 1
initial_backoff_ms = 1
maximum_backoff_ms = 1
maximum_attempts = 1
dead_letter_after_ms = 1
"#,
    ))
    .unwrap()
    .compile()
    .unwrap()
}

#[test]
fn replay_orders_available_receipts_preserves_delivery_identity_and_leaves_source_unchanged() {
    let source: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let target: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let source_service = service(source.clone());
    assert_eq!(
        source_service.source_adapter_identity().config_identity(),
        None
    );
    let first_delivery = DurableDeliveryIdentity::Stable("source-delivery-1".to_string());
    let second_delivery = DurableDeliveryIdentity::Stable("source-delivery-2".to_string());

    source_service
        .submit(
            event("event-1", 1, "2026-08-11T00:00:01Z"),
            first_delivery.clone(),
        )
        .unwrap();
    source_service
        .submit(
            event("event-1", 2, "2026-08-11T00:00:02Z"),
            second_delivery.clone(),
        )
        .unwrap();
    let source_receipts = source.recorded_receipts().unwrap();
    assert_eq!(source_receipts.len(), 2);
    assert_eq!(source_receipts[0].source_adapter.config_identity, None);
    assert_eq!(source_receipts[1].source_adapter.config_identity, None);

    let manifest = manifest();
    let report = ReplayIngestionRunner::new(source.clone(), service(target.clone()), &manifest, 1)
        .unwrap()
        .run()
        .unwrap();

    assert_eq!(report.available_receipts, 2);
    assert_eq!(report.replayed_receipts, 1);
    assert_eq!(report.committed_receipts, 1);
    assert_eq!(report.existing_receipts, 0);
    assert_eq!(report.retry_required_receipts, 0);
    assert_eq!(report.truncated_receipts, 1);
    assert_eq!(source.recorded_receipts().unwrap().len(), 2);
    assert_eq!(
        source.recorded_receipts().unwrap()[0].delivery_identity,
        first_delivery
    );
    assert_eq!(
        source.recorded_receipts().unwrap()[1].delivery_identity,
        second_delivery
    );
    let target_receipts = target.recorded_receipts().unwrap();
    assert_eq!(target_receipts.len(), 1);
    assert_eq!(target_receipts[0].delivery_identity, first_delivery);
}

#[test]
fn replay_rejects_a_revision_that_is_unavailable_until_after_its_successor() {
    let source: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let target: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let source_service = service(source.clone());

    source_service
        .submit(
            event("event-1", 1, "2026-08-11T00:00:02Z"),
            DurableDeliveryIdentity::Stable("source-delivery-1".to_string()),
        )
        .unwrap();
    source_service
        .submit(
            event("event-1", 2, "2026-08-11T00:00:01Z"),
            DurableDeliveryIdentity::Stable("source-delivery-2".to_string()),
        )
        .unwrap();

    let manifest = manifest();
    let error = ReplayIngestionRunner::new(source, service(target.clone()), &manifest, 8)
        .unwrap()
        .run()
        .unwrap_err();

    assert!(matches!(
        error,
        ReplayIngestionError::InvalidReceiptOrder { .. }
    ));
    assert!(target.recorded_receipts().unwrap().is_empty());
}

#[test]
fn replay_rejects_unavailable_recorded_execution_before_target_mutation() {
    let source: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let target: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let manifest = manifest();
    let mut unavailable_execution: AdmittedExecutionIdentity = manifest.execution_identity.clone();
    unavailable_execution.routing_graph.as_mut_slice()[0] ^= 1;
    let source_service = Arc::new(
        structured_jsonl_service(
            SourceId::new("replay:contract-test").unwrap(),
            source.clone(),
            IngestionServiceConfig::default(),
        )
        .unwrap()
        .with_execution_identity(unavailable_execution),
    );
    source_service
        .submit(
            event("event-1", 1, "2026-08-11T00:00:01Z"),
            DurableDeliveryIdentity::Stable("source-delivery-1".to_string()),
        )
        .unwrap();

    let error = ReplayIngestionRunner::new(source, service(target.clone()), &manifest, 8)
        .unwrap()
        .run()
        .unwrap_err();

    assert!(matches!(
        error,
        ReplayIngestionError::RecordedExecutionUnavailable { intake_index: 1 }
    ));
    assert!(target.recorded_receipts().unwrap().is_empty());
}

#[test]
fn replay_validates_receipts_beyond_the_replay_limit_before_target_mutation() {
    let source: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let target: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let manifest = manifest();
    let valid_service = service(source.clone());
    valid_service
        .submit(
            event("event-1", 1, "2026-08-11T00:00:01Z"),
            DurableDeliveryIdentity::Stable("source-delivery-1".to_string()),
        )
        .unwrap();

    let mut unavailable_execution: AdmittedExecutionIdentity = manifest.execution_identity.clone();
    unavailable_execution
        .finalizer
        .config_identity
        .as_mut_slice()[0] ^= 1;
    let unavailable_service = Arc::new(
        structured_jsonl_service(
            SourceId::new("replay:contract-test").unwrap(),
            source.clone(),
            IngestionServiceConfig::default(),
        )
        .unwrap()
        .with_execution_identity(unavailable_execution),
    );
    unavailable_service
        .submit(
            event("event-2", 1, "2026-08-11T00:00:02Z"),
            DurableDeliveryIdentity::Stable("source-delivery-2".to_string()),
        )
        .unwrap();

    let error = ReplayIngestionRunner::new(source, service(target.clone()), &manifest, 1)
        .unwrap()
        .run()
        .unwrap_err();

    assert!(matches!(
        error,
        ReplayIngestionError::RecordedExecutionUnavailable { intake_index: 2 }
    ));
    assert!(target.recorded_receipts().unwrap().is_empty());
}

#[test]
fn replay_rejects_unavailable_recorded_adapter_before_target_mutation() {
    let source: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let target: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let unavailable_adapter = SourceAdapterIdentity::without_config(
        ComponentId::try_new("unavailable-adapter", "adapter ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
    );
    let source_service = Arc::new(
        structured_jsonl_service(
            SourceId::new("replay:contract-test").unwrap(),
            source.clone(),
            IngestionServiceConfig::default(),
        )
        .unwrap()
        .with_source_adapter(unavailable_adapter),
    );
    source_service
        .submit(
            event("event-1", 1, "2026-08-11T00:00:01Z"),
            DurableDeliveryIdentity::Stable("source-delivery-1".to_string()),
        )
        .unwrap();

    let manifest = manifest();
    let error = ReplayIngestionRunner::new(source, service(target.clone()), &manifest, 8)
        .unwrap()
        .run()
        .unwrap_err();

    assert!(matches!(
        error,
        ReplayIngestionError::RecordedAdapterUnavailable { intake_index: 1 }
    ));
    assert!(target.recorded_receipts().unwrap().is_empty());
}
