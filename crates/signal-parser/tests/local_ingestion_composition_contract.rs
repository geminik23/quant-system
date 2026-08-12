use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use signal_parser::adapters::structured_json::{
    decode_committed_normalization_batch_jsonl, encode_source_event_jsonl_record,
};
use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::raw_signals_v1_schema;

use signal_parser::runner::{
    CommittedBatchSink, PublicationSinkError,
    composition::LocalIngestionComposition,
    manifest::{
        BUILTIN_CANONICAL_RAW_SIGNALS_DECODER, BUILTIN_COMMITTED_JSONL_SINK,
        BUILTIN_SOURCE_EVENT_JSONL_ADAPTER, BUILTIN_SQLITE_STATE,
        BUILTIN_STANDARD_SIGNAL_FINALIZER, COMMITTED_NORMALIZATION_JSONL_CODEC, RunnerManifest,
        SOURCE_EVENT_JSONL_CODEC,
    },
    publication::{DeliveryAcknowledgementPolicy, PublicationDeliveryReceipt},
};

static TEMP_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn source_event() -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:local-composition-test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
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

#[test]
fn loads_manifest_and_runs_local_jsonl_ingestion() {
    let sequence = TEMP_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "qs-local-ingestion-composition-{}-{sequence}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();

    let source_path = directory.join("source.jsonl");
    let state_path = directory.join("state.sqlite");
    let sink_path = directory.join("committed.jsonl");
    let manifest_path = directory.join("runner.toml");
    fs::write(
        &source_path,
        encode_source_event_jsonl_record(&source_event()).unwrap(),
    )
    .unwrap();
    fs::write(
        &manifest_path,
        format!(
            r#"schema_version = 1
id = "local-composition"
version = "1.0.0"
malformed_records = "stop"

[[sources]]
id = "jsonl-input"
adapter = "{BUILTIN_SOURCE_EVENT_JSONL_ADAPTER}"
codec = "{SOURCE_EVENT_JSONL_CODEC}"
path = "{}"
source_id = "jsonl:local-composition-test"
pipeline = "strict-json"
revision_policy = "monotonic"

[pipelines.strict-json]
kind = "structured"
decoder = "{BUILTIN_CANONICAL_RAW_SIGNALS_DECODER}"
draft_validation = "none"
finalizer = "{BUILTIN_STANDARD_SIGNAL_FINALIZER}"

[state]
backend = "{BUILTIN_SQLITE_STATE}"
path = "{}"

[[sinks]]
id = "committed-jsonl"
component = "{BUILTIN_COMMITTED_JSONL_SINK}"
codec = "{COMMITTED_NORMALIZATION_JSONL_CODEC}"
path = "{}"
acknowledgement = "duplicate_tolerant"

[limits]
admission_queue_depth = 32
publication_queue_depth = 16
maximum_payload_bytes = 65536
admission_deadline_ms = 100
event_deadline_ms = 500
stage_deadline_ms = 250
compare_commit_retries = 2
reservation_ttl_seconds = 300
maximum_active_outputs = 8
replacement_policy = "patch"

[publication]
lease_ttl_seconds = 60
batch_size = 8
attempt_deadline_ms = 100
initial_backoff_ms = 10
maximum_backoff_ms = 1000
maximum_attempts = 12
dead_letter_after_ms = 86400000
"#,
            source_path.display(),
            state_path.display(),
            sink_path.display(),
        ),
    )
    .unwrap();

    let composition = LocalIngestionComposition::load(&manifest_path).unwrap();
    let report = composition.run_offline().unwrap();

    assert_eq!(report.ingestion.admitted_records, 1);
    assert_eq!(report.ingestion.malformed_records, 0);
    assert_eq!(report.ingestion.retry_required_records, 0);
    assert_eq!(report.published_batches, 1);
    assert_eq!(report.runtime.metrics.admitted_submissions, 1);
    assert_eq!(report.runtime.metrics.completed_submissions, 1);
    assert_eq!(report.runtime.metrics.deadline_expired_submissions, 0);
    assert_eq!(report.artifact.schema_version, 1);
    assert_eq!(report.artifact.source.records, 1);
    assert_eq!(report.artifact.source.malformed_records, 0);
    assert_eq!(report.artifact.preflight.reserved, 1);
    assert_eq!(report.artifact.evaluation.completed, 1);
    assert_eq!(report.artifact.commit.committed, 1);
    assert_eq!(report.artifact.delivery.acknowledged, 1);
    assert_eq!(report.artifact.dead_lettered, 0);
    assert_eq!(report.artifact.timeout.expired, 0);
    assert!(report.artifact.completed_at.into_inner() >= report.artifact.started_at.into_inner());
    assert!(report.artifact.manifest_digest.to_string().len() == 64);
    assert!(report.artifact.outcome_summaries.len() <= 4);
    let published = fs::read(&sink_path).unwrap();
    let batches = decode_committed_normalization_batch_jsonl(&published);
    assert_eq!(batches.len(), 1);
    assert!(batches.into_iter().next().unwrap().is_ok());

    drop(composition);
    fs::remove_dir_all(directory).unwrap();
}

enum SinkResult {
    RetryableFailure,
}

struct ScriptedSink {
    results: Mutex<VecDeque<SinkResult>>,
}

impl ScriptedSink {
    fn new(results: impl IntoIterator<Item = SinkResult>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
        }
    }
}

impl CommittedBatchSink for ScriptedSink {
    fn acknowledgement_policy(&self) -> DeliveryAcknowledgementPolicy {
        DeliveryAcknowledgementPolicy::DuplicateTolerant
    }

    fn publish(
        &self,
        _delivery: signal_parser::runner::publication::CommittedDelivery<'_>,
    ) -> Result<PublicationDeliveryReceipt, PublicationSinkError> {
        match self.results.lock().unwrap().pop_front().unwrap() {
            SinkResult::RetryableFailure => Err(PublicationSinkError::Io(std::io::Error::other(
                "unavailable",
            ))),
        }
    }
}

#[test]
fn composition_retries_then_dead_letters_using_the_manifest_maximum_attempts() {
    let sequence = TEMP_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
        "qs-local-ingestion-publication-policy-{}-{sequence}",
        std::process::id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let source_path = directory.join("source.jsonl");
    let state_path = directory.join("state.sqlite");
    fs::write(
        &source_path,
        encode_source_event_jsonl_record(&source_event()).unwrap(),
    )
    .unwrap();

    let manifest = RunnerManifest::from_toml_str(&format!(
        r#"schema_version = 1
id = "local-publication-policy"
version = "1.0.0"
malformed_records = "stop"

[[sources]]
id = "jsonl-input"
adapter = "{BUILTIN_SOURCE_EVENT_JSONL_ADAPTER}"
codec = "{SOURCE_EVENT_JSONL_CODEC}"
path = "{}"
source_id = "jsonl:local-composition-test"
pipeline = "strict-json"
revision_policy = "monotonic"

[pipelines.strict-json]
kind = "structured"
decoder = "{BUILTIN_CANONICAL_RAW_SIGNALS_DECODER}"
draft_validation = "none"
finalizer = "{BUILTIN_STANDARD_SIGNAL_FINALIZER}"

[state]
backend = "{BUILTIN_SQLITE_STATE}"
path = "{}"

[[sinks]]
id = "committed-jsonl"
component = "{BUILTIN_COMMITTED_JSONL_SINK}"
codec = "{COMMITTED_NORMALIZATION_JSONL_CODEC}"
path = "{}"
acknowledgement = "duplicate_tolerant"

[limits]
admission_queue_depth = 32
publication_queue_depth = 16
maximum_payload_bytes = 65536
admission_deadline_ms = 100
event_deadline_ms = 500
stage_deadline_ms = 250
compare_commit_retries = 2
reservation_ttl_seconds = 300
maximum_active_outputs = 8
replacement_policy = "patch"

[publication]
lease_ttl_seconds = 60
batch_size = 8
attempt_deadline_ms = 100
initial_backoff_ms = 10
maximum_backoff_ms = 1000
maximum_attempts = 2
dead_letter_after_ms = 86400000
"#,
        source_path.display(),
        state_path.display(),
        directory.join("unused.jsonl").display(),
    ))
    .unwrap();
    let sink = Arc::new(ScriptedSink::new([
        SinkResult::RetryableFailure,
        SinkResult::RetryableFailure,
    ]));
    let composition = LocalIngestionComposition::build_with_sink(manifest, sink).unwrap();

    let first = composition.run_offline().unwrap();
    assert_eq!(first.publication.acknowledged, 0);
    assert_eq!(first.publication.retries_scheduled, 1);
    assert_eq!(first.publication.dead_lettered, 0);
    assert_eq!(first.artifact.delivery.retries_scheduled, 1);
    assert_eq!(first.artifact.dead_lettered, 0);

    std::thread::sleep(std::time::Duration::from_millis(20));
    std::thread::sleep(std::time::Duration::from_millis(10));
    let second = composition.run_offline().unwrap();
    assert_eq!(second.publication.leased, 1);
    assert_eq!(second.publication.acknowledged, 0);
    assert_eq!(second.publication.retries_scheduled, 0);
    assert_eq!(second.publication.dead_lettered, 1);
    assert_eq!(second.artifact.delivery.dead_lettered, 1);
    assert_eq!(second.artifact.dead_lettered, 1);

    drop(composition);
    fs::remove_dir_all(directory).unwrap();
}
