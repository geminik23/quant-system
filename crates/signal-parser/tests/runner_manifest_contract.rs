use signal_parser::ingestion::{
    DateTimeUtc, ExternalEventId, SourceEvent, SourceEventKey, SourceId, SourceOperation,
    SourcePayload, SourceRevision, SourceTimestamp, SourceTimestampQuality,
};
use signal_parser::runner::{
    OfflineErrorPolicy,
    manifest::{
        BUILTIN_CANONICAL_RAW_SIGNALS_DECODER, BUILTIN_COMMITTED_JSONL_SINK,
        BUILTIN_SOURCE_EVENT_JSONL_ADAPTER, BUILTIN_SQLITE_STATE,
        BUILTIN_STANDARD_SIGNAL_FINALIZER, COMMITTED_NORMALIZATION_JSONL_CODEC, ManifestError,
        RunnerManifest, SOURCE_EVENT_JSONL_CODEC,
    },
};

fn manifest_toml() -> String {
    format!(
        r#"
schema_version = 1
id = "local-jsonl"
version = "1.0.0"
malformed_records = "continue"

[[sources]]
id = "jsonl-input"
adapter = "{BUILTIN_SOURCE_EVENT_JSONL_ADAPTER}"
codec = "{SOURCE_EVENT_JSONL_CODEC}"
path = "fixtures/source-events.jsonl"
source_id = "jsonl:manifest-test"
pipeline = "strict-json"
revision_policy = "monotonic"

[pipelines.strict-json]
kind = "structured"
decoder = "{BUILTIN_CANONICAL_RAW_SIGNALS_DECODER}"
draft_validation = "none"
finalizer = "{BUILTIN_STANDARD_SIGNAL_FINALIZER}"

[state]
backend = "{BUILTIN_SQLITE_STATE}"
path = "target/manifest-state.sqlite"

[[sinks]]
id = "committed-jsonl"
component = "{BUILTIN_COMMITTED_JSONL_SINK}"
codec = "{COMMITTED_NORMALIZATION_JSONL_CODEC}"
path = "target/committed.jsonl"
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
maximum_active_outputs = 32
replacement_policy = "patch"

[publication]
lease_ttl_seconds = 60
batch_size = 16
attempt_deadline_ms = 100
initial_backoff_ms = 10
maximum_backoff_ms = 1000
maximum_attempts = 12
dead_letter_after_ms = 60000
"#
    )
}

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn source_event(revision: SourceRevision) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:manifest-test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        revision,
        SourceTimestamp::new(
            timestamp("2026-08-08T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-08T00:00:01Z"),
        SourcePayload::Empty,
    )
}

#[test]
fn compiles_the_complete_local_jsonl_manifest() {
    let manifest = RunnerManifest::from_toml_str(&manifest_toml()).unwrap();
    let resolved = manifest.compile().unwrap();
    let wiring = resolved.wiring().unwrap();

    assert_eq!(resolved.id, "local-jsonl");
    assert_eq!(resolved.sources[0].pipeline, "strict-json");
    assert_eq!(wiring.source_id.as_str(), "jsonl:manifest-test");
    assert_eq!(wiring.source_adapter.id().as_str(), "source-event-jsonl");
    assert_eq!(wiring.source_adapter.version().major(), 1);
    assert_eq!(wiring.publication_batch_size, 16);
    assert_eq!(wiring.sink_binding_id, "committed-jsonl");
    assert_eq!(
        wiring.ingestion_config.publication_sink.as_deref(),
        Some("committed-jsonl")
    );
    assert_eq!(wiring.ingestion_config.maximum_active_outputs, 32);
    assert_eq!(wiring.ingestion_config.compare_and_commit_retries, 2);
    assert_eq!(wiring.admission_queue_depth, 32);
    assert_eq!(wiring.event_deadline, std::time::Duration::from_millis(500));
    assert_eq!(wiring.malformed_records, OfflineErrorPolicy::Continue);
    assert_eq!(
        wiring.publication_retry_policy.lease_ttl(),
        chrono::Duration::seconds(60)
    );
    assert_eq!(wiring.publication_retry_policy.maximum_attempts(), 12);
    assert_eq!(wiring.manifest_digest.algorithm(), "sha256");
    assert_eq!(wiring.manifest_digest.to_string().len(), 64);
    assert_eq!(resolved.execution_identity, wiring.execution_identity);
    assert_ne!(resolved.execution_identity.routing_graph, [0; 32]);
    assert_ne!(resolved.execution_identity.pipeline, [0; 32]);
    assert_eq!(
        resolved.execution_identity.decoder.id,
        "canonical-raw-signals"
    );
    assert_eq!(
        resolved.execution_identity.finalizer.id,
        "standard-signal-finalizer"
    );
    assert_eq!(
        resolved.validate_source_event(&source_event(SourceRevision::Monotonic(1))),
        Ok(())
    );
}

#[test]
fn digest_is_stable_for_equivalent_input_and_changes_with_non_secret_configuration() {
    let first = RunnerManifest::from_toml_str(&manifest_toml())
        .unwrap()
        .compile()
        .unwrap();
    let second = RunnerManifest::from_toml_str(&manifest_toml())
        .unwrap()
        .compile()
        .unwrap();
    let changed = RunnerManifest::from_toml_str(
        &manifest_toml().replace("batch_size = 16", "batch_size = 8"),
    )
    .unwrap()
    .compile()
    .unwrap();

    assert_eq!(first.manifest_digest, second.manifest_digest);
    assert_ne!(first.manifest_digest, changed.manifest_digest);
}

#[test]
fn rejects_unresolved_components_and_cross_section_limit_conflicts() {
    let cases = [
        (
            "adapter = \"source-event-jsonl@1.0.0\"",
            "adapter = \"source-event-jsonl@2.0.0\"",
            "unsupported source adapter component reference",
        ),
        (
            "pipeline = \"strict-json\"",
            "pipeline = \"missing\"",
            "pipeline missing is not configured",
        ),
        (
            "stage_deadline_ms = 250",
            "stage_deadline_ms = 501",
            "stage_deadline_ms must not exceed event_deadline_ms",
        ),
        (
            "acknowledgement = \"duplicate_tolerant\"",
            "acknowledgement = \"transport_success\"",
            "unknown variant `transport_success`",
        ),
    ];

    for (before, after, expected) in cases {
        let error =
            RunnerManifest::from_toml_str(&manifest_toml().replacen(before, after, 1)).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn rejects_opaque_and_unversioned_source_revisions() {
    let manifest = RunnerManifest::from_toml_str(&manifest_toml()).unwrap();

    assert_eq!(
        manifest
            .validate_source_event(&source_event(SourceRevision::Opaque(
                "provider-revision".try_into().unwrap(),
            )))
            .unwrap_err(),
        ManifestError::OpaqueRevision
    );
    assert_eq!(
        manifest
            .validate_source_event(&source_event(SourceRevision::Unversioned))
            .unwrap_err(),
        ManifestError::UnversionedRevision
    );
}
