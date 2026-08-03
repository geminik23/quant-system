use std::collections::BTreeMap;

use signal_parser::ingestion::{
    BoundedBytes, BoundedText, DateTimeUtc, ExternalAuthorId, ExternalCorrelationId,
    ExternalEventId, ExternalThreadId, LanguageTag, MAX_EXTERNAL_ID_BYTES, MAX_METADATA_LABELS,
    MAX_PAYLOAD_BYTES, MetadataKey, MetadataValue, OpaqueSourceRevision, OpaqueSourceSequence,
    PayloadEncoding, PayloadSchemaRef, SOURCE_EVENT_SCHEMA_VERSION, SourceEvent, SourceEventKey,
    SourceEventRef, SourceId, SourceMetadata, SourceOperation, SourcePayload, SourceRevision,
    SourceSequence, SourceTimestamp, SourceTimestampQuality, StructuredPayload, TextFormat,
    TextPayload,
};

fn key(source: &str, external_id: &str) -> SourceEventKey {
    SourceEventKey::new(
        SourceId::new(source).unwrap(),
        ExternalEventId::new(external_id).unwrap(),
    )
}

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn event(payload: SourcePayload) -> SourceEvent {
    SourceEvent::new(
        key("webhook:provider-a", "evt-001"),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-07-18T01:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-07-18T01:00:00.042Z"),
        payload,
    )
}

#[test]
fn external_identity_is_source_scoped_and_case_preserving() {
    let left = key("webhook:provider-a", "Event-Ä");
    let same_external_id_other_source = key("webhook:provider-b", "Event-Ä");

    assert_ne!(left, same_external_id_other_source);
    assert_eq!(left.external_id().as_str(), "Event-Ä");
    assert!(SourceId::new("Webhook:provider-a").is_err());
    assert!(SourceId::new("webhook::provider-a").is_err());
    assert!(ExternalEventId::new("event\n1").is_err());
    assert!(ExternalEventId::new("x".repeat(MAX_EXTERNAL_ID_BYTES + 1)).is_err());
}

#[test]
fn utc_timestamp_normalizes_offsets_and_rejects_naive_values() {
    let parsed = timestamp("2026-07-18T10:00:00.120+09:00");

    assert_eq!(
        serde_json::to_string(&parsed).unwrap(),
        "\"2026-07-18T01:00:00.12Z\""
    );
    assert!(DateTimeUtc::parse("2026-07-18T01:00:00").is_err());
}

#[test]
fn source_event_round_trips_strict_version_one_shape() {
    let mut labels = BTreeMap::new();
    labels.insert(
        MetadataKey::new("author_class").unwrap(),
        MetadataValue::new("verified").unwrap(),
    );
    let payload = SourcePayload::Text(TextPayload::new(
        BoundedText::new("signal text\nwith exact whitespace").unwrap(),
        TextFormat::Plain,
        Some(LanguageTag::new("en-US").unwrap()),
    ));
    let original = event(payload)
        .with_thread(ExternalThreadId::new("thread-A").unwrap())
        .with_parent(key("archive:provider-a", "parent-1"))
        .with_author(ExternalAuthorId::new("Author-7").unwrap())
        .with_correlation(ExternalCorrelationId::new("corr-9").unwrap())
        .with_sequence(SourceSequence::Opaque(
            OpaqueSourceSequence::new("offset-0007").unwrap(),
        ))
        .with_metadata(SourceMetadata::new(labels).unwrap());

    let json = serde_json::to_string(&original).unwrap();
    let decoded: SourceEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(decoded, original);
    assert_eq!(decoded.schema_version(), SOURCE_EVENT_SCHEMA_VERSION);
    assert_eq!(
        decoded.parent().unwrap().source().as_str(),
        "archive:provider-a"
    );
    assert_eq!(decoded.metadata().labels().len(), 1);
}

#[test]
fn source_event_matches_the_exact_version_one_fixture() {
    let actual = format!(
        "{}\n",
        serde_json::to_string(&event(SourcePayload::Empty)).unwrap()
    );

    assert_eq!(
        actual,
        include_str!("fixtures/generic_source_event/source_event_v1.json")
    );
}

#[test]
fn source_event_rejects_unknown_fields_versions_and_invalid_nested_values() {
    let base = serde_json::to_value(event(SourcePayload::Empty)).unwrap();

    let mut unknown = base.clone();
    unknown
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), serde_json::json!(true));
    assert!(serde_json::from_value::<SourceEvent>(unknown).is_err());

    let mut unsupported = base.clone();
    unsupported["schema_version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<SourceEvent>(unsupported).is_err());

    let mut invalid_source = base.clone();
    invalid_source["key"]["source"] = serde_json::json!("Webhook:provider-a");
    assert!(serde_json::from_value::<SourceEvent>(invalid_source).is_err());

    let mut invalid_payload = base.clone();
    invalid_payload["payload"]["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<SourceEvent>(invalid_payload).is_err());

    let mut unknown_operation = base.clone();
    unknown_operation["operation"] = serde_json::json!("archive");
    assert!(serde_json::from_value::<SourceEvent>(unknown_operation).is_err());

    for field in ["thread", "parent", "author", "correlation", "sequence"] {
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<SourceEvent>(missing).is_err(),
            "missing field {field} was accepted"
        );
    }

    let mut missing_language = serde_json::to_value(SourcePayload::Text(TextPayload::new(
        BoundedText::new("text").unwrap(),
        TextFormat::Plain,
        None,
    )))
    .unwrap();
    missing_language.as_object_mut().unwrap().remove("language");
    assert!(serde_json::from_value::<SourcePayload>(missing_language).is_err());
}

#[test]
fn all_operations_and_revision_forms_are_serializable_facts() {
    let operations = [
        SourceOperation::Create,
        SourceOperation::Update,
        SourceOperation::Delete,
        SourceOperation::Upsert,
        SourceOperation::Snapshot,
    ];
    let revisions = [
        SourceRevision::Monotonic(7),
        SourceRevision::Opaque(OpaqueSourceRevision::new("rev-A").unwrap()),
        SourceRevision::Unversioned,
    ];

    for operation in operations {
        for revision in revisions.clone() {
            let base = event(SourcePayload::Empty);
            let candidate = SourceEvent::new(
                base.key().clone(),
                operation,
                revision,
                base.occurred_at(),
                base.received_at(),
                SourcePayload::Empty,
            );
            let value = serde_json::to_value(&candidate).unwrap();
            let decoded: SourceEvent = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(decoded.operation(), operation);
            assert_eq!(decoded.revision(), candidate.revision());
            assert_eq!(value["payload"]["type"], "empty");
        }
    }
}

#[test]
fn reception_fallback_and_clock_skew_are_preserved() {
    let occurred_at = timestamp("2026-07-18T02:00:00Z");
    let received_at = timestamp("2026-07-18T01:59:59Z");
    let candidate = SourceEvent::new(
        key("file:import", "row-1"),
        SourceOperation::Create,
        SourceRevision::Unversioned,
        SourceTimestamp::new(occurred_at, SourceTimestampQuality::ReceptionFallback),
        received_at,
        SourcePayload::Empty,
    );

    assert_eq!(
        candidate.occurred_at().quality(),
        SourceTimestampQuality::ReceptionFallback
    );
    assert!(candidate.received_at() < candidate.occurred_at().value());
}

#[test]
fn structured_payload_uses_canonical_padded_base64() {
    let payload = StructuredPayload::new(
        PayloadSchemaRef::new("provider-a/signal@1").unwrap(),
        PayloadEncoding::Binary,
        BoundedBytes::new([0_u8, 1, 2, 255]).unwrap(),
    );
    let json = serde_json::to_value(SourcePayload::Structured(payload.clone())).unwrap();

    assert_eq!(json["data_base64"], "AAEC/w==");
    let decoded: SourcePayload = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, SourcePayload::Structured(payload));

    let non_canonical = serde_json::json!({
        "type": "structured",
        "schema": "provider-a/signal@1",
        "encoding": "binary",
        "data_base64": "AAEC_w"
    });
    assert!(serde_json::from_value::<SourcePayload>(non_canonical).is_err());
}

#[test]
fn payload_and_metadata_limits_are_enforced() {
    assert!(BoundedText::new("x".repeat(MAX_PAYLOAD_BYTES + 1)).is_err());
    assert!(BoundedBytes::new(vec![0; MAX_PAYLOAD_BYTES + 1]).is_err());
    assert!(PayloadSchemaRef::new("provider-a/signal@01").is_err());
    assert!(PayloadSchemaRef::new("Provider/signal@1").is_err());
    assert!(LanguageTag::new("-").is_err());
    assert!(MetadataKey::new("Invalid-Key").is_err());
    assert!(MetadataValue::new("value\nwith-control").is_err());

    let too_many_labels = (0..=MAX_METADATA_LABELS)
        .map(|index| {
            (
                MetadataKey::new(format!("key-{index}")).unwrap(),
                MetadataValue::new("value").unwrap(),
            )
        })
        .collect();
    assert!(SourceMetadata::new(too_many_labels).is_err());

    let oversized_metadata = (0..MAX_METADATA_LABELS)
        .map(|index| {
            (
                MetadataKey::new(format!("key-{index}")).unwrap(),
                MetadataValue::new("x".repeat(1024)).unwrap(),
            )
        })
        .collect();
    assert!(SourceMetadata::new(oversized_metadata).is_err());
}

#[test]
fn source_reference_does_not_claim_unversioned_delivery_uniqueness() {
    let candidate = SourceEvent::new(
        key("jsonl:demo", "event-1"),
        SourceOperation::Delete,
        SourceRevision::Unversioned,
        SourceTimestamp::new(
            timestamp("2026-07-18T01:00:00Z"),
            SourceTimestampQuality::AdapterDerived,
        ),
        timestamp("2026-07-18T01:00:01Z"),
        SourcePayload::Empty,
    );

    let reference = SourceEventRef::from(&candidate);
    assert_eq!(reference.key(), candidate.key());
    assert_eq!(reference.revision(), &SourceRevision::Unversioned);
}
