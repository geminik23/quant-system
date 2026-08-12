use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use qs_core::{PositionRef, RawSignal};
use signal_parser::adapters::structured_json::{
    COMMITTED_NORMALIZATION_BATCH_ARTIFACT_TYPE, COMMITTED_NORMALIZATION_BATCH_SCHEMA_VERSION,
    MAX_STRUCTURED_JSON_ERROR_BYTES, MAX_STRUCTURED_JSONL_RECORD_BYTES,
    SOURCE_EVENT_JSONL_ARTIFACT_ID_PREFIX, SourceEventJsonlRecord, StructuredJsonErrorKind,
    decode_committed_normalization_batch_jsonl, decode_committed_normalization_batch_jsonl_record,
    decode_source_event_jsonl, encode_committed_normalization_batch_jsonl,
    encode_committed_normalization_batch_jsonl_record, encode_source_event_jsonl_record,
    source_event_jsonl_artifact_identity,
};
use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceEventRef, SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::{
    CanonicalRawSignalsDecoder, CompiledPipeline, CompiledRoutingGraph, ComponentConfigSchemaRef,
    ComponentDescriptor, ComponentId, ComponentKind, DraftValidationStep, EmptyOutputPolicy,
    EvaluationInput, NoConfig, NormalizationOutcome, PayloadKind, PipelineContextRequirements,
    PipelineEvaluationResult, PipelineId, RouteEvaluation, RouteSelector, RouteSpec,
    SemanticVersion, Sha256Digest, SourceAdapterIdentity, StandardSignalFinalizer,
    StructuredInputCapability, bind_decoder, bind_finalizer, normalized_signal_semantic_digest,
    raw_signals_v1_schema,
};
use signal_parser::state::{
    ApplicationCommitInput, AppliedEventId, CommittedBatchId, CommittedEvaluationIdentity,
    CommittedNormalizationBatch, CommittedNormalizationOutcome, CommittedNormalizedSignalEnvelope,
    CompareAndCommitRequest, CompareAndCommitResult, DurableDeliveryIdentity,
    MemorySourceStateStore, NormalizationCommitRef, NormalizedLifecycleEvent, NormalizedSignalId,
    PreflightRequest, PreflightResult, ReplacementPolicy, SnapshotRequest, SourceStateStore,
};

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn source_event(external_id: &str) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:neutral-test").unwrap(),
            ExternalEventId::new(external_id).unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-08T01:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-08T01:00:01Z"),
        SourcePayload::Empty,
    )
}

fn source_ref() -> SourceEventRef {
    SourceEventRef::from(&source_event("committed-1"))
}

fn raw_signal() -> RawSignal {
    RawSignal::Close {
        ts: NaiveDateTime::parse_from_str("2026-08-08T01:00:00", "%Y-%m-%dT%H:%M:%S").unwrap(),
        position: PositionRef::ByTradeId {
            trade_id: "trade-neutral-1".to_string(),
        },
    }
}

fn accepted_batch() -> CommittedNormalizationBatch {
    let batch_id = CommittedBatchId::from_bytes([1; 32]);
    let applied_event_id = AppliedEventId::from_bytes([2; 32]);
    let normalized_id = NormalizedSignalId::from_bytes([3; 32]);
    let evaluation_identity = CommittedEvaluationIdentity {
        routing_graph: [4; 32],
        selected_pipeline: Some([5; 32]),
    };
    let signal = raw_signal();
    let semantic_digest = *normalized_signal_semantic_digest(&signal, None)
        .unwrap()
        .digest()
        .as_bytes();
    let source = source_ref();

    CommittedNormalizationBatch {
        batch_id,
        applied_event_id,
        source: source.clone(),
        evaluation_identity: Some(evaluation_identity.clone()),
        evaluation_semantic_digest: Some([6; 32]),
        outcome: CommittedNormalizationOutcome::Accepted {
            outputs: vec![normalized_id],
        },
        envelopes: vec![CommittedNormalizedSignalEnvelope {
            commit: NormalizationCommitRef {
                batch_id,
                commit_index: 7,
            },
            normalized_id,
            applied_event_id,
            signal,
            source,
            evaluation_identity,
            instrument_hint: None,
            candidate_ordinal: 0,
            semantic_digest,
            correlation_hints: vec!["trade-neutral-1".to_string()],
        }],
        lifecycle: vec![NormalizedLifecycleEvent::Added {
            output: normalized_id,
        }],
        commit_index: 7,
        committed_at: timestamp("2026-08-08T01:00:02Z"),
    }
}

fn lifecycle_only_batch() -> CommittedNormalizationBatch {
    let applied_event_id = AppliedEventId::from_bytes([12; 32]);
    CommittedNormalizationBatch {
        batch_id: CommittedBatchId::from_bytes([11; 32]),
        applied_event_id,
        source: source_ref(),
        evaluation_identity: None,
        evaluation_semantic_digest: None,
        outcome: CommittedNormalizationOutcome::LifecycleOnlyDelete,
        envelopes: vec![],
        lifecycle: vec![NormalizedLifecycleEvent::Withdrawn {
            output: NormalizedSignalId::from_bytes([13; 32]),
            cause: applied_event_id,
        }],
        commit_index: 8,
        committed_at: timestamp("2026-08-08T01:00:03Z"),
    }
}

fn lifecycle_event(revision: u64, operation: SourceOperation, risk: Option<f64>) -> SourceEvent {
    let payload = match risk {
        Some(risk) => SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "Entry",
                        "ts": "2026-08-08T02:00:00Z",
                        "symbol": "EURUSD",
                        "side": "Buy",
                        "order_type": "Market",
                        "price": null,
                        "risk": risk,
                        "stoploss": null,
                        "targets": [],
                        "group": null,
                        "trade_id": null
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )),
        None => SourcePayload::Empty,
    };
    let (occurred_at, received_at) = match revision {
        1 => (
            timestamp("2026-08-08T02:00:00Z"),
            timestamp("2026-08-08T02:00:01Z"),
        ),
        2 => (
            timestamp("2026-08-08T02:01:00Z"),
            timestamp("2026-08-08T02:01:01Z"),
        ),
        3 => (
            timestamp("2026-08-08T02:02:00Z"),
            timestamp("2026-08-08T02:02:01Z"),
        ),
        _ => panic!("unexpected lifecycle revision"),
    };
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:lifecycle-test").unwrap(),
            ExternalEventId::new("entry-1").unwrap(),
        ),
        operation,
        SourceRevision::Monotonic(revision),
        SourceTimestamp::new(occurred_at, SourceTimestampQuality::SourceProvided),
        received_at,
        payload,
    )
}

fn lifecycle_descriptor(kind: ComponentKind, id: &str) -> ComponentDescriptor {
    ComponentDescriptor::try_new(
        ComponentId::try_new(id, "component ID").unwrap(),
        kind,
        SemanticVersion::new(1, 0, 0),
        1,
        ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap(),
        PipelineContextRequirements::none(),
        EmptyOutputPolicy::Reject,
        if kind == ComponentKind::Decoder {
            vec![StructuredInputCapability::new(
                raw_signals_v1_schema(),
                PayloadEncoding::Json,
            )]
        } else {
            vec![]
        },
        vec![],
        vec![],
    )
    .unwrap()
}

fn lifecycle_graph() -> CompiledRoutingGraph {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let decoder = bind_decoder(
        lifecycle_descriptor(ComponentKind::Decoder, "jsonl-lifecycle-decoder"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();
    let finalizer = bind_finalizer(
        lifecycle_descriptor(ComponentKind::Finalizer, "jsonl-lifecycle-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_structured(
        PipelineId::try_new("jsonl-lifecycle", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "jsonl-lifecycle",
        1,
        RouteSelector::try_new(
            Some(SourceId::new("jsonl:lifecycle-test").unwrap()),
            None,
            Some(PayloadKind::Structured),
            Some(raw_signals_v1_schema()),
            Some(PayloadEncoding::Json),
            None,
            None,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
        pipeline.identity().clone(),
    )
    .unwrap();
    CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap()
}

fn lifecycle_adapter() -> SourceAdapterIdentity {
    SourceAdapterIdentity::new(
        ComponentId::try_new("jsonl-lifecycle-adapter", "adapter ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        Sha256Digest::new([31; 32]),
    )
}

fn preflight_jsonl_record(
    store: &MemorySourceStateStore,
    record: &SourceEventJsonlRecord,
) -> PreflightResult {
    store
        .preflight(PreflightRequest {
            event: record.event().clone(),
            delivery_identity: Some(record.delivery_identity().clone()),
            source_adapter: lifecycle_adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: record.event().received_at(),
            expires_at: timestamp("2026-08-08T03:00:00Z"),
        })
        .unwrap()
}

fn evaluate_and_commit_jsonl_record(
    store: &MemorySourceStateStore,
    graph: &CompiledRoutingGraph,
    record: &SourceEventJsonlRecord,
    replacement_policy: ReplacementPolicy,
) -> CommittedBatchId {
    let PreflightResult::Reserved(reservation) = preflight_jsonl_record(store, record) else {
        panic!("expected JSONL record reservation");
    };
    let prepared = match graph.route(EvaluationInput::new(
        record.event().clone(),
        lifecycle_adapter(),
        None,
    )) {
        RouteEvaluation::Prepared(prepared) => prepared,
        RouteEvaluation::Completed(_) => panic!("expected selected structured pipeline"),
    };
    let snapshot = store
        .snapshot(SnapshotRequest {
            applied_event_id: reservation.applied_event_id,
            fence: reservation.fence,
            selected_pipeline: prepared.identity().selected_pipeline().unwrap().clone(),
            requirements: prepared.requirements().clone(),
            requested_at: record.event().received_at(),
        })
        .unwrap();
    let PipelineEvaluationResult::Completed(report) = prepared.evaluate(&snapshot.base_context)
    else {
        panic!("canonical raw-signals evaluation did not complete");
    };
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Accepted { .. }
    ));
    match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: snapshot.compare_token,
            input: ApplicationCommitInput::CompletedEvaluation(&report),
            replacement_policy,
            maximum_active_outputs: 32,
            publication_sink: None,
            committed_at: record.event().received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected commit result: {other:?}"),
    }
}

fn assert_committed_batch_codec_roundtrip(batch: &CommittedNormalizationBatch) {
    let encoded = encode_committed_normalization_batch_jsonl_record(batch).unwrap();
    let decoded = decode_committed_normalization_batch_jsonl_record(&encoded).unwrap();
    assert_eq!(decoded.batch_id, batch.batch_id);
    assert_eq!(decoded.applied_event_id, batch.applied_event_id);
    assert_eq!(decoded.source, batch.source);
    assert_eq!(decoded.evaluation_identity, batch.evaluation_identity);
    assert_eq!(
        decoded.evaluation_semantic_digest,
        batch.evaluation_semantic_digest
    );
    assert_eq!(decoded.outcome, batch.outcome);
    assert_eq!(decoded.lifecycle, batch.lifecycle);
    assert_eq!(decoded.commit_index, batch.commit_index);
    assert_eq!(decoded.committed_at, batch.committed_at);
    assert_eq!(decoded.envelopes.len(), batch.envelopes.len());
    for (decoded, expected) in decoded.envelopes.iter().zip(&batch.envelopes) {
        assert_eq!(decoded.commit, expected.commit);
        assert_eq!(decoded.normalized_id, expected.normalized_id);
        assert_eq!(decoded.applied_event_id, expected.applied_event_id);
        assert_eq!(decoded.source, expected.source);
        assert_eq!(decoded.evaluation_identity, expected.evaluation_identity);
        assert_eq!(decoded.instrument_hint, expected.instrument_hint);
        assert_eq!(decoded.candidate_ordinal, expected.candidate_ordinal);
        assert_eq!(decoded.semantic_digest, expected.semantic_digest);
        assert_eq!(decoded.correlation_hints, expected.correlation_hints);
    }
}

#[test]
fn source_event_jsonl_uses_exact_artifact_bytes_and_physical_lines() {
    let first = serde_json::to_vec(&source_event("event-1")).unwrap();
    let fourth = serde_json::to_vec(&source_event("event-4")).unwrap();
    let mut artifact = first;
    artifact.extend_from_slice(b"\r\n\r\n{malformed}\n");
    artifact.extend_from_slice(&fourth);

    let decoded = decode_source_event_jsonl(&artifact);
    let renamed_copy = artifact.clone();
    assert!(
        decoded
            .artifact_identity()
            .starts_with(SOURCE_EVENT_JSONL_ARTIFACT_ID_PREFIX)
    );
    assert_eq!(
        decoded.artifact_identity(),
        source_event_jsonl_artifact_identity(&renamed_copy)
    );
    assert_eq!(
        decoded.artifact_identity(),
        source_event_jsonl_artifact_identity(&artifact)
    );
    let lf_artifact = artifact
        .iter()
        .copied()
        .filter(|byte| *byte != b'\r')
        .collect::<Vec<_>>();
    assert_ne!(
        decoded.artifact_identity(),
        source_event_jsonl_artifact_identity(&lf_artifact)
    );
    assert_eq!(decoded.records().len(), 3);

    let first = decoded.records()[0].as_ref().unwrap();
    assert_eq!(first.physical_line(), 1);
    assert_eq!(first.event().key().external_id().as_str(), "event-1");
    assert_eq!(
        first.delivery_identity(),
        &DurableDeliveryIdentity::OfflinePosition {
            artifact: decoded.artifact_identity().to_string(),
            ordinal: 1,
        }
    );

    let malformed = decoded.records()[1].as_ref().unwrap_err();
    assert_eq!(malformed.physical_line(), 3);
    assert_eq!(
        malformed.error().kind(),
        StructuredJsonErrorKind::InvalidJson
    );
    assert!(malformed.error().message().len() <= MAX_STRUCTURED_JSON_ERROR_BYTES);
    assert!(malformed.to_string().len() <= MAX_STRUCTURED_JSON_ERROR_BYTES);

    let fourth = decoded.records()[2].as_ref().unwrap();
    assert_eq!(fourth.physical_line(), 4);
    assert_eq!(fourth.event().key().external_id().as_str(), "event-4");
}

#[test]
fn source_event_jsonl_rejects_unknown_fields_per_record() {
    let mut value = serde_json::to_value(source_event("event-unknown")).unwrap();
    value["unexpected"] = serde_json::json!(true);
    let mut artifact = serde_json::to_vec(&value).unwrap();
    artifact.push(b'\n');
    artifact.extend_from_slice(
        &encode_source_event_jsonl_record(&source_event("event-valid")).unwrap(),
    );

    let decoded = decode_source_event_jsonl(&artifact);
    assert_eq!(decoded.records().len(), 2);
    assert_eq!(
        decoded.records()[0].as_ref().unwrap_err().physical_line(),
        1
    );
    assert_eq!(
        decoded.records()[0].as_ref().unwrap_err().error().kind(),
        StructuredJsonErrorKind::InvalidJson
    );
    assert_eq!(decoded.records()[1].as_ref().unwrap().physical_line(), 2);
}

#[test]
fn source_event_jsonl_rejects_bom_and_bounds_lines_without_stop_policy() {
    let valid = encode_source_event_jsonl_record(&source_event("event-2")).unwrap();
    assert_eq!(valid.last(), Some(&b'\n'));

    let mut artifact = vec![0xef, 0xbb, 0xbf];
    artifact.extend_from_slice(&valid);
    artifact.extend(std::iter::repeat_n(
        b'x',
        MAX_STRUCTURED_JSONL_RECORD_BYTES + 1,
    ));
    artifact.push(b'\n');
    artifact.extend_from_slice(&valid);

    let decoded = decode_source_event_jsonl(&artifact);
    assert_eq!(decoded.records().len(), 3);
    assert_eq!(
        decoded.records()[0].as_ref().unwrap_err().error().kind(),
        StructuredJsonErrorKind::ByteOrderMark
    );
    assert_eq!(
        decoded.records()[1].as_ref().unwrap_err().error().kind(),
        StructuredJsonErrorKind::RecordTooLarge
    );
    assert_eq!(decoded.records()[2].as_ref().unwrap().physical_line(), 3);
}

#[test]
fn committed_batch_codec_round_trips_complete_authoritative_records() {
    let accepted = accepted_batch();
    let encoded = encode_committed_normalization_batch_jsonl_record(&accepted).unwrap();
    assert_eq!(encoded.last(), Some(&b'\n'));
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(
        value["schema_version"],
        COMMITTED_NORMALIZATION_BATCH_SCHEMA_VERSION
    );
    assert_eq!(
        value["artifact_type"],
        COMMITTED_NORMALIZATION_BATCH_ARTIFACT_TYPE
    );
    assert_eq!(value["batch_id"], accepted.batch_id.to_string_id());
    assert!(value["batch_id"].as_str().unwrap().starts_with("nb2_"));
    assert!(
        value["applied_event_id"]
            .as_str()
            .unwrap()
            .starts_with("ae1_")
    );
    assert!(
        value["outcome"]["outputs"][0]
            .as_str()
            .unwrap()
            .starts_with("ns1_")
    );
    assert_eq!(
        value["envelopes"][0]["signal"]["ts"],
        "2026-08-08T01:00:00Z"
    );
    assert_eq!(
        value["envelopes"][0]["commit"]["commit_index"],
        accepted.commit_index
    );
    assert_eq!(value["semantic_basis"]["type"], "completed_evaluation");
    assert_eq!(value["outcome"]["type"], "accepted");
    assert_eq!(value["lifecycle"][0]["type"], "added");

    let decoded = decode_committed_normalization_batch_jsonl_record(&encoded).unwrap();
    assert_eq!(decoded.batch_id, accepted.batch_id);
    assert_eq!(decoded.commit_index, accepted.commit_index);
    assert_eq!(decoded.envelopes.len(), 1);
    assert_eq!(decoded.envelopes[0].candidate_ordinal, 0);
    assert_eq!(
        decoded.lifecycle,
        vec![NormalizedLifecycleEvent::Added {
            output: accepted.envelopes[0].normalized_id,
        }]
    );

    let lifecycle_only = lifecycle_only_batch();
    let encoded = encode_committed_normalization_batch_jsonl_record(&lifecycle_only).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["semantic_basis"]["type"], "lifecycle_only_delete");
    let decoded = decode_committed_normalization_batch_jsonl_record(&encoded).unwrap();
    assert!(decoded.evaluation_identity.is_none());
    assert!(decoded.evaluation_semantic_digest.is_none());
    assert!(decoded.envelopes.is_empty());
    assert!(matches!(
        decoded.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    ));

    let encoded = encode_committed_normalization_batch_jsonl([&accepted, &lifecycle_only]).unwrap();
    let decoded = decode_committed_normalization_batch_jsonl(&encoded);
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].as_ref().unwrap().batch_id, accepted.batch_id);
    assert_eq!(
        decoded[1].as_ref().unwrap().batch_id,
        lifecycle_only.batch_id
    );
}

#[test]
fn committed_batch_decode_is_recursively_strict() {
    let encoded = encode_committed_normalization_batch_jsonl_record(&accepted_batch()).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["unexpected"] = serde_json::json!(true);
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidJson
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value.as_object_mut().unwrap().remove("schema_version");
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidJson
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["artifact_type"] = serde_json::json!("quant-system/unknown@1");
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidSchema
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["batch_id"] = serde_json::json!("01".repeat(32));
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidIdentifier
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["envelopes"][0]["signal"]["position"]["unexpected"] = serde_json::json!(true);
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidRecord
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["envelopes"][0]["commit"]["unexpected"] = serde_json::json!(true);
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidJson
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value.as_object_mut().unwrap().remove("evaluation_identity");
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidJson
    );
}

#[test]
fn committed_batch_codec_rejects_inconsistent_commit_semantics() {
    let accepted = accepted_batch();
    let encoded = encode_committed_normalization_batch_jsonl_record(&accepted).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["envelopes"][0]["commit"]["batch_id"] =
        serde_json::json!(CommittedBatchId::from_bytes([90; 32]).to_string_id());
    let malformed = serde_json::to_vec(&value).unwrap();
    assert_eq!(
        decode_committed_normalization_batch_jsonl_record(&malformed)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidRecord
    );

    let mut value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    value["semantic_basis"]["type"] = serde_json::json!("lifecycle_only_delete");
    assert!(
        decode_committed_normalization_batch_jsonl_record(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );

    let mut accepted = accepted_batch();
    accepted.envelopes[0].commit.commit_index += 1;
    assert_eq!(
        encode_committed_normalization_batch_jsonl_record(&accepted)
            .unwrap_err()
            .kind(),
        StructuredJsonErrorKind::InvalidRecord
    );

    let mut accepted = accepted_batch();
    accepted.envelopes[0].applied_event_id = AppliedEventId::from_bytes([91; 32]);
    assert!(encode_committed_normalization_batch_jsonl_record(&accepted).is_err());

    let mut accepted = accepted_batch();
    accepted.envelopes[0].evaluation_identity.routing_graph = [92; 32];
    assert!(encode_committed_normalization_batch_jsonl_record(&accepted).is_err());

    let mut accepted = accepted_batch();
    accepted.envelopes[0].candidate_ordinal = 1;
    assert!(encode_committed_normalization_batch_jsonl_record(&accepted).is_err());

    let mut accepted = accepted_batch();
    accepted.lifecycle = vec![NormalizedLifecycleEvent::Added {
        output: NormalizedSignalId::from_bytes([99; 32]),
    }];
    assert!(encode_committed_normalization_batch_jsonl_record(&accepted).is_err());

    let mut lifecycle_only = lifecycle_only_batch();
    lifecycle_only.evaluation_identity = Some(CommittedEvaluationIdentity {
        routing_graph: [20; 32],
        selected_pipeline: None,
    });
    assert!(encode_committed_normalization_batch_jsonl_record(&lifecycle_only).is_err());
}

#[test]
fn committed_batch_embedded_identity_is_not_physical_order() {
    let accepted = accepted_batch();
    let lifecycle_only = lifecycle_only_batch();
    let forward = encode_committed_normalization_batch_jsonl([&accepted, &lifecycle_only]).unwrap();
    let reverse = encode_committed_normalization_batch_jsonl([&lifecycle_only, &accepted]).unwrap();
    let forward = decode_committed_normalization_batch_jsonl(&forward);
    let reverse = decode_committed_normalization_batch_jsonl(&reverse);

    assert_eq!(forward[0].as_ref().unwrap().batch_id, accepted.batch_id);
    assert_eq!(
        forward[0].as_ref().unwrap().commit_index,
        accepted.commit_index
    );
    assert_eq!(reverse[1].as_ref().unwrap().batch_id, accepted.batch_id);
    assert_eq!(
        reverse[1].as_ref().unwrap().commit_index,
        accepted.commit_index
    );
    assert_eq!(
        forward[1].as_ref().unwrap().batch_id,
        lifecycle_only.batch_id
    );
    assert_eq!(
        reverse[0].as_ref().unwrap().batch_id,
        lifecycle_only.batch_id
    );
}

#[test]
fn source_event_jsonl_drives_corrected_durable_lifecycle() {
    let create = lifecycle_event(1, SourceOperation::Create, Some(1.0));
    let update = lifecycle_event(2, SourceOperation::Update, Some(2.0));
    let delete = lifecycle_event(3, SourceOperation::Delete, None);
    assert_eq!(create.key(), update.key());
    assert_eq!(create.key(), delete.key());

    let mut artifact_bytes = vec![b'\n'];
    artifact_bytes.extend_from_slice(&encode_source_event_jsonl_record(&create).unwrap());
    artifact_bytes.extend_from_slice(&encode_source_event_jsonl_record(&update).unwrap());
    artifact_bytes.extend_from_slice(&encode_source_event_jsonl_record(&delete).unwrap());
    let artifact = decode_source_event_jsonl(&artifact_bytes);
    let artifact_identity = artifact.artifact_identity().to_string();
    let records = artifact
        .into_records()
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 3);
    for (record, ordinal) in records.iter().zip(2_u64..=4) {
        assert_eq!(record.physical_line(), ordinal);
        assert_eq!(
            record.delivery_identity(),
            &DurableDeliveryIdentity::OfflinePosition {
                artifact: artifact_identity.clone(),
                ordinal,
            }
        );
    }

    let store = MemorySourceStateStore::new();
    let graph = lifecycle_graph();
    let first_batch_id = evaluate_and_commit_jsonl_record(
        &store,
        &graph,
        &records[0],
        ReplacementPolicy::ReplaceCurrentSourceKey,
    );
    let first = store.committed_batch(first_batch_id).unwrap().unwrap();
    assert!(matches!(
        first.outcome,
        CommittedNormalizationOutcome::Accepted { .. }
    ));
    assert_eq!(first.envelopes.len(), 1);
    let first_output = first.envelopes[0].normalized_id;
    assert_eq!(
        first.lifecycle,
        vec![NormalizedLifecycleEvent::Added {
            output: first_output,
        }]
    );
    assert_committed_batch_codec_roundtrip(&first);

    let duplicate = preflight_jsonl_record(&store, &records[0]);
    assert!(matches!(
        duplicate,
        PreflightResult::ExistingCommitted(batch_id) if batch_id == first_batch_id
    ));
    let state = store
        .source_state(records[0].event().key())
        .unwrap()
        .unwrap();
    assert_eq!(state.active_outputs, vec![first_output]);
    assert_eq!(store.recorded_receipts().unwrap().len(), 1);

    let update_batch_id = evaluate_and_commit_jsonl_record(
        &store,
        &graph,
        &records[1],
        ReplacementPolicy::ReplaceCurrentSourceKey,
    );
    let update_batch = store.committed_batch(update_batch_id).unwrap().unwrap();
    assert!(matches!(
        update_batch.outcome,
        CommittedNormalizationOutcome::Accepted { .. }
    ));
    assert_eq!(update_batch.envelopes.len(), 1);
    let update_output = update_batch.envelopes[0].normalized_id;
    assert_ne!(update_output, first_output);
    assert_eq!(
        update_batch.lifecycle,
        vec![
            NormalizedLifecycleEvent::Added {
                output: update_output,
            },
            NormalizedLifecycleEvent::Withdrawn {
                output: first_output,
                cause: update_batch.applied_event_id,
            },
        ]
    );
    assert_committed_batch_codec_roundtrip(&update_batch);
    let state = store
        .source_state(records[1].event().key())
        .unwrap()
        .unwrap();
    assert_eq!(state.active_outputs, vec![update_output]);

    let PreflightResult::Reserved(delete_reservation) = preflight_jsonl_record(&store, &records[2])
    else {
        panic!("expected delete reservation");
    };
    let delete_token = store.route_only_compare_token(&delete_reservation).unwrap();
    let delete_batch_id = match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: delete_token,
            input: ApplicationCommitInput::LifecycleOnlyDelete,
            replacement_policy: ReplacementPolicy::ReplaceCurrentSourceKey,
            maximum_active_outputs: 32,
            publication_sink: None,
            committed_at: records[2].event().received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected delete commit result: {other:?}"),
    };
    let delete_batch = store.committed_batch(delete_batch_id).unwrap().unwrap();
    assert!(matches!(
        delete_batch.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    ));
    assert!(delete_batch.evaluation_identity.is_none());
    assert!(delete_batch.evaluation_semantic_digest.is_none());
    assert!(delete_batch.envelopes.is_empty());
    assert_eq!(
        delete_batch.lifecycle,
        vec![NormalizedLifecycleEvent::Withdrawn {
            output: update_output,
            cause: delete_batch.applied_event_id,
        }]
    );
    assert_committed_batch_codec_roundtrip(&delete_batch);
    let state = store
        .source_state(records[2].event().key())
        .unwrap()
        .unwrap();
    assert!(state.active_outputs.is_empty());
    assert_eq!(store.recorded_receipts().unwrap().len(), 3);
}
