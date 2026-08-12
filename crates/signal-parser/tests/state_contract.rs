use std::collections::BTreeMap;
use std::path::PathBuf;

use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::{
    BaseContextSnapshot, CanonicalRawSignalsDecoder, CompiledPipeline, CompiledRoutingGraph,
    CompletionKnowledge, ComponentConfigSchemaRef, ComponentDescriptor, ComponentId, ComponentKind,
    ComponentResult, DiagnosticSet, DraftValidationStep, EmptyOutputPolicy, EvaluationFailureClass,
    EvaluationInput, EvaluationRetrySafety, NoConfig, NormalizationOutcome,
    PipelineContextRequirements, PipelineEvaluationResult, PipelineId, PreNormalizedProducer,
    PreNormalizedSignalBatch, RouteEvaluation, RouteSelector, RouteSpec, SemanticVersion,
    Sha256Digest, SourceAdapterIdentity, StageExecutionFailure, StandardSignalFinalizer,
    StructuredInputCapability, bind_decoder, bind_finalizer, bind_pre_normalized_producer,
    raw_signals_v1_schema,
};
use signal_parser::state::{
    ApplicationCommitInput, CommittedBatchId, CommittedNormalizationOutcome,
    CompareAndCommitRequest, CompareAndCommitResult, DurableDeliveryIdentity,
    MemorySourceStateStore, NormalizedLifecycleEvent, PreflightRequest, PreflightResult,
    PublicationState, ReplacementPolicy, SnapshotRequest, SourceLifecycleState, SourceStateStore,
    SqliteSourceStateStore,
};

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn reservation_expiry() -> DateTimeUtc {
    timestamp("2026-08-06T02:00:00Z")
}

fn event(revision: u64, operation: SourceOperation) -> SourceEvent {
    let payload = if operation == SourceOperation::Delete {
        SourcePayload::Empty
    } else {
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "CloseAll",
                        "ts": "2026-08-06T00:00:00Z"
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        ))
    };
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:state-test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        operation,
        SourceRevision::Monotonic(revision),
        SourceTimestamp::new(
            timestamp("2026-08-06T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-06T00:00:01Z"),
        payload,
    )
}

fn descriptor(kind: ComponentKind, id: &str) -> ComponentDescriptor {
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

fn graph() -> CompiledRoutingGraph {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let decoder = bind_decoder(
        descriptor(ComponentKind::Decoder, "state-test-decoder"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();
    let finalizer = bind_finalizer(
        descriptor(ComponentKind::Finalizer, "state-test-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_structured(
        PipelineId::try_new("state-test", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "state-test",
        1,
        RouteSelector::try_new(
            Some(SourceId::new("jsonl:state-test").unwrap()),
            None,
            Some(signal_parser::normalization::PayloadKind::Structured),
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

fn adapter() -> SourceAdapterIdentity {
    SourceAdapterIdentity::new(
        ComponentId::try_new("state-test-adapter", "adapter ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        Sha256Digest::new([7; 32]),
    )
}

struct FailingProducer;

impl PreNormalizedProducer for FailingProducer {
    fn produce(
        &self,
        _event: &SourceEvent,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<PreNormalizedSignalBatch> {
        Err(StageExecutionFailure::new(
            EvaluationFailureClass::ComponentUnavailable,
            EvaluationRetrySafety::SafeToRetry,
            CompletionKnowledge::NotStarted,
            DiagnosticSet::empty(),
        ))
    }
}

fn failure_event() -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("failure:state-test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Unversioned,
        SourceTimestamp::new(
            timestamp("2026-08-06T01:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-06T01:00:01Z"),
        SourcePayload::Empty,
    )
}

fn evaluate_and_commit(
    store: &dyn SourceStateStore,
    graph: &CompiledRoutingGraph,
    source: SourceEvent,
) -> signal_parser::state::CommittedBatchId {
    let preflight = store
        .preflight(PreflightRequest {
            event: source.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable(format!(
                "delivery-{:?}",
                source.revision()
            ))),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: source.received_at(),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    let PreflightResult::Reserved(reservation) = preflight else {
        panic!("expected reservation");
    };
    let prepared = match graph.route(EvaluationInput::new(source.clone(), adapter(), None)) {
        RouteEvaluation::Prepared(prepared) => prepared,
        RouteEvaluation::Completed(_) => panic!("expected selected pipeline"),
    };
    let selected_pipeline = prepared.identity().selected_pipeline().unwrap().clone();
    let requirements = prepared.requirements().clone();
    let snapshot = store
        .snapshot(SnapshotRequest {
            applied_event_id: reservation.applied_event_id,
            fence: reservation.fence,
            selected_pipeline,
            requirements,
            requested_at: source.received_at(),
        })
        .unwrap();
    let result = prepared.evaluate(&snapshot.base_context);
    let PipelineEvaluationResult::Completed(report) = result else {
        panic!("evaluation did not complete");
    };
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Accepted { .. }
    ));
    match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: snapshot.compare_token,
            input: ApplicationCommitInput::CompletedEvaluation(&report),
            replacement_policy: ReplacementPolicy::Patch,
            maximum_active_outputs: 32,
            publication_sink: Some("committed-jsonl".to_string()),
            committed_at: source.received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected commit result: {other:?}"),
    }
}

fn run_store_conformance(store: &dyn SourceStateStore) {
    let graph = graph();
    let first = event(1, SourceOperation::Create);
    let first_batch = evaluate_and_commit(store, &graph, first.clone());
    let committed = store.committed_batch(first_batch).unwrap().unwrap();
    assert_eq!(committed.envelopes.len(), 1);
    assert!(matches!(
        committed.outcome,
        CommittedNormalizationOutcome::Accepted { .. }
    ));
    assert!(matches!(
        committed.lifecycle.as_slice(),
        [NormalizedLifecycleEvent::Added { .. }]
    ));

    let duplicate = store
        .preflight(PreflightRequest {
            event: first.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable(
                "another-delivery".to_string(),
            )),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: first.received_at(),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    assert!(matches!(
        duplicate,
        PreflightResult::ExistingCommitted(id) if id == first_batch
    ));

    let second_batch = evaluate_and_commit(store, &graph, event(2, SourceOperation::Update));
    let second = store.committed_batch(second_batch).unwrap().unwrap();
    assert!(matches!(
        second.lifecycle.as_slice(),
        [NormalizedLifecycleEvent::Equivalent { .. }]
    ));
    let state = store.source_state(first.key()).unwrap().unwrap();
    assert_eq!(state.active_outputs.len(), 1);
    assert_eq!(state.lifecycle, SourceLifecycleState::Active);

    let delete = event(3, SourceOperation::Delete);
    let preflight = store
        .preflight(PreflightRequest {
            event: delete.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable("delete-3".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: delete.received_at(),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    let PreflightResult::Reserved(reservation) = preflight else {
        panic!("expected delete reservation");
    };
    let token = store.route_only_compare_token(&reservation).unwrap();
    let delete_batch = match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: token,
            input: ApplicationCommitInput::LifecycleOnlyDelete,
            replacement_policy: ReplacementPolicy::Patch,
            maximum_active_outputs: 32,
            publication_sink: Some("committed-jsonl".to_string()),
            committed_at: delete.received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected delete result: {other:?}"),
    };
    let delete_batch = store.committed_batch(delete_batch).unwrap().unwrap();
    assert!(matches!(
        delete_batch.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    ));
    assert!(matches!(
        delete_batch.lifecycle.as_slice(),
        [NormalizedLifecycleEvent::Withdrawn { .. }]
    ));
    let state = store.source_state(first.key()).unwrap().unwrap();
    assert!(state.active_outputs.is_empty());
    assert_eq!(state.lifecycle, SourceLifecycleState::Deleted);

    let stale = store
        .preflight(PreflightRequest {
            event: event(2, SourceOperation::Create),
            delivery_identity: Some(DurableDeliveryIdentity::Stable("stale".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: timestamp("2026-08-06T00:00:02Z"),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    assert!(matches!(
        stale,
        PreflightResult::Stale { latest_revision: 3 }
    ));
    assert_eq!(store.recorded_receipts().unwrap().len(), 3);
    assert_eq!(
        store
            .checkpoint("jsonl:state-test")
            .unwrap()
            .unwrap()
            .commit_index,
        3
    );

    let leases = store
        .lease_publications(
            16,
            timestamp("2026-08-06T00:00:10Z"),
            timestamp("2026-08-06T00:01:00Z"),
        )
        .unwrap();
    assert_eq!(leases.len(), 3);
    let first_lease = leases[0].clone();
    let published = store
        .acknowledge_publication(first_lease.fence, timestamp("2026-08-06T00:00:30Z"))
        .unwrap();
    assert!(matches!(published, PublicationState::Published { .. }));
    assert!(
        store
            .acknowledge_publication(first_lease.fence, timestamp("2026-08-06T00:00:31Z"))
            .is_err()
    );
}

#[test]
fn in_memory_store_passes_durable_application_conformance() {
    run_store_conformance(&MemorySourceStateStore::new());
}

#[test]
fn sqlite_store_passes_conformance_and_recovers_after_restart() {
    let path = temporary_database_path();
    let first_batch;
    {
        let store = SqliteSourceStateStore::open(&path).unwrap();
        run_store_conformance(&store);
        store.quick_check().unwrap();
        first_batch = store
            .recorded_receipts()
            .unwrap()
            .first()
            .map(|receipt| receipt.applied_event_id)
            .unwrap();
    }
    {
        let store = SqliteSourceStateStore::open(&path).unwrap();
        store.quick_check().unwrap();
        assert_eq!(store.recorded_receipts().unwrap().len(), 3);
        assert!(
            store
                .source_state(event(1, SourceOperation::Create).key())
                .unwrap()
                .is_some()
        );
        let checkpoint = store.checkpoint("jsonl:state-test").unwrap().unwrap();
        assert_eq!(checkpoint.commit_index, 3);
        let duplicate = store
            .preflight(PreflightRequest {
                event: event(1, SourceOperation::Create),
                delivery_identity: Some(DurableDeliveryIdentity::Stable(
                    "restart-duplicate".to_string(),
                )),
                source_adapter: adapter(),
                adapter_evidence: None,
                execution_identity: None,
                requested_at: timestamp("2026-08-06T00:02:00Z"),
                expires_at: timestamp("2026-08-06T00:03:00Z"),
            })
            .unwrap();
        assert!(matches!(duplicate, PreflightResult::ExistingCommitted(_)));
        let recovered = store
            .lease_publications(
                3,
                timestamp("2026-08-06T00:02:00Z"),
                timestamp("2026-08-06T00:03:00Z"),
            )
            .unwrap();
        assert_eq!(recovered.len(), 2);
        assert_ne!(first_batch.as_bytes(), &[0; 32]);
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[test]
fn operational_failure_is_recorded_without_checkpoint_or_batch() {
    let store = MemorySourceStateStore::new();
    let source = failure_event();
    let preflight = store
        .preflight(PreflightRequest {
            event: source.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable("failure-1".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: source.received_at(),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    let PreflightResult::Reserved(reservation) = preflight else {
        panic!("expected failure reservation");
    };

    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let producer = bind_pre_normalized_producer(
        descriptor(
            ComponentKind::PreNormalizedProducer,
            "failing-state-producer",
        ),
        &config,
        |_| Ok(FailingProducer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_compatibility(
        PipelineId::try_new("failing-state", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        producer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "failing-state",
        1,
        RouteSelector::any(),
        pipeline.identity().clone(),
    )
    .unwrap();
    let graph = CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap();
    let prepared = match graph.route(EvaluationInput::new(source.clone(), adapter(), None)) {
        RouteEvaluation::Prepared(prepared) => prepared,
        RouteEvaluation::Completed(_) => panic!("expected selected failure pipeline"),
    };
    let PipelineEvaluationResult::Failed(failure) =
        prepared.evaluate(&BaseContextSnapshot::empty(source.received_at()))
    else {
        panic!("expected operational failure");
    };
    store
        .record_evaluation_failure(
            reservation.applied_event_id,
            reservation.fence,
            &failure,
            source.received_at(),
        )
        .unwrap();
    assert_eq!(store.evaluation_attempts().unwrap().len(), 1);
    assert!(store.checkpoint("failure:state-test").unwrap().is_none());
    assert!(
        store
            .committed_batch(CommittedBatchId::from_bytes([0; 32]))
            .unwrap()
            .is_none()
    );
}

#[test]
fn stale_reservation_fence_cannot_create_a_compare_token() {
    let store = MemorySourceStateStore::new();
    let source = failure_event();
    let preflight = store
        .preflight(PreflightRequest {
            event: source.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable("fence-1".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: source.received_at(),
            expires_at: reservation_expiry(),
        })
        .unwrap();
    let PreflightResult::Reserved(mut reservation) = preflight else {
        panic!("expected reservation");
    };
    reservation.fence.generation += 1;
    assert!(store.route_only_compare_token(&reservation).is_err());
}

#[test]
fn unversioned_delivery_identity_is_idempotent() {
    let store = MemorySourceStateStore::new();
    let source = failure_event();
    let request = || PreflightRequest {
        event: source.clone(),
        delivery_identity: Some(DurableDeliveryIdentity::Stable("unversioned-1".to_string())),
        source_adapter: adapter(),
        adapter_evidence: Some(vec![1, 2, 3]),
        execution_identity: None,
        requested_at: source.received_at(),
        expires_at: reservation_expiry(),
    };
    let PreflightResult::Reserved(first) = store.preflight(request()).unwrap() else {
        panic!("expected first reservation");
    };
    let PreflightResult::Reserved(second) = store.preflight(request()).unwrap() else {
        panic!("expected duplicate reservation");
    };
    assert_eq!(first.applied_event_id, second.applied_event_id);
    assert_eq!(first.fence, second.fence);
    let receipts = store.recorded_receipts().unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        receipts[0].adapter_evidence.as_deref(),
        Some([1, 2, 3].as_slice())
    );
    assert_eq!(receipts[0].source_adapter.id, "state-test-adapter");
}

#[test]
fn expired_reservation_is_reclaimed_with_a_new_generation() {
    let store = MemorySourceStateStore::new();
    let source = failure_event();
    let first = store
        .preflight(PreflightRequest {
            event: source.clone(),
            delivery_identity: Some(DurableDeliveryIdentity::Stable("reclaim-1".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: timestamp("2026-08-06T01:00:01Z"),
            expires_at: timestamp("2026-08-06T01:00:02Z"),
        })
        .unwrap();
    let PreflightResult::Reserved(first) = first else {
        panic!("expected first reservation");
    };
    let reclaimed = store
        .preflight(PreflightRequest {
            event: source,
            delivery_identity: Some(DurableDeliveryIdentity::Stable("reclaim-1".to_string())),
            source_adapter: adapter(),
            adapter_evidence: None,
            execution_identity: None,
            requested_at: timestamp("2026-08-06T01:00:03Z"),
            expires_at: timestamp("2026-08-06T01:01:00Z"),
        })
        .unwrap();
    let PreflightResult::Reserved(reclaimed) = reclaimed else {
        panic!("expected reclaimed reservation");
    };
    assert_eq!(reclaimed.fence.reservation_id, first.fence.reservation_id);
    assert_eq!(reclaimed.fence.generation, first.fence.generation + 1);
    assert!(store.route_only_compare_token(&first).is_err());
    assert!(store.route_only_compare_token(&reclaimed).is_ok());
}

#[test]
fn expired_publication_lease_is_recovered_with_the_same_delivery() {
    let store = MemorySourceStateStore::new();
    evaluate_and_commit(&store, &graph(), event(1, SourceOperation::Create));
    let first = store
        .lease_publications(
            1,
            timestamp("2026-08-06T00:00:02Z"),
            timestamp("2026-08-06T00:00:03Z"),
        )
        .unwrap()
        .remove(0);
    let recovered = store
        .lease_publications(
            1,
            timestamp("2026-08-06T00:00:04Z"),
            timestamp("2026-08-06T00:00:05Z"),
        )
        .unwrap()
        .remove(0);
    assert_eq!(recovered.fence.delivery_id, first.fence.delivery_id);
    assert!(recovered.fence.generation > first.fence.generation);
    assert!(
        store
            .acknowledge_publication(first.fence, timestamp("2026-08-06T00:00:04Z"))
            .is_err()
    );
}

#[test]
fn sqlite_rejects_future_schema_and_malformed_state() {
    let future_path = temporary_database_path();
    {
        let store = SqliteSourceStateStore::open(&future_path).unwrap();
        store.quick_check().unwrap();
    }
    {
        let connection = rusqlite::Connection::open(&future_path).unwrap();
        connection
            .execute(
                "UPDATE ingestion_state SET schema_version = 99 WHERE singleton = 1",
                [],
            )
            .unwrap();
    }
    assert!(matches!(
        SqliteSourceStateStore::open(&future_path),
        Err(signal_parser::state::SourceStateError::UnsupportedSchemaVersion(99))
    ));
    let _ = std::fs::remove_file(&future_path);

    let malformed_path = temporary_database_path();
    {
        let store = SqliteSourceStateStore::open(&malformed_path).unwrap();
        store.quick_check().unwrap();
    }
    {
        let connection = rusqlite::Connection::open(&malformed_path).unwrap();
        connection
            .execute(
                "UPDATE ingestion_state SET payload = '{' WHERE singleton = 1",
                [],
            )
            .unwrap();
    }
    let store = SqliteSourceStateStore::open(&malformed_path).unwrap();
    assert!(store.recorded_receipts().is_err());
    let _ = std::fs::remove_file(&malformed_path);
}

#[test]
fn context_requirements_change_pipeline_identity() {
    let none = graph();
    let none_identity = none.identity();

    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let requirements = PipelineContextRequirements::new(
        Some(signal_parser::normalization::HistoryRequirement::new(
            signal_parser::normalization::ItemLimit::new(4),
            signal_parser::normalization::ByteLimit::new(4096),
            true,
            false,
        )),
        signal_parser::normalization::ParentRequirement::Optional,
        signal_parser::normalization::ItemLimit::new(4),
        signal_parser::normalization::ByteLimit::new(4096),
    );
    let custom_descriptor = |kind, id| {
        ComponentDescriptor::try_new(
            ComponentId::try_new(id, "component ID").unwrap(),
            kind,
            SemanticVersion::new(1, 0, 0),
            1,
            ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap(),
            requirements.clone(),
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
    };
    let decoder = bind_decoder(
        custom_descriptor(ComponentKind::Decoder, "state-test-decoder"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();
    let finalizer = bind_finalizer(
        custom_descriptor(ComponentKind::Finalizer, "state-test-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_structured(
        PipelineId::try_new("state-test", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "state-test",
        1,
        RouteSelector::try_new(
            Some(SourceId::new("jsonl:state-test").unwrap()),
            None,
            Some(signal_parser::normalization::PayloadKind::Structured),
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
    let with_history = CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap();
    assert_ne!(none_identity, with_history.identity());
}

fn temporary_database_path() -> PathBuf {
    let unique = format!(
        "quant-system-state-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::temp_dir().join(unique)
}
