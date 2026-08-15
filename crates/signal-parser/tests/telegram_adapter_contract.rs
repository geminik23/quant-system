use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::NaiveDateTime;
use signal_parser::adapters::telegram::{
    TelegramAdaptationOutcome, TelegramAdapterConfig, TelegramAdapterError, TelegramAdapterPath,
    TelegramBatchPosition, TelegramBatchSourceAdapter, TelegramRelayInput, TelegramRelayOperation,
    TelegramRelaySourceAdapter, TelegramSourceEvidenceV1, TelegramTimestampRule,
    bind_legacy_telegram_producer, telegram_event_id, telegram_thread_id,
};
use signal_parser::ingestion::{
    DateTimeUtc, SourceEvent, SourceId, SourceOperation, SourcePayload, SourceRevision,
    SourceTimestampQuality, TextFormat,
};
use signal_parser::normalization::{
    CompiledPipeline, CompiledRoutingGraph, EvaluationInput, NormalizationOutcome,
    PipelineEvaluationResult, PipelineId, RouteEvaluation, RouteSelector, RouteSpec,
    SemanticVersion,
};
use signal_parser::state::{
    ApplicationCommitInput, CommittedBatchId, CommittedNormalizationOutcome,
    CompareAndCommitRequest, CompareAndCommitResult, DurableDeliveryIdentity,
    MemorySourceStateStore, NormalizedLifecycleEvent, PreflightRequest, PreflightResult,
    ReplacementPolicy, SnapshotRequest, SourceLifecycleState, SourceStateStore,
    SqliteSourceStateStore,
};
use signal_parser::{
    ChannelParser, ParseContext, ParsedAction, ParserRegistry, RawSignal, RawTgMessage,
};

const CHAT_ID: i64 = 4_242;
const ROOT_ID: i64 = 100;
const REPLY_ID: i64 = 101;
const AFTER_DELETE_ID: i64 = 102;
const SOURCE_ID: &str = "telegram:synthetic";

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn adapter_config() -> TelegramAdapterConfig {
    TelegramAdapterConfig::new(SourceId::new(SOURCE_ID).unwrap(), false, false)
}

fn accepted(
    outcome: TelegramAdaptationOutcome,
) -> (
    SourceEvent,
    TelegramSourceEvidenceV1,
    DurableDeliveryIdentity,
) {
    match outcome {
        TelegramAdaptationOutcome::Accepted {
            event,
            evidence,
            delivery_identity,
        } => (*event, evidence, delivery_identity),
        TelegramAdaptationOutcome::Ignored { reason, .. } => {
            panic!("expected accepted adaptation, got ignored: {reason:?}")
        }
        TelegramAdaptationOutcome::Rejected { diagnostic, .. } => {
            panic!("expected accepted adaptation, got rejected: {diagnostic:?}")
        }
    }
}

fn text_payload(event: &SourceEvent) -> &str {
    match event.payload() {
        SourcePayload::Text(payload) => payload.text().as_str(),
        other => panic!("expected text payload, got {other:?}"),
    }
}

#[test]
fn canonical_identities_cover_integer_and_configuration_boundaries() {
    assert_eq!(
        telegram_event_id(i64::MIN, i64::MAX),
        "tgmsg:v1:-9223372036854775808:9223372036854775807"
    );
    assert_eq!(
        telegram_thread_id(i64::MIN),
        "tgchat:v1:-9223372036854775808"
    );

    let batch = TelegramBatchSourceAdapter::try_new(adapter_config()).unwrap();
    let same_batch = TelegramBatchSourceAdapter::try_new(adapter_config()).unwrap();
    let relay = TelegramRelaySourceAdapter::try_new(adapter_config()).unwrap();
    let permissive_batch = TelegramBatchSourceAdapter::try_new(TelegramAdapterConfig::new(
        SourceId::new(SOURCE_ID).unwrap(),
        true,
        false,
    ))
    .unwrap();
    let other_source_batch = TelegramBatchSourceAdapter::try_new(TelegramAdapterConfig::new(
        SourceId::new("telegram:other").unwrap(),
        false,
        false,
    ))
    .unwrap();

    assert_eq!(
        batch.source_adapter_identity(),
        same_batch.source_adapter_identity()
    );
    assert_eq!(
        batch.source_adapter_identity().id().as_str(),
        "telegram-batch-source-adapter"
    );
    assert_eq!(
        relay.source_adapter_identity().id().as_str(),
        "telegram-relay-source-adapter"
    );
    let batch_policy = batch
        .source_adapter_identity()
        .config_identity()
        .expect("Telegram adapter configuration is identified");
    assert!(
        batch_policy
            .as_slice()
            .windows(SOURCE_ID.len())
            .any(|window| window == SOURCE_ID.as_bytes())
    );
    assert!(relay.source_adapter_identity().config_identity().is_some());
    assert!(
        permissive_batch
            .source_adapter_identity()
            .config_identity()
            .is_some()
    );
    assert!(
        other_source_batch
            .source_adapter_identity()
            .config_identity()
            .is_some()
    );
    assert_ne!(
        batch.source_adapter_identity().config_identity(),
        relay.source_adapter_identity().config_identity()
    );
    assert_ne!(
        batch.source_adapter_identity().config_identity(),
        permissive_batch.source_adapter_identity().config_identity()
    );
    assert_ne!(
        batch.source_adapter_identity().config_identity(),
        other_source_batch
            .source_adapter_identity()
            .config_identity()
    );
}

#[test]
fn batch_upsert_preserves_offset_evidence_and_offline_delivery() {
    let adapter = TelegramBatchSourceAdapter::try_new(adapter_config()).unwrap();
    let message = RawTgMessage {
        chat_id: CHAT_ID,
        msg_id: REPLY_ID,
        ts: "2026-08-07T12:34:56.125+05:30".to_string(),
        message: "synthetic update".to_string(),
        reply_to: Some(ROOT_ID),
    };
    let outcome = adapter
        .adapt(
            &message,
            timestamp("2026-08-07T07:05:00Z"),
            TelegramBatchPosition::try_new("fixture.jsonl", 17).unwrap(),
        )
        .unwrap();
    let (event, evidence, delivery_identity) = accepted(outcome);

    assert_eq!(event.key().source().as_str(), SOURCE_ID);
    assert_eq!(event.key().external_id().as_str(), "tgmsg:v1:4242:101");
    assert_eq!(event.operation(), SourceOperation::Upsert);
    assert_eq!(event.revision(), &SourceRevision::Unversioned);
    assert_eq!(event.thread().unwrap().as_str(), "tgchat:v1:4242");
    assert_eq!(
        event.parent().unwrap().external_id().as_str(),
        "tgmsg:v1:4242:100"
    );
    assert_eq!(text_payload(&event), "synthetic update");
    assert_eq!(
        event.occurred_at().value(),
        timestamp("2026-08-07T07:04:56.125Z")
    );
    assert_eq!(
        event.occurred_at().quality(),
        SourceTimestampQuality::SourceProvided
    );
    assert_eq!(
        delivery_identity,
        DurableDeliveryIdentity::OfflinePosition {
            artifact: "fixture.jsonl".to_string(),
            ordinal: 17,
        }
    );

    assert_eq!(evidence.path(), TelegramAdapterPath::Batch);
    assert_eq!(evidence.chat_id(), CHAT_ID);
    assert_eq!(evidence.message_id(), Some(REPLY_ID));
    assert_eq!(evidence.reply_to(), Some(ROOT_ID));
    assert_eq!(evidence.operation(), SourceOperation::Upsert);
    assert_eq!(
        evidence.timestamp().original_text(),
        Some("2026-08-07T12:34:56.125+05:30")
    );
    assert_eq!(
        evidence.timestamp().rule(),
        TelegramTimestampRule::ExplicitOffset
    );
    assert_eq!(evidence.ingress_delivery_id(), Some("fixture.jsonl:17"));

    let encoded = evidence.encode().unwrap();
    assert_eq!(
        TelegramSourceEvidenceV1::decode(&encoded).unwrap(),
        evidence
    );

    let mut unknown: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    unknown["unexpected"] = serde_json::json!(true);
    assert!(TelegramSourceEvidenceV1::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());

    let mut missing: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    missing.as_object_mut().unwrap().remove("timestamp");
    assert!(TelegramSourceEvidenceV1::decode(&serde_json::to_vec(&missing).unwrap()).is_err());

    let mut future: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    future["schema_version"] = serde_json::json!(2);
    assert!(matches!(
        TelegramSourceEvidenceV1::decode(&serde_json::to_vec(&future).unwrap()),
        Err(TelegramAdapterError::UnsupportedEvidenceVersion(2))
    ));
}

#[test]
fn relay_new_edit_mapping_and_timestamp_fallback_are_one_shot() {
    let adapter = TelegramRelaySourceAdapter::try_new(adapter_config()).unwrap();
    let received_at = timestamp("2026-08-07T10:00:00Z");
    let new_outcomes = adapter
        .adapt(
            TelegramRelayInput::try_new_message(
                TelegramRelayOperation::New,
                CHAT_ID,
                Some(ROOT_ID),
                Some("new message".to_string()),
                Some(0.25),
                None,
                "delivery-new",
            )
            .unwrap(),
            received_at,
        )
        .unwrap();
    assert_eq!(new_outcomes.len(), 1);
    let (new_event, new_evidence, new_delivery) =
        accepted(new_outcomes.into_iter().next().unwrap());
    assert_eq!(new_event.operation(), SourceOperation::Create);
    assert_eq!(
        new_event.occurred_at().value(),
        timestamp("1970-01-01T00:00:00.25Z")
    );
    assert_eq!(
        new_evidence.timestamp().rule(),
        TelegramTimestampRule::RelayEpoch
    );
    assert_eq!(
        new_evidence.timestamp().relay_epoch_bits(),
        Some(0.25_f64.to_bits())
    );
    assert_eq!(
        new_delivery,
        DurableDeliveryIdentity::Stable("telegram-relay-v1:delivery-new:0".to_string())
    );

    for (delivery_id, epoch_seconds, expected_rule) in [
        (
            "delivery-edit-missing",
            None,
            TelegramTimestampRule::ReceptionFallbackMissing,
        ),
        (
            "delivery-edit-invalid",
            Some(f64::NAN),
            TelegramTimestampRule::ReceptionFallbackInvalid,
        ),
    ] {
        let outcomes = adapter
            .adapt(
                TelegramRelayInput::try_new_message(
                    TelegramRelayOperation::Edit,
                    CHAT_ID,
                    Some(ROOT_ID),
                    Some("edited message".to_string()),
                    epoch_seconds,
                    None,
                    delivery_id,
                )
                .unwrap(),
                received_at,
            )
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        let (event, evidence, delivery) = accepted(outcomes.into_iter().next().unwrap());
        assert_eq!(event.operation(), SourceOperation::Update);
        assert_eq!(event.occurred_at().value(), received_at);
        assert_eq!(
            event.occurred_at().quality(),
            SourceTimestampQuality::ReceptionFallback
        );
        assert_eq!(evidence.timestamp().rule(), expected_rule);
        assert_eq!(
            delivery,
            DurableDeliveryIdentity::Stable(format!("telegram-relay-v1:{delivery_id}:0"))
        );
    }
}

#[test]
fn relay_delete_is_ordered_deduplicated_and_payload_free() {
    let adapter = TelegramRelaySourceAdapter::try_new(adapter_config()).unwrap();
    let received_at = timestamp("2026-08-07T11:00:00Z");
    let outcomes = adapter
        .adapt(
            TelegramRelayInput::try_new_delete(CHAT_ID, vec![9, 4, 9, 7, 4], "delivery-delete")
                .unwrap(),
            received_at,
        )
        .unwrap();

    assert_eq!(outcomes.len(), 3);
    for (ordinal, (outcome, expected_id)) in outcomes.into_iter().zip([9, 4, 7]).enumerate() {
        let (event, evidence, delivery) = accepted(outcome);
        assert_eq!(event.operation(), SourceOperation::Delete);
        assert_eq!(
            event.key().external_id().as_str(),
            telegram_event_id(CHAT_ID, expected_id)
        );
        assert_eq!(event.payload(), &SourcePayload::Empty);
        assert!(event.parent().is_none());
        assert_eq!(event.occurred_at().value(), received_at);
        assert_eq!(
            event.occurred_at().quality(),
            SourceTimestampQuality::ReceptionFallback
        );
        assert_eq!(evidence.message_id(), Some(expected_id));
        assert_eq!(evidence.reply_to(), None);
        assert_eq!(evidence.operation(), SourceOperation::Delete);
        assert_eq!(evidence.ingress_delivery_id(), Some("delivery-delete"));
        assert_eq!(
            delivery,
            DurableDeliveryIdentity::Stable(format!("telegram-relay-v1:delivery-delete:{ordinal}"))
        );
    }

    assert!(matches!(
        TelegramRelayInput::try_new_delete(CHAT_ID, vec![1; 257], "oversized-delete"),
        Err(TelegramAdapterError::DeleteLimitExceeded {
            maximum: 256,
            actual: 257,
        })
    ));
}

struct SyntheticHistoryParser {
    channels: [i64; 1],
}

impl ChannelParser for SyntheticHistoryParser {
    fn name(&self) -> &str {
        "synthetic-history"
    }

    fn channel_ids(&self) -> &[i64] {
        &self.channels
    }

    fn max_history(&self) -> usize {
        8
    }

    fn parse_root(&self, message: &str, ts: NaiveDateTime, context: &ParseContext) -> ParsedAction {
        match message {
            "root" => {
                assert_eq!(context.current_message().unwrap().msg_id, ROOT_ID);
                assert!(context.history.is_empty());
                ParsedAction::Signals(vec![
                    RawSignal::CloseAll { ts },
                    RawSignal::CancelAllPending { ts },
                ])
            }
            "after-delete" => {
                assert_eq!(context.current_message().unwrap().msg_id, AFTER_DELETE_ID);
                assert_eq!(
                    context
                        .history
                        .iter()
                        .map(|message| message.msg_id)
                        .collect::<Vec<_>>(),
                    [ROOT_ID, REPLY_ID]
                );
                ParsedAction::one(RawSignal::CloseAll { ts })
            }
            other => panic!("unexpected synthetic root message: {other}"),
        }
    }

    fn parse_reply(
        &self,
        message: &str,
        ts: NaiveDateTime,
        parent: Option<&RawTgMessage>,
        context: &ParseContext,
    ) -> ParsedAction {
        assert_eq!(message, "reply-edit");
        assert_eq!(context.current_message().unwrap().msg_id, REPLY_ID);
        assert_eq!(parent.unwrap().msg_id, ROOT_ID);
        assert_eq!(context.history.len(), 1);
        assert_eq!(context.history[0].msg_id, ROOT_ID);
        assert_eq!(context.ultimate_root_message().unwrap().msg_id, ROOT_ID);
        ParsedAction::Signals(vec![
            RawSignal::CancelAllPending { ts },
            RawSignal::CloseAll { ts },
        ])
    }
}

#[test]
fn legacy_producer_identity_uses_contract_and_history_policy() {
    let empty = bind_legacy_telegram_producer(Arc::new(ParserRegistry::new())).unwrap();

    let mut first_registry = ParserRegistry::new();
    first_registry.register(Box::new(SyntheticHistoryParser {
        channels: [CHAT_ID],
    }));
    let first = bind_legacy_telegram_producer(Arc::new(first_registry)).unwrap();

    let mut second_registry = ParserRegistry::new();
    second_registry.register(Box::new(SyntheticHistoryParser {
        channels: [CHAT_ID + 1],
    }));
    let second = bind_legacy_telegram_producer(Arc::new(second_registry)).unwrap();

    assert_eq!(first.resolved().id().as_str(), "telegram-legacy-producer");
    assert_eq!(
        first.resolved().implementation_version(),
        &SemanticVersion::new(1, 0, 0)
    );
    assert_eq!(first.resolved().contract_version(), 1);
    assert_eq!(first.resolved(), second.resolved());
    assert_ne!(
        first.resolved().config_identity(),
        empty.resolved().config_identity()
    );
}

struct LifecycleFixture {
    adapter: TelegramRelaySourceAdapter,
    graph: CompiledRoutingGraph,
}

impl LifecycleFixture {
    fn new() -> Self {
        let adapter = TelegramRelaySourceAdapter::try_new(adapter_config()).unwrap();
        let mut registry = ParserRegistry::new();
        registry.register(Box::new(SyntheticHistoryParser {
            channels: [CHAT_ID],
        }));
        let producer = bind_legacy_telegram_producer(Arc::new(registry)).unwrap();
        let pipeline = CompiledPipeline::compile_compatibility(
            PipelineId::try_new("telegram-legacy", "pipeline ID").unwrap(),
            SemanticVersion::new(1, 0, 0),
            producer,
        )
        .unwrap();
        let route = RouteSpec::try_new(
            "telegram-text",
            1,
            RouteSelector::try_new(
                Some(SourceId::new(SOURCE_ID).unwrap()),
                None,
                Some(signal_parser::normalization::PayloadKind::Text),
                None,
                None,
                Some(TextFormat::Plain),
                None,
                None,
                BTreeMap::new(),
            )
            .unwrap(),
            pipeline.identity().clone(),
        )
        .unwrap();
        let graph = CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap();
        Self { adapter, graph }
    }

    fn root(&self) -> AdaptedEvent {
        let outcomes = self
            .adapter
            .adapt(
                TelegramRelayInput::try_new_message(
                    TelegramRelayOperation::New,
                    CHAT_ID,
                    Some(ROOT_ID),
                    Some("root".to_string()),
                    Some(1_786_080_000.0),
                    None,
                    "root-delivery",
                )
                .unwrap(),
                timestamp("2026-08-07T12:00:00Z"),
            )
            .unwrap();
        AdaptedEvent::from_outcome(outcomes.into_iter().next().unwrap())
    }

    fn reply(&self) -> AdaptedEvent {
        let outcomes = self
            .adapter
            .adapt(
                TelegramRelayInput::try_new_message(
                    TelegramRelayOperation::Edit,
                    CHAT_ID,
                    Some(REPLY_ID),
                    Some("reply-edit".to_string()),
                    None,
                    Some(ROOT_ID),
                    "reply-delivery",
                )
                .unwrap(),
                timestamp("2026-08-07T12:01:00Z"),
            )
            .unwrap();
        AdaptedEvent::from_outcome(outcomes.into_iter().next().unwrap())
    }

    fn delete_reply(&self) -> AdaptedEvent {
        let outcomes = self
            .adapter
            .adapt(
                TelegramRelayInput::try_new_delete(
                    CHAT_ID,
                    vec![REPLY_ID],
                    "reply-delete-delivery",
                )
                .unwrap(),
                timestamp("2026-08-07T12:02:00Z"),
            )
            .unwrap();
        AdaptedEvent::from_outcome(outcomes.into_iter().next().unwrap())
    }

    fn after_delete(&self) -> AdaptedEvent {
        let outcomes = self
            .adapter
            .adapt(
                TelegramRelayInput::try_new_message(
                    TelegramRelayOperation::New,
                    CHAT_ID,
                    Some(AFTER_DELETE_ID),
                    Some("after-delete".to_string()),
                    None,
                    None,
                    "after-delete-delivery",
                )
                .unwrap(),
                timestamp("2026-08-07T12:03:00Z"),
            )
            .unwrap();
        AdaptedEvent::from_outcome(outcomes.into_iter().next().unwrap())
    }

    fn unregistered(&self) -> AdaptedEvent {
        let outcomes = self
            .adapter
            .adapt(
                TelegramRelayInput::try_new_message(
                    TelegramRelayOperation::New,
                    CHAT_ID + 1,
                    Some(200),
                    Some("unregistered".to_string()),
                    None,
                    None,
                    "unregistered-delivery",
                )
                .unwrap(),
                timestamp("2026-08-07T12:04:00Z"),
            )
            .unwrap();
        AdaptedEvent::from_outcome(outcomes.into_iter().next().unwrap())
    }
}

#[derive(Clone)]
struct AdaptedEvent {
    event: SourceEvent,
    evidence: TelegramSourceEvidenceV1,
    delivery_identity: DurableDeliveryIdentity,
}

impl AdaptedEvent {
    fn from_outcome(outcome: TelegramAdaptationOutcome) -> Self {
        let (event, evidence, delivery_identity) = accepted(outcome);
        Self {
            event,
            evidence,
            delivery_identity,
        }
    }
}

fn preflight(
    store: &dyn SourceStateStore,
    fixture: &LifecycleFixture,
    adapted: &AdaptedEvent,
) -> PreflightResult {
    store
        .preflight(PreflightRequest {
            event: adapted.event.clone(),
            delivery_identity: Some(adapted.delivery_identity.clone()),
            source_adapter: fixture.adapter.source_adapter_identity().clone(),
            adapter_evidence: Some(adapted.evidence.encode().unwrap()),
            execution_identity: None,
            requested_at: adapted.event.received_at(),
            expires_at: timestamp("2026-08-08T00:00:00Z"),
        })
        .unwrap()
}

fn signal_kind(signal: &RawSignal) -> &'static str {
    match signal {
        RawSignal::CloseAll { .. } => "close_all",
        RawSignal::CancelAllPending { .. } => "cancel_all_pending",
        other => panic!("unexpected synthetic signal: {other:?}"),
    }
}

fn evaluate_and_commit(
    store: &dyn SourceStateStore,
    fixture: &LifecycleFixture,
    adapted: &AdaptedEvent,
) -> (CommittedBatchId, Vec<&'static str>) {
    let PreflightResult::Reserved(reservation) = preflight(store, fixture, adapted) else {
        panic!("expected reservation");
    };
    let prepared = match fixture.graph.route(EvaluationInput::new(
        adapted.event.clone(),
        fixture.adapter.source_adapter_identity().clone(),
        None,
    )) {
        RouteEvaluation::Prepared(prepared) => prepared,
        RouteEvaluation::Completed(_) => panic!("expected selected compatibility pipeline"),
    };
    let snapshot = store
        .snapshot(SnapshotRequest {
            applied_event_id: reservation.applied_event_id,
            fence: reservation.fence,
            selected_pipeline: prepared.identity().selected_pipeline().unwrap().clone(),
            requirements: prepared.requirements().clone(),
            requested_at: adapted.event.received_at(),
        })
        .unwrap();
    let PipelineEvaluationResult::Completed(report) = prepared.evaluate(&snapshot.base_context)
    else {
        panic!("legacy producer evaluation failed operationally");
    };
    let NormalizationOutcome::Accepted { candidates } = report.outcome() else {
        panic!("legacy producer did not accept the synthetic message");
    };
    let order = candidates
        .as_slice()
        .iter()
        .enumerate()
        .map(|(ordinal, candidate)| {
            assert_eq!(candidate.candidate_ordinal(), ordinal as u32);
            signal_kind(candidate.signal())
        })
        .collect::<Vec<_>>();
    let batch_id = match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: snapshot.compare_token,
            input: ApplicationCommitInput::CompletedEvaluation(&report),
            replacement_policy: ReplacementPolicy::Patch,
            maximum_active_outputs: 32,
            publication_sink: None,
            committed_at: adapted.event.received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected commit result: {other:?}"),
    };
    (batch_id, order)
}

fn assert_duplicate(
    store: &dyn SourceStateStore,
    fixture: &LifecycleFixture,
    adapted: &AdaptedEvent,
    expected: CommittedBatchId,
) {
    assert!(matches!(
        preflight(store, fixture, adapted),
        PreflightResult::ExistingCommitted(batch_id) if batch_id == expected
    ));
}

fn commit_reply_delete(
    store: &dyn SourceStateStore,
    fixture: &LifecycleFixture,
    root_batch: CommittedBatchId,
    root: &AdaptedEvent,
) {
    assert_duplicate(store, fixture, root, root_batch);

    let reply = fixture.reply();
    let (reply_batch, reply_order) = evaluate_and_commit(store, fixture, &reply);
    assert_eq!(reply_order, ["cancel_all_pending", "close_all"]);
    let committed_reply = store.committed_batch(reply_batch).unwrap().unwrap();
    assert!(matches!(
        committed_reply.outcome,
        CommittedNormalizationOutcome::Accepted { .. }
    ));
    assert_eq!(committed_reply.lifecycle.len(), 2);
    assert!(
        committed_reply
            .lifecycle
            .iter()
            .all(|event| matches!(event, NormalizedLifecycleEvent::Added { .. }))
    );

    let delete = fixture.delete_reply();
    let PreflightResult::Reserved(reservation) = preflight(store, fixture, &delete) else {
        panic!("expected delete reservation");
    };
    let token = store.route_only_compare_token(&reservation).unwrap();
    let delete_batch = match store
        .compare_and_commit(CompareAndCommitRequest {
            compare_token: token,
            input: ApplicationCommitInput::LifecycleOnlyDelete,
            replacement_policy: ReplacementPolicy::Patch,
            maximum_active_outputs: 32,
            publication_sink: None,
            committed_at: delete.event.received_at(),
        })
        .unwrap()
    {
        CompareAndCommitResult::Committed(batch_id) => batch_id,
        other => panic!("unexpected delete commit result: {other:?}"),
    };
    let committed_delete = store.committed_batch(delete_batch).unwrap().unwrap();
    assert_eq!(
        committed_delete.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    );
    assert_eq!(committed_delete.lifecycle.len(), 2);
    assert!(
        committed_delete
            .lifecycle
            .iter()
            .all(|event| matches!(event, NormalizedLifecycleEvent::Withdrawn { .. }))
    );

    let reply_state = store.source_state(reply.event.key()).unwrap().unwrap();
    assert_eq!(reply_state.lifecycle, SourceLifecycleState::Deleted);
    assert!(reply_state.active_outputs.is_empty());
    let root_state = store.source_state(root.event.key()).unwrap().unwrap();
    assert_eq!(root_state.lifecycle, SourceLifecycleState::Active);
    assert_eq!(root_state.active_outputs.len(), 2);

    let receipts = store.recorded_receipts().unwrap();
    assert_eq!(receipts.len(), 3);
    assert_eq!(
        TelegramSourceEvidenceV1::decode(receipts[0].adapter_evidence.as_ref().unwrap()).unwrap(),
        root.evidence
    );
}

#[test]
fn memory_store_runs_legacy_graph_duplicate_history_and_delete_lifecycle() {
    let fixture = LifecycleFixture::new();
    let store = MemorySourceStateStore::new();
    let root = fixture.root();
    let (root_batch, root_order) = evaluate_and_commit(&store, &fixture, &root);
    assert_eq!(root_order, ["close_all", "cancel_all_pending"]);

    commit_reply_delete(&store, &fixture, root_batch, &root);
}

#[test]
fn sqlite_store_restarts_before_duplicate_history_and_delete_lifecycle() {
    let fixture = LifecycleFixture::new();
    let path = temporary_database_path();
    let root = fixture.root();
    let root_batch;
    {
        let store = SqliteSourceStateStore::open(&path).unwrap();
        let (batch, root_order) = evaluate_and_commit(&store, &fixture, &root);
        root_batch = batch;
        assert_eq!(root_order, ["close_all", "cancel_all_pending"]);
        store.quick_check().unwrap();
    }
    {
        let store = SqliteSourceStateStore::open(&path).unwrap();
        store.quick_check().unwrap();
        commit_reply_delete(&store, &fixture, root_batch, &root);
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[test]
fn lifecycle_delete_receipt_is_excluded_from_later_parser_history() {
    let fixture = LifecycleFixture::new();
    let store = MemorySourceStateStore::new();
    let root = fixture.root();
    let (root_batch, _) = evaluate_and_commit(&store, &fixture, &root);
    commit_reply_delete(&store, &fixture, root_batch, &root);

    let after_delete = fixture.after_delete();
    let (_, order) = evaluate_and_commit(&store, &fixture, &after_delete);
    assert_eq!(order, ["close_all"]);
}

#[test]
fn unregistered_channel_completes_with_ignored_outcome() {
    let fixture = LifecycleFixture::new();
    let store = MemorySourceStateStore::new();
    let unregistered = fixture.unregistered();
    let PreflightResult::Reserved(reservation) = preflight(&store, &fixture, &unregistered) else {
        panic!("expected reservation");
    };
    let prepared = match fixture.graph.route(EvaluationInput::new(
        unregistered.event.clone(),
        fixture.adapter.source_adapter_identity().clone(),
        None,
    )) {
        RouteEvaluation::Prepared(prepared) => prepared,
        RouteEvaluation::Completed(_) => panic!("expected selected compatibility pipeline"),
    };
    let snapshot = store
        .snapshot(SnapshotRequest {
            applied_event_id: reservation.applied_event_id,
            fence: reservation.fence,
            selected_pipeline: prepared.identity().selected_pipeline().unwrap().clone(),
            requirements: prepared.requirements().clone(),
            requested_at: unregistered.event.received_at(),
        })
        .unwrap();

    let PipelineEvaluationResult::Completed(report) = prepared.evaluate(&snapshot.base_context)
    else {
        panic!("unregistered channel failed operationally");
    };
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Ignored { reason }
            if reason.as_str() == "telegram_unregistered_channel"
    ));
}

fn temporary_database_path() -> PathBuf {
    let unique = format!(
        "telegram-adapter-contract-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    std::env::temp_dir().join(unique)
}
