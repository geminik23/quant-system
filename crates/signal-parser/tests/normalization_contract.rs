use std::collections::BTreeMap;

use qs_core::RawSignal;
use signal_parser::ingestion::{
    BoundedBytes, BoundedText, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent,
    SourceEventKey, SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload, TextFormat, TextPayload,
};
use signal_parser::normalization::{
    BaseContextSnapshot, CanonicalRawSignalsDecoder, CanonicalWriter, CompiledPipeline,
    CompiledRoutingGraph, CompletionKnowledge, ComponentBindError, ComponentConfigSchemaRef,
    ComponentDescriptor, ComponentId, ComponentKind, ComponentReport, ComponentResult,
    ContractBytes, ContractList, ContractText, DiagnosticSet, DraftValidationStep,
    EmptyOutputPolicy, EvaluationFailureClass, EvaluationInput, EvaluationRetrySafety,
    EvaluationStage, EvidenceFact, GraphCompileError, MAX_CANONICAL_IDENTITY_BYTES, MeaningBatch,
    MeaningContract, MeaningEncoding, MeaningNormalizer, MeaningSchemaRef, MessageParser, NoConfig,
    NormalizationOutcome, ParsedMeaning, PayloadKind, PipelineContextRequirements,
    PipelineEvaluationResult, PipelineId, PreNormalizedProducer, PreNormalizedSignalBatch,
    RAW_SIGNALS_V1_SCHEMA, RouteEvaluation, RouteSelector, RouteSpec, SemanticVersion, SignalDraft,
    SignalDraftAction, SourceAdapterIdentity, StageExecutionFailure, StandardSignalFinalizer,
    StructuredInputCapability, VersionedMeaning, bind_decoder, bind_finalizer,
    bind_meaning_normalizer, bind_parser, bind_pre_normalized_producer, raw_signals_v1_schema,
};

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn event(value: serde_json::Value) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-05T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-05T00:00:01Z"),
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(serde_json::to_vec(&value).unwrap()).unwrap(),
        )),
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

fn pipeline(id: &str) -> CompiledPipeline {
    let schema = ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap();
    let config = NoConfig::new(schema);
    let decoder = bind_decoder(
        descriptor(ComponentKind::Decoder, "canonical-raw-signals"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();

    let finalizer = bind_finalizer(
        descriptor(ComponentKind::Finalizer, "standard-signal-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    CompiledPipeline::compile_structured(
        PipelineId::try_new(id, "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap()
}

fn source_adapter() -> SourceAdapterIdentity {
    SourceAdapterIdentity::without_config(
        ComponentId::try_new("jsonl-test-adapter", "adapter ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
    )
}

fn structured_selector() -> RouteSelector {
    structured_selector_for_source(None)
}

fn structured_selector_for_source(source: Option<&str>) -> RouteSelector {
    RouteSelector::try_new(
        source.map(|value| SourceId::new(value).unwrap()),
        None,
        Some(PayloadKind::Structured),
        Some(raw_signals_v1_schema()),
        Some(PayloadEncoding::Json),
        None,
        None,
        None,
        BTreeMap::new(),
    )
    .unwrap()
}

fn graph_for(pipelines: Vec<CompiledPipeline>, routes: Vec<RouteSpec>) -> CompiledRoutingGraph {
    CompiledRoutingGraph::compile(routes, pipelines).unwrap()
}

fn evaluate(graph: &CompiledRoutingGraph, event: SourceEvent) -> PipelineEvaluationResult {
    let received_at = event.received_at();
    let prepared = match graph.route(EvaluationInput::new(event, source_adapter(), None)) {
        RouteEvaluation::Prepared(value) => value,
        RouteEvaluation::Completed(_) => panic!("expected a selected pipeline"),
    };
    prepared.evaluate(&BaseContextSnapshot::empty(received_at))
}

fn text_event() -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("text:test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-05T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-05T00:00:01Z"),
        SourcePayload::Text(TextPayload::new(
            BoundedText::new("close all").unwrap(),
            TextFormat::Plain,
            None,
        )),
    )
}

fn empty_event() -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("compat:test").unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-05T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-05T00:00:01Z"),
        SourcePayload::Empty,
    )
}

fn meaning_contract() -> MeaningContract {
    MeaningContract::new(
        MeaningSchemaRef::new(
            ComponentId::try_new("example-close-all", "meaning schema").unwrap(),
            1,
        ),
        MeaningEncoding::CanonicalJson,
    )
}

fn stage_descriptor(
    kind: ComponentKind,
    id: &str,
    meaning_inputs: Vec<MeaningContract>,
    meaning_outputs: Vec<MeaningContract>,
) -> ComponentDescriptor {
    ComponentDescriptor::try_new(
        ComponentId::try_new(id, "component ID").unwrap(),
        kind,
        SemanticVersion::new(1, 0, 0),
        1,
        ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap(),
        PipelineContextRequirements::none(),
        EmptyOutputPolicy::Reject,
        vec![],
        meaning_inputs,
        meaning_outputs,
    )
    .unwrap()
}

struct CloseAllParser;

impl MessageParser for CloseAllParser {
    fn parse(
        &self,
        _event: &SourceEvent,
        _payload: &TextPayload,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<MeaningBatch> {
        let contract = meaning_contract();
        let meaning = VersionedMeaning::new(
            contract.schema().clone(),
            contract.encoding(),
            ContractBytes::try_new(b"{}".to_vec(), "meaning bytes").unwrap(),
        );
        Ok(ComponentReport::accepted(
            ContractList::try_new(vec![ParsedMeaning::Management(meaning)], "meanings").unwrap(),
        ))
    }
}

struct CloseAllNormalizer;

impl MeaningNormalizer for CloseAllNormalizer {
    fn normalize(
        &self,
        _meanings: MeaningBatch,
        event: &SourceEvent,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<signal_parser::normalization::DraftBatch> {
        let draft = SignalDraft::try_new(
            event.occurred_at().value(),
            None,
            SignalDraftAction::CloseAll,
            DiagnosticSet::empty(),
            vec![],
        )
        .unwrap();
        Ok(ComponentReport::accepted(
            ContractList::try_new(vec![draft], "drafts").unwrap(),
        ))
    }
}

struct CloseAllProducer;

impl PreNormalizedProducer for CloseAllProducer {
    fn produce(
        &self,
        event: &SourceEvent,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<PreNormalizedSignalBatch> {
        Ok(ComponentReport::accepted(
            PreNormalizedSignalBatch::try_new(vec![RawSignal::CloseAll {
                ts: event.occurred_at().value().into_inner().naive_utc(),
            }])
            .unwrap(),
        )
        .with_facts(vec![
            EvidenceFact::try_new("compatibility_path", "pre_normalized").unwrap(),
        ])
        .unwrap())
    }
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

fn all_actions() -> serde_json::Value {
    let ts = "2026-08-05T00:00:00+00:00";
    let position = serde_json::json!({"type":"ByTradeId","trade_id":"trade-1"});
    serde_json::json!({
        "schema_version": 1,
        "signals": [
            {"action":"Entry","ts":ts,"symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":null,"targets":[],"group":null,"trade_id":"trade-1"},
            {"action":"Close","ts":ts,"position":position},
            {"action":"ClosePartial","ts":ts,"position":position,"ratio":0.5},
            {"action":"ModifyStoploss","ts":ts,"position":position,"price":1.08},
            {"action":"MoveStoplossToEntry","ts":ts,"position":position},
            {"action":"AddTarget","ts":ts,"position":position,"price":1.10,"close_ratio":0.5},
            {"action":"RemoveTarget","ts":ts,"position":position,"price":1.10},
            {"action":"ModifyTarget","ts":ts,"position":position,"old_price":1.10,"new_price":1.11},
            {"action":"AddRule","ts":ts,"position":position,"rule":{"type":"TimeExit","max_seconds":60}},
            {"action":"RemoveRule","ts":ts,"position":position,"rule_name":"time-exit"},
            {"action":"ScaleIn","ts":ts,"position":position,"price":null,"size":0.1},
            {"action":"CancelPending","ts":ts,"position":position},
            {"action":"CloseAllOf","ts":ts,"symbol":"EURUSD"},
            {"action":"CloseAll","ts":ts},
            {"action":"CancelAllPending","ts":ts},
            {"action":"ModifyAllStoploss","ts":ts,"symbol":"EURUSD","price":1.08},
            {"action":"CloseAllInGroup","ts":ts,"group_id":"group-1"},
            {"action":"ModifyAllStoplossInGroup","ts":ts,"group_id":"group-1","price":1.08}
        ]
    })
}

#[test]
fn bounded_values_and_canonical_encoding_are_stable() {
    assert!(ContractText::<4>::try_new("hello", "text").is_err());
    assert!(ContractText::<8>::try_new("bad\n", "text").is_err());
    assert!(ContractList::<_, 1>::try_new(vec![1, 2], "list").is_err());

    let mut writer = CanonicalWriter::new();
    writer.bool(true);
    writer.u16(258);
    writer.u32(16_909_060);
    writer.i64(-2);
    writer.text("A").unwrap();
    assert_eq!(
        writer.into_bytes(),
        hex("01010201020304fffffffffffffffe0000000141")
    );
    assert!(signal_parser::normalization::CanonicalIdentityBytes::try_new(vec![]).is_err());
    assert!(
        signal_parser::normalization::CanonicalIdentityBytes::try_new(vec![
            0;
            MAX_CANONICAL_IDENTITY_BYTES
                + 1
        ])
        .is_err()
    );
}

#[test]
fn component_identity_retains_direct_canonical_configuration_bytes() {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let decoder = bind_decoder(
        descriptor(ComponentKind::Decoder, "direct-config-identity"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();
    let bytes = decoder
        .resolved()
        .config_identity()
        .canonical_bytes()
        .as_slice();

    assert!(
        bytes
            .windows("direct-config-identity".len())
            .any(|window| window == b"direct-config-identity")
    );
    assert!(
        bytes
            .windows("quant-system/no-config@1".len())
            .any(|window| window == b"quant-system/no-config@1")
    );
}

#[test]
fn typed_binding_rejects_component_kind_mismatch() {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let result = bind_finalizer(
        descriptor(ComponentKind::Decoder, "wrong-kind"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    );
    assert!(matches!(result, Err(ComponentBindError::KindMismatch)));
}

#[test]
fn strict_structured_pipeline_accepts_every_current_action_in_order() {
    let pipeline = pipeline("strict-json");
    let identity = pipeline.identity().clone();
    let route = RouteSpec::try_new("structured", 10, structured_selector(), identity).unwrap();
    let graph = graph_for(vec![pipeline], vec![route]);

    let result = evaluate(&graph, event(all_actions()));
    let PipelineEvaluationResult::Completed(report) = result else {
        panic!("evaluation failed operationally");
    };
    let NormalizationOutcome::Accepted { candidates } = report.outcome() else {
        panic!("strict valid batch was not accepted");
    };
    assert_eq!(candidates.as_slice().len(), 18);
    for (ordinal, candidate) in candidates.as_slice().iter().enumerate() {
        assert_eq!(candidate.candidate_ordinal(), ordinal as u32);
        assert_eq!(
            candidate.provenance().source_adapter().id().as_str(),
            "jsonl-test-adapter"
        );
    }
}

#[test]
fn strict_schema_rejects_missing_nullable_nested_unknown_and_trailing_bytes() {
    let pipeline = pipeline("strict-json");
    let identity = pipeline.identity().clone();
    let route = RouteSpec::try_new("structured", 10, structured_selector(), identity).unwrap();
    let graph = graph_for(vec![pipeline], vec![route]);

    let mut missing = all_actions();
    missing["signals"][0]
        .as_object_mut()
        .unwrap()
        .remove("price");
    assert_rejected(evaluate(&graph, event(missing)), "missing nullable field");

    let mut unknown = all_actions();
    unknown["signals"][1]["position"]["unexpected"] = serde_json::json!(true);
    assert_rejected(evaluate(&graph, event(unknown)), "unknown nested field");

    let mut source = event(all_actions());
    let SourcePayload::Structured(payload) = source.payload().clone() else {
        unreachable!();
    };
    let mut bytes = payload.data().as_slice().to_vec();
    bytes.extend_from_slice(b" true");
    source = SourceEvent::new(
        source.key().clone(),
        source.operation(),
        source.revision().clone(),
        source.occurred_at(),
        source.received_at(),
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(bytes).unwrap(),
        )),
    );
    assert_rejected(evaluate(&graph, source), "trailing bytes");
}

#[test]
fn routing_separates_terminal_outcomes_from_prepared_evaluation() {
    let first = pipeline("first");
    let second = pipeline("second");
    let first_identity = first.identity().clone();
    let second_identity = second.identity().clone();
    let graph = graph_for(
        vec![first, second],
        vec![
            RouteSpec::try_new("a", 10, structured_selector(), first_identity).unwrap(),
            RouteSpec::try_new(
                "b",
                10,
                structured_selector_for_source(Some("jsonl:test")),
                second_identity,
            )
            .unwrap(),
        ],
    );
    let result = graph.route(EvaluationInput::new(
        event(all_actions()),
        source_adapter(),
        None,
    ));
    let RouteEvaluation::Completed(report) = result else {
        panic!("cross-pipeline tie selected a pipeline");
    };
    assert!(report.identity().selected_pipeline().is_none());
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Ambiguous { .. }
    ));

    let no_route_graph = graph_for(vec![], vec![]);
    let result = no_route_graph.route(EvaluationInput::new(
        event(all_actions()),
        source_adapter(),
        None,
    ));
    let RouteEvaluation::Completed(report) = result else {
        panic!("empty route table selected a pipeline");
    };
    assert!(report.identity().selected_pipeline().is_none());
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Ignored { .. }
    ));
}

#[test]
fn identical_selectors_cannot_target_different_pipelines() {
    let first = pipeline("first");
    let second = pipeline("second");
    let routes = vec![
        RouteSpec::try_new("first", 1, structured_selector(), first.identity().clone()).unwrap(),
        RouteSpec::try_new(
            "second",
            2,
            structured_selector(),
            second.identity().clone(),
        )
        .unwrap(),
    ];
    assert!(matches!(
        CompiledRoutingGraph::compile(routes, vec![first, second]),
        Err(GraphCompileError::ConflictingSelector)
    ));
}

#[test]
fn text_and_compatibility_shapes_use_the_same_candidate_boundary() {
    let schema = ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap();
    let config = NoConfig::new(schema);
    let parser = bind_parser(
        stage_descriptor(
            ComponentKind::Parser,
            "close-all-parser",
            vec![],
            vec![meaning_contract()],
        ),
        &config,
        |_| Ok(CloseAllParser),
    )
    .unwrap();
    let normalizer = bind_meaning_normalizer(
        stage_descriptor(
            ComponentKind::MeaningNormalizer,
            "close-all-normalizer",
            vec![meaning_contract()],
            vec![],
        ),
        &config,
        |_| Ok(CloseAllNormalizer),
    )
    .unwrap();
    let finalizer = bind_finalizer(
        descriptor(ComponentKind::Finalizer, "standard-signal-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    let text_pipeline = CompiledPipeline::compile_text(
        PipelineId::try_new("text", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        parser,
        normalizer,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap();
    let text_route = RouteSpec::try_new(
        "text",
        1,
        RouteSelector::try_new(
            None,
            None,
            Some(PayloadKind::Text),
            None,
            None,
            Some(TextFormat::Plain),
            None,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
        text_pipeline.identity().clone(),
    )
    .unwrap();
    let result = evaluate(
        &graph_for(vec![text_pipeline], vec![text_route]),
        text_event(),
    );
    let PipelineEvaluationResult::Completed(report) = result else {
        panic!("text evaluation failed");
    };
    assert!(matches!(
        report.outcome(),
        NormalizationOutcome::Accepted { .. }
    ));

    let producer = bind_pre_normalized_producer(
        stage_descriptor(
            ComponentKind::PreNormalizedProducer,
            "close-all-producer",
            vec![],
            vec![],
        ),
        &config,
        |_| Ok(CloseAllProducer),
    )
    .unwrap();
    let compat = CompiledPipeline::compile_compatibility(
        PipelineId::try_new("compat", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        producer,
    )
    .unwrap();
    let route =
        RouteSpec::try_new("compat", 1, RouteSelector::any(), compat.identity().clone()).unwrap();
    let result = evaluate(&graph_for(vec![compat], vec![route]), empty_event());
    let PipelineEvaluationResult::Completed(report) = result else {
        panic!("compatibility evaluation failed");
    };
    let NormalizationOutcome::Accepted { candidates } = report.outcome() else {
        panic!("compatibility evaluation was not accepted");
    };
    let stage = &candidates.as_slice()[0].evidence().components()[0];
    assert_eq!(stage.stage(), EvaluationStage::Finalization);
    assert_eq!(stage.component().id().as_str(), "close-all-producer");
    assert_eq!(stage.facts()[0].code().as_str(), "compatibility_path");
    assert_eq!(stage.facts()[0].value().as_str(), "pre_normalized");
}

#[test]
fn operational_failure_is_not_a_semantic_disposition() {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let producer = bind_pre_normalized_producer(
        stage_descriptor(
            ComponentKind::PreNormalizedProducer,
            "failing-producer",
            vec![],
            vec![],
        ),
        &config,
        |_| Ok(FailingProducer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_compatibility(
        PipelineId::try_new("failing", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        producer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "failing",
        1,
        RouteSelector::any(),
        pipeline.identity().clone(),
    )
    .unwrap();
    let result = evaluate(&graph_for(vec![pipeline], vec![route]), empty_event());
    assert!(matches!(result, PipelineEvaluationResult::Failed(_)));
}

#[test]
fn route_declaration_order_does_not_change_identity() {
    let first = pipeline("first");
    let second = pipeline("second");
    let first_identity = first.identity().clone();
    let second_identity = second.identity().clone();
    let routes = vec![
        RouteSpec::try_new("a", 20, structured_selector(), first_identity).unwrap(),
        RouteSpec::try_new(
            "b",
            10,
            structured_selector_for_source(Some("jsonl:test")),
            second_identity,
        )
        .unwrap(),
    ];
    let left = graph_for(vec![first.clone(), second.clone()], routes.clone());
    let right = graph_for(vec![first, second], routes.into_iter().rev().collect());
    assert_eq!(left.identity(), right.identity());
}

fn assert_rejected(result: PipelineEvaluationResult, case: &str) {
    let PipelineEvaluationResult::Completed(report) = result else {
        panic!("expected semantic rejection");
    };
    assert!(
        matches!(report.outcome(), NormalizationOutcome::Rejected { .. }),
        "{case}: got {:?}",
        report.outcome()
    );
}

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

#[test]
fn built_in_schema_name_is_exact() {
    assert_eq!(raw_signals_v1_schema().as_str(), RAW_SIGNALS_V1_SCHEMA);
    assert_eq!(RAW_SIGNALS_V1_SCHEMA, "quant-system/raw-signals@1");
}
