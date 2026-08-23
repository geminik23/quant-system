use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{
    EventMetadata, FallibleBatchFeed, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch,
};
use qs_backtest::report::{BacktestResult, TradeResult};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    AnalysisPipeline, AnnotationId, AnnotationLimits, AnnotationUse, BacktestRunner, BarSeriesSpec,
    FutureQuoteConfig, HistoricalStrategy, JournalKind, MAX_ANNOTATIONS, MissingIntervalPolicy,
    ObservationStoreLimits, PriceBasis, RawSignal, SeriesId, SeriesRequirement, StrategyAnnotation,
    StrategyBacktestResult, StrategyContext, StrategyDecisionDraft, StrategyDecisionKind,
    StrategyDecisionRecorder, StrategyDescriptor, StrategyEvent, StrategyExperimentComparison,
    StrategyId, StrategyJournalDraft, StrategyJournalError, StrategyJournalRecord,
    StrategyJournalRecorder, StrategyObservationValue, StrategyOutput, StrategyRequirements,
    StrategyResearchLimits, StrategyResearchOutput, StrategyRetentionLimits, Timeframe,
    WarmupRequirement,
};
use qs_core::types::{CloseReason, OrderType, PositionId, Side};
use qs_symbols::SymbolSpec;
use serde::de::{self, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

const SYMBOL: &str = "EURUSD";

fn ts(minute: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 2, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::minutes(minute)
}

fn event(minute: i64, row: u64, bid: f64) -> FeedEvent {
    FeedEvent::new(
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(minute),
            bid,
            ask: bid + 0.0002,
        },
        EventMetadata::new(SeriesRoles::PRIMARY, 0, row),
    )
}

fn requirement(warmup: usize) -> SeriesRequirement {
    SeriesRequirement::new(
        SeriesId::new("m1").unwrap(),
        SYMBOL,
        Timeframe::minutes(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(warmup).unwrap(),
    )
    .unwrap()
}

fn requirements(warmup: usize) -> StrategyRequirements {
    StrategyRequirements::new(
        vec![SYMBOL.into()],
        vec![requirement(warmup)],
        0,
        true,
        true,
    )
    .unwrap()
}

fn spec(warmup: usize) -> BarSeriesSpec {
    BarSeriesSpec::new(requirement(warmup), 32, 0, MissingIntervalPolicy::Skip).unwrap()
}

fn analysis() -> AnalysisPipeline {
    AnalysisPipeline::new(
        vec![],
        ObservationStoreLimits::new(32, 32).unwrap(),
        AnnotationLimits::default(),
    )
    .unwrap()
}

fn config() -> BacktestConfig {
    BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: [(
            SYMBOL.into(),
            SymbolSpec {
                canonical: "eurusd".into(),
                pip_position: 4,
                digits: 5,
                category: "forex".into(),
                lot_base_units: 100_000,
                lot_step_units: 1_000,
                lot_min_steps: 1,
                lot_max_steps: 0,
            },
        )]
        .into_iter()
        .collect(),
        ..BacktestConfig::default()
    }
}

fn descriptor(id: &str) -> StrategyDescriptor {
    StrategyDescriptor::new(StrategyId::new(id).unwrap(), "r1", id).unwrap()
}

fn entry(timestamp: NaiveDateTime) -> RawSignal {
    RawSignal::Entry {
        ts: timestamp,
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(1.0),
        targets: vec![],
        group: Some("campaign".into()),
        trade_id: Some("trade-1".into()),
    }
}

fn journal(kind: JournalKind, reason: &str, trade_id: Option<&str>) -> StrategyJournalDraft {
    StrategyJournalDraft::new(
        kind,
        SYMBOL,
        trade_id.map(str::to_string),
        reason,
        Some(format!("chart://{reason}")),
        BTreeMap::from([("reference_price".into(), 1.101)]),
        StrategyResearchLimits::default(),
    )
    .unwrap()
}

struct JournalStrategy {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
    emit_journal: bool,
    seen_observation_counts: Vec<usize>,
}

impl JournalStrategy {
    fn new(id: &str, emit_journal: bool) -> Self {
        Self {
            descriptor: descriptor(id),
            requirements: requirements(1),
            emit_journal,
            seen_observation_counts: Vec::new(),
        }
    }
}

impl HistoricalStrategy for JournalStrategy {
    type Error = Infallible;

    fn descriptor(&self) -> &StrategyDescriptor {
        &self.descriptor
    }

    fn requirements(&self) -> &StrategyRequirements {
        &self.requirements
    }

    fn on_event(
        &mut self,
        event: StrategyEvent<'_>,
        context: StrategyContext<'_>,
    ) -> Result<StrategyOutput, Self::Error> {
        self.seen_observation_counts
            .push(event.observations().len());
        let output = if context.observed_through() == ts(1) {
            StrategyOutput::from_decision(
                StrategyDecisionDraft::new(
                    StrategyDecisionKind::Entry,
                    "economic entry reason",
                    Some("trade-1".into()),
                    vec![entry(ts(1))],
                    StrategyRetentionLimits::default(),
                )
                .unwrap(),
            )
        } else {
            StrategyOutput::none()
        };
        if !self.emit_journal {
            return Ok(output);
        }
        let drafts = if context.observed_through() == ts(0) {
            vec![journal(JournalKind::NoAction, "warming up", None)]
        } else if context.observed_through() == ts(1) {
            vec![
                journal(
                    JournalKind::DecisionContext,
                    "selected context",
                    Some("trade-1"),
                ),
                journal(
                    JournalKind::Hypothetical,
                    "alternative exit",
                    Some("trade-1"),
                ),
            ]
        } else if context.observed_through() == ts(2) {
            vec![journal(
                JournalKind::OutcomeReview,
                "review outcome",
                Some("trade-1"),
            )]
        } else {
            Vec::new()
        };
        Ok(output.with_journal(drafts))
    }
}

fn run_strategy(
    strategy: &mut JournalStrategy,
    analysis: AnalysisPipeline,
    research_limits: StrategyResearchLimits,
) -> StrategyBacktestResult {
    let mut feed = qs_backtest::VecFeed::from_feed_events(vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
    ]);
    BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .with_strategy_research_limits(research_limits)
        .run_historical_strategy_future(
            &mut feed,
            strategy,
            vec![spec(1)],
            analysis,
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap()
}

#[test]
fn runtime_stamps_ordered_journals_and_retention_never_suppresses_signals() {
    let limits = StrategyResearchLimits::new(2, 4, 128, 128, 4, 32, 32).unwrap();
    let mut strategy = JournalStrategy::new("journal-retention", true);
    let result = run_strategy(&mut strategy, analysis(), limits);

    assert_eq!(result.research.journal.retention.retained, 2);
    assert_eq!(result.research.journal.retention.omitted, 2);
    assert_eq!(result.research.journal.records[0].sequence(), 0);
    assert_eq!(result.research.journal.records[0].observed_through(), ts(0));
    assert_eq!(result.research.journal.records[1].sequence(), 1);
    assert_eq!(result.research.journal.records[1].observed_through(), ts(1));
    assert_eq!(result.research.journal.records[0].reason(), "warming up");
    assert_eq!(
        result.research.journal.records[1].reason(),
        "selected context"
    );
    assert_eq!(result.decisions.records.len(), 1);
    assert_eq!(
        result.decisions.records[0].reason(),
        "economic entry reason"
    );
    assert_eq!(result.replay.recorded_fills.len(), 1);
    assert_eq!(result.replay.recorded_fills[0].quote_ts, ts(2));
}

#[test]
fn journal_linkage_and_decision_retention_are_independent() {
    let mut strategy = JournalStrategy::new("independent-retention", true);
    let mut feed = qs_backtest::VecFeed::from_feed_events(vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
    ]);
    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::new(0, 8, 128).unwrap(),
            None,
        )
        .unwrap();

    assert!(result.decisions.records.is_empty());
    assert_eq!(result.decisions.retention.omitted, 1);
    assert_eq!(result.research.journal.records.len(), 4);
    assert_eq!(
        result.research.journal.records[1].kind(),
        JournalKind::DecisionContext
    );
    assert_eq!(
        result.research.journal.records[3].kind(),
        JournalKind::OutcomeReview
    );
    assert_eq!(
        result.research.journal.records[1].related_trade_id(),
        Some("trade-1")
    );
    assert_eq!(
        result.research.journal.records[3].related_trade_id(),
        Some("trade-1")
    );
    assert_eq!(result.replay.recorded_fills.len(), 1);
}

#[test]
fn callback_validation_is_atomic_and_all_bounds_are_enforced() {
    assert!(StrategyResearchLimits::new(0, 0, 1, 1, 0, 1, 1).is_err());
    assert!(StrategyResearchLimits::new(1_000_001, 1, 1, 1, 0, 1, 1).is_err());

    let default = StrategyResearchLimits::default();
    assert!(
        StrategyJournalDraft::new(
            JournalKind::NoAction,
            SYMBOL,
            None,
            " bad ",
            None,
            BTreeMap::new(),
            default,
        )
        .is_err()
    );
    assert!(
        StrategyJournalDraft::new(
            JournalKind::NoAction,
            SYMBOL,
            None,
            "ok",
            Some(" bad ".into()),
            BTreeMap::new(),
            default,
        )
        .is_err()
    );
    assert!(
        StrategyJournalDraft::new(
            JournalKind::NoAction,
            SYMBOL,
            Some(" bad ".into()),
            "ok",
            None,
            BTreeMap::new(),
            default,
        )
        .is_err()
    );
    assert!(
        StrategyJournalDraft::new(
            JournalKind::NoAction,
            SYMBOL,
            None,
            "ok",
            None,
            BTreeMap::from([("bad key".into(), 1.0)]),
            default,
        )
        .is_err()
    );
    assert!(matches!(
        StrategyJournalDraft::new(
            JournalKind::NoAction,
            SYMBOL,
            None,
            "ok",
            None,
            BTreeMap::from([("value".into(), f64::NAN)]),
            default,
        ),
        Err(StrategyJournalError::NonFiniteValue { .. })
    ));

    let narrow = StrategyResearchLimits::new(4, 2, 4, 4, 1, 4, 4).unwrap();
    let valid = StrategyJournalDraft::new(
        JournalKind::NoAction,
        SYMBOL,
        None,
        "ok",
        None,
        BTreeMap::new(),
        default,
    )
    .unwrap();
    let invalid = StrategyJournalDraft::new(
        JournalKind::NoAction,
        SYMBOL,
        None,
        "too long",
        None,
        BTreeMap::new(),
        default,
    )
    .unwrap();
    let mut recorder = StrategyJournalRecorder::new(narrow);
    assert!(recorder.push_callback(ts(0), vec![valid, invalid]).is_err());
    assert!(recorder.finish().records.is_empty());

    let too_many_values = StrategyJournalDraft::new(
        JournalKind::NoAction,
        SYMBOL,
        None,
        "ok",
        None,
        BTreeMap::from([("one".into(), 1.0), ("two".into(), 2.0)]),
        narrow,
    );
    assert!(too_many_values.is_err());
    let too_many_drafts = vec![
        journal(JournalKind::NoAction, "one", None),
        journal(JournalKind::NoAction, "two", None),
        journal(JournalKind::NoAction, "three", None),
    ];
    let mut recorder = StrategyJournalRecorder::new(narrow);
    assert!(matches!(
        recorder.push_callback(ts(0), too_many_drafts),
        Err(StrategyJournalError::TooManyDrafts { .. })
    ));
}

#[test]
fn hypothetical_journal_records_do_not_change_economic_replay() {
    let mut with_journal = JournalStrategy::new("with-journal", true);
    let with_journal = run_strategy(
        &mut with_journal,
        analysis(),
        StrategyResearchLimits::default(),
    );
    let mut without_journal = JournalStrategy::new("without-journal", false);
    let without_journal = run_strategy(
        &mut without_journal,
        analysis(),
        StrategyResearchLimits::default(),
    );

    assert_eq!(
        with_journal.replay.recorded_fills,
        without_journal.replay.recorded_fills
    );
    assert_eq!(
        with_journal.replay.close_events,
        without_journal.replay.close_events
    );
    assert_eq!(
        with_journal.replay.total_pnl,
        without_journal.replay.total_pnl
    );
    assert_eq!(
        with_journal.replay.total_positions,
        without_journal.replay.total_positions
    );
    assert_eq!(
        with_journal.replay.mtm_equity_curve,
        without_journal.replay.mtm_equity_curve
    );
}

fn annotation(
    id: &str,
    input_sequence: u64,
    use_kind: AnnotationUse,
    valid_from: Option<NaiveDateTime>,
) -> StrategyAnnotation {
    StrategyAnnotation::new(
        AnnotationId::new(id).unwrap(),
        input_sequence,
        ts(0),
        ts(0),
        valid_from,
        use_kind,
        SYMBOL,
        vec![SeriesId::new("m1").unwrap()],
        StrategyObservationValue::Momentum(qs_backtest::MomentumState::Stalling),
        Some(id.into()),
        AnnotationLimits::default(),
    )
    .unwrap()
}

#[test]
fn research_annotations_are_returned_in_input_order_and_never_enter_context() {
    let mut analysis = analysis();
    analysis
        .add_annotation(annotation(
            "hindsight",
            4,
            AnnotationUse::HindsightLabel,
            None,
        ))
        .unwrap();
    analysis
        .add_annotation(annotation("journal", 2, AnnotationUse::JournalOnly, None))
        .unwrap();
    analysis
        .add_annotation(annotation(
            "causal",
            1,
            AnnotationUse::CausalDecisionInput,
            Some(ts(1)),
        ))
        .unwrap();
    let mut strategy = JournalStrategy::new("annotations", false);
    let result = run_strategy(&mut strategy, analysis, StrategyResearchLimits::default());

    assert_eq!(strategy.seen_observation_counts, vec![0, 1, 0]);
    let ids = result
        .research
        .research_annotations
        .iter()
        .map(|annotation| annotation.annotation_id().as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["hindsight", "journal"]);
    assert!(
        result
            .research
            .research_annotations
            .iter()
            .all(|annotation| annotation.use_kind() != AnnotationUse::CausalDecisionInput)
    );
}

fn strategy_result(id: &str, replay: BacktestResult) -> StrategyBacktestResult {
    StrategyBacktestResult {
        replay,
        descriptor: descriptor(id),
        decisions: StrategyDecisionRecorder::new(StrategyRetentionLimits::default()).finish(),
        research: StrategyResearchOutput::default(),
    }
}

fn partial_close_result() -> BacktestResult {
    let position_id = PositionId::from("position-1");
    BacktestResult::from_trade_log(
        10_000.0,
        vec![
            TradeResult {
                position_id: position_id.clone(),
                symbol: SYMBOL.into(),
                side: Side::Buy,
                entry_price: 1.0,
                exit_price: 1.1,
                size: 0.5,
                pnl: 50.0,
                open_ts: ts(0),
                close_ts: ts(1),
                close_reason: CloseReason::Target,
                group: None,
            },
            TradeResult {
                position_id,
                symbol: SYMBOL.into(),
                side: Side::Buy,
                entry_price: 1.0,
                exit_price: 1.2,
                size: 0.5,
                pnl: 100.0,
                open_ts: ts(0),
                close_ts: ts(2),
                close_reason: CloseReason::Target,
                group: None,
            },
        ],
    )
}

#[test]
fn comparison_uses_position_metrics_and_preserves_caller_order() {
    let baseline = strategy_result("baseline-strategy", partial_close_result());
    let candidate = strategy_result(
        "candidate-strategy",
        BacktestResult::from_trade_log(10_000.0, vec![]),
    );
    assert_eq!(baseline.replay.total_trades, 2);
    assert_eq!(baseline.replay.total_positions, 1);

    let comparison = StrategyExperimentComparison::new(
        "baseline",
        &baseline,
        "candidate",
        &candidate,
        StrategyResearchLimits::default(),
    )
    .unwrap();
    assert_eq!(comparison.baseline.label, "baseline");
    assert_eq!(comparison.baseline.metrics.total_positions, 1);
    assert_eq!(comparison.baseline.metrics.position_win_rate, 1.0);
    assert_eq!(
        comparison.baseline.metrics.average_position_duration_secs,
        Some(90)
    );
    assert_eq!(comparison.candidate.label, "candidate");
    assert_eq!(comparison.candidate.metrics.total_positions, 0);

    assert!(
        StrategyExperimentComparison::new(
            "label too long",
            &baseline,
            "candidate",
            &candidate,
            StrategyResearchLimits::new(1, 1, 8, 8, 1, 8, 8).unwrap(),
        )
        .is_err()
    );
}

struct OversizedResearchOutput;

impl<'de> Deserializer<'de> for OversizedResearchOutput {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(ResearchOutputMap { next_field: 0 })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
        ignored_any
    }
}

struct ResearchOutputMap {
    next_field: u8,
}

impl<'de> MapAccess<'de> for ResearchOutputMap {
    type Error = de::value::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let key = match self.next_field {
            0 => "journal",
            1 => "research_annotations",
            _ => return Ok(None),
        };
        self.next_field += 1;
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.next_field - 1 {
            0 => {
                let mut deserializer = serde_json::Deserializer::from_str(
                    r#"{"records":[],"retention":{"retained":0,"omitted":0}}"#,
                );
                seed.deserialize(&mut deserializer)
                    .map_err(de::Error::custom)
            }
            1 => seed.deserialize(OversizedAnnotations),
            _ => unreachable!("research output map yields only known fields"),
        }
    }
}

struct OversizedAnnotations;

impl<'de> Deserializer<'de> for OversizedAnnotations {
    type Error = de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(OversizedAnnotationSequence)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(OversizedAnnotationSequence)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct tuple tuple_struct map struct enum identifier
        ignored_any
    }
}

struct OversizedAnnotationSequence;

impl<'de> SeqAccess<'de> for OversizedAnnotationSequence {
    type Error = de::value::Error;

    fn next_element_seed<T>(&mut self, _seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        Ok(None)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(MAX_ANNOTATIONS + 1)
    }
}

#[test]
fn research_values_round_trip_strictly_and_reject_unknown_fields() {
    let draft = journal(JournalKind::OutcomeReview, "review", Some("trade-1"));
    let draft_json = serde_json::to_string(&draft).unwrap();
    let decoded: StrategyJournalDraft = serde_json::from_str(&draft_json).unwrap();
    assert_eq!(decoded, draft);
    assert!(
        serde_json::from_str::<StrategyJournalDraft>(
            r#"{"kind":"no_action","symbol":"EURUSD","related_trade_id":null,"reason":"ok","chart_ref":null,"values":{},"unknown":true}"#,
        )
        .is_err()
    );

    let record_json = r#"{"sequence":0,"observed_through":"2026-02-02T12:00:00","kind":"outcome_review","symbol":"EURUSD","related_trade_id":"trade-1","reason":"review","chart_ref":"chart://review","values":{"reference_price":1.101}}"#;
    let record: StrategyJournalRecord = serde_json::from_str(record_json).unwrap();
    assert_eq!(serde_json::to_string(&record).unwrap(), record_json);

    let research = StrategyResearchOutput {
        journal: StrategyJournalRecorder::new(StrategyResearchLimits::default()).finish(),
        research_annotations: vec![annotation("label", 1, AnnotationUse::HindsightLabel, None)],
    };
    let research_json = serde_json::to_string(&research).unwrap();
    let decoded: StrategyResearchOutput = serde_json::from_str(&research_json).unwrap();
    assert_eq!(decoded, research);
    assert!(
        serde_json::from_str::<qs_backtest::StrategyJournalOutput>(
            r#"{"records":[],"retention":{"retained":1,"omitted":0}}"#,
        )
        .is_err()
    );
    let invalid_research = StrategyResearchOutput {
        journal: StrategyJournalRecorder::new(StrategyResearchLimits::default()).finish(),
        research_annotations: vec![annotation(
            "causal-output",
            2,
            AnnotationUse::CausalDecisionInput,
            Some(ts(1)),
        )],
    };
    assert!(
        serde_json::from_str::<StrategyResearchOutput>(
            &serde_json::to_string(&invalid_research).unwrap()
        )
        .is_err()
    );

    let baseline = strategy_result("serde-baseline", partial_close_result());
    let candidate = strategy_result(
        "serde-candidate",
        BacktestResult::from_trade_log(10_000.0, vec![]),
    );
    let comparison = StrategyExperimentComparison::new(
        "baseline",
        &baseline,
        "candidate",
        &candidate,
        StrategyResearchLimits::default(),
    )
    .unwrap();
    let comparison_json = serde_json::to_string(&comparison).unwrap();
    let decoded: StrategyExperimentComparison = serde_json::from_str(&comparison_json).unwrap();
    assert_eq!(decoded, comparison);
}

#[test]
fn journal_values_reject_duplicate_wire_keys() {
    let duplicate_draft = r#"{"kind":"no_action","symbol":"EURUSD","related_trade_id":null,"reason":"ok","chart_ref":null,"values":{"price":1.0,"price":2.0}}"#;
    let error = serde_json::from_str::<StrategyJournalDraft>(duplicate_draft).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("journal value key 'price' is duplicated")
    );

    let duplicate_record = r#"{"sequence":0,"observed_through":"2026-02-02T12:00:00","kind":"no_action","symbol":"EURUSD","related_trade_id":null,"reason":"ok","chart_ref":null,"values":{"price":1.0,"price":2.0}}"#;
    let error = serde_json::from_str::<StrategyJournalRecord>(duplicate_record).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("journal value key 'price' is duplicated")
    );
}

#[test]
fn research_output_revalidates_annotation_collection_invariants() {
    let empty_journal = StrategyJournalRecorder::new(StrategyResearchLimits::default()).finish();
    let duplicate_id = StrategyResearchOutput {
        journal: empty_journal.clone(),
        research_annotations: vec![
            annotation("duplicate", 1, AnnotationUse::HindsightLabel, None),
            annotation("duplicate", 2, AnnotationUse::JournalOnly, None),
        ],
    };
    let error = serde_json::from_str::<StrategyResearchOutput>(
        &serde_json::to_string(&duplicate_id).unwrap(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("annotation ID 'duplicate' is already present")
    );

    let duplicate_sequence = StrategyResearchOutput {
        journal: empty_journal,
        research_annotations: vec![
            annotation("first", 7, AnnotationUse::HindsightLabel, None),
            annotation("second", 7, AnnotationUse::JournalOnly, None),
        ],
    };
    let error = serde_json::from_str::<StrategyResearchOutput>(
        &serde_json::to_string(&duplicate_sequence).unwrap(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("annotation input sequence 7 is already present")
    );

    let error = StrategyResearchOutput::deserialize(OversizedResearchOutput).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("annotation count 1000001 exceeds maximum 1000000")
    );
}

struct BatchFeed {
    batches: VecDeque<TimestampBatch>,
}

impl FallibleBatchFeed for BatchFeed {
    type Error = Infallible;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Ok(self.batches.pop_front())
    }
}

#[test]
fn materialized_and_streaming_replay_return_equal_research_output() {
    let events = vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
    ];
    let mut materialized_strategy = JournalStrategy::new("materialized", true);
    let mut materialized_feed = qs_backtest::VecFeed::from_feed_events(events.clone());
    let materialized = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut materialized_feed,
            &mut materialized_strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    let mut streaming_strategy = JournalStrategy::new("streaming", true);
    let mut streaming_feed = BatchFeed {
        batches: events
            .into_iter()
            .map(|event| TimestampBatch {
                ts: event.event.ts(),
                events: vec![event],
            })
            .collect(),
    };
    let streaming = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_historical_strategy_future_streaming(
            &mut streaming_feed,
            Some(ts(2)),
            &mut streaming_strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(materialized.research, streaming.research);
    assert_eq!(
        materialized.replay.recorded_fills,
        streaming.replay.recorded_fills
    );
}
