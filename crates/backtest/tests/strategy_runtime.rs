use std::convert::Infallible;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch};
use qs_backtest::ledger::{ActionDisposition, ActionDispositionStatus};
use qs_backtest::{
    AnalysisBoundary, AnalysisContext, AnalysisError, AnalysisPipeline, AnnotationLimits,
    BarSeriesSpec, HistoricalAnalyzer, HistoricalStrategy, MissingIntervalPolicy, MomentumState,
    MultiTimeframeSeries, ObservationStoreLimits, PriceBasis, RawSignal, SeriesId,
    SeriesRequirement, StrategyContext, StrategyDecisionDraft, StrategyDecisionKind,
    StrategyDecisionRecorder, StrategyDescriptor, StrategyDomainError, StrategyEvent,
    StrategyFeedback, StrategyId, StrategyObservationDraft, StrategyObservationValue,
    StrategyOutput, StrategyRequirements, StrategyRetentionLimits, StrategyRuntimeError, Timeframe,
    WarmupRequirement,
};
use qs_core::TradeEngine;
use qs_core::types::{Effect, FutureEffect};

fn ts(minute: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(3, 0, 0)
        .unwrap()
        + Duration::minutes(minute)
}

fn requirement() -> SeriesRequirement {
    SeriesRequirement::new(
        SeriesId::new("m1").unwrap(),
        "EURUSD",
        Timeframe::minutes(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(1).unwrap(),
    )
    .unwrap()
}

fn requirements() -> StrategyRequirements {
    StrategyRequirements::new(
        vec!["EURUSD".to_string()],
        vec![requirement()],
        0,
        true,
        true,
    )
    .unwrap()
}

fn feed_event(timestamp: NaiveDateTime, row_sequence: u64, bid: f64) -> FeedEvent {
    FeedEvent::new(
        MarketEvent::Tick {
            symbol: "EURUSD".to_string(),
            ts: timestamp,
            bid,
            ask: bid + 0.0002,
        },
        EventMetadata::new(SeriesRoles::PRIMARY, 0, row_sequence),
    )
}

struct MomentumAnalyzer(MomentumState);

impl HistoricalAnalyzer for MomentumAnalyzer {
    fn on_bar(
        &mut self,
        bar: &qs_backtest::ClosedBar,
        _context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        Ok(vec![StrategyObservationDraft::new(
            bar.symbol(),
            vec![bar.series_id().clone()],
            StrategyObservationValue::Momentum(self.0),
        )?])
    }
}

fn historical_state() -> (
    MultiTimeframeSeries,
    AnalysisPipeline,
    Vec<qs_backtest::ClosedBar>,
    Vec<qs_backtest::StrategyObservation>,
) {
    let spec = BarSeriesSpec::new(requirement(), 8, 0, MissingIntervalPolicy::Skip).unwrap();
    let mut series = MultiTimeframeSeries::new(vec![spec]).unwrap();
    let first = feed_event(ts(0), 0, 1.1000);
    let second = feed_event(ts(1), 1, 1.1005);
    assert!(
        series
            .on_batch(&TimestampBatch {
                ts: ts(0),
                events: vec![first],
            })
            .unwrap()
            .is_empty()
    );
    let bars = series
        .on_batch(&TimestampBatch {
            ts: ts(1),
            events: vec![second],
        })
        .unwrap();

    let mut analysis = AnalysisPipeline::new(
        vec![
            Box::new(MomentumAnalyzer(MomentumState::Advancing)),
            Box::new(MomentumAnalyzer(MomentumState::Stalling)),
        ],
        ObservationStoreLimits::new(8, 8).unwrap(),
        AnnotationLimits::default(),
    )
    .unwrap();
    let observations = analysis
        .on_boundary(AnalysisBoundary::new(ts(1), &bars, &series))
        .unwrap()
        .observations()
        .to_vec();
    (series, analysis, bars, observations)
}

struct StatefulCounter {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
    callbacks: usize,
    custom_sum: f64,
    seen: Vec<String>,
}

impl StatefulCounter {
    fn new() -> Self {
        Self {
            descriptor: StrategyDescriptor::new(
                StrategyId::new("stateful-counter").unwrap(),
                "r1",
                "Stateful counter",
            )
            .unwrap(),
            requirements: requirements(),
            callbacks: 0,
            custom_sum: 0.0,
            seen: Vec::new(),
        }
    }
}

impl HistoricalStrategy for StatefulCounter {
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
        self.callbacks += 1;
        self.seen.extend(
            event
                .primary_events()
                .iter()
                .map(|event| format!("event:{}", event.metadata.row_sequence)),
        );
        self.seen.extend(
            event
                .closed_bars()
                .iter()
                .map(|bar| format!("bar:{:.4}", bar.close())),
        );
        self.seen.extend(
            event
                .observations()
                .iter()
                .map(|observation| format!("observation:{}", observation.sequence())),
        );
        self.seen.extend(
            event
                .feedback()
                .effects()
                .iter()
                .map(|effect| match effect {
                    FutureEffect::Plain {
                        effect: Effect::OrderPlaced { id },
                        ..
                    } => format!("effect:{id}"),
                    _ => "effect:other".to_string(),
                }),
        );
        self.seen.extend(
            event
                .feedback()
                .dispositions()
                .iter()
                .map(|disposition| format!("disposition:{}", disposition.action_id)),
        );
        self.custom_sum += context
            .series()
            .latest_bar(&SeriesId::new("m1").unwrap())
            .unwrap()
            .unwrap()
            .close();
        assert!(context.warmup_complete());
        Ok(StrategyOutput::none())
    }
}

#[test]
fn complete_boundaries_preserve_order_and_strategy_owned_state() {
    let (series, analysis, bars, observations) = historical_state();
    let engine = TradeEngine::new();
    let events = vec![feed_event(ts(1), 7, 1.2), feed_event(ts(1), 3, 1.1)];
    let effects = vec![
        FutureEffect::plain(Effect::OrderPlaced { id: "p2".into() }),
        FutureEffect::plain(Effect::OrderPlaced { id: "p1".into() }),
    ];
    let dispositions = vec![
        ActionDisposition::new("a2", ActionDispositionStatus::Rejected),
        ActionDisposition::new("a1", ActionDispositionStatus::Applied),
    ];
    let feedback = StrategyFeedback::new(&effects, &dispositions);
    let event = StrategyEvent::new(&events, &bars, &observations, feedback);
    let context = StrategyContext::new(ts(1), &series, analysis.observations(), &engine, true);

    assert_eq!(context.observed_through(), ts(1));
    assert!(std::ptr::eq(context.engine(), &engine));
    assert_eq!(
        context
            .series()
            .bars(&SeriesId::new("m1").unwrap(), 8)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(context.observations().observations(8).len(), 2);
    assert!(!event.feedback().is_empty());

    let mut strategy = StatefulCounter::new();
    assert_eq!(strategy.descriptor().id().as_str(), "stateful-counter");
    assert!(strategy.requirements().needs_execution_feedback());
    assert!(
        strategy
            .on_event(event, context)
            .unwrap()
            .decision()
            .is_none()
    );
    let next_event = StrategyEvent::new(&[], &[], &[], StrategyFeedback::default());
    let next_context = StrategyContext::new(ts(2), &series, analysis.observations(), &engine, true);
    assert!(
        strategy
            .on_event(next_event, next_context)
            .unwrap()
            .decision()
            .is_none()
    );

    assert_eq!(strategy.callbacks, 2);
    assert!((strategy.custom_sum - 2.2).abs() < 1.0e-12);
    let once = vec![
        "event:7",
        "event:3",
        "bar:1.1000",
        "observation:0",
        "observation:1",
        "effect:p2",
        "effect:p1",
        "disposition:a2",
        "disposition:a1",
    ];
    assert_eq!(strategy.seen, once);
}

#[test]
fn decision_output_preserves_signals_and_runtime_sequence() {
    let limits = StrategyRetentionLimits::new(4, 4, 64).unwrap();
    let draft = StrategyDecisionDraft::new(
        StrategyDecisionKind::Management,
        "close and cancel",
        Some("trade-1".to_string()),
        vec![
            RawSignal::CloseAll { ts: ts(2) },
            RawSignal::CancelAllPending { ts: ts(2) },
        ],
        limits,
    )
    .unwrap();
    assert_eq!(draft.kind(), StrategyDecisionKind::Management);
    assert_eq!(draft.reason(), "close and cancel");
    assert_eq!(draft.related_trade_id(), Some("trade-1"));
    assert_eq!(draft.signals().len(), 2);

    let output = StrategyOutput::from_decision(draft);
    let record = output
        .into_decision()
        .unwrap()
        .into_record(9, ts(2), limits)
        .unwrap();
    assert_eq!(record.sequence(), 9);
    assert_eq!(record.observed_through(), ts(2));
    assert!(matches!(
        record.emitted_signals()[0],
        RawSignal::CloseAll { .. }
    ));
    assert!(matches!(
        record.emitted_signals()[1],
        RawSignal::CancelAllPending { .. }
    ));

    let mut recorder = StrategyDecisionRecorder::new(limits);
    recorder.push(record).unwrap();
    let repeated = StrategyDecisionDraft::new(
        StrategyDecisionKind::Exit,
        "repeat",
        None,
        vec![RawSignal::CloseAll { ts: ts(3) }],
        limits,
    )
    .unwrap()
    .into_record(9, ts(3), limits)
    .unwrap();
    assert!(matches!(
        recorder.push(repeated),
        Err(StrategyDomainError::NonMonotonicDecisionSequence { .. })
    ));
}

#[test]
fn decision_drafts_reject_bounds_and_timestamp_mismatch() {
    let limits = StrategyRetentionLimits::new(2, 1, 8).unwrap();
    assert!(matches!(
        StrategyDecisionDraft::new(
            StrategyDecisionKind::Entry,
            "enter",
            None,
            vec![
                RawSignal::CloseAll { ts: ts(1) },
                RawSignal::CloseAll { ts: ts(1) }
            ],
            limits,
        ),
        Err(StrategyDomainError::TooManySignals { .. })
    ));
    assert!(matches!(
        StrategyDecisionDraft::new(
            StrategyDecisionKind::Entry,
            "reason is too long",
            None,
            vec![],
            limits,
        ),
        Err(StrategyDomainError::InvalidDecisionReason { .. })
    ));
    assert!(matches!(
        StrategyDecisionDraft::new(
            StrategyDecisionKind::Entry,
            "enter",
            Some(" bad ".to_string()),
            vec![],
            limits,
        ),
        Err(StrategyDomainError::InvalidTradeId)
    ));

    let mismatch = StrategyDecisionDraft::new(
        StrategyDecisionKind::Exit,
        "close",
        None,
        vec![RawSignal::CloseAll { ts: ts(1) }],
        limits,
    )
    .unwrap()
    .into_record(1, ts(2), limits);
    assert!(matches!(
        mismatch,
        Err(StrategyRuntimeError::SignalTimestampMismatch {
            signal_index: 0,
            ..
        })
    ));
}
