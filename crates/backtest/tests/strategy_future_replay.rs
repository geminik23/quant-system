use std::convert::Infallible;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{
    EventMetadata, FallibleBatchFeed, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch,
};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestRunner, BarSeriesSpec, FutureQuoteConfig,
    HistoricalStrategy, MissingIntervalPolicy, ObservationStoreLimits, PositionRef, PriceBasis,
    RawSignal, SeriesId, SeriesRequirement, StrategyContext, StrategyDecisionDraft,
    StrategyDecisionKind, StrategyDescriptor, StrategyEvent, StrategyId, StrategyOutput,
    StrategyReplayError, StrategyRequirements, StrategyRetentionLimits, Timeframe, VecFeed,
    WarmupRequirement,
};
use qs_core::types::{Effect, FutureEffect, OrderType, Side};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "EURUSD";

fn ts(minute: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
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

fn requirements(warmup: usize, latency_ms: u64) -> StrategyRequirements {
    StrategyRequirements::new(
        vec![SYMBOL.into()],
        vec![requirement(warmup)],
        latency_ms,
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

fn config(close_on_finish: bool) -> BacktestConfig {
    BacktestConfig {
        close_on_finish,
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
        stoploss: Some(0.9),
        targets: vec![],
        group: Some("campaign".into()),
        trade_id: Some("trade-1".into()),
    }
}

struct RecordingStrategy {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
    callbacks: Vec<(NaiveDateTime, Vec<u64>, bool)>,
    emit_entry_at: Option<NaiveDateTime>,
    feedback: Vec<(NaiveDateTime, usize, usize)>,
}

impl RecordingStrategy {
    fn new(warmup: usize, emit_entry_at: Option<NaiveDateTime>) -> Self {
        Self {
            descriptor: descriptor("recording"),
            requirements: requirements(warmup, 0),
            callbacks: Vec::new(),
            emit_entry_at,
            feedback: Vec::new(),
        }
    }
}

impl HistoricalStrategy for RecordingStrategy {
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
        self.callbacks.push((
            context.observed_through(),
            event
                .primary_events()
                .iter()
                .map(|event| event.metadata.row_sequence)
                .collect(),
            context.warmup_complete(),
        ));
        self.feedback.push((
            context.observed_through(),
            event.feedback().effects().len(),
            event.feedback().dispositions().len(),
        ));
        if self.emit_entry_at == Some(context.observed_through()) {
            let draft = StrategyDecisionDraft::new(
                StrategyDecisionKind::Entry,
                "enter",
                Some("trade-1".into()),
                vec![entry(context.observed_through())],
                StrategyRetentionLimits::default(),
            )
            .unwrap();
            Ok(StrategyOutput::from_decision(draft))
        } else {
            Ok(StrategyOutput::none())
        }
    }
}

#[test]
fn complete_boundaries_preserve_order_and_dynamic_liveness() {
    let mut strategy = RecordingStrategy::new(1, None);
    let events = vec![
        FeedEvent::new(
            MarketEvent::Tick {
                symbol: "USDJPY".into(),
                ts: ts(0),
                bid: 150.0,
                ask: 150.01,
            },
            EventMetadata::new(SeriesRoles::CONVERSION, 0, 0),
        ),
        event(0, 2, 1.1002),
        event(0, 1, 1.1001),
        FeedEvent::new(
            MarketEvent::Tick {
                symbol: SYMBOL.into(),
                ts: ts(0),
                bid: f64::NAN,
                ask: 1.0,
            },
            EventMetadata::new(SeriesRoles::PRIMARY, 0, 3),
        ),
        event(1, 3, 1.1003),
        event(5, 4, 1.1004),
    ];
    let mut feed = VecFeed::from_feed_events(events);
    let result = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(strategy.callbacks.len(), 3);
    assert_eq!(strategy.callbacks[0].1, vec![1, 2]);
    assert_eq!(strategy.callbacks[2].0, ts(5));
    assert_eq!(result.decisions.retention.retained, 0);
    assert!(
        result
            .replay
            .equity_curve
            .iter()
            .all(|point| point.0 >= ts(1))
    );
    assert_eq!(
        result.replay.execution_metadata.as_ref().unwrap().tags["termination_reason"],
        "end_of_data"
    );
}

#[test]
fn generated_entry_uses_a_later_quote_and_feedback_is_delivered_once() {
    let mut strategy = RecordingStrategy::new(1, Some(ts(1)));
    let mut feed = VecFeed::from_feed_events(vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
        event(3, 3, 1.1030),
    ]);
    let retention = StrategyRetentionLimits::new(0, 8, 128).unwrap();
    let result = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            retention,
            None,
        )
        .unwrap();

    assert!(result.decisions.records.is_empty());
    assert_eq!(result.decisions.retention.omitted, 1);
    assert_eq!(result.replay.recorded_fills.len(), 1);
    assert_eq!(result.replay.recorded_fills[0].signal_ts, Some(ts(1)));
    assert_eq!(result.replay.recorded_fills[0].quote_ts, ts(2));
    assert_eq!(strategy.feedback[2], (ts(2), 1, 1));
    assert_eq!(strategy.feedback[3], (ts(3), 0, 0));
}

struct WarmupEmitter(RecordingStrategy);

impl HistoricalStrategy for WarmupEmitter {
    type Error = Infallible;

    fn descriptor(&self) -> &StrategyDescriptor {
        &self.0.descriptor
    }

    fn requirements(&self) -> &StrategyRequirements {
        &self.0.requirements
    }

    fn on_event(
        &mut self,
        _event: StrategyEvent<'_>,
        context: StrategyContext<'_>,
    ) -> Result<StrategyOutput, Self::Error> {
        let draft = StrategyDecisionDraft::new(
            StrategyDecisionKind::Entry,
            "too early",
            None,
            vec![entry(context.observed_through())],
            StrategyRetentionLimits::default(),
        )
        .unwrap();
        Ok(StrategyOutput::from_decision(draft))
    }
}

#[test]
fn warmup_signal_is_rejected_before_economic_mutation() {
    let mut strategy = WarmupEmitter(RecordingStrategy::new(2, None));
    let mut feed = VecFeed::from_feed_events(vec![event(0, 0, 1.1000), event(1, 1, 1.1010)]);
    let error = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(2)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        StrategyReplayError::WarmupSignals { timestamp } if timestamp == ts(0)
    ));
}

struct ManagementStrategy {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
    feedback: Vec<(NaiveDateTime, Vec<String>, Vec<String>)>,
}

impl HistoricalStrategy for ManagementStrategy {
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
        self.feedback.push((
            context.observed_through(),
            event
                .feedback()
                .effects()
                .iter()
                .map(|effect| match effect {
                    FutureEffect::Plain { effect, .. } => match effect {
                        Effect::StoplossModified { .. } => "stop".into(),
                        _ => "plain".into(),
                    },
                    FutureEffect::Filled { .. } => "fill".into(),
                })
                .collect(),
            event
                .feedback()
                .dispositions()
                .iter()
                .map(|disposition| disposition.action_id.clone())
                .collect(),
        ));
        let signals = if context.observed_through() == ts(1) {
            vec![entry(ts(1))]
        } else if context.observed_through() == ts(2) {
            vec![RawSignal::ModifyStoploss {
                ts: ts(2),
                position: PositionRef::ByTradeId {
                    trade_id: "trade-1".into(),
                },
                price: 1.1000,
            }]
        } else {
            return Ok(StrategyOutput::none());
        };
        Ok(StrategyOutput::from_decision(
            StrategyDecisionDraft::new(
                StrategyDecisionKind::Management,
                "manage",
                Some("trade-1".into()),
                signals,
                StrategyRetentionLimits::default(),
            )
            .unwrap(),
        ))
    }
}

#[test]
fn post_callback_non_fill_feedback_arrives_on_the_next_boundary() {
    let mut strategy = ManagementStrategy {
        descriptor: descriptor("management"),
        requirements: requirements(1, 0),
        feedback: Vec::new(),
    };
    let mut feed = VecFeed::from_feed_events(vec![
        event(0, 0, 1.0990),
        event(1, 1, 1.1000),
        event(2, 2, 1.1010),
        event(3, 3, 1.1020),
    ]);
    BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert!(strategy.feedback[2].1.contains(&"fill".to_string()));
    assert!(!strategy.feedback[2].1.contains(&"stop".to_string()));
    assert_eq!(strategy.feedback[3].1, vec!["stop"]);
    assert_eq!(strategy.feedback[3].2.len(), 1);
}

struct BatchFeed {
    batches: std::collections::VecDeque<TimestampBatch>,
}

impl FallibleBatchFeed for BatchFeed {
    type Error = Infallible;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Ok(self.batches.pop_front())
    }
}

#[test]
fn materialized_and_streaming_strategy_replay_match() {
    let events = vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
    ];
    let mut materialized_strategy = RecordingStrategy::new(1, Some(ts(1)));
    let mut materialized_feed = VecFeed::from_feed_events(events.clone());
    let materialized = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut materialized_feed,
            &mut materialized_strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    let mut streaming_strategy = RecordingStrategy::new(1, Some(ts(1)));
    let mut streaming_feed = BatchFeed {
        batches: events
            .into_iter()
            .map(|event| TimestampBatch {
                ts: event.event.ts(),
                events: vec![event],
            })
            .collect(),
    };
    let streaming = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
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

    assert_eq!(
        materialized.replay.recorded_fills,
        streaming.replay.recorded_fills
    );
    assert_eq!(
        materialized.replay.action_dispositions,
        streaming.replay.action_dispositions
    );
    assert_eq!(materialized.decisions.records.len(), 1);
    assert_eq!(streaming.decisions.records.len(), 1);
    assert_eq!(
        materialized.decisions.records[0].observed_through(),
        streaming.decisions.records[0].observed_through()
    );
    assert_eq!(
        materialized.decisions.records[0].emitted_signals().len(),
        streaming.decisions.records[0].emitted_signals().len()
    );
}

#[test]
fn generated_latency_ignores_static_signal_latency_and_matches_direct_economics() {
    let events = vec![
        event(0, 0, 1.1000),
        event(1, 1, 1.1010),
        event(2, 2, 1.1020),
    ];
    let mut strategy = RecordingStrategy::new(1, Some(ts(1)));
    let mut strategy_feed = VecFeed::from_feed_events(events.clone());
    let strategy_result = BacktestRunner::new_future(
        config(false),
        FutureQuoteConfig {
            signal_latency_ms: 600_000,
            ..FutureQuoteConfig::default()
        },
    )
    .run_historical_strategy_future(
        &mut strategy_feed,
        &mut strategy,
        vec![spec(1)],
        analysis(),
        StrategyRetentionLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(strategy_result.replay.recorded_fills[0].quote_ts, ts(2));

    let mut direct_signal = entry(ts(1) + Duration::milliseconds(1));
    if let RawSignal::Entry { trade_id, .. } = &mut direct_signal {
        *trade_id = Some("direct-trade".into());
    }
    let mut direct_feed = VecFeed::from_feed_events(events);
    let direct_result = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_raw_signals_future(&mut direct_feed, vec![direct_signal], None);
    assert_eq!(
        strategy_result.replay.recorded_fills[0].fill,
        direct_result.recorded_fills[0].fill
    );
    assert_eq!(strategy_result.replay.total_pnl, direct_result.total_pnl);
    assert_eq!(
        strategy_result.replay.open_position_snapshots[0].average_entry_price,
        direct_result.open_position_snapshots[0].average_entry_price
    );
}

#[test]
fn tick_requirement_rejects_primary_bars() {
    let mut strategy = RecordingStrategy::new(1, None);
    let bar = FeedEvent::new(
        MarketEvent::Bar {
            symbol: SYMBOL.into(),
            ts: ts(0),
            open: 1.0,
            high: 1.1,
            low: 0.9,
            close: 1.0,
            volume: 1,
        },
        EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
    );
    let mut feed = VecFeed::from_feed_events(vec![bar]);
    let error = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future(
            &mut feed,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StrategyReplayError::TickExecutionRequired { symbol, timestamp }
            if symbol == SYMBOL && timestamp == ts(0)
    ));
    assert!(strategy.callbacks.is_empty());
}

struct FailingFeed;

impl FallibleBatchFeed for FailingFeed {
    type Error = &'static str;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Err("source failed")
    }
}

#[test]
fn streaming_feed_errors_are_preserved() {
    let mut strategy = RecordingStrategy::new(1, None);
    let error = BacktestRunner::new_future(config(false), FutureQuoteConfig::default())
        .run_historical_strategy_future_streaming(
            &mut FailingFeed,
            None,
            &mut strategy,
            vec![spec(1)],
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();
    assert!(matches!(error, StrategyReplayError::Feed("source failed")));
    assert!(strategy.callbacks.is_empty());
}
