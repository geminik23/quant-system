//! A configured strategy driven by stored bars must see the same completed bars, and therefore make the same decisions, as the same strategy driven by the ticks those bars summarize.

mod support;

use qs_backtest::data_feed::{EventMetadata, FeedEvent, SeriesRoles};
use qs_backtest::{
    BacktestRunner, FutureQuoteConfig, MarketEvent, StrategyBacktestResult,
    StrategyRetentionLimits, VecFeed,
};
use qs_strategy::{
    BarField, Expr, Literal, MaterialLibrary, SourceId, StateConfig, StrategyConfig,
    TransitionConfig,
};
use support::configured::{
    analysis, crossover_adapter, crossover_adapter_with_volume, crossover_events,
    historical_adapter_with_volume, runner_config,
};

const SPREAD: f64 = 0.0002;

/// One stored one-minute bar per tick, stamped at its bucket open, priced on the bid basis the crossover series declares.
fn stored_bar_feed() -> VecFeed {
    stored_bar_feed_with_count(Some(1))
}

fn stored_bar_feed_with_count(tick_count: Option<u64>) -> VecFeed {
    let events = crossover_events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let MarketEvent::Tick {
                symbol, ts, bid, ..
            } = event
            else {
                unreachable!("the crossover fixture is tick-only");
            };
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol,
                    ts,
                    open: bid,
                    high: bid,
                    low: bid,
                    close: bid,
                    volume: 0,
                    spread: Some(SPREAD),
                    timeframe_seconds: Some(60),
                    tick_count,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )
        })
        .collect();
    VecFeed::from_feed_events(events)
}

fn run(mut feed: VecFeed) -> StrategyBacktestResult {
    let mut adapter = crossover_adapter();
    BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed,
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap()
}

fn decisions(result: &StrategyBacktestResult) -> Vec<serde_json::Value> {
    result
        .decisions
        .records
        .iter()
        .map(|record| {
            serde_json::json!({
                "observed_through": record.observed_through(),
                "kind": format!("{:?}", record.kind()),
                "reason": record.reason(),
                "signals": record.emitted_signals(),
            })
        })
        .collect()
}

#[test]
fn stored_bar_run_makes_the_same_decisions_as_the_tick_run() {
    let ticks = run(VecFeed::new(crossover_events()));
    let bars = run(stored_bar_feed());

    let expected = decisions(&ticks);
    assert!(
        expected.len() >= 2,
        "the fixture must enter and exit to be meaningful"
    );
    assert_eq!(decisions(&bars), expected);
}

/// The same stored bars with an open below the close, so an open fill and a close fill differ while the closes the strategy reads are unchanged.
fn stored_bar_feed_with_distinct_opens() -> VecFeed {
    let events = crossover_events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let MarketEvent::Tick {
                symbol, ts, bid, ..
            } = event
            else {
                unreachable!("the crossover fixture is tick-only");
            };
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol,
                    ts,
                    open: bid - OPEN_OFFSET,
                    high: bid,
                    low: bid - OPEN_OFFSET,
                    close: bid,
                    volume: 0,
                    spread: Some(SPREAD),
                    timeframe_seconds: Some(60),
                    tick_count: Some(1),
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )
        })
        .collect();
    VecFeed::from_feed_events(events)
}

const OPEN_OFFSET: f64 = 0.00005;

#[test]
fn price_only_bars_run_without_inventing_counts_and_count_consumers_fail_preflight() {
    let mut optional =
        crossover_adapter_with_volume(qs_backtest::HistoricalVolumeProjection::OptionalTickCount);
    let mut feed = stored_bar_feed_with_count(None);
    let price_only = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed,
            &mut optional,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    assert!(price_only.decisions.records.len() >= 2);

    let source = SourceId::new("primary_bars").unwrap();
    let count_config = StrategyConfig {
        strategy_id: "count_required".into(),
        title: "Count-required fixture".into(),
        parameters: vec![],
        initial_state: "idle".into(),
        sources: vec![source.clone()],
        trade_slots: vec![],
        materials: vec![],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::Gt {
                        left: Box::new(Expr::Bar {
                            source,
                            field: BarField::Volume,
                        }),
                        right: Box::new(Expr::Literal {
                            value: Literal::Number(0.0),
                        }),
                    },
                    assignments: vec![],
                    decision: None,
                    actions: vec![],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "done".into(),
                transitions: vec![],
            },
        ],
    };
    let mut count_required = historical_adapter_with_volume(
        count_config,
        &MaterialLibrary::builtins(),
        "count",
        1,
        0,
        qs_backtest::HistoricalVolumeProjection::OptionalTickCount,
    );
    assert_eq!(
        count_required
            .configured_requirements()
            .count_required_sources
            .len(),
        1
    );
    let mut unknown = stored_bar_feed_with_count(None);
    let error = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut unknown,
            &mut count_required,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("requires tick count"));
}

#[test]
fn a_stored_bar_decision_fills_at_the_open_of_the_bar_that_revealed_it() {
    let bars = run(stored_bar_feed_with_distinct_opens());
    let decision = bars
        .decisions
        .records
        .iter()
        .find(|record| !record.emitted_signals().is_empty())
        .expect("the crossover decides to enter");
    let entry = bars
        .replay
        .recorded_fills
        .first()
        .expect("the crossover enters");
    let decided_at = decision.observed_through();
    assert_eq!(entry.execution_ts, Some(decided_at));

    let tick_bid = crossover_events()
        .into_iter()
        .find_map(|event| match event {
            MarketEvent::Tick { ts, bid, .. } if ts == decided_at => Some(bid),
            _ => None,
        })
        .expect("a decision happens at a stored bar timestamp");
    let open = tick_bid - OPEN_OFFSET;
    let expected = match entry.fill.side {
        qs_core::types::Side::Buy => open + SPREAD / 2.0,
        qs_core::types::Side::Sell => open - SPREAD / 2.0,
    };
    assert!((entry.fill.price - expected).abs() < 1e-12);
}
