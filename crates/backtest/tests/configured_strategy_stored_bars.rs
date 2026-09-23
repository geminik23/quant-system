//! A configured strategy driven by stored bars must see the same completed bars, and therefore make the same decisions, as the same strategy driven by the ticks those bars summarize.

mod support;

use qs_backtest::data_feed::{EventMetadata, FeedEvent, SeriesRoles};
use qs_backtest::{
    BacktestRunner, FutureQuoteConfig, MarketEvent, StrategyBacktestResult,
    StrategyRetentionLimits, VecFeed,
};
use support::configured::{analysis, crossover_adapter, crossover_events, runner_config};

const SPREAD: f64 = 0.0002;

/// One stored one-minute bar per tick, stamped at its bucket open, priced on the bid basis the crossover series declares.
fn stored_bar_feed() -> VecFeed {
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
                    tick_count: Some(1),
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

#[test]
fn stored_bar_fills_use_bar_close_quotes_rather_than_intrabar_prices() {
    let bars = run(stored_bar_feed());
    let entry = bars
        .replay
        .recorded_fills
        .first()
        .expect("the crossover enters");
    let bar_close = entry.bid + SPREAD / 2.0;
    assert!((entry.fill.price - (bar_close + SPREAD / 2.0)).abs() < 1e-12);
    assert!(
        crossover_events()
            .iter()
            .any(|event| event.ts() == entry.execution_ts.unwrap()),
        "a bar fill happens at a stored bar timestamp"
    );
}
