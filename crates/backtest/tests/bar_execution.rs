//! Bar-fed FutureQuote execution: a bar trades at its open, then through its range with each side meeting its adverse extreme first, and its close only marks what remains.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestResult, BacktestRunner, ConversionRoute, EntryGeometryPolicy, FutureQuoteConfig,
    ManagementProfile, MarketEvent, RawSignal, RuleConfigDef, RunCurrencyPlan, StoplossMode,
    VecFeed,
};
use qs_core::types::{CloseReason, OrderType, Side};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "EURUSD";
const SPREAD: f64 = 0.0002;
const HALF: f64 = SPREAD / 2.0;

fn ts(minutes: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 5)
        .unwrap()
        .and_hms_opt(10, 0, 0)
        .unwrap()
        + Duration::minutes(minutes)
}

fn bar(minute: i64, open: f64, high: f64, low: f64, close: f64) -> MarketEvent {
    MarketEvent::Bar {
        symbol: SYMBOL.into(),
        ts: ts(minute),
        open,
        high,
        low,
        close,
        volume: 0,
        spread: Some(SPREAD),
        timeframe_seconds: Some(60),
        tick_count: Some(10),
    }
}

fn tick(seconds: i64, mid: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: SYMBOL.into(),
        ts: ts(0) + Duration::seconds(seconds),
        bid: mid - HALF,
        ask: mid + HALF,
    }
}

fn entry(
    at: NaiveDateTime,
    side: Side,
    order_type: OrderType,
    price: Option<f64>,
    stoploss: Option<f64>,
    targets: Vec<f64>,
) -> RawSignal {
    RawSignal::Entry {
        ts: at,
        symbol: SYMBOL.into(),
        side,
        order_type,
        price,
        risk_multiplier: 1.0,
        stoploss,
        targets,
        group: None,
        trade_id: Some("t1".into()),
        entry_class: None,
    }
}

fn market(side: Side, stoploss: Option<f64>, targets: Vec<f64>) -> RawSignal {
    entry(
        ts(0) - Duration::seconds(30),
        side,
        OrderType::Market,
        None,
        stoploss,
        targets,
    )
}

fn config() -> BacktestConfig {
    BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: [(
            SYMBOL.to_owned(),
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
        .into(),
        ..BacktestConfig::default()
    }
}

fn future() -> FutureQuoteConfig {
    FutureQuoteConfig {
        currency_plan: Some(
            RunCurrencyPlan::new(
                "USD",
                BTreeSet::from([SYMBOL.to_owned()]),
                BTreeSet::new(),
                BTreeMap::from([(SYMBOL.to_owned(), "USD".to_owned())]),
                BTreeMap::from([(
                    "USD".to_owned(),
                    ConversionRoute::Identity {
                        currency: "USD".to_owned(),
                    },
                )]),
                vec![],
            )
            .unwrap(),
        ),
        ..FutureQuoteConfig::default()
    }
}

fn run(events: Vec<MarketEvent>, signals: Vec<RawSignal>) -> BacktestResult {
    BacktestRunner::new_future(config(), future()).run_raw_signals_future(
        &mut VecFeed::new(events),
        signals,
        None,
    )
}

fn run_profiled(
    events: Vec<MarketEvent>,
    signals: Vec<RawSignal>,
    profile: &ManagementProfile,
) -> BacktestResult {
    BacktestRunner::new_future(config(), future()).run_raw_signals_future(
        &mut VecFeed::new(events),
        signals,
        Some(profile),
    )
}

fn close_prices(result: &BacktestResult) -> Vec<(CloseReason, f64)> {
    result
        .close_events
        .iter()
        .map(|close| (close.reason, close.price))
        .collect()
}

fn assert_price(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "expected {expected}, got {actual}"
    );
}

#[test]
fn a_market_entry_waiting_for_a_bar_fills_at_its_open() {
    let result = run(
        vec![bar(0, 1.1000, 1.1010, 1.0990, 1.1005)],
        vec![market(Side::Buy, None, vec![])],
    );
    let fill = &result.recorded_fills[0];
    assert_price(fill.fill.price, 1.1000 + HALF);
    assert_eq!(fill.execution_ts, Some(ts(0)));
}

#[test]
fn a_long_stop_inside_the_range_fills_at_its_level_even_when_the_close_recovers() {
    let result = run(
        vec![bar(0, 1.1000, 1.1010, 1.0950, 1.1005)],
        vec![market(Side::Buy, Some(1.0970), vec![])],
    );
    assert_eq!(close_prices(&result).len(), 1);
    let (reason, price) = close_prices(&result)[0];
    assert_eq!(reason, CloseReason::Stoploss);
    assert_price(price, 1.0970);
    assert_eq!(result.close_events[0].ts, ts(0));
}

#[test]
fn a_bar_covering_the_stop_and_a_target_fills_only_the_stop() {
    let long = run(
        vec![bar(0, 1.1000, 1.1030, 1.0950, 1.1020)],
        vec![market(Side::Buy, Some(1.0970), vec![1.1025])],
    );
    assert_eq!(close_prices(&long).len(), 1);
    assert_eq!(close_prices(&long)[0].0, CloseReason::Stoploss);
    assert_price(close_prices(&long)[0].1, 1.0970);

    let short = run(
        vec![bar(0, 1.1000, 1.1050, 1.0970, 1.0980)],
        vec![market(Side::Sell, Some(1.1030), vec![1.0975])],
    );
    assert_eq!(close_prices(&short).len(), 1);
    assert_eq!(close_prices(&short)[0].0, CloseReason::Stoploss);
    assert_price(close_prices(&short)[0].1, 1.1030);
}

#[test]
fn targets_without_a_stop_in_range_fill_nearest_to_the_open_first() {
    let result = run(
        vec![bar(0, 1.1000, 1.1030, 1.0995, 1.1020)],
        vec![market(Side::Buy, Some(1.0900), vec![1.1025, 1.1010])],
    );
    let closes = close_prices(&result);
    assert_eq!(closes.len(), 2);
    assert!(
        closes
            .iter()
            .all(|(reason, _)| *reason == CloseReason::Target)
    );
    assert_price(closes[0].1, 1.1010);
    assert_price(closes[1].1, 1.1025);
}

#[test]
fn a_level_outside_the_range_does_not_fill() {
    let result = run(
        vec![
            bar(0, 1.1000, 1.1010, 1.0980, 1.1005),
            bar(1, 1.1005, 1.1015, 1.0985, 1.1010),
        ],
        vec![market(Side::Buy, Some(1.0970), vec![1.1050])],
    );
    assert!(result.close_events.is_empty());
    assert_eq!(result.open_position_snapshots.len(), 1);
}

#[test]
fn a_pending_limit_and_its_stop_can_both_fill_inside_one_bar() {
    let result = run(
        vec![bar(0, 1.1000, 1.1005, 1.0950, 1.0990)],
        vec![entry(
            ts(0) - Duration::seconds(30),
            Side::Buy,
            OrderType::Limit,
            Some(1.0980),
            Some(1.0960),
            vec![],
        )],
    );
    assert_price(result.recorded_fills[0].fill.price, 1.0980);
    let closes = close_prices(&result);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].0, CloseReason::Stoploss);
    assert_price(closes[0].1, 1.0960);
}

#[test]
fn a_gap_through_the_stop_at_the_open_fills_at_the_open() {
    let result = run(
        vec![
            bar(0, 1.1000, 1.1005, 1.0995, 1.1000),
            bar(1, 1.0950, 1.0960, 1.0940, 1.0955),
        ],
        vec![market(Side::Buy, Some(1.0970), vec![])],
    );
    let closes = close_prices(&result);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].0, CloseReason::Stoploss);
    assert_price(closes[0].1, 1.0950 - HALF);
    assert_eq!(result.close_events[0].ts, ts(1));
}

#[test]
fn a_stop_raised_by_trailing_on_the_favorable_leg_does_not_fill_in_the_same_bar() {
    let profile = ManagementProfile {
        name: "trail".into(),
        target_selection: None,
        use_targets: vec![],
        close_ratios: vec![],
        target_source: qs_backtest::TargetSource::FromSignal,
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![RuleConfigDef::TrailingStop { distance: 0.0010 }],
        group_override: None,
        let_remainder_run: false,
        entry_geometry: EntryGeometryPolicy::Strict,
    };
    // The long walk visits the low before the high, so the trailing stop rises to the high's bid minus 0.0010 after the low has passed, and the close below that stop does not trigger it because the close only marks.
    let result = run_profiled(
        vec![
            bar(0, 1.1000, 1.1030, 1.0995, 1.1015),
            bar(1, 1.1025, 1.1028, 1.1022, 1.1026),
        ],
        vec![market(Side::Buy, Some(1.0900), vec![])],
        &profile,
    );
    assert!(result.close_events.is_empty());

    // The next bar opens above the raised stop, reaches it inside its range, and fills it at its level.
    let result = run_profiled(
        vec![
            bar(0, 1.1000, 1.1030, 1.0995, 1.1015),
            bar(1, 1.1025, 1.1028, 1.1010, 1.1012),
        ],
        vec![market(Side::Buy, Some(1.0900), vec![])],
        &profile,
    );
    let closes = close_prices(&result);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].0, CloseReason::TrailingStop);
    assert_price(closes[0].1, 1.1030 - HALF - 0.0010);
}

#[test]
fn the_bar_close_marks_positions_but_does_not_fill() {
    let result = run(
        vec![bar(0, 1.1000, 1.1010, 1.0990, 1.1005)],
        vec![market(Side::Buy, Some(1.0980), vec![1.1050])],
    );
    assert!(result.close_events.is_empty());
    let snapshot = &result.open_position_snapshots[0];
    assert_price(
        snapshot.mark_price.expect("an open position is marked"),
        1.1005 - HALF,
    );
}

#[test]
fn a_monotone_tick_path_and_its_bar_fill_at_the_same_levels() {
    // Ticks inside one bucket that fall to the low before rising to the high, which is exactly the long walk.
    let ticks = vec![
        tick(0, 1.1000),
        tick(5, 1.0985),
        tick(10, 1.0971),
        tick(15, 1.0950),
        tick(30, 1.1000),
        tick(45, 1.1010),
        tick(55, 1.1005),
    ];
    let signals = || vec![market(Side::Buy, Some(1.0970 - HALF), vec![])];
    let from_ticks = run(ticks, signals());
    let from_bar = run(vec![bar(0, 1.1000, 1.1010, 1.0950, 1.1005)], signals());

    assert_price(
        from_bar.recorded_fills[0].fill.price,
        from_ticks.recorded_fills[0].fill.price,
    );
    let tick_closes = close_prices(&from_ticks);
    let bar_closes = close_prices(&from_bar);
    assert_eq!(tick_closes.len(), 1);
    assert_eq!(bar_closes.len(), 1);
    assert_eq!(bar_closes[0].0, tick_closes[0].0);
    // The tick path crosses the stop between two ticks and fills at the later tick's bid; the bar fills at the level the path passed through, which is never worse.
    assert!(bar_closes[0].1 >= tick_closes[0].1 - 1e-12);
    assert_price(bar_closes[0].1, 1.0970 - HALF);
}

#[test]
fn a_bar_that_cannot_be_quoted_is_an_invalid_quote_and_a_narrow_range_is_widened() {
    let result = run(
        vec![
            bar(0, 1.1000, 1.1010, 1.0990, f64::NAN),
            // The recorded high sits below the close, so the range is widened to the close and the target at 1.1015 is reached.
            bar(1, 1.1000, 1.1005, 1.0995, 1.1020),
        ],
        vec![market(Side::Buy, Some(1.0900), vec![1.1015])],
    );
    let tags = &result.execution_metadata.as_ref().unwrap().tags;
    assert_eq!(tags["invalid_quote_count"], "1");
    assert_eq!(result.recorded_fills[0].execution_ts, Some(ts(1)));
    let closes = close_prices(&result);
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].0, CloseReason::Target);
    assert_price(closes[0].1, 1.1015);
}
