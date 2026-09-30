#![allow(dead_code)]

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_research::SymbolEvents;
use qs_symbols::SymbolSpec;

pub const SYMBOL: &str = "EURUSD";
pub const POINT_SIZE: f64 = 1.0e-5;

pub fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 5)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

pub fn at(minutes: i64) -> NaiveDateTime {
    base() + Duration::minutes(minutes)
}

/// A deterministic tick series with a slow trend and enough turns to make crossovers fire.
///
/// The generator is a fixed-seed linear congruential sequence rather than a random source, so a table built from it is reproducible across runs and across machines, which is what lets the same data serve both the metric tests and the sequential-versus-parallel comparison.
pub fn synthetic_ticks(minutes: i64) -> SymbolEvents {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut events = Vec::new();
    for minute in 0..minutes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let noise = ((state >> 33) % 21) as i64 - 10;
        let phase = (minute % 240) as f64 / 240.0 * std::f64::consts::TAU;
        let drift = (phase.sin() * 120.0) as i64;
        let price = 1.10000 + (drift + noise) as f64 * POINT_SIZE;
        let spread = 2.0 * POINT_SIZE;
        events.push(FeedEvent::new(
            MarketEvent::Tick {
                symbol: SYMBOL.into(),
                ts: at(minute),
                bid: price,
                ask: price + spread,
            },
            EventMetadata::new(SeriesRoles::PRIMARY, 0, minute as u64),
        ));
    }
    events.into()
}

pub fn symbol_spec() -> SymbolSpec {
    SymbolSpec {
        canonical: SYMBOL.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

pub fn config() -> BacktestConfig {
    BacktestConfig {
        initial_balance: 10_000.0,
        sizing: Some(SizingPolicy::FixedLot { lots: 0.1 }),
        symbol_specs: [(SYMBOL.to_owned(), symbol_spec())].into_iter().collect(),
        contract_sizes: [(SYMBOL.to_owned(), 100_000.0)].into_iter().collect(),
        ..BacktestConfig::default()
    }
}

/// The synthetic ticks resampled into one-minute mid bars through the stored-bar aggregator, as a resample run would write them.
pub fn synthetic_stored_bars(minutes: i64) -> Vec<data_preprocess::models::Bar> {
    use data_preprocess::models::Timeframe as StoredTimeframe;
    use data_preprocess::resample::{BarAggregator, BucketSpec, PriceBasis as StoredPriceBasis};

    let mut aggregator = BarAggregator::new(
        "demo",
        SYMBOL,
        StoredTimeframe::M1,
        BucketSpec::new(60, 0).unwrap(),
        StoredPriceBasis::Mid,
        POINT_SIZE,
    );
    let mut bars = Vec::new();
    for event in synthetic_ticks(minutes).iter() {
        let MarketEvent::Tick { ts, bid, ask, .. } = &event.event else {
            unreachable!("synthetic ticks are ticks");
        };
        bars.extend(aggregator.push(*ts, Some(*bid), Some(*ask)));
    }
    bars
}

/// The synthetic stored bars converted to feed events, stamped at their bucket open.
pub fn synthetic_bars(minutes: i64) -> SymbolEvents {
    let mut feed = qs_backtest::data_feed::bars_to_feed_with_metadata(
        synthetic_stored_bars(minutes),
        SeriesRoles::PRIMARY,
        0,
    );
    let mut events = Vec::new();
    while let Some(event) = feed.next_feed_event() {
        events.push(event);
    }
    events.into()
}
