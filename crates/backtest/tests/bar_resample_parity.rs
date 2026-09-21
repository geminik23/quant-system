//! Stored bars must equal the bars the replay engine builds from the same ticks.
//!
//! A parameter search runs over stored bars and a confirmation run executes over ticks, so the two views have to agree bar for bar. Anything that changes bucket geometry, quote acceptance, or accumulation in one path has to change in the other, and these tests fail when it does not.

use std::collections::BTreeMap;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use data_preprocess::models::Timeframe as StoredTimeframe;
use data_preprocess::resample::{BarAggregator, BucketSpec, PriceBasis as StoredPriceBasis};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch};
use qs_backtest::{
    ClosedBar, MissingIntervalPolicy, MultiTimeframeSeries, PriceBasis, SeriesId,
    SeriesRequirement, Timeframe, WarmupRequirement,
};

const EXCHANGE: &str = "demo";
const SYMBOL: &str = "EURUSD";
const POINT_SIZE: f64 = 1.0e-5;

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 6, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

/// One deterministic pseudo-random walk of two-sided quotes, including gaps and invalid rows.
fn ticks() -> Vec<(NaiveDateTime, Option<f64>, Option<f64>)> {
    let mut rows = Vec::new();
    let mut price = 1.10000_f64;
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let mut minute = 0_i64;
    while minute < 60 * 30 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let step = ((state >> 33) % 21) as i64 - 10;
        price += step as f64 * POINT_SIZE;
        let spread = 1.0 + ((state >> 17) % 4) as f64;
        let ts = base() + Duration::minutes(minute);

        match (state >> 5) % 23 {
            // A one-sided row is never executable and must be skipped by both paths.
            0 => rows.push((ts, None, Some(price + spread * POINT_SIZE))),
            1 => rows.push((ts, Some(price), None)),
            // A crossed quote is rejected by both paths.
            2 => rows.push((ts, Some(price), Some(price - POINT_SIZE))),
            _ => rows.push((ts, Some(price), Some(price + spread * POINT_SIZE))),
        }

        // Advance unevenly so some buckets stay empty entirely.
        minute += match (state >> 41) % 7 {
            0 => 97,
            1 => 43,
            _ => 7,
        };
    }
    rows
}

fn engine_bars(
    timeframe: Timeframe,
    basis: PriceBasis,
    alignment_offset_seconds: i32,
    rows: &[(NaiveDateTime, Option<f64>, Option<f64>)],
) -> Vec<ClosedBar> {
    let requirement = SeriesRequirement::new(
        SeriesId::new("series").unwrap(),
        SYMBOL,
        timeframe,
        basis,
        WarmupRequirement::bars(1).unwrap(),
    )
    .unwrap();
    let spec = qs_backtest::BarSeriesSpec::new(
        requirement,
        1_000,
        alignment_offset_seconds,
        MissingIntervalPolicy::Skip,
    )
    .unwrap();
    let mut series = MultiTimeframeSeries::new(vec![spec]).unwrap();

    let mut emitted = Vec::new();
    for (index, (ts, bid, ask)) in rows.iter().enumerate() {
        let (Some(bid), Some(ask)) = (bid, ask) else {
            continue;
        };
        let batch = TimestampBatch {
            ts: *ts,
            events: vec![FeedEvent::new(
                MarketEvent::Tick {
                    symbol: SYMBOL.into(),
                    ts: *ts,
                    bid: *bid,
                    ask: *ask,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )],
        };
        emitted.extend(series.on_batch(&batch).unwrap());
    }
    emitted
}

fn stored_bars(
    timeframe: StoredTimeframe,
    basis: StoredPriceBasis,
    alignment_offset_seconds: i64,
    rows: &[(NaiveDateTime, Option<f64>, Option<f64>)],
) -> Vec<data_preprocess::models::Bar> {
    let spec = BucketSpec::new(
        timeframe.fixed_duration_seconds().unwrap(),
        alignment_offset_seconds,
    )
    .unwrap();
    let mut aggregator = BarAggregator::new(EXCHANGE, SYMBOL, timeframe, spec, basis, POINT_SIZE);
    let mut bars = Vec::new();
    for (ts, bid, ask) in rows {
        if let Some(bar) = aggregator.push(*ts, *bid, *ask) {
            bars.push(bar);
        }
    }
    bars
}

fn assert_identical(engine: &[ClosedBar], stored: &[data_preprocess::models::Bar], label: &str) {
    assert_eq!(
        engine.len(),
        stored.len(),
        "{label}: bar count differs (engine {}, stored {})",
        engine.len(),
        stored.len()
    );
    for (index, (engine_bar, stored_bar)) in engine.iter().zip(stored).enumerate() {
        assert_eq!(
            engine_bar.open_time(),
            stored_bar.ts,
            "{label}[{index}] open time"
        );
        assert_eq!(engine_bar.open(), stored_bar.open, "{label}[{index}] open");
        assert_eq!(engine_bar.high(), stored_bar.high, "{label}[{index}] high");
        assert_eq!(engine_bar.low(), stored_bar.low, "{label}[{index}] low");
        assert_eq!(
            engine_bar.close(),
            stored_bar.close,
            "{label}[{index}] close"
        );
        assert_eq!(
            i64::try_from(engine_bar.tick_count()).unwrap(),
            stored_bar.tick_vol,
            "{label}[{index}] tick count"
        );
    }
}

#[test]
fn stored_bars_match_engine_bars_across_timeframes_bases_and_offsets() {
    let rows = ticks();
    let cases: Vec<(&str, Timeframe, StoredTimeframe, i32)> = vec![
        ("1m", Timeframe::minutes(1).unwrap(), StoredTimeframe::M1, 0),
        ("1h", Timeframe::hours(1).unwrap(), StoredTimeframe::H1, 0),
        ("4h", Timeframe::hours(4).unwrap(), StoredTimeframe::H4, 0),
        ("1d", Timeframe::days(1).unwrap(), StoredTimeframe::D1, 0),
        (
            "1d at 22:00",
            Timeframe::days(1).unwrap(),
            StoredTimeframe::D1,
            79_200,
        ),
    ];
    let bases = [
        ("bid", PriceBasis::Bid, StoredPriceBasis::Bid),
        ("ask", PriceBasis::Ask, StoredPriceBasis::Ask),
        ("mid", PriceBasis::Mid, StoredPriceBasis::Mid),
    ];

    for (label, engine_tf, stored_tf, offset) in cases {
        for (basis_label, engine_basis, stored_basis) in bases {
            let engine = engine_bars(engine_tf, engine_basis, offset, &rows);
            let stored = stored_bars(stored_tf, stored_basis, i64::from(offset), &rows);
            assert!(
                !engine.is_empty(),
                "{label}/{basis_label}: fixture produced no bars"
            );
            assert_identical(&engine, &stored, &format!("{label}/{basis_label}"));
        }
    }
}

#[test]
fn a_gap_produces_no_bars_on_either_path() {
    let rows = vec![
        (base(), Some(1.10000), Some(1.10002)),
        (base() + Duration::hours(5), Some(1.10500), Some(1.10502)),
        (base() + Duration::hours(6), Some(1.10600), Some(1.10602)),
    ];
    let engine = engine_bars(Timeframe::hours(1).unwrap(), PriceBasis::Bid, 0, &rows);
    let stored = stored_bars(StoredTimeframe::H1, StoredPriceBasis::Bid, 0, &rows);
    assert_eq!(engine.len(), 2);
    assert_identical(&engine, &stored, "gap");
}

#[test]
fn the_average_spread_is_recorded_in_points() {
    let rows = vec![
        (base(), Some(1.10000), Some(1.10001)),
        (base() + Duration::minutes(10), Some(1.10000), Some(1.10003)),
        (base() + Duration::hours(1), Some(1.10000), Some(1.10001)),
    ];
    let stored = stored_bars(StoredTimeframe::H1, StoredPriceBasis::Bid, 0, &rows);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].spread, 2);
}

#[test]
fn a_stored_spread_becomes_a_two_sided_bar_quote() {
    let bar = MarketEvent::Bar {
        symbol: SYMBOL.into(),
        ts: base(),
        open: 1.1,
        high: 1.2,
        low: 1.0,
        close: 1.1,
        volume: 0,
        spread: Some(0.00002),
    };
    let quote = bar.to_quote();
    assert!((quote.bid - 1.09999).abs() < 1.0e-12);
    assert!((quote.ask - 1.10001).abs() < 1.0e-12);
    assert!(!bar.is_zero_spread_bar());
}

#[test]
fn a_bar_without_a_spread_uses_the_fallback_and_is_reported() {
    let bar = MarketEvent::Bar {
        symbol: SYMBOL.into(),
        ts: base(),
        open: 1.1,
        high: 1.2,
        low: 1.0,
        close: 1.1,
        volume: 0,
        spread: None,
    };
    assert!(bar.is_zero_spread_bar());

    let unpriced = bar.to_quote();
    assert_eq!(unpriced.bid, unpriced.ask);

    let priced = bar.to_quote_with_spread_fallback(Some(0.00004));
    assert!((priced.bid - 1.09998).abs() < 1.0e-12);
    assert!((priced.ask - 1.10002).abs() < 1.0e-12);
}

#[test]
fn bucket_geometry_is_shared_between_both_paths() {
    // A daily bucket aligned to 22:00 must place 23:00 into the bucket that opened the same evening.
    let spec = BucketSpec::new(86_400, 79_200).unwrap();
    let (open, close) =
        data_preprocess::resample::bucket_bounds(base() + Duration::hours(23), spec).unwrap();
    assert_eq!(open, base() + Duration::hours(22));
    assert_eq!(close, open + Duration::days(1));

    let mut offsets = BTreeMap::new();
    offsets.insert("reduced", BucketSpec::new(3_600, 7_200).unwrap());
    assert_eq!(offsets["reduced"].alignment_offset_seconds(), 0);
}
