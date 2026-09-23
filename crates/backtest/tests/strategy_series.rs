use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch};
use qs_backtest::{
    BarSeriesSpec, HistoricalSeriesView, MAX_RETAINED_BARS, MissingIntervalPolicy,
    MultiTimeframeSeries, PriceBasis, SeriesError, SeriesId, SeriesRequirement, SeriesViewError,
    StrategyRequirements, Timeframe, WarmupRequirement,
};

fn base_ts() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn requirement(
    id: &str,
    symbol: &str,
    timeframe: Timeframe,
    basis: PriceBasis,
    warmup: usize,
) -> SeriesRequirement {
    SeriesRequirement::new(
        SeriesId::new(id).unwrap(),
        symbol,
        timeframe,
        basis,
        WarmupRequirement::bars(warmup).unwrap(),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn spec(
    id: &str,
    symbol: &str,
    timeframe: Timeframe,
    basis: PriceBasis,
    warmup: usize,
    retained: usize,
    offset: i32,
    missing: MissingIntervalPolicy,
) -> BarSeriesSpec {
    BarSeriesSpec::new(
        requirement(id, symbol, timeframe, basis, warmup),
        retained,
        offset,
        missing,
    )
    .unwrap()
}

fn tick(
    symbol: &str,
    ts: NaiveDateTime,
    bid: f64,
    ask: f64,
    roles: SeriesRoles,
    rank: u32,
    sequence: u64,
) -> FeedEvent {
    FeedEvent::new(
        MarketEvent::Tick {
            symbol: symbol.to_string(),
            ts,
            bid,
            ask,
        },
        EventMetadata::new(roles, rank, sequence),
    )
}

/// One stored bar stamped at its bucket open, as the resampler writes it.
fn stored_bar(
    symbol: &str,
    ts: NaiveDateTime,
    ohlc: [f64; 4],
    timeframe_seconds: Option<u64>,
    tick_count: Option<u64>,
) -> FeedEvent {
    FeedEvent::new(
        MarketEvent::Bar {
            symbol: symbol.to_string(),
            ts,
            open: ohlc[0],
            high: ohlc[1],
            low: ohlc[2],
            close: ohlc[3],
            volume: 0,
            spread: None,
            timeframe_seconds,
            tick_count,
        },
        EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
    )
}

fn m5_bar(minute: i64, close: f64) -> TimestampBatch {
    let ts = base_ts() + Duration::minutes(minute);
    batch(
        ts,
        vec![stored_bar(
            "EURUSD",
            ts,
            [close - 0.1, close + 0.2, close - 0.2, close],
            Some(300),
            Some(7),
        )],
    )
}

fn m5_series(missing: MissingIntervalPolicy) -> MultiTimeframeSeries {
    MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Mid,
        0,
        8,
        0,
        missing,
    )])
    .unwrap()
}

fn batch(ts: NaiveDateTime, events: Vec<FeedEvent>) -> TimestampBatch {
    TimestampBatch { ts, events }
}

fn primary_tick(symbol: &str, ts: NaiveDateTime, bid: f64, ask: f64) -> TimestampBatch {
    batch(
        ts,
        vec![tick(symbol, ts, bid, ask, SeriesRoles::PRIMARY, 0, 0)],
    )
}

#[test]
fn validates_capacity_offsets_and_unique_series_ids() {
    let requirement = requirement(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Mid,
        2,
    );
    assert!(matches!(
        BarSeriesSpec::new(requirement.clone(), 0, 0, MissingIntervalPolicy::Skip),
        Err(SeriesError::ZeroRetention { .. })
    ));
    assert!(matches!(
        BarSeriesSpec::new(requirement.clone(), 1, 0, MissingIntervalPolicy::Skip),
        Err(SeriesError::RetentionBelowWarmup { .. })
    ));
    assert!(matches!(
        BarSeriesSpec::new(
            requirement.clone(),
            MAX_RETAINED_BARS + 1,
            0,
            MissingIntervalPolicy::Skip
        ),
        Err(SeriesError::RetentionTooLarge { .. })
    ));

    let positive =
        BarSeriesSpec::new(requirement.clone(), 2, 60, MissingIntervalPolicy::Skip).unwrap();
    let negative =
        BarSeriesSpec::new(requirement.clone(), 2, -240, MissingIntervalPolicy::Skip).unwrap();
    let equivalent = BarSeriesSpec::new(requirement, 2, 360, MissingIntervalPolicy::Skip).unwrap();
    assert_eq!(positive.alignment_offset_seconds(), 60);
    assert_eq!(negative.alignment_offset_seconds(), 60);
    assert_eq!(equivalent.alignment_offset_seconds(), 60);
    for aligned in [positive.clone(), negative, equivalent] {
        let mut series = MultiTimeframeSeries::new(vec![aligned]).unwrap();
        let open = base_ts() + Duration::seconds(60);
        series
            .on_batch(&primary_tick("EURUSD", open, 1.0, 1.1))
            .unwrap();
        let closed = series
            .on_batch(&primary_tick(
                "EURUSD",
                open + Duration::minutes(5),
                2.0,
                2.1,
            ))
            .unwrap();
        assert_eq!(closed[0].open_time(), open);
        assert_eq!(closed[0].close_time(), open + Duration::minutes(5));
    }

    let duplicate = MultiTimeframeSeries::new(vec![positive.clone(), positive]);
    assert!(matches!(
        duplicate,
        Err(SeriesError::DuplicateSeriesId { .. })
    ));
}

#[test]
fn closes_standard_timeframes_at_aligned_boundaries() {
    let cases = [
        ("m5", Timeframe::minutes(5).unwrap()),
        ("m15", Timeframe::minutes(15).unwrap()),
        ("h1", Timeframe::hours(1).unwrap()),
        ("h4", Timeframe::hours(4).unwrap()),
        ("d1", Timeframe::days(1).unwrap()),
    ];
    for (id, timeframe) in cases {
        let mut series = MultiTimeframeSeries::new(vec![spec(
            id,
            "EURUSD",
            timeframe,
            PriceBasis::Bid,
            0,
            2,
            0,
            MissingIntervalPolicy::Skip,
        )])
        .unwrap();
        let start = base_ts();
        assert!(
            series
                .on_batch(&primary_tick("EURUSD", start, 1.0, 1.1))
                .unwrap()
                .is_empty()
        );
        let boundary = start + Duration::seconds(timeframe.duration_seconds() as i64);
        let closed = series
            .on_batch(&primary_tick("EURUSD", boundary, 2.0, 2.1))
            .unwrap();
        assert_eq!(closed.len(), 1, "{id}");
        assert_eq!(closed[0].open_time(), start, "{id}");
        assert_eq!(closed[0].close_time(), boundary, "{id}");
        assert_eq!(closed[0].close(), 1.0, "{id}");
    }
}

#[test]
fn exact_boundary_tick_starts_new_bar_and_subseconds_keep_source_order() {
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        3,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    let first = start + Duration::seconds(299) + Duration::milliseconds(100);
    let second = start + Duration::seconds(299) + Duration::milliseconds(900);
    series
        .on_batch(&batch(
            first,
            vec![tick("EURUSD", first, 3.0, 3.1, SeriesRoles::PRIMARY, 0, 1)],
        ))
        .unwrap();
    series
        .on_batch(&batch(
            second,
            vec![tick("EURUSD", second, 2.0, 2.1, SeriesRoles::PRIMARY, 0, 2)],
        ))
        .unwrap();
    let boundary = start + Duration::minutes(5);
    let closed = series
        .on_batch(&primary_tick("EURUSD", boundary, 9.0, 9.1))
        .unwrap();
    assert_eq!(closed[0].open(), 3.0);
    assert_eq!(closed[0].close(), 2.0);
    assert_eq!(closed[0].tick_count(), 2);

    let next = series
        .on_batch(&primary_tick(
            "EURUSD",
            boundary + Duration::minutes(5),
            8.0,
            8.1,
        ))
        .unwrap();
    assert_eq!(next[0].open(), 9.0);
    assert_eq!(next[0].tick_count(), 1);
}

#[test]
fn computes_exact_bid_ask_and_overflow_safe_midpoint_ohlc() {
    let timeframe = Timeframe::minutes(5).unwrap();
    let mut series = MultiTimeframeSeries::new(vec![
        spec(
            "ask",
            "EURUSD",
            timeframe,
            PriceBasis::Ask,
            0,
            2,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "bid",
            "EURUSD",
            timeframe,
            PriceBasis::Bid,
            0,
            2,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "mid",
            "EURUSD",
            timeframe,
            PriceBasis::Mid,
            0,
            2,
            0,
            MissingIntervalPolicy::Skip,
        ),
    ])
    .unwrap();
    let start = base_ts();
    for (index, (bid, ask)) in [(1.0, 3.0), (2.0, 4.0), (0.5, 5.0)].into_iter().enumerate() {
        let ts = start + Duration::seconds(index as i64);
        series
            .on_batch(&primary_tick("EURUSD", ts, bid, ask))
            .unwrap();
    }
    let closed = series
        .on_batch(&primary_tick(
            "EURUSD",
            start + Duration::minutes(5),
            10.0,
            12.0,
        ))
        .unwrap();
    let ask = closed
        .iter()
        .find(|bar| bar.series_id().as_str() == "ask")
        .unwrap();
    assert_eq!(
        (ask.open(), ask.high(), ask.low(), ask.close()),
        (3.0, 5.0, 3.0, 5.0)
    );
    let bid = closed
        .iter()
        .find(|bar| bar.series_id().as_str() == "bid")
        .unwrap();
    assert_eq!(
        (bid.open(), bid.high(), bid.low(), bid.close()),
        (1.0, 2.0, 0.5, 0.5)
    );
    let mid = closed
        .iter()
        .find(|bar| bar.series_id().as_str() == "mid")
        .unwrap();
    assert_eq!(
        (mid.open(), mid.high(), mid.low(), mid.close()),
        (2.0, 3.0, 2.0, 2.75)
    );
    assert_eq!(mid.tick_count(), 3);

    let mut large = MultiTimeframeSeries::new(vec![spec(
        "large-mid",
        "BIG",
        timeframe,
        PriceBasis::Mid,
        0,
        2,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let bid = f64::MAX / 2.0;
    let ask = f64::MAX;
    large
        .on_batch(&primary_tick("BIG", start, bid, ask))
        .unwrap();
    let bar = large
        .on_batch(&primary_tick("BIG", start + Duration::minutes(5), bid, ask))
        .unwrap();
    assert!(bar[0].open().is_finite());
}

#[test]
fn one_tick_updates_all_matching_timeframes_and_output_order_is_stable() {
    let mut series = MultiTimeframeSeries::new(vec![
        spec(
            "z-h1",
            "EURUSD",
            Timeframe::hours(1).unwrap(),
            PriceBasis::Bid,
            0,
            3,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "a-m5",
            "EURUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Bid,
            0,
            3,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "b-m5",
            "GBPUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Bid,
            0,
            3,
            0,
            MissingIntervalPolicy::Skip,
        ),
    ])
    .unwrap();
    let start = base_ts();
    series
        .on_batch(&batch(
            start,
            vec![
                tick("GBPUSD", start, 2.0, 2.1, SeriesRoles::PRIMARY, 2, 2),
                tick("EURUSD", start, 1.0, 1.1, SeriesRoles::PRIMARY, 1, 1),
            ],
        ))
        .unwrap();
    let pre_close = start + Duration::minutes(55);
    series
        .on_batch(&batch(
            pre_close,
            vec![
                tick("GBPUSD", pre_close, 3.0, 3.1, SeriesRoles::PRIMARY, 2, 4),
                tick("EURUSD", pre_close, 4.0, 4.1, SeriesRoles::PRIMARY, 1, 3),
            ],
        ))
        .unwrap();
    let reveal = start + Duration::hours(1);
    let closed = series
        .on_batch(&batch(
            reveal,
            vec![
                tick("GBPUSD", reveal, 5.0, 5.1, SeriesRoles::PRIMARY, 2, 6),
                tick("EURUSD", reveal, 6.0, 6.1, SeriesRoles::PRIMARY, 1, 5),
            ],
        ))
        .unwrap();
    let ids = closed
        .iter()
        .map(|bar| bar.series_id().as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["a-m5", "b-m5", "z-h1"]);
    assert!(closed.iter().all(|bar| bar.close_time() == reveal));
}

#[test]
fn selects_primary_ticks_once_and_ignores_conversion_ticks() {
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        3,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    series
        .on_batch(&batch(
            start,
            vec![
                tick("EURUSD", start, 9.0, 9.1, SeriesRoles::CONVERSION, 0, 0),
                tick(
                    "EURUSD",
                    start,
                    2.0,
                    2.1,
                    SeriesRoles::PRIMARY_AND_CONVERSION,
                    2,
                    2,
                ),
                tick("EURUSD", start, 3.0, 3.1, SeriesRoles::PRIMARY, 3, 3),
            ],
        ))
        .unwrap();
    let closed = series
        .on_batch(&primary_tick(
            "EURUSD",
            start + Duration::minutes(5),
            4.0,
            4.1,
        ))
        .unwrap();
    assert_eq!(closed[0].open(), 2.0);
    assert_eq!(closed[0].close(), 3.0);
    assert_eq!(closed[0].tick_count(), 2);
}

#[test]
fn skips_invalid_quotes_but_keeps_their_timestamp_for_regression_detection() {
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        3,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    for (index, (bid, ask)) in [
        (0.0, 1.0),
        (2.0, 1.0),
        (f64::NAN, 1.0),
        (1.0, f64::INFINITY),
    ]
    .into_iter()
    .enumerate()
    {
        let ts = start + Duration::seconds(index as i64);
        assert!(
            series
                .on_batch(&primary_tick("EURUSD", ts, bid, ask))
                .unwrap()
                .is_empty()
        );
    }
    let valid_ts = start + Duration::minutes(5);
    series
        .on_batch(&primary_tick("EURUSD", valid_ts, 1.0, 1.1))
        .unwrap();
    let error = series
        .on_batch(&primary_tick(
            "EURUSD",
            valid_ts - Duration::seconds(1),
            1.0,
            1.1,
        ))
        .unwrap_err();
    assert!(matches!(error, SeriesError::TimestampRegression { .. }));
    assert_eq!(
        series
            .warmup(&SeriesId::new("m5").unwrap())
            .unwrap()
            .available_bars(),
        0
    );
}

#[test]
fn rejects_bad_batch_metadata_and_preserves_state() {
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        3,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    series
        .on_batch(&primary_tick("EURUSD", start, 1.0, 1.1))
        .unwrap();

    let mismatch = batch(
        start + Duration::minutes(5),
        vec![tick(
            "EURUSD",
            start + Duration::minutes(6),
            2.0,
            2.1,
            SeriesRoles::PRIMARY,
            0,
            0,
        )],
    );
    assert!(matches!(
        series.on_batch(&mismatch),
        Err(SeriesError::BatchTimestampMismatch { .. })
    ));

    let boundary = start + Duration::minutes(5);
    let duplicate = batch(
        boundary,
        vec![
            tick("EURUSD", boundary, 2.0, 2.1, SeriesRoles::PRIMARY, 0, 7),
            tick("EURUSD", boundary, 3.0, 3.1, SeriesRoles::PRIMARY, 0, 7),
        ],
    );
    assert!(matches!(
        series.on_batch(&duplicate),
        Err(SeriesError::DuplicateOrderingMetadata { .. })
    ));

    let closed = series
        .on_batch(&primary_tick("EURUSD", boundary, 4.0, 4.1))
        .unwrap();
    assert_eq!(closed[0].open(), 1.0);
    assert_eq!(closed[0].tick_count(), 1);
}

#[test]
fn missing_interval_policies_skip_or_reject_and_errors_are_batch_atomic() {
    let mut series = MultiTimeframeSeries::new(vec![
        spec(
            "eur-skip",
            "EURUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Bid,
            0,
            3,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "gbp-reject",
            "GBPUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Bid,
            0,
            3,
            0,
            MissingIntervalPolicy::Reject,
        ),
    ])
    .unwrap();
    let start = base_ts();
    series
        .on_batch(&batch(
            start,
            vec![
                tick("EURUSD", start, 1.0, 1.1, SeriesRoles::PRIMARY, 0, 0),
                tick("GBPUSD", start, 2.0, 2.1, SeriesRoles::PRIMARY, 1, 1),
            ],
        ))
        .unwrap();
    let delayed = start + Duration::minutes(10);
    let error = series
        .on_batch(&batch(
            delayed,
            vec![
                tick("EURUSD", delayed, 3.0, 3.1, SeriesRoles::PRIMARY, 0, 2),
                tick("GBPUSD", delayed, 4.0, 4.1, SeriesRoles::PRIMARY, 1, 3),
            ],
        ))
        .unwrap_err();
    assert!(matches!(error, SeriesError::MissingInterval { .. }));
    assert!(
        series
            .latest_bar(&SeriesId::new("eur-skip").unwrap())
            .unwrap()
            .is_none()
    );

    let adjacent = start + Duration::minutes(5);
    let closed = series
        .on_batch(&batch(
            adjacent,
            vec![
                tick("EURUSD", adjacent, 5.0, 5.1, SeriesRoles::PRIMARY, 0, 4),
                tick("GBPUSD", adjacent, 6.0, 6.1, SeriesRoles::PRIMARY, 1, 5),
            ],
        ))
        .unwrap();
    assert_eq!(closed.len(), 2);
    assert_eq!(closed[0].close_time(), adjacent);
    assert_eq!(closed[1].close_time(), adjacent);
}

#[test]
fn incomplete_bars_are_hidden_and_boundary_overflow_is_typed() {
    let id = SeriesId::new("m5").unwrap();
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        2,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    series
        .on_batch(&primary_tick("EURUSD", base_ts(), 1.0, 1.1))
        .unwrap();
    assert!(series.latest_bar(&id).unwrap().is_none());
    assert!(series.bars(&id, 10).unwrap().is_empty());

    let mut extreme = MultiTimeframeSeries::new(vec![spec(
        "huge",
        "EURUSD",
        Timeframe::days(u32::MAX).unwrap(),
        PriceBasis::Bid,
        0,
        1,
        i32::MAX,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    assert!(matches!(
        extreme.on_batch(&primary_tick("EURUSD", NaiveDateTime::MAX, 1.0, 1.1)),
        Err(SeriesError::BoundaryOverflow { .. })
    ));
}

#[test]
fn bounded_windows_work_before_and_after_wrap_and_return_available_suffix() {
    let id = SeriesId::new("m5").unwrap();
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        2,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    for index in 0..=3 {
        let ts = start + Duration::minutes(index * 5);
        series
            .on_batch(&primary_tick(
                "EURUSD",
                ts,
                index as f64 + 1.0,
                index as f64 + 1.1,
            ))
            .unwrap();
    }
    let all = series.bars(&id, 10).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        all.iter().map(|bar| bar.open()).collect::<Vec<_>>(),
        [2.0, 3.0]
    );
    assert_eq!(all.latest().unwrap().open(), 3.0);
    assert_eq!(
        series
            .bars(&id, 1)
            .unwrap()
            .iter()
            .map(|bar| bar.open())
            .collect::<Vec<_>>(),
        [3.0]
    );
    assert!(series.bars(&id, 0).unwrap().is_empty());

    fn object_safe_len(view: &dyn HistoricalSeriesView, id: &SeriesId) -> usize {
        view.bars(id, usize::MAX).unwrap().len()
    }
    assert_eq!(object_safe_len(&series, &id), 2);
}

#[test]
fn retention_does_not_change_emitted_bars_or_total_warmup() {
    let make = |retained| {
        MultiTimeframeSeries::new(vec![spec(
            "m5",
            "EURUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Bid,
            2,
            retained,
            0,
            MissingIntervalPolicy::Skip,
        )])
        .unwrap()
    };
    let mut small = make(2);
    let mut large = make(5);
    let start = base_ts();
    let mut small_emitted = Vec::new();
    let mut large_emitted = Vec::new();
    for index in 0..=5 {
        let ts = start + Duration::minutes(index * 5);
        let input = primary_tick("EURUSD", ts, index as f64 + 1.0, index as f64 + 1.1);
        small_emitted.extend(small.on_batch(&input).unwrap());
        large_emitted.extend(large.on_batch(&input).unwrap());
    }
    assert_eq!(small_emitted, large_emitted);
    let id = SeriesId::new("m5").unwrap();
    assert_eq!(small.bars(&id, 10).unwrap().len(), 2);
    assert_eq!(large.bars(&id, 10).unwrap().len(), 5);
    assert_eq!(small.warmup(&id).unwrap().available_bars(), 5);
    assert_eq!(large.warmup(&id).unwrap().available_bars(), 5);
}

#[test]
fn warmup_is_derived_per_series_and_aggregated_by_existing_requirements() {
    let zero = requirement(
        "h1",
        "EURUSD",
        Timeframe::hours(1).unwrap(),
        PriceBasis::Bid,
        0,
    );
    let two = requirement(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Mid,
        2,
    );
    let requirements = StrategyRequirements::new(
        vec!["EURUSD".to_string()],
        vec![zero.clone(), two.clone()],
        0,
        true,
        false,
    )
    .unwrap();
    let mut series = MultiTimeframeSeries::new(vec![
        BarSeriesSpec::new(zero, 1, 0, MissingIntervalPolicy::Skip).unwrap(),
        BarSeriesSpec::new(two, 2, 0, MissingIntervalPolicy::Skip).unwrap(),
    ])
    .unwrap();
    let h1 = SeriesId::new("h1").unwrap();
    let m5 = SeriesId::new("m5").unwrap();
    assert!(series.warmup(&h1).unwrap().is_ready());
    assert!(!series.warmup(&m5).unwrap().is_ready());
    assert!(!series.warmup_complete(&requirements).unwrap());

    let start = base_ts();
    for index in 0..=2 {
        let ts = start + Duration::minutes(index * 5);
        series
            .on_batch(&primary_tick("EURUSD", ts, 1.0, 1.1))
            .unwrap();
    }
    assert_eq!(series.warmup(&m5).unwrap().available_bars(), 2);
    assert!(series.warmup(&m5).unwrap().is_ready());
    assert!(series.warmup_complete(&requirements).unwrap());
}

#[test]
fn unknown_series_errors_are_typed_and_lookup_ignores_spec_order() {
    let first = spec(
        "z-series",
        "EURUSD",
        Timeframe::hours(1).unwrap(),
        PriceBasis::Bid,
        0,
        1,
        0,
        MissingIntervalPolicy::Skip,
    );
    let second = spec(
        "a-series",
        "GBPUSD",
        Timeframe::hours(1).unwrap(),
        PriceBasis::Bid,
        0,
        1,
        0,
        MissingIntervalPolicy::Skip,
    );
    let series = MultiTimeframeSeries::new(vec![first, second]).unwrap();
    assert!(series.warmup(&SeriesId::new("a-series").unwrap()).is_ok());
    let missing = SeriesId::new("missing").unwrap();
    assert!(matches!(
        series.latest_bar(&missing),
        Err(SeriesViewError::UnknownSeries { .. })
    ));
}

#[test]
fn delayed_reveal_keeps_scheduled_close_and_never_rewrites_history() {
    let id = SeriesId::new("m5").unwrap();
    let mut series = MultiTimeframeSeries::new(vec![spec(
        "m5",
        "EURUSD",
        Timeframe::minutes(5).unwrap(),
        PriceBasis::Bid,
        0,
        3,
        0,
        MissingIntervalPolicy::Skip,
    )])
    .unwrap();
    let start = base_ts();
    series
        .on_batch(&primary_tick("EURUSD", start, 1.0, 1.1))
        .unwrap();
    let reveal = start + Duration::minutes(17);
    let closed = series
        .on_batch(&primary_tick("EURUSD", reveal, 2.0, 2.1))
        .unwrap();
    assert_eq!(closed[0].close_time(), start + Duration::minutes(5));
    let snapshot = series.latest_bar(&id).unwrap().unwrap().clone();
    series
        .on_batch(&primary_tick(
            "EURUSD",
            reveal + Duration::seconds(1),
            3.0,
            3.1,
        ))
        .unwrap();
    assert_eq!(series.latest_bar(&id).unwrap().unwrap(), &snapshot);
    assert_eq!(snapshot.close_time().nanosecond(), 0);
}

#[test]
fn stored_bar_becomes_visible_only_when_a_later_bucket_arrives() {
    let mut series = m5_series(MissingIntervalPolicy::Skip);
    assert!(series.on_batch(&m5_bar(0, 1.5)).unwrap().is_empty());
    let id = SeriesId::new("m5").unwrap();
    assert!(series.latest_bar(&id).unwrap().is_none());

    let closed = series.on_batch(&m5_bar(5, 1.7)).unwrap();
    assert_eq!(closed.len(), 1);
    let bar = &closed[0];
    assert_eq!(bar.open_time(), base_ts());
    assert_eq!(bar.close_time(), base_ts() + Duration::minutes(5));
    assert_eq!(
        (bar.open(), bar.high(), bar.low(), bar.close()),
        (1.4, 1.7, 1.3, 1.5)
    );
    assert_eq!(bar.tick_count(), 7);
    assert_eq!(series.warmup(&id).unwrap().available_bars(), 1);
}

#[test]
fn stored_bars_route_only_to_series_with_their_timeframe() {
    let mut series = MultiTimeframeSeries::new(vec![
        spec(
            "m1",
            "EURUSD",
            Timeframe::minutes(1).unwrap(),
            PriceBasis::Mid,
            0,
            8,
            0,
            MissingIntervalPolicy::Skip,
        ),
        spec(
            "m5",
            "EURUSD",
            Timeframe::minutes(5).unwrap(),
            PriceBasis::Mid,
            0,
            8,
            0,
            MissingIntervalPolicy::Skip,
        ),
    ])
    .unwrap();
    series.on_batch(&m5_bar(0, 1.5)).unwrap();
    let closed = series.on_batch(&m5_bar(5, 1.6)).unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].series_id().as_str(), "m5");
    assert!(
        series
            .latest_bar(&SeriesId::new("m1").unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn stored_bars_with_unknown_or_mismatched_geometry_are_rejected() {
    let start = base_ts();
    let reject = |event: FeedEvent| {
        let mut series = m5_series(MissingIntervalPolicy::Skip);
        let ts = event.event.ts();
        series.on_batch(&batch(ts, vec![event])).unwrap_err()
    };
    let ohlc = [1.0, 1.2, 0.9, 1.1];

    assert!(matches!(
        reject(stored_bar("EURUSD", start, ohlc, None, Some(1))),
        SeriesError::StoredBarWithoutTimeframe { .. }
    ));
    assert!(matches!(
        reject(stored_bar("EURUSD", start, ohlc, Some(60), Some(1))),
        SeriesError::StoredBarUnmatched {
            timeframe_seconds: 60,
            ..
        }
    ));
    let misaligned = start + Duration::minutes(2);
    assert_eq!(
        reject(stored_bar("EURUSD", misaligned, ohlc, Some(300), Some(1))),
        SeriesError::StoredBarMisaligned {
            series_id: SeriesId::new("m5").unwrap(),
            timestamp: misaligned,
            bucket_open: start,
        }
    );
    assert!(matches!(
        reject(stored_bar("EURUSD", start, ohlc, Some(300), None)),
        SeriesError::InvalidStoredBar { .. }
    ));
    assert!(matches!(
        reject(stored_bar(
            "EURUSD",
            start,
            [1.0, 1.05, 0.9, 1.1],
            Some(300),
            Some(1)
        )),
        SeriesError::InvalidStoredBar { .. }
    ));
    assert!(matches!(
        reject(stored_bar(
            "EURUSD",
            start,
            [f64::NAN, 1.2, 0.9, 1.1],
            Some(300),
            Some(1)
        )),
        SeriesError::InvalidStoredBar { .. }
    ));

    let mut unrelated = m5_series(MissingIntervalPolicy::Skip);
    unrelated
        .on_batch(&batch(
            start,
            vec![stored_bar("GBPUSD", start, ohlc, None, None)],
        ))
        .unwrap();
}

#[test]
fn duplicate_stored_bars_mixed_input_and_gaps_are_rejected() {
    let mut series = m5_series(MissingIntervalPolicy::Skip);
    series.on_batch(&m5_bar(0, 1.5)).unwrap();
    let start = base_ts();
    let duplicate = batch(
        start,
        vec![stored_bar(
            "EURUSD",
            start,
            [1.0, 1.2, 0.9, 1.1],
            Some(300),
            Some(1),
        )],
    );
    assert!(matches!(
        series.on_batch(&duplicate).unwrap_err(),
        SeriesError::DuplicateStoredBar { .. }
    ));

    let mut mixed = m5_series(MissingIntervalPolicy::Skip);
    mixed.on_batch(&m5_bar(0, 1.5)).unwrap();
    let error = mixed
        .on_batch(&primary_tick(
            "EURUSD",
            start + Duration::minutes(6),
            1.0,
            1.1,
        ))
        .unwrap_err();
    assert!(matches!(error, SeriesError::MixedSeriesInput { .. }));

    let mut strict = m5_series(MissingIntervalPolicy::Reject);
    strict.on_batch(&m5_bar(0, 1.5)).unwrap();
    assert!(matches!(
        strict.on_batch(&m5_bar(15, 1.6)).unwrap_err(),
        SeriesError::MissingInterval { .. }
    ));
    let mut skipping = m5_series(MissingIntervalPolicy::Skip);
    skipping.on_batch(&m5_bar(0, 1.5)).unwrap();
    assert_eq!(skipping.on_batch(&m5_bar(15, 1.6)).unwrap().len(), 1);
}
