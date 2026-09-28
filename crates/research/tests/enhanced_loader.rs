use chrono::{Duration, NaiveDate};
use data_preprocess::{ParquetStore, PriceBar, StoredTick, Tick, Timeframe};
use qs_backtest::data_feed::FallibleBatchFeed;
use qs_research::*;
use std::sync::Arc;
fn ts(minute: i64) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        + Duration::minutes(minute)
}
#[test]
fn enhanced_loaders_preserve_order_optional_count_and_declared_memory_bounds() {
    let root = std::env::temp_dir().join(format!("qs-research-enhanced-{}", std::process::id()));
    let store = ParquetStore::open(&root).unwrap();
    let ticks = (0..3)
        .map(|ordinal| StoredTick {
            tick: Tick {
                exchange: "demo".into(),
                symbol: "EURUSD".into(),
                ts: ts(0),
                bid: Some(1.0 + ordinal as f64 * 0.01),
                ask: Some(1.1 + ordinal as f64 * 0.01),
                last: None,
                volume: None,
                flags: None,
            },
            source_ordinal: ordinal,
            source_identity: None,
            provider_sequence: None,
        })
        .collect::<Vec<_>>();
    store.insert_stored_ticks(&ticks).unwrap();
    let limits = MarketLoadLimits::new(16, 1 << 20).unwrap();
    let loaded =
        load_symbol_ordered_ticks(root.to_str().unwrap(), "demo", "EURUSD", limits).unwrap();
    assert_eq!(loaded.len(), 3);
    let mut tick_stream = qs_market_loader::open_ordered_stored_tick_stream(
        root.to_str().unwrap(),
        "demo",
        "EURUSD",
        "EURUSD",
        data_preprocess::ParquetScanBounds::default(),
        limits,
        Arc::new(|| false),
    )
    .unwrap();
    let mut streamed_ticks = Vec::new();
    while let Some(batch) = tick_stream.next_batch().unwrap() {
        streamed_ticks.extend(batch.events);
    }
    assert_eq!(
        streamed_ticks
            .iter()
            .map(qs_backtest::data_feed::FeedEvent::ordering_key)
            .collect::<Vec<_>>(),
        loaded
            .iter()
            .map(qs_backtest::data_feed::FeedEvent::ordering_key)
            .collect::<Vec<_>>()
    );
    assert!(
        loaded
            .windows(2)
            .all(|pair| pair[0].event.ts() == pair[1].event.ts())
    );
    let descriptor = SeriesDescriptor {
        source_identity: "fixture".into(),
        exchange: "demo".into(),
        symbol: "EURUSD".into(),
        timeframe_seconds: 60,
        price_basis: StoredPriceBasis::Mid,
        alignment_offset_seconds: 0,
        digits: 5,
        point_size: 0.00001,
        count_capability: CountCapability::Optional,
        verified: true,
    };
    let bars = vec![
        PriceBar {
            exchange: "demo".into(),
            symbol: "EURUSD".into(),
            timeframe: Timeframe::M1,
            ts: ts(0),
            available_at: ts(1),
            open: 1.0,
            high: 1.2,
            low: 0.9,
            close: 1.1,
            tick_count: None,
            spread: Some(2),
        },
        PriceBar {
            exchange: "demo".into(),
            symbol: "EURUSD".into(),
            timeframe: Timeframe::M1,
            ts: ts(1),
            available_at: ts(5),
            open: 1.1,
            high: 1.3,
            low: 1.0,
            close: 1.2,
            tick_count: Some(7),
            spread: Some(2),
        },
    ];
    store.insert_price_bars(&descriptor, &bars).unwrap();
    let loaded = load_symbol_price_bars(root.to_str().unwrap(), &descriptor, limits).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].available_at(), ts(1));
    assert_eq!(loaded[1].available_at(), ts(5));
    assert_eq!(loaded[1].event.ts(), ts(1));
    let mut bar_stream = qs_market_loader::open_price_bar_stream(
        root.to_str().unwrap(),
        &descriptor,
        "EURUSD",
        data_preprocess::ParquetScanBounds::default(),
        limits,
        Arc::new(|| false),
    )
    .unwrap();
    let mut streamed_bars = Vec::new();
    while let Some(batch) = bar_stream.next_batch().unwrap() {
        streamed_bars.extend(batch.events);
    }
    assert_eq!(
        streamed_bars
            .iter()
            .map(qs_backtest::data_feed::FeedEvent::ordering_key)
            .collect::<Vec<_>>(),
        loaded
            .iter()
            .map(qs_backtest::data_feed::FeedEvent::ordering_key)
            .collect::<Vec<_>>()
    );
    assert!(
        qs_market_loader::open_price_bar_stream(
            root.to_str().unwrap(),
            &descriptor,
            "EURUSD",
            data_preprocess::ParquetScanBounds::default(),
            limits,
            Arc::new(|| true),
        )
        .is_err()
    );
    assert!(matches!(
        loaded[0].event,
        qs_backtest::MarketEvent::Bar {
            tick_count: None,
            ..
        }
    ));
    assert!(
        load_symbol_price_bars(
            root.to_str().unwrap(),
            &descriptor,
            MarketLoadLimits::new(2, 1).unwrap()
        )
        .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}
