use std::collections::HashMap;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::{
    BacktestRunner, FutureQuoteConfig, MarketEvent, PositionRef, RawSignal, VecFeed,
};
use qs_core::{OrderType, Side, SizingPolicy};
use qs_symbols::SymbolSpec;

fn ts(milliseconds: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::milliseconds(milliseconds)
}

fn spec(symbol: &str, category: &str, lot_base_units: i64) -> SymbolSpec {
    SymbolSpec {
        canonical: symbol.to_ascii_lowercase(),
        pip_position: 2,
        digits: 5,
        category: category.into(),
        lot_base_units,
        lot_step_units: lot_base_units,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn run_linear_round_trip(
    symbol: &str,
    category: &str,
    contract_multiplier: i64,
    entry_price: f64,
    exit_price: f64,
) -> qs_backtest::BacktestResult {
    let mut feed = VecFeed::new(vec![
        MarketEvent::Tick {
            symbol: symbol.into(),
            ts: ts(0),
            bid: entry_price,
            ask: entry_price,
        },
        MarketEvent::Tick {
            symbol: symbol.into(),
            ts: ts(1_000),
            bid: exit_price,
            ask: exit_price,
        },
    ]);
    let signals = vec![
        RawSignal::Entry {
            ts: ts(0),
            symbol: symbol.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some("economic-guard-baseline".into()),
        },
        RawSignal::Close {
            ts: ts(1_000),
            position: PositionRef::ByTradeId {
                trade_id: "economic-guard-baseline".into(),
            },
        },
    ];
    let config = qs_backtest::runner::BacktestConfig {
        close_on_finish: false,
        contract_sizes: HashMap::from([(symbol.into(), contract_multiplier as f64)]),
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(symbol.into(), spec(symbol, category, contract_multiplier))]),
        ..qs_backtest::runner::BacktestConfig::default()
    };

    BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .run_raw_signals_future(&mut feed, signals, None)
}

#[test]
fn supported_legacy_fx_and_cfd_pnl_baselines_remain_exact() {
    let cases = [
        ("EURUSD", "forex", 100_000, 1.1000, 1.1010, 100.0),
        ("XAUUSD", "metal", 100, 2_000.0, 2_001.0, 100.0),
        ("XTIUSD", "commodity", 100, 70.0, 71.0, 100.0),
        ("US100", "index", 1, 20_000.0, 20_100.0, 100.0),
    ];

    for (symbol, category, multiplier, entry, exit, expected_pnl) in cases {
        let result = run_linear_round_trip(symbol, category, multiplier, entry, exit);
        assert_eq!(result.total_trades, 1, "{symbol}");
        assert!((result.total_pnl - expected_pnl).abs() < 1.0e-9, "{symbol}");
        let tags = &result.execution_metadata.as_ref().unwrap().tags;
        let expected_multiplier = multiplier.to_string();
        assert_eq!(
            tags.get(&format!("economics.symbol.{symbol}.status"))
                .map(String::as_str),
            Some("supported")
        );
        assert_eq!(
            tags.get(&format!("economics.symbol.{symbol}.contract_multiplier"))
                .map(String::as_str),
            Some(expected_multiplier.as_str())
        );
    }
}

#[test]
fn crypto_spec_is_rejected_before_feed_consumption_and_cannot_produce_pnl() {
    let symbol = "BTCUSD";
    let config = qs_backtest::runner::BacktestConfig {
        contract_sizes: HashMap::from([(symbol.into(), 100_000_000.0)]),
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(symbol.into(), spec(symbol, "crypto", 100_000_000))]),
        ..qs_backtest::runner::BacktestConfig::default()
    };
    let mut feed = VecFeed::new(vec![MarketEvent::Tick {
        symbol: symbol.into(),
        ts: ts(0),
        bid: 50_000.0,
        ask: 50_000.0,
    }]);

    let result = BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .run_raw_signals_future(&mut feed, Vec::new(), None);

    assert_eq!(feed.remaining(), 1);
    assert_eq!(result.total_trades, 0);
    assert_eq!(result.total_pnl, 0.0);
    let reason = result.action_dispositions[0]
        .reason
        .as_deref()
        .expect("configuration rejection reason");
    assert!(reason.contains("unsupported_economic_model"));
    let tags = &result.execution_metadata.as_ref().unwrap().tags;
    assert_eq!(
        tags.get("economics.symbol.BTCUSD.status")
            .map(String::as_str),
        Some("unsupported")
    );
}
