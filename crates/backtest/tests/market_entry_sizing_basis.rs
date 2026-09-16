use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::{
    BacktestResult, BacktestRunner, ConversionRoute, EntryGeometryPolicy, FutureQuoteConfig,
    ManagementProfile, MarketEntrySizingBasis, MarketEvent, RawSignal, RunCurrencyPlan,
    StoplossMode, VecFeed,
};
use qs_core::{OrderType, Side, SizingPolicy};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "TEST";

fn ts(milliseconds: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::milliseconds(milliseconds)
}

fn symbol_spec() -> SymbolSpec {
    SymbolSpec {
        canonical: SYMBOL.to_ascii_lowercase(),
        pip_position: 2,
        digits: 2,
        category: "index".into(),
        lot_base_units: 1,
        lot_step_units: 1,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn currency_plan() -> RunCurrencyPlan {
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
        Vec::new(),
    )
    .unwrap()
}

fn signal(price: Option<f64>, stoploss: Option<f64>, trade_id: &str) -> RawSignal {
    RawSignal::Entry {
        ts: ts(0),
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price,
        risk_multiplier: 1.0,
        stoploss,
        targets: Vec::new(),
        group: None,
        trade_id: Some(trade_id.into()),
    }
}

fn run(
    policy: SizingPolicy,
    basis: MarketEntrySizingBasis,
    signal: RawSignal,
    profile: Option<&ManagementProfile>,
) -> BacktestResult {
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(policy),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), symbol_spec())]),
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        market_entry_sizing_basis: basis,
        ..FutureQuoteConfig::default()
    };
    let mut feed = VecFeed::new(vec![MarketEvent::Tick {
        symbol: SYMBOL.into(),
        ts: ts(0),
        bid: 110.0,
        ask: 110.0,
    }]);

    BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![signal],
        profile,
    )
}

fn audit(result: &BacktestResult) -> &qs_backtest::MarketEntrySizingAudit {
    let metadata = result.execution_metadata.as_ref().unwrap();
    assert_eq!(metadata.market_entry_sizing.len(), 1);
    &metadata.market_entry_sizing[0]
}

#[test]
fn fill_price_is_the_compatible_serializable_default() {
    assert_eq!(
        FutureQuoteConfig::default().market_entry_sizing_basis,
        MarketEntrySizingBasis::FillPrice
    );
    assert_eq!(
        serde_json::to_string(&MarketEntrySizingBasis::FillPrice).unwrap(),
        r#""fill_price""#
    );
    assert_eq!(
        serde_json::from_str::<MarketEntrySizingBasis>(r#""signal_entry_price""#).unwrap(),
        MarketEntrySizingBasis::SignalEntryPrice
    );

    let result = run(
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
        MarketEntrySizingBasis::FillPrice,
        signal(Some(100.0), Some(90.0), "fill-default"),
        None,
    );
    let entry = audit(&result);
    assert_eq!(entry.configured_basis, MarketEntrySizingBasis::FillPrice);
    assert_eq!(entry.applied_basis, MarketEntrySizingBasis::FillPrice);
    assert!(!entry.fallback_to_fill);
    assert_eq!(entry.original_signal_price, Some(100.0));
    assert_eq!(entry.sizing_reference_price, 110.0);
    assert_eq!(entry.final_lot, 5.0);
}

#[test]
fn signal_price_changes_monetary_lot_but_actual_risk_stays_fill_based() {
    let fill = run(
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
        MarketEntrySizingBasis::FillPrice,
        signal(Some(100.0), Some(90.0), "fill-risk"),
        None,
    );
    let selected = run(
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
        MarketEntrySizingBasis::SignalEntryPrice,
        signal(Some(100.0), Some(90.0), "signal-risk"),
        None,
    );

    assert_eq!(fill.recorded_fills[0].size, 5.0);
    assert_eq!(selected.recorded_fills[0].size, 10.0);
    let entry = audit(&selected);
    assert_eq!(
        entry.applied_basis,
        MarketEntrySizingBasis::SignalEntryPrice
    );
    assert!(!entry.fallback_to_fill);
    assert_eq!(entry.sizing_reference_price, 100.0);
    assert_eq!(entry.execution_price, 110.0);
    assert_eq!(entry.protective_stop, Some(90.0));
    assert_eq!(entry.requested_account_risk, Some(100.0));
    assert_eq!(entry.native_loss_per_lot, Some(10.0));
    assert_eq!(entry.account_loss_per_lot, Some(10.0));
    assert_eq!(entry.trade_id.as_deref(), Some("signal-risk"));
    assert_eq!(entry.action_id, "signal:00000000");

    let tranche = &selected.completed_positions[0].risk_tranches[0];
    assert_eq!(tranche.entry_price, 110.0);
    assert_eq!(tranche.initial_stop, Some(90.0));
    assert_eq!(tranche.risk_per_unit, Some(20.0));
    assert_eq!(tranche.risk_amount, Some(200.0));
}

#[test]
fn missing_signal_price_falls_back_and_fixed_lot_is_unchanged() {
    let fallback = run(
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
        MarketEntrySizingBasis::SignalEntryPrice,
        signal(None, Some(90.0), "fallback"),
        None,
    );
    let fallback_audit = audit(&fallback);
    assert_eq!(fallback.recorded_fills[0].size, 5.0);
    assert_eq!(
        fallback_audit.configured_basis,
        MarketEntrySizingBasis::SignalEntryPrice
    );
    assert_eq!(
        fallback_audit.applied_basis,
        MarketEntrySizingBasis::FillPrice
    );
    assert!(fallback_audit.fallback_to_fill);
    assert_eq!(fallback_audit.original_signal_price, None);
    assert_eq!(fallback_audit.sizing_reference_price, 110.0);

    for basis in [
        MarketEntrySizingBasis::FillPrice,
        MarketEntrySizingBasis::SignalEntryPrice,
    ] {
        let fixed = run(
            SizingPolicy::FixedLot { lots: 7.0 },
            basis,
            signal(Some(100.0), Some(90.0), "fixed"),
            None,
        );
        assert_eq!(fixed.recorded_fills[0].size, 7.0);
        assert_eq!(audit(&fixed).final_lot, 7.0);
    }
}

#[test]
fn signal_price_sizing_preserves_sell_direction() {
    let sell_signal = RawSignal::Entry {
        ts: ts(0),
        symbol: SYMBOL.into(),
        side: Side::Sell,
        order_type: OrderType::Market,
        price: Some(100.0),
        risk_multiplier: 1.0,
        stoploss: Some(110.0),
        targets: Vec::new(),
        group: None,
        trade_id: Some("sell-signal".into()),
    };
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(SizingPolicy::FixedRiskAmount { amount: 100.0 }),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), symbol_spec())]),
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        market_entry_sizing_basis: MarketEntrySizingBasis::SignalEntryPrice,
        ..FutureQuoteConfig::default()
    };
    let mut feed = VecFeed::new(vec![MarketEvent::Tick {
        symbol: SYMBOL.into(),
        ts: ts(0),
        bid: 90.0,
        ask: 90.0,
    }]);

    let result = BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![sell_signal],
        None,
    );

    assert_eq!(result.recorded_fills[0].fill.price, 90.0);
    assert_eq!(result.recorded_fills[0].size, 10.0);
    let entry = audit(&result);
    assert_eq!(entry.sizing_reference_price, 100.0);
    assert_eq!(entry.execution_price, 90.0);
    assert_eq!(entry.native_loss_per_lot, Some(10.0));
    assert_eq!(
        result.completed_positions[0].risk_tranches[0].risk_amount,
        Some(200.0)
    );
}

#[test]
fn fixed_distance_profile_uses_fill_while_sizing_uses_signal_price() {
    let profile = ManagementProfile {
        name: "fixed-distance".into(),
        target_selection: None,
        use_targets: Vec::new(),
        close_ratios: Vec::new(),
        stoploss_mode: StoplossMode::FixedDistance { distance: 10.0 },
        rules: Vec::new(),
        group_override: None,
        let_remainder_run: false,
        entry_geometry: EntryGeometryPolicy::Strict,
    };
    let result = run(
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
        MarketEntrySizingBasis::SignalEntryPrice,
        signal(Some(120.0), None, "profile"),
        Some(&profile),
    );

    assert_eq!(result.recorded_fills[0].fill.price, 110.0);
    assert_eq!(result.recorded_fills[0].size, 5.0);
    let entry = audit(&result);
    assert_eq!(entry.original_signal_price, Some(120.0));
    assert_eq!(entry.sizing_reference_price, 120.0);
    assert_eq!(entry.execution_price, 110.0);
    assert_eq!(entry.protective_stop, Some(100.0));
    assert_eq!(entry.native_loss_per_lot, Some(20.0));
    assert_eq!(entry.account_loss_per_lot, Some(20.0));
    assert_eq!(result.completed_positions[0].initial_stop, Some(100.0));
}
