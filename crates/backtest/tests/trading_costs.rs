use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Weekday};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::{
    BacktestResult, BacktestRunner, CommissionModel, ConversionRoute, CostKind, FutureQuoteConfig,
    InstrumentCosts, MarketEvent, RawSignal, RunCurrencyPlan, SwapAmount, SwapSchedule, VecFeed,
};
use qs_core::{OrderType, PositionRef, Side, SizingPolicy};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "EURUSD";

fn ts(minutes: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 6, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::minutes(minutes)
}

fn at(day: u32, hour: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 6, day)
        .unwrap()
        .and_hms_opt(hour, 0, 0)
        .unwrap()
}

fn symbol_spec() -> SymbolSpec {
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

fn per_lot_commission(amount: f64) -> InstrumentCosts {
    InstrumentCosts {
        commission: Some(CommissionModel::PerLotPerSide {
            amount,
            currency: "USD".into(),
        }),
        swap: None,
    }
}

fn notional_commission(buy_rate: f64, sell_rate: f64) -> InstrumentCosts {
    InstrumentCosts {
        commission: Some(CommissionModel::NotionalRatePerSide {
            buy_rate,
            sell_rate,
        }),
        swap: None,
    }
}

fn point_swap(long: f64, short: f64) -> InstrumentCosts {
    InstrumentCosts {
        commission: None,
        swap: Some(SwapSchedule {
            amount: SwapAmount::Points { long, short },
            rollover: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
            triple_weekday: Weekday::Wed,
            skipped_weekdays: vec![Weekday::Sat, Weekday::Sun],
        }),
    }
}

fn entry(entry_ts: NaiveDateTime) -> RawSignal {
    RawSignal::Entry {
        ts: entry_ts,
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(1.0900),
        targets: Vec::new(),
        group: None,
        trade_id: Some("t1".into()),
        entry_class: None,
    }
}

fn close(close_ts: NaiveDateTime) -> RawSignal {
    RawSignal::Close {
        ts: close_ts,
        position: PositionRef::AllOnSymbol {
            symbol: SYMBOL.into(),
        },
    }
}

fn tick(quote_ts: NaiveDateTime, price: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: SYMBOL.into(),
        ts: quote_ts,
        bid: price,
        ask: price,
    }
}

fn run_with(costs: HashMap<String, InstrumentCosts>, events: Vec<MarketEvent>) -> BacktestResult {
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), symbol_spec())]),
        contract_sizes: HashMap::from([(SYMBOL.to_owned(), 100_000.0)]),
        costs,
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        ..FutureQuoteConfig::default()
    };
    let mut feed = VecFeed::new(events);
    BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![entry(ts(0)), close(ts(2))],
        None,
    )
}

/// A run whose position closes half at a target and the rest on a later close signal.
fn partial_then_full_close_run(costs: HashMap<String, InstrumentCosts>) -> BacktestResult {
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), symbol_spec())]),
        contract_sizes: HashMap::from([(SYMBOL.to_owned(), 100_000.0)]),
        costs,
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        ..FutureQuoteConfig::default()
    };
    let mut entry_signal = entry(ts(0));
    if let RawSignal::Entry { targets, .. } = &mut entry_signal {
        // Two targets split the position, so the second close is the final one.
        targets.extend([1.1005, 1.1010]);
    }
    let mut feed = VecFeed::new(vec![
        tick(ts(0), 1.1000),
        tick(ts(1), 1.1005),
        tick(ts(2), 1.1010),
        tick(ts(3), 1.1010),
    ]);
    BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![entry_signal],
        None,
    )
}

fn flat_run(costs: HashMap<String, InstrumentCosts>) -> BacktestResult {
    run_with(
        costs,
        vec![
            tick(ts(0), 1.1000),
            tick(ts(1), 1.1000),
            tick(ts(2), 1.1010),
            tick(ts(3), 1.1010),
        ],
    )
}

#[test]
fn a_run_without_costs_is_unchanged() {
    let result = flat_run(HashMap::new());
    assert!(result.cost_events.is_empty());
    assert_eq!(result.total_commission, 0.0);
    assert_eq!(result.total_swap, 0.0);
    assert_eq!(result.gross_pnl, None);
    let position = &result.completed_positions[0];
    assert_eq!(position.commission_total, 0.0);
    assert_eq!(position.swap_total, 0.0);
    assert_eq!(position.gross_pnl, None);
    assert!((position.net_pnl - 100.0).abs() < 1.0e-6);
    assert!((result.final_balance - (result.initial_balance + 100.0)).abs() < 1.0e-6);
}

#[test]
fn per_lot_commission_is_charged_on_both_sides() {
    let costs = HashMap::from([(SYMBOL.to_owned(), per_lot_commission(3.5))]);
    let result = flat_run(costs);

    let kinds: Vec<CostKind> = result.cost_events.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        vec![CostKind::EntryCommission, CostKind::ExitCommission]
    );
    for event in &result.cost_events {
        assert_eq!(event.amount, 3.5);
        assert_eq!(event.native_amount, None);
        assert_eq!(event.size, 1.0);
        assert_eq!(event.nights, None);
    }

    assert_eq!(result.total_commission, 7.0);
    assert_eq!(result.total_swap, 0.0);

    let position = &result.completed_positions[0];
    assert_eq!(position.commission_total, 7.0);
    assert!((position.gross_pnl.unwrap() - 100.0).abs() < 1.0e-6);
    assert!((position.net_pnl - 93.0).abs() < 1.0e-6);
    assert!((result.total_pnl - 93.0).abs() < 1.0e-6);
    assert!((result.final_balance - (result.initial_balance + 93.0)).abs() < 1.0e-6);
    assert!((result.gross_pnl.unwrap() - 100.0).abs() < 1.0e-6);
}

#[test]
fn realized_r_uses_the_net_result() {
    let with_costs = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    let without_costs = flat_run(HashMap::new());

    let net = with_costs.completed_positions[0].realized_r.unwrap();
    let gross = without_costs.completed_positions[0].realized_r.unwrap();
    assert!(net < gross);

    let position = &with_costs.completed_positions[0];
    let risk = position.initial_risk().unwrap();
    assert!((net - position.net_pnl / risk).abs() < 1.0e-9);
}

#[test]
fn notional_commission_charges_the_side_specific_rate() {
    let costs = HashMap::from([(SYMBOL.to_owned(), notional_commission(0.005, 0.002))]);
    let result = flat_run(costs);

    let entry_cost = &result.cost_events[0];
    let exit_cost = &result.cost_events[1];
    // A long campaign pays the buy rate to open and the sell rate to close.
    assert!((entry_cost.amount - 1.1000 * 100_000.0 * 0.005).abs() < 1.0e-6);
    assert!((exit_cost.amount - 1.1010 * 100_000.0 * 0.002).abs() < 1.0e-6);
    assert_eq!(entry_cost.native_currency.as_deref(), Some("USD"));
    assert!(entry_cost.native_amount.is_some());
}

#[test]
fn point_swap_is_charged_once_per_night_held() {
    let events = vec![
        tick(at(1, 12), 1.1000),
        tick(at(2, 12), 1.1000),
        tick(at(3, 12), 1.1010),
        tick(at(3, 13), 1.1010),
    ];
    let config_costs = HashMap::from([(SYMBOL.to_owned(), point_swap(-6.1, 1.9))]);
    let result = run_signal_window(config_costs, events, at(1, 12), at(3, 12));

    let swaps: Vec<_> = result
        .cost_events
        .iter()
        .filter(|event| event.kind == CostKind::Swap)
        .collect();
    // Rollovers at 22:00 on the first and second day are crossed while the position is open.
    assert_eq!(swaps.len(), 2);
    for swap in &swaps {
        assert_eq!(swap.nights, Some(1));
        assert!((swap.amount - 6.1).abs() < 1.0e-6);
        assert!((swap.native_amount.unwrap() - 6.1).abs() < 1.0e-6);
    }
    assert!((result.total_swap - 12.2).abs() < 1.0e-6);

    let position = &result.completed_positions[0];
    assert!((position.swap_total - 12.2).abs() < 1.0e-6);
    assert!((position.gross_pnl.unwrap() - position.net_pnl - 12.2).abs() < 1.0e-6);
}

#[test]
fn the_triple_weekday_charges_three_nights() {
    // 2026-06-03 is a Wednesday, so its rollover charges three nights at once.
    let events = vec![
        tick(at(3, 12), 1.1000),
        tick(at(4, 12), 1.1000),
        tick(at(4, 13), 1.1000),
    ];
    let costs = HashMap::from([(SYMBOL.to_owned(), point_swap(-6.1, 1.9))]);
    let result = run_signal_window(costs, events, at(3, 12), at(4, 12));

    let swaps: Vec<_> = result
        .cost_events
        .iter()
        .filter(|event| event.kind == CostKind::Swap)
        .collect();
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].nights, Some(3));
    assert!((swaps[0].amount - 18.3).abs() < 1.0e-6);
}

#[test]
fn a_weekend_gap_skips_saturday_and_sunday() {
    // 2026-06-05 is a Friday and 2026-06-08 is the following Monday.
    let events = vec![
        tick(at(5, 12), 1.1000),
        tick(at(8, 12), 1.1000),
        tick(at(8, 13), 1.1000),
    ];
    let costs = HashMap::from([(SYMBOL.to_owned(), point_swap(-6.1, 1.9))]);
    let result = run_signal_window(costs, events, at(5, 12), at(8, 12));

    let swaps: Vec<_> = result
        .cost_events
        .iter()
        .filter(|event| event.kind == CostKind::Swap)
        .collect();
    // Friday and Sunday rollovers exist in the gap, but only Friday charges a night.
    assert_eq!(swaps.len(), 1);
    assert_eq!(swaps[0].nights, Some(1));
}

#[test]
fn a_position_closed_before_any_rollover_pays_no_swap() {
    let costs = HashMap::from([(SYMBOL.to_owned(), point_swap(-6.1, 1.9))]);
    let result = flat_run(costs);
    assert!(
        result
            .cost_events
            .iter()
            .all(|event| event.kind != CostKind::Swap)
    );
    assert_eq!(result.total_swap, 0.0);
}

fn configuration_error(result: &BacktestResult) -> String {
    result
        .execution_metadata
        .as_ref()
        .and_then(|metadata| metadata.tags.get("configuration_error"))
        .cloned()
        .unwrap_or_default()
}

#[test]
fn cost_configuration_is_validated_before_a_run() {
    let invalid = InstrumentCosts {
        commission: Some(CommissionModel::NotionalRatePerSide {
            buy_rate: 0.9,
            sell_rate: 0.0,
        }),
        swap: None,
    };
    let result = flat_run(HashMap::from([(SYMBOL.to_owned(), invalid)]));
    assert!(configuration_error(&result).contains("commission rate"));
    assert!(result.cost_events.is_empty());
}

#[test]
fn point_swap_requires_a_symbol_specification() {
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        contract_sizes: HashMap::from([(SYMBOL.to_owned(), 100_000.0)]),
        costs: HashMap::from([(SYMBOL.to_owned(), point_swap(-6.1, 1.9))]),
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        ..FutureQuoteConfig::default()
    };
    let mut feed = VecFeed::new(vec![tick(ts(0), 1.1000)]);
    let result = BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![entry(ts(0))],
        None,
    );
    assert!(configuration_error(&result).contains("requires a symbol specification"));
}

#[test]
fn declared_cost_currency_must_match_the_account() {
    let mismatched = InstrumentCosts {
        commission: Some(CommissionModel::PerLotPerSide {
            amount: 3.5,
            currency: "EUR".into(),
        }),
        swap: None,
    };
    let result = flat_run(HashMap::from([(SYMBOL.to_owned(), mismatched)]));
    assert!(configuration_error(&result).contains("does not match account currency"));
}

#[test]
fn artifacts_round_trip_with_cost_fields() {
    let result = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    let position = &result.completed_positions[0];
    let encoded = serde_json::to_string(position).unwrap();
    let decoded: qs_backtest::CompletedPosition = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.commission_total, position.commission_total);
    assert_eq!(decoded.swap_total, position.swap_total);
    assert!(decoded.gross_pnl.is_some());
    assert!((decoded.gross_pnl.unwrap() - position.gross_pnl.unwrap()).abs() < 1.0e-9);
    assert!((decoded.net_pnl - position.net_pnl).abs() < 1.0e-9);

    let event = &result.cost_events[0];
    let encoded = serde_json::to_string(event).unwrap();
    let decoded: qs_backtest::CostEvent = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.kind, event.kind);
    assert_eq!(decoded.id, event.id);
    assert_eq!(decoded.nights, event.nights);
    assert!((decoded.amount - event.amount).abs() < 1.0e-9);
}

#[test]
fn old_artifacts_without_cost_fields_still_decode() {
    let legacy = r#"{"format_version":1,"execution":{"run_id":null,"execution_model":{"convention":"FutureQuoteV1","fill_model":"BidAsk","slippage":"None"},"initial_balance":10000.0,"account_currency":null,"currency_plan":null,"contract_sizes":{},"instrument_manifest":null,"instrument_sizing":[],"market_entry_sizing_basis":"fill_price","market_entry_sizing":[],"entry_profile_default":null,"entry_profile_routes":{},"entry_profile_resolutions":[],"stale_quote_after_millis":null,"pnl_epsilon":1e-9,"tags":{}},"fills":[],"close_events":[],"completed_positions":[],"open_positions":[],"pending_orders":[],"pending_order_lifecycle":[],"lifecycle":[],"equity_curve":[],"mtm_output_summary":{"policy":"none","observed":0,"emitted":0,"truncated":false},"max_drawdown":null,"max_drawdown_pct":null}"#;
    let decoded: qs_backtest::FutureBacktestArtifacts = serde_json::from_str(legacy).unwrap();
    assert!(decoded.cost_events.is_empty());
    assert_eq!(decoded.execution.unconverted_cost_events, 0);
    assert!(decoded.execution.costs.is_empty());
}

fn run_signal_window(
    costs: HashMap<String, InstrumentCosts>,
    events: Vec<MarketEvent>,
    entry_ts: NaiveDateTime,
    close_ts: NaiveDateTime,
) -> BacktestResult {
    let config = BacktestConfig {
        close_on_finish: true,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), symbol_spec())]),
        contract_sizes: HashMap::from([(SYMBOL.to_owned(), 100_000.0)]),
        costs,
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(currency_plan()),
        ..FutureQuoteConfig::default()
    };
    let mut feed = VecFeed::new(events);
    BacktestRunner::new_future(config, future).run_raw_signals_future(
        &mut feed,
        vec![entry(entry_ts), close(close_ts)],
        None,
    )
}

#[test]
fn a_final_trade_row_settles_the_whole_position_cost() {
    let result = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    let trade = &result.trade_log[0];
    // The single close is also the final close, so it settles entry and exit commission together.
    assert_eq!(trade.commission, 7.0);
    assert_eq!(trade.swap, 0.0);
    assert!((trade.gross_pnl.unwrap() - (trade.pnl + 7.0)).abs() < 1.0e-9);

    let position = &result.completed_positions[0];
    let settled: f64 = result
        .trade_log
        .iter()
        .map(|trade| trade.commission + trade.swap)
        .sum();
    assert!((settled - (position.commission_total + position.swap_total)).abs() < 1.0e-9);
}

#[test]
fn trade_rows_without_costs_report_nothing() {
    let result = flat_run(HashMap::new());
    let trade = &result.trade_log[0];
    assert_eq!(trade.commission, 0.0);
    assert_eq!(trade.swap, 0.0);
    assert_eq!(trade.gross_pnl, None);
    assert_eq!(result.summary.commission, 0.0);
    assert_eq!(result.summary.swap, 0.0);
    assert_eq!(result.summary.gross_pnl, None);
}

#[test]
fn subset_statistics_report_settled_costs() {
    let result = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    let summary = &result.summary;
    assert_eq!(summary.commission, 7.0);
    assert_eq!(summary.swap, 0.0);
    assert!((summary.gross_pnl.unwrap() - (summary.total_pnl + 7.0)).abs() < 1.0e-9);

    let per_symbol = &result.per_symbol[SYMBOL];
    assert_eq!(per_symbol.commission, 7.0);
}

#[test]
fn headline_statistics_use_the_same_net_basis_as_the_run_total() {
    let result = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    // Every derived figure must sit on the fully net basis, not on a partly costed one.
    assert!((result.summary.total_pnl - result.total_pnl).abs() < 1.0e-9);
    let (_, last_equity) = *result.equity_curve.last().unwrap();
    assert!((last_equity - result.final_balance).abs() < 1.0e-9);
    let position_net: f64 = result
        .completed_positions
        .iter()
        .map(|position| position.net_pnl)
        .sum();
    assert!((result.summary.total_pnl - position_net).abs() < 1.0e-9);
}

#[test]
fn a_partial_close_keeps_position_costs_on_the_final_row() {
    let costs = HashMap::from([(SYMBOL.to_owned(), per_lot_commission(3.5))]);
    let result = partial_then_full_close_run(costs);
    assert_eq!(result.trade_log.len(), 2);

    // The first row carries only its own exit commission; the final row also settles the entry commission.
    assert_eq!(result.trade_log[0].swap, 0.0);
    assert!(result.trade_log[0].commission < result.trade_log[1].commission);

    let position = &result.completed_positions[0];
    let settled: f64 = result
        .trade_log
        .iter()
        .map(|trade| trade.commission + trade.swap)
        .sum();
    assert!((settled - position.commission_total).abs() < 1.0e-9);
    let rows_net: f64 = result.trade_log.iter().map(|trade| trade.pnl).sum();
    assert!((rows_net - position.net_pnl).abs() < 1.0e-9);
}

#[test]
fn provider_evaluation_reports_a_cost_section() {
    let result = flat_run(HashMap::from([(
        SYMBOL.to_owned(),
        per_lot_commission(3.5),
    )]));
    let costs = result
        .provider_evaluation
        .as_ref()
        .unwrap()
        .costs
        .as_ref()
        .unwrap();

    assert_eq!(costs.positions_with_costs, 1);
    assert_eq!(costs.total_commission, 7.0);
    assert_eq!(costs.total_swap, 0.0);
    assert_eq!(costs.total_cost, 7.0);
    assert!((costs.gross_outcome - 100.0).abs() < 1.0e-6);
    assert!((costs.net_outcome - 93.0).abs() < 1.0e-6);
    assert!((costs.cost_share_of_gross.value.unwrap() - 0.07).abs() < 1.0e-6);
    assert!((costs.mean_cost_per_position.value.unwrap() - 7.0).abs() < 1.0e-9);
}

#[test]
fn a_cost_free_run_reports_an_empty_cost_section() {
    let result = flat_run(HashMap::new());
    let costs = result
        .provider_evaluation
        .as_ref()
        .unwrap()
        .costs
        .as_ref()
        .unwrap();
    assert_eq!(costs.positions_with_costs, 0);
    assert_eq!(costs.total_cost, 0.0);
    assert!((costs.gross_outcome - costs.net_outcome).abs() < 1.0e-12);
    assert!(costs.mean_cost_per_position.value.is_none());
}
