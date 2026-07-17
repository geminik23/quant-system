use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::evaluation::LifecycleCounts;
use qs_backtest::ledger::ActionDispositionStatus;
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestResult, BacktestRunner, ConversionRoute, FutureQuoteConfig, ManagementProfile,
    MarketEvent, PendingOrderLifecycleState, PositionRef, RawSignal, RiskBasisStatus,
    RuleConfigDef, RunCurrencyPlan, StoplossMode, VecFeed,
};
use qs_core::types::{CloseReason, FillPurpose, OrderType, Side, StopOrigin};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "EURUSD";

fn ts(milliseconds: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::milliseconds(milliseconds)
}

fn quote(milliseconds: i64, bid: f64, ask: f64) -> MarketEvent {
    quote_for(SYMBOL, milliseconds, bid, ask)
}

fn quote_for(symbol: &str, milliseconds: i64, bid: f64, ask: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: symbol.into(),
        ts: ts(milliseconds),
        bid,
        ask,
    }
}

fn entry(milliseconds: i64, trade_id: &str, stoploss: Option<f64>, targets: Vec<f64>) -> RawSignal {
    RawSignal::Entry {
        ts: ts(milliseconds),
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss,
        targets,
        group: None,
        trade_id: Some(trade_id.into()),
    }
}

fn symbol_spec(symbol: &str) -> SymbolSpec {
    SymbolSpec {
        canonical: symbol.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn fixed_lot_config(close_on_finish: bool, lots: f64, symbols: &[&str]) -> BacktestConfig {
    BacktestConfig {
        close_on_finish,
        sizing: Some(SizingPolicy::FixedLot { lots }),
        symbol_specs: symbols
            .iter()
            .map(|symbol| ((*symbol).to_owned(), symbol_spec(symbol)))
            .collect(),
        ..BacktestConfig::default()
    }
}

fn usd_identity_plan(symbol: &str) -> RunCurrencyPlan {
    RunCurrencyPlan::new(
        "USD",
        BTreeSet::from([symbol.to_owned()]),
        BTreeSet::new(),
        BTreeMap::from([(symbol.to_owned(), "USD".to_owned())]),
        BTreeMap::from([(
            "USD".to_owned(),
            ConversionRoute::Identity {
                currency: "USD".to_owned(),
            },
        )]),
        vec![],
    )
    .unwrap()
}

fn run(events: Vec<MarketEvent>, signals: Vec<RawSignal>, close_on_finish: bool) -> BacktestResult {
    let symbols: Vec<_> = signals
        .iter()
        .filter_map(|signal| match signal {
            RawSignal::Entry { symbol, .. } => Some(symbol.as_str()),
            _ => None,
        })
        .collect();
    let config = fixed_lot_config(close_on_finish, 1.0, &symbols);
    run_with_config(events, signals, config, FutureQuoteConfig::default())
}

fn run_with_config(
    events: Vec<MarketEvent>,
    signals: Vec<RawSignal>,
    config: BacktestConfig,
    future: FutureQuoteConfig,
) -> BacktestResult {
    let runner = BacktestRunner::new_future(config, future);
    let mut feed = VecFeed::new(events);

    runner.run_raw_signals_future(&mut feed, signals, None)
}

fn close(milliseconds: i64, trade_id: &str) -> RawSignal {
    RawSignal::Close {
        ts: ts(milliseconds),
        position: PositionRef::ByTradeId {
            trade_id: trade_id.into(),
        },
    }
}

fn provider_lifecycle(result: &BacktestResult) -> LifecycleCounts {
    result
        .provider_evaluation
        .as_ref()
        .expect("FutureQuote report includes provider evaluation")
        .coverage
        .as_ref()
        .expect("coverage is requested by default")
        .lifecycle
        .expect("FutureQuote adapter supplies lifecycle counts")
}

fn assert_f64(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1.0e-12,
        "expected {expected}, got {actual}"
    );
}

fn assert_one_pending_terminal_per_placed_order(result: &BacktestResult) {
    let mut placed = HashMap::<String, usize>::new();
    let mut terminal = HashMap::<String, usize>::new();
    let mut event_ids = std::collections::HashSet::new();

    for (sequence, event) in result.pending_order_lifecycle.iter().enumerate() {
        assert_eq!(event.sequence, sequence as u64);
        assert!(event_ids.insert(event.id.clone()), "duplicate event id");
        let expected_kind = match event.state {
            PendingOrderLifecycleState::Placed => "pending_placed",
            PendingOrderLifecycleState::Filled => "pending_filled",
            PendingOrderLifecycleState::Cancelled => "pending_cancelled",
            PendingOrderLifecycleState::UnfilledAtEnd => "pending_unfilled_at_end",
        };
        assert_eq!(
            event.id,
            format!(
                "{}:{expected_kind}:{:08}",
                event.position_id, event.sequence
            )
        );
        if event.state == PendingOrderLifecycleState::Placed {
            *placed.entry(event.position_id.clone()).or_default() += 1;
        } else {
            assert!(event.state.is_terminal());
            *terminal.entry(event.position_id.clone()).or_default() += 1;
        }
    }

    assert_eq!(placed, terminal);
    assert!(placed.values().all(|count| *count == 1));
}

#[test]
fn market_entries_use_first_eligible_quote_and_report_terminal_lifecycle() {
    let result = run(
        vec![
            quote(0, 99.9, 100.0),
            quote(1_000, 100.9, 101.0),
            quote(2_000, 101.9, 102.0),
        ],
        vec![
            entry(500, "first-eligible", None, vec![]),
            entry(2_000, "same-timestamp", None, vec![]),
            entry(2_000, "first-eligible", None, vec![]),
            entry(3_000, "after-feed", None, vec![]),
        ],
        true,
    );

    let entry_fills: Vec<_> = result
        .recorded_fills
        .iter()
        .filter(|fill| fill.fill.purpose == FillPurpose::MarketEntry)
        .collect();
    assert_eq!(entry_fills.len(), 2);

    assert_eq!(entry_fills[0].signal_ts, Some(ts(500)));
    assert_eq!(entry_fills[0].effective_ts, ts(500));
    assert_eq!(entry_fills[0].quote_ts, ts(1_000));
    assert_f64(entry_fills[0].fill.price, 101.0);

    assert_eq!(entry_fills[1].signal_ts, Some(ts(2_000)));
    assert_eq!(entry_fills[1].effective_ts, ts(2_000));
    assert_eq!(entry_fills[1].quote_ts, ts(2_000));
    assert_f64(entry_fills[1].fill.price, 102.0);

    let duplicate = result
        .action_dispositions
        .iter()
        .find(|disposition| disposition.reason.as_deref() == Some("duplicate_trade_id"))
        .expect("duplicate trade id must have a terminal disposition");
    assert_eq!(duplicate.status, ActionDispositionStatus::Rejected);
    assert_eq!(duplicate.action_id, "signal:00000002");

    let after_feed = result
        .action_dispositions
        .iter()
        .find(|disposition| disposition.reason.as_deref() == Some("no_eligible_quote"))
        .expect("a post-feed signal must be rejected without reusing an old quote");
    assert_eq!(after_feed.status, ActionDispositionStatus::Rejected);
    assert_eq!(after_feed.action_id, "signal:00000003");
    assert_eq!(after_feed.effective_ts, Some(ts(3_000)));

    assert_eq!(result.completed_positions.len(), 2);
    assert_eq!(result.close_events.len(), 2);
    assert!(
        result
            .close_events
            .iter()
            .all(|event| event.reason == CloseReason::EndOfData)
    );
    assert!(result.completed_positions.iter().all(|position| {
        position.close_reasons == [CloseReason::EndOfData] && position.close_events.len() == 1
    }));
    assert!(result.open_position_snapshots.is_empty());

    let lifecycle = provider_lifecycle(&result);
    assert_eq!(lifecycle.candidates, 4);
    assert_eq!(lifecycle.accepted, 2);
    assert_eq!(lifecycle.rejected, 2);
    assert_eq!(lifecycle.opened, 2);
    assert_eq!(lifecycle.completed, 2);
    assert_eq!(lifecycle.filled, 0);
    assert_eq!(lifecycle.cancelled, 0);
    assert_eq!(lifecycle.unfilled_at_end, 0);
    assert_eq!(
        result
            .action_dispositions
            .iter()
            .filter(|disposition| disposition.action_kind.as_deref() == Some("end_of_data"))
            .count(),
        2,
        "synthetic EOD actions remain auditable but are not entry candidates"
    );
}

#[test]
fn equal_targets_aggregate_partial_take_profit_and_stoploss_into_one_campaign() {
    let result = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 101.0, 101.0),
            quote(2_000, 99.0, 99.0),
        ],
        vec![entry(0, "tp-then-sl", Some(99.0), vec![101.0, 102.0])],
        false,
    );

    assert_eq!(result.close_events.len(), 2);
    assert_eq!(result.close_events[0].reason, CloseReason::Target);
    assert_f64(result.close_events[0].size, 0.5);
    assert_f64(result.close_events[0].pnl, 0.5);
    assert_eq!(result.close_events[1].reason, CloseReason::Stoploss);
    assert_f64(result.close_events[1].size, 0.5);
    assert_f64(result.close_events[1].pnl, -0.5);

    assert_eq!(result.completed_positions.len(), 1);
    let campaign = &result.completed_positions[0];
    assert_eq!(campaign.trade_id.as_deref(), Some("tp-then-sl"));
    assert_f64(campaign.entry_size, 1.0);
    assert_f64(campaign.net_pnl, 0.0);
    assert_eq!(
        campaign.close_reasons,
        [CloseReason::Target, CloseReason::Stoploss]
    );
    assert_eq!(campaign.close_events.len(), 2);
    assert!(result.open_position_snapshots.is_empty());
}

#[test]
fn open_partial_campaign_is_excluded_from_completed_position_evaluation() {
    let result = run(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![entry(0, "still-open", Some(99.0), vec![101.0, 102.0])],
        false,
    );

    assert_eq!(result.close_events.len(), 1);
    assert_eq!(result.close_events[0].reason, CloseReason::Target);
    assert_f64(result.close_events[0].size, 0.5);
    assert!(result.completed_positions.is_empty());

    assert_eq!(result.open_position_snapshots.len(), 1);
    let open = &result.open_position_snapshots[0];
    assert_eq!(open.trade_id.as_deref(), Some("still-open"));
    assert_f64(open.remaining_size, 0.5);
    assert_f64(open.realized_pnl, 0.5);

    let evaluation = result
        .provider_evaluation
        .expect("FutureQuoteV1 results expose completed-position evaluation");
    assert_eq!(
        evaluation
            .position_performance
            .expect("position performance requested")
            .position_count,
        0
    );
}

#[test]
fn pre_quote_entry_can_be_closed_by_trade_id_in_the_same_stable_batch() {
    let result = run(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![
            entry(100, "same-batch-close", None, vec![]),
            close(200, "same-batch-close"),
        ],
        false,
    );

    assert_eq!(result.recorded_fills.len(), 2);
    assert_eq!(
        result.recorded_fills[0].fill.purpose,
        FillPurpose::MarketEntry
    );
    assert_eq!(
        result.recorded_fills[1].fill.purpose,
        FillPurpose::MarketExit
    );
    assert_eq!(result.recorded_fills[0].quote_ts, ts(1_000));
    assert_eq!(result.recorded_fills[1].quote_ts, ts(1_000));

    assert_eq!(result.completed_positions.len(), 1);
    assert_eq!(
        result.completed_positions[0].trade_id.as_deref(),
        Some("same-batch-close")
    );
    assert_eq!(
        result.completed_positions[0].close_reasons,
        [CloseReason::Manual]
    );
    assert!(result.open_position_snapshots.is_empty());
}

#[test]
fn pre_quote_bulk_close_membership_includes_an_earlier_same_batch_entry() {
    let result = run(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![
            entry(100, "same-batch-bulk", None, vec![]),
            RawSignal::CloseAllOf {
                ts: ts(200),
                symbol: SYMBOL.into(),
            },
        ],
        false,
    );

    assert_eq!(result.completed_positions.len(), 1);
    assert_eq!(
        result.completed_positions[0].trade_id.as_deref(),
        Some("same-batch-bulk")
    );
    assert_eq!(
        result.completed_positions[0].close_reasons,
        [CloseReason::Manual]
    );
    assert_eq!(result.close_events.len(), 1);
    assert!(result.open_position_snapshots.is_empty());
}

#[test]
fn between_tick_manual_close_includes_the_closing_quote_in_excursions() {
    let result = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(400, 110.0, 110.0),
            quote(1_000, 90.0, 90.0),
        ],
        vec![
            entry(0, "closing-excursion", None, vec![]),
            close(500, "closing-excursion"),
        ],
        false,
    );

    assert_eq!(result.completed_positions.len(), 1);
    let completed = &result.completed_positions[0];
    assert_eq!(completed.close_reasons, [CloseReason::Manual]);
    assert_eq!(completed.mae, Some(-10.0));
    assert_eq!(completed.mfe, Some(10.0));
    assert_f64(completed.net_pnl, -10.0);
}

#[test]
fn pending_order_lifecycle_records_place_then_fill() {
    let result = run(
        vec![quote(0, 99.9, 100.0), quote(1_000, 98.9, 99.0)],
        vec![RawSignal::Entry {
            ts: ts(0),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(99.0),
            risk_multiplier: 2.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("pending-fill".into()),
        }],
        false,
    );

    assert_one_pending_terminal_per_placed_order(&result);
    assert_eq!(result.pending_order_lifecycle.len(), 2);
    let placed = &result.pending_order_lifecycle[0];
    assert_eq!(placed.state, PendingOrderLifecycleState::Placed);
    assert_eq!(placed.requested_size, 2.0);
    assert_eq!(placed.requested_price, Some(99.0));
    assert_eq!(placed.filled_size, None);
    assert_eq!(placed.fill_price, None);
    assert_eq!(placed.signal_ts, Some(ts(0)));
    assert_eq!(placed.effective_ts, Some(ts(0)));
    assert_eq!(placed.placed_ts, Some(ts(0)));
    assert_eq!(placed.terminal_ts, None);
    assert_eq!(placed.wait_latency_ms, None);
    assert_eq!(placed.fill_ratio, None);

    let filled = &result.pending_order_lifecycle[1];
    assert_eq!(filled.position_id, placed.position_id);
    assert_eq!(filled.state, PendingOrderLifecycleState::Filled);
    assert_eq!(filled.requested_size, 2.0);
    assert_eq!(filled.filled_size, Some(2.0));
    assert_eq!(filled.requested_price, Some(99.0));
    assert_eq!(filled.fill_price, Some(99.0));
    assert_eq!(filled.placed_ts, Some(ts(0)));
    assert_eq!(filled.effective_ts, Some(ts(0)));
    assert_eq!(filled.terminal_ts, Some(ts(1_000)));
    assert_eq!(filled.wait_latency_ms, Some(1_000));
    assert_eq!(filled.fill_ratio, Some(1.0));
    assert!(result.pending_order_snapshots.is_empty());

    let lifecycle = provider_lifecycle(&result);
    assert_eq!(lifecycle.candidates, 1);
    assert_eq!(lifecycle.accepted, 1);
    assert_eq!(lifecycle.rejected, 0);
    assert_eq!(lifecycle.opened, 1);
    assert_eq!(lifecycle.filled, 1);
    assert_eq!(lifecycle.cancelled, 0);
    assert_eq!(lifecycle.unfilled_at_end, 0);
}

#[test]
fn pending_order_lifecycle_records_place_then_cancel() {
    let result = run(
        vec![quote(0, 99.9, 100.0), quote(1_000, 99.9, 100.0)],
        vec![
            RawSignal::Entry {
                ts: ts(0),
                symbol: SYMBOL.into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(90.0),
                risk_multiplier: 1.5,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("pending-cancel".into()),
            },
            RawSignal::CancelPending {
                ts: ts(500),
                position: PositionRef::ByTradeId {
                    trade_id: "pending-cancel".into(),
                },
            },
        ],
        true,
    );

    assert_one_pending_terminal_per_placed_order(&result);
    assert_eq!(result.pending_order_lifecycle.len(), 2);
    let cancelled = &result.pending_order_lifecycle[1];
    assert_eq!(cancelled.state, PendingOrderLifecycleState::Cancelled);
    assert_eq!(cancelled.requested_size, 1.5);
    assert_eq!(cancelled.filled_size, Some(0.0));
    assert_eq!(cancelled.requested_price, Some(90.0));
    assert_eq!(cancelled.fill_price, None);
    assert_eq!(cancelled.terminal_ts, Some(ts(1_000)));
    assert_eq!(cancelled.wait_latency_ms, Some(1_000));
    assert_eq!(cancelled.fill_ratio, Some(0.0));
    assert!(cancelled.terminal_action_id.is_some());
    assert!(result.pending_order_snapshots.is_empty());
    assert!(result.recorded_fills.is_empty());

    let lifecycle = provider_lifecycle(&result);
    assert_eq!(lifecycle.candidates, 1);
    assert_eq!(lifecycle.accepted, 1);
    assert_eq!(lifecycle.rejected, 0);
    assert_eq!(lifecycle.opened, 0);
    assert_eq!(lifecycle.filled, 0);
    assert_eq!(lifecycle.cancelled, 1);
    assert_eq!(lifecycle.unfilled_at_end, 0);
    assert_eq!(
        result
            .action_dispositions
            .iter()
            .filter(|disposition| disposition.action_kind.as_deref() == Some("cancel_pending"))
            .count(),
        1,
        "management actions remain auditable but are not entry candidates"
    );
}

#[test]
fn pending_order_lifecycle_marks_unfilled_at_end_without_synthetic_fill_or_cancel() {
    let result = run(
        vec![quote(0, 99.9, 100.0), quote(1_000, 99.9, 100.0)],
        vec![RawSignal::Entry {
            ts: ts(0),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(90.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("pending-at-end".into()),
        }],
        true,
    );

    assert_one_pending_terminal_per_placed_order(&result);
    assert_eq!(result.pending_order_lifecycle.len(), 2);
    let unfilled = &result.pending_order_lifecycle[1];
    assert_eq!(unfilled.state, PendingOrderLifecycleState::UnfilledAtEnd);
    assert_eq!(unfilled.filled_size, Some(0.0));
    assert_eq!(unfilled.fill_price, None);
    assert_eq!(unfilled.terminal_ts, Some(ts(1_000)));
    assert_eq!(unfilled.wait_latency_ms, Some(1_000));
    assert_eq!(unfilled.fill_ratio, Some(0.0));
    assert_eq!(unfilled.terminal_action_id, None);
    assert_eq!(result.pending_order_snapshots.len(), 1);
    assert_eq!(
        result.pending_order_snapshots[0].position_id,
        unfilled.position_id
    );
    assert!(result.recorded_fills.is_empty());
    assert!(result.close_events.is_empty());

    let lifecycle = provider_lifecycle(&result);
    assert_eq!(lifecycle.candidates, 1);
    assert_eq!(lifecycle.accepted, 1);
    assert_eq!(lifecycle.rejected, 0);
    assert_eq!(lifecycle.opened, 0);
    assert_eq!(lifecycle.filled, 0);
    assert_eq!(lifecycle.cancelled, 0);
    assert_eq!(lifecycle.unfilled_at_end, 1);
}

#[test]
fn pending_stop_gap_uses_one_carried_quote_fill() {
    let result = run(
        vec![quote(0, 99.9, 100.0), quote(1_000, 102.9, 103.0)],
        vec![RawSignal::Entry {
            ts: ts(0),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Stop,
            price: Some(101.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("gap-stop".into()),
        }],
        false,
    );

    assert_eq!(result.recorded_fills.len(), 1);
    let fill = &result.recorded_fills[0];
    assert_eq!(fill.fill.purpose, FillPurpose::StopEntry);
    assert_eq!(fill.quote_ts, ts(1_000));
    assert_f64(fill.ask, 103.0);
    assert_f64(fill.fill.quote_price, 103.0);
    assert_f64(fill.fill.price, 103.0);
    assert_eq!(fill.fill.requested_price, Some(101.0));

    assert_eq!(result.open_position_snapshots.len(), 1);
    assert_f64(
        result.open_position_snapshots[0].average_entry_price,
        fill.fill.price,
    );
    assert_f64(result.open_position_snapshots[0].remaining_size, fill.size);
}

#[test]
fn pending_limit_improvement_and_pending_stop_adverse_gap_use_carried_fills() {
    let result = run(
        vec![
            quote(0, 99.9, 100.0),
            quote(1_000, 98.9, 99.0),
            quote(2_000, 102.9, 103.0),
        ],
        vec![
            RawSignal::Entry {
                ts: ts(0),
                symbol: SYMBOL.into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(100.0),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("improved-limit".into()),
            },
            RawSignal::Entry {
                ts: ts(0),
                symbol: SYMBOL.into(),
                side: Side::Buy,
                order_type: OrderType::Stop,
                price: Some(101.0),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("gapped-stop".into()),
            },
        ],
        false,
    );

    assert_eq!(result.recorded_fills.len(), 2);
    let limit = result
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::LimitEntry)
        .expect("limit order must fill from the improved quote");
    assert_eq!(limit.quote_ts, ts(1_000));
    assert_eq!(limit.fill.requested_price, Some(100.0));
    assert_f64(limit.fill.quote_price, 99.0);
    assert_f64(limit.fill.price, 99.0);

    let stop = result
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::StopEntry)
        .expect("stop order must retain the adverse opening gap");
    assert_eq!(stop.quote_ts, ts(2_000));
    assert_eq!(stop.fill.requested_price, Some(101.0));
    assert_f64(stop.fill.quote_price, 103.0);
    assert_f64(stop.fill.price, 103.0);

    assert_eq!(result.open_position_snapshots.len(), 2);
    assert!(result.pending_order_snapshots.is_empty());
}

#[test]
fn automatic_exit_gaps_caps_and_equal_target_units_are_conserved() {
    let gapped = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 103.0, 103.0),
            quote(2_000, 90.0, 90.0),
        ],
        vec![
            entry(0, "tp-gap", None, vec![101.0]),
            entry(0, "sl-gap", Some(99.0), vec![]),
        ],
        false,
    );

    let favorable_tp = gapped
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::TakeProfit)
        .expect("take profit must fill through the favorable gap");
    assert_eq!(favorable_tp.quote_ts, ts(1_000));
    assert_eq!(favorable_tp.fill.requested_price, Some(101.0));
    assert_f64(favorable_tp.fill.quote_price, 103.0);
    assert_f64(favorable_tp.fill.price, 103.0);

    let adverse_sl = gapped
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::StopLoss)
        .expect("stop loss must fill through the adverse gap");
    assert_eq!(adverse_sl.quote_ts, ts(2_000));
    assert_eq!(adverse_sl.fill.requested_price, Some(99.0));
    assert_f64(adverse_sl.fill.quote_price, 90.0);
    assert_f64(adverse_sl.fill.price, 90.0);

    let capped = run_with_config(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![entry(0, "tp-cap", None, vec![101.0])],
        fixed_lot_config(false, 1.0, &[SYMBOL]),
        FutureQuoteConfig {
            slippage_pips: 10.0,
            ..FutureQuoteConfig::default()
        },
    );
    let capped_tp = capped
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::TakeProfit)
        .expect("take profit must execute at its target cap");
    assert_eq!(capped_tp.fill.requested_price, Some(101.0));
    assert_f64(capped_tp.fill.quote_price, 101.0);
    assert_f64(capped_tp.fill.price, 101.0);
    assert_f64(capped_tp.fill.slippage_pips, 10.0);

    let equal_targets = run_with_config(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 101.0, 101.0),
            quote(2_000, 102.0, 102.0),
        ],
        vec![entry(0, "all-equal-targets", None, vec![101.0, 102.0])],
        fixed_lot_config(false, 0.02, &[SYMBOL]),
        FutureQuoteConfig::default(),
    );
    assert_eq!(equal_targets.close_events.len(), 2);
    assert!(
        equal_targets
            .close_events
            .iter()
            .all(|event| event.reason == CloseReason::Target)
    );
    assert!(
        equal_targets
            .close_events
            .iter()
            .all(|event| (event.size - 0.01).abs() < 1.0e-12)
    );
    assert_f64(
        equal_targets
            .close_events
            .iter()
            .map(|event| event.size)
            .sum(),
        0.02,
    );
    assert_eq!(equal_targets.completed_positions.len(), 1);
    assert_f64(equal_targets.completed_positions[0].entry_size, 0.02);
    assert!(equal_targets.open_position_snapshots.is_empty());
}

#[test]
fn future_atomic_target_modification_retains_non_default_ratio() {
    let profile = ManagementProfile {
        name: "non-default-ratios".into(),
        target_selection: None,
        use_targets: vec![1, 2],
        close_ratios: vec![0.25, 0.75],
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };
    let signals = vec![
        entry(0, "modified-target", None, vec![101.0, 103.0]),
        RawSignal::ModifyTarget {
            ts: ts(500),
            position: PositionRef::ByTradeId {
                trade_id: "modified-target".into(),
            },
            old_price: 101.0,
            new_price: 102.0,
        },
    ];
    let runner = BacktestRunner::new_future(
        fixed_lot_config(false, 1.0, &[SYMBOL]),
        FutureQuoteConfig::default(),
    );
    let mut feed = VecFeed::new(vec![
        quote(0, 100.0, 100.0),
        quote(500, 100.5, 100.5),
        quote(1_000, 101.0, 101.0),
        quote(2_000, 102.0, 102.0),
        quote(3_000, 103.0, 103.0),
    ]);

    let result = runner.run_raw_signals_future(&mut feed, signals, Some(&profile));

    assert_eq!(result.close_events.len(), 2);
    assert_f64(result.close_events[0].price, 102.0);
    assert_f64(result.close_events[0].size, 0.25);
    assert_f64(result.close_events[1].price, 103.0);
    assert_f64(result.close_events[1].size, 0.75);
    assert!(result.action_dispositions.iter().any(|disposition| {
        disposition.action_kind.as_deref() == Some("modify_target")
            && disposition.status == ActionDispositionStatus::Applied
    }));
}

#[test]
fn future_equal_targets_reject_undersized_lot_without_changing_entry_risk() {
    let spec = SymbolSpec {
        canonical: SYMBOL.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    };
    let signal = entry(0, "too-small-for-two-targets", None, vec![101.0, 102.0]);
    let result = run_with_config(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![signal],
        BacktestConfig {
            close_on_finish: false,
            sizing: Some(SizingPolicy::FixedLot { lots: 0.01 }),
            symbol_specs: HashMap::from([(SYMBOL.to_owned(), spec)]),
            ..BacktestConfig::default()
        },
        FutureQuoteConfig::default(),
    );

    assert!(result.recorded_fills.is_empty());
    assert!(result.open_position_snapshots.is_empty());
    assert!(result.completed_positions.is_empty());
    assert!(result.action_dispositions.iter().any(|disposition| {
        disposition.action_kind.as_deref() == Some("entry")
            && disposition.status == ActionDispositionStatus::Rejected
            && disposition
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("rounds to zero lot units"))
    }));
}

#[test]
fn invalid_partial_ratios_preserve_fixed_risk_initial_risk_after_stop_modification() {
    let spec = SymbolSpec {
        canonical: "eurusd".into(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    };
    let config = BacktestConfig {
        close_on_finish: false,
        contract_sizes: HashMap::from([(SYMBOL.to_owned(), 100_000.0)]),
        sizing: Some(SizingPolicy::FixedRiskAmount { amount: 100.0 }),
        symbol_specs: HashMap::from([(SYMBOL.to_owned(), spec)]),
        ..BacktestConfig::default()
    };

    // Public configuration can express exact account risk without conversion for
    // USD-quoted EURUSD. Conversion rates are public only as sizing inputs, so a
    // non-USD exact-risk artifact cannot be asserted through this API.
    let mut signals = vec![
        entry(0, "rr-risk", Some(1.0990), vec![]),
        RawSignal::ModifyStoploss {
            ts: ts(100),
            position: PositionRef::ByTradeId {
                trade_id: "rr-risk".into(),
            },
            price: 1.0995,
        },
    ];
    for (index, ratio) in [0.0, -0.25, 1.25, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
        .into_iter()
        .enumerate()
    {
        signals.push(RawSignal::ClosePartial {
            ts: ts(200 + index as i64 * 100),
            position: PositionRef::ByTradeId {
                trade_id: "rr-risk".into(),
            },
            ratio,
        });
    }
    signals.push(close(900, "rr-risk"));

    let result = run_with_config(
        vec![quote(0, 1.1000, 1.1000), quote(1_000, 1.1020, 1.1020)],
        signals,
        config,
        FutureQuoteConfig {
            currency_plan: Some(usd_identity_plan(SYMBOL)),
            ..FutureQuoteConfig::default()
        },
    );

    let invalid: Vec<_> = result
        .action_dispositions
        .iter()
        .filter(|disposition| disposition.action_kind.as_deref() == Some("close_partial"))
        .collect();
    assert_eq!(invalid.len(), 6);
    assert!(
        invalid
            .iter()
            .all(|disposition| disposition.status == ActionDispositionStatus::Rejected)
    );
    assert!(invalid.iter().all(|disposition| {
        disposition
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("partial-close ratio"))
    }));

    assert_eq!(result.recorded_fills.len(), 2);
    assert_f64(result.recorded_fills[0].size, 1.0);
    assert_eq!(result.completed_positions.len(), 1);
    let completed = &result.completed_positions[0];
    assert_eq!(completed.trade_id.as_deref(), Some("rr-risk"));
    assert_eq!(completed.risk_basis_status, RiskBasisStatus::Available);
    assert_eq!(completed.initial_stop, Some(1.0990));
    assert_eq!(
        completed.effective_stop.map(|stop| stop.origin),
        Some(StopOrigin::Modified)
    );
    assert_eq!(
        completed.effective_stop.map(|stop| stop.price),
        Some(1.0995)
    );
    assert_eq!(completed.risk_tranches.len(), 1);
    assert_eq!(completed.risk_tranches[0].initial_stop, Some(1.0990));
    assert!(
        (completed.initial_risk().expect("initial risk") - 100.0).abs() < 1.0e-8,
        "expected exact initial risk of 100, got {:?}",
        completed.initial_risk()
    );
    assert!((completed.net_pnl - 200.0).abs() < 1.0e-8);
    assert!((completed.realized_r.expect("realized R") - 2.0).abs() < 1.0e-12);
}

#[test]
fn repeated_multi_symbol_bulk_replay_preserves_hidden_drawdown_and_json_bytes() {
    let replay = || {
        run(
            vec![
                quote_for("EURUSD", 0, 100.0, 100.0),
                quote_for("XAUUSD", 0, 200.0, 200.0),
                quote_for("EURUSD", 1_000, 90.0, 90.0),
                quote_for("XAUUSD", 1_000, 190.0, 190.0),
                quote_for("EURUSD", 2_000, 105.0, 105.0),
                quote_for("XAUUSD", 2_500, 210.0, 210.0),
            ],
            vec![
                RawSignal::Entry {
                    ts: ts(0),
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: None,
                    risk_multiplier: 1.0,
                    stoploss: None,
                    targets: vec![],
                    group: Some("fx".into()),
                    trade_id: Some("bulk-eur".into()),
                },
                RawSignal::Entry {
                    ts: ts(0),
                    symbol: "XAUUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: None,
                    risk_multiplier: 1.0,
                    stoploss: None,
                    targets: vec![],
                    group: Some("metal".into()),
                    trade_id: Some("bulk-xau".into()),
                },
                RawSignal::CloseAll { ts: ts(1_500) },
            ],
            false,
        )
    };

    let first = replay();
    assert_eq!(first.mtm_max_drawdown, Some(20.0));
    assert_f64(first.max_drawdown, 0.0);
    assert!(first.mtm_equity_curve.iter().any(|point| {
        point.ts == ts(1_000) && point.equity == Some(9_980.0) && point.drawdown == Some(20.0)
    }));

    let second = replay();
    let first_json = serde_json::to_string(&first).expect("first result must serialize");
    let second_json = serde_json::to_string(&second).expect("second result must serialize");
    assert_eq!(first_json.as_bytes(), second_json.as_bytes());

    assert_eq!(
        first.completed_positions.len(),
        2,
        "CloseAll must complete both symbols; open={:?}; dispositions={:?}",
        first
            .open_position_snapshots
            .iter()
            .map(|position| (&position.symbol, &position.trade_id))
            .collect::<Vec<_>>(),
        first
            .action_dispositions
            .iter()
            .map(|disposition| (
                &disposition.action_id,
                disposition.status,
                disposition.reason.as_deref()
            ))
            .collect::<Vec<_>>()
    );
    assert!(first.open_position_snapshots.is_empty());

    let exits: Vec<_> = first
        .recorded_fills
        .iter()
        .filter(|fill| fill.fill.purpose == FillPurpose::MarketExit)
        .collect();
    assert_eq!(exits.len(), 2);
    let eur_exit = exits
        .iter()
        .find(|fill| fill.symbol == "EURUSD")
        .expect("EURUSD must close on its own eligible quote");
    assert_eq!(eur_exit.quote_ts, ts(2_000));
    assert_f64(eur_exit.fill.price, 105.0);
    let xau_exit = exits
        .iter()
        .find(|fill| fill.symbol == "XAUUSD")
        .expect("XAUUSD must close on its own eligible quote");
    assert_eq!(xau_exit.quote_ts, ts(2_500));
    assert_f64(xau_exit.fill.price, 210.0);

    let no_matching_quote = run(
        vec![
            quote_for("EURUSD", 0, 100.0, 100.0),
            quote_for("XAUUSD", 0, 200.0, 200.0),
            quote_for("EURUSD", 2_000, 105.0, 105.0),
        ],
        vec![
            RawSignal::Entry {
                ts: ts(0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("unmatched-eur".into()),
            },
            RawSignal::Entry {
                ts: ts(0),
                symbol: "XAUUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("unmatched-xau".into()),
            },
            RawSignal::CloseAll { ts: ts(1_500) },
        ],
        false,
    );
    assert_eq!(no_matching_quote.completed_positions.len(), 1);
    assert_eq!(no_matching_quote.open_position_snapshots.len(), 1);
    assert_eq!(
        no_matching_quote.open_position_snapshots[0]
            .trade_id
            .as_deref(),
        Some("unmatched-xau")
    );
    let rejected = no_matching_quote
        .action_dispositions
        .iter()
        .find(|disposition| disposition.reason.as_deref() == Some("no_eligible_quote"))
        .expect("the unmatched XAUUSD close must remain queued until feed end");
    assert_eq!(rejected.status, ActionDispositionStatus::Rejected);
    assert_eq!(rejected.action_kind.as_deref(), Some("close_all"));
    assert_eq!(rejected.effective_ts, Some(ts(1_500)));
}

#[test]
fn fixed_stop_add_remove_updates_open_risk_and_scale_in_risk_basis() {
    let add_and_scale = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 100.0, 100.0),
            quote(2_000, 110.0, 110.0),
        ],
        vec![
            entry(0, "stop-risk", None, vec![]),
            RawSignal::AddRule {
                ts: ts(100),
                position: PositionRef::ByTradeId {
                    trade_id: "stop-risk".into(),
                },
                rule: RuleConfigDef::FixedStoploss { price: 95.0 },
            },
            RawSignal::ScaleIn {
                ts: ts(1_100),
                position: PositionRef::ByTradeId {
                    trade_id: "stop-risk".into(),
                },
                price: None,
                size: 1.0,
            },
        ],
        true,
    );
    assert!(
        add_and_scale
            .mtm_equity_curve
            .iter()
            .any(|point| { point.ts == ts(1_000) && point.open_risk == Some(5.0) })
    );
    let completed = &add_and_scale.completed_positions[0];
    assert_eq!(
        completed.effective_stop.map(|stop| stop.origin),
        Some(StopOrigin::Modified)
    );
    assert_eq!(completed.effective_stop.map(|stop| stop.price), Some(95.0));
    assert_eq!(completed.risk_tranches.len(), 2);
    assert_eq!(completed.risk_tranches[0].initial_stop, None);
    assert_eq!(completed.risk_tranches[1].initial_stop, Some(95.0));

    let add_then_remove = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 100.0, 100.0),
            quote(1_500, 100.0, 100.0),
            quote(2_000, 100.0, 100.0),
        ],
        vec![
            entry(0, "removed-stop", None, vec![]),
            RawSignal::AddRule {
                ts: ts(100),
                position: PositionRef::ByTradeId {
                    trade_id: "removed-stop".into(),
                },
                rule: RuleConfigDef::FixedStoploss { price: 90.0 },
            },
            RawSignal::RemoveRule {
                ts: ts(1_600),
                position: PositionRef::ByTradeId {
                    trade_id: "removed-stop".into(),
                },
                rule_name: "FixedStoploss".into(),
            },
        ],
        true,
    );
    assert!(
        add_then_remove
            .mtm_equity_curve
            .iter()
            .any(|point| { point.ts == ts(1_500) && point.open_risk == Some(10.0) })
    );
    assert!(add_then_remove.mtm_equity_curve.iter().any(|point| {
        point.ts == ts(2_000)
            && point.open_position_count == 1
            && point.open_risk.is_none()
            && point.unavailable_open_risk_count == 1
    }));
    assert_eq!(add_then_remove.completed_positions[0].effective_stop, None);
}

#[test]
fn pending_modified_stop_preserves_origin_through_fill_and_completion() {
    let result = run(
        vec![
            quote(0, 100.0, 100.0),
            quote(1_000, 90.0, 90.0),
            quote(2_000, 95.0, 95.0),
        ],
        vec![
            RawSignal::Entry {
                ts: ts(0),
                symbol: SYMBOL.into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(90.0),
                risk_multiplier: 1.0,
                stoploss: Some(80.0),
                targets: vec![],
                group: None,
                trade_id: Some("pending-modified-stop".into()),
            },
            RawSignal::ModifyStoploss {
                ts: ts(500),
                position: PositionRef::ByTradeId {
                    trade_id: "pending-modified-stop".into(),
                },
                price: 85.0,
            },
        ],
        true,
    );

    let completed = &result.completed_positions[0];
    assert_eq!(completed.initial_stop, Some(80.0));
    assert_eq!(
        completed.effective_stop.map(|stop| stop.origin),
        Some(StopOrigin::Modified)
    );
    assert_eq!(completed.effective_stop.map(|stop| stop.price), Some(85.0));
    assert_eq!(completed.risk_tranches[0].initial_stop, Some(85.0));
}

#[test]
fn stale_multi_symbol_eod_close_uses_global_execution_time_and_source_quote_age() {
    let source_ts = NaiveDate::from_ymd_opt(2026, 1, 31)
        .unwrap()
        .and_hms_opt(23, 0, 0)
        .unwrap();
    let feed_end_ts = NaiveDate::from_ymd_opt(2026, 2, 1)
        .unwrap()
        .and_hms_opt(1, 0, 0)
        .unwrap();
    let mut feed = VecFeed::new(vec![
        MarketEvent::Tick {
            symbol: "STALE".into(),
            ts: source_ts,
            bid: 100.0,
            ask: 100.0,
        },
        MarketEvent::Tick {
            symbol: "FRESH".into(),
            ts: feed_end_ts,
            bid: 200.0,
            ask: 200.0,
        },
    ]);
    let signals = vec![RawSignal::Entry {
        ts: source_ts,
        symbol: "STALE".into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: None,
        targets: vec![],
        group: None,
        trade_id: Some("stale-eod".into()),
    }];
    let result = BacktestRunner::new_future(
        fixed_lot_config(true, 1.0, &["STALE"]),
        FutureQuoteConfig {
            stale_quote_after_ms: Some(1_000),
            ..FutureQuoteConfig::default()
        },
    )
    .run_raw_signals_future(&mut feed, signals, None);

    let exit = result
        .recorded_fills
        .iter()
        .find(|fill| fill.fill.purpose == FillPurpose::MarketExit)
        .unwrap();
    assert_eq!(exit.quote_ts, source_ts);
    assert_eq!(exit.execution_ts, Some(feed_end_ts));
    assert_eq!(exit.quote_age_millis, Some(7_200_000));
    assert_eq!(result.close_events[0].ts, feed_end_ts);
    assert_eq!(result.completed_positions[0].close_ts, feed_end_ts);
    assert_eq!(result.monthly_returns[0].month, 2);
}

#[test]
fn quote_source_order_is_checked_per_symbol_before_global_sorting() {
    let reversed = run(
        vec![quote(1_000, 101.0, 101.0), quote(0, 100.0, 100.0)],
        vec![entry(0, "reversed", None, vec![])],
        false,
    );
    assert_eq!(
        reversed
            .execution_metadata
            .as_ref()
            .unwrap()
            .tags
            .get("invalid_quote_count")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(reversed.recorded_fills[0].quote_ts, ts(1_000));

    let interleaved = run(
        vec![
            quote_for("A", 1_000, 100.0, 100.0),
            quote_for("B", 0, 200.0, 200.0),
            quote_for("A", 2_000, 101.0, 101.0),
            quote_for("B", 500, 201.0, 201.0),
        ],
        vec![],
        false,
    );
    assert_eq!(
        interleaved
            .execution_metadata
            .as_ref()
            .unwrap()
            .tags
            .get("invalid_quote_count")
            .map(String::as_str),
        Some("0")
    );
}

#[test]
fn invalid_accounting_configs_are_rejected_before_feed_consumption() {
    let mut invalid_balance_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let invalid_balance = BacktestRunner::new_future(
        BacktestConfig {
            initial_balance: f64::NAN,
            ..BacktestConfig::default()
        },
        FutureQuoteConfig::default(),
    )
    .run_raw_signals_future(&mut invalid_balance_feed, vec![], None);
    assert_eq!(invalid_balance_feed.remaining(), 1);
    assert!(invalid_balance.recorded_fills.is_empty());
    assert!(
        invalid_balance.action_dispositions[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("initial balance")
    );

    let mut invalid_contract_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let invalid_contract = BacktestRunner::new_future(
        BacktestConfig {
            contract_sizes: HashMap::from([(SYMBOL.into(), 0.0)]),
            ..BacktestConfig::default()
        },
        FutureQuoteConfig::default(),
    )
    .run_raw_signals_future(&mut invalid_contract_feed, vec![], None);
    assert_eq!(invalid_contract_feed.remaining(), 1);
    assert!(
        invalid_contract.action_dispositions[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("contract size")
    );

    let mut invalid_lot_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let invalid_lot = BacktestRunner::new_future(
        BacktestConfig {
            sizing: Some(SizingPolicy::FixedLot { lots: 0.01 }),
            symbol_specs: HashMap::from([(
                SYMBOL.into(),
                SymbolSpec {
                    canonical: "eurusd".into(),
                    pip_position: 4,
                    digits: 5,
                    category: "forex".into(),
                    lot_base_units: 0,
                    lot_step_units: 1_000,
                    lot_min_steps: 1,
                    lot_max_steps: 0,
                },
            )]),
            ..BacktestConfig::default()
        },
        FutureQuoteConfig::default(),
    )
    .run_raw_signals_future(&mut invalid_lot_feed, vec![], None);
    assert_eq!(invalid_lot_feed.remaining(), 1);
    assert!(
        invalid_lot.action_dispositions[0]
            .reason
            .as_deref()
            .unwrap()
            .contains("lot metadata")
    );
}

#[test]
fn supplied_symbol_specs_and_latency_overflow_are_rejected_before_feed_consumption() {
    let invalid_spec = SymbolSpec {
        canonical: "eurusd".into(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 0,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    };
    let config = BacktestConfig {
        symbol_specs: HashMap::from([(SYMBOL.into(), invalid_spec)]),
        sizing: None,
        ..BacktestConfig::default()
    };

    let mut future_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let future_result = BacktestRunner::new_future(config.clone(), FutureQuoteConfig::default())
        .run_raw_signals_future(&mut future_feed, vec![], None);
    assert_eq!(future_feed.remaining(), 1);
    assert!(
        future_result.action_dispositions[0]
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("lot metadata"))
    );

    let mut legacy_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let legacy_result = BacktestRunner::new(config).run_raw_signals(&mut legacy_feed, vec![], None);
    assert_eq!(legacy_feed.remaining(), 1);
    assert!(legacy_result.trade_log.is_empty());

    let mut overflow_feed = VecFeed::new(vec![quote(0, 100.0, 100.0)]);
    let overflow = BacktestRunner::new_future(
        BacktestConfig::default(),
        FutureQuoteConfig {
            signal_latency_ms: 1,
            ..FutureQuoteConfig::default()
        },
    )
    .run_raw_signals_future(
        &mut overflow_feed,
        vec![RawSignal::CloseAll {
            ts: NaiveDateTime::MAX,
        }],
        None,
    );
    assert_eq!(overflow_feed.remaining(), 1);
    assert!(
        overflow.action_dispositions[0]
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("overflows datetime"))
    );
}

#[test]
fn stale_management_actions_are_rejected_without_completed_artifact_drift() {
    let position = || PositionRef::ByTradeId {
        trade_id: "stale".into(),
    };
    let result = run(
        vec![quote(0, 100.0, 100.0), quote(1_000, 101.0, 101.0)],
        vec![
            entry(0, "stale", Some(95.0), vec![101.0]),
            RawSignal::ModifyStoploss {
                ts: ts(1_000),
                position: position(),
                price: 94.0,
            },
            RawSignal::MoveStoplossToEntry {
                ts: ts(1_000),
                position: position(),
            },
            RawSignal::AddTarget {
                ts: ts(1_000),
                position: position(),
                price: 110.0,
                close_ratio: 1.0,
            },
            RawSignal::RemoveTarget {
                ts: ts(1_000),
                position: position(),
                price: 101.0,
            },
            RawSignal::ModifyTarget {
                ts: ts(1_000),
                position: position(),
                old_price: 101.0,
                new_price: 110.0,
            },
            RawSignal::AddRule {
                ts: ts(1_000),
                position: position(),
                rule: RuleConfigDef::FixedStoploss { price: 90.0 },
            },
            RawSignal::RemoveRule {
                ts: ts(1_000),
                position: position(),
                rule_name: "FixedStoploss".into(),
            },
            RawSignal::ScaleIn {
                ts: ts(1_000),
                position: position(),
                price: Some(100.0),
                size: 1.0,
            },
            RawSignal::ClosePartial {
                ts: ts(1_000),
                position: position(),
                ratio: 0.5,
            },
            close(1_000, "stale"),
        ],
        false,
    );

    let stale: Vec<_> = result
        .action_dispositions
        .iter()
        .filter(|disposition| disposition.action_kind.as_deref() != Some("entry"))
        .collect();
    assert_eq!(stale.len(), 10);
    assert!(stale.iter().all(|disposition| {
        matches!(
            disposition.status,
            ActionDispositionStatus::Rejected | ActionDispositionStatus::Skipped
        )
    }));
    assert!(
        stale
            .iter()
            .all(|disposition| disposition.status != ActionDispositionStatus::Applied)
    );
    assert_eq!(result.recorded_fills.len(), 2);
    assert_eq!(result.close_events.len(), 1);
    assert_eq!(result.completed_positions.len(), 1);
    let completed = &result.completed_positions[0];
    assert_eq!(completed.initial_stop, Some(95.0));
    assert_eq!(completed.effective_stop.map(|stop| stop.price), Some(95.0));
    assert_eq!(completed.entry_size, 1.0);
}

#[test]
fn runner_accounting_failure_rolls_back_engine_executor_and_portfolio_state() {
    let result = run_with_config(
        vec![quote(0, 100.0, 100.0), quote(1_000, 200.0, 200.0)],
        vec![entry(0, "rollback", None, vec![]), close(500, "rollback")],
        BacktestConfig {
            close_on_finish: false,
            contract_sizes: HashMap::from([(SYMBOL.into(), f64::MAX)]),
            sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
            symbol_specs: HashMap::from([(SYMBOL.into(), symbol_spec(SYMBOL))]),
            ..BacktestConfig::default()
        },
        FutureQuoteConfig::default(),
    );

    let close_disposition = result
        .action_dispositions
        .iter()
        .find(|disposition| disposition.action_kind.as_deref() == Some("close"))
        .expect("close disposition");
    assert_eq!(close_disposition.status, ActionDispositionStatus::Failed);
    assert_eq!(result.recorded_fills.len(), 1);
    assert!(result.close_events.is_empty());
    assert!(result.completed_positions.is_empty());
    assert_eq!(result.open_position_snapshots.len(), 1);
    assert_eq!(result.open_position_snapshots[0].remaining_size, 1.0);
    assert!(result.trade_log.is_empty());
}

#[test]
fn runner_conserves_size_and_pnl_and_records_normal_fill_timestamps() {
    let signals = vec![
        RawSignal::Entry {
            ts: ts(0),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 2.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("conservation".into()),
        },
        RawSignal::ScaleIn {
            ts: ts(1_100),
            position: PositionRef::ByTradeId {
                trade_id: "conservation".into(),
            },
            price: None,
            size: 1.0,
        },
        RawSignal::ClosePartial {
            ts: ts(2_100),
            position: PositionRef::ByTradeId {
                trade_id: "conservation".into(),
            },
            ratio: 0.5,
        },
        close(3_100, "conservation"),
    ];
    let result = run_with_config(
        vec![
            quote(1_000, 100.0, 100.0),
            quote(2_000, 110.0, 110.0),
            quote(3_000, 120.0, 120.0),
            quote(4_000, 130.0, 130.0),
        ],
        signals,
        fixed_lot_config(false, 1.0, &[SYMBOL]),
        FutureQuoteConfig {
            signal_latency_ms: 100,
            ..FutureQuoteConfig::default()
        },
    );

    assert_eq!(result.recorded_fills.len(), 4);
    let entry_fill = &result.recorded_fills[0];
    assert_eq!(entry_fill.signal_ts, Some(ts(0)));
    assert_eq!(entry_fill.effective_ts, ts(100));
    assert_eq!(entry_fill.execution_ts, Some(ts(1_000)));
    assert_eq!(entry_fill.quote_ts, ts(1_000));
    assert!(entry_fill.signal_ts.unwrap() < entry_fill.effective_ts);
    assert!(entry_fill.effective_ts < entry_fill.execution_ts.unwrap());

    let entered_size: f64 = result
        .recorded_fills
        .iter()
        .filter(|fill| fill.fill.purpose.is_entry())
        .map(|fill| fill.size)
        .sum();
    let closed_size: f64 = result.close_events.iter().map(|event| event.size).sum();
    assert_f64(entered_size, 3.0);
    assert_f64(closed_size, entered_size);

    let close_pnl: f64 = result.close_events.iter().map(|event| event.pnl).sum();
    let trade_pnl: f64 = result.trade_log.iter().map(|trade| trade.pnl).sum();
    assert_f64(close_pnl, 65.0);
    assert_f64(trade_pnl, close_pnl);
    assert_eq!(result.completed_positions.len(), 1);
    assert_f64(result.completed_positions[0].net_pnl, close_pnl);
    assert_f64(result.total_pnl, close_pnl);
    assert_f64(result.final_balance, result.initial_balance + close_pnl);
    assert!(result.open_position_snapshots.is_empty());
}

#[test]
fn tiny_sizes_are_rejected_in_legacy_and_future_without_ghost_positions() {
    let tiny = RawSignal::Entry {
        ts: ts(0),
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: Some(100.0),
        risk_multiplier: 1.0,
        stoploss: None,
        targets: vec![],
        group: None,
        trade_id: Some("tiny".into()),
    };
    let events = vec![quote(0, 100.0, 100.0)];
    let config = BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0e-12 }),
        symbol_specs: HashMap::from([(
            SYMBOL.into(),
            SymbolSpec {
                canonical: SYMBOL.to_ascii_lowercase(),
                pip_position: 4,
                digits: 5,
                category: "forex".into(),
                lot_base_units: 1_000_000_000_000,
                lot_step_units: 1,
                lot_min_steps: 1,
                lot_max_steps: 0,
            },
        )]),
        ..BacktestConfig::default()
    };

    let mut legacy_feed = VecFeed::new(events.clone());
    let legacy = BacktestRunner::new(config.clone()).run_raw_signals(
        &mut legacy_feed,
        vec![tiny.clone()],
        None,
    );
    assert!(legacy.trade_log.is_empty());

    let future = run_with_config(events, vec![tiny], config, FutureQuoteConfig::default());
    assert!(future.recorded_fills.is_empty());
    assert!(future.open_position_snapshots.is_empty());
    assert!(future.completed_positions.is_empty());
    assert!(future.action_dispositions.iter().any(|disposition| {
        disposition.status == ActionDispositionStatus::Rejected
            && disposition
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("accounting tolerance"))
    }));
}

#[test]
fn near_full_close_consumes_account_and_future_scale_in_guard_remains() {
    let result = run(
        vec![quote(0, 100.0, 100.0), quote(1_000, 110.0, 110.0)],
        vec![
            entry(0, "near-full", None, vec![]),
            RawSignal::ClosePartial {
                ts: ts(500),
                position: PositionRef::ByTradeId {
                    trade_id: "near-full".into(),
                },
                ratio: 1.0 - 5.0e-13,
            },
            RawSignal::ScaleIn {
                ts: ts(600),
                position: PositionRef::ByTradeId {
                    trade_id: "near-full".into(),
                },
                price: None,
                size: 1.0,
            },
        ],
        false,
    );

    assert_eq!(result.close_events.len(), 1);
    assert_eq!(result.close_events[0].size, 1.0);
    assert!(result.open_position_snapshots.is_empty());
    assert!(result.action_dispositions.iter().any(|disposition| {
        disposition.action_kind.as_deref() == Some("scale_in")
            && disposition.reason.as_deref() == Some("scale_in_after_close_not_supported")
    }));
}
