//! Integration tests for qs-core trade engine.

use chrono::NaiveDate;
use chrono::NaiveDateTime;

use qs_core::TradeEngine;
use qs_core::types::{
    Action, CloseReason, Effect, OrderType, PositionRecord, PriceQuote, RuleConfig, Side,
    TargetSpec,
};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(h, m, s)
        .unwrap()
}

fn quote(symbol: &str, bid: f64, ask: f64, time: NaiveDateTime) -> PriceQuote {
    PriceQuote {
        symbol: symbol.into(),
        ts: time,
        bid,
        ask,
    }
}

fn open_buy(engine: &mut TradeEngine, symbol: &str, price: f64, size: f64) -> String {
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: symbol.into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(price),
                size,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        other => panic!("expected PositionOpened, got {:?}", other),
    }
}

fn open_sell(engine: &mut TradeEngine, symbol: &str, price: f64, size: f64) -> String {
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: symbol.into(),
                side: Side::Sell,
                order_type: OrderType::Market,
                price: Some(price),
                size,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        other => panic!("expected PositionOpened, got {:?}", other),
    }
}

// ─── Full lifecycle: market order → SL hit ──────────────────────────────────

#[test]
fn lifecycle_buy_stoploss() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![
                    TargetSpec {
                        price: 1.0900,
                        close_ratio: 0.5,
                    },
                    TargetSpec {
                        price: 1.0950,
                        close_ratio: 0.5,
                    },
                ],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();

    assert_eq!(effects.len(), 1);
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.rules.len(), 3); // 1 SL + 2 TP

    // Price rises a bit — no events
    let effects = engine.on_price(&quote("EURUSD", 1.0860, 1.0862, ts(10, 1, 0)));
    assert!(effects.is_empty());

    // Price drops to SL
    let effects = engine.on_price(&quote("EURUSD", 1.0799, 1.0801, ts(10, 5, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::Stoploss,
            ..
        }
    )));

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.status, qs_core::PositionStatus::Closed);
}

// ─── Full lifecycle: sell order → TP hit ────────────────────────────────────

#[test]
fn lifecycle_sell_take_profit() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "XAUUSD".into(),
                side: Side::Sell,
                order_type: OrderType::Market,
                price: Some(2000.0),
                size: 1.0,
                stoploss: Some(2020.0),
                targets: vec![TargetSpec {
                    price: 1980.0,
                    close_ratio: 1.0,
                }],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Price drops to TP (for sell, check ask <= target)
    let effects = engine.on_price(&quote("XAUUSD", 1979.0, 1980.0, ts(10, 5, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::Target,
            ..
        }
    )));

    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

// ─── Partial close then remaining closed by SL ─────────────────────────────

#[test]
fn partial_tp_then_sl() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 2.0,
                stoploss: Some(1.0800),
                targets: vec![TargetSpec {
                    price: 1.0900,
                    close_ratio: 0.5,
                }],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // TP1 hit → partial close 50%
    let effects = engine.on_price(&quote("EURUSD", 1.0901, 1.0903, ts(10, 5, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PartialClose {
            reason: CloseReason::Target,
            ..
        }
    )));

    let pos = engine.get_position(&id).unwrap();
    assert!((pos.data.remaining_ratio - 0.5).abs() < f64::EPSILON);
    assert_eq!(pos.data.target_hits, 1);

    // SL hit → close remaining
    let effects = engine.on_price(&quote("EURUSD", 1.0799, 1.0801, ts(10, 10, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::Stoploss,
            ..
        }
    )));

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.status, qs_core::PositionStatus::Closed);
    assert!(pos.data.remaining_ratio.abs() < f64::EPSILON);
}

// ─── Limit order lifecycle ──────────────────────────────────────────────────

#[test]
fn limit_buy_fills_and_tp_closes() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(1.0800),
                size: 1.0,
                stoploss: Some(1.0750),
                targets: vec![TargetSpec {
                    price: 1.0850,
                    close_ratio: 1.0,
                }],
                rules: vec![],
                group: None,
            },
            ts(9, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::OrderPlaced { id } => id.clone(),
        _ => panic!(),
    };

    // Doesn't fill yet
    let effects = engine.on_price(&quote("EURUSD", 1.0810, 1.0812, ts(9, 30, 0)));
    assert!(effects.is_empty());

    // Fills
    let effects = engine.on_price(&quote("EURUSD", 1.0798, 1.0800, ts(9, 45, 0)));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::PositionOpened { .. }))
    );

    // TP hit
    let effects = engine.on_price(&quote("EURUSD", 1.0850, 1.0852, ts(10, 0, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::Target,
            ..
        }
    )));

    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

// ─── Stop order lifecycle ───────────────────────────────────────────────────

#[test]
fn stop_buy_fills_on_breakout() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Stop,
                price: Some(1.0900),
                size: 1.0,
                stoploss: Some(1.0850),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(9, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::OrderPlaced { id } => id.clone(),
        _ => panic!(),
    };

    // Not yet at stop price
    let effects = engine.on_price(&quote("EURUSD", 1.0880, 1.0882, ts(9, 30, 0)));
    assert!(effects.is_empty());

    // Breakout — ask hits stop price
    let effects = engine.on_price(&quote("EURUSD", 1.0898, 1.0900, ts(9, 45, 0)));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::PositionOpened { .. }))
    );

    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Open
    );
}

// ─── Scale-in changes average entry ─────────────────────────────────────────

#[test]
fn scale_in_updates_average_entry() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0800, 1.0);

    engine
        .apply_action(
            Action::ScaleIn {
                position_id: id.clone(),
                price: Some(1.0900),
                size: 1.0,
            },
            ts(10, 5, 0),
        )
        .unwrap();

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.entries.len(), 2);
    assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
    assert!((pos.data.total_filled_size() - 2.0).abs() < f64::EPSILON);
}

// ─── Breakeven after N targets ──────────────────────────────────────────────

#[test]
fn breakeven_after_two_targets() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![
                    TargetSpec {
                        price: 1.0880,
                        close_ratio: 0.25,
                    },
                    TargetSpec {
                        price: 1.0900,
                        close_ratio: 0.25,
                    },
                ],
                rules: vec![RuleConfig::BreakevenAfterTargets { after_n: 2 }],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Hit TP1
    engine.on_price(&quote("EURUSD", 1.0881, 1.0883, ts(10, 1, 0)));
    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.target_hits, 1);
    assert!((pos.current_stoploss().unwrap() - 1.0800).abs() < f64::EPSILON);

    // Hit TP2 → breakeven rule should fire on next tick
    engine.on_price(&quote("EURUSD", 1.0901, 1.0903, ts(10, 2, 0)));
    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.target_hits, 2);

    // Next tick: breakeven rule evaluates, sees target_hits >= 2, moves SL to entry
    let effects = engine.on_price(&quote("EURUSD", 1.0905, 1.0907, ts(10, 3, 0)));
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::StoplossModified { .. }))
    );

    let pos = engine.get_position(&id).unwrap();
    assert!((pos.current_stoploss().unwrap() - 1.0850).abs() < f64::EPSILON);
}

// ─── Time exit ──────────────────────────────────────────────────────────────

#[test]
fn time_exit_closes_position() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![RuleConfig::TimeExit { max_seconds: 3600 }],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // 30 min later — no exit
    let effects = engine.on_price(&quote("EURUSD", 1.0860, 1.0862, ts(10, 30, 0)));
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::PositionClosed { .. }))
    );

    // 1 hour later — exit
    let effects = engine.on_price(&quote("EURUSD", 1.0860, 1.0862, ts(11, 0, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::TimeExit,
            ..
        }
    )));

    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

// ─── Trailing stop ──────────────────────────────────────────────────────────

#[test]
fn trailing_stop_follows_price_and_closes() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![RuleConfig::TrailingStop { distance: 0.0030 }],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Price rises: peak tracks upward
    engine.on_price(&quote("EURUSD", 1.0870, 1.0872, ts(10, 1, 0)));
    engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 2, 0)));
    engine.on_price(&quote("EURUSD", 1.0920, 1.0922, ts(10, 3, 0)));

    // Price drops but within trailing distance (peak=1.0920, sl=1.0890)
    let effects = engine.on_price(&quote("EURUSD", 1.0895, 1.0897, ts(10, 4, 0)));
    assert!(
        !effects
            .iter()
            .any(|e| matches!(e, Effect::PositionClosed { .. }))
    );

    // Price drops below trailing stop
    let effects = engine.on_price(&quote("EURUSD", 1.0889, 1.0891, ts(10, 5, 0)));
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::TrailingStop,
            ..
        }
    )));

    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

// ─── Breakeven when price trigger ───────────────────────────────────────────

#[test]
fn breakeven_when_moves_sl_to_entry() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![RuleConfig::BreakevenWhen {
                    trigger_price: 1.0900,
                }],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Before trigger
    engine.on_price(&quote("EURUSD", 1.0880, 1.0882, ts(10, 1, 0)));
    assert!(
        (engine
            .get_position(&id)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0800)
            .abs()
            < f64::EPSILON
    );

    // Trigger
    engine.on_price(&quote("EURUSD", 1.0901, 1.0903, ts(10, 2, 0)));
    assert!(
        (engine
            .get_position(&id)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0850)
            .abs()
            < f64::EPSILON
    );

    // Subsequent ticks don't re-trigger
    engine.on_price(&quote("EURUSD", 1.0920, 1.0922, ts(10, 3, 0)));
    assert!(
        (engine
            .get_position(&id)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0850)
            .abs()
            < f64::EPSILON
    );
}

// ─── Manual close and cancel ────────────────────────────────────────────────

#[test]
fn manual_close_and_partial_close() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 2.0);

    // Seed a price for partial close calculation
    engine.on_price(&quote("EURUSD", 1.0860, 1.0862, ts(10, 1, 0)));

    // Close 30%
    let effects = engine
        .apply_action(
            Action::ClosePartial {
                position_id: id.clone(),
                ratio: 0.3,
            },
            ts(10, 2, 0),
        )
        .unwrap();
    assert!(
        effects
            .iter()
            .any(|e| matches!(e, Effect::PartialClose { .. }))
    );

    let pos = engine.get_position(&id).unwrap();
    assert!((pos.data.remaining_ratio - 0.7).abs() < f64::EPSILON);

    // Close remaining
    let effects = engine
        .apply_action(
            Action::ClosePosition {
                position_id: id.clone(),
            },
            ts(10, 3, 0),
        )
        .unwrap();
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::Manual,
            ..
        }
    )));
}

#[test]
fn cancel_pending_order() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(1.0800),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(9, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::OrderPlaced { id } => id.clone(),
        _ => panic!(),
    };

    let effects = engine
        .apply_action(
            Action::CancelPending {
                position_id: id.clone(),
            },
            ts(9, 30, 0),
        )
        .unwrap();

    assert!(matches!(&effects[0], Effect::OrderCancelled { .. }));
    assert_eq!(
        engine.get_position(&id).unwrap().data.status,
        qs_core::PositionStatus::Cancelled
    );
}

// ─── Bulk operations ────────────────────────────────────────────────────────

#[test]
fn close_all_positions() {
    let mut engine = TradeEngine::new();
    open_buy(&mut engine, "EURUSD", 1.0850, 1.0);
    open_sell(&mut engine, "XAUUSD", 2000.0, 1.0);
    open_buy(&mut engine, "GBPUSD", 1.2500, 1.0);

    assert_eq!(engine.open_positions().len(), 3);

    let effects = engine.apply_action(Action::CloseAll, ts(10, 5, 0)).unwrap();
    assert_eq!(effects.len(), 3);
    assert_eq!(engine.open_positions().len(), 0);
    assert_eq!(engine.closed_positions().len(), 3);
}

#[test]
fn close_all_of_symbol() {
    let mut engine = TradeEngine::new();
    open_buy(&mut engine, "EURUSD", 1.0850, 1.0);
    open_sell(&mut engine, "EURUSD", 1.0850, 1.0);
    open_buy(&mut engine, "XAUUSD", 2000.0, 1.0);

    let effects = engine
        .apply_action(
            Action::CloseAllOf {
                symbol: "EURUSD".into(),
            },
            ts(10, 5, 0),
        )
        .unwrap();

    assert_eq!(effects.len(), 2);
    assert_eq!(engine.open_positions().len(), 1);
    assert_eq!(engine.open_positions()[0].data.symbol, "XAUUSD");
}

#[test]
fn cancel_all_pending() {
    let mut engine = TradeEngine::new();
    engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(1.0800),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(9, 0, 0),
        )
        .unwrap();
    engine
        .apply_action(
            Action::Open {
                symbol: "XAUUSD".into(),
                side: Side::Sell,
                order_type: OrderType::Stop,
                price: Some(1990.0),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(9, 0, 0),
        )
        .unwrap();

    // Also add an open position that shouldn't be affected
    open_buy(&mut engine, "GBPUSD", 1.2500, 1.0);

    assert_eq!(engine.pending_positions().len(), 2);
    assert_eq!(engine.open_positions().len(), 1);

    let effects = engine
        .apply_action(Action::CancelAllPending, ts(9, 30, 0))
        .unwrap();
    assert_eq!(effects.len(), 2);
    assert_eq!(engine.pending_positions().len(), 0);
    assert_eq!(engine.open_positions().len(), 1); // GBPUSD unaffected
}

// ─── Modify stoploss and targets via actions ────────────────────────────────

#[test]
fn modify_stoploss_via_action() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    engine
        .apply_action(
            Action::ModifyStoploss {
                position_id: id.clone(),
                price: 1.0820,
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert!(
        (engine
            .get_position(&id)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0820)
            .abs()
            < f64::EPSILON
    );
}

#[test]
fn move_stoploss_to_entry() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    engine
        .apply_action(
            Action::MoveStoplossToEntry {
                position_id: id.clone(),
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert!(
        (engine
            .get_position(&id)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0850)
            .abs()
            < f64::EPSILON
    );
}

#[test]
fn add_and_remove_target() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 1.0);

    // Add target
    engine
        .apply_action(
            Action::AddTarget {
                position_id: id.clone(),
                price: 1.0900,
                close_ratio: 0.5,
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert_eq!(engine.get_position(&id).unwrap().rules.len(), 1);

    // Add another
    engine
        .apply_action(
            Action::AddTarget {
                position_id: id.clone(),
                price: 1.0950,
                close_ratio: 0.5,
            },
            ts(10, 2, 0),
        )
        .unwrap();

    assert_eq!(engine.get_position(&id).unwrap().rules.len(), 2);

    // Remove first target
    engine
        .apply_action(
            Action::RemoveTarget {
                position_id: id.clone(),
                price: 1.0900,
            },
            ts(10, 3, 0),
        )
        .unwrap();

    assert_eq!(engine.get_position(&id).unwrap().rules.len(), 1);
}

// ─── Modify all stoploss on symbol ──────────────────────────────────────────

#[test]
fn modify_all_stoploss_on_symbol() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id1 = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Sell,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0900),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id2 = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    let id_xau = open_buy(&mut engine, "XAUUSD", 2000.0, 1.0);

    engine
        .apply_action(
            Action::ModifyAllStoploss {
                symbol: "EURUSD".into(),
                price: 1.0850,
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert!(
        (engine
            .get_position(&id1)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0850)
            .abs()
            < f64::EPSILON
    );
    assert!(
        (engine
            .get_position(&id2)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0850)
            .abs()
            < f64::EPSILON
    );
    // XAUUSD should not be affected (no stoploss)
    assert!(
        engine
            .get_position(&id_xau)
            .unwrap()
            .current_stoploss()
            .is_none()
    );
}

// ─── Error cases ────────────────────────────────────────────────────────────

#[test]
fn error_close_nonexistent_position() {
    let mut engine = TradeEngine::new();
    let result = engine.apply_action(
        Action::ClosePosition {
            position_id: "nope".into(),
        },
        ts(10, 0, 0),
    );
    assert!(result.is_err());
}

#[test]
fn error_close_already_closed() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 1.0);

    engine
        .apply_action(
            Action::ClosePosition {
                position_id: id.clone(),
            },
            ts(10, 1, 0),
        )
        .unwrap();

    // Trying to close again should fail
    let result = engine.apply_action(
        Action::ClosePosition {
            position_id: id.clone(),
        },
        ts(10, 2, 0),
    );
    assert!(result.is_err());
}

#[test]
fn error_cancel_open_position() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 1.0);

    let result = engine.apply_action(
        Action::CancelPending {
            position_id: id.clone(),
        },
        ts(10, 1, 0),
    );
    assert!(result.is_err());
}

#[test]
fn error_scale_into_closed() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 1.0);

    engine
        .apply_action(
            Action::ClosePosition {
                position_id: id.clone(),
            },
            ts(10, 1, 0),
        )
        .unwrap();

    let result = engine.apply_action(
        Action::ScaleIn {
            position_id: id.clone(),
            price: Some(1.0900),
            size: 1.0,
        },
        ts(10, 2, 0),
    );
    assert!(result.is_err());
}

// ─── Multi-symbol isolation ─────────────────────────────────────────────────

#[test]
fn price_updates_only_affect_matching_symbol() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let eur_id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // XAUUSD price drop should NOT trigger EURUSD SL
    let effects = engine.on_price(&quote("XAUUSD", 1.0700, 1.0702, ts(10, 1, 0)));
    assert!(effects.is_empty());
    assert_eq!(
        engine.get_position(&eur_id).unwrap().data.status,
        qs_core::PositionStatus::Open
    );

    // EURUSD price drop DOES trigger SL
    let effects = engine.on_price(&quote("EURUSD", 1.0799, 1.0801, ts(10, 2, 0)));
    assert!(!effects.is_empty());
    assert_eq!(
        engine.get_position(&eur_id).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

// ─── Audit trail ────────────────────────────────────────────────────────────

#[test]
fn position_records_audit_trail() {
    let mut engine = TradeEngine::new();
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: None,
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id = match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Modify SL
    engine
        .apply_action(
            Action::ModifyStoploss {
                position_id: id.clone(),
                price: 1.0820,
            },
            ts(10, 1, 0),
        )
        .unwrap();

    // Close
    engine
        .apply_action(
            Action::ClosePosition {
                position_id: id.clone(),
            },
            ts(10, 2, 0),
        )
        .unwrap();

    let pos = engine.get_position(&id).unwrap();
    let records: Vec<_> = pos.data.records.iter().map(|(r, _)| r).collect();

    // Should have: Created, StoplossModified, Closed
    assert!(records.len() >= 3);
    assert!(matches!(
        records[0],
        qs_core::PositionRecord::Created { .. }
    ));
    assert!(matches!(
        records[1],
        qs_core::PositionRecord::StoplossModified { .. }
    ));
    assert!(matches!(
        records[records.len() - 1],
        qs_core::PositionRecord::Closed { .. }
    ));
}

// ─── Add and remove rule via action ─────────────────────────────────────────

#[test]
fn add_and_remove_rule() {
    let mut engine = TradeEngine::new();
    let id = open_buy(&mut engine, "EURUSD", 1.0850, 1.0);

    engine
        .apply_action(
            Action::AddRule {
                position_id: id.clone(),
                rule: RuleConfig::TimeExit { max_seconds: 7200 },
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert_eq!(engine.get_position(&id).unwrap().rules.len(), 1);
    assert_eq!(
        engine.get_position(&id).unwrap().rules[0].name(),
        "TimeExit"
    );

    engine
        .apply_action(
            Action::RemoveRule {
                position_id: id.clone(),
                rule_name: "TimeExit".into(),
            },
            ts(10, 2, 0),
        )
        .unwrap();

    assert_eq!(engine.get_position(&id).unwrap().rules.len(), 0);
}

/// Helper: open a position with a group assignment.
fn open_with_group(
    engine: &mut TradeEngine,
    symbol: &str,
    side: Side,
    price: f64,
    size: f64,
    group: Option<&str>,
) -> String {
    let effects = engine
        .apply_action(
            Action::Open {
                symbol: symbol.into(),
                side,
                order_type: OrderType::Market,
                price: Some(price),
                size,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: group.map(|s| s.to_owned()),
            },
            ts(10, 0, 0),
        )
        .unwrap();
    match &effects[0] {
        Effect::PositionOpened { id } => id.clone(),
        other => panic!("expected PositionOpened, got {:?}", other),
    }
}

#[test]
fn open_with_group_assigns_position_to_group() {
    let mut engine = TradeEngine::new();
    let id = open_with_group(
        &mut engine,
        "EURUSD",
        Side::Buy,
        1.0850,
        1.0,
        Some("signals_v1"),
    );

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.group, Some("signals_v1".to_owned()));

    // Group is registered in the manager.
    let group_ids = engine.manager.group_position_ids("signals_v1");
    assert_eq!(group_ids.len(), 1);
    assert_eq!(group_ids[0], id);

    // Audit trail has GroupAssigned record.
    assert!(pos.data.records.iter().any(|(rec, _)| matches!(
        rec,
        PositionRecord::GroupAssigned { group_id } if group_id == "signals_v1"
    )));
}

#[test]
fn open_without_group_no_assignment() {
    let mut engine = TradeEngine::new();
    let id = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, None);

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.group, None);

    // No GroupAssigned record in audit trail.
    assert!(
        !pos.data
            .records
            .iter()
            .any(|(rec, _)| matches!(rec, PositionRecord::GroupAssigned { .. }))
    );
}

#[test]
fn multiple_positions_same_group() {
    let mut engine = TradeEngine::new();
    let id1 = open_with_group(
        &mut engine,
        "EURUSD",
        Side::Buy,
        1.0850,
        1.0,
        Some("momentum"),
    );
    let id2 = open_with_group(
        &mut engine,
        "XAUUSD",
        Side::Sell,
        2000.0,
        1.0,
        Some("momentum"),
    );
    let id3 = open_with_group(
        &mut engine,
        "EURUSD",
        Side::Sell,
        1.0860,
        0.5,
        Some("momentum"),
    );

    let group_ids = engine.manager.group_position_ids("momentum");
    assert_eq!(group_ids.len(), 3);
    assert!(group_ids.contains(&id1));
    assert!(group_ids.contains(&id2));
    assert!(group_ids.contains(&id3));

    let open_ids = engine.manager.open_ids_by_group("momentum");
    assert_eq!(open_ids.len(), 3);
}

#[test]
fn close_all_in_group() {
    let mut engine = TradeEngine::new();
    let id_a1 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, Some("A"));
    let id_a2 = open_with_group(&mut engine, "XAUUSD", Side::Sell, 2000.0, 1.0, Some("A"));
    let id_b1 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0860, 1.0, Some("B"));

    let effects = engine
        .apply_action(
            Action::CloseAllInGroup {
                group_id: "A".into(),
            },
            ts(10, 5, 0),
        )
        .unwrap();

    // Two positions in group A closed.
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|e| matches!(
        e,
        Effect::PositionClosed {
            reason: CloseReason::GroupRule,
            ..
        }
    )));

    // Both group A positions are Closed.
    assert_eq!(
        engine.get_position(&id_a1).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
    assert_eq!(
        engine.get_position(&id_a2).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );

    // Group B position is still Open.
    assert_eq!(
        engine.get_position(&id_b1).unwrap().data.status,
        qs_core::PositionStatus::Open
    );
}

#[test]
fn close_all_in_group_skips_closed() {
    let mut engine = TradeEngine::new();
    let id1 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, Some("A"));
    let id2 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0860, 1.0, Some("A"));

    // Close one position manually first.
    engine
        .apply_action(
            Action::ClosePosition {
                position_id: id1.clone(),
            },
            ts(10, 3, 0),
        )
        .unwrap();

    // Now close all in group — only the still-open one should close.
    let effects = engine
        .apply_action(
            Action::CloseAllInGroup {
                group_id: "A".into(),
            },
            ts(10, 5, 0),
        )
        .unwrap();

    assert_eq!(effects.len(), 1);
    match &effects[0] {
        Effect::PositionClosed { id, reason } => {
            assert_eq!(id, &id2);
            assert_eq!(*reason, CloseReason::GroupRule);
        }
        other => panic!("expected PositionClosed, got {:?}", other),
    }
}

#[test]
fn close_all_in_group_empty_group() {
    let mut engine = TradeEngine::new();

    // Non-existent group — should return empty effects, not an error.
    let effects = engine
        .apply_action(
            Action::CloseAllInGroup {
                group_id: "nonexistent".into(),
            },
            ts(10, 0, 0),
        )
        .unwrap();

    assert!(effects.is_empty());
}

#[test]
fn modify_all_stoploss_in_group() {
    let mut engine = TradeEngine::new();

    // Open two Buy positions in group "scalp" with SL at 1.0800.
    let effects1 = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: Some("scalp".into()),
            },
            ts(10, 0, 0),
        )
        .unwrap();
    let id1 = match &effects1[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    let effects2 = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0855),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                rules: vec![],
                group: Some("scalp".into()),
            },
            ts(10, 0, 1),
        )
        .unwrap();
    let id2 = match &effects2[0] {
        Effect::PositionOpened { id } => id.clone(),
        _ => panic!(),
    };

    // Modify all SL in group to 1.0820.
    let effects = engine
        .apply_action(
            Action::ModifyAllStoplossInGroup {
                group_id: "scalp".into(),
                price: 1.0820,
            },
            ts(10, 1, 0),
        )
        .unwrap();

    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|e| matches!(
        e,
        Effect::StoplossModified {
            new_price,
            ..
        } if (*new_price - 1.0820).abs() < f64::EPSILON
    )));

    // Both positions now have SL at 1.0820.
    assert!(
        (engine
            .get_position(&id1)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0820)
            .abs()
            < f64::EPSILON
    );
    assert!(
        (engine
            .get_position(&id2)
            .unwrap()
            .current_stoploss()
            .unwrap()
            - 1.0820)
            .abs()
            < f64::EPSILON
    );

    // Audit trail has StoplossModified records.
    let pos1 = engine.get_position(&id1).unwrap();
    assert!(pos1.data.records.iter().any(|(rec, _)| matches!(
        rec,
        PositionRecord::StoplossModified { to, .. } if (*to - 1.0820).abs() < f64::EPSILON
    )));
}

#[test]
fn open_ids_by_group_filters_by_status() {
    let mut engine = TradeEngine::new();
    let id1 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, Some("G"));
    let _id2 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0860, 1.0, Some("G"));
    let _id3 = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0870, 1.0, Some("G"));

    // Close one position.
    engine
        .apply_action(
            Action::ClosePosition {
                position_id: id1.clone(),
            },
            ts(10, 1, 0),
        )
        .unwrap();

    // group_position_ids returns all 3 (includes closed).
    assert_eq!(engine.manager.group_position_ids("G").len(), 3);

    // open_ids_by_group returns only the 2 still open.
    assert_eq!(engine.manager.open_ids_by_group("G").len(), 2);
}

#[test]
fn all_group_ids_lists_groups() {
    let mut engine = TradeEngine::new();
    open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, Some("A"));
    open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0860, 1.0, Some("B"));
    open_with_group(&mut engine, "XAUUSD", Side::Sell, 2000.0, 1.0, Some("C"));

    let mut groups = engine
        .manager
        .all_group_ids()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    groups.sort();
    assert_eq!(groups, vec!["A", "B", "C"]);

    // Remove group B.
    engine.manager.remove_group("B");
    let mut groups = engine
        .manager
        .all_group_ids()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    groups.sort();
    assert_eq!(groups, vec!["A", "C"]);
}

#[test]
fn group_field_in_position_data_serde() {
    let mut engine = TradeEngine::new();
    let id = open_with_group(
        &mut engine,
        "EURUSD",
        Side::Buy,
        1.0850,
        1.0,
        Some("test_group"),
    );

    let pos = engine.get_position(&id).unwrap();
    let json = serde_json::to_string(&pos.data).unwrap();
    let deserialized: qs_core::position::PositionData = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.group, Some("test_group".to_owned()));
}

#[test]
fn mixed_group_and_ungrouped_positions() {
    let mut engine = TradeEngine::new();
    let id_grouped_1 =
        open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0850, 1.0, Some("alpha"));
    let id_grouped_2 = open_with_group(
        &mut engine,
        "EURUSD",
        Side::Sell,
        1.0860,
        1.0,
        Some("alpha"),
    );
    let id_ungrouped = open_with_group(&mut engine, "EURUSD", Side::Buy, 1.0870, 1.0, None);

    // Ungrouped positions not returned by group queries.
    let group_ids = engine.manager.open_ids_by_group("alpha");
    assert_eq!(group_ids.len(), 2);
    assert!(group_ids.contains(&id_grouped_1));
    assert!(group_ids.contains(&id_grouped_2));
    assert!(!group_ids.contains(&id_ungrouped));

    // CloseAllInGroup does not affect ungrouped positions.
    engine
        .apply_action(
            Action::CloseAllInGroup {
                group_id: "alpha".into(),
            },
            ts(10, 5, 0),
        )
        .unwrap();

    assert_eq!(
        engine.get_position(&id_grouped_1).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
    assert_eq!(
        engine.get_position(&id_grouped_2).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
    assert_eq!(
        engine.get_position(&id_ungrouped).unwrap().data.status,
        qs_core::PositionStatus::Open
    );

    // CloseAll still closes everything (group and ungrouped).
    engine.apply_action(Action::CloseAll, ts(10, 6, 0)).unwrap();
    assert_eq!(
        engine.get_position(&id_ungrouped).unwrap().data.status,
        qs_core::PositionStatus::Closed
    );
}

#[test]
fn pending_order_with_group() {
    let mut engine = TradeEngine::new();

    let effects = engine
        .apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(1.0800),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: Some("pending_group".into()),
            },
            ts(9, 0, 0),
        )
        .unwrap();

    let id = match &effects[0] {
        Effect::OrderPlaced { id } => id.clone(),
        other => panic!("expected OrderPlaced, got {:?}", other),
    };

    let pos = engine.get_position(&id).unwrap();
    assert_eq!(pos.data.group, Some("pending_group".to_owned()));
    assert_eq!(engine.manager.group_position_ids("pending_group").len(), 1);
    assert_eq!(
        engine.manager.pending_ids_by_group("pending_group").len(),
        1
    );
    assert_eq!(engine.manager.open_ids_by_group("pending_group").len(), 0);

    // Audit trail has GroupAssigned record.
    assert!(pos.data.records.iter().any(|(rec, _)| matches!(
        rec,
        PositionRecord::GroupAssigned { group_id } if group_id == "pending_group"
    )));
}

#[test]
fn modify_all_stoploss_in_group_empty_group() {
    let mut engine = TradeEngine::new();

    let effects = engine
        .apply_action(
            Action::ModifyAllStoplossInGroup {
                group_id: "nonexistent".into(),
                price: 1.0820,
            },
            ts(10, 0, 0),
        )
        .unwrap();

    assert!(effects.is_empty());
}
