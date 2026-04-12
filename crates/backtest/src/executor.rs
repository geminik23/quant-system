//! Backtest executor — simulates fills and tracks P&L.
//!
//! The [`BacktestExecutor`] receives [`Effect`]s produced by the trade engine
//! and translates them into simulated trade results (entries, exits, P&L).
//! It maintains a map of open entries so that when a position closes it can
//! compute realised profit/loss.
//!
//! ## Contract sizes
//!
//! P&L is calculated as `(exit - entry) * close_size * contract_size`.
//! The `contract_size` (also called "point value") converts lot-denominated
//! sizes into monetary units.  For forex, 1 standard lot = 100,000 base
//! currency units, so the contract size is 100,000.  For gold (XAUUSD),
//! 1 lot = 100 troy ounces, so the contract size is 100.
//!
//! When no contract size is provided for a symbol the multiplier defaults
//! to `1.0`, preserving backward compatibility with existing tests.

use std::collections::HashMap;

use chrono::NaiveDateTime;

use qs_core::TradeEngine;
use qs_core::types::{CloseReason, Effect, GroupId, PositionId, PriceQuote, Side};

use crate::report::TradeResult;

// ─── Open entry tracking ────────────────────────────────────────────────────

/// Snapshot of a position at the time it opened.  Kept until the position
/// closes so that P&L can be computed.
#[derive(Debug, Clone)]
struct OpenEntry {
    symbol: String,
    side: Side,
    entry_price: f64,
    /// Original position size at open (never mutated after scale-in updates).
    original_size: f64,
    /// Remaining size after partial closes.
    remaining_size: f64,
    open_ts: NaiveDateTime,
    /// Group this position belongs to (propagated to TradeResult on close).
    group: Option<GroupId>,
}

// ─── BacktestExecutor ───────────────────────────────────────────────────────

/// Simulates trade execution and tracks account balance / P&L.
///
/// The executor does **not** own the [`TradeEngine`] — instead it receives
/// effects and a reference to the engine after each price update or action.
#[derive(Debug, Clone)]
pub struct BacktestExecutor {
    /// Starting account balance.
    pub initial_balance: f64,
    /// Current account balance (initial + realised P&L).
    pub balance: f64,
    /// Per-close trade results collected over the backtest.
    pub trade_log: Vec<TradeResult>,
    /// Currently tracked open entries (position_id → entry snapshot).
    open_entries: HashMap<PositionId, OpenEntry>,
    /// Per-symbol contract size (point value) for P&L calculation.
    /// Missing symbols default to 1.0.
    contract_sizes: HashMap<String, f64>,
}

impl BacktestExecutor {
    /// Create a new executor with the given starting balance and contract sizes.
    ///
    /// `contract_sizes` maps symbol name → contract size (e.g. 100_000 for forex).
    /// Pass an empty map to get the legacy behaviour (multiplier = 1.0).
    pub fn new(initial_balance: f64, contract_sizes: HashMap<String, f64>) -> Self {
        Self {
            initial_balance,
            balance: initial_balance,
            trade_log: Vec::new(),
            open_entries: HashMap::new(),
            contract_sizes,
        }
    }

    /// Process a batch of effects produced by the engine.
    ///
    /// `engine` is passed by reference so that the executor can look up
    /// position details (e.g. entry price, side) when recording opens.
    /// `quote` is the current market price used for close-price calculation.
    pub fn process_effects(
        &mut self,
        effects: &[Effect],
        engine: &TradeEngine,
        quote: &PriceQuote,
    ) {
        for effect in effects {
            match effect {
                // ── Position opened: record the entry ───────────────
                Effect::PositionOpened { id } => {
                    if let Some(pos) = engine.get_position(id) {
                        let filled = pos.data.total_filled_size();
                        self.open_entries.insert(
                            id.clone(),
                            OpenEntry {
                                symbol: pos.data.symbol.clone(),
                                side: pos.data.side,
                                entry_price: pos.data.average_entry(),
                                original_size: filled,
                                remaining_size: filled,
                                open_ts: pos.data.open_ts.unwrap_or(quote.ts),
                                group: pos.data.group.clone(),
                            },
                        );
                    }
                }

                // ── Position fully closed ───────────────────────────
                Effect::PositionClosed { id, reason } => {
                    self.record_close(id, 1.0, *reason, quote);
                }

                // ── Partial close ───────────────────────────────────
                Effect::PartialClose { id, ratio, reason } => {
                    self.record_close(id, *ratio, *reason, quote);
                }

                // ── Scale-in: update the tracked entry ──────────────
                Effect::ScaledIn { id, .. } => {
                    if let Some(pos) = engine.get_position(id) {
                        if let Some(entry) = self.open_entries.get_mut(id) {
                            entry.entry_price = pos.data.average_entry();
                            let new_total = pos.data.total_filled_size();
                            let added = new_total - entry.original_size;
                            entry.original_size = new_total;
                            entry.remaining_size += added;
                        }
                    }
                }

                // Other effects are informational — no P&L impact.
                _ => {}
            }
        }
    }

    /// Realised P&L so far.
    pub fn realized_pnl(&self) -> f64 {
        self.trade_log.iter().map(|t| t.pnl).sum()
    }

    /// Number of tracked open entries.
    pub fn open_count(&self) -> usize {
        self.open_entries.len()
    }

    // ── Internal ────────────────────────────────────────────────────────

    /// Record a close (full or partial) and compute P&L.
    fn record_close(
        &mut self,
        position_id: &str,
        close_ratio: f64,
        reason: CloseReason,
        quote: &PriceQuote,
    ) {
        // For a full close (ratio == 1.0) we remove the entry; for partial
        // we keep it and reduce the tracked size.
        let is_full = (close_ratio - 1.0).abs() < f64::EPSILON
            || reason == CloseReason::Stoploss
            || reason == CloseReason::TrailingStop
            || reason == CloseReason::TimeExit
            || reason == CloseReason::BreakevenStop;

        let entry = if is_full {
            self.open_entries.remove(position_id)
        } else {
            self.open_entries.get(position_id).cloned()
        };

        let Some(entry) = entry else {
            return;
        };

        let exit_price = quote.close_price(entry.side);
        // close_ratio is always relative to the *original* position size,
        // so compute close_size from original_size.  For full closes, use
        // remaining_size to capture everything that's left.
        let close_size = if is_full {
            entry.remaining_size
        } else {
            entry.original_size * close_ratio
        };

        let cs = self
            .contract_sizes
            .get(&entry.symbol)
            .copied()
            .unwrap_or(1.0);

        let pnl = match entry.side {
            Side::Buy => (exit_price - entry.entry_price) * close_size * cs,
            Side::Sell => (entry.entry_price - exit_price) * close_size * cs,
        };

        self.balance += pnl;

        self.trade_log.push(TradeResult {
            position_id: position_id.to_owned(),
            symbol: entry.symbol.clone(),
            side: entry.side,
            entry_price: entry.entry_price,
            exit_price,
            size: close_size,
            pnl,
            open_ts: entry.open_ts,
            close_ts: quote.ts,
            close_reason: reason,
            group: entry.group.clone(),
        });

        // If partial, reduce the remaining size for future closes.
        if !is_full {
            if let Some(tracked) = self.open_entries.get_mut(position_id) {
                tracked.remaining_size -= close_size;
                if tracked.remaining_size <= f64::EPSILON {
                    self.open_entries.remove(position_id);
                }
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_core::types::{Action, OrderType, Side, TargetSpec};
    use std::collections::HashMap;

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn make_quote(symbol: &str, bid: f64, ask: f64, time: NaiveDateTime) -> PriceQuote {
        PriceQuote {
            symbol: symbol.into(),
            ts: time,
            bid,
            ask,
        }
    }

    #[test]
    fn tracks_open_and_full_close_pnl() {
        let mut engine = TradeEngine::new();
        let mut exec = BacktestExecutor::new(10_000.0, HashMap::new());

        // Open a buy
        let open_quote = make_quote("EURUSD", 1.0848, 1.0850, ts(10, 0, 0));
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
        exec.process_effects(&effects, &engine, &open_quote);
        assert_eq!(exec.open_count(), 1);

        // SL triggers
        let sl_quote = make_quote("EURUSD", 1.0799, 1.0801, ts(10, 5, 0));
        let effects = engine.on_price(&sl_quote);
        exec.process_effects(&effects, &engine, &sl_quote);

        assert_eq!(exec.open_count(), 0);
        assert_eq!(exec.trade_log.len(), 1);

        let trade = &exec.trade_log[0];
        assert_eq!(trade.close_reason, CloseReason::Stoploss);
        // P&L = (bid - entry) * size = (1.0799 - 1.0850) * 1.0 = -0.0051
        assert!((trade.pnl - (-0.0051)).abs() < 1e-10);
        assert!((exec.balance - (10_000.0 - 0.0051)).abs() < 1e-10);
    }

    #[test]
    fn tracks_partial_close() {
        let mut engine = TradeEngine::new();
        let mut exec = BacktestExecutor::new(10_000.0, HashMap::new());

        let open_quote = make_quote("EURUSD", 1.0848, 1.0850, ts(10, 0, 0));
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
        exec.process_effects(&effects, &engine, &open_quote);

        // TP1 hit: partial close 50%
        let tp_quote = make_quote("EURUSD", 1.0900, 1.0902, ts(10, 5, 0));
        let effects = engine.on_price(&tp_quote);
        exec.process_effects(&effects, &engine, &tp_quote);

        assert_eq!(exec.trade_log.len(), 1);
        let partial = &exec.trade_log[0];
        // P&L = (1.0900 - 1.0850) * (2.0 * 0.5) = 0.0050 * 1.0 = 0.005
        assert!((partial.pnl - 0.005).abs() < 1e-10);
        assert_eq!(partial.close_reason, CloseReason::Target);

        // Entry still tracked (remaining size = 1.0)
        assert_eq!(exec.open_count(), 1);

        // SL hit: close remaining
        let sl_quote = make_quote("EURUSD", 1.0799, 1.0801, ts(10, 10, 0));
        let effects = engine.on_price(&sl_quote);
        exec.process_effects(&effects, &engine, &sl_quote);

        assert_eq!(exec.trade_log.len(), 2);
        assert_eq!(exec.open_count(), 0);
        let remaining = &exec.trade_log[1];
        // remaining_size = 2.0 - 1.0 = 1.0; P&L = (1.0799 - 1.0850) * 1.0 = -0.0051
        assert!((remaining.pnl - (-0.0051)).abs() < 1e-10);
    }

    #[test]
    fn two_targets_partial_close_pnl() {
        // Open: size=2.0, entry at ask=1.0850 (BidAsk model, Buy side)
        // TP1: price=1.0900, close_ratio=0.3  → close 0.6 lots
        // TP2: price=1.0950, close_ratio=0.3  → close 0.6 lots
        // SL:  price=1.0800                    → close remaining 0.8 lots
        let mut engine = TradeEngine::new();
        let mut exec = BacktestExecutor::new(10_000.0, HashMap::new());

        let open_quote = make_quote("EURUSD", 1.0848, 1.0850, ts(10, 0, 0));
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 2.0,
                    stoploss: Some(1.0800),
                    targets: vec![
                        TargetSpec {
                            price: 1.0900,
                            close_ratio: 0.3,
                        },
                        TargetSpec {
                            price: 1.0950,
                            close_ratio: 0.3,
                        },
                    ],
                    rules: vec![],
                    group: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        exec.process_effects(&effects, &engine, &open_quote);
        assert_eq!(exec.open_count(), 1);

        // ── TP1 hits ────────────────────────────────────────────────────
        let tp1_quote = make_quote("EURUSD", 1.0900, 1.0902, ts(10, 5, 0));
        let effects = engine.on_price(&tp1_quote);
        exec.process_effects(&effects, &engine, &tp1_quote);

        assert_eq!(exec.trade_log.len(), 1);
        let tp1 = &exec.trade_log[0];
        assert_eq!(tp1.close_reason, CloseReason::Target);
        // close_size = original 2.0 * 0.3 = 0.6
        assert!(
            (tp1.size - 0.6).abs() < 1e-10,
            "TP1 size: expected 0.6, got {}",
            tp1.size
        );
        // pnl = (1.0900 - 1.0850) * 0.6 = 0.003
        assert!(
            (tp1.pnl - 0.003).abs() < 1e-10,
            "TP1 pnl: expected 0.003, got {}",
            tp1.pnl
        );
        assert_eq!(exec.open_count(), 1);

        // ── TP2 hits ────────────────────────────────────────────────────
        let tp2_quote = make_quote("EURUSD", 1.0950, 1.0952, ts(10, 10, 0));
        let effects = engine.on_price(&tp2_quote);
        exec.process_effects(&effects, &engine, &tp2_quote);

        assert_eq!(
            exec.trade_log.len(),
            2,
            "Expected 2 trades after TP2, got {}",
            exec.trade_log.len()
        );
        let tp2 = &exec.trade_log[1];
        assert_eq!(tp2.close_reason, CloseReason::Target);
        // close_size = original 2.0 * 0.3 = 0.6 (NOT 1.4 * 0.3 = 0.42)
        assert!(
            (tp2.size - 0.6).abs() < 1e-10,
            "TP2 size: expected 0.6, got {}",
            tp2.size
        );
        // pnl = (1.0950 - 1.0850) * 0.6 = 0.006
        assert!(
            (tp2.pnl - 0.006).abs() < 1e-10,
            "TP2 pnl: expected 0.006, got {}",
            tp2.pnl
        );
        assert_eq!(exec.open_count(), 1);

        // ── SL hits — close remaining 0.8 lots ─────────────────────────
        let sl_quote = make_quote("EURUSD", 1.0799, 1.0801, ts(10, 15, 0));
        let effects = engine.on_price(&sl_quote);
        exec.process_effects(&effects, &engine, &sl_quote);

        assert_eq!(exec.trade_log.len(), 3);
        assert_eq!(exec.open_count(), 0);
        let sl = &exec.trade_log[2];
        assert_eq!(sl.close_reason, CloseReason::Stoploss);
        // remaining = 2.0 - 0.6 - 0.6 = 0.8
        assert!(
            (sl.size - 0.8).abs() < 1e-10,
            "SL size: expected 0.8, got {}",
            sl.size
        );
        // pnl = (1.0799 - 1.0850) * 0.8 = -0.00408
        assert!(
            (sl.pnl - (-0.00408)).abs() < 1e-10,
            "SL pnl: expected -0.00408, got {}",
            sl.pnl
        );

        // Total: 0.003 + 0.006 - 0.00408 = 0.00492
        let total_pnl: f64 = exec.trade_log.iter().map(|t| t.pnl).sum();
        assert!(
            (total_pnl - 0.00492).abs() < 1e-10,
            "Total pnl: expected 0.00492, got {}",
            total_pnl
        );
    }

    #[test]
    fn sell_position_pnl() {
        let mut engine = TradeEngine::new();
        let mut exec = BacktestExecutor::new(10_000.0, HashMap::new());

        let open_quote = make_quote("XAUUSD", 1999.0, 2000.0, ts(10, 0, 0));
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "XAUUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: Some(2000.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        exec.process_effects(&effects, &engine, &open_quote);

        // Close manually
        let close_quote = make_quote("XAUUSD", 1979.0, 1980.0, ts(10, 5, 0));
        engine.on_price(&close_quote); // seed last quote
        let effects = engine
            .apply_action(
                Action::ClosePosition {
                    position_id: exec.open_entries.keys().next().unwrap().clone(),
                },
                ts(10, 5, 0),
            )
            .unwrap();
        exec.process_effects(&effects, &engine, &close_quote);

        assert_eq!(exec.trade_log.len(), 1);
        // Sell P&L = (entry - exit_ask) * size = (2000 - 1980) * 1 = 20
        assert!((exec.trade_log[0].pnl - 20.0).abs() < 1e-10);
    }

    #[test]
    fn scale_in_updates_entry() {
        let mut engine = TradeEngine::new();
        let mut exec = BacktestExecutor::new(10_000.0, HashMap::new());

        let q1 = make_quote("EURUSD", 1.0848, 1.0850, ts(10, 0, 0));
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0800),
                    size: 1.0,
                    stoploss: None,
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
        exec.process_effects(&effects, &engine, &q1);

        // Scale in
        let q2 = make_quote("EURUSD", 1.0898, 1.0900, ts(10, 5, 0));
        let effects = engine
            .apply_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: Some(1.0900),
                    size: 1.0,
                },
                ts(10, 5, 0),
            )
            .unwrap();
        exec.process_effects(&effects, &engine, &q2);

        // Check that the tracked entry now has averaged price and combined size
        let entry = exec.open_entries.get(&id).unwrap();
        assert!((entry.entry_price - 1.0850).abs() < 1e-10); // (1.08+1.09)/2
        assert!((entry.original_size - 2.0).abs() < f64::EPSILON);
        assert!((entry.remaining_size - 2.0).abs() < f64::EPSILON);
    }
}
