//! Backtest runner — orchestrates the backtest loop.
//!
//! [`BacktestRunner`] combines a [`TradeEngine`], a [`BacktestExecutor`], and
//! either a [`Strategy`] or a set of predefined [`Signal`]s to produce a
//! [`BacktestResult`].
//!
//! # Two modes of operation
//!
//! 1. **Strategy-driven** ([`run_strategy`](BacktestRunner::run_strategy)):
//!    The runner feeds market events to a [`Strategy`] implementation.  The
//!    strategy returns [`Action`]s which are forwarded to the engine.
//!
//! 2. **Signal replay** ([`run_signals`](BacktestRunner::run_signals)):
//!    A pre-sorted `Vec<Signal>` is merged with the market data timeline.
//!    Signals are injected at the correct timestamps.

use qs_core::TradeEngine;
use qs_core::types::{Action, FillModel, PriceQuote, Signal};

use crate::data_feed::DataFeed;
use crate::executor::BacktestExecutor;
use crate::report::BacktestResult;
use crate::strategy::Strategy;

/// Configuration for a backtest run.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Starting account balance.
    pub initial_balance: f64,
    /// If `true`, all remaining open positions are closed at market when the
    /// data feed is exhausted.
    pub close_on_finish: bool,
    /// How fill conditions and rule triggers interpret price quotes.
    ///
    /// Defaults to [`FillModel::BidAsk`] — the most realistic model that
    /// uses the appropriate side of the spread for each operation.
    pub fill_model: FillModel,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_balance: 10_000.0,
            close_on_finish: true,
            fill_model: FillModel::default(),
        }
    }
}

/// Orchestrates a backtest by driving the engine with data and actions.
pub struct BacktestRunner {
    engine: TradeEngine,
    executor: BacktestExecutor,
    config: BacktestConfig,
}

impl BacktestRunner {
    /// Create a new runner with the given configuration.
    pub fn new(config: BacktestConfig) -> Self {
        Self {
            engine: TradeEngine::with_fill_model(config.fill_model),
            executor: BacktestExecutor::new(config.initial_balance),
            config,
        }
    }

    /// Create a runner with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BacktestConfig::default())
    }

    /// Access the underlying engine (e.g. for inspection between runs).
    pub fn engine(&self) -> &TradeEngine {
        &self.engine
    }

    /// Access the underlying executor.
    pub fn executor(&self) -> &BacktestExecutor {
        &self.executor
    }

    // ── Mode 1: Strategy-driven ─────────────────────────────────────────

    /// Run a strategy-driven backtest.
    ///
    /// For every event in the data feed:
    /// 1. The event is converted to a [`PriceQuote`] and fed to the engine
    ///    (which checks pending fills and evaluates rules).
    /// 2. The strategy's [`on_event`](Strategy::on_event) is called; any
    ///    returned actions are applied to the engine.
    /// 3. All resulting effects are forwarded to the executor for P&L tracking.
    ///
    /// When the feed is exhausted, [`Strategy::on_finished`] is called for any
    /// final actions, and (if configured) remaining positions are closed.
    pub fn run_strategy<F: DataFeed, S: Strategy>(
        mut self,
        feed: &mut F,
        strategy: &mut S,
    ) -> BacktestResult {
        while let Some(event) = feed.next_event() {
            let quote = event.to_quote();

            // 1. Feed price to engine → pending fills + rule evaluation.
            let price_effects = self.engine.on_price(&quote);
            self.executor
                .process_effects(&price_effects, &self.engine, &quote);

            // 2. Strategy decides actions based on the event.
            let actions = strategy.on_event(&event);
            self.apply_actions(actions, &quote);
        }

        // 3. Strategy cleanup.
        let final_actions = strategy.on_finished();
        if !final_actions.is_empty() {
            // Use the last known quote for the final actions.  If we have
            // nothing, create a dummy — but in practice the feed will have
            // produced at least one event.
            if let Some(last_quote) = self.last_available_quote() {
                self.apply_actions(final_actions, &last_quote);
                // One more price tick so rules can fire after final actions.
                let effects = self.engine.on_price(&last_quote);
                self.executor
                    .process_effects(&effects, &self.engine, &last_quote);
            }
        }

        // 4. Force-close remaining if configured.
        self.close_remaining_if_configured();

        BacktestResult::from_trade_log(self.config.initial_balance, self.executor.trade_log)
    }

    // ── Mode 2: Signal replay ───────────────────────────────────────────

    /// Run a signal-replay backtest.
    ///
    /// `signals` must be **sorted by timestamp** (ascending).  For every event
    /// in the data feed:
    /// 1. All signals whose timestamp is ≤ the event timestamp are injected
    ///    into the engine.
    /// 2. The event is fed to the engine for pending fills + rule evaluation.
    /// 3. All effects are forwarded to the executor.
    ///
    /// After the feed is exhausted any remaining signals are still injected
    /// (using the last known quote).
    pub fn run_signals<F: DataFeed>(
        mut self,
        feed: &mut F,
        signals: Vec<Signal>,
    ) -> BacktestResult {
        let mut sig_idx = 0;

        while let Some(event) = feed.next_event() {
            let quote = event.to_quote();

            // 1. Inject signals that should fire at or before this event's ts.
            while sig_idx < signals.len() && signals[sig_idx].ts <= event.ts() {
                let signal = &signals[sig_idx];
                self.apply_single_action(signal.action.clone(), signal.ts, &quote);
                sig_idx += 1;
            }

            // 2. Feed price to engine.
            let effects = self.engine.on_price(&quote);
            self.executor
                .process_effects(&effects, &self.engine, &quote);
        }

        // 3. Inject remaining signals (if any) after data is exhausted.
        if sig_idx < signals.len() {
            if let Some(last_quote) = self.last_available_quote() {
                while sig_idx < signals.len() {
                    let signal = &signals[sig_idx];
                    self.apply_single_action(signal.action.clone(), signal.ts, &last_quote);
                    sig_idx += 1;
                }
                // One final price evaluation.
                let effects = self.engine.on_price(&last_quote);
                self.executor
                    .process_effects(&effects, &self.engine, &last_quote);
            }
        }

        // 4. Force-close remaining if configured.
        self.close_remaining_if_configured();

        BacktestResult::from_trade_log(self.config.initial_balance, self.executor.trade_log)
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /// Apply a batch of actions to the engine and forward effects to executor.
    fn apply_actions(&mut self, actions: Vec<Action>, quote: &PriceQuote) {
        for action in actions {
            self.apply_single_action(action, quote.ts, quote);
        }
    }

    /// Apply a single action, forwarding effects to the executor.
    fn apply_single_action(
        &mut self,
        action: Action,
        ts: chrono::NaiveDateTime,
        quote: &PriceQuote,
    ) {
        match self.engine.apply_action(action, ts) {
            Ok(effects) => {
                self.executor.process_effects(&effects, &self.engine, quote);
            }
            Err(_) => {
                // In backtesting we silently skip invalid actions (e.g.
                // trying to close a position that was already closed by SL).
                // A more sophisticated implementation could log these.
            }
        }
    }

    /// Try to find the last known quote from the engine (any symbol).
    fn last_available_quote(&self) -> Option<PriceQuote> {
        // Look up quotes for symbols that have open positions first, then
        // fall back to any known quote.
        for pos in self.engine.open_positions() {
            if let Some(q) = self.engine.last_quote(&pos.data.symbol) {
                return Some(q.clone());
            }
        }
        // No open positions — try closed ones.
        for pos in self.engine.closed_positions() {
            if let Some(q) = self.engine.last_quote(&pos.data.symbol) {
                return Some(q.clone());
            }
        }
        None
    }

    /// If `close_on_finish` is set, close all remaining open positions at
    /// their last known price.
    fn close_remaining_if_configured(&mut self) {
        if !self.config.close_on_finish {
            return;
        }

        let open_ids: Vec<String> = self
            .engine
            .open_positions()
            .iter()
            .map(|p| p.data.id.clone())
            .collect();

        for id in open_ids {
            let symbol = match self.engine.get_position(&id) {
                Some(pos) => pos.data.symbol.clone(),
                None => continue,
            };
            let quote = match self.engine.last_quote(&symbol) {
                Some(q) => q.clone(),
                None => continue,
            };

            if let Ok(effects) = self.engine.apply_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                quote.ts,
            ) {
                self.executor
                    .process_effects(&effects, &self.engine, &quote);
            }
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_feed::{MarketEvent, VecFeed};
    use chrono::NaiveDate;
    use qs_core::types::{CloseReason, OrderType, RuleConfig, Side, TargetSpec};

    fn ts(h: u32, m: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn tick(symbol: &str, bid: f64, ask: f64, time: chrono::NaiveDateTime) -> MarketEvent {
        MarketEvent::Tick {
            symbol: symbol.into(),
            ts: time,
            bid,
            ask,
        }
    }

    // ── Simple strategy for testing ─────────────────────────────────────

    /// Buys on the first tick, with SL and TP.
    struct BuyOnceStrategy {
        entered: bool,
    }

    impl BuyOnceStrategy {
        fn new() -> Self {
            Self { entered: false }
        }
    }

    impl Strategy for BuyOnceStrategy {
        fn on_event(&mut self, event: &MarketEvent) -> Vec<Action> {
            if self.entered {
                return vec![];
            }
            if let MarketEvent::Tick { symbol, ask, .. } = event {
                self.entered = true;
                vec![Action::Open {
                    symbol: symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(*ask),
                    size: 1.0,
                    stoploss: Some(*ask - 0.0050),
                    targets: vec![TargetSpec {
                        price: *ask + 0.0050,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                }]
            } else {
                vec![]
            }
        }

        fn on_finished(&mut self) -> Vec<Action> {
            // Don't close — let close_on_finish handle it if TP/SL haven't
            // triggered.
            vec![]
        }
    }

    // ── Strategy-driven tests ───────────────────────────────────────────

    #[test]
    fn strategy_backtest_tp_hit() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
            tick("EURUSD", 1.0890, 1.0892, ts(10, 0, 3)),
            // TP at 1.0900 (entry 1.0850 + 0.005)
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert!(result.total_pnl > 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn strategy_backtest_sl_hit() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0830, 1.0832, ts(10, 0, 1)),
            // SL at 1.0800 (entry 1.0850 - 0.005)
            tick("EURUSD", 1.0799, 1.0801, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.losing_trades, 1);
        assert!(result.total_pnl < 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn strategy_close_on_finish() {
        // Price never reaches TP or SL — position should be closed at end.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0852, 1.0854, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn strategy_no_close_on_finish() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        // Position left open — no trades recorded.
        assert_eq!(result.total_trades, 0);
    }

    // ── Signal replay tests ─────────────────────────────────────────────

    #[test]
    fn signal_replay_basic() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            // TP at 1.0900
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let signals = vec![Signal {
            ts: ts(10, 0, 0),
            action: Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![TargetSpec {
                    price: 1.0900,
                    close_ratio: 1.0,
                }],
                rules: vec![],
                group: None,
            },
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_signals(&mut feed, signals);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn signal_replay_multiple_signals() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            // TP1 hit for first position
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 2)),
            tick("EURUSD", 1.0910, 1.0912, ts(10, 0, 3)),
            tick("EURUSD", 1.0920, 1.0922, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);

        let signals = vec![
            Signal {
                ts: ts(10, 0, 0),
                action: Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: Some(1.0800),
                    targets: vec![TargetSpec {
                        price: 1.0900,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                },
            },
            // Second signal a bit later
            Signal {
                ts: ts(10, 0, 1),
                action: Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0857),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![RuleConfig::TimeExit { max_seconds: 60 }],
                    group: None,
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_signals(&mut feed, signals);

        // First position closed by TP, second by close_on_finish
        assert!(result.total_trades >= 2);
    }

    #[test]
    fn signal_replay_signal_before_data() {
        // Signal timestamp is before first data event — should still be
        // injected when the first event arrives.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let signals = vec![Signal {
            ts: ts(9, 0, 0), // before first tick
            action: Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![TargetSpec {
                    price: 1.0900,
                    close_ratio: 1.0,
                }],
                rules: vec![],
                group: None,
            },
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_signals(&mut feed, signals);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn empty_feed_empty_result() {
        let mut feed = VecFeed::new(vec![]);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 0);
        assert!((result.final_balance - 10_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn report_display_does_not_panic() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        // Just verify Display doesn't panic.
        let _display = format!("{}", result);
    }
}
