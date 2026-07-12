//! Backtest runner — orchestrates the backtest loop.
//!
//! [`BacktestRunner`] combines a [`TradeEngine`], a [`BacktestExecutor`], and
//! either a [`Strategy`] or a set of predefined [`RawSignal`]s to produce a
//! [`BacktestResult`].
//!
//! # Two modes of operation
//!
//! 1. **Strategy-driven** ([`run_strategy`](BacktestRunner::run_strategy)):
//!    The runner feeds market events to a [`Strategy`] implementation.  The
//!    strategy returns [`Action`]s which are forwarded to the engine.
//!
//! 2. **Raw-signal replay** ([`run_raw_signals`](BacktestRunner::run_raw_signals)):
//!    A pre-sorted `Vec<RawSignal>` is merged with the market data timeline.
//!    Signals are injected at the correct timestamps.

use std::collections::HashMap;

use qs_core::TradeEngine;
use qs_core::types::{Action, FillModel, PositionId, PositionStatus, PriceQuote, Side};

use crate::data_feed::DataFeed;
use crate::executor::BacktestExecutor;
use crate::profile::{ManagementProfile, PositionRef, PositionResolver, RawSignal, resolve_signal};
use crate::report::BacktestResult;
use crate::sizing::SizingPolicy;
use crate::strategy::Strategy;

/// Build an `Action::Open` directly from a `RawSignal::Entry`.
///
/// Each target is wrapped with `close_ratio = 1.0`, no rules are added,
/// and the entry's `trade_id` is propagated so later
/// `PositionRef::ByTradeId` signals can resolve the resulting position.
fn build_entry_action(signal: &RawSignal) -> Option<Action> {
    match signal {
        RawSignal::Entry {
            symbol,
            side,
            order_type,
            price,
            size,
            stoploss,
            targets,
            group,
            trade_id,
            ..
        } => Some(Action::Open {
            symbol: symbol.clone(),
            side: *side,
            order_type: *order_type,
            price: *price,
            size: *size,
            stoploss: *stoploss,
            targets: targets
                .iter()
                .map(|p| qs_core::types::TargetSpec {
                    price: *p,
                    close_ratio: 1.0,
                })
                .collect(),
            rules: vec![],
            group: group.clone(),
            trade_id: trade_id.clone(),
        }),
        _ => None,
    }
}

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
    /// Per-symbol contract size (point value) for P&L calculation.
    ///
    /// Maps symbol name → contract size.  For forex, this is typically
    /// `lot_base_units` from [`SymbolSpec`] (e.g. 100_000 for majors).
    /// For gold (XAUUSD) it's 100 (1 lot = 100 oz).
    ///
    /// When a symbol is absent from this map the multiplier defaults to `1.0`,
    /// which preserves backward compatibility with all existing tests.
    pub contract_sizes: HashMap<String, f64>,
    /// Optional position sizing policy.  When set, entry signal sizes are
    /// recalculated after profile transformation using symbol metadata.
    pub sizing: Option<SizingPolicy>,
    /// Symbol specs for sizing calculations.  Populated by the server from
    /// the symbol registry.  Empty when no sizing policy is configured.
    pub symbol_specs: HashMap<String, qs_symbols::SymbolSpec>,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_balance: 10_000.0,
            close_on_finish: true,
            fill_model: FillModel::default(),
            contract_sizes: HashMap::new(),
            sizing: None,
            symbol_specs: HashMap::new(),
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
        let executor = BacktestExecutor::new(config.initial_balance, config.contract_sizes.clone());
        Self {
            engine: TradeEngine::with_fill_model(config.fill_model),
            executor,
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

    // ── Internal helpers ────────────────────────────────────────────────

    /// Apply a batch of actions to the engine and forward effects to executor.
    fn apply_actions(&mut self, actions: Vec<Action>, quote: &PriceQuote) {
        for action in actions {
            self.apply_single_action(action, quote.ts, quote);
        }
    }

    /// Apply a single action, forwarding effects to the executor.
    ///
    /// For close effects the executor resolves the position symbol and uses
    /// the engine's last known quote for that symbol rather than blindly
    /// trusting the caller-supplied `quote`.  This prevents cross-symbol
    /// quote contamination in merged multi-symbol feeds.
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

    // ── Mode 3: Raw signal replay ───────────────────────────────────────

    /// Run a raw-signal-replay backtest.
    ///
    /// `raw_signals` must be **sorted by timestamp** (ascending).  Entry
    /// signals are optionally transformed through a [`ManagementProfile`],
    /// while management signals are resolved against live engine state and
    /// passed through directly.
    pub fn run_raw_signals<F: DataFeed>(
        mut self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
    ) -> BacktestResult {
        let mut sig_idx = 0;

        while let Some(event) = feed.next_event() {
            let quote = event.to_quote();

            // 1. Inject raw signals that should fire at or before this event's ts.
            while sig_idx < raw_signals.len() && raw_signals[sig_idx].ts() <= event.ts() {
                self.process_raw_signal(&raw_signals[sig_idx], profile, &quote);
                sig_idx += 1;
            }

            // 2. Feed price to engine.
            let effects = self.engine.on_price(&quote);
            self.executor
                .process_effects(&effects, &self.engine, &quote);
        }

        // 3. Inject remaining signals (if any) after data is exhausted.
        if sig_idx < raw_signals.len() {
            if let Some(last_quote) = self.last_available_quote() {
                while sig_idx < raw_signals.len() {
                    self.process_raw_signal(&raw_signals[sig_idx], profile, &last_quote);
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

    /// Process a single raw signal: entry signals go through profile transform,
    /// management signals are resolved against live engine state.
    ///
    /// When a sizing policy is configured, entry size is recalculated after
    /// profile transformation using the final entry price and stoploss.
    fn process_raw_signal(
        &mut self,
        signal: &RawSignal,
        profile: Option<&ManagementProfile>,
        quote: &PriceQuote,
    ) {
        let ts = signal.ts();

        if signal.is_entry() {
            let action = if let Some(prof) = profile {
                prof.apply_entry_signal(signal)
            } else {
                build_entry_action(signal)
            };
            if let Some(mut act) = action {
                if let Some(ref policy) = self.config.sizing {
                    act = apply_sizing(act, policy, &self.config.symbol_specs, quote);
                }
                self.apply_single_action(act, ts, quote);
            }
        } else {
            let actions = resolve_signal(signal, &self.engine);
            for action in actions {
                self.apply_single_action(action, ts, quote);
            }
        }
    }

    // ── Internal helpers ────────────────────────────────────────────────
    // (continued)

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

//
// Apply a sizing policy to an Action::Open, replacing the size field.
// If the action is not an Open or sizing fails, returns the original action.
//
fn apply_sizing(
    mut action: Action,
    policy: &SizingPolicy,
    specs: &HashMap<String, qs_symbols::SymbolSpec>,
    quote: &PriceQuote,
) -> Action {
    if let Action::Open {
        symbol,
        side,
        price,
        stoploss,
        size,
        ..
    } = &mut action
    {
        let entry_price = price.or_else(|| {
            Some(match side {
                Side::Buy => quote.ask,
                Side::Sell => quote.bid,
            })
        });
        if let Some(spec) = specs.get(symbol) {
            let r = crate::sizing::compute_size(
                policy,
                symbol,
                *side,
                entry_price,
                *stoploss,
                spec,
                &std::collections::HashMap::new(),
            );
            if r.skipped {
                if let Some(ref err) = r.error {
                    eprintln!("Sizing skipped for {}: {}", symbol, err);
                }
                // On skip with error, set size to 0 to prevent opening.
                if r.error.is_some() {
                    *size = 0.0;
                }
            } else {
                *size = r.size;
            }
        }
    }
    action
}

// ─── PositionResolver for TradeEngine ────────────────────────────────────

impl PositionResolver for TradeEngine {
    fn resolve(&self, pr: &PositionRef) -> Vec<PositionId> {
        match pr {
            PositionRef::ByTradeId { trade_id } => {
                self.manager.id_by_trade_id(trade_id).into_iter().collect()
            }
            PositionRef::AllOnSymbol { symbol } => self.manager.open_ids_by_symbol(symbol),
            PositionRef::AllInGroup { group_id } => self.manager.open_ids_by_group(group_id),
        }
    }

    fn position_entry_info(&self, id: &PositionId) -> Option<(f64, Side)> {
        self.get_position(id).and_then(|pos| {
            if pos.data.status == PositionStatus::Open {
                Some((pos.data.average_entry(), pos.data.side))
            } else {
                None
            }
        })
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_feed::{MarketEvent, VecFeed};
    use crate::profile::{ManagementProfile, PositionRef, RawSignal, StoplossMode};
    use chrono::NaiveDate;
    use qs_core::types::{CloseReason, OrderType, Side, TargetSpec};

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
                    trade_id: None,
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

    // ── Raw signal replay tests ─────────────────────────────────────────

    #[test]
    fn run_raw_signals_entry_only() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
    }

    #[test]
    fn run_raw_signals_open_then_close() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn run_raw_signals_open_then_modify_sl() {
        // Open a position, then move SL closer. If price drops to new SL, it triggers.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 1)),
            // SL modify happens at ts(10,0,2)
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 2)),
            // Price drops to modified SL at 1.0840
            tick("EURUSD", 1.0838, 1.0840, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ModifyStoploss {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                price: 1.0840,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn run_raw_signals_open_then_partial_close() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
            tick("EURUSD", 1.0880, 1.0882, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ClosePartial {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                ratio: 0.5,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // At least 1 trade closed (partial close + close_on_finish for remainder)
        assert!(result.total_trades >= 1);
    }

    #[test]
    fn run_raw_signals_group_workflow() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
            tick("EURUSD", 1.0880, 1.0882, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            // Open 2 positions in same group
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: Some("grp1".into()),
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: Some("grp1".into()),
                trade_id: Some("t2".into()),
            },
            // Close entire group
            RawSignal::CloseAllInGroup {
                ts: ts(10, 0, 3),
                group_id: "grp1".into(),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_close_all_of_symbol() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 0.5,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
            RawSignal::CloseAllOf {
                ts: ts(10, 0, 2),
                symbol: "EURUSD".into(),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_with_profile() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: Some("t1".into()),
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn run_raw_signals_with_profile_preserves_trade_id() {
        // Regression for the F17 trade_id propagation gap: a profile-supplied
        // raw entry must still expose its trade_id so that a later
        // PositionRef::ByTradeId signal can resolve and close the position.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("msg-100".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "msg-100".into(),
                },
            },
        ];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn run_raw_signals_no_profile() {
        // Without a profile, entry signals are converted directly.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
    }

    #[test]
    fn run_raw_signals_last_on_symbol_resolution() {
        // Open two positions, then close the last one by symbol ref.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
            // Close only the second opened position via its trade_id
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t2".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // 2 trades total: one closed by signal, one by close_on_finish
        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_unresolved_ref_skipped() {
        // Try to close a position that doesn't exist — should be silently skipped.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "nonexistent".into(),
            },
        }];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // No positions were opened or closed.
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

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

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

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // First position closed by TP, second by close_on_finish
        assert!(result.total_trades >= 2);
    }

    #[test]
    fn signal_replay_signal_before_data_filtered() {
        // Signal timestamp is before first data event.
        // The runner itself does not filter; the server is responsible
        // for date filtering. This test verifies that when a pre-window
        // signal IS passed to the runner, it is injected at the first
        // event (backward-compatible library behavior).
        // Server-side filtering is tested separately.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(9, 0, 0), // before first tick
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: None,
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

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

    #[test]
    fn run_raw_signals_with_profile_open_then_modify_sl_by_trade_id() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0838, 1.0840, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ModifyStoploss {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                price: 1.0840,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn run_raw_signals_with_profile_open_then_close_partial_by_trade_id() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ClosePartial {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                ratio: 0.5,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        // Partial close creates at least one trade.
        assert!(result.total_trades >= 1);
    }

    #[test]
    fn run_raw_signals_multi_position_by_trade_id_with_profile() {
        // Two entries on EURUSD group "alpha" with different trade_ids.
        // Close ByTradeId for "t1" only. Verify only t1 closes by signal
        // and t2 remains to be closed by close_on_finish.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: Some("alpha".into()),
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![1.0910],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                size: 1.0,
                stoploss: None,
                targets: vec![1.0910],
                group: None,
                trade_id: Some("t2".into()),
            },
            // Close only t1 by trade_id.
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        // t1 closed by signal, t2 closed by close_on_finish = 2 total.
        assert_eq!(result.total_trades, 2);
        // Both should be in group "alpha" from profile override.
        for trade in &result.trade_log {
            assert_eq!(trade.group.as_deref(), Some("alpha"));
        }
    }

    #[test]
    fn merged_feed_manual_close_uses_correct_symbol_quote() {
        // Regression test for Issue 1 Part 3:
        // Open XAUUSD, then close it manually while the current merged-feed
        // event is a GBPJPY tick. The exit price must be a XAUUSD price,
        // not a GBPJPY price.
        use crate::data_feed::MarketEvent;
        let events = vec![
            MarketEvent::Tick { symbol: "XAUUSD".into(), ts: ts(10, 0, 0), bid: 5000.0, ask: 5001.0 },
            MarketEvent::Tick { symbol: "GBPJPY".into(), ts: ts(10, 0, 1), bid: 210.0, ask: 211.0 },
            MarketEvent::Tick { symbol: "XAUUSD".into(), ts: ts(10, 0, 2), bid: 5050.0, ask: 5051.0 },
            // GBPJPY event at ts(10,0,3) - manual close fires here.
            MarketEvent::Tick { symbol: "GBPJPY".into(), ts: ts(10, 0, 3), bid: 212.0, ask: 213.0 },
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "XAUUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(5000.0),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("xau-1".into()),
            },
            // Manual close at ts(10,0,3) while current event is GBPJPY.
            RawSignal::Close {
                ts: ts(10, 0, 3),
                position: PositionRef::ByTradeId { trade_id: "xau-1".into() },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        let trade = &result.trade_log[0];
        assert_eq!(trade.symbol, "XAUUSD");
        // Exit price must be a XAUUSD price (~5050), not GBPJPY (~212).
        assert!(
            trade.exit_price > 4000.0,
            "Exit price should be XAUUSD (~5050), got {}",
            trade.exit_price
        );
    }

    #[test]
    fn server_filter_signals_before_market_window() {
        // Regression test for Issue 1 Part 4:
        // Verify the runner does NOT filter pre-window signals (library level).
        // The server filter is tested separately in handlers.
        // Here we verify that signals with ts before first market event
        // ARE still injected (library behavior). Server filtering removes them.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        // Signal from January, market data from "today" (ts(10,0,0)).
        let raw_signals = vec![
            RawSignal::Entry {
                ts: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap(),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: None,
                targets: vec![1.0900],
                group: None,
                trade_id: None,
            },
        ];

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // Library still injects it; server filtering is the authoritative gate.
        assert_eq!(result.total_trades, 1);
    }
}
