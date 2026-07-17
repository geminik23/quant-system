//! Trade engine — the main entry point for processing actions and price updates.
//!
//! [`TradeEngine`] is a **synchronous, side-effect-free** orchestrator.  It
//! owns a [`PositionManager`] and tracks the last known price per symbol.
//! All mutations flow through two methods:
//!
//! - [`apply_action`](TradeEngine::apply_action) — process a trading action
//!   (open, close, modify, …)
//! - [`on_price`](TradeEngine::on_price) — feed a new price quote, which
//!   triggers pending-fill checks and rule evaluation
//!
//! Both methods return `Vec<Effect>` that the caller (backtest runner, live
//! executor, …) handles according to its context.

use std::collections::HashMap;

use chrono::NaiveDateTime;
use nanoid::nanoid;
use thiserror::Error;

use crate::alert_register::{
    AlertKind, PriceAlertRegister, PriceAlertRegisterQuoteCheckpoint, TriggeredAlert,
};
use crate::error::{CoreError, Result};
use crate::execution::{ExecutionError, ExecutionPricer};
use crate::position::Position;
use crate::position_manager::{
    PositionManager, PositionManagerCheckpoint, PositionManagerError,
    PositionManagerQuoteCheckpoint,
};
use crate::rules::Rule;
use crate::types::{
    Action, CloseReason, Effect, ExecutionFill, ExecutionModel, Fill, FillModel, FillPurpose,
    FutureEffect, FutureFill, FutureIntent, OrderType, PositionId, PositionRecord, PositionStatus,
    PreparedPendingFill, PriceQuote, RuleConfig, Side, TargetSpec, position_size_tolerance,
};

/// Errors raised while preparing or atomically applying FutureQuote state.
#[derive(Debug, Error)]
pub enum FutureApplyError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Pricing(#[from] ExecutionError),
    #[error("invalid prepared fill for {position_id}: {reason}")]
    InvalidPreparedFill {
        position_id: PositionId,
        reason: String,
    },
}

pub type FutureApplyResult<T> = std::result::Result<T, FutureApplyError>;

/// Generate a short random id for new positions.
fn gen_id() -> PositionId {
    nanoid!(12)
}

struct OpenActionParams {
    symbol: String,
    side: Side,
    order_type: OrderType,
    price: Option<f64>,
    size: f64,
    stoploss: Option<f64>,
    targets: Vec<TargetSpec>,
    rules: Vec<crate::types::RuleConfig>,
    group: Option<String>,
    trade_id: Option<crate::types::TradeId>,
}

#[derive(Debug)]
enum TradeEngineCheckpoint {
    Action {
        manager: PositionManagerCheckpoint,
        alert_register: Option<PriceAlertRegister>,
        next_position_sequence: u64,
    },
    Quote {
        manager: PositionManagerQuoteCheckpoint,
        symbol: String,
        last_quote: Option<PriceQuote>,
        alert_register: Option<PriceAlertRegisterQuoteCheckpoint>,
    },
}

/// An in-place FutureQuote mutation that can be committed or rolled back.
#[derive(Debug)]
pub struct FutureEngineTransaction {
    effects: Vec<FutureEffect>,
    checkpoint: TradeEngineCheckpoint,
}

impl FutureEngineTransaction {
    /// Effects produced by the staged in-place mutation.
    pub fn effects(&self) -> &[FutureEffect] {
        &self.effects
    }

    /// Whether external effect processing can be skipped.
    pub fn has_effects(&self) -> bool {
        !self.effects.is_empty()
    }

    /// Keep the in-place engine mutation and return its effects.
    pub fn commit(self) -> Vec<FutureEffect> {
        self.effects
    }

    /// Restore the engine state captured before the mutation.
    pub fn rollback(self, engine: &mut TradeEngine) {
        engine.restore_checkpoint(self.checkpoint);
    }
}

/// The core trade engine.
///
/// Pure logic — no async, no IO.  Takes inputs, returns effects.
#[derive(Debug, Clone)]
pub struct TradeEngine {
    pub manager: PositionManager,
    last_quotes: HashMap<String, PriceQuote>,
    /// How fill conditions and rule triggers interpret price quotes.
    ///
    /// Defaults to [`FillModel::BidAsk`] (the most realistic model).
    pub fill_model: FillModel,
    /// Optional BTreeMap-indexed alert register for O(log N + K) rule evaluation.
    /// When `None`, the engine uses tick-by-tick evaluation for all rules.
    alert_register: Option<PriceAlertRegister>,
    deterministic_ids: bool,
    next_position_sequence: u64,
}

impl Default for TradeEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TradeEngine {
    /// Standard engine (tick-by-tick, no alert register). Best for backtesting.
    pub fn new() -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model: FillModel::default(),
            alert_register: None,
            deterministic_ids: false,
            next_position_sequence: 0,
        }
    }

    /// Create a new engine with a specific fill model.
    pub fn with_fill_model(fill_model: FillModel) -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model,
            alert_register: None,
            deterministic_ids: false,
            next_position_sequence: 0,
        }
    }

    /// Create a backtest engine whose position IDs are stable across runs.
    pub fn with_fill_model_and_deterministic_ids(fill_model: FillModel) -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model,
            alert_register: None,
            deterministic_ids: true,
            next_position_sequence: 0,
        }
    }

    /// Engine with alert register for indexed evaluation. Best for real-time with many positions.
    pub fn with_alert_register() -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model: FillModel::default(),
            alert_register: Some(PriceAlertRegister::new()),
            deterministic_ids: false,
            next_position_sequence: 0,
        }
    }

    /// Engine with both alert register and custom fill model.
    pub fn with_alert_register_and_fill_model(fill_model: FillModel) -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model,
            alert_register: Some(PriceAlertRegister::new()),
            deterministic_ids: false,
            next_position_sequence: 0,
        }
    }

    // ── Queries ─────────────────────────────────────────────────────────

    /// Last known quote for a symbol.
    pub fn last_quote(&self, symbol: &str) -> Option<&PriceQuote> {
        self.last_quotes.get(symbol)
    }

    /// Convenience: get a position by id.
    pub fn get_position(&self, id: &str) -> Option<&Position> {
        self.manager.get(id)
    }

    /// All currently open positions.
    pub fn open_positions(&self) -> Vec<&Position> {
        self.manager.open_positions()
    }

    /// All currently pending positions.
    pub fn pending_positions(&self) -> Vec<&Position> {
        self.manager.pending_positions()
    }

    /// All closed positions still tracked by the manager.
    pub fn closed_positions(&self) -> Vec<&Position> {
        self.manager.closed_positions()
    }

    fn checkpoint_for_action(&self, action: &Action) -> TradeEngineCheckpoint {
        self.checkpoint_for_positions(self.position_ids_for_action(action))
    }

    fn checkpoint_for_quote(&self, quote: &PriceQuote) -> TradeEngineCheckpoint {
        let symbol = quote.symbol.clone();
        TradeEngineCheckpoint::Quote {
            manager: self.manager.checkpoint_for_quote(&symbol),
            last_quote: self.last_quotes.get(&symbol).cloned(),
            alert_register: self
                .alert_register
                .as_ref()
                .map(|register| register.checkpoint_for_quote(&symbol)),
            symbol,
        }
    }

    fn checkpoint_for_positions(&self, position_ids: Vec<PositionId>) -> TradeEngineCheckpoint {
        TradeEngineCheckpoint::Action {
            manager: self.manager.checkpoint(position_ids),
            alert_register: self.alert_register.clone(),
            next_position_sequence: self.next_position_sequence,
        }
    }

    fn restore_checkpoint(&mut self, checkpoint: TradeEngineCheckpoint) {
        match checkpoint {
            TradeEngineCheckpoint::Action {
                manager,
                alert_register,
                next_position_sequence,
            } => {
                self.manager.restore(manager);
                self.alert_register = alert_register;
                self.next_position_sequence = next_position_sequence;
            }
            TradeEngineCheckpoint::Quote {
                manager,
                symbol,
                last_quote,
                alert_register,
            } => {
                self.manager.restore_quote(manager);
                match last_quote {
                    Some(quote) => {
                        self.last_quotes.insert(symbol.clone(), quote);
                    }
                    None => {
                        self.last_quotes.remove(&symbol);
                    }
                }
                if let Some(checkpoint) = alert_register {
                    self.alert_register
                        .as_mut()
                        .expect("quote transaction alert register must remain enabled")
                        .restore_quote(checkpoint);
                }
            }
        }
    }

    fn position_ids_for_action(&self, action: &Action) -> Vec<PositionId> {
        match action {
            Action::Open { .. } if self.deterministic_ids => {
                vec![format!("position:{:08}", self.next_position_sequence)]
            }
            Action::Open { .. } => Vec::new(),
            Action::ScaleIn { position_id, .. }
            | Action::ClosePosition { position_id }
            | Action::ClosePartial { position_id, .. }
            | Action::CancelPending { position_id }
            | Action::ModifyStoploss { position_id, .. }
            | Action::MoveStoplossToEntry { position_id }
            | Action::AddTarget { position_id, .. }
            | Action::RemoveTarget { position_id, .. }
            | Action::ModifyTarget { position_id, .. }
            | Action::AddRule { position_id, .. }
            | Action::RemoveRule { position_id, .. } => vec![position_id.clone()],
            Action::CloseAllOf { symbol } | Action::ModifyAllStoploss { symbol, .. } => {
                self.manager.open_ids_by_symbol_sorted(symbol)
            }
            Action::CloseAll => self.manager.all_open_ids_sorted(),
            Action::CancelAllPending => self.manager.all_pending_ids_sorted(),
            Action::CloseAllInGroup { group_id }
            | Action::ModifyAllStoplossInGroup { group_id, .. } => {
                let mut ids = self.manager.open_ids_by_group(group_id);
                ids.sort();
                ids
            }
        }
    }

    // ── Price feed ──────────────────────────────────────────────────────

    /// Feed a new price quote into the engine.
    ///
    /// 1. Stores the quote as the last known price for the symbol.
    /// 2. Checks all **pending** positions on that symbol for fill conditions.
    /// 3. Evaluates **management rules** for all open positions on that symbol.
    /// 4. Applies resulting effects to internal state.
    /// 5. Returns all effects for the caller to process externally.
    pub fn on_price(&mut self, quote: &PriceQuote) -> Vec<Effect> {
        self.last_quotes.insert(quote.symbol.clone(), quote.clone());

        let mut all_effects = Vec::new();
        let fill_model = self.fill_model;

        // ── 1. Check pending fills ──────────────────────────────────────
        if self.alert_register.is_some() {
            // Alert register path: pending fills are handled as alerts.
            // (registered when the pending order is placed)
        } else {
            let pending_ids = self.manager.pending_ids_by_symbol(&quote.symbol);
            for id in pending_ids {
                if let Some(pos) = self.manager.get_mut(&id)
                    && pos.try_fill(quote, fill_model)
                {
                    all_effects.push(Effect::PositionOpened { id: id.clone() });
                }
            }
        }

        // ── 2. Check alert register (static thresholds) ─────────────────
        if let Some(ref mut register) = self.alert_register {
            let triggered = register.check(quote, fill_model);
            // Collect triggered alerts, then apply them below (avoids borrow conflict).
            let triggered_alerts: Vec<TriggeredAlert> = triggered;

            for alert in triggered_alerts {
                let effects = self.apply_triggered_alert(&alert, quote);
                all_effects.extend(effects);
            }
        }

        // ── 3. Evaluate rules for open positions ────────────────────────
        if self.alert_register.is_some() {
            // Alert register path: only tick-evaluate stateful positions.
            let tick_ids = self
                .alert_register
                .as_ref()
                .unwrap()
                .tick_eval_ids(&quote.symbol);

            for id in tick_ids {
                let effects = {
                    let pos = match self.manager.get_mut(&id) {
                        Some(p) if p.data.status == PositionStatus::Open => p,
                        _ => continue,
                    };
                    pos.evaluate_stateful_rules(quote, fill_model)
                };
                for effect in &effects {
                    self.apply_effect(effect, quote);
                }
                all_effects.extend(effects);
            }
        } else {
            // Tick-by-tick path: evaluate all rules on all open positions.
            let open_ids = self.manager.open_ids_by_symbol(&quote.symbol);
            for id in open_ids {
                let effects = {
                    let pos = match self.manager.get_mut(&id) {
                        Some(p) => p,
                        None => continue,
                    };
                    pos.evaluate_rules(quote, fill_model)
                };
                for effect in &effects {
                    self.apply_effect(effect, quote);
                }
                all_effects.extend(effects);
            }
        }

        all_effects
    }

    /// Future-quote tick processing with no retroactive rule evaluation.
    ///
    /// This compatibility adapter uses FutureQuoteV1 without slippage. The
    /// configurable backtest path uses [`Self::on_price_future_effects_priced`].
    pub fn on_price_future_quote(&mut self, quote: &PriceQuote) -> Vec<Effect> {
        self.on_price_future_effects(quote)
            .into_iter()
            .map(FutureEffect::into_effect)
            .collect()
    }

    /// Compatibility FutureQuote adapter using FutureQuoteV1 without slippage.
    pub fn on_price_future_effects(&mut self, quote: &PriceQuote) -> Vec<FutureEffect> {
        let pricer = ExecutionPricer::new(ExecutionModel::future_quote_v1(self.fill_model));
        let prepared = match self.prepare_pending_fills(quote, &pricer, 1.0) {
            Ok(prepared) => prepared,
            Err(_) => return Vec::new(),
        };
        self.on_price_future_effects_priced(quote, &prepared, &pricer, 1.0)
            .unwrap_or_default()
    }

    /// Apply one quote in place and retain a rollback token for external processing.
    pub fn begin_on_price_future_effects_priced(
        &mut self,
        quote: &PriceQuote,
        prepared_pending: &[PreparedPendingFill],
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> FutureApplyResult<FutureEngineTransaction> {
        let checkpoint = self.checkpoint_for_quote(quote);
        match self.on_price_future_effects_in_place(quote, prepared_pending, pricer, pip_size) {
            Ok(effects) => Ok(FutureEngineTransaction {
                effects,
                checkpoint,
            }),
            Err(error) => {
                self.restore_checkpoint(checkpoint);
                Err(error)
            }
        }
    }

    /// Build a disposable next-engine state for one FutureQuote settlement.
    /// The caller can run accounting against it and commit only after success.
    pub fn stage_on_price_future_effects_priced(
        &self,
        quote: &PriceQuote,
        prepared_pending: &[PreparedPendingFill],
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> FutureApplyResult<(Self, Vec<FutureEffect>)> {
        let mut staged = self.clone();
        let transaction = staged.begin_on_price_future_effects_priced(
            quote,
            prepared_pending,
            pricer,
            pip_size,
        )?;
        let effects = transaction.commit();
        Ok((staged, effects))
    }

    /// Atomically settle one quote with already-priced pending entries and
    /// engine-priced rule exits.
    pub fn on_price_future_effects_priced(
        &mut self,
        quote: &PriceQuote,
        prepared_pending: &[PreparedPendingFill],
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        Ok(self
            .begin_on_price_future_effects_priced(quote, prepared_pending, pricer, pip_size)?
            .commit())
    }

    fn prepare_pending_fills(
        &self,
        quote: &PriceQuote,
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> std::result::Result<Vec<PreparedPendingFill>, ExecutionError> {
        let mut prepared = Vec::new();
        for id in self.manager.pending_ids_by_symbol_sorted(&quote.symbol) {
            let Some(position) = self.manager.get(&id) else {
                continue;
            };
            let Some(purpose) = position.pending_fill_purpose(quote, self.fill_model) else {
                continue;
            };
            let execution = pricer.price(
                purpose,
                position.data.side,
                quote,
                position.data.pending_price,
                pip_size,
            )?;
            prepared.push(PreparedPendingFill {
                position_id: id,
                execution,
                size: position.data.size,
            });
        }
        Ok(prepared)
    }

    fn on_price_future_effects_in_place(
        &mut self,
        quote: &PriceQuote,
        prepared_pending: &[PreparedPendingFill],
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        self.last_quotes.insert(quote.symbol.clone(), quote.clone());

        let fill_model = self.fill_model;
        let existing_open_ids = self.manager.open_ids_by_symbol_sorted(&quote.symbol);
        let pending_ids = self.manager.pending_ids_by_symbol_sorted(&quote.symbol);
        let mut all_effects = Vec::new();

        for id in pending_ids {
            let Some(position) = self.manager.get(&id) else {
                continue;
            };
            let Some(expected_purpose) = position.pending_fill_purpose(quote, fill_model) else {
                continue;
            };
            let prepared = prepared_pending
                .iter()
                .find(|prepared| prepared.position_id == id)
                .ok_or_else(|| FutureApplyError::InvalidPreparedFill {
                    position_id: id.clone(),
                    reason: "triggered pending order has no prepared execution".into(),
                })?;
            validate_future_execution(
                &prepared.execution,
                expected_purpose,
                position.data.side,
                &id,
            )?;
            if prepared.execution.requested_price != position.data.pending_price {
                return Err(FutureApplyError::InvalidPreparedFill {
                    position_id: id,
                    reason: "requested price does not match pending order".into(),
                });
            }
            if !valid_position_size(prepared.size) {
                return Err(FutureApplyError::InvalidPreparedFill {
                    position_id: id,
                    reason: format!(
                        "size must be finite and greater than the accounting tolerance, got {}",
                        prepared.size
                    ),
                });
            }

            let fill = FutureFill {
                execution: prepared.execution,
                size: prepared.size,
                ts: quote.ts,
                source_quote_ts: Some(quote.ts),
            };
            let position = self
                .manager
                .get_mut(&prepared.position_id)
                .ok_or_else(|| CoreError::PositionNotFound(prepared.position_id.clone()))?;
            position.data.size = prepared.size;
            if !position.apply_pending_fill(fill.as_fill()) {
                return Err(FutureApplyError::InvalidPreparedFill {
                    position_id: prepared.position_id.clone(),
                    reason: "position is no longer pending".into(),
                });
            }
            all_effects.push(FutureEffect::filled(
                Effect::PositionOpened {
                    id: prepared.position_id.clone(),
                },
                fill,
                None,
            ));
        }

        for id in existing_open_ids {
            let intents = {
                let Some(position) = self.manager.get_mut(&id) else {
                    continue;
                };
                if position.data.status != PositionStatus::Open {
                    continue;
                }
                position.evaluate_rules_future(quote, fill_model)
            };
            for intent in intents {
                all_effects.push(self.apply_future_intent(intent, quote, pricer, pip_size)?);
            }
        }

        Ok(all_effects)
    }

    /// Update the intended size of a still-pending order before its fill check.
    pub fn set_pending_size(&mut self, position_id: &str, size: f64) -> Result<()> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
        if pos.data.status != PositionStatus::Pending {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Pending".into(),
                actual: pos.data.status.to_string(),
            });
        }
        validate_position_size("pending size", size)?;
        pos.data.size = size;
        Ok(())
    }

    /// Synchronize a core entry fill with an externally calculated execution.
    pub fn synchronize_latest_fill(&mut self, position_id: &str, fill: Fill) -> Result<()> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
        if !pos.data.synchronize_latest_fill(fill) {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "position with an entry fill".into(),
                actual: pos.data.status.to_string(),
            });
        }
        Ok(())
    }

    // ── Action processing ───────────────────────────────────────────────

    /// Close one open position with an explicit authoritative reason.
    /// This is used by deterministic end-of-data liquidation; normal callers
    /// should continue using `Action::ClosePosition`.
    pub fn close_position_with_reason(
        &mut self,
        position_id: &str,
        reason: CloseReason,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
        if pos.data.status != PositionStatus::Open {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Open".into(),
                actual: pos.data.status.to_string(),
            });
        }
        pos.data.apply_full_close(reason, ts);
        if let Some(ref mut register) = self.alert_register {
            register.deregister_position(position_id);
        }
        Ok(vec![Effect::PositionClosed {
            id: position_id.to_owned(),
            reason,
        }])
    }

    /// Process a trading action and return effects.
    ///
    /// This is the primary way to interact with the engine from strategies,
    /// signal providers, or manual input.
    pub fn apply_action(&mut self, action: Action, ts: NaiveDateTime) -> Result<Vec<Effect>> {
        match action {
            // ── Open new position ───────────────────────────────────
            Action::Open {
                symbol,
                side,
                order_type,
                price,
                size,
                stoploss,
                targets,
                rules,
                group,
                trade_id,
            } => self.action_open(
                OpenActionParams {
                    symbol,
                    side,
                    order_type,
                    price,
                    size,
                    stoploss,
                    targets,
                    rules,
                    group,
                    trade_id,
                },
                ts,
            ),

            // ── Scale in ────────────────────────────────────────────
            Action::ScaleIn {
                position_id,
                price,
                size,
                trade_id,
            } => self.action_scale_in(&position_id, price, size, trade_id, ts),

            // ── Close position ──────────────────────────────────────
            Action::ClosePosition { position_id } => self.action_close_position(&position_id, ts),

            // ── Close partial ───────────────────────────────────────
            Action::ClosePartial { position_id, ratio } => {
                self.action_close_partial(&position_id, ratio, ts)
            }

            // ── Cancel pending ──────────────────────────────────────
            Action::CancelPending { position_id } => self.action_cancel_pending(&position_id, ts),

            // ── Modify stoploss ─────────────────────────────────────
            Action::ModifyStoploss { position_id, price } => {
                self.action_modify_stoploss(&position_id, price, ts)
            }

            // ── Move stoploss to entry ──────────────────────────────
            Action::MoveStoplossToEntry { position_id } => {
                self.action_move_sl_to_entry(&position_id, ts)
            }

            // ── Add target ──────────────────────────────────────────
            Action::AddTarget {
                position_id,
                price,
                close_ratio,
            } => self.action_add_target(&position_id, price, close_ratio, ts),

            // ── Remove target ───────────────────────────────────────
            Action::RemoveTarget { position_id, price } => {
                self.action_remove_target(&position_id, price, ts)
            }

            // ── Modify target ───────────────────────────────────────
            Action::ModifyTarget {
                position_id,
                old_price,
                new_price,
            } => self.action_modify_target(&position_id, old_price, new_price, ts),

            // ── Add rule ────────────────────────────────────────────
            Action::AddRule { position_id, rule } => self.action_add_rule(&position_id, rule, ts),

            // ── Remove rule ─────────────────────────────────────────
            Action::RemoveRule {
                position_id,
                rule_name,
            } => self.action_remove_rule(&position_id, &rule_name, ts),

            // ── Bulk: close all of symbol ───────────────────────────
            Action::CloseAllOf { symbol } => self.action_close_all_of(&symbol, ts),

            // ── Bulk: close all ─────────────────────────────────────
            Action::CloseAll => self.action_close_all(ts),

            // ── Bulk: cancel all pending ────────────────────────────
            Action::CancelAllPending => self.action_cancel_all_pending(ts),

            // ── Bulk: modify all stoploss on symbol ─────────────────
            Action::ModifyAllStoploss { symbol, price } => {
                self.action_modify_all_stoploss(&symbol, price, ts)
            }

            // ── Bulk: close all in group ────────────────────────────
            Action::CloseAllInGroup { group_id } => self.action_close_all_in_group(&group_id, ts),

            // ── Bulk: modify all stoploss in group ──────────────────
            Action::ModifyAllStoplossInGroup { group_id, price } => {
                self.action_modify_all_stoploss_in_group(&group_id, price, ts)
            }
        }
    }

    /// Apply one non-fill FutureQuote action in place with rollback support.
    pub fn begin_future_action(
        &mut self,
        action: Action,
        effective_ts: NaiveDateTime,
    ) -> FutureApplyResult<FutureEngineTransaction> {
        let checkpoint = self.checkpoint_for_action(&action);
        match self.apply_future_action(action, effective_ts) {
            Ok(effects) => Ok(FutureEngineTransaction {
                effects,
                checkpoint,
            }),
            Err(error) => {
                self.restore_checkpoint(checkpoint);
                Err(error)
            }
        }
    }

    /// Apply a FutureQuote action that does not create a fill. Market opens,
    /// scale-ins, and closes must use [`Self::apply_priced_future_action`].
    pub fn apply_future_action(
        &mut self,
        action: Action,
        effective_ts: NaiveDateTime,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        if matches!(
            action,
            Action::Open {
                order_type: OrderType::Market,
                ..
            } | Action::ScaleIn { .. }
                | Action::ClosePosition { .. }
                | Action::ClosePartial { .. }
                | Action::CloseAllOf { .. }
                | Action::CloseAll
                | Action::CloseAllInGroup { .. }
        ) {
            return Err(FutureApplyError::InvalidPreparedFill {
                position_id: String::new(),
                reason: "fill-bearing action requires a priced execution".into(),
            });
        }
        let effects = self.apply_action(action, effective_ts)?;
        Ok(effects
            .into_iter()
            .map(|effect| self.plain_future_effect(effect))
            .collect())
    }

    /// Apply one priced FutureQuote action in place with rollback support.
    pub fn begin_priced_future_action(
        &mut self,
        action: Action,
        quote: &PriceQuote,
        execution: ExecutionFill,
    ) -> FutureApplyResult<FutureEngineTransaction> {
        let checkpoint = self.checkpoint_for_action(&action);
        match self.apply_priced_future_action(action, quote, execution) {
            Ok(effects) => Ok(FutureEngineTransaction {
                effects,
                checkpoint,
            }),
            Err(error) => {
                self.restore_checkpoint(checkpoint);
                Err(error)
            }
        }
    }

    /// Apply one already-priced FutureQuote market action. The supplied
    /// execution is used directly for both core state and the returned effect.
    pub fn apply_priced_future_action(
        &mut self,
        action: Action,
        quote: &PriceQuote,
        execution: ExecutionFill,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        match action {
            Action::Open {
                symbol,
                side,
                order_type,
                price,
                size,
                stoploss,
                targets,
                rules,
                group,
                trade_id,
            } => {
                if order_type != OrderType::Market {
                    return Err(FutureApplyError::InvalidPreparedFill {
                        position_id: String::new(),
                        reason: "only market opens are fill-bearing actions".into(),
                    });
                }
                if symbol != quote.symbol {
                    return Err(FutureApplyError::InvalidPreparedFill {
                        position_id: String::new(),
                        reason: format!(
                            "action symbol {symbol} does not match quote symbol {}",
                            quote.symbol
                        ),
                    });
                }
                validate_position_size("position size", size)?;
                if let Some(price) = price {
                    validate_positive_price("supplied entry price", price)?;
                }
                validate_future_execution(
                    &execution,
                    FillPurpose::MarketEntry,
                    side,
                    "<new-position>",
                )?;
                let effect = self
                    .action_open(
                        OpenActionParams {
                            symbol,
                            side,
                            order_type,
                            price: Some(execution.price),
                            size,
                            stoploss,
                            targets,
                            rules,
                            group,
                            trade_id,
                        },
                        quote.ts,
                    )?
                    .into_iter()
                    .next()
                    .expect("market open produces one effect");
                Ok(vec![FutureEffect::filled(
                    effect,
                    FutureFill {
                        execution,
                        size,
                        ts: quote.ts,
                        source_quote_ts: Some(quote.ts),
                    },
                    None,
                )])
            }
            Action::ScaleIn {
                position_id,
                price,
                size,
                trade_id,
            } => {
                validate_position_size("scale-in size", size)?;
                if let Some(price) = price {
                    validate_positive_price("supplied scale-in price", price)?;
                }
                let position = self
                    .manager
                    .get(&position_id)
                    .ok_or_else(|| CoreError::PositionNotFound(position_id.clone()))?;
                if position.data.symbol != quote.symbol {
                    return Err(FutureApplyError::InvalidPreparedFill {
                        position_id,
                        reason: "position symbol does not match quote symbol".into(),
                    });
                }
                validate_future_execution(
                    &execution,
                    FillPurpose::MarketEntry,
                    position.data.side,
                    &position_id,
                )?;
                let effect = self
                    .action_scale_in(
                        &position_id,
                        Some(execution.price),
                        size,
                        trade_id,
                        quote.ts,
                    )?
                    .into_iter()
                    .next()
                    .expect("scale-in produces one effect");
                Ok(vec![FutureEffect::filled(
                    effect,
                    FutureFill {
                        execution,
                        size,
                        ts: quote.ts,
                        source_quote_ts: Some(quote.ts),
                    },
                    None,
                )])
            }
            Action::ClosePosition { position_id } => self.close_position_with_reason_future(
                &position_id,
                CloseReason::Manual,
                quote,
                execution,
            ),
            Action::ClosePartial { position_id, ratio } => {
                let position = self
                    .manager
                    .get(&position_id)
                    .ok_or_else(|| CoreError::PositionNotFound(position_id.clone()))?;
                if position.data.symbol != quote.symbol {
                    return Err(FutureApplyError::InvalidPreparedFill {
                        position_id,
                        reason: "position symbol does not match quote symbol".into(),
                    });
                }
                validate_future_execution(
                    &execution,
                    FillPurpose::MarketExit,
                    position.data.side,
                    &position_id,
                )?;
                if !ratio.is_finite() || ratio <= 0.0 || ratio > 1.0 {
                    return Err(CoreError::InvalidAction(format!(
                        "partial-close ratio must be finite and in (0, 1], got {ratio}"
                    ))
                    .into());
                }
                if position.data.status != PositionStatus::Open {
                    return Err(CoreError::InvalidState {
                        id: position_id,
                        expected: "Open".into(),
                        actual: position.data.status.to_string(),
                    }
                    .into());
                }
                let actual_ratio = position.data.capped_close_ratio(ratio);
                let close_size = position.data.close_size_for_ratio(actual_ratio);
                let effect = self
                    .action_close_partial_at(&position_id, ratio, execution.price, quote.ts)?
                    .into_iter()
                    .next()
                    .expect("partial close produces one effect");
                Ok(vec![FutureEffect::filled(
                    effect,
                    FutureFill {
                        execution,
                        size: close_size,
                        ts: quote.ts,
                        source_quote_ts: Some(quote.ts),
                    },
                    None,
                )])
            }
            _ => Err(FutureApplyError::InvalidPreparedFill {
                position_id: String::new(),
                reason: "non-fill action passed to priced FutureQuote API".into(),
            }),
        }
    }

    /// Close with an explicit reason using one previously priced market exit.
    pub fn close_position_with_reason_future(
        &mut self,
        position_id: &str,
        reason: CloseReason,
        quote: &PriceQuote,
        execution: ExecutionFill,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        self.close_position_with_reason_future_at(position_id, reason, quote, execution, quote.ts)
    }

    /// Close in place with separate timestamps and retain rollback support.
    pub fn begin_close_position_with_reason_future_at(
        &mut self,
        position_id: &str,
        reason: CloseReason,
        quote: &PriceQuote,
        execution: ExecutionFill,
        execution_ts: NaiveDateTime,
    ) -> FutureApplyResult<FutureEngineTransaction> {
        let checkpoint = self.checkpoint_for_positions(vec![position_id.to_owned()]);
        match self.close_position_with_reason_future_at(
            position_id,
            reason,
            quote,
            execution,
            execution_ts,
        ) {
            Ok(effects) => Ok(FutureEngineTransaction {
                effects,
                checkpoint,
            }),
            Err(error) => {
                self.restore_checkpoint(checkpoint);
                Err(error)
            }
        }
    }

    /// Close with separate execution and source-quote timestamps.
    pub fn close_position_with_reason_future_at(
        &mut self,
        position_id: &str,
        reason: CloseReason,
        quote: &PriceQuote,
        execution: ExecutionFill,
        execution_ts: NaiveDateTime,
    ) -> FutureApplyResult<Vec<FutureEffect>> {
        if execution_ts < quote.ts {
            return Err(FutureApplyError::InvalidPreparedFill {
                position_id: position_id.to_owned(),
                reason: "execution timestamp precedes source quote timestamp".into(),
            });
        }
        let position = self
            .manager
            .get(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
        if position.data.symbol != quote.symbol {
            return Err(FutureApplyError::InvalidPreparedFill {
                position_id: position_id.to_owned(),
                reason: "position symbol does not match quote symbol".into(),
            });
        }
        validate_future_execution(
            &execution,
            FillPurpose::MarketExit,
            position.data.side,
            position_id,
        )?;
        if position.data.status != PositionStatus::Open {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Open".into(),
                actual: position.data.status.to_string(),
            }
            .into());
        }
        let close_size = position.data.remaining_size();
        let effect = self
            .close_position_with_reason(position_id, reason, execution_ts)?
            .into_iter()
            .next()
            .expect("full close produces one effect");
        Ok(vec![FutureEffect::filled(
            effect,
            FutureFill {
                execution,
                size: close_size,
                ts: execution_ts,
                source_quote_ts: Some(quote.ts),
            },
            None,
        )])
    }

    fn plain_future_effect(&self, effect: Effect) -> FutureEffect {
        let stop_origin = match &effect {
            Effect::StoplossModified { id, .. } => self
                .manager
                .get(id)
                .and_then(|position| position.data.stop_origin),
            _ => None,
        };
        FutureEffect::plain_with_metadata(effect, None, stop_origin)
    }

    // ── Private: action handlers ────────────────────────────────────────

    fn action_open(&mut self, params: OpenActionParams, ts: NaiveDateTime) -> Result<Vec<Effect>> {
        let OpenActionParams {
            symbol,
            side,
            order_type,
            price,
            size,
            stoploss,
            targets,
            rules,
            group,
            trade_id,
        } = params;

        validate_position_size("position size", size)?;
        let entry_price = match (order_type, price) {
            (OrderType::Market, Some(price)) => price,
            (OrderType::Market, None) => self
                .last_quotes
                .get(&symbol)
                .ok_or_else(|| CoreError::NoPriceAvailable(symbol.clone()))?
                .open_price(side),
            (OrderType::Limit | OrderType::Stop, Some(price)) => price,
            (OrderType::Limit | OrderType::Stop, None) => {
                return Err(CoreError::InvalidAction(format!(
                    "{order_type} order requires a price"
                )));
            }
        };
        validate_positive_price("entry price", entry_price)?;

        let mut target_price_keys = Vec::new();
        if let Some(stoploss) = stoploss {
            validate_stop_price("open stoploss", side, entry_price, stoploss)?;
        }
        for target in &targets {
            validate_target_ratio("open target", target.close_ratio)?;
            validate_target_price("open target", side, entry_price, target.price)?;
            register_unique_target_price("open target", target.price, &mut target_price_keys)?;
        }
        for rule in &rules {
            validate_rule_config(
                "open rule",
                rule,
                side,
                Some(entry_price),
                &mut target_price_keys,
            )?;
        }

        if let Some(ref trade_id) = trade_id {
            self.manager
                .ensure_trade_id_available(trade_id, None)
                .map_err(core_error_from_manager)?;
        }

        let id = if self.deterministic_ids {
            let id = format!("position:{:08}", self.next_position_sequence);
            self.next_position_sequence += 1;
            id
        } else {
            gen_id()
        };

        // Build rules only after every fallible precondition has passed.
        let mut live_rules: Vec<Rule> = Vec::new();
        if let Some(sl) = stoploss {
            live_rules.push(Rule::fixed_stoploss(sl));
        }
        for t in &targets {
            live_rules.push(Rule::take_profit(t.price, t.close_ratio));
        }
        for rc in rules {
            live_rules.push(Rule::from_config(rc));
        }

        match order_type {
            OrderType::Market => {
                let fill = Fill {
                    price: entry_price,
                    size,
                    ts,
                };
                let mut pos =
                    Position::new_market(id.clone(), symbol.clone(), side, fill, live_rules);
                if stoploss.is_some() {
                    pos.data.stop_origin = Some(crate::types::StopOrigin::Initial);
                }
                // Assign group if specified.
                if let Some(ref gid) = group {
                    pos.data.group = Some(gid.clone());
                    pos.data.records.push((
                        PositionRecord::GroupAssigned {
                            group_id: gid.clone(),
                        },
                        ts,
                    ));
                }
                if let Some(ref tid) = trade_id {
                    pos.set_trade_id(Some(tid.clone()));
                }
                self.manager
                    .add_checked(pos)
                    .map_err(core_error_from_manager)?;
                if let Some(gid) = group.as_deref() {
                    self.manager.add_to_group(gid, id.clone());
                }
                // Register alerts if alert register is active.
                self.register_alerts_for_position(&id, &symbol, side);
                Ok(vec![Effect::PositionOpened { id }])
            }
            OrderType::Limit | OrderType::Stop => {
                let pending_price = entry_price;
                let mut pos = Position::new_pending(
                    id.clone(),
                    symbol.clone(),
                    side,
                    order_type,
                    pending_price,
                    size,
                    ts,
                    live_rules,
                );
                if stoploss.is_some() {
                    pos.data.stop_origin = Some(crate::types::StopOrigin::Initial);
                }
                // Assign group if specified.
                if let Some(ref gid) = group {
                    pos.data.group = Some(gid.clone());
                    pos.data.records.push((
                        PositionRecord::GroupAssigned {
                            group_id: gid.clone(),
                        },
                        ts,
                    ));
                }
                if let Some(ref tid) = trade_id {
                    pos.set_trade_id(Some(tid.clone()));
                }
                self.manager
                    .add_checked(pos)
                    .map_err(core_error_from_manager)?;
                if let Some(gid) = group.as_deref() {
                    self.manager.add_to_group(gid, id.clone());
                }
                // Register pending fill alert if alert register is active.
                if let Some(register) = self.alert_register.as_mut() {
                    register.register(
                        &symbol,
                        pending_price,
                        id.clone(),
                        side,
                        AlertKind::PendingFill { order_type, side },
                    );
                }
                Ok(vec![Effect::OrderPlaced { id }])
            }
        }
    }

    fn action_scale_in(
        &mut self,
        position_id: &str,
        price: Option<f64>,
        size: f64,
        trade_id: Option<crate::types::TradeId>,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_position_size("scale-in size", size)?;
        if let Some(price) = price {
            validate_positive_price("scale-in price", price)?;
        }

        let (symbol, side, status, has_trade_id) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            (
                pos.data.symbol.clone(),
                pos.data.side,
                pos.data.status,
                pos.data.trade_id.is_some(),
            )
        };

        if status != PositionStatus::Open {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Open".into(),
                actual: status.to_string(),
            });
        }

        if let Some(ref trade_id) = trade_id {
            self.manager
                .ensure_trade_id_available(trade_id, Some(position_id))
                .map_err(core_error_from_manager)?;
        }

        let fill_price = match price {
            Some(price) => price,
            None => self
                .last_quotes
                .get(&symbol)
                .ok_or_else(|| CoreError::NoPriceAvailable(symbol.clone()))?
                .open_price(side),
        };
        validate_positive_price("scale-in fill price", fill_price)?;
        let fill = Fill {
            price: fill_price,
            size,
            ts,
        };

        // Attach identity before the fill mutation, after all other fallible
        // validation, so duplicate rejection cannot leave a partial scale-in.
        if !has_trade_id && let Some(trade_id) = trade_id {
            self.manager
                .set_trade_id_checked(position_id, trade_id)
                .map_err(core_error_from_manager)?;
        }

        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        pos.data.add_fill(fill.clone());
        pos.data
            .records
            .push((PositionRecord::Filled { fill: fill.clone() }, ts));

        Ok(vec![Effect::ScaledIn {
            id: position_id.to_owned(),
            fill,
        }])
    }

    fn action_close_position(
        &mut self,
        position_id: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        self.close_position_with_reason(position_id, CloseReason::Manual, ts)
    }

    fn action_close_partial(
        &mut self,
        position_id: &str,
        ratio: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let pos = self
            .manager
            .get(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
        let close_price = self
            .last_quotes
            .get(&pos.data.symbol)
            .map(|quote| quote.close_price(pos.data.side))
            .unwrap_or(pos.data.average_entry());
        self.action_close_partial_at(position_id, ratio, close_price, ts)
    }

    fn action_close_partial_at(
        &mut self,
        position_id: &str,
        ratio: f64,
        close_price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        if !ratio.is_finite() || ratio <= 0.0 || ratio > 1.0 {
            return Err(CoreError::InvalidAction(format!(
                "partial-close ratio must be finite and in (0, 1], got {ratio}"
            )));
        }
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

        if pos.data.status != PositionStatus::Open {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Open".into(),
                actual: pos.data.status.to_string(),
            });
        }

        let actual_ratio = pos.data.capped_close_ratio(ratio);
        pos.data
            .apply_partial_close(actual_ratio, close_price, CloseReason::Manual, ts);

        if pos.data.status == PositionStatus::Closed {
            Ok(vec![Effect::PositionClosed {
                id: position_id.to_owned(),
                reason: CloseReason::Manual,
            }])
        } else {
            Ok(vec![Effect::PartialClose {
                id: position_id.to_owned(),
                ratio: actual_ratio,
                reason: CloseReason::Manual,
            }])
        }
    }

    fn action_cancel_pending(
        &mut self,
        position_id: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

        if pos.data.status != PositionStatus::Pending {
            return Err(CoreError::InvalidState {
                id: position_id.to_owned(),
                expected: "Pending".into(),
                actual: pos.data.status.to_string(),
            });
        }

        pos.data.status = PositionStatus::Cancelled;
        pos.data.close_ts = Some(ts);
        pos.data.records.push((PositionRecord::Cancelled, ts));

        // Deregister pending fill alert.
        if let Some(ref mut register) = self.alert_register {
            register.deregister_position(position_id);
        }

        Ok(vec![Effect::OrderCancelled {
            id: position_id.to_owned(),
        }])
    }

    fn action_modify_stoploss(
        &mut self,
        position_id: &str,
        new_price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let (symbol, side, status, old) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            let entry = position_entry_basis(pos)?;
            validate_stop_price("modified stoploss", pos.data.side, entry, new_price)?;
            (
                pos.data.symbol.clone(),
                pos.data.side,
                pos.data.status,
                pos.current_stoploss(),
            )
        };

        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        pos.set_stoploss(new_price);
        pos.data.records.push((
            PositionRecord::StoplossModified {
                from: old,
                to: new_price,
            },
            ts,
        ));
        let old_price = old.unwrap_or(0.0);

        if status == PositionStatus::Open
            && let Some(register) = self.alert_register.as_mut()
        {
            replace_stoploss_alert(register, &symbol, side, position_id, old, new_price);
        }

        Ok(vec![Effect::StoplossModified {
            id: position_id.to_owned(),
            old_price,
            new_price,
        }])
    }

    fn action_move_sl_to_entry(
        &mut self,
        position_id: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let (entry, old, symbol, side) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, false)?;
            let entry = position_entry_basis(pos)?;
            validate_positive_price("average entry price", entry)?;
            (
                entry,
                pos.current_stoploss(),
                pos.data.symbol.clone(),
                pos.data.side,
            )
        };

        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        pos.set_stoploss_with_origin(entry, crate::types::StopOrigin::Breakeven);
        pos.data.records.push((
            PositionRecord::StoplossModified {
                from: old,
                to: entry,
            },
            ts,
        ));

        if let Some(register) = self.alert_register.as_mut() {
            replace_stoploss_alert(register, &symbol, side, position_id, old, entry);
        }

        Ok(vec![Effect::StoplossModified {
            id: position_id.to_owned(),
            old_price: old.unwrap_or(0.0),
            new_price: entry,
        }])
    }

    fn action_add_target(
        &mut self,
        position_id: &str,
        price: f64,
        close_ratio: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_target_ratio("added target", close_ratio)?;
        let (symbol, side, status) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            let entry = position_entry_basis(pos)?;
            validate_target_price("added target", pos.data.side, entry, price)?;
            ensure_target_price_available(pos, price, None)?;
            (pos.data.symbol.clone(), pos.data.side, pos.data.status)
        };

        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        pos.rules.push(Rule::take_profit(price, close_ratio));
        pos.data
            .records
            .push((PositionRecord::TargetAdded { price, close_ratio }, ts));

        if status == PositionStatus::Open
            && let Some(register) = self.alert_register.as_mut()
        {
            register.register(
                &symbol,
                price,
                position_id.to_owned(),
                side,
                AlertKind::TakeProfit { close_ratio },
            );
        }

        Ok(vec![])
    }

    fn action_remove_target(
        &mut self,
        position_id: &str,
        price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_positive_price("removed target price", price)?;
        let (symbol, side, status, removed_ratio) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            let removed_ratio = pos.rules.iter().find_map(|rule| match rule {
                Rule::TakeProfit {
                    price: target_price,
                    close_ratio,
                    ..
                } if same_alert_price(*target_price, price) => Some(*close_ratio),
                _ => None,
            });
            (
                pos.data.symbol.clone(),
                pos.data.side,
                pos.data.status,
                removed_ratio,
            )
        };

        if let Some(close_ratio) = removed_ratio {
            let pos = self
                .manager
                .get_mut(position_id)
                .expect("position was validated above");
            pos.rules.retain(|rule| {
                !matches!(rule, Rule::TakeProfit { price: target_price, .. } if same_alert_price(*target_price, price))
            });
            pos.data
                .records
                .push((PositionRecord::TargetRemoved { price }, ts));

            if status == PositionStatus::Open
                && let Some(register) = self.alert_register.as_mut()
            {
                register.deregister_alert(
                    &symbol,
                    price,
                    position_id,
                    side,
                    &AlertKind::TakeProfit { close_ratio },
                );
            }
        }

        Ok(vec![])
    }

    fn action_modify_target(
        &mut self,
        position_id: &str,
        old_price: f64,
        new_price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_positive_price("existing target price", old_price)?;
        validate_positive_price("replacement target price", new_price)?;

        let (target_index, symbol, side, status, close_ratio) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            let entry = position_entry_basis(pos)?;
            validate_target_price("replacement target", pos.data.side, entry, new_price)?;
            let target_index = pos
                .rules
                .iter()
                .position(|rule| {
                    matches!(rule, Rule::TakeProfit { price, .. } if same_alert_price(*price, old_price))
                })
                .ok_or_else(|| CoreError::TargetNotFound {
                    position_id: position_id.to_owned(),
                    price: old_price,
                })?;
            let Rule::TakeProfit {
                close_ratio,
                triggered,
                ..
            } = &pos.rules[target_index]
            else {
                unreachable!("target lookup only returns take-profit rules");
            };
            if *triggered {
                return Err(CoreError::TargetAlreadyTriggered {
                    position_id: position_id.to_owned(),
                    price: old_price,
                });
            }
            validate_target_ratio("modified target", *close_ratio)?;
            ensure_target_price_available(pos, new_price, Some(target_index))?;
            (
                target_index,
                pos.data.symbol.clone(),
                pos.data.side,
                pos.data.status,
                *close_ratio,
            )
        };

        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        let Rule::TakeProfit { price, .. } = &mut pos.rules[target_index] else {
            unreachable!("validated target index changed without mutation");
        };
        *price = new_price;
        pos.data.records.push((
            PositionRecord::TargetModified {
                from: old_price,
                to: new_price,
                close_ratio,
            },
            ts,
        ));

        if status == PositionStatus::Open
            && let Some(register) = self.alert_register.as_mut()
        {
            register.deregister_alert(
                &symbol,
                old_price,
                position_id,
                side,
                &AlertKind::TakeProfit { close_ratio },
            );
            register.register(
                &symbol,
                new_price,
                position_id.to_owned(),
                side,
                AlertKind::TakeProfit { close_ratio },
            );
        }

        Ok(vec![])
    }

    fn action_add_rule(
        &mut self,
        position_id: &str,
        rule_config: crate::types::RuleConfig,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let (symbol, side, status) = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            let entry = position_entry_basis(pos)?;
            let mut target_keys = target_price_keys(pos);
            validate_rule_config(
                "added rule",
                &rule_config,
                pos.data.side,
                Some(entry),
                &mut target_keys,
            )?;
            (pos.data.symbol.clone(), pos.data.side, pos.data.status)
        };

        let rule = Rule::from_config(rule_config);
        let is_stateful = rule.is_stateful();
        let name = rule.name().to_owned();
        let fixed_price = match &rule {
            Rule::FixedStoploss { price } => Some(*price),
            _ => None,
        };
        let pos = self
            .manager
            .get_mut(position_id)
            .expect("position was validated above");
        let old_stop = if let Some(price) = fixed_price {
            pos.set_stoploss_with_origin(price, crate::types::StopOrigin::Modified)
        } else {
            pos.rules.push(rule.clone());
            None
        };
        pos.data.records.push((
            PositionRecord::RuleAdded {
                rule_name: name.clone(),
            },
            ts,
        ));

        if status == PositionStatus::Open
            && let Some(register) = self.alert_register.as_mut()
        {
            if is_stateful {
                register.register_tick_eval(&symbol, position_id.to_owned());
            } else {
                match &rule {
                    Rule::FixedStoploss { price } => replace_stoploss_alert(
                        register,
                        &symbol,
                        side,
                        position_id,
                        old_stop,
                        *price,
                    ),
                    Rule::TakeProfit {
                        price, close_ratio, ..
                    } => register.register(
                        &symbol,
                        *price,
                        position_id.to_owned(),
                        side,
                        AlertKind::TakeProfit {
                            close_ratio: *close_ratio,
                        },
                    ),
                    Rule::BreakevenWhen { trigger_price, .. } => register.register(
                        &symbol,
                        *trigger_price,
                        position_id.to_owned(),
                        side,
                        AlertKind::BreakevenTrigger,
                    ),
                    _ => {}
                }
            }
        }

        Ok(fixed_price
            .map(|new_price| Effect::StoplossModified {
                id: position_id.to_owned(),
                old_price: old_stop.unwrap_or(0.0),
                new_price,
            })
            .into_iter()
            .collect())
    }

    fn action_remove_rule(
        &mut self,
        position_id: &str,
        rule_name: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let status = {
            let pos = self
                .manager
                .get(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;
            ensure_management_status(pos, true)?;
            pos.data.status
        };
        let (symbol, side, removed, has_stateful_rules) = {
            let pos = self
                .manager
                .get_mut(position_id)
                .expect("position was validated above");
            let removed: Vec<Rule> = pos
                .rules
                .iter()
                .filter(|rule| rule.name() == rule_name)
                .cloned()
                .collect();
            if removed.is_empty() {
                return Ok(Vec::new());
            }
            pos.rules.retain(|rule| rule.name() != rule_name);
            if removed
                .iter()
                .any(|rule| matches!(rule, Rule::FixedStoploss { .. }))
            {
                pos.data.stop_origin = None;
            }
            pos.data.records.push((
                PositionRecord::RuleRemoved {
                    rule_name: rule_name.to_owned(),
                },
                ts,
            ));
            (
                pos.data.symbol.clone(),
                pos.data.side,
                removed,
                pos.has_stateful_rules(),
            )
        };

        if status == PositionStatus::Open
            && let Some(register) = self.alert_register.as_mut()
        {
            for rule in &removed {
                match rule {
                    Rule::FixedStoploss { price } => register.deregister_alert(
                        &symbol,
                        *price,
                        position_id,
                        side,
                        &AlertKind::Stoploss,
                    ),
                    Rule::TakeProfit {
                        price, close_ratio, ..
                    } => register.deregister_alert(
                        &symbol,
                        *price,
                        position_id,
                        side,
                        &AlertKind::TakeProfit {
                            close_ratio: *close_ratio,
                        },
                    ),
                    Rule::BreakevenWhen { trigger_price, .. } => register.deregister_alert(
                        &symbol,
                        *trigger_price,
                        position_id,
                        side,
                        &AlertKind::BreakevenTrigger,
                    ),
                    _ => {}
                }
            }
            if !has_stateful_rules {
                register.unregister_tick_eval(&symbol, position_id);
            }
        }

        Ok(removed
            .iter()
            .find_map(|rule| match rule {
                Rule::FixedStoploss { price } => Some(Effect::StoplossRemoved {
                    id: position_id.to_owned(),
                    old_price: *price,
                }),
                _ => None,
            })
            .into_iter()
            .collect())
    }

    // ── Bulk actions ────────────────────────────────────────────────────

    fn action_close_all_of(&mut self, symbol: &str, ts: NaiveDateTime) -> Result<Vec<Effect>> {
        let ids = self.manager.open_ids_by_symbol(symbol);
        let mut effects = Vec::new();
        for id in &ids {
            if let Some(pos) = self.manager.get_mut(id) {
                pos.data.apply_full_close(CloseReason::Manual, ts);
                effects.push(Effect::PositionClosed {
                    id: id.clone(),
                    reason: CloseReason::Manual,
                });
            }
        }
        // Deregister alerts for all closed positions.
        if let Some(ref mut register) = self.alert_register {
            for id in &ids {
                register.deregister_position(id);
            }
        }
        Ok(effects)
    }

    fn action_close_all(&mut self, ts: NaiveDateTime) -> Result<Vec<Effect>> {
        let ids = self.manager.all_open_ids();
        let mut effects = Vec::new();
        for id in ids {
            if let Some(pos) = self.manager.get_mut(&id) {
                pos.data.apply_full_close(CloseReason::Manual, ts);
                effects.push(Effect::PositionClosed {
                    id,
                    reason: CloseReason::Manual,
                });
            }
        }
        // Clear all alerts.
        if let Some(ref mut register) = self.alert_register {
            register.clear_all();
        }
        Ok(effects)
    }

    fn action_cancel_all_pending(&mut self, ts: NaiveDateTime) -> Result<Vec<Effect>> {
        let ids = self.manager.all_pending_ids();
        let mut effects = Vec::new();
        for id in &ids {
            if let Some(pos) = self.manager.get_mut(id) {
                pos.data.status = PositionStatus::Cancelled;
                pos.data.close_ts = Some(ts);
                pos.data.records.push((PositionRecord::Cancelled, ts));
                effects.push(Effect::OrderCancelled { id: id.clone() });
            }
        }
        // Deregister pending fill alerts.
        if let Some(ref mut register) = self.alert_register {
            for id in &ids {
                register.deregister_position(id);
            }
        }
        Ok(effects)
    }

    fn action_modify_all_stoploss(
        &mut self,
        symbol: &str,
        price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_positive_price("bulk stoploss price", price)?;
        let ids = self.manager.open_ids_by_symbol_sorted(symbol);
        let mut preflight = Vec::with_capacity(ids.len());
        for id in &ids {
            let pos = self
                .manager
                .get(id)
                .expect("open position id came from the manager");
            let entry = position_entry_basis(pos)?;
            validate_stop_price("bulk stoploss", pos.data.side, entry, price)?;
            preflight.push((
                id.clone(),
                pos.data.symbol.clone(),
                pos.data.side,
                pos.current_stoploss(),
            ));
        }

        let mut effects = Vec::with_capacity(preflight.len());
        for (id, position_symbol, side, old) in preflight {
            let pos = self
                .manager
                .get_mut(&id)
                .expect("position was validated above");
            pos.set_stoploss(price);
            pos.data.records.push((
                PositionRecord::StoplossModified {
                    from: old,
                    to: price,
                },
                ts,
            ));
            if let Some(register) = self.alert_register.as_mut() {
                replace_stoploss_alert(register, &position_symbol, side, &id, old, price);
            }
            effects.push(Effect::StoplossModified {
                id,
                old_price: old.unwrap_or(0.0),
                new_price: price,
            });
        }
        Ok(effects)
    }

    /// Close all open positions belonging to a group.
    fn action_close_all_in_group(
        &mut self,
        group_id: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let ids = self.manager.open_ids_by_group(group_id);
        let mut effects = Vec::new();
        for id in &ids {
            if let Some(pos) = self.manager.get_mut(id)
                && pos.data.status == PositionStatus::Open
            {
                pos.data.apply_full_close(CloseReason::GroupRule, ts);
                effects.push(Effect::PositionClosed {
                    id: id.clone(),
                    reason: CloseReason::GroupRule,
                });
            }
        }
        // Deregister alerts for all closed positions.
        if let Some(ref mut register) = self.alert_register {
            for id in &ids {
                register.deregister_position(id);
            }
        }
        Ok(effects)
    }

    /// Set the stoploss for all open positions in a group.
    fn action_modify_all_stoploss_in_group(
        &mut self,
        group_id: &str,
        price: f64,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        validate_positive_price("group bulk stoploss price", price)?;
        let mut ids = self.manager.open_ids_by_group(group_id);
        ids.sort();
        let mut preflight = Vec::with_capacity(ids.len());
        for id in &ids {
            let pos = self
                .manager
                .get(id)
                .expect("open group position id came from the manager");
            let entry = position_entry_basis(pos)?;
            validate_stop_price("group bulk stoploss", pos.data.side, entry, price)?;
            preflight.push((
                id.clone(),
                pos.data.symbol.clone(),
                pos.data.side,
                pos.current_stoploss(),
            ));
        }

        let mut effects = Vec::with_capacity(preflight.len());
        for (id, symbol, side, old) in preflight {
            let pos = self
                .manager
                .get_mut(&id)
                .expect("position was validated above");
            pos.set_stoploss(price);
            pos.data.records.push((
                PositionRecord::StoplossModified {
                    from: old,
                    to: price,
                },
                ts,
            ));
            if let Some(register) = self.alert_register.as_mut() {
                replace_stoploss_alert(register, &symbol, side, &id, old, price);
            }
            effects.push(Effect::StoplossModified {
                id,
                old_price: old.unwrap_or(0.0),
                new_price: price,
            });
        }
        Ok(effects)
    }

    // ── Internal: apply an effect to position state ─────────────────────

    /// Apply a single effect produced by rule evaluation to the internal
    /// position state.  This is called for effects that come out of
    /// `evaluate_rules`, **not** for effects produced by `apply_action`
    /// (which already modify state directly).
    fn apply_effect(&mut self, effect: &Effect, quote: &PriceQuote) {
        match effect {
            Effect::PositionClosed { id, reason } => {
                if let Some(pos) = self.manager.get_mut(id) {
                    if pos.data.status != PositionStatus::Open {
                        return; // already terminal (e.g. SL and TP on the same tick)
                    }
                    pos.data.apply_full_close(*reason, quote.ts);
                }
                // Deregister all alerts for this position.
                if let Some(ref mut register) = self.alert_register {
                    register.deregister_position(id);
                }
            }
            Effect::PartialClose { id, ratio, reason } => {
                if let Some(pos) = self.manager.get_mut(id) {
                    if pos.data.status != PositionStatus::Open {
                        return;
                    }
                    let close_price = quote.close_price(pos.data.side);
                    pos.data
                        .apply_partial_close(*ratio, close_price, *reason, quote.ts);
                }
            }
            Effect::StoplossModified { id, new_price, .. } => {
                if !new_price.is_finite() || *new_price <= 0.0 {
                    return;
                }
                let old_and_info = if let Some(pos) = self.manager.get_mut(id) {
                    if pos.data.status != PositionStatus::Open {
                        return;
                    }
                    let old = pos.set_stoploss(*new_price);
                    pos.data.records.push((
                        PositionRecord::StoplossModified {
                            from: old,
                            to: *new_price,
                        },
                        quote.ts,
                    ));
                    Some((old, pos.data.symbol.clone(), pos.data.side))
                } else {
                    None
                };

                // Re-register SL alert if alert register is active.
                if let Some((old, symbol, side)) = old_and_info
                    && let Some(ref mut register) = self.alert_register
                {
                    if let Some(old_price) = old {
                        register.deregister_alert(
                            &symbol,
                            old_price,
                            id,
                            side,
                            &AlertKind::Stoploss,
                        );
                    }
                    register.register(&symbol, *new_price, id.clone(), side, AlertKind::Stoploss);
                }
            }
            // Other effects are informational — no internal state change needed.
            _ => {}
        }
    }

    fn apply_future_intent(
        &mut self,
        intent: FutureIntent,
        quote: &PriceQuote,
        pricer: &ExecutionPricer,
        pip_size: f64,
    ) -> FutureApplyResult<FutureEffect> {
        match intent.effect {
            Effect::PositionClosed { id, reason } => {
                let position = self
                    .manager
                    .get(&id)
                    .ok_or_else(|| CoreError::PositionNotFound(id.clone()))?;
                let side = position.data.side;
                let close_size = position.data.remaining_size();
                let purpose = fill_purpose_for_close(reason);
                let execution =
                    pricer.price(purpose, side, quote, intent.requested_price, pip_size)?;
                validate_future_execution(&execution, purpose, side, &id)?;

                let position = self
                    .manager
                    .get_mut(&id)
                    .ok_or_else(|| CoreError::PositionNotFound(id.clone()))?;
                if position.data.status != PositionStatus::Open {
                    return Err(CoreError::InvalidState {
                        id,
                        expected: "Open".into(),
                        actual: position.data.status.to_string(),
                    }
                    .into());
                }
                position.data.apply_full_close(reason, quote.ts);
                if let Some(ref mut register) = self.alert_register {
                    register.deregister_position(&id);
                }
                Ok(FutureEffect::filled(
                    Effect::PositionClosed { id, reason },
                    FutureFill {
                        execution,
                        size: close_size,
                        ts: quote.ts,
                        source_quote_ts: Some(quote.ts),
                    },
                    intent.stop_origin,
                ))
            }
            Effect::PartialClose { id, ratio, reason } => {
                let position = self
                    .manager
                    .get(&id)
                    .ok_or_else(|| CoreError::PositionNotFound(id.clone()))?;
                let side = position.data.side;
                let purpose = fill_purpose_for_close(reason);
                let execution =
                    pricer.price(purpose, side, quote, intent.requested_price, pip_size)?;
                validate_future_execution(&execution, purpose, side, &id)?;

                let position = self
                    .manager
                    .get_mut(&id)
                    .ok_or_else(|| CoreError::PositionNotFound(id.clone()))?;
                if position.data.status != PositionStatus::Open {
                    return Err(CoreError::InvalidState {
                        id,
                        expected: "Open".into(),
                        actual: position.data.status.to_string(),
                    }
                    .into());
                }
                let actual_ratio = position.data.capped_close_ratio(ratio);
                let close_size = position.data.close_size_for_ratio(actual_ratio);
                position
                    .data
                    .apply_partial_close(actual_ratio, execution.price, reason, quote.ts);
                Ok(FutureEffect::filled(
                    Effect::PartialClose {
                        id,
                        ratio: actual_ratio,
                        reason,
                    },
                    FutureFill {
                        execution,
                        size: close_size,
                        ts: quote.ts,
                        source_quote_ts: Some(quote.ts),
                    },
                    intent.stop_origin,
                ))
            }
            Effect::StoplossModified {
                id,
                old_price,
                new_price,
            } => {
                let effect = Effect::StoplossModified {
                    id: id.clone(),
                    old_price,
                    new_price,
                };
                self.apply_effect(&effect, quote);
                if let Some(origin) = intent.stop_origin
                    && let Some(position) = self.manager.get_mut(&id)
                    && position.data.status == PositionStatus::Open
                {
                    position.set_stoploss_with_origin(new_price, origin);
                }
                Ok(FutureEffect::plain_with_metadata(
                    effect,
                    intent.requested_price,
                    intent.stop_origin,
                ))
            }
            effect => Ok(FutureEffect::plain_with_metadata(
                effect,
                intent.requested_price,
                intent.stop_origin,
            )),
        }
    }

    // ── Alert register helpers ──────────────────────────────────────────

    /// Register all static rule alerts for a position (called after open/fill).
    fn register_alerts_for_position(&mut self, position_id: &str, symbol: &str, side: Side) {
        if self.alert_register.is_none() {
            return;
        }

        let rules_snapshot: Vec<Rule> = {
            let pos = match self.manager.get(position_id) {
                Some(p) => p,
                None => return,
            };
            pos.rules.clone()
        };

        let register = self.alert_register.as_mut().unwrap();
        let mut has_stateful = false;

        for rule in &rules_snapshot {
            match rule {
                Rule::FixedStoploss { price } => {
                    register.register(
                        symbol,
                        *price,
                        position_id.to_owned(),
                        side,
                        AlertKind::Stoploss,
                    );
                }
                Rule::TakeProfit {
                    price,
                    close_ratio,
                    triggered,
                } => {
                    if !triggered {
                        register.register(
                            symbol,
                            *price,
                            position_id.to_owned(),
                            side,
                            AlertKind::TakeProfit {
                                close_ratio: *close_ratio,
                            },
                        );
                    }
                }
                Rule::BreakevenWhen {
                    trigger_price,
                    triggered,
                } => {
                    if !triggered {
                        register.register(
                            symbol,
                            *trigger_price,
                            position_id.to_owned(),
                            side,
                            AlertKind::BreakevenTrigger,
                        );
                    }
                }
                Rule::TrailingStop { .. }
                | Rule::TimeExit { .. }
                | Rule::BreakevenAfterTargets { .. } => {
                    has_stateful = true;
                }
            }
        }

        if has_stateful {
            register.register_tick_eval(symbol, position_id.to_owned());
        }
    }

    /// Apply a triggered alert — convert it into effects and apply them.
    fn apply_triggered_alert(&mut self, alert: &TriggeredAlert, quote: &PriceQuote) -> Vec<Effect> {
        match &alert.kind {
            AlertKind::Stoploss => {
                let pos = match self.manager.get_mut(&alert.position_id) {
                    Some(p) if p.data.status == PositionStatus::Open => p,
                    _ => return vec![],
                };
                pos.data.apply_full_close(CloseReason::Stoploss, quote.ts);
                // Deregister all remaining alerts for this position.
                if let Some(ref mut register) = self.alert_register {
                    register.deregister_position(&alert.position_id);
                }
                vec![Effect::PositionClosed {
                    id: alert.position_id.clone(),
                    reason: CloseReason::Stoploss,
                }]
            }
            AlertKind::TakeProfit { close_ratio } => {
                let pos = match self.manager.get_mut(&alert.position_id) {
                    Some(p) if p.data.status == PositionStatus::Open => p,
                    _ => return vec![],
                };

                let remaining = pos.data.open_ratio();
                let actual_ratio = pos.data.capped_close_ratio(*close_ratio);

                // Mark the corresponding TakeProfit rule as triggered.
                for rule in &mut pos.rules {
                    if let Rule::TakeProfit {
                        price, triggered, ..
                    } = rule
                        && !*triggered
                        && (price_to_micros_static(*price)
                            == price_to_micros_static(alert.trigger_price))
                    {
                        *triggered = true;
                        break;
                    }
                }

                if remaining - actual_ratio <= position_size_tolerance(1.0) {
                    // Full close via TP.
                    let close_price = quote.close_price(alert.side);
                    pos.data.apply_partial_close(
                        actual_ratio,
                        close_price,
                        CloseReason::Target,
                        quote.ts,
                    );
                    if let Some(ref mut register) = self.alert_register {
                        register.deregister_position(&alert.position_id);
                    }
                    vec![Effect::PositionClosed {
                        id: alert.position_id.clone(),
                        reason: CloseReason::Target,
                    }]
                } else {
                    // Partial close via TP.
                    let close_price = quote.close_price(alert.side);
                    pos.data.apply_partial_close(
                        actual_ratio,
                        close_price,
                        CloseReason::Target,
                        quote.ts,
                    );
                    vec![Effect::PartialClose {
                        id: alert.position_id.clone(),
                        ratio: actual_ratio,
                        reason: CloseReason::Target,
                    }]
                }
            }
            AlertKind::BreakevenTrigger => {
                let (entry_price, _symbol, _side) = {
                    let pos = match self.manager.get_mut(&alert.position_id) {
                        Some(p) if p.data.status == PositionStatus::Open => p,
                        _ => return vec![],
                    };

                    // Mark breakeven rule as triggered.
                    for rule in &mut pos.rules {
                        if let Rule::BreakevenWhen { triggered, .. } = rule {
                            *triggered = true;
                            break;
                        }
                    }

                    (
                        pos.data.average_entry(),
                        pos.data.symbol.clone(),
                        pos.data.side,
                    )
                };

                // Move SL to entry — produces a StoplossModified effect.
                let effect = Effect::StoplossModified {
                    id: alert.position_id.clone(),
                    old_price: 0.0,
                    new_price: entry_price,
                };
                self.apply_effect(&effect, quote);
                vec![effect]
            }
            AlertKind::PendingFill { .. } => {
                // Fill the pending order.
                let pos = match self.manager.get_mut(&alert.position_id) {
                    Some(p) if p.data.status == PositionStatus::Pending => p,
                    _ => return vec![],
                };

                let fill_model = self.fill_model;
                if pos.try_fill(quote, fill_model) {
                    let symbol = pos.data.symbol.clone();
                    let side = pos.data.side;
                    let id = alert.position_id.clone();

                    // Now register alerts for the newly opened position.
                    self.register_alerts_for_position(&id, &symbol, side);

                    vec![Effect::PositionOpened { id }]
                } else {
                    vec![]
                }
            }
        }
    }
}

fn core_error_from_manager(error: PositionManagerError) -> CoreError {
    match error {
        PositionManagerError::PositionNotFound(id) => CoreError::PositionNotFound(id),
        duplicate @ PositionManagerError::DuplicateTradeId { .. } => {
            CoreError::InvalidAction(duplicate.to_string())
        }
    }
}

fn validate_target_ratio(context: &str, ratio: f64) -> Result<()> {
    if ratio.is_finite() && ratio > 0.0 && ratio <= 1.0 {
        Ok(())
    } else {
        Err(CoreError::InvalidAction(format!(
            "{context} close ratio must be finite and in (0, 1], got {ratio}"
        )))
    }
}

fn valid_position_size(size: f64) -> bool {
    size.is_finite() && size > position_size_tolerance(size)
}

fn validate_position_size(context: &str, size: f64) -> Result<()> {
    if valid_position_size(size) {
        Ok(())
    } else {
        Err(CoreError::InvalidAction(format!(
            "{context} must be finite and greater than the accounting tolerance, got {size}"
        )))
    }
}

fn validate_positive_price(context: &str, price: f64) -> Result<()> {
    if price.is_finite() && price > 0.0 {
        Ok(())
    } else {
        Err(CoreError::InvalidAction(format!(
            "{context} must be finite and positive, got {price}"
        )))
    }
}

fn validate_stop_price(context: &str, side: Side, entry: f64, price: f64) -> Result<()> {
    validate_positive_price(context, price)?;
    let valid = match side {
        Side::Buy => price < entry,
        Side::Sell => price > entry,
    };
    if valid {
        Ok(())
    } else {
        Err(CoreError::InvalidAction(format!(
            "{context} has invalid {side} geometry: entry {entry}, stop {price}"
        )))
    }
}

fn validate_target_price(context: &str, side: Side, entry: f64, price: f64) -> Result<()> {
    validate_positive_price(context, price)?;
    let valid = match side {
        Side::Buy => price > entry,
        Side::Sell => price < entry,
    };
    if valid {
        Ok(())
    } else {
        Err(CoreError::InvalidAction(format!(
            "{context} has invalid {side} geometry: entry {entry}, target {price}"
        )))
    }
}

fn register_unique_target_price(
    context: &str,
    price: f64,
    target_price_keys: &mut Vec<i64>,
) -> Result<()> {
    let key = price_to_micros_static(price);
    if target_price_keys.contains(&key) {
        return Err(CoreError::InvalidAction(format!(
            "{context} duplicates take-profit price {price}"
        )));
    }
    target_price_keys.push(key);
    Ok(())
}

fn validate_rule_config(
    context: &str,
    rule: &RuleConfig,
    side: Side,
    entry: Option<f64>,
    target_price_keys: &mut Vec<i64>,
) -> Result<()> {
    match rule {
        RuleConfig::FixedStoploss { price } => match entry {
            Some(entry) => validate_stop_price(context, side, entry, *price),
            None => validate_positive_price(context, *price),
        },
        RuleConfig::TrailingStop { distance } => {
            validate_positive_price(&format!("{context} trailing distance"), *distance)?;
            if let Some(entry) = entry {
                let initial_stop = match side {
                    Side::Buy => entry - distance,
                    Side::Sell => entry + distance,
                };
                validate_stop_price(
                    &format!("{context} initial trailing stop"),
                    side,
                    entry,
                    initial_stop,
                )?;
            }
            Ok(())
        }
        RuleConfig::TakeProfit { price, close_ratio } => {
            validate_target_ratio(&format!("{context} take-profit"), *close_ratio)?;
            match entry {
                Some(entry) => validate_target_price(context, side, entry, *price)?,
                None => validate_positive_price(context, *price)?,
            }
            register_unique_target_price(context, *price, target_price_keys)
        }
        RuleConfig::BreakevenWhen { trigger_price } => match entry {
            Some(entry) => validate_target_price(context, side, entry, *trigger_price),
            None => validate_positive_price(context, *trigger_price),
        },
        RuleConfig::BreakevenAfterTargets { after_n } => {
            if *after_n == 0 {
                Err(CoreError::InvalidAction(format!(
                    "{context} target count must be greater than zero"
                )))
            } else {
                Ok(())
            }
        }
        RuleConfig::TimeExit { max_seconds } => {
            if *max_seconds == 0 {
                Err(CoreError::InvalidAction(format!(
                    "{context} maximum seconds must be greater than zero"
                )))
            } else {
                Ok(())
            }
        }
    }
}

fn ensure_management_status(position: &Position, allow_pending: bool) -> Result<()> {
    let valid = position.data.status == PositionStatus::Open
        || (allow_pending && position.data.status == PositionStatus::Pending);
    if valid {
        Ok(())
    } else {
        Err(CoreError::InvalidState {
            id: position.data.id.clone(),
            expected: if allow_pending {
                "Open or Pending".into()
            } else {
                "Open".into()
            },
            actual: position.data.status.to_string(),
        })
    }
}

fn position_entry_basis(position: &Position) -> Result<f64> {
    let entry = match position.data.status {
        PositionStatus::Open => position.data.average_entry(),
        PositionStatus::Pending => position.data.pending_price.ok_or_else(|| {
            CoreError::InvalidAction(format!(
                "pending position {} has no entry price",
                position.data.id
            ))
        })?,
        PositionStatus::Closed | PositionStatus::Cancelled => {
            return Err(CoreError::InvalidState {
                id: position.data.id.clone(),
                expected: "Open or Pending".into(),
                actual: position.data.status.to_string(),
            });
        }
    };
    validate_positive_price("position entry basis", entry)?;
    Ok(entry)
}

fn target_price_keys(position: &Position) -> Vec<i64> {
    position
        .rules
        .iter()
        .filter_map(|rule| match rule {
            Rule::TakeProfit { price, .. } => Some(price_to_micros_static(*price)),
            _ => None,
        })
        .collect()
}

fn ensure_target_price_available(
    position: &Position,
    price: f64,
    except_index: Option<usize>,
) -> Result<()> {
    let duplicate = position.rules.iter().enumerate().any(|(index, rule)| {
        except_index != Some(index)
            && matches!(rule, Rule::TakeProfit { price: existing, .. } if same_alert_price(*existing, price))
    });
    if duplicate {
        Err(CoreError::InvalidAction(format!(
            "take-profit price {price} is already present for position {}",
            position.data.id
        )))
    } else {
        Ok(())
    }
}

fn same_alert_price(left: f64, right: f64) -> bool {
    price_to_micros_static(left) == price_to_micros_static(right)
}

fn replace_stoploss_alert(
    register: &mut PriceAlertRegister,
    symbol: &str,
    side: Side,
    position_id: &str,
    old_price: Option<f64>,
    new_price: f64,
) {
    if let Some(old_price) = old_price {
        register.deregister_alert(symbol, old_price, position_id, side, &AlertKind::Stoploss);
    }
    register.register(
        symbol,
        new_price,
        position_id.to_owned(),
        side,
        AlertKind::Stoploss,
    );
}

fn fill_purpose_for_close(reason: CloseReason) -> FillPurpose {
    match reason {
        CloseReason::Target => FillPurpose::TakeProfit,
        CloseReason::Stoploss | CloseReason::TrailingStop | CloseReason::BreakevenStop => {
            FillPurpose::StopLoss
        }
        _ => FillPurpose::MarketExit,
    }
}

fn validate_future_execution(
    execution: &ExecutionFill,
    expected_purpose: FillPurpose,
    expected_side: Side,
    position_id: &str,
) -> FutureApplyResult<()> {
    if execution.purpose != expected_purpose {
        return Err(FutureApplyError::InvalidPreparedFill {
            position_id: position_id.to_owned(),
            reason: format!("expected {expected_purpose:?}, got {:?}", execution.purpose),
        });
    }
    if execution.side != expected_side {
        return Err(FutureApplyError::InvalidPreparedFill {
            position_id: position_id.to_owned(),
            reason: format!("expected side {expected_side:?}, got {:?}", execution.side),
        });
    }
    if !execution.price.is_finite() || execution.price <= 0.0 {
        return Err(FutureApplyError::InvalidPreparedFill {
            position_id: position_id.to_owned(),
            reason: format!(
                "execution price must be finite and positive, got {}",
                execution.price
            ),
        });
    }
    Ok(())
}

/// Helper to convert price to micros (standalone function usable in non-method contexts).
fn price_to_micros_static(price: f64) -> i64 {
    (price * 1_000_000.0).round() as i64
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        Action, ExecutionConvention, OrderType, RuleConfig, SlippageModel, StopOrigin, TargetSpec,
    };
    use chrono::NaiveDate;

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

    fn execution(purpose: FillPurpose, side: Side, price: f64) -> ExecutionFill {
        ExecutionFill {
            purpose,
            side,
            price,
            quote_price: price,
            requested_price: None,
            slippage_pips: 0.0,
        }
    }

    fn effect_position_id(effect: &Effect) -> &str {
        match effect {
            Effect::OrderPlaced { id }
            | Effect::OrderCancelled { id }
            | Effect::PositionOpened { id }
            | Effect::PositionClosed { id, .. }
            | Effect::PartialClose { id, .. }
            | Effect::StoplossModified { id, .. }
            | Effect::StoplossRemoved { id, .. }
            | Effect::ScaledIn { id, .. }
            | Effect::RuleTriggered { id, .. } => id,
        }
    }

    fn open_future_position(
        engine: &mut TradeEngine,
        side: Side,
        stoploss: Option<f64>,
        rules: Vec<RuleConfig>,
    ) -> PositionId {
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side,
                    order_type: OrderType::Market,
                    price: Some(100.0),
                    size: 1.0,
                    stoploss,
                    targets: vec![],
                    rules,
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected position-opened effect, got {effect:?}"),
        }
    }

    #[test]
    fn future_priced_actions_carry_the_authoritative_fill_into_state() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let open_quote = quote("EURUSD", 99.0, 100.0, ts(10, 0, 0));
        let open_execution = execution(FillPurpose::MarketEntry, Side::Buy, 100.25);
        let effects = engine
            .apply_priced_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0),
                    size: 2.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                &open_quote,
                open_execution,
            )
            .unwrap();
        let id = match effects.as_slice() {
            [
                FutureEffect::Filled {
                    effect: Effect::PositionOpened { id },
                    fill,
                    ..
                },
            ] => {
                assert_eq!(fill.execution, open_execution);
                assert_eq!(fill.size, 2.0);
                assert_eq!(fill.ts, open_quote.ts);
                id.clone()
            }
            other => panic!("unexpected open effects: {other:?}"),
        };
        assert_eq!(
            engine.get_position(&id).unwrap().data.entries[0].price,
            100.25
        );

        let scale_quote = quote("EURUSD", 100.0, 101.0, ts(10, 1, 0));
        let scale_execution = execution(FillPurpose::MarketEntry, Side::Buy, 101.5);
        let effects = engine
            .apply_priced_future_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: Some(2.0),
                    size: 1.0,
                    trade_id: None,
                },
                &scale_quote,
                scale_execution,
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled { fill, .. }]
                if fill.execution == scale_execution && fill.size == 1.0
        ));
        assert_eq!(
            engine.get_position(&id).unwrap().data.entries[1].price,
            101.5
        );

        let partial_quote = quote("EURUSD", 98.0, 99.0, ts(10, 2, 0));
        let partial_execution = execution(FillPurpose::MarketExit, Side::Buy, 97.75);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePartial {
                    position_id: id.clone(),
                    ratio: 0.25,
                },
                &partial_quote,
                partial_execution,
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled {
                effect: Effect::PartialClose { ratio, .. },
                fill,
                ..
            }] if (*ratio - 0.25).abs() < f64::EPSILON
                && fill.execution == partial_execution
                && (fill.size - 0.75).abs() < f64::EPSILON
        ));
        let position = engine.get_position(&id).unwrap();
        assert!(position.data.records.iter().any(|(record, recorded_ts)| {
            matches!(
                record,
                PositionRecord::PartialClose { price, .. }
                    if (*price - partial_execution.price).abs() < f64::EPSILON
                        && *recorded_ts == partial_quote.ts
            )
        }));

        let close_quote = quote("EURUSD", 97.0, 98.0, ts(10, 3, 0));
        let close_execution = execution(FillPurpose::MarketExit, Side::Buy, 96.5);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                &close_quote,
                close_execution,
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled {
                effect: Effect::PositionClosed { .. },
                fill,
                ..
            }] if fill.execution == close_execution
                && (fill.size - 2.25).abs() < f64::EPSILON
        ));
        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.status, PositionStatus::Closed);
        assert_eq!(position.data.close_ts, Some(close_quote.ts));
    }

    #[test]
    fn future_pending_gap_fill_is_priced_once_and_carried() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let effects = engine
            .apply_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Stop,
                    price: Some(101.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(9, 59, 0),
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::OrderPlaced { id } => id.clone(),
            effect => panic!("expected order placement, got {effect:?}"),
        };
        let gap_quote = quote("EURUSD", 102.0, 102.25, ts(10, 0, 0));
        let pricer = ExecutionPricer::new(ExecutionModel::new(
            ExecutionConvention::FutureQuoteV1,
            FillModel::BidAsk,
            SlippageModel::FixedPips { pips: 0.5 },
        ));
        let priced = pricer
            .stop_entry(Side::Buy, &gap_quote, 101.0, 0.1)
            .unwrap();
        let effects = engine
            .on_price_future_effects_priced(
                &gap_quote,
                &[PreparedPendingFill {
                    position_id: id.clone(),
                    execution: priced,
                    size: 1.0,
                }],
                &pricer,
                0.1,
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled {
                effect: Effect::PositionOpened { id: effect_id },
                fill,
                ..
            }] if effect_id == &id && fill.execution == priced && fill.size == 1.0
        ));
        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.status, PositionStatus::Open);
        assert_eq!(position.data.entries[0].price, priced.price);
        assert_eq!(position.data.entries[0].ts, gap_quote.ts);
    }

    #[test]
    fn future_open_transaction_rollback_restores_indexes_and_id_sequence() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let open_quote = quote("EURUSD", 99.0, 100.0, ts(10, 0, 0));
        let action = Action::Open {
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            size: 1.0,
            stoploss: None,
            targets: vec![],
            rules: vec![],
            group: Some("group-a".into()),
            trade_id: Some("trade-a".into()),
        };
        let transaction = engine
            .begin_priced_future_action(
                action.clone(),
                &open_quote,
                execution(FillPurpose::MarketEntry, Side::Buy, 100.0),
            )
            .unwrap();
        let first_id = effect_position_id(transaction.effects()[0].effect()).to_owned();
        assert_eq!(first_id, "position:00000000");
        assert_eq!(
            engine.manager.id_by_trade_id("trade-a"),
            Some(first_id.clone())
        );
        assert_eq!(engine.manager.group_position_ids("group-a"), vec![first_id]);

        transaction.rollback(&mut engine);

        assert!(engine.manager.is_empty());
        assert_eq!(engine.manager.id_by_trade_id("trade-a"), None);
        assert!(engine.manager.group_position_ids("group-a").is_empty());
        let transaction = engine
            .begin_priced_future_action(
                action,
                &open_quote,
                execution(FillPurpose::MarketEntry, Side::Buy, 100.0),
            )
            .unwrap();
        assert_eq!(
            effect_position_id(transaction.effects()[0].effect()),
            "position:00000000"
        );
        transaction.commit();
    }

    #[test]
    fn future_pending_quote_transaction_rollback_restores_quote_and_position() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let pricer = ExecutionPricer::new(ExecutionModel::future_quote_v1(FillModel::BidAsk));
        let previous_quote = quote("EURUSD", 100.0, 100.5, ts(9, 59, 0));
        engine
            .on_price_future_effects_priced(&previous_quote, &[], &pricer, 0.1)
            .unwrap();
        let effects = engine
            .apply_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Limit,
                    price: Some(99.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: Some("pending-a".into()),
                },
                ts(9, 59, 30),
            )
            .unwrap();
        let id = effect_position_id(effects[0].effect()).to_owned();
        let trigger_quote = quote("EURUSD", 98.0, 98.5, ts(10, 0, 0));
        let transaction = engine
            .begin_on_price_future_effects_priced(
                &trigger_quote,
                &[PreparedPendingFill {
                    position_id: id.clone(),
                    execution: ExecutionFill {
                        purpose: FillPurpose::LimitEntry,
                        side: Side::Buy,
                        price: 98.5,
                        quote_price: 98.5,
                        requested_price: Some(99.0),
                        slippage_pips: 0.0,
                    },
                    size: 1.0,
                }],
                &pricer,
                0.1,
            )
            .unwrap();
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );
        let current_quote = engine.last_quote("EURUSD").unwrap();
        assert_eq!(current_quote.ts, trigger_quote.ts);
        assert_eq!(current_quote.bid, trigger_quote.bid);
        assert_eq!(current_quote.ask, trigger_quote.ask);

        transaction.rollback(&mut engine);

        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Pending
        );
        assert_eq!(engine.manager.id_by_trade_id("pending-a"), Some(id));
        let restored_quote = engine.last_quote("EURUSD").unwrap();
        assert_eq!(restored_quote.ts, previous_quote.ts);
        assert_eq!(restored_quote.bid, previous_quote.bid);
        assert_eq!(restored_quote.ask, previous_quote.ask);
    }

    #[test]
    fn future_quote_accounting_rollback_restores_rule_and_alert_state() {
        let mut engine = TradeEngine::with_alert_register_and_fill_model(FillModel::BidAsk);
        let pricer = ExecutionPricer::new(ExecutionModel::future_quote_v1(FillModel::BidAsk));
        let id = open_future_position(
            &mut engine,
            Side::Buy,
            None,
            vec![RuleConfig::TakeProfit {
                price: 105.0,
                close_ratio: 1.0,
            }],
        );
        let trigger_quote = quote("EURUSD", 106.0, 106.5, ts(10, 1, 0));

        let transaction = engine
            .begin_on_price_future_effects_priced(&trigger_quote, &[], &pricer, 0.1)
            .unwrap();

        assert!(matches!(
            transaction.effects(),
            [FutureEffect::Filled {
                effect: Effect::PositionClosed { id: effect_id, .. },
                ..
            }] if effect_id == &id
        ));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
        assert!(!engine.alert_register.as_ref().unwrap().has_alerts(&id));

        transaction.rollback(&mut engine);

        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.status, PositionStatus::Open);
        assert!(matches!(
            position.rules.as_slice(),
            [Rule::TakeProfit {
                triggered: false,
                ..
            }]
        ));
        assert!(engine.alert_register.as_ref().unwrap().has_alerts(&id));
        assert!(engine.last_quote("EURUSD").is_none());
    }

    #[test]
    fn future_transaction_reports_empty_effects_without_losing_rollback() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let id = open_future_position(&mut engine, Side::Buy, None, vec![]);
        let rules_before = engine.get_position(&id).unwrap().rules.len();
        let transaction = engine
            .begin_future_action(
                Action::AddTarget {
                    position_id: id.clone(),
                    price: 105.0,
                    close_ratio: 0.5,
                },
                ts(10, 1, 0),
            )
            .unwrap();
        assert!(!transaction.has_effects());
        assert_eq!(
            engine.get_position(&id).unwrap().rules.len(),
            rules_before + 1
        );

        transaction.rollback(&mut engine);

        assert_eq!(engine.get_position(&id).unwrap().rules.len(), rules_before);
    }

    #[test]
    fn future_rule_pricing_failure_is_atomic() {
        let mut engine = TradeEngine::new();
        let id = open_future_position(
            &mut engine,
            Side::Buy,
            None,
            vec![RuleConfig::TakeProfit {
                price: 105.0,
                close_ratio: 0.5,
            }],
        );
        let before = engine.get_position(&id).unwrap();
        let before_status = before.data.status;
        let before_remaining = before.data.remaining_ratio;
        let before_records = before.data.records.len();
        let before_triggered = matches!(
            before.rules.as_slice(),
            [Rule::TakeProfit {
                triggered: false,
                ..
            }]
        );
        assert!(before_triggered);

        let pricer = ExecutionPricer::new(ExecutionModel::new(
            ExecutionConvention::FutureQuoteV1,
            FillModel::BidAsk,
            SlippageModel::FixedPips { pips: f64::NAN },
        ));
        let result = engine.on_price_future_effects_priced(
            &quote("EURUSD", 106.0, 106.1, ts(10, 1, 0)),
            &[],
            &pricer,
            0.1,
        );
        assert!(matches!(
            result,
            Err(FutureApplyError::Pricing(ExecutionError::InvalidSlippage(value)))
                if value.is_nan()
        ));

        let after = engine.get_position(&id).unwrap();
        assert_eq!(after.data.status, before_status);
        assert_eq!(after.data.remaining_ratio, before_remaining);
        assert_eq!(after.data.records.len(), before_records);
        assert!(matches!(
            after.rules.as_slice(),
            [Rule::TakeProfit {
                triggered: false,
                ..
            }]
        ));
    }

    #[test]
    fn open_market_order() {
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
                    targets: vec![TargetSpec {
                        price: 1.0900,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        assert_eq!(effects.len(), 1);
        assert!(matches!(&effects[0], Effect::PositionOpened { .. }));

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.data.status, PositionStatus::Open);
        assert_eq!(pos.data.side, Side::Buy);
        assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
        // 2 rules: FixedStoploss + TakeProfit
        assert_eq!(pos.rules.len(), 2);
    }

    #[test]
    fn open_market_order_uses_last_quote() {
        let mut engine = TradeEngine::new();
        // Seed a quote
        engine.on_price(&quote("EURUSD", 1.0848, 1.0850, ts(9, 59, 0)));

        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: None, // should use ask from last quote
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };
        let pos = engine.get_position(&id).unwrap();
        assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn open_limit_order_and_fill() {
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
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(9, 0, 0),
            )
            .unwrap();

        assert!(matches!(&effects[0], Effect::OrderPlaced { .. }));
        let id = match &effects[0] {
            Effect::OrderPlaced { id } => id.clone(),
            _ => panic!(),
        };

        // Price not yet at limit
        let effects = engine.on_price(&quote("EURUSD", 1.0810, 1.0812, ts(10, 0, 0)));
        assert!(effects.is_empty());
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Pending
        );

        // Price drops to limit
        let effects = engine.on_price(&quote("EURUSD", 1.0798, 1.0800, ts(10, 5, 0)));
        assert_eq!(effects.len(), 1);
        assert!(matches!(&effects[0], Effect::PositionOpened { .. }));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );
    }

    #[test]
    fn stoploss_triggers_on_price() {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price above SL
        let effects = engine.on_price(&quote("EURUSD", 1.0840, 1.0842, ts(10, 1, 0)));
        assert!(effects.is_empty());

        // Price hits SL
        let effects = engine.on_price(&quote("EURUSD", 1.0799, 1.0801, ts(10, 2, 0)));
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        ));

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.data.status, PositionStatus::Closed);
    }

    #[test]
    fn take_profit_partial_then_stoploss() {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // TP hit: partial close 50%
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 5, 0)));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::PartialClose { ratio, .. } if (*ratio - 0.5).abs() < f64::EPSILON)));

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.data.status, PositionStatus::Open);
        assert!((pos.data.remaining_ratio - 0.5).abs() < f64::EPSILON);
        assert_eq!(pos.data.target_hits, 1);

        // Now SL hit
        let effects = engine.on_price(&quote("EURUSD", 1.0799, 1.0801, ts(10, 10, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.data.status, PositionStatus::Closed);
    }

    #[test]
    fn scale_in() {
        let mut engine = TradeEngine::new();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        let effects = engine
            .apply_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: Some(1.0900),
                    size: 1.0,
                    trade_id: None,
                },
                ts(10, 5, 0),
            )
            .unwrap();

        assert!(matches!(&effects[0], Effect::ScaledIn { .. }));

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.data.entries.len(), 2);
        // Avg: (1.0800 + 1.0900) / 2 = 1.0850
        assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn partial_close_then_scale_in_conserves_lots_through_engine() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0800),
                    size: 2.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected open, got {effect:?}"),
        };

        engine
            .apply_action(
                Action::ClosePartial {
                    position_id: id.clone(),
                    ratio: 0.5,
                },
                ts(10, 1, 0),
            )
            .unwrap();
        engine
            .apply_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: Some(1.0900),
                    size: 1.0,
                    trade_id: None,
                },
                ts(10, 2, 0),
            )
            .unwrap();

        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.total_filled_size(), 3.0);
        assert_eq!(position.data.closed_size, 1.0);
        assert_eq!(position.data.remaining_size(), 2.0);
        assert!((position.data.remaining_ratio - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn target_ratios_are_validated_atomically_at_core_boundaries() {
        for ratio in [0.0, -0.1, 1.1, f64::NAN, f64::INFINITY] {
            let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
            let result = engine.apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(100.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![TargetSpec {
                        price: 101.0,
                        close_ratio: ratio,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            );
            assert!(matches!(result, Err(CoreError::InvalidAction(_))));
            assert!(engine.open_positions().is_empty());
        }

        let mut invalid_rule_engine = TradeEngine::new();
        assert!(matches!(
            invalid_rule_engine.apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(100.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![RuleConfig::TakeProfit {
                        price: 101.0,
                        close_ratio: f64::NEG_INFINITY,
                    }],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            ),
            Err(CoreError::InvalidAction(_))
        ));
        assert!(invalid_rule_engine.open_positions().is_empty());

        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let id = open_future_position(&mut engine, Side::Buy, None, vec![]);
        let before_rules = engine.get_position(&id).unwrap().rules.len();
        let before_records = engine.get_position(&id).unwrap().data.records.len();
        for action in [
            Action::AddTarget {
                position_id: id.clone(),
                price: 101.0,
                close_ratio: f64::NAN,
            },
            Action::AddRule {
                position_id: id.clone(),
                rule: RuleConfig::TakeProfit {
                    price: 102.0,
                    close_ratio: 0.0,
                },
            },
        ] {
            assert!(matches!(
                engine.apply_action(action, ts(10, 1, 0)),
                Err(CoreError::InvalidAction(_))
            ));
            assert_eq!(engine.get_position(&id).unwrap().rules.len(), before_rules);
            assert_eq!(
                engine.get_position(&id).unwrap().data.records.len(),
                before_records
            );
        }

        engine
            .apply_action(
                Action::AddTarget {
                    position_id: id.clone(),
                    price: 103.0,
                    close_ratio: 0.5,
                },
                ts(10, 2, 0),
            )
            .unwrap();
        if let Some(Rule::TakeProfit { close_ratio, .. }) = engine
            .manager
            .get_mut(&id)
            .unwrap()
            .rules
            .iter_mut()
            .find(|rule| matches!(rule, Rule::TakeProfit { price, .. } if *price == 103.0))
        {
            *close_ratio = f64::NAN;
        }
        assert!(matches!(
            engine.apply_action(
                Action::ModifyTarget {
                    position_id: id.clone(),
                    old_price: 103.0,
                    new_price: 104.0,
                },
                ts(10, 3, 0),
            ),
            Err(CoreError::InvalidAction(_))
        ));
        assert!(
            engine
                .get_position(&id)
                .unwrap()
                .rules
                .iter()
                .any(|rule| matches!(rule, Rule::TakeProfit { price, .. } if *price == 103.0))
        );
    }

    #[test]
    fn fixed_stop_rule_add_and_remove_emit_synchronization_effects() {
        let mut engine = TradeEngine::new();
        let id = open_future_position(&mut engine, Side::Buy, None, vec![]);

        let added = engine
            .apply_future_action(
                Action::AddRule {
                    position_id: id.clone(),
                    rule: RuleConfig::FixedStoploss { price: 95.0 },
                },
                ts(10, 1, 0),
            )
            .unwrap();
        assert!(matches!(
            added.as_slice(),
            [FutureEffect::Plain {
                effect: Effect::StoplossModified {
                    old_price: 0.0,
                    new_price: 95.0,
                    ..
                },
                stop_origin: Some(StopOrigin::Modified),
                ..
            }]
        ));
        assert_eq!(
            engine.get_position(&id).unwrap().current_effective_stop(),
            Some(crate::types::EffectiveStop::new(95.0, StopOrigin::Modified))
        );

        let removed = engine
            .apply_future_action(
                Action::RemoveRule {
                    position_id: id.clone(),
                    rule_name: "FixedStoploss".into(),
                },
                ts(10, 2, 0),
            )
            .unwrap();
        assert!(matches!(
            removed.as_slice(),
            [FutureEffect::Plain {
                effect: Effect::StoplossRemoved {
                    old_price: 95.0,
                    ..
                },
                ..
            }]
        ));
        assert!(
            engine
                .get_position(&id)
                .unwrap()
                .current_effective_stop()
                .is_none()
        );
    }

    #[test]
    fn near_full_partial_close_emits_full_close_and_leaves_no_residual() {
        let mut engine = TradeEngine::new();
        let id = open_future_position(&mut engine, Side::Buy, None, vec![]);
        let effects = engine
            .apply_action(
                Action::ClosePartial {
                    position_id: id.clone(),
                    ratio: 1.0 - 5.0e-13,
                },
                ts(10, 1, 0),
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PositionClosed { .. }]
        ));
        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.status, PositionStatus::Closed);
        assert_eq!(position.data.remaining_size(), 0.0);
        assert_eq!(position.data.open_entry_value, 0.0);
    }

    #[test]
    fn engine_rejects_duplicate_trade_ids_without_partial_mutation() {
        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let first = engine
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
                    trade_id: Some("trade-7".into()),
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let first_id = match &first[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected open, got {effect:?}"),
        };

        let duplicate = engine.apply_action(
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
                trade_id: Some("trade-7".into()),
            },
            ts(10, 1, 0),
        );
        assert!(matches!(
            duplicate,
            Err(CoreError::InvalidAction(message)) if message.contains("trade-7")
        ));
        assert_eq!(engine.manager.len(), 1);
        assert_eq!(
            engine.manager.id_by_trade_id("trade-7"),
            Some(first_id.clone())
        );
        assert!(engine.manager.ids_for_symbol("XAUUSD").is_empty());

        let second = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: Some(1.0800),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 2, 0),
            )
            .unwrap();
        let second_id = match &second[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected open, got {effect:?}"),
        };
        let duplicate_scale = engine.apply_action(
            Action::ScaleIn {
                position_id: second_id.clone(),
                price: Some(1.0750),
                size: 2.0,
                trade_id: Some("trade-7".into()),
            },
            ts(10, 3, 0),
        );
        assert!(matches!(
            duplicate_scale,
            Err(CoreError::InvalidAction(message)) if message.contains("trade-7")
        ));
        let second = engine.get_position(&second_id).unwrap();
        assert_eq!(second.data.entries.len(), 1);
        assert_eq!(second.data.total_filled_size(), 1.0);
        assert_eq!(second.data.trade_id, None);
        assert_eq!(engine.manager.id_by_trade_id("trade-7"), Some(first_id));
    }

    #[test]
    fn close_position_manually() {
        let mut engine = TradeEngine::new();
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        let effects = engine
            .apply_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                ts(10, 5, 0),
            )
            .unwrap();

        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Manual,
                ..
            }
        ));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn cancel_pending() {
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
                    trade_id: None,
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
            PositionStatus::Cancelled
        );
    }

    #[test]
    fn modify_stoploss() {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        let effects = engine
            .apply_action(
                Action::ModifyStoploss {
                    position_id: id.clone(),
                    price: 1.0820,
                },
                ts(10, 5, 0),
            )
            .unwrap();

        assert!(matches!(
            &effects[0],
            Effect::StoplossModified {
                old_price,
                new_price,
                ..
            } if (*old_price - 1.0800).abs() < f64::EPSILON && (*new_price - 1.0820).abs() < f64::EPSILON
        ));

        let pos = engine.get_position(&id).unwrap();
        assert!((pos.current_stoploss().unwrap() - 1.0820).abs() < f64::EPSILON);
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
                    trade_id: None,
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
                ts(10, 5, 0),
            )
            .unwrap();

        let pos = engine.get_position(&id).unwrap();
        assert!((pos.current_stoploss().unwrap() - 1.0850).abs() < f64::EPSILON);
        assert_eq!(
            pos.current_effective_stop().unwrap().origin,
            StopOrigin::Breakeven
        );
    }

    #[test]
    fn future_quote_breakeven_close_retains_reason_and_provenance() {
        let mut engine = TradeEngine::new();
        let id = open_future_position(
            &mut engine,
            Side::Buy,
            Some(95.0),
            vec![RuleConfig::BreakevenWhen {
                trigger_price: 105.0,
            }],
        );

        let effects = engine.on_price_future_effects(&quote("EURUSD", 105.0, 105.1, ts(10, 1, 0)));
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Plain {
                effect: Effect::StoplossModified { new_price, .. },
                requested_price: Some(requested_price),
                stop_origin: Some(StopOrigin::Breakeven),
            }] if (*new_price - 100.0).abs() < f64::EPSILON
                && (*requested_price - 100.0).abs() < f64::EPSILON
        ));
        assert_eq!(
            engine
                .get_position(&id)
                .unwrap()
                .current_effective_stop()
                .unwrap()
                .origin,
            StopOrigin::Breakeven
        );

        let effects = engine.on_price_future_effects(&quote("EURUSD", 99.9, 100.0, ts(10, 2, 0)));
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled {
                effect: Effect::PositionClosed {
                    reason: CloseReason::BreakevenStop,
                    ..
                },
                fill,
                stop_origin: Some(StopOrigin::Breakeven),
            }] if fill.execution.requested_price == Some(100.0)
        ));
    }

    #[test]
    fn manual_stops_on_the_target_side_are_rejected_atomically() {
        for (side, initial_stop, invalid_stop) in
            [(Side::Buy, 95.0, 102.0), (Side::Sell, 105.0, 98.0)]
        {
            let mut engine = TradeEngine::new();
            let id = open_future_position(&mut engine, side, Some(initial_stop), vec![]);
            let before = engine.get_position(&id).unwrap();
            let before_rules = format!("{:?}", before.rules);
            let before_records = before.data.records.len();
            let before_origin = before.data.stop_origin;

            assert!(matches!(
                engine.apply_action(
                    Action::ModifyStoploss {
                        position_id: id.clone(),
                        price: invalid_stop,
                    },
                    ts(10, 0, 30),
                ),
                Err(CoreError::InvalidAction(_))
            ));

            let position = engine.get_position(&id).unwrap();
            assert_eq!(format!("{:?}", position.rules), before_rules);
            assert_eq!(position.data.records.len(), before_records);
            assert_eq!(position.data.stop_origin, before_origin);
            assert_eq!(position.current_stoploss(), Some(initial_stop));
        }
    }

    #[test]
    fn future_quote_trailing_stop_wins_breakeven_and_retains_provenance() {
        let mut engine = TradeEngine::new();
        let id = open_future_position(
            &mut engine,
            Side::Buy,
            None,
            vec![
                RuleConfig::TrailingStop { distance: 2.0 },
                RuleConfig::BreakevenWhen {
                    trigger_price: 105.0,
                },
            ],
        );

        let effects = engine.on_price_future_effects(&quote("EURUSD", 105.0, 105.1, ts(10, 1, 0)));
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Plain {
                effect: Effect::StoplossModified { new_price, .. },
                requested_price: Some(requested_price),
                stop_origin: Some(StopOrigin::Trailing),
            }] if (*new_price - 103.0).abs() < f64::EPSILON
                && (*requested_price - 103.0).abs() < f64::EPSILON
        ));
        let stop = engine
            .get_position(&id)
            .unwrap()
            .current_effective_stop()
            .unwrap();
        assert!((stop.price - 103.0).abs() < f64::EPSILON);
        assert_eq!(stop.origin, StopOrigin::Trailing);

        let effects = engine.on_price_future_effects(&quote("EURUSD", 102.9, 103.0, ts(10, 2, 0)));
        assert!(matches!(
            effects.as_slice(),
            [FutureEffect::Filled {
                effect: Effect::PositionClosed {
                    reason: CloseReason::TrailingStop,
                    ..
                },
                fill,
                stop_origin: Some(StopOrigin::Trailing),
            }] if fill.execution.requested_price == Some(103.0)
        ));
        assert_eq!(
            engine
                .get_position(&id)
                .unwrap()
                .current_effective_stop()
                .unwrap()
                .origin,
            StopOrigin::Trailing
        );
    }

    #[test]
    fn bulk_close_all_of_symbol() {
        let mut engine = TradeEngine::new();

        // Open two EURUSD and one XAUUSD
        engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        engine
            .apply_action(
                Action::Open {
                    symbol: "XAUUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(2000.0),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        assert_eq!(engine.open_positions().len(), 3);

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
    fn bulk_cancel_all_pending() {
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
                    trade_id: None,
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
                    trade_id: None,
                },
                ts(9, 0, 0),
            )
            .unwrap();

        assert_eq!(engine.pending_positions().len(), 2);

        let effects = engine
            .apply_action(Action::CancelAllPending, ts(9, 30, 0))
            .unwrap();

        assert_eq!(effects.len(), 2);
        assert_eq!(engine.pending_positions().len(), 0);
    }

    #[test]
    fn breakeven_when_triggers_via_on_price() {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price hasn't reached breakeven trigger yet
        engine.on_price(&quote("EURUSD", 1.0880, 1.0882, ts(10, 1, 0)));
        let pos = engine.get_position(&id).unwrap();
        assert!((pos.current_stoploss().unwrap() - 1.0800).abs() < f64::EPSILON);

        // Price reaches breakeven trigger
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 2, 0)));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::StoplossModified { .. }))
        );

        let pos = engine.get_position(&id).unwrap();
        // SL should now be at entry (1.0850)
        assert!((pos.current_stoploss().unwrap() - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn trailing_stop_via_on_price() {
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
                    rules: vec![RuleConfig::TrailingStop { distance: 0.0020 }],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price goes up — no trigger
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 1, 0)));
        assert!(effects.is_empty());

        // Price drops but within trailing distance (peak=1.0900, sl=1.0880)
        let effects = engine.on_price(&quote("EURUSD", 1.0882, 1.0884, ts(10, 2, 0)));
        assert!(effects.is_empty());

        // Price drops below trailing stop
        let effects = engine.on_price(&quote("EURUSD", 1.0879, 1.0881, ts(10, 3, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::TrailingStop,
                ..
            }
        )));

        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn add_and_remove_target_via_action() {
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
                    rules: vec![],
                    group: None,
                    trade_id: None,
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
                Action::AddTarget {
                    position_id: id.clone(),
                    price: 1.0900,
                    close_ratio: 0.5,
                },
                ts(10, 1, 0),
            )
            .unwrap();

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.rules.len(), 1);
        assert_eq!(pos.rules[0].name(), "TakeProfit");

        engine
            .apply_action(
                Action::RemoveTarget {
                    position_id: id.clone(),
                    price: 1.0900,
                },
                ts(10, 2, 0),
            )
            .unwrap();

        let pos = engine.get_position(&id).unwrap();
        assert_eq!(pos.rules.len(), 0);
    }

    #[test]
    fn modify_target_preserves_colocated_alerts_with_register_parity() {
        let open = Action::Open {
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![TargetSpec {
                price: 1.0900,
                close_ratio: 0.5,
            }],
            rules: vec![RuleConfig::BreakevenWhen {
                trigger_price: 1.0900,
            }],
            group: None,
            trade_id: None,
        };
        let mut tick_engine = TradeEngine::new();
        let mut register_engine = TradeEngine::with_alert_register();

        let tick_id = match &tick_engine
            .apply_action(open.clone(), ts(10, 0, 0))
            .unwrap()[0]
        {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected position open, got {effect:?}"),
        };
        let register_id = match &register_engine.apply_action(open, ts(10, 0, 0)).unwrap()[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("expected position open, got {effect:?}"),
        };

        tick_engine
            .apply_action(
                Action::ModifyTarget {
                    position_id: tick_id.clone(),
                    old_price: 1.0900,
                    new_price: 1.0950,
                },
                ts(10, 1, 0),
            )
            .unwrap();
        register_engine
            .apply_action(
                Action::ModifyTarget {
                    position_id: register_id.clone(),
                    old_price: 1.0900,
                    new_price: 1.0950,
                },
                ts(10, 1, 0),
            )
            .unwrap();

        assert_eq!(
            register_engine
                .alert_register
                .as_ref()
                .unwrap()
                .alert_count(),
            3
        );

        let old_target_quote = quote("EURUSD", 1.0900, 1.0902, ts(10, 2, 0));
        let tick_effects = tick_engine.on_price(&old_target_quote);
        let register_effects = register_engine.on_price(&old_target_quote);
        assert!(matches!(
            tick_effects.as_slice(),
            [Effect::StoplossModified { new_price, .. }]
                if (*new_price - 1.0850).abs() < f64::EPSILON
        ));
        assert!(matches!(
            register_effects.as_slice(),
            [Effect::StoplossModified { new_price, .. }]
                if (*new_price - 1.0850).abs() < f64::EPSILON
        ));

        let new_target_quote = quote("EURUSD", 1.0950, 1.0952, ts(10, 3, 0));
        let tick_effects = tick_engine.on_price(&new_target_quote);
        let register_effects = register_engine.on_price(&new_target_quote);
        assert!(matches!(
            tick_effects.as_slice(),
            [Effect::PartialClose { ratio, .. }]
                if (*ratio - 0.5).abs() < f64::EPSILON
        ));
        assert!(matches!(
            register_effects.as_slice(),
            [Effect::PartialClose { ratio, .. }]
                if (*ratio - 0.5).abs() < f64::EPSILON
        ));

        let stop_quote = quote("EURUSD", 1.0850, 1.0852, ts(10, 4, 0));
        let tick_effects = tick_engine.on_price(&stop_quote);
        let register_effects = register_engine.on_price(&stop_quote);
        assert!(matches!(
            tick_effects.as_slice(),
            [Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }]
        ));
        assert!(matches!(
            register_effects.as_slice(),
            [Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }]
        ));
    }

    #[test]
    fn modify_target_preserves_ratio_rekeys_alert_and_rejects_invalid_state() {
        let mut engine = TradeEngine::with_alert_register();
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![
                        TargetSpec {
                            price: 1.0900,
                            close_ratio: 0.3,
                        },
                        TargetSpec {
                            price: 1.1000,
                            close_ratio: 0.7,
                        },
                    ],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            other => panic!("expected position open, got {other:?}"),
        };

        engine
            .apply_action(
                Action::ModifyTarget {
                    position_id: id.clone(),
                    old_price: 1.0900,
                    new_price: 1.0950,
                },
                ts(10, 1, 0),
            )
            .unwrap();

        let position = engine.get_position(&id).unwrap();
        assert!(matches!(
            &position.rules[0],
            Rule::TakeProfit {
                price,
                close_ratio,
                triggered: false,
            } if (*price - 1.0950).abs() < f64::EPSILON
                && (*close_ratio - 0.3).abs() < f64::EPSILON
        ));
        assert!(matches!(
            position.data.records.last(),
            Some((
                PositionRecord::TargetModified {
                    from,
                    to,
                    close_ratio,
                },
                _
            )) if (*from - 1.0900).abs() < f64::EPSILON
                && (*to - 1.0950).abs() < f64::EPSILON
                && (*close_ratio - 0.3).abs() < f64::EPSILON
        ));

        assert!(
            engine
                .on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 2, 0)))
                .is_empty()
        );
        let effects = engine.on_price(&quote("EURUSD", 1.0950, 1.0952, ts(10, 3, 0)));
        assert!(matches!(
            effects.as_slice(),
            [Effect::PartialClose { ratio, .. }] if (*ratio - 0.3).abs() < f64::EPSILON
        ));

        assert!(matches!(
            engine.apply_action(
                Action::ModifyTarget {
                    position_id: id.clone(),
                    old_price: 1.0950,
                    new_price: 1.0960,
                },
                ts(10, 4, 0),
            ),
            Err(CoreError::TargetAlreadyTriggered { .. })
        ));
        assert!(matches!(
            engine.apply_action(
                Action::ModifyTarget {
                    position_id: id,
                    old_price: 1.0910,
                    new_price: 1.0960,
                },
                ts(10, 5, 0),
            ),
            Err(CoreError::TargetNotFound { .. })
        ));
    }

    #[test]
    fn error_on_missing_position() {
        let mut engine = TradeEngine::new();
        let result = engine.apply_action(
            Action::ClosePosition {
                position_id: "nonexistent".into(),
            },
            ts(10, 0, 0),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::PositionNotFound(_)
        ));
    }

    #[test]
    fn error_on_market_order_no_price_no_quote() {
        let mut engine = TradeEngine::new();
        let result = engine.apply_action(
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
                trade_id: None,
            },
            ts(10, 0, 0),
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::NoPriceAvailable(_)
        ));
    }

    // ── FillModel tests ─────────────────────────────────────────────────

    #[test]
    fn fill_model_ask_only_sl_triggers_on_ask() {
        // In AskOnly mode, a Buy's SL should check against ask (not bid).
        // SL at 1.0800: with bid=1.0790 ask=1.0810, BidAsk would trigger
        // (bid <= 1.0800) but AskOnly should NOT trigger (ask 1.0810 > 1.0800).
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // bid below SL but ask above SL → no trigger in AskOnly
        let effects = engine.on_price(&quote("EURUSD", 1.0790, 1.0810, ts(10, 1, 0)));
        assert!(effects.is_empty());
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );

        // ask drops to SL → triggers
        let effects = engine.on_price(&quote("EURUSD", 1.0790, 1.0800, ts(10, 2, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
    }

    #[test]
    fn fill_model_ask_only_tp_triggers_on_ask() {
        // In AskOnly mode, a Buy's TP should also check against ask.
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        let _effects = engine
            .apply_action(
                Action::Open {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        // bid at TP but ask below TP → no trigger in AskOnly
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0895, ts(10, 1, 0)));
        assert!(effects.is_empty());

        // ask at TP → triggers
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0900, ts(10, 2, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Target,
                ..
            }
        )));
    }

    #[test]
    fn fill_model_ask_only_sell_sl_triggers_on_ask() {
        // In AskOnly mode, a Sell's SL also checks against ask.
        // SL at 1.0900: triggers when ask >= 1.0900
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        engine
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        // ask below SL → no trigger
        let effects = engine.on_price(&quote("EURUSD", 1.0880, 1.0890, ts(10, 1, 0)));
        assert!(effects.is_empty());

        // ask at SL → triggers
        let effects = engine.on_price(&quote("EURUSD", 1.0895, 1.0900, ts(10, 2, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
    }

    #[test]
    fn fill_model_mid_price_uses_midpoint() {
        // MidPrice mode: checks (bid+ask)/2 for everything.
        // SL at 1.0800. bid=1.0790 ask=1.0820 → mid=1.0805 > 1.0800 → no trigger.
        let mut engine = TradeEngine::with_fill_model(FillModel::MidPrice);
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // mid = (1.0790 + 1.0820) / 2 = 1.0805 → above SL, no trigger
        let effects = engine.on_price(&quote("EURUSD", 1.0790, 1.0820, ts(10, 1, 0)));
        assert!(effects.is_empty());
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );

        // mid = (1.0790 + 1.0810) / 2 = 1.0800 → at SL, triggers
        let effects = engine.on_price(&quote("EURUSD", 1.0790, 1.0810, ts(10, 2, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
    }

    #[test]
    fn fill_model_ask_only_limit_sell_fills_on_ask() {
        // In AskOnly mode, Limit Sell fill check uses ask (not bid).
        // Limit Sell at 1.0900: fills when ask >= 1.0900.
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Limit,
                    price: Some(1.0900),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(9, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::OrderPlaced { id } => id.clone(),
            _ => panic!(),
        };

        // bid above limit but ask below → no fill in AskOnly
        let effects = engine.on_price(&quote("EURUSD", 1.0905, 1.0895, ts(10, 0, 0)));
        assert!(effects.is_empty());
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Pending
        );

        // ask at limit → fills
        let effects = engine.on_price(&quote("EURUSD", 1.0898, 1.0900, ts(10, 1, 0)));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::PositionOpened { .. }))
        );
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );
    }

    #[test]
    fn fill_model_mid_price_limit_buy_fills_on_mid() {
        // MidPrice mode: Limit Buy at 1.0800 fills when mid <= 1.0800.
        let mut engine = TradeEngine::with_fill_model(FillModel::MidPrice);
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
                    trade_id: None,
                },
                ts(9, 0, 0),
            )
            .unwrap();

        // mid = (1.0798 + 1.0810) / 2 = 1.0804 → above limit, no fill
        let effects = engine.on_price(&quote("EURUSD", 1.0798, 1.0810, ts(10, 0, 0)));
        assert!(effects.is_empty());

        // mid = (1.0790 + 1.0810) / 2 = 1.0800 → at limit, fills
        let effects = engine.on_price(&quote("EURUSD", 1.0790, 1.0810, ts(10, 1, 0)));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::PositionOpened { .. }))
        );
    }

    #[test]
    fn fill_model_ask_only_trailing_stop_tracks_ask() {
        // In AskOnly mode, trailing stop for Buy tracks ask (peak) and
        // triggers when ask drops below peak - distance.
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![RuleConfig::TrailingStop { distance: 0.0020 }],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        // Ask rises to 1.0910 → peak updates to 1.0910
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0910, ts(10, 1, 0)));
        assert!(effects.is_empty());

        // Ask drops to 1.0895 → trailing SL = 1.0910 - 0.0020 = 1.0890
        // 1.0895 > 1.0890 → no trigger
        let effects = engine.on_price(&quote("EURUSD", 1.0890, 1.0895, ts(10, 2, 0)));
        assert!(effects.is_empty());

        // Ask drops to 1.0889 → 1.0889 < 1.0890 → triggers
        let effects = engine.on_price(&quote("EURUSD", 1.0880, 1.0889, ts(10, 3, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::TrailingStop,
                ..
            }
        )));
    }

    #[test]
    fn fill_model_default_is_bidask() {
        let engine = TradeEngine::new();
        assert_eq!(engine.fill_model, FillModel::BidAsk);
    }

    #[test]
    fn fill_model_with_fill_model_constructor() {
        let engine = TradeEngine::with_fill_model(FillModel::MidPrice);
        assert_eq!(engine.fill_model, FillModel::MidPrice);
    }

    #[test]
    fn askonly_sell_market_order_fills_at_bid() {
        // In AskOnly mode, a Sell market order with price: None should
        // record the fill at bid (realistic), not ask (model price).
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        engine.on_price(&quote("EURUSD", 1.0848, 1.0850, ts(9, 59, 0)));

        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: None,
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        let pos = engine.get_position(&id).unwrap();
        // Sell opens at bid (realistic), not ask (model)
        assert!(
            (pos.data.average_entry() - 1.0848).abs() < 1e-10,
            "Sell market order should fill at bid=1.0848, got {}",
            pos.data.average_entry()
        );
    }

    #[test]
    fn askonly_partial_close_records_realistic_price() {
        // In AskOnly mode, when a TP triggers a partial close on a Buy
        // position, the recorded close price should be bid (realistic),
        // not ask (model).
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![TargetSpec {
                        price: 1.0900,
                        close_ratio: 0.5,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        // ask hits TP → triggers partial close via apply_effect
        let _effects = engine.on_price(&quote("EURUSD", 1.0905, 1.0910, ts(10, 5, 0)));

        let pos = engine.get_position(&id).unwrap();
        // Find the PartialClose record
        let partial_record = pos
            .data
            .records
            .iter()
            .find_map(|(rec, _ts)| match rec {
                PositionRecord::PartialClose { price, .. } => Some(*price),
                _ => None,
            })
            .expect("should have a PartialClose record");

        // The recorded close price should be bid (realistic), not ask
        assert!(
            (partial_record - 1.0905).abs() < 1e-10,
            "PartialClose should record bid=1.0905, got {}",
            partial_record
        );
    }

    #[test]
    fn askonly_manual_partial_close_records_bid() {
        // Manually partial-closing a Buy position in AskOnly mode should
        // record the close price as bid (realistic), not ask.
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 2.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        // Seed a quote, then manually partial close
        engine.on_price(&quote("EURUSD", 1.0870, 1.0880, ts(10, 5, 0)));
        let _effects = engine
            .apply_action(
                Action::ClosePartial {
                    position_id: id.clone(),
                    ratio: 0.5,
                },
                ts(10, 5, 0),
            )
            .unwrap();

        let pos = engine.get_position(&id).unwrap();
        let partial_record = pos
            .data
            .records
            .iter()
            .find_map(|(rec, _ts)| match rec {
                PositionRecord::PartialClose { price, .. } => Some(*price),
                _ => None,
            })
            .expect("should have a PartialClose record");

        // Buy closes at bid (realistic)
        assert!(
            (partial_record - 1.0870).abs() < 1e-10,
            "Manual partial close should record bid=1.0870, got {}",
            partial_record
        );
    }

    #[test]
    fn midprice_sell_fills_at_bid() {
        // In MidPrice mode, a Sell market order with price: None should
        // still record the fill at bid (realistic), not mid (model).
        let mut engine = TradeEngine::with_fill_model(FillModel::MidPrice);
        engine.on_price(&quote("EURUSD", 1.0848, 1.0852, ts(9, 59, 0)));

        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: None,
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        let pos = engine.get_position(&id).unwrap();
        // Sell opens at bid=1.0848 (realistic), not mid=1.0850
        assert!(
            (pos.data.average_entry() - 1.0848).abs() < 1e-10,
            "Sell market order in MidPrice should fill at bid=1.0848, got {}",
            pos.data.average_entry()
        );
    }

    #[test]
    fn askonly_scale_in_sell_records_bid() {
        // Scale-in to a Sell position with price: None in AskOnly mode
        // should record the new fill at bid (realistic), not ask.
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: Some(1.0850),
                    size: 1.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        // Seed a quote and scale in with price: None
        engine.on_price(&quote("EURUSD", 1.0830, 1.0840, ts(10, 5, 0)));
        let _effects = engine
            .apply_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: None,
                    size: 1.0,
                    trade_id: None,
                },
                ts(10, 5, 0),
            )
            .unwrap();

        let pos = engine.get_position(&id).unwrap();
        // Second fill should be at bid=1.0830, not ask=1.0840
        let second_fill = &pos.data.entries[1];
        assert!(
            (second_fill.price - 1.0830).abs() < 1e-10,
            "Scale-in Sell should fill at bid=1.0830, got {}",
            second_fill.price
        );
        // average_entry = (1.0850 + 1.0830) / 2 = 1.0840
        assert!(
            (pos.data.average_entry() - 1.0840).abs() < 1e-10,
            "average_entry should be 1.0840, got {}",
            pos.data.average_entry()
        );
    }

    #[test]
    fn askonly_breakeven_uses_correct_average_entry() {
        // Open a Sell with price: None in AskOnly mode, add BreakevenWhen.
        // Verify that when breakeven triggers, the SL moves to the correct
        // average_entry (computed from realistic bid fill, not ask).
        let mut engine = TradeEngine::with_fill_model(FillModel::AskOnly);
        engine.on_price(&quote("EURUSD", 1.0848, 1.0850, ts(9, 59, 0)));

        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    price: None,
                    size: 1.0,
                    stoploss: Some(1.0900),
                    targets: vec![],
                    rules: vec![RuleConfig::BreakevenWhen {
                        trigger_price: 1.0800,
                    }],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();

        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!("expected PositionOpened"),
        };

        // Entry should be at bid=1.0848 (realistic)
        let entry = engine.get_position(&id).unwrap().data.average_entry();
        assert!(
            (entry - 1.0848).abs() < 1e-10,
            "Entry should be bid=1.0848, got {}",
            entry
        );

        // Breakeven trigger: for Sell, triggers when ask <= trigger_price
        // In AskOnly, eval_price uses ask for everything.
        // ask=1.0800 triggers BreakevenWhen(1.0800)
        let effects = engine.on_price(&quote("EURUSD", 1.0795, 1.0800, ts(10, 5, 0)));

        // Check that breakeven moved SL to average_entry = 1.0848
        assert!(
            effects.iter().any(|e| matches!(
                e,
                Effect::StoplossModified { new_price, .. } if (*new_price - 1.0848).abs() < 1e-10
            )),
            "Breakeven should move SL to average_entry=1.0848, effects: {:?}",
            effects
        );

        let sl = engine.get_position(&id).unwrap().current_stoploss();
        assert!(
            (sl.unwrap() - 1.0848).abs() < 1e-10,
            "SL should be at average_entry=1.0848, got {:?}",
            sl
        );
    }

    // ── Alert register integration tests ────────────────────────────────

    #[test]
    fn engine_with_register_open_registers_alerts() {
        let mut engine = TradeEngine::with_alert_register();
        let effects = engine
            .apply_action(
                Action::Open {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        assert_eq!(effects.len(), 1);
        assert!(matches!(effects[0], Effect::PositionOpened { .. }));

        // SL triggers via register when price drops.
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        let effects = engine.on_price(&quote("EURUSD", 1.0800, 1.0802, ts(10, 1, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn engine_with_register_sl_triggers_via_register() {
        let mut engine = TradeEngine::with_alert_register();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price above SL — no trigger.
        let effects = engine.on_price(&quote("EURUSD", 1.0840, 1.0842, ts(10, 0, 1)));
        assert!(effects.is_empty());

        // Price at SL — triggers.
        let effects = engine.on_price(&quote("EURUSD", 1.0800, 1.0802, ts(10, 0, 2)));
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        ));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn engine_with_register_tp_triggers_via_register() {
        let mut engine = TradeEngine::with_alert_register();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // TP1 hit — partial close.
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 1, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PartialClose {
                reason: CloseReason::Target,
                ..
            }
        )));
        let pos = engine.get_position(&id).unwrap();
        assert!((pos.data.remaining_ratio - 0.5).abs() < f64::EPSILON);

        // TP2 hit — full close via stoploss (remaining ratio exhausted).
        let effects = engine.on_price(&quote("EURUSD", 1.0950, 1.0952, ts(10, 2, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Target,
                ..
            }
        )));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn engine_with_register_trailing_stop_works() {
        let mut engine = TradeEngine::with_alert_register();
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
                    rules: vec![RuleConfig::TrailingStop { distance: 0.0020 }],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price rises — peak updates but no trigger.
        let effects = engine.on_price(&quote("EURUSD", 1.0870, 1.0872, ts(10, 0, 1)));
        assert!(effects.is_empty());
        let effects = engine.on_price(&quote("EURUSD", 1.0890, 1.0892, ts(10, 0, 2)));
        assert!(effects.is_empty());

        // Price drops within distance — no trigger (peak=1.0890, trail=1.0870).
        let effects = engine.on_price(&quote("EURUSD", 1.0875, 1.0877, ts(10, 0, 3)));
        assert!(effects.is_empty());

        // Price drops to trail level — triggers (peak=1.0890, trail=1.0870).
        let effects = engine.on_price(&quote("EURUSD", 1.0870, 1.0872, ts(10, 0, 4)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::TrailingStop,
                ..
            }
        )));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn engine_with_register_modify_sl_reregisters() {
        let mut engine = TradeEngine::with_alert_register();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Modify SL to 1.0820.
        engine
            .apply_action(
                Action::ModifyStoploss {
                    position_id: id.clone(),
                    price: 1.0820,
                },
                ts(10, 0, 1),
            )
            .unwrap();

        // Price above new SL — no trigger.
        let effects = engine.on_price(&quote("EURUSD", 1.0830, 1.0832, ts(10, 0, 2)));
        assert!(effects.is_empty());

        // New SL at 1.0820 — triggers.
        let effects = engine.on_price(&quote("EURUSD", 1.0820, 1.0822, ts(10, 0, 3)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
    }

    #[test]
    fn engine_with_register_close_deregisters() {
        let mut engine = TradeEngine::with_alert_register();
        let effects = engine
            .apply_action(
                Action::Open {
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Close manually.
        engine
            .apply_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                ts(10, 0, 1),
            )
            .unwrap();

        // SL and TP prices — nothing triggers (deregistered on close).
        let effects = engine.on_price(&quote("EURUSD", 1.0750, 1.0752, ts(10, 0, 2)));
        assert!(effects.is_empty());
        let effects = engine.on_price(&quote("EURUSD", 1.0950, 1.0952, ts(10, 0, 3)));
        assert!(effects.is_empty());
    }

    #[test]
    fn engine_with_register_pending_fill() {
        let mut engine = TradeEngine::with_alert_register();
        // Place a Limit Buy at 1.0800.
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
                        price: 1.0900,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        assert!(matches!(effects[0], Effect::OrderPlaced { .. }));
        let id = match &effects[0] {
            Effect::OrderPlaced { id } => id.clone(),
            _ => panic!(),
        };

        // Price above limit — no fill.
        let effects = engine.on_price(&quote("EURUSD", 1.0848, 1.0850, ts(10, 0, 1)));
        assert!(effects.is_empty());

        // Price drops to limit — fills.
        let effects = engine.on_price(&quote("EURUSD", 1.0798, 1.0800, ts(10, 0, 2)));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::PositionOpened { .. }))
        );
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Open
        );

        // Now SL/TP should be registered — SL triggers.
        let effects = engine.on_price(&quote("EURUSD", 1.0750, 1.0752, ts(10, 0, 3)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
    }

    #[test]
    fn engine_with_register_close_all_deregisters() {
        let mut engine = TradeEngine::with_alert_register();
        // Open 3 positions with SL.
        for i in 0..3 {
            engine
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
                        trade_id: None,
                    },
                    ts(10, 0, i),
                )
                .unwrap();
        }

        // Close all.
        engine.apply_action(Action::CloseAll, ts(10, 1, 0)).unwrap();

        // SL price — nothing triggers (all deregistered).
        let effects = engine.on_price(&quote("EURUSD", 1.0750, 1.0752, ts(10, 2, 0)));
        assert!(effects.is_empty());
    }

    #[test]
    fn engine_with_register_breakeven_reregisters_sl() {
        let mut engine = TradeEngine::with_alert_register();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price hits breakeven trigger — SL should move to entry (1.0850).
        let effects = engine.on_price(&quote("EURUSD", 1.0900, 1.0902, ts(10, 1, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::StoplossModified { new_price, .. } if (*new_price - 1.0850).abs() < 1e-10
        )));

        // Price above new SL (entry=1.0850) — no trigger. Old SL at 1.0800 is deregistered.
        let effects = engine.on_price(&quote("EURUSD", 1.0860, 1.0862, ts(10, 2, 0)));
        assert!(effects.is_empty());

        // New SL at entry (1.0850) — triggers.
        let effects = engine.on_price(&quote("EURUSD", 1.0850, 1.0852, ts(10, 3, 0)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn engine_with_register_matches_tickbytick_results() {
        // Run the same sequence through both engine modes and verify identical results.
        let actions_and_prices: Vec<(Option<Action>, Option<PriceQuote>)> = vec![
            // Open a buy with SL + 2 TPs.
            (
                Some(Action::Open {
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
                    trade_id: None,
                }),
                None,
            ),
            // Price moves up — no trigger.
            (None, Some(quote("EURUSD", 1.0860, 1.0862, ts(10, 0, 1)))),
            (None, Some(quote("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)))),
            // TP1 hit.
            (None, Some(quote("EURUSD", 1.0900, 1.0902, ts(10, 0, 3)))),
            // Continue up.
            (None, Some(quote("EURUSD", 1.0920, 1.0922, ts(10, 0, 4)))),
            // TP2 hit — full close.
            (None, Some(quote("EURUSD", 1.0950, 1.0952, ts(10, 0, 5)))),
        ];

        let mut engine_tick = TradeEngine::new();
        let mut engine_reg = TradeEngine::with_alert_register();

        let mut effects_tick_all = Vec::new();
        let mut effects_reg_all = Vec::new();

        for (action, price) in &actions_and_prices {
            if let Some(a) = action {
                let e1 = engine_tick.apply_action(a.clone(), ts(10, 0, 0)).unwrap();
                let e2 = engine_reg.apply_action(a.clone(), ts(10, 0, 0)).unwrap();
                effects_tick_all.extend(e1);
                effects_reg_all.extend(e2);
            }
            if let Some(q) = price {
                let e1 = engine_tick.on_price(q);
                let e2 = engine_reg.on_price(q);
                effects_tick_all.extend(e1);
                effects_reg_all.extend(e2);
            }
        }

        // Both engines should produce the same number of effects.
        assert_eq!(
            effects_tick_all.len(),
            effects_reg_all.len(),
            "Effect count mismatch: tick={}, reg={}\ntick: {:?}\nreg: {:?}",
            effects_tick_all.len(),
            effects_reg_all.len(),
            effects_tick_all,
            effects_reg_all,
        );

        // Both engines' positions should have the same final status.
        let tick_positions: Vec<_> = engine_tick.closed_positions();
        let reg_positions: Vec<_> = engine_reg.closed_positions();
        assert_eq!(tick_positions.len(), reg_positions.len());
    }

    #[test]
    fn invalid_open_prices_rules_duplicates_and_tiny_sizes_are_atomic() {
        let invalid_actions = vec![
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(f64::NAN),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: position_size_tolerance(1.0),
                stoploss: None,
                targets: vec![],
                rules: vec![],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: 1.0,
                stoploss: Some(100.0),
                targets: vec![],
                rules: vec![],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: 1.0,
                stoploss: None,
                targets: vec![TargetSpec {
                    price: 99.0,
                    close_ratio: 1.0,
                }],
                rules: vec![],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: 1.0,
                stoploss: None,
                targets: vec![TargetSpec {
                    price: 101.0,
                    close_ratio: 0.5,
                }],
                rules: vec![RuleConfig::TakeProfit {
                    price: 101.0,
                    close_ratio: 0.5,
                }],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![RuleConfig::TrailingStop { distance: 100.0 }],
                group: None,
                trade_id: None,
            },
            Action::Open {
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(100.0),
                size: 1.0,
                stoploss: None,
                targets: vec![],
                rules: vec![RuleConfig::TimeExit { max_seconds: 0 }],
                group: None,
                trade_id: None,
            },
        ];

        let mut engine = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        for action in invalid_actions {
            assert!(matches!(
                engine.apply_action(action, ts(10, 0, 0)),
                Err(CoreError::InvalidAction(_))
            ));
            assert!(engine.manager.is_empty());
        }

        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(100.0),
                    size: 1.0,
                    stoploss: Some(99.0),
                    targets: vec![TargetSpec {
                        price: 101.0,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 1, 0),
            )
            .unwrap();
        assert!(matches!(
            effects.as_slice(),
            [Effect::PositionOpened { id }] if id == "position:00000000"
        ));
    }

    #[test]
    fn legacy_and_future_scale_in_validation_is_atomic() {
        let mut legacy = TradeEngine::with_fill_model_and_deterministic_ids(FillModel::BidAsk);
        let id = open_future_position(&mut legacy, Side::Buy, Some(95.0), vec![]);
        let before = legacy.get_position(&id).unwrap();
        let before_entries = before.data.entries.len();
        let before_records = before.data.records.len();
        let before_trade_id = before.data.trade_id.clone();

        for (price, size) in [
            (Some(f64::NAN), 1.0),
            (Some(101.0), f64::NAN),
            (Some(101.0), 0.0),
            (Some(101.0), position_size_tolerance(1.0)),
        ] {
            assert!(matches!(
                legacy.apply_action(
                    Action::ScaleIn {
                        position_id: id.clone(),
                        price,
                        size,
                        trade_id: Some("scale-trade".into()),
                    },
                    ts(10, 1, 0),
                ),
                Err(CoreError::InvalidAction(_))
            ));
            let position = legacy.get_position(&id).unwrap();
            assert_eq!(position.data.entries.len(), before_entries);
            assert_eq!(position.data.records.len(), before_records);
            assert_eq!(position.data.trade_id, before_trade_id);
        }

        let mut future = legacy.clone();
        let result = future.apply_priced_future_action(
            Action::ScaleIn {
                position_id: id.clone(),
                price: Some(f64::INFINITY),
                size: 1.0,
                trade_id: Some("future-scale".into()),
            },
            &quote("EURUSD", 100.0, 100.1, ts(10, 2, 0)),
            execution(FillPurpose::MarketEntry, Side::Buy, 100.1),
        );
        assert!(matches!(
            result,
            Err(FutureApplyError::Core(CoreError::InvalidAction(_)))
        ));
        let position = future.get_position(&id).unwrap();
        assert_eq!(position.data.entries.len(), before_entries);
        assert_eq!(position.data.records.len(), before_records);
        assert_eq!(position.data.trade_id, before_trade_id);
    }

    #[test]
    fn target_and_rule_mutations_reject_duplicates_and_terminal_positions_atomically() {
        let mut engine = TradeEngine::with_alert_register();
        let effects = engine
            .apply_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(100.0),
                    size: 1.0,
                    stoploss: Some(95.0),
                    targets: vec![
                        TargetSpec {
                            price: 105.0,
                            close_ratio: 0.5,
                        },
                        TargetSpec {
                            price: 110.0,
                            close_ratio: 0.5,
                        },
                    ],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };
        let before_rules = format!("{:?}", engine.get_position(&id).unwrap().rules);
        let before_records = engine.get_position(&id).unwrap().data.records.len();

        for action in [
            Action::AddTarget {
                position_id: id.clone(),
                price: 105.0,
                close_ratio: 0.25,
            },
            Action::AddRule {
                position_id: id.clone(),
                rule: RuleConfig::TakeProfit {
                    price: 110.0,
                    close_ratio: 0.25,
                },
            },
            Action::ModifyTarget {
                position_id: id.clone(),
                old_price: 105.0,
                new_price: 110.0,
            },
        ] {
            assert!(matches!(
                engine.apply_action(action, ts(10, 1, 0)),
                Err(CoreError::InvalidAction(_))
            ));
            let position = engine.get_position(&id).unwrap();
            assert_eq!(format!("{:?}", position.rules), before_rules);
            assert_eq!(position.data.records.len(), before_records);
        }

        engine
            .apply_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                ts(10, 2, 0),
            )
            .unwrap();
        let closed_rules = format!("{:?}", engine.get_position(&id).unwrap().rules);
        let closed_records = engine.get_position(&id).unwrap().data.records.len();
        let closed_origin = engine.get_position(&id).unwrap().data.stop_origin;
        for action in [
            Action::ModifyStoploss {
                position_id: id.clone(),
                price: 94.0,
            },
            Action::MoveStoplossToEntry {
                position_id: id.clone(),
            },
            Action::AddTarget {
                position_id: id.clone(),
                price: 120.0,
                close_ratio: 1.0,
            },
            Action::RemoveTarget {
                position_id: id.clone(),
                price: 105.0,
            },
            Action::AddRule {
                position_id: id.clone(),
                rule: RuleConfig::TrailingStop { distance: 1.0 },
            },
            Action::RemoveRule {
                position_id: id.clone(),
                rule_name: "FixedStoploss".into(),
            },
        ] {
            assert!(matches!(
                engine.apply_action(action, ts(10, 3, 0)),
                Err(CoreError::InvalidState { .. })
            ));
            let position = engine.get_position(&id).unwrap();
            assert_eq!(format!("{:?}", position.rules), closed_rules);
            assert_eq!(position.data.records.len(), closed_records);
            assert_eq!(position.data.stop_origin, closed_origin);
        }
    }

    #[test]
    fn move_and_bulk_stop_changes_match_tick_and_indexed_evaluation() {
        let open = Action::Open {
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(100.0),
            size: 1.0,
            stoploss: Some(95.0),
            targets: vec![],
            rules: vec![],
            group: Some("g".into()),
            trade_id: None,
        };

        for mutation in [
            Action::MoveStoplossToEntry {
                position_id: String::new(),
            },
            Action::ModifyAllStoploss {
                symbol: "EURUSD".into(),
                price: 98.0,
            },
            Action::ModifyAllStoplossInGroup {
                group_id: "g".into(),
                price: 97.0,
            },
        ] {
            let mut tick_engine = TradeEngine::new();
            let mut indexed_engine = TradeEngine::with_alert_register();
            let tick_id = match tick_engine
                .apply_action(open.clone(), ts(10, 0, 0))
                .unwrap()
                .remove(0)
            {
                Effect::PositionOpened { id } => id,
                effect => panic!("unexpected effect: {effect:?}"),
            };
            let indexed_id = match indexed_engine
                .apply_action(open.clone(), ts(10, 0, 0))
                .unwrap()
                .remove(0)
            {
                Effect::PositionOpened { id } => id,
                effect => panic!("unexpected effect: {effect:?}"),
            };

            let tick_mutation = match &mutation {
                Action::MoveStoplossToEntry { .. } => Action::MoveStoplossToEntry {
                    position_id: tick_id.clone(),
                },
                action => action.clone(),
            };
            let indexed_mutation = match &mutation {
                Action::MoveStoplossToEntry { .. } => Action::MoveStoplossToEntry {
                    position_id: indexed_id.clone(),
                },
                action => action.clone(),
            };
            tick_engine
                .apply_action(tick_mutation, ts(10, 1, 0))
                .unwrap();
            indexed_engine
                .apply_action(indexed_mutation, ts(10, 1, 0))
                .unwrap();

            let trigger = match mutation {
                Action::MoveStoplossToEntry { .. } => 99.5,
                Action::ModifyAllStoploss { .. } => 97.5,
                Action::ModifyAllStoplossInGroup { .. } => 96.5,
                _ => unreachable!(),
            };
            let quote = quote("EURUSD", trigger, trigger, ts(10, 2, 0));
            let tick_effects = tick_engine.on_price(&quote);
            let indexed_effects = indexed_engine.on_price(&quote);
            assert_eq!(tick_effects.len(), indexed_effects.len());
            assert_eq!(
                tick_engine.get_position(&tick_id).unwrap().data.status,
                PositionStatus::Closed
            );
            assert_eq!(
                indexed_engine
                    .get_position(&indexed_id)
                    .unwrap()
                    .data
                    .status,
                PositionStatus::Closed
            );
        }
    }

    #[test]
    fn mixed_side_bulk_stop_rejection_is_atomic() {
        let mut engine = TradeEngine::with_alert_register();
        let buy_id = open_future_position(&mut engine, Side::Buy, Some(95.0), vec![]);
        let sell_id = open_future_position(&mut engine, Side::Sell, Some(105.0), vec![]);
        let buy_records = engine.get_position(&buy_id).unwrap().data.records.len();
        let sell_records = engine.get_position(&sell_id).unwrap().data.records.len();

        assert!(matches!(
            engine.apply_action(
                Action::ModifyAllStoploss {
                    symbol: "EURUSD".into(),
                    price: 99.0,
                },
                ts(10, 1, 0),
            ),
            Err(CoreError::InvalidAction(_))
        ));
        assert_eq!(
            engine.get_position(&buy_id).unwrap().current_stoploss(),
            Some(95.0)
        );
        assert_eq!(
            engine.get_position(&sell_id).unwrap().current_stoploss(),
            Some(105.0)
        );
        assert_eq!(
            engine.get_position(&buy_id).unwrap().data.records.len(),
            buy_records
        );
        assert_eq!(
            engine.get_position(&sell_id).unwrap().data.records.len(),
            sell_records
        );
    }

    #[test]
    fn engine_with_register_sell_sl_triggers() {
        let mut engine = TradeEngine::with_alert_register();
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
                    trade_id: None,
                },
                ts(10, 0, 0),
            )
            .unwrap();
        let id = match &effects[0] {
            Effect::PositionOpened { id } => id.clone(),
            _ => panic!(),
        };

        // Price rises to SL — triggers (sell SL checks ask).
        let effects = engine.on_price(&quote("EURUSD", 1.0898, 1.0900, ts(10, 0, 1)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        )));
        assert_eq!(
            engine.get_position(&id).unwrap().data.status,
            PositionStatus::Closed
        );
    }
}
