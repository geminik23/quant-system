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

use crate::alert_register::{AlertKind, PriceAlertRegister, TriggeredAlert};
use crate::error::{CoreError, Result};
use crate::position::Position;
use crate::position_manager::PositionManager;
use crate::rules::Rule;
use crate::types::{
    Action, CloseReason, Effect, Fill, FillModel, OrderType, PositionId, PositionRecord,
    PositionStatus, PriceQuote, Side, TargetSpec,
};

/// Generate a short random id for new positions.
fn gen_id() -> PositionId {
    nanoid!(12)
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
        }
    }

    /// Create a new engine with a specific fill model.
    pub fn with_fill_model(fill_model: FillModel) -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model,
            alert_register: None,
        }
    }

    /// Engine with alert register for indexed evaluation. Best for real-time with many positions.
    pub fn with_alert_register() -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model: FillModel::default(),
            alert_register: Some(PriceAlertRegister::new()),
        }
    }

    /// Engine with both alert register and custom fill model.
    pub fn with_alert_register_and_fill_model(fill_model: FillModel) -> Self {
        Self {
            manager: PositionManager::new(),
            last_quotes: HashMap::new(),
            fill_model,
            alert_register: Some(PriceAlertRegister::new()),
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
                if let Some(pos) = self.manager.get_mut(&id) {
                    if pos.try_fill(quote, fill_model) {
                        all_effects.push(Effect::PositionOpened { id: id.clone() });
                    }
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

    // ── Action processing ───────────────────────────────────────────────

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
            } => self.action_open(
                symbol, side, order_type, price, size, stoploss, targets, rules, group, ts,
            ),

            // ── Scale in ────────────────────────────────────────────
            Action::ScaleIn {
                position_id,
                price,
                size,
            } => self.action_scale_in(&position_id, price, size, ts),

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

    // ── Private: action handlers ────────────────────────────────────────

    fn action_open(
        &mut self,
        symbol: String,
        side: Side,
        order_type: OrderType,
        price: Option<f64>,
        size: f64,
        stoploss: Option<f64>,
        targets: Vec<TargetSpec>,
        rules: Vec<crate::types::RuleConfig>,
        group: Option<String>,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let id = gen_id();

        // Build rules from stoploss + targets + explicit rules.
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
                // Determine fill price.
                let fill_price = match price {
                    Some(p) => p,
                    None => {
                        let q = self
                            .last_quotes
                            .get(&symbol)
                            .ok_or_else(|| CoreError::NoPriceAvailable(symbol.clone()))?;
                        q.open_price(side)
                    }
                };

                let fill = Fill {
                    price: fill_price,
                    size,
                    ts,
                };
                let mut pos =
                    Position::new_market(id.clone(), symbol.clone(), side, fill, live_rules);
                // Assign group if specified.
                if let Some(ref gid) = group {
                    pos.data.group = Some(gid.clone());
                    pos.data.records.push((
                        PositionRecord::GroupAssigned {
                            group_id: gid.clone(),
                        },
                        ts,
                    ));
                    self.manager.add(pos);
                    self.manager.add_to_group(gid, id.clone());
                } else {
                    self.manager.add(pos);
                }
                // Register alerts if alert register is active.
                self.register_alerts_for_position(&id, &symbol, side);
                Ok(vec![Effect::PositionOpened { id }])
            }
            OrderType::Limit | OrderType::Stop => {
                let pending_price = price.ok_or_else(|| {
                    CoreError::InvalidAction(format!("{order_type} order requires a price"))
                })?;
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
                // Assign group if specified.
                if let Some(ref gid) = group {
                    pos.data.group = Some(gid.clone());
                    pos.data.records.push((
                        PositionRecord::GroupAssigned {
                            group_id: gid.clone(),
                        },
                        ts,
                    ));
                    self.manager.add(pos);
                    self.manager.add_to_group(gid, id.clone());
                } else {
                    self.manager.add(pos);
                }
                // Register pending fill alert if alert register is active.
                if self.alert_register.is_some() {
                    self.alert_register.as_mut().unwrap().register(
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

        let fill_price = match price {
            Some(p) => p,
            None => {
                let q = self
                    .last_quotes
                    .get(&pos.data.symbol)
                    .ok_or_else(|| CoreError::NoPriceAvailable(pos.data.symbol.clone()))?;
                q.open_price(pos.data.side)
            }
        };

        let fill = Fill {
            price: fill_price,
            size,
            ts,
        };
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

        pos.data.apply_full_close(CloseReason::Manual, ts);

        // Deregister all alerts for this position.
        if let Some(ref mut register) = self.alert_register {
            register.deregister_position(position_id);
        }

        Ok(vec![Effect::PositionClosed {
            id: position_id.to_owned(),
            reason: CloseReason::Manual,
        }])
    }

    fn action_close_partial(
        &mut self,
        position_id: &str,
        ratio: f64,
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

        let close_price = self
            .last_quotes
            .get(&pos.data.symbol)
            .map(|q| q.close_price(pos.data.side))
            .unwrap_or(pos.data.average_entry());

        let actual_ratio = ratio.min(pos.data.remaining_ratio);
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
        let (symbol, side, old_price_val) = {
            let pos = self
                .manager
                .get_mut(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

            let old = pos.set_stoploss(new_price);
            let old_price = old.unwrap_or(0.0);

            pos.data.records.push((
                PositionRecord::StoplossModified {
                    from: old,
                    to: new_price,
                },
                ts,
            ));
            (pos.data.symbol.clone(), pos.data.side, old_price)
        };

        // Re-register SL alert: deregister old, register new.
        if let Some(ref mut register) = self.alert_register {
            if old_price_val != 0.0 {
                register.deregister_alert(
                    &symbol,
                    old_price_val,
                    position_id,
                    side,
                    &AlertKind::Stoploss,
                );
            }
            register.register(
                &symbol,
                new_price,
                position_id.to_owned(),
                side,
                AlertKind::Stoploss,
            );
        }

        Ok(vec![Effect::StoplossModified {
            id: position_id.to_owned(),
            old_price: old_price_val,
            new_price,
        }])
    }

    fn action_move_sl_to_entry(
        &mut self,
        position_id: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

        let entry = pos.data.average_entry();
        let old = pos.set_stoploss(entry);
        let old_price = old.unwrap_or(0.0);

        pos.data.records.push((
            PositionRecord::StoplossModified {
                from: old,
                to: entry,
            },
            ts,
        ));

        Ok(vec![Effect::StoplossModified {
            id: position_id.to_owned(),
            old_price,
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
        let (symbol, side) = {
            let pos = self
                .manager
                .get_mut(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

            pos.rules.push(Rule::take_profit(price, close_ratio));
            pos.data
                .records
                .push((PositionRecord::TargetAdded { price, close_ratio }, ts));
            (pos.data.symbol.clone(), pos.data.side)
        };

        // Register TP alert if alert register is active.
        if let Some(ref mut register) = self.alert_register {
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
        let (symbol, side, removed) = {
            let pos = self
                .manager
                .get_mut(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

            let before = pos.rules.len();
            pos.rules.retain(|r| {
                if let Rule::TakeProfit {
                    price: tp_price, ..
                } = r
                {
                    (*tp_price - price).abs() > f64::EPSILON
                } else {
                    true
                }
            });

            let did_remove = pos.rules.len() < before;
            if did_remove {
                pos.data
                    .records
                    .push((PositionRecord::TargetRemoved { price }, ts));
            }
            (pos.data.symbol.clone(), pos.data.side, did_remove)
        };

        // Deregister TP alert if alert register is active.
        if removed {
            if let Some(ref mut register) = self.alert_register {
                register.deregister_alert(
                    &symbol,
                    price,
                    position_id,
                    side,
                    &AlertKind::TakeProfit { close_ratio: 0.0 },
                );
            }
        }

        Ok(vec![])
    }

    fn action_add_rule(
        &mut self,
        position_id: &str,
        rule_config: crate::types::RuleConfig,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let rule = Rule::from_config(rule_config);
        let is_stateful = rule.is_stateful();
        let name = rule.name().to_owned();

        let (symbol, side) = {
            let pos = self
                .manager
                .get_mut(position_id)
                .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

            let sym = pos.data.symbol.clone();
            let s = pos.data.side;

            // Register alert for static rules.
            if let Some(ref mut register) = self.alert_register {
                if is_stateful {
                    register.register_tick_eval(&sym, position_id.to_owned());
                } else {
                    match &rule {
                        Rule::FixedStoploss { price } => {
                            register.register(
                                &sym,
                                *price,
                                position_id.to_owned(),
                                s,
                                AlertKind::Stoploss,
                            );
                        }
                        Rule::TakeProfit {
                            price, close_ratio, ..
                        } => {
                            register.register(
                                &sym,
                                *price,
                                position_id.to_owned(),
                                s,
                                AlertKind::TakeProfit {
                                    close_ratio: *close_ratio,
                                },
                            );
                        }
                        Rule::BreakevenWhen { trigger_price, .. } => {
                            register.register(
                                &sym,
                                *trigger_price,
                                position_id.to_owned(),
                                s,
                                AlertKind::BreakevenTrigger,
                            );
                        }
                        _ => {}
                    }
                }
            }

            pos.rules.push(rule);
            pos.data
                .records
                .push((PositionRecord::RuleAdded { rule_name: name }, ts));
            (sym, s)
        };

        let _ = (symbol, side);
        Ok(vec![])
    }

    fn action_remove_rule(
        &mut self,
        position_id: &str,
        rule_name: &str,
        ts: NaiveDateTime,
    ) -> Result<Vec<Effect>> {
        let pos = self
            .manager
            .get_mut(position_id)
            .ok_or_else(|| CoreError::PositionNotFound(position_id.to_owned()))?;

        if pos.remove_rule(rule_name) {
            pos.data.records.push((
                PositionRecord::RuleRemoved {
                    rule_name: rule_name.to_owned(),
                },
                ts,
            ));
        }

        Ok(vec![])
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
        let ids = self.manager.open_ids_by_symbol(symbol);
        let mut effects = Vec::new();
        for id in ids {
            if let Some(pos) = self.manager.get_mut(&id) {
                let old = pos.set_stoploss(price);
                let old_price = old.unwrap_or(0.0);
                pos.data.records.push((
                    PositionRecord::StoplossModified {
                        from: old,
                        to: price,
                    },
                    ts,
                ));
                effects.push(Effect::StoplossModified {
                    id,
                    old_price,
                    new_price: price,
                });
            }
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
            if let Some(pos) = self.manager.get_mut(id) {
                if pos.data.status == PositionStatus::Open {
                    pos.data.apply_full_close(CloseReason::GroupRule, ts);
                    effects.push(Effect::PositionClosed {
                        id: id.clone(),
                        reason: CloseReason::GroupRule,
                    });
                }
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
        let ids = self.manager.open_ids_by_group(group_id);
        let mut effects = Vec::new();
        for id in ids {
            if let Some(pos) = self.manager.get_mut(&id) {
                if pos.data.status == PositionStatus::Open {
                    let old = pos.set_stoploss(price);
                    let old_price = old.unwrap_or(0.0);
                    pos.data.records.push((
                        PositionRecord::StoplossModified {
                            from: old,
                            to: price,
                        },
                        ts,
                    ));
                    effects.push(Effect::StoplossModified {
                        id,
                        old_price,
                        new_price: price,
                    });
                }
            }
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
                    if pos.data.status == PositionStatus::Closed {
                        return; // already closed (e.g. SL and TP on same tick)
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
                    if pos.data.status == PositionStatus::Closed {
                        return;
                    }
                    let close_price = quote.close_price(pos.data.side);
                    pos.data
                        .apply_partial_close(*ratio, close_price, *reason, quote.ts);
                }
            }
            Effect::StoplossModified { id, new_price, .. } => {
                let old_and_info = if let Some(pos) = self.manager.get_mut(id) {
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
                if let Some((old, symbol, side)) = old_and_info {
                    if let Some(ref mut register) = self.alert_register {
                        if let Some(old_price) = old {
                            register.deregister_alert(
                                &symbol,
                                old_price,
                                id,
                                side,
                                &AlertKind::Stoploss,
                            );
                        }
                        register.register(
                            &symbol,
                            *new_price,
                            id.clone(),
                            side,
                            AlertKind::Stoploss,
                        );
                    }
                }
            }
            // Other effects are informational — no internal state change needed.
            _ => {}
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

                let remaining = pos.data.remaining_ratio;
                let actual_ratio = close_ratio.min(remaining);

                // Mark the corresponding TakeProfit rule as triggered.
                for rule in &mut pos.rules {
                    if let Rule::TakeProfit {
                        price, triggered, ..
                    } = rule
                    {
                        if !*triggered
                            && (price_to_micros_static(*price)
                                == price_to_micros_static(alert.trigger_price))
                        {
                            *triggered = true;
                            break;
                        }
                    }
                }

                if (remaining - actual_ratio).abs() < f64::EPSILON {
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

/// Helper to convert price to micros (standalone function usable in non-method contexts).
fn price_to_micros_static(price: f64) -> i64 {
    (price * 1_000_000.0).round() as i64
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Action, OrderType, RuleConfig, TargetSpec};
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
