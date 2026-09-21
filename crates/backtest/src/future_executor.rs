//! Fill-authoritative accounting for the FutureQuoteV1 replay path.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::NaiveDateTime;

use qs_core::TradeEngine;
use qs_core::costs::{CostBasis, CostCharge, CostKind, InstrumentCosts};
use qs_core::types::{
    CloseReason, Effect, EffectiveStop, FillPurpose, FutureEffect, FutureFill, PriceQuote, Side,
    StopOrigin, position_size_tolerance,
};
use thiserror::Error;

use crate::artifacts::{
    CloseEvent, CompletedPosition, CostEvent, OpenPositionSnapshot, PendingOrderLifecycleEvent,
    PendingOrderLifecycleState, RecordedFill, RiskBasisStatus, RiskTranche, deterministic_event_id,
};
use crate::currency::{
    ConversionError, ConversionQuoteBook, ConversionResult, ConversionRoute, RunCurrencyPlan,
};
use crate::portfolio::PortfolioRecorder;
use crate::report::TradeResult;

#[derive(Debug, Clone)]
struct PendingOrigin {
    position_id: String,
    placement_action_id: Option<String>,
    signal_ts: Option<NaiveDateTime>,
    effective_ts: NaiveDateTime,
    placed_ts: NaiveDateTime,
    symbol: String,
    side: Side,
    order_type: qs_core::types::OrderType,
    requested_size: f64,
    requested_price: Option<f64>,
    placement_sequence: u64,
    initial_stop: Option<f64>,
}

#[derive(Debug, Error)]
pub(crate) enum FutureExecutorError {
    #[error("position not found while processing FutureQuote effect: {0}")]
    PositionNotFound(String),
    #[error("account not found while processing FutureQuote effect: {0}")]
    AccountNotFound(String),
    #[error("fill-bearing effect was emitted without a fill: {0}")]
    MissingFill(String),
    #[error("non-fill effect unexpectedly carried a fill: {0}")]
    UnexpectedFill(String),
    #[error("invalid carried fill for {position_id}: {reason}")]
    InvalidFill { position_id: String, reason: String },
    #[error("portfolio rejected realized P&L {pnl} for {position_id}")]
    PortfolioRejectedRealizedPnl { position_id: String, pnl: f64 },
    #[error("currency plan has no P&L currency or route for primary symbol {0}")]
    MissingCurrencyRoute(String),

    #[error("account conversion failed for {symbol} at {operation_ts}: {source}")]
    Conversion {
        symbol: String,
        operation_ts: NaiveDateTime,
        #[source]
        source: ConversionError,
    },
    #[error("account conversion for {symbol} produced invalid {kind} amount {amount}")]
    InvalidConvertedAmount {
        symbol: String,
        kind: &'static str,
        amount: f64,
    },
}

#[derive(Debug, Clone)]
struct PositionAccount {
    position_id: String,
    symbol: String,
    side: Side,
    group: Option<String>,
    trade_id: Option<String>,
    open_ts: NaiveDateTime,
    entry_size: f64,
    remaining_size: f64,
    /// Value of every historical entry, used only for campaign-level audit.
    entry_value: f64,
    /// Average-cost basis assigned only to inventory that is still open.
    open_entry_value: f64,
    initial_stop: Option<f64>,
    effective_stop: Option<EffectiveStop>,
    risk_tranches: Vec<RiskTranche>,
    close_events: Vec<CloseEvent>,
    realized_pnl: f64,
    native_realized_pnl: f64,
    native_currency: Option<String>,
    account_currency: Option<String>,
    /// Account-currency commission charged on entry and scale-in fills.
    entry_commission_total: f64,
    /// Account-currency swap charged while this position was open.
    swap_total: f64,
}

/// One trading cost after optional account conversion.
#[derive(Debug, Clone, Default)]
struct ResolvedCost {
    /// Signed account-currency amount, where positive reduces the balance.
    amount: f64,
    native_amount: Option<f64>,
    native_currency: Option<String>,
    conversion: Option<ConversionResult>,
}

#[derive(Debug, Clone)]
struct AccountedAmount {
    amount: f64,
    native_currency: Option<String>,
    conversion: Option<ConversionResult>,
}

impl PositionAccount {
    fn average_entry(&self) -> f64 {
        if self.remaining_size <= position_size_tolerance(self.entry_size) {
            0.0
        } else {
            self.open_entry_value / self.remaining_size
        }
    }

    fn historical_average_entry(&self) -> f64 {
        if self.entry_size <= position_size_tolerance(self.entry_size) {
            0.0
        } else {
            self.entry_value / self.entry_size
        }
    }

    fn snapshot(&self) -> OpenPositionSnapshot {
        let mut snapshot = OpenPositionSnapshot::new(
            self.position_id.clone(),
            self.symbol.clone(),
            self.side,
            self.average_entry(),
            self.remaining_size,
        );
        snapshot.group = self.group.clone();
        snapshot.trade_id = self.trade_id.clone();
        snapshot.open_ts = Some(self.open_ts);
        snapshot.initial_stop = self.initial_stop;
        snapshot.effective_stop = self.effective_stop;
        snapshot.realized_pnl = self.realized_pnl;
        snapshot.native_realized_pnl = Some(self.native_realized_pnl);
        snapshot.native_currency = self.native_currency.clone();
        snapshot.account_currency = self.account_currency.clone();
        snapshot
    }
}

/// Accounting adapter for quote-authoritative FutureQuoteV1 execution.
#[derive(Debug, Clone)]
pub struct FutureExecutor {
    initial_balance: f64,
    balance: f64,
    contract_sizes: HashMap<String, f64>,
    costs: HashMap<String, InstrumentCosts>,
    point_sizes: HashMap<String, f64>,
    currency_plan: Option<RunCurrencyPlan>,
    accounts: BTreeMap<String, PositionAccount>,
    pending_origins: BTreeMap<String, PendingOrigin>,
    terminal_pending_orders: BTreeSet<String>,

    pub fills: Vec<RecordedFill>,
    pub pending_order_lifecycle: Vec<PendingOrderLifecycleEvent>,
    pub close_events: Vec<CloseEvent>,
    pub cost_events: Vec<CostEvent>,
    pub completed_positions: Vec<CompletedPosition>,
    pub trade_log: Vec<TradeResult>,
    fill_sequence: u64,
    close_sequence: u64,
    cost_sequence: u64,
    pending_lifecycle_sequence: u64,
    pnl_epsilon: f64,
    last_rollover_ts: Option<NaiveDateTime>,
}

#[derive(Debug)]
struct FutureExecutorCheckpoint {
    accounts: BTreeMap<String, Option<PositionAccount>>,
    pending_origins: BTreeMap<String, Option<PendingOrigin>>,
    terminal_pending_orders: BTreeMap<String, bool>,
    balance: f64,
    fill_sequence: u64,
    close_sequence: u64,
    cost_sequence: u64,
    pending_lifecycle_sequence: u64,
    fills_len: usize,
    pending_order_lifecycle_len: usize,
    close_events_len: usize,
    cost_events_len: usize,
    completed_positions_len: usize,
    trade_log_len: usize,
}

#[derive(Debug)]
struct PendingCampaignCompletion {
    position_id: String,
    final_net_pnl: f64,
    completed_position_index: usize,
}

#[derive(Debug)]
struct PortfolioBatch {
    realized_pnl: f64,
    realized_pnl_changed: bool,
    campaign_completions: Vec<PendingCampaignCompletion>,
}

impl PortfolioBatch {
    fn new(portfolio: &PortfolioRecorder) -> Self {
        Self {
            realized_pnl: portfolio.realized_pnl(),
            realized_pnl_changed: false,
            campaign_completions: Vec::new(),
        }
    }

    fn add_realized_pnl(&mut self, pnl: f64) -> bool {
        let next = self.realized_pnl + pnl;
        if !pnl.is_finite() || !next.is_finite() {
            return false;
        }
        self.realized_pnl = next;
        self.realized_pnl_changed = true;
        true
    }

    fn finish_campaign(
        &mut self,
        position_id: String,
        final_net_pnl: f64,
        completed_position_index: usize,
    ) {
        self.campaign_completions.push(PendingCampaignCompletion {
            position_id,
            final_net_pnl,
            completed_position_index,
        });
    }

    fn commit(self, portfolio: &mut PortfolioRecorder, completed: &mut [CompletedPosition]) {
        if self.realized_pnl_changed {
            let updated = portfolio.set_realized_pnl(self.realized_pnl);
            debug_assert!(updated, "staged realized P&L was prevalidated");
        }
        for completion in self.campaign_completions {
            let excursion =
                portfolio.finish_campaign(&completion.position_id, completion.final_net_pnl);
            if let Some(position) = completed.get_mut(completion.completed_position_index) {
                position.mae = excursion.map(|value| value.mae);
                position.mfe = excursion.map(|value| value.mfe);
            }
        }
    }
}

impl FutureExecutorCheckpoint {
    fn capture(executor: &FutureExecutor, effects: &[FutureEffect]) -> Self {
        let affected_ids: BTreeSet<_> = effects
            .iter()
            .map(|effect| effect_position_id(effect.effect()).to_owned())
            .collect();
        let accounts = affected_ids
            .iter()
            .map(|id| (id.clone(), executor.accounts.get(id).cloned()))
            .collect();
        let pending_origins = affected_ids
            .iter()
            .map(|id| (id.clone(), executor.pending_origins.get(id).cloned()))
            .collect();
        let terminal_pending_orders = affected_ids
            .into_iter()
            .map(|id| {
                let present = executor.terminal_pending_orders.contains(&id);
                (id, present)
            })
            .collect();
        Self {
            accounts,
            pending_origins,
            terminal_pending_orders,
            balance: executor.balance,
            fill_sequence: executor.fill_sequence,
            close_sequence: executor.close_sequence,
            cost_sequence: executor.cost_sequence,
            pending_lifecycle_sequence: executor.pending_lifecycle_sequence,
            fills_len: executor.fills.len(),
            pending_order_lifecycle_len: executor.pending_order_lifecycle.len(),
            close_events_len: executor.close_events.len(),
            cost_events_len: executor.cost_events.len(),
            completed_positions_len: executor.completed_positions.len(),
            trade_log_len: executor.trade_log.len(),
        }
    }

    fn restore(self, executor: &mut FutureExecutor) {
        restore_entries(&mut executor.accounts, self.accounts);
        restore_entries(&mut executor.pending_origins, self.pending_origins);
        for (id, present) in self.terminal_pending_orders {
            if present {
                executor.terminal_pending_orders.insert(id);
            } else {
                executor.terminal_pending_orders.remove(&id);
            }
        }
        executor.balance = self.balance;
        executor.fill_sequence = self.fill_sequence;
        executor.close_sequence = self.close_sequence;
        executor.cost_sequence = self.cost_sequence;
        executor.pending_lifecycle_sequence = self.pending_lifecycle_sequence;
        executor.fills.truncate(self.fills_len);
        executor
            .pending_order_lifecycle
            .truncate(self.pending_order_lifecycle_len);
        executor.close_events.truncate(self.close_events_len);
        executor.cost_events.truncate(self.cost_events_len);
        executor
            .completed_positions
            .truncate(self.completed_positions_len);
        executor.trade_log.truncate(self.trade_log_len);
    }
}

fn restore_entries<T>(entries: &mut BTreeMap<String, T>, checkpoint: BTreeMap<String, Option<T>>) {
    for (id, value) in checkpoint {
        match value {
            Some(value) => {
                entries.insert(id, value);
            }
            None => {
                entries.remove(&id);
            }
        }
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

impl FutureExecutor {
    pub fn new(
        initial_balance: f64,
        contract_sizes: HashMap<String, f64>,
        pnl_epsilon: f64,
    ) -> Self {
        Self {
            initial_balance,
            balance: initial_balance,
            contract_sizes,
            costs: HashMap::new(),
            point_sizes: HashMap::new(),
            currency_plan: None,
            accounts: BTreeMap::new(),
            pending_origins: BTreeMap::new(),
            terminal_pending_orders: BTreeSet::new(),

            fills: Vec::new(),
            pending_order_lifecycle: Vec::new(),
            close_events: Vec::new(),
            cost_events: Vec::new(),
            completed_positions: Vec::new(),
            trade_log: Vec::new(),
            fill_sequence: 0,
            close_sequence: 0,
            cost_sequence: 0,
            pending_lifecycle_sequence: 0,
            pnl_epsilon: if pnl_epsilon.is_finite() {
                pnl_epsilon.abs()
            } else {
                1.0e-9
            },
            last_rollover_ts: None,
        }
    }

    pub fn with_currency_plan(mut self, currency_plan: Option<RunCurrencyPlan>) -> Self {
        self.currency_plan = currency_plan;
        self
    }

    /// Attach per-symbol trading costs and the price point size used by point-denominated swap.
    pub fn with_costs(
        mut self,
        costs: HashMap<String, InstrumentCosts>,
        point_sizes: HashMap<String, f64>,
    ) -> Self {
        self.costs = costs;
        self.point_sizes = point_sizes;
        self
    }

    pub fn balance(&self) -> f64 {
        self.balance
    }

    pub fn realized_pnl(&self) -> f64 {
        self.balance - self.initial_balance
    }

    pub(crate) fn requires_processing(effects: &[FutureEffect]) -> bool {
        effects.iter().any(|effect| {
            !matches!(
                effect,
                FutureEffect::Plain {
                    effect: Effect::RuleTriggered { .. },
                    ..
                }
            )
        })
    }

    pub fn has_close(&self, position_id: &str) -> bool {
        self.accounts
            .get(position_id)
            .is_some_and(|account| !account.close_events.is_empty())
            || self
                .completed_positions
                .iter()
                .any(|position| position.position_id == position_id)
    }

    pub fn pending_metadata(
        &self,
        position_id: &str,
    ) -> Option<(String, NaiveDateTime, NaiveDateTime)> {
        self.pending_origins.get(position_id).and_then(|origin| {
            Some((
                origin.placement_action_id.clone()?,
                origin.signal_ts?,
                origin.effective_ts,
            ))
        })
    }

    pub fn open_snapshots(&self) -> Vec<OpenPositionSnapshot> {
        self.accounts
            .values()
            .filter(|account| account.remaining_size > position_size_tolerance(account.entry_size))
            .map(PositionAccount::snapshot)
            .collect()
    }

    /// Mark every still-pending order as unfilled at the deterministic end of
    /// the accepted quote stream. Pending snapshots remain available separately.
    pub fn finalize_pending_orders_at_end(&mut self, terminal_ts: NaiveDateTime) {
        let mut pending: Vec<_> = self.pending_origins.values().cloned().collect();
        pending.sort_by_key(|origin| origin.placement_sequence);
        for origin in pending {
            self.record_pending_terminal(
                &origin,
                PendingOrderLifecycleState::UnfilledAtEnd,
                None,
                0.0,
                None,
                terminal_ts,
            );
        }
    }

    #[allow(dead_code, clippy::too_many_arguments)]
    pub(crate) fn process_future_effects(
        &mut self,
        effects: &[FutureEffect],
        engine: &TradeEngine,
        quote: &PriceQuote,
        action_id: Option<&str>,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        portfolio: &mut PortfolioRecorder,
    ) -> Result<Vec<String>, FutureExecutorError> {
        self.process_future_effects_with_currency(
            effects,
            engine,
            quote,
            action_id,
            signal_ts,
            effective_ts,
            portfolio,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process_future_effects_with_currency(
        &mut self,
        effects: &[FutureEffect],
        engine: &TradeEngine,
        quote: &PriceQuote,
        action_id: Option<&str>,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        portfolio: &mut PortfolioRecorder,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<Vec<String>, FutureExecutorError> {
        if !Self::requires_processing(effects) {
            return Ok(Vec::new());
        }

        let checkpoint = FutureExecutorCheckpoint::capture(self, effects);
        let mut portfolio_batch = PortfolioBatch::new(portfolio);
        let result = (|| -> Result<Vec<String>, FutureExecutorError> {
            let mut affected = Vec::new();
            for future_effect in effects {
                match future_effect {
                    FutureEffect::Plain {
                        effect,
                        stop_origin,
                        ..
                    } => match effect {
                        Effect::PositionOpened { .. }
                        | Effect::PositionClosed { .. }
                        | Effect::PartialClose { .. }
                        | Effect::ScaledIn { .. } => {
                            return Err(FutureExecutorError::MissingFill(format!("{effect:?}")));
                        }
                        Effect::OrderPlaced { id } => {
                            let position = engine
                                .get_position(id)
                                .ok_or_else(|| FutureExecutorError::PositionNotFound(id.clone()))?;
                            let origin = PendingOrigin {
                                position_id: id.clone(),
                                placement_action_id: action_id.map(str::to_owned),
                                signal_ts,
                                effective_ts,
                                placed_ts: quote.ts,
                                symbol: position.data.symbol.clone(),
                                side: position.data.side,
                                order_type: position.data.order_type,
                                requested_size: position.data.size,
                                requested_price: position.data.pending_price,
                                placement_sequence: self.pending_lifecycle_sequence,
                                initial_stop: position.current_stoploss(),
                            };
                            self.record_pending_placed(&origin);
                            self.pending_origins.insert(id.clone(), origin);
                        }
                        Effect::OrderCancelled { id } => {
                            if let Some(origin) = self.pending_origins.remove(id) {
                                self.record_pending_terminal(
                                    &origin,
                                    PendingOrderLifecycleState::Cancelled,
                                    action_id.map(str::to_owned),
                                    0.0,
                                    None,
                                    quote.ts,
                                );
                            }
                        }
                        Effect::StoplossModified { id, new_price, .. } => {
                            if !new_price.is_finite() || *new_price <= 0.0 {
                                return Err(FutureExecutorError::InvalidFill {
                                    position_id: id.clone(),
                                    reason: format!(
                                        "stoploss must be finite and positive, got {new_price}"
                                    ),
                                });
                            }
                            if let Some(account) = self.accounts.get_mut(id) {
                                account.effective_stop = Some(EffectiveStop::new(
                                    *new_price,
                                    stop_origin.unwrap_or(StopOrigin::Modified),
                                ));
                            } else if self.pending_origins.contains_key(id) {
                                // The immutable initial stop stays on the pending origin;
                                // the engine supplies the effective stop when it fills.
                            } else {
                                return Err(FutureExecutorError::AccountNotFound(id.clone()));
                            }
                            affected.push(id.clone());
                        }
                        Effect::StoplossRemoved { id, .. } => {
                            if let Some(account) = self.accounts.get_mut(id) {
                                account.effective_stop = None;
                            } else if self.pending_origins.contains_key(id) {
                                // Removing a pending stop changes engine state only; the
                                // placement-time initial stop remains an audit field.
                            } else {
                                return Err(FutureExecutorError::AccountNotFound(id.clone()));
                            }
                            affected.push(id.clone());
                        }
                        Effect::RuleTriggered { .. } => {}
                    },
                    FutureEffect::Filled { effect, fill, .. } => {
                        self.validate_carried_fill(effect, fill, quote)?;
                        match effect {
                            Effect::PositionOpened { id } => {
                                self.record_open(
                                    id,
                                    fill,
                                    engine,
                                    quote,
                                    action_id,
                                    signal_ts,
                                    effective_ts,
                                    &mut portfolio_batch,
                                    conversion_quotes,
                                )?;
                                affected.push(id.clone());
                            }
                            Effect::ScaledIn { id, .. } => {
                                self.record_scale_in(
                                    id,
                                    fill,
                                    engine,
                                    quote,
                                    action_id,
                                    signal_ts,
                                    effective_ts,
                                    &mut portfolio_batch,
                                    conversion_quotes,
                                )?;
                                affected.push(id.clone());
                            }
                            Effect::PositionClosed { id, reason } => {
                                self.record_close(
                                    id,
                                    *reason,
                                    fill,
                                    quote,
                                    action_id,
                                    signal_ts,
                                    effective_ts,
                                    &mut portfolio_batch,
                                    true,
                                    conversion_quotes,
                                )?;
                                if self.accounts.contains_key(id) {
                                    return Err(FutureExecutorError::InvalidFill {
                                        position_id: id.clone(),
                                        reason: "full-close effect left an open account".into(),
                                    });
                                }
                                affected.push(id.clone());
                            }
                            Effect::PartialClose { id, reason, .. } => {
                                self.record_close(
                                    id,
                                    *reason,
                                    fill,
                                    quote,
                                    action_id,
                                    signal_ts,
                                    effective_ts,
                                    &mut portfolio_batch,
                                    false,
                                    conversion_quotes,
                                )?;
                                affected.push(id.clone());
                            }
                            _ => {
                                return Err(FutureExecutorError::UnexpectedFill(format!(
                                    "{effect:?}"
                                )));
                            }
                        }
                    }
                }
            }
            affected.sort();
            affected.dedup();
            Ok(affected)
        })();

        match result {
            Ok(affected) => {
                portfolio_batch.commit(portfolio, &mut self.completed_positions);
                Ok(affected)
            }
            Err(error) => {
                checkpoint.restore(self);
                Err(error)
            }
        }
    }

    fn record_pending_placed(&mut self, origin: &PendingOrigin) {
        let event = self.pending_lifecycle_event(
            origin,
            PendingOrderLifecycleState::Placed,
            None,
            None,
            None,
            None,
        );
        self.pending_order_lifecycle.push(event);
    }

    fn record_pending_terminal(
        &mut self,
        origin: &PendingOrigin,
        state: PendingOrderLifecycleState,
        terminal_action_id: Option<String>,
        filled_size: f64,
        fill_price: Option<f64>,
        terminal_ts: NaiveDateTime,
    ) {
        debug_assert!(state.is_terminal());
        if !self
            .terminal_pending_orders
            .insert(origin.position_id.clone())
        {
            return;
        }
        let event = self.pending_lifecycle_event(
            origin,
            state,
            terminal_action_id,
            Some(filled_size),
            fill_price,
            Some(terminal_ts),
        );
        self.pending_order_lifecycle.push(event);
    }

    fn pending_lifecycle_event(
        &mut self,
        origin: &PendingOrigin,
        state: PendingOrderLifecycleState,
        terminal_action_id: Option<String>,
        filled_size: Option<f64>,
        fill_price: Option<f64>,
        terminal_ts: Option<NaiveDateTime>,
    ) -> PendingOrderLifecycleEvent {
        let sequence = self.pending_lifecycle_sequence;
        self.pending_lifecycle_sequence += 1;
        let kind = match state {
            PendingOrderLifecycleState::Placed => "pending_placed",
            PendingOrderLifecycleState::Filled => "pending_filled",
            PendingOrderLifecycleState::Cancelled => "pending_cancelled",
            PendingOrderLifecycleState::UnfilledAtEnd => "pending_unfilled_at_end",
        };
        let wait_latency_ms = terminal_ts
            .map(|terminal_ts| (terminal_ts - origin.placed_ts).num_milliseconds().max(0));
        let fill_ratio = filled_size.and_then(|filled_size| {
            (origin.requested_size.is_finite() && origin.requested_size > 0.0)
                .then_some(filled_size / origin.requested_size)
        });

        PendingOrderLifecycleEvent {
            id: deterministic_event_id(&origin.position_id, kind, sequence),
            sequence,
            position_id: origin.position_id.clone(),
            placement_action_id: origin.placement_action_id.clone(),
            terminal_action_id,
            state,
            symbol: origin.symbol.clone(),
            side: origin.side,
            order_type: origin.order_type,
            requested_size: origin.requested_size,
            filled_size,
            requested_price: origin.requested_price,
            fill_price,
            signal_ts: origin.signal_ts,
            placed_ts: Some(origin.placed_ts),
            effective_ts: Some(origin.effective_ts),
            terminal_ts,
            wait_latency_ms,
            fill_ratio,
        }
    }

    fn validate_carried_fill(
        &self,
        effect: &Effect,
        fill: &FutureFill,
        quote: &PriceQuote,
    ) -> Result<(), FutureExecutorError> {
        let position_id = match effect {
            Effect::PositionOpened { id }
            | Effect::PositionClosed { id, .. }
            | Effect::PartialClose { id, .. }
            | Effect::ScaledIn { id, .. } => id.clone(),
            _ => "<non-fill-effect>".into(),
        };
        if fill.source_quote_ts() != quote.ts {
            return Err(FutureExecutorError::InvalidFill {
                position_id,
                reason: format!(
                    "fill source quote timestamp {} does not match quote {}",
                    fill.source_quote_ts(),
                    quote.ts
                ),
            });
        }
        if fill.ts < quote.ts {
            return Err(FutureExecutorError::InvalidFill {
                position_id,
                reason: format!(
                    "fill execution timestamp {} precedes source quote {}",
                    fill.ts, quote.ts
                ),
            });
        }
        if !fill.size.is_finite() || fill.size <= position_size_tolerance(fill.size) {
            return Err(FutureExecutorError::InvalidFill {
                position_id,
                reason: format!(
                    "size must be finite and greater than the accounting tolerance, got {}",
                    fill.size
                ),
            });
        }
        if !fill.execution.price.is_finite() || fill.execution.price <= 0.0 {
            return Err(FutureExecutorError::InvalidFill {
                position_id,
                reason: format!(
                    "price must be finite and positive, got {}",
                    fill.execution.price
                ),
            });
        }
        let purpose_matches = match effect {
            Effect::PositionOpened { .. } => matches!(
                fill.execution.purpose,
                FillPurpose::MarketEntry | FillPurpose::LimitEntry | FillPurpose::StopEntry
            ),
            Effect::ScaledIn { .. } => fill.execution.purpose == FillPurpose::MarketEntry,
            Effect::PositionClosed { reason, .. } | Effect::PartialClose { reason, .. } => {
                fill.execution.purpose
                    == match reason {
                        CloseReason::Target => FillPurpose::TakeProfit,
                        CloseReason::Stoploss
                        | CloseReason::TrailingStop
                        | CloseReason::BreakevenStop => FillPurpose::StopLoss,
                        _ => FillPurpose::MarketExit,
                    }
            }
            _ => false,
        };
        if !purpose_matches {
            return Err(FutureExecutorError::InvalidFill {
                position_id,
                reason: format!(
                    "execution purpose {:?} does not match effect",
                    fill.execution.purpose
                ),
            });
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_open(
        &mut self,
        id: &str,
        fill: &FutureFill,
        engine: &TradeEngine,
        quote: &PriceQuote,
        action_id: Option<&str>,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        portfolio_batch: &mut PortfolioBatch,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<(), FutureExecutorError> {
        if self.accounts.contains_key(id) {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "position already has an open accounting record".into(),
            });
        }
        let position = engine
            .get_position(id)
            .ok_or_else(|| FutureExecutorError::PositionNotFound(id.to_owned()))?;
        if position.data.status != qs_core::types::PositionStatus::Open {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: format!("position is not open: {}", position.data.status),
            });
        }
        if position.data.side != fill.execution.side {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "execution side does not match position".into(),
            });
        }
        let execution = fill.execution;
        let size = fill.size;
        let current_stop = position.current_effective_stop();
        let group = position.data.group.clone();
        let trade_id = position.data.trade_id.clone();
        let side = position.data.side;
        let symbol = position.data.symbol.clone();
        let open_ts = fill.ts;

        let origin = self.pending_origins.get(id).cloned();
        let initial_stop = origin.as_ref().map_or_else(
            || current_stop.map(|stop| stop.price),
            |value| value.initial_stop,
        );
        let recorded_action_id = action_id.map(str::to_owned).or_else(|| {
            origin
                .as_ref()
                .and_then(|value| value.placement_action_id.clone())
        });
        let recorded_signal_ts =
            signal_ts.or_else(|| origin.as_ref().and_then(|value| value.signal_ts));
        let recorded_effective_ts = origin
            .as_ref()
            .map(|value| value.effective_ts)
            .unwrap_or(effective_ts);
        let recorded = RecordedFill::from_quote_at(
            id.to_owned(),
            recorded_action_id,
            self.fill_sequence,
            recorded_signal_ts,
            recorded_effective_ts,
            fill.ts,
            size,
            quote,
            execution,
        );
        let contract_size = self.contract_size(&symbol);
        let risk = self.account_risk_tranche(
            &symbol,
            fill.ts,
            RiskTranche::calculate(
                Some(recorded.id.clone()),
                side,
                size,
                execution.price,
                current_stop.map(|stop| stop.price),
                contract_size,
                self.pnl_epsilon,
            ),
            conversion_quotes,
        )?;
        let symbol_for_cost = symbol.clone();
        let commission = self.resolve_commission(
            &symbol,
            side,
            size,
            execution.price,
            fill.ts,
            conversion_quotes,
        )?;
        let next_balance = self.balance - commission.amount;
        if !next_balance.is_finite()
            || (commission.amount != 0.0 && !portfolio_batch.add_realized_pnl(-commission.amount))
        {
            return Err(FutureExecutorError::PortfolioRejectedRealizedPnl {
                position_id: id.to_owned(),
                pnl: -commission.amount,
            });
        }
        self.balance = next_balance;
        self.fill_sequence += 1;
        self.pending_origins.remove(id);
        self.fills.push(recorded);
        self.accounts.insert(
            id.to_owned(),
            PositionAccount {
                position_id: id.to_owned(),
                symbol,
                side,
                group,
                trade_id,
                open_ts,
                entry_size: size,
                remaining_size: size,
                entry_value: execution.price * size,
                open_entry_value: execution.price * size,
                initial_stop,
                effective_stop: current_stop,
                native_currency: risk.native_currency.clone(),
                account_currency: self
                    .currency_plan
                    .as_ref()
                    .map(|plan| plan.account_currency().to_owned()),
                risk_tranches: vec![risk],
                close_events: Vec::new(),
                realized_pnl: -commission.amount,
                native_realized_pnl: 0.0,
                entry_commission_total: commission.amount,
                swap_total: 0.0,
            },
        );
        if commission.amount != 0.0 {
            self.push_cost_event(
                id,
                &symbol_for_cost,
                side,
                fill.ts,
                CostKind::EntryCommission,
                &commission,
                size,
                None,
            );
        }
        if let Some(origin) = origin {
            self.record_pending_terminal(
                &origin,
                PendingOrderLifecycleState::Filled,
                action_id.map(str::to_owned),
                size,
                Some(execution.price),
                fill.ts,
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_scale_in(
        &mut self,
        id: &str,
        fill: &FutureFill,
        engine: &TradeEngine,
        quote: &PriceQuote,
        action_id: Option<&str>,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        portfolio_batch: &mut PortfolioBatch,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<(), FutureExecutorError> {
        let position = engine
            .get_position(id)
            .ok_or_else(|| FutureExecutorError::PositionNotFound(id.to_owned()))?;
        if position.data.status != qs_core::types::PositionStatus::Open {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: format!("position is not open: {}", position.data.status),
            });
        }
        if position.data.side != fill.execution.side {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "execution side does not match position".into(),
            });
        }
        let account = self
            .accounts
            .get(id)
            .ok_or_else(|| FutureExecutorError::AccountNotFound(id.to_owned()))?;
        let symbol = account.symbol.clone();
        let side = account.side;
        let effective_stop = account.effective_stop;
        let execution = fill.execution;
        let size = fill.size;
        let recorded = RecordedFill::from_quote_at(
            id.to_owned(),
            action_id.map(str::to_owned),
            self.fill_sequence,
            signal_ts,
            effective_ts,
            fill.ts,
            size,
            quote,
            execution,
        );
        let contract_size = self.contract_size(&symbol);
        let risk = self.account_risk_tranche(
            &symbol,
            fill.ts,
            RiskTranche::calculate(
                Some(recorded.id.clone()),
                side,
                size,
                execution.price,
                effective_stop.map(|stop| stop.price),
                contract_size,
                self.pnl_epsilon,
            ),
            conversion_quotes,
        )?;
        let commission = self.resolve_commission(
            &symbol,
            side,
            size,
            execution.price,
            fill.ts,
            conversion_quotes,
        )?;
        let next_balance = self.balance - commission.amount;
        if !next_balance.is_finite()
            || (commission.amount != 0.0 && !portfolio_batch.add_realized_pnl(-commission.amount))
        {
            return Err(FutureExecutorError::PortfolioRejectedRealizedPnl {
                position_id: id.to_owned(),
                pnl: -commission.amount,
            });
        }
        self.balance = next_balance;
        self.fill_sequence += 1;
        let account = self.accounts.get_mut(id).expect("account checked above");
        account.risk_tranches.push(risk);
        account.entry_size += size;
        account.remaining_size += size;
        account.entry_value += execution.price * size;
        account.open_entry_value += execution.price * size;
        account.entry_commission_total += commission.amount;
        account.realized_pnl -= commission.amount;
        self.fills.push(recorded);
        if commission.amount != 0.0 {
            self.push_cost_event(
                id,
                &symbol,
                side,
                fill.ts,
                CostKind::EntryCommission,
                &commission,
                size,
                None,
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_close(
        &mut self,
        id: &str,
        reason: CloseReason,
        fill: &FutureFill,
        quote: &PriceQuote,
        action_id: Option<&str>,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        portfolio_batch: &mut PortfolioBatch,
        full_close: bool,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<(), FutureExecutorError> {
        let account = self
            .accounts
            .get(id)
            .ok_or_else(|| FutureExecutorError::AccountNotFound(id.to_owned()))?;
        let tolerance = position_size_tolerance(account.entry_size);
        if account.remaining_size <= tolerance {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "position has no remaining size".into(),
            });
        }
        if account.side != fill.execution.side {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "execution side does not match account".into(),
            });
        }
        if fill.size > account.remaining_size + tolerance {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: format!(
                    "close size {} exceeds remaining size {}",
                    fill.size, account.remaining_size
                ),
            });
        }
        if full_close && (fill.size - account.remaining_size).abs() > tolerance {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: format!(
                    "full-close size {} does not consume remaining size {}",
                    fill.size, account.remaining_size
                ),
            });
        }

        let execution = fill.execution;
        let close_size = if full_close {
            account.remaining_size
        } else {
            fill.size.min(account.remaining_size)
        };
        let next_remaining = (account.remaining_size - close_size).max(0.0);
        if !full_close && next_remaining <= tolerance {
            return Err(FutureExecutorError::InvalidFill {
                position_id: id.to_owned(),
                reason: "partial-close effect consumed the entire account".into(),
            });
        }
        let contract_size = self.contract_size(&account.symbol);
        let entry_price = account.average_entry();
        let native_pnl = match account.side {
            Side::Buy => execution.price - entry_price,
            Side::Sell => entry_price - execution.price,
        } * close_size
            * contract_size;
        let accounted =
            self.convert_native_amount(&account.symbol, native_pnl, fill.ts, conversion_quotes)?;
        let cost_symbol = account.symbol.clone();
        // A close trades against the position, so an asymmetric venue charges the opposite side's rate.
        let cost_side = account.side.opposite();
        let commission = self.resolve_commission(
            &cost_symbol,
            cost_side,
            close_size,
            execution.price,
            fill.ts,
            conversion_quotes,
        )?;
        let pnl = accounted.amount - commission.amount;
        let next_balance = self.balance + pnl;
        if !next_balance.is_finite() || !portfolio_batch.add_realized_pnl(pnl) {
            return Err(FutureExecutorError::PortfolioRejectedRealizedPnl {
                position_id: id.to_owned(),
                pnl,
            });
        }

        let recorded = RecordedFill::from_quote_at(
            id.to_owned(),
            action_id.map(str::to_owned),
            self.fill_sequence,
            signal_ts,
            effective_ts,
            fill.ts,
            close_size,
            quote,
            execution,
        );
        self.fill_sequence += 1;

        let account = self.accounts.get_mut(id).expect("account checked above");
        account.remaining_size = if full_close { 0.0 } else { next_remaining };
        account.open_entry_value = if full_close {
            0.0
        } else {
            (account.open_entry_value - entry_price * close_size).max(0.0)
        };
        account.realized_pnl += pnl;
        account.native_realized_pnl += native_pnl;

        let mut event = CloseEvent::new(
            id.to_owned(),
            self.close_sequence,
            account.symbol.clone(),
            account.side,
            fill.ts,
            close_size,
            execution.price,
            pnl,
            reason,
        );
        self.close_sequence += 1;
        event.action_id = action_id.map(str::to_owned);
        event.fill_id = Some(recorded.id.clone());
        event.entry_price = Some(entry_price);
        event.commission = commission.amount;
        event.native_pnl = Some(native_pnl);
        event.native_currency = accounted.native_currency;
        event.pnl_conversion = accounted.conversion;
        event.remaining_size = Some(account.remaining_size);
        account.close_events.push(event.clone());
        let trade = TradeResult {
            position_id: id.to_owned(),
            symbol: account.symbol.clone(),
            side: account.side,
            entry_price,
            exit_price: execution.price,
            size: close_size,
            pnl,
            open_ts: account.open_ts,
            close_ts: fill.ts,
            close_reason: reason,
            group: account.group.clone(),
            commission: 0.0,
            gross_pnl: None,
        };

        self.balance = next_balance;
        self.fills.push(recorded);
        self.close_events.push(event);
        self.trade_log.push(trade);
        if commission.amount != 0.0 {
            self.push_cost_event(
                id,
                &cost_symbol,
                cost_side,
                fill.ts,
                CostKind::ExitCommission,
                &commission,
                close_size,
                None,
            );
        }

        if full_close {
            let account = self.accounts.remove(id).expect("completed account exists");
            let final_net_pnl = account.realized_pnl;
            let average_entry = account.historical_average_entry();
            let mut completed = CompletedPosition::from_close_events(
                account.position_id,
                account.symbol,
                account.side,
                account.open_ts,
                fill.ts,
                account.entry_size,
                average_entry,
                account.initial_stop,
                account.effective_stop,
                account.risk_tranches,
                account.close_events,
                None,
                None,
                self.pnl_epsilon,
            );
            completed.group = account.group;
            completed.trade_id = account.trade_id;
            completed.charge_position_costs(account.entry_commission_total, account.swap_total);
            let completed_position_index = self.completed_positions.len();
            self.completed_positions.push(completed);
            portfolio_batch.finish_campaign(id.to_owned(), final_net_pnl, completed_position_index);
        }
        Ok(())
    }

    fn account_risk_tranche(
        &self,
        symbol: &str,
        operation_ts: NaiveDateTime,
        mut tranche: RiskTranche,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<RiskTranche, FutureExecutorError> {
        let Some(plan) = self.currency_plan.as_ref() else {
            return Ok(tranche);
        };
        let native_currency = plan
            .pnl_currency_for_primary_symbol(symbol)
            .ok_or_else(|| FutureExecutorError::MissingCurrencyRoute(symbol.to_owned()))?;
        tranche.native_currency = Some(native_currency.to_owned());
        if tranche.status != RiskBasisStatus::Available {
            return Ok(tranche);
        }
        let Some(native_risk) = tranche.native_risk_amount else {
            return Ok(tranche);
        };
        let accounted =
            self.convert_native_amount(symbol, -native_risk, operation_ts, conversion_quotes)?;
        let account_risk = -accounted.amount;
        if !account_risk.is_finite() || account_risk < 0.0 {
            return Err(FutureExecutorError::InvalidConvertedAmount {
                symbol: symbol.to_owned(),
                kind: "risk",
                amount: account_risk,
            });
        }
        tranche.risk_amount = Some(account_risk);
        tranche.risk_conversion = accounted.conversion;
        Ok(tranche)
    }

    fn convert_native_amount(
        &self,
        symbol: &str,
        amount: f64,
        operation_ts: NaiveDateTime,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<AccountedAmount, FutureExecutorError> {
        let Some(plan) = self.currency_plan.as_ref() else {
            return Ok(AccountedAmount {
                amount,
                native_currency: None,
                conversion: None,
            });
        };
        let native_currency = plan
            .pnl_currency_for_primary_symbol(symbol)
            .ok_or_else(|| FutureExecutorError::MissingCurrencyRoute(symbol.to_owned()))?;
        let route = plan
            .route_for_primary_symbol(symbol)
            .ok_or_else(|| FutureExecutorError::MissingCurrencyRoute(symbol.to_owned()))?;
        let conversion = match conversion_quotes {
            Some(quotes) => quotes.convert_route(amount, operation_ts, route),
            None => match route {
                ConversionRoute::Identity { .. } => Ok(ConversionResult {
                    from_currency: route.from_currency().to_owned(),
                    to_currency: route.to_currency().to_owned(),
                    input_amount: amount,
                    output_amount: amount,
                    operation_ts,
                    route: route.clone(),
                    legs: Vec::new(),
                }),
                _ => Err(ConversionError::NoCausalQuote {
                    symbol: route.symbols().next().unwrap_or(symbol).to_owned(),
                    operation_ts,
                    next_quote_ts: None,
                }),
            },
        }
        .map_err(|source| FutureExecutorError::Conversion {
            symbol: symbol.to_owned(),
            operation_ts,
            source,
        })?;
        if !conversion.output_amount.is_finite() {
            return Err(FutureExecutorError::InvalidConvertedAmount {
                symbol: symbol.to_owned(),
                kind: "P&L",
                amount: conversion.output_amount,
            });
        }
        Ok(AccountedAmount {
            amount: conversion.output_amount,
            native_currency: Some(native_currency.to_owned()),
            conversion: Some(conversion),
        })
    }

    fn contract_size(&self, symbol: &str) -> f64 {
        self.contract_sizes.get(symbol).copied().unwrap_or(1.0)
    }

    fn point_size(&self, symbol: &str) -> f64 {
        self.point_sizes.get(symbol).copied().unwrap_or(f64::NAN)
    }

    /// Convert one computed charge into the account currency without touching the ledger.
    fn resolve_cost(
        &self,
        symbol: &str,
        operation_ts: NaiveDateTime,
        charge: CostCharge,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<ResolvedCost, FutureExecutorError> {
        let resolved = match charge.basis {
            CostBasis::AccountCurrency => ResolvedCost {
                amount: charge.amount,
                ..ResolvedCost::default()
            },
            CostBasis::InstrumentNative => {
                let accounted = self.convert_native_amount(
                    symbol,
                    charge.amount,
                    operation_ts,
                    conversion_quotes,
                )?;
                ResolvedCost {
                    amount: accounted.amount,
                    native_amount: Some(charge.amount),
                    native_currency: accounted.native_currency,
                    conversion: accounted.conversion,
                }
            }
        };
        if !resolved.amount.is_finite() {
            return Err(FutureExecutorError::InvalidConvertedAmount {
                symbol: symbol.to_owned(),
                kind: "cost",
                amount: resolved.amount,
            });
        }
        Ok(resolved)
    }

    /// Commission for one fill, already converted into the account currency.
    fn resolve_commission(
        &self,
        symbol: &str,
        side: Side,
        size: f64,
        fill_price: f64,
        operation_ts: NaiveDateTime,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<ResolvedCost, FutureExecutorError> {
        let Some(costs) = self.costs.get(symbol) else {
            return Ok(ResolvedCost::default());
        };
        let contract_size = self.contract_size(symbol);
        let Some(charge) = costs.commission_for_fill(side, size, fill_price, contract_size) else {
            return Ok(ResolvedCost::default());
        };
        self.resolve_cost(symbol, operation_ts, charge, conversion_quotes)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_cost_event(
        &mut self,
        position_id: &str,
        symbol: &str,
        side: Side,
        ts: NaiveDateTime,
        kind: CostKind,
        resolved: &ResolvedCost,
        size: f64,
        nights: Option<u32>,
    ) {
        let mut event = CostEvent::new(
            position_id,
            self.cost_sequence,
            symbol,
            side,
            ts,
            kind,
            resolved.amount,
            size,
        );
        event.native_amount = resolved.native_amount;
        event.native_currency = resolved.native_currency.clone();
        event.conversion = resolved.conversion.clone();
        event.nights = nights;
        self.cost_sequence += 1;
        self.cost_events.push(event);
    }

    /// Charge overnight swap for every rollover instant in `(previous, current]`.
    ///
    /// The runner calls this once per accepted timestamp batch after all fills at that timestamp are recorded, so a position opened and closed inside one batch is never charged, and a weekend gap still charges every instant it skipped over.
    ///
    /// Returns the number of charges that could not be converted into the account currency and were therefore skipped. A skipped charge is never silent: the caller reports the count in the run's execution metadata.
    pub(crate) fn charge_rollovers(
        &mut self,
        current: NaiveDateTime,
        portfolio: &mut PortfolioRecorder,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> u64 {
        let previous = self.last_rollover_ts.replace(current);
        if self.costs.is_empty() {
            return 0;
        }
        let open_ids: Vec<String> = self
            .accounts
            .iter()
            .filter(|(_, account)| {
                account.remaining_size > position_size_tolerance(account.entry_size)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if open_ids.is_empty() {
            return 0;
        }
        let mut charged = false;
        let mut skipped = 0;
        for id in open_ids {
            let Some(account) = self.accounts.get(&id) else {
                continue;
            };
            let symbol = account.symbol.clone();
            let side = account.side;
            let size = account.remaining_size;
            let open_ts = account.open_ts;
            let Some(costs) = self.costs.get(&symbol) else {
                continue;
            };
            let Some(schedule) = costs.swap.as_ref() else {
                continue;
            };
            let rollover = schedule.rollover;
            let start = match previous {
                Some(previous) => previous.max(open_ts),
                None => open_ts,
            };
            let instants = qs_core::costs::rollover_instants(Some(start), current, rollover);
            for instant in instants {
                let Some(costs) = self.costs.get(&symbol) else {
                    continue;
                };
                let Some(schedule) = costs.swap.as_ref() else {
                    continue;
                };
                let nights = schedule.nights_at(instant);
                let contract_size = self.contract_size(&symbol);
                let point_size = self.point_size(&symbol);
                let Some(charge) =
                    costs.swap_for_rollover(side, size, contract_size, point_size, nights)
                else {
                    continue;
                };
                let Ok(resolved) = self.resolve_cost(&symbol, instant, charge, conversion_quotes)
                else {
                    skipped += 1;
                    continue;
                };
                if resolved.amount == 0.0 {
                    continue;
                }
                let next_balance = self.balance - resolved.amount;
                if !next_balance.is_finite() {
                    skipped += 1;
                    continue;
                }
                self.balance = next_balance;
                if let Some(account) = self.accounts.get_mut(&id) {
                    account.realized_pnl -= resolved.amount;
                    account.swap_total += resolved.amount;
                }
                self.push_cost_event(
                    &id,
                    &symbol,
                    side,
                    instant,
                    CostKind::Swap,
                    &resolved,
                    size,
                    Some(nights),
                );
                charged = true;
            }
        }
        if charged {
            portfolio.set_realized_pnl(self.realized_pnl());
        }
        skipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};
    use qs_core::types::{
        Action, ExecutionFill, FillPurpose, OrderType, PositionStatus, TargetSpec,
    };

    use crate::currency::{ConversionPriceSide, FxPair};

    fn ts() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
    }

    fn quote_at(seconds: i64, price: f64) -> PriceQuote {
        quote_for("EURUSD", seconds, price, price)
    }

    fn quote_for(symbol: &str, seconds: i64, bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: symbol.into(),
            ts: ts() + Duration::seconds(seconds),
            bid,
            ask,
        }
    }

    fn eur_account_plan() -> RunCurrencyPlan {
        RunCurrencyPlan::new(
            "USD",
            ["PRIMARY".to_owned()].into_iter().collect(),
            ["EURUSD".to_owned()].into_iter().collect(),
            [("PRIMARY".to_owned(), "EUR".to_owned())]
                .into_iter()
                .collect(),
            [(
                "EUR".to_owned(),
                ConversionRoute::Direct {
                    pair: FxPair {
                        symbol: "EURUSD".to_owned(),
                        base_currency: "EUR".to_owned(),
                        quote_currency: "USD".to_owned(),
                    },
                },
            )]
            .into_iter()
            .collect(),
            Vec::new(),
        )
        .unwrap()
    }

    fn execution(purpose: FillPurpose, price: f64) -> ExecutionFill {
        ExecutionFill {
            purpose,
            side: Side::Buy,
            price,
            quote_price: price,
            requested_price: None,
            slippage_pips: 0.0,
        }
    }

    #[test]
    fn close_and_initial_risk_use_signed_account_conversion() {
        let plan = eur_account_plan();
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9)
            .with_currency_plan(Some(plan.clone()));
        let mut portfolio =
            PortfolioRecorder::new(10_000.0, HashMap::new()).with_currency_plan(Some(plan));
        let mut conversions = ConversionQuoteBook::new(Duration::hours(1)).unwrap();
        conversions
            .record_canonical_tick(quote_for("EURUSD", 0, 2.0, 3.0))
            .unwrap();
        conversions
            .record_canonical_tick(quote_for("EURUSD", 1, 2.0, 3.0))
            .unwrap();

        let open_quote = quote_for("PRIMARY", 0, 100.0, 100.0);
        let effects = engine
            .apply_priced_future_action(
                Action::Open {
                    symbol: "PRIMARY".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: None,
                    size: 1.0,
                    stoploss: Some(90.0),
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                &open_quote,
                execution(FillPurpose::MarketEntry, 100.0),
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };
        executor
            .process_future_effects_with_currency(
                &effects,
                &engine,
                &open_quote,
                Some("open"),
                Some(open_quote.ts),
                open_quote.ts,
                &mut portfolio,
                Some(&conversions),
            )
            .unwrap();

        let close_quote = quote_for("PRIMARY", 1, 110.0, 110.0);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePosition { position_id: id },
                &close_quote,
                execution(FillPurpose::MarketExit, 110.0),
            )
            .unwrap();
        executor
            .process_future_effects_with_currency(
                &effects,
                &engine,
                &close_quote,
                Some("close"),
                Some(close_quote.ts),
                close_quote.ts,
                &mut portfolio,
                Some(&conversions),
            )
            .unwrap();

        assert_eq!(executor.realized_pnl(), 20.0);
        assert_eq!(executor.trade_log[0].pnl, 20.0);
        let close = &executor.close_events[0];
        assert_eq!(close.native_pnl, Some(10.0));
        assert_eq!(close.native_currency.as_deref(), Some("EUR"));
        let pnl_conversion = close.pnl_conversion.as_ref().unwrap();
        assert_eq!(pnl_conversion.input_amount, 10.0);
        assert_eq!(pnl_conversion.output_amount, 20.0);
        assert_eq!(pnl_conversion.legs[0].price_side, ConversionPriceSide::Bid);

        let risk = &executor.completed_positions[0].risk_tranches[0];
        assert_eq!(risk.native_risk_amount, Some(10.0));
        assert_eq!(risk.risk_amount, Some(30.0));
        let risk_conversion = risk.risk_conversion.as_ref().unwrap();
        assert_eq!(risk_conversion.input_amount, -10.0);
        assert_eq!(risk_conversion.output_amount, -30.0);
        assert_eq!(risk_conversion.legs[0].price_side, ConversionPriceSide::Ask);
        assert_eq!(executor.completed_positions[0].realized_r, Some(2.0 / 3.0));
    }

    #[test]
    fn stale_close_conversion_commits_no_accounting_artifacts() {
        let plan = eur_account_plan();
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9)
            .with_currency_plan(Some(plan.clone()));
        let mut portfolio =
            PortfolioRecorder::new(10_000.0, HashMap::new()).with_currency_plan(Some(plan));
        let mut conversions = ConversionQuoteBook::new(Duration::zero()).unwrap();
        conversions
            .record_canonical_tick(quote_for("EURUSD", 0, 2.0, 3.0))
            .unwrap();

        let open_quote = quote_for("PRIMARY", 0, 100.0, 100.0);
        let effects = engine
            .apply_priced_future_action(
                Action::Open {
                    symbol: "PRIMARY".into(),
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
                &open_quote,
                execution(FillPurpose::MarketEntry, 100.0),
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };
        executor
            .process_future_effects_with_currency(
                &effects,
                &engine,
                &open_quote,
                None,
                None,
                open_quote.ts,
                &mut portfolio,
                Some(&conversions),
            )
            .unwrap();
        portfolio.record_quote(open_quote.clone());
        portfolio.record_with_currency(
            open_quote.ts,
            executor.open_snapshots(),
            Some(&conversions),
        );
        assert!(portfolio.campaign_excursion(&id).is_some());

        let close_quote = quote_for("PRIMARY", 1, 110.0, 110.0);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                &close_quote,
                execution(FillPurpose::MarketExit, 110.0),
            )
            .unwrap();
        let error = executor
            .process_future_effects_with_currency(
                &effects,
                &engine,
                &close_quote,
                None,
                None,
                close_quote.ts,
                &mut portfolio,
                Some(&conversions),
            )
            .unwrap_err();

        assert!(matches!(error, FutureExecutorError::Conversion { .. }));
        assert_eq!(executor.balance(), 10_000.0);
        assert_eq!(executor.fills.len(), 1);
        assert!(executor.close_events.is_empty());
        assert!(executor.completed_positions.is_empty());
        assert_eq!(portfolio.realized_pnl(), 0.0);
        assert!(portfolio.campaign_excursion(&id).is_some());
    }

    #[test]
    fn partial_close_scale_in_and_final_close_use_remaining_average_cost() {
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9);
        let mut portfolio = PortfolioRecorder::new(10_000.0, HashMap::new());

        let open_quote = quote_at(0, 100.0);
        let effects = engine
            .apply_priced_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: None,
                    size: 2.0,
                    stoploss: None,
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                &open_quote,
                execution(FillPurpose::MarketEntry, 100.0),
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };
        executor
            .process_future_effects(
                &effects,
                &engine,
                &open_quote,
                Some("open"),
                Some(open_quote.ts),
                open_quote.ts,
                &mut portfolio,
            )
            .unwrap();

        let partial_quote = quote_at(1, 110.0);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePartial {
                    position_id: id.clone(),
                    ratio: 0.5,
                },
                &partial_quote,
                execution(FillPurpose::MarketExit, 110.0),
            )
            .unwrap();
        executor
            .process_future_effects(
                &effects,
                &engine,
                &partial_quote,
                Some("partial"),
                Some(partial_quote.ts),
                partial_quote.ts,
                &mut portfolio,
            )
            .unwrap();

        let scale_quote = quote_at(2, 120.0);
        let effects = engine
            .apply_priced_future_action(
                Action::ScaleIn {
                    position_id: id.clone(),
                    price: None,
                    size: 1.0,
                    trade_id: None,
                },
                &scale_quote,
                execution(FillPurpose::MarketEntry, 120.0),
            )
            .unwrap();
        executor
            .process_future_effects(
                &effects,
                &engine,
                &scale_quote,
                Some("scale"),
                Some(scale_quote.ts),
                scale_quote.ts,
                &mut portfolio,
            )
            .unwrap();
        assert_eq!(executor.open_snapshots()[0].average_entry_price, 110.0);

        let final_quote = quote_at(3, 130.0);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                &final_quote,
                execution(FillPurpose::MarketExit, 130.0),
            )
            .unwrap();
        executor
            .process_future_effects(
                &effects,
                &engine,
                &final_quote,
                Some("close"),
                Some(final_quote.ts),
                final_quote.ts,
                &mut portfolio,
            )
            .unwrap();

        assert!(executor.open_snapshots().is_empty());
        assert_eq!(executor.close_events.len(), 2);
        assert_eq!(executor.close_events[0].entry_price, Some(100.0));
        assert_eq!(executor.close_events[0].pnl, 10.0);
        assert_eq!(executor.close_events[1].entry_price, Some(110.0));
        assert_eq!(executor.close_events[1].pnl, 40.0);
        assert_eq!(executor.realized_pnl(), 50.0);
        assert_eq!(executor.trade_log[1].entry_price, 110.0);
    }

    #[test]
    fn portfolio_rejection_does_not_commit_executor_close_state() {
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9);
        let mut portfolio = PortfolioRecorder::new(10_000.0, HashMap::new());
        let open_quote = quote_at(0, 100.0);
        let effects = engine
            .apply_priced_future_action(
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
                &open_quote,
                execution(FillPurpose::MarketEntry, 100.0),
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };
        executor
            .process_future_effects(
                &effects,
                &engine,
                &open_quote,
                None,
                None,
                open_quote.ts,
                &mut portfolio,
            )
            .unwrap();
        assert!(portfolio.set_realized_pnl(f64::MAX));
        let balance_before = executor.balance();
        let fills_before = executor.fills.len();

        let close_quote = quote_at(1, 1.0e308);
        let effects = engine
            .apply_priced_future_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                &close_quote,
                execution(FillPurpose::MarketExit, 1.0e308),
            )
            .unwrap();
        let result = executor.process_future_effects(
            &effects,
            &engine,
            &close_quote,
            None,
            None,
            close_quote.ts,
            &mut portfolio,
        );

        assert!(matches!(
            result,
            Err(FutureExecutorError::PortfolioRejectedRealizedPnl { .. })
        ));
        assert_eq!(executor.balance(), balance_before);
        assert_eq!(executor.fills.len(), fills_before);
        assert!(executor.accounts.contains_key(&id));
        assert!(executor.close_events.is_empty());
    }

    #[test]
    fn failed_pending_batch_restores_lifecycle_and_deterministic_sequence() {
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let placement = engine
            .apply_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Limit,
                    price: Some(99.0),
                    size: 1.0,
                    stoploss: Some(95.0),
                    targets: vec![],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                ts(),
            )
            .unwrap();
        let id = effect_position_id(placement[0].effect()).to_owned();
        let mut batch = placement.clone();
        batch.push(FutureEffect::plain(Effect::StoplossModified {
            id: id.clone(),
            old_price: 95.0,
            new_price: f64::NAN,
        }));
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9);
        let mut portfolio = PortfolioRecorder::new(10_000.0, HashMap::new());
        let quote = quote_at(0, 100.0);

        let result = executor.process_future_effects(
            &batch,
            &engine,
            &quote,
            Some("pending"),
            Some(ts()),
            ts(),
            &mut portfolio,
        );

        assert!(matches!(
            result,
            Err(FutureExecutorError::InvalidFill { .. })
        ));
        assert!(executor.pending_origins.is_empty());
        assert!(executor.pending_order_lifecycle.is_empty());
        assert_eq!(executor.pending_lifecycle_sequence, 0);
        executor
            .process_future_effects(
                &placement,
                &engine,
                &quote,
                Some("pending"),
                Some(ts()),
                ts(),
                &mut portfolio,
            )
            .unwrap();
        assert_eq!(executor.pending_order_lifecycle[0].sequence, 0);
        assert_eq!(
            executor.pending_order_lifecycle[0].id,
            deterministic_event_id(&id, "pending_placed", 0)
        );
    }

    #[test]
    fn failed_close_batch_restores_executor_and_leaves_portfolio_unchanged() {
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9);
        let mut portfolio = PortfolioRecorder::new(10_000.0, HashMap::new());
        let open_quote = quote_at(0, 100.0);
        let open_effects = engine
            .apply_priced_future_action(
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
                &open_quote,
                execution(FillPurpose::MarketEntry, 100.0),
            )
            .unwrap();
        let id = effect_position_id(open_effects[0].effect()).to_owned();
        executor
            .process_future_effects(
                &open_effects,
                &engine,
                &open_quote,
                None,
                None,
                open_quote.ts,
                &mut portfolio,
            )
            .unwrap();
        portfolio.record_quote(open_quote.clone());
        portfolio.record(open_quote.ts, executor.open_snapshots());
        let campaign_before = portfolio.campaign_excursion(&id);

        let close_quote = quote_at(1, 110.0);
        let close_effects = engine
            .apply_priced_future_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                &close_quote,
                execution(FillPurpose::MarketExit, 110.0),
            )
            .unwrap();
        let mut batch = close_effects.clone();
        batch.push(FutureEffect::plain(Effect::StoplossRemoved {
            id: "missing".into(),
            old_price: 1.0,
        }));
        let result = executor.process_future_effects(
            &batch,
            &engine,
            &close_quote,
            None,
            None,
            close_quote.ts,
            &mut portfolio,
        );

        assert!(matches!(result, Err(FutureExecutorError::AccountNotFound(id)) if id == "missing"));
        assert_eq!(executor.balance(), 10_000.0);
        assert_eq!(executor.fills.len(), 1);
        assert!(executor.close_events.is_empty());
        assert!(executor.completed_positions.is_empty());
        assert!(executor.trade_log.is_empty());
        assert!(executor.accounts.contains_key(&id));
        assert_eq!(executor.fill_sequence, 1);
        assert_eq!(executor.close_sequence, 0);
        assert_eq!(portfolio.realized_pnl(), 0.0);
        assert_eq!(portfolio.campaign_excursion(&id), campaign_before);

        executor
            .process_future_effects(
                &close_effects,
                &engine,
                &close_quote,
                None,
                None,
                close_quote.ts,
                &mut portfolio,
            )
            .unwrap();
        assert_eq!(executor.fills[1].id, deterministic_event_id(&id, "fill", 1));
        assert_eq!(
            executor.close_events[0].id,
            deterministic_event_id(&id, "close", 0)
        );
        assert_eq!(portfolio.realized_pnl(), 10.0);
        assert!(portfolio.campaign_excursion(&id).is_none());
    }

    #[test]
    fn carried_fill_is_consumed_without_repricing_or_engine_synchronization() {
        let quote = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(),
            bid: 99.0,
            ask: 100.0,
        };
        let execution = ExecutionFill {
            purpose: FillPurpose::MarketEntry,
            side: Side::Buy,
            price: 123.456,
            quote_price: 100.0,
            requested_price: None,
            slippage_pips: 0.0,
        };
        let mut engine =
            TradeEngine::with_fill_model_and_deterministic_ids(qs_core::types::FillModel::BidAsk);
        let effects = engine
            .apply_priced_future_action(
                Action::Open {
                    symbol: "EURUSD".into(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(1.0),
                    size: 2.0,
                    stoploss: None,
                    targets: Vec::<TargetSpec>::new(),
                    rules: vec![],
                    group: None,
                    trade_id: None,
                },
                &quote,
                execution,
            )
            .unwrap();
        let id = match effects[0].effect() {
            Effect::PositionOpened { id } => id.clone(),
            effect => panic!("unexpected effect: {effect:?}"),
        };

        let mut executor = FutureExecutor::new(10_000.0, HashMap::new(), 1.0e-9);
        let mut portfolio = PortfolioRecorder::new(10_000.0, HashMap::new());
        executor
            .process_future_effects(
                &effects,
                &engine,
                &quote,
                Some("open"),
                Some(ts()),
                ts(),
                &mut portfolio,
            )
            .unwrap();

        assert_eq!(executor.fills.len(), 1);
        assert_eq!(executor.fills[0].fill, execution);
        assert_eq!(executor.fills[0].size, 2.0);
        let position = engine.get_position(&id).unwrap();
        assert_eq!(position.data.status, PositionStatus::Open);
        assert_eq!(position.data.entries[0].price, execution.price);
        assert_eq!(position.data.entries[0].ts, quote.ts);
    }
}
