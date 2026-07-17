//! Position — the atomic unit of market exposure.
//!
//! A `Position` represents a single directional exposure on a single symbol.
//! It can be filled in one shot or scaled into over time (multiple [`Fill`]s).
//! Management rules are stored alongside the position data and evaluated on
//! every price tick by the engine.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::rules::{PositionView, Rule};
use crate::types::{
    CloseReason, Effect, Fill, FillModel, FillPurpose, FutureIntent, GroupId, OrderType,
    PositionId, PositionRecord, PositionStatus, PriceQuote, Side, StopOrigin, TradeId,
    position_size_tolerance,
};

/// Core position data — the pure state without rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "PositionDataSerde")]
pub struct PositionData {
    /// Unique identifier.
    pub id: PositionId,

    /// Instrument symbol (e.g. "EURUSD", "XAUUSD").
    pub symbol: String,

    /// Trade direction.
    pub side: Side,

    /// How the order was placed.
    pub order_type: OrderType,

    /// Current lifecycle status.
    pub status: PositionStatus,

    /// For Limit/Stop orders: the price at which the order should fill.
    pub pending_price: Option<f64>,

    /// Intended order size (lots / units).
    pub size: f64,

    /// Actual execution fills (one for market, potentially many for scale-in).
    pub entries: Vec<Fill>,

    /// Fraction of all entered size still open (1.0 = full, 0.0 = closed).
    ///
    /// Retained for wire compatibility and rule views. Absolute sizes are the
    /// source of truth and every core mutation keeps this value synchronized.
    pub remaining_ratio: f64,

    /// Absolute size closed across all partial and full exits.
    ///
    /// Entry size is the sum of `entries`; open size is derived as entered size
    /// minus this value. Older serialized positions infer this from
    /// `remaining_ratio` during deserialization.
    pub closed_size: f64,

    /// Cost basis assigned to the inventory that is still open.
    ///
    /// Positions use average-cost accounting: every close releases
    /// `average_entry * close_size` from this value, while a scale-in adds only
    /// the new fill's value. Historical `entries` remain unchanged for audit.
    /// Older serialized positions infer this from their historical weighted
    /// average and remaining size.
    pub open_entry_value: f64,

    /// Number of take-profit levels that have been hit.  Used by
    /// `BreakevenAfterTargets` rule.
    pub target_hits: u32,

    /// When the position first filled.
    pub open_ts: Option<NaiveDateTime>,

    /// When the position was fully closed.
    pub close_ts: Option<NaiveDateTime>,

    /// Optional group for per-signal-source tracking and group-level actions.
    #[serde(default)]
    pub group: Option<GroupId>,

    /// Optional application-defined trade identity.
    ///
    /// Parsers mint a stable `TradeId` (for example, `chat_id:msg_id`) and
    /// reference it from later management signals via `PositionRef::ByTradeId`.
    /// When `None`, management signals must use bulk references or the
    /// engine's `PositionId`.
    #[serde(default)]
    pub trade_id: Option<TradeId>,

    /// Provenance of the current fixed protective stop.
    #[serde(default)]
    pub stop_origin: Option<crate::types::StopOrigin>,

    /// Immutable audit trail.
    pub records: Vec<(PositionRecord, NaiveDateTime)>,
}

/// A position: data + composable management rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub data: PositionData,
    pub rules: Vec<Rule>,
}

#[derive(Deserialize)]
struct PositionDataSerde {
    id: PositionId,
    symbol: String,
    side: Side,
    order_type: OrderType,
    status: PositionStatus,
    pending_price: Option<f64>,
    size: f64,
    entries: Vec<Fill>,
    remaining_ratio: f64,
    #[serde(default)]
    closed_size: Option<f64>,
    #[serde(default)]
    open_entry_value: Option<f64>,
    target_hits: u32,
    open_ts: Option<NaiveDateTime>,
    close_ts: Option<NaiveDateTime>,
    #[serde(default)]
    group: Option<GroupId>,
    #[serde(default)]
    trade_id: Option<TradeId>,
    #[serde(default)]
    stop_origin: Option<StopOrigin>,
    records: Vec<(PositionRecord, NaiveDateTime)>,
}

impl From<PositionDataSerde> for PositionData {
    fn from(value: PositionDataSerde) -> Self {
        let entered_size: f64 = value.entries.iter().map(|fill| fill.size).sum();
        let inferred_closed_size = entered_size * (1.0 - value.remaining_ratio.clamp(0.0, 1.0));
        let closed_size = value
            .closed_size
            .unwrap_or(inferred_closed_size)
            .max(0.0)
            .min(entered_size.max(0.0));
        let remaining_size = (entered_size - closed_size).max(0.0);
        let remaining_ratio = if entered_size > 0.0 {
            remaining_size / entered_size
        } else {
            value.remaining_ratio
        };
        let historical_entry_value: f64 = value
            .entries
            .iter()
            .map(|fill| fill.price * fill.size)
            .sum();
        let inferred_open_entry_value = if entered_size > 0.0 {
            historical_entry_value * (remaining_size / entered_size)
        } else {
            0.0
        };
        let open_entry_value = if remaining_size <= position_size_tolerance(entered_size) {
            0.0
        } else {
            value
                .open_entry_value
                .filter(|basis| basis.is_finite() && *basis >= 0.0)
                .unwrap_or(inferred_open_entry_value)
        };

        Self {
            id: value.id,
            symbol: value.symbol,
            side: value.side,
            order_type: value.order_type,
            status: value.status,
            pending_price: value.pending_price,
            size: value.size,
            entries: value.entries,
            remaining_ratio,
            closed_size,
            open_entry_value,
            target_hits: value.target_hits,
            open_ts: value.open_ts,
            close_ts: value.close_ts,
            group: value.group,
            trade_id: value.trade_id,
            stop_origin: value.stop_origin,
            records: value.records,
        }
    }
}

// ─── PositionData helpers ───────────────────────────────────────────────────

impl PositionData {
    /// Average-cost entry price of the inventory that is still open.
    ///
    /// Historical fills are intentionally not re-averaged here: after a
    /// partial close, only the remaining inventory basis participates in a
    /// later scale-in and subsequent close.
    pub fn average_entry(&self) -> f64 {
        let remaining_size = self.remaining_size();
        if remaining_size == 0.0 {
            0.0
        } else {
            self.open_entry_value / remaining_size
        }
    }

    /// Volume-weighted average across all historical entry fills.
    pub fn historical_average_entry(&self) -> f64 {
        let total_size = self.total_filled_size();
        if total_size == 0.0 {
            0.0
        } else {
            self.entries
                .iter()
                .map(|fill| fill.price * fill.size)
                .sum::<f64>()
                / total_size
        }
    }

    /// Total filled size (sum of all fills).
    pub fn total_filled_size(&self) -> f64 {
        self.entries.iter().map(|f| f.size).sum()
    }

    /// Size still active in the market, derived from absolute quantities.
    pub fn remaining_size(&self) -> f64 {
        let entered_size = self.total_filled_size();
        let remaining = (entered_size - self.closed_size).max(0.0);
        if remaining <= position_size_tolerance(entered_size) {
            0.0
        } else {
            remaining
        }
    }

    /// Fraction of all entered size that is still open.
    pub fn open_ratio(&self) -> f64 {
        let entered_size = self.total_filled_size();
        if entered_size <= 0.0 {
            return 0.0;
        }
        self.remaining_size() / entered_size
    }

    /// Cap an original-entered-size close ratio to the exposure still open.
    pub fn capped_close_ratio(&self, ratio: f64) -> f64 {
        if !ratio.is_finite() || ratio <= 0.0 {
            return 0.0;
        }
        ratio.min(self.open_ratio())
    }

    /// Absolute size represented by a close ratio, capped to open exposure.
    pub fn close_size_for_ratio(&self, ratio: f64) -> f64 {
        let actual_ratio = self.capped_close_ratio(ratio);
        (self.total_filled_size() * actual_ratio).min(self.remaining_size())
    }

    fn sync_remaining_ratio(&mut self) {
        let entered_size = self.total_filled_size().max(0.0);
        self.closed_size = self.closed_size.max(0.0).min(entered_size);
        self.remaining_ratio = if entered_size > 0.0 {
            self.remaining_size() / entered_size
        } else if self.status == PositionStatus::Pending {
            1.0
        } else {
            0.0
        };
        if self.remaining_size() == 0.0 {
            self.open_entry_value = 0.0;
        }
    }

    /// Unrealised P&L at the given price.
    pub fn unrealized_pnl(&self, current_price: f64) -> f64 {
        let entry = self.average_entry();
        let size = self.remaining_size();
        match self.side {
            Side::Buy => (current_price - entry) * size,
            Side::Sell => (entry - current_price) * size,
        }
    }

    /// Whether the position is live (Open) and has remaining size.
    pub fn is_active(&self) -> bool {
        self.status == PositionStatus::Open && self.remaining_size() > 0.0
    }

    /// Add a fill (scale-in), preserving previously closed absolute size and
    /// adding the fill only to active inventory cost basis.
    pub fn add_fill(&mut self, fill: Fill) {
        self.open_entry_value += fill.price * fill.size;
        self.entries.push(fill);
        self.sync_remaining_ratio();
    }

    /// Replace the most recent entry fill price and timestamp.
    ///
    /// Future-quote backtests use this after a pending order triggers so the
    /// engine's average entry matches the authoritative gap-aware execution
    /// fill produced by the execution pricer. Returns `false` when no fill
    /// exists or the replacement is invalid.
    pub fn replace_latest_fill_execution(&mut self, price: f64, ts: NaiveDateTime) -> bool {
        if !price.is_finite() || price <= 0.0 {
            return false;
        }
        let Some(fill) = self.entries.last_mut() else {
            return false;
        };
        self.open_entry_value += (price - fill.price) * fill.size;
        fill.price = price;
        fill.ts = ts;
        true
    }

    /// Replace the latest entry fill and its audit record.
    ///
    /// Future-quote executors use this after the engine transitions a pending
    /// order to `Open`, keeping core position state synchronized with the
    /// externally calculated gap/improvement execution price.
    pub fn synchronize_latest_fill(&mut self, fill: Fill) -> bool {
        let Some(latest) = self.entries.last_mut() else {
            return false;
        };
        self.open_entry_value += fill.price * fill.size - latest.price * latest.size;
        *latest = fill.clone();
        self.open_ts = Some(fill.ts);
        self.sync_remaining_ratio();
        if let Some((PositionRecord::Filled { fill: recorded }, _)) = self
            .records
            .iter_mut()
            .rev()
            .find(|(record, _)| matches!(record, PositionRecord::Filled { .. }))
        {
            *recorded = fill;
        }
        true
    }

    /// Record a partial close using an original-entered-size ratio.
    ///
    /// The close is capped to the absolute size still open. If no exposure
    /// remains, the status is flipped to `Closed` and both absolute and ratio
    /// accounting reach exact zero.
    pub fn apply_partial_close(
        &mut self,
        ratio: f64,
        price: f64,
        reason: CloseReason,
        ts: NaiveDateTime,
    ) {
        let actual_ratio = self.capped_close_ratio(ratio);
        let entered_size = self.total_filled_size();
        let open_size = self.remaining_size();
        let close_size = self.close_size_for_ratio(actual_ratio);
        let released_entry_value = self.average_entry() * close_size;
        if open_size - close_size <= position_size_tolerance(entered_size) {
            self.closed_size = entered_size;
            self.open_entry_value = 0.0;
        } else {
            self.closed_size = (self.closed_size + close_size).min(entered_size);
            self.open_entry_value = (self.open_entry_value - released_entry_value).max(0.0);
        }
        self.sync_remaining_ratio();
        if reason == CloseReason::Target {
            self.target_hits += 1;
        }
        self.records.push((
            PositionRecord::PartialClose {
                ratio: actual_ratio,
                price,
                reason,
            },
            ts,
        ));
        if self.remaining_size() == 0.0 {
            self.closed_size = entered_size;
            self.open_entry_value = 0.0;
            self.remaining_ratio = 0.0;
            self.status = PositionStatus::Closed;
            self.close_ts = Some(ts);
            self.records.push((PositionRecord::Closed { reason }, ts));
        }
    }

    /// Mark the position as fully closed.
    pub fn apply_full_close(&mut self, reason: CloseReason, ts: NaiveDateTime) {
        self.closed_size = self.total_filled_size();
        self.open_entry_value = 0.0;
        self.remaining_ratio = 0.0;
        self.status = PositionStatus::Closed;
        self.close_ts = Some(ts);
        if reason == CloseReason::Target {
            self.target_hits += 1;
        }
        self.records.push((PositionRecord::Closed { reason }, ts));
    }

    /// Create a read-only view for rule evaluation.
    pub fn view(&self) -> PositionView<'_> {
        PositionView {
            id: &self.id,
            symbol: &self.symbol,
            side: self.side,
            status: self.status,
            average_entry: self.average_entry(),
            remaining_ratio: self.open_ratio(),
            target_hits: self.target_hits,
            open_ts: self.open_ts,
        }
    }
}

// ─── Position constructors & methods ────────────────────────────────────────

impl Position {
    /// Create a new position that is immediately filled (Market order).
    pub fn new_market(
        id: PositionId,
        symbol: String,
        side: Side,
        fill: Fill,
        rules: Vec<Rule>,
    ) -> Self {
        let open_ts = fill.ts;
        let size = fill.size;
        let open_entry_value = fill.price * fill.size;
        Self {
            data: PositionData {
                id,
                symbol: symbol.clone(),
                side,
                order_type: OrderType::Market,
                status: PositionStatus::Open,
                pending_price: None,
                size,
                entries: vec![fill],
                remaining_ratio: 1.0,
                closed_size: 0.0,
                open_entry_value,
                target_hits: 0,
                open_ts: Some(open_ts),
                close_ts: None,
                group: None,
                trade_id: None,
                stop_origin: None,
                records: vec![(
                    PositionRecord::Created {
                        symbol,
                        side,
                        order_type: OrderType::Market,
                    },
                    open_ts,
                )],
            },
            rules,
        }
    }

    /// Create a pending position (Limit or Stop order).
    // Preserve the established public constructor shape for API compatibility.
    #[allow(clippy::too_many_arguments)]
    pub fn new_pending(
        id: PositionId,
        symbol: String,
        side: Side,
        order_type: OrderType,
        pending_price: f64,
        size: f64,
        ts: NaiveDateTime,
        rules: Vec<Rule>,
    ) -> Self {
        debug_assert!(
            order_type == OrderType::Limit || order_type == OrderType::Stop,
            "new_pending requires Limit or Stop order type"
        );
        Self {
            data: PositionData {
                id,
                symbol: symbol.clone(),
                side,
                order_type,
                status: PositionStatus::Pending,
                pending_price: Some(pending_price),
                size,
                entries: Vec::new(),
                remaining_ratio: 1.0,
                closed_size: 0.0,
                open_entry_value: 0.0,
                target_hits: 0,
                open_ts: None,
                close_ts: None,
                group: None,
                trade_id: None,
                stop_origin: None,
                records: vec![(
                    PositionRecord::Created {
                        symbol,
                        side,
                        order_type,
                    },
                    ts,
                )],
            },
            rules,
        }
    }

    /// Attach or replace a `trade_id` on this position.
    pub fn set_trade_id(&mut self, trade_id: Option<TradeId>) {
        self.data.trade_id = trade_id;
    }

    /// Return the execution purpose when this pending order is triggered by
    /// `quote`. This check is pure and is shared by Legacy and FutureQuote paths.
    pub fn pending_fill_purpose(
        &self,
        quote: &PriceQuote,
        model: FillModel,
    ) -> Option<FillPurpose> {
        if self.data.status != PositionStatus::Pending {
            return None;
        }
        let pending_price = self.data.pending_price?;
        let check = quote.fill_price(self.data.side, model);
        let triggered = match (self.data.order_type, self.data.side) {
            (OrderType::Limit, Side::Buy) => check <= pending_price,
            (OrderType::Limit, Side::Sell) => check >= pending_price,
            (OrderType::Stop, Side::Buy) => check >= pending_price,
            (OrderType::Stop, Side::Sell) => check <= pending_price,
            (OrderType::Market, _) => false,
        };
        if !triggered {
            return None;
        }
        match self.data.order_type {
            OrderType::Limit => Some(FillPurpose::LimitEntry),
            OrderType::Stop => Some(FillPurpose::StopEntry),
            OrderType::Market => None,
        }
    }

    /// Commit a previously priced pending fill.
    pub(crate) fn apply_pending_fill(&mut self, fill: Fill) -> bool {
        if self.data.status != PositionStatus::Pending {
            return false;
        }
        let ts = fill.ts;
        self.data.status = PositionStatus::Open;
        self.data.add_fill(fill.clone());
        self.data.open_ts = Some(ts);
        self.data
            .records
            .push((PositionRecord::Filled { fill }, ts));
        true
    }

    /// Check if a pending order should fill at the given quote.
    ///
    /// Returns `true` (and transitions the position to Open) if the fill
    /// condition is met. Legacy semantics retain the requested-price fill.
    pub fn try_fill(&mut self, quote: &PriceQuote, model: FillModel) -> bool {
        if self.pending_fill_purpose(quote, model).is_none() {
            return false;
        }
        let Some(pending_price) = self.data.pending_price else {
            return false;
        };
        self.apply_pending_fill(Fill {
            price: pending_price,
            size: self.data.size,
            ts: quote.ts,
        })
    }

    /// Evaluate all management rules against the current quote.
    ///
    /// Rules may mutate their own internal state (e.g. mark themselves as
    /// triggered), but the position data is only read, not written.
    /// The engine applies the returned effects to the position afterwards.
    pub fn evaluate_rules(&mut self, quote: &PriceQuote, model: FillModel) -> Vec<Effect> {
        if self.data.status != PositionStatus::Open {
            return vec![];
        }

        let view = self.data.view();
        let mut effects = Vec::new();

        for rule in &mut self.rules {
            let rule_effects = rule.evaluate(&view, quote, model);
            effects.extend(rule_effects);
        }

        effects
    }

    /// Deterministic FutureQuote rule arbitration.
    ///
    /// One authoritative protective stop is evaluated first, crossed targets
    /// are processed in economic order, terminal time exits follow, and
    /// breakeven transitions are emitted only for surviving exposure.
    pub(crate) fn evaluate_rules_future(
        &mut self,
        quote: &PriceQuote,
        model: FillModel,
    ) -> Vec<FutureIntent> {
        if self.data.status != PositionStatus::Open {
            return Vec::new();
        }
        let side = self.data.side;
        let check = quote.eval_price(side, model);
        let average_entry = self.data.average_entry();
        let current_stop = self
            .current_effective_stop()
            .map(|stop| (stop.price, stop.origin));
        let mut effective_stop = None;

        for rule in &mut self.rules {
            match rule {
                Rule::FixedStoploss { price } => {
                    let origin = self.data.stop_origin.unwrap_or(StopOrigin::Initial);
                    effective_stop = more_protective_stop(side, effective_stop, (*price, origin));
                }
                Rule::TrailingStop {
                    distance,
                    peak_price,
                    initialized,
                } => {
                    if !*initialized {
                        *peak_price = average_entry;
                        *initialized = true;
                    }
                    match side {
                        Side::Buy => *peak_price = peak_price.max(check),
                        Side::Sell => {
                            *peak_price = if *peak_price == 0.0 {
                                check
                            } else {
                                peak_price.min(check)
                            }
                        }
                    }
                    let candidate = match side {
                        Side::Buy => *peak_price - *distance,
                        Side::Sell => *peak_price + *distance,
                    };
                    effective_stop = more_protective_stop(
                        side,
                        effective_stop,
                        (candidate, StopOrigin::Trailing),
                    );
                }
                _ => {}
            }
        }

        if let Some((price, origin)) = effective_stop {
            let hit = match side {
                Side::Buy => check <= price,
                Side::Sell => check >= price,
            };
            if hit {
                let mut effects = Vec::new();
                if let Some(effect) =
                    stop_transition_effect(&self.data.id, current_stop, effective_stop)
                {
                    effects.push(effect);
                }
                let reason = match origin {
                    StopOrigin::Breakeven => CloseReason::BreakevenStop,
                    StopOrigin::Trailing => CloseReason::TrailingStop,
                    _ => CloseReason::Stoploss,
                };
                effects.push(FutureIntent {
                    effect: Effect::PositionClosed {
                        id: self.data.id.clone(),
                        reason,
                    },
                    requested_price: Some(price),
                    stop_origin: Some(origin),
                });
                return effects;
            }
        }

        let mut target_indices: Vec<(usize, f64, f64)> = self
            .rules
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| match rule {
                Rule::TakeProfit {
                    price,
                    close_ratio,
                    triggered: false,
                } if match side {
                    Side::Buy => check >= *price,
                    Side::Sell => check <= *price,
                } =>
                {
                    Some((index, *price, *close_ratio))
                }
                _ => None,
            })
            .collect();
        target_indices.sort_by(|left, right| match side {
            Side::Buy => left.1.total_cmp(&right.1),
            Side::Sell => right.1.total_cmp(&left.1),
        });

        let mut effects = Vec::new();
        let mut remaining = self.data.open_ratio();
        let mut target_hits = self.data.target_hits;
        for (index, price, ratio) in target_indices {
            if remaining <= position_size_tolerance(1.0) {
                break;
            }
            if let Rule::TakeProfit { triggered, .. } = &mut self.rules[index] {
                *triggered = true;
            }
            let actual = ratio.min(remaining).max(0.0);
            if actual <= position_size_tolerance(1.0) {
                continue;
            }
            target_hits += 1;
            remaining = (remaining - actual).max(0.0);
            let effect = if remaining <= position_size_tolerance(1.0) {
                Effect::PositionClosed {
                    id: self.data.id.clone(),
                    reason: CloseReason::Target,
                }
            } else {
                Effect::PartialClose {
                    id: self.data.id.clone(),
                    ratio: actual,
                    reason: CloseReason::Target,
                }
            };
            effects.push(FutureIntent {
                effect,
                requested_price: Some(price),
                stop_origin: None,
            });
            if remaining <= position_size_tolerance(1.0) {
                if let Some(effect) =
                    stop_transition_effect(&self.data.id, current_stop, effective_stop)
                {
                    effects.insert(effects.len() - 1, effect);
                }
                return effects;
            }
        }

        for rule in &self.rules {
            if let Rule::TimeExit { max_seconds } = rule
                && self
                    .data
                    .open_ts
                    .is_some_and(|open| (quote.ts - open).num_seconds() >= *max_seconds as i64)
            {
                if let Some(effect) =
                    stop_transition_effect(&self.data.id, current_stop, effective_stop)
                {
                    effects.push(effect);
                }
                effects.push(FutureIntent::plain(Effect::PositionClosed {
                    id: self.data.id.clone(),
                    reason: CloseReason::TimeExit,
                }));
                return effects;
            }
        }

        let mut breakeven_triggered = false;
        for rule in &mut self.rules {
            let trigger = match rule {
                Rule::BreakevenWhen {
                    trigger_price,
                    triggered,
                } if !*triggered => {
                    let hit = match side {
                        Side::Buy => check >= *trigger_price,
                        Side::Sell => check <= *trigger_price,
                    };
                    if hit {
                        *triggered = true;
                    }
                    hit
                }
                Rule::BreakevenAfterTargets { after_n, triggered } if !*triggered => {
                    let hit = target_hits >= *after_n;
                    if hit {
                        *triggered = true;
                    }
                    hit
                }
                _ => false,
            };
            if trigger {
                breakeven_triggered = true;
                break;
            }
        }
        if breakeven_triggered {
            effective_stop =
                more_protective_stop(side, effective_stop, (average_entry, StopOrigin::Breakeven));
        }
        if let Some(effect) = stop_transition_effect(&self.data.id, current_stop, effective_stop) {
            effects.push(effect);
        }
        effects
    }

    /// Find the current fixed-stoploss price, if any.
    pub fn current_effective_stop(&self) -> Option<crate::types::EffectiveStop> {
        self.current_stoploss()
            .map(|price| crate::types::EffectiveStop {
                price,
                origin: self
                    .data
                    .stop_origin
                    .unwrap_or(crate::types::StopOrigin::Initial),
            })
    }

    pub fn current_stoploss(&self) -> Option<f64> {
        for rule in &self.rules {
            if let Rule::FixedStoploss { price } = rule {
                return Some(*price);
            }
        }
        None
    }

    /// Update the fixed-stoploss price.  Returns the old price (if any).
    pub fn set_stoploss(&mut self, new_price: f64) -> Option<f64> {
        self.set_stoploss_with_origin(new_price, crate::types::StopOrigin::Modified)
    }

    pub fn set_stoploss_with_origin(
        &mut self,
        new_price: f64,
        origin: crate::types::StopOrigin,
    ) -> Option<f64> {
        self.data.stop_origin = Some(origin);
        for rule in &mut self.rules {
            if let Rule::FixedStoploss { price } = rule {
                let old = *price;
                *price = new_price;
                return Some(old);
            }
        }
        // No existing stoploss — add one.
        self.rules.push(Rule::fixed_stoploss(new_price));
        None
    }

    /// Remove a rule by name.  Returns `true` if a rule was removed.
    pub fn remove_rule(&mut self, name: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|r| r.name() != name);
        self.rules.len() < before
    }

    /// Evaluate only stateful rules (trailing stop, time exit, breakeven-after-targets).
    /// Used when static rules are handled by the alert register.
    pub fn evaluate_stateful_rules(&mut self, quote: &PriceQuote, model: FillModel) -> Vec<Effect> {
        if self.data.status != PositionStatus::Open {
            return vec![];
        }
        let view = self.data.view();
        let mut effects = Vec::new();
        for rule in &mut self.rules {
            if rule.is_stateful() {
                effects.extend(rule.evaluate(&view, quote, model));
            }
        }
        effects
    }

    /// Whether this position has any stateful rules requiring tick-by-tick evaluation.
    pub fn has_stateful_rules(&self) -> bool {
        self.rules.iter().any(|r| r.is_stateful())
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

fn stop_transition_effect(
    position_id: &str,
    current: Option<(f64, StopOrigin)>,
    next: Option<(f64, StopOrigin)>,
) -> Option<FutureIntent> {
    let (new_price, origin) = next?;
    if current == next {
        return None;
    }
    Some(FutureIntent {
        effect: Effect::StoplossModified {
            id: position_id.to_owned(),
            old_price: current.map_or(0.0, |stop| stop.0),
            new_price,
        },
        requested_price: Some(new_price),
        stop_origin: Some(origin),
    })
}

fn more_protective_stop(
    side: Side,
    current: Option<(f64, StopOrigin)>,
    candidate: (f64, StopOrigin),
) -> Option<(f64, StopOrigin)> {
    match current {
        None => Some(candidate),
        Some(existing) => match side {
            Side::Buy if candidate.0 > existing.0 => Some(candidate),
            Side::Sell if candidate.0 < existing.0 => Some(candidate),
            _ => Some(existing),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn make_fill(price: f64, size: f64) -> Fill {
        Fill {
            price,
            size,
            ts: ts(10, 0, 0),
        }
    }

    #[test]
    fn average_entry_single_fill() {
        let pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![],
        );
        assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn average_entry_multiple_fills() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0800, 1.0),
            vec![],
        );
        pos.data.add_fill(Fill {
            price: 1.0900,
            size: 1.0,
            ts: ts(10, 5, 0),
        });
        // (1.0800 * 1.0 + 1.0900 * 1.0) / 2.0 = 1.0850
        assert!((pos.data.average_entry() - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn average_entry_weighted() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0800, 2.0),
            vec![],
        );
        pos.data.add_fill(Fill {
            price: 1.0900,
            size: 1.0,
            ts: ts(10, 5, 0),
        });
        // (1.0800 * 2 + 1.0900 * 1) / 3 = 1.08333...
        let expected = (1.0800 * 2.0 + 1.0900 * 1.0) / 3.0;
        assert!((pos.data.average_entry() - expected).abs() < 1e-10);
    }

    #[test]
    fn remaining_size_after_partial_close() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 2.0),
            vec![],
        );
        assert!((pos.data.remaining_size() - 2.0).abs() < f64::EPSILON);

        pos.data
            .apply_partial_close(0.5, 1.0900, CloseReason::Target, ts(10, 30, 0));
        // remaining_ratio = 0.5, total_filled = 2.0, remaining = 1.0
        assert!((pos.data.remaining_size() - 1.0).abs() < f64::EPSILON);
        assert_eq!(pos.data.status, PositionStatus::Open);
        assert_eq!(pos.data.target_hits, 1);
    }

    #[test]
    fn partial_close_then_scale_in_conserves_absolute_size() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 2.0),
            vec![],
        );

        pos.data
            .apply_partial_close(0.5, 1.0900, CloseReason::Manual, ts(10, 30, 0));
        pos.data.add_fill(Fill {
            price: 1.0950,
            size: 1.0,
            ts: ts(10, 35, 0),
        });

        assert!((pos.data.total_filled_size() - 3.0).abs() < f64::EPSILON);
        assert!((pos.data.closed_size - 1.0).abs() < f64::EPSILON);
        assert!((pos.data.remaining_size() - 2.0).abs() < f64::EPSILON);
        assert!((pos.data.remaining_ratio - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn partial_close_then_scale_in_preserves_average_cost_cash_flow() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(100.0, 2.0),
            vec![],
        );

        let first_basis = pos.data.average_entry();
        pos.data
            .apply_partial_close(0.5, 110.0, CloseReason::Manual, ts(10, 30, 0));
        let first_pnl = (110.0 - first_basis) * 1.0;
        assert_eq!(pos.data.open_entry_value, 100.0);
        assert_eq!(pos.data.average_entry(), 100.0);

        pos.data.add_fill(Fill {
            price: 120.0,
            size: 1.0,
            ts: ts(10, 35, 0),
        });
        assert_eq!(pos.data.average_entry(), 110.0);
        assert_eq!(pos.data.open_entry_value, 220.0);

        let final_basis = pos.data.average_entry();
        let final_pnl = (130.0 - final_basis) * pos.data.remaining_size();
        pos.data
            .apply_full_close(CloseReason::Manual, ts(10, 40, 0));

        assert_eq!(first_pnl + final_pnl, 50.0);
        assert_eq!(pos.data.entries.len(), 2);
        assert_eq!(pos.data.historical_average_entry(), 320.0 / 3.0);
        assert_eq!(pos.data.open_entry_value, 0.0);
    }

    #[test]
    fn scale_in_then_partial_close_uses_all_entered_size() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 2.0),
            vec![],
        );
        pos.data.add_fill(Fill {
            price: 1.0950,
            size: 1.0,
            ts: ts(10, 5, 0),
        });
        pos.data
            .apply_partial_close(0.5, 1.1000, CloseReason::Manual, ts(10, 30, 0));

        assert!((pos.data.closed_size - 1.5).abs() < f64::EPSILON);
        assert!((pos.data.remaining_size() - 1.5).abs() < f64::EPSILON);
        assert!((pos.data.remaining_ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn repeated_partial_closes_cap_and_reach_exact_zero() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![],
        );

        for minute in [10, 20, 30] {
            pos.data
                .apply_partial_close(0.4, 1.0900, CloseReason::Manual, ts(10, minute, 0));
        }

        assert_eq!(pos.data.closed_size, 1.0);
        assert_eq!(pos.data.remaining_size(), 0.0);
        assert_eq!(pos.data.remaining_ratio, 0.0);
        assert_eq!(pos.data.status, PositionStatus::Closed);
        let last_ratio = pos
            .data
            .records
            .iter()
            .rev()
            .find_map(|(record, _)| match record {
                PositionRecord::PartialClose { ratio, .. } => Some(*ratio),
                _ => None,
            })
            .unwrap();
        assert!((last_ratio - 0.2).abs() < 1e-12);
    }

    #[test]
    fn serde_migrates_legacy_ratio_to_absolute_closed_size() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 2.0),
            vec![],
        );
        pos.data
            .apply_partial_close(0.25, 1.0900, CloseReason::Manual, ts(10, 30, 0));

        let mut legacy = serde_json::to_value(&pos.data).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("closed_size")
            .unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("open_entry_value")
            .unwrap();
        let migrated: PositionData = serde_json::from_value(legacy).unwrap();
        assert!((migrated.closed_size - 0.5).abs() < f64::EPSILON);
        assert!((migrated.remaining_size() - 1.5).abs() < f64::EPSILON);
        assert!((migrated.remaining_ratio - 0.75).abs() < f64::EPSILON);
        assert!((migrated.open_entry_value - 1.6275).abs() < f64::EPSILON);
        assert!((migrated.average_entry() - 1.0850).abs() < f64::EPSILON);

        let mut current = serde_json::to_value(&migrated).unwrap();
        current["remaining_ratio"] = serde_json::json!(0.99);
        let round_trip: PositionData = serde_json::from_value(current).unwrap();
        assert!((round_trip.closed_size - 0.5).abs() < f64::EPSILON);
        assert!((round_trip.remaining_ratio - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn serde_defaults_legacy_unclosed_position_to_zero_closed_size() {
        let pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 2.0),
            vec![],
        );
        let mut legacy = serde_json::to_value(&pos.data).unwrap();
        legacy.as_object_mut().unwrap().remove("closed_size");
        legacy.as_object_mut().unwrap().remove("open_entry_value");

        let migrated: PositionData = serde_json::from_value(legacy).unwrap();
        assert_eq!(migrated.closed_size, 0.0);
        assert_eq!(migrated.remaining_size(), 2.0);
        assert_eq!(migrated.remaining_ratio, 1.0);
    }

    #[test]
    fn full_close_via_partial() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![],
        );
        pos.data
            .apply_partial_close(1.0, 1.0900, CloseReason::Target, ts(10, 30, 0));
        assert_eq!(pos.data.status, PositionStatus::Closed);
        assert!(pos.data.close_ts.is_some());
        assert_eq!(pos.data.closed_size, 1.0);
        assert_eq!(pos.data.remaining_size(), 0.0);
        assert_eq!(pos.data.remaining_ratio, 0.0);
    }

    #[test]
    fn full_close() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Sell,
            make_fill(1.0850, 1.0),
            vec![],
        );
        pos.data
            .apply_full_close(CloseReason::Stoploss, ts(10, 30, 0));
        assert_eq!(pos.data.status, PositionStatus::Closed);
        assert_eq!(pos.data.closed_size, 1.0);
        assert_eq!(pos.data.remaining_size(), 0.0);
        assert_eq!(pos.data.remaining_ratio, 0.0);
    }

    #[test]
    fn unrealized_pnl_buy() {
        let pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![],
        );
        let pnl = pos.data.unrealized_pnl(1.0900);
        assert!((pnl - 0.0050).abs() < 1e-10);
    }

    #[test]
    fn unrealized_pnl_sell() {
        let pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Sell,
            make_fill(1.0850, 1.0),
            vec![],
        );
        let pnl = pos.data.unrealized_pnl(1.0800);
        assert!((pnl - 0.0050).abs() < 1e-10);
    }

    #[test]
    fn try_fill_limit_buy() {
        let mut pos = Position::new_pending(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            OrderType::Limit,
            1.0800,
            1.0,
            ts(9, 0, 0),
            vec![],
        );
        assert_eq!(pos.data.status, PositionStatus::Pending);

        // Ask still above limit → no fill
        let q1 = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            bid: 1.0808,
            ask: 1.0810,
        };
        assert!(!pos.try_fill(&q1, FillModel::BidAsk));
        assert_eq!(pos.data.status, PositionStatus::Pending);

        // Ask at or below limit → fill
        let q2 = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 5, 0),
            bid: 1.0798,
            ask: 1.0800,
        };
        assert!(pos.try_fill(&q2, FillModel::BidAsk));
        assert_eq!(pos.data.status, PositionStatus::Open);
        assert_eq!(pos.data.entries.len(), 1);
        assert!((pos.data.entries[0].price - 1.0800).abs() < f64::EPSILON);
    }

    #[test]
    fn try_fill_stop_sell() {
        let mut pos = Position::new_pending(
            "p1".into(),
            "EURUSD".into(),
            Side::Sell,
            OrderType::Stop,
            1.0800,
            1.0,
            ts(9, 0, 0),
            vec![],
        );

        // Ask still above stop → no fill (BidAsk mode: sell checks bid)
        let q1 = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            bid: 1.0810,
            ask: 1.0812,
        };
        assert!(!pos.try_fill(&q1, FillModel::BidAsk));

        // Bid at or below stop → fill
        let q2 = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 5, 0),
            bid: 1.0800,
            ask: 1.0802,
        };
        assert!(pos.try_fill(&q2, FillModel::BidAsk));
        assert_eq!(pos.data.status, PositionStatus::Open);
    }

    #[test]
    fn set_stoploss_updates_existing() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![Rule::fixed_stoploss(1.0800)],
        );
        assert!((pos.current_stoploss().unwrap() - 1.0800).abs() < f64::EPSILON);

        let old = pos.set_stoploss(1.0820);
        assert!((old.unwrap() - 1.0800).abs() < f64::EPSILON);
        assert!((pos.current_stoploss().unwrap() - 1.0820).abs() < f64::EPSILON);
    }

    #[test]
    fn set_stoploss_adds_when_missing() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![],
        );
        assert!(pos.current_stoploss().is_none());

        let old = pos.set_stoploss(1.0800);
        assert!(old.is_none());
        assert!((pos.current_stoploss().unwrap() - 1.0800).abs() < f64::EPSILON);
    }

    #[test]
    fn remove_rule_by_name() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![Rule::fixed_stoploss(1.0800), Rule::take_profit(1.0900, 1.0)],
        );
        assert_eq!(pos.rules.len(), 2);
        assert!(pos.remove_rule("TakeProfit"));
        assert_eq!(pos.rules.len(), 1);
        assert_eq!(pos.rules[0].name(), "FixedStoploss");
    }

    #[test]
    fn evaluate_rules_produces_effects() {
        let mut pos = Position::new_market(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            make_fill(1.0850, 1.0),
            vec![Rule::fixed_stoploss(1.0800), Rule::take_profit(1.0900, 1.0)],
        );

        // Price between SL and TP → no effects
        let q = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 5, 0),
            bid: 1.0860,
            ask: 1.0862,
        };
        let effects = pos.evaluate_rules(&q, FillModel::BidAsk);
        assert!(effects.is_empty());

        // Price hits SL → close effect
        let q_sl = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 10, 0),
            bid: 1.0799,
            ask: 1.0801,
        };
        let effects = pos.evaluate_rules(&q_sl, FillModel::BidAsk);
        assert!(!effects.is_empty());
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        ));
    }

    #[test]
    fn pending_position_skips_rule_evaluation() {
        let mut pos = Position::new_pending(
            "p1".into(),
            "EURUSD".into(),
            Side::Buy,
            OrderType::Limit,
            1.0800,
            1.0,
            ts(9, 0, 0),
            vec![Rule::fixed_stoploss(1.0750)],
        );

        // Even though bid is below SL, position is pending → no effects
        let q = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            bid: 1.0740,
            ask: 1.0742,
        };
        let effects = pos.evaluate_rules(&q, FillModel::BidAsk);
        assert!(effects.is_empty());
    }
}
