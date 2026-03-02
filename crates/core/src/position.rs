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
    CloseReason, Effect, Fill, FillModel, GroupId, OrderType, PositionId, PositionRecord,
    PositionStatus, PriceQuote, Side,
};

/// Core position data — the pure state without rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Fraction of the original position still open (1.0 = full, 0.0 = closed).
    pub remaining_ratio: f64,

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

    /// Immutable audit trail.
    pub records: Vec<(PositionRecord, NaiveDateTime)>,
}

/// A position: data + composable management rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub data: PositionData,
    pub rules: Vec<Rule>,
}

// ─── PositionData helpers ───────────────────────────────────────────────────

impl PositionData {
    /// Volume-weighted average entry price across all fills.
    pub fn average_entry(&self) -> f64 {
        if self.entries.is_empty() {
            return 0.0;
        }
        let total_value: f64 = self.entries.iter().map(|f| f.price * f.size).sum();
        let total_size: f64 = self.entries.iter().map(|f| f.size).sum();
        if total_size == 0.0 {
            0.0
        } else {
            total_value / total_size
        }
    }

    /// Total filled size (sum of all fills).
    pub fn total_filled_size(&self) -> f64 {
        self.entries.iter().map(|f| f.size).sum()
    }

    /// Size still active in the market.
    pub fn remaining_size(&self) -> f64 {
        self.total_filled_size() * self.remaining_ratio
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
        self.status == PositionStatus::Open && self.remaining_ratio > 0.0
    }

    /// Add a fill (scale-in).
    pub fn add_fill(&mut self, fill: Fill) {
        self.entries.push(fill);
    }

    /// Record a partial close, reducing `remaining_ratio`.
    ///
    /// If the remaining ratio reaches zero the status is flipped to `Closed`.
    pub fn apply_partial_close(
        &mut self,
        ratio: f64,
        price: f64,
        reason: CloseReason,
        ts: NaiveDateTime,
    ) {
        self.remaining_ratio = (self.remaining_ratio - ratio).max(0.0);
        if reason == CloseReason::Target {
            self.target_hits += 1;
        }
        self.records.push((
            PositionRecord::PartialClose {
                ratio,
                price,
                reason,
            },
            ts,
        ));
        if self.remaining_ratio <= f64::EPSILON {
            self.status = PositionStatus::Closed;
            self.close_ts = Some(ts);
            self.records.push((PositionRecord::Closed { reason }, ts));
        }
    }

    /// Mark the position as fully closed.
    pub fn apply_full_close(&mut self, reason: CloseReason, ts: NaiveDateTime) {
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
            remaining_ratio: self.remaining_ratio,
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
                target_hits: 0,
                open_ts: Some(open_ts),
                close_ts: None,
                group: None,
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
                target_hits: 0,
                open_ts: None,
                close_ts: None,
                group: None,
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

    /// Check if a pending order should fill at the given quote.
    ///
    /// Returns `true` (and transitions the position to Open) if the fill
    /// condition is met.
    pub fn try_fill(&mut self, quote: &PriceQuote, model: FillModel) -> bool {
        if self.data.status != PositionStatus::Pending {
            return false;
        }

        let pending_price = match self.data.pending_price {
            Some(p) => p,
            None => return false,
        };

        let check = quote.fill_price(self.data.side, model);

        let should_fill = match (self.data.order_type, self.data.side) {
            // Limit Buy: fill when price drops to or below limit
            (OrderType::Limit, Side::Buy) => check <= pending_price,
            // Limit Sell: fill when price rises to or above limit
            (OrderType::Limit, Side::Sell) => check >= pending_price,
            // Stop Buy: fill when price rises to or above stop
            (OrderType::Stop, Side::Buy) => check >= pending_price,
            // Stop Sell: fill when price drops to or below stop
            (OrderType::Stop, Side::Sell) => check <= pending_price,
            // Market orders should never be pending
            (OrderType::Market, _) => false,
        };

        if should_fill {
            let fill = Fill {
                price: pending_price,
                size: self.data.size,
                ts: quote.ts,
            };
            self.data.entries.push(fill.clone());
            self.data.status = PositionStatus::Open;
            self.data.open_ts = Some(quote.ts);
            self.data
                .records
                .push((PositionRecord::Filled { fill }, quote.ts));
            true
        } else {
            false
        }
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

    /// Find the current fixed-stoploss price, if any.
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
}

// ─── Tests ──────────────────────────────────────────────────────────────────

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
        assert!((pos.data.remaining_ratio).abs() < f64::EPSILON);
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
