//! Core type definitions shared across the trade engine.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

// ─── Fixed-point price ──────────────────────────────────────────────────────

/// Fixed-point price representation using integer arithmetic.
///
/// Internally stores price as an integer scaled by `10^digits`.
/// For EURUSD (digits=5): 1.08500 → 108500
/// For XAUUSD (digits=2): 2350.50 → 235050
/// For USDJPY (digits=3): 154.325 → 154325
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FixedPrice(i64);

impl FixedPrice {
    pub const ZERO: Self = Self(0);

    /// Convert from f64 using the symbol's digit count.
    pub fn from_f64(value: f64, digits: u16) -> Self {
        let scale = 10i64.pow(digits as u32);
        Self((value * scale as f64).round() as i64)
    }

    /// Convert back to f64 for display or P&L calculation.
    pub fn to_f64(self, digits: u16) -> f64 {
        let scale = 10i64.pow(digits as u32);
        self.0 as f64 / scale as f64
    }

    /// Raw integer value (for arithmetic).
    pub fn raw(self) -> i64 {
        self.0
    }

    /// Create from raw integer value.
    pub fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// Absolute value.
    pub fn abs(self) -> Self {
        Self(self.0.abs())
    }
}

impl std::ops::Add for FixedPrice {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for FixedPrice {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl std::ops::Neg for FixedPrice {
    type Output = Self;
    fn neg(self) -> Self {
        Self(-self.0)
    }
}

impl std::fmt::Display for FixedPrice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FixedPrice({})", self.0)
    }
}

// ─── Lot size ───────────────────────────────────────────────────────────────

/// Lot size as integer multiples of the symbol's lot step.
///
/// For forex with lot_step=0.01:
///   0.01 lots = Lots(1), 0.02 lots = Lots(2), 1.00 lots = Lots(100)
///
/// Values are always on the grid. No rounding surprises. No broker rejections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lots(i64);

impl Lots {
    pub const ZERO: Self = Self(0);

    /// Create from a step count.
    pub fn new(steps: i64) -> Self {
        Self(steps)
    }

    /// Number of lot steps.
    pub fn steps(self) -> i64 {
        self.0
    }

    /// Convert from f64 lot size using the symbol's lot step.
    /// Rounds to the nearest valid step — the ONLY place where rounding happens.
    pub fn from_f64(lot: f64, lot_step: f64) -> Self {
        Self((lot / lot_step).round() as i64)
    }

    /// Convert back to f64 lot size for broker API submission.
    pub fn to_f64(self, lot_step: f64) -> f64 {
        self.0 as f64 * lot_step
    }

    /// Convert to broker volume units (e.g. CTrader uses raw units).
    pub fn to_broker_units(self, lot_step_units: i64) -> i64 {
        self.0 * lot_step_units
    }

    /// Create from broker volume units.
    pub fn from_broker_units(units: i64, lot_step_units: i64) -> Self {
        Self(units / lot_step_units)
    }

    /// Apply a partial close ratio (0–100 pct). Returns (close_lots, remaining_lots).
    /// Both values are guaranteed to be valid lot step multiples.
    pub fn partial_close(self, ratio_pct: u32) -> (Lots, Lots) {
        let close_steps = (self.0 * ratio_pct as i64 + 50) / 100;
        let close_steps = close_steps.max(1).min(self.0);
        let remaining = self.0 - close_steps;
        (Lots(close_steps), Lots(remaining))
    }

    /// Apply a partial close with a float ratio (0.0–1.0).
    /// Converts ratio to integer steps, then delegates to integer arithmetic.
    pub fn partial_close_f64(self, ratio: f64) -> (Lots, Lots) {
        let close_steps = (self.0 as f64 * ratio).round() as i64;
        let close_steps = close_steps.max(1).min(self.0);
        let remaining = self.0 - close_steps;
        (Lots(close_steps), Lots(remaining))
    }

    /// Check if this is a valid lot size (above minimum, below maximum).
    pub fn is_valid(self, min_steps: i64, max_steps: i64) -> bool {
        self.0 >= min_steps && (max_steps == 0 || self.0 <= max_steps)
    }
}

impl std::ops::Add for Lots {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl std::ops::Sub for Lots {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

impl std::fmt::Display for Lots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lots({})", self.0)
    }
}

// ─── Fill model ─────────────────────────────────────────────────────────────

/// How fill conditions, rule triggers, and close prices interpret a
/// [`PriceQuote`].
///
/// This is a **simulation-wide** setting stored on [`TradeEngine`](crate::engine::TradeEngine).
/// It controls which price from the quote is used for pending-order fills,
/// stoploss/take-profit evaluation, trailing stop tracking, and close-price
/// calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum FillModel {
    /// Use the appropriate side of the spread for each operation.
    ///
    /// - **Buy** opens at **ask**, closes / SL / TP evaluate against **bid**.
    /// - **Sell** opens at **bid**, closes / SL / TP evaluate against **ask**.
    ///
    /// This is the most realistic model and the default.
    #[default]
    BidAsk,

    /// Use the **ask** price for all fill checks, rule evaluation, and close
    /// price calculation, regardless of side.
    ///
    /// Simpler model that ignores spread directionality.  Slightly optimistic
    /// for sell-side evaluations.
    AskOnly,

    /// Use the mid-point `(bid + ask) / 2` for everything.
    ///
    /// Common in academic backtesting — removes all spread cost.
    MidPrice,
}

impl std::fmt::Display for FillModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FillModel::BidAsk => write!(f, "BidAsk"),
            FillModel::AskOnly => write!(f, "AskOnly"),
            FillModel::MidPrice => write!(f, "MidPrice"),
        }
    }
}

// ─── Identity types ─────────────────────────────────────────────────────────

/// Unique identifier for a position.
pub type PositionId = String;

/// Unique identifier for a position group.
pub type GroupId = String;

// ─── Enums ──────────────────────────────────────────────────────────────────

/// Trade direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Returns the opposite side.
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "Buy"),
            Side::Sell => write!(f, "Sell"),
        }
    }
}

/// How an order enters the market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderType {
    /// Fill immediately at current price.
    Market,
    /// Fill when price reaches the limit (buy below / sell above current).
    Limit,
    /// Fill when price breaks through the stop (buy above / sell below current).
    Stop,
}

impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Market => write!(f, "Market"),
            OrderType::Limit => write!(f, "Limit"),
            OrderType::Stop => write!(f, "Stop"),
        }
    }
}

/// Lifecycle status of a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionStatus {
    /// Order placed but not yet filled.
    Pending,
    /// Position is live in the market.
    Open,
    /// Position fully closed (P&L realized).
    Closed,
    /// Pending order was cancelled before filling.
    Cancelled,
}

impl std::fmt::Display for PositionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionStatus::Pending => write!(f, "Pending"),
            PositionStatus::Open => write!(f, "Open"),
            PositionStatus::Closed => write!(f, "Closed"),
            PositionStatus::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// Why a position (or part of it) was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CloseReason {
    Stoploss,
    Target,
    TrailingStop,
    TimeExit,
    BreakevenStop,
    Manual,
    GroupRule,
    Cancelled,
}

impl std::fmt::Display for CloseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloseReason::Stoploss => write!(f, "Stoploss"),
            CloseReason::Target => write!(f, "Target"),
            CloseReason::TrailingStop => write!(f, "TrailingStop"),
            CloseReason::TimeExit => write!(f, "TimeExit"),
            CloseReason::BreakevenStop => write!(f, "BreakevenStop"),
            CloseReason::Manual => write!(f, "Manual"),
            CloseReason::GroupRule => write!(f, "GroupRule"),
            CloseReason::Cancelled => write!(f, "Cancelled"),
        }
    }
}

// ─── Value types ────────────────────────────────────────────────────────────

/// A single execution fill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub price: f64,
    pub size: f64,
    pub ts: NaiveDateTime,
}

/// A bid/ask snapshot for a symbol at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceQuote {
    pub symbol: String,
    pub ts: NaiveDateTime,
    pub bid: f64,
    pub ask: f64,
}

impl PriceQuote {
    /// Mid price.
    pub fn mid(&self) -> f64 {
        (self.bid + self.ask) / 2.0
    }

    /// The price you would get when closing a position on this `side`.
    ///
    /// - Closing a **Buy** (long) means selling → you receive the **bid**.
    /// - Closing a **Sell** (short) means buying → you pay the **ask**.
    pub fn close_price(&self, side: Side) -> f64 {
        match side {
            Side::Buy => self.bid,
            Side::Sell => self.ask,
        }
    }

    /// The price you would pay when opening a position on this `side`.
    ///
    /// - Opening a **Buy** (long) means buying → you pay the **ask**.
    /// - Opening a **Sell** (short) means selling → you receive the **bid**.
    pub fn open_price(&self, side: Side) -> f64 {
        match side {
            Side::Buy => self.ask,
            Side::Sell => self.bid,
        }
    }

    /// The price used for **rule evaluation** (SL, TP, trailing stop,
    /// breakeven trigger checks) under the given [`FillModel`].
    ///
    /// | Model | Behaviour |
    /// |-------|-----------|
    /// | `BidAsk` | `close_price(side)` — bid for Buy, ask for Sell |
    /// | `AskOnly` | Always `ask` |
    /// | `MidPrice` | `(bid + ask) / 2` |
    pub fn eval_price(&self, side: Side, model: FillModel) -> f64 {
        match model {
            FillModel::BidAsk => self.close_price(side),
            FillModel::AskOnly => self.ask,
            FillModel::MidPrice => self.mid(),
        }
    }

    /// The price used for **opening fills** (market orders, pending order
    /// fill price lookups) under the given [`FillModel`].
    ///
    /// | Model | Behaviour |
    /// |-------|-----------|
    /// | `BidAsk` | `open_price(side)` — ask for Buy, bid for Sell |
    /// | `AskOnly` | Always `ask` |
    /// | `MidPrice` | `(bid + ask) / 2` |
    pub fn fill_price(&self, side: Side, model: FillModel) -> f64 {
        match model {
            FillModel::BidAsk => self.open_price(side),
            FillModel::AskOnly => self.ask,
            FillModel::MidPrice => self.mid(),
        }
    }
}

/// Specifies a take-profit level with the fraction of position to close.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetSpec {
    pub price: f64,
    /// Fraction of the **original** position size to close at this level (0.0–1.0).
    pub close_ratio: f64,
}

// ─── Signals ────────────────────────────────────────────────────────────────

/// A timestamped action — used for predefined signal replay in backtesting
/// or for forwarding strategy decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub ts: NaiveDateTime,
    pub action: Action,
}

// ─── Actions (input vocabulary) ─────────────────────────────────────────────

/// An action the engine can process.  Produced by strategies, signal providers,
/// or manual user input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    // ── Position lifecycle ───────────────────────────────────────────────
    /// Open a new independent position.
    Open {
        symbol: String,
        side: Side,
        order_type: OrderType,
        /// Entry price — required for Limit/Stop, optional for Market (uses
        /// last known quote if `None`).
        price: Option<f64>,
        size: f64,
        stoploss: Option<f64>,
        targets: Vec<TargetSpec>,
        rules: Vec<RuleConfig>,
        /// Optional group for per-signal-source tracking and group-level actions.
        #[serde(default)]
        group: Option<GroupId>,
    },

    /// Add size to an existing open position (scale-in).  Shares the
    /// position's existing management rules.
    ScaleIn {
        position_id: PositionId,
        price: Option<f64>,
        size: f64,
    },

    /// Close an entire position at market.
    ClosePosition { position_id: PositionId },

    /// Close a fraction of a position at market.
    ClosePartial {
        position_id: PositionId,
        /// Fraction of *original* size to close (0.0–1.0).
        ratio: f64,
    },

    /// Cancel a pending (unfilled) order.
    CancelPending { position_id: PositionId },

    // ── Modify ──────────────────────────────────────────────────────────
    /// Set or replace the fixed stoploss price.
    ModifyStoploss { position_id: PositionId, price: f64 },

    /// Move stoploss to the position's average entry price.
    MoveStoplossToEntry { position_id: PositionId },

    /// Add a take-profit level.
    AddTarget {
        position_id: PositionId,
        price: f64,
        close_ratio: f64,
    },

    /// Remove a take-profit level at a specific price.
    RemoveTarget { position_id: PositionId, price: f64 },

    /// Attach a new management rule to a position.
    AddRule {
        position_id: PositionId,
        rule: RuleConfig,
    },

    /// Remove a management rule by name.
    RemoveRule {
        position_id: PositionId,
        rule_name: String,
    },

    // ── Bulk ────────────────────────────────────────────────────────────
    /// Close all open positions on a given symbol.
    CloseAllOf { symbol: String },

    /// Close every open position.
    CloseAll,

    /// Cancel every pending order.
    CancelAllPending,

    /// Set the stoploss for all open positions on a symbol.
    ModifyAllStoploss { symbol: String, price: f64 },

    /// Close all open positions belonging to a group.
    CloseAllInGroup { group_id: GroupId },

    /// Set the stoploss for all open positions in a group.
    ModifyAllStoplossInGroup { group_id: GroupId, price: f64 },
}

// ─── Rule configuration (serializable) ──────────────────────────────────────

/// Declarative rule specification.  Converted into a live [`Rule`](crate::rules::Rule)
/// at runtime via `into_rule()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleConfig {
    FixedStoploss { price: f64 },
    TrailingStop { distance: f64 },
    TakeProfit { price: f64, close_ratio: f64 },
    BreakevenWhen { trigger_price: f64 },
    BreakevenAfterTargets { after_n: u32 },
    TimeExit { max_seconds: u64 },
}

// ─── FixedPrice & Lots tests ────────────────────────────────────────────────

#[cfg(test)]
mod fixed_point_tests {
    use super::*;

    // ── FixedPrice ──────────────────────────────────────────────────────

    #[test]
    fn fixed_price_from_f64_5digit() {
        let fp = FixedPrice::from_f64(1.08500, 5);
        assert_eq!(fp.raw(), 108500);
    }

    #[test]
    fn fixed_price_from_f64_2digit() {
        let fp = FixedPrice::from_f64(2350.50, 2);
        assert_eq!(fp.raw(), 235050);
    }

    #[test]
    fn fixed_price_from_f64_3digit() {
        let fp = FixedPrice::from_f64(154.325, 3);
        assert_eq!(fp.raw(), 154325);
    }

    #[test]
    fn fixed_price_from_f64_0digit() {
        let fp = FixedPrice::from_f64(18500.0, 0);
        assert_eq!(fp.raw(), 18500);
    }

    #[test]
    fn fixed_price_to_f64_roundtrip() {
        let prices = [1.08500_f64, 2350.50, 154.325, 0.00001, 100000.0];
        let digits = [5_u16, 2, 3, 5, 0];
        for (&p, &d) in prices.iter().zip(digits.iter()) {
            let fp = FixedPrice::from_f64(p, d);
            let back = fp.to_f64(d);
            assert!(
                (back - p).abs() < 1e-10,
                "roundtrip failed for {p} (digits={d}): got {back}"
            );
        }
    }

    #[test]
    fn fixed_price_arithmetic() {
        let a = FixedPrice::from_raw(108500);
        let b = FixedPrice::from_raw(1000);
        assert_eq!((a + b).raw(), 109500);
        assert_eq!((a - b).raw(), 107500);
        assert_eq!((-a).raw(), -108500);
        assert_eq!(FixedPrice::from_raw(-500).abs().raw(), 500);
    }

    #[test]
    fn fixed_price_comparison() {
        let a = FixedPrice::from_raw(108500);
        let b = FixedPrice::from_raw(108600);
        assert!(a < b);
        assert!(a <= b);
        assert!(b > a);
        assert!(b >= a);
        assert_eq!(a, FixedPrice::from_raw(108500));
        assert_ne!(a, b);
    }

    #[test]
    fn fixed_price_mid() {
        let bid = FixedPrice::from_raw(108480);
        let ask = FixedPrice::from_raw(108500);
        let mid = FixedPrice::from_raw((bid.raw() + ask.raw()) / 2);
        assert_eq!(mid.raw(), 108490);
    }

    #[test]
    fn fixed_price_rounding() {
        // 1.085005 with 5 digits → 108501 (rounds up)
        let fp = FixedPrice::from_f64(1.085005, 5);
        assert_eq!(fp.raw(), 108501);
    }

    #[test]
    fn fixed_price_zero() {
        assert_eq!(FixedPrice::ZERO.raw(), 0);
        assert_eq!(FixedPrice::from_f64(0.0, 5), FixedPrice::ZERO);
    }

    // ── Lots ────────────────────────────────────────────────────────────

    #[test]
    fn lots_from_f64() {
        let l = Lots::from_f64(0.02, 0.01);
        assert_eq!(l, Lots(2));
    }

    #[test]
    fn lots_to_f64() {
        let l = Lots(2);
        assert!((l.to_f64(0.01) - 0.02).abs() < f64::EPSILON);
    }

    #[test]
    fn lots_partial_close_50pct() {
        let size = Lots(2);
        let (close, remaining) = size.partial_close(50);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(1));
    }

    #[test]
    fn lots_partial_close_33pct() {
        let size = Lots(3);
        let (close, remaining) = size.partial_close(33);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(2));
    }

    #[test]
    fn lots_partial_close_25pct() {
        let size = Lots(4);
        let (close, remaining) = size.partial_close(25);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(3));
    }

    #[test]
    fn lots_partial_close_chain() {
        let mut remaining = Lots(4);
        for _ in 0..4 {
            let (close, rem) = remaining.partial_close_f64(1.0 / remaining.steps() as f64);
            assert!(close.steps() >= 1);
            remaining = rem;
        }
        assert_eq!(remaining, Lots::ZERO);
    }

    #[test]
    fn lots_partial_close_f64() {
        let size = Lots(2);
        let (close, remaining) = size.partial_close_f64(0.5);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(1));
    }

    #[test]
    fn lots_partial_close_min_1() {
        let size = Lots(1);
        let (close, remaining) = size.partial_close(50);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots::ZERO);
    }

    #[test]
    fn lots_to_broker_units() {
        let l = Lots(2);
        assert_eq!(l.to_broker_units(1000), 2000);
    }

    #[test]
    fn lots_from_broker_units() {
        let l = Lots::from_broker_units(2000, 1000);
        assert_eq!(l, Lots(2));
    }

    #[test]
    fn lots_is_valid() {
        assert!(Lots(1).is_valid(1, 0));
        assert!(Lots(1).is_valid(1, 100));
        assert!(!Lots(0).is_valid(1, 100));
        assert!(!Lots(101).is_valid(1, 100));
        assert!(Lots(100).is_valid(1, 100));
    }

    #[test]
    fn lots_add_sub() {
        assert_eq!(Lots(2) + Lots(3), Lots(5));
        assert_eq!(Lots(5) - Lots(2), Lots(3));
    }

    #[test]
    fn lots_zero_comparison() {
        assert_eq!(Lots(0), Lots::ZERO);
        assert_ne!(Lots(1), Lots::ZERO);
    }

    #[test]
    fn lots_to_f64_always_on_grid() {
        // The 0.02 * 0.5 = 0.019999... bug — solved by construction.
        let size = Lots::from_f64(0.02, 0.01);
        let (close, remaining) = size.partial_close_f64(0.5);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(1));
        assert!((close.to_f64(0.01) - 0.01).abs() < f64::EPSILON);
        assert!((remaining.to_f64(0.01) - 0.01).abs() < f64::EPSILON);

        // Even adversarial ratios produce valid lots.
        let size = Lots::from_f64(0.03, 0.01);
        let (close, remaining) = size.partial_close_f64(1.0 / 3.0);
        assert_eq!(close, Lots(1));
        assert_eq!(remaining, Lots(2));
        assert!((close.to_f64(0.01) - 0.01).abs() < f64::EPSILON);
        assert!((remaining.to_f64(0.01) - 0.02).abs() < f64::EPSILON);
    }

    #[test]
    fn lots_new_and_steps() {
        let l = Lots::new(42);
        assert_eq!(l.steps(), 42);
    }

    #[test]
    fn lots_display() {
        assert_eq!(format!("{}", Lots(5)), "Lots(5)");
    }

    #[test]
    fn fixed_price_display() {
        assert_eq!(
            format!("{}", FixedPrice::from_raw(108500)),
            "FixedPrice(108500)"
        );
    }
}

// ─── Effects (output vocabulary) ────────────────────────────────────────────

/// An observable side-effect produced by the engine.
///
/// The caller (backtest executor, live executor, …) decides what to do with
/// each effect (e.g. record P&L, send a broker order, write to DB).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Effect {
    /// A pending order was placed.
    OrderPlaced { id: PositionId },

    /// A pending order was cancelled.
    OrderCancelled { id: PositionId },

    /// A position was opened (filled).
    PositionOpened { id: PositionId },

    /// A position was fully closed.
    PositionClosed { id: PositionId, reason: CloseReason },

    /// A position was partially closed.
    PartialClose {
        id: PositionId,
        ratio: f64,
        reason: CloseReason,
    },

    /// The fixed stoploss of a position was moved.
    StoplossModified {
        id: PositionId,
        old_price: f64,
        new_price: f64,
    },

    /// A new fill was added to an existing position (scale-in).
    ScaledIn { id: PositionId, fill: Fill },

    /// A management rule was triggered (informational).
    RuleTriggered { id: PositionId, rule_name: String },
}

// ─── Position records (audit trail) ─────────────────────────────────────────

/// Immutable log entry for everything that happens to a position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PositionRecord {
    Created {
        symbol: String,
        side: Side,
        order_type: OrderType,
    },
    Filled {
        fill: Fill,
    },
    PartialClose {
        ratio: f64,
        price: f64,
        reason: CloseReason,
    },
    StoplossModified {
        from: Option<f64>,
        to: f64,
    },
    TargetAdded {
        price: f64,
        close_ratio: f64,
    },
    TargetRemoved {
        price: f64,
    },
    RuleAdded {
        rule_name: String,
    },
    RuleRemoved {
        rule_name: String,
    },
    Closed {
        reason: CloseReason,
    },
    Cancelled,
    /// Position was assigned to a group.
    GroupAssigned {
        group_id: GroupId,
    },
}
