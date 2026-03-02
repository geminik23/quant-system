//! Core type definitions shared across the trade engine.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

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
