//! `qs-core` — Core trade engine for the quant-system workspace.
//!
//! This crate provides a **synchronous, side-effect-free** trade engine built
//! around composable position management rules.  It is the shared foundation
//! used by both the backtesting crate (`qs-backtest`) and live trading
//! integrations.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`TradeEngine`] | Main entry point — processes actions and price updates |
//! | [`Position`] | Atomic unit of market exposure (data + rules) |
//! | [`Rule`] | Composable management rule (stoploss, trailing, TP, …) |
//! | [`Action`] | Input vocabulary — what a strategy can request |
//! | [`Effect`] | Output vocabulary — observable side-effects for the caller |
//! | [`Signal`] | Timestamped action for replay / backtesting |
//!
//! # Design principle
//!
//! **Effects out, logic pure.**  The engine never performs IO.  It takes inputs
//! (`Action`, `PriceQuote`) and returns `Vec<Effect>`.  The caller decides how
//! to handle effects (simulate fills for backtest, send broker orders for live).

pub mod alert_register;
pub mod engine;
pub mod error;
pub mod execution;
pub mod position;
pub mod position_manager;
pub mod rules;
pub mod types;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use alert_register::PriceAlertRegister;
pub use engine::{FutureApplyError, FutureApplyResult, TradeEngine};
pub use error::{CoreError, Result};
pub use execution::{ExecutionError, ExecutionPricer, ExecutionResult};
pub use position::Position;
pub use rules::Rule;
pub use types::{
    Action, CloseReason, Effect, EffectiveStop, ExecutionConvention, ExecutionFill, ExecutionModel,
    Fill, FillModel, FillPurpose, FixedPrice, FutureEffect, FutureFill, GroupId, Lots, OrderType,
    PositionId, PositionRecord, PositionStatus, PreparedPendingFill, PriceQuote, RuleConfig, Side,
    Signal, SlippageModel, StopOrigin, TargetSpec, TradeId,
};
