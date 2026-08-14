//! `quant-system-core` package (`qs_core` library) - Core trade engine for the quant-system workspace.
//!
//! This crate provides the **synchronous, side-effect-free** trading domain used by backtesting and future live integrations. It contains the trade engine, normalized signal intent, management-policy resolution, position sizing, and currency-conversion logic, but performs no configuration IO, networking, storage, or broker calls.
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
pub mod currency;
pub mod engine;
pub mod error;
pub mod execution;
pub mod position;
pub mod position_manager;
pub mod profile;
pub mod rules;
pub mod sizing;
pub mod types;
pub mod validation;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use alert_register::PriceAlertRegister;
pub use currency::{
    ConversionError, ConversionLeg, ConversionLegAudit, ConversionPriceSide, ConversionQuoteBook,
    ConversionResult, ConversionRoute, FxPair, FxPairDirection, QuoteValidationError,
    RunCurrencyPlan, RunCurrencyPlanError, resolve_conversion_route, resolve_fx_pair,
};
pub use engine::{FutureApplyError, FutureApplyResult, TradeEngine};
pub use error::{CoreError, Result};
pub use execution::{ExecutionError, ExecutionPricer, ExecutionResult};
pub use position::Position;
pub use profile::{
    ManagementProfile, PositionRef, PositionResolver, ProfileApplicationError,
    ProfileValidationError, RawSignal, ResolvedEntry, RuleConfigDef, StoplossMode,
    TargetResolution, TargetSelection, allocate_target_steps, allocate_target_units,
    resolve_signal, resolve_unprofiled_entry, validate_profile,
};
pub use rules::Rule;
pub use sizing::{
    InstrumentSizingError, LotCapStatus, SizingError, SizingPolicy, SizingResult,
    compute_instrument_native_loss_per_lot, compute_instrument_size,
    compute_instrument_size_for_spec, compute_size,
};
pub use types::{
    Action, CloseReason, Effect, EffectiveStop, ExecutionConvention, ExecutionFill, ExecutionModel,
    Fill, FillModel, FillPurpose, FixedPrice, FutureEffect, FutureFill, GroupId, Lots, OrderType,
    PositionId, PositionRecord, PositionStatus, PreparedPendingFill, PriceQuote, RuleConfig, Side,
    Signal, SlippageModel, StopOrigin, TargetSpec, TradeId,
};
pub use validation::{RawSignalValidationError, validate_raw_signal, validate_raw_signals};
