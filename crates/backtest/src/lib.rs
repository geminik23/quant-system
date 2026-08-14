//! `qs-backtest` — Backtesting engine for the quant-system workspace.
//!
//! This crate provides tools for replaying historical market data through the
//! [`qs_core::TradeEngine`] to evaluate trading strategies and predefined
//! signal sets.
//!
//! # Two modes of operation
//!
//! 1. **Strategy-driven** — implement the [`Strategy`] trait; the runner feeds
//!    market data tick-by-tick and your strategy decides when to act.
//! 2. **Signal replay** — provide a `Vec<Signal>` with timestamps; the runner
//!    injects them at the correct moments while replaying price data.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`BacktestRunner`] | Orchestrates the backtest loop (both modes) |
//! | [`BacktestExecutor`] | Tracks simulated fills, positions, and P&L |
//! | [`BacktestResult`] | Final report — P&L, win rate, drawdown, trade log |
//! | [`DataFeed`] | Trait for sequential market event sources |
//! | [`Strategy`] | Trait for strategy-driven backtests |

pub mod artifacts;
pub mod currency;
pub mod data_feed;
pub mod economic_support;
pub mod evaluation;
pub mod executor;
pub mod future_executor;
pub mod ledger;
pub mod mtm;
pub mod portfolio;
pub mod profile;
pub mod report;
pub mod runner;
pub mod sizing;
pub mod strategy;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use artifacts::{
    CloseEvent, CompletedPosition, ExecutionMetadata, FutureBacktestArtifacts,
    InstrumentSizingArtifact, NetPnlOutcome, OpenPositionSnapshot, PendingOrderLifecycleEvent,
    PendingOrderLifecycleState, PendingOrderSnapshot, RecordedFill, ReplayInstrumentArtifact,
    ReplayInstrumentManifest, RiskBasisStatus, RiskTranche,
};
pub use currency::{
    ConversionError, ConversionLeg, ConversionLegAudit, ConversionPriceSide, ConversionQuoteBook,
    ConversionResult, ConversionRoute, FxPair, FxPairDirection, QuoteValidationError,
    RunCurrencyPlan, RunCurrencyPlanError, resolve_conversion_route, resolve_fx_pair,
};
pub use data_feed::{DataFeed, MarketEvent, VecFeed};
pub use economic_support::{
    EconomicSupportError, LEGACY_ECONOMIC_GUARD_ID, LegacyEconomicModel, SupportedLegacyEconomics,
    guarded_instrument_spec, resolve_legacy_economics,
};
pub use evaluation::{
    BootstrapConfig, BreakdownDimension, EvaluationContext, EvaluationOptions, EvaluationReport,
    EvaluationSection, GroupFilter, PositionFilter, PositionSide,
};
pub use executor::BacktestExecutor;
pub use future_executor::FutureExecutor;
pub use mtm::{
    DEFAULT_MTM_MAX_POINTS, MAX_MTM_MAX_POINTS, MIN_MTM_MAX_POINTS, MtmCurveCollector,
    MtmOutputPolicy, MtmOutputPolicyError, MtmOutputSummary,
};
pub use profile::{
    ManagementProfile, PositionRef, PositionResolver, ProfileApplicationError, ProfileError,
    ProfileRegistry, ProfileRegistryError, ProfileValidationError, RawSignal, ResolvedEntry,
    RuleConfigDef, StoplossMode, TargetResolution, TargetSelection, allocate_target_units,
    resolve_signal, resolve_unprofiled_entry,
};
pub use report::{
    BacktestResult, CloseReasonStats, DurationStats, MonthlyReturn, PositionSummary, RiskMetrics,
    StreakStats, SubsetStats, TradeResult,
};
pub use runner::{
    BacktestRunner, FutureQuoteConfig, ReplayCancelled, ReplayProgress, StreamingReplayError,
};
pub use strategy::Strategy;
