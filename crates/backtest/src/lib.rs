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
//! 2. **Signal replay** - provide strict timestamped [`RawSignal`] values; the runner injects them at the correct moments while replaying price data.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`BacktestRunner`] | Orchestrates the backtest loop (both modes) |
//! | [`BacktestExecutor`] | Tracks simulated fills, positions, and P&L |
//! | [`BacktestResult`] | Final report with P&L, drawdown, and execution artifacts |
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
pub use strategy::{
    AnalysisBoundary, AnalysisBoundaryOutput, AnalysisContext, AnalysisError, AnalysisPipeline,
    AnnotationError, AnnotationId, AnnotationLimits, AnnotationTimeline, AnnotationUse,
    BarSeriesSpec, BarWindow, ClosedBar, ConfirmedPivotAnalyzer, HistoricalAnalyzer,
    HistoricalObservationView, HistoricalSeriesView, HistoricalStrategy, MAX_ANALYZERS,
    MAX_ANNOTATION_ID_BYTES, MAX_ANNOTATION_NOTE_BYTES, MAX_ANNOTATIONS, MAX_DECISION_LATENCY_MS,
    MAX_DECISION_RECORDS, MAX_INSTRUMENT_BYTES, MAX_OBSERVATION_SOURCE_SERIES,
    MAX_OBSERVATIONS_PER_BOUNDARY, MAX_PIVOT_SIDE_BARS, MAX_REASON_BYTES, MAX_RETAINED_BARS,
    MAX_RETAINED_OBSERVATIONS, MAX_SERIES_ID_BYTES, MAX_SIGNALS_PER_CALLBACK,
    MAX_STRATEGY_ID_BYTES, MAX_STRATEGY_REVISION_BYTES, MAX_STRATEGY_TITLE_BYTES,
    MAX_TRADE_ID_BYTES, MAX_WARMUP_BARS, MAX_ZONE_ID_BYTES, MissingIntervalPolicy, MomentumState,
    MultiTimeframeSeries, ObservationOrigin, ObservationSelection, ObservationStore,
    ObservationStoreLimits, ObservationWindow, PivotConfig, PriceBasis, PriceZone,
    RejectionPattern, SeriesError, SeriesId, SeriesRequirement, SeriesViewError, SeriesWarmupState,
    Strategy, StrategyAnnotation, StrategyBacktestResult, StrategyConfigError, StrategyContext,
    StrategyDecisionDraft, StrategyDecisionKind, StrategyDecisionOutput, StrategyDecisionRecord,
    StrategyDecisionRecorder, StrategyDecisionRetention, StrategyDescriptor, StrategyDomainError,
    StrategyEvent, StrategyFeedback, StrategyId, StrategyObservation, StrategyObservationDraft,
    StrategyObservationValue, StrategyOutput, StrategyRequirements, StrategyRetentionLimits,
    StrategyRuntimeError, SwingKind, SwingPoint, Timeframe, WarmupRequirement, ZoneId, ZoneSide,
    ZoneSource, ZoneState,
};
