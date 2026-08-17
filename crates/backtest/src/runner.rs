//! Backtest runner — orchestrates the backtest loop.
//!
//! [`BacktestRunner`] combines a [`TradeEngine`], a [`BacktestExecutor`], and
//! either a [`Strategy`] or a set of predefined [`RawSignal`]s to produce a
//! [`BacktestResult`].
//!
//! # Two modes of operation
//!
//! 1. **Strategy-driven** ([`run_strategy`](BacktestRunner::run_strategy)):
//!    The runner feeds market events to a [`Strategy`] implementation.  The
//!    strategy returns [`Action`]s which are forwarded to the engine.
//!
//! 2. **Raw-signal replay** ([`run_raw_signals`](BacktestRunner::run_raw_signals)):
//!    A pre-sorted `Vec<RawSignal>` is merged with the market data timeline.
//!    Signals are injected at the correct timestamps.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::Infallible;

use chrono::{Duration, NaiveDateTime};
use qs_core::sizing::{compute_instrument_native_loss_per_lot, compute_instrument_size_for_spec};
use qs_core::types::{
    Action, CloseReason, Effect, ExecutionFill, ExecutionModel, FillModel, FutureEffect, OrderType,
    PositionStatus, PreparedPendingFill, PriceQuote, Side, SlippageModel, position_size_tolerance,
};
use qs_core::{ExecutionPricer, FutureApplyError, TradeEngine};
use qs_instruments::{Decimal, EconomicsModelId, InstrumentSpec, ListingStatus, QuantityUnit};

use crate::artifacts::{
    ExecutionMetadata, FUTURE_ARTIFACT_FORMAT_VERSION, FutureBacktestArtifacts,
    InstrumentSizingArtifact, PendingOrderSnapshot, ReplayInstrumentManifest,
};
use crate::currency::{ConversionQuoteBook, RunCurrencyPlan};
use crate::data_feed::{DataFeed, FallibleBatchFeed, FeedEvent, MarketEvent, TimestampBatch};
use crate::economic_support::{LEGACY_ECONOMIC_GUARD_ID, resolve_legacy_economics};
use crate::evaluation::EvaluationOptions;
use crate::executor::BacktestExecutor;
use crate::future_executor::{FutureExecutor, FutureExecutorError};
use crate::ledger::{ActionDisposition, LifecycleLedger};
use crate::mtm::{MtmCurveCollector, MtmOutputPolicy, MtmOutputSummary};
use crate::portfolio::{EquityPoint, PortfolioRecorder};
use crate::profile::{
    ManagementProfile, RawSignal, ResolvedEntry, allocate_target_steps, resolve_signal,
    resolve_unprofiled_entry,
};
use crate::report::BacktestResult;
use crate::sizing::{SizingPolicy, compute_native_loss_per_lot, compute_size};
use crate::strategy::{
    AnalysisBoundary, AnalysisPipeline, BarSeriesSpec, HistoricalStrategy, MultiTimeframeSeries,
    Strategy, StrategyBacktestResult, StrategyContext, StrategyDecisionRecorder, StrategyEvent,
    StrategyFeedback, StrategyReplayError, StrategyReplayInputError, StrategyRetentionLimits,
};

/// Future-quote execution settings. Existing runners remain on legacy semantics
/// unless [`BacktestRunner::run_raw_signals_future`] is used.
#[derive(Debug, Clone)]
pub struct FutureQuoteConfig {
    /// Signal processing latency added before an action becomes eligible.
    pub signal_latency_ms: i64,
    /// Fixed signed slippage in pips (`+` adverse, `-` favorable).
    pub slippage_pips: f64,
    /// Quote age threshold used by mark-to-market diagnostics.
    pub stale_quote_after_ms: Option<i64>,
    /// Absolute account-currency tolerance for breakeven classification.
    pub pnl_epsilon: f64,
    /// Immutable primary and conversion currency routing for this run.
    pub currency_plan: Option<RunCurrencyPlan>,
    /// Maximum age of a conversion quote used for sizing.
    pub conversion_stale_after_ms: i64,
    /// Controls how many exact mark-to-market observations are emitted.
    pub mtm_output: MtmOutputPolicy,
}

impl Default for FutureQuoteConfig {
    fn default() -> Self {
        Self {
            signal_latency_ms: 0,
            slippage_pips: 0.0,
            stale_quote_after_ms: None,
            pnl_epsilon: 1.0e-9,
            currency_plan: None,
            conversion_stale_after_ms: 300_000,
            mtm_output: MtmOutputPolicy::default(),
        }
    }
}

/// Monotonic replay counters emitted by cancellable signal replays.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayProgress {
    pub processed_events: usize,
    pub total_events: usize,
    pub processed_signals: usize,
    pub total_signals: usize,
}

/// Cooperative cancellation marker for a controlled replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("backtest replay cancelled")]
pub struct ReplayCancelled;

/// Error returned by a controlled FutureQuote streaming replay.
#[derive(Debug, thiserror::Error)]
pub enum StreamingReplayError<E> {
    #[error("market-data stream failed: {0}")]
    Feed(E),
    #[error(transparent)]
    Cancelled(#[from] ReplayCancelled),
}

const REPLAY_PROGRESS_INTERVAL: usize = 256;

fn should_report_progress(processed: usize, total: usize) -> bool {
    processed == total || processed.is_multiple_of(REPLAY_PROGRESS_INTERVAL)
}

#[derive(Debug, Clone)]
struct ScheduledSignal {
    sequence: u64,
    signal_ts: NaiveDateTime,
    effective_ts: NaiveDateTime,
    signal: RawSignal,
    requires_later_quote: bool,
}

#[derive(Debug, Clone)]
struct QueuedAction {
    action_id: String,
    action_kind: String,
    action: Action,
    execution: Option<ExecutionFill>,
    symbol: String,
    signal_ts: NaiveDateTime,
    effective_ts: NaiveDateTime,
    entry_signal: Option<RawSignal>,
    entry_profile: Option<ManagementProfile>,
    requires_later_quote: bool,
}

#[derive(Debug, thiserror::Error)]
enum FutureTransactionError {
    #[error(transparent)]
    Core(#[from] FutureApplyError),
    #[error(transparent)]
    Accounting(#[from] FutureExecutorError),
}

#[derive(Debug)]
enum StrategyDriverError<E> {
    Series(crate::strategy::SeriesError),
    SeriesView(crate::strategy::SeriesViewError),
    Analysis(crate::strategy::AnalysisError),
    Strategy(E),
    Runtime(crate::strategy::StrategyRuntimeError),
    WarmupSignals {
        timestamp: NaiveDateTime,
    },
    InvalidGeneratedSignal {
        signal_index: usize,
        reason: String,
    },
    TickExecutionRequired {
        symbol: String,
        timestamp: NaiveDateTime,
    },
}

enum FutureBatchReplayError<E> {
    Feed(E),
    Cancelled,
    Dynamic,
}

trait FutureReplayHook {
    fn is_active(&self) -> bool;
    fn output_ready(&self) -> bool;
    fn preflight_primary_events(&mut self, events: &[FeedEvent]) -> bool;
    fn reject_generated_configuration(&mut self, reason: String);
    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        pending_effects: &mut Vec<FutureEffect>,
    ) -> Option<Vec<ScheduledSignal>>;
}

struct StaticReplayHook;

impl FutureReplayHook for StaticReplayHook {
    fn is_active(&self) -> bool {
        false
    }

    fn output_ready(&self) -> bool {
        true
    }

    fn preflight_primary_events(&mut self, _events: &[FeedEvent]) -> bool {
        true
    }

    fn reject_generated_configuration(&mut self, _reason: String) {
        unreachable!("static replay does not generate strategy signals");
    }

    fn on_boundary(
        &mut self,
        _batch: &TimestampBatch,
        _engine: &TradeEngine,
        _lifecycle: &LifecycleLedger,
        pending_effects: &mut Vec<FutureEffect>,
    ) -> Option<Vec<ScheduledSignal>> {
        pending_effects.clear();
        Some(Vec::new())
    }
}

struct StrategyReplayDriver<'a, S: HistoricalStrategy> {
    strategy: &'a mut S,
    requirements: crate::strategy::StrategyRequirements,
    series: MultiTimeframeSeries,
    analysis: AnalysisPipeline,
    limits: StrategyRetentionLimits,
    decisions: StrategyDecisionRecorder,
    next_decision_sequence: u64,
    next_signal_sequence: u64,
    delivered_dispositions: usize,
    warmup_complete: bool,
    failure: Option<StrategyDriverError<S::Error>>,
}

impl<'a, S: HistoricalStrategy> StrategyReplayDriver<'a, S> {
    fn new(
        strategy: &'a mut S,
        series: MultiTimeframeSeries,
        analysis: AnalysisPipeline,
        limits: StrategyRetentionLimits,
    ) -> Self {
        Self {
            requirements: strategy.requirements().clone(),
            strategy,
            series,
            analysis,
            limits,
            decisions: StrategyDecisionRecorder::new(limits),
            next_decision_sequence: 0,
            next_signal_sequence: 0,
            delivered_dispositions: 0,
            warmup_complete: false,
            failure: None,
        }
    }

    fn finish(
        self,
    ) -> Result<crate::strategy::StrategyDecisionOutput, StrategyDriverError<S::Error>> {
        match self.failure {
            Some(error) => Err(error),
            None => Ok(self.decisions.finish()),
        }
    }

    fn fail(&mut self, error: StrategyDriverError<S::Error>) -> Option<Vec<ScheduledSignal>> {
        self.failure = Some(error);
        None
    }
}

impl<S: HistoricalStrategy> FutureReplayHook for StrategyReplayDriver<'_, S> {
    fn is_active(&self) -> bool {
        true
    }

    fn output_ready(&self) -> bool {
        self.warmup_complete
    }

    fn preflight_primary_events(&mut self, events: &[FeedEvent]) -> bool {
        if self.requirements.needs_tick_execution()
            && let Some(event) = events
                .iter()
                .find(|event| matches!(event.event, MarketEvent::Bar { .. }))
        {
            self.failure = Some(StrategyDriverError::TickExecutionRequired {
                symbol: event.event.symbol().to_owned(),
                timestamp: event.event.ts(),
            });
            return false;
        }
        true
    }

    fn reject_generated_configuration(&mut self, reason: String) {
        self.failure = Some(StrategyDriverError::InvalidGeneratedSignal {
            signal_index: 0,
            reason,
        });
    }

    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        pending_effects: &mut Vec<FutureEffect>,
    ) -> Option<Vec<ScheduledSignal>> {
        let closed_bars = match self.series.on_batch(batch) {
            Ok(bars) => bars,
            Err(error) => return self.fail(StrategyDriverError::Series(error)),
        };
        let boundary = AnalysisBoundary::new(batch.ts, &closed_bars, &self.series);
        let observations = match self.analysis.on_boundary(boundary) {
            Ok(output) => output.observations().to_vec(),
            Err(error) => return self.fail(StrategyDriverError::Analysis(error)),
        };
        self.warmup_complete = match self.series.warmup_complete(&self.requirements) {
            Ok(complete) => complete,
            Err(error) => return self.fail(StrategyDriverError::SeriesView(error)),
        };
        let disposition_end = lifecycle.len();
        let feedback = StrategyFeedback::new(
            pending_effects,
            &lifecycle.as_slice()[self.delivered_dispositions..disposition_end],
        );
        let event = StrategyEvent::new(&batch.events, &closed_bars, &observations, feedback);
        let context = StrategyContext::new(
            batch.ts,
            &self.series,
            self.analysis.observations(),
            engine,
            self.warmup_complete,
        );
        let output = match self.strategy.on_event(event, context) {
            Ok(output) => output,
            Err(error) => return self.fail(StrategyDriverError::Strategy(error)),
        };
        pending_effects.clear();
        self.delivered_dispositions = disposition_end;

        let Some(draft) = output.into_decision() else {
            return Some(Vec::new());
        };
        let record = match draft.into_record(self.next_decision_sequence, batch.ts, self.limits) {
            Ok(record) => record,
            Err(error) => return self.fail(StrategyDriverError::Runtime(error)),
        };
        if !self.warmup_complete && !record.emitted_signals().is_empty() {
            return self.fail(StrategyDriverError::WarmupSignals {
                timestamp: batch.ts,
            });
        }
        if let Some((signal_index, error)) =
            record
                .emitted_signals()
                .iter()
                .enumerate()
                .find_map(|(index, signal)| {
                    qs_core::validation::validate_raw_signal(signal)
                        .err()
                        .map(|error| (index, error))
                })
        {
            return self.fail(StrategyDriverError::InvalidGeneratedSignal {
                signal_index,
                reason: error.to_string(),
            });
        }
        let effective_ts = match self.requirements.effective_timestamp(batch.ts) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                return self.fail(StrategyDriverError::Runtime(
                    crate::strategy::StrategyRuntimeError::Domain(error),
                ));
            }
        };
        let signals = match self.decisions.push(record) {
            Ok(signals) => signals,
            Err(error) => {
                return self.fail(StrategyDriverError::Runtime(
                    crate::strategy::StrategyRuntimeError::Domain(error),
                ));
            }
        };
        self.next_decision_sequence = match self.next_decision_sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                return self.fail(StrategyDriverError::Runtime(
                    crate::strategy::StrategyRuntimeError::Domain(
                        crate::strategy::StrategyDomainError::OmittedCounterOverflow,
                    ),
                ));
            }
        };
        let mut scheduled = Vec::with_capacity(signals.len());
        for signal in signals {
            let sequence = self.next_signal_sequence;
            self.next_signal_sequence = match self.next_signal_sequence.checked_add(1) {
                Some(sequence) => sequence,
                None => {
                    return self.fail(StrategyDriverError::Runtime(
                        crate::strategy::StrategyRuntimeError::Domain(
                            crate::strategy::StrategyDomainError::OmittedCounterOverflow,
                        ),
                    ));
                }
            };
            scheduled.push(ScheduledSignal {
                sequence,
                signal_ts: batch.ts,
                effective_ts,
                signal,
                requires_later_quote: true,
            });
        }
        Some(scheduled)
    }
}

/// Stage at which an exact FutureQuote equity observation was made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EquityObservationKind {
    PreSettlement,
    PostOutput,
    ConversionRevaluation,
    QuiescentTermination,
    EndOfData,
}

impl EquityObservationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreSettlement => "pre_settlement",
            Self::PostOutput => "post_output",
            Self::ConversionRevaluation => "conversion_revaluation",
            Self::QuiescentTermination => "quiescent_termination",
            Self::EndOfData => "end_of_data",
        }
    }
}

struct BufferedFutureFeed {
    events: VecDeque<FeedEvent>,
    total_events: usize,
}

impl BufferedFutureFeed {
    fn new(events: Vec<FeedEvent>) -> Self {
        let total_events = events.len();
        Self {
            events: VecDeque::from(events),
            total_events,
        }
    }
}

impl DataFeed for BufferedFutureFeed {
    fn next_event(&mut self) -> Option<MarketEvent> {
        self.events.pop_front().map(|event| event.event)
    }

    fn peek(&self) -> Option<&MarketEvent> {
        self.events.front().map(|event| &event.event)
    }

    fn next_batch(&mut self) -> Option<TimestampBatch> {
        let ts = self.events.front()?.event.ts();
        let mut events = Vec::new();
        while self
            .events
            .front()
            .is_some_and(|event| event.event.ts() == ts)
        {
            events.push(self.events.pop_front().expect("front checked"));
        }
        Some(TimestampBatch { ts, events })
    }

    fn total_events(&self) -> Option<usize> {
        Some(self.total_events)
    }
}

struct DataFeedBatchAdapter<'a, F> {
    feed: &'a mut F,
}

impl<F: DataFeed> FallibleBatchFeed for DataFeedBatchAdapter<'_, F> {
    type Error = Infallible;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Ok(self.feed.next_batch())
    }
}

/// Configuration for a backtest run.
#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// Starting account balance.
    pub initial_balance: f64,
    /// If `true`, all remaining open positions are closed at market when the
    /// data feed is exhausted.
    pub close_on_finish: bool,
    /// How fill conditions and rule triggers interpret price quotes.
    ///
    /// Defaults to [`FillModel::BidAsk`] — the most realistic model that
    /// uses the appropriate side of the spread for each operation.
    pub fill_model: FillModel,
    /// Per-symbol contract size (point value) for P&L calculation.
    ///
    /// Maps symbol name → contract size.  For forex, this is typically
    /// `lot_base_units` from `SymbolSpec` (e.g. 100_000 for majors).
    /// For gold (XAUUSD) it's 100 (1 lot = 100 oz).
    ///
    /// When a symbol is absent from this map the multiplier defaults to `1.0`,
    /// which preserves backward compatibility with all existing tests.
    pub contract_sizes: HashMap<String, f64>,
    /// Optional position sizing policy.  When set, entry signal sizes are
    /// recalculated after profile transformation using symbol metadata.
    pub sizing: Option<SizingPolicy>,
    /// Symbol specs for sizing calculations.  Populated by the server from
    /// the symbol registry.  Empty when no sizing policy is configured.
    pub symbol_specs: HashMap<String, qs_symbols::SymbolSpec>,
    /// Optional explicit instrument specifications and stored-series bindings pinned for this run.
    pub instrument_manifest: Option<ReplayInstrumentManifest>,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_balance: 10_000.0,
            close_on_finish: true,
            fill_model: FillModel::default(),
            contract_sizes: HashMap::new(),
            sizing: None,
            symbol_specs: HashMap::new(),
            instrument_manifest: None,
        }
    }
}

/// Orchestrates a backtest by driving the engine with data and actions.
pub struct BacktestRunner {
    engine: TradeEngine,
    executor: BacktestExecutor,
    config: BacktestConfig,
    future_config: Option<FutureQuoteConfig>,
    evaluation_options: EvaluationOptions,
    instrument_sizing: Vec<InstrumentSizingArtifact>,
    committed_feedback: Vec<FutureEffect>,
}

impl BacktestRunner {
    /// Create a new runner with the given configuration.
    pub fn new(config: BacktestConfig) -> Self {
        let executor =
            BacktestExecutor::new(config.initial_balance, effective_contract_sizes(&config));
        Self {
            engine: TradeEngine::with_fill_model(config.fill_model),
            executor,
            config,
            future_config: None,
            evaluation_options: EvaluationOptions::default(),
            instrument_sizing: Vec::new(),
            committed_feedback: Vec::new(),
        }
    }

    /// Create a runner using deterministic FutureQuoteV1 scheduling and pricing.
    pub fn new_future(config: BacktestConfig, future_config: FutureQuoteConfig) -> Self {
        let executor =
            BacktestExecutor::new(config.initial_balance, effective_contract_sizes(&config));
        let engine = TradeEngine::with_fill_model_and_deterministic_ids(config.fill_model);
        Self {
            engine,
            executor,
            config,
            future_config: Some(future_config),
            evaluation_options: EvaluationOptions::default(),
            instrument_sizing: Vec::new(),
            committed_feedback: Vec::new(),
        }
    }

    /// Create a runner with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BacktestConfig::default())
    }

    /// Apply typed provider-evaluation options to FutureQuoteV1 results.
    /// Legacy execution ignores these options and preserves its existing report.
    pub fn with_evaluation_options(mut self, options: EvaluationOptions) -> Self {
        self.evaluation_options = options;
        self
    }

    /// Access the underlying engine (e.g. for inspection between runs).
    pub fn engine(&self) -> &TradeEngine {
        &self.engine
    }

    /// Access the underlying executor.
    pub fn executor(&self) -> &BacktestExecutor {
        &self.executor
    }

    // ── Mode 1: Strategy-driven ─────────────────────────────────────────

    /// Run a strategy-driven backtest.
    ///
    /// For every event in the data feed:
    /// 1. The event is converted to a [`PriceQuote`] and fed to the engine
    ///    (which checks pending fills and evaluates rules).
    /// 2. The strategy's [`on_event`](Strategy::on_event) is called; any
    ///    returned actions are applied to the engine.
    /// 3. All resulting effects are forwarded to the executor for P&L tracking.
    ///
    /// When the feed is exhausted, [`Strategy::on_finished`] is called for any
    /// final actions, and (if configured) remaining positions are closed.
    pub fn run_strategy<F: DataFeed, S: Strategy>(
        mut self,
        feed: &mut F,
        strategy: &mut S,
    ) -> BacktestResult {
        if validate_replay_config(&self.config, None, &[]).is_err() {
            return rejected_legacy_result(&self.config);
        }
        let mut last_quote_ts = BTreeMap::new();
        while let Some(event) = feed.next_event() {
            let quote = event.to_quote();
            if !accept_legacy_quote(&quote, &mut last_quote_ts) {
                continue;
            }

            // 1. Feed price to engine → pending fills + rule evaluation.
            let price_effects = self.engine.on_price(&quote);
            self.executor
                .process_effects(&price_effects, &self.engine, &quote);

            // 2. Strategy decides actions based on the event.
            let actions = strategy.on_event(&event);
            self.apply_actions(actions, &quote);
        }

        // 3. Strategy cleanup.
        let final_actions = strategy.on_finished();
        if !final_actions.is_empty() {
            // Use the last known quote for the final actions.  If we have
            // nothing, create a dummy — but in practice the feed will have
            // produced at least one event.
            if let Some(last_quote) = self.last_available_quote() {
                self.apply_actions(final_actions, &last_quote);
                // One more price tick so rules can fire after final actions.
                let effects = self.engine.on_price(&last_quote);
                self.executor
                    .process_effects(&effects, &self.engine, &last_quote);
            }
        }

        // 4. Force-close remaining if configured.
        self.close_remaining_if_configured();

        BacktestResult::from_trade_log(self.config.initial_balance, self.executor.trade_log)
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /// Apply a batch of actions to the engine and forward effects to executor.
    fn apply_actions(&mut self, actions: Vec<Action>, quote: &PriceQuote) {
        for action in actions {
            self.apply_single_action(action, quote.ts, quote);
        }
    }

    /// Apply a single action, forwarding effects to the executor.
    ///
    /// For close effects the executor resolves the position symbol and uses
    /// the engine's last known quote for that symbol rather than blindly
    /// trusting the caller-supplied `quote`.  This prevents cross-symbol
    /// quote contamination in merged multi-symbol feeds.
    fn apply_single_action(
        &mut self,
        action: Action,
        ts: chrono::NaiveDateTime,
        quote: &PriceQuote,
    ) {
        match self.engine.apply_action(action, ts) {
            Ok(effects) => {
                self.executor.process_effects(&effects, &self.engine, quote);
            }
            Err(_) => {
                // In backtesting we silently skip invalid actions (e.g.
                // trying to close a position that was already closed by SL).
                // A more sophisticated implementation could log these.
            }
        }
    }

    /// Try to find the last known quote from the engine (any symbol).
    fn last_available_quote(&self) -> Option<PriceQuote> {
        // Look up quotes for symbols that have open positions first, then
        // fall back to any known quote.
        for pos in self.engine.open_positions() {
            if let Some(q) = self.engine.last_quote(&pos.data.symbol) {
                return Some(q.clone());
            }
        }
        // No open positions — try closed ones.
        for pos in self.engine.closed_positions() {
            if let Some(q) = self.engine.last_quote(&pos.data.symbol) {
                return Some(q.clone());
            }
        }
        None
    }

    // ── Mode 3: Raw signal replay ───────────────────────────────────────

    /// Run a raw-signal-replay backtest.
    ///
    /// `raw_signals` must be **sorted by timestamp** (ascending).  Entry
    /// signals are optionally transformed through a [`ManagementProfile`],
    /// while management signals are resolved against live engine state and
    /// passed through directly.
    pub fn run_raw_signals<F: DataFeed>(
        self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
    ) -> BacktestResult {
        self.run_raw_signals_controlled(feed, raw_signals, profile, || false, |_| {})
            .expect("non-cancellable replay cannot be cancelled")
    }

    /// Run a raw-signal replay with cooperative cancellation and progress updates.
    ///
    /// Cancellation is checked at every market-event and signal boundary. Progress
    /// callbacks are rate-limited for long event streams while always reporting
    /// the initial and terminal counters.
    pub fn run_raw_signals_controlled<F, C, P>(
        mut self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        mut is_cancelled: C,
        mut on_progress: P,
    ) -> std::result::Result<BacktestResult, ReplayCancelled>
    where
        F: DataFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        if let Some(future_config) = self.future_config.clone() {
            return self.run_raw_signals_future_controlled(
                feed,
                raw_signals,
                profile,
                future_config,
                &mut is_cancelled,
                &mut on_progress,
            );
        }
        if validate_replay_config(&self.config, None, &raw_signals).is_err()
            || profile.is_some_and(|profile| profile.validate().is_err())
        {
            return Ok(rejected_legacy_result(&self.config));
        }

        let total_events = feed.total_events().unwrap_or(0);
        let total_signals = raw_signals.len();
        let mut processed_events = 0;
        let mut sig_idx = 0;
        let mut last_quote_ts = BTreeMap::new();
        on_progress(ReplayProgress {
            processed_events,
            total_events,
            processed_signals: sig_idx,
            total_signals,
        });

        while let Some(event) = feed.next_event() {
            if is_cancelled() {
                return Err(ReplayCancelled);
            }
            let quote = event.to_quote();
            if !accept_legacy_quote(&quote, &mut last_quote_ts) {
                processed_events += 1;
                if should_report_progress(processed_events, total_events) {
                    on_progress(ReplayProgress {
                        processed_events,
                        total_events,
                        processed_signals: sig_idx,
                        total_signals,
                    });
                }
                continue;
            }

            // 1. Inject raw signals that should fire at or before this event's ts.
            while sig_idx < raw_signals.len() && raw_signals[sig_idx].ts() <= event.ts() {
                if is_cancelled() {
                    return Err(ReplayCancelled);
                }
                self.process_raw_signal(&raw_signals[sig_idx], profile, &quote);
                sig_idx += 1;
                if should_report_progress(sig_idx, total_signals) {
                    on_progress(ReplayProgress {
                        processed_events,
                        total_events,
                        processed_signals: sig_idx,
                        total_signals,
                    });
                }
            }

            // 2. Feed price to engine.
            let effects = self.engine.on_price(&quote);
            self.executor
                .process_effects(&effects, &self.engine, &quote);
            processed_events += 1;
            if should_report_progress(processed_events, total_events) {
                on_progress(ReplayProgress {
                    processed_events,
                    total_events,
                    processed_signals: sig_idx,
                    total_signals,
                });
            }
        }

        // 3. Inject remaining signals (if any) after data is exhausted.
        if sig_idx < raw_signals.len()
            && let Some(last_quote) = self.last_available_quote()
        {
            while sig_idx < raw_signals.len() {
                if is_cancelled() {
                    return Err(ReplayCancelled);
                }
                self.process_raw_signal(&raw_signals[sig_idx], profile, &last_quote);
                sig_idx += 1;
                if should_report_progress(sig_idx, total_signals) {
                    on_progress(ReplayProgress {
                        processed_events,
                        total_events,
                        processed_signals: sig_idx,
                        total_signals,
                    });
                }
            }
            // One final price evaluation.
            let effects = self.engine.on_price(&last_quote);
            self.executor
                .process_effects(&effects, &self.engine, &last_quote);
        }

        if is_cancelled() {
            return Err(ReplayCancelled);
        }

        // 4. Force-close remaining if configured.
        self.close_remaining_if_configured();
        on_progress(ReplayProgress {
            processed_events,
            total_events,
            processed_signals: sig_idx,
            total_signals,
        });

        Ok(BacktestResult::from_trade_log(
            self.config.initial_balance,
            self.executor.trade_log,
        ))
    }

    /// Run raw signals with FutureQuoteV1 scheduling and execution.
    ///
    /// Quotes are validated, required to be nondecreasing per symbol in source
    /// order, and then globally stable-sorted by timestamp. Signals are sorted by
    /// effective time (`signal timestamp + latency`).
    pub fn run_raw_signals_future<F: DataFeed>(
        self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
    ) -> BacktestResult {
        let future_config = self.future_config.clone().unwrap_or_default();
        self.run_raw_signals_future_with_config(feed, raw_signals, profile, future_config)
    }

    /// Run FutureQuoteV1 directly from a fallible stream of complete timestamp batches.
    ///
    /// `primary_eod` must be determined before replay from valid primary quotes. The feed must already be globally ordered and preserve all events at a timestamp in one batch. Event totals are unknown until the stream terminates, so terminal progress reports `total_events == processed_events`.
    #[allow(clippy::too_many_arguments)]
    pub fn run_raw_signals_future_streaming_controlled<F, C, P>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        mut is_cancelled: C,
        mut on_progress: P,
    ) -> std::result::Result<BacktestResult, StreamingReplayError<F::Error>>
    where
        F: FallibleBatchFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        let future = self.future_config.clone().unwrap_or_default();
        self.future_config = Some(future.clone());
        if let Err(error) = validate_replay_config(&self.config, Some(&future), &raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
            ));
        }
        if let Some(profile) = profile
            && let Err(error) = profile.validate()
        {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error.to_string(),
            ));
        }

        let mut hook = StaticReplayHook;
        match self.run_raw_signals_future_batches(
            feed,
            primary_eod,
            raw_signals,
            profile,
            future,
            None,
            0,
            0,
            &mut is_cancelled,
            &mut on_progress,
            &mut hook,
        ) {
            Ok(result) => Ok(result),
            Err(FutureBatchReplayError::Feed(error)) => Err(StreamingReplayError::Feed(error)),
            Err(FutureBatchReplayError::Cancelled) => {
                Err(StreamingReplayError::Cancelled(ReplayCancelled))
            }
            Err(FutureBatchReplayError::Dynamic) => {
                unreachable!("static replay has no dynamic hook")
            }
        }
    }

    /// Consume a fallible stream of complete timestamp batches and run FutureQuoteV1.
    ///
    /// Feed errors are returned without being converted into a backtest result. Batches are buffered so global ordering and primary EOD semantics remain identical to the compatible [`DataFeed`] entry point.
    pub fn run_raw_signals_future_fallible<F: FallibleBatchFeed>(
        self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
    ) -> Result<BacktestResult, F::Error> {
        let future_config = self.future_config.clone().unwrap_or_default();
        self.run_raw_signals_future_fallible_with_config(feed, raw_signals, profile, future_config)
    }

    /// Consume a fallible timestamp-batch stream with explicit FutureQuote settings.
    pub fn run_raw_signals_future_fallible_with_config<F: FallibleBatchFeed>(
        self,
        feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        future: FutureQuoteConfig,
    ) -> Result<BacktestResult, F::Error> {
        if let Err(error) = validate_replay_config(&self.config, Some(&future), &raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
            ));
        }
        if let Some(profile) = profile
            && let Err(error) = profile.validate()
        {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error.to_string(),
            ));
        }

        let mut events = Vec::new();
        while let Some(batch) = FallibleBatchFeed::next_batch(feed)? {
            events.extend(batch.events);
        }
        let mut buffered_feed = BufferedFutureFeed::new(events);
        Ok(self.run_raw_signals_future_with_config(
            &mut buffered_feed,
            raw_signals,
            profile,
            future,
        ))
    }

    /// Run a historical strategy from a materialized data feed through FutureQuote.
    #[allow(clippy::too_many_arguments)]
    pub fn run_historical_strategy_future<F, S>(
        self,
        source_feed: &mut F,
        strategy: &mut S,
        series_specs: Vec<BarSeriesSpec>,
        analysis: AnalysisPipeline,
        retention: StrategyRetentionLimits,
        profile: Option<&ManagementProfile>,
    ) -> Result<StrategyBacktestResult, StrategyReplayError<Infallible, S::Error>>
    where
        F: DataFeed,
        S: HistoricalStrategy,
    {
        crate::strategy::replay::validate_series_specs(strategy.requirements(), &series_specs)?;
        MultiTimeframeSeries::new(series_specs.clone())?;
        let future = self.future_config.clone().unwrap_or_default();
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(StrategyReplayInputError::FutureQuote)?;
        if let Some(profile) = profile {
            profile
                .validate()
                .map_err(|error| StrategyReplayInputError::ManagementProfile(error.to_string()))?;
        }
        let mut ordered_events = Vec::new();
        let mut source_last_ts = BTreeMap::<String, NaiveDateTime>::new();
        while let Some(batch) = source_feed.next_batch() {
            for event in batch.events {
                let symbol = event.event.symbol().to_owned();
                let timestamp = event.event.ts();
                if source_last_ts
                    .get(&symbol)
                    .is_some_and(|previous| *previous > timestamp)
                {
                    continue;
                }
                source_last_ts.insert(symbol, timestamp);
                ordered_events.push(event);
            }
        }
        ordered_events.sort_by_key(FeedEvent::ordering_key);
        let primary_eod = ordered_events
            .iter()
            .filter(|event| event.metadata.roles.primary)
            .filter_map(|event| event.event.to_valid_quote())
            .map(|quote| quote.ts)
            .max();
        let mut ordered_feed = crate::data_feed::VecFeed::from_feed_events(ordered_events);
        let mut feed = DataFeedBatchAdapter {
            feed: &mut ordered_feed,
        };
        self.run_historical_strategy_future_streaming(
            &mut feed,
            primary_eod,
            strategy,
            series_specs,
            analysis,
            retention,
            profile,
        )
    }

    /// Run a historical strategy from complete ordered timestamp batches.
    #[allow(clippy::too_many_arguments)]
    pub fn run_historical_strategy_future_streaming<F, S>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        strategy: &mut S,
        series_specs: Vec<BarSeriesSpec>,
        analysis: AnalysisPipeline,
        retention: StrategyRetentionLimits,
        profile: Option<&ManagementProfile>,
    ) -> Result<StrategyBacktestResult, StrategyReplayError<F::Error, S::Error>>
    where
        F: FallibleBatchFeed,
        S: HistoricalStrategy,
    {
        crate::strategy::replay::validate_series_specs(strategy.requirements(), &series_specs)?;
        let future = self.future_config.clone().unwrap_or_default();
        self.future_config = Some(future.clone());
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(StrategyReplayInputError::FutureQuote)?;
        if let Some(profile) = profile {
            profile
                .validate()
                .map_err(|error| StrategyReplayInputError::ManagementProfile(error.to_string()))?;
        }
        let descriptor = strategy.descriptor().clone();
        let series = MultiTimeframeSeries::new(series_specs)?;
        let mut hook = StrategyReplayDriver::new(strategy, series, analysis, retention);
        let mut is_cancelled = || false;
        let mut on_progress = |_| {};
        let replay = match self.run_raw_signals_future_batches(
            feed,
            primary_eod,
            Vec::new(),
            profile,
            future,
            None,
            0,
            0,
            &mut is_cancelled,
            &mut on_progress,
            &mut hook,
        ) {
            Ok(replay) => replay,
            Err(FutureBatchReplayError::Feed(error)) => {
                return Err(StrategyReplayError::Feed(error));
            }
            Err(FutureBatchReplayError::Cancelled) => {
                unreachable!("strategy replay is not cancellable")
            }
            Err(FutureBatchReplayError::Dynamic) => {
                let error = hook.finish().expect_err("dynamic failure stores its cause");
                return Err(map_strategy_driver_error(error));
            }
        };
        let decisions = hook.finish().map_err(map_strategy_driver_error)?;
        Ok(StrategyBacktestResult {
            replay,
            descriptor,
            decisions,
        })
    }

    /// Process a single raw signal: entry signals go through profile transform,
    /// management signals are resolved against live engine state.
    fn process_raw_signal(
        &mut self,
        signal: &RawSignal,
        profile: Option<&ManagementProfile>,
        quote: &PriceQuote,
    ) {
        let ts = signal.ts();

        if signal.is_entry() {
            let mut signal = signal.clone();
            if let RawSignal::Entry {
                side,
                order_type: OrderType::Market,
                price,
                ..
            } = &mut signal
                && price.is_none()
            {
                *price = Some(match side {
                    Side::Buy => quote.ask,
                    Side::Sell => quote.bid,
                });
            }
            let resolved = match profile {
                Some(profile) => profile.apply_entry_signal(&signal),
                None => resolve_unprofiled_entry(&signal),
            };
            if let Ok(Some(resolved)) = resolved
                && let Ok(action) =
                    self.finalize_resolved_entry(resolved, self.executor.balance, ts, None)
            {
                self.apply_single_action(action, ts, quote);
            }
        } else {
            let actions = resolve_signal(signal, &self.engine);
            for action in actions {
                self.apply_single_action(action, ts, quote);
            }
        }
    }

    fn finalize_resolved_entry(
        &mut self,
        mut resolved: ResolvedEntry,
        balance_before: f64,
        operation_ts: NaiveDateTime,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Result<Action, String> {
        let policy = self
            .config
            .sizing
            .as_ref()
            .ok_or_else(|| "raw entry requires a sizing policy".to_owned())?;
        let entry_price = resolved.price.ok_or_else(|| {
            if resolved.order_type == OrderType::Market {
                "market entry requires an execution price".to_owned()
            } else {
                "pending entry requires a requested price".to_owned()
            }
        })?;
        let explicit_spec = explicit_instrument_spec(&self.config, &resolved.symbol);
        let legacy_spec = self.config.symbol_specs.get(&resolved.symbol);
        if explicit_spec.is_none() && legacy_spec.is_none() {
            return Err(format!(
                "missing instrument or symbol spec for {}",
                resolved.symbol
            ));
        }

        let (account_loss_per_lot, native_to_account_rate) = if is_monetary_sizing(policy) {
            let stop = resolved
                .stoploss
                .ok_or_else(|| "monetary sizing requires a protective stop".to_owned())?;
            let native_loss = match explicit_spec {
                Some(spec) => compute_instrument_native_loss_per_lot(
                    resolved.side,
                    entry_price,
                    stop,
                    u16::from(spec.price.display_scale),
                    &spec.economics,
                )
                .map_err(|error| error.to_string())?,
                None => compute_native_loss_per_lot(
                    resolved.side,
                    entry_price,
                    stop,
                    legacy_spec.expect("legacy spec presence checked"),
                )
                .map_err(|error| error.to_string())?,
            };
            let plan = self
                .future_config
                .as_ref()
                .and_then(|config| config.currency_plan.as_ref())
                .ok_or_else(|| "monetary sizing requires a FutureQuote currency plan".to_owned())?;
            let route = plan
                .route_for_primary_symbol(&resolved.symbol)
                .ok_or_else(|| {
                    format!(
                        "currency plan has no frozen route for primary symbol {}",
                        resolved.symbol
                    )
                })?;
            let converted = conversion_quotes
                .ok_or_else(|| "monetary sizing requires conversion quotes".to_owned())?
                .convert_route(-native_loss, operation_ts, route)
                .map_err(|error| error.to_string())?;
            let account_loss = -converted.output_amount;
            (Some(account_loss), Some(account_loss / native_loss))
        } else {
            (None, None)
        };

        let sizing = match explicit_spec {
            Some(spec) => compute_instrument_size_for_spec(
                policy,
                resolved.risk_multiplier,
                balance_before,
                resolved.side,
                entry_price,
                resolved.stoploss,
                spec,
                native_to_account_rate,
            )
            .map_err(|error| error.to_string())?,
            None => compute_size(
                policy,
                resolved.risk_multiplier,
                balance_before,
                resolved.side,
                entry_price,
                resolved.stoploss,
                legacy_spec.expect("legacy spec presence checked"),
                account_loss_per_lot,
            )
            .map_err(|error| error.to_string())?,
        };
        if let Some(quantity) = sizing.quantity_adjustment {
            self.instrument_sizing.push(InstrumentSizingArtifact {
                symbol: resolved.symbol.clone(),
                operation_ts,
                quantity,
                final_notional: sizing.final_notional.clone(),
            });
        }
        let target_steps = allocate_target_steps(
            sizing.final_lot_steps,
            &resolved.target_resolution.weights,
            resolved.target_resolution.remainder,
        )
        .map_err(|error| error.to_string())?;
        if target_steps.len() != resolved.targets.len() {
            return Err("target allocation does not match resolved targets".to_owned());
        }
        for (target, steps) in resolved.targets.iter_mut().zip(target_steps) {
            target.close_ratio = steps as f64 / sizing.final_lot_steps as f64;
        }

        Ok(resolved.into_action(sizing.final_lot))
    }

    // ── Internal helpers ────────────────────────────────────────────────
    // (continued)

    /// If `close_on_finish` is set, close all remaining open positions at
    /// their last known price.
    fn close_remaining_if_configured(&mut self) {
        if !self.config.close_on_finish {
            return;
        }

        let open_ids: Vec<String> = self
            .engine
            .open_positions()
            .iter()
            .map(|p| p.data.id.clone())
            .collect();

        for id in open_ids {
            let symbol = match self.engine.get_position(&id) {
                Some(pos) => pos.data.symbol.clone(),
                None => continue,
            };
            let quote = match self.engine.last_quote(&symbol) {
                Some(q) => q.clone(),
                None => continue,
            };

            if let Ok(effects) = self.engine.apply_action(
                Action::ClosePosition {
                    position_id: id.clone(),
                },
                quote.ts,
            ) {
                self.executor
                    .process_effects(&effects, &self.engine, &quote);
            }
        }
    }

    /// Run raw signals using deterministic FutureQuoteV1 execution.
    ///
    /// Fill-bearing actions never reuse a quote older than their effective
    /// timestamp. Signals are stably ordered by `(effective_ts, input_sequence)`;
    /// existing pending orders and rules win ties against signals at the exact
    /// quote timestamp.
    pub fn run_raw_signals_future_with_config<F: DataFeed>(
        self,
        source_feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        future: FutureQuoteConfig,
    ) -> BacktestResult {
        self.run_raw_signals_future_controlled(
            source_feed,
            raw_signals,
            profile,
            future,
            &mut || false,
            &mut |_| {},
        )
        .expect("non-cancellable replay cannot be cancelled")
    }

    fn run_raw_signals_future_controlled<F, C, P>(
        mut self,
        source_feed: &mut F,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        future: FutureQuoteConfig,
        is_cancelled: &mut C,
        on_progress: &mut P,
    ) -> std::result::Result<BacktestResult, ReplayCancelled>
    where
        F: DataFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        self.future_config = Some(future.clone());
        if let Err(error) = validate_replay_config(&self.config, Some(&future), &raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
            ));
        }
        if let Some(profile) = profile
            && let Err(error) = profile.validate()
        {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error.to_string(),
            ));
        }

        let total_events = source_feed.total_events();
        let mut processed_events = 0;
        let mut ordered_events = Vec::<FeedEvent>::new();
        let mut source_last_ts = BTreeMap::<String, NaiveDateTime>::new();
        let mut invalid_quotes = 0_u64;
        while let Some(batch) = source_feed.next_batch() {
            if is_cancelled() {
                return Err(ReplayCancelled);
            }
            for feed_event in batch.events {
                if is_cancelled() {
                    return Err(ReplayCancelled);
                }
                let symbol = feed_event.event.symbol().to_owned();
                let event_ts = feed_event.event.ts();
                if source_last_ts
                    .get(&symbol)
                    .is_some_and(|last| *last > event_ts)
                {
                    invalid_quotes += 1;
                    processed_events += 1;
                    continue;
                }
                source_last_ts.insert(symbol, event_ts);
                ordered_events.push(feed_event);
            }
        }
        if is_cancelled() {
            return Err(ReplayCancelled);
        }
        ordered_events.sort_by_key(FeedEvent::ordering_key);
        if is_cancelled() {
            return Err(ReplayCancelled);
        }
        let primary_eod = ordered_events
            .iter()
            .filter(|event| event.metadata.roles.primary)
            .filter_map(|event| event.event.to_valid_quote())
            .map(|quote| quote.ts)
            .max();
        let mut ordered_feed = crate::data_feed::VecFeed::from_feed_events(ordered_events);
        let mut feed = DataFeedBatchAdapter {
            feed: &mut ordered_feed,
        };
        let mut hook = StaticReplayHook;
        match self.run_raw_signals_future_batches(
            &mut feed,
            primary_eod,
            raw_signals,
            profile,
            future,
            total_events,
            processed_events,
            invalid_quotes,
            is_cancelled,
            on_progress,
            &mut hook,
        ) {
            Ok(result) => Ok(result),
            Err(FutureBatchReplayError::Cancelled) => Err(ReplayCancelled),
            Err(FutureBatchReplayError::Feed(error)) => match error {},
            Err(FutureBatchReplayError::Dynamic) => {
                unreachable!("static replay has no dynamic hook")
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_raw_signals_future_batches<F, C, P, H>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        raw_signals: Vec<RawSignal>,
        profile: Option<&ManagementProfile>,
        future: FutureQuoteConfig,
        known_total_events: Option<usize>,
        mut processed_events: usize,
        mut invalid_quotes: u64,
        is_cancelled: &mut C,
        on_progress: &mut P,
        hook: &mut H,
    ) -> std::result::Result<BacktestResult, FutureBatchReplayError<F::Error>>
    where
        F: FallibleBatchFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
        H: FutureReplayHook,
    {
        let total_events = known_total_events.unwrap_or(0);
        let total_signals = raw_signals.len();
        let mut processed_signals = 0;
        on_progress(ReplayProgress {
            processed_events,
            total_events,
            processed_signals,
            total_signals,
        });

        let execution_model = ExecutionModel::new(
            qs_core::types::ExecutionConvention::FutureQuoteV1,
            self.config.fill_model,
            if future.slippage_pips == 0.0 {
                SlippageModel::None
            } else {
                SlippageModel::FixedPips {
                    pips: future.slippage_pips,
                }
            },
        );
        let pricer = ExecutionPricer::new(execution_model);
        let mut scheduled: Vec<ScheduledSignal> = raw_signals
            .into_iter()
            .enumerate()
            .map(|(sequence, signal)| {
                let signal_ts = signal.ts();
                ScheduledSignal {
                    sequence: sequence as u64,
                    signal_ts,
                    effective_ts: signal_ts
                        .checked_add_signed(Duration::milliseconds(future.signal_latency_ms))
                        .expect("signal latency overflow was validated before scheduling"),
                    signal,
                    requires_later_quote: false,
                }
            })
            .collect();
        scheduled.sort_by_key(|signal| (signal.effective_ts, signal.sequence));
        let mut scheduled = VecDeque::from(scheduled);
        let mut queued = VecDeque::<QueuedAction>::new();
        let mut lifecycle = LifecycleLedger::new();
        let contract_sizes = effective_contract_sizes(&self.config);
        let mut future_executor = FutureExecutor::new(
            self.config.initial_balance,
            contract_sizes.clone(),
            future.pnl_epsilon,
        )
        .with_currency_plan(future.currency_plan.clone());
        let mut portfolio =
            PortfolioRecorder::new(self.config.initial_balance, contract_sizes.clone())
                .with_fill_model(self.config.fill_model)
                .with_stale_quote_after_millis(future.stale_quote_after_ms)
                .with_currency_plan(future.currency_plan.clone());
        let mut mtm_curve = MtmCurveCollector::new(future.mtm_output)
            .expect("MTM output policy was validated before replay");
        let mut last_mtm_candidate = None;
        let mut conversion_quotes =
            ConversionQuoteBook::new(Duration::milliseconds(future.conversion_stale_after_ms))
                .expect("conversion quote staleness was validated before replay");
        if let Some(plan) = future.currency_plan.as_ref() {
            for quote in plan.strict_before_warmup_quotes() {
                conversion_quotes
                    .record_canonical_tick(quote.clone())
                    .expect("currency plan warmups were validated during construction");
            }
        }
        let mut last_quote_ts = BTreeMap::<String, NaiveDateTime>::new();
        let mut last_processed_primary_ts = None;
        let mut effective_terminal_ts = primary_eod;
        let mut terminated_quiescently = false;

        while let Some(mut batch) =
            FallibleBatchFeed::next_batch(feed).map_err(FutureBatchReplayError::Feed)?
        {
            let batch_ts = batch.ts;
            if is_cancelled() {
                return Err(FutureBatchReplayError::Cancelled);
            }
            batch
                .events
                .sort_by_key(|event| (event.metadata.series_rank, event.metadata.row_sequence));
            let mut accepted = Vec::new();
            for feed_event in batch.events {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                let quote = feed_event.event.to_quote();
                if ExecutionPricer::validate_quote(&quote).is_err()
                    || last_quote_ts
                        .get(&quote.symbol)
                        .is_some_and(|last| *last > quote.ts)
                {
                    invalid_quotes += 1;
                    processed_events += 1;
                    if should_report_progress(processed_events, total_events) {
                        on_progress(ReplayProgress {
                            processed_events,
                            total_events,
                            processed_signals,
                            total_signals,
                        });
                    }
                    continue;
                }
                last_quote_ts.insert(quote.symbol.clone(), quote.ts);
                accepted.push((feed_event, quote));
            }
            let accepted_primary_events = accepted
                .iter()
                .filter(|(event, _)| event.metadata.roles.primary)
                .map(|(event, _)| event.clone())
                .collect::<Vec<_>>();
            if !hook.preflight_primary_events(&accepted_primary_events) {
                return Err(FutureBatchReplayError::Dynamic);
            }

            for (feed_event, quote) in &accepted {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                if feed_event.metadata.roles.conversion
                    && matches!(feed_event.event, MarketEvent::Tick { .. })
                    && conversion_quotes
                        .record_canonical_tick(quote.clone())
                        .is_err()
                {
                    invalid_quotes += 1;
                }
            }
            let valuation_only = accepted
                .iter()
                .any(|event| event.0.metadata.roles.conversion)
                && !accepted.iter().any(|event| event.0.metadata.roles.primary)
                && primary_eod.is_some_and(|eod| batch_ts <= eod);

            let mut primary_quotes = Vec::new();
            let mut primary_events = Vec::new();
            let mut batch_quotes = BTreeMap::new();
            for (feed_event, quote) in accepted {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                if !feed_event.metadata.roles.primary {
                    processed_events += 1;
                    if should_report_progress(processed_events, total_events) {
                        on_progress(ReplayProgress {
                            processed_events,
                            total_events,
                            processed_signals,
                            total_signals,
                        });
                    }
                    continue;
                }

                last_processed_primary_ts = Some(quote.ts);
                primary_events.push(feed_event);
                portfolio.record_quote(quote.clone());
                if hook.output_ready() {
                    observe_future_equity(
                        &mut portfolio,
                        &future_executor,
                        quote.ts,
                        &conversion_quotes,
                        EquityObservationKind::PreSettlement,
                        &mut mtm_curve,
                        &mut last_mtm_candidate,
                        false,
                    );
                }
                batch_quotes.insert(quote.symbol.clone(), quote.clone());
                primary_quotes.push(quote);
            }

            if let Some(representative_quote) = primary_quotes.first() {
                let mut settled_quotes = vec![false; primary_quotes.len()];

                // Actions waiting from an earlier batch keep their stable queue order and use the matching quote from this batch.
                self.execute_queued_future(
                    &batch_quotes,
                    false,
                    &mut queued,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                );
                let increasing_symbols =
                    queued_exposure_symbols(&queued, &batch_quotes, representative_quote.ts);
                invalid_quotes += self.settle_future_batch_symbols(
                    &primary_quotes,
                    &mut settled_quotes,
                    Some(&increasing_symbols),
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                );
                self.execute_queued_future(
                    &batch_quotes,
                    true,
                    &mut queued,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                );

                while scheduled
                    .front()
                    .is_some_and(|signal| signal.effective_ts < representative_quote.ts)
                {
                    if is_cancelled() {
                        return Err(FutureBatchReplayError::Cancelled);
                    }
                    let signal = scheduled.pop_front().expect("front checked");
                    self.schedule_future_signal(
                        signal,
                        profile,
                        representative_quote,
                        &batch_quotes,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                    processed_signals += 1;
                    if should_report_progress(processed_signals, total_signals) {
                        on_progress(ReplayProgress {
                            processed_events,
                            total_events,
                            processed_signals,
                            total_signals,
                        });
                    }

                    self.execute_queued_future(
                        &batch_quotes,
                        false,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                    let increasing_symbols =
                        queued_exposure_symbols(&queued, &batch_quotes, representative_quote.ts);
                    invalid_quotes += self.settle_future_batch_symbols(
                        &primary_quotes,
                        &mut settled_quotes,
                        Some(&increasing_symbols),
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                    self.execute_queued_future(
                        &batch_quotes,
                        true,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                }

                // Every primary quote settles before any exact-time signal can resolve or execute.
                invalid_quotes += self.settle_future_batch_symbols(
                    &primary_quotes,
                    &mut settled_quotes,
                    None,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                );

                while scheduled
                    .front()
                    .is_some_and(|signal| signal.effective_ts == representative_quote.ts)
                {
                    if is_cancelled() {
                        return Err(FutureBatchReplayError::Cancelled);
                    }
                    let signal = scheduled.pop_front().expect("front checked");
                    self.schedule_future_signal(
                        signal,
                        profile,
                        representative_quote,
                        &batch_quotes,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                    processed_signals += 1;
                    if should_report_progress(processed_signals, total_signals) {
                        on_progress(ReplayProgress {
                            processed_events,
                            total_events,
                            processed_signals,
                            total_signals,
                        });
                    }
                    self.execute_queued_future(
                        &batch_quotes,
                        false,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                    self.execute_queued_future(
                        &batch_quotes,
                        true,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                }
            }

            let strategy_batch = TimestampBatch {
                ts: batch_ts,
                events: primary_events,
            };
            let generated = hook
                .on_boundary(
                    &strategy_batch,
                    &self.engine,
                    &lifecycle,
                    &mut self.committed_feedback,
                )
                .ok_or(FutureBatchReplayError::Dynamic)?;
            if !generated.is_empty() {
                let generated_signals = generated
                    .iter()
                    .map(|scheduled| scheduled.signal.clone())
                    .collect::<Vec<_>>();
                if let Err(error) =
                    validate_replay_config(&self.config, Some(&future), &generated_signals)
                {
                    hook.reject_generated_configuration(error);
                    return Err(FutureBatchReplayError::Dynamic);
                }
            }
            for generated_signal in generated {
                if generated_signal.effective_ts <= batch_ts
                    && let Some(representative_quote) = primary_quotes.first()
                {
                    self.schedule_future_signal(
                        generated_signal,
                        profile,
                        representative_quote,
                        &batch_quotes,
                        &mut queued,
                        &mut lifecycle,
                        &mut future_executor,
                        &mut portfolio,
                        &pricer,
                        &conversion_quotes,
                    );
                } else {
                    scheduled.push_back(generated_signal);
                }
            }

            for quote in &primary_quotes {
                if !hook.output_ready() {
                    processed_events += 1;
                    continue;
                }
                observe_future_equity(
                    &mut portfolio,
                    &future_executor,
                    quote.ts,
                    &conversion_quotes,
                    EquityObservationKind::PostOutput,
                    &mut mtm_curve,
                    &mut last_mtm_candidate,
                    true,
                );
                processed_events += 1;
                if should_report_progress(processed_events, total_events) {
                    on_progress(ReplayProgress {
                        processed_events,
                        total_events,
                        processed_signals,
                        total_signals,
                    });
                }
            }
            if valuation_only && hook.output_ready() {
                observe_future_equity(
                    &mut portfolio,
                    &future_executor,
                    batch_ts,
                    &conversion_quotes,
                    EquityObservationKind::ConversionRevaluation,
                    &mut mtm_curve,
                    &mut last_mtm_candidate,
                    false,
                );
            }
            conversion_quotes.retain_replay_causal_predecessors(
                batch_ts,
                scheduled
                    .iter()
                    .map(|signal| signal.effective_ts)
                    .chain(primary_eod),
            );

            if !hook.is_active()
                && last_processed_primary_ts.is_some()
                && scheduled.is_empty()
                && queued.is_empty()
                && self.engine.open_positions().is_empty()
                && self.engine.pending_positions().is_empty()
            {
                effective_terminal_ts = last_processed_primary_ts;
                terminated_quiescently = true;
                break;
            }
        }

        for action in queued {
            if is_cancelled() {
                return Err(FutureBatchReplayError::Cancelled);
            }
            let mut disposition =
                ActionDisposition::rejected(action.action_id, "no_eligible_quote");
            disposition.action_kind = Some(action.action_kind);
            disposition.signal_ts = Some(action.signal_ts);
            disposition.effective_ts = Some(action.effective_ts);
            let _ = lifecycle.record(disposition);
        }
        for signal in scheduled {
            if is_cancelled() {
                return Err(FutureBatchReplayError::Cancelled);
            }
            let mut disposition = ActionDisposition::rejected(
                format!("signal:{:08}", signal.sequence),
                "no_eligible_quote",
            );
            disposition.action_kind = Some(raw_signal_kind(&signal.signal).to_owned());
            disposition.signal_ts = Some(signal.signal_ts);
            disposition.effective_ts = Some(signal.effective_ts);
            let _ = lifecycle.record(disposition);
            processed_signals += 1;
            if should_report_progress(processed_signals, total_signals) {
                on_progress(ReplayProgress {
                    processed_events,
                    total_events,
                    processed_signals,
                    total_signals,
                });
            }
        }

        if is_cancelled() {
            return Err(FutureBatchReplayError::Cancelled);
        }

        if self.config.close_on_finish {
            let execution_ts = effective_terminal_ts;
            let ids: Vec<String> = future_executor
                .open_snapshots()
                .into_iter()
                .map(|position| position.position_id)
                .collect();
            for (sequence, id) in ids.into_iter().enumerate() {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                let Some(symbol) = self
                    .engine
                    .get_position(&id)
                    .map(|position| position.data.symbol.clone())
                else {
                    continue;
                };
                let Some(quote) = portfolio.quote(&symbol).cloned() else {
                    continue;
                };
                let action_id = format!("end_of_data:{sequence:08}");
                let transaction = (|| -> Result<_, FutureTransactionError> {
                    let side = self
                        .engine
                        .get_position(&id)
                        .ok_or_else(|| {
                            FutureApplyError::Core(qs_core::CoreError::PositionNotFound(id.clone()))
                        })?
                        .data
                        .side;
                    let execution = pricer
                        .market_exit(side, &quote, self.pip_size(&symbol))
                        .map_err(FutureApplyError::from)?;
                    let engine_transaction =
                        self.engine.begin_close_position_with_reason_future_at(
                            &id,
                            CloseReason::EndOfData,
                            &quote,
                            execution,
                            execution_ts.unwrap_or(quote.ts),
                        )?;
                    let affected =
                        if FutureExecutor::requires_processing(engine_transaction.effects()) {
                            match future_executor.process_future_effects_with_currency(
                                engine_transaction.effects(),
                                &self.engine,
                                &quote,
                                Some(&action_id),
                                None,
                                execution_ts.unwrap_or(quote.ts),
                                &mut portfolio,
                                Some(&conversion_quotes),
                            ) {
                                Ok(affected) => affected,
                                Err(error) => {
                                    engine_transaction.rollback(&mut self.engine);
                                    return Err(error.into());
                                }
                            }
                        } else {
                            Vec::new()
                        };
                    let _ = engine_transaction.commit();
                    Ok(affected)
                })();

                let mut disposition = match transaction {
                    Ok(affected) => {
                        let mut disposition = ActionDisposition::applied(action_id);
                        disposition.position_ids = affected;
                        disposition
                    }
                    Err(error) => ActionDisposition::failed(action_id, error.to_string()),
                };
                disposition.action_kind = Some("end_of_data".into());
                disposition.effective_ts = Some(execution_ts.unwrap_or(quote.ts));
                let _ = lifecycle.record(disposition);
            }
        }

        if is_cancelled() {
            return Err(FutureBatchReplayError::Cancelled);
        }

        if let Some(ts) = effective_terminal_ts
            && hook.output_ready()
        {
            let observation_kind = if terminated_quiescently {
                EquityObservationKind::QuiescentTermination
            } else {
                EquityObservationKind::EndOfData
            };
            observe_future_equity(
                &mut portfolio,
                &future_executor,
                ts,
                &conversion_quotes,
                observation_kind,
                &mut mtm_curve,
                &mut last_mtm_candidate,
                false,
            );
            future_executor.finalize_pending_orders_at_end(ts);
        }

        let pending_orders = self
            .engine
            .pending_positions()
            .into_iter()
            .map(|position| {
                let metadata = future_executor.pending_metadata(&position.data.id);
                PendingOrderSnapshot {
                    position_id: position.data.id.clone(),
                    action_id: metadata.as_ref().map(|value| value.0.clone()),
                    signal_ts: metadata.as_ref().map(|value| value.1),
                    effective_ts: metadata.as_ref().map(|value| value.2),
                    symbol: position.data.symbol.clone(),
                    side: position.data.side,
                    order_type: position.data.order_type,
                    requested_price: position.data.pending_price,
                    size: position.data.size,
                    initial_stop: position.current_stoploss(),
                    group: position.data.group.clone(),
                    trade_id: position.data.trade_id.clone(),
                }
            })
            .collect();
        let mut tags = BTreeMap::new();
        tags.insert("invalid_quote_count".into(), invalid_quotes.to_string());
        tags.insert(
            "termination_reason".into(),
            if terminated_quiescently {
                "quiescent"
            } else {
                "end_of_data"
            }
            .into(),
        );
        insert_economic_support_metadata(&mut tags, &self.config);
        let (equity_curve, mtm_output_summary) = mtm_curve.into_parts();
        let artifacts = FutureBacktestArtifacts {
            format_version: FUTURE_ARTIFACT_FORMAT_VERSION,
            execution: ExecutionMetadata {
                execution_model,
                initial_balance: self.config.initial_balance,
                account_currency: future
                    .currency_plan
                    .as_ref()
                    .map(|plan| plan.account_currency().to_owned()),
                currency_plan: future.currency_plan.clone(),
                contract_sizes: contract_sizes.into_iter().collect(),
                instrument_manifest: self.config.instrument_manifest.clone(),
                instrument_sizing: std::mem::take(&mut self.instrument_sizing),
                stale_quote_after_millis: future.stale_quote_after_ms,
                pnl_epsilon: future.pnl_epsilon,
                tags,
                ..ExecutionMetadata::default()
            },
            fills: future_executor.fills.clone(),
            close_events: future_executor.close_events.clone(),
            completed_positions: future_executor.completed_positions.clone(),
            open_positions: portfolio.latest_open_positions().to_vec(),
            pending_orders,
            pending_order_lifecycle: future_executor.pending_order_lifecycle,
            lifecycle,
            equity_curve,
            mtm_output_summary,
            max_drawdown: portfolio.max_drawdown(),
            max_drawdown_pct: portfolio.max_drawdown_pct(),
        };
        on_progress(ReplayProgress {
            processed_events,
            total_events: known_total_events.unwrap_or(processed_events),
            processed_signals,
            total_signals,
        });
        Ok(BacktestResult::from_future_artifacts_with_options(
            artifacts,
            self.evaluation_options,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn settle_future_batch_symbols(
        &mut self,
        primary_quotes: &[PriceQuote],
        settled_quotes: &mut [bool],
        symbols: Option<&BTreeSet<String>>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> u64 {
        let mut failures = 0;
        for (index, quote) in primary_quotes.iter().enumerate() {
            if settled_quotes[index]
                || symbols.is_some_and(|symbols| !symbols.contains(&quote.symbol))
            {
                continue;
            }
            if self
                .settle_future_quote(
                    quote,
                    lifecycle,
                    future_executor,
                    portfolio,
                    pricer,
                    conversion_quotes,
                )
                .is_err()
            {
                failures += 1;
            }
            settled_quotes[index] = true;
        }
        failures
    }

    #[allow(clippy::too_many_arguments)]
    fn settle_future_quote(
        &mut self,
        quote: &PriceQuote,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> Result<(), FutureTransactionError> {
        let (prepared, failures) = self.prepare_triggering_pending(quote, pricer);
        for (position_id, error) in failures {
            let action_id = format!("pending_execution:{position_id}");
            let mut disposition = ActionDisposition::rejected(action_id.clone(), error);
            disposition.action_kind = Some("pending_execution".into());
            disposition.effective_ts = Some(quote.ts);
            disposition.position_ids.push(position_id.clone());
            let _ = lifecycle.record(disposition);
            if let Ok(engine_transaction) = self.engine.begin_future_action(
                Action::CancelPending {
                    position_id: position_id.clone(),
                },
                quote.ts,
            ) {
                let committed_effects = engine_transaction.effects().to_vec();
                if FutureExecutor::requires_processing(engine_transaction.effects())
                    && let Err(error) = future_executor.process_future_effects_with_currency(
                        engine_transaction.effects(),
                        &self.engine,
                        quote,
                        Some(&action_id),
                        None,
                        quote.ts,
                        portfolio,
                        Some(conversion_quotes),
                    )
                {
                    engine_transaction.rollback(&mut self.engine);
                    return Err(error.into());
                }
                let _ = engine_transaction.commit();
                self.committed_feedback.extend(committed_effects);
            }
        }

        let pip_size = self.pip_size(&quote.symbol);
        let engine_transaction = self
            .engine
            .begin_on_price_future_effects_priced(quote, &prepared, pricer, pip_size)?;
        let committed_effects = engine_transaction.effects().to_vec();
        if FutureExecutor::requires_processing(engine_transaction.effects())
            && let Err(error) = future_executor.process_future_effects_with_currency(
                engine_transaction.effects(),
                &self.engine,
                quote,
                None,
                None,
                quote.ts,
                portfolio,
                Some(conversion_quotes),
            )
        {
            engine_transaction.rollback(&mut self.engine);
            return Err(error.into());
        }
        let _ = engine_transaction.commit();
        self.committed_feedback.extend(committed_effects);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_future_signal(
        &mut self,
        scheduled: ScheduledSignal,
        profile: Option<&ManagementProfile>,
        quote: &PriceQuote,
        batch_quotes: &BTreeMap<String, PriceQuote>,
        queued: &mut VecDeque<QueuedAction>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) {
        let base_id = format!("signal:{:08}", scheduled.sequence);
        if let RawSignal::Entry {
            symbol,
            side,
            order_type: OrderType::Market,
            ..
        } = &scheduled.signal
        {
            queued.push_back(QueuedAction {
                action_id: base_id,
                action_kind: "entry".into(),
                action: Action::Open {
                    symbol: symbol.clone(),
                    side: *side,
                    order_type: OrderType::Market,
                    price: None,
                    size: 1.0,
                    stoploss: None,
                    targets: Vec::new(),
                    rules: Vec::new(),
                    group: None,
                    trade_id: None,
                },
                execution: None,
                symbol: symbol.clone(),
                signal_ts: scheduled.signal_ts,
                effective_ts: scheduled.effective_ts,
                entry_signal: Some(scheduled.signal),
                entry_profile: profile.cloned(),
                requires_later_quote: scheduled.requires_later_quote,
            });
            return;
        }
        if scheduled.signal.is_entry() {
            let resolved = match profile {
                Some(profile) => profile.apply_entry_signal(&scheduled.signal),
                None => resolve_unprofiled_entry(&scheduled.signal),
            };
            match resolved {
                Ok(Some(resolved)) => {
                    let entry_quote = batch_quotes.get(&resolved.symbol).unwrap_or(quote);
                    self.enqueue_resolved_entry(
                        base_id,
                        scheduled,
                        resolved,
                        entry_quote,
                        lifecycle,
                        future_executor,
                        portfolio,
                        pricer,
                        conversion_quotes,
                    )
                }
                Ok(None) => {
                    let mut disposition = ActionDisposition::skipped(base_id, "not_an_entry");
                    disposition.action_kind = Some("entry".into());
                    disposition.signal_ts = Some(scheduled.signal_ts);
                    disposition.effective_ts = Some(scheduled.effective_ts);
                    let _ = lifecycle.record(disposition);
                }
                Err(error) => {
                    let mut disposition = ActionDisposition::rejected(base_id, error.to_string());
                    disposition.action_kind = Some("entry".into());
                    disposition.signal_ts = Some(scheduled.signal_ts);
                    disposition.effective_ts = Some(scheduled.effective_ts);
                    let _ = lifecycle.record(disposition);
                }
            }
            return;
        }

        let actions = self.resolve_future_actions(&scheduled.signal);
        if actions.is_empty() {
            let mut disposition = ActionDisposition::skipped(base_id, "position_not_found");
            disposition.action_kind = Some(raw_signal_kind(&scheduled.signal).to_owned());
            disposition.signal_ts = Some(scheduled.signal_ts);
            disposition.effective_ts = Some(scheduled.effective_ts);
            let _ = lifecycle.record(disposition);
            return;
        }
        for (index, action) in actions.into_iter().enumerate() {
            let action_id = format!("{base_id}:action:{index:03}");
            let Some(symbol) = self.action_symbol(&action) else {
                self.apply_future_action(
                    action_id,
                    raw_signal_kind(&scheduled.signal).to_owned(),
                    action,
                    None,
                    scheduled.signal_ts,
                    scheduled.effective_ts,
                    quote,
                    lifecycle,
                    future_executor,
                    portfolio,
                    pricer,
                    conversion_quotes,
                );
                continue;
            };
            if is_fill_bearing(&action) {
                queued.push_back(QueuedAction {
                    action_id,
                    action_kind: raw_signal_kind(&scheduled.signal).to_owned(),
                    action,
                    execution: None,
                    symbol,
                    signal_ts: scheduled.signal_ts,
                    effective_ts: scheduled.effective_ts,
                    entry_signal: None,
                    entry_profile: None,
                    requires_later_quote: scheduled.requires_later_quote,
                });
            } else {
                let action_quote = batch_quotes.get(&symbol).unwrap_or(quote);
                self.apply_future_action(
                    action_id,
                    raw_signal_kind(&scheduled.signal).to_owned(),
                    action,
                    None,
                    scheduled.signal_ts,
                    scheduled.effective_ts,
                    action_quote,
                    lifecycle,
                    future_executor,
                    portfolio,
                    pricer,
                    conversion_quotes,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_resolved_entry(
        &mut self,
        action_id: String,
        scheduled: ScheduledSignal,
        resolved: ResolvedEntry,
        quote: &PriceQuote,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) {
        match self.finalize_resolved_entry(
            resolved,
            future_executor.balance(),
            scheduled.effective_ts,
            Some(conversion_quotes),
        ) {
            Ok(action) => self.apply_future_action(
                action_id,
                "entry".into(),
                action,
                None,
                scheduled.signal_ts,
                scheduled.effective_ts,
                quote,
                lifecycle,
                future_executor,
                portfolio,
                pricer,
                conversion_quotes,
            ),
            Err(error) => {
                let mut disposition = ActionDisposition::rejected(action_id, error);
                disposition.action_kind = Some("entry".into());
                disposition.signal_ts = Some(scheduled.signal_ts);
                disposition.effective_ts = Some(scheduled.effective_ts);
                let _ = lifecycle.record(disposition);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_queued_future(
        &mut self,
        quotes: &BTreeMap<String, PriceQuote>,
        exposure_increasing: bool,
        queued: &mut VecDeque<QueuedAction>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) {
        let mut remaining = VecDeque::new();
        while let Some(mut action) = queued.pop_front() {
            let increases = is_exposure_increasing(&action.action);
            let Some(quote) = quotes.get(&action.symbol) else {
                remaining.push_back(action);
                continue;
            };
            if action.effective_ts > quote.ts
                || (action.requires_later_quote && quote.ts <= action.signal_ts)
                || increases != exposure_increasing
            {
                remaining.push_back(action);
                continue;
            }

            if let Some(mut signal) = action.entry_signal.take() {
                let (side, symbol) = match &signal {
                    RawSignal::Entry { side, symbol, .. } => (*side, symbol.clone()),
                    _ => unreachable!("queued entry metadata must contain an entry signal"),
                };
                let execution = match pricer.market_entry(side, quote, self.pip_size(&symbol)) {
                    Ok(fill) => fill,
                    Err(error) => {
                        let mut disposition =
                            ActionDisposition::rejected(action.action_id, error.to_string());
                        disposition.action_kind = Some(action.action_kind);
                        disposition.signal_ts = Some(action.signal_ts);
                        disposition.effective_ts = Some(action.effective_ts);
                        let _ = lifecycle.record(disposition);
                        continue;
                    }
                };
                if let RawSignal::Entry { price, .. } = &mut signal {
                    *price = Some(execution.price);
                }
                action.execution = Some(execution);
                let resolved = match action.entry_profile.as_ref() {
                    Some(profile) => profile.apply_entry_signal(&signal),
                    None => resolve_unprofiled_entry(&signal),
                };
                match resolved {
                    Ok(Some(resolved)) => match self.finalize_resolved_entry(
                        resolved,
                        future_executor.balance(),
                        quote.ts,
                        Some(conversion_quotes),
                    ) {
                        Ok(finalized) => action.action = finalized,
                        Err(error) => {
                            let mut disposition =
                                ActionDisposition::rejected(action.action_id, error);
                            disposition.action_kind = Some(action.action_kind);
                            disposition.signal_ts = Some(action.signal_ts);
                            disposition.effective_ts = Some(action.effective_ts);
                            let _ = lifecycle.record(disposition);
                            continue;
                        }
                    },
                    Ok(None) => {
                        let mut disposition =
                            ActionDisposition::skipped(action.action_id, "not_an_entry");
                        disposition.action_kind = Some(action.action_kind);
                        disposition.signal_ts = Some(action.signal_ts);
                        disposition.effective_ts = Some(action.effective_ts);
                        let _ = lifecycle.record(disposition);
                        continue;
                    }
                    Err(error) => {
                        let mut disposition =
                            ActionDisposition::rejected(action.action_id, error.to_string());
                        disposition.action_kind = Some(action.action_kind);
                        disposition.signal_ts = Some(action.signal_ts);
                        disposition.effective_ts = Some(action.effective_ts);
                        let _ = lifecycle.record(disposition);
                        continue;
                    }
                }
            }
            self.apply_future_action(
                action.action_id,
                action.action_kind,
                action.action,
                action.execution,
                action.signal_ts,
                action.effective_ts,
                quote,
                lifecycle,
                future_executor,
                portfolio,
                pricer,
                conversion_quotes,
            );
        }
        *queued = remaining;
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_future_action(
        &mut self,
        action_id: String,
        action_kind: String,
        mut action: Action,
        execution: Option<ExecutionFill>,
        signal_ts: NaiveDateTime,
        effective_ts: NaiveDateTime,
        quote: &PriceQuote,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) {
        if let Action::Open {
            trade_id: Some(trade_id),
            ..
        } = &action
            && self.engine.manager.id_by_trade_id(trade_id).is_some()
        {
            let mut disposition = ActionDisposition::rejected(action_id, "duplicate_trade_id");
            disposition.action_kind = Some(action_kind);
            disposition.signal_ts = Some(signal_ts);
            disposition.effective_ts = Some(effective_ts);
            let _ = lifecycle.record(disposition);
            return;
        }
        if let Action::ScaleIn { position_id, .. } = &action
            && future_executor.has_close(position_id)
        {
            let mut disposition =
                ActionDisposition::rejected(action_id, "scale_in_after_close_not_supported");
            disposition.action_kind = Some(action_kind);
            disposition.signal_ts = Some(signal_ts);
            disposition.effective_ts = Some(effective_ts);
            disposition.position_ids.push(position_id.clone());
            let _ = lifecycle.record(disposition);
            return;
        }

        let execution = match self.prepare_future_action(&mut action, execution, quote, pricer) {
            Ok(execution) => execution,
            Err(reason) => {
                let mut disposition = ActionDisposition::rejected(action_id, reason);
                disposition.action_kind = Some(action_kind);
                disposition.signal_ts = Some(signal_ts);
                disposition.effective_ts = Some(effective_ts);
                let _ = lifecycle.record(disposition);
                return;
            }
        };

        let engine_transaction = match execution {
            Some(execution) => self
                .engine
                .begin_priced_future_action(action, quote, execution),
            None => self.engine.begin_future_action(action, effective_ts),
        };
        let engine_transaction = match engine_transaction {
            Ok(transaction) => transaction,
            Err(error) => {
                let mut disposition = ActionDisposition::rejected(action_id, error.to_string());
                disposition.action_kind = Some(action_kind);
                disposition.signal_ts = Some(signal_ts);
                disposition.effective_ts = Some(effective_ts);
                let _ = lifecycle.record(disposition);
                return;
            }
        };

        let committed_effects = engine_transaction.effects().to_vec();
        let mut affected = if FutureExecutor::requires_processing(engine_transaction.effects()) {
            match future_executor.process_future_effects_with_currency(
                engine_transaction.effects(),
                &self.engine,
                quote,
                Some(&action_id),
                Some(signal_ts),
                effective_ts,
                portfolio,
                Some(conversion_quotes),
            ) {
                Ok(affected) => affected,
                Err(error) => {
                    engine_transaction.rollback(&mut self.engine);
                    let mut disposition = ActionDisposition::failed(action_id, error.to_string());
                    disposition.action_kind = Some(action_kind);
                    disposition.signal_ts = Some(signal_ts);
                    disposition.effective_ts = Some(effective_ts);
                    let _ = lifecycle.record(disposition);
                    return;
                }
            }
        } else {
            Vec::new()
        };
        for future_effect in engine_transaction.effects() {
            match future_effect.effect() {
                Effect::OrderPlaced { id } | Effect::OrderCancelled { id } => {
                    affected.push(id.clone());
                }
                _ => {}
            }
        }
        affected.sort();
        affected.dedup();
        let _ = engine_transaction.commit();
        self.committed_feedback.extend(committed_effects);

        let mut disposition = ActionDisposition::applied(action_id);
        disposition.action_kind = Some(action_kind);
        disposition.signal_ts = Some(signal_ts);
        disposition.effective_ts = Some(effective_ts);
        disposition.position_ids = affected;
        let _ = lifecycle.record(disposition);
    }

    fn prepare_future_action(
        &self,
        action: &mut Action,
        prepriced: Option<ExecutionFill>,
        quote: &PriceQuote,
        pricer: &ExecutionPricer,
    ) -> Result<Option<ExecutionFill>, String> {
        let mut execution = prepriced;
        match action {
            Action::Open {
                symbol,
                side,
                order_type,
                price,
                size,
                ..
            } => {
                if !valid_accounting_size(*size) {
                    return Err(format!(
                        "position size must be finite and greater than the accounting tolerance, got {size}"
                    ));
                }
                if price.is_some_and(|price| !price.is_finite() || price <= 0.0) {
                    return Err(format!(
                        "supplied entry price must be finite and positive, got {price:?}"
                    ));
                }
                if *order_type == OrderType::Market {
                    let priced = match execution {
                        Some(priced) => priced,
                        None => pricer
                            .market_entry(*side, quote, self.pip_size(symbol))
                            .map_err(|error| error.to_string())?,
                    };
                    *price = Some(priced.price);
                    execution = Some(priced);
                } else {
                    if price.is_none() {
                        return Err("pending entry requires a requested price".to_owned());
                    }
                    execution = None;
                }
            }
            Action::ScaleIn {
                position_id,
                price,
                size,
                ..
            } => {
                if !valid_accounting_size(*size) {
                    return Err(format!(
                        "scale-in size must be finite and greater than the accounting tolerance, got {size}"
                    ));
                }
                if price.is_some_and(|price| !price.is_finite() || price <= 0.0) {
                    return Err(format!(
                        "supplied scale-in price must be finite and positive, got {price:?}"
                    ));
                }
                let side = self
                    .engine
                    .get_position(position_id)
                    .map(|position| position.data.side)
                    .ok_or_else(|| format!("position not found: {position_id}"))?;
                let priced = match execution {
                    Some(priced) => priced,
                    None => pricer
                        .market_entry(side, quote, self.pip_size(&quote.symbol))
                        .map_err(|error| error.to_string())?,
                };
                *price = Some(priced.price);
                execution = Some(priced);
            }
            Action::ClosePosition { position_id } | Action::ClosePartial { position_id, .. } => {
                let position = self
                    .engine
                    .get_position(position_id)
                    .ok_or_else(|| format!("position not found: {position_id}"))?;
                if position.data.symbol != quote.symbol {
                    return Err(format!(
                        "position symbol {} does not match quote symbol {}",
                        position.data.symbol, quote.symbol
                    ));
                }
                execution = Some(
                    pricer
                        .market_exit(
                            position.data.side,
                            quote,
                            self.pip_size(&position.data.symbol),
                        )
                        .map_err(|error| error.to_string())?,
                );
            }
            _ => execution = None,
        }
        Ok(execution)
    }

    fn prepare_triggering_pending(
        &self,
        quote: &PriceQuote,
        pricer: &ExecutionPricer,
    ) -> (Vec<PreparedPendingFill>, Vec<(String, String)>) {
        let ids = self
            .engine
            .manager
            .pending_ids_by_symbol_sorted(&quote.symbol);
        let mut prepared = Vec::new();
        let mut failures = Vec::new();
        for id in ids {
            let Some(position) = self.engine.get_position(&id) else {
                continue;
            };
            let Some(purpose) = position.pending_fill_purpose(quote, self.config.fill_model) else {
                continue;
            };
            let execution = match pricer.price(
                purpose,
                position.data.side,
                quote,
                position.data.pending_price,
                self.pip_size(&quote.symbol),
            ) {
                Ok(fill) => fill,
                Err(error) => {
                    failures.push((id, error.to_string()));
                    continue;
                }
            };

            let size = position.data.size;
            if !valid_accounting_size(size) {
                failures.push((
                    id,
                    format!(
                        "pending size must be finite and greater than the accounting tolerance, got {size}"
                    ),
                ));
                continue;
            }

            prepared.push(PreparedPendingFill {
                position_id: id,
                execution,
                size,
            });
        }
        (prepared, failures)
    }

    fn pip_size(&self, symbol: &str) -> f64 {
        self.config
            .symbol_specs
            .get(symbol)
            .map(|spec| 10_f64.powi(-(spec.pip_position as i32)))
            .unwrap_or(0.0001)
    }

    fn action_symbol(&self, action: &Action) -> Option<String> {
        match action {
            Action::Open { symbol, .. } => Some(symbol.clone()),
            Action::ClosePosition { position_id }
            | Action::ClosePartial { position_id, .. }
            | Action::ModifyStoploss { position_id, .. }
            | Action::MoveStoplossToEntry { position_id }
            | Action::AddTarget { position_id, .. }
            | Action::RemoveTarget { position_id, .. }
            | Action::ModifyTarget { position_id, .. }
            | Action::AddRule { position_id, .. }
            | Action::RemoveRule { position_id, .. }
            | Action::ScaleIn { position_id, .. }
            | Action::CancelPending { position_id } => self
                .engine
                .get_position(position_id)
                .map(|position| position.data.symbol.clone()),
            Action::CloseAllOf { symbol } | Action::ModifyAllStoploss { symbol, .. } => {
                Some(symbol.clone())
            }
            _ => None,
        }
    }

    fn resolve_future_actions(&self, signal: &RawSignal) -> Vec<Action> {
        match signal {
            RawSignal::CloseAllOf { symbol, .. } => self
                .engine
                .manager
                .open_ids_by_symbol_sorted(symbol)
                .into_iter()
                .map(|position_id| Action::ClosePosition { position_id })
                .collect(),
            RawSignal::CloseAll { .. } => self
                .engine
                .manager
                .ids_by_status_sorted(PositionStatus::Open)
                .into_iter()
                .map(|position_id| Action::ClosePosition { position_id })
                .collect(),
            RawSignal::CloseAllInGroup { group_id, .. } => {
                let mut ids = self.engine.manager.open_ids_by_group(group_id);
                ids.sort();
                ids.into_iter()
                    .map(|position_id| Action::ClosePosition { position_id })
                    .collect()
            }
            RawSignal::CancelAllPending { .. } => self
                .engine
                .manager
                .ids_by_status_sorted(PositionStatus::Pending)
                .into_iter()
                .map(|position_id| Action::CancelPending { position_id })
                .collect(),
            _ => resolve_signal(signal, &self.engine),
        }
    }
}

fn map_strategy_driver_error<FeedError, StrategyError>(
    error: StrategyDriverError<StrategyError>,
) -> StrategyReplayError<FeedError, StrategyError> {
    match error {
        StrategyDriverError::Series(error) => StrategyReplayError::Series(error),
        StrategyDriverError::SeriesView(error) => StrategyReplayError::SeriesView(error),
        StrategyDriverError::Analysis(error) => StrategyReplayError::Analysis(error),
        StrategyDriverError::Strategy(error) => StrategyReplayError::Strategy(error),
        StrategyDriverError::Runtime(error) => StrategyReplayError::Runtime(error),
        StrategyDriverError::WarmupSignals { timestamp } => {
            StrategyReplayError::WarmupSignals { timestamp }
        }
        StrategyDriverError::InvalidGeneratedSignal {
            signal_index,
            reason,
        } => StrategyReplayError::InvalidGeneratedSignal {
            signal_index,
            reason,
        },
        StrategyDriverError::TickExecutionRequired { symbol, timestamp } => {
            StrategyReplayError::TickExecutionRequired { symbol, timestamp }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn observe_future_equity(
    portfolio: &mut PortfolioRecorder,
    future_executor: &FutureExecutor,
    ts: NaiveDateTime,
    conversion_quotes: &ConversionQuoteBook,
    kind: EquityObservationKind,
    collector: &mut MtmCurveCollector,
    last_candidate: &mut Option<EquityPoint>,
    suppress_unchanged_post_output: bool,
) {
    portfolio.set_realized_pnl(future_executor.realized_pnl());
    let mut point = portfolio.observe_with_currency(
        ts,
        future_executor.open_snapshots(),
        Some(conversion_quotes),
    );
    point.observation_kind = Some(kind.as_str().to_owned());
    if suppress_unchanged_post_output
        && last_candidate
            .as_ref()
            .is_some_and(|previous| same_equity_values(previous, &point))
    {
        return;
    }
    collector.observe(point.clone());
    *last_candidate = Some(point);
}

fn same_equity_values(left: &EquityPoint, right: &EquityPoint) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.observation_kind = None;
    left.observation_sequence = None;
    right.observation_kind = None;
    right.observation_sequence = None;
    left == right
}

fn valid_accounting_size(size: f64) -> bool {
    size.is_finite() && size > position_size_tolerance(size)
}

fn explicit_instrument_spec<'a>(
    config: &'a BacktestConfig,
    symbol: &str,
) -> Option<&'a InstrumentSpec> {
    config
        .instrument_manifest
        .as_ref()?
        .instruments
        .get(symbol)
        .map(|artifact| &artifact.spec)
}

fn decimal_to_f64(value: Decimal, field: &str) -> Result<f64, String> {
    let value = value
        .to_string()
        .parse::<f64>()
        .map_err(|error| format!("invalid {field}: {error}"))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(format!("{field} must be finite"))
    }
}

fn instrument_multiplier(spec: &InstrumentSpec) -> Result<f64, String> {
    decimal_to_f64(
        spec.economics.contract_multiplier.get(),
        "instrument contract multiplier",
    )
    .and_then(|value| {
        if value > 0.0 {
            Ok(value)
        } else {
            Err("instrument contract multiplier must be positive".into())
        }
    })
}

fn supported_instrument_multiplier(spec: &InstrumentSpec) -> Result<f64, String> {
    if spec.status != ListingStatus::Trading {
        return Err(format!(
            "instrument {} is not in trading status",
            spec.instrument
        ));
    }
    if spec.economics.quantity_unit != QuantityUnit::StandardLot {
        return Err(format!(
            "unsupported quantity unit for instrument {}: {:?}",
            spec.instrument, spec.economics.quantity_unit
        ));
    }
    let model = spec.economics.pnl_model.as_str();
    if model != EconomicsModelId::FX_QUOTE_LINEAR_V1
        && model != EconomicsModelId::CFD_QUOTE_LINEAR_V1
    {
        return Err(format!(
            "unsupported P&L model for instrument {}: {model}",
            spec.instrument
        ));
    }
    instrument_multiplier(spec)
}

fn validate_instrument_manifest(config: &BacktestConfig) -> Result<(), String> {
    let Some(manifest) = &config.instrument_manifest else {
        return Ok(());
    };
    for (symbol, artifact) in &manifest.instruments {
        if symbol.is_empty() {
            return Err("instrument manifest symbol must not be empty".into());
        }
        artifact
            .spec
            .validate()
            .map_err(|error| format!("invalid instrument spec for {symbol}: {error}"))?;
        if artifact.resolved.instrument != artifact.spec.instrument {
            return Err(format!(
                "resolved instrument and spec identity differ for {symbol}"
            ));
        }
        if artifact.resolved.spec_revision != artifact.spec.revision {
            return Err(format!(
                "resolved specification revision does not match the embedded spec for {symbol}"
            ));
        }
        supported_instrument_multiplier(&artifact.spec)?;
    }
    for binding in &manifest.stored_series {
        let known = manifest
            .instruments
            .values()
            .any(|artifact| artifact.resolved == binding.instrument);
        if !known {
            return Err(format!(
                "stored series {}:{} references an instrument outside the manifest",
                binding.source_partition, binding.source_symbol
            ));
        }
        let artifact = manifest
            .instruments
            .values()
            .find(|artifact| artifact.resolved == binding.instrument)
            .expect("known binding reference has an instrument artifact");
        if binding.effective != artifact.spec.effective {
            return Err(format!(
                "stored series {}:{} effective interval differs from its instrument spec",
                binding.source_partition, binding.source_symbol
            ));
        }
    }
    Ok(())
}

fn effective_contract_sizes(config: &BacktestConfig) -> HashMap<String, f64> {
    let mut contract_sizes = config.contract_sizes.clone();
    if let Some(manifest) = &config.instrument_manifest {
        for (symbol, artifact) in &manifest.instruments {
            if let Ok(multiplier) = instrument_multiplier(&artifact.spec) {
                contract_sizes.insert(symbol.clone(), multiplier);
            }
        }
    }
    contract_sizes
}

fn is_monetary_sizing(policy: &SizingPolicy) -> bool {
    matches!(
        policy,
        SizingPolicy::FixedRiskAmount { .. } | SizingPolicy::BalanceRiskPercent { .. }
    )
}

fn accept_legacy_quote(
    quote: &PriceQuote,
    last_quote_ts: &mut BTreeMap<String, NaiveDateTime>,
) -> bool {
    if ExecutionPricer::validate_quote(quote).is_err()
        || last_quote_ts
            .get(&quote.symbol)
            .is_some_and(|last| *last > quote.ts)
    {
        return false;
    }
    last_quote_ts.insert(quote.symbol.clone(), quote.ts);
    true
}

fn validate_replay_config(
    config: &BacktestConfig,
    future: Option<&FutureQuoteConfig>,
    raw_signals: &[RawSignal],
) -> Result<(), String> {
    if !config.initial_balance.is_finite() || config.initial_balance <= 0.0 {
        return Err(format!(
            "initial balance must be finite and positive, got {}",
            config.initial_balance
        ));
    }
    for (symbol, contract_size) in &config.contract_sizes {
        if symbol.is_empty() {
            return Err("contract-size symbol must not be empty".into());
        }
        if !contract_size.is_finite() || *contract_size <= 0.0 {
            return Err(format!(
                "contract size for {symbol} must be finite and positive, got {contract_size}"
            ));
        }
    }

    validate_instrument_manifest(config)?;
    for (symbol, spec) in &config.symbol_specs {
        validate_symbol_spec(symbol, spec)?;
        if explicit_instrument_spec(config, symbol).is_none() {
            resolve_legacy_economics(spec).map_err(|error| error.to_string())?;
        }
    }

    let entry_symbols: Vec<&str> = raw_signals
        .iter()
        .filter_map(|signal| match signal {
            RawSignal::Entry { symbol, .. } => Some(symbol.as_str()),
            _ => None,
        })
        .collect();
    if !entry_symbols.is_empty() && config.sizing.is_none() {
        return Err("raw entry requires BacktestConfig.sizing".to_owned());
    }
    if let Some(policy) = &config.sizing {
        validate_sizing_policy(policy)?;
        for symbol in &entry_symbols {
            if !config.symbol_specs.contains_key(*symbol)
                && explicit_instrument_spec(config, symbol).is_none()
            {
                return Err(format!("missing instrument or symbol spec for {symbol}"));
            }
        }
        if !entry_symbols.is_empty() && is_monetary_sizing(policy) {
            let future = future.ok_or_else(|| {
                "monetary sizing requires FutureQuote execution and a currency plan".to_owned()
            })?;
            let plan = future
                .currency_plan
                .as_ref()
                .ok_or_else(|| "monetary sizing requires a FutureQuote currency plan".to_owned())?;
            for symbol in &entry_symbols {
                if plan.route_for_primary_symbol(symbol).is_none() {
                    return Err(format!(
                        "currency plan has no frozen route for primary symbol {symbol}"
                    ));
                }
            }
        }
    }

    if let Some(future) = future {
        if future.signal_latency_ms < 0 {
            return Err(format!(
                "signal latency must be non-negative, got {}",
                future.signal_latency_ms
            ));
        }
        let latency = Duration::milliseconds(future.signal_latency_ms);
        for signal in raw_signals {
            if signal.ts().checked_add_signed(latency).is_none() {
                return Err(format!(
                    "signal latency overflows datetime for signal at {}",
                    signal.ts()
                ));
            }
        }
        if !future.slippage_pips.is_finite() {
            return Err(format!(
                "slippage pips must be finite, got {}",
                future.slippage_pips
            ));
        }
        if future.stale_quote_after_ms.is_some_and(|value| value < 0) {
            return Err("stale quote threshold must be non-negative".into());
        }
        if !future.pnl_epsilon.is_finite() || future.pnl_epsilon < 0.0 {
            return Err(format!(
                "P&L epsilon must be finite and non-negative, got {}",
                future.pnl_epsilon
            ));
        }
        if future.conversion_stale_after_ms < 0 {
            return Err("conversion quote threshold must be non-negative".to_owned());
        }
        future
            .mtm_output
            .validate()
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn validate_sizing_policy(policy: &SizingPolicy) -> Result<(), String> {
    let (name, value) = match policy {
        SizingPolicy::FixedLot { lots } => ("fixed lots", *lots),
        SizingPolicy::FixedRiskAmount { amount } => ("fixed risk amount", *amount),
        SizingPolicy::BalanceRiskPercent { percent } => ("balance risk percent", *percent),
    };
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(format!("{name} must be finite and positive, got {value}"))
    }
}

fn validate_symbol_spec(symbol: &str, spec: &qs_symbols::SymbolSpec) -> Result<(), String> {
    if symbol.is_empty() || spec.canonical.is_empty() {
        return Err("symbol spec names must not be empty".into());
    }
    if spec.digits > 18 || spec.pip_position > spec.digits {
        return Err(format!(
            "invalid price precision for {symbol}: digits={}, pip_position={}",
            spec.digits, spec.pip_position
        ));
    }
    if spec.lot_base_units <= 0
        || spec.lot_step_units <= 0
        || spec.lot_min_steps <= 0
        || spec.lot_max_steps < 0
        || (spec.lot_max_steps > 0 && spec.lot_max_steps < spec.lot_min_steps)
    {
        return Err(format!("invalid lot metadata for {symbol}"));
    }
    let lot_step = spec.lot_step();
    let min_lot = spec.lot_min();
    let max_lot = spec.lot_max();
    if !lot_step.is_finite()
        || lot_step <= 0.0
        || !min_lot.is_finite()
        || min_lot <= 0.0
        || !max_lot.is_finite()
    {
        return Err(format!("invalid derived lot metadata for {symbol}"));
    }
    Ok(())
}

fn rejected_legacy_result(config: &BacktestConfig) -> BacktestResult {
    BacktestResult::from_trade_log(
        if config.initial_balance.is_finite() {
            config.initial_balance
        } else {
            0.0
        },
        Vec::new(),
    )
}

fn rejected_future_result(
    config: &BacktestConfig,
    future: &FutureQuoteConfig,
    evaluation_options: EvaluationOptions,
    error: String,
) -> BacktestResult {
    let execution_model = ExecutionModel::new(
        qs_core::types::ExecutionConvention::FutureQuoteV1,
        config.fill_model,
        if future.slippage_pips == 0.0 {
            SlippageModel::None
        } else {
            SlippageModel::FixedPips {
                pips: future.slippage_pips,
            }
        },
    );
    let mut lifecycle = LifecycleLedger::new();
    let _ = lifecycle.record(ActionDisposition::rejected(
        "configuration",
        format!("invalid_configuration: {error}"),
    ));
    let mut tags = BTreeMap::new();
    tags.insert("configuration_error".into(), error);
    insert_economic_support_metadata(&mut tags, config);
    let artifacts = FutureBacktestArtifacts {
        execution: ExecutionMetadata {
            execution_model,
            initial_balance: if config.initial_balance.is_finite() {
                config.initial_balance
            } else {
                0.0
            },
            account_currency: future
                .currency_plan
                .as_ref()
                .map(|plan| plan.account_currency().to_owned()),
            currency_plan: future.currency_plan.clone(),
            contract_sizes: effective_contract_sizes(config)
                .into_iter()
                .filter(|(symbol, size)| !symbol.is_empty() && size.is_finite() && *size > 0.0)
                .collect(),
            instrument_manifest: config.instrument_manifest.clone(),
            instrument_sizing: Vec::new(),
            stale_quote_after_millis: future.stale_quote_after_ms,
            pnl_epsilon: if future.pnl_epsilon.is_finite() && future.pnl_epsilon >= 0.0 {
                future.pnl_epsilon
            } else {
                crate::artifacts::DEFAULT_PNL_EPSILON
            },
            tags,
            ..ExecutionMetadata::default()
        },
        lifecycle,
        mtm_output_summary: MtmOutputSummary {
            policy: future.mtm_output,
            ..MtmOutputSummary::default()
        },
        ..FutureBacktestArtifacts::default()
    };
    BacktestResult::from_future_artifacts_with_options(artifacts, evaluation_options)
}

fn insert_economic_support_metadata(tags: &mut BTreeMap<String, String>, config: &BacktestConfig) {
    let mut compatibility_specs = config.symbol_specs.iter().peekable();
    if compatibility_specs.peek().is_none() {
        return;
    }
    tags.insert(
        "economics.guard".into(),
        LEGACY_ECONOMIC_GUARD_ID.to_owned(),
    );
    for (symbol, spec) in compatibility_specs {
        let prefix = format!("economics.symbol.{symbol}");
        tags.insert(format!("{prefix}.category"), spec.category.clone());
        match resolve_legacy_economics(spec) {
            Ok(economics) => {
                tags.insert(format!("{prefix}.status"), "supported".into());
                tags.insert(format!("{prefix}.model"), economics.model.as_str().into());
                tags.insert(
                    format!("{prefix}.contract_multiplier"),
                    economics.contract_multiplier.to_string(),
                );
            }
            Err(error) => {
                tags.insert(format!("{prefix}.status"), "unsupported".into());
                tags.insert(format!("{prefix}.reason"), error.to_string());
            }
        }
    }
}

fn queued_exposure_symbols(
    queued: &VecDeque<QueuedAction>,
    quotes: &BTreeMap<String, PriceQuote>,
    batch_ts: NaiveDateTime,
) -> BTreeSet<String> {
    queued
        .iter()
        .filter(|action| {
            action.effective_ts <= batch_ts
                && quotes.contains_key(&action.symbol)
                && is_exposure_increasing(&action.action)
        })
        .map(|action| action.symbol.clone())
        .collect()
}

fn is_exposure_increasing(action: &Action) -> bool {
    matches!(
        action,
        Action::Open {
            order_type: OrderType::Market,
            ..
        } | Action::ScaleIn { .. }
    )
}

fn is_fill_bearing(action: &Action) -> bool {
    matches!(
        action,
        Action::Open {
            order_type: OrderType::Market,
            ..
        } | Action::ClosePosition { .. }
            | Action::ClosePartial { .. }
            | Action::ScaleIn { .. }
    )
}

fn raw_signal_kind(signal: &RawSignal) -> &'static str {
    match signal {
        RawSignal::Entry { .. } => "entry",
        RawSignal::Close { .. } => "close",
        RawSignal::ClosePartial { .. } => "close_partial",
        RawSignal::ModifyStoploss { .. } => "modify_stoploss",
        RawSignal::MoveStoplossToEntry { .. } => "move_stoploss_to_entry",
        RawSignal::AddTarget { .. } => "add_target",
        RawSignal::RemoveTarget { .. } => "remove_target",
        RawSignal::ModifyTarget { .. } => "modify_target",
        RawSignal::AddRule { .. } => "add_rule",
        RawSignal::RemoveRule { .. } => "remove_rule",
        RawSignal::ScaleIn { .. } => "scale_in",
        RawSignal::CancelPending { .. } => "cancel_pending",
        RawSignal::CloseAllOf { .. } => "close_all_of",
        RawSignal::CloseAll { .. } => "close_all",
        RawSignal::CancelAllPending { .. } => "cancel_all_pending",
        RawSignal::ModifyAllStoploss { .. } => "modify_all_stoploss",
        RawSignal::CloseAllInGroup { .. } => "close_all_in_group",
        RawSignal::ModifyAllStoplossInGroup { .. } => "modify_all_stoploss_in_group",
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::currency::{ConversionRoute, FxPair};
    use crate::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, VecFeed};
    use crate::profile::{ManagementProfile, PositionRef, RawSignal, StoplossMode};
    use chrono::NaiveDate;
    use qs_core::types::{CloseReason, FillPurpose, OrderType, Side, TargetSpec};

    fn ts(h: u32, m: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn tick(symbol: &str, bid: f64, ask: f64, time: chrono::NaiveDateTime) -> MarketEvent {
        MarketEvent::Tick {
            symbol: symbol.into(),
            ts: time,
            bid,
            ask,
        }
    }

    fn test_symbol_spec(symbol: &str) -> qs_symbols::SymbolSpec {
        qs_symbols::SymbolSpec {
            canonical: symbol.to_ascii_lowercase(),
            pip_position: 4,
            digits: 5,
            category: "forex".into(),
            lot_base_units: 100,
            lot_step_units: 1,
            lot_min_steps: 1,
            lot_max_steps: 0,
        }
    }

    fn fixed_lot_config() -> BacktestConfig {
        BacktestConfig {
            sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
            symbol_specs: ["EURUSD", "XAUUSD"]
                .into_iter()
                .map(|symbol| (symbol.to_owned(), test_symbol_spec(symbol)))
                .collect(),
            ..BacktestConfig::default()
        }
    }

    fn identity_currency_plan(symbol: &str) -> RunCurrencyPlan {
        RunCurrencyPlan::new(
            "USD",
            [symbol.to_owned()].into_iter().collect(),
            Default::default(),
            [(symbol.to_owned(), "USD".to_owned())]
                .into_iter()
                .collect(),
            [(
                "USD".to_owned(),
                ConversionRoute::Identity {
                    currency: "USD".to_owned(),
                },
            )]
            .into_iter()
            .collect(),
            Vec::new(),
        )
        .unwrap()
    }

    struct ScriptedBatchFeed {
        batches: VecDeque<Result<Option<TimestampBatch>, &'static str>>,
    }

    impl FallibleBatchFeed for ScriptedBatchFeed {
        type Error = &'static str;

        fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
            self.batches.pop_front().unwrap_or(Ok(None))
        }
    }

    struct CountingBatchFeed {
        batches: VecDeque<TimestampBatch>,
        polls: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl FallibleBatchFeed for CountingBatchFeed {
        type Error = Infallible;

        fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
            self.polls.set(self.polls.get() + 1);
            Ok(self.batches.pop_front())
        }
    }

    fn primary_batch(event: MarketEvent) -> TimestampBatch {
        TimestampBatch {
            ts: event.ts(),
            events: vec![FeedEvent::new(
                event,
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
            )],
        }
    }

    fn market_entry(timestamp: NaiveDateTime, symbol: &str, order_type: OrderType) -> RawSignal {
        RawSignal::Entry {
            ts: timestamp,
            symbol: symbol.into(),
            side: Side::Buy,
            order_type,
            price: (order_type == OrderType::Limit).then_some(1.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some(format!("{symbol}-blocker")),
        }
    }

    #[test]
    fn future_streaming_matches_materialized_and_stops_without_draining() {
        let events = vec![
            tick("EURUSD", 1.1000, 1.1002, ts(10, 0, 0)),
            tick("EURUSD", 1.1001, 1.1003, ts(10, 0, 1)),
            tick("EURUSD", 1.1002, 1.1004, ts(10, 0, 2)),
        ];
        let signals = vec![
            market_entry(ts(10, 0, 0), "EURUSD", OrderType::Market),
            RawSignal::CloseAll { ts: ts(10, 0, 1) },
        ];
        let config = BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let mut materialized_feed = VecFeed::new(events.clone());
        let materialized = BacktestRunner::new_future(
            config.clone(),
            FutureQuoteConfig {
                mtm_output: MtmOutputPolicy::Full,
                ..FutureQuoteConfig::default()
            },
        )
        .run_raw_signals_future(&mut materialized_feed, signals.clone(), None);

        let mut stream = ScriptedBatchFeed {
            batches: VecDeque::from([
                Ok(Some(primary_batch(events[0].clone()))),
                Ok(Some(primary_batch(events[1].clone()))),
                Ok(Some(primary_batch(events[2].clone()))),
                Err("must not drain"),
            ]),
        };
        let mut progress = Vec::new();
        let streamed = BacktestRunner::new_future(
            config,
            FutureQuoteConfig {
                mtm_output: MtmOutputPolicy::Full,
                ..FutureQuoteConfig::default()
            },
        )
        .run_raw_signals_future_streaming_controlled(
            &mut stream,
            Some(ts(10, 0, 2)),
            signals,
            None,
            || false,
            |update| progress.push(update),
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(&streamed).unwrap(),
            serde_json::to_value(&materialized).unwrap()
        );
        assert_eq!(
            stream.batches.len(),
            2,
            "quiescence must leave the tail unread"
        );
        assert_eq!(progress.first().unwrap().total_events, 0);
        assert_eq!(progress.last().unwrap().processed_events, 2);
        assert_eq!(progress.last().unwrap().total_events, 2);
        assert_eq!(
            streamed.mtm_equity_curve.last().unwrap().ts,
            ts(10, 0, 1),
            "terminal observation must use the last processed primary timestamp"
        );
        assert_eq!(
            streamed
                .mtm_equity_curve
                .last()
                .unwrap()
                .observation_kind
                .as_deref(),
            Some(EquityObservationKind::QuiescentTermination.as_str())
        );
        assert_eq!(
            streamed
                .execution_metadata
                .as_ref()
                .unwrap()
                .tags
                .get("termination_reason")
                .map(String::as_str),
            Some("quiescent")
        );
    }

    #[test]
    fn exact_time_close_waits_for_later_symbol_pending_fill() {
        let open_ts = ts(10, 0, 0);
        let execution_ts = ts(10, 0, 1);
        let events = vec![
            FeedEvent::new(
                tick("XAUUSD", 101.0, 101.0, open_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 1, 0),
            ),
            FeedEvent::new(
                tick("EURUSD", 1.1, 1.1, execution_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 1),
            ),
            FeedEvent::new(
                tick("XAUUSD", 100.0, 100.0, execution_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 1, 1),
            ),
        ];
        let signals = vec![
            RawSignal::Entry {
                ts: open_ts,
                symbol: "XAUUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(100.0),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: Vec::new(),
                group: None,
                trade_id: Some("later-pending".into()),
            },
            RawSignal::Close {
                ts: execution_ts,
                position: PositionRef::ByTradeId {
                    trade_id: "later-pending".into(),
                },
            },
        ];
        let mut feed = VecFeed::from_feed_events(events);
        let result = BacktestRunner::new_future(
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
            FutureQuoteConfig::default(),
        )
        .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(
            result
                .recorded_fills
                .iter()
                .map(|fill| fill.fill.purpose)
                .collect::<Vec<_>>(),
            vec![FillPurpose::LimitEntry, FillPurpose::MarketExit]
        );
        assert_eq!(result.close_events.len(), 1);
        assert_eq!(result.close_events[0].reason, CloseReason::Manual);
        assert!(result.open_position_snapshots.is_empty());
        assert!(result.pending_order_snapshots.is_empty());
    }

    #[test]
    fn exact_time_close_cannot_beat_later_symbol_stoploss() {
        let open_ts = ts(10, 0, 0);
        let execution_ts = ts(10, 0, 1);
        let events = vec![
            FeedEvent::new(
                tick("XAUUSD", 100.0, 100.0, open_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 1, 0),
            ),
            FeedEvent::new(
                tick("EURUSD", 1.1, 1.1, execution_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 1),
            ),
            FeedEvent::new(
                tick("XAUUSD", 98.0, 98.0, execution_ts),
                EventMetadata::new(SeriesRoles::PRIMARY, 1, 1),
            ),
        ];
        let signals = vec![
            RawSignal::Entry {
                ts: open_ts,
                symbol: "XAUUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: Some(99.0),
                targets: Vec::new(),
                group: None,
                trade_id: Some("later-stop".into()),
            },
            RawSignal::Close {
                ts: execution_ts,
                position: PositionRef::ByTradeId {
                    trade_id: "later-stop".into(),
                },
            },
        ];
        let mut feed = VecFeed::from_feed_events(events);
        let result = BacktestRunner::new_future(
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
            FutureQuoteConfig::default(),
        )
        .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(result.close_events.len(), 1);
        assert_eq!(result.close_events[0].reason, CloseReason::Stoploss);
        assert_eq!(
            result.recorded_fills.last().unwrap().fill.purpose,
            FillPurpose::StopLoss
        );
        assert!(!result.action_dispositions.iter().any(|disposition| {
            disposition.action_id.starts_with("signal:00000001")
                && disposition.status == crate::ledger::ActionDispositionStatus::Applied
        }));
    }

    #[test]
    fn exact_time_multisymbol_closes_preserve_signal_order() {
        let open_ts = ts(10, 0, 0);
        let close_ts = ts(10, 0, 1);
        let mut events = Vec::new();
        for (timestamp, row) in [(open_ts, 0), (close_ts, 1)] {
            events.push(FeedEvent::new(
                tick("EURUSD", 1.1, 1.1, timestamp),
                EventMetadata::new(SeriesRoles::PRIMARY, 0, row),
            ));
            events.push(FeedEvent::new(
                tick("XAUUSD", 100.0, 100.0, timestamp),
                EventMetadata::new(SeriesRoles::PRIMARY, 1, row),
            ));
        }
        let entry = |symbol: &str, trade_id: &str| RawSignal::Entry {
            ts: open_ts,
            symbol: symbol.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some(trade_id.into()),
        };
        let close = |trade_id: &str| RawSignal::Close {
            ts: close_ts,
            position: PositionRef::ByTradeId {
                trade_id: trade_id.into(),
            },
        };
        let signals = vec![
            entry("XAUUSD", "close-first"),
            entry("EURUSD", "close-second"),
            close("close-first"),
            close("close-second"),
        ];
        let mut feed = VecFeed::from_feed_events(events);
        let result = BacktestRunner::new_future(
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
            FutureQuoteConfig::default(),
        )
        .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(
            result
                .close_events
                .iter()
                .map(|event| event.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["XAUUSD", "EURUSD"]
        );
        assert!(
            result
                .close_events
                .iter()
                .all(|event| event.reason == CloseReason::Manual)
        );
    }

    #[test]
    fn future_streaming_quiescence_waits_for_all_blockers() {
        let run = |events: Vec<MarketEvent>, signals: Vec<RawSignal>, config: BacktestConfig| {
            let polls = std::rc::Rc::new(std::cell::Cell::new(0));
            let primary_eod = events.last().map(MarketEvent::ts);
            let mut feed = CountingBatchFeed {
                batches: events.into_iter().map(primary_batch).collect(),
                polls: polls.clone(),
            };
            BacktestRunner::new_future(config, FutureQuoteConfig::default())
                .run_raw_signals_future_streaming_controlled(
                    &mut feed,
                    primary_eod,
                    signals,
                    None,
                    || false,
                    |_| {},
                )
                .unwrap();
            polls.get()
        };
        let eur_events = vec![
            tick("EURUSD", 1.1000, 1.1002, ts(10, 0, 0)),
            tick("EURUSD", 1.1001, 1.1003, ts(10, 0, 1)),
            tick("EURUSD", 1.1002, 1.1004, ts(10, 0, 2)),
        ];

        let immediately_quiescent = run(
            eur_events.clone(),
            vec![RawSignal::CloseAll { ts: ts(10, 0, 0) }],
            BacktestConfig::default(),
        );
        assert_eq!(immediately_quiescent, 1);

        let scheduled = run(
            eur_events.clone(),
            vec![RawSignal::CloseAll { ts: ts(10, 0, 2) }],
            BacktestConfig::default(),
        );
        assert_eq!(scheduled, 3, "scheduled signals must block termination");

        let mut two_symbol_config = BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        };
        two_symbol_config
            .symbol_specs
            .insert("GBPUSD".into(), test_symbol_spec("GBPUSD"));
        let queued = run(
            vec![
                tick("EURUSD", 1.1000, 1.1002, ts(10, 0, 0)),
                tick("GBPUSD", 1.2500, 1.2502, ts(10, 0, 1)),
                tick("GBPUSD", 1.2501, 1.2503, ts(10, 0, 2)),
            ],
            vec![
                market_entry(ts(10, 0, 0), "GBPUSD", OrderType::Market),
                RawSignal::CloseAll { ts: ts(10, 0, 1) },
            ],
            two_symbol_config,
        );
        assert_eq!(
            queued, 2,
            "queued actions must wait for an eligible symbol quote"
        );

        let open = run(
            eur_events.clone(),
            vec![market_entry(ts(10, 0, 0), "EURUSD", OrderType::Market)],
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
        );
        assert_eq!(
            open, 4,
            "open positions must consume the stream through EOD"
        );

        let pending = run(
            eur_events,
            vec![market_entry(ts(10, 0, 0), "EURUSD", OrderType::Limit)],
            fixed_lot_config(),
        );
        assert_eq!(
            pending, 4,
            "pending orders must consume the stream through EOD"
        );
    }

    #[test]
    fn future_mtm_output_policies_bound_curve_and_validate_before_feed_use() {
        assert_eq!(
            FutureQuoteConfig::default().mtm_output,
            MtmOutputPolicy::Bounded { max_points: 4_096 }
        );
        let events: Vec<_> = (0..12)
            .map(|second| tick("EURUSD", 100.0, 100.0, ts(10, 0, second)))
            .collect();

        let pending = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(90.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some("mtm-policy-blocker".into()),
        };
        let run = |policy| {
            let mut feed = VecFeed::new(events.clone());
            BacktestRunner::new_future(
                fixed_lot_config(),
                FutureQuoteConfig {
                    mtm_output: policy,
                    ..FutureQuoteConfig::default()
                },
            )
            .run_raw_signals_future(&mut feed, vec![pending.clone()], None)
        };

        let none = run(MtmOutputPolicy::None);
        assert!(none.mtm_equity_curve.is_empty());
        assert_eq!(none.mtm_output_summary.observed_points, 13);
        assert_eq!(none.mtm_output_summary.omitted_points, 13);

        let bounded = run(MtmOutputPolicy::Bounded { max_points: 8 });
        assert_eq!(bounded.mtm_equity_curve.len(), 8);
        assert_eq!(bounded.mtm_output_summary.observed_points, 13);
        assert_eq!(bounded.mtm_output_summary.retained_points, 8);
        assert_eq!(bounded.mtm_output_summary.omitted_points, 5);

        let full = run(MtmOutputPolicy::Full);
        assert_eq!(full.mtm_equity_curve.len(), 13);
        assert_eq!(full.mtm_output_summary.observed_points, 13);
        assert_eq!(full.mtm_output_summary.omitted_points, 0);
        assert_eq!(
            full.mtm_equity_curve
                .iter()
                .filter(|point| {
                    point.observation_kind.as_deref()
                        == Some(EquityObservationKind::PostOutput.as_str())
                })
                .count(),
            0
        );

        let mut invalid_feed = VecFeed::new(vec![tick("EURUSD", 100.0, 100.0, ts(10, 0, 0))]);
        let rejected = BacktestRunner::new_future(
            BacktestConfig::default(),
            FutureQuoteConfig {
                mtm_output: MtmOutputPolicy::Bounded { max_points: 7 },
                ..FutureQuoteConfig::default()
            },
        )
        .run_raw_signals_future(&mut invalid_feed, Vec::new(), None);
        assert_eq!(invalid_feed.remaining(), 1);
        assert!(rejected.action_dispositions.iter().any(|disposition| {
            disposition.action_id == "configuration"
                && disposition
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("MTM max_points"))
        }));
    }

    #[test]
    fn future_mtm_records_changed_post_output_observation_kind() {
        let mut feed = VecFeed::new(vec![tick("EURUSD", 100.0, 100.0, ts(10, 0, 0))]);
        let signal = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some("mtm-kind".into()),
        };
        let result = BacktestRunner::new_future(
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
            FutureQuoteConfig {
                mtm_output: MtmOutputPolicy::Full,
                ..FutureQuoteConfig::default()
            },
        )
        .run_raw_signals_future(&mut feed, vec![signal], None);

        let kinds: Vec<_> = result
            .mtm_equity_curve
            .iter()
            .filter_map(|point| point.observation_kind.as_deref())
            .collect();
        assert_eq!(
            kinds,
            vec![
                EquityObservationKind::PreSettlement.as_str(),
                EquityObservationKind::PostOutput.as_str(),
                EquityObservationKind::EndOfData.as_str(),
            ]
        );
        assert_eq!(
            result
                .execution_metadata
                .as_ref()
                .unwrap()
                .tags
                .get("termination_reason")
                .map(String::as_str),
            Some("end_of_data")
        );
    }

    #[test]
    fn future_fallible_batch_feed_propagates_source_error() {
        let batch = TimestampBatch {
            ts: ts(10, 0, 0),
            events: vec![FeedEvent::new(
                tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
            )],
        };
        let mut feed = ScriptedBatchFeed {
            batches: VecDeque::from([Ok(Some(batch)), Err("feed failed")]),
        };
        let result =
            BacktestRunner::new_future(BacktestConfig::default(), FutureQuoteConfig::default())
                .run_raw_signals_future_fallible(&mut feed, Vec::new(), None);

        assert!(matches!(result, Err("feed failed")));
    }

    // ── Simple strategy for testing ─────────────────────────────────────

    /// Buys on the first tick, with SL and TP.
    struct BuyOnceStrategy {
        entered: bool,
    }

    impl BuyOnceStrategy {
        fn new() -> Self {
            Self { entered: false }
        }
    }

    impl Strategy for BuyOnceStrategy {
        fn on_event(&mut self, event: &MarketEvent) -> Vec<Action> {
            if self.entered {
                return vec![];
            }
            if let MarketEvent::Tick { symbol, ask, .. } = event {
                self.entered = true;
                vec![Action::Open {
                    symbol: symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::Market,
                    price: Some(*ask),
                    size: 1.0,
                    stoploss: Some(*ask - 0.0050),
                    targets: vec![TargetSpec {
                        price: *ask + 0.0050,
                        close_ratio: 1.0,
                    }],
                    rules: vec![],
                    group: None,
                    trade_id: None,
                }]
            } else {
                vec![]
            }
        }

        fn on_finished(&mut self) -> Vec<Action> {
            // Don't close — let close_on_finish handle it if TP/SL haven't
            // triggered.
            vec![]
        }
    }

    // ── Strategy-driven tests ───────────────────────────────────────────

    #[test]
    fn strategy_backtest_tp_hit() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
            tick("EURUSD", 1.0890, 1.0892, ts(10, 0, 3)),
            // TP at 1.0900 (entry 1.0850 + 0.005)
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert!(result.total_pnl > 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn strategy_backtest_sl_hit() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0830, 1.0832, ts(10, 0, 1)),
            // SL at 1.0800 (entry 1.0850 - 0.005)
            tick("EURUSD", 1.0799, 1.0801, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.losing_trades, 1);
        assert!(result.total_pnl < 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn strategy_close_on_finish() {
        // Price never reaches TP or SL — position should be closed at end.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0852, 1.0854, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn strategy_no_close_on_finish() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_strategy(&mut feed, &mut strategy);

        // Position left open — no trades recorded.
        assert_eq!(result.total_trades, 0);
    }

    // ── Raw signal replay tests ─────────────────────────────────────────

    #[test]
    fn legacy_unprofiled_targets_default_to_equal_weights() {
        let events = vec![
            tick("EURUSD", 1.0000, 1.0000, ts(10, 0, 0)),
            tick("EURUSD", 1.1000, 1.1000, ts(10, 0, 1)),
            tick("EURUSD", 1.2000, 1.2000, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);
        let signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0000),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![1.1000, 1.2000],
            group: None,
            trade_id: Some("equal-targets".into()),
        }];

        let result = BacktestRunner::new(BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        })
        .run_raw_signals(&mut feed, signals, None);

        assert_eq!(result.trade_log.len(), 2);
        assert!(
            result
                .trade_log
                .iter()
                .all(|trade| (trade.size - 0.5).abs() < f64::EPSILON)
        );
        assert!(
            result
                .trade_log
                .iter()
                .all(|trade| trade.close_reason == CloseReason::Target)
        );
    }

    #[test]
    fn legacy_atomic_target_modification_retains_profile_ratio() {
        let events = vec![
            tick("EURUSD", 1.0000, 1.0000, ts(10, 0, 0)),
            tick("EURUSD", 1.1000, 1.1000, ts(10, 0, 1)),
            tick("EURUSD", 1.2000, 1.2000, ts(10, 0, 2)),
            tick("EURUSD", 1.3000, 1.3000, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);
        let position = PositionRef::ByTradeId {
            trade_id: "modified-target".into(),
        };
        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0000),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![1.1000, 1.3000],
                group: None,
                trade_id: Some("modified-target".into()),
            },
            RawSignal::ModifyTarget {
                ts: ts(10, 0, 0),
                position,
                old_price: 1.1000,
                new_price: 1.2000,
            },
        ];
        let profile = ManagementProfile {
            name: "non-default-ratios".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![0.25, 0.75],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let result = BacktestRunner::new(BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        })
        .run_raw_signals(&mut feed, signals, Some(&profile));

        assert_eq!(result.trade_log.len(), 2);
        assert!((result.trade_log[0].exit_price - 1.2000).abs() < f64::EPSILON);
        assert!((result.trade_log[0].size - 0.25).abs() < f64::EPSILON);
        assert!((result.trade_log[1].exit_price - 1.3000).abs() < f64::EPSILON);
        assert!((result.trade_log[1].size - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn run_raw_signals_entry_only() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
    }

    #[test]
    fn run_raw_signals_open_then_close() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn run_raw_signals_open_then_modify_sl() {
        // Open a position, then move SL closer. If price drops to new SL, it triggers.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 1)),
            // SL modify happens at ts(10,0,2)
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 2)),
            // Price drops to modified SL at 1.0840
            tick("EURUSD", 1.0838, 1.0840, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: Some(1.0800),
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ModifyStoploss {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                price: 1.0840,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn run_raw_signals_open_then_partial_close() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
            tick("EURUSD", 1.0880, 1.0882, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ClosePartial {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                ratio: 0.5,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // At least 1 trade closed (partial close + close_on_finish for remainder)
        assert!(result.total_trades >= 1);
    }

    #[test]
    fn run_raw_signals_group_workflow() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
            tick("EURUSD", 1.0880, 1.0882, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            // Open 2 positions in same group
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: Some("grp1".into()),
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: Some("grp1".into()),
                trade_id: Some("t2".into()),
            },
            // Close entire group
            RawSignal::CloseAllInGroup {
                ts: ts(10, 0, 3),
                group_id: "grp1".into(),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_close_all_of_symbol() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 0.5,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
            RawSignal::CloseAllOf {
                ts: ts(10, 0, 2),
                symbol: "EURUSD".into(),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_with_profile() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: Some("t1".into()),
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn run_raw_signals_with_profile_preserves_trade_id() {
        // Regression for profile-supplied trade ID propagation.
        // A raw entry must expose its trade ID so a later PositionRef::ByTradeId signal can resolve and close the position.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("msg-100".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "msg-100".into(),
                },
            },
        ];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Manual);
    }

    #[test]
    fn run_raw_signals_no_profile() {
        // Without a profile, entry signals are converted directly.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
    }

    #[test]
    fn run_raw_signals_last_on_symbol_resolution() {
        // Open two positions, then close the last one by symbol ref.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
            // Close only the second opened position via its trade_id
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t2".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // 2 trades total: one closed by signal, one by close_on_finish
        assert_eq!(result.total_trades, 2);
    }

    #[test]
    fn run_raw_signals_unresolved_ref_skipped() {
        // Try to close a position that doesn't exist — should be silently skipped.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "nonexistent".into(),
            },
        }];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // No positions were opened or closed.
        assert_eq!(result.total_trades, 0);
    }

    // ── Signal replay tests ─────────────────────────────────────────────

    #[test]
    fn signal_replay_basic() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            // TP at 1.0900
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.winning_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn signal_replay_multiple_signals() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            // TP1 hit for first position
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 2)),
            tick("EURUSD", 1.0910, 1.0912, ts(10, 0, 3)),
            tick("EURUSD", 1.0920, 1.0922, ts(10, 0, 4)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("t2".into()),
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // First position closed by TP, second by close_on_finish
        assert!(result.total_trades >= 2);
    }

    #[test]
    fn signal_replay_signal_before_data_filtered() {
        // Signal timestamp is before first data event.
        // The runner itself does not filter; the server is responsible
        // for date filtering. This test verifies that when a pre-window
        // signal IS passed to the runner, it is injected at the first
        // event (backward-compatible library behavior).
        // Server-side filtering is tested separately.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![RawSignal::Entry {
            ts: ts(9, 0, 0), // before first tick
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn empty_feed_empty_result() {
        let mut feed = VecFeed::new(vec![]);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        assert_eq!(result.total_trades, 0);
        assert!((result.final_balance - 10_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn report_display_does_not_panic() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);
        let mut strategy = BuyOnceStrategy::new();

        let runner = BacktestRunner::with_defaults();
        let result = runner.run_strategy(&mut feed, &mut strategy);

        let _display = format!("{result}");
    }

    #[test]
    fn run_raw_signals_with_profile_open_then_modify_sl_by_trade_id() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0838, 1.0840, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ModifyStoploss {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                price: 1.0840,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        assert_eq!(result.total_trades, 1);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Stoploss);
    }

    #[test]
    fn run_raw_signals_with_profile_open_then_close_partial_by_trade_id() {
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::ClosePartial {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
                ratio: 0.5,
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        // Partial close creates at least one trade.
        assert!(result.total_trades >= 1);
    }

    #[test]
    fn run_raw_signals_multi_position_by_trade_id_with_profile() {
        // Two entries on EURUSD group "alpha" with different trade_ids.
        // Close ByTradeId for "t1" only. Verify only t1 closes by signal
        // and t2 remains to be closed by close_on_finish.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0855, 1.0857, ts(10, 0, 1)),
            tick("EURUSD", 1.0860, 1.0862, ts(10, 0, 2)),
            tick("EURUSD", 1.0870, 1.0872, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);

        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: Some("alpha".into()),
            let_remainder_run: false,
        };

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![1.0910],
                group: None,
                trade_id: Some("t1".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0857),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![1.0910],
                group: None,
                trade_id: Some("t2".into()),
            },
            // Close only t1 by trade_id.
            RawSignal::Close {
                ts: ts(10, 0, 2),
                position: PositionRef::ByTradeId {
                    trade_id: "t1".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(&profile));

        // t1 closed by signal, t2 closed by close_on_finish = 2 total.
        assert_eq!(result.total_trades, 2);
        // Both should be in group "alpha" from profile override.
        for trade in &result.trade_log {
            assert_eq!(trade.group.as_deref(), Some("alpha"));
        }
    }

    #[test]
    fn merged_feed_manual_close_uses_correct_symbol_quote() {
        // Regression test for Issue 1 Part 3:
        // Open XAUUSD, then close it manually while the current merged-feed
        // event is a GBPJPY tick. The exit price must be a XAUUSD price,
        // not a GBPJPY price.
        use crate::data_feed::MarketEvent;
        let events = vec![
            MarketEvent::Tick {
                symbol: "XAUUSD".into(),
                ts: ts(10, 0, 0),
                bid: 5000.0,
                ask: 5001.0,
            },
            MarketEvent::Tick {
                symbol: "GBPJPY".into(),
                ts: ts(10, 0, 1),
                bid: 210.0,
                ask: 211.0,
            },
            MarketEvent::Tick {
                symbol: "XAUUSD".into(),
                ts: ts(10, 0, 2),
                bid: 5050.0,
                ask: 5051.0,
            },
            // GBPJPY event at ts(10,0,3) - manual close fires here.
            MarketEvent::Tick {
                symbol: "GBPJPY".into(),
                ts: ts(10, 0, 3),
                bid: 212.0,
                ask: 213.0,
            },
        ];
        let mut feed = VecFeed::new(events);

        let raw_signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "XAUUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(5000.0),
                risk_multiplier: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: Some("xau-1".into()),
            },
            // Manual close at ts(10,0,3) while current event is GBPJPY.
            RawSignal::Close {
                ts: ts(10, 0, 3),
                position: PositionRef::ByTradeId {
                    trade_id: "xau-1".into(),
                },
            },
        ];

        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        assert_eq!(result.total_trades, 1);
        let trade = &result.trade_log[0];
        assert_eq!(trade.symbol, "XAUUSD");
        // Exit price must be a XAUUSD price (~5050), not GBPJPY (~212).
        assert!(
            trade.exit_price > 4000.0,
            "Exit price should be XAUUSD (~5050), got {}",
            trade.exit_price
        );
    }

    fn long_tick_feed(count: usize) -> VecFeed {
        let start = ts(10, 0, 0);
        VecFeed::new(
            (0..count)
                .map(|index| {
                    tick(
                        "EURUSD",
                        1.0848,
                        1.0850,
                        start + Duration::milliseconds(index as i64),
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn legacy_replay_can_be_cancelled_during_event_processing() {
        let cancelled = std::cell::Cell::new(false);
        let mut feed = long_tick_feed(1_000);
        let outcome = BacktestRunner::with_defaults().run_raw_signals_controlled(
            &mut feed,
            Vec::new(),
            None,
            || cancelled.get(),
            |progress| {
                if progress.processed_events >= REPLAY_PROGRESS_INTERVAL {
                    cancelled.set(true);
                }
            },
        );

        assert_eq!(outcome.unwrap_err(), ReplayCancelled);
        assert!(
            feed.remaining() > 0,
            "cancellation must stop further replay"
        );
    }

    #[test]
    fn future_quote_replay_can_be_cancelled_during_event_processing() {
        let cancelled = std::cell::Cell::new(false);
        let mut feed = long_tick_feed(1_000);
        let runner = BacktestRunner::new_future(fixed_lot_config(), FutureQuoteConfig::default());
        let pending = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(1.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: Some("cancellation-blocker".into()),
        };
        let outcome = runner.run_raw_signals_controlled(
            &mut feed,
            vec![pending],
            None,
            || cancelled.get(),
            |progress| {
                if progress.processed_events >= REPLAY_PROGRESS_INTERVAL {
                    cancelled.set(true);
                }
            },
        );

        assert_eq!(outcome.unwrap_err(), ReplayCancelled);
    }

    #[test]
    fn controlled_replay_progress_is_monotonic_and_reaches_event_total() {
        let mut feed = long_tick_feed(600);
        let mut updates = Vec::new();
        BacktestRunner::with_defaults()
            .run_raw_signals_controlled(
                &mut feed,
                Vec::new(),
                None,
                || false,
                |progress| updates.push(progress),
            )
            .unwrap();

        assert!(updates.len() >= 3);
        assert!(updates.windows(2).all(|pair| {
            pair[0].processed_events <= pair[1].processed_events
                && pair[0].processed_signals <= pair[1].processed_signals
                && pair[0].total_events <= pair[1].total_events
                && pair[0].total_signals <= pair[1].total_signals
        }));
        assert_eq!(updates.last().unwrap().processed_events, 600);
        assert_eq!(updates.last().unwrap().total_events, 600);
    }

    #[test]
    fn legacy_replay_skips_invalid_crossed_and_reversed_quotes_without_nonfinite_pnl() {
        let events = vec![
            tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
            tick("EURUSD", f64::NAN, 101.0, ts(10, 0, 1)),
            tick("EURUSD", 102.0, 101.0, ts(10, 0, 2)),
            tick("EURUSD", 90.0, 90.0, ts(9, 59, 59)),
            tick("EURUSD", 110.0, 110.0, ts(10, 0, 3)),
        ];
        let mut feed = VecFeed::new(events);
        let signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(100.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("safe-feed".into()),
        }];

        let result =
            BacktestRunner::new(fixed_lot_config()).run_raw_signals(&mut feed, signals, None);
        assert_eq!(result.trade_log.len(), 1);
        assert_eq!(result.trade_log[0].exit_price, 110.0);
        assert_eq!(result.trade_log[0].pnl, 10.0);
        assert!(result.total_pnl.is_finite());
        assert!(result.final_balance.is_finite());
    }

    #[test]
    fn legacy_and_future_profile_replay_share_empty_ratio_target_resolution() {
        let profile = ManagementProfile {
            name: "equal-target".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        let signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(100.0),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![101.0],
            group: None,
            trade_id: Some("profile-parity".into()),
        }];
        let events = vec![
            tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
            tick("EURUSD", 101.0, 101.0, ts(10, 0, 1)),
        ];

        let mut legacy_feed = VecFeed::new(events.clone());
        let legacy = BacktestRunner::new(BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        })
        .run_raw_signals(&mut legacy_feed, signals.clone(), Some(&profile));
        let mut future_feed = VecFeed::new(events);
        let future = BacktestRunner::new_future(
            BacktestConfig {
                close_on_finish: false,
                ..fixed_lot_config()
            },
            FutureQuoteConfig::default(),
        )
        .run_raw_signals_future(&mut future_feed, signals, Some(&profile));

        assert_eq!(legacy.trade_log.len(), 1);
        assert_eq!(future.trade_log.len(), 1);
        assert_eq!(legacy.trade_log[0].close_reason, CloseReason::Target);
        assert_eq!(future.trade_log[0].close_reason, CloseReason::Target);
        assert_eq!(legacy.trade_log[0].size, future.trade_log[0].size);
    }

    #[test]
    fn future_batch_sizes_from_shared_conversion_before_primary_and_uses_primary_eod() {
        let currency_plan = RunCurrencyPlan::new(
            "USD",
            ["EURUSD".to_owned()].into_iter().collect(),
            ["EURUSD".to_owned()].into_iter().collect(),
            [("EURUSD".to_owned(), "EUR".to_owned())]
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
        .unwrap();
        let mut config = fixed_lot_config();
        config.sizing = Some(SizingPolicy::FixedRiskAmount { amount: 12.0 });
        let future = FutureQuoteConfig {
            currency_plan: Some(currency_plan),
            conversion_stale_after_ms: 1_000,
            ..FutureQuoteConfig::default()
        };
        let events = vec![
            FeedEvent::new(
                tick("EURUSD", 1.1, 1.2, ts(10, 0, 0)),
                EventMetadata::new(SeriesRoles::PRIMARY_AND_CONVERSION, 0, 0),
            ),
            FeedEvent::new(
                tick("EURUSD", 2.0, 2.1, ts(10, 0, 1)),
                EventMetadata::new(SeriesRoles::CONVERSION, 1, 0),
            ),
        ];
        let signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0),
            risk_multiplier: 1.0,
            stoploss: Some(1.19),
            targets: Vec::new(),
            group: None,
            trade_id: Some("shared-conversion".into()),
        }];

        let mut feed = VecFeed::from_feed_events(events);
        let result = BacktestRunner::new_future(config, future)
            .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(result.recorded_fills.len(), 2);
        assert!((result.recorded_fills[0].fill.price - 1.2).abs() < 1.0e-12);
        assert!((result.recorded_fills[0].size - 10.0).abs() < 1.0e-12);
        assert_eq!(result.recorded_fills[1].execution_ts, Some(ts(10, 0, 0)));
        assert_eq!(result.recorded_fills[1].quote_ts, ts(10, 0, 0));
        assert!(
            result
                .mtm_equity_curve
                .iter()
                .all(|point| point.ts == ts(10, 0, 0))
        );
    }

    #[test]
    fn conversion_only_batch_revalues_but_defers_execution_to_primary_quote() {
        let currency_plan = RunCurrencyPlan::new(
            "USD",
            ["EURUSD".to_owned()].into_iter().collect(),
            ["EURUSD".to_owned()].into_iter().collect(),
            [("EURUSD".to_owned(), "EUR".to_owned())]
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
        .unwrap();
        let config = BacktestConfig {
            close_on_finish: false,
            ..fixed_lot_config()
        };
        let future = FutureQuoteConfig {
            currency_plan: Some(currency_plan),
            conversion_stale_after_ms: 10_000,
            ..FutureQuoteConfig::default()
        };
        let events = vec![
            FeedEvent::new(
                tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
                EventMetadata::new(SeriesRoles::PRIMARY_AND_CONVERSION, 0, 0),
            ),
            FeedEvent::new(
                tick("EURUSD", 2.0, 2.0, ts(10, 0, 1)),
                EventMetadata::new(SeriesRoles::CONVERSION, 1, 0),
            ),
            FeedEvent::new(
                tick("EURUSD", 110.0, 110.0, ts(10, 0, 2)),
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 1),
            ),
        ];
        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: None,
                targets: Vec::new(),
                group: None,
                trade_id: Some("conversion-only".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "conversion-only".into(),
                },
            },
        ];

        let mut feed = VecFeed::from_feed_events(events);
        let result = BacktestRunner::new_future(config, future)
            .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(result.recorded_fills.len(), 2);
        assert_eq!(result.recorded_fills[0].quote_ts, ts(10, 0, 0));
        assert_eq!(result.recorded_fills[1].quote_ts, ts(10, 0, 2));
        assert!(
            result
                .mtm_equity_curve
                .iter()
                .any(|point| point.ts == ts(10, 0, 1))
        );
        assert_eq!(result.total_pnl, 20.0);
        assert_eq!(result.close_events[0].native_pnl, Some(10.0));
        assert_eq!(
            result.close_events[0]
                .pnl_conversion
                .as_ref()
                .unwrap()
                .operation_ts,
            ts(10, 0, 2)
        );
    }

    #[test]
    fn exact_timestamp_close_updates_balance_before_later_risk_entry() {
        let mut config = fixed_lot_config();
        config.close_on_finish = false;
        config.sizing = Some(SizingPolicy::BalanceRiskPercent { percent: 1.0 });
        let spec = config.symbol_specs.get_mut("EURUSD").unwrap();
        spec.digits = 2;
        spec.pip_position = 2;
        spec.lot_base_units = 1;
        spec.lot_step_units = 1;
        let future = FutureQuoteConfig {
            currency_plan: Some(identity_currency_plan("EURUSD")),
            ..FutureQuoteConfig::default()
        };
        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: Some(99.0),
                targets: Vec::new(),
                group: None,
                trade_id: Some("first".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "first".into(),
                },
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: Some(100.0),
                targets: Vec::new(),
                group: None,
                trade_id: Some("second".into()),
            },
        ];
        let mut feed = VecFeed::new(vec![
            tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
            tick("EURUSD", 101.0, 101.0, ts(10, 0, 1)),
        ]);

        let result = BacktestRunner::new_future(config, future)
            .run_raw_signals_future(&mut feed, signals, None);

        assert!((result.total_pnl - 100.0).abs() < 1.0e-12);
        assert_eq!(result.open_position_snapshots.len(), 1);
        assert_eq!(
            result.open_position_snapshots[0].trade_id.as_deref(),
            Some("second")
        );
        assert!((result.open_position_snapshots[0].remaining_size - 101.0).abs() < 1.0e-12);
    }

    #[test]
    fn pending_fill_keeps_placement_size_after_balance_changes() {
        let mut config = fixed_lot_config();
        config.close_on_finish = false;
        config.sizing = Some(SizingPolicy::BalanceRiskPercent { percent: 1.0 });
        let spec = config.symbol_specs.get_mut("EURUSD").unwrap();
        spec.digits = 2;
        spec.pip_position = 2;
        spec.lot_base_units = 1;
        spec.lot_step_units = 1;
        let future = FutureQuoteConfig {
            currency_plan: Some(identity_currency_plan("EURUSD")),
            ..FutureQuoteConfig::default()
        };
        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 1.0,
                stoploss: Some(99.0),
                targets: Vec::new(),
                group: None,
                trade_id: Some("market".into()),
            },
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: Some(99.0),
                risk_multiplier: 1.0,
                stoploss: Some(98.0),
                targets: Vec::new(),
                group: None,
                trade_id: Some("pending".into()),
            },
            RawSignal::Close {
                ts: ts(10, 0, 1),
                position: PositionRef::ByTradeId {
                    trade_id: "market".into(),
                },
            },
        ];
        let mut feed = VecFeed::new(vec![
            tick("EURUSD", 100.0, 100.0, ts(10, 0, 0)),
            tick("EURUSD", 101.0, 101.0, ts(10, 0, 1)),
            tick("EURUSD", 99.0, 99.0, ts(10, 0, 2)),
        ]);

        let result = BacktestRunner::new_future(config, future)
            .run_raw_signals_future(&mut feed, signals, None);

        assert_eq!(result.pending_order_snapshots.len(), 0);
        assert_eq!(result.open_position_snapshots.len(), 1);
        assert_eq!(
            result.open_position_snapshots[0].trade_id.as_deref(),
            Some("pending")
        );
        assert!((result.open_position_snapshots[0].remaining_size - 100.0).abs() < 1.0e-12);
    }

    #[test]
    fn raw_entries_require_sizing_but_management_only_replay_does_not() {
        let entry = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: None,
        };
        let mut entry_feed = VecFeed::new(vec![tick("EURUSD", 100.0, 100.0, ts(10, 0, 0))]);
        let rejected =
            BacktestRunner::new_future(BacktestConfig::default(), FutureQuoteConfig::default())
                .run_raw_signals_future(&mut entry_feed, vec![entry], None);
        assert!(rejected.action_dispositions.iter().any(|disposition| {
            disposition.action_id == "configuration"
                && disposition
                    .reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("BacktestConfig.sizing"))
        }));

        let mut management_feed = VecFeed::new(vec![tick("EURUSD", 100.0, 100.0, ts(10, 0, 0))]);
        let management =
            BacktestRunner::new_future(BacktestConfig::default(), FutureQuoteConfig::default())
                .run_raw_signals_future(
                    &mut management_feed,
                    vec![RawSignal::CloseAll { ts: ts(10, 0, 0) }],
                    None,
                );
        assert!(
            management
                .action_dispositions
                .iter()
                .all(|disposition| disposition.action_id != "configuration")
        );
    }

    #[test]
    fn server_filter_signals_before_market_window() {
        // Regression test for Issue 1 Part 4:
        // Verify the runner does NOT filter pre-window signals (library level).
        // The server filter is tested separately in handlers.
        // Here we verify that signals with ts before first market event
        // ARE still injected (library behavior). Server filtering removes them.
        let events = vec![
            tick("EURUSD", 1.0848, 1.0850, ts(10, 0, 0)),
            tick("EURUSD", 1.0900, 1.0902, ts(10, 0, 1)),
        ];
        let mut feed = VecFeed::new(events);

        // Signal from January, market data from "today" (ts(10,0,0)).
        let raw_signals = vec![RawSignal::Entry {
            ts: NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![1.0900],
            group: None,
            trade_id: None,
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // Library still injects it; server filtering is the authoritative gate.
        assert_eq!(result.total_trades, 1);
    }
}
