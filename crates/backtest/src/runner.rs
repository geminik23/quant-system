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
use qs_core::sizing::{
    compute_instrument_native_loss_per_lot, compute_instrument_size_for_spec_with_prices,
};
use qs_core::types::{
    Action, CloseReason, Effect, ExecutionFill, ExecutionModel, FillModel, FutureEffect, OrderType,
    PositionStatus, PreparedPendingFill, PriceQuote, Side, SlippageModel, position_size_tolerance,
};
use qs_core::{ExecutionPricer, FutureApplyError, TradeEngine};
use qs_instruments::{
    Decimal, DecimalGrid, EconomicsModelId, InstrumentSpec, ListingStatus, PositiveDecimal,
    QuantityUnit,
};

use crate::artifacts::{
    EntryProfileResolutionAudit, EntryProfileSelectionSource, EntryResolutionStage,
    ExecutionMetadata, FUTURE_ARTIFACT_FORMAT_VERSION, FutureBacktestArtifacts,
    InstrumentSizingArtifact, MarketEntrySizingAudit, MarketEntrySizingBasis, PendingOrderSnapshot,
    ReplayInstrumentManifest,
};
use crate::currency::{ConversionQuoteBook, RunCurrencyPlan};
use crate::data_feed::{
    BarExecutionPrices, DataFeed, FallibleBatchFeed, FeedEvent, MarketEvent, TimestampBatch,
};
use crate::economic_support::{LEGACY_ECONOMIC_GUARD_ID, resolve_legacy_economics};
use crate::evaluation::EvaluationOptions;
use crate::executor::BacktestExecutor;
use crate::future_executor::{FutureExecutor, FutureExecutorError};
use crate::ledger::{ActionDisposition, ActionDispositionStatus, LifecycleLedger};
use crate::mtm::{MtmCurveCollector, MtmOutputPolicy, MtmOutputSummary};
use crate::portfolio::{EquityPoint, PortfolioRecorder};
use crate::profile::{
    EntryProfileRoutingError, EntryResolutionContext, ManagementProfile, PreparedEntryProfiles,
    PriceGridSource, RawSignal, ResolvedEntry, allocate_target_steps, resolve_signal,
    resolve_unprofiled_entry,
};
use crate::report::BacktestResult;
use crate::sizing::{SizingPolicy, compute_native_loss_per_lot, compute_size};
use crate::strategy::configured::BoundaryPositionFacts;
use crate::strategy::{
    AnalysisBoundary, AnalysisPipeline, BacktestConfiguredStrategyAdapter, BarSeriesSpec,
    ConfiguredStrategyAdapterError, HistoricalStrategy, MultiTimeframeSeries, Strategy,
    StrategyBacktestResult, StrategyContext, StrategyDecisionRecorder, StrategyEvent,
    StrategyFeedback, StrategyFeedbackEvent, StrategyJournalRecorder, StrategyReplayError,
    StrategyReplayInputError, StrategyResearchLimits, StrategyResearchOutput,
    StrategyRetentionLimits,
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
    /// Selects the risk reference price for market-entry sizing.
    pub market_entry_sizing_basis: MarketEntrySizingBasis,
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
            market_entry_sizing_basis: MarketEntrySizingBasis::default(),
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

mod portfolio_replay;

/// Upper bound on the quotes one leg of a bar's range walk may settle.
const MAX_BAR_LEG_STEPS: usize = 100_000;

/// Which price of a quote a trigger level is compared against.
#[derive(Debug, Clone, Copy)]
enum QuoteSide {
    Bid,
    Ask,
    Mid,
}

fn should_report_progress(processed: usize, total: usize) -> bool {
    processed == total || processed.is_multiple_of(REPLAY_PROGRESS_INTERVAL)
}

#[derive(Debug, Clone)]
pub(crate) struct ScheduledSignal {
    sequence: u64,
    signal_ts: NaiveDateTime,
    effective_ts: NaiveDateTime,
    signal: RawSignal,
    action_id: Option<String>,
    /// Whether `action_id` names exactly one action; a base identifier instead prefixes every action a bulk signal resolves to.
    explicit_action_id: bool,
    requires_later_quote: bool,
    /// Portfolio instance that generated the signal, which selects its entry profiles and labels its positions.
    instance: Option<usize>,
}

impl ScheduledSignal {
    pub(crate) fn new(
        sequence: u64,
        signal_ts: NaiveDateTime,
        effective_ts: NaiveDateTime,
        signal: RawSignal,
        requires_later_quote: bool,
    ) -> Self {
        Self {
            sequence,
            signal_ts,
            effective_ts,
            signal,
            action_id: None,
            explicit_action_id: false,
            requires_later_quote,
            instance: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_action_id(mut self, action_id: impl Into<String>) -> Self {
        self.action_id = Some(action_id.into());
        self.explicit_action_id = true;
        self
    }

    /// Identify the signal by a base identifier that each resolved action extends, as a bulk signal needs.
    fn with_action_base(mut self, base: impl Into<String>) -> Self {
        self.action_id = Some(base.into());
        self.explicit_action_id = false;
        self
    }

    fn resolved_action_id(&self) -> String {
        self.action_id
            .clone()
            .unwrap_or_else(|| format!("signal:{:08}", self.sequence))
    }
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
    entry_profile_selection_source: Option<EntryProfileSelectionSource>,
    selected_profile_name: Option<String>,
    market_entry_sizing_audit: Option<MarketEntrySizingAudit>,
    entry_profile_resolution_audit: Option<EntryProfileResolutionAudit>,
    requires_later_quote: bool,
}

#[derive(Debug, Clone)]
struct SelectedEntryProfile {
    profile: Option<ManagementProfile>,
    source: EntryProfileSelectionSource,
    entry_class: Option<String>,
    profile_name: Option<String>,
}

struct FinalizedEntry {
    action: Action,
    requested_account_risk: Option<f64>,
    native_loss_per_lot: Option<f64>,
    account_loss_per_lot: Option<f64>,
    final_lot: f64,
    level_resolution: qs_core::EntryLevelResolution,
    target_resolution: qs_core::TargetResolution,
    configured_weights: Vec<f64>,
    allocated_target_steps: Vec<u64>,
    remainder_steps: u64,
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
    fn reject_generated_configuration(&mut self, instance: Option<usize>, reason: String);
    /// Whether the boundary reads open-position economics, which makes the replay snapshot campaign excursion at the start of every batch.
    fn observes_position_economics(&self) -> bool;
    /// Stored-bar duration that drives execution for each symbol whose series the hook declares. A primary bar of another duration still reaches the hook's series but is not executed, so a symbol read at several bar lengths is executed once, on its shortest bars. A symbol absent from the map executes every bar it receives.
    fn bar_execution_timeframes(&self) -> BTreeMap<String, u64> {
        BTreeMap::new()
    }
    /// Whether the hook's strategies read a stored bar only once its bucket has closed. Such a hook decides on a bar batch before the new bars trade, so its orders may fill at their open; a hook that reads the batch's own bars, as a direct strategy does through its primary events, decides after the bars settle and fills at the next bar, because it has already seen the bar's close.
    fn reads_completed_bars_only(&self) -> bool {
        false
    }
    /// Supervision state of a multi-instance replay, which the replay consults before scheduling new exposure.
    fn portfolio_state(&mut self) -> Option<&mut portfolio_replay::PortfolioReplayState> {
        None
    }
    #[allow(clippy::too_many_arguments)]
    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> Option<Vec<ScheduledSignal>>;
    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool;
}

/// Shortest declared series duration per symbol, which is the bar length a strategy-driven replay executes on.
fn shortest_series_per_symbol(
    requirements: &crate::strategy::StrategyRequirements,
) -> BTreeMap<String, u64> {
    let mut shortest = BTreeMap::<String, u64>::new();
    for series in requirements.series() {
        let seconds = series.timeframe().duration_seconds();
        shortest
            .entry(series.symbol().to_owned())
            .and_modify(|current| *current = (*current).min(seconds))
            .or_insert(seconds);
    }
    shortest
}

struct StaticReplayHook;

impl FutureReplayHook for StaticReplayHook {
    fn observes_position_economics(&self) -> bool {
        false
    }

    fn is_active(&self) -> bool {
        false
    }

    fn output_ready(&self) -> bool {
        true
    }

    fn preflight_primary_events(&mut self, _events: &[FeedEvent]) -> bool {
        true
    }

    fn reject_generated_configuration(&mut self, _instance: Option<usize>, _reason: String) {
        unreachable!("static replay does not generate strategy signals");
    }

    fn on_boundary(
        &mut self,
        _batch: &TimestampBatch,
        _engine: &TradeEngine,
        _lifecycle: &LifecycleLedger,
        _positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> Option<Vec<ScheduledSignal>> {
        pending_effects.clear();
        pending_events.clear();
        Some(Vec::new())
    }

    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool {
        pending_effects.clear();
        pending_events.clear();
        true
    }
}

struct StrategyReplayDriver<'a, S: HistoricalStrategy> {
    strategy: &'a mut S,
    requirements: crate::strategy::StrategyRequirements,
    series: MultiTimeframeSeries,
    analysis: AnalysisPipeline,
    limits: StrategyRetentionLimits,
    decisions: StrategyDecisionRecorder,
    journal: StrategyJournalRecorder,
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
        research_limits: StrategyResearchLimits,
    ) -> Self {
        Self {
            requirements: strategy.requirements().clone(),
            strategy,
            series,
            analysis,
            limits,
            decisions: StrategyDecisionRecorder::new(limits),
            journal: StrategyJournalRecorder::new(research_limits),
            next_decision_sequence: 0,
            next_signal_sequence: 0,
            delivered_dispositions: 0,
            warmup_complete: false,
            failure: None,
        }
    }

    fn finish(
        self,
    ) -> Result<
        (
            crate::strategy::StrategyDecisionOutput,
            StrategyResearchOutput,
        ),
        StrategyDriverError<S::Error>,
    > {
        match self.failure {
            Some(error) => Err(error),
            None => Ok((
                self.decisions.finish(),
                StrategyResearchOutput {
                    journal: self.journal.finish(),
                    research_annotations: self.analysis.into_research_annotations(),
                },
            )),
        }
    }

    fn fail(&mut self, error: StrategyDriverError<S::Error>) -> Option<Vec<ScheduledSignal>> {
        self.failure = Some(error);
        None
    }
}

impl<S: HistoricalStrategy> FutureReplayHook for StrategyReplayDriver<'_, S> {
    fn observes_position_economics(&self) -> bool {
        false
    }

    fn bar_execution_timeframes(&self) -> BTreeMap<String, u64> {
        shortest_series_per_symbol(&self.requirements)
    }

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

    fn reject_generated_configuration(&mut self, _instance: Option<usize>, reason: String) {
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
        _positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
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
        let feedback = StrategyFeedback::with_events(
            pending_effects,
            &lifecycle.as_slice()[self.delivered_dispositions..disposition_end],
            pending_events,
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
        pending_events.clear();
        self.delivered_dispositions = disposition_end;

        let (decision, journal) = output.into_parts();
        if let Err(error) = self.journal.push_callback(batch.ts, journal) {
            return self.fail(StrategyDriverError::Runtime(
                crate::strategy::StrategyRuntimeError::Journal(error),
            ));
        }
        let Some(draft) = decision else {
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
            scheduled.push(ScheduledSignal::new(
                sequence,
                batch.ts,
                effective_ts,
                signal,
                true,
            ));
        }
        Some(scheduled)
    }

    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool {
        pending_effects.clear();
        pending_events.clear();
        true
    }
}

struct ConfiguredStrategyReplayDriver<'a> {
    adapter: &'a mut BacktestConfiguredStrategyAdapter,
    requirements: crate::strategy::StrategyRequirements,
    series: MultiTimeframeSeries,
    analysis: AnalysisPipeline,
    limits: StrategyRetentionLimits,
    research_limits: StrategyResearchLimits,
    decisions: StrategyDecisionRecorder,
    journal: StrategyJournalRecorder,
    next_decision_sequence: u64,
    next_signal_sequence: u64,
    warmup_complete: bool,
    failure: Option<StrategyDriverError<ConfiguredStrategyAdapterError>>,
}

impl<'a> ConfiguredStrategyReplayDriver<'a> {
    fn new(
        adapter: &'a mut BacktestConfiguredStrategyAdapter,
        series: MultiTimeframeSeries,
        analysis: AnalysisPipeline,
        limits: StrategyRetentionLimits,
        research_limits: StrategyResearchLimits,
    ) -> Self {
        Self {
            requirements: adapter.requirements().clone(),
            adapter,
            series,
            analysis,
            limits,
            research_limits,
            decisions: StrategyDecisionRecorder::new(limits),
            journal: StrategyJournalRecorder::new(research_limits),
            next_decision_sequence: 0,
            next_signal_sequence: 0,
            warmup_complete: false,
            failure: None,
        }
    }

    fn finish(
        self,
    ) -> Result<
        (
            crate::strategy::StrategyDecisionOutput,
            StrategyResearchOutput,
        ),
        StrategyDriverError<ConfiguredStrategyAdapterError>,
    > {
        match self.failure {
            Some(error) => Err(error),
            None => Ok((
                self.decisions.finish(),
                StrategyResearchOutput {
                    journal: self.journal.finish(),
                    research_annotations: self.analysis.into_research_annotations(),
                },
            )),
        }
    }

    fn fail(
        &mut self,
        error: StrategyDriverError<ConfiguredStrategyAdapterError>,
    ) -> Option<Vec<ScheduledSignal>> {
        self.failure = Some(error);
        None
    }
}

impl FutureReplayHook for ConfiguredStrategyReplayDriver<'_> {
    fn observes_position_economics(&self) -> bool {
        true
    }

    fn bar_execution_timeframes(&self) -> BTreeMap<String, u64> {
        shortest_series_per_symbol(&self.requirements)
    }

    fn reads_completed_bars_only(&self) -> bool {
        true
    }

    fn is_active(&self) -> bool {
        true
    }

    fn output_ready(&self) -> bool {
        self.warmup_complete
    }

    /// Configured strategies accept stored bars as completed bars; the series validates their geometry and rejects anything it cannot place.
    fn preflight_primary_events(&mut self, _events: &[FeedEvent]) -> bool {
        true
    }

    fn reject_generated_configuration(&mut self, _instance: Option<usize>, reason: String) {
        self.failure = Some(StrategyDriverError::InvalidGeneratedSignal {
            signal_index: 0,
            reason,
        });
    }

    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        _lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
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
        let output = match self.adapter.evaluate_boundary(
            batch.ts,
            self.warmup_complete,
            &closed_bars,
            &observations,
            &self.series,
            self.analysis.observations(),
            engine,
            positions,
            pending_events,
            self.limits,
            self.research_limits,
        ) {
            Ok(output) => output,
            Err(error) => return self.fail(StrategyDriverError::Strategy(error)),
        };
        pending_effects.clear();
        pending_events.clear();

        if let Err(error) = self.journal.push_callback(batch.ts, output.journal) {
            return self.fail(StrategyDriverError::Runtime(
                crate::strategy::StrategyRuntimeError::Journal(error),
            ));
        }
        if let Some(decision) = output.decision {
            let record =
                match decision.into_record(self.next_decision_sequence, batch.ts, self.limits) {
                    Ok(record) => record,
                    Err(error) => return self.fail(StrategyDriverError::Runtime(error)),
                };
            if let Err(error) = self.decisions.push(record) {
                return self.fail(StrategyDriverError::Runtime(
                    crate::strategy::StrategyRuntimeError::Domain(error),
                ));
            }
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
        }

        if !self.warmup_complete && !output.commands.is_empty() {
            return self.fail(StrategyDriverError::WarmupSignals {
                timestamp: batch.ts,
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
        let mut scheduled = Vec::with_capacity(output.commands.len());
        for (signal_index, command) in output.commands.into_iter().enumerate() {
            if let Err(error) = qs_core::validation::validate_raw_signal(&command.signal) {
                return self.fail(StrategyDriverError::InvalidGeneratedSignal {
                    signal_index,
                    reason: error.to_string(),
                });
            }
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
            scheduled.push(
                ScheduledSignal::new(sequence, batch.ts, effective_ts, command.signal, true)
                    .with_action_id(command.command_id),
            );
        }
        Some(scheduled)
    }

    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool {
        pending_effects.clear();
        match self.adapter.finalize_feedback(pending_events) {
            Ok(()) => {
                pending_events.clear();
                true
            }
            Err(error) => {
                self.failure = Some(StrategyDriverError::Strategy(error));
                false
            }
        }
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
    /// Per-symbol symmetric bid/ask spread in price units, applied to bars that carry no recorded spread.
    ///
    /// Stored bars normally carry the average spread observed while they formed. This map covers feeds that cannot supply one; without it such bars execute at a zero spread, and the run reports how often that happened.
    pub bar_spread_fallback: HashMap<String, f64>,
    /// Caller-supplied labels recorded with the run and attached to every completed position.
    ///
    /// A batch of runs uses these to say what distinguishes one run from another, such as the parameter values or the data window, so that evaluation can break results down by them afterwards. They are inert: the engine never reads a tag to decide anything. Keys and values are bounded and validated before replay starts, and they are kept separate from the engine's own diagnostic tags so neither can overwrite the other.
    pub run_tags: BTreeMap<String, String>,
    /// Per-symbol commission and swap specification.
    ///
    /// An empty map charges nothing and reproduces runs made before costs existed. Point-denominated swap additionally requires a matching `symbol_specs` entry, because the price point size comes from its digit count.
    pub costs: HashMap<String, qs_core::InstrumentCosts>,
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
            bar_spread_fallback: HashMap::new(),
            run_tags: BTreeMap::new(),
            costs: HashMap::new(),
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
    strategy_research_limits: StrategyResearchLimits,
    instrument_sizing: Vec<InstrumentSizingArtifact>,
    market_entry_sizing: Vec<MarketEntrySizingAudit>,
    entry_profile_resolutions: Vec<EntryProfileResolutionAudit>,
    committed_feedback: Vec<FutureEffect>,
    committed_feedback_events: Vec<StrategyFeedbackEvent>,
    entry_profiles: Option<PreparedEntryProfiles>,
    /// Entry profiles of each portfolio instance, selected by the instance a signal came from.
    instance_profiles: Vec<PreparedEntryProfiles>,
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
            strategy_research_limits: StrategyResearchLimits::default(),
            instrument_sizing: Vec::new(),
            market_entry_sizing: Vec::new(),
            entry_profile_resolutions: Vec::new(),
            committed_feedback: Vec::new(),
            committed_feedback_events: Vec::new(),
            entry_profiles: None,
            instance_profiles: Vec::new(),
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
            strategy_research_limits: StrategyResearchLimits::default(),
            instrument_sizing: Vec::new(),
            market_entry_sizing: Vec::new(),
            entry_profile_resolutions: Vec::new(),
            committed_feedback: Vec::new(),
            committed_feedback_events: Vec::new(),
            entry_profiles: None,
            instance_profiles: Vec::new(),
        }
    }

    /// Create a runner with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BacktestConfig::default())
    }

    /// Apply an immutable per-entry profile routing snapshot.
    pub fn with_entry_profiles(mut self, profiles: PreparedEntryProfiles) -> Self {
        self.entry_profiles = Some(profiles);
        self
    }

    /// Apply typed provider-evaluation options to FutureQuoteV1 results.
    /// Legacy execution ignores these options and preserves its existing report.
    pub fn with_evaluation_options(mut self, options: EvaluationOptions) -> Self {
        self.evaluation_options = options;
        self
    }

    /// Apply journal bounds to historical strategy replay.
    pub fn with_strategy_research_limits(mut self, limits: StrategyResearchLimits) -> Self {
        self.strategy_research_limits = limits;
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
            || self.validate_entry_profile_routes(&raw_signals).is_err()
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
        if let Err(error) = self.validate_entry_profile_routes(&raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
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
        if let Err(error) = self.validate_entry_profile_routes(&raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
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
        let mut hook = StrategyReplayDriver::new(
            strategy,
            series,
            analysis,
            retention,
            self.strategy_research_limits,
        );
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
        let (decisions, research) = hook.finish().map_err(map_strategy_driver_error)?;
        Ok(StrategyBacktestResult {
            replay,
            descriptor,
            decisions,
            research,
        })
    }

    /// Run a configured strategy from a materialized data feed through FutureQuote.
    #[allow(clippy::too_many_arguments)]
    pub fn run_configured_strategy_future<F>(
        self,
        source_feed: &mut F,
        adapter: &mut BacktestConfiguredStrategyAdapter,
        analysis: AnalysisPipeline,
        retention: StrategyRetentionLimits,
        profile: Option<&ManagementProfile>,
    ) -> Result<
        StrategyBacktestResult,
        StrategyReplayError<Infallible, ConfiguredStrategyAdapterError>,
    >
    where
        F: DataFeed,
    {
        self.preflight_configured_entry_profiles(adapter, profile)?;
        adapter
            .preflight(retention, self.strategy_research_limits)
            .map_err(StrategyReplayInputError::ConfiguredAdapter)?;
        let series_specs = adapter.series_specs().cloned().collect::<Vec<_>>();
        crate::strategy::replay::validate_series_specs(adapter.requirements(), &series_specs)?;
        MultiTimeframeSeries::new(series_specs.clone())?;
        let future = self.future_config.clone().unwrap_or_default();
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(StrategyReplayInputError::FutureQuote)?;

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
        self.run_configured_strategy_future_streaming(
            &mut feed,
            primary_eod,
            adapter,
            analysis,
            retention,
            profile,
        )
    }

    /// Run a configured strategy from complete ordered timestamp batches.
    #[allow(clippy::too_many_arguments)]
    pub fn run_configured_strategy_future_streaming<F>(
        self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        adapter: &mut BacktestConfiguredStrategyAdapter,
        analysis: AnalysisPipeline,
        retention: StrategyRetentionLimits,
        profile: Option<&ManagementProfile>,
    ) -> Result<StrategyBacktestResult, StrategyReplayError<F::Error, ConfiguredStrategyAdapterError>>
    where
        F: FallibleBatchFeed,
    {
        self.run_configured_strategy_future_streaming_controlled(
            feed,
            primary_eod,
            adapter,
            analysis,
            retention,
            profile,
            || false,
            |_| {},
        )
    }

    /// Run a configured strategy from complete ordered timestamp batches with cooperative cancellation and replay progress, as the retained-job service does for raw-signal replay.
    #[allow(clippy::too_many_arguments)]
    pub fn run_configured_strategy_future_streaming_controlled<F, C, P>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        adapter: &mut BacktestConfiguredStrategyAdapter,
        analysis: AnalysisPipeline,
        retention: StrategyRetentionLimits,
        profile: Option<&ManagementProfile>,
        mut is_cancelled: C,
        mut on_progress: P,
    ) -> Result<StrategyBacktestResult, StrategyReplayError<F::Error, ConfiguredStrategyAdapterError>>
    where
        F: FallibleBatchFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        self.preflight_configured_entry_profiles(adapter, profile)?;
        adapter
            .preflight(retention, self.strategy_research_limits)
            .map_err(StrategyReplayInputError::ConfiguredAdapter)?;
        let series_specs = adapter.series_specs().cloned().collect::<Vec<_>>();
        crate::strategy::replay::validate_series_specs(adapter.requirements(), &series_specs)?;
        let future = self.future_config.clone().unwrap_or_default();
        self.future_config = Some(future.clone());
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(StrategyReplayInputError::FutureQuote)?;
        let descriptor = adapter.descriptor().clone();
        let series = MultiTimeframeSeries::new(series_specs)?;
        let mut hook = ConfiguredStrategyReplayDriver::new(
            adapter,
            series,
            analysis,
            retention,
            self.strategy_research_limits,
        );
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
            Err(FutureBatchReplayError::Cancelled) => return Err(StrategyReplayError::Cancelled),
            Err(FutureBatchReplayError::Dynamic) => {
                let error = hook.finish().expect_err("dynamic failure stores its cause");
                return Err(map_strategy_driver_error(error));
            }
        };
        let (decisions, research) = hook.finish().map_err(map_strategy_driver_error)?;
        Ok(StrategyBacktestResult {
            replay,
            descriptor,
            decisions,
            research,
        })
    }

    /// Validate a supplied run default profile and check the configured strategy's entries against the profiles replay will select, so an unrouted class or a stop-owner conflict fails before any feed is read.
    fn preflight_configured_entry_profiles(
        &self,
        adapter: &BacktestConfiguredStrategyAdapter,
        profile: Option<&ManagementProfile>,
    ) -> Result<(), StrategyReplayInputError> {
        if let Some(profile) = profile {
            profile
                .validate()
                .map_err(|error| StrategyReplayInputError::ManagementProfile(error.to_string()))?;
        }
        let run_default;
        let profiles = match self.entry_profiles.as_ref() {
            Some(profiles) => profiles,
            None => {
                run_default = PreparedEntryProfiles::default_only(profile.cloned());
                &run_default
            }
        };
        adapter.preflight_entry_profiles(profiles)?;
        Ok(())
    }

    fn validate_entry_profile_routes(&self, signals: &[RawSignal]) -> Result<(), String> {
        if let Some(profiles) = self.entry_profiles.as_ref() {
            return profiles
                .validate_signals(signals)
                .map_err(|error| error.to_string());
        }
        if let Some(entry_class) = signals.iter().find_map(|signal| match signal {
            RawSignal::Entry {
                entry_class: Some(entry_class),
                ..
            } => Some(entry_class),
            _ => None,
        }) {
            return Err(
                EntryProfileRoutingError::UnknownEntryClass(entry_class.clone()).to_string(),
            );
        }
        Ok(())
    }

    fn select_entry_profile(
        &self,
        signal: &RawSignal,
        fallback: Option<&ManagementProfile>,
        instance: Option<usize>,
    ) -> Result<SelectedEntryProfile, EntryProfileRoutingError> {
        let entry_class = match signal {
            RawSignal::Entry { entry_class, .. } => entry_class.clone(),
            _ => None,
        };
        let instance_profiles = instance.and_then(|instance| self.instance_profiles.get(instance));
        if let Some(profiles) = instance_profiles.or(self.entry_profiles.as_ref()) {
            let profile = profiles.select(signal)?.cloned();
            let source = if entry_class.is_some() {
                EntryProfileSelectionSource::Mapped
            } else if profile.is_some() {
                EntryProfileSelectionSource::RunDefault
            } else {
                EntryProfileSelectionSource::Unprofiled
            };
            let profile_name = profile.as_ref().map(|profile| profile.name.clone());
            return Ok(SelectedEntryProfile {
                profile,
                source,
                entry_class,
                profile_name,
            });
        }
        if let Some(entry_class) = entry_class {
            return Err(EntryProfileRoutingError::UnknownEntryClass(entry_class));
        }
        let profile = fallback.cloned();
        let source = if profile.is_some() {
            EntryProfileSelectionSource::RunDefault
        } else {
            EntryProfileSelectionSource::Unprofiled
        };
        let profile_name = profile.as_ref().map(|profile| profile.name.clone());
        Ok(SelectedEntryProfile {
            profile,
            source,
            entry_class: None,
            profile_name,
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
            let selected_profile = match self.select_entry_profile(&signal, profile, None) {
                Ok(profile) => profile,
                Err(_) => return,
            };
            let resolved = match selected_profile.profile.as_ref() {
                Some(profile) => self.resolve_profiled_entry(profile, &signal),
                None => resolve_unprofiled_entry(&signal),
            };
            if let Ok(Some(resolved)) = resolved
                && let Ok(action) =
                    self.finalize_resolved_entry(resolved, self.executor.balance, ts, None, None)
            {
                self.apply_single_action(action.action, ts, quote);
            }
        } else {
            let actions = resolve_signal(signal, &self.engine);
            for action in actions {
                self.apply_single_action(action, ts, quote);
            }
        }
    }

    fn resolve_profiled_entry(
        &self,
        profile: &ManagementProfile,
        signal: &RawSignal,
    ) -> Result<Option<ResolvedEntry>, qs_core::ProfileApplicationError> {
        let symbol = match signal {
            RawSignal::Entry { symbol, .. } => symbol,
            _ => return profile.apply_entry_signal(signal),
        };
        match self.entry_resolution_context(symbol) {
            Ok(context) => profile.apply_entry_signal_with_context(signal, context),
            Err(_) => profile.apply_entry_signal(signal),
        }
    }

    fn entry_resolution_context(&self, symbol: &str) -> Result<EntryResolutionContext, String> {
        if let Some(spec) = explicit_instrument_spec(&self.config, symbol) {
            return Ok(EntryResolutionContext {
                price_grid: spec.price.grid,
                price_grid_source: PriceGridSource::InstrumentPriceGrid,
            });
        }
        let legacy = self
            .config
            .symbol_specs
            .get(symbol)
            .ok_or_else(|| format!("missing price grid for {symbol}"))?;
        let scale = u8::try_from(legacy.digits)
            .map_err(|_| format!("price scale is too large for {symbol}"))?;
        let step = Decimal::new(1, scale).map_err(|error| error.to_string())?;
        let step = PositiveDecimal::new(step).map_err(|error| error.to_string())?;
        Ok(EntryResolutionContext {
            price_grid: DecimalGrid::new(Decimal::ZERO, step),
            price_grid_source: PriceGridSource::LegacyDigitsFallback,
        })
    }

    fn finalize_resolved_entry(
        &mut self,
        mut resolved: ResolvedEntry,
        balance_before: f64,
        operation_ts: NaiveDateTime,
        conversion_quotes: Option<&ConversionQuoteBook>,
        sizing_reference_price: Option<f64>,
    ) -> Result<FinalizedEntry, String> {
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
        let sizing_reference_price = sizing_reference_price.unwrap_or(entry_price);
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
                    sizing_reference_price,
                    stop,
                    u16::from(spec.price.display_scale),
                    &spec.economics,
                )
                .map_err(|error| error.to_string())?,
                None => compute_native_loss_per_lot(
                    resolved.side,
                    sizing_reference_price,
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
            Some(spec) => compute_instrument_size_for_spec_with_prices(
                policy,
                resolved.risk_multiplier,
                balance_before,
                resolved.side,
                sizing_reference_price,
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
                sizing_reference_price,
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
        let configured_weights = resolved.target_resolution.weights.clone();
        let target_resolution = resolved.target_resolution.clone();
        let level_resolution = resolved.level_resolution.clone();
        let target_steps = allocate_target_steps(
            sizing.final_lot_steps,
            &configured_weights,
            resolved.target_resolution.remainder,
        )
        .map_err(|error| error.to_string())?;
        if target_steps.len() != resolved.targets.len() {
            return Err("target allocation does not match resolved targets".to_owned());
        }
        for (target, steps) in resolved.targets.iter_mut().zip(&target_steps) {
            target.close_ratio = *steps as f64 / sizing.final_lot_steps as f64;
        }
        let allocated_steps: u64 = target_steps.iter().sum();
        let remainder_steps = sizing.final_lot_steps.saturating_sub(allocated_steps);

        Ok(FinalizedEntry {
            action: resolved.into_action(sizing.final_lot),
            requested_account_risk: sizing.requested_account_risk,
            native_loss_per_lot: sizing.native_loss_per_lot,
            account_loss_per_lot: sizing.account_loss_per_lot,
            final_lot: sizing.final_lot,
            level_resolution,
            target_resolution,
            configured_weights,
            allocated_target_steps: target_steps,
            remainder_steps,
        })
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
        if let Err(error) = self.validate_entry_profile_routes(&raw_signals) {
            return Ok(rejected_future_result(
                &self.config,
                &future,
                self.evaluation_options,
                error,
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
                ScheduledSignal::new(
                    sequence as u64,
                    signal_ts,
                    signal_ts
                        .checked_add_signed(Duration::milliseconds(future.signal_latency_ms))
                        .expect("signal latency overflow was validated before scheduling"),
                    signal,
                    false,
                )
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
        .with_currency_plan(future.currency_plan.clone())
        .with_costs(
            self.config.costs.clone(),
            effective_point_sizes(&self.config),
        );
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
        let mut unconverted_cost_events = 0u64;
        let mut zero_spread_bar_quotes = 0u64;
        let mut last_processed_primary_ts = None;
        let mut effective_terminal_ts = primary_eod;
        let mut terminated_quiescently = false;
        let bar_execution_timeframes = hook.bar_execution_timeframes();

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
            let boundary_excursions = if hook.observes_position_economics() {
                portfolio.open_campaign_excursions()
            } else {
                BTreeMap::new()
            };
            let mut accepted = Vec::new();
            for feed_event in batch.events {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                let fallback = self
                    .config
                    .bar_spread_fallback
                    .get(feed_event.event.symbol())
                    .copied();
                // A bar executes on its open, range, and close; a bar of a longer duration than the symbol's execution bars only feeds strategy series, and a bar whose prices cannot be quoted is an invalid quote like any other.
                let prices = match feed_event.event.bar_execution_prices(fallback) {
                    None => None,
                    Some(prices) => match prices.executable() {
                        Some(prices) => Some(prices),
                        None => {
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
                    },
                };
                let bar = prices.filter(|bar| {
                    match (
                        bar_execution_timeframes.get(&bar.symbol),
                        bar.timeframe_seconds,
                    ) {
                        (Some(execution), Some(seconds)) => *execution == seconds,
                        _ => true,
                    }
                });
                let series_only =
                    bar.is_none() && matches!(feed_event.event, MarketEvent::Bar { .. });
                let quote = match &bar {
                    Some(bar) => bar.open_quote(),
                    None => feed_event.event.to_quote_with_spread_fallback(fallback),
                };
                if bar.is_some() && quote.bid == quote.ask {
                    zero_spread_bar_quotes += 1;
                }
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
                accepted.push((feed_event, quote, bar, series_only));
            }
            let accepted_primary_events = accepted
                .iter()
                .filter(|(event, ..)| event.metadata.roles.primary)
                .map(|(event, ..)| event.clone())
                .collect::<Vec<_>>();
            if !hook.preflight_primary_events(&accepted_primary_events) {
                return Err(FutureBatchReplayError::Dynamic);
            }

            for (feed_event, quote, ..) in &accepted {
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
            // Overnight financing is charged for every rollover instant already crossed, before any fill or signal at this timestamp can change what is open.
            unconverted_cost_events += future_executor.charge_rollovers(
                batch_ts,
                &mut portfolio,
                Some(&conversion_quotes),
            );
            if let Some(state) = hook.portfolio_state() {
                state.begin_batch(batch_ts, future_executor.balance());
            }

            let valuation_only = accepted
                .iter()
                .any(|event| event.0.metadata.roles.conversion)
                && !accepted.iter().any(|event| event.0.metadata.roles.primary)
                && primary_eod.is_some_and(|eod| batch_ts <= eod);

            let mut primary_quotes = Vec::new();
            let mut primary_events = Vec::new();
            let mut batch_quotes = BTreeMap::new();
            let mut executed_bars = Vec::new();
            for (feed_event, quote, bar, series_only) in accepted {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                if !feed_event.metadata.roles.primary || series_only {
                    if feed_event.metadata.roles.primary {
                        last_processed_primary_ts = Some(quote.ts);
                        primary_events.push(feed_event);
                    }
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
                if let Some(bar) = bar {
                    executed_bars.push(bar);
                }
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

            // A strategy that reads a stored bar only after its bucket closes decides on a bar batch before the new bars trade, so its orders can fill at their open. A tick batch, and a strategy that reads the batch's own bars, keep deciding after the quotes settle.
            let bar_batch = hook.reads_completed_bars_only()
                && primary_events
                    .iter()
                    .any(|event| matches!(event.event, MarketEvent::Bar { .. }));
            let mut boundary_events = Some(primary_events);
            if bar_batch {
                self.run_future_boundary(
                    hook,
                    batch_ts,
                    boundary_events
                        .take()
                        .expect("boundary events are taken once"),
                    &boundary_excursions,
                    &primary_quotes,
                    &batch_quotes,
                    profile,
                    &future,
                    true,
                    last_mtm_candidate
                        .as_ref()
                        .and_then(|point: &EquityPoint| point.drawdown_pct),
                    &mut scheduled,
                    &mut queued,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                )?;
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

            // Each bar trades through its range after its open, and its close marks what remains.
            for bar in &executed_bars {
                if is_cancelled() {
                    return Err(FutureBatchReplayError::Cancelled);
                }
                invalid_quotes += self.settle_future_bar_range(
                    bar,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                );
            }
            for bar in &executed_bars {
                portfolio.record_quote(bar.close_quote());
            }

            if let Some(events) = boundary_events.take() {
                self.run_future_boundary(
                    hook,
                    batch_ts,
                    events,
                    &boundary_excursions,
                    &primary_quotes,
                    &batch_quotes,
                    profile,
                    &future,
                    false,
                    last_mtm_candidate
                        .as_ref()
                        .and_then(|point: &EquityPoint| point.drawdown_pct),
                    &mut scheduled,
                    &mut queued,
                    &mut lifecycle,
                    &mut future_executor,
                    &mut portfolio,
                    &pricer,
                    &conversion_quotes,
                )?;
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
            if let Some(signal) = action.entry_signal.as_ref() {
                self.record_entry_resolution_rejection(
                    action.action_id.clone(),
                    signal,
                    Some(&SelectedEntryProfile {
                        profile: action.entry_profile.clone(),
                        source: action
                            .entry_profile_selection_source
                            .unwrap_or(EntryProfileSelectionSource::Unprofiled),
                        entry_class: match signal {
                            RawSignal::Entry { entry_class, .. } => entry_class.clone(),
                            _ => None,
                        },
                        profile_name: action.selected_profile_name.clone(),
                    }),
                    EntryResolutionStage::MarketExecution,
                    None,
                    None,
                    "quote_eligibility",
                    "no_eligible_quote".into(),
                );
            }
            let mut disposition =
                ActionDisposition::rejected(action.action_id, "no_eligible_quote");
            disposition.action_kind = Some(action.action_kind);
            disposition.signal_ts = Some(action.signal_ts);
            disposition.effective_ts = Some(action.effective_ts);
            self.record_disposition(&mut lifecycle, disposition);
        }
        for signal in scheduled {
            if is_cancelled() {
                return Err(FutureBatchReplayError::Cancelled);
            }
            let action_id = signal.resolved_action_id();
            if signal.signal.is_entry() {
                let selected = self
                    .select_entry_profile(&signal.signal, profile, signal.instance)
                    .ok();
                let stage = match &signal.signal {
                    RawSignal::Entry {
                        order_type: OrderType::Market,
                        ..
                    } => EntryResolutionStage::MarketExecution,
                    _ => EntryResolutionStage::PendingPlacement,
                };
                self.record_entry_resolution_rejection(
                    action_id.clone(),
                    &signal.signal,
                    selected.as_ref(),
                    stage,
                    None,
                    None,
                    "quote_eligibility",
                    "no_eligible_quote".into(),
                );
            }
            let mut disposition = ActionDisposition::rejected(action_id, "no_eligible_quote");
            disposition.action_kind = Some(raw_signal_kind(&signal.signal).to_owned());
            disposition.signal_ts = Some(signal.signal_ts);
            disposition.effective_ts = Some(signal.effective_ts);
            self.record_disposition(&mut lifecycle, disposition);
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
                    let committed_effects = engine_transaction.effects().to_vec();
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
                    self.record_committed_effects(committed_effects, Some(action_id.clone()));
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
                self.record_disposition(&mut lifecycle, disposition);
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

        if !hook.on_final_committed(
            &mut self.committed_feedback,
            &mut self.committed_feedback_events,
        ) {
            return Err(FutureBatchReplayError::Dynamic);
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
        let entry_profile_default = self
            .entry_profiles
            .as_ref()
            .and_then(|profiles| profiles.default_profile().cloned());
        let entry_profile_routes = self
            .entry_profiles
            .as_ref()
            .map(|profiles| profiles.routes().clone())
            .unwrap_or_default();
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
                market_entry_sizing_basis: future.market_entry_sizing_basis,
                market_entry_sizing: std::mem::take(&mut self.market_entry_sizing),
                entry_profile_default,
                entry_profile_routes,
                entry_profile_resolutions: std::mem::take(&mut self.entry_profile_resolutions),
                costs: self.config.costs.clone().into_iter().collect(),
                unconverted_cost_events,
                zero_spread_bar_quotes,
                stale_quote_after_millis: future.stale_quote_after_ms,
                pnl_epsilon: future.pnl_epsilon,
                tags,
                run_tags: self.config.run_tags.clone(),
                position_tags: hook
                    .portfolio_state()
                    .map(|state| state.position_tags(&future_executor.fills))
                    .unwrap_or_default(),
                ..ExecutionMetadata::default()
            },
            fills: future_executor.fills.clone(),
            close_events: future_executor.close_events.clone(),
            cost_events: future_executor.cost_events.clone(),
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

    /// Run the hook's boundary for one batch and schedule what it generates.
    ///
    /// `decided_before_quotes` marks a boundary that ran before the batch's quotes settled, which is how stored bars are replayed: the strategy has seen only completed bars, so its orders may fill at the first quote of this batch instead of waiting for a later one.
    #[allow(clippy::too_many_arguments)]
    fn run_future_boundary<H, E>(
        &mut self,
        hook: &mut H,
        batch_ts: NaiveDateTime,
        primary_events: Vec<FeedEvent>,
        boundary_excursions: &BTreeMap<String, crate::portfolio::CampaignExcursion>,
        primary_quotes: &[PriceQuote],
        batch_quotes: &BTreeMap<String, PriceQuote>,
        profile: Option<&ManagementProfile>,
        future: &FutureQuoteConfig,
        decided_before_quotes: bool,
        drawdown_fraction: Option<f64>,
        scheduled: &mut VecDeque<ScheduledSignal>,
        queued: &mut VecDeque<QueuedAction>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> std::result::Result<(), FutureBatchReplayError<E>>
    where
        H: FutureReplayHook,
    {
        let strategy_batch = TimestampBatch {
            ts: batch_ts,
            events: primary_events,
        };
        let generated = {
            let positions = BoundaryPositionFacts::new(boundary_excursions, future_executor);
            hook.on_boundary(
                &strategy_batch,
                &self.engine,
                lifecycle,
                &positions,
                &mut self.committed_feedback,
                &mut self.committed_feedback_events,
            )
            .ok_or(FutureBatchReplayError::Dynamic)?
        };
        if !generated.is_empty() {
            let instances = generated
                .iter()
                .map(|scheduled| scheduled.instance)
                .collect::<BTreeSet<_>>();
            for instance in instances {
                let generated_signals = generated
                    .iter()
                    .filter(|scheduled| scheduled.instance == instance)
                    .map(|scheduled| scheduled.signal.clone())
                    .collect::<Vec<_>>();
                if let Err(error) =
                    validate_replay_config(&self.config, Some(future), &generated_signals)
                {
                    hook.reject_generated_configuration(instance, error);
                    return Err(FutureBatchReplayError::Dynamic);
                }
            }
        }
        let generated = self.supervise_generated(
            hook,
            batch_ts,
            generated,
            decided_before_quotes,
            drawdown_fraction,
            scheduled,
            queued,
            lifecycle,
            future_executor,
        );
        for mut generated_signal in generated {
            if decided_before_quotes {
                generated_signal.requires_later_quote = false;
            }
            if generated_signal.effective_ts <= batch_ts
                && let Some(representative_quote) = primary_quotes.first()
            {
                self.schedule_future_signal(
                    generated_signal,
                    profile,
                    representative_quote,
                    batch_quotes,
                    queued,
                    lifecycle,
                    future_executor,
                    portfolio,
                    pricer,
                    conversion_quotes,
                );
            } else {
                scheduled.push_back(generated_signal);
            }
        }
        Ok(())
    }

    /// Settle one bar's range after its open quote settled.
    ///
    /// Long exposure walks open, low, high and short exposure walks open, high, low, so each side meets its adverse extreme first. Along each leg the walk stops at every price where a stop, target, breakeven trigger, or pending order of that side would act, with a quote whose evaluated price equals that level, so the existing FutureQuote rules fill at the level itself. A stop that a later move of the same walk raises or lowers cannot fill in the same bar, because the walk never returns.
    #[allow(clippy::too_many_arguments)]
    fn settle_future_bar_range(
        &mut self,
        bar: &BarExecutionPrices,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> u64 {
        let mut failures = 0;
        for side in [Side::Buy, Side::Sell] {
            let legs = match side {
                Side::Buy => [(bar.open, bar.low), (bar.low, bar.high)],
                Side::Sell => [(bar.open, bar.high), (bar.high, bar.low)],
            };
            for (from, to) in legs {
                failures += self.walk_future_bar_leg(
                    bar,
                    side,
                    from,
                    to,
                    lifecycle,
                    future_executor,
                    portfolio,
                    pricer,
                    conversion_quotes,
                );
            }
        }
        failures
    }

    #[allow(clippy::too_many_arguments)]
    fn walk_future_bar_leg(
        &mut self,
        bar: &BarExecutionPrices,
        side: Side,
        from: f64,
        to: f64,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> u64 {
        if !(from.is_finite() && to.is_finite()) || from == to {
            return 0;
        }
        let rising = to > from;
        let ahead = |mid: f64, cursor: f64| {
            if rising {
                mid > cursor && mid <= to
            } else {
                mid < cursor && mid >= to
            }
        };
        let mut failures = 0;
        let mut cursor = from;
        // Every step moves strictly along the leg, and each stop only adds levels behind it or finitely many ahead of it, so the walk ends; the bound only guards against a malformed rule set.
        for _ in 0..MAX_BAR_LEG_STEPS {
            let next = self
                .bar_trigger_levels(&bar.symbol, side, bar.half_spread)
                .into_iter()
                .filter(|(mid, _)| ahead(*mid, cursor))
                .min_by(|left, right| {
                    let order = left.0.total_cmp(&right.0);
                    if rising { order } else { order.reverse() }
                });
            let Some((mid, quote_prices)) = next else {
                break;
            };
            cursor = mid;
            let quote = PriceQuote {
                symbol: bar.symbol.clone(),
                ts: bar.ts,
                bid: quote_prices.0,
                ask: quote_prices.1,
            };
            if ExecutionPricer::validate_quote(&quote).is_err() {
                continue;
            }
            if self
                .settle_future_quote(
                    &quote,
                    Some(side),
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
        }
        let extreme = bar.quote_at_mid(to);
        if ExecutionPricer::validate_quote(&extreme).is_ok()
            && self
                .settle_future_quote(
                    &extreme,
                    Some(side),
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
        failures
    }

    /// Each level at which a position of `side` on `symbol` could act, as the midpoint where the walk reaches it and the bid and ask of a quote whose compared price equals the level exactly.
    fn bar_trigger_levels(
        &self,
        symbol: &str,
        side: Side,
        half_spread: f64,
    ) -> Vec<(f64, (f64, f64))> {
        let model = self.config.fill_model;
        let spread = half_spread * 2.0;
        let mut levels = Vec::new();
        let ids = self
            .engine
            .manager
            .pending_ids_by_symbol_sorted(symbol)
            .into_iter()
            .chain(self.engine.manager.open_ids_by_symbol_sorted(symbol));
        for id in ids {
            let Some(position) = self.engine.get_position(&id) else {
                continue;
            };
            if position.data.side != side {
                continue;
            }
            // A pending order compares its fill price and an open position its evaluation price.
            let compared = match (position.data.status, model, side) {
                (_, FillModel::MidPrice, _) => QuoteSide::Mid,
                (_, FillModel::AskOnly, _) => QuoteSide::Ask,
                (PositionStatus::Pending, FillModel::BidAsk, Side::Buy)
                | (PositionStatus::Open, FillModel::BidAsk, Side::Sell) => QuoteSide::Ask,
                _ => QuoteSide::Bid,
            };
            for level in position.future_trigger_levels() {
                levels.push(match compared {
                    QuoteSide::Bid => (level + half_spread, (level, level + spread)),
                    QuoteSide::Ask => (level - half_spread, (level - spread, level)),
                    // Keep the bar's spread when its midpoint lands exactly on the level; otherwise a zero-spread quote at the level, which the midpoint model prices identically.
                    QuoteSide::Mid => {
                        let (bid, ask) = (level - half_spread, level + half_spread);
                        if (bid + ask) / 2.0 == level {
                            (level, (bid, ask))
                        } else {
                            (level, (level, level))
                        }
                    }
                });
            }
        }
        levels
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
                    None,
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
        side: Option<Side>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &mut FutureExecutor,
        portfolio: &mut PortfolioRecorder,
        pricer: &ExecutionPricer,
        conversion_quotes: &ConversionQuoteBook,
    ) -> Result<(), FutureTransactionError> {
        let (prepared, failures) = self.prepare_triggering_pending(quote, side, pricer);
        for (position_id, error) in failures {
            let action_id = format!("pending_execution:{position_id}");
            let mut disposition = ActionDisposition::rejected(action_id.clone(), error);
            disposition.action_kind = Some("pending_execution".into());
            disposition.effective_ts = Some(quote.ts);
            disposition.position_ids.push(position_id.clone());
            self.record_disposition(lifecycle, disposition);
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
                self.record_committed_effects(committed_effects, Some(action_id.clone()));
            }
        }

        let pending_action_ids = prepared
            .iter()
            .filter_map(|pending| {
                future_executor
                    .pending_metadata(&pending.position_id)
                    .map(|metadata| (pending.position_id.clone(), metadata.0))
            })
            .collect::<BTreeMap<_, _>>();
        let pip_size = self.pip_size(&quote.symbol);
        let engine_transaction = match side {
            Some(side) => self.engine.begin_on_price_future_effects_priced_for_side(
                quote, &prepared, pricer, pip_size, side,
            )?,
            None => self
                .engine
                .begin_on_price_future_effects_priced(quote, &prepared, pricer, pip_size)?,
        };
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
        for effect in committed_effects {
            let action_id = pending_fill_position_id(&effect)
                .and_then(|position_id| pending_action_ids.get(position_id))
                .cloned();
            self.record_committed_effects(vec![effect], action_id);
        }
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
        let explicit_action_id = scheduled.explicit_action_id;
        let base_id = scheduled.resolved_action_id();
        let selected_profile = if scheduled.signal.is_entry() {
            match self.select_entry_profile(&scheduled.signal, profile, scheduled.instance) {
                Ok(profile) => profile,
                Err(error) => {
                    let reason = error.to_string();
                    let stage = match scheduled.signal {
                        RawSignal::Entry {
                            order_type: OrderType::Market,
                            ..
                        } => EntryResolutionStage::MarketExecution,
                        _ => EntryResolutionStage::PendingPlacement,
                    };
                    self.record_entry_resolution_rejection(
                        base_id.clone(),
                        &scheduled.signal,
                        None,
                        stage,
                        None,
                        None,
                        "profile_selection",
                        reason.clone(),
                    );
                    let mut disposition = ActionDisposition::rejected(base_id, reason);
                    disposition.action_kind = Some("entry".into());
                    disposition.signal_ts = Some(scheduled.signal_ts);
                    disposition.effective_ts = Some(scheduled.effective_ts);
                    self.record_disposition(lifecycle, disposition);
                    return;
                }
            }
        } else {
            SelectedEntryProfile {
                profile: None,
                source: EntryProfileSelectionSource::Unprofiled,
                entry_class: None,
                profile_name: None,
            }
        };
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
                entry_profile: selected_profile.profile,
                entry_profile_selection_source: Some(selected_profile.source),
                selected_profile_name: selected_profile.profile_name,
                market_entry_sizing_audit: None,
                entry_profile_resolution_audit: None,
                requires_later_quote: scheduled.requires_later_quote,
            });
            return;
        }
        if scheduled.signal.is_entry() {
            let resolved = match selected_profile.profile.as_ref() {
                Some(profile) => self.resolve_profiled_entry(profile, &scheduled.signal),
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
                        &selected_profile,
                    )
                }
                Ok(None) => {
                    let mut disposition = ActionDisposition::skipped(base_id, "not_an_entry");
                    disposition.action_kind = Some("entry".into());
                    disposition.signal_ts = Some(scheduled.signal_ts);
                    disposition.effective_ts = Some(scheduled.effective_ts);
                    self.record_disposition(lifecycle, disposition);
                }
                Err(error) => {
                    let reason = error.to_string();
                    self.record_entry_resolution_rejection(
                        base_id.clone(),
                        &scheduled.signal,
                        Some(&selected_profile),
                        EntryResolutionStage::PendingPlacement,
                        match &scheduled.signal {
                            RawSignal::Entry { price, .. } => *price,
                            _ => None,
                        },
                        None,
                        "profile_resolution",
                        reason.clone(),
                    );
                    let mut disposition = ActionDisposition::rejected(base_id, reason);
                    disposition.action_kind = Some("entry".into());
                    disposition.signal_ts = Some(scheduled.signal_ts);
                    disposition.effective_ts = Some(scheduled.effective_ts);
                    self.record_disposition(lifecycle, disposition);
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
            self.record_disposition(lifecycle, disposition);
            return;
        }
        let action_count = actions.len();
        if explicit_action_id && action_count != 1 {
            let mut disposition = ActionDisposition::rejected(
                base_id,
                "configured_command_resolved_multiple_actions",
            );
            disposition.action_kind = Some(raw_signal_kind(&scheduled.signal).to_owned());
            disposition.signal_ts = Some(scheduled.signal_ts);
            disposition.effective_ts = Some(scheduled.effective_ts);
            self.record_disposition(lifecycle, disposition);
            return;
        }
        for (index, action) in actions.into_iter().enumerate() {
            let action_id = if explicit_action_id && action_count == 1 {
                base_id.clone()
            } else {
                format!("{base_id}:action:{index:03}")
            };
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
                    entry_profile_selection_source: None,
                    selected_profile_name: None,
                    market_entry_sizing_audit: None,
                    entry_profile_resolution_audit: None,
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
        selected_profile: &SelectedEntryProfile,
    ) {
        let level_reference_price = resolved.price;
        let level_resolution = resolved.level_resolution.clone();
        match self.finalize_resolved_entry(
            resolved,
            future_executor.balance(),
            scheduled.effective_ts,
            Some(conversion_quotes),
            None,
        ) {
            Ok(finalized) => {
                let original_signal_price = match &scheduled.signal {
                    RawSignal::Entry { price, .. } => *price,
                    _ => None,
                };
                let (trade_id, level_reference_price) = match &finalized.action {
                    Action::Open {
                        trade_id, price, ..
                    } => (
                        trade_id.clone(),
                        price.unwrap_or(quote.open_price(match &finalized.action {
                            Action::Open { side, .. } => *side,
                            _ => unreachable!(),
                        })),
                    ),
                    _ => unreachable!("finalized entry must be an open action"),
                };
                let mut audit = EntryProfileResolutionAudit {
                    action_id: action_id.clone(),
                    trade_id,
                    entry_class: selected_profile.entry_class.clone(),
                    selection_source: selected_profile.source,
                    selected_profile_name: selected_profile.profile_name.clone(),
                    resolution_stage: EntryResolutionStage::PendingPlacement,
                    original_signal_price,
                    level_reference_price: Some(level_reference_price),
                    level_resolution: Some(finalized.level_resolution.clone()),
                    target_resolution: Some(finalized.target_resolution.clone()),
                    configured_weights: finalized.configured_weights.clone(),
                    allocated_target_steps: finalized.allocated_target_steps.clone(),
                    remainder_steps: finalized.remainder_steps,
                    outcome: crate::ledger::ActionDispositionStatus::Applied,
                    rejection_stage: None,
                    reason: None,
                };
                let committed = self.apply_future_action(
                    action_id,
                    "entry".into(),
                    finalized.action,
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
                if committed {
                    self.entry_profile_resolutions.push(audit);
                } else {
                    if let Some(disposition) = lifecycle
                        .as_slice()
                        .iter()
                        .rev()
                        .find(|disposition| disposition.action_id == audit.action_id)
                    {
                        audit.outcome = disposition.status;
                        audit.rejection_stage = Some("engine_or_accounting".into());
                        audit.reason = disposition.reason.clone();
                    }
                    self.entry_profile_resolutions.push(audit);
                }
            }
            Err(error) => {
                self.record_entry_resolution_rejection(
                    action_id.clone(),
                    &scheduled.signal,
                    Some(selected_profile),
                    EntryResolutionStage::PendingPlacement,
                    level_reference_price,
                    Some(level_resolution),
                    "sizing",
                    error.clone(),
                );
                let mut disposition = ActionDisposition::rejected(action_id, error);
                disposition.action_kind = Some("entry".into());
                disposition.signal_ts = Some(scheduled.signal_ts);
                disposition.effective_ts = Some(scheduled.effective_ts);
                self.record_disposition(lifecycle, disposition);
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
                let (side, symbol, original_signal_price, entry_class) = match &signal {
                    RawSignal::Entry {
                        side,
                        symbol,
                        price,
                        entry_class,
                        ..
                    } => (*side, symbol.clone(), *price, entry_class.clone()),
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
                        self.record_disposition(lifecycle, disposition);
                        continue;
                    }
                };
                if let RawSignal::Entry { price, .. } = &mut signal {
                    *price = Some(execution.price);
                }
                action.execution = Some(execution);
                let configured_basis = self
                    .future_config
                    .as_ref()
                    .map(|config| config.market_entry_sizing_basis)
                    .unwrap_or_default();
                let (applied_basis, fallback_to_fill, sizing_reference_price) =
                    match (configured_basis, original_signal_price) {
                        (MarketEntrySizingBasis::SignalEntryPrice, Some(price)) => {
                            (MarketEntrySizingBasis::SignalEntryPrice, false, price)
                        }
                        (MarketEntrySizingBasis::SignalEntryPrice, None) => {
                            (MarketEntrySizingBasis::FillPrice, true, execution.price)
                        }
                        (MarketEntrySizingBasis::FillPrice, _) => {
                            (MarketEntrySizingBasis::FillPrice, false, execution.price)
                        }
                    };
                let resolved = match action.entry_profile.as_ref() {
                    Some(profile) => self.resolve_profiled_entry(profile, &signal),
                    None => resolve_unprofiled_entry(&signal),
                };
                match resolved {
                    Ok(Some(resolved)) => {
                        let side_for_audit = resolved.side;
                        let level_resolution = resolved.level_resolution.clone();
                        let resolved_target_prices: Vec<f64> =
                            resolved.targets.iter().map(|target| target.price).collect();
                        match self.finalize_resolved_entry(
                            resolved,
                            future_executor.balance(),
                            quote.ts,
                            Some(conversion_quotes),
                            Some(sizing_reference_price),
                        ) {
                            Ok(finalized) => {
                                let (trade_id, protective_stop) = match &finalized.action {
                                    Action::Open {
                                        trade_id, stoploss, ..
                                    } => (trade_id.clone(), *stoploss),
                                    _ => unreachable!("finalized entry must be an open action"),
                                };
                                // Record resolved levels already crossed at the fill;
                                // later engine validation remains authoritative.
                                let mut levels_crossed_at_fill = Vec::new();
                                if let Some(stop) = protective_stop {
                                    let crossed = match side_for_audit {
                                        Side::Buy => execution.price <= stop,
                                        Side::Sell => execution.price >= stop,
                                    };
                                    if crossed {
                                        levels_crossed_at_fill.push("stop".to_owned());
                                    }
                                }
                                for (offset, target_price) in
                                    resolved_target_prices.iter().enumerate()
                                {
                                    let crossed = match side_for_audit {
                                        Side::Buy => execution.price >= *target_price,
                                        Side::Sell => execution.price <= *target_price,
                                    };
                                    if crossed {
                                        levels_crossed_at_fill
                                            .push(format!("target{}", offset + 1));
                                    }
                                }
                                action.entry_profile_resolution_audit =
                                    Some(EntryProfileResolutionAudit {
                                        action_id: action.action_id.clone(),
                                        trade_id: trade_id.clone(),
                                        entry_class,
                                        selection_source: action
                                            .entry_profile_selection_source
                                            .unwrap_or(EntryProfileSelectionSource::Unprofiled),
                                        selected_profile_name: action.selected_profile_name.clone(),
                                        resolution_stage: EntryResolutionStage::MarketExecution,
                                        original_signal_price,
                                        level_reference_price: Some(execution.price),
                                        level_resolution: Some(finalized.level_resolution.clone()),
                                        target_resolution: Some(
                                            finalized.target_resolution.clone(),
                                        ),
                                        configured_weights: finalized.configured_weights.clone(),
                                        allocated_target_steps: finalized
                                            .allocated_target_steps
                                            .clone(),
                                        remainder_steps: finalized.remainder_steps,
                                        outcome: crate::ledger::ActionDispositionStatus::Applied,
                                        rejection_stage: None,
                                        reason: None,
                                    });
                                action.market_entry_sizing_audit = Some(MarketEntrySizingAudit {
                                    action_id: action.action_id.clone(),
                                    trade_id,
                                    configured_basis,
                                    applied_basis,
                                    fallback_to_fill,
                                    original_signal_price,
                                    sizing_reference_price,
                                    execution_price: execution.price,
                                    protective_stop,
                                    requested_account_risk: finalized.requested_account_risk,
                                    native_loss_per_lot: finalized.native_loss_per_lot,
                                    account_loss_per_lot: finalized.account_loss_per_lot,
                                    final_lot: finalized.final_lot,
                                    levels_crossed_at_fill,
                                });
                                action.action = finalized.action;
                            }
                            Err(error) => {
                                self.record_entry_resolution_rejection(
                                    action.action_id.clone(),
                                    &signal,
                                    Some(&SelectedEntryProfile {
                                        profile: action.entry_profile.clone(),
                                        source: action
                                            .entry_profile_selection_source
                                            .unwrap_or(EntryProfileSelectionSource::Unprofiled),
                                        entry_class: entry_class.clone(),
                                        profile_name: action.selected_profile_name.clone(),
                                    }),
                                    EntryResolutionStage::MarketExecution,
                                    Some(execution.price),
                                    Some(level_resolution),
                                    "sizing",
                                    error.clone(),
                                );
                                let mut disposition =
                                    ActionDisposition::rejected(action.action_id, error);
                                disposition.action_kind = Some(action.action_kind);
                                disposition.signal_ts = Some(action.signal_ts);
                                disposition.effective_ts = Some(action.effective_ts);
                                self.record_disposition(lifecycle, disposition);
                                continue;
                            }
                        }
                    }
                    Ok(None) => {
                        let mut disposition =
                            ActionDisposition::skipped(action.action_id, "not_an_entry");
                        disposition.action_kind = Some(action.action_kind);
                        disposition.signal_ts = Some(action.signal_ts);
                        disposition.effective_ts = Some(action.effective_ts);
                        self.record_disposition(lifecycle, disposition);
                        continue;
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        self.record_entry_resolution_rejection(
                            action.action_id.clone(),
                            &signal,
                            Some(&SelectedEntryProfile {
                                profile: action.entry_profile.clone(),
                                source: action
                                    .entry_profile_selection_source
                                    .unwrap_or(EntryProfileSelectionSource::Unprofiled),
                                entry_class: entry_class.clone(),
                                profile_name: action.selected_profile_name.clone(),
                            }),
                            EntryResolutionStage::MarketExecution,
                            Some(execution.price),
                            None,
                            "profile_resolution",
                            reason.clone(),
                        );
                        let mut disposition = ActionDisposition::rejected(action.action_id, reason);
                        disposition.action_kind = Some(action.action_kind);
                        disposition.signal_ts = Some(action.signal_ts);
                        disposition.effective_ts = Some(action.effective_ts);
                        self.record_disposition(lifecycle, disposition);
                        continue;
                    }
                }
            }
            let committed = self.apply_future_action(
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
            if committed {
                if let Some(audit) = action.market_entry_sizing_audit {
                    self.market_entry_sizing.push(audit);
                }
                if let Some(audit) = action.entry_profile_resolution_audit {
                    self.entry_profile_resolutions.push(audit);
                }
            } else if let Some(mut audit) = action.entry_profile_resolution_audit {
                if let Some(disposition) = lifecycle
                    .as_slice()
                    .iter()
                    .rev()
                    .find(|disposition| disposition.action_id == audit.action_id)
                {
                    audit.outcome = disposition.status;
                    audit.rejection_stage = Some("engine_or_accounting".into());
                    audit.reason = disposition.reason.clone();
                }
                self.entry_profile_resolutions.push(audit);
            }
        }
        *queued = remaining;
    }

    #[allow(clippy::too_many_arguments)]
    fn record_entry_resolution_rejection(
        &mut self,
        action_id: String,
        signal: &RawSignal,
        selected: Option<&SelectedEntryProfile>,
        stage: EntryResolutionStage,
        level_reference_price: Option<f64>,
        level_resolution: Option<qs_core::EntryLevelResolution>,
        rejection_stage: &str,
        reason: String,
    ) {
        let (trade_id, original_signal_price, entry_class) = match signal {
            RawSignal::Entry {
                trade_id,
                price,
                entry_class,
                ..
            } => (trade_id.clone(), *price, entry_class.clone()),
            _ => (None, None, None),
        };
        let selection_source = selected.map_or_else(
            || {
                if entry_class.is_some() {
                    EntryProfileSelectionSource::Mapped
                } else {
                    EntryProfileSelectionSource::Unprofiled
                }
            },
            |selection| selection.source,
        );
        self.entry_profile_resolutions
            .push(EntryProfileResolutionAudit {
                action_id,
                trade_id,
                entry_class,
                selection_source,
                selected_profile_name: selected
                    .and_then(|selection| selection.profile_name.clone()),
                resolution_stage: stage,
                original_signal_price,
                level_reference_price,
                level_resolution,
                target_resolution: None,
                configured_weights: Vec::new(),
                allocated_target_steps: Vec::new(),
                remainder_steps: 0,
                outcome: ActionDispositionStatus::Rejected,
                rejection_stage: Some(rejection_stage.into()),
                reason: Some(reason),
            });
    }

    fn record_disposition(
        &mut self,
        lifecycle: &mut LifecycleLedger,
        disposition: ActionDisposition,
    ) {
        if lifecycle.record(disposition.clone()).is_ok() {
            self.committed_feedback_events
                .push(StrategyFeedbackEvent::Disposition(disposition));
        }
    }

    fn record_committed_effects(&mut self, effects: Vec<FutureEffect>, action_id: Option<String>) {
        for effect in effects {
            self.committed_feedback_events
                .push(StrategyFeedbackEvent::Effect {
                    action_id: action_id.clone(),
                    effect: effect.clone(),
                });
            self.committed_feedback.push(effect);
        }
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
    ) -> bool {
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
            self.record_disposition(lifecycle, disposition);
            return false;
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
            self.record_disposition(lifecycle, disposition);
            return false;
        }

        let execution = match self.prepare_future_action(&mut action, execution, quote, pricer) {
            Ok(execution) => execution,
            Err(reason) => {
                let mut disposition = ActionDisposition::rejected(action_id, reason);
                disposition.action_kind = Some(action_kind);
                disposition.signal_ts = Some(signal_ts);
                disposition.effective_ts = Some(effective_ts);
                self.record_disposition(lifecycle, disposition);
                return false;
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
                // A closed-position state mismatch on a management action mirrors
                // a live broker no-op, so it is skipped rather than failed.
                let closed_state = matches!(
                    error,
                    FutureApplyError::Core(qs_core::CoreError::InvalidState { .. })
                ) && action_kind != "entry";
                let mut disposition = if closed_state {
                    ActionDisposition::skipped(action_id, "position_closed")
                } else {
                    ActionDisposition::rejected(action_id, error.to_string())
                };
                disposition.action_kind = Some(action_kind);
                disposition.signal_ts = Some(signal_ts);
                disposition.effective_ts = Some(effective_ts);
                self.record_disposition(lifecycle, disposition);
                return false;
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
                    self.record_disposition(lifecycle, disposition);
                    return false;
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
        self.record_committed_effects(committed_effects, Some(action_id.clone()));

        let mut disposition = ActionDisposition::applied(action_id);
        disposition.action_kind = Some(action_kind);
        disposition.signal_ts = Some(signal_ts);
        disposition.effective_ts = Some(effective_ts);
        disposition.position_ids = affected;
        self.record_disposition(lifecycle, disposition);
        true
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
        side: Option<Side>,
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
            if side.is_some_and(|side| position.data.side != side) {
                continue;
            }
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

fn pending_fill_position_id(effect: &FutureEffect) -> Option<&str> {
    match effect.effect() {
        Effect::PositionOpened { id } => Some(id),
        _ => None,
    }
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

/// Reject cost specifications that cannot be applied deterministically to this run.
fn validate_replay_costs(
    config: &BacktestConfig,
    future: Option<&FutureQuoteConfig>,
) -> Result<(), String> {
    let account_currency = future
        .and_then(|future| future.currency_plan.as_ref())
        .map(|plan| plan.account_currency().to_owned());
    for (symbol, costs) in &config.costs {
        if symbol.is_empty() {
            return Err("cost symbol must not be empty".into());
        }
        costs
            .validate()
            .map_err(|error| format!("costs for {symbol} are invalid: {error}"))?;
        if let Some(account_currency) = account_currency.as_deref() {
            costs
                .validate_against_account_currency(account_currency)
                .map_err(|error| format!("costs for {symbol} are invalid: {error}"))?;
        }
        if costs.requires_point_size() && !config.symbol_specs.contains_key(symbol) {
            return Err(format!(
                "point-denominated swap for {symbol} requires a symbol specification for its digit count"
            ));
        }
    }
    Ok(())
}

/// Price point size per symbol, derived from the digit count used by the symbol registry.
fn effective_point_sizes(config: &BacktestConfig) -> HashMap<String, f64> {
    config
        .symbol_specs
        .iter()
        .map(|(symbol, spec)| (symbol.clone(), 10f64.powi(-i32::from(spec.digits))))
        .collect()
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

/// Largest number of run tags one replay may carry.
pub const MAX_RUN_TAGS: usize = 32;
/// Largest byte length of one run-tag key or value.
pub const MAX_RUN_TAG_BYTES: usize = 64;

fn validate_run_tags(tags: &BTreeMap<String, String>) -> Result<(), String> {
    if tags.len() > MAX_RUN_TAGS {
        return Err(format!(
            "run tags must not exceed {MAX_RUN_TAGS} entries, got {}",
            tags.len()
        ));
    }
    for (key, value) in tags {
        if key.is_empty() || key.len() > MAX_RUN_TAG_BYTES {
            return Err(format!(
                "run tag key must be 1 to {MAX_RUN_TAG_BYTES} bytes, got '{key}'"
            ));
        }
        if !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(format!(
                "run tag key must be ASCII alphanumeric, underscore, or hyphen, got '{key}'"
            ));
        }
        if value.len() > MAX_RUN_TAG_BYTES {
            return Err(format!(
                "run tag value for '{key}' must not exceed {MAX_RUN_TAG_BYTES} bytes"
            ));
        }
        if value.chars().any(char::is_control) {
            return Err(format!(
                "run tag value for '{key}' must not contain control characters"
            ));
        }
    }
    Ok(())
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

    validate_run_tags(&config.run_tags)?;
    validate_replay_costs(config, future)?;
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
            market_entry_sizing_basis: future.market_entry_sizing_basis,
            market_entry_sizing: Vec::new(),
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
    use crate::profile::{
        EntryGeometryPolicy, ManagementProfile, PositionRef, RawSignal, StoplossMode, TargetSource,
    };
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

    #[test]
    fn scheduled_signal_preserves_an_explicit_opaque_action_id() {
        let scheduled = ScheduledSignal::new(
            7,
            ts(10, 0, 0),
            ts(10, 0, 1),
            RawSignal::CloseAll { ts: ts(10, 0, 0) },
            true,
        )
        .with_action_id("caller-command/opaque:7");

        assert_eq!(scheduled.resolved_action_id(), "caller-command/opaque:7");
    }

    #[test]
    fn scheduled_signal_keeps_the_compatible_generated_action_id() {
        let scheduled = ScheduledSignal::new(
            7,
            ts(10, 0, 0),
            ts(10, 0, 1),
            RawSignal::CloseAll { ts: ts(10, 0, 0) },
            false,
        );

        assert_eq!(scheduled.resolved_action_id(), "signal:00000007");
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
            entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            entry_class: None,
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
            entry_class: None,
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
            entry_class: None,
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
            entry_class: None,
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
                entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
            entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
            entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
                entry_class: None,
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
            entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
                entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
                entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: Some("alpha".into()),
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            entry_class: None,
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
            entry_class: None,
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
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
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
            entry_class: None,
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
            entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
                entry_class: None,
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
            market_entry_sizing_basis: MarketEntrySizingBasis::SignalEntryPrice,
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
                entry_class: None,
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
                entry_class: None,
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
        let metadata = result.execution_metadata.as_ref().unwrap();
        assert_eq!(metadata.market_entry_sizing.len(), 1);
        assert_eq!(
            metadata.market_entry_sizing[0].trade_id.as_deref(),
            Some("market")
        );
        assert_eq!(metadata.entry_profile_resolutions.len(), 2);
        assert!(metadata.entry_profile_resolutions.iter().any(|audit| {
            audit.trade_id.as_deref() == Some("pending")
                && audit.resolution_stage == EntryResolutionStage::PendingPlacement
        }));
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
            entry_class: None,
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
            entry_class: None,
        }];

        let runner = BacktestRunner::new(fixed_lot_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, None);

        // Library still injects it; server filtering is the authoritative gate.
        assert_eq!(result.total_trades, 1);
    }

    #[test]
    fn future_replay_audits_profile_resolution_rejection() {
        let profile = ManagementProfile {
            name: "requires_stop".into(),
            target_selection: Some(crate::profile::TargetSelection::None),
            use_targets: vec![],
            close_ratios: vec![],
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignalDistance { multiplier: 1.5 },
            rules: vec![],
            group_override: None,
            let_remainder_run: true,
            entry_geometry: EntryGeometryPolicy::Strict,
        };
        let profiles = PreparedEntryProfiles::try_new(
            Some(profile),
            Vec::<(String, ManagementProfile)>::new(),
        )
        .unwrap();
        let signals = vec![RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.1000),
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some("missing-stop".into()),
            entry_class: None,
        }];
        let mut feed = VecFeed::new(vec![tick("EURUSD", 1.1000, 1.1000, ts(10, 0, 1))]);
        let result = BacktestRunner::new_future(fixed_lot_config(), FutureQuoteConfig::default())
            .with_entry_profiles(profiles)
            .run_raw_signals_future(&mut feed, signals, None);
        let audit = &result
            .execution_metadata
            .as_ref()
            .unwrap()
            .entry_profile_resolutions[0];
        assert_eq!(audit.outcome, ActionDispositionStatus::Rejected);
        assert_eq!(audit.rejection_stage.as_deref(), Some("profile_resolution"));
        assert!(audit.reason.as_deref().unwrap().contains("signal stoploss"));
    }

    #[test]
    fn future_replay_routes_entry_profiles_and_audits_resolved_levels() {
        let default_profile = ManagementProfile {
            name: "default".into(),
            target_selection: Some(crate::profile::TargetSelection::None),
            use_targets: vec![],
            close_ratios: vec![],
            target_source: TargetSource::FromSignal,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: true,
            entry_geometry: EntryGeometryPolicy::Strict,
        };
        let expanded_profile = ManagementProfile {
            name: "expanded".into(),
            target_selection: None,
            use_targets: vec![],
            close_ratios: vec![1.0],
            target_source: TargetSource::StopDistanceMultiples {
                multiples: vec![1.0],
            },
            stoploss_mode: StoplossMode::FromSignalDistance { multiplier: 1.5 },
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
            entry_geometry: EntryGeometryPolicy::Strict,
        };
        let profiles = PreparedEntryProfiles::try_new(
            Some(default_profile),
            [("expanded".to_owned(), expanded_profile)],
        )
        .unwrap();
        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.1000),
                risk_multiplier: 1.0,
                stoploss: Some(1.0990),
                targets: vec![],
                group: Some("same-group".into()),
                trade_id: Some("default-entry".into()),
                entry_class: None,
            },
            RawSignal::Entry {
                ts: ts(10, 0, 1),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.1000),
                risk_multiplier: 1.0,
                stoploss: Some(1.0990),
                targets: vec![],
                group: Some("same-group".into()),
                trade_id: Some("expanded-entry".into()),
                entry_class: Some("expanded".into()),
            },
        ];
        let mut feed = VecFeed::new(vec![
            tick("EURUSD", 1.1000, 1.1000, ts(10, 0, 1)),
            tick("EURUSD", 1.1002, 1.1002, ts(10, 0, 2)),
            tick("EURUSD", 1.1003, 1.1003, ts(10, 0, 3)),
        ]);
        let result = BacktestRunner::new_future(fixed_lot_config(), FutureQuoteConfig::default())
            .with_entry_profiles(profiles)
            .run_raw_signals_future(&mut feed, signals, None);

        let audits = &result
            .execution_metadata
            .as_ref()
            .unwrap()
            .entry_profile_resolutions;
        assert_eq!(audits.len(), 2);
        assert_eq!(
            audits[0].selection_source,
            EntryProfileSelectionSource::RunDefault
        );
        assert_eq!(audits[0].selected_profile_name.as_deref(), Some("default"));
        assert_eq!(
            audits[1].selection_source,
            EntryProfileSelectionSource::Mapped
        );
        assert_eq!(audits[1].entry_class.as_deref(), Some("expanded"));
        assert_eq!(audits[1].selected_profile_name.as_deref(), Some("expanded"));
        let levels = audits[1].level_resolution.as_ref().unwrap();
        let reference = audits[1].level_reference_price.unwrap();
        let stop = levels.resolved_stoploss.unwrap();
        let target = levels.resolved_targets[0];
        let risk_distance = reference - stop;
        assert!(risk_distance > 0.0);
        assert!((target - reference - risk_distance).abs() < 1.0e-9);
    }
}
