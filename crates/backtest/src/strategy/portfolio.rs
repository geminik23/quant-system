//! Several configured strategy instances replayed against one account, optionally under a portfolio supervisor.
//!
//! Every instance keeps its own series, analysis, decisions, and entry-profile routes, while fills, balance, costs, marks, and drawdown are shared. Each instance sees feedback only for the commands it issued, because configured command identifiers are scoped by strategy and instance.

use std::convert::Infallible;

use chrono::NaiveDateTime;
use qs_risk::{HaltCommand, HaltInterval, IntentKind, RiskConfigError, Verdict};
use serde::Serialize;

use super::{
    AnalysisPipeline, BacktestConfiguredStrategyAdapter, ConfiguredStrategyAdapterError,
    StrategyDecisionOutput, StrategyDescriptor, StrategyReplayError, StrategyReplayInputError,
    StrategyResearchOutput,
};
use crate::profile::PreparedEntryProfiles;
use crate::report::BacktestResult;

/// Largest number of instances one portfolio replay accepts.
pub const MAX_PORTFOLIO_INSTANCES: usize = 64;

/// Position tag the portfolio replay owns: every position records the instance that opened it.
pub const INSTANCE_POSITION_TAG: &str = "instance";

/// One configured strategy instance of a portfolio replay.
pub struct ConfiguredInstance {
    pub adapter: BacktestConfiguredStrategyAdapter,
    pub analysis: AnalysisPipeline,
    /// Profiles this instance's Entries select from, by entry class or as the instance default.
    pub entry_profiles: PreparedEntryProfiles,
    /// First instant whose market events this instance's series read, normally its own derived warmup start. A shared feed may begin earlier for another instance, and without this bound a strategy with a shorter warmup would become ready, and could trade, before the window it was asked to evaluate.
    pub feed_from: Option<NaiveDateTime>,
}

impl ConfiguredInstance {
    pub fn new(adapter: BacktestConfiguredStrategyAdapter, analysis: AnalysisPipeline) -> Self {
        Self {
            adapter,
            analysis,
            entry_profiles: PreparedEntryProfiles::default(),
            feed_from: None,
        }
    }

    pub fn with_entry_profiles(mut self, profiles: PreparedEntryProfiles) -> Self {
        self.entry_profiles = profiles;
        self
    }

    pub fn with_feed_from(mut self, from: NaiveDateTime) -> Self {
        self.feed_from = Some(from);
        self
    }

    pub fn strategy_id(&self) -> &str {
        self.adapter.configured_strategy().strategy_id()
    }

    pub fn instance_id(&self) -> &str {
        self.adapter.configured_strategy().instance_id()
    }
}

/// What one instance decided and recorded during a portfolio replay.
#[derive(Debug, Clone, Serialize)]
pub struct PortfolioInstanceOutput {
    pub strategy_id: String,
    pub instance_id: String,
    pub descriptor: StrategyDescriptor,
    pub decisions: StrategyDecisionOutput,
    pub research: StrategyResearchOutput,
}

/// One review of a request for new exposure.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SupervisorEvent {
    pub ts: NaiveDateTime,
    pub instance_id: String,
    pub action_id: String,
    pub kind: IntentKind,
    pub symbol: String,
    /// Account-currency risk the request asked for, when it was known before the fill.
    pub requested_risk: Option<f64>,
    pub verdict: Verdict,
}

/// A bulk action the supervisor scheduled when a halt began.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SupervisorHaltAction {
    pub ts: NaiveDateTime,
    pub action_id: String,
    pub command: HaltCommand,
}

/// Everything the supervisor decided during a portfolio replay.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SupervisorOutput {
    pub events: Vec<SupervisorEvent>,
    pub halt_actions: Vec<SupervisorHaltAction>,
    /// Halt periods; a period without an end lasted until the run ended.
    pub halts: Vec<HaltInterval>,
    /// Boundaries at which no marked equity existed yet, during which a drawdown policy could not act.
    pub unmarked_boundaries: u64,
}

impl SupervisorOutput {
    /// Number of Entries the supervisor rejected, leaving out rejected scale-ins.
    pub fn rejected_entries(&self) -> usize {
        self.events
            .iter()
            .filter(|event| event.kind == IntentKind::Entry && !event.verdict.is_approved())
            .count()
    }

    /// Total halted time in whole minutes, where a period still in force at the end of the run runs to `run_end`.
    pub fn halt_minutes(&self, run_end: NaiveDateTime) -> i64 {
        self.halts
            .iter()
            .map(|halt| {
                let end = halt.to.unwrap_or(run_end).max(halt.from);
                (end - halt.from).num_minutes()
            })
            .sum()
    }
}

/// Result of a portfolio replay: one shared account and one output per instance in declared order.
#[derive(Debug)]
pub struct PortfolioBacktestResult {
    pub replay: BacktestResult,
    pub instances: Vec<PortfolioInstanceOutput>,
    pub supervisor: Option<SupervisorOutput>,
}

/// Failures of a portfolio replay.
#[derive(Debug, thiserror::Error)]
pub enum PortfolioReplayError<FeedError> {
    #[error("a portfolio replay needs at least one instance")]
    NoInstances,
    #[error("a portfolio replay accepts at most {MAX_PORTFOLIO_INSTANCES} instances, got {0}")]
    TooManyInstances(usize),
    #[error(
        "instance '{instance_id}' appears more than once; every instance of a portfolio needs its own identifier"
    )]
    DuplicateInstanceIdentity { instance_id: String },
    #[error("portfolio replay input is invalid: {0}")]
    Input(#[from] StrategyReplayInputError),
    #[error("portfolio supervisor is invalid: {0}")]
    Supervisor(String),
    #[error("instance '{instance_id}' failed: {source}")]
    Instance {
        instance_id: String,
        #[source]
        source: StrategyReplayError<Infallible, ConfiguredStrategyAdapterError>,
    },
    #[error(
        "the portfolio feed mixes ticks and stored bars from {timestamp}; a portfolio replays one kind of primary input"
    )]
    MixedPrimaryInput { timestamp: NaiveDateTime },
    #[error("market-data stream failed: {0}")]
    Feed(FeedError),
    #[error("portfolio replay was cancelled")]
    Cancelled,
}

impl<FeedError> From<RiskConfigError> for PortfolioReplayError<FeedError> {
    fn from(error: RiskConfigError) -> Self {
        Self::Supervisor(error.to_string())
    }
}
