use std::collections::HashSet;

use qs_backtest::evaluation::EvaluationOptions;
use qs_backtest::runner::BacktestConfig;
use qs_backtest::{FutureQuoteConfig, PreparedEntryProfiles, StrategyRetentionLimits};

use crate::error::ResearchError;
use crate::window::WindowPlan;

/// Everything a batch needs besides the family and the market data itself.
#[derive(Debug, Clone)]
pub struct ResearchPlan {
    /// Symbols to search, each evaluated independently.
    pub symbols: Vec<String>,
    /// The evaluation periods.
    pub window_plan: WindowPlan,
    /// Replay configuration shared by every run, including costs and sizing.
    ///
    /// Its `run_tags` are the labels every run carries in addition to the family's own parameter tags.
    pub config: BacktestConfig,
    /// FutureQuote configuration shared by every run.
    pub future: FutureQuoteConfig,
    /// Provider-evaluation selection applied to each run.
    ///
    /// A batch that wants to break its results down by a parameter asks for that tag dimension here; the tag itself is already attached to every position by the family.
    pub evaluation: EvaluationOptions,
    /// Retention limits applied to each run's strategy output.
    pub retention: StrategyRetentionLimits,
    /// Decision latency in milliseconds applied by each run's adapter.
    pub decision_latency_ms: u64,
    /// Worker threads used to evaluate points. One means sequential.
    pub workers: usize,
    /// Management profiles every run selects from, exactly as raw-signal replay does; `None` runs unprofiled.
    pub entry_profiles: Option<PreparedEntryProfiles>,
}

impl ResearchPlan {
    pub fn new(symbols: Vec<String>, window_plan: WindowPlan, config: BacktestConfig) -> Self {
        Self {
            symbols,
            window_plan,
            config,
            future: FutureQuoteConfig::default(),
            evaluation: EvaluationOptions::default(),
            retention: StrategyRetentionLimits::default(),
            decision_latency_ms: 0,
            workers: 1,
            entry_profiles: None,
        }
    }

    pub fn with_entry_profiles(mut self, profiles: PreparedEntryProfiles) -> Self {
        self.entry_profiles = Some(profiles);
        self
    }

    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn with_future(mut self, future: FutureQuoteConfig) -> Self {
        self.future = future;
        self
    }

    pub fn with_evaluation(mut self, evaluation: EvaluationOptions) -> Self {
        self.evaluation = evaluation;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), ResearchError> {
        if self.symbols.is_empty() {
            return Err(ResearchError::InvalidPlan(
                "a research plan must name at least one symbol".into(),
            ));
        }
        let mut seen: HashSet<&str> = HashSet::new();
        for symbol in &self.symbols {
            if symbol.trim().is_empty() {
                return Err(ResearchError::InvalidPlan(
                    "symbol must not be empty".into(),
                ));
            }
            if !seen.insert(symbol.as_str()) {
                return Err(ResearchError::InvalidPlan(format!(
                    "symbol '{symbol}' appears more than once"
                )));
            }
        }
        if self.workers == 0 {
            return Err(ResearchError::InvalidPlan(
                "a research plan must use at least one worker".into(),
            ));
        }
        Ok(())
    }
}
