//! Explicit comparison of two completed historical strategy results.

use serde::{Deserialize, Deserializer, Serialize};

use super::journal::{StrategyJournalError, maximum_research_limits, validate_experiment_label};
use super::{StrategyBacktestResult, StrategyDescriptor, StrategyResearchLimits};

/// Existing position-level metrics projected from one completed replay.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StrategyComparisonMetrics {
    pub total_positions: usize,
    pub position_win_rate: f64,
    pub total_pnl: f64,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    pub average_position_duration_secs: Option<i64>,
}

impl StrategyComparisonMetrics {
    fn from_result(result: &StrategyBacktestResult) -> Self {
        Self {
            total_positions: result.replay.total_positions,
            position_win_rate: result.replay.position_win_rate,
            total_pnl: result.replay.total_pnl,
            max_drawdown: result.replay.max_drawdown,
            max_drawdown_pct: result.replay.max_drawdown_pct,
            average_position_duration_secs: result
                .replay
                .duration_stats
                .as_ref()
                .map(|stats| stats.avg_duration_secs),
        }
    }

    fn validate(&self) -> Result<(), StrategyExperimentError> {
        for (field, value) in [
            ("position_win_rate", self.position_win_rate),
            ("total_pnl", self.total_pnl),
            ("max_drawdown", self.max_drawdown),
            ("max_drawdown_pct", self.max_drawdown_pct),
        ] {
            if !value.is_finite() {
                return Err(StrategyExperimentError::NonFiniteMetric { field });
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyComparisonMetricsDef {
    total_positions: usize,
    position_win_rate: f64,
    total_pnl: f64,
    max_drawdown: f64,
    max_drawdown_pct: f64,
    average_position_duration_secs: Option<i64>,
}

impl<'de> Deserialize<'de> for StrategyComparisonMetrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyComparisonMetricsDef::deserialize(deserializer)?;
        let metrics = Self {
            total_positions: value.total_positions,
            position_win_rate: value.position_win_rate,
            total_pnl: value.total_pnl,
            max_drawdown: value.max_drawdown,
            max_drawdown_pct: value.max_drawdown_pct,
            average_position_duration_secs: value.average_position_duration_secs,
        };
        metrics.validate().map_err(serde::de::Error::custom)?;
        Ok(metrics)
    }
}

/// Bounded label, descriptor, and projected metrics for one result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StrategyComparisonSnapshot {
    pub label: String,
    pub descriptor: StrategyDescriptor,
    pub metrics: StrategyComparisonMetrics,
}

impl StrategyComparisonSnapshot {
    fn new(
        label: impl Into<String>,
        result: &StrategyBacktestResult,
        limits: StrategyResearchLimits,
    ) -> Result<Self, StrategyExperimentError> {
        let label = label.into();
        validate_experiment_label(&label, limits)?;
        let metrics = StrategyComparisonMetrics::from_result(result);
        metrics.validate()?;
        Ok(Self {
            label,
            descriptor: result.descriptor.clone(),
            metrics,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyComparisonSnapshotDef {
    label: String,
    descriptor: StrategyDescriptor,
    metrics: StrategyComparisonMetrics,
}

impl<'de> Deserialize<'de> for StrategyComparisonSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyComparisonSnapshotDef::deserialize(deserializer)?;
        validate_experiment_label(&value.label, maximum_research_limits())
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            label: value.label,
            descriptor: value.descriptor,
            metrics: value.metrics,
        })
    }
}

/// Caller-ordered baseline and candidate result snapshots.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyExperimentComparison {
    pub baseline: StrategyComparisonSnapshot,
    pub candidate: StrategyComparisonSnapshot,
}

impl StrategyExperimentComparison {
    pub fn new(
        baseline_label: impl Into<String>,
        baseline: &StrategyBacktestResult,
        candidate_label: impl Into<String>,
        candidate: &StrategyBacktestResult,
        limits: StrategyResearchLimits,
    ) -> Result<Self, StrategyExperimentError> {
        Ok(Self {
            baseline: StrategyComparisonSnapshot::new(baseline_label, baseline, limits)?,
            candidate: StrategyComparisonSnapshot::new(candidate_label, candidate, limits)?,
        })
    }
}

/// Validation failures for explicit strategy-result comparison.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyExperimentError {
    #[error(transparent)]
    Journal(#[from] StrategyJournalError),
    #[error("comparison metric '{field}' must be finite")]
    NonFiniteMetric { field: &'static str },
}
