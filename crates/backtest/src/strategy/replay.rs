//! Historical strategy integration with FutureQuote replay.

use chrono::NaiveDateTime;

use super::{
    AnalysisError, BarSeriesSpec, ConfiguredStrategyAdapterPreflightError, SeriesError,
    SeriesViewError, StrategyRequirements, StrategyRuntimeError,
};

/// Errors returned while running a historical strategy through FutureQuote.
#[derive(Debug, thiserror::Error)]
pub enum StrategyReplayError<FeedError, StrategyError> {
    #[error("market-data stream failed: {0}")]
    Feed(FeedError),
    #[error("strategy replay input is invalid: {0}")]
    Input(#[from] StrategyReplayInputError),
    #[error("historical series failed: {0}")]
    Series(#[from] SeriesError),
    #[error("historical series view failed: {0}")]
    SeriesView(#[from] SeriesViewError),
    #[error("historical analysis failed: {0}")]
    Analysis(#[from] AnalysisError),
    #[error("strategy callback failed: {0}")]
    Strategy(StrategyError),
    #[error("strategy output failed validation: {0}")]
    Runtime(#[from] StrategyRuntimeError),
    #[error("strategy emitted economic signals during warmup at {timestamp}")]
    WarmupSignals { timestamp: NaiveDateTime },
    #[error("generated signal {signal_index} is invalid: {reason}")]
    InvalidGeneratedSignal { signal_index: usize, reason: String },
    #[error(
        "strategy requires tick execution but received a primary bar for {symbol} at {timestamp}"
    )]
    TickExecutionRequired {
        symbol: String,
        timestamp: NaiveDateTime,
    },
}

/// Preflight failures for strategy series and replay configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyReplayInputError {
    #[error("FutureQuote configuration is invalid: {0}")]
    FutureQuote(String),
    #[error("management profile is invalid: {0}")]
    ManagementProfile(String),
    #[error("configured historical strategies do not support ManagementProfile")]
    ConfiguredManagementProfileUnsupported,
    #[error("configured historical adapter is incompatible with replay limits: {0}")]
    ConfiguredAdapter(#[from] ConfiguredStrategyAdapterPreflightError),
    #[error("series '{series_id}' is required but was not supplied")]
    MissingSeries { series_id: String },
    #[error("series '{series_id}' does not match the strategy requirement")]
    ConflictingSeries { series_id: String },
    #[error("series '{series_id}' was supplied but is not declared by the strategy")]
    UndeclaredSeries { series_id: String },
}

pub(crate) fn validate_series_specs(
    requirements: &StrategyRequirements,
    specs: &[BarSeriesSpec],
) -> Result<(), StrategyReplayInputError> {
    for requirement in requirements.series() {
        let Some(spec) = specs
            .iter()
            .find(|spec| spec.requirement().id() == requirement.id())
        else {
            return Err(StrategyReplayInputError::MissingSeries {
                series_id: requirement.id().to_string(),
            });
        };
        if spec.requirement() != requirement {
            return Err(StrategyReplayInputError::ConflictingSeries {
                series_id: requirement.id().to_string(),
            });
        }
    }
    for spec in specs {
        if !requirements
            .series()
            .iter()
            .any(|requirement| requirement.id() == spec.requirement().id())
        {
            return Err(StrategyReplayInputError::UndeclaredSeries {
                series_id: spec.requirement().id().to_string(),
            });
        }
    }
    Ok(())
}
