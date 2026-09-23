use std::fmt;

/// Why one run in a batch did not produce a result.
///
/// A batch never aborts on a single failure. The failure is recorded on that run's row so the table shows which configurations could not be evaluated alongside the ones that could.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunFailure {
    /// The family produced a configuration the strategy compiler rejected.
    Compile(String),
    /// The configuration compiled but could not be bound to the declared historical series.
    Bind(String),
    /// Replay rejected its inputs or failed while running.
    Replay(String),
}

impl RunFailure {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Compile(_) => "compile",
            Self::Bind(_) => "bind",
            Self::Replay(_) => "replay",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Compile(message) | Self::Bind(message) | Self::Replay(message) => message,
        }
    }
}

impl fmt::Display for RunFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.kind(), self.message())
    }
}

/// Failures that stop a batch before any run starts.
#[derive(Debug, thiserror::Error)]
pub enum ResearchError {
    #[error("failed to read research document: {0}")]
    Io(#[source] std::io::Error),

    #[error("invalid TOML research document: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("invalid strategy or space document: {0}")]
    InvalidDocument(String),

    /// The plan itself is not runnable.
    #[error("invalid research plan: {0}")]
    InvalidPlan(String),

    /// A declared window cannot be built.
    #[error("invalid window: {0}")]
    InvalidWindow(String),

    /// The caller cancelled the batch before every run finished.
    #[error("research batch was cancelled")]
    Cancelled,

    /// Stored market data could not be loaded.
    #[error(transparent)]
    Load(#[from] qs_market_loader::MarketLoadError),
}
