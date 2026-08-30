use thiserror::Error;

/// Stable high-level category for client workflow failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowErrorCategory {
    Input,
    Configuration,
    Connection,
    Submission,
    Job,
    ArtifactIntegrity,
    Output,
    ResultFormat,
    Analysis,
}

/// Provider-neutral failure produced while inspecting or preparing a run.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkflowError {
    #[error("input preparation was cancelled")]
    PreparationCancelled,
    #[error("could not open input {display_name}: {detail}")]
    InputOpen {
        display_name: String,
        detail: String,
    },
    #[error("could not read input {display_name} near physical line {line}: {detail}")]
    InputRead {
        display_name: String,
        line: u64,
        detail: String,
    },
    #[error("input {display_name} exceeds the {resource} limit of {limit} bytes")]
    InputByteLimit {
        display_name: String,
        resource: &'static str,
        limit: u64,
    },
    #[error("input {display_name} physical line {line} exceeds the line limit of {limit} bytes")]
    InputLineLimit {
        display_name: String,
        line: u64,
        limit: usize,
    },
    #[error("input {display_name} exceeds the signal count limit of {limit}")]
    SignalCountLimit { display_name: String, limit: usize },
    #[error("input {display_name} physical line {line} is not valid UTF-8")]
    InvalidUtf8 { display_name: String, line: u64 },
    #[error("input {display_name} physical line {line} is not a valid signal: {detail}")]
    SignalDecode {
        display_name: String,
        line: u64,
        detail: String,
    },
    #[error("input {display_name} physical line {line} has an invalid timestamp: {detail}")]
    SignalTimestamp {
        display_name: String,
        line: u64,
        detail: String,
    },
    #[error("invalid {field}: {detail}")]
    InvalidConfiguration { field: &'static str, detail: String },
    #[error("failed to serialize the backtest request: {detail}")]
    RequestSerialization { detail: String },
    #[error("serialized backtest request is {actual} bytes, exceeding the {limit}-byte limit")]
    RequestTooLarge { actual: usize, limit: usize },
    #[error("background preparation task failed: {detail}")]
    PreparationTask { detail: String },
}

impl WorkflowError {
    pub fn category(&self) -> WorkflowErrorCategory {
        match self {
            Self::PreparationCancelled
            | Self::InputOpen { .. }
            | Self::InputRead { .. }
            | Self::InputByteLimit { .. }
            | Self::InputLineLimit { .. }
            | Self::SignalCountLimit { .. }
            | Self::InvalidUtf8 { .. }
            | Self::SignalDecode { .. }
            | Self::SignalTimestamp { .. }
            | Self::PreparationTask { .. } => WorkflowErrorCategory::Input,
            Self::InvalidConfiguration { .. }
            | Self::RequestSerialization { .. }
            | Self::RequestTooLarge { .. } => WorkflowErrorCategory::Configuration,
        }
    }
}
