use qs_strategy::{CommandFeedback, CommandTerminalStatus};
use thiserror::Error;

use crate::types::bounded_reason;

/// Invalid high-level execution intent.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IntentError {
    #[error("{field} must be non-empty ASCII no longer than {maximum} bytes")]
    InvalidIdentifier { field: &'static str, maximum: usize },
    #[error("decision time {decision_time} does not match signal time {signal_time}")]
    TimestampMismatch {
        decision_time: chrono::NaiveDateTime,
        signal_time: chrono::NaiveDateTime,
    },
    #[error("invalid raw signal: {0}")]
    InvalidSignal(String),
}

/// A signal could not be lowered into a provider request.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum PreparationError {
    #[error(transparent)]
    Intent(#[from] IntentError),
    #[error("current quote is required for {operation}")]
    MissingQuote { operation: &'static str },
    #[error("instrument facts are required for {operation}")]
    MissingInstrument { operation: &'static str },
    #[error("signal symbol '{signal}' does not match {field} symbol '{actual}'")]
    SymbolMismatch {
        field: &'static str,
        signal: String,
        actual: String,
    },
    #[error("invalid current quote: {0}")]
    InvalidQuote(String),
    #[error("preparation time {prepared_at} is before decision time {decision_time}")]
    PreparationBeforeDecision {
        prepared_at: chrono::NaiveDateTime,
        decision_time: chrono::NaiveDateTime,
    },
    #[error("quote time {quote_time} is after preparation time {prepared_at}")]
    FutureQuote {
        quote_time: chrono::NaiveDateTime,
        prepared_at: chrono::NaiveDateTime,
    },
    #[error("instrument is unavailable for entry: {reason}")]
    InstrumentUnavailable { reason: String },
    #[error("entry profile resolution failed: {0}")]
    Profile(String),
    #[error("position sizing failed: {0}")]
    Sizing(String),
    #[error("provider does not support {operation}")]
    UnsupportedCapability { operation: &'static str },
    #[error("ongoing management rules require an explicit provider or runtime owner")]
    MissingManagementOwner,
    #[error("unsupported raw signal operation '{operation}'")]
    UnsupportedOperation { operation: &'static str },
    #[error("position reference is ambiguous across {count} candidates")]
    AmbiguousReference { count: usize },
    #[error("position reference cannot be resolved from current authoritative state")]
    UnknownReference,
    #[error("the referenced position is known absent for {operation}")]
    KnownAbsent { operation: &'static str },
    #[error("position '{position_id}' is {actual}, but {operation} requires {required}")]
    InvalidPositionStatus {
        position_id: String,
        actual: &'static str,
        required: &'static str,
        operation: &'static str,
    },
    #[error("invalid position snapshot: {0}")]
    InvalidPositionSnapshot(String),
    #[error("invalid {field} price {value}")]
    InvalidPrice { field: &'static str, value: f64 },
    #[error("{field} price {value} is outside the position price grid")]
    PriceOffGrid { field: &'static str, value: f64 },
    #[error("the requested close rounds to zero quantity steps")]
    ZeroCloseQuantity,
    #[error("risk review rejected the request under '{policy}': {reason}")]
    RiskRejected { policy: String, reason: String },
    #[error("request identity could not be constructed: {0}")]
    RequestIdentity(String),
}

impl PreparationError {
    /// Convert a preparation refusal into configured terminal feedback.
    pub fn terminal_feedback(&self, command_id: impl Into<String>) -> CommandFeedback {
        let command_id = command_id.into();
        CommandFeedback::Terminal {
            command_id,
            status: CommandTerminalStatus::Rejected,
            reason: Some(bounded_reason(self.to_string())),
        }
    }
}

/// Approval-gate validation or state error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ApprovalError {
    #[error("approval mode must be explicitly configured")]
    MissingMode,
    #[error("approval timeout must be positive")]
    InvalidTimeout,
    #[error("approval capacity must be positive")]
    InvalidCapacity,
    #[error("approval queue is full at {maximum} pending entries")]
    CapacityExceeded { maximum: usize },
    #[error("approval gate consumed-command capacity is full at {maximum}")]
    ConsumedCapacityExceeded { maximum: usize },
    #[error("approval expiry timestamp overflowed")]
    TimestampOverflow,
}

/// Invalid public provider request.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid execution request: {0}")]
pub struct RequestValidationError(pub String);

/// Provider submission failure classification.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SubmissionError {
    #[error("request was definitely not submitted: {reason}")]
    DefinitelyNotSubmitted { reason: String },
    #[error("provider acceptance is unknown: {reason}")]
    AcceptanceUnknown { reason: String },
}

impl SubmissionError {
    /// Whether a caller may release an exposure reservation immediately.
    pub const fn definitely_not_submitted(&self) -> bool {
        matches!(self, Self::DefinitelyNotSubmitted { .. })
    }
}

/// Failure while receiving provider observations after submission.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReportStreamError {
    #[error("report stream is unavailable: {reason}")]
    Unavailable { reason: String },
    #[error("provider report protocol failed: {reason}")]
    Protocol { reason: String },
}

/// Invalid, conflicting, or unprojectable provider report.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FeedbackError {
    #[error("feedback capacity must be positive")]
    InvalidCapacity,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("request '{request_id}' is already registered")]
    DuplicateRequest { request_id: String },
    #[error("parent command '{command_id}' is already registered")]
    DuplicateParent { command_id: String },
    #[error("parent command '{command_id}' still has active or incomplete child requests")]
    ActiveParentChildren { command_id: String },
    #[error("too many active requests; maximum is {maximum}")]
    RequestCapacityExceeded { maximum: usize },
    #[error("report observation capacity exceeded at {maximum}")]
    ObservationCapacityExceeded { maximum: usize },
    #[error("report references unknown or already completed request '{request_id}'")]
    UnknownRequest { request_id: String },
    #[error("report parent command '{actual}' does not match expected '{expected}'")]
    ParentCommandMismatch { expected: String, actual: String },
    #[error("observation '{observation_id}' was repeated with conflicting contents")]
    ConflictingDuplicate { observation_id: String },
    #[error("invalid report: {0}")]
    InvalidReport(String),
    #[error("report outcome {outcome} is invalid for request kind {request_kind}")]
    OutcomeMismatch {
        outcome: &'static str,
        request_kind: &'static str,
    },
    #[error(
        "request '{request_id}' has committed partial exposure but ended without a projectable terminal: {reason}"
    )]
    PartialOutcomeUnresolved { request_id: String, reason: String },
}
