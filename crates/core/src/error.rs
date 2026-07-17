use thiserror::Error;

/// Errors produced by the core trade engine.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("position not found: {0}")]
    PositionNotFound(String),

    #[error("group not found: {0}")]
    GroupNotFound(String),

    #[error("invalid action: {0}")]
    InvalidAction(String),

    #[error("take-profit target {price} not found for position {position_id}")]
    TargetNotFound { position_id: String, price: f64 },

    #[error("take-profit target {price} has already triggered for position {position_id}")]
    TargetAlreadyTriggered { position_id: String, price: f64 },

    #[error("no price available for symbol: {0}")]
    NoPriceAvailable(String),

    #[error("position not in expected state: id={id}, expected={expected}, actual={actual}")]
    InvalidState {
        id: String,
        expected: String,
        actual: String,
    },

    #[error("{0}")]
    Other(String),
}

/// Convenience alias used throughout qs-core.
pub type Result<T> = std::result::Result<T, CoreError>;
