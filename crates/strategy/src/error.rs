use crate::{ScalarType, ValueType};

/// A terminal configuration compilation failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CompileError {
    #[error("invalid identifier at {path}: {reason}")]
    InvalidIdentifier { path: String, reason: String },
    #[error("duplicate identifier at {path}: {id}")]
    DuplicateIdentifier { path: String, id: String },
    #[error("unknown material key at {path}: {key}")]
    UnknownMaterialKey { path: String, key: String },
    #[error("unknown reference at {path}: {reference}")]
    UnknownReference { path: String, reference: String },
    #[error("material dependency cycle: {materials:?}")]
    DependencyCycle { materials: Vec<String> },
    #[error("type mismatch at {path}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        path: String,
        expected: ValueType,
        actual: ValueType,
    },
    #[error("invalid optional-to-required flow at {path}")]
    OptionalToRequired { path: String },
    #[error("invalid state target at {path}: {target}")]
    InvalidStateTarget { path: String, target: String },
    #[error("transition priority conflict in state {state}: {priority}")]
    PriorityConflict { state: String, priority: i32 },
    #[error("unreachable state: {state}")]
    UnreachableState { state: String },
    #[error("invalid configuration at {path}: {reason}")]
    InvalidConfig { path: String, reason: String },
    #[error("configured bound exceeded at {path}: {actual} > {limit}")]
    ExcessiveBound {
        path: String,
        actual: usize,
        limit: usize,
    },
    #[error("generated identity capacity is invalid")]
    InvalidIdCapacity,
    #[error("material factory failed at {path}: {reason}")]
    MaterialFactory { path: String, reason: String },
}

/// A terminal strategy evaluation failure.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EvaluationError {
    #[error("arithmetic overflow at {path}")]
    ArithmeticOverflow { path: String },
    #[error("division by zero at {path}")]
    DivisionByZero { path: String },
    #[error("non-finite result at {path}")]
    NonFinite { path: String },
    #[error("required value is missing at {path}")]
    MissingRequired { path: String },
    #[error("runtime type mismatch at {path}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        path: String,
        expected: ScalarType,
        actual: Option<ScalarType>,
    },
    #[error("material evaluation failed for {material}: {reason}")]
    Material { material: String, reason: String },
    #[error("invalid generated action at {path}: {reason}")]
    InvalidAction { path: String, reason: String },
    #[error("generated identity counter exhausted: {kind}")]
    CounterExhausted { kind: &'static str },
    #[error("generated identity exceeds the configured bound")]
    IdCapacity,
    #[error("runtime output bound exceeded: {kind}")]
    OutputBound { kind: &'static str },
    #[error("strategy is terminal after a previous evaluation error")]
    Terminal,
}
