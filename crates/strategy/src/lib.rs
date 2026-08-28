//! Synchronous configured strategy compilation and evaluation.
//!
//! The crate accepts strict, bounded, unversioned configuration and compiles it
//! into a deterministic material graph and finite state machine. Callers own
//! input adaptation, material libraries, command execution, and persistence.

mod config;
mod error;
mod expression;
mod material;
mod runtime;
mod value;

pub use config::{
    ActionTemplate, AssignmentConfig, BarField, ConfiguredActionKind, DecisionKind,
    DecisionTemplate, Expr, FeedbackField, MAX_ACTIONS, MAX_ASSIGNMENTS, MAX_COMPLETED_BARS,
    MAX_ENTRY_TARGETS, MAX_EXPR_DEPTH, MAX_EXPR_NODES, MAX_LEGS, MAX_MATERIAL_INPUTS,
    MAX_MATERIAL_LOOKBACK, MAX_MATERIAL_STATE_BYTES, MAX_MATERIALS, MAX_NAMED_VALUES, MAX_NOTES,
    MAX_OUTPUT_COMMANDS, MAX_OUTPUT_NOTES, MAX_PENDING_FEEDBACK, MAX_SOURCES, MAX_STATES,
    MAX_TRANSITIONS, MAX_VARIABLES, MaterialConfig, MaterialParams, NamedExpr, NoteKind,
    NoteTemplate, PositionField, SourceId, StateConfig, StrategyConfig, TransitionConfig,
    VariableConfig,
};
pub use error::{CompileError, EvaluationError};
pub use material::{
    CommandFact, CommandFeedback, CommandTerminalStatus, CompletedBar, CompletedBarRequirement,
    CompletedBarUpdate, ConfiguredStrategyRequirements, MATERIAL_ATR, MATERIAL_BAR_FIELD,
    MATERIAL_CANCELLATION_APPLIED, MATERIAL_CANCELLATION_REJECTED, MATERIAL_CROSS_ABOVE,
    MATERIAL_CROSS_BELOW, MATERIAL_EMA, MATERIAL_ENTRY_FILLED, MATERIAL_ENTRY_REJECTED,
    MATERIAL_INPUT_TIME, MATERIAL_POSITION_CLOSED, MATERIAL_POSITION_ENTRY_PRICE,
    MATERIAL_POSITION_EXISTS, MATERIAL_POSITION_OPEN, MATERIAL_POSITION_PENDING,
    MATERIAL_POSITION_REMAINING_SIZE, MATERIAL_POSITION_SIDE, MATERIAL_POSITION_STOPLOSS,
    MATERIAL_READINESS, MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory,
    MaterialLibrary, MaterialLookback, MaterialUpdateTrigger, NamedInputRequirement, NamedValue,
    StrategyInput, TradeSlotFacts, TradeSlotState,
};
pub use runtime::{
    ConfiguredCommand, ConfiguredStrategy, Decision, NamedOutput, Note, OutputScalar, RelatedTrade,
    StrategyOutput,
};
pub use value::{
    Literal, MAX_GENERATED_ID_BYTES, MAX_ID_BYTES, MAX_TEXT_BYTES, ScalarType, Value, ValueType,
};

pub(crate) use config::{MAX_COMMAND_CORRELATIONS, bar_field_type};
pub(crate) use value::{validate_id, validate_text};
