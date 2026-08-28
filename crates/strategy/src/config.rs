use std::fmt;

use qs_core::OrderType;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{Literal, ScalarType, ValueType, validate_id};

pub const MAX_SOURCES: usize = 32;
pub const MAX_COMPLETED_BARS: usize = 32;
pub const MAX_MATERIALS: usize = 128;
pub const MAX_MATERIAL_INPUTS: usize = 16;
pub const MAX_MATERIAL_LOOKBACK: usize = 4096;
pub const MAX_MATERIAL_STATE_BYTES: usize = 65_536;
pub const MAX_STATES: usize = 64;
pub const MAX_VARIABLES: usize = 64;
pub const MAX_TRANSITIONS: usize = 64;
pub const MAX_ACTIONS: usize = 16;
pub const MAX_ASSIGNMENTS: usize = 32;
pub const MAX_ENTRY_TARGETS: usize = 16;
pub const MAX_NOTES: usize = 16;
pub const MAX_NAMED_VALUES: usize = 32;
pub const MAX_EXPR_NODES: usize = 512;
pub const MAX_EXPR_DEPTH: usize = 32;
pub const MAX_LEGS: usize = 16;
pub const MAX_OUTPUT_COMMANDS: usize = 16;
pub const MAX_OUTPUT_NOTES: usize = 16;
pub const MAX_COMMAND_CORRELATIONS: usize = 64;
pub const MAX_PENDING_FEEDBACK: usize = 64;

/// Bounded logical completed-bar source identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SourceId(String);

impl SourceId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Strict configured strategy document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub strategy_id: String,
    pub title: String,
    pub initial_state: String,
    pub sources: Vec<SourceId>,
    pub trade_slots: Vec<String>,
    #[serde(default)]
    pub materials: Vec<MaterialConfig>,
    #[serde(default)]
    pub variables: Vec<VariableConfig>,
    pub states: Vec<StateConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterialConfig {
    pub id: String,
    pub key: String,
    #[serde(default)]
    pub inputs: Vec<Expr>,
    #[serde(default)]
    pub params: MaterialParams,
}

/// Strict built-in material parameters. Custom factories are parameterless.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaterialParams {
    #[default]
    None,
    BarField {
        source: SourceId,
        field: BarField,
    },
    Ema {
        period: u16,
    },
    Atr {
        source: SourceId,
        period: u16,
    },
    Position {
        slot: String,
    },
    Feedback {
        slot: String,
        action: ConfiguredActionKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarField {
    Open,
    High,
    Low,
    Close,
    Volume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionField {
    Exists,
    IsPending,
    IsOpen,
    EntryPrice,
    Side,
    RemainingSize,
    Stoploss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackField {
    EntryFilled,
    EntryRejected,
    PositionClosed,
    CancellationApplied,
    CancellationRejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfiguredActionKind {
    Entry,
    Close,
    ClosePartial,
    MoveStoplossToEntry,
    ModifyStoploss,
    CancelPending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Entry,
    Management,
    Exit,
    Observation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoteKind {
    Observation,
    Risk,
    Execution,
    Lifecycle,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VariableConfig {
    pub id: String,
    pub value_type: ValueType,
    pub initial: Literal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    pub id: String,
    #[serde(default)]
    pub transitions: Vec<TransitionConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionConfig {
    pub priority: i32,
    pub target: String,
    pub when: Expr,
    #[serde(default)]
    pub assignments: Vec<AssignmentConfig>,
    pub decision: Option<DecisionTemplate>,
    #[serde(default)]
    pub actions: Vec<ActionTemplate>,
    #[serde(default)]
    pub notes: Vec<NoteTemplate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentConfig {
    pub variable: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionTemplate {
    pub kind: DecisionKind,
    pub reason: String,
    pub trade_slot: Option<String>,
    #[serde(default)]
    pub values: Vec<NamedExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoteTemplate {
    pub kind: NoteKind,
    pub reason: String,
    pub trade_slot: Option<String>,
    #[serde(default)]
    pub values: Vec<NamedExpr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedExpr {
    pub name: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expr {
    Literal {
        value: Literal,
    },
    Variable {
        id: String,
    },
    Material {
        id: String,
    },
    Input {
        field: String,
        value_type: ValueType,
    },
    Bar {
        source: SourceId,
        field: BarField,
    },
    Position {
        slot: String,
        field: PositionField,
    },
    Feedback {
        slot: String,
        action: ConfiguredActionKind,
        field: FeedbackField,
    },
    InputTime,
    Readiness,
    Eq {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Ne {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Lt {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Le {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Gt {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Ge {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    All {
        items: Vec<Expr>,
    },
    Any {
        items: Vec<Expr>,
    },
    Not {
        value: Box<Expr>,
    },
    Add {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Sub {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Mul {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Div {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Min {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Max {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Abs {
        value: Box<Expr>,
    },
    IsPresent {
        value: Box<Expr>,
    },
    IsMissing {
        value: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActionTemplate {
    Entry {
        slot: String,
        side: Expr,
        order_type: OrderType,
        price: Expr,
        risk: Expr,
        stoploss: Expr,
        #[serde(default)]
        targets: Vec<Expr>,
    },
    Close {
        slot: String,
    },
    ClosePartial {
        slot: String,
        ratio: Expr,
    },
    MoveStoplossToEntry {
        slot: String,
    },
    ModifyStoploss {
        slot: String,
        price: Expr,
    },
    CancelPending {
        slot: String,
    },
}

pub(crate) fn bar_field_type(field: BarField) -> ValueType {
    match field {
        BarField::Volume => ValueType::optional(ScalarType::Number),
        _ => ValueType::optional(ScalarType::Price),
    }
}
