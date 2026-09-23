use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Timelike};
use qs_core::Side;

use crate::{
    BarField, CompileError, ConfiguredActionKind, EvaluationError, FeedbackField, MaterialArg,
    MaterialArgs, PositionField, ScalarType, SourceId, Value, ValueType,
};

pub const MATERIAL_BAR_FIELD: &str = "completed_bar_field";
pub const MATERIAL_INPUT_TIME: &str = "input_time";
pub const MATERIAL_READINESS: &str = "readiness";
/// ISO weekday of the authoritative input time, Monday = 1 through Sunday = 7.
pub const MATERIAL_WEEKDAY: &str = "weekday";
/// Seconds elapsed since midnight of the authoritative input time, 0 through 86399.
pub const MATERIAL_SECONDS_OF_DAY: &str = "seconds_of_day";
pub const MATERIAL_EMA: &str = "ema";
pub const MATERIAL_ATR: &str = "atr";
pub const MATERIAL_SMA: &str = "sma";
pub const MATERIAL_STDDEV: &str = "stddev";
pub const MATERIAL_ROLLING_MIN: &str = "rolling_min";
pub const MATERIAL_ROLLING_MAX: &str = "rolling_max";
pub const MATERIAL_LAG: &str = "lag";
pub const MATERIAL_RSI: &str = "rsi";
pub const MATERIAL_CROSS_ABOVE: &str = "cross_above";
pub const MATERIAL_CROSS_BELOW: &str = "cross_below";
pub const MATERIAL_POSITION_EXISTS: &str = "position_exists";
pub const MATERIAL_POSITION_PENDING: &str = "position_pending";
pub const MATERIAL_POSITION_OPEN: &str = "position_open";
pub const MATERIAL_POSITION_ENTRY_PRICE: &str = "position_entry_price";
pub const MATERIAL_POSITION_SIDE: &str = "position_side";
pub const MATERIAL_POSITION_REMAINING_SIZE: &str = "position_remaining_size";
pub const MATERIAL_POSITION_STOPLOSS: &str = "position_stoploss";
pub const MATERIAL_POSITION_OPENED_AT: &str = "position_opened_at";
pub const MATERIAL_POSITION_FAVORABLE_EXCURSION: &str = "position_favorable_excursion";
pub const MATERIAL_POSITION_ADVERSE_EXCURSION: &str = "position_adverse_excursion";
pub const MATERIAL_POSITION_INITIAL_RISK: &str = "position_initial_risk";
pub const MATERIAL_BARS_SINCE_OPEN: &str = "bars_since_open";
pub const MATERIAL_ENTRY_FILLED: &str = "entry_filled";
pub const MATERIAL_ENTRY_REJECTED: &str = "entry_rejected";
pub const MATERIAL_POSITION_CLOSED: &str = "position_closed";
pub const MATERIAL_CANCELLATION_APPLIED: &str = "cancellation_applied";
pub const MATERIAL_CANCELLATION_REJECTED: &str = "cancellation_rejected";

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedBar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedBarUpdate {
    pub source: SourceId,
    pub bar: CompletedBar,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedValue {
    pub name: String,
    pub value: Value,
    pub updated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TradeSlotState {
    Vacant,
    Pending {
        side: Side,
        requested_price: Option<f64>,
        stoploss: Option<f64>,
    },
    Open {
        side: Side,
        entry_price: f64,
        remaining_size: f64,
        stoploss: Option<f64>,
        /// Authoritative time of the committed entry fill.
        opened_at: NaiveDateTime,
        /// Best campaign profit and loss since entry in the adapter's account currency, never below zero, or missing when the adapter cannot price the position yet.
        favorable_excursion: Option<f64>,
        /// Worst campaign profit and loss since entry in the adapter's account currency, never above zero, or missing when the adapter cannot price the position yet.
        adverse_excursion: Option<f64>,
        /// Positive initial risk amount in account currency for R normalization, or missing when the entry has no usable protective stop.
        initial_risk: Option<f64>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TradeSlotFacts {
    pub slot: String,
    pub state: TradeSlotState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandFact {
    EntryFilled,
    PositionReduced,
    PositionClosed,
    StoplossModified,
    PendingCancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTerminalStatus {
    Applied,
    Skipped,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandFeedback {
    Fact {
        command_id: String,
        fact: CommandFact,
    },
    Terminal {
        command_id: String,
        status: CommandTerminalStatus,
        reason: Option<String>,
    },
}

impl CommandFeedback {
    pub fn command_id(&self) -> &str {
        match self {
            Self::Fact { command_id, .. } | Self::Terminal { command_id, .. } => command_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StrategyInput {
    pub time: NaiveDateTime,
    pub ready: bool,
    pub completed_bars: Vec<CompletedBarUpdate>,
    pub values: Vec<NamedValue>,
    pub trade_slots: Vec<TradeSlotFacts>,
    pub feedback: Vec<CommandFeedback>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedBarRequirement {
    pub source: SourceId,
    pub required_lookback: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedInputRequirement {
    pub name: String,
    pub value_type: ValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredStrategyRequirements {
    pub completed_bars: Vec<CompletedBarRequirement>,
    pub named_inputs: Vec<NamedInputRequirement>,
    pub trade_slots: Vec<String>,
    pub needs_command_feedback: bool,
    /// Every distinct trade slot and routing class an Entry action can emit, in sorted order.
    pub entries: Vec<EntryRequirement>,
    /// Trade slots whose stoploss the strategy moves itself through `ModifyStoploss` or `MoveStoplossToEntry`, in sorted order.
    pub stop_managed_slots: Vec<String>,
}

/// One trade slot an Entry action reserves and the class it carries, where `None` is an unclassified entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntryRequirement {
    pub slot: String,
    pub entry_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterialLookback {
    None,
    Sources(Vec<CompletedBarRequirement>),
    InheritInputs { minimum: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeedbackObservation {
    pub slot: String,
    pub action: ConfiguredActionKind,
    pub field: FeedbackField,
}

pub struct MaterialEvalContext<'a> {
    pub input: &'a StrategyInput,
    pub input_updates: &'a [bool],
    pub(crate) feedback: &'a [FeedbackObservation],
    pub(crate) retained_feedback: &'a [FeedbackObservation],
}

impl MaterialEvalContext<'_> {
    pub fn feedback_matches(
        &self,
        slot: &str,
        action: ConfiguredActionKind,
        field: FeedbackField,
    ) -> bool {
        feedback_matches(self.feedback, slot, action, field)
    }

    fn visible_feedback_matches(
        &self,
        slot: &str,
        action: ConfiguredActionKind,
        field: FeedbackField,
    ) -> bool {
        self.feedback_matches(slot, action, field)
            || feedback_matches(self.retained_feedback, slot, action, field)
    }
}

fn feedback_matches(
    feedback: &[FeedbackObservation],
    slot: &str,
    action: ConfiguredActionKind,
    field: FeedbackField,
) -> bool {
    feedback
        .iter()
        .any(|item| item.slot == slot && item.action == action && item.field == field)
}

/// Stateful deterministic material evaluator.
///
/// `clone_box` must deep-clone all semantic evaluator state. Shared mutable semantic state and
/// external side effects violate this contract. Shared immutable factory data and non-semantic
/// telemetry are allowed. The runtime clones evaluators before a boundary and commits those clones
/// only after the complete strategy evaluation succeeds.
pub trait MaterialEvaluator: Send {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator>;
    fn evaluate(
        &mut self,
        inputs: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String>;
}

impl Clone for Box<dyn MaterialEvaluator> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterialUpdateTrigger {
    EveryInput,
    Source(SourceId),
    FeedbackPulse,
    /// Evaluate when every input expression updated at this boundary.
    AllInputs,
    /// Evaluate when any causal leaf of any input expression updated at this boundary.
    AnyInput,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParamKind {
    Integer { min: i64, max: i64 },
    Number { min: f64, max: f64 },
    Source,
    Slot,
    BarField,
    ActionKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ParamSpec {
    pub name: &'static str,
    pub kind: ParamKind,
    pub required: bool,
}

/// Result of constructing one material evaluator.
///
/// `max_state_bytes` is the factory's deterministic upper bound for evaluator-owned semantic state.
pub struct MaterialBuild {
    pub output_type: ValueType,
    pub lookback: MaterialLookback,
    pub max_state_bytes: usize,
    pub evaluator: Box<dyn MaterialEvaluator>,
}

pub trait MaterialFactory: Send + Sync {
    fn params(&self) -> &[ParamSpec] {
        &[]
    }

    fn build(
        &self,
        params: &MaterialArgs,
        input_types: &[ValueType],
    ) -> Result<MaterialBuild, String>;

    fn update_trigger(
        &self,
        _params: &MaterialArgs,
        _input_types: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::EveryInput)
    }
}

#[derive(Clone)]
struct Registration {
    key: String,
    factory: Arc<dyn MaterialFactory>,
}

#[derive(Clone)]
pub struct MaterialLibrary {
    registrations: Vec<Registration>,
}

impl MaterialLibrary {
    pub fn builtins() -> Self {
        let keys = [
            MATERIAL_BAR_FIELD,
            MATERIAL_INPUT_TIME,
            MATERIAL_READINESS,
            MATERIAL_WEEKDAY,
            MATERIAL_SECONDS_OF_DAY,
            MATERIAL_EMA,
            MATERIAL_ATR,
            MATERIAL_SMA,
            MATERIAL_STDDEV,
            MATERIAL_ROLLING_MIN,
            MATERIAL_ROLLING_MAX,
            MATERIAL_LAG,
            MATERIAL_RSI,
            MATERIAL_CROSS_ABOVE,
            MATERIAL_CROSS_BELOW,
            MATERIAL_POSITION_EXISTS,
            MATERIAL_POSITION_PENDING,
            MATERIAL_POSITION_OPEN,
            MATERIAL_POSITION_ENTRY_PRICE,
            MATERIAL_POSITION_SIDE,
            MATERIAL_POSITION_REMAINING_SIZE,
            MATERIAL_POSITION_STOPLOSS,
            MATERIAL_POSITION_OPENED_AT,
            MATERIAL_POSITION_FAVORABLE_EXCURSION,
            MATERIAL_POSITION_ADVERSE_EXCURSION,
            MATERIAL_POSITION_INITIAL_RISK,
            MATERIAL_BARS_SINCE_OPEN,
            MATERIAL_ENTRY_FILLED,
            MATERIAL_ENTRY_REJECTED,
            MATERIAL_POSITION_CLOSED,
            MATERIAL_CANCELLATION_APPLIED,
            MATERIAL_CANCELLATION_REJECTED,
        ];
        Self {
            registrations: keys
                .into_iter()
                .map(|key| Registration {
                    key: key.into(),
                    factory: Arc::new(BuiltinFactory { key }),
                })
                .collect(),
        }
    }

    pub fn with_factory(
        mut self,
        key: impl Into<String>,
        factory: Arc<dyn MaterialFactory>,
    ) -> Result<Self, CompileError> {
        let key = key.into();
        crate::validate_id(&key).map_err(|reason| CompileError::InvalidIdentifier {
            path: "material_library.key".into(),
            reason,
        })?;
        if self.registrations.iter().any(|item| item.key == key) {
            return Err(CompileError::DuplicateIdentifier {
                path: "material_library".into(),
                id: key,
            });
        }
        self.registrations.push(Registration { key, factory });
        Ok(self)
    }

    pub(crate) fn factory(&self, key: &str) -> Option<&Arc<dyn MaterialFactory>> {
        self.registration(key).map(|item| &item.factory)
    }

    pub fn parameter_schema(&self, key: &str) -> Option<&[ParamSpec]> {
        self.registration(key).map(|item| item.factory.params())
    }

    fn registration(&self, key: &str) -> Option<&Registration> {
        self.registrations.iter().find(|item| item.key == key)
    }
}

struct BuiltinFactory {
    key: &'static str,
}

const PERIOD_SCHEMA: [ParamSpec; 1] = [ParamSpec {
    name: "period",
    kind: ParamKind::Integer {
        min: 1,
        max: crate::MAX_MATERIAL_LOOKBACK as i64,
    },
    required: true,
}];
const SOURCE_FIELD_SCHEMA: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "field",
        kind: ParamKind::BarField,
        required: true,
    },
];
const SOURCE_PERIOD_SCHEMA: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 1,
            max: crate::MAX_MATERIAL_LOOKBACK as i64 - 1,
        },
        required: true,
    },
];
const SLOT_SCHEMA: [ParamSpec; 1] = [ParamSpec {
    name: "slot",
    kind: ParamKind::Slot,
    required: true,
}];
const SLOT_SOURCE_SCHEMA: [ParamSpec; 2] = [
    ParamSpec {
        name: "slot",
        kind: ParamKind::Slot,
        required: true,
    },
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
];
const FEEDBACK_SCHEMA: [ParamSpec; 2] = [
    ParamSpec {
        name: "slot",
        kind: ParamKind::Slot,
        required: true,
    },
    ParamSpec {
        name: "action",
        kind: ParamKind::ActionKind,
        required: true,
    },
];

impl MaterialFactory for BuiltinFactory {
    fn params(&self) -> &[ParamSpec] {
        match self.key {
            MATERIAL_BAR_FIELD => &SOURCE_FIELD_SCHEMA,
            MATERIAL_EMA | MATERIAL_SMA | MATERIAL_STDDEV | MATERIAL_ROLLING_MIN
            | MATERIAL_ROLLING_MAX | MATERIAL_LAG | MATERIAL_RSI => &PERIOD_SCHEMA,
            MATERIAL_ATR => &SOURCE_PERIOD_SCHEMA,
            MATERIAL_POSITION_EXISTS
            | MATERIAL_POSITION_PENDING
            | MATERIAL_POSITION_OPEN
            | MATERIAL_POSITION_ENTRY_PRICE
            | MATERIAL_POSITION_SIDE
            | MATERIAL_POSITION_REMAINING_SIZE
            | MATERIAL_POSITION_STOPLOSS
            | MATERIAL_POSITION_OPENED_AT
            | MATERIAL_POSITION_FAVORABLE_EXCURSION
            | MATERIAL_POSITION_ADVERSE_EXCURSION
            | MATERIAL_POSITION_INITIAL_RISK => &SLOT_SCHEMA,
            MATERIAL_BARS_SINCE_OPEN => &SLOT_SOURCE_SCHEMA,
            MATERIAL_ENTRY_FILLED
            | MATERIAL_ENTRY_REJECTED
            | MATERIAL_POSITION_CLOSED
            | MATERIAL_CANCELLATION_APPLIED
            | MATERIAL_CANCELLATION_REJECTED => &FEEDBACK_SCHEMA,
            _ => &[],
        }
    }

    fn build(&self, params: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let state_bytes = match self.key {
            MATERIAL_EMA | MATERIAL_RSI => 64,
            MATERIAL_ATR | MATERIAL_CROSS_ABOVE | MATERIAL_CROSS_BELOW => 48,
            MATERIAL_SMA | MATERIAL_STDDEV | MATERIAL_ROLLING_MIN | MATERIAL_ROLLING_MAX
            | MATERIAL_LAG => checked_period(params)? * std::mem::size_of::<f64>() + 64,
            _ => crate::MAX_GENERATED_ID_BYTES + 64,
        };
        let build = |output_type, lookback, evaluator: Box<dyn MaterialEvaluator>| {
            Ok(MaterialBuild {
                output_type,
                lookback,
                max_state_bytes: state_bytes,
                evaluator,
            })
        };
        match self.key {
            MATERIAL_BAR_FIELD => {
                require_inputs(inputs, &[])?;
                let source = source_arg(params, "source")?;
                let field = bar_field_arg(params, "field")?;
                build(
                    crate::bar_field_type(field),
                    source_lookback(source.clone(), 1),
                    Box::new(BarFieldEvaluator { source, field }),
                )
            }
            MATERIAL_INPUT_TIME => {
                require_none(params)?;
                require_inputs(inputs, &[])?;
                build(
                    ValueType::required(ScalarType::Timestamp),
                    MaterialLookback::None,
                    Box::new(InputTimeEvaluator),
                )
            }
            MATERIAL_WEEKDAY | MATERIAL_SECONDS_OF_DAY => {
                require_none(params)?;
                require_inputs(inputs, &[])?;
                build(
                    ValueType::required(ScalarType::Integer),
                    MaterialLookback::None,
                    Box::new(CalendarEvaluator {
                        weekday: self.key == MATERIAL_WEEKDAY,
                    }),
                )
            }
            MATERIAL_READINESS => {
                require_none(params)?;
                require_inputs(inputs, &[])?;
                build(
                    ValueType::required(ScalarType::Bool),
                    MaterialLookback::None,
                    Box::new(ReadinessEvaluator),
                )
            }
            MATERIAL_EMA => {
                let period = checked_period(params)?;
                require_one_numeric(inputs)?;
                build(
                    ValueType::optional(inputs[0].scalar),
                    MaterialLookback::InheritInputs { minimum: period },
                    Box::new(EmaEvaluator {
                        alpha: 2.0 / (period as f64 + 1.0),
                        value: None,
                        scalar: inputs[0].scalar,
                    }),
                )
            }
            MATERIAL_ATR => {
                let source = source_arg(params, "source")?;
                let period = checked_period(params)?;
                require_inputs(inputs, &[])?;
                build(
                    ValueType::optional(ScalarType::Price),
                    source_lookback(source.clone(), period + 1),
                    Box::new(AtrEvaluator {
                        source,
                        alpha: 1.0 / period as f64,
                        previous_close: None,
                        value: None,
                    }),
                )
            }
            MATERIAL_SMA | MATERIAL_STDDEV | MATERIAL_ROLLING_MIN | MATERIAL_ROLLING_MAX => {
                let period = checked_period(params)?;
                require_one_numeric(inputs)?;
                let kind = match self.key {
                    MATERIAL_SMA => RollingKind::Mean,
                    MATERIAL_STDDEV => RollingKind::PopulationStdDev,
                    MATERIAL_ROLLING_MIN => RollingKind::Min,
                    MATERIAL_ROLLING_MAX => RollingKind::Max,
                    _ => unreachable!(),
                };
                build(
                    ValueType::optional(inputs[0].scalar),
                    MaterialLookback::InheritInputs { minimum: period },
                    Box::new(RollingEvaluator {
                        period,
                        values: VecDeque::with_capacity(period),
                        scalar: inputs[0].scalar,
                        kind,
                    }),
                )
            }
            MATERIAL_LAG => {
                let period = checked_period(params)?;
                require_one_numeric(inputs)?;
                build(
                    ValueType::optional(inputs[0].scalar),
                    MaterialLookback::InheritInputs {
                        minimum: period + 1,
                    },
                    Box::new(LagEvaluator {
                        period,
                        values: VecDeque::with_capacity(period + 1),
                        scalar: inputs[0].scalar,
                    }),
                )
            }
            MATERIAL_RSI => {
                let period = checked_period(params)?;
                require_one_numeric(inputs)?;
                build(
                    ValueType::optional(ScalarType::Number),
                    MaterialLookback::InheritInputs {
                        minimum: period + 1,
                    },
                    Box::new(RsiEvaluator {
                        period,
                        previous: None,
                        seed_gains: 0.0,
                        seed_losses: 0.0,
                        seed_changes: 0,
                        average_gain: None,
                        average_loss: None,
                    }),
                )
            }
            MATERIAL_CROSS_ABOVE | MATERIAL_CROSS_BELOW => {
                require_none(params)?;
                require_cross(inputs)?;
                build(
                    ValueType::required(ScalarType::Bool),
                    MaterialLookback::InheritInputs { minimum: 0 },
                    Box::new(CrossEvaluator {
                        above: self.key == MATERIAL_CROSS_ABOVE,
                        previous: None,
                    }),
                )
            }
            MATERIAL_POSITION_EXISTS => position_build(params, inputs, PositionField::Exists),
            MATERIAL_POSITION_PENDING => position_build(params, inputs, PositionField::IsPending),
            MATERIAL_POSITION_OPEN => position_build(params, inputs, PositionField::IsOpen),
            MATERIAL_POSITION_ENTRY_PRICE => {
                position_build(params, inputs, PositionField::EntryPrice)
            }
            MATERIAL_POSITION_SIDE => position_build(params, inputs, PositionField::Side),
            MATERIAL_POSITION_REMAINING_SIZE => {
                position_build(params, inputs, PositionField::RemainingSize)
            }
            MATERIAL_POSITION_STOPLOSS => position_build(params, inputs, PositionField::Stoploss),
            MATERIAL_POSITION_OPENED_AT => position_build(params, inputs, PositionField::OpenedAt),
            MATERIAL_POSITION_FAVORABLE_EXCURSION => {
                position_build(params, inputs, PositionField::FavorableExcursion)
            }
            MATERIAL_POSITION_ADVERSE_EXCURSION => {
                position_build(params, inputs, PositionField::AdverseExcursion)
            }
            MATERIAL_POSITION_INITIAL_RISK => {
                position_build(params, inputs, PositionField::InitialRisk)
            }
            MATERIAL_BARS_SINCE_OPEN => {
                require_inputs(inputs, &[])?;
                let slot = slot_arg(params, "slot")?;
                crate::validate_id(&slot)?;
                let source = source_arg(params, "source")?;
                build(
                    ValueType::optional(ScalarType::Integer),
                    source_lookback(source.clone(), 1),
                    Box::new(BarsSinceOpenEvaluator {
                        slot,
                        source,
                        opened_at: None,
                        count: 0,
                    }),
                )
            }
            MATERIAL_ENTRY_FILLED => feedback_build(params, inputs, FeedbackField::EntryFilled),
            MATERIAL_ENTRY_REJECTED => feedback_build(params, inputs, FeedbackField::EntryRejected),
            MATERIAL_POSITION_CLOSED => {
                feedback_build(params, inputs, FeedbackField::PositionClosed)
            }
            MATERIAL_CANCELLATION_APPLIED => {
                feedback_build(params, inputs, FeedbackField::CancellationApplied)
            }
            MATERIAL_CANCELLATION_REJECTED => {
                feedback_build(params, inputs, FeedbackField::CancellationRejected)
            }
            _ => Err("unknown built-in material".into()),
        }
    }

    fn update_trigger(
        &self,
        params: &MaterialArgs,
        _inputs: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(match self.key {
            MATERIAL_BAR_FIELD | MATERIAL_ATR => {
                MaterialUpdateTrigger::Source(source_arg(params, "source")?)
            }
            MATERIAL_EMA | MATERIAL_SMA | MATERIAL_STDDEV | MATERIAL_ROLLING_MIN
            | MATERIAL_ROLLING_MAX | MATERIAL_LAG | MATERIAL_RSI | MATERIAL_CROSS_ABOVE
            | MATERIAL_CROSS_BELOW => MaterialUpdateTrigger::AnyInput,
            MATERIAL_ENTRY_FILLED
            | MATERIAL_ENTRY_REJECTED
            | MATERIAL_POSITION_CLOSED
            | MATERIAL_CANCELLATION_APPLIED
            | MATERIAL_CANCELLATION_REJECTED => MaterialUpdateTrigger::FeedbackPulse,
            _ => MaterialUpdateTrigger::EveryInput,
        })
    }
}

fn source_lookback(source: SourceId, required_lookback: usize) -> MaterialLookback {
    MaterialLookback::Sources(vec![CompletedBarRequirement {
        source,
        required_lookback,
    }])
}

fn checked_period(params: &MaterialArgs) -> Result<usize, String> {
    let value = integer_arg(params, "period")?;
    usize::try_from(value).map_err(|_| "period is outside the supported lookback bound".into())
}

fn integer_arg(params: &MaterialArgs, name: &str) -> Result<i64, String> {
    match params.get(name) {
        Some(MaterialArg::Integer(value)) => Ok(*value),
        _ => Err(format!("integer material argument '{name}' is required")),
    }
}

fn source_arg(params: &MaterialArgs, name: &str) -> Result<SourceId, String> {
    match params.get(name) {
        Some(MaterialArg::Source(value)) => Ok(value.clone()),
        _ => Err(format!("source material argument '{name}' is required")),
    }
}

fn slot_arg(params: &MaterialArgs, name: &str) -> Result<String, String> {
    match params.get(name) {
        Some(MaterialArg::Slot(value)) => Ok(value.clone()),
        _ => Err(format!("slot material argument '{name}' is required")),
    }
}

fn bar_field_arg(params: &MaterialArgs, name: &str) -> Result<BarField, String> {
    match params.get(name) {
        Some(MaterialArg::BarField(value)) => Ok(*value),
        _ => Err(format!("bar-field material argument '{name}' is required")),
    }
}

fn action_kind_arg(params: &MaterialArgs, name: &str) -> Result<ConfiguredActionKind, String> {
    match params.get(name) {
        Some(MaterialArg::ActionKind(value)) => Ok(*value),
        _ => Err(format!(
            "action-kind material argument '{name}' is required"
        )),
    }
}

fn require_none(params: &MaterialArgs) -> Result<(), String> {
    if params.is_empty() {
        Ok(())
    } else {
        Err("material takes no parameters".into())
    }
}

fn require_inputs(actual: &[ValueType], expected: &[ValueType]) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("expected inputs {expected:?}, got {actual:?}"))
    }
}

fn require_one_numeric(inputs: &[ValueType]) -> Result<(), String> {
    if inputs.len() == 1 && matches!(inputs[0].scalar, ScalarType::Number | ScalarType::Price) {
        Ok(())
    } else {
        Err("expected one number or price input".into())
    }
}

fn require_cross(inputs: &[ValueType]) -> Result<(), String> {
    if inputs.len() == 2
        && inputs[0].scalar == inputs[1].scalar
        && matches!(
            inputs[0].scalar,
            ScalarType::Integer | ScalarType::Number | ScalarType::Price
        )
    {
        Ok(())
    } else {
        Err("expected two inputs of the same numeric type".into())
    }
}

fn position_build(
    params: &MaterialArgs,
    inputs: &[ValueType],
    field: PositionField,
) -> Result<MaterialBuild, String> {
    require_inputs(inputs, &[])?;
    let slot = slot_arg(params, "slot")?;
    crate::validate_id(&slot)?;
    let output_type = position_field_type(field);
    Ok(MaterialBuild {
        output_type,
        lookback: MaterialLookback::None,
        max_state_bytes: crate::MAX_ID_BYTES + 64,
        evaluator: Box::new(PositionEvaluator { slot, field }),
    })
}

fn feedback_build(
    params: &MaterialArgs,
    inputs: &[ValueType],
    field: FeedbackField,
) -> Result<MaterialBuild, String> {
    require_inputs(inputs, &[])?;
    let slot = slot_arg(params, "slot")?;
    let action = action_kind_arg(params, "action")?;
    crate::validate_id(&slot)?;
    Ok(MaterialBuild {
        output_type: ValueType::required(ScalarType::Bool),
        lookback: MaterialLookback::None,
        max_state_bytes: 0,
        evaluator: Box::new(FeedbackEvaluator {
            slot,
            action,
            field,
        }),
    })
}

pub(crate) fn position_field_type(field: PositionField) -> ValueType {
    match field {
        PositionField::Exists | PositionField::IsPending | PositionField::IsOpen => {
            ValueType::required(ScalarType::Bool)
        }
        PositionField::EntryPrice | PositionField::Stoploss => {
            ValueType::optional(ScalarType::Price)
        }
        PositionField::Side => ValueType::optional(ScalarType::Side),
        PositionField::RemainingSize
        | PositionField::FavorableExcursion
        | PositionField::AdverseExcursion
        | PositionField::InitialRisk => ValueType::optional(ScalarType::Number),
        PositionField::OpenedAt => ValueType::optional(ScalarType::Timestamp),
    }
}

macro_rules! clone_eval {
    ($ty:ty) => {
        fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
            Box::new(self.clone())
        }
    };
}

#[derive(Clone)]
struct BarFieldEvaluator {
    source: SourceId,
    field: BarField,
}
impl MaterialEvaluator for BarFieldEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let update = context
            .input
            .completed_bars
            .iter()
            .find(|item| item.source == self.source)
            .ok_or_else(|| "configured bar source did not update".to_string())?;
        Ok(bar_value(&update.bar, self.field))
    }
}

#[derive(Clone)]
struct InputTimeEvaluator;
impl MaterialEvaluator for InputTimeEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        Ok(Value::Timestamp(context.input.time))
    }
}

/// A pure function of the authoritative input time, so every adapter supplies it identically.
#[derive(Clone)]
struct CalendarEvaluator {
    weekday: bool,
}
impl MaterialEvaluator for CalendarEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let time = context.input.time;
        Ok(Value::Integer(if self.weekday {
            i64::from(time.weekday().number_from_monday())
        } else {
            i64::from(time.num_seconds_from_midnight())
        }))
    }
}

#[derive(Clone)]
struct ReadinessEvaluator;
impl MaterialEvaluator for ReadinessEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        Ok(Value::Bool(context.input.ready))
    }
}

#[derive(Clone)]
struct EmaEvaluator {
    alpha: f64,
    value: Option<f64>,
    scalar: ScalarType,
}
impl MaterialEvaluator for EmaEvaluator {
    clone_eval!(Self);
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let current = numeric_value(&inputs[0])?;
        if let Some(current) = current {
            self.value = Some(self.value.map_or(current, |previous| {
                self.alpha * current + (1.0 - self.alpha) * previous
            }));
        }
        Ok(self
            .value
            .map(|value| numeric(self.scalar, value))
            .unwrap_or(Value::Missing(self.scalar)))
    }
}

#[derive(Clone, Copy)]
enum RollingKind {
    Mean,
    PopulationStdDev,
    Min,
    Max,
}

#[derive(Clone)]
struct RollingEvaluator {
    period: usize,
    values: VecDeque<f64>,
    scalar: ScalarType,
    kind: RollingKind,
}

impl MaterialEvaluator for RollingEvaluator {
    clone_eval!(Self);
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        if let Some(value) = numeric_value(&inputs[0])? {
            self.values.push_back(value);
            if self.values.len() > self.period {
                self.values.pop_front();
            }
        }
        if self.values.len() < self.period {
            return Ok(Value::Missing(self.scalar));
        }
        let value = match self.kind {
            RollingKind::Mean => self.values.iter().sum::<f64>() / self.period as f64,
            RollingKind::PopulationStdDev => {
                let mean = self.values.iter().sum::<f64>() / self.period as f64;
                (self
                    .values
                    .iter()
                    .map(|value| (value - mean).powi(2))
                    .sum::<f64>()
                    / self.period as f64)
                    .sqrt()
            }
            RollingKind::Min => self.values.iter().copied().fold(f64::INFINITY, f64::min),
            RollingKind::Max => self
                .values
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max),
        };
        Ok(numeric(self.scalar, value))
    }
}

#[derive(Clone)]
struct LagEvaluator {
    period: usize,
    values: VecDeque<f64>,
    scalar: ScalarType,
}

impl MaterialEvaluator for LagEvaluator {
    clone_eval!(Self);
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let Some(value) = numeric_value(&inputs[0])? else {
            return Ok(Value::Missing(self.scalar));
        };
        self.values.push_back(value);
        if self.values.len() <= self.period {
            return Ok(Value::Missing(self.scalar));
        }
        let lagged = self
            .values
            .pop_front()
            .expect("a lagged value is available");
        Ok(numeric(self.scalar, lagged))
    }
}

#[derive(Clone)]
struct RsiEvaluator {
    period: usize,
    previous: Option<f64>,
    seed_gains: f64,
    seed_losses: f64,
    seed_changes: usize,
    average_gain: Option<f64>,
    average_loss: Option<f64>,
}

impl MaterialEvaluator for RsiEvaluator {
    clone_eval!(Self);
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let Some(current) = numeric_value(&inputs[0])? else {
            return Ok(Value::Missing(ScalarType::Number));
        };
        let Some(previous) = self.previous.replace(current) else {
            return Ok(Value::Missing(ScalarType::Number));
        };
        let change = current - previous;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        let (average_gain, average_loss) = match (self.average_gain, self.average_loss) {
            (Some(average_gain), Some(average_loss)) => {
                let divisor = self.period as f64;
                (
                    (average_gain * (divisor - 1.0) + gain) / divisor,
                    (average_loss * (divisor - 1.0) + loss) / divisor,
                )
            }
            _ => {
                self.seed_gains += gain;
                self.seed_losses += loss;
                self.seed_changes += 1;
                if self.seed_changes < self.period {
                    return Ok(Value::Missing(ScalarType::Number));
                }
                (
                    self.seed_gains / self.period as f64,
                    self.seed_losses / self.period as f64,
                )
            }
        };
        self.average_gain = Some(average_gain);
        self.average_loss = Some(average_loss);
        let value = if average_loss == 0.0 {
            if average_gain == 0.0 { 50.0 } else { 100.0 }
        } else if average_gain == 0.0 {
            0.0
        } else {
            100.0 - 100.0 / (1.0 + average_gain / average_loss)
        };
        Ok(Value::Number(value))
    }
}

#[derive(Clone)]
struct AtrEvaluator {
    source: SourceId,
    alpha: f64,
    previous_close: Option<f64>,
    value: Option<f64>,
}
impl MaterialEvaluator for AtrEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let bar = &context
            .input
            .completed_bars
            .iter()
            .find(|item| item.source == self.source)
            .ok_or_else(|| "configured ATR source did not update".to_string())?
            .bar;
        let range = bar.high - bar.low;
        let true_range = self.previous_close.map_or(range, |close| {
            range
                .max((bar.high - close).abs())
                .max((bar.low - close).abs())
        });
        self.value = Some(self.value.map_or(true_range, |previous| {
            self.alpha * true_range + (1.0 - self.alpha) * previous
        }));
        self.previous_close = Some(bar.close);
        Ok(Value::Price(self.value.unwrap()))
    }
}

#[derive(Clone)]
struct CrossEvaluator {
    above: bool,
    previous: Option<(f64, f64)>,
}
impl MaterialEvaluator for CrossEvaluator {
    clone_eval!(Self);
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let (Some(left), Some(right)) = (numeric_value(&inputs[0])?, numeric_value(&inputs[1])?)
        else {
            return Ok(Value::Bool(false));
        };
        let crossed = self.previous.is_some_and(|(old_left, old_right)| {
            if self.above {
                old_left <= old_right && left > right
            } else {
                old_left >= old_right && left < right
            }
        });
        self.previous = Some((left, right));
        Ok(Value::Bool(crossed))
    }
}

#[derive(Clone)]
struct PositionEvaluator {
    slot: String,
    field: PositionField,
}
impl MaterialEvaluator for PositionEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let facts = context
            .input
            .trade_slots
            .iter()
            .find(|item| item.slot == self.slot)
            .ok_or_else(|| "declared trade slot facts are missing".to_string())?;
        Ok(trade_slot_value(&facts.state, self.field))
    }
}

/// Count completed bars of one source that arrive after the slot's entry fill, restarting whenever the slot holds a different position.
#[derive(Clone)]
struct BarsSinceOpenEvaluator {
    slot: String,
    source: SourceId,
    opened_at: Option<NaiveDateTime>,
    count: i64,
}
impl MaterialEvaluator for BarsSinceOpenEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let facts = context
            .input
            .trade_slots
            .iter()
            .find(|item| item.slot == self.slot)
            .ok_or_else(|| "declared trade slot facts are missing".to_string())?;
        let TradeSlotState::Open { opened_at, .. } = facts.state else {
            self.opened_at = None;
            self.count = 0;
            return Ok(Value::Missing(ScalarType::Integer));
        };
        if self.opened_at != Some(opened_at) {
            self.opened_at = Some(opened_at);
            self.count = 0;
        }
        if context.input.time > opened_at {
            let arrived = context
                .input
                .completed_bars
                .iter()
                .filter(|update| update.source == self.source)
                .count();
            self.count = self
                .count
                .checked_add(
                    i64::try_from(arrived).map_err(|_| "bar count overflowed".to_string())?,
                )
                .ok_or_else(|| "bar count overflowed".to_string())?;
        }
        Ok(Value::Integer(self.count))
    }
}

#[derive(Clone)]
struct FeedbackEvaluator {
    slot: String,
    action: ConfiguredActionKind,
    field: FeedbackField,
}
impl MaterialEvaluator for FeedbackEvaluator {
    clone_eval!(Self);
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        Ok(Value::Bool(context.visible_feedback_matches(
            &self.slot,
            self.action,
            self.field,
        )))
    }
}

pub(crate) fn bar_value(bar: &CompletedBar, field: BarField) -> Value {
    match field {
        BarField::Open => Value::Price(bar.open),
        BarField::High => Value::Price(bar.high),
        BarField::Low => Value::Price(bar.low),
        BarField::Close => Value::Price(bar.close),
        BarField::Volume => Value::Number(bar.volume),
    }
}

pub(crate) fn trade_slot_value(state: &TradeSlotState, field: PositionField) -> Value {
    match field {
        PositionField::Exists => Value::Bool(!matches!(state, TradeSlotState::Vacant)),
        PositionField::IsPending => Value::Bool(matches!(state, TradeSlotState::Pending { .. })),
        PositionField::IsOpen => Value::Bool(matches!(state, TradeSlotState::Open { .. })),
        PositionField::Side => match state {
            TradeSlotState::Pending { side, .. } | TradeSlotState::Open { side, .. } => {
                Value::Side(*side)
            }
            TradeSlotState::Vacant => Value::Missing(ScalarType::Side),
        },
        PositionField::EntryPrice => match state {
            TradeSlotState::Open { entry_price, .. } => Value::Price(*entry_price),
            _ => Value::Missing(ScalarType::Price),
        },
        PositionField::RemainingSize => match state {
            TradeSlotState::Open { remaining_size, .. } => Value::Number(*remaining_size),
            _ => Value::Missing(ScalarType::Number),
        },
        PositionField::Stoploss => match state {
            TradeSlotState::Pending { stoploss, .. } | TradeSlotState::Open { stoploss, .. } => {
                stoploss
                    .map(Value::Price)
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            TradeSlotState::Vacant => Value::Missing(ScalarType::Price),
        },
        PositionField::OpenedAt => match state {
            TradeSlotState::Open { opened_at, .. } => Value::Timestamp(*opened_at),
            _ => Value::Missing(ScalarType::Timestamp),
        },
        PositionField::FavorableExcursion => open_number(state, |facts| facts.0),
        PositionField::AdverseExcursion => open_number(state, |facts| facts.1),
        PositionField::InitialRisk => open_number(state, |facts| facts.2),
    }
}

/// Read one optional open-position economic fact as a number, missing unless the slot is open and the adapter supplied it.
fn open_number(
    state: &TradeSlotState,
    select: impl Fn((Option<f64>, Option<f64>, Option<f64>)) -> Option<f64>,
) -> Value {
    match state {
        TradeSlotState::Open {
            favorable_excursion,
            adverse_excursion,
            initial_risk,
            ..
        } => select((*favorable_excursion, *adverse_excursion, *initial_risk))
            .map(Value::Number)
            .unwrap_or(Value::Missing(ScalarType::Number)),
        _ => Value::Missing(ScalarType::Number),
    }
}

fn numeric(scalar: ScalarType, value: f64) -> Value {
    if scalar == ScalarType::Price {
        Value::Price(value)
    } else {
        Value::Number(value)
    }
}

fn numeric_value(value: &Value) -> Result<Option<f64>, String> {
    match value {
        Value::Missing(_) => Ok(None),
        Value::Integer(value) => Ok(Some(*value as f64)),
        Value::Number(value) | Value::Price(value) if value.is_finite() => Ok(Some(*value)),
        Value::Number(_) | Value::Price(_) => Err("numeric material input must be finite".into()),
        _ => Err("material input must be numeric".into()),
    }
}

pub(crate) fn material_error(id: &str, reason: String) -> EvaluationError {
    EvaluationError::Material {
        material: id.into(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> StrategyInput {
        StrategyInput {
            time: NaiveDateTime::default(),
            ready: true,
            completed_bars: vec![],
            values: vec![],
            trade_slots: vec![],
            feedback: vec![],
        }
    }

    fn evaluate_values(evaluator: &mut dyn MaterialEvaluator, values: &[f64]) -> Vec<Value> {
        let input = input();
        let context = MaterialEvalContext {
            input: &input,
            input_updates: &[true],
            feedback: &[],
            retained_feedback: &[],
        };
        values
            .iter()
            .map(|value| {
                evaluator
                    .evaluate(&[Value::Number(*value)], &context)
                    .unwrap()
            })
            .collect()
    }

    fn final_number(values: Vec<Value>) -> f64 {
        match values.last().unwrap() {
            Value::Number(value) | Value::Price(value) => *value,
            value => panic!("unexpected material output {value:?}"),
        }
    }

    #[test]
    fn ema_matches_a_known_recursive_update() {
        let mut ema = EmaEvaluator {
            alpha: 0.5,
            value: None,
            scalar: ScalarType::Number,
        };
        assert!((final_number(evaluate_values(&mut ema, &[1.0, 2.0, 3.0])) - 2.25).abs() < 1e-12);
    }

    #[test]
    fn rolling_indicators_evict_the_oldest_sample() {
        let mut sma = RollingEvaluator {
            period: 3,
            values: VecDeque::new(),
            scalar: ScalarType::Number,
            kind: RollingKind::Mean,
        };
        assert_eq!(
            final_number(evaluate_values(&mut sma, &[1.0, 2.0, 3.0, 4.0])),
            3.0
        );

        let mut stddev = RollingEvaluator {
            period: 3,
            values: VecDeque::new(),
            scalar: ScalarType::Number,
            kind: RollingKind::PopulationStdDev,
        };
        assert!(
            (final_number(evaluate_values(&mut stddev, &[1.0, 2.0, 3.0, 7.0]))
                - (14.0_f64 / 3.0).sqrt())
            .abs()
                < 1e-12
        );

        let mut minimum = RollingEvaluator {
            period: 3,
            values: VecDeque::new(),
            scalar: ScalarType::Number,
            kind: RollingKind::Min,
        };
        assert_eq!(
            final_number(evaluate_values(&mut minimum, &[0.0, 3.0, 1.0, 2.0])),
            1.0
        );

        let mut maximum = RollingEvaluator {
            period: 3,
            values: VecDeque::new(),
            scalar: ScalarType::Number,
            kind: RollingKind::Max,
        };
        assert_eq!(
            final_number(evaluate_values(&mut maximum, &[9.0, 1.0, 3.0, 2.0])),
            3.0
        );
    }

    #[test]
    fn lag_evicts_values_after_the_requested_distance() {
        let mut lag = LagEvaluator {
            period: 2,
            values: VecDeque::new(),
            scalar: ScalarType::Number,
        };
        assert_eq!(
            final_number(evaluate_values(&mut lag, &[1.0, 2.0, 3.0, 4.0])),
            2.0
        );
    }

    #[test]
    fn rsi_applies_wilder_smoothing_after_its_seed_window() {
        let mut rsi = RsiEvaluator {
            period: 2,
            previous: None,
            seed_gains: 0.0,
            seed_losses: 0.0,
            seed_changes: 0,
            average_gain: None,
            average_loss: None,
        };
        let values = evaluate_values(&mut rsi, &[1.0, 2.0, 1.0, 3.0]);
        assert_eq!(values[2], Value::Number(50.0));
        assert!((final_number(values) - 83.333_333_333_333_33).abs() < 1e-12);
        assert_eq!(rsi.average_gain, Some(1.25));
        assert_eq!(rsi.average_loss, Some(0.25));
    }

    #[test]
    fn atr_matches_a_hand_computed_wilder_update() {
        let source = SourceId::new("primary").unwrap();
        let mut atr = AtrEvaluator {
            source: source.clone(),
            alpha: 0.5,
            previous_close: None,
            value: None,
        };
        let first = StrategyInput {
            completed_bars: vec![CompletedBarUpdate {
                source: source.clone(),
                bar: CompletedBar {
                    open: 10.0,
                    high: 10.0,
                    low: 10.0,
                    close: 10.0,
                    volume: 1.0,
                },
            }],
            ..input()
        };
        let second = StrategyInput {
            completed_bars: vec![CompletedBarUpdate {
                source,
                bar: CompletedBar {
                    open: 11.0,
                    high: 13.0,
                    low: 9.0,
                    close: 12.0,
                    volume: 1.0,
                },
            }],
            ..input()
        };
        for item in [&first, &second] {
            atr.evaluate(
                &[],
                &MaterialEvalContext {
                    input: item,
                    input_updates: &[],
                    feedback: &[],
                    retained_feedback: &[],
                },
            )
            .unwrap();
        }
        assert_eq!(atr.value, Some(2.0));
    }
}
