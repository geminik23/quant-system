use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_core::Side;

use crate::{
    BarField, CompileError, ConfiguredActionKind, EvaluationError, FeedbackField, MaterialParams,
    PositionField, ScalarType, SourceId, Value, ValueType,
};

pub const MATERIAL_BAR_FIELD: &str = "completed_bar_field";
pub const MATERIAL_INPUT_TIME: &str = "input_time";
pub const MATERIAL_READINESS: &str = "readiness";
pub const MATERIAL_EMA: &str = "ema";
pub const MATERIAL_ATR: &str = "atr";
pub const MATERIAL_CROSS_ABOVE: &str = "cross_above";
pub const MATERIAL_CROSS_BELOW: &str = "cross_below";
pub const MATERIAL_POSITION_EXISTS: &str = "position_exists";
pub const MATERIAL_POSITION_PENDING: &str = "position_pending";
pub const MATERIAL_POSITION_OPEN: &str = "position_open";
pub const MATERIAL_POSITION_ENTRY_PRICE: &str = "position_entry_price";
pub const MATERIAL_POSITION_SIDE: &str = "position_side";
pub const MATERIAL_POSITION_REMAINING_SIZE: &str = "position_remaining_size";
pub const MATERIAL_POSITION_STOPLOSS: &str = "position_stoploss";
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
    AllInputs,
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
    fn build(
        &self,
        params: &MaterialParams,
        input_types: &[ValueType],
    ) -> Result<MaterialBuild, String>;

    fn update_trigger(
        &self,
        _params: &MaterialParams,
        _input_types: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::EveryInput)
    }
}

#[derive(Clone)]
struct Registration {
    key: String,
    factory: Arc<dyn MaterialFactory>,
    custom: bool,
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
            MATERIAL_EMA,
            MATERIAL_ATR,
            MATERIAL_CROSS_ABOVE,
            MATERIAL_CROSS_BELOW,
            MATERIAL_POSITION_EXISTS,
            MATERIAL_POSITION_PENDING,
            MATERIAL_POSITION_OPEN,
            MATERIAL_POSITION_ENTRY_PRICE,
            MATERIAL_POSITION_SIDE,
            MATERIAL_POSITION_REMAINING_SIZE,
            MATERIAL_POSITION_STOPLOSS,
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
                    custom: false,
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
        self.registrations.push(Registration {
            key,
            factory,
            custom: true,
        });
        Ok(self)
    }

    pub(crate) fn factory(&self, key: &str) -> Option<&Arc<dyn MaterialFactory>> {
        self.registration(key).map(|item| &item.factory)
    }

    pub(crate) fn is_custom(&self, key: &str) -> bool {
        self.registration(key).is_some_and(|item| item.custom)
    }

    fn registration(&self, key: &str) -> Option<&Registration> {
        self.registrations.iter().find(|item| item.key == key)
    }
}

struct BuiltinFactory {
    key: &'static str,
}

impl MaterialFactory for BuiltinFactory {
    fn build(
        &self,
        params: &MaterialParams,
        inputs: &[ValueType],
    ) -> Result<MaterialBuild, String> {
        let state_bytes = match self.key {
            MATERIAL_EMA => 32,
            MATERIAL_ATR | MATERIAL_CROSS_ABOVE | MATERIAL_CROSS_BELOW => 48,
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
                let MaterialParams::BarField { source, field } = params else {
                    return Err("bar field parameters are required".into());
                };
                build(
                    crate::bar_field_type(*field),
                    source_lookback(source.clone(), 1),
                    Box::new(BarFieldEvaluator {
                        source: source.clone(),
                        field: *field,
                    }),
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
                let MaterialParams::Ema { period } = params else {
                    return Err("EMA parameters are required".into());
                };
                let period = checked_period(*period)?;
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
                let MaterialParams::Atr { source, period } = params else {
                    return Err("ATR parameters are required".into());
                };
                let period = checked_period(*period)?;
                require_inputs(inputs, &[])?;
                build(
                    ValueType::optional(ScalarType::Price),
                    source_lookback(source.clone(), period + 1),
                    Box::new(AtrEvaluator {
                        source: source.clone(),
                        alpha: 1.0 / period as f64,
                        previous_close: None,
                        value: None,
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
        params: &MaterialParams,
        _inputs: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(match (self.key, params) {
            (MATERIAL_BAR_FIELD, MaterialParams::BarField { source, .. })
            | (MATERIAL_ATR, MaterialParams::Atr { source, .. }) => {
                MaterialUpdateTrigger::Source(source.clone())
            }
            (MATERIAL_EMA | MATERIAL_CROSS_ABOVE | MATERIAL_CROSS_BELOW, _) => {
                MaterialUpdateTrigger::AllInputs
            }
            (
                MATERIAL_ENTRY_FILLED
                | MATERIAL_ENTRY_REJECTED
                | MATERIAL_POSITION_CLOSED
                | MATERIAL_CANCELLATION_APPLIED
                | MATERIAL_CANCELLATION_REJECTED,
                _,
            ) => MaterialUpdateTrigger::FeedbackPulse,
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

fn checked_period(period: u16) -> Result<usize, String> {
    let period = usize::from(period);
    if period == 0 || period > crate::MAX_MATERIAL_LOOKBACK {
        Err("period is outside the supported lookback bound".into())
    } else {
        Ok(period)
    }
}

fn require_none(params: &MaterialParams) -> Result<(), String> {
    if matches!(params, MaterialParams::None) {
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
    params: &MaterialParams,
    inputs: &[ValueType],
    field: PositionField,
) -> Result<MaterialBuild, String> {
    require_inputs(inputs, &[])?;
    let MaterialParams::Position { slot } = params else {
        return Err("position parameters are required".into());
    };
    crate::validate_id(slot)?;
    let output_type = position_field_type(field);
    Ok(MaterialBuild {
        output_type,
        lookback: MaterialLookback::None,
        max_state_bytes: crate::MAX_ID_BYTES + 64,
        evaluator: Box::new(PositionEvaluator {
            slot: slot.clone(),
            field,
        }),
    })
}

fn feedback_build(
    params: &MaterialParams,
    inputs: &[ValueType],
    field: FeedbackField,
) -> Result<MaterialBuild, String> {
    require_inputs(inputs, &[])?;
    let MaterialParams::Feedback { slot, action } = params else {
        return Err("feedback parameters are required".into());
    };
    crate::validate_id(slot)?;
    Ok(MaterialBuild {
        output_type: ValueType::required(ScalarType::Bool),
        lookback: MaterialLookback::None,
        max_state_bytes: 0,
        evaluator: Box::new(FeedbackEvaluator {
            slot: slot.clone(),
            action: *action,
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
        PositionField::RemainingSize => ValueType::optional(ScalarType::Number),
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
