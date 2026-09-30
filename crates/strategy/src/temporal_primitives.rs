use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    CompletedBarRequirement, MaterialArg, MaterialArgs, ScalarType, SourceId, Value, ValueType,
};
use std::collections::VecDeque;
use std::sync::Arc;
pub const MATERIAL_STRICT_CROSS_ABOVE: &str = "strict_cross_above";
pub const MATERIAL_STRICT_CROSS_BELOW: &str = "strict_cross_below";
pub const MATERIAL_DELTA: &str = "delta";
pub const MATERIAL_RISE: &str = "rise";
pub const MATERIAL_FALL: &str = "fall";
pub const MATERIAL_HOLD_ALL: &str = "hold_all";
pub const MATERIAL_COUNT_TRUE: &str = "count_true";
pub const MATERIAL_SHARE_TRUE: &str = "share_true";
pub const MATERIAL_STREAK_TRUE: &str = "streak_true";
pub const MATERIAL_BARS_SINCE_TRUE: &str = "bars_since_true";
pub const MATERIAL_SEQUENCE_AB: &str = "sequence_ab";
pub const MATERIAL_SETUP_LONG_SINGLE_KEEP_FIRST: &str = "setup_long_single_keep_first";
pub const MATERIAL_SETUP_LONG_REPLACE_LATEST: &str = "setup_long_replace_latest";
pub const MATERIAL_SETUP_LONG_BOUNDED_QUEUE: &str = "setup_long_bounded_queue";
pub const MATERIAL_SETUP_SHORT_SINGLE_KEEP_FIRST: &str = "setup_short_single_keep_first";
pub const MATERIAL_SETUP_SHORT_REPLACE_LATEST: &str = "setup_short_replace_latest";
pub const MATERIAL_SETUP_SHORT_BOUNDED_QUEUE: &str = "setup_short_bounded_queue";
const KEYS: &[&str] = &[
    MATERIAL_STRICT_CROSS_ABOVE,
    MATERIAL_STRICT_CROSS_BELOW,
    MATERIAL_DELTA,
    MATERIAL_RISE,
    MATERIAL_FALL,
    MATERIAL_HOLD_ALL,
    MATERIAL_COUNT_TRUE,
    MATERIAL_SHARE_TRUE,
    MATERIAL_STREAK_TRUE,
    MATERIAL_BARS_SINCE_TRUE,
    MATERIAL_SEQUENCE_AB,
    MATERIAL_SETUP_LONG_SINGLE_KEEP_FIRST,
    MATERIAL_SETUP_LONG_REPLACE_LATEST,
    MATERIAL_SETUP_LONG_BOUNDED_QUEUE,
    MATERIAL_SETUP_SHORT_SINGLE_KEEP_FIRST,
    MATERIAL_SETUP_SHORT_REPLACE_LATEST,
    MATERIAL_SETUP_SHORT_BOUNDED_QUEUE,
];
const SOURCE: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
const SOURCE_PERIOD: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer { min: 1, max: 1024 },
        required: true,
    },
];
const SOURCE_GAP: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "max_gap",
        kind: ParamKind::Integer { min: 1, max: 4096 },
        required: true,
    },
];
const SOURCE_EXPIRY: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "expiry",
        kind: ParamKind::Integer { min: 1, max: 4096 },
        required: true,
    },
];
const SOURCE_EXPIRY_CAPACITY: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "expiry",
        kind: ParamKind::Integer { min: 1, max: 4096 },
        required: true,
    },
    ParamSpec {
        name: "capacity",
        kind: ParamKind::Integer { min: 1, max: 8 },
        required: true,
    },
];
pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(TemporalFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}
struct TemporalFactory {
    key: &'static str,
}
impl TemporalFactory {
    fn source(p: &MaterialArgs) -> Result<SourceId, String> {
        match p.get("source") {
            Some(MaterialArg::Source(v)) => Ok(v.clone()),
            _ => Err("temporal primitive requires source".into()),
        }
    }
    fn int(p: &MaterialArgs, n: &str, d: Option<usize>) -> Result<usize, String> {
        match p.get(n) {
            Some(MaterialArg::Integer(v)) => usize::try_from(*v)
                .ok()
                .ok_or_else(|| format!("invalid {n}")),
            None => d.ok_or_else(|| format!("missing {n}")),
            _ => Err(format!("invalid {n}")),
        }
    }
    fn setup(&self) -> bool {
        self.key.starts_with("setup_")
    }
}
impl MaterialFactory for TemporalFactory {
    fn params(&self) -> &[ParamSpec] {
        if matches!(
            self.key,
            MATERIAL_DELTA
                | MATERIAL_RISE
                | MATERIAL_FALL
                | MATERIAL_HOLD_ALL
                | MATERIAL_COUNT_TRUE
                | MATERIAL_SHARE_TRUE
        ) {
            &SOURCE_PERIOD
        } else if self.key == MATERIAL_SEQUENCE_AB {
            &SOURCE_GAP
        } else if self.setup() {
            if self.key.ends_with("bounded_queue") {
                &SOURCE_EXPIRY_CAPACITY
            } else {
                &SOURCE_EXPIRY
            }
        } else {
            &SOURCE
        }
    }
    fn build(&self, p: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let source = Self::source(p)?;
        let (evaluator, output, lookback, state): (
            Box<dyn MaterialEvaluator>,
            ValueType,
            usize,
            usize,
        ) = match self.key {
            MATERIAL_STRICT_CROSS_ABOVE | MATERIAL_STRICT_CROSS_BELOW => {
                if inputs.len() != 2 || inputs[0].scalar != inputs[1].scalar {
                    return Err("strict cross requires two matching inputs".into());
                }
                (
                    Box::new(Cross {
                        above: self.key == MATERIAL_STRICT_CROSS_ABOVE,
                        previous: None,
                    }),
                    ValueType::optional(ScalarType::Bool),
                    2,
                    std::mem::size_of::<Cross>(),
                )
            }
            MATERIAL_DELTA => {
                one_float(inputs)?;
                let n = Self::int(p, "period", None)?;
                (
                    Box::new(Delta {
                        horizon: n,
                        values: Window::new(n + 1)?,
                    }),
                    ValueType::optional(inputs[0].scalar),
                    n + 1,
                    Window::bytes(n + 1)? + 64,
                )
            }
            MATERIAL_RISE | MATERIAL_FALL => {
                one_float(inputs)?;
                let n = Self::int(p, "period", None)?;
                (
                    Box::new(RiseFall {
                        rise: self.key == MATERIAL_RISE,
                        values: Window::new(n + 1)?,
                    }),
                    ValueType::optional(ScalarType::Bool),
                    n + 1,
                    Window::bytes(n + 1)? + 64,
                )
            }
            MATERIAL_HOLD_ALL | MATERIAL_COUNT_TRUE | MATERIAL_SHARE_TRUE => {
                one_bool(inputs)?;
                let n = Self::int(p, "period", None)?;
                let output = if self.key == MATERIAL_HOLD_ALL {
                    ValueType::optional(ScalarType::Bool)
                } else if self.key == MATERIAL_COUNT_TRUE {
                    ValueType::optional(ScalarType::Integer)
                } else {
                    ValueType::optional(ScalarType::Ratio)
                };
                (
                    Box::new(BoolWindow {
                        kind: self.key,
                        values: VecDeque::with_capacity(n),
                        period: n,
                    }),
                    output,
                    n,
                    n + 64,
                )
            }
            MATERIAL_STREAK_TRUE => {
                one_bool(inputs)?;
                (
                    Box::new(Streak { count: 0 }),
                    ValueType::optional(ScalarType::Integer),
                    1,
                    64,
                )
            }
            MATERIAL_BARS_SINCE_TRUE => {
                one_bool(inputs)?;
                (
                    Box::new(BarsSince { count: None }),
                    ValueType::optional(ScalarType::Integer),
                    1,
                    64,
                )
            }
            MATERIAL_SEQUENCE_AB => {
                if inputs
                    != [
                        ValueType::optional(ScalarType::Bool),
                        ValueType::optional(ScalarType::Bool),
                    ]
                    && inputs
                        != [
                            ValueType::required(ScalarType::Bool),
                            ValueType::required(ScalarType::Bool),
                        ]
                {
                    return Err("sequence requires two Bool inputs".into());
                }
                let gap = Self::int(p, "max_gap", None)?;
                (
                    Box::new(Sequence {
                        gap,
                        first_at: None,
                        ordinal: 0,
                    }),
                    ValueType::optional(ScalarType::Bool),
                    1,
                    64,
                )
            }
            _ if self.setup() => {
                if inputs.len() != 9 {
                    return Err("setup requires reset, gap, breakout, retest, level, normalization, close, tolerance and ordinal".into());
                }
                let expected = [
                    ScalarType::Bool,
                    ScalarType::Bool,
                    ScalarType::Bool,
                    ScalarType::Bool,
                    ScalarType::Price,
                    ScalarType::Number,
                    ScalarType::Price,
                    ScalarType::Price,
                    ScalarType::Integer,
                ];
                if inputs.iter().zip(expected).any(|(v, s)| v.scalar != s) {
                    return Err("setup input types are incompatible".into());
                }
                let expiry = Self::int(p, "expiry", None)?;
                let capacity = Self::int(p, "capacity", Some(1))?;
                let mode = if self.key.ends_with("replace_latest") {
                    SetupMode::ReplaceLatest
                } else if self.key.ends_with("bounded_queue") {
                    SetupMode::BoundedQueue
                } else {
                    SetupMode::SingleKeepFirst
                };
                (
                    Box::new(SetupEvaluator {
                        long: self.key.starts_with("setup_long"),
                        mode,
                        expiry: u64::try_from(expiry).unwrap(),
                        capacity,
                        setups: VecDeque::with_capacity(capacity),
                        next_setup: 0,
                        accepted_captures: 0,
                        rejected_captures: 0,
                    }),
                    ValueType::optional(ScalarType::Bool),
                    1,
                    capacity * std::mem::size_of::<Setup>() + 128,
                )
            }
            _ => return Err("unknown temporal primitive".into()),
        };
        Ok(MaterialBuild {
            output_type: output,
            lookback: MaterialLookback::Sources(vec![CompletedBarRequirement {
                source,
                required_lookback: lookback,
            }]),
            max_state_bytes: state,
            evaluator: Box::new(TemporalClock { evaluator }),
        })
    }
    fn update_trigger(
        &self,
        p: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::Source(Self::source(p)?))
    }
}
#[derive(Clone)]
struct TemporalClock {
    evaluator: Box<dyn MaterialEvaluator>,
}
impl MaterialEvaluator for TemporalClock {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        inputs: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let observed = inputs
            .iter()
            .enumerate()
            .map(|(index, value)| {
                if context.input_updates.get(index) == Some(&false) {
                    Value::Missing(value.scalar_type())
                } else {
                    value.clone()
                }
            })
            .collect::<Vec<_>>();
        self.evaluator.evaluate(&observed, context)
    }
}

fn one_float(inputs: &[ValueType]) -> Result<(), String> {
    if inputs.len() == 1
        && matches!(
            inputs[0].scalar,
            ScalarType::Number
                | ScalarType::Price
                | ScalarType::Ratio
                | ScalarType::Percent
                | ScalarType::PricePerObservation
                | ScalarType::PricePerObservationSquared
                | ScalarType::RatioPerObservation
                | ScalarType::RatioPerObservationSquared
                | ScalarType::LogReturn
                | ScalarType::LogReturnVariance
        )
    {
        Ok(())
    } else {
        Err("temporal calculation requires one floating scalar".into())
    }
}
fn one_bool(inputs: &[ValueType]) -> Result<(), String> {
    if inputs.len() == 1 && inputs[0].scalar == ScalarType::Bool {
        Ok(())
    } else {
        Err("temporal calculation requires one Bool".into())
    }
}
fn value(v: &Value) -> Option<f64> {
    match v {
        Value::Number(v)
        | Value::Price(v)
        | Value::Ratio(v)
        | Value::Percent(v)
        | Value::PricePerObservation(v)
        | Value::PricePerObservationSquared(v)
        | Value::RatioPerObservation(v)
        | Value::RatioPerObservationSquared(v)
        | Value::LogReturn(v)
        | Value::LogReturnVariance(v) => Some(*v),
        _ => None,
    }
}
fn typed(s: ScalarType, v: f64) -> Value {
    match s {
        ScalarType::Number => Value::Number(v),
        ScalarType::Price => Value::Price(v),
        ScalarType::Ratio => Value::Ratio(v),
        ScalarType::Percent => Value::Percent(v),
        ScalarType::PricePerObservation => Value::PricePerObservation(v),
        ScalarType::PricePerObservationSquared => Value::PricePerObservationSquared(v),
        ScalarType::RatioPerObservation => Value::RatioPerObservation(v),
        ScalarType::RatioPerObservationSquared => Value::RatioPerObservationSquared(v),
        ScalarType::LogReturn => Value::LogReturn(v),
        ScalarType::LogReturnVariance => Value::LogReturnVariance(v),
        _ => unreachable!(),
    }
}
#[derive(Clone)]
struct Cross {
    above: bool,
    previous: Option<(f64, f64)>,
}
impl MaterialEvaluator for Cross {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let (Some(a), Some(b)) = (value(&inputs[0]), value(&inputs[1])) else {
            self.previous = None;
            return Ok(Value::Missing(ScalarType::Bool));
        };
        let result = self.previous.map(|(pa, pb)| {
            if self.above {
                pa <= pb && a > b
            } else {
                pa >= pb && a < b
            }
        });
        self.previous = Some((a, b));
        Ok(result
            .map(Value::Bool)
            .unwrap_or(Value::Missing(ScalarType::Bool)))
    }
}
#[derive(Clone)]
struct Window {
    slots: Box<[Option<f64>]>,
    next: usize,
    len: usize,
}
impl Window {
    fn new(n: usize) -> Result<Self, String> {
        Self::bytes(n)?;
        Ok(Self {
            slots: vec![None; n].into_boxed_slice(),
            next: 0,
            len: 0,
        })
    }
    fn bytes(n: usize) -> Result<usize, String> {
        n.checked_mul(std::mem::size_of::<Option<f64>>())
            .ok_or_else(|| "temporal window overflow".into())
    }
    fn push(&mut self, v: Option<f64>) {
        self.slots[self.next] = v;
        self.next = (self.next + 1) % self.slots.len();
        self.len = (self.len + 1).min(self.slots.len())
    }
    fn values(&self) -> Option<Vec<f64>> {
        if self.len < self.slots.len() || self.slots.iter().any(Option::is_none) {
            None
        } else {
            Some(
                (0..self.len)
                    .rev()
                    .map(|a| {
                        self.slots[(self.next + self.slots.len() - 1 - a) % self.slots.len()]
                            .unwrap()
                    })
                    .collect(),
            )
        }
    }
}
#[derive(Clone)]
struct Delta {
    horizon: usize,
    values: Window,
}
impl MaterialEvaluator for Delta {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        self.values.push(value(&inputs[0]));
        let Some(v) = self.values.values() else {
            return Ok(Value::Missing(inputs[0].scalar_type()));
        };
        Ok(typed(inputs[0].scalar_type(), v[self.horizon] - v[0]))
    }
}
#[derive(Clone)]
struct RiseFall {
    rise: bool,
    values: Window,
}
impl MaterialEvaluator for RiseFall {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        self.values.push(value(&inputs[0]));
        let Some(v) = self.values.values() else {
            return Ok(Value::Missing(ScalarType::Bool));
        };
        Ok(Value::Bool(v.windows(2).all(|w| {
            if self.rise { w[1] > w[0] } else { w[1] < w[0] }
        })))
    }
}
#[derive(Clone)]
struct BoolWindow {
    kind: &'static str,
    values: VecDeque<Option<bool>>,
    period: usize,
}
impl MaterialEvaluator for BoolWindow {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let v = match inputs[0] {
            Value::Bool(v) => Some(v),
            Value::Missing(ScalarType::Bool) => None,
            _ => return Err("Bool temporal input mismatch".into()),
        };
        if self.values.len() == self.period {
            self.values.pop_front();
        }
        self.values.push_back(v);
        if self.values.len() < self.period || self.values.iter().any(Option::is_none) {
            return Ok(Value::Missing(if self.kind == MATERIAL_HOLD_ALL {
                ScalarType::Bool
            } else if self.kind == MATERIAL_COUNT_TRUE {
                ScalarType::Integer
            } else {
                ScalarType::Ratio
            }));
        }
        let count = self.values.iter().filter(|v| **v == Some(true)).count();
        Ok(if self.kind == MATERIAL_HOLD_ALL {
            Value::Bool(count == self.period)
        } else if self.kind == MATERIAL_COUNT_TRUE {
            Value::Integer(count as i64)
        } else {
            Value::Ratio(count as f64 / self.period as f64)
        })
    }
}
#[derive(Clone)]
struct Streak {
    count: u64,
}
impl MaterialEvaluator for Streak {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        match inputs[0] {
            Value::Bool(true) => {
                self.count = self
                    .count
                    .checked_add(1)
                    .ok_or_else(|| "streak overflow".to_string())?
            }
            Value::Bool(false) => self.count = 0,
            Value::Missing(ScalarType::Bool) => {
                self.count = 0;
                return Ok(Value::Missing(ScalarType::Integer));
            }
            _ => return Err("streak input mismatch".into()),
        }
        Ok(Value::Integer(
            i64::try_from(self.count).map_err(|_| "streak exceeds i64")?,
        ))
    }
}
#[derive(Clone)]
struct BarsSince {
    count: Option<u64>,
}
impl MaterialEvaluator for BarsSince {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        match inputs[0] {
            Value::Bool(true) => self.count = Some(0),
            Value::Bool(false) => {
                if let Some(v) = self.count {
                    self.count = Some(
                        v.checked_add(1)
                            .ok_or_else(|| "bars-since overflow".to_string())?,
                    )
                }
            }
            Value::Missing(ScalarType::Bool) => return Ok(Value::Missing(ScalarType::Integer)),
            _ => return Err("bars-since input mismatch".into()),
        }
        Ok(self
            .count
            .map(|v| Value::Integer(i64::try_from(v).unwrap()))
            .unwrap_or(Value::Missing(ScalarType::Integer)))
    }
}
#[derive(Clone)]
struct Sequence {
    gap: usize,
    first_at: Option<u64>,
    ordinal: u64,
}
impl MaterialEvaluator for Sequence {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| "sequence ordinal overflow".to_string())?;
        let (a, b) = match (&inputs[0], &inputs[1]) {
            (Value::Bool(a), Value::Bool(b)) => (*a, *b),
            (Value::Missing(_), _) | (_, Value::Missing(_)) => {
                self.first_at = None;
                return Ok(Value::Missing(ScalarType::Bool));
            }
            _ => return Err("sequence input mismatch".into()),
        };
        if self
            .first_at
            .is_some_and(|start| self.ordinal - start > self.gap as u64)
        {
            self.first_at = None
        }
        let fired = b
            && self.first_at.is_some_and(|start| {
                self.ordinal > start && self.ordinal - start <= self.gap as u64
            });
        if fired {
            self.first_at = None
        } else if a {
            self.first_at = Some(self.ordinal)
        }
        Ok(Value::Bool(fired))
    }
}
#[derive(Clone, Copy)]
enum SetupMode {
    SingleKeepFirst,
    ReplaceLatest,
    BoundedQueue,
}
#[derive(Clone)]
struct Setup {
    created: u64,
    expires_at: u64,
    ordinal: u64,
    level: f64,
    normalization: f64,
    tolerance: f64,
}
#[derive(Clone)]
struct SetupEvaluator {
    long: bool,
    mode: SetupMode,
    expiry: u64,
    capacity: usize,
    setups: VecDeque<Setup>,
    next_setup: u64,
    accepted_captures: u64,
    rejected_captures: u64,
}
impl MaterialEvaluator for SetupEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let boolean = |i| match inputs[i] {
            Value::Bool(v) => Ok(v),
            Value::Missing(ScalarType::Bool) => Err("missing setup Bool"),
            _ => Err("setup Bool mismatch"),
        };
        let (reset, gap, breakout, retest) = (boolean(0)?, boolean(1)?, boolean(2)?, boolean(3)?);
        let price = |i| match inputs[i] {
            Value::Price(v) if v.is_finite() => Ok(v),
            _ => Err("setup Price mismatch"),
        };
        let level = price(4)?;
        let normalization = match inputs[5] {
            Value::Number(v) if v.is_finite() => v,
            _ => return Err("setup normalization mismatch".into()),
        };
        let close = price(6)?;
        let tolerance = price(7)?;
        if tolerance < 0.0 {
            return Err("setup tolerance must be nonnegative".into());
        }
        let ordinal = match inputs[8] {
            Value::Integer(v) if v >= 0 => v as u64,
            _ => return Err("setup ordinal must be nonnegative".into()),
        };
        if reset || gap {
            self.setups.clear()
        }
        self.setups.retain(|setup| ordinal <= setup.expires_at);
        self.setups.retain(|s| {
            if self.long {
                close >= s.level - s.tolerance
            } else {
                close <= s.level + s.tolerance
            }
        });
        let fired = if retest {
            let index = self
                .setups
                .iter()
                .enumerate()
                .filter(|(_, setup)| setup.created < ordinal)
                .min_by_key(|(_, setup)| (setup.created, setup.ordinal))
                .map(|(index, _)| index);
            if let Some(index) = index {
                let captured = self.setups.remove(index).unwrap();
                debug_assert!(captured.normalization.is_finite());
                true
            } else {
                false
            }
        } else {
            false
        };
        if breakout {
            let expires_at = ordinal
                .checked_add(self.expiry)
                .ok_or_else(|| "setup expiry ordinal overflow".to_string())?;
            let setup = Setup {
                created: ordinal,
                expires_at,
                ordinal: self.next_setup,
                level,
                normalization,
                tolerance,
            };
            self.next_setup = self
                .next_setup
                .checked_add(1)
                .ok_or_else(|| "setup ordinal overflow".to_string())?;
            let accepted = match self.mode {
                SetupMode::SingleKeepFirst => {
                    if self.setups.is_empty() {
                        self.setups.push_back(setup);
                        true
                    } else {
                        false
                    }
                }
                SetupMode::ReplaceLatest => {
                    self.setups.clear();
                    self.setups.push_back(setup);
                    true
                }
                SetupMode::BoundedQueue => {
                    if self.setups.len() < self.capacity {
                        self.setups.push_back(setup);
                        true
                    } else {
                        false
                    }
                }
            };
            if accepted {
                self.accepted_captures = self
                    .accepted_captures
                    .checked_add(1)
                    .ok_or_else(|| "accepted setup count overflow".to_string())?;
            } else {
                self.rejected_captures = self
                    .rejected_captures
                    .checked_add(1)
                    .ok_or_else(|| "rejected setup count overflow".to_string())?;
            }
        }
        Ok(Value::Bool(fired))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StrategyInput;
    use chrono::NaiveDateTime;
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
    fn context<'a>(input: &'a StrategyInput, updates: &'a [bool]) -> MaterialEvalContext<'a> {
        MaterialEvalContext {
            input,
            input_updates: updates,
            any_input_updates: updates,
            feedback: &[],
            retained_feedback: &[],
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn setup_inputs(
        reset: bool,
        gap: bool,
        breakout: bool,
        retest: bool,
        level: f64,
        close: f64,
        tolerance: f64,
        ordinal: i64,
    ) -> Vec<Value> {
        vec![
            Value::Bool(reset),
            Value::Bool(gap),
            Value::Bool(breakout),
            Value::Bool(retest),
            Value::Price(level),
            Value::Number(2.0),
            Value::Price(close),
            Value::Price(tolerance),
            Value::Integer(ordinal),
        ]
    }
    #[test]
    fn strict_cross_delta_and_window_operators_keep_invalidity() {
        let i = input();
        let c = context(&i, &[true, true]);
        let mut cross = Cross {
            above: true,
            previous: None,
        };
        assert_eq!(
            cross
                .evaluate(&[Value::Price(1.0), Value::Price(2.0)], &c)
                .unwrap(),
            Value::Missing(ScalarType::Bool)
        );
        assert_eq!(
            cross
                .evaluate(&[Value::Price(3.0), Value::Price(2.0)], &c)
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            cross
                .evaluate(&[Value::Missing(ScalarType::Price), Value::Price(2.0)], &c)
                .unwrap(),
            Value::Missing(ScalarType::Bool)
        );
        assert_eq!(
            cross
                .evaluate(&[Value::Price(3.0), Value::Price(2.0)], &c)
                .unwrap(),
            Value::Missing(ScalarType::Bool)
        );
        let mut delta = Delta {
            horizon: 2,
            values: Window::new(3).unwrap(),
        };
        for (v, e) in [
            (1.0, Value::Missing(ScalarType::Price)),
            (4.0, Value::Missing(ScalarType::Price)),
            (7.0, Value::Price(6.0)),
        ] {
            assert_eq!(
                delta
                    .evaluate(&[Value::Price(v)], &context(&i, &[true]))
                    .unwrap(),
                e
            )
        }
        let mut hold = BoolWindow {
            kind: MATERIAL_SHARE_TRUE,
            values: VecDeque::with_capacity(3),
            period: 3,
        };
        for (v, e) in [
            (true, Value::Missing(ScalarType::Ratio)),
            (false, Value::Missing(ScalarType::Ratio)),
            (true, Value::Ratio(2.0 / 3.0)),
        ] {
            assert_eq!(
                hold.evaluate(&[Value::Bool(v)], &context(&i, &[true]))
                    .unwrap(),
                e
            )
        }
    }
    #[test]
    fn sequence_requires_order_and_bounded_gap() {
        let i = input();
        let c = context(&i, &[true, true]);
        let mut sequence = Sequence {
            gap: 2,
            first_at: None,
            ordinal: 0,
        };
        assert_eq!(
            sequence
                .evaluate(&[Value::Bool(false), Value::Bool(true)], &c)
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            sequence
                .evaluate(&[Value::Bool(true), Value::Bool(false)], &c)
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            sequence
                .evaluate(&[Value::Bool(false), Value::Bool(false)], &c)
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            sequence
                .evaluate(&[Value::Bool(false), Value::Bool(true)], &c)
                .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            sequence
                .evaluate(&[Value::Missing(ScalarType::Bool), Value::Bool(false)], &c)
                .unwrap(),
            Value::Missing(ScalarType::Bool)
        );
    }
    #[test]
    fn setup_order_forbids_same_bar_retest_and_honors_inclusive_expiry() {
        let i = input();
        let c = context(&i, &[true; 9]);
        let mut setup = SetupEvaluator {
            long: true,
            mode: SetupMode::SingleKeepFirst,
            expiry: 2,
            capacity: 1,
            setups: VecDeque::with_capacity(1),
            next_setup: 0,
            accepted_captures: 0,
            rejected_captures: 0,
        };
        assert_eq!(
            setup
                .evaluate(
                    &setup_inputs(false, false, true, true, 10.0, 10.0, 0.5, 10),
                    &c
                )
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(setup.setups.len(), 1);
        assert_eq!(
            setup
                .evaluate(
                    &setup_inputs(false, false, false, true, 99.0, 10.0, 0.0, 11),
                    &c
                )
                .unwrap(),
            Value::Bool(true)
        );
        assert!(setup.setups.is_empty());
        setup
            .evaluate(
                &setup_inputs(false, false, true, false, 10.0, 10.0, 0.5, 20),
                &c,
            )
            .unwrap();
        assert_eq!(
            setup
                .evaluate(
                    &setup_inputs(false, false, false, false, 0.0, 9.5, 0.0, 21),
                    &c
                )
                .unwrap(),
            Value::Bool(false)
        );
        assert_eq!(setup.setups.len(), 1, "equality must not invalidate");
        assert_eq!(
            setup
                .evaluate(
                    &setup_inputs(false, false, false, true, 0.0, 10.0, 0.0, 22),
                    &c
                )
                .unwrap(),
            Value::Bool(true)
        );
        setup
            .evaluate(
                &setup_inputs(false, false, true, false, 10.0, 10.0, 0.5, 30),
                &c,
            )
            .unwrap();
        setup
            .evaluate(
                &setup_inputs(false, false, false, false, 0.0, 10.0, 0.0, 33),
                &c,
            )
            .unwrap();
        assert!(setup.setups.is_empty(), "b+k+1 expires before evaluation");
    }
    #[test]
    fn setup_modes_queue_capacity_invalidation_and_explicit_resets_are_deterministic() {
        let i = input();
        let c = context(&i, &[true; 9]);
        let mut queue = SetupEvaluator {
            long: false,
            mode: SetupMode::BoundedQueue,
            expiry: 8,
            capacity: 2,
            setups: VecDeque::with_capacity(2),
            next_setup: 0,
            accepted_captures: 0,
            rejected_captures: 0,
        };
        for ordinal in 1..=3 {
            queue
                .evaluate(
                    &setup_inputs(
                        false,
                        false,
                        true,
                        false,
                        10.0 + ordinal as f64,
                        10.0,
                        0.0,
                        ordinal,
                    ),
                    &c,
                )
                .unwrap();
        }
        assert_eq!(
            queue.setups.iter().map(|s| s.created).collect::<Vec<_>>(),
            vec![1, 2],
            "full queue rejects without eviction"
        );
        assert_eq!(queue.accepted_captures, 2);
        assert_eq!(queue.rejected_captures, 1);
        queue
            .evaluate(
                &setup_inputs(false, false, false, false, 0.0, 11.5, 0.0, 4),
                &c,
            )
            .unwrap();
        assert_eq!(
            queue.setups.len(),
            1,
            "short invalidation is strict above level+tolerance"
        );
        queue
            .evaluate(
                &setup_inputs(false, true, false, false, 0.0, 0.0, 0.0, 5),
                &c,
            )
            .unwrap();
        assert!(queue.setups.is_empty());
        let mut replace = SetupEvaluator {
            long: true,
            mode: SetupMode::ReplaceLatest,
            expiry: 8,
            capacity: 1,
            setups: VecDeque::new(),
            next_setup: 0,
            accepted_captures: 0,
            rejected_captures: 0,
        };
        replace
            .evaluate(
                &setup_inputs(false, false, true, false, 10.0, 10.0, 0.0, 1),
                &c,
            )
            .unwrap();
        replace
            .evaluate(
                &setup_inputs(false, false, true, false, 12.0, 12.0, 0.0, 2),
                &c,
            )
            .unwrap();
        assert_eq!(replace.setups[0].level, 12.0);
        replace
            .evaluate(
                &setup_inputs(true, false, false, false, 0.0, 12.0, 0.0, 3),
                &c,
            )
            .unwrap();
        assert!(replace.setups.is_empty());

        let mut replace_after_retest = SetupEvaluator {
            long: true,
            mode: SetupMode::ReplaceLatest,
            expiry: 8,
            capacity: 1,
            setups: VecDeque::new(),
            next_setup: 0,
            accepted_captures: 0,
            rejected_captures: 0,
        };
        replace_after_retest
            .evaluate(
                &setup_inputs(false, false, true, false, 10.0, 10.0, 0.0, 1),
                &c,
            )
            .unwrap();
        assert_eq!(
            replace_after_retest
                .evaluate(
                    &setup_inputs(false, false, true, true, 12.0, 12.0, 0.0, 2),
                    &c,
                )
                .unwrap(),
            Value::Bool(true),
            "an existing setup retest still fires before the new capture"
        );
        assert_eq!(replace_after_retest.setups.len(), 1);
        assert_eq!(replace_after_retest.setups[0].level, 12.0);
        assert_eq!(replace_after_retest.setups[0].created, 2);
    }
}
