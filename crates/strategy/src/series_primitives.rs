use std::sync::Arc;

use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    BarField, CompletedBar, CompletedBarRequirement, MaDerivativeCalculation, MaterialArg,
    MaterialArgs, MovingAverageCalculation, NumericCalculation, NumericDescriptor, NumericInputs,
    NumericMissingPolicy, NumericRange, NumericUnit, PriceInputCalculation, ScalarType, SourceId,
    Value, ValueType,
};

pub const MATERIAL_CLOSE_PRICE: &str = "close_price";
pub const MATERIAL_HL2: &str = "hl2";
pub const MATERIAL_HLC3: &str = "hlc3";
pub const MATERIAL_OHLC4: &str = "ohlc4";
pub const MATERIAL_STRICT_WMA: &str = "strict_wma";
pub const MATERIAL_STRICT_RMA: &str = "strict_rma";
pub const MATERIAL_TRUE_RANGE: &str = "true_range";
pub const MATERIAL_STRICT_ATR: &str = "strict_atr";
pub const MATERIAL_MA_DISTANCE: &str = "ma_distance";
pub const MATERIAL_MA_SLOPE: &str = "ma_slope";
pub const MATERIAL_MA_ACCELERATION: &str = "ma_acceleration";
pub const MATERIAL_MA_GAP: &str = "ma_gap";
pub const MATERIAL_MA_ALIGNMENT: &str = "ma_alignment";

const KEYS: &[&str] = &[
    MATERIAL_CLOSE_PRICE,
    MATERIAL_HL2,
    MATERIAL_HLC3,
    MATERIAL_OHLC4,
    MATERIAL_STRICT_WMA,
    MATERIAL_STRICT_RMA,
    MATERIAL_TRUE_RANGE,
    MATERIAL_STRICT_ATR,
    MATERIAL_MA_DISTANCE,
    MATERIAL_MA_SLOPE,
    MATERIAL_MA_ACCELERATION,
    MATERIAL_MA_GAP,
    MATERIAL_MA_ALIGNMENT,
];
const SOURCE_SCHEMA: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
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
            max: crate::numeric::MAX_STRICT_PERIOD as i64,
        },
        required: true,
    },
];
const SOURCE_HORIZON_SCHEMA: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "horizon",
        kind: ParamKind::Integer {
            min: 1,
            max: (crate::numeric::MAX_STRICT_PERIOD / 2) as i64,
        },
        required: true,
    },
];
const CLOSE: &[BarField] = &[BarField::Close];
const HL: &[BarField] = &[BarField::High, BarField::Low];
const HLC: &[BarField] = &[BarField::High, BarField::Low, BarField::Close];
const OHLC: &[BarField] = &[
    BarField::Open,
    BarField::High,
    BarField::Low,
    BarField::Close,
];

pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(SeriesPrimitiveFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}

struct SeriesPrimitiveFactory {
    key: &'static str,
}

impl SeriesPrimitiveFactory {
    fn source(params: &MaterialArgs) -> Result<SourceId, String> {
        match params.get("source") {
            Some(MaterialArg::Source(source)) => Ok(source.clone()),
            _ => Err("series primitive requires a source".into()),
        }
    }
    fn integer(params: &MaterialArgs, name: &str) -> Result<usize, String> {
        match params.get(name) {
            Some(MaterialArg::Integer(value)) => usize::try_from(*value)
                .ok()
                .filter(|value| (1..=crate::numeric::MAX_STRICT_PERIOD).contains(value))
                .ok_or_else(|| format!("{name} is out of bounds")),
            _ => Err(format!("series primitive requires {name}")),
        }
    }
    fn scalar_unit(scalar: ScalarType) -> Result<NumericUnit, String> {
        Ok(match scalar {
            ScalarType::Number => NumericUnit::Number,
            ScalarType::Price => NumericUnit::Price,
            ScalarType::Ratio => NumericUnit::Ratio,
            ScalarType::Percent => NumericUnit::Percent,
            ScalarType::PricePerObservation => NumericUnit::PricePerObservation,
            ScalarType::PricePerObservationSquared => NumericUnit::PricePerObservationSquared,
            ScalarType::LogReturn => NumericUnit::LogReturn,
            ScalarType::LogReturnVariance => NumericUnit::LogReturnVariance,
            _ => return Err("series primitive requires floating-point scalar input".into()),
        })
    }
}

impl MaterialFactory for SeriesPrimitiveFactory {
    fn numeric_descriptor(
        &self,
        params: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let source_clock = Self::source(params)?;
        let fixed = |calculation, fields, output_type, unit, first, state| NumericDescriptor {
            calculation,
            source_clock: source_clock.clone(),
            inputs: NumericInputs::CompletedBarFields(fields),
            output_type,
            unit,
            range: NumericRange::Unbounded,
            missing: NumericMissingPolicy::ResetAndReseed,
            first_output_observations: first,
            required_lookback: first,
            max_state_bytes: state,
            exact_aliases: &[],
        };
        let descriptor = match self.key {
            MATERIAL_CLOSE_PRICE => {
                if !inputs.is_empty() {
                    return Err("price input accepts no expressions".into());
                }
                let mut d = fixed(
                    NumericCalculation::PriceInput(PriceInputCalculation::Close),
                    CLOSE,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    1,
                    source_owned_state_bytes::<PriceInputEvaluator>(),
                );
                d.exact_aliases = &["completed_bar_field(close)"];
                d
            }
            MATERIAL_HL2 => {
                if !inputs.is_empty() {
                    return Err("price input accepts no expressions".into());
                }
                fixed(
                    NumericCalculation::PriceInput(PriceInputCalculation::Hl2),
                    HL,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    1,
                    source_owned_state_bytes::<PriceInputEvaluator>(),
                )
            }
            MATERIAL_HLC3 => {
                if !inputs.is_empty() {
                    return Err("price input accepts no expressions".into());
                }
                fixed(
                    NumericCalculation::PriceInput(PriceInputCalculation::Hlc3),
                    HLC,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    1,
                    source_owned_state_bytes::<PriceInputEvaluator>(),
                )
            }
            MATERIAL_OHLC4 => {
                if !inputs.is_empty() {
                    return Err("price input accepts no expressions".into());
                }
                fixed(
                    NumericCalculation::PriceInput(PriceInputCalculation::Ohlc4),
                    OHLC,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    1,
                    source_owned_state_bytes::<PriceInputEvaluator>(),
                )
            }
            MATERIAL_TRUE_RANGE => {
                if !inputs.is_empty() {
                    return Err("true range accepts no expressions".into());
                }
                fixed(
                    NumericCalculation::TrueRange,
                    HLC,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    1,
                    source_owned_state_bytes::<TrueRangeEvaluator>(),
                )
            }
            MATERIAL_STRICT_ATR => {
                if !inputs.is_empty() {
                    return Err("strict ATR accepts no expressions".into());
                }
                let period = Self::integer(params, "period")?;
                fixed(
                    NumericCalculation::StrictAtr { period },
                    HLC,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    period,
                    crate::numeric::ObservedWindow::state_bytes(period)?
                        + source_owned_state_bytes::<AtrEvaluator>(),
                )
            }
            MATERIAL_STRICT_WMA | MATERIAL_STRICT_RMA => {
                if inputs.len() != 1 {
                    return Err("strict moving average requires one scalar input".into());
                }
                let period = Self::integer(params, "period")?;
                let scalar = inputs[0].scalar;
                let rma = self.key == MATERIAL_STRICT_RMA;
                NumericDescriptor {
                    calculation: NumericCalculation::MovingAverage {
                        calculation: if rma {
                            MovingAverageCalculation::Rma
                        } else {
                            MovingAverageCalculation::Wma
                        },
                        period,
                    },
                    source_clock,
                    inputs: NumericInputs::Scalar(inputs.to_vec()),
                    output_type: ValueType::optional(scalar),
                    unit: Self::scalar_unit(scalar)?,
                    range: NumericRange::Unbounded,
                    missing: if rma {
                        NumericMissingPolicy::ResetAndReseed
                    } else {
                        NumericMissingPolicy::ConsumeWindowSlot
                    },
                    first_output_observations: period,
                    required_lookback: period,
                    max_state_bytes: crate::numeric::ObservedWindow::state_bytes(period)?
                        + std::mem::size_of::<MovingAverageEvaluator>(),
                    exact_aliases: &[],
                }
            }
            MATERIAL_MA_DISTANCE | MATERIAL_MA_GAP | MATERIAL_MA_ALIGNMENT => {
                if inputs
                    != [
                        ValueType::optional(ScalarType::Price),
                        ValueType::optional(ScalarType::Price),
                    ]
                    && inputs
                        != [
                            ValueType::required(ScalarType::Price),
                            ValueType::required(ScalarType::Price),
                        ]
                {
                    return Err(
                        "MA pair primitive requires two Price inputs with matching optionality"
                            .into(),
                    );
                }
                let alignment = self.key == MATERIAL_MA_ALIGNMENT;
                NumericDescriptor {
                    calculation: NumericCalculation::MaDerivative {
                        calculation: if self.key == MATERIAL_MA_DISTANCE {
                            MaDerivativeCalculation::Distance
                        } else if alignment {
                            MaDerivativeCalculation::Alignment
                        } else {
                            MaDerivativeCalculation::Gap
                        },
                        horizon: None,
                    },
                    source_clock,
                    inputs: NumericInputs::Scalar(inputs.to_vec()),
                    output_type: ValueType::optional(if alignment {
                        ScalarType::Bool
                    } else {
                        ScalarType::Price
                    }),
                    unit: if alignment {
                        NumericUnit::Bool
                    } else {
                        NumericUnit::Price
                    },
                    range: NumericRange::Unbounded,
                    missing: NumericMissingPolicy::ConsumeWindowSlot,
                    first_output_observations: 1,
                    required_lookback: 1,
                    max_state_bytes: std::mem::size_of::<MaDerivativeEvaluator>(),
                    exact_aliases: &[],
                }
            }
            MATERIAL_MA_SLOPE | MATERIAL_MA_ACCELERATION => {
                if inputs.len() != 1 || inputs[0].scalar != ScalarType::Price {
                    return Err("MA slope primitive requires one Price input".into());
                }
                let horizon = Self::integer(params, "horizon")?;
                let acceleration = self.key == MATERIAL_MA_ACCELERATION;
                let first = if acceleration {
                    horizon.checked_mul(2).and_then(|v| v.checked_add(1))
                } else {
                    horizon.checked_add(1)
                }
                .ok_or_else(|| "MA derivative history overflowed".to_string())?;
                NumericDescriptor {
                    calculation: NumericCalculation::MaDerivative {
                        calculation: if acceleration {
                            MaDerivativeCalculation::Acceleration
                        } else {
                            MaDerivativeCalculation::Slope
                        },
                        horizon: Some(horizon),
                    },
                    source_clock,
                    inputs: NumericInputs::Scalar(inputs.to_vec()),
                    output_type: ValueType::optional(if acceleration {
                        ScalarType::PricePerObservationSquared
                    } else {
                        ScalarType::PricePerObservation
                    }),
                    unit: if acceleration {
                        NumericUnit::PricePerObservationSquared
                    } else {
                        NumericUnit::PricePerObservation
                    },
                    range: NumericRange::Unbounded,
                    missing: NumericMissingPolicy::ConsumeWindowSlot,
                    first_output_observations: first,
                    required_lookback: first,
                    max_state_bytes: crate::numeric::ObservedWindow::state_bytes(first)?
                        + std::mem::size_of::<MaDerivativeEvaluator>(),
                    exact_aliases: &[],
                }
            }
            _ => return Err("unknown series primitive".into()),
        };
        Ok(Some(descriptor))
    }

    fn params(&self) -> &[ParamSpec] {
        match self.key {
            MATERIAL_STRICT_WMA | MATERIAL_STRICT_RMA | MATERIAL_STRICT_ATR => {
                &SOURCE_PERIOD_SCHEMA
            }
            MATERIAL_MA_SLOPE | MATERIAL_MA_ACCELERATION => &SOURCE_HORIZON_SCHEMA,
            _ => &SOURCE_SCHEMA,
        }
    }

    fn build(&self, params: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let descriptor = self.numeric_descriptor(params, inputs)?.unwrap();
        let lookback = if matches!(descriptor.inputs, NumericInputs::Scalar(_)) {
            MaterialLookback::InheritInputs {
                minimum: descriptor.first_output_observations,
            }
        } else {
            MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: descriptor.source_clock.clone(),
                required_lookback: descriptor.required_lookback,
            }])
        };
        let evaluator: Box<dyn MaterialEvaluator> = match descriptor.calculation.clone() {
            NumericCalculation::PriceInput(calculation) => Box::new(PriceInputEvaluator {
                calculation,
                source: descriptor.source_clock.clone(),
            }),
            NumericCalculation::TrueRange => Box::new(TrueRangeEvaluator {
                source: descriptor.source_clock.clone(),
                previous_close: None,
            }),
            NumericCalculation::StrictAtr { period } => Box::new(AtrEvaluator {
                source: descriptor.source_clock.clone(),
                period,
                previous_close: None,
                seed: crate::numeric::ObservedWindow::new(period)?,
                value: None,
            }),
            NumericCalculation::MovingAverage {
                calculation,
                period,
            } => Box::new(MovingAverageEvaluator {
                calculation,
                period,
                seed: crate::numeric::ObservedWindow::new(period)?,
                value: None,
                scalar: descriptor.output_type.scalar,
            }),
            NumericCalculation::MaDerivative {
                calculation,
                horizon,
            } => Box::new(MaDerivativeEvaluator {
                calculation,
                horizon,
                values: horizon
                    .map(|_| {
                        crate::numeric::ObservedWindow::new(descriptor.first_output_observations)
                    })
                    .transpose()?,
                output_type: descriptor.output_type,
            }),
            _ => return Err("series factory received another calculation family".into()),
        };
        Ok(MaterialBuild {
            output_type: descriptor.output_type,
            lookback,
            max_state_bytes: descriptor.max_state_bytes,
            evaluator,
        })
    }

    fn update_trigger(
        &self,
        params: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::Source(Self::source(params)?))
    }
}

fn current_bar<'a>(
    source: &SourceId,
    context: &'a MaterialEvalContext<'_>,
) -> Result<&'a CompletedBar, String> {
    context
        .input
        .completed_bars
        .iter()
        .find(|update| &update.source == source)
        .map(|update| &update.bar)
        .ok_or_else(|| "series primitive triggered without its source bar".into())
}

fn source_owned_state_bytes<T>() -> usize {
    std::mem::size_of::<T>() + crate::MAX_ID_BYTES
}

#[derive(Clone)]
struct PriceInputEvaluator {
    calculation: PriceInputCalculation,
    source: SourceId,
}
impl MaterialEvaluator for PriceInputEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let bar = current_bar(&self.source, context)?;
        let value = match self.calculation {
            PriceInputCalculation::Close => bar.close,
            PriceInputCalculation::Hl2 => crate::numeric::stable_mean([bar.high, bar.low])?,
            PriceInputCalculation::Hlc3 => {
                crate::numeric::stable_mean([bar.high, bar.low, bar.close])?
            }
            PriceInputCalculation::Ohlc4 => {
                crate::numeric::stable_mean([bar.open, bar.high, bar.low, bar.close])?
            }
        };
        Ok(Value::Price(value))
    }
}

fn true_range(bar: &CompletedBar, previous_close: Option<f64>) -> f64 {
    previous_close.map_or(bar.high - bar.low, |previous| {
        (bar.high - bar.low)
            .max((bar.high - previous).abs())
            .max((bar.low - previous).abs())
    })
}

#[derive(Clone)]
struct TrueRangeEvaluator {
    source: SourceId,
    previous_close: Option<f64>,
}
impl MaterialEvaluator for TrueRangeEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let bar = current_bar(&self.source, context)?;
        let value = true_range(bar, self.previous_close);
        self.previous_close = Some(bar.close);
        Ok(Value::Price(value))
    }
}

#[derive(Clone)]
struct AtrEvaluator {
    source: SourceId,
    period: usize,
    previous_close: Option<f64>,
    seed: crate::numeric::ObservedWindow,
    value: Option<f64>,
}
impl MaterialEvaluator for AtrEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let bar = current_bar(&self.source, context)?;
        let tr = true_range(bar, self.previous_close);
        self.previous_close = Some(bar.close);
        let next = if let Some(previous) = self.value {
            crate::numeric::rma_step(tr, previous, self.period)?
        } else {
            self.seed.push(Some(tr))?;
            let Some(seed) = self.seed.mean()? else {
                return Ok(Value::Missing(ScalarType::Price));
            };
            self.seed.reset();
            seed
        };
        self.value = Some(next);
        Ok(Value::Price(next))
    }
}

#[derive(Clone)]
struct MovingAverageEvaluator {
    calculation: MovingAverageCalculation,
    period: usize,
    seed: crate::numeric::ObservedWindow,
    value: Option<f64>,
    scalar: ScalarType,
}
impl MaterialEvaluator for MovingAverageEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        inputs: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let sample = if context.input_updates.first() == Some(&false) {
            None
        } else {
            scalar_value(&inputs[0])?
        };
        let Some(sample) = sample else {
            if self.calculation == MovingAverageCalculation::Rma {
                self.seed.reset();
                self.value = None;
            } else {
                self.seed.push(None)?;
            }
            return Ok(Value::Missing(self.scalar));
        };
        let output = match self.calculation {
            MovingAverageCalculation::Wma => {
                self.seed.push(Some(sample))?;
                if !self.seed.complete() || self.seed.chronological().any(|value| value.is_none()) {
                    None
                } else {
                    let weight = self
                        .period
                        .checked_mul(self.period + 1)
                        .and_then(|value| value.checked_div(2))
                        .ok_or_else(|| "WMA weight overflowed".to_string())?
                        as u64;
                    Some(crate::numeric::weighted_mean(
                        self.seed
                            .chronological()
                            .enumerate()
                            .map(|(index, value)| (value.unwrap(), index as u64 + 1)),
                        weight,
                    )?)
                }
            }
            MovingAverageCalculation::Rma => {
                if let Some(previous) = self.value {
                    Some(crate::numeric::rma_step(sample, previous, self.period)?)
                } else {
                    self.seed.push(Some(sample))?;
                    self.seed.mean()?
                }
            }
            _ => return Err("moving average evaluator received unsupported calculation".into()),
        };
        if let Some(output) = output {
            self.value = Some(output);
            Ok(typed_value(self.scalar, output))
        } else {
            Ok(Value::Missing(self.scalar))
        }
    }
}

#[derive(Clone)]
struct MaDerivativeEvaluator {
    calculation: MaDerivativeCalculation,
    horizon: Option<usize>,
    values: Option<crate::numeric::ObservedWindow>,
    output_type: ValueType,
}
impl MaterialEvaluator for MaDerivativeEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        inputs: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let sample = |index: usize| {
            if context.input_updates.get(index) == Some(&false) {
                Ok(None)
            } else {
                scalar_value(&inputs[index])
            }
        };
        let value = match self.calculation {
            MaDerivativeCalculation::Distance | MaDerivativeCalculation::Gap => {
                match (sample(0)?, sample(1)?) {
                    (Some(left), Some(right)) => Some(Value::Price(left - right)),
                    _ => None,
                }
            }
            MaDerivativeCalculation::Alignment => match (sample(0)?, sample(1)?) {
                (Some(left), Some(right)) => Some(Value::Bool(left > right)),
                _ => None,
            },
            MaDerivativeCalculation::Slope | MaDerivativeCalculation::Acceleration => {
                let current = sample(0)?;
                let window = self.values.as_mut().unwrap();
                window.push(current)?;
                if !window.complete() || window.chronological().any(|value| value.is_none()) {
                    None
                } else {
                    let values = window
                        .chronological()
                        .map(Option::unwrap)
                        .collect::<Vec<_>>();
                    let h = self.horizon.unwrap();
                    if self.calculation == MaDerivativeCalculation::Slope {
                        Some(Value::PricePerObservation(
                            (values[values.len() - 1] - values[0]) / h as f64,
                        ))
                    } else {
                        let first = (values[h] - values[0]) / h as f64;
                        let second = (values[2 * h] - values[h]) / h as f64;
                        Some(Value::PricePerObservationSquared(
                            (second - first) / h as f64,
                        ))
                    }
                }
            }
        };
        Ok(value.unwrap_or(Value::Missing(self.output_type.scalar)))
    }
}

fn scalar_value(value: &Value) -> Result<Option<f64>, String> {
    match value {
        Value::Missing(_) => Ok(None),
        Value::Number(value)
        | Value::Price(value)
        | Value::Ratio(value)
        | Value::Percent(value)
        | Value::PricePerObservation(value)
        | Value::PricePerObservationSquared(value)
        | Value::LogReturn(value)
        | Value::LogReturnVariance(value)
            if value.is_finite() =>
        {
            Ok(Some(*value))
        }
        _ => Err("series primitive requires a finite floating-point input".into()),
    }
}
fn typed_value(scalar: ScalarType, value: f64) -> Value {
    match scalar {
        ScalarType::Number => Value::Number(value),
        ScalarType::Price => Value::Price(value),
        ScalarType::Ratio => Value::Ratio(value),
        ScalarType::Percent => Value::Percent(value),
        ScalarType::PricePerObservation => Value::PricePerObservation(value),
        ScalarType::PricePerObservationSquared => Value::PricePerObservationSquared(value),
        ScalarType::LogReturn => Value::LogReturn(value),
        ScalarType::LogReturnVariance => Value::LogReturnVariance(value),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompletedBarUpdate, NamedValue, StrategyInput};
    use chrono::NaiveDateTime;

    fn bar(open: f64, high: f64, low: f64, close: f64) -> CompletedBar {
        CompletedBar {
            open,
            high,
            low,
            close,
            volume: Some(1.0),
        }
    }
    fn context(source: &SourceId, bar: CompletedBar) -> StrategyInput {
        StrategyInput {
            time: NaiveDateTime::default(),
            ready: true,
            completed_bars: vec![CompletedBarUpdate {
                source: source.clone(),
                bar,
            }],
            values: vec![],
            trade_slots: vec![],
            feedback: vec![],
        }
    }
    fn evaluate_bars(
        key: &'static str,
        period: Option<usize>,
        bars: &[CompletedBar],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut params = vec![("source", MaterialArg::Source(source.clone()))];
        if let Some(period) = period {
            params.push(("period", MaterialArg::Integer(period as i64)));
        }
        let mut evaluator = SeriesPrimitiveFactory { key }
            .build(&MaterialArgs::new(params), &[])
            .unwrap()
            .evaluator;
        bars.iter()
            .cloned()
            .map(|bar| {
                let input = context(&source, bar);
                evaluator
                    .evaluate(
                        &[],
                        &MaterialEvalContext {
                            input: &input,
                            input_updates: &[],
                            any_input_updates: &[],
                            feedback: &[],
                            retained_feedback: &[],
                        },
                    )
                    .unwrap()
            })
            .collect()
    }
    fn evaluate_scalar(key: &'static str, period: usize, samples: &[Option<f64>]) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let params = MaterialArgs::new([
            ("source", MaterialArg::Source(source.clone())),
            ("period", MaterialArg::Integer(period as i64)),
        ]);
        let mut evaluator = SeriesPrimitiveFactory { key }
            .build(&params, &[ValueType::optional(ScalarType::Price)])
            .unwrap()
            .evaluator;
        samples
            .iter()
            .map(|sample| {
                let mut input = context(&source, bar(1.0, 1.0, 1.0, 1.0));
                input.values.push(NamedValue {
                    name: "sample".into(),
                    value: sample
                        .map(Value::Price)
                        .unwrap_or(Value::Missing(ScalarType::Price)),
                    updated: true,
                });
                evaluator
                    .evaluate(
                        &[input.values[0].value.clone()],
                        &MaterialEvalContext {
                            input: &input,
                            input_updates: &[true],
                            any_input_updates: &[true],
                            feedback: &[],
                            retained_feedback: &[],
                        },
                    )
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn price_inputs_true_range_and_strict_atr_match_hand_values() {
        let fixture = [bar(2.0, 5.0, 1.0, 4.0)];
        assert_eq!(
            evaluate_bars(MATERIAL_CLOSE_PRICE, None, &fixture),
            vec![Value::Price(4.0)]
        );
        assert_eq!(
            evaluate_bars(MATERIAL_HL2, None, &fixture),
            vec![Value::Price(3.0)]
        );
        assert_eq!(
            evaluate_bars(MATERIAL_HLC3, None, &fixture),
            vec![Value::Price(10.0 / 3.0)]
        );
        assert_eq!(
            evaluate_bars(MATERIAL_OHLC4, None, &fixture),
            vec![Value::Price(3.0)]
        );
        let extreme = [bar(f64::MAX, f64::MAX, f64::MAX, f64::MAX)];
        for key in [MATERIAL_HL2, MATERIAL_HLC3, MATERIAL_OHLC4] {
            assert_eq!(
                evaluate_bars(key, None, &extreme),
                vec![Value::Price(f64::MAX)],
                "{key} must not overflow a representable finite mean"
            );
        }
        let source = SourceId::new("x".repeat(crate::MAX_ID_BYTES)).unwrap();
        let params = MaterialArgs::new([("source", MaterialArg::Source(source))]);
        for key in [
            MATERIAL_CLOSE_PRICE,
            MATERIAL_HL2,
            MATERIAL_HLC3,
            MATERIAL_OHLC4,
        ] {
            let descriptor = SeriesPrimitiveFactory { key }
                .numeric_descriptor(&params, &[])
                .unwrap()
                .unwrap();
            assert!(
                descriptor.max_state_bytes
                    >= std::mem::size_of::<PriceInputEvaluator>() + crate::MAX_ID_BYTES
            );
        }
        let tr_bars = [
            bar(1.0, 2.0, 0.0 + f64::MIN_POSITIVE, 1.0),
            bar(3.0, 5.0, 1.0, 4.0),
            bar(4.0, 7.0, 4.0, 5.0),
            bar(8.0, 10.0, 5.0, 9.0),
        ];
        assert_eq!(
            evaluate_bars(MATERIAL_TRUE_RANGE, None, &tr_bars),
            vec![
                Value::Price(2.0 - f64::MIN_POSITIVE),
                Value::Price(4.0),
                Value::Price(3.0),
                Value::Price(5.0)
            ]
        );
        let atr = evaluate_bars(MATERIAL_STRICT_ATR, Some(3), &tr_bars);
        assert_eq!(
            atr[..2],
            [
                Value::Missing(ScalarType::Price),
                Value::Missing(ScalarType::Price)
            ]
        );
        assert!(
            (match atr[2] {
                Value::Price(value) => value,
                _ => unreachable!(),
            } - 3.0)
                .abs()
                < 1e-15
        );
        assert!(
            (match atr[3] {
                Value::Price(value) => value,
                _ => unreachable!(),
            } - 11.0 / 3.0)
                .abs()
                < 1e-15
        );
    }

    #[test]
    fn strict_wma_and_rma_seed_update_and_recover_as_declared() {
        let samples = [Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        let wma = evaluate_scalar(MATERIAL_STRICT_WMA, 3, &samples);
        assert_eq!(
            wma[..2],
            [
                Value::Missing(ScalarType::Price),
                Value::Missing(ScalarType::Price)
            ]
        );
        assert_eq!(wma[2], Value::Price(14.0 / 6.0));
        assert_eq!(wma[3], Value::Price(20.0 / 6.0));
        let rma = evaluate_scalar(MATERIAL_STRICT_RMA, 3, &samples);
        assert_eq!(rma[2], Value::Price(2.0));
        assert_eq!(rma[3], Value::Price(8.0 / 3.0));
        let missing = [Some(1.0), Some(2.0), None, Some(4.0), Some(5.0), Some(6.0)];
        assert_eq!(
            evaluate_scalar(MATERIAL_STRICT_RMA, 3, &missing).last(),
            Some(&Value::Price(5.0))
        );
        assert_eq!(
            evaluate_scalar(MATERIAL_STRICT_WMA, 3, &missing).last(),
            Some(&Value::Price(32.0 / 6.0))
        );
    }

    #[test]
    fn ma_derivatives_use_composed_observation_horizons() {
        let source = SourceId::new("bars").unwrap();
        let params = |horizon: Option<usize>| {
            let mut values = vec![("source", MaterialArg::Source(source.clone()))];
            if let Some(horizon) = horizon {
                values.push(("horizon", MaterialArg::Integer(horizon as i64)));
            }
            MaterialArgs::new(values)
        };
        for (key, inputs, expected) in [
            (
                MATERIAL_MA_DISTANCE,
                vec![Value::Price(5.0), Value::Price(3.0)],
                Value::Price(2.0),
            ),
            (
                MATERIAL_MA_GAP,
                vec![Value::Price(5.0), Value::Price(3.0)],
                Value::Price(2.0),
            ),
            (
                MATERIAL_MA_ALIGNMENT,
                vec![Value::Price(5.0), Value::Price(3.0)],
                Value::Bool(true),
            ),
        ] {
            let types = vec![ValueType::optional(ScalarType::Price); 2];
            let mut evaluator = SeriesPrimitiveFactory { key }
                .build(&params(None), &types)
                .unwrap()
                .evaluator;
            let input = context(&source, bar(1.0, 1.0, 1.0, 1.0));
            assert_eq!(
                evaluator
                    .evaluate(
                        &inputs,
                        &MaterialEvalContext {
                            input: &input,
                            input_updates: &[true, true],
                            any_input_updates: &[true, true],
                            feedback: &[],
                            retained_feedback: &[]
                        }
                    )
                    .unwrap(),
                expected
            );
        }
        for (key, expected) in [
            (MATERIAL_MA_SLOPE, Value::PricePerObservation(1.0)),
            (
                MATERIAL_MA_ACCELERATION,
                Value::PricePerObservationSquared(0.0),
            ),
        ] {
            let mut evaluator = SeriesPrimitiveFactory { key }
                .build(&params(Some(2)), &[ValueType::optional(ScalarType::Price)])
                .unwrap()
                .evaluator;
            let count = if key == MATERIAL_MA_SLOPE { 3 } else { 5 };
            let mut output = Value::Missing(if key == MATERIAL_MA_SLOPE {
                ScalarType::PricePerObservation
            } else {
                ScalarType::PricePerObservationSquared
            });
            for value in 1..=count {
                let input = context(&source, bar(1.0, 1.0, 1.0, 1.0));
                output = evaluator
                    .evaluate(
                        &[Value::Price(value as f64)],
                        &MaterialEvalContext {
                            input: &input,
                            input_updates: &[true],
                            any_input_updates: &[true],
                            feedback: &[],
                            retained_feedback: &[],
                        },
                    )
                    .unwrap();
            }
            assert_eq!(output, expected);
        }
    }
}
