use std::sync::Arc;

use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    BarField, CompletedBar, CompletedBarRequirement, MaterialArg, MaterialArgs,
    MomentumCalculation, NumericCalculation, NumericDescriptor, NumericInputs,
    NumericMissingPolicy, NumericRange, NumericUnit, ScalarType, SourceId, Value, ValueType,
};

pub const MATERIAL_STRICT_RSI: &str = "strict_rsi";
pub const MATERIAL_RSI_CHANGE: &str = "rsi_change";
pub const MATERIAL_STOCHASTIC_FAST_K: &str = "stochastic_fast_k";
pub const MATERIAL_STOCHASTIC_SLOW_K: &str = "stochastic_slow_k";
pub const MATERIAL_STOCHASTIC_SLOW_D: &str = "stochastic_slow_d";
pub const MATERIAL_MACD: &str = "macd";
pub const MATERIAL_MACD_SIGNAL: &str = "macd_signal";
pub const MATERIAL_MACD_HISTOGRAM: &str = "macd_histogram";
pub const MATERIAL_CCI: &str = "cci";
pub const MATERIAL_PLUS_DI: &str = "plus_di";
pub const MATERIAL_MINUS_DI: &str = "minus_di";
pub const MATERIAL_DI_DIFFERENCE: &str = "di_difference";
pub const MATERIAL_DX: &str = "dx";
pub const MATERIAL_ADX: &str = "adx";

const MAX_MULTI_PERIOD: i64 = 256;
const KEYS: &[&str] = &[
    MATERIAL_STRICT_RSI,
    MATERIAL_RSI_CHANGE,
    MATERIAL_STOCHASTIC_FAST_K,
    MATERIAL_STOCHASTIC_SLOW_K,
    MATERIAL_STOCHASTIC_SLOW_D,
    MATERIAL_MACD,
    MATERIAL_MACD_SIGNAL,
    MATERIAL_MACD_HISTOGRAM,
    MATERIAL_CCI,
    MATERIAL_PLUS_DI,
    MATERIAL_MINUS_DI,
    MATERIAL_DI_DIFFERENCE,
    MATERIAL_DX,
    MATERIAL_ADX,
];
const SOURCE_PERIOD: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
];
const SOURCE_HORIZON: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "horizon",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
];
const STOCHASTIC_SCHEMA: [ParamSpec; 4] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "smooth_k",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: false,
    },
    ParamSpec {
        name: "smooth_d",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: false,
    },
];
const MACD_SCHEMA: [ParamSpec; 4] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "fast",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "slow",
        kind: ParamKind::Integer {
            min: 2,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "signal",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_MULTI_PERIOD,
        },
        required: true,
    },
];
const HLC: &[BarField] = &[BarField::High, BarField::Low, BarField::Close];

pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(MomentumFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}

struct MomentumFactory {
    key: &'static str,
}
impl MomentumFactory {
    fn source(params: &MaterialArgs) -> Result<SourceId, String> {
        match params.get("source") {
            Some(MaterialArg::Source(source)) => Ok(source.clone()),
            _ => Err("momentum primitive requires source".into()),
        }
    }
    fn integer(params: &MaterialArgs, name: &str, default: Option<usize>) -> Result<usize, String> {
        match params.get(name) {
            Some(MaterialArg::Integer(value)) => usize::try_from(*value)
                .ok()
                .filter(|value| (1..=MAX_MULTI_PERIOD as usize).contains(value))
                .ok_or_else(|| format!("{name} is out of bounds")),
            None => default.ok_or_else(|| format!("momentum primitive requires {name}")),
            _ => Err(format!("momentum primitive requires integer {name}")),
        }
    }
    fn calculation(
        &self,
        params: &MaterialArgs,
    ) -> Result<(MomentumCalculation, [usize; 3], usize), String> {
        Ok(match self.key {
            MATERIAL_STRICT_RSI => {
                let n = Self::integer(params, "period", None)?;
                (MomentumCalculation::WilderRsi, [n, 0, 0], n + 1)
            }
            MATERIAL_RSI_CHANGE => {
                let h = Self::integer(params, "horizon", None)?;
                (MomentumCalculation::RsiChange, [h, 0, 0], h + 1)
            }
            MATERIAL_STOCHASTIC_FAST_K
            | MATERIAL_STOCHASTIC_SLOW_K
            | MATERIAL_STOCHASTIC_SLOW_D => {
                let n = Self::integer(params, "period", None)?;
                let k = Self::integer(params, "smooth_k", Some(3))?;
                let d = Self::integer(params, "smooth_d", Some(3))?;
                let calculation = if self.key == MATERIAL_STOCHASTIC_FAST_K {
                    MomentumCalculation::StochasticFastK
                } else if self.key == MATERIAL_STOCHASTIC_SLOW_K {
                    MomentumCalculation::StochasticSlowK
                } else {
                    MomentumCalculation::StochasticSlowD
                };
                let first = match calculation {
                    MomentumCalculation::StochasticFastK => n,
                    MomentumCalculation::StochasticSlowK => n + k - 1,
                    _ => n + k + d - 2,
                };
                (calculation, [n, k, d], first)
            }
            MATERIAL_MACD | MATERIAL_MACD_SIGNAL | MATERIAL_MACD_HISTOGRAM => {
                let fast = Self::integer(params, "fast", None)?;
                let slow = Self::integer(params, "slow", None)?;
                let signal = Self::integer(params, "signal", None)?;
                if fast >= slow {
                    return Err("MACD requires fast < slow".into());
                }
                let calculation = if self.key == MATERIAL_MACD {
                    MomentumCalculation::Macd
                } else if self.key == MATERIAL_MACD_SIGNAL {
                    MomentumCalculation::MacdSignal
                } else {
                    MomentumCalculation::MacdHistogram
                };
                let first = if calculation == MomentumCalculation::Macd {
                    slow
                } else {
                    slow + signal - 1
                };
                (calculation, [fast, slow, signal], first)
            }
            MATERIAL_CCI => {
                let n = Self::integer(params, "period", None)?;
                (MomentumCalculation::Cci, [n, 0, 0], n)
            }
            MATERIAL_PLUS_DI
            | MATERIAL_MINUS_DI
            | MATERIAL_DI_DIFFERENCE
            | MATERIAL_DX
            | MATERIAL_ADX => {
                let n = Self::integer(params, "period", None)?;
                let calculation = match self.key {
                    MATERIAL_PLUS_DI => MomentumCalculation::PlusDi,
                    MATERIAL_MINUS_DI => MomentumCalculation::MinusDi,
                    MATERIAL_DI_DIFFERENCE => MomentumCalculation::DiDifference,
                    MATERIAL_DX => MomentumCalculation::Dx,
                    _ => MomentumCalculation::Adx,
                };
                let first = if calculation == MomentumCalculation::Adx {
                    2 * n
                } else {
                    n + 1
                };
                (calculation, [n, 0, 0], first)
            }
            _ => return Err("unknown momentum primitive".into()),
        })
    }
}

impl MaterialFactory for MomentumFactory {
    fn numeric_descriptor(
        &self,
        params: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let source_clock = Self::source(params)?;
        let (calculation, periods, first) = self.calculation(params)?;
        let scalar_input = matches!(
            calculation,
            MomentumCalculation::WilderRsi
                | MomentumCalculation::RsiChange
                | MomentumCalculation::Macd
                | MomentumCalculation::MacdSignal
                | MomentumCalculation::MacdHistogram
        );
        if scalar_input
            && (inputs.len() != 1
                || !matches!(inputs[0].scalar, ScalarType::Price | ScalarType::Percent))
        {
            return Err("momentum scalar primitive has an incompatible input".into());
        }
        if !scalar_input && !inputs.is_empty() {
            return Err("bar momentum primitive accepts no expression inputs".into());
        }
        let (output_type, unit, range) = match calculation {
            MomentumCalculation::Macd
            | MomentumCalculation::MacdSignal
            | MomentumCalculation::MacdHistogram => (
                ValueType::optional(ScalarType::Price),
                NumericUnit::Price,
                NumericRange::Unbounded,
            ),
            MomentumCalculation::Cci => (
                ValueType::optional(ScalarType::Number),
                NumericUnit::Number,
                NumericRange::Unbounded,
            ),
            _ => (
                ValueType::optional(ScalarType::Percent),
                NumericUnit::Percent,
                NumericRange::Inclusive {
                    minimum: if calculation == MomentumCalculation::DiDifference {
                        -100.0
                    } else {
                        0.0
                    },
                    maximum: 100.0,
                },
            ),
        };
        let state = match calculation {
            MomentumCalculation::WilderRsi => {
                2 * crate::numeric::ObservedWindow::state_bytes(periods[0])?
                    + std::mem::size_of::<RsiEvaluator>()
            }
            MomentumCalculation::RsiChange => {
                crate::numeric::ObservedWindow::state_bytes(first)?
                    + std::mem::size_of::<RsiChangeEvaluator>()
            }
            MomentumCalculation::StochasticFastK
            | MomentumCalculation::StochasticSlowK
            | MomentumCalculation::StochasticSlowD => {
                BarRing::state_bytes(periods[0])?
                    + crate::numeric::ObservedWindow::state_bytes(periods[1])?
                    + crate::numeric::ObservedWindow::state_bytes(periods[2])?
                    + std::mem::size_of::<StochasticEvaluator>()
            }
            MomentumCalculation::Macd
            | MomentumCalculation::MacdSignal
            | MomentumCalculation::MacdHistogram => {
                crate::numeric::ObservedWindow::state_bytes(periods[0])?
                    + crate::numeric::ObservedWindow::state_bytes(periods[1])?
                    + crate::numeric::ObservedWindow::state_bytes(periods[2])?
                    + std::mem::size_of::<MacdEvaluator>()
            }
            MomentumCalculation::Cci => {
                crate::numeric::ObservedWindow::state_bytes(periods[0])?
                    + std::mem::size_of::<CciEvaluator>()
            }
            _ => {
                4 * crate::numeric::ObservedWindow::state_bytes(periods[0])?
                    + std::mem::size_of::<DmiEvaluator>()
            }
        };
        if state > crate::MAX_MATERIAL_STATE_BYTES {
            return Err("momentum state exceeds material bound".into());
        }
        Ok(Some(NumericDescriptor {
            calculation: NumericCalculation::Momentum {
                calculation,
                periods,
            },
            source_clock,
            inputs: if scalar_input {
                NumericInputs::Scalar(inputs.to_vec())
            } else {
                NumericInputs::CompletedBarFields(HLC)
            },
            output_type,
            unit,
            range,
            missing: NumericMissingPolicy::ResetAndReseed,
            first_output_observations: first,
            required_lookback: first,
            max_state_bytes: state,
            exact_aliases: &[],
        }))
    }
    fn params(&self) -> &[ParamSpec] {
        match self.key {
            MATERIAL_RSI_CHANGE => &SOURCE_HORIZON,
            MATERIAL_STOCHASTIC_FAST_K
            | MATERIAL_STOCHASTIC_SLOW_K
            | MATERIAL_STOCHASTIC_SLOW_D => &STOCHASTIC_SCHEMA,
            MATERIAL_MACD | MATERIAL_MACD_SIGNAL | MATERIAL_MACD_HISTOGRAM => &MACD_SCHEMA,
            _ => &SOURCE_PERIOD,
        }
    }
    fn build(&self, params: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let d = self.numeric_descriptor(params, inputs)?.unwrap();
        let NumericCalculation::Momentum {
            calculation,
            periods,
        } = d.calculation
        else {
            unreachable!()
        };
        let evaluator: Box<dyn MaterialEvaluator> = match calculation {
            MomentumCalculation::WilderRsi => Box::new(RsiEvaluator::new(periods[0])?),
            MomentumCalculation::RsiChange => Box::new(RsiChangeEvaluator {
                horizon: periods[0],
                values: crate::numeric::ObservedWindow::new(periods[0] + 1)?,
            }),
            MomentumCalculation::StochasticFastK
            | MomentumCalculation::StochasticSlowK
            | MomentumCalculation::StochasticSlowD => Box::new(StochasticEvaluator::new(
                calculation,
                periods,
                d.source_clock.clone(),
            )?),
            MomentumCalculation::Macd
            | MomentumCalculation::MacdSignal
            | MomentumCalculation::MacdHistogram => {
                Box::new(MacdEvaluator::new(calculation, periods)?)
            }
            MomentumCalculation::Cci => Box::new(CciEvaluator {
                source: d.source_clock.clone(),
                values: crate::numeric::ObservedWindow::new(periods[0])?,
            }),
            _ => Box::new(DmiEvaluator::new(
                calculation,
                periods[0],
                d.source_clock.clone(),
            )?),
        };
        let lookback = if matches!(d.inputs, NumericInputs::Scalar(_)) {
            MaterialLookback::InheritInputs {
                minimum: d.first_output_observations,
            }
        } else {
            MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: d.source_clock.clone(),
                required_lookback: d.required_lookback,
            }])
        };
        Ok(MaterialBuild {
            output_type: d.output_type,
            lookback,
            max_state_bytes: d.max_state_bytes,
            evaluator: Box::new(ClockedMomentumEvaluator {
                source: d.source_clock,
                evaluator,
            }),
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

struct ClockedMomentumEvaluator {
    source: SourceId,
    evaluator: Box<dyn MaterialEvaluator>,
}
impl Clone for ClockedMomentumEvaluator {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            evaluator: self.evaluator.clone(),
        }
    }
}
impl MaterialEvaluator for ClockedMomentumEvaluator {
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

fn scalar(value: &Value) -> Result<Option<f64>, String> {
    match value {
        Value::Missing(_) => Ok(None),
        Value::Price(value) | Value::Percent(value) if value.is_finite() => Ok(Some(*value)),
        _ => Err("momentum input must be finite Price or Percent".into()),
    }
}
fn percent(value: Option<f64>) -> Value {
    value
        .map(Value::Percent)
        .unwrap_or(Value::Missing(ScalarType::Percent))
}
fn price(value: Option<f64>) -> Value {
    value
        .map(Value::Price)
        .unwrap_or(Value::Missing(ScalarType::Price))
}

#[derive(Clone)]
struct RsiEvaluator {
    period: usize,
    previous: Option<f64>,
    gains: crate::numeric::ObservedWindow,
    losses: crate::numeric::ObservedWindow,
    average_gain: Option<f64>,
    average_loss: Option<f64>,
}
impl RsiEvaluator {
    fn new(period: usize) -> Result<Self, String> {
        Ok(Self {
            period,
            previous: None,
            gains: crate::numeric::ObservedWindow::new(period)?,
            losses: crate::numeric::ObservedWindow::new(period)?,
            average_gain: None,
            average_loss: None,
        })
    }
    fn reset(&mut self) {
        self.previous = None;
        self.gains.reset();
        self.losses.reset();
        self.average_gain = None;
        self.average_loss = None
    }
}
impl MaterialEvaluator for RsiEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let Some(current) = scalar(&inputs[0])? else {
            self.reset();
            return Ok(Value::Missing(ScalarType::Percent));
        };
        let Some(previous) = self.previous.replace(current) else {
            return Ok(Value::Missing(ScalarType::Percent));
        };
        let change = current - previous;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        let (g, l) = if let (Some(g), Some(l)) = (self.average_gain, self.average_loss) {
            (
                crate::numeric::rma_step(gain, g, self.period)?,
                crate::numeric::rma_step(loss, l, self.period)?,
            )
        } else {
            self.gains.push(Some(gain))?;
            self.losses.push(Some(loss))?;
            let (Some(g), Some(l)) = (self.gains.mean()?, self.losses.mean()?) else {
                return Ok(Value::Missing(ScalarType::Percent));
            };
            (g, l)
        };
        self.average_gain = Some(g);
        self.average_loss = Some(l);
        Ok(percent(Some(if g == 0.0 && l == 0.0 {
            50.0
        } else if l == 0.0 {
            100.0
        } else if g == 0.0 {
            0.0
        } else {
            100.0 - 100.0 / (1.0 + g / l)
        })))
    }
}

#[derive(Clone)]
struct RsiChangeEvaluator {
    horizon: usize,
    values: crate::numeric::ObservedWindow,
}
impl MaterialEvaluator for RsiChangeEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        self.values.push(scalar(&inputs[0])?)?;
        if !self.values.complete() || self.values.chronological().any(|v| v.is_none()) {
            return Ok(Value::Missing(ScalarType::Percent));
        }
        let values = self
            .values
            .chronological()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        Ok(Value::Percent(values[self.horizon] - values[0]))
    }
}

#[derive(Clone)]
struct BarRing {
    slots: Box<[Option<CompletedBar>]>,
    next: usize,
    len: usize,
}
impl BarRing {
    fn new(period: usize) -> Result<Self, String> {
        Self::state_bytes(period)?;
        Ok(Self {
            slots: vec![None; period].into_boxed_slice(),
            next: 0,
            len: 0,
        })
    }
    fn state_bytes(period: usize) -> Result<usize, String> {
        period
            .checked_mul(std::mem::size_of::<Option<CompletedBar>>())
            .and_then(|v| v.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| "bar ring bound overflowed".into())
    }
    fn push(&mut self, bar: CompletedBar) {
        self.slots[self.next] = Some(bar);
        self.next = (self.next + 1) % self.slots.len();
        self.len = (self.len + 1).min(self.slots.len())
    }
    fn complete(&self) -> bool {
        self.len == self.slots.len()
    }
    fn get(&self, age: usize) -> &CompletedBar {
        self.slots[(self.next + self.slots.len() - 1 - age) % self.slots.len()]
            .as_ref()
            .unwrap()
    }
    fn iter(&self) -> impl Iterator<Item = &CompletedBar> {
        (0..self.len).map(|age| self.get(age))
    }
}
fn bar<'a>(
    source: &SourceId,
    context: &'a MaterialEvalContext<'_>,
) -> Result<&'a CompletedBar, String> {
    context
        .input
        .completed_bars
        .iter()
        .find(|u| &u.source == source)
        .map(|u| &u.bar)
        .ok_or_else(|| "momentum primitive missing clock bar".into())
}

#[derive(Clone)]
struct StochasticEvaluator {
    calculation: MomentumCalculation,
    source: SourceId,
    bars: BarRing,
    slow_k: crate::numeric::ObservedWindow,
    slow_d: crate::numeric::ObservedWindow,
}
impl StochasticEvaluator {
    fn new(
        calculation: MomentumCalculation,
        p: [usize; 3],
        source: SourceId,
    ) -> Result<Self, String> {
        Ok(Self {
            calculation,
            source,
            bars: BarRing::new(p[0])?,
            slow_k: crate::numeric::ObservedWindow::new(p[1])?,
            slow_d: crate::numeric::ObservedWindow::new(p[2])?,
        })
    }
}
impl MaterialEvaluator for StochasticEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        self.bars.push(bar(&self.source, context)?.clone());
        let fast = if self.bars.complete() {
            let high = self
                .bars
                .iter()
                .map(|b| b.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let low = self
                .bars
                .iter()
                .map(|b| b.low)
                .fold(f64::INFINITY, f64::min);
            if high == low {
                None
            } else {
                Some(100.0 * (self.bars.get(0).close - low) / (high - low))
            }
        } else {
            None
        };
        self.slow_k.push(fast)?;
        let slow_k = self.slow_k.mean()?;
        self.slow_d.push(slow_k)?;
        let slow_d = self.slow_d.mean()?;
        Ok(percent(match self.calculation {
            MomentumCalculation::StochasticFastK => fast,
            MomentumCalculation::StochasticSlowK => slow_k,
            _ => slow_d,
        }))
    }
}

#[derive(Clone)]
struct EmaKernel {
    period: usize,
    seed: crate::numeric::ObservedWindow,
    value: Option<f64>,
}
impl EmaKernel {
    fn new(period: usize) -> Result<Self, String> {
        Ok(Self {
            period,
            seed: crate::numeric::ObservedWindow::new(period)?,
            value: None,
        })
    }
    fn reset(&mut self) {
        self.seed.reset();
        self.value = None
    }
    fn push(&mut self, sample: Option<f64>) -> Result<Option<f64>, String> {
        let Some(sample) = sample else {
            self.reset();
            return Ok(None);
        };
        let next = if let Some(previous) = self.value {
            crate::numeric::ema_step(sample, previous, self.period)?
        } else {
            self.seed.push(Some(sample))?;
            let Some(seed) = self.seed.mean()? else {
                return Ok(None);
            };
            self.seed.reset();
            seed
        };
        self.value = Some(next);
        Ok(Some(next))
    }
}
#[derive(Clone)]
struct MacdEvaluator {
    calculation: MomentumCalculation,
    fast: EmaKernel,
    slow: EmaKernel,
    signal: EmaKernel,
}
impl MacdEvaluator {
    fn new(calculation: MomentumCalculation, p: [usize; 3]) -> Result<Self, String> {
        Ok(Self {
            calculation,
            fast: EmaKernel::new(p[0])?,
            slow: EmaKernel::new(p[1])?,
            signal: EmaKernel::new(p[2])?,
        })
    }
}
impl MaterialEvaluator for MacdEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let sample = scalar(&inputs[0])?;
        let fast = self.fast.push(sample)?;
        let slow = self.slow.push(sample)?;
        let macd = fast.zip(slow).map(|(f, s)| f - s);
        let signal = self.signal.push(macd)?;
        Ok(price(match self.calculation {
            MomentumCalculation::Macd => macd,
            MomentumCalculation::MacdSignal => signal,
            _ => macd.zip(signal).map(|(m, s)| m - s),
        }))
    }
}

#[derive(Clone)]
struct CciEvaluator {
    source: SourceId,
    values: crate::numeric::ObservedWindow,
}
impl MaterialEvaluator for CciEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let b = bar(&self.source, context)?;
        self.values.push(Some((b.high + b.low + b.close) / 3.0))?;
        if !self.values.complete() {
            return Ok(Value::Missing(ScalarType::Number));
        }
        let values = self
            .values
            .chronological()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        let mean = crate::numeric::stable_mean(values.iter().copied())?;
        let mad = crate::numeric::stable_mean(values.iter().map(|v| (v - mean).abs()))?;
        Ok(if mad == 0.0 {
            Value::Missing(ScalarType::Number)
        } else {
            Value::Number((values.last().unwrap() - mean) / (0.015 * mad))
        })
    }
}

#[derive(Clone)]
struct DmiEvaluator {
    calculation: MomentumCalculation,
    source: SourceId,
    period: usize,
    previous: Option<CompletedBar>,
    plus_seed: crate::numeric::ObservedWindow,
    minus_seed: crate::numeric::ObservedWindow,
    tr_seed: crate::numeric::ObservedWindow,
    adx_seed: crate::numeric::ObservedWindow,
    plus: Option<f64>,
    minus: Option<f64>,
    tr: Option<f64>,
    adx: Option<f64>,
}
impl DmiEvaluator {
    fn new(
        calculation: MomentumCalculation,
        period: usize,
        source: SourceId,
    ) -> Result<Self, String> {
        Ok(Self {
            calculation,
            source,
            period,
            previous: None,
            plus_seed: crate::numeric::ObservedWindow::new(period)?,
            minus_seed: crate::numeric::ObservedWindow::new(period)?,
            tr_seed: crate::numeric::ObservedWindow::new(period)?,
            adx_seed: crate::numeric::ObservedWindow::new(period)?,
            plus: None,
            minus: None,
            tr: None,
            adx: None,
        })
    }
}
impl MaterialEvaluator for DmiEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let current = bar(&self.source, context)?.clone();
        let Some(previous) = self.previous.replace(current.clone()) else {
            return Ok(Value::Missing(
                if self.calculation == MomentumCalculation::Adx
                    || self.calculation == MomentumCalculation::PlusDi
                    || self.calculation == MomentumCalculation::MinusDi
                    || self.calculation == MomentumCalculation::DiDifference
                    || self.calculation == MomentumCalculation::Dx
                {
                    ScalarType::Percent
                } else {
                    ScalarType::Number
                },
            ));
        };
        let up = current.high - previous.high;
        let down = previous.low - current.low;
        let plus_dm = if up > down && up > 0.0 { up } else { 0.0 };
        let minus_dm = if down > up && down > 0.0 { down } else { 0.0 };
        let tr = (current.high - current.low)
            .max((current.high - previous.close).abs())
            .max((current.low - previous.close).abs());
        let (p, m, t) = if let (Some(p), Some(m), Some(t)) = (self.plus, self.minus, self.tr) {
            (
                crate::numeric::rma_step(plus_dm, p, self.period)?,
                crate::numeric::rma_step(minus_dm, m, self.period)?,
                crate::numeric::rma_step(tr, t, self.period)?,
            )
        } else {
            self.plus_seed.push(Some(plus_dm))?;
            self.minus_seed.push(Some(minus_dm))?;
            self.tr_seed.push(Some(tr))?;
            let (Some(p), Some(m), Some(t)) = (
                self.plus_seed.mean()?,
                self.minus_seed.mean()?,
                self.tr_seed.mean()?,
            ) else {
                return Ok(Value::Missing(ScalarType::Percent));
            };
            (p, m, t)
        };
        self.plus = Some(p);
        self.minus = Some(m);
        self.tr = Some(t);
        let (plus_di, minus_di) = if t == 0.0 {
            (0.0, 0.0)
        } else {
            (100.0 * p / t, 100.0 * m / t)
        };
        let sum = plus_di + minus_di;
        let dx = if sum == 0.0 {
            0.0
        } else {
            100.0 * (plus_di - minus_di).abs() / sum
        };
        let adx = if let Some(previous) = self.adx {
            Some(crate::numeric::rma_step(dx, previous, self.period)?)
        } else {
            self.adx_seed.push(Some(dx))?;
            self.adx_seed.mean()?
        };
        if let Some(value) = adx {
            self.adx = Some(value)
        };
        Ok(percent(match self.calculation {
            MomentumCalculation::PlusDi => Some(plus_di),
            MomentumCalculation::MinusDi => Some(minus_di),
            MomentumCalculation::DiDifference => Some(plus_di - minus_di),
            MomentumCalculation::Dx => Some(dx),
            MomentumCalculation::Adx => adx,
            _ => None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompletedBarUpdate, StrategyInput};
    use chrono::NaiveDateTime;

    fn b(open: f64, high: f64, low: f64, close: f64) -> CompletedBar {
        CompletedBar {
            open,
            high,
            low,
            close,
            volume: Some(1.0),
        }
    }
    fn input(source: &SourceId, bar: CompletedBar) -> StrategyInput {
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
    fn params(source: &SourceId, values: &[(&str, i64)]) -> MaterialArgs {
        let mut result = vec![("source", MaterialArg::Source(source.clone()))];
        for (name, value) in values {
            result.push((*name, MaterialArg::Integer(*value)))
        }
        MaterialArgs::new(result)
    }
    fn scalar_outputs(
        key: &'static str,
        values: &[(&str, i64)],
        samples: &[Value],
        ty: ValueType,
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut evaluator = MomentumFactory { key }
            .build(&params(&source, values), &[ty])
            .unwrap()
            .evaluator;
        samples
            .iter()
            .cloned()
            .map(|sample| {
                let i = input(&source, b(1.0, 1.0, 1.0, 1.0));
                evaluator
                    .evaluate(
                        &[sample],
                        &MaterialEvalContext {
                            input: &i,
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
    fn bar_outputs(key: &'static str, values: &[(&str, i64)], bars: &[CompletedBar]) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut evaluator = MomentumFactory { key }
            .build(&params(&source, values), &[])
            .unwrap()
            .evaluator;
        bars.iter()
            .cloned()
            .map(|value| {
                let i = input(&source, value);
                evaluator
                    .evaluate(
                        &[],
                        &MaterialEvalContext {
                            input: &i,
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

    #[test]
    fn wilder_rsi_uses_absolute_changes_and_flat_conventions() {
        let samples = [10.0, 11.0, 10.0, 12.0, 11.0].map(Value::Price);
        let output = scalar_outputs(
            MATERIAL_STRICT_RSI,
            &[("period", 3)],
            &samples,
            ValueType::optional(ScalarType::Price),
        );
        assert_eq!(
            output[..3],
            [
                Value::Missing(ScalarType::Percent),
                Value::Missing(ScalarType::Percent),
                Value::Missing(ScalarType::Percent)
            ]
        );
        assert_eq!(output[3], Value::Percent(75.0));
        assert_eq!(
            scalar_outputs(
                MATERIAL_RSI_CHANGE,
                &[("horizon", 2)],
                &[
                    Value::Percent(10.0),
                    Value::Percent(25.0),
                    Value::Percent(20.0)
                ],
                ValueType::optional(ScalarType::Percent),
            )[2],
            Value::Percent(10.0)
        );
        assert!(
            (match output[4] {
                Value::Percent(value) => value,
                _ => unreachable!(),
            } - 600.0 / 11.0)
                .abs()
                < 1e-12
        );
        for (expected, prices) in [
            (50.0, [2.0, 2.0, 2.0, 2.0]),
            (100.0, [1.0, 2.0, 3.0, 4.0]),
            (0.0, [4.0, 3.0, 2.0, 1.0]),
        ] {
            assert_eq!(
                scalar_outputs(
                    MATERIAL_STRICT_RSI,
                    &[("period", 3)],
                    &prices.map(Value::Price),
                    ValueType::optional(ScalarType::Price)
                )[3],
                Value::Percent(expected)
            );
        }
    }

    #[test]
    fn stochastic_slow_k_and_slow_d_apply_distinct_smoothing_stages() {
        let bars = [0.0, 60.0, 90.0, 30.0, 60.0].map(|close| b(close, 100.0, 0.0, close));
        let slow_k = bar_outputs(
            MATERIAL_STOCHASTIC_SLOW_K,
            &[("period", 1), ("smooth_k", 3), ("smooth_d", 3)],
            &bars,
        );
        assert_eq!(
            slow_k[2..],
            [
                Value::Percent(50.0),
                Value::Percent(60.0),
                Value::Percent(60.0)
            ]
        );
        let slow_d = bar_outputs(
            MATERIAL_STOCHASTIC_SLOW_D,
            &[("period", 1), ("smooth_k", 3), ("smooth_d", 3)],
            &bars,
        );
        assert!(
            (match slow_d[4] {
                Value::Percent(value) => value,
                _ => unreachable!(),
            } - 170.0 / 3.0)
                .abs()
                < 1e-12
        );
        let flat = bar_outputs(
            MATERIAL_STOCHASTIC_FAST_K,
            &[("period", 2)],
            &[b(1.0, 1.0, 1.0, 1.0), b(1.0, 1.0, 1.0, 1.0)],
        );
        assert_eq!(flat[1], Value::Missing(ScalarType::Percent));
    }

    #[test]
    fn macd_uses_seeded_price_emas_and_separate_signal_seed() {
        let samples = [1.0, 2.0, 3.0, 4.0].map(Value::Price);
        let args = [("fast", 2), ("slow", 3), ("signal", 2)];
        assert_eq!(
            scalar_outputs(
                MATERIAL_MACD,
                &args,
                &samples,
                ValueType::optional(ScalarType::Price)
            )[2..],
            [Value::Price(0.5), Value::Price(0.5)]
        );
        assert_eq!(
            scalar_outputs(
                MATERIAL_MACD_SIGNAL,
                &args,
                &samples,
                ValueType::optional(ScalarType::Price)
            )[3],
            Value::Price(0.5)
        );
        assert_eq!(
            scalar_outputs(
                MATERIAL_MACD_HISTOGRAM,
                &args,
                &samples,
                ValueType::optional(ScalarType::Price)
            )[3],
            Value::Price(0.0)
        );
    }

    #[test]
    fn cci_uses_current_window_mean_absolute_deviation() {
        let bars = [1.0, 2.0, 4.0].map(|value| b(value, value, value, value));
        let output = bar_outputs(MATERIAL_CCI, &[("period", 3)], &bars);
        assert!(
            (match output[2] {
                Value::Number(value) => value,
                _ => unreachable!(),
            } - 100.0)
                .abs()
                < 1e-12
        );
        assert_eq!(
            bar_outputs(
                MATERIAL_CCI,
                &[("period", 3)],
                &std::array::from_fn::<_, 3, _>(|_| b(1.0, 1.0, 1.0, 1.0))
            )[2],
            Value::Missing(ScalarType::Number)
        );
    }

    #[test]
    fn dmi_and_adx_have_separate_seed_windows() {
        let bars = [
            b(0.5, 1.0, 0.1, 0.5),
            b(1.0, 2.0, 0.1, 1.5),
            b(2.0, 3.0, 0.1, 2.5),
            b(3.0, 4.0, 0.1, 3.5),
        ];
        let plus = bar_outputs(MATERIAL_PLUS_DI, &[("period", 2)], &bars);
        let plus_value = match plus[2] {
            Value::Percent(value) => value,
            _ => panic!("plus DI did not become valid"),
        };
        assert!(plus_value > 0.0 && plus_value < 100.0);
        assert_eq!(
            bar_outputs(MATERIAL_MINUS_DI, &[("period", 2)], &bars)[2],
            Value::Percent(0.0)
        );
        assert_eq!(
            bar_outputs(MATERIAL_DI_DIFFERENCE, &[("period", 2)], &bars)[2],
            Value::Percent(plus_value)
        );
        assert_eq!(
            bar_outputs(MATERIAL_DX, &[("period", 2)], &bars)[2],
            Value::Percent(100.0)
        );
        let adx = bar_outputs(MATERIAL_ADX, &[("period", 2)], &bars);
        assert_eq!(adx[2], Value::Missing(ScalarType::Percent));
        assert_eq!(adx[3], Value::Percent(100.0));
        let flat = std::array::from_fn::<_, 4, _>(|_| b(1.0, 1.0, 1.0, 1.0));
        assert_eq!(
            bar_outputs(MATERIAL_ADX, &[("period", 2)], &flat)[3],
            Value::Percent(0.0)
        );
    }

    #[test]
    fn momentum_descriptors_cover_every_registered_output_with_bounded_state() {
        for key in KEYS {
            let values = match *key {
                MATERIAL_RSI_CHANGE => vec![("horizon", 3)],
                MATERIAL_MACD | MATERIAL_MACD_SIGNAL | MATERIAL_MACD_HISTOGRAM => {
                    vec![("fast", 2), ("slow", 3), ("signal", 2)]
                }
                MATERIAL_STOCHASTIC_FAST_K
                | MATERIAL_STOCHASTIC_SLOW_K
                | MATERIAL_STOCHASTIC_SLOW_D => {
                    vec![("period", 3), ("smooth_k", 2), ("smooth_d", 2)]
                }
                _ => vec![("period", 3)],
            };
            let source = SourceId::new("bars").unwrap();
            let scalar = matches!(
                *key,
                MATERIAL_STRICT_RSI
                    | MATERIAL_MACD
                    | MATERIAL_MACD_SIGNAL
                    | MATERIAL_MACD_HISTOGRAM
                    | MATERIAL_RSI_CHANGE
            );
            let ty = if *key == MATERIAL_RSI_CHANGE {
                ScalarType::Percent
            } else {
                ScalarType::Price
            };
            let inputs = if scalar {
                vec![ValueType::optional(ty)]
            } else {
                vec![]
            };
            let descriptor = MomentumFactory { key }
                .numeric_descriptor(&params(&source, &values), &inputs)
                .unwrap()
                .unwrap();
            descriptor.validate(&inputs).unwrap();
            assert!(
                descriptor.max_state_bytes <= crate::MAX_MATERIAL_STATE_BYTES,
                "{key}"
            );
        }
    }
}
