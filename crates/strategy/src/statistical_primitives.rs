use std::sync::Arc;

use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    BarField, CompletedBar, CompletedBarRequirement, MaterialArg, MaterialArgs, NumericCalculation,
    NumericDescriptor, NumericInputs, NumericMissingPolicy, NumericRange, NumericUnit, ScalarType,
    SourceId, StatisticalCalculation, Value, ValueType,
};

pub const MATERIAL_DONCHIAN_UPPER: &str = "donchian_upper";
pub const MATERIAL_DONCHIAN_LOWER: &str = "donchian_lower";
pub const MATERIAL_CHANNEL_WIDTH: &str = "channel_width";
pub const MATERIAL_CHANNEL_MID_DISTANCE: &str = "channel_mid_distance";
pub const MATERIAL_RANGE_POSITION: &str = "range_position";
pub const MATERIAL_PREVIOUS_RANGE_POSITION: &str = "previous_range_position";
pub const MATERIAL_ZSCORE: &str = "zscore";
pub const MATERIAL_BOLLINGER_UPPER: &str = "bollinger_upper";
pub const MATERIAL_BOLLINGER_LOWER: &str = "bollinger_lower";
pub const MATERIAL_BOLLINGER_PERCENT_B: &str = "bollinger_percent_b";
pub const MATERIAL_BOLLINGER_WIDTH: &str = "bollinger_width";
pub const MATERIAL_ROLLING_MEDIAN: &str = "rolling_median";
pub const MATERIAL_MEDIAN_DEVIATION: &str = "median_deviation";
pub const MATERIAL_REALIZED_VARIANCE: &str = "realized_variance";
pub const MATERIAL_REALIZED_VOLATILITY: &str = "realized_volatility";
pub const MATERIAL_RETURN_RMS: &str = "return_rms";
pub const MATERIAL_RETURN_STDDEV: &str = "return_stddev";
pub const MATERIAL_EWMA_VOLATILITY: &str = "ewma_volatility";
pub const MATERIAL_POSITIVE_SEMIVARIANCE: &str = "positive_semivariance";
pub const MATERIAL_NEGATIVE_SEMIVARIANCE: &str = "negative_semivariance";
pub const MATERIAL_VOLATILITY_ASYMMETRY: &str = "volatility_asymmetry";
pub const MATERIAL_HISTORICAL_PERCENTILE: &str = "historical_percentile";
pub const MATERIAL_EFFICIENCY_RATIO: &str = "efficiency_ratio";
pub const MATERIAL_REGRESSION_SLOPE: &str = "regression_slope";
pub const MATERIAL_REGRESSION_R2: &str = "regression_r2";
pub const MATERIAL_REGRESSION_RESIDUAL_RMS: &str = "regression_residual_rms";
pub const MATERIAL_REGRESSION_DEVIATION: &str = "regression_deviation";
pub const MATERIAL_ROLLING_HIGH_AGE: &str = "rolling_high_age";
pub const MATERIAL_ROLLING_LOW_AGE: &str = "rolling_low_age";
pub const MATERIAL_AROON_UP: &str = "aroon_up";
pub const MATERIAL_AROON_DOWN: &str = "aroon_down";
pub const MATERIAL_CHOPPINESS: &str = "choppiness";
pub const MATERIAL_RETURN_AUTOCORRELATION: &str = "return_autocorrelation";
pub const MATERIAL_RANGE_EXPANSION: &str = "range_expansion";
pub const MATERIAL_ATR_PERCENT: &str = "atr_percent";
pub const MATERIAL_ATR_RATIO: &str = "atr_ratio";
pub const MATERIAL_ATR_CHANGE: &str = "atr_change";
const KEYS: &[&str] = &[
    MATERIAL_DONCHIAN_UPPER,
    MATERIAL_DONCHIAN_LOWER,
    MATERIAL_CHANNEL_WIDTH,
    MATERIAL_CHANNEL_MID_DISTANCE,
    MATERIAL_RANGE_POSITION,
    MATERIAL_PREVIOUS_RANGE_POSITION,
    MATERIAL_ZSCORE,
    MATERIAL_BOLLINGER_UPPER,
    MATERIAL_BOLLINGER_LOWER,
    MATERIAL_BOLLINGER_PERCENT_B,
    MATERIAL_BOLLINGER_WIDTH,
    MATERIAL_ROLLING_MEDIAN,
    MATERIAL_MEDIAN_DEVIATION,
    MATERIAL_REALIZED_VARIANCE,
    MATERIAL_REALIZED_VOLATILITY,
    MATERIAL_RETURN_RMS,
    MATERIAL_RETURN_STDDEV,
    MATERIAL_EWMA_VOLATILITY,
    MATERIAL_POSITIVE_SEMIVARIANCE,
    MATERIAL_NEGATIVE_SEMIVARIANCE,
    MATERIAL_VOLATILITY_ASYMMETRY,
    MATERIAL_HISTORICAL_PERCENTILE,
    MATERIAL_EFFICIENCY_RATIO,
    MATERIAL_REGRESSION_SLOPE,
    MATERIAL_REGRESSION_R2,
    MATERIAL_REGRESSION_RESIDUAL_RMS,
    MATERIAL_REGRESSION_DEVIATION,
    MATERIAL_ROLLING_HIGH_AGE,
    MATERIAL_ROLLING_LOW_AGE,
    MATERIAL_AROON_UP,
    MATERIAL_AROON_DOWN,
    MATERIAL_CHOPPINESS,
    MATERIAL_RETURN_AUTOCORRELATION,
    MATERIAL_RANGE_EXPANSION,
    MATERIAL_ATR_PERCENT,
    MATERIAL_ATR_RATIO,
    MATERIAL_ATR_CHANGE,
];
const MAX_PERIOD: i64 = 512;
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
            max: MAX_PERIOD,
        },
        required: true,
    },
];
const SOURCE_PERIOD_MULTIPLIER: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "multiplier",
        kind: ParamKind::Number {
            min: f64::MIN_POSITIVE,
            max: 1000.0,
        },
        required: false,
    },
];
const SOURCE_ONLY: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
const SOURCE_LAMBDA: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "lambda",
        kind: ParamKind::Number {
            min: 0.0,
            max: 0.999999999999,
        },
        required: false,
    },
];
const SOURCE_PERIOD_LAG: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 2,
            max: MAX_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "lag",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: false,
    },
];
const SOURCE_SHORT_LONG: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "short",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "long",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: true,
    },
];
const SOURCE_PERIOD_HORIZON: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: true,
    },
    ParamSpec {
        name: "horizon",
        kind: ParamKind::Integer {
            min: 1,
            max: MAX_PERIOD,
        },
        required: true,
    },
];
const HLC: &[BarField] = &[BarField::High, BarField::Low, BarField::Close];

pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(StatFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}
struct StatFactory {
    key: &'static str,
}
impl StatFactory {
    fn source(p: &MaterialArgs) -> Result<SourceId, String> {
        match p.get("source") {
            Some(MaterialArg::Source(v)) => Ok(v.clone()),
            _ => Err("statistical primitive requires source".into()),
        }
    }
    fn int(p: &MaterialArgs, n: &str, d: Option<usize>) -> Result<usize, String> {
        match p.get(n) {
            Some(MaterialArg::Integer(v)) => usize::try_from(*v)
                .ok()
                .filter(|v| (1..=MAX_PERIOD as usize).contains(v))
                .ok_or_else(|| format!("{n} out of bounds")),
            None => d.ok_or_else(|| format!("missing {n}")),
            _ => Err(format!("invalid {n}")),
        }
    }
    fn number(p: &MaterialArgs, n: &str, d: f64) -> Result<f64, String> {
        match p.get(n) {
            Some(MaterialArg::Number(v)) if v.is_finite() => Ok(*v),
            None => Ok(d),
            _ => Err(format!("invalid {n}")),
        }
    }
    fn calc(
        &self,
        p: &MaterialArgs,
    ) -> Result<(StatisticalCalculation, [usize; 3], Option<f64>, usize), String> {
        let n = || Self::int(p, "period", None);
        Ok(match self.key {
            MATERIAL_DONCHIAN_UPPER => (
                StatisticalCalculation::DonchianUpper,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_DONCHIAN_LOWER => (
                StatisticalCalculation::DonchianLower,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_CHANNEL_WIDTH => (
                StatisticalCalculation::ChannelWidth,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_CHANNEL_MID_DISTANCE => (
                StatisticalCalculation::ChannelMidDistance,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_RANGE_POSITION => (
                StatisticalCalculation::RangePosition,
                [n()?, 0, 0],
                None,
                n()?,
            ),
            MATERIAL_PREVIOUS_RANGE_POSITION => (
                StatisticalCalculation::PreviousRangePosition,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_ZSCORE => (StatisticalCalculation::ZScore, [n()?, 0, 0], None, n()?),
            MATERIAL_BOLLINGER_UPPER => (
                StatisticalCalculation::BollingerUpper,
                [n()?, 0, 0],
                Some(Self::number(p, "multiplier", 2.0)?),
                n()?,
            ),
            MATERIAL_BOLLINGER_LOWER => (
                StatisticalCalculation::BollingerLower,
                [n()?, 0, 0],
                Some(Self::number(p, "multiplier", 2.0)?),
                n()?,
            ),
            MATERIAL_BOLLINGER_PERCENT_B => (
                StatisticalCalculation::BollingerPercentB,
                [n()?, 0, 0],
                Some(Self::number(p, "multiplier", 2.0)?),
                n()?,
            ),
            MATERIAL_BOLLINGER_WIDTH => (
                StatisticalCalculation::BollingerWidth,
                [n()?, 0, 0],
                Some(Self::number(p, "multiplier", 2.0)?),
                n()?,
            ),
            MATERIAL_ROLLING_MEDIAN => (
                StatisticalCalculation::RollingMedian,
                [n()?, 0, 0],
                None,
                n()?,
            ),
            MATERIAL_MEDIAN_DEVIATION => (
                StatisticalCalculation::MedianDeviation,
                [n()?, 0, 0],
                None,
                n()?,
            ),
            MATERIAL_REALIZED_VARIANCE => (
                StatisticalCalculation::RealizedVariance,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_REALIZED_VOLATILITY => (
                StatisticalCalculation::RealizedVolatility,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_RETURN_RMS => (
                StatisticalCalculation::ReturnRms,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_RETURN_STDDEV => (
                StatisticalCalculation::ReturnStdDev,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_EWMA_VOLATILITY => (
                StatisticalCalculation::EwmaVolatility,
                [1, 0, 0],
                Some(Self::number(p, "lambda", 0.94)?),
                2,
            ),
            MATERIAL_POSITIVE_SEMIVARIANCE => (
                StatisticalCalculation::PositiveSemivariance,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_NEGATIVE_SEMIVARIANCE => (
                StatisticalCalculation::NegativeSemivariance,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_VOLATILITY_ASYMMETRY => (
                StatisticalCalculation::VolatilityAsymmetry,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_HISTORICAL_PERCENTILE => (
                StatisticalCalculation::HistoricalPercentile,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_EFFICIENCY_RATIO => (
                StatisticalCalculation::EfficiencyRatio,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_REGRESSION_SLOPE => {
                let value = n()?;
                if value < 2 {
                    return Err("regression period must be at least 2".into());
                }
                (
                    StatisticalCalculation::RegressionSlope,
                    [value, 0, 0],
                    None,
                    value,
                )
            }
            MATERIAL_REGRESSION_R2 => {
                let value = n()?;
                if value < 2 {
                    return Err("regression period must be at least 2".into());
                }
                (
                    StatisticalCalculation::RegressionR2,
                    [value, 0, 0],
                    None,
                    value,
                )
            }
            MATERIAL_REGRESSION_RESIDUAL_RMS => {
                let value = n()?;
                if value < 2 {
                    return Err("regression period must be at least 2".into());
                }
                (
                    StatisticalCalculation::RegressionResidualRms,
                    [value, 0, 0],
                    None,
                    value,
                )
            }
            MATERIAL_REGRESSION_DEVIATION => {
                let value = n()?;
                if value < 2 {
                    return Err("regression period must be at least 2".into());
                }
                (
                    StatisticalCalculation::RegressionDeviation,
                    [value, 0, 0],
                    None,
                    value,
                )
            }
            MATERIAL_ROLLING_HIGH_AGE => (
                StatisticalCalculation::RollingHighAge,
                [n()?, 0, 0],
                None,
                n()?,
            ),
            MATERIAL_ROLLING_LOW_AGE => (
                StatisticalCalculation::RollingLowAge,
                [n()?, 0, 0],
                None,
                n()?,
            ),
            MATERIAL_AROON_UP => (
                StatisticalCalculation::AroonUp,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_AROON_DOWN => (
                StatisticalCalculation::AroonDown,
                [n()?, 0, 0],
                None,
                n()? + 1,
            ),
            MATERIAL_CHOPPINESS => {
                let v = n()?;
                if v < 2 {
                    return Err("choppiness period must be at least 2".into());
                }
                (StatisticalCalculation::Choppiness, [v, 0, 0], None, v + 1)
            }
            MATERIAL_RETURN_AUTOCORRELATION => {
                let v = n()?;
                if v < 2 {
                    return Err("autocorrelation period must be at least 2".into());
                }
                let l = Self::int(p, "lag", Some(1))?;
                (
                    StatisticalCalculation::ReturnAutocorrelation,
                    [v, l, 0],
                    None,
                    v + l + 1,
                )
            }
            MATERIAL_RANGE_EXPANSION => {
                (StatisticalCalculation::RangeExpansion, [1, 0, 0], None, 2)
            }
            MATERIAL_ATR_PERCENT => {
                let v = n()?;
                (StatisticalCalculation::AtrPercent, [v, 0, 0], None, v)
            }
            MATERIAL_ATR_RATIO => {
                let s = Self::int(p, "short", None)?;
                let l = Self::int(p, "long", None)?;
                (StatisticalCalculation::AtrRatio, [s, l, 0], None, s.max(l))
            }
            MATERIAL_ATR_CHANGE => {
                let v = n()?;
                let h = Self::int(p, "horizon", None)?;
                (StatisticalCalculation::AtrChange, [v, h, 0], None, v + h)
            }
            _ => return Err("unknown statistical primitive".into()),
        })
    }
    fn scalar(c: StatisticalCalculation) -> bool {
        matches!(
            c,
            StatisticalCalculation::ZScore
                | StatisticalCalculation::BollingerUpper
                | StatisticalCalculation::BollingerLower
                | StatisticalCalculation::BollingerPercentB
                | StatisticalCalculation::BollingerWidth
                | StatisticalCalculation::RollingMedian
                | StatisticalCalculation::MedianDeviation
                | StatisticalCalculation::RealizedVariance
                | StatisticalCalculation::RealizedVolatility
                | StatisticalCalculation::ReturnRms
                | StatisticalCalculation::ReturnStdDev
                | StatisticalCalculation::EwmaVolatility
                | StatisticalCalculation::PositiveSemivariance
                | StatisticalCalculation::NegativeSemivariance
                | StatisticalCalculation::VolatilityAsymmetry
                | StatisticalCalculation::HistoricalPercentile
                | StatisticalCalculation::EfficiencyRatio
                | StatisticalCalculation::RegressionSlope
                | StatisticalCalculation::RegressionR2
                | StatisticalCalculation::RegressionResidualRms
                | StatisticalCalculation::RegressionDeviation
                | StatisticalCalculation::ReturnAutocorrelation
        )
    }
}
impl MaterialFactory for StatFactory {
    fn numeric_descriptor(
        &self,
        p: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let source = Self::source(p)?;
        let (c, periods, param, first) = self.calc(p)?;
        let scalar = Self::scalar(c);
        if scalar && (inputs.len() != 1 || inputs[0].scalar != ScalarType::Price) {
            return Err("statistical scalar primitive requires one Price input".into());
        }
        if !scalar && !inputs.is_empty() {
            return Err("bar statistical primitive accepts no expression inputs".into());
        }
        let (output_type, unit, range) = match c {
            StatisticalCalculation::DonchianUpper
            | StatisticalCalculation::DonchianLower
            | StatisticalCalculation::ChannelWidth
            | StatisticalCalculation::ChannelMidDistance
            | StatisticalCalculation::BollingerUpper
            | StatisticalCalculation::BollingerLower
            | StatisticalCalculation::RollingMedian
            | StatisticalCalculation::RegressionResidualRms
            | StatisticalCalculation::RegressionDeviation => (
                ValueType::optional(ScalarType::Price),
                NumericUnit::Price,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::RegressionSlope => (
                ValueType::optional(ScalarType::PricePerObservation),
                NumericUnit::PricePerObservation,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::RollingHighAge | StatisticalCalculation::RollingLowAge => (
                ValueType::optional(ScalarType::Integer),
                NumericUnit::BarCount,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::AroonUp
            | StatisticalCalculation::AroonDown
            | StatisticalCalculation::AtrPercent
            | StatisticalCalculation::Choppiness => (
                ValueType::optional(ScalarType::Percent),
                NumericUnit::Percent,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::RealizedVariance
            | StatisticalCalculation::PositiveSemivariance
            | StatisticalCalculation::NegativeSemivariance => (
                ValueType::optional(ScalarType::LogReturnVariance),
                NumericUnit::LogReturnVariance,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::RealizedVolatility
            | StatisticalCalculation::ReturnRms
            | StatisticalCalculation::ReturnStdDev
            | StatisticalCalculation::EwmaVolatility => (
                ValueType::optional(ScalarType::LogReturn),
                NumericUnit::LogReturn,
                NumericRange::Unbounded,
            ),
            StatisticalCalculation::ZScore | StatisticalCalculation::RegressionR2 => (
                ValueType::optional(ScalarType::Number),
                NumericUnit::Number,
                NumericRange::Unbounded,
            ),
            _ => (
                ValueType::optional(ScalarType::Ratio),
                NumericUnit::Ratio,
                NumericRange::Unbounded,
            ),
        };
        let state = if scalar {
            crate::numeric::ObservedWindow::state_bytes(first)?
                + std::mem::size_of::<ScalarStatEvaluator>()
        } else {
            BarRing::state_bytes(first)?
                + crate::numeric::ObservedWindow::state_bytes(periods[0].max(1))?
                + crate::numeric::ObservedWindow::state_bytes(periods[1].max(1))?
                + crate::numeric::ObservedWindow::state_bytes(periods[1].saturating_add(1).max(1))?
                + std::mem::size_of::<BarStatEvaluator>()
        };
        if state > crate::MAX_MATERIAL_STATE_BYTES {
            return Err("statistical state exceeds material bound".into());
        }
        Ok(Some(NumericDescriptor {
            calculation: NumericCalculation::Statistical {
                calculation: c,
                periods,
                parameter: param.map(f64::to_bits),
            },
            source_clock: source,
            inputs: if scalar {
                NumericInputs::Scalar(inputs.to_vec())
            } else {
                NumericInputs::CompletedBarFields(HLC)
            },
            output_type,
            unit,
            range,
            missing: NumericMissingPolicy::ConsumeWindowSlot,
            first_output_observations: first,
            required_lookback: first,
            max_state_bytes: state,
            exact_aliases: &[],
        }))
    }
    fn params(&self) -> &[ParamSpec] {
        match self.key {
            MATERIAL_BOLLINGER_UPPER
            | MATERIAL_BOLLINGER_LOWER
            | MATERIAL_BOLLINGER_PERCENT_B
            | MATERIAL_BOLLINGER_WIDTH => &SOURCE_PERIOD_MULTIPLIER,
            MATERIAL_EWMA_VOLATILITY => &SOURCE_LAMBDA,
            MATERIAL_RETURN_AUTOCORRELATION => &SOURCE_PERIOD_LAG,
            MATERIAL_ATR_RATIO => &SOURCE_SHORT_LONG,
            MATERIAL_ATR_CHANGE => &SOURCE_PERIOD_HORIZON,
            MATERIAL_RANGE_EXPANSION => &SOURCE_ONLY,
            _ => &SOURCE_PERIOD,
        }
    }
    fn build(&self, p: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let d = self.numeric_descriptor(p, inputs)?.unwrap();
        let NumericCalculation::Statistical {
            calculation,
            periods,
            parameter,
        } = d.calculation
        else {
            unreachable!()
        };
        let evaluator: Box<dyn MaterialEvaluator> = if Self::scalar(calculation) {
            Box::new(ScalarStatEvaluator {
                calculation,
                periods,
                parameter: parameter.map(f64::from_bits),
                window: crate::numeric::ObservedWindow::new(d.first_output_observations)?,
                previous: None,
                ewma: None,
            })
        } else {
            Box::new(BarStatEvaluator::new(
                calculation,
                periods,
                d.first_output_observations,
                d.source_clock.clone(),
            )?)
        };
        let lookback = if Self::scalar(calculation) {
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
            evaluator: Box::new(StatClock { evaluator }),
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
struct StatClock {
    evaluator: Box<dyn MaterialEvaluator>,
}
impl MaterialEvaluator for StatClock {
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
            .map(|(i, v)| {
                if context.input_updates.get(i) == Some(&false) {
                    Value::Missing(v.scalar_type())
                } else {
                    v.clone()
                }
            })
            .collect::<Vec<_>>();
        self.evaluator.evaluate(&observed, context)
    }
}
fn scalar(v: &Value) -> Result<Option<f64>, String> {
    match v {
        Value::Missing(_) => Ok(None),
        Value::Price(v) if v.is_finite() => Ok(Some(*v)),
        _ => Err("statistical input must be finite Price".into()),
    }
}
fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        crate::numeric::stable_mean([v[n / 2 - 1], v[n / 2]]).unwrap()
    }
}
fn variance(v: &[f64], mean: f64) -> Result<f64, String> {
    crate::numeric::stable_mean(v.iter().map(|x| (x - mean) * (x - mean)))
}
#[derive(Clone)]
struct ScalarStatEvaluator {
    calculation: StatisticalCalculation,
    periods: [usize; 3],
    parameter: Option<f64>,
    window: crate::numeric::ObservedWindow,
    previous: Option<f64>,
    ewma: Option<f64>,
}
impl MaterialEvaluator for ScalarStatEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        let sample = scalar(&inputs[0])?;
        if self.calculation == StatisticalCalculation::EwmaVolatility {
            let Some(current) = sample else {
                self.previous = None;
                self.ewma = None;
                return Ok(Value::Missing(ScalarType::LogReturn));
            };
            let Some(previous) = self.previous.replace(current) else {
                return Ok(Value::Missing(ScalarType::LogReturn));
            };
            if current <= 0.0 || previous <= 0.0 {
                self.previous = None;
                self.ewma = None;
                return Ok(Value::Missing(ScalarType::LogReturn));
            }
            let r = current.ln() - previous.ln();
            let lambda = self.parameter.unwrap();
            let variance = self
                .ewma
                .map_or(r * r, |v| lambda * v + (1.0 - lambda) * r * r);
            self.ewma = Some(variance);
            return Ok(Value::LogReturn(variance.sqrt()));
        }
        self.window.push(sample)?;
        if !self.window.complete() || self.window.chronological().any(|v| v.is_none()) {
            return Ok(Value::Missing(output_scalar(self.calculation)));
        }
        let values = self
            .window
            .chronological()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        calculate_scalar(self.calculation, self.periods, self.parameter, &values)
    }
}
fn output_scalar(c: StatisticalCalculation) -> ScalarType {
    match c {
        StatisticalCalculation::BollingerUpper
        | StatisticalCalculation::BollingerLower
        | StatisticalCalculation::RollingMedian
        | StatisticalCalculation::RegressionResidualRms
        | StatisticalCalculation::RegressionDeviation => ScalarType::Price,
        StatisticalCalculation::RegressionSlope => ScalarType::PricePerObservation,
        StatisticalCalculation::RealizedVariance
        | StatisticalCalculation::PositiveSemivariance
        | StatisticalCalculation::NegativeSemivariance => ScalarType::LogReturnVariance,
        StatisticalCalculation::RealizedVolatility
        | StatisticalCalculation::ReturnRms
        | StatisticalCalculation::ReturnStdDev
        | StatisticalCalculation::EwmaVolatility => ScalarType::LogReturn,
        StatisticalCalculation::ZScore | StatisticalCalculation::RegressionR2 => ScalarType::Number,
        _ => ScalarType::Ratio,
    }
}
fn calculate_scalar(
    c: StatisticalCalculation,
    p: [usize; 3],
    parameter: Option<f64>,
    values: &[f64],
) -> Result<Value, String> {
    let missing = || Value::Missing(output_scalar(c));
    let ratio = |v| Value::Ratio(v);
    let returns = || -> Option<Vec<f64>> {
        values
            .windows(2)
            .map(|w| {
                if w[0] > 0.0 && w[1] > 0.0 {
                    Some(w[1].ln() - w[0].ln())
                } else {
                    None
                }
            })
            .collect()
    };
    Ok(match c {
        StatisticalCalculation::ZScore => {
            let m = crate::numeric::stable_mean(values.iter().copied())?;
            let s = variance(values, m)?.sqrt();
            if s == 0.0 {
                missing()
            } else {
                Value::Number((values.last().unwrap() - m) / s)
            }
        }
        StatisticalCalculation::BollingerUpper
        | StatisticalCalculation::BollingerLower
        | StatisticalCalculation::BollingerPercentB
        | StatisticalCalculation::BollingerWidth => {
            let m = crate::numeric::stable_mean(values.iter().copied())?;
            let s = variance(values, m)?.sqrt();
            let k = parameter.unwrap();
            let lower = m - k * s;
            let upper = m + k * s;
            match c {
                StatisticalCalculation::BollingerUpper => Value::Price(upper),
                StatisticalCalculation::BollingerLower => Value::Price(lower),
                StatisticalCalculation::BollingerPercentB => {
                    if upper == lower {
                        missing()
                    } else {
                        ratio((values.last().unwrap() - lower) / (upper - lower))
                    }
                }
                _ => {
                    if m == 0.0 {
                        missing()
                    } else {
                        ratio((upper - lower) / m.abs())
                    }
                }
            }
        }
        StatisticalCalculation::RollingMedian => Value::Price(median(values.to_vec())),
        StatisticalCalculation::MedianDeviation => {
            let m = median(values.to_vec());
            let mad = median(values.iter().map(|v| (v - m).abs()).collect());
            if mad == 0.0 {
                missing()
            } else {
                ratio((values.last().unwrap() - m) / mad)
            }
        }
        StatisticalCalculation::RealizedVariance
        | StatisticalCalculation::RealizedVolatility
        | StatisticalCalculation::ReturnRms
        | StatisticalCalculation::ReturnStdDev
        | StatisticalCalculation::PositiveSemivariance
        | StatisticalCalculation::NegativeSemivariance
        | StatisticalCalculation::VolatilityAsymmetry => {
            let Some(r) = returns() else {
                return Ok(missing());
            };
            let rv = r.iter().map(|v| v * v).sum::<f64>();
            let pos = r.iter().map(|v| v.max(0.0).powi(2)).sum::<f64>() / r.len() as f64;
            let neg = r.iter().map(|v| v.min(0.0).powi(2)).sum::<f64>() / r.len() as f64;
            match c {
                StatisticalCalculation::RealizedVariance => Value::LogReturnVariance(rv),
                StatisticalCalculation::RealizedVolatility => Value::LogReturn(rv.sqrt()),
                StatisticalCalculation::ReturnRms => Value::LogReturn((rv / r.len() as f64).sqrt()),
                StatisticalCalculation::ReturnStdDev => {
                    let m = crate::numeric::stable_mean(r.iter().copied())?;
                    Value::LogReturn(variance(&r, m)?.sqrt())
                }
                StatisticalCalculation::PositiveSemivariance => Value::LogReturnVariance(pos),
                StatisticalCalculation::NegativeSemivariance => Value::LogReturnVariance(neg),
                _ => {
                    if pos + neg == 0.0 {
                        missing()
                    } else {
                        ratio((neg - pos) / (neg + pos))
                    }
                }
            }
        }
        StatisticalCalculation::HistoricalPercentile => {
            let current = *values.last().unwrap();
            let prior = &values[..values.len() - 1];
            let lower = prior.iter().filter(|v| **v < current).count() as f64;
            let equal = prior.iter().filter(|v| **v == current).count() as f64;
            ratio((lower + 0.5 * equal) / prior.len() as f64)
        }
        StatisticalCalculation::EfficiencyRatio => {
            let movement = (values.last().unwrap() - values[0]).abs();
            let path = values.windows(2).map(|w| (w[1] - w[0]).abs()).sum::<f64>();
            ratio(if path == 0.0 { 0.0 } else { movement / path })
        }
        StatisticalCalculation::RegressionSlope
        | StatisticalCalculation::RegressionR2
        | StatisticalCalculation::RegressionResidualRms
        | StatisticalCalculation::RegressionDeviation => regression(c, values)?,
        StatisticalCalculation::ReturnAutocorrelation => {
            let Some(r) = returns() else {
                return Ok(missing());
            };
            let n = p[0];
            let lag = p[1];
            let a = &r[lag..lag + n];
            let b = &r[..n];
            let ma = crate::numeric::stable_mean(a.iter().copied())?;
            let mb = crate::numeric::stable_mean(b.iter().copied())?;
            let va = a.iter().map(|v| (v - ma).powi(2)).sum::<f64>();
            let vb = b.iter().map(|v| (v - mb).powi(2)).sum::<f64>();
            if va == 0.0 || vb == 0.0 {
                missing()
            } else {
                ratio(
                    a.iter()
                        .zip(b)
                        .map(|(x, y)| (x - ma) * (y - mb))
                        .sum::<f64>()
                        / (va * vb).sqrt(),
                )
            }
        }
        _ => return Err("scalar statistical calculation routed incorrectly".into()),
    })
}
fn regression(c: StatisticalCalculation, v: &[f64]) -> Result<Value, String> {
    let n = v.len() as f64;
    let mx = (n - 1.0) / 2.0;
    let my = crate::numeric::stable_mean(v.iter().copied())?;
    let denominator = (0..v.len()).map(|i| (i as f64 - mx).powi(2)).sum::<f64>();
    let slope = v
        .iter()
        .enumerate()
        .map(|(i, y)| (i as f64 - mx) * (y - my))
        .sum::<f64>()
        / denominator;
    let intercept = my - slope * mx;
    let residuals = v
        .iter()
        .enumerate()
        .map(|(i, y)| y - (intercept + slope * i as f64))
        .collect::<Vec<_>>();
    let sse = residuals.iter().map(|r| r * r).sum::<f64>();
    let sst = v.iter().map(|y| (y - my).powi(2)).sum::<f64>();
    Ok(match c {
        StatisticalCalculation::RegressionSlope => Value::PricePerObservation(slope),
        StatisticalCalculation::RegressionR2 => {
            if sst == 0.0 {
                Value::Missing(ScalarType::Number)
            } else {
                Value::Number(1.0 - sse / sst)
            }
        }
        StatisticalCalculation::RegressionResidualRms => Value::Price((sse / n).sqrt()),
        _ => Value::Price(*residuals.last().unwrap()),
    })
}

#[derive(Clone)]
struct BarRing {
    slots: Box<[Option<CompletedBar>]>,
    next: usize,
    len: usize,
}
impl BarRing {
    fn new(n: usize) -> Result<Self, String> {
        Self::state_bytes(n)?;
        Ok(Self {
            slots: vec![None; n].into_boxed_slice(),
            next: 0,
            len: 0,
        })
    }
    fn state_bytes(n: usize) -> Result<usize, String> {
        n.checked_mul(std::mem::size_of::<Option<CompletedBar>>())
            .and_then(|v| v.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| "bar stat bound overflow".into())
    }
    fn push(&mut self, b: CompletedBar) {
        self.slots[self.next] = Some(b);
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
    fn chronological(&self) -> impl Iterator<Item = &CompletedBar> {
        (0..self.len).rev().map(|a| self.get(a))
    }
}
#[derive(Clone)]
struct Atr {
    period: usize,
    previous: Option<f64>,
    seed: crate::numeric::ObservedWindow,
    value: Option<f64>,
}
impl Atr {
    fn new(n: usize) -> Result<Self, String> {
        Ok(Self {
            period: n,
            previous: None,
            seed: crate::numeric::ObservedWindow::new(n)?,
            value: None,
        })
    }
    fn push(&mut self, b: &CompletedBar) -> Result<Option<f64>, String> {
        let tr = self.previous.map_or(b.high - b.low, |p| {
            (b.high - b.low)
                .max((b.high - p).abs())
                .max((b.low - p).abs())
        });
        self.previous = Some(b.close);
        let value = if let Some(v) = self.value {
            Some(crate::numeric::rma_step(tr, v, self.period)?)
        } else {
            self.seed.push(Some(tr))?;
            self.seed.mean()?
        };
        if let Some(v) = value {
            self.value = Some(v)
        }
        Ok(value)
    }
}
#[derive(Clone)]
struct BarStatEvaluator {
    calculation: StatisticalCalculation,
    periods: [usize; 3],
    source: SourceId,
    bars: BarRing,
    atr_a: Atr,
    atr_b: Atr,
    atr_history: crate::numeric::ObservedWindow,
}
impl BarStatEvaluator {
    fn new(
        c: StatisticalCalculation,
        p: [usize; 3],
        first: usize,
        source: SourceId,
    ) -> Result<Self, String> {
        Ok(Self {
            calculation: c,
            periods: p,
            source,
            bars: BarRing::new(first)?,
            atr_a: Atr::new(p[0].max(1))?,
            atr_b: Atr::new(p[1].max(1))?,
            atr_history: crate::numeric::ObservedWindow::new(p[1].max(1) + 1)?,
        })
    }
}
impl MaterialEvaluator for BarStatEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let b = context
            .input
            .completed_bars
            .iter()
            .find(|u| u.source == self.source)
            .ok_or_else(|| "bar statistic missing source update".to_string())?
            .bar
            .clone();
        let atr_a = self.atr_a.push(&b)?;
        let atr_b = self.atr_b.push(&b)?;
        if self.calculation == StatisticalCalculation::AtrChange {
            self.atr_history.push(atr_a)?
        }
        self.bars.push(b);
        if !self.bars.complete() {
            return Ok(Value::Missing(bar_output(self.calculation)));
        }
        calculate_bar(self, self.calculation, atr_a, atr_b)
    }
}
fn bar_output(c: StatisticalCalculation) -> ScalarType {
    match c {
        StatisticalCalculation::DonchianUpper
        | StatisticalCalculation::DonchianLower
        | StatisticalCalculation::ChannelWidth
        | StatisticalCalculation::ChannelMidDistance => ScalarType::Price,
        StatisticalCalculation::RollingHighAge | StatisticalCalculation::RollingLowAge => {
            ScalarType::Integer
        }
        StatisticalCalculation::AroonUp
        | StatisticalCalculation::AroonDown
        | StatisticalCalculation::AtrPercent
        | StatisticalCalculation::Choppiness => ScalarType::Percent,
        _ => ScalarType::Ratio,
    }
}
fn calculate_bar(
    e: &BarStatEvaluator,
    c: StatisticalCalculation,
    atr_a: Option<f64>,
    atr_b: Option<f64>,
) -> Result<Value, String> {
    let current = e.bars.get(0);
    let n = e.periods[0];
    let ratio = |a: f64, b: f64| {
        if b == 0.0 {
            Value::Missing(ScalarType::Ratio)
        } else {
            Value::Ratio(a / b)
        }
    };
    let prior = || e.bars.chronological().take(e.bars.len - 1);
    Ok(match c {
        StatisticalCalculation::DonchianUpper => {
            Value::Price(prior().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max))
        }
        StatisticalCalculation::DonchianLower => {
            Value::Price(prior().map(|b| b.low).fold(f64::INFINITY, f64::min))
        }
        StatisticalCalculation::ChannelWidth
        | StatisticalCalculation::ChannelMidDistance
        | StatisticalCalculation::PreviousRangePosition => {
            let high = prior().map(|b| b.high).fold(f64::NEG_INFINITY, f64::max);
            let low = prior().map(|b| b.low).fold(f64::INFINITY, f64::min);
            if c == StatisticalCalculation::ChannelWidth {
                Value::Price(high - low)
            } else if c == StatisticalCalculation::ChannelMidDistance {
                Value::Price(current.close - (high + low) / 2.0)
            } else {
                ratio(current.close - low, high - low)
            }
        }
        StatisticalCalculation::RangePosition => {
            let high = e
                .bars
                .chronological()
                .map(|b| b.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let low = e
                .bars
                .chronological()
                .map(|b| b.low)
                .fold(f64::INFINITY, f64::min);
            ratio(current.close - low, high - low)
        }
        StatisticalCalculation::RollingHighAge => Value::Integer(e.bars.iter_ages_high() as i64),
        StatisticalCalculation::RollingLowAge => Value::Integer(e.bars.iter_ages_low() as i64),
        StatisticalCalculation::AroonUp => {
            Value::Percent(100.0 * (n - e.bars.iter_ages_high()) as f64 / n as f64)
        }
        StatisticalCalculation::AroonDown => {
            Value::Percent(100.0 * (n - e.bars.iter_ages_low()) as f64 / n as f64)
        }
        StatisticalCalculation::RangeExpansion => ratio(
            current.high - current.low,
            e.bars.get(1).high - e.bars.get(1).low,
        ),
        StatisticalCalculation::AtrPercent => match atr_a {
            Some(a) if current.close != 0.0 => Value::Percent(100.0 * a / current.close.abs()),
            _ => Value::Missing(ScalarType::Percent),
        },
        StatisticalCalculation::AtrRatio => match (atr_a, atr_b) {
            (Some(a), Some(b)) => ratio(a, b),
            _ => Value::Missing(ScalarType::Ratio),
        },
        StatisticalCalculation::AtrChange => {
            let v = e.atr_history.chronological().collect::<Vec<_>>();
            if v.len() < e.periods[1] + 1 || v.iter().any(Option::is_none) {
                Value::Missing(ScalarType::Ratio)
            } else {
                ratio(v.last().unwrap().unwrap() - v[0].unwrap(), v[0].unwrap())
            }
        }
        StatisticalCalculation::Choppiness => {
            let bars = e.bars.chronological().collect::<Vec<_>>();
            let high = bars[1..]
                .iter()
                .map(|b| b.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let low = bars[1..]
                .iter()
                .map(|b| b.low)
                .fold(f64::INFINITY, f64::min);
            let tr = bars
                .windows(2)
                .map(|w| {
                    (w[1].high - w[1].low)
                        .max((w[1].high - w[0].close).abs())
                        .max((w[1].low - w[0].close).abs())
                })
                .sum::<f64>();
            if high == low || tr <= 0.0 {
                Value::Missing(ScalarType::Percent)
            } else {
                Value::Percent(100.0 * (tr / (high - low)).log10() / (n as f64).log10())
            }
        }
        _ => return Err("bar statistical calculation routed incorrectly".into()),
    })
}
impl BarRing {
    fn iter_ages_high(&self) -> usize {
        (0..self.len)
            .find(|age| {
                self.get(*age).high
                    == (0..self.len)
                        .map(|a| self.get(a).high)
                        .fold(f64::NEG_INFINITY, f64::max)
            })
            .unwrap()
    }
    fn iter_ages_low(&self) -> usize {
        (0..self.len)
            .find(|age| {
                self.get(*age).low
                    == (0..self.len)
                        .map(|a| self.get(a).low)
                        .fold(f64::INFINITY, f64::min)
            })
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompletedBarUpdate, StrategyInput};
    use chrono::NaiveDateTime;
    fn b(o: f64, h: f64, l: f64, c: f64) -> CompletedBar {
        CompletedBar {
            open: o,
            high: h,
            low: l,
            close: c,
            volume: Some(1.0),
        }
    }
    fn p(source: &SourceId, values: &[(&str, MaterialArg)]) -> MaterialArgs {
        let mut all = vec![("source", MaterialArg::Source(source.clone()))];
        all.extend(values.iter().cloned());
        MaterialArgs::new(all)
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
    fn scalar_values(
        key: &'static str,
        args: &[(&str, MaterialArg)],
        values: &[f64],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut e = StatFactory { key }
            .build(&p(&source, args), &[ValueType::optional(ScalarType::Price)])
            .unwrap()
            .evaluator;
        values
            .iter()
            .map(|value| {
                let i = input(&source, b(1.0, 1.0, 1.0, 1.0));
                e.evaluate(
                    &[Value::Price(*value)],
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
    fn bar_values(
        key: &'static str,
        args: &[(&str, MaterialArg)],
        bars: &[CompletedBar],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut e = StatFactory { key }
            .build(&p(&source, args), &[])
            .unwrap()
            .evaluator;
        bars.iter()
            .cloned()
            .map(|value| {
                let i = input(&source, value);
                e.evaluate(
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
    fn period(n: i64) -> [(&'static str, MaterialArg); 1] {
        [("period", MaterialArg::Integer(n))]
    }

    #[test]
    fn channels_exclude_current_and_positions_preserve_out_of_range() {
        let bars = [
            b(2.0, 5.0, 1.0, 4.0),
            b(3.0, 6.0, 2.0, 5.0),
            b(4.0, 7.0, 3.0, 8.0),
        ];
        assert_eq!(
            bar_values(MATERIAL_DONCHIAN_UPPER, &period(2), &bars)[2],
            Value::Price(6.0)
        );
        assert_eq!(
            bar_values(MATERIAL_DONCHIAN_LOWER, &period(2), &bars)[2],
            Value::Price(1.0)
        );
        assert_eq!(
            bar_values(MATERIAL_CHANNEL_WIDTH, &period(2), &bars)[2],
            Value::Price(5.0)
        );
        assert_eq!(
            bar_values(MATERIAL_CHANNEL_MID_DISTANCE, &period(2), &bars)[2],
            Value::Price(4.5)
        );
        assert_eq!(
            bar_values(MATERIAL_PREVIOUS_RANGE_POSITION, &period(2), &bars)[2],
            Value::Ratio(7.0 / 5.0)
        );
        assert_eq!(
            bar_values(MATERIAL_RANGE_POSITION, &period(3), &bars)[2],
            Value::Ratio(7.0 / 6.0)
        );
    }

    #[test]
    fn deviation_median_and_bollinger_use_current_complete_window() {
        let values = [1.0, 2.0, 3.0];
        let z = scalar_values(MATERIAL_ZSCORE, &period(3), &values);
        assert!(
            (match z[2] {
                Value::Number(v) => v,
                _ => unreachable!(),
            } - (3.0 / 2.0f64).sqrt())
            .abs()
                < 1e-12
        );
        let upper = scalar_values(
            MATERIAL_BOLLINGER_UPPER,
            &[
                ("period", MaterialArg::Integer(3)),
                ("multiplier", MaterialArg::Number(2.0)),
            ],
            &values,
        );
        let deviation = (2.0f64 / 3.0).sqrt();
        assert!(
            (match upper[2] {
                Value::Price(v) => v,
                _ => unreachable!(),
            } - (2.0 + 2.0 * deviation))
                .abs()
                < 1e-12
        );
        let band_args = [
            ("period", MaterialArg::Integer(3)),
            ("multiplier", MaterialArg::Number(2.0)),
        ];
        assert!(matches!(
            scalar_values(MATERIAL_BOLLINGER_LOWER, &band_args, &values)[2],
            Value::Price(value) if (value - (2.0 - 2.0 * deviation)).abs() < 1e-12
        ));
        assert!(matches!(
            scalar_values(MATERIAL_BOLLINGER_PERCENT_B, &band_args, &values)[2],
            Value::Ratio(value)
                if (value - (1.0 + 2.0 * deviation) / (4.0 * deviation)).abs() < 1e-12
        ));
        assert!(matches!(
            scalar_values(MATERIAL_BOLLINGER_WIDTH, &band_args, &values)[2],
            Value::Ratio(value) if (value - 2.0 * deviation).abs() < 1e-12
        ));
        assert_eq!(
            scalar_values(MATERIAL_ROLLING_MEDIAN, &period(3), &[1.0, 100.0, 3.0])[2],
            Value::Price(3.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_MEDIAN_DEVIATION, &period(3), &[1.0, 100.0, 3.0])[2],
            Value::Ratio(0.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_ZSCORE, &period(3), &[2.0, 2.0, 2.0])[2],
            Value::Missing(ScalarType::Number)
        );
    }
    #[test]
    fn return_statistics_match_log_return_oracle() {
        let a = 0.1_f64;
        let prices = [1.0, a.exp(), 1.0, a.exp()];
        let rv = scalar_values(MATERIAL_REALIZED_VARIANCE, &period(3), &prices);
        assert!(
            (match rv[3] {
                Value::LogReturnVariance(v) => v,
                _ => unreachable!(),
            } - 3.0 * a * a)
                .abs()
                < 1e-14
        );
        assert!(
            (match scalar_values(MATERIAL_REALIZED_VOLATILITY, &period(3), &prices)[3] {
                Value::LogReturn(v) => v,
                _ => unreachable!(),
            } - 3.0f64.sqrt() * a)
                .abs()
                < 1e-14
        );
        assert!(
            (match scalar_values(MATERIAL_RETURN_RMS, &period(3), &prices)[3] {
                Value::LogReturn(v) => v,
                _ => unreachable!(),
            } - a)
                .abs()
                < 1e-14
        );
        assert!(
            (match scalar_values(MATERIAL_RETURN_STDDEV, &period(3), &prices)[3] {
                Value::LogReturn(v) => v,
                _ => unreachable!(),
            } - (8.0 / 9.0f64).sqrt() * a)
                .abs()
                < 1e-14
        );
        assert!(
            (match scalar_values(MATERIAL_POSITIVE_SEMIVARIANCE, &period(3), &prices)[3] {
                Value::LogReturnVariance(v) => v,
                _ => unreachable!(),
            } - 2.0 * a * a / 3.0)
                .abs()
                < 1e-14
        );
        assert!(
            (match scalar_values(MATERIAL_NEGATIVE_SEMIVARIANCE, &period(3), &prices)[3] {
                Value::LogReturnVariance(v) => v,
                _ => unreachable!(),
            } - a * a / 3.0)
                .abs()
                < 1e-14
        );
        let ewma = scalar_values(
            MATERIAL_EWMA_VOLATILITY,
            &[("lambda", MaterialArg::Number(0.5))],
            &[1.0, a.exp(), 1.0],
        );
        assert!(matches!(ewma[1], Value::LogReturn(value) if (value-a).abs()<1e-14));
        assert!(matches!(ewma[2], Value::LogReturn(value) if (value-a).abs()<1e-14));
        let flat = scalar_values(MATERIAL_VOLATILITY_ASYMMETRY, &period(2), &[1.0, 1.0, 1.0]);
        assert_eq!(flat[2], Value::Missing(ScalarType::Ratio));
    }
    #[test]
    fn percentile_er_regression_and_autocorrelation_cover_ties_and_degeneracy() {
        assert_eq!(
            scalar_values(
                MATERIAL_HISTORICAL_PERCENTILE,
                &period(3),
                &[2.0, 2.0, 4.0, 2.0]
            )[3],
            Value::Ratio(1.0 / 3.0)
        );
        assert_eq!(
            scalar_values(
                MATERIAL_HISTORICAL_PERCENTILE,
                &period(3),
                &[2.0, 2.0, 2.0, 2.0]
            )[3],
            Value::Ratio(0.5)
        );
        assert_eq!(
            scalar_values(MATERIAL_EFFICIENCY_RATIO, &period(3), &[2.0, 2.0, 2.0, 2.0])[3],
            Value::Ratio(0.0)
        );
        let line = [1.0, 2.0, 3.0, 4.0];
        assert_eq!(
            scalar_values(MATERIAL_REGRESSION_SLOPE, &period(4), &line)[3],
            Value::PricePerObservation(1.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_REGRESSION_R2, &period(4), &line)[3],
            Value::Number(1.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_REGRESSION_RESIDUAL_RMS, &period(4), &line)[3],
            Value::Price(0.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_REGRESSION_DEVIATION, &period(4), &line)[3],
            Value::Price(0.0)
        );
        assert_eq!(
            scalar_values(MATERIAL_REGRESSION_R2, &period(4), &[2.0; 4])[3],
            Value::Missing(ScalarType::Number)
        );
        let prices = [1.0, 2.0, 1.0, 2.0, 1.0];
        let ac = scalar_values(
            MATERIAL_RETURN_AUTOCORRELATION,
            &[
                ("period", MaterialArg::Integer(2)),
                ("lag", MaterialArg::Integer(1)),
            ],
            &prices,
        );
        assert!(matches!(ac[4],Value::Ratio(v) if (v+1.0).abs()<1e-12));
    }
    #[test]
    fn extrema_aroon_chop_and_atr_derivatives_use_raw_bars() {
        let bars = [
            b(1.0, 5.0, 0.1, 1.0),
            b(2.0, 4.0, 0.1, 2.0),
            b(3.0, 3.0, 0.1, 3.0),
        ];
        assert_eq!(
            bar_values(MATERIAL_ROLLING_HIGH_AGE, &period(3), &bars)[2],
            Value::Integer(2)
        );
        assert_eq!(
            bar_values(MATERIAL_AROON_UP, &period(2), &bars)[2],
            Value::Percent(0.0)
        );
        let tied_lows = [
            b(1.0, 3.0, 0.0, 1.0),
            b(1.0, 2.0, 0.0, 1.0),
            b(1.0, 1.0, 0.0, 1.0),
        ];
        assert_eq!(
            bar_values(MATERIAL_ROLLING_LOW_AGE, &period(3), &tied_lows)[2],
            Value::Integer(0)
        );
        assert_eq!(
            bar_values(MATERIAL_AROON_DOWN, &period(2), &tied_lows)[2],
            Value::Percent(100.0)
        );
        let atr_bars = [b(1.0, 2.0, 0.0, 1.0), b(2.0, 4.0, 0.0, 2.0)];
        assert_eq!(
            bar_values(
                MATERIAL_ATR_RATIO,
                &[
                    ("short", MaterialArg::Integer(1)),
                    ("long", MaterialArg::Integer(2)),
                ],
                &atr_bars,
            )[1],
            Value::Ratio(4.0 / 3.0)
        );
        assert_eq!(
            bar_values(
                MATERIAL_ATR_CHANGE,
                &[
                    ("period", MaterialArg::Integer(1)),
                    ("horizon", MaterialArg::Integer(1)),
                ],
                &atr_bars,
            )[1],
            Value::Ratio(1.0)
        );
        let percent = bar_values(MATERIAL_ATR_PERCENT, &period(1), &[b(1.0, 3.0, 1.0, 1.0)]);
        assert_eq!(percent[0], Value::Percent(200.0));
        assert_eq!(
            bar_values(
                MATERIAL_RANGE_EXPANSION,
                &[],
                &[b(1.0, 2.0, 1.0, 1.5), b(1.0, 3.0, 1.0, 2.0)]
            )[1],
            Value::Ratio(2.0)
        );
        let chop = bar_values(
            MATERIAL_CHOPPINESS,
            &period(2),
            &[
                b(0.5, 1.0, 0.1, 0.5),
                b(3.5, 4.0, 3.0, 3.5),
                b(3.5, 4.0, 3.0, 3.5),
            ],
        );
        assert!(matches!(
            chop[2],
            Value::Percent(value) if (value - 100.0 * 4.5_f64.log10() / 2.0_f64.log10()).abs() < 1e-12
        ));
    }
    #[test]
    fn statistical_descriptors_are_bounded_for_every_key() {
        for key in KEYS {
            let args = match *key {
                MATERIAL_EWMA_VOLATILITY => vec![],
                MATERIAL_RANGE_EXPANSION => vec![],
                MATERIAL_ATR_RATIO => vec![
                    ("short", MaterialArg::Integer(2)),
                    ("long", MaterialArg::Integer(3)),
                ],
                MATERIAL_ATR_CHANGE => vec![
                    ("period", MaterialArg::Integer(2)),
                    ("horizon", MaterialArg::Integer(2)),
                ],
                MATERIAL_RETURN_AUTOCORRELATION => vec![
                    ("period", MaterialArg::Integer(3)),
                    ("lag", MaterialArg::Integer(1)),
                ],
                MATERIAL_BOLLINGER_UPPER
                | MATERIAL_BOLLINGER_LOWER
                | MATERIAL_BOLLINGER_PERCENT_B
                | MATERIAL_BOLLINGER_WIDTH => vec![
                    ("period", MaterialArg::Integer(3)),
                    ("multiplier", MaterialArg::Number(2.0)),
                ],
                _ => vec![("period", MaterialArg::Integer(3))],
            };
            let source = SourceId::new("bars").unwrap();
            let scalar =
                StatFactory::scalar(StatFactory { key }.calc(&p(&source, &args)).unwrap().0);
            let inputs = if scalar {
                vec![ValueType::optional(ScalarType::Price)]
            } else {
                vec![]
            };
            let d = StatFactory { key }
                .numeric_descriptor(&p(&source, &args), &inputs)
                .unwrap()
                .unwrap();
            d.validate(&inputs).unwrap();
            assert!(
                d.max_state_bytes <= crate::MAX_MATERIAL_STATE_BYTES,
                "{key}"
            );
        }
    }
}
