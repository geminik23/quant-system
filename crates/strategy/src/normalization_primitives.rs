use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    MaterialArg, MaterialArgs, NormalizedCalculation, NumericCalculation, NumericDescriptor,
    NumericInputs, NumericMissingPolicy, NumericRange, NumericUnit, ScalarType, SourceId, Value,
    ValueType,
};
use std::sync::Arc;
pub const MATERIAL_MACD_ATR: &str = "macd_atr";
pub const MATERIAL_MACD_SIGNAL_ATR: &str = "macd_signal_atr";
pub const MATERIAL_MACD_HISTOGRAM_ATR: &str = "macd_histogram_atr";
pub const MATERIAL_MA_DISTANCE_ATR: &str = "ma_distance_atr";
pub const MATERIAL_MA_SLOPE_ATR: &str = "ma_slope_atr";
pub const MATERIAL_MA_ACCELERATION_ATR: &str = "ma_acceleration_atr";
pub const MATERIAL_MA_GAP_ATR: &str = "ma_gap_atr";
const KEYS: &[&str] = &[
    MATERIAL_MACD_ATR,
    MATERIAL_MACD_SIGNAL_ATR,
    MATERIAL_MACD_HISTOGRAM_ATR,
    MATERIAL_MA_DISTANCE_ATR,
    MATERIAL_MA_SLOPE_ATR,
    MATERIAL_MA_ACCELERATION_ATR,
    MATERIAL_MA_GAP_ATR,
];
const SCHEMA: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(NormalizationFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}
struct NormalizationFactory {
    key: &'static str,
}
impl NormalizationFactory {
    fn source(p: &MaterialArgs) -> Result<SourceId, String> {
        match p.get("source") {
            Some(MaterialArg::Source(v)) => Ok(v.clone()),
            _ => Err("normalization requires source".into()),
        }
    }
    fn calculation(&self) -> NormalizedCalculation {
        match self.key {
            MATERIAL_MACD_ATR => NormalizedCalculation::MacdAtr,
            MATERIAL_MACD_SIGNAL_ATR => NormalizedCalculation::MacdSignalAtr,
            MATERIAL_MACD_HISTOGRAM_ATR => NormalizedCalculation::MacdHistogramAtr,
            MATERIAL_MA_DISTANCE_ATR => NormalizedCalculation::MaDistanceAtr,
            MATERIAL_MA_SLOPE_ATR => NormalizedCalculation::MaSlopeAtr,
            MATERIAL_MA_ACCELERATION_ATR => NormalizedCalculation::MaAccelerationAtr,
            _ => NormalizedCalculation::MaGapAtr,
        }
    }
}
impl MaterialFactory for NormalizationFactory {
    fn numeric_descriptor(
        &self,
        p: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let numerator = if self.key == MATERIAL_MA_SLOPE_ATR {
            ScalarType::PricePerObservation
        } else if self.key == MATERIAL_MA_ACCELERATION_ATR {
            ScalarType::PricePerObservationSquared
        } else {
            ScalarType::Price
        };
        if inputs.len() != 2
            || inputs[0].scalar != numerator
            || inputs[1].scalar != ScalarType::Price
        {
            return Err("ATR normalization requires a typed numerator and Price ATR".into());
        }
        let (output, unit) = if numerator == ScalarType::PricePerObservation {
            (
                ScalarType::RatioPerObservation,
                NumericUnit::RatioPerObservation,
            )
        } else if numerator == ScalarType::PricePerObservationSquared {
            (
                ScalarType::RatioPerObservationSquared,
                NumericUnit::RatioPerObservationSquared,
            )
        } else {
            (ScalarType::Ratio, NumericUnit::Ratio)
        };
        Ok(Some(NumericDescriptor {
            calculation: NumericCalculation::NormalizedPair {
                calculation: self.calculation(),
            },
            source_clock: Self::source(p)?,
            inputs: NumericInputs::Scalar(inputs.to_vec()),
            output_type: ValueType::optional(output),
            unit,
            range: NumericRange::Unbounded,
            missing: NumericMissingPolicy::ConsumeWindowSlot,
            first_output_observations: 1,
            required_lookback: 1,
            max_state_bytes: std::mem::size_of::<NormalizationEvaluator>(),
            exact_aliases: &[],
        }))
    }
    fn params(&self) -> &[ParamSpec] {
        &SCHEMA
    }
    fn build(&self, p: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let d = self.numeric_descriptor(p, inputs)?.unwrap();
        Ok(MaterialBuild {
            output_type: d.output_type,
            lookback: MaterialLookback::InheritInputs { minimum: 1 },
            max_state_bytes: d.max_state_bytes,
            evaluator: Box::new(NormalizationEvaluator {
                output: d.output_type.scalar,
            }),
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
struct NormalizationEvaluator {
    output: ScalarType,
}
impl MaterialEvaluator for NormalizationEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(
        &mut self,
        inputs: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        if context.input_updates.iter().any(|updated| !*updated) {
            return Ok(Value::Missing(self.output));
        }
        let numerator = number(&inputs[0])?;
        let denominator = number(&inputs[1])?;
        let (Some(n), Some(d)) = (numerator, denominator) else {
            return Ok(Value::Missing(self.output));
        };
        if d <= 0.0 {
            return Ok(Value::Missing(self.output));
        }
        Ok(match self.output {
            ScalarType::Ratio => Value::Ratio(n / d),
            ScalarType::RatioPerObservation => Value::RatioPerObservation(n / d),
            ScalarType::RatioPerObservationSquared => Value::RatioPerObservationSquared(n / d),
            _ => unreachable!(),
        })
    }
}
fn number(v: &Value) -> Result<Option<f64>, String> {
    match v {
        Value::Missing(_) => Ok(None),
        Value::Price(v) | Value::PricePerObservation(v) | Value::PricePerObservationSquared(v)
            if v.is_finite() =>
        {
            Ok(Some(*v))
        }
        _ => Err("normalization input is not a finite compatible scalar".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::StrategyInput;
    use chrono::NaiveDateTime;
    #[test]
    fn normalized_pairs_preserve_units_and_zero_domain() {
        let source = SourceId::new("bars").unwrap();
        let input = StrategyInput {
            time: NaiveDateTime::default(),
            ready: true,
            completed_bars: vec![],
            values: vec![],
            trade_slots: vec![],
            feedback: vec![],
        };
        for (key, types, values, expected) in [
            (
                MATERIAL_MACD_ATR,
                [
                    ValueType::optional(ScalarType::Price),
                    ValueType::optional(ScalarType::Price),
                ],
                [Value::Price(2.0), Value::Price(4.0)],
                Value::Ratio(0.5),
            ),
            (
                MATERIAL_MA_SLOPE_ATR,
                [
                    ValueType::optional(ScalarType::PricePerObservation),
                    ValueType::optional(ScalarType::Price),
                ],
                [Value::PricePerObservation(2.0), Value::Price(4.0)],
                Value::RatioPerObservation(0.5),
            ),
            (
                MATERIAL_MA_ACCELERATION_ATR,
                [
                    ValueType::optional(ScalarType::PricePerObservationSquared),
                    ValueType::optional(ScalarType::Price),
                ],
                [Value::PricePerObservationSquared(2.0), Value::Price(4.0)],
                Value::RatioPerObservationSquared(0.5),
            ),
        ] {
            let p = MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]);
            let mut e = NormalizationFactory { key }
                .build(&p, &types)
                .unwrap()
                .evaluator;
            let context = MaterialEvalContext {
                input: &input,
                input_updates: &[true, true],
                any_input_updates: &[true, true],
                feedback: &[],
                retained_feedback: &[],
            };
            assert_eq!(e.evaluate(&values, &context).unwrap(), expected);
            let zero = [values[0].clone(), Value::Price(0.0)];
            assert_eq!(
                e.evaluate(&zero, &context).unwrap(),
                Value::Missing(expected.scalar_type())
            );
        }
    }
}
