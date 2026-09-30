use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    CompletedBarRequirement, MaterialArg, MaterialArgs, ScalarType, SourceId, Value, ValueType,
};
use std::sync::Arc;
pub const MATERIAL_CONFIRMED_LEVEL: &str = "confirmed_level";
pub const MATERIAL_CONFIRMED_LINE_VALUE: &str = "confirmed_line_value";
pub const MATERIAL_SWING_CHANGE: &str = "swing_change";
pub const MATERIAL_SWING_HIGHER: &str = "swing_higher";
pub const MATERIAL_SWING_LOWER: &str = "swing_lower";
pub const MATERIAL_LEVEL_DISTANCE: &str = "level_distance";
pub const MATERIAL_LEVEL_AGE: &str = "level_age";
pub const MATERIAL_LEG_AMPLITUDE: &str = "leg_amplitude";
pub const MATERIAL_LEG_DURATION: &str = "leg_duration";
pub const MATERIAL_RETRACEMENT: &str = "retracement";
pub const MATERIAL_DIVERGENCE: &str = "divergence";
const KEYS: &[&str] = &[
    MATERIAL_CONFIRMED_LEVEL,
    MATERIAL_CONFIRMED_LINE_VALUE,
    MATERIAL_SWING_CHANGE,
    MATERIAL_SWING_HIGHER,
    MATERIAL_SWING_LOWER,
    MATERIAL_LEVEL_DISTANCE,
    MATERIAL_LEVEL_AGE,
    MATERIAL_LEG_AMPLITUDE,
    MATERIAL_LEG_DURATION,
    MATERIAL_RETRACEMENT,
    MATERIAL_DIVERGENCE,
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
            Arc::new(StructureFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}
struct StructureFactory {
    key: &'static str,
}
impl StructureFactory {
    fn source(p: &MaterialArgs) -> Result<SourceId, String> {
        match p.get("source") {
            Some(MaterialArg::Source(v)) => Ok(v.clone()),
            _ => Err("structure primitive requires source".into()),
        }
    }
}
impl MaterialFactory for StructureFactory {
    fn params(&self) -> &[ParamSpec] {
        &SCHEMA
    }
    fn build(&self, p: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let (expected, output) = match self.key {
            MATERIAL_CONFIRMED_LEVEL => (
                vec![ScalarType::Price],
                ValueType::optional(ScalarType::Price),
            ),
            MATERIAL_CONFIRMED_LINE_VALUE => (
                vec![
                    ScalarType::Price,
                    ScalarType::Price,
                    ScalarType::Integer,
                    ScalarType::Integer,
                    ScalarType::Integer,
                ],
                ValueType::optional(ScalarType::Price),
            ),
            MATERIAL_SWING_CHANGE | MATERIAL_LEVEL_DISTANCE => (
                vec![ScalarType::Price, ScalarType::Price],
                ValueType::optional(ScalarType::Price),
            ),
            MATERIAL_SWING_HIGHER | MATERIAL_SWING_LOWER => (
                vec![ScalarType::Price, ScalarType::Price],
                ValueType::optional(ScalarType::Bool),
            ),
            MATERIAL_LEVEL_AGE => (
                vec![ScalarType::Integer, ScalarType::Integer],
                ValueType::optional(ScalarType::Integer),
            ),
            MATERIAL_LEG_AMPLITUDE => (
                vec![ScalarType::Price, ScalarType::Price],
                ValueType::optional(ScalarType::Price),
            ),
            MATERIAL_LEG_DURATION => (
                vec![ScalarType::Timestamp, ScalarType::Timestamp],
                ValueType::optional(ScalarType::Duration),
            ),
            MATERIAL_RETRACEMENT => (
                vec![ScalarType::Price, ScalarType::Price, ScalarType::Price],
                ValueType::optional(ScalarType::Ratio),
            ),
            MATERIAL_DIVERGENCE => (
                vec![ScalarType::Price, ScalarType::Number],
                ValueType::optional(ScalarType::Bool),
            ),
            _ => return Err("unknown structure primitive".into()),
        };
        if inputs.len() != expected.len() || inputs.iter().zip(expected).any(|(v, e)| v.scalar != e)
        {
            return Err("structure primitive input types are incompatible".into());
        }
        Ok(MaterialBuild {
            output_type: output,
            lookback: MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: Self::source(p)?,
                required_lookback: 1,
            }]),
            max_state_bytes: 64,
            evaluator: Box::new(StructureEvaluator {
                key: self.key,
                output,
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
struct StructureEvaluator {
    key: &'static str,
    output: ValueType,
}
impl MaterialEvaluator for StructureEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, i: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        if i.iter().any(Value::is_missing) {
            return Ok(Value::Missing(self.output.scalar));
        }
        let price = |n| match i[n] {
            Value::Price(v) => Ok(v),
            _ => Err("expected Price"),
        };
        let integer = |n| match i[n] {
            Value::Integer(v) => Ok(v),
            _ => Err("expected Integer"),
        };
        Ok(match self.key {
            MATERIAL_CONFIRMED_LEVEL => Value::Price(price(0)?),
            MATERIAL_SWING_CHANGE | MATERIAL_LEVEL_DISTANCE => Value::Price(price(0)? - price(1)?),
            MATERIAL_SWING_HIGHER => Value::Bool(price(0)? > price(1)?),
            MATERIAL_SWING_LOWER => Value::Bool(price(0)? < price(1)?),
            MATERIAL_LEVEL_AGE => Value::Integer(
                integer(0)?
                    .checked_sub(integer(1)?)
                    .filter(|v| *v >= 0)
                    .ok_or_else(|| "level age chronology is invalid".to_string())?,
            ),
            MATERIAL_LEG_AMPLITUDE => Value::Price((price(1)? - price(0)?).abs()),
            MATERIAL_LEG_DURATION => {
                let (a, b) = match (&i[0], &i[1]) {
                    (Value::Timestamp(a), Value::Timestamp(b)) => (*a, *b),
                    _ => return Err("leg duration expects timestamps".into()),
                };
                if b < a {
                    return Err("leg duration chronology is invalid".into());
                }
                Value::Duration(b - a)
            }
            MATERIAL_RETRACEMENT => {
                let (start, end, current) = (price(0)?, price(1)?, price(2)?);
                let denominator = start - end;
                if denominator == 0.0 {
                    Value::Missing(ScalarType::Ratio)
                } else {
                    Value::Ratio((current - end) / denominator)
                }
            }
            MATERIAL_DIVERGENCE => {
                let indicator = match i[1] {
                    Value::Number(v) => v,
                    _ => return Err("divergence expects Number change".into()),
                };
                Value::Bool(price(0)?.signum() * indicator.signum() < 0.0)
            }
            MATERIAL_CONFIRMED_LINE_VALUE => {
                let (p1, p2) = (price(0)?, price(1)?);
                let (o1, o2, current) = (integer(2)?, integer(3)?, integer(4)?);
                let span = o2
                    .checked_sub(o1)
                    .filter(|v| *v > 0)
                    .ok_or_else(|| "line endpoints require increasing ordinals".to_string())?;
                Value::Price(p2 + (p2 - p1) * (current - o2) as f64 / span as f64)
            }
            _ => return Err("structure calculation unavailable".into()),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::StrategyInput;
    use chrono::{Duration, NaiveDate};
    #[test]
    fn structure_formulas_preserve_chronology_and_unclamped_retracement() {
        let input = StrategyInput {
            time: NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            ready: true,
            completed_bars: vec![],
            values: vec![],
            trade_slots: vec![],
            feedback: vec![],
        };
        let context = MaterialEvalContext {
            input: &input,
            input_updates: &[],
            any_input_updates: &[],
            feedback: &[],
            retained_feedback: &[],
        };
        let mut retracement = StructureEvaluator {
            key: MATERIAL_RETRACEMENT,
            output: ValueType::optional(ScalarType::Ratio),
        };
        assert_eq!(
            retracement
                .evaluate(
                    &[Value::Price(10.0), Value::Price(6.0), Value::Price(0.0)],
                    &context
                )
                .unwrap(),
            Value::Ratio(-1.5)
        );
        let mut line = StructureEvaluator {
            key: MATERIAL_CONFIRMED_LINE_VALUE,
            output: ValueType::optional(ScalarType::Price),
        };
        assert_eq!(
            line.evaluate(
                &[
                    Value::Price(10.0),
                    Value::Price(14.0),
                    Value::Integer(2),
                    Value::Integer(4),
                    Value::Integer(7)
                ],
                &context
            )
            .unwrap(),
            Value::Price(20.0)
        );
        let mut duration = StructureEvaluator {
            key: MATERIAL_LEG_DURATION,
            output: ValueType::optional(ScalarType::Duration),
        };
        assert_eq!(
            duration
                .evaluate(
                    &[
                        Value::Timestamp(input.time),
                        Value::Timestamp(input.time + Duration::minutes(5))
                    ],
                    &context
                )
                .unwrap(),
            Value::Duration(Duration::minutes(5))
        );
    }
}
