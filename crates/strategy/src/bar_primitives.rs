use std::sync::Arc;

use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    BarField, BarStructureCalculation, CompletedBar, CompletedBarRequirement, MaterialArg,
    MaterialArgs, NumericCalculation, NumericDescriptor, NumericInputs, NumericMissingPolicy,
    NumericRange, NumericUnit, PriceChangeCalculation, ScalarType, SourceId, Value, ValueType,
};

pub const MATERIAL_BODY_FRACTION: &str = "body_fraction";
pub const MATERIAL_BODY_DIRECTION_FRACTION: &str = "body_direction_fraction";
pub const MATERIAL_UPPER_WICK_FRACTION: &str = "upper_wick_fraction";
pub const MATERIAL_LOWER_WICK_FRACTION: &str = "lower_wick_fraction";
pub const MATERIAL_CLOSE_POSITION: &str = "close_position";
pub const MATERIAL_CLOSE_LOCATION_VALUE: &str = "close_location_value";
pub const MATERIAL_RETURN_LOG: &str = "return_log";
pub const MATERIAL_ROC: &str = "roc";
pub const MATERIAL_INSIDE_BAR: &str = "inside_bar";
pub const MATERIAL_OUTSIDE_BAR: &str = "outside_bar";
pub const MATERIAL_BODY_ENGULFING: &str = "body_engulfing";
pub const MATERIAL_ENGULF_SIZE_RATIO: &str = "engulf_size_ratio";
pub const MATERIAL_NARROW_RANGE: &str = "narrow_range";
pub const MATERIAL_WIDE_RANGE: &str = "wide_range";
pub const MATERIAL_RELATIVE_RANGE: &str = "relative_range";
pub const MATERIAL_BAR_OVERLAP: &str = "bar_overlap";
pub const MATERIAL_THREE_BAR_GAP_UP: &str = "three_bar_gap_up";
pub const MATERIAL_THREE_BAR_GAP_DOWN: &str = "three_bar_gap_down";

const KEYS: &[&str] = &[
    MATERIAL_BODY_FRACTION,
    MATERIAL_BODY_DIRECTION_FRACTION,
    MATERIAL_UPPER_WICK_FRACTION,
    MATERIAL_LOWER_WICK_FRACTION,
    MATERIAL_CLOSE_POSITION,
    MATERIAL_CLOSE_LOCATION_VALUE,
    MATERIAL_RETURN_LOG,
    MATERIAL_ROC,
    MATERIAL_INSIDE_BAR,
    MATERIAL_OUTSIDE_BAR,
    MATERIAL_BODY_ENGULFING,
    MATERIAL_ENGULF_SIZE_RATIO,
    MATERIAL_NARROW_RANGE,
    MATERIAL_WIDE_RANGE,
    MATERIAL_RELATIVE_RANGE,
    MATERIAL_BAR_OVERLAP,
    MATERIAL_THREE_BAR_GAP_UP,
    MATERIAL_THREE_BAR_GAP_DOWN,
];
const SOURCE_SCHEMA: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
const SOURCE_LENGTH_SCHEMA: [ParamSpec; 2] = [
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
            max: crate::numeric::MAX_STRICT_PERIOD as i64 - 1,
        },
        required: true,
    },
];
const OHLC: &[BarField] = &[
    BarField::Open,
    BarField::High,
    BarField::Low,
    BarField::Close,
];
const HIGH_LOW: &[BarField] = &[BarField::High, BarField::Low];
const HIGH_LOW_CLOSE: &[BarField] = &[BarField::High, BarField::Low, BarField::Close];
const CLOSE: &[BarField] = &[BarField::Close];

pub(crate) fn registrations() -> impl Iterator<Item = (&'static str, Arc<dyn MaterialFactory>)> {
    KEYS.iter().copied().map(|key| {
        (
            key,
            Arc::new(BarPrimitiveFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}

type BarDefinition = (
    NumericCalculation,
    &'static [BarField],
    ValueType,
    NumericUnit,
    NumericRange,
    usize,
);

struct BarPrimitiveFactory {
    key: &'static str,
}

impl BarPrimitiveFactory {
    fn source(&self, params: &MaterialArgs) -> Result<SourceId, String> {
        match params.get("source") {
            Some(MaterialArg::Source(source)) => Ok(source.clone()),
            _ => Err("bar primitive requires a source".into()),
        }
    }

    fn length(&self, params: &MaterialArgs) -> Result<usize, String> {
        let name = if matches!(self.key, MATERIAL_RETURN_LOG | MATERIAL_ROC) {
            "horizon"
        } else {
            "period"
        };
        match params.get(name) {
            Some(MaterialArg::Integer(value)) => usize::try_from(*value)
                .ok()
                .filter(|value| (1..=crate::numeric::MAX_STRICT_PERIOD).contains(value))
                .ok_or_else(|| format!("{name} is out of bounds")),
            _ => Err(format!("bar primitive requires {name}")),
        }
    }

    fn definition(&self, params: &MaterialArgs) -> Result<BarDefinition, String> {
        let optional_ratio = ValueType::optional(ScalarType::Ratio);
        let optional_log_return = ValueType::optional(ScalarType::LogReturn);
        let optional_bool = ValueType::optional(ScalarType::Bool);
        Ok(match self.key {
            MATERIAL_BODY_FRACTION => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::BodyFraction),
                OHLC,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: 0.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_BODY_DIRECTION_FRACTION => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::BodyDirectionFraction),
                OHLC,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: -1.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_UPPER_WICK_FRACTION => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::UpperWickFraction),
                OHLC,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: 0.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_LOWER_WICK_FRACTION => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::LowerWickFraction),
                OHLC,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: 0.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_CLOSE_POSITION => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::ClosePosition),
                HIGH_LOW_CLOSE,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: 0.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_CLOSE_LOCATION_VALUE => (
                NumericCalculation::BarShape(crate::BarShapeCalculation::CloseLocationValue),
                HIGH_LOW_CLOSE,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: -1.0,
                    maximum: 1.0,
                },
                1,
            ),
            MATERIAL_RETURN_LOG => {
                let h = self.length(params)?;
                (
                    NumericCalculation::PriceChange {
                        calculation: PriceChangeCalculation::LogReturn,
                        horizon: h,
                    },
                    CLOSE,
                    optional_log_return,
                    NumericUnit::LogReturn,
                    NumericRange::Unbounded,
                    h + 1,
                )
            }
            MATERIAL_ROC => {
                let h = self.length(params)?;
                (
                    NumericCalculation::PriceChange {
                        calculation: PriceChangeCalculation::Roc,
                        horizon: h,
                    },
                    CLOSE,
                    optional_ratio,
                    NumericUnit::Ratio,
                    NumericRange::Unbounded,
                    h + 1,
                )
            }
            MATERIAL_INSIDE_BAR => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::InsideBar,
                    period: None,
                },
                HIGH_LOW,
                optional_bool,
                NumericUnit::Bool,
                NumericRange::Unbounded,
                2,
            ),
            MATERIAL_OUTSIDE_BAR => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::OutsideBar,
                    period: None,
                },
                HIGH_LOW,
                optional_bool,
                NumericUnit::Bool,
                NumericRange::Unbounded,
                2,
            ),
            MATERIAL_BODY_ENGULFING => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::BodyEngulfing,
                    period: None,
                },
                OHLC,
                optional_bool,
                NumericUnit::Bool,
                NumericRange::Unbounded,
                2,
            ),
            MATERIAL_ENGULF_SIZE_RATIO => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::EngulfSizeRatio,
                    period: None,
                },
                OHLC,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Unbounded,
                2,
            ),
            MATERIAL_NARROW_RANGE => {
                let n = self.length(params)?;
                (
                    NumericCalculation::BarStructure {
                        calculation: BarStructureCalculation::NarrowRange,
                        period: Some(n),
                    },
                    HIGH_LOW,
                    optional_bool,
                    NumericUnit::Bool,
                    NumericRange::Unbounded,
                    n,
                )
            }
            MATERIAL_WIDE_RANGE => {
                let n = self.length(params)?;
                (
                    NumericCalculation::BarStructure {
                        calculation: BarStructureCalculation::WideRange,
                        period: Some(n),
                    },
                    HIGH_LOW,
                    optional_bool,
                    NumericUnit::Bool,
                    NumericRange::Unbounded,
                    n,
                )
            }
            MATERIAL_RELATIVE_RANGE => {
                let n = self.length(params)?;
                (
                    NumericCalculation::BarStructure {
                        calculation: BarStructureCalculation::RelativeRange,
                        period: Some(n),
                    },
                    HIGH_LOW,
                    optional_ratio,
                    NumericUnit::Ratio,
                    NumericRange::Unbounded,
                    n + 1,
                )
            }
            MATERIAL_BAR_OVERLAP => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::BarOverlap,
                    period: None,
                },
                HIGH_LOW,
                optional_ratio,
                NumericUnit::Ratio,
                NumericRange::Inclusive {
                    minimum: 0.0,
                    maximum: 1.0,
                },
                2,
            ),
            MATERIAL_THREE_BAR_GAP_UP => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::ThreeBarGapUp,
                    period: None,
                },
                HIGH_LOW,
                optional_bool,
                NumericUnit::Bool,
                NumericRange::Unbounded,
                3,
            ),
            MATERIAL_THREE_BAR_GAP_DOWN => (
                NumericCalculation::BarStructure {
                    calculation: BarStructureCalculation::ThreeBarGapDown,
                    period: None,
                },
                HIGH_LOW,
                optional_bool,
                NumericUnit::Bool,
                NumericRange::Unbounded,
                3,
            ),
            _ => return Err("unknown bar primitive".into()),
        })
    }
}

impl MaterialFactory for BarPrimitiveFactory {
    fn numeric_descriptor(
        &self,
        params: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        if !inputs.is_empty() {
            return Err("bar primitive does not accept expression inputs".into());
        }
        let source_clock = self.source(params)?;
        let (calculation, fields, output_type, unit, range, first) = self.definition(params)?;
        let max_state_bytes = BarWindow::state_bytes(first)?;
        Ok(Some(NumericDescriptor {
            calculation,
            source_clock,
            inputs: NumericInputs::CompletedBarFields(fields),
            output_type,
            unit,
            range,
            missing: NumericMissingPolicy::ConsumeWindowSlot,
            first_output_observations: first,
            required_lookback: first,
            max_state_bytes,
            exact_aliases: &[],
        }))
    }

    fn params(&self) -> &[ParamSpec] {
        match self.key {
            MATERIAL_RETURN_LOG | MATERIAL_ROC => &SOURCE_HORIZON_SCHEMA,
            MATERIAL_NARROW_RANGE | MATERIAL_WIDE_RANGE | MATERIAL_RELATIVE_RANGE => {
                &SOURCE_LENGTH_SCHEMA
            }
            _ => &SOURCE_SCHEMA,
        }
    }

    fn build(&self, params: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let descriptor = self.numeric_descriptor(params, inputs)?.unwrap();
        Ok(MaterialBuild {
            output_type: descriptor.output_type,
            lookback: MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: descriptor.source_clock.clone(),
                required_lookback: descriptor.required_lookback,
            }]),
            max_state_bytes: descriptor.max_state_bytes,
            evaluator: Box::new(BarPrimitiveEvaluator {
                calculation: descriptor.calculation,
                source: descriptor.source_clock,
                bars: BarWindow::new(descriptor.first_output_observations)?,
                output_type: descriptor.output_type,
            }),
        })
    }

    fn update_trigger(
        &self,
        params: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::Source(self.source(params)?))
    }
}

#[derive(Clone)]
struct BarWindow {
    slots: Box<[Option<CompletedBar>]>,
    next: usize,
    len: usize,
}

impl BarWindow {
    fn state_bytes(period: usize) -> Result<usize, String> {
        if period == 0 || period > crate::numeric::MAX_STRICT_PERIOD {
            return Err("bar window period is out of bounds".into());
        }
        period
            .checked_mul(std::mem::size_of::<Option<CompletedBar>>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .filter(|bytes| *bytes <= crate::MAX_MATERIAL_STATE_BYTES)
            .ok_or_else(|| "bar primitive state exceeds the material bound".into())
    }
    fn new(period: usize) -> Result<Self, String> {
        Self::state_bytes(period)?;
        Ok(Self {
            slots: vec![None; period].into_boxed_slice(),
            next: 0,
            len: 0,
        })
    }
    fn push(&mut self, bar: CompletedBar) {
        self.slots[self.next] = Some(bar);
        self.next = (self.next + 1) % self.slots.len();
        self.len = (self.len + 1).min(self.slots.len());
    }
    fn complete(&self) -> bool {
        self.len == self.slots.len()
    }
    fn get(&self, age: usize) -> &CompletedBar {
        let index = (self.next + self.slots.len() - 1 - age) % self.slots.len();
        self.slots[index]
            .as_ref()
            .expect("observed bar window is initialized")
    }
    fn chronological(&self) -> impl Iterator<Item = &CompletedBar> {
        (0..self.len).rev().map(|age| self.get(age))
    }
}

#[derive(Clone)]
struct BarPrimitiveEvaluator {
    calculation: NumericCalculation,
    source: SourceId,
    bars: BarWindow,
    output_type: ValueType,
}

impl MaterialEvaluator for BarPrimitiveEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        let bar = context
            .input
            .completed_bars
            .iter()
            .find(|update| update.source == self.source)
            .ok_or_else(|| "bar primitive triggered without its source update".to_string())?;
        self.bars.push(bar.bar.clone());
        if !self.bars.complete() {
            return Ok(Value::Missing(self.output_type.scalar));
        }
        self.calculate()
    }
}

impl BarPrimitiveEvaluator {
    fn calculate(&self) -> Result<Value, String> {
        let current = self.bars.get(0);
        let number = |value: Option<f64>| {
            value
                .map(|value| match self.output_type.scalar {
                    ScalarType::Ratio => Value::Ratio(value),
                    ScalarType::LogReturn => Value::LogReturn(value),
                    scalar => unreachable!("unexpected bar primitive numeric scalar {scalar:?}"),
                })
                .unwrap_or(Value::Missing(self.output_type.scalar))
        };
        let boolean = |value: bool| Value::Bool(value);
        let range = |bar: &CompletedBar| bar.high - bar.low;
        let body = |bar: &CompletedBar| bar.close - bar.open;
        let ratio = |numerator: f64, denominator: f64| {
            if denominator == 0.0 {
                None
            } else {
                Some(numerator / denominator)
            }
        };
        let value = match self.calculation {
            NumericCalculation::BarShape(calculation) => match calculation {
                crate::BarShapeCalculation::BodyFraction => {
                    number(ratio(body(current).abs(), range(current)))
                }
                crate::BarShapeCalculation::BodyDirectionFraction => {
                    number(ratio(body(current), range(current)))
                }
                crate::BarShapeCalculation::UpperWickFraction => number(ratio(
                    current.high - current.open.max(current.close),
                    range(current),
                )),
                crate::BarShapeCalculation::LowerWickFraction => number(ratio(
                    current.open.min(current.close) - current.low,
                    range(current),
                )),
                crate::BarShapeCalculation::ClosePosition => {
                    number(ratio(current.close - current.low, range(current)))
                }
                crate::BarShapeCalculation::CloseLocationValue => number(ratio(
                    (current.close - current.low) - (current.high - current.close),
                    range(current),
                )),
                _ => {
                    return Err("ATR-normalized bar shape is not registered by this factory".into());
                }
            },
            NumericCalculation::PriceChange {
                calculation,
                horizon,
            } => {
                let prior = self.bars.get(horizon);
                match calculation {
                    PriceChangeCalculation::LogReturn => {
                        number(if current.close > 0.0 && prior.close > 0.0 {
                            Some(current.close.ln() - prior.close.ln())
                        } else {
                            None
                        })
                    }
                    PriceChangeCalculation::Roc => {
                        number(ratio(current.close - prior.close, prior.close))
                    }
                    _ => {
                        return Err(
                            "ATR-normalized price change is not registered by this factory".into(),
                        );
                    }
                }
            }
            NumericCalculation::BarStructure {
                calculation,
                period,
            } => match calculation {
                BarStructureCalculation::InsideBar => {
                    let p = self.bars.get(1);
                    boolean(current.high < p.high && current.low > p.low)
                }
                BarStructureCalculation::OutsideBar => {
                    let p = self.bars.get(1);
                    boolean(current.high > p.high && current.low < p.low)
                }
                BarStructureCalculation::BodyEngulfing => {
                    let p = self.bars.get(1);
                    boolean(
                        current.open.min(current.close) < p.open.min(p.close)
                            && current.open.max(current.close) > p.open.max(p.close),
                    )
                }
                BarStructureCalculation::EngulfSizeRatio => {
                    let p = self.bars.get(1);
                    number(ratio(body(current).abs(), body(p).abs()))
                }
                BarStructureCalculation::NarrowRange => boolean(
                    self.bars
                        .chronological()
                        .map(range)
                        .all(|value| range(current) <= value),
                ),
                BarStructureCalculation::WideRange => boolean(
                    self.bars
                        .chronological()
                        .map(range)
                        .all(|value| range(current) >= value),
                ),
                BarStructureCalculation::RelativeRange => {
                    let n = period.expect("relative range has a period");
                    let prior_mean =
                        crate::numeric::stable_mean((1..=n).map(|age| range(self.bars.get(age))))?;
                    number(ratio(range(current), prior_mean))
                }
                BarStructureCalculation::BarOverlap => {
                    let p = self.bars.get(1);
                    number(ratio(
                        (current.high.min(p.high) - current.low.max(p.low)).max(0.0),
                        current.high.max(p.high) - current.low.min(p.low),
                    ))
                }
                BarStructureCalculation::ThreeBarGapUp => {
                    boolean(current.low > self.bars.get(2).high)
                }
                BarStructureCalculation::ThreeBarGapDown => {
                    boolean(current.high < self.bars.get(2).low)
                }
            },
            _ => {
                return Err(
                    "bar primitive evaluator received a different calculation family".into(),
                );
            }
        };
        if matches!(&value, Value::Ratio(value) | Value::LogReturn(value) if !value.is_finite()) {
            return Err("bar primitive arithmetic overflowed".into());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StrategyInput;
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

    fn evaluate(
        key: &'static str,
        extra: Option<(&'static str, i64)>,
        bars: &[CompletedBar],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut params = vec![("source", MaterialArg::Source(source.clone()))];
        if let Some((name, value)) = extra {
            params.push((name, MaterialArg::Integer(value)));
        }
        let factory = BarPrimitiveFactory { key };
        let mut evaluator = factory
            .build(&MaterialArgs::new(params), &[])
            .unwrap()
            .evaluator;
        bars.iter()
            .cloned()
            .map(|bar| {
                let input = StrategyInput {
                    time: NaiveDateTime::default(),
                    ready: true,
                    completed_bars: vec![crate::CompletedBarUpdate {
                        source: source.clone(),
                        bar,
                    }],
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
                evaluator.evaluate(&[], &context).unwrap()
            })
            .collect()
    }

    #[test]
    fn single_bar_shapes_match_the_hand_oracle_and_flat_domain() {
        let fixture = [bar(2.0, 5.0, 1.0, 4.0)];
        for (key, expected) in [
            (MATERIAL_BODY_FRACTION, 0.5),
            (MATERIAL_BODY_DIRECTION_FRACTION, 0.5),
            (MATERIAL_UPPER_WICK_FRACTION, 0.25),
            (MATERIAL_LOWER_WICK_FRACTION, 0.25),
            (MATERIAL_CLOSE_POSITION, 0.75),
            (MATERIAL_CLOSE_LOCATION_VALUE, 0.5),
        ] {
            assert_eq!(
                evaluate(key, None, &fixture),
                vec![Value::Ratio(expected)],
                "{key}"
            );
            assert_eq!(
                evaluate(key, None, &[bar(2.0, 2.0, 2.0, 2.0)]),
                vec![Value::Missing(ScalarType::Ratio)],
                "flat {key}"
            );
        }
        let outputs = [
            MATERIAL_BODY_FRACTION,
            MATERIAL_UPPER_WICK_FRACTION,
            MATERIAL_LOWER_WICK_FRACTION,
        ]
        .map(|key| match evaluate(key, None, &fixture)[0] {
            Value::Ratio(value) => value,
            _ => unreachable!(),
        });
        assert_eq!(outputs.into_iter().sum::<f64>(), 1.0);
    }

    #[test]
    fn price_changes_use_exact_horizon_and_declared_domain() {
        let prices = [1.0, 3.0, 4.0].map(|close| bar(close, close, close, close));
        assert_eq!(
            evaluate(MATERIAL_ROC, Some(("horizon", 2)), &prices),
            vec![
                Value::Missing(ScalarType::Ratio),
                Value::Missing(ScalarType::Ratio),
                Value::Ratio(3.0)
            ]
        );
        let log = evaluate(MATERIAL_RETURN_LOG, Some(("horizon", 2)), &prices);
        assert_eq!(
            log[..2],
            [
                Value::Missing(ScalarType::LogReturn),
                Value::Missing(ScalarType::LogReturn)
            ]
        );
        assert!(matches!(log[2], Value::LogReturn(value) if (value - 4.0f64.ln()).abs() < 1e-15));
    }

    #[test]
    fn relative_bar_structures_match_strict_tie_and_window_rules() {
        assert_eq!(
            evaluate(
                MATERIAL_INSIDE_BAR,
                None,
                &[bar(2.0, 5.0, 1.0, 4.0), bar(2.0, 4.0, 2.0, 3.0)]
            ),
            vec![Value::Missing(ScalarType::Bool), Value::Bool(true)]
        );
        assert_eq!(
            evaluate(
                MATERIAL_OUTSIDE_BAR,
                None,
                &[bar(2.0, 5.0, 1.0, 4.0), bar(2.0, 6.0, 0.5, 3.0)]
            )[1],
            Value::Bool(true)
        );
        assert_eq!(
            evaluate(
                MATERIAL_BODY_ENGULFING,
                None,
                &[bar(2.0, 5.0, 1.0, 4.0), bar(5.0, 6.0, 0.5, 1.0)]
            )[1],
            Value::Bool(true)
        );
        assert_eq!(
            evaluate(
                MATERIAL_ENGULF_SIZE_RATIO,
                None,
                &[bar(2.0, 5.0, 1.0, 4.0), bar(5.0, 6.0, 0.5, 1.0)]
            )[1],
            Value::Ratio(2.0)
        );
        let ranges = [
            bar(2.0, 4.0, 2.0, 3.0),
            bar(2.0, 6.0, 2.0, 3.0),
            bar(2.0, 5.0, 2.0, 3.0),
        ];
        assert_eq!(
            evaluate(MATERIAL_NARROW_RANGE, Some(("period", 3)), &ranges)[2],
            Value::Bool(false)
        );
        assert_eq!(
            evaluate(MATERIAL_WIDE_RANGE, Some(("period", 3)), &ranges)[2],
            Value::Bool(false)
        );
        assert_eq!(
            evaluate(MATERIAL_RELATIVE_RANGE, Some(("period", 2)), &ranges)[2],
            Value::Ratio(1.0)
        );
        assert_eq!(
            evaluate(
                MATERIAL_BAR_OVERLAP,
                None,
                &[bar(2.0, 5.0, 1.0, 4.0), bar(5.0, 8.0, 4.0, 7.0)]
            )[1],
            Value::Ratio(1.0 / 7.0)
        );
        assert_eq!(
            evaluate(
                MATERIAL_THREE_BAR_GAP_UP,
                None,
                &[
                    bar(1.0, 2.0, 1.0, 1.5),
                    bar(3.0, 4.0, 3.0, 3.5),
                    bar(5.0, 6.0, 4.0, 5.0)
                ]
            )[2],
            Value::Bool(true)
        );
        assert_eq!(
            evaluate(
                MATERIAL_THREE_BAR_GAP_DOWN,
                None,
                &[
                    bar(5.0, 6.0, 5.0, 5.5),
                    bar(3.0, 4.0, 3.0, 3.5),
                    bar(1.0, 2.0, 1.0, 1.5)
                ]
            )[2],
            Value::Bool(true)
        );
        let equal = [bar(2.0, 5.0, 1.0, 4.0), bar(2.0, 5.0, 1.0, 4.0)];
        assert_eq!(
            evaluate(MATERIAL_INSIDE_BAR, None, &equal)[1],
            Value::Bool(false)
        );
        assert_eq!(
            evaluate(MATERIAL_OUTSIDE_BAR, None, &equal)[1],
            Value::Bool(false)
        );
    }

    #[test]
    fn descriptors_and_storage_are_bounded_for_every_registered_bar_primitive() {
        for key in KEYS {
            let extra = match *key {
                MATERIAL_RETURN_LOG | MATERIAL_ROC => Some(("horizon", 3)),
                MATERIAL_NARROW_RANGE | MATERIAL_WIDE_RANGE | MATERIAL_RELATIVE_RANGE => {
                    Some(("period", 3))
                }
                _ => None,
            };
            let mut params = vec![(
                "source",
                MaterialArg::Source(SourceId::new("bars").unwrap()),
            )];
            if let Some((name, value)) = extra {
                params.push((name, MaterialArg::Integer(value)));
            }
            let descriptor = BarPrimitiveFactory { key }
                .numeric_descriptor(&MaterialArgs::new(params), &[])
                .unwrap()
                .unwrap();
            descriptor.validate(&[]).unwrap();
            assert!(descriptor.max_state_bytes <= crate::MAX_MATERIAL_STATE_BYTES);
            assert_eq!(
                descriptor.required_lookback,
                descriptor.first_output_observations
            );
        }
    }
}
