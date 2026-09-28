use crate::material::{
    MaterialBuild, MaterialEvalContext, MaterialEvaluator, MaterialFactory, MaterialLookback,
    MaterialUpdateTrigger, ParamKind, ParamSpec,
};
use crate::{
    BarField, CompletedBar, CompletedBarRequirement, MaterialArg, MaterialArgs,
    NormalizedCalculation, NumericCalculation, NumericDescriptor, NumericInputs,
    NumericMissingPolicy, NumericRange, NumericUnit, RecursiveCalculation, ScalarType, SourceId,
    Value, ValueType,
};
use std::sync::Arc;

pub const MATERIAL_HMA: &str = "hma";
pub const MATERIAL_KAMA: &str = "kama";
pub const MATERIAL_KAMA_SMOOTHING_CONSTANT: &str = "kama_smoothing_constant";
pub const MATERIAL_KELTNER_MIDDLE: &str = "keltner_middle";
pub const MATERIAL_KELTNER_UPPER: &str = "keltner_upper";
pub const MATERIAL_KELTNER_LOWER: &str = "keltner_lower";
pub const MATERIAL_BB_KC_SQUEEZE: &str = "bb_kc_squeeze";
pub const MATERIAL_SUPERTREND_LEVEL: &str = "supertrend_level";
pub const MATERIAL_SUPERTREND_DIRECTION: &str = "supertrend_direction";
pub const MATERIAL_HEIKIN_ASHI_OPEN: &str = "heikin_ashi_open";
pub const MATERIAL_HEIKIN_ASHI_HIGH: &str = "heikin_ashi_high";
pub const MATERIAL_HEIKIN_ASHI_LOW: &str = "heikin_ashi_low";
pub const MATERIAL_HEIKIN_ASHI_CLOSE: &str = "heikin_ashi_close";
pub const MATERIAL_BODY_SIGNED_ATR: &str = "body_signed_atr";
pub const MATERIAL_BODY_ABS_ATR: &str = "body_abs_atr";
pub const MATERIAL_RANGE_ATR: &str = "range_atr";
pub const MATERIAL_GAP_ATR: &str = "gap_atr";
pub const MATERIAL_MOVE_ATR: &str = "move_atr";
pub const MATERIAL_HIGH_CHANGE_ATR: &str = "high_change_atr";
pub const MATERIAL_LOW_CHANGE_ATR: &str = "low_change_atr";
pub const MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR: &str = "donchian_upper_distance_atr";
pub const MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR: &str = "donchian_lower_distance_atr";
pub const MATERIAL_BODY_SIGNED_CURRENT_ATR: &str = "body_signed_current_atr";
pub const MATERIAL_BODY_ABS_CURRENT_ATR: &str = "body_abs_current_atr";
pub const MATERIAL_RANGE_CURRENT_ATR: &str = "range_current_atr";
pub const MATERIAL_GAP_CURRENT_ATR: &str = "gap_current_atr";
pub const MATERIAL_MOVE_CURRENT_ATR: &str = "move_current_atr";
pub const MATERIAL_HIGH_CHANGE_CURRENT_ATR: &str = "high_change_current_atr";
pub const MATERIAL_LOW_CHANGE_CURRENT_ATR: &str = "low_change_current_atr";
pub const MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR: &str =
    "donchian_upper_distance_current_atr";
pub const MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR: &str =
    "donchian_lower_distance_current_atr";
const KEYS: &[&str] = &[
    MATERIAL_HMA,
    MATERIAL_KAMA,
    MATERIAL_KAMA_SMOOTHING_CONSTANT,
    MATERIAL_KELTNER_MIDDLE,
    MATERIAL_KELTNER_UPPER,
    MATERIAL_KELTNER_LOWER,
    MATERIAL_BB_KC_SQUEEZE,
    MATERIAL_SUPERTREND_LEVEL,
    MATERIAL_SUPERTREND_DIRECTION,
    MATERIAL_HEIKIN_ASHI_OPEN,
    MATERIAL_HEIKIN_ASHI_HIGH,
    MATERIAL_HEIKIN_ASHI_LOW,
    MATERIAL_HEIKIN_ASHI_CLOSE,
    MATERIAL_BODY_SIGNED_ATR,
    MATERIAL_BODY_ABS_ATR,
    MATERIAL_RANGE_ATR,
    MATERIAL_GAP_ATR,
    MATERIAL_MOVE_ATR,
    MATERIAL_HIGH_CHANGE_ATR,
    MATERIAL_LOW_CHANGE_ATR,
    MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR,
    MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR,
    MATERIAL_BODY_SIGNED_CURRENT_ATR,
    MATERIAL_BODY_ABS_CURRENT_ATR,
    MATERIAL_RANGE_CURRENT_ATR,
    MATERIAL_GAP_CURRENT_ATR,
    MATERIAL_MOVE_CURRENT_ATR,
    MATERIAL_HIGH_CHANGE_CURRENT_ATR,
    MATERIAL_LOW_CHANGE_CURRENT_ATR,
    MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR,
    MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR,
];
const MAX: i64 = 256;
const SOURCE_PERIOD: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
];
const KAMA_SCHEMA: [ParamSpec; 4] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "fast",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: false,
    },
    ParamSpec {
        name: "slow",
        kind: ParamKind::Integer { min: 2, max: MAX },
        required: false,
    },
];
const KELTNER_SCHEMA: [ParamSpec; 4] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "ma_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "atr_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
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
const SQUEEZE_SCHEMA: [ParamSpec; 6] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "bb_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "bb_multiplier",
        kind: ParamKind::Number {
            min: f64::MIN_POSITIVE,
            max: 1000.0,
        },
        required: false,
    },
    ParamSpec {
        name: "kc_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "atr_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "kc_multiplier",
        kind: ParamKind::Number {
            min: f64::MIN_POSITIVE,
            max: 1000.0,
        },
        required: false,
    },
];
const SUPERTREND_SCHEMA: [ParamSpec; 4] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer { min: 1, max: MAX },
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
    ParamSpec {
        name: "initial_direction",
        kind: ParamKind::Integer { min: -1, max: 1 },
        required: false,
    },
];
const SOURCE_ATR: [ParamSpec; 2] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "atr_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
];
const SOURCE_ATR_HORIZON: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "atr_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "horizon",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
];
const SOURCE_ATR_PERIOD: [ParamSpec; 3] = [
    ParamSpec {
        name: "source",
        kind: ParamKind::Source,
        required: true,
    },
    ParamSpec {
        name: "atr_period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
    ParamSpec {
        name: "period",
        kind: ParamKind::Integer { min: 1, max: MAX },
        required: true,
    },
];
const SOURCE_ONLY: [ParamSpec; 1] = [ParamSpec {
    name: "source",
    kind: ParamKind::Source,
    required: true,
}];
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
            Arc::new(RecursiveFactory { key }) as Arc<dyn MaterialFactory>,
        )
    })
}
struct RecursiveFactory {
    key: &'static str,
}
impl RecursiveFactory {
    fn source(p: &MaterialArgs) -> Result<SourceId, String> {
        match p.get("source") {
            Some(MaterialArg::Source(v)) => Ok(v.clone()),
            _ => Err("recursive primitive requires source".into()),
        }
    }
    fn int(p: &MaterialArgs, n: &str, d: Option<usize>) -> Result<usize, String> {
        match p.get(n) {
            Some(MaterialArg::Integer(v)) => usize::try_from(*v)
                .ok()
                .filter(|v| (1..=MAX as usize).contains(v))
                .ok_or_else(|| format!("{n} out of bounds")),
            None => d.ok_or_else(|| format!("missing {n}")),
            _ => Err(format!("invalid {n}")),
        }
    }
    fn num(p: &MaterialArgs, n: &str, d: f64) -> Result<f64, String> {
        match p.get(n) {
            Some(MaterialArg::Number(v)) if v.is_finite() => Ok(*v),
            None => Ok(d),
            _ => Err(format!("invalid {n}")),
        }
    }
    fn current(key: &str) -> bool {
        key.contains("current_atr")
    }
    fn normalized(
        &self,
        p: &MaterialArgs,
    ) -> Result<Option<(NormalizedCalculation, usize, usize, bool)>, String> {
        let c = match self.key {
            MATERIAL_BODY_SIGNED_ATR | MATERIAL_BODY_SIGNED_CURRENT_ATR => {
                NormalizedCalculation::BodySignedAtr
            }
            MATERIAL_BODY_ABS_ATR | MATERIAL_BODY_ABS_CURRENT_ATR => {
                NormalizedCalculation::BodyAbsAtr
            }
            MATERIAL_RANGE_ATR | MATERIAL_RANGE_CURRENT_ATR => NormalizedCalculation::RangeAtr,
            MATERIAL_GAP_ATR | MATERIAL_GAP_CURRENT_ATR => NormalizedCalculation::GapAtr,
            MATERIAL_MOVE_ATR | MATERIAL_MOVE_CURRENT_ATR => NormalizedCalculation::MoveAtr,
            MATERIAL_HIGH_CHANGE_ATR | MATERIAL_HIGH_CHANGE_CURRENT_ATR => {
                NormalizedCalculation::HighChangeAtr
            }
            MATERIAL_LOW_CHANGE_ATR | MATERIAL_LOW_CHANGE_CURRENT_ATR => {
                NormalizedCalculation::LowChangeAtr
            }
            MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR | MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR => {
                NormalizedCalculation::DonchianUpperDistanceAtr
            }
            MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR | MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR => {
                NormalizedCalculation::DonchianLowerDistanceAtr
            }
            _ => return Ok(None),
        };
        let atr = Self::int(p, "atr_period", None)?;
        let horizon = match c {
            NormalizedCalculation::MoveAtr
            | NormalizedCalculation::HighChangeAtr
            | NormalizedCalculation::LowChangeAtr => Self::int(p, "horizon", None)?,
            NormalizedCalculation::DonchianUpperDistanceAtr
            | NormalizedCalculation::DonchianLowerDistanceAtr => Self::int(p, "period", None)?,
            _ => 1,
        };
        Ok(Some((c, atr, horizon, Self::current(self.key))))
    }
}
impl MaterialFactory for RecursiveFactory {
    fn numeric_descriptor(
        &self,
        p: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let source = Self::source(p)?;
        if let Some((calculation, atr, horizon, current)) = self.normalized(p)? {
            if !inputs.is_empty() {
                return Err("ATR-normalized bar primitive accepts no expression input".into());
            }
            let history = match calculation {
                NormalizedCalculation::GapAtr => 2,
                NormalizedCalculation::MoveAtr
                | NormalizedCalculation::HighChangeAtr
                | NormalizedCalculation::LowChangeAtr => horizon + 1,
                NormalizedCalculation::DonchianUpperDistanceAtr
                | NormalizedCalculation::DonchianLowerDistanceAtr => horizon + 1,
                _ => 1,
            };
            let first = history.max(if current { atr } else { atr + 1 });
            let state = BarRing::state_bytes(history.max(atr + 1))?
                + crate::numeric::ObservedWindow::state_bytes(atr)?
                + std::mem::size_of::<NormalizedEvaluator>();
            return Ok(Some(NumericDescriptor {
                calculation: NumericCalculation::Normalized {
                    calculation,
                    atr_period: atr,
                    horizon,
                    current_atr: current,
                },
                source_clock: source,
                inputs: NumericInputs::CompletedBarFields(OHLC),
                output_type: ValueType::optional(ScalarType::Ratio),
                unit: NumericUnit::Ratio,
                range: NumericRange::Unbounded,
                missing: NumericMissingPolicy::ResetAndReseed,
                first_output_observations: first,
                required_lookback: first,
                max_state_bytes: state,
                exact_aliases: &[],
            }));
        }
        let (c, periods, parameters, first, output, unit, scalar) = match self.key {
            MATERIAL_HMA => {
                let n = Self::int(p, "period", None)?;
                if n < 2 {
                    return Err("HMA period must be at least 2".into());
                }
                let root = (n as f64).sqrt().floor() as usize;
                (
                    RecursiveCalculation::Hma,
                    [n, n / 2, root, 0],
                    [0, 0],
                    n + root - 1,
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    true,
                )
            }
            MATERIAL_KAMA | MATERIAL_KAMA_SMOOTHING_CONSTANT => {
                let n = Self::int(p, "period", None)?;
                let f = Self::int(p, "fast", Some(2))?;
                let s = Self::int(p, "slow", Some(30))?;
                if f >= s {
                    return Err("KAMA requires fast < slow".into());
                }
                (
                    if self.key == MATERIAL_KAMA {
                        RecursiveCalculation::Kama
                    } else {
                        RecursiveCalculation::KamaSmoothingConstant
                    },
                    [n, f, s, 0],
                    [0, 0],
                    n + 1,
                    if self.key == MATERIAL_KAMA {
                        ValueType::optional(ScalarType::Price)
                    } else {
                        ValueType::optional(ScalarType::Ratio)
                    },
                    if self.key == MATERIAL_KAMA {
                        NumericUnit::Price
                    } else {
                        NumericUnit::Ratio
                    },
                    true,
                )
            }
            MATERIAL_KELTNER_MIDDLE | MATERIAL_KELTNER_UPPER | MATERIAL_KELTNER_LOWER => {
                let m = Self::int(p, "ma_period", None)?;
                let a = Self::int(p, "atr_period", None)?;
                let k = Self::num(p, "multiplier", 2.0)?;
                (
                    if self.key == MATERIAL_KELTNER_MIDDLE {
                        RecursiveCalculation::KeltnerMiddle
                    } else if self.key == MATERIAL_KELTNER_UPPER {
                        RecursiveCalculation::KeltnerUpper
                    } else {
                        RecursiveCalculation::KeltnerLower
                    },
                    [m, a, 0, 0],
                    [k.to_bits(), 0],
                    m.max(a),
                    ValueType::optional(ScalarType::Price),
                    NumericUnit::Price,
                    false,
                )
            }
            MATERIAL_BB_KC_SQUEEZE => {
                let bb = Self::int(p, "bb_period", None)?;
                let kc = Self::int(p, "kc_period", None)?;
                let atr = Self::int(p, "atr_period", None)?;
                (
                    RecursiveCalculation::BollingerKeltnerSqueeze,
                    [bb, kc, atr, 0],
                    [
                        Self::num(p, "bb_multiplier", 2.0)?.to_bits(),
                        Self::num(p, "kc_multiplier", 2.0)?.to_bits(),
                    ],
                    bb.max(kc).max(atr),
                    ValueType::optional(ScalarType::Bool),
                    NumericUnit::Bool,
                    false,
                )
            }
            MATERIAL_SUPERTREND_LEVEL | MATERIAL_SUPERTREND_DIRECTION => {
                let n = Self::int(p, "period", None)?;
                let direction = match p.get("initial_direction") {
                    Some(MaterialArg::Integer(1)) | None => 1,
                    Some(MaterialArg::Integer(-1)) => -1,
                    _ => return Err("SuperTrend initial_direction must be 1 or -1".into()),
                };
                (
                    if self.key == MATERIAL_SUPERTREND_LEVEL {
                        RecursiveCalculation::SuperTrendLevel
                    } else {
                        RecursiveCalculation::SuperTrendDirection
                    },
                    [n, 0, 0, 0],
                    [Self::num(p, "multiplier", 3.0)?.to_bits(), direction as u64],
                    n,
                    if self.key == MATERIAL_SUPERTREND_LEVEL {
                        ValueType::optional(ScalarType::Price)
                    } else {
                        ValueType::optional(ScalarType::Number)
                    },
                    if self.key == MATERIAL_SUPERTREND_LEVEL {
                        NumericUnit::Price
                    } else {
                        NumericUnit::Number
                    },
                    false,
                )
            }
            MATERIAL_HEIKIN_ASHI_OPEN
            | MATERIAL_HEIKIN_ASHI_HIGH
            | MATERIAL_HEIKIN_ASHI_LOW
            | MATERIAL_HEIKIN_ASHI_CLOSE => (
                match self.key {
                    MATERIAL_HEIKIN_ASHI_OPEN => RecursiveCalculation::HeikinAshiOpen,
                    MATERIAL_HEIKIN_ASHI_HIGH => RecursiveCalculation::HeikinAshiHigh,
                    MATERIAL_HEIKIN_ASHI_LOW => RecursiveCalculation::HeikinAshiLow,
                    _ => RecursiveCalculation::HeikinAshiClose,
                },
                [1, 0, 0, 0],
                [0, 0],
                1,
                ValueType::optional(ScalarType::Price),
                NumericUnit::Price,
                false,
            ),
            _ => return Err("unknown recursive primitive".into()),
        };
        if scalar && (inputs.len() != 1 || inputs[0].scalar != ScalarType::Price) {
            return Err("recursive scalar primitive requires Price input".into());
        }
        if !scalar && !inputs.is_empty() {
            return Err("recursive bar primitive accepts no expression inputs".into());
        }
        let state = if scalar {
            3 * crate::numeric::ObservedWindow::state_bytes(first.max(periods[0]))?
                + std::mem::size_of::<ScalarRecursiveEvaluator>()
        } else {
            BarRing::state_bytes(first.max(periods[0]))?
                + 2 * crate::numeric::ObservedWindow::state_bytes(periods[0].max(1))?
                + std::mem::size_of::<BarRecursiveEvaluator>()
        };
        if state > crate::MAX_MATERIAL_STATE_BYTES {
            return Err("recursive state exceeds material bound".into());
        }
        Ok(Some(NumericDescriptor {
            calculation: NumericCalculation::Recursive {
                calculation: c,
                periods,
                parameters,
            },
            source_clock: source,
            inputs: if scalar {
                NumericInputs::Scalar(inputs.to_vec())
            } else {
                NumericInputs::CompletedBarFields(OHLC)
            },
            output_type: output,
            unit,
            range: NumericRange::Unbounded,
            missing: NumericMissingPolicy::ResetAndReseed,
            first_output_observations: first,
            required_lookback: first,
            max_state_bytes: state,
            exact_aliases: &[],
        }))
    }
    fn params(&self) -> &[ParamSpec] {
        if matches!(self.key, MATERIAL_HMA) {
            &SOURCE_PERIOD
        } else if matches!(self.key, MATERIAL_KAMA | MATERIAL_KAMA_SMOOTHING_CONSTANT) {
            &KAMA_SCHEMA
        } else if matches!(
            self.key,
            MATERIAL_KELTNER_MIDDLE | MATERIAL_KELTNER_UPPER | MATERIAL_KELTNER_LOWER
        ) {
            &KELTNER_SCHEMA
        } else if self.key == MATERIAL_BB_KC_SQUEEZE {
            &SQUEEZE_SCHEMA
        } else if matches!(
            self.key,
            MATERIAL_SUPERTREND_LEVEL | MATERIAL_SUPERTREND_DIRECTION
        ) {
            &SUPERTREND_SCHEMA
        } else if matches!(
            self.key,
            MATERIAL_HEIKIN_ASHI_OPEN
                | MATERIAL_HEIKIN_ASHI_HIGH
                | MATERIAL_HEIKIN_ASHI_LOW
                | MATERIAL_HEIKIN_ASHI_CLOSE
        ) {
            &SOURCE_ONLY
        } else if matches!(
            self.key,
            MATERIAL_MOVE_ATR
                | MATERIAL_HIGH_CHANGE_ATR
                | MATERIAL_LOW_CHANGE_ATR
                | MATERIAL_MOVE_CURRENT_ATR
                | MATERIAL_HIGH_CHANGE_CURRENT_ATR
                | MATERIAL_LOW_CHANGE_CURRENT_ATR
        ) {
            &SOURCE_ATR_HORIZON
        } else if matches!(
            self.key,
            MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR
                | MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR
                | MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR
                | MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR
        ) {
            &SOURCE_ATR_PERIOD
        } else {
            &SOURCE_ATR
        }
    }
    fn build(&self, p: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        let d = self.numeric_descriptor(p, inputs)?.unwrap();
        let evaluator: Box<dyn MaterialEvaluator> = match d.calculation.clone() {
            NumericCalculation::Normalized {
                calculation,
                atr_period,
                horizon,
                current_atr,
            } => Box::new(NormalizedEvaluator::new(
                calculation,
                d.source_clock.clone(),
                atr_period,
                horizon,
                current_atr,
            )?),
            NumericCalculation::Recursive {
                calculation,
                periods,
                parameters,
            } => {
                if matches!(d.inputs, NumericInputs::Scalar(_)) {
                    Box::new(ScalarRecursiveEvaluator::new(calculation, periods)?)
                } else {
                    Box::new(BarRecursiveEvaluator::new(
                        calculation,
                        d.source_clock.clone(),
                        periods,
                        parameters,
                    )?)
                }
            }
            _ => unreachable!(),
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
            evaluator,
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
struct Ring {
    slots: Box<[Option<f64>]>,
    next: usize,
    len: usize,
}
impl Ring {
    fn new(n: usize) -> Self {
        Self {
            slots: vec![None; n].into_boxed_slice(),
            next: 0,
            len: 0,
        }
    }
    fn push(&mut self, v: Option<f64>) {
        self.slots[self.next] = v;
        self.next = (self.next + 1) % self.slots.len();
        self.len = (self.len + 1).min(self.slots.len())
    }
    fn complete(&self) -> bool {
        self.len == self.slots.len() && self.slots.iter().all(Option::is_some)
    }
    fn values(&self) -> Vec<f64> {
        (0..self.len)
            .rev()
            .map(|age| {
                self.slots[(self.next + self.slots.len() - 1 - age) % self.slots.len()].unwrap()
            })
            .collect()
    }
}
fn wma(values: &[f64]) -> Result<f64, String> {
    let total = values.len() * (values.len() + 1) / 2;
    crate::numeric::weighted_mean(
        values.iter().enumerate().map(|(i, v)| (*v, i as u64 + 1)),
        total as u64,
    )
}
#[derive(Clone)]
struct ScalarRecursiveEvaluator {
    calculation: RecursiveCalculation,
    periods: [usize; 4],
    prices: Ring,
    derived: Ring,
    kama: Option<f64>,
}
impl ScalarRecursiveEvaluator {
    fn new(c: RecursiveCalculation, p: [usize; 4]) -> Result<Self, String> {
        Ok(Self {
            calculation: c,
            periods: p,
            prices: Ring::new(
                if matches!(
                    c,
                    RecursiveCalculation::Kama | RecursiveCalculation::KamaSmoothingConstant
                ) {
                    p[0] + 1
                } else {
                    p[0].max(1)
                },
            ),
            derived: Ring::new(p[2].max(1)),
            kama: None,
        })
    }
}
impl MaterialEvaluator for ScalarRecursiveEvaluator {
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
            match inputs[0] {
                Value::Price(v) if v.is_finite() => Some(v),
                Value::Missing(_) => None,
                _ => return Err("recursive input must be Price".into()),
            }
        };
        let Some(current) = sample else {
            self.prices = Ring::new(self.periods[0]);
            self.derived = Ring::new(self.periods[2].max(1));
            self.kama = None;
            return Ok(Value::Missing(
                if self.calculation == RecursiveCalculation::KamaSmoothingConstant {
                    ScalarType::Ratio
                } else {
                    ScalarType::Price
                },
            ));
        };
        self.prices.push(Some(current));
        match self.calculation {
            RecursiveCalculation::Hma => {
                if !self.prices.complete() {
                    return Ok(Value::Missing(ScalarType::Price));
                }
                let values = self.prices.values();
                let half = self.periods[1];
                let d = 2.0 * wma(&values[values.len() - half..])? - wma(&values)?;
                self.derived.push(Some(d));
                if !self.derived.complete() {
                    Ok(Value::Missing(ScalarType::Price))
                } else {
                    Ok(Value::Price(wma(&self.derived.values())?))
                }
            }
            RecursiveCalculation::Kama | RecursiveCalculation::KamaSmoothingConstant => {
                if !self.prices.complete() {
                    return Ok(Value::Missing(
                        if self.calculation == RecursiveCalculation::Kama {
                            ScalarType::Price
                        } else {
                            ScalarType::Ratio
                        },
                    ));
                }
                let v = self.prices.values();
                let movement = (v.last().unwrap() - v[0]).abs();
                let path = v.windows(2).map(|w| (w[1] - w[0]).abs()).sum::<f64>();
                let er = if path == 0.0 { 0.0 } else { movement / path };
                let fast = 2.0 / (self.periods[1] as f64 + 1.0);
                let slow = 2.0 / (self.periods[2] as f64 + 1.0);
                let sc = (er * (fast - slow) + slow).powi(2);
                let prior = self.kama.unwrap_or(v[v.len() - 2]);
                let next = prior + sc * (current - prior);
                self.kama = Some(next);
                Ok(if self.calculation == RecursiveCalculation::Kama {
                    Value::Price(next)
                } else {
                    Value::Ratio(sc)
                })
            }
            _ => Err("scalar recursive calculation routed incorrectly".into()),
        }
    }
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
            .ok_or_else(|| "bar ring bound overflow".into())
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
    fn values(&self) -> Vec<&CompletedBar> {
        (0..self.len).rev().map(|a| self.get(a)).collect()
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
    fn push(&mut self, b: &CompletedBar) -> Result<(Option<f64>, Option<f64>), String> {
        let before = self.value;
        let tr = self.previous.map_or(b.high - b.low, |p| {
            (b.high - b.low)
                .max((b.high - p).abs())
                .max((b.low - p).abs())
        });
        self.previous = Some(b.close);
        let after = if let Some(v) = self.value {
            Some(crate::numeric::rma_step(tr, v, self.period)?)
        } else {
            self.seed.push(Some(tr))?;
            self.seed.mean()?
        };
        if let Some(v) = after {
            self.value = Some(v)
        }
        Ok((before, after))
    }
}
#[derive(Clone)]
struct Ema {
    period: usize,
    seed: crate::numeric::ObservedWindow,
    value: Option<f64>,
}
impl Ema {
    fn new(n: usize) -> Result<Self, String> {
        Ok(Self {
            period: n,
            seed: crate::numeric::ObservedWindow::new(n)?,
            value: None,
        })
    }
    fn push(&mut self, v: f64) -> Result<Option<f64>, String> {
        let next = if let Some(p) = self.value {
            Some(crate::numeric::ema_step(v, p, self.period)?)
        } else {
            self.seed.push(Some(v))?;
            self.seed.mean()?
        };
        if let Some(v) = next {
            self.value = Some(v)
        }
        Ok(next)
    }
}
#[derive(Clone)]
struct NormalizedEvaluator {
    calculation: NormalizedCalculation,
    source: SourceId,
    horizon: usize,
    current_atr: bool,
    bars: BarRing,
    atr: Atr,
}
impl NormalizedEvaluator {
    fn new(
        c: NormalizedCalculation,
        source: SourceId,
        atr: usize,
        horizon: usize,
        current: bool,
    ) -> Result<Self, String> {
        Ok(Self {
            calculation: c,
            source,
            horizon,
            current_atr: current,
            bars: BarRing::new(horizon + 1)?,
            atr: Atr::new(atr)?,
        })
    }
}
impl MaterialEvaluator for NormalizedEvaluator {
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
            .ok_or_else(|| "normalized primitive missing source".to_string())?
            .bar
            .clone();
        let (before, after) = self.atr.push(&b)?;
        self.bars.push(b);
        let denominator = if self.current_atr { after } else { before };
        let Some(a) = denominator.filter(|v| *v > 0.0) else {
            return Ok(Value::Missing(ScalarType::Ratio));
        };
        let current = self.bars.get(0);
        let numerator = match self.calculation {
            NormalizedCalculation::BodySignedAtr => current.close - current.open,
            NormalizedCalculation::BodyAbsAtr => (current.close - current.open).abs(),
            NormalizedCalculation::RangeAtr => current.high - current.low,
            NormalizedCalculation::GapAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                current.open - self.bars.get(1).close
            }
            NormalizedCalculation::MoveAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                current.close - self.bars.get(self.horizon).close
            }
            NormalizedCalculation::HighChangeAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                current.high - self.bars.get(self.horizon).high
            }
            NormalizedCalculation::LowChangeAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                current.low - self.bars.get(self.horizon).low
            }
            NormalizedCalculation::DonchianUpperDistanceAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                let upper = (1..=self.horizon)
                    .map(|age| self.bars.get(age).high)
                    .fold(f64::NEG_INFINITY, f64::max);
                current.close - upper
            }
            NormalizedCalculation::DonchianLowerDistanceAtr => {
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Ratio));
                }
                let lower = (1..=self.horizon)
                    .map(|age| self.bars.get(age).low)
                    .fold(f64::INFINITY, f64::min);
                lower - current.close
            }
            _ => return Err("pair normalization was routed to the bar normalizer".into()),
        };
        Ok(Value::Ratio(numerator / a))
    }
}
#[derive(Clone)]
struct BarRecursiveEvaluator {
    calculation: RecursiveCalculation,
    source: SourceId,
    parameters: [u64; 2],
    bars: BarRing,
    atr: Atr,
    ema: Ema,
    ha: Option<(f64, f64)>,
    supertrend: Option<(f64, f64, i64, f64)>,
}
impl BarRecursiveEvaluator {
    fn new(
        c: RecursiveCalculation,
        source: SourceId,
        p: [usize; 4],
        parameters: [u64; 2],
    ) -> Result<Self, String> {
        Ok(Self {
            calculation: c,
            source,
            bars: BarRing::new(match c {
                RecursiveCalculation::BollingerKeltnerSqueeze => p[0],
                _ => p[0].max(p[1]).max(p[2]).max(1),
            })?,
            atr: Atr::new(
                match c {
                    RecursiveCalculation::BollingerKeltnerSqueeze => p[2],
                    RecursiveCalculation::KeltnerMiddle
                    | RecursiveCalculation::KeltnerUpper
                    | RecursiveCalculation::KeltnerLower => p[1],
                    _ => p[0],
                }
                .max(1),
            )?,
            ema: Ema::new(
                match c {
                    RecursiveCalculation::BollingerKeltnerSqueeze => p[1],
                    _ => p[0],
                }
                .max(1),
            )?,
            parameters,
            ha: None,
            supertrend: None,
        })
    }
}
impl MaterialEvaluator for BarRecursiveEvaluator {
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
            .ok_or_else(|| "recursive bar primitive missing source".to_string())?
            .bar
            .clone();
        self.bars.push(b.clone());
        match self.calculation {
            RecursiveCalculation::HeikinAshiOpen
            | RecursiveCalculation::HeikinAshiHigh
            | RecursiveCalculation::HeikinAshiLow
            | RecursiveCalculation::HeikinAshiClose => {
                let close = (b.open + b.high + b.low + b.close) / 4.0;
                let open = self
                    .ha
                    .map_or((b.open + b.close) / 2.0, |(o, c)| (o + c) / 2.0);
                self.ha = Some((open, close));
                Ok(Value::Price(match self.calculation {
                    RecursiveCalculation::HeikinAshiOpen => open,
                    RecursiveCalculation::HeikinAshiHigh => b.high.max(open).max(close),
                    RecursiveCalculation::HeikinAshiLow => b.low.min(open).min(close),
                    _ => close,
                }))
            }
            RecursiveCalculation::KeltnerMiddle
            | RecursiveCalculation::KeltnerUpper
            | RecursiveCalculation::KeltnerLower => {
                let middle = self.ema.push(b.close)?;
                let (_, atr) = self.atr.push(&b)?;
                let (Some(m), Some(a)) = (middle, atr) else {
                    return Ok(Value::Missing(ScalarType::Price));
                };
                let k = f64::from_bits(self.parameters[0]);
                Ok(Value::Price(match self.calculation {
                    RecursiveCalculation::KeltnerMiddle => m,
                    RecursiveCalculation::KeltnerUpper => m + k * a,
                    _ => m - k * a,
                }))
            }
            RecursiveCalculation::BollingerKeltnerSqueeze => {
                let middle = self.ema.push(b.close)?;
                let (_, atr) = self.atr.push(&b)?;
                if !self.bars.complete() {
                    return Ok(Value::Missing(ScalarType::Bool));
                }
                let values = self
                    .bars
                    .values()
                    .into_iter()
                    .map(|b| b.close)
                    .collect::<Vec<_>>();
                let mean = crate::numeric::stable_mean(values.iter().copied())?;
                let variance =
                    crate::numeric::stable_mean(values.iter().map(|v| (v - mean).powi(2)))?;
                let (Some(km), Some(a)) = (middle, atr) else {
                    return Ok(Value::Missing(ScalarType::Bool));
                };
                let bb = f64::from_bits(self.parameters[0]) * variance.sqrt();
                let kc = f64::from_bits(self.parameters[1]) * a;
                Ok(Value::Bool(mean - bb > km - kc && mean + bb < km + kc))
            }
            RecursiveCalculation::SuperTrendLevel | RecursiveCalculation::SuperTrendDirection => {
                let (_, atr) = self.atr.push(&b)?;
                let Some(a) = atr else {
                    return Ok(Value::Missing(
                        if self.calculation == RecursiveCalculation::SuperTrendLevel {
                            ScalarType::Price
                        } else {
                            ScalarType::Number
                        },
                    ));
                };
                let midpoint = (b.high + b.low) / 2.0;
                let k = f64::from_bits(self.parameters[0]);
                let bu = midpoint + k * a;
                let bl = midpoint - k * a;
                let (fu, fl, direction) =
                    if let Some((pfu, pfl, pd, previous_close)) = self.supertrend {
                        let fu = if bu < pfu || previous_close > pfu {
                            bu
                        } else {
                            pfu
                        };
                        let fl = if bl > pfl || previous_close < pfl {
                            bl
                        } else {
                            pfl
                        };
                        let direction = if pd > 0 {
                            if b.close < fl { -1 } else { 1 }
                        } else if b.close > fu {
                            1
                        } else {
                            -1
                        };
                        (fu, fl, direction)
                    } else {
                        (bu, bl, self.parameters[1] as i64)
                    };
                self.supertrend = Some((fu, fl, direction, b.close));
                Ok(
                    if self.calculation == RecursiveCalculation::SuperTrendLevel {
                        Value::Price(if direction > 0 { fl } else { fu })
                    } else {
                        Value::Number(direction as f64)
                    },
                )
            }
            _ => Err("bar recursive calculation routed incorrectly".into()),
        }
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
    fn args(source: &SourceId, values: &[(&str, MaterialArg)]) -> MaterialArgs {
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
        parameters: &[(&str, MaterialArg)],
        values: &[Option<f64>],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut e = RecursiveFactory { key }
            .build(
                &args(&source, parameters),
                &[ValueType::optional(ScalarType::Price)],
            )
            .unwrap()
            .evaluator;
        values
            .iter()
            .map(|v| {
                let i = input(&source, b(1.0, 1.0, 1.0, 1.0));
                e.evaluate(
                    &[v.map(Value::Price)
                        .unwrap_or(Value::Missing(ScalarType::Price))],
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
        parameters: &[(&str, MaterialArg)],
        bars: &[CompletedBar],
    ) -> Vec<Value> {
        let source = SourceId::new("bars").unwrap();
        let mut e = RecursiveFactory { key }
            .build(&args(&source, parameters), &[])
            .unwrap()
            .evaluator;
        bars.iter()
            .cloned()
            .map(|bar| {
                let i = input(&source, bar);
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
    fn period(value: i64) -> [(&'static str, MaterialArg); 1] {
        [("period", MaterialArg::Integer(value))]
    }

    #[test]
    fn hma_and_kama_follow_derived_period_and_preceding_price_seeds() {
        let hma = scalar_values(
            MATERIAL_HMA,
            &period(4),
            &[Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)],
        );
        assert_eq!(
            hma[..4],
            [
                Value::Missing(ScalarType::Price),
                Value::Missing(ScalarType::Price),
                Value::Missing(ScalarType::Price),
                Value::Missing(ScalarType::Price)
            ]
        );
        assert!(
            (match hma[4] {
                Value::Price(v) => v,
                _ => unreachable!(),
            } - 5.0)
                .abs()
                < 1e-12
        );
        let parameters = [
            ("period", MaterialArg::Integer(3)),
            ("fast", MaterialArg::Integer(2)),
            ("slow", MaterialArg::Integer(30)),
        ];
        let kama = scalar_values(
            MATERIAL_KAMA,
            &parameters,
            &[Some(1.0), Some(2.0), Some(3.0), Some(4.0)],
        );
        assert!(
            (match kama[3] {
                Value::Price(v) => v,
                _ => unreachable!(),
            } - 31.0 / 9.0)
                .abs()
                < 1e-12
        );
        assert!(
            (match scalar_values(
                MATERIAL_KAMA_SMOOTHING_CONSTANT,
                &parameters,
                &[Some(1.0), Some(2.0), Some(3.0), Some(4.0)]
            )[3]
            {
                Value::Ratio(value) => value,
                _ => unreachable!(),
            } - 4.0 / 9.0)
                .abs()
                < 1e-15
        );
        let reset = scalar_values(
            MATERIAL_KAMA,
            &parameters,
            &[
                Some(1.0),
                Some(2.0),
                None,
                Some(3.0),
                Some(4.0),
                Some(5.0),
                Some(6.0),
            ],
        );
        assert!(matches!(reset[6], Value::Price(_)));
    }
    #[test]
    fn heikin_ashi_recurses_without_becoming_execution_data() {
        let bars = [b(2.0, 5.0, 1.0, 4.0), b(4.0, 6.0, 2.0, 5.0)];
        assert_eq!(
            bar_values(MATERIAL_HEIKIN_ASHI_OPEN, &[], &bars),
            vec![Value::Price(3.0), Value::Price(3.0)]
        );
        assert_eq!(
            bar_values(MATERIAL_HEIKIN_ASHI_CLOSE, &[], &bars),
            vec![Value::Price(3.0), Value::Price(4.25)]
        );
        assert_eq!(
            bar_values(MATERIAL_HEIKIN_ASHI_HIGH, &[], &bars)[1],
            Value::Price(6.0)
        );
        assert_eq!(
            bar_values(MATERIAL_HEIKIN_ASHI_LOW, &[], &bars)[1],
            Value::Price(2.0)
        );
    }
    #[test]
    fn keltner_and_squeeze_use_distinct_centers_and_current_strict_atr() {
        let parameters = [
            ("ma_period", MaterialArg::Integer(2)),
            ("atr_period", MaterialArg::Integer(2)),
            ("multiplier", MaterialArg::Number(2.0)),
        ];
        let bars = [
            b(10.0, 12.0, 8.0, 10.0),
            b(10.0, 12.0, 8.0, 10.0),
            b(11.0, 13.0, 9.0, 11.0),
        ];
        assert_eq!(
            bar_values(MATERIAL_KELTNER_MIDDLE, &parameters, &bars)[1],
            Value::Price(10.0)
        );
        assert_eq!(
            bar_values(MATERIAL_KELTNER_UPPER, &parameters, &bars)[1],
            Value::Price(18.0)
        );
        assert_eq!(
            bar_values(MATERIAL_KELTNER_LOWER, &parameters, &bars)[1],
            Value::Price(2.0)
        );
        let squeeze = [
            ("bb_period", MaterialArg::Integer(2)),
            ("bb_multiplier", MaterialArg::Number(1.0)),
            ("kc_period", MaterialArg::Integer(2)),
            ("atr_period", MaterialArg::Integer(2)),
            ("kc_multiplier", MaterialArg::Number(2.0)),
        ];
        assert_eq!(
            bar_values(MATERIAL_BB_KC_SQUEEZE, &squeeze, &bars)[1],
            Value::Bool(true)
        );
    }
    #[test]
    fn supertrend_preserves_final_bands_direction_and_seed_choice() {
        let parameters = [
            ("period", MaterialArg::Integer(1)),
            ("multiplier", MaterialArg::Number(1.0)),
            ("initial_direction", MaterialArg::Integer(1)),
        ];
        let bars = [
            b(11.0, 12.0, 10.0, 11.0),
            b(9.0, 10.0, 8.0, 8.5),
            b(9.0, 11.0, 8.0, 10.5),
        ];
        assert_eq!(
            bar_values(MATERIAL_SUPERTREND_DIRECTION, &parameters, &bars),
            vec![Value::Number(1.0), Value::Number(-1.0), Value::Number(-1.0)]
        );
        let levels = bar_values(MATERIAL_SUPERTREND_LEVEL, &parameters, &bars);
        assert_eq!(levels[0], Value::Price(9.0));
        assert_eq!(levels[1], Value::Price(12.0));
        let down = [
            ("period", MaterialArg::Integer(1)),
            ("multiplier", MaterialArg::Number(1.0)),
            ("initial_direction", MaterialArg::Integer(-1)),
        ];
        assert_eq!(
            bar_values(MATERIAL_SUPERTREND_DIRECTION, &down, &bars)[0],
            Value::Number(-1.0)
        );
    }
    #[test]
    fn previous_and_current_atr_normalization_are_distinct_and_causal() {
        let bars = [b(1.0, 3.0, 1.0, 1.0), b(2.0, 4.0, 2.0, 3.0)];
        let parameters = [("atr_period", MaterialArg::Integer(1))];
        let previous = bar_values(MATERIAL_BODY_SIGNED_ATR, &parameters, &bars);
        let current = bar_values(MATERIAL_BODY_SIGNED_CURRENT_ATR, &parameters, &bars);
        assert_eq!(previous[0], Value::Missing(ScalarType::Ratio));
        assert_eq!(previous[1], Value::Ratio(0.5));
        assert_eq!(current[0], Value::Ratio(0.0));
        assert_eq!(current[1], Value::Ratio(1.0 / 3.0));
        let gap = bar_values(MATERIAL_GAP_ATR, &parameters, &bars);
        assert_eq!(gap[1], Value::Ratio(0.5));
        let donchian = [
            ("atr_period", MaterialArg::Integer(1)),
            ("period", MaterialArg::Integer(1)),
        ];
        assert_eq!(
            bar_values(MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR, &donchian, &bars)[1],
            Value::Ratio(0.0)
        );
    }
    #[test]
    fn recursive_descriptors_cover_every_registered_key_and_bounds() {
        for key in KEYS {
            let parameters = if matches!(*key, MATERIAL_HMA) {
                vec![("period", MaterialArg::Integer(4))]
            } else if matches!(*key, MATERIAL_KAMA | MATERIAL_KAMA_SMOOTHING_CONSTANT) {
                vec![
                    ("period", MaterialArg::Integer(3)),
                    ("fast", MaterialArg::Integer(2)),
                    ("slow", MaterialArg::Integer(30)),
                ]
            } else if matches!(
                *key,
                MATERIAL_KELTNER_MIDDLE | MATERIAL_KELTNER_UPPER | MATERIAL_KELTNER_LOWER
            ) {
                vec![
                    ("ma_period", MaterialArg::Integer(3)),
                    ("atr_period", MaterialArg::Integer(3)),
                ]
            } else if *key == MATERIAL_BB_KC_SQUEEZE {
                vec![
                    ("bb_period", MaterialArg::Integer(3)),
                    ("kc_period", MaterialArg::Integer(3)),
                    ("atr_period", MaterialArg::Integer(3)),
                ]
            } else if matches!(
                *key,
                MATERIAL_SUPERTREND_LEVEL | MATERIAL_SUPERTREND_DIRECTION
            ) {
                vec![("period", MaterialArg::Integer(3))]
            } else if matches!(
                *key,
                MATERIAL_HEIKIN_ASHI_OPEN
                    | MATERIAL_HEIKIN_ASHI_HIGH
                    | MATERIAL_HEIKIN_ASHI_LOW
                    | MATERIAL_HEIKIN_ASHI_CLOSE
            ) {
                vec![]
            } else if matches!(
                *key,
                MATERIAL_MOVE_ATR
                    | MATERIAL_HIGH_CHANGE_ATR
                    | MATERIAL_LOW_CHANGE_ATR
                    | MATERIAL_MOVE_CURRENT_ATR
                    | MATERIAL_HIGH_CHANGE_CURRENT_ATR
                    | MATERIAL_LOW_CHANGE_CURRENT_ATR
            ) {
                vec![
                    ("atr_period", MaterialArg::Integer(3)),
                    ("horizon", MaterialArg::Integer(2)),
                ]
            } else if matches!(
                *key,
                MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR
                    | MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR
                    | MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR
                    | MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR
            ) {
                vec![
                    ("atr_period", MaterialArg::Integer(3)),
                    ("period", MaterialArg::Integer(2)),
                ]
            } else {
                vec![("atr_period", MaterialArg::Integer(3))]
            };
            let source = SourceId::new("bars").unwrap();
            let scalar = matches!(
                *key,
                MATERIAL_HMA | MATERIAL_KAMA | MATERIAL_KAMA_SMOOTHING_CONSTANT
            );
            let inputs = if scalar {
                vec![ValueType::optional(ScalarType::Price)]
            } else {
                vec![]
            };
            let d = RecursiveFactory { key }
                .numeric_descriptor(&args(&source, &parameters), &inputs)
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
