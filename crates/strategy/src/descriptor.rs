use crate::{BarField, SourceId, ValueType};

/// Semantic unit carried by a named numeric or predicate output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NumericUnit {
    Bool,
    Number,
    Price,
    Ratio,
    Percent,
    BarCount,
    PricePerObservation,
    PricePerObservationSquared,
    RatioPerObservation,
    RatioPerObservationSquared,
    LogReturn,
    LogReturnVariance,
}

/// Declared input shape. Factories validate only the inputs needed by their calculation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NumericInputs {
    Scalar(Vec<ValueType>),
    CompletedBarFields(&'static [BarField]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BarShapeCalculation {
    BodySignedAtr,
    BodyAbsAtr,
    BodyFraction,
    BodyDirectionFraction,
    UpperWickFraction,
    LowerWickFraction,
    ClosePosition,
    CloseLocationValue,
    RangeAtr,
    GapAtr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PriceChangeCalculation {
    LogReturn,
    Roc,
    MoveAtr,
    HighChangeAtr,
    LowChangeAtr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PriceInputCalculation {
    Close,
    Hl2,
    Hlc3,
    Ohlc4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MovingAverageCalculation {
    Sma,
    Ema,
    Rma,
    Wma,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecursiveCalculation {
    Hma,
    Kama,
    KamaSmoothingConstant,
    KeltnerMiddle,
    KeltnerUpper,
    KeltnerLower,
    BollingerKeltnerSqueeze,
    SuperTrendLevel,
    SuperTrendDirection,
    HeikinAshiOpen,
    HeikinAshiHigh,
    HeikinAshiLow,
    HeikinAshiClose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NormalizedCalculation {
    BodySignedAtr,
    BodyAbsAtr,
    RangeAtr,
    GapAtr,
    MoveAtr,
    HighChangeAtr,
    LowChangeAtr,
    DonchianUpperDistanceAtr,
    DonchianLowerDistanceAtr,
    MacdAtr,
    MacdSignalAtr,
    MacdHistogramAtr,
    MaDistanceAtr,
    MaSlopeAtr,
    MaAccelerationAtr,
    MaGapAtr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StatisticalCalculation {
    DonchianUpper,
    DonchianLower,
    ChannelWidth,
    ChannelMidDistance,
    RangePosition,
    PreviousRangePosition,
    ZScore,
    BollingerUpper,
    BollingerLower,
    BollingerPercentB,
    BollingerWidth,
    RollingMedian,
    MedianDeviation,
    RealizedVariance,
    RealizedVolatility,
    ReturnRms,
    ReturnStdDev,
    EwmaVolatility,
    PositiveSemivariance,
    NegativeSemivariance,
    VolatilityAsymmetry,
    HistoricalPercentile,
    EfficiencyRatio,
    RegressionSlope,
    RegressionR2,
    RegressionResidualRms,
    RegressionDeviation,
    RollingHighAge,
    RollingLowAge,
    AroonUp,
    AroonDown,
    Choppiness,
    ReturnAutocorrelation,
    RangeExpansion,
    AtrPercent,
    AtrRatio,
    AtrChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MomentumCalculation {
    WilderRsi,
    RsiChange,
    StochasticFastK,
    StochasticSlowK,
    StochasticSlowD,
    Macd,
    MacdSignal,
    MacdHistogram,
    Cci,
    PlusDi,
    MinusDi,
    DiDifference,
    Dx,
    Adx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MaDerivativeCalculation {
    Distance,
    Slope,
    Acceleration,
    Gap,
    Alignment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BarStructureCalculation {
    InsideBar,
    OutsideBar,
    BodyEngulfing,
    EngulfSizeRatio,
    NarrowRange,
    WideRange,
    RelativeRange,
    BarOverlap,
    ThreeBarGapUp,
    ThreeBarGapDown,
}

/// The complete effective calculation, including bounded lengths and recursive weights.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum NumericCalculation {
    ObservedSma {
        period: usize,
    },
    SmaSeededEma {
        period: usize,
        alpha: f64,
    },
    BarShape(BarShapeCalculation),
    PriceChange {
        calculation: PriceChangeCalculation,
        horizon: usize,
    },
    BarStructure {
        calculation: BarStructureCalculation,
        period: Option<usize>,
    },
    PriceInput(PriceInputCalculation),
    MovingAverage {
        calculation: MovingAverageCalculation,
        period: usize,
    },
    TrueRange,
    StrictAtr {
        period: usize,
    },
    MaDerivative {
        calculation: MaDerivativeCalculation,
        horizon: Option<usize>,
    },
    Momentum {
        calculation: MomentumCalculation,
        periods: [usize; 3],
    },
    Statistical {
        calculation: StatisticalCalculation,
        periods: [usize; 3],
        parameter: Option<u64>,
    },
    Recursive {
        calculation: RecursiveCalculation,
        periods: [usize; 4],
        parameters: [u64; 2],
    },
    Normalized {
        calculation: NormalizedCalculation,
        atr_period: usize,
        horizon: usize,
        current_atr: bool,
    },
    NormalizedPair {
        calculation: NormalizedCalculation,
    },
}

/// Behavior on an observed Missing input, distinct from an idle source clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NumericMissingPolicy {
    ConsumeWindowSlot,
    ResetAndReseed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum NumericRange {
    Unbounded,
    Inclusive { minimum: f64, maximum: f64 },
}

/// A working factory's effective numeric contract, inspected before state allocation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NumericDescriptor {
    pub calculation: NumericCalculation,
    pub source_clock: SourceId,
    pub inputs: NumericInputs,
    pub output_type: ValueType,
    pub unit: NumericUnit,
    pub range: NumericRange,
    pub missing: NumericMissingPolicy,
    /// Source observations required for the first output after initialization or reset.
    pub first_output_observations: usize,
    /// Effective source lookback after composing direct inputs and calculation history.
    pub required_lookback: usize,
    /// Upper bound on one evaluator's owned state, including fixed backing storage.
    pub max_state_bytes: usize,
    pub exact_aliases: &'static [&'static str],
}

impl NumericDescriptor {
    pub(crate) fn validate(&self, inputs: &[ValueType]) -> Result<(), String> {
        if self.first_output_observations == 0
            || self.required_lookback < self.first_output_observations
            || self.max_state_bytes > crate::MAX_MATERIAL_STATE_BYTES
        {
            return Err("numeric descriptor has an invalid history or state bound".into());
        }
        match &self.inputs {
            NumericInputs::Scalar(expected) if expected == inputs => {}
            NumericInputs::CompletedBarFields(fields)
                if inputs.is_empty() && !fields.is_empty() => {}
            _ => return Err("numeric descriptor disagrees with its material inputs".into()),
        }
        let scalar = self.output_type.scalar;
        let unit_matches = match self.unit {
            NumericUnit::Bool => scalar == crate::ScalarType::Bool,
            NumericUnit::BarCount => scalar == crate::ScalarType::Integer,
            NumericUnit::Price => scalar == crate::ScalarType::Price,
            NumericUnit::Ratio => scalar == crate::ScalarType::Ratio,
            NumericUnit::Percent => scalar == crate::ScalarType::Percent,
            NumericUnit::PricePerObservation => scalar == crate::ScalarType::PricePerObservation,
            NumericUnit::PricePerObservationSquared => {
                scalar == crate::ScalarType::PricePerObservationSquared
            }
            NumericUnit::RatioPerObservation => scalar == crate::ScalarType::RatioPerObservation,
            NumericUnit::RatioPerObservationSquared => {
                scalar == crate::ScalarType::RatioPerObservationSquared
            }
            NumericUnit::LogReturn => scalar == crate::ScalarType::LogReturn,
            NumericUnit::LogReturnVariance => scalar == crate::ScalarType::LogReturnVariance,
            NumericUnit::Number => scalar == crate::ScalarType::Number,
        };
        if !unit_matches {
            return Err("numeric descriptor unit disagrees with its output scalar".into());
        }
        if let NumericRange::Inclusive { minimum, maximum } = self.range
            && (!minimum.is_finite() || !maximum.is_finite() || minimum > maximum)
        {
            return Err("numeric descriptor has an invalid range".into());
        }
        match self.calculation {
            NumericCalculation::ObservedSma { period } => {
                self.validate_average(inputs, period, NumericMissingPolicy::ConsumeWindowSlot)?;
            }
            NumericCalculation::SmaSeededEma { period, alpha } => {
                if alpha != 2.0 / (period as f64 + 1.0) {
                    return Err("numeric descriptor has an inconsistent EMA weight".into());
                }
                self.validate_average(inputs, period, NumericMissingPolicy::ResetAndReseed)?;
            }
            NumericCalculation::BarShape(_)
            | NumericCalculation::PriceChange { .. }
            | NumericCalculation::BarStructure { .. }
            | NumericCalculation::PriceInput(_)
            | NumericCalculation::TrueRange
            | NumericCalculation::StrictAtr { .. } => {
                if !matches!(self.inputs, NumericInputs::CompletedBarFields(_)) {
                    return Err("bar calculation requires completed-bar fields".into());
                }
            }
            NumericCalculation::MovingAverage { period, .. } => {
                self.validate_average(inputs, period, self.missing)?;
            }
            NumericCalculation::MaDerivative { .. } => {
                if !matches!(self.inputs, NumericInputs::Scalar(_)) {
                    return Err("MA derivative requires scalar material inputs".into());
                }
            }
            NumericCalculation::Momentum { .. }
            | NumericCalculation::Statistical { .. }
            | NumericCalculation::Recursive { .. }
            | NumericCalculation::Normalized { .. }
            | NumericCalculation::NormalizedPair { .. } => {}
        }
        Ok(())
    }

    fn validate_average(
        &self,
        inputs: &[ValueType],
        period: usize,
        missing: NumericMissingPolicy,
    ) -> Result<(), String> {
        if !(1..=crate::numeric::MAX_STRICT_PERIOD).contains(&period)
            || inputs.len() != 1
            || !matches!(
                inputs[0].scalar,
                crate::ScalarType::Number
                    | crate::ScalarType::Price
                    | crate::ScalarType::Ratio
                    | crate::ScalarType::Percent
                    | crate::ScalarType::PricePerObservation
                    | crate::ScalarType::PricePerObservationSquared
                    | crate::ScalarType::RatioPerObservation
                    | crate::ScalarType::RatioPerObservationSquared
                    | crate::ScalarType::LogReturn
                    | crate::ScalarType::LogReturnVariance
            )
            || self.output_type != ValueType::optional(inputs[0].scalar)
            || self.missing != missing
            || self.first_output_observations != period
            || self.required_lookback != period
        {
            return Err("numeric average descriptor is inconsistent".into());
        }
        Ok(())
    }

    pub const fn flat_input_is_defined(&self) -> bool {
        matches!(
            self.calculation,
            NumericCalculation::ObservedSma { .. } | NumericCalculation::SmaSeededEma { .. }
        )
    }
}
