use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_strategy::*;

fn source() -> SourceId {
    SourceId::new("bars").unwrap()
}
fn params(values: &[(&str, MaterialArg)]) -> MaterialArgs {
    let mut all = vec![("source", MaterialArg::Source(source()))];
    all.extend(values.iter().cloned());
    MaterialArgs::new(all)
}
fn period() -> Vec<(&'static str, MaterialArg)> {
    vec![("period", MaterialArg::Integer(3))]
}
#[derive(Clone)]
pub struct Spec {
    pub key: &'static str,
    pub params: Vec<(&'static str, MaterialArg)>,
    pub inputs: Vec<ValueType>,
}
fn spec(
    key: &'static str,
    params: Vec<(&'static str, MaterialArg)>,
    inputs: Vec<ValueType>,
) -> Spec {
    Spec {
        key,
        params,
        inputs,
    }
}
fn price() -> ValueType {
    ValueType::optional(ScalarType::Price)
}
fn no_params() -> Vec<(&'static str, MaterialArg)> {
    Vec::new()
}
fn no_inputs() -> Vec<ValueType> {
    Vec::new()
}
pub fn catalog() -> Vec<Spec> {
    let mut v = vec![
        spec(MATERIAL_STRICT_SMA, period(), vec![price()]),
        spec(MATERIAL_STRICT_EMA, period(), vec![price()]),
        spec(MATERIAL_STRICT_WMA, period(), vec![price()]),
        spec(MATERIAL_STRICT_RMA, period(), vec![price()]),
    ];
    for key in [
        MATERIAL_BODY_FRACTION,
        MATERIAL_BODY_DIRECTION_FRACTION,
        MATERIAL_UPPER_WICK_FRACTION,
        MATERIAL_LOWER_WICK_FRACTION,
        MATERIAL_CLOSE_POSITION,
        MATERIAL_CLOSE_LOCATION_VALUE,
        MATERIAL_INSIDE_BAR,
        MATERIAL_OUTSIDE_BAR,
        MATERIAL_BODY_ENGULFING,
        MATERIAL_ENGULF_SIZE_RATIO,
        MATERIAL_BAR_OVERLAP,
        MATERIAL_THREE_BAR_GAP_UP,
        MATERIAL_THREE_BAR_GAP_DOWN,
        MATERIAL_CLOSE_PRICE,
        MATERIAL_HL2,
        MATERIAL_HLC3,
        MATERIAL_OHLC4,
        MATERIAL_TRUE_RANGE,
        MATERIAL_HEIKIN_ASHI_OPEN,
        MATERIAL_HEIKIN_ASHI_HIGH,
        MATERIAL_HEIKIN_ASHI_LOW,
        MATERIAL_HEIKIN_ASHI_CLOSE,
        MATERIAL_RANGE_EXPANSION,
    ] {
        v.push(spec(key, no_params(), no_inputs()))
    }
    for key in [MATERIAL_RETURN_LOG, MATERIAL_ROC] {
        v.push(spec(
            key,
            vec![("horizon", MaterialArg::Integer(2))],
            no_inputs(),
        ))
    }
    for key in [
        MATERIAL_NARROW_RANGE,
        MATERIAL_WIDE_RANGE,
        MATERIAL_RELATIVE_RANGE,
        MATERIAL_STRICT_ATR,
        MATERIAL_CCI,
        MATERIAL_PLUS_DI,
        MATERIAL_MINUS_DI,
        MATERIAL_DI_DIFFERENCE,
        MATERIAL_DX,
        MATERIAL_ADX,
        MATERIAL_DONCHIAN_UPPER,
        MATERIAL_DONCHIAN_LOWER,
        MATERIAL_CHANNEL_WIDTH,
        MATERIAL_CHANNEL_MID_DISTANCE,
        MATERIAL_RANGE_POSITION,
        MATERIAL_PREVIOUS_RANGE_POSITION,
        MATERIAL_ROLLING_HIGH_AGE,
        MATERIAL_ROLLING_LOW_AGE,
        MATERIAL_AROON_UP,
        MATERIAL_AROON_DOWN,
        MATERIAL_CHOPPINESS,
        MATERIAL_ATR_PERCENT,
        MATERIAL_SUPERTREND_LEVEL,
        MATERIAL_SUPERTREND_DIRECTION,
    ] {
        v.push(spec(key, period(), no_inputs()))
    }
    for key in [MATERIAL_MA_DISTANCE, MATERIAL_MA_GAP, MATERIAL_MA_ALIGNMENT] {
        v.push(spec(key, no_params(), vec![price(), price()]))
    }
    for key in [MATERIAL_MA_SLOPE, MATERIAL_MA_ACCELERATION] {
        v.push(spec(
            key,
            vec![("horizon", MaterialArg::Integer(2))],
            vec![price()],
        ))
    }
    v.push(spec(MATERIAL_STRICT_RSI, period(), vec![price()]));
    v.push(spec(
        MATERIAL_RSI_CHANGE,
        vec![("horizon", MaterialArg::Integer(2))],
        vec![ValueType::optional(ScalarType::Percent)],
    ));
    for key in [
        MATERIAL_STOCHASTIC_FAST_K,
        MATERIAL_STOCHASTIC_SLOW_K,
        MATERIAL_STOCHASTIC_SLOW_D,
    ] {
        v.push(spec(
            key,
            vec![
                ("period", MaterialArg::Integer(3)),
                ("smooth_k", MaterialArg::Integer(2)),
                ("smooth_d", MaterialArg::Integer(2)),
            ],
            no_inputs(),
        ))
    }
    for key in [MATERIAL_MACD, MATERIAL_MACD_SIGNAL, MATERIAL_MACD_HISTOGRAM] {
        v.push(spec(
            key,
            vec![
                ("fast", MaterialArg::Integer(2)),
                ("slow", MaterialArg::Integer(3)),
                ("signal", MaterialArg::Integer(2)),
            ],
            vec![price()],
        ))
    }
    for key in [
        MATERIAL_ZSCORE,
        MATERIAL_ROLLING_MEDIAN,
        MATERIAL_MEDIAN_DEVIATION,
        MATERIAL_REALIZED_VARIANCE,
        MATERIAL_REALIZED_VOLATILITY,
        MATERIAL_RETURN_RMS,
        MATERIAL_RETURN_STDDEV,
        MATERIAL_POSITIVE_SEMIVARIANCE,
        MATERIAL_NEGATIVE_SEMIVARIANCE,
        MATERIAL_VOLATILITY_ASYMMETRY,
        MATERIAL_HISTORICAL_PERCENTILE,
        MATERIAL_EFFICIENCY_RATIO,
        MATERIAL_REGRESSION_SLOPE,
        MATERIAL_REGRESSION_R2,
        MATERIAL_REGRESSION_RESIDUAL_RMS,
        MATERIAL_REGRESSION_DEVIATION,
    ] {
        v.push(spec(key, period(), vec![price()]))
    }
    for key in [
        MATERIAL_BOLLINGER_UPPER,
        MATERIAL_BOLLINGER_LOWER,
        MATERIAL_BOLLINGER_PERCENT_B,
        MATERIAL_BOLLINGER_WIDTH,
    ] {
        v.push(spec(
            key,
            vec![
                ("period", MaterialArg::Integer(3)),
                ("multiplier", MaterialArg::Number(2.0)),
            ],
            vec![price()],
        ))
    }
    v.push(spec(
        MATERIAL_EWMA_VOLATILITY,
        vec![("lambda", MaterialArg::Number(0.8))],
        vec![price()],
    ));
    v.push(spec(
        MATERIAL_RETURN_AUTOCORRELATION,
        vec![
            ("period", MaterialArg::Integer(3)),
            ("lag", MaterialArg::Integer(1)),
        ],
        vec![price()],
    ));
    v.push(spec(
        MATERIAL_ATR_RATIO,
        vec![
            ("short", MaterialArg::Integer(2)),
            ("long", MaterialArg::Integer(3)),
        ],
        no_inputs(),
    ));
    v.push(spec(
        MATERIAL_ATR_CHANGE,
        vec![
            ("period", MaterialArg::Integer(2)),
            ("horizon", MaterialArg::Integer(2)),
        ],
        no_inputs(),
    ));
    v.push(spec(
        MATERIAL_HMA,
        vec![("period", MaterialArg::Integer(4))],
        vec![price()],
    ));
    for key in [MATERIAL_KAMA, MATERIAL_KAMA_SMOOTHING_CONSTANT] {
        v.push(spec(
            key,
            vec![
                ("period", MaterialArg::Integer(3)),
                ("fast", MaterialArg::Integer(2)),
                ("slow", MaterialArg::Integer(5)),
            ],
            vec![price()],
        ))
    }
    for key in [
        MATERIAL_KELTNER_MIDDLE,
        MATERIAL_KELTNER_UPPER,
        MATERIAL_KELTNER_LOWER,
    ] {
        v.push(spec(
            key,
            vec![
                ("ma_period", MaterialArg::Integer(3)),
                ("atr_period", MaterialArg::Integer(3)),
                ("multiplier", MaterialArg::Number(2.0)),
            ],
            no_inputs(),
        ))
    }
    v.push(spec(
        MATERIAL_BB_KC_SQUEEZE,
        vec![
            ("bb_period", MaterialArg::Integer(3)),
            ("bb_multiplier", MaterialArg::Number(1.0)),
            ("kc_period", MaterialArg::Integer(3)),
            ("atr_period", MaterialArg::Integer(3)),
            ("kc_multiplier", MaterialArg::Number(2.0)),
        ],
        no_inputs(),
    ));
    for key in [
        MATERIAL_BODY_SIGNED_ATR,
        MATERIAL_BODY_ABS_ATR,
        MATERIAL_RANGE_ATR,
        MATERIAL_GAP_ATR,
        MATERIAL_BODY_SIGNED_CURRENT_ATR,
        MATERIAL_BODY_ABS_CURRENT_ATR,
        MATERIAL_RANGE_CURRENT_ATR,
        MATERIAL_GAP_CURRENT_ATR,
    ] {
        v.push(spec(
            key,
            vec![("atr_period", MaterialArg::Integer(3))],
            no_inputs(),
        ))
    }
    for key in [
        MATERIAL_MOVE_ATR,
        MATERIAL_HIGH_CHANGE_ATR,
        MATERIAL_LOW_CHANGE_ATR,
        MATERIAL_MOVE_CURRENT_ATR,
        MATERIAL_HIGH_CHANGE_CURRENT_ATR,
        MATERIAL_LOW_CHANGE_CURRENT_ATR,
    ] {
        v.push(spec(
            key,
            vec![
                ("atr_period", MaterialArg::Integer(3)),
                ("horizon", MaterialArg::Integer(2)),
            ],
            no_inputs(),
        ))
    }
    for key in [
        MATERIAL_DONCHIAN_UPPER_DISTANCE_ATR,
        MATERIAL_DONCHIAN_LOWER_DISTANCE_ATR,
        MATERIAL_DONCHIAN_UPPER_DISTANCE_CURRENT_ATR,
        MATERIAL_DONCHIAN_LOWER_DISTANCE_CURRENT_ATR,
    ] {
        v.push(spec(
            key,
            vec![
                ("atr_period", MaterialArg::Integer(3)),
                ("period", MaterialArg::Integer(2)),
            ],
            no_inputs(),
        ))
    }
    let pair = |key, numerator| {
        spec(
            key,
            no_params(),
            vec![ValueType::optional(numerator), price()],
        )
    };
    for key in [
        MATERIAL_MACD_ATR,
        MATERIAL_MACD_SIGNAL_ATR,
        MATERIAL_MACD_HISTOGRAM_ATR,
        MATERIAL_MA_DISTANCE_ATR,
        MATERIAL_MA_GAP_ATR,
    ] {
        v.push(pair(key, ScalarType::Price))
    }
    v.push(pair(MATERIAL_MA_SLOPE_ATR, ScalarType::PricePerObservation));
    v.push(pair(
        MATERIAL_MA_ACCELERATION_ATR,
        ScalarType::PricePerObservationSquared,
    ));
    v
}
fn time(i: usize) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        + Duration::minutes(i as i64)
}
pub fn bar(i: usize) -> CompletedBar {
    let close = 100.0 + i as f64 * 0.13 + ((i * 7) % 11) as f64 * 0.07;
    let open = close + if i.is_multiple_of(2) { -0.2 } else { 0.15 };
    CompletedBar {
        open,
        high: open.max(close) + 1.0 + (i % 3) as f64 * 0.1,
        low: open.min(close) - 1.0 - usize::from(!i.is_multiple_of(2)) as f64 * 0.1,
        close,
        volume: Some(10.0 + i as f64),
    }
}
pub fn named_value(index: usize, ty: ValueType, i: usize) -> NamedValue {
    let base = 100.0 + i as f64 * 0.13 + ((i * 7) % 11) as f64 * 0.07;
    let value = match ty.scalar {
        ScalarType::Price => Value::Price(if index == 1 { 2.0 } else { base }),
        ScalarType::Percent => Value::Percent(20.0 + ((i * 13) % 17) as f64),
        ScalarType::PricePerObservation => Value::PricePerObservation(0.5 + i as f64 * 0.01),
        ScalarType::PricePerObservationSquared => {
            Value::PricePerObservationSquared(0.1 + i as f64 * 0.001)
        }
        other => panic!("unsupported fixture input {other:?}"),
    };
    NamedValue {
        name: format!("input_{index}"),
        value,
        updated: true,
    }
}
pub fn document(spec: &Spec) -> StrategyConfig {
    let inputs = spec
        .inputs
        .iter()
        .enumerate()
        .map(|(index, ty)| Expr::Input {
            field: format!("input_{index}"),
            value_type: *ty,
        })
        .collect();
    StrategyConfig {
        strategy_id: "catalog".into(),
        title: "Numeric catalog consumer".into(),
        parameters: vec![],
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec![],
        materials: vec![MaterialConfig {
            id: "feature".into(),
            key: spec.key.into(),
            inputs,
            params: params(&spec.params),
        }],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::IsPresent {
                        value: Box::new(Expr::Material {
                            id: "feature".into(),
                        }),
                    },
                    assignments: vec![],
                    decision: None,
                    actions: vec![],
                    notes: vec![NoteTemplate {
                        kind: NoteKind::Observation,
                        reason: "numeric catalog value".into(),
                        trade_slot: None,
                        values: vec![NamedExpr {
                            name: "feature".into(),
                            value: Expr::Material {
                                id: "feature".into(),
                            },
                        }],
                    }],
                }],
            },
            StateConfig {
                id: "done".into(),
                transitions: vec![],
            },
        ],
    }
}

fn output_value(value: &OutputScalar) -> f64 {
    match value {
        OutputScalar::Bool(value) => f64::from(*value),
        OutputScalar::Integer(value) => *value as f64,
        OutputScalar::Number(value)
        | OutputScalar::Price(value)
        | OutputScalar::Ratio(value)
        | OutputScalar::Percent(value)
        | OutputScalar::PricePerObservation(value)
        | OutputScalar::PricePerObservationSquared(value)
        | OutputScalar::RatioPerObservation(value)
        | OutputScalar::RatioPerObservationSquared(value)
        | OutputScalar::LogReturn(value)
        | OutputScalar::LogReturnVariance(value) => *value,
    }
}

pub fn configured_value(spec: &Spec) -> f64 {
    configured_value_from(spec, 0)
}

pub fn configured_value_from(spec: &Spec, start: usize) -> f64 {
    configured_value_through(spec, start, start + 699)
}

pub fn configured_value_through(spec: &Spec, start: usize, end: usize) -> f64 {
    configured_value_with_indices(spec, start..=end, |index| index)
}

#[allow(dead_code)]
pub fn configured_historical_value_through(spec: &Spec, end: usize) -> f64 {
    configured_value_with_indices(spec, 0..=end, |index| index + 1)
}

fn configured_value_with_indices(
    spec: &Spec,
    indices: impl Iterator<Item = usize>,
    named_index: impl Fn(usize) -> usize,
) -> f64 {
    let mut strategy = compile_spec(spec);
    let indices = indices.collect::<Vec<_>>();
    let end = *indices
        .last()
        .expect("configured fixture range is nonempty");
    for i in indices {
        let input = StrategyInput {
            time: time(i),
            ready: i == end,
            completed_bars: vec![CompletedBarUpdate {
                source: source(),
                bar: bar(i),
            }],
            values: spec
                .inputs
                .iter()
                .enumerate()
                .map(|(index, ty)| named_value(index, *ty, named_index(i)))
                .collect(),
            trade_slots: vec![],
            feedback: vec![],
        };
        let output = strategy.evaluate(&input).unwrap();
        if let Some(note) = output.notes.first() {
            return output_value(&note.values[0].value);
        }
    }
    panic!("{} never produced a configured value", spec.key)
}

fn compile_spec(spec: &Spec) -> ConfiguredStrategy {
    ConfiguredStrategy::compile(
        document(spec),
        &MaterialLibrary::builtins(),
        "catalog_instance",
        "EURUSD",
    )
    .unwrap_or_else(|error| panic!("{} failed to compile: {error}", spec.key))
}
#[test]
fn every_named_numeric_output_compiles_and_executes_through_the_canonical_runtime() {
    let catalog = catalog();
    assert!(
        catalog.len() > 90,
        "catalog unexpectedly small: {}",
        catalog.len()
    );
    for spec in catalog {
        let expected = configured_value(&spec);
        assert!(
            expected.is_finite(),
            "{} produced a non-finite value",
            spec.key
        );
        let mut strategy = compile_spec(&spec);
        for i in 0..700 {
            let input = StrategyInput {
                time: time(i),
                ready: true,
                completed_bars: vec![CompletedBarUpdate {
                    source: source(),
                    bar: bar(i),
                }],
                values: spec
                    .inputs
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| named_value(index, *ty, i))
                    .collect(),
                trade_slots: vec![],
                feedback: vec![],
            };
            strategy.evaluate(&input).unwrap();
            if strategy.state_id() == "done" {
                break;
            }
        }
        assert_eq!(
            strategy.state_id(),
            "done",
            "{} never produced a valid output",
            spec.key
        );
        let descriptors = strategy.numeric_descriptors().collect::<Vec<_>>();
        assert_eq!(descriptors.len(), 1, "{}", spec.key);
        assert_eq!(descriptors[0].0, "feature");
    }
}
