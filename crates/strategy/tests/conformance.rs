use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use qs_core::{OrderType, RawSignal, Side};
use qs_strategy::*;
use serde_json::json;

fn source(value: &str) -> SourceId {
    SourceId::new(value).unwrap()
}

fn time(second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(10, 0, second)
        .unwrap()
}

fn bar(close: f64) -> CompletedBar {
    CompletedBar {
        open: close - 0.25,
        high: close + 1.0,
        low: close - 1.0,
        close,
        volume: Some(10.0),
    }
}

fn vacant(slot: &str) -> TradeSlotFacts {
    TradeSlotFacts {
        slot: slot.into(),
        state: TradeSlotState::Vacant,
    }
}

fn input(second: u32, ready: bool) -> StrategyInput {
    StrategyInput {
        time: time(second),
        ready,
        completed_bars: vec![],
        values: vec![],
        trade_slots: vec![vacant("primary"), vacant("secondary")],
        feedback: vec![],
    }
}

fn literal(value: Literal) -> Expr {
    Expr::Literal { value }
}
fn boolean(value: bool) -> Expr {
    literal(Literal::Bool(value))
}
fn number(value: f64) -> Expr {
    literal(Literal::Number(value))
}
fn price(value: f64) -> Expr {
    literal(Literal::Price(value))
}
fn side(value: Side) -> Expr {
    literal(Literal::Side(value))
}
fn missing(value_type: ScalarType) -> Expr {
    literal(Literal::Missing(value_type))
}

fn decision(slot: Option<&str>) -> DecisionTemplate {
    DecisionTemplate {
        kind: DecisionKind::Observation,
        reason: "configured transition".into(),
        trade_slot: slot.map(str::to_string),
        values: vec![],
    }
}

fn transition(priority: i32, target: &str, when: Expr) -> TransitionConfig {
    TransitionConfig {
        priority,
        target: target.into(),
        when,
        assignments: vec![],
        decision: None,
        actions: vec![],
        notes: vec![],
    }
}

fn state(id: &str, transitions: Vec<TransitionConfig>) -> StateConfig {
    StateConfig {
        id: id.into(),
        transitions,
    }
}

fn base(states: Vec<StateConfig>) -> StrategyConfig {
    StrategyConfig {
        strategy_id: "alpha".into(),
        title: "Neutral strategy".into(),
        parameters: vec![],
        initial_state: states[0].id.clone(),
        sources: vec![source("fast"), source("slow")],
        trade_slots: vec!["primary".into(), "secondary".into()],
        materials: vec![],
        variables: vec![],
        states,
    }
}

fn compile(config: StrategyConfig) -> Result<ConfiguredStrategy, CompileError> {
    ConfiguredStrategy::compile(config, &MaterialLibrary::builtins(), "instance_a", "EURUSD")
}

#[test]
fn configured_strategy_exposes_declared_sources_and_primary_symbol() {
    let strategy = compile(base(vec![state("idle", vec![])])).unwrap();
    assert_eq!(
        strategy
            .declared_sources()
            .iter()
            .map(SourceId::as_str)
            .collect::<Vec<_>>(),
        vec!["fast", "slow"]
    );
    assert_eq!(strategy.primary_symbol(), "EURUSD");
}

#[test]
fn strict_predicates_preserve_missing_through_not_and_boolean_lists() {
    let close = || Expr::Bar {
        source: source("fast"),
        field: BarField::Close,
    };
    let condition = Expr::Gt {
        left: Box::new(close()),
        right: Box::new(price(10.0)),
    };
    let strict = |value| Expr::Strict {
        value: Box::new(value),
    };
    let and_not = strict(Expr::Not {
        value: Box::new(Expr::All {
            items: vec![boolean(false), condition.clone()],
        }),
    });
    let or_not = strict(Expr::Not {
        value: Box::new(Expr::Any {
            items: vec![boolean(true), condition],
        }),
    });
    for expression in [and_not, or_not] {
        let encoded = serde_json::to_value(&expression).unwrap();
        assert_eq!(serde_json::from_value::<Expr>(encoded).unwrap(), expression);
        let mut strategy = compile(base(vec![
            state("idle", vec![transition(1, "done", expression)]),
            state("done", vec![]),
        ]))
        .unwrap();
        strategy.evaluate(&input(0, true)).unwrap();
        assert_eq!(
            strategy.state_id(),
            "idle",
            "an invalid predicate must not fire"
        );
    }
    let mut strategy = compile(base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                strict(Expr::Not {
                    value: Box::new(Expr::All {
                        items: vec![
                            boolean(false),
                            Expr::Gt {
                                left: Box::new(close()),
                                right: Box::new(price(10.0)),
                            },
                        ],
                    }),
                }),
            )],
        ),
        state("done", vec![]),
    ]))
    .unwrap();
    let mut observed = input(0, true);
    observed.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bar(12.0),
    });
    strategy.evaluate(&observed).unwrap();
    assert_eq!(strategy.state_id(), "done");
}

#[test]
fn strict_presence_preserves_nested_optional_boolean_evaluation() {
    for nested in [
        Expr::Gt {
            left: Box::new(Expr::Input {
                field: "sample".into(),
                value_type: ValueType::optional(ScalarType::Price),
            }),
            right: Box::new(price(10.0)),
        },
        Expr::Not {
            value: Box::new(Expr::Input {
                field: "sample".into(),
                value_type: ValueType::optional(ScalarType::Bool),
            }),
        },
    ] {
        let scalar = if matches!(nested, Expr::Not { .. }) {
            ScalarType::Bool
        } else {
            ScalarType::Price
        };
        let mut strategy = compile(base(vec![
            state(
                "idle",
                vec![transition(
                    1,
                    "done",
                    Expr::Strict {
                        value: Box::new(Expr::IsPresent {
                            value: Box::new(nested),
                        }),
                    },
                )],
            ),
            state("done", vec![]),
        ]))
        .unwrap();
        let mut snapshot = input(0, true);
        snapshot.values.push(NamedValue {
            name: "sample".into(),
            value: Value::Missing(scalar),
            updated: true,
        });
        strategy.evaluate(&snapshot).unwrap();
        assert_eq!(strategy.state_id(), "idle");
        snapshot.time = time(1);
        snapshot.values[0].value = if scalar == ScalarType::Bool {
            Value::Bool(false)
        } else {
            Value::Price(12.0)
        };
        strategy.evaluate(&snapshot).unwrap();
        assert_eq!(strategy.state_id(), "done");
    }
}

#[test]
fn explicit_presence_of_strict_predicates_is_a_required_boolean() {
    for present in [false, true] {
        for available in [false, true] {
            let value = Box::new(Expr::Strict {
                value: Box::new(Expr::Gt {
                    left: Box::new(Expr::Bar {
                        source: source("fast"),
                        field: BarField::Close,
                    }),
                    right: Box::new(price(10.0)),
                }),
            });
            let diagnostic = if present {
                Expr::IsPresent { value }
            } else {
                Expr::IsMissing { value }
            };
            let mut strategy = compile(base(vec![
                state(
                    "idle",
                    vec![transition(
                        1,
                        "done",
                        Expr::Eq {
                            left: Box::new(diagnostic),
                            right: Box::new(boolean(true)),
                        },
                    )],
                ),
                state("done", vec![]),
            ]))
            .unwrap();
            let mut snapshot = input(0, true);
            if available {
                snapshot.completed_bars.push(CompletedBarUpdate {
                    source: source("fast"),
                    bar: bar(12.0),
                });
            }
            strategy.evaluate(&snapshot).unwrap();
            assert_eq!(
                strategy.state_id(),
                if available == present { "done" } else { "idle" }
            );
        }
    }
}

#[test]
fn strict_optional_boolean_leaves_do_not_turn_missing_into_a_transition() {
    let optional = || Expr::Input {
        field: "filter".into(),
        value_type: ValueType::optional(ScalarType::Bool),
    };
    let mut strategy = compile(base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Strict {
                    value: Box::new(Expr::Not {
                        value: Box::new(Expr::All {
                            items: vec![boolean(false), optional()],
                        }),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]))
    .unwrap();
    let mut unavailable = input(0, true);
    unavailable.values.push(NamedValue {
        name: "filter".into(),
        value: Value::Missing(ScalarType::Bool),
        updated: true,
    });
    strategy.evaluate(&unavailable).unwrap();
    assert_eq!(strategy.state_id(), "idle");
    let mut available = input(1, true);
    available.values.push(NamedValue {
        name: "filter".into(),
        value: Value::Bool(false),
        updated: true,
    });
    strategy.evaluate(&available).unwrap();
    assert_eq!(strategy.state_id(), "done");
}

#[test]
fn strict_average_observes_missing_named_samples_only_on_its_bar_clock() {
    let mut config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Strict {
                    value: Box::new(Expr::Gt {
                        left: Box::new(Expr::Material {
                            id: "average".into(),
                        }),
                        right: Box::new(price(12.0)),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    config.materials.push(MaterialConfig {
        id: "average".into(),
        key: MATERIAL_STRICT_SMA.into(),
        inputs: vec![Expr::Input {
            field: "feature".into(),
            value_type: ValueType::optional(ScalarType::Price),
        }],
        params: MaterialArgs::new([
            ("source", MaterialArg::Source(source("fast"))),
            ("period", MaterialArg::Integer(3)),
        ]),
    });
    let mut strategy = compile(config).unwrap();
    for (second, bar_update, value, updated) in [
        (0, true, Value::Price(10.0), true),
        (1, false, Value::Missing(ScalarType::Price), true),
        (2, true, Value::Price(100.0), false),
        (3, true, Value::Price(12.0), true),
        (4, true, Value::Price(13.0), true),
        (5, true, Value::Price(14.0), true),
    ] {
        let mut snapshot = input(second, true);
        if bar_update {
            snapshot.completed_bars.push(CompletedBarUpdate {
                source: source("fast"),
                bar: bar(10.0),
            });
        }
        snapshot.values.push(NamedValue {
            name: "feature".into(),
            value,
            updated,
        });
        strategy.evaluate(&snapshot).unwrap();
        assert_eq!(
            strategy.state_id(),
            if second == 5 { "done" } else { "idle" }
        );
    }
}

#[test]
fn strict_predicate_cannot_be_negated_through_a_legacy_comparison() {
    let predicate = Expr::Strict {
        value: Box::new(Expr::Gt {
            left: Box::new(Expr::Bar {
                source: source("fast"),
                field: BarField::Close,
            }),
            right: Box::new(price(10.0)),
        }),
    };
    let unsafe_expression = Expr::Not {
        value: Box::new(Expr::Eq {
            left: Box::new(predicate),
            right: Box::new(boolean(false)),
        }),
    };
    let document = base(vec![
        state("idle", vec![transition(1, "done", unsafe_expression)]),
        state("done", vec![]),
    ]);
    assert!(
        matches!(compile(document), Err(CompileError::InvalidConfig { reason, .. }) if reason.contains("strict predicate"))
    );
}

#[test]
fn strict_sma_is_sampled_once_per_declared_bar_and_gates_a_transition() {
    let mut config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Strict {
                    value: Box::new(Expr::Gt {
                        left: Box::new(Expr::Material {
                            id: "average".into(),
                        }),
                        right: Box::new(price(10.5)),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    config.materials.push(MaterialConfig {
        id: "average".into(),
        key: MATERIAL_STRICT_SMA.into(),
        inputs: vec![Expr::Bar {
            source: source("fast"),
            field: BarField::Close,
        }],
        params: MaterialArgs::new([
            ("source", MaterialArg::Source(source("fast"))),
            ("period", MaterialArg::Integer(3)),
        ]),
    });
    let mut strategy = compile(config.clone()).unwrap();
    for (second, close) in [(0, 10.0), (1, 11.0), (2, 12.0)] {
        let mut snapshot = input(second, true);
        snapshot.completed_bars.push(CompletedBarUpdate {
            source: source("fast"),
            bar: bar(close),
        });
        strategy.evaluate(&snapshot).unwrap();
        assert_eq!(
            strategy.state_id(),
            if second == 2 { "done" } else { "idle" }
        );
    }
    let mut invalid = config;
    invalid.materials[0].inputs = vec![Expr::Add {
        left: Box::new(Expr::Bar {
            source: source("fast"),
            field: BarField::Close,
        }),
        right: Box::new(Expr::Bar {
            source: source("slow"),
            field: BarField::Close,
        }),
    }];
    assert!(
        compile(invalid).is_err(),
        "a mixed-source input has no single sample clock"
    );
}

fn strict_average_observer(key: &str, period: i64) -> StrategyConfig {
    let average = || Expr::Material {
        id: "average".into(),
    };
    let expected = || Expr::Input {
        field: "expected".into(),
        value_type: ValueType::optional(ScalarType::Price),
    };
    let mut observe = transition(
        1,
        "idle",
        Expr::Strict {
            value: Box::new(Expr::Le {
                left: Box::new(Expr::Abs {
                    value: Box::new(Expr::Sub {
                        left: Box::new(average()),
                        right: Box::new(expected()),
                    }),
                }),
                right: Box::new(price(1e-12)),
            }),
        },
    );
    let mut unavailable = transition(
        2,
        "idle",
        Expr::All {
            items: vec![
                Expr::IsMissing {
                    value: Box::new(average()),
                },
                Expr::IsMissing {
                    value: Box::new(expected()),
                },
            ],
        },
    );
    observe.decision = Some(DecisionTemplate {
        kind: DecisionKind::Observation,
        reason: "strict average observation".into(),
        trade_slot: None,
        values: vec![],
    });
    unavailable.decision = observe.decision.clone();
    let mut return_missing = unavailable.clone();
    unavailable.target = "observed".into();
    return_missing.target = "idle".into();
    let mut return_observation = observe.clone();
    observe.target = "observed".into();
    return_observation.target = "idle".into();
    let mut config = base(vec![
        state("idle", vec![observe, unavailable]),
        state("observed", vec![return_observation, return_missing]),
    ]);
    config.materials.push(MaterialConfig {
        id: "average".into(),
        key: key.into(),
        inputs: vec![Expr::Input {
            field: "sample".into(),
            value_type: ValueType::optional(ScalarType::Price),
        }],
        params: MaterialArgs::new([
            ("source", MaterialArg::Source(source("fast"))),
            ("period", MaterialArg::Integer(period)),
        ]),
    });
    config
}

fn strict_average_observation(
    strategy: &mut ConfiguredStrategy,
    index: usize,
    sample: Option<f64>,
    clock: bool,
    updated: bool,
    expected: Option<f64>,
) {
    let mut snapshot = input(0, true);
    snapshot.time += chrono::Duration::seconds(index as i64);
    if clock {
        snapshot.completed_bars.push(CompletedBarUpdate {
            source: source("fast"),
            bar: bar(10.0),
        });
    }
    snapshot.values.push(NamedValue {
        name: "sample".into(),
        value: sample
            .map(Value::Price)
            .unwrap_or(Value::Missing(ScalarType::Price)),
        updated,
    });
    snapshot.values.push(NamedValue {
        name: "expected".into(),
        value: expected
            .map(Value::Price)
            .unwrap_or(Value::Missing(ScalarType::Price)),
        updated: true,
    });
    assert!(
        strategy.evaluate(&snapshot).unwrap().decision.is_some(),
        "numeric oracle mismatch at sample {index}, expected {expected:?}"
    );
}

#[test]
fn strict_averages_match_independent_nonmonotonic_oracles_and_full_prefixes() {
    let samples = (0..96)
        .map(|i| {
            if i == 19 || i == 45 {
                None
            } else {
                Some(((i * 17 + 3) % 23) as f64)
            }
        })
        .collect::<Vec<_>>();
    for key in [MATERIAL_STRICT_SMA, MATERIAL_STRICT_EMA] {
        let mut streaming = compile(strict_average_observer(key, 3)).unwrap();
        let mut history = Vec::new();
        let mut ema = None;
        let mut expectations = Vec::new();
        for (index, sample) in samples.iter().copied().enumerate() {
            history.push(sample);
            let expected = if key == MATERIAL_STRICT_SMA {
                if history.len() < 3 {
                    None
                } else {
                    history[history.len() - 3..]
                        .iter()
                        .copied()
                        .sum::<Option<f64>>()
                        .map(|sum| sum / 3.0)
                }
            } else {
                ema = match (sample, ema) {
                    (None, _) => None,
                    (Some(value), Some(previous)) => Some((value + previous) / 2.0),
                    (Some(_), None) if history.len() >= 3 => history[history.len() - 3..]
                        .iter()
                        .copied()
                        .sum::<Option<f64>>()
                        .map(|sum| sum / 3.0),
                    _ => None,
                };
                ema
            };
            expectations.push(expected);
            strict_average_observation(&mut streaming, index, sample, true, true, expected);
            let mut prefix = compile(strict_average_observer(key, 3)).unwrap();
            for (step, value) in samples[..=index].iter().copied().enumerate() {
                strict_average_observation(
                    &mut prefix,
                    step,
                    value,
                    true,
                    true,
                    expectations[step],
                );
            }
        }
    }
}

#[test]
fn strict_ema_retains_idle_output_and_reseeds_after_stale_or_missing_observations() {
    let mut strategy = compile(strict_average_observer(MATERIAL_STRICT_EMA, 3)).unwrap();
    for (index, (sample, clock, updated, expected)) in [
        (Some(1.0), true, true, None),
        (Some(4.0), true, true, None),
        (Some(1.0), true, true, Some(2.0)),
        (None, false, true, Some(2.0)),
        (Some(8.0), true, true, Some(5.0)),
        (Some(100.0), true, false, None),
        (Some(2.0), true, true, None),
        (Some(8.0), true, true, None),
        (Some(2.0), true, true, Some(4.0)),
        (None, true, true, None),
    ]
    .into_iter()
    .enumerate()
    {
        strict_average_observation(&mut strategy, index, sample, clock, updated, expected);
    }
}

#[test]
fn numeric_descriptors_are_effective_compiler_contracts_not_catalog_promises() {
    let library = MaterialLibrary::builtins();
    for key in [MATERIAL_STRICT_SMA, MATERIAL_STRICT_EMA] {
        let document = strict_average_observer(key, 3);
        let descriptor = library
            .numeric_descriptor(
                key,
                &document.materials[0].params,
                &[ValueType::optional(ScalarType::Price)],
            )
            .unwrap()
            .unwrap();
        assert_eq!(descriptor.source_clock, source("fast"));
        assert_eq!(descriptor.unit, NumericUnit::Price);
        assert_eq!(
            descriptor.output_type,
            ValueType::optional(ScalarType::Price)
        );
        assert_eq!(descriptor.first_output_observations, 3);
        assert!(descriptor.max_state_bytes <= MAX_MATERIAL_STATE_BYTES);
        assert!(descriptor.flat_input_is_defined());
        assert!(descriptor.exact_aliases.is_empty());
        if key == MATERIAL_STRICT_EMA {
            assert_eq!(
                descriptor.calculation,
                NumericCalculation::SmaSeededEma {
                    period: 3,
                    alpha: 0.5
                }
            );
            assert_eq!(descriptor.missing, NumericMissingPolicy::ResetAndReseed);
        } else {
            assert_eq!(
                descriptor.calculation,
                NumericCalculation::ObservedSma { period: 3 }
            );
            assert_eq!(descriptor.missing, NumericMissingPolicy::ConsumeWindowSlot);
        }
        let compiled = compile(document.clone()).unwrap();
        assert_eq!(
            compiled.numeric_descriptors().collect::<Vec<_>>(),
            vec![("average", &descriptor)]
        );
        assert_eq!(
            compiled.input_requirements().completed_bars[0].required_lookback,
            descriptor.first_output_observations
        );
        let mut unknown = document.materials[0].params.clone();
        unknown.0.insert("ignored".into(), MaterialArg::Integer(3));
        assert!(
            library
                .numeric_descriptor(key, &unknown, &[ValueType::optional(ScalarType::Price)])
                .is_err()
        );
    }
    assert!(
        library
            .numeric_descriptor(MATERIAL_EMA, &MaterialArgs::default(), &[])
            .unwrap()
            .is_none()
    );
    assert!(
        library
            .numeric_descriptor("absent", &MaterialArgs::default(), &[])
            .is_err()
    );
}

struct ContradictoryNumericFactory(&'static str);

impl MaterialFactory for ContradictoryNumericFactory {
    fn params(&self) -> &[ParamSpec] {
        &[
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
        ]
    }

    fn numeric_descriptor(
        &self,
        params: &MaterialArgs,
        inputs: &[ValueType],
    ) -> Result<Option<NumericDescriptor>, String> {
        let mut descriptor = MaterialLibrary::builtins()
            .numeric_descriptor(MATERIAL_STRICT_SMA, params, inputs)?
            .unwrap();
        if self.0 == "unit" {
            descriptor.unit = NumericUnit::Number;
        }
        if self.0 == "input" {
            descriptor.inputs = NumericInputs::Scalar(vec![ValueType::required(ScalarType::Price)]);
        }
        Ok(Some(descriptor))
    }

    fn update_trigger(
        &self,
        _: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(if self.0 == "trigger" {
            MaterialUpdateTrigger::EveryInput
        } else {
            MaterialUpdateTrigger::Source(source("fast"))
        })
    }

    fn build(&self, _: &MaterialArgs, _: &[ValueType]) -> Result<MaterialBuild, String> {
        Ok(MaterialBuild {
            output_type: ValueType::optional(if self.0 == "output" {
                ScalarType::Number
            } else {
                ScalarType::Price
            }),
            lookback: MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: source("fast"),
                required_lookback: if self.0 == "lookback" { 2 } else { 3 },
            }]),
            max_state_bytes: if self.0 == "state" {
                MAX_MATERIAL_STATE_BYTES
            } else {
                16
            },
            evaluator: Box::new(CounterEvaluator { count: 0 }),
        })
    }
}

#[test]
fn numeric_descriptors_reject_mechanically_contradictory_custom_factory_contracts() {
    for mismatch in ["trigger", "lookback", "output", "state", "unit", "input"] {
        let library = MaterialLibrary::builtins()
            .with_factory(
                "contradictory",
                Arc::new(ContradictoryNumericFactory(mismatch)),
            )
            .unwrap();
        let result = ConfiguredStrategy::compile(
            strict_average_observer("contradictory", 3),
            &library,
            "test",
            "EURUSD",
        );
        let error = match result {
            Ok(_) => panic!("accepted contradictory {mismatch}"),
            Err(error) => error,
        };
        assert!(
            matches!(error, CompileError::InvalidConfig { ref path, .. } | CompileError::MaterialFactory { ref path, .. } if path.ends_with("descriptor")),
            "{mismatch}: {error}"
        );
    }
}

#[test]
fn strict_average_compilation_checks_period_clock_types_and_registered_keys() {
    for key in [MATERIAL_STRICT_SMA, MATERIAL_STRICT_EMA] {
        for period in [0, 1025, i64::MAX] {
            assert!(compile(strict_average_observer(key, period)).is_err());
        }
        for period in [1, 1024] {
            assert!(compile(strict_average_observer(key, period)).is_ok());
        }
        let mut wrong_source = strict_average_observer(key, 3);
        wrong_source.materials[0]
            .params
            .0
            .insert("source".into(), MaterialArg::Source(source("unknown")));
        assert!(compile(wrong_source).is_err());
        let mut wrong_type = strict_average_observer(key, 3);
        wrong_type.materials[0].inputs[0] = Expr::Input {
            field: "sample".into(),
            value_type: ValueType::optional(ScalarType::Bool),
        };
        assert!(compile(wrong_type).is_err());
        let mut wrong_threshold = strict_average_observer(key, 3);
        wrong_threshold.states[0].transitions[0].when = Expr::Strict {
            value: Box::new(Expr::Gt {
                left: Box::new(Expr::Material {
                    id: "average".into(),
                }),
                right: Box::new(number(10.0)),
            }),
        };
        assert!(compile(wrong_threshold).is_err());
        assert!(
            MaterialLibrary::builtins()
                .with_factory(key, Arc::new(CounterFactory))
                .is_err()
        );
        let document = strict_average_observer(key, 3);
        let encoded = serde_json::to_value(&document).unwrap();
        let decoded: StrategyConfig = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, document);
        assert!(compile(decoded).is_ok());
    }
}

#[derive(Clone, Copy)]
enum PrimitiveExpected {
    Ratio(f64),
    LogReturn(f64),
    Bool(bool),
}

fn semantic_literal(expected: PrimitiveExpected, tolerance: bool) -> Expr {
    literal(match expected {
        PrimitiveExpected::Ratio(value) => Literal::Ratio(if tolerance { 1e-12 } else { value }),
        PrimitiveExpected::LogReturn(value) => {
            Literal::LogReturn(if tolerance { 1e-12 } else { value })
        }
        PrimitiveExpected::Bool(value) => Literal::Bool(value),
    })
}

fn configured_bar_primitive(
    key: &str,
    length: Option<(&str, i64)>,
    expected: PrimitiveExpected,
) -> ConfiguredStrategy {
    let value = Expr::Material {
        id: "feature".into(),
    };
    let condition = match expected {
        PrimitiveExpected::Bool(_) => Expr::Eq {
            left: Box::new(value),
            right: Box::new(semantic_literal(expected, false)),
        },
        _ => Expr::Le {
            left: Box::new(Expr::Abs {
                value: Box::new(Expr::Sub {
                    left: Box::new(value),
                    right: Box::new(semantic_literal(expected, false)),
                }),
            }),
            right: Box::new(semantic_literal(expected, true)),
        },
    };
    let mut config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Strict {
                    value: Box::new(condition),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    let mut params = vec![("source", MaterialArg::Source(source("fast")))];
    if let Some((name, value)) = length {
        params.push((name, MaterialArg::Integer(value)));
    }
    config.materials.push(MaterialConfig {
        id: "feature".into(),
        key: key.into(),
        inputs: vec![],
        params: MaterialArgs::new(params),
    });
    compile(config).unwrap()
}

#[test]
fn bar_shape_change_and_structure_catalog_runs_through_configured_semantic_types() {
    let b = |open, high, low, close| CompletedBar {
        open,
        high,
        low,
        close,
        volume: Some(1.0),
    };
    let cases = vec![
        (
            MATERIAL_BODY_FRACTION,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.5),
        ),
        (
            MATERIAL_BODY_DIRECTION_FRACTION,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.5),
        ),
        (
            MATERIAL_UPPER_WICK_FRACTION,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.25),
        ),
        (
            MATERIAL_LOWER_WICK_FRACTION,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.25),
        ),
        (
            MATERIAL_CLOSE_POSITION,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.75),
        ),
        (
            MATERIAL_CLOSE_LOCATION_VALUE,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0)],
            PrimitiveExpected::Ratio(0.5),
        ),
        (
            MATERIAL_RETURN_LOG,
            Some(("horizon", 2)),
            vec![
                b(1.0, 1.0, 1.0, 1.0),
                b(3.0, 3.0, 3.0, 3.0),
                b(4.0, 4.0, 4.0, 4.0),
            ],
            PrimitiveExpected::LogReturn(4.0f64.ln()),
        ),
        (
            MATERIAL_ROC,
            Some(("horizon", 2)),
            vec![
                b(1.0, 1.0, 1.0, 1.0),
                b(3.0, 3.0, 3.0, 3.0),
                b(4.0, 4.0, 4.0, 4.0),
            ],
            PrimitiveExpected::Ratio(3.0),
        ),
        (
            MATERIAL_INSIDE_BAR,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0), b(2.0, 4.0, 2.0, 3.0)],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_OUTSIDE_BAR,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0), b(2.0, 6.0, 0.5, 3.0)],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_BODY_ENGULFING,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0), b(5.0, 6.0, 0.5, 1.0)],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_ENGULF_SIZE_RATIO,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0), b(5.0, 6.0, 0.5, 1.0)],
            PrimitiveExpected::Ratio(2.0),
        ),
        (
            MATERIAL_NARROW_RANGE,
            Some(("period", 3)),
            vec![
                b(2.0, 6.0, 1.0, 3.0),
                b(2.0, 5.0, 1.0, 3.0),
                b(2.0, 4.0, 1.0, 3.0),
            ],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_WIDE_RANGE,
            Some(("period", 3)),
            vec![
                b(2.0, 4.0, 1.0, 3.0),
                b(2.0, 5.0, 1.0, 3.0),
                b(2.0, 6.0, 1.0, 3.0),
            ],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_RELATIVE_RANGE,
            Some(("period", 2)),
            vec![
                b(2.0, 4.0, 2.0, 3.0),
                b(2.0, 6.0, 2.0, 3.0),
                b(2.0, 5.0, 2.0, 3.0),
            ],
            PrimitiveExpected::Ratio(1.0),
        ),
        (
            MATERIAL_BAR_OVERLAP,
            None,
            vec![b(2.0, 5.0, 1.0, 4.0), b(5.0, 8.0, 4.0, 7.0)],
            PrimitiveExpected::Ratio(1.0 / 7.0),
        ),
        (
            MATERIAL_THREE_BAR_GAP_UP,
            None,
            vec![
                b(1.0, 2.0, 1.0, 1.5),
                b(3.0, 4.0, 3.0, 3.5),
                b(5.0, 6.0, 4.0, 5.0),
            ],
            PrimitiveExpected::Bool(true),
        ),
        (
            MATERIAL_THREE_BAR_GAP_DOWN,
            None,
            vec![
                b(5.0, 6.0, 5.0, 5.5),
                b(3.0, 4.0, 3.0, 3.5),
                b(1.0, 2.0, 1.0, 1.5),
            ],
            PrimitiveExpected::Bool(true),
        ),
    ];
    for (key, length, bars, expected) in cases {
        let mut strategy = configured_bar_primitive(key, length, expected);
        let last = bars.len() - 1;
        for (index, bar) in bars.into_iter().enumerate() {
            let mut snapshot = input(index as u32, true);
            snapshot.completed_bars.push(CompletedBarUpdate {
                source: source("fast"),
                bar,
            });
            strategy.evaluate(&snapshot).unwrap();
            assert_eq!(
                strategy.state_id(),
                if index == last { "done" } else { "idle" },
                "{key} at {index}"
            );
        }
    }
}

#[test]
fn semantic_units_reject_number_ratio_percent_and_log_return_threshold_mixups() {
    let mut config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Strict {
                    value: Box::new(Expr::Gt {
                        left: Box::new(Expr::Material {
                            id: "feature".into(),
                        }),
                        right: Box::new(number(0.5)),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    config.materials.push(MaterialConfig {
        id: "feature".into(),
        key: MATERIAL_BODY_FRACTION.into(),
        inputs: vec![],
        params: MaterialArgs::new([("source", MaterialArg::Source(source("fast")))]),
    });
    assert!(matches!(
        compile(config.clone()),
        Err(CompileError::TypeMismatch { .. })
    ));
    if let Expr::Strict { value } = &mut config.states[0].transitions[0].when
        && let Expr::Gt { right, .. } = value.as_mut()
    {
        **right = literal(Literal::Ratio(0.5));
    }
    assert!(compile(config).is_ok());
    let round_trip = [
        Literal::Ratio(0.5),
        Literal::Percent(25.0),
        Literal::LogReturn(-0.2),
    ];
    for literal in round_trip {
        let json = serde_json::to_value(&literal).unwrap();
        assert_eq!(serde_json::from_value::<Literal>(json).unwrap(), literal);
    }
}

fn entry_action(slot: &str) -> ActionTemplate {
    ActionTemplate::Entry {
        slot: slot.into(),
        side: side(Side::Buy),
        order_type: OrderType::Market,
        price: missing(ScalarType::Price),
        risk: number(1.0),
        stoploss: missing(ScalarType::Price),
        targets: vec![],
        entry_class: None,
    }
}

fn entry_strategy() -> ConfiguredStrategy {
    let mut enter = transition(1, "done", boolean(true));
    enter.decision = Some(decision(Some("primary")));
    enter.actions = vec![entry_action("primary")];
    compile(base(vec![
        state("idle", vec![enter]),
        state("done", vec![]),
    ]))
    .unwrap()
}

#[test]
fn strict_schema_rejects_versions_parameter_bags_and_unknown_fields() {
    let valid = json!({
        "strategy_id":"alpha", "title":"A", "initial_state":"idle",
        "sources":["fast"], "trade_slots":["primary"], "materials":[], "variables":[],
        "states":[{"id":"idle","transitions":[]}]
    });
    assert!(serde_json::from_value::<StrategyConfig>(valid.clone()).is_ok());
    let mut versioned = valid.clone();
    versioned["schema_version"] = json!(1);
    assert!(serde_json::from_value::<StrategyConfig>(versioned).is_err());
    let mut undeclared = base(vec![state("idle", vec![])]);
    undeclared.materials.push(MaterialConfig {
        id: "time".into(),
        key: MATERIAL_INPUT_TIME.into(),
        inputs: vec![],
        params: MaterialArgs::new([("unknown", MaterialArg::Integer(1))]),
    });
    assert!(compile(undeclared).is_err());
    assert!(SourceId::new(" bad ").is_err());
}

#[test]
fn graph_type_cycle_state_and_priority_validation_remain_strict() {
    let mut duplicate = base(vec![state("idle", vec![])]);
    duplicate.materials = vec![
        MaterialConfig {
            id: "x".into(),
            key: MATERIAL_INPUT_TIME.into(),
            inputs: vec![],
            params: MaterialParams::None.into(),
        },
        MaterialConfig {
            id: "x".into(),
            key: MATERIAL_READINESS.into(),
            inputs: vec![],
            params: MaterialParams::None.into(),
        },
    ];
    assert!(matches!(
        compile(duplicate),
        Err(CompileError::DuplicateIdentifier { .. })
    ));

    let mut cycle = base(vec![state("idle", vec![])]);
    cycle.materials = vec![
        MaterialConfig {
            id: "a".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "b".into() }],
            params: MaterialParams::Ema { period: 2 }.into(),
        },
        MaterialConfig {
            id: "b".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "a".into() }],
            params: MaterialParams::Ema { period: 2 }.into(),
        },
    ];
    assert!(matches!(
        compile(cycle),
        Err(CompileError::DependencyCycle { .. })
    ));

    assert!(matches!(
        compile(base(vec![state("idle", vec![]), state("lost", vec![])])),
        Err(CompileError::UnreachableState { .. })
    ));
    let priority = base(vec![
        state(
            "idle",
            vec![
                transition(1, "a", boolean(true)),
                transition(1, "b", boolean(true)),
            ],
        ),
        state("a", vec![]),
        state("b", vec![]),
    ]);
    assert!(matches!(
        compile(priority),
        Err(CompileError::PriorityConflict { .. })
    ));
}

#[test]
fn requirements_are_ordered_source_specific_and_propagate_lookback() {
    let mut config = base(vec![state("idle", vec![])]);
    config.materials = vec![
        MaterialConfig {
            id: "fast_close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("fast"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "fast_ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material {
                id: "fast_close".into(),
            }],
            params: MaterialParams::Ema { period: 5 }.into(),
        },
        MaterialConfig {
            id: "slow_atr".into(),
            key: MATERIAL_ATR.into(),
            inputs: vec![],
            params: MaterialParams::Atr {
                source: source("slow"),
                period: 3,
            }
            .into(),
        },
        MaterialConfig {
            id: "cross".into(),
            key: MATERIAL_CROSS_ABOVE.into(),
            inputs: vec![
                Expr::Material {
                    id: "fast_close".into(),
                },
                Expr::Material {
                    id: "fast_ema".into(),
                },
            ],
            params: MaterialParams::None.into(),
        },
    ];
    config.states[0].transitions = vec![];
    let strategy = compile(config).unwrap();
    assert_eq!(
        strategy.input_requirements().completed_bars,
        vec![
            CompletedBarRequirement {
                source: source("fast"),
                required_lookback: 5,
            },
            CompletedBarRequirement {
                source: source("slow"),
                required_lookback: 4,
            },
        ]
    );
}

#[test]
fn two_sources_update_once_per_boundary_and_unrelated_source_is_isolated() {
    let mut cross = transition(1, "crossed", Expr::Material { id: "cross".into() });
    cross.decision = Some(decision(None));
    let mut config = base(vec![state("idle", vec![cross]), state("crossed", vec![])]);
    config.materials = vec![
        MaterialConfig {
            id: "close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("fast"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "close".into() }],
            params: MaterialParams::Ema { period: 3 }.into(),
        },
        MaterialConfig {
            id: "cross".into(),
            key: MATERIAL_CROSS_ABOVE.into(),
            inputs: vec![
                Expr::Material { id: "close".into() },
                Expr::Material { id: "ema".into() },
            ],
            params: MaterialParams::None.into(),
        },
        MaterialConfig {
            id: "slow".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("slow"),
                field: BarField::Close,
            }
            .into(),
        },
    ];
    let mut strategy = compile(config).unwrap();
    let mut first = input(0, false);
    first.completed_bars = vec![
        CompletedBarUpdate {
            source: source("slow"),
            bar: bar(20.0),
        },
        CompletedBarUpdate {
            source: source("fast"),
            bar: bar(10.0),
        },
    ];
    strategy.evaluate(&first).unwrap();

    let mut unrelated = input(1, true);
    unrelated.completed_bars.push(CompletedBarUpdate {
        source: source("slow"),
        bar: bar(21.0),
    });
    strategy.evaluate(&unrelated).unwrap();
    assert_eq!(strategy.state_id(), "idle");

    let mut fast = input(2, true);
    fast.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bar(12.0),
    });
    strategy.evaluate(&fast).unwrap();
    assert_eq!(strategy.state_id(), "crossed");
}

#[test]
fn completed_bar_updates_reject_duplicate_unknown_unrequired_and_invalid() {
    let mut config = base(vec![state("idle", vec![])]);
    config.materials = vec![MaterialConfig {
        id: "close".into(),
        key: MATERIAL_BAR_FIELD.into(),
        inputs: vec![],
        params: MaterialParams::BarField {
            source: source("fast"),
            field: BarField::Close,
        }
        .into(),
    }];
    let mut duplicate = compile(config.clone()).unwrap();
    let mut snapshot = input(0, true);
    snapshot.completed_bars = vec![
        CompletedBarUpdate {
            source: source("fast"),
            bar: bar(10.0),
        },
        CompletedBarUpdate {
            source: source("fast"),
            bar: bar(11.0),
        },
    ];
    assert!(duplicate.evaluate(&snapshot).is_err());

    let mut unrequired = compile(config.clone()).unwrap();
    let mut snapshot = input(0, true);
    snapshot.completed_bars.push(CompletedBarUpdate {
        source: source("slow"),
        bar: bar(10.0),
    });
    assert!(unrequired.evaluate(&snapshot).is_err());

    let mut invalid = compile(config).unwrap();
    let mut bad = bar(10.0);
    bad.high = f64::NAN;
    let mut snapshot = input(0, true);
    snapshot.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bad,
    });
    assert!(invalid.evaluate(&snapshot).is_err());
}

#[derive(Clone)]
struct PassEvaluator;
impl MaterialEvaluator for PassEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, inputs: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        Ok(inputs[0].clone())
    }
}

struct PassFactory {
    trigger: MaterialUpdateTrigger,
    lookback: MaterialLookback,
}
impl MaterialFactory for PassFactory {
    fn build(&self, _: &MaterialArgs, input_types: &[ValueType]) -> Result<MaterialBuild, String> {
        if input_types.len() != 1 {
            return Err("one input required".into());
        }
        Ok(MaterialBuild {
            output_type: input_types[0],
            lookback: self.lookback.clone(),
            max_state_bytes: 0,
            evaluator: Box::new(PassEvaluator),
        })
    }
    fn update_trigger(
        &self,
        _: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(self.trigger.clone())
    }
}

#[derive(Clone, Default)]
struct CountEvaluator {
    evaluations: i64,
}

impl MaterialEvaluator for CountEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        if context.input_updates.len() != 2
            || context.any_input_updates.len() != 2
            || !context.input_updates[1]
            || context.any_input_updates[1]
        {
            return Err("constant update provenance changed".into());
        }
        self.evaluations += 1;
        Ok(Value::Integer(self.evaluations))
    }
}

struct CountFactory;

impl MaterialFactory for CountFactory {
    fn build(&self, _: &MaterialArgs, input_types: &[ValueType]) -> Result<MaterialBuild, String> {
        if input_types.len() != 2 {
            return Err("two inputs required".into());
        }
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Integer),
            lookback: MaterialLookback::InheritInputs { minimum: 1 },
            max_state_bytes: std::mem::size_of::<i64>(),
            evaluator: Box::<CountEvaluator>::default(),
        })
    }

    fn update_trigger(
        &self,
        _: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::AnyInput)
    }
}

#[test]
fn any_input_uses_any_causal_leaf_without_constant_spurious_updates() {
    let library = MaterialLibrary::builtins()
        .with_factory(
            "pass_any",
            Arc::new(PassFactory {
                trigger: MaterialUpdateTrigger::AnyInput,
                lookback: MaterialLookback::InheritInputs { minimum: 1 },
            }),
        )
        .unwrap()
        .with_factory("count_any", Arc::new(CountFactory))
        .unwrap();

    let mut compound = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Eq {
                    left: Box::new(Expr::Material { id: "sum".into() }),
                    right: Box::new(price(13.0)),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    compound.materials = vec![
        MaterialConfig {
            id: "fast_close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("fast"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "slow_close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("slow"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "sum".into(),
            key: "pass_any".into(),
            inputs: vec![Expr::Add {
                left: Box::new(Expr::Material {
                    id: "fast_close".into(),
                }),
                right: Box::new(Expr::Material {
                    id: "slow_close".into(),
                }),
            }],
            params: MaterialParams::None.into(),
        },
    ];
    let mut strategy =
        ConfiguredStrategy::compile(compound, &library, "compound", "EURUSD").unwrap();
    let mut initial = input(0, false);
    initial.completed_bars = vec![
        CompletedBarUpdate {
            source: source("fast"),
            bar: bar(2.0),
        },
        CompletedBarUpdate {
            source: source("slow"),
            bar: bar(10.0),
        },
    ];
    strategy.evaluate(&initial).unwrap();
    let mut fast_only = input(1, true);
    fast_only.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bar(3.0),
    });
    strategy.evaluate(&fast_only).unwrap();
    assert_eq!(strategy.state_id(), "done");

    let mut constant = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Eq {
                    left: Box::new(Expr::Material { id: "count".into() }),
                    right: Box::new(literal(Literal::Integer(2))),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    constant.materials = vec![
        MaterialConfig {
            id: "fast_close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("fast"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "count".into(),
            key: "count_any".into(),
            inputs: vec![
                Expr::Material {
                    id: "fast_close".into(),
                },
                number(1.0),
            ],
            params: MaterialParams::None.into(),
        },
    ];
    let mut strategy =
        ConfiguredStrategy::compile(constant, &library, "constant", "EURUSD").unwrap();
    let mut initial = input(0, false);
    initial.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bar(2.0),
    });
    strategy.evaluate(&initial).unwrap();
    strategy.evaluate(&input(1, true)).unwrap();
    assert_eq!(strategy.state_id(), "idle");
    let mut updated = input(2, true);
    updated.completed_bars.push(CompletedBarUpdate {
        source: source("fast"),
        bar: bar(2.0),
    });
    strategy.evaluate(&updated).unwrap();
    assert_eq!(strategy.state_id(), "done");
}

#[test]
fn named_input_schema_conflicts_and_updated_provenance_are_enforced() {
    let conflict = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::Eq {
                    left: Box::new(Expr::Input {
                        field: "level".into(),
                        value_type: ValueType::required(ScalarType::Number),
                    }),
                    right: Box::new(Expr::Input {
                        field: "level".into(),
                        value_type: ValueType::optional(ScalarType::Price),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    assert!(matches!(
        compile(conflict),
        Err(CompileError::TypeMismatch { .. })
    ));

    let library = MaterialLibrary::builtins()
        .with_factory(
            "pass",
            Arc::new(PassFactory {
                trigger: MaterialUpdateTrigger::AllInputs,
                lookback: MaterialLookback::None,
            }),
        )
        .unwrap();
    let mut move_state = transition(
        1,
        "done",
        Expr::Gt {
            left: Box::new(Expr::Material { id: "pass".into() }),
            right: Box::new(number(0.0)),
        },
    );
    move_state.decision = Some(decision(None));
    let mut config = base(vec![state("idle", vec![move_state]), state("done", vec![])]);
    config.materials = vec![MaterialConfig {
        id: "pass".into(),
        key: "pass".into(),
        inputs: vec![Expr::Input {
            field: "level".into(),
            value_type: ValueType::required(ScalarType::Number),
        }],
        params: MaterialParams::None.into(),
    }];
    let mut strategy = ConfiguredStrategy::compile(config, &library, "i", "EURUSD").unwrap();
    assert_eq!(
        strategy.input_requirements().named_inputs,
        vec![NamedInputRequirement {
            name: "level".into(),
            value_type: ValueType::required(ScalarType::Number),
        }]
    );
    let mut unchanged = input(0, true);
    unchanged.values.push(NamedValue {
        name: "level".into(),
        value: Value::Number(2.0),
        updated: false,
    });
    strategy.evaluate(&unchanged).unwrap();
    assert_eq!(strategy.state_id(), "idle");
    let mut updated = input(1, true);
    updated.values.push(NamedValue {
        name: "level".into(),
        value: Value::Number(2.0),
        updated: true,
    });
    strategy.evaluate(&updated).unwrap();
    assert_eq!(strategy.state_id(), "done");
}

#[test]
fn named_input_runtime_rejects_missing_unknown_duplicate_and_wrong_type() {
    let config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "done",
                Expr::IsPresent {
                    value: Box::new(Expr::Input {
                        field: "level".into(),
                        value_type: ValueType::required(ScalarType::Number),
                    }),
                },
            )],
        ),
        state("done", vec![]),
    ]);
    assert!(
        compile(config.clone())
            .unwrap()
            .evaluate(&input(0, true))
            .is_err()
    );
    let mut unknown = input(0, true);
    unknown.values.push(NamedValue {
        name: "other".into(),
        value: Value::Number(1.0),
        updated: true,
    });
    assert!(compile(config.clone()).unwrap().evaluate(&unknown).is_err());
    let mut wrong = input(0, true);
    wrong.values.push(NamedValue {
        name: "level".into(),
        value: Value::Price(1.0),
        updated: true,
    });
    assert!(compile(config).unwrap().evaluate(&wrong).is_err());
}

#[test]
fn impossible_dependency_trigger_and_unprovenanced_lookback_reject() {
    struct NoInputFactory;
    impl MaterialFactory for NoInputFactory {
        fn build(&self, _: &MaterialArgs, _: &[ValueType]) -> Result<MaterialBuild, String> {
            Ok(MaterialBuild {
                output_type: ValueType::required(ScalarType::Bool),
                lookback: MaterialLookback::InheritInputs { minimum: 2 },
                max_state_bytes: 0,
                evaluator: Box::new(PassEvaluator),
            })
        }
        fn update_trigger(
            &self,
            _: &MaterialArgs,
            _: &[ValueType],
        ) -> Result<MaterialUpdateTrigger, String> {
            Ok(MaterialUpdateTrigger::AllInputs)
        }
    }
    let library = MaterialLibrary::builtins()
        .with_factory("bad", Arc::new(NoInputFactory))
        .unwrap();
    let mut config = base(vec![state("idle", vec![])]);
    config.materials.push(MaterialConfig {
        id: "bad".into(),
        key: "bad".into(),
        inputs: vec![],
        params: MaterialParams::None.into(),
    });
    assert!(ConfiguredStrategy::compile(config, &library, "i", "EURUSD").is_err());
}

#[test]
fn trade_slot_snapshots_are_total_and_pending_is_not_open() {
    let config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "pending",
                Expr::Position {
                    slot: "primary".into(),
                    field: PositionField::IsPending,
                },
            )],
        ),
        state("pending", vec![]),
    ]);
    let mut strategy = compile(config.clone()).unwrap();
    let mut pending = input(0, true);
    pending.trade_slots[0].state = TradeSlotState::Pending {
        side: Side::Buy,
        requested_price: Some(10.0),
        stoploss: Some(9.0),
    };
    strategy.evaluate(&pending).unwrap();
    assert_eq!(strategy.state_id(), "pending");

    let mut missing_slot = input(0, true);
    missing_slot.trade_slots.pop();
    assert!(
        compile(config.clone())
            .unwrap()
            .evaluate(&missing_slot)
            .is_err()
    );
    for (side, stoploss) in [(Side::Buy, 10.0), (Side::Buy, 11.0), (Side::Sell, 9.0)] {
        let mut managed = input(0, true);
        managed.trade_slots[0].state = TradeSlotState::Open {
            side,
            entry_price: 10.0,
            remaining_size: 1.0,
            stoploss: Some(stoploss),
            opened_at: time(0),
            favorable_excursion: None,
            adverse_excursion: None,
            initial_risk: None,
        };
        compile(config.clone()).unwrap().evaluate(&managed).unwrap();
    }
    let mut invalid = input(0, true);
    invalid.trade_slots[0].state = TradeSlotState::Open {
        side: Side::Buy,
        entry_price: 10.0,
        remaining_size: 0.0,
        stoploss: Some(10.0),
        opened_at: time(0),
        favorable_excursion: None,
        adverse_excursion: None,
        initial_risk: None,
    };
    assert!(compile(config).unwrap().evaluate(&invalid).is_err());
}

#[test]
fn readiness_advances_materials_but_state_evaluates_once_when_ready() {
    let mut move_state = transition(
        1,
        "ready",
        Expr::Gt {
            left: Box::new(Expr::Material { id: "ema".into() }),
            right: Box::new(price(10.5)),
        },
    );
    move_state.decision = Some(decision(None));
    let mut config = base(vec![
        state("idle", vec![move_state]),
        state("ready", vec![]),
    ]);
    config.sources = vec![source("fast")];
    config.materials = vec![
        MaterialConfig {
            id: "close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("fast"),
                field: BarField::Close,
            }
            .into(),
        },
        MaterialConfig {
            id: "ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "close".into() }],
            params: MaterialParams::Ema { period: 3 }.into(),
        },
    ];
    let mut strategy = compile(config).unwrap();
    for (second, close) in [(0, 10.0), (1, 12.0)] {
        let mut snapshot = input(second, false);
        snapshot.completed_bars.push(CompletedBarUpdate {
            source: source("fast"),
            bar: bar(close),
        });
        strategy.evaluate(&snapshot).unwrap();
    }
    assert_eq!(strategy.state_id(), "idle");
    strategy.evaluate(&input(2, true)).unwrap();
    assert_eq!(strategy.state_id(), "ready");
}

#[test]
fn action_requires_decision_and_envelope_resolves_related_trade() {
    let mut missing_decision = transition(1, "done", boolean(true));
    missing_decision.actions.push(entry_action("primary"));
    assert!(
        compile(base(vec![
            state("idle", vec![missing_decision]),
            state("done", vec![]),
        ]))
        .is_err()
    );

    let mut strategy = entry_strategy();
    let output = strategy.evaluate(&input(0, true)).unwrap();
    let command = &output.commands[0];
    assert_eq!(command.action_kind, ConfiguredActionKind::Entry);
    assert_eq!(command.trade_slot, "primary");
    let related = output.decision.unwrap().related_trade.unwrap();
    assert_eq!(related.slot, "primary");
    assert_eq!(
        Some(related.trade_id.as_str()),
        strategy.trade_id_for_slot("primary")
    );
    assert!(matches!(command.signal, RawSignal::Entry { .. }));
}

#[test]
fn output_scalars_are_typed_and_unbound_related_trade_fails_atomically() {
    let mut move_state = transition(1, "done", boolean(true));
    move_state.decision = Some(DecisionTemplate {
        kind: DecisionKind::Observation,
        reason: "values".into(),
        trade_slot: None,
        values: vec![NamedExpr {
            name: "score".into(),
            value: number(1.5),
        }],
    });
    move_state.notes.push(NoteTemplate {
        kind: NoteKind::Observation,
        reason: "note".into(),
        trade_slot: None,
        values: vec![NamedExpr {
            name: "level".into(),
            value: price(10.0),
        }],
    });
    let mut strategy = compile(base(vec![
        state("idle", vec![move_state]),
        state("done", vec![]),
    ]))
    .unwrap();
    let output = strategy.evaluate(&input(0, true)).unwrap();
    assert_eq!(
        output.decision.unwrap().values[0].value,
        OutputScalar::Number(1.5)
    );
    assert_eq!(output.notes[0].values[0].value, OutputScalar::Price(10.0));

    let mut fail = transition(1, "done", boolean(true));
    fail.notes.push(NoteTemplate {
        kind: NoteKind::Observation,
        reason: "unbound".into(),
        trade_slot: Some("primary".into()),
        values: vec![],
    });
    let mut strategy =
        compile(base(vec![state("idle", vec![fail]), state("done", vec![])])).unwrap();
    assert!(strategy.evaluate(&input(0, true)).is_err());
    assert_eq!(strategy.state_id(), "idle");
}

fn terminal(command_id: &str, status: CommandTerminalStatus) -> CommandFeedback {
    CommandFeedback::Terminal {
        command_id: command_id.into(),
        status,
        reason: (status != CommandTerminalStatus::Applied).then(|| "adapter result".into()),
    }
}

fn fact(command_id: &str, fact: CommandFact) -> CommandFeedback {
    CommandFeedback::Fact {
        command_id: command_id.into(),
        fact,
    }
}

#[test]
fn feedback_rejects_unknown_mismatch_duplicate_terminal_and_replay() {
    let mut unknown = entry_strategy();
    let mut snapshot = input(0, true);
    snapshot
        .feedback
        .push(terminal("unknown", CommandTerminalStatus::Applied));
    assert!(unknown.evaluate(&snapshot).is_err());

    let mut mismatch = entry_strategy();
    let command = mismatch.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut snapshot = input(1, true);
    snapshot
        .feedback
        .push(fact(&command, CommandFact::PositionClosed));
    assert!(mismatch.evaluate(&snapshot).is_err());

    let mut duplicate = entry_strategy();
    let command = duplicate.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut snapshot = input(1, true);
    snapshot
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Applied));
    duplicate.evaluate(&snapshot).unwrap();
    let mut snapshot = input(2, true);
    snapshot
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Applied));
    assert!(duplicate.evaluate(&snapshot).is_err());

    let mut replay = entry_strategy();
    let command = replay.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut snapshot = input(1, true);
    snapshot
        .feedback
        .push(fact(&command, CommandFact::EntryFilled));
    replay.evaluate(&snapshot).unwrap();
    let mut snapshot = input(2, true);
    snapshot
        .feedback
        .push(fact(&command, CommandFact::EntryFilled));
    assert!(replay.evaluate(&snapshot).is_err());
}

#[test]
fn entry_effect_then_applied_terminal_has_finite_lifecycle() {
    let mut strategy = entry_strategy();
    let command = strategy.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut filled = input(1, true);
    filled
        .feedback
        .push(fact(&command, CommandFact::EntryFilled));
    strategy.evaluate(&filled).unwrap();
    assert!(strategy.trade_id_for_slot("primary").is_some());
    let mut applied = input(2, true);
    applied
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Applied));
    strategy.evaluate(&applied).unwrap();
    assert!(strategy.trade_id_for_slot("primary").is_some());
}

#[derive(Clone)]
struct RejectionCountEvaluator {
    count: i64,
}

impl MaterialEvaluator for RejectionCountEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(
        &mut self,
        _: &[Value],
        context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        for slot in ["primary", "secondary"] {
            if context.feedback_matches(
                slot,
                ConfiguredActionKind::Entry,
                FeedbackField::EntryRejected,
            ) {
                self.count += 1;
            }
        }
        Ok(Value::Integer(self.count))
    }
}

struct RejectionCountFactory;

impl MaterialFactory for RejectionCountFactory {
    fn build(&self, _: &MaterialArgs, _: &[ValueType]) -> Result<MaterialBuild, String> {
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Integer),
            lookback: MaterialLookback::None,
            max_state_bytes: 8,
            evaluator: Box::new(RejectionCountEvaluator { count: 0 }),
        })
    }

    fn update_trigger(
        &self,
        _: &MaterialArgs,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::FeedbackPulse)
    }
}

#[test]
fn false_readiness_feedback_is_applied_once_without_redelivery() {
    let mut enter = transition(1, "waiting", boolean(true));
    enter.decision = Some(decision(Some("primary")));
    enter.actions.push(entry_action("primary"));
    let mut rejected = transition(
        1,
        "done",
        Expr::Feedback {
            slot: "primary".into(),
            action: ConfiguredActionKind::Entry,
            field: FeedbackField::EntryRejected,
        },
    );
    rejected.decision = Some(decision(None));
    let mut config = base(vec![
        state("idle", vec![enter]),
        state("waiting", vec![rejected]),
        state("done", vec![]),
    ]);
    config.sources.clear();
    let mut strategy = compile(config).unwrap();
    let command = strategy.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut not_ready = input(1, false);
    not_ready
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Rejected));
    strategy.evaluate(&not_ready).unwrap();
    assert_eq!(strategy.state_id(), "waiting");
    strategy.evaluate(&input(2, true)).unwrap();
    assert_eq!(strategy.state_id(), "done");
    assert!(strategy.trade_id_for_slot("primary").is_none());
}

#[test]
fn custom_feedback_material_updates_once_across_false_readiness() {
    let library = MaterialLibrary::builtins()
        .with_factory("rejection_count", Arc::new(RejectionCountFactory))
        .unwrap();
    let mut enter = transition(1, "waiting", boolean(true));
    enter.decision = Some(decision(Some("primary")));
    enter.actions.push(entry_action("primary"));
    enter.actions.push(entry_action("secondary"));
    let mut rejected = transition(
        1,
        "done",
        Expr::Eq {
            left: Box::new(Expr::Material {
                id: "rejections".into(),
            }),
            right: Box::new(literal(Literal::Integer(2))),
        },
    );
    rejected.decision = Some(decision(None));
    let mut config = base(vec![
        state("idle", vec![enter]),
        state("waiting", vec![rejected]),
        state("done", vec![]),
    ]);
    config.sources.clear();
    config.materials.push(MaterialConfig {
        id: "rejections".into(),
        key: "rejection_count".into(),
        inputs: vec![],
        params: MaterialParams::None.into(),
    });
    let mut strategy = ConfiguredStrategy::compile(config, &library, "i", "EURUSD").unwrap();
    assert!(strategy.input_requirements().needs_command_feedback);
    let commands = strategy.evaluate(&input(0, true)).unwrap().commands;
    let primary = commands
        .iter()
        .find(|command| command.trade_slot == "primary")
        .unwrap()
        .command_id
        .clone();
    let secondary = commands
        .iter()
        .find(|command| command.trade_slot == "secondary")
        .unwrap()
        .command_id
        .clone();
    let mut not_ready = input(1, false);
    not_ready
        .feedback
        .push(terminal(&primary, CommandTerminalStatus::Rejected));
    strategy.evaluate(&not_ready).unwrap();
    let mut ready = input(2, true);
    ready
        .feedback
        .push(terminal(&secondary, CommandTerminalStatus::Rejected));
    strategy.evaluate(&ready).unwrap();
    assert_eq!(strategy.state_id(), "done");
}

fn command_strategy(action: ActionTemplate) -> ConfiguredStrategy {
    let mut enter = transition(1, "command", boolean(true));
    enter.decision = Some(decision(Some("primary")));
    enter.actions.push(entry_action("primary"));
    let mut command = transition(1, "done", boolean(true));
    command.decision = Some(decision(Some("primary")));
    command.actions.push(action);
    compile(base(vec![
        state("idle", vec![enter]),
        state("command", vec![command]),
        state("done", vec![]),
    ]))
    .unwrap()
}

#[test]
fn all_management_commands_release_correlation_on_terminal() {
    let actions = vec![
        ActionTemplate::ClosePartial {
            slot: "primary".into(),
            ratio: number(0.5),
        },
        ActionTemplate::MoveStoplossToEntry {
            slot: "primary".into(),
        },
        ActionTemplate::ModifyStoploss {
            slot: "primary".into(),
            price: price(9.0),
        },
    ];
    for action in actions {
        let mut strategy = command_strategy(action.clone());
        let entry = strategy.evaluate(&input(0, true)).unwrap().commands[0]
            .command_id
            .clone();
        let mut next = input(1, true);
        next.feedback.push(fact(&entry, CommandFact::EntryFilled));
        let command = strategy.evaluate(&next).unwrap().commands[0]
            .command_id
            .clone();
        let expected_fact = match action {
            ActionTemplate::ClosePartial { .. } => CommandFact::PositionReduced,
            ActionTemplate::MoveStoplossToEntry { .. } | ActionTemplate::ModifyStoploss { .. } => {
                CommandFact::StoplossModified
            }
            _ => unreachable!(),
        };
        let mut terminal_input = input(2, true);
        terminal_input.feedback.push(fact(&command, expected_fact));
        terminal_input
            .feedback
            .push(terminal(&command, CommandTerminalStatus::Applied));
        strategy.evaluate(&terminal_input).unwrap();
        let mut replay = input(3, true);
        replay
            .feedback
            .push(terminal(&command, CommandTerminalStatus::Applied));
        assert!(strategy.evaluate(&replay).is_err());
    }
}

#[test]
fn close_and_cancel_wait_for_facts_and_release_slots() {
    let mut close = command_strategy(ActionTemplate::Close {
        slot: "primary".into(),
    });
    let entry = close.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut next = input(1, true);
    next.feedback.push(fact(&entry, CommandFact::EntryFilled));
    let command = close.evaluate(&next).unwrap().commands[0]
        .command_id
        .clone();
    let mut closed = input(2, true);
    closed
        .feedback
        .push(fact(&command, CommandFact::PositionClosed));
    close.evaluate(&closed).unwrap();
    assert!(close.trade_id_for_slot("primary").is_some());
    let mut applied = input(3, true);
    applied
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Applied));
    close.evaluate(&applied).unwrap();
    assert!(close.trade_id_for_slot("primary").is_none());

    let mut cancel = command_strategy(ActionTemplate::CancelPending {
        slot: "primary".into(),
    });
    cancel.evaluate(&input(0, true)).unwrap();
    let command = cancel.evaluate(&input(1, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut cancelled = input(2, true);
    cancelled
        .feedback
        .push(fact(&command, CommandFact::PendingCancelled));
    cancel.evaluate(&cancelled).unwrap();
    assert!(cancel.trade_id_for_slot("primary").is_some());
    let mut applied = input(3, true);
    applied
        .feedback
        .push(terminal(&command, CommandTerminalStatus::Applied));
    cancel.evaluate(&applied).unwrap();
    assert!(cancel.trade_id_for_slot("primary").is_none());
}

#[test]
fn a_skipped_close_releases_the_slot_so_the_strategy_can_trade_again() {
    // A protective stop closes a position without the strategy asking. The strategy's own close then
    // finds nothing to close and terminates as skipped. Holding the reservation in that case would
    // strand the slot for the rest of the run, so any strategy that uses a stop could trade only once.
    let mut strategy = command_strategy(ActionTemplate::Close {
        slot: "primary".into(),
    });
    let entry = strategy.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut filled = input(1, true);
    filled.feedback.push(fact(&entry, CommandFact::EntryFilled));
    let close = strategy.evaluate(&filled).unwrap().commands[0]
        .command_id
        .clone();
    assert!(strategy.trade_id_for_slot("primary").is_some());

    let mut skipped = input(2, true);
    skipped
        .feedback
        .push(terminal(&close, CommandTerminalStatus::Skipped));
    strategy.evaluate(&skipped).unwrap();
    assert!(
        strategy.trade_id_for_slot("primary").is_none(),
        "a close with nothing left to close must free its slot"
    );
}

#[test]
fn a_rejected_close_keeps_the_slot_reserved() {
    // A rejected close did not happen, so the position may still be open and the slot is still spoken for.
    let mut strategy = command_strategy(ActionTemplate::Close {
        slot: "primary".into(),
    });
    let entry = strategy.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut filled = input(1, true);
    filled.feedback.push(fact(&entry, CommandFact::EntryFilled));
    let close = strategy.evaluate(&filled).unwrap().commands[0]
        .command_id
        .clone();

    let mut rejected = input(2, true);
    rejected
        .feedback
        .push(terminal(&close, CommandTerminalStatus::Rejected));
    strategy.evaluate(&rejected).unwrap();
    assert!(strategy.trade_id_for_slot("primary").is_some());
}

#[test]
fn repeated_immediate_management_does_not_exhaust_correlations() {
    let mut enter = transition(1, "a", boolean(true));
    enter.decision = Some(decision(Some("primary")));
    enter.actions.push(entry_action("primary"));
    let mut to_b = transition(1, "b", boolean(true));
    to_b.decision = Some(decision(Some("primary")));
    to_b.actions.push(ActionTemplate::ModifyStoploss {
        slot: "primary".into(),
        price: price(9.0),
    });
    let mut to_a = transition(1, "a", boolean(true));
    to_a.decision = Some(decision(Some("primary")));
    to_a.actions.push(ActionTemplate::ClosePartial {
        slot: "primary".into(),
        ratio: number(0.5),
    });
    let mut strategy = compile(base(vec![
        state("idle", vec![enter]),
        state("a", vec![to_b]),
        state("b", vec![to_a]),
    ]))
    .unwrap();
    let mut previous = strategy.evaluate(&input(0, true)).unwrap().commands[0]
        .command_id
        .clone();
    let mut previous_fact = CommandFact::EntryFilled;
    for second in 1..90 {
        let mut snapshot = input((second % 60) as u32, true);
        snapshot.feedback.push(fact(&previous, previous_fact));
        snapshot
            .feedback
            .push(terminal(&previous, CommandTerminalStatus::Applied));
        previous = strategy.evaluate(&snapshot).unwrap().commands[0]
            .command_id
            .clone();
        previous_fact = if second % 2 == 1 {
            CommandFact::StoplossModified
        } else {
            CommandFact::PositionReduced
        };
    }
}

#[derive(Clone)]
struct CounterEvaluator {
    count: usize,
}
impl MaterialEvaluator for CounterEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }
    fn evaluate(&mut self, _: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        self.count += 1;
        Ok(Value::Integer(self.count as i64))
    }
}
struct CounterFactory;
impl MaterialFactory for CounterFactory {
    fn build(&self, _: &MaterialArgs, _: &[ValueType]) -> Result<MaterialBuild, String> {
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Integer),
            lookback: MaterialLookback::None,
            max_state_bytes: 8,
            evaluator: Box::new(CounterEvaluator { count: 0 }),
        })
    }
}

#[test]
fn custom_factory_instances_are_independent_and_output_failure_is_atomic() {
    let library = MaterialLibrary::builtins()
        .with_factory("counter", Arc::new(CounterFactory))
        .unwrap();
    let mut move_state = transition(
        1,
        "done",
        Expr::Eq {
            left: Box::new(Expr::Material {
                id: "counter".into(),
            }),
            right: Box::new(literal(Literal::Integer(1))),
        },
    );
    move_state.decision = Some(decision(None));
    let mut config = base(vec![state("idle", vec![move_state]), state("done", vec![])]);
    config.materials.push(MaterialConfig {
        id: "counter".into(),
        key: "counter".into(),
        inputs: vec![],
        params: MaterialParams::None.into(),
    });
    let mut parameterized = config.clone();
    parameterized.materials[0].params = MaterialParams::Ema { period: 2 }.into();
    assert!(ConfiguredStrategy::compile(parameterized, &library, "bad", "EURUSD").is_err());
    let mut first = ConfiguredStrategy::compile(config.clone(), &library, "a", "EURUSD").unwrap();
    let mut second = ConfiguredStrategy::compile(config, &library, "b", "EURUSD").unwrap();
    first.evaluate(&input(0, true)).unwrap();
    second.evaluate(&input(0, true)).unwrap();
    assert_eq!(first.state_id(), "done");
    assert_eq!(second.state_id(), "done");

    let mut fail = transition(1, "done", boolean(true));
    fail.notes.push(NoteTemplate {
        kind: NoteKind::Observation,
        reason: "requires trade".into(),
        trade_slot: Some("primary".into()),
        values: vec![],
    });
    let config = base(vec![state("idle", vec![fail]), state("done", vec![])]);
    let mut strategy = ConfiguredStrategy::compile(config, &library, "i1", "EURUSD").unwrap();
    assert!(strategy.evaluate(&input(0, true)).is_err());
    assert_eq!(strategy.state_id(), "idle");
}

#[test]
fn checked_arithmetic_missing_and_priority_selection_remain_deterministic() {
    let config = base(vec![
        state(
            "idle",
            vec![
                transition(
                    10,
                    "high",
                    Expr::Eq {
                        left: Box::new(missing(ScalarType::Number)),
                        right: Box::new(missing(ScalarType::Number)),
                    },
                ),
                transition(1, "low", boolean(true)),
            ],
        ),
        state("high", vec![]),
        state("low", vec![]),
    ]);
    let mut strategy = compile(config).unwrap();
    strategy.evaluate(&input(0, true)).unwrap();
    assert_eq!(strategy.state_id(), "low");

    let missing_not_equal = base(vec![
        state(
            "idle",
            vec![
                transition(
                    10,
                    "ne",
                    Expr::Ne {
                        left: Box::new(missing(ScalarType::Number)),
                        right: Box::new(number(1.0)),
                    },
                ),
                transition(
                    5,
                    "not",
                    Expr::Not {
                        value: Box::new(Expr::Eq {
                            left: Box::new(missing(ScalarType::Number)),
                            right: Box::new(number(1.0)),
                        }),
                    },
                ),
            ],
        ),
        state("ne", vec![]),
        state("not", vec![]),
    ]);
    let mut strategy = compile(missing_not_equal).unwrap();
    strategy.evaluate(&input(0, true)).unwrap();
    assert_eq!(strategy.state_id(), "not");

    let divide = Expr::Gt {
        left: Box::new(Expr::Div {
            left: Box::new(number(1.0)),
            right: Box::new(number(0.0)),
        }),
        right: Box::new(number(0.0)),
    };
    let mut strategy = compile(base(vec![
        state("idle", vec![transition(1, "done", divide)]),
        state("done", vec![]),
    ]))
    .unwrap();
    assert!(matches!(
        strategy.evaluate(&input(0, true)),
        Err(EvaluationError::DivisionByZero { .. })
    ));

    let eager = Expr::Any {
        items: vec![
            boolean(true),
            Expr::Gt {
                left: Box::new(Expr::Div {
                    left: Box::new(number(1.0)),
                    right: Box::new(number(0.0)),
                }),
                right: Box::new(number(0.0)),
            },
        ],
    };
    let mut strategy = compile(base(vec![
        state("idle", vec![transition(1, "done", eager)]),
        state("done", vec![]),
    ]))
    .unwrap();
    assert!(matches!(
        strategy.evaluate(&input(0, true)),
        Err(EvaluationError::DivisionByZero { .. })
    ));
}

fn classified_entry(slot: &str, entry_class: &str) -> ActionTemplate {
    let mut action = entry_action(slot);
    if let ActionTemplate::Entry {
        entry_class: class, ..
    } = &mut action
    {
        *class = Some(entry_class.into());
    }
    action
}

fn action_transition(target: &str, slot: &str, action: ActionTemplate) -> TransitionConfig {
    let mut transition = transition(1, target, boolean(true));
    transition.decision = Some(decision(Some(slot)));
    transition.actions = vec![action];
    transition
}

#[test]
fn entry_class_is_validated_and_lowered_into_the_entry_signal() {
    let classified = |entry_class: &str| {
        base(vec![
            state(
                "idle",
                vec![action_transition(
                    "done",
                    "primary",
                    classified_entry("primary", entry_class),
                )],
            ),
            state("done", vec![]),
        ])
    };

    let mut strategy = compile(classified("trend")).unwrap();
    let output = strategy.evaluate(&input(0, true)).unwrap();
    assert!(matches!(
        &output.commands[0].signal,
        RawSignal::Entry { entry_class: Some(class), .. } if class == "trend"
    ));

    for invalid in ["", " trend", "trend\n"] {
        let error = compile(classified(invalid)).err().unwrap();
        assert!(
            matches!(&error, CompileError::InvalidIdentifier { path, .. } if path.ends_with(".entry_class")),
            "{invalid:?} produced {error:?}"
        );
    }
}

#[test]
fn requirements_list_entry_classes_and_stop_managed_slots() {
    let strategy = compile(base(vec![
        state(
            "idle",
            vec![action_transition(
                "second",
                "primary",
                classified_entry("primary", "trend"),
            )],
        ),
        state(
            "second",
            vec![action_transition(
                "breakeven",
                "secondary",
                entry_action("secondary"),
            )],
        ),
        state(
            "breakeven",
            vec![action_transition(
                "modify",
                "secondary",
                ActionTemplate::MoveStoplossToEntry {
                    slot: "secondary".into(),
                },
            )],
        ),
        state(
            "modify",
            vec![action_transition(
                "again",
                "secondary",
                ActionTemplate::ModifyStoploss {
                    slot: "secondary".into(),
                    price: price(1.0),
                },
            )],
        ),
        state(
            "again",
            vec![action_transition(
                "done",
                "primary",
                classified_entry("primary", "trend"),
            )],
        ),
        state("done", vec![]),
    ]))
    .unwrap();
    let requirements = strategy.input_requirements();
    assert_eq!(
        requirements.entries,
        vec![
            EntryRequirement {
                slot: "primary".into(),
                entry_class: Some("trend".into()),
            },
            EntryRequirement {
                slot: "secondary".into(),
                entry_class: None,
            },
        ]
    );
    assert_eq!(
        requirements.stop_managed_slots,
        vec!["secondary".to_owned()]
    );
}

#[test]
fn entry_class_is_optional_in_documents_and_survives_binding() {
    let unclassified = serde_json::to_value(entry_action("primary")).unwrap();
    assert!(unclassified.get("entry_class").is_none());
    assert_eq!(
        serde_json::from_value::<ActionTemplate>(unclassified).unwrap(),
        entry_action("primary")
    );

    let classified = serde_json::to_value(classified_entry("primary", "trend")).unwrap();
    assert_eq!(classified["entry_class"], json!("trend"));

    let document = base(vec![
        state(
            "idle",
            vec![action_transition(
                "done",
                "primary",
                classified_entry("primary", "trend"),
            )],
        ),
        state("done", vec![]),
    ]);
    let bound = StrategyTemplate::new(document.clone())
        .bind(&ParameterBinding::default(), &MaterialLibrary::builtins())
        .unwrap();
    assert_eq!(bound, document);
}

fn open_primary(
    opened_at: NaiveDateTime,
    excursion: Option<(f64, f64)>,
    initial_risk: Option<f64>,
) -> TradeSlotFacts {
    TradeSlotFacts {
        slot: "primary".into(),
        state: TradeSlotState::Open {
            side: Side::Buy,
            entry_price: 10.0,
            remaining_size: 1.0,
            stoploss: Some(9.0),
            opened_at,
            favorable_excursion: excursion.map(|(favorable, _)| favorable),
            adverse_excursion: excursion.map(|(_, adverse)| adverse),
            initial_risk,
        },
    }
}

fn input_with(second: u32, primary: TradeSlotFacts, bars: &[&str]) -> StrategyInput {
    let mut value = input(second, true);
    value.trade_slots[0] = primary;
    value.completed_bars = bars
        .iter()
        .map(|name| CompletedBarUpdate {
            source: source(name),
            bar: bar(10.0),
        })
        .collect();
    value
}

fn position(field: PositionField) -> Expr {
    Expr::Position {
        slot: "primary".into(),
        field,
    }
}

#[test]
fn open_position_facts_are_validated_against_input_time_and_sign() {
    let config = base(vec![state("idle", vec![])]);
    let evaluate = |facts: TradeSlotFacts| {
        compile(config.clone())
            .unwrap()
            .evaluate(&input_with(5, facts, &[]))
    };
    evaluate(open_primary(time(0), Some((3.0, -2.0)), Some(4.0))).unwrap();
    evaluate(open_primary(time(5), None, None)).unwrap();
    for invalid in [
        open_primary(time(6), None, None),
        open_primary(time(0), Some((-1.0, -2.0)), None),
        open_primary(time(0), Some((3.0, 1.0)), None),
        open_primary(time(0), Some((f64::NAN, -2.0)), None),
        open_primary(time(0), None, Some(0.0)),
        open_primary(time(0), None, Some(f64::INFINITY)),
    ] {
        assert!(evaluate(invalid).is_err());
    }
}

#[test]
fn elapsed_time_and_favorable_r_rule_fires_at_the_exact_boundary() {
    let stale = Expr::All {
        items: vec![
            Expr::Gt {
                left: Box::new(Expr::Sub {
                    left: Box::new(Expr::InputTime),
                    right: Box::new(position(PositionField::OpenedAt)),
                }),
                right: Box::new(literal(Literal::DurationMillis(8_000))),
            },
            Expr::Lt {
                left: Box::new(Expr::Div {
                    left: Box::new(position(PositionField::FavorableExcursion)),
                    right: Box::new(position(PositionField::InitialRisk)),
                }),
                right: Box::new(number(0.2)),
            },
        ],
    };
    let config = base(vec![
        state("open", vec![transition(1, "timed_out", stale)]),
        state("timed_out", vec![]),
    ]);
    let first_firing = |excursion: Option<(f64, f64)>| {
        let mut strategy = compile(config.clone()).unwrap();
        (0..=12).find(|second| {
            strategy
                .evaluate(&input_with(
                    *second,
                    open_primary(time(0), excursion, Some(10.0)),
                    &[],
                ))
                .unwrap();
            strategy.state_id() == "timed_out"
        })
    };
    assert_eq!(first_firing(Some((0.5, -1.0))), Some(9));
    assert_eq!(first_firing(Some((3.0, -1.0))), None);
    assert_eq!(
        first_firing(None),
        None,
        "missing excursion never compares true"
    );
}

#[test]
fn position_materials_expose_open_facts_and_are_missing_otherwise() {
    let slot = || MaterialArgs::new([("slot", MaterialArg::Slot("primary".into()))]);
    let mut config = base(vec![
        state(
            "idle",
            vec![transition(
                1,
                "seen",
                Expr::All {
                    items: vec![
                        Expr::Eq {
                            left: Box::new(Expr::Material {
                                id: "opened_at".into(),
                            }),
                            right: Box::new(literal(Literal::Timestamp(time(1)))),
                        },
                        Expr::Eq {
                            left: Box::new(Expr::Material {
                                id: "favorable".into(),
                            }),
                            right: Box::new(number(3.0)),
                        },
                        Expr::Eq {
                            left: Box::new(Expr::Material {
                                id: "adverse".into(),
                            }),
                            right: Box::new(number(-2.0)),
                        },
                        Expr::Eq {
                            left: Box::new(Expr::Material { id: "risk".into() }),
                            right: Box::new(number(4.0)),
                        },
                    ],
                },
            )],
        ),
        state("seen", vec![]),
    ]);
    for (id, key) in [
        ("opened_at", MATERIAL_POSITION_OPENED_AT),
        ("favorable", MATERIAL_POSITION_FAVORABLE_EXCURSION),
        ("adverse", MATERIAL_POSITION_ADVERSE_EXCURSION),
        ("risk", MATERIAL_POSITION_INITIAL_RISK),
    ] {
        config.materials.push(MaterialConfig {
            id: id.into(),
            key: key.into(),
            inputs: vec![],
            params: slot(),
        });
    }
    let mut strategy = compile(config).unwrap();
    strategy.evaluate(&input(2, true)).unwrap();
    assert_eq!(
        strategy.state_id(),
        "idle",
        "a vacant slot has no open facts"
    );
    strategy
        .evaluate(&input_with(
            3,
            open_primary(time(1), Some((3.0, -2.0)), Some(4.0)),
            &[],
        ))
        .unwrap();
    assert_eq!(strategy.state_id(), "seen");
}

/// Whether `check` holds at the last input, observed through a transition that can fire only at that input's time.
fn holds_at_last_input(mut config: StrategyConfig, inputs: &[StrategyInput], check: Expr) -> bool {
    let last = inputs.last().unwrap().time;
    config.states = vec![
        state(
            "idle",
            vec![transition(
                1,
                "matched",
                Expr::All {
                    items: vec![
                        Expr::Eq {
                            left: Box::new(Expr::InputTime),
                            right: Box::new(literal(Literal::Timestamp(last))),
                        },
                        check,
                    ],
                },
            )],
        ),
        state("matched", vec![]),
    ];
    config.initial_state = "idle".into();
    let mut strategy = compile(config).unwrap();
    for input in inputs {
        strategy.evaluate(input).unwrap();
    }
    strategy.state_id() == "matched"
}

fn material_equals(id: &str, value: Literal) -> Expr {
    Expr::Eq {
        left: Box::new(Expr::Material { id: id.into() }),
        right: Box::new(literal(value)),
    }
}

#[test]
fn bars_since_open_counts_only_its_source_after_entry_and_restarts_per_position() {
    let mut config = base(vec![state("idle", vec![])]);
    config.materials.push(MaterialConfig {
        id: "bars".into(),
        key: MATERIAL_BARS_SINCE_OPEN.into(),
        inputs: vec![],
        params: MaterialArgs::new([
            ("slot", MaterialArg::Slot("primary".into())),
            ("source", MaterialArg::Source(source("fast"))),
        ]),
    });
    config.materials.push(MaterialConfig {
        id: "slow_close".into(),
        key: MATERIAL_BAR_FIELD.into(),
        inputs: vec![],
        params: MaterialParams::BarField {
            source: source("slow"),
            field: BarField::Close,
        }
        .into(),
    });
    let requirements = compile(config.clone())
        .unwrap()
        .input_requirements()
        .completed_bars
        .clone();
    assert!(
        requirements
            .iter()
            .any(|item| item.source == source("fast"))
    );

    let first = time(10);
    let second = time(14);
    let sequence = [
        input_with(10, open_primary(first, None, None), &["fast"]),
        input_with(11, open_primary(first, None, None), &["slow"]),
        input_with(12, open_primary(first, None, None), &["fast", "slow"]),
        input_with(13, vacant("primary"), &["fast"]),
        input_with(15, open_primary(second, None, None), &["fast"]),
    ];
    let count_after = |steps: usize, expected: Literal| {
        holds_at_last_input(
            config.clone(),
            &sequence[..steps],
            material_equals("bars", expected),
        )
    };
    assert!(
        count_after(1, Literal::Integer(0)),
        "a bar completing at the entry boundary closed before the position existed"
    );
    assert!(
        count_after(2, Literal::Integer(0)),
        "another source is not counted"
    );
    assert!(count_after(3, Literal::Integer(1)));
    assert!(holds_at_last_input(
        config.clone(),
        &sequence[..4],
        Expr::IsMissing {
            value: Box::new(Expr::Material { id: "bars".into() }),
        },
    ));
    assert!(
        count_after(5, Literal::Integer(1)),
        "a new position restarts the count"
    );
}

#[test]
fn calendar_materials_follow_input_time_across_a_utc_day_boundary() {
    let mut config = base(vec![state("idle", vec![])]);
    for (id, key) in [
        ("weekday", MATERIAL_WEEKDAY),
        ("seconds", MATERIAL_SECONDS_OF_DAY),
    ] {
        config.materials.push(MaterialConfig {
            id: id.into(),
            key: key.into(),
            inputs: vec![],
            params: MaterialArgs::default(),
        });
    }
    let at = |day: u32, hour: u32, minute: u32, second: u32| {
        let mut value = input(0, true);
        value.time = NaiveDate::from_ymd_opt(2026, 1, day)
            .unwrap()
            .and_hms_opt(hour, minute, second)
            .unwrap();
        value
    };
    // 2026-01-04 is a Sunday and 2026-01-05 a Monday.
    for (input, weekday, seconds) in [
        (at(4, 23, 59, 59), 7, 86_399),
        (at(5, 0, 0, 0), 1, 0),
        (at(5, 13, 30, 5), 1, 48_605),
    ] {
        assert!(holds_at_last_input(
            config.clone(),
            &[input],
            Expr::All {
                items: vec![
                    material_equals("weekday", Literal::Integer(weekday)),
                    material_equals("seconds", Literal::Integer(seconds)),
                ],
            },
        ));
    }
}
