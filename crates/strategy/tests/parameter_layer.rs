use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{Duration, NaiveDate};
use qs_strategy::*;

fn source(name: &str) -> SourceId {
    SourceId::new(name).unwrap()
}

fn literal(value: Literal) -> Expr {
    Expr::Literal { value }
}

fn material(id: &str, key: &str, inputs: Vec<Expr>, params: MaterialArgs) -> MaterialConfig {
    MaterialConfig {
        id: id.into(),
        key: key.into(),
        inputs,
        params,
    }
}

fn close_material(id: &str, source: &str) -> MaterialConfig {
    material(
        id,
        MATERIAL_BAR_FIELD,
        vec![],
        MaterialParams::BarField {
            source: self::source(source),
            field: BarField::Close,
        }
        .into(),
    )
}

fn base(materials: Vec<MaterialConfig>, output: Expr, sources: Vec<SourceId>) -> StrategyConfig {
    StrategyConfig {
        strategy_id: "parameter_test".into(),
        title: "Parameter test".into(),
        parameters: vec![],
        initial_state: "active".into(),
        sources,
        trade_slots: vec![],
        materials,
        variables: vec![],
        states: vec![
            StateConfig {
                id: "active".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::IsPresent {
                        value: Box::new(output),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Observation,
                        reason: "sample".into(),
                        trade_slot: None,
                        values: vec![],
                    }),
                    actions: vec![],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "done".into(),
                transitions: vec![],
            },
        ],
    }
}

fn strategy_input(second: u32, updates: Vec<(&str, f64)>, ready: bool) -> StrategyInput {
    StrategyInput {
        time: NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::seconds(i64::from(second)),
        ready,
        completed_bars: updates
            .into_iter()
            .map(|(source, close)| CompletedBarUpdate {
                source: self::source(source),
                bar: CompletedBar {
                    open: close,
                    high: close,
                    low: close,
                    close,
                    volume: 1.0,
                },
            })
            .collect(),
        values: vec![],
        trade_slots: vec![],
        feedback: vec![],
    }
}

#[test]
fn chained_rolling_materials_compose_lookback() {
    let config = base(
        vec![
            close_material("close", "primary"),
            material(
                "first",
                MATERIAL_SMA,
                vec![Expr::Material { id: "close".into() }],
                MaterialArgs::new([("period", MaterialArg::Integer(3))]),
            ),
            material(
                "second",
                MATERIAL_SMA,
                vec![Expr::Material { id: "first".into() }],
                MaterialArgs::new([("period", MaterialArg::Integer(2))]),
            ),
        ],
        Expr::Material {
            id: "second".into(),
        },
        vec![source("primary")],
    );
    let strategy =
        ConfiguredStrategy::compile(config, &MaterialLibrary::builtins(), "instance", "EURUSD")
            .unwrap();
    assert_eq!(
        strategy.input_requirements().completed_bars[0].required_lookback,
        4
    );
}

#[test]
fn crossing_accepts_derived_values_and_levels_but_not_constants_only() {
    let materials = vec![
        close_material("close", "primary"),
        material(
            "ema",
            MATERIAL_EMA,
            vec![Expr::Material { id: "close".into() }],
            MaterialArgs::new([("period", MaterialArg::Integer(2))]),
        ),
        material(
            "cross",
            MATERIAL_CROSS_ABOVE,
            vec![
                Expr::Sub {
                    left: Box::new(Expr::Material { id: "close".into() }),
                    right: Box::new(Expr::Material { id: "ema".into() }),
                },
                literal(Literal::Price(0.0)),
            ],
            MaterialArgs::default(),
        ),
    ];
    ConfiguredStrategy::compile(
        base(
            materials,
            Expr::Material { id: "cross".into() },
            vec![source("primary")],
        ),
        &MaterialLibrary::builtins(),
        "instance",
        "EURUSD",
    )
    .unwrap();

    let constants = base(
        vec![material(
            "cross",
            MATERIAL_CROSS_ABOVE,
            vec![literal(Literal::Number(1.0)), literal(Literal::Number(2.0))],
            MaterialArgs::default(),
        )],
        Expr::Material { id: "cross".into() },
        vec![],
    );
    assert!(
        ConfiguredStrategy::compile(
            constants,
            &MaterialLibrary::builtins(),
            "instance",
            "EURUSD"
        )
        .is_err()
    );
}

#[test]
fn template_binding_substitutes_selects_and_prunes_unused_materials() {
    let mut document = base(
        vec![
            close_material("close", "primary"),
            material(
                "short",
                MATERIAL_SMA,
                vec![Expr::Material { id: "close".into() }],
                MaterialArgs::new([("period", MaterialArg::Param("period".into()))]),
            ),
            material(
                "long",
                MATERIAL_SMA,
                vec![Expr::Material { id: "close".into() }],
                MaterialArgs::new([("period", MaterialArg::Integer(100))]),
            ),
        ],
        Expr::Select {
            param: "branch".into(),
            cases: BTreeMap::from([
                ("short".into(), Expr::Material { id: "short".into() }),
                ("long".into(), Expr::Material { id: "long".into() }),
            ]),
        },
        vec![source("primary")],
    );
    document.parameters = vec![
        ParameterConfig {
            id: "period".into(),
            kind: ParameterKind::Integer,
        },
        ParameterConfig {
            id: "branch".into(),
            kind: ParameterKind::Choice {
                options: vec!["short".into(), "long".into()],
            },
        },
    ];
    let template = StrategyTemplate::new(document);
    template.validate(&MaterialLibrary::builtins()).unwrap();
    let bound = template
        .bind(
            &ParameterBinding::new([
                ("period", ParameterValue::Integer(3)),
                ("branch", ParameterValue::Choice("short".into())),
            ]),
            &MaterialLibrary::builtins(),
        )
        .unwrap();
    assert!(bound.parameters.is_empty());
    assert_eq!(
        bound
            .materials
            .iter()
            .map(|material| material.id.as_str())
            .collect::<Vec<_>>(),
        vec!["close", "short"]
    );
    let compiled =
        ConfiguredStrategy::compile(bound, &MaterialLibrary::builtins(), "instance", "EURUSD")
            .unwrap();
    assert_eq!(
        compiled.input_requirements().completed_bars[0].required_lookback,
        3
    );
}

#[test]
fn template_validation_rejects_incomplete_mismatched_and_unknown_parameter_expressions() {
    let parameter = ParameterConfig {
        id: "choice".into(),
        kind: ParameterKind::Choice {
            options: vec!["a".into(), "b".into()],
        },
    };
    let make = |cases| {
        let mut document = base(
            vec![],
            Expr::Select {
                param: "choice".into(),
                cases,
            },
            vec![],
        );
        document.parameters = vec![parameter.clone()];
        StrategyTemplate::new(document)
    };
    assert!(
        make(BTreeMap::from([(
            "a".into(),
            literal(Literal::Number(1.0))
        )]))
        .validate(&MaterialLibrary::builtins())
        .is_err()
    );
    assert!(
        make(BTreeMap::from([
            ("a".into(), literal(Literal::Number(1.0))),
            ("b".into(), literal(Literal::Price(1.0))),
        ]))
        .validate(&MaterialLibrary::builtins())
        .is_err()
    );
    let document = base(
        vec![],
        Expr::Param {
            id: "unknown".into(),
        },
        vec![],
    );
    assert!(
        StrategyTemplate::new(document)
            .validate(&MaterialLibrary::builtins())
            .is_err()
    );
}

#[test]
fn multi_source_crossing_uses_the_latest_value_when_either_source_updates() {
    let config = base(
        vec![
            close_material("fast", "fast"),
            close_material("slow", "slow"),
            material(
                "cross",
                MATERIAL_CROSS_ABOVE,
                vec![
                    Expr::Material { id: "fast".into() },
                    Expr::Material { id: "slow".into() },
                ],
                MaterialArgs::default(),
            ),
        ],
        Expr::Material { id: "cross".into() },
        vec![source("fast"), source("slow")],
    );
    let mut strategy =
        ConfiguredStrategy::compile(config, &MaterialLibrary::builtins(), "instance", "EURUSD")
            .unwrap();
    strategy
        .evaluate(&strategy_input(0, vec![("fast", 1.0)], false))
        .unwrap();
    strategy
        .evaluate(&strategy_input(1, vec![("slow", 2.0)], false))
        .unwrap();
    let output = strategy
        .evaluate(&strategy_input(2, vec![("fast", 3.0)], true))
        .unwrap();
    assert!(output.decision.is_some());
}

#[test]
fn standard_indicator_combinations_compile_and_evaluate_from_primitives() {
    let close = || Expr::Material { id: "close".into() };
    let materials = vec![
        close_material("close", "primary"),
        material(
            "ema_fast",
            MATERIAL_EMA,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
        material(
            "ema_slow",
            MATERIAL_EMA,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(5))]),
        ),
        material(
            "macd_signal",
            MATERIAL_EMA,
            vec![Expr::Sub {
                left: Box::new(Expr::Material {
                    id: "ema_fast".into(),
                }),
                right: Box::new(Expr::Material {
                    id: "ema_slow".into(),
                }),
            }],
            MaterialArgs::new([("period", MaterialArg::Integer(2))]),
        ),
        material(
            "sma",
            MATERIAL_SMA,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
        material(
            "stddev",
            MATERIAL_STDDEV,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
        material(
            "rolling_min",
            MATERIAL_ROLLING_MIN,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
        material(
            "rolling_max",
            MATERIAL_ROLLING_MAX,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
        material(
            "stochastic_d",
            MATERIAL_SMA,
            vec![Expr::Div {
                left: Box::new(Expr::Sub {
                    left: Box::new(close()),
                    right: Box::new(Expr::Material {
                        id: "rolling_min".into(),
                    }),
                }),
                right: Box::new(Expr::Sub {
                    left: Box::new(Expr::Material {
                        id: "rolling_max".into(),
                    }),
                    right: Box::new(Expr::Material {
                        id: "rolling_min".into(),
                    }),
                }),
            }],
            MaterialArgs::new([("period", MaterialArg::Integer(2))]),
        ),
        material(
            "lag",
            MATERIAL_LAG,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(2))]),
        ),
        material(
            "rsi",
            MATERIAL_RSI,
            vec![close()],
            MaterialArgs::new([("period", MaterialArg::Integer(3))]),
        ),
    ];
    let macd_line = Expr::Sub {
        left: Box::new(Expr::Material {
            id: "ema_fast".into(),
        }),
        right: Box::new(Expr::Material {
            id: "ema_slow".into(),
        }),
    };
    let expressions = vec![
        macd_line.clone(),
        Expr::Material {
            id: "macd_signal".into(),
        },
        Expr::Sub {
            left: Box::new(macd_line),
            right: Box::new(Expr::Material {
                id: "macd_signal".into(),
            }),
        },
        Expr::Add {
            left: Box::new(Expr::Material { id: "sma".into() }),
            right: Box::new(Expr::Mul {
                left: Box::new(Expr::Material {
                    id: "stddev".into(),
                }),
                right: Box::new(literal(Literal::Number(2.0))),
            }),
        },
        Expr::Material {
            id: "stochastic_d".into(),
        },
        Expr::Sub {
            left: Box::new(Expr::Div {
                left: Box::new(close()),
                right: Box::new(Expr::Material { id: "lag".into() }),
            }),
            right: Box::new(literal(Literal::Number(1.0))),
        },
        Expr::Material { id: "rsi".into() },
    ];
    let condition = Expr::All {
        items: expressions
            .into_iter()
            .map(|value| Expr::IsPresent {
                value: Box::new(value),
            })
            .collect(),
    };
    let mut strategy = ConfiguredStrategy::compile(
        base(materials, condition, vec![source("primary")]),
        &MaterialLibrary::builtins(),
        "instance",
        "EURUSD",
    )
    .unwrap();
    for index in 0..10 {
        strategy
            .evaluate(&strategy_input(
                index,
                vec![("primary", 1.0 + f64::from(index) * 0.1)],
                true,
            ))
            .unwrap();
        if strategy.state_id() == "done" {
            break;
        }
    }
    assert_eq!(strategy.state_id(), "done");
}

#[derive(Clone)]
struct ParameterizedFactory;

#[derive(Clone)]
struct ParameterizedEvaluator;

impl MaterialEvaluator for ParameterizedEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(&mut self, _: &[Value], _: &MaterialEvalContext<'_>) -> Result<Value, String> {
        Ok(Value::Bool(true))
    }
}

impl MaterialFactory for ParameterizedFactory {
    fn params(&self) -> &[ParamSpec] {
        static PARAMS: [ParamSpec; 1] = [ParamSpec {
            name: "period",
            kind: ParamKind::Integer { min: 1, max: 10 },
            required: true,
        }];
        &PARAMS
    }

    fn build(&self, _: &MaterialArgs, inputs: &[ValueType]) -> Result<MaterialBuild, String> {
        if !inputs.is_empty() {
            return Err("no inputs expected".into());
        }
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Bool),
            lookback: MaterialLookback::None,
            max_state_bytes: 0,
            evaluator: Box::new(ParameterizedEvaluator),
        })
    }
}

#[test]
fn custom_factory_schema_accepts_only_declared_typed_bounded_arguments() {
    let library = MaterialLibrary::builtins()
        .with_factory("parameterized", Arc::new(ParameterizedFactory))
        .unwrap();
    let make = |params| {
        base(
            vec![material("custom", "parameterized", vec![], params)],
            Expr::Material {
                id: "custom".into(),
            },
            vec![],
        )
    };
    assert!(
        ConfiguredStrategy::compile(
            make(MaterialArgs::new([("period", MaterialArg::Integer(3))])),
            &library,
            "instance",
            "EURUSD"
        )
        .is_ok()
    );
    for invalid in [
        MaterialArgs::default(),
        MaterialArgs::new([("period", MaterialArg::Integer(0))]),
        MaterialArgs::new([("period", MaterialArg::Number(3.0))]),
        MaterialArgs::new([("unknown", MaterialArg::Integer(3))]),
    ] {
        assert!(
            ConfiguredStrategy::compile(make(invalid), &library, "instance", "EURUSD").is_err()
        );
    }
}
