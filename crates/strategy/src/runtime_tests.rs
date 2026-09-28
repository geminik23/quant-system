use super::*;
use crate::{CompletedBar, CompletedBarUpdate, Literal, MaterialArgs, NamedValue, StateConfig};

#[test]
fn failed_boundary_does_not_commit_strict_average_state() {
    for key in [crate::MATERIAL_STRICT_SMA, crate::MATERIAL_STRICT_EMA] {
        let source = SourceId::new("clock").unwrap();
        let mut strategy = ConfiguredStrategy::compile(
            StrategyConfig {
                strategy_id: "rollback".into(),
                title: "Strict numeric rollback".into(),
                parameters: vec![],
                initial_state: "idle".into(),
                sources: vec![source.clone()],
                trade_slots: vec![],
                variables: vec![],
                materials: vec![MaterialConfig {
                    id: "average".into(),
                    key: key.into(),
                    inputs: vec![Expr::Input {
                        field: "sample".into(),
                        value_type: ValueType::optional(ScalarType::Price),
                    }],
                    params: MaterialArgs::new([
                        ("source", MaterialArg::Source(source.clone())),
                        ("period", MaterialArg::Integer(3)),
                    ]),
                }],
                states: vec![
                    StateConfig {
                        id: "idle".into(),
                        transitions: vec![TransitionConfig {
                            priority: 1,
                            target: "done".into(),
                            assignments: vec![],
                            decision: None,
                            actions: vec![],
                            notes: vec![],
                            when: Expr::Gt {
                                left: Box::new(Expr::Div {
                                    left: Box::new(Expr::Literal {
                                        value: Literal::Number(1.0),
                                    }),
                                    right: Box::new(Expr::Input {
                                        field: "divisor".into(),
                                        value_type: ValueType::required(ScalarType::Number),
                                    }),
                                }),
                                right: Box::new(Expr::Literal {
                                    value: Literal::Number(0.0),
                                }),
                            },
                        }],
                    },
                    StateConfig {
                        id: "done".into(),
                        transitions: vec![],
                    },
                ],
            },
            &MaterialLibrary::builtins(),
            "instance",
            "EURUSD",
        )
        .unwrap();
        let mut input = StrategyInput {
            time: chrono::NaiveDateTime::default(),
            ready: false,
            completed_bars: vec![CompletedBarUpdate {
                source,
                bar: CompletedBar {
                    open: 10.0,
                    high: 11.0,
                    low: 9.0,
                    close: 10.0,
                    volume: Some(1.0),
                },
            }],
            values: vec![
                NamedValue {
                    name: "sample".into(),
                    value: Value::Price(10.0),
                    updated: true,
                },
                NamedValue {
                    name: "divisor".into(),
                    value: Value::Number(0.0),
                    updated: true,
                },
            ],
            trade_slots: vec![],
            feedback: vec![],
        };
        for sample in [10.0, 12.0, 14.0] {
            input.values[0].value = Value::Price(sample);
            strategy.evaluate(&input).unwrap();
        }
        assert_eq!(strategy.material_values, vec![Value::Price(12.0)]);
        input.values[0].value = Value::Price(16.0);
        input.ready = true;
        assert!(matches!(
            strategy.evaluate(&input),
            Err(EvaluationError::DivisionByZero { .. })
        ));
        assert_eq!(strategy.material_values, vec![Value::Price(12.0)]);
        assert_eq!(strategy.state_id(), "idle");
        assert!(strategy.terminal);
        let context = MaterialEvalContext {
            input: &input,
            input_updates: &[true],
            any_input_updates: &[true],
            feedback: &[],
            retained_feedback: &[],
        };
        let mut retained = strategy.materials[0].evaluator.clone_box();
        let actual = retained.evaluate(&[Value::Price(20.0)], &context).unwrap();
        assert_eq!(
            actual,
            Value::Price(if key == crate::MATERIAL_STRICT_SMA {
                46.0 / 3.0
            } else {
                16.0
            })
        );
    }
}
