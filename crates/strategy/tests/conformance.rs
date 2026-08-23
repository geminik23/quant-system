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
        volume: 10.0,
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

fn entry_action(slot: &str) -> ActionTemplate {
    ActionTemplate::Entry {
        slot: slot.into(),
        side: side(Side::Buy),
        order_type: OrderType::Market,
        price: missing(ScalarType::Price),
        risk: number(1.0),
        stoploss: missing(ScalarType::Price),
        targets: vec![],
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
    assert!(
        serde_json::from_value::<MaterialParams>(json!({
            "type":"custom", "values":[]
        }))
        .is_err()
    );
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
            params: MaterialParams::None,
        },
        MaterialConfig {
            id: "x".into(),
            key: MATERIAL_READINESS.into(),
            inputs: vec![],
            params: MaterialParams::None,
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
            params: MaterialParams::Ema { period: 2 },
        },
        MaterialConfig {
            id: "b".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "a".into() }],
            params: MaterialParams::Ema { period: 2 },
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
            },
        },
        MaterialConfig {
            id: "fast_ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material {
                id: "fast_close".into(),
            }],
            params: MaterialParams::Ema { period: 5 },
        },
        MaterialConfig {
            id: "slow_atr".into(),
            key: MATERIAL_ATR.into(),
            inputs: vec![],
            params: MaterialParams::Atr {
                source: source("slow"),
                period: 3,
            },
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
            params: MaterialParams::None,
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
            },
        },
        MaterialConfig {
            id: "ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "close".into() }],
            params: MaterialParams::Ema { period: 3 },
        },
        MaterialConfig {
            id: "cross".into(),
            key: MATERIAL_CROSS_ABOVE.into(),
            inputs: vec![
                Expr::Material { id: "close".into() },
                Expr::Material { id: "ema".into() },
            ],
            params: MaterialParams::None,
        },
        MaterialConfig {
            id: "slow".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source("slow"),
                field: BarField::Close,
            },
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
        },
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
    fn build(
        &self,
        _: &MaterialParams,
        input_types: &[ValueType],
    ) -> Result<MaterialBuild, String> {
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
        _: &MaterialParams,
        _: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(self.trigger.clone())
    }
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
        params: MaterialParams::None,
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
        fn build(&self, _: &MaterialParams, _: &[ValueType]) -> Result<MaterialBuild, String> {
            Ok(MaterialBuild {
                output_type: ValueType::required(ScalarType::Bool),
                lookback: MaterialLookback::InheritInputs { minimum: 2 },
                max_state_bytes: 0,
                evaluator: Box::new(PassEvaluator),
            })
        }
        fn update_trigger(
            &self,
            _: &MaterialParams,
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
        params: MaterialParams::None,
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
        };
        compile(config.clone()).unwrap().evaluate(&managed).unwrap();
    }
    let mut invalid = input(0, true);
    invalid.trade_slots[0].state = TradeSlotState::Open {
        side: Side::Buy,
        entry_price: 10.0,
        remaining_size: 0.0,
        stoploss: Some(10.0),
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
            },
        },
        MaterialConfig {
            id: "ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "close".into() }],
            params: MaterialParams::Ema { period: 3 },
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
    fn build(&self, _: &MaterialParams, _: &[ValueType]) -> Result<MaterialBuild, String> {
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Integer),
            lookback: MaterialLookback::None,
            max_state_bytes: 8,
            evaluator: Box::new(RejectionCountEvaluator { count: 0 }),
        })
    }

    fn update_trigger(
        &self,
        _: &MaterialParams,
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
        params: MaterialParams::None,
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
    fn build(&self, _: &MaterialParams, _: &[ValueType]) -> Result<MaterialBuild, String> {
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
        params: MaterialParams::None,
    });
    let mut parameterized = config.clone();
    parameterized.materials[0].params = MaterialParams::Ema { period: 2 };
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
    let mut config = base(vec![state("idle", vec![fail]), state("done", vec![])]);
    config.materials.push(MaterialConfig {
        id: "counter".into(),
        key: "counter".into(),
        inputs: vec![],
        params: MaterialParams::None,
    });
    let mut strategy = ConfiguredStrategy::compile(config, &library, "c", "EURUSD").unwrap();
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
}
