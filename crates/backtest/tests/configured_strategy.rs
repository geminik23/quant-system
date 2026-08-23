mod support;

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
    BarSeriesSpec, ConfiguredHistoricalBindings, ConfiguredNamedInputBinding,
    ConfiguredSourceBinding, ConfiguredStrategyAdapterBuildError, ConfiguredStrategyAdapterError,
    FutureQuoteConfig, HistoricalNamedInputProjector, HistoricalVolumeProjection,
    ManagementProfile, MarketEvent, MissingIntervalPolicy, NamedInputProjectionContext,
    NamedInputProjectionError, ObservationStoreLimits, PendingOrderLifecycleState, PriceBasis,
    ProjectedNamedInput, SeriesId, SeriesRequirement, StoplossMode, StrategyDescriptor, StrategyId,
    StrategyReplayError, StrategyReplayInputError, StrategyRetentionLimits, Timeframe, VecFeed,
    WarmupRequirement,
};
use qs_core::{OrderType, Side};
use qs_strategy::{
    ActionTemplate, CompletedBarRequirement, ConfiguredActionKind, DecisionKind, DecisionTemplate,
    Expr, Literal, MATERIAL_BAR_FIELD, MATERIAL_CANCELLATION_APPLIED, MATERIAL_EMA,
    MATERIAL_POSITION_PENDING, MaterialBuild, MaterialConfig, MaterialEvalContext,
    MaterialEvaluator, MaterialFactory, MaterialLibrary, MaterialLookback, MaterialParams,
    MaterialUpdateTrigger, NamedExpr, NoteKind, NoteTemplate, ScalarType, SourceId, StateConfig,
    StrategyConfig, TransitionConfig, Value, ValueType,
};
use qs_symbols::SymbolSpec;
use support::configured as shared;

const SYMBOL: &str = "EURUSD";

fn ts(minute: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::minutes(minute)
}

fn source() -> SourceId {
    SourceId::new("primary_bars").unwrap()
}

fn secondary_source() -> SourceId {
    SourceId::new("secondary_bars").unwrap()
}

fn expr_literal(value: Literal) -> Expr {
    Expr::Literal { value }
}

fn strategy_config(enter: bool, ema_period: Option<u16>) -> StrategyConfig {
    let mut materials = Vec::new();
    let condition = if let Some(period) = ema_period {
        materials.push(MaterialConfig {
            id: "close".into(),
            key: MATERIAL_BAR_FIELD.into(),
            inputs: vec![],
            params: MaterialParams::BarField {
                source: source(),
                field: qs_strategy::BarField::Close,
            },
        });
        materials.push(MaterialConfig {
            id: "ema".into(),
            key: MATERIAL_EMA.into(),
            inputs: vec![Expr::Material { id: "close".into() }],
            params: MaterialParams::Ema { period },
        });
        Expr::Gt {
            left: Box::new(Expr::Bar {
                source: source(),
                field: qs_strategy::BarField::Close,
            }),
            right: Box::new(Expr::Material { id: "ema".into() }),
        }
    } else {
        Expr::Gt {
            left: Box::new(Expr::Bar {
                source: source(),
                field: qs_strategy::BarField::Close,
            }),
            right: Box::new(expr_literal(Literal::Price(0.5))),
        }
    };
    let transitions = if enter {
        vec![TransitionConfig {
            priority: 1,
            target: "entered".into(),
            when: condition,
            assignments: vec![],
            decision: Some(DecisionTemplate {
                kind: DecisionKind::Entry,
                reason: "enter on completed bar".into(),
                trade_slot: Some("primary".into()),
                values: vec![],
            }),
            actions: vec![ActionTemplate::Entry {
                slot: "primary".into(),
                side: expr_literal(Literal::Side(Side::Buy)),
                order_type: OrderType::Market,
                price: expr_literal(Literal::Missing(ScalarType::Price)),
                risk: expr_literal(Literal::Number(1.0)),
                stoploss: expr_literal(Literal::Price(0.9)),
                targets: vec![],
            }],
            notes: vec![],
        }]
    } else {
        vec![]
    };
    let mut states = vec![StateConfig {
        id: "idle".into(),
        transitions,
    }];
    if enter {
        states.push(StateConfig {
            id: "entered".into(),
            transitions: vec![],
        });
    }
    StrategyConfig {
        strategy_id: "alpha".into(),
        title: "Neutral configured strategy".into(),
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials,
        variables: vec![],
        states,
    }
}

fn requirement_for(id: &str, symbol: &str, warmup: usize) -> SeriesRequirement {
    SeriesRequirement::new(
        SeriesId::new(id).unwrap(),
        symbol,
        Timeframe::minutes(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(warmup).unwrap(),
    )
    .unwrap()
}

fn requirement(warmup: usize) -> SeriesRequirement {
    requirement_for("m1", SYMBOL, warmup)
}

fn spec_for(id: &str, symbol: &str, warmup: usize, retained: usize) -> BarSeriesSpec {
    BarSeriesSpec::new(
        requirement_for(id, symbol, warmup),
        retained,
        0,
        MissingIntervalPolicy::Skip,
    )
    .unwrap()
}

fn spec(warmup: usize, retained: usize) -> BarSeriesSpec {
    BarSeriesSpec::new(
        requirement(warmup),
        retained,
        0,
        MissingIntervalPolicy::Skip,
    )
    .unwrap()
}

fn adapter(
    enter: bool,
    ema_period: Option<u16>,
    binding: BarSeriesSpec,
) -> Result<BacktestConfiguredStrategyAdapter, ConfiguredStrategyAdapterBuildError> {
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        strategy_config(enter, ema_period),
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), binding)],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
}

struct ReadyProjector;

impl HistoricalNamedInputProjector for ReadyProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Bool)
    }

    fn project(
        &self,
        _context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        Ok(ProjectedNamedInput {
            value: Value::Bool(true),
            updated: true,
        })
    }
}

struct FailingProjector;

impl HistoricalNamedInputProjector for FailingProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Bool)
    }

    fn project(
        &self,
        _context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        Err(NamedInputProjectionError::new("projection unavailable"))
    }
}

struct WrongRuntimeTypeProjector;

impl HistoricalNamedInputProjector for WrongRuntimeTypeProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Bool)
    }

    fn project(
        &self,
        _context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        Ok(ProjectedNamedInput {
            value: Value::Number(1.0),
            updated: true,
        })
    }
}

struct NumberProjector;

impl HistoricalNamedInputProjector for NumberProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Number)
    }

    fn project(
        &self,
        _context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        Ok(ProjectedNamedInput {
            value: Value::Number(1.0),
            updated: true,
        })
    }
}

fn named_input_strategy() -> qs_strategy::ConfiguredStrategy {
    let config = StrategyConfig {
        strategy_id: "named".into(),
        title: "Named input".into(),
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: vec![],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::Input {
                        field: "ready_input".into(),
                        value_type: ValueType::required(ScalarType::Bool),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Observation,
                        reason: "named input updated".into(),
                        trade_slot: None,
                        values: vec![],
                    }),
                    actions: vec![],
                    notes: vec![NoteTemplate {
                        kind: NoteKind::Observation,
                        reason: "named input observation".into(),
                        trade_slot: None,
                        values: vec![NamedExpr {
                            name: "score".into(),
                            value: expr_literal(Literal::Number(1.0)),
                        }],
                    }],
                }],
            },
            StateConfig {
                id: "done".into(),
                transitions: vec![],
            },
        ],
    };
    qs_strategy::ConfiguredStrategy::compile(
        config,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap()
}

fn named_input_adapter_with(
    projector: Box<dyn HistoricalNamedInputProjector>,
) -> Result<BacktestConfiguredStrategyAdapter, ConfiguredStrategyAdapterBuildError> {
    BacktestConfiguredStrategyAdapter::new(
        named_input_strategy(),
        StrategyDescriptor::new(StrategyId::new("named").unwrap(), "r1", "Named").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![ConfiguredNamedInputBinding::new("ready_input", projector)],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
}

fn named_input_adapter() -> BacktestConfiguredStrategyAdapter {
    named_input_adapter_with(Box::new(ReadyProjector)).unwrap()
}

fn pending_adapter(
    strategy_id: &str,
    management_action: ActionTemplate,
) -> BacktestConfiguredStrategyAdapter {
    let entry_condition = Expr::Gt {
        left: Box::new(Expr::Bar {
            source: source(),
            field: qs_strategy::BarField::Close,
        }),
        right: Box::new(expr_literal(Literal::Price(0.5))),
    };
    let config = StrategyConfig {
        strategy_id: strategy_id.into(),
        title: "Pending management".into(),
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: vec![],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "waiting".into(),
                    when: entry_condition,
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Entry,
                        reason: "place pending entry".into(),
                        trade_slot: Some("primary".into()),
                        values: vec![],
                    }),
                    actions: vec![ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: expr_literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Limit,
                        price: expr_literal(Literal::Price(0.5)),
                        risk: expr_literal(Literal::Number(1.0)),
                        stoploss: expr_literal(Literal::Price(0.4)),
                        targets: vec![],
                    }],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "waiting".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: expr_literal(Literal::Bool(true)),
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Management,
                        reason: "cancel pending entry".into(),
                        trade_slot: Some("primary".into()),
                        values: vec![],
                    }),
                    actions: vec![management_action],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "done".into(),
                transitions: vec![],
            },
        ],
    };
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        config,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new(strategy_id).unwrap(), "r1", "Pending").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap()
}

fn causal_pending_cancellation_adapter() -> BacktestConfiguredStrategyAdapter {
    let config = StrategyConfig {
        strategy_id: "pending_causal".into(),
        title: "Causal pending cancellation".into(),
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: vec![
            MaterialConfig {
                id: "pending".into(),
                key: MATERIAL_POSITION_PENDING.into(),
                inputs: vec![],
                params: MaterialParams::Position {
                    slot: "primary".into(),
                },
            },
            MaterialConfig {
                id: "cancelled".into(),
                key: MATERIAL_CANCELLATION_APPLIED.into(),
                inputs: vec![],
                params: MaterialParams::Feedback {
                    slot: "primary".into(),
                    action: ConfiguredActionKind::CancelPending,
                },
            },
        ],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "waiting".into(),
                    when: Expr::Gt {
                        left: Box::new(Expr::Bar {
                            source: source(),
                            field: qs_strategy::BarField::Close,
                        }),
                        right: Box::new(expr_literal(Literal::Price(0.5))),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Entry,
                        reason: "place pending entry".into(),
                        trade_slot: Some("primary".into()),
                        values: vec![],
                    }),
                    actions: vec![ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: expr_literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Limit,
                        price: expr_literal(Literal::Price(0.5)),
                        risk: expr_literal(Literal::Number(1.0)),
                        stoploss: expr_literal(Literal::Price(0.4)),
                        targets: vec![],
                    }],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "waiting".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "cancelling".into(),
                    when: Expr::Material {
                        id: "pending".into(),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Management,
                        reason: "cancel observed pending entry".into(),
                        trade_slot: Some("primary".into()),
                        values: vec![],
                    }),
                    actions: vec![ActionTemplate::CancelPending {
                        slot: "primary".into(),
                    }],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "cancelling".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::Material {
                        id: "cancelled".into(),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Observation,
                        reason: "pending cancellation committed".into(),
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
    };
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        config,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(
            StrategyId::new("pending_causal").unwrap(),
            "r1",
            "Pending causal",
        )
        .unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap()
}

fn analysis() -> AnalysisPipeline {
    AnalysisPipeline::new(
        vec![],
        ObservationStoreLimits::new(32, 32).unwrap(),
        AnnotationLimits::default(),
    )
    .unwrap()
}

fn config() -> BacktestConfig {
    BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: [(
            SYMBOL.into(),
            SymbolSpec {
                canonical: "eurusd".into(),
                pip_position: 4,
                digits: 5,
                category: "forex".into(),
                lot_base_units: 100_000,
                lot_step_units: 1_000,
                lot_min_steps: 1,
                lot_max_steps: 0,
            },
        )]
        .into_iter()
        .collect(),
        ..BacktestConfig::default()
    }
}

fn feed() -> VecFeed {
    VecFeed::new(vec![
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(0),
            bid: 1.0,
            ask: 1.0002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(1),
            bid: 1.1,
            ask: 1.1002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(2),
            bid: 1.2,
            ask: 1.2002,
        },
    ])
}

fn pending_cancellation_feed() -> VecFeed {
    VecFeed::new(vec![
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(0),
            bid: 1.0,
            ask: 1.0002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(1),
            bid: 1.1,
            ask: 1.1002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(2),
            bid: 1.2,
            ask: 1.2002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(3),
            bid: 1.2,
            ask: 1.2002,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(4),
            bid: 0.4,
            ask: 0.4002,
        },
    ])
}

#[test]
fn ema_crossovers_enter_then_close_deterministically() {
    let run = || {
        let mut adapter = shared::crossover_adapter();
        let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut shared::crossover_feed(),
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();
        assert_eq!(adapter.configured_strategy().state_id(), "closed");
        (result, adapter)
    };
    let (first, _) = run();
    let (second, _) = run();

    assert_eq!(first.decisions.records.len(), 2);
    assert_eq!(first.replay.recorded_fills.len(), 2);
    assert_eq!(first.replay.completed_positions.len(), 1);
    assert_eq!(first.replay.action_dispositions.len(), 2);
    assert_eq!(
        first
            .replay
            .action_dispositions
            .iter()
            .map(|item| item.action_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "9:crossover|10:instance_a|command:1",
            "9:crossover|10:instance_a|command:2"
        ]
    );
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
}

#[test]
fn ema_atr_lifecycle_projects_open_stop_reduction_close_and_notes() {
    let mut adapter = shared::lifecycle_adapter();
    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut shared::scenario_feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(adapter.configured_strategy().state_id(), "closed");
    assert_eq!(
        adapter.configured_strategy().trade_id_for_slot("primary"),
        None
    );
    assert_eq!(result.decisions.records.len(), 5);
    assert_eq!(result.research.journal.records.len(), 4);
    assert_eq!(
        result
            .research
            .journal
            .records
            .iter()
            .map(|record| record.reason())
            .collect::<Vec<_>>(),
        vec![
            "ATR observed at entry",
            "entry committed and position open",
            "partial close requested",
            "remaining size confirmed",
        ]
    );
    assert!(result.research.journal.records.iter().all(|record| {
        record
            .values()
            .get("atr_period")
            .is_some_and(|value| *value == 2.0)
    }));
    assert_eq!(result.replay.action_dispositions.len(), 4);
    assert!(
        result
            .replay
            .action_dispositions
            .iter()
            .all(|item| item.status == qs_backtest::ledger::ActionDispositionStatus::Applied)
    );
    assert_eq!(result.replay.recorded_fills.len(), 3);
    assert_eq!(result.replay.close_events.len(), 2);
    assert_eq!(result.replay.close_events[0].remaining_size, Some(0.5));
    assert_eq!(result.replay.close_events[1].remaining_size, Some(0.0));
    assert_eq!(result.replay.completed_positions.len(), 1);
    assert!(result.replay.open_position_snapshots.is_empty());
    assert_eq!(
        result.replay.completed_positions[0].trade_id.as_deref(),
        Some(shared::lifecycle_trade_id().as_str())
    );
}

#[derive(Clone)]
struct AlwaysTrueEvaluator;

impl MaterialEvaluator for AlwaysTrueEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(
        &mut self,
        _inputs: &[Value],
        _context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        Ok(Value::Bool(true))
    }
}

struct AlwaysTrueFactory;

impl MaterialFactory for AlwaysTrueFactory {
    fn build(
        &self,
        params: &MaterialParams,
        input_types: &[ValueType],
    ) -> Result<MaterialBuild, String> {
        if *params != MaterialParams::None || !input_types.is_empty() {
            return Err("always_true accepts no parameters or inputs".into());
        }
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Bool),
            lookback: MaterialLookback::None,
            max_state_bytes: 1,
            evaluator: Box::new(AlwaysTrueEvaluator),
        })
    }
}

#[derive(Clone)]
struct CausalCountingEvaluator {
    evaluations: Arc<AtomicUsize>,
}

impl MaterialEvaluator for CausalCountingEvaluator {
    fn clone_box(&self) -> Box<dyn MaterialEvaluator> {
        Box::new(self.clone())
    }

    fn evaluate(
        &mut self,
        _inputs: &[Value],
        _context: &MaterialEvalContext<'_>,
    ) -> Result<Value, String> {
        self.evaluations.fetch_add(1, Ordering::SeqCst);
        Ok(Value::Bool(true))
    }
}

struct CausalCountingFactory {
    evaluations: Arc<AtomicUsize>,
}

impl MaterialFactory for CausalCountingFactory {
    fn build(
        &self,
        params: &MaterialParams,
        input_types: &[ValueType],
    ) -> Result<MaterialBuild, String> {
        if *params != MaterialParams::None || !input_types.is_empty() {
            return Err("causal_counter accepts no parameters or inputs".into());
        }
        Ok(MaterialBuild {
            output_type: ValueType::required(ScalarType::Bool),
            lookback: MaterialLookback::Sources(vec![CompletedBarRequirement {
                source: source(),
                required_lookback: 1,
            }]),
            max_state_bytes: 1,
            evaluator: Box::new(CausalCountingEvaluator {
                evaluations: Arc::clone(&self.evaluations),
            }),
        })
    }

    fn update_trigger(
        &self,
        _params: &MaterialParams,
        _input_types: &[ValueType],
    ) -> Result<MaterialUpdateTrigger, String> {
        Ok(MaterialUpdateTrigger::Source(source()))
    }
}

fn custom_strategy_config(id: &str) -> StrategyConfig {
    StrategyConfig {
        strategy_id: id.into(),
        title: format!("Custom material {id}"),
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: vec![MaterialConfig {
            id: "always".into(),
            key: "always_true".into(),
            inputs: vec![],
            params: MaterialParams::None,
        }],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::Material {
                        id: "always".into(),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Observation,
                        reason: "custom material ready".into(),
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

#[test]
fn one_custom_factory_runs_in_two_historical_strategies() {
    let factory: Arc<dyn MaterialFactory> = Arc::new(AlwaysTrueFactory);
    let library = MaterialLibrary::builtins()
        .with_factory("always_true", factory)
        .unwrap();
    let mut first = shared::historical_adapter(
        custom_strategy_config("custom_a"),
        &library,
        "instance_a",
        1,
        0,
    );
    let mut second = shared::historical_adapter(
        custom_strategy_config("custom_b"),
        &library,
        "instance_b",
        1,
        0,
    );
    for adapter in [&mut first, &mut second] {
        let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut feed(),
                adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();
        assert_eq!(adapter.configured_strategy().state_id(), "done");
        assert_eq!(result.decisions.records.len(), 1);
        assert!(result.replay.action_dispositions.is_empty());
    }
}

#[test]
fn configured_entry_uses_completed_bar_warmup_and_preserves_command_id() {
    let mut adapter = adapter(true, None, spec(1, 32)).unwrap();
    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(adapter.configured_strategy().state_id(), "entered");
    assert_eq!(result.decisions.records.len(), 1);
    assert_eq!(result.decisions.records[0].emitted_signals().len(), 1);
    assert_eq!(result.decisions.records[0].emitted_signals()[0].ts(), ts(1));
    assert_eq!(result.replay.action_dispositions.len(), 1);
    assert_eq!(
        result.replay.action_dispositions[0].action_id,
        "5:alpha|10:instance_a|command:1"
    );
    assert_eq!(result.replay.action_dispositions[0].signal_ts, Some(ts(1)));
}

#[test]
fn configured_command_without_an_eligible_quote_preserves_id_and_reason() {
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        strategy_config(true, None),
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    let mut adapter = BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        120_000,
    )
    .unwrap();

    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(result.replay.action_dispositions.len(), 1);
    let disposition = &result.replay.action_dispositions[0];
    assert_eq!(disposition.action_id, "5:alpha|10:instance_a|command:1");
    assert_eq!(
        disposition.status,
        qs_backtest::ledger::ActionDispositionStatus::Rejected
    );
    assert_eq!(disposition.reason.as_deref(), Some("no_eligible_quote"));
    assert_eq!(
        adapter.configured_strategy().trade_id_for_slot("primary"),
        None
    );
}

#[test]
fn pending_entry_eod_finalization_preserves_pending_slot() {
    let mut pending_config = strategy_config(true, None);
    if let ActionTemplate::Entry {
        order_type,
        price,
        stoploss,
        ..
    } = &mut pending_config.states[0].transitions[0].actions[0]
    {
        *order_type = OrderType::Limit;
        *price = expr_literal(Literal::Price(0.5));
        *stoploss = expr_literal(Literal::Price(0.4));
    }
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        pending_config,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    let mut adapter = BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap();

    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert!(result.replay.pending_order_lifecycle.iter().any(|event| {
        event.state == PendingOrderLifecycleState::UnfilledAtEnd
            && event.placement_action_id.as_deref() == Some("5:alpha|10:instance_a|command:1")
    }));
    assert!(
        adapter
            .configured_strategy()
            .trade_id_for_slot("primary")
            .is_some()
    );
}

#[test]
fn named_input_projector_preserves_exact_type_and_update_provenance() {
    let mut adapter = named_input_adapter();
    let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    assert_eq!(adapter.configured_strategy().state_id(), "done");
    assert_eq!(result.decisions.records.len(), 1);
    assert_eq!(result.research.journal.records.len(), 1);
    assert!(result.replay.action_dispositions.is_empty());
}

#[test]
fn named_input_projector_failure_is_a_typed_runtime_error() {
    let mut adapter = named_input_adapter_with(Box::new(FailingProjector)).unwrap();
    let error = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();

    match error {
        StrategyReplayError::Strategy(ConfiguredStrategyAdapterError::NamedInput {
            name,
            source,
        }) => {
            assert_eq!(name, "ready_input");
            assert_eq!(source.to_string(), "projection unavailable");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn named_input_projector_runtime_type_mismatch_is_typed() {
    let mut adapter = named_input_adapter_with(Box::new(WrongRuntimeTypeProjector)).unwrap();
    let error = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        StrategyReplayError::Strategy(ConfiguredStrategyAdapterError::NamedInputValueType {
            name,
            expected,
        }) if name == "ready_input" && expected == ValueType::required(ScalarType::Bool)
    ));
}

#[test]
fn pending_cancellation_uses_pending_state_and_commits_once_without_later_fill() {
    let run = || {
        let mut adapter = causal_pending_cancellation_adapter();
        let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut pending_cancellation_feed(),
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();
        assert_eq!(adapter.configured_strategy().state_id(), "done");
        result
    };
    let first = run();
    let second = run();

    assert_eq!(first.decisions.records.len(), 3);
    assert_eq!(first.replay.action_dispositions.len(), 2);
    assert_eq!(
        first
            .replay
            .action_dispositions
            .iter()
            .map(|item| item.action_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "14:pending_causal|10:instance_a|command:1",
            "14:pending_causal|10:instance_a|command:2",
        ]
    );
    assert!(first.replay.action_dispositions.iter().all(|item| {
        item.status == qs_backtest::ledger::ActionDispositionStatus::Applied
            && item.reason.is_none()
    }));
    assert_eq!(
        first
            .decisions
            .records
            .iter()
            .map(|record| record.reason())
            .collect::<Vec<_>>(),
        vec![
            "place pending entry",
            "cancel observed pending entry",
            "pending cancellation committed",
        ]
    );
    assert_eq!(first.replay.pending_order_lifecycle.len(), 2);
    assert_eq!(
        first.replay.pending_order_lifecycle[0].state,
        PendingOrderLifecycleState::Placed
    );
    let cancelled = &first.replay.pending_order_lifecycle[1];
    assert_eq!(cancelled.state, PendingOrderLifecycleState::Cancelled);
    assert_eq!(
        cancelled.terminal_action_id.as_deref(),
        Some("14:pending_causal|10:instance_a|command:2")
    );
    assert_eq!(
        first
            .replay
            .pending_order_lifecycle
            .iter()
            .filter(|event| event.state.is_terminal())
            .count(),
        1
    );
    assert!(first.replay.recorded_fills.is_empty());
    assert!(first.replay.open_position_snapshots.is_empty());
    assert!(first.replay.pending_order_snapshots.is_empty());
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
}

#[test]
fn rejected_entry_and_management_preserve_command_id_and_reason() {
    let mut rejected_config = strategy_config(true, None);
    rejected_config.strategy_id = "reject_entry".into();
    if let ActionTemplate::Entry { stoploss, .. } =
        &mut rejected_config.states[0].transitions[0].actions[0]
    {
        *stoploss = expr_literal(Literal::Price(2.0));
    }
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        rejected_config,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    let mut rejected_entry = BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(
            StrategyId::new("reject_entry").unwrap(),
            "r1",
            "Rejected entry",
        )
        .unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap();
    let entry_result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut rejected_entry,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    let entry_disposition = &entry_result.replay.action_dispositions[0];
    assert_eq!(
        entry_disposition.action_id,
        "12:reject_entry|10:instance_a|command:1"
    );
    assert_eq!(
        entry_disposition.status,
        qs_backtest::ledger::ActionDispositionStatus::Rejected
    );
    assert!(
        entry_disposition
            .reason
            .as_deref()
            .is_some_and(|reason| !reason.is_empty())
    );
    assert!(entry_result.replay.recorded_fills.is_empty());

    let mut rejected_management = pending_adapter(
        "pending_reject",
        ActionTemplate::MoveStoplossToEntry {
            slot: "primary".into(),
        },
    );
    let management_result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut rejected_management,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    assert_eq!(management_result.replay.action_dispositions.len(), 2);
    let management_disposition = &management_result.replay.action_dispositions[1];
    assert_eq!(
        management_disposition.action_id,
        "14:pending_reject|10:instance_a|command:2"
    );
    assert_eq!(
        management_disposition.status,
        qs_backtest::ledger::ActionDispositionStatus::Rejected
    );
    assert!(
        management_disposition
            .reason
            .as_deref()
            .is_some_and(|reason| !reason.is_empty())
    );
}

#[test]
fn no_op_updates_causal_material_and_repeats_deterministically() {
    let run = || {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let library = MaterialLibrary::builtins()
            .with_factory(
                "causal_counter",
                Arc::new(CausalCountingFactory {
                    evaluations: Arc::clone(&evaluations),
                }),
            )
            .unwrap();
        let mut strategy = strategy_config(false, None);
        strategy.materials.push(MaterialConfig {
            id: "causal_count".into(),
            key: "causal_counter".into(),
            inputs: vec![],
            params: MaterialParams::None,
        });
        let configured =
            qs_strategy::ConfiguredStrategy::compile(strategy, &library, "instance_a", SYMBOL)
                .unwrap();
        let mut adapter = BacktestConfiguredStrategyAdapter::new(
            configured,
            StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap(),
            ConfiguredHistoricalBindings::new(
                vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        )
        .unwrap();
        let result = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut feed(),
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();
        assert_eq!(evaluations.load(Ordering::SeqCst), 2);
        assert_eq!(adapter.configured_strategy().state_id(), "idle");
        result
    };
    let first = run();
    let second = run();

    assert!(first.decisions.records.is_empty());
    assert!(first.replay.action_dispositions.is_empty());
    assert!(first.replay.recorded_fills.is_empty());
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
}

#[test]
fn source_bindings_reject_missing_duplicate_and_undeclared_sources() {
    let compile = |config| {
        qs_strategy::ConfiguredStrategy::compile(
            config,
            &MaterialLibrary::builtins(),
            "instance_a",
            SYMBOL,
        )
        .unwrap()
    };
    let descriptor =
        || StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap();

    assert!(matches!(
        BacktestConfiguredStrategyAdapter::new(
            compile(strategy_config(false, None)),
            descriptor(),
            ConfiguredHistoricalBindings::new(
                vec![],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        ),
        Err(ConfiguredStrategyAdapterBuildError::MissingSourceBinding { source_id })
            if source_id == source()
    ));

    let duplicate = ConfiguredSourceBinding::new(source(), spec(1, 32));
    assert!(matches!(
        BacktestConfiguredStrategyAdapter::new(
            compile(strategy_config(false, None)),
            descriptor(),
            ConfiguredHistoricalBindings::new(
                vec![duplicate.clone(), duplicate],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        ),
        Err(ConfiguredStrategyAdapterBuildError::DuplicateSourceBinding { source_id })
            if source_id == source()
    ));

    assert!(matches!(
        BacktestConfiguredStrategyAdapter::new(
            compile(strategy_config(false, None)),
            descriptor(),
            ConfiguredHistoricalBindings::new(
                vec![
                    ConfiguredSourceBinding::new(source(), spec(1, 32)),
                    ConfiguredSourceBinding::new(
                        secondary_source(),
                        spec_for("secondary_m1", SYMBOL, 1, 32),
                    ),
                ],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        ),
        Err(ConfiguredStrategyAdapterBuildError::UndeclaredSourceBinding { source_id })
            if source_id == secondary_source()
    ));
}

#[test]
fn source_bindings_reject_duplicate_series_symbol_mismatch_and_lookback_gaps() {
    let descriptor =
        || StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap();
    let mut multi_source = strategy_config(false, None);
    multi_source.sources.push(secondary_source());
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        multi_source,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    assert!(matches!(
        BacktestConfiguredStrategyAdapter::new(
            strategy,
            descriptor(),
            ConfiguredHistoricalBindings::new(
                vec![
                    ConfiguredSourceBinding::new(source(), spec(1, 32)),
                    ConfiguredSourceBinding::new(secondary_source(), spec(1, 32)),
                ],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        ),
        Err(ConfiguredStrategyAdapterBuildError::DuplicateSeriesBinding { series_id })
            if series_id == SeriesId::new("m1").unwrap()
    ));

    let mut symbol_mismatch = strategy_config(false, None);
    symbol_mismatch.sources.push(secondary_source());
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        symbol_mismatch,
        &MaterialLibrary::builtins(),
        "instance_a",
        SYMBOL,
    )
    .unwrap();
    assert!(matches!(
        BacktestConfiguredStrategyAdapter::new(
            strategy,
            descriptor(),
            ConfiguredHistoricalBindings::new(
                vec![
                    ConfiguredSourceBinding::new(source(), spec(1, 32)),
                    ConfiguredSourceBinding::new(
                        secondary_source(),
                        spec_for("secondary_m1", "GBPUSD", 1, 32),
                    ),
                ],
                vec![],
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        ),
        Err(ConfiguredStrategyAdapterBuildError::SourceSymbolMismatch {
            source_id,
            primary_symbol,
            series_symbol,
        }) if source_id == secondary_source()
            && primary_symbol == SYMBOL
            && series_symbol == "GBPUSD"
    ));

    assert!(matches!(
        adapter(true, Some(3), spec(1, 2)),
        Err(ConfiguredStrategyAdapterBuildError::RetentionBelowLookback {
            source_id,
            required: 3,
            retained: 2,
        }) if source_id == source()
    ));
    assert!(matches!(
        adapter(true, Some(2), spec(1, 2)),
        Err(ConfiguredStrategyAdapterBuildError::WarmupBelowLookback {
            source_id,
            required: 2,
            warmup: 1,
        }) if source_id == source()
    ));
}

#[test]
fn named_projector_bindings_reject_missing_duplicate_mismatch_and_undeclared_names() {
    let build = |named_inputs| {
        BacktestConfiguredStrategyAdapter::new(
            named_input_strategy(),
            StrategyDescriptor::new(StrategyId::new("named").unwrap(), "r1", "Named").unwrap(),
            ConfiguredHistoricalBindings::new(
                vec![ConfiguredSourceBinding::new(source(), spec(1, 32))],
                named_inputs,
                HistoricalVolumeProjection::TickCountExact,
            ),
            0,
        )
    };

    assert!(matches!(
        build(vec![]),
        Err(ConfiguredStrategyAdapterBuildError::MissingNamedInputProjector { name })
            if name == "ready_input"
    ));
    assert!(matches!(
        build(vec![
            ConfiguredNamedInputBinding::new("ready_input", Box::new(ReadyProjector)),
            ConfiguredNamedInputBinding::new("ready_input", Box::new(ReadyProjector)),
        ]),
        Err(ConfiguredStrategyAdapterBuildError::DuplicateNamedInputProjector { name })
            if name == "ready_input"
    ));
    assert!(matches!(
        build(vec![ConfiguredNamedInputBinding::new(
            "ready_input",
            Box::new(NumberProjector),
        )]),
        Err(ConfiguredStrategyAdapterBuildError::NamedInputTypeMismatch {
            name,
            expected,
            actual,
        }) if name == "ready_input"
            && expected == ValueType::required(ScalarType::Bool)
            && actual == ValueType::required(ScalarType::Number)
    ));
    assert!(matches!(
        build(vec![
            ConfiguredNamedInputBinding::new("ready_input", Box::new(ReadyProjector)),
            ConfiguredNamedInputBinding::new("undeclared", Box::new(ReadyProjector)),
        ]),
        Err(ConfiguredStrategyAdapterBuildError::UndeclaredNamedInputProjector { name })
            if name == "undeclared"
    ));
}

struct PollTrackingFeed {
    inner: VecFeed,
    polls: Cell<usize>,
}

impl qs_backtest::DataFeed for PollTrackingFeed {
    fn next_event(&mut self) -> Option<MarketEvent> {
        self.polls.set(self.polls.get() + 1);
        self.inner.next_event()
    }

    fn peek(&self) -> Option<&MarketEvent> {
        self.inner.peek()
    }
}

#[test]
fn management_profile_is_rejected_before_feed_polling() {
    let mut adapter = adapter(false, None, spec(1, 32)).unwrap();
    let mut feed = PollTrackingFeed {
        inner: feed(),
        polls: Cell::new(0),
    };
    let profile = ManagementProfile {
        name: "unsupported".into(),
        target_selection: None,
        use_targets: vec![],
        close_ratios: vec![],
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };
    let error = BacktestRunner::new_future(config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed,
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            Some(&profile),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StrategyReplayError::Input(
            StrategyReplayInputError::ConfiguredManagementProfileUnsupported
        )
    ));
    assert_eq!(feed.polls.get(), 0);
}
