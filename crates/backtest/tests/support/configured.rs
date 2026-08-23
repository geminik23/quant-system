#![allow(dead_code)]

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BarSeriesSpec,
    ConfiguredHistoricalBindings, ConfiguredSourceBinding, HistoricalVolumeProjection,
    MissingIntervalPolicy, ObservationStoreLimits, PositionRef, PriceBasis, RawSignal, SeriesId,
    SeriesRequirement, StrategyDescriptor, StrategyId, Timeframe, VecFeed, WarmupRequirement,
};
use qs_core::{OrderType, Side};
use qs_strategy::{
    ActionTemplate, DecisionKind, DecisionTemplate, Expr, Literal, MATERIAL_ATR,
    MATERIAL_BAR_FIELD, MATERIAL_CROSS_ABOVE, MATERIAL_CROSS_BELOW, MATERIAL_EMA,
    MATERIAL_POSITION_OPEN, MATERIAL_POSITION_REMAINING_SIZE, MaterialConfig, MaterialLibrary,
    MaterialParams, NamedExpr, NoteKind, NoteTemplate, ScalarType, SourceId, StateConfig,
    StrategyConfig, TransitionConfig,
};
use qs_symbols::SymbolSpec;

pub const SYMBOL: &str = "EURUSD";

pub fn ts(minute: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::minutes(minute)
}

fn source() -> SourceId {
    SourceId::new("primary_bars").unwrap()
}

fn literal(value: Literal) -> Expr {
    Expr::Literal { value }
}

pub fn adapter() -> BacktestConfiguredStrategyAdapter {
    let config = StrategyConfig {
        strategy_id: "alpha".into(),
        title: "Neutral configured strategy".into(),
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
                    target: "entered".into(),
                    when: Expr::Gt {
                        left: Box::new(Expr::Bar {
                            source: source(),
                            field: qs_strategy::BarField::Close,
                        }),
                        right: Box::new(literal(Literal::Price(0.5))),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Entry,
                        reason: "enter on completed bar".into(),
                        trade_slot: Some("primary".into()),
                        values: vec![],
                    }),
                    actions: vec![ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Market,
                        price: literal(Literal::Missing(ScalarType::Price)),
                        risk: literal(Literal::Number(1.0)),
                        stoploss: literal(Literal::Price(0.9)),
                        targets: vec![],
                    }],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "entered".into(),
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
    let requirement = SeriesRequirement::new(
        SeriesId::new("m1").unwrap(),
        SYMBOL,
        Timeframe::minutes(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(1).unwrap(),
    )
    .unwrap();
    let series = BarSeriesSpec::new(requirement, 32, 0, MissingIntervalPolicy::Skip).unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new("alpha").unwrap(), "r1", "Alpha").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), series)],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap()
}

pub fn analysis() -> AnalysisPipeline {
    AnalysisPipeline::new(
        vec![],
        ObservationStoreLimits::new(32, 32).unwrap(),
        AnnotationLimits::default(),
    )
    .unwrap()
}

pub fn runner_config() -> BacktestConfig {
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

pub fn events() -> Vec<MarketEvent> {
    vec![
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
    ]
}

pub fn feed() -> VecFeed {
    VecFeed::new(events())
}

pub fn batches() -> Vec<TimestampBatch> {
    events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| TimestampBatch {
            ts: event.ts(),
            events: vec![FeedEvent::new(
                event,
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )],
        })
        .collect()
}

fn material(id: &str, key: &str, inputs: Vec<Expr>, params: MaterialParams) -> MaterialConfig {
    MaterialConfig {
        id: id.into(),
        key: key.into(),
        inputs,
        params,
    }
}

fn decision(kind: DecisionKind, reason: &str) -> DecisionTemplate {
    DecisionTemplate {
        kind,
        reason: reason.into(),
        trade_slot: Some("primary".into()),
        values: vec![],
    }
}

fn transition(
    target: &str,
    when: Expr,
    decision: DecisionTemplate,
    action: ActionTemplate,
    notes: Vec<NoteTemplate>,
) -> TransitionConfig {
    TransitionConfig {
        priority: 1,
        target: target.into(),
        when,
        assignments: vec![],
        decision: Some(decision),
        actions: vec![action],
        notes,
    }
}

fn indicator_materials(include_position: bool) -> Vec<MaterialConfig> {
    let mut materials = vec![
        material(
            "close",
            MATERIAL_BAR_FIELD,
            vec![],
            MaterialParams::BarField {
                source: source(),
                field: qs_strategy::BarField::Close,
            },
        ),
        material(
            "ema",
            MATERIAL_EMA,
            vec![Expr::Material { id: "close".into() }],
            MaterialParams::Ema { period: 2 },
        ),
        material(
            "atr",
            MATERIAL_ATR,
            vec![],
            MaterialParams::Atr {
                source: source(),
                period: 2,
            },
        ),
        material(
            "cross_up",
            MATERIAL_CROSS_ABOVE,
            vec![
                Expr::Material { id: "close".into() },
                Expr::Material { id: "ema".into() },
            ],
            MaterialParams::None,
        ),
        material(
            "cross_down",
            MATERIAL_CROSS_BELOW,
            vec![
                Expr::Material { id: "close".into() },
                Expr::Material { id: "ema".into() },
            ],
            MaterialParams::None,
        ),
    ];
    if include_position {
        materials.extend([
            material(
                "position_open",
                MATERIAL_POSITION_OPEN,
                vec![],
                MaterialParams::Position {
                    slot: "primary".into(),
                },
            ),
            material(
                "remaining_size",
                MATERIAL_POSITION_REMAINING_SIZE,
                vec![],
                MaterialParams::Position {
                    slot: "primary".into(),
                },
            ),
        ]);
    }
    materials
}

pub fn historical_adapter(
    config: StrategyConfig,
    library: &MaterialLibrary,
    instance: &str,
    warmup: usize,
    latency_ms: u64,
) -> BacktestConfiguredStrategyAdapter {
    let strategy_id = config.strategy_id.clone();
    let strategy =
        qs_strategy::ConfiguredStrategy::compile(config, library, instance, SYMBOL).unwrap();
    let requirement = SeriesRequirement::new(
        SeriesId::new("m1").unwrap(),
        SYMBOL,
        Timeframe::minutes(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(warmup).unwrap(),
    )
    .unwrap();
    let series = BarSeriesSpec::new(requirement, 64, 0, MissingIntervalPolicy::Skip).unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(
            StrategyId::new(strategy_id.clone()).unwrap(),
            "r1",
            strategy_id,
        )
        .unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(source(), series)],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        latency_ms,
    )
    .unwrap()
}

pub fn crossover_adapter() -> BacktestConfiguredStrategyAdapter {
    let config = StrategyConfig {
        strategy_id: "crossover".into(),
        title: "EMA crossover".into(),
        initial_state: "flat".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: indicator_materials(false),
        variables: vec![],
        states: vec![
            StateConfig {
                id: "flat".into(),
                transitions: vec![transition(
                    "long",
                    Expr::Material {
                        id: "cross_up".into(),
                    },
                    decision(DecisionKind::Entry, "EMA crossed above"),
                    ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Market,
                        price: literal(Literal::Missing(ScalarType::Price)),
                        risk: literal(Literal::Number(1.0)),
                        stoploss: literal(Literal::Price(0.5)),
                        targets: vec![],
                    },
                    vec![],
                )],
            },
            StateConfig {
                id: "long".into(),
                transitions: vec![transition(
                    "closed",
                    Expr::Material {
                        id: "cross_down".into(),
                    },
                    decision(DecisionKind::Exit, "EMA crossed below"),
                    ActionTemplate::Close {
                        slot: "primary".into(),
                    },
                    vec![],
                )],
            },
            StateConfig {
                id: "closed".into(),
                transitions: vec![],
            },
        ],
    };
    historical_adapter(config, &MaterialLibrary::builtins(), "instance_a", 3, 0)
}

pub fn lifecycle_adapter() -> BacktestConfiguredStrategyAdapter {
    let note = |kind, reason: &str| NoteTemplate {
        kind,
        reason: reason.into(),
        trade_slot: Some("primary".into()),
        values: vec![NamedExpr {
            name: "atr_period".into(),
            value: literal(Literal::Number(2.0)),
        }],
    };
    let config = StrategyConfig {
        strategy_id: "lifecycle".into(),
        title: "EMA ATR lifecycle".into(),
        initial_state: "flat".into(),
        sources: vec![source()],
        trade_slots: vec!["primary".into()],
        materials: indicator_materials(true),
        variables: vec![],
        states: vec![
            StateConfig {
                id: "flat".into(),
                transitions: vec![transition(
                    "awaiting_open",
                    Expr::All {
                        items: vec![
                            Expr::Material {
                                id: "cross_up".into(),
                            },
                            Expr::IsPresent {
                                value: Box::new(Expr::Material { id: "atr".into() }),
                            },
                        ],
                    },
                    decision(DecisionKind::Entry, "open lifecycle position"),
                    ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Market,
                        price: literal(Literal::Missing(ScalarType::Price)),
                        risk: literal(Literal::Number(1.0)),
                        stoploss: literal(Literal::Price(0.5)),
                        targets: vec![],
                    },
                    vec![note(NoteKind::Risk, "ATR observed at entry")],
                )],
            },
            StateConfig {
                id: "awaiting_open".into(),
                transitions: vec![transition(
                    "protected",
                    Expr::Material {
                        id: "position_open".into(),
                    },
                    decision(DecisionKind::Management, "move stop to entry"),
                    ActionTemplate::MoveStoplossToEntry {
                        slot: "primary".into(),
                    },
                    vec![note(
                        NoteKind::Execution,
                        "entry committed and position open",
                    )],
                )],
            },
            StateConfig {
                id: "protected".into(),
                transitions: vec![transition(
                    "awaiting_reduction",
                    literal(Literal::Bool(true)),
                    decision(DecisionKind::Management, "reduce position"),
                    ActionTemplate::ClosePartial {
                        slot: "primary".into(),
                        ratio: literal(Literal::Number(0.5)),
                    },
                    vec![note(NoteKind::Lifecycle, "partial close requested")],
                )],
            },
            StateConfig {
                id: "awaiting_reduction".into(),
                transitions: vec![transition(
                    "closing",
                    Expr::Lt {
                        left: Box::new(Expr::Material {
                            id: "remaining_size".into(),
                        }),
                        right: Box::new(literal(Literal::Number(0.75))),
                    },
                    decision(DecisionKind::Exit, "close reduced remainder"),
                    ActionTemplate::Close {
                        slot: "primary".into(),
                    },
                    vec![note(NoteKind::Lifecycle, "remaining size confirmed")],
                )],
            },
            StateConfig {
                id: "closing".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "closed".into(),
                    when: Expr::Not {
                        value: Box::new(Expr::Material {
                            id: "position_open".into(),
                        }),
                    },
                    assignments: vec![],
                    decision: Some(DecisionTemplate {
                        kind: DecisionKind::Observation,
                        reason: "position close committed".into(),
                        trade_slot: None,
                        values: vec![],
                    }),
                    actions: vec![],
                    notes: vec![],
                }],
            },
            StateConfig {
                id: "closed".into(),
                transitions: vec![],
            },
        ],
    };
    historical_adapter(
        config,
        &MaterialLibrary::builtins(),
        "instance_a",
        3,
        60_000,
    )
}

pub fn crossover_events() -> Vec<MarketEvent> {
    [2.0, 1.0, 2.0, 2.0, 0.8, 0.7, 0.6, 0.5]
        .into_iter()
        .enumerate()
        .map(|(minute, bid)| MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(minute as i64),
            bid,
            ask: bid + 0.0002,
        })
        .collect()
}

pub fn crossover_feed() -> VecFeed {
    VecFeed::new(crossover_events())
}

pub fn scenario_events() -> Vec<MarketEvent> {
    [2.0, 1.0, 2.0, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6]
        .into_iter()
        .enumerate()
        .map(|(minute, bid)| MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(minute as i64),
            bid,
            ask: bid + 0.0002,
        })
        .collect()
}

pub fn scenario_feed() -> VecFeed {
    VecFeed::new(scenario_events())
}

pub fn scenario_batches() -> Vec<TimestampBatch> {
    scenario_events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| TimestampBatch {
            ts: event.ts(),
            events: vec![FeedEvent::new(
                event,
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )],
        })
        .collect()
}

pub fn lifecycle_trade_id() -> String {
    "9:lifecycle|10:instance_a|campaign:1|trade:1".into()
}

pub fn direct_lifecycle_signals() -> Vec<RawSignal> {
    let trade_id = lifecycle_trade_id();
    vec![
        RawSignal::Entry {
            ts: ts(3),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: Some(0.5),
            targets: vec![],
            group: Some("9:lifecycle|10:instance_a|campaign:1".into()),
            trade_id: Some(trade_id.clone()),
        },
        RawSignal::MoveStoplossToEntry {
            ts: ts(4),
            position: PositionRef::ByTradeId {
                trade_id: trade_id.clone(),
            },
        },
        RawSignal::ClosePartial {
            ts: ts(5),
            position: PositionRef::ByTradeId {
                trade_id: trade_id.clone(),
            },
            ratio: 0.5,
        },
        RawSignal::Close {
            ts: ts(6),
            position: PositionRef::ByTradeId { trade_id },
        },
    ]
}

pub fn direct_entry() -> RawSignal {
    RawSignal::Entry {
        ts: ts(1),
        symbol: SYMBOL.into(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(0.9),
        targets: vec![],
        group: Some("5:alpha|10:instance_a|campaign:1".into()),
        trade_id: Some("5:alpha|10:instance_a|trade:1".into()),
    }
}
