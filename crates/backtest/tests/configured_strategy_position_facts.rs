//! Open-position time, excursion, and initial-risk facts and fixed-UTC sessions, as a configured strategy sees them during historical replay.

mod support;

use chrono::NaiveTime;
use qs_backtest::{
    BacktestConfiguredStrategyAdapter, BacktestRunner, BarSeriesSpec, ConfiguredHistoricalBindings,
    ConfiguredNamedInputBinding, ConfiguredSourceBinding, FixedUtcSessionProjector,
    FutureQuoteConfig, HistoricalVolumeProjection, MarketEvent, MissingIntervalPolicy, PriceBasis,
    SeriesId, SeriesRequirement, StrategyBacktestResult, StrategyDecisionKind, StrategyDescriptor,
    StrategyId, StrategyRetentionLimits, Timeframe, VecFeed, WarmupRequirement,
};
use qs_core::{OrderType, Side};
use qs_strategy::{
    ActionTemplate, DecisionKind, DecisionTemplate, Expr, Literal, MaterialLibrary, PositionField,
    ScalarType, SourceId, StateConfig, StrategyConfig, TransitionConfig, ValueType,
};
use support::configured::{SYMBOL, analysis, runner_config, ts};

/// The support configuration sizes one lot and prices it at contract size one, so profit is the price move itself.
const CONTRACT_UNITS: f64 = 1.0;
const SPREAD: f64 = 0.0002;
const STOPLOSS: f64 = 0.5;

fn literal(value: Literal) -> Expr {
    Expr::Literal { value }
}

fn position(field: PositionField) -> Expr {
    Expr::Position {
        slot: "primary".into(),
        field,
    }
}

fn number(value: f64) -> Expr {
    literal(Literal::Number(value))
}

fn transition(
    target: &str,
    when: Expr,
    kind: DecisionKind,
    action: ActionTemplate,
) -> TransitionConfig {
    TransitionConfig {
        priority: 1,
        target: target.into(),
        when,
        assignments: vec![],
        decision: Some(DecisionTemplate {
            kind,
            reason: format!("move to {target}"),
            trade_slot: Some("primary".into()),
            values: vec![],
        }),
        actions: vec![action],
        notes: vec![],
    }
}

/// Enter long when `entry` holds, then close when `exit` holds.
fn strategy(entry: Expr, exit: Expr) -> StrategyConfig {
    StrategyConfig {
        strategy_id: "facts".into(),
        title: "Position facts".into(),
        parameters: vec![],
        initial_state: "flat".into(),
        sources: vec![SourceId::new("primary").unwrap()],
        trade_slots: vec!["primary".into()],
        materials: vec![],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "flat".into(),
                transitions: vec![transition(
                    "long",
                    entry,
                    DecisionKind::Entry,
                    ActionTemplate::Entry {
                        slot: "primary".into(),
                        side: literal(Literal::Side(Side::Buy)),
                        order_type: OrderType::Market,
                        price: literal(Literal::Missing(ScalarType::Price)),
                        risk: number(1.0),
                        stoploss: literal(Literal::Price(STOPLOSS)),
                        targets: vec![],
                        entry_class: None,
                    },
                )],
            },
            StateConfig {
                id: "long".into(),
                transitions: vec![transition(
                    "exited",
                    exit,
                    DecisionKind::Exit,
                    ActionTemplate::Close {
                        slot: "primary".into(),
                    },
                )],
            },
            StateConfig {
                id: "exited".into(),
                transitions: vec![],
            },
        ],
    }
}

fn adapter(
    config: StrategyConfig,
    named: Vec<ConfiguredNamedInputBinding>,
) -> BacktestConfiguredStrategyAdapter {
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
    let series = BarSeriesSpec::new(requirement, 8, 0, MissingIntervalPolicy::Skip).unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new("facts").unwrap(), "r1", "Facts").unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(
                SourceId::new("primary").unwrap(),
                series,
            )],
            named,
            HistoricalVolumeProjection::TickCountExact,
        ),
        0,
    )
    .unwrap()
}

/// One tick per minute; the entry decided at minute 1 fills at minute 2 at 2.0002.
fn feed() -> VecFeed {
    VecFeed::new(
        [1.0, 1.0, 2.0, 2.1, 2.3, 2.2, 2.2, 2.2, 2.2]
            .into_iter()
            .enumerate()
            .map(|(minute, bid)| MarketEvent::Tick {
                symbol: SYMBOL.into(),
                ts: ts(minute as i64),
                bid,
                ask: bid + SPREAD,
            })
            .collect(),
    )
}

fn run(config: StrategyConfig, named: Vec<ConfiguredNamedInputBinding>) -> StrategyBacktestResult {
    let mut adapter = adapter(config, named);
    BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap()
}

fn decided_at(
    result: &StrategyBacktestResult,
    kind: StrategyDecisionKind,
) -> Option<chrono::NaiveDateTime> {
    result
        .decisions
        .records
        .iter()
        .find(|record| record.kind() == kind)
        .map(|record| record.observed_through())
}

fn always() -> Expr {
    literal(Literal::Bool(true))
}

#[test]
fn opened_at_is_the_entry_fill_time() {
    let result = run(
        strategy(
            always(),
            Expr::Gt {
                left: Box::new(Expr::Sub {
                    left: Box::new(Expr::InputTime),
                    right: Box::new(position(PositionField::OpenedAt)),
                }),
                right: Box::new(literal(Literal::DurationMillis(120_000))),
            },
        ),
        vec![],
    );
    let fill = result.replay.recorded_fills[0].execution_ts.unwrap();
    assert_eq!(fill, ts(2));
    assert_eq!(decided_at(&result, StrategyDecisionKind::Exit), Some(ts(5)));
}

#[test]
fn favorable_excursion_excludes_the_boundary_s_own_quote() {
    // Campaign profit after the 2.1 tick is 0.0998 and after the 2.3 tick is 0.2998; a rule above 0.2 must wait for the boundary after the 2.3 tick.
    let result = run(
        strategy(
            always(),
            Expr::Gt {
                left: Box::new(position(PositionField::FavorableExcursion)),
                right: Box::new(number(0.2)),
            },
        ),
        vec![],
    );
    assert_eq!(decided_at(&result, StrategyDecisionKind::Exit), Some(ts(5)));

    let adverse = run(
        strategy(
            always(),
            Expr::Lt {
                left: Box::new(position(PositionField::AdverseExcursion)),
                right: Box::new(number(0.0)),
            },
        ),
        vec![],
    );
    // The fill quote marks the position at the bid, one spread below the entry, and that mark is visible from the next boundary.
    assert_eq!(
        decided_at(&adverse, StrategyDecisionKind::Exit),
        Some(ts(3))
    );
}

#[test]
fn initial_risk_uses_the_reported_r_basis() {
    let expected = (2.0 + SPREAD - STOPLOSS) * CONTRACT_UNITS;
    let result = run(
        strategy(
            always(),
            Expr::All {
                items: vec![
                    Expr::Gt {
                        left: Box::new(position(PositionField::InitialRisk)),
                        right: Box::new(number(expected - 1e-9)),
                    },
                    Expr::Lt {
                        left: Box::new(position(PositionField::InitialRisk)),
                        right: Box::new(number(expected + 1e-9)),
                    },
                ],
            },
        ),
        vec![],
    );
    assert_eq!(decided_at(&result, StrategyDecisionKind::Exit), Some(ts(2)));
    let reported = result.replay.completed_positions[0].initial_risk().unwrap();
    assert!((reported - expected).abs() < 1e-6);
}

#[test]
fn a_fixed_utc_session_gates_entries_as_a_named_input() {
    let session =
        FixedUtcSessionProjector::new([(ts(4).time(), NaiveTime::from_hms_opt(23, 0, 0).unwrap())])
            .unwrap();
    let entry = Expr::Input {
        field: "session".into(),
        value_type: ValueType::required(ScalarType::Bool),
    };
    let result = run(
        strategy(entry, literal(Literal::Bool(false))),
        vec![ConfiguredNamedInputBinding::new(
            "session",
            Box::new(session),
        )],
    );
    assert_eq!(
        decided_at(&result, StrategyDecisionKind::Entry),
        Some(ts(4))
    );
}
