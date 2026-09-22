mod support;

use std::collections::BTreeMap;

use qs_backtest::Timeframe;
use qs_backtest::evaluation::{BreakdownDimension, EvaluationOptions};
use qs_research::families::{EmaCrossFamily, EmaEntryCondition};
use qs_research::{
    DataWindow, DeclaredSpace, ResearchPlan, StrategyFamily, SymbolEvents, WindowPlan, run_batch,
};
use qs_strategy::{ParameterConfig, ParameterKind, StrategyConfig};
use support::{SYMBOL, at, config, synthetic_ticks};

fn strategy_toml() -> &'static str {
    include_str!("../examples/ema_strategy.toml")
}

fn public_space_toml() -> &'static str {
    include_str!("../examples/ema_space.toml")
}

fn space_toml() -> &'static str {
    r#"
family_id = "ema_cross"

[parameters.ema_fast]
range = { from = 3, to = 4, step = 1 }

[parameters.ema_slow]
values = [8]

[parameters.atr_stop]
range = { from = 1.0, to = 1.5, step = 0.5 }

[parameters.entry]
all = true

[[constraints]]
op = "lt"
left = { op = "param", id = "ema_fast" }
right = { op = "param", id = "ema_slow" }

[[series]]
source = "primary"
symbol = { plan_symbol = true }
timeframe_seconds = 60
price_basis = "mid"
alignment_offset_seconds = 0
"#
}

fn events() -> BTreeMap<String, SymbolEvents> {
    BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(960))])
}

fn plan(workers: usize) -> ResearchPlan {
    ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::Fixed {
            in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
            out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
        },
        config(),
    )
    .with_workers(workers)
}

#[test]
fn declared_space_enumerates_constraints_and_matches_the_rust_family() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    assert_eq!(declared.points().len(), 12);

    let rust = EmaCrossFamily::new(3..=4, 8..=8, vec![10, 15], Timeframe::minutes(1).unwrap())
        .with_atr_period(14)
        .with_entry_conditions(vec![
            EmaEntryCondition::Cross,
            EmaEntryCondition::PriceAboveFast,
            EmaEntryCondition::RsiAboveFifty,
        ]);
    let declared_batch = run_batch(&plan(1), &declared, &events()).unwrap();
    let rust_batch = run_batch(&plan(1), &rust, &events()).unwrap();
    assert_eq!(declared_batch.table(), rust_batch.table());
    assert_eq!(declared_batch.table().rows()[0].points_total, 12);
}

#[test]
fn checked_in_public_documents_load_and_execute_as_a_batch() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), public_space_toml()).unwrap();
    assert_eq!(declared.points().len(), 48);
    let batch = run_batch(&plan(4), &declared, &events()).unwrap();
    assert_eq!(batch.table().len(), 96);
    assert!(
        batch
            .table()
            .rows()
            .iter()
            .all(|row| row.status.is_completed())
    );

    let report = batch.evaluate(EvaluationOptions {
        breakdowns: vec![BreakdownDimension::Tag("entry".into())],
        minimum_breakdown_bucket_count: 1,
        ..EvaluationOptions::default()
    });
    let entry = report
        .breakdowns
        .unwrap()
        .into_iter()
        .find(|breakdown| breakdown.dimension == BreakdownDimension::Tag("entry".into()))
        .unwrap();
    assert_eq!(entry.buckets.len(), 3);
}

#[test]
fn pooled_evaluation_has_one_bucket_per_parameter_value_and_is_parallel_stable() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    let sequential = run_batch(&plan(1), &declared, &events()).unwrap();
    let parallel = run_batch(&plan(4), &declared, &events()).unwrap();
    assert_eq!(sequential, parallel);

    let report = sequential.evaluate(EvaluationOptions {
        breakdowns: vec![BreakdownDimension::Tag("entry".into())],
        minimum_breakdown_bucket_count: 1,
        ..EvaluationOptions::default()
    });
    let breakdown = report
        .breakdowns
        .unwrap()
        .into_iter()
        .find(|breakdown| breakdown.dimension == BreakdownDimension::Tag("entry".into()))
        .unwrap();
    assert_eq!(breakdown.buckets.len(), 3);
    let bucket_positions: usize = breakdown
        .buckets
        .iter()
        .map(|bucket| bucket.performance.position_count)
        .sum();
    assert_eq!(bucket_positions, sequential.position_outcomes().len());
}

#[test]
fn bound_documents_round_trip_and_geometry_may_use_a_parameter() {
    let mut template: StrategyConfig = toml::from_str(strategy_toml()).unwrap();
    template.parameters.push(ParameterConfig {
        id: "timeframe_seconds".into(),
        kind: ParameterKind::Integer,
    });
    let strategy = toml::to_string_pretty(&template).unwrap();
    let space = space_toml().replace(
        "[parameters.entry]\nall = true",
        "[parameters.entry]\nvalues = [\"cross\"]\n\n[parameters.timeframe_seconds]\nvalues = [60, 120]",
    )
    .replace("timeframe_seconds = 60", "timeframe_seconds = { param = \"timeframe_seconds\" }");
    let declared = DeclaredSpace::from_toml(&strategy, &space).unwrap();
    assert_eq!(declared.points().len(), 8);
    let first = declared.points()[0];
    let last = *declared.points().last().unwrap();
    let first_seconds = declared.geometry(SYMBOL, &first)[0]
        .timeframe
        .duration_seconds();
    let last_seconds = declared.geometry(SYMBOL, &last)[0]
        .timeframe
        .duration_seconds();
    assert_ne!(first_seconds, last_seconds);

    let batch = run_batch(&plan(1), &declared, &events()).unwrap();
    let bound = batch.bound_document(0).unwrap();
    let encoded = toml::to_string(bound).unwrap();
    let decoded: qs_strategy::StrategyConfig = toml::from_str(&encoded).unwrap();
    assert_eq!(&decoded, bound);
}

#[test]
fn invalid_space_bindings_and_runtime_constraints_are_rejected() {
    let strategy = strategy_toml();
    let missing = space_toml().replace("\n[parameters.ema_slow]\nvalues = [8]\n", "\n");
    assert!(DeclaredSpace::from_toml(strategy, &missing).is_err());

    let bad_step = space_toml().replace("step = 1", "step = 0");
    assert!(DeclaredSpace::from_toml(strategy, &bad_step).is_err());

    let runtime_constraint = space_toml().replace(
        "left = { op = \"param\", id = \"ema_fast\" }",
        "left = { op = \"material\", id = \"fast\" }",
    );
    assert!(DeclaredSpace::from_toml(strategy, &runtime_constraint).is_err());
}
