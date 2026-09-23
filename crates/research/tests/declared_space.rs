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

#[test]
fn classified_entry_fails_every_run_because_a_batch_supplies_no_profile_route() {
    let classified = strategy_toml().replacen(
        "action = \"entry\"\n",
        "action = \"entry\"\nentry_class = \"trend\"\n",
        1,
    );
    assert_ne!(classified, strategy_toml());
    let declared = DeclaredSpace::from_toml(&classified, space_toml()).unwrap();
    let batch = run_batch(&plan(1), &declared, &events()).unwrap();
    assert!(!batch.table().rows().is_empty());
    for row in batch.table().rows() {
        match &row.status {
            qs_research::RunStatus::Failed { message, .. } => assert!(
                message.contains("entry class `trend`"),
                "unexpected failure: {message}"
            ),
            qs_research::RunStatus::Completed => panic!("a classified entry must not run unrouted"),
        }
    }
}

#[test]
fn declared_space_runs_over_stored_bars_and_records_the_bar_mode() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    let bars = BTreeMap::from([(SYMBOL.to_owned(), support::synthetic_bars(960))]);
    let batch = run_batch(&plan(2), &declared, &bars).unwrap();
    let rows = batch.table().rows();
    assert_eq!(rows.len(), 24);
    assert!(rows.iter().all(|row| row.status.is_completed()));
    assert!(rows.iter().all(|row| row.data_mode == "bars"));
    assert!(rows.iter().any(|row| row.positions > 0));
    assert!(batch.position_outcomes().iter().all(|position| {
        position
            .dimensions
            .tags
            .get("data_mode")
            .map(String::as_str)
            == Some("bars")
    }));

    let ticks = run_batch(&plan(2), &declared, &events()).unwrap();
    assert!(
        ticks
            .table()
            .rows()
            .iter()
            .all(|row| row.data_mode == "ticks")
    );
}

#[test]
fn a_symbol_mixing_ticks_and_stored_bars_is_rejected() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    let mut mixed = support::synthetic_ticks(10).to_vec();
    mixed.extend(support::synthetic_bars(10).iter().cloned());
    mixed.sort_by_key(|event| event.event.ts());
    let events = BTreeMap::from([(SYMBOL.to_owned(), mixed.into())]);
    let error = run_batch(&plan(1), &declared, &events).err().unwrap();
    assert!(
        error.to_string().contains("mix ticks and stored bars"),
        "{error}"
    );
}

#[test]
fn stored_bars_load_from_a_store_and_drive_a_batch() {
    let data_dir = std::env::temp_dir().join(format!(
        "qs-research-stored-bars-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let store = data_preprocess::ParquetStore::open(&data_dir).unwrap();
    store
        .insert_bars(&support::synthetic_stored_bars(960))
        .unwrap();

    let events = qs_research::load_symbol_bars(
        data_dir.to_str().unwrap(),
        "demo",
        SYMBOL,
        "1m",
        Some(at(0)),
        Some(at(960)),
    )
    .unwrap();
    std::fs::remove_dir_all(&data_dir).unwrap();
    assert!(!events.is_empty());
    assert!(events.iter().all(|event| matches!(
        event.event,
        qs_backtest::data_feed::MarketEvent::Bar {
            timeframe_seconds: Some(60),
            tick_count: Some(_),
            ..
        }
    )));

    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    let from_store = run_batch(
        &plan(1),
        &declared,
        &BTreeMap::from([(SYMBOL.to_owned(), events)]),
    )
    .unwrap();
    let in_memory = run_batch(
        &plan(1),
        &declared,
        &BTreeMap::from([(SYMBOL.to_owned(), support::synthetic_bars(960))]),
    )
    .unwrap();
    assert_eq!(from_store.table(), in_memory.table());
}

#[test]
fn a_controlled_batch_reports_each_run_and_stops_when_cancelled() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    let reported = AtomicUsize::new(0);
    let total = AtomicUsize::new(0);
    let batch =
        qs_research::run_batch_controlled(&plan(1), &declared, &events(), &|| false, &|progress| {
            assert_eq!(
                progress.completed_runs,
                reported.fetch_add(1, Ordering::SeqCst) + 1
            );
            total.store(progress.total_runs, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(reported.load(Ordering::SeqCst), batch.table().len());
    assert_eq!(total.load(Ordering::SeqCst), batch.table().len());

    for workers in [1, 3] {
        let completed = AtomicUsize::new(0);
        let error = qs_research::run_batch_controlled(
            &plan(workers),
            &declared,
            &events(),
            &|| completed.load(Ordering::SeqCst) >= 2,
            &|_| {
                completed.fetch_add(1, Ordering::SeqCst);
            },
        )
        .err()
        .unwrap();
        assert!(matches!(error, qs_research::ResearchError::Cancelled));
        assert!(completed.load(Ordering::SeqCst) < 12);
    }
}

#[test]
fn documents_from_any_serde_source_build_the_same_space() {
    let template: StrategyConfig = toml::from_str(strategy_toml()).unwrap();
    let space: toml::Value = toml::from_str(space_toml()).unwrap();
    let from_documents = DeclaredSpace::from_documents(template, space).unwrap();
    let from_toml = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    assert_eq!(
        run_batch(&plan(1), &from_documents, &events())
            .unwrap()
            .table(),
        run_batch(&plan(1), &from_toml, &events()).unwrap().table()
    );

    let mut unknown: toml::Value = toml::from_str(space_toml()).unwrap();
    unknown["parameters"].as_table_mut().unwrap().insert(
        "unknown".into(),
        toml::Value::try_from(BTreeMap::from([("values", vec![1])])).unwrap(),
    );
    let error = DeclaredSpace::from_documents(toml::from_str(strategy_toml()).unwrap(), unknown)
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("unknown parameter 'unknown'"),
        "{error}"
    );
}

#[test]
fn validation_and_data_range_need_no_market_data() {
    let declared = DeclaredSpace::from_toml(strategy_toml(), space_toml()).unwrap();
    assert_eq!(
        qs_research::validate_batch(&plan(1), &declared).unwrap(),
        24
    );
    let (start, end) = qs_research::batch_data_range(&plan(1), &declared)
        .unwrap()
        .unwrap();
    assert_eq!(end, at(900));
    assert!(
        start < at(200),
        "the range includes the warmup before the first window"
    );

    let sliced: Vec<_> = support::synthetic_ticks(960)
        .iter()
        .filter(|event| event.event.ts() >= start && event.event.ts() <= end)
        .cloned()
        .collect();
    let narrow = BTreeMap::from([(SYMBOL.to_owned(), sliced.into())]);
    assert_eq!(
        run_batch(&plan(1), &declared, &narrow).unwrap().table(),
        run_batch(&plan(1), &declared, &events()).unwrap().table(),
        "loading exactly the data range changes nothing"
    );
}

#[test]
fn a_plan_with_profiles_routes_classified_entries_in_every_run() {
    use qs_backtest::profile::{
        ManagementProfile, PreparedEntryProfiles, RuleConfigDef, StoplossMode,
    };

    let classified = strategy_toml().replacen(
        "action = \"entry\"\n",
        "action = \"entry\"\nentry_class = \"trend\"\n",
        1,
    );
    let declared = DeclaredSpace::from_toml(&classified, space_toml()).unwrap();
    let trailing = ManagementProfile {
        name: "trail".into(),
        target_selection: None,
        use_targets: vec![],
        close_ratios: vec![],
        target_source: qs_backtest::TargetSource::FromSignal,
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![RuleConfigDef::TrailingStop { distance: 0.0005 }],
        group_override: None,
        let_remainder_run: false,
        entry_geometry: qs_backtest::EntryGeometryPolicy::Strict,
    };
    let plan = plan(1).with_entry_profiles(
        PreparedEntryProfiles::try_new(None, [("trend".to_owned(), trailing)]).unwrap(),
    );
    let batch = run_batch(&plan, &declared, &events()).unwrap();
    assert!(
        batch
            .table()
            .rows()
            .iter()
            .all(|row| row.status.is_completed())
    );
    assert!(batch.table().rows().iter().any(|row| row.positions > 0));
}
