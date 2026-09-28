mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use qs_backtest::Timeframe;
use qs_research::families::EmaCrossFamily;
use qs_research::{
    CheckpointDependency, CompletedRunCheckpoint, DataWindow, EndpointBounds, EvaluationRole,
    ExperimentId, ExperimentOptions, FeatureCache, FeatureCacheKey, FrozenSelection,
    ProtectedExperiment, ResearchAdmissionLimits, ResearchError, ResearchPlan, RunStatus,
    SearchCheckpoint, SplitAccessKind, SymbolEvents, WindowPlan, cached_market_midpoints,
    rerun_selected_candidate_protected, run_batch, run_batch_controlled_with_experiment_resume,
    run_batch_with_experiment,
};
use support::{SYMBOL, at, config, synthetic_ticks};

fn family() -> EmaCrossFamily {
    EmaCrossFamily::new(3..=4, 8..=9, vec![10, 20], Timeframe::minutes(1).unwrap())
        .with_atr_period(5)
}

fn windows() -> WindowPlan {
    WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
    }
}

fn events() -> BTreeMap<String, SymbolEvents> {
    BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(960))])
}

fn plan(workers: usize) -> ResearchPlan {
    ResearchPlan::new(vec![SYMBOL.to_owned()], windows(), config()).with_workers(workers)
}

#[test]
fn a_batch_evaluates_every_point_over_every_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();

    // Every point is evaluated once per window, and both windows of the pair are run.
    let points = qs_research::StrategyFamily::points(&family()).len();
    assert_eq!(points, 8);
    assert_eq!(table.len(), points * 2);
    for row in table.rows() {
        assert_eq!(row.points_total, points);
        assert_eq!(row.family_id, "ema_cross");
        assert_eq!(row.symbol, SYMBOL);
        assert_eq!(row.data_mode, "ticks");
        assert!(
            row.status.is_completed(),
            "run failed: {:?} for {:?}",
            row.status,
            row.params
        );
    }
}

#[test]
fn cancellation_retains_only_complete_runs_for_checkpoint_publication() {
    let completed = AtomicUsize::new(0);
    let outcome = run_batch_controlled_with_experiment_resume(
        &plan(1),
        &family(),
        &events(),
        ResearchAdmissionLimits::default(),
        ExperimentOptions::default(),
        &BTreeSet::new(),
        &|| completed.load(Ordering::Acquire) >= 1,
        &|progress| {
            completed.store(progress.completed_runs, Ordering::Release);
        },
    );
    let ResearchError::CancelledWithPartial(partial) = outcome.unwrap_err() else {
        panic!("cancellation after one run must retain a partial batch");
    };
    assert_eq!(partial.run_recipes().len(), 1);
    assert_eq!(partial.rows().len(), 1);
    assert_eq!(partial.run_recipes()[0].ordinal, 0);
    assert!(partial.run_recipes()[0].coverage.is_some());
}

#[test]
fn immutable_market_feature_cache_preserves_replay_economics_and_clone_isolation() {
    let events = events();
    let baseline = run_batch(&plan(1), &family(), &events).unwrap();
    let source = &events[SYMBOL];
    let key = FeatureCacheKey {
        dataset_reference: "synthetic-minute-ticks".into(),
        symbol: SYMBOL.into(),
        from: at(0),
        to: at(960),
        source: "primary".into(),
        price_basis: "mid".into(),
        alignment_offset_seconds: 0,
        output: "midpoint".into(),
        parameters: BTreeMap::new(),
        seed_policy: "none".into(),
        missing_policy: "explicit".into(),
        clock: "event_availability".into(),
        availability_policy: "actual".into(),
    };
    let bytes = FeatureCache::entry_bytes_upper_bound(&key, source.len()).unwrap();
    let mut cache = FeatureCache::new(bytes).unwrap();
    let mut first = cached_market_midpoints(&mut cache, key.clone(), source).unwrap();
    let second = cached_market_midpoints(&mut cache, key, source).unwrap();
    assert_eq!(first, second);
    first[0] = None;
    assert_ne!(first, second);
    assert_eq!(cache.len(), 1);

    let cached = run_batch(&plan(1), &family(), &events).unwrap();
    assert_eq!(cached.table(), baseline.table());
    assert_eq!(cached.position_outcomes(), baseline.position_outcomes());
}

#[test]
fn a_batch_produces_the_same_table_sequentially_and_in_parallel() {
    let events = events();
    let sequential = run_batch(&plan(1), &family(), &events).unwrap();
    let parallel = run_batch(&plan(4), &family(), &events).unwrap();
    assert_eq!(sequential, parallel);
    assert_eq!(sequential.to_csv(), parallel.to_csv());
}

#[test]
fn effective_recipes_are_deduplicated_and_worker_stable() {
    let events = events();
    let options = ExperimentOptions {
        experiment_id: Some(ExperimentId::new("recipe-fixture").unwrap()),
        caller_revision: Some("test-revision".into()),
        dataset_reference: Some("synthetic-minute-ticks".into()),
    };
    let sequential = run_batch_with_experiment(
        &plan(1),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        options.clone(),
    )
    .unwrap();
    let parallel = run_batch_with_experiment(
        &plan(4),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        options,
    )
    .unwrap();

    assert_eq!(sequential.experiment_recipe(), parallel.experiment_recipe());
    assert_eq!(sequential.candidate_recipes(), parallel.candidate_recipes());
    assert_eq!(sequential.run_recipes(), parallel.run_recipes());
    assert_eq!(
        sequential.experiment_recipe().endpoint_bounds,
        EndpointBounds::HalfOpen
    );
    assert_eq!(
        sequential.experiment_recipe().backtest["close_on_finish"],
        true
    );
    assert!(
        sequential
            .experiment_recipe()
            .research_retention
            .is_object()
    );
    assert!(
        sequential
            .run_recipes()
            .iter()
            .all(|recipe| recipe.run_tags["window"] == recipe.window)
    );
    assert_eq!(sequential.candidate_recipes().len(), 8);
    assert_eq!(sequential.run_recipes().len(), 16);
    assert!(
        sequential
            .candidate_recipes()
            .iter()
            .all(|recipe| recipe.series_by_symbol.contains_key(SYMBOL))
    );
    assert!(
        sequential
            .run_recipes()
            .iter()
            .all(|recipe| recipe.coverage.is_some())
    );

    let encoded = serde_json::to_string(sequential.candidate_recipes()).unwrap();
    let decoded: Vec<qs_research::CandidateRecipe> = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, sequential.candidate_recipes());
    let encoded = serde_json::to_string(sequential.run_recipes()).unwrap();
    let decoded: Vec<qs_research::RunRecipe> = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, sequential.run_recipes());
}

#[test]
fn downloaded_configured_recipe_reruns_with_frozen_settings_and_rejects_tuning() {
    let events = events();
    let options = ExperimentOptions {
        experiment_id: Some(ExperimentId::new("rerun-fixture").unwrap()),
        caller_revision: Some("r1".into()),
        dataset_reference: Some("synthetic".into()),
    };
    let original = run_batch_with_experiment(
        &plan(1),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        options,
    )
    .unwrap();
    let candidate = &original.candidate_recipes()[0];
    let run = original
        .run_recipes()
        .iter()
        .find(|run| run.candidate_ordinal == candidate.ordinal)
        .unwrap();
    let rerun = qs_research::rerun_selected_candidate(
        &plan(1),
        original.experiment_recipe(),
        candidate,
        run,
        &events,
    )
    .unwrap();
    let expected = original
        .rows()
        .iter()
        .find(|row| {
            row.window == run.window
                && row.symbol == run.symbol
                && row.params == rerun.rows()[0].params
        })
        .unwrap();
    assert_eq!(&rerun.rows()[0], expected);

    let final_window = DataWindow::new(run.window.clone(), run.from, run.to).unwrap();
    let mut protected = ProtectedExperiment::default();
    protected
        .freeze(FrozenSelection {
            candidate: candidate.clone(),
            experiment: original.experiment_recipe().clone(),
            caller_revision: "r1".into(),
            future_horizon_millis: Some(1),
            embargo_millis: Some(1),
        })
        .unwrap();
    assert!(
        rerun_selected_candidate_protected(
            &plan(1),
            original.experiment_recipe(),
            candidate,
            run,
            &events,
            &mut protected,
            EvaluationRole::Final,
            "r1",
        )
        .is_err(),
        "final data must not open before release"
    );
    protected.release_final_for(&final_window, "r1").unwrap();
    let protected_rerun = rerun_selected_candidate_protected(
        &plan(1),
        original.experiment_recipe(),
        candidate,
        run,
        &events,
        &mut protected,
        EvaluationRole::Final,
        "r1",
    )
    .unwrap();
    assert_eq!(protected_rerun.table(), rerun.table());
    assert_eq!(protected.records()[0].kind, SplitAccessKind::Release);
    assert_eq!(protected.records()[1].kind, SplitAccessKind::Access);

    let committed_run = original
        .run_recipes()
        .iter()
        .find(|candidate_run| {
            candidate_run.candidate_ordinal == candidate.ordinal
                && candidate_run.ordinal != run.ordinal
        })
        .unwrap();
    let committed_row = original
        .rows()
        .iter()
        .find(|row| {
            row.window == committed_run.window
                && row.symbol == committed_run.symbol
                && row.params == expected.params
        })
        .unwrap()
        .clone();
    let checkpoint = SearchCheckpoint {
        dependency: CheckpointDependency {
            experiment_id: Some("rerun-fixture".into()),
            caller_revision: "r1".into(),
            dataset_reference: "synthetic".into(),
            factory_revision: None,
        },
        experiment_recipe: Some(original.experiment_recipe().clone()),
        candidate_recipes: original
            .candidate_recipes()
            .iter()
            .cloned()
            .map(|candidate| (candidate.ordinal, candidate))
            .collect(),
        frozen_selection: protected.frozen().cloned(),
        frontier: u64::try_from(original.candidate_recipes().len()).unwrap(),
        completed_candidates: BTreeSet::new(),
        completed_runs: BTreeSet::from([committed_run.ordinal]),
        committed_runs: BTreeMap::from([(
            committed_run.ordinal,
            CompletedRunCheckpoint {
                recipe: committed_run.clone(),
                row: committed_row,
                positions: vec![],
            },
        )]),
        failures: BTreeMap::new(),
        generated: u64::try_from(original.candidate_recipes().len()).unwrap(),
        executed: 1,
        generation_exhaustive: true,
        split_access: protected.records().to_vec(),
    };
    let recombined = rerun.merge_checkpoint(&checkpoint).unwrap();
    assert_eq!(recombined.rows().len(), 2);
    assert_eq!(recombined.run_recipes().len(), 2);

    let mut tuned = plan(1);
    tuned.future.slippage_pips = 1.0;
    assert!(
        qs_research::rerun_selected_candidate(
            &tuned,
            original.experiment_recipe(),
            candidate,
            run,
            &events
        )
        .is_err()
    );
}

#[test]
fn coverage_is_recorded_only_after_successful_replay() {
    let completed = run_batch(&plan(1), &family(), &events()).unwrap();
    for recipe in completed.run_recipes() {
        let coverage = recipe.coverage.as_ref().unwrap();
        assert!(coverage.processed_primary_events > 0);
        assert!(coverage.first_input_at <= coverage.last_input_at);
        assert_eq!(coverage.unavailable.len(), 2);
        assert!(coverage.first_ready_at.is_some());
    }

    let broken = EmaCrossFamily::new(0..=0, 8..=8, vec![10], Timeframe::minutes(1).unwrap());
    let failed = run_batch(&plan(1), &broken, &events()).unwrap();
    assert!(
        failed
            .run_recipes()
            .iter()
            .all(|recipe| recipe.coverage.is_none())
    );
    assert!(failed.candidate_recipes().iter().all(|recipe| {
        recipe
            .admission_error
            .as_deref()
            .is_some_and(|error| error.starts_with("compile:"))
    }));
}

#[test]
fn a_run_never_opens_a_position_before_its_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();
    for row in table.rows() {
        assert_eq!(
            row.entries_before_window, 0,
            "warmup should make an entry before the window impossible, {:?}",
            row.params
        );
    }
}

#[test]
fn a_missing_symbol_fails_its_rows_without_aborting_the_batch() {
    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned(), "GBPUSD".to_owned()],
        windows(),
        config(),
    );
    let table = run_batch(&plan, &family(), &events()).unwrap();

    let completed = table
        .rows()
        .iter()
        .filter(|row| row.status.is_completed())
        .count();
    let failed: Vec<&qs_research::ResearchRow> = table
        .rows()
        .iter()
        .filter(|row| !row.status.is_completed())
        .collect();

    assert!(completed > 0);
    assert_eq!(failed.len(), completed);
    for row in failed {
        assert_eq!(row.symbol, "GBPUSD");
        assert!(matches!(
            &row.status,
            RunStatus::Failed { kind, message }
                if kind == "replay" && message.contains("GBPUSD")
        ));
    }
}

#[test]
fn every_row_carries_its_parameters_and_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();
    for row in table.rows() {
        assert!(row.params.contains_key("ema_fast"));
        assert!(row.params.contains_key("ema_slow"));
        assert!(row.params.contains_key("atr_stop"));
        assert!(row.params.contains_key("entry"));
        assert!(row.window == "is" || row.window == "oos");
    }

    let pairs = table.paired("is", "oos");
    assert_eq!(
        pairs.len(),
        qs_research::StrategyFamily::points(&family()).len()
    );
    for pair in pairs {
        assert_eq!(pair.in_sample.params, pair.out_of_sample.params);
        assert_eq!(pair.in_sample.window, "is");
        assert_eq!(pair.out_of_sample.window, "oos");
    }
}

#[test]
fn a_family_whose_document_does_not_compile_fails_only_its_own_rows() {
    // A zero-length average is rejected by the strategy compiler, which is what a caller's broken
    // generator looks like from the batch's side.
    let broken = EmaCrossFamily::new(0..=0, 8..=8, vec![10], Timeframe::minutes(1).unwrap());
    let table = run_batch(&plan(1), &broken, &events()).unwrap();

    assert!(!table.is_empty());
    for row in table.rows() {
        assert!(matches!(
            &row.status,
            RunStatus::Failed { kind, .. } if kind == "compile"
        ));
        assert_eq!(row.positions, 0);
    }
}

#[test]
fn an_empty_plan_or_family_is_rejected_before_any_run() {
    let empty_symbols = ResearchPlan::new(Vec::new(), windows(), config());
    assert!(run_batch(&empty_symbols, &family(), &events()).is_err());

    let duplicate = ResearchPlan::new(
        vec![SYMBOL.to_owned(), SYMBOL.to_owned()],
        windows(),
        config(),
    );
    assert!(run_batch(&duplicate, &family(), &events()).is_err());

    let no_points = EmaCrossFamily::new(9..=9, 3..=3, vec![10], Timeframe::minutes(1).unwrap());
    assert!(run_batch(&plan(1), &no_points, &events()).is_err());
}

#[test]
fn a_walk_forward_plan_runs_every_split() {
    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::RollingWalkForward {
            start: at(100),
            end: at(900),
            train: chrono::Duration::minutes(300),
            test: chrono::Duration::minutes(200),
            step: chrono::Duration::minutes(200),
        },
        config(),
    );
    let table = run_batch(&plan, &family(), &events()).unwrap();

    let windows: std::collections::BTreeSet<&str> =
        table.rows().iter().map(|row| row.window.as_str()).collect();
    assert_eq!(
        windows,
        ["is0", "is1", "oos0", "oos1"].into_iter().collect()
    );
    assert_eq!(table.paired("is0", "oos0").len(), 8);
    assert!(table.rows().iter().all(|row| row.status.is_completed()));
}

#[test]
fn a_family_that_labels_two_points_alike_is_rejected() {
    // Rows are ordered by their labels, so indistinguishable points would make the output order
    // depend on which worker finished first.
    struct Ambiguous(EmaCrossFamily);

    impl qs_research::StrategyFamily for Ambiguous {
        type Params = <EmaCrossFamily as qs_research::StrategyFamily>::Params;

        fn family_id(&self) -> &str {
            self.0.family_id()
        }
        fn points(&self) -> Vec<Self::Params> {
            self.0.points()
        }
        fn config(&self, point: &Self::Params) -> qs_strategy::StrategyConfig {
            self.0.config(point)
        }
        fn parameter_binding(&self, _point: &Self::Params) -> qs_strategy::ParameterBinding {
            qs_strategy::ParameterBinding::new([(
                "fixed",
                qs_strategy::ParameterValue::Choice("same".into()),
            )])
        }
        fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<qs_research::SeriesGeometry> {
            self.0.geometry(symbol, point)
        }
    }

    let error = run_batch(&plan(1), &Ambiguous(family()), &events()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("binds two parameter points identically")
    );
}

#[test]
fn unordered_events_are_rejected_before_any_run() {
    let mut events = events();
    let reversed: Vec<_> = events[SYMBOL].iter().rev().cloned().collect();
    events.insert(SYMBOL.to_owned(), reversed.into());

    let error = run_batch(&plan(1), &family(), &events).unwrap_err();
    assert!(error.to_string().contains("ascending timestamp order"));
}
