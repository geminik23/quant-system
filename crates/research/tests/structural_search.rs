use chrono::{Duration, NaiveDate};
use qs_backtest::Timeframe;
use qs_research::families::EmaCrossFamily;
use qs_research::*;
use qs_strategy::*;
use support::{SYMBOL, at, config, synthetic_ticks};
mod support;

use std::collections::BTreeMap;
fn source() -> SourceId {
    SourceId::new("bars").unwrap()
}

#[derive(Clone)]
struct ManualFamily {
    document: StrategyConfig,
    geometry: Vec<SeriesGeometry>,
    label: String,
}

impl StrategyFamily for ManualFamily {
    type Params = ();

    fn family_id(&self) -> &str {
        "generated_economics"
    }

    fn points(&self) -> Vec<Self::Params> {
        vec![()]
    }

    fn parameter_binding(&self, _: &Self::Params) -> ParameterBinding {
        ParameterBinding::new([("structure", ParameterValue::Choice(self.label.clone()))])
    }

    fn config(&self, _: &Self::Params) -> StrategyConfig {
        self.document.clone()
    }

    fn geometry(&self, _: &str, _: &Self::Params) -> Vec<SeriesGeometry> {
        self.geometry.clone()
    }
}
fn base() -> StrategyConfig {
    StrategyConfig {
        strategy_id: "generated".into(),
        title: "Generated neutral strategy".into(),
        parameters: vec![],
        initial_state: "idle".into(),
        sources: vec![source()],
        trade_slots: vec![],
        materials: vec![],
        variables: vec![],
        states: vec![
            StateConfig {
                id: "idle".into(),
                transitions: vec![TransitionConfig {
                    priority: 1,
                    target: "done".into(),
                    when: Expr::Literal {
                        value: Literal::Bool(false),
                    },
                    assignments: vec![],
                    decision: None,
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
fn atom(id: &str, key: &str, threshold: f64) -> PredicateAtom {
    PredicateAtom {
        id: id.into(),
        material: MaterialConfig {
            id: format!("feature_{id}"),
            key: key.into(),
            inputs: vec![],
            params: MaterialArgs::new([("source", MaterialArg::Source(source()))]),
        },
        comparison: StructuralComparison::Gt,
        threshold: Literal::Ratio(threshold),
    }
}
fn spec(limit: usize) -> StructuralSearchSpec {
    let mut limits = StructuralResourceLimits::small_test();
    limits.max_candidates = limit;
    limits.max_depth = 2;
    StructuralSearchSpec {
        family_id: "tiny".into(),
        base_document: base(),
        state_id: "idle".into(),
        transition_priority: 1,
        atoms: vec![
            atom("body", MATERIAL_BODY_FRACTION, 0.4),
            atom("close", MATERIAL_CLOSE_POSITION, 0.5),
        ],
        operators: StructuralOperators {
            not: true,
            and: true,
            or: true,
            sequence: true,
        },
        sequence_source: source(),
        sequence_max_gap: 3,
        captures: vec![],
        geometry_by_symbol: BTreeMap::new(),
        limits,
    }
}
#[test]
fn tiny_breadth_first_universe_has_exact_stable_documents_and_identities() {
    let first = spec(32).generate().unwrap();
    let second = spec(32).generate().unwrap();
    assert_eq!(first.completion, GenerationCompletion::Exhaustive);
    assert_eq!(
        first
            .candidates
            .iter()
            .map(|c| c.label.as_str())
            .collect::<Vec<_>>(),
        vec![
            "body",
            "close",
            "not(body)",
            "not(close)",
            "and(body,close)",
            "or(body,close)",
            "sequence(body,close)",
            "sequence(close,body)"
        ]
    );
    assert_eq!(first.candidates, second.candidates);
    assert_eq!(
        first
            .candidates
            .iter()
            .map(|c| c.ordinal)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    for candidate in first.candidates {
        ConfiguredStrategy::compile(
            candidate.document,
            &MaterialLibrary::builtins(),
            "generated",
            "EURUSD",
        )
        .unwrap();
    }
}

#[test]
fn breadth_first_generation_recomposes_prior_levels_to_the_declared_depth() {
    let mut search = spec(128);
    search.limits.max_depth = 3;
    search.limits.max_candidates = 128;
    search.limits.max_queue_bytes = 8 << 20;
    let generation = search.generate().unwrap();
    assert!(
        generation
            .candidates
            .iter()
            .any(|candidate| candidate.label == "not(not(body))" && candidate.node_count == 3)
    );
    let depth_two_end = generation
        .candidates
        .iter()
        .position(|candidate| candidate.label == "sequence(close,body)")
        .unwrap();
    let depth_three_start = generation
        .candidates
        .iter()
        .position(|candidate| candidate.label == "not(not(body))")
        .unwrap();
    assert!(depth_three_start > depth_two_end);
}

#[test]
fn exact_duplicates_are_counted_and_no_unproved_economic_pruning_occurs() {
    let mut search = spec(32);
    search.operators = StructuralOperators {
        not: false,
        and: true,
        or: false,
        sequence: false,
    };
    let mut duplicate = search.atoms[0].clone();
    duplicate.id = "body_alias".into();
    search.atoms.push(duplicate);
    let generation = search.generate().unwrap();
    assert!(generation.dispositions.duplicate >= 1);
    assert_eq!(generation.dispositions.sound_pruned, 0);
    assert_eq!(generation.dispositions.heuristic_pruned, 0);
    assert!(
        generation
            .candidates
            .iter()
            .any(|candidate| candidate.label == "and(body,close)")
    );
}

#[test]
fn exact_aliases_and_matching_threshold_implications_canonicalize_without_float_rewrites() {
    let source = source();
    let close_material = MaterialConfig {
        id: "named_close".into(),
        key: MATERIAL_CLOSE_PRICE.into(),
        inputs: vec![],
        params: MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]),
    };
    let legacy_close = MaterialConfig {
        id: "legacy_close".into(),
        key: MATERIAL_BAR_FIELD.into(),
        inputs: vec![],
        params: MaterialArgs::new([
            ("source", MaterialArg::Source(source.clone())),
            ("field", MaterialArg::BarField(BarField::Close)),
        ]),
    };
    let body = MaterialConfig {
        id: "body".into(),
        key: MATERIAL_BODY_FRACTION.into(),
        inputs: vec![],
        params: MaterialArgs::new([("source", MaterialArg::Source(source))]),
    };
    let mut search = spec(32);
    search.atoms = vec![
        PredicateAtom {
            id: "close_named".into(),
            material: close_material,
            comparison: StructuralComparison::Gt,
            threshold: Literal::Price(100.0),
        },
        PredicateAtom {
            id: "close_legacy".into(),
            material: legacy_close,
            comparison: StructuralComparison::Gt,
            threshold: Literal::Price(100.0),
        },
        PredicateAtom {
            id: "body_loose".into(),
            material: body.clone(),
            comparison: StructuralComparison::Gt,
            threshold: Literal::Ratio(0.2),
        },
        PredicateAtom {
            id: "body_strict".into(),
            material: body,
            comparison: StructuralComparison::Gt,
            threshold: Literal::Ratio(0.4),
        },
    ];
    search.operators = StructuralOperators {
        not: false,
        and: true,
        or: false,
        sequence: false,
    };
    let generation = search.generate().unwrap();
    assert!(
        generation.dispositions.duplicate >= 2,
        "dispositions: {:?}; labels: {:?}",
        generation.dispositions,
        generation
            .candidates
            .iter()
            .map(|candidate| candidate.label.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        generation
            .candidates
            .iter()
            .filter(|candidate| candidate.label.starts_with("close_"))
            .count(),
        1
    );
    assert!(
        generation
            .candidates
            .iter()
            .all(|candidate| candidate.label != "and(body_loose,body_strict)")
    );
}

#[test]
fn generation_budgets_stop_before_expansion_and_report_incomplete_frontier() {
    let generation = spec(3).generate().unwrap();
    assert_eq!(
        generation.completion,
        GenerationCompletion::IncompleteBudget
    );
    assert_eq!(generation.candidates.len(), 3);
    assert!(generation.dispositions.unvisited > 0);
    let mut bytes = spec(32);
    bytes.limits.max_queue_bytes = 1;
    let bytes = bytes.generate().unwrap();
    assert_eq!(bytes.completion, GenerationCompletion::IncompleteBudget);
    assert!(bytes.candidates.is_empty());
    let mut zero = StructuralResourceLimits::small_test();
    zero.max_nodes = 0;
    assert!(zero.validate().is_err());
}
#[test]
fn capture_and_sequence_lower_into_canonical_temporal_materials() {
    let mut search = spec(32);
    search.captures.push(CaptureCandidate {
        id: "delayed_retest".into(),
        material_key: MATERIAL_SETUP_LONG_BOUNDED_QUEUE.into(),
        source: source(),
        expiry: 3,
        capacity: Some(2),
        reset: Expr::Literal {
            value: Literal::Bool(false),
        },
        gap: Expr::Input {
            field: "source_gap_before".into(),
            value_type: ValueType::optional(ScalarType::Bool),
        },
        breakout: Expr::Literal {
            value: Literal::Bool(true),
        },
        retest: Expr::Literal {
            value: Literal::Bool(true),
        },
        level: Expr::Literal {
            value: Literal::Price(10.0),
        },
        normalization: Expr::Literal {
            value: Literal::Number(2.0),
        },
        close: Expr::Literal {
            value: Literal::Price(10.0),
        },
        tolerance: Expr::Literal {
            value: Literal::Price(0.1),
        },
        ordinal: Expr::Input {
            field: "source_ordinal".into(),
            value_type: ValueType::optional(ScalarType::Integer),
        },
    });
    search.geometry_by_symbol.insert(
        SYMBOL.into(),
        vec![SeriesGeometry::new(
            source(),
            SYMBOL,
            Timeframe::minutes(1).unwrap(),
            qs_backtest::PriceBasis::Bid,
            0,
        )],
    );
    let generation = search.generate().unwrap();
    let capture = generation
        .candidates
        .iter()
        .find(|c| c.label == "delayed_retest")
        .unwrap();
    assert!(
        capture
            .document
            .materials
            .iter()
            .any(|m| m.key == MATERIAL_SETUP_LONG_BOUNDED_QUEUE)
    );
    let (family, _) = GeneratedStructuralFamily::new(&search).unwrap();
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(400)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(400), at(600)).unwrap(),
    };
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(650))]);
    let batch = run_batch(
        &ResearchPlan::new(vec![SYMBOL.into()], windows, config()),
        &family,
        &events,
    )
    .unwrap();
    assert!(batch.rows().iter().all(|row| row.status.is_completed()));
}
fn candidate_recipe() -> CandidateRecipe {
    serde_json::from_value(serde_json::json!({
        "ordinal": 1, "family_id": "tiny", "parameters": {},
        "document": {"candidate":"one"}, "series_by_symbol": {}, "admission_error": null
    }))
    .unwrap()
}
fn experiment_recipe() -> ExperimentRecipe {
    serde_json::from_value(serde_json::json!({
        "experiment_id": null, "caller_revision": "r1", "dataset_reference": "synthetic",
        "endpoint_bounds": "half_open", "ordered_symbols": ["EURUSD"], "decision_latency_ms": 0,
        "portfolio": null, "backtest": {}, "future": {}, "evaluation": {}, "retention": {},
        "research_retention": {}, "profiles": null
    }))
    .unwrap()
}
fn window() -> DataWindow {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    DataWindow::new("final", start, start + Duration::days(10)).unwrap()
}
#[test]
fn generated_candidates_run_through_existing_replay_and_are_worker_stable() {
    let seed = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let point = seed.points().remove(0);
    let document = seed.config(&point);
    let source = document.sources[0].clone();
    let state_id = document.initial_state.clone();
    let priority = document
        .states
        .iter()
        .find(|state| state.id == state_id)
        .unwrap()
        .transitions[0]
        .priority;
    let make_atom = |id: &str, key: &str, threshold| PredicateAtom {
        id: id.into(),
        material: MaterialConfig {
            id: format!("generated_{id}"),
            key: key.into(),
            inputs: vec![],
            params: MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]),
        },
        comparison: StructuralComparison::Gt,
        threshold: Literal::Ratio(threshold),
    };
    let mut limits = StructuralResourceLimits::small_test();
    limits.max_candidates = 8;
    limits.max_depth = 2;
    let spec = StructuralSearchSpec {
        family_id: "generated_economics".into(),
        base_document: document,
        state_id,
        transition_priority: priority,
        atoms: vec![
            make_atom("body", MATERIAL_BODY_FRACTION, 0.05),
            make_atom("close", MATERIAL_CLOSE_POSITION, 0.45),
        ],
        operators: StructuralOperators {
            not: false,
            and: true,
            or: false,
            sequence: false,
        },
        sequence_source: source,
        sequence_max_gap: 2,
        captures: vec![],
        geometry_by_symbol: BTreeMap::from([(SYMBOL.into(), seed.geometry(SYMBOL, &point))]),
        limits,
    };
    let (family, generation) = GeneratedStructuralFamily::new(&spec).unwrap();
    assert_eq!(generation.candidates.len(), 3);
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let sequential = run_batch(
        &ResearchPlan::new(vec![SYMBOL.into()], windows.clone(), config()).with_workers(1),
        &family,
        &events,
    )
    .unwrap();
    let parallel = run_batch(
        &ResearchPlan::new(vec![SYMBOL.into()], windows, config()).with_workers(2),
        &family,
        &events,
    )
    .unwrap();
    assert_eq!(sequential.table(), parallel.table());
    let generated_and = generation
        .candidates
        .iter()
        .find(|candidate| candidate.label == "and(body,close)")
        .unwrap();
    let manual = ManualFamily {
        document: generated_and.document.clone(),
        geometry: seed.geometry(SYMBOL, &point),
        label: generated_and.label.clone(),
    };
    let manual_batch = run_batch(
        &ResearchPlan::new(
            vec![SYMBOL.into()],
            WindowPlan::Fixed {
                in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
                out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
            },
            config(),
        ),
        &manual,
        &events,
    )
    .unwrap();
    let generated_rows = sequential
        .rows()
        .iter()
        .filter(|row| row.params.get("structure") == Some(&generated_and.label))
        .collect::<Vec<_>>();
    assert_eq!(generated_rows.len(), manual_batch.rows().len());
    for (generated, manual) in generated_rows.iter().zip(manual_batch.rows()) {
        assert_eq!(generated.net_pnl, manual.net_pnl);
        assert_eq!(generated.positions, manual.positions);
        assert_eq!(generated.max_drawdown_pct, manual.max_drawdown_pct);
    }
    assert!(
        sequential
            .rows()
            .iter()
            .all(|row| row.status.is_completed())
    );
}

#[test]
fn neutral_report_scenarios_lower_to_canonical_candidates_and_execute() {
    let seed = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let point = seed.points().remove(0);
    let document = seed.config(&point);
    let source = document.sources[0].clone();
    let state_id = document.initial_state.clone();
    let priority = document
        .states
        .iter()
        .find(|state| state.id == state_id)
        .unwrap()
        .transitions[0]
        .priority;
    let atoms = vec![
        PredicateAtom {
            id: "trend_pullback_reclaim".into(),
            material: MaterialConfig {
                id: "trend_shape".into(),
                key: MATERIAL_BODY_DIRECTION_FRACTION.into(),
                inputs: vec![],
                params: MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]),
            },
            comparison: StructuralComparison::Gt,
            threshold: Literal::Ratio(0.0),
        },
        PredicateAtom {
            id: "squeeze_breakout_expansion".into(),
            material: MaterialConfig {
                id: "squeeze".into(),
                key: MATERIAL_BB_KC_SQUEEZE.into(),
                inputs: vec![],
                params: MaterialArgs::new([
                    ("source", MaterialArg::Source(source.clone())),
                    ("bb_period", MaterialArg::Integer(3)),
                    ("kc_period", MaterialArg::Integer(3)),
                    ("atr_period", MaterialArg::Integer(3)),
                ]),
            },
            comparison: StructuralComparison::Eq,
            threshold: Literal::Bool(true),
        },
        PredicateAtom {
            id: "failed_downside_breakout".into(),
            material: MaterialConfig {
                id: "down_gap".into(),
                key: MATERIAL_THREE_BAR_GAP_DOWN.into(),
                inputs: vec![],
                params: MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]),
            },
            comparison: StructuralComparison::Eq,
            threshold: Literal::Bool(true),
        },
    ];
    let spec = StructuralSearchSpec {
        family_id: "neutral_scenarios".into(),
        base_document: document,
        state_id,
        transition_priority: priority,
        atoms,
        operators: StructuralOperators {
            not: false,
            and: false,
            or: false,
            sequence: false,
        },
        sequence_source: source,
        sequence_max_gap: 2,
        captures: vec![],
        geometry_by_symbol: BTreeMap::from([(SYMBOL.into(), seed.geometry(SYMBOL, &point))]),
        limits: StructuralResourceLimits::small_test(),
    };
    let (family, generation) = GeneratedStructuralFamily::new(&spec).unwrap();
    assert_eq!(
        generation
            .candidates
            .iter()
            .map(|candidate| candidate.label.as_str())
            .collect::<Vec<_>>(),
        vec![
            "trend_pullback_reclaim",
            "squeeze_breakout_expansion",
            "failed_downside_breakout"
        ]
    );
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let plan = ResearchPlan::new(vec![SYMBOL.into()], windows, config());
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let batch = run_batch(&plan, &family, &events).unwrap();
    assert_eq!(batch.len(), 6);
    assert!(batch.rows().iter().all(|row| row.status.is_completed()));
}

#[test]
fn resume_reconstructs_the_same_deterministic_frontier_without_duplicate_candidates() {
    let dependency = CheckpointDependency {
        experiment_id: Some("exp".into()),
        caller_revision: "r1".into(),
        dataset_reference: "synthetic".into(),
        factory_revision: None,
    };
    let checkpoint = SearchCheckpoint {
        dependency: dependency.clone(),
        experiment_recipe: None,
        candidate_recipes: BTreeMap::new(),
        frozen_selection: None,
        frontier: 8,
        completed_candidates: std::collections::BTreeSet::from([0, 1]),
        completed_runs: std::collections::BTreeSet::new(),
        committed_runs: BTreeMap::new(),
        failures: BTreeMap::new(),
        generated: 8,
        executed: 2,
        generation_exhaustive: true,
        split_access: vec![],
    };
    let resumed = resume_structural_generation(
        &spec(32),
        &checkpoint,
        &dependency,
        CheckpointLimits::new(4096, 16).unwrap(),
    )
    .unwrap();
    assert_eq!(
        resumed
            .remaining
            .iter()
            .map(|candidate| candidate.ordinal)
            .collect::<Vec<_>>(),
        vec![2, 3, 4, 5, 6, 7]
    );
    assert!(
        resumed
            .remaining
            .iter()
            .all(|candidate| !resumed.completed_runs.contains(&candidate.ordinal))
    );
}

#[test]
fn protected_final_requires_freeze_release_horizon_and_records_post_test_tuning() {
    let mut protected = ProtectedExperiment::default();
    assert!(
        protected
            .access(EvaluationRole::Final, &window(), "r1", false)
            .is_err()
    );
    protected
        .freeze(FrozenSelection {
            candidate: candidate_recipe(),
            experiment: experiment_recipe(),
            caller_revision: "r1".into(),
            future_horizon_millis: Some(86_400_000),
            embargo_millis: Some(86_400_000),
        })
        .unwrap();
    assert!(
        protected
            .access(EvaluationRole::Final, &window(), "r1", false)
            .is_err()
    );
    protected
        .access(EvaluationRole::Search, &window(), "r1", false)
        .unwrap();
    protected.release_final_for(&window(), "r1").unwrap();
    protected
        .access(EvaluationRole::Final, &window(), "r1", false)
        .unwrap();
    protected
        .access(EvaluationRole::Final, &window(), "r2", true)
        .unwrap();
    assert_eq!(protected.records()[1].kind, SplitAccessKind::Release);
    assert_eq!(protected.records()[2].kind, SplitAccessKind::Access);
    assert!(!protected.records()[2].post_test_tuning);
    assert!(protected.records()[3].post_test_tuning);
    assert_eq!(
        strict_embargo(&window(), Duration::days(1)).unwrap().to()
            - strict_embargo(&window(), Duration::days(1)).unwrap().from(),
        Duration::days(9)
    );
    let mut unknown = ProtectedExperiment::default();
    unknown
        .freeze(FrozenSelection {
            candidate: candidate_recipe(),
            experiment: experiment_recipe(),
            caller_revision: "r1".into(),
            future_horizon_millis: None,
            embargo_millis: None,
        })
        .unwrap();
    unknown.release_final().unwrap();
    assert!(
        unknown
            .access(EvaluationRole::Final, &window(), "r1", false)
            .is_err()
    );
}
