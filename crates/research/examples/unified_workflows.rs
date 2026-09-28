//! Runnable neutral evidence for the unified research workflows.
//!
//! Run every workflow with:
//! `cargo run -p qs-research --example unified_workflows -- all`
//!
//! Individual modes are `bar-inputs`, `direct`, `variants`, `mixed`, `temporal`, and
//! `resume-final`. The data is deterministic synthetic input and carries no economic claim.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BarSeriesSpec, FutureQuoteConfig, HistoricalStrategy,
    MissingIntervalPolicy, ObservationStoreLimits, PriceBasis, SeriesId, SeriesRequirement,
    StrategyContext, StrategyDescriptor, StrategyEvent, StrategyId, StrategyOutput,
    StrategyRequirements, Timeframe, WarmupRequirement,
};
use qs_research::families::EmaCrossFamily;
use qs_research::*;
use qs_strategy::{
    Expr, Literal, MATERIAL_BODY_FRACTION, MATERIAL_SETUP_LONG_BOUNDED_QUEUE, MaterialArg,
    MaterialArgs, MaterialConfig, ParameterBinding, ParameterValue, ScalarType, SourceId,
    StateConfig, StrategyConfig, TransitionConfig, ValueType,
};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "EURUSD";
const POINT: f64 = 1.0e-5;
type WorkflowResult = Result<(), Box<dyn std::error::Error>>;
type Workflow = (&'static str, fn() -> WorkflowResult);

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 5)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn at(minute: i64) -> NaiveDateTime {
    base() + Duration::minutes(minute)
}

fn ticks(minutes: i64) -> SymbolEvents {
    (0..minutes)
        .map(|minute| {
            let phase = (minute % 120) as f64 / 120.0 * std::f64::consts::TAU;
            let bid = 1.1 + phase.sin() * 80.0 * POINT;
            FeedEvent::new(
                MarketEvent::Tick {
                    symbol: SYMBOL.into(),
                    ts: at(minute),
                    bid,
                    ask: bid + 2.0 * POINT,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, minute as u64),
            )
        })
        .collect::<Vec<_>>()
        .into()
}

fn bars(minutes: i64, count_known: bool) -> SymbolEvents {
    (0..minutes)
        .map(|minute| {
            let phase = (minute % 120) as f64 / 120.0 * std::f64::consts::TAU;
            let close = 1.1 + phase.sin() * 80.0 * POINT;
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol: SYMBOL.into(),
                    ts: at(minute),
                    open: close - POINT,
                    high: close + 2.0 * POINT,
                    low: close - 2.0 * POINT,
                    close,
                    volume: 0,
                    spread: Some(2.0 * POINT),
                    timeframe_seconds: Some(60),
                    tick_count: count_known.then_some(4),
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, minute as u64)
                    .with_available_at(at(minute + 1)),
            )
        })
        .collect::<Vec<_>>()
        .into()
}

fn config() -> BacktestConfig {
    BacktestConfig {
        initial_balance: 10_000.0,
        sizing: Some(SizingPolicy::FixedLot { lots: 0.1 }),
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
        contract_sizes: [(SYMBOL.into(), 100_000.0)].into_iter().collect(),
        ..BacktestConfig::default()
    }
}

fn windows() -> WindowPlan {
    WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(120), at(360)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(360), at(600)).unwrap(),
    }
}

fn family() -> EmaCrossFamily {
    EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap()).with_atr_period(3)
}

fn plan() -> ResearchPlan {
    ResearchPlan::new(vec![SYMBOL.into()], windows(), config())
}

struct Noop {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
}

impl HistoricalStrategy for Noop {
    type Error = DirectStrategyError;

    fn descriptor(&self) -> &StrategyDescriptor {
        &self.descriptor
    }

    fn requirements(&self) -> &StrategyRequirements {
        &self.requirements
    }

    fn on_event(
        &mut self,
        _: StrategyEvent<'_>,
        _: StrategyContext<'_>,
    ) -> Result<StrategyOutput, Self::Error> {
        Ok(StrategyOutput::none())
    }
}

struct NoopFactory;

impl DirectResearchFactory for NoopFactory {
    fn factory_name(&self) -> &str {
        "example_noop"
    }

    fn revision(&self) -> &str {
        "r1"
    }

    fn point_count(&self) -> usize {
        1
    }

    fn point(&self, index: usize) -> Option<DirectFactoryPoint> {
        (index == 0).then(|| DirectFactoryPoint {
            binding: ParameterBinding::new([("variant", ParameterValue::Choice("neutral".into()))]),
        })
    }

    fn create(
        &self,
        _: &DirectFactoryPoint,
        symbol: &str,
        window: &DataWindow,
    ) -> Result<DirectRunCandidate, ResearchError> {
        let series = SeriesRequirement::new(
            SeriesId::new("primary").unwrap(),
            symbol,
            Timeframe::minutes(1).unwrap(),
            PriceBasis::Bid,
            WarmupRequirement::bars(1).unwrap(),
        )
        .unwrap();
        Ok(DirectRunCandidate {
            strategy: Box::new(Noop {
                descriptor: StrategyDescriptor::new(
                    StrategyId::new("example_noop").unwrap(),
                    "r1",
                    format!("No-op {}", window.label()),
                )
                .unwrap(),
                requirements: StrategyRequirements::new(
                    vec![symbol.into()],
                    vec![series.clone()],
                    0,
                    false,
                    false,
                )
                .unwrap(),
            }),
            series: vec![BarSeriesSpec::new(series, 2, 0, MissingIntervalPolicy::Skip).unwrap()],
            analysis: AnalysisPipeline::new(
                vec![],
                ObservationStoreLimits::default(),
                AnnotationLimits::default(),
            )
            .unwrap(),
        })
    }
}

fn bar_inputs() -> Result<(), Box<dyn std::error::Error>> {
    for (label, events) in [
        ("counted", bars(620, true)),
        ("unknown-count", bars(620, false)),
    ] {
        let batch = run_batch(
            &plan(),
            &family(),
            &BTreeMap::from([(SYMBOL.into(), events)]),
        )?;
        println!("{label} bar-only rows: {}", batch.rows().len());
    }
    Ok(())
}

fn direct() -> Result<(), Box<dyn std::error::Error>> {
    let batch = run_direct_factory_batch(
        &plan(),
        &NoopFactory,
        &BTreeMap::from([(SYMBOL.into(), ticks(620))]),
        ExperimentOptions::default(),
    )?;
    println!("direct factory rows: {}", batch.rows().len());
    Ok(())
}

fn variants() -> Result<(), Box<dyn std::error::Error>> {
    let variants = [
        ExecutionVariant {
            id: "baseline".into(),
            backtest: config(),
            future: FutureQuoteConfig::default(),
            profiles: None,
            portfolio: None,
        },
        ExecutionVariant {
            id: "slippage".into(),
            backtest: config(),
            future: FutureQuoteConfig {
                slippage_pips: 1.0,
                ..FutureQuoteConfig::default()
            },
            profiles: None,
            portfolio: None,
        },
    ];
    let batch = run_execution_variants(
        &plan(),
        &family(),
        &BTreeMap::from([(SYMBOL.into(), ticks(620))]),
        &variants,
        ExecutionVariantLimits::new(2, 8)?,
        ExperimentOptions::default(),
    )?;
    println!("execution variant rows: {}", batch.table().len());
    Ok(())
}

fn mixed() -> Result<(), Box<dyn std::error::Error>> {
    let family = family();
    let point = family.points().remove(0);
    let configured = HeterogeneousInstanceSpec {
        instance_id: "configured".into(),
        symbol: SYMBOL.into(),
        document: family.config(&point),
        geometry: family.geometry(SYMBOL, &point),
        profiles: None,
    };
    let direct = HeterogeneousDirectInstanceSpec {
        instance_id: "direct".into(),
        symbol: SYMBOL.into(),
        factory_name: "example_noop".into(),
        point: NoopFactory.point(0).unwrap(),
        profiles: None,
    };
    let candidate = MixedHeterogeneousPortfolioCandidate {
        id: "mixed".into(),
        configured: vec![configured],
        direct: vec![direct],
        portfolio: PortfolioPlan::default(),
    };
    let factory: &dyn DirectResearchFactory = &NoopFactory;
    let factories = BTreeMap::from([("example_noop".into(), factory)]);
    let batch = run_mixed_heterogeneous_portfolios(
        &plan(),
        &[candidate],
        &factories,
        &BTreeMap::from([(SYMBOL.into(), ticks(620))]),
        ExecutionVariantLimits::new(1, 4)?,
    )?;
    println!("mixed portfolio rows: {}", batch.table().len());
    Ok(())
}

fn temporal() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceId::new("bars").unwrap();
    let base = StrategyConfig {
        strategy_id: "temporal_example".into(),
        title: "Temporal example".into(),
        parameters: vec![],
        initial_state: "idle".into(),
        sources: vec![source.clone()],
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
    };
    let mut limits = StructuralResourceLimits::small_test();
    limits.max_candidates = 8;
    let search = StructuralSearchSpec {
        family_id: "temporal_example".into(),
        base_document: base,
        state_id: "idle".into(),
        transition_priority: 1,
        atoms: vec![PredicateAtom {
            id: "body".into(),
            material: MaterialConfig {
                id: "body".into(),
                key: MATERIAL_BODY_FRACTION.into(),
                inputs: vec![],
                params: MaterialArgs::new([("source", MaterialArg::Source(source.clone()))]),
            },
            comparison: StructuralComparison::Gt,
            threshold: Literal::Ratio(0.1),
        }],
        operators: StructuralOperators {
            not: false,
            and: false,
            or: false,
            sequence: false,
        },
        sequence_source: source.clone(),
        sequence_max_gap: 2,
        captures: vec![CaptureCandidate {
            id: "captured_retest".into(),
            material_key: MATERIAL_SETUP_LONG_BOUNDED_QUEUE.into(),
            source: source.clone(),
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
                value: Literal::Price(1.1),
            },
            normalization: Expr::Literal {
                value: Literal::Number(1.0),
            },
            close: Expr::Literal {
                value: Literal::Price(1.1),
            },
            tolerance: Expr::Literal {
                value: Literal::Price(0.001),
            },
            ordinal: Expr::Input {
                field: "source_ordinal".into(),
                value_type: ValueType::optional(ScalarType::Integer),
            },
        }],
        geometry_by_symbol: BTreeMap::from([(
            SYMBOL.into(),
            vec![SeriesGeometry::new(
                source,
                SYMBOL,
                Timeframe::minutes(1).unwrap(),
                PriceBasis::Bid,
                0,
            )],
        )]),
        limits,
    };
    let (family, generation) = GeneratedStructuralFamily::new(&search)?;
    let batch = run_batch(
        &plan(),
        &family,
        &BTreeMap::from([(SYMBOL.into(), ticks(620))]),
    )?;
    println!(
        "temporal/capture candidates: {}, rows: {}",
        generation.candidates.len(),
        batch.rows().len()
    );
    Ok(())
}

fn cache() -> Result<(), Box<dyn std::error::Error>> {
    let events = ticks(620);
    let key = FeatureCacheKey {
        dataset_reference: "synthetic".into(),
        symbol: SYMBOL.into(),
        from: at(0),
        to: at(620),
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
    let bytes = FeatureCache::entry_bytes_upper_bound(&key, events.len())?;
    let mut cache = FeatureCache::new(bytes)?;
    let started = Instant::now();
    let produced = cached_market_midpoints(&mut cache, key.clone(), &events)?;
    let produced_micros = started.elapsed().as_micros();
    let started = Instant::now();
    let reused = cached_market_midpoints(&mut cache, key, &events)?;
    let clone_micros = started.elapsed().as_micros();
    assert_eq!(produced, reused);
    println!(
        "feature cache values: {}, produce_us: {}, staged_clone_us: {}",
        produced.len(),
        produced_micros,
        clone_micros
    );
    Ok(())
}

fn resume_final() -> Result<(), Box<dyn std::error::Error>> {
    let events = BTreeMap::from([(SYMBOL.into(), ticks(620))]);
    let options = ExperimentOptions {
        experiment_id: Some(ExperimentId::new("workflow")?),
        caller_revision: Some("example-r1".into()),
        dataset_reference: Some("synthetic".into()),
    };
    let full = run_batch_with_experiment(
        &plan(),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        options.clone(),
    )?;
    let completed = AtomicUsize::new(0);
    let partial = run_batch_controlled_with_experiment_resume(
        &plan(),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        options,
        &BTreeSet::new(),
        &|| completed.load(Ordering::Acquire) >= 1,
        &|progress| completed.store(progress.completed_runs, Ordering::Release),
    );
    let ResearchError::CancelledWithPartial(partial) = partial.unwrap_err() else {
        return Err("expected one-run partial batch".into());
    };
    let completed_runs = partial
        .run_recipes()
        .iter()
        .map(|recipe| recipe.ordinal)
        .collect::<BTreeSet<_>>();
    let remaining = run_batch_controlled_with_experiment_resume(
        &plan(),
        &family(),
        &events,
        ResearchAdmissionLimits::default(),
        ExperimentOptions::default(),
        &completed_runs,
        &|| false,
        &|_| {},
    )?;
    assert_eq!(
        partial.rows().len() + remaining.rows().len(),
        full.rows().len()
    );

    let candidate = &full.candidate_recipes()[0];
    let run = &full.run_recipes()[0];
    let window = DataWindow::new(run.window.clone(), run.from, run.to)?;
    let mut protected = ProtectedExperiment::default();
    protected.freeze(FrozenSelection {
        candidate: candidate.clone(),
        experiment: full.experiment_recipe().clone(),
        caller_revision: "example-r1".into(),
        future_horizon_millis: Some(1),
        embargo_millis: Some(1),
    })?;
    protected.release_final_for(&window, "example-r1")?;
    rerun_selected_candidate_protected(
        &plan(),
        full.experiment_recipe(),
        candidate,
        run,
        &events,
        &mut protected,
        EvaluationRole::Final,
        "example-r1",
    )?;
    println!(
        "checkpoint completed runs: {}, final access records: {}",
        completed_runs.len(),
        protected.records().len()
    );
    Ok(())
}

fn main() -> WorkflowResult {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "all".into());
    let workflows: [Workflow; 7] = [
        ("bar-inputs", bar_inputs),
        ("direct", direct),
        ("variants", variants),
        ("mixed", mixed),
        ("temporal", temporal),
        ("cache", cache),
        ("resume-final", resume_final),
    ];
    for (name, workflow) in workflows {
        if mode == "all" || mode == name {
            workflow()?;
        }
    }
    if mode != "all" && !workflows.iter().any(|(name, _)| *name == mode) {
        return Err(format!("unknown workflow '{mode}'").into());
    }
    Ok(())
}
