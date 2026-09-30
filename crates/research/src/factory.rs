use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::NaiveDateTime;
use qs_backtest::evaluation::PositionOutcome;
use qs_backtest::{
    AnalysisPipeline, BacktestRunner, BarSeriesSpec, HistoricalStrategy, StrategyBacktestResult,
    StrategyResearchLimits, VecFeed,
};
use qs_strategy::{ParameterBinding, parameter_value_label};

use crate::recipe::{
    CandidateRecipe, EndpointBounds, ExperimentOptions, ExperimentRecipe,
    RegisteredFactorySelection, RunRecipe, SeriesBindingSnapshot,
};
use crate::runner::{
    BatchProgress, ResearchBatch, SymbolEvents, coverage_with_result, fill_metrics, input_coverage,
    slice_events,
};
use crate::{
    DataWindow, ResearchAdmissionLimits, ResearchError, ResearchPlan, ResearchRow, ResearchTable,
    RunStatus,
};

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct DirectStrategyError(pub String);

#[derive(Debug, Clone, PartialEq)]
pub struct DirectFactoryPoint {
    pub binding: ParameterBinding,
}

pub struct DirectRunCandidate {
    pub strategy: Box<dyn HistoricalStrategy<Error = DirectStrategyError> + Send>,
    pub series: Vec<BarSeriesSpec>,
    pub analysis: AnalysisPipeline,
}

/// Trusted caller-compiled direct Rust strategy factory. Every call must return fresh mutable state.
pub trait DirectResearchFactory: Send + Sync {
    fn factory_name(&self) -> &str;
    fn revision(&self) -> &str;
    fn point_count(&self) -> usize;
    fn point(&self, index: usize) -> Option<DirectFactoryPoint>;
    fn create(
        &self,
        point: &DirectFactoryPoint,
        symbol: &str,
        window: &DataWindow,
    ) -> Result<DirectRunCandidate, ResearchError>;
}

pub fn run_direct_factory_batch(
    plan: &ResearchPlan,
    factory: &dyn DirectResearchFactory,
    events: &BTreeMap<String, SymbolEvents>,
    options: ExperimentOptions,
) -> Result<ResearchBatch, ResearchError> {
    run_direct_factory_batch_controlled(
        plan,
        factory,
        events,
        options,
        ResearchAdmissionLimits::default(),
        &|| false,
        &|_| {},
    )
}

pub fn run_direct_factory_batch_controlled(
    plan: &ResearchPlan,
    factory: &dyn DirectResearchFactory,
    events: &BTreeMap<String, SymbolEvents>,
    options: ExperimentOptions,
    limits: ResearchAdmissionLimits,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError> {
    run_direct_factory_batch_controlled_resume(
        plan,
        factory,
        events,
        options,
        limits,
        &std::collections::BTreeSet::new(),
        is_cancelled,
        on_progress,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_direct_factory_batch_controlled_resume(
    plan: &ResearchPlan,
    factory: &dyn DirectResearchFactory,
    events: &BTreeMap<String, SymbolEvents>,
    options: ExperimentOptions,
    limits: ResearchAdmissionLimits,
    completed_runs: &std::collections::BTreeSet<u64>,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError> {
    plan.validate()?;
    options.validate()?;
    ResearchAdmissionLimits::new(limits.max_window_pairs, limits.max_scheduled_runs)?;
    if is_cancelled() {
        return Err(ResearchError::Cancelled);
    }
    if plan.portfolio.is_some() {
        return Err(ResearchError::InvalidPlan(
            "direct factory portfolio composition uses the heterogeneous portfolio path".into(),
        ));
    }
    validate_text(factory.factory_name(), "factory name")?;
    validate_text(factory.revision(), "factory revision")?;
    let point_count = factory.point_count();
    if point_count == 0 {
        return Err(ResearchError::InvalidPlan(
            "direct factory produced no points".into(),
        ));
    }
    if point_count > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(
            "direct factory point count exceeds the pre-allocation limit".into(),
        ));
    }
    let mut points = Vec::with_capacity(point_count);
    for index in 0..point_count {
        points.push(factory.point(index).ok_or_else(|| {
            ResearchError::InvalidPlan("direct factory point count changed during admission".into())
        })?);
    }
    if factory.point(point_count).is_some() {
        return Err(ResearchError::InvalidPlan(
            "direct factory emitted a point beyond its declared count".into(),
        ));
    }
    let pairs = plan.window_plan.pairs_with_limit(limits.max_window_pairs)?;
    let windows = pairs
        .iter()
        .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
        .collect::<Vec<_>>();
    let total_runs = points
        .len()
        .checked_mul(plan.symbols.len())
        .and_then(|count| count.checked_mul(windows.len()))
        .ok_or_else(|| ResearchError::InvalidPlan("direct run count overflowed".into()))?;
    if total_runs > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(
            "direct run count exceeds the scheduled-run limit".into(),
        ));
    }

    let experiment_recipe = ExperimentRecipe {
        experiment_id: options.experiment_id,
        candidate_count: points.len(),
        caller_revision: options.caller_revision,
        dataset_reference: options.dataset_reference,
        endpoint_bounds: EndpointBounds::HalfOpen,
        ordered_symbols: plan.symbols.clone(),
        decision_latency_ms: plan.decision_latency_ms,
        portfolio: None,
        backtest: snapshot("backtest", &plan.config)?,
        future: snapshot("future", &plan.future)?,
        evaluation: snapshot("evaluation", &plan.evaluation)?,
        retention: snapshot("retention", &plan.retention)?,
        research_retention: snapshot("research retention", &StrategyResearchLimits::default())?,
        profiles: plan
            .entry_profiles
            .as_ref()
            .map(|profiles| snapshot("profiles", profiles))
            .transpose()?,
    };
    let selection = RegisteredFactorySelection {
        name: factory.factory_name().into(),
        revision: factory.revision().into(),
    };
    let mut candidate_recipes = points
        .iter()
        .enumerate()
        .map(|(index, point)| CandidateRecipe {
            ordinal: u64::try_from(index).unwrap(),
            family_id: factory.factory_name().into(),
            parameters: point
                .binding
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            document: None,
            registered_factory: Some(selection.clone()),
            series_by_symbol: BTreeMap::new(),
            series_by_instance: BTreeMap::new(),
            input_projectors: Vec::new(),
            admission_error: None,
        })
        .collect::<Vec<_>>();

    let mut jobs = Vec::with_capacity(total_runs);
    for point_index in 0..points.len() {
        for symbol_index in 0..plan.symbols.len() {
            for window_index in 0..windows.len() {
                jobs.push(DirectJob {
                    run_index: (point_index * plan.symbols.len() + symbol_index) * windows.len()
                        + window_index,
                    point_index,
                    symbol_index,
                    window_index,
                });
            }
        }
    }
    jobs.retain(|job| {
        u64::try_from(job.run_index)
            .ok()
            .is_none_or(|ordinal| !completed_runs.contains(&ordinal))
    });
    let (results, cancelled) = run_direct_jobs(
        plan,
        factory,
        events,
        &points,
        &windows,
        &jobs,
        is_cancelled,
        on_progress,
    )?;
    let mut rows = Vec::with_capacity(total_runs);
    let mut positions = Vec::<PositionOutcome>::new();
    let mut run_recipes = Vec::with_capacity(total_runs);
    for result in results {
        let recipe = &mut candidate_recipes[result.point_index];
        match recipe.series_by_symbol.get(&result.symbol) {
            Some(previous) if previous != &result.snapshots => {
                return Err(ResearchError::InvalidPlan(
                    "direct factory changed effective series across windows".into(),
                ));
            }
            Some(_) => {}
            None => {
                recipe
                    .series_by_symbol
                    .insert(result.symbol.clone(), result.snapshots);
            }
        }
        positions.extend(result.positions);
        rows.push(result.row);
        run_recipes.push(result.recipe);
    }
    let batch = ResearchBatch {
        table: ResearchTable::new(rows),
        positions,
        bound_documents: vec![None; points.len()],
        experiment_recipe,
        candidate_recipes,
        run_recipes,
    };
    if cancelled {
        if batch.run_recipes.is_empty() {
            Err(ResearchError::Cancelled)
        } else {
            Err(ResearchError::CancelledWithPartial(Box::new(batch)))
        }
    } else {
        Ok(batch)
    }
}

#[derive(Clone, Copy)]
struct DirectJob {
    run_index: usize,
    point_index: usize,
    symbol_index: usize,
    window_index: usize,
}
struct DirectJobResult {
    run_index: usize,
    point_index: usize,
    symbol: String,
    snapshots: Vec<SeriesBindingSnapshot>,
    row: ResearchRow,
    positions: Vec<PositionOutcome>,
    recipe: RunRecipe,
}

#[allow(clippy::too_many_arguments)]
fn run_direct_jobs(
    plan: &ResearchPlan,
    factory: &dyn DirectResearchFactory,
    events: &BTreeMap<String, SymbolEvents>,
    points: &[DirectFactoryPoint],
    windows: &[&DataWindow],
    jobs: &[DirectJob],
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<(Vec<DirectJobResult>, bool), ResearchError> {
    let execute = |job: DirectJob| execute_direct_job(plan, factory, events, points, windows, job);
    let completed = AtomicUsize::new(0);
    let report_completed = || {
        let completed_runs = completed.fetch_add(1, Ordering::Relaxed) + 1;
        on_progress(BatchProgress {
            completed_runs,
            total_runs: jobs.len(),
        });
    };
    let mut results = if plan.workers <= 1 || jobs.len() <= 1 {
        let mut results = Vec::with_capacity(jobs.len());
        for job in jobs.iter().copied() {
            if is_cancelled() {
                break;
            }
            results.push(execute(job)?);
            report_completed();
        }
        results
    } else {
        let next = AtomicUsize::new(0);
        let collected = Mutex::new(Vec::with_capacity(jobs.len()));
        std::thread::scope(|scope| {
            for _ in 0..plan.workers.min(jobs.len()) {
                let next = &next;
                let collected = &collected;
                let execute = &execute;
                let completed = &completed;
                scope.spawn(move || {
                    loop {
                        if is_cancelled() {
                            break;
                        }
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(job) = jobs.get(index).copied() else {
                            break;
                        };
                        let result = execute(job);
                        if result.is_ok() {
                            let completed_runs = completed.fetch_add(1, Ordering::Relaxed) + 1;
                            on_progress(BatchProgress {
                                completed_runs,
                                total_runs: jobs.len(),
                            });
                        }
                        collected.lock().unwrap().push(result);
                    }
                });
            }
        });
        collected
            .into_inner()
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
    };
    let cancelled = is_cancelled() || results.len() != jobs.len();
    results.sort_by_key(|result| result.run_index);
    Ok((results, cancelled))
}

fn execute_direct_job(
    plan: &ResearchPlan,
    factory: &dyn DirectResearchFactory,
    events: &BTreeMap<String, SymbolEvents>,
    points: &[DirectFactoryPoint],
    windows: &[&DataWindow],
    job: DirectJob,
) -> Result<DirectJobResult, ResearchError> {
    let point = &points[job.point_index];
    let symbol = &plan.symbols[job.symbol_index];
    let window = windows[job.window_index];
    let labels = point
        .binding
        .iter()
        .map(|(name, value)| (name.clone(), parameter_value_label(value)))
        .collect::<BTreeMap<_, _>>();
    let mut candidate = factory.create(point, symbol, window)?;
    let snapshots = candidate
        .series
        .iter()
        .map(series_snapshot)
        .collect::<Vec<_>>();
    let start = direct_warmup_start(&candidate.series, window.from())?;
    let slice = slice_events(
        events.get(symbol).ok_or_else(|| {
            ResearchError::InvalidPlan(format!("no market data supplied for '{symbol}'"))
        })?,
        window,
        start,
    );
    let coverage = input_coverage(&slice, window).map_err(|error| {
        ResearchError::InvalidPlan(format!("direct coverage failed: {error:?}"))
    })?;
    let data_mode = if slice
        .iter()
        .any(|event| matches!(event.event, qs_backtest::MarketEvent::Bar { .. }))
    {
        "bars"
    } else {
        "ticks"
    };
    let mut feed = VecFeed::from_feed_events(slice);
    let mut config = plan.config.clone();
    config.close_on_finish = true;
    config.run_tags.extend(labels.clone());
    config
        .run_tags
        .insert("window".into(), window.label().into());
    config.run_tags.insert("symbol".into(), symbol.clone());
    config.run_tags.insert("data_mode".into(), data_mode.into());
    let mut runner = BacktestRunner::new_future(config, plan.future.clone())
        .with_evaluation_options(plan.evaluation.clone());
    if let Some(profiles) = plan.entry_profiles.clone() {
        runner = runner.with_entry_profiles(profiles)
    }
    let result = runner
        .run_historical_strategy_future(
            &mut feed,
            candidate.strategy.as_mut(),
            candidate.series,
            candidate.analysis,
            plan.retention,
            plan.entry_profiles
                .as_ref()
                .and_then(|profiles| profiles.default_profile()),
        )
        .map_err(|error| ResearchError::InvalidPlan(format!("direct replay failed: {error}")))?;
    let StrategyBacktestResult { replay, .. } = result;
    let mut row = empty_row(
        factory.factory_name(),
        symbol,
        labels.clone(),
        window,
        points.len(),
    );
    row.data_mode = data_mode.into();
    fill_metrics(&mut row, &replay, window);
    let mut positions = replay.provider_positions.clone();
    for position in &mut positions {
        position.id = format!("run:{}|{}", job.run_index, position.id);
        if let Some(trade_id) = position.trade_id.as_mut() {
            *trade_id = format!("run:{}|{trade_id}", job.run_index)
        }
    }
    let coverage = coverage_with_result(coverage, &replay);
    let mut tags = plan.config.run_tags.clone();
    tags.extend(labels);
    tags.insert("window".into(), window.label().into());
    tags.insert("symbol".into(), symbol.clone());
    tags.insert("data_mode".into(), data_mode.into());
    let recipe = RunRecipe {
        ordinal: u64::try_from(job.run_index).unwrap(),
        candidate_ordinal: u64::try_from(job.point_index).unwrap(),
        window: window.label().into(),
        symbol: symbol.clone(),
        data_mode: data_mode.into(),
        run_tags: tags,
        from: window.from(),
        to: window.to(),
        coverage: Some(coverage),
    };
    Ok(DirectJobResult {
        run_index: job.run_index,
        point_index: job.point_index,
        symbol: symbol.clone(),
        snapshots,
        row,
        positions,
        recipe,
    })
}

pub(crate) fn empty_row(
    family: &str,
    symbol: &str,
    params: BTreeMap<String, String>,
    window: &DataWindow,
    points_total: usize,
) -> ResearchRow {
    ResearchRow {
        family_id: family.into(),
        symbol: symbol.into(),
        params,
        window: window.label().into(),
        status: RunStatus::Completed,
        data_mode: "ticks".into(),
        positions: 0,
        win_rate: None,
        net_pnl: 0.0,
        gross_pnl: None,
        commission: 0.0,
        swap: 0.0,
        expectancy_r: None,
        r_p05: None,
        r_p50: None,
        r_p95: None,
        avg_favorable_r: None,
        avg_adverse_r: None,
        max_drawdown_pct: 0.0,
        forced_closes: 0,
        entries_before_window: 0,
        points_total,
        rejected_entries: None,
        halt_minutes: None,
    }
}

pub(crate) fn direct_warmup_start(
    series: &[BarSeriesSpec],
    from: NaiveDateTime,
) -> Result<NaiveDateTime, ResearchError> {
    let mut start = from;
    for spec in series {
        let requirement = spec.requirement();
        let seconds = i64::try_from(requirement.timeframe().duration_seconds())
            .map_err(|_| ResearchError::InvalidPlan("timeframe exceeds i64".into()))?;
        let offset = spec.alignment_offset_seconds();
        let from_seconds = from.and_utc().timestamp();
        let aligned = from_seconds
            .checked_sub((from_seconds - offset).rem_euclid(seconds))
            .ok_or_else(|| ResearchError::InvalidPlan("warmup alignment overflowed".into()))?;
        let warmup = seconds
            .checked_mul(
                i64::try_from(requirement.warmup().required_bars())
                    .map_err(|_| ResearchError::InvalidPlan("warmup count exceeds i64".into()))?,
            )
            .ok_or_else(|| ResearchError::InvalidPlan("warmup duration overflowed".into()))?;
        let candidate = chrono::DateTime::from_timestamp(
            aligned
                .checked_sub(warmup)
                .ok_or_else(|| ResearchError::InvalidPlan("warmup timestamp overflowed".into()))?,
            0,
        )
        .map(|value| value.naive_utc())
        .ok_or_else(|| ResearchError::InvalidPlan("warmup timestamp is out of range".into()))?;
        start = start.min(candidate);
    }
    Ok(start)
}

pub(crate) fn series_snapshot(spec: &BarSeriesSpec) -> SeriesBindingSnapshot {
    let requirement = spec.requirement();
    SeriesBindingSnapshot {
        source: requirement.id().as_str().into(),
        symbol: requirement.symbol().into(),
        timeframe_seconds: requirement.timeframe().duration_seconds(),
        price_basis: format!("{:?}", requirement.price_basis()).to_lowercase(),
        alignment_offset_seconds: spec.alignment_offset_seconds(),
        warmup_bars: requirement.warmup().required_bars(),
        retained_bars: spec.retained_bars(),
    }
}

fn snapshot(name: &str, value: &impl serde::Serialize) -> Result<serde_json::Value, ResearchError> {
    serde_json::to_value(value).map_err(|error| {
        ResearchError::InvalidPlan(format!("failed to snapshot direct {name}: {error}"))
    })
}

fn validate_text(value: &str, name: &str) -> Result<(), ResearchError> {
    if value.is_empty() || value.len() > 256 || value.trim() != value || !value.is_ascii() {
        Err(ResearchError::InvalidPlan(format!("invalid {name}")))
    } else {
        Ok(())
    }
}
