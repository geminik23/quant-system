use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_backtest::data_feed::{FeedEvent, MarketEvent};
use qs_backtest::evaluation::{
    EvaluationOptions, EvaluationReport, EvaluationRequest, PositionOutcome,
};
use qs_backtest::report::BacktestResult;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
    ConfiguredInstance, INSTANCE_POSITION_TAG, ObservationStoreLimits, StrategyDescriptor,
    StrategyId, StrategyResearchLimits, SupervisorOutput, VecFeed,
};
use qs_core::CloseReason;
use qs_strategy::{ConfiguredStrategy, ParameterBinding, StrategyConfig, parameter_value_label};
use serde::Serialize;

use crate::error::{ResearchError, RunFailure};
use crate::family::StrategyFamily;
use crate::plan::{ResearchAdmissionLimits, ResearchPlan};
use crate::recipe::{
    CandidateRecipe, EndpointBounds, ExperimentOptions, ExperimentRecipe, RunCoverage, RunRecipe,
    SeriesBindingSnapshot, snapshot_bindings,
};
use crate::table::{ResearchRow, ResearchTable, RunStatus};
use crate::window::DataWindow;

/// Data mode recorded for a symbol that supplied no primary events.
const DEFAULT_DATA_MODE: &str = "ticks";
const RESERVED_TAGS: [&str; 3] = ["window", "symbol", "data_mode"];
/// Separator of the symbols a portfolio run's row and `symbol` tag name.
const PORTFOLIO_SYMBOL_SEPARATOR: &str = "+";

#[derive(Clone)]
struct RecipeFamily {
    family_id: String,
    binding: ParameterBinding,
    document: StrategyConfig,
    geometry: BTreeMap<String, Vec<crate::SeriesGeometry>>,
}
impl StrategyFamily for RecipeFamily {
    type Params = ();
    fn family_id(&self) -> &str {
        &self.family_id
    }
    fn points(&self) -> Vec<Self::Params> {
        vec![()]
    }
    fn parameter_binding(&self, _: &Self::Params) -> ParameterBinding {
        self.binding.clone()
    }
    fn config(&self, _: &Self::Params) -> StrategyConfig {
        self.document.clone()
    }
    fn geometry(&self, symbol: &str, _: &Self::Params) -> Vec<crate::SeriesGeometry> {
        self.geometry.get(symbol).cloned().unwrap_or_default()
    }
}

pub type SymbolEvents = Arc<[FeedEvent]>;

struct AdmittedPoint<P> {
    point: P,
    binding: ParameterBinding,
    document: StrategyConfig,
}

struct RunSpec<'a, P> {
    run_index: usize,
    candidate: &'a AdmittedPoint<P>,
    point_index: usize,
    symbol: &'a str,
    window: &'a DataWindow,
    data_mode: &'static str,
}

struct RunRecord {
    run_index: usize,
    row: ResearchRow,
    positions: Vec<PositionOutcome>,
    binding_snapshots: BTreeMap<String, Vec<SeriesBindingSnapshot>>,
    coverage: Option<RunCoverage>,
}

struct ExecutedRun {
    result: BacktestResult,
    binding_snapshots: BTreeMap<String, Vec<SeriesBindingSnapshot>>,
    coverage: RunCoverage,
}

struct ExecutedPortfolio {
    run: ExecutedRun,
    supervisor: Option<SupervisorOutput>,
}

/// All retained outcomes and projections produced by one parameter search.
#[derive(Debug, Clone, PartialEq)]
pub struct ResearchBatch {
    pub(crate) table: ResearchTable,
    pub(crate) positions: Vec<PositionOutcome>,
    pub(crate) bound_documents: Vec<Option<StrategyConfig>>,
    pub(crate) experiment_recipe: ExperimentRecipe,
    pub(crate) candidate_recipes: Vec<CandidateRecipe>,
    pub(crate) run_recipes: Vec<RunRecipe>,
}

impl ResearchBatch {
    pub fn table(&self) -> &ResearchTable {
        &self.table
    }

    pub fn evaluate(&self, options: EvaluationOptions) -> EvaluationReport {
        qs_backtest::evaluation::evaluate(&EvaluationRequest {
            positions: self.positions.clone(),
            lifecycle: None,
            options,
        })
    }

    pub fn bound_document(&self, point: usize) -> Option<&StrategyConfig> {
        self.bound_documents.get(point).and_then(Option::as_ref)
    }

    pub fn position_outcomes(&self) -> &[PositionOutcome] {
        &self.positions
    }

    pub fn experiment_recipe(&self) -> &ExperimentRecipe {
        &self.experiment_recipe
    }

    pub fn candidate_recipes(&self) -> &[CandidateRecipe] {
        &self.candidate_recipes
    }

    pub fn run_recipes(&self) -> &[RunRecipe] {
        &self.run_recipes
    }

    pub fn merge_checkpoint(
        mut self,
        checkpoint: &crate::SearchCheckpoint,
    ) -> Result<Self, ResearchError> {
        let mut candidates = checkpoint.candidate_recipes.clone();
        for candidate in self.candidate_recipes.drain(..) {
            if let Some(previous) = candidates.insert(candidate.ordinal, candidate.clone())
                && previous != candidate
            {
                return Err(ResearchError::InvalidPlan(
                    "resumed candidate recipe differs from its checkpoint".into(),
                ));
            }
        }
        let mut runs = checkpoint
            .committed_runs
            .values()
            .map(|outcome| (outcome.recipe.ordinal, outcome.recipe.clone()))
            .collect::<BTreeMap<_, _>>();
        for run in self.run_recipes.drain(..) {
            if runs.insert(run.ordinal, run.clone()).is_some() {
                return Err(ResearchError::InvalidPlan(
                    "resumed run duplicates a committed checkpoint run".into(),
                ));
            }
        }
        let mut rows = checkpoint
            .committed_runs
            .values()
            .map(|outcome| outcome.row.clone())
            .collect::<Vec<_>>();
        rows.extend(self.table.rows().iter().cloned());
        let mut positions = checkpoint
            .committed_runs
            .values()
            .flat_map(|outcome| outcome.positions.iter().cloned())
            .collect::<Vec<_>>();
        positions.extend(self.positions);
        let mut position_ids = BTreeSet::new();
        if positions
            .iter()
            .any(|position| !position_ids.insert(position.id.clone()))
        {
            return Err(ResearchError::InvalidPlan(
                "resumed positions duplicate a committed position identity".into(),
            ));
        }
        let mut experiment = checkpoint
            .experiment_recipe
            .clone()
            .unwrap_or(self.experiment_recipe);
        experiment.candidate_count = candidates.len();
        let mut candidate_recipes = candidates.into_values().collect::<Vec<_>>();
        candidate_recipes.sort_by_key(|candidate| candidate.ordinal);
        let bound_documents = candidate_recipes
            .iter()
            .map(|candidate| {
                candidate
                    .document
                    .clone()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|error| ResearchError::InvalidDocument(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            table: ResearchTable::new(rows),
            positions,
            bound_documents,
            experiment_recipe: experiment,
            candidate_recipes,
            run_recipes: runs.into_values().collect(),
        })
    }
}

impl Deref for ResearchBatch {
    type Target = ResearchTable;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

pub fn run_batch<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    run_batch_with_experiment(
        plan,
        family,
        events,
        ResearchAdmissionLimits::default(),
        ExperimentOptions::default(),
    )
}

pub fn run_batch_with_experiment<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    limits: ResearchAdmissionLimits,
    options: ExperimentOptions,
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    run_batch_controlled_with_experiment(plan, family, events, limits, options, &|| false, &|_| {})
}

#[allow(clippy::too_many_arguments)]
pub fn rerun_selected_candidate_protected(
    plan: &ResearchPlan,
    experiment: &ExperimentRecipe,
    candidate: &CandidateRecipe,
    run: &RunRecipe,
    events: &BTreeMap<String, SymbolEvents>,
    protected: &mut crate::ProtectedExperiment,
    role: crate::EvaluationRole,
    caller_revision: &str,
) -> Result<ResearchBatch, ResearchError> {
    if role == crate::EvaluationRole::Final {
        let frozen = protected.frozen().ok_or_else(|| {
            ResearchError::InvalidPlan("final rerun requires frozen selection".into())
        })?;
        if &frozen.candidate != candidate || &frozen.experiment != experiment {
            return Err(ResearchError::InvalidPlan(
                "final rerun differs from the frozen selection".into(),
            ));
        }
    }
    let window = DataWindow::new(run.window.clone(), run.from, run.to)?;
    protected.access(role, &window, caller_revision, true)?;
    rerun_selected_candidate(plan, experiment, candidate, run, events)
}

pub fn rerun_selected_candidate(
    plan: &ResearchPlan,
    experiment: &ExperimentRecipe,
    candidate: &CandidateRecipe,
    run: &RunRecipe,
    events: &BTreeMap<String, SymbolEvents>,
) -> Result<ResearchBatch, ResearchError> {
    if experiment.endpoint_bounds != EndpointBounds::HalfOpen
        || run.symbol.contains(PORTFOLIO_SYMBOL_SEPARATOR)
    {
        return Err(ResearchError::InvalidPlan(
            "selected rerun requires one half-open configured run".into(),
        ));
    }
    let document_value = candidate.document.clone().ok_or_else(|| {
        ResearchError::InvalidPlan(
            "selected candidate is a direct factory, not a configured document".into(),
        )
    })?;
    if candidate.registered_factory.is_some() {
        return Err(ResearchError::InvalidPlan(
            "candidate recipe cannot contain both document and factory".into(),
        ));
    }
    let document: StrategyConfig = serde_json::from_value(document_value)
        .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
    let effective_backtest = {
        let mut value = plan.config.clone();
        value.close_on_finish = true;
        recipe_value("rerun backtest", &value)?
    };
    if effective_backtest != experiment.backtest
        || recipe_value("rerun future", &plan.future)? != experiment.future
        || recipe_value("rerun evaluation", &plan.evaluation)? != experiment.evaluation
        || recipe_value("rerun retention", &plan.retention)? != experiment.retention
    {
        return Err(ResearchError::InvalidPlan(
            "selected rerun settings differ from the frozen experiment recipe".into(),
        ));
    }
    let snapshots = candidate.series_by_symbol.get(&run.symbol).ok_or_else(|| {
        ResearchError::InvalidPlan("candidate recipe has no series for rerun symbol".into())
    })?;
    let geometry = snapshots
        .iter()
        .map(|snapshot| {
            let timeframe = qs_backtest::Timeframe::seconds(
                u32::try_from(snapshot.timeframe_seconds).map_err(|_| {
                    ResearchError::InvalidPlan("rerun timeframe exceeds u32".into())
                })?,
            )
            .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
            let basis = match snapshot.price_basis.as_str() {
                "bid" => qs_backtest::PriceBasis::Bid,
                "ask" => qs_backtest::PriceBasis::Ask,
                "mid" => qs_backtest::PriceBasis::Mid,
                other => {
                    return Err(ResearchError::InvalidPlan(format!(
                        "unknown rerun price basis {other}"
                    )));
                }
            };
            Ok(crate::SeriesGeometry::new(
                qs_strategy::SourceId::new(snapshot.source.clone())
                    .map_err(ResearchError::InvalidPlan)?,
                snapshot.symbol.clone(),
                timeframe,
                basis,
                i32::try_from(snapshot.alignment_offset_seconds).map_err(|_| {
                    ResearchError::InvalidPlan("rerun alignment exceeds i32".into())
                })?,
            ))
        })
        .collect::<Result<Vec<_>, ResearchError>>()?;
    let family = RecipeFamily {
        family_id: candidate.family_id.clone(),
        binding: ParameterBinding::new(candidate.parameters.clone()),
        document: document.clone(),
        geometry: BTreeMap::from([(run.symbol.clone(), geometry)]),
    };
    let window = DataWindow::new(run.window.clone(), run.from, run.to)?;
    let admitted = AdmittedPoint {
        point: (),
        binding: family.binding.clone(),
        document: document.clone(),
    };
    let data_mode = match run.data_mode.as_str() {
        "ticks" => "ticks",
        "bars" => "bars",
        other => {
            return Err(ResearchError::InvalidPlan(format!(
                "unsupported rerun data mode '{other}'"
            )));
        }
    };
    let spec = RunSpec {
        run_index: 0,
        candidate: &admitted,
        point_index: 0,
        symbol: &run.symbol,
        window: &window,
        data_mode,
    };
    let mut record = evaluate_run(
        plan,
        &family,
        &spec,
        events,
        experiment.candidate_count.max(1),
    );
    let mut positions = Vec::new();
    for position in &mut record.positions {
        position.id = format!("run:{}|{}", run.ordinal, position.id);
        positions.push(position.clone());
    }
    let mut run_recipe = run.clone();
    run_recipe.coverage = record.coverage;
    Ok(ResearchBatch {
        table: ResearchTable::new(vec![record.row]),
        positions,
        bound_documents: vec![Some(document)],
        experiment_recipe: experiment.clone(),
        candidate_recipes: vec![candidate.clone()],
        run_recipes: vec![run_recipe],
    })
}

/// Runs completed so far out of the batch's total, reported after each run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchProgress {
    pub completed_runs: usize,
    pub total_runs: usize,
}

/// Run a batch that checks `is_cancelled` before starting each run and reports progress after each completed run, so a long search can be stopped and observed without losing determinism.
pub fn run_batch_controlled<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    run_batch_controlled_with_limits(
        plan,
        family,
        events,
        ResearchAdmissionLimits::default(),
        is_cancelled,
        on_progress,
    )
}

/// Run a batch with explicit pre-allocation window and scheduled-run limits.
pub fn run_batch_controlled_with_limits<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    limits: ResearchAdmissionLimits,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    run_batch_controlled_with_experiment(
        plan,
        family,
        events,
        limits,
        ExperimentOptions::default(),
        is_cancelled,
        on_progress,
    )
}

pub fn run_batch_controlled_with_experiment<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    limits: ResearchAdmissionLimits,
    options: ExperimentOptions,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    run_batch_controlled_with_experiment_resume(
        plan,
        family,
        events,
        limits,
        options,
        &BTreeSet::new(),
        is_cancelled,
        on_progress,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_batch_controlled_with_experiment_resume<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    limits: ResearchAdmissionLimits,
    options: ExperimentOptions,
    completed_runs: &BTreeSet<u64>,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    plan.validate()?;
    options.validate()?;
    ResearchAdmissionLimits::new(limits.max_window_pairs, limits.max_scheduled_runs)?;
    let pairs = plan.window_plan.pairs_with_limit(limits.max_window_pairs)?;
    if pairs.is_empty() {
        return Err(ResearchError::InvalidWindow(
            "the window plan produced no evaluation periods".into(),
        ));
    }
    let points = family.points();
    if points.is_empty() {
        return Err(ResearchError::InvalidPlan(
            "the family produced no parameter points".into(),
        ));
    }
    let points_total = points.len();
    let symbol_runs = if plan.portfolio.is_some() {
        1
    } else {
        plan.symbols.len()
    };
    let total_runs = checked_run_count(symbol_runs, pairs.len(), points_total, limits)?;
    let candidates = points
        .into_iter()
        .map(|point| AdmittedPoint {
            binding: family.parameter_binding(&point),
            document: family.config(&point),
            point,
        })
        .collect::<Vec<_>>();
    let bound_documents = candidates
        .iter()
        .map(|candidate| Some(candidate.document.clone()))
        .collect();
    let bindings = candidates
        .iter()
        .map(|candidate| candidate.binding.clone())
        .collect::<Vec<_>>();
    validate_parameter_labels(family.family_id(), &bindings, plan)?;
    let mut effective_backtest = plan.config.clone();
    effective_backtest.close_on_finish = true;
    let experiment_recipe = ExperimentRecipe {
        experiment_id: options.experiment_id,
        candidate_count: points_total,
        caller_revision: options.caller_revision,
        dataset_reference: options.dataset_reference,
        endpoint_bounds: EndpointBounds::HalfOpen,
        ordered_symbols: plan.symbols.clone(),
        decision_latency_ms: plan.decision_latency_ms,
        portfolio: plan
            .portfolio
            .as_ref()
            .map(|portfolio| recipe_value("portfolio configuration", portfolio))
            .transpose()?,
        backtest: recipe_value("backtest configuration", &effective_backtest)?,
        future: recipe_value("FutureQuote configuration", &plan.future)?,
        evaluation: recipe_value("evaluation configuration", &plan.evaluation)?,
        retention: recipe_value("retention configuration", &plan.retention)?,
        research_retention: recipe_value(
            "research retention configuration",
            &StrategyResearchLimits::default(),
        )?,
        profiles: plan
            .entry_profiles
            .as_ref()
            .map(|profiles| recipe_value("entry profiles", profiles))
            .transpose()?,
    };
    let mut candidate_recipes = candidates
        .iter()
        .enumerate()
        .map(|(ordinal, candidate)| {
            admission_candidate_recipe(family, candidate, ordinal, &plan.symbols)
        })
        .collect::<Result<Vec<_>, ResearchError>>()?;

    let mut data_modes = BTreeMap::new();
    for symbol in &plan.symbols {
        let Some(symbol_events) = events.get(symbol.as_str()) else {
            continue;
        };
        if symbol_events
            .windows(2)
            .any(|pair| pair[0].event.ts() > pair[1].event.ts())
        {
            return Err(ResearchError::InvalidPlan(format!(
                "events for '{symbol}' are not in ascending timestamp order"
            )));
        }
        data_modes.insert(symbol.as_str(), data_mode(symbol, symbol_events)?);
    }
    if plan.portfolio.is_some() {
        if data_modes.values().any(|mode| *mode == "bars") {
            validate_bar_window_alignment_with_limits(plan, family, limits)?;
        }
    } else {
        for (symbol, mode) in &data_modes {
            if *mode == "bars" {
                let mut bar_plan = plan.clone();
                bar_plan.symbols = vec![(*symbol).to_owned()];
                validate_bar_window_alignment_with_limits(&bar_plan, family, limits)?;
            }
        }
    }

    let portfolio_label = plan.symbols.join(PORTFOLIO_SYMBOL_SEPARATOR);
    let run_symbols: Vec<&str> = if plan.portfolio.is_some() {
        let modes = data_modes
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if modes.len() > 1 {
            return Err(ResearchError::InvalidPlan(
                "a portfolio replays every symbol in one feed, so they must all be ticks or all be stored bars".into(),
            ));
        }
        vec![portfolio_label.as_str()]
    } else {
        plan.symbols.iter().map(String::as_str).collect()
    };
    let portfolio_mode = data_modes
        .values()
        .next()
        .copied()
        .unwrap_or(DEFAULT_DATA_MODE);
    let mut specs = Vec::with_capacity(total_runs);
    for symbol in run_symbols {
        for pair in &pairs {
            for window in [&pair.in_sample, &pair.out_of_sample] {
                for (point_index, candidate) in candidates.iter().enumerate() {
                    specs.push(RunSpec {
                        run_index: specs.len(),
                        candidate,
                        point_index,
                        symbol,
                        window,
                        data_mode: if plan.portfolio.is_some() {
                            portfolio_mode
                        } else {
                            data_modes.get(symbol).copied().unwrap_or(DEFAULT_DATA_MODE)
                        },
                    });
                }
            }
        }
    }

    specs.retain(|spec| {
        u64::try_from(spec.run_index)
            .ok()
            .is_none_or(|ordinal| !completed_runs.contains(&ordinal))
    });
    let workers = plan.workers.min(specs.len()).max(1);
    let control = RunControl {
        is_cancelled,
        on_progress,
        completed: std::sync::atomic::AtomicUsize::new(0),
        total: specs.len(),
    };
    let mut records = if workers == 1 {
        let mut records = Vec::with_capacity(specs.len());
        for spec in &specs {
            if is_cancelled() {
                break;
            }
            records.push(evaluate_run(plan, family, spec, events, points_total));
            control.completed_one();
        }
        records
    } else {
        run_in_parallel(
            plan,
            family,
            &specs,
            events,
            points_total,
            workers,
            &control,
        )
    };
    let cancelled = is_cancelled() || records.len() < specs.len();
    records.sort_by_key(|record| record.run_index);

    let mut positions = Vec::new();
    let mut rows = Vec::with_capacity(records.len());
    let mut run_recipes = Vec::with_capacity(records.len());
    for mut record in records {
        let spec = specs
            .iter()
            .find(|spec| spec.run_index == record.run_index)
            .ok_or_else(|| {
                ResearchError::InvalidPlan("completed run has no admitted specification".into())
            })?;
        let candidate_recipe = &mut candidate_recipes[spec.point_index];
        for (symbol, snapshots) in record.binding_snapshots {
            match candidate_recipe.series_by_symbol.get(&symbol) {
                Some(previous) if previous != &snapshots => {
                    return Err(ResearchError::InvalidPlan(format!(
                        "family '{}' produced different effective bindings for candidate {} symbol '{}' across windows",
                        family.family_id(),
                        spec.point_index,
                        symbol
                    )));
                }
                Some(_) => {}
                None => {
                    candidate_recipe.series_by_symbol.insert(symbol, snapshots);
                }
            }
        }
        run_recipes.push(RunRecipe {
            ordinal: persisted_ordinal(record.run_index, "run")?,
            candidate_ordinal: persisted_ordinal(spec.point_index, "candidate")?,
            window: spec.window.label().to_owned(),
            symbol: spec.symbol.to_owned(),
            data_mode: spec.data_mode.to_owned(),
            run_tags: run_tags(plan, &binding_labels(&spec.candidate.binding), spec),
            from: spec.window.from(),
            to: spec.window.to(),
            coverage: record.coverage,
        });
        for position in &mut record.positions {
            position.id = format!("run:{}|{}", record.run_index, position.id);
            if let Some(trade_id) = position.trade_id.as_mut() {
                *trade_id = format!("run:{}|{trade_id}", record.run_index);
            }
        }
        positions.extend(record.positions);
        rows.push(record.row);
    }

    let batch = ResearchBatch {
        table: ResearchTable::new(rows),
        positions,
        bound_documents,
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

/// Validate every evaluation boundary against each candidate's shortest execution series per symbol.
pub fn validate_bar_window_alignment<F>(
    plan: &ResearchPlan,
    family: &F,
) -> Result<(), ResearchError>
where
    F: StrategyFamily,
{
    validate_bar_window_alignment_with_limits(plan, family, ResearchAdmissionLimits::default())
}

pub fn validate_bar_window_alignment_with_limits<F>(
    plan: &ResearchPlan,
    family: &F,
    limits: ResearchAdmissionLimits,
) -> Result<(), ResearchError>
where
    F: StrategyFamily,
{
    plan.validate()?;
    ResearchAdmissionLimits::new(limits.max_window_pairs, limits.max_scheduled_runs)?;
    let pairs = plan.window_plan.pairs_with_limit(limits.max_window_pairs)?;
    for (point_index, point) in family.points().iter().enumerate() {
        for plan_symbol in &plan.symbols {
            let strategy = ConfiguredStrategy::compile(
                family.config(point),
                &family.library(),
                format!("alignment-{point_index}"),
                plan_symbol,
            )
            .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
            let bindings = family
                .bindings(plan_symbol, point, strategy.input_requirements())
                .map_err(ResearchError::InvalidDocument)?;
            let mut shortest = BTreeMap::<&str, u64>::new();
            for binding in bindings.sources() {
                let requirement = binding.series().requirement();
                let duration = requirement.timeframe().duration_seconds();
                shortest
                    .entry(requirement.symbol())
                    .and_modify(|current| *current = (*current).min(duration))
                    .or_insert(duration);
            }
            for binding in bindings.sources() {
                let series = binding.series();
                let requirement = series.requirement();
                let duration = requirement.timeframe().duration_seconds();
                if shortest.get(requirement.symbol()) != Some(&duration) {
                    continue;
                }
                for pair in &pairs {
                    for window in [&pair.in_sample, &pair.out_of_sample] {
                        for (name, timestamp) in [("from", window.from()), ("to", window.to())] {
                            let offset =
                                i32::try_from(series.alignment_offset_seconds()).map_err(|_| {
                                    ResearchError::InvalidDocument(format!(
                                        "source '{}' alignment offset does not fit i32",
                                        binding.source()
                                    ))
                                })?;
                            let geometry = qs_backtest::SeriesGeometry::new(
                                binding.source().clone(),
                                requirement.symbol(),
                                requirement.timeframe(),
                                requirement.price_basis(),
                                offset,
                            );
                            if !geometry.is_aligned(timestamp) {
                                return Err(ResearchError::InvalidWindow(format!(
                                    "window '{}' {name} {timestamp} is not aligned to search point {point_index} source '{}' {duration}s bars with offset {}s",
                                    window.label(),
                                    binding.source(),
                                    series.alignment_offset_seconds()
                                )));
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Check everything about a batch that does not need market data, the same checks a batch runs before its first replay, and return how many runs it would schedule.
pub fn validate_batch<F>(plan: &ResearchPlan, family: &F) -> Result<usize, ResearchError>
where
    F: StrategyFamily,
{
    validate_batch_with_limits(plan, family, ResearchAdmissionLimits::default())
}

pub fn validate_batch_with_limits<F>(
    plan: &ResearchPlan,
    family: &F,
    limits: ResearchAdmissionLimits,
) -> Result<usize, ResearchError>
where
    F: StrategyFamily,
{
    plan.validate()?;
    ResearchAdmissionLimits::new(limits.max_window_pairs, limits.max_scheduled_runs)?;
    let pairs = plan.window_plan.pairs_with_limit(limits.max_window_pairs)?;
    if pairs.is_empty() {
        return Err(ResearchError::InvalidWindow(
            "the window plan produced no evaluation periods".into(),
        ));
    }
    let points = family.points();
    if points.is_empty() {
        return Err(ResearchError::InvalidPlan(
            "the family produced no parameter points".into(),
        ));
    }
    let symbol_runs = if plan.portfolio.is_some() {
        1
    } else {
        plan.symbols.len()
    };
    let runs = checked_run_count(symbol_runs, pairs.len(), points.len(), limits)?;
    let bindings: Vec<ParameterBinding> = points
        .iter()
        .map(|point| family.parameter_binding(point))
        .collect();
    validate_parameter_labels(family.family_id(), &bindings, plan)?;
    Ok(runs)
}

fn persisted_ordinal(value: usize, name: &str) -> Result<u64, ResearchError> {
    u64::try_from(value).map_err(|_| {
        ResearchError::InvalidPlan(format!(
            "{name} ordinal does not fit the persisted u64 format"
        ))
    })
}

fn admission_candidate_recipe<F>(
    family: &F,
    candidate: &AdmittedPoint<F::Params>,
    ordinal: usize,
    symbols: &[String],
) -> Result<CandidateRecipe, ResearchError>
where
    F: StrategyFamily,
{
    let mut series_by_symbol = BTreeMap::new();
    let mut input_projectors = Vec::new();
    let mut admission_error = None;
    for symbol in symbols {
        let strategy = match ConfiguredStrategy::compile(
            candidate.document.clone(),
            &family.library(),
            format!("admission-{ordinal}"),
            symbol,
        ) {
            Ok(strategy) => strategy,
            Err(error) => {
                admission_error.get_or_insert_with(|| format!("compile: {error}"));
                continue;
            }
        };
        input_projectors.extend(family.input_projector_recipe(symbol, &candidate.point));
        match family.bindings(symbol, &candidate.point, strategy.input_requirements()) {
            Ok(bindings) => {
                series_by_symbol.insert(symbol.clone(), snapshot_bindings(&bindings));
            }
            Err(error) => {
                admission_error.get_or_insert_with(|| format!("bind: {error}"));
            }
        }
    }
    Ok(CandidateRecipe {
        ordinal: persisted_ordinal(ordinal, "candidate")?,
        family_id: family.family_id().to_owned(),
        parameters: candidate
            .binding
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        document: Some(recipe_value(
            "bound strategy document",
            &candidate.document,
        )?),
        registered_factory: None,
        series_by_symbol,
        series_by_instance: BTreeMap::new(),
        input_projectors,
        admission_error,
    })
}

fn recipe_value(name: &str, value: &impl Serialize) -> Result<serde_json::Value, ResearchError> {
    serde_json::to_value(value)
        .map_err(|error| ResearchError::InvalidPlan(format!("failed to snapshot {name}: {error}")))
}

fn checked_run_count(
    symbol_runs: usize,
    pair_count: usize,
    point_count: usize,
    limits: ResearchAdmissionLimits,
) -> Result<usize, ResearchError> {
    let runs = symbol_runs
        .checked_mul(pair_count)
        .and_then(|count| count.checked_mul(2))
        .and_then(|count| count.checked_mul(point_count))
        .ok_or_else(|| ResearchError::InvalidPlan("scheduled run count overflowed".into()))?;
    if runs > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(format!(
            "the research plan schedules {runs} runs, above the limit of {}",
            limits.max_scheduled_runs
        )));
    }
    Ok(runs)
}

/// The span of stored data a batch reads: the earliest warmup start any point needs for any window on any symbol, through the latest window end.
///
/// A caller that loads market data itself, such as a service, loads exactly this range so every run has its full derived warmup. `None` means the plan or family produces no runs.
pub fn batch_data_range<F>(
    plan: &ResearchPlan,
    family: &F,
) -> Result<Option<(NaiveDateTime, NaiveDateTime)>, ResearchError>
where
    F: StrategyFamily,
{
    batch_data_range_with_limits(plan, family, ResearchAdmissionLimits::default())
}

pub fn batch_data_range_with_limits<F>(
    plan: &ResearchPlan,
    family: &F,
    limits: ResearchAdmissionLimits,
) -> Result<Option<(NaiveDateTime, NaiveDateTime)>, ResearchError>
where
    F: StrategyFamily,
{
    plan.validate()?;
    ResearchAdmissionLimits::new(limits.max_window_pairs, limits.max_scheduled_runs)?;
    let pairs = plan.window_plan.pairs_with_limit(limits.max_window_pairs)?;
    let windows = pairs
        .iter()
        .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
        .collect::<Vec<_>>();
    let Some(end) = windows.iter().map(|window| window.to()).max() else {
        return Ok(None);
    };
    let mut start: Option<NaiveDateTime> = None;
    for point in family.points() {
        for symbol in &plan.symbols {
            let strategy = ConfiguredStrategy::compile(
                family.config(&point),
                &family.library(),
                "range",
                symbol.as_str(),
            )
            .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
            let bindings = family
                .bindings(symbol, &point, strategy.input_requirements())
                .map_err(ResearchError::InvalidDocument)?;
            for window in &windows {
                let window_start = bindings
                    .warmup_start(window.from())
                    .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                start = Some(start.map_or(window_start, |current| current.min(window_start)));
            }
        }
    }
    Ok(start.map(|start| (start, end)))
}

fn validate_parameter_labels(
    family_id: &str,
    bindings: &[ParameterBinding],
    plan: &ResearchPlan,
) -> Result<(), ResearchError> {
    let common_tags = &plan.config.run_tags;
    // A portfolio run labels every position with its instance, so that name is reserved as well.
    let portfolio_reserved = plan.portfolio.as_ref().map(|_| INSTANCE_POSITION_TAG);
    let mut labels = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let label = binding_labels(binding);
        for key in label.keys().chain(common_tags.keys()) {
            if RESERVED_TAGS.contains(&key.as_str()) || portfolio_reserved == Some(key.as_str()) {
                return Err(ResearchError::InvalidPlan(format!(
                    "run tag '{key}' is reserved by the research runner"
                )));
            }
        }
        let total = common_tags.len() + label.len() + RESERVED_TAGS.len();
        if total > qs_backtest::runner::MAX_RUN_TAGS {
            return Err(ResearchError::InvalidPlan(format!(
                "composed run tags require {total} entries, exceeding {}",
                qs_backtest::runner::MAX_RUN_TAGS
            )));
        }
        labels.push(label);
    }
    labels.sort();
    let distinct = labels.len();
    labels.dedup();
    if labels.len() != distinct {
        return Err(ResearchError::InvalidPlan(format!(
            "family '{family_id}' binds two parameter points identically"
        )));
    }
    Ok(())
}

fn binding_labels(binding: &ParameterBinding) -> BTreeMap<String, String> {
    binding
        .iter()
        .map(|(name, value)| (name.clone(), parameter_value_label(value)))
        .collect()
}

struct RunControl<'a> {
    is_cancelled: &'a (dyn Fn() -> bool + Sync),
    on_progress: &'a (dyn Fn(BatchProgress) + Sync),
    completed: std::sync::atomic::AtomicUsize,
    total: usize,
}

impl RunControl<'_> {
    fn completed_one(&self) {
        let completed = self
            .completed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        (self.on_progress)(BatchProgress {
            completed_runs: completed,
            total_runs: self.total,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn run_in_parallel<F>(
    plan: &ResearchPlan,
    family: &F,
    specs: &[RunSpec<'_, F::Params>],
    events: &BTreeMap<String, SymbolEvents>,
    points_total: usize,
    workers: usize,
    control: &RunControl<'_>,
) -> Vec<RunRecord>
where
    F: StrategyFamily,
{
    let next = std::sync::atomic::AtomicUsize::new(0);
    let collected = std::sync::Mutex::new(Vec::with_capacity(specs.len()));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    if (control.is_cancelled)() {
                        break;
                    }
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(spec) = specs.get(index) else {
                        break;
                    };
                    let record = evaluate_run(plan, family, spec, events, points_total);
                    collected
                        .lock()
                        .expect("a worker panicked while holding the result lock")
                        .push(record);
                    control.completed_one();
                }
            });
        }
    });

    collected
        .into_inner()
        .expect("every worker has finished by now")
}

fn evaluate_run<F>(
    plan: &ResearchPlan,
    family: &F,
    spec: &RunSpec<'_, F::Params>,
    events: &BTreeMap<String, SymbolEvents>,
    points_total: usize,
) -> RunRecord
where
    F: StrategyFamily,
{
    let params = binding_labels(&spec.candidate.binding);
    let tags = run_tags(plan, &params, spec);
    let make_row = |status: RunStatus| ResearchRow {
        family_id: family.family_id().to_owned(),
        symbol: spec.symbol.to_owned(),
        params: params.clone(),
        window: spec.window.label().to_owned(),
        status,
        data_mode: spec.data_mode.to_owned(),
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
    };
    if let Some(portfolio) = &plan.portfolio {
        return match run_portfolio(plan, portfolio, family, spec, events, &tags) {
            Ok(executed) => {
                let mut row = make_row(RunStatus::Completed);
                fill_metrics(&mut row, &executed.run.result, spec.window);
                if let Some(supervisor) = executed.supervisor {
                    row.rejected_entries = Some(supervisor.rejected_entries());
                    row.halt_minutes = Some(supervisor.halt_minutes(spec.window.to()));
                }
                RunRecord {
                    run_index: spec.run_index,
                    row,
                    positions: executed.run.result.provider_positions,
                    binding_snapshots: executed.run.binding_snapshots,
                    coverage: Some(executed.run.coverage),
                }
            }
            Err(failure) => failed_record(spec.run_index, make_row(failure.into())),
        };
    }

    let Some(symbol_events) = events.get(spec.symbol) else {
        return failed_record(
            spec.run_index,
            make_row(
                RunFailure::Replay(format!("no market data supplied for '{}'", spec.symbol)).into(),
            ),
        );
    };

    match run_one(plan, family, spec, symbol_events, &tags) {
        Ok(executed) => {
            let mut row = make_row(RunStatus::Completed);
            fill_metrics(&mut row, &executed.result, spec.window);
            RunRecord {
                run_index: spec.run_index,
                row,
                positions: executed.result.provider_positions,
                binding_snapshots: executed.binding_snapshots,
                coverage: Some(executed.coverage),
            }
        }
        Err(failure) => failed_record(spec.run_index, make_row(failure.into())),
    }
}

fn failed_record(run_index: usize, row: ResearchRow) -> RunRecord {
    RunRecord {
        run_index,
        row,
        positions: Vec::new(),
        binding_snapshots: BTreeMap::new(),
        coverage: None,
    }
}

/// Name how a symbol's primary events were recorded, because a table built from stored bars and one built from ticks do not mean the same thing.
fn data_mode(symbol: &str, events: &SymbolEvents) -> Result<&'static str, ResearchError> {
    let mut ticks = false;
    let mut bars = false;
    for event in events.iter().filter(|event| event.metadata.roles.primary) {
        match event.event {
            MarketEvent::Tick { .. } => ticks = true,
            MarketEvent::Bar { .. } => bars = true,
        }
    }
    match (ticks, bars) {
        (true, true) => Err(ResearchError::InvalidPlan(format!(
            "events for '{symbol}' mix ticks and stored bars"
        ))),
        (false, true) => Ok("bars"),
        _ => Ok(DEFAULT_DATA_MODE),
    }
}

fn run_tags<P>(
    plan: &ResearchPlan,
    params: &BTreeMap<String, String>,
    spec: &RunSpec<'_, P>,
) -> BTreeMap<String, String> {
    let mut tags = plan.config.run_tags.clone();
    tags.extend(params.clone());
    tags.insert("window".into(), spec.window.label().to_owned());
    tags.insert("symbol".into(), spec.symbol.to_owned());
    tags.insert("data_mode".into(), spec.data_mode.to_owned());
    tags
}

fn run_one<F>(
    plan: &ResearchPlan,
    family: &F,
    spec: &RunSpec<'_, F::Params>,
    events: &SymbolEvents,
    tags: &BTreeMap<String, String>,
) -> Result<ExecutedRun, RunFailure>
where
    F: StrategyFamily,
{
    let instance_id = format!("p{}", spec.point_index);
    let config_document = spec.candidate.document.clone();
    let strategy_id = config_document.strategy_id.clone();
    let strategy = ConfiguredStrategy::compile(
        config_document,
        &family.library(),
        instance_id.clone(),
        spec.symbol,
    )
    .map_err(|error| RunFailure::Compile(error.to_string()))?;

    let bindings = family
        .bindings(
            spec.symbol,
            &spec.candidate.point,
            strategy.input_requirements(),
        )
        .map_err(RunFailure::Bind)?;
    let binding_snapshots =
        BTreeMap::from([(spec.symbol.to_owned(), snapshot_bindings(&bindings))]);
    let warmup_start = warmup_start(&bindings, spec.window.from())?;
    let descriptor = StrategyDescriptor::new(
        StrategyId::new(strategy_id.clone())
            .map_err(|error| RunFailure::Compile(error.to_string()))?,
        instance_id,
        strategy_id,
    )
    .map_err(|error| RunFailure::Compile(error.to_string()))?;

    let mut adapter = BacktestConfiguredStrategyAdapter::new(
        strategy,
        descriptor,
        bindings,
        plan.decision_latency_ms,
    )
    .map_err(|error| RunFailure::Bind(error.to_string()))?;

    let slice = slice_events(events, spec.window, warmup_start);
    let coverage = input_coverage(&slice, spec.window)?;
    let mut feed = VecFeed::from_feed_events(slice);

    let mut config = plan.config.clone();
    config.run_tags = tags.clone();
    config.close_on_finish = true;

    let analysis = AnalysisPipeline::new(
        Vec::new(),
        ObservationStoreLimits::default(),
        AnnotationLimits::default(),
    )
    .map_err(|error| RunFailure::Bind(error.to_string()))?;

    let mut runner = BacktestRunner::new_future(config, plan.future.clone())
        .with_evaluation_options(plan.evaluation.clone());
    if let Some(profiles) = plan.entry_profiles.clone() {
        runner = runner.with_entry_profiles(profiles);
    }
    let result = runner
        .run_configured_strategy_future(&mut feed, &mut adapter, analysis, plan.retention, None)
        .map(|result| result.replay)
        .map_err(|error| RunFailure::Replay(error.to_string()))?;
    let mut coverage = coverage_with_result(coverage, &result);
    coverage.first_ready_at = adapter.first_ready_at();
    coverage
        .unavailable
        .retain(|capability| *capability != crate::UnavailableCoverage::FirstReady);
    Ok(ExecutedRun {
        coverage,
        result,
        binding_snapshots,
    })
}

/// Replay every plan symbol as one instance of the point against one account over the window, each instance reading from its own derived warmup start.
fn run_portfolio<F>(
    plan: &ResearchPlan,
    portfolio: &crate::plan::PortfolioPlan,
    family: &F,
    spec: &RunSpec<'_, F::Params>,
    events: &BTreeMap<String, SymbolEvents>,
    tags: &BTreeMap<String, String>,
) -> Result<ExecutedPortfolio, RunFailure>
where
    F: StrategyFamily,
{
    let mut instances = Vec::with_capacity(plan.symbols.len());
    let mut feed_events = Vec::new();
    let mut binding_snapshots = BTreeMap::new();
    for symbol in &plan.symbols {
        let symbol_events = events
            .get(symbol.as_str())
            .ok_or_else(|| RunFailure::Replay(format!("no market data supplied for '{symbol}'")))?;
        let config_document = spec.candidate.document.clone();
        let strategy_id = config_document.strategy_id.clone();
        // The symbol is the instance identity, so each position's instance tag names the symbol it traded.
        let strategy = ConfiguredStrategy::compile(
            config_document,
            &family.library(),
            symbol.clone(),
            symbol.as_str(),
        )
        .map_err(|error| RunFailure::Compile(error.to_string()))?;
        let bindings = family
            .bindings(symbol, &spec.candidate.point, strategy.input_requirements())
            .map_err(RunFailure::Bind)?;
        binding_snapshots.insert(symbol.clone(), snapshot_bindings(&bindings));
        let start = warmup_start(&bindings, spec.window.from())?;
        let descriptor = StrategyDescriptor::new(
            StrategyId::new(strategy_id.clone())
                .map_err(|error| RunFailure::Compile(error.to_string()))?,
            format!("p{}", spec.point_index),
            strategy_id,
        )
        .map_err(|error| RunFailure::Compile(error.to_string()))?;
        let adapter = BacktestConfiguredStrategyAdapter::new(
            strategy,
            descriptor,
            bindings,
            plan.decision_latency_ms,
        )
        .map_err(|error| RunFailure::Bind(error.to_string()))?;
        let analysis = AnalysisPipeline::new(
            Vec::new(),
            ObservationStoreLimits::default(),
            AnnotationLimits::default(),
        )
        .map_err(|error| RunFailure::Bind(error.to_string()))?;
        instances.push(
            ConfiguredInstance::new(adapter, analysis)
                .with_entry_profiles(plan.entry_profiles.clone().unwrap_or_default())
                .with_feed_from(start),
        );
        feed_events.extend(slice_events(symbol_events, spec.window, start));
    }
    let coverage = input_coverage(&feed_events, spec.window)?;
    let mut feed = VecFeed::from_feed_events(feed_events);

    let mut config = plan.config.clone();
    config.run_tags = tags.clone();
    config.close_on_finish = true;
    let supervisor = portfolio
        .supervisor()
        .map_err(|error| RunFailure::Replay(error.to_string()))?;
    let result = BacktestRunner::new_future(config, plan.future.clone())
        .with_evaluation_options(plan.evaluation.clone())
        .run_portfolio_future(&mut feed, instances, supervisor, plan.retention)
        .map_err(|error| RunFailure::Replay(error.to_string()))?;
    let replay = result.replay;
    Ok(ExecutedPortfolio {
        run: ExecutedRun {
            coverage: coverage_with_result(coverage, &replay),
            result: replay,
            binding_snapshots,
        },
        supervisor: result.supervisor,
    })
}

fn warmup_start(
    bindings: &qs_backtest::ConfiguredHistoricalBindings,
    from: NaiveDateTime,
) -> Result<NaiveDateTime, RunFailure> {
    bindings
        .warmup_start(from)
        .map_err(|error| RunFailure::Bind(error.to_string()))
}

pub(crate) fn input_coverage(
    events: &[FeedEvent],
    window: &DataWindow,
) -> Result<RunCoverage, RunFailure> {
    let primary = events
        .iter()
        .filter(|event| event.metadata.roles.primary)
        .map(|event| event.event.ts());
    let mut first = None;
    let mut last = None;
    let mut count = 0_u64;
    for timestamp in primary {
        first = Some(first.map_or(timestamp, |current: NaiveDateTime| current.min(timestamp)));
        last = Some(last.map_or(timestamp, |current: NaiveDateTime| current.max(timestamp)));
        count = count
            .checked_add(1)
            .ok_or_else(|| RunFailure::Replay("primary event coverage overflowed".into()))?;
    }
    Ok(RunCoverage {
        first_input_at: first,
        last_input_at: last,
        processed_primary_events: count,
        permitted_from: window.from(),
        permitted_to: window.to(),
        first_ready_at: None,
        unavailable: vec![
            crate::UnavailableCoverage::FirstReady,
            crate::UnavailableCoverage::PerSourceValidity,
            crate::UnavailableCoverage::MissingBuckets,
        ],
        forced_closes: 0,
    })
}

pub(crate) fn coverage_with_result(
    mut coverage: RunCoverage,
    result: &BacktestResult,
) -> RunCoverage {
    coverage.forced_closes = result
        .completed_positions
        .iter()
        .filter(|position| position.close_reasons.contains(&CloseReason::EndOfData))
        .count()
        .try_into()
        .unwrap_or(u64::MAX);
    coverage
}

pub(crate) fn slice_events(
    events: &SymbolEvents,
    window: &DataWindow,
    warmup_start: NaiveDateTime,
) -> Vec<FeedEvent> {
    let start = warmup_start;
    let end = window.to();
    let lower = partition_point(events, |ts| ts < start);
    let upper = partition_point(events, |ts| ts < end);
    events[lower..upper].to_vec()
}

fn partition_point(
    events: &SymbolEvents,
    mut predicate: impl FnMut(NaiveDateTime) -> bool,
) -> usize {
    let mut low = 0usize;
    let mut high = events.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if predicate(events[mid].event.ts()) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

pub(crate) fn fill_metrics(row: &mut ResearchRow, result: &BacktestResult, window: &DataWindow) {
    row.net_pnl = result.total_pnl;
    row.gross_pnl = result.gross_pnl;
    row.commission = result.total_commission;
    row.swap = result.total_swap;
    row.max_drawdown_pct = result.max_drawdown_pct;
    row.positions = result.completed_positions.len();
    row.forced_closes = result
        .completed_positions
        .iter()
        .filter(|position| position.close_reasons.contains(&CloseReason::EndOfData))
        .count();
    row.entries_before_window = result
        .completed_positions
        .iter()
        .filter(|position| position.open_ts < window.from())
        .count();

    if let Some(evaluation) = result.provider_evaluation.as_ref() {
        if let Some(performance) = evaluation.position_performance.as_ref() {
            row.win_rate = performance.win_rate.value;
        }
        if let Some(r_metrics) = evaluation.r_metrics.as_ref() {
            row.expectancy_r = r_metrics.mean_r.value;
            if let Some(quantiles) = r_metrics.quantiles.value.as_ref() {
                row.r_p05 = Some(quantiles.p05);
                row.r_p50 = Some(quantiles.p50);
                row.r_p95 = Some(quantiles.p95);
            }
        }
        if let Some(excursions) = evaluation.excursions.as_ref() {
            row.avg_favorable_r = excursions.mean_favorable_r.value;
            row.avg_adverse_r = excursions.mean_adverse_r.value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::EmaCrossFamily;
    use crate::window::DataWindow;
    use chrono::{Duration, NaiveDate};
    use qs_backtest::Timeframe;
    use qs_backtest::runner::BacktestConfig;

    fn ts(minute: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::minutes(minute)
    }

    #[test]
    fn a_run_is_labelled_with_the_batch_the_point_and_the_window() {
        let mut config = BacktestConfig::default();
        config.run_tags.insert("dataset".into(), "synthetic".into());
        let plan = crate::plan::ResearchPlan::new(
            vec!["EURUSD".into()],
            crate::window::WindowPlan::Fixed {
                in_sample: DataWindow::new("is", ts(0), ts(100)).unwrap(),
                out_of_sample: DataWindow::new("oos", ts(100), ts(200)).unwrap(),
            },
            config,
        );
        let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap());
        let point = family.points()[0];
        let candidate = AdmittedPoint {
            binding: family.parameter_binding(&point),
            document: family.config(&point),
            point,
        };
        let window = DataWindow::new("is", ts(0), ts(100)).unwrap();
        let spec = RunSpec {
            run_index: 0,
            candidate: &candidate,
            point_index: 0,
            symbol: "EURUSD",
            window: &window,
            data_mode: DEFAULT_DATA_MODE,
        };
        let params = binding_labels(&candidate.binding);

        let tags = run_tags(&plan, &params, &spec);
        assert_eq!(tags["dataset"], "synthetic");
        assert_eq!(tags["ema_fast"], "3");
        assert_eq!(tags["ema_slow"], "8");
        assert_eq!(tags["atr_stop"], "1");
        assert_eq!(tags["entry"], "cross");
        assert_eq!(tags["window"], "is");
        assert_eq!(tags["symbol"], "EURUSD");
        assert_eq!(tags["data_mode"], "ticks");
    }

    #[test]
    fn unaligned_windows_start_warmup_on_a_complete_bucket_boundary() {
        let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
            .with_atr_period(5);
        let point = family.points()[0];
        let strategy = ConfiguredStrategy::compile(
            family.config(&point),
            &family.library(),
            "instance",
            "EURUSD",
        )
        .unwrap();
        let bindings = family
            .bindings("EURUSD", &point, strategy.input_requirements())
            .unwrap();
        let from = ts(40) + Duration::seconds(30);
        assert_eq!(warmup_start(&bindings, from).unwrap(), ts(33));
    }

    #[test]
    fn a_slice_uses_a_half_open_evaluation_window() {
        let events: SymbolEvents = (0..100)
            .map(|minute| {
                qs_backtest::data_feed::FeedEvent::new(
                    qs_backtest::data_feed::MarketEvent::Tick {
                        symbol: "EURUSD".into(),
                        ts: ts(minute),
                        bid: 1.0,
                        ask: 1.1,
                    },
                    qs_backtest::data_feed::EventMetadata::new(
                        qs_backtest::data_feed::SeriesRoles::PRIMARY,
                        0,
                        minute as u64,
                    ),
                )
            })
            .collect::<Vec<_>>()
            .into();
        let window = DataWindow::new("w", ts(40), ts(60)).unwrap();
        let slice = slice_events(&events, &window, ts(30));

        assert_eq!(slice.first().unwrap().event.ts(), ts(30));
        assert_eq!(slice.last().unwrap().event.ts(), ts(59));
        assert_eq!(slice.len(), 30);
    }
}
