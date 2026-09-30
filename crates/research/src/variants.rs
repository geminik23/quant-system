use crate::{
    BatchProgress, ExperimentOptions, PortfolioPlan, ResearchAdmissionLimits, ResearchBatch,
    ResearchError, ResearchPlan, ResearchTable, StrategyFamily, SymbolEvents,
    run_batch_controlled_with_experiment,
};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::{
    ConfiguredCalendarInput, ConfiguredHistoricalBindings, FutureQuoteConfig, PreparedEntryProfiles,
};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone)]
pub struct ExecutionVariant {
    pub id: String,
    pub backtest: BacktestConfig,
    pub future: FutureQuoteConfig,
    pub profiles: Option<PreparedEntryProfiles>,
    pub portfolio: Option<PortfolioPlan>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionVariantLimits {
    pub max_variants: usize,
    pub max_scheduled_runs: usize,
}
impl ExecutionVariantLimits {
    pub fn new(max_variants: usize, max_scheduled_runs: usize) -> Result<Self, ResearchError> {
        if max_variants == 0 || max_scheduled_runs == 0 {
            Err(ResearchError::InvalidPlan(
                "execution variant limits must be positive".into(),
            ))
        } else {
            Ok(Self {
                max_variants,
                max_scheduled_runs,
            })
        }
    }
}
#[derive(Debug, Clone)]
pub struct VariantResearchBatch {
    batches: BTreeMap<String, ResearchBatch>,
    table: ResearchTable,
}
impl VariantResearchBatch {
    pub fn batches(&self) -> &BTreeMap<String, ResearchBatch> {
        &self.batches
    }
    pub fn table(&self) -> &ResearchTable {
        &self.table
    }

    pub fn combined(&self) -> Result<ResearchBatch, ResearchError> {
        self.combined_as("execution_variant", "execution_variants")
    }

    pub fn combined_portfolios(&self) -> Result<ResearchBatch, ResearchError> {
        self.combined_as("portfolio_candidate", "portfolio_candidates")
    }

    fn combined_as(
        &self,
        parameter_name: &str,
        snapshot_name: &str,
    ) -> Result<ResearchBatch, ResearchError> {
        let Some((_, first)) = self.batches.first_key_value() else {
            return Err(ResearchError::InvalidPlan("variant batch is empty".into()));
        };
        let mut experiment = first.experiment_recipe().clone();
        let variant_recipes = self.batches.iter().map(|(id, batch)| {
            serde_json::json!({"id": id, "experiment": batch.experiment_recipe()})
        }).collect::<Vec<_>>();
        experiment.portfolio = Some(serde_json::json!({snapshot_name: variant_recipes}));
        let mut documents = Vec::new();
        let mut candidates = Vec::new();
        let mut runs = Vec::new();
        let mut positions = Vec::new();
        for (variant_id, batch) in &self.batches {
            let mut ordinal_map = BTreeMap::new();
            for (index, recipe) in batch.candidate_recipes().iter().enumerate() {
                let ordinal = u64::try_from(candidates.len()).map_err(|_| {
                    ResearchError::InvalidPlan("combined candidate ordinal overflowed".into())
                })?;
                ordinal_map.insert(recipe.ordinal, ordinal);
                let mut recipe = recipe.clone();
                recipe.ordinal = ordinal;
                recipe.parameters.insert(
                    parameter_name.into(),
                    qs_strategy::ParameterValue::Choice(variant_id.clone()),
                );
                candidates.push(recipe);
                documents.push(batch.bound_document(index).cloned());
            }
            for recipe in batch.run_recipes() {
                let mut recipe = recipe.clone();
                recipe.ordinal = u64::try_from(runs.len()).map_err(|_| {
                    ResearchError::InvalidPlan("combined run ordinal overflowed".into())
                })?;
                recipe.candidate_ordinal = ordinal_map[&recipe.candidate_ordinal];
                runs.push(recipe);
            }
            for position in batch.position_outcomes() {
                let mut position = position.clone();
                position.id = format!("variant:{variant_id}|{}", position.id);
                if let Some(trade_id) = position.trade_id.as_mut() {
                    *trade_id = format!("variant:{variant_id}|{trade_id}");
                }
                positions.push(position);
            }
        }
        experiment.candidate_count = candidates.len();
        let mut rows = self.table.rows().to_vec();
        for row in &mut rows {
            row.points_total = candidates.len();
        }
        Ok(ResearchBatch {
            table: ResearchTable::new(rows),
            positions,
            bound_documents: documents,
            experiment_recipe: experiment,
            candidate_recipes: candidates,
            run_recipes: runs,
        })
    }
}
pub fn run_execution_variants<F: StrategyFamily>(
    base: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    variants: &[ExecutionVariant],
    limits: ExecutionVariantLimits,
    options: ExperimentOptions,
) -> Result<VariantResearchBatch, ResearchError> {
    run_execution_variants_controlled(
        base,
        family,
        events,
        variants,
        limits,
        options,
        &|| false,
        &|_| {},
    )
}

#[allow(clippy::too_many_arguments)]
pub fn run_execution_variants_controlled<F: StrategyFamily>(
    base: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
    variants: &[ExecutionVariant],
    limits: ExecutionVariantLimits,
    options: ExperimentOptions,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<VariantResearchBatch, ResearchError> {
    limits.new_checked()?;
    if is_cancelled() {
        return Err(ResearchError::Cancelled);
    }
    if variants.is_empty() || variants.len() > limits.max_variants {
        return Err(ResearchError::InvalidPlan(
            "execution variant count is outside limits".into(),
        ));
    }
    let mut ids = BTreeSet::new();
    for variant in variants {
        validate_id(&variant.id)?;
        if !ids.insert(variant.id.clone()) {
            return Err(ResearchError::InvalidPlan(
                "duplicate execution variant ID".into(),
            ));
        }
    }
    let points = family.points().len();
    let pairs = base
        .window_plan
        .pairs_with_limit(limits.max_scheduled_runs)?
        .len();
    let symbols = if variants.iter().any(|v| v.portfolio.is_some()) {
        1
    } else {
        base.symbols.len()
    };
    let runs = variants
        .len()
        .checked_mul(points)
        .and_then(|v| v.checked_mul(pairs))
        .and_then(|v| v.checked_mul(2))
        .and_then(|v| v.checked_mul(symbols))
        .ok_or_else(|| {
            ResearchError::InvalidPlan("execution variant run count overflowed".into())
        })?;
    if runs > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(
            "execution variant run count exceeds limit".into(),
        ));
    }
    let mut batches = BTreeMap::new();
    let mut rows = Vec::new();
    let runs_per_variant = runs / variants.len();
    for (variant_index, variant) in variants.iter().enumerate() {
        if is_cancelled() {
            return Err(ResearchError::Cancelled);
        }
        let mut plan = base.clone();
        plan.config = variant.backtest.clone();
        plan.config
            .run_tags
            .insert("execution_variant".into(), variant.id.clone());
        plan.future = variant.future.clone();
        plan.entry_profiles = variant.profiles.clone();
        plan.portfolio = variant.portfolio.clone();
        let mut variant_options = options.clone();
        if let Some(revision) = variant_options.caller_revision.as_mut() {
            *revision = format!("{revision}:{}", variant.id)
        }
        let progress = |value: BatchProgress| {
            on_progress(BatchProgress {
                completed_runs: variant_index * runs_per_variant + value.completed_runs,
                total_runs: runs,
            });
        };
        let mut batch = match run_batch_controlled_with_experiment(
            &plan,
            family,
            events,
            ResearchAdmissionLimits::new(pairs.max(1), runs_per_variant.max(1))?,
            variant_options,
            is_cancelled,
            &progress,
        ) {
            Err(ResearchError::CancelledWithPartial(_)) => {
                return Err(ResearchError::Cancelled);
            }
            result => result?,
        };
        if is_cancelled() {
            return Err(ResearchError::Cancelled);
        }
        let mut tagged_rows = batch.rows().to_vec();
        for row in &mut tagged_rows {
            row.params
                .insert("execution_variant".into(), variant.id.clone());
        }
        batch.table = ResearchTable::new(tagged_rows);
        rows.extend(batch.rows().iter().cloned());
        batches.insert(variant.id.clone(), batch);
    }
    Ok(VariantResearchBatch {
        batches,
        table: ResearchTable::new(rows),
    })
}
impl ExecutionVariantLimits {
    fn new_checked(self) -> Result<Self, ResearchError> {
        Self::new(self.max_variants, self.max_scheduled_runs)
    }
}
#[derive(Debug, Clone)]
pub struct HeterogeneousInstanceSpec {
    pub instance_id: String,
    pub symbol: String,
    pub document: qs_strategy::StrategyConfig,
    pub geometry: Vec<crate::SeriesGeometry>,
    pub historical_inputs: Vec<ConfiguredCalendarInput>,
    pub profiles: Option<PreparedEntryProfiles>,
}

fn configured_instance_bindings(
    instance: &HeterogeneousInstanceSpec,
    requirements: &qs_strategy::ConfiguredStrategyRequirements,
) -> Result<ConfiguredHistoricalBindings, ResearchError> {
    let base = ConfiguredHistoricalBindings::from_geometry(instance.geometry.clone(), requirements)
        .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
    let (sources, mut named, volume) = base.into_parts();
    let required = requirements
        .named_inputs
        .iter()
        .map(|requirement| requirement.name.as_str())
        .collect::<BTreeSet<_>>();
    for input in &instance.historical_inputs {
        if !required.contains(input.name.as_str()) {
            continue;
        }
        if named.iter().any(|binding| binding.name() == input.name) {
            return Err(ResearchError::InvalidPlan(format!(
                "named input '{}' has more than one projector",
                input.name
            )));
        }
        named.push(
            input
                .binding()
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?,
        );
    }
    Ok(ConfiguredHistoricalBindings::new(sources, named, volume))
}

fn configured_instance_history_start(
    instance: &HeterogeneousInstanceSpec,
    evaluation_start: chrono::NaiveDateTime,
) -> Result<chrono::NaiveDateTime, ResearchError> {
    instance
        .historical_inputs
        .iter()
        .try_fold(evaluation_start, |start, input| {
            input
                .history_start(evaluation_start)
                .map(|candidate| start.min(candidate))
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))
        })
}

fn calendar_input_snapshot(
    input: &ConfiguredCalendarInput,
    symbol: &str,
    owner: &str,
) -> crate::InputProjectorSnapshot {
    crate::InputProjectorSnapshot {
        owner: owner.into(),
        symbol: symbol.into(),
        name: input.name.clone(),
        kind: "calendar".into(),
        configuration: serde_json::json!({
            "source": input.source,
            "calendar": input.calendar,
            "input": input.input,
            "limits": input.limits,
        }),
    }
}

#[derive(Debug, Clone)]
pub struct HeterogeneousPortfolioCandidate {
    pub id: String,
    pub instances: Vec<HeterogeneousInstanceSpec>,
    pub portfolio: PortfolioPlan,
}

#[derive(Debug, Clone)]
pub struct HeterogeneousDirectInstanceSpec {
    pub instance_id: String,
    pub symbol: String,
    pub factory_name: String,
    pub point: crate::DirectFactoryPoint,
    pub profiles: Option<PreparedEntryProfiles>,
}

#[derive(Debug, Clone)]
pub struct MixedHeterogeneousPortfolioCandidate {
    pub id: String,
    pub configured: Vec<HeterogeneousInstanceSpec>,
    pub direct: Vec<HeterogeneousDirectInstanceSpec>,
    pub portfolio: PortfolioPlan,
}

pub fn run_mixed_heterogeneous_portfolios(
    base: &ResearchPlan,
    candidates: &[MixedHeterogeneousPortfolioCandidate],
    factories: &BTreeMap<String, &dyn crate::DirectResearchFactory>,
    events: &BTreeMap<String, SymbolEvents>,
    limits: ExecutionVariantLimits,
) -> Result<VariantResearchBatch, ResearchError> {
    use crate::recipe::{
        CandidateRecipe, EndpointBounds, ExperimentRecipe, RegisteredFactorySelection, RunRecipe,
        snapshot_bindings,
    };
    use qs_backtest::{
        AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
        ConfiguredInstance, DirectPortfolioInstance, ObservationStoreLimits, StrategyDescriptor,
        StrategyId, StrategyRetentionLimits, VecFeed,
    };
    limits.new_checked()?;
    if candidates.is_empty() || candidates.len() > limits.max_variants {
        return Err(ResearchError::InvalidPlan(
            "mixed heterogeneous candidate count is outside limits".into(),
        ));
    }
    let pairs = base
        .window_plan
        .pairs_with_limit(limits.max_scheduled_runs)?;
    let windows = pairs
        .iter()
        .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
        .collect::<Vec<_>>();
    let runs = candidates
        .len()
        .checked_mul(windows.len())
        .ok_or_else(|| ResearchError::InvalidPlan("mixed portfolio run count overflowed".into()))?;
    if runs > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(
            "mixed portfolio run count exceeds limit".into(),
        ));
    }

    let mut batches = BTreeMap::new();
    let mut combined_rows = Vec::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        validate_id(&candidate.id)?;
        if candidate.configured.is_empty() || candidate.direct.is_empty() {
            return Err(ResearchError::InvalidPlan(
                "mixed portfolio needs configured and direct instances".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        for instance_id in candidate
            .configured
            .iter()
            .map(|instance| instance.instance_id.as_str())
            .chain(
                candidate
                    .direct
                    .iter()
                    .map(|instance| instance.instance_id.as_str()),
            )
        {
            validate_id(instance_id)?;
            if !identities.insert(instance_id.to_owned()) {
                return Err(ResearchError::InvalidPlan(
                    "duplicate mixed portfolio instance ID".into(),
                ));
            }
        }
        let mut rows = Vec::new();
        let mut positions = Vec::new();
        let mut run_recipes = Vec::new();
        let mut series_by_instance = BTreeMap::new();
        let mut factory_selections = Vec::new();
        let mut input_projectors = Vec::new();
        for (window_index, window) in windows.iter().enumerate() {
            let mut configured = Vec::new();
            let mut direct = Vec::new();
            let mut feed_starts = BTreeMap::<String, chrono::NaiveDateTime>::new();
            for instance in &candidate.configured {
                let strategy = qs_strategy::ConfiguredStrategy::compile(
                    instance.document.clone(),
                    &qs_strategy::MaterialLibrary::builtins(),
                    instance.instance_id.clone(),
                    instance.symbol.clone(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                let bindings =
                    configured_instance_bindings(instance, strategy.input_requirements())?;
                for input in &instance.historical_inputs {
                    let snapshot =
                        calendar_input_snapshot(input, &instance.symbol, &instance.instance_id);
                    if !input_projectors.contains(&snapshot) {
                        input_projectors.push(snapshot);
                    }
                }
                series_by_instance
                    .entry(instance.instance_id.clone())
                    .or_insert_with(|| snapshot_bindings(&bindings));
                let start = bindings
                    .warmup_start(window.from())
                    .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?
                    .min(configured_instance_history_start(instance, window.from())?);
                let descriptor = StrategyDescriptor::new(
                    StrategyId::new(instance.document.strategy_id.clone())
                        .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?,
                    instance.instance_id.clone(),
                    instance.document.title.clone(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                let mut adapter = BacktestConfiguredStrategyAdapter::new(
                    strategy,
                    descriptor,
                    bindings,
                    base.decision_latency_ms,
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                if !instance.historical_inputs.is_empty() {
                    adapter.set_evaluation_start(Some(window.from()));
                }
                let analysis = AnalysisPipeline::new(
                    vec![],
                    ObservationStoreLimits::default(),
                    AnnotationLimits::default(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                configured.push(
                    ConfiguredInstance::new(adapter, analysis)
                        .with_entry_profiles(instance.profiles.clone().unwrap_or_default())
                        .with_feed_from(start),
                );
                feed_starts
                    .entry(instance.symbol.clone())
                    .and_modify(|current| *current = (*current).min(start))
                    .or_insert(start);
            }
            for instance in &candidate.direct {
                let factory = factories.get(&instance.factory_name).ok_or_else(|| {
                    ResearchError::InvalidPlan(format!(
                        "unknown direct factory '{}'",
                        instance.factory_name
                    ))
                })?;
                let produced = factory.create(&instance.point, &instance.symbol, window)?;
                series_by_instance
                    .entry(instance.instance_id.clone())
                    .or_insert_with(|| {
                        produced
                            .series
                            .iter()
                            .map(crate::factory::series_snapshot)
                            .collect()
                    });
                let start = crate::factory::direct_warmup_start(&produced.series, window.from())?;
                direct.push(
                    DirectPortfolioInstance::new(
                        instance.instance_id.clone(),
                        produced.strategy,
                        produced.series,
                        produced.analysis,
                    )
                    .with_entry_profiles(instance.profiles.clone().unwrap_or_default())
                    .with_feed_from(start),
                );
                feed_starts
                    .entry(instance.symbol.clone())
                    .and_modify(|current| *current = (*current).min(start))
                    .or_insert(start);
                if window_index == 0 {
                    factory_selections.push(serde_json::json!({
                        "instance_id": instance.instance_id,
                        "symbol": instance.symbol,
                        "factory": RegisteredFactorySelection {
                            name: factory.factory_name().into(),
                            revision: factory.revision().into(),
                        },
                        "parameters": instance.point.binding,
                    }));
                }
            }
            let mut feed_events = Vec::new();
            for (symbol, start) in feed_starts {
                let source = events.get(&symbol).ok_or_else(|| {
                    ResearchError::InvalidPlan(format!("missing events for {symbol}"))
                })?;
                feed_events.extend(crate::runner::slice_events(source, window, start));
            }
            let coverage = crate::runner::input_coverage(&feed_events, window)
                .map_err(|error| ResearchError::InvalidPlan(format!("coverage: {error:?}")))?;
            let mut feed = VecFeed::from_feed_events(feed_events);
            let mut config = base.config.clone();
            config.close_on_finish = true;
            config
                .run_tags
                .insert("heterogeneous_candidate".into(), candidate.id.clone());
            config
                .run_tags
                .insert("window".into(), window.label().into());
            let result = BacktestRunner::new_future(config, base.future.clone())
                .with_evaluation_options(base.evaluation.clone())
                .run_mixed_portfolio_future(
                    &mut feed,
                    configured,
                    direct,
                    candidate.portfolio.supervisor()?,
                    StrategyRetentionLimits::default(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
            let replay = result.replay;
            let portfolio_symbol = candidate
                .configured
                .iter()
                .map(|instance| instance.symbol.as_str())
                .chain(
                    candidate
                        .direct
                        .iter()
                        .map(|instance| instance.symbol.as_str()),
                )
                .collect::<Vec<_>>()
                .join("+");
            let mut row = super::factory::empty_row(
                "mixed_heterogeneous",
                &portfolio_symbol,
                BTreeMap::from([("portfolio_candidate".into(), candidate.id.clone())]),
                window,
                candidates.len(),
            );
            crate::runner::fill_metrics(&mut row, &replay, window);
            combined_rows.push(row.clone());
            rows.push(row);
            let coverage = crate::runner::coverage_with_result(coverage, &replay);
            for mut position in replay.provider_positions {
                position.id = format!(
                    "candidate:{candidate_index}|run:{window_index}|{}",
                    position.id
                );
                if let Some(trade_id) = position.trade_id.as_mut() {
                    *trade_id =
                        format!("candidate:{candidate_index}|run:{window_index}|{trade_id}");
                }
                positions.push(position);
            }
            run_recipes.push(RunRecipe {
                ordinal: u64::try_from(window_index).unwrap(),
                candidate_ordinal: u64::try_from(candidate_index).unwrap(),
                window: window.label().into(),
                symbol: portfolio_symbol,
                data_mode: "ticks".into(),
                run_tags: BTreeMap::from([
                    ("heterogeneous_candidate".into(), candidate.id.clone()),
                    ("window".into(), window.label().into()),
                ]),
                from: window.from(),
                to: window.to(),
                coverage: Some(coverage),
            });
        }
        let document = serde_json::json!({
            "configured": candidate.configured.iter().map(|instance| serde_json::json!({
                "instance_id": instance.instance_id,
                "symbol": instance.symbol,
                "document": instance.document,
            })).collect::<Vec<_>>(),
            "direct": factory_selections,
        });
        let recipe = CandidateRecipe {
            ordinal: u64::try_from(candidate_index).unwrap(),
            family_id: "mixed_heterogeneous".into(),
            parameters: BTreeMap::from([(
                "portfolio_candidate".into(),
                qs_strategy::ParameterValue::Choice(candidate.id.clone()),
            )]),
            document: Some(document),
            registered_factory: None,
            series_by_symbol: BTreeMap::new(),
            series_by_instance,
            input_projectors,
            admission_error: None,
        };
        let experiment = ExperimentRecipe {
            experiment_id: None,
            candidate_count: candidates.len(),
            caller_revision: None,
            dataset_reference: None,
            endpoint_bounds: EndpointBounds::HalfOpen,
            ordered_symbols: candidate
                .configured
                .iter()
                .map(|instance| instance.symbol.clone())
                .chain(
                    candidate
                        .direct
                        .iter()
                        .map(|instance| instance.symbol.clone()),
                )
                .collect(),
            decision_latency_ms: base.decision_latency_ms,
            portfolio: Some(
                serde_json::to_value(&candidate.portfolio)
                    .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?,
            ),
            backtest: serde_json::to_value(&base.config).unwrap(),
            future: serde_json::to_value(&base.future).unwrap(),
            evaluation: serde_json::to_value(&base.evaluation).unwrap(),
            retention: serde_json::to_value(base.retention).unwrap(),
            research_retention: serde_json::json!({}),
            profiles: None,
        };
        batches.insert(
            candidate.id.clone(),
            ResearchBatch {
                table: ResearchTable::new(rows),
                positions,
                bound_documents: vec![None],
                experiment_recipe: experiment,
                candidate_recipes: vec![recipe],
                run_recipes,
            },
        );
    }
    Ok(VariantResearchBatch {
        batches,
        table: ResearchTable::new(combined_rows),
    })
}

pub fn run_heterogeneous_portfolios(
    base: &ResearchPlan,
    candidates: &[HeterogeneousPortfolioCandidate],
    events: &BTreeMap<String, SymbolEvents>,
    limits: ExecutionVariantLimits,
) -> Result<VariantResearchBatch, ResearchError> {
    run_heterogeneous_portfolios_controlled(base, candidates, events, limits, &|| false, &|_| {})
}

pub fn run_heterogeneous_portfolios_controlled(
    base: &ResearchPlan,
    candidates: &[HeterogeneousPortfolioCandidate],
    events: &BTreeMap<String, SymbolEvents>,
    limits: ExecutionVariantLimits,
    is_cancelled: &(dyn Fn() -> bool + Sync),
    on_progress: &(dyn Fn(BatchProgress) + Sync),
) -> Result<VariantResearchBatch, ResearchError> {
    use crate::recipe::{
        CandidateRecipe, EndpointBounds, ExperimentRecipe, RunRecipe, snapshot_bindings,
    };
    use qs_backtest::{
        AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
        ConfiguredInstance, ObservationStoreLimits, StrategyDescriptor, StrategyId,
        StrategyRetentionLimits, VecFeed,
    };
    limits.new_checked()?;
    if candidates.is_empty() || candidates.len() > limits.max_variants {
        return Err(ResearchError::InvalidPlan(
            "heterogeneous candidate count is outside limits".into(),
        ));
    }
    let pairs = base
        .window_plan
        .pairs_with_limit(limits.max_scheduled_runs)?;
    let windows = pairs
        .iter()
        .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
        .collect::<Vec<_>>();
    let runs = candidates
        .len()
        .checked_mul(windows.len())
        .ok_or_else(|| ResearchError::InvalidPlan("heterogeneous run count overflowed".into()))?;
    if runs > limits.max_scheduled_runs {
        return Err(ResearchError::InvalidPlan(
            "heterogeneous run count exceeds limit".into(),
        ));
    }
    let mut batches = BTreeMap::new();
    let mut combined = Vec::new();
    let mut completed_runs = 0usize;
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        validate_id(&candidate.id)?;
        if candidate.instances.is_empty() {
            return Err(ResearchError::InvalidPlan(
                "heterogeneous candidate needs instances".into(),
            ));
        }
        let mut instance_ids = BTreeSet::new();
        for instance in &candidate.instances {
            if !instance_ids.insert(instance.instance_id.clone()) {
                return Err(ResearchError::InvalidPlan(
                    "duplicate heterogeneous instance ID".into(),
                ));
            }
        }
        let mut rows = Vec::new();
        let mut positions = Vec::new();
        let mut run_recipes = Vec::new();
        let mut series_by_symbol = BTreeMap::new();
        let mut input_projectors = Vec::new();
        for (window_index, window) in windows.iter().enumerate() {
            if is_cancelled() {
                return Err(ResearchError::Cancelled);
            }
            let mut configured = Vec::new();
            let mut feed_starts = BTreeMap::<String, chrono::NaiveDateTime>::new();
            for instance in &candidate.instances {
                let strategy = qs_strategy::ConfiguredStrategy::compile(
                    instance.document.clone(),
                    &qs_strategy::MaterialLibrary::builtins(),
                    instance.instance_id.clone(),
                    instance.symbol.clone(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                let bindings =
                    configured_instance_bindings(instance, strategy.input_requirements())?;
                for input in &instance.historical_inputs {
                    let snapshot =
                        calendar_input_snapshot(input, &instance.symbol, &instance.instance_id);
                    if !input_projectors.contains(&snapshot) {
                        input_projectors.push(snapshot);
                    }
                }
                series_by_symbol.insert(instance.instance_id.clone(), snapshot_bindings(&bindings));
                let start = bindings
                    .warmup_start(window.from())
                    .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?
                    .min(configured_instance_history_start(instance, window.from())?);
                let descriptor = StrategyDescriptor::new(
                    StrategyId::new(instance.document.strategy_id.clone())
                        .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?,
                    instance.instance_id.clone(),
                    instance.document.title.clone(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                let mut adapter = BacktestConfiguredStrategyAdapter::new(
                    strategy,
                    descriptor,
                    bindings,
                    base.decision_latency_ms,
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                if !instance.historical_inputs.is_empty() {
                    adapter.set_evaluation_start(Some(window.from()));
                }
                let analysis = AnalysisPipeline::new(
                    vec![],
                    ObservationStoreLimits::default(),
                    AnnotationLimits::default(),
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
                configured.push(
                    ConfiguredInstance::new(adapter, analysis)
                        .with_entry_profiles(instance.profiles.clone().unwrap_or_default())
                        .with_feed_from(start),
                );
                if !events.contains_key(&instance.symbol) {
                    return Err(ResearchError::InvalidPlan(format!(
                        "missing events for {}",
                        instance.symbol
                    )));
                }
                feed_starts
                    .entry(instance.symbol.clone())
                    .and_modify(|existing| *existing = (*existing).min(start))
                    .or_insert(start);
            }
            let mut feed_events = Vec::new();
            for (symbol, start) in feed_starts {
                feed_events.extend(crate::runner::slice_events(&events[&symbol], window, start));
            }
            let coverage = crate::runner::input_coverage(&feed_events, window)
                .map_err(|error| ResearchError::InvalidPlan(format!("coverage: {error:?}")))?;
            let primary_eod = feed_events.iter().map(|event| event.available_at()).max();
            let mut feed = VecFeed::from_feed_events(feed_events);
            let mut config = base.config.clone();
            config.close_on_finish = true;
            config
                .run_tags
                .insert("heterogeneous_candidate".into(), candidate.id.clone());
            config
                .run_tags
                .insert("window".into(), window.label().into());
            let supervisor = candidate.portfolio.supervisor()?;
            let result = BacktestRunner::new_future(config, base.future.clone())
                .with_evaluation_options(base.evaluation.clone())
                .run_portfolio_future_streaming_controlled(
                    &mut feed,
                    primary_eod,
                    configured,
                    supervisor,
                    StrategyRetentionLimits::default(),
                    is_cancelled,
                    |_| {},
                )
                .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
            if is_cancelled() {
                return Err(ResearchError::Cancelled);
            }
            completed_runs += 1;
            on_progress(BatchProgress {
                completed_runs,
                total_runs: runs,
            });
            let replay = result.replay;
            let portfolio_symbol = candidate
                .instances
                .iter()
                .map(|instance| instance.symbol.as_str())
                .collect::<Vec<_>>()
                .join("+");
            let mut row = super::factory::empty_row(
                "heterogeneous",
                &portfolio_symbol,
                BTreeMap::from([("portfolio_candidate".into(), candidate.id.clone())]),
                window,
                candidates.len(),
            );
            crate::runner::fill_metrics(&mut row, &replay, window);
            combined.push(row.clone());
            rows.push(row);
            let run_index = window_index;
            let coverage = crate::runner::coverage_with_result(coverage, &replay);
            for mut position in replay.provider_positions {
                position.id = format!(
                    "candidate:{candidate_index}|run:{run_index}|{}",
                    position.id
                );
                positions.push(position);
            }
            run_recipes.push(RunRecipe {
                ordinal: u64::try_from(run_index).unwrap(),
                candidate_ordinal: u64::try_from(candidate_index).unwrap(),
                window: window.label().into(),
                symbol: portfolio_symbol,
                data_mode: "ticks".into(),
                run_tags: BTreeMap::from([
                    ("heterogeneous_candidate".into(), candidate.id.clone()),
                    ("window".into(), window.label().into()),
                ]),
                from: window.from(),
                to: window.to(),
                coverage: Some(coverage),
            });
        }
        let document = serde_json::to_value(
            candidate
                .instances
                .iter()
                .map(|instance| &instance.document)
                .collect::<Vec<_>>(),
        )
        .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
        let recipe = CandidateRecipe {
            ordinal: u64::try_from(candidate_index).unwrap(),
            family_id: "heterogeneous".into(),
            parameters: BTreeMap::from([(
                "portfolio_candidate".into(),
                qs_strategy::ParameterValue::Choice(candidate.id.clone()),
            )]),
            document: Some(document),
            registered_factory: None,
            series_by_symbol: BTreeMap::new(),
            series_by_instance: series_by_symbol,
            input_projectors,
            admission_error: None,
        };
        let experiment = ExperimentRecipe {
            experiment_id: None,
            candidate_count: candidates.len(),
            caller_revision: None,
            dataset_reference: None,
            endpoint_bounds: EndpointBounds::HalfOpen,
            ordered_symbols: candidate
                .instances
                .iter()
                .map(|instance| instance.symbol.clone())
                .collect(),
            decision_latency_ms: base.decision_latency_ms,
            portfolio: Some(
                serde_json::to_value(&candidate.portfolio)
                    .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?,
            ),
            backtest: serde_json::to_value(&base.config).unwrap(),
            future: serde_json::to_value(&base.future).unwrap(),
            evaluation: serde_json::to_value(&base.evaluation).unwrap(),
            retention: serde_json::to_value(base.retention).unwrap(),
            research_retention: serde_json::json!({}),
            profiles: None,
        };
        batches.insert(
            candidate.id.clone(),
            ResearchBatch {
                table: ResearchTable::new(rows),
                positions,
                bound_documents: vec![None],
                experiment_recipe: experiment,
                candidate_recipes: vec![recipe],
                run_recipes,
            },
        );
    }
    Ok(VariantResearchBatch {
        batches,
        table: ResearchTable::new(combined),
    })
}

fn validate_id(id: &str) -> Result<(), ResearchError> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Err(ResearchError::InvalidPlan(
            "invalid execution variant ID".into(),
        ))
    } else {
        Ok(())
    }
}
