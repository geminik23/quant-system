//! Configured strategy runs, portfolio runs, and server-side parameter searches.
//!
//! All reuse the retained-job workflow of raw-signal backtests: one job map, status, watch, cancellation, and artifact store. A configured run loads its one symbol through the same stream, conversion, and instrument path a raw-signal run uses, starting at the warmup its compiled strategy derives. A portfolio run loads the symbols of all its instances through that path into one feed, starting at the earliest warmup, and replays every instance against one account under the policies it carries. A search loads each symbol once into memory and runs through `qs-research` behind a gate that admits one search at a time, because a search holds each symbol's whole range in memory.

use std::collections::{BTreeMap, BTreeSet};

use qs_backtest::data_feed::FallibleBatchFeed;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BarSeriesSpec,
    CalendarAdmissionLimits, CalendarFeatureKind, CalendarInputSpec, CalendarTimeBasis,
    ConfiguredCalendarInput, ConfiguredHistoricalBindings, ConfiguredInstance,
    ConfiguredStrategyAdapterError, HistoricalStrategy, LocalMarketIntervalSpec,
    MAX_PORTFOLIO_INSTANCES, MarketScheduleSpec, MissingIntervalPolicy, NamedSessionSpec,
    ObservationStoreLimits, PortfolioReplayError, PriceBasis, RunCurrencyPlan, SeriesGeometry,
    SeriesId, SeriesRequirement, SessionScheduleSpec, SessionSpanSpec, StrategyContext,
    StrategyDescriptor, StrategyEvent, StrategyId, StrategyOutput, StrategyReplayError,
    StrategyRequirements, StrategyRetentionLimits, Timeframe as SeriesTimeframe,
    TradingCalendarSpec, WarmupRequirement, WeeklyMarketIntervalSpec,
};
use qs_market_loader::{
    MarketLoadLimits, MarketStreamError, open_ordered_stored_tick_stream, open_price_bar_stream,
};
use qs_research::{
    BoundedTrace, CheckpointDependency, CheckpointLimits, CompletedRunCheckpoint, DataWindow,
    DeclaredSpace, DeclaredSpaceLimits, DirectFactoryPoint, DirectResearchFactory,
    DirectRunCandidate, DirectStrategyError, ExecutionVariant, ExecutionVariantLimits,
    GeneratedStructuralFamily, HeterogeneousDirectInstanceSpec, HeterogeneousInstanceSpec,
    HeterogeneousPortfolioCandidate, MixedHeterogeneousPortfolioCandidate, PredicateAtom,
    ResearchAdmissionLimits, ResearchError, ResearchPlan, SearchCheckpoint, StrategyFamily,
    StructuralCandidate, StructuralOperators, StructuralResourceLimits, StructuralSearchSpec,
    TraceLimits, TraceRecord, UncertaintyAssessment, WindowPlan, batch_data_range_with_limits,
    load_symbol_bars_with_resolver as load_symbol_bars,
    load_symbol_ticks_with_resolver as load_symbol_ticks,
    run_batch_controlled_with_experiment_resume, run_direct_factory_batch_controlled_resume,
    run_execution_variants_controlled, run_heterogeneous_portfolios_controlled,
    run_mixed_heterogeneous_portfolios, selected_candidate_data_range,
    validate_bar_window_alignment_with_limits, validate_batch_with_limits,
};
use qs_risk::{CorrelationGroup, PortfolioSupervisor, RiskPolicy};
use qs_strategy::{
    ConfiguredStrategy, ConfiguredStrategyRequirements, MaterialLibrary, ParameterBinding,
    ParameterValue, SourceId, StrategyConfig,
};

use super::*;

const DEFAULT_INSTANCE_ID: &str = "instance";

/// A validated configured strategy run, kept until its worker starts.
#[derive(Debug, Clone)]
pub struct AcceptedConfiguredRun {
    document: StrategyConfig,
    document_value: serde_json::Value,
    sources: Vec<SourceBindingMsg>,
    geometry: Vec<SeriesGeometry>,
    symbol: String,
    instance_id: String,
    decision_latency_ms: u64,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    requested_from: Option<String>,
    requested_to: Option<String>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    config: BacktestConfigMsg,
    future: FutureQuoteConfigMsg,
    evaluation: ProviderEvaluationOptionsMsg,
    profiles: PreparedEntryProfiles,
    historical_inputs: Vec<ConfiguredCalendarInput>,
    historical_inputs_value: Option<serde_json::Value>,
    delivery: ResultDeliveryMsg,
}

#[derive(Clone)]
enum SearchFamily {
    Declared(DeclaredSpace),
    Structural(GeneratedStructuralFamily),
    Direct,
}

struct TrustedNoopStrategy {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
}

impl HistoricalStrategy for TrustedNoopStrategy {
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
    ) -> std::result::Result<StrategyOutput, Self::Error> {
        Ok(StrategyOutput::none())
    }
}

#[derive(Clone)]
struct TrustedDirectFactory {
    points: Vec<DirectFactoryPoint>,
    timeframe: SeriesTimeframe,
}

impl DirectResearchFactory for TrustedDirectFactory {
    fn factory_name(&self) -> &str {
        "noop_direct"
    }

    fn revision(&self) -> &str {
        "r1"
    }

    fn point_count(&self) -> usize {
        self.points.len()
    }

    fn point(&self, index: usize) -> Option<DirectFactoryPoint> {
        self.points.get(index).cloned()
    }

    fn create(
        &self,
        point: &DirectFactoryPoint,
        symbol: &str,
        window: &DataWindow,
    ) -> std::result::Result<DirectRunCandidate, ResearchError> {
        let warmup_bars = match point.binding.get("warmup_bars") {
            Some(ParameterValue::Integer(value)) => usize::try_from(*value).map_err(|_| {
                ResearchError::InvalidPlan("trusted factory warmup is negative".into())
            })?,
            _ => {
                return Err(ResearchError::InvalidPlan(
                    "trusted factory point omitted warmup_bars".into(),
                ));
            }
        };
        let series = SeriesRequirement::new(
            SeriesId::new("primary").map_err(|error| {
                ResearchError::InvalidPlan(format!("trusted factory series: {error}"))
            })?,
            symbol,
            self.timeframe,
            PriceBasis::Bid,
            WarmupRequirement::bars(warmup_bars).map_err(|error| {
                ResearchError::InvalidPlan(format!("trusted factory warmup: {error}"))
            })?,
        )
        .map_err(|error| ResearchError::InvalidPlan(format!("trusted factory series: {error}")))?;
        let requirements = StrategyRequirements::new(
            vec![symbol.to_owned()],
            vec![series.clone()],
            0,
            false,
            false,
        )
        .map_err(|error| {
            ResearchError::InvalidPlan(format!("trusted factory requirements: {error}"))
        })?;
        let point_label = point
            .binding
            .iter()
            .map(|(name, value)| format!("{name}={}", qs_strategy::parameter_value_label(value)))
            .collect::<Vec<_>>()
            .join(",");
        let descriptor = StrategyDescriptor::new(
            StrategyId::new("trusted_noop").map_err(|error| {
                ResearchError::InvalidPlan(format!("trusted factory strategy ID: {error}"))
            })?,
            "r1",
            format!("Trusted no-op {symbol} {} {point_label}", window.label()),
        )
        .map_err(|error| {
            ResearchError::InvalidPlan(format!("trusted factory descriptor: {error}"))
        })?;
        Ok(DirectRunCandidate {
            strategy: Box::new(TrustedNoopStrategy {
                descriptor,
                requirements,
            }),
            series: vec![
                BarSeriesSpec::new(series, 2, 0, MissingIntervalPolicy::Skip).map_err(|error| {
                    ResearchError::InvalidPlan(format!("trusted factory bar series: {error}"))
                })?,
            ],
            analysis: AnalysisPipeline::new(
                vec![],
                ObservationStoreLimits::default(),
                AnnotationLimits::default(),
            )
            .map_err(|error| {
                ResearchError::InvalidPlan(format!("trusted factory analysis: {error}"))
            })?,
        })
    }
}

#[derive(Clone)]
enum SearchPoint {
    Declared(usize),
    Structural(Box<StructuralCandidate>),
}

impl StrategyFamily for SearchFamily {
    type Params = SearchPoint;
    fn family_id(&self) -> &str {
        match self {
            Self::Declared(value) => value.family_id(),
            Self::Structural(value) => value.family_id(),
            Self::Direct => "noop_direct",
        }
    }
    fn points(&self) -> Vec<Self::Params> {
        match self {
            Self::Declared(value) => value
                .points()
                .into_iter()
                .map(SearchPoint::Declared)
                .collect(),
            Self::Structural(value) => value
                .points()
                .into_iter()
                .map(|point| SearchPoint::Structural(Box::new(point)))
                .collect(),
            Self::Direct => Vec::new(),
        }
    }
    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => value.parameter_binding(point),
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.parameter_binding(point)
            }
            _ => unreachable!(),
        }
    }
    fn config(&self, point: &Self::Params) -> StrategyConfig {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => value.config(point),
            (Self::Structural(value), SearchPoint::Structural(point)) => value.config(point),
            _ => unreachable!(),
        }
    }
    fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<SeriesGeometry> {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => value.geometry(symbol, point),
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.geometry(symbol, point)
            }
            _ => unreachable!(),
        }
    }
    fn bindings(
        &self,
        symbol: &str,
        point: &Self::Params,
        requirements: &ConfiguredStrategyRequirements,
    ) -> std::result::Result<ConfiguredHistoricalBindings, String> {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => {
                value.bindings(symbol, point, requirements)
            }
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.bindings(symbol, point, requirements)
            }
            _ => unreachable!(),
        }
    }
    fn history_start(
        &self,
        symbol: &str,
        point: &Self::Params,
        evaluation_start: NaiveDateTime,
    ) -> std::result::Result<NaiveDateTime, String> {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => {
                value.history_start(symbol, point, evaluation_start)
            }
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.history_start(symbol, point, evaluation_start)
            }
            _ => Ok(evaluation_start),
        }
    }
    fn history_start_for_requirements(
        &self,
        symbol: &str,
        point: &Self::Params,
        evaluation_start: NaiveDateTime,
        requirements: &ConfiguredStrategyRequirements,
    ) -> std::result::Result<NaiveDateTime, String> {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => {
                value.history_start_for_requirements(symbol, point, evaluation_start, requirements)
            }
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.history_start_for_requirements(symbol, point, evaluation_start, requirements)
            }
            _ => Ok(evaluation_start),
        }
    }
    fn library(&self) -> MaterialLibrary {
        match self {
            Self::Declared(value) => value.library(),
            Self::Structural(value) => value.library(),
            Self::Direct => MaterialLibrary::builtins(),
        }
    }
    fn input_projector_recipe(
        &self,
        symbol: &str,
        point: &Self::Params,
    ) -> Vec<qs_research::InputProjectorSnapshot> {
        match (self, point) {
            (Self::Declared(value), SearchPoint::Declared(point)) => {
                value.input_projector_recipe(symbol, point)
            }
            (Self::Structural(value), SearchPoint::Structural(point)) => {
                value.input_projector_recipe(symbol, point)
            }
            _ => Vec::new(),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StructuralRequestDocument {
    family_id: String,
    state_id: String,
    transition_priority: i32,
    atoms: Vec<PredicateAtom>,
    operators: StructuralOperators,
    sequence_source: SourceId,
    sequence_max_gap: usize,
    #[serde(default)]
    captures: Vec<qs_research::CaptureCandidate>,
    limits: StructuralResourceLimits,
}

/// A validated parameter search, kept until its worker starts.
#[derive(Clone)]
struct AcceptedSelectedRerun {
    experiment: qs_research::ExperimentRecipe,
    candidate: qs_research::CandidateRecipe,
    run: qs_research::RunRecipe,
    role: qs_research::EvaluationRole,
    caller_revision: String,
    release_final: bool,
    future_horizon_millis: Option<u64>,
    embargo_millis: Option<u64>,
}

#[derive(Clone)]
pub struct AcceptedSearch {
    family: SearchFamily,
    plan: ResearchPlan,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    range: (NaiveDateTime, NaiveDateTime),
    admission_limits: ResearchAdmissionLimits,
    generation_dispositions: Option<serde_json::Value>,
    checkpoint_limits: CheckpointLimits,
    trace_limits: TraceLimits,
    market_load_limits: MarketLoadLimits,
    enhanced_descriptors: BTreeMap<String, data_preprocess::SeriesDescriptor>,
    resume_checkpoint: Option<SearchCheckpoint>,
    selected_rerun: Option<AcceptedSelectedRerun>,
    portfolio_candidates: Vec<HeterogeneousPortfolioCandidate>,
    mixed_portfolio_candidates: Vec<MixedHeterogeneousPortfolioCandidate>,
    variants: Vec<ExecutionVariant>,
    direct_factory: Option<TrustedDirectFactory>,
}

impl std::fmt::Debug for AcceptedSearch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcceptedSearch")
            .field("family_id", &self.family.family_id())
            .field("symbols", &self.plan.symbols)
            .field("exchange", &self.exchange)
            .field("data_type", &self.data_type)
            .field("timeframe", &self.timeframe)
            .field("range", &self.range)
            .finish()
    }
}

// ── Configured strategy runs ────────────────────────────────────────────────

/// Handle `run_configured_strategy`: validate, run, and return the result synchronously.
pub fn handle_run_configured_strategy(
    state: &ServerState,
    req: &RunConfiguredStrategyRequest,
) -> RunBacktestResponse {
    let start = Instant::now();
    let outcome = prepare_configured_run(state, req).and_then(|run| {
        execute_configured_run(state, &run, &JobCancellationToken::default(), &mut |_| {})
    });
    match outcome {
        Ok(message) => {
            single_response_from_result(state, message, start, Some(req.result_delivery))
        }
        Err(error) => RunBacktestResponse {
            success: false,
            error: Some(error.to_string()),
            result: None,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        },
    }
}

/// Handle `submit_configured_strategy`: validate completely, then admit a retained job.
pub fn handle_submit_configured_strategy(
    state: &ServerState,
    req: &SubmitConfiguredStrategyRequest,
) -> SubmitBacktestResponse {
    match prepare_configured_run(state, &req.request) {
        Ok(run) => admit_job(
            state,
            JobKind::Backtest,
            AcceptedJobInput::ConfiguredStrategy(Box::new(run)),
        ),
        Err(error) => SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some(error.to_string()),
        },
    }
}

pub(super) fn run_configured_job(
    state: Arc<ServerState>,
    job_id: String,
    run: AcceptedConfiguredRun,
) {
    let delivery = run.delivery;
    run_result_job(
        state,
        job_id,
        delivery,
        move |state, job_id, cancellation| {
            execute_configured_run(state, &run, cancellation, &mut |progress| {
                update_job_progress(state, job_id, progress)
            })
        },
    );
}

fn prepare_configured_run(
    state: &ServerState,
    req: &RunConfiguredStrategyRequest,
) -> Result<AcceptedConfiguredRun> {
    let spec = &req.request;
    validate_future_quote_scalars(&req.future)?;
    account_currency_from_msg(&req.future)?;
    let symbol = required_symbol(&state.symbol_registry, &spec.symbol)?;
    let bar_seconds = data_geometry(&spec.data_type, spec.timeframe.as_deref())?;
    let limits = &state.strategies.limits;
    check_document_size(limits, "strategy document", &spec.strategy.document)?;
    let document: StrategyConfig = decode_document("strategy document", &spec.strategy.document)?;
    let instance_id = spec
        .strategy
        .instance_id
        .clone()
        .unwrap_or_else(|| DEFAULT_INSTANCE_ID.into());
    let geometry = spec
        .strategy
        .sources
        .iter()
        .map(|binding| geometry_from_msg(binding, &symbol, bar_seconds))
        .collect::<Result<Vec<_>>>()?;
    let from = parse_optional_datetime(&spec.from)?;
    let to = parse_optional_datetime(&spec.to)?;
    if let (Some(from), Some(to)) = (from, to)
        && from >= to
    {
        return Err(invalid(format!("from {from} must be before to {to}")));
    }
    let historical_inputs =
        historical_inputs_from_msg(spec.strategy.historical_inputs.as_ref(), limits)?;
    if !historical_inputs.is_empty() && (from.is_none() || to.is_none()) {
        return Err(invalid(
            "configured historical inputs require finite from and to bounds".into(),
        ));
    }
    let adapter = build_adapter(
        &document,
        &instance_id,
        &symbol,
        geometry.clone(),
        spec.strategy.decision_latency_ms,
        &historical_inputs,
        limits,
    )?;
    let profiles = resolve_entry_profiles(
        state,
        spec.profile.as_ref(),
        spec.profile_def.as_ref(),
        &spec.entry_profile_routes,
    )?;
    adapter
        .preflight_entry_profiles(&profiles)
        .map_err(|error| invalid(format!("entry profile routing: {error}")))?;
    if !adapter.configured_requirements().entries.is_empty() && spec.config.sizing.is_none() {
        return Err(invalid(
            "a strategy that emits Entry actions requires config.sizing".into(),
        ));
    }
    let symbols = [symbol.clone()];
    config_from_msg(&spec.config, &state.symbol_registry, &symbols)?;
    evaluation_options_from_msg_for_symbols(&req.evaluation, &state.symbol_registry, &symbols)?;
    if bar_seconds.is_some() {
        validate_server_bar_bounds("configured run", &geometry, from, to)?;
    }
    Ok(AcceptedConfiguredRun {
        document,
        document_value: spec.strategy.document.clone(),
        sources: spec.strategy.sources.clone(),
        geometry,
        symbol,
        instance_id,
        decision_latency_ms: spec.strategy.decision_latency_ms,
        exchange: spec.exchange.to_lowercase(),
        data_type: spec.data_type.to_lowercase(),
        timeframe: spec.timeframe.clone(),
        requested_from: spec.from.clone(),
        requested_to: spec.to.clone(),
        from,
        to,
        config: spec.config.clone(),
        future: req.future.clone(),
        evaluation: req.evaluation.clone(),
        profiles,
        historical_inputs_value: spec
            .strategy
            .historical_inputs
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
        historical_inputs,
        delivery: req.result_delivery,
    })
}

fn execute_configured_run(
    state: &ServerState,
    run: &AcceptedConfiguredRun,
    cancellation: &JobCancellationToken,
    progress: &mut dyn FnMut(BacktestProgress),
) -> Result<BacktestResultMsg> {
    ensure_not_cancelled(Some(cancellation))?;
    let mut adapter = build_adapter(
        &run.document,
        &run.instance_id,
        &run.symbol,
        run.geometry.clone(),
        run.decision_latency_ms,
        &run.historical_inputs,
        &state.strategies.limits,
    )?;
    if !run.historical_inputs.is_empty() {
        adapter.set_evaluation_start(run.from);
    }
    let loading_start = match run.from {
        Some(from) => Some(
            ConfiguredHistoricalBindings::from_geometry(
                run.geometry.clone(),
                adapter.configured_requirements(),
            )
            .and_then(|bindings| bindings.warmup_start(from))
            .map_err(|error| invalid(error.to_string()))?
            .min(configured_history_start(&run.historical_inputs, from)?),
        ),
        None => None,
    };
    let symbols = vec![run.symbol.clone()];
    let evaluation_options =
        evaluation_options_from_msg_for_symbols(&run.evaluation, &state.symbol_registry, &symbols)?;
    let mut config = config_from_msg(&run.config, &state.symbol_registry, &symbols)?;
    let account_currency = account_currency_from_msg(&run.future)?;

    progress(BacktestProgress {
        stage: "loading_data".into(),
        total_symbols: 1,
        ..BacktestProgress::default()
    });
    let resolver = state
        .instrument_domain
        .symbol_resolver(&state.symbol_registry);
    let mut cancelled = || cancellation.is_cancelled();
    let mut primary = describe_primary_market_stream(
        &resolver,
        &state.data_dir,
        &run.exchange,
        &symbols,
        &run.data_type,
        run.timeframe.as_deref(),
        loading_start,
        run.to,
        &mut cancelled,
        &mut |processed_symbols| {
            progress(BacktestProgress {
                stage: "loading_data".into(),
                processed_symbols,
                total_symbols: 1,
                ..BacktestProgress::default()
            })
        },
    )?;
    primary.apply_bar_point_sizes(&bar_point_sizes(&state.symbol_registry, &symbols));
    let bundle = describe_future_stream(
        &state.data_dir,
        &run.exchange,
        &state.symbol_registry,
        &resolver,
        &account_currency,
        &symbols,
        &run.data_type,
        loading_start,
        primary,
        &mut cancelled,
    )?;
    let primary_eod = bundle.description.primary_eod();
    if let Some(loading_start) = loading_start {
        let mut instrument_symbols = symbols.clone();
        instrument_symbols.extend(bundle.currency_plan.conversion_symbols().iter().cloned());
        instrument_symbols.sort();
        instrument_symbols.dedup();
        let mut manifest = state.instrument_domain.resolve_manifest(
            &instrument_symbols,
            loading_start,
            primary_eod.or(run.to),
        )?;
        state.instrument_domain.attach_stored_series(
            &mut manifest,
            bundle.description.stored_series_coordinates(),
        )?;
        bundle
            .description
            .validate_stored_series_bindings(&manifest)?;
        config.instrument_manifest = Some(manifest);
    }
    let future_config = future_config_from_msg(&run.future, bundle.currency_plan)?;
    let token = cancellation.clone();
    let stream_cancellation: CancellationCheck = Arc::new(move || token.is_cancelled());
    let mut feed = bundle.description.open(stream_cancellation)?;
    ensure_not_cancelled(Some(cancellation))?;
    progress(BacktestProgress {
        stage: "replay".into(),
        processed_symbols: 1,
        total_symbols: 1,
        ..BacktestProgress::default()
    });
    let analysis = AnalysisPipeline::new(
        Vec::new(),
        ObservationStoreLimits::default(),
        AnnotationLimits::default(),
    )
    .map_err(|error| invalid(error.to_string()))?;
    let output = BacktestRunner::new_future(config, future_config)
        .with_entry_profiles(run.profiles.clone())
        .with_evaluation_options(evaluation_options)
        .run_configured_strategy_future_streaming_controlled(
            &mut feed,
            primary_eod,
            &mut adapter,
            analysis,
            StrategyRetentionLimits::default(),
            None,
            || cancellation.is_cancelled(),
            |ReplayProgress {
                 processed_events,
                 total_events,
                 ..
             }| {
                progress(BacktestProgress {
                    stage: "replay".into(),
                    processed_events: processed_events as u64,
                    total_events: total_events as u64,
                    processed_symbols: 1,
                    total_symbols: 1,
                    ..BacktestProgress::default()
                })
            },
        )
        .map_err(map_configured_replay_error)?;
    ensure_not_cancelled(Some(cancellation))?;

    let requirements = requirements_value(adapter.configured_requirements());
    let mut result = output.replay;
    attach_configured_metadata(&mut result, run, loading_start);
    let mut message = result_to_msg(&result);
    message.strategy = Some(ConfiguredStrategyOutputMsg {
        document: run.document_value.clone(),
        sources: run.sources.clone(),
        requirements,
        data_mode: data_mode(&run.data_type).into(),
        decisions: serde_json::to_value(&output.decisions)
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
        research: serde_json::to_value(&output.research)
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
        historical_inputs: run.historical_inputs_value.clone(),
    });
    Ok(message)
}

/// The compiled requirements a run was bound with, recorded so the service result states what history and routing the strategy needed.
fn requirements_value(
    requirements: &qs_strategy::ConfiguredStrategyRequirements,
) -> serde_json::Value {
    serde_json::json!({
        "completed_bars": requirements
            .completed_bars
            .iter()
            .map(|item| serde_json::json!({
                "source": item.source.as_str(),
                "required_lookback": item.required_lookback,
            }))
            .collect::<Vec<_>>(),
        "named_inputs": requirements
            .named_inputs
            .iter()
            .map(|item| serde_json::json!({
                "name": item.name,
                "value_type": format!("{:?}", item.value_type).to_lowercase(),
            }))
            .collect::<Vec<_>>(),
        "trade_slots": requirements.trade_slots,
        "entries": requirements
            .entries
            .iter()
            .map(|item| serde_json::json!({ "slot": item.slot, "entry_class": item.entry_class }))
            .collect::<Vec<_>>(),
        "stop_managed_slots": requirements.stop_managed_slots,
    })
}

fn map_configured_replay_error(
    error: StrategyReplayError<MarketStreamError, ConfiguredStrategyAdapterError>,
) -> BacktestServerError {
    match error {
        StrategyReplayError::Cancelled => BacktestServerError::Cancelled,
        StrategyReplayError::Feed(error) => {
            map_streaming_replay_error(StreamingReplayError::Feed(error))
        }
        StrategyReplayError::Input(error) => invalid(error.to_string()),
        other => BacktestServerError::Strategy(other.to_string()),
    }
}

/// Record the same reproducibility tags a raw-signal run records, plus the strategy identity and data mode.
fn attach_configured_metadata(
    result: &mut BacktestResult,
    run: &AcceptedConfiguredRun,
    loading_start: Option<NaiveDateTime>,
) {
    let Some(metadata) = result.execution_metadata.as_mut() else {
        return;
    };
    let tags = &mut metadata.tags;
    tags.insert("data.exchange".into(), run.exchange.clone());
    tags.insert("data.type".into(), run.data_type.clone());
    tags.insert(
        "data.timeframe".into(),
        run.timeframe.clone().unwrap_or_else(|| "none".into()),
    );
    tags.insert(
        "data.requested_from".into(),
        run.requested_from
            .clone()
            .unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert(
        "data.requested_to".into(),
        run.requested_to
            .clone()
            .unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert("data.symbols".into(), run.symbol.clone());
    tags.insert(
        "data.loading_from".into(),
        loading_start
            .map(|timestamp| timestamp.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "none".into()),
    );
    tags.insert(
        "execution.decision_latency_ms".into(),
        run.decision_latency_ms.to_string(),
    );
    if run.data_type == "bar" {
        tags.insert(
            "data.bar_quote_convention".into(),
            "open_range_close".into(),
        );
        tags.insert("data.intrabar_order".into(), "adverse_extreme_first".into());
    }
    tags.insert("strategy.id".into(), run.document.strategy_id.clone());
    tags.insert("strategy.instance".into(), run.instance_id.clone());
    tags.insert(
        "strategy.data_mode".into(),
        data_mode(&run.data_type).into(),
    );
    tags.insert(
        "profile.identity".into(),
        run.profiles
            .default_profile()
            .map_or_else(|| "none".into(), |profile| profile.name.clone()),
    );
    tags.insert(
        "profile.entry_routes".into(),
        serde_json::to_string(run.profiles.routes()).unwrap_or_else(|_| "unavailable".into()),
    );
}

// ── Portfolio runs ──────────────────────────────────────────────────────────

/// One validated instance of a portfolio run.
#[derive(Debug, Clone)]
struct AcceptedPortfolioInstance {
    document: StrategyConfig,
    document_value: serde_json::Value,
    sources: Vec<SourceBindingMsg>,
    geometry: Vec<SeriesGeometry>,
    symbol: String,
    instance_id: String,
    decision_latency_ms: u64,
    profiles: PreparedEntryProfiles,
    historical_inputs: Vec<ConfiguredCalendarInput>,
    historical_inputs_value: Option<serde_json::Value>,
}

/// A validated portfolio run, kept until its worker starts.
#[derive(Debug, Clone)]
pub struct AcceptedPortfolioRun {
    instances: Vec<AcceptedPortfolioInstance>,
    symbols: Vec<String>,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    requested_from: Option<String>,
    requested_to: Option<String>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    config: BacktestConfigMsg,
    future: FutureQuoteConfigMsg,
    evaluation: ProviderEvaluationOptionsMsg,
    policies: Vec<RiskPolicy>,
    groups: Vec<CorrelationGroup>,
    delivery: ResultDeliveryMsg,
}

/// Handle `run_portfolio`: validate, run, and return the shared result synchronously.
pub fn handle_run_portfolio(state: &ServerState, req: &RunPortfolioRequest) -> RunBacktestResponse {
    let start = Instant::now();
    let outcome = prepare_portfolio_run(state, req).and_then(|run| {
        execute_portfolio_run(state, &run, &JobCancellationToken::default(), &mut |_| {})
    });
    match outcome {
        Ok(message) => {
            single_response_from_result(state, message, start, Some(req.result_delivery))
        }
        Err(error) => RunBacktestResponse {
            success: false,
            error: Some(error.to_string()),
            result: None,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        },
    }
}

/// Handle `submit_portfolio`: validate every instance and policy, then admit a retained job.
pub fn handle_submit_portfolio(
    state: &ServerState,
    req: &SubmitPortfolioRequest,
) -> SubmitBacktestResponse {
    match prepare_portfolio_run(state, &req.request) {
        Ok(run) => admit_job(
            state,
            JobKind::Backtest,
            AcceptedJobInput::Portfolio(Box::new(run)),
        ),
        Err(error) => SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some(error.to_string()),
        },
    }
}

pub(super) fn run_portfolio_job(
    state: Arc<ServerState>,
    job_id: String,
    run: AcceptedPortfolioRun,
) {
    let delivery = run.delivery;
    run_result_job(
        state,
        job_id,
        delivery,
        move |state, job_id, cancellation| {
            execute_portfolio_run(state, &run, cancellation, &mut |progress| {
                update_job_progress(state, job_id, progress)
            })
        },
    );
}

/// Decode the policies and groups strictly and build the supervisor they describe, or `None` when the request carries no policy.
fn decode_supervisor(
    policies: Option<&serde_json::Value>,
    groups: Option<&serde_json::Value>,
) -> Result<(Vec<RiskPolicy>, Vec<CorrelationGroup>)> {
    let policies: Vec<RiskPolicy> = match policies {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|error| invalid(format!("invalid policies: {error}")))?,
    };
    let groups: Vec<CorrelationGroup> = match groups {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|error| invalid(format!("invalid groups: {error}")))?,
    };
    if policies.is_empty() && !groups.is_empty() {
        return Err(invalid(
            "correlation groups were supplied without any policy that uses them".into(),
        ));
    }
    PortfolioSupervisor::new(policies.clone(), groups.clone())
        .map_err(|error| invalid(format!("invalid policies: {error}")))?;
    Ok((policies, groups))
}

fn prepare_portfolio_run(
    state: &ServerState,
    req: &RunPortfolioRequest,
) -> Result<AcceptedPortfolioRun> {
    let spec = &req.request;
    validate_future_quote_scalars(&req.future)?;
    account_currency_from_msg(&req.future)?;
    let limits = &state.strategies.limits;
    let max_instances = limits.max_portfolio_instances.min(MAX_PORTFOLIO_INSTANCES);
    if spec.instances.is_empty() {
        return Err(invalid(
            "a portfolio run needs at least one instance".into(),
        ));
    }
    if spec.instances.len() > max_instances {
        return Err(invalid(format!(
            "the portfolio has {} instances, above the server limit of {max_instances}",
            spec.instances.len()
        )));
    }
    let bar_seconds = data_geometry(&spec.data_type, spec.timeframe.as_deref())?;
    let mut instances = Vec::with_capacity(spec.instances.len());
    let mut identities = BTreeSet::new();
    let mut retained = 0usize;
    let mut emits_entries = false;
    for (index, item) in spec.instances.iter().enumerate() {
        let context = |error: BacktestServerError| match error {
            BacktestServerError::InvalidRequest(message) => {
                invalid(format!("instances[{index}]: {message}"))
            }
            other => other,
        };
        let symbol = required_symbol(&state.symbol_registry, &item.symbol).map_err(context)?;
        check_document_size(limits, "strategy document", &item.strategy.document)
            .map_err(context)?;
        let document: StrategyConfig =
            decode_document("strategy document", &item.strategy.document).map_err(context)?;
        let instance_id = item.strategy.instance_id.clone().ok_or_else(|| {
            invalid(format!(
                "instances[{index}]: a portfolio instance requires strategy.instance_id"
            ))
        })?;
        if !identities.insert(instance_id.clone()) {
            return Err(invalid(format!(
                "instances[{index}]: instance '{instance_id}' appears more than once"
            )));
        }
        let geometry = item
            .strategy
            .sources
            .iter()
            .map(|binding| geometry_from_msg(binding, &symbol, bar_seconds))
            .collect::<Result<Vec<_>>>()
            .map_err(context)?;
        let historical_inputs =
            historical_inputs_from_msg(item.strategy.historical_inputs.as_ref(), limits)
                .map_err(context)?;
        let adapter = build_adapter(
            &document,
            &instance_id,
            &symbol,
            geometry.clone(),
            item.strategy.decision_latency_ms,
            &historical_inputs,
            limits,
        )
        .map_err(context)?;
        retained += adapter
            .series_specs()
            .map(|series| series.retained_bars())
            .sum::<usize>();
        let profiles = resolve_entry_profiles(
            state,
            item.profile.as_ref(),
            item.profile_def.as_ref(),
            &item.entry_profile_routes,
        )
        .map_err(context)?;
        adapter
            .preflight_entry_profiles(&profiles)
            .map_err(|error| {
                invalid(format!(
                    "instances[{index}]: entry profile routing: {error}"
                ))
            })?;
        emits_entries |= !adapter.configured_requirements().entries.is_empty();
        instances.push(AcceptedPortfolioInstance {
            document,
            document_value: item.strategy.document.clone(),
            sources: item.strategy.sources.clone(),
            geometry,
            symbol,
            instance_id,
            decision_latency_ms: item.strategy.decision_latency_ms,
            profiles,
            historical_inputs_value: item
                .strategy
                .historical_inputs
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
            historical_inputs,
        });
    }
    if retained > limits.max_retained_bars {
        return Err(invalid(format!(
            "the portfolio retains {retained} bars across all instances, above the server limit of {}",
            limits.max_retained_bars
        )));
    }
    if emits_entries && spec.config.sizing.is_none() {
        return Err(invalid(
            "a portfolio whose strategies emit Entry actions requires config.sizing".into(),
        ));
    }
    let mut symbols = instances
        .iter()
        .map(|instance| instance.symbol.clone())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols.dedup();
    let config = config_from_msg(&spec.config, &state.symbol_registry, &symbols)?;
    evaluation_options_from_msg_for_symbols(&req.evaluation, &state.symbol_registry, &symbols)?;
    let (policies, mut groups) = decode_supervisor(spec.policies.as_ref(), spec.groups.as_ref())?;
    // Group symbols name symbols the same way instance symbols do, so a cap compares normalized symbols; a group that names none of the portfolio's symbols could never apply and is almost certainly a mistake.
    for group in &mut groups {
        group.symbols = group
            .symbols
            .iter()
            .map(|symbol| normalize_symbol(&state.symbol_registry, symbol.trim()))
            .collect();
        if !group.symbols.iter().any(|symbol| symbols.contains(symbol)) {
            return Err(invalid(format!(
                "group '{}' names none of the portfolio's symbols {}",
                group.id,
                symbols.join(", ")
            )));
        }
    }
    if policies
        .iter()
        .any(|policy| matches!(policy, RiskPolicy::GroupRiskCap { .. }))
        && !matches!(
            config.sizing,
            Some(qs_backtest::sizing::SizingPolicy::FixedRiskAmount { .. })
                | Some(qs_backtest::sizing::SizingPolicy::BalanceRiskPercent { .. })
        )
    {
        return Err(invalid(
            "a group risk cap needs a monetary sizing policy, because a fixed-lot entry's risk is unknown until it fills".into(),
        ));
    }
    let from = parse_optional_datetime(&spec.from)?;
    let to = parse_optional_datetime(&spec.to)?;
    if let (Some(from), Some(to)) = (from, to)
        && from >= to
    {
        return Err(invalid(format!("from {from} must be before to {to}")));
    }
    if instances
        .iter()
        .any(|instance| !instance.historical_inputs.is_empty())
        && (from.is_none() || to.is_none())
    {
        return Err(invalid(
            "portfolio historical inputs require finite from and to bounds".into(),
        ));
    }
    if bar_seconds.is_some() {
        for (index, instance) in instances.iter().enumerate() {
            validate_server_bar_bounds(
                &format!("portfolio instances[{index}]"),
                &instance.geometry,
                from,
                to,
            )?;
        }
    }
    Ok(AcceptedPortfolioRun {
        instances,
        symbols,
        exchange: spec.exchange.to_lowercase(),
        data_type: spec.data_type.to_lowercase(),
        timeframe: spec.timeframe.clone(),
        requested_from: spec.from.clone(),
        requested_to: spec.to.clone(),
        from,
        to,
        config: spec.config.clone(),
        future: req.future.clone(),
        evaluation: req.evaluation.clone(),
        policies,
        groups,
        delivery: req.result_delivery,
    })
}

fn execute_portfolio_run(
    state: &ServerState,
    run: &AcceptedPortfolioRun,
    cancellation: &JobCancellationToken,
    progress: &mut dyn FnMut(BacktestProgress),
) -> Result<BacktestResultMsg> {
    ensure_not_cancelled(Some(cancellation))?;
    let mut adapters = Vec::with_capacity(run.instances.len());
    let mut instance_starts = Vec::with_capacity(run.instances.len());
    let mut loading_start: Option<NaiveDateTime> = None;
    for instance in &run.instances {
        let adapter = build_adapter(
            &instance.document,
            &instance.instance_id,
            &instance.symbol,
            instance.geometry.clone(),
            instance.decision_latency_ms,
            &instance.historical_inputs,
            &state.strategies.limits,
        )?;
        let mut adapter = adapter;
        if !instance.historical_inputs.is_empty() {
            adapter.set_evaluation_start(run.from);
        }
        if let Some(from) = run.from {
            let start = ConfiguredHistoricalBindings::from_geometry(
                instance.geometry.clone(),
                adapter.configured_requirements(),
            )
            .and_then(|bindings| bindings.warmup_start(from))
            .map_err(|error| invalid(error.to_string()))?
            .min(configured_history_start(&instance.historical_inputs, from)?);
            loading_start = Some(loading_start.map_or(start, |current| current.min(start)));
            instance_starts.push(Some(start));
        } else {
            instance_starts.push(None);
        }
        adapters.push(adapter);
    }
    let symbols = run.symbols.clone();
    let total_symbols = symbols.len() as u64;
    let evaluation_options =
        evaluation_options_from_msg_for_symbols(&run.evaluation, &state.symbol_registry, &symbols)?;
    let mut config = config_from_msg(&run.config, &state.symbol_registry, &symbols)?;
    let account_currency = account_currency_from_msg(&run.future)?;

    progress(BacktestProgress {
        stage: "loading_data".into(),
        total_symbols,
        ..BacktestProgress::default()
    });
    let resolver = state
        .instrument_domain
        .symbol_resolver(&state.symbol_registry);
    let mut cancelled = || cancellation.is_cancelled();
    let mut primary = describe_primary_market_stream(
        &resolver,
        &state.data_dir,
        &run.exchange,
        &symbols,
        &run.data_type,
        run.timeframe.as_deref(),
        loading_start,
        run.to,
        &mut cancelled,
        &mut |processed_symbols| {
            progress(BacktestProgress {
                stage: "loading_data".into(),
                processed_symbols,
                total_symbols,
                ..BacktestProgress::default()
            })
        },
    )?;
    primary.apply_bar_point_sizes(&bar_point_sizes(&state.symbol_registry, &symbols));
    let bundle = describe_future_stream(
        &state.data_dir,
        &run.exchange,
        &state.symbol_registry,
        &resolver,
        &account_currency,
        &symbols,
        &run.data_type,
        loading_start,
        primary,
        &mut cancelled,
    )?;
    let primary_eod = bundle.description.primary_eod();
    if let Some(loading_start) = loading_start {
        let mut instrument_symbols = symbols.clone();
        instrument_symbols.extend(bundle.currency_plan.conversion_symbols().iter().cloned());
        instrument_symbols.sort();
        instrument_symbols.dedup();
        let mut manifest = state.instrument_domain.resolve_manifest(
            &instrument_symbols,
            loading_start,
            primary_eod.or(run.to),
        )?;
        state.instrument_domain.attach_stored_series(
            &mut manifest,
            bundle.description.stored_series_coordinates(),
        )?;
        bundle
            .description
            .validate_stored_series_bindings(&manifest)?;
        config.instrument_manifest = Some(manifest);
    }
    let future_config = future_config_from_msg(&run.future, bundle.currency_plan)?;
    let token = cancellation.clone();
    let stream_cancellation: CancellationCheck = Arc::new(move || token.is_cancelled());
    let mut feed = bundle.description.open(stream_cancellation)?;
    ensure_not_cancelled(Some(cancellation))?;
    progress(BacktestProgress {
        stage: "replay".into(),
        processed_symbols: total_symbols,
        total_symbols,
        ..BacktestProgress::default()
    });
    let requirements = adapters
        .iter()
        .map(|adapter| requirements_value(adapter.configured_requirements()))
        .collect::<Vec<_>>();
    let mut instances = Vec::with_capacity(adapters.len());
    for ((adapter, accepted), start) in adapters
        .into_iter()
        .zip(&run.instances)
        .zip(instance_starts)
    {
        let analysis = AnalysisPipeline::new(
            Vec::new(),
            ObservationStoreLimits::default(),
            AnnotationLimits::default(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        // The shared feed starts at the earliest warmup; each instance reads from its own, as a single run would.
        let mut instance = ConfiguredInstance::new(adapter, analysis)
            .with_entry_profiles(accepted.profiles.clone());
        if let Some(start) = start {
            instance = instance.with_feed_from(start);
        }
        instances.push(instance);
    }
    let supervisor = if run.policies.is_empty() {
        None
    } else {
        Some(
            PortfolioSupervisor::new(run.policies.clone(), run.groups.clone())
                .map_err(|error| invalid(format!("invalid policies: {error}")))?,
        )
    };
    let output = BacktestRunner::new_future(config, future_config)
        .with_evaluation_options(evaluation_options)
        .run_portfolio_future_streaming_controlled(
            &mut feed,
            primary_eod,
            instances,
            supervisor,
            StrategyRetentionLimits::default(),
            || cancellation.is_cancelled(),
            |ReplayProgress {
                 processed_events,
                 total_events,
                 ..
             }| {
                progress(BacktestProgress {
                    stage: "replay".into(),
                    processed_events: processed_events as u64,
                    total_events: total_events as u64,
                    processed_symbols: total_symbols,
                    total_symbols,
                    ..BacktestProgress::default()
                })
            },
        )
        .map_err(map_portfolio_replay_error)?;
    ensure_not_cancelled(Some(cancellation))?;

    let mut result = output.replay;
    attach_portfolio_metadata(&mut result, run, loading_start);
    let mut message = result_to_msg(&result);
    let mut instance_outputs = Vec::with_capacity(output.instances.len());
    for ((instance, accepted), requirements) in output
        .instances
        .iter()
        .zip(&run.instances)
        .zip(requirements)
    {
        instance_outputs.push(PortfolioInstanceOutputMsg {
            instance_id: instance.instance_id.clone(),
            symbol: accepted.symbol.clone(),
            strategy: ConfiguredStrategyOutputMsg {
                document: accepted.document_value.clone(),
                sources: accepted.sources.clone(),
                requirements,
                data_mode: data_mode(&run.data_type).into(),
                decisions: to_json(&instance.decisions)?,
                research: to_json(&instance.research)?,
                historical_inputs: accepted.historical_inputs_value.clone(),
            },
        });
    }
    message.portfolio = Some(PortfolioOutputMsg {
        instances: instance_outputs,
        policies: if run.policies.is_empty() {
            None
        } else {
            Some(serde_json::json!({
                "policies": to_json(&run.policies)?,
                "groups": to_json(&run.groups)?,
            }))
        },
        supervisor: output.supervisor.as_ref().map(to_json).transpose()?,
    });
    Ok(message)
}

fn to_json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| BacktestServerError::Serde(error.to_string()))
}

fn map_portfolio_replay_error(
    error: PortfolioReplayError<MarketStreamError>,
) -> BacktestServerError {
    match error {
        PortfolioReplayError::Cancelled => BacktestServerError::Cancelled,
        PortfolioReplayError::Feed(error) => {
            map_streaming_replay_error(StreamingReplayError::Feed(error))
        }
        PortfolioReplayError::Instance {
            instance_id,
            source: StrategyReplayError::Input(error),
        } => invalid(format!("instance '{instance_id}': {error}")),
        error @ (PortfolioReplayError::NoInstances
        | PortfolioReplayError::TooManyInstances(_)
        | PortfolioReplayError::DuplicateInstanceIdentity { .. }
        | PortfolioReplayError::Input(_)
        | PortfolioReplayError::MixedPrimaryInput { .. }
        | PortfolioReplayError::Supervisor(_)) => invalid(error.to_string()),
        other => BacktestServerError::Strategy(other.to_string()),
    }
}

/// Record the reproducibility tags of a configured run for the whole portfolio, with instances and policies listed.
fn attach_portfolio_metadata(
    result: &mut BacktestResult,
    run: &AcceptedPortfolioRun,
    loading_start: Option<NaiveDateTime>,
) {
    let Some(metadata) = result.execution_metadata.as_mut() else {
        return;
    };
    let tags = &mut metadata.tags;
    tags.insert("data.exchange".into(), run.exchange.clone());
    tags.insert("data.type".into(), run.data_type.clone());
    tags.insert(
        "data.timeframe".into(),
        run.timeframe.clone().unwrap_or_else(|| "none".into()),
    );
    tags.insert(
        "data.requested_from".into(),
        run.requested_from
            .clone()
            .unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert(
        "data.requested_to".into(),
        run.requested_to
            .clone()
            .unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert("data.symbols".into(), run.symbols.join(","));
    tags.insert(
        "data.loading_from".into(),
        loading_start
            .map(|timestamp| timestamp.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "none".into()),
    );
    if run.data_type == "bar" {
        tags.insert(
            "data.bar_quote_convention".into(),
            "open_range_close".into(),
        );
        tags.insert("data.intrabar_order".into(), "adverse_extreme_first".into());
    }
    tags.insert(
        "strategy.data_mode".into(),
        data_mode(&run.data_type).into(),
    );
    tags.insert(
        "portfolio.instances".into(),
        run.instances
            .iter()
            .map(|instance| format!("{}/{}", instance.document.strategy_id, instance.instance_id))
            .collect::<Vec<_>>()
            .join(","),
    );
    tags.insert(
        "portfolio.policies".into(),
        if run.policies.is_empty() {
            "none".into()
        } else {
            run.policies
                .iter()
                .map(RiskPolicy::name)
                .collect::<Vec<_>>()
                .join(",")
        },
    );
}

// ── Parameter searches ──────────────────────────────────────────────────────

/// Handle `submit_search`: validate the whole search without loading data, then admit a retained job.
pub fn handle_submit_search(
    state: &ServerState,
    req: &SubmitSearchRequest,
) -> SubmitBacktestResponse {
    match prepare_search(state, req) {
        Ok(search) => admit_job(
            state,
            JobKind::Search,
            AcceptedJobInput::Search(Box::new(search)),
        ),
        Err(error) => SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some(error.to_string()),
        },
    }
}

/// Handle `get_search_result`: return a completed search's summary and the artifact holding its output.
pub fn handle_get_search_result(
    state: &ServerState,
    req: &GetSearchResultRequest,
) -> GetSearchResultResponse {
    let failure = |error: String| GetSearchResultResponse {
        success: false,
        job_id: req.job_id.clone(),
        error: Some(error),
        summary: None,
        artifact: None,
        checkpoint_artifact: None,
    };
    let jobs = state.jobs.lock().unwrap();
    match jobs.get(&req.job_id) {
        None => failure(format!("Job '{}' not found", req.job_id)),
        Some(job) if job.kind != JobKind::Search => {
            failure("Job is not a parameter search; use get_backtest_result".into())
        }
        Some(job) if job.status == JobStatus::Cancelled && job.checkpoint_artifact.is_some() => {
            GetSearchResultResponse {
                success: false,
                job_id: req.job_id.clone(),
                error: Some("Search was cancelled; a resumable checkpoint is available".into()),
                summary: None,
                artifact: None,
                checkpoint_artifact: job.checkpoint_artifact.clone(),
            }
        }
        Some(job) if job.status != JobStatus::Completed => failure(format!(
            "Job is not completed (status: {})",
            job.status.as_str()
        )),
        Some(job) => GetSearchResultResponse {
            success: true,
            job_id: req.job_id.clone(),
            error: None,
            summary: job.search.clone(),
            artifact: job.artifact.clone(),
            checkpoint_artifact: None,
        },
    }
}

fn prepare_search(state: &ServerState, req: &SubmitSearchRequest) -> Result<AcceptedSearch> {
    let spec = &req.request;
    validate_future_quote_scalars(&req.future)?;
    let account_currency = account_currency_from_msg(&req.future)?;
    let bar_seconds = data_geometry(&spec.data_type, spec.timeframe.as_deref())?;
    let limits = &state.strategies.limits;
    if limits.max_search_runs == 0 || limits.max_search_generation_points == 0 {
        return Err(invalid(
            "max_search_runs and max_search_generation_points must be positive".into(),
        ));
    }
    let direct_requested = spec.direct_factory.is_some();
    let template = if direct_requested {
        if !spec.template.is_null() || !spec.space.is_null() {
            return Err(invalid(
                "a direct factory request must use null template and space documents".into(),
            ));
        }
        if spec.structural.is_some()
            || spec.resource_limits.is_some()
            || !spec.variants.is_empty()
            || !spec.portfolio_candidates.is_empty()
            || spec.selected_rerun.is_some()
        {
            return Err(invalid(
                "a direct factory request cannot be combined with configured, structural, variant, portfolio, or selected-rerun modes".into(),
            ));
        }
        None
    } else {
        check_document_size(limits, "strategy template", &spec.template)?;
        check_document_size(limits, "space document", &spec.space)?;
        Some(decode_document("strategy template", &spec.template)?)
    };

    let mut symbols = Vec::new();
    for raw in &spec.symbols {
        symbols.push(required_symbol(&state.symbol_registry, raw)?);
    }
    let data_type = spec.data_type.to_lowercase();
    let mut enhanced_descriptors = BTreeMap::new();
    for value in &spec.series_descriptors {
        let descriptor: data_preprocess::SeriesDescriptor =
            serde_json::from_value(value.clone())
                .map_err(|error| invalid(format!("invalid series descriptor: {error}")))?;
        descriptor
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        if !descriptor.verified {
            return Err(invalid(
                "enhanced service input requires verified series descriptors".into(),
            ));
        }
        if !symbols.contains(&descriptor.symbol) {
            return Err(invalid(format!(
                "series descriptor symbol '{}' is not admitted",
                descriptor.symbol
            )));
        }
        let key = descriptor.symbol.clone();
        if enhanced_descriptors
            .insert(key.clone(), descriptor)
            .is_some()
        {
            return Err(invalid(format!("duplicate series descriptor for '{key}'")));
        }
    }
    if data_type == "price_bar" {
        if enhanced_descriptors.len() != symbols.len() {
            return Err(invalid(
                "price_bar search requires one verified descriptor per symbol".into(),
            ));
        }
        if enhanced_descriptors
            .values()
            .any(|descriptor| Some(descriptor.timeframe_seconds) != bar_seconds)
        {
            return Err(invalid(
                "price_bar descriptor timeframe differs from the request".into(),
            ));
        }
    } else if !enhanced_descriptors.is_empty() {
        return Err(invalid(
            "series_descriptors is only valid for price_bar input".into(),
        ));
    }
    let window_plan = window_plan_from_msg(&spec.windows)?;
    let pair_count = window_plan
        .pairs_with_limit(limits.max_search_runs)
        .map_err(research_error)?
        .len();
    let runs_per_point = symbols
        .len()
        .checked_mul(pair_count)
        .and_then(|count| count.checked_mul(2))
        .ok_or_else(|| invalid("search run multiplier overflowed".into()))?;
    if runs_per_point == 0 || runs_per_point > limits.max_search_runs {
        return Err(invalid(format!(
            "the search needs {runs_per_point} runs before adding any parameter points, above the server limit of {}",
            limits.max_search_runs
        )));
    }
    let max_points = limits.max_search_runs / runs_per_point;
    let space_limits = DeclaredSpaceLimits::new(
        limits.max_search_generation_points,
        limits.max_search_generation_points,
        max_points,
    )
    .map_err(research_error)?;
    let declared = template
        .map(|template| {
            DeclaredSpace::from_documents_with_limits(template, spec.space.clone(), space_limits)
                .map_err(research_error)
        })
        .transpose()?;
    if let Some(declared) = &declared {
        declared
            .validate_calendar_limits(
                CalendarAdmissionLimits::new(
                    limits.max_calendar_sessions,
                    limits.max_calendar_intervals,
                    limits.max_calendar_exceptions,
                    limits.max_calendar_history,
                    limits.max_calendar_children,
                    limits.max_calendar_bytes,
                )
                .map_err(|error| invalid(error.to_string()))?,
            )
            .map_err(invalid)?;
    }
    let mut checkpoint_limits =
        CheckpointLimits::new(limits.max_document_bytes, limits.max_search_runs)
            .map_err(research_error)?;
    let mut trace_limits = TraceLimits::new(limits.max_search_runs, limits.max_document_bytes)
        .map_err(research_error)?;
    let mut structural_resource_limits = None;
    let (family, generation_dispositions) = if direct_requested {
        (SearchFamily::Direct, None)
    } else if let Some(document) = &spec.structural {
        let request: StructuralRequestDocument = serde_json::from_value(document.clone())
            .map_err(|error| invalid(format!("invalid structural search document: {error}")))?;
        if let Some(limits) = &spec.resource_limits {
            let limits: StructuralResourceLimits = serde_json::from_value(limits.clone())
                .map_err(|error| invalid(format!("invalid structural resource limits: {error}")))?;
            if limits != request.limits {
                return Err(invalid(
                    "resource_limits must equal the structural document limits".into(),
                ));
            }
        }
        structural_resource_limits = Some(request.limits);
        checkpoint_limits = CheckpointLimits::new(
            request.limits.max_checkpoint_bytes,
            request.limits.max_checkpoint_records,
        )
        .map_err(research_error)?;
        trace_limits = TraceLimits::new(
            request.limits.max_trace_records,
            request.limits.max_trace_bytes,
        )
        .map_err(research_error)?;
        let declared = declared
            .as_ref()
            .expect("configured declaration exists for structural requests");
        let points = declared.points();
        if points.len() != 1 {
            return Err(invalid(
                "a structural request requires a one-point base declared space".into(),
            ));
        }
        let point = &points[0];
        let geometry_by_symbol = symbols
            .iter()
            .map(|symbol| (symbol.clone(), declared.geometry(symbol, point)))
            .collect();
        let structural = StructuralSearchSpec {
            family_id: request.family_id,
            base_document: declared.config(point),
            state_id: request.state_id,
            transition_priority: request.transition_priority,
            atoms: request.atoms,
            operators: request.operators,
            sequence_source: request.sequence_source,
            sequence_max_gap: request.sequence_max_gap,
            captures: request.captures,
            geometry_by_symbol,
            limits: request.limits,
        };
        let (generated, generation) =
            GeneratedStructuralFamily::new(&structural).map_err(research_error)?;
        let generated = generated
            .with_projectors(declared.projector_selections().map_err(invalid)?)
            .map_err(research_error)?;
        (
            SearchFamily::Structural(generated),
            Some(
                serde_json::to_value(generation.dispositions)
                    .map_err(|error| invalid(error.to_string()))?,
            ),
        )
    } else {
        if spec.resource_limits.is_some() {
            return Err(invalid(
                "resource_limits requires structural generation".into(),
            ));
        }
        (
            SearchFamily::Declared(
                declared.expect("configured declaration exists for declared requests"),
            ),
            None,
        )
    };
    if let Some(bar_seconds) = bar_seconds
        && !direct_requested
    {
        validate_search_bar_geometry(&family, &symbols, bar_seconds)?;
    }
    let config = config_from_msg(&spec.config, &state.symbol_registry, &symbols)?;
    let currency_plan = same_currency_plan(state, &account_currency, &symbols)?;
    let future = future_config_from_msg(&req.future, currency_plan)?;
    let evaluation =
        evaluation_options_from_msg_for_symbols(&req.evaluation, &state.symbol_registry, &symbols)?;
    if spec
        .entry_profile_routes
        .iter()
        .any(|route| matches!(route.profile, ProfileRef::Inline(_)))
    {
        return Err(invalid(
            "a search routes entry classes to registered profiles only".into(),
        ));
    }
    let profiles = resolve_entry_profiles(
        state,
        spec.profile.as_ref(),
        None,
        &spec.entry_profile_routes,
    )?;
    let direct_factory = spec
        .direct_factory
        .as_ref()
        .map(|request| trusted_direct_factory(request, bar_seconds, max_points))
        .transpose()?;

    let mut variants = Vec::with_capacity(spec.variants.len());
    for variant in &spec.variants {
        let variant_config = config_from_msg(&variant.config, &state.symbol_registry, &symbols)?;
        let variant_currency = same_currency_plan(state, &account_currency, &symbols)?;
        let variant_future = future_config_from_msg(&variant.future, variant_currency)?;
        let variant_profiles = resolve_entry_profiles(state, variant.profile.as_ref(), None, &[])?;
        variants.push(ExecutionVariant {
            id: variant.id.clone(),
            backtest: variant_config,
            future: variant_future,
            profiles: (!variant_profiles.is_empty()).then_some(variant_profiles),
            portfolio: None,
        });
    }
    let mut portfolio_candidates = Vec::with_capacity(spec.portfolio_candidates.len());
    let mut mixed_portfolio_candidates = Vec::new();
    for (candidate_index, candidate) in spec.portfolio_candidates.iter().enumerate() {
        if candidate.instances.is_empty() {
            return Err(invalid(format!(
                "portfolio_candidates[{candidate_index}] needs at least one instance"
            )));
        }
        let total_instances = candidate
            .instances
            .len()
            .checked_add(candidate.direct_instances.len())
            .ok_or_else(|| invalid("portfolio instance count overflowed".into()))?;
        if total_instances > limits.max_portfolio_instances {
            return Err(invalid(format!(
                "portfolio_candidates[{candidate_index}] exceeds the instance limit"
            )));
        }
        let mut identities = BTreeSet::new();
        let mut instances = Vec::with_capacity(candidate.instances.len());
        for (instance_index, item) in candidate.instances.iter().enumerate() {
            let symbol = required_symbol(&state.symbol_registry, &item.symbol)?;
            if !symbols.contains(&symbol) {
                return Err(invalid(format!(
                    "portfolio_candidates[{candidate_index}].instances[{instance_index}] symbol is not admitted by search.symbols"
                )));
            }
            check_document_size(limits, "strategy document", &item.strategy.document)?;
            let document: StrategyConfig =
                decode_document("strategy document", &item.strategy.document)?;
            let instance_id = item.strategy.instance_id.clone().ok_or_else(|| {
                invalid(format!(
                    "portfolio_candidates[{candidate_index}].instances[{instance_index}] requires strategy.instance_id"
                ))
            })?;
            if !identities.insert(instance_id.clone()) {
                return Err(invalid(format!(
                    "portfolio_candidates[{candidate_index}] has duplicate instance '{instance_id}'"
                )));
            }
            let geometry = item
                .strategy
                .sources
                .iter()
                .map(|binding| geometry_from_msg(binding, &symbol, bar_seconds))
                .collect::<Result<Vec<_>>>()?;
            let historical_inputs =
                historical_inputs_from_msg(item.strategy.historical_inputs.as_ref(), limits)?;
            let adapter = build_adapter(
                &document,
                &instance_id,
                &symbol,
                geometry.clone(),
                item.strategy.decision_latency_ms,
                &historical_inputs,
                limits,
            )?;
            let profiles = resolve_entry_profiles(
                state,
                item.profile.as_ref(),
                item.profile_def.as_ref(),
                &item.entry_profile_routes,
            )?;
            adapter
                .preflight_entry_profiles(&profiles)
                .map_err(|error| invalid(format!("portfolio profile routing: {error}")))?;
            instances.push(HeterogeneousInstanceSpec {
                instance_id,
                symbol,
                document,
                geometry,
                historical_inputs,
                profiles: (!profiles.is_empty()).then_some(profiles),
            });
        }
        let mut direct = Vec::with_capacity(candidate.direct_instances.len());
        for (instance_index, item) in candidate.direct_instances.iter().enumerate() {
            let symbol = required_symbol(&state.symbol_registry, &item.symbol)?;
            if !symbols.contains(&symbol) {
                return Err(invalid(format!(
                    "portfolio_candidates[{candidate_index}].direct_instances[{instance_index}] symbol is not admitted by search.symbols"
                )));
            }
            if !identities.insert(item.instance_id.clone()) {
                return Err(invalid(format!(
                    "portfolio_candidates[{candidate_index}] has duplicate instance '{}'",
                    item.instance_id
                )));
            }
            let factory = trusted_direct_factory(&item.factory, bar_seconds, 1)?;
            if factory.point_count() != 1 {
                return Err(invalid(
                    "a mixed direct instance requires exactly one factory point".into(),
                ));
            }
            let profiles = resolve_entry_profiles(state, item.profile.as_ref(), None, &[])?;
            direct.push(HeterogeneousDirectInstanceSpec {
                instance_id: item.instance_id.clone(),
                symbol,
                factory_name: factory.factory_name().into(),
                point: factory.point(0).expect("one factory point was admitted"),
                profiles: (!profiles.is_empty()).then_some(profiles),
            });
        }
        let (policies, groups) =
            decode_supervisor(candidate.policies.as_ref(), candidate.groups.as_ref())?;
        let portfolio = qs_research::PortfolioPlan { policies, groups };
        if direct.is_empty() {
            portfolio_candidates.push(HeterogeneousPortfolioCandidate {
                id: candidate.id.clone(),
                instances,
                portfolio,
            });
        } else {
            mixed_portfolio_candidates.push(MixedHeterogeneousPortfolioCandidate {
                id: candidate.id.clone(),
                configured: instances,
                direct,
                portfolio,
            });
        }
    }
    if (!portfolio_candidates.is_empty() || !mixed_portfolio_candidates.is_empty())
        && (!variants.is_empty()
            || spec.selected_rerun.is_some()
            || spec.resume_checkpoint.is_some())
    {
        return Err(invalid(
            "portfolio candidates cannot be combined with variants, resume, or selected rerun"
                .into(),
        ));
    }
    let portfolio_runs = portfolio_candidates
        .len()
        .checked_add(mixed_portfolio_candidates.len())
        .ok_or_else(|| invalid("portfolio candidate count overflowed".into()))?
        .checked_mul(pair_count)
        .and_then(|count| count.checked_mul(2))
        .ok_or_else(|| invalid("portfolio candidate run count overflowed".into()))?;
    if portfolio_runs > limits.max_search_runs {
        return Err(invalid(
            "portfolio candidate search exceeds max_search_runs".into(),
        ));
    }

    let resume_checkpoint = spec
        .resume_checkpoint
        .as_ref()
        .map(|value| {
            if !variants.is_empty() {
                return Err(invalid(
                    "checkpoint resume cannot be combined with execution variants".into(),
                ));
            }
            let checkpoint: SearchCheckpoint = serde_json::from_value(value.clone())
                .map_err(|error| invalid(format!("invalid search checkpoint: {error}")))?;
            checkpoint
                .validate(checkpoint_limits)
                .map_err(research_error)?;
            let expected = CheckpointDependency {
                experiment_id: checkpoint.dependency.experiment_id.clone(),
                caller_revision: "server-admission".into(),
                dataset_reference: state.data_dir.clone(),
                factory_revision: direct_factory
                    .as_ref()
                    .map(|factory| factory.revision().to_owned()),
            };
            if checkpoint.dependency != expected {
                return Err(invalid("search checkpoint dependencies changed".into()));
            }
            Ok(checkpoint)
        })
        .transpose()?;
    let selected_rerun = spec
        .selected_rerun
        .as_ref()
        .map(|selected| {
            if !variants.is_empty() {
                return Err(invalid(
                    "selected rerun cannot be combined with execution variants".into(),
                ));
            }
            let experiment = serde_json::from_value(selected.experiment_recipe.clone())
                .map_err(|error| invalid(format!("invalid experiment recipe: {error}")))?;
            let candidate = serde_json::from_value(selected.candidate_recipe.clone())
                .map_err(|error| invalid(format!("invalid candidate recipe: {error}")))?;
            let run: qs_research::RunRecipe =
                serde_json::from_value(selected.run_recipe.clone())
                    .map_err(|error| invalid(format!("invalid run recipe: {error}")))?;
            if selected.caller_revision.is_empty()
                || selected.caller_revision.len() > 256
                || selected.caller_revision.chars().any(char::is_control)
            {
                return Err(invalid("invalid selected rerun caller revision".into()));
            }
            if !symbols.contains(&run.symbol) {
                return Err(invalid(
                    "selected rerun symbol is not admitted by the request".into(),
                ));
            }
            let window_matches = window_plan
                .pairs_with_limit(limits.max_search_runs)
                .map_err(research_error)?
                .iter()
                .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
                .any(|window| {
                    window.label() == run.window
                        && window.from() == run.from
                        && window.to() == run.to
                });
            if !window_matches {
                return Err(invalid(
                    "selected rerun window differs from the admitted windows".into(),
                ));
            }
            let role = match selected.role {
                SearchEvaluationRoleMsg::Search => qs_research::EvaluationRole::Search,
                SearchEvaluationRoleMsg::Validation => qs_research::EvaluationRole::Validation,
                SearchEvaluationRoleMsg::Final => qs_research::EvaluationRole::Final,
            };
            Ok(AcceptedSelectedRerun {
                experiment,
                candidate,
                run,
                role,
                caller_revision: selected.caller_revision.clone(),
                release_final: selected.release_final,
                future_horizon_millis: selected.future_horizon_millis,
                embargo_millis: selected.embargo_millis,
            })
        })
        .transpose()?;
    let variant_multiplier = variants.len().max(1);
    let admitted_points = direct_factory.as_ref().map_or_else(
        || max_points.min(family.points().len()),
        |factory| factory.point_count(),
    );
    let variant_runs = runs_per_point
        .checked_mul(admitted_points)
        .and_then(|runs| runs.checked_mul(variant_multiplier))
        .ok_or_else(|| invalid("variant run count overflowed".into()))?;
    if variant_runs > limits.max_search_runs {
        return Err(invalid(
            "variant search exceeds max_search_runs before replay".into(),
        ));
    }
    let requested_worker_limit = structural_resource_limits
        .map_or(limits.max_search_workers, |resource| resource.max_workers)
        .min(limits.max_search_workers)
        .max(1);
    let workers = spec.workers.unwrap_or(1).clamp(1, requested_worker_limit);
    let admission_limits =
        ResearchAdmissionLimits::new(limits.max_search_runs, limits.max_search_runs)
            .map_err(research_error)?;
    let mut plan = ResearchPlan::new(symbols, window_plan, config)
        .with_future(future)
        .with_evaluation(evaluation)
        .with_workers(workers);
    plan.decision_latency_ms = spec.decision_latency_ms;
    if !profiles.is_empty() {
        plan = plan.with_entry_profiles(profiles);
    }
    let runs = if let Some(factory) = &direct_factory {
        if bar_seconds.is_some() {
            for pair in plan
                .window_plan
                .pairs_with_limit(admission_limits.max_window_pairs)
                .map_err(research_error)?
            {
                for boundary in [
                    pair.in_sample.from(),
                    pair.in_sample.to(),
                    pair.out_of_sample.from(),
                    pair.out_of_sample.to(),
                ] {
                    if boundary.and_utc().timestamp().rem_euclid(60) != 0
                        || boundary.and_utc().timestamp_subsec_nanos() != 0
                    {
                        return Err(invalid(
                            "trusted direct bar search boundaries must align to one-minute bars"
                                .into(),
                        ));
                    }
                }
            }
        }
        factory
            .point_count()
            .checked_mul(plan.symbols.len())
            .and_then(|runs| runs.checked_mul(pair_count))
            .and_then(|runs| runs.checked_mul(2))
            .ok_or_else(|| invalid("trusted direct run count overflowed".into()))?
    } else {
        if bar_seconds.is_some() {
            validate_bar_window_alignment_with_limits(&plan, &family, admission_limits)
                .map_err(research_error)?;
        }
        validate_batch_with_limits(&plan, &family, admission_limits).map_err(research_error)?
    };
    if runs > limits.max_search_runs {
        return Err(invalid(format!(
            "the search schedules {runs} runs, above the server limit of {}",
            limits.max_search_runs
        )));
    }
    if structural_resource_limits.is_some_and(|resource| runs > resource.max_runs) {
        return Err(invalid(
            "the structural search exceeds its declared run limit".into(),
        ));
    }
    let mut range = if let Some(selected) = &selected_rerun {
        selected_candidate_data_range(&selected.candidate, &selected.run).map_err(research_error)?
    } else if direct_factory.is_some() {
        let pairs = plan
            .window_plan
            .pairs_with_limit(admission_limits.max_window_pairs)
            .map_err(research_error)?;
        let from = pairs
            .iter()
            .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
            .map(DataWindow::from)
            .min()
            .and_then(|from| from.checked_sub_signed(chrono::Duration::minutes(1)))
            .ok_or_else(|| invalid("trusted direct loading range overflowed".into()))?;
        let to = pairs
            .iter()
            .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
            .map(DataWindow::to)
            .max()
            .ok_or_else(|| invalid("the search schedules no runs".into()))?;
        (from, to)
    } else {
        batch_data_range_with_limits(&plan, &family, admission_limits)
            .map_err(research_error)?
            .ok_or_else(|| invalid("the search schedules no runs".into()))?
    };
    let windows = plan
        .window_plan
        .pairs_with_limit(admission_limits.max_window_pairs)
        .map_err(research_error)?;
    for instance in portfolio_candidates
        .iter()
        .flat_map(|candidate| candidate.instances.iter())
        .chain(
            mixed_portfolio_candidates
                .iter()
                .flat_map(|candidate| candidate.configured.iter()),
        )
    {
        let adapter = build_adapter(
            &instance.document,
            &instance.instance_id,
            &instance.symbol,
            instance.geometry.clone(),
            plan.decision_latency_ms,
            &instance.historical_inputs,
            limits,
        )?;
        for window in windows
            .iter()
            .flat_map(|pair| [&pair.in_sample, &pair.out_of_sample])
        {
            let source_start = ConfiguredHistoricalBindings::from_geometry(
                instance.geometry.clone(),
                adapter.configured_requirements(),
            )
            .and_then(|bindings| bindings.warmup_start(window.from()))
            .map_err(|error| invalid(error.to_string()))?;
            let calendar_start =
                configured_history_start(&instance.historical_inputs, window.from())?;
            range.0 = range.0.min(source_start.min(calendar_start));
        }
    }
    let default_market_bytes = limits
        .max_retained_bars
        .checked_mul(std::mem::size_of::<qs_backtest::data_feed::FeedEvent>())
        .ok_or_else(|| invalid("server market byte limit overflowed".into()))?;
    let market_load_limits = MarketLoadLimits::new(
        structural_resource_limits.map_or(limits.max_retained_bars, |resource| {
            resource.max_retained_records
        }),
        structural_resource_limits.map_or(default_market_bytes, |resource| {
            resource.max_feed_bytes.min(resource.max_resident_bytes)
        }),
    )
    .map_err(ResearchError::from)
    .map_err(research_error)?;
    Ok(AcceptedSearch {
        family,
        plan,
        exchange: spec.exchange.to_lowercase(),
        data_type: spec.data_type.to_lowercase(),
        timeframe: spec.timeframe.clone(),
        range,
        admission_limits,
        generation_dispositions,
        checkpoint_limits,
        trace_limits,
        market_load_limits,
        enhanced_descriptors,
        resume_checkpoint,
        selected_rerun,
        portfolio_candidates,
        mixed_portfolio_candidates,
        variants,
        direct_factory,
    })
}

pub(super) fn run_search_job(state: Arc<ServerState>, job_id: String, search: AcceptedSearch) {
    let Some(cancellation) = start_job(&state, &job_id) else {
        return;
    };
    let outcome = execute_search(&state, &job_id, &search, &cancellation).and_then(|output| {
        let bytes = serde_json::to_vec(&output)
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
        let reference = state
            .artifact_store
            .persist_json(&bytes)
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
        Ok((output.summary, reference))
    });
    let checkpoint_reference = match &outcome {
        Err(BacktestServerError::CancelledWithCheckpoint(checkpoint)) => {
            serde_json::to_vec(checkpoint.as_ref())
                .map_err(|error| BacktestServerError::Serde(error.to_string()))
                .and_then(|bytes| {
                    state
                        .artifact_store
                        .persist_json(&bytes)
                        .map_err(|error| BacktestServerError::Serde(error.to_string()))
                })
                .ok()
        }
        _ => None,
    };

    let mut jobs = state.jobs.lock().unwrap();
    let Some(job) = jobs.get_mut(&job_id) else {
        return;
    };
    job.worker_active = false;
    let cancelled = cancellation.is_cancelled()
        || job.status == JobStatus::Cancelled
        || matches!(outcome, Err(BacktestServerError::Cancelled));
    if cancelled {
        if let Ok((_, reference)) = &outcome {
            let _ = state.artifact_store.delete(&reference.artifact_id);
        }
        mark_job_cancelled(&job_id, job);
        job.checkpoint_artifact = checkpoint_reference;
        publish_job_status(&job_id, job);
        return;
    }
    match outcome {
        Ok((summary, reference)) => {
            job.status = JobStatus::Completed;
            job.search = Some(summary);
            job.artifact = Some(reference);
            job.result = None;
            job.inline_complete = false;
            job.artifact_consumed = false;
            job.error = None;
            job.progress.stage = "completed".into();
            job.completed_at = Some(Instant::now());
            publish_job_status(&job_id, job);
        }
        Err(error) => mark_job_failed(&job_id, job, error.to_string()),
    }
}

fn collect_enhanced_stream(
    mut stream: qs_market_loader::MarketStream,
    limits: MarketLoadLimits,
    cancellation: &JobCancellationToken,
) -> Result<qs_research::SymbolEvents> {
    let mut events = Vec::new();
    let mut resident_bytes = 0usize;
    while let Some(batch) = stream
        .next_batch()
        .map_err(|error| map_streaming_replay_error(StreamingReplayError::Feed(error)))?
    {
        ensure_not_cancelled(Some(cancellation))?;
        let next_rows = events
            .len()
            .checked_add(batch.events.len())
            .ok_or_else(|| invalid("enhanced feed row count overflowed".into()))?;
        let batch_bytes = batch.events.iter().try_fold(0usize, |bytes, event| {
            bytes.checked_add(event.retained_bytes_upper_bound())
        });
        resident_bytes = resident_bytes
            .checked_add(
                batch_bytes.ok_or_else(|| invalid("enhanced feed byte count overflowed".into()))?,
            )
            .ok_or_else(|| invalid("enhanced feed byte count overflowed".into()))?;
        if next_rows > limits.max_rows || resident_bytes > limits.max_resident_bytes {
            return Err(invalid(
                "enhanced feed exceeds its admitted row or resident-byte limit".into(),
            ));
        }
        events.extend(batch.events);
    }
    ensure_not_cancelled(Some(cancellation))?;
    Ok(events.into())
}

fn checkpoint_from_batch(
    state: &ServerState,
    search: &AcceptedSearch,
    batch: &qs_research::ResearchBatch,
    frozen_selection: Option<qs_research::FrozenSelection>,
    split_access: Vec<qs_research::SplitAccessRecord>,
) -> Result<SearchCheckpoint> {
    let table = batch.table();
    let points_total = batch.candidate_recipes().len();
    let completed_runs = batch
        .run_recipes()
        .iter()
        .filter_map(|recipe| recipe.coverage.is_some().then_some(recipe.ordinal))
        .collect();
    let failures = batch
        .run_recipes()
        .iter()
        .filter(|recipe| recipe.coverage.is_none())
        .map(|recipe| {
            (
                recipe.ordinal,
                "run did not produce committed coverage".into(),
            )
        })
        .collect();
    let completed_candidates = batch
        .candidate_recipes()
        .iter()
        .filter(|candidate| {
            let runs = batch
                .run_recipes()
                .iter()
                .filter(|run| run.candidate_ordinal == candidate.ordinal)
                .collect::<Vec<_>>();
            !runs.is_empty() && runs.iter().all(|run| run.coverage.is_some())
        })
        .map(|candidate| candidate.ordinal)
        .collect();
    let mut committed_runs = std::collections::BTreeMap::new();
    for recipe in batch
        .run_recipes()
        .iter()
        .filter(|recipe| recipe.coverage.is_some())
    {
        let candidate = batch
            .candidate_recipes()
            .iter()
            .find(|candidate| candidate.ordinal == recipe.candidate_ordinal)
            .ok_or_else(|| BacktestServerError::Serde("run candidate recipe is absent".into()))?;
        let params = candidate
            .parameters
            .iter()
            .map(|(key, value)| (key.clone(), qs_strategy::parameter_value_label(value)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let row = table
            .rows()
            .iter()
            .find(|row| {
                row.family_id == candidate.family_id
                    && row.symbol == recipe.symbol
                    && row.window == recipe.window
                    && row.params == params
            })
            .cloned()
            .ok_or_else(|| BacktestServerError::Serde("completed run row is absent".into()))?;
        let positions = batch
            .position_outcomes()
            .iter()
            .filter(|position| {
                recipe
                    .run_tags
                    .iter()
                    .all(|(key, value)| position.dimensions.tags.get(key) == Some(value))
            })
            .cloned()
            .collect();
        committed_runs.insert(
            recipe.ordinal,
            CompletedRunCheckpoint {
                recipe: recipe.clone(),
                row,
                positions,
            },
        );
    }
    let checkpoint = SearchCheckpoint {
        dependency: CheckpointDependency {
            experiment_id: batch
                .experiment_recipe()
                .experiment_id
                .as_ref()
                .map(|id| id.as_str().to_owned()),
            caller_revision: batch
                .experiment_recipe()
                .caller_revision
                .clone()
                .unwrap_or_else(|| "server-admission".into()),
            dataset_reference: batch
                .experiment_recipe()
                .dataset_reference
                .clone()
                .unwrap_or_else(|| state.data_dir.clone()),
            factory_revision: batch
                .candidate_recipes()
                .iter()
                .find_map(|candidate| candidate.registered_factory.as_ref())
                .map(|factory| factory.revision.clone()),
        },
        experiment_recipe: Some(batch.experiment_recipe().clone()),
        candidate_recipes: batch
            .candidate_recipes()
            .iter()
            .cloned()
            .map(|candidate| (candidate.ordinal, candidate))
            .collect(),
        frozen_selection,
        frontier: u64::try_from(points_total).unwrap_or(u64::MAX),
        completed_candidates,
        completed_runs,
        committed_runs,
        failures,
        generated: search
            .generation_dispositions
            .as_ref()
            .and_then(|value| value.get("generated"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(points_total as u64),
        executed: u64::try_from(table.len()).unwrap_or(u64::MAX),
        generation_exhaustive: search
            .generation_dispositions
            .as_ref()
            .and_then(|value| value.get("unvisited"))
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|unvisited| unvisited == 0),
        split_access,
    };
    checkpoint
        .validate(search.checkpoint_limits)
        .map_err(research_error)?;
    Ok(checkpoint)
}

fn execute_search(
    state: &ServerState,
    job_id: &str,
    search: &AcceptedSearch,
    cancellation: &JobCancellationToken,
) -> Result<SearchResultMsg> {
    let _gate = state
        .strategies
        .search_gate
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ensure_not_cancelled(Some(cancellation))?;
    let family = search.family.clone();
    let (from, to) = search.range;
    let total_symbols = search.plan.symbols.len() as u64;
    let mut frozen_selection = search
        .resume_checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.frozen_selection.clone());
    let mut split_access = search
        .resume_checkpoint
        .as_ref()
        .map_or_else(Vec::new, |checkpoint| checkpoint.split_access.clone());
    let mut selected_protected = None;
    if let Some(selected) = &search.selected_rerun {
        let frozen = match frozen_selection.clone() {
            Some(frozen)
                if frozen.candidate == selected.candidate
                    && frozen.experiment == selected.experiment =>
            {
                frozen
            }
            Some(_) => {
                return Err(invalid(
                    "selected rerun differs from the persisted frozen selection".into(),
                ));
            }
            None => qs_research::FrozenSelection {
                candidate: selected.candidate.clone(),
                experiment: selected.experiment.clone(),
                caller_revision: selected.caller_revision.clone(),
                future_horizon_millis: selected.future_horizon_millis,
                embargo_millis: selected.embargo_millis,
            },
        };
        let mut protected = qs_research::ProtectedExperiment::restore(frozen, split_access)
            .map_err(research_error)?;
        let window = DataWindow::new(
            selected.run.window.clone(),
            selected.run.from,
            selected.run.to,
        )
        .map_err(research_error)?;
        if selected.release_final && !protected.is_final_released() {
            protected
                .release_final_for(&window, &selected.caller_revision)
                .map_err(research_error)?;
        }
        protected
            .access(selected.role, &window, &selected.caller_revision, true)
            .map_err(research_error)?;
        frozen_selection = protected.frozen().cloned();
        split_access = protected.records().to_vec();
        selected_protected = Some(protected);
    }
    let mut events = BTreeMap::new();
    for (index, symbol) in search.plan.symbols.iter().enumerate() {
        ensure_not_cancelled(Some(cancellation))?;
        let loaded = match search.data_type.as_str() {
            "bar" => load_symbol_bars(
                &state
                    .instrument_domain
                    .symbol_resolver(&state.symbol_registry),
                &state.data_dir,
                &search.exchange,
                symbol,
                search.timeframe.as_deref().expect("bar timeframe admitted"),
                Some(from),
                Some(to),
            )
            .map_err(research_error)?,
            "ordered_tick" => {
                let (disk_exchange, disk_symbol) = state
                    .instrument_domain
                    .symbol_resolver(&state.symbol_registry)
                    .resolve_tick_source(&state.data_dir, &search.exchange, symbol, &mut || {
                        cancellation.is_cancelled()
                    })?;
                let stream = open_ordered_stored_tick_stream(
                    &state.data_dir,
                    &disk_exchange,
                    &disk_symbol,
                    symbol,
                    data_preprocess::ParquetScanBounds::new(Some(from), Some(to)),
                    search.market_load_limits,
                    Arc::new({
                        let cancellation = cancellation.clone();
                        move || cancellation.is_cancelled()
                    }),
                )
                .map_err(ResearchError::from)
                .map_err(research_error)?;
                collect_enhanced_stream(stream, search.market_load_limits, cancellation)?
            }
            "price_bar" => {
                let descriptor = search
                    .enhanced_descriptors
                    .get(symbol)
                    .expect("price-bar descriptor admitted");
                let stream = open_price_bar_stream(
                    &state.data_dir,
                    descriptor,
                    symbol,
                    data_preprocess::ParquetScanBounds::new(Some(from), Some(to)),
                    search.market_load_limits,
                    Arc::new({
                        let cancellation = cancellation.clone();
                        move || cancellation.is_cancelled()
                    }),
                )
                .map_err(ResearchError::from)
                .map_err(research_error)?;
                collect_enhanced_stream(stream, search.market_load_limits, cancellation)?
            }
            _ => load_symbol_ticks(
                &state
                    .instrument_domain
                    .symbol_resolver(&state.symbol_registry),
                &state.data_dir,
                &search.exchange,
                symbol,
                Some(from),
                Some(to),
            )
            .map_err(research_error)?,
        };
        events.insert(symbol.clone(), loaded);
        update_job_progress(
            state,
            job_id,
            BacktestProgress {
                stage: "loading_data".into(),
                processed_symbols: index as u64 + 1,
                total_symbols,
                ..BacktestProgress::default()
            },
        );
    }

    let experiment_options = qs_research::ExperimentOptions {
        experiment_id: None,
        caller_revision: Some("server-admission".into()),
        dataset_reference: Some(state.data_dir.clone()),
    };
    let report_progress = |progress: qs_research::BatchProgress| {
        update_job_progress(
            state,
            job_id,
            BacktestProgress {
                stage: "replay".into(),
                processed_events: progress.completed_runs as u64,
                total_events: progress.total_runs as u64,
                processed_symbols: total_symbols,
                total_symbols,
                ..BacktestProgress::default()
            },
        )
    };
    let batch = if let Some(factory) = &search.direct_factory {
        ensure_not_cancelled(Some(cancellation))?;
        let completed = search
            .resume_checkpoint
            .as_ref()
            .map_or_else(BTreeSet::new, |checkpoint| {
                checkpoint.completed_runs.clone()
            });
        let batch = match run_direct_factory_batch_controlled_resume(
            &search.plan,
            factory,
            &events,
            experiment_options.clone(),
            search.admission_limits,
            &completed,
            &|| cancellation.is_cancelled(),
            &report_progress,
        ) {
            Ok(batch) => batch,
            Err(ResearchError::CancelledWithPartial(partial)) => {
                let partial = match &search.resume_checkpoint {
                    Some(checkpoint) => partial
                        .merge_checkpoint(checkpoint)
                        .map_err(research_error)?,
                    None => *partial,
                };
                let checkpoint = checkpoint_from_batch(
                    state,
                    search,
                    &partial,
                    frozen_selection.clone(),
                    split_access.clone(),
                )?;
                return Err(BacktestServerError::CancelledWithCheckpoint(Box::new(
                    checkpoint,
                )));
            }
            Err(error) => return Err(research_error(error)),
        };
        match &search.resume_checkpoint {
            Some(checkpoint) => batch.merge_checkpoint(checkpoint).map_err(research_error)?,
            None => batch,
        }
    } else if !search.mixed_portfolio_candidates.is_empty() {
        ensure_not_cancelled(Some(cancellation))?;
        let trusted = TrustedDirectFactory {
            points: Vec::new(),
            timeframe: SeriesTimeframe::minutes(1)
                .map_err(|error| invalid(format!("trusted factory timeframe: {error}")))?,
        };
        let factories = BTreeMap::from([(
            trusted.factory_name().to_owned(),
            &trusted as &dyn DirectResearchFactory,
        )]);
        let batch = run_mixed_heterogeneous_portfolios(
            &search.plan,
            &search.mixed_portfolio_candidates,
            &factories,
            &events,
            ExecutionVariantLimits::new(
                search.mixed_portfolio_candidates.len(),
                search.admission_limits.max_scheduled_runs,
            )
            .map_err(research_error)?,
        )
        .map_err(research_error)?
        .combined_portfolios()
        .map_err(research_error)?;
        ensure_not_cancelled(Some(cancellation))?;
        batch
    } else if !search.portfolio_candidates.is_empty() {
        ensure_not_cancelled(Some(cancellation))?;
        let batch = run_heterogeneous_portfolios_controlled(
            &search.plan,
            &search.portfolio_candidates,
            &events,
            ExecutionVariantLimits::new(
                search.portfolio_candidates.len(),
                search.admission_limits.max_scheduled_runs,
            )
            .map_err(research_error)?,
            &|| cancellation.is_cancelled(),
            &report_progress,
        )
        .map_err(research_error)?
        .combined_portfolios()
        .map_err(research_error)?;
        ensure_not_cancelled(Some(cancellation))?;
        batch
    } else if let Some(selected) = &search.selected_rerun {
        let protected = selected_protected
            .take()
            .expect("selected rerun access was admitted before market loading");
        let batch = qs_research::rerun_selected_candidate(
            &search.plan,
            &selected.experiment,
            &selected.candidate,
            &selected.run,
            &events,
        )
        .map_err(research_error)?;
        frozen_selection = protected.frozen().cloned();
        split_access = protected.records().to_vec();
        batch
    } else if search.variants.is_empty() {
        let completed = search
            .resume_checkpoint
            .as_ref()
            .map_or_else(std::collections::BTreeSet::new, |checkpoint| {
                checkpoint.completed_runs.clone()
            });
        let batch = match run_batch_controlled_with_experiment_resume(
            &search.plan,
            &family,
            &events,
            search.admission_limits,
            experiment_options.clone(),
            &completed,
            &|| cancellation.is_cancelled(),
            &report_progress,
        ) {
            Ok(batch) => batch,
            Err(ResearchError::CancelledWithPartial(partial)) => {
                let partial = match &search.resume_checkpoint {
                    Some(checkpoint) => partial
                        .merge_checkpoint(checkpoint)
                        .map_err(research_error)?,
                    None => *partial,
                };
                let checkpoint = checkpoint_from_batch(
                    state,
                    search,
                    &partial,
                    frozen_selection.clone(),
                    split_access.clone(),
                )?;
                return Err(BacktestServerError::CancelledWithCheckpoint(Box::new(
                    checkpoint,
                )));
            }
            Err(error) => return Err(research_error(error)),
        };
        match &search.resume_checkpoint {
            Some(checkpoint) => batch.merge_checkpoint(checkpoint).map_err(research_error)?,
            None => batch,
        }
    } else {
        ensure_not_cancelled(Some(cancellation))?;
        run_execution_variants_controlled(
            &search.plan,
            &family,
            &events,
            &search.variants,
            ExecutionVariantLimits::new(
                search.variants.len(),
                search.admission_limits.max_scheduled_runs,
            )
            .map_err(research_error)?,
            experiment_options,
            &|| cancellation.is_cancelled(),
            &|progress| {
                update_job_progress(
                    state,
                    job_id,
                    BacktestProgress {
                        stage: "replay".into(),
                        processed_events: progress.completed_runs as u64,
                        total_events: progress.total_runs as u64,
                        processed_symbols: total_symbols,
                        total_symbols,
                        ..BacktestProgress::default()
                    },
                )
            },
        )
        .map_err(research_error)?
        .combined()
        .map_err(research_error)?
    };

    let table = batch.table();
    let completed_rows = table
        .rows()
        .iter()
        .filter(|row| row.status.is_completed())
        .count();
    let points_total = table.rows().first().map_or(0, |row| row.points_total);
    let evaluation = serde_json::to_value(batch.evaluate(search.plan.evaluation.clone()))
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
    let bound_documents = (0..points_total)
        .filter_map(|point| batch.bound_document(point))
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
    let checkpoint = checkpoint_from_batch(state, search, &batch, frozen_selection, split_access)?;
    let mut generation_dispositions = search.generation_dispositions.clone();
    if let Some(serde_json::Value::Object(dispositions)) = generation_dispositions.as_mut() {
        dispositions.insert(
            "executed".into(),
            serde_json::json!(u64::try_from(table.len()).unwrap_or(u64::MAX)),
        );
        dispositions.insert(
            "failed".into(),
            serde_json::json!(u64::try_from(table.len() - completed_rows).unwrap_or(u64::MAX)),
        );
    }
    let checkpoint = serde_json::to_value(&checkpoint)
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
    let experiment_recipe = Some(
        serde_json::to_value(batch.experiment_recipe())
            .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
    );
    let candidate_recipes = batch
        .candidate_recipes()
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?;
    let run_recipes = batch
        .run_recipes()
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?;

    let (selected_evidence, trace) = if search.selected_rerun.is_some() {
        let evidence = qs_research::selected_candidate_evidence(
            batch.position_outcomes(),
            "regime",
            UncertaintyAssessment::Incomplete {
                reason: "no dependence-aware uncertainty method was selected".into(),
            },
        )
        .map_err(research_error)?;
        let mut trace = BoundedTrace::default();
        for position in batch.position_outcomes() {
            let observed = chrono::DateTime::from_timestamp_millis(position.ordinal)
                .map(|value| value.naive_utc())
                .unwrap_or_else(|| batch.run_recipes()[0].to);
            trace
                .push(
                    TraceRecord {
                        feature: "position_outcome".into(),
                        value: Some(position.outcome),
                        valid: position.outcome.is_finite(),
                        source: position.dimensions.symbol.clone(),
                        sample_at: observed,
                        available_at: observed,
                        predicate: None,
                        event: Some("position_closed".into()),
                        capture: None,
                        decision_id: None,
                        command_id: None,
                        position_id: Some(position.id.clone()),
                        entry_regime: position.dimensions.tags.get("regime").cloned(),
                        fill_regime: position.dimensions.tags.get("fill_regime").cloned(),
                        hindsight_regime: position.dimensions.tags.get("hindsight_regime").cloned(),
                    },
                    search.trace_limits,
                )
                .map_err(research_error)?;
        }
        (
            Some(
                serde_json::to_value(evidence)
                    .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
            ),
            Some(
                serde_json::to_value(trace)
                    .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
            ),
        )
    } else {
        (None, None)
    };

    Ok(SearchResultMsg {
        summary: SearchSummaryMsg {
            points_total,
            rows: table.len(),
            completed_rows,
            failed_rows: table.len() - completed_rows,
            data_mode: data_mode(&search.data_type).into(),
        },
        table_csv: table.to_csv(),
        evaluation,
        bound_documents,
        experiment_recipe,
        candidate_recipes,
        run_recipes,
        generation_dispositions,
        checkpoint: Some(checkpoint),
        selected_evidence,
        trace,
    })
}

// ── Shared validation ───────────────────────────────────────────────────────

fn invalid(message: String) -> BacktestServerError {
    BacktestServerError::InvalidRequest(message)
}

fn research_error(error: ResearchError) -> BacktestServerError {
    match error {
        ResearchError::Cancelled => BacktestServerError::Cancelled,
        other => invalid(other.to_string()),
    }
}

fn data_mode(data_type: &str) -> &'static str {
    if data_type.eq_ignore_ascii_case("bar") || data_type.eq_ignore_ascii_case("price_bar") {
        "bars"
    } else {
        "ticks"
    }
}

fn required_symbol(registry: &SymbolRegistry, raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid("symbol is required".into()));
    }
    Ok(normalize_symbol(registry, trimmed))
}

/// Accept `tick`, or `bar` with a fixed-duration timeframe, and return the bar duration in seconds.
fn trusted_direct_factory(
    request: &SearchDirectFactoryMsg,
    bar_seconds: Option<u64>,
    max_points: usize,
) -> Result<TrustedDirectFactory> {
    if request.name != "noop_direct" || request.revision != "r1" {
        return Err(invalid(format!(
            "unknown trusted direct factory '{}@{}'",
            request.name, request.revision
        )));
    }
    if request.points.is_empty() || request.points.len() > max_points {
        return Err(invalid(
            "trusted direct factory point count is empty or exceeds admission".into(),
        ));
    }
    if bar_seconds.is_some_and(|seconds| seconds != 60) {
        return Err(invalid(
            "noop_direct@r1 requires one-minute bars when bar input is selected".into(),
        ));
    }
    let mut points = Vec::with_capacity(request.points.len());
    for (point_index, point) in request.points.iter().enumerate() {
        if point.parameters.len() != 1 || !point.parameters.contains_key("warmup_bars") {
            return Err(invalid(format!(
                "direct factory point {point_index} requires only integer warmup_bars"
            )));
        }
        let mut values = Vec::with_capacity(point.parameters.len());
        for (name, value) in &point.parameters {
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                return Err(invalid(format!(
                    "direct factory point {point_index} has an invalid parameter name"
                )));
            }
            let value = match value {
                SearchFactoryParameterMsg::Integer(value)
                    if name == "warmup_bars" && (1..=64).contains(value) =>
                {
                    ParameterValue::Integer(*value)
                }
                SearchFactoryParameterMsg::Integer(_) => {
                    return Err(invalid(format!(
                        "direct factory point {point_index} warmup_bars must be 1 through 64"
                    )));
                }
                SearchFactoryParameterMsg::Number(value) if value.is_finite() => {
                    ParameterValue::Number(*value)
                }
                SearchFactoryParameterMsg::Number(_) => {
                    return Err(invalid(format!(
                        "direct factory point {point_index} has a non-finite number"
                    )));
                }
                SearchFactoryParameterMsg::Choice(value)
                    if !value.is_empty()
                        && value.len() <= 64
                        && !value.chars().any(char::is_control) =>
                {
                    ParameterValue::Choice(value.clone())
                }
                SearchFactoryParameterMsg::Choice(_) => {
                    return Err(invalid(format!(
                        "direct factory point {point_index} has an invalid choice"
                    )));
                }
            };
            values.push((name.clone(), value));
        }
        points.push(DirectFactoryPoint {
            binding: ParameterBinding::new(values),
        });
    }
    Ok(TrustedDirectFactory {
        points,
        timeframe: SeriesTimeframe::minutes(1)
            .map_err(|error| invalid(format!("trusted factory timeframe: {error}")))?,
    })
}

fn data_geometry(data_type: &str, timeframe: Option<&str>) -> Result<Option<u64>> {
    match data_type.to_lowercase().as_str() {
        "tick" | "ordered_tick" => match timeframe {
            None => Ok(None),
            Some(_) => Err(invalid("a tick request takes no timeframe".into())),
        },
        "bar" | "price_bar" => {
            let raw =
                timeframe.ok_or_else(|| invalid("a bar request requires timeframe".into()))?;
            let parsed = data_preprocess::models::Timeframe::parse(raw)
                .map_err(|error| invalid(error.to_string()))?;
            let seconds = parsed
                .fixed_duration_seconds()
                .and_then(|seconds| u64::try_from(seconds).ok())
                .ok_or_else(|| invalid(format!("timeframe {raw} has no fixed duration")))?;
            Ok(Some(seconds))
        }
        other => Err(invalid(format!(
            "data_type must be 'tick', 'bar', 'ordered_tick', or 'price_bar', got '{other}'"
        ))),
    }
}

fn validate_server_bar_bounds(
    context: &str,
    geometry: &[SeriesGeometry],
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<()> {
    let mut shortest = BTreeMap::<&str, u64>::new();
    for series in geometry {
        let duration = series.timeframe.duration_seconds();
        shortest
            .entry(series.symbol.as_str())
            .and_modify(|current| *current = (*current).min(duration))
            .or_insert(duration);
    }
    for series in geometry {
        if shortest.get(series.symbol.as_str()) != Some(&series.timeframe.duration_seconds()) {
            continue;
        }
        for (name, timestamp) in [("from", from), ("to", to)] {
            if let Some(timestamp) = timestamp
                && !series.is_aligned(timestamp)
            {
                return Err(invalid(format!(
                    "{context} {name} {timestamp} is not aligned to source '{}' {}s bars with offset {}s",
                    series.source,
                    series.timeframe.duration_seconds(),
                    series.alignment_offset_seconds
                )));
            }
        }
    }
    Ok(())
}

fn validate_search_bar_geometry<F: StrategyFamily>(
    family: &F,
    symbols: &[String],
    bar_seconds: u64,
) -> Result<()> {
    for (point_index, point) in family.points().iter().enumerate() {
        for symbol in symbols {
            for geometry in family.geometry(symbol, point) {
                let declared = geometry.timeframe.duration_seconds();
                if declared != bar_seconds {
                    return Err(invalid(format!(
                        "search point {point_index} source '{}' declares {declared}s bars but the request loads {bar_seconds}s bars",
                        geometry.source
                    )));
                }
            }
        }
    }
    Ok(())
}

fn check_document_size(
    limits: &crate::config::StrategiesSection,
    what: &str,
    value: &serde_json::Value,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| BacktestServerError::Serde(error.to_string()))?
        .len();
    if bytes > limits.max_document_bytes {
        return Err(invalid(format!(
            "{what} is {bytes} bytes, above the server limit of {} bytes",
            limits.max_document_bytes
        )));
    }
    Ok(())
}

fn decode_document(what: &str, value: &serde_json::Value) -> Result<StrategyConfig> {
    serde_json::from_value(value.clone())
        .map_err(|error| invalid(format!("invalid {what}: {error}")))
}

fn geometry_from_msg(
    binding: &SourceBindingMsg,
    symbol: &str,
    bar_seconds: Option<u64>,
) -> Result<SeriesGeometry> {
    if let Some(bar_seconds) = bar_seconds
        && u64::from(binding.timeframe_seconds) != bar_seconds
    {
        return Err(invalid(format!(
            "source '{}' declares {}s bars but the request loads {bar_seconds}s bars",
            binding.source, binding.timeframe_seconds
        )));
    }
    let source = SourceId::new(&binding.source)
        .map_err(|error| invalid(format!("source '{}': {error}", binding.source)))?;
    let timeframe = SeriesTimeframe::seconds(binding.timeframe_seconds)
        .map_err(|error| invalid(format!("source '{}': {error}", binding.source)))?;
    let price_basis = match binding.price_basis {
        PriceBasisMsg::Bid => PriceBasis::Bid,
        PriceBasisMsg::Ask => PriceBasis::Ask,
        PriceBasisMsg::Mid => PriceBasis::Mid,
    };
    Ok(SeriesGeometry::new(
        source,
        symbol,
        timeframe,
        price_basis,
        binding.alignment_offset_seconds,
    ))
}

fn historical_inputs_from_msg(
    message: Option<&HistoricalInputsMsg>,
    server: &crate::config::StrategiesSection,
) -> Result<Vec<ConfiguredCalendarInput>> {
    let Some(message) = message else {
        return Ok(Vec::new());
    };
    let server_limits = CalendarAdmissionLimits::new(
        server.max_calendar_sessions,
        server.max_calendar_intervals,
        server.max_calendar_exceptions,
        server.max_calendar_history,
        server.max_calendar_children,
        server.max_calendar_bytes,
    )
    .map_err(|error| invalid(error.to_string()))?;
    let limits = match message.limits {
        Some(value)
            if value.max_sessions > server_limits.max_sessions
                || value.max_market_intervals > server_limits.max_market_intervals
                || value.max_exceptions > server_limits.max_exceptions
                || value.max_history_occurrences > server_limits.max_history_occurrences
                || value.max_resolved_children > server_limits.max_resolved_children
                || value.max_owned_bytes > server_limits.max_owned_bytes =>
        {
            return Err(invalid(
                "calendar request limits cannot exceed the server limits".into(),
            ));
        }
        Some(value) => CalendarAdmissionLimits::new(
            value.max_sessions.min(server_limits.max_sessions),
            value
                .max_market_intervals
                .min(server_limits.max_market_intervals),
            value.max_exceptions.min(server_limits.max_exceptions),
            value
                .max_history_occurrences
                .min(server_limits.max_history_occurrences),
            value
                .max_resolved_children
                .min(server_limits.max_resolved_children),
            value.max_owned_bytes.min(server_limits.max_owned_bytes),
        )
        .map_err(|error| invalid(error.to_string()))?,
        None => server_limits,
    };
    let calendars = message
        .calendars
        .iter()
        .map(|(id, calendar)| {
            if id != &calendar.id {
                return Err(invalid(format!(
                    "calendar map key '{id}' differs from calendar id '{}'",
                    calendar.id
                )));
            }
            let day_boundary = parse_local_time(&calendar.day_boundary)?;
            let sessions = match &calendar.sessions {
                SessionScheduleMsg::FullDay => SessionScheduleSpec::FullDay,
                SessionScheduleMsg::Custom { items } => SessionScheduleSpec::Custom {
                    items: items
                        .iter()
                        .map(|item| {
                            let span = match &item.span {
                                SessionSpanMsg::FullDay => SessionSpanSpec::FullDay,
                                SessionSpanMsg::Timed {
                                    start,
                                    end,
                                    end_day_offset,
                                } => SessionSpanSpec::Timed {
                                    start: parse_local_time(start)?,
                                    end: parse_local_time(end)?,
                                    end_day_offset: *end_day_offset,
                                },
                            };
                            Ok(NamedSessionSpec {
                                id: item.id.clone(),
                                timezone: item.timezone.clone(),
                                span,
                                weekdays: item.weekdays.iter().copied().collect(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                },
            };
            let market = match &calendar.market {
                MarketScheduleMsg::Unspecified => MarketScheduleSpec::Unspecified,
                MarketScheduleMsg::Continuous => MarketScheduleSpec::Continuous,
                MarketScheduleMsg::Weekly {
                    intervals,
                    exceptions,
                } => MarketScheduleSpec::Weekly {
                    intervals: intervals
                        .iter()
                        .map(|item| {
                            Ok(WeeklyMarketIntervalSpec {
                                weekday: item.weekday,
                                start: parse_local_time(&item.start)?,
                                end: parse_local_time(&item.end)?,
                                end_day_offset: item.end_day_offset,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    exceptions: exceptions
                        .iter()
                        .map(|(date, items)| {
                            let date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
                                .map_err(|error| {
                                    invalid(format!("calendar exception date: {error}"))
                                })?;
                            let items = items
                                .iter()
                                .map(|item| {
                                    Ok(LocalMarketIntervalSpec {
                                        start: parse_local_time(&item.start)?,
                                        end: parse_local_time(&item.end)?,
                                        end_day_offset: item.end_day_offset,
                                    })
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Ok((date, items))
                        })
                        .collect::<Result<BTreeMap<_, _>>>()?,
                },
            };
            Ok((
                id.clone(),
                TradingCalendarSpec {
                    id: calendar.id.clone(),
                    timezone: calendar.timezone.clone(),
                    day_boundary,
                    sessions,
                    market,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    if calendars.len() > server.max_calendar_sessions
        || message.inputs.len() > server.max_calendar_sessions
    {
        return Err(invalid(
            "calendar or historical-input count exceeds the server limit".into(),
        ));
    }
    let mut names = BTreeSet::new();
    let configured = message
        .inputs
        .iter()
        .map(|input| {
            if !names.insert(input.name.clone()) {
                return Err(invalid(format!(
                    "duplicate historical input '{}'",
                    input.name
                )));
            }
            let calendar = calendars.get(&input.calendar).ok_or_else(|| {
                invalid(format!(
                    "historical input '{}' references unknown calendar '{}'",
                    input.name, input.calendar
                ))
            })?;
            let configured = ConfiguredCalendarInput {
                name: input.name.clone(),
                source: input.source.clone(),
                calendar: calendar.clone(),
                input: CalendarInputSpec {
                    calendar_id: input.calendar.clone(),
                    kind: calendar_feature_from_msg(input.feature),
                    session_id: input.session.clone(),
                    time_basis: match input.time_basis {
                        CalendarTimeBasisMsg::SourceOpen => CalendarTimeBasis::SourceOpen,
                        CalendarTimeBasisMsg::DecisionTime => CalendarTimeBasis::DecisionTime,
                    },
                    opening_range_minutes: input.opening_range_minutes,
                    child_seconds: input.child_seconds,
                    alignment_offset_seconds: input.alignment_offset_seconds,
                    maximum_history: input.maximum_history,
                },
                limits,
            };
            configured
                .binding()
                .map_err(|error| invalid(format!("historical input '{}': {error}", input.name)))?;
            Ok(configured)
        })
        .collect::<Result<Vec<_>>>()?;
    let total_bytes = configured.iter().try_fold(0usize, |total, input| {
        input
            .estimated_owned_bytes()
            .map_err(|error| invalid(error.to_string()))
            .and_then(|bytes| {
                total
                    .checked_add(bytes)
                    .ok_or_else(|| invalid("calendar aggregate byte count overflowed".into()))
            })
    })?;
    if total_bytes > server.max_calendar_bytes {
        return Err(invalid(format!(
            "calendar inputs need an estimated {total_bytes} bytes, above the server limit of {}",
            server.max_calendar_bytes
        )));
    }
    Ok(configured)
}

fn configured_history_start(
    inputs: &[ConfiguredCalendarInput],
    evaluation_start: NaiveDateTime,
) -> Result<NaiveDateTime> {
    inputs.iter().try_fold(evaluation_start, |start, input| {
        input
            .history_start(evaluation_start)
            .map(|candidate| start.min(candidate))
            .map_err(|error| invalid(error.to_string()))
    })
}

fn parse_local_time(value: &str) -> Result<chrono::NaiveTime> {
    chrono::NaiveTime::parse_from_str(value, "%H:%M:%S")
        .map_err(|error| invalid(format!("calendar local time '{value}': {error}")))
}

fn calendar_feature_from_msg(value: CalendarFeatureMsg) -> CalendarFeatureKind {
    match value {
        CalendarFeatureMsg::LocalSecondOfDay => CalendarFeatureKind::LocalSecondOfDay,
        CalendarFeatureMsg::SessionElapsedSeconds => CalendarFeatureKind::SessionElapsedSeconds,
        CalendarFeatureMsg::SessionMembership => CalendarFeatureKind::SessionMembership,
        CalendarFeatureMsg::PreviousSessionHigh => CalendarFeatureKind::PreviousSessionHigh,
        CalendarFeatureMsg::PreviousSessionLow => CalendarFeatureKind::PreviousSessionLow,
        CalendarFeatureMsg::PreviousDayHigh => CalendarFeatureKind::PreviousDayHigh,
        CalendarFeatureMsg::PreviousDayLow => CalendarFeatureKind::PreviousDayLow,
        CalendarFeatureMsg::PreviousWeekHigh => CalendarFeatureKind::PreviousWeekHigh,
        CalendarFeatureMsg::PreviousWeekLow => CalendarFeatureKind::PreviousWeekLow,
        CalendarFeatureMsg::OpeningRangeHighSoFar => CalendarFeatureKind::OpeningRangeHighSoFar,
        CalendarFeatureMsg::OpeningRangeLowSoFar => CalendarFeatureKind::OpeningRangeLowSoFar,
        CalendarFeatureMsg::OpeningRangeHighFinal => CalendarFeatureKind::OpeningRangeHighFinal,
        CalendarFeatureMsg::OpeningRangeLowFinal => CalendarFeatureKind::OpeningRangeLowFinal,
        CalendarFeatureMsg::PastSameSlotRangeRatio => CalendarFeatureKind::PastSameSlotRangeRatio,
        CalendarFeatureMsg::PastSameSlotCount => CalendarFeatureKind::PastSameSlotCount,
    }
}

/// Compile the document with the built-in material library and bind its sources, rejecting anything the adapter or the retained-history limit would refuse.
fn build_adapter(
    document: &StrategyConfig,
    instance_id: &str,
    symbol: &str,
    geometry: Vec<SeriesGeometry>,
    decision_latency_ms: u64,
    historical_inputs: &[ConfiguredCalendarInput],
    limits: &crate::config::StrategiesSection,
) -> Result<BacktestConfiguredStrategyAdapter> {
    let strategy = ConfiguredStrategy::compile(
        document.clone(),
        &MaterialLibrary::builtins(),
        instance_id,
        symbol,
    )
    .map_err(|error| invalid(format!("strategy does not compile: {error}")))?;
    let base = ConfiguredHistoricalBindings::from_geometry(geometry, strategy.input_requirements())
        .map_err(|error| invalid(format!("source binding: {error}")))?;
    let (sources, mut named, volume) = base.into_parts();
    let required = strategy
        .input_requirements()
        .named_inputs
        .iter()
        .map(|requirement| requirement.name.as_str())
        .collect::<BTreeSet<_>>();
    for input in historical_inputs {
        if !required.contains(input.name.as_str()) {
            continue;
        }
        if named.iter().any(|binding| binding.name() == input.name) {
            return Err(invalid(format!(
                "named input '{}' has more than one projector",
                input.name
            )));
        }
        named.push(
            input
                .binding()
                .map_err(|error| invalid(format!("historical input '{}': {error}", input.name)))?,
        );
    }
    let bindings = ConfiguredHistoricalBindings::new(sources, named, volume);
    let retained = bindings
        .sources()
        .iter()
        .map(|binding| binding.series().retained_bars())
        .sum::<usize>();
    if retained > limits.max_retained_bars {
        return Err(invalid(format!(
            "the strategy retains {retained} bars across its sources, above the server limit of {}",
            limits.max_retained_bars
        )));
    }
    let descriptor = StrategyDescriptor::new(
        StrategyId::new(document.strategy_id.clone())
            .map_err(|error| invalid(format!("strategy id: {error}")))?,
        instance_id,
        document.title.clone(),
    )
    .map_err(|error| invalid(format!("strategy descriptor: {error}")))?;
    BacktestConfiguredStrategyAdapter::new(strategy, descriptor, bindings, decision_latency_ms)
        .map_err(|error| invalid(format!("source binding: {error}")))
}

fn window_plan_from_msg(windows: &SearchWindowsMsg) -> Result<WindowPlan> {
    let window = |message: &SearchWindowMsg| -> Result<DataWindow> {
        DataWindow::new(
            message.label.clone(),
            parse_datetime(&message.from)?,
            parse_datetime(&message.to)?,
        )
        .map_err(research_error)
    };
    let seconds = |value: i64, name: &str| -> Result<chrono::Duration> {
        if value <= 0 {
            return Err(invalid(format!("{name} must be positive")));
        }
        Ok(chrono::Duration::seconds(value))
    };
    Ok(match windows {
        SearchWindowsMsg::Fixed {
            in_sample,
            out_of_sample,
        } => WindowPlan::Fixed {
            in_sample: window(in_sample)?,
            out_of_sample: window(out_of_sample)?,
        },
        SearchWindowsMsg::RollingWalkForward {
            start,
            end,
            train_seconds,
            test_seconds,
            step_seconds,
        } => WindowPlan::RollingWalkForward {
            start: parse_datetime(start)?,
            end: parse_datetime(end)?,
            train: seconds(*train_seconds, "train_seconds")?,
            test: seconds(*test_seconds, "test_seconds")?,
            step: seconds(*step_seconds, "step_seconds")?,
        },
    })
}

/// A search replays primary events only, so every searched symbol must already settle in the account currency.
fn same_currency_plan(
    state: &ServerState,
    account_currency: &str,
    symbols: &[String],
) -> Result<RunCurrencyPlan> {
    let mut pnl = BTreeMap::new();
    for symbol in symbols {
        let currency = state
            .symbol_registry
            .pnl_currency(symbol)
            .ok_or_else(|| invalid(format!("symbol '{symbol}' has no P&L currency")))?;
        if !currency.eq_ignore_ascii_case(account_currency) {
            return Err(invalid(format!(
                "symbol '{symbol}' settles in {currency}; a search does not load conversion quotes, so account_currency must be {currency}"
            )));
        }
        pnl.insert(symbol.clone(), account_currency.to_owned());
    }
    RunCurrencyPlan::new(
        account_currency,
        symbols.iter().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::new(),
        pnl,
        BTreeMap::from([(
            account_currency.to_owned(),
            qs_backtest::ConversionRoute::Identity {
                currency: account_currency.to_owned(),
            },
        )]),
        Vec::new(),
    )
    .map_err(BacktestServerError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMPLATE: &str = include_str!("../../../research/examples/ema_strategy.toml");
    const SPACE: &str = r#"
family_id = "ema_cross"

[parameters.ema_fast]
values = [3]

[parameters.ema_slow]
values = [8]

[parameters.atr_stop]
values = [1.5]

[parameters.entry]
values = ["cross"]

[[series]]
source = "primary"
symbol = { plan_symbol = true }
timeframe_seconds = 60
price_basis = "mid"
alignment_offset_seconds = 0
"#;

    fn at(minutes: i64) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + chrono::Duration::minutes(minutes)
    }

    fn state() -> Arc<ServerState> {
        let data_dir = std::env::temp_dir().join(format!(
            "qs_backtest_server_search_gate_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let store = ParquetStore::open(&data_dir).unwrap();
        let ticks = (0..400)
            .map(|minute| {
                let price = 1.1 + ((minute % 60) as f64 - 30.0) * 1.0e-5;
                data_preprocess::Tick {
                    exchange: "fixture".into(),
                    symbol: "EURUSD".into(),
                    ts: at(minute),
                    bid: Some(price),
                    ask: Some(price + 2.0e-5),
                    last: None,
                    volume: None,
                    flags: None,
                }
            })
            .collect::<Vec<_>>();
        store.insert_ticks(&ticks).unwrap();
        let symbol_registry = SymbolRegistry::from_toml(
            r#"
[[symbol]]
canonical = "eurusd"
pip_position = 4
digits = 5
category = "forex"
base_currency = "EUR"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000
lot_step_units = 1000
"#,
        )
        .unwrap();
        let instrument_domain =
            crate::instrument_catalog::InstrumentDomain::compatibility(&symbol_registry).unwrap();
        Arc::new(ServerState {
            symbol_registry,
            instrument_domain,
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: data_dir.to_string_lossy().into_owned(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: Mutex::new(HashMap::new()),
            max_retained_jobs: 100,
            artifact_store: ArtifactStore::new(
                data_dir.join("artifacts"),
                1024 * 1024,
                64 * 1024,
                Duration::from_secs(3_600),
                64 * 1024 * 1024,
            )
            .unwrap(),
            strategies: StrategyServiceState::default(),
        })
    }

    fn request(workers: Option<usize>) -> SubmitSearchRequest {
        let value = |text: &str| {
            serde_json::to_value(toml::from_str::<toml::Value>(text).unwrap()).unwrap()
        };
        let window = |label: &str, from: i64, to: i64| SearchWindowMsg {
            label: label.into(),
            from: at(from).format("%Y-%m-%dT%H:%M:%S").to_string(),
            to: at(to).format("%Y-%m-%dT%H:%M:%S").to_string(),
        };
        SubmitSearchRequest {
            request: SearchRunSpec {
                template: value(TEMPLATE),
                space: value(SPACE),
                symbols: vec!["EURUSD".into()],
                exchange: "fixture".into(),
                data_type: "tick".into(),
                timeframe: None,
                windows: SearchWindowsMsg::Fixed {
                    in_sample: window("is", 100, 250),
                    out_of_sample: window("oos", 250, 390),
                },
                config: BacktestConfigMsg {
                    initial_balance: Some(10_000.0),
                    close_on_finish: Some(true),
                    fill_model: None,
                    sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.1 }),
                    costs: Default::default(),
                },
                profile: None,
                entry_profile_routes: vec![],
                workers,
                decision_latency_ms: 0,
                structural: None,
                resource_limits: None,
                variants: vec![],
                direct_factory: None,
                resume_checkpoint: None,
                selected_rerun: None,
                portfolio_candidates: vec![],
                series_descriptors: vec![],
            },
            future: FutureQuoteConfigMsg {
                account_currency: "USD".into(),
                ..FutureQuoteConfigMsg::default()
            },
            evaluation: ProviderEvaluationOptionsMsg::default(),
        }
    }

    fn status(state: &ServerState, job_id: &str) -> JobStatus {
        state.jobs.lock().unwrap()[job_id].status.clone()
    }

    #[test]
    fn a_search_waits_while_another_search_holds_the_gate() {
        let state = state();
        let job_id = handle_submit_search(&state, &request(None)).job_id.unwrap();
        let gate = state.strategies.search_gate.lock().unwrap();
        let worker = std::thread::spawn({
            let state = state.clone();
            let job_id = job_id.clone();
            move || run_job_and_store(state, job_id)
        });
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(status(&state, &job_id), JobStatus::LoadingData);
        drop(gate);
        worker.join().unwrap();
        assert_eq!(status(&state, &job_id), JobStatus::Completed);
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }

    #[test]
    fn a_requested_worker_count_is_capped_by_the_server_limit() {
        let state = state();
        let limit = state.strategies.limits.max_search_workers;
        assert_eq!(
            prepare_search(&state, &request(Some(64)))
                .unwrap()
                .plan
                .workers,
            limit
        );
        assert_eq!(
            prepare_search(&state, &request(Some(1)))
                .unwrap()
                .plan
                .workers,
            1
        );
        assert_eq!(
            prepare_search(&state, &request(None)).unwrap().plan.workers,
            1
        );
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }
}
