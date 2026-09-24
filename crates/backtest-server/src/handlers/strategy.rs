//! Configured strategy runs, portfolio runs, and server-side parameter searches.
//!
//! All reuse the retained-job workflow of raw-signal backtests: one job map, status, watch, cancellation, and artifact store. A configured run loads its one symbol through the same stream, conversion, and instrument path a raw-signal run uses, starting at the warmup its compiled strategy derives. A portfolio run loads the symbols of all its instances through that path into one feed, starting at the earliest warmup, and replays every instance against one account under the policies it carries. A search loads each symbol once into memory and runs through `qs-research` behind a gate that admits one search at a time, because a search holds each symbol's whole range in memory.

use std::collections::{BTreeMap, BTreeSet};

use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter,
    ConfiguredHistoricalBindings, ConfiguredInstance, ConfiguredStrategyAdapterError,
    MAX_PORTFOLIO_INSTANCES, ObservationStoreLimits, PortfolioReplayError, PriceBasis,
    RunCurrencyPlan, SeriesGeometry, StrategyDescriptor, StrategyId, StrategyReplayError,
    StrategyRetentionLimits, Timeframe as SeriesTimeframe,
};
use qs_market_loader::MarketStreamError;
use qs_research::{
    DataWindow, DeclaredSpace, ResearchError, ResearchPlan, WindowPlan, batch_data_range,
    load_symbol_bars, load_symbol_ticks, run_batch_controlled, validate_batch,
};
use qs_risk::{CorrelationGroup, PortfolioSupervisor, RiskPolicy};
use qs_strategy::{ConfiguredStrategy, MaterialLibrary, SourceId, StrategyConfig};

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
    delivery: ResultDeliveryMsg,
}

/// A validated parameter search, kept until its worker starts.
#[derive(Debug, Clone)]
pub struct AcceptedSearch {
    template: StrategyConfig,
    space: serde_json::Value,
    plan: ResearchPlan,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    range: (NaiveDateTime, NaiveDateTime),
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
    let adapter = build_adapter(
        &document,
        &instance_id,
        &symbol,
        geometry.clone(),
        spec.strategy.decision_latency_ms,
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
    let from = parse_optional_datetime(&spec.from)?;
    let to = parse_optional_datetime(&spec.to)?;
    if let (Some(from), Some(to)) = (from, to)
        && from >= to
    {
        return Err(invalid(format!("from {from} must be before to {to}")));
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
        &state.strategies.limits,
    )?;
    let loading_start = match run.from {
        Some(from) => Some(
            ConfiguredHistoricalBindings::from_geometry(
                run.geometry.clone(),
                adapter.configured_requirements(),
            )
            .and_then(|bindings| bindings.warmup_start(from))
            .map_err(|error| invalid(error.to_string()))?,
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
    let mut cancelled = || cancellation.is_cancelled();
    let mut primary = describe_primary_market_stream(
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
        let adapter = build_adapter(
            &document,
            &instance_id,
            &symbol,
            geometry.clone(),
            item.strategy.decision_latency_ms,
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
            &state.strategies.limits,
        )?;
        if let Some(from) = run.from {
            let start = ConfiguredHistoricalBindings::from_geometry(
                instance.geometry.clone(),
                adapter.configured_requirements(),
            )
            .and_then(|bindings| bindings.warmup_start(from))
            .map_err(|error| invalid(error.to_string()))?;
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
    let mut cancelled = || cancellation.is_cancelled();
    let mut primary = describe_primary_market_stream(
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
    };
    let jobs = state.jobs.lock().unwrap();
    match jobs.get(&req.job_id) {
        None => failure(format!("Job '{}' not found", req.job_id)),
        Some(job) if job.kind != JobKind::Search => {
            failure("Job is not a parameter search; use get_backtest_result".into())
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
        },
    }
}

fn prepare_search(state: &ServerState, req: &SubmitSearchRequest) -> Result<AcceptedSearch> {
    let spec = &req.request;
    validate_future_quote_scalars(&req.future)?;
    let account_currency = account_currency_from_msg(&req.future)?;
    data_geometry(&spec.data_type, spec.timeframe.as_deref())?;
    let limits = &state.strategies.limits;
    check_document_size(limits, "strategy template", &spec.template)?;
    check_document_size(limits, "space document", &spec.space)?;
    let template: StrategyConfig = decode_document("strategy template", &spec.template)?;
    let family = DeclaredSpace::from_documents(template.clone(), spec.space.clone())
        .map_err(research_error)?;

    let mut symbols = Vec::new();
    for raw in &spec.symbols {
        symbols.push(required_symbol(&state.symbol_registry, raw)?);
    }
    let window_plan = window_plan_from_msg(&spec.windows)?;
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
    let workers = spec
        .workers
        .unwrap_or(1)
        .clamp(1, limits.max_search_workers.max(1));
    let mut plan = ResearchPlan::new(symbols, window_plan, config)
        .with_future(future)
        .with_evaluation(evaluation)
        .with_workers(workers);
    plan.decision_latency_ms = spec.decision_latency_ms;
    if !profiles.is_empty() {
        plan = plan.with_entry_profiles(profiles);
    }

    let runs = validate_batch(&plan, &family).map_err(research_error)?;
    if runs > limits.max_search_runs {
        return Err(invalid(format!(
            "the search schedules {runs} runs, above the server limit of {}",
            limits.max_search_runs
        )));
    }
    let range = batch_data_range(&plan, &family)
        .map_err(research_error)?
        .ok_or_else(|| invalid("the search schedules no runs".into()))?;
    Ok(AcceptedSearch {
        template,
        space: spec.space.clone(),
        plan,
        exchange: spec.exchange.to_lowercase(),
        data_type: spec.data_type.to_lowercase(),
        timeframe: spec.timeframe.clone(),
        range,
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
    let family = DeclaredSpace::from_documents(search.template.clone(), search.space.clone())
        .map_err(research_error)?;
    let (from, to) = search.range;
    let total_symbols = search.plan.symbols.len() as u64;
    let mut events = BTreeMap::new();
    for (index, symbol) in search.plan.symbols.iter().enumerate() {
        ensure_not_cancelled(Some(cancellation))?;
        let loaded = match search.timeframe.as_deref() {
            Some(timeframe) if search.data_type == "bar" => load_symbol_bars(
                &state.data_dir,
                &search.exchange,
                symbol,
                timeframe,
                Some(from),
                Some(to),
            ),
            _ => load_symbol_ticks(
                &state.data_dir,
                &search.exchange,
                symbol,
                Some(from),
                Some(to),
            ),
        }
        .map_err(research_error)?;
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

    let batch = run_batch_controlled(
        &search.plan,
        &family,
        &events,
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
    .map_err(research_error)?;

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
    if data_type.eq_ignore_ascii_case("bar") {
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
fn data_geometry(data_type: &str, timeframe: Option<&str>) -> Result<Option<u64>> {
    match data_type.to_lowercase().as_str() {
        "tick" => match timeframe {
            None => Ok(None),
            Some(_) => Err(invalid("a tick request takes no timeframe".into())),
        },
        "bar" => {
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
            "data_type must be 'tick' or 'bar', got '{other}'"
        ))),
    }
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

/// Compile the document with the built-in material library and bind its sources, rejecting anything the adapter or the retained-history limit would refuse.
fn build_adapter(
    document: &StrategyConfig,
    instance_id: &str,
    symbol: &str,
    geometry: Vec<SeriesGeometry>,
    decision_latency_ms: u64,
    limits: &crate::config::StrategiesSection,
) -> Result<BacktestConfiguredStrategyAdapter> {
    let strategy = ConfiguredStrategy::compile(
        document.clone(),
        &MaterialLibrary::builtins(),
        instance_id,
        symbol,
    )
    .map_err(|error| invalid(format!("strategy does not compile: {error}")))?;
    let bindings =
        ConfiguredHistoricalBindings::from_geometry(geometry, strategy.input_requirements())
            .map_err(|error| invalid(format!("source binding: {error}")))?;
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
