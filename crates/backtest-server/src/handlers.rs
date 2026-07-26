//! RPC handler implementations for the backtest server.
//!
//! Each handler receives a request message, processes it against the shared
//! server state (symbol registry, profile registry, Parquet store), and
//! returns a response message. Errors are captured in the response rather
//! than crashing the server.

use std::collections::{BTreeSet, HashMap};
#[cfg(test)]
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::NaiveDateTime;
#[cfg(test)]
use data_preprocess::DataError;
use data_preprocess::ParquetStore;
#[cfg(test)]
use data_preprocess::models::{BarQueryOpts, QueryOpts, Timeframe};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use qs_backtest::BacktestResult;
#[cfg(test)]
use qs_backtest::data_feed::{DataFeed, MarketEvent, VecFeed, bars_to_feed, ticks_to_feed};
use qs_backtest::data_feed::{EventBatchFeedError, KWayMergeError};
use qs_backtest::evaluation::EvaluationOptions;
use qs_backtest::profile::{ManagementProfile, ProfileRegistry, RawSignal};
use qs_backtest::runner::{
    BacktestConfig, BacktestRunner, FutureQuoteConfig, ReplayProgress, StreamingReplayError,
};
use qs_symbols::SymbolRegistry;
use tokio::sync::watch;

use crate::artifact_store::ArtifactStore;
use crate::convert::{
    account_currency_from_msg, config_from_msg, evaluation_options_from_msg_for_symbols,
    future_config_from_msg, profile_from_msg, raw_signal_from_msg, result_to_msg,
    validate_future_quote_scalars,
};
use crate::error::{BacktestServerError, Result};
use crate::fx_loader::describe_future_stream;
use crate::market_loader::{
    CancellationCheck, MarketStreamDescription, MarketStreamError, describe_primary_market_stream,
};
use crate::replay_plan::{ReplayPlan, RequestedSymbolScope};
use crate::rpc_types::*;

/// Shared state accessible by all client handlers.
pub struct ServerState {
    /// Symbol registry for normalization and contract size metadata.
    pub symbol_registry: SymbolRegistry,
    /// Management profile registry (TOML-loaded + dynamically added).
    pub profile_registry: RwLock<ProfileRegistry>,
    /// Root directory for Parquet market data.
    pub data_dir: String,
    /// Path to the profiles TOML file (for reload).
    pub profiles_path: String,
    /// Server start time for uptime reporting.
    pub start_time: Instant,
    /// Async backtest job storage (Issue 2).
    pub jobs: Mutex<HashMap<String, BacktestJob>>,
    /// Hard bound for queued, running, and retained terminal jobs.
    pub max_retained_jobs: usize,
    /// Filesystem storage for complete large result JSON payloads.
    pub artifact_store: ArtifactStore,
}

/// Internal representation of an async backtest job.
#[derive(Debug, Clone)]
pub struct BacktestJob {
    /// Current job status.
    pub status: JobStatus,
    /// When the job was submitted.
    pub submitted_at: Instant,
    /// When the job completed (if finished).
    pub completed_at: Option<Instant>,
    /// Structured loading and replay progress.
    pub progress: BacktestProgress,
    /// Complete inline result or compact console summary.
    pub result: Option<BacktestResultMsg>,
    /// Complete result artifact when the full inline object was released.
    pub artifact: Option<ResultArtifactRefMsg>,
    /// True when the complete result is present in `result`.
    pub inline_complete: bool,
    /// True after the job artifact has been deleted following delivery.
    pub artifact_consumed: bool,
    /// Error message (when failed).
    pub error: Option<String>,
    /// Cooperative cancellation shared with the blocking worker.
    pub cancellation: JobCancellationToken,
    /// True while a blocking worker slot is reserved or still accessing this job.
    pub worker_active: bool,
    /// Coalesced current status published to server-streaming subscribers.
    pub updates: watch::Sender<BacktestStatusResponse>,
}

/// Lightweight per-job cancellation token without an additional runtime dependency.
#[derive(Debug, Clone, Default)]
pub struct JobCancellationToken(Arc<AtomicBool>);

impl JobCancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Typed job status for the async backtest API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Queued,
    LoadingData,
    Running,
    Completed,
    Failed,
    Cancelled,
}

enum PreparedResult<T> {
    Inline(T),
    Artifact {
        reference: ResultArtifactRefMsg,
        summary: Option<T>,
    },
}

fn prepare_result<T, F>(
    state: &ServerState,
    result: T,
    delivery: Option<ResultDeliveryMsg>,
    summarize: F,
) -> std::result::Result<PreparedResult<T>, String>
where
    T: serde::Serialize,
    F: FnOnce(&T) -> T,
{
    let bytes = serde_json::to_vec(&result)
        .map_err(|error| format!("failed to serialize result JSON: {error}"))?;
    let Some(delivery) = delivery else {
        return Ok(PreparedResult::Inline(result));
    };
    let inline_limit = state.artifact_store.inline_limit_bytes();
    match delivery {
        ResultDeliveryMsg::Inline if bytes.len() > inline_limit => Err(format!(
            "result JSON is {} bytes, exceeding the configured inline limit of {} bytes; use result_delivery 'auto' or 'artifact'",
            bytes.len(),
            inline_limit
        )),
        ResultDeliveryMsg::Inline => Ok(PreparedResult::Inline(result)),
        ResultDeliveryMsg::Auto if bytes.len() <= inline_limit => {
            Ok(PreparedResult::Inline(result))
        }
        ResultDeliveryMsg::Auto | ResultDeliveryMsg::Artifact => {
            let reference = state
                .artifact_store
                .persist_json(&bytes)
                .map_err(|error| format!("failed to persist result artifact: {error}"))?;
            let summary = summarize(&result);
            let summary = serde_json::to_vec(&summary)
                .ok()
                .filter(|bytes| bytes.len() <= inline_limit)
                .map(|_| summary);
            Ok(PreparedResult::Artifact { reference, summary })
        }
    }
}

fn compact_result_for_console(result: &BacktestResultMsg) -> BacktestResultMsg {
    let mut summary = result.clone();
    summary.equity_curve.clear();
    summary.trade_log.truncate(30);
    summary.positions.truncate(15);
    if let Some(future) = summary.future.as_mut() {
        future.recorded_fills = serde_json::Value::Null;
        future.action_dispositions = serde_json::Value::Null;
        future.close_events = serde_json::Value::Null;
        future.completed_positions = serde_json::Value::Null;
        future.open_positions = serde_json::Value::Null;
        future.pending_orders = serde_json::Value::Null;
        future.pending_order_lifecycle.clear();
        future.mtm_equity_curve = serde_json::Value::Null;
    }
    summary
}

fn compact_profile_results(results: &[ProfileResult]) -> Vec<ProfileResult> {
    results
        .iter()
        .cloned()
        .map(|mut profile| {
            profile.result = profile.result.as_ref().map(compact_result_for_console);
            profile
        })
        .collect()
}

fn single_response_from_result(
    state: &ServerState,
    result: BacktestResultMsg,
    start: Instant,
    delivery: Option<ResultDeliveryMsg>,
) -> RunBacktestResponse {
    match prepare_result(state, result, delivery, compact_result_for_console) {
        Ok(PreparedResult::Inline(result)) => RunBacktestResponse {
            success: true,
            error: None,
            result: Some(result),
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        },
        Ok(PreparedResult::Artifact { reference, summary }) => RunBacktestResponse {
            success: true,
            error: None,
            result: summary,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: Some(reference),
            inline_complete: false,
        },
        Err(error) => RunBacktestResponse {
            success: false,
            error: Some(error),
            result: None,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: false,
        },
    }
}

fn multi_response_from_results(
    state: &ServerState,
    results: Vec<ProfileResult>,
    start: Instant,
    delivery: Option<ResultDeliveryMsg>,
) -> RunBacktestMultiResponse {
    let result_error = results
        .iter()
        .find(|result| !result.success)
        .and_then(|result| result.error.clone());
    let result_success = result_error.is_none();
    match prepare_result(state, results, delivery, |results| {
        compact_profile_results(results)
    }) {
        Ok(PreparedResult::Inline(results)) => RunBacktestMultiResponse {
            success: result_success,
            error: result_error,
            results,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        },
        Ok(PreparedResult::Artifact { reference, summary }) => RunBacktestMultiResponse {
            success: result_success,
            error: result_error,
            results: summary.unwrap_or_default(),
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: Some(reference),
            inline_complete: false,
        },
        Err(error) => RunBacktestMultiResponse {
            success: false,
            error: Some(error),
            results: Vec::new(),
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: false,
        },
    }
}

impl JobStatus {
    /// Convert to string for wire transport.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Queued => "Queued",
            JobStatus::LoadingData => "LoadingData",
            JobStatus::Running => "Running",
            JobStatus::Completed => "Completed",
            JobStatus::Failed => "Failed",
            JobStatus::Cancelled => "Cancelled",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

fn job_status_response(job_id: &str, job: &BacktestJob) -> BacktestStatusResponse {
    let elapsed_ms = job
        .completed_at
        .map(|completed| completed.duration_since(job.submitted_at).as_millis() as u64);
    BacktestStatusResponse {
        success: true,
        job_id: job_id.to_owned(),
        status: job.status.as_str().to_owned(),
        error: job.error.clone(),
        elapsed_ms,
        progress: job.progress.clone(),
    }
}

fn publish_job_status(job_id: &str, job: &BacktestJob) {
    job.updates.send_replace(job_status_response(job_id, job));
}

/// Subscribe to the current and future coalesced snapshots of a retained job.
pub fn subscribe_backtest_status(
    state: &ServerState,
    job_id: &str,
) -> std::result::Result<(watch::Receiver<BacktestStatusResponse>, Instant), String> {
    let jobs = state.jobs.lock().unwrap();
    let job = jobs
        .get(job_id)
        .ok_or_else(|| format!("Job '{job_id}' not found"))?;
    Ok((job.updates.subscribe(), job.submitted_at))
}

struct BacktestWatchState {
    job_id: String,
    updates: watch::Receiver<BacktestStatusResponse>,
    submitted_at: Instant,
    heartbeat: tokio::time::Interval,
    emit_initial: bool,
    finished: bool,
}

/// Build the server stream for a retained backtest job.
pub fn watch_backtest_stream(
    state: Arc<ServerState>,
    req: WatchBacktestRequest,
    heartbeat_interval: Duration,
) -> BoxStream<'static, std::result::Result<BacktestEvent, xrpc::RpcError>> {
    let (updates, submitted_at) = match subscribe_backtest_status(&state, &req.job_id) {
        Ok(subscription) => subscription,
        Err(error) => {
            return stream::once(async move { Err(xrpc::RpcError::ServerError(error)) }).boxed();
        }
    };

    let period = heartbeat_interval.max(Duration::from_millis(1));
    let start = tokio::time::Instant::now() + period;
    let mut heartbeat = tokio::time::interval_at(start, period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let initial = BacktestWatchState {
        job_id: req.job_id,
        updates,
        submitted_at,
        heartbeat,
        emit_initial: true,
        finished: false,
    };

    stream::unfold(initial, |mut state| async move {
        if state.finished {
            return None;
        }

        if state.emit_initial {
            state.emit_initial = false;
            let status = state.updates.borrow().clone();
            state.finished = status.is_terminal();
            return Some((Ok(BacktestEvent::Snapshot { status }), state));
        }

        tokio::select! {
            changed = state.updates.changed() => {
                match changed {
                    Ok(()) => {
                        let status = state.updates.borrow().clone();
                        state.finished = status.is_terminal();
                        Some((Ok(BacktestEvent::Snapshot { status }), state))
                    }
                    Err(_) => {
                        state.finished = true;
                        Some((Err(xrpc::RpcError::ServerError(format!(
                            "Backtest job '{}' update channel closed before a terminal snapshot",
                            state.job_id
                        ))), state))
                    }
                }
            }
            _ = state.heartbeat.tick() => {
                let event = BacktestEvent::Heartbeat {
                    job_id: state.job_id.clone(),
                    elapsed_ms: state.submitted_at.elapsed().as_millis() as u64,
                };
                Some((Ok(event), state))
            }
        }
    })
    .boxed()
}

fn progress_stage_rank(stage: &str) -> u8 {
    match stage {
        "queued" => 0,
        "loading_data" => 1,
        "replay" => 2,
        "completed" | "failed" | "cancelled" => 3,
        _ => 0,
    }
}

fn merge_progress(current: &mut BacktestProgress, next: BacktestProgress) {
    if progress_stage_rank(&next.stage) >= progress_stage_rank(&current.stage) {
        current.stage = next.stage;
    }
    current.processed_events = current.processed_events.max(next.processed_events);
    current.total_events = current.total_events.max(next.total_events);
    current.processed_signals = current.processed_signals.max(next.processed_signals);
    current.total_signals = current.total_signals.max(next.total_signals);
    current.processed_symbols = current.processed_symbols.max(next.processed_symbols);
    current.total_symbols = current.total_symbols.max(next.total_symbols);
}

fn remove_oldest_terminal_job(jobs: &mut HashMap<String, BacktestJob>) -> Option<BacktestJob> {
    let oldest = jobs
        .iter()
        .filter(|(_, job)| job.status.is_terminal() && !job.worker_active)
        .min_by(|(left_id, left), (right_id, right)| {
            left.completed_at
                .cmp(&right.completed_at)
                .then_with(|| left.submitted_at.cmp(&right.submitted_at))
                .then_with(|| left_id.cmp(right_id))
        })
        .map(|(id, _)| id.clone())?;
    jobs.remove(&oldest)
}

fn delete_job_artifacts(state: &ServerState, removed: &[BacktestJob]) {
    for job in removed {
        if !job.artifact_consumed
            && let Some(artifact) = job.artifact.as_ref()
        {
            let _ = state.artifact_store.delete(&artifact.artifact_id);
        }
    }
}

/// Remove terminal jobs older than `retention` and enforce the configured bound.
pub fn cleanup_expired_jobs(state: &ServerState, retention: Duration) -> usize {
    let removed = {
        let mut jobs = state.jobs.lock().unwrap();
        let expired = jobs
            .iter()
            .filter(|(_, job)| {
                job.status.is_terminal()
                    && !job.worker_active
                    && job
                        .completed_at
                        .is_some_and(|completed| completed.elapsed() >= retention)
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut removed = expired
            .into_iter()
            .filter_map(|id| jobs.remove(&id))
            .collect::<Vec<_>>();
        while jobs.len() > state.max_retained_jobs {
            let Some(job) = remove_oldest_terminal_job(&mut jobs) else {
                break;
            };
            removed.push(job);
        }
        removed
    };
    delete_job_artifacts(state, &removed);
    removed.len()
}

/// Cooperatively cancel every active job during server shutdown.
pub fn cancel_active_jobs(state: &ServerState) -> usize {
    let mut cancelled = 0;
    let mut jobs = state.jobs.lock().unwrap();
    for (job_id, job) in jobs.iter_mut().filter(|(_, job)| !job.status.is_terminal()) {
        job.cancellation.cancel();
        job.status = JobStatus::Cancelled;
        job.completed_at = Some(Instant::now());
        job.result = None;
        job.artifact = None;
        job.inline_complete = true;
        job.artifact_consumed = false;
        job.error = None;
        job.progress.stage = "cancelled".into();
        publish_job_status(job_id, job);
        cancelled += 1;
    }
    cancelled
}

fn update_job_progress(state: &ServerState, job_id: &str, progress: BacktestProgress) {
    let mut jobs = state.jobs.lock().unwrap();
    if let Some(job) = jobs.get_mut(job_id)
        && !job.status.is_terminal()
    {
        match progress.stage.as_str() {
            "loading_data" => job.status = JobStatus::LoadingData,
            "replay" => job.status = JobStatus::Running,
            _ => {}
        }
        merge_progress(&mut job.progress, progress);
        publish_job_status(job_id, job);
    }
}

impl std::str::FromStr for JobStatus {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Queued" => Ok(Self::Queued),
            "LoadingData" => Ok(Self::LoadingData),
            "Running" => Ok(Self::Running),
            "Completed" => Ok(Self::Completed),
            "Failed" => Ok(Self::Failed),
            "Cancelled" => Ok(Self::Cancelled),
            _ => Err("invalid job status"),
        }
    }
}

// ── Ping ────────────────────────────────────────────────────────────────────

/// Handle `ping` — returns server status and uptime.
pub fn handle_ping(state: &ServerState) -> PingResponse {
    PingResponse {
        status: "OK".into(),
        uptime_secs: state.start_time.elapsed().as_secs(),
        data_dir: state.data_dir.clone(),
    }
}

// ── List Profiles ───────────────────────────────────────────────────────────

/// Handle `list_profiles` — returns all loaded management profiles.
pub fn handle_list_profiles(state: &ServerState) -> ListProfilesResponse {
    let registry = state.profile_registry.read().unwrap();
    let profiles = registry
        .names()
        .into_iter()
        .filter_map(|name| {
            let p = registry.get(name)?;
            Some(ProfileInfo {
                name: p.name.clone(),
                use_targets: p.use_targets.clone(),
                close_ratios: p.close_ratios.clone(),
                stoploss_mode: format!("{:?}", p.stoploss_mode),
                rules_count: p.rules.len(),
                let_remainder_run: p.let_remainder_run,
            })
        })
        .collect();
    ListProfilesResponse { profiles }
}

// ── List Symbols ────────────────────────────────────────────────────────────

/// Handle `list_symbols` — returns available data from the Parquet store.
pub fn handle_list_symbols(
    state: &ServerState,
    req: &ListSymbolsRequest,
) -> std::result::Result<ListSymbolsResponse, String> {
    let store = ParquetStore::open(&state.data_dir).map_err(|e| e.to_string())?;

    let exchange_filter = req.exchange.as_deref();
    let symbol_filter: Option<&str> = None;

    let stat_rows = store
        .stats(exchange_filter, symbol_filter)
        .map_err(|e| e.to_string())?;

    let symbols: Vec<SymbolAvailability> = stat_rows
        .into_iter()
        .filter(|row| {
            // Apply data_type filter if requested.
            if let Some(ref dt) = req.data_type {
                let dt_lower = dt.to_lowercase();
                if dt_lower == "tick" && row.data_type != "tick" {
                    return false;
                }
                if dt_lower == "bar" && row.data_type == "tick" {
                    return false;
                }
            }
            true
        })
        .map(|row| {
            // The store reports bars as `bar (1m)`; tolerate the historical
            // no-space spelling too so existing data remains discoverable.
            let (data_type, timeframe) = parse_availability_data_type(&row.data_type);

            SymbolAvailability {
                exchange: row.exchange,
                symbol: row.symbol,
                data_type,
                timeframe,
                row_count: row.count,
                earliest: row.ts_min.format("%Y-%m-%dT%H:%M:%S").to_string(),
                latest: row.ts_max.format("%Y-%m-%dT%H:%M:%S").to_string(),
            }
        })
        .collect();

    Ok(ListSymbolsResponse { symbols })
}

fn parse_availability_data_type(raw: &str) -> (String, Option<String>) {
    let trimmed = raw.trim();
    let Some(rest) = trimmed.strip_prefix("bar") else {
        return (trimmed.to_string(), None);
    };
    let timeframe = rest
        .trim()
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    ("bar".into(), timeframe)
}

// ── Run Backtest ────────────────────────────────────────────────────────────

/// Handle the canonical FutureQuoteV1 execution endpoint.
pub fn handle_run_backtest(state: &ServerState, req: &RunBacktestRequest) -> RunBacktestResponse {
    let start = Instant::now();
    if let Err(error) = validate_future_quote_scalars(&req.future) {
        return RunBacktestResponse {
            success: false,
            error: Some(error.to_string()),
            result: None,
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        };
    }
    match execute_backtest_with_future(state, &req.request, &req.future, &req.evaluation) {
        Ok(result) => single_response_from_result(
            state,
            result_to_msg(&result),
            start,
            Some(req.result_delivery),
        ),
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

fn execute_backtest_with_future(
    state: &ServerState,
    req: &BacktestRunSpec,
    future: &FutureQuoteConfigMsg,
    evaluation: &ProviderEvaluationOptionsMsg,
) -> Result<BacktestResult> {
    execute_backtest_with_future_controlled(state, req, future, evaluation, None, &mut |_| {})
}

fn ensure_not_cancelled(cancellation: Option<&JobCancellationToken>) -> Result<()> {
    if cancellation.is_some_and(JobCancellationToken::is_cancelled) {
        Err(BacktestServerError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_streaming_replay_error(
    error: StreamingReplayError<MarketStreamError>,
) -> BacktestServerError {
    match error {
        StreamingReplayError::Cancelled(_) => BacktestServerError::Cancelled,
        StreamingReplayError::Feed(KWayMergeError::Source {
            error: EventBatchFeedError::Source(error),
            ..
        }) => error,
        StreamingReplayError::Feed(error) => BacktestServerError::MarketStream(error.to_string()),
    }
}

fn execute_backtest_with_future_controlled(
    state: &ServerState,
    req: &BacktestRunSpec,
    future: &FutureQuoteConfigMsg,
    evaluation: &ProviderEvaluationOptionsMsg,
    cancellation: Option<&JobCancellationToken>,
    progress: &mut dyn FnMut(BacktestProgress),
) -> Result<BacktestResult> {
    ensure_not_cancelled(cancellation)?;
    validate_request(req)?;
    validate_future_quote_scalars(future)?;

    // Validate and resolve profiles before expensive data loading.
    if let Some(ref profile_msg) = req.profile_def {
        let profile = profile_from_msg(profile_msg)?;
        profile.validate().map_err(|error| {
            BacktestServerError::InvalidRequest(format!("Invalid inline profile: {error}"))
        })?;
    }
    let profile = resolve_profile(state, req)?;

    let from = parse_optional_datetime(&req.from)?;
    let to = parse_optional_datetime(&req.to)?;
    let plan = build_replay_plan(
        state,
        &req.symbol,
        &req.symbols,
        req.all_symbols,
        &req.raw_signals,
        from,
        to,
        future.signal_latency_ms,
    )?;
    validate_replay_sizing(req, &plan)?;
    let evaluation_options = evaluation_options_from_msg_for_symbols(
        evaluation,
        &state.symbol_registry,
        plan.requested_symbols(),
    )?;
    let config = config_from_msg(&req.config, &state.symbol_registry, plan.active_symbols())?;
    let account_currency = account_currency_from_msg(future)?;
    let exchange = req.exchange.to_lowercase();
    tracing::info!(
        "run_backtest: requested_symbols={:?} active_symbols={:?} idle_symbols={:?} loading_start={:?} exchange={} data_type={}",
        plan.requested_symbols(),
        plan.active_symbols(),
        plan.idle_explicit_symbols(),
        plan.loading_start(),
        exchange,
        req.data_type
    );
    tracing::info!(
        "run_backtest: {} signals after date filtering",
        plan.retained_signals().len()
    );
    ensure_not_cancelled(cancellation)?;

    let total_symbols = plan.active_symbols().len() as u64;
    let total_signals = plan.retained_signals().len() as u64;
    progress(BacktestProgress {
        stage: "loading_data".into(),
        total_signals,
        total_symbols,
        ..BacktestProgress::default()
    });
    tracing::info!(
        "run_backtest: loading market data for {} active symbols...",
        plan.active_symbols().len()
    );
    let mut result = {
        let mut cancelled = || cancellation.is_some_and(JobCancellationToken::is_cancelled);
        let primary = describe_primary_market_stream(
            &state.data_dir,
            &exchange,
            plan.active_symbols(),
            &req.data_type,
            req.timeframe.as_deref(),
            plan.loading_start(),
            to,
            &mut cancelled,
            &mut |processed_symbols| {
                progress(BacktestProgress {
                    stage: "loading_data".into(),
                    processed_symbols,
                    total_symbols,
                    total_signals,
                    ..BacktestProgress::default()
                });
            },
        )?;
        progress(BacktestProgress {
            stage: "loading_conversion_data".into(),
            processed_symbols: total_symbols,
            total_symbols,
            total_signals,
            ..BacktestProgress::default()
        });
        let bundle = describe_future_stream(
            &state.data_dir,
            &exchange,
            &state.symbol_registry,
            &account_currency,
            plan.active_symbols(),
            &req.data_type,
            plan.loading_start(),
            primary,
            &mut cancelled,
        )?;
        let primary_eod = bundle.description.primary_eod();
        let future_config = future_config_from_msg(future, bundle.currency_plan)?;
        let cancellation_token = cancellation.cloned();
        let stream_cancellation: CancellationCheck = Arc::new(move || {
            cancellation_token
                .as_ref()
                .is_some_and(JobCancellationToken::is_cancelled)
        });
        let mut feed = bundle.description.open(stream_cancellation)?;
        ensure_not_cancelled(cancellation)?;
        progress(BacktestProgress {
            stage: "replay".into(),
            total_signals,
            processed_symbols: total_symbols,
            total_symbols,
            ..BacktestProgress::default()
        });
        tracing::info!("run_backtest: starting streaming FutureQuote engine...");
        let mut runner = BacktestRunner::new_future(config, future_config);
        runner = runner.with_evaluation_options(evaluation_options.clone());
        runner
            .run_raw_signals_future_streaming_controlled(
                &mut feed,
                primary_eod,
                plan.retained_signals().to_vec(),
                profile.as_ref(),
                || cancellation.is_some_and(JobCancellationToken::is_cancelled),
                |ReplayProgress {
                     processed_events,
                     total_events,
                     processed_signals,
                     total_signals,
                 }| {
                    progress(BacktestProgress {
                        stage: "replay".into(),
                        processed_events: processed_events as u64,
                        total_events: total_events as u64,
                        processed_signals: processed_signals as u64,
                        total_signals: total_signals as u64,
                        processed_symbols: total_symbols,
                        total_symbols,
                    });
                },
            )
            .map_err(map_streaming_replay_error)?
    };
    ensure_not_cancelled(cancellation)?;

    attach_future_reproducibility_metadata(
        &mut result,
        state,
        req,
        &plan,
        future,
        profile.as_ref(),
    );
    tracing::info!(
        "run_backtest: done, {} trades, {} positions",
        result.total_trades,
        result.total_positions
    );
    Ok(result)
}

fn attach_future_reproducibility_metadata(
    result: &mut BacktestResult,
    state: &ServerState,
    req: &BacktestRunSpec,
    plan: &ReplayPlan,
    future: &FutureQuoteConfigMsg,
    profile: Option<&ManagementProfile>,
) {
    let Some(metadata) = result.execution_metadata.as_mut() else {
        return;
    };
    let tags = &mut metadata.tags;
    tags.insert("data.exchange".into(), req.exchange.to_lowercase());
    tags.insert("data.type".into(), req.data_type.to_lowercase());
    tags.insert(
        "data.timeframe".into(),
        req.timeframe.clone().unwrap_or_else(|| "none".into()),
    );
    tags.insert(
        "data.requested_from".into(),
        req.from.clone().unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert(
        "data.requested_to".into(),
        req.to.clone().unwrap_or_else(|| "unbounded".into()),
    );
    tags.insert("data.symbols".into(), plan.active_symbols().join(","));
    tags.insert(
        "data.requested_symbols".into(),
        plan.requested_symbols().join(","),
    );
    tags.insert(
        "data.active_symbols".into(),
        plan.active_symbols().join(","),
    );
    tags.insert(
        "data.idle_symbols".into(),
        plan.idle_explicit_symbols().join(","),
    );
    tags.insert("data.idle_run".into(), plan.is_idle().to_string());
    tags.insert(
        "data.loading_from".into(),
        plan.loading_start()
            .map(|timestamp| timestamp.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "none".into()),
    );
    tags.insert(
        "execution.signal_latency_ms".into(),
        future.signal_latency_ms.max(0).to_string(),
    );
    if req.data_type.eq_ignore_ascii_case("bar") {
        tags.insert(
            "data.bar_quote_convention".into(),
            "close_only_zero_spread".into(),
        );
        tags.insert("data.intrabar_simulation".into(), "false".into());
    }

    match profile {
        Some(profile) => {
            tags.insert("profile.identity".into(), profile.name.clone());
            tags.insert(
                "profile.options".into(),
                serde_json::to_string(profile).unwrap_or_else(|_| "unavailable".into()),
            );
        }
        None => {
            tags.insert("profile.identity".into(), "none".into());
            tags.insert("profile.options".into(), "null".into());
        }
    }
    match req.config.sizing.as_ref() {
        Some(sizing) => {
            let identity = match sizing {
                SizingPolicyMsg::FixedLot { .. } => "fixed_lot",
                SizingPolicyMsg::FixedRiskAmount { .. } => "fixed_risk_amount",
                SizingPolicyMsg::BalanceRiskPercent { .. } => "balance_risk_percent",
            };
            tags.insert("sizing.identity".into(), identity.into());
            tags.insert(
                "sizing.options".into(),
                serde_json::to_string(sizing).unwrap_or_else(|_| "unavailable".into()),
            );
        }
        None => {
            tags.insert("sizing.identity".into(), "signal_size".into());
            tags.insert("sizing.options".into(), "null".into());
        }
    }

    for symbol in plan.active_symbols() {
        let Some(spec) = state.symbol_registry.spec(symbol) else {
            continue;
        };
        let prefix = format!("symbol.{symbol}");
        tags.insert(format!("{prefix}.canonical"), spec.canonical.clone());
        tags.insert(
            format!("{prefix}.pip_position"),
            spec.pip_position.to_string(),
        );
        tags.insert(format!("{prefix}.digits"), spec.digits.to_string());
        tags.insert(format!("{prefix}.category"), spec.category.clone());
        tags.insert(
            format!("{prefix}.lot_base_units"),
            spec.lot_base_units.to_string(),
        );
        tags.insert(
            format!("{prefix}.lot_step_units"),
            spec.lot_step_units.to_string(),
        );
        tags.insert(
            format!("{prefix}.lot_min_steps"),
            spec.lot_min_steps.to_string(),
        );
        tags.insert(
            format!("{prefix}.lot_max_steps"),
            spec.lot_max_steps.to_string(),
        );
    }
}

// ── Run Backtest Multi ──────────────────────────────────────────────────────

pub fn handle_run_backtest_multi(
    state: &ServerState,
    req: &RunBacktestMultiRequest,
) -> RunBacktestMultiResponse {
    let start = Instant::now();
    if req.request.profiles.is_empty() {
        return RunBacktestMultiResponse {
            success: false,
            error: Some("At least one profile is required.".into()),
            results: Vec::new(),
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        };
    }
    if let Err(error) = validate_future_quote_scalars(&req.future) {
        let error = error.to_string();
        return RunBacktestMultiResponse {
            success: false,
            error: Some(error.clone()),
            results: profile_error_results(&req.request, error),
            elapsed_ms: start.elapsed().as_millis() as u64,
            artifact: None,
            inline_complete: true,
        };
    }

    let results =
        execute_backtest_multi_with_future(state, &req.request, &req.future, &req.evaluation);
    multi_response_from_results(state, results, start, Some(req.result_delivery))
}

fn execute_backtest_multi_with_future(
    state: &ServerState,
    req: &BacktestMultiRunSpec,
    future: &FutureQuoteConfigMsg,
    evaluation: &ProviderEvaluationOptionsMsg,
) -> Vec<ProfileResult> {
    if let Err(error) = validate_future_quote_scalars(future) {
        return profile_error_results(req, error.to_string());
    }

    // Early validation common to all profiles.
    let data_type = req.data_type.to_lowercase();
    if data_type != "tick" && data_type != "bar" {
        return req
            .profiles
            .iter()
            .map(|pr| ProfileResult {
                profile: profile_ref_name(pr),
                success: false,
                error: Some(format!(
                    "Invalid data_type: '{}'. Must be 'tick' or 'bar'.",
                    req.data_type
                )),
                result: None,
            })
            .collect();
    }

    if data_type == "bar" && req.timeframe.is_none() {
        return req
            .profiles
            .iter()
            .map(|pr| ProfileResult {
                profile: profile_ref_name(pr),
                success: false,
                error: Some("timeframe is required when data_type is 'bar'.".into()),
                result: None,
            })
            .collect();
    }

    if req.raw_signals.is_empty() {
        return req
            .profiles
            .iter()
            .map(|pr| ProfileResult {
                profile: profile_ref_name(pr),
                success: false,
                error: Some("At least one raw_signal is required.".into()),
                result: None,
            })
            .collect();
    }

    let from = match parse_optional_datetime(&req.from) {
        Ok(value) => value,
        Err(error) => return profile_error_results(req, error.to_string()),
    };
    let to = match parse_optional_datetime(&req.to) {
        Ok(value) => value,
        Err(error) => return profile_error_results(req, error.to_string()),
    };
    let plan = match build_replay_plan(
        state,
        &req.symbol,
        &req.symbols,
        req.all_symbols,
        &req.raw_signals,
        from,
        to,
        future.signal_latency_ms,
    ) {
        Ok(plan) => plan,
        Err(error) => return profile_error_results(req, error.to_string()),
    };
    if let Err(error) = validate_replay_sizing_for_config(&req.config, &plan) {
        return profile_error_results(req, error.to_string());
    }
    let evaluation_options = match evaluation_options_from_msg_for_symbols(
        evaluation,
        &state.symbol_registry,
        plan.requested_symbols(),
    ) {
        Ok(options) => options,
        Err(error) => {
            return req
                .profiles
                .iter()
                .map(|profile| ProfileResult {
                    profile: profile_ref_name(profile),
                    success: false,
                    error: Some(error.to_string()),
                    result: None,
                })
                .collect();
        }
    };
    let config = match config_from_msg(&req.config, &state.symbol_registry, plan.active_symbols()) {
        Ok(config) => config,
        Err(error) => {
            return profile_error_results(req, error.to_string());
        }
    };
    let account_currency = match account_currency_from_msg(future) {
        Ok(account_currency) => account_currency,
        Err(error) => {
            return profile_error_results(req, error.to_string());
        }
    };
    let exchange = req.exchange.to_lowercase();

    {
        let mut never_cancelled = || false;
        let primary = match describe_primary_market_stream(
            &state.data_dir,
            &exchange,
            plan.active_symbols(),
            &data_type,
            req.timeframe.as_deref(),
            plan.loading_start(),
            to,
            &mut never_cancelled,
            &mut |_| {},
        ) {
            Ok(primary) => primary,
            Err(error) => return profile_error_results(req, error.to_string()),
        };
        let bundle = match describe_future_stream(
            &state.data_dir,
            &exchange,
            &state.symbol_registry,
            &account_currency,
            plan.active_symbols(),
            &data_type,
            plan.loading_start(),
            primary,
            &mut never_cancelled,
        ) {
            Ok(bundle) => bundle,
            Err(error) => return profile_error_results(req, error.to_string()),
        };
        let future_config = match future_config_from_msg(future, bundle.currency_plan) {
            Ok(config) => config,
            Err(error) => return profile_error_results(req, error.to_string()),
        };
        let primary_eod = bundle.description.primary_eod();
        let metadata_request = single_request_from_multi(req);

        req.profiles
            .iter()
            .map(|profile_ref| {
                let name = profile_ref_name(profile_ref);
                let profile = match profile_ref {
                    ProfileRef::Named(profile_name) => state
                        .profile_registry
                        .read()
                        .unwrap()
                        .get(profile_name)
                        .cloned()
                        .ok_or_else(|| BacktestServerError::ProfileNotFound(profile_name.clone())),
                    ProfileRef::Inline(message) => profile_from_msg(message).and_then(|profile| {
                        profile.validate().map_err(|error| {
                            BacktestServerError::InvalidRequest(format!(
                                "Invalid inline profile: {error}"
                            ))
                        })?;
                        Ok(profile)
                    }),
                };
                let run_result = profile.and_then(|profile| {
                    run_profile_streaming(
                        &profile,
                        plan.retained_signals(),
                        &bundle.description,
                        primary_eod,
                        &config,
                        &future_config,
                        future,
                        Some(&evaluation_options),
                        state,
                        &metadata_request,
                        &plan,
                    )
                });
                match run_result {
                    Ok(result) => ProfileResult {
                        profile: name,
                        success: true,
                        error: None,
                        result: Some(result_to_msg(&result)),
                    },
                    Err(error) => ProfileResult {
                        profile: name,
                        success: false,
                        error: Some(error.to_string()),
                        result: None,
                    },
                }
            })
            .collect()
    }
}

fn single_request_from_multi(req: &BacktestMultiRunSpec) -> BacktestRunSpec {
    BacktestRunSpec {
        symbol: req.symbol.clone(),
        symbols: req.symbols.clone(),
        all_symbols: req.all_symbols,
        exchange: req.exchange.clone(),
        data_type: req.data_type.clone(),
        timeframe: req.timeframe.clone(),
        from: req.from.clone(),
        to: req.to.clone(),
        raw_signals: req.raw_signals.clone(),
        profile: None,
        profile_def: None,
        config: req.config.clone(),
    }
}

/// Extract the profile name from a `ProfileRef`.
fn profile_ref_name(pr: &ProfileRef) -> String {
    match pr {
        ProfileRef::Named(name) => name.clone(),
        ProfileRef::Inline(msg) => msg.name.clone(),
    }
}

fn profile_error_results(req: &BacktestMultiRunSpec, error: String) -> Vec<ProfileResult> {
    req.profiles
        .iter()
        .map(|profile| ProfileResult {
            profile: profile_ref_name(profile),
            success: false,
            error: Some(error.clone()),
            result: None,
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn run_profile_streaming(
    profile: &ManagementProfile,
    raw_signals: &[RawSignal],
    description: &MarketStreamDescription,
    primary_eod: Option<NaiveDateTime>,
    config: &BacktestConfig,
    future_config: &FutureQuoteConfig,
    future: &FutureQuoteConfigMsg,
    evaluation: Option<&EvaluationOptions>,
    state: &ServerState,
    metadata_request: &BacktestRunSpec,
    plan: &ReplayPlan,
) -> Result<BacktestResult> {
    let cancellation: CancellationCheck = Arc::new(|| false);
    let mut feed = description.open(cancellation)?;
    let mut runner = BacktestRunner::new_future(config.clone(), future_config.clone());
    if let Some(options) = evaluation {
        runner = runner.with_evaluation_options(options.clone());
    }
    let mut result = runner
        .run_raw_signals_future_streaming_controlled(
            &mut feed,
            primary_eod,
            raw_signals.to_vec(),
            Some(profile),
            || false,
            |_| {},
        )
        .map_err(map_streaming_replay_error)?;
    attach_future_reproducibility_metadata(
        &mut result,
        state,
        metadata_request,
        plan,
        future,
        Some(profile),
    );
    Ok(result)
}

// ── Signal Conversion ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn build_replay_plan(
    state: &ServerState,
    symbol: &str,
    symbols: &[String],
    all_symbols: bool,
    raw_signal_msgs: &[RawSignalMsg],
    requested_from: Option<NaiveDateTime>,
    requested_to: Option<NaiveDateTime>,
    signal_latency_ms: i64,
) -> Result<ReplayPlan> {
    let scope =
        resolve_requested_symbol_scope(&state.symbol_registry, symbol, symbols, all_symbols)?;
    let raw_signals = raw_signal_msgs
        .iter()
        .map(|signal| raw_signal_from_msg(signal, scope.default_symbol(), &state.symbol_registry))
        .collect::<Result<Vec<_>>>()?;
    ReplayPlan::build(
        scope,
        raw_signals,
        requested_from,
        requested_to,
        signal_latency_ms,
    )
}

fn resolve_requested_symbol_scope(
    registry: &SymbolRegistry,
    symbol: &str,
    symbols: &[String],
    all_symbols: bool,
) -> Result<RequestedSymbolScope> {
    if all_symbols {
        return Ok(RequestedSymbolScope::Inferred);
    }

    let mut resolved = BTreeSet::new();
    if !symbols.is_empty() {
        for raw in symbols {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                resolved.insert(normalize_symbol(registry, trimmed));
            }
        }
        if resolved.is_empty() {
            return Err(BacktestServerError::InvalidRequest(
                "symbols was provided but did not contain any non-empty symbol".into(),
            ));
        }
    } else {
        let trimmed = symbol.trim();
        if trimmed.is_empty() {
            return Err(BacktestServerError::InvalidRequest(
                "symbol is required unless symbols or all_symbols is provided".into(),
            ));
        }
        resolved.insert(normalize_symbol(registry, trimmed));
    }

    Ok(RequestedSymbolScope::explicit(resolved))
}

/// Resolve the management profile from a request (inline or named).
fn resolve_profile(
    state: &ServerState,
    req: &BacktestRunSpec,
) -> Result<Option<ManagementProfile>> {
    if let Some(ref profile_msg) = req.profile_def {
        let profile = profile_from_msg(profile_msg)?;
        profile.validate().map_err(|e| {
            BacktestServerError::InvalidRequest(format!("Invalid inline profile: {e}"))
        })?;
        Ok(Some(profile))
    } else if let Some(ref profile_name) = req.profile {
        let registry = state.profile_registry.read().unwrap();
        let profile = registry
            .get(profile_name)
            .ok_or_else(|| BacktestServerError::ProfileNotFound(profile_name.clone()))?;
        Ok(Some(profile.clone()))
    } else {
        Ok(None)
    }
}

// ── Data Loading ────────────────────────────────────────────────────────────

/// Load raw market events from Parquet (shared between single and multi runs).
#[cfg(test)]
fn load_market_events(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<Vec<MarketEvent>> {
    load_market_events_controlled(
        data_dir, exchange, symbol, data_type, timeframe, from, to, None,
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn load_market_events_controlled(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    cancellation: Option<&JobCancellationToken>,
) -> Result<Vec<MarketEvent>> {
    ensure_not_cancelled(cancellation)?;
    let store = ParquetStore::open(data_dir)?;
    let dt = data_type.to_lowercase();

    if dt == "tick" {
        let disk_exchange =
            resolve_partition_value(data_dir, "ticks", "exchange", exchange, "", cancellation)?;
        let disk_symbol = resolve_partition_value(
            data_dir,
            "ticks",
            "symbol",
            symbol,
            &format!("exchange={disk_exchange}"),
            cancellation,
        )?;
        let opts = QueryOpts {
            exchange: disk_exchange.clone(),
            symbol: disk_symbol.clone(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (ticks, _total) = store
            .query_ticks_cancellable(&opts, || {
                cancellation.is_some_and(JobCancellationToken::is_cancelled)
            })
            .map_err(map_data_cancellation)?;
        if ticks.is_empty() {
            return Err(BacktestServerError::NoDataFound {
                symbol: disk_symbol,
                exchange: disk_exchange,
                data_type: "tick".into(),
            });
        }
        let feed = ticks_to_feed(ticks);
        Ok(canonicalize_market_event_symbols(
            feed_to_events(feed),
            symbol,
        ))
    } else if dt == "bar" {
        let tf_str = timeframe.ok_or_else(|| {
            BacktestServerError::InvalidRequest("timeframe is required for bar data".into())
        })?;
        let tf = Timeframe::parse(tf_str).map_err(|_| {
            BacktestServerError::InvalidRequest(format!("Invalid timeframe: '{tf_str}'"))
        })?;
        let disk_exchange =
            resolve_partition_value(data_dir, "bars", "exchange", exchange, "", cancellation)?;
        let disk_symbol = resolve_partition_value(
            data_dir,
            "bars",
            "symbol",
            symbol,
            &format!("exchange={disk_exchange}"),
            cancellation,
        )?;
        let disk_timeframe = resolve_partition_value(
            data_dir,
            "bars",
            "timeframe",
            tf.as_str(),
            &format!("exchange={disk_exchange}/symbol={disk_symbol}"),
            cancellation,
        )?;
        let opts = BarQueryOpts {
            exchange: disk_exchange.clone(),
            symbol: disk_symbol.clone(),
            timeframe: disk_timeframe.clone(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (bars, _total) = store
            .query_bars_cancellable(&opts, || {
                cancellation.is_some_and(JobCancellationToken::is_cancelled)
            })
            .map_err(map_data_cancellation)?;
        if bars.is_empty() {
            return Err(BacktestServerError::NoDataFound {
                symbol: disk_symbol,
                exchange: disk_exchange,
                data_type: format!("bar({})", disk_timeframe),
            });
        }
        let feed = bars_to_feed(bars);
        Ok(canonicalize_market_event_symbols(
            feed_to_events(feed),
            symbol,
        ))
    } else {
        Err(BacktestServerError::InvalidRequest(format!(
            "Invalid data_type: '{}'. Must be 'tick' or 'bar'.",
            data_type
        )))
    }
}

#[cfg(test)]
fn map_data_cancellation(error: DataError) -> BacktestServerError {
    match error {
        DataError::Cancelled => BacktestServerError::Cancelled,
        other => BacktestServerError::Database(other),
    }
}

/// Resolve a Hive partition value by scanning child directories case-insensitively.
#[cfg(test)]
fn resolve_partition_value(
    data_dir: &str,
    data_subdir: &str,
    key: &str,
    requested: &str,
    parent: &str,
    cancellation: Option<&JobCancellationToken>,
) -> Result<String> {
    let dir = if parent.is_empty() {
        Path::new(data_dir).join(data_subdir)
    } else {
        Path::new(data_dir).join(data_subdir).join(parent)
    };
    let prefix = format!("{key}=");
    let mut case_insensitive_match = None;

    ensure_not_cancelled(cancellation)?;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            ensure_not_cancelled(cancellation)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(value) = name.strip_prefix(&prefix) else {
                continue;
            };
            if value == requested {
                return Ok(value.to_string());
            }
            if case_insensitive_match.is_none() && value.eq_ignore_ascii_case(requested) {
                case_insensitive_match = Some(value.to_string());
            }
        }
    }

    ensure_not_cancelled(cancellation)?;
    Ok(case_insensitive_match.unwrap_or_else(|| requested.to_string()))
}

/// Rewrite loaded market events to the canonical symbol used by signals.
#[cfg(test)]
fn canonicalize_market_event_symbols(
    mut events: Vec<MarketEvent>,
    canonical_symbol: &str,
) -> Vec<MarketEvent> {
    for event in &mut events {
        match event {
            MarketEvent::Tick { symbol, .. } | MarketEvent::Bar { symbol, .. } => {
                *symbol = canonical_symbol.to_string();
            }
        }
    }
    events
}

/// Drain a VecFeed into its underlying Vec<MarketEvent>.
#[cfg(test)]
fn feed_to_events(mut feed: VecFeed) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(feed.total());
    while let Some(event) = feed.next_event() {
        events.push(event);
    }
    events
}

// ── Validation ──────────────────────────────────────────────────────────────

/// Validate the common fields of a run_backtest request.
fn validate_request(req: &BacktestRunSpec) -> Result<()> {
    let dt = req.data_type.to_lowercase();
    if dt != "tick" && dt != "bar" {
        return Err(BacktestServerError::InvalidRequest(format!(
            "Invalid data_type: '{}'. Must be 'tick' or 'bar'.",
            req.data_type
        )));
    }
    if dt == "bar" && req.timeframe.is_none() {
        return Err(BacktestServerError::InvalidRequest(
            "timeframe is required when data_type is 'bar'.".into(),
        ));
    }
    if req.raw_signals.is_empty() {
        return Err(BacktestServerError::InvalidRequest(
            "At least one raw_signal is required.".into(),
        ));
    }
    Ok(())
}

fn validate_replay_sizing(req: &BacktestRunSpec, plan: &ReplayPlan) -> Result<()> {
    validate_replay_sizing_for_config(&req.config, plan)
}

fn validate_replay_sizing_for_config(config: &BacktestConfigMsg, plan: &ReplayPlan) -> Result<()> {
    if !plan.is_idle() && config.sizing.is_none() {
        return Err(BacktestServerError::InvalidRequest(
            "Entry signals require an account sizing policy.".into(),
        ));
    }
    Ok(())
}

// ── Parsing Helpers ─────────────────────────────────────────────────────────

/// Normalize a symbol name via the registry, falling back to passthrough.
fn normalize_symbol(registry: &SymbolRegistry, raw: &str) -> String {
    registry.normalize_or_passthrough(raw)
}

/// Parse an ISO datetime string into NaiveDateTime.
fn parse_datetime(s: &str) -> Result<NaiveDateTime> {
    // Try multiple common formats.
    let formats = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for fmt in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt);
        }
    }
    // Try date-only (appends midnight).
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(date.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(BacktestServerError::InvalidRequest(format!(
        "Cannot parse datetime: '{s}'. Use ISO format (e.g. '2026-01-15T10:30:00' or '2026-01-15')."
    )))
}

/// Parse an optional datetime string.
fn parse_optional_datetime(s: &Option<String>) -> Result<Option<NaiveDateTime>> {
    match s {
        Some(v) => parse_datetime(v).map(Some),
        None => Ok(None),
    }
}

// ── Phase 2: Profile Management Handlers ────────────────────────────────────

/// Handle `add_profile` — add or overwrite a management profile at runtime.
pub fn handle_add_profile(state: &ServerState, req: &AddProfileRequest) -> AddProfileResponse {
    let profile = match profile_from_msg(&req.profile) {
        Ok(p) => p,
        Err(e) => {
            let registry = state.profile_registry.read().unwrap();
            return AddProfileResponse {
                success: false,
                error: Some(e.to_string()),
                profile_count: registry.len(),
            };
        }
    };

    let mut registry = state.profile_registry.write().unwrap();
    match registry.insert(profile, req.overwrite) {
        Ok(()) => AddProfileResponse {
            success: true,
            error: None,
            profile_count: registry.len(),
        },
        Err(e) => AddProfileResponse {
            success: false,
            error: Some(e.to_string()),
            profile_count: registry.len(),
        },
    }
}

/// Handle `remove_profile` — remove a management profile by name.
pub fn handle_remove_profile(
    state: &ServerState,
    req: &RemoveProfileRequest,
) -> RemoveProfileResponse {
    let mut registry = state.profile_registry.write().unwrap();
    let removed = registry.remove(&req.name);
    RemoveProfileResponse {
        success: removed,
        error: if removed {
            None
        } else {
            Some(format!("Profile '{}' not found", req.name))
        },
        profile_count: registry.len(),
    }
}

/// Handle `reload_profiles` — reload profiles from the configured TOML file.
pub fn handle_reload_profiles(state: &ServerState) -> ReloadProfilesResponse {
    if state.profiles_path.is_empty() {
        return ReloadProfilesResponse {
            success: false,
            error: Some("No profiles_path configured".into()),
            profile_count: state.profile_registry.read().unwrap().len(),
            loaded_from: String::new(),
        };
    }

    match ProfileRegistry::load(&state.profiles_path) {
        Ok(new_registry) => {
            let count = new_registry.len();
            let mut registry = state.profile_registry.write().unwrap();
            *registry = new_registry;
            ReloadProfilesResponse {
                success: true,
                error: None,
                profile_count: count,
                loaded_from: state.profiles_path.clone(),
            }
        }
        Err(e) => ReloadProfilesResponse {
            success: false,
            error: Some(e.to_string()),
            profile_count: state.profile_registry.read().unwrap().len(),
            loaded_from: state.profiles_path.clone(),
        },
    }
}

// ── Async Job API Handlers (Issue 2) ─────────────────────────────────────────

/// Admit a validated request to the bounded async job store.
fn admit_backtest_job(state: &ServerState) -> SubmitBacktestResponse {
    let job_id = format!("job-{}", uuid_v4_simple());
    let initial_progress = BacktestProgress {
        stage: "queued".into(),
        ..BacktestProgress::default()
    };
    let (updates, _) = watch::channel(BacktestStatusResponse {
        success: true,
        job_id: job_id.clone(),
        status: JobStatus::Queued.as_str().into(),
        error: None,
        elapsed_ms: None,
        progress: initial_progress.clone(),
    });
    let job = BacktestJob {
        status: JobStatus::Queued,
        submitted_at: Instant::now(),
        completed_at: None,
        progress: initial_progress,
        result: None,
        artifact: None,
        inline_complete: false,
        artifact_consumed: false,
        error: None,
        cancellation: JobCancellationToken::default(),
        worker_active: true,
        updates,
    };
    if state.max_retained_jobs == 0 {
        return SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some("Async job retention limit is zero".into()),
        };
    }
    let mut evicted = Vec::new();
    {
        let mut jobs = state.jobs.lock().unwrap();
        while jobs.len() >= state.max_retained_jobs {
            let Some(removed) = remove_oldest_terminal_job(&mut jobs) else {
                break;
            };
            evicted.push(removed);
        }
        if jobs.len() >= state.max_retained_jobs {
            drop(jobs);
            delete_job_artifacts(state, &evicted);
            return SubmitBacktestResponse {
                success: false,
                job_id: None,
                error: Some(format!(
                    "Async job limit reached (max {})",
                    state.max_retained_jobs
                )),
            };
        }
        jobs.insert(job_id.clone(), job);
    }
    delete_job_artifacts(state, &evicted);
    SubmitBacktestResponse {
        success: true,
        job_id: Some(job_id),
        error: None,
    }
}

pub fn handle_submit_backtest(
    state: &ServerState,
    req: &SubmitBacktestRequest,
) -> SubmitBacktestResponse {
    let validation = (|| -> Result<()> {
        let request = &req.request.request;
        validate_future_quote_scalars(&req.request.future)?;
        validate_request(request)?;
        let from = parse_optional_datetime(&request.from)?;
        let to = parse_optional_datetime(&request.to)?;
        let plan = build_replay_plan(
            state,
            &request.symbol,
            &request.symbols,
            request.all_symbols,
            &request.raw_signals,
            from,
            to,
            req.request.future.signal_latency_ms,
        )?;
        validate_replay_sizing(request, &plan)?;
        account_currency_from_msg(&req.request.future)?;
        config_from_msg(
            &request.config,
            &state.symbol_registry,
            plan.active_symbols(),
        )?;
        evaluation_options_from_msg_for_symbols(
            &req.request.evaluation,
            &state.symbol_registry,
            plan.requested_symbols(),
        )?;
        Ok(())
    })();
    if let Err(error) = validation {
        return SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some(error.to_string()),
        };
    }
    admit_backtest_job(state)
}

/// Handle `get_backtest_status` — poll the status of a submitted job.
pub fn handle_get_backtest_status(
    state: &ServerState,
    req: &GetBacktestStatusRequest,
) -> BacktestStatusResponse {
    let jobs = state.jobs.lock().unwrap();
    match jobs.get(&req.job_id) {
        Some(job) => job_status_response(&req.job_id, job),
        None => BacktestStatusResponse {
            success: false,
            job_id: req.job_id.clone(),
            status: "NotFound".into(),
            error: Some(format!("Job '{}' not found", req.job_id)),
            elapsed_ms: None,
            progress: BacktestProgress::default(),
        },
    }
}

/// Handle `get_backtest_result` — fetch the result of a completed job.
pub fn handle_get_backtest_result(
    state: &ServerState,
    req: &GetBacktestResultRequest,
) -> GetBacktestResultResponse {
    let jobs = state.jobs.lock().unwrap();
    match jobs.get(&req.job_id) {
        Some(job) if job.status == JobStatus::Completed && job.artifact_consumed => {
            GetBacktestResultResponse {
                success: false,
                job_id: req.job_id.clone(),
                result: job.result.clone(),
                error: Some("Job result artifact has already been consumed".into()),
                artifact: None,
                inline_complete: false,
                artifact_consumed: true,
            }
        }
        Some(job) if job.status == JobStatus::Completed => GetBacktestResultResponse {
            success: true,
            job_id: req.job_id.clone(),
            result: job.result.clone(),
            error: None,
            artifact: job.artifact.clone(),
            inline_complete: job.inline_complete,
            artifact_consumed: false,
        },
        Some(job) => GetBacktestResultResponse {
            success: false,
            job_id: req.job_id.clone(),
            result: None,
            error: Some(format!(
                "Job is not completed (status: {})",
                job.status.as_str()
            )),
            artifact: None,
            inline_complete: true,
            artifact_consumed: false,
        },
        None => GetBacktestResultResponse {
            success: false,
            job_id: req.job_id.clone(),
            result: None,
            error: Some(format!("Job '{}' not found", req.job_id)),
            artifact: None,
            inline_complete: true,
            artifact_consumed: false,
        },
    }
}

/// Return one base64-encoded raw result artifact chunk.
pub fn handle_get_result_artifact_chunk(
    state: &ServerState,
    req: &GetResultArtifactChunkRequest,
) -> GetResultArtifactChunkResponse {
    match state
        .artifact_store
        .read_chunk(&req.artifact_id, req.offset)
    {
        Ok(chunk) => GetResultArtifactChunkResponse {
            success: true,
            artifact_id: req.artifact_id.clone(),
            offset: chunk.offset,
            data_base64: BASE64_STANDARD.encode(chunk.bytes),
            eof: chunk.eof,
            error: None,
        },
        Err(error) => GetResultArtifactChunkResponse {
            success: false,
            artifact_id: req.artifact_id.clone(),
            offset: req.offset,
            data_base64: String::new(),
            eof: false,
            error: Some(error.to_string()),
        },
    }
}

/// Delete a complete result artifact.
pub fn handle_delete_result_artifact(
    state: &ServerState,
    req: &DeleteResultArtifactRequest,
) -> DeleteResultArtifactResponse {
    let mut jobs = state.jobs.lock().unwrap();
    match state.artifact_store.delete(&req.artifact_id) {
        Ok(deleted) => {
            for job in jobs.values_mut().filter(|job| {
                job.artifact
                    .as_ref()
                    .is_some_and(|artifact| artifact.artifact_id == req.artifact_id)
            }) {
                job.artifact = None;
                job.artifact_consumed = true;
                job.inline_complete = false;
            }
            if deleted {
                DeleteResultArtifactResponse {
                    success: true,
                    artifact_id: req.artifact_id.clone(),
                    error: None,
                }
            } else {
                DeleteResultArtifactResponse {
                    success: false,
                    artifact_id: req.artifact_id.clone(),
                    error: Some(format!("Artifact '{}' not found", req.artifact_id)),
                }
            }
        }
        Err(error) => DeleteResultArtifactResponse {
            success: false,
            artifact_id: req.artifact_id.clone(),
            error: Some(error.to_string()),
        },
    }
}

/// Handle `cancel_backtest` — cancel a submitted job.
pub fn handle_cancel_backtest(
    state: &ServerState,
    req: &CancelBacktestRequest,
) -> CancelBacktestResponse {
    let mut jobs = state.jobs.lock().unwrap();
    match jobs.get_mut(&req.job_id) {
        Some(job)
            if job.status == JobStatus::Queued
                || job.status == JobStatus::LoadingData
                || job.status == JobStatus::Running =>
        {
            job.cancellation.cancel();
            job.status = JobStatus::Cancelled;
            job.completed_at = Some(Instant::now());
            job.result = None;
            job.artifact = None;
            job.inline_complete = true;
            job.artifact_consumed = false;
            job.error = None;
            job.progress.stage = "cancelled".into();
            publish_job_status(&req.job_id, job);
            CancelBacktestResponse {
                success: true,
                job_id: req.job_id.clone(),
                error: None,
            }
        }
        Some(job) => CancelBacktestResponse {
            success: false,
            job_id: req.job_id.clone(),
            error: Some(format!(
                "Cannot cancel job in status: {}",
                job.status.as_str()
            )),
        },
        None => CancelBacktestResponse {
            success: false,
            job_id: req.job_id.clone(),
            error: Some(format!("Job '{}' not found", req.job_id)),
        },
    }
}

pub fn run_job_and_store(state: Arc<ServerState>, job_id: String, req: RunBacktestRequest) {
    run_job_and_store_inner(
        state,
        job_id,
        req.request,
        req.future,
        req.evaluation,
        req.result_delivery,
    );
}

fn run_job_and_store_inner(
    state: Arc<ServerState>,
    job_id: String,
    req: BacktestRunSpec,
    future: FutureQuoteConfigMsg,
    evaluation: ProviderEvaluationOptionsMsg,
    delivery: ResultDeliveryMsg,
) {
    let cancellation = {
        let mut jobs = state.jobs.lock().unwrap();
        let Some(job) = jobs.get_mut(&job_id) else {
            return;
        };
        if job.status == JobStatus::Cancelled || job.cancellation.is_cancelled() {
            job.worker_active = false;
            return;
        }
        job.status = JobStatus::LoadingData;
        job.progress.stage = "loading_data".into();
        publish_job_status(&job_id, job);
        job.cancellation.clone()
    };

    let result = execute_backtest_with_future_controlled(
        &state,
        &req,
        &future,
        &evaluation,
        Some(&cancellation),
        &mut |progress| update_job_progress(&state, &job_id, progress),
    );

    let execution_cancelled = matches!(&result, Err(BacktestServerError::Cancelled));
    let prepared = if execution_cancelled {
        None
    } else {
        Some(match result {
            Ok(backtest_result) => prepare_result(
                &state,
                result_to_msg(&backtest_result),
                Some(delivery),
                compact_result_for_console,
            ),
            Err(error) => Err(error.to_string()),
        })
    };

    let mut jobs = state.jobs.lock().unwrap();
    let Some(job) = jobs.get_mut(&job_id) else {
        return;
    };
    job.worker_active = false;
    if cancellation.is_cancelled() || job.status == JobStatus::Cancelled || execution_cancelled {
        if let Some(Ok(PreparedResult::Artifact { reference, .. })) = prepared.as_ref() {
            let _ = state.artifact_store.delete(&reference.artifact_id);
        }
        job.cancellation.cancel();
        job.status = JobStatus::Cancelled;
        job.result = None;
        job.artifact = None;
        job.inline_complete = true;
        job.artifact_consumed = false;
        job.error = None;
        job.progress.stage = "cancelled".into();
        job.completed_at.get_or_insert_with(Instant::now);
        publish_job_status(&job_id, job);
        return;
    }

    match prepared.expect("non-cancelled execution has a prepared result") {
        Ok(PreparedResult::Inline(result)) => {
            job.status = JobStatus::Completed;
            job.result = Some(result);
            job.artifact = None;
            job.inline_complete = true;
            job.artifact_consumed = false;
            job.error = None;
            job.progress.stage = "completed".into();
            job.completed_at = Some(Instant::now());
            publish_job_status(&job_id, job);
        }
        Ok(PreparedResult::Artifact { reference, summary }) => {
            job.status = JobStatus::Completed;
            job.result = summary;
            job.artifact = Some(reference);
            job.inline_complete = false;
            job.artifact_consumed = false;
            job.error = None;
            job.progress.stage = "completed".into();
            job.completed_at = Some(Instant::now());
            publish_job_status(&job_id, job);
        }
        Err(error) => {
            job.status = JobStatus::Failed;
            job.result = None;
            job.artifact = None;
            job.inline_complete = true;
            job.artifact_consumed = false;
            job.error = Some(error);
            job.progress.stage = "failed".into();
            job.completed_at = Some(Instant::now());
            publish_job_status(&job_id, job);
        }
    }
}

/// Generate a simple unique ID without external dependencies.
fn uuid_v4_simple() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let nanos = now.as_nanos();
    format!("{:x}", nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use chrono::NaiveDate;
    #[allow(unused_imports)]
    use qs_core::types::{OrderType, Side};
    #[allow(unused_imports)]
    use std::sync::Arc;

    fn test_request(request: BacktestRunSpec) -> RunBacktestRequest {
        RunBacktestRequest {
            request,
            future: FutureQuoteConfigMsg {
                account_currency: "USD".into(),
                ..FutureQuoteConfigMsg::default()
            },
            evaluation: ProviderEvaluationOptionsMsg::default(),
            result_delivery: ResultDeliveryMsg::Auto,
        }
    }

    fn submit_for_test(state: &ServerState, request: &BacktestRunSpec) -> SubmitBacktestResponse {
        handle_submit_backtest(
            state,
            &SubmitBacktestRequest {
                request: test_request(request.clone()),
            },
        )
    }

    fn run_job_for_test(state: Arc<ServerState>, job_id: String, request: BacktestRunSpec) {
        run_job_and_store(state, job_id, test_request(request));
    }

    fn test_state() -> ServerState {
        let artifact_directory = std::env::temp_dir().join(format!(
            "qs_backtest_server_handler_artifacts_{}",
            std::process::id()
        ));
        ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
            max_retained_jobs: 1_000,
            artifact_store: ArtifactStore::new(
                artifact_directory,
                12 * 1024 * 1024,
                1024 * 1024,
                Duration::from_secs(3_600),
                1024 * 1024 * 1024,
            )
            .unwrap(),
        }
    }

    #[test]
    fn parse_datetime_iso() {
        let dt = parse_datetime("2026-01-15T10:30:00").unwrap();
        assert_eq!(dt.to_string(), "2026-01-15 10:30:00");
    }

    #[test]
    fn parse_datetime_space_separator() {
        let dt = parse_datetime("2026-01-15 10:30:00").unwrap();
        assert_eq!(dt.to_string(), "2026-01-15 10:30:00");
    }

    #[test]
    fn parse_datetime_date_only() {
        let dt = parse_datetime("2026-01-15").unwrap();
        assert_eq!(dt.to_string(), "2026-01-15 00:00:00");
    }

    #[test]
    fn parse_datetime_invalid() {
        assert!(parse_datetime("not-a-date").is_err());
    }

    #[test]
    fn parse_optional_datetime_none() {
        assert!(parse_optional_datetime(&None).unwrap().is_none());
    }

    #[test]
    fn parse_optional_datetime_some() {
        let result = parse_optional_datetime(&Some("2026-01-15".into())).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn normalize_symbol_passthrough() {
        let reg = SymbolRegistry::empty();
        assert_eq!(normalize_symbol(&reg, "BTCUSD"), "btcusd");
    }

    #[test]
    fn requested_symbol_scope_normalizes_explicit_symbols() {
        let scope = resolve_requested_symbol_scope(
            &SymbolRegistry::empty(),
            "",
            &["XAU/USD".into(), " GBPJPY ".into()],
            false,
        )
        .unwrap();

        assert_eq!(
            scope,
            RequestedSymbolScope::explicit(["gbpjpy".into(), "xauusd".into()])
        );
    }

    #[test]
    fn replay_plan_derives_inferred_symbols_from_retained_entries() {
        let state = test_state();
        let signals = vec![
            RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "XAUUSD".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: Some(2000.0),
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
            RawSignalMsg::Entry {
                ts: "2026-01-15T10:01:00".into(),
                symbol: "GBP/JPY".into(),
                side: "Sell".into(),
                order_type: "Market".into(),
                price: Some(190.0),
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
        ];
        let plan = build_replay_plan(&state, "", &[], true, &signals, None, None, 0).unwrap();

        assert_eq!(plan.active_symbols(), ["gbpjpy", "xauusd"]);
    }

    #[test]
    fn replay_plan_preserves_explicit_single_symbol_default() {
        let state = test_state();
        let signals = vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: Some(2000.0),
            risk: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let replay =
            build_replay_plan(&state, "XAU/USD", &[], false, &signals, None, None, 0).unwrap();

        assert_eq!(replay.active_symbols(), ["xauusd"]);
    }

    #[test]
    fn replay_plan_rejects_missing_entry_symbol_in_explicit_multi_scope() {
        let state = test_state();
        let signals = vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: Some(2000.0),
            risk: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let error = build_replay_plan(
            &state,
            "",
            &["xauusd".into(), "gbpjpy".into()],
            false,
            &signals,
            None,
            None,
            0,
        )
        .unwrap_err();

        assert!(error.to_string().contains("symbol is required"));
    }

    #[test]
    fn validate_request_rejects_empty_signals() {
        let req = BacktestRunSpec {
            symbol: "eurusd".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "ctrader".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: None,
                close_on_finish: None,
                fill_model: None,
                sizing: None,
            },
        };
        assert!(validate_request(&req).is_err());
    }

    #[test]
    fn validate_request_rejects_invalid_data_type() {
        let req = BacktestRunSpec {
            symbol: "eurusd".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "ctrader".into(),
            data_type: "invalid".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            }],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: None,
                close_on_finish: None,
                fill_model: None,
                sizing: None,
            },
        };
        assert!(validate_request(&req).is_err());
    }

    #[test]
    fn validate_request_rejects_bar_without_timeframe() {
        let req = BacktestRunSpec {
            symbol: "eurusd".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "ctrader".into(),
            data_type: "bar".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            }],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: None,
                close_on_finish: None,
                fill_model: None,
                sizing: None,
            },
        };
        assert!(validate_request(&req).is_err());
    }

    #[test]
    fn validate_request_accepts_valid_tick() {
        let req = BacktestRunSpec {
            symbol: "eurusd".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "ctrader".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            }],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: None,
                close_on_finish: None,
                fill_model: None,
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.01 }),
            },
        };
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn validate_request_rejects_entry_without_sizing() {
        let mut req = BacktestRunSpec {
            symbol: "eurusd".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "ctrader".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                risk: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            }],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: None,
                close_on_finish: None,
                fill_model: None,
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.01 }),
            },
        };
        req.config.sizing = None;
        let state = test_state();
        let replay = build_replay_plan(
            &state,
            &req.symbol,
            &req.symbols,
            req.all_symbols,
            &req.raw_signals,
            None,
            None,
            0,
        )
        .unwrap();

        assert!(validate_request(&req).is_ok());
        assert!(validate_replay_sizing(&req, &replay).is_err());
    }

    #[test]
    fn ping_returns_ok() {
        let state = test_state();
        let resp = handle_ping(&state);
        assert_eq!(resp.status, "OK");
        assert_eq!(resp.data_dir, "/tmp/test");
    }

    #[allow(dead_code)]
    fn temp_data_dir(name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "qs_backtest_server_{name}_{}_{}",
            std::process::id(),
            unique
        ))
    }

    #[test]
    fn resolve_partition_value_matches_case_insensitive_symbol() {
        let root = temp_data_dir("partition_symbol");
        let symbol_dir = root
            .join("ticks")
            .join("exchange=icmarkets")
            .join("symbol=AUDCAD");
        std::fs::create_dir_all(&symbol_dir).unwrap();
        let root_str = root.to_string_lossy().to_string();

        let resolved = resolve_partition_value(
            &root_str,
            "ticks",
            "symbol",
            "audcad",
            "exchange=icmarkets",
            None,
        )
        .unwrap();

        assert_eq!(resolved, "AUDCAD");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_market_events_resolves_uppercase_tick_partition() {
        let root = temp_data_dir("tick_partition");
        let store = ParquetStore::open(&root).unwrap();
        let ts = parse_datetime("2026-01-15T10:00:00").unwrap();
        let ticks = vec![data_preprocess::Tick {
            exchange: "icmarkets".into(),
            symbol: "AUDCAD".into(),
            ts,
            bid: Some(0.9000),
            ask: Some(0.9002),
            last: None,
            volume: None,
            flags: None,
        }];
        store.insert_ticks(&ticks).unwrap();
        let root_str = root.to_string_lossy().to_string();

        let events =
            load_market_events(&root_str, "icmarkets", "audcad", "tick", None, None, None).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Tick {
                symbol, bid, ask, ..
            } => {
                assert_eq!(symbol, "audcad");
                assert_eq!(*bid, 0.9000);
                assert_eq!(*ask, 0.9002);
            }
            MarketEvent::Bar { .. } => panic!("expected tick event"),
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_market_events_resolves_uppercase_bar_partition() {
        let root = temp_data_dir("bar_partition");
        let store = ParquetStore::open(&root).unwrap();
        let ts = parse_datetime("2026-01-15T10:00:00").unwrap();
        let bars = vec![data_preprocess::Bar {
            exchange: "icmarkets".into(),
            symbol: "AUDCAD".into(),
            timeframe: Timeframe::M1,
            ts,
            open: 0.9000,
            high: 0.9010,
            low: 0.8990,
            close: 0.9005,
            tick_vol: 10,
            volume: 0,
            spread: 2,
        }];
        store.insert_bars(&bars).unwrap();
        let root_str = root.to_string_lossy().to_string();

        let events = load_market_events(
            &root_str,
            "icmarkets",
            "audcad",
            "bar",
            Some("1m"),
            None,
            None,
        )
        .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            MarketEvent::Bar { symbol, close, .. } => {
                assert_eq!(symbol, "audcad");
                assert_eq!(*close, 0.9005);
            }
            MarketEvent::Tick { .. } => panic!("expected bar event"),
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn list_profiles_empty_registry() {
        let state = test_state();
        let resp = handle_list_profiles(&state);
        assert!(resp.profiles.is_empty());
    }

    #[test]
    fn add_profile_success() {
        let state = test_state();
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "new_prof".into(),
                target_selection: None,
                use_targets: vec![1],
                close_ratios: vec![1.0],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: false,
        };
        let resp = handle_add_profile(&state, &req);
        assert!(resp.success);
        assert!(resp.error.is_none());
        assert_eq!(resp.profile_count, 1);
    }

    #[test]
    fn add_profile_duplicate_rejected() {
        let state = test_state();
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "dup".into(),
                target_selection: None,
                use_targets: vec![1],
                close_ratios: vec![1.0],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: false,
        };
        let resp1 = handle_add_profile(&state, &req);
        assert!(resp1.success);
        let resp2 = handle_add_profile(&state, &req);
        assert!(!resp2.success);
        assert!(resp2.error.as_ref().unwrap().contains("Duplicate"));
    }

    #[test]
    fn add_profile_overwrite_success() {
        let state = test_state();
        let req1 = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "ow".into(),
                target_selection: None,
                use_targets: vec![1],
                close_ratios: vec![1.0],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: false,
        };
        handle_add_profile(&state, &req1);
        let req2 = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "ow".into(),
                target_selection: None,
                use_targets: vec![1, 2],
                close_ratios: vec![0.5, 0.5],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: true,
        };
        let resp = handle_add_profile(&state, &req2);
        assert!(resp.success);
        assert_eq!(resp.profile_count, 1);
    }

    #[test]
    fn add_profile_invalid_rejected() {
        let state = test_state();
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "bad".into(),
                target_selection: None,
                use_targets: vec![1, 2],
                close_ratios: vec![1.0], // mismatch
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: false,
        };
        let resp = handle_add_profile(&state, &req);
        assert!(!resp.success);
        assert!(resp.error.is_some());
        assert_eq!(resp.profile_count, 0);
    }

    #[test]
    fn remove_profile_success() {
        let state = test_state();
        // Add a profile first.
        let add_req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "rm_me".into(),
                target_selection: None,
                use_targets: vec![1],
                close_ratios: vec![1.0],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            },
            overwrite: false,
        };
        handle_add_profile(&state, &add_req);
        let resp = handle_remove_profile(
            &state,
            &RemoveProfileRequest {
                name: "rm_me".into(),
            },
        );
        assert!(resp.success);
        assert!(resp.error.is_none());
        assert_eq!(resp.profile_count, 0);
    }

    #[test]
    fn remove_profile_not_found() {
        let state = test_state();
        let resp = handle_remove_profile(
            &state,
            &RemoveProfileRequest {
                name: "nope".into(),
            },
        );
        assert!(!resp.success);
        assert!(resp.error.as_ref().unwrap().contains("not found"));
    }

    //
    // Issue 1: filter_signals_by_date tests
    //

    #[test]
    fn filter_signals_by_date_inclusive() {
        use qs_backtest::profile::RawSignal;
        let t = |d: u32| {
            NaiveDate::from_ymd_opt(2026, 3, d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        };
        let signals: Vec<RawSignal> = vec![
            RawSignal::Entry {
                ts: t(8),
                symbol: "X".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 0.01,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
            RawSignal::Entry {
                ts: t(9),
                symbol: "X".into(),
                side: Side::Sell,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 0.01,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
            RawSignal::Entry {
                ts: t(12),
                symbol: "X".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier: 0.01,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
        ];
        let replay = ReplayPlan::build(
            RequestedSymbolScope::explicit(["X".into()]),
            signals,
            Some(t(8)),
            Some(t(11)),
            0,
        )
        .unwrap();
        assert_eq!(replay.retained_signals().len(), 2);
    }

    #[test]
    fn filter_signals_by_date_no_filter() {
        use qs_backtest::profile::RawSignal;
        let signals: Vec<RawSignal> = vec![RawSignal::Entry {
            ts: NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            symbol: "X".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 0.01,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let replay = ReplayPlan::build(
            RequestedSymbolScope::explicit(["X".into()]),
            signals,
            None,
            None,
            0,
        )
        .unwrap();
        assert_eq!(replay.retained_signals().len(), 1);
    }

    //
    // Issue 2: Async job tests
    //

    #[allow(dead_code)]
    fn job_test_state() -> ServerState {
        let mut state = test_state();
        state.symbol_registry = SymbolRegistry::from_toml(
            r#"
[[symbol]]
canonical = "xauusd"
aliases = ["xau/usd"]
pip_position = 1
digits = 2
category = "metal"
base_currency = "XAU"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100
lot_step_units = 1
"#,
        )
        .unwrap();
        state
    }

    #[allow(dead_code)]
    fn valid_submit_request() -> BacktestRunSpec {
        BacktestRunSpec {
            symbol: "XAUUSD".into(),
            symbols: vec![],
            all_symbols: false,
            exchange: "icmarkets".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-01T00:00:00".into(),
                symbol: "xauusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: Some(5000.0),
                risk: 1.0,
                stoploss: Some(4990.0),
                targets: vec![],
                group: None,
                trade_id: None,
            }],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.01 }),
            },
        }
    }

    #[test]
    fn submit_and_cancel_job() {
        let state = job_test_state();
        let submit = submit_for_test(&state, &valid_submit_request());
        assert!(submit.success);
        let job_id = submit.job_id.unwrap();

        let cancel = handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: job_id.clone(),
            },
        );
        assert!(cancel.success);

        let status = handle_get_backtest_status(&state, &GetBacktestStatusRequest { job_id });
        assert_eq!(status.status, "Cancelled");
    }

    #[test]
    fn submit_invalid_request_rejected() {
        let state = job_test_state();
        let mut req = valid_submit_request();
        req.raw_signals = vec![];
        let submit = submit_for_test(&state, &req);
        assert!(!submit.success);
        assert!(submit.job_id.is_none());
    }

    #[test]
    fn get_status_not_found() {
        let state = job_test_state();
        let status = handle_get_backtest_status(
            &state,
            &GetBacktestStatusRequest {
                job_id: "nonexistent".into(),
            },
        );
        assert!(!status.success);
        assert_eq!(status.status, "NotFound");
    }

    #[tokio::test]
    async fn watch_stream_emits_initial_progress_terminal_and_end() {
        let state = Arc::new(job_test_state());
        let submit = submit_for_test(&state, &valid_submit_request());
        let job_id = submit.job_id.unwrap();
        let mut stream = watch_backtest_stream(
            state.clone(),
            WatchBacktestRequest {
                job_id: job_id.clone(),
            },
            Duration::from_secs(60),
        );

        let initial = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            initial,
            BacktestEvent::Snapshot { ref status }
                if status.job_id == job_id && status.status == "Queued"
        ));

        update_job_progress(
            &state,
            &job_id,
            BacktestProgress {
                stage: "replay".into(),
                processed_events: 10,
                total_events: 100,
                processed_signals: 2,
                total_signals: 8,
                processed_symbols: 1,
                total_symbols: 2,
            },
        );
        let progress = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            progress,
            BacktestEvent::Snapshot { ref status }
                if status.status == "Running"
                    && status.progress.processed_events == 10
                    && status.progress.total_events == 100
        ));

        assert!(
            handle_cancel_backtest(
                &state,
                &CancelBacktestRequest {
                    job_id: job_id.clone(),
                },
            )
            .success
        );
        let terminal = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            terminal,
            BacktestEvent::Snapshot { ref status }
                if status.status == "Cancelled" && status.is_terminal()
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn watch_stream_heartbeats_and_resubscribes_to_terminal_snapshot() {
        let state = Arc::new(job_test_state());
        let submit = submit_for_test(&state, &valid_submit_request());
        let job_id = submit.job_id.unwrap();
        let mut stream = watch_backtest_stream(
            state.clone(),
            WatchBacktestRequest {
                job_id: job_id.clone(),
            },
            Duration::from_millis(5),
        );

        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            BacktestEvent::Snapshot { .. }
        ));
        let heartbeat = tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            heartbeat,
            BacktestEvent::Heartbeat { job_id: ref id, .. } if id == &job_id
        ));

        assert!(
            handle_cancel_backtest(
                &state,
                &CancelBacktestRequest {
                    job_id: job_id.clone(),
                },
            )
            .success
        );
        drop(stream);

        let mut resumed = watch_backtest_stream(
            state,
            WatchBacktestRequest {
                job_id: job_id.clone(),
            },
            Duration::from_secs(60),
        );
        assert!(matches!(
            resumed.next().await.unwrap().unwrap(),
            BacktestEvent::Snapshot { ref status }
                if status.job_id == job_id && status.status == "Cancelled"
        ));
        assert!(resumed.next().await.is_none());
    }

    #[tokio::test]
    async fn watch_stream_reports_missing_job_as_stream_error() {
        let mut stream = watch_backtest_stream(
            Arc::new(job_test_state()),
            WatchBacktestRequest {
                job_id: "missing".into(),
            },
            Duration::from_secs(60),
        );
        assert!(matches!(
            stream.next().await,
            Some(Err(xrpc::RpcError::ServerError(error))) if error == "Job 'missing' not found"
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn shutdown_cancellation_publishes_terminal_snapshot() {
        let state = Arc::new(job_test_state());
        let submit = submit_for_test(&state, &valid_submit_request());
        let job_id = submit.job_id.unwrap();
        let (mut updates, _) = subscribe_backtest_status(&state, &job_id).unwrap();

        assert_eq!(cancel_active_jobs(&state), 1);
        updates.changed().await.unwrap();
        let status = updates.borrow().clone();
        assert_eq!(status.status, "Cancelled");
        assert!(status.is_terminal());
    }

    #[test]
    fn cancel_nonexistent_job() {
        let state = job_test_state();
        let resp = handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: "nope".into(),
            },
        );
        assert!(!resp.success);
    }

    //
    // Issue 3: Contract size wiring test
    //

    #[test]
    fn config_from_msg_populates_contract_sizes() {
        let toml = r#"
[[symbol]]
canonical = "xauusd"
aliases = ["gold"]
pip_position = 1
digits = 2
category = "metal"
base_currency = "XAU"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100
lot_step_units = 1
lot_min_steps = 1
lot_max_steps = 0

[[symbol]]
canonical = "gbpjpy"
aliases = []
pip_position = 2
digits = 3
category = "forex"
base_currency = "GBP"
quote_currency = "JPY"
pnl_currency = "JPY"
lot_base_units = 100000
lot_step_units = 1000
lot_min_steps = 1
lot_max_steps = 0
"#;
        let registry = SymbolRegistry::from_toml(toml).unwrap();
        let symbols = vec!["xauusd".to_string(), "gbpjpy".to_string()];
        let msg = BacktestConfigMsg {
            initial_balance: Some(10_000.0),
            close_on_finish: Some(true),
            fill_model: Some("BidAsk".into()),
            sizing: None,
        };
        let config = config_from_msg(&msg, &registry, &symbols).unwrap();
        assert_eq!(config.contract_sizes.get("xauusd"), Some(&100.0));
        assert_eq!(config.contract_sizes.get("gbpjpy"), Some(&100_000.0));
        assert!(!config.symbol_specs.is_empty());
    }

    #[tokio::test]
    async fn job_full_lifecycle_transitions_publish_terminal_snapshot() {
        use std::sync::Arc;
        let state = Arc::new(job_test_state());

        // Submit a valid job.
        let submit = submit_for_test(&state, &valid_submit_request());
        assert!(submit.success);
        let job_id = submit.job_id.unwrap();

        // Status should be Queued.
        let status = handle_get_backtest_status(
            &state,
            &GetBacktestStatusRequest {
                job_id: job_id.clone(),
            },
        );
        assert_eq!(status.status, "Queued");
        let mut stream = watch_backtest_stream(
            state.clone(),
            WatchBacktestRequest {
                job_id: job_id.clone(),
            },
            Duration::from_secs(60),
        );
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            BacktestEvent::Snapshot { ref status } if status.status == "Queued"
        ));

        // Run the job via the canonical worker (will fail since no real data,
        // but should transition through LoadingData -> Failed).
        let req = valid_submit_request();
        run_job_for_test(state.clone(), job_id.clone(), req);

        // Status should be Failed (no market data at /tmp/test).
        let status = handle_get_backtest_status(
            &state,
            &GetBacktestStatusRequest {
                job_id: job_id.clone(),
            },
        );
        assert!(
            status.status == "Failed" || status.status == "Completed",
            "Expected Failed or Completed, got {}",
            status.status
        );
        let terminal = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            terminal,
            BacktestEvent::Snapshot { ref status }
                if status.status == "Failed" || status.status == "Completed"
        ));
        assert!(stream.next().await.is_none());

        // Fetch result should fail since job is not Completed.
        let result_resp = handle_get_backtest_result(&state, &GetBacktestResultRequest { job_id });
        if status.status == "Failed" {
            assert!(!result_resp.success);
        }
    }

    #[test]
    fn concurrent_jobs_independent() {
        let state = job_test_state();

        // Submit two jobs.
        let submit1 = submit_for_test(&state, &valid_submit_request());
        let submit2 = submit_for_test(&state, &valid_submit_request());

        assert!(submit1.success);
        assert!(submit2.success);

        let id1 = submit1.job_id.unwrap();
        let id2 = submit2.job_id.unwrap();

        // IDs must be different.
        assert_ne!(id1, id2);

        // Both should be Queued.
        let s1 = handle_get_backtest_status(
            &state,
            &GetBacktestStatusRequest {
                job_id: id1.clone(),
            },
        );
        let s2 = handle_get_backtest_status(
            &state,
            &GetBacktestStatusRequest {
                job_id: id2.clone(),
            },
        );
        assert_eq!(s1.status, "Queued");
        assert_eq!(s2.status, "Queued");

        // Cancel one, the other should still be Queued.
        handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: id1.clone(),
            },
        );

        let s1_after =
            handle_get_backtest_status(&state, &GetBacktestStatusRequest { job_id: id1 });
        let s2_after =
            handle_get_backtest_status(&state, &GetBacktestStatusRequest { job_id: id2 });
        assert_eq!(s1_after.status, "Cancelled");
        assert_eq!(s2_after.status, "Queued");
    }

    #[test]
    fn job_cleanup_removes_expired() {
        let state = job_test_state();

        // Submit and cancel a job.
        let submit = submit_for_test(&state, &valid_submit_request());
        let job_id = submit.job_id.unwrap();
        handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: job_id.clone(),
            },
        );

        // Job should exist.
        {
            let jobs = state.jobs.lock().unwrap();
            assert!(jobs.contains_key(&job_id));
        }

        // Simulate the spawned worker acknowledging the pre-start cancellation.
        state
            .jobs
            .lock()
            .unwrap()
            .get_mut(&job_id)
            .unwrap()
            .worker_active = false;

        // Cleanup with max_age=0 removes completed/cancelled jobs.
        assert_eq!(cleanup_expired_jobs(&state, Duration::ZERO), 1);

        // Job should be gone.
        {
            let jobs = state.jobs.lock().unwrap();
            assert!(!jobs.contains_key(&job_id));
        }
    }

    #[test]
    fn job_status_enum_roundtrip() {
        assert_eq!(JobStatus::Queued.as_str(), "Queued");
        assert_eq!(JobStatus::LoadingData.as_str(), "LoadingData");
        assert_eq!(JobStatus::Running.as_str(), "Running");
        assert_eq!(JobStatus::Completed.as_str(), "Completed");
        assert_eq!(JobStatus::Failed.as_str(), "Failed");
        assert_eq!(JobStatus::Cancelled.as_str(), "Cancelled");

        assert_eq!("Queued".parse::<JobStatus>(), Ok(JobStatus::Queued));
        assert_eq!(
            "LoadingData".parse::<JobStatus>(),
            Ok(JobStatus::LoadingData)
        );
        assert_eq!("invalid".parse::<JobStatus>(), Err("invalid job status"));
    }
}
