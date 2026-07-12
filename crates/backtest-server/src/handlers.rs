//! RPC handler implementations for the backtest server.
//!
//! Each handler receives a request message, processes it against the shared
//! server state (symbol registry, profile registry, Parquet store), and
//! returns a response message. Errors are captured in the response rather
//! than crashing the server.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use chrono::NaiveDateTime;
use data_preprocess::ParquetStore;
use data_preprocess::models::{BarQueryOpts, QueryOpts, Timeframe};
use qs_backtest::BacktestResult;
use qs_backtest::data_feed::{
    DataFeed, MarketEvent, VecFeed, bars_to_feed, merge_feeds, ticks_to_feed,
};
use qs_backtest::profile::{ManagementProfile, ProfileRegistry, RawSignal};
use qs_backtest::runner::{BacktestConfig, BacktestRunner};
use qs_symbols::SymbolRegistry;

use crate::convert::{config_from_msg, profile_from_msg, raw_signal_from_msg, result_to_msg};
use crate::error::{BacktestServerError, Result};
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
    /// Optional progress info (e.g. number of events processed).
    pub progress: Option<String>,
    /// Serialized result message (when completed).
    pub result: Option<BacktestResultMsg>,
    /// Error message (when failed).
    pub error: Option<String>,
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

    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Queued" => Some(Self::Queued),
            "LoadingData" => Some(Self::LoadingData),
            "Running" => Some(Self::Running),
            "Completed" => Some(Self::Completed),
            "Failed" => Some(Self::Failed),
            "Cancelled" => Some(Self::Cancelled),
            _ => None,
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
            // Extract timeframe from data_type if it's a bar (e.g. "bar(1m)").
            let (data_type, timeframe) = if row.data_type.starts_with("bar") {
                let tf = row
                    .data_type
                    .strip_prefix("bar(")
                    .and_then(|s| s.strip_suffix(')'))
                    .map(|s| s.to_string());
                ("bar".to_string(), tf)
            } else {
                (row.data_type.clone(), None)
            };

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

// ── Run Backtest ────────────────────────────────────────────────────────────

/// Handle `run_backtest` — loads data, applies profile, runs backtest, returns result.
pub fn handle_run_backtest(state: &ServerState, req: &RunBacktestRequest) -> RunBacktestResponse {
    let start = Instant::now();

    match execute_backtest(state, req) {
        Ok(result) => {
            let msg = result_to_msg(&result);
            RunBacktestResponse {
                success: true,
                error: None,
                result: Some(msg),
                elapsed_ms: start.elapsed().as_millis() as u64,
            }
        }
        Err(e) => RunBacktestResponse {
            success: false,
            error: Some(e.to_string()),
            result: None,
            elapsed_ms: start.elapsed().as_millis() as u64,
        },
    }
}

/// Internal implementation: validate, load data, transform signals, run backtest.
fn execute_backtest(state: &ServerState, req: &RunBacktestRequest) -> Result<BacktestResult> {
    // 1. Validate request.
    validate_request(req)?;

    // 1b. Validate inline profile early (before expensive data loading).
    if let Some(ref profile_msg) = req.profile_def {
        let profile = profile_from_msg(profile_msg)?;
        profile.validate().map_err(|e| {
            BacktestServerError::InvalidRequest(format!("Invalid inline profile: {e}"))
        })?;
    }

    // 2. Resolve one or more symbols via registry.
    let symbols = resolve_request_symbols(
        &state.symbol_registry,
        &req.symbol,
        &req.symbols,
        req.all_symbols,
        &req.raw_signals,
    )?;
    let exchange = req.exchange.to_lowercase();
    tracing::info!(
        "run_backtest: symbols={:?} exchange={} data_type={}",
        symbols,
        exchange,
        req.data_type
    );

    // 3. Parse date range filters.
    let from = parse_optional_datetime(&req.from)?;
    let to = parse_optional_datetime(&req.to)?;

    // 4. Load and merge market data for every requested symbol.
    tracing::info!(
        "run_backtest: loading market data for {} symbols...",
        symbols.len()
    );
    let mut feed = load_market_data_for_symbols(
        &state.data_dir,
        &exchange,
        &symbols,
        &req.data_type,
        req.timeframe.as_deref(),
        from,
        to,
    )?;
    tracing::info!("run_backtest: market data loaded, {} events", feed.total());

    // 5. Convert raw signals, filter by date range, and run backtest.
    let mut raw_signals = build_raw_signals_for_symbols(state, &req.raw_signals, &symbols)?;
    tracing::info!("run_backtest: {} raw signals converted", raw_signals.len());
    raw_signals = filter_signals_by_date(raw_signals, from, to);
    tracing::info!(
        "run_backtest: {} signals after date filtering",
        raw_signals.len()
    );
    let profile = resolve_profile(state, req)?;
    let config = config_from_msg(&req.config, &state.symbol_registry, &symbols);
    let runner = BacktestRunner::new(config);
    tracing::info!("run_backtest: starting backtest engine...");
    let result = runner.run_raw_signals(&mut feed, raw_signals, profile.as_ref());
    tracing::info!(
        "run_backtest: done, {} trades, {} positions",
        result.total_trades,
        result.total_positions
    );
    Ok(result)
}

// ── Run Backtest Multi ──────────────────────────────────────────────────────

/// Handle `run_backtest_multi` — compares multiple profiles on the same data.
pub fn handle_run_backtest_multi(
    state: &ServerState,
    req: &RunBacktestMultiRequest,
) -> RunBacktestMultiResponse {
    let start = Instant::now();

    let results = execute_backtest_multi(state, req);

    RunBacktestMultiResponse {
        results,
        elapsed_ms: start.elapsed().as_millis() as u64,
    }
}

/// Internal: validate once, load data once, run each profile.
fn execute_backtest_multi(
    state: &ServerState,
    req: &RunBacktestMultiRequest,
) -> Vec<ProfileResult> {
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

    let symbols = match resolve_request_symbols(
        &state.symbol_registry,
        &req.symbol,
        &req.symbols,
        req.all_symbols,
        &req.raw_signals,
    ) {
        Ok(v) => v,
        Err(e) => {
            return req
                .profiles
                .iter()
                .map(|pr| ProfileResult {
                    profile: profile_ref_name(pr),
                    success: false,
                    error: Some(e.to_string()),
                    result: None,
                })
                .collect();
        }
    };
    let exchange = req.exchange.to_lowercase();

    let from = match parse_optional_datetime(&req.from) {
        Ok(v) => v,
        Err(e) => {
            return req
                .profiles
                .iter()
                .map(|pr| ProfileResult {
                    profile: profile_ref_name(pr),
                    success: false,
                    error: Some(e.to_string()),
                    result: None,
                })
                .collect();
        }
    };

    let to = match parse_optional_datetime(&req.to) {
        Ok(v) => v,
        Err(e) => {
            return req
                .profiles
                .iter()
                .map(|pr| ProfileResult {
                    profile: profile_ref_name(pr),
                    success: false,
                    error: Some(e.to_string()),
                    result: None,
                })
                .collect();
        }
    };

    // Load market data once - shared across all profile runs.
    let events = match load_market_events_for_symbols(
        &state.data_dir,
        &exchange,
        &symbols,
        &data_type,
        req.timeframe.as_deref(),
        from,
        to,
    ) {
        Ok(events) => events,
        Err(e) => {
            return req
                .profiles
                .iter()
                .map(|pr| ProfileResult {
                    profile: profile_ref_name(pr),
                    success: false,
                    error: Some(e.to_string()),
                    result: None,
                })
                .collect();
        }
    };

    let config = config_from_msg(&req.config, &state.symbol_registry, &symbols);

    // Convert raw signal messages to internal format once, then filter by date.
    let raw_signals_vec: Vec<RawSignal> =
        match build_raw_signals_for_symbols(state, &req.raw_signals, &symbols) {
            Ok(v) => v,
            Err(e) => {
                return req
                    .profiles
                    .iter()
                    .map(|pr| ProfileResult {
                        profile: profile_ref_name(pr),
                        success: false,
                        error: Some(e.to_string()),
                        result: None,
                    })
                    .collect();
            }
        };
    let raw_signals_vec = filter_signals_by_date(raw_signals_vec, from, to);

    // Run each profile independently.
    req.profiles
        .iter()
        .map(|pr| {
            let name = profile_ref_name(pr);
            let run_result = match pr {
                ProfileRef::Named(profile_name) => {
                    let registry = state.profile_registry.read().unwrap();
                    run_single_profile(&registry, profile_name, &raw_signals_vec, &events, &config)
                }
                ProfileRef::Inline(msg) => match profile_from_msg(msg) {
                    Ok(profile) => {
                        if let Err(e) = profile.validate() {
                            Err(BacktestServerError::InvalidRequest(format!(
                                "Invalid inline profile: {e}"
                            )))
                        } else {
                            run_profile_direct(&profile, &raw_signals_vec, &events, &config)
                        }
                    }
                    Err(e) => Err(e),
                },
            };
            match run_result {
                Ok(result) => ProfileResult {
                    profile: name,
                    success: true,
                    error: None,
                    result: Some(result_to_msg(&result)),
                },
                Err(e) => ProfileResult {
                    profile: name,
                    success: false,
                    error: Some(e.to_string()),
                    result: None,
                },
            }
        })
        .collect()
}

/// Extract the profile name from a `ProfileRef`.
fn profile_ref_name(pr: &ProfileRef) -> String {
    match pr {
        ProfileRef::Named(name) => name.clone(),
        ProfileRef::Inline(msg) => msg.name.clone(),
    }
}

/// Run a single named profile against pre-loaded events (acquires read lock).
fn run_single_profile(
    registry: &ProfileRegistry,
    profile_name: &str,
    raw_signals: &[RawSignal],
    events: &[qs_backtest::data_feed::MarketEvent],
    config: &BacktestConfig,
) -> Result<BacktestResult> {
    let profile = registry
        .get(profile_name)
        .ok_or_else(|| BacktestServerError::ProfileNotFound(profile_name.into()))?;

    let mut feed = VecFeed::new(events.to_vec());
    let runner = BacktestRunner::new(config.clone());
    let result = runner.run_raw_signals(&mut feed, raw_signals.to_vec(), Some(profile));
    Ok(result)
}

/// Run a profile directly (already converted from inline definition).
fn run_profile_direct(
    profile: &ManagementProfile,
    raw_signals: &[RawSignal],
    events: &[qs_backtest::data_feed::MarketEvent],
    config: &BacktestConfig,
) -> Result<BacktestResult> {
    let mut feed = VecFeed::new(events.to_vec());
    let runner = BacktestRunner::new(config.clone());
    let result = runner.run_raw_signals(&mut feed, raw_signals.to_vec(), Some(profile));
    Ok(result)
}

// ── Signal Conversion ───────────────────────────────────────────────────────

/// Resolve the market-data symbols requested for one portfolio backtest.
fn resolve_request_symbols(
    registry: &SymbolRegistry,
    symbol: &str,
    symbols: &[String],
    all_symbols: bool,
    raw_signals: &[RawSignalMsg],
) -> Result<Vec<String>> {
    let mut resolved = BTreeSet::new();

    if all_symbols {
        for signal in raw_signals {
            collect_raw_signal_symbols(registry, signal, &mut resolved);
        }
        if resolved.is_empty() {
            return Err(BacktestServerError::InvalidRequest(
                "all_symbols requested, but no symbols were found in raw entry signals".into(),
            ));
        }
    } else if !symbols.is_empty() {
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

    Ok(resolved.into_iter().collect())
}

fn collect_raw_signal_symbols(
    registry: &SymbolRegistry,
    signal: &RawSignalMsg,
    symbols: &mut BTreeSet<String>,
) {
    match signal {
        RawSignalMsg::Entry { symbol, .. }
        | RawSignalMsg::CloseAllOf { symbol, .. }
        | RawSignalMsg::ModifyAllStoploss { symbol, .. } => {
            let trimmed = symbol.trim();
            if !trimmed.is_empty() {
                symbols.insert(normalize_symbol(registry, trimmed));
            }
        }
        RawSignalMsg::Close { position, .. }
        | RawSignalMsg::ClosePartial { position, .. }
        | RawSignalMsg::ModifyStoploss { position, .. }
        | RawSignalMsg::MoveStoplossToEntry { position, .. }
        | RawSignalMsg::AddTarget { position, .. }
        | RawSignalMsg::RemoveTarget { position, .. }
        | RawSignalMsg::AddRule { position, .. }
        | RawSignalMsg::RemoveRule { position, .. }
        | RawSignalMsg::ScaleIn { position, .. }
        | RawSignalMsg::CancelPending { position, .. } => {
            if let PositionRefMsg::AllOnSymbol { symbol } = position {
                let trimmed = symbol.trim();
                if !trimmed.is_empty() {
                    symbols.insert(normalize_symbol(registry, trimmed));
                }
            }
        }
        RawSignalMsg::CloseAll { .. }
        | RawSignalMsg::CancelAllPending { .. }
        | RawSignalMsg::CloseAllInGroup { .. }
        | RawSignalMsg::ModifyAllStoplossInGroup { .. } => {}
    }
}

/// Build `Vec<RawSignal>` from the `raw_signals` field.
fn build_raw_signals_for_symbols(
    state: &ServerState,
    raw_signal_msgs: &[RawSignalMsg],
    symbols: &[String],
) -> Result<Vec<RawSignal>> {
    let default_symbol = if symbols.len() == 1 {
        symbols[0].as_str()
    } else {
        ""
    };
    let raw_signals = raw_signal_msgs
        .iter()
        .map(|s| raw_signal_from_msg(s, default_symbol, &state.symbol_registry))
        .collect::<Result<Vec<_>>>()?;

    if symbols.len() > 1 {
        ensure_entry_symbols_are_explicit(raw_signal_msgs)?;
        ensure_entry_symbols_are_loaded(&raw_signals, symbols)?;
    }

    Ok(raw_signals)
}

fn ensure_entry_symbols_are_explicit(raw_signal_msgs: &[RawSignalMsg]) -> Result<()> {
    for signal in raw_signal_msgs {
        if let RawSignalMsg::Entry { symbol, .. } = signal {
            if symbol.trim().is_empty() {
                return Err(BacktestServerError::InvalidRequest(
                    "Entry signal symbol is required for multi-symbol backtests".into(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_entry_symbols_are_loaded(raw_signals: &[RawSignal], symbols: &[String]) -> Result<()> {
    let loaded: BTreeSet<&str> = symbols.iter().map(String::as_str).collect();
    for signal in raw_signals {
        if let RawSignal::Entry { symbol, .. } = signal {
            if !loaded.contains(symbol.as_str()) {
                return Err(BacktestServerError::InvalidRequest(format!(
                    "Entry signal symbol '{symbol}' is not included in requested market-data symbols: {}",
                    symbols.join(",")
                )));
            }
        }
    }
    Ok(())
}

/// Resolve the management profile from a request (inline or named).
fn resolve_profile(
    state: &ServerState,
    req: &RunBacktestRequest,
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

/// Load market data from Parquet and return a VecFeed.
fn load_market_data(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<VecFeed> {
    let events = load_market_events(data_dir, exchange, symbol, data_type, timeframe, from, to)?;
    Ok(VecFeed::new(events))
}

/// Load market data for multiple symbols and merge the events into one timeline.
fn load_market_data_for_symbols(
    data_dir: &str,
    exchange: &str,
    symbols: &[String],
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<VecFeed> {
    if symbols.is_empty() {
        return Err(BacktestServerError::InvalidRequest(
            "At least one symbol is required".into(),
        ));
    }

    let mut feeds = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        tracing::info!("run_backtest: loading {}...", symbol);
        feeds.push(load_market_data(
            data_dir, exchange, symbol, data_type, timeframe, from, to,
        )?);
        tracing::info!("run_backtest: loaded {}", symbol);
    }

    Ok(merge_feeds(feeds))
}

/// Load raw market events for one or more symbols.
fn load_market_events_for_symbols(
    data_dir: &str,
    exchange: &str,
    symbols: &[String],
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<Vec<MarketEvent>> {
    Ok(feed_to_events(load_market_data_for_symbols(
        data_dir, exchange, symbols, data_type, timeframe, from, to,
    )?))
}

/// Load raw market events from Parquet (shared between single and multi runs).
fn load_market_events(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<Vec<MarketEvent>> {
    let store = ParquetStore::open(data_dir)?;
    let dt = data_type.to_lowercase();

    if dt == "tick" {
        let disk_exchange = resolve_partition_value(data_dir, "ticks", "exchange", exchange, "");
        let disk_symbol = resolve_partition_value(
            data_dir,
            "ticks",
            "symbol",
            symbol,
            &format!("exchange={disk_exchange}"),
        );
        let opts = QueryOpts {
            exchange: disk_exchange.clone(),
            symbol: disk_symbol.clone(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (ticks, _total) = store.query_ticks(&opts)?;
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
        let disk_exchange = resolve_partition_value(data_dir, "bars", "exchange", exchange, "");
        let disk_symbol = resolve_partition_value(
            data_dir,
            "bars",
            "symbol",
            symbol,
            &format!("exchange={disk_exchange}"),
        );
        let disk_timeframe = resolve_partition_value(
            data_dir,
            "bars",
            "timeframe",
            tf.as_str(),
            &format!("exchange={disk_exchange}/symbol={disk_symbol}"),
        );
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
        let (bars, _total) = store.query_bars(&opts)?;
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

/// Resolve a Hive partition value by scanning child directories case-insensitively.
fn resolve_partition_value(
    data_dir: &str,
    data_subdir: &str,
    key: &str,
    requested: &str,
    parent: &str,
) -> String {
    let dir = if parent.is_empty() {
        Path::new(data_dir).join(data_subdir)
    } else {
        Path::new(data_dir).join(data_subdir).join(parent)
    };
    let prefix = format!("{key}=");
    let mut case_insensitive_match = None;

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(value) = name.strip_prefix(&prefix) else {
                continue;
            };
            if value == requested {
                return value.to_string();
            }
            if case_insensitive_match.is_none() && value.eq_ignore_ascii_case(requested) {
                case_insensitive_match = Some(value.to_string());
            }
        }
    }

    case_insensitive_match.unwrap_or_else(|| requested.to_string())
}

/// Rewrite loaded market events to the canonical symbol used by signals.
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
fn feed_to_events(mut feed: VecFeed) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(feed.total());
    while let Some(event) = feed.next_event() {
        events.push(event);
    }
    events
}

// ── Validation ──────────────────────────────────────────────────────────────

/// Validate the common fields of a run_backtest request.
fn validate_request(req: &RunBacktestRequest) -> Result<()> {
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

/// Filter raw signals to only those within the requested date range.
///
/// Signals outside the range are dropped.  When both `from` and `to` are
/// `None` the full list is returned unchanged.  This is the authoritative
/// server-side filter; correctness must not depend on client behaviour.
fn filter_signals_by_date(
    signals: Vec<RawSignal>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Vec<RawSignal> {
    if from.is_none() && to.is_none() {
        return signals;
    }
    signals
        .into_iter()
        .filter(|s| {
            let ts = s.ts();
            let after_from = from.map_or(true, |f| ts >= f);
            let before_to = to.map_or(true, |t| ts <= t);
            after_from && before_to
        })
        .collect()
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

/// Handle `submit_backtest` — queue a backtest for async execution.
///
/// Returns immediately with a job_id.  The caller polls
/// `get_backtest_status` and fetches results via `get_backtest_result`.
pub fn handle_submit_backtest(
    state: &ServerState,
    req: &SubmitBacktestRequest,
) -> SubmitBacktestResponse {
    // Validate request before creating job.
    if let Err(e) = validate_request(&req.request) {
        return SubmitBacktestResponse {
            success: false,
            job_id: None,
            error: Some(e.to_string()),
        };
    }

    let job_id = format!("job-{}", uuid_v4_simple());
    let job = BacktestJob {
        status: JobStatus::Queued,
        submitted_at: Instant::now(),
        completed_at: None,
        progress: None,
        result: None,
        error: None,
    };
    {
        let mut jobs = state.jobs.lock().unwrap();
        jobs.insert(job_id.clone(), job);
    }
    SubmitBacktestResponse {
        success: true,
        job_id: Some(job_id),
        error: None,
    }
}

/// Handle `get_backtest_status` — poll the status of a submitted job.
pub fn handle_get_backtest_status(
    state: &ServerState,
    req: &GetBacktestStatusRequest,
) -> BacktestStatusResponse {
    let jobs = state.jobs.lock().unwrap();
    match jobs.get(&req.job_id) {
        Some(job) => {
            let elapsed_ms = job
                .completed_at
                .map(|c| c.duration_since(job.submitted_at).as_millis() as u64);
            BacktestStatusResponse {
                success: true,
                job_id: req.job_id.clone(),
                status: job.status.as_str().to_string(),
                error: job.error.clone(),
                elapsed_ms,
            }
        }
        None => BacktestStatusResponse {
            success: false,
            job_id: req.job_id.clone(),
            status: "NotFound".into(),
            error: Some(format!("Job '{}' not found", req.job_id)),
            elapsed_ms: None,
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
        Some(job) if job.status == JobStatus::Completed => GetBacktestResultResponse {
            success: true,
            job_id: req.job_id.clone(),
            result: job.result.clone(),
            error: None,
        },
        Some(job) => GetBacktestResultResponse {
            success: false,
            job_id: req.job_id.clone(),
            result: None,
            error: Some(format!(
                "Job is not completed (status: {})",
                job.status.as_str()
            )),
        },
        None => GetBacktestResultResponse {
            success: false,
            job_id: req.job_id.clone(),
            result: None,
            error: Some(format!("Job '{}' not found", req.job_id)),
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
            job.status = JobStatus::Cancelled;
            job.completed_at = Some(Instant::now());
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

/// Run a submitted backtest job synchronously and store the result.
/// Called from spawn_blocking in the server binary.
pub fn run_job_and_store(state: Arc<ServerState>, job_id: String, req: RunBacktestRequest) {
    {
        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            if job.status == JobStatus::Cancelled {
                return;
            }
            job.status = JobStatus::LoadingData;
        } else {
            return;
        }
    }

    // Transition to Running before the backtest engine starts.
    {
        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            if job.status == JobStatus::Cancelled {
                return;
            }
            job.status = JobStatus::Running;
        }
    }

    let result = execute_backtest(&state, &req);

    {
        let mut jobs = state.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            if job.status == JobStatus::Cancelled {
                return;
            }
            match result {
                Ok(backtest_result) => {
                    job.status = JobStatus::Completed;
                    job.result = Some(result_to_msg(&backtest_result));
                    job.progress = Some(format!(
                        "{} trades, {} positions",
                        backtest_result.total_trades, backtest_result.total_positions
                    ));
                    job.completed_at = Some(Instant::now());
                }
                Err(e) => {
                    job.status = JobStatus::Failed;
                    job.error = Some(e.to_string());
                    job.completed_at = Some(Instant::now());
                }
            }
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

/// Remove expired jobs from the job map.
/// Completed/Failed/Cancelled jobs older than `max_age_secs` are removed.
pub fn cleanup_expired_jobs(state: &ServerState, max_age_secs: u64) {
    let now = Instant::now();
    let max_duration = std::time::Duration::from_secs(max_age_secs);
    let mut jobs = state.jobs.lock().unwrap();
    jobs.retain(|_, job| {
        let completed = match job.completed_at {
            Some(c) => c,
            None => return true, // still running, keep
        };
        now.duration_since(completed) < max_duration
    });
}

mod tests {
    use super::*;
    #[allow(unused_imports)]
    use chrono::NaiveDate;
    #[allow(unused_imports)]
    use qs_core::types::{OrderType, Side};
    #[allow(unused_imports)]
    use std::sync::Arc;

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
    fn resolve_request_symbols_uses_explicit_symbols() {
        let reg = SymbolRegistry::empty();
        let signals = vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "xauusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: Some(2000.0),
            size: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let symbols = resolve_request_symbols(
            &reg,
            "",
            &["XAU/USD".into(), " GBPJPY ".into()],
            false,
            &signals,
        )
        .unwrap();
        assert_eq!(symbols, vec!["gbpjpy", "xauusd"]);
    }

    #[test]
    fn resolve_request_symbols_derives_all_symbols_from_entries() {
        let reg = SymbolRegistry::empty();
        let signals = vec![
            RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "XAUUSD".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: Some(2000.0),
                size: 1.0,
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
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
        ];
        let symbols = resolve_request_symbols(&reg, "", &[], true, &signals).unwrap();
        assert_eq!(symbols, vec!["gbpjpy", "xauusd"]);
    }

    #[test]
    fn build_raw_signals_for_multi_symbol_rejects_empty_entry_symbol() {
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let signals = vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: Some(2000.0),
            size: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let err =
            build_raw_signals_for_symbols(&state, &signals, &["xauusd".into(), "gbpjpy".into()])
                .unwrap_err();
        assert!(err.to_string().contains("symbol is required"));
    }

    #[test]
    fn validate_request_rejects_empty_signals() {
        let req = RunBacktestRequest {
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
        let req = RunBacktestRequest {
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
                size: 1.0,
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
        let req = RunBacktestRequest {
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
                size: 1.0,
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
        let req = RunBacktestRequest {
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
                size: 1.0,
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
        assert!(validate_request(&req).is_ok());
    }

    #[test]
    fn ping_returns_ok() {
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
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

        let resolved =
            resolve_partition_value(&root_str, "ticks", "symbol", "audcad", "exchange=icmarkets");

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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let resp = handle_list_profiles(&state);
        assert!(resp.profiles.is_empty());
    }

    #[test]
    fn add_profile_success() {
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "new_prof".into(),
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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "dup".into(),
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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let req1 = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "ow".into(),
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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        let req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "bad".into(),
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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
        // Add a profile first.
        let add_req = AddProfileRequest {
            profile: ManagementProfileMsg {
                name: "rm_me".into(),
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
        let state = ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        };
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
                size: 0.01,
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
                size: 0.01,
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
                size: 0.01,
                stoploss: None,
                targets: vec![],
                group: None,
                trade_id: None,
            },
        ];
        let filtered = filter_signals_by_date(signals, Some(t(8)), Some(t(11)));
        assert_eq!(filtered.len(), 2);
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
            size: 0.01,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }];
        let filtered = filter_signals_by_date(signals, None, None);
        assert_eq!(filtered.len(), 1);
    }

    //
    // Issue 2: Async job tests
    //

    #[allow(dead_code)]
    fn job_test_state() -> ServerState {
        ServerState {
            symbol_registry: SymbolRegistry::empty(),
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: "/tmp/test".into(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    #[allow(dead_code)]
    fn valid_submit_request() -> SubmitBacktestRequest {
        SubmitBacktestRequest {
            request: RunBacktestRequest {
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
                    size: 0.01,
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
                    sizing: None,
                },
            },
        }
    }

    #[test]
    fn submit_and_cancel_job() {
        let state = job_test_state();
        let submit = handle_submit_backtest(&state, &valid_submit_request());
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
        req.request.raw_signals = vec![];
        let submit = handle_submit_backtest(&state, &req);
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
        let config = config_from_msg(&msg, &registry, &symbols);
        assert_eq!(config.contract_sizes.get("xauusd"), Some(&100.0));
        assert_eq!(config.contract_sizes.get("gbpjpy"), Some(&100_000.0));
        assert!(!config.symbol_specs.is_empty());
    }

    #[test]
    fn job_full_lifecycle_transitions() {
        use std::sync::Arc;
        let state = Arc::new(job_test_state());

        // Submit a valid job.
        let submit = handle_submit_backtest(&state, &valid_submit_request());
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

        // Run the job via run_job_and_store (will fail since no real data,
        // but should transition through LoadingData -> Failed).
        let req = valid_submit_request().request;
        run_job_and_store(state.clone(), job_id.clone(), req);

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
        let submit1 = handle_submit_backtest(&state, &valid_submit_request());
        let submit2 = handle_submit_backtest(&state, &valid_submit_request());

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
        let submit = handle_submit_backtest(&state, &valid_submit_request());
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

        // Cleanup with max_age=0 removes completed/cancelled jobs.
        cleanup_expired_jobs(&state, 0);

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

        assert_eq!(JobStatus::from_str("Queued"), Some(JobStatus::Queued));
        assert_eq!(
            JobStatus::from_str("LoadingData"),
            Some(JobStatus::LoadingData)
        );
        assert_eq!(JobStatus::from_str("invalid"), None);
    }
}
