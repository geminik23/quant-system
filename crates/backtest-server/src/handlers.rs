//! RPC handler implementations for the backtest server.
//!
//! Each handler receives a request message, processes it against the shared
//! server state (symbol registry, profile registry, Parquet store), and
//! returns a response message. Errors are captured in the response rather
//! than crashing the server.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::RwLock;
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
    pub symbol_registry: SymbolRegistry,
    pub profile_registry: RwLock<ProfileRegistry>,
    pub data_dir: String,
    pub profiles_path: String,
    pub start_time: Instant,
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

    // 3. Parse date range filters.
    let from = parse_optional_datetime(&req.from)?;
    let to = parse_optional_datetime(&req.to)?;

    // 4. Load and merge market data for every requested symbol.
    let mut feed = load_market_data_for_symbols(
        &state.data_dir,
        &exchange,
        &symbols,
        &req.data_type,
        req.timeframe.as_deref(),
        from,
        to,
    )?;

    // 5. Convert raw signals and run one portfolio backtest.
    let raw_signals = build_raw_signals_for_symbols(state, &req.raw_signals, &symbols)?;
    let profile = resolve_profile(state, req)?;
    let config = config_from_msg(&req.config);
    let runner = BacktestRunner::new(config);
    let result = runner.run_raw_signals(&mut feed, raw_signals, profile.as_ref());
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

    let config = config_from_msg(&req.config);

    // Convert raw signal messages to internal format once.
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
        feeds.push(load_market_data(
            data_dir, exchange, symbol, data_type, timeframe, from, to,
        )?);
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

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        let resp = handle_ping(&state);
        assert_eq!(resp.status, "OK");
        assert_eq!(resp.data_dir, "/tmp/test");
    }

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
}
