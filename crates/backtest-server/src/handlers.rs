//! RPC handler implementations for the backtest server.
//!
//! Each handler receives a request message, processes it against the shared
//! server state (symbol registry, profile registry, Parquet store), and
//! returns a response message. Errors are captured in the response rather
//! than crashing the server.

use std::sync::RwLock;
use std::time::Instant;

use chrono::NaiveDateTime;
use data_preprocess::ParquetStore;
use data_preprocess::models::{BarQueryOpts, QueryOpts, Timeframe};
use qs_backtest::BacktestResult;
use qs_backtest::data_feed::{DataFeed, VecFeed, bars_to_feed, ticks_to_feed};
use qs_backtest::profile::{ManagementProfile, ProfileRegistry, RawSignal, RawSignalEntry};
use qs_backtest::runner::{BacktestConfig, BacktestRunner};
use qs_core::types::{Action, OrderType, Side, Signal};
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

    // 2. Normalize symbol via registry.
    let symbol = normalize_symbol(&state.symbol_registry, &req.symbol);
    let exchange = req.exchange.to_lowercase();

    // 3. Parse date range filters.
    let from = parse_optional_datetime(&req.from)?;
    let to = parse_optional_datetime(&req.to)?;

    // 4. Load market data from Parquet store.
    let mut feed = load_market_data(
        &state.data_dir,
        &exchange,
        &symbol,
        &req.data_type,
        req.timeframe.as_deref(),
        from,
        to,
    )?;

    // 5. Check for raw_signals (F14) — takes precedence over signals.
    if !req.raw_signals.is_empty() {
        let raw_signals = build_raw_signals(state, req, &symbol)?;
        let profile = resolve_profile(state, req)?;
        let config = config_from_msg(&req.config);
        let runner = BacktestRunner::new(config);
        let result = runner.run_raw_signals(&mut feed, raw_signals, profile.as_ref());
        return Ok(result);
    }

    // 6. Legacy path: convert signals and optionally apply profile.
    let signals = build_signals(state, req, &symbol)?;

    // 7. Run the backtest.
    let config = config_from_msg(&req.config);
    let runner = BacktestRunner::new(config);
    let result = runner.run_signals(&mut feed, signals);

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

    let symbol = normalize_symbol(&state.symbol_registry, &req.symbol);
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

    // Load market data once — shared across all profile runs.
    let events = match load_market_events(
        &state.data_dir,
        &exchange,
        &symbol,
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

    // Check for raw_signals (F14) — takes precedence over signals.
    let use_raw = !req.raw_signals.is_empty();

    // Convert signal messages to internal format once.
    let legacy_signals: Option<Vec<RawSignalEntry>> = if use_raw {
        None
    } else {
        match req
            .signals
            .iter()
            .map(|s| convert_signal_msg(s, &symbol, &state.symbol_registry))
            .collect::<Result<Vec<_>>>()
        {
            Ok(v) => Some(v),
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
        }
    };

    let raw_signals_vec: Option<Vec<RawSignal>> = if use_raw {
        match req
            .raw_signals
            .iter()
            .map(|s| raw_signal_from_msg(s, &symbol, &state.symbol_registry))
            .collect::<Result<Vec<_>>>()
        {
            Ok(v) => Some(v),
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
        }
    } else {
        None
    };

    // Run each profile independently.
    req.profiles
        .iter()
        .map(|pr| {
            let name = profile_ref_name(pr);
            let run_result = if use_raw {
                let raw_sigs = raw_signals_vec.as_ref().unwrap();
                match pr {
                    ProfileRef::Named(profile_name) => {
                        let registry = state.profile_registry.read().unwrap();
                        run_single_profile_raw(&registry, profile_name, raw_sigs, &events, &config)
                    }
                    ProfileRef::Inline(msg) => match profile_from_msg(msg) {
                        Ok(profile) => {
                            if let Err(e) = profile.validate() {
                                Err(BacktestServerError::InvalidRequest(format!(
                                    "Invalid inline profile: {e}"
                                )))
                            } else {
                                run_profile_direct_raw(&profile, raw_sigs, &events, &config)
                            }
                        }
                        Err(e) => Err(e),
                    },
                }
            } else {
                let legacy_sigs = legacy_signals.as_ref().unwrap();
                match pr {
                    ProfileRef::Named(profile_name) => {
                        let registry = state.profile_registry.read().unwrap();
                        run_single_profile(&registry, profile_name, legacy_sigs, &events, &config)
                    }
                    ProfileRef::Inline(msg) => match profile_from_msg(msg) {
                        Ok(profile) => {
                            if let Err(e) = profile.validate() {
                                Err(BacktestServerError::InvalidRequest(format!(
                                    "Invalid inline profile: {e}"
                                )))
                            } else {
                                run_profile_direct(&profile, legacy_sigs, &events, &config)
                            }
                        }
                        Err(e) => Err(e),
                    },
                }
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
    raw_signals: &[RawSignalEntry],
    events: &[qs_backtest::data_feed::MarketEvent],
    config: &BacktestConfig,
) -> Result<BacktestResult> {
    let profile = registry
        .get(profile_name)
        .ok_or_else(|| BacktestServerError::ProfileNotFound(profile_name.into()))?;

    let signals = profile.apply_batch(raw_signals);

    let mut feed = VecFeed::new(events.to_vec());
    let runner = BacktestRunner::new(config.clone());
    let result = runner.run_signals(&mut feed, signals);
    Ok(result)
}

/// Run a profile directly (already converted from inline definition).
fn run_profile_direct(
    profile: &ManagementProfile,
    raw_signals: &[RawSignalEntry],
    events: &[qs_backtest::data_feed::MarketEvent],
    config: &BacktestConfig,
) -> Result<BacktestResult> {
    let signals = profile.apply_batch(raw_signals);
    let mut feed = VecFeed::new(events.to_vec());
    let runner = BacktestRunner::new(config.clone());
    let result = runner.run_signals(&mut feed, signals);
    Ok(result)
}

/// Run a single named profile with raw signals (F14).
fn run_single_profile_raw(
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

/// Run a profile directly with raw signals (F14).
fn run_profile_direct_raw(
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

/// Build the Vec<Signal> from request, optionally applying a management profile.
///
/// Priority: inline `profile_def` > named `profile` > raw passthrough.
fn build_signals(
    state: &ServerState,
    req: &RunBacktestRequest,
    symbol: &str,
) -> Result<Vec<Signal>> {
    // Convert wire messages to internal RawSignalEntry.
    let raw_signals: Vec<RawSignalEntry> = req
        .signals
        .iter()
        .map(|s| convert_signal_msg(s, symbol, &state.symbol_registry))
        .collect::<Result<Vec<_>>>()?;

    if let Some(ref profile_msg) = req.profile_def {
        // Inline profile definition takes highest priority.
        let profile = profile_from_msg(profile_msg)?;
        profile.validate().map_err(|e| {
            BacktestServerError::InvalidRequest(format!("Invalid inline profile: {e}"))
        })?;
        Ok(profile.apply_batch(&raw_signals))
    } else if let Some(ref profile_name) = req.profile {
        // Named profile lookup via registry.
        let registry = state.profile_registry.read().unwrap();
        let profile = registry
            .get(profile_name)
            .ok_or_else(|| BacktestServerError::ProfileNotFound(profile_name.clone()))?;
        Ok(profile.apply_batch(&raw_signals))
    } else {
        // No profile — convert raw signals directly to Action::Open.
        let signals: Vec<Signal> = raw_signals
            .into_iter()
            .filter_map(|raw| {
                let action = Action::Open {
                    symbol: raw.symbol,
                    side: raw.side,
                    order_type: raw.order_type,
                    price: raw.price,
                    size: raw.size,
                    stoploss: raw.stoploss,
                    targets: raw
                        .targets
                        .iter()
                        .enumerate()
                        .map(|(_i, &price)| qs_core::types::TargetSpec {
                            price,
                            close_ratio: if raw.targets.len() == 1 {
                                1.0
                            } else {
                                1.0 / raw.targets.len() as f64
                            },
                        })
                        .collect(),
                    rules: Vec::new(),
                    group: raw.group,
                };
                Some(Signal { ts: raw.ts, action })
            })
            .collect();
        Ok(signals)
    }
}

/// Convert a wire-format signal message to the internal `RawSignalEntry`.
fn convert_signal_msg(
    msg: &RawSignalEntryMsg,
    default_symbol: &str,
    registry: &SymbolRegistry,
) -> Result<RawSignalEntry> {
    let ts = parse_datetime(&msg.ts)?;
    let symbol = if msg.symbol.is_empty() {
        default_symbol.to_string()
    } else {
        normalize_symbol(registry, &msg.symbol)
    };
    let side = parse_side(&msg.side)?;
    let order_type = parse_order_type(&msg.order_type)?;

    Ok(RawSignalEntry {
        ts,
        symbol,
        side,
        order_type,
        price: msg.price,
        size: msg.size,
        stoploss: msg.stoploss,
        targets: msg.targets.clone(),
        group: msg.group.clone(),
    })
}

/// Build `Vec<RawSignal>` from the `raw_signals` field (F14).
fn build_raw_signals(
    state: &ServerState,
    req: &RunBacktestRequest,
    symbol: &str,
) -> Result<Vec<RawSignal>> {
    req.raw_signals
        .iter()
        .map(|s| raw_signal_from_msg(s, symbol, &state.symbol_registry))
        .collect::<Result<Vec<_>>>()
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

/// Load raw market events from Parquet (shared between single and multi runs).
fn load_market_events(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<Vec<qs_backtest::data_feed::MarketEvent>> {
    let store = ParquetStore::open(data_dir)?;
    let dt = data_type.to_lowercase();

    if dt == "tick" {
        let opts = QueryOpts {
            exchange: exchange.into(),
            symbol: symbol.into(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (ticks, _total) = store.query_ticks(&opts)?;
        if ticks.is_empty() {
            return Err(BacktestServerError::NoDataFound {
                symbol: symbol.into(),
                exchange: exchange.into(),
                data_type: "tick".into(),
            });
        }
        let feed = ticks_to_feed(ticks);
        // Extract the events from the VecFeed via clone to get the inner vec.
        // VecFeed::new sorts nothing, so we just need the events.
        Ok(feed_to_events(feed))
    } else if dt == "bar" {
        let tf_str = timeframe.ok_or_else(|| {
            BacktestServerError::InvalidRequest("timeframe is required for bar data".into())
        })?;
        let tf = Timeframe::parse(tf_str).map_err(|_| {
            BacktestServerError::InvalidRequest(format!("Invalid timeframe: '{tf_str}'"))
        })?;
        let opts = BarQueryOpts {
            exchange: exchange.into(),
            symbol: symbol.into(),
            timeframe: tf.as_str().to_string(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (bars, _total) = store.query_bars(&opts)?;
        if bars.is_empty() {
            return Err(BacktestServerError::NoDataFound {
                symbol: symbol.into(),
                exchange: exchange.into(),
                data_type: format!("bar({})", tf_str),
            });
        }
        let feed = bars_to_feed(bars);
        Ok(feed_to_events(feed))
    } else {
        Err(BacktestServerError::InvalidRequest(format!(
            "Invalid data_type: '{}'. Must be 'tick' or 'bar'.",
            data_type
        )))
    }
}

/// Drain a VecFeed into its underlying Vec<MarketEvent>.
fn feed_to_events(mut feed: VecFeed) -> Vec<qs_backtest::data_feed::MarketEvent> {
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
    if req.signals.is_empty() && req.raw_signals.is_empty() {
        return Err(BacktestServerError::InvalidRequest(
            "At least one signal is required.".into(),
        ));
    }
    Ok(())
}

// ── Parsing Helpers ─────────────────────────────────────────────────────────

/// Normalize a symbol name via the registry, falling back to passthrough.
fn normalize_symbol(registry: &SymbolRegistry, raw: &str) -> String {
    registry.normalize_or_passthrough(raw)
}

/// Parse a side string ("Buy" or "Sell") into the core enum.
fn parse_side(s: &str) -> Result<Side> {
    match s {
        "Buy" | "buy" | "BUY" | "Long" | "long" => Ok(Side::Buy),
        "Sell" | "sell" | "SELL" | "Short" | "short" => Ok(Side::Sell),
        other => Err(BacktestServerError::InvalidRequest(format!(
            "Invalid side: '{other}'. Must be 'Buy' or 'Sell'."
        ))),
    }
}

/// Parse an order type string into the core enum.
fn parse_order_type(s: &str) -> Result<OrderType> {
    match s {
        "Market" | "market" | "MARKET" => Ok(OrderType::Market),
        "Limit" | "limit" | "LIMIT" => Ok(OrderType::Limit),
        "Stop" | "stop" | "STOP" => Ok(OrderType::Stop),
        other => Err(BacktestServerError::InvalidRequest(format!(
            "Invalid order_type: '{other}'. Must be 'Market', 'Limit', or 'Stop'."
        ))),
    }
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
    fn parse_side_variants() {
        assert_eq!(parse_side("Buy").unwrap(), Side::Buy);
        assert_eq!(parse_side("sell").unwrap(), Side::Sell);
        assert_eq!(parse_side("Long").unwrap(), Side::Buy);
        assert_eq!(parse_side("Short").unwrap(), Side::Sell);
        assert!(parse_side("invalid").is_err());
    }

    #[test]
    fn parse_order_type_variants() {
        assert_eq!(parse_order_type("Market").unwrap(), OrderType::Market);
        assert_eq!(parse_order_type("limit").unwrap(), OrderType::Limit);
        assert_eq!(parse_order_type("STOP").unwrap(), OrderType::Stop);
        assert!(parse_order_type("foobar").is_err());
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
    fn validate_request_rejects_empty_signals() {
        let req = RunBacktestRequest {
            symbol: "eurusd".into(),
            exchange: "ctrader".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            signals: vec![],
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
            exchange: "ctrader".into(),
            data_type: "invalid".into(),
            timeframe: None,
            from: None,
            to: None,
            signals: vec![RawSignalEntryMsg {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
            }],
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
    fn validate_request_rejects_bar_without_timeframe() {
        let req = RunBacktestRequest {
            symbol: "eurusd".into(),
            exchange: "ctrader".into(),
            data_type: "bar".into(),
            timeframe: None,
            from: None,
            to: None,
            signals: vec![RawSignalEntryMsg {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
            }],
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
    fn validate_request_accepts_valid_tick() {
        let req = RunBacktestRequest {
            symbol: "eurusd".into(),
            exchange: "ctrader".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            signals: vec![RawSignalEntryMsg {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                size: 1.0,
                stoploss: None,
                targets: vec![],
                group: None,
            }],
            raw_signals: vec![],
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

    #[test]
    fn convert_signal_msg_basic() {
        let reg = SymbolRegistry::empty();
        let msg = RawSignalEntryMsg {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "EURUSD".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900, 1.0950],
            group: Some("g1".into()),
        };
        let raw = convert_signal_msg(&msg, "eurusd", &reg).unwrap();
        assert_eq!(raw.symbol, "eurusd");
        assert_eq!(raw.side, Side::Buy);
        assert_eq!(raw.order_type, OrderType::Market);
        assert!(raw.price.is_none());
        assert_eq!(raw.targets.len(), 2);
        assert_eq!(raw.group, Some("g1".into()));
    }

    #[test]
    fn convert_signal_msg_empty_symbol_uses_default() {
        let reg = SymbolRegistry::empty();
        let msg = RawSignalEntryMsg {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "".into(),
            side: "Sell".into(),
            order_type: "Limit".into(),
            price: Some(1.0900),
            size: 0.5,
            stoploss: None,
            targets: vec![],
            group: None,
        };
        let raw = convert_signal_msg(&msg, "eurusd", &reg).unwrap();
        assert_eq!(raw.symbol, "eurusd");
    }
}
