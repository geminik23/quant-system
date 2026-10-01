use chrono::{Duration, NaiveDateTime};
use std::collections::{BTreeMap, BTreeSet};

use qs_backtest::runner::BacktestConfig;
use qs_backtest::{FutureQuoteConfig, ReplayInstrumentManifest, resolve_legacy_economics};
use qs_core::{PositionRef, RawSignal};
use qs_market_loader::{
    MarketLoadError, MarketStreamDescription, describe_primary_market_stream_with_resolver,
};

use crate::convert::{
    account_currency_from_msg, config_from_msg, config_from_msg_with_manifest,
    future_config_from_msg, raw_signal_from_msg,
};
use crate::error::{BacktestServerError, Result};
use crate::fx_loader::{
    LoadedFutureStream, check_conversion_data, describe_future_stream_with_resolver,
};
use crate::handlers::{JobCancellationToken, ServerState, resolve_requested_symbol_scope};
use crate::replay_plan::{ReplayPlan, RequestedSymbolScope};
use crate::rpc_types::*;

const MAX_REFERENCES: usize = 64;
const MAX_TOTAL_REFERENCES: usize = 256;
const MAX_INSTRUMENTS: usize = 64;

fn bounded_text(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let mut result = String::new();
    result.extend(chars.by_ref().take(limit));
    if chars.next().is_some() {
        result.push_str("...");
    }
    result
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedReplay {
    pub plan: ReplayPlan,
    pub config: BacktestConfig,
    pub future: FutureQuoteConfig,
    pub bundle: LoadedFutureStream,
}

type Exclusions = BTreeMap<String, (InstrumentExclusionReasonMsg, String)>;

pub(crate) fn prepare_replay(
    state: &ServerState,
    req: &BacktestRunSpec,
    future: &FutureQuoteConfigMsg,
    cancellation: Option<&JobCancellationToken>,
) -> Result<PreparedReplay> {
    let mut cancelled = || cancellation.is_some_and(JobCancellationToken::is_cancelled);
    let from = req
        .from
        .as_deref()
        .map(crate::handlers::parse_datetime)
        .transpose()?;
    let to = req
        .to
        .as_deref()
        .map(crate::handlers::parse_datetime)
        .transpose()?;
    let scope = resolve_requested_symbol_scope(
        &state.symbol_registry,
        &req.symbol,
        &req.symbols,
        req.all_symbols,
    )?;
    config_from_msg(&req.config, &state.symbol_registry, &[])?;
    let account = account_currency_from_msg(future)?;
    let mut signals = Vec::new();
    for (index, message) in req.raw_signals.iter().enumerate() {
        ensure_running(&mut cancelled)?;
        let signal = raw_signal_from_msg(message, scope.default_symbol(), &state.symbol_registry)?;
        if from.is_none_or(|start| signal.ts() >= start) && to.is_none_or(|end| signal.ts() <= end)
        {
            signals.push((index, signal));
        }
    }
    let original_symbols = signals
        .iter()
        .filter_map(|(_, signal)| entry_symbol(signal))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if original_symbols.contains("") {
        return Err(BacktestServerError::InvalidRequest(
            "Entry signal symbol is required".into(),
        ));
    }
    let requested_scope = match &scope {
        RequestedSymbolScope::Explicit(_) => scope.clone(),
        RequestedSymbolScope::Inferred => {
            RequestedSymbolScope::explicit(original_symbols.iter().cloned())
        }
    };
    let mut excluded = BTreeMap::new();
    if let RequestedSymbolScope::Explicit(requested) = &scope {
        for symbol in &original_symbols {
            if !requested.contains(symbol) {
                excluded.insert(
                    symbol.clone(),
                    (
                        InstrumentExclusionReasonMsg::NotSelected,
                        "instrument is not selected by the request".into(),
                    ),
                );
            }
        }
    }
    let resolver = state
        .instrument_domain
        .symbol_resolver(&state.symbol_registry);
    let exchange = req.exchange.to_ascii_lowercase();
    let (mut plan, bundle, manifest) = loop {
        ensure_running(&mut cancelled)?;
        let retained = filter_signals(&signals, &excluded)?;
        let plan = ReplayPlan::build(
            requested_scope.clone(),
            retained.iter().map(|(_, signal)| signal.clone()).collect(),
            None,
            None,
            future.signal_latency_ms,
        )?;
        let mut descriptions = Vec::new();
        let mut added = BTreeMap::new();
        let mut inactive = BTreeMap::new();
        let at = plan.loading_start();
        for symbol in plan.active_symbols() {
            ensure_running(&mut cancelled)?;
            let start = at.expect("active Entries have a loading start");
            match state
                .instrument_domain
                .resolve_manifest(std::slice::from_ref(symbol), start, to)
            {
                Ok(manifest) => {
                    config_from_msg_with_manifest(&req.config, &state.symbol_registry, manifest)?;
                }
                Err(error @ BacktestServerError::InactiveInstrument { .. }) => {
                    let local_start =
                        instrument_loading_start(symbol, &retained, future.signal_latency_ms)
                            .expect("instrument Entry");
                    if local_start > start {
                        match state.instrument_domain.resolve_manifest(
                            std::slice::from_ref(symbol),
                            local_start,
                            to,
                        ) {
                            Ok(_) => {
                                inactive.insert(
                                    symbol.clone(),
                                    unavailable(&error).expect("inactive reason"),
                                );
                            }
                            Err(local_error) => {
                                if let Some(reason) = unavailable(&local_error) {
                                    added.insert(symbol.clone(), reason);
                                } else {
                                    return Err(local_error);
                                }
                            }
                        }
                    } else {
                        added.insert(
                            symbol.clone(),
                            unavailable(&error).expect("inactive reason"),
                        );
                    }
                    continue;
                }
                Err(error) => {
                    if let Some((mut reason, mut details)) = unavailable(&error) {
                        if reason == InstrumentExclusionReasonMsg::UnknownInstrument
                            && let Some(spec) = state.symbol_registry.spec(symbol)
                            && let Err(legacy_error) = resolve_legacy_economics(spec)
                        {
                            match legacy_error {
                                qs_backtest::economic_support::EconomicSupportError::UnsupportedCategory { .. } => {
                                    reason = InstrumentExclusionReasonMsg::UnsupportedEconomics;
                                    details = legacy_error.to_string();
                                }
                                _ => return Err(BacktestServerError::Config(legacy_error.to_string())),
                            }
                        }
                        added.insert(symbol.clone(), (reason, details));
                        continue;
                    }
                    return Err(error);
                }
            }
            let description = match describe_primary_market_stream_with_resolver(
                &resolver,
                &state.data_dir,
                &exchange,
                std::slice::from_ref(symbol),
                &req.data_type,
                req.timeframe.as_deref(),
                at,
                to,
                &mut cancelled,
                &mut |_| {},
            ) {
                Ok(description) => description,
                Err(error) => {
                    let error = BacktestServerError::MarketLoad(error);
                    if let Some(reason) = unavailable(&error) {
                        added.insert(symbol.clone(), reason);
                        continue;
                    }
                    return Err(error);
                }
            };
            descriptions.push(description);
        }
        if !added.is_empty() {
            excluded.extend(added);
            continue;
        }
        if !inactive.is_empty() {
            excluded.extend(inactive);
            continue;
        }
        let conversion_end = descriptions
            .iter()
            .filter_map(MarketStreamDescription::primary_eod)
            .max()
            .or(to);
        let mut deferred = BTreeMap::new();
        for symbol in plan.active_symbols() {
            let start = at.expect("active Entry has a loading start");
            if to.is_some_and(|end| start > end) {
                continue;
            }
            match check_conversion_data(
                &state.data_dir,
                &exchange,
                &state.symbol_registry,
                &resolver,
                &state.instrument_domain,
                &account,
                symbol,
                start,
                conversion_end,
                &mut cancelled,
            ) {
                Ok(()) => {}
                Err(
                    error @ (BacktestServerError::ConversionWarmupUnavailable { .. }
                    | BacktestServerError::InactiveInstrument { .. }),
                ) => {
                    let local_start =
                        instrument_loading_start(symbol, &retained, future.signal_latency_ms)
                            .expect("instrument Entry");
                    if local_start > start {
                        match check_conversion_data(
                            &state.data_dir,
                            &exchange,
                            &state.symbol_registry,
                            &resolver,
                            &state.instrument_domain,
                            &account,
                            symbol,
                            local_start,
                            conversion_end,
                            &mut cancelled,
                        ) {
                            Ok(()) => {
                                deferred.insert(
                                    symbol.clone(),
                                    (
                                        InstrumentExclusionReasonMsg::NoConversionData,
                                        error.to_string(),
                                    ),
                                );
                            }
                            Err(local_error) => {
                                if let Some((reason, details)) = unavailable(&local_error) {
                                    let reason = if matches!(
                                        local_error,
                                        BacktestServerError::InactiveInstrument { .. }
                                    ) {
                                        InstrumentExclusionReasonMsg::NoConversionData
                                    } else {
                                        reason
                                    };
                                    added.insert(symbol.clone(), (reason, details));
                                } else {
                                    return Err(local_error);
                                }
                            }
                        }
                    } else {
                        added.insert(
                            symbol.clone(),
                            (
                                InstrumentExclusionReasonMsg::NoConversionData,
                                error.to_string(),
                            ),
                        );
                    }
                }
                Err(error) => {
                    if let Some(reason) = unavailable(&error) {
                        added.insert(symbol.clone(), reason);
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        if !added.is_empty() {
            excluded.extend(added);
            continue;
        }
        if !deferred.is_empty() {
            excluded.extend(deferred);
            continue;
        }
        let primary = MarketStreamDescription::merge_primary(descriptions, to)?;
        let bundle = describe_future_stream_with_resolver(
            &state.data_dir,
            &exchange,
            &state.symbol_registry,
            &resolver,
            &account,
            plan.active_symbols(),
            &req.data_type,
            at,
            primary,
            &mut cancelled,
        )?;
        let mut instrument_symbols = plan.active_symbols().to_vec();
        instrument_symbols.extend(bundle.currency_plan.conversion_symbols().iter().cloned());
        instrument_symbols.sort();
        instrument_symbols.dedup();
        let mut manifest = match at {
            Some(start) => state.instrument_domain.resolve_manifest(
                &instrument_symbols,
                start,
                bundle.description.primary_eod().or(to),
            )?,
            None => ReplayInstrumentManifest {
                instruments: BTreeMap::new(),
                stored_series: Vec::new(),
            },
        };
        state.instrument_domain.attach_stored_series(
            &mut manifest,
            bundle.description.stored_series_coordinates(),
        )?;
        bundle
            .description
            .validate_stored_series_bindings(&manifest)?;
        break (plan, bundle, manifest);
    };
    let retained = filter_signals(&signals, &excluded)?;
    let report = build_report(&signals, &retained, &excluded, req.on_unavailable);
    if req.on_unavailable == UnavailableInstrumentPolicyMsg::Error
        && excluded
            .values()
            .any(|(reason, _)| *reason != InstrumentExclusionReasonMsg::NotSelected)
    {
        return Err(BacktestServerError::AdmissionRejected(
            serde_json::to_string(&report)
                .map_err(|error| BacktestServerError::Serde(error.to_string()))?,
        ));
    }
    plan.set_admission_report(report, &excluded.keys().cloned().collect());
    let config = config_from_msg_with_manifest(&req.config, &state.symbol_registry, manifest)?;
    let future = future_config_from_msg(future, bundle.currency_plan.clone())?;
    let mut bundle = bundle;
    let points = config
        .instrument_manifest
        .as_ref()
        .expect("prepared manifest")
        .instruments
        .iter()
        .map(|(symbol, artifact)| {
            (
                symbol.clone(),
                10f64.powi(-i32::from(artifact.spec.price.display_scale)),
            )
        })
        .collect();
    bundle.description.apply_bar_point_sizes(&points);
    Ok(PreparedReplay {
        plan,
        config,
        future,
        bundle,
    })
}

fn ensure_running(cancelled: &mut dyn FnMut() -> bool) -> Result<()> {
    if cancelled() {
        Err(BacktestServerError::Cancelled)
    } else {
        Ok(())
    }
}

fn unavailable(error: &BacktestServerError) -> Option<(InstrumentExclusionReasonMsg, String)> {
    match error {
        BacktestServerError::InstrumentUnavailable {
            reason, details, ..
        } => Some((*reason, details.clone())),
        BacktestServerError::InactiveInstrument { .. } => Some((
            InstrumentExclusionReasonMsg::UnknownInstrument,
            error.to_string(),
        )),
        BacktestServerError::ConversionWarmupUnavailable { .. } => Some((
            InstrumentExclusionReasonMsg::NoConversionData,
            error.to_string(),
        )),
        BacktestServerError::MarketLoad(MarketLoadError::NoDataFound { .. }) => Some((
            InstrumentExclusionReasonMsg::NoMarketData,
            error.to_string(),
        )),
        BacktestServerError::MarketLoad(MarketLoadError::AmbiguousSymbol { .. }) => Some((
            InstrumentExclusionReasonMsg::AmbiguousMapping,
            error.to_string(),
        )),
        _ => None,
    }
}

fn instrument_loading_start(
    symbol: &str,
    signals: &[(usize, RawSignal)],
    latency_ms: i64,
) -> Option<NaiveDateTime> {
    let ids = signals
        .iter()
        .filter_map(|(_, signal)| match signal {
            RawSignal::Entry {
                symbol: entry_symbol,
                trade_id: Some(id),
                ..
            } if entry_symbol == symbol => Some(id.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let all_ids = signals
        .iter()
        .filter_map(|(_, signal)| match signal {
            RawSignal::Entry {
                trade_id: Some(id), ..
            } => Some(id.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    signals
        .iter()
        .filter(|(_, signal)| match signal {
            RawSignal::Entry { symbol: target, .. }
            | RawSignal::CloseAllOf { symbol: target, .. }
            | RawSignal::ModifyAllStoploss { symbol: target, .. } => target == symbol,
            _ => match reference(signal) {
                Some(PositionRef::ByTradeId { trade_id }) => {
                    ids.contains(trade_id.as_str()) || !all_ids.contains(trade_id.as_str())
                }
                Some(PositionRef::AllOnSymbol { symbol: target }) => target == symbol,
                _ => true,
            },
        })
        .filter_map(|(_, signal)| {
            signal
                .ts()
                .checked_add_signed(Duration::milliseconds(latency_ms))
        })
        .min()
}

fn entry_symbol(signal: &RawSignal) -> Option<&str> {
    match signal {
        RawSignal::Entry { symbol, .. } => Some(symbol),
        _ => None,
    }
}

fn reference(signal: &RawSignal) -> Option<&PositionRef> {
    match signal {
        RawSignal::Close { position, .. }
        | RawSignal::ClosePartial { position, .. }
        | RawSignal::ModifyStoploss { position, .. }
        | RawSignal::MoveStoplossToEntry { position, .. }
        | RawSignal::AddTarget { position, .. }
        | RawSignal::RemoveTarget { position, .. }
        | RawSignal::ModifyTarget { position, .. }
        | RawSignal::AddRule { position, .. }
        | RawSignal::RemoveRule { position, .. }
        | RawSignal::ScaleIn { position, .. }
        | RawSignal::CancelPending { position, .. } => Some(position),
        _ => None,
    }
}

fn excluded_owner<'a>(
    signal: &'a RawSignal,
    excluded: &Exclusions,
    trades: &'a BTreeMap<String, String>,
) -> Option<&'a str> {
    match signal {
        RawSignal::Entry { symbol, .. }
        | RawSignal::CloseAllOf { symbol, .. }
        | RawSignal::ModifyAllStoploss { symbol, .. } => {
            excluded.contains_key(symbol).then_some(symbol.as_str())
        }
        _ => match reference(signal) {
            Some(PositionRef::ByTradeId { trade_id }) => trades.get(trade_id).map(String::as_str),
            Some(PositionRef::AllOnSymbol { symbol }) => {
                excluded.contains_key(symbol).then_some(symbol.as_str())
            }
            _ => None,
        },
    }
}

fn excluded_trades(
    signals: &[(usize, RawSignal)],
    excluded: &Exclusions,
) -> Result<BTreeMap<String, String>> {
    let mut skipped = BTreeMap::new();
    let mut retained = BTreeSet::new();
    for (_, signal) in signals {
        if let RawSignal::Entry {
            symbol,
            trade_id: Some(id),
            ..
        } = signal
        {
            if excluded.contains_key(symbol) {
                if skipped
                    .insert(id.clone(), symbol.clone())
                    .is_some_and(|previous| previous != *symbol)
                {
                    return Err(BacktestServerError::InvalidRequest(format!(
                        "ambiguous excluded trade_id '{id}'"
                    )));
                }
            } else {
                retained.insert(id.clone());
            }
        }
    }
    if let Some(id) = skipped.keys().find(|id| retained.contains(*id)) {
        return Err(BacktestServerError::InvalidRequest(format!(
            "trade_id '{id}' occurs in retained and excluded Entries"
        )));
    }
    Ok(skipped)
}

fn filter_signals(
    signals: &[(usize, RawSignal)],
    excluded: &Exclusions,
) -> Result<Vec<(usize, RawSignal)>> {
    let trades = excluded_trades(signals, excluded)?;
    Ok(signals
        .iter()
        .filter(|(_, signal)| excluded_owner(signal, excluded, &trades).is_none())
        .cloned()
        .collect())
}

fn build_report(
    signals: &[(usize, RawSignal)],
    retained: &[(usize, RawSignal)],
    excluded: &Exclusions,
    policy: UnavailableInstrumentPolicyMsg,
) -> ReplayAdmissionReportMsg {
    let trades = excluded_trades(signals, excluded).expect("identity checked during filtering");
    let mut reports = excluded
        .iter()
        .take(MAX_INSTRUMENTS)
        .map(|(symbol, (reason, details))| {
            (
                symbol.clone(),
                ExcludedInstrumentMsg {
                    symbol: bounded_text(symbol, 128),
                    reason: *reason,
                    details: bounded_text(details, 512),
                    skipped_entries: 0,
                    skipped_management: 0,
                    signal_references: Vec::new(),
                    omitted_signal_references: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut omitted_entries = 0;
    let mut omitted_management = 0;
    let mut total_references = 0;
    for (index, signal) in signals {
        if let Some(owner) = excluded_owner(signal, excluded, &trades) {
            let Some(report) = reports.get_mut(owner) else {
                if entry_symbol(signal).is_some() {
                    omitted_entries += 1;
                } else {
                    omitted_management += 1;
                }
                continue;
            };
            if entry_symbol(signal).is_some() {
                report.skipped_entries += 1;
            } else {
                report.skipped_management += 1;
            }
            if report.signal_references.len() == MAX_REFERENCES
                || total_references == MAX_TOTAL_REFERENCES
            {
                report.omitted_signal_references += 1;
                continue;
            }
            total_references += 1;
            let encoded = serde_json::to_value(signal).expect("validated finite RawSignal");
            let trade_id = match signal {
                RawSignal::Entry { trade_id, .. } => trade_id.clone(),
                _ => match reference(signal) {
                    Some(PositionRef::ByTradeId { trade_id }) => Some(trade_id.clone()),
                    _ => None,
                },
            };
            report.signal_references.push(ExcludedSignalRefMsg {
                input_index: *index,
                ts: signal.ts().format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
                action: encoded["action"].as_str().expect("tagged signal").into(),
                trade_id: trade_id.filter(|id| id.len() <= 128),
            });
        }
    }
    ReplayAdmissionReportMsg {
        omitted_instruments: excluded.len().saturating_sub(reports.len()),
        omitted_entries,
        omitted_management,
        policy,
        input_signals: signals.len(),
        retained_signals: retained.len(),
        excluded_instruments: reports.into_values().collect(),
    }
}
