//! Conversions between internal backtest types and wire-safe RPC messages.

use std::collections::BTreeSet;

use chrono::NaiveDateTime;
use qs_backtest::artifacts::{
    FUTURE_ARTIFACT_FORMAT_VERSION, PendingOrderLifecycleEvent, PendingOrderLifecycleState,
};
use qs_backtest::currency::RunCurrencyPlan;
use qs_backtest::economic_support::resolve_legacy_economics;
use qs_backtest::evaluation::{
    BootstrapConfig, BreakdownDimension, EvaluationContext, EvaluationOptions, EvaluationSection,
    GroupFilter, PositionFilter, PositionSide, SourceCoverageCounts,
};
use qs_backtest::profile::{
    ManagementProfile, PositionRef, RawSignal, RuleConfigDef, StoplossMode, TargetSelection,
};
use qs_backtest::report::{
    BacktestResult, CloseReasonStats, DurationStats, MonthlyReturn, PositionSummary, RiskMetrics,
    StreakStats, SubsetStats, TradeResult,
};
use qs_backtest::runner::{BacktestConfig, FutureQuoteConfig};
use qs_backtest::{MtmOutputPolicy, MtmOutputSummary};
use qs_core::types::{FillModel, OrderType, Side};
use qs_symbols::{SymbolRegistry, normalize_currency_code};

use crate::error::BacktestServerError;
use crate::rpc_types::{
    BacktestConfigMsg, BacktestResultMsg, BreakdownDimensionMsg, CloseReasonStatsMsg,
    DurationStatsMsg, EquityPoint, EvaluationGroupFilterMsg, EvaluationPositionSideMsg,
    EvaluationSectionMsg, FutureBacktestResultMsg, FutureQuoteConfigMsg, ManagementProfileMsg,
    MonthlyReturnMsg, MtmOutputPolicyMsg, MtmOutputSummaryMsg, PendingOrderLifecycleEventMsg,
    PendingOrderLifecycleStateMsg, PositionRefMsg, PositionSummaryMsg,
    ProviderEvaluationOptionsMsg, RawSignalMsg, RiskMetricsMsg, RuleConfigDefMsg, SizingPolicyMsg,
    StoplossModeMsg, StreakStatsMsg, SubsetStatsMsg, TargetSelectionMsg, TradeResultMsg,
    parse_backtest_timestamp,
};

// ── Timestamp formatting ────────────────────────────────────────────────────

const TS_FMT: &str = "%Y-%m-%dT%H:%M:%S%.f";

fn ndt_to_string(ts: NaiveDateTime) -> String {
    ts.format(TS_FMT).to_string()
}

// ── BacktestConfigMsg -> BacktestConfig ──────────────────────────────────────

/// Convert the wire config message into the internal `BacktestConfig`.
///
/// `registry` and `symbols` are used to populate per-symbol contract sizes
/// and symbol specs from the symbol registry metadata.
pub fn config_from_msg(
    msg: &BacktestConfigMsg,
    registry: &SymbolRegistry,
    symbols: &[String],
) -> crate::error::Result<BacktestConfig> {
    let initial_balance = msg.initial_balance.unwrap_or(10_000.0);
    if !initial_balance.is_finite() || initial_balance <= 0.0 {
        return Err(BacktestServerError::InvalidRequest(format!(
            "initial balance must be finite and positive, got {initial_balance}"
        )));
    }

    let mut contract_sizes = std::collections::HashMap::new();
    let mut symbol_specs = std::collections::HashMap::new();
    for symbol in symbols {
        let spec = registry
            .spec(symbol)
            .ok_or_else(|| BacktestServerError::SymbolNotFound(symbol.clone()))?;
        let economics = resolve_legacy_economics(spec)
            .map_err(|error| BacktestServerError::InvalidRequest(error.to_string()))?;
        contract_sizes.insert(symbol.clone(), economics.contract_multiplier);
        symbol_specs.insert(symbol.clone(), spec.clone());
    }
    let sizing = msg.sizing.as_ref().map(sizing_from_msg).transpose()?;
    Ok(BacktestConfig {
        initial_balance,
        close_on_finish: msg.close_on_finish.unwrap_or(true),
        fill_model: parse_fill_model(msg.fill_model.as_deref()),
        contract_sizes,
        sizing,
        symbol_specs,
        instrument_manifest: None,
    })
}

pub fn account_currency_from_msg(msg: &FutureQuoteConfigMsg) -> crate::error::Result<String> {
    normalize_currency_code(&msg.account_currency).ok_or_else(|| {
        BacktestServerError::InvalidRequest(format!(
            "account_currency must be 3 ASCII letters, got '{}'",
            msg.account_currency
        ))
    })
}

fn mtm_output_policy_from_msg(msg: &MtmOutputPolicyMsg) -> crate::error::Result<MtmOutputPolicy> {
    let policy = match *msg {
        MtmOutputPolicyMsg::None => MtmOutputPolicy::None,
        MtmOutputPolicyMsg::Bounded { max_points } => MtmOutputPolicy::Bounded { max_points },
        MtmOutputPolicyMsg::Full => MtmOutputPolicy::Full,
    };
    policy.validate().map_err(|error| {
        BacktestServerError::InvalidRequest(format!("invalid mtm_output: {error}"))
    })?;
    Ok(policy)
}

fn mtm_output_policy_to_msg(policy: MtmOutputPolicy) -> MtmOutputPolicyMsg {
    match policy {
        MtmOutputPolicy::None => MtmOutputPolicyMsg::None,
        MtmOutputPolicy::Bounded { max_points } => MtmOutputPolicyMsg::Bounded { max_points },
        MtmOutputPolicy::Full => MtmOutputPolicyMsg::Full,
    }
}

fn mtm_output_summary_to_msg(summary: &MtmOutputSummary) -> MtmOutputSummaryMsg {
    MtmOutputSummaryMsg {
        policy: mtm_output_policy_to_msg(summary.policy),
        observed_points: summary.observed_points,
        retained_points: summary.retained_points,
        omitted_points: summary.omitted_points,
    }
}

/// Validate FutureQuote scalar settings without requiring a replay or currency plan.
pub fn validate_future_quote_scalars(msg: &FutureQuoteConfigMsg) -> crate::error::Result<()> {
    account_currency_from_msg(msg)?;
    if msg.signal_latency_ms < 0 {
        return Err(BacktestServerError::InvalidRequest(format!(
            "signal_latency_ms must be non-negative, got {}",
            msg.signal_latency_ms
        )));
    }
    if !msg.slippage_pips.is_finite() {
        return Err(BacktestServerError::InvalidRequest(format!(
            "slippage_pips must be finite, got {}",
            msg.slippage_pips
        )));
    }
    if msg.stale_quote_after_ms.is_some_and(|value| value < 0) {
        return Err(BacktestServerError::InvalidRequest(
            "stale_quote_after_ms must be non-negative".into(),
        ));
    }
    if !msg.pnl_epsilon.is_finite() || msg.pnl_epsilon < 0.0 {
        return Err(BacktestServerError::InvalidRequest(format!(
            "pnl_epsilon must be finite and non-negative, got {}",
            msg.pnl_epsilon
        )));
    }
    if msg.conversion_stale_after_ms < 0 {
        return Err(BacktestServerError::InvalidRequest(format!(
            "conversion_stale_after_ms must be non-negative, got {}",
            msg.conversion_stale_after_ms
        )));
    }
    mtm_output_policy_from_msg(&msg.mtm_output)?;
    Ok(())
}

/// Convert and validate FutureQuote settings with the server-derived currency plan.
pub fn future_config_from_msg(
    msg: &FutureQuoteConfigMsg,
    currency_plan: RunCurrencyPlan,
) -> crate::error::Result<FutureQuoteConfig> {
    validate_future_quote_scalars(msg)?;
    let account_currency = account_currency_from_msg(msg)?;
    if account_currency != currency_plan.account_currency() {
        return Err(BacktestServerError::InvalidRequest(format!(
            "account_currency {account_currency} does not match currency plan {}",
            currency_plan.account_currency()
        )));
    }
    let mtm_output = mtm_output_policy_from_msg(&msg.mtm_output)?;

    Ok(FutureQuoteConfig {
        signal_latency_ms: msg.signal_latency_ms,
        slippage_pips: msg.slippage_pips,
        stale_quote_after_ms: msg.stale_quote_after_ms,
        pnl_epsilon: msg.pnl_epsilon,
        currency_plan: Some(currency_plan),
        conversion_stale_after_ms: msg.conversion_stale_after_ms,
        mtm_output,
    })
}

/// Convert and validate the strict provider-evaluation configuration.
pub fn evaluation_options_from_msg(
    msg: &ProviderEvaluationOptionsMsg,
    registry: &SymbolRegistry,
) -> crate::error::Result<EvaluationOptions> {
    evaluation_options_from_msg_for_symbols(msg, registry, &[])
}

/// Convert evaluation options after request symbols have been resolved.
///
/// Registry-known filters are always accepted. A registry-unknown passthrough
/// symbol is accepted only when it names one of the resolved request symbols.
pub fn evaluation_options_from_msg_for_symbols(
    msg: &ProviderEvaluationOptionsMsg,
    registry: &SymbolRegistry,
    request_symbols: &[String],
) -> crate::error::Result<EvaluationOptions> {
    let invalid = |message: String| BacktestServerError::InvalidRequest(message);
    for (name, value) in [
        ("provider_id", msg.context.provider_id.as_deref()),
        ("source_id", msg.context.source_id.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty()) {
            return Err(invalid(format!("evaluation {name} must not be empty")));
        }
    }
    if msg.bootstrap.samples == 0 {
        return Err(invalid(
            "evaluation bootstrap.samples must be positive".into(),
        ));
    }
    if !msg.bootstrap.confidence_level.is_finite()
        || !(0.0..1.0).contains(&msg.bootstrap.confidence_level)
        || msg.bootstrap.confidence_level == 0.0
    {
        return Err(invalid(
            "evaluation bootstrap.confidence_level must be finite and between 0 and 1".into(),
        ));
    }
    if msg.bootstrap.minimum_sample_size == 0 {
        return Err(invalid(
            "evaluation bootstrap.minimum_sample_size must be positive".into(),
        ));
    }
    if msg.rolling_window == 0 {
        return Err(invalid("evaluation rolling_window must be positive".into()));
    }
    if msg.minimum_breakdown_bucket_count == 0 {
        return Err(invalid(
            "evaluation minimum_breakdown_bucket_count must be positive".into(),
        ));
    }
    if msg.maximum_position_rows.is_some() && !msg.include_positions {
        return Err(invalid(
            "evaluation maximum_position_rows requires include_positions=true".into(),
        ));
    }
    if !msg.filter.tags.is_empty() {
        return Err(invalid(
            "unsupported evaluation selector: tag filters are not supported by integrated backtests because completed positions have no tags".into(),
        ));
    }
    if msg
        .breakdowns
        .iter()
        .any(|dimension| matches!(dimension, BreakdownDimensionMsg::Tag(_)))
    {
        return Err(invalid(
            "unsupported evaluation selector: tag breakdowns are not supported by integrated backtests because completed positions have no tags".into(),
        ));
    }

    let source_coverage = msg.source_coverage.map(|coverage| SourceCoverageCounts {
        raw_messages: coverage.raw_messages,
        parsed_messages: coverage.parsed_messages,
        skipped_messages: coverage.skipped_messages,
        failed_messages: coverage.failed_messages,
        emitted_signals: coverage.emitted_signals,
        emitted_entry_signals: coverage.emitted_entry_signals,
    });
    if let Some(error) = source_coverage.and_then(SourceCoverageCounts::validation_error) {
        return Err(invalid(format!(
            "invalid evaluation source_coverage: {error}"
        )));
    }

    let symbols = msg
        .filter
        .symbols
        .iter()
        .map(|symbol| normalize_evaluation_symbol(registry, request_symbols, symbol))
        .collect::<crate::error::Result<Vec<_>>>()?;
    let sections: BTreeSet<_> = msg.sections.iter().copied().map(section_from_msg).collect();
    if !msg.breakdowns.is_empty() && !sections.contains(&EvaluationSection::Breakdowns) {
        return Err(invalid(
            "evaluation breakdowns require the breakdowns report section".into(),
        ));
    }

    Ok(EvaluationOptions {
        context: EvaluationContext {
            provider_id: msg.context.provider_id.clone(),
            source_id: msg.context.source_id.clone(),
        },
        source_coverage,
        sections,
        filter: PositionFilter {
            symbols,
            sides: msg
                .filter
                .sides
                .iter()
                .copied()
                .map(position_side_from_msg)
                .collect(),
            groups: msg
                .filter
                .groups
                .iter()
                .cloned()
                .map(group_filter_from_msg)
                .collect(),
            close_reasons: msg.filter.close_reasons.clone(),
            tags: msg.filter.tags.clone(),
        },
        breakdowns: msg
            .breakdowns
            .iter()
            .cloned()
            .map(breakdown_from_msg)
            .collect(),
        bootstrap: BootstrapConfig {
            samples: msg.bootstrap.samples,
            confidence_level: msg.bootstrap.confidence_level,
            seed: msg.bootstrap.seed,
            minimum_sample_size: msg.bootstrap.minimum_sample_size,
        },
        rolling_window: msg.rolling_window,
        minimum_breakdown_bucket_count: msg.minimum_breakdown_bucket_count,
        maximum_breakdown_rows: msg.maximum_breakdown_rows,
        include_position_rows: msg.include_positions,
        maximum_position_rows: msg.maximum_position_rows,
    })
}

fn normalize_evaluation_symbol(
    registry: &SymbolRegistry,
    request_symbols: &[String],
    raw: &str,
) -> crate::error::Result<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(BacktestServerError::InvalidRequest(
            "evaluation symbol filters must not be empty".into(),
        ));
    }
    let normalized = registry.normalize_or_passthrough(raw);
    if registry.is_known(raw)
        || request_symbols
            .iter()
            .any(|request_symbol| request_symbol == &normalized)
    {
        return Ok(normalized);
    }

    let suggestions = registry.suggest(raw, 3, 3);
    let suggestion = if suggestions.is_empty() {
        String::new()
    } else {
        format!(
            "; did you mean {}?",
            suggestions
                .iter()
                .map(|(symbol, _)| format!("`{symbol}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    Err(BacktestServerError::InvalidRequest(format!(
        "unknown evaluation symbol `{raw}`{suggestion}"
    )))
}

fn section_from_msg(section: EvaluationSectionMsg) -> EvaluationSection {
    match section {
        EvaluationSectionMsg::Coverage => EvaluationSection::Coverage,
        EvaluationSectionMsg::PositionPerformance => EvaluationSection::PositionPerformance,
        EvaluationSectionMsg::RMetrics => EvaluationSection::RMetrics,
        EvaluationSectionMsg::Excursions => EvaluationSection::Excursions,
        EvaluationSectionMsg::Execution => EvaluationSection::Execution,
        EvaluationSectionMsg::Robustness => EvaluationSection::Robustness,
        EvaluationSectionMsg::Breakdowns => EvaluationSection::Breakdowns,
    }
}

fn position_side_from_msg(side: EvaluationPositionSideMsg) -> PositionSide {
    match side {
        EvaluationPositionSideMsg::Long => PositionSide::Long,
        EvaluationPositionSideMsg::Short => PositionSide::Short,
    }
}

fn group_filter_from_msg(group: EvaluationGroupFilterMsg) -> GroupFilter {
    match group {
        EvaluationGroupFilterMsg::Named(name) => GroupFilter::Named(name),
        EvaluationGroupFilterMsg::Ungrouped => GroupFilter::Ungrouped,
    }
}

fn breakdown_from_msg(dimension: BreakdownDimensionMsg) -> BreakdownDimension {
    match dimension {
        BreakdownDimensionMsg::Symbol => BreakdownDimension::Symbol,
        BreakdownDimensionMsg::Side => BreakdownDimension::Side,
        BreakdownDimensionMsg::Group => BreakdownDimension::Group,
        BreakdownDimensionMsg::CloseReason => BreakdownDimension::CloseReason,
        BreakdownDimensionMsg::Tag(key) => BreakdownDimension::Tag(key),
    }
}

pub fn sizing_from_msg(
    msg: &SizingPolicyMsg,
) -> crate::error::Result<qs_backtest::sizing::SizingPolicy> {
    use qs_backtest::sizing::SizingPolicy;
    let (name, value, policy) = match msg {
        SizingPolicyMsg::FixedLot { lots } => {
            ("fixed lots", *lots, SizingPolicy::FixedLot { lots: *lots })
        }
        SizingPolicyMsg::FixedRiskAmount { amount } => (
            "fixed risk amount",
            *amount,
            SizingPolicy::FixedRiskAmount { amount: *amount },
        ),
        SizingPolicyMsg::BalanceRiskPercent { percent } => (
            "balance risk percent",
            *percent,
            SizingPolicy::BalanceRiskPercent { percent: *percent },
        ),
    };
    if !value.is_finite() || value <= 0.0 {
        return Err(BacktestServerError::InvalidRequest(format!(
            "{name} must be finite and positive, got {value}"
        )));
    }
    Ok(policy)
}

/// Parse a fill model string, defaulting to BidAsk for unknown values.
pub fn parse_fill_model(s: Option<&str>) -> FillModel {
    match s {
        Some("AskOnly") => FillModel::AskOnly,
        Some("MidPrice") => FillModel::MidPrice,
        _ => FillModel::BidAsk,
    }
}

// Management profile conversions.

fn target_selection_from_msg(msg: &TargetSelectionMsg) -> TargetSelection {
    match msg {
        TargetSelectionMsg::All => TargetSelection::All,
        TargetSelectionMsg::None => TargetSelection::None,
        TargetSelectionMsg::Selected(indices) => TargetSelection::Selected(indices.clone()),
    }
}

fn target_selection_to_msg(selection: &TargetSelection) -> TargetSelectionMsg {
    match selection {
        TargetSelection::All => TargetSelectionMsg::All,
        TargetSelection::None => TargetSelectionMsg::None,
        TargetSelection::Selected(indices) => TargetSelectionMsg::Selected(indices.clone()),
    }
}

/// Convert a wire-format `ManagementProfileMsg` into the internal `ManagementProfile`.
///
/// An explicit `target_selection` is preserved and takes precedence during strict
/// application. Omission remains `None`, allowing the internal profile to derive
/// its current selection from compatibility `use_targets` only for older payloads.
pub fn profile_from_msg(msg: &ManagementProfileMsg) -> crate::error::Result<ManagementProfile> {
    let stoploss_mode = match &msg.stoploss_mode {
        Some(StoplossModeMsg::FromSignal) | None => StoplossMode::FromSignal,
        Some(StoplossModeMsg::None) => StoplossMode::None,
        Some(StoplossModeMsg::FixedDistance { distance }) => StoplossMode::FixedDistance {
            distance: *distance,
        },
        Some(StoplossModeMsg::FixedPrice { price }) => StoplossMode::FixedPrice { price: *price },
    };

    let rules: Vec<RuleConfigDef> = msg
        .rules
        .iter()
        .map(|r| match r {
            RuleConfigDefMsg::FixedStoploss { price } => {
                RuleConfigDef::FixedStoploss { price: *price }
            }
            RuleConfigDefMsg::TrailingStop { distance } => RuleConfigDef::TrailingStop {
                distance: *distance,
            },
            RuleConfigDefMsg::TakeProfit { price, close_ratio } => RuleConfigDef::TakeProfit {
                price: *price,
                close_ratio: *close_ratio,
            },
            RuleConfigDefMsg::BreakevenWhen { trigger_price } => RuleConfigDef::BreakevenWhen {
                trigger_price: *trigger_price,
            },
            RuleConfigDefMsg::BreakevenWhenOffset {
                trigger_price_offset,
            } => RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset: *trigger_price_offset,
            },
            RuleConfigDefMsg::BreakevenAfterTargets { after_n } => {
                RuleConfigDef::BreakevenAfterTargets { after_n: *after_n }
            }
            RuleConfigDefMsg::TimeExit { max_seconds } => RuleConfigDef::TimeExit {
                max_seconds: *max_seconds,
            },
        })
        .collect();

    Ok(ManagementProfile {
        name: msg.name.clone(),
        target_selection: msg.target_selection.as_ref().map(target_selection_from_msg),
        use_targets: msg.use_targets.clone(),
        close_ratios: msg.close_ratios.clone(),
        stoploss_mode,
        rules,
        group_override: msg.group_override.clone(),
        let_remainder_run: msg.let_remainder_run,
    })
}

/// Convert an internal `ManagementProfile` into a wire-format `ManagementProfileMsg`.
pub fn profile_to_msg(p: &ManagementProfile) -> ManagementProfileMsg {
    let stoploss_mode = Some(match &p.stoploss_mode {
        StoplossMode::FromSignal => StoplossModeMsg::FromSignal,
        StoplossMode::None => StoplossModeMsg::None,
        StoplossMode::FixedDistance { distance } => StoplossModeMsg::FixedDistance {
            distance: *distance,
        },
        StoplossMode::FixedPrice { price } => StoplossModeMsg::FixedPrice { price: *price },
    });

    let rules = p
        .rules
        .iter()
        .map(|r| match r {
            RuleConfigDef::FixedStoploss { price } => {
                RuleConfigDefMsg::FixedStoploss { price: *price }
            }
            RuleConfigDef::TrailingStop { distance } => RuleConfigDefMsg::TrailingStop {
                distance: *distance,
            },
            RuleConfigDef::TakeProfit { price, close_ratio } => RuleConfigDefMsg::TakeProfit {
                price: *price,
                close_ratio: *close_ratio,
            },
            RuleConfigDef::BreakevenWhen { trigger_price } => RuleConfigDefMsg::BreakevenWhen {
                trigger_price: *trigger_price,
            },
            RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset,
            } => RuleConfigDefMsg::BreakevenWhenOffset {
                trigger_price_offset: *trigger_price_offset,
            },
            RuleConfigDef::BreakevenAfterTargets { after_n } => {
                RuleConfigDefMsg::BreakevenAfterTargets { after_n: *after_n }
            }
            RuleConfigDef::TimeExit { max_seconds } => RuleConfigDefMsg::TimeExit {
                max_seconds: *max_seconds,
            },
        })
        .collect();

    ManagementProfileMsg {
        name: p.name.clone(),
        target_selection: p.target_selection.as_ref().map(target_selection_to_msg),
        use_targets: p.use_targets.clone(),
        close_ratios: p.close_ratios.clone(),
        stoploss_mode,
        rules,
        group_override: p.group_override.clone(),
        let_remainder_run: p.let_remainder_run,
    }
}

// ── BacktestResult -> BacktestResultMsg ──────────────────────────────────────

/// Convert the full backtest result into its wire-safe message form.
pub fn result_to_msg(r: &BacktestResult) -> BacktestResultMsg {
    BacktestResultMsg {
        initial_balance: r.initial_balance,
        final_balance: r.final_balance,
        total_pnl: r.total_pnl,
        total_trades: r.total_trades,
        winning_trades: r.winning_trades,
        losing_trades: r.losing_trades,
        win_rate: r.win_rate,
        profit_factor: sanitize_f64(r.profit_factor),
        max_drawdown: r.max_drawdown,
        max_drawdown_pct: r.max_drawdown_pct,
        summary: subset_stats_to_msg(&r.summary),
        per_symbol: r
            .per_symbol
            .iter()
            .map(|(k, v)| (k.clone(), subset_stats_to_msg(v)))
            .collect(),
        per_group: r
            .per_group
            .iter()
            .map(|(k, v)| (k.clone(), subset_stats_to_msg(v)))
            .collect(),
        long_stats: subset_stats_to_msg(&r.long_stats),
        short_stats: subset_stats_to_msg(&r.short_stats),
        per_close_reason: r
            .per_close_reason
            .iter()
            .map(close_reason_stats_to_msg)
            .collect(),
        streaks: streak_stats_to_msg(&r.streaks),
        risk_metrics: risk_metrics_to_msg(&r.risk_metrics),
        duration_stats: r.duration_stats.as_ref().map(duration_stats_to_msg),
        monthly_returns: r
            .monthly_returns
            .iter()
            .map(monthly_return_to_msg)
            .collect(),
        equity_curve: r
            .equity_curve
            .iter()
            .map(|(ts, bal)| EquityPoint {
                ts: ndt_to_string(*ts),
                balance: *bal,
            })
            .collect(),
        trade_log: r.trade_log.iter().map(trade_result_to_msg).collect(),
        positions: r.positions.iter().map(position_summary_to_msg).collect(),
        total_positions: r.total_positions,
        winning_positions: r.winning_positions,
        losing_positions: r.losing_positions,
        position_win_rate: r.position_win_rate,
        future: r
            .execution_metadata
            .as_ref()
            .map(|metadata| FutureBacktestResultMsg {
                format_version: r
                    .future_format_version
                    .unwrap_or(FUTURE_ARTIFACT_FORMAT_VERSION),
                execution_metadata: serde_json::to_value(metadata)
                    .unwrap_or(serde_json::Value::Null),
                recorded_fills: serde_json::to_value(&r.recorded_fills)
                    .unwrap_or(serde_json::Value::Null),
                action_dispositions: serde_json::to_value(&r.action_dispositions)
                    .unwrap_or(serde_json::Value::Null),
                close_events: serde_json::to_value(&r.close_events)
                    .unwrap_or(serde_json::Value::Null),
                completed_positions: serde_json::to_value(&r.completed_positions)
                    .unwrap_or(serde_json::Value::Null),
                open_positions: serde_json::to_value(&r.open_position_snapshots)
                    .unwrap_or(serde_json::Value::Null),
                pending_orders: serde_json::to_value(&r.pending_order_snapshots)
                    .unwrap_or(serde_json::Value::Null),
                pending_order_lifecycle: r
                    .pending_order_lifecycle
                    .iter()
                    .map(pending_order_lifecycle_to_msg)
                    .collect(),
                mtm_equity_curve: serde_json::to_value(&r.mtm_equity_curve)
                    .unwrap_or(serde_json::Value::Null),
                mtm_output_summary: mtm_output_summary_to_msg(&r.mtm_output_summary),
                mtm_max_drawdown: r.mtm_max_drawdown,
                mtm_max_drawdown_pct: r.mtm_max_drawdown_pct,
                provider_evaluation: serde_json::to_value(&r.provider_evaluation)
                    .unwrap_or(serde_json::Value::Null),
            }),
    }
}

// ── Individual struct conversions ───────────────────────────────────────────

fn pending_order_lifecycle_to_msg(
    event: &PendingOrderLifecycleEvent,
) -> PendingOrderLifecycleEventMsg {
    let state = match event.state {
        PendingOrderLifecycleState::Placed => PendingOrderLifecycleStateMsg::Placed,
        PendingOrderLifecycleState::Filled => PendingOrderLifecycleStateMsg::Filled,
        PendingOrderLifecycleState::Cancelled => PendingOrderLifecycleStateMsg::Cancelled,
        PendingOrderLifecycleState::UnfilledAtEnd => PendingOrderLifecycleStateMsg::UnfilledAtEnd,
    };
    PendingOrderLifecycleEventMsg {
        id: event.id.clone(),
        sequence: event.sequence,
        position_id: event.position_id.clone(),
        placement_action_id: event.placement_action_id.clone(),
        terminal_action_id: event.terminal_action_id.clone(),
        state,
        symbol: event.symbol.clone(),
        side: format!("{:?}", event.side),
        order_type: format!("{:?}", event.order_type),
        requested_size: event.requested_size,
        filled_size: event.filled_size,
        requested_price: event.requested_price,
        fill_price: event.fill_price,
        signal_ts: event.signal_ts.map(ndt_to_string),
        placed_ts: event.placed_ts.map(ndt_to_string),
        effective_ts: event.effective_ts.map(ndt_to_string),
        terminal_ts: event.terminal_ts.map(ndt_to_string),
        wait_latency_ms: event.wait_latency_ms,
        fill_ratio: event.fill_ratio,
    }
}

fn subset_stats_to_msg(s: &SubsetStats) -> SubsetStatsMsg {
    SubsetStatsMsg {
        total_trades: s.total_trades,
        winning_trades: s.winning_trades,
        losing_trades: s.losing_trades,
        breakeven_trades: s.breakeven_trades,
        total_pnl: s.total_pnl,
        gross_profit: s.gross_profit,
        gross_loss: s.gross_loss,
        win_rate: s.win_rate,
        profit_factor: sanitize_f64(s.profit_factor),
        avg_win: s.avg_win,
        avg_loss: s.avg_loss,
        win_loss_ratio: sanitize_f64(s.win_loss_ratio),
        expectancy: s.expectancy,
        largest_win: s.largest_win,
        largest_loss: s.largest_loss,
    }
}

fn streak_stats_to_msg(s: &StreakStats) -> StreakStatsMsg {
    StreakStatsMsg {
        max_consecutive_wins: s.max_consecutive_wins,
        max_consecutive_losses: s.max_consecutive_losses,
        current_streak: s.current_streak,
    }
}

fn risk_metrics_to_msg(r: &RiskMetrics) -> RiskMetricsMsg {
    RiskMetricsMsg {
        sharpe_ratio: r.sharpe_ratio,
        sortino_ratio: r.sortino_ratio,
        calmar_ratio: r.calmar_ratio,
        return_on_max_drawdown: r.return_on_max_drawdown,
        max_drawdown: r.max_drawdown,
        max_drawdown_pct: r.max_drawdown_pct,
        max_drawdown_duration_secs: r.max_drawdown_duration_secs,
    }
}

fn duration_stats_to_msg(d: &DurationStats) -> DurationStatsMsg {
    DurationStatsMsg {
        avg_duration_secs: d.avg_duration_secs,
        min_duration_secs: d.min_duration_secs,
        max_duration_secs: d.max_duration_secs,
        avg_winner_duration_secs: d.avg_winner_duration_secs,
        avg_loser_duration_secs: d.avg_loser_duration_secs,
    }
}

fn monthly_return_to_msg(m: &MonthlyReturn) -> MonthlyReturnMsg {
    MonthlyReturnMsg {
        year: m.year,
        month: m.month,
        pnl: m.pnl,
        trade_count: m.trade_count,
        ending_balance: m.ending_balance,
    }
}

fn close_reason_stats_to_msg(c: &CloseReasonStats) -> CloseReasonStatsMsg {
    CloseReasonStatsMsg {
        reason: format!("{:?}", c.reason),
        count: c.count,
        total_pnl: c.total_pnl,
        avg_pnl: c.avg_pnl,
        percentage: c.percentage,
    }
}

fn trade_result_to_msg(t: &TradeResult) -> TradeResultMsg {
    TradeResultMsg {
        position_id: t.position_id.clone(),
        symbol: t.symbol.clone(),
        side: format!("{:?}", t.side),
        entry_price: t.entry_price,
        exit_price: t.exit_price,
        size: t.size,
        pnl: t.pnl,
        open_ts: ndt_to_string(t.open_ts),
        close_ts: ndt_to_string(t.close_ts),
        close_reason: format!("{:?}", t.close_reason),
        group: t.group.clone(),
    }
}

fn position_summary_to_msg(p: &PositionSummary) -> PositionSummaryMsg {
    PositionSummaryMsg {
        position_id: p.position_id.clone(),
        symbol: p.symbol.clone(),
        side: format!("{:?}", p.side),
        group: p.group.clone(),
        entry_price: p.entry_price,
        avg_exit_price: p.avg_exit_price,
        original_size: p.original_size,
        close_count: p.close_count,
        net_pnl: p.net_pnl,
        close_reasons: p.close_reasons.iter().map(|r| format!("{:?}", r)).collect(),
        open_ts: ndt_to_string(p.open_ts),
        final_close_ts: Some(ndt_to_string(p.final_close_ts)),
        duration_seconds: p.duration_seconds,
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Replace non-finite f64 values (INFINITY, NaN) with 0.0 for safe serialization.
fn sanitize_f64(v: f64) -> f64 {
    if v.is_finite() { v } else { 0.0 }
}

// RawSignalMsg and PositionRefMsg conversions.

/// Convert a wire-safe `PositionRefMsg` into the internal `PositionRef`.
pub fn position_ref_from_msg(msg: &PositionRefMsg, registry: &SymbolRegistry) -> PositionRef {
    match msg {
        PositionRefMsg::ByTradeId { trade_id } => PositionRef::ByTradeId {
            trade_id: trade_id.clone(),
        },
        PositionRefMsg::AllOnSymbol { symbol } => PositionRef::AllOnSymbol {
            symbol: registry.normalize_or_passthrough(symbol),
        },
        PositionRefMsg::AllInGroup { group_id } => PositionRef::AllInGroup {
            group_id: group_id.clone(),
        },
    }
}

/// Convert a wire-safe `RawSignalMsg` into the internal `RawSignal`.
///
/// `default_symbol` is used when the Entry variant has an empty symbol field.
/// `registry` normalizes symbol names.
/// Converts the wire message, then applies the shared `qs-core` signal contract.
///
/// Structural decoding (timestamp, side, order-type, symbol normalization) is
/// owned here because it is wire-specific. Semantic validation is delegated to
/// `qs_core::validate_raw_signal`, the same function the parser pipeline uses, so
/// the two entry paths cannot drift. Delegating also makes this path strictly
/// stronger than before: it previously checked only entry risk, Limit/Stop price,
/// and the protective stop, and now covers target side, partial-close ratio,
/// management prices, and ScaleIn geometry as well.
pub fn raw_signal_from_msg(
    msg: &RawSignalMsg,
    default_symbol: &str,
    registry: &SymbolRegistry,
) -> crate::error::Result<RawSignal> {
    let signal = decode_raw_signal_msg(msg, default_symbol, registry)?;
    qs_core::validate_raw_signal(&signal)
        .map_err(|error| BacktestServerError::InvalidRequest(error.to_string()))?;
    Ok(signal)
}

/// Structural wire decoding without semantic validation.
fn decode_raw_signal_msg(
    msg: &RawSignalMsg,
    default_symbol: &str,
    registry: &SymbolRegistry,
) -> crate::error::Result<RawSignal> {
    match msg {
        RawSignalMsg::Entry {
            ts,
            symbol,
            side,
            order_type,
            price,
            risk,
            stoploss,
            targets,
            group,
            trade_id,
        } => {
            let parsed_ts = parse_datetime_internal(ts)?;
            let parsed_symbol = if symbol.is_empty() {
                default_symbol.to_string()
            } else {
                registry.normalize_or_passthrough(symbol)
            };
            let parsed_side = parse_side_internal(side)?;
            let parsed_order_type = parse_order_type_internal(order_type)?;
            Ok(RawSignal::Entry {
                ts: parsed_ts,
                symbol: parsed_symbol,
                side: parsed_side,
                order_type: parsed_order_type,
                price: *price,
                risk_multiplier: *risk,
                stoploss: *stoploss,
                targets: targets.clone(),
                group: group.clone(),
                trade_id: trade_id.clone(),
            })
        }
        RawSignalMsg::Close { ts, position } => Ok(RawSignal::Close {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
        }),
        RawSignalMsg::ClosePartial {
            ts,
            position,
            ratio,
        } => Ok(RawSignal::ClosePartial {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            ratio: *ratio,
        }),
        RawSignalMsg::ModifyStoploss {
            ts,
            position,
            price,
        } => Ok(RawSignal::ModifyStoploss {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            price: *price,
        }),
        RawSignalMsg::MoveStoplossToEntry { ts, position } => Ok(RawSignal::MoveStoplossToEntry {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
        }),
        RawSignalMsg::AddTarget {
            ts,
            position,
            price,
            close_ratio,
        } => Ok(RawSignal::AddTarget {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            price: *price,
            close_ratio: *close_ratio,
        }),
        RawSignalMsg::RemoveTarget {
            ts,
            position,
            price,
        } => Ok(RawSignal::RemoveTarget {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            price: *price,
        }),
        RawSignalMsg::ModifyTarget {
            ts,
            position,
            old_price,
            new_price,
        } => Ok(RawSignal::ModifyTarget {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            old_price: *old_price,
            new_price: *new_price,
        }),
        RawSignalMsg::AddRule { ts, position, rule } => {
            let rule_def = rule_config_def_from_msg(rule);
            Ok(RawSignal::AddRule {
                ts: parse_datetime_internal(ts)?,
                position: position_ref_from_msg(position, registry),
                rule: rule_def,
            })
        }
        RawSignalMsg::RemoveRule {
            ts,
            position,
            rule_name,
        } => Ok(RawSignal::RemoveRule {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            rule_name: rule_name.clone(),
        }),
        RawSignalMsg::ScaleIn {
            ts,
            position,
            price,
            size,
        } => Ok(RawSignal::ScaleIn {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
            price: *price,
            size: *size,
        }),
        RawSignalMsg::CancelPending { ts, position } => Ok(RawSignal::CancelPending {
            ts: parse_datetime_internal(ts)?,
            position: position_ref_from_msg(position, registry),
        }),
        RawSignalMsg::CloseAllOf { ts, symbol } => Ok(RawSignal::CloseAllOf {
            ts: parse_datetime_internal(ts)?,
            symbol: registry.normalize_or_passthrough(symbol),
        }),
        RawSignalMsg::CloseAll { ts } => Ok(RawSignal::CloseAll {
            ts: parse_datetime_internal(ts)?,
        }),
        RawSignalMsg::CancelAllPending { ts } => Ok(RawSignal::CancelAllPending {
            ts: parse_datetime_internal(ts)?,
        }),
        RawSignalMsg::ModifyAllStoploss { ts, symbol, price } => Ok(RawSignal::ModifyAllStoploss {
            ts: parse_datetime_internal(ts)?,
            symbol: registry.normalize_or_passthrough(symbol),
            price: *price,
        }),
        RawSignalMsg::CloseAllInGroup { ts, group_id } => Ok(RawSignal::CloseAllInGroup {
            ts: parse_datetime_internal(ts)?,
            group_id: group_id.clone(),
        }),
        RawSignalMsg::ModifyAllStoplossInGroup {
            ts,
            group_id,
            price,
        } => Ok(RawSignal::ModifyAllStoplossInGroup {
            ts: parse_datetime_internal(ts)?,
            group_id: group_id.clone(),
            price: *price,
        }),
    }
}

/// Convert a `RuleConfigDefMsg` into the internal `RuleConfigDef`.
fn rule_config_def_from_msg(msg: &RuleConfigDefMsg) -> RuleConfigDef {
    match msg {
        RuleConfigDefMsg::FixedStoploss { price } => RuleConfigDef::FixedStoploss { price: *price },
        RuleConfigDefMsg::TrailingStop { distance } => RuleConfigDef::TrailingStop {
            distance: *distance,
        },
        RuleConfigDefMsg::TakeProfit { price, close_ratio } => RuleConfigDef::TakeProfit {
            price: *price,
            close_ratio: *close_ratio,
        },
        RuleConfigDefMsg::BreakevenWhen { trigger_price } => RuleConfigDef::BreakevenWhen {
            trigger_price: *trigger_price,
        },
        RuleConfigDefMsg::BreakevenWhenOffset {
            trigger_price_offset,
        } => RuleConfigDef::BreakevenWhenOffset {
            trigger_price_offset: *trigger_price_offset,
        },
        RuleConfigDefMsg::BreakevenAfterTargets { after_n } => {
            RuleConfigDef::BreakevenAfterTargets { after_n: *after_n }
        }
        RuleConfigDefMsg::TimeExit { max_seconds } => RuleConfigDef::TimeExit {
            max_seconds: *max_seconds,
        },
    }
}

// Internal parsing helpers.

fn parse_datetime_internal(s: &str) -> crate::error::Result<NaiveDateTime> {
    parse_backtest_timestamp(s)
        .map_err(|error| BacktestServerError::InvalidRequest(error.to_string()))
}

fn parse_side_internal(s: &str) -> crate::error::Result<Side> {
    match s {
        "Buy" | "buy" | "BUY" | "Long" | "long" => Ok(Side::Buy),
        "Sell" | "sell" | "SELL" | "Short" | "short" => Ok(Side::Sell),
        other => Err(BacktestServerError::InvalidRequest(format!(
            "Invalid side: '{other}'."
        ))),
    }
}

fn parse_order_type_internal(s: &str) -> crate::error::Result<OrderType> {
    match s {
        "Market" | "market" | "MARKET" => Ok(OrderType::Market),
        "Limit" | "limit" | "LIMIT" => Ok(OrderType::Limit),
        "Stop" | "stop" | "STOP" => Ok(OrderType::Stop),
        other => Err(BacktestServerError::InvalidRequest(format!(
            "Invalid order_type: '{other}'."
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_backtest::profile::{ManagementProfile, TargetSelection};
    use qs_core::types::{CloseReason, OrderType, Side};

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    #[test]
    fn config_defaults() {
        let msg = BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: None,
        };
        let registry = qs_symbols::SymbolRegistry::empty();
        let symbols: Vec<String> = vec![];
        let cfg = config_from_msg(&msg, &registry, &symbols).unwrap();
        assert!((cfg.initial_balance - 10_000.0).abs() < f64::EPSILON);
        assert!(cfg.close_on_finish);
        assert_eq!(cfg.fill_model, FillModel::BidAsk);
    }

    #[test]
    fn config_overrides() {
        let msg = BacktestConfigMsg {
            initial_balance: Some(50_000.0),
            close_on_finish: Some(false),
            fill_model: Some("MidPrice".into()),
            sizing: None,
        };
        let registry = qs_symbols::SymbolRegistry::empty();
        let symbols: Vec<String> = vec![];
        let cfg = config_from_msg(&msg, &registry, &symbols).unwrap();
        assert!((cfg.initial_balance - 50_000.0).abs() < f64::EPSILON);
        assert!(!cfg.close_on_finish);
        assert_eq!(cfg.fill_model, FillModel::MidPrice);
    }

    #[test]
    fn config_rejects_invalid_sizing_value() {
        let msg = BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::BalanceRiskPercent { percent: 0.0 }),
        };
        let error = config_from_msg(&msg, &SymbolRegistry::empty(), &[]).unwrap_err();
        assert!(error.to_string().contains("balance risk percent"));
    }

    #[test]
    fn future_config_validates_and_embeds_currency_plan() {
        use qs_backtest::currency::ConversionRoute;
        use std::collections::{BTreeMap, BTreeSet};

        let primary_symbols = BTreeSet::from(["eurusd".to_owned()]);
        let pnl = BTreeMap::from([("eurusd".to_owned(), "USD".to_owned())]);
        let routes = BTreeMap::from([(
            "USD".to_owned(),
            ConversionRoute::Identity {
                currency: "USD".to_owned(),
            },
        )]);
        let plan = RunCurrencyPlan::new(
            "USD",
            primary_symbols,
            BTreeSet::new(),
            pnl,
            routes,
            Vec::new(),
        )
        .unwrap();
        let msg = FutureQuoteConfigMsg {
            account_currency: " usd ".into(),
            conversion_stale_after_ms: 42_000,
            ..FutureQuoteConfigMsg::default()
        };
        let config = future_config_from_msg(&msg, plan).unwrap();
        assert_eq!(config.conversion_stale_after_ms, 42_000);
        assert_eq!(
            config.mtm_output,
            MtmOutputPolicy::Bounded { max_points: 4_096 }
        );
        assert_eq!(config.currency_plan.unwrap().account_currency(), "USD");
    }

    #[test]
    fn mtm_output_policy_maps_and_validates_internal_bounds() {
        for (message, expected) in [
            (MtmOutputPolicyMsg::None, MtmOutputPolicy::None),
            (
                MtmOutputPolicyMsg::Bounded { max_points: 512 },
                MtmOutputPolicy::Bounded { max_points: 512 },
            ),
            (MtmOutputPolicyMsg::Full, MtmOutputPolicy::Full),
        ] {
            assert_eq!(mtm_output_policy_from_msg(&message).unwrap(), expected);
        }

        for max_points in [7, 16_385] {
            let error = mtm_output_policy_from_msg(&MtmOutputPolicyMsg::Bounded { max_points })
                .unwrap_err();
            assert!(error.to_string().contains("invalid mtm_output"));
            assert!(error.to_string().contains(&max_points.to_string()));
        }
    }

    #[test]
    fn fill_model_parsing() {
        assert_eq!(parse_fill_model(Some("BidAsk")), FillModel::BidAsk);
        assert_eq!(parse_fill_model(Some("AskOnly")), FillModel::AskOnly);
        assert_eq!(parse_fill_model(Some("MidPrice")), FillModel::MidPrice);
        assert_eq!(parse_fill_model(Some("unknown")), FillModel::BidAsk);
        assert_eq!(parse_fill_model(None), FillModel::BidAsk);
    }

    #[test]
    fn trade_result_converts() {
        let tr = TradeResult {
            position_id: "p1".into(),
            symbol: "eurusd".into(),
            side: Side::Buy,
            entry_price: 1.0850,
            exit_price: 1.0900,
            size: 1.0,
            pnl: 50.0,
            open_ts: ts(10, 0, 0),
            close_ts: ts(11, 0, 0),
            close_reason: CloseReason::Target,
            group: Some("g1".into()),
        };
        let msg = trade_result_to_msg(&tr);
        assert_eq!(msg.position_id, "p1");
        assert_eq!(msg.side, "Buy");
        assert_eq!(msg.close_reason, "Target");
        assert_eq!(msg.group, Some("g1".into()));
        assert!(msg.open_ts.contains("2026-01-01"));
    }

    #[test]
    fn subset_stats_sanitizes_infinity() {
        let s = SubsetStats {
            total_trades: 2,
            winning_trades: 2,
            losing_trades: 0,
            breakeven_trades: 0,
            total_pnl: 100.0,
            gross_profit: 100.0,
            gross_loss: 0.0,
            win_rate: 1.0,
            profit_factor: f64::INFINITY,
            avg_win: 50.0,
            avg_loss: 0.0,
            win_loss_ratio: f64::INFINITY,
            expectancy: 50.0,
            largest_win: 60.0,
            largest_loss: 0.0,
        };
        let msg = subset_stats_to_msg(&s);
        assert!((msg.profit_factor - 0.0).abs() < f64::EPSILON);
        assert!((msg.win_loss_ratio - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn equity_point_timestamp_format() {
        let ts_val = ts(14, 30, 15);
        let s = ndt_to_string(ts_val);
        assert!(s.starts_with("2026-01-01T14:30:15"));
    }

    #[test]
    fn empty_result_converts_without_panic() {
        let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
        let msg = result_to_msg(&result);
        assert_eq!(msg.total_trades, 0);
        assert!(msg.trade_log.is_empty());
        assert!(msg.equity_curve.is_empty());
        assert!(msg.positions.is_empty());
    }

    #[test]
    fn future_result_converts_pending_order_lifecycle_to_typed_message() {
        let mut result = BacktestResult::from_trade_log(10_000.0, Vec::new());
        result.execution_metadata = Some(qs_backtest::ExecutionMetadata::default());
        result.mtm_output_summary = MtmOutputSummary {
            policy: MtmOutputPolicy::Full,
            observed_points: 12,
            retained_points: 12,
            omitted_points: 0,
        };
        result.pending_order_lifecycle = vec![PendingOrderLifecycleEvent {
            id: "position-1:pending_filled:00000001".into(),
            sequence: 1,
            position_id: "position-1".into(),
            placement_action_id: Some("signal:00000000".into()),
            state: PendingOrderLifecycleState::Filled,
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            requested_size: 2.0,
            filled_size: Some(2.0),
            requested_price: Some(1.1),
            fill_price: Some(1.09),
            signal_ts: Some(ts(10, 0, 0)),
            placed_ts: Some(ts(10, 0, 1)),
            effective_ts: Some(ts(10, 0, 0)),
            terminal_ts: Some(ts(10, 0, 3)),
            wait_latency_ms: Some(2_000),
            fill_ratio: Some(1.0),
            ..PendingOrderLifecycleEvent::default()
        }];

        let future = result_to_msg(&result).future.expect("FutureQuote payload");
        assert_eq!(
            future.mtm_output_summary,
            MtmOutputSummaryMsg {
                policy: MtmOutputPolicyMsg::Full,
                observed_points: 12,
                retained_points: 12,
                omitted_points: 0,
            }
        );
        assert_eq!(future.pending_order_lifecycle.len(), 1);
        let event = &future.pending_order_lifecycle[0];
        assert_eq!(event.state, PendingOrderLifecycleStateMsg::Filled);
        assert_eq!(event.order_type, "Limit");
        assert_eq!(event.requested_size, 2.0);
        assert_eq!(event.filled_size, Some(2.0));
        assert_eq!(event.wait_latency_ms, Some(2_000));
        assert_eq!(event.fill_ratio, Some(1.0));
        assert_eq!(event.terminal_ts.as_deref(), Some("2026-01-01T10:00:03"));
    }

    // Management profile conversion tests.

    #[test]
    fn profile_from_msg_basic() {
        let msg = ManagementProfileMsg {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![0.5, 0.5],
            stoploss_mode: Some(StoplossModeMsg::FromSignal),
            rules: vec![RuleConfigDefMsg::TrailingStop { distance: 10.0 }],
            group_override: Some("grp".into()),
            let_remainder_run: true,
        };
        let p = profile_from_msg(&msg).unwrap();
        assert_eq!(p.name, "test");
        assert_eq!(p.use_targets, vec![1, 2]);
        assert_eq!(p.close_ratios, vec![0.5, 0.5]);
        assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));
        assert_eq!(p.rules.len(), 1);
        assert_eq!(p.group_override, Some("grp".into()));
        assert!(p.let_remainder_run);
    }

    #[test]
    fn profile_from_msg_defaults() {
        let msg = ManagementProfileMsg {
            name: "minimal".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        let p = profile_from_msg(&msg).unwrap();
        assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));
        assert!(p.rules.is_empty());
        assert!(p.group_override.is_none());
        assert!(!p.let_remainder_run);
    }

    #[test]
    fn profile_from_msg_all_stoploss_modes() {
        // FromSignal
        let msg = ManagementProfileMsg {
            name: "a".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: Some(StoplossModeMsg::FromSignal),
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        let p = profile_from_msg(&msg).unwrap();
        assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));

        // None
        let msg2 = ManagementProfileMsg {
            stoploss_mode: Some(StoplossModeMsg::None),
            ..msg.clone()
        };
        let p2 = profile_from_msg(&msg2).unwrap();
        assert!(matches!(p2.stoploss_mode, StoplossMode::None));

        // FixedDistance
        let msg3 = ManagementProfileMsg {
            stoploss_mode: Some(StoplossModeMsg::FixedDistance { distance: 50.0 }),
            ..msg.clone()
        };
        let p3 = profile_from_msg(&msg3).unwrap();
        assert!(matches!(
            p3.stoploss_mode,
            StoplossMode::FixedDistance { distance } if (distance - 50.0).abs() < f64::EPSILON
        ));

        // FixedPrice
        let msg4 = ManagementProfileMsg {
            stoploss_mode: Some(StoplossModeMsg::FixedPrice { price: 1.0800 }),
            ..msg.clone()
        };
        let p4 = profile_from_msg(&msg4).unwrap();
        assert!(matches!(
            p4.stoploss_mode,
            StoplossMode::FixedPrice { price } if (price - 1.0800).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn profile_from_msg_all_rule_types() {
        let rules = vec![
            RuleConfigDefMsg::FixedStoploss { price: 1.0 },
            RuleConfigDefMsg::TrailingStop { distance: 10.0 },
            RuleConfigDefMsg::TakeProfit {
                price: 2.0,
                close_ratio: 0.5,
            },
            RuleConfigDefMsg::BreakevenWhen { trigger_price: 1.5 },
            RuleConfigDefMsg::BreakevenWhenOffset {
                trigger_price_offset: 0.5,
            },
            RuleConfigDefMsg::BreakevenAfterTargets { after_n: 2 },
            RuleConfigDefMsg::TimeExit { max_seconds: 3600 },
        ];
        let msg = ManagementProfileMsg {
            name: "allrules".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules,
            group_override: None,
            let_remainder_run: false,
        };
        let p = profile_from_msg(&msg).unwrap();
        assert_eq!(p.rules.len(), 7);
        assert!(matches!(p.rules[0], RuleConfigDef::FixedStoploss { .. }));
        assert!(matches!(p.rules[1], RuleConfigDef::TrailingStop { .. }));
        assert!(matches!(p.rules[2], RuleConfigDef::TakeProfit { .. }));
        assert!(matches!(p.rules[3], RuleConfigDef::BreakevenWhen { .. }));
        assert!(matches!(
            p.rules[4],
            RuleConfigDef::BreakevenWhenOffset { .. }
        ));
        assert!(matches!(
            p.rules[5],
            RuleConfigDef::BreakevenAfterTargets { .. }
        ));
        assert!(matches!(p.rules[6], RuleConfigDef::TimeExit { .. }));
    }

    #[test]
    fn profile_target_selection_serde_and_conversion_roundtrip() {
        let selections = [
            TargetSelectionMsg::All,
            TargetSelectionMsg::None,
            TargetSelectionMsg::Selected(vec![2, 1]),
        ];

        for selection in selections {
            let msg = ManagementProfileMsg {
                name: "selection".into(),
                target_selection: Some(selection.clone()),
                use_targets: vec![1],
                close_ratios: vec![],
                stoploss_mode: None,
                rules: vec![],
                group_override: None,
                let_remainder_run: false,
            };
            let json = serde_json::to_value(&msg).unwrap();
            assert!(json.get("target_selection").is_some());

            let decoded: ManagementProfileMsg = serde_json::from_value(json).unwrap();
            assert_eq!(decoded.target_selection, Some(selection.clone()));

            let profile = profile_from_msg(&decoded).unwrap();
            let roundtrip = profile_to_msg(&profile);
            assert_eq!(roundtrip.target_selection, Some(selection));
            assert!(roundtrip.close_ratios.is_empty());
        }
    }

    #[test]
    fn legacy_profile_msg_omission_uses_legacy_selection_default() {
        let json = serde_json::json!({
            "name": "legacy",
            "use_targets": [2],
            "close_ratios": [1.0]
        });
        let msg: ManagementProfileMsg = serde_json::from_value(json).unwrap();
        assert_eq!(msg.target_selection, None);

        let profile = profile_from_msg(&msg).unwrap();
        assert_eq!(profile.target_selection, None);
        assert_eq!(
            profile.effective_target_selection(),
            TargetSelection::Selected(vec![2])
        );
        assert_eq!(profile_to_msg(&profile).target_selection, None);
    }

    #[test]
    fn profile_to_msg_roundtrip() {
        let original = ManagementProfile {
            name: "rt".into(),
            target_selection: Some(TargetSelection::Selected(vec![2, 1])),
            use_targets: vec![1, 2],
            close_ratios: vec![0.6, 0.4],
            stoploss_mode: StoplossMode::FixedDistance { distance: 25.0 },
            rules: vec![
                RuleConfigDef::TrailingStop { distance: 15.0 },
                RuleConfigDef::TimeExit { max_seconds: 7200 },
            ],
            group_override: Some("mygroup".into()),
            let_remainder_run: true,
        };
        let msg = profile_to_msg(&original);
        let back = profile_from_msg(&msg).unwrap();

        assert_eq!(back.name, original.name);
        assert_eq!(back.target_selection, original.target_selection);
        assert_eq!(back.use_targets, original.use_targets);
        assert_eq!(back.close_ratios, original.close_ratios);
        assert!(matches!(
            back.stoploss_mode,
            StoplossMode::FixedDistance { distance } if (distance - 25.0).abs() < f64::EPSILON
        ));
        assert_eq!(back.rules.len(), 2);
        assert_eq!(back.group_override, original.group_override);
        assert_eq!(back.let_remainder_run, original.let_remainder_run);
    }

    // RawSignalMsg and PositionRefMsg conversion tests.

    #[test]
    fn position_ref_from_msg_id() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::ByTradeId {
            trade_id: "pos_123".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        assert!(matches!(result, PositionRef::ByTradeId { trade_id } if trade_id == "pos_123"));
    }

    #[test]
    fn position_ref_from_msg_all_on_symbol_normalizes() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::AllOnSymbol {
            symbol: "EUR/USD".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        // empty registry normalizes via passthrough: lowercase + strip separators
        assert!(matches!(result, PositionRef::AllOnSymbol { symbol } if symbol == "eurusd"));
    }

    #[test]
    fn position_ref_from_msg_all_in_group() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::AllInGroup {
            group_id: "scalp".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        assert!(matches!(result, PositionRef::AllInGroup { group_id } if group_id == "scalp"));
    }

    #[test]
    fn raw_signal_from_msg_entry_basic() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 0.02,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: Some("grp".into()),
            trade_id: Some("t1".into()),
        };
        let result = raw_signal_from_msg(&msg, "default", &reg).unwrap();
        assert!(result.is_entry());
        match &result {
            RawSignal::Entry {
                symbol,
                side,
                order_type,
                risk_multiplier,
                stoploss,
                targets,
                group,
                trade_id,
                ..
            } => {
                assert_eq!(symbol, "eurusd");
                assert_eq!(*side, Side::Buy);
                assert_eq!(*order_type, OrderType::Market);
                assert_eq!(*risk_multiplier, 0.02);
                assert_eq!(*stoploss, Some(1.0800));
                assert_eq!(*targets, vec![1.0900]);
                assert_eq!(*group, Some("grp".into()));
                assert_eq!(trade_id.as_deref(), Some("t1"));
            }
            _ => panic!("Expected Entry"),
        }
    }

    #[test]
    fn raw_signal_from_msg_normalizes_offset_timestamp_to_utc() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::Entry {
            ts: "2026-01-15T00:30:00+02:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: None,
        };

        let signal = raw_signal_from_msg(&msg, "default", &reg).unwrap();
        assert_eq!(
            signal.ts(),
            chrono::NaiveDate::from_ymd_opt(2026, 1, 14)
                .unwrap()
                .and_hms_opt(22, 30, 0)
                .unwrap()
        );
    }

    #[test]
    fn raw_signal_from_msg_close_partial() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::ClosePartial {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "t1".into(),
            },
            ratio: 0.5,
        };
        let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
        match result {
            RawSignal::ClosePartial {
                ratio, position, ..
            } => {
                assert!((ratio - 0.5).abs() < f64::EPSILON);
                assert!(
                    matches!(position, PositionRef::ByTradeId { trade_id } if trade_id == "t1")
                );
            }
            _ => panic!("Expected ClosePartial"),
        }
    }

    #[test]
    fn raw_signal_from_msg_modify_target() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::ModifyTarget {
            ts: "2026-01-15T10:25:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "targeted".into(),
            },
            old_price: 1.0900,
            new_price: 1.0950,
        };

        let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();

        assert!(matches!(
            result,
            RawSignal::ModifyTarget {
                position: PositionRef::ByTradeId { trade_id },
                old_price,
                new_price,
                ..
            } if trade_id == "targeted"
                && (old_price - 1.0900).abs() < f64::EPSILON
                && (new_price - 1.0950).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn raw_signal_from_msg_add_rule_trailing() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::AddRule {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "p1".into(),
            },
            rule: RuleConfigDefMsg::TrailingStop { distance: 0.0020 },
        };
        let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
        match result {
            RawSignal::AddRule { rule, .. } => {
                assert!(
                    matches!(rule, RuleConfigDef::TrailingStop { distance } if (distance - 0.0020).abs() < f64::EPSILON)
                );
            }
            _ => panic!("Expected AddRule"),
        }
    }

    #[test]
    fn raw_signal_from_msg_scale_in() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::ScaleIn {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "g1-trade-1".into(),
            },
            price: Some(1.0850),
            size: 0.01,
        };
        let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
        match result {
            RawSignal::ScaleIn {
                price,
                size,
                position,
                ..
            } => {
                assert_eq!(price, Some(1.0850));
                assert_eq!(size, 0.01);
                assert!(
                    matches!(position, PositionRef::ByTradeId { trade_id } if trade_id == "g1-trade-1")
                );
            }
            _ => panic!("Expected ScaleIn"),
        }
    }

    #[test]
    fn raw_signal_from_msg_bulk_close_all_in_group() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::CloseAllInGroup {
            ts: "2026-01-15T11:00:00".into(),
            group_id: "momentum".into(),
        };
        let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
        match result {
            RawSignal::CloseAllInGroup { group_id, .. } => {
                assert_eq!(group_id, "momentum");
            }
            _ => panic!("Expected CloseAllInGroup"),
        }
    }

    #[test]
    fn raw_signal_from_msg_invalid_side_errors() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "WRONG".into(),
            order_type: "Market".into(),
            price: None,
            trade_id: None,
            risk: 0.01,
            stoploss: None,
            targets: vec![],
            group: None,
        };
        assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
    }

    #[test]
    fn raw_signal_from_msg_invalid_ts_errors() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::CloseAll {
            ts: "bad-date".into(),
        };
        assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
    }

    #[test]
    fn raw_signal_from_msg_empty_symbol_uses_default() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "".into(),
            side: "Sell".into(),
            order_type: "Limit".into(),
            price: Some(1.0900),
            risk: 0.01,
            stoploss: None,
            targets: vec![],
            trade_id: None,
            group: None,
        };
        let result = raw_signal_from_msg(&msg, "xauusd", &reg).unwrap();
        assert!(result.is_entry());
        match &result {
            RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "xauusd"),
            _ => panic!("expected Entry"),
        }
    }

    #[test]
    fn rule_config_def_from_msg_all_variants() {
        let cases: Vec<(RuleConfigDefMsg, &str)> = vec![
            (
                RuleConfigDefMsg::FixedStoploss { price: 1.08 },
                "FixedStoploss",
            ),
            (
                RuleConfigDefMsg::TrailingStop { distance: 0.002 },
                "TrailingStop",
            ),
            (
                RuleConfigDefMsg::TakeProfit {
                    price: 1.10,
                    close_ratio: 0.5,
                },
                "TakeProfit",
            ),
            (
                RuleConfigDefMsg::BreakevenWhen {
                    trigger_price: 1.09,
                },
                "BreakevenWhen",
            ),
            (
                RuleConfigDefMsg::BreakevenWhenOffset {
                    trigger_price_offset: 0.005,
                },
                "BreakevenWhenOffset",
            ),
            (
                RuleConfigDefMsg::BreakevenAfterTargets { after_n: 2 },
                "BreakevenAfterTargets",
            ),
            (RuleConfigDefMsg::TimeExit { max_seconds: 3600 }, "TimeExit"),
        ];
        for (msg, expected_name) in cases {
            let result = rule_config_def_from_msg(&msg);
            let debug_str = format!("{:?}", result);
            assert!(
                debug_str.contains(expected_name),
                "Expected {} in {:?}",
                expected_name,
                debug_str
            );
        }
    }
}
