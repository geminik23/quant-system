//! Conversions between internal backtest types and wire-safe RPC messages.

use chrono::NaiveDateTime;
use qs_backtest::profile::{
    ManagementProfile, PositionRef, RawSignal, RuleConfigDef, StoplossMode,
};
use qs_backtest::report::{
    BacktestResult, CloseReasonStats, DurationStats, MonthlyReturn, PositionSummary, RiskMetrics,
    StreakStats, SubsetStats, TradeResult,
};
use qs_backtest::runner::BacktestConfig;
use qs_core::types::{FillModel, OrderType, Side};
use qs_symbols::SymbolRegistry;

use crate::error::BacktestServerError;
use crate::rpc_types::{
    BacktestConfigMsg, BacktestResultMsg, CloseReasonStatsMsg, DurationStatsMsg, EquityPoint,
    ManagementProfileMsg, MonthlyReturnMsg, PositionRefMsg, PositionSummaryMsg, RawSignalMsg,
    RiskMetricsMsg, RuleConfigDefMsg, StoplossModeMsg, StreakStatsMsg, SubsetStatsMsg,
    TradeResultMsg,
};

// ── Timestamp formatting ────────────────────────────────────────────────────

const TS_FMT: &str = "%Y-%m-%dT%H:%M:%S%.f";

fn ndt_to_string(ts: NaiveDateTime) -> String {
    ts.format(TS_FMT).to_string()
}

// ── BacktestConfigMsg → BacktestConfig ──────────────────────────────────────

/// Convert the wire config message into the internal `BacktestConfig`.
pub fn config_from_msg(msg: &BacktestConfigMsg) -> BacktestConfig {
    BacktestConfig {
        initial_balance: msg.initial_balance.unwrap_or(10_000.0),
        close_on_finish: msg.close_on_finish.unwrap_or(true),
        fill_model: parse_fill_model(msg.fill_model.as_deref()),
        contract_sizes: std::collections::HashMap::new(),
    }
}

/// Parse a fill model string, defaulting to BidAsk for unknown values.
pub fn parse_fill_model(s: Option<&str>) -> FillModel {
    match s {
        Some("AskOnly") => FillModel::AskOnly,
        Some("MidPrice") => FillModel::MidPrice,
        _ => FillModel::BidAsk,
    }
}

// ── Profile Conversions (F13) ───────────────────────────────────────────────

/// Convert a wire-format `ManagementProfileMsg` into the internal `ManagementProfile`.
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
        use_targets: p.use_targets.clone(),
        close_ratios: p.close_ratios.clone(),
        stoploss_mode,
        rules,
        group_override: p.group_override.clone(),
        let_remainder_run: p.let_remainder_run,
    }
}

// ── BacktestResult → BacktestResultMsg ──────────────────────────────────────

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
    }
}

// ── Individual struct conversions ───────────────────────────────────────────

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

// ── F14: RawSignalMsg / PositionRefMsg Conversions ──────────────────────────

/// Convert a wire-safe `PositionRefMsg` into the internal `PositionRef`.
pub fn position_ref_from_msg(msg: &PositionRefMsg, registry: &SymbolRegistry) -> PositionRef {
    match msg {
        PositionRefMsg::Id { id } => PositionRef::Id { id: id.clone() },
        PositionRefMsg::LastOnSymbol { symbol } => PositionRef::LastOnSymbol {
            symbol: registry.normalize_or_passthrough(symbol),
        },
        PositionRefMsg::LastInGroup { group_id } => PositionRef::LastInGroup {
            group_id: group_id.clone(),
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
pub fn raw_signal_from_msg(
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
            size,
            stoploss,
            targets,
            group,
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
                size: *size,
                stoploss: *stoploss,
                targets: targets.clone(),
                group: group.clone(),
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

// ── Internal parsing helpers (duplicated from handlers to avoid circular deps) ──

fn parse_datetime_internal(s: &str) -> crate::error::Result<NaiveDateTime> {
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
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(date.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(BacktestServerError::InvalidRequest(format!(
        "Cannot parse datetime: '{s}'."
    )))
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

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_backtest::profile::ManagementProfile;
    use qs_core::types::{CloseReason, Side};

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
        };
        let cfg = config_from_msg(&msg);
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
        };
        let cfg = config_from_msg(&msg);
        assert!((cfg.initial_balance - 50_000.0).abs() < f64::EPSILON);
        assert!(!cfg.close_on_finish);
        assert_eq!(cfg.fill_model, FillModel::MidPrice);
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

    // ── Profile conversion tests (F13) ──────────────────────────────────

    #[test]
    fn profile_from_msg_basic() {
        let msg = ManagementProfileMsg {
            name: "test".into(),
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
    fn profile_to_msg_roundtrip() {
        let original = ManagementProfile {
            name: "rt".into(),
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

    // ── F14: RawSignalMsg / PositionRefMsg conversion tests ─────────────

    #[test]
    fn position_ref_from_msg_id() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::Id {
            id: "pos_123".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        assert!(matches!(result, PositionRef::Id { id } if id == "pos_123"));
    }

    #[test]
    fn position_ref_from_msg_last_on_symbol_normalizes() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::LastOnSymbol {
            symbol: "EUR/USD".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        // empty registry normalizes via passthrough: lowercase + strip separators
        assert!(matches!(result, PositionRef::LastOnSymbol { symbol } if symbol == "eurusd"));
    }

    #[test]
    fn position_ref_from_msg_all_in_group() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = PositionRefMsg::AllInGroup {
            group_id: "scalp_v2".into(),
        };
        let result = position_ref_from_msg(&msg, &reg);
        assert!(matches!(result, PositionRef::AllInGroup { group_id } if group_id == "scalp_v2"));
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
            size: 0.02,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: Some("grp".into()),
        };
        let result = raw_signal_from_msg(&msg, "default", &reg).unwrap();
        assert!(result.is_entry());
        let entry = result.as_entry().unwrap();
        assert_eq!(entry.symbol, "eurusd");
        assert_eq!(entry.side, Side::Buy);
        assert_eq!(entry.order_type, OrderType::Market);
        assert_eq!(entry.size, 0.02);
        assert_eq!(entry.stoploss, Some(1.0800));
        assert_eq!(entry.targets, vec![1.0900]);
        assert_eq!(entry.group, Some("grp".into()));
    }

    #[test]
    fn raw_signal_from_msg_close_partial() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::ClosePartial {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: "eurusd".into(),
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
                    matches!(position, PositionRef::LastOnSymbol { symbol } if symbol == "eurusd")
                );
            }
            _ => panic!("Expected ClosePartial"),
        }
    }

    #[test]
    fn raw_signal_from_msg_add_rule_trailing() {
        let reg = qs_symbols::SymbolRegistry::empty();
        let msg = RawSignalMsg::AddRule {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::Id { id: "p1".into() },
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
            position: PositionRefMsg::LastInGroup {
                group_id: "g1".into(),
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
                    matches!(position, PositionRef::LastInGroup { group_id } if group_id == "g1")
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
            size: 0.01,
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
            size: 0.01,
            stoploss: None,
            targets: vec![],
            group: None,
        };
        let result = raw_signal_from_msg(&msg, "xauusd", &reg).unwrap();
        let entry = result.as_entry().unwrap();
        assert_eq!(entry.symbol, "xauusd");
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
