//! Backtest reporting — trade log, aggregate statistics, and enhanced analytics.
//!
//! This module provides per-trade results, per-position summaries, and rich
//! aggregate statistics including risk-adjusted metrics (Sharpe, Sortino, Calmar),
//! streak analysis, duration stats, monthly returns, and breakdowns by symbol,
//! group, side, and close reason.

use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use qs_core::types::{CloseReason, GroupId, PositionId, Side};

// ─── Serde helper for f64 fields that may be INFINITY or NaN ────────────────

/// Serializes non-finite f64 (INFINITY, NEG_INFINITY, NaN) as JSON null.
mod finite_f64 {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if value.is_finite() {
            serializer.serialize_f64(*value)
        } else {
            serializer.serialize_none()
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Accept either a number or null (null → 0.0).
        let opt = Option::<f64>::deserialize(deserializer)?;
        Ok(opt.unwrap_or(0.0))
    }
}

// ─── TradeResult ────────────────────────────────────────────────────────────

/// Result of a single closed trade (or partial close).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    pub position_id: PositionId,
    pub symbol: String,
    pub side: Side,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub pnl: f64,
    pub open_ts: NaiveDateTime,
    pub close_ts: NaiveDateTime,
    pub close_reason: CloseReason,
    /// Group this position belonged to (for per-group reporting).
    #[serde(default)]
    pub group: Option<GroupId>,
}

// ─── SubsetStats ────────────────────────────────────────────────────────────

/// Reusable statistics block computed from any subset of trades.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsetStats {
    /// Number of trades in this subset.
    pub total_trades: usize,
    /// Trades with positive P&L.
    pub winning_trades: usize,
    /// Trades with negative P&L.
    pub losing_trades: usize,
    /// Trades with exactly zero P&L.
    pub breakeven_trades: usize,
    /// Sum of all P&L.
    pub total_pnl: f64,
    /// Sum of positive P&L.
    pub gross_profit: f64,
    /// Sum of absolute negative P&L.
    pub gross_loss: f64,
    /// winning / total (0.0 if no trades).
    pub win_rate: f64,
    /// gross_profit / gross_loss (INFINITY if no losers, 0.0 if no trades).
    #[serde(with = "finite_f64")]
    pub profit_factor: f64,
    /// gross_profit / winning_trades (0.0 if no winners).
    pub avg_win: f64,
    /// gross_loss / losing_trades (0.0 if no losers).
    pub avg_loss: f64,
    /// avg_win / avg_loss (INFINITY if no losers, 0.0 if no winners).
    #[serde(with = "finite_f64")]
    pub win_loss_ratio: f64,
    /// (win_rate * avg_win) - (loss_rate * avg_loss). Expected P&L per trade.
    pub expectancy: f64,
    /// Largest single winning trade P&L.
    pub largest_win: f64,
    /// Largest single losing trade (as positive number).
    pub largest_loss: f64,
}

impl SubsetStats {
    /// Compute statistics from a slice of trade references.
    pub fn from_trades(trades: &[&TradeResult]) -> Self {
        let total_trades = trades.len();
        let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count();
        let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count();
        let breakeven_trades = trades.iter().filter(|t| t.pnl == 0.0).count();

        let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = trades
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .sum();

        let win_rate = if total_trades > 0 {
            winning_trades as f64 / total_trades as f64
        } else {
            0.0
        };

        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let avg_win = if winning_trades > 0 {
            gross_profit / winning_trades as f64
        } else {
            0.0
        };

        let avg_loss = if losing_trades > 0 {
            gross_loss / losing_trades as f64
        } else {
            0.0
        };

        let win_loss_ratio = if avg_loss > 0.0 {
            avg_win / avg_loss
        } else if avg_win > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let loss_rate = if total_trades > 0 {
            losing_trades as f64 / total_trades as f64
        } else {
            0.0
        };
        let expectancy = (win_rate * avg_win) - (loss_rate * avg_loss);

        let largest_win = trades
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .fold(0.0_f64, f64::max);

        let largest_loss = trades
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .fold(0.0_f64, f64::max);

        Self {
            total_trades,
            winning_trades,
            losing_trades,
            breakeven_trades,
            total_pnl,
            gross_profit,
            gross_loss,
            win_rate,
            profit_factor,
            avg_win,
            avg_loss,
            win_loss_ratio,
            expectancy,
            largest_win,
            largest_loss,
        }
    }

    /// Compute statistics from an owned slice (convenience wrapper).
    pub fn from_trade_slice(trades: &[TradeResult]) -> Self {
        let refs: Vec<&TradeResult> = trades.iter().collect();
        Self::from_trades(&refs)
    }
}

// ─── StreakStats ─────────────────────────────────────────────────────────────

/// Consecutive win/loss streak analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreakStats {
    /// Maximum consecutive winning trades.
    pub max_consecutive_wins: u32,
    /// Maximum consecutive losing trades.
    pub max_consecutive_losses: u32,
    /// Current streak (positive = wins, negative = losses, 0 = no trades or breakeven).
    pub current_streak: i32,
}

impl StreakStats {
    /// Compute streak statistics from a chronologically-ordered trade log.
    pub fn from_trades(trades: &[&TradeResult]) -> Self {
        let mut current_streak: i32 = 0;
        let mut max_wins: u32 = 0;
        let mut max_losses: u32 = 0;

        for trade in trades {
            if trade.pnl > 0.0 {
                if current_streak > 0 {
                    current_streak += 1;
                } else {
                    current_streak = 1;
                }
                max_wins = max_wins.max(current_streak as u32);
            } else if trade.pnl < 0.0 {
                if current_streak < 0 {
                    current_streak -= 1;
                } else {
                    current_streak = -1;
                }
                max_losses = max_losses.max(current_streak.unsigned_abs());
            } else {
                // Breakeven resets streak.
                current_streak = 0;
            }
        }

        Self {
            max_consecutive_wins: max_wins,
            max_consecutive_losses: max_losses,
            current_streak,
        }
    }
}

// ─── RiskMetrics ────────────────────────────────────────────────────────────

/// Risk-adjusted return metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    /// Annualized Sharpe ratio. `None` if fewer than 2 trades.
    pub sharpe_ratio: Option<f64>,
    /// Annualized Sortino ratio (penalizes only downside). `None` if fewer than 2 trades or no downside.
    pub sortino_ratio: Option<f64>,
    /// Calmar ratio: annualized_return / max_drawdown_pct. `None` if max_drawdown is zero or duration < 1 day.
    pub calmar_ratio: Option<f64>,
    /// total_pnl / max_drawdown. `None` if max_drawdown is zero.
    pub return_on_max_drawdown: Option<f64>,
    /// Largest peak-to-trough drawdown in absolute terms.
    pub max_drawdown: f64,
    /// Largest peak-to-trough drawdown as percentage of peak.
    pub max_drawdown_pct: f64,
    /// Duration of the longest drawdown period (seconds).
    pub max_drawdown_duration_secs: Option<i64>,
}

impl RiskMetrics {
    /// Compute risk metrics from the trade log, equity curve, and drawdown values.
    fn compute(
        trade_log: &[TradeResult],
        initial_balance: f64,
        max_drawdown: f64,
        max_drawdown_pct: f64,
        equity_curve: &[(NaiveDateTime, f64)],
        total_pnl: f64,
    ) -> Self {
        let return_on_max_drawdown = if max_drawdown > 0.0 {
            Some(total_pnl / max_drawdown)
        } else {
            None
        };

        // Compute per-trade returns (relative to balance before the trade).
        let mut balance = initial_balance;
        let mut returns = Vec::with_capacity(trade_log.len());
        for trade in trade_log {
            let ret = if balance.abs() > f64::EPSILON {
                trade.pnl / balance
            } else {
                0.0
            };
            returns.push(ret);
            balance += trade.pnl;
        }

        let sharpe_ratio = compute_sharpe(&returns, trade_log);
        let sortino_ratio = compute_sortino(&returns, trade_log);
        let calmar_ratio = compute_calmar(trade_log, initial_balance, total_pnl, max_drawdown_pct);

        // Max drawdown duration.
        let max_drawdown_duration_secs = compute_max_dd_duration(equity_curve, initial_balance);

        Self {
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            return_on_max_drawdown,
            max_drawdown,
            max_drawdown_pct,
            max_drawdown_duration_secs,
        }
    }
}

/// Annualized Sharpe: mean(returns) / std(returns) * sqrt(trades_per_year).
fn compute_sharpe(returns: &[f64], trade_log: &[TradeResult]) -> Option<f64> {
    if returns.len() < 2 {
        return None;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let std_dev = variance.sqrt();
    if std_dev < f64::EPSILON {
        return None;
    }
    let trades_per_year = annualization_factor(trade_log)?;
    Some((mean / std_dev) * trades_per_year.sqrt())
}

/// Annualized Sortino: mean(returns) / downside_dev * sqrt(trades_per_year).
fn compute_sortino(returns: &[f64], trade_log: &[TradeResult]) -> Option<f64> {
    if returns.len() < 2 {
        return None;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let downside_sq_sum: f64 = returns
        .iter()
        .filter(|&&r| r < 0.0)
        .map(|r| r.powi(2))
        .sum();
    let downside_count = returns.iter().filter(|&&r| r < 0.0).count();
    if downside_count == 0 {
        return None; // No downside deviation — Sortino undefined.
    }
    let downside_dev = (downside_sq_sum / n).sqrt();
    if downside_dev < f64::EPSILON {
        return None;
    }
    let trades_per_year = annualization_factor(trade_log)?;
    Some((mean / downside_dev) * trades_per_year.sqrt())
}

/// Calmar: annualized_return / max_drawdown_pct.
fn compute_calmar(
    trade_log: &[TradeResult],
    initial_balance: f64,
    total_pnl: f64,
    max_drawdown_pct: f64,
) -> Option<f64> {
    if trade_log.len() < 2 || max_drawdown_pct < f64::EPSILON {
        return None;
    }
    let first_ts = trade_log.first()?.open_ts;
    let last_ts = trade_log.last()?.close_ts;
    let duration = last_ts - first_ts;
    let days = duration.num_seconds() as f64 / 86400.0;
    if days < 1.0 {
        return None;
    }
    let years = days / 365.25;
    let annualized_return = (total_pnl / initial_balance) / years;
    Some(annualized_return / max_drawdown_pct)
}

/// Estimate trades per year from the backtest span and trade count.
fn annualization_factor(trade_log: &[TradeResult]) -> Option<f64> {
    if trade_log.len() < 2 {
        return None;
    }
    let first_ts = trade_log.first()?.open_ts;
    let last_ts = trade_log.last()?.close_ts;
    let duration = last_ts - first_ts;
    let days = duration.num_seconds() as f64 / 86400.0;
    if days < f64::EPSILON {
        return None;
    }
    Some(trade_log.len() as f64 / (days / 365.25))
}

/// Compute max drawdown duration from the equity curve.
fn compute_max_dd_duration(
    equity_curve: &[(NaiveDateTime, f64)],
    initial_balance: f64,
) -> Option<i64> {
    if equity_curve.is_empty() {
        return None;
    }

    let mut peak = initial_balance;
    let mut peak_ts = equity_curve[0].0;
    let mut max_dd_dur_secs: i64 = 0;

    for &(ts, bal) in equity_curve {
        if bal >= peak {
            // Recovered or new peak — measure duration of the drawdown that just ended.
            let dur = (ts - peak_ts).num_seconds();
            if dur > max_dd_dur_secs {
                max_dd_dur_secs = dur;
            }
            peak = bal;
            peak_ts = ts;
        }
    }

    // Check unrecovered drawdown at end.
    if let Some(&(last_ts, last_bal)) = equity_curve.last() {
        if last_bal < peak {
            let dur = (last_ts - peak_ts).num_seconds();
            if dur > max_dd_dur_secs {
                max_dd_dur_secs = dur;
            }
        }
    }

    if max_dd_dur_secs > 0 {
        Some(max_dd_dur_secs)
    } else {
        None
    }
}

// ─── DurationStats ──────────────────────────────────────────────────────────

/// Trade holding time statistics (all values in seconds).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationStats {
    /// Average holding time across all trades (seconds).
    pub avg_duration_secs: i64,
    /// Shortest trade duration (seconds).
    pub min_duration_secs: i64,
    /// Longest trade duration (seconds).
    pub max_duration_secs: i64,
    /// Average holding time for winning trades (seconds).
    pub avg_winner_duration_secs: i64,
    /// Average holding time for losing trades (seconds).
    pub avg_loser_duration_secs: i64,
}

impl DurationStats {
    /// Compute duration statistics from trades. Returns `None` if no trades.
    pub fn from_trades(trades: &[&TradeResult]) -> Option<Self> {
        if trades.is_empty() {
            return None;
        }

        let durations: Vec<i64> = trades
            .iter()
            .map(|t| (t.close_ts - t.open_ts).num_seconds())
            .collect();

        let total: i64 = durations.iter().sum();
        let avg_duration_secs = total / durations.len() as i64;
        let min_duration_secs = *durations.iter().min().unwrap();
        let max_duration_secs = *durations.iter().max().unwrap();

        let winner_durations: Vec<i64> = trades
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| (t.close_ts - t.open_ts).num_seconds())
            .collect();
        let avg_winner_duration_secs = if winner_durations.is_empty() {
            0
        } else {
            winner_durations.iter().sum::<i64>() / winner_durations.len() as i64
        };

        let loser_durations: Vec<i64> = trades
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| (t.close_ts - t.open_ts).num_seconds())
            .collect();
        let avg_loser_duration_secs = if loser_durations.is_empty() {
            0
        } else {
            loser_durations.iter().sum::<i64>() / loser_durations.len() as i64
        };

        Some(Self {
            avg_duration_secs,
            min_duration_secs,
            max_duration_secs,
            avg_winner_duration_secs,
            avg_loser_duration_secs,
        })
    }
}

// ─── MonthlyReturn ──────────────────────────────────────────────────────────

/// P&L summary for one calendar month.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyReturn {
    /// Year (e.g. 2026).
    pub year: i32,
    /// Month (1–12).
    pub month: u32,
    /// Sum of P&L for trades closed in this month.
    pub pnl: f64,
    /// Number of trades closed in this month.
    pub trade_count: usize,
    /// Balance at end of month.
    pub ending_balance: f64,
}

/// Compute monthly returns from a chronologically-ordered trade log.
fn compute_monthly_returns(trade_log: &[TradeResult], initial_balance: f64) -> Vec<MonthlyReturn> {
    if trade_log.is_empty() {
        return Vec::new();
    }

    // Group by (year, month).
    let mut groups: Vec<((i32, u32), Vec<&TradeResult>)> = Vec::new();
    for trade in trade_log {
        let key = (trade.close_ts.date().year(), trade.close_ts.date().month());
        if let Some(last) = groups.last_mut() {
            if last.0 == key {
                last.1.push(trade);
                continue;
            }
        }
        groups.push((key, vec![trade]));
    }

    let mut balance = initial_balance;
    groups
        .into_iter()
        .map(|((year, month), trades)| {
            let pnl: f64 = trades.iter().map(|t| t.pnl).sum();
            let trade_count = trades.len();
            balance += pnl;
            MonthlyReturn {
                year,
                month,
                pnl,
                trade_count,
                ending_balance: balance,
            }
        })
        .collect()
}

// We need chrono's Datelike for year()/month().
use chrono::Datelike;

// ─── PositionSummary ────────────────────────────────────────────────────────

/// Aggregated result for one position across all its close events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    pub position_id: PositionId,
    pub symbol: String,
    pub side: Side,
    pub group: Option<GroupId>,
    /// Entry price (from the first close event — all share the same entry).
    pub entry_price: f64,
    /// Size-weighted average exit price across all closes.
    pub avg_exit_price: f64,
    /// Total size across all close events.
    pub original_size: f64,
    /// Number of close events (partial + final).
    pub close_count: usize,
    /// Net P&L across all close events.
    pub net_pnl: f64,
    /// Ordered list of close reasons.
    pub close_reasons: Vec<CloseReason>,
    /// When the position was opened.
    pub open_ts: NaiveDateTime,
    /// When the last close event occurred.
    pub final_close_ts: NaiveDateTime,
    /// Total holding duration in seconds.
    pub duration_seconds: i64,
}

impl PositionSummary {
    /// Build a summary from all trade results for one position.
    pub fn from_trades(trades: &[&TradeResult]) -> Self {
        assert!(
            !trades.is_empty(),
            "PositionSummary requires at least one trade"
        );

        let first = trades[0];
        let net_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let original_size: f64 = trades.iter().map(|t| t.size).sum();

        let avg_exit_price = if original_size > 0.0 {
            trades.iter().map(|t| t.exit_price * t.size).sum::<f64>() / original_size
        } else {
            0.0
        };

        let final_close_ts = trades.iter().map(|t| t.close_ts).max().unwrap();
        let close_reasons: Vec<CloseReason> = trades.iter().map(|t| t.close_reason).collect();

        Self {
            position_id: first.position_id.clone(),
            symbol: first.symbol.clone(),
            side: first.side,
            group: first.group.clone(),
            entry_price: first.entry_price,
            avg_exit_price,
            original_size,
            close_count: trades.len(),
            net_pnl,
            close_reasons,
            open_ts: first.open_ts,
            final_close_ts,
            duration_seconds: (final_close_ts - first.open_ts).num_seconds(),
        }
    }

    /// Net position result: positive P&L.
    pub fn is_winner(&self) -> bool {
        self.net_pnl > 0.0
    }

    /// Net position result: negative P&L.
    pub fn is_loser(&self) -> bool {
        self.net_pnl < 0.0
    }
}

// ─── CloseReasonStats ───────────────────────────────────────────────────────

/// Statistics for one close reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseReasonStats {
    /// The close reason.
    pub reason: CloseReason,
    /// How many trades closed for this reason.
    pub count: usize,
    /// Sum of P&L for trades with this reason.
    pub total_pnl: f64,
    /// Average P&L per trade for this reason.
    pub avg_pnl: f64,
    /// Fraction of all trades that closed for this reason.
    pub percentage: f64,
}

/// Compute per-close-reason statistics.
fn compute_close_reason_stats(trade_log: &[TradeResult]) -> Vec<CloseReasonStats> {
    if trade_log.is_empty() {
        return Vec::new();
    }

    let total_count = trade_log.len();
    let mut by_reason: HashMap<CloseReason, Vec<f64>> = HashMap::new();
    for trade in trade_log {
        by_reason
            .entry(trade.close_reason)
            .or_default()
            .push(trade.pnl);
    }

    let mut stats: Vec<CloseReasonStats> = by_reason
        .into_iter()
        .map(|(reason, pnls)| {
            let count = pnls.len();
            let total_pnl: f64 = pnls.iter().sum();
            CloseReasonStats {
                reason,
                count,
                total_pnl,
                avg_pnl: total_pnl / count as f64,
                percentage: count as f64 / total_count as f64,
            }
        })
        .collect();

    // Sort by count descending.
    stats.sort_by(|a, b| b.count.cmp(&a.count));
    stats
}

// ─── BacktestResult ─────────────────────────────────────────────────────────

/// Aggregate backtest statistics produced by [`BacktestRunner`](crate::runner::BacktestRunner).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    // ── Existing fields (preserved for backward compatibility) ───────
    /// Starting account balance.
    pub initial_balance: f64,
    /// Final account balance (initial + total realized P&L).
    pub final_balance: f64,
    /// Sum of all realized P&L.
    pub total_pnl: f64,
    /// Number of completed trades (full + partial closes).
    pub total_trades: usize,
    /// Number of trades with positive P&L.
    pub winning_trades: usize,
    /// Number of trades with negative P&L.
    pub losing_trades: usize,
    /// `winning_trades / total_trades` (0.0 if no trades).
    pub win_rate: f64,
    /// Largest peak-to-trough drawdown in absolute terms.
    pub max_drawdown: f64,
    /// Largest peak-to-trough drawdown as a percentage of the peak.
    pub max_drawdown_pct: f64,
    /// Sum of winning P&L / abs(sum of losing P&L). `f64::INFINITY` if no losers.
    #[serde(with = "finite_f64")]
    pub profit_factor: f64,
    /// Equity value at each trade close: `(timestamp, balance)`.
    pub equity_curve: Vec<(NaiveDateTime, f64)>,
    /// Full trade log (one entry per close event).
    pub trade_log: Vec<TradeResult>,

    /// Full aggregate stats in SubsetStats form.
    pub summary: SubsetStats,

    /// Stats broken down by symbol.
    pub per_symbol: HashMap<String, SubsetStats>,

    /// Stats broken down by group (empty if no positions were grouped).
    pub per_group: HashMap<GroupId, SubsetStats>,

    /// Stats for long (Buy) trades.
    pub long_stats: SubsetStats,
    /// Stats for short (Sell) trades.
    pub short_stats: SubsetStats,

    /// Breakdown by close reason, sorted by count descending.
    pub per_close_reason: Vec<CloseReasonStats>,

    /// Consecutive win/loss streak analysis.
    pub streaks: StreakStats,

    /// Risk-adjusted return metrics (Sharpe, Sortino, Calmar, drawdown duration).
    pub risk_metrics: RiskMetrics,

    /// Trade holding time statistics. `None` if no trades.
    pub duration_stats: Option<DurationStats>,

    /// Monthly P&L breakdown.
    pub monthly_returns: Vec<MonthlyReturn>,

    // ── Per-position aggregation ────────────────────────────────────
    /// Per-position summaries (all close events for one position aggregated).
    pub positions: Vec<PositionSummary>,

    /// Number of unique positions.
    pub total_positions: usize,
    /// Positions with net_pnl > 0.
    pub winning_positions: usize,
    /// Positions with net_pnl < 0.
    pub losing_positions: usize,
    /// Position-level win rate: winning_positions / total_positions.
    pub position_win_rate: f64,
}

impl BacktestResult {
    /// Build aggregate statistics from a trade log.
    pub fn from_trade_log(initial_balance: f64, trade_log: Vec<TradeResult>) -> Self {
        let total_pnl: f64 = trade_log.iter().map(|t| t.pnl).sum();
        let final_balance = initial_balance + total_pnl;
        let total_trades = trade_log.len();

        let winning_trades = trade_log.iter().filter(|t| t.pnl > 0.0).count();
        let losing_trades = trade_log.iter().filter(|t| t.pnl < 0.0).count();

        let win_rate = if total_trades > 0 {
            winning_trades as f64 / total_trades as f64
        } else {
            0.0
        };

        let gross_profit: f64 = trade_log
            .iter()
            .filter(|t| t.pnl > 0.0)
            .map(|t| t.pnl)
            .sum();
        let gross_loss: f64 = trade_log
            .iter()
            .filter(|t| t.pnl < 0.0)
            .map(|t| t.pnl.abs())
            .sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Build equity curve and compute max drawdown.
        let mut balance = initial_balance;
        let mut equity_curve = Vec::with_capacity(trade_log.len());
        let mut peak = initial_balance;
        let mut max_drawdown = 0.0_f64;
        let mut max_drawdown_pct = 0.0_f64;

        for trade in &trade_log {
            balance += trade.pnl;
            equity_curve.push((trade.close_ts, balance));

            if balance > peak {
                peak = balance;
            }
            let dd = peak - balance;
            if dd > max_drawdown {
                max_drawdown = dd;
            }
            let dd_pct = if peak > 0.0 { dd / peak } else { 0.0 };
            if dd_pct > max_drawdown_pct {
                max_drawdown_pct = dd_pct;
            }
        }

        // ── SubsetStats (overall summary) ───────────────────────────
        let all_refs: Vec<&TradeResult> = trade_log.iter().collect();
        let summary = SubsetStats::from_trades(&all_refs);

        // ── Per-symbol breakdown ────────────────────────────────────
        let mut by_symbol: HashMap<String, Vec<&TradeResult>> = HashMap::new();
        for trade in &trade_log {
            by_symbol
                .entry(trade.symbol.clone())
                .or_default()
                .push(trade);
        }
        let per_symbol: HashMap<String, SubsetStats> = by_symbol
            .iter()
            .map(|(sym, trades)| (sym.clone(), SubsetStats::from_trades(trades)))
            .collect();

        // ── Per-group breakdown ─────────────────────────────────────
        let mut by_group: HashMap<GroupId, Vec<&TradeResult>> = HashMap::new();
        for trade in &trade_log {
            if let Some(ref g) = trade.group {
                by_group.entry(g.clone()).or_default().push(trade);
            }
        }
        let per_group: HashMap<GroupId, SubsetStats> = by_group
            .iter()
            .map(|(g, trades)| (g.clone(), SubsetStats::from_trades(trades)))
            .collect();

        // ── Per-side breakdown ──────────────────────────────────────
        let longs: Vec<&TradeResult> = trade_log.iter().filter(|t| t.side == Side::Buy).collect();
        let shorts: Vec<&TradeResult> = trade_log.iter().filter(|t| t.side == Side::Sell).collect();
        let long_stats = SubsetStats::from_trades(&longs);
        let short_stats = SubsetStats::from_trades(&shorts);

        // ── Per-close-reason breakdown ──────────────────────────────
        let per_close_reason = compute_close_reason_stats(&trade_log);

        // ── Streak analysis ─────────────────────────────────────────
        let streaks = StreakStats::from_trades(&all_refs);

        // ── Risk metrics ────────────────────────────────────────────
        let risk_metrics = RiskMetrics::compute(
            &trade_log,
            initial_balance,
            max_drawdown,
            max_drawdown_pct,
            &equity_curve,
            total_pnl,
        );

        // ── Duration stats ──────────────────────────────────────────
        let duration_stats = DurationStats::from_trades(&all_refs);

        // ── Monthly returns ─────────────────────────────────────────
        let monthly_returns = compute_monthly_returns(&trade_log, initial_balance);

        // ── Position summaries ──────────────────────────────────────
        let mut by_position: HashMap<PositionId, Vec<&TradeResult>> = HashMap::new();
        for trade in &trade_log {
            by_position
                .entry(trade.position_id.clone())
                .or_default()
                .push(trade);
        }
        let mut positions: Vec<PositionSummary> = by_position
            .values()
            .map(|trades| PositionSummary::from_trades(trades))
            .collect();
        // Sort by open time for deterministic output.
        positions.sort_by_key(|p| p.open_ts);

        let total_positions = positions.len();
        let winning_positions = positions.iter().filter(|p| p.is_winner()).count();
        let losing_positions = positions.iter().filter(|p| p.is_loser()).count();
        let position_win_rate = if total_positions > 0 {
            winning_positions as f64 / total_positions as f64
        } else {
            0.0
        };

        Self {
            initial_balance,
            final_balance,
            total_pnl,
            total_trades,
            winning_trades,
            losing_trades,
            win_rate,
            max_drawdown,
            max_drawdown_pct,
            profit_factor,
            equity_curve,
            trade_log,
            summary,
            per_symbol,
            per_group,
            long_stats,
            short_stats,
            per_close_reason,
            streaks,
            risk_metrics,
            duration_stats,
            monthly_returns,
            positions,
            total_positions,
            winning_positions,
            losing_positions,
            position_win_rate,
        }
    }
}

// ─── Display ────────────────────────────────────────────────────────────────

/// Format duration seconds into a human-readable string (e.g. "3d 14h 5m").
fn fmt_duration(secs: i64) -> String {
    if secs < 0 {
        return format!("-{}", fmt_duration(-secs));
    }
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else {
        format!("{}m", minutes)
    }
}

/// Format a SubsetStats one-line summary for breakdown sections.
fn fmt_subset_line(label: &str, stats: &SubsetStats) -> String {
    format!(
        "{:<14}: {} trades, P&L: {:+.2}, WR: {:.1}%, PF: {:.2}",
        label,
        stats.total_trades,
        stats.total_pnl,
        stats.win_rate * 100.0,
        stats.profit_factor,
    )
}

impl std::fmt::Display for BacktestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let be = self.summary.breakeven_trades;

        writeln!(f, "═══ Backtest Result ═══")?;
        writeln!(
            f,
            "Balance      : {:.2} -> {:.2}",
            self.initial_balance, self.final_balance
        )?;
        writeln!(f, "Total P&L    : {:+.2}", self.total_pnl)?;
        writeln!(f, "Trades       : {}", self.total_trades)?;

        if be > 0 {
            writeln!(
                f,
                "Win / Lose   : {} / {} / {} (BE)",
                self.winning_trades, self.losing_trades, be
            )?;
        } else {
            writeln!(
                f,
                "Win / Lose   : {} / {}",
                self.winning_trades, self.losing_trades
            )?;
        }

        writeln!(f, "Win Rate     : {:.1}%", self.win_rate * 100.0)?;
        writeln!(f, "Profit Factor: {:.2}", self.profit_factor)?;
        writeln!(f, "Expectancy   : {:.2} per trade", self.summary.expectancy)?;

        // ── Risk Metrics ────────────────────────────────────────────
        writeln!(f)?;
        writeln!(f, "-- Risk Metrics --")?;

        match self.risk_metrics.sharpe_ratio {
            Some(v) => writeln!(f, "Sharpe Ratio       : {:.2}", v)?,
            None => writeln!(f, "Sharpe Ratio       : N/A")?,
        }
        match self.risk_metrics.sortino_ratio {
            Some(v) => writeln!(f, "Sortino Ratio      : {:.2}", v)?,
            None => writeln!(f, "Sortino Ratio      : N/A")?,
        }
        match self.risk_metrics.calmar_ratio {
            Some(v) => writeln!(f, "Calmar Ratio       : {:.2}", v)?,
            None => writeln!(f, "Calmar Ratio       : N/A")?,
        }
        writeln!(
            f,
            "Max Drawdown       : {:.2} ({:.1}%)",
            self.max_drawdown,
            self.max_drawdown_pct * 100.0
        )?;
        match self.risk_metrics.max_drawdown_duration_secs {
            Some(s) => writeln!(f, "Max DD Duration    : {}", fmt_duration(s))?,
            None => writeln!(f, "Max DD Duration    : N/A")?,
        }
        match self.risk_metrics.return_on_max_drawdown {
            Some(v) => writeln!(f, "Return / Max DD    : {:.2}", v)?,
            None => writeln!(f, "Return / Max DD    : N/A")?,
        }

        // ── Win / Loss Analysis ─────────────────────────────────────
        writeln!(f)?;
        writeln!(f, "-- Win / Loss Analysis --")?;
        writeln!(
            f,
            "Avg Win    : {:.2}    Largest Win  : {:.2}",
            self.summary.avg_win, self.summary.largest_win
        )?;
        writeln!(
            f,
            "Avg Loss   : {:.2}    Largest Loss : {:.2}",
            self.summary.avg_loss, self.summary.largest_loss
        )?;
        writeln!(
            f,
            "Win/Loss   : {:.2}     Expectancy   : {:.2}",
            self.summary.win_loss_ratio, self.summary.expectancy
        )?;
        writeln!(
            f,
            "Max Consec Wins  : {}",
            self.streaks.max_consecutive_wins
        )?;
        writeln!(
            f,
            "Max Consec Losses: {}",
            self.streaks.max_consecutive_losses
        )?;

        // ── Side Breakdown ──────────────────────────────────────────
        writeln!(f)?;
        writeln!(f, "-- Side Breakdown --")?;
        writeln!(f, "{}", fmt_subset_line("Long", &self.long_stats))?;
        writeln!(f, "{}", fmt_subset_line("Short", &self.short_stats))?;

        // ── Symbol Breakdown ────────────────────────────────────────
        if !self.per_symbol.is_empty() {
            writeln!(f)?;
            writeln!(f, "-- Symbol Breakdown --")?;
            let mut symbols: Vec<_> = self.per_symbol.iter().collect();
            symbols.sort_by(|a, b| {
                b.1.total_trades
                    .cmp(&a.1.total_trades)
                    .then_with(|| a.0.cmp(b.0))
            });
            for (sym, stats) in &symbols {
                writeln!(f, "{}", fmt_subset_line(sym, stats))?;
            }
        }

        // ── Group Breakdown ─────────────────────────────────────────
        if !self.per_group.is_empty() {
            writeln!(f)?;
            writeln!(f, "-- Group Breakdown --")?;
            let mut groups: Vec<_> = self.per_group.iter().collect();
            groups.sort_by(|a, b| {
                b.1.total_trades
                    .cmp(&a.1.total_trades)
                    .then_with(|| a.0.cmp(b.0))
            });
            for (grp, stats) in &groups {
                writeln!(f, "{}", fmt_subset_line(grp, stats))?;
            }
        }

        // ── Close Reasons ───────────────────────────────────────────
        if !self.per_close_reason.is_empty() {
            writeln!(f)?;
            writeln!(f, "-- Close Reasons --")?;
            for cr in &self.per_close_reason {
                writeln!(
                    f,
                    "{:<14}: {:>3} ({:>4.1}%), P&L: {:+.2}",
                    cr.reason.to_string(),
                    cr.count,
                    cr.percentage * 100.0,
                    cr.total_pnl,
                )?;
            }
        }

        // ── Duration ────────────────────────────────────────────────
        if let Some(ref ds) = self.duration_stats {
            writeln!(f)?;
            writeln!(f, "-- Duration --")?;
            writeln!(
                f,
                "Avg Duration     : {}",
                fmt_duration(ds.avg_duration_secs)
            )?;
            writeln!(
                f,
                "Avg Winner Dur   : {}",
                fmt_duration(ds.avg_winner_duration_secs)
            )?;
            writeln!(
                f,
                "Avg Loser Dur    : {}",
                fmt_duration(ds.avg_loser_duration_secs)
            )?;
            writeln!(
                f,
                "Shortest         : {}",
                fmt_duration(ds.min_duration_secs)
            )?;
            writeln!(
                f,
                "Longest          : {}",
                fmt_duration(ds.max_duration_secs)
            )?;
        }

        // ── Monthly Returns ─────────────────────────────────────────
        if !self.monthly_returns.is_empty() {
            writeln!(f)?;
            writeln!(f, "-- Monthly Returns --")?;
            for mr in &self.monthly_returns {
                writeln!(
                    f,
                    "{:04}-{:02} : {:+.2} ({} trades)",
                    mr.year, mr.month, mr.pnl, mr.trade_count,
                )?;
            }
        }

        // ── Position Summary ────────────────────────────────────────
        if self.total_positions > 0 {
            writeln!(f)?;
            writeln!(f, "-- Position Summary --")?;
            writeln!(f, "Total Positions  : {}", self.total_positions)?;
            writeln!(
                f,
                "Win / Lose       : {} / {}",
                self.winning_positions, self.losing_positions
            )?;
            writeln!(
                f,
                "Position WR      : {:.1}%",
                self.position_win_rate * 100.0
            )?;
        }

        Ok(())
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(year: i32, month: u32, day: u32, h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn ts_hms(h: u32, m: u32, s: u32) -> NaiveDateTime {
        ts(2026, 1, 1, h, m, s)
    }

    fn make_trade(pnl: f64, close_h: u32) -> TradeResult {
        TradeResult {
            position_id: "p1".into(),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            entry_price: 1.0850,
            exit_price: 1.0850 + pnl,
            size: 1.0,
            pnl,
            open_ts: ts_hms(10, 0, 0),
            close_ts: ts_hms(close_h, 0, 0),
            close_reason: if pnl > 0.0 {
                CloseReason::Target
            } else if pnl < 0.0 {
                CloseReason::Stoploss
            } else {
                CloseReason::Manual
            },
            group: None,
        }
    }

    fn make_trade_full(
        pos_id: &str,
        symbol: &str,
        side: Side,
        pnl: f64,
        open_ts: NaiveDateTime,
        close_ts: NaiveDateTime,
        reason: CloseReason,
        group: Option<GroupId>,
    ) -> TradeResult {
        TradeResult {
            position_id: pos_id.into(),
            symbol: symbol.into(),
            side,
            entry_price: 1.0850,
            exit_price: 1.0850 + pnl,
            size: 1.0,
            pnl,
            open_ts,
            close_ts,
            close_reason: reason,
            group,
        }
    }

    // ── Backward compatibility tests (existing, preserved) ──────────

    #[test]
    fn empty_trade_log() {
        let result = BacktestResult::from_trade_log(10_000.0, vec![]);
        assert_eq!(result.total_trades, 0);
        assert!((result.final_balance - 10_000.0).abs() < f64::EPSILON);
        assert!((result.win_rate - 0.0).abs() < f64::EPSILON);
        assert!((result.max_drawdown - 0.0).abs() < f64::EPSILON);
        // Enhanced fields on empty.
        assert_eq!(result.summary.total_trades, 0);
        assert!(result.duration_stats.is_none());
        assert!(result.monthly_returns.is_empty());
        assert_eq!(result.total_positions, 0);
        assert!((result.position_win_rate - 0.0).abs() < f64::EPSILON);
        assert_eq!(result.streaks.max_consecutive_wins, 0);
        assert_eq!(result.streaks.max_consecutive_losses, 0);
        assert!(result.risk_metrics.sharpe_ratio.is_none());
    }

    #[test]
    fn basic_stats() {
        let trades = vec![
            make_trade(100.0, 11),
            make_trade(-50.0, 12),
            make_trade(200.0, 13),
            make_trade(-30.0, 14),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert_eq!(result.total_trades, 4);
        assert_eq!(result.winning_trades, 2);
        assert_eq!(result.losing_trades, 2);
        assert!((result.total_pnl - 220.0).abs() < f64::EPSILON);
        assert!((result.final_balance - 10_220.0).abs() < f64::EPSILON);
        assert!((result.win_rate - 0.5).abs() < f64::EPSILON);
        assert!((result.profit_factor - 3.75).abs() < f64::EPSILON);
    }

    #[test]
    fn drawdown_calculation() {
        let trades = vec![
            make_trade(100.0, 11),
            make_trade(-200.0, 12),
            make_trade(50.0, 13),
            make_trade(-100.0, 14),
            make_trade(500.0, 15),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!((result.max_drawdown - 250.0).abs() < f64::EPSILON);
        assert_eq!(result.equity_curve.len(), 5);
    }

    #[test]
    fn all_winners() {
        let trades = vec![make_trade(100.0, 11), make_trade(200.0, 12)];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!((result.win_rate - 1.0).abs() < f64::EPSILON);
        assert!(result.profit_factor.is_infinite());
        assert!((result.max_drawdown - 0.0).abs() < f64::EPSILON);
    }

    // ── SubsetStats tests ───────────────────────────────────────────

    #[test]
    fn subset_stats_basic() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(-50.0, 12);
        let t3 = make_trade(200.0, 13);
        let t4 = make_trade(-30.0, 14);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4];

        let s = SubsetStats::from_trades(&refs);
        assert_eq!(s.total_trades, 4);
        assert_eq!(s.winning_trades, 2);
        assert_eq!(s.losing_trades, 2);
        assert_eq!(s.breakeven_trades, 0);
        assert!((s.total_pnl - 220.0).abs() < f64::EPSILON);
        assert!((s.gross_profit - 300.0).abs() < f64::EPSILON);
        assert!((s.gross_loss - 80.0).abs() < f64::EPSILON);
        assert!((s.win_rate - 0.5).abs() < f64::EPSILON);
        assert!((s.profit_factor - 3.75).abs() < f64::EPSILON);
        assert!((s.avg_win - 150.0).abs() < f64::EPSILON);
        assert!((s.avg_loss - 40.0).abs() < f64::EPSILON);
        assert!((s.win_loss_ratio - 3.75).abs() < f64::EPSILON);
        // expectancy = 0.5*150 - 0.5*40 = 55
        assert!((s.expectancy - 55.0).abs() < f64::EPSILON);
    }

    #[test]
    fn subset_stats_all_winners() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(200.0, 12);
        let refs: Vec<&TradeResult> = vec![&t1, &t2];

        let s = SubsetStats::from_trades(&refs);
        assert_eq!(s.losing_trades, 0);
        assert!((s.avg_loss - 0.0).abs() < f64::EPSILON);
        assert!(s.win_loss_ratio.is_infinite());
        assert!(s.profit_factor.is_infinite());
    }

    #[test]
    fn subset_stats_all_losers() {
        let t1 = make_trade(-100.0, 11);
        let t2 = make_trade(-200.0, 12);
        let refs: Vec<&TradeResult> = vec![&t1, &t2];

        let s = SubsetStats::from_trades(&refs);
        assert_eq!(s.winning_trades, 0);
        assert!((s.avg_win - 0.0).abs() < f64::EPSILON);
        assert!((s.win_loss_ratio - 0.0).abs() < f64::EPSILON);
        assert!((s.profit_factor - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn subset_stats_empty() {
        let s = SubsetStats::from_trades(&[]);
        assert_eq!(s.total_trades, 0);
        assert!((s.total_pnl - 0.0).abs() < f64::EPSILON);
        assert!((s.win_rate - 0.0).abs() < f64::EPSILON);
        assert!((s.expectancy - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn subset_stats_largest_win_loss() {
        let t1 = make_trade(50.0, 11);
        let t2 = make_trade(200.0, 12);
        let t3 = make_trade(-30.0, 13);
        let t4 = make_trade(-100.0, 14);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4];

        let s = SubsetStats::from_trades(&refs);
        assert!((s.largest_win - 200.0).abs() < f64::EPSILON);
        assert!((s.largest_loss - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn subset_stats_breakeven_trades() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(0.0, 12);
        let t3 = make_trade(-50.0, 13);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3];

        let s = SubsetStats::from_trades(&refs);
        assert_eq!(s.breakeven_trades, 1);
        assert_eq!(s.winning_trades, 1);
        assert_eq!(s.losing_trades, 1);
    }

    // ── StreakStats tests ───────────────────────────────────────────

    #[test]
    fn streaks_alternating() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(-50.0, 12);
        let t3 = make_trade(100.0, 13);
        let t4 = make_trade(-50.0, 14);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4];

        let s = StreakStats::from_trades(&refs);
        assert_eq!(s.max_consecutive_wins, 1);
        assert_eq!(s.max_consecutive_losses, 1);
    }

    #[test]
    fn streaks_consecutive_wins() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(50.0, 12);
        let t3 = make_trade(80.0, 13);
        let t4 = make_trade(-50.0, 14);
        let t5 = make_trade(100.0, 15);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4, &t5];

        let s = StreakStats::from_trades(&refs);
        assert_eq!(s.max_consecutive_wins, 3);
        assert_eq!(s.max_consecutive_losses, 1);
    }

    #[test]
    fn streaks_consecutive_losses() {
        let t1 = make_trade(-10.0, 11);
        let t2 = make_trade(-20.0, 12);
        let t3 = make_trade(-30.0, 13);
        let t4 = make_trade(-40.0, 14);
        let t5 = make_trade(100.0, 15);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4, &t5];

        let s = StreakStats::from_trades(&refs);
        assert_eq!(s.max_consecutive_wins, 1);
        assert_eq!(s.max_consecutive_losses, 4);
    }

    #[test]
    fn streaks_all_winners() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(200.0, 12);
        let t3 = make_trade(300.0, 13);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3];

        let s = StreakStats::from_trades(&refs);
        assert_eq!(s.max_consecutive_wins, 3);
        assert_eq!(s.max_consecutive_losses, 0);
        assert_eq!(s.current_streak, 3);
    }

    #[test]
    fn streaks_empty() {
        let s = StreakStats::from_trades(&[]);
        assert_eq!(s.max_consecutive_wins, 0);
        assert_eq!(s.max_consecutive_losses, 0);
        assert_eq!(s.current_streak, 0);
    }

    #[test]
    fn streaks_breakeven_resets() {
        let t1 = make_trade(100.0, 11);
        let t2 = make_trade(200.0, 12);
        let t3 = make_trade(0.0, 13); // breakeven resets
        let t4 = make_trade(100.0, 14);
        let refs: Vec<&TradeResult> = vec![&t1, &t2, &t3, &t4];

        let s = StreakStats::from_trades(&refs);
        assert_eq!(s.max_consecutive_wins, 2); // not 3
        assert_eq!(s.current_streak, 1);
    }

    // ── RiskMetrics tests ───────────────────────────────────────────

    #[test]
    fn sharpe_ratio_positive() {
        // Consistent small wins should produce positive Sharpe.
        let trades: Vec<TradeResult> = (0..20)
            .map(|i| {
                make_trade_full(
                    &format!("p{}", i),
                    "EURUSD",
                    Side::Buy,
                    10.0 + (i as f64),
                    ts(2026, 1, 1, 10, 0, 0),
                    ts(2026, 1, 1 + (i as u32 / 5), 11 + (i as u32 % 12), 0, 0),
                    CloseReason::Target,
                    None,
                )
            })
            .collect();
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!(result.risk_metrics.sharpe_ratio.is_some());
        assert!(result.risk_metrics.sharpe_ratio.unwrap() > 0.0);
    }

    #[test]
    fn sharpe_ratio_insufficient_data() {
        let trades = vec![make_trade(100.0, 11)];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!(result.risk_metrics.sharpe_ratio.is_none());
    }

    #[test]
    fn sortino_ratio_no_downside() {
        let trades = vec![make_trade(100.0, 11), make_trade(200.0, 12)];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        // No negative returns → Sortino undefined.
        assert!(result.risk_metrics.sortino_ratio.is_none());
    }

    #[test]
    fn calmar_ratio_zero_drawdown() {
        let trades = vec![make_trade(100.0, 11), make_trade(200.0, 12)];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        // No drawdown → Calmar undefined.
        assert!(result.risk_metrics.calmar_ratio.is_none());
    }

    #[test]
    fn max_drawdown_duration_recovered() {
        // Win, then lose (creates drawdown), then win big (recovers).
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                -200.0,
                ts(2026, 1, 1, 11, 0, 0),
                ts(2026, 1, 2, 11, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                300.0,
                ts(2026, 1, 2, 11, 0, 0),
                ts(2026, 1, 5, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        // Drawdown from day 1 11:00 (peak after first trade) to day 5 11:00 (recovery).
        assert!(result.risk_metrics.max_drawdown_duration_secs.is_some());
        let dur = result.risk_metrics.max_drawdown_duration_secs.unwrap();
        assert!(dur > 0);
    }

    #[test]
    fn max_drawdown_duration_unrecovered() {
        // Win then lose — never recovers.
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                -200.0,
                ts(2026, 1, 1, 11, 0, 0),
                ts(2026, 1, 5, 11, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!(result.risk_metrics.max_drawdown_duration_secs.is_some());
        // Duration should span from peak_ts (day 1 11:00) to last trade (day 5 11:00) = 4 days.
        let dur = result.risk_metrics.max_drawdown_duration_secs.unwrap();
        assert_eq!(dur, 4 * 86400);
    }

    // ── DurationStats tests ─────────────────────────────────────────

    #[test]
    fn duration_stats_basic() {
        let t1 = make_trade_full(
            "p1",
            "EURUSD",
            Side::Buy,
            100.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 12, 0, 0),
            CloseReason::Target,
            None,
        );
        let t2 = make_trade_full(
            "p2",
            "EURUSD",
            Side::Buy,
            -50.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 14, 0, 0),
            CloseReason::Stoploss,
            None,
        );
        let refs: Vec<&TradeResult> = vec![&t1, &t2];

        let ds = DurationStats::from_trades(&refs).unwrap();
        assert_eq!(ds.min_duration_secs, 7200); // 2h
        assert_eq!(ds.max_duration_secs, 14400); // 4h
        assert_eq!(ds.avg_duration_secs, 10800); // 3h
        assert_eq!(ds.avg_winner_duration_secs, 7200);
        assert_eq!(ds.avg_loser_duration_secs, 14400);
    }

    #[test]
    fn duration_stats_single_trade() {
        let t1 = make_trade_full(
            "p1",
            "EURUSD",
            Side::Buy,
            100.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 11, 0, 0),
            CloseReason::Target,
            None,
        );
        let refs: Vec<&TradeResult> = vec![&t1];

        let ds = DurationStats::from_trades(&refs).unwrap();
        assert_eq!(ds.avg_duration_secs, 3600);
        assert_eq!(ds.min_duration_secs, 3600);
        assert_eq!(ds.max_duration_secs, 3600);
    }

    #[test]
    fn duration_stats_empty() {
        assert!(DurationStats::from_trades(&[]).is_none());
    }

    #[test]
    fn duration_stats_winner_vs_loser() {
        // Winners held shorter, losers longer.
        let t1 = make_trade_full(
            "p1",
            "EURUSD",
            Side::Buy,
            100.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 10, 30, 0),
            CloseReason::Target,
            None,
        );
        let t2 = make_trade_full(
            "p2",
            "EURUSD",
            Side::Buy,
            -50.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 16, 0, 0),
            CloseReason::Stoploss,
            None,
        );
        let refs: Vec<&TradeResult> = vec![&t1, &t2];

        let ds = DurationStats::from_trades(&refs).unwrap();
        assert!(ds.avg_winner_duration_secs < ds.avg_loser_duration_secs);
    }

    // ── MonthlyReturn tests ─────────────────────────────────────────

    #[test]
    fn monthly_returns_single_month() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 5, 10, 0, 0),
                ts(2026, 1, 10, 10, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                -30.0,
                ts(2026, 1, 12, 10, 0, 0),
                ts(2026, 1, 15, 10, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let monthly = compute_monthly_returns(&trades, 10_000.0);
        assert_eq!(monthly.len(), 1);
        assert_eq!(monthly[0].year, 2026);
        assert_eq!(monthly[0].month, 1);
        assert!((monthly[0].pnl - 70.0).abs() < f64::EPSILON);
        assert_eq!(monthly[0].trade_count, 2);
    }

    #[test]
    fn monthly_returns_multi_month() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 5, 10, 0, 0),
                ts(2026, 1, 10, 10, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                200.0,
                ts(2026, 2, 5, 10, 0, 0),
                ts(2026, 2, 10, 10, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                -50.0,
                ts(2026, 3, 5, 10, 0, 0),
                ts(2026, 3, 10, 10, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let monthly = compute_monthly_returns(&trades, 10_000.0);
        assert_eq!(monthly.len(), 3);
        assert_eq!(monthly[0].month, 1);
        assert_eq!(monthly[1].month, 2);
        assert_eq!(monthly[2].month, 3);
    }

    #[test]
    fn monthly_returns_ending_balance() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 5, 10, 0, 0),
                ts(2026, 1, 10, 10, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                200.0,
                ts(2026, 2, 5, 10, 0, 0),
                ts(2026, 2, 10, 10, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let monthly = compute_monthly_returns(&trades, 10_000.0);
        assert!((monthly[0].ending_balance - 10_100.0).abs() < f64::EPSILON);
        assert!((monthly[1].ending_balance - 10_300.0).abs() < f64::EPSILON);
    }

    // ── Per-breakdown tests ─────────────────────────────────────────

    #[test]
    fn per_symbol_breakdown() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "XAUUSD",
                Side::Buy,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                200.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 13, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert_eq!(result.per_symbol.len(), 2);

        let eu = result.per_symbol.get("EURUSD").unwrap();
        assert_eq!(eu.total_trades, 2);
        assert!((eu.total_pnl - 300.0).abs() < f64::EPSILON);

        let xau = result.per_symbol.get("XAUUSD").unwrap();
        assert_eq!(xau.total_trades, 1);
        assert!((xau.total_pnl - -50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn per_side_breakdown() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Sell,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                200.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 13, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert_eq!(result.long_stats.total_trades, 2);
        assert_eq!(result.short_stats.total_trades, 1);
        assert!((result.long_stats.total_pnl - 300.0).abs() < f64::EPSILON);
        assert!((result.short_stats.total_pnl - -50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn per_close_reason_breakdown() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                80.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 13, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p4",
                "EURUSD",
                Side::Buy,
                30.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 14, 0, 0),
                CloseReason::TrailingStop,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        assert_eq!(result.per_close_reason.len(), 3);
        // Sorted by count descending — Target (2), then SL (1) and Trailing (1).
        assert_eq!(result.per_close_reason[0].reason, CloseReason::Target);
        assert_eq!(result.per_close_reason[0].count, 2);
        assert!((result.per_close_reason[0].percentage - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn per_group_breakdown() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                Some("momentum".into()),
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Buy,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                Some("reversion".into()),
            ),
            make_trade_full(
                "p3",
                "EURUSD",
                Side::Buy,
                200.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 13, 0, 0),
                CloseReason::Target,
                Some("momentum".into()),
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        assert_eq!(result.per_group.len(), 2);
        let mom = result.per_group.get("momentum").unwrap();
        assert_eq!(mom.total_trades, 2);
        assert!((mom.total_pnl - 300.0).abs() < f64::EPSILON);
        let rev = result.per_group.get("reversion").unwrap();
        assert_eq!(rev.total_trades, 1);
    }

    #[test]
    fn per_group_empty_when_no_groups() {
        let trades = vec![make_trade(100.0, 11), make_trade(-50.0, 12)];
        let result = BacktestResult::from_trade_log(10_000.0, trades);
        assert!(result.per_group.is_empty());
    }

    // ── PositionSummary tests ───────────────────────────────────────

    #[test]
    fn position_summary_single_close() {
        let t1 = make_trade_full(
            "p1",
            "EURUSD",
            Side::Buy,
            100.0,
            ts(2026, 1, 1, 10, 0, 0),
            ts(2026, 1, 1, 12, 0, 0),
            CloseReason::Target,
            None,
        );
        let refs: Vec<&TradeResult> = vec![&t1];

        let ps = PositionSummary::from_trades(&refs);
        assert_eq!(ps.position_id, "p1");
        assert_eq!(ps.close_count, 1);
        assert!((ps.net_pnl - 100.0).abs() < f64::EPSILON);
        assert!(ps.is_winner());
        assert!(!ps.is_loser());
    }

    #[test]
    fn position_summary_multiple_closes() {
        // TP1 wins, then SL loses — net positive.
        let t1 = TradeResult {
            position_id: "p1".into(),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            entry_price: 1.0850,
            exit_price: 1.0900,
            size: 0.5,
            pnl: 25.0,
            open_ts: ts(2026, 1, 1, 10, 0, 0),
            close_ts: ts(2026, 1, 1, 11, 0, 0),
            close_reason: CloseReason::Target,
            group: None,
        };
        let t2 = TradeResult {
            position_id: "p1".into(),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            entry_price: 1.0850,
            exit_price: 1.0830,
            size: 0.5,
            pnl: -10.0,
            open_ts: ts(2026, 1, 1, 10, 0, 0),
            close_ts: ts(2026, 1, 1, 14, 0, 0),
            close_reason: CloseReason::Stoploss,
            group: None,
        };
        let refs: Vec<&TradeResult> = vec![&t1, &t2];
        let ps = PositionSummary::from_trades(&refs);

        assert_eq!(ps.close_count, 2);
        assert!((ps.net_pnl - 15.0).abs() < f64::EPSILON);
        assert!((ps.original_size - 1.0).abs() < f64::EPSILON);
        assert!(ps.is_winner());
        assert_eq!(
            ps.close_reasons,
            vec![CloseReason::Target, CloseReason::Stoploss]
        );
        assert_eq!(ps.duration_seconds, 4 * 3600); // 10:00 to 14:00
    }

    #[test]
    fn position_win_rate_differs_from_trade_win_rate() {
        // Position p1: TP1 +$25, SL -$10 → net +$15 (position is a winner)
        // Trade-level: 1 win, 1 loss → 50% win rate
        // Position-level: 1 winner / 1 total → 100% win rate
        let trades = vec![
            TradeResult {
                position_id: "p1".into(),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                entry_price: 1.085,
                exit_price: 1.090,
                size: 0.5,
                pnl: 25.0,
                open_ts: ts(2026, 1, 1, 10, 0, 0),
                close_ts: ts(2026, 1, 1, 11, 0, 0),
                close_reason: CloseReason::Target,
                group: None,
            },
            TradeResult {
                position_id: "p1".into(),
                symbol: "EURUSD".into(),
                side: Side::Buy,
                entry_price: 1.085,
                exit_price: 1.083,
                size: 0.5,
                pnl: -10.0,
                open_ts: ts(2026, 1, 1, 10, 0, 0),
                close_ts: ts(2026, 1, 1, 14, 0, 0),
                close_reason: CloseReason::Stoploss,
                group: None,
            },
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        // Trade-level.
        assert_eq!(result.total_trades, 2);
        assert!((result.win_rate - 0.5).abs() < f64::EPSILON);

        // Position-level.
        assert_eq!(result.total_positions, 1);
        assert_eq!(result.winning_positions, 1);
        assert!((result.position_win_rate - 1.0).abs() < f64::EPSILON);
    }

    // ── Integration tests ───────────────────────────────────────────

    #[test]
    fn full_report_matches_summary() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Sell,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p3",
                "XAUUSD",
                Side::Buy,
                200.0,
                ts(2026, 1, 2, 10, 0, 0),
                ts(2026, 1, 2, 13, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        // Summary should match top-level fields.
        assert!((result.summary.total_pnl - result.total_pnl).abs() < f64::EPSILON);
        assert_eq!(result.summary.total_trades, result.total_trades);
        assert_eq!(result.summary.winning_trades, result.winning_trades);
        assert_eq!(result.summary.losing_trades, result.losing_trades);
        assert!((result.summary.win_rate - result.win_rate).abs() < f64::EPSILON);
        assert!((result.summary.profit_factor - result.profit_factor).abs() < f64::EPSILON);
    }

    #[test]
    fn per_symbol_sums_to_overall() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "XAUUSD",
                Side::Buy,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
            make_trade_full(
                "p3",
                "GBPUSD",
                Side::Sell,
                80.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 13, 0, 0),
                CloseReason::Target,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        let sym_total_trades: usize = result.per_symbol.values().map(|s| s.total_trades).sum();
        let sym_total_pnl: f64 = result.per_symbol.values().map(|s| s.total_pnl).sum();

        assert_eq!(sym_total_trades, result.total_trades);
        assert!((sym_total_pnl - result.total_pnl).abs() < 1e-10);
    }

    #[test]
    fn per_side_sums_to_overall() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "EURUSD",
                Side::Sell,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        let side_trades = result.long_stats.total_trades + result.short_stats.total_trades;
        let side_pnl = result.long_stats.total_pnl + result.short_stats.total_pnl;

        assert_eq!(side_trades, result.total_trades);
        assert!((side_pnl - result.total_pnl).abs() < 1e-10);
    }

    #[test]
    fn display_does_not_panic_with_new_fields() {
        // Empty.
        let r1 = BacktestResult::from_trade_log(10_000.0, vec![]);
        let _ = format!("{}", r1);

        // Single trade.
        let r2 = BacktestResult::from_trade_log(10_000.0, vec![make_trade(100.0, 11)]);
        let _ = format!("{}", r2);

        // Mixed.
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                Some("grp1".into()),
            ),
            make_trade_full(
                "p2",
                "XAUUSD",
                Side::Sell,
                -50.0,
                ts(2026, 1, 2, 10, 0, 0),
                ts(2026, 1, 2, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let r3 = BacktestResult::from_trade_log(10_000.0, trades);
        let output = format!("{}", r3);
        assert!(output.contains("Backtest Result"));
        assert!(output.contains("Risk Metrics"));
        assert!(output.contains("Side Breakdown"));
    }

    #[test]
    fn serde_roundtrip_enhanced_result() {
        let trades = vec![
            make_trade_full(
                "p1",
                "EURUSD",
                Side::Buy,
                100.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 11, 0, 0),
                CloseReason::Target,
                None,
            ),
            make_trade_full(
                "p2",
                "XAUUSD",
                Side::Sell,
                -50.0,
                ts(2026, 1, 1, 10, 0, 0),
                ts(2026, 1, 1, 12, 0, 0),
                CloseReason::Stoploss,
                None,
            ),
        ];
        let result = BacktestResult::from_trade_log(10_000.0, trades);

        let json = serde_json::to_string(&result).unwrap();
        let restored: BacktestResult = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.total_trades, result.total_trades);
        assert!((restored.total_pnl - result.total_pnl).abs() < f64::EPSILON);
        assert_eq!(restored.summary.total_trades, result.summary.total_trades);
        assert_eq!(restored.positions.len(), result.positions.len());
        assert_eq!(
            restored.per_close_reason.len(),
            result.per_close_reason.len()
        );
        assert_eq!(restored.monthly_returns.len(), result.monthly_returns.len());
    }

    // ── fmt_duration helper tests ───────────────────────────────────

    #[test]
    fn fmt_duration_basic() {
        assert_eq!(fmt_duration(0), "0m");
        assert_eq!(fmt_duration(300), "5m");
        assert_eq!(fmt_duration(3600), "1h 0m");
        assert_eq!(fmt_duration(3660), "1h 1m");
        assert_eq!(fmt_duration(86400), "1d 0h 0m");
        assert_eq!(fmt_duration(90061), "1d 1h 1m");
    }
}
