//! Backtest reporting — trade log and aggregate statistics.

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use qs_core::types::{CloseReason, GroupId, PositionId, Side};

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
    /// Group this position belonged to (for per-group reporting in F08).
    #[serde(default)]
    pub group: Option<GroupId>,
}

/// Aggregate backtest statistics produced by [`BacktestRunner`](crate::runner::BacktestRunner).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
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
    /// Sum of winning P&L / abs(sum of losing P&L).  `f64::INFINITY` if no losers.
    pub profit_factor: f64,
    /// Equity value at each trade close: `(timestamp, balance)`.
    pub equity_curve: Vec<(NaiveDateTime, f64)>,
    /// Full trade log.
    pub trade_log: Vec<TradeResult>,
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
        }
    }
}

impl std::fmt::Display for BacktestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "═══ Backtest Result ═══")?;
        writeln!(
            f,
            "Balance     : {:.2} → {:.2}",
            self.initial_balance, self.final_balance
        )?;
        writeln!(f, "Total P&L   : {:.2}", self.total_pnl)?;
        writeln!(f, "Trades      : {}", self.total_trades)?;
        writeln!(
            f,
            "Win / Lose  : {} / {}",
            self.winning_trades, self.losing_trades
        )?;
        writeln!(f, "Win Rate    : {:.1}%", self.win_rate * 100.0)?;
        writeln!(f, "Profit Factor: {:.2}", self.profit_factor)?;
        writeln!(
            f,
            "Max Drawdown : {:.2} ({:.1}%)",
            self.max_drawdown,
            self.max_drawdown_pct * 100.0
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
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
            open_ts: ts(10, 0, 0),
            close_ts: ts(close_h, 0, 0),
            close_reason: if pnl > 0.0 {
                CloseReason::Target
            } else {
                CloseReason::Stoploss
            },
            group: None,
        }
    }

    #[test]
    fn empty_trade_log() {
        let result = BacktestResult::from_trade_log(10_000.0, vec![]);
        assert_eq!(result.total_trades, 0);
        assert!((result.final_balance - 10_000.0).abs() < f64::EPSILON);
        assert!((result.win_rate - 0.0).abs() < f64::EPSILON);
        assert!((result.max_drawdown - 0.0).abs() < f64::EPSILON);
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
        // profit_factor = (100+200) / (50+30) = 300/80 = 3.75
        assert!((result.profit_factor - 3.75).abs() < f64::EPSILON);
    }

    #[test]
    fn drawdown_calculation() {
        let trades = vec![
            make_trade(100.0, 11),  // balance: 10100, peak: 10100
            make_trade(-200.0, 12), // balance: 9900, dd: 200
            make_trade(50.0, 13),   // balance: 9950, dd: 150
            make_trade(-100.0, 14), // balance: 9850, dd: 250
            make_trade(500.0, 15),  // balance: 10350, new peak
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
}
