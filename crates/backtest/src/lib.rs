//! `qs-backtest` — Backtesting engine for the quant-system workspace.
//!
//! This crate provides tools for replaying historical market data through the
//! [`qs_core::TradeEngine`] to evaluate trading strategies and predefined
//! signal sets.
//!
//! # Two modes of operation
//!
//! 1. **Strategy-driven** — implement the [`Strategy`] trait; the runner feeds
//!    market data tick-by-tick and your strategy decides when to act.
//! 2. **Signal replay** — provide a `Vec<Signal>` with timestamps; the runner
//!    injects them at the correct moments while replaying price data.
//!
//! # Key types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`BacktestRunner`] | Orchestrates the backtest loop (both modes) |
//! | [`BacktestExecutor`] | Tracks simulated fills, positions, and P&L |
//! | [`BacktestResult`] | Final report — P&L, win rate, drawdown, trade log |
//! | [`DataFeed`] | Trait for sequential market event sources |
//! | [`Strategy`] | Trait for strategy-driven backtests |

pub mod data_feed;
pub mod executor;
pub mod report;
pub mod runner;
pub mod strategy;

// ── Convenience re-exports ──────────────────────────────────────────────────

pub use data_feed::{DataFeed, MarketEvent, VecFeed};
pub use executor::BacktestExecutor;
pub use report::{BacktestResult, TradeResult};
pub use runner::BacktestRunner;
pub use strategy::Strategy;
