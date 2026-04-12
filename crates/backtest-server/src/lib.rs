//! `qs-backtest-server` — Backtest server with shared memory IPC.
//!
//! Exposes the backtesting pipeline as an xrpc-rs shared-memory RPC service.
//! Clients submit signals, a management profile, and a date range; the server
//! loads Parquet data, runs the backtest, and returns serialized results.

pub mod config;
pub mod convert;
pub mod error;
pub mod handlers;
pub mod rpc_types;

pub use config::ServerConfig;
pub use error::{BacktestServerError, Result};
pub use handlers::ServerState;
pub use rpc_types::*;
