//! `qs-backtest-server` — Backtest server with shared memory IPC.
//!
//! Exposes the backtesting pipeline as an xrpc-rs shared-memory RPC service.
//! Clients submit signals, a management profile, and a date range; the server
//! loads Parquet data, runs the backtest, and returns serialized results.

pub mod artifact_store;
pub mod config;
pub mod convert;
pub mod error;
mod fx_loader;
pub mod handlers;
mod instrument_catalog;
mod market_loader;
mod replay_plan;
pub mod rpc_types;

pub use artifact_store::ArtifactStore;
pub use config::ServerConfig;
pub use error::{BacktestServerError, Result};
pub use handlers::ServerState;
pub use instrument_catalog::InstrumentDomain;
pub use rpc_types::*;

pub fn instrument_domain_from_config(
    config: &config::InstrumentsSection,
    registry: &qs_symbols::SymbolRegistry,
) -> Result<InstrumentDomain> {
    InstrumentDomain::load(config, registry)
}
