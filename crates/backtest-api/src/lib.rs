//! Logical contracts for the quant-system backtest service.
//!
//! The default build contains no xrpc dependency. Enable the `xrpc` feature
//! only in a process composition root that selects the current provider.

mod client;
mod rpc_types;

pub use client::{
    BacktestAdminClient, BacktestClient, BacktestClientError, BacktestDiscoveryClient,
    BacktestEventStream, BacktestServiceProtocolError, BacktestSyncClient,
};
pub use rpc_types::*;

#[cfg(feature = "xrpc")]
pub mod provider;
