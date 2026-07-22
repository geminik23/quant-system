//! Logical contracts for the quant-system live market-data service.

mod client;
mod rpc_types;

pub use client::{
    AlertEventStream, MarketDataClient, MarketDataClientError, MarketDataServiceProtocolError,
    PriceEventStream,
};
pub use rpc_types::*;

#[cfg(feature = "xrpc")]
pub mod provider;
