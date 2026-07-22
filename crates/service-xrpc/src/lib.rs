//! Shared xrpc provider runtime for quant-system services.
//!
//! Service crates provide method registration and logical DTOs. This crate
//! owns frame-transport selection, SHM handshakes, client receive-task
//! lifecycle, listener loops, connection limits, and provider error mapping.

mod client;
mod config;
mod error;
mod host;
mod shared_memory;

pub use client::{DynRpcClient, XrpcClientSession, channel_pair, connect};
pub use config::XrpcTransportConfig;
pub use error::{XrpcProviderError, map_rpc_error, map_transport_error};
pub use host::{ConnectionContext, XrpcServiceRegistrar, serve_host, serve_transport};
pub use shared_memory::cleanup_owned_shared_memory;

pub use xrpc::{BincodeCodec, Codec, JsonCodec};
