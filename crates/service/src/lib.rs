//! Provider-neutral service infrastructure for quant-system.
//!
//! This crate deliberately contains no RPC implementation. It defines the
//! endpoint and transport-failure vocabulary shared by service clients and hosts.

mod endpoint;
mod error;

pub use endpoint::{EndpointScheme, ServiceEndpoint, ServiceEndpointError};
pub use error::{RetryDisposition, TransportFailure, TransportFailureKind};
