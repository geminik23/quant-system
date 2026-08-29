//! Provider-neutral backtest client workflow and connection utilities.
//!
//! The default graph contains no transport provider. Enable `xrpc` only in a
//! process composition root that selects the current provider.

mod connector;
mod error;
pub mod scripted;

#[cfg(feature = "xrpc")]
pub mod provider;

pub use connector::{
    BacktestCatalogConnector, ServiceCatalogSnapshot, parse_desktop_endpoint, probe_service_catalog,
};
pub use error::{CatalogProbeError, CatalogProbeStage, DesktopEndpointError};
