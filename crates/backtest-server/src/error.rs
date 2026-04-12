//! Error types for the backtest server.

use thiserror::Error;

/// All error variants the backtest server can produce.
#[derive(Debug, Error)]
pub enum BacktestServerError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] data_preprocess::DataError),

    #[error("Symbol not found: '{0}'")]
    SymbolNotFound(String),

    #[error("Profile not found: '{0}'")]
    ProfileNotFound(String),

    #[error("Profile error: {0}")]
    Profile(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("No market data found for {symbol} on {exchange} ({data_type})")]
    NoDataFound {
        symbol: String,
        exchange: String,
        data_type: String,
    },

    #[error("Backtest engine error: {0}")]
    Engine(#[from] qs_core::CoreError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Serialization error: {0}")]
    Serde(String),
}

/// Convenience alias used throughout the backtest server.
pub type Result<T> = std::result::Result<T, BacktestServerError>;
