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

    #[error("instrument '{symbol}' unavailable ({reason:?}): {details}")]
    InstrumentUnavailable {
        symbol: String,
        reason: crate::rpc_types::InstrumentExclusionReasonMsg,
        details: String,
    },

    #[error("instrument '{symbol}' is inactive at {at}: {details}")]
    InactiveInstrument {
        symbol: String,
        at: chrono::DateTime<chrono::Utc>,
        details: String,
    },

    #[error("no valid conversion tick for '{symbol}' strictly before {start}")]
    ConversionWarmupUnavailable {
        symbol: String,
        start: chrono::NaiveDateTime,
    },

    #[error("replay admission rejected: {0}")]
    AdmissionRejected(String),

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

    #[error("Backtest cancelled")]
    Cancelled,

    #[error("Backtest cancelled with a resumable research checkpoint")]
    CancelledWithCheckpoint(Box<qs_research::SearchCheckpoint>),

    #[error("Configured strategy replay failed: {0}")]
    Strategy(String),

    #[error("Market-data stream error: {0}")]
    MarketStream(String),

    #[error("{0}")]
    MarketLoad(#[from] qs_market_loader::MarketLoadError),

    #[error("Backtest engine error: {0}")]
    Engine(#[from] qs_core::CoreError),

    #[error("Currency conversion error: {0}")]
    Currency(#[from] qs_backtest::ConversionError),

    #[error("Currency plan error: {0}")]
    CurrencyPlan(#[from] qs_backtest::RunCurrencyPlanError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Serialization error: {0}")]
    Serde(String),
}

/// Convenience alias used throughout the backtest server.
pub type Result<T> = std::result::Result<T, BacktestServerError>;
