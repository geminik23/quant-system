use data_preprocess::DataError;

/// Failure while describing or opening a stored market stream.
///
/// The variants describe the loading boundary itself, not the caller that asked for the load. A service maps them onto its own transport errors, and an in-process search reports them as a failed run.
#[derive(Debug, thiserror::Error)]
pub enum MarketLoadError {
    /// The stored data layer failed.
    #[error("Market data error: {0}")]
    Data(#[from] DataError),

    /// The requested series cannot be described, because a field is missing, unparseable, or disagrees with the pinned manifest.
    #[error("Invalid market series: {0}")]
    InvalidSeries(String),

    /// The requested coordinates hold no stored data.
    #[error("No market data found for {symbol} on {exchange} ({data_type})")]
    NoDataFound {
        symbol: String,
        exchange: String,
        data_type: String,
    },

    /// The caller's cancellation check fired while opening or draining a stream.
    #[error("Market data loading cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, MarketLoadError>;
