use chrono::NaiveDateTime;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataError {
    #[cfg(feature = "duckdb-backend")]
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[cfg(feature = "parquet")]
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Data traversal cancelled")]
    Cancelled,

    #[error("invalid scan bounds: from {from} is after to {to}")]
    InvalidScanBounds {
        from: NaiveDateTime,
        to: NaiveDateTime,
    },

    #[error("Parquet scan rows per read must be greater than zero")]
    InvalidScanReadSize,

    #[error("invalid Parquet date partition filename: {0}")]
    InvalidDatePartition(String),

    #[error("Parquet partition changed during scan: {path}")]
    ParquetPartitionChanged { path: String },

    #[error("non-monotonic Parquet timestamps in {path}: {current} follows {previous}")]
    NonMonotonicParquetData {
        path: String,
        previous: NaiveDateTime,
        current: NaiveDateTime,
    },

    #[error("Invalid timeframe: {0}")]
    InvalidTimeframe(String),

    #[error("Could not extract symbol from filename: {0}")]
    SymbolExtraction(String),

    #[error("Parse error in {file}:{line} — {message}")]
    ParseError {
        file: String,
        line: usize,
        message: String,
    },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, DataError>;
