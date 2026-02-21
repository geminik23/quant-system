use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),

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
