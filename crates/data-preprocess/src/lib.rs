#[cfg(feature = "parquet")]
pub mod convert;
#[cfg(feature = "duckdb-backend")]
pub mod db;
pub mod display;
pub mod error;
pub mod models;
#[cfg(feature = "parquet")]
pub mod parquet_store;
pub mod parser;
#[cfg(feature = "parquet")]
pub mod scanner;

#[cfg(feature = "duckdb-backend")]
pub use db::Database;
pub use error::{DataError, Result};
pub use models::*;
#[cfg(feature = "parquet")]
pub use parquet_store::ParquetStore;
#[cfg(feature = "parquet")]
pub use scanner::{
    DEFAULT_PARQUET_SCAN_ROWS, ParquetBarCursor, ParquetScanBounds, ParquetTickCursor,
};
