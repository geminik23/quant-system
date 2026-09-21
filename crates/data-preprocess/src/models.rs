use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::error::{DataError, Result};

/// Query parameters for tick view/query commands.
pub struct QueryOpts {
    pub exchange: String,
    pub symbol: String,
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
    pub limit: usize,
    pub tail: bool,
    pub descending: bool,
}

/// Query parameters for bar view/query commands.
pub struct BarQueryOpts {
    pub exchange: String,
    pub symbol: String,
    pub timeframe: String,
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
    pub limit: usize,
    pub tail: bool,
    pub descending: bool,
}

/// Supported bar timeframes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Timeframe {
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H4,
    D1,
    W1,
    MN1,
}

impl Timeframe {
    /// Parse a CLI or storage timeframe, preserving the canonical monthly `1M` label.
    pub fn parse(s: &str) -> Result<Self> {
        if s == "1M" {
            return Ok(Self::MN1);
        }

        match s.to_ascii_lowercase().as_str() {
            "1m" | "m1" => Ok(Self::M1),
            "3m" | "m3" => Ok(Self::M3),
            "5m" | "m5" => Ok(Self::M5),
            "15m" | "m15" => Ok(Self::M15),
            "30m" | "m30" => Ok(Self::M30),
            "1h" | "h1" => Ok(Self::H1),
            "4h" | "h4" => Ok(Self::H4),
            "1d" | "d1" => Ok(Self::D1),
            "1w" | "w1" => Ok(Self::W1),
            "1mn" | "mn1" | "1m0" | "mn" => Ok(Self::MN1),
            _ => Err(DataError::InvalidTimeframe(s.to_string())),
        }
    }

    /// Length in seconds for timeframes that have a fixed duration.
    ///
    /// Monthly bars have no fixed length and return `None`, so a caller that needs deterministic bucket arithmetic must reject them. Weekly buckets are fixed-length but align to the Unix epoch week unless the caller supplies an alignment offset.
    pub fn fixed_duration_seconds(&self) -> Option<i64> {
        let seconds = match self {
            Self::M1 => 60,
            Self::M3 => 180,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::M30 => 1_800,
            Self::H1 => 3_600,
            Self::H4 => 14_400,
            Self::D1 => 86_400,
            Self::W1 => 604_800,
            Self::MN1 => return None,
        };
        Some(seconds)
    }

    /// Canonical short label for storage: "1m", "3m", "5m", ...
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M3 => "3m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
            Self::W1 => "1w",
            Self::MN1 => "1M",
        }
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single tick (bid/ask/last at a point in time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub exchange: String,
    pub symbol: String,
    pub ts: NaiveDateTime,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub volume: Option<f64>,
    pub flags: Option<i32>,
}

/// A single OHLCV bar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub exchange: String,
    pub symbol: String,
    pub timeframe: Timeframe,
    pub ts: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub tick_vol: i64,
    pub volume: i64,
    pub spread: i32,
}

/// Summary row returned by stats queries.
#[derive(Debug)]
pub struct StatRow {
    pub exchange: String,
    pub symbol: String,
    pub data_type: String,
    pub count: u64,
    pub ts_min: NaiveDateTime,
    pub ts_max: NaiveDateTime,
}

/// Result of an import operation.
#[derive(Debug)]
pub struct ImportResult {
    pub file: String,
    pub exchange: String,
    pub symbol: String,
    pub rows_parsed: usize,
    pub rows_inserted: usize,
    pub rows_skipped: usize,
    pub elapsed: std::time::Duration,
}
