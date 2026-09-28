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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Observed capabilities of one legacy tick CSV import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickImportAudit {
    pub parsed_rows: usize,
    pub distinct_timestamps: usize,
    pub simultaneous_rows: usize,
    pub maximum_fractional_digits: u8,
    pub provider_sequence_available: bool,
    pub stable_import_ordinal_persisted: bool,
    pub legacy_timestamp_dedup: bool,
    pub exact_quote_path_capable: bool,
}

impl TickImportAudit {
    pub fn parquet_precision_loss_possible(&self) -> bool {
        self.maximum_fractional_digits > 6
    }
}

/// Declared capability of a stored tick path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TickPathCapability {
    LegacyTimestampDeduplicated,
    OrderedWithoutProviderSequence,
    OrderedWithProviderSequence,
}

impl TickPathCapability {
    pub const fn preserves_simultaneous_rows(self) -> bool {
        !matches!(self, Self::LegacyTimestampDeduplicated)
    }

    pub const fn exact_quote_path_capable(self) -> bool {
        !matches!(self, Self::LegacyTimestampDeduplicated)
    }

    pub const fn verifies_true_duplicates(self) -> bool {
        matches!(self, Self::OrderedWithProviderSequence)
    }
}

/// Stable imported quote with persisted source order and optional provider identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTick {
    pub tick: Tick,
    pub source_ordinal: u64,
    pub source_identity: Option<String>,
    pub provider_sequence: Option<u64>,
}

impl StoredTick {
    pub fn validate(&self) -> Result<()> {
        if self.source_identity.as_deref().is_some_and(str::is_empty)
            || self.source_identity.is_some() != self.provider_sequence.is_some()
        {
            return Err(DataError::Other(
                "provider sequence and nonempty source identity must be supplied together".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredPriceBasis {
    Bid,
    Ask,
    Mid,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CountCapability {
    KnownPositive,
    Optional,
    Unavailable,
}

/// Caller-owned physical-series contract. `verified = false` identifies a legacy assertion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeriesDescriptor {
    pub source_identity: String,
    pub exchange: String,
    pub symbol: String,
    pub timeframe_seconds: u64,
    pub price_basis: StoredPriceBasis,
    pub alignment_offset_seconds: i64,
    pub digits: u32,
    pub point_size: f64,
    pub count_capability: CountCapability,
    pub verified: bool,
}

impl SeriesDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.source_identity.is_empty()
            || self.exchange.is_empty()
            || self.symbol.is_empty()
            || self.timeframe_seconds == 0
            || !self.point_size.is_finite()
            || self.point_size <= 0.0
            || self.digits > 18
        {
            return Err(DataError::Other("invalid series descriptor".into()));
        }
        Ok(())
    }
}

/// Additive price-only bar representation with an explicit optional source count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceBar {
    pub exchange: String,
    pub symbol: String,
    pub timeframe: Timeframe,
    pub ts: NaiveDateTime,
    pub available_at: NaiveDateTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub tick_count: Option<u64>,
    pub spread: Option<i32>,
}

impl PriceBar {
    pub fn validate(&self) -> Result<()> {
        if self.exchange.is_empty()
            || self.symbol.is_empty()
            || self.available_at < self.ts
            || ![self.open, self.high, self.low, self.close]
                .into_iter()
                .all(f64::is_finite)
            || self.open <= 0.0
            || self.high < self.open.max(self.close)
            || self.low > self.open.min(self.close)
            || self.low <= 0.0
            || self.tick_count == Some(0)
        {
            return Err(DataError::Other("invalid price-only bar".into()));
        }
        Ok(())
    }
}

impl TryFrom<Bar> for PriceBar {
    type Error = DataError;

    fn try_from(value: Bar) -> Result<Self> {
        let tick_count = u64::try_from(value.tick_vol)
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| DataError::Other("legacy tick count must be positive".into()))?;
        let duration = value.timeframe.fixed_duration_seconds().ok_or_else(|| {
            DataError::Other("price-only conversion requires fixed timeframe".into())
        })?;
        let available_at = value
            .ts
            .checked_add_signed(chrono::Duration::seconds(duration))
            .ok_or_else(|| DataError::Other("bar availability overflowed".into()))?;
        let result = Self {
            exchange: value.exchange,
            symbol: value.symbol,
            timeframe: value.timeframe,
            ts: value.ts,
            available_at,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            tick_count: Some(tick_count),
            spread: Some(value.spread),
        };
        result.validate()?;
        Ok(result)
    }
}

impl TryFrom<PriceBar> for Bar {
    type Error = DataError;

    fn try_from(value: PriceBar) -> Result<Self> {
        value.validate()?;
        let count = value
            .tick_count
            .ok_or_else(|| DataError::Other("legacy bar requires a known count".into()))?;
        let tick_vol = i64::try_from(count)
            .map_err(|_| DataError::Other("tick count exceeds legacy i64 range".into()))?;
        Ok(Self {
            exchange: value.exchange,
            symbol: value.symbol,
            timeframe: value.timeframe,
            ts: value.ts,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            tick_vol,
            volume: 0,
            spread: value.spread.unwrap_or(0),
        })
    }
}

/// Aggregate exactly one complete, ordered parent bucket. No incomplete tail is emitted.
pub fn aggregate_price_bars(
    children: &[PriceBar],
    parent_timeframe: Timeframe,
    descriptor: &SeriesDescriptor,
) -> Result<PriceBar> {
    descriptor.validate()?;
    let child_seconds = descriptor.timeframe_seconds;
    let parent_seconds = u64::try_from(
        parent_timeframe
            .fixed_duration_seconds()
            .ok_or_else(|| DataError::Other("parent timeframe must be fixed".into()))?,
    )
    .map_err(|_| DataError::Other("invalid parent duration".into()))?;
    if parent_seconds <= child_seconds || parent_seconds % child_seconds != 0 {
        return Err(DataError::Other(
            "parent duration must be a larger exact multiple of child duration".into(),
        ));
    }
    let expected = usize::try_from(parent_seconds / child_seconds)
        .map_err(|_| DataError::Other("parent child count overflowed".into()))?;
    if children.len() != expected {
        return Err(DataError::Other("parent bucket is incomplete".into()));
    }
    let first = &children[0];
    let offset = descriptor.alignment_offset_seconds;
    let parent_i64 = i64::try_from(parent_seconds)
        .map_err(|_| DataError::Other("parent duration exceeds i64".into()))?;
    if (first.ts.and_utc().timestamp() - offset).rem_euclid(parent_i64) != 0 {
        return Err(DataError::Other("parent bucket is misaligned".into()));
    }
    let child_i64 = i64::try_from(child_seconds)
        .map_err(|_| DataError::Other("child duration exceeds i64".into()))?;
    for (index, child) in children.iter().enumerate() {
        child.validate()?;
        let expected_ts = first
            .ts
            .checked_add_signed(chrono::Duration::seconds(
                child_i64
                    .checked_mul(i64::try_from(index).unwrap())
                    .ok_or_else(|| DataError::Other("child timestamp overflowed".into()))?,
            ))
            .ok_or_else(|| DataError::Other("child timestamp overflowed".into()))?;
        if child.exchange != descriptor.exchange
            || child.symbol != descriptor.symbol
            || child.ts != expected_ts
            || u64::try_from(child.timeframe.fixed_duration_seconds().unwrap_or_default()).ok()
                != Some(child_seconds)
        {
            return Err(DataError::Other(
                "child identity, duration, order or completeness mismatch".into(),
            ));
        }
    }
    let tick_count =
        children
            .iter()
            .try_fold(Some(0_u64), |sum, child| match (sum, child.tick_count) {
                (Some(sum), Some(count)) => sum
                    .checked_add(count)
                    .map(Some)
                    .ok_or_else(|| DataError::Other("aggregated count overflowed".into())),
                _ => Ok(None),
            })?;
    let result = PriceBar {
        exchange: first.exchange.clone(),
        symbol: first.symbol.clone(),
        timeframe: parent_timeframe,
        ts: first.ts,
        available_at: children.iter().map(|bar| bar.available_at).max().unwrap(),
        open: first.open,
        high: children
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max),
        low: children
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min),
        close: children.last().unwrap().close,
        tick_count,
        spread: None,
    };
    result.validate()?;
    Ok(result)
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
