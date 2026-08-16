//! Shared configuration values for historical strategy requirements.

use std::fmt;
use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

pub const MAX_SERIES_ID_BYTES: usize = 64;
pub const MAX_WARMUP_BARS: usize = 1_000_000;
pub const MAX_DECISION_RECORDS: usize = 1_000_000;
pub const MAX_SIGNALS_PER_CALLBACK: usize = 4096;
pub const MAX_REASON_BYTES: usize = 4096;

/// Errors returned while constructing historical strategy configuration values.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyConfigError {
    #[error("{field} must be greater than zero")]
    ZeroValue { field: &'static str },
    #[error("series ID must contain 1 to {MAX_SERIES_ID_BYTES} ASCII identifier bytes")]
    InvalidSeriesId,
    #[error("warmup bars {value} exceed the maximum {MAX_WARMUP_BARS}")]
    WarmupTooLarge { value: usize },
    #[error("{field} {value} exceeds the maximum {maximum}")]
    LimitTooLarge {
        field: &'static str,
        value: usize,
        maximum: usize,
    },
}

/// Stable caller-supplied identity for one required historical series.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SeriesId(String);

impl SeriesId {
    pub fn new(value: impl Into<String>) -> Result<Self, StrategyConfigError> {
        let value = value.into();
        if valid_identifier(&value, MAX_SERIES_ID_BYTES) {
            Ok(Self(value))
        } else {
            Err(StrategyConfigError::InvalidSeriesId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SeriesId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SeriesId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Fixed-duration historical analysis interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timeframe {
    Seconds(NonZeroU32),
    Minutes(NonZeroU32),
    Hours(NonZeroU32),
    Days(NonZeroU32),
}

impl Timeframe {
    pub fn seconds(value: u32) -> Result<Self, StrategyConfigError> {
        nonzero(value, "timeframe seconds").map(Self::Seconds)
    }

    pub fn minutes(value: u32) -> Result<Self, StrategyConfigError> {
        nonzero(value, "timeframe minutes").map(Self::Minutes)
    }

    pub fn hours(value: u32) -> Result<Self, StrategyConfigError> {
        nonzero(value, "timeframe hours").map(Self::Hours)
    }

    pub fn days(value: u32) -> Result<Self, StrategyConfigError> {
        nonzero(value, "timeframe days").map(Self::Days)
    }

    pub fn duration_seconds(self) -> u64 {
        match self {
            Self::Seconds(value) => u64::from(value.get()),
            Self::Minutes(value) => u64::from(value.get()) * 60,
            Self::Hours(value) => u64::from(value.get()) * 60 * 60,
            Self::Days(value) => u64::from(value.get()) * 24 * 60 * 60,
        }
    }
}

/// Quote side used to aggregate one historical analysis series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceBasis {
    Bid,
    Ask,
    Mid,
}

/// Number of completed bars required before one series is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct WarmupRequirement(usize);

impl WarmupRequirement {
    pub fn bars(value: usize) -> Result<Self, StrategyConfigError> {
        if value <= MAX_WARMUP_BARS {
            Ok(Self(value))
        } else {
            Err(StrategyConfigError::WarmupTooLarge { value })
        }
    }

    pub fn required_bars(self) -> usize {
        self.0
    }
}

impl<'de> Deserialize<'de> for WarmupRequirement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = usize::deserialize(deserializer)?;
        Self::bars(value).map_err(serde::de::Error::custom)
    }
}

/// Caller-visible bounds for retained decisions and callback output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StrategyRetentionLimits {
    max_decisions: usize,
    max_signals_per_callback: usize,
    max_reason_bytes: usize,
}

impl StrategyRetentionLimits {
    pub fn new(
        max_decisions: usize,
        max_signals_per_callback: usize,
        max_reason_bytes: usize,
    ) -> Result<Self, StrategyConfigError> {
        validate_limit("max_decisions", max_decisions, MAX_DECISION_RECORDS, true)?;
        validate_limit(
            "max_signals_per_callback",
            max_signals_per_callback,
            MAX_SIGNALS_PER_CALLBACK,
            false,
        )?;
        validate_limit(
            "max_reason_bytes",
            max_reason_bytes,
            MAX_REASON_BYTES,
            false,
        )?;
        Ok(Self {
            max_decisions,
            max_signals_per_callback,
            max_reason_bytes,
        })
    }

    pub fn max_decisions(self) -> usize {
        self.max_decisions
    }

    pub fn max_signals_per_callback(self) -> usize {
        self.max_signals_per_callback
    }

    pub fn max_reason_bytes(self) -> usize {
        self.max_reason_bytes
    }
}

impl Default for StrategyRetentionLimits {
    fn default() -> Self {
        Self {
            max_decisions: 10_000,
            max_signals_per_callback: 256,
            max_reason_bytes: 1024,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyRetentionLimitsDef {
    max_decisions: usize,
    max_signals_per_callback: usize,
    max_reason_bytes: usize,
}

impl<'de> Deserialize<'de> for StrategyRetentionLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyRetentionLimitsDef::deserialize(deserializer)?;
        Self::new(
            value.max_decisions,
            value.max_signals_per_callback,
            value.max_reason_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

fn nonzero(value: u32, field: &'static str) -> Result<NonZeroU32, StrategyConfigError> {
    NonZeroU32::new(value).ok_or(StrategyConfigError::ZeroValue { field })
}

fn validate_limit(
    field: &'static str,
    value: usize,
    maximum: usize,
    allow_zero: bool,
) -> Result<(), StrategyConfigError> {
    if value == 0 && !allow_zero {
        return Err(StrategyConfigError::ZeroValue { field });
    }
    if value > maximum {
        return Err(StrategyConfigError::LimitTooLarge {
            field,
            value,
            maximum,
        });
    }
    Ok(())
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
