use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use qs_instruments::{Decimal, PositiveDecimal};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const MAX_DOMAIN_ID_BYTES: usize = 160;
pub const MAX_TRADE_INTENT_ID_BYTES: usize = "intent:".len() + MAX_DOMAIN_ID_BYTES + 1 + 10;
pub const MAX_EXECUTION_COMMAND_ID_BYTES: usize =
    "command:".len() + MAX_TRADE_INTENT_ID_BYTES + 1 + 20;
pub const MAX_OPAQUE_REF_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonicalDomainError {
    #[error("{kind} length must be between 1 and {maximum} bytes, got {actual}")]
    InvalidIdentifierLength {
        kind: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{kind} contains an unsupported character")]
    InvalidIdentifierCharacter { kind: &'static str },
    #[error("invalid UTC timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("fraction must be greater than zero and at most one")]
    InvalidFraction,
    #[error("duration must be greater than zero milliseconds")]
    InvalidDuration,
}

fn validate_identifier(
    kind: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), CanonicalDomainError> {
    if value.is_empty() || value.len() > maximum {
        return Err(CanonicalDomainError::InvalidIdentifierLength {
            kind,
            maximum,
            actual: value.len(),
        });
    }
    if !value.is_ascii()
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(CanonicalDomainError::InvalidIdentifierCharacter { kind });
    }
    Ok(())
}

macro_rules! domain_id {
    ($name:ident, $kind:literal, $maximum:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, CanonicalDomainError> {
                let value = value.into();
                validate_identifier($kind, &value, $maximum)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = CanonicalDomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

domain_id!(TradeIntentId, "trade intent ID", MAX_TRADE_INTENT_ID_BYTES);
domain_id!(IntentProducerId, "intent producer ID", MAX_DOMAIN_ID_BYTES);
domain_id!(
    IntentCorrelationId,
    "intent correlation ID",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(
    IntentIdentityNamespace,
    "intent identity namespace",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(
    IntentPositionRef,
    "intent position reference",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(
    IntentCampaignRef,
    "intent campaign reference",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(
    IntentStateRef,
    "intent state reference",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(
    ExecutionCommandId,
    "execution command ID",
    MAX_EXECUTION_COMMAND_ID_BYTES
);
domain_id!(VenueOrderRef, "venue order reference", MAX_DOMAIN_ID_BYTES);
domain_id!(
    VenuePositionRef,
    "venue position reference",
    MAX_DOMAIN_ID_BYTES
);
domain_id!(FillId, "fill ID", MAX_DOMAIN_ID_BYTES);
domain_id!(
    OpaqueProvenanceRef,
    "opaque provenance reference",
    MAX_OPAQUE_REF_BYTES
);
domain_id!(
    OpaquePayloadRef,
    "opaque payload reference",
    MAX_OPAQUE_REF_BYTES
);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DateTimeUtc(DateTime<Utc>);

impl DateTimeUtc {
    pub fn new(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    pub fn from_naive_utc(value: NaiveDateTime) -> Self {
        Self(DateTime::from_naive_utc_and_offset(value, Utc))
    }

    pub fn parse(value: &str) -> Result<Self, CanonicalDomainError> {
        DateTime::parse_from_rfc3339(value)
            .map(|timestamp| Self(timestamp.with_timezone(&Utc)))
            .map_err(|error| CanonicalDomainError::InvalidTimestamp(error.to_string()))
    }

    pub fn into_inner(self) -> DateTime<Utc> {
        self.0
    }

    pub fn as_inner(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl fmt::Display for DateTimeUtc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&canonical_timestamp(self.0))
    }
}

impl Serialize for DateTimeUtc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&canonical_timestamp(self.0))
    }
}

impl<'de> Deserialize<'de> for DateTimeUtc {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn canonical_timestamp(value: DateTime<Utc>) -> String {
    let mut value = value.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let suffix = value.pop();
    debug_assert_eq!(suffix, Some('Z'));
    while value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    value.push('Z');
    value
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PositiveFraction(PositiveDecimal);

impl PositiveFraction {
    pub fn new(value: PositiveDecimal) -> Result<Self, CanonicalDomainError> {
        let one = Decimal::new(1, 0).expect("one is a valid decimal");
        if value.get() > one {
            return Err(CanonicalDomainError::InvalidFraction);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> PositiveDecimal {
        self.0
    }
}

impl Serialize for PositiveFraction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PositiveFraction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(PositiveDecimal::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PriceDistance(PositiveDecimal);

impl PriceDistance {
    pub const fn new(value: PositiveDecimal) -> Self {
        Self(value)
    }

    pub const fn get(self) -> PositiveDecimal {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DurationMillis(u64);

impl DurationMillis {
    pub fn new(value: u64) -> Result<Self, CanonicalDomainError> {
        if value == 0 {
            return Err(CanonicalDomainError::InvalidDuration);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatingMode {
    Research,
    Replay,
    Shadow,
    Paper,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCapability {
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder,
    PartialReduction,
    ReplaceProtection,
    ReplaceTargets,
    AddTranche,
    CancelEntry,
    ScopedFlatten,
    ReduceOnly,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_strict() {
        assert!(TradeIntentId::new("").is_err());
        assert!(TradeIntentId::new("intent with spaces").is_err());
        assert!(TradeIntentId::new("x".repeat(MAX_TRADE_INTENT_ID_BYTES)).is_ok());
        assert!(TradeIntentId::new("x".repeat(MAX_TRADE_INTENT_ID_BYTES + 1)).is_err());
        assert!(ExecutionCommandId::new("x".repeat(MAX_EXECUTION_COMMAND_ID_BYTES)).is_ok());
        assert!(ExecutionCommandId::new("x".repeat(MAX_EXECUTION_COMMAND_ID_BYTES + 1)).is_err());
    }

    #[test]
    fn timestamp_serialization_is_canonical_utc() {
        let timestamp = DateTimeUtc::parse("2026-08-14T12:00:00.120000+02:00").unwrap();
        assert_eq!(
            serde_json::to_string(&timestamp).unwrap(),
            "\"2026-08-14T10:00:00.12Z\""
        );
    }

    #[test]
    fn positive_fraction_rejects_values_above_one() {
        let half = PositiveDecimal::new("0.5".parse().unwrap()).unwrap();
        assert_eq!(PositiveFraction::new(half).unwrap().get(), half);
        let too_large = PositiveDecimal::new("1.1".parse().unwrap()).unwrap();
        assert!(PositiveFraction::new(too_large).is_err());
    }
}
