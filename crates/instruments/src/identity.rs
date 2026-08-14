use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const MAX_SCOPED_ID_LEN: usize = 64;
const MAX_ASSET_ID_LEN: usize = 24;

fn validate_ascii_component(
    kind: &'static str,
    value: &str,
    minimum: usize,
    maximum: usize,
    separator: impl Fn(u8) -> bool,
) -> Result<(), IdentifierError> {
    let length = value.len();
    if !(minimum..=maximum).contains(&length) {
        return Err(IdentifierError::InvalidLength {
            kind,
            minimum,
            maximum,
            actual: length,
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || separator(byte))
    {
        return Err(IdentifierError::InvalidCharacter { kind });
    }
    Ok(())
}

macro_rules! normalized_id {
    ($name:ident, $kind:literal, $normalize:expr, $validate:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, IdentifierError> {
                let normalized: String = ($normalize)(value.as_ref().trim());
                ($validate)(&normalized)?;
                Ok(Self(normalized))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

fn lowercase(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn uppercase(value: &str) -> String {
    value.to_ascii_uppercase()
}

fn unchanged(value: &str) -> String {
    value.to_owned()
}

fn validate_scoped(kind: &'static str, value: &str) -> Result<(), IdentifierError> {
    validate_ascii_component(kind, value, 1, MAX_SCOPED_ID_LEN, |byte| {
        matches!(byte, b'_' | b'-' | b'.')
    })?;
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(IdentifierError::NonCanonicalCase { kind });
    }
    Ok(())
}

fn validate_listing(value: &str) -> Result<(), IdentifierError> {
    validate_ascii_component("listing ID", value, 1, MAX_SCOPED_ID_LEN, |byte| {
        matches!(byte, b'_' | b'-' | b'.')
    })?;
    if value.bytes().any(|byte| byte.is_ascii_lowercase()) {
        return Err(IdentifierError::NonCanonicalCase { kind: "listing ID" });
    }
    Ok(())
}

normalized_id!(AssetId, "asset ID", uppercase, |value: &str| {
    validate_ascii_component("asset ID", value, 2, MAX_ASSET_ID_LEN, |byte| {
        matches!(byte, b'_' | b'-' | b'.')
    })?;
    if value.bytes().any(|byte| byte.is_ascii_lowercase()) {
        return Err(IdentifierError::NonCanonicalCase { kind: "asset ID" });
    }
    Ok(())
});
normalized_id!(
    ListingVenueId,
    "listing venue ID",
    lowercase,
    |value: &str| { validate_scoped("listing venue ID", value) }
);
normalized_id!(ListingId, "listing ID", uppercase, validate_listing);
normalized_id!(MarketKind, "market kind", lowercase, |value: &str| {
    validate_scoped("market kind", value)
});
normalized_id!(
    TradingPlatformId,
    "trading platform ID",
    lowercase,
    |value: &str| { validate_scoped("trading platform ID", value) }
);
normalized_id!(
    ExecutionVenueId,
    "execution venue ID",
    lowercase,
    |value: &str| { validate_scoped("execution venue ID", value) }
);
normalized_id!(
    MarketDataSourceId,
    "market data source ID",
    lowercase,
    |value: &str| { validate_scoped("market data source ID", value) }
);
normalized_id!(
    EconomicsModelId,
    "economics model ID",
    lowercase,
    |value: &str| { validate_scoped("economics model ID", value) }
);
normalized_id!(
    EconomicsImplementationId,
    "economics implementation ID",
    lowercase,
    |value: &str| { validate_scoped("economics implementation ID", value) }
);
normalized_id!(SpecRevision, "spec revision", unchanged, |value: &str| {
    validate_spec_revision(value)
});
normalized_id!(
    InstrumentAlias,
    "instrument alias",
    uppercase,
    |value: &str| {
        validate_ascii_component("instrument alias", value, 1, MAX_SCOPED_ID_LEN, |byte| {
            matches!(byte, b'_' | b'-' | b'.' | b'/')
        })?;
        if value.bytes().any(|byte| byte.is_ascii_lowercase()) {
            return Err(IdentifierError::NonCanonicalCase {
                kind: "instrument alias",
            });
        }
        Ok(())
    }
);

impl MarketKind {
    pub const FX_CFD: &'static str = "fx_cfd";
    pub const METAL_CFD: &'static str = "metal_cfd";
    pub const COMMODITY_CFD: &'static str = "commodity_cfd";
    pub const INDEX_CFD: &'static str = "index_cfd";
    pub const LINEAR_EXPOSURE: &'static str = "linear_exposure";
    pub const CASH_SPOT: &'static str = "cash_spot";
    pub const LINEAR_FUTURE: &'static str = "linear_future";
    pub const LINEAR_PERPETUAL: &'static str = "linear_perpetual";
    pub const INVERSE_FUTURE: &'static str = "inverse_future";
    pub const INVERSE_PERPETUAL: &'static str = "inverse_perpetual";
}

impl EconomicsModelId {
    pub const FX_QUOTE_LINEAR_V1: &'static str = "fx_quote_linear_v1";
    pub const CFD_QUOTE_LINEAR_V1: &'static str = "cfd_quote_linear_v1";
    pub const LINEAR_BASE_QUANTITY_V1: &'static str = "linear_base_quantity_v1";
    pub const CASH_SPOT_INVENTORY_V1: &'static str = "cash_spot_inventory_v1";
    pub const LINEAR_CONTRACT_V1: &'static str = "linear_contract_v1";
    pub const INVERSE_CONTRACT_V1: &'static str = "inverse_contract_v1";
}

fn validate_spec_revision(value: &str) -> Result<(), IdentifierError> {
    if value.is_empty() || value.len() > MAX_SCOPED_ID_LEN || !value.is_ascii() {
        return Err(IdentifierError::InvalidLength {
            kind: "spec revision",
            minimum: 1,
            maximum: MAX_SCOPED_ID_LEN,
            actual: value.len(),
        });
    }

    let (without_build, build) = value
        .split_once('+')
        .map_or((value, None), |(core, build)| (core, Some(build)));
    if build.is_some_and(|build| !valid_semantic_identifiers(build, false)) {
        return Err(IdentifierError::InvalidSemanticRevision);
    }
    let (core, prerelease) = without_build
        .split_once('-')
        .map_or((without_build, None), |(core, prerelease)| {
            (core, Some(prerelease))
        });
    if prerelease.is_some_and(|prerelease| !valid_semantic_identifiers(prerelease, true)) {
        return Err(IdentifierError::InvalidSemanticRevision);
    }

    let components = core.split('.').collect::<Vec<_>>();
    if components.len() != 3
        || components.iter().any(|component| {
            component.is_empty()
                || !component.bytes().all(|byte| byte.is_ascii_digit())
                || (component.len() > 1 && component.starts_with('0'))
        })
    {
        return Err(IdentifierError::InvalidSemanticRevision);
    }
    Ok(())
}

fn valid_semantic_identifiers(value: &str, reject_numeric_leading_zero: bool) -> bool {
    !value.is_empty()
        && value.split('.').all(|identifier| {
            !identifier.is_empty()
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && !(reject_numeric_leading_zero
                    && identifier.len() > 1
                    && identifier.bytes().all(|byte| byte.is_ascii_digit())
                    && identifier.starts_with('0'))
        })
}

/// A broker-, exchange-, or internally-owned listing identity.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentId {
    pub listing_venue: ListingVenueId,
    pub market_kind: MarketKind,
    pub listing: ListingId,
}

impl InstrumentId {
    pub fn new(listing_venue: ListingVenueId, market_kind: MarketKind, listing: ListingId) -> Self {
        Self {
            listing_venue,
            market_kind,
            listing,
        }
    }
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.listing_venue, self.market_kind, self.listing
        )
    }
}

impl FromStr for InstrumentId {
    type Err = InstrumentIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut components = value.split('/');
        let listing_venue = components.next().ok_or(InstrumentIdError::InvalidFormat)?;
        let market_kind = components.next().ok_or(InstrumentIdError::InvalidFormat)?;
        let listing = components.next().ok_or(InstrumentIdError::InvalidFormat)?;
        if components.next().is_some() {
            return Err(InstrumentIdError::InvalidFormat);
        }
        Ok(Self::new(
            listing_venue.parse()?,
            market_kind.parse()?,
            listing.parse()?,
        ))
    }
}

/// Validation failures for normalized identifiers.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IdentifierError {
    #[error("{kind} length must be between {minimum} and {maximum} bytes, got {actual}")]
    InvalidLength {
        kind: &'static str,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    #[error("{kind} contains an unsupported character")]
    InvalidCharacter { kind: &'static str },
    #[error("{kind} uses noncanonical letter case")]
    NonCanonicalCase { kind: &'static str },
    #[error("spec revision must be a semantic version such as 1.0.0")]
    InvalidSemanticRevision,
}

/// Parsing failures for the display form of an instrument identity.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentIdError {
    #[error("instrument ID must have listing-venue/market-kind/listing form")]
    InvalidFormat,
    #[error(transparent)]
    InvalidComponent(#[from] IdentifierError),
}
