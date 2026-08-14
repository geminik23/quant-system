use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AssetId, DecimalGrid, EconomicsModelId, InstrumentAlias, InstrumentId, PositiveDecimal,
    SpecRevision,
};

/// Broad asset classification separate from an asset identifier.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetKind {
    Fiat,
    Stablecoin,
    Crypto,
    Commodity,
    Equity,
    Index,
    Synthetic,
    Other,
}

/// Metadata for one economic asset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetSpec {
    pub asset: AssetId,
    pub kind: AssetKind,
    pub display_code: String,
    pub storage_scale: Option<u8>,
}

/// Assets participating in an instrument's economic contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentAssets {
    pub base: Option<AssetId>,
    pub quote: Option<AssetId>,
    pub settlement: AssetId,
    #[serde(default)]
    pub fee_assets: BTreeSet<AssetId>,
}

/// Listing availability at an effective time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingStatus {
    PreTrading,
    Trading,
    ReduceOnly,
    Halted,
    Delisted,
}

/// Exact price-grid metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceRules {
    pub grid: DecimalGrid,
    pub display_scale: u8,
}

/// Exact quantity-grid metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantityRules {
    pub grid: DecimalGrid,
    pub minimum: PositiveDecimal,
    pub maximum: Option<PositiveDecimal>,
    pub storage_scale: u8,
}

/// Optional bounds for a model-calculated notional value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotionalRules {
    pub asset: AssetId,
    pub minimum: Option<PositiveDecimal>,
    pub maximum: Option<PositiveDecimal>,
}

/// Unit represented by an instrument quantity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityUnit {
    StandardLot,
    BaseAsset,
    Contract,
    QuoteAsset,
}

/// Declarative economics requirements for one instrument revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentEconomics {
    pub pnl_model: EconomicsModelId,
    pub quantity_unit: QuantityUnit,
    pub contract_multiplier: PositiveDecimal,
    pub settlement_asset: AssetId,
    pub fee_model: Option<EconomicsModelId>,
    pub funding_model: Option<EconomicsModelId>,
    pub margin_model: Option<EconomicsModelId>,
}

/// Half-open UTC interval `[valid_from, valid_until)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveInterval {
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
}

impl EffectiveInterval {
    pub fn new(
        valid_from: DateTime<Utc>,
        valid_until: Option<DateTime<Utc>>,
    ) -> Result<Self, EffectiveIntervalError> {
        if valid_until.is_some_and(|until| until <= valid_from) {
            return Err(EffectiveIntervalError::InvalidBounds);
        }
        Ok(Self {
            valid_from,
            valid_until,
        })
    }

    pub fn contains(&self, at: DateTime<Utc>) -> bool {
        at >= self.valid_from && self.valid_until.is_none_or(|until| at < until)
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.valid_until
            .is_none_or(|until| other.valid_from < until)
            && other
                .valid_until
                .is_none_or(|until| self.valid_from < until)
    }
}

/// One effective-dated instrument specification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentSpec {
    pub revision: SpecRevision,
    pub instrument: InstrumentId,
    pub effective: EffectiveInterval,
    pub status: ListingStatus,
    pub assets: InstrumentAssets,
    pub price: PriceRules,
    pub quantity: QuantityRules,
    pub notional: Option<NotionalRules>,
    pub economics: InstrumentEconomics,
    #[serde(default)]
    pub aliases: BTreeSet<InstrumentAlias>,
}

impl AssetSpec {
    pub fn validate(&self) -> Result<(), SpecValidationError> {
        if self.display_code.is_empty() || !self.display_code.is_ascii() {
            return Err(SpecValidationError::InvalidAssetDisplayCode {
                asset: self.asset.clone(),
            });
        }
        if self
            .storage_scale
            .is_some_and(|scale| scale > crate::MAX_DECIMAL_SCALE)
        {
            return Err(SpecValidationError::ScaleTooLarge);
        }
        Ok(())
    }
}

impl InstrumentSpec {
    pub fn validate(&self) -> Result<(), SpecValidationError> {
        if self
            .effective
            .valid_until
            .is_some_and(|until| until <= self.effective.valid_from)
        {
            return Err(SpecValidationError::InvalidEffectiveInterval);
        }
        if self.price.display_scale > crate::MAX_DECIMAL_SCALE
            || self.quantity.storage_scale > crate::MAX_DECIMAL_SCALE
        {
            return Err(SpecValidationError::ScaleTooLarge);
        }
        if !self.quantity.grid.contains(self.quantity.minimum.get())? {
            return Err(SpecValidationError::QuantityMinimumOffGrid);
        }
        if let Some(maximum) = self.quantity.maximum {
            if maximum < self.quantity.minimum {
                return Err(SpecValidationError::InvalidQuantityBounds);
            }
            if !self.quantity.grid.contains(maximum.get())? {
                return Err(SpecValidationError::QuantityMaximumOffGrid);
            }
        }
        if let Some(notional) = &self.notional {
            validate_optional_bounds(notional.minimum, notional.maximum)?;
        }
        if self.assets.settlement != self.economics.settlement_asset {
            return Err(SpecValidationError::SettlementAssetMismatch);
        }
        Ok(())
    }
}

fn validate_optional_bounds(
    minimum: Option<PositiveDecimal>,
    maximum: Option<PositiveDecimal>,
) -> Result<(), SpecValidationError> {
    if let (Some(minimum), Some(maximum)) = (minimum, maximum)
        && maximum < minimum
    {
        return Err(SpecValidationError::InvalidNotionalBounds);
    }
    Ok(())
}

/// Effective-interval construction failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EffectiveIntervalError {
    #[error("effective interval valid_until must be after valid_from")]
    InvalidBounds,
}

/// Instrument specification invariant failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SpecValidationError {
    #[error("asset {asset} has an invalid display code")]
    InvalidAssetDisplayCode { asset: AssetId },
    #[error("effective interval valid_until must be after valid_from")]
    InvalidEffectiveInterval,
    #[error("declared scale exceeds the supported maximum")]
    ScaleTooLarge,
    #[error("quantity minimum is outside the declared grid")]
    QuantityMinimumOffGrid,
    #[error("quantity maximum is outside the declared grid")]
    QuantityMaximumOffGrid,
    #[error("quantity maximum is below quantity minimum")]
    InvalidQuantityBounds,
    #[error("notional maximum is below notional minimum")]
    InvalidNotionalBounds,
    #[error("instrument and economics settlement assets differ")]
    SettlementAssetMismatch,
    #[error(transparent)]
    Grid(#[from] crate::GridError),
    #[error(transparent)]
    Decimal(#[from] crate::DecimalError),
}
