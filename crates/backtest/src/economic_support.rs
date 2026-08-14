//! Fail-closed capability checks for the legacy backtest economic model.
//!
//! `SymbolSpec` currently carries both lot-grid metadata and the value historically used as the per-lot P&L multiplier.
//! Registry-backed replay allows only categories whose existing contract-multiplier convention is intentionally supported until explicit instrument economics replace this compatibility model.
//! Registered crypto symbols remain useful for normalization and quantity metadata, but they are not economically executable through this model.

use std::collections::BTreeSet;

use qs_instruments::{
    AssetId, Decimal, DecimalGrid, EconomicsModelId, EffectiveInterval, InstrumentAlias,
    InstrumentAssets, InstrumentEconomics, InstrumentId, InstrumentSpec, ListingStatus,
    PositiveDecimal, PriceRules, QuantityRules, QuantityUnit, SpecRevision,
};
use qs_symbols::{SymbolCurrencyMetadata, SymbolSpec};

/// Stable identity recorded in replay metadata for the transitional economic guard.
pub const LEGACY_ECONOMIC_GUARD_ID: &str = "legacy-economic-guard-v1";

/// Economic models explicitly supported by the current contract-multiplier replay path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyEconomicModel {
    /// Quote-linear FX P&L using the configured standard-lot base-unit multiplier.
    FxLinearV1,
    /// Quote-linear CFD P&L using the configured per-lot contract multiplier.
    CfdLinearV1,
}

impl LegacyEconomicModel {
    /// Stable identifier used in execution metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FxLinearV1 => "legacy_fx_linear_v1",
            Self::CfdLinearV1 => "legacy_cfd_linear_v1",
        }
    }
}

/// Resolved economic capability for one registry-backed symbol.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SupportedLegacyEconomics {
    /// Explicit legacy P&L model selected for the symbol.
    pub model: LegacyEconomicModel,
    /// Monetary point-value multiplier used for one lot.
    pub contract_multiplier: f64,
}

/// A registry-backed symbol cannot use the current replay economic model.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EconomicSupportError {
    /// The category has no explicitly supported legacy P&L convention.
    #[error(
        "unsupported_economic_model: instrument {instrument} has category '{category}' and cannot use the legacy contract-multiplier P&L model"
    )]
    UnsupportedCategory {
        /// Canonical instrument name.
        instrument: String,
        /// Registry category that failed closed.
        category: String,
    },

    /// A supported category has unusable legacy multiplier metadata.
    #[error(
        "invalid_economic_multiplier: instrument {instrument} has invalid lot_base_units {lot_base_units}"
    )]
    InvalidContractMultiplier {
        /// Canonical instrument name.
        instrument: String,
        /// Configured value that would otherwise become the contract multiplier.
        lot_base_units: i64,
    },

    /// Compatibility symbol metadata cannot be represented by the neutral domain.
    #[error("invalid_compatibility_instrument: {0}")]
    InvalidCompatibilityInstrument(String),
}

/// Resolve the explicitly supported legacy economics for one symbol specification.
///
/// This is a transitional compatibility function. It does not infer crypto, spot, derivative,
/// fee, funding, margin, or liquidation behavior. Any category outside the current FX/CFD
/// allowlist fails closed.
pub fn resolve_legacy_economics(
    spec: &SymbolSpec,
) -> Result<SupportedLegacyEconomics, EconomicSupportError> {
    let model = match spec.category.as_str() {
        "forex" => LegacyEconomicModel::FxLinearV1,
        "metal" | "commodity" | "index" => LegacyEconomicModel::CfdLinearV1,
        _ => {
            return Err(EconomicSupportError::UnsupportedCategory {
                instrument: spec.canonical.clone(),
                category: spec.category.clone(),
            });
        }
    };

    if spec.lot_base_units <= 0 {
        return Err(EconomicSupportError::InvalidContractMultiplier {
            instrument: spec.canonical.clone(),
            lot_base_units: spec.lot_base_units,
        });
    }
    let contract_multiplier = spec.lot_base_units as f64;
    if !contract_multiplier.is_finite() || contract_multiplier <= 0.0 {
        return Err(EconomicSupportError::InvalidContractMultiplier {
            instrument: spec.canonical.clone(),
            lot_base_units: spec.lot_base_units,
        });
    }

    Ok(SupportedLegacyEconomics {
        model,
        contract_multiplier,
    })
}

/// Translate an already guarded compatibility symbol into an explicit neutral instrument spec.
///
/// Economic support and the contract multiplier come only from `economics`. Raw `lot_base_units`
/// is used only to preserve the existing lot grid after support has already been authorized.
pub fn guarded_instrument_spec(
    symbol: &SymbolSpec,
    currencies: &SymbolCurrencyMetadata,
    economics: SupportedLegacyEconomics,
    instrument: InstrumentId,
    effective: EffectiveInterval,
) -> Result<InstrumentSpec, EconomicSupportError> {
    let error = |message: String| EconomicSupportError::InvalidCompatibilityInstrument(message);
    if symbol.canonical.is_empty() || symbol.digits > 18 || symbol.pip_position > symbol.digits {
        return Err(error(format!(
            "invalid symbol precision for {}",
            symbol.canonical
        )));
    }
    if symbol.lot_base_units <= 0
        || symbol.lot_step_units <= 0
        || symbol.lot_min_steps <= 0
        || symbol.lot_max_steps < 0
        || (symbol.lot_max_steps > 0 && symbol.lot_max_steps < symbol.lot_min_steps)
    {
        return Err(error(format!(
            "invalid lot metadata for {}",
            symbol.canonical
        )));
    }
    if !economics.contract_multiplier.is_finite() || economics.contract_multiplier <= 0.0 {
        return Err(error(format!(
            "invalid guarded contract multiplier for {}",
            symbol.canonical
        )));
    }

    let settlement_code = if currencies.pnl_currency.is_empty() {
        currencies.quote_currency.as_deref()
    } else {
        Some(currencies.pnl_currency.as_str())
    }
    .ok_or_else(|| error(format!("missing settlement asset for {}", symbol.canonical)))?;
    let settlement = AssetId::new(settlement_code).map_err(|source| error(source.to_string()))?;
    let base = currencies
        .base_currency
        .as_deref()
        .map(AssetId::new)
        .transpose()
        .map_err(|source| error(source.to_string()))?;
    let quote = currencies
        .quote_currency
        .as_deref()
        .map(AssetId::new)
        .transpose()
        .map_err(|source| error(source.to_string()))?;
    let price_step = decimal_power_of_ten(symbol.digits)?;
    let quantity_step = decimal_ratio(symbol.lot_step_units, symbol.lot_base_units)?;
    let quantity_minimum = decimal_ratio(
        symbol
            .lot_step_units
            .checked_mul(symbol.lot_min_steps)
            .ok_or_else(|| error("minimum quantity overflow".into()))?,
        symbol.lot_base_units,
    )?;
    let quantity_maximum = if symbol.lot_max_steps == 0 {
        None
    } else {
        Some(
            PositiveDecimal::new(decimal_ratio(
                symbol
                    .lot_step_units
                    .checked_mul(symbol.lot_max_steps)
                    .ok_or_else(|| error("maximum quantity overflow".into()))?,
                symbol.lot_base_units,
            )?)
            .map_err(|source| error(source.to_string()))?,
        )
    };
    let quantity_storage_scale = quantity_maximum
        .map(|value| value.get().scale())
        .unwrap_or(0)
        .max(quantity_step.scale())
        .max(quantity_minimum.scale());
    let model = match economics.model {
        LegacyEconomicModel::FxLinearV1 => EconomicsModelId::FX_QUOTE_LINEAR_V1,
        LegacyEconomicModel::CfdLinearV1 => EconomicsModelId::CFD_QUOTE_LINEAR_V1,
    };
    let contract_multiplier = economics
        .contract_multiplier
        .to_string()
        .parse::<PositiveDecimal>()
        .map_err(|source| error(source.to_string()))?;
    let aliases = BTreeSet::from([
        InstrumentAlias::new(&symbol.canonical).map_err(|source| error(source.to_string()))?
    ]);

    let spec = InstrumentSpec {
        revision: SpecRevision::new("1.0.0").map_err(|source| error(source.to_string()))?,
        instrument,
        effective,
        status: ListingStatus::Trading,
        assets: InstrumentAssets {
            base,
            quote,
            settlement: settlement.clone(),
            fee_assets: BTreeSet::new(),
        },
        price: PriceRules {
            grid: DecimalGrid::new(
                Decimal::ZERO,
                PositiveDecimal::new(price_step).map_err(|source| error(source.to_string()))?,
            ),
            display_scale: symbol.digits as u8,
        },
        quantity: QuantityRules {
            grid: DecimalGrid::new(
                Decimal::ZERO,
                PositiveDecimal::new(quantity_step).map_err(|source| error(source.to_string()))?,
            ),
            minimum: PositiveDecimal::new(quantity_minimum)
                .map_err(|source| error(source.to_string()))?,
            maximum: quantity_maximum,
            storage_scale: quantity_storage_scale,
        },
        notional: None,
        economics: InstrumentEconomics {
            pnl_model: EconomicsModelId::new(model).map_err(|source| error(source.to_string()))?,
            quantity_unit: QuantityUnit::StandardLot,
            contract_multiplier,
            settlement_asset: settlement,
            fee_model: None,
            funding_model: None,
            margin_model: None,
        },
        aliases,
    };
    spec.validate()
        .map_err(|source| error(source.to_string()))?;
    Ok(spec)
}

fn decimal_power_of_ten(scale: u16) -> Result<Decimal, EconomicSupportError> {
    let scale = u8::try_from(scale).map_err(|_| {
        EconomicSupportError::InvalidCompatibilityInstrument("price scale is too large".into())
    })?;
    Decimal::new(1, scale)
        .map_err(|source| EconomicSupportError::InvalidCompatibilityInstrument(source.to_string()))
}

fn decimal_ratio(numerator: i64, denominator: i64) -> Result<Decimal, EconomicSupportError> {
    if numerator <= 0 || denominator <= 0 {
        return Err(EconomicSupportError::InvalidCompatibilityInstrument(
            "quantity ratio must be positive".into(),
        ));
    }
    let numerator = i128::from(numerator);
    let denominator = i128::from(denominator);
    let mut scaled = numerator;
    for scale in 0..=qs_instruments::MAX_DECIMAL_SCALE {
        if scaled % denominator == 0 {
            return Decimal::new(scaled / denominator, scale).map_err(|source| {
                EconomicSupportError::InvalidCompatibilityInstrument(source.to_string())
            });
        }
        scaled = scaled.checked_mul(10).ok_or_else(|| {
            EconomicSupportError::InvalidCompatibilityInstrument(
                "quantity ratio exceeds exact decimal range".into(),
            )
        })?;
    }
    Err(EconomicSupportError::InvalidCompatibilityInstrument(
        "quantity ratio cannot be represented exactly".into(),
    ))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use qs_symbols::{SymbolRegistry, SymbolSpec};

    use super::*;

    fn spec(symbol: &str, category: &str, lot_base_units: i64) -> SymbolSpec {
        SymbolSpec {
            canonical: symbol.into(),
            pip_position: 2,
            digits: 5,
            category: category.into(),
            lot_base_units,
            lot_step_units: 1,
            lot_min_steps: 1,
            lot_max_steps: 0,
        }
    }

    #[test]
    fn current_fx_and_cfd_categories_resolve_to_explicit_legacy_models() {
        let cases = [
            (
                spec("eurusd", "forex", 100_000),
                LegacyEconomicModel::FxLinearV1,
                100_000.0,
            ),
            (
                spec("xauusd", "metal", 100),
                LegacyEconomicModel::CfdLinearV1,
                100.0,
            ),
            (
                spec("xtiusd", "commodity", 100),
                LegacyEconomicModel::CfdLinearV1,
                100.0,
            ),
            (
                spec("us100", "index", 1),
                LegacyEconomicModel::CfdLinearV1,
                1.0,
            ),
        ];

        for (spec, expected_model, expected_multiplier) in cases {
            let resolved = resolve_legacy_economics(&spec).unwrap();
            assert_eq!(resolved.model, expected_model);
            assert_eq!(resolved.contract_multiplier, expected_multiplier);
        }
    }

    #[test]
    fn every_shipped_crypto_symbol_fails_closed() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../symbols/symbols.toml");
        let registry = SymbolRegistry::load(path).unwrap();
        let crypto = registry.symbols_in_category("crypto");
        assert_eq!(
            crypto.len(),
            4,
            "update the crypto economic inventory when the catalog changes"
        );

        let mut rejected = crypto
            .into_iter()
            .map(|spec| {
                let symbol = spec.canonical.clone();
                let error = resolve_legacy_economics(spec).unwrap_err();
                assert!(matches!(
                    error,
                    EconomicSupportError::UnsupportedCategory { .. }
                ));
                symbol
            })
            .collect::<Vec<_>>();
        rejected.sort();
        assert_eq!(rejected, ["btcusd", "dotusd", "ethusd", "solusd"]);
    }

    #[test]
    fn unknown_category_fails_closed() {
        let error = resolve_legacy_economics(&spec("mystery", "synthetic", 1)).unwrap_err();
        assert_eq!(
            error.to_string(),
            "unsupported_economic_model: instrument mystery has category 'synthetic' and cannot use the legacy contract-multiplier P&L model"
        );
    }

    #[test]
    fn invalid_supported_multiplier_is_rejected() {
        let error = resolve_legacy_economics(&spec("eurusd", "forex", 0)).unwrap_err();
        assert!(matches!(
            error,
            EconomicSupportError::InvalidContractMultiplier {
                lot_base_units: 0,
                ..
            }
        ));
    }
}
