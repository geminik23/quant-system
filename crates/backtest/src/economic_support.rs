//! Fail-closed capability checks for the legacy backtest economic model.
//!
//! `SymbolSpec` currently carries both lot-grid metadata and the value historically used as the per-lot P&L multiplier.
//! Registry-backed replay allows only categories whose existing contract-multiplier convention is intentionally supported until explicit instrument economics replace this compatibility model.
//! Registered crypto symbols remain useful for normalization and quantity metadata, but they are not economically executable through this model.

use qs_symbols::SymbolSpec;

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
