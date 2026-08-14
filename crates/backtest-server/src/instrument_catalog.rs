use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use qs_backtest::{
    ReplayInstrumentArtifact, ReplayInstrumentManifest, guarded_instrument_spec,
    resolve_legacy_economics,
};
use qs_instruments::{
    AssetId, AssetKind, AssetSpec, CatalogDocument, EconomicsModelId, EffectiveInterval,
    InstrumentAlias, InstrumentCatalogSnapshot, InstrumentId, InstrumentResolutionContext,
    InstrumentSelector, ListingStatus, ListingVenueId, MarketDataSourceId, MarketKind,
    QuantityUnit, StoredSeriesBinding,
};
use qs_symbols::{SymbolCurrencyMetadata, SymbolRegistry, SymbolSpec};

use crate::config::InstrumentsSection;
use crate::error::{BacktestServerError, Result};

const CATALOG_SCHEMA_VERSION: u32 = 1;
const COMPATIBILITY_LISTING_VENUE: &str = "repository-default";
const REGISTRY_CATALOG_VERSION: &str = "symbol-registry-1";
const SPEC_VALID_FROM: &str = "1970-01-01T00:00:00Z";

#[derive(Clone)]
pub struct InstrumentDomain {
    snapshot: Arc<InstrumentCatalogSnapshot>,
    default_listing_venue: Option<ListingVenueId>,
    data_source: MarketDataSourceId,
}

impl InstrumentDomain {
    pub fn load(config: &InstrumentsSection, registry: &SymbolRegistry) -> Result<Self> {
        let configured_listing_venue = config
            .default_listing_venue
            .as_deref()
            .map(ListingVenueId::new)
            .transpose()
            .map_err(|error| BacktestServerError::Config(error.to_string()))?;
        let data_source = MarketDataSourceId::new(&config.market_data_source)
            .map_err(|error| BacktestServerError::Config(error.to_string()))?;
        let (snapshot, default_listing_venue) = match &config.catalog_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|error| {
                    BacktestServerError::Config(format!(
                        "failed to read instrument catalog '{path}': {error}"
                    ))
                })?;
                let document = toml::from_str::<CatalogDocument>(&content).map_err(|error| {
                    BacktestServerError::Config(format!(
                        "failed to parse instrument catalog '{path}': {error}"
                    ))
                })?;
                let snapshot = InstrumentCatalogSnapshot::compile(document).map_err(|error| {
                    BacktestServerError::Config(format!(
                        "invalid instrument catalog '{path}': {error}"
                    ))
                })?;
                (snapshot, configured_listing_venue)
            }
            None => {
                let listing_venue = configured_listing_venue.unwrap_or_else(|| {
                    ListingVenueId::new(COMPATIBILITY_LISTING_VENUE)
                        .expect("valid built-in compatibility listing venue")
                });
                (
                    compatibility_snapshot(registry, &listing_venue)?,
                    Some(listing_venue),
                )
            }
        };
        Ok(Self {
            snapshot: Arc::new(snapshot),
            default_listing_venue,
            data_source,
        })
    }

    pub fn compatibility(registry: &SymbolRegistry) -> Result<Self> {
        Self::load(&InstrumentsSection::default(), registry)
    }

    pub fn snapshot(&self) -> &InstrumentCatalogSnapshot {
        &self.snapshot
    }

    pub fn data_source(&self) -> &MarketDataSourceId {
        &self.data_source
    }

    pub fn resolve_manifest(
        &self,
        symbols: &[String],
        at: NaiveDateTime,
        through: Option<NaiveDateTime>,
    ) -> Result<ReplayInstrumentManifest> {
        let allowed_instruments = self
            .snapshot
            .instrument_ids()
            .cloned()
            .collect::<BTreeSet<_>>();
        let context = InstrumentResolutionContext {
            allowed_instruments,
            default_listing_venue: self.default_listing_venue.clone(),
            default_market_kind: None,
        };
        let at = at.and_utc();
        let through = through.map(|value| value.and_utc());
        let mut instruments = BTreeMap::new();
        for symbol in symbols {
            let selector = InstrumentSelector::Alias {
                alias: InstrumentAlias::new(symbol).map_err(|error| {
                    BacktestServerError::InvalidRequest(format!(
                        "invalid instrument selector for '{symbol}': {error}"
                    ))
                })?,
                listing_venue: None,
                market_kind: None,
            };
            let resolved = self
                .snapshot
                .resolve(&selector, &context, at)
                .map_err(|error| {
                    BacktestServerError::InvalidRequest(format!(
                        "cannot resolve instrument '{symbol}': {error}"
                    ))
                })?;
            validate_replay_spec(symbol, &resolved.spec)?;
            if let Some(through) = through {
                let end = self
                    .snapshot
                    .resolve(&selector, &context, through)
                    .map_err(|error| {
                        BacktestServerError::InvalidRequest(format!(
                            "cannot resolve instrument '{symbol}' at replay end: {error}"
                        ))
                    })?;
                if end.reference != resolved.reference {
                    return Err(BacktestServerError::InvalidRequest(format!(
                        "instrument '{symbol}' changes specification during the requested replay range"
                    )));
                }
            }
            instruments.insert(
                symbol.clone(),
                ReplayInstrumentArtifact {
                    resolved: resolved.reference,
                    spec: resolved.spec.as_ref().clone(),
                },
            );
        }
        Ok(ReplayInstrumentManifest {
            instruments,
            stored_series: Vec::new(),
        })
    }

    pub fn attach_stored_series(
        &self,
        manifest: &mut ReplayInstrumentManifest,
        coordinates: impl IntoIterator<Item = (String, String, String)>,
    ) -> Result<()> {
        let mut bindings = Vec::new();
        for (symbol, source_partition, source_symbol) in coordinates {
            let artifact = manifest.instruments.get(&symbol).ok_or_else(|| {
                BacktestServerError::InvalidRequest(format!(
                    "stored series for '{symbol}' has no resolved instrument"
                ))
            })?;
            bindings.push(StoredSeriesBinding {
                data_source: self.data_source.clone(),
                source_partition,
                source_symbol,
                instrument: artifact.resolved.clone(),
                effective: artifact.spec.effective,
            });
        }
        bindings.sort_by(|left, right| {
            left.source_partition
                .cmp(&right.source_partition)
                .then(left.source_symbol.cmp(&right.source_symbol))
                .then(left.instrument.instrument.cmp(&right.instrument.instrument))
        });
        bindings.dedup();
        manifest.stored_series = bindings;
        Ok(())
    }
}

fn compatibility_snapshot(
    registry: &SymbolRegistry,
    listing_venue: &ListingVenueId,
) -> Result<InstrumentCatalogSnapshot> {
    let effective = EffectiveInterval::new(
        SPEC_VALID_FROM
            .parse::<DateTime<Utc>>()
            .expect("valid built-in instrument epoch"),
        None,
    )
    .expect("valid built-in instrument interval");
    let mut assets = BTreeMap::<AssetId, AssetSpec>::new();
    let mut instruments = Vec::new();
    for (symbol, currencies) in registry.entries() {
        let Ok(economics) = resolve_legacy_economics(symbol) else {
            continue;
        };
        register_assets(&mut assets, symbol, currencies)?;
        let instrument = InstrumentId::new(
            listing_venue.clone(),
            market_kind(symbol)?,
            symbol.canonical.parse().map_err(|error| {
                BacktestServerError::Config(format!(
                    "invalid listing ID for '{}': {error}",
                    symbol.canonical
                ))
            })?,
        );
        instruments.push(
            guarded_instrument_spec(symbol, currencies, economics, instrument, effective)
                .map_err(|error| BacktestServerError::Config(error.to_string()))?,
        );
    }
    InstrumentCatalogSnapshot::compile(CatalogDocument {
        schema_version: CATALOG_SCHEMA_VERSION,
        version: REGISTRY_CATALOG_VERSION.into(),
        assets: assets.into_values().collect(),
        instruments,
    })
    .map_err(|error| BacktestServerError::Config(error.to_string()))
}

fn register_assets(
    assets: &mut BTreeMap<AssetId, AssetSpec>,
    symbol: &SymbolSpec,
    currencies: &SymbolCurrencyMetadata,
) -> Result<()> {
    let values = [
        currencies.base_currency.as_deref(),
        currencies.quote_currency.as_deref(),
        Some(currencies.pnl_currency.as_str()),
    ];
    for value in values.into_iter().flatten() {
        let asset =
            AssetId::new(value).map_err(|error| BacktestServerError::Config(error.to_string()))?;
        let kind = match symbol.category.as_str() {
            "metal" | "commodity" if currencies.base_currency.as_deref() == Some(value) => {
                AssetKind::Commodity
            }
            _ => AssetKind::Fiat,
        };
        assets.entry(asset.clone()).or_insert(AssetSpec {
            asset,
            kind,
            display_code: value.to_ascii_uppercase(),
            storage_scale: None,
        });
    }
    Ok(())
}

fn market_kind(symbol: &SymbolSpec) -> Result<MarketKind> {
    let kind = match symbol.category.as_str() {
        "forex" => MarketKind::FX_CFD,
        "metal" => MarketKind::METAL_CFD,
        "commodity" => MarketKind::COMMODITY_CFD,
        "index" => MarketKind::INDEX_CFD,
        category => {
            return Err(BacktestServerError::Config(format!(
                "unsupported registry category '{category}' for {}",
                symbol.canonical
            )));
        }
    };
    MarketKind::new(kind).map_err(|error| BacktestServerError::Config(error.to_string()))
}

fn validate_replay_spec(symbol: &str, spec: &qs_instruments::InstrumentSpec) -> Result<()> {
    if spec.status != ListingStatus::Trading {
        return Err(BacktestServerError::InvalidRequest(format!(
            "instrument '{symbol}' is not in trading status"
        )));
    }
    if spec.economics.quantity_unit != QuantityUnit::StandardLot {
        return Err(BacktestServerError::InvalidRequest(format!(
            "unsupported quantity unit for instrument '{symbol}'"
        )));
    }
    let model = spec.economics.pnl_model.as_str();
    if model != EconomicsModelId::FX_QUOTE_LINEAR_V1
        && model != EconomicsModelId::CFD_QUOTE_LINEAR_V1
    {
        return Err(BacktestServerError::InvalidRequest(format!(
            "unsupported P&L model for instrument '{symbol}': {model}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{SystemTime, UNIX_EPOCH};

    use qs_instruments::{
        Decimal, DecimalGrid, InstrumentAssets, InstrumentEconomics, InstrumentSpec, PriceRules,
        QuantityRules,
    };

    use super::*;

    fn registry() -> SymbolRegistry {
        SymbolRegistry::from_toml(
            r#"
[[symbol]]
canonical = "eurusd"
aliases = ["eur/usd"]
pip_position = 4
digits = 5
category = "forex"
base_currency = "EUR"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000
lot_step_units = 1000

[[symbol]]
canonical = "btcusd"
aliases = ["btc/usd"]
pip_position = 1
digits = 2
category = "crypto"
base_currency = "BTC"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000000
lot_step_units = 100000
"#,
        )
        .unwrap()
    }

    #[test]
    fn compatibility_catalog_uses_owned_listing_namespace_not_data_platform() {
        let domain = InstrumentDomain::compatibility(&registry()).unwrap();
        let manifest = domain
            .resolve_manifest(
                &["eurusd".into()],
                "2026-01-01T00:00:00".parse().unwrap(),
                None,
            )
            .unwrap();
        let instrument = &manifest.instruments["eurusd"].resolved.instrument;
        assert_eq!(instrument.listing_venue.as_str(), "repository-default");
        assert_ne!(instrument.listing_venue.as_str(), "ctrader");
        assert_eq!(instrument.market_kind.as_str(), MarketKind::FX_CFD);
    }

    #[test]
    fn unsupported_registry_rows_do_not_enter_the_compatibility_catalog() {
        let domain = InstrumentDomain::compatibility(&registry()).unwrap();
        let error = domain
            .resolve_manifest(
                &["btcusd".into()],
                "2026-01-01T00:00:00".parse().unwrap(),
                None,
            )
            .unwrap_err();
        assert!(error.to_string().contains("cannot resolve instrument"));
    }

    #[test]
    fn explicit_catalog_loader_preserves_broker_listing_and_rejects_unknown_fields() {
        let usd: AssetId = "USD".parse().unwrap();
        let document = CatalogDocument {
            schema_version: 1,
            version: "broker-catalog-1".into(),
            assets: vec![
                AssetSpec {
                    asset: "EUR".parse().unwrap(),
                    kind: AssetKind::Fiat,
                    display_code: "EUR".into(),
                    storage_scale: Some(2),
                },
                AssetSpec {
                    asset: usd.clone(),
                    kind: AssetKind::Fiat,
                    display_code: "USD".into(),
                    storage_scale: Some(2),
                },
            ],
            instruments: vec![InstrumentSpec {
                revision: "1.0.0".parse().unwrap(),
                instrument: InstrumentId::new(
                    "ic-markets".parse().unwrap(),
                    MarketKind::new(MarketKind::FX_CFD).unwrap(),
                    "EURUSD".parse().unwrap(),
                ),
                effective: EffectiveInterval::new("2026-01-01T00:00:00Z".parse().unwrap(), None)
                    .unwrap(),
                status: ListingStatus::Trading,
                assets: InstrumentAssets {
                    base: Some("EUR".parse().unwrap()),
                    quote: Some(usd.clone()),
                    settlement: usd.clone(),
                    fee_assets: BTreeSet::new(),
                },
                price: PriceRules {
                    grid: DecimalGrid::new(Decimal::ZERO, "0.00001".parse().unwrap()),
                    display_scale: 5,
                },
                quantity: QuantityRules {
                    grid: DecimalGrid::new(Decimal::ZERO, "0.01".parse().unwrap()),
                    minimum: "0.01".parse().unwrap(),
                    maximum: Some("100".parse().unwrap()),
                    storage_scale: 2,
                },
                notional: None,
                economics: InstrumentEconomics {
                    pnl_model: EconomicsModelId::new(EconomicsModelId::FX_QUOTE_LINEAR_V1).unwrap(),
                    quantity_unit: QuantityUnit::StandardLot,
                    contract_multiplier: "100000".parse().unwrap(),
                    settlement_asset: usd,
                    fee_model: None,
                    funding_model: None,
                    margin_model: None,
                },
                aliases: BTreeSet::from(["EURUSD".parse().unwrap()]),
            }],
        };
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "qs_instrument_catalog_{}_{}.toml",
            std::process::id(),
            unique
        ));
        let content = toml::to_string(&document).unwrap();
        fs::write(&path, &content).unwrap();
        let config = InstrumentsSection {
            catalog_path: Some(path.to_string_lossy().into_owned()),
            default_listing_venue: None,
            market_data_source: "test-parquet".into(),
        };

        let domain = InstrumentDomain::load(&config, &registry()).unwrap();
        let manifest = domain
            .resolve_manifest(
                &["eurusd".into()],
                "2026-02-01T00:00:00".parse().unwrap(),
                None,
            )
            .unwrap();
        let instrument = &manifest.instruments["eurusd"].resolved.instrument;
        assert_eq!(instrument.listing_venue.as_str(), "ic-markets");
        assert_ne!(instrument.listing_venue.as_str(), "ctrader");

        fs::write(&path, format!("unexpected = true\n{content}")).unwrap();
        let error = match InstrumentDomain::load(&config, &registry()) {
            Ok(_) => panic!("catalog with an unknown field must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unknown field"));
        fs::remove_file(path).unwrap();
    }
}
