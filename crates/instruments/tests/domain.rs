use std::collections::BTreeSet;

use chrono::{DateTime, TimeDelta, Utc};
use qs_instruments::*;

fn time(value: &str) -> DateTime<Utc> {
    value.parse().unwrap()
}

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn positive(value: &str) -> PositiveDecimal {
    value.parse().unwrap()
}

fn asset(code: &str, kind: AssetKind, storage_scale: u8) -> AssetSpec {
    AssetSpec {
        asset: code.parse().unwrap(),
        kind,
        display_code: code.into(),
        storage_scale: Some(storage_scale),
    }
}

fn instrument(venue: &str, market_kind: &str, listing: &str) -> InstrumentId {
    InstrumentId::new(
        venue.parse().unwrap(),
        market_kind.parse().unwrap(),
        listing.parse().unwrap(),
    )
}

fn spec(
    instrument: InstrumentId,
    valid_from: &str,
    valid_until: Option<&str>,
    alias: &str,
    storage_scale: u8,
    multiplier: &str,
) -> InstrumentSpec {
    let usd: AssetId = "USD".parse().unwrap();
    InstrumentSpec {
        revision: "1.0.0".parse().unwrap(),
        instrument,
        effective: EffectiveInterval::new(time(valid_from), valid_until.map(time)).unwrap(),
        status: ListingStatus::Trading,
        assets: InstrumentAssets {
            base: Some("EUR".parse().unwrap()),
            quote: Some(usd.clone()),
            settlement: usd.clone(),
            fee_assets: BTreeSet::from([usd.clone()]),
        },
        price: PriceRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.00001")),
            display_scale: 5,
        },
        quantity: QuantityRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.01")),
            minimum: positive("0.01"),
            maximum: Some(positive("100")),
            storage_scale,
        },
        notional: None,
        economics: InstrumentEconomics {
            pnl_model: EconomicsModelId::new(EconomicsModelId::FX_QUOTE_LINEAR_V1).unwrap(),
            quantity_unit: QuantityUnit::StandardLot,
            contract_multiplier: positive(multiplier),
            settlement_asset: usd,
            fee_model: None,
            funding_model: None,
            margin_model: None,
        },
        aliases: BTreeSet::from([alias.parse().unwrap()]),
    }
}

fn document(instruments: Vec<InstrumentSpec>) -> CatalogDocument {
    CatalogDocument {
        schema_version: 1,
        version: "2026.08".into(),
        assets: vec![
            asset("USD", AssetKind::Fiat, 2),
            asset("EUR", AssetKind::Fiat, 2),
        ],
        instruments,
    }
}

fn context(instruments: impl IntoIterator<Item = InstrumentId>) -> InstrumentResolutionContext {
    InstrumentResolutionContext {
        allowed_instruments: instruments.into_iter().collect(),
        default_listing_venue: None,
        default_market_kind: None,
    }
}

#[test]
fn broker_owns_listing_while_ctrader_is_only_the_platform() {
    let instrument = instrument("ic-markets", MarketKind::FX_CFD, "EURUSD");
    assert_eq!(instrument.to_string(), "ic-markets/fx_cfd/EURUSD");
    assert_eq!(instrument, instrument.to_string().parse().unwrap());

    let json = serde_json::to_value(&instrument).unwrap();
    assert_eq!(json["listing_venue"], "ic-markets");
    assert!(json.get("platform").is_none());

    let snapshot = InstrumentCatalogSnapshot::compile(document(vec![spec(
        instrument.clone(),
        "2026-01-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    )]))
    .unwrap();
    let resolved = snapshot
        .spec_at(&instrument, time("2026-02-01T00:00:00Z"))
        .unwrap();
    let binding = PlatformInstrumentBinding {
        execution_venue: "ic-markets-demo".parse().unwrap(),
        platform: "ctrader".parse().unwrap(),
        platform_instrument_id: "1".into(),
        instrument: resolved.reference,
        effective: EffectiveInterval::new(time("2026-01-01T00:00:00Z"), None).unwrap(),
    };
    assert_eq!(binding.platform.as_str(), "ctrader");
    assert_eq!(
        binding.instrument.instrument.listing_venue.as_str(),
        "ic-markets"
    );
}

#[test]
fn identifiers_normalize_and_use_validated_string_serde() {
    for code in ["USD", "EUR", "USDT", "USDC", "BTC"] {
        assert_eq!(AssetId::new(code).unwrap().as_str(), code);
    }
    let asset: AssetId = " usdt ".parse().unwrap();
    let alias: InstrumentAlias = " btc/usdt ".parse().unwrap();
    assert_eq!(asset.as_str(), "USDT");
    assert_eq!(alias.as_str(), "BTC/USDT");
    assert_eq!(serde_json::to_string(&asset).unwrap(), "\"USDT\"");
    assert!(serde_json::from_str::<AssetId>("\"A\"").is_err());
    assert!(AssetId::new("").is_err());
    assert!(AssetId::new("US/D").is_err());
    assert!(AssetId::new("US\nD").is_err());
    assert!("bad venue".parse::<ListingVenueId>().is_err());
    assert!("1.0".parse::<SpecRevision>().is_err());
}

#[test]
fn decimal_is_exact_checked_and_canonical() {
    let value: Decimal = "001.2300".parse().unwrap();
    assert_eq!(value.to_string(), "1.23");
    assert_eq!(serde_json::to_string(&value).unwrap(), "\"1.23\"");
    assert_eq!(
        decimal("0.1").checked_add(decimal("0.2")).unwrap(),
        decimal("0.3")
    );
    assert_eq!(
        decimal("1.25").checked_mul(decimal("2")).unwrap(),
        decimal("2.5")
    );
    assert!("1e-3".parse::<Decimal>().is_err());
    assert!("-0.0".parse::<Decimal>().is_err());
    assert!("0.0000000000000000001".parse::<Decimal>().is_err());
    assert!(
        Decimal::new(i128::MAX, 0)
            .unwrap()
            .checked_add(decimal("1"))
            .is_err()
    );
    assert!(decimal("1.01").checked_rescale(1).is_err());
}

#[test]
fn decimal_ordering_handles_alignment_overflow_without_panicking() {
    let large = Decimal::new(i128::MAX, 0).unwrap();
    let smaller = Decimal::new(i128::MAX / 10, 0).unwrap();
    assert!(large > smaller);
    assert!(decimal("-10") < decimal("-1.5"));
    assert!(decimal("100000000000000000000") > decimal("0.000000000000000001"));
}

#[test]
fn grid_rounding_is_explicit_and_auditable() {
    let grid = DecimalGrid::new(decimal("0.005"), positive("0.01"));
    assert!(grid.contains(decimal("0.025")).unwrap());
    assert!(!grid.contains(decimal("0.026")).unwrap());
    assert!(grid.adjust(decimal("0.026"), GridRounding::Reject).is_err());

    let floor = grid.adjust(decimal("0.026"), GridRounding::Floor).unwrap();
    assert_eq!(floor.requested, decimal("0.026"));
    assert_eq!(floor.adjusted, decimal("0.025"));
    assert_eq!(floor.direction, AdjustmentDirection::Down);

    let ceil = grid.adjust(decimal("-0.006"), GridRounding::Ceil).unwrap();
    assert_eq!(ceil.adjusted, decimal("-0.005"));
    assert_eq!(ceil.direction, AdjustmentDirection::Up);
}

#[test]
fn effective_intervals_are_half_open() {
    let start = time("2026-01-01T00:00:00Z");
    let end = time("2026-02-01T00:00:00Z");
    let interval = EffectiveInterval::new(start, Some(end)).unwrap();
    assert!(interval.contains(start));
    assert!(interval.contains(end - TimeDelta::nanoseconds(1)));
    assert!(!interval.contains(end));
    let adjacent = EffectiveInterval::new(end, None).unwrap();
    assert!(!interval.overlaps(&adjacent));
    assert!(EffectiveInterval::new(start, Some(start)).is_err());
}

#[test]
fn catalog_rejects_overlap_unknown_assets_and_invalid_rules() {
    let id = instrument("broker-a", MarketKind::FX_CFD, "EURUSD");
    let first = spec(
        id.clone(),
        "2026-01-01T00:00:00Z",
        Some("2026-03-01T00:00:00Z"),
        "EURUSD",
        2,
        "100000",
    );
    let second = spec(
        id.clone(),
        "2026-02-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    );
    assert!(matches!(
        InstrumentCatalogSnapshot::compile(document(vec![first, second])),
        Err(CatalogCompileError::OverlappingInterval { .. })
    ));

    let mut unknown = spec(
        id.clone(),
        "2026-01-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    );
    unknown.assets.settlement = "USDC".parse().unwrap();
    unknown.economics.settlement_asset = "USDC".parse().unwrap();
    assert!(matches!(
        InstrumentCatalogSnapshot::compile(document(vec![unknown])),
        Err(CatalogCompileError::UnknownAsset { .. })
    ));

    let mut invalid = spec(id, "2026-01-01T00:00:00Z", None, "EURUSD", 2, "100000");
    invalid.quantity.minimum = positive("0.015");
    assert!(matches!(
        InstrumentCatalogSnapshot::compile(document(vec![invalid])),
        Err(CatalogCompileError::InvalidSpec(
            SpecValidationError::QuantityMinimumOffGrid
        ))
    ));
}

#[test]
fn alias_resolution_reports_ambiguity_and_honors_defaults_allowlist_and_inactivity() {
    let a = instrument("broker-a", MarketKind::FX_CFD, "EURUSD");
    let b = instrument("broker-b", MarketKind::FX_CFD, "EURUSD");
    let snapshot = InstrumentCatalogSnapshot::compile(document(vec![
        spec(
            a.clone(),
            "2026-01-01T00:00:00Z",
            Some("2027-01-01T00:00:00Z"),
            "EURUSD",
            2,
            "100000",
        ),
        spec(
            b.clone(),
            "2026-01-01T00:00:00Z",
            None,
            "EURUSD",
            2,
            "100000",
        ),
    ]))
    .unwrap();
    let selector = InstrumentSelector::Alias {
        alias: "EURUSD".parse().unwrap(),
        listing_venue: None,
        market_kind: None,
    };

    let error = snapshot
        .resolve(
            &selector,
            &context([a.clone(), b.clone()]),
            time("2026-06-01T00:00:00Z"),
        )
        .unwrap_err();
    assert_eq!(
        error,
        InstrumentResolutionError::Ambiguous {
            candidates: vec![a.clone(), b.clone()]
        }
    );

    let mut defaulted = context([a.clone(), b.clone()]);
    defaulted.default_listing_venue = Some("broker-b".parse().unwrap());
    assert_eq!(
        snapshot
            .resolve(&selector, &defaulted, time("2026-06-01T00:00:00Z"))
            .unwrap()
            .reference
            .instrument,
        b
    );

    assert_eq!(
        snapshot
            .resolve(
                &selector,
                &context([a.clone()]),
                time("2026-06-01T00:00:00Z")
            )
            .unwrap()
            .reference
            .instrument,
        a
    );
    assert!(matches!(
        snapshot.resolve(&selector, &context([]), time("2026-06-01T00:00:00Z")),
        Err(InstrumentResolutionError::Disallowed { .. })
    ));
    assert!(matches!(
        snapshot.spec_at(&a, time("2027-01-01T00:00:00Z")),
        Err(InstrumentResolutionError::Inactive { .. })
    ));
    assert!(matches!(
        snapshot.spec_at(
            &instrument("unknown", MarketKind::FX_CFD, "EURUSD"),
            time("2026-06-01T00:00:00Z")
        ),
        Err(InstrumentResolutionError::Unknown)
    ));
}

struct Provider {
    id: EconomicsImplementationId,
    model: EconomicsModelId,
    operations: BTreeSet<EconomicOperation>,
}

impl EconomicsCapabilityProvider for Provider {
    fn implementation_id(&self) -> &EconomicsImplementationId {
        &self.id
    }

    fn supports(&self, model: &EconomicsModelId, operation: EconomicOperation) -> bool {
        model == &self.model && self.operations.contains(&operation)
    }
}

#[test]
fn same_alias_on_one_venue_requires_market_kind_scope() {
    let spot = instrument("exchange-a", MarketKind::CASH_SPOT, "BTCUSDT");
    let perpetual = instrument("exchange-a", MarketKind::LINEAR_PERPETUAL, "BTCUSDT");
    let spot_spec = spec(
        spot.clone(),
        "2026-01-01T00:00:00Z",
        None,
        "BTCUSDT",
        3,
        "1",
    );
    let mut perpetual_spec = spot_spec.clone();
    perpetual_spec.instrument = perpetual.clone();
    let snapshot =
        InstrumentCatalogSnapshot::compile(document(vec![spot_spec, perpetual_spec])).unwrap();
    let selector = InstrumentSelector::Alias {
        alias: "BTCUSDT".parse().unwrap(),
        listing_venue: Some("exchange-a".parse().unwrap()),
        market_kind: None,
    };
    assert!(matches!(
        snapshot.resolve(
            &selector,
            &context([spot.clone(), perpetual.clone()]),
            time("2026-06-01T00:00:00Z")
        ),
        Err(InstrumentResolutionError::Ambiguous { .. })
    ));

    let scoped = InstrumentSelector::Alias {
        alias: "BTCUSDT".parse().unwrap(),
        listing_venue: Some("exchange-a".parse().unwrap()),
        market_kind: Some(MarketKind::new(MarketKind::LINEAR_PERPETUAL).unwrap()),
    };
    assert_eq!(
        snapshot
            .resolve(
                &scoped,
                &context([spot, perpetual.clone()]),
                time("2026-06-01T00:00:00Z")
            )
            .unwrap()
            .reference
            .instrument,
        perpetual
    );
}

#[test]
fn economics_binding_requires_code_backed_support_per_operation() {
    let id = instrument("broker-a", MarketKind::FX_CFD, "EURUSD");
    let snapshot = InstrumentCatalogSnapshot::compile(document(vec![spec(
        id.clone(),
        "2026-01-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    )]))
    .unwrap();
    let resolved = snapshot.spec_at(&id, time("2026-06-01T00:00:00Z")).unwrap();
    let provider = Provider {
        id: "quote-linear-rust-v1".parse().unwrap(),
        model: EconomicsModelId::new(EconomicsModelId::FX_QUOTE_LINEAR_V1).unwrap(),
        operations: BTreeSet::from([
            EconomicOperation::PositionValue,
            EconomicOperation::RealizedPnl,
        ]),
    };

    let binding = bind_economics(
        &resolved.reference,
        &resolved.spec.economics,
        [
            EconomicOperation::PositionValue,
            EconomicOperation::RealizedPnl,
        ],
        &[&provider],
    )
    .unwrap();
    assert_eq!(binding.capabilities.len(), 2);
    assert!(matches!(
        bind_economics(
            &resolved.reference,
            &resolved.spec.economics,
            [EconomicOperation::UnrealizedPnl],
            &[]
        ),
        Err(EconomicsCapabilityError::Unsupported { .. })
    ));
    assert!(matches!(
        bind_economics(
            &resolved.reference,
            &resolved.spec.economics,
            [EconomicOperation::Fees],
            &[&provider]
        ),
        Err(EconomicsCapabilityError::MissingModel { .. })
    ));
}

#[test]
fn storage_scale_does_not_change_economics() {
    let id = instrument("broker-a", MarketKind::FX_CFD, "EURUSD");
    let low_scale = spec(
        id.clone(),
        "2026-01-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    );
    let high_scale = spec(id, "2026-01-01T00:00:00Z", None, "EURUSD", 8, "100000");
    assert_eq!(low_scale.economics, high_scale.economics);
    assert_ne!(
        low_scale.quantity.storage_scale,
        high_scale.quantity.storage_scale
    );
}

#[test]
fn alias_is_effective_dated_with_the_spec_that_declares_it() {
    let id = instrument("broker-a", MarketKind::FX_CFD, "EURUSD");
    let old = spec(
        id.clone(),
        "2026-01-01T00:00:00Z",
        Some("2026-07-01T00:00:00Z"),
        "OLD-EURUSD",
        2,
        "100000",
    );
    let current = spec(
        id.clone(),
        "2026-07-01T00:00:00Z",
        None,
        "EURUSD",
        2,
        "100000",
    );
    let snapshot = InstrumentCatalogSnapshot::compile(document(vec![old, current])).unwrap();
    let selector = InstrumentSelector::Alias {
        alias: "OLD-EURUSD".parse().unwrap(),
        listing_venue: None,
        market_kind: None,
    };

    assert!(
        snapshot
            .resolve(
                &selector,
                &context([id.clone()]),
                time("2026-06-01T00:00:00Z")
            )
            .is_ok()
    );
    assert!(matches!(
        snapshot.resolve(&selector, &context([id]), time("2026-08-01T00:00:00Z")),
        Err(InstrumentResolutionError::Inactive { .. })
    ));
}

#[test]
fn strict_catalog_and_instrument_serde_reject_unknown_fields() {
    let instrument_json = r#"{
        "listing_venue":"broker-a",
        "market_kind":"fx_cfd",
        "listing":"EURUSD",
        "unexpected":true
    }"#;
    assert!(serde_json::from_str::<InstrumentId>(instrument_json).is_err());

    let document_json = r#"{
        "schema_version":1,
        "version":"1",
        "assets":[],
        "instruments":[],
        "unexpected":true
    }"#;
    assert!(serde_json::from_str::<CatalogDocument>(document_json).is_err());
}
