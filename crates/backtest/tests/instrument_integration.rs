use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::{
    BacktestRunner, FutureBacktestArtifacts, FutureQuoteConfig, MarketEvent, PositionRef,
    RawSignal, ReplayInstrumentArtifact, ReplayInstrumentManifest, VecFeed,
    guarded_instrument_spec, resolve_legacy_economics,
};
use qs_core::{OrderType, Side, SizingPolicy};
use qs_instruments::{
    CatalogSnapshotId, EconomicsModelId, EffectiveInterval, InstrumentId, MarketDataSourceId,
    QuantityUnit, ResolvedInstrumentRef, StoredSeriesBinding,
};
use qs_symbols::{SymbolCurrencyMetadata, SymbolSpec};

const SYMBOL: &str = "EURUSD";

fn ts(milliseconds: i64) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::milliseconds(milliseconds)
}

fn effective() -> EffectiveInterval {
    EffectiveInterval::new(
        "2020-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
        None,
    )
    .unwrap()
}

fn symbol_spec(category: &str, lot_base_units: i64) -> SymbolSpec {
    SymbolSpec {
        canonical: SYMBOL.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: category.into(),
        lot_base_units,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn currencies() -> SymbolCurrencyMetadata {
    SymbolCurrencyMetadata {
        base_currency: Some("EUR".into()),
        quote_currency: Some("USD".into()),
        pnl_currency: "USD".into(),
    }
}

fn instrument_id() -> InstrumentId {
    "compat/fx_cfd/EURUSD".parse().unwrap()
}

fn explicit_manifest(storage_scale: u8) -> ReplayInstrumentManifest {
    let legacy = symbol_spec("forex", 100_000);
    let economics = resolve_legacy_economics(&legacy).unwrap();
    let mut spec = guarded_instrument_spec(
        &legacy,
        &currencies(),
        economics,
        instrument_id(),
        effective(),
    )
    .unwrap();
    spec.quantity.storage_scale = storage_scale;
    let resolved = ResolvedInstrumentRef {
        instrument: spec.instrument.clone(),
        catalog: CatalogSnapshotId {
            version: "test-catalog".into(),
        },
        spec_revision: spec.revision.clone(),
    };
    ReplayInstrumentManifest {
        instruments: BTreeMap::from([(
            SYMBOL.into(),
            ReplayInstrumentArtifact {
                resolved: resolved.clone(),
                spec,
            },
        )]),
        stored_series: vec![StoredSeriesBinding {
            data_source: MarketDataSourceId::new("test-feed").unwrap(),
            source_partition: "demo".into(),
            source_symbol: SYMBOL.into(),
            instrument: resolved,
            effective: effective(),
        }],
    }
}

fn signals() -> Vec<RawSignal> {
    vec![
        RawSignal::Entry {
            ts: ts(0),
            symbol: SYMBOL.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: Some(1.0990),
            targets: Vec::new(),
            group: None,
            trade_id: Some("instrument-parity".into()),
        },
        RawSignal::Close {
            ts: ts(1_000),
            position: PositionRef::ByTradeId {
                trade_id: "instrument-parity".into(),
            },
        },
    ]
}

fn feed() -> VecFeed {
    VecFeed::new(vec![
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(0),
            bid: 1.1000,
            ask: 1.1000,
        },
        MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(1_000),
            bid: 1.1010,
            ask: 1.1010,
        },
    ])
}

fn run_explicit(storage_scale: u8) -> qs_backtest::BacktestResult {
    let config = BacktestConfig {
        close_on_finish: false,
        contract_sizes: HashMap::from([(SYMBOL.into(), 7.0)]),
        sizing: Some(SizingPolicy::FixedLot { lots: 1.25 }),
        instrument_manifest: Some(explicit_manifest(storage_scale)),
        ..BacktestConfig::default()
    };
    BacktestRunner::new_future(config, FutureQuoteConfig::default()).run_raw_signals_future(
        &mut feed(),
        signals(),
        None,
    )
}

#[test]
fn guarded_translation_uses_neutral_economics_and_standard_lots() {
    let legacy = symbol_spec("forex", 100_000);
    let guarded = resolve_legacy_economics(&legacy).unwrap();
    let translated = guarded_instrument_spec(
        &legacy,
        &currencies(),
        guarded,
        instrument_id(),
        effective(),
    )
    .unwrap();

    assert_eq!(
        translated.economics.pnl_model.as_str(),
        EconomicsModelId::FX_QUOTE_LINEAR_V1
    );
    assert_eq!(
        translated.economics.quantity_unit,
        QuantityUnit::StandardLot
    );
    assert_eq!(
        translated.economics.contract_multiplier.to_string(),
        "100000"
    );
    assert_eq!(translated.quantity.grid.step.to_string(), "0.01");
    assert!(resolve_legacy_economics(&symbol_spec("crypto", 100_000_000)).is_err());
}

#[test]
fn explicit_instrument_preserves_legacy_sizing_and_pnl() {
    let explicit = run_explicit(2);
    let compatibility_config = BacktestConfig {
        close_on_finish: false,
        contract_sizes: HashMap::from([(SYMBOL.into(), 100_000.0)]),
        sizing: Some(SizingPolicy::FixedLot { lots: 1.25 }),
        symbol_specs: HashMap::from([(SYMBOL.into(), symbol_spec("forex", 100_000))]),
        ..BacktestConfig::default()
    };
    let compatibility =
        BacktestRunner::new_future(compatibility_config, FutureQuoteConfig::default())
            .run_raw_signals_future(&mut feed(), signals(), None);

    assert_eq!(explicit.total_trades, 1);
    assert!((explicit.total_pnl - 125.0).abs() < 1.0e-9);
    assert_eq!(explicit.total_pnl, compatibility.total_pnl);
    assert_eq!(
        explicit.recorded_fills[0].size,
        compatibility.recorded_fills[0].size
    );
    let metadata = explicit.execution_metadata.as_ref().unwrap();
    assert_eq!(metadata.contract_sizes.get(SYMBOL), Some(&100_000.0));
    assert!(metadata.instrument_manifest.is_some());
    assert!(!metadata.tags.contains_key("economics.guard"));
    let sizing = &metadata.instrument_sizing[0];
    assert_eq!(sizing.symbol, SYMBOL);
    assert_eq!(sizing.quantity.requested.to_string(), "1.25");
    assert_eq!(sizing.quantity.adjusted.to_string(), "1.25");
    assert_eq!(sizing.final_notional, None);
}

#[test]
fn quantity_storage_scale_does_not_change_sizing_or_pnl() {
    let low_scale = run_explicit(2);
    let high_scale = run_explicit(18);

    assert_eq!(
        low_scale.recorded_fills[0].size,
        high_scale.recorded_fills[0].size
    );
    assert_eq!(low_scale.total_pnl, high_scale.total_pnl);
    assert_eq!(
        low_scale.completed_positions,
        high_scale.completed_positions
    );
}

#[test]
fn unsupported_explicit_spec_is_rejected_before_feed_consumption() {
    let mut manifest = explicit_manifest(2);
    let artifact = manifest.instruments.get_mut(SYMBOL).unwrap();
    artifact.spec.economics.pnl_model =
        EconomicsModelId::new(EconomicsModelId::LINEAR_BASE_QUANTITY_V1).unwrap();
    let config = BacktestConfig {
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        instrument_manifest: Some(manifest),
        ..BacktestConfig::default()
    };
    let mut source = feed();

    let result = BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .run_raw_signals_future(&mut source, signals(), None);

    assert_eq!(source.remaining(), 2);
    assert_eq!(result.total_trades, 0);
    let reason = result.action_dispositions[0].reason.as_deref().unwrap();
    assert!(reason.contains("unsupported P&L model"));
}

#[test]
fn stored_series_interval_must_match_the_embedded_spec() {
    let mut manifest = explicit_manifest(2);
    manifest.stored_series[0].effective = EffectiveInterval::new(
        "2021-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
        None,
    )
    .unwrap();
    let config = BacktestConfig {
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        instrument_manifest: Some(manifest),
        ..BacktestConfig::default()
    };
    let mut source = feed();

    let result = BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .run_raw_signals_future(&mut source, signals(), None);

    assert_eq!(source.remaining(), 2);
    assert_eq!(result.total_trades, 0);
    let reason = result.action_dispositions[0].reason.as_deref().unwrap();
    assert!(reason.contains("effective interval differs"));
}

#[test]
fn older_artifact_json_defaults_the_instrument_manifest() {
    let artifacts: FutureBacktestArtifacts = serde_json::from_str(
        r#"{"format_version":1,"execution":{"initial_balance":10000.0,"contract_sizes":{"EURUSD":100000.0}}}"#,
    )
    .unwrap();

    assert_eq!(artifacts.execution.instrument_manifest, None);
    assert!(artifacts.execution.instrument_sizing.is_empty());
    assert_eq!(
        artifacts.execution.contract_sizes.get(SYMBOL),
        Some(&100_000.0)
    );
}
