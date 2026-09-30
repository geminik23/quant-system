//! The loader's stream coordinates must agree with the instrument manifest the service pinned for the run.
//!
//! This lives with the service because it exercises the boundary between two owners: `qs-market-loader` reports where it will read from, and the service's instrument catalog decides what the run is allowed to read. Neither crate can assert the agreement alone.

use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use data_preprocess::{ParquetStore, Tick};
use qs_market_loader::describe_primary_market_stream;
use qs_symbols::SymbolRegistry;

fn ts(second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 2, 3)
        .unwrap()
        .and_hms_opt(10, 0, second)
        .unwrap()
}

fn temp_data_dir() -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("qs-stored-series-{unique}"));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn tick(symbol: &str, second: u32) -> Tick {
    Tick {
        exchange: "fixture".into(),
        symbol: symbol.into(),
        ts: ts(second),
        bid: Some(1.1000),
        ask: Some(1.1002),
        last: None,
        volume: None,
        flags: None,
    }
}

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
"#,
    )
    .unwrap()
}

#[test]
fn stored_series_bindings_must_match_planned_stream_coordinates() {
    let data_dir = temp_data_dir();
    let store = ParquetStore::open(&data_dir).unwrap();
    store.insert_ticks(&[tick("EURUSD", 0)]).unwrap();
    let mut never_cancelled = || false;
    let description = describe_primary_market_stream(
        data_dir.to_str().unwrap(),
        "fixture",
        &["eurusd".into()],
        "tick",
        None,
        Some(ts(0)),
        Some(ts(0)),
        &mut never_cancelled,
        &mut |_| {},
    )
    .unwrap();

    let registry = registry();
    let domain = backtest_server::InstrumentDomain::compatibility(&registry).unwrap();
    let mut manifest = domain
        .resolve_manifest(&["eurusd".into()], ts(0), Some(ts(0)))
        .unwrap();
    domain
        .attach_stored_series(&mut manifest, description.stored_series_coordinates())
        .unwrap();

    description
        .validate_stored_series_bindings(&manifest)
        .unwrap();

    manifest.stored_series[0].source_partition = "other".into();
    let error = description
        .validate_stored_series_bindings(&manifest)
        .unwrap_err();
    assert!(error.to_string().contains("do not match"));

    // The description is still usable after a rejected manifest, so a caller may retry with a corrected one.
    assert!(description.open(Arc::new(|| false)).is_ok());

    std::fs::remove_dir_all(data_dir).unwrap();
}
