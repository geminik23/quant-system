use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use backtest_server::artifact_store::ArtifactStore;
use backtest_server::config::InstrumentsSection;
use backtest_server::handlers::{
    handle_run_backtest, handle_run_backtest_multi, handle_submit_backtest, run_job_and_store,
};
use backtest_server::{InstrumentDomain, ServerState};
use chrono::{NaiveDate, NaiveDateTime};
use data_preprocess::{ParquetStore, Tick};
use qs_backtest::profile::ProfileRegistry;
use qs_backtest_api::*;
use qs_symbols::SymbolRegistry;

#[test]
fn scale_in_quantities_obey_explicit_rules_without_resizing_final_intent() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    insert_quote(&fixture, "BTCUSD", ts(5), 62000.0);
    let mut req = request(vec![entry("BTCUSD", "bitcoin", "Buy")]);
    req.request.to = Some(ts(6).to_string());
    req.request.raw_signals.pop();
    req.request.raw_signals.push(RawSignalMsg::ScaleIn {
        ts: ts(2).to_string(),
        position: PositionRefMsg::ByTradeId {
            trade_id: "bitcoin".into(),
        },
        price: None,
        size: 0.05,
    });
    req.request.raw_signals.push(RawSignalMsg::CloseAll {
        ts: ts(4).to_string(),
    });
    let result = run(&fixture, &req);
    assert!((result.positions[0].original_size - 0.15).abs() < 1e-12);
    assert!((result.total_pnl - 250.0).abs() < 1e-8);
    if let RawSignalMsg::ScaleIn { size, .. } = &mut req.request.raw_signals[1] {
        *size = 15.005;
    }
    let result = run(&fixture, &req);
    assert!((result.positions[0].original_size - 0.1).abs() < 1e-12);
    assert!(
        result
            .future
            .unwrap()
            .action_dispositions
            .to_string()
            .contains("invalid_instrument_quantity")
    );
}

#[test]
fn effective_catalog_start_is_reconsidered_after_earlier_unknown_entry_is_excluded() {
    use qs_instruments::{AssetKind, AssetSpec, CatalogDocument, EffectiveInterval};
    let mut fixture = Fixture::new(&[], rules());
    insert_quote(&fixture, "USTEC", ts(11), 100.0);
    insert_quote(&fixture, "USTEC", ts(13), 110.0);
    let mut spec = fixture
        .state
        .instrument_domain
        .resolve_manifest(&["us100".into()], ts(0), Some(ts(14)))
        .unwrap()
        .instruments
        .remove("us100")
        .unwrap()
        .spec;
    spec.effective = EffectiveInterval::new(ts(5).and_utc(), None).unwrap();
    let document = CatalogDocument {
        schema_version: 1,
        version: "effective-simulation".into(),
        assets: vec![AssetSpec {
            asset: "USD".parse().unwrap(),
            kind: AssetKind::Fiat,
            display_code: "USD".into(),
            storage_scale: None,
        }],
        instruments: vec![spec],
    };
    let path = fixture.root.join("effective-catalog.toml");
    std::fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
    let domain = InstrumentDomain::load(
        &InstrumentsSection {
            catalog_path: Some(path.to_string_lossy().into_owned()),
            ..Default::default()
        },
        &fixture.state.symbol_registry,
    )
    .unwrap();
    Arc::get_mut(&mut fixture.state).unwrap().instrument_domain = domain;
    let mut req = request(vec![
        entry("UNKNOWN", "early", "Buy"),
        entry("US100", "later", "Buy"),
    ]);
    if let RawSignalMsg::Entry { ts: timestamp, .. } = &mut req.request.raw_signals[1] {
        *timestamp = ts(10).to_string();
    }
    if let RawSignalMsg::CloseAll { ts: timestamp } = req.request.raw_signals.last_mut().unwrap() {
        *timestamp = ts(12).to_string();
    }
    req.request.to = Some(ts(14).to_string());
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 1.0 });
    let result = run(&fixture, &req);
    assert_eq!(result.positions[0].symbol, "us100");
    assert!((result.total_pnl - 10.0).abs() < 1e-9);
}

fn install_catalog(fixture: &mut Fixture, specs: Vec<qs_instruments::InstrumentSpec>) {
    use qs_instruments::{AssetKind, AssetSpec, CatalogDocument};
    let document = CatalogDocument {
        schema_version: 1,
        version: "dated-fixture".into(),
        assets: ["USD", "JPY"]
            .into_iter()
            .map(|name| AssetSpec {
                asset: name.parse().unwrap(),
                kind: AssetKind::Fiat,
                display_code: name.into(),
                storage_scale: None,
            })
            .collect(),
        instruments: specs,
    };
    let path = fixture.root.join("dated-catalog.toml");
    std::fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
    let domain = InstrumentDomain::load(
        &InstrumentsSection {
            catalog_path: Some(path.to_string_lossy().into_owned()),
            ..Default::default()
        },
        &fixture.state.symbol_registry,
    )
    .unwrap();
    Arc::get_mut(&mut fixture.state).unwrap().instrument_domain = domain;
}

fn dated_conversion_fixture(global_start: bool) -> (Fixture, RunBacktestRequest) {
    let mut fixture = Fixture::new(&[("USDJPY", 150.0, 151.0)], rules());
    insert_quote(&fixture, "USTEC", ts(11), 100.0);
    insert_quote(&fixture, "USTEC", ts(13), 110.0);
    let manifest = fixture
        .state
        .instrument_domain
        .resolve_manifest(&["us100".into(), "usdjpy".into()], ts(0), Some(ts(14)))
        .unwrap();
    let specs = manifest
        .instruments
        .into_iter()
        .map(|(symbol, mut artifact)| {
            if symbol == "us100" {
                artifact.spec.effective =
                    qs_instruments::EffectiveInterval::new(ts(5).and_utc(), None).unwrap();
            }
            artifact.spec
        })
        .collect();
    install_catalog(&mut fixture, specs);
    let mut later = entry("US100", "later", "Buy");
    if let RawSignalMsg::Entry { ts: timestamp, .. } = &mut later {
        *timestamp = ts(10).to_string();
    }
    let mut req = request(vec![entry("USDJPY", "early", "Buy"), later]);
    req.request.to = Some(ts(14).to_string());
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 1.0 });
    if let RawSignalMsg::CloseAll { ts: timestamp } = req.request.raw_signals.last_mut().unwrap() {
        *timestamp = ts(12).to_string();
    }
    if global_start {
        req.request.raw_signals.insert(
            0,
            RawSignalMsg::CloseAll {
                ts: ts(0).to_string(),
            },
        );
    }
    (fixture, req)
}

#[test]
fn effective_start_retries_after_conversion_exclusion() {
    let (fixture, req) = dated_conversion_fixture(false);
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 1);
    assert_eq!(result.positions[0].symbol, "us100");
    assert!((result.total_pnl - 10.0).abs() < 1e-9);
    let report = result.admission_report.as_ref().unwrap();
    assert_eq!(report.excluded_instruments.len(), 1);
    assert_eq!(report.excluded_instruments[0].symbol, "usdjpy");
    assert_eq!(
        report.excluded_instruments[0].reason,
        InstrumentExclusionReasonMsg::NoConversionData
    );
    let submitted = handle_submit_backtest(
        &fixture.state,
        &SubmitBacktestRequest {
            request: req.clone(),
        },
    );
    assert!(submitted.success, "{:?}", submitted.error);
    let id = submitted.job_id.unwrap();
    run_job_and_store(fixture.state.clone(), id.clone());
    let jobs = fixture.state.jobs.lock().unwrap();
    let stored = jobs[&id].result.as_ref().unwrap();
    assert_eq!(stored.admission_report, result.admission_report);
    assert_eq!(stored.total_pnl, result.total_pnl);
}

#[test]
fn retained_global_signal_keeps_effective_start_early() {
    let (fixture, req) = dated_conversion_fixture(true);
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 0);
    let report = result.admission_report.unwrap();
    assert_eq!(report.excluded_instruments.len(), 2);
    assert_eq!(report.retained_signals, 2);
}

fn open_ended_fixture(rollover: bool) -> (Fixture, RunBacktestRequest) {
    let mut fixture = Fixture::new(
        &[("US100", 100.0, 110.0), ("XAUUSD", 2000.0, 2001.0)],
        rules(),
    );
    let manifest = fixture
        .state
        .instrument_domain
        .resolve_manifest(&["us100".into(), "xauusd".into()], ts(0), Some(ts(4)))
        .unwrap();
    let mut specs = Vec::new();
    for (symbol, artifact) in manifest.instruments {
        let mut spec = artifact.spec;
        if symbol == "us100" {
            spec.effective = qs_instruments::EffectiveInterval::new(
                spec.effective.valid_from,
                Some(ts(2).and_utc()),
            )
            .unwrap();
            if rollover {
                let mut next = spec.clone();
                next.revision = "2.0.0".parse().unwrap();
                next.effective =
                    qs_instruments::EffectiveInterval::new(ts(2).and_utc(), None).unwrap();
                specs.push(next);
            }
        }
        specs.push(spec);
    }
    let document = qs_instruments::CatalogDocument {
        schema_version: 1,
        version: "ending-fixture".into(),
        assets: ["USD", "XAU"]
            .into_iter()
            .map(|name| qs_instruments::AssetSpec {
                asset: name.parse().unwrap(),
                kind: if name == "USD" {
                    qs_instruments::AssetKind::Fiat
                } else {
                    qs_instruments::AssetKind::Commodity
                },
                display_code: name.into(),
                storage_scale: None,
            })
            .collect(),
        instruments: specs,
    };

    let path = fixture.root.join("ending-catalog.toml");
    std::fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
    let domain = InstrumentDomain::load(
        &InstrumentsSection {
            catalog_path: Some(path.to_string_lossy().into_owned()),
            ..Default::default()
        },
        &fixture.state.symbol_registry,
    )
    .unwrap();
    Arc::get_mut(&mut fixture.state).unwrap().instrument_domain = domain;

    let mut req = request(vec![
        entry("US100", "ending", "Buy"),
        entry("XAUUSD", "valid", "Buy"),
    ]);
    req.request.to = None;
    req.request.raw_signals.insert(
        2,
        RawSignalMsg::ModifyStoploss {
            ts: ts(1).to_string(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "ending".into(),
            },
            price: 95.0,
        },
    );
    (fixture, req)
}

#[test]
fn open_ended_expiry_uses_skip_and_strict_report() {
    let (fixture, mut req) = open_ended_fixture(false);
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 1);
    assert_eq!(result.positions[0].symbol, "xauusd");
    assert!((result.total_pnl - 10.0).abs() < 1e-9);
    let report = result.admission_report.unwrap();
    assert_eq!(report.excluded_instruments.len(), 1);
    assert_eq!(report.excluded_instruments[0].symbol, "us100");
    assert_eq!(report.excluded_instruments[0].skipped_management, 1);
    let single = req.request.clone();
    let profile: ManagementProfileMsg = serde_json::from_value(
        serde_json::json!({"name":"neutral","use_targets":[],"close_ratios":[]}),
    )
    .unwrap();
    let response = handle_run_backtest_multi(
        &fixture.state,
        &RunBacktestMultiRequest {
            request: BacktestMultiRunSpec {
                on_unavailable: single.on_unavailable,
                symbol: single.symbol,
                symbols: single.symbols,
                all_symbols: single.all_symbols,
                exchange: single.exchange,
                data_type: single.data_type,
                timeframe: single.timeframe,
                from: single.from,
                to: single.to,
                raw_signals: single.raw_signals,
                profiles: vec![
                    ProfileRef::Inline(profile.clone()),
                    ProfileRef::Inline(profile),
                ],
                entry_profile_routes: single.entry_profile_routes,
                config: single.config,
            },
            future: req.future.clone(),
            evaluation: req.evaluation.clone(),
            result_delivery: ResultDeliveryMsg::Inline,
        },
    );
    assert!(response.success, "{:?}", response.error);
    for result in response.results {
        assert_eq!(
            result.result.unwrap().admission_report.as_ref(),
            Some(&report)
        );
    }
    req.request.on_unavailable = UnavailableInstrumentPolicyMsg::Error;
    let submitted = handle_submit_backtest(&fixture.state, &SubmitBacktestRequest { request: req });
    assert!(!submitted.success);
    assert!(submitted.job_id.is_none());
    assert!(submitted.error.unwrap().contains("excluded_instruments"));
    assert!(fixture.state.jobs.lock().unwrap().is_empty());
}

#[test]
fn open_ended_all_expired_terminates_with_report() {
    let (fixture, mut req) = open_ended_fixture(false);
    req.request.raw_signals.retain(
        |signal| !matches!(signal, RawSignalMsg::Entry { symbol, .. } if symbol == "XAUUSD"),
    );
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 0);
    assert_eq!(result.admission_report.unwrap().retained_signals, 1);
}

#[test]
fn open_ended_revision_change_is_reported_without_switching_specs() {
    let (fixture, req) = open_ended_fixture(true);
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 1);
    let report = result.admission_report.unwrap();
    assert_eq!(
        report.excluded_instruments[0].reason,
        InstrumentExclusionReasonMsg::UnsupportedEconomics
    );
}

fn insert_quote(fixture: &Fixture, symbol: &str, at: NaiveDateTime, price: f64) {
    ParquetStore::open(&fixture.root)
        .unwrap()
        .insert_ticks(&[Tick {
            exchange: "fixture".into(),
            symbol: symbol.into(),
            ts: at,
            bid: Some(price),
            ask: Some(price),
            last: None,
            volume: None,
            flags: None,
        }])
        .unwrap();
}

#[test]
fn later_conversion_warmup_is_reconsidered_after_earlier_instrument_is_excluded() {
    let fixture = Fixture::new(&[], rules());
    insert_quote(&fixture, "USDJPY", ts(1), 150.0);
    insert_quote(&fixture, "USDJPY", ts(3), 151.0);
    insert_quote(&fixture, "EURCHF", ts(11), 1.0);
    insert_quote(&fixture, "EURCHF", ts(13), 1.1);
    insert_quote(&fixture, "USDCHF", ts(5), 0.9);
    let mut req = request(vec![
        entry("USDJPY", "early", "Buy"),
        entry("EURCHF", "later", "Buy"),
    ]);
    if let RawSignalMsg::Entry { ts: timestamp, .. } = &mut req.request.raw_signals[1] {
        *timestamp = ts(10).to_string();
    }
    if let RawSignalMsg::CloseAll { ts: timestamp } = req.request.raw_signals.last_mut().unwrap() {
        *timestamp = ts(12).to_string();
    }
    req.request.to = Some(ts(14).to_string());
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 1);
    assert_eq!(result.positions[0].symbol, "eurchf");
    let report = result.admission_report.unwrap();
    assert_eq!(report.excluded_instruments.len(), 1);
    assert_eq!(report.excluded_instruments[0].symbol, "usdjpy");
}

#[test]
fn missing_conversion_catalog_spec_excludes_only_dependent_instruments() {
    use qs_instruments::{AssetKind, AssetSpec, CatalogDocument};
    let mut fixture = Fixture::new(
        &[("BTCUSD", 60000.0, 61000.0), ("GBPJPY", 150.0, 151.0)],
        rules(),
    );
    insert_quote(
        &fixture,
        "USDJPY",
        ts(0) - chrono::Duration::seconds(1),
        150.0,
    );
    let manifest = fixture
        .state
        .instrument_domain
        .resolve_manifest(&["btcusd".into(), "gbpjpy".into()], ts(0), Some(ts(4)))
        .unwrap();
    let assets = ["BTC", "GBP", "JPY", "USD"]
        .into_iter()
        .map(|name| AssetSpec {
            asset: name.parse().unwrap(),
            kind: if name == "BTC" {
                AssetKind::Crypto
            } else {
                AssetKind::Fiat
            },
            display_code: name.into(),
            storage_scale: None,
        })
        .collect();
    let document = CatalogDocument {
        schema_version: 1,
        version: "explicit-simulation".into(),
        assets,
        instruments: manifest
            .instruments
            .into_values()
            .map(|artifact| artifact.spec)
            .collect(),
    };
    let path = fixture.root.join("catalog.toml");
    std::fs::write(&path, toml::to_string(&document).unwrap()).unwrap();
    let config = InstrumentsSection {
        catalog_path: Some(path.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let domain = InstrumentDomain::load(&config, &fixture.state.symbol_registry).unwrap();
    Arc::get_mut(&mut fixture.state).unwrap().instrument_domain = domain;
    let result = run(
        &fixture,
        &request(vec![
            entry("BTCUSD", "bitcoin", "Buy"),
            entry("GBPJPY", "yen", "Buy"),
        ]),
    );
    assert!((result.total_pnl - 100.0).abs() < 1e-9);
    let report = result.admission_report.unwrap();
    assert_eq!(report.excluded_instruments[0].symbol, "gbpjpy");
    assert_eq!(
        report.excluded_instruments[0].reason,
        InstrumentExclusionReasonMsg::NoConversionData
    );
}

#[test]
fn identity_conversion_does_not_validate_unrelated_source_bindings() {
    let mut config = rules();
    config.source_symbols.insert("US100".into(), "USTEC".into());
    let fixture = Fixture::new(
        &[("BTCUSD", 60000.0, 61000.0), ("US100", 100.0, 110.0)],
        config,
    );
    assert!(
        (run(&fixture, &request(vec![entry("BTCUSD", "bitcoin", "Buy")])).total_pnl - 100.0).abs()
            < 1e-9
    );
}

#[test]
fn report_limits_keep_exact_omission_counts_and_bounded_strict_errors() {
    let fixture = Fixture::new(&[], rules());
    let entries = (0..100)
        .map(|index| {
            entry(
                &format!("UNKNOWN{index}"),
                &format!("{}-{index}", "x".repeat(300)),
                "Buy",
            )
        })
        .collect();
    let req = request(entries);
    let result = run(&fixture, &req);
    let report = result.admission_report.unwrap();
    assert_eq!(report.excluded_instruments.len(), 64);
    assert_eq!(report.omitted_instruments, 36);
    assert_eq!(report.omitted_entries, 36);
    assert_eq!(report.retained_signals, 1);
    assert_eq!(
        result.future.as_ref().unwrap().execution_metadata["tags"]["data.idle_symbols"],
        ""
    );
    assert!(
        report
            .excluded_instruments
            .iter()
            .flat_map(|item| &item.signal_references)
            .all(|reference| reference.trade_id.is_none())
    );
    let mut strict = req;
    strict.request.on_unavailable = UnavailableInstrumentPolicyMsg::Error;
    let response = handle_run_backtest(&fixture.state, &strict);
    assert!(!response.success);
    assert!(response.error.unwrap().len() < 256_000);
}

#[test]
fn minimum_and_maximum_lot_rules_are_enforced_by_existing_sizing() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let mut req = request(vec![entry("BTCUSD", "bitcoin", "Buy")]);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 20.0 });
    let result = run(&fixture, &req);
    assert_eq!(result.positions[0].original_size, 10.0);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 0.001 });
    assert_eq!(run(&fixture, &req).total_positions, 0);
}

#[test]
fn corrupt_market_data_remains_fatal_in_skip_mode() {
    let fixture = Fixture::new(&[], rules());
    let directory = fixture.root.join("ticks/exchange=fixture/symbol=BTCUSD");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("2026-01-15.parquet"), b"not parquet").unwrap();
    assert!(
        !handle_run_backtest(
            &fixture.state,
            &request(vec![entry("BTCUSD", "bitcoin", "Buy")])
        )
        .success
    );
}

fn ts(second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 15)
        .unwrap()
        .and_hms_opt(10, 0, second)
        .unwrap()
}

fn rules() -> InstrumentsSection {
    toml::from_str(
        r#"
[[linear_instruments]]
symbol = "BTCUSD"
contract_multiplier = "1"
price_step = "0.01"
display_scale = 2
quantity_step = "0.01"
minimum = "0.01"
maximum = "10"
[[linear_instruments]]
symbol = "ETHUSD"
contract_multiplier = "1"
price_step = "0.01"
display_scale = 2
quantity_step = "0.01"
minimum = "0.01"
maximum = "10"
"#,
    )
    .unwrap()
}

struct Fixture {
    state: Arc<ServerState>,
    root: PathBuf,
}

impl Fixture {
    fn new(quotes: &[(&str, f64, f64)], config: InstrumentsSection) -> Self {
        let root = std::env::temp_dir().join(format!(
            "qs-instrument-admission-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = ParquetStore::open(&root).unwrap();
        for (symbol, first, last) in quotes {
            store
                .insert_ticks(
                    &[(1, first), (3, last)]
                        .into_iter()
                        .map(|(second, price)| Tick {
                            exchange: "fixture".into(),
                            symbol: (*symbol).into(),
                            ts: ts(second),
                            bid: Some(*price),
                            ask: Some(*price),
                            last: None,
                            volume: None,
                            flags: None,
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
        }
        let registry = SymbolRegistry::load(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../symbols/symbols.toml"),
        )
        .unwrap();
        let domain = InstrumentDomain::load(&config, &registry).unwrap();
        let state = Arc::new(ServerState {
            symbol_registry: registry,
            instrument_domain: domain,
            profile_registry: RwLock::new(ProfileRegistry::empty()),
            data_dir: root.to_string_lossy().into_owned(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: Mutex::new(Default::default()),
            max_retained_jobs: 100,
            artifact_store: ArtifactStore::new(
                root.join("artifacts"),
                12 * 1024 * 1024,
                1024 * 1024,
                Duration::from_secs(3600),
                1024 * 1024 * 1024,
            )
            .unwrap(),
            strategies: Default::default(),
        });
        Self { state, root }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn entry(symbol: &str, id: &str, side: &str) -> RawSignalMsg {
    RawSignalMsg::Entry {
        ts: ts(0).to_string(),
        symbol: symbol.into(),
        side: side.into(),
        order_type: "Market".into(),
        price: None,
        risk: 1.0,
        stoploss: None,
        targets: Vec::new(),
        group: Some("shared".into()),
        trade_id: Some(id.into()),
        entry_class: None,
    }
}

fn request(entries: Vec<RawSignalMsg>) -> RunBacktestRequest {
    let mut signals = entries;
    signals.push(RawSignalMsg::CloseAll {
        ts: ts(2).to_string(),
    });
    RunBacktestRequest {
        request: BacktestRunSpec {
            on_unavailable: Default::default(),
            symbol: String::new(),
            symbols: Vec::new(),
            all_symbols: true,
            exchange: "fixture".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: Some(ts(0).to_string()),
            to: Some(ts(4).to_string()),
            raw_signals: signals,
            profile: None,
            profile_def: None,
            entry_profile_routes: Vec::new(),
            config: BacktestConfigMsg {
                initial_balance: Some(10000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.1 }),
                costs: BTreeMap::new(),
            },
        },
        future: FutureQuoteConfigMsg {
            signal_latency_ms: 0,
            account_currency: "USD".into(),
            ..Default::default()
        },
        evaluation: Default::default(),
        result_delivery: ResultDeliveryMsg::Inline,
    }
}

fn run(fixture: &Fixture, request: &RunBacktestRequest) -> BacktestResultMsg {
    let response = handle_run_backtest(&fixture.state, request);
    assert!(response.success, "{:?}", response.error);
    response.result.unwrap()
}

#[test]
fn configured_bitcoin_and_ether_execute_linear_long_and_short_pnl() {
    let fixture = Fixture::new(
        &[("BTCUSD", 60000.0, 61000.0), ("ETHUSD", 2000.0, 2100.0)],
        rules(),
    );
    let result = run(
        &fixture,
        &request(vec![
            entry("BTCUSD", "bitcoin", "Buy"),
            entry("ETHUSD", "ether", "Buy"),
        ]),
    );
    assert_eq!(result.total_positions, 2);
    assert!((result.total_pnl - 110.0).abs() < 1e-9);
    assert!(
        result
            .positions
            .iter()
            .all(|position| (position.original_size - 0.1).abs() < 1e-12)
    );
    let short = run(
        &fixture,
        &request(vec![
            entry("BTCUSD", "bitcoin", "Sell"),
            entry("ETHUSD", "ether", "Sell"),
        ]),
    );
    assert!((short.total_pnl + 110.0).abs() < 1e-9);
    assert!(
        result
            .admission_report
            .unwrap()
            .excluded_instruments
            .is_empty()
    );
}

#[test]
fn contract_multiplier_and_quantity_grid_are_configurable() {
    let mut config = rules();
    config.linear_instruments[0].contract_multiplier = "2".parse().unwrap();
    config.linear_instruments[0].quantity_step = "0.05".parse().unwrap();
    config.linear_instruments[0].minimum = "0.05".parse().unwrap();
    config.linear_instruments[0].maximum = "0.15".parse().unwrap();
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], config);
    let mut req = request(vec![entry("BTCUSD", "bitcoin", "Buy")]);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 0.12 });
    let result = run(&fixture, &req);
    assert!((result.positions[0].original_size - 0.1).abs() < 1e-12);
    assert!((result.total_pnl - 200.0).abs() < 1e-9);
}

#[test]
fn risk_sizing_uses_contract_units_not_atomic_storage_scale() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let mut signal = entry("BTCUSD", "bitcoin", "Buy");
    if let RawSignalMsg::Entry { stoploss, .. } = &mut signal {
        *stoploss = Some(59400.0);
    }
    let mut req = request(vec![signal]);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedRiskAmount { amount: 100.0 });
    let result = run(&fixture, &req);
    assert!((result.positions[0].original_size - 0.16).abs() < 1e-12);
    assert!((result.total_pnl - 160.0).abs() < 1e-9);
}

#[test]
fn missing_and_unsupported_instruments_are_reported_without_blocking_bitcoin() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let mut req = request(vec![
        entry("BTCUSD", "bitcoin", "Buy"),
        entry("USDX", "index", "Buy"),
        entry("DOTUSD", "unsupported", "Buy"),
    ]);
    req.request.raw_signals.insert(
        3,
        RawSignalMsg::ModifyStoploss {
            ts: ts(1).to_string(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "unsupported".into(),
            },
            price: 1.0,
        },
    );
    let result = run(&fixture, &req);
    assert!((result.total_pnl - 100.0).abs() < 1e-9);
    let report = result.admission_report.unwrap();
    assert_eq!(report.input_signals, 5);
    assert_eq!(report.retained_signals, 2);
    let dot = report
        .excluded_instruments
        .iter()
        .find(|item| item.symbol == "dotusd")
        .unwrap();
    assert_eq!(
        dot.reason,
        InstrumentExclusionReasonMsg::UnsupportedEconomics
    );
    assert_eq!(dot.skipped_management, 1);
    assert_eq!(dot.signal_references[1].input_index, 3);
    assert_eq!(
        report
            .excluded_instruments
            .iter()
            .find(|item| item.symbol == "usdx")
            .unwrap()
            .reason,
        InstrumentExclusionReasonMsg::NoMarketData
    );
}

#[test]
fn strict_admission_rejects_with_report_and_does_not_create_a_job() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let mut req = request(vec![
        entry("BTCUSD", "bitcoin", "Buy"),
        entry("USDX", "index", "Buy"),
    ]);
    req.request.on_unavailable = UnavailableInstrumentPolicyMsg::Error;
    let response = handle_run_backtest(&fixture.state, &req);
    assert!(!response.success);
    assert!(response.error.unwrap().contains("no_market_data"));
    let response = handle_submit_backtest(&fixture.state, &SubmitBacktestRequest { request: req });
    assert!(!response.success);
    assert!(response.job_id.is_none());
    assert!(fixture.state.jobs.lock().unwrap().is_empty());
}

#[test]
fn explicit_selection_filters_even_in_strict_mode_and_preserves_group_operations() {
    let fixture = Fixture::new(
        &[("BTCUSD", 60000.0, 61000.0), ("ETHUSD", 2000.0, 2100.0)],
        rules(),
    );
    let mut req = request(vec![
        entry("BTCUSD", "bitcoin", "Buy"),
        entry("ETHUSD", "ether", "Buy"),
    ]);
    req.request.all_symbols = false;
    req.request.symbols = vec!["BTCUSD".into()];
    req.request.on_unavailable = UnavailableInstrumentPolicyMsg::Error;
    req.request.raw_signals.pop();
    req.request.raw_signals.push(RawSignalMsg::CloseAllInGroup {
        ts: ts(2).to_string(),
        group_id: "shared".into(),
    });
    let result = run(&fixture, &req);
    assert!((result.total_pnl - 100.0).abs() < 1e-9);
    assert_eq!(
        result.admission_report.unwrap().excluded_instruments[0].reason,
        InstrumentExclusionReasonMsg::NotSelected
    );
}

#[test]
fn all_skipped_keeps_zero_trade_report_and_global_management_without_sizing() {
    let fixture = Fixture::new(&[], rules());
    let mut req = request(vec![entry("DOTUSD", "unsupported", "Buy")]);
    req.request.config.sizing = None;
    let result = run(&fixture, &req);
    assert_eq!(result.total_positions, 0);
    assert_eq!(result.admission_report.unwrap().retained_signals, 1);
    assert_eq!(
        result
            .future
            .unwrap()
            .action_dispositions
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn alias_mapping_preserves_canonical_and_physical_identity() {
    let fixture = Fixture::new(&[("USTEC", 100.0, 110.0)], rules());
    let mut req = request(vec![entry("US100", "index", "Buy")]);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 1.0 });
    let result = run(&fixture, &req);
    assert!((result.total_pnl - 10.0).abs() < 1e-9);
    assert_eq!(result.positions[0].symbol, "us100");
    let encoded = result.future.unwrap().execution_metadata.to_string();
    assert!(encoded.contains("USTEC"));
}

#[test]
fn duplicate_aliases_are_local_unavailability_and_explicit_binding_resolves_them() {
    let quotes = [
        ("US100", 100.0, 110.0),
        ("USTEC", 100.0, 120.0),
        ("BTCUSD", 60000.0, 61000.0),
    ];
    let fixture = Fixture::new(&quotes, rules());
    let req = request(vec![
        entry("US100", "index", "Buy"),
        entry("BTCUSD", "bitcoin", "Buy"),
    ]);
    let result = run(&fixture, &req);
    assert!((result.total_pnl - 100.0).abs() < 1e-9);
    assert_eq!(
        result.admission_report.unwrap().excluded_instruments[0].reason,
        InstrumentExclusionReasonMsg::AmbiguousMapping
    );
    let mut config = rules();
    config.source_symbols.insert("US100".into(), "USTEC".into());
    let fixture = Fixture::new(&quotes, config);
    let mut req = request(vec![entry("US100", "index", "Buy")]);
    req.request.config.sizing = Some(SizingPolicyMsg::FixedLot { lots: 1.0 });
    assert!((run(&fixture, &req).total_pnl - 20.0).abs() < 1e-9);
}

#[test]
fn async_worker_uses_prepared_decisions_and_preserves_report() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let req = request(vec![
        entry("BTCUSD", "bitcoin", "Buy"),
        entry("USDX", "index", "Buy"),
    ]);
    let expected = run(&fixture, &req);
    let admitted = handle_submit_backtest(&fixture.state, &SubmitBacktestRequest { request: req });
    assert!(admitted.success, "{:?}", admitted.error);
    let id = admitted.job_id.unwrap();
    run_job_and_store(fixture.state.clone(), id.clone());
    let jobs = fixture.state.jobs.lock().unwrap();
    let result = jobs[&id].result.as_ref().unwrap();
    assert_eq!(result.total_pnl, expected.total_pnl);
    assert_eq!(result.admission_report, expected.admission_report);
}

#[test]
fn comparison_profiles_share_admission_and_single_account_economics() {
    let fixture = Fixture::new(&[("BTCUSD", 60000.0, 61000.0)], rules());
    let req = request(vec![
        entry("BTCUSD", "bitcoin", "Buy"),
        entry("USDX", "index", "Buy"),
    ]);
    let profile: ManagementProfileMsg = serde_json::from_value(
        serde_json::json!({"name":"neutral","use_targets":[],"close_ratios":[]}),
    )
    .unwrap();
    let single = req.request;
    let multi = RunBacktestMultiRequest {
        request: BacktestMultiRunSpec {
            on_unavailable: single.on_unavailable,
            symbol: single.symbol,
            symbols: single.symbols,
            all_symbols: single.all_symbols,
            exchange: single.exchange,
            data_type: single.data_type,
            timeframe: single.timeframe,
            from: single.from,
            to: single.to,
            raw_signals: single.raw_signals,
            profiles: vec![
                ProfileRef::Inline(profile.clone()),
                ProfileRef::Inline(profile),
            ],
            entry_profile_routes: Vec::new(),
            config: single.config,
        },
        future: req.future,
        evaluation: req.evaluation,
        result_delivery: ResultDeliveryMsg::Inline,
    };
    let response = handle_run_backtest_multi(&fixture.state, &multi);
    assert!(response.success, "{:?}", response.error);
    assert_eq!(response.results.len(), 2);
    for result in response.results {
        let result = result.result.unwrap();
        assert!((result.total_pnl - 100.0).abs() < 1e-9);
        assert_eq!(
            result.admission_report.unwrap().excluded_instruments.len(),
            1
        );
    }
}
