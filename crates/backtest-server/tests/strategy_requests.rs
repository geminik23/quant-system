//! Configured strategy runs and parameter searches through the backtest service must match the same work done in process.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use backtest_server::artifact_store::ArtifactStore;
use backtest_server::convert::{config_from_msg, future_config_from_msg};
use backtest_server::handlers::{
    JobStatus, ServerState, handle_cancel_backtest, handle_get_backtest_result,
    handle_get_backtest_status, handle_get_search_result, handle_run_configured_strategy,
    handle_submit_configured_strategy, handle_submit_search, run_job_and_store,
};
use backtest_server::rpc_types::*;
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime};
use data_preprocess::{ParquetStore, Tick};
use qs_backtest::profile::{ManagementProfile, ProfileRegistry, RuleConfigDef, StoplossMode};
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
    ConfiguredHistoricalBindings, ObservationStoreLimits, PriceBasis, RunCurrencyPlan,
    SeriesGeometry, StrategyDescriptor, StrategyId, StrategyRetentionLimits, Timeframe, VecFeed,
};
use qs_research::{DataWindow, DeclaredSpace, ResearchPlan, StrategyFamily, WindowPlan, run_batch};
use qs_strategy::{ConfiguredStrategy, MaterialLibrary, SourceId, StrategyConfig};
use qs_symbols::SymbolRegistry;

const EXCHANGE: &str = "fixture";
const SYMBOL: &str = "eurusd";
const MINUTES: i64 = 960;

fn strategy_template() -> &'static str {
    include_str!("../../research/examples/ema_strategy.toml")
}

/// A small space over the public EMA template: two fast periods, one slow period, one stop multiple, every entry condition.
fn space_toml() -> &'static str {
    r#"
family_id = "ema_cross"

[parameters.ema_fast]
values = [3, 4]

[parameters.ema_slow]
values = [8]

[parameters.atr_stop]
values = [1.5]

[parameters.entry]
all = true

[[series]]
source = "primary"
symbol = { plan_symbol = true }
timeframe_seconds = 60
price_basis = "mid"
alignment_offset_seconds = 0
"#
}

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 5)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn at(minutes: i64) -> NaiveDateTime {
    base() + ChronoDuration::minutes(minutes)
}

fn text(timestamp: NaiveDateTime) -> String {
    timestamp.format("%Y-%m-%dT%H:%M:%S").to_string()
}

/// The same fixed-seed cycle the research tests use, one tick per minute.
fn ticks() -> Vec<Tick> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..MINUTES)
        .map(|minute| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let noise = ((state >> 33) % 21) as i64 - 10;
            let phase = (minute % 240) as f64 / 240.0 * std::f64::consts::TAU;
            let drift = (phase.sin() * 120.0) as i64;
            let price = 1.10000 + (drift + noise) as f64 * 1.0e-5;
            Tick {
                exchange: EXCHANGE.into(),
                symbol: "EURUSD".into(),
                ts: at(minute),
                bid: Some(price),
                ask: Some(price + 2.0e-5),
                last: None,
                volume: None,
                flags: None,
            }
        })
        .collect()
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

fn trailing_profile() -> ManagementProfile {
    ManagementProfile {
        name: "trail".into(),
        target_selection: None,
        use_targets: vec![],
        close_ratios: vec![],
        target_source: qs_backtest::TargetSource::FromSignal,
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![RuleConfigDef::TrailingStop { distance: 0.0005 }],
        group_override: None,
        let_remainder_run: false,
        entry_geometry: qs_backtest::EntryGeometryPolicy::Strict,
    }
}

struct Fixture {
    state: Arc<ServerState>,
    data_dir: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn fixture() -> Fixture {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "qs_backtest_server_strategy_{}_{unique}",
        std::process::id()
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    store.insert_ticks(&ticks()).unwrap();
    let symbol_registry = registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    let mut profiles = ProfileRegistry::empty();
    profiles.insert(trailing_profile(), false).unwrap();
    Fixture {
        state: Arc::new(ServerState {
            strategies: Default::default(),
            symbol_registry,
            instrument_domain,
            profile_registry: RwLock::new(profiles),
            data_dir: data_dir.to_string_lossy().into_owned(),
            profiles_path: String::new(),
            start_time: Instant::now(),
            jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
            max_retained_jobs: 1_000,
            artifact_store: ArtifactStore::new(
                data_dir.join("artifacts"),
                12 * 1024 * 1024,
                1024 * 1024,
                Duration::from_secs(3_600),
                1024 * 1024 * 1024,
            )
            .unwrap(),
        }),
        data_dir,
    }
}

fn config_msg() -> BacktestConfigMsg {
    BacktestConfigMsg {
        initial_balance: Some(10_000.0),
        close_on_finish: Some(true),
        fill_model: None,
        sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.1 }),
        costs: BTreeMap::new(),
    }
}

fn future_msg() -> FutureQuoteConfigMsg {
    FutureQuoteConfigMsg {
        account_currency: "USD".into(),
        ..FutureQuoteConfigMsg::default()
    }
}

/// The first bound point of the public template, the document a client would send for one run.
fn bound_document() -> StrategyConfig {
    let space = DeclaredSpace::from_toml(strategy_template(), space_toml()).unwrap();
    space.config(&0)
}

fn primary_source() -> SourceBindingMsg {
    SourceBindingMsg {
        source: "primary".into(),
        timeframe_seconds: 60,
        price_basis: PriceBasisMsg::Mid,
        alignment_offset_seconds: 0,
    }
}

fn run_request(document: serde_json::Value) -> RunConfiguredStrategyRequest {
    RunConfiguredStrategyRequest {
        request: ConfiguredStrategyRunSpec {
            symbol: "EURUSD".into(),
            exchange: EXCHANGE.into(),
            data_type: "tick".into(),
            timeframe: None,
            from: Some(text(at(200))),
            to: Some(text(at(900))),
            strategy: ConfiguredStrategyRunMsg {
                document,
                sources: vec![primary_source()],
                instance_id: None,
                decision_latency_ms: 0,
            },
            profile: None,
            profile_def: None,
            entry_profile_routes: vec![],
            config: config_msg(),
        },
        future: future_msg(),
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Inline,
    }
}

fn account_plan() -> RunCurrencyPlan {
    RunCurrencyPlan::new(
        "USD",
        [SYMBOL.to_owned()].into_iter().collect(),
        Default::default(),
        [(SYMBOL.to_owned(), "USD".to_owned())]
            .into_iter()
            .collect(),
        [(
            "USD".to_owned(),
            qs_backtest::ConversionRoute::Identity {
                currency: "USD".into(),
            },
        )]
        .into_iter()
        .collect(),
        Vec::new(),
    )
    .unwrap()
}

/// The same run done in process over the same stored ticks.
fn in_process_run(fixture: &Fixture, document: StrategyConfig) -> qs_backtest::BacktestResult {
    let strategy = ConfiguredStrategy::compile(
        document.clone(),
        &MaterialLibrary::builtins(),
        "instance",
        SYMBOL,
    )
    .unwrap();
    let geometry = vec![SeriesGeometry::new(
        SourceId::new("primary").unwrap(),
        SYMBOL,
        Timeframe::seconds(60).unwrap(),
        PriceBasis::Mid,
        0,
    )];
    let bindings =
        ConfiguredHistoricalBindings::from_geometry(geometry, strategy.input_requirements())
            .unwrap();
    let warmup_start = bindings.warmup_start(at(200)).unwrap();
    let mut adapter = BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(
            StrategyId::new(document.strategy_id.clone()).unwrap(),
            "instance",
            document.title.clone(),
        )
        .unwrap(),
        bindings,
        0,
    )
    .unwrap();
    let events = qs_research::load_symbol_ticks(
        fixture.state.data_dir.as_str(),
        EXCHANGE,
        SYMBOL,
        Some(warmup_start),
        Some(at(900)),
    )
    .unwrap();
    let registry = registry();
    let config = config_from_msg(&config_msg(), &registry, &[SYMBOL.to_owned()]).unwrap();
    let future = future_config_from_msg(&future_msg(), account_plan()).unwrap();
    BacktestRunner::new_future(config, future)
        .run_configured_strategy_future(
            &mut VecFeed::from_feed_events(events.to_vec()),
            &mut adapter,
            AnalysisPipeline::new(
                Vec::new(),
                ObservationStoreLimits::default(),
                AnnotationLimits::default(),
            )
            .unwrap(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap()
        .replay
}

fn document_value(document: &StrategyConfig) -> serde_json::Value {
    serde_json::to_value(document).unwrap()
}

#[test]
fn service_run_matches_the_in_process_run_and_returns_the_strategy_output() {
    let fixture = fixture();
    let document = bound_document();
    let response =
        handle_run_configured_strategy(&fixture.state, &run_request(document_value(&document)));
    assert!(response.success, "{:?}", response.error);
    let result = response.result.unwrap();

    let expected = in_process_run(&fixture, document.clone());
    assert!(
        expected.total_trades > 0,
        "the fixture must trade to be meaningful"
    );
    assert_eq!(result.total_trades, expected.total_trades);
    assert_eq!(result.total_positions, expected.total_positions);
    assert!((result.total_pnl - expected.total_pnl).abs() < 1e-9);
    assert!((result.final_balance - expected.final_balance).abs() < 1e-9);

    let output = result.strategy.unwrap();
    assert_eq!(output.document, document_value(&document));
    assert_eq!(output.data_mode, "ticks");
    assert_eq!(
        output.requirements["completed_bars"][0]["source"],
        "primary"
    );
    assert!(
        output.requirements["completed_bars"][0]["required_lookback"]
            .as_u64()
            .unwrap()
            > 1
    );
    assert_eq!(output.requirements["entries"][0]["slot"], "primary");
    assert!(!output.decisions["records"].as_array().unwrap().is_empty());
}

#[test]
fn a_retained_job_returns_the_same_result_as_the_synchronous_run() {
    let fixture = fixture();
    let request = run_request(document_value(&bound_document()));
    let synchronous = handle_run_configured_strategy(&fixture.state, &request)
        .result
        .unwrap();

    let submitted = handle_submit_configured_strategy(
        &fixture.state,
        &SubmitConfiguredStrategyRequest { request },
    );
    let job_id = submitted.job_id.expect("the request is valid");
    run_job_and_store(fixture.state.clone(), job_id.clone());
    let retained = handle_get_backtest_result(
        &fixture.state,
        &GetBacktestResultRequest {
            job_id: job_id.clone(),
        },
    );
    assert!(retained.success, "{:?}", retained.error);
    let retained = retained.result.unwrap();
    assert_eq!(retained.total_pnl, synchronous.total_pnl);
    assert_eq!(retained.total_trades, synchronous.total_trades);
    assert_eq!(
        serde_json::to_value(&retained.strategy).unwrap(),
        serde_json::to_value(&synchronous.strategy).unwrap()
    );
    assert!(
        handle_get_search_result(&fixture.state, &GetSearchResultRequest { job_id })
            .error
            .unwrap()
            .contains("not a parameter search")
    );
}

#[test]
fn invalid_documents_and_bindings_are_rejected_before_admission_with_their_location() {
    let fixture = fixture();
    let rejected = |request: RunConfiguredStrategyRequest| {
        let response = handle_submit_configured_strategy(
            &fixture.state,
            &SubmitConfiguredStrategyRequest { request },
        );
        assert!(response.job_id.is_none());
        response.error.unwrap()
    };

    let mut unknown_field = document_value(&bound_document());
    unknown_field["unexpected"] = serde_json::json!(true);
    assert!(rejected(run_request(unknown_field)).contains("unexpected"));

    let mut unknown_material = document_value(&bound_document());
    unknown_material["materials"][0]["key"] = serde_json::json!("not_a_material");
    assert!(rejected(run_request(unknown_material)).contains("materials[0].key"));

    let template =
        serde_json::to_value(toml::from_str::<StrategyConfig>(strategy_template()).unwrap())
            .unwrap();
    assert!(rejected(run_request(template)).contains("does not compile"));

    let mut undeclared = run_request(document_value(&bound_document()));
    undeclared.request.strategy.sources[0].source = "other".into();
    assert!(rejected(undeclared).contains("source binding"));

    let mut without_sizing = run_request(document_value(&bound_document()));
    without_sizing.request.config.sizing = None;
    assert!(rejected(without_sizing).contains("config.sizing"));

    let mut classified = document_value(&bound_document());
    for state in classified["states"].as_array_mut().unwrap() {
        for transition in state["transitions"].as_array_mut().unwrap() {
            for action in transition["actions"].as_array_mut().unwrap() {
                if action["action"] == "entry" {
                    action["entry_class"] = serde_json::json!("trend");
                }
            }
        }
    }
    assert!(rejected(run_request(classified.clone())).contains("entry class `trend`"));
    let mut routed = run_request(classified);
    routed.request.entry_profile_routes = vec![EntryProfileRouteMsg {
        entry_class: "trend".into(),
        profile: ProfileRef::Named("trail".into()),
    }];
    let routed = handle_run_configured_strategy(&fixture.state, &routed);
    assert!(routed.success, "{:?}", routed.error);
}

#[test]
fn a_stored_bar_request_runs_over_bars_of_the_declared_timeframe() {
    let fixture = fixture();
    let bars = {
        use data_preprocess::models::Timeframe as StoredTimeframe;
        use data_preprocess::resample::{
            BarAggregator, BucketSpec, PriceBasis as StoredPriceBasis,
        };
        let mut aggregator = BarAggregator::new(
            EXCHANGE,
            "EURUSD",
            StoredTimeframe::M1,
            BucketSpec::new(60, 0).unwrap(),
            StoredPriceBasis::Mid,
            1.0e-5,
        );
        ticks()
            .into_iter()
            .filter_map(|tick| aggregator.push(tick.ts, tick.bid, tick.ask))
            .collect::<Vec<_>>()
    };
    ParquetStore::open(&fixture.data_dir)
        .unwrap()
        .insert_bars(&bars)
        .unwrap();

    let mut request = run_request(document_value(&bound_document()));
    request.request.data_type = "bar".into();
    request.request.timeframe = Some("1m".into());
    let response = handle_run_configured_strategy(&fixture.state, &request);
    assert!(response.success, "{:?}", response.error);
    assert_eq!(response.result.unwrap().strategy.unwrap().data_mode, "bars");

    let mut mismatched = request.clone();
    mismatched.request.timeframe = Some("5m".into());
    let response = handle_run_configured_strategy(&fixture.state, &mismatched);
    assert!(response.error.unwrap().contains("declares 60s bars"));
}

#[test]
fn a_job_cancelled_before_its_worker_starts_ends_cancelled() {
    let fixture = fixture();
    let submitted = handle_submit_configured_strategy(
        &fixture.state,
        &SubmitConfiguredStrategyRequest {
            request: run_request(document_value(&bound_document())),
        },
    );
    let job_id = submitted.job_id.unwrap();
    assert!(
        handle_cancel_backtest(
            &fixture.state,
            &CancelBacktestRequest {
                job_id: job_id.clone()
            }
        )
        .success
    );
    run_job_and_store(fixture.state.clone(), job_id.clone());
    let status = handle_get_backtest_status(&fixture.state, &GetBacktestStatusRequest { job_id });
    assert_eq!(status.status, JobStatus::Cancelled.as_str());
}

fn search_request() -> SubmitSearchRequest {
    let template =
        serde_json::to_value(toml::from_str::<toml::Value>(strategy_template()).unwrap()).unwrap();
    let space = serde_json::to_value(toml::from_str::<toml::Value>(space_toml()).unwrap()).unwrap();
    SubmitSearchRequest {
        request: SearchRunSpec {
            template,
            space,
            symbols: vec!["EURUSD".into()],
            exchange: EXCHANGE.into(),
            data_type: "tick".into(),
            timeframe: None,
            windows: SearchWindowsMsg::Fixed {
                in_sample: SearchWindowMsg {
                    label: "is".into(),
                    from: text(at(200)),
                    to: text(at(600)),
                },
                out_of_sample: SearchWindowMsg {
                    label: "oos".into(),
                    from: text(at(600)),
                    to: text(at(900)),
                },
            },
            config: config_msg(),
            profile: None,
            entry_profile_routes: vec![],
            workers: Some(2),
            decision_latency_ms: 0,
        },
        future: future_msg(),
        evaluation: ProviderEvaluationOptionsMsg::default(),
    }
}

fn read_artifact(state: &ServerState, reference: &ResultArtifactRefMsg) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let chunk = backtest_server::handlers::handle_get_result_artifact_chunk(
            state,
            &GetResultArtifactChunkRequest {
                artifact_id: reference.artifact_id.clone(),
                offset: bytes.len() as u64,
            },
        );
        assert!(chunk.success, "{:?}", chunk.error);
        use base64::Engine as _;
        bytes.extend(
            base64::engine::general_purpose::STANDARD
                .decode(chunk.data_base64)
                .unwrap(),
        );
        if chunk.eof {
            return bytes;
        }
    }
}

#[test]
fn a_server_search_matches_the_in_process_batch() {
    let fixture = fixture();
    let submitted = handle_submit_search(&fixture.state, &search_request());
    let job_id = submitted
        .job_id
        .unwrap_or_else(|| panic!("{:?}", submitted.error));
    run_job_and_store(fixture.state.clone(), job_id.clone());

    let response = handle_get_search_result(
        &fixture.state,
        &GetSearchResultRequest {
            job_id: job_id.clone(),
        },
    );
    assert!(response.success, "{:?}", response.error);
    let summary = response.summary.unwrap();
    assert_eq!(summary.points_total, 6);
    assert_eq!(summary.rows, 12);
    assert_eq!(summary.completed_rows, 12);
    assert_eq!(summary.data_mode, "ticks");
    let output: SearchResultMsg =
        serde_json::from_slice(&read_artifact(&fixture.state, &response.artifact.unwrap()))
            .unwrap();
    assert_eq!(output.bound_documents.len(), 6);

    let family = DeclaredSpace::from_toml(strategy_template(), space_toml()).unwrap();
    let registry = registry();
    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::Fixed {
            in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
            out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
        },
        config_from_msg(&config_msg(), &registry, &[SYMBOL.to_owned()]).unwrap(),
    )
    .with_future(future_config_from_msg(&future_msg(), account_plan()).unwrap())
    .with_workers(1);
    let (start, end) = qs_research::batch_data_range(&plan, &family)
        .unwrap()
        .unwrap();
    let events = qs_research::load_symbol_ticks(
        fixture.state.data_dir.as_str(),
        EXCHANGE,
        SYMBOL,
        Some(start),
        Some(end),
    )
    .unwrap();
    let expected = run_batch(
        &plan,
        &family,
        &BTreeMap::from([(SYMBOL.to_owned(), events)]),
    )
    .unwrap();
    assert_eq!(output.table_csv, expected.table().to_csv());

    let backtest_result =
        handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(backtest_result.error.unwrap().contains("get_search_result"));
}

#[test]
fn searches_are_validated_completely_before_any_data_is_loaded() {
    let fixture = fixture();
    let rejected = |request: SubmitSearchRequest| {
        let response = handle_submit_search(&fixture.state, &request);
        assert!(response.job_id.is_none());
        response.error.unwrap()
    };

    let mut unknown_profile = search_request();
    unknown_profile.request.profile = Some("missing".into());
    assert!(rejected(unknown_profile).contains("missing"));

    let mut inline_route = search_request();
    inline_route.request.entry_profile_routes = vec![EntryProfileRouteMsg {
        entry_class: "trend".into(),
        profile: ProfileRef::Inline(backtest_server::convert::profile_to_msg(&trailing_profile())),
    }];
    assert!(rejected(inline_route).contains("registered profiles only"));

    let mut other_currency = search_request();
    other_currency.future.account_currency = "EUR".into();
    assert!(rejected(other_currency).contains("settles in USD"));

    let mut bad_space = search_request();
    bad_space.request.space["parameters"]["unknown"] = serde_json::json!({ "values": [1] });
    let message = rejected(bad_space);
    assert!(message.contains("unknown"), "{message}");

    let mut reserved = search_request();
    reserved.request.space["parameters"]["window"] = serde_json::json!({ "values": [1] });
    assert!(!rejected(reserved).is_empty());
}
