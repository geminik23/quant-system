//! Portfolio runs through the backtest service must match the same portfolio replayed in process, and must be validated completely before admission.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use backtest_server::artifact_store::ArtifactStore;
use backtest_server::convert::{config_from_msg, future_config_from_msg};
use backtest_server::handlers::{
    ServerState, handle_get_backtest_result, handle_run_portfolio, handle_submit_portfolio,
    run_job_and_store,
};
use backtest_server::rpc_types::*;
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime};
use data_preprocess::{ParquetStore, Tick};
use qs_backtest::profile::ProfileRegistry;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
    ConfiguredHistoricalBindings, ConfiguredInstance, ObservationStoreLimits, PriceBasis,
    RunCurrencyPlan, SeriesGeometry, StrategyDescriptor, StrategyId, StrategyRetentionLimits,
    Timeframe, VecFeed,
};
use qs_research::{DeclaredSpace, StrategyFamily};
use qs_risk::{PortfolioSupervisor, RiskPolicy};
use qs_strategy::{ConfiguredStrategy, MaterialLibrary, SourceId, StrategyConfig};
use qs_symbols::SymbolRegistry;

const EXCHANGE: &str = "fixture";
const MINUTES: i64 = 960;
const SYMBOLS: [(&str, &str, f64); 2] = [("EURUSD", "eurusd", 1.10), ("GBPUSD", "gbpusd", 1.30)];

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

/// One tick per minute per symbol, on a fixed-seed cycle shifted per symbol so the instances trade at different times.
fn ticks() -> Vec<Tick> {
    let mut ticks = Vec::new();
    for (index, (symbol, _, level)) in SYMBOLS.iter().enumerate() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64 + index as u64;
        for minute in 0..MINUTES {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let noise = ((state >> 33) % 21) as i64 - 10;
            let phase = ((minute + 60 * index as i64) % 240) as f64 / 240.0 * std::f64::consts::TAU;
            let drift = (phase.sin() * 120.0) as i64;
            let price = level + (drift + noise) as f64 * 1.0e-5;
            ticks.push(Tick {
                exchange: EXCHANGE.into(),
                symbol: (*symbol).into(),
                ts: at(minute),
                bid: Some(price),
                ask: Some(price + 2.0e-5),
                last: None,
                volume: None,
                flags: None,
            });
        }
    }
    ticks
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

[[symbol]]
canonical = "gbpusd"
pip_position = 4
digits = 5
category = "forex"
base_currency = "GBP"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000
lot_step_units = 1000
"#,
    )
    .unwrap()
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
        "qs_backtest_server_portfolio_{}_{unique}",
        std::process::id()
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    store.insert_ticks(&ticks()).unwrap();
    let symbol_registry = registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    Fixture {
        state: Arc::new(ServerState {
            strategies: Default::default(),
            symbol_registry,
            instrument_domain,
            profile_registry: RwLock::new(ProfileRegistry::empty()),
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

fn config_msg(sizing: SizingPolicyMsg) -> BacktestConfigMsg {
    BacktestConfigMsg {
        initial_balance: Some(10_000.0),
        close_on_finish: Some(true),
        fill_model: None,
        sizing: Some(sizing),
        costs: BTreeMap::new(),
    }
}

fn future_msg() -> FutureQuoteConfigMsg {
    FutureQuoteConfigMsg {
        account_currency: "USD".into(),
        ..FutureQuoteConfigMsg::default()
    }
}

/// The first bound point of the public EMA template.
fn bound_document() -> StrategyConfig {
    let space = DeclaredSpace::from_toml(
        include_str!("../../research/examples/ema_strategy.toml"),
        r#"
family_id = "ema_cross"

[parameters.ema_fast]
values = [3]

[parameters.ema_slow]
values = [8]

[parameters.atr_stop]
values = [1.5]

[parameters.entry]
values = ["cross"]

[[series]]
source = "primary"
symbol = { plan_symbol = true }
timeframe_seconds = 60
price_basis = "mid"
alignment_offset_seconds = 0
"#,
    )
    .unwrap();
    space.config(&0)
}

fn instance_msg(symbol: &str, instance_id: Option<&str>) -> PortfolioInstanceMsg {
    PortfolioInstanceMsg {
        symbol: symbol.into(),
        strategy: ConfiguredStrategyRunMsg {
            document: serde_json::to_value(bound_document()).unwrap(),
            sources: vec![SourceBindingMsg {
                source: "primary".into(),
                timeframe_seconds: 60,
                price_basis: PriceBasisMsg::Mid,
                alignment_offset_seconds: 0,
            }],
            instance_id: instance_id.map(Into::into),
            decision_latency_ms: 0,
        },
        profile: None,
        profile_def: None,
        entry_profile_routes: vec![],
    }
}

fn portfolio_request(
    instances: Vec<PortfolioInstanceMsg>,
    policies: Option<serde_json::Value>,
    sizing: SizingPolicyMsg,
) -> RunPortfolioRequest {
    RunPortfolioRequest {
        request: PortfolioRunSpec {
            instances,
            exchange: EXCHANGE.into(),
            data_type: "tick".into(),
            timeframe: None,
            from: Some(text(at(200))),
            to: Some(text(at(900))),
            config: config_msg(sizing),
            policies,
            groups: None,
        },
        future: future_msg(),
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Inline,
    }
}

fn supervised_request() -> RunPortfolioRequest {
    portfolio_request(
        vec![
            instance_msg("EURUSD", Some("eur")),
            instance_msg("GBPUSD", Some("gbp")),
        ],
        Some(serde_json::json!([{ "type": "max_open_positions", "limit": 1 }])),
        SizingPolicyMsg::FixedLot { lots: 0.1 },
    )
}

fn in_process_adapter(symbol: &str, instance: &str) -> BacktestConfiguredStrategyAdapter {
    let document = bound_document();
    let strategy = ConfiguredStrategy::compile(
        document.clone(),
        &MaterialLibrary::builtins(),
        instance,
        symbol,
    )
    .unwrap();
    let geometry = vec![SeriesGeometry::new(
        SourceId::new("primary").unwrap(),
        symbol,
        Timeframe::seconds(60).unwrap(),
        PriceBasis::Mid,
        0,
    )];
    let bindings =
        ConfiguredHistoricalBindings::from_geometry(geometry, strategy.input_requirements())
            .unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(
            StrategyId::new(document.strategy_id.clone()).unwrap(),
            instance,
            document.title.clone(),
        )
        .unwrap(),
        bindings,
        0,
    )
    .unwrap()
}

/// The same supervised portfolio replayed in process over the same stored ticks.
fn in_process_portfolio(fixture: &Fixture) -> qs_backtest::PortfolioBacktestResult {
    let adapters = vec![
        in_process_adapter("eurusd", "eur"),
        in_process_adapter("gbpusd", "gbp"),
    ];
    let warmup_start = ConfiguredHistoricalBindings::from_geometry(
        vec![SeriesGeometry::new(
            SourceId::new("primary").unwrap(),
            "eurusd",
            Timeframe::seconds(60).unwrap(),
            PriceBasis::Mid,
            0,
        )],
        adapters[0].configured_requirements(),
    )
    .unwrap()
    .warmup_start(at(200))
    .unwrap();
    let mut events = Vec::new();
    for (_, symbol, _) in SYMBOLS {
        events.extend(
            qs_research::load_symbol_ticks(
                fixture.state.data_dir.as_str(),
                EXCHANGE,
                symbol,
                Some(warmup_start),
                Some(at(900)),
            )
            .unwrap()
            .iter()
            .cloned(),
        );
    }
    let symbols = SYMBOLS
        .iter()
        .map(|(_, symbol, _)| (*symbol).to_owned())
        .collect::<Vec<_>>();
    let registry = registry();
    let config = config_from_msg(
        &config_msg(SizingPolicyMsg::FixedLot { lots: 0.1 }),
        &registry,
        &symbols,
    )
    .unwrap();
    let plan = RunCurrencyPlan::new(
        "USD",
        symbols.iter().cloned().collect(),
        Default::default(),
        symbols
            .iter()
            .map(|symbol| (symbol.clone(), "USD".to_owned()))
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
    .unwrap();
    let future = future_config_from_msg(&future_msg(), plan).unwrap();
    let instances = adapters
        .into_iter()
        .map(|adapter| {
            ConfiguredInstance::new(
                adapter,
                AnalysisPipeline::new(
                    Vec::new(),
                    ObservationStoreLimits::default(),
                    AnnotationLimits::default(),
                )
                .unwrap(),
            )
        })
        .collect();
    BacktestRunner::new_future(config, future)
        .run_portfolio_future(
            &mut VecFeed::from_feed_events(events),
            instances,
            Some(
                PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 1 }], vec![])
                    .unwrap(),
            ),
            StrategyRetentionLimits::default(),
        )
        .unwrap()
}

#[test]
fn a_service_portfolio_matches_the_in_process_portfolio() {
    let fixture = fixture();
    let response = handle_run_portfolio(&fixture.state, &supervised_request());
    assert!(response.success, "{:?}", response.error);
    let result = response.result.unwrap();
    let expected = in_process_portfolio(&fixture);

    assert!(expected.replay.total_trades > 0, "the fixture must trade");
    assert_eq!(result.total_trades, expected.replay.total_trades);
    assert_eq!(result.total_positions, expected.replay.total_positions);
    assert!((result.total_pnl - expected.replay.total_pnl).abs() < 1e-9);
    assert!((result.final_balance - expected.replay.final_balance).abs() < 1e-9);

    let portfolio = result
        .portfolio
        .expect("a portfolio run returns its instance output");
    assert_eq!(portfolio.instances.len(), 2);
    assert_eq!(portfolio.instances[0].instance_id, "eur");
    assert_eq!(portfolio.instances[1].symbol, "gbpusd");
    for (output, expected) in portfolio.instances.iter().zip(&expected.instances) {
        assert_eq!(
            output.strategy.decisions,
            serde_json::to_value(&expected.decisions).unwrap()
        );
    }
    let supervisor = portfolio.supervisor.expect("the run was supervised");
    let expected_supervisor = serde_json::to_value(expected.supervisor.as_ref().unwrap()).unwrap();
    assert_eq!(supervisor, expected_supervisor);
    assert!(
        supervisor["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["verdict"]["verdict"] == "reject"),
        "the one-position limit must reject something to be meaningful"
    );
    assert_eq!(
        portfolio.policies.unwrap()["policies"][0]["type"],
        "max_open_positions"
    );
}

#[test]
fn a_retained_portfolio_job_returns_the_same_result_as_the_synchronous_run() {
    let fixture = fixture();
    let synchronous = handle_run_portfolio(&fixture.state, &supervised_request())
        .result
        .unwrap();
    let submitted = handle_submit_portfolio(
        &fixture.state,
        &SubmitPortfolioRequest {
            request: supervised_request(),
        },
    );
    let job_id = submitted.job_id.expect("a valid portfolio is admitted");
    run_job_and_store(fixture.state.clone(), job_id.clone());
    let retained = handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(retained.success, "{:?}", retained.error);
    let retained = retained.result.expect("an inline result");
    assert!((retained.total_pnl - synchronous.total_pnl).abs() < 1e-12);
    assert_eq!(
        serde_json::to_value(&retained.portfolio).unwrap(),
        serde_json::to_value(&synchronous.portfolio).unwrap()
    );
}

#[test]
fn invalid_portfolios_are_rejected_before_admission_with_their_location() {
    let fixture = fixture();
    let reject = |request: RunPortfolioRequest, expected: &str| {
        let response = handle_submit_portfolio(&fixture.state, &SubmitPortfolioRequest { request });
        let error = response.error.expect("the portfolio must be rejected");
        assert!(response.job_id.is_none());
        assert!(error.contains(expected), "{expected:?} not in {error:?}");
    };
    reject(
        portfolio_request(vec![], None, SizingPolicyMsg::FixedLot { lots: 0.1 }),
        "at least one instance",
    );
    reject(
        portfolio_request(
            vec![
                instance_msg("EURUSD", Some("eur")),
                instance_msg("GBPUSD", None),
            ],
            None,
            SizingPolicyMsg::FixedLot { lots: 0.1 },
        ),
        "instances[1]: a portfolio instance requires strategy.instance_id",
    );
    reject(
        portfolio_request(
            vec![
                instance_msg("EURUSD", Some("same")),
                instance_msg("GBPUSD", Some("same")),
            ],
            None,
            SizingPolicyMsg::FixedLot { lots: 0.1 },
        ),
        "appears more than once",
    );
    reject(
        portfolio_request(
            vec![instance_msg("EURUSD", Some("eur"))],
            Some(serde_json::json!([{ "type": "max_open_positions", "limit": 1, "extra": 1 }])),
            SizingPolicyMsg::FixedLot { lots: 0.1 },
        ),
        "invalid policies",
    );
    reject(
        portfolio_request(
            vec![instance_msg("EURUSD", Some("eur"))],
            Some(
                serde_json::json!([{ "type": "group_risk_cap", "group": "usd", "max_group_risk": 100.0 }]),
            ),
            SizingPolicyMsg::FixedLot { lots: 0.1 },
        ),
        "undeclared group",
    );
    let mut capped = portfolio_request(
        vec![instance_msg("EURUSD", Some("eur"))],
        Some(
            serde_json::json!([{ "type": "group_risk_cap", "group": "usd", "max_group_risk": 100.0 }]),
        ),
        SizingPolicyMsg::FixedLot { lots: 0.1 },
    );
    capped.request.groups = Some(serde_json::json!([{ "id": "usd", "symbols": ["EUR/USD"] }]));
    reject(capped, "monetary sizing policy");
    let mut unrelated = portfolio_request(
        vec![instance_msg("EURUSD", Some("eur"))],
        Some(
            serde_json::json!([{ "type": "group_risk_cap", "group": "jpy", "max_group_risk": 100.0 }]),
        ),
        SizingPolicyMsg::FixedRiskAmount { amount: 50.0 },
    );
    unrelated.request.groups = Some(serde_json::json!([{ "id": "jpy", "symbols": ["USDJPY"] }]));
    reject(unrelated, "names none of the portfolio's symbols");
    let limit = fixture.state.strategies.limits.max_portfolio_instances;
    reject(
        portfolio_request(
            (0..=limit)
                .map(|index| instance_msg("EURUSD", Some(&format!("i{index}"))))
                .collect(),
            None,
            SizingPolicyMsg::FixedLot { lots: 0.1 },
        ),
        "above the server limit",
    );
    assert!(fixture.state.jobs.lock().unwrap().is_empty());
}

#[test]
fn group_symbols_are_normalized_like_instance_symbols() {
    let fixture = fixture();
    let mut request = portfolio_request(
        vec![
            instance_msg("EURUSD", Some("eur")),
            instance_msg("GBPUSD", Some("gbp")),
        ],
        Some(
            serde_json::json!([{ "type": "group_risk_cap", "group": "usd", "max_group_risk": 60.0 }]),
        ),
        SizingPolicyMsg::FixedRiskAmount { amount: 50.0 },
    );
    request.request.groups =
        Some(serde_json::json!([{ "id": "usd", "symbols": ["EUR/USD", "gbpusd"] }]));
    let response = handle_run_portfolio(&fixture.state, &request);
    assert!(response.success, "{:?}", response.error);
    let portfolio = response.result.unwrap().portfolio.unwrap();
    let events = portfolio.supervisor.unwrap()["events"]
        .as_array()
        .unwrap()
        .clone();
    assert!(
        events
            .iter()
            .any(|event| event["verdict"]["policy"] == "group_risk_cap"),
        "a cap written with aliases must still bind both symbols"
    );
    assert_eq!(
        portfolio.policies.unwrap()["groups"][0]["symbols"],
        serde_json::json!(["eurusd", "gbpusd"])
    );
}
