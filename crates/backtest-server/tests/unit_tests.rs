//! Unit tests for the backtest server crate.

use backtest_server::artifact_store::{ArtifactStore, ArtifactStoreError};
use backtest_server::config::ServerConfig;
use backtest_server::convert::{
    config_from_msg, evaluation_options_from_msg, parse_fill_model, position_ref_from_msg,
    profile_from_msg, profile_to_msg, raw_signal_from_msg, result_to_msg,
};
use backtest_server::handlers::{
    BacktestJob, JobCancellationToken, JobStatus, ServerState, cleanup_expired_jobs,
    handle_add_profile, handle_cancel_backtest, handle_delete_result_artifact,
    handle_get_backtest_result, handle_get_backtest_status, handle_get_result_artifact_chunk,
    handle_list_profiles, handle_list_symbols, handle_ping, handle_remove_profile,
    handle_run_backtest, handle_run_backtest_multi, handle_submit_backtest, run_job_and_store,
};
use backtest_server::rpc_types::*;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::{NaiveDate, NaiveDateTime};
use data_preprocess::{Bar, ParquetStore, Tick, Timeframe};
use qs_backtest::evaluation::{BreakdownDimension, EvaluationSection, GroupFilter, PositionSide};
use qs_backtest::profile::{
    ManagementProfile, PositionRef, ProfileRegistry, RawSignal, RuleConfigDef, StoplossMode,
};
use qs_backtest::report::BacktestResult;
use qs_core::types::{FillModel, Side};
use qs_symbols::SymbolRegistry;

fn test_request(request: BacktestRunSpec) -> RunBacktestRequest {
    RunBacktestRequest {
        request,
        future: FutureQuoteConfigMsg {
            account_currency: "USD".into(),
            ..FutureQuoteConfigMsg::default()
        },
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    }
}

fn run_for_test(state: &ServerState, request: &BacktestRunSpec) -> RunBacktestResponse {
    handle_run_backtest(state, &test_request(request.clone()))
}

fn submit_for_test(state: &ServerState, request: &BacktestRunSpec) -> SubmitBacktestResponse {
    handle_submit_backtest(
        state,
        &SubmitBacktestRequest {
            request: test_request(request.clone()),
        },
    )
}

fn run_job_for_test(state: Arc<ServerState>, job_id: String, request: BacktestRunSpec) {
    run_job_and_store(state, job_id, test_request(request));
}

fn run_multi_for_test(
    state: &ServerState,
    request: &BacktestMultiRunSpec,
) -> RunBacktestMultiResponse {
    handle_run_backtest_multi(
        state,
        &RunBacktestMultiRequest {
            request: request.clone(),
            future: FutureQuoteConfigMsg {
                account_currency: "USD".into(),
                ..FutureQuoteConfigMsg::default()
            },
            evaluation: ProviderEvaluationOptionsMsg::default(),
            result_delivery: ResultDeliveryMsg::Auto,
        },
    )
}

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn evaluation_symbol_registry() -> SymbolRegistry {
    SymbolRegistry::from_toml(
        r#"
[[symbol]]
canonical = "eurusd"
aliases = ["eur/usd", "eur-usd"]
pip_position = 4
digits = 5
category = "forex"
base_currency = "EUR"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000
lot_step_units = 1000

[[symbol]]
canonical = "us100"
aliases = ["nasdaq", "nas-100"]
pip_position = 1
digits = 2
category = "index"
pnl_currency = "USD"
lot_base_units = 1
lot_step_units = 1

[[symbol]]
canonical = "xauusd"
aliases = ["xau/usd", "gold"]
pip_position = 1
digits = 2
category = "metal"
base_currency = "XAU"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100
lot_step_units = 1
"#,
    )
    .unwrap()
}

fn test_artifact_store(directory: PathBuf) -> ArtifactStore {
    ArtifactStore::new(
        directory,
        12 * 1024 * 1024,
        1024 * 1024,
        Duration::from_secs(3_600),
        1024 * 1024 * 1024,
    )
    .unwrap()
}

fn empty_state() -> ServerState {
    let symbol_registry = evaluation_symbol_registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    ServerState {
        symbol_registry,
        instrument_domain,
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: "/tmp/test-data".into(),
        profiles_path: String::new(),
        start_time: Instant::now(),
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        max_retained_jobs: 1_000,
        artifact_store: test_artifact_store(std::env::temp_dir().join(format!(
            "qs_backtest_server_unit_artifacts_{}",
            std::process::id()
        ))),
    }
}

fn sample_raw_signal() -> RawSignalMsg {
    RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        order_type: "Market".into(),
        price: None,
        risk: 1.0,
        stoploss: Some(1.0800),
        targets: vec![1.0900, 1.0950],
        group: None,
        trade_id: None,
    }
}

fn sample_run_request() -> BacktestRunSpec {
    BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: Some(10_000.0),
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    }
}

struct ReplayPathFixture {
    state: Arc<ServerState>,
    data_dir: PathBuf,
}

impl Drop for ReplayPathFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn fixture_ts(second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 15)
        .unwrap()
        .and_hms_opt(10, 0, second)
        .unwrap()
}

fn replay_path_fixture() -> ReplayPathFixture {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "qs_backtest_server_replay_path_parity_{}_{}",
        std::process::id(),
        unique
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    let ticks = [
        (0, 1.1000, 1.1002),
        (1, 1.1005, 1.1007),
        (2, 1.1017, 1.1019),
        (3, 1.1010, 1.1012),
    ]
    .into_iter()
    .map(|(second, bid, ask)| Tick {
        exchange: "fixture".into(),
        symbol: "EURUSD".into(),
        ts: fixture_ts(second),
        bid: Some(bid),
        ask: Some(ask),
        last: None,
        volume: Some(1.0),
        flags: None,
    })
    .collect::<Vec<_>>();
    assert_eq!(store.insert_ticks(&ticks).unwrap(), ticks.len());

    ReplayPathFixture {
        state: Arc::new({
            let symbol_registry = evaluation_symbol_registry();
            let instrument_domain =
                backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
            ServerState {
                symbol_registry,
                instrument_domain,
                profile_registry: RwLock::new(ProfileRegistry::empty()),
                data_dir: data_dir.to_string_lossy().into_owned(),
                profiles_path: String::new(),
                start_time: Instant::now(),
                jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
                max_retained_jobs: 1_000,
                artifact_store: test_artifact_store(data_dir.join("artifacts")),
            }
        }),
        data_dir,
    }
}

fn fixture_inline_profile() -> ManagementProfileMsg {
    ManagementProfileMsg {
        name: "future-parity".into(),
        target_selection: Some(TargetSelectionMsg::Selected(vec![1])),
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: Some(StoplossModeMsg::FromSignal),
        rules: Vec::new(),
        group_override: Some("parity-group".into()),
        let_remainder_run: false,
    }
}

fn replay_request() -> RunBacktestRequest {
    RunBacktestRequest {
        request: BacktestRunSpec {
            symbol: "EUR/USD".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "fixture".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: None,
            to: None,
            raw_signals: vec![
                RawSignalMsg::Entry {
                    ts: "2026-01-15T10:00:00".into(),
                    symbol: "EURUSD".into(),
                    side: "Buy".into(),
                    order_type: "Market".into(),
                    price: Some(1.1002),
                    risk: 1.0,
                    stoploss: Some(1.0990),
                    targets: vec![1.1015],
                    group: Some("signal-group".into()),
                    trade_id: Some("future-parity-trade".into()),
                },
                RawSignalMsg::Entry {
                    ts: "2026-01-15T10:00:00".into(),
                    symbol: "EUR/USD".into(),
                    side: "Sell".into(),
                    order_type: "Market".into(),
                    price: Some(1.1000),
                    risk: 1.0,
                    stoploss: Some(1.1020),
                    targets: vec![1.0990],
                    group: Some("signal-group".into()),
                    trade_id: Some("future-parity-filtered".into()),
                },
            ],
            profile: None,
            profile_def: Some(fixture_inline_profile()),
            config: BacktestConfigMsg {
                initial_balance: Some(25_000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
            },
        },
        future: FutureQuoteConfigMsg {
            signal_latency_ms: 500,
            slippage_pips: 0.25,
            stale_quote_after_ms: Some(1_500),
            pnl_epsilon: 1.0e-6,
            account_currency: "USD".into(),
            conversion_stale_after_ms: 300_000,
            mtm_output: MtmOutputPolicyMsg::default(),
        },
        evaluation: ProviderEvaluationOptionsMsg {
            context: EvaluationContextMsg {
                provider_id: Some("fixture-provider".into()),
                source_id: Some("fixture-source".into()),
            },
            source_coverage: Some(SourceCoverageCountsMsg {
                raw_messages: 3,
                parsed_messages: 1,
                skipped_messages: 1,
                failed_messages: 1,
                emitted_signals: 3,
                emitted_entry_signals: 2,
            }),
            sections: EvaluationSectionMsg::ALL.to_vec(),
            filter: PositionFilterMsg {
                symbols: vec!["EUR-USD".into()],
                sides: vec![EvaluationPositionSideMsg::Long],
                groups: vec![EvaluationGroupFilterMsg::Named("parity-group".into())],
                close_reasons: Vec::new(),
                tags: BTreeMap::new(),
            },
            breakdowns: vec![BreakdownDimensionMsg::Symbol, BreakdownDimensionMsg::Group],
            bootstrap: BootstrapConfigMsg {
                samples: 32,
                confidence_level: 0.9,
                seed: 42,
                minimum_sample_size: 1,
            },
            rolling_window: 2,
            minimum_breakdown_bucket_count: 1,
            maximum_breakdown_rows: Some(10),
            include_positions: true,
            maximum_position_rows: Some(10),
        },
        result_delivery: ResultDeliveryMsg::Auto,
    }
}

fn multi_request(single: &RunBacktestRequest) -> RunBacktestMultiRequest {
    let request = &single.request;
    RunBacktestMultiRequest {
        request: BacktestMultiRunSpec {
            symbol: request.symbol.clone(),
            symbols: request.symbols.clone(),
            all_symbols: request.all_symbols,
            exchange: request.exchange.clone(),
            data_type: request.data_type.clone(),
            timeframe: request.timeframe.clone(),
            from: request.from.clone(),
            to: request.to.clone(),
            raw_signals: request.raw_signals.clone(),
            profiles: vec![ProfileRef::Inline(
                request.profile_def.clone().expect("inline fixture profile"),
            )],
            config: request.config.clone(),
        },
        future: single.future.clone(),
        evaluation: single.evaluation.clone(),
        result_delivery: single.result_delivery,
    }
}

fn active_symbol_registry() -> SymbolRegistry {
    SymbolRegistry::from_toml(
        r#"
[[symbol]]
canonical = "xauusd"
aliases = ["xau/usd", "xau-usd"]
pip_position = 1
digits = 2
category = "commodity"
base_currency = "XAU"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100
lot_step_units = 1

[[symbol]]
canonical = "gbpjpy"
aliases = ["gbp/jpy", "gbp-jpy"]
pip_position = 2
digits = 3
category = "forex"
base_currency = "GBP"
quote_currency = "JPY"
pnl_currency = "JPY"
lot_base_units = 100000
lot_step_units = 1000
"#,
    )
    .unwrap()
}

fn active_symbol_fixture() -> ReplayPathFixture {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "qs_backtest_server_active_symbol_{}_{}",
        std::process::id(),
        unique
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    let ticks = [
        (0, 2000.0, 2000.2),
        (1, 2000.2, 2000.4),
        (2, 2001.2, 2001.4),
        (3, 2001.0, 2001.2),
    ]
    .into_iter()
    .map(|(second, bid, ask)| Tick {
        exchange: "fixture".into(),
        symbol: "XAUUSD".into(),
        ts: fixture_ts(second),
        bid: Some(bid),
        ask: Some(ask),
        last: None,
        volume: Some(1.0),
        flags: None,
    })
    .collect::<Vec<_>>();
    assert_eq!(store.insert_ticks(&ticks).unwrap(), ticks.len());

    ReplayPathFixture {
        state: Arc::new({
            let symbol_registry = active_symbol_registry();
            let instrument_domain =
                backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
            ServerState {
                symbol_registry,
                instrument_domain,
                profile_registry: RwLock::new(ProfileRegistry::empty()),
                data_dir: data_dir.to_string_lossy().into_owned(),
                profiles_path: String::new(),
                start_time: Instant::now(),
                jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
                max_retained_jobs: 1_000,
                artifact_store: test_artifact_store(data_dir.join("artifacts")),
            }
        }),
        data_dir,
    }
}

fn active_symbol_request() -> RunBacktestRequest {
    RunBacktestRequest {
        request: BacktestRunSpec {
            symbol: "XAUUSD".into(),
            symbols: vec!["XAU/USD".into(), "GBPJPY".into()],
            all_symbols: false,
            exchange: "fixture".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: Some("2026-01-15T10:00:00".into()),
            to: Some("2026-01-15T10:00:03".into()),
            raw_signals: vec![
                RawSignalMsg::Entry {
                    ts: "2026-01-15T09:59:59".into(),
                    symbol: "GBP/JPY".into(),
                    side: "Sell".into(),
                    order_type: "Market".into(),
                    price: Some(190.0),
                    risk: 1.0,
                    stoploss: Some(191.0),
                    targets: vec![189.0],
                    group: None,
                    trade_id: Some("filtered-gbpjpy".into()),
                },
                RawSignalMsg::Entry {
                    ts: "2026-01-15T10:00:00".into(),
                    symbol: "XAUUSD".into(),
                    side: "Buy".into(),
                    order_type: "Market".into(),
                    price: Some(2000.2),
                    risk: 1.0,
                    stoploss: Some(1999.0),
                    targets: vec![2001.0],
                    group: None,
                    trade_id: Some("active-xauusd".into()),
                },
            ],
            profile: None,
            profile_def: Some(fixture_inline_profile()),
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.01 }),
            },
        },
        future: FutureQuoteConfigMsg {
            signal_latency_ms: 500,
            account_currency: "USD".into(),
            conversion_stale_after_ms: 300_000,
            ..FutureQuoteConfigMsg::default()
        },
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    }
}

fn read_artifact_bytes(state: &ServerState, reference: &ResultArtifactRefMsg) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut offset = 0;
    loop {
        let response = handle_get_result_artifact_chunk(
            state,
            &GetResultArtifactChunkRequest {
                artifact_id: reference.artifact_id.clone(),
                offset,
            },
        );
        assert!(response.success, "chunk: {:?}", response.error);
        assert_eq!(response.offset, offset);
        let chunk = BASE64_STANDARD.decode(response.data_base64).unwrap();
        bytes.extend_from_slice(&chunk);
        offset += chunk.len() as u64;
        if response.eof {
            break;
        }
    }
    assert_eq!(bytes.len() as u64, reference.byte_len);
    assert_eq!(
        backtest_server::artifact_store::sha256_hex(&bytes),
        reference.sha256
    );
    bytes
}

fn assert_future_quote_results_equal(
    expected: &BacktestResultMsg,
    actual: &BacktestResultMsg,
    path: &str,
) {
    let expected_future = expected.future.as_ref().expect("sync FutureQuote result");
    let actual_future = actual
        .future
        .as_ref()
        .unwrap_or_else(|| panic!("{path} FutureQuote result"));

    assert_eq!(
        expected_future.format_version, actual_future.format_version,
        "{path} format_version"
    );
    assert_eq!(
        expected_future.execution_metadata, actual_future.execution_metadata,
        "{path} execution_metadata"
    );
    assert_eq!(
        expected_future.recorded_fills, actual_future.recorded_fills,
        "{path} recorded_fills"
    );
    assert_eq!(
        expected_future.action_dispositions, actual_future.action_dispositions,
        "{path} action_dispositions"
    );
    assert_eq!(
        expected_future.close_events, actual_future.close_events,
        "{path} close_events"
    );
    assert_eq!(
        expected_future.completed_positions, actual_future.completed_positions,
        "{path} completed_positions"
    );
    assert_eq!(
        expected_future.open_positions, actual_future.open_positions,
        "{path} open_positions"
    );
    assert_eq!(
        expected_future.pending_orders, actual_future.pending_orders,
        "{path} pending_orders"
    );
    assert_eq!(
        expected_future.pending_order_lifecycle, actual_future.pending_order_lifecycle,
        "{path} pending_order_lifecycle"
    );
    assert_eq!(
        expected_future.mtm_equity_curve, actual_future.mtm_equity_curve,
        "{path} mtm_equity_curve"
    );
    assert_eq!(
        expected_future.mtm_max_drawdown, actual_future.mtm_max_drawdown,
        "{path} mtm_max_drawdown"
    );
    assert_eq!(
        expected_future.mtm_max_drawdown_pct, actual_future.mtm_max_drawdown_pct,
        "{path} mtm_max_drawdown_pct"
    );
    assert_eq!(
        expected_future.provider_evaluation, actual_future.provider_evaluation,
        "{path} provider_evaluation"
    );
    assert_eq!(
        serde_json::to_value(expected).unwrap(),
        serde_json::to_value(actual).unwrap(),
        "{path} complete BacktestResultMsg"
    );
}

// ── RPC Types Serde ─────────────────────────────────────────────────────────

#[test]
fn ping_response_serde_roundtrip() {
    let resp = PingResponse {
        status: "OK".into(),
        uptime_secs: 120,
        data_dir: "/data".into(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let decoded: PingResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.status, "OK");
    assert_eq!(decoded.uptime_secs, 120);
}

#[test]
fn backtest_config_msg_serde_roundtrip() {
    let msg = BacktestConfigMsg {
        initial_balance: Some(50_000.0),
        close_on_finish: Some(false),
        fill_model: Some("MidPrice".into()),
        sizing: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: BacktestConfigMsg = serde_json::from_str(&json).unwrap();
    assert!((decoded.initial_balance.unwrap() - 50_000.0).abs() < f64::EPSILON);
    assert_eq!(decoded.close_on_finish, Some(false));
    assert_eq!(decoded.fill_model.unwrap(), "MidPrice");
}

#[test]
fn run_backtest_request_serde_roundtrip() {
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: Some("2026-01-01".into()),
        to: Some("2026-02-01".into()),
        raw_signals: vec![sample_raw_signal()],
        profile: Some("aggressive".into()),
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: Some(10000.0),
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: BacktestRunSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.symbol, "eurusd");
    assert_eq!(decoded.exchange, "ctrader");
    assert_eq!(decoded.raw_signals.len(), 1);
    assert_eq!(decoded.profile, Some("aggressive".into()));
}

#[test]
fn evaluation_serde_defaults_and_conversion_use_current_defaults() {
    let request = RunBacktestRequest {
        request: sample_run_request(),
        future: FutureQuoteConfigMsg {
            account_currency: "USD".into(),
            conversion_stale_after_ms: 300_000,
            ..FutureQuoteConfigMsg::default()
        },
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    };
    let mut omitted_fields = serde_json::to_value(&request).unwrap();
    omitted_fields
        .as_object_mut()
        .expect("request object")
        .remove("evaluation");
    omitted_fields
        .as_object_mut()
        .expect("request object")
        .remove("result_delivery");
    let decoded: RunBacktestRequest = serde_json::from_value(omitted_fields).unwrap();
    assert_eq!(decoded.result_delivery, ResultDeliveryMsg::Auto);
    assert_eq!(decoded.evaluation.sections, EvaluationSectionMsg::ALL);
    assert_eq!(decoded.evaluation.rolling_window, 20);
    assert_eq!(decoded.evaluation.minimum_breakdown_bucket_count, 1);
    assert_eq!(decoded.evaluation.maximum_breakdown_rows, None);
    assert_eq!(decoded.evaluation.source_coverage, None);
    assert!(!decoded.evaluation.include_positions);
    assert_eq!(decoded.evaluation.maximum_position_rows, None);

    let mut omitted_multi_fields = serde_json::to_value(multi_request(&replay_request())).unwrap();
    omitted_multi_fields
        .as_object_mut()
        .expect("multi request object")
        .remove("result_delivery");
    let decoded_multi: RunBacktestMultiRequest =
        serde_json::from_value(omitted_multi_fields).unwrap();
    assert_eq!(decoded_multi.result_delivery, ResultDeliveryMsg::Auto);

    let custom = ProviderEvaluationOptionsMsg {
        context: EvaluationContextMsg {
            provider_id: Some("provider-1".into()),
            source_id: Some("telegram:42".into()),
        },
        source_coverage: Some(SourceCoverageCountsMsg {
            raw_messages: 4,
            parsed_messages: 2,
            skipped_messages: 1,
            failed_messages: 1,
            emitted_signals: 3,
            emitted_entry_signals: 2,
        }),
        sections: vec![
            EvaluationSectionMsg::Coverage,
            EvaluationSectionMsg::PositionPerformance,
            EvaluationSectionMsg::Breakdowns,
        ],
        filter: PositionFilterMsg {
            symbols: vec!["EUR/USD".into(), "NASDAQ".into()],
            sides: vec![EvaluationPositionSideMsg::Long],
            groups: vec![EvaluationGroupFilterMsg::Named("trend".into())],
            close_reasons: vec!["target".into(), "manual".into()],
            tags: BTreeMap::new(),
        },
        breakdowns: vec![BreakdownDimensionMsg::Symbol, BreakdownDimensionMsg::Group],
        bootstrap: BootstrapConfigMsg {
            samples: 500,
            confidence_level: 0.9,
            seed: 7,
            minimum_sample_size: 3,
        },
        rolling_window: 8,
        minimum_breakdown_bucket_count: 2,
        maximum_breakdown_rows: Some(25),
        include_positions: true,
        maximum_position_rows: Some(10),
    };
    let wire = serde_json::to_string(&custom).unwrap();
    let roundtrip: ProviderEvaluationOptionsMsg = serde_json::from_str(&wire).unwrap();
    assert_eq!(roundtrip, custom);

    let internal = evaluation_options_from_msg(&roundtrip, &evaluation_symbol_registry()).unwrap();
    assert_eq!(internal.context.provider_id.as_deref(), Some("provider-1"));
    assert_eq!(internal.context.source_id.as_deref(), Some("telegram:42"));
    assert!(internal.sections.contains(&EvaluationSection::Coverage));
    assert!(
        internal
            .sections
            .contains(&EvaluationSection::PositionPerformance)
    );
    assert!(internal.sections.contains(&EvaluationSection::Breakdowns));
    assert_eq!(internal.filter.symbols, ["eurusd", "us100"]);
    assert_eq!(internal.filter.sides, [PositionSide::Long]);
    assert_eq!(internal.filter.groups, [GroupFilter::Named("trend".into())]);
    assert_eq!(
        internal.breakdowns,
        [BreakdownDimension::Symbol, BreakdownDimension::Group]
    );
    assert_eq!(internal.bootstrap.samples, 500);
    assert_eq!(internal.rolling_window, 8);
    assert_eq!(internal.minimum_breakdown_bucket_count, 2);
    assert_eq!(internal.maximum_breakdown_rows, Some(25));
    assert!(internal.include_position_rows);
    assert_eq!(internal.maximum_position_rows, Some(10));
    assert_eq!(internal.source_coverage.unwrap().raw_messages, 4);
}

#[test]
fn recursively_rejects_unknown_fields_across_sync_submit_and_multi() {
    let request = replay_request();
    let mut unknown_selector = serde_json::to_value(&request).unwrap();
    unknown_selector["evaluation"]["sections"] = serde_json::json!(["mystery"]);
    let selector_error = serde_json::from_value::<RunBacktestRequest>(unknown_selector)
        .expect_err("unknown section must fail typed deserialization");
    assert!(selector_error.to_string().contains("unknown variant"));

    let mut unknown_config = serde_json::to_value(&request).unwrap();
    unknown_config["evaluation"]["mystery_limit"] = serde_json::json!(3);
    let config_error = serde_json::from_value::<RunBacktestRequest>(unknown_config)
        .expect_err("unknown evaluation config must fail typed deserialization");
    assert!(config_error.to_string().contains("unknown field"));

    let mut request_typo = serde_json::to_value(&request).unwrap();
    request_typo["request"]["data_typo"] = serde_json::json!("tick");
    assert!(
        serde_json::from_value::<RunBacktestRequest>(request_typo)
            .expect_err("nested canonical request typos must be rejected")
            .to_string()
            .contains("data_typo")
    );

    let mut config_typo = serde_json::to_value(&request).unwrap();
    config_typo["request"]["config"]["initial_balnce"] = serde_json::json!(10_000.0);
    assert!(
        serde_json::from_value::<RunBacktestRequest>(config_typo)
            .expect_err("nested canonical config typos must be rejected")
            .to_string()
            .contains("initial_balnce")
    );

    let mut signal_typo = serde_json::to_value(&request).unwrap();
    signal_typo["request"]["raw_signals"][0]["stop_loss"] = serde_json::json!(1.09);
    assert!(
        serde_json::from_value::<RunBacktestRequest>(signal_typo)
            .expect_err("nested canonical raw signal typos must be rejected")
            .to_string()
            .contains("stop_loss")
    );

    let mut missing_entry_risk = serde_json::to_value(&request).unwrap();
    missing_entry_risk["request"]["raw_signals"][0]
        .as_object_mut()
        .expect("entry object")
        .remove("risk");
    assert!(
        serde_json::from_value::<RunBacktestRequest>(missing_entry_risk)
            .expect_err("canonical entries must require risk")
            .to_string()
            .contains("missing field `risk`")
    );

    let mut obsolete_entry_size = serde_json::to_value(&request).unwrap();
    obsolete_entry_size["request"]["raw_signals"][0]["size"] = serde_json::json!(1.0);
    assert!(
        serde_json::from_value::<RunBacktestRequest>(obsolete_entry_size)
            .expect_err("canonical entries must reject obsolete size")
            .to_string()
            .contains("unknown field `size`")
    );

    let mut profile_typo = serde_json::to_value(&request).unwrap();
    profile_typo["request"]["profile_def"]["close_ratio"] = serde_json::json!([1.0]);
    assert!(
        serde_json::from_value::<RunBacktestRequest>(profile_typo)
            .expect_err("nested canonical profile typos must be rejected")
            .to_string()
            .contains("close_ratio")
    );

    let mut future_typo = serde_json::to_value(&request).unwrap();
    future_typo["future"]["signal_lattency_ms"] = serde_json::json!(250);
    let typo_error = serde_json::from_value::<RunBacktestRequest>(future_typo)
        .expect_err("FutureQuote field typos must not silently default");
    assert!(typo_error.to_string().contains("signal_lattency_ms"));

    let mut delivery_typo = serde_json::to_value(&request).unwrap();
    delivery_typo["result_delivry"] = serde_json::json!("artifact");
    assert!(
        serde_json::from_value::<RunBacktestRequest>(delivery_typo)
            .expect_err("delivery field typos must be rejected")
            .to_string()
            .contains("result_delivry")
    );

    let mut invalid_delivery = serde_json::to_value(&request).unwrap();
    invalid_delivery["result_delivery"] = serde_json::json!("stream");
    assert!(
        serde_json::from_value::<RunBacktestRequest>(invalid_delivery)
            .expect_err("unknown delivery modes must be rejected")
            .to_string()
            .contains("unknown variant")
    );

    let mut outer_typo = serde_json::to_value(&request).unwrap();
    outer_typo["unexpected_outer"] = serde_json::json!(true);
    let outer_error = serde_json::from_value::<RunBacktestRequest>(outer_typo)
        .expect_err("outer envelope typos must be rejected");
    assert!(outer_error.to_string().contains("unexpected_outer"));

    let mut submit_outer = serde_json::to_value(SubmitBacktestRequest {
        request: request.clone(),
    })
    .unwrap();
    submit_outer["requset"] = submit_outer["request"].clone();
    assert!(
        serde_json::from_value::<SubmitBacktestRequest>(submit_outer)
            .expect_err("async canonical outer typos must be rejected")
            .to_string()
            .contains("requset")
    );

    let mut submit_nested = serde_json::to_value(SubmitBacktestRequest {
        request: request.clone(),
    })
    .unwrap();
    submit_nested["request"]["request"]["raw_signals"][0]["tradeid"] = serde_json::json!("typo");
    assert!(
        serde_json::from_value::<SubmitBacktestRequest>(submit_nested)
            .expect_err("async canonical nested typos must be rejected")
            .to_string()
            .contains("tradeid")
    );

    let mut multi_outer = serde_json::to_value(multi_request(&request)).unwrap();
    multi_outer["unexpected_outer"] = serde_json::json!(true);
    assert!(
        serde_json::from_value::<RunBacktestMultiRequest>(multi_outer)
            .expect_err("multi outer typos must be rejected")
            .to_string()
            .contains("unexpected_outer")
    );

    let mut multi_nested = serde_json::to_value(multi_request(&request)).unwrap();
    multi_nested["request"]["config"]["close_on_finsih"] = serde_json::json!(true);
    assert!(
        serde_json::from_value::<RunBacktestMultiRequest>(multi_nested)
            .expect_err("multi canonical nested config typos must be rejected")
            .to_string()
            .contains("close_on_finsih")
    );

    let mut multi_profile = serde_json::to_value(multi_request(&request)).unwrap();
    multi_profile["request"]["profiles"][0]["group_overide"] = serde_json::json!("typo");
    serde_json::from_value::<RunBacktestMultiRequest>(multi_profile)
        .expect_err("multi canonical inline profile typos must be rejected");

    let mut unwrapped_spec = serde_json::to_value(sample_run_request()).unwrap();
    unwrapped_spec["compatibility_extension"] = serde_json::json!(true);
    unwrapped_spec["config"]["compatibility_config_extension"] = serde_json::json!(true);
    let decoded = serde_json::from_value::<BacktestRunSpec>(unwrapped_spec)
        .expect("unwrapped run specs retain unknown-field compatibility");

    let mut unwrapped_profile = serde_json::to_value(fixture_inline_profile()).unwrap();
    unwrapped_profile["compatibility_profile_extension"] = serde_json::json!(true);
    serde_json::from_value::<ManagementProfileMsg>(unwrapped_profile)
        .expect("unwrapped inline profiles retain unknown-field compatibility");
    assert_eq!(decoded.raw_signals.len(), 1);
}

#[test]
fn evaluation_conversion_rejects_inconsistent_config() {
    let config = ProviderEvaluationOptionsMsg {
        sections: vec![EvaluationSectionMsg::Coverage],
        breakdowns: vec![BreakdownDimensionMsg::Symbol],
        ..ProviderEvaluationOptionsMsg::default()
    };
    let error = evaluation_options_from_msg(&config, &evaluation_symbol_registry())
        .expect_err("breakdowns without the section must be rejected");
    assert!(error.to_string().contains("breakdowns report section"));
}

#[test]
fn evaluation_conversion_rejects_integrated_tag_selectors() {
    let registry = evaluation_symbol_registry();
    let tag_filter = ProviderEvaluationOptionsMsg {
        filter: PositionFilterMsg {
            tags: BTreeMap::from([("session".into(), vec!["us".into()])]),
            ..PositionFilterMsg::default()
        },
        ..ProviderEvaluationOptionsMsg::default()
    };
    let filter_error = evaluation_options_from_msg(&tag_filter, &registry)
        .expect_err("integrated tag filters must be rejected");
    assert!(filter_error.to_string().contains("tag filters"));

    let tag_breakdown = ProviderEvaluationOptionsMsg {
        breakdowns: vec![BreakdownDimensionMsg::Tag("session".into())],
        ..ProviderEvaluationOptionsMsg::default()
    };
    let breakdown_error = evaluation_options_from_msg(&tag_breakdown, &registry)
        .expect_err("integrated tag breakdowns must be rejected");
    assert!(breakdown_error.to_string().contains("tag breakdowns"));
}

#[test]
fn evaluation_conversion_rejects_unknown_filter_symbols() {
    let config = ProviderEvaluationOptionsMsg {
        filter: PositionFilterMsg {
            symbols: vec!["not-a-market".into()],
            ..PositionFilterMsg::default()
        },
        ..ProviderEvaluationOptionsMsg::default()
    };
    let error = evaluation_options_from_msg(&config, &evaluation_symbol_registry())
        .expect_err("unknown evaluation symbols must not silently select zero rows");
    assert!(
        error
            .to_string()
            .contains("unknown evaluation symbol `not-a-market`")
    );
}

#[test]
fn artifact_response_is_compact_and_reconstructs_the_complete_result() {
    let fixture = replay_path_fixture();
    let mut artifact_request = replay_request();
    artifact_request.result_delivery = ResultDeliveryMsg::Artifact;
    let response = handle_run_backtest(&fixture.state, &artifact_request);
    assert!(response.success, "artifact: {:?}", response.error);
    let summary = response.result.as_ref().expect("compact result summary");
    assert!(summary.equity_curve.is_empty());
    assert!(summary.trade_log.len() <= 30);
    assert!(summary.positions.len() <= 15);
    assert!(!response.inline_complete);
    let reference = response.artifact.clone().expect("artifact reference");
    assert!(serde_json::to_vec(&response).unwrap().len() < reference.byte_len as usize);
    assert_eq!(reference.format_version, RESULT_FORMAT_VERSION);

    let bytes = read_artifact_bytes(&fixture.state, &reference);
    let artifact_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let actual: BacktestResultMsg = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_value(&actual).unwrap(), artifact_json);
    assert_eq!(actual.total_trades, 2);
    assert!(actual.future.is_some());
    let deleted = handle_delete_result_artifact(
        &fixture.state,
        &DeleteResultArtifactRequest {
            artifact_id: reference.artifact_id,
        },
    );
    assert!(deleted.success, "delete: {:?}", deleted.error);
}

#[test]
fn inline_mode_returns_a_compact_error_when_result_exceeds_the_limit() {
    let fixture = replay_path_fixture();
    let symbol_registry = evaluation_symbol_registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    let state = ServerState {
        symbol_registry,
        instrument_domain,
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: fixture.data_dir.to_string_lossy().into_owned(),
        profiles_path: String::new(),
        start_time: Instant::now(),
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        max_retained_jobs: 1_000,
        artifact_store: ArtifactStore::new(
            fixture.data_dir.join("small-limit-artifacts"),
            1,
            128,
            Duration::from_secs(3_600),
            1024 * 1024,
        )
        .unwrap(),
    };
    let mut request = replay_request();
    request.result_delivery = ResultDeliveryMsg::Inline;

    let response = handle_run_backtest(&state, &request);
    assert!(!response.success);
    assert!(response.result.is_none());
    assert!(response.artifact.is_none());
    assert!(!response.inline_complete);
    assert!(
        response
            .error
            .unwrap()
            .contains("exceeding the configured inline limit")
    );

    request.result_delivery = ResultDeliveryMsg::Auto;
    let auto = handle_run_backtest(&state, &request);
    assert!(auto.success, "auto: {:?}", auto.error);
    assert!(auto.result.is_none());
    assert!(auto.artifact.is_some());
    assert!(!auto.inline_complete);
    assert!(serde_json::to_vec(&auto).unwrap().len() < 1024);
}

#[test]
fn multi_artifact_response_reconstructs_all_profile_results() {
    let fixture = replay_path_fixture();
    let request = replay_request();
    let mut multi = multi_request(&request);
    multi.result_delivery = ResultDeliveryMsg::Artifact;

    let response = handle_run_backtest_multi(&fixture.state, &multi);
    assert!(response.success, "multi artifact: {:?}", response.error);
    assert_eq!(response.results.len(), 1);
    assert!(response.results[0].result.is_some());
    assert!(!response.inline_complete);
    let reference = response.artifact.clone().expect("multi artifact reference");
    assert!(serde_json::to_vec(&response).unwrap().len() < reference.byte_len as usize);
    let bytes = read_artifact_bytes(&fixture.state, &reference);
    let results: Vec<ProfileResult> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].success, "profile: {:?}", results[0].error);
    assert!(results[0].result.is_some());
}

#[test]
fn async_artifact_job_retains_only_a_compact_result_summary() {
    let fixture = replay_path_fixture();
    let mut request = replay_request();
    request.result_delivery = ResultDeliveryMsg::Artifact;
    let submission = handle_submit_backtest(
        &fixture.state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(submission.success, "submit: {:?}", submission.error);
    let job_id = submission.job_id.unwrap();
    run_job_and_store(fixture.state.clone(), job_id.clone(), request);

    {
        let jobs = fixture.state.jobs.lock().unwrap();
        let job = &jobs[&job_id];
        assert!(job.result.is_some());
        assert!(job.artifact.is_some());
        assert!(!job.inline_complete);
        assert!(!job.artifact_consumed);
    }
    let response = handle_get_backtest_result(
        &fixture.state,
        &GetBacktestResultRequest {
            job_id: job_id.clone(),
        },
    );
    assert!(response.success, "result: {:?}", response.error);
    assert!(response.result.is_some());
    let artifact = response.artifact.expect("async artifact reference");
    assert!(!response.inline_complete);
    assert!(!response.artifact_consumed);

    let deleted = handle_delete_result_artifact(
        &fixture.state,
        &DeleteResultArtifactRequest {
            artifact_id: artifact.artifact_id,
        },
    );
    assert!(deleted.success, "delete: {:?}", deleted.error);
    let consumed = handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(!consumed.success);
    assert!(consumed.artifact.is_none());
    assert!(consumed.artifact_consumed);
    assert_eq!(
        consumed.error.as_deref(),
        Some("Job result artifact has already been consumed")
    );
}

#[test]
fn sync_async_and_multi_profile_future_quote_results_are_equivalent() {
    let fixture = replay_path_fixture();
    let request = replay_request();

    let sync_response = handle_run_backtest(&fixture.state, &request);
    assert!(sync_response.success, "sync: {:?}", sync_response.error);
    let sync_result = sync_response.result.expect("sync result");
    let sync_future = sync_result
        .future
        .as_ref()
        .expect("sync FutureQuote result");

    assert_eq!(
        sync_future
            .recorded_fills
            .as_array()
            .expect("recorded_fills array")
            .len(),
        4
    );
    assert_eq!(
        sync_future
            .completed_positions
            .as_array()
            .expect("completed_positions array")
            .len(),
        2
    );
    assert!(
        !sync_future.provider_evaluation.is_null(),
        "evaluation options must produce a report"
    );
    let evaluation = &sync_future.provider_evaluation;
    assert_eq!(evaluation["coverage"]["source"]["raw_messages"], 3);
    assert_eq!(evaluation["coverage"]["source"]["emitted_signals"], 3);
    assert_eq!(evaluation["coverage"]["source"]["emitted_entry_signals"], 2);
    assert_eq!(evaluation["coverage"]["lifecycle"]["candidates"], 2);
    assert_eq!(evaluation["coverage"]["lifecycle"]["accepted"], 2);
    assert_eq!(evaluation["coverage"]["lifecycle"]["rejected"], 0);
    assert_eq!(
        evaluation["position_rows"]["available_rows"],
        evaluation["coverage"]["selected_positions"]
    );
    let rows = evaluation["position_rows"]["rows"]
        .as_array()
        .expect("filtered position rows array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["dimensions"]["symbol"], "eurusd");
    assert!(rows[0]["outcome"].is_number());
    assert!(rows[0]["r_multiple"].is_number());
    assert_eq!(rows[0]["outcome_classification"], "win");

    let submission = handle_submit_backtest(
        &fixture.state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(submission.success, "async submit: {:?}", submission.error);
    let job_id = submission.job_id.expect("submitted job id");
    run_job_and_store(fixture.state.clone(), job_id.clone(), request.clone());
    let status = handle_get_backtest_status(
        &fixture.state,
        &GetBacktestStatusRequest {
            job_id: job_id.clone(),
        },
    );
    assert_eq!(status.status, "Completed", "async: {:?}", status.error);
    assert_eq!(status.progress.stage, "completed");
    assert_eq!(status.progress.processed_symbols, 1);
    assert_eq!(status.progress.total_symbols, 1);
    assert_eq!(status.progress.processed_events, 3);
    assert_eq!(status.progress.total_events, 3);
    assert_eq!(status.progress.processed_signals, 2);
    assert_eq!(status.progress.total_signals, 2);
    let async_response =
        handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(async_response.success, "async: {:?}", async_response.error);
    let async_result = async_response.result.expect("async result");

    let multi_response = handle_run_backtest_multi(&fixture.state, &multi_request(&request));
    assert!(multi_response.success, "multi: {:?}", multi_response.error);
    assert_eq!(multi_response.results.len(), 1);
    let profile_result = &multi_response.results[0];
    assert_eq!(profile_result.profile, "future-parity");
    assert!(profile_result.success, "multi: {:?}", profile_result.error);
    let multi_result = profile_result
        .result
        .as_ref()
        .expect("multi-profile result");

    let metadata = &sync_future.execution_metadata;
    assert_eq!(metadata["tags"]["data.exchange"], "fixture");
    assert_eq!(metadata["tags"]["data.type"], "tick");
    assert_eq!(metadata["tags"]["data.timeframe"], "none");
    assert_eq!(metadata["tags"]["execution.signal_latency_ms"], "500");
    assert_eq!(metadata["tags"]["data.requested_symbols"], "eurusd");
    assert_eq!(metadata["tags"]["data.active_symbols"], "eurusd");
    assert_eq!(metadata["tags"]["data.idle_symbols"], "");
    assert_eq!(metadata["tags"]["data.idle_run"], "false");
    assert_eq!(
        metadata["tags"]["data.loading_from"],
        "2026-01-15T10:00:00.500"
    );
    assert_eq!(metadata["tags"]["profile.identity"], "future-parity");
    assert!(
        metadata["tags"]["profile.options"]
            .as_str()
            .is_some_and(|options| options.contains("future-parity"))
    );
    assert_eq!(metadata["tags"]["sizing.identity"], "fixed_lot");
    assert_eq!(metadata["tags"]["symbol.eurusd.pip_position"], "4");
    assert_eq!(metadata["tags"]["symbol.eurusd.lot_base_units"], "100000");
    assert_eq!(
        metadata["tags"]["economics.guard"],
        "legacy-economic-guard-v1"
    );
    assert_eq!(
        metadata["tags"]["economics.symbol.eurusd.status"],
        "supported"
    );
    assert_eq!(
        metadata["tags"]["economics.symbol.eurusd.model"],
        "legacy_fx_linear_v1"
    );
    assert_eq!(
        metadata["tags"]["economics.symbol.eurusd.contract_multiplier"],
        "100000"
    );
    let instrument_manifest = &metadata["instrument_manifest"];
    assert_eq!(
        instrument_manifest["instruments"]["eurusd"]["resolved"]["instrument"]["listing_venue"],
        "repository-default"
    );
    assert_eq!(
        instrument_manifest["instruments"]["eurusd"]["resolved"]["instrument"]["market_kind"],
        "fx_cfd"
    );
    assert_eq!(
        instrument_manifest["instruments"]["eurusd"]["spec"]["economics"]["pnl_model"],
        "fx_quote_linear_v1"
    );
    assert_eq!(
        instrument_manifest["stored_series"][0]["data_source"],
        "local-parquet"
    );
    assert_eq!(
        instrument_manifest["stored_series"][0]["source_partition"],
        "fixture"
    );
    assert_eq!(
        instrument_manifest["stored_series"][0]["source_symbol"],
        "EURUSD"
    );
    let sizing = &metadata["instrument_sizing"];
    assert_eq!(sizing[0]["symbol"], "eurusd");
    assert_eq!(sizing[0]["quantity"]["requested"], "1");
    assert_eq!(sizing[0]["quantity"]["adjusted"], "1");

    assert_future_quote_results_equal(&sync_result, &async_result, "async");
    assert_future_quote_results_equal(&sync_result, multi_result, "multi-profile");
}

#[test]
fn multi_profile_reopens_future_stream_for_each_profile() {
    let fixture = replay_path_fixture();
    let request = replay_request();
    let mut multi = multi_request(&request);
    let mut second_profile = fixture_inline_profile();
    second_profile.name = "future-reopen-second".into();
    multi
        .request
        .profiles
        .push(ProfileRef::Inline(second_profile));

    let response = handle_run_backtest_multi(&fixture.state, &multi);
    assert!(response.success, "multi reopen: {:?}", response.error);
    assert_eq!(response.results.len(), 2);
    for result in &response.results {
        assert!(result.success, "{}: {:?}", result.profile, result.error);
        let future = result
            .result
            .as_ref()
            .and_then(|result| result.future.as_ref())
            .expect("FutureQuote result");
        assert_eq!(future.recorded_fills.as_array().unwrap().len(), 4);
        assert_eq!(future.completed_positions.as_array().unwrap().len(), 2);
    }
}

#[test]
fn prunes_idle_explicit_symbols_and_matches_sync_async_multi() {
    let fixture = active_symbol_fixture();
    let request = active_symbol_request();

    let sync_response = handle_run_backtest(&fixture.state, &request);
    assert!(sync_response.success, "sync: {:?}", sync_response.error);
    let sync_result = sync_response.result.expect("sync result");
    let metadata = &sync_result
        .future
        .as_ref()
        .expect("sync FutureQuote result")
        .execution_metadata;
    assert_eq!(metadata["tags"]["data.requested_symbols"], "gbpjpy,xauusd");
    assert_eq!(metadata["tags"]["data.symbols"], "xauusd");
    assert_eq!(metadata["tags"]["data.active_symbols"], "xauusd");
    assert_eq!(metadata["tags"]["data.idle_symbols"], "gbpjpy");
    assert_eq!(metadata["tags"]["data.idle_run"], "false");
    assert_eq!(
        metadata["tags"]["data.loading_from"],
        "2026-01-15T10:00:00.500"
    );
    assert_eq!(
        metadata["currency_plan"]["primary_symbols"],
        serde_json::json!(["xauusd"])
    );

    let submission = handle_submit_backtest(
        &fixture.state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(submission.success, "async submit: {:?}", submission.error);
    let job_id = submission.job_id.expect("submitted job id");
    run_job_and_store(fixture.state.clone(), job_id.clone(), request.clone());
    let status = handle_get_backtest_status(
        &fixture.state,
        &GetBacktestStatusRequest {
            job_id: job_id.clone(),
        },
    );
    assert_eq!(status.status, "Completed", "async: {:?}", status.error);
    assert_eq!(status.progress.processed_symbols, 1);
    assert_eq!(status.progress.total_symbols, 1);
    assert_eq!(status.progress.processed_events, 2);
    assert_eq!(status.progress.total_events, 2);
    assert_eq!(status.progress.processed_signals, 1);
    assert_eq!(status.progress.total_signals, 1);
    let async_response =
        handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(async_response.success, "async: {:?}", async_response.error);
    let async_result = async_response.result.expect("async result");

    let multi_response = handle_run_backtest_multi(&fixture.state, &multi_request(&request));
    assert!(multi_response.success, "multi: {:?}", multi_response.error);
    let multi_result = multi_response.results[0]
        .result
        .as_ref()
        .expect("multi-profile result");

    assert_future_quote_results_equal(&sync_result, &async_result, "async active-symbol plan");
    assert_future_quote_results_equal(&sync_result, multi_result, "multi active-symbol plan");
}

#[test]
fn filtered_entry_and_management_only_run_is_idle_without_market_data() {
    let state = empty_state();
    let request = RunBacktestRequest {
        request: BacktestRunSpec {
            symbol: "XAUUSD".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "missing".into(),
            data_type: "tick".into(),
            timeframe: None,
            from: Some("2026-01-15T10:00:00".into()),
            to: Some("2026-01-15T10:01:00".into()),
            raw_signals: vec![
                RawSignalMsg::Entry {
                    ts: "2026-01-15T09:59:59".into(),
                    symbol: "GBPJPY".into(),
                    side: "Sell".into(),
                    order_type: "Market".into(),
                    price: Some(190.0),
                    risk: 1.0,
                    stoploss: Some(191.0),
                    targets: vec![189.0],
                    group: None,
                    trade_id: Some("filtered-out-of-scope".into()),
                },
                RawSignalMsg::CloseAll {
                    ts: "2026-01-15T10:00:00".into(),
                },
            ],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: None,
            },
        },
        future: FutureQuoteConfigMsg {
            signal_latency_ms: 250,
            account_currency: "USD".into(),
            conversion_stale_after_ms: 300_000,
            ..FutureQuoteConfigMsg::default()
        },
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    };

    let response = handle_run_backtest(&state, &request);
    assert!(response.success, "idle run: {:?}", response.error);
    let result = response.result.expect("idle result");
    assert_eq!(result.total_trades, 0);
    let future = result.future.expect("idle FutureQuote result");
    let tags = &future.execution_metadata["tags"];
    assert_eq!(tags["data.requested_symbols"], "xauusd");
    assert_eq!(tags["data.active_symbols"], "");
    assert_eq!(tags["data.idle_symbols"], "xauusd");
    assert_eq!(tags["data.idle_run"], "true");
    assert_eq!(tags["data.loading_from"], "2026-01-15T10:00:00.250");
    assert_eq!(
        future.execution_metadata["currency_plan"]["primary_symbols"],
        serde_json::json!([])
    );
    assert_eq!(
        future
            .action_dispositions
            .as_array()
            .expect("idle action dispositions")
            .len(),
        1
    );
}

#[test]
fn effective_start_after_requested_end_returns_zero_trade_result() {
    let fixture = active_symbol_fixture();
    let mut request = active_symbol_request();
    request.request.to = Some("2026-01-15T10:00:00".into());

    let response = handle_run_backtest(&fixture.state, &request);
    assert!(response.success, "early-load window: {:?}", response.error);
    let result = response.result.expect("early-load result");
    assert_eq!(result.total_trades, 0);
    let future = result.future.expect("early-load FutureQuote result");
    assert_eq!(
        future.execution_metadata["tags"]["data.loading_from"],
        "2026-01-15T10:00:00.500"
    );
    assert_eq!(
        future
            .action_dispositions
            .as_array()
            .expect("no-quote action dispositions")
            .len(),
        1
    );
}

#[test]
fn future_quote_bar_result_records_reproducibility_metadata_without_intrabar_claims() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "qs_backtest_server_bar_metadata_{}_{}",
        std::process::id(),
        unique
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    let bars = [
        (0, 1.1000, 1.1010, 1.0990, 1.1002),
        (1, 1.1002, 1.1020, 1.1000, 1.1010),
        (2, 1.1010, 1.1030, 1.1005, 1.1020),
    ]
    .into_iter()
    .map(|(second, open, high, low, close)| Bar {
        exchange: "fixture".into(),
        symbol: "EURUSD".into(),
        timeframe: Timeframe::M1,
        ts: fixture_ts(second),
        open,
        high,
        low,
        close,
        tick_vol: 10,
        volume: 10,
        spread: 3,
    })
    .collect::<Vec<_>>();
    assert_eq!(store.insert_bars(&bars).unwrap(), bars.len());
    let symbol_registry = evaluation_symbol_registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    let state = ServerState {
        symbol_registry,
        instrument_domain,
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: data_dir.to_string_lossy().into_owned(),
        profiles_path: String::new(),
        start_time: Instant::now(),
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        max_retained_jobs: 1_000,
        artifact_store: test_artifact_store(data_dir.join("artifacts")),
    };
    let request = RunBacktestRequest {
        request: BacktestRunSpec {
            symbol: "EUR/USD".into(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: "FIXTURE".into(),
            data_type: "bar".into(),
            timeframe: Some("1m".into()),
            from: Some("2026-01-15T12:00:00+02:00".into()),
            to: Some("2026-01-15T05:00:02-05:00".into()),
            raw_signals: vec![RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "EURUSD".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: Some(1.1002),
                risk: 1.0,
                stoploss: Some(1.0990),
                targets: vec![1.1015],
                group: None,
                trade_id: Some("bar-metadata".into()),
            }],
            profile: None,
            profile_def: Some(fixture_inline_profile()),
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.01 }),
            },
        },
        future: FutureQuoteConfigMsg {
            signal_latency_ms: 750,
            account_currency: "USD".into(),
            conversion_stale_after_ms: 300_000,
            ..FutureQuoteConfigMsg::default()
        },
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    };

    let response = handle_run_backtest(&state, &request);
    assert!(response.success, "bar run: {:?}", response.error);
    let metadata = &response.result.unwrap().future.unwrap().execution_metadata;
    let tags = &metadata["tags"];
    assert_eq!(tags["data.exchange"], "fixture");
    assert_eq!(tags["data.type"], "bar");
    assert_eq!(tags["data.timeframe"], "1m");
    assert_eq!(tags["data.requested_from"], "2026-01-15T10:00:00");
    assert_eq!(tags["data.requested_to"], "2026-01-15T10:00:02");
    assert_eq!(tags["data.bar_quote_convention"], "close_only_zero_spread");
    assert_eq!(tags["data.intrabar_simulation"], "false");
    assert_eq!(tags["execution.signal_latency_ms"], "750");
    assert_eq!(tags["profile.identity"], "future-parity");
    assert_eq!(tags["sizing.identity"], "fixed_lot");
    assert!(
        tags["sizing.options"]
            .as_str()
            .is_some_and(|options| options.contains("FixedLot"))
    );
    assert_eq!(tags["symbol.eurusd.category"], "forex");
    assert_eq!(tags["symbol.eurusd.digits"], "5");

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn cancelled_job_remains_cancelled_and_never_stores_a_result() {
    let fixture = replay_path_fixture();
    let request = replay_request();
    let submission = handle_submit_backtest(
        &fixture.state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    let job_id = submission.job_id.unwrap();
    let cancelled = handle_cancel_backtest(
        &fixture.state,
        &CancelBacktestRequest {
            job_id: job_id.clone(),
        },
    );
    assert!(cancelled.success);

    run_job_and_store(fixture.state.clone(), job_id.clone(), request);
    let status = handle_get_backtest_status(
        &fixture.state,
        &GetBacktestStatusRequest {
            job_id: job_id.clone(),
        },
    );
    assert_eq!(status.status, "Cancelled");
    assert_eq!(status.progress.stage, "cancelled");
    let result = handle_get_backtest_result(&fixture.state, &GetBacktestResultRequest { job_id });
    assert!(!result.success);
    assert!(result.result.is_none());
}

#[test]
fn cancelled_active_worker_remains_capacity_accounted_until_it_releases_ownership() {
    let state = Arc::new(ServerState {
        max_retained_jobs: 1,
        ..empty_state()
    });
    let request = sample_run_request();
    let first = submit_for_test(&state, &request);
    let first_id = first.job_id.unwrap();

    assert!(
        handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: first_id.clone(),
            },
        )
        .success
    );
    let blocked = submit_for_test(&state, &request);
    assert!(!blocked.success);
    assert!(blocked.error.unwrap().contains("job limit"));

    run_job_for_test(state.clone(), first_id.clone(), request.clone());
    assert!(!state.jobs.lock().unwrap()[&first_id].worker_active);
    assert!(submit_for_test(&state, &request).success);
}

#[test]
fn async_job_store_is_bounded_and_cleanup_removes_terminal_jobs() {
    let state = ServerState {
        max_retained_jobs: 2,
        ..empty_state()
    };
    let request = sample_run_request();
    let first = submit_for_test(&state, &request);
    let second = submit_for_test(&state, &request);
    assert!(first.success && second.success);
    let full = submit_for_test(&state, &request);
    assert!(!full.success);
    assert!(full.error.unwrap().contains("job limit"));

    let first_id = first.job_id.unwrap();
    handle_cancel_backtest(
        &state,
        &CancelBacktestRequest {
            job_id: first_id.clone(),
        },
    );
    state
        .jobs
        .lock()
        .unwrap()
        .get_mut(&first_id)
        .unwrap()
        .worker_active = false;
    let replacement = submit_for_test(&state, &request);
    assert!(
        replacement.success,
        "worker-released terminal jobs should be evictable"
    );
    let jobs = state.jobs.lock().unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(!jobs.contains_key(&first_id));
    drop(jobs);

    let second_id = second.job_id.unwrap();
    let replacement_id = replacement.job_id.unwrap();
    for job_id in [second_id, replacement_id] {
        handle_cancel_backtest(
            &state,
            &CancelBacktestRequest {
                job_id: job_id.clone(),
            },
        );
        state
            .jobs
            .lock()
            .unwrap()
            .get_mut(&job_id)
            .unwrap()
            .worker_active = false;
    }
    assert_eq!(cleanup_expired_jobs(&state, Duration::ZERO), 2);
    assert!(state.jobs.lock().unwrap().is_empty());
}

#[test]
fn job_cleanup_and_admission_eviction_delete_owned_artifacts() {
    let state = ServerState {
        max_retained_jobs: 1,
        ..empty_state()
    };
    let insert_completed_artifact_job = |job_id: &str| {
        let artifact = state.artifact_store.persist_json(b"job artifact").unwrap();
        state.jobs.lock().unwrap().insert(
            job_id.into(),
            BacktestJob {
                status: JobStatus::Completed,
                submitted_at: Instant::now(),
                completed_at: Some(Instant::now()),
                progress: BacktestProgress::default(),
                result: None,
                artifact: Some(artifact.clone()),
                inline_complete: false,
                artifact_consumed: false,
                error: None,
                cancellation: JobCancellationToken::default(),
                worker_active: false,
                updates: tokio::sync::watch::channel(BacktestStatusResponse {
                    success: true,
                    job_id: job_id.into(),
                    status: "Completed".into(),
                    error: None,
                    elapsed_ms: Some(0),
                    progress: BacktestProgress::default(),
                })
                .0,
            },
        );
        artifact
    };

    let expired = insert_completed_artifact_job("expired-artifact-job");
    assert_eq!(cleanup_expired_jobs(&state, Duration::ZERO), 1);
    assert!(matches!(
        state.artifact_store.read_chunk(&expired.artifact_id, 0),
        Err(ArtifactStoreError::NotFound(_))
    ));

    let evicted = insert_completed_artifact_job("evicted-artifact-job");
    let replacement = submit_for_test(&state, &sample_run_request());
    assert!(replacement.success, "replacement: {:?}", replacement.error);
    assert!(matches!(
        state.artifact_store.read_chunk(&evicted.artifact_id, 0),
        Err(ArtifactStoreError::NotFound(_))
    ));
}

#[test]
fn future_scalar_validation_matches_sync_async_and_multi_before_planning() {
    let state = empty_state();
    let mut request = replay_request();
    request.future.slippage_pips = f64::NAN;

    let sync = handle_run_backtest(&state, &request);
    assert!(!sync.success);
    let expected = sync.error.expect("sync scalar error");
    assert!(expected.contains("slippage_pips must be finite"));

    let asynchronous = handle_submit_backtest(
        &state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(!asynchronous.success);
    assert!(asynchronous.job_id.is_none());
    assert_eq!(asynchronous.error.as_deref(), Some(expected.as_str()));
    assert!(state.jobs.lock().unwrap().is_empty());

    let multi = handle_run_backtest_multi(&state, &multi_request(&request));
    assert!(!multi.success);
    assert_eq!(multi.error.as_deref(), Some(expected.as_str()));
    assert_eq!(multi.results.len(), 1);
    assert_eq!(multi.results[0].error.as_deref(), Some(expected.as_str()));
}

#[test]
fn unsupported_crypto_economics_fail_consistently_before_market_data_access() {
    let mut state = empty_state();
    state.symbol_registry = SymbolRegistry::from_toml(
        r#"
[[symbol]]
canonical = "btcusd"
aliases = ["btc/usd"]
pip_position = 2
digits = 2
category = "crypto"
base_currency = "BTC"
quote_currency = "USD"
pnl_currency = "USD"
lot_base_units = 100000000
lot_step_units = 1
"#,
    )
    .unwrap();
    state.data_dir = "/path/that/must/not/be/scanned/by/crypto-guard".into();

    let mut request = replay_request();
    request.request.symbol = "BTC/USD".into();
    for signal in &mut request.request.raw_signals {
        if let RawSignalMsg::Entry { symbol, .. } = signal {
            *symbol = "BTCUSD".into();
        }
    }
    request.evaluation = ProviderEvaluationOptionsMsg::default();

    let sync = handle_run_backtest(&state, &request);
    assert!(!sync.success);
    let expected = sync.error.expect("sync economic support error");
    assert!(expected.contains("unsupported_economic_model"));
    assert!(expected.contains("instrument btcusd"));
    assert!(expected.contains("category 'crypto'"));
    assert!(!expected.contains("market data"));

    let asynchronous = handle_submit_backtest(
        &state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(!asynchronous.success);
    assert!(asynchronous.job_id.is_none());
    assert_eq!(asynchronous.error.as_deref(), Some(expected.as_str()));
    assert!(state.jobs.lock().unwrap().is_empty());

    let multi = handle_run_backtest_multi(&state, &multi_request(&request));
    assert!(!multi.success);
    assert_eq!(multi.results.len(), 1);
    assert!(!multi.results[0].success);
    assert_eq!(multi.results[0].error.as_deref(), Some(expected.as_str()));
}

#[test]
fn multi_rejects_empty_profiles_with_top_level_error() {
    let state = empty_state();
    let request = replay_request();
    let mut multi = multi_request(&request);
    multi.request.profiles.clear();

    let response = handle_run_backtest_multi(&state, &multi);
    assert!(!response.success);
    assert_eq!(
        response.error.as_deref(),
        Some("At least one profile is required.")
    );
    assert!(response.results.is_empty());

    let decoded: RunBacktestMultiResponse = serde_json::from_value(serde_json::json!({
        "results": [],
        "elapsed_ms": 1
    }))
    .expect("older multi responses remain decodable");
    assert!(decoded.success);
    assert!(decoded.error.is_none());
}

#[test]
fn invalid_evaluation_is_rejected_consistently_across_paths() {
    let state = empty_state();
    let mut request = replay_request();
    request.evaluation.sections = vec![EvaluationSectionMsg::Coverage];
    request.evaluation.breakdowns = vec![BreakdownDimensionMsg::Symbol];

    let sync_response = handle_run_backtest(&state, &request);
    assert!(!sync_response.success);
    let expected = sync_response.error.expect("sync evaluation error");
    assert!(expected.contains("breakdowns report section"));

    let async_response = handle_submit_backtest(
        &state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(!async_response.success);
    assert!(async_response.job_id.is_none());
    assert_eq!(async_response.error.as_deref(), Some(expected.as_str()));

    let multi_response = handle_run_backtest_multi(&state, &multi_request(&request));
    assert_eq!(multi_response.results.len(), 1);
    assert!(!multi_response.results[0].success);
    assert_eq!(
        multi_response.results[0].error.as_deref(),
        Some(expected.as_str())
    );
}

#[test]
fn unknown_evaluation_symbol_is_rejected_consistently_across_paths() {
    let state = empty_state();
    let mut request = replay_request();
    request.evaluation.filter.symbols = vec!["not-a-market".into()];

    let sync_response = handle_run_backtest(&state, &request);
    assert!(!sync_response.success);
    assert!(
        sync_response
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unknown evaluation symbol `not-a-market`"))
    );

    let async_response = handle_submit_backtest(
        &state,
        &SubmitBacktestRequest {
            request: request.clone(),
        },
    );
    assert!(!async_response.success);
    assert!(async_response.job_id.is_none());
    assert!(
        async_response
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unknown evaluation symbol `not-a-market`"))
    );

    let multi_response = handle_run_backtest_multi(&state, &multi_request(&request));
    assert_eq!(multi_response.results.len(), 1);
    assert!(!multi_response.results[0].success);
    assert!(
        multi_response.results[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("unknown evaluation symbol `not-a-market`"))
    );
}

#[test]
fn unknown_evaluation_selector_deserialization_is_consistent_across_paths() {
    let request = replay_request();

    let mut sync_json = serde_json::to_value(&request).unwrap();
    sync_json["evaluation"]["sections"] = serde_json::json!(["mystery"]);
    let sync_error = serde_json::from_value::<RunBacktestRequest>(sync_json)
        .expect_err("sync selector must fail")
        .to_string();

    let mut async_json = serde_json::to_value(SubmitBacktestRequest {
        request: request.clone(),
    })
    .unwrap();
    async_json["request"]["evaluation"]["sections"] = serde_json::json!(["mystery"]);
    let async_error = serde_json::from_value::<SubmitBacktestRequest>(async_json)
        .expect_err("async selector must fail")
        .to_string();

    let mut multi_json = serde_json::to_value(multi_request(&request)).unwrap();
    multi_json["evaluation"]["sections"] = serde_json::json!(["mystery"]);
    let multi_error = serde_json::from_value::<RunBacktestMultiRequest>(multi_json)
        .expect_err("multi-profile selector must fail")
        .to_string();

    assert!(sync_error.contains("unknown variant"));
    assert_eq!(async_error, sync_error);
    assert_eq!(multi_error, sync_error);
}

#[test]
fn run_backtest_multi_request_serde_roundtrip() {
    let req = BacktestMultiRunSpec {
        symbol: "xauusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "bar".into(),
        timeframe: Some("1h".into()),
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profiles: vec![
            ProfileRef::Named("conservative".into()),
            ProfileRef::Named("aggressive".into()),
        ],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: BacktestMultiRunSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.profiles.len(), 2);
    assert_eq!(decoded.data_type, "bar");
    assert_eq!(decoded.timeframe, Some("1h".into()));
}

#[test]
fn symbol_availability_serde_roundtrip() {
    let sa = SymbolAvailability {
        exchange: "icmarkets".into(),
        symbol: "EURUSD".into(),
        data_type: "tick".into(),
        timeframe: None,
        row_count: 12345,
        earliest: "2024-01-01T00:00:00".into(),
        latest: "2025-01-01T00:00:00".into(),
    };
    let json = serde_json::to_string(&sa).unwrap();
    let decoded: SymbolAvailability = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.exchange, "icmarkets");
    assert_eq!(decoded.symbol, "EURUSD");
}

#[test]
fn profile_info_serde_roundtrip() {
    let pi = ProfileInfo {
        name: "aggressive".into(),
        use_targets: vec![0, 1, 2],
        close_ratios: vec![0.5, 0.3, 0.2],
        stoploss_mode: "FixedDistance".into(),
        rules_count: 3,
        let_remainder_run: false,
    };
    let json = serde_json::to_string(&pi).unwrap();
    let decoded: ProfileInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.name, "aggressive");
    assert_eq!(decoded.rules_count, 3);
}

#[test]
fn backtest_result_msg_serde_roundtrip() {
    let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
    let msg = result_to_msg(&result);
    let json = serde_json::to_string(&msg).unwrap();
    let _decoded: BacktestResultMsg = serde_json::from_str(&json).unwrap();
}

#[test]
fn run_backtest_response_serde_roundtrip() {
    let resp = RunBacktestResponse {
        success: true,
        error: None,
        result: None,
        elapsed_ms: 42,
        artifact: None,
        inline_complete: true,
    };
    let json = serde_json::to_string(&resp).unwrap();
    let decoded: RunBacktestResponse = serde_json::from_str(&json).unwrap();
    assert!(decoded.success);
    assert_eq!(decoded.elapsed_ms, 42);

    let legacy: RunBacktestResponse = serde_json::from_value(serde_json::json!({
        "success": true,
        "error": null,
        "result": null,
        "elapsed_ms": 9
    }))
    .unwrap();
    assert!(legacy.inline_complete);
    assert!(legacy.artifact.is_none());

    let legacy_async: GetBacktestResultResponse = serde_json::from_value(serde_json::json!({
        "success": true,
        "job_id": "job-old",
        "result": null,
        "error": null
    }))
    .unwrap();
    assert!(legacy_async.inline_complete);
    assert!(legacy_async.artifact.is_none());

    let legacy_multi: RunBacktestMultiResponse = serde_json::from_value(serde_json::json!({
        "results": [],
        "elapsed_ms": 3
    }))
    .unwrap();
    assert!(legacy_multi.success);
    assert!(legacy_multi.inline_complete);
    assert!(legacy_multi.artifact.is_none());
}

#[test]
fn profile_result_serde_roundtrip() {
    let pr = ProfileResult {
        profile: "test".into(),
        success: true,
        error: None,
        result: None,
    };
    let json = serde_json::to_string(&pr).unwrap();
    let decoded: ProfileResult = serde_json::from_str(&json).unwrap();
    assert!(decoded.success);
    assert_eq!(decoded.profile, "test");
}

// ── Config ───────────────────────────────────────────────────────────────────

#[test]
fn config_msg_defaults() {
    let msg = BacktestConfigMsg {
        initial_balance: None,
        close_on_finish: None,
        fill_model: None,
        sizing: None,
    };
    let registry = qs_symbols::SymbolRegistry::empty();
    let symbols: Vec<String> = vec![];
    let cfg = config_from_msg(&msg, &registry, &symbols).unwrap();
    assert!((cfg.initial_balance - 10_000.0).abs() < f64::EPSILON);
    assert!(cfg.close_on_finish);
    assert_eq!(cfg.fill_model, FillModel::BidAsk);
}

#[test]
fn config_msg_overrides() {
    let msg = BacktestConfigMsg {
        initial_balance: Some(50_000.0),
        close_on_finish: Some(false),
        fill_model: Some("MidPrice".into()),
        sizing: None,
    };
    let registry = qs_symbols::SymbolRegistry::empty();
    let symbols: Vec<String> = vec![];
    let cfg = config_from_msg(&msg, &registry, &symbols).unwrap();
    assert!((cfg.initial_balance - 50_000.0).abs() < f64::EPSILON);
    assert!(!cfg.close_on_finish);
    assert_eq!(cfg.fill_model, FillModel::MidPrice);
}

#[test]
fn fill_model_string_parsing() {
    assert_eq!(parse_fill_model(Some("BidAsk")), FillModel::BidAsk);
    assert_eq!(parse_fill_model(Some("AskOnly")), FillModel::AskOnly);
    assert_eq!(parse_fill_model(Some("MidPrice")), FillModel::MidPrice);
    assert_eq!(parse_fill_model(Some("unknown")), FillModel::BidAsk);
    assert_eq!(parse_fill_model(None), FillModel::BidAsk);
}

#[test]
fn empty_result_converts_without_panic() {
    let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
    let msg = result_to_msg(&result);
    assert_eq!(msg.total_trades, 0);
    assert!(msg.trade_log.is_empty());
}

#[test]
fn result_msg_sanitizes_infinity_profit_factor() {
    use qs_backtest::report::SubsetStats;
    let stats = SubsetStats {
        total_trades: 2,
        winning_trades: 2,
        losing_trades: 0,
        breakeven_trades: 0,
        total_pnl: 100.0,
        gross_profit: 100.0,
        gross_loss: 0.0,
        win_rate: 1.0,
        profit_factor: f64::INFINITY,
        avg_win: 50.0,
        avg_loss: 0.0,
        win_loss_ratio: f64::INFINITY,
        expectancy: 50.0,
        largest_win: 60.0,
        largest_loss: 0.0,
    };
    let msg = result_to_msg(&BacktestResult {
        summary: stats,
        ..BacktestResult::from_trade_log(10_000.0, Vec::new())
    });
    assert!((msg.profit_factor - 0.0).abs() < f64::EPSILON);
}

#[test]
fn parse_config_toml() {
    let toml_str = r#"
[server]
shm_name = "bt-test"
shm_buffer_size = 8388608

[database]
data_dir = "/data/market"

[symbols]
registry_path = "symbols.toml"

[profiles]
profiles_path = "profiles.toml"

[jobs]
retention_secs = 120
cleanup_interval_secs = 5
max_retained_jobs = 25

[logging]
level = "debug"
"#;
    let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.server.shm_name, "bt-test");
    assert_eq!(cfg.server.shm_buffer_size, 8_388_608);
    assert_eq!(cfg.database.data_dir, "/data/market");
    assert_eq!(cfg.symbols.registry_path, "symbols.toml");
    assert_eq!(cfg.profiles.profiles_path, "profiles.toml");
    assert_eq!(cfg.jobs.retention_secs, 120);
    assert_eq!(cfg.jobs.cleanup_interval_secs, 5);
    assert_eq!(cfg.jobs.max_retained_jobs, 25);
    assert_eq!(cfg.artifacts.directory, "backtest-artifacts");
    assert_eq!(cfg.artifacts.inline_limit_bytes, 12 * 1024 * 1024);
    assert_eq!(cfg.artifacts.chunk_size, 1024 * 1024);
    assert_eq!(cfg.artifacts.retention_secs, 3_600);
    assert_eq!(cfg.artifacts.max_total_bytes, 1024 * 1024 * 1024);
    assert_eq!(cfg.logging.level, "debug");
}

#[test]
fn parse_config_defaults() {
    let toml_str = r#"
[server]
shm_name = "bt"

[database]
data_dir = "data"

[symbols]
registry_path = "sym.toml"

[profiles]
profiles_path = "prof.toml"
"#;
    let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.server.shm_buffer_size, 16 * 1024 * 1024); // default 16MB
    assert_eq!(cfg.jobs.retention_secs, 3_600);
    assert_eq!(cfg.jobs.cleanup_interval_secs, 60);
    assert_eq!(cfg.jobs.max_retained_jobs, 1_000);
    assert_eq!(cfg.artifacts.directory, "backtest-artifacts");
    assert_eq!(cfg.artifacts.inline_limit_bytes, 12 * 1024 * 1024);
    assert_eq!(cfg.artifacts.chunk_size, 1024 * 1024);
    assert_eq!(cfg.artifacts.retention_secs, 3_600);
    assert_eq!(cfg.artifacts.max_total_bytes, 1024 * 1024 * 1024);
    assert_eq!(cfg.logging.level, "info"); // default
}

#[test]
fn service_endpoint_config_supports_current_legacy_and_conflict_diagnostics() {
    fn config(server: &str) -> ServerConfig {
        toml::from_str(&format!(
            r#"
[server]
{server}

[database]
data_dir = "data"

[symbols]
registry_path = "sym.toml"

[profiles]
profiles_path = "prof.toml"
"#,
        ))
        .unwrap()
    }

    let current = config("endpoint = \"unix:///tmp/backtest.sock\"");
    assert_eq!(
        current.server.resolved_endpoint().unwrap().to_string(),
        "unix:///tmp/backtest.sock"
    );

    let legacy = config("shm_name = \"legacy-backtest\"");
    assert_eq!(
        legacy.server.resolved_endpoint().unwrap().to_string(),
        "shm://legacy-backtest"
    );

    let conflicting = config("endpoint = \"tcp://127.0.0.1:41001\"\nshm_name = \"backtest\"");
    assert!(
        conflicting
            .server
            .resolved_endpoint()
            .unwrap_err()
            .to_string()
            .contains("conflicting server.endpoint")
    );
}

// ── Handlers ─────────────────────────────────────────────────────────────────

#[test]
fn handler_ping_returns_ok() {
    let state = empty_state();
    let resp = handle_ping(&state);
    assert_eq!(resp.status, "OK");
    assert_eq!(resp.data_dir, "/tmp/test-data");
}

#[test]
fn handler_list_profiles_empty() {
    let state = empty_state();
    let resp = handle_list_profiles(&state);
    assert!(resp.profiles.is_empty());
}

#[test]
fn handler_list_profiles_with_loaded_profiles() {
    let state = empty_state();
    // Add a profile first.
    let add_req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "aggressive".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![0.5, 0.5],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    handle_add_profile(&state, &add_req);
    let resp = handle_list_profiles(&state);
    assert_eq!(resp.profiles.len(), 1);
    let agg = resp
        .profiles
        .iter()
        .find(|p| p.name == "aggressive")
        .unwrap();
    assert_eq!(agg.use_targets, vec![1, 2]);
    assert_eq!(agg.close_ratios, vec![0.5, 0.5]);
    assert_eq!(agg.rules_count, 0);
}

#[test]
fn handler_list_symbols_parses_actual_bar_timeframe_format() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!(
        "qs_backtest_server_bar_listing_{}_{}",
        std::process::id(),
        unique
    ));
    let store = ParquetStore::open(&data_dir).unwrap();
    let bars = vec![Bar {
        exchange: "fixture".into(),
        symbol: "EURUSD".into(),
        timeframe: Timeframe::M1,
        ts: fixture_ts(0),
        open: 1.1000,
        high: 1.1010,
        low: 1.0990,
        close: 1.1005,
        tick_vol: 10,
        volume: 10,
        spread: 2,
    }];
    assert_eq!(store.insert_bars(&bars).unwrap(), 1);
    let state = ServerState {
        data_dir: data_dir.to_string_lossy().into_owned(),
        ..empty_state()
    };

    let response = handle_list_symbols(
        &state,
        &ListSymbolsRequest {
            exchange: Some("fixture".into()),
            data_type: Some("bar".into()),
        },
    )
    .unwrap();
    let availability = response.symbols.first().expect("listed bar");
    assert_eq!(availability.data_type, "bar");
    assert_eq!(availability.timeframe.as_deref(), Some("1m"));

    let _ = std::fs::remove_dir_all(data_dir);
}

#[test]
fn handler_run_backtest_invalid_data_type() {
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "invalid".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.is_some());
    assert!(resp.error.unwrap().contains("Invalid data_type"));
    assert!(resp.result.is_none());
}

#[test]
fn handler_run_backtest_bar_without_timeframe() {
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "bar".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("timeframe"));
}

#[test]
fn handler_run_backtest_empty_signals() {
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: None,
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("signal"));
}

#[test]
fn handler_run_backtest_no_data_returns_error() {
    // Data dir exists but has no data for the requested symbol.
    let tmp = std::env::temp_dir().join("qs_bt_test_empty");
    std::fs::create_dir_all(&tmp).ok();

    let symbol_registry = evaluation_symbol_registry();
    let instrument_domain =
        backtest_server::InstrumentDomain::compatibility(&symbol_registry).unwrap();
    let state = ServerState {
        symbol_registry,
        instrument_domain,
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: tmp.to_string_lossy().to_string(),
        profiles_path: String::new(),
        start_time: Instant::now(),
        jobs: std::sync::Mutex::new(std::collections::HashMap::new()),
        max_retained_jobs: 1_000,
        artifact_store: test_artifact_store(tmp.join("artifacts")),
    };
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("No market data found"));

    // Cleanup.
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn handler_run_backtest_unknown_profile() {
    let state = empty_state();
    let req = BacktestMultiRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "invalid".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profiles: vec![ProfileRef::Named("nonexistent".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_multi_for_test(&state, &req);
    assert_eq!(resp.results.len(), 1);
    assert!(!resp.results[0].success);
    assert!(resp.results[0].error.is_some());
}

#[test]
fn handler_run_backtest_multi_invalid_data_type_all_fail() {
    let state = empty_state();
    let req = BacktestMultiRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "wrong".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profiles: vec![ProfileRef::Named("a".into()), ProfileRef::Named("b".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_multi_for_test(&state, &req);
    assert_eq!(resp.results.len(), 2);
    assert!(resp.results.iter().all(|r| !r.success));
}

// ── Sub-message type serde ──────────────────────────────────────────────────

#[test]
fn subset_stats_msg_serde_roundtrip() {
    let msg = SubsetStatsMsg {
        total_trades: 10,
        winning_trades: 6,
        losing_trades: 4,
        breakeven_trades: 0,
        total_pnl: 150.0,
        gross_profit: 300.0,
        gross_loss: 150.0,
        win_rate: 0.6,
        profit_factor: 2.0,
        avg_win: 50.0,
        avg_loss: 37.5,
        win_loss_ratio: 1.333,
        expectancy: 15.0,
        largest_win: 80.0,
        largest_loss: 60.0,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: SubsetStatsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.total_trades, 10);
    assert!((decoded.profit_factor - 2.0).abs() < f64::EPSILON);
}

#[test]
fn streak_stats_msg_serde_roundtrip() {
    let msg = StreakStatsMsg {
        max_consecutive_wins: 5,
        max_consecutive_losses: 3,
        current_streak: 2,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: StreakStatsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.current_streak, 2);
}

#[test]
fn risk_metrics_msg_serde_roundtrip() {
    let msg = RiskMetricsMsg {
        sharpe_ratio: Some(1.5),
        sortino_ratio: Some(2.0),
        calmar_ratio: None,
        return_on_max_drawdown: Some(3.0),
        max_drawdown: 500.0,
        max_drawdown_pct: 0.05,
        max_drawdown_duration_secs: Some(86400),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RiskMetricsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.sharpe_ratio, Some(1.5));
    assert!(decoded.calmar_ratio.is_none());
}

#[test]
fn duration_stats_msg_serde_roundtrip() {
    let msg = DurationStatsMsg {
        avg_duration_secs: 3600,
        min_duration_secs: 600,
        max_duration_secs: 7200,
        avg_winner_duration_secs: 4000,
        avg_loser_duration_secs: 3000,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: DurationStatsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.avg_duration_secs, 3600);
}

#[test]
fn monthly_return_msg_serde_roundtrip() {
    let msg = MonthlyReturnMsg {
        year: 2025,
        month: 3,
        pnl: 500.0,
        trade_count: 12,
        ending_balance: 10500.0,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: MonthlyReturnMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.pnl, 500.0);
}

#[test]
fn trade_result_msg_serde_roundtrip() {
    let msg = TradeResultMsg {
        position_id: "p1".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        entry_price: 1.0850,
        exit_price: 1.0900,
        size: 1.0,
        pnl: 50.0,
        open_ts: "2026-01-15T10:00:00".into(),
        close_ts: "2026-01-15T11:00:00".into(),
        close_reason: "Target".into(),
        group: Some("g1".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: TradeResultMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.position_id, "p1");
}

#[test]
fn equity_point_serde_roundtrip() {
    let pt = EquityPoint {
        ts: "2026-01-15T10:00:00".into(),
        balance: 10100.0,
    };
    let json = serde_json::to_string(&pt).unwrap();
    let decoded: EquityPoint = serde_json::from_str(&json).unwrap();
    assert!((decoded.balance - 10100.0).abs() < f64::EPSILON);
}

#[test]
fn position_summary_msg_serde_roundtrip() {
    let msg = PositionSummaryMsg {
        position_id: "pos_1".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        group: Some("g1".into()),
        entry_price: 1.0800,
        avg_exit_price: 1.0900,
        original_size: 1.0,
        close_count: 2,
        net_pnl: 100.0,
        close_reasons: vec!["Target".into(), "Target".into()],
        open_ts: "2026-01-15T10:00:00".into(),
        final_close_ts: Some("2026-01-15T11:00:00".into()),
        duration_seconds: 3600,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: PositionSummaryMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.net_pnl, 100.0);
}

#[test]
fn close_reason_stats_msg_serde_roundtrip() {
    let msg = CloseReasonStatsMsg {
        reason: "Target".into(),
        count: 5,
        total_pnl: 250.0,
        avg_pnl: 50.0,
        percentage: 62.5,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: CloseReasonStatsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.count, 5);
}

#[test]
fn list_symbols_request_serde_roundtrip() {
    let req = ListSymbolsRequest {
        exchange: Some("oanda".into()),
        data_type: Some("tick".into()),
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: ListSymbolsRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.exchange.unwrap(), "oanda");
}

#[test]
fn list_symbols_request_none_fields() {
    let req = ListSymbolsRequest {
        exchange: None,
        data_type: None,
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: ListSymbolsRequest = serde_json::from_str(&json).unwrap();
    assert!(decoded.exchange.is_none());
}

// Management profile conversion tests.

#[test]
fn profile_from_msg_basic() {
    let msg = ManagementProfileMsg {
        name: "test".into(),
        target_selection: None,
        use_targets: vec![1, 2],
        close_ratios: vec![0.5, 0.5],
        stoploss_mode: Some(StoplossModeMsg::FromSignal),
        rules: vec![RuleConfigDefMsg::TrailingStop { distance: 10.0 }],
        group_override: Some("grp".into()),
        let_remainder_run: true,
    };
    let p = profile_from_msg(&msg).unwrap();
    assert_eq!(p.name, "test");
    assert_eq!(p.use_targets, vec![1, 2]);
    assert_eq!(p.close_ratios, vec![0.5, 0.5]);
    assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));
    assert_eq!(p.rules.len(), 1);
    assert_eq!(p.group_override, Some("grp".into()));
    assert!(p.let_remainder_run);
}

#[test]
fn profile_from_msg_defaults() {
    let msg = ManagementProfileMsg {
        name: "minimal".into(),
        target_selection: None,
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: None,
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };
    let p = profile_from_msg(&msg).unwrap();
    assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));
    assert!(p.rules.is_empty());
    assert!(p.group_override.is_none());
    assert!(!p.let_remainder_run);
}

#[test]
fn profile_from_msg_all_stoploss_modes() {
    let msg = ManagementProfileMsg {
        name: "a".into(),
        target_selection: None,
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: Some(StoplossModeMsg::FromSignal),
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };
    let p = profile_from_msg(&msg).unwrap();
    assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));

    let msg2 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::None),
        ..msg.clone()
    };
    let p2 = profile_from_msg(&msg2).unwrap();
    assert!(matches!(p2.stoploss_mode, StoplossMode::None));

    let msg3 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::FixedDistance { distance: 50.0 }),
        ..msg.clone()
    };
    let p3 = profile_from_msg(&msg3).unwrap();
    assert!(matches!(
        p3.stoploss_mode,
        StoplossMode::FixedDistance { distance } if (distance - 50.0).abs() < f64::EPSILON
    ));

    let msg4 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::FixedPrice { price: 1.0800 }),
        ..msg.clone()
    };
    let p4 = profile_from_msg(&msg4).unwrap();
    assert!(matches!(
        p4.stoploss_mode,
        StoplossMode::FixedPrice { price } if (price - 1.0800).abs() < f64::EPSILON
    ));
}

#[test]
fn profile_from_msg_all_rule_types() {
    let rules = vec![
        RuleConfigDefMsg::FixedStoploss { price: 1.0 },
        RuleConfigDefMsg::TrailingStop { distance: 10.0 },
        RuleConfigDefMsg::TakeProfit {
            price: 2.0,
            close_ratio: 0.5,
        },
        RuleConfigDefMsg::BreakevenWhen { trigger_price: 1.5 },
        RuleConfigDefMsg::BreakevenWhenOffset {
            trigger_price_offset: 0.5,
        },
        RuleConfigDefMsg::BreakevenAfterTargets { after_n: 2 },
        RuleConfigDefMsg::TimeExit { max_seconds: 3600 },
    ];
    let msg = ManagementProfileMsg {
        name: "allrules".into(),
        target_selection: None,
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: None,
        rules,
        group_override: None,
        let_remainder_run: false,
    };
    let p = profile_from_msg(&msg).unwrap();
    assert_eq!(p.rules.len(), 7);
    assert!(matches!(p.rules[0], RuleConfigDef::FixedStoploss { .. }));
    assert!(matches!(p.rules[1], RuleConfigDef::TrailingStop { .. }));
    assert!(matches!(p.rules[2], RuleConfigDef::TakeProfit { .. }));
    assert!(matches!(p.rules[3], RuleConfigDef::BreakevenWhen { .. }));
    assert!(matches!(
        p.rules[4],
        RuleConfigDef::BreakevenWhenOffset { .. }
    ));
    assert!(matches!(
        p.rules[5],
        RuleConfigDef::BreakevenAfterTargets { .. }
    ));
    assert!(matches!(p.rules[6], RuleConfigDef::TimeExit { .. }));
}

#[test]
fn profile_to_msg_roundtrip() {
    let original = ManagementProfile {
        name: "rt".into(),
        target_selection: None,
        use_targets: vec![1, 2],
        close_ratios: vec![0.6, 0.4],
        stoploss_mode: StoplossMode::FixedDistance { distance: 25.0 },
        rules: vec![
            RuleConfigDef::TrailingStop { distance: 15.0 },
            RuleConfigDef::TimeExit { max_seconds: 7200 },
        ],
        group_override: Some("mygroup".into()),
        let_remainder_run: true,
    };
    let msg = profile_to_msg(&original);
    let back = profile_from_msg(&msg).unwrap();

    assert_eq!(back.name, original.name);
    assert_eq!(back.use_targets, original.use_targets);
    assert_eq!(back.close_ratios, original.close_ratios);
    assert!(matches!(
        back.stoploss_mode,
        StoplossMode::FixedDistance { distance } if (distance - 25.0).abs() < f64::EPSILON
    ));
    assert_eq!(back.rules.len(), 2);
    assert_eq!(back.group_override, original.group_override);
    assert_eq!(back.let_remainder_run, original.let_remainder_run);
}

#[test]
fn management_profile_msg_serde_roundtrip() {
    let msg = ManagementProfileMsg {
        name: "serde_test".into(),
        target_selection: None,
        use_targets: vec![1, 2],
        close_ratios: vec![0.6, 0.4],
        stoploss_mode: Some(StoplossModeMsg::FixedDistance { distance: 20.0 }),
        rules: vec![
            RuleConfigDefMsg::TrailingStop { distance: 10.0 },
            RuleConfigDefMsg::BreakevenAfterTargets { after_n: 1 },
        ],
        group_override: Some("ovr".into()),
        let_remainder_run: true,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: ManagementProfileMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.name, "serde_test");
    assert_eq!(decoded.rules.len(), 2);
}

#[test]
fn stoploss_mode_msg_serde_all_variants() {
    let cases = vec![
        StoplossModeMsg::FromSignal,
        StoplossModeMsg::None,
        StoplossModeMsg::FixedDistance { distance: 50.0 },
        StoplossModeMsg::FixedPrice { price: 1.0800 },
    ];
    for mode in cases {
        let json = serde_json::to_string(&mode).unwrap();
        let _decoded: StoplossModeMsg = serde_json::from_str(&json).unwrap();
    }
}

#[test]
fn rule_config_def_msg_serde_all_variants() {
    let rules = vec![
        RuleConfigDefMsg::FixedStoploss { price: 1.08 },
        RuleConfigDefMsg::TrailingStop { distance: 0.005 },
        RuleConfigDefMsg::TakeProfit {
            price: 1.10,
            close_ratio: 0.5,
        },
        RuleConfigDefMsg::BreakevenWhen {
            trigger_price: 1.09,
        },
        RuleConfigDefMsg::BreakevenWhenOffset {
            trigger_price_offset: 0.003,
        },
        RuleConfigDefMsg::BreakevenAfterTargets { after_n: 2 },
        RuleConfigDefMsg::TimeExit { max_seconds: 3600 },
    ];
    for rule in rules {
        let json = serde_json::to_string(&rule).unwrap();
        let _decoded: RuleConfigDefMsg = serde_json::from_str(&json).unwrap();
    }
}

#[test]
fn profile_ref_named_serde() {
    let pr = ProfileRef::Named("test_profile".into());
    let json = serde_json::to_string(&pr).unwrap();
    let decoded: ProfileRef = serde_json::from_str(&json).unwrap();
    assert!(matches!(decoded, ProfileRef::Named(n) if n == "test_profile"));
}

#[test]
fn profile_ref_inline_serde() {
    let msg = ManagementProfileMsg {
        name: "inline_test".into(),
        target_selection: None,
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: None,
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };
    let json = serde_json::to_string(&ProfileRef::Inline(msg)).unwrap();
    let decoded: ProfileRef = serde_json::from_str(&json).unwrap();
    match decoded {
        ProfileRef::Inline(m) => assert_eq!(m.name, "inline_test"),
        ProfileRef::Named(_) => panic!("Expected Inline variant"),
    }
}

#[test]
fn run_backtest_request_with_profile_def_serde() {
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: Some(ManagementProfileMsg {
            name: "inline".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        }),
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: BacktestRunSpec = serde_json::from_str(&json).unwrap();
    assert!(decoded.profile_def.is_some());
    assert_eq!(decoded.profile_def.unwrap().name, "inline");
    assert!(decoded.profile.is_none());
}

// Inline management profile handler tests.

#[test]
fn inline_profile_validation_error() {
    // An inline profile with mismatched targets/ratios should fail validation
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: Some(ManagementProfileMsg {
            name: "bad_inline".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![1.0], // mismatch
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        }),
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(
        resp.error
            .as_ref()
            .unwrap()
            .contains("Invalid inline profile")
    );
}

#[test]
fn backward_compat_no_profile_def() {
    // A request without profile_def (None) should still work (no regression)
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![sample_raw_signal()],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
        },
    };
    // This will fail at data loading (no data), but should NOT fail at profile validation
    let resp = run_for_test(&state, &req);
    // The error should be about data, not profiles
    if let Some(ref err) = resp.error {
        assert!(
            !err.contains("profile"),
            "Unexpected profile error: {}",
            err
        );
    }
}

#[test]
fn backward_compat_multi_string_profiles() {
    // JSON with plain string array should deserialize profiles as Named variants
    let json = r#"{
        "symbol": "eurusd",
        "exchange": "ctrader",
        "data_type": "tick",
        "raw_signals": [],
        "profiles": ["conservative", "aggressive"],
        "config": {}
    }"#;
    let decoded: BacktestMultiRunSpec = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.profiles.len(), 2);
    assert!(matches!(&decoded.profiles[0], ProfileRef::Named(n) if n == "conservative"));
    assert!(matches!(&decoded.profiles[1], ProfileRef::Named(n) if n == "aggressive"));
}

// Dynamic management profile handler tests.

#[test]
fn handler_add_profile_success() {
    let state = empty_state();
    let req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "new_prof".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    let resp = handle_add_profile(&state, &req);
    assert!(resp.success);
    assert!(resp.error.is_none());
    assert_eq!(resp.profile_count, 1);
}

#[test]
fn handler_add_profile_duplicate_rejected() {
    let state = empty_state();
    let req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "dup".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    let resp1 = handle_add_profile(&state, &req);
    assert!(resp1.success);
    let resp2 = handle_add_profile(&state, &req);
    assert!(!resp2.success);
    assert!(resp2.error.as_ref().unwrap().contains("Duplicate"));
}

#[test]
fn handler_add_profile_overwrite_success() {
    let state = empty_state();
    let req1 = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "ow".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    handle_add_profile(&state, &req1);
    let req2 = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "ow".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![0.5, 0.5],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: true,
    };
    let resp = handle_add_profile(&state, &req2);
    assert!(resp.success);
    assert_eq!(resp.profile_count, 1);
}

#[test]
fn handler_add_profile_invalid_rejected() {
    let state = empty_state();
    let req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "bad".into(),
            target_selection: None,
            use_targets: vec![1, 2],
            close_ratios: vec![1.0], // mismatch
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    let resp = handle_add_profile(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.is_some());
    assert_eq!(resp.profile_count, 0);
}

#[test]
fn handler_remove_profile_success() {
    let state = empty_state();
    let add_req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "rm_me".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: None,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        },
        overwrite: false,
    };
    handle_add_profile(&state, &add_req);
    let resp = handle_remove_profile(
        &state,
        &RemoveProfileRequest {
            name: "rm_me".into(),
        },
    );
    assert!(resp.success);
    assert!(resp.error.is_none());
    assert_eq!(resp.profile_count, 0);
}

#[test]
fn handler_remove_profile_not_found() {
    let state = empty_state();
    let resp = handle_remove_profile(
        &state,
        &RemoveProfileRequest {
            name: "nope".into(),
        },
    );
    assert!(!resp.success);
    assert!(resp.error.as_ref().unwrap().contains("not found"));
}

// RawSignalMsg and PositionRefMsg serde tests.

#[test]
fn raw_signal_msg_serde_entry() {
    let msg = RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        order_type: "Market".into(),
        price: None,
        risk: 1.0,
        stoploss: Some(1.0800),
        targets: vec![1.0900],
        group: Some("grp".into()),
        trade_id: Some("t1".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    assert!(matches!(decoded, RawSignalMsg::Entry { .. }));
}

#[test]
fn raw_signal_msg_serde_close() {
    let msg = RawSignalMsg::Close {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::ByTradeId {
            trade_id: "t1".into(),
        },
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    assert!(matches!(decoded, RawSignalMsg::Close { .. }));
}

#[test]
fn raw_signal_msg_serde_modify_stoploss() {
    let msg = RawSignalMsg::ModifyStoploss {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::ByTradeId {
            trade_id: "t1".into(),
        },
        price: 1.0850,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    assert!(matches!(decoded, RawSignalMsg::ModifyStoploss { .. }));
}

#[test]
fn raw_signal_msg_serde_all_variants() {
    let messages = vec![
        RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        },
        RawSignalMsg::Close {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
        },
        RawSignalMsg::ClosePartial {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            ratio: 0.5,
        },
        RawSignalMsg::ModifyStoploss {
            ts: "2026-01-15T10:15:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            price: 1.0850,
        },
        RawSignalMsg::MoveStoplossToEntry {
            ts: "2026-01-15T10:20:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
        },
        RawSignalMsg::AddTarget {
            ts: "2026-01-15T10:25:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            price: 1.1000,
            close_ratio: 0.3,
        },
        RawSignalMsg::RemoveTarget {
            ts: "2026-01-15T10:25:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            price: 1.0900,
        },
        RawSignalMsg::ModifyTarget {
            ts: "2026-01-15T10:26:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            old_price: 1.1000,
            new_price: 1.1010,
        },
        RawSignalMsg::AddRule {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            rule: RuleConfigDefMsg::TrailingStop { distance: 0.002 },
        },
        RawSignalMsg::RemoveRule {
            ts: "2026-01-15T10:35:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            rule_name: "TrailingStop".into(),
        },
        RawSignalMsg::ScaleIn {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
            price: Some(1.0850),
            size: 0.01,
        },
        RawSignalMsg::CancelPending {
            ts: "2026-01-15T10:30:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "abc".into(),
            },
        },
        RawSignalMsg::CloseAllOf {
            ts: "2026-01-15T11:00:00".into(),
            symbol: "eurusd".into(),
        },
        RawSignalMsg::CloseAll {
            ts: "2026-01-15T11:00:00".into(),
        },
        RawSignalMsg::CancelAllPending {
            ts: "2026-01-15T11:00:00".into(),
        },
        RawSignalMsg::ModifyAllStoploss {
            ts: "2026-01-15T11:00:00".into(),
            symbol: "eurusd".into(),
            price: 1.0800,
        },
        RawSignalMsg::CloseAllInGroup {
            ts: "2026-01-15T11:00:00".into(),
            group_id: "grp".into(),
        },
        RawSignalMsg::ModifyAllStoplossInGroup {
            ts: "2026-01-15T11:00:00".into(),
            group_id: "grp".into(),
            price: 1.0800,
        },
    ];
    for msg in &messages {
        let json = serde_json::to_string(msg).unwrap();
        let _decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    }
}

#[test]
fn position_ref_msg_serde_all_variants() {
    let refs = vec![
        PositionRefMsg::ByTradeId {
            trade_id: "abc".into(),
        },
        PositionRefMsg::AllOnSymbol {
            symbol: "eurusd".into(),
        },
        PositionRefMsg::AllInGroup {
            group_id: "grp".into(),
        },
    ];
    for r in refs {
        let json = serde_json::to_string(&r).unwrap();
        let _decoded: PositionRefMsg = serde_json::from_str(&json).unwrap();
    }
}

#[test]
fn run_backtest_request_raw_signals_serde() {
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![
            RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                risk: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: Some("grp".into()),
                trade_id: Some("t1".into()),
            },
            RawSignalMsg::CloseAllInGroup {
                ts: "2026-01-15T11:00:00".into(),
                group_id: "grp".into(),
            },
        ],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.02 }),
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: BacktestRunSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.raw_signals.len(), 2);
}

#[test]
fn raw_signal_from_msg_entry_converts() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "EURUSD".into(),
        side: "Buy".into(),
        order_type: "Market".into(),
        price: None,
        risk: 1.0,
        stoploss: Some(1.0800),
        targets: vec![1.0900],
        group: Some("grp".into()),
        trade_id: Some("t1".into()),
    };
    let result = raw_signal_from_msg(&msg, "default", &reg).unwrap();
    assert!(result.is_entry());
    assert!(result.is_entry());
    match &result {
        RawSignal::Entry {
            symbol,
            side,
            risk_multiplier,
            stoploss,
            group,
            ..
        } => {
            assert_eq!(symbol, "eurusd");
            assert_eq!(*side, Side::Buy);
            assert_eq!(*risk_multiplier, 1.0);
            assert_eq!(*stoploss, Some(1.0800));
            assert_eq!(*group, Some("grp".into()));
        }
        _ => panic!("expected Entry"),
    }
}

#[test]
fn raw_signal_from_msg_close_converts() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Close {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::ByTradeId {
            trade_id: "grp1-trade-1".into(),
        },
    };
    let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
    match result {
        RawSignal::Close { position, .. } => {
            assert!(
                matches!(position, PositionRef::ByTradeId { trade_id } if trade_id == "grp1-trade-1")
            );
        }
        _ => panic!("Expected Close variant"),
    }
}

#[test]
fn raw_signal_from_msg_add_rule_converts() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::AddRule {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::AllOnSymbol {
            symbol: "eurusd".into(),
        },
        rule: RuleConfigDefMsg::TrailingStop { distance: 0.0020 },
    };
    let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
    match result {
        RawSignal::AddRule { rule, position, .. } => {
            assert!(
                matches!(rule, RuleConfigDef::TrailingStop { distance } if (distance - 0.0020).abs() < f64::EPSILON)
            );
            assert!(matches!(position, PositionRef::AllOnSymbol { symbol } if symbol == "eurusd"));
        }
        _ => panic!("Expected AddRule variant"),
    }
}

#[test]
fn raw_signal_from_msg_empty_symbol_uses_default() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "".into(),
        side: "Sell".into(),
        order_type: "Limit".into(),
        price: Some(1.0900),
        risk: 1.0,
        stoploss: None,
        targets: vec![],
        group: None,
        trade_id: None,
    };
    let result = raw_signal_from_msg(&msg, "xauusd", &reg).unwrap();
    assert!(result.is_entry());
    match &result {
        RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "xauusd"),
        _ => panic!("expected Entry"),
    }
}

#[test]
fn position_ref_from_msg_all_variants_convert() {
    let reg = SymbolRegistry::empty();

    let by_trade_id = position_ref_from_msg(
        &PositionRefMsg::ByTradeId {
            trade_id: "abc".into(),
        },
        &reg,
    );
    assert!(matches!(by_trade_id, PositionRef::ByTradeId { trade_id } if trade_id == "abc"));

    let all_sym = position_ref_from_msg(
        &PositionRefMsg::AllOnSymbol {
            symbol: "xauusd".into(),
        },
        &reg,
    );
    assert!(matches!(all_sym, PositionRef::AllOnSymbol { symbol } if symbol == "xauusd"));

    let all_grp = position_ref_from_msg(
        &PositionRefMsg::AllInGroup {
            group_id: "scalp".into(),
        },
        &reg,
    );
    assert!(matches!(all_grp, PositionRef::AllInGroup { group_id } if group_id == "scalp"));
}

#[test]
fn raw_signal_from_msg_bulk_variants_convert() {
    let reg = SymbolRegistry::empty();

    let close_all = raw_signal_from_msg(
        &RawSignalMsg::CloseAll {
            ts: "2026-01-15T11:00:00".into(),
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(matches!(close_all, RawSignal::CloseAll { .. }));

    let close_all_of = raw_signal_from_msg(
        &RawSignalMsg::CloseAllOf {
            ts: "2026-01-15T11:00:00".into(),
            symbol: "eurusd".into(),
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(matches!(close_all_of, RawSignal::CloseAllOf { .. }));

    let cancel_all = raw_signal_from_msg(
        &RawSignalMsg::CancelAllPending {
            ts: "2026-01-15T11:00:00".into(),
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(matches!(cancel_all, RawSignal::CancelAllPending { .. }));

    let modify_all_sl = raw_signal_from_msg(
        &RawSignalMsg::ModifyAllStoploss {
            ts: "2026-01-15T11:00:00".into(),
            symbol: "eurusd".into(),
            price: 1.0750,
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(
        matches!(modify_all_sl, RawSignal::ModifyAllStoploss { price, .. } if (price - 1.0750).abs() < f64::EPSILON)
    );

    let close_grp = raw_signal_from_msg(
        &RawSignalMsg::CloseAllInGroup {
            ts: "2026-01-15T11:00:00".into(),
            group_id: "g1".into(),
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(matches!(close_grp, RawSignal::CloseAllInGroup { group_id, .. } if group_id == "g1"));

    let modify_grp_sl = raw_signal_from_msg(
        &RawSignalMsg::ModifyAllStoplossInGroup {
            ts: "2026-01-15T11:00:00".into(),
            group_id: "g1".into(),
            price: 1.0800,
        },
        "eurusd",
        &reg,
    )
    .unwrap();
    assert!(matches!(
        modify_grp_sl,
        RawSignal::ModifyAllStoplossInGroup { .. }
    ));
}

#[test]
fn raw_signal_from_msg_invalid_side_errors() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "eurusd".into(),
        side: "INVALID".into(),
        order_type: "Market".into(),
        price: None,
        risk: 1.0,
        stoploss: None,
        targets: vec![],
        group: None,
        trade_id: None,
    };
    assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
}

#[test]
fn raw_signal_from_msg_invalid_timestamp_errors() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Close {
        ts: "not-a-date".into(),
        position: PositionRefMsg::ByTradeId {
            trade_id: "abc".into(),
        },
    };
    assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
}

#[test]
fn run_backtest_multi_request_raw_signals_serde() {
    let req = BacktestMultiRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        }],
        profiles: vec![ProfileRef::Named("test".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.02 }),
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: BacktestMultiRunSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.raw_signals.len(), 1);
}

#[test]
fn handler_run_backtest_empty_raw_signals_rejected() {
    let state = empty_state();
    let req = BacktestRunSpec {
        symbol: "eurusd".into(),
        symbols: Vec::new(),
        all_symbols: false,
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
            sizing: None,
        },
    };
    let resp = run_for_test(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.as_ref().unwrap().contains("signal"));
}

#[test]
fn raw_signal_msg_full_workflow_json() {
    // Test a realistic full workflow JSON: open, modify SL, partial close, close group
    let json = r#"[
        {
            "action": "Entry",
            "ts": "2026-01-15T10:00:00",
            "symbol": "eurusd",
            "side": "Buy",
            "order_type": "Market",
            "risk": 1.0,
            "stoploss": 1.0800,
            "targets": [1.0880, 1.0920],
            "group": "momentum_v1"
        },
        {
            "action": "ModifyStoploss",
            "ts": "2026-01-15T10:15:00",
            "position": { "type": "ByTradeId", "trade_id": "momentum-v1-buy-1" },
            "price": 1.0850
        },
        {
            "action": "ClosePartial",
            "ts": "2026-01-15T10:30:00",
            "position": { "type": "AllInGroup", "group_id": "momentum_v1" },
            "ratio": 0.5
        },
        {
            "action": "AddRule",
            "ts": "2026-01-15T10:30:00",
            "position": { "type": "AllInGroup", "group_id": "momentum_v1" },
            "rule": { "type": "TrailingStop", "distance": 0.0020 }
        },
        {
            "action": "CloseAllInGroup",
            "ts": "2026-01-15T11:00:00",
            "group_id": "momentum_v1"
        }
    ]"#;
    let decoded: Vec<RawSignalMsg> = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.len(), 5);
    assert!(matches!(&decoded[0], RawSignalMsg::Entry { .. }));
    assert!(matches!(&decoded[1], RawSignalMsg::ModifyStoploss { .. }));
    assert!(matches!(&decoded[2], RawSignalMsg::ClosePartial { .. }));
    assert!(matches!(&decoded[3], RawSignalMsg::AddRule { .. }));
    assert!(matches!(&decoded[4], RawSignalMsg::CloseAllInGroup { .. }));
}
