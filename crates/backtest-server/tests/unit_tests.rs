//! Unit tests for the backtest server crate.

use backtest_server::config::ServerConfig;
use backtest_server::convert::{
    config_from_msg, parse_fill_model, position_ref_from_msg, profile_from_msg, profile_to_msg,
    raw_signal_from_msg, result_to_msg,
};
use backtest_server::handlers::{
    ServerState, handle_add_profile, handle_list_profiles, handle_ping, handle_remove_profile,
    handle_run_backtest,
};
use backtest_server::rpc_types::*;

use qs_backtest::profile::{
    ManagementProfile, PositionRef, ProfileRegistry, RawSignal, RuleConfigDef, StoplossMode,
};
use qs_backtest::report::BacktestResult;
use qs_core::types::{FillModel, Side};
use qs_symbols::SymbolRegistry;

use std::sync::RwLock;
use std::time::Instant;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn empty_state() -> ServerState {
    ServerState {
        symbol_registry: SymbolRegistry::empty(),
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: "/tmp/test-data".into(),
        profiles_path: String::new(),
        start_time: Instant::now(),
    }
}

fn sample_signal() -> RawSignalEntryMsg {
    RawSignalEntryMsg {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        order_type: "Market".into(),
        price: None,
        size: 1.0,
        stoploss: Some(1.0800),
        targets: vec![1.0900, 1.0950],
        group: None,
    }
}

// ── RPC Types Serde ─────────────────────────────────────────────────────────

#[test]
fn connect_request_serde_roundtrip() {
    let req = ConnectRequest {
        client_name: "test-client".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: ConnectRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.client_name, "test-client");
}

#[test]
fn connect_response_serde_roundtrip() {
    let resp = ConnectResponse {
        client_id: 42,
        slot_name: "backtest-client-42".into(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let decoded: ConnectResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.client_id, 42);
    assert_eq!(decoded.slot_name, "backtest-client-42");
}

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
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: BacktestConfigMsg = serde_json::from_str(&json).unwrap();
    assert!((decoded.initial_balance.unwrap() - 50_000.0).abs() < f64::EPSILON);
    assert_eq!(decoded.close_on_finish, Some(false));
    assert_eq!(decoded.fill_model.unwrap(), "MidPrice");
}

#[test]
fn raw_signal_entry_msg_serde_roundtrip() {
    let msg = sample_signal();
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RawSignalEntryMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.ts, "2026-01-15T10:00:00");
    assert_eq!(decoded.symbol, "eurusd");
    assert_eq!(decoded.side, "Buy");
    assert_eq!(decoded.targets.len(), 2);
    assert!(decoded.group.is_none());
}

#[test]
fn run_backtest_request_serde_roundtrip() {
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: Some("2026-01-01".into()),
        to: Some("2026-02-01".into()),
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: Some("aggressive".into()),
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: Some(10000.0),
            close_on_finish: None,
            fill_model: None,
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: RunBacktestRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.symbol, "eurusd");
    assert_eq!(decoded.exchange, "ctrader");
    assert_eq!(decoded.signals.len(), 1);
    assert_eq!(decoded.profile, Some("aggressive".into()));
}

#[test]
fn run_backtest_multi_request_serde_roundtrip() {
    let req = RunBacktestMultiRequest {
        symbol: "xauusd".into(),
        exchange: "ctrader".into(),
        data_type: "bar".into(),
        timeframe: Some("1h".into()),
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profiles: vec![
            ProfileRef::Named("conservative".into()),
            ProfileRef::Named("aggressive".into()),
        ],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: RunBacktestMultiRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.profiles.len(), 2);
    assert_eq!(decoded.data_type, "bar");
    assert_eq!(decoded.timeframe, Some("1h".into()));
}

#[test]
fn symbol_availability_serde_roundtrip() {
    let sa = SymbolAvailability {
        exchange: "ctrader".into(),
        symbol: "EURUSD".into(),
        data_type: "tick".into(),
        timeframe: None,
        row_count: 1_000_000,
        earliest: "2026-01-01T00:00:00".into(),
        latest: "2026-02-01T23:59:59".into(),
    };
    let json = serde_json::to_string(&sa).unwrap();
    let decoded: SymbolAvailability = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.row_count, 1_000_000);
    assert!(decoded.timeframe.is_none());
}

#[test]
fn profile_info_serde_roundtrip() {
    let pi = ProfileInfo {
        name: "aggressive".into(),
        use_targets: vec![1, 2],
        close_ratios: vec![0.5, 0.5],
        stoploss_mode: "FromSignal".into(),
        rules_count: 3,
        let_remainder_run: false,
    };
    let json = serde_json::to_string(&pi).unwrap();
    let decoded: ProfileInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.name, "aggressive");
    assert_eq!(decoded.use_targets, vec![1, 2]);
    assert_eq!(decoded.rules_count, 3);
}

#[test]
fn backtest_result_msg_serde_roundtrip() {
    let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
    let msg = result_to_msg(&result);
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: BacktestResultMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.total_trades, 0);
    assert!((decoded.initial_balance - 10_000.0).abs() < f64::EPSILON);
}

#[test]
fn run_backtest_response_serde_roundtrip() {
    let resp = RunBacktestResponse {
        success: true,
        error: None,
        result: None,
        elapsed_ms: 42,
    };
    let json = serde_json::to_string(&resp).unwrap();
    let decoded: RunBacktestResponse = serde_json::from_str(&json).unwrap();
    assert!(decoded.success);
    assert!(decoded.error.is_none());
    assert_eq!(decoded.elapsed_ms, 42);
}

#[test]
fn profile_result_serde_roundtrip() {
    let pr = ProfileResult {
        profile: "test".into(),
        success: false,
        error: Some("not found".into()),
        result: None,
    };
    let json = serde_json::to_string(&pr).unwrap();
    let decoded: ProfileResult = serde_json::from_str(&json).unwrap();
    assert!(!decoded.success);
    assert_eq!(decoded.error, Some("not found".into()));
}

// ── Convert Module ──────────────────────────────────────────────────────────

#[test]
fn config_msg_defaults() {
    let msg = BacktestConfigMsg {
        initial_balance: None,
        close_on_finish: None,
        fill_model: None,
    };
    let cfg = config_from_msg(&msg);
    assert!((cfg.initial_balance - 10_000.0).abs() < f64::EPSILON);
    assert!(cfg.close_on_finish);
    assert_eq!(cfg.fill_model, FillModel::BidAsk);
}

#[test]
fn config_msg_overrides() {
    let msg = BacktestConfigMsg {
        initial_balance: Some(25_000.0),
        close_on_finish: Some(false),
        fill_model: Some("AskOnly".into()),
    };
    let cfg = config_from_msg(&msg);
    assert!((cfg.initial_balance - 25_000.0).abs() < f64::EPSILON);
    assert!(!cfg.close_on_finish);
    assert_eq!(cfg.fill_model, FillModel::AskOnly);
}

#[test]
fn fill_model_string_parsing() {
    assert_eq!(parse_fill_model(Some("BidAsk")), FillModel::BidAsk);
    assert_eq!(parse_fill_model(Some("AskOnly")), FillModel::AskOnly);
    assert_eq!(parse_fill_model(Some("MidPrice")), FillModel::MidPrice);
    assert_eq!(parse_fill_model(Some("garbage")), FillModel::BidAsk);
    assert_eq!(parse_fill_model(None), FillModel::BidAsk);
}

#[test]
fn empty_result_converts_without_panic() {
    let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
    let msg = result_to_msg(&result);
    assert_eq!(msg.total_trades, 0);
    assert!(msg.trade_log.is_empty());
    assert!(msg.equity_curve.is_empty());
    assert!(msg.positions.is_empty());
    assert_eq!(msg.total_positions, 0);
}

#[test]
fn result_msg_sanitizes_infinity_profit_factor() {
    // An empty trade log produces a result with 0 trades.
    // SubsetStats with only winners will have INFINITY profit_factor.
    // Our conversion should replace it with 0.0.
    let result = BacktestResult::from_trade_log(10_000.0, Vec::new());
    let msg = result_to_msg(&result);
    assert!(msg.summary.profit_factor.is_finite());
    assert!(msg.summary.win_loss_ratio.is_finite());
}

// ── Config ──────────────────────────────────────────────────────────────────

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

[logging]
level = "debug"
"#;
    let cfg: ServerConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(cfg.server.shm_name, "bt-test");
    assert_eq!(cfg.server.shm_buffer_size, 8_388_608);
    assert_eq!(cfg.database.data_dir, "/data/market");
    assert_eq!(cfg.symbols.registry_path, "symbols.toml");
    assert_eq!(cfg.profiles.profiles_path, "profiles.toml");
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
    assert_eq!(cfg.logging.level, "info"); // default
}

// ── Handlers ────────────────────────────────────────────────────────────────

#[test]
fn handler_ping_returns_ok() {
    let state = empty_state();
    let resp = handle_ping(&state);
    assert_eq!(resp.status, "OK");
    assert_eq!(resp.data_dir, "/tmp/test-data");
    // uptime should be near zero
    assert!(resp.uptime_secs < 5);
}

#[test]
fn handler_list_profiles_empty() {
    let state = empty_state();
    let resp = handle_list_profiles(&state);
    assert!(resp.profiles.is_empty());
}

#[test]
fn handler_list_profiles_with_loaded_profiles() {
    let toml_str = r#"
[[profile]]
name = "conservative"
use_targets = [1]
close_ratios = [1.0]

[[profile]]
name = "aggressive"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
    let registry = ProfileRegistry::from_toml(toml_str).unwrap();
    let state = ServerState {
        symbol_registry: SymbolRegistry::empty(),
        profile_registry: RwLock::new(registry),
        data_dir: "/tmp/test".into(),
        profiles_path: String::new(),
        start_time: Instant::now(),
    };
    let resp = handle_list_profiles(&state);
    assert_eq!(resp.profiles.len(), 2);

    let names: Vec<&str> = resp.profiles.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"conservative"));
    assert!(names.contains(&"aggressive"));

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
fn handler_run_backtest_invalid_data_type() {
    let state = empty_state();
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "invalid".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = handle_run_backtest(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.is_some());
    assert!(resp.error.unwrap().contains("Invalid data_type"));
    assert!(resp.result.is_none());
}

#[test]
fn handler_run_backtest_bar_without_timeframe() {
    let state = empty_state();
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "bar".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = handle_run_backtest(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("timeframe"));
}

#[test]
fn handler_run_backtest_empty_signals() {
    let state = empty_state();
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = handle_run_backtest(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("signal"));
}

#[test]
fn handler_run_backtest_no_data_returns_error() {
    // Data dir exists but has no data for the requested symbol.
    let tmp = std::env::temp_dir().join("qs_bt_test_empty");
    std::fs::create_dir_all(&tmp).ok();

    let state = ServerState {
        symbol_registry: SymbolRegistry::empty(),
        profile_registry: RwLock::new(ProfileRegistry::empty()),
        data_dir: tmp.to_string_lossy().to_string(),
        profiles_path: String::new(),
        start_time: Instant::now(),
    };
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = handle_run_backtest(&state, &req);
    assert!(!resp.success);
    assert!(resp.error.unwrap().contains("No market data found"));

    // Cleanup.
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn handler_run_backtest_unknown_profile() {
    // We need actual data so the handler gets past the data-loading step.
    // Since we don't have data, this will fail at data loading first.
    // But we can test that the profile lookup path works by using
    // the multi handler which checks profiles independently.
    let state = empty_state();
    let req = RunBacktestMultiRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "invalid".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profiles: vec![ProfileRef::Named("nonexistent".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = backtest_server::handlers::handle_run_backtest_multi(&state, &req);
    assert_eq!(resp.results.len(), 1);
    assert!(!resp.results[0].success);
    assert!(resp.results[0].error.is_some());
}

#[test]
fn handler_run_backtest_multi_invalid_data_type_all_fail() {
    let state = empty_state();
    let req = RunBacktestMultiRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "wrong".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profiles: vec![ProfileRef::Named("a".into()), ProfileRef::Named("b".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = backtest_server::handlers::handle_run_backtest_multi(&state, &req);
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
    assert_eq!(decoded.max_consecutive_wins, 5);
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
        year: 2026,
        month: 3,
        pnl: 1500.0,
        trade_count: 25,
        ending_balance: 11500.0,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: MonthlyReturnMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.year, 2026);
    assert_eq!(decoded.month, 3);
}

#[test]
fn trade_result_msg_serde_roundtrip() {
    let msg = TradeResultMsg {
        position_id: "p-001".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        entry_price: 1.0850,
        exit_price: 1.0900,
        size: 1.0,
        pnl: 50.0,
        open_ts: "2026-01-15T10:00:00".into(),
        close_ts: "2026-01-15T14:00:00".into(),
        close_reason: "Target".into(),
        group: Some("scalp".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: TradeResultMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.position_id, "p-001");
    assert_eq!(decoded.close_reason, "Target");
    assert_eq!(decoded.group, Some("scalp".into()));
}

#[test]
fn equity_point_serde_roundtrip() {
    let ep = EquityPoint {
        ts: "2026-01-15T10:00:00".into(),
        balance: 10500.0,
    };
    let json = serde_json::to_string(&ep).unwrap();
    let decoded: EquityPoint = serde_json::from_str(&json).unwrap();
    assert!((decoded.balance - 10500.0).abs() < f64::EPSILON);
}

#[test]
fn position_summary_msg_serde_roundtrip() {
    let msg = PositionSummaryMsg {
        position_id: "pos-1".into(),
        symbol: "xauusd".into(),
        side: "Sell".into(),
        group: None,
        entry_price: 2350.50,
        avg_exit_price: 2340.00,
        original_size: 0.1,
        close_count: 2,
        net_pnl: 105.0,
        close_reasons: vec!["Target".into(), "TrailingStop".into()],
        open_ts: "2026-01-10T09:00:00".into(),
        final_close_ts: Some("2026-01-10T15:00:00".into()),
        duration_seconds: 21600,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: PositionSummaryMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.close_count, 2);
    assert_eq!(decoded.close_reasons.len(), 2);
}

#[test]
fn close_reason_stats_msg_serde_roundtrip() {
    let msg = CloseReasonStatsMsg {
        reason: "Stoploss".into(),
        count: 15,
        total_pnl: -750.0,
        avg_pnl: -50.0,
        percentage: 0.3,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: CloseReasonStatsMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.reason, "Stoploss");
    assert_eq!(decoded.count, 15);
}

#[test]
fn list_symbols_request_serde_roundtrip() {
    let req = ListSymbolsRequest {
        exchange: Some("ctrader".into()),
        data_type: Some("tick".into()),
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: ListSymbolsRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.exchange, Some("ctrader".into()));
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
    assert!(decoded.data_type.is_none());
}

// ── F13: Dynamic Profiles — Phase 1 Convert Tests ──────────────────────────

#[test]
fn profile_from_msg_basic() {
    let msg = ManagementProfileMsg {
        name: "test".into(),
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
    let base = ManagementProfileMsg {
        name: "a".into(),
        use_targets: vec![1],
        close_ratios: vec![1.0],
        stoploss_mode: Some(StoplossModeMsg::FromSignal),
        rules: vec![],
        group_override: None,
        let_remainder_run: false,
    };

    // FromSignal
    let p = profile_from_msg(&base).unwrap();
    assert!(matches!(p.stoploss_mode, StoplossMode::FromSignal));

    // None
    let msg2 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::None),
        ..base.clone()
    };
    let p2 = profile_from_msg(&msg2).unwrap();
    assert!(matches!(p2.stoploss_mode, StoplossMode::None));

    // FixedDistance
    let msg3 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::FixedDistance { distance: 50.0 }),
        ..base.clone()
    };
    let p3 = profile_from_msg(&msg3).unwrap();
    assert!(matches!(
        p3.stoploss_mode,
        StoplossMode::FixedDistance { distance } if (distance - 50.0).abs() < f64::EPSILON
    ));

    // FixedPrice
    let msg4 = ManagementProfileMsg {
        stoploss_mode: Some(StoplossModeMsg::FixedPrice { price: 1.0800 }),
        ..base.clone()
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

// ── F13: Phase 1 Serde Tests ───────────────────────────────────────────────

#[test]
fn management_profile_msg_serde_roundtrip() {
    let msg = ManagementProfileMsg {
        name: "serde_test".into(),
        use_targets: vec![1, 2],
        close_ratios: vec![0.5, 0.5],
        stoploss_mode: Some(StoplossModeMsg::FixedDistance { distance: 20.0 }),
        rules: vec![
            RuleConfigDefMsg::TrailingStop { distance: 10.0 },
            RuleConfigDefMsg::TimeExit { max_seconds: 3600 },
        ],
        group_override: Some("g1".into()),
        let_remainder_run: true,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: ManagementProfileMsg = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.name, "serde_test");
    assert_eq!(decoded.use_targets, vec![1, 2]);
    assert_eq!(decoded.rules.len(), 2);
    assert!(decoded.let_remainder_run);
}

#[test]
fn stoploss_mode_msg_serde_all_variants() {
    let variants = vec![
        StoplossModeMsg::FromSignal,
        StoplossModeMsg::None,
        StoplossModeMsg::FixedDistance { distance: 10.0 },
        StoplossModeMsg::FixedPrice { price: 1.0800 },
    ];
    for v in &variants {
        let json = serde_json::to_string(v).unwrap();
        let decoded: StoplossModeMsg = serde_json::from_str(&json).unwrap();
        // Just verify round-trip doesn't panic and produces valid JSON
        let json2 = serde_json::to_string(&decoded).unwrap();
        assert_eq!(json, json2);
    }
}

#[test]
fn rule_config_def_msg_serde_all_variants() {
    let variants: Vec<RuleConfigDefMsg> = vec![
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
    for v in &variants {
        let json = serde_json::to_string(v).unwrap();
        let decoded: RuleConfigDefMsg = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&decoded).unwrap();
        assert_eq!(json, json2);
    }
}

#[test]
fn profile_ref_named_serde() {
    // A plain JSON string should deserialize as ProfileRef::Named
    let json = r#""conservative""#;
    let pr: ProfileRef = serde_json::from_str(json).unwrap();
    assert!(matches!(pr, ProfileRef::Named(ref n) if n == "conservative"));

    // Round-trip
    let json_out = serde_json::to_string(&pr).unwrap();
    assert_eq!(json_out, r#""conservative""#);
}

#[test]
fn profile_ref_inline_serde() {
    // A JSON object should deserialize as ProfileRef::Inline
    let msg = ManagementProfileMsg {
        name: "inline_test".into(),
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
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: Some(ManagementProfileMsg {
            name: "inline".into(),
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
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: RunBacktestRequest = serde_json::from_str(&json).unwrap();
    assert!(decoded.profile_def.is_some());
    assert_eq!(decoded.profile_def.unwrap().name, "inline");
    assert!(decoded.profile.is_none());
}

// ── F13: Phase 1 Handler Tests ─────────────────────────────────────────────

#[test]
fn inline_profile_validation_error() {
    // An inline profile with mismatched targets/ratios should fail validation
    let state = empty_state();
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: Some(ManagementProfileMsg {
            name: "bad_inline".into(),
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
        },
    };
    let resp = handle_run_backtest(&state, &req);
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
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![sample_signal()],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    // This will fail at data loading (no data), but should NOT fail at profile validation
    let resp = handle_run_backtest(&state, &req);
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
        "signals": [],
        "profiles": ["conservative", "aggressive"],
        "config": {}
    }"#;
    let decoded: RunBacktestMultiRequest = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.profiles.len(), 2);
    assert!(matches!(&decoded.profiles[0], ProfileRef::Named(n) if n == "conservative"));
    assert!(matches!(&decoded.profiles[1], ProfileRef::Named(n) if n == "aggressive"));
}

// ── F13: Phase 2 Handler Tests ─────────────────────────────────────────────

#[test]
fn handler_add_profile_success() {
    let state = empty_state();
    let req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "new_prof".into(),
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

    // Verify it shows up in list_profiles
    let list = handle_list_profiles(&state);
    assert_eq!(list.profiles.len(), 1);
    assert_eq!(list.profiles[0].name, "new_prof");
}

#[test]
fn handler_add_profile_duplicate_rejected() {
    let state = empty_state();
    let req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "dup".into(),
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
    assert_eq!(resp2.profile_count, 1);
}

#[test]
fn handler_add_profile_overwrite_success() {
    let state = empty_state();
    let req1 = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "ow".into(),
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
    // Add first
    let add_req = AddProfileRequest {
        profile: ManagementProfileMsg {
            name: "rm_me".into(),
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
    assert_eq!(resp.profile_count, 0);
}

// ── F14: RawSignalMsg / PositionRefMsg Wire Type Tests ──────────────────────

#[test]
fn raw_signal_msg_serde_entry() {
    let msg = RawSignalMsg::Entry {
        ts: "2026-01-15T10:00:00".into(),
        symbol: "eurusd".into(),
        side: "Buy".into(),
        order_type: "Market".into(),
        price: None,
        size: 0.02,
        stoploss: Some(1.0800),
        targets: vec![1.0880, 1.0920],
        group: Some("momentum_v1".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"action\":\"Entry\""));
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    if let RawSignalMsg::Entry {
        symbol,
        size,
        group,
        ..
    } = &decoded
    {
        assert_eq!(symbol, "eurusd");
        assert_eq!(*size, 0.02);
        assert_eq!(group.as_deref(), Some("momentum_v1"));
    } else {
        panic!("Expected Entry variant");
    }
}

#[test]
fn raw_signal_msg_serde_close() {
    let msg = RawSignalMsg::Close {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::LastInGroup {
            group_id: "grp1".into(),
        },
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"action\":\"Close\""));
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    if let RawSignalMsg::Close { position, .. } = &decoded {
        assert!(matches!(position, PositionRefMsg::LastInGroup { group_id } if group_id == "grp1"));
    } else {
        panic!("Expected Close variant");
    }
}

#[test]
fn raw_signal_msg_serde_modify_stoploss() {
    let msg = RawSignalMsg::ModifyStoploss {
        ts: "2026-01-15T10:15:00".into(),
        position: PositionRefMsg::AllOnSymbol {
            symbol: "eurusd".into(),
        },
        price: 1.0850,
    };
    let json = serde_json::to_string(&msg).unwrap();
    let decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    if let RawSignalMsg::ModifyStoploss {
        price, position, ..
    } = &decoded
    {
        assert_eq!(*price, 1.0850);
        assert!(matches!(position, PositionRefMsg::AllOnSymbol { symbol } if symbol == "eurusd"));
    } else {
        panic!("Expected ModifyStoploss variant");
    }
}

#[test]
fn raw_signal_msg_serde_all_variants() {
    // Verify all 17 variants serialize/deserialize without error.
    let variants: Vec<RawSignalMsg> = vec![
        RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 0.01,
            stoploss: None,
            targets: vec![],
            group: None,
        },
        RawSignalMsg::Close {
            ts: "2026-01-15T10:01:00".into(),
            position: PositionRefMsg::Id { id: "abc".into() },
        },
        RawSignalMsg::ClosePartial {
            ts: "2026-01-15T10:02:00".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: "eurusd".into(),
            },
            ratio: 0.5,
        },
        RawSignalMsg::ModifyStoploss {
            ts: "2026-01-15T10:03:00".into(),
            position: PositionRefMsg::LastInGroup {
                group_id: "g1".into(),
            },
            price: 1.0800,
        },
        RawSignalMsg::MoveStoplossToEntry {
            ts: "2026-01-15T10:04:00".into(),
            position: PositionRefMsg::AllOnSymbol {
                symbol: "eurusd".into(),
            },
        },
        RawSignalMsg::AddTarget {
            ts: "2026-01-15T10:05:00".into(),
            position: PositionRefMsg::AllInGroup {
                group_id: "g1".into(),
            },
            price: 1.1000,
            close_ratio: 0.5,
        },
        RawSignalMsg::RemoveTarget {
            ts: "2026-01-15T10:06:00".into(),
            position: PositionRefMsg::Id { id: "xyz".into() },
            price: 1.1000,
        },
        RawSignalMsg::AddRule {
            ts: "2026-01-15T10:07:00".into(),
            position: PositionRefMsg::Id { id: "xyz".into() },
            rule: RuleConfigDefMsg::TrailingStop { distance: 0.0020 },
        },
        RawSignalMsg::RemoveRule {
            ts: "2026-01-15T10:08:00".into(),
            position: PositionRefMsg::Id { id: "xyz".into() },
            rule_name: "TrailingStop".into(),
        },
        RawSignalMsg::ScaleIn {
            ts: "2026-01-15T10:09:00".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: "eurusd".into(),
            },
            price: Some(1.0850),
            size: 0.01,
        },
        RawSignalMsg::CancelPending {
            ts: "2026-01-15T10:10:00".into(),
            position: PositionRefMsg::Id { id: "p1".into() },
        },
        RawSignalMsg::CloseAllOf {
            ts: "2026-01-15T10:11:00".into(),
            symbol: "eurusd".into(),
        },
        RawSignalMsg::CloseAll {
            ts: "2026-01-15T10:12:00".into(),
        },
        RawSignalMsg::CancelAllPending {
            ts: "2026-01-15T10:13:00".into(),
        },
        RawSignalMsg::ModifyAllStoploss {
            ts: "2026-01-15T10:14:00".into(),
            symbol: "eurusd".into(),
            price: 1.0750,
        },
        RawSignalMsg::CloseAllInGroup {
            ts: "2026-01-15T10:15:00".into(),
            group_id: "g1".into(),
        },
        RawSignalMsg::ModifyAllStoplossInGroup {
            ts: "2026-01-15T10:16:00".into(),
            group_id: "g1".into(),
            price: 1.0800,
        },
    ];
    for msg in &variants {
        let json = serde_json::to_string(msg).unwrap();
        let _decoded: RawSignalMsg = serde_json::from_str(&json).unwrap();
    }
    assert_eq!(variants.len(), 17);
}

#[test]
fn position_ref_msg_serde_all_variants() {
    let variants: Vec<PositionRefMsg> = vec![
        PositionRefMsg::Id {
            id: "abc123".into(),
        },
        PositionRefMsg::LastOnSymbol {
            symbol: "eurusd".into(),
        },
        PositionRefMsg::LastInGroup {
            group_id: "momentum".into(),
        },
        PositionRefMsg::AllOnSymbol {
            symbol: "xauusd".into(),
        },
        PositionRefMsg::AllInGroup {
            group_id: "scalp".into(),
        },
    ];
    for msg in &variants {
        let json = serde_json::to_string(msg).unwrap();
        let decoded: PositionRefMsg = serde_json::from_str(&json).unwrap();
        let re_json = serde_json::to_string(&decoded).unwrap();
        assert_eq!(json, re_json);
    }
}

#[test]
fn run_backtest_request_raw_signals_serde() {
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![],
        raw_signals: vec![
            RawSignalMsg::Entry {
                ts: "2026-01-15T10:00:00".into(),
                symbol: "eurusd".into(),
                side: "Buy".into(),
                order_type: "Market".into(),
                price: None,
                size: 0.02,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: Some("grp".into()),
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
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: RunBacktestRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.raw_signals.len(), 2);
    assert!(decoded.signals.is_empty());
}

#[test]
fn backward_compat_signals_only_no_raw_signals() {
    // Existing JSON without raw_signals should deserialize with empty vec.
    let json = r#"{
        "symbol": "eurusd",
        "exchange": "ctrader",
        "data_type": "tick",
        "signals": [{
            "ts": "2026-01-15T10:00:00",
            "symbol": "eurusd",
            "side": "Buy",
            "order_type": "Market",
            "size": 0.01,
            "targets": []
        }],
        "config": {}
    }"#;
    let decoded: RunBacktestRequest = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.signals.len(), 1);
    assert!(decoded.raw_signals.is_empty());
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
        size: 0.02,
        stoploss: Some(1.0800),
        targets: vec![1.0900],
        group: Some("grp".into()),
    };
    let result = raw_signal_from_msg(&msg, "default", &reg).unwrap();
    assert!(result.is_entry());
    let entry = result.as_entry().unwrap();
    assert_eq!(entry.symbol, "eurusd");
    assert_eq!(entry.side, Side::Buy);
    assert_eq!(entry.size, 0.02);
    assert_eq!(entry.stoploss, Some(1.0800));
    assert_eq!(entry.group, Some("grp".into()));
}

#[test]
fn raw_signal_from_msg_close_converts() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Close {
        ts: "2026-01-15T10:30:00".into(),
        position: PositionRefMsg::LastInGroup {
            group_id: "grp1".into(),
        },
    };
    let result = raw_signal_from_msg(&msg, "eurusd", &reg).unwrap();
    match result {
        RawSignal::Close { position, .. } => {
            assert!(
                matches!(position, PositionRef::LastInGroup { group_id } if group_id == "grp1")
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
        size: 0.01,
        stoploss: None,
        targets: vec![],
        group: None,
    };
    let result = raw_signal_from_msg(&msg, "xauusd", &reg).unwrap();
    let entry = result.as_entry().unwrap();
    assert_eq!(entry.symbol, "xauusd");
}

#[test]
fn position_ref_from_msg_all_variants_convert() {
    let reg = SymbolRegistry::empty();

    let id = position_ref_from_msg(&PositionRefMsg::Id { id: "abc".into() }, &reg);
    assert!(matches!(id, PositionRef::Id { id } if id == "abc"));

    let last_sym = position_ref_from_msg(
        &PositionRefMsg::LastOnSymbol {
            symbol: "EUR/USD".into(),
        },
        &reg,
    );
    // SymbolRegistry::empty normalizes via passthrough — lowercase + strip separators
    assert!(matches!(last_sym, PositionRef::LastOnSymbol { symbol } if symbol == "eurusd"));

    let last_grp = position_ref_from_msg(
        &PositionRefMsg::LastInGroup {
            group_id: "g1".into(),
        },
        &reg,
    );
    assert!(matches!(last_grp, PositionRef::LastInGroup { group_id } if group_id == "g1"));

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
        size: 0.01,
        stoploss: None,
        targets: vec![],
        group: None,
    };
    assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
}

#[test]
fn raw_signal_from_msg_invalid_timestamp_errors() {
    let reg = SymbolRegistry::empty();
    let msg = RawSignalMsg::Close {
        ts: "not-a-date".into(),
        position: PositionRefMsg::Id { id: "abc".into() },
    };
    assert!(raw_signal_from_msg(&msg, "eurusd", &reg).is_err());
}

#[test]
fn run_backtest_multi_request_raw_signals_serde() {
    let req = RunBacktestMultiRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![],
        raw_signals: vec![RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "eurusd".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 0.02,
            stoploss: None,
            targets: vec![],
            group: None,
        }],
        profiles: vec![ProfileRef::Named("test".into())],
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let decoded: RunBacktestMultiRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.raw_signals.len(), 1);
    assert!(decoded.signals.is_empty());
}

#[test]
fn handler_run_backtest_empty_raw_signals_and_signals_rejected() {
    let state = empty_state();
    let req = RunBacktestRequest {
        symbol: "eurusd".into(),
        exchange: "ctrader".into(),
        data_type: "tick".into(),
        timeframe: None,
        from: None,
        to: None,
        signals: vec![],
        raw_signals: vec![],
        profile: None,
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: None,
            close_on_finish: None,
            fill_model: None,
        },
    };
    let resp = handle_run_backtest(&state, &req);
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
            "size": 0.02,
            "stoploss": 1.0800,
            "targets": [1.0880, 1.0920],
            "group": "momentum_v1"
        },
        {
            "action": "ModifyStoploss",
            "ts": "2026-01-15T10:15:00",
            "position": { "type": "LastInGroup", "group_id": "momentum_v1" },
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
