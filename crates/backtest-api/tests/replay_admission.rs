use qs_backtest_api::{
    BacktestMultiRunSpec, BacktestResultMsg, BacktestRunSpec, ExcludedInstrumentMsg,
    ExcludedSignalRefMsg, InstrumentExclusionReasonMsg, ReplayAdmissionReportMsg,
    RunBacktestMultiRequest, RunBacktestRequest, SubmitBacktestRequest,
    UnavailableInstrumentPolicyMsg,
};
use serde_json::{Value, json};

fn request_json() -> Value {
    json!({
        "request": {
            "symbol": "EURUSD",
            "exchange": "fixture",
            "data_type": "tick",
            "raw_signals": [],
            "config": {}
        }
    })
}

fn multi_request_json(mut value: Value) -> Value {
    let fields = value["request"].as_object_mut().unwrap();
    fields.remove("profile");
    fields.remove("profile_def");
    fields.insert("profiles".into(), json!([]));
    value
}

#[test]
fn legacy_requests_default_to_skip_at_every_raw_replay_boundary() {
    assert_eq!(
        UnavailableInstrumentPolicyMsg::default(),
        UnavailableInstrumentPolicyMsg::Skip
    );

    let value = request_json();
    let spec: BacktestRunSpec = serde_json::from_value(value["request"].clone()).unwrap();
    let request: RunBacktestRequest = serde_json::from_value(value.clone()).unwrap();
    let submit: SubmitBacktestRequest =
        serde_json::from_value(json!({ "request": value })).unwrap();
    assert_eq!(spec.on_unavailable, UnavailableInstrumentPolicyMsg::Skip);
    assert_eq!(
        request.request.on_unavailable,
        UnavailableInstrumentPolicyMsg::Skip
    );
    assert_eq!(
        submit.request.request.on_unavailable,
        UnavailableInstrumentPolicyMsg::Skip
    );

    let value = multi_request_json(request_json());
    let spec: BacktestMultiRunSpec = serde_json::from_value(value["request"].clone()).unwrap();
    let request: RunBacktestMultiRequest = serde_json::from_value(value).unwrap();
    assert_eq!(spec.on_unavailable, UnavailableInstrumentPolicyMsg::Skip);
    assert_eq!(
        request.request.on_unavailable,
        UnavailableInstrumentPolicyMsg::Skip
    );
}

#[test]
fn unavailable_policies_roundtrip_through_strict_requests() {
    for (wire, expected) in [
        ("skip", UnavailableInstrumentPolicyMsg::Skip),
        ("error", UnavailableInstrumentPolicyMsg::Error),
    ] {
        assert_eq!(serde_json::to_value(expected).unwrap(), json!(wire));
        let policy: UnavailableInstrumentPolicyMsg = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(policy, expected);

        let mut value = request_json();
        value["request"]["on_unavailable"] = json!(wire);
        let request: RunBacktestRequest = serde_json::from_value(value.clone()).unwrap();
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["request"]["on_unavailable"], json!(wire));
        let decoded: RunBacktestRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.request.on_unavailable, expected);

        let submit: SubmitBacktestRequest =
            serde_json::from_value(json!({ "request": value.clone() })).unwrap();
        let encoded = serde_json::to_value(&submit).unwrap();
        assert_eq!(encoded["request"]["request"]["on_unavailable"], json!(wire));
        let decoded: SubmitBacktestRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.request.request.on_unavailable, expected);

        let multi: RunBacktestMultiRequest =
            serde_json::from_value(multi_request_json(value)).unwrap();
        let encoded = serde_json::to_value(&multi).unwrap();
        assert_eq!(encoded["request"]["on_unavailable"], json!(wire));
        let decoded: RunBacktestMultiRequest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.request.on_unavailable, expected);
    }
}

#[test]
fn invalid_unavailable_policies_are_rejected() {
    for policy in [
        json!("Skip"),
        json!("Error"),
        json!("ignore"),
        json!(null),
        json!(true),
        json!({ "skip": {} }),
    ] {
        assert!(serde_json::from_value::<UnavailableInstrumentPolicyMsg>(policy.clone()).is_err());
        let mut value = request_json();
        value["request"]["on_unavailable"] = policy;
        assert!(serde_json::from_value::<BacktestRunSpec>(value["request"].clone()).is_err());
        assert!(serde_json::from_value::<RunBacktestRequest>(value.clone()).is_err());
        assert!(
            serde_json::from_value::<SubmitBacktestRequest>(json!({ "request": value.clone() }))
                .is_err()
        );
        let multi = multi_request_json(value);
        assert!(serde_json::from_value::<BacktestMultiRunSpec>(multi["request"].clone()).is_err());
        assert!(serde_json::from_value::<RunBacktestMultiRequest>(multi).is_err());
    }
}

#[test]
fn explicit_policies_preserve_recursive_request_strictness() {
    for policy in ["skip", "error"] {
        let mut value = request_json();
        value["request"]["on_unavailable"] = json!(policy);
        value["request"]["raw_signals"] = json!([{
            "action": "AddRule",
            "ts": "2026-01-15T10:02:00",
            "position": { "type": "ByTradeId", "trade_id": "trade-1" },
            "rule": { "type": "TrailingStop", "distance": 0.001 }
        }]);
        value["request"]["entry_profile_routes"] = json!([{
            "entry_class": "example",
            "profile": { "name": "example", "use_targets": [], "close_ratios": [] }
        }]);
        value["future"] = json!({});
        value["evaluation"] = json!({});
        let multi = multi_request_json(value.clone());
        assert!(serde_json::from_value::<RunBacktestRequest>(value.clone()).is_ok());
        assert!(serde_json::from_value::<RunBacktestMultiRequest>(multi.clone()).is_ok());

        for pointer in [
            "",
            "/request",
            "/request/config",
            "/request/raw_signals/0",
            "/request/raw_signals/0/position",
            "/request/raw_signals/0/rule",
            "/request/entry_profile_routes/0",
            "/request/entry_profile_routes/0/profile",
            "/future",
            "/evaluation",
        ] {
            let inject_unknown = |mut value: Value| {
                value
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert("unexpected".into(), json!(true));
                value
            };
            let invalid = inject_unknown(value.clone());
            assert!(
                serde_json::from_value::<RunBacktestRequest>(invalid.clone()).is_err(),
                "single request accepted an unknown field at {pointer} with {policy}"
            );
            assert!(
                serde_json::from_value::<SubmitBacktestRequest>(json!({ "request": invalid }))
                    .is_err(),
                "submission accepted an unknown field at {pointer} with {policy}"
            );
            assert!(
                serde_json::from_value::<RunBacktestMultiRequest>(inject_unknown(multi.clone()))
                    .is_err(),
                "multi request accepted an unknown field at {pointer} with {policy}"
            );
        }
    }
}

fn legacy_result_json() -> Value {
    let stats = json!({
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "breakeven_trades": 0,
        "total_pnl": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "win_loss_ratio": 0.0,
        "expectancy": 0.0,
        "largest_win": 0.0,
        "largest_loss": 0.0
    });
    json!({
        "initial_balance": 10000.0,
        "final_balance": 10000.0,
        "total_pnl": 0.0,
        "total_trades": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "max_drawdown": 0.0,
        "max_drawdown_pct": 0.0,
        "summary": stats.clone(),
        "per_symbol": {},
        "per_group": {},
        "long_stats": stats.clone(),
        "short_stats": stats,
        "per_close_reason": [],
        "streaks": {
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0,
            "current_streak": 0
        },
        "risk_metrics": { "max_drawdown": 0.0, "max_drawdown_pct": 0.0 },
        "monthly_returns": [],
        "equity_curve": [],
        "trade_log": [],
        "positions": [],
        "total_positions": 0,
        "winning_positions": 0,
        "losing_positions": 0,
        "position_win_rate": 0.0
    })
}

#[test]
fn legacy_empty_results_default_to_no_admission_report() {
    let result: BacktestResultMsg = serde_json::from_value(legacy_result_json()).unwrap();
    assert_eq!(result.admission_report, None);
    assert!(
        serde_json::to_value(result)
            .unwrap()
            .get("admission_report")
            .is_none()
    );

    let mut value = legacy_result_json();
    value["admission_report"] = json!(null);
    let result: BacktestResultMsg = serde_json::from_value(value).unwrap();
    assert_eq!(result.admission_report, None);
}

fn admission_report() -> ReplayAdmissionReportMsg {
    ReplayAdmissionReportMsg {
        omitted_instruments: 0,
        omitted_entries: 0,
        omitted_management: 0,
        policy: UnavailableInstrumentPolicyMsg::Skip,
        input_signals: 12,
        retained_signals: 3,
        excluded_instruments: vec![ExcludedInstrumentMsg {
            symbol: "UNAVAILABLE".into(),
            reason: InstrumentExclusionReasonMsg::NoMarketData,
            details: "No quotes in the requested window".into(),
            skipped_entries: 4,
            skipped_management: 5,
            signal_references: vec![
                ExcludedSignalRefMsg {
                    input_index: 0,
                    ts: "2026-01-15T10:00:00".into(),
                    action: "Entry".into(),
                    trade_id: Some("trade-1".into()),
                },
                ExcludedSignalRefMsg {
                    input_index: 2,
                    ts: "2026-01-15T10:01:00".into(),
                    action: "CloseAllOf".into(),
                    trade_id: None,
                },
            ],
            omitted_signal_references: 7,
        }],
    }
}

#[test]
fn admission_reports_roundtrip_standalone_and_in_results() {
    let report = admission_report();
    let encoded = serde_json::to_value(&report).unwrap();
    assert_eq!(encoded["policy"], json!("skip"));
    assert_eq!(
        encoded["excluded_instruments"][0]["reason"],
        json!("no_market_data")
    );
    let decoded: ReplayAdmissionReportMsg = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, report);

    let mut result: BacktestResultMsg = serde_json::from_value(legacy_result_json()).unwrap();
    result.admission_report = Some(report.clone());
    let encoded = serde_json::to_string(&result).unwrap();
    let decoded: BacktestResultMsg = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded.admission_report, Some(report));
}

#[test]
fn exclusion_reasons_use_snake_case_and_reject_unknown_values() {
    for (wire, reason) in [
        ("not_selected", InstrumentExclusionReasonMsg::NotSelected),
        (
            "unknown_instrument",
            InstrumentExclusionReasonMsg::UnknownInstrument,
        ),
        (
            "unsupported_economics",
            InstrumentExclusionReasonMsg::UnsupportedEconomics,
        ),
        ("no_market_data", InstrumentExclusionReasonMsg::NoMarketData),
        (
            "no_conversion_data",
            InstrumentExclusionReasonMsg::NoConversionData,
        ),
        (
            "ambiguous_mapping",
            InstrumentExclusionReasonMsg::AmbiguousMapping,
        ),
    ] {
        assert_eq!(serde_json::to_value(reason).unwrap(), json!(wire));
        let decoded: InstrumentExclusionReasonMsg = serde_json::from_value(json!(wire)).unwrap();
        assert_eq!(decoded, reason);
    }
    for wire in ["NoMarketData", "unknown"] {
        assert!(serde_json::from_value::<InstrumentExclusionReasonMsg>(json!(wire)).is_err());
    }
}
