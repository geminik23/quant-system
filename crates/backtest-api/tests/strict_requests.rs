use qs_backtest_api::{
    BacktestConfigMsg, BacktestRunSpec, BacktestTimestampError, FutureQuoteConfigMsg,
    PositionRefMsg, ProviderEvaluationOptionsMsg, RawSignalDecodeError, RawSignalMsg,
    ResultDeliveryMsg, RunBacktestRequest, canonical_backtest_timestamp,
    decode_raw_signal_json_strict, parse_backtest_timestamp,
};
use serde_json::{Value, json};

fn entry() -> Value {
    json!({
        "action": "Entry",
        "ts": "2026-01-15T10:00:00",
        "symbol": "EURUSD",
        "side": "Buy",
        "order_type": "Market",
        "price": null,
        "risk": 1.0,
        "stoploss": null,
        "targets": [],
        "group": null,
        "trade_id": "trade-1"
    })
}

fn close() -> Value {
    json!({
        "action": "Close",
        "ts": "2026-01-15T10:01:00",
        "position": {
            "type": "ByTradeId",
            "trade_id": "trade-1"
        }
    })
}

fn add_rule() -> Value {
    json!({
        "action": "AddRule",
        "ts": "2026-01-15T10:02:00",
        "position": {
            "type": "ByTradeId",
            "trade_id": "trade-1"
        },
        "rule": {
            "type": "TrailingStop",
            "distance": 0.001
        }
    })
}

fn request_with_signal(signal: Value) -> Value {
    json!({
        "request": {
            "symbol": "EURUSD",
            "exchange": "fixture",
            "data_type": "tick",
            "raw_signals": [signal],
            "profile": null,
            "config": {
                "initial_balance": null,
                "close_on_finish": null,
                "fill_model": null
            }
        }
    })
}

#[test]
fn standalone_raw_signal_messages_keep_the_compatibility_boundary() {
    let mut unknown_action_field = close();
    unknown_action_field["unexpected"] = json!(true);
    assert!(serde_json::from_value::<RawSignalMsg>(unknown_action_field).is_err());

    let mut obsolete_entry_size = entry();
    obsolete_entry_size["size"] = json!(0.1);
    assert!(serde_json::from_value::<RawSignalMsg>(obsolete_entry_size).is_err());

    let mut nested_position_field = close();
    nested_position_field["position"]["future_position_field"] = json!(true);
    let close: RawSignalMsg = serde_json::from_value(nested_position_field).unwrap();
    assert!(matches!(close, RawSignalMsg::Close { .. }));

    let mut nested_rule_field = add_rule();
    nested_rule_field["rule"]["future_rule_field"] = json!(true);
    let add_rule: RawSignalMsg = serde_json::from_value(nested_rule_field).unwrap();
    assert!(matches!(add_rule, RawSignalMsg::AddRule { .. }));
}

#[test]
fn strict_standalone_decode_reuses_recursive_request_strictness() {
    let decoded = decode_raw_signal_json_strict(&entry().to_string()).unwrap();
    assert!(matches!(decoded, RawSignalMsg::Entry { risk: 1.0, .. }));

    let mut nested_position_field = close();
    nested_position_field["position"]["future_position_field"] = json!(true);
    assert!(
        serde_json::from_value::<RawSignalMsg>(nested_position_field.clone()).is_ok(),
        "compatibility decoding must remain unchanged"
    );
    assert!(decode_raw_signal_json_strict(&nested_position_field.to_string()).is_err());

    let mut nested_rule_field = add_rule();
    nested_rule_field["rule"]["future_rule_field"] = json!(true);
    assert!(
        serde_json::from_value::<RawSignalMsg>(nested_rule_field.clone()).is_ok(),
        "compatibility decoding must remain unchanged"
    );
    assert!(decode_raw_signal_json_strict(&nested_rule_field.to_string()).is_err());
}

#[test]
fn strict_standalone_decode_enforces_entry_risk_without_rejecting_scale_in_size() {
    let mut missing_risk = entry();
    missing_risk.as_object_mut().unwrap().remove("risk");
    assert!(decode_raw_signal_json_strict(&missing_risk.to_string()).is_err());

    for risk in [0.0, -1.0] {
        let mut invalid_risk = entry();
        invalid_risk["risk"] = json!(risk);
        assert!(matches!(
            decode_raw_signal_json_strict(&invalid_risk.to_string()),
            Err(RawSignalDecodeError::InvalidEntryRisk)
        ));
    }

    let mut obsolete_size = entry();
    obsolete_size["size"] = json!(0.1);
    assert!(decode_raw_signal_json_strict(&obsolete_size.to_string()).is_err());

    let scale_in = json!({
        "action": "ScaleIn",
        "ts": "2026-01-15T10:01:00",
        "position": {
            "type": "ByTradeId",
            "trade_id": "trade-1"
        },
        "price": null,
        "size": 0.25
    });
    assert!(matches!(
        decode_raw_signal_json_strict(&scale_in.to_string()).unwrap(),
        RawSignalMsg::ScaleIn { size: 0.25, .. }
    ));

    let two_values = format!("{} {}", entry(), close());
    assert!(decode_raw_signal_json_strict(&two_values).is_err());
}

#[test]
fn public_timestamp_contract_normalizes_utc_without_local_timezone() {
    assert_eq!(
        canonical_backtest_timestamp("2026-01-01T00:30:00+02:00").unwrap(),
        "2025-12-31T22:30:00"
    );
    assert_eq!(
        canonical_backtest_timestamp("2026-01-01 19:51:04.120000").unwrap(),
        "2026-01-01T19:51:04.12"
    );
    assert_eq!(
        canonical_backtest_timestamp("2026-01-01").unwrap(),
        "2026-01-01T00:00:00"
    );
    assert_eq!(
        parse_backtest_timestamp("2026-01-01T19:51:04-00:00"),
        Err(BacktestTimestampError::UnknownLocalOffset)
    );
}

#[test]
fn wrapped_run_requests_reject_unknown_fields_recursively() {
    let mut cases = Vec::new();

    let mut root = request_with_signal(entry());
    root["unexpected"] = json!(true);
    cases.push(("request wrapper", root));

    let mut run_spec = request_with_signal(entry());
    run_spec["request"]["unexpected"] = json!(true);
    cases.push(("run spec", run_spec));

    let mut config = request_with_signal(entry());
    config["request"]["config"]["unexpected"] = json!(true);
    cases.push(("config", config));

    let mut sizing = request_with_signal(entry());
    sizing["request"]["config"]["sizing"] = json!({
        "type": "FixedLot",
        "lots": 0.1,
        "unexpected": true
    });
    cases.push(("sizing", sizing));

    let mut action = request_with_signal(entry());
    action["request"]["raw_signals"][0]["unexpected"] = json!(true);
    cases.push(("raw signal", action));

    let mut position = request_with_signal(close());
    position["request"]["raw_signals"][0]["position"]["unexpected"] = json!(true);
    cases.push(("position reference", position));

    let mut rule = request_with_signal(add_rule());
    rule["request"]["raw_signals"][0]["rule"]["unexpected"] = json!(true);
    cases.push(("rule definition", rule));

    let mut future = request_with_signal(entry());
    future["future"] = json!({"unexpected": true});
    cases.push(("future config", future));

    let mut evaluation = request_with_signal(entry());
    evaluation["evaluation"] = json!({
        "context": {
            "provider_id": null,
            "source_id": null,
            "unexpected": true
        }
    });
    cases.push(("evaluation context", evaluation));

    for (boundary, value) in cases {
        assert!(
            serde_json::from_value::<RunBacktestRequest>(value).is_err(),
            "unknown field was accepted in {boundary}"
        );
    }
}

#[test]
fn run_requests_support_parser_free_direct_construction() {
    let request = RunBacktestRequest {
        request: BacktestRunSpec {
            symbol: "EURUSD".into(),
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
                    price: None,
                    risk: 1.0,
                    stoploss: None,
                    targets: Vec::new(),
                    group: None,
                    trade_id: Some("trade-1".into()),
                },
                RawSignalMsg::ScaleIn {
                    ts: "2026-01-15T10:01:00".into(),
                    position: PositionRefMsg::ByTradeId {
                        trade_id: "trade-1".into(),
                    },
                    price: None,
                    size: 0.25,
                },
            ],
            profile: None,
            profile_def: None,
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: None,
                sizing: None,
            },
        },
        future: FutureQuoteConfigMsg::default(),
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    };

    let decoded: RunBacktestRequest =
        serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
    assert_eq!(decoded.request.raw_signals.len(), 2);
    assert!(matches!(
        decoded.request.raw_signals[1],
        RawSignalMsg::ScaleIn { size: 0.25, .. }
    ));
}
