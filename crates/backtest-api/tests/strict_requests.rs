use qs_backtest_api::{
    BacktestConfigMsg, BacktestRunSpec, CommissionModelMsg, EntryProfileRouteMsg,
    FutureQuoteConfigMsg, PositionRefMsg, ProfileRef, ProviderEvaluationOptionsMsg, RawSignalMsg,
    ResultDeliveryMsg, RunBacktestRequest,
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

    let mut costs = request_with_signal(entry());
    costs["request"]["config"]["costs"] = json!({
        "EURUSD": {
            "commission": {"type": "PerLotPerSide", "amount": 3.5, "currency": "USD"},
            "unexpected": true
        }
    });
    cases.push(("instrument costs", costs));

    let mut commission = request_with_signal(entry());
    commission["request"]["config"]["costs"] = json!({
        "EURUSD": {
            "commission": {
                "type": "NotionalRatePerSide",
                "buy_rate": 0.005,
                "sell_rate": 0.005,
                "unexpected": true
            }
        }
    });
    cases.push(("commission model", commission));

    let mut swap = request_with_signal(entry());
    swap["request"]["config"]["costs"] = json!({
        "EURUSD": {
            "swap": {
                "amount": {"unit": "Points", "long": -6.1, "short": 1.9},
                "rollover": "22:00:00",
                "triple_weekday": "Wed",
                "unexpected": true
            }
        }
    });
    cases.push(("swap schedule", swap));

    let mut swap_amount = request_with_signal(entry());
    swap_amount["request"]["config"]["costs"] = json!({
        "EURUSD": {
            "swap": {
                "amount": {"unit": "Points", "long": -6.1, "short": 1.9, "unexpected": true},
                "rollover": "22:00:00",
                "triple_weekday": "Wed"
            }
        }
    });
    cases.push(("swap amount", swap_amount));

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
                    entry_class: Some("expanded".into()),
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
            entry_profile_routes: vec![EntryProfileRouteMsg {
                entry_class: "expanded".into(),
                profile: ProfileRef::Named("demo".into()),
            }],
            config: BacktestConfigMsg {
                initial_balance: Some(10_000.0),
                close_on_finish: Some(true),
                fill_model: None,
                sizing: None,
                costs: Default::default(),
            },
        },
        future: FutureQuoteConfigMsg::default(),
        evaluation: ProviderEvaluationOptionsMsg::default(),
        result_delivery: ResultDeliveryMsg::Auto,
    };

    let decoded: RunBacktestRequest =
        serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
    assert_eq!(decoded.request.raw_signals.len(), 2);
    assert_eq!(decoded.request.entry_profile_routes.len(), 1);
    assert!(matches!(
        decoded.request.raw_signals[1],
        RawSignalMsg::ScaleIn { size: 0.25, .. }
    ));
}

#[test]
fn run_requests_carry_per_symbol_costs() {
    let mut value = request_with_signal(entry());
    value["request"]["config"]["costs"] = json!({
        "EURUSD": {
            "commission": {"type": "PerLotPerSide", "amount": 3.5, "currency": "USD"},
            "swap": {
                "amount": {"unit": "Points", "long": -6.1, "short": 1.9},
                "rollover": "22:00:00",
                "triple_weekday": "Wed"
            }
        },
        "BTCUSD": {
            "commission": {"type": "NotionalRatePerSide", "buy_rate": 0.005, "sell_rate": 0.005}
        }
    });

    let decoded: RunBacktestRequest = serde_json::from_value(value).unwrap();
    let costs = &decoded.request.config.costs;
    assert_eq!(costs.len(), 2);
    assert!(matches!(
        costs["EURUSD"].commission,
        Some(CommissionModelMsg::PerLotPerSide { amount, .. }) if amount == 3.5
    ));
    let swap = costs["EURUSD"].swap.as_ref().unwrap();
    assert_eq!(swap.rollover, "22:00:00");
    assert_eq!(swap.triple_weekday, "Wed");
    assert!(swap.skipped_weekdays.is_empty());
    assert!(matches!(
        costs["BTCUSD"].commission,
        Some(CommissionModelMsg::NotionalRatePerSide { buy_rate, sell_rate })
            if buy_rate == 0.005 && sell_rate == 0.005
    ));
    assert!(costs["BTCUSD"].swap.is_none());
}

#[test]
fn a_request_without_costs_stays_absent_on_the_wire() {
    let value = request_with_signal(entry());
    let decoded: RunBacktestRequest = serde_json::from_value(value).unwrap();
    assert!(decoded.request.config.costs.is_empty());
    let encoded = serde_json::to_value(&decoded).unwrap();
    assert!(encoded["request"]["config"].get("costs").is_none());
}
