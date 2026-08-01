use qs_core::RawSignal;
use serde_json::{Value, json};

fn entry(risk: Value) -> Value {
    json!({
        "action": "Entry",
        "ts": "2026-01-15T10:00:00",
        "symbol": "EURUSD",
        "side": "Buy",
        "order_type": "Market",
        "price": null,
        "risk": risk,
        "stoploss": null,
        "targets": [],
        "group": null,
        "trade_id": null
    })
}

fn position() -> Value {
    json!({
        "type": "ByTradeId",
        "trade_id": "trade-1"
    })
}

#[test]
fn entry_risk_is_required_and_must_be_a_positive_number() {
    let signal: RawSignal = serde_json::from_value(entry(json!(1.0))).unwrap();
    assert!(matches!(
        signal,
        RawSignal::Entry {
            risk_multiplier: 1.0,
            ..
        }
    ));

    let mut missing = entry(json!(1.0));
    missing.as_object_mut().unwrap().remove("risk");
    assert!(serde_json::from_value::<RawSignal>(missing).is_err());

    for risk in [json!(0.0), json!(-1.0), json!(null), json!("1.0")] {
        assert!(
            serde_json::from_value::<RawSignal>(entry(risk)).is_err(),
            "invalid risk was accepted"
        );
    }

    let non_finite = r#"{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1e400,"stoploss":null}"#;
    assert!(serde_json::from_str::<RawSignal>(non_finite).is_err());
}

#[test]
fn action_fields_are_strict_while_nested_compatibility_is_retained() {
    for signal in [
        json!({
            "action": "CloseAll",
            "ts": "2026-01-15T10:00:00",
            "unexpected": true
        }),
        json!({
            "action": "Entry",
            "ts": "2026-01-15T10:00:00",
            "symbol": "EURUSD",
            "side": "Buy",
            "order_type": "Market",
            "price": null,
            "risk": 1.0,
            "size": 0.1,
            "stoploss": null
        }),
    ] {
        let error = serde_json::from_value::<RawSignal>(signal).unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
    }

    let close: RawSignal = serde_json::from_value(json!({
        "action": "Close",
        "ts": "2026-01-15T10:01:00",
        "position": {
            "type": "ByTradeId",
            "trade_id": "trade-1",
            "future_position_field": true
        }
    }))
    .unwrap();
    assert!(matches!(close, RawSignal::Close { .. }));

    let add_rule: RawSignal = serde_json::from_value(json!({
        "action": "AddRule",
        "ts": "2026-01-15T10:02:00",
        "position": position(),
        "rule": {
            "type": "TrailingStop",
            "distance": 0.001,
            "future_rule_field": true
        }
    }))
    .unwrap();
    assert!(matches!(add_rule, RawSignal::AddRule { .. }));
}

#[test]
fn current_management_action_shapes_remain_deserializable() {
    let signals = vec![
        json!({"action":"Close","ts":"2026-01-15T10:01:00","position":position()}),
        json!({"action":"ClosePartial","ts":"2026-01-15T10:02:00","position":position(),"ratio":0.5}),
        json!({"action":"ModifyStoploss","ts":"2026-01-15T10:03:00","position":position(),"price":1.08}),
        json!({"action":"MoveStoplossToEntry","ts":"2026-01-15T10:04:00","position":position()}),
        json!({"action":"AddTarget","ts":"2026-01-15T10:05:00","position":position(),"price":1.10,"close_ratio":0.5}),
        json!({"action":"RemoveTarget","ts":"2026-01-15T10:06:00","position":position(),"price":1.10}),
        json!({"action":"ModifyTarget","ts":"2026-01-15T10:07:00","position":position(),"old_price":1.10,"new_price":1.11}),
        json!({"action":"AddRule","ts":"2026-01-15T10:08:00","position":position(),"rule":{"type":"TimeExit","max_seconds":3600}}),
        json!({"action":"RemoveRule","ts":"2026-01-15T10:09:00","position":position(),"rule_name":"time_exit"}),
        json!({"action":"ScaleIn","ts":"2026-01-15T10:10:00","position":position(),"price":null,"size":0.25}),
        json!({"action":"CancelPending","ts":"2026-01-15T10:11:00","position":position()}),
        json!({"action":"CloseAllOf","ts":"2026-01-15T10:12:00","symbol":"EURUSD"}),
        json!({"action":"CloseAll","ts":"2026-01-15T10:13:00"}),
        json!({"action":"CancelAllPending","ts":"2026-01-15T10:14:00"}),
        json!({"action":"ModifyAllStoploss","ts":"2026-01-15T10:15:00","symbol":"EURUSD","price":1.08}),
        json!({"action":"CloseAllInGroup","ts":"2026-01-15T10:16:00","group_id":"group-1"}),
        json!({"action":"ModifyAllStoplossInGroup","ts":"2026-01-15T10:17:00","group_id":"group-1","price":1.08}),
    ];

    let parsed = signals
        .into_iter()
        .map(serde_json::from_value::<RawSignal>)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(parsed.len(), 17);
    assert!(matches!(parsed[9], RawSignal::ScaleIn { size: 0.25, .. }));
}
