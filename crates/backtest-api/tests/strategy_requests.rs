//! Configured strategy, portfolio, and search requests decode strictly at every level except the strategy documents and portfolio policies, which the service decodes itself.

use qs_backtest_api::{
    RunConfiguredStrategyRequest, RunPortfolioRequest, SubmitConfiguredStrategyRequest,
    SubmitPortfolioRequest, SubmitSearchRequest,
};
use serde_json::{Value, json};

fn configured() -> Value {
    json!({
        "request": {
            "symbol": "EURUSD",
            "exchange": "fixture",
            "data_type": "tick",
            "strategy": {
                "document": { "strategy_id": "alpha", "any": ["shape", 1] },
                "sources": [
                    { "source": "primary", "timeframe_seconds": 60, "price_basis": "mid" }
                ]
            },
            "entry_profile_routes": [{ "entry_class": "trend", "profile": "trail" }],
            "config": { "initial_balance": 10000.0, "sizing": { "type": "FixedLot", "lots": 0.1 } }
        },
        "future": { "account_currency": "USD" }
    })
}

fn search() -> Value {
    json!({
        "request": {
            "template": { "strategy_id": "alpha" },
            "space": { "family_id": "alpha" },
            "symbols": ["EURUSD"],
            "exchange": "fixture",
            "data_type": "bar",
            "timeframe": "1m",
            "windows": {
                "type": "fixed",
                "in_sample": { "label": "is", "from": "2026-01-01", "to": "2026-02-01" },
                "out_of_sample": { "label": "oos", "from": "2026-02-01", "to": "2026-03-01" }
            },
            "config": { "sizing": { "type": "FixedLot", "lots": 0.1 } },
            "workers": 2
        }
    })
}

fn portfolio() -> Value {
    json!({
        "request": {
            "instances": [{
                "symbol": "EURUSD",
                "strategy": {
                    "document": { "strategy_id": "alpha" },
                    "sources": [
                        { "source": "primary", "timeframe_seconds": 60, "price_basis": "mid" }
                    ],
                    "instance_id": "eur"
                },
                "profile_def": { "name": "trail", "use_targets": [], "close_ratios": [] },
                "entry_profile_routes": [{ "entry_class": "trend", "profile": "trail" }]
            }],
            "exchange": "fixture",
            "data_type": "tick",
            "config": { "sizing": { "type": "FixedLot", "lots": 0.1 } },
            "policies": [{ "type": "max_open_positions", "limit": 2, "future_field": true }],
            "groups": [{ "id": "usd", "symbols": ["EURUSD"] }]
        },
        "future": { "account_currency": "USD" }
    })
}

fn rejects<T: serde::de::DeserializeOwned>(mut value: Value, pointer: &str) {
    value
        .pointer_mut(pointer)
        .unwrap_or_else(|| panic!("{pointer} exists"))
        .as_object_mut()
        .unwrap()
        .insert("unexpected".into(), json!(true));
    let error = serde_json::from_value::<T>(value).err();
    assert!(
        error.is_some(),
        "an unknown field under {pointer} must be rejected"
    );
}

#[test]
fn configured_requests_reject_unknown_fields_at_every_level_but_carry_documents_as_values() {
    let decoded: RunConfiguredStrategyRequest = serde_json::from_value(configured()).unwrap();
    assert_eq!(
        decoded.request.strategy.document,
        json!({ "strategy_id": "alpha", "any": ["shape", 1] }),
        "the document is carried as an opaque value for the service to decode"
    );
    assert_eq!(
        decoded.request.strategy.sources[0].alignment_offset_seconds,
        0
    );
    for pointer in [
        "",
        "/request",
        "/request/strategy",
        "/request/strategy/sources/0",
        "/request/entry_profile_routes/0",
        "/request/config",
        "/request/config/sizing",
        "/future",
    ] {
        rejects::<RunConfiguredStrategyRequest>(configured(), pointer);
    }

    let submit = json!({ "request": configured() });
    serde_json::from_value::<SubmitConfiguredStrategyRequest>(submit.clone()).unwrap();
    rejects::<SubmitConfiguredStrategyRequest>(submit, "");
}

#[test]
fn search_requests_reject_unknown_fields_at_every_level_and_round_trip() {
    let decoded: SubmitSearchRequest = serde_json::from_value(search()).unwrap();
    let encoded = serde_json::to_value(&decoded).unwrap();
    let again: SubmitSearchRequest = serde_json::from_value(encoded).unwrap();
    assert_eq!(again.request.windows, decoded.request.windows);
    assert_eq!(again.request.workers, Some(2));
    for pointer in [
        "",
        "/request",
        "/request/windows",
        "/request/windows/in_sample",
        "/request/config",
    ] {
        rejects::<SubmitSearchRequest>(search(), pointer);
    }
}

#[test]
fn portfolio_requests_reject_unknown_fields_at_every_level_but_carry_documents_and_policies_as_values()
 {
    let decoded: RunPortfolioRequest = serde_json::from_value(portfolio()).unwrap();
    let spec = &decoded.request;
    assert_eq!(
        spec.instances[0].strategy.instance_id.as_deref(),
        Some("eur")
    );
    assert_eq!(
        spec.policies.as_ref().unwrap()[0]["future_field"],
        json!(true),
        "policies are carried as values for the service to decode strictly"
    );
    for pointer in [
        "",
        "/request",
        "/request/instances/0",
        "/request/instances/0/strategy",
        "/request/instances/0/strategy/sources/0",
        "/request/instances/0/profile_def",
        "/request/instances/0/entry_profile_routes/0",
        "/request/config",
        "/future",
    ] {
        rejects::<RunPortfolioRequest>(portfolio(), pointer);
    }

    let submit = json!({ "request": portfolio() });
    let decoded = serde_json::from_value::<SubmitPortfolioRequest>(submit.clone()).unwrap();
    let again: SubmitPortfolioRequest =
        serde_json::from_value(serde_json::to_value(&decoded).unwrap()).unwrap();
    assert_eq!(again.request.request.instances.len(), 1);
    rejects::<SubmitPortfolioRequest>(submit, "");
}
