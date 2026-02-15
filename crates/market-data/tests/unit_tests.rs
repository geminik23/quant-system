use market_data::rpc_types::*;
use market_data::xrpc_state::XrpcState;

// ── rpc_types: serde round-trip ──

#[test]
fn rpc_types_serde_roundtrip() {
    // ConnectRequest / ConnectResponse
    let req = ConnectRequest {
        client_name: "test".into(),
    };
    let json = serde_json::to_string(&req).unwrap();
    let back: ConnectRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(back.client_name, "test");

    let resp = ConnectResponse {
        client_id: 42,
        slot_name: "slot-42".into(),
    };
    let json = serde_json::to_string(&resp).unwrap();
    let back: ConnectResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(back.client_id, 42);
    assert_eq!(back.slot_name, "slot-42");

    // GetPriceRequest / GetPriceResponse
    let price_resp = GetPriceResponse {
        symbol: "EURUSD".into(),
        bid: 1.1234,
        ask: 1.1236,
        ts_ms: 1700000000000,
        found: true,
    };
    let json = serde_json::to_string(&price_resp).unwrap();
    let back: GetPriceResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(back.symbol, "EURUSD");
    assert!((back.bid - 1.1234).abs() < f64::EPSILON);
    assert!(back.found);

    // GetPricesResponse with empty list
    let prices_resp = GetPricesResponse { prices: vec![] };
    let json = serde_json::to_string(&prices_resp).unwrap();
    let back: GetPricesResponse = serde_json::from_str(&json).unwrap();
    assert!(back.prices.is_empty());

    // SetAlertRequest
    let alert = SetAlertRequest {
        alert_id: String::new(),
        symbol: "XAUUSD".into(),
        price: 2000.50,
        kind: "ABOVE".into(),
    };
    let json = serde_json::to_string(&alert).unwrap();
    let back: SetAlertRequest = serde_json::from_str(&json).unwrap();
    assert!(back.alert_id.is_empty());
    assert_eq!(back.kind, "ABOVE");

    // PriceTick
    let tick = PriceTick {
        symbol: "GBPUSD".into(),
        bid: 1.27,
        ask: 1.2702,
        ts_ms: 0,
    };
    let json = serde_json::to_string(&tick).unwrap();
    let back: PriceTick = serde_json::from_str(&json).unwrap();
    assert_eq!(back.symbol, "GBPUSD");

    // AlertResult
    let ar = AlertResult {
        alert_id: "a1".into(),
        status: "TRIGGERED".into(),
        symbol: "EURUSD".into(),
        ref_price: 1.12,
        ts_ms: 999,
    };
    let json = serde_json::to_string(&ar).unwrap();
    let back: AlertResult = serde_json::from_str(&json).unwrap();
    assert_eq!(back.alert_id, "a1");
    assert_eq!(back.status, "TRIGGERED");
}

#[test]
fn command_ack_constructors() {
    let ok = CommandAck::ok("SUBSCRIBED", "3 symbols");
    assert_eq!(ok.kind, "SUBSCRIBED");
    assert_eq!(ok.reference, "3 symbols");

    let err = CommandAck::error("something went wrong");
    assert_eq!(err.kind, "ERROR");
    assert_eq!(err.reference, "something went wrong");
}

#[test]
fn rpc_types_alert_info_serde() {
    let resp = GetAlertsResponse {
        alerts: vec![
            AlertInfo {
                alert_id: "a1".into(),
                symbol: "EURUSD".into(),
                price: 1.12,
                kind: "ABOVE".into(),
            },
            AlertInfo {
                alert_id: "a2".into(),
                symbol: "XAUUSD".into(),
                price: 2000.0,
                kind: "BELOW".into(),
            },
        ],
    };
    let json = serde_json::to_string(&resp).unwrap();
    let back: GetAlertsResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(back.alerts.len(), 2);
    assert_eq!(back.alerts[0].alert_id, "a1");
    assert_eq!(back.alerts[0].kind, "ABOVE");
    assert_eq!(back.alerts[1].alert_id, "a2");
    assert_eq!(back.alerts[1].kind, "BELOW");
    assert!((back.alerts[1].price - 2000.0).abs() < f64::EPSILON);
}

#[test]
fn rpc_types_stream_event_serde() {
    // Price event
    let price_event = StreamEvent {
        event_type: "PRICE".into(),
        symbol: Some("EURUSD".into()),
        bid: Some(1.1234),
        ask: Some(1.1236),
        state: None,
        ts_ms: 1700000000000,
    };
    let json = serde_json::to_string(&price_event).unwrap();
    let back: StreamEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event_type, "PRICE");
    assert_eq!(back.symbol.as_deref(), Some("EURUSD"));
    assert!(back.state.is_none());

    // State event
    let state_event = StreamEvent {
        event_type: "STATE".into(),
        symbol: None,
        bid: None,
        ask: None,
        state: Some("CONNECTED".into()),
        ts_ms: 1700000000000,
    };
    let json = serde_json::to_string(&state_event).unwrap();
    let back: StreamEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event_type, "STATE");
    assert_eq!(back.state.as_deref(), Some("CONNECTED"));
    assert!(back.symbol.is_none());
    assert!(back.bid.is_none());
}

// ── xrpc_state: client id, alert ownership, cleanup ──

#[tokio::test]
async fn xrpc_state_client_id_increments() {
    let state = XrpcState::new();
    assert_eq!(state.next_client_id().await, 1);
    assert_eq!(state.next_client_id().await, 2);
    assert_eq!(state.next_client_id().await, 3);
}

#[tokio::test]
async fn xrpc_state_alert_ownership_lifecycle() {
    let state = XrpcState::new();

    // Own alerts
    state.own_alert("alert-1", 1).await;
    state.own_alert("alert-2", 1).await;
    state.own_alert("alert-3", 2).await;
    state
        .set_alert_meta("alert-1", "EURUSD", 1.12, "ABOVE")
        .await;
    state
        .set_alert_meta("alert-2", "XAUUSD", 2000.0, "BELOW")
        .await;
    state
        .set_alert_meta("alert-3", "GBPUSD", 1.27, "ABOVE")
        .await;

    // Verify ownership
    assert_eq!(state.owner_of("alert-1").await, Some(1));
    assert_eq!(state.owner_of("alert-3").await, Some(2));
    assert_eq!(state.owner_of("nonexistent").await, None);

    // Release single alert
    state.release_alert("alert-2").await;
    assert_eq!(state.owner_of("alert-2").await, None);
    assert!(state.take_alert_meta("alert-2").await.is_none());

    // Release all alerts for client 1 (only alert-1 remains after alert-2 was released)
    let released = state.release_alerts_of(1).await;
    assert_eq!(released, vec!["alert-1"]);
    assert_eq!(state.owner_of("alert-1").await, None);

    // Client 2's alert untouched
    assert_eq!(state.owner_of("alert-3").await, Some(2));

    // Release for client with no alerts returns empty
    let released = state.release_alerts_of(99).await;
    assert!(released.is_empty());
}

#[tokio::test]
async fn xrpc_state_alert_meta() {
    let state = XrpcState::new();
    state.set_alert_meta("a1", "EURUSD", 1.12, "ABOVE").await;

    // take_alert_meta removes it
    let meta = state.take_alert_meta("a1").await;
    assert_eq!(
        meta,
        Some(("EURUSD".to_string(), 1.12, "ABOVE".to_string()))
    );

    // Second take returns None
    assert!(state.take_alert_meta("a1").await.is_none());
}

#[tokio::test]
async fn xrpc_state_alerts_of() {
    let state = XrpcState::new();

    state.own_alert("a1", 1).await;
    state.set_alert_meta("a1", "EURUSD", 1.12, "ABOVE").await;
    state.own_alert("a2", 1).await;
    state.set_alert_meta("a2", "XAUUSD", 2000.0, "BELOW").await;
    state.own_alert("a3", 2).await;
    state.set_alert_meta("a3", "GBPUSD", 1.27, "ABOVE").await;

    // Client 1 has 2 alerts
    let mut alerts = state.alerts_of(1).await;
    alerts.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].0, "a1");
    assert_eq!(alerts[0].1, "EURUSD");
    assert!((alerts[0].2 - 1.12).abs() < f64::EPSILON);
    assert_eq!(alerts[0].3, "ABOVE");
    assert_eq!(alerts[1].0, "a2");
    assert_eq!(alerts[1].1, "XAUUSD");
    assert_eq!(alerts[1].3, "BELOW");

    // Client 2 only sees their own
    let alerts2 = state.alerts_of(2).await;
    assert_eq!(alerts2.len(), 1);
    assert_eq!(alerts2[0].0, "a3");
    assert_eq!(alerts2[0].1, "GBPUSD");

    // Non-existent client
    let empty = state.alerts_of(99).await;
    assert!(empty.is_empty());

    // After releasing client 1's alerts, alerts_of returns empty
    state.release_alerts_of(1).await;
    let after = state.alerts_of(1).await;
    assert!(after.is_empty());

    // Client 2 unaffected
    assert_eq!(state.alerts_of(2).await.len(), 1);
}

// ── price_alert: core alert triggering logic ──

use market_data::core::AlertSet;
use market_data::market_data::price_alert::PriceAlert;

#[test]
fn price_alert_set_and_trigger() {
    let mut pa = PriceAlert::new();

    // Set a HIGH alert at 1.12 for symbol_id=1
    let id = pa.set_alert(1, AlertSet::High(1.12), Some("h1".into()));
    assert_eq!(id, "h1");

    // Price below threshold — no trigger
    assert!(pa.on_price(1, (1.10, 1.1002)).is_none());

    // Price crosses above — triggers
    let triggered = pa.on_price(1, (1.13, 1.1302));
    assert_eq!(triggered, Some(vec!["h1".to_string()]));

    // Alert consumed — no second trigger
    assert!(pa.on_price(1, (1.14, 1.1402)).is_none());
}

#[test]
fn price_alert_low_trigger() {
    let mut pa = PriceAlert::new();
    pa.set_alert(1, AlertSet::Low(1.10), Some("low1".into()));

    // Price above — no trigger
    pa.on_price(1, (1.12, 1.1202));
    assert!(pa.on_price(1, (1.11, 1.1102)).is_none());

    // Price drops to/below — triggers
    let triggered = pa.on_price(1, (1.09, 1.0902));
    assert_eq!(triggered, Some(vec!["low1".to_string()]));
}

#[test]
fn price_alert_remove() {
    let mut pa = PriceAlert::new();
    pa.set_alert(1, AlertSet::High(1.20), Some("r1".into()));

    let removed = pa.remove("r1".into());
    assert!(removed.is_some());

    // Removed alert should not trigger
    assert!(pa.on_price(1, (1.25, 1.2502)).is_none());

    // Removing non-existent returns None
    assert!(pa.remove("nonexistent".into()).is_none());
}

#[test]
fn price_alert_modify_price() {
    let mut pa = PriceAlert::new();
    pa.set_alert(1, AlertSet::High(1.20), Some("m1".into()));

    // Modify threshold upward
    let new_set = pa.modify_price("m1".into(), 1.30);
    assert_eq!(new_set, Some(AlertSet::High(1.30)));

    // Old threshold no longer triggers
    pa.on_price(1, (1.10, 1.1002));
    assert!(pa.on_price(1, (1.25, 1.2502)).is_none());

    // New threshold triggers
    let triggered = pa.on_price(1, (1.31, 1.3102));
    assert_eq!(triggered, Some(vec!["m1".to_string()]));
}

// ── utils: config loading ──

#[test]
fn load_config_toml() {
    use market_data::utils::load_config;
    use serde::Deserialize;
    use std::io::Write;

    #[derive(Deserialize, Debug)]
    struct TestConfig {
        name: String,
        value: u32,
    }

    let dir = std::env::temp_dir().join("quant_test_config");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("test.toml");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "name = \"hello\"\nvalue = 42").unwrap();

    let cfg: TestConfig = load_config(&path).unwrap();
    assert_eq!(cfg.name, "hello");
    assert_eq!(cfg.value, 42);

    std::fs::remove_dir_all(&dir).ok();
}
