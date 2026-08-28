use qs_market_data_api::{DataQualityEvent, MarketDataEvent, PriceTick, StreamEvent};
use serde_json::json;

#[test]
fn data_quality_event_has_a_strict_serde_contract() {
    let event = DataQualityEvent::new("upstream sequence gap", Some(3), 1_700_000_000_123);

    let encoded = serde_json::to_value(&event).unwrap();
    assert_eq!(
        encoded,
        json!({
            "reason": "upstream sequence gap",
            "dropped": 3,
            "ts_ms": 1_700_000_000_123_i64,
        })
    );
    assert_eq!(
        serde_json::from_value::<DataQualityEvent>(encoded).unwrap(),
        event
    );

    let unknown_field = json!({
        "reason": "upstream sequence gap",
        "dropped": 3,
        "ts_ms": 1_700_000_000_123_i64,
        "source": "venue-a",
    });
    assert!(serde_json::from_value::<DataQualityEvent>(unknown_field).is_err());
}

#[test]
fn stream_event_helpers_encode_price_state_and_data_quality() {
    let price = StreamEvent::price(PriceTick {
        symbol: "EURUSD".into(),
        bid: 1.1234,
        ask: 1.1236,
        ts_ms: 1_700_000_000_100,
    });
    assert_eq!(
        serde_json::to_value(price).unwrap(),
        json!({
            "event_type": "PRICE",
            "symbol": "EURUSD",
            "bid": 1.1234,
            "ask": 1.1236,
            "state": null,
            "quality": null,
            "ts_ms": 1_700_000_000_100_i64,
        })
    );

    let state = StreamEvent::source_state("CONNECTED", 1_700_000_000_200);
    assert_eq!(
        serde_json::to_value(state).unwrap(),
        json!({
            "event_type": "STATE",
            "symbol": null,
            "bid": null,
            "ask": null,
            "state": "CONNECTED",
            "quality": null,
            "ts_ms": 1_700_000_000_200_i64,
        })
    );

    let quality = StreamEvent::data_quality(DataQualityEvent::new(
        "consumer lag",
        None,
        1_700_000_000_300,
    ));
    assert_eq!(
        serde_json::to_value(quality).unwrap(),
        json!({
            "event_type": "DATA_QUALITY",
            "symbol": null,
            "bid": null,
            "ask": null,
            "state": null,
            "quality": {
                "reason": "consumer lag",
                "dropped": null,
                "ts_ms": 1_700_000_000_300_i64,
            },
            "ts_ms": 1_700_000_000_300_i64,
        })
    );
}

#[test]
fn stream_event_defaults_missing_quality_and_rejects_unknown_fields() {
    let legacy_price = json!({
        "event_type": "PRICE",
        "symbol": "EURUSD",
        "bid": 1.1234,
        "ask": 1.1236,
        "state": null,
        "ts_ms": 1_700_000_000_100_i64,
    });
    let decoded: StreamEvent = serde_json::from_value(legacy_price).unwrap();
    assert!(decoded.quality.is_none());

    let unknown_field = json!({
        "event_type": "STATE",
        "symbol": null,
        "bid": null,
        "ask": null,
        "state": "CONNECTED",
        "quality": null,
        "ts_ms": 1_700_000_000_200_i64,
        "sequence": 7,
    });
    assert!(serde_json::from_value::<StreamEvent>(unknown_field).is_err());
}

#[test]
fn typed_data_quality_event_uses_the_shared_payload() {
    let event = MarketDataEvent::DataQuality(DataQualityEvent::new(
        "upstream sequence gap",
        Some(2),
        1_700_000_000_400,
    ));
    let encoded = serde_json::to_value(event).unwrap();
    assert_eq!(
        encoded,
        json!({
            "type": "data_quality",
            "reason": "upstream sequence gap",
            "dropped": 2,
            "ts_ms": 1_700_000_000_400_i64,
        })
    );

    let unknown_field = json!({
        "type": "data_quality",
        "reason": "upstream sequence gap",
        "dropped": 2,
        "ts_ms": 1_700_000_000_400_i64,
        "source": "venue-a",
    });
    assert!(serde_json::from_value::<MarketDataEvent>(unknown_field).is_err());
}
