use serde::{Deserialize, Serialize};

// Unary requests and responses

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPriceRequest {
    pub symbol: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPriceResponse {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    /// Service quote observation time in Unix milliseconds, not request handling time.
    pub ts_ms: i64,
    pub found: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPricesRequest {
    pub symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceSnapshot {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    /// Service quote observation time in Unix milliseconds, not request handling time.
    pub ts_ms: i64,
    pub found: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPricesResponse {
    pub prices: Vec<PriceSnapshot>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetSymbolListResponse {
    pub symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetStateResponse {
    pub state: String,
    /// Latest source-state transition time in Unix milliseconds, not response generation time.
    pub ts_ms: i64,
}

// Subscription commands

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribePricesRequest {
    pub symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnsubscribePricesRequest {
    pub symbols: Vec<String>,
}

// Alert commands

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetAlertRequest {
    pub alert_id: String, // empty = server generates
    pub symbol: String,
    pub price: f64,
    pub kind: String, // "ABOVE" / "BELOW"
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RemoveAlertRequest {
    pub alert_id: String,
}

// Alert query

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AlertInfo {
    pub alert_id: String,
    pub symbol: String,
    pub price: f64,
    pub kind: String, // "ABOVE" / "BELOW"
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetAlertsResponse {
    pub alerts: Vec<AlertInfo>,
}

// Streaming events from server to client

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PriceTick {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub ts_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AlertResult {
    pub alert_id: String,
    pub status: String,
    pub symbol: String,
    pub ref_price: f64,
    pub ts_ms: i64,
}

// Combined stream events

/// A service-observed market-data quality condition.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DataQualityEvent {
    /// Human-readable description of the quality condition.
    pub reason: String,
    /// Number of dropped observations when the service can determine it.
    pub dropped: Option<u64>,
    /// Unix timestamp in milliseconds when the service detected the condition.
    pub ts_ms: i64,
}

impl DataQualityEvent {
    /// Creates a data-quality event with an optional dropped-observation count.
    pub fn new(reason: impl Into<String>, dropped: Option<u64>, ts_ms: i64) -> Self {
        Self {
            reason: reason.into(),
            dropped,
            ts_ms,
        }
    }
}

/// Compatibility wire event for price, source-state, and data-quality streams.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct StreamEvent {
    /// Event discriminator: `PRICE`, `STATE`, or `DATA_QUALITY`.
    pub event_type: String,
    /// Symbol for a `PRICE` event. This is `None` for other event types.
    pub symbol: Option<String>,
    /// Bid for a `PRICE` event. This is `None` for other event types.
    pub bid: Option<f64>,
    /// Ask for a `PRICE` event. This is `None` for other event types.
    pub ask: Option<f64>,
    /// Source state for a `STATE` event. This is `None` for other event types.
    pub state: Option<String>,
    /// Quality payload for a `DATA_QUALITY` event. This is `None` for other event types.
    #[serde(default)]
    pub quality: Option<DataQualityEvent>,
    /// Event time in Unix milliseconds: quote observation for `PRICE`, source-state transition for `STATE`, or quality detection for `DATA_QUALITY`.
    pub ts_ms: i64,
}

impl StreamEvent {
    /// Discriminator used by price events.
    pub const PRICE: &'static str = "PRICE";
    /// Discriminator used by source-state events.
    pub const STATE: &'static str = "STATE";
    /// Discriminator used by data-quality events.
    pub const DATA_QUALITY: &'static str = "DATA_QUALITY";

    /// Creates a price event from a service-observed quote.
    pub fn price(tick: PriceTick) -> Self {
        Self {
            event_type: Self::PRICE.into(),
            symbol: Some(tick.symbol),
            bid: Some(tick.bid),
            ask: Some(tick.ask),
            state: None,
            quality: None,
            ts_ms: tick.ts_ms,
        }
    }

    /// Creates a source-state transition event.
    pub fn source_state(state: impl Into<String>, ts_ms: i64) -> Self {
        Self {
            event_type: Self::STATE.into(),
            symbol: None,
            bid: None,
            ask: None,
            state: Some(state.into()),
            quality: None,
            ts_ms,
        }
    }

    /// Creates a data-quality event and keeps the envelope timestamp aligned.
    pub fn data_quality(quality: DataQualityEvent) -> Self {
        Self {
            event_type: Self::DATA_QUALITY.into(),
            symbol: None,
            bid: None,
            ask: None,
            state: None,
            ts_ms: quality.ts_ms,
            quality: Some(quality),
        }
    }
}

/// Typed application event used by new consumers. `StreamEvent` remains the compatibility wire shape for the current service codec.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MarketDataEvent {
    Price(PriceTick),
    SourceState { state: String, ts_ms: i64 },
    Alert(AlertResult),
    DataQuality(DataQualityEvent),
}

// Generic acknowledgement

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommandAck {
    pub kind: String,
    pub reference: String,
}

impl CommandAck {
    pub fn ok(kind: &str, reference: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            reference: reference.into(),
        }
    }

    pub fn error(reference: impl Into<String>) -> Self {
        Self {
            kind: "ERROR".into(),
            reference: reference.into(),
        }
    }
}
