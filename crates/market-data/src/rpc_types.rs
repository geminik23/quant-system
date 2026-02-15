use serde::{Deserialize, Serialize};

// ── Connection Handshake ──

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectRequest {
    pub client_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectResponse {
    pub client_id: usize,
    pub slot_name: String,
}

// ── Unary Requests / Responses ──

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPriceRequest {
    pub symbol: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetPriceResponse {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
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
    pub ts_ms: i64,
}

// ── Subscription Commands ──

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribePricesRequest {
    pub symbols: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnsubscribePricesRequest {
    pub symbols: Vec<String>,
}

// ── Alert Commands ──

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

// ── Alert Query ──

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

// ── Streaming Events (server -> client) ──

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

// ── Combined Stream Event (prices + state changes) ──

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamEvent {
    pub event_type: String, // "PRICE" | "STATE"
    // Price fields (present when event_type = "PRICE")
    pub symbol: Option<String>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    // State fields (present when event_type = "STATE")
    pub state: Option<String>, // "CONNECTED" | "DISCONNECTED" | "CONNECTING" | "LOGON"
    // Common
    pub ts_ms: i64,
}

// ── Generic Ack ──

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
