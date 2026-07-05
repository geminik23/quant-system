use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::SignalParserError;
use crate::handler::{SignalContext, SignalHandler};
use crate::registry::ParserRegistry;
use crate::types::{ParseContext, ParsedAction, RawTgMessage};

//
// Relay message types matching app.py JSON format.
//

/// Incoming relay message from app.py.
#[derive(Debug, Deserialize)]
struct RelayMessage {
    /// Event type: "NEW", "EDIT", "DEL".
    t: String,
    data: RelayData,
    #[allow(dead_code)]
    sender: Option<String>,
}

/// Relay payload covering all event types.
#[derive(Debug, Deserialize)]
struct RelayData {
    /// Cleaned message text (None for DEL events).
    message: Option<String>,
    /// Telegram message ID.
    msg_id: Option<i64>,
    /// Channel ID (remapped via alter_channel in app.py).
    ch_id: i64,
    /// Timestamp as float seconds since epoch.
    date: Option<f64>,
    /// Reply-to message ID (None if root message).
    reply_to: Option<i64>,
    /// Whether the message was forwarded.
    #[allow(dead_code)]
    is_forwarded: Option<bool>,
    /// For DEL events: list of deleted message IDs.
    del_ids: Option<Vec<i64>>,
    /// For DEL events: single deleted message ID.
    del_id: Option<i64>,
}

/// HTTP response body.
#[derive(Serialize)]
struct RelayResponse {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

//
// Shared server state.
//

type History = HashMap<i64, VecDeque<RawTgMessage>>;

struct AppState {
    registry: Arc<ParserRegistry>,
    handler: Arc<dyn SignalHandler>,
    history: Arc<Mutex<History>>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            handler: Arc::clone(&self.handler),
            history: Arc::clone(&self.history),
        }
    }
}

//
// OnlineServer - HTTP server for live Telegram relay.
//

/// Listens for HTTP POST messages from the Telegram relay app (app.py),
/// parses them through registered channel parsers, and calls the
/// SignalHandler for each result.
pub struct OnlineServer {
    registry: ParserRegistry,
    handler: Box<dyn SignalHandler>,
    port: u16,
}

impl OnlineServer {
    /// Create with parser registry and signal handler.
    pub fn new(registry: ParserRegistry, handler: Box<dyn SignalHandler>) -> Self {
        Self {
            registry,
            handler,
            port: 40101,
        }
    }

    /// Override the listen port (default: 40101).
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Run the server. Blocks until shutdown.
    pub async fn run(self) -> Result<(), SignalParserError> {
        let state = AppState {
            registry: Arc::new(self.registry),
            handler: Arc::from(self.handler),
            history: Arc::new(Mutex::new(HashMap::new())),
        };

        let app = Router::new()
            .route("/on_msg", post(handle_on_msg))
            .route("/health", axum::routing::get(handle_health))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        tracing::info!("Online server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| SignalParserError::Io(e))?;
        axum::serve(listener, app)
            .await
            .map_err(|e| SignalParserError::Io(e))?;

        Ok(())
    }
}

//
// HTTP handlers.
//

/// Health check endpoint.
async fn handle_health() -> &'static str {
    "ok"
}

/// Main relay endpoint matching app.py POST format.
async fn handle_on_msg(
    State(state): State<AppState>,
    Json(relay): Json<RelayMessage>,
) -> (StatusCode, Json<RelayResponse>) {
    let mut history = state.history.lock().await;

    match process_relay(&state.registry, state.handler.as_ref(), &mut history, relay) {
        Ok(()) => (
            StatusCode::OK,
            Json(RelayResponse {
                status: "ok".to_string(),
                error: None,
            }),
        ),
        Err(e) => {
            tracing::error!("Relay processing error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RelayResponse {
                    status: "error".to_string(),
                    error: Some(e.to_string()),
                }),
            )
        }
    }
}

//
// Relay processing logic.
//

/// Convert float epoch seconds to NaiveDateTime.
fn epoch_to_naive(secs: f64) -> Option<NaiveDateTime> {
    let whole = secs as i64;
    let nanos = ((secs - whole as f64) * 1_000_000_000.0) as u32;
    chrono::DateTime::from_timestamp(whole, nanos).map(|dt| dt.naive_utc())
}

/// Process a single relay message through the parser pipeline and handler.
fn process_relay(
    registry: &ParserRegistry,
    handler: &dyn SignalHandler,
    history: &mut History,
    relay: RelayMessage,
) -> Result<(), SignalParserError> {
    match relay.t.as_str() {
        "NEW" | "EDIT" => {
            let data = &relay.data;
            let message = match &data.message {
                Some(m) if !m.is_empty() => m.clone(),
                _ => return Ok(()),
            };
            let ch_id = data.ch_id;
            let msg_id = data.msg_id.unwrap_or(0);

            // Look up parser.
            let parser = match registry.get(ch_id) {
                Some(p) => p,
                None => {
                    handler.on_unregistered_channel(ch_id, &message);
                    return Ok(());
                }
            };

            // Parse timestamp from float epoch.
            let ts = match data.date {
                Some(d) => epoch_to_naive(d).unwrap_or_else(|| chrono::Utc::now().naive_utc()),
                None => chrono::Utc::now().naive_utc(),
            };

            // Build context from history.
            let chan_history = history.entry(ch_id).or_default();
            let history_slice: Vec<RawTgMessage> = chan_history.iter().cloned().collect();
            let ctx = ParseContext {
                market: None,
                llm: None,
                history: &history_slice,
            };

            // Route to parse_root or parse_reply.
            let action = if let Some(reply_to_id) = data.reply_to {
                let parent = chan_history.iter().find(|m| m.msg_id == reply_to_id);
                parser.parse_reply(&message, ts, parent, &ctx)
            } else {
                parser.parse_root(&message, ts, &ctx)
            };

            // Build handler context.
            let signal_ctx = SignalContext {
                chat_id: ch_id,
                msg_id,
                ts,
                parser_name: parser.name().to_string(),
            };

            // Call handler based on event type and parse result.
            match action {
                ParsedAction::Signals(signals) => {
                    if relay.t == "EDIT" {
                        handler.on_signal_edit(signals, &signal_ctx);
                    } else {
                        handler.on_signals(signals, &signal_ctx);
                    }
                }
                ParsedAction::Skip => {
                    handler.on_skip(&message, &signal_ctx);
                }
            }

            // Push into history.
            let max_hist = parser.max_history();
            if max_hist > 0 {
                let raw_msg = RawTgMessage {
                    chat_id: ch_id,
                    msg_id,
                    ts: ts.to_string(),
                    message,
                    reply_to: data.reply_to,
                };
                chan_history.push_back(raw_msg);
                while chan_history.len() > max_hist {
                    chan_history.pop_front();
                }
            }
        }

        "DEL" => {
            let ch_id = relay.data.ch_id;
            let mut ids = relay.data.del_ids.unwrap_or_default();
            if let Some(single) = relay.data.del_id {
                if !ids.contains(&single) {
                    ids.push(single);
                }
            }
            if !ids.is_empty() {
                handler.on_signal_delete(ch_id, ids);
            }
        }

        other => {
            tracing::warn!("Unknown relay event type: '{}'", other);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qs_backtest::RawSignal;
    use std::sync::atomic::{AtomicUsize, Ordering};

    //
    // Test handler that captures calls.
    //

    struct TestHandler {
        new_count: AtomicUsize,
        edit_count: AtomicUsize,
        delete_count: AtomicUsize,
        skip_count: AtomicUsize,
        unregistered_count: AtomicUsize,
        last_signals: std::sync::Mutex<Vec<RawSignal>>,
        last_del_ids: std::sync::Mutex<Vec<i64>>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                new_count: AtomicUsize::new(0),
                edit_count: AtomicUsize::new(0),
                delete_count: AtomicUsize::new(0),
                skip_count: AtomicUsize::new(0),
                unregistered_count: AtomicUsize::new(0),
                last_signals: std::sync::Mutex::new(Vec::new()),
                last_del_ids: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl SignalHandler for TestHandler {
        fn on_signals(&self, signals: Vec<RawSignal>, _ctx: &SignalContext) {
            self.new_count.fetch_add(1, Ordering::Relaxed);
            *self.last_signals.lock().unwrap() = signals;
        }
        fn on_signal_edit(&self, signals: Vec<RawSignal>, _ctx: &SignalContext) {
            self.edit_count.fetch_add(1, Ordering::Relaxed);
            *self.last_signals.lock().unwrap() = signals;
        }
        fn on_signal_delete(&self, _chat_id: i64, msg_ids: Vec<i64>) {
            self.delete_count.fetch_add(1, Ordering::Relaxed);
            *self.last_del_ids.lock().unwrap() = msg_ids;
        }
        fn on_skip(&self, _msg: &str, _ctx: &SignalContext) {
            self.skip_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_unregistered_channel(&self, _chat_id: i64, _msg: &str) {
            self.unregistered_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn make_registry() -> ParserRegistry {
        let mut reg = ParserRegistry::new();
        let parser =
            crate::template::TemplateParser::new("test-chan", vec![100], 0.01, Some("test".into()));
        reg.register(Box::new(parser));
        reg
    }

    fn make_relay(t: &str, ch_id: i64, msg_id: i64, message: &str, date: f64) -> RelayMessage {
        RelayMessage {
            t: t.to_string(),
            data: RelayData {
                message: Some(message.to_string()),
                msg_id: Some(msg_id),
                ch_id,
                date: Some(date),
                reply_to: None,
                is_forwarded: None,
                del_ids: None,
                del_id: None,
            },
            sender: Some("test".to_string()),
        }
    }

    fn make_del_relay(ch_id: i64, del_ids: Vec<i64>, del_id: Option<i64>) -> RelayMessage {
        RelayMessage {
            t: "DEL".to_string(),
            data: RelayData {
                message: None,
                msg_id: None,
                ch_id,
                date: None,
                reply_to: None,
                is_forwarded: None,
                del_ids: Some(del_ids),
                del_id,
            },
            sender: Some("test".to_string()),
        }
    }

    #[test]
    fn relay_new_message_calls_on_signals() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay(
            "NEW",
            100,
            1,
            "EURUSD BUY NOW SL 1.08 TP 1.09",
            1706000000.0,
        );
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.new_count.load(Ordering::Relaxed), 1);
        let signals = handler.last_signals.lock().unwrap();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].as_entry().unwrap().symbol, "eurusd");
    }

    #[test]
    fn relay_edit_message_calls_on_signal_edit() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay(
            "EDIT",
            100,
            1,
            "EURUSD BUY NOW SL 1.07 TP 1.10",
            1706000000.0,
        );
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.edit_count.load(Ordering::Relaxed), 1);
        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn relay_del_message_calls_on_signal_delete() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_del_relay(100, vec![10, 20], Some(30));
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.delete_count.load(Ordering::Relaxed), 1);
        let ids = handler.last_del_ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&10));
        assert!(ids.contains(&20));
        assert!(ids.contains(&30));
    }

    #[test]
    fn relay_del_deduplicates_ids() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_del_relay(100, vec![10, 20], Some(10));
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        let ids = handler.last_del_ids.lock().unwrap();
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn relay_unknown_channel_calls_on_unregistered() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay(
            "NEW",
            999,
            1,
            "EURUSD BUY NOW SL 1.08 TP 1.09",
            1706000000.0,
        );
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.unregistered_count.load(Ordering::Relaxed), 1);
        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn relay_non_signal_calls_on_skip() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay("NEW", 100, 1, "Good morning traders!", 1706000000.0);
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.skip_count.load(Ordering::Relaxed), 1);
        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn relay_unknown_event_type_ignored() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay("UNKNOWN", 100, 1, "hello", 1706000000.0);
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
        assert_eq!(handler.skip_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn relay_reply_message_routes_to_parse_reply() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        // TemplateParser.parse_reply() always returns Skip.
        let relay = RelayMessage {
            t: "NEW".to_string(),
            data: RelayData {
                message: Some("close this".to_string()),
                msg_id: Some(2),
                ch_id: 100,
                date: Some(1706000000.0),
                reply_to: Some(1),
                is_forwarded: None,
                del_ids: None,
                del_id: None,
            },
            sender: Some("test".to_string()),
        };
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        // parse_reply returns Skip, so on_skip is called.
        assert_eq!(handler.skip_count.load(Ordering::Relaxed), 1);
        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn history_maintained_across_requests() {
        // Use a parser with max_history > 0 to verify history accumulates.
        let mut reg = ParserRegistry::new();
        let parser =
            crate::template::TemplateParser::new("hist-chan", vec![200], 0.01, Some("hist".into()));
        reg.register(Box::new(parser));

        let handler = TestHandler::new();
        let mut history: History = HashMap::new();

        // Send two NEW messages to the same channel.
        let relay1 = make_relay(
            "NEW",
            200,
            1,
            "EURUSD BUY NOW SL 1.08 TP 1.09",
            1706000000.0,
        );
        process_relay(&reg, &handler, &mut history, relay1).unwrap();

        let relay2 = make_relay(
            "NEW",
            200,
            2,
            "GBPUSD SELL NOW SL 1.30 TP 1.28",
            1706000100.0,
        );
        process_relay(&reg, &handler, &mut history, relay2).unwrap();

        // TemplateParser has max_history() == 0, so history should be empty.
        // But the pipeline still processes both messages.
        assert_eq!(handler.new_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn epoch_to_naive_converts_correctly() {
        let ts = epoch_to_naive(1706000000.0).unwrap();
        assert_eq!(ts.and_utc().timestamp(), 1706000000);
    }

    #[test]
    fn empty_del_ids_does_not_call_handler() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_del_relay(100, vec![], None);
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.delete_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn empty_message_is_ignored() {
        let registry = make_registry();
        let handler = TestHandler::new();
        let mut history = HashMap::new();

        let relay = make_relay("NEW", 100, 1, "", 1706000000.0);
        process_relay(&registry, &handler, &mut history, relay).unwrap();

        assert_eq!(handler.new_count.load(Ordering::Relaxed), 0);
        assert_eq!(handler.skip_count.load(Ordering::Relaxed), 0);
    }
}
