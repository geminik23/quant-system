use chrono::NaiveDateTime;
use qs_backtest::RawSignalEntry;
use serde::{Deserialize, Serialize};

/// Result of parsing a single message.
pub enum ParsedAction {
    /// One or more entry signals extracted.
    Entries(Vec<RawSignalEntry>),
    /// Message is not a trade signal — skip.
    Skip,
}

/// A raw Telegram message as extracted from SQLite (via JSONL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTgMessage {
    pub chat_id: i64,
    pub msg_id: i64,
    /// ISO 8601 UTC timestamp string.
    pub ts: String,
    pub message: String,
    /// If set, this message is a reply to the given msg_id.
    pub reply_to: Option<i64>,
}

/// Current market quote snapshot (bid/ask) for price-aware parsing.
#[derive(Debug, Clone, Copy)]
pub struct MarketQuote {
    pub bid: f64,
    pub ask: f64,
    pub ts: NaiveDateTime,
}

/// Placeholder for future LLM integration (e.g. OpenAI text extraction).
#[derive(Debug, Clone)]
pub struct LlmClient;

/// Context passed to every parse call — bundles optional services and history.
pub struct ParseContext<'a> {
    /// Live price snapshot for the symbol, if available.
    pub market: Option<&'a MarketQuote>,
    /// LLM client for AI-based extraction, if available.
    pub llm: Option<&'a LlmClient>,
    /// Recent messages for this channel (oldest first, capped at parser's max_history).
    pub history: &'a [RawTgMessage],
}

impl<'a> ParseContext<'a> {
    /// Convenience constructor with no services and empty history.
    pub fn empty() -> Self {
        Self {
            market: None,
            llm: None,
            history: &[],
        }
    }
}
