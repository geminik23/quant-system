use std::cell::RefCell;

use chrono::NaiveDateTime;
use qs_core::RawSignal;
use serde::{Deserialize, Serialize};

thread_local! {
    static CURRENT_MESSAGES: RefCell<Vec<RawTgMessage>> = const { RefCell::new(Vec::new()) };
}

struct CurrentMessageGuard;

impl Drop for CurrentMessageGuard {
    fn drop(&mut self) {
        CURRENT_MESSAGES.with(|messages| {
            messages.borrow_mut().pop();
        });
    }
}

/// Result of parsing a single message.
pub enum ParsedAction {
    /// One or more raw signals extracted.
    Signals(Vec<RawSignal>),
    /// Message is not a trade signal - skip.
    Skip,
    /// Message looked actionable but failed parser/domain validation.
    Rejected(ParseFailure),
}

impl ParsedAction {
    /// Create a signal result, collapsing empty batches to `Skip`.
    pub fn signals(signals: Vec<RawSignal>) -> Self {
        if signals.is_empty() {
            Self::Skip
        } else {
            Self::Signals(signals)
        }
    }

    /// Create a signal result containing exactly one signal.
    pub fn one(signal: RawSignal) -> Self {
        Self::Signals(vec![signal])
    }

    /// Returns true when this parse result should be skipped.
    pub fn is_skip(&self) -> bool {
        matches!(self, Self::Skip)
    }
}

/// A raw Telegram message as extracted from SQLite (via JSONL).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Return the message currently being parsed, when invoked through an
    /// identity-aware parser wrapper.
    ///
    /// The value is cloned so `ParseContext` keeps its existing public fields
    /// and remains constructible with the same struct literals. The scope is
    /// thread-local and lasts only for the synchronous parser call.
    pub fn current_message(&self) -> Option<RawTgMessage> {
        CURRENT_MESSAGES.with(|messages| messages.borrow().last().cloned())
    }

    /// Resolve the immutable Telegram root for the current message by walking
    /// reply ancestry through retained history.
    pub fn ultimate_root_message(&self) -> Result<RawTgMessage, ParseFailure> {
        let mut current =
            self.current_message()
                .ok_or_else(|| ParseFailure::AmbiguousIdentity {
                    reason: "current message identity is unavailable".into(),
                })?;
        let mut visited = std::collections::BTreeSet::new();
        visited.insert(current.msg_id);
        while let Some(parent_id) = current.reply_to {
            if !visited.insert(parent_id) {
                return Err(ParseFailure::AmbiguousIdentity {
                    reason: format!("reply cycle detected at message {parent_id}"),
                });
            }
            current = self
                .history
                .iter()
                .find(|message| message.chat_id == current.chat_id && message.msg_id == parent_id)
                .cloned()
                .ok_or(ParseFailure::MissingParent {
                    reply_to: parent_id,
                })?;
        }
        Ok(current)
    }

    pub(crate) fn with_current_message<T>(
        &self,
        message: &RawTgMessage,
        f: impl FnOnce() -> T,
    ) -> T {
        CURRENT_MESSAGES.with(|messages| messages.borrow_mut().push(message.clone()));
        let _guard = CurrentMessageGuard;
        f()
    }
}

/// Structured explanation for a message that was intentionally not parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// No parser is registered for the message's Telegram channel.
    UnregisteredChannel,
    /// The registered parser classified the message as non-actionable.
    ParserReturnedSkip,
}

/// Structured per-message parse failure captured by the V2 pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ParseFailure {
    /// The source timestamp could not be parsed.
    InvalidTimestamp { value: String, reason: String },
    /// A parser reported a failure rather than an intentional skip.
    Parser { reason: String },
    /// A reply references a parent unavailable in retained history.
    MissingParent { reply_to: i64 },
    /// An emitted signal violates the common signal contract.
    InvalidSignal { reason: String },
    /// Reply ancestry is cyclic or otherwise ambiguous.
    AmbiguousIdentity { reason: String },
}

/// Result of routing and parsing one source message.
///
/// `source` preserves the Telegram chat/message identity for diagnostics and
/// for correlating emitted signals with their original message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MessageParseOutcome {
    Parsed {
        source: RawTgMessage,
        parser: String,
        signals: Vec<RawSignal>,
    },
    Skipped {
        source: RawTgMessage,
        parser: Option<String>,
        reason: SkipReason,
    },
    Failed {
        source: RawTgMessage,
        parser: Option<String>,
        failure: ParseFailure,
    },
}

impl MessageParseOutcome {
    /// Source message associated with this outcome.
    pub fn source(&self) -> &RawTgMessage {
        match self {
            Self::Parsed { source, .. }
            | Self::Skipped { source, .. }
            | Self::Failed { source, .. } => source,
        }
    }

    /// Parser name, if a parser was selected for the source channel.
    pub fn parser(&self) -> Option<&str> {
        match self {
            Self::Parsed { parser, .. } => Some(parser),
            Self::Skipped { parser, .. } | Self::Failed { parser, .. } => parser.as_deref(),
        }
    }

    /// Signals emitted for this message, or an empty slice for skips/failures.
    pub fn signals(&self) -> &[RawSignal] {
        match self {
            Self::Parsed { signals, .. } => signals,
            Self::Skipped { .. } | Self::Failed { .. } => &[],
        }
    }
}

/// V2 batch result with input-ordered diagnostics and sorted successful signals.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParseBatchResult {
    /// One outcome per input message, in the same order as the input slice.
    /// Processing order remains deterministic for history-dependent parsing.
    pub outcomes: Vec<MessageParseOutcome>,
    /// All successfully parsed signals, sorted by signal timestamp.
    pub signals: Vec<RawSignal>,
}

impl ParseBatchResult {
    /// True when at least one source message failed to parse.
    pub fn has_failures(&self) -> bool {
        self.outcomes
            .iter()
            .any(|outcome| matches!(outcome, MessageParseOutcome::Failed { .. }))
    }
}
