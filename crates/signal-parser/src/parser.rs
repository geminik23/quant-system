use crate::types::{ParseContext, ParsedAction, RawTgMessage};
use chrono::NaiveDateTime;

/// Per-channel message parser trait.
/// Each signal provider (Telegram channel) has its own message format.
pub trait ChannelParser: Send + Sync {
    /// Human-readable parser name (e.g. "wave-trader").
    fn name(&self) -> &str;
    /// Telegram channel ID(s) this parser handles.
    fn channel_ids(&self) -> &[i64];

    /// Max recent messages to retain per channel for context (0 = no history needed).
    fn max_history(&self) -> usize {
        0
    }

    /// Parse a root message (one without `reply_to`).
    fn parse_root(&self, message: &str, ts: NaiveDateTime, ctx: &ParseContext) -> ParsedAction;

    /// Parse a reply message. `parent` is the original message being replied to
    /// (looked up from history by `reply_to` msg_id), or `None` if not found.
    fn parse_reply(
        &self,
        message: &str,
        ts: NaiveDateTime,
        parent: Option<&RawTgMessage>,
        ctx: &ParseContext,
    ) -> ParsedAction;
}

