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

    /// Parse a root message while exposing its full source identity through
    /// [`ParseContext::current_message`]. Existing implementations only need to
    /// implement [`ChannelParser::parse_root`].
    fn parse_root_message(
        &self,
        current: &RawTgMessage,
        ts: NaiveDateTime,
        ctx: &ParseContext,
    ) -> ParsedAction {
        ctx.with_current_message(current, || self.parse_root(&current.message, ts, ctx))
    }

    /// Parse a reply message. `parent` is the original message being replied to
    /// (looked up from history by `reply_to` msg_id), or `None` if not found.
    ///
    /// The compatibility pipeline preserves the `None` case so entry-only
    /// parsers can intentionally return [`ParsedAction::Skip`]. The structured
    /// V2 pipeline reports an unavailable parent as `MissingParent` before
    /// invoking this method.
    fn parse_reply(
        &self,
        message: &str,
        ts: NaiveDateTime,
        parent: Option<&RawTgMessage>,
        ctx: &ParseContext,
    ) -> ParsedAction;

    /// Parse a reply while exposing its full source identity through
    /// [`ParseContext::current_message`]. Existing implementations only need to
    /// implement [`ChannelParser::parse_reply`].
    fn parse_reply_message(
        &self,
        current: &RawTgMessage,
        ts: NaiveDateTime,
        parent: Option<&RawTgMessage>,
        ctx: &ParseContext,
    ) -> ParsedAction {
        ctx.with_current_message(current, || {
            self.parse_reply(&current.message, ts, parent, ctx)
        })
    }
}
