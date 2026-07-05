use std::collections::{HashMap, VecDeque};

use qs_backtest::RawSignal;

use crate::error::SignalParserError;
use crate::registry::ParserRegistry;
use crate::types::{LlmClient, MarketQuote, ParseContext, ParsedAction, RawTgMessage};

use chrono::NaiveDateTime;

/// Try multiple ISO 8601 datetime formats.
fn parse_iso_datetime(s: &str) -> Result<NaiveDateTime, SignalParserError> {
    for fmt in [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt);
        }
    }
    Err(SignalParserError::TimestampParse(
        s.to_string(),
        "unrecognized format".to_string(),
    ))
}

/// Parse a batch of raw Telegram messages into signals using the registry.
///
/// Maintains per-channel message history (capped at each parser's `max_history()`),
/// routes root messages to `parse_root` and reply messages to `parse_reply`,
/// and passes optional market/LLM context through `ParseContext`.
/// Returns signals sorted by timestamp.
pub fn parse_messages(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
) -> Result<Vec<RawSignal>, SignalParserError> {
    parse_messages_with_context(registry, messages, None, None)
}

/// Full-context variant - same as `parse_messages` but accepts optional market quote and LLM client.
pub fn parse_messages_with_context(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
    market: Option<&MarketQuote>,
    llm: Option<&LlmClient>,
) -> Result<Vec<RawSignal>, SignalParserError> {
    let mut signals: Vec<RawSignal> = Vec::new();

    // Per-channel sliding window of recent messages (oldest-first).
    let mut history: HashMap<i64, VecDeque<RawTgMessage>> = HashMap::new();

    for msg in messages {
        let parser = match registry.get(msg.chat_id) {
            Some(p) => p,
            None => continue,
        };

        let ts = parse_iso_datetime(&msg.ts)?;
        let max_hist = parser.max_history();

        // Build the history slice for this channel.
        let chan_history = history.entry(msg.chat_id).or_default();
        let history_slice: Vec<RawTgMessage> = chan_history.iter().cloned().collect();

        let ctx = ParseContext {
            market,
            llm,
            history: &history_slice,
        };

        // Route to parse_root or parse_reply based on reply_to.
        let action = if let Some(reply_to_id) = msg.reply_to {
            // Find the parent message in history by msg_id.
            let parent = chan_history.iter().find(|m| m.msg_id == reply_to_id);
            parser.parse_reply(&msg.message, ts, parent, &ctx)
        } else {
            parser.parse_root(&msg.message, ts, &ctx)
        };

        match action {
            ParsedAction::Signals(batch) => signals.extend(batch),
            ParsedAction::Skip => {}
        }

        // Push current message into history, enforce max_history cap.
        if max_hist > 0 {
            chan_history.push_back(msg.clone());
            while chan_history.len() > max_hist {
                chan_history.pop_front();
            }
        }
    }

    signals.sort_by_key(|s| s.ts());
    Ok(signals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::TemplateParser;

    fn make_registry() -> ParserRegistry {
        let mut reg = ParserRegistry::new();
        let parser = TemplateParser::new("test-channel", vec![100], 0.01, Some("test".into()));
        reg.register(Box::new(parser));
        reg
    }

    fn make_msg(chat_id: i64, msg_id: i64, ts: &str, message: &str) -> RawTgMessage {
        RawTgMessage {
            chat_id,
            msg_id,
            ts: ts.to_string(),
            message: message.to_string(),
            reply_to: None,
        }
    }

    fn make_reply(
        chat_id: i64,
        msg_id: i64,
        ts: &str,
        message: &str,
        reply_to: i64,
    ) -> RawTgMessage {
        RawTgMessage {
            chat_id,
            msg_id,
            ts: ts.to_string(),
            message: message.to_string(),
            reply_to: Some(reply_to),
        }
    }

    #[test]
    fn parse_messages_filters_unknown_channels() {
        let reg = make_registry();
        let messages = vec![make_msg(
            999,
            1,
            "2025-01-01T10:00:00Z",
            "EURUSD BUY NOW SL 1.0800 TP 1.0900",
        )];
        let result = parse_messages(&reg, &messages).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_messages_sorts_by_timestamp() {
        let reg = make_registry();
        let messages = vec![
            make_msg(
                100,
                1,
                "2025-01-03T12:00:00Z",
                "XAUUSD SELL LIMIT 2650 SL 2680 TP 2620",
            ),
            make_msg(
                100,
                2,
                "2025-01-01T08:00:00Z",
                "EURUSD BUY NOW SL 1.0800 TP 1.0900",
            ),
            make_msg(
                100,
                3,
                "2025-01-02T10:00:00Z",
                "GBPUSD BUY MARKET SL 1.2500 TP 1.2700",
            ),
        ];
        let result = parse_messages(&reg, &messages).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result[0].ts() < result[1].ts());
        assert!(result[1].ts() < result[2].ts());
        let entries: Vec<_> = result
            .into_iter()
            .filter_map(|s| if s.is_entry() { Some(s) } else { None })
            .collect();
        match &entries[0] {
            RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "eurusd"),
            _ => panic!("expected Entry"),
        }
        match &entries[1] {
            RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "gbpusd"),
            _ => panic!("expected Entry"),
        }
        match &entries[2] {
            RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "xauusd"),
            _ => panic!("expected Entry"),
        }
    }

    #[test]
    fn raw_tg_message_serde_roundtrip() {
        let msg = RawTgMessage {
            chat_id: 42,
            msg_id: 7,
            ts: "2025-06-01T09:30:00Z".to_string(),
            message: "EURUSD BUY NOW SL 1.08 TP 1.09".to_string(),
            reply_to: Some(5),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: RawTgMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.chat_id, 42);
        assert_eq!(deser.msg_id, 7);
        assert_eq!(deser.reply_to, Some(5));
        assert_eq!(deser.message, msg.message);
    }

    #[test]
    fn template_parser_skips_replies() {
        // TemplateParser.parse_reply always returns Skip.
        let reg = make_registry();
        let messages = vec![
            make_msg(
                100,
                1,
                "2025-01-01T10:00:00Z",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            ),
            make_reply(100, 2, "2025-01-01T11:00:00Z", "close this", 1),
        ];
        let result = parse_messages(&reg, &messages).unwrap();
        // Only the root signal is parsed; the reply is skipped.
        assert_eq!(result.len(), 1);
        match &result[0] {
            RawSignal::Entry { symbol, .. } => assert_eq!(symbol, "eurusd"),
            _ => panic!("expected Entry"),
        }
    }

    #[test]
    fn context_with_market_and_llm() {
        let reg = make_registry();
        let messages = vec![make_msg(
            100,
            1,
            "2025-01-01T10:00:00Z",
            "EURUSD BUY NOW SL 1.08 TP 1.09",
        )];
        let quote = MarketQuote {
            bid: 1.0850,
            ask: 1.0852,
            ts: chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
        };
        let llm = LlmClient;
        // Passing market/llm doesn't affect template parser (it ignores them).
        let result =
            parse_messages_with_context(&reg, &messages, Some(&quote), Some(&llm)).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn parse_messages_can_emit_reply_management_signal() {
        use crate::parser::ChannelParser;
        use chrono::NaiveDateTime;
        use qs_backtest::PositionRef;

        struct ReplyParser {
            channels: Vec<i64>,
        }

        impl ChannelParser for ReplyParser {
            fn name(&self) -> &str {
                "reply-parser"
            }

            fn channel_ids(&self) -> &[i64] {
                &self.channels
            }

            fn max_history(&self) -> usize {
                8
            }

            fn parse_root(
                &self,
                _message: &str,
                ts: NaiveDateTime,
                _ctx: &ParseContext,
            ) -> ParsedAction {
                ParsedAction::one(RawSignal::Entry {
                    ts,
                    symbol: "eurusd".into(),
                    side: qs_core::Side::Buy,
                    order_type: qs_core::OrderType::Market,
                    price: None,
                    size: 0.01,
                    stoploss: Some(1.08),
                    targets: vec![1.09],
                    group: Some("tg_test".into()),
                    trade_id: Some("tg_test-trade-1".into()),
                })
            }

            fn parse_reply(
                &self,
                message: &str,
                ts: NaiveDateTime,
                _parent: Option<&RawTgMessage>,
                _ctx: &ParseContext,
            ) -> ParsedAction {
                if message.eq_ignore_ascii_case("close this") {
                    ParsedAction::one(RawSignal::Close {
                        ts,
                        position: PositionRef::ByTradeId {
                            trade_id: "tg_test-trade-1".into(),
                        },
                    })
                } else {
                    ParsedAction::Skip
                }
            }
        }

        let mut reg = ParserRegistry::new();
        reg.register(Box::new(ReplyParser {
            channels: vec![777],
        }));
        let messages = vec![
            make_msg(777, 1, "2025-01-01T10:00:00Z", "entry"),
            make_reply(777, 2, "2025-01-01T10:05:00Z", "close this", 1),
        ];

        let result = parse_messages(&reg, &messages).unwrap();
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], RawSignal::Entry { .. }));
        assert!(matches!(result[1], RawSignal::Close { .. }));
    }
}
