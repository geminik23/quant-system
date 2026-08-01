use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, NaiveDateTime};
use qs_core::RawSignal;

use crate::error::SignalParserError;
use crate::registry::ParserRegistry;
use crate::types::{
    LlmClient, MarketQuote, MessageParseOutcome, ParseBatchResult, ParseContext, ParseFailure,
    ParsedAction, RawTgMessage, SkipReason,
};

/// Parse RFC 3339 timestamps (normalizing offsets to UTC), then legacy naive formats.
pub(crate) fn parse_iso_datetime(s: &str) -> Result<NaiveDateTime, SignalParserError> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.naive_utc());
    }

    for fmt in [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
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
/// This compatibility wrapper preserves the original API and error behavior.
/// Use [`parse_messages_v2`] to retain per-message skips and failures.
pub fn parse_messages(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
) -> Result<Vec<RawSignal>, SignalParserError> {
    parse_messages_with_context(registry, messages, None, None)
}

/// Full-context compatibility wrapper for [`parse_messages_with_context_v2`].
pub fn parse_messages_with_context(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
    market: Option<&MarketQuote>,
    llm: Option<&LlmClient>,
) -> Result<Vec<RawSignal>, SignalParserError> {
    into_legacy_result(parse_messages_impl(
        registry,
        messages,
        market,
        llm,
        ParseMode::Compatibility,
    ))
}

/// Parse messages and return one structured outcome per input message.
///
/// Outcomes retain input order even though messages are processed in
/// deterministic Telegram source order for history resolution. Successful
/// signals are collected and sorted by timestamp. Unlike the compatibility
/// API, one malformed message does not prevent later messages from being
/// parsed.
pub fn parse_messages_v2(registry: &ParserRegistry, messages: &[RawTgMessage]) -> ParseBatchResult {
    parse_messages_with_context_v2(registry, messages, None, None)
}

/// Full-context V2 parser with structured per-message diagnostics.
pub fn parse_messages_with_context_v2(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
    market: Option<&MarketQuote>,
    llm: Option<&LlmClient>,
) -> ParseBatchResult {
    parse_messages_impl(registry, messages, market, llm, ParseMode::V2)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParseMode {
    Compatibility,
    V2,
}

fn parse_messages_impl(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
    market: Option<&MarketQuote>,
    llm: Option<&LlmClient>,
    mode: ParseMode,
) -> ParseBatchResult {
    let mut result = ParseBatchResult {
        outcomes: Vec::new(),
        signals: Vec::new(),
    };
    let mut outcomes: Vec<Option<MessageParseOutcome>> = std::iter::repeat_with(|| None)
        .take(messages.len())
        .collect();
    let mut history: HashMap<i64, VecDeque<RawTgMessage>> = HashMap::new();
    let mut ordered: Vec<(usize, &RawTgMessage)> = messages.iter().enumerate().collect();
    ordered.sort_by_key(|(source_sequence, message)| {
        (message.chat_id, message.msg_id, *source_sequence)
    });

    for (source_index, msg) in ordered {
        let parser = match registry.get(msg.chat_id) {
            Some(parser) => parser,
            None => {
                outcomes[source_index] = Some(MessageParseOutcome::Skipped {
                    source: msg.clone(),
                    parser: None,
                    reason: SkipReason::UnregisteredChannel,
                });
                continue;
            }
        };
        let parser_name = parser.name().to_string();

        let ts = match parse_iso_datetime(&msg.ts) {
            Ok(ts) => ts,
            Err(SignalParserError::TimestampParse(value, reason)) => {
                outcomes[source_index] = Some(MessageParseOutcome::Failed {
                    source: msg.clone(),
                    parser: Some(parser_name),
                    failure: ParseFailure::InvalidTimestamp { value, reason },
                });
                if mode == ParseMode::V2 {
                    continue;
                }
                break;
            }
            Err(error) => {
                outcomes[source_index] = Some(MessageParseOutcome::Failed {
                    source: msg.clone(),
                    parser: Some(parser_name),
                    failure: ParseFailure::Parser {
                        reason: error.to_string(),
                    },
                });
                if mode == ParseMode::V2 {
                    continue;
                }
                break;
            }
        };

        let max_hist = parser.max_history();
        let chan_history = history.entry(msg.chat_id).or_default();
        let history_slice: Vec<RawTgMessage> = chan_history.iter().cloned().collect();
        let ctx = ParseContext {
            market,
            llm,
            history: &history_slice,
        };

        let action = if let Some(reply_to_id) = msg.reply_to {
            let parent = chan_history
                .iter()
                .find(|candidate| candidate.msg_id == reply_to_id);
            if mode == ParseMode::V2 && parent.is_none() {
                ParsedAction::Rejected(ParseFailure::MissingParent {
                    reply_to: reply_to_id,
                })
            } else {
                parser.parse_reply_message(msg, ts, parent, &ctx)
            }
        } else {
            parser.parse_root_message(msg, ts, &ctx)
        };

        let outcome = match action {
            ParsedAction::Signals(signals) => match validate_signals(&signals) {
                Ok(()) => {
                    result.signals.extend(signals.iter().cloned());
                    MessageParseOutcome::Parsed {
                        source: msg.clone(),
                        parser: parser_name,
                        signals,
                    }
                }
                Err(failure) => MessageParseOutcome::Failed {
                    source: msg.clone(),
                    parser: Some(parser_name),
                    failure,
                },
            },
            ParsedAction::Skip => MessageParseOutcome::Skipped {
                source: msg.clone(),
                parser: Some(parser_name),
                reason: SkipReason::ParserReturnedSkip,
            },
            ParsedAction::Rejected(failure) => MessageParseOutcome::Failed {
                source: msg.clone(),
                parser: Some(parser_name),
                failure,
            },
        };
        let retain_in_history = !matches!(&outcome, MessageParseOutcome::Failed { .. });
        outcomes[source_index] = Some(outcome);

        if retain_in_history && max_hist > 0 {
            chan_history.push_back(msg.clone());
            while chan_history.len() > max_hist {
                chan_history.pop_front();
            }
        }
    }

    result.outcomes = outcomes.into_iter().flatten().collect();
    result.signals.sort_by_key(|signal| signal.ts());
    result
}

/// Validate emitted signals against the shared `qs-core` contract.
///
/// The rules and their exact wording live in `qs_core::validation`, which owns
/// `RawSignal`. This wrapper only adapts the error into `ParseFailure` so the
/// outcome JSONL shape is unchanged; the reason strings are reproduced verbatim
/// and committed goldens stay byte-identical.
pub(crate) fn validate_signals(signals: &[RawSignal]) -> Result<(), ParseFailure> {
    qs_core::validate_raw_signals(signals).map_err(|error| ParseFailure::InvalidSignal {
        reason: error.to_string(),
    })
}

fn into_legacy_result(result: ParseBatchResult) -> Result<Vec<RawSignal>, SignalParserError> {
    for outcome in &result.outcomes {
        if let MessageParseOutcome::Failed {
            source,
            parser,
            failure,
        } = outcome
        {
            return Err(match failure {
                ParseFailure::InvalidTimestamp { value, reason } => {
                    SignalParserError::TimestampParse(value.clone(), reason.clone())
                }
                ParseFailure::Parser { reason }
                | ParseFailure::InvalidSignal { reason }
                | ParseFailure::AmbiguousIdentity { reason } => SignalParserError::ParseError {
                    parser: parser.clone().unwrap_or_else(|| "unknown".to_string()),
                    msg_id: source.msg_id,
                    reason: reason.clone(),
                },
                ParseFailure::MissingParent { reply_to } => SignalParserError::ParseError {
                    parser: parser.clone().unwrap_or_else(|| "unknown".to_string()),
                    msg_id: source.msg_id,
                    reason: format!("reply parent {reply_to} is unavailable"),
                },
            });
        }
    }

    Ok(result.signals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ChannelParser;
    use crate::template::TemplateParser;
    use qs_core::PositionRef;
    use qs_core::types::{OrderType, Side};
    use std::sync::{Arc, Mutex};

    fn make_registry() -> ParserRegistry {
        let mut reg = ParserRegistry::new();
        let parser = TemplateParser::new("test-channel", vec![100], 1.0, Some("test".into()));
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

    type RecordedCalls = Arc<Mutex<Vec<(i64, Vec<i64>)>>>;

    struct RecordingParser {
        channels: Vec<i64>,
        max_history: usize,
        signals_per_message: usize,
        calls: RecordedCalls,
    }

    impl ChannelParser for RecordingParser {
        fn name(&self) -> &str {
            "recording-parser"
        }

        fn channel_ids(&self) -> &[i64] {
            &self.channels
        }

        fn max_history(&self) -> usize {
            self.max_history
        }

        fn parse_root(
            &self,
            _message: &str,
            ts: NaiveDateTime,
            ctx: &ParseContext,
        ) -> ParsedAction {
            let current = ctx.current_message().expect("current source message");
            self.calls.lock().unwrap().push((
                current.msg_id,
                ctx.history.iter().map(|message| message.msg_id).collect(),
            ));
            ParsedAction::signals(
                (0..self.signals_per_message)
                    .map(|index| RawSignal::Close {
                        ts,
                        position: PositionRef::ByTradeId {
                            trade_id: format!("msg-{}-{index}", current.msg_id),
                        },
                    })
                    .collect(),
            )
        }

        fn parse_reply(
            &self,
            message: &str,
            ts: NaiveDateTime,
            _parent: Option<&RawTgMessage>,
            ctx: &ParseContext,
        ) -> ParsedAction {
            self.parse_root(message, ts, ctx)
        }
    }

    fn make_recording_registry(
        max_history: usize,
        signals_per_message: usize,
    ) -> (ParserRegistry, RecordedCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ParserRegistry::new();
        registry.register(Box::new(RecordingParser {
            channels: vec![800],
            max_history,
            signals_per_message,
            calls: Arc::clone(&calls),
        }));
        (registry, calls)
    }

    fn close_trade_ids(signals: &[RawSignal]) -> Vec<&str> {
        signals
            .iter()
            .map(|signal| match signal {
                RawSignal::Close {
                    position: PositionRef::ByTradeId { trade_id },
                    ..
                } => trade_id.as_str(),
                _ => panic!("expected close signal"),
            })
            .collect()
    }

    struct ParentRequiredParser {
        channels: Vec<i64>,
    }

    impl crate::parser::ChannelParser for ParentRequiredParser {
        fn name(&self) -> &str {
            "parent-required"
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
            _ts: NaiveDateTime,
            _ctx: &ParseContext,
        ) -> ParsedAction {
            ParsedAction::Skip
        }

        fn parse_reply(
            &self,
            _message: &str,
            ts: NaiveDateTime,
            parent: Option<&RawTgMessage>,
            _ctx: &ParseContext,
        ) -> ParsedAction {
            assert_eq!(parent.map(|message| message.msg_id), Some(1));
            ParsedAction::one(RawSignal::Close {
                ts,
                position: qs_core::PositionRef::ByTradeId {
                    trade_id: "parent-trade".into(),
                },
            })
        }
    }

    fn make_parent_required_registry() -> ParserRegistry {
        let mut registry = ParserRegistry::new();
        registry.register(Box::new(ParentRequiredParser {
            channels: vec![700],
        }));
        registry
    }

    #[test]
    fn validate_signals_rejects_invalid_entry_risk_multiplier() {
        for risk_multiplier in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let signal = RawSignal::Entry {
                ts: parse_iso_datetime("2025-01-01T10:00:00Z").unwrap(),
                symbol: "eurusd".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                risk_multiplier,
                stoploss: Some(1.08),
                targets: vec![1.09],
                group: None,
                trade_id: None,
            };

            assert!(matches!(
                validate_signals(&[signal]),
                Err(ParseFailure::InvalidSignal { reason })
                    if reason.contains("entry risk multiplier")
            ));
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
        // TemplateParser retains no history, but compatibility parsing still
        // lets it classify replies as intentional skips.
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
    fn v2_reports_each_outcome_and_continues_after_failures() {
        let reg = make_registry();
        let messages = vec![
            make_msg(999, 1, "not-a-timestamp", "ignored"),
            make_msg(100, 2, "not-a-timestamp", "broken"),
            make_msg(100, 3, "2025-01-01T10:00:00+02:00", "good morning"),
            make_msg(
                100,
                4,
                "2025-01-01T10:00:00+02:00",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            ),
        ];

        let result = parse_messages_v2(&reg, &messages);

        assert_eq!(result.outcomes.len(), messages.len());
        assert_eq!(result.signals.len(), 1);
        assert!(result.has_failures());
        assert!(matches!(
            &result.outcomes[0],
            MessageParseOutcome::Skipped {
                reason: SkipReason::UnregisteredChannel,
                parser: None,
                ..
            }
        ));
        assert!(matches!(
            &result.outcomes[1],
            MessageParseOutcome::Failed {
                source,
                parser: Some(parser),
                failure: ParseFailure::InvalidTimestamp { value, .. },
            } if source.msg_id == 2 && parser == "test-channel" && value == "not-a-timestamp"
        ));
        assert!(matches!(
            &result.outcomes[2],
            MessageParseOutcome::Skipped {
                reason: SkipReason::ParserReturnedSkip,
                ..
            }
        ));
        assert!(matches!(
            &result.outcomes[3],
            MessageParseOutcome::Parsed { source, signals, .. }
                if source.chat_id == 100 && source.msg_id == 4 && signals.len() == 1
        ));

        let expected = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();
        assert_eq!(result.signals[0].ts(), expected);
    }

    #[test]
    fn v2_preserves_input_outcome_order_while_resolving_sorted_history() {
        let reg = make_parent_required_registry();
        let messages = vec![
            make_reply(700, 2, "2025-01-01T10:05:00Z", "close", 1),
            make_msg(700, 1, "2025-01-01T10:00:00Z", "entry"),
        ];

        let result = parse_messages_v2(&reg, &messages);

        assert_eq!(result.outcomes.len(), 2);
        assert!(matches!(
            &result.outcomes[0],
            MessageParseOutcome::Parsed { source, .. } if source.msg_id == 2
        ));
        assert!(matches!(
            &result.outcomes[1],
            MessageParseOutcome::Skipped { source, .. } if source.msg_id == 1
        ));
        assert!(matches!(
            result.signals.as_slice(),
            [RawSignal::Close { .. }]
        ));
    }

    #[test]
    fn v2_rejects_missing_parent_before_invoking_parent_required_parser() {
        let reg = make_parent_required_registry();
        let messages = vec![make_reply(700, 2, "2025-01-01T10:05:00Z", "close", 1)];

        let result = parse_messages_v2(&reg, &messages);

        assert!(matches!(
            result.outcomes.as_slice(),
            [MessageParseOutcome::Failed {
                source,
                parser: Some(parser),
                failure: ParseFailure::MissingParent { reply_to: 1 },
            }] if source.msg_id == 2 && parser == "parent-required"
        ));
        assert!(result.signals.is_empty());
    }

    #[test]
    fn legacy_api_preserves_timestamp_error_behavior() {
        let reg = make_registry();
        let messages = vec![make_msg(100, 7, "invalid", "good morning")];

        assert!(matches!(
            parse_messages(&reg, &messages),
            Err(SignalParserError::TimestampParse(value, _)) if value == "invalid"
        ));
    }

    #[test]
    fn parser_can_read_current_source_identity_without_context_field_changes() {
        use crate::parser::ChannelParser;

        struct IdentityParser {
            channels: Vec<i64>,
        }

        impl ChannelParser for IdentityParser {
            fn name(&self) -> &str {
                "identity-parser"
            }

            fn channel_ids(&self) -> &[i64] {
                &self.channels
            }

            fn parse_root(
                &self,
                _message: &str,
                ts: NaiveDateTime,
                ctx: &ParseContext,
            ) -> ParsedAction {
                let current = ctx.current_message().expect("current source message");
                ParsedAction::one(RawSignal::Entry {
                    ts,
                    symbol: "eurusd".into(),
                    side: qs_core::Side::Buy,
                    order_type: qs_core::OrderType::Market,
                    price: None,
                    risk_multiplier: 1.0,
                    stoploss: None,
                    targets: vec![],
                    group: None,
                    trade_id: Some(format!("tg:{}:{}", current.chat_id, current.msg_id)),
                })
            }

            fn parse_reply(
                &self,
                _message: &str,
                _ts: NaiveDateTime,
                _parent: Option<&RawTgMessage>,
                _ctx: &ParseContext,
            ) -> ParsedAction {
                ParsedAction::Skip
            }
        }

        let mut reg = ParserRegistry::new();
        reg.register(Box::new(IdentityParser {
            channels: vec![321],
        }));
        let result =
            parse_messages(&reg, &[make_msg(321, 654, "2025-01-01T00:00:00Z", "entry")]).unwrap();

        match &result[0] {
            RawSignal::Entry { trade_id, .. } => {
                assert_eq!(trade_id.as_deref(), Some("tg:321:654"));
            }
            _ => panic!("expected entry"),
        }
        assert!(ParseContext::empty().current_message().is_none());
    }

    #[test]
    fn parse_messages_can_emit_reply_management_signal() {
        use chrono::NaiveDateTime;

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
                    risk_multiplier: 1.0,
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

    #[test]
    fn duplicate_source_messages_are_accepted() {
        let (registry, calls) = make_recording_registry(2, 1);
        let messages = vec![
            make_msg(800, 1, "2025-01-01T10:00:00Z", "first"),
            make_msg(800, 1, "2025-01-01T10:00:01Z", "duplicate"),
        ];

        let result = parse_messages_v2(&registry, &messages);

        assert_eq!(result.outcomes.len(), 2);
        assert_eq!(result.signals.len(), 2);
        assert_eq!(*calls.lock().unwrap(), vec![(1, vec![]), (1, vec![1])]);
    }

    #[test]
    fn compatibility_stops_at_invalid_timestamp_before_later_sorted_messages() {
        let (registry, calls) = make_recording_registry(4, 1);
        let messages = vec![
            make_msg(800, 3, "2025-01-01T10:00:03Z", "later"),
            make_msg(800, 2, "invalid", "broken"),
            make_msg(800, 1, "2025-01-01T10:00:01Z", "earlier"),
        ];

        assert!(matches!(
            parse_messages(&registry, &messages),
            Err(SignalParserError::TimestampParse(value, _)) if value == "invalid"
        ));
        assert_eq!(*calls.lock().unwrap(), vec![(1, vec![])]);
    }

    #[test]
    fn v2_continues_in_input_outcome_order_and_stably_sorts_equal_timestamp_signals() {
        let (registry, _) = make_recording_registry(4, 1);
        let messages = vec![
            make_msg(800, 3, "2025-01-01T10:00:00Z", "third"),
            make_msg(800, 4, "invalid", "broken"),
            make_msg(800, 1, "2025-01-01T10:00:00Z", "first"),
            make_msg(800, 2, "2025-01-01T10:00:00Z", "second"),
        ];

        let result = parse_messages_v2(&registry, &messages);

        let outcome_ids: Vec<_> = result
            .outcomes
            .iter()
            .map(|outcome| outcome.source().msg_id)
            .collect();
        assert_eq!(outcome_ids, vec![3, 4, 1, 2]);
        assert!(matches!(
            &result.outcomes[1],
            MessageParseOutcome::Failed {
                failure: ParseFailure::InvalidTimestamp { .. },
                ..
            }
        ));
        assert_eq!(
            close_trade_ids(&result.signals),
            vec!["msg-1-0", "msg-2-0", "msg-3-0"]
        );
    }

    #[test]
    fn v2_retains_bounded_history() {
        let (registry, calls) = make_recording_registry(2, 1);
        let messages: Vec<_> = (1..=4)
            .map(|msg_id| {
                make_msg(
                    800,
                    msg_id,
                    "2025-01-01T10:00:00Z",
                    &format!("message-{msg_id}"),
                )
            })
            .collect();

        let result = parse_messages_v2(&registry, &messages);

        assert_eq!(result.signals.len(), 4);
        assert_eq!(
            *calls.lock().unwrap(),
            vec![(1, vec![]), (2, vec![1]), (3, vec![1, 2]), (4, vec![2, 3]),]
        );
    }

    #[test]
    fn v2_retains_multiple_signals_from_one_message() {
        let (registry, _) = make_recording_registry(1, 2);

        let result = parse_messages_v2(
            &registry,
            &[make_msg(800, 9, "2025-01-01T10:00:00Z", "two")],
        );

        assert_eq!(result.signals.len(), 2);
        assert_eq!(close_trade_ids(&result.signals), vec!["msg-9-0", "msg-9-1"]);
        assert!(matches!(
            result.outcomes.as_slice(),
            [MessageParseOutcome::Parsed { signals, .. }] if signals.len() == 2
        ));
    }
}
