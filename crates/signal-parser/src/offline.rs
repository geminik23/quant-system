use std::collections::{HashMap, VecDeque};
use std::io::{self, BufRead, Write};

use clap::Parser;
use qs_core::RawSignal;

use crate::error::SignalParserError;
use crate::handler::{SignalContext, SignalHandler};
use crate::parser::ChannelParser;
use crate::registry::ParserRegistry;
use crate::types::{ParseContext, ParsedAction, RawTgMessage};

//
// CLI args parsed internally by the runner.
//

#[derive(Parser)]
#[command(
    name = "offline-runner",
    about = "Parse raw Telegram JSONL into trade signals"
)]
struct OfflineCli {
    /// Path to the raw messages JSONL file (or "-" for stdin).
    #[arg(short, long)]
    input: String,

    /// Output file path (default: stdout). Ignored when a handler is set.
    #[arg(short, long)]
    output: Option<String>,

    /// Optional JSONL file receiving one structured parse outcome per message.
    #[arg(long)]
    outcomes_output: Option<String>,
}

//
// Programmatic args for run_with_args().
//

/// Arguments for the offline runner when invoked programmatically.
pub struct OfflineArgs {
    /// Path to the raw messages JSONL file (or "-" for stdin).
    pub input: String,
    /// Output file path. None means stdout. Ignored when a handler is set.
    pub output: Option<String>,
}

//
// OfflineRunner - offline signal parsing runner.
//

/// Reads raw Telegram JSONL, parses through registered channel parsers,
/// and either writes parsed signal JSONL to output or calls the provided
/// SignalHandler for each result.
pub struct OfflineRunner {
    registry: ParserRegistry,
    handler: Option<Box<dyn SignalHandler>>,
}

impl OfflineRunner {
    /// Create a new runner with the given parser registry.
    pub fn new(registry: ParserRegistry) -> Self {
        Self {
            registry,
            handler: None,
        }
    }

    /// Set a custom signal handler (called for each parsed result).
    /// If not set, the runner writes parsed signal JSONL to output.
    pub fn with_handler(mut self, handler: Box<dyn SignalHandler>) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Run the offline pipeline. Parses CLI args from the process arguments.
    pub fn run(self) -> Result<(), SignalParserError> {
        let cli = OfflineCli::parse();
        if let Some(outcomes_output) = cli.outcomes_output {
            self.run_with_args_and_outcomes(
                OfflineArgs {
                    input: cli.input,
                    output: cli.output,
                },
                outcomes_output,
            )
        } else {
            self.run_with_args(OfflineArgs {
                input: cli.input,
                output: cli.output,
            })
        }
    }

    pub fn run_with_args_and_outcomes(
        self,
        args: OfflineArgs,
        outcomes_output: String,
    ) -> Result<(), SignalParserError> {
        let messages = read_jsonl(&args.input)?;
        let batch = crate::pipeline::parse_messages_v2(&self.registry, &messages);
        write_jsonl_values(&args.output, &batch.signals)?;
        write_jsonl_values(&Some(outcomes_output), &batch.outcomes)?;
        Ok(())
    }

    /// Run the offline pipeline with pre-built arguments.
    pub fn run_with_args(self, args: OfflineArgs) -> Result<(), SignalParserError> {
        let messages = read_jsonl(&args.input)?;
        tracing::info!(count = messages.len(), "loaded raw messages");

        if let Some(handler) = &self.handler {
            let count = run_with_handler(&self.registry, &messages, handler.as_ref());
            tracing::info!(count, "processed signals via handler");
        } else {
            let signals = crate::pipeline::parse_messages(&self.registry, &messages)?;
            tracing::info!(count = signals.len(), "parsed raw signals");
            write_jsonl(&args.output, &signals)?;
        }

        Ok(())
    }
}

/// Process messages one by one through the registry and call handler callbacks.
/// Returns the total number of signal entries dispatched.
fn run_with_handler(
    registry: &ParserRegistry,
    messages: &[RawTgMessage],
    handler: &dyn SignalHandler,
) -> usize {
    let mut history: HashMap<i64, VecDeque<RawTgMessage>> = HashMap::new();
    let mut total = 0;
    let mut ordered: Vec<(usize, &RawTgMessage)> = messages.iter().enumerate().collect();
    ordered.sort_by_key(|(source_sequence, message)| {
        (message.chat_id, message.msg_id, *source_sequence)
    });

    for (_, msg) in ordered {
        let parser = match registry.get(msg.chat_id) {
            Some(p) => p,
            None => {
                handler.on_unregistered_channel(msg.chat_id, &msg.message);
                continue;
            }
        };

        let ts = match crate::pipeline::parse_iso_datetime(&msg.ts) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("skipping msg_id={}: {}", msg.msg_id, e);
                continue;
            }
        };

        let chan_history = history.entry(msg.chat_id).or_default();
        let history_slice: Vec<RawTgMessage> = chan_history.iter().cloned().collect();
        let ctx = ParseContext {
            market: None,
            llm: None,
            history: &history_slice,
        };

        let action = route_message(parser, msg, ts, chan_history, &ctx);

        let signal_ctx = SignalContext {
            chat_id: msg.chat_id,
            msg_id: msg.msg_id,
            ts,
            parser_name: parser.name().to_string(),
        };

        let retain_in_history = match action {
            ParsedAction::Signals(signals) => match crate::pipeline::validate_signals(&signals) {
                Ok(()) => {
                    total += signals.len();
                    handler.on_signals(signals, &signal_ctx);
                    true
                }
                Err(failure) => {
                    tracing::warn!(?failure, "signal parser emitted invalid signals");
                    handler.on_skip(&msg.message, &signal_ctx);
                    false
                }
            },
            ParsedAction::Skip => {
                handler.on_skip(&msg.message, &signal_ctx);
                true
            }
            ParsedAction::Rejected(failure) => {
                tracing::warn!(?failure, "signal parser rejected message");
                handler.on_skip(&msg.message, &signal_ctx);
                false
            }
        };

        // Push successful and intentionally skipped messages into history.
        let max_hist = parser.max_history();
        if retain_in_history && max_hist > 0 {
            chan_history.push_back(msg.clone());
            while chan_history.len() > max_hist {
                chan_history.pop_front();
            }
        }
    }

    total
}

/// Route a message to parse_root or parse_reply based on reply_to field.
fn route_message(
    parser: &dyn ChannelParser,
    msg: &RawTgMessage,
    ts: chrono::NaiveDateTime,
    chan_history: &VecDeque<RawTgMessage>,
    ctx: &ParseContext,
) -> ParsedAction {
    if let Some(reply_to_id) = msg.reply_to {
        let parent = chan_history.iter().find(|m| m.msg_id == reply_to_id);
        parser.parse_reply_message(msg, ts, parent, ctx)
    } else {
        parser.parse_root_message(msg, ts, ctx)
    }
}

/// Read raw messages from a JSONL file (or stdin if path is "-").
fn read_jsonl(path: &str) -> Result<Vec<RawTgMessage>, SignalParserError> {
    let reader: Box<dyn BufRead> = if path == "-" {
        Box::new(io::BufReader::new(io::stdin().lock()))
    } else {
        let file = std::fs::File::open(path)?;
        Box::new(io::BufReader::new(file))
    };

    let mut messages = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: RawTgMessage = serde_json::from_str(trimmed)
            .map_err(|e| SignalParserError::Config(format!("line {}: {e}", i + 1)))?;
        messages.push(msg);
    }

    Ok(messages)
}

/// Write parsed raw signals as JSONL to a file (or stdout if path is None).
fn write_jsonl(path: &Option<String>, signals: &[RawSignal]) -> Result<(), SignalParserError> {
    write_jsonl_values(path, signals)
}

fn write_jsonl_values<T: serde::Serialize>(
    path: &Option<String>,
    values: &[T],
) -> Result<(), SignalParserError> {
    let mut writer: Box<dyn Write> = match path {
        Some(p) => Box::new(io::BufWriter::new(std::fs::File::create(p)?)),
        None => Box::new(io::BufWriter::new(io::stdout().lock())),
    };

    for value in values {
        serde_json::to_writer(&mut writer, value)?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDateTime;
    use qs_core::PositionRef;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct CountingHandler {
        signal_count: Arc<AtomicUsize>,
        skip_count: Arc<AtomicUsize>,
        unregistered_count: Arc<AtomicUsize>,
    }

    impl SignalHandler for CountingHandler {
        fn on_signals(&self, signals: Vec<RawSignal>, _ctx: &SignalContext) {
            self.signal_count
                .fetch_add(signals.len(), Ordering::Relaxed);
        }
        fn on_signal_edit(&self, _signals: Vec<RawSignal>, _ctx: &SignalContext) {}
        fn on_signal_delete(&self, _chat_id: i64, _msg_ids: Vec<i64>) {}
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
            crate::template::TemplateParser::new("test-chan", vec![100], 1.0, Some("test".into()));
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

    type RecordedCalls = Arc<Mutex<Vec<(i64, Option<i64>, Vec<i64>)>>>;

    struct RecordingParser {
        channels: Vec<i64>,
        max_history: usize,
        calls: RecordedCalls,
    }

    impl ChannelParser for RecordingParser {
        fn name(&self) -> &str {
            "recording-handler"
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
            self.record_and_signal(ts, None, ctx)
        }

        fn parse_reply(
            &self,
            _message: &str,
            ts: NaiveDateTime,
            parent: Option<&RawTgMessage>,
            ctx: &ParseContext,
        ) -> ParsedAction {
            self.record_and_signal(ts, parent.map(|message| message.msg_id), ctx)
        }
    }

    impl RecordingParser {
        fn record_and_signal(
            &self,
            ts: NaiveDateTime,
            parent: Option<i64>,
            ctx: &ParseContext,
        ) -> ParsedAction {
            let current = ctx.current_message().expect("current source message");
            self.calls.lock().unwrap().push((
                current.msg_id,
                parent,
                ctx.history.iter().map(|message| message.msg_id).collect(),
            ));
            ParsedAction::one(RawSignal::Close {
                ts,
                position: PositionRef::ByTradeId {
                    trade_id: format!("offline-{}", current.msg_id),
                },
            })
        }
    }

    fn make_recording_registry(max_history: usize) -> (ParserRegistry, RecordedCalls) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ParserRegistry::new();
        registry.register(Box::new(RecordingParser {
            channels: vec![200],
            max_history,
            calls: Arc::clone(&calls),
        }));
        (registry, calls)
    }

    #[test]
    fn offline_runner_with_handler_calls_on_signals() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));
        let skips = Arc::new(AtomicUsize::new(0));
        let unreg = Arc::new(AtomicUsize::new(0));

        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::clone(&skips),
            unregistered_count: Arc::clone(&unreg),
        };

        let messages = vec![
            make_msg(
                100,
                1,
                "2025-01-01T10:00:00Z",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            ),
            make_msg(
                100,
                2,
                "2025-01-01T11:00:00Z",
                "GBPUSD SELL LIMIT 1.30 SL 1.32 TP 1.28",
            ),
        ];

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 2);
        assert_eq!(signals.load(Ordering::Relaxed), 2);
        assert_eq!(skips.load(Ordering::Relaxed), 0);
        assert_eq!(unreg.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn offline_runner_empty_input_no_crash() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));

        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::new(AtomicUsize::new(0)),
            unregistered_count: Arc::new(AtomicUsize::new(0)),
        };

        let messages: Vec<RawTgMessage> = vec![];
        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 0);
        assert_eq!(signals.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn offline_runner_unknown_channel_skipped() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));
        let unreg = Arc::new(AtomicUsize::new(0));

        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::new(AtomicUsize::new(0)),
            unregistered_count: Arc::clone(&unreg),
        };

        let messages = vec![make_msg(
            999,
            1,
            "2025-01-01T10:00:00Z",
            "EURUSD BUY NOW SL 1.08 TP 1.09",
        )];

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 0);
        assert_eq!(signals.load(Ordering::Relaxed), 0);
        assert_eq!(unreg.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn offline_runner_non_signal_calls_on_skip() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));
        let skips = Arc::new(AtomicUsize::new(0));

        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::clone(&skips),
            unregistered_count: Arc::new(AtomicUsize::new(0)),
        };

        let messages = vec![make_msg(
            100,
            1,
            "2025-01-01T10:00:00Z",
            "good morning traders",
        )];

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 0);
        assert_eq!(signals.load(Ordering::Relaxed), 0);
        assert_eq!(skips.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn offline_runner_handler_receives_correct_context() {
        use std::sync::Mutex;

        struct CtxCapture {
            contexts: Mutex<Vec<(i64, i64, String)>>,
        }

        impl SignalHandler for CtxCapture {
            fn on_signals(&self, _signals: Vec<RawSignal>, ctx: &SignalContext) {
                self.contexts.lock().unwrap().push((
                    ctx.chat_id,
                    ctx.msg_id,
                    ctx.parser_name.clone(),
                ));
            }
            fn on_signal_edit(&self, _signals: Vec<RawSignal>, _ctx: &SignalContext) {}
            fn on_signal_delete(&self, _chat_id: i64, _msg_ids: Vec<i64>) {}
        }

        let registry = make_registry();
        let handler = CtxCapture {
            contexts: Mutex::new(Vec::new()),
        };

        let messages = vec![make_msg(
            100,
            42,
            "2025-01-01T10:00:00Z",
            "EURUSD BUY NOW SL 1.08 TP 1.09",
        )];

        run_with_handler(&registry, &messages, &handler);

        let captured = handler.contexts.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0, 100);
        assert_eq!(captured[0].1, 42);
        assert_eq!(captured[0].2, "test-chan");
    }

    #[test]
    fn read_jsonl_from_file() {
        let dir = std::env::temp_dir().join("qs_offline_test_read");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.jsonl");

        let content = r#"{"chat_id":100,"msg_id":1,"ts":"2025-01-01T10:00:00Z","message":"EURUSD BUY NOW SL 1.08 TP 1.09","reply_to":null}
{"chat_id":100,"msg_id":2,"ts":"2025-01-01T11:00:00Z","message":"GBPUSD SELL NOW SL 1.30 TP 1.28","reply_to":null}
"#;
        std::fs::write(&path, content).unwrap();

        let messages = read_jsonl(path.to_str().unwrap()).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].msg_id, 1);
        assert_eq!(messages[1].msg_id, 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_jsonl_to_file() {
        use chrono::NaiveDate;
        use qs_core::{OrderType, Side};

        let dir = std::env::temp_dir().join("qs_offline_test_write");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.jsonl");

        let signals = vec![RawSignal::Entry {
            ts: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            symbol: "eurusd".to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: Some(1.08),
            targets: vec![1.09],
            group: Some("test".to_string()),
            trade_id: None,
        }];

        write_jsonl(&Some(path.to_str().unwrap().to_string()), &signals).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["action"], "Entry");
        assert_eq!(parsed["symbol"], "eurusd");
        assert_eq!(parsed["side"], "Buy");
        assert_eq!(parsed["risk"], 1.0);
        assert!(parsed.get("size").is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn offline_runner_mixed_signals_and_skips() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));
        let skips = Arc::new(AtomicUsize::new(0));
        let unreg = Arc::new(AtomicUsize::new(0));

        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::clone(&skips),
            unregistered_count: Arc::clone(&unreg),
        };

        let messages = vec![
            make_msg(
                100,
                1,
                "2025-01-01T10:00:00Z",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            ),
            make_msg(100, 2, "2025-01-01T10:05:00Z", "good morning"),
            make_msg(
                999,
                3,
                "2025-01-01T10:10:00Z",
                "GBPUSD SELL NOW SL 1.30 TP 1.28",
            ),
            make_msg(
                100,
                4,
                "2025-01-01T10:15:00Z",
                "XAUUSD BUY NOW SL 2600 TP 2700",
            ),
        ];

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 2);
        assert_eq!(signals.load(Ordering::Relaxed), 2);
        assert_eq!(skips.load(Ordering::Relaxed), 1);
        assert_eq!(unreg.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handler_mode_invokes_parser_with_missing_parent() {
        let (registry, calls) = make_recording_registry(2);
        let signals = Arc::new(AtomicUsize::new(0));
        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::new(AtomicUsize::new(0)),
            unregistered_count: Arc::new(AtomicUsize::new(0)),
        };
        let mut reply = make_msg(200, 2, "2025-01-01T10:00:00Z", "close");
        reply.reply_to = Some(999);

        let count = run_with_handler(&registry, &[reply], &handler);

        assert_eq!(count, 1);
        assert_eq!(signals.load(Ordering::Relaxed), 1);
        assert_eq!(*calls.lock().unwrap(), vec![(2, None, vec![])]);
    }

    #[test]
    fn handler_mode_continues_after_invalid_timestamp() {
        let registry = make_registry();
        let signals = Arc::new(AtomicUsize::new(0));
        let handler = CountingHandler {
            signal_count: Arc::clone(&signals),
            skip_count: Arc::new(AtomicUsize::new(0)),
            unregistered_count: Arc::new(AtomicUsize::new(0)),
        };
        let messages = vec![
            make_msg(100, 1, "invalid", "broken"),
            make_msg(
                100,
                2,
                "2025-01-01T10:00:00Z",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            ),
        ];

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 1);
        assert_eq!(signals.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handler_mode_retains_bounded_history() {
        let (registry, calls) = make_recording_registry(2);
        let handler = CountingHandler {
            signal_count: Arc::new(AtomicUsize::new(0)),
            skip_count: Arc::new(AtomicUsize::new(0)),
            unregistered_count: Arc::new(AtomicUsize::new(0)),
        };
        let messages: Vec<_> = (1..=4)
            .map(|msg_id| {
                make_msg(
                    200,
                    msg_id,
                    "2025-01-01T10:00:00Z",
                    &format!("message-{msg_id}"),
                )
            })
            .collect();

        let count = run_with_handler(&registry, &messages, &handler);

        assert_eq!(count, 4);
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                (1, None, vec![]),
                (2, None, vec![1]),
                (3, None, vec![1, 2]),
                (4, None, vec![2, 3]),
            ]
        );
    }
}
