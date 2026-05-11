use std::collections::{HashMap, VecDeque};
use std::io::{self, BufRead, Write};

use clap::Parser;
use qs_backtest::RawSignalEntry;

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
        self.run_with_args(OfflineArgs {
            input: cli.input,
            output: cli.output,
        })
    }

    /// Run the offline pipeline with pre-built arguments.
    pub fn run_with_args(self, args: OfflineArgs) -> Result<(), SignalParserError> {
        let messages = read_jsonl(&args.input)?;
        tracing::info!(count = messages.len(), "loaded raw messages");

        if let Some(handler) = &self.handler {
            let count = run_with_handler(&self.registry, &messages, handler.as_ref());
            tracing::info!(count, "processed signals via handler");
        } else {
            let entries = crate::pipeline::parse_messages(&self.registry, &messages)?;
            tracing::info!(count = entries.len(), "parsed signal entries");
            write_jsonl(&args.output, &entries)?;
        }

        Ok(())
    }
}

/// Try multiple ISO 8601 datetime formats (same as pipeline.rs).
fn parse_iso_datetime(s: &str) -> Result<chrono::NaiveDateTime, SignalParserError> {
    for fmt in [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt);
        }
    }
    Err(SignalParserError::TimestampParse(
        s.to_string(),
        "unrecognized format".to_string(),
    ))
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

    for msg in messages {
        let parser = match registry.get(msg.chat_id) {
            Some(p) => p,
            None => {
                handler.on_unregistered_channel(msg.chat_id, &msg.message);
                continue;
            }
        };

        let ts = match parse_iso_datetime(&msg.ts) {
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

        match action {
            ParsedAction::Entries(entries) => {
                total += entries.len();
                handler.on_signals(entries, &signal_ctx);
            }
            ParsedAction::Skip => {
                handler.on_skip(&msg.message, &signal_ctx);
            }
        }

        // Push current message into history.
        let max_hist = parser.max_history();
        if max_hist > 0 {
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
        parser.parse_reply(&msg.message, ts, parent, ctx)
    } else {
        parser.parse_root(&msg.message, ts, ctx)
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

/// Write parsed signals as JSONL to a file (or stdout if path is None).
fn write_jsonl(path: &Option<String>, entries: &[RawSignalEntry]) -> Result<(), SignalParserError> {
    let mut writer: Box<dyn Write> = match path {
        Some(p) => Box::new(io::BufWriter::new(std::fs::File::create(p)?)),
        None => Box::new(io::BufWriter::new(io::stdout().lock())),
    };

    for entry in entries {
        serde_json::to_writer(&mut writer, entry)?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingHandler {
        signal_count: Arc<AtomicUsize>,
        skip_count: Arc<AtomicUsize>,
        unregistered_count: Arc<AtomicUsize>,
    }

    impl SignalHandler for CountingHandler {
        fn on_signals(&self, entries: Vec<RawSignalEntry>, _ctx: &SignalContext) {
            self.signal_count
                .fetch_add(entries.len(), Ordering::Relaxed);
        }
        fn on_signal_edit(&self, _entries: Vec<RawSignalEntry>, _ctx: &SignalContext) {}
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
            crate::template::TemplateParser::new("test-chan", vec![100], 0.01, Some("test".into()));
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
            fn on_signals(&self, _entries: Vec<RawSignalEntry>, ctx: &SignalContext) {
                self.contexts.lock().unwrap().push((
                    ctx.chat_id,
                    ctx.msg_id,
                    ctx.parser_name.clone(),
                ));
            }
            fn on_signal_edit(&self, _entries: Vec<RawSignalEntry>, _ctx: &SignalContext) {}
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

        let entries = vec![RawSignalEntry {
            ts: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            symbol: "eurusd".to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            size: 0.01,
            stoploss: Some(1.08),
            targets: vec![1.09],
            group: Some("test".to_string()),
        }];

        write_jsonl(&Some(path.to_str().unwrap().to_string()), &entries).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["symbol"], "eurusd");
        assert_eq!(parsed["side"], "Buy");

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
}
