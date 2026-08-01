use std::hint::black_box;
use std::time::{Duration, Instant};

use chrono::NaiveDateTime;
use signal_parser::{
    ChannelParser, ParseContext, ParsedAction, ParserRegistry, PositionRef, RawSignal,
    RawTgMessage, TemplateParser, parse_messages_v2,
};

struct HistoryParser {
    channels: Vec<i64>,
    maximum_history: usize,
}

struct FailureParser {
    channels: Vec<i64>,
}

impl ChannelParser for FailureParser {
    fn name(&self) -> &str {
        "failure-baseline"
    }

    fn channel_ids(&self) -> &[i64] {
        &self.channels
    }

    fn max_history(&self) -> usize {
        8
    }

    fn parse_root(&self, message: &str, ts: NaiveDateTime, _ctx: &ParseContext) -> ParsedAction {
        if message == "invalid-signal" {
            ParsedAction::one(RawSignal::ScaleIn {
                ts,
                position: PositionRef::ByTradeId {
                    trade_id: "missing-trade".into(),
                },
                price: None,
                size: 0.0,
            })
        } else {
            ParsedAction::Skip
        }
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

impl ChannelParser for HistoryParser {
    fn name(&self) -> &str {
        "history-baseline"
    }

    fn channel_ids(&self) -> &[i64] {
        &self.channels
    }

    fn max_history(&self) -> usize {
        self.maximum_history
    }

    fn parse_root(&self, _message: &str, _ts: NaiveDateTime, ctx: &ParseContext) -> ParsedAction {
        black_box(ctx.history.len());
        ParsedAction::Skip
    }

    fn parse_reply(
        &self,
        _message: &str,
        _ts: NaiveDateTime,
        _parent: Option<&RawTgMessage>,
        ctx: &ParseContext,
    ) -> ParsedAction {
        black_box(ctx.history.len());
        ParsedAction::Skip
    }
}

fn message(chat_id: i64, msg_id: i64, timestamp: &str, body: &str) -> RawTgMessage {
    RawTgMessage {
        chat_id,
        msg_id,
        ts: timestamp.into(),
        message: body.into(),
        reply_to: None,
    }
}

fn template_registry() -> ParserRegistry {
    let mut registry = ParserRegistry::new();
    registry.register(Box::new(TemplateParser::new(
        "baseline-template",
        vec![1, 2, 3, 4],
        1.0,
        Some("baseline".into()),
    )));
    registry
}

fn valid_messages(count: usize, channels: i64) -> Vec<RawTgMessage> {
    (0..count)
        .map(|index| {
            message(
                index as i64 % channels + 1,
                index as i64 + 1,
                "2026-01-01T10:00:00Z",
                "EURUSD BUY NOW SL 1.08 TP 1.09",
            )
        })
        .collect()
}

fn failure_messages(count: usize) -> Vec<RawTgMessage> {
    (0..count)
        .map(|index| match index % 5 {
            0 => message(1, index as i64 + 1, "invalid", "broken"),
            1 => message(1, index as i64 + 1, "2026-01-01T10:00:00Z", "hello"),
            2 => message(99, index as i64 + 1, "2026-01-01T10:00:00Z", "unregistered"),
            3 => {
                let mut value = message(
                    8,
                    index as i64 + 1,
                    "2026-01-01T10:00:00Z",
                    "missing-parent",
                );
                value.reply_to = Some(-1);
                value
            }
            _ => message(
                8,
                index as i64 + 1,
                "2026-01-01T10:00:00Z",
                "invalid-signal",
            ),
        })
        .collect()
}

fn failure_registry() -> ParserRegistry {
    let mut registry = template_registry();
    registry.register(Box::new(FailureParser { channels: vec![8] }));
    registry
}

fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn measure(label: &str, records: usize, mut operation: impl FnMut()) -> Duration {
    let started = Instant::now();
    operation();
    let elapsed = started.elapsed();
    let throughput = records as f64 / elapsed.as_secs_f64();
    println!(
        "{label}: records={records} elapsed_ms={} records_per_second={throughput:.0}",
        elapsed.as_millis()
    );
    elapsed
}

fn main() {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "optimized"
    };
    println!(
        "environment: os={} arch={} profile={profile}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );

    let registry = template_registry();
    let small = valid_messages(1_000, 1);
    measure("small_batch", small.len(), || {
        black_box(parse_messages_v2(&registry, black_box(&small)));
    });

    let large = valid_messages(100_000, 4);
    measure("large_batch", large.len(), || {
        black_box(parse_messages_v2(&registry, black_box(&large)));
    });

    let mut history_registry = ParserRegistry::new();
    history_registry.register(Box::new(HistoryParser {
        channels: vec![7],
        maximum_history: 64,
    }));
    let history = valid_messages(100_000, 1)
        .into_iter()
        .enumerate()
        .map(|(index, mut message)| {
            message.chat_id = 7;
            message.message = format!("history-{index}");
            if index > 0 {
                message.reply_to = Some(message.msg_id - 1);
            }
            message
        })
        .collect::<Vec<_>>();
    measure("history_workload", history.len(), || {
        black_box(parse_messages_v2(&history_registry, black_box(&history)));
    });

    let failure_registry = failure_registry();
    let failures = failure_messages(100_000);
    measure("failure_workload", failures.len(), || {
        black_box(parse_messages_v2(&failure_registry, black_box(&failures)));
    });

    match peak_rss_kib() {
        Some(value) => println!("peak_rss_kib={value}"),
        None => println!("peak_rss_kib=unavailable"),
    }
}
