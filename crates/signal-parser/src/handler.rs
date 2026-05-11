use chrono::NaiveDateTime;
use qs_backtest::RawSignalEntry;

//
// Context passed to signal handler callbacks.
//

/// Metadata about the message that produced the parsed signals.
pub struct SignalContext {
    /// Channel ID the message came from.
    pub chat_id: i64,
    /// Original Telegram message ID.
    pub msg_id: i64,
    /// Timestamp of the message.
    pub ts: NaiveDateTime,
    /// Parser name that produced the signals.
    pub parser_name: String,
}

//
// Callback trait for post-parse signal handling.
//

/// Third-party implements this trait to define what happens after parsing.
/// The framework (OfflineRunner / OnlineServer) parses messages and calls
/// these methods with the results.
pub trait SignalHandler: Send + Sync {
    /// Called when a NEW message produces one or more entry signals.
    fn on_signals(&self, entries: Vec<RawSignalEntry>, ctx: &SignalContext);

    /// Called when an EDIT message produces updated signals.
    fn on_signal_edit(&self, entries: Vec<RawSignalEntry>, ctx: &SignalContext);

    /// Called when a DELETE event is received.
    /// The framework cannot parse deleted messages (text is gone).
    /// The handler decides what to do - close positions, cancel orders, etc.
    fn on_signal_delete(&self, chat_id: i64, msg_ids: Vec<i64>);

    /// Called when a message is received but the parser returns Skip.
    /// Default does nothing.
    fn on_skip(&self, _msg: &str, _ctx: &SignalContext) {}

    /// Called when no parser is registered for the channel.
    /// Default does nothing.
    fn on_unregistered_channel(&self, _chat_id: i64, _msg: &str) {}
}

//
// Built-in handlers.
//

/// No-op handler that does nothing. Useful for parse-only / dry-run modes.
pub struct NoopHandler;

impl SignalHandler for NoopHandler {
    fn on_signals(&self, _entries: Vec<RawSignalEntry>, _ctx: &SignalContext) {}
    fn on_signal_edit(&self, _entries: Vec<RawSignalEntry>, _ctx: &SignalContext) {}
    fn on_signal_delete(&self, _chat_id: i64, _msg_ids: Vec<i64>) {}
}

/// Logging handler that prints parsed signals via tracing. Useful for debugging.
pub struct LoggingHandler;

impl SignalHandler for LoggingHandler {
    fn on_signals(&self, entries: Vec<RawSignalEntry>, ctx: &SignalContext) {
        for entry in &entries {
            tracing::info!(
                "[NEW] [{}] {} {} {} sl={:?} tp={:?}",
                ctx.parser_name,
                entry.symbol,
                entry.side,
                entry.order_type,
                entry.stoploss,
                entry.targets,
            );
        }
    }

    fn on_signal_edit(&self, entries: Vec<RawSignalEntry>, ctx: &SignalContext) {
        for entry in &entries {
            tracing::info!(
                "[EDIT] [{}] msg_id={} {} {} {}",
                ctx.parser_name,
                ctx.msg_id,
                entry.symbol,
                entry.side,
                entry.order_type,
            );
        }
    }

    fn on_signal_delete(&self, chat_id: i64, msg_ids: Vec<i64>) {
        tracing::info!("[DEL] chat_id={} msg_ids={:?}", chat_id, msg_ids);
    }

    fn on_skip(&self, msg: &str, ctx: &SignalContext) {
        tracing::debug!(
            "[SKIP] [{}] chat_id={} msg_id={}: {}",
            ctx.parser_name,
            ctx.chat_id,
            ctx.msg_id,
            &msg[..msg.len().min(80)],
        );
    }

    fn on_unregistered_channel(&self, chat_id: i64, msg: &str) {
        tracing::debug!(
            "[UNREGISTERED] chat_id={}: {}",
            chat_id,
            &msg[..msg.len().min(80)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn make_ctx() -> SignalContext {
        SignalContext {
            chat_id: 100,
            msg_id: 1,
            ts: NaiveDate::from_ymd_opt(2026, 1, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            parser_name: "test-parser".to_string(),
        }
    }

    #[test]
    fn noop_handler_does_nothing() {
        let h = NoopHandler;
        let ctx = make_ctx();
        h.on_signals(vec![], &ctx);
        h.on_signal_edit(vec![], &ctx);
        h.on_signal_delete(100, vec![1, 2]);
        h.on_skip("hello", &ctx);
        h.on_unregistered_channel(999, "text");
    }

    #[test]
    fn logging_handler_does_not_panic() {
        let h = LoggingHandler;
        let ctx = make_ctx();
        h.on_signals(vec![], &ctx);
        h.on_signal_edit(vec![], &ctx);
        h.on_signal_delete(100, vec![1, 2]);
        h.on_skip("hello", &ctx);
        h.on_unregistered_channel(999, "text");
    }

    #[test]
    fn signal_context_fields() {
        let ctx = SignalContext {
            chat_id: 42,
            msg_id: 7,
            ts: NaiveDate::from_ymd_opt(2026, 3, 1)
                .unwrap()
                .and_hms_opt(12, 30, 0)
                .unwrap(),
            parser_name: "wave-trader".to_string(),
        };
        assert_eq!(ctx.chat_id, 42);
        assert_eq!(ctx.msg_id, 7);
        assert_eq!(ctx.parser_name, "wave-trader");
        assert_eq!(
            ctx.ts,
            NaiveDate::from_ymd_opt(2026, 3, 1)
                .unwrap()
                .and_hms_opt(12, 30, 0)
                .unwrap()
        );
    }
}
