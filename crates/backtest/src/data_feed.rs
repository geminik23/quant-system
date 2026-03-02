//! Data feed abstraction for backtesting.
//!
//! A [`DataFeed`] produces a time-ordered sequence of [`MarketEvent`]s that the
//! backtest runner consumes.  Two built-in implementations are provided:
//!
//! - [`VecFeed`] — wraps a pre-loaded `Vec<MarketEvent>` (useful for tests and
//!   any data source you can materialise up-front).
//! - Conversion helpers from `qs-data-preprocess` types ([`Tick`], [`Bar`]) so
//!   you can query DuckDB, convert the results, and feed them straight in.

use chrono::NaiveDateTime;
use qs_core::types::PriceQuote;
use serde::{Deserialize, Serialize};

// ─── MarketEvent ────────────────────────────────────────────────────────────

/// A single market data event consumed by the backtest runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketEvent {
    /// A bid/ask tick.
    Tick {
        symbol: String,
        ts: NaiveDateTime,
        bid: f64,
        ask: f64,
    },
    /// An OHLCV bar.
    Bar {
        symbol: String,
        ts: NaiveDateTime,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: i64,
    },
}

impl MarketEvent {
    /// Timestamp of the event.
    pub fn ts(&self) -> NaiveDateTime {
        match self {
            MarketEvent::Tick { ts, .. } => *ts,
            MarketEvent::Bar { ts, .. } => *ts,
        }
    }

    /// Symbol of the event.
    pub fn symbol(&self) -> &str {
        match self {
            MarketEvent::Tick { symbol, .. } => symbol,
            MarketEvent::Bar { symbol, .. } => symbol,
        }
    }

    /// Convert the event into a [`PriceQuote`] suitable for the trade engine.
    ///
    /// - For ticks this is straightforward (bid/ask).
    /// - For bars the close price is used for both bid and ask (zero spread
    ///   approximation).  A more sophisticated feed could model the spread
    ///   separately.
    pub fn to_quote(&self) -> PriceQuote {
        match self {
            MarketEvent::Tick {
                symbol,
                ts,
                bid,
                ask,
            } => PriceQuote {
                symbol: symbol.clone(),
                ts: *ts,
                bid: *bid,
                ask: *ask,
            },
            MarketEvent::Bar {
                symbol, ts, close, ..
            } => PriceQuote {
                symbol: symbol.clone(),
                ts: *ts,
                bid: *close,
                ask: *close,
            },
        }
    }
}

// ─── DataFeed trait ─────────────────────────────────────────────────────────

/// Sequential source of market events for backtesting.
pub trait DataFeed {
    /// Return the next event, or `None` when the feed is exhausted.
    fn next_event(&mut self) -> Option<MarketEvent>;

    /// Peek at the next event without consuming it.
    fn peek(&self) -> Option<&MarketEvent>;
}

// ─── VecFeed ────────────────────────────────────────────────────────────────

/// In-memory data feed backed by a `Vec<MarketEvent>`.
///
/// Events should be sorted by timestamp before construction (the feed does
/// **not** re-sort internally).
#[derive(Debug, Clone)]
pub struct VecFeed {
    events: Vec<MarketEvent>,
    index: usize,
}

impl VecFeed {
    /// Create a new feed from a pre-sorted vector of events.
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self { events, index: 0 }
    }

    /// Number of events remaining.
    pub fn remaining(&self) -> usize {
        self.events.len().saturating_sub(self.index)
    }

    /// Total number of events in the feed (consumed + remaining).
    pub fn total(&self) -> usize {
        self.events.len()
    }

    /// Reset the feed to the beginning.
    pub fn reset(&mut self) {
        self.index = 0;
    }
}

impl DataFeed for VecFeed {
    fn next_event(&mut self) -> Option<MarketEvent> {
        if self.index < self.events.len() {
            let event = self.events[self.index].clone();
            self.index += 1;
            Some(event)
        } else {
            None
        }
    }

    fn peek(&self) -> Option<&MarketEvent> {
        self.events.get(self.index)
    }
}

// ─── Conversion from data-preprocess types ──────────────────────────────────

/// Convert a vector of [`data_preprocess::Tick`] into a [`VecFeed`].
///
/// Ticks with missing `bid` **or** `ask` are silently skipped.
pub fn ticks_to_feed(ticks: Vec<data_preprocess::Tick>) -> VecFeed {
    let events: Vec<MarketEvent> = ticks
        .into_iter()
        .filter_map(|t| {
            let bid = t.bid?;
            let ask = t.ask?;
            Some(MarketEvent::Tick {
                symbol: t.symbol,
                ts: t.ts,
                bid,
                ask,
            })
        })
        .collect();
    VecFeed::new(events)
}

/// Convert a vector of [`data_preprocess::Bar`] into a [`VecFeed`].
pub fn bars_to_feed(bars: Vec<data_preprocess::Bar>) -> VecFeed {
    let events: Vec<MarketEvent> = bars
        .into_iter()
        .map(|b| MarketEvent::Bar {
            symbol: b.symbol,
            ts: b.ts,
            open: b.open,
            high: b.high,
            low: b.low,
            close: b.close,
            volume: b.volume,
        })
        .collect();
    VecFeed::new(events)
}

// ─── Multi-symbol merge helper ──────────────────────────────────────────────

/// Merge multiple pre-sorted `VecFeed`s into one time-ordered feed.
///
/// This is useful when you load ticks/bars for several symbols independently
/// and want a single interleaved timeline for the backtest.
pub fn merge_feeds(feeds: Vec<VecFeed>) -> VecFeed {
    let mut all_events: Vec<MarketEvent> = feeds.into_iter().flat_map(|f| f.events).collect();
    all_events.sort_by_key(|e| e.ts());
    VecFeed::new(all_events)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn sample_events() -> Vec<MarketEvent> {
        vec![
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid: 1.0848,
                ask: 1.0850,
            },
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 1),
                bid: 1.0849,
                ask: 1.0851,
            },
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 2),
                bid: 1.0847,
                ask: 1.0849,
            },
        ]
    }

    #[test]
    fn vec_feed_iterates() {
        let mut feed = VecFeed::new(sample_events());
        assert_eq!(feed.total(), 3);
        assert_eq!(feed.remaining(), 3);

        let e1 = feed.next_event().unwrap();
        assert_eq!(e1.symbol(), "EURUSD");
        assert_eq!(feed.remaining(), 2);

        let _ = feed.next_event().unwrap();
        let _ = feed.next_event().unwrap();
        assert!(feed.next_event().is_none());
        assert_eq!(feed.remaining(), 0);
    }

    #[test]
    fn vec_feed_peek() {
        let feed = VecFeed::new(sample_events());
        let peeked = feed.peek().unwrap();
        assert_eq!(peeked.ts(), ts(10, 0, 0));
    }

    #[test]
    fn vec_feed_reset() {
        let mut feed = VecFeed::new(sample_events());
        let _ = feed.next_event();
        let _ = feed.next_event();
        feed.reset();
        assert_eq!(feed.remaining(), 3);
    }

    #[test]
    fn to_quote_tick() {
        let event = MarketEvent::Tick {
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            bid: 1.0848,
            ask: 1.0850,
        };
        let q = event.to_quote();
        assert_eq!(q.symbol, "EURUSD");
        assert!((q.bid - 1.0848).abs() < f64::EPSILON);
        assert!((q.ask - 1.0850).abs() < f64::EPSILON);
    }

    #[test]
    fn to_quote_bar() {
        let event = MarketEvent::Bar {
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            open: 1.0840,
            high: 1.0860,
            low: 1.0830,
            close: 1.0855,
            volume: 1000,
        };
        let q = event.to_quote();
        // Bar uses close for both bid and ask
        assert!((q.bid - 1.0855).abs() < f64::EPSILON);
        assert!((q.ask - 1.0855).abs() < f64::EPSILON);
    }

    #[test]
    fn merge_two_feeds() {
        let feed_a = VecFeed::new(vec![
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid: 1.08,
                ask: 1.09,
            },
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 2),
                bid: 1.08,
                ask: 1.09,
            },
        ]);
        let feed_b = VecFeed::new(vec![MarketEvent::Tick {
            symbol: "XAUUSD".into(),
            ts: ts(10, 0, 1),
            bid: 2000.0,
            ask: 2001.0,
        }]);

        let mut merged = merge_feeds(vec![feed_a, feed_b]);
        assert_eq!(merged.total(), 3);

        let e1 = merged.next_event().unwrap();
        assert_eq!(e1.ts(), ts(10, 0, 0));
        assert_eq!(e1.symbol(), "EURUSD");

        let e2 = merged.next_event().unwrap();
        assert_eq!(e2.ts(), ts(10, 0, 1));
        assert_eq!(e2.symbol(), "XAUUSD");

        let e3 = merged.next_event().unwrap();
        assert_eq!(e3.ts(), ts(10, 0, 2));
        assert_eq!(e3.symbol(), "EURUSD");
    }

    #[test]
    fn ticks_to_feed_skips_missing_prices() {
        let ticks = vec![
            data_preprocess::Tick {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid: Some(1.08),
                ask: Some(1.09),
                last: None,
                volume: None,
                flags: None,
            },
            data_preprocess::Tick {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 1),
                bid: Some(1.08),
                ask: None, // missing ask
                last: None,
                volume: None,
                flags: None,
            },
            data_preprocess::Tick {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 2),
                bid: None, // missing bid
                ask: Some(1.09),
                last: None,
                volume: None,
                flags: None,
            },
        ];

        let feed = ticks_to_feed(ticks);
        assert_eq!(feed.total(), 1); // only the first tick survives
    }

    #[test]
    fn bars_to_feed_converts_all() {
        let bars = vec![
            data_preprocess::Bar {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                timeframe: data_preprocess::Timeframe::M5,
                ts: ts(10, 0, 0),
                open: 1.0840,
                high: 1.0860,
                low: 1.0830,
                close: 1.0855,
                tick_vol: 100,
                volume: 1000,
                spread: 2,
            },
            data_preprocess::Bar {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                timeframe: data_preprocess::Timeframe::M5,
                ts: ts(10, 5, 0),
                open: 1.0855,
                high: 1.0870,
                low: 1.0845,
                close: 1.0865,
                tick_vol: 120,
                volume: 1200,
                spread: 2,
            },
        ];

        let feed = bars_to_feed(bars);
        assert_eq!(feed.total(), 2);
    }
}
