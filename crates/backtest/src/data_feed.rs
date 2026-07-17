//! Data feed abstraction for backtesting.
//!
//! A [`DataFeed`] produces a time-ordered sequence of [`MarketEvent`]s that the
//! backtest runner consumes.  Two built-in implementations are provided:
//!
//! - [`VecFeed`] — wraps a pre-loaded `Vec<MarketEvent>` (useful for tests and
//!   any data source you can materialise up-front).
//! - Conversion helpers from `qs-data-preprocess` types ([`Tick`], [`Bar`]) so
//!   you can query DuckDB, convert the results, and feed them straight in.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::marker::PhantomData;

use chrono::NaiveDateTime;
use qs_core::ExecutionPricer;
use qs_core::types::PriceQuote;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

    /// Convert this event into a quote only when its executable bid/ask view is
    /// finite, positive, and not crossed.
    pub fn to_valid_quote(&self) -> Option<PriceQuote> {
        let quote = self.to_quote();
        ExecutionPricer::validate_quote(&quote).ok().map(|()| quote)
    }
}

/// Roles a feed event serves in a multi-series backtest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesRoles {
    pub primary: bool,
    pub conversion: bool,
}

impl SeriesRoles {
    pub const PRIMARY: Self = Self {
        primary: true,
        conversion: false,
    };
    pub const CONVERSION: Self = Self {
        primary: false,
        conversion: true,
    };
    pub const PRIMARY_AND_CONVERSION: Self = Self {
        primary: true,
        conversion: true,
    };
}

/// Deterministic identity and ordering metadata for one market event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMetadata {
    pub roles: SeriesRoles,
    pub series_rank: u32,
    /// Physical source-row ordinal when supplied by a streaming source, otherwise the emitted event ordinal.
    pub row_sequence: u64,
}

impl EventMetadata {
    pub const fn new(roles: SeriesRoles, series_rank: u32, row_sequence: u64) -> Self {
        Self {
            roles,
            series_rank,
            row_sequence,
        }
    }
}

/// A market event paired with non-persisted feed metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedEvent {
    pub event: MarketEvent,
    pub metadata: EventMetadata,
}

impl FeedEvent {
    pub fn new(event: MarketEvent, metadata: EventMetadata) -> Self {
        Self { event, metadata }
    }

    /// Ordering key used by deterministic feeds.
    pub fn ordering_key(&self) -> (NaiveDateTime, u32, u64) {
        (
            self.event.ts(),
            self.metadata.series_rank,
            self.metadata.row_sequence,
        )
    }
}

/// A streaming event paired with its physical source-row ordinal.
#[derive(Debug, Clone)]
pub struct SequencedMarketEvent {
    pub event: MarketEvent,
    pub source_row_ordinal: u64,
}

impl SequencedMarketEvent {
    pub fn new(event: MarketEvent, source_row_ordinal: u64) -> Self {
        Self {
            event,
            source_row_ordinal,
        }
    }
}

/// Converts an event source item into an event and optional physical ordinal.
pub trait EventSourceItem {
    fn into_event_and_ordinal(self) -> (MarketEvent, Option<u64>);
}

impl EventSourceItem for MarketEvent {
    fn into_event_and_ordinal(self) -> (MarketEvent, Option<u64>) {
        (self, None)
    }
}

impl EventSourceItem for SequencedMarketEvent {
    fn into_event_and_ordinal(self) -> (MarketEvent, Option<u64>) {
        (self.event, Some(self.source_row_ordinal))
    }
}

/// A contiguous batch of events with the same timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampBatch {
    pub ts: NaiveDateTime,
    pub events: Vec<FeedEvent>,
}

impl TimestampBatch {
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Sequential source of market events for backtesting.
pub trait DataFeed {
    /// Return the next event, or `None` when the feed is exhausted.
    fn next_event(&mut self) -> Option<MarketEvent>;

    /// Peek at the next event without consuming it.
    fn peek(&self) -> Option<&MarketEvent>;

    /// Return all contiguous events at the next timestamp.
    ///
    /// Implementations with native metadata should override this method.
    /// The default preserves compatibility for custom feeds and assigns primary role metadata in their existing event order.
    fn next_batch(&mut self) -> Option<TimestampBatch> {
        let ts = self.peek()?.ts();
        let mut events = Vec::new();
        let mut row_sequence = 0;

        while self.peek().is_some_and(|event| event.ts() == ts) {
            let event = self.next_event()?;
            events.push(FeedEvent::new(
                event,
                EventMetadata::new(SeriesRoles::PRIMARY, 0, row_sequence),
            ));
            row_sequence += 1;
        }

        Some(TimestampBatch { ts, events })
    }

    /// Total number of events when known without consuming the feed.
    fn total_events(&self) -> Option<usize> {
        None
    }
}

/// Fallible source of complete timestamp batches.
pub trait FallibleBatchFeed {
    type Error;

    /// Return every event at the next timestamp, or `None` at end of input.
    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error>;
}

impl<F> FallibleBatchFeed for Box<F>
where
    F: FallibleBatchFeed + ?Sized,
{
    type Error = F::Error;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        (**self).next_batch()
    }
}

/// Errors produced while grouping a fallible event source into timestamp batches.
#[derive(Debug, Error)]
pub enum EventBatchFeedError<E> {
    #[error("event source failed: {0}")]
    Source(E),
    #[error("event source moved backwards from {previous} to {current}")]
    NonMonotonic {
        previous: NaiveDateTime,
        current: NaiveDateTime,
    },
    #[error("event row sequence is exhausted")]
    RowSequenceExhausted,
}

/// Groups a fallible event cursor into complete timestamp batches.
pub struct EventBatchFeed<F, I = MarketEvent> {
    next_event: F,
    pending: Option<FeedEvent>,
    roles: SeriesRoles,
    series_rank: u32,
    next_row_sequence: u64,
    last_source_ts: Option<NaiveDateTime>,
    exhausted: bool,
    item: PhantomData<fn() -> I>,
}

impl<F, I> EventBatchFeed<F, I> {
    pub fn new<E>(next_event: F, roles: SeriesRoles, series_rank: u32) -> Self
    where
        F: FnMut() -> Result<Option<I>, E>,
        I: EventSourceItem,
    {
        Self {
            next_event,
            pending: None,
            roles,
            series_rank,
            next_row_sequence: 0,
            last_source_ts: None,
            exhausted: false,
            item: PhantomData,
        }
    }
}

impl<F, I, E> EventBatchFeed<F, I>
where
    F: FnMut() -> Result<Option<I>, E>,
    I: EventSourceItem,
{
    /// Return the next complete timestamp batch.
    pub fn next_timestamp_batch(
        &mut self,
    ) -> Result<Option<TimestampBatch>, EventBatchFeedError<E>> {
        let first = match self.pending.take() {
            Some(event) => event,
            None => match self.pull_event()? {
                Some(event) => event,
                None => return Ok(None),
            },
        };
        let ts = first.event.ts();
        let mut events = vec![first];

        loop {
            match self.pull_event()? {
                Some(event) if event.event.ts() == ts => events.push(event),
                Some(event) => {
                    self.pending = Some(event);
                    break;
                }
                None => break,
            }
        }

        Ok(Some(TimestampBatch { ts, events }))
    }

    fn pull_event(&mut self) -> Result<Option<FeedEvent>, EventBatchFeedError<E>> {
        if self.exhausted {
            return Ok(None);
        }
        let item = match (self.next_event)() {
            Ok(Some(item)) => item,
            Ok(None) => {
                self.exhausted = true;
                return Ok(None);
            }
            Err(error) => {
                self.exhausted = true;
                return Err(EventBatchFeedError::Source(error));
            }
        };
        let (event, source_row_ordinal) = item.into_event_and_ordinal();
        let current = event.ts();
        if let Some(previous) = self.last_source_ts
            && current < previous
        {
            self.exhausted = true;
            return Err(EventBatchFeedError::NonMonotonic { previous, current });
        }
        let row_sequence = match source_row_ordinal {
            Some(source_row_ordinal) => source_row_ordinal,
            None => {
                let row_sequence = self.next_row_sequence;
                self.next_row_sequence = self
                    .next_row_sequence
                    .checked_add(1)
                    .ok_or(EventBatchFeedError::RowSequenceExhausted)?;
                row_sequence
            }
        };
        self.last_source_ts = Some(current);
        Ok(Some(FeedEvent::new(
            event,
            EventMetadata::new(self.roles, self.series_rank, row_sequence),
        )))
    }
}

impl<F, I, E> FallibleBatchFeed for EventBatchFeed<F, I>
where
    F: FnMut() -> Result<Option<I>, E>,
    I: EventSourceItem,
{
    type Error = EventBatchFeedError<E>;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        self.next_timestamp_batch()
    }
}

/// Errors produced while deterministically merging timestamp batch feeds.
#[derive(Debug, Error)]
pub enum KWayMergeError<E> {
    #[error("series {series_rank} failed: {error}")]
    Source { series_rank: u32, error: E },
    #[error("series {series_rank} returned an empty timestamp batch at {ts}")]
    EmptyBatch { series_rank: u32, ts: NaiveDateTime },
    #[error("series {series_rank} batch at {batch_ts} contains an event at {event_ts}")]
    TimestampMismatch {
        series_rank: u32,
        batch_ts: NaiveDateTime,
        event_ts: NaiveDateTime,
    },
    #[error("series {series_rank} moved backwards from {previous} to {current}")]
    NonMonotonic {
        series_rank: u32,
        previous: NaiveDateTime,
        current: NaiveDateTime,
    },
    #[error("duplicate merge ordering key ({ts}, {series_rank}, {row_sequence})")]
    DuplicateOrderingKey {
        ts: NaiveDateTime,
        series_rank: u32,
        row_sequence: u64,
    },
}

/// Streaming deterministic merge over multiple fallible timestamp batch feeds.
pub struct KWayMergeFeed<F>
where
    F: FallibleBatchFeed,
{
    feeds: Vec<F>,
    heads: Vec<Option<TimestampBatch>>,
    head_heap: BinaryHeap<Reverse<(NaiveDateTime, usize)>>,
    exhausted: Vec<bool>,
    last_batch_ts: Vec<Option<NaiveDateTime>>,
    initialized: bool,
}

impl<F> KWayMergeFeed<F>
where
    F: FallibleBatchFeed,
{
    /// Create a merge using input position as `series_rank`.
    pub fn new(feeds: Vec<F>) -> Self {
        assert!(
            feeds.len() <= u32::MAX as usize,
            "KWayMergeFeed supports at most u32::MAX series"
        );
        let series_count = feeds.len();
        Self {
            feeds,
            heads: (0..series_count).map(|_| None).collect(),
            head_heap: BinaryHeap::with_capacity(series_count),
            exhausted: vec![false; series_count],
            last_batch_ts: vec![None; series_count],
            initialized: false,
        }
    }

    pub fn series_count(&self) -> usize {
        self.feeds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.feeds.is_empty()
    }

    /// Return the next complete merged timestamp batch.
    pub fn next_timestamp_batch(
        &mut self,
    ) -> Result<Option<TimestampBatch>, KWayMergeError<F::Error>> {
        if !self.initialized {
            for index in 0..self.feeds.len() {
                self.fill_head(index)?;
            }
            self.initialized = true;
        }
        let Some(Reverse((ts, _))) = self.head_heap.peek().copied() else {
            return Ok(None);
        };

        let mut events = Vec::new();
        while self
            .head_heap
            .peek()
            .is_some_and(|Reverse((head_ts, _))| *head_ts == ts)
        {
            let Reverse((_, index)) = self.head_heap.pop().expect("matching heap head exists");
            let batch = self.heads[index]
                .take()
                .expect("heap head retains its batch");
            let series_rank = index as u32;
            events.extend(batch.events.into_iter().map(|mut event| {
                event.metadata.series_rank = series_rank;
                event
            }));
            self.fill_head(index)?;
        }

        events.sort_by_key(FeedEvent::ordering_key);
        for duplicate in events.windows(2) {
            if duplicate[0].ordering_key() == duplicate[1].ordering_key() {
                let (_, series_rank, row_sequence) = duplicate[0].ordering_key();
                return Err(KWayMergeError::DuplicateOrderingKey {
                    ts,
                    series_rank,
                    row_sequence,
                });
            }
        }
        Ok(Some(TimestampBatch { ts, events }))
    }

    fn fill_head(&mut self, index: usize) -> Result<(), KWayMergeError<F::Error>> {
        if self.heads[index].is_some() || self.exhausted[index] {
            return Ok(());
        }
        let series_rank = index as u32;
        let Some(batch) = self.feeds[index]
            .next_batch()
            .map_err(|error| KWayMergeError::Source { series_rank, error })?
        else {
            self.exhausted[index] = true;
            return Ok(());
        };
        if batch.is_empty() {
            return Err(KWayMergeError::EmptyBatch {
                series_rank,
                ts: batch.ts,
            });
        }
        for event in &batch.events {
            if event.event.ts() != batch.ts {
                return Err(KWayMergeError::TimestampMismatch {
                    series_rank,
                    batch_ts: batch.ts,
                    event_ts: event.event.ts(),
                });
            }
        }
        if let Some(previous) = self.last_batch_ts[index]
            && batch.ts < previous
        {
            return Err(KWayMergeError::NonMonotonic {
                series_rank,
                previous,
                current: batch.ts,
            });
        }
        self.last_batch_ts[index] = Some(batch.ts);
        self.head_heap.push(Reverse((batch.ts, index)));
        self.heads[index] = Some(batch);
        Ok(())
    }
}

impl<F> FallibleBatchFeed for KWayMergeFeed<F>
where
    F: FallibleBatchFeed,
{
    type Error = KWayMergeError<F::Error>;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        self.next_timestamp_batch()
    }
}

/// In-memory data feed backed by metadata-bearing market events.
///
/// [`Self::new`] retains the existing contract and does not sort its input.
/// [`Self::from_feed_events`] sorts by `(timestamp, series_rank, row_sequence)`.
#[derive(Debug, Clone)]
pub struct VecFeed {
    events: Vec<FeedEvent>,
    index: usize,
}

impl VecFeed {
    /// Create a new feed from a pre-sorted vector of primary events.
    pub fn new(events: Vec<MarketEvent>) -> Self {
        let events = events
            .into_iter()
            .enumerate()
            .map(|(row_sequence, event)| {
                FeedEvent::new(
                    event,
                    EventMetadata::new(SeriesRoles::PRIMARY, 0, row_sequence as u64),
                )
            })
            .collect();
        Self { events, index: 0 }
    }

    /// Create a deterministically sorted feed from metadata-bearing events.
    pub fn from_feed_events(mut events: Vec<FeedEvent>) -> Self {
        events.sort_by_key(FeedEvent::ordering_key);
        Self { events, index: 0 }
    }

    /// Return the next event together with its ordering metadata.
    pub fn next_feed_event(&mut self) -> Option<FeedEvent> {
        let event = self.events.get(self.index)?.clone();
        self.index += 1;
        Some(event)
    }

    /// Peek at the metadata for the next event without consuming it.
    pub fn peek_metadata(&self) -> Option<&EventMetadata> {
        self.events.get(self.index).map(|event| &event.metadata)
    }

    /// Return all remaining events at the next timestamp.
    pub fn next_timestamp_batch(&mut self) -> Option<TimestampBatch> {
        let ts = self.events.get(self.index)?.event.ts();
        let start = self.index;
        while self
            .events
            .get(self.index)
            .is_some_and(|event| event.event.ts() == ts)
        {
            self.index += 1;
        }

        Some(TimestampBatch {
            ts,
            events: self.events[start..self.index].to_vec(),
        })
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
        self.next_feed_event().map(|event| event.event)
    }

    fn peek(&self) -> Option<&MarketEvent> {
        self.events.get(self.index).map(|event| &event.event)
    }

    fn next_batch(&mut self) -> Option<TimestampBatch> {
        self.next_timestamp_batch()
    }

    fn total_events(&self) -> Option<usize> {
        Some(self.events.len())
    }
}

/// Convert ticks into a primary-series feed.
///
/// Ticks without a valid executable bid/ask quote are silently skipped.
pub fn ticks_to_feed(ticks: Vec<data_preprocess::Tick>) -> VecFeed {
    ticks_to_feed_with_metadata(ticks, SeriesRoles::PRIMARY, 0)
}

/// Convert ticks into a deterministically ranked feed.
pub fn ticks_to_feed_with_metadata(
    ticks: Vec<data_preprocess::Tick>,
    roles: SeriesRoles,
    series_rank: u32,
) -> VecFeed {
    let events = ticks
        .into_iter()
        .enumerate()
        .filter_map(|(row_sequence, tick)| {
            let bid = tick.bid?;
            let ask = tick.ask?;
            let event = MarketEvent::Tick {
                symbol: tick.symbol,
                ts: tick.ts,
                bid,
                ask,
            };
            event.to_valid_quote().map(|_| {
                FeedEvent::new(
                    event,
                    EventMetadata::new(roles, series_rank, row_sequence as u64),
                )
            })
        })
        .collect();
    VecFeed::from_feed_events(events)
}

/// Convert bars into a primary-series feed.
pub fn bars_to_feed(bars: Vec<data_preprocess::Bar>) -> VecFeed {
    bars_to_feed_with_metadata(bars, SeriesRoles::PRIMARY, 0)
}

/// Convert bars into a deterministically ranked feed.
pub fn bars_to_feed_with_metadata(
    bars: Vec<data_preprocess::Bar>,
    roles: SeriesRoles,
    series_rank: u32,
) -> VecFeed {
    let events = bars
        .into_iter()
        .enumerate()
        .map(|(row_sequence, bar)| {
            let event = MarketEvent::Bar {
                symbol: bar.symbol,
                ts: bar.ts,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
            };
            FeedEvent::new(
                event,
                EventMetadata::new(roles, series_rank, row_sequence as u64),
            )
        })
        .collect();
    VecFeed::from_feed_events(events)
}

/// Merge feeds using input position as `series_rank`.
///
/// Events are ordered by `(timestamp, series_rank, row_sequence)`.
/// Role metadata is preserved, so a single event can serve both primary and conversion roles.
pub fn merge_feeds(feeds: Vec<VecFeed>) -> VecFeed {
    let mut all_events = Vec::new();
    for (series_rank, feed) in feeds.into_iter().enumerate() {
        let series_rank = series_rank.min(u32::MAX as usize) as u32;
        all_events.extend(feed.events.into_iter().map(|mut event| {
            event.metadata.series_rank = series_rank;
            event
        }));
    }
    VecFeed::from_feed_events(all_events)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

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
    fn validated_quote_contract_rejects_nonfinite_nonpositive_and_crossed_ticks() {
        for (bid, ask) in [
            (f64::NAN, 1.0),
            (1.0, f64::INFINITY),
            (0.0, 1.0),
            (2.0, 1.0),
        ] {
            let event = MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid,
                ask,
            };
            assert!(event.to_valid_quote().is_none());
        }

        let ticks = vec![
            data_preprocess::Tick {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid: Some(1.1),
                ask: Some(1.0),
                last: None,
                volume: None,
                flags: None,
            },
            data_preprocess::Tick {
                exchange: "test".into(),
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 1),
                bid: Some(1.0),
                ask: Some(1.1),
                last: None,
                volume: None,
                flags: None,
            },
        ];
        assert_eq!(ticks_to_feed(ticks).total(), 1);
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

    #[test]
    fn equal_timestamp_batch_uses_deterministic_metadata_order() {
        let batch_ts = ts(10, 0, 0);
        let later_ts = ts(10, 0, 1);
        let events = vec![
            FeedEvent::new(
                MarketEvent::Tick {
                    symbol: "CONVERSION".into(),
                    ts: batch_ts,
                    bid: 2.0,
                    ask: 2.1,
                },
                EventMetadata::new(SeriesRoles::CONVERSION, 1, 0),
            ),
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol: "PRIMARY".into(),
                    ts: batch_ts,
                    open: 1.0,
                    high: 1.2,
                    low: 0.9,
                    close: 1.1,
                    volume: 10,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 1),
            ),
            FeedEvent::new(
                MarketEvent::Tick {
                    symbol: "PRIMARY".into(),
                    ts: batch_ts,
                    bid: 1.0,
                    ask: 1.1,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
            ),
            FeedEvent::new(
                MarketEvent::Tick {
                    symbol: "PRIMARY".into(),
                    ts: later_ts,
                    bid: 1.1,
                    ask: 1.2,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, 2),
            ),
        ];
        let mut feed = VecFeed::from_feed_events(events);

        let batch = feed
            .next_timestamp_batch()
            .expect("equal timestamp batch should be available");
        assert_eq!(batch.ts, batch_ts);
        assert_eq!(batch.len(), 3);
        let keys: Vec<_> = batch.events.iter().map(FeedEvent::ordering_key).collect();
        assert_eq!(
            keys,
            vec![(batch_ts, 0, 0), (batch_ts, 0, 1), (batch_ts, 1, 0)]
        );
        assert!(matches!(batch.events[1].event, MarketEvent::Bar { .. }));
        assert!(matches!(batch.events[2].event, MarketEvent::Tick { .. }));

        let later = DataFeed::next_batch(&mut feed).expect("later batch should remain");
        assert_eq!(later.ts, later_ts);
        assert_eq!(later.len(), 1);
        assert!(DataFeed::next_batch(&mut feed).is_none());
    }

    #[test]
    fn one_tick_can_serve_primary_and_conversion_roles() {
        let ticks = vec![data_preprocess::Tick {
            exchange: "test".into(),
            symbol: "EURUSD".into(),
            ts: ts(10, 0, 0),
            bid: Some(1.08),
            ask: Some(1.09),
            last: None,
            volume: None,
            flags: None,
        }];
        let mut feed = ticks_to_feed_with_metadata(ticks, SeriesRoles::PRIMARY_AND_CONVERSION, 0);

        let batch = feed.next_timestamp_batch().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch.events[0].metadata.roles,
            SeriesRoles::PRIMARY_AND_CONVERSION
        );
        assert_eq!(batch.events[0].metadata.row_sequence, 0);
    }

    #[test]
    fn merge_feeds_ranks_series_and_batches_equal_timestamps_stably() {
        let event_ts = ts(10, 0, 0);
        let primary = VecFeed::new(vec![MarketEvent::Bar {
            symbol: "PRIMARY".into(),
            ts: event_ts,
            open: 1.0,
            high: 1.2,
            low: 0.9,
            close: 1.1,
            volume: 10,
        }]);
        let conversion = VecFeed::from_feed_events(vec![FeedEvent::new(
            MarketEvent::Tick {
                symbol: "CONVERSION".into(),
                ts: event_ts,
                bid: 2.0,
                ask: 2.1,
            },
            EventMetadata::new(SeriesRoles::CONVERSION, 0, 0),
        )]);

        let mut merged = merge_feeds(vec![primary, conversion]);
        let batch = merged.next_timestamp_batch().unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.events[0].metadata.series_rank, 0);
        assert_eq!(batch.events[0].metadata.roles, SeriesRoles::PRIMARY);
        assert_eq!(batch.events[1].metadata.series_rank, 1);
        assert_eq!(batch.events[1].metadata.roles, SeriesRoles::CONVERSION);
    }

    fn ranked_tick(
        symbol: &str,
        event_ts: NaiveDateTime,
        roles: SeriesRoles,
        row_sequence: u64,
    ) -> FeedEvent {
        FeedEvent::new(
            MarketEvent::Tick {
                symbol: symbol.into(),
                ts: event_ts,
                bid: 1.0,
                ask: 1.1,
            },
            EventMetadata::new(roles, 99, row_sequence),
        )
    }

    struct ScriptedFeed {
        batches: VecDeque<Result<TimestampBatch, &'static str>>,
    }

    impl ScriptedFeed {
        fn new(batches: Vec<Result<TimestampBatch, &'static str>>) -> Self {
            Self {
                batches: batches.into(),
            }
        }
    }

    impl FallibleBatchFeed for ScriptedFeed {
        type Error = &'static str;

        fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
            self.batches.pop_front().transpose()
        }
    }

    #[test]
    fn event_batch_feed_groups_complete_timestamps_and_supports_shared_roles() {
        let first_ts = ts(10, 0, 0);
        let second_ts = ts(10, 0, 1);
        let mut source = vec![
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: first_ts,
                bid: 1.0,
                ask: 1.1,
            },
            MarketEvent::Bar {
                symbol: "EURUSD".into(),
                ts: first_ts,
                open: 1.0,
                high: 1.2,
                low: 0.9,
                close: 1.1,
                volume: 10,
            },
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: second_ts,
                bid: 1.1,
                ask: 1.2,
            },
        ]
        .into_iter();
        let mut feed = EventBatchFeed::new(
            move || Ok::<_, &'static str>(source.next()),
            SeriesRoles::PRIMARY_AND_CONVERSION,
            4,
        );

        let first = feed.next_timestamp_batch().unwrap().unwrap();
        assert_eq!(first.ts, first_ts);
        assert_eq!(first.len(), 2);
        assert!(
            first
                .events
                .iter()
                .all(|event| event.metadata.roles == SeriesRoles::PRIMARY_AND_CONVERSION)
        );
        assert_eq!(first.events[0].metadata.row_sequence, 0);
        assert_eq!(first.events[1].metadata.row_sequence, 1);

        let second = feed.next_timestamp_batch().unwrap().unwrap();
        assert_eq!(second.ts, second_ts);
        assert_eq!(second.events[0].metadata.row_sequence, 2);
        assert!(feed.next_timestamp_batch().unwrap().is_none());
    }

    #[test]
    fn event_batch_feed_preserves_explicit_physical_source_ordinals() {
        let mut source = vec![
            SequencedMarketEvent::new(
                MarketEvent::Tick {
                    symbol: "EURUSD".into(),
                    ts: ts(10, 0, 0),
                    bid: 1.0,
                    ask: 1.1,
                },
                4,
            ),
            SequencedMarketEvent::new(
                MarketEvent::Tick {
                    symbol: "EURUSD".into(),
                    ts: ts(10, 0, 0),
                    bid: 1.1,
                    ask: 1.2,
                },
                9,
            ),
        ]
        .into_iter();
        let mut feed = EventBatchFeed::new(
            move || Ok::<_, &'static str>(source.next()),
            SeriesRoles::PRIMARY,
            2,
        );

        let batch = feed.next_timestamp_batch().unwrap().unwrap();
        assert_eq!(
            batch
                .events
                .iter()
                .map(|event| event.metadata.row_sequence)
                .collect::<Vec<_>>(),
            vec![4, 9]
        );
        assert_eq!(batch.events[0].metadata.series_rank, 2);
    }

    #[test]
    fn event_batch_feed_rejects_a_non_monotonic_event_source() {
        let mut source = vec![
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 1),
                bid: 1.0,
                ask: 1.1,
            },
            MarketEvent::Tick {
                symbol: "EURUSD".into(),
                ts: ts(10, 0, 0),
                bid: 1.0,
                ask: 1.1,
            },
        ]
        .into_iter();
        let mut feed = EventBatchFeed::new(
            move || Ok::<_, &'static str>(source.next()),
            SeriesRoles::PRIMARY,
            0,
        );

        assert!(matches!(
            feed.next_timestamp_batch(),
            Err(EventBatchFeedError::NonMonotonic { previous, current })
                if previous == ts(10, 0, 1) && current == ts(10, 0, 0)
        ));
    }

    #[test]
    fn k_way_merge_drains_all_equal_timestamp_batches_and_orders_by_full_key() {
        let first_ts = ts(10, 0, 0);
        let second_ts = ts(10, 0, 1);
        let first = ScriptedFeed::new(vec![
            Ok(TimestampBatch {
                ts: first_ts,
                events: vec![ranked_tick(
                    "SHARED",
                    first_ts,
                    SeriesRoles::PRIMARY_AND_CONVERSION,
                    0,
                )],
            }),
            Ok(TimestampBatch {
                ts: first_ts,
                events: vec![ranked_tick("PRIMARY", first_ts, SeriesRoles::PRIMARY, 1)],
            }),
            Ok(TimestampBatch {
                ts: second_ts,
                events: vec![ranked_tick("PRIMARY", second_ts, SeriesRoles::PRIMARY, 2)],
            }),
        ]);
        let second = ScriptedFeed::new(vec![Ok(TimestampBatch {
            ts: first_ts,
            events: vec![ranked_tick(
                "CONVERSION",
                first_ts,
                SeriesRoles::CONVERSION,
                0,
            )],
        })]);
        let mut merged = KWayMergeFeed::new(vec![first, second]);

        let batch = merged.next_timestamp_batch().unwrap().unwrap();
        assert_eq!(batch.ts, first_ts);
        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch
                .events
                .iter()
                .map(FeedEvent::ordering_key)
                .collect::<Vec<_>>(),
            vec![(first_ts, 0, 0), (first_ts, 0, 1), (first_ts, 1, 0)]
        );
        assert_eq!(
            batch.events[0].metadata.roles,
            SeriesRoles::PRIMARY_AND_CONVERSION
        );

        let later = merged.next_timestamp_batch().unwrap().unwrap();
        assert_eq!(later.ts, second_ts);
        assert_eq!(later.len(), 1);
        assert!(merged.next_timestamp_batch().unwrap().is_none());
    }

    #[test]
    fn k_way_merge_propagates_errors_while_draining_a_complete_timestamp() {
        let event_ts = ts(10, 0, 0);
        let first = ScriptedFeed::new(vec![
            Ok(TimestampBatch {
                ts: event_ts,
                events: vec![ranked_tick("PRIMARY", event_ts, SeriesRoles::PRIMARY, 0)],
            }),
            Err("same timestamp continuation failed"),
        ]);
        let second = ScriptedFeed::new(vec![Ok(TimestampBatch {
            ts: event_ts,
            events: vec![ranked_tick(
                "CONVERSION",
                event_ts,
                SeriesRoles::CONVERSION,
                0,
            )],
        })]);
        let mut merged = KWayMergeFeed::new(vec![first, second]);

        assert!(matches!(
            merged.next_timestamp_batch(),
            Err(KWayMergeError::Source {
                series_rank: 0,
                error: "same timestamp continuation failed"
            })
        ));
    }

    #[test]
    fn k_way_merge_propagates_ranked_source_failures() {
        let mut merged = KWayMergeFeed::new(vec![ScriptedFeed::new(vec![Err("read failed")])]);
        assert!(matches!(
            merged.next_timestamp_batch(),
            Err(KWayMergeError::Source {
                series_rank: 0,
                error: "read failed"
            })
        ));
    }
}
