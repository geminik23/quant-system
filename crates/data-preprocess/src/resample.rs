//! Deterministic tick-to-bar aggregation shared by stored bars and in-memory replay series.
//!
//! Historical replay builds its analysis bars from ticks while it runs, and a research workflow needs the same bars on disk so that a parameter search over bars agrees with a confirmation run over ticks. Both paths therefore use the bucket arithmetic and accumulation rules in this module, and a parity test asserts that they produce identical bars for the same input.
//!
//! The module is intentionally free of storage and dataframe dependencies so that it compiles for consumers that only need the models.

use chrono::{DateTime, NaiveDateTime};

use crate::models::{Bar, Timeframe};

/// Quote side used to derive one bar price from a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PriceBasis {
    Bid,
    Ask,
    Mid,
}

impl PriceBasis {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "bid" => Some(Self::Bid),
            "ask" => Some(Self::Ask),
            "mid" => Some(Self::Mid),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bid => "bid",
            Self::Ask => "ask",
            Self::Mid => "mid",
        }
    }

    /// Bar price for one two-sided quote.
    pub fn price(self, bid: f64, ask: f64) -> f64 {
        match self {
            Self::Bid => bid,
            Self::Ask => ask,
            Self::Mid => bid + (ask - bid) / 2.0,
        }
    }
}

/// Fixed-duration bucket geometry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketSpec {
    duration_seconds: i64,
    alignment_offset_seconds: i64,
}

impl BucketSpec {
    /// Build a bucket geometry, reducing the alignment offset into the bucket duration.
    pub fn new(duration_seconds: i64, alignment_offset_seconds: i64) -> Option<Self> {
        if duration_seconds <= 0 {
            return None;
        }
        Some(Self {
            duration_seconds,
            alignment_offset_seconds: alignment_offset_seconds.rem_euclid(duration_seconds),
        })
    }

    pub fn duration_seconds(self) -> i64 {
        self.duration_seconds
    }

    pub fn alignment_offset_seconds(self) -> i64 {
        self.alignment_offset_seconds
    }
}

/// Half-open `[open, close)` bucket containing `timestamp`.
///
/// Returns `None` only when the bucket boundary would overflow the representable timestamp range.
pub fn bucket_bounds(
    timestamp: NaiveDateTime,
    spec: BucketSpec,
) -> Option<(NaiveDateTime, NaiveDateTime)> {
    let timestamp_seconds = i128::from(timestamp.and_utc().timestamp());
    let duration = i128::from(spec.duration_seconds);
    let offset = i128::from(spec.alignment_offset_seconds);
    let bucket_index = (timestamp_seconds - offset).div_euclid(duration);
    let open_seconds = bucket_index.checked_mul(duration)?.checked_add(offset)?;
    let close_seconds = open_seconds.checked_add(duration)?;
    let open_seconds = i64::try_from(open_seconds).ok()?;
    let close_seconds = i64::try_from(close_seconds).ok()?;
    let open_time = DateTime::from_timestamp(open_seconds, 0)?.naive_utc();
    let close_time = DateTime::from_timestamp(close_seconds, 0)?.naive_utc();
    Some((open_time, close_time))
}

/// One bucket being accumulated.
#[derive(Debug, Clone, PartialEq)]
struct OpenBar {
    open_time: NaiveDateTime,
    close_time: NaiveDateTime,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    tick_count: u64,
    spread_points_sum: f64,
    spread_samples: u64,
}

impl OpenBar {
    fn new(open_time: NaiveDateTime, close_time: NaiveDateTime, price: f64) -> Self {
        Self {
            open_time,
            close_time,
            open: price,
            high: price,
            low: price,
            close: price,
            tick_count: 1,
            spread_points_sum: 0.0,
            spread_samples: 0,
        }
    }

    fn update(&mut self, price: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.tick_count = self.tick_count.saturating_add(1);
    }

    fn observe_spread(&mut self, bid: f64, ask: f64, point_size: f64) {
        if point_size <= 0.0 || !point_size.is_finite() {
            return;
        }
        let spread = (ask - bid) / point_size;
        if spread.is_finite() && spread >= 0.0 {
            self.spread_points_sum += spread;
            self.spread_samples += 1;
        }
    }

    fn average_spread_points(&self) -> i32 {
        if self.spread_samples == 0 {
            return 0;
        }
        let average = self.spread_points_sum / self.spread_samples as f64;
        if !average.is_finite() || average < 0.0 {
            return 0;
        }
        average.round().min(f64::from(i32::MAX)) as i32
    }
}

/// Accumulates ticks into fixed-duration bars using the replay engine's bucket rules.
///
/// A tick is accepted only when the selected price basis is available, finite, positive, and not crossed, which matches the validity rule the replay feed applies before a quote reaches the engine. An interval with no accepted tick produces no bar, so a weekend gap simply has no bars rather than empty ones.
#[derive(Debug, Clone)]
pub struct BarAggregator {
    spec: BucketSpec,
    basis: PriceBasis,
    exchange: String,
    symbol: String,
    timeframe: Timeframe,
    point_size: f64,
    open: Option<OpenBar>,
}

impl BarAggregator {
    pub fn new(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        timeframe: Timeframe,
        spec: BucketSpec,
        basis: PriceBasis,
        point_size: f64,
    ) -> Self {
        Self {
            spec,
            basis,
            exchange: exchange.into(),
            symbol: symbol.into(),
            timeframe,
            point_size,
            open: None,
        }
    }

    /// Feed one tick, returning the bar that the tick just completed.
    pub fn push(&mut self, ts: NaiveDateTime, bid: Option<f64>, ask: Option<f64>) -> Option<Bar> {
        let (bid, ask) = (bid?, ask?);
        if !is_executable_quote(bid, ask) {
            return None;
        }
        let price = self.basis.price(bid, ask);
        if !price.is_finite() {
            return None;
        }
        let (open_time, close_time) = bucket_bounds(ts, self.spec)?;

        let Some(current) = self.open.as_mut() else {
            let mut bar = OpenBar::new(open_time, close_time, price);
            bar.observe_spread(bid, ask, self.point_size);
            self.open = Some(bar);
            return None;
        };
        if open_time == current.open_time {
            current.update(price);
            current.observe_spread(bid, ask, self.point_size);
            return None;
        }

        let completed = self.open.take().expect("open bar checked above");
        let mut next = OpenBar::new(open_time, close_time, price);
        next.observe_spread(bid, ask, self.point_size);
        self.open = Some(next);
        Some(self.build(&completed))
    }

    /// Emit the bucket that is still open, for callers that deliberately want a partial bar.
    pub fn flush(&mut self) -> Option<Bar> {
        let open = self.open.take()?;
        Some(self.build(&open))
    }

    fn build(&self, bar: &OpenBar) -> Bar {
        Bar {
            exchange: self.exchange.clone(),
            symbol: self.symbol.clone(),
            timeframe: self.timeframe,
            ts: bar.open_time,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            tick_vol: i64::try_from(bar.tick_count).unwrap_or(i64::MAX),
            volume: 0,
            spread: bar.average_spread_points(),
        }
    }
}

/// Whether a two-sided quote is executable, matching the replay feed's acceptance rule.
pub fn is_executable_quote(bid: f64, ask: f64) -> bool {
    bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && ask >= bid
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 6, 1)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    fn hourly() -> BarAggregator {
        BarAggregator::new(
            "demo",
            "EURUSD",
            Timeframe::H1,
            BucketSpec::new(3600, 0).unwrap(),
            PriceBasis::Bid,
            1.0e-5,
        )
    }

    #[test]
    fn bucket_bounds_align_to_the_offset() {
        let spec = BucketSpec::new(86_400, 79_200).unwrap();
        let (open, close) = bucket_bounds(ts(23, 0), spec).unwrap();
        assert_eq!(open, ts(22, 0));
        assert_eq!(close, ts(22, 0) + chrono::Duration::days(1));
    }

    #[test]
    fn an_offset_is_reduced_into_the_duration() {
        let spec = BucketSpec::new(3600, 7200).unwrap();
        assert_eq!(spec.alignment_offset_seconds(), 0);
    }

    #[test]
    fn a_completed_bucket_is_emitted_when_the_next_one_opens() {
        let mut aggregator = hourly();
        assert!(
            aggregator
                .push(ts(10, 0), Some(1.1), Some(1.10002))
                .is_none()
        );
        assert!(
            aggregator
                .push(ts(10, 30), Some(1.2), Some(1.20002))
                .is_none()
        );
        let bar = aggregator
            .push(ts(11, 0), Some(1.05), Some(1.05002))
            .unwrap();
        assert_eq!(bar.ts, ts(10, 0));
        assert_eq!(bar.open, 1.1);
        assert_eq!(bar.high, 1.2);
        assert_eq!(bar.low, 1.1);
        assert_eq!(bar.close, 1.2);
        assert_eq!(bar.tick_vol, 2);
    }

    #[test]
    fn an_empty_interval_produces_no_bar() {
        let mut aggregator = hourly();
        aggregator.push(ts(10, 0), Some(1.1), Some(1.10002));
        let bar = aggregator
            .push(ts(13, 0), Some(1.2), Some(1.20002))
            .unwrap();
        assert_eq!(bar.ts, ts(10, 0));
        assert!(aggregator.flush().unwrap().ts == ts(13, 0));
    }

    #[test]
    fn invalid_and_one_sided_ticks_are_skipped() {
        let mut aggregator = hourly();
        assert!(aggregator.push(ts(10, 0), None, Some(1.1)).is_none());
        assert!(aggregator.push(ts(10, 1), Some(1.1), None).is_none());
        assert!(aggregator.push(ts(10, 2), Some(1.2), Some(1.1)).is_none());
        assert!(aggregator.push(ts(10, 3), Some(-1.0), Some(1.1)).is_none());
        assert!(aggregator.flush().is_none());
    }

    #[test]
    fn the_average_spread_is_stored_in_points() {
        let mut aggregator = hourly();
        aggregator.push(ts(10, 0), Some(1.10000), Some(1.10001));
        aggregator.push(ts(10, 1), Some(1.10000), Some(1.10003));
        let bar = aggregator
            .push(ts(11, 0), Some(1.10000), Some(1.10001))
            .unwrap();
        assert_eq!(bar.spread, 2);
    }

    #[test]
    fn the_mid_basis_averages_both_sides() {
        let mut aggregator = BarAggregator::new(
            "demo",
            "EURUSD",
            Timeframe::H1,
            BucketSpec::new(3600, 0).unwrap(),
            PriceBasis::Mid,
            1.0e-5,
        );
        aggregator.push(ts(10, 0), Some(1.0), Some(1.2));
        let bar = aggregator.push(ts(11, 0), Some(1.0), Some(1.0)).unwrap();
        assert_eq!(bar.open, 1.1);
    }
}
