//! Causal fixed-duration closed bars derived from historical tick batches.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{DateTime, NaiveDateTime};

use crate::data_feed::{MarketEvent, TimestampBatch};

use super::{PriceBasis, SeriesId, SeriesRequirement, StrategyRequirements, WarmupRequirement};

pub const MAX_RETAINED_BARS: usize = 1_000_000;

/// Missing fixed-duration buckets between observed ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingIntervalPolicy {
    Skip,
    Reject,
}

/// Operational configuration for one tick-derived closed-bar series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSeriesSpec {
    requirement: SeriesRequirement,
    retained_bars: usize,
    alignment_offset_seconds: i64,
    missing_interval: MissingIntervalPolicy,
}

impl BarSeriesSpec {
    pub fn new(
        requirement: SeriesRequirement,
        retained_bars: usize,
        alignment_offset_seconds: i32,
        missing_interval: MissingIntervalPolicy,
    ) -> Result<Self, SeriesError> {
        if retained_bars == 0 {
            return Err(SeriesError::ZeroRetention {
                series_id: requirement.id().clone(),
            });
        }
        if retained_bars > MAX_RETAINED_BARS {
            return Err(SeriesError::RetentionTooLarge {
                series_id: requirement.id().clone(),
                retained_bars,
                maximum: MAX_RETAINED_BARS,
            });
        }
        let required_bars = requirement.warmup().required_bars();
        if retained_bars < required_bars {
            return Err(SeriesError::RetentionBelowWarmup {
                series_id: requirement.id().clone(),
                retained_bars,
                required_bars,
            });
        }
        let duration = i64::try_from(requirement.timeframe().duration_seconds())
            .expect("fixed timeframe duration always fits i64");
        let alignment_offset_seconds = i64::from(alignment_offset_seconds).rem_euclid(duration);
        Ok(Self {
            requirement,
            retained_bars,
            alignment_offset_seconds,
            missing_interval,
        })
    }

    pub fn requirement(&self) -> &SeriesRequirement {
        &self.requirement
    }

    pub fn retained_bars(&self) -> usize {
        self.retained_bars
    }

    pub fn alignment_offset_seconds(&self) -> i64 {
        self.alignment_offset_seconds
    }

    pub fn missing_interval(&self) -> MissingIntervalPolicy {
        self.missing_interval
    }
}

/// One immutable nonempty bar visible at its exclusive close boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedBar {
    series_id: SeriesId,
    symbol: String,
    open_time: NaiveDateTime,
    close_time: NaiveDateTime,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    tick_count: u64,
}

impl ClosedBar {
    pub fn series_id(&self) -> &SeriesId {
        &self.series_id
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn open_time(&self) -> NaiveDateTime {
        self.open_time
    }

    pub fn close_time(&self) -> NaiveDateTime {
        self.close_time
    }

    pub fn open(&self) -> f64 {
        self.open
    }

    pub fn high(&self) -> f64 {
        self.high
    }

    pub fn low(&self) -> f64 {
        self.low
    }

    pub fn close(&self) -> f64 {
        self.close
    }

    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }
}

/// Allocation-free suffix view over a possibly wrapped retained deque.
#[derive(Debug, Clone, Copy)]
pub struct BarWindow<'a> {
    older: &'a [ClosedBar],
    newer: &'a [ClosedBar],
}

impl<'a> BarWindow<'a> {
    pub fn len(&self) -> usize {
        self.older.len() + self.newer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.older.is_empty() && self.newer.is_empty()
    }

    pub fn latest(&self) -> Option<&'a ClosedBar> {
        self.newer.last().or_else(|| self.older.last())
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a ClosedBar> {
        self.older.iter().chain(self.newer.iter())
    }
}

/// Readiness facts for one configured series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeriesWarmupState {
    required: WarmupRequirement,
    available_bars: usize,
}

impl SeriesWarmupState {
    pub fn required(self) -> WarmupRequirement {
        self.required
    }

    pub fn available_bars(self) -> usize {
        self.available_bars
    }

    pub fn is_ready(self) -> bool {
        self.available_bars >= self.required.required_bars()
    }
}

/// Read-only causal history exposed to analyzers and strategies.
pub trait HistoricalSeriesView {
    fn latest_bar(&self, id: &SeriesId) -> Result<Option<&ClosedBar>, SeriesViewError>;
    fn bars(&self, id: &SeriesId, count: usize) -> Result<BarWindow<'_>, SeriesViewError>;
    fn warmup(&self, id: &SeriesId) -> Result<SeriesWarmupState, SeriesViewError>;
}

/// Errors returned while constructing or updating historical series.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeriesError {
    #[error("series '{series_id}' retained-bar capacity must be greater than zero")]
    ZeroRetention { series_id: SeriesId },
    #[error("series '{series_id}' retained-bar capacity {retained_bars} exceeds maximum {maximum}")]
    RetentionTooLarge {
        series_id: SeriesId,
        retained_bars: usize,
        maximum: usize,
    },
    #[error(
        "series '{series_id}' retained-bar capacity {retained_bars} is below warmup {required_bars}"
    )]
    RetentionBelowWarmup {
        series_id: SeriesId,
        retained_bars: usize,
        required_bars: usize,
    },
    #[error("series ID '{series_id}' is configured more than once")]
    DuplicateSeriesId { series_id: SeriesId },
    #[error("batch at {batch_ts} contains an event at {event_ts}")]
    BatchTimestampMismatch {
        batch_ts: NaiveDateTime,
        event_ts: NaiveDateTime,
    },
    #[error("duplicate event ordering metadata ({series_rank}, {row_sequence}) at {timestamp}")]
    DuplicateOrderingMetadata {
        timestamp: NaiveDateTime,
        series_rank: u32,
        row_sequence: u64,
    },
    #[error("primary tick for '{symbol}' moved backwards from {previous} to {current}")]
    TimestampRegression {
        symbol: String,
        previous: NaiveDateTime,
        current: NaiveDateTime,
    },
    #[error("series '{series_id}' cannot represent a bucket boundary for {timestamp}")]
    BoundaryOverflow {
        series_id: SeriesId,
        timestamp: NaiveDateTime,
    },
    #[error("series '{series_id}' has one or more empty intervals before {next_open}")]
    MissingInterval {
        series_id: SeriesId,
        previous_close: NaiveDateTime,
        next_open: NaiveDateTime,
    },
    #[error("series '{series_id}' tick count overflowed")]
    TickCountOverflow { series_id: SeriesId },
    #[error("series '{series_id}' completed-bar count overflowed")]
    CompletedBarCountOverflow { series_id: SeriesId },
}

/// Errors returned by read-only series lookup.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeriesViewError {
    #[error("unknown historical series '{series_id}'")]
    UnknownSeries { series_id: SeriesId },
}

#[derive(Debug, Clone)]
struct OpenBar {
    open_time: NaiveDateTime,
    close_time: NaiveDateTime,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    tick_count: u64,
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
        }
    }

    fn update(&mut self, series_id: &SeriesId, price: f64) -> Result<(), SeriesError> {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.tick_count =
            self.tick_count
                .checked_add(1)
                .ok_or_else(|| SeriesError::TickCountOverflow {
                    series_id: series_id.clone(),
                })?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct SeriesState {
    spec: BarSeriesSpec,
    open: Option<OpenBar>,
    closed: VecDeque<ClosedBar>,
    completed_bars: usize,
}

impl SeriesState {
    fn new(spec: BarSeriesSpec) -> Self {
        Self {
            closed: VecDeque::with_capacity(spec.retained_bars),
            spec,
            open: None,
            completed_bars: 0,
        }
    }

    fn duration_seconds(&self) -> i64 {
        i64::try_from(self.spec.requirement.timeframe().duration_seconds())
            .expect("fixed timeframe duration always fits i64")
    }
}

#[derive(Debug)]
struct BatchSeriesState {
    open: Option<OpenBar>,
    completed_bars: usize,
    emitted: Vec<ClosedBar>,
}

impl BatchSeriesState {
    fn from_committed(state: &SeriesState) -> Self {
        Self {
            open: state.open.clone(),
            completed_bars: state.completed_bars,
            emitted: Vec::new(),
        }
    }

    fn apply_tick(
        &mut self,
        spec: &BarSeriesSpec,
        timestamp: NaiveDateTime,
        bid: f64,
        ask: f64,
    ) -> Result<(), SeriesError> {
        let price = match spec.requirement.price_basis() {
            PriceBasis::Bid => bid,
            PriceBasis::Ask => ask,
            PriceBasis::Mid => bid + (ask - bid) / 2.0,
        };
        let duration = i64::try_from(spec.requirement.timeframe().duration_seconds())
            .expect("fixed timeframe duration always fits i64");
        let (open_time, close_time) = bucket_bounds(
            spec.requirement.id(),
            timestamp,
            duration,
            spec.alignment_offset_seconds,
        )?;

        let Some(current) = self.open.as_mut() else {
            self.open = Some(OpenBar::new(open_time, close_time, price));
            return Ok(());
        };
        if open_time == current.open_time {
            current.update(spec.requirement.id(), price)?;
            return Ok(());
        }

        if spec.missing_interval == MissingIntervalPolicy::Reject && open_time > current.close_time
        {
            return Err(SeriesError::MissingInterval {
                series_id: spec.requirement.id().clone(),
                previous_close: current.close_time,
                next_open: open_time,
            });
        }

        let completed = self
            .open
            .take()
            .expect("open bar exists after transition validation");
        self.completed_bars = self.completed_bars.checked_add(1).ok_or_else(|| {
            SeriesError::CompletedBarCountOverflow {
                series_id: spec.requirement.id().clone(),
            }
        })?;
        self.emitted.push(ClosedBar {
            series_id: spec.requirement.id().clone(),
            symbol: spec.requirement.symbol().to_string(),
            open_time: completed.open_time,
            close_time: completed.close_time,
            open: completed.open,
            high: completed.high,
            low: completed.low,
            close: completed.close,
            tick_count: completed.tick_count,
        });
        self.open = Some(OpenBar::new(open_time, close_time, price));
        Ok(())
    }
}

/// Bounded causal closed-bar state for several symbols and timeframes.
#[derive(Debug)]
pub struct MultiTimeframeSeries {
    series: BTreeMap<SeriesId, SeriesState>,
    last_source_ts: BTreeMap<String, NaiveDateTime>,
}

impl MultiTimeframeSeries {
    pub fn new(specs: Vec<BarSeriesSpec>) -> Result<Self, SeriesError> {
        let mut series = BTreeMap::new();
        for spec in specs {
            let id = spec.requirement.id().clone();
            if series.insert(id.clone(), SeriesState::new(spec)).is_some() {
                return Err(SeriesError::DuplicateSeriesId { series_id: id });
            }
        }
        Ok(Self {
            series,
            last_source_ts: BTreeMap::new(),
        })
    }

    pub fn on_batch(&mut self, batch: &TimestampBatch) -> Result<Vec<ClosedBar>, SeriesError> {
        validate_batch(batch)?;
        let mut staged_series = self
            .series
            .iter()
            .map(|(id, state)| (id.clone(), BatchSeriesState::from_committed(state)))
            .collect::<BTreeMap<_, _>>();
        let mut staged_source_ts = self.last_source_ts.clone();
        self.preflight_batch(batch, &mut staged_series, &mut staged_source_ts)?;

        let mut emitted = Vec::new();
        for (id, mut staged) in staged_series {
            let state = self
                .series
                .get_mut(&id)
                .expect("staged series originates from committed state");
            state.open = staged.open;
            state.completed_bars = staged.completed_bars;
            for closed in staged.emitted.drain(..) {
                if state.closed.len() == state.spec.retained_bars {
                    state.closed.pop_front();
                }
                state.closed.push_back(closed.clone());
                emitted.push(closed);
            }
        }
        self.last_source_ts = staged_source_ts;
        emitted.sort_by(|left, right| {
            left.close_time
                .cmp(&right.close_time)
                .then_with(|| {
                    self.series[left.series_id()]
                        .duration_seconds()
                        .cmp(&self.series[right.series_id()].duration_seconds())
                })
                .then_with(|| left.series_id.cmp(&right.series_id))
        });
        Ok(emitted)
    }

    pub fn latest_bar(&self, series: &SeriesId) -> Result<Option<&ClosedBar>, SeriesViewError> {
        HistoricalSeriesView::latest_bar(self, series)
    }

    pub fn bars(&self, series: &SeriesId, count: usize) -> Result<BarWindow<'_>, SeriesViewError> {
        HistoricalSeriesView::bars(self, series, count)
    }

    pub fn warmup(&self, series: &SeriesId) -> Result<SeriesWarmupState, SeriesViewError> {
        HistoricalSeriesView::warmup(self, series)
    }

    pub fn warmup_complete(
        &self,
        requirements: &StrategyRequirements,
    ) -> Result<bool, SeriesViewError> {
        for requirement in requirements.series() {
            self.state(requirement.id())?;
        }
        Ok(requirements
            .warmup_complete(|id| self.series.get(id).map_or(0, |state| state.completed_bars)))
    }

    fn preflight_batch(
        &self,
        batch: &TimestampBatch,
        staged_series: &mut BTreeMap<SeriesId, BatchSeriesState>,
        staged_source_ts: &mut BTreeMap<String, NaiveDateTime>,
    ) -> Result<(), SeriesError> {
        let mut ordered = batch.events.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|event| (event.metadata.series_rank, event.metadata.row_sequence));

        for feed_event in ordered {
            if !feed_event.metadata.roles.primary {
                continue;
            }
            let MarketEvent::Tick {
                symbol,
                ts,
                bid,
                ask,
            } = &feed_event.event
            else {
                continue;
            };
            if let Some(previous) = staged_source_ts.get(symbol)
                && *ts < *previous
            {
                return Err(SeriesError::TimestampRegression {
                    symbol: symbol.clone(),
                    previous: *previous,
                    current: *ts,
                });
            }
            staged_source_ts.insert(symbol.clone(), *ts);

            if feed_event.event.to_valid_quote().is_none() {
                continue;
            }
            for (id, state) in self
                .series
                .iter()
                .filter(|(_, state)| state.spec.requirement.symbol() == symbol)
            {
                staged_series
                    .get_mut(id)
                    .expect("staged series originates from committed state")
                    .apply_tick(&state.spec, *ts, *bid, *ask)?;
            }
        }
        Ok(())
    }

    fn state(&self, id: &SeriesId) -> Result<&SeriesState, SeriesViewError> {
        self.series
            .get(id)
            .ok_or_else(|| SeriesViewError::UnknownSeries {
                series_id: id.clone(),
            })
    }
}

impl HistoricalSeriesView for MultiTimeframeSeries {
    fn latest_bar(&self, id: &SeriesId) -> Result<Option<&ClosedBar>, SeriesViewError> {
        Ok(self.state(id)?.closed.back())
    }

    fn bars(&self, id: &SeriesId, count: usize) -> Result<BarWindow<'_>, SeriesViewError> {
        let state = self.state(id)?;
        let (older, newer) = state.closed.as_slices();
        let available = count.min(state.closed.len());
        let skip = state.closed.len() - available;
        if skip < older.len() {
            Ok(BarWindow {
                older: &older[skip..],
                newer,
            })
        } else {
            Ok(BarWindow {
                older: &older[older.len()..],
                newer: &newer[skip - older.len()..],
            })
        }
    }

    fn warmup(&self, id: &SeriesId) -> Result<SeriesWarmupState, SeriesViewError> {
        let state = self.state(id)?;
        Ok(SeriesWarmupState {
            required: state.spec.requirement.warmup(),
            available_bars: state.completed_bars,
        })
    }
}

fn validate_batch(batch: &TimestampBatch) -> Result<(), SeriesError> {
    let mut ordering = BTreeSet::new();
    for feed_event in &batch.events {
        let event_ts = feed_event.event.ts();
        if event_ts != batch.ts {
            return Err(SeriesError::BatchTimestampMismatch {
                batch_ts: batch.ts,
                event_ts,
            });
        }
        let key = (
            feed_event.metadata.series_rank,
            feed_event.metadata.row_sequence,
        );
        if !ordering.insert(key) {
            return Err(SeriesError::DuplicateOrderingMetadata {
                timestamp: batch.ts,
                series_rank: key.0,
                row_sequence: key.1,
            });
        }
    }
    Ok(())
}

fn bucket_bounds(
    series_id: &SeriesId,
    timestamp: NaiveDateTime,
    duration: i64,
    offset: i64,
) -> Result<(NaiveDateTime, NaiveDateTime), SeriesError> {
    let timestamp_seconds = i128::from(timestamp.and_utc().timestamp());
    let duration = i128::from(duration);
    let offset = i128::from(offset);
    let bucket_index = (timestamp_seconds - offset).div_euclid(duration);
    let open_seconds = bucket_index
        .checked_mul(duration)
        .and_then(|value| value.checked_add(offset))
        .ok_or_else(|| SeriesError::BoundaryOverflow {
            series_id: series_id.clone(),
            timestamp,
        })?;
    let close_seconds =
        open_seconds
            .checked_add(duration)
            .ok_or_else(|| SeriesError::BoundaryOverflow {
                series_id: series_id.clone(),
                timestamp,
            })?;
    let open_seconds = i64::try_from(open_seconds).map_err(|_| SeriesError::BoundaryOverflow {
        series_id: series_id.clone(),
        timestamp,
    })?;
    let close_seconds =
        i64::try_from(close_seconds).map_err(|_| SeriesError::BoundaryOverflow {
            series_id: series_id.clone(),
            timestamp,
        })?;
    let open_time = DateTime::from_timestamp(open_seconds, 0)
        .map(|value| value.naive_utc())
        .ok_or_else(|| SeriesError::BoundaryOverflow {
            series_id: series_id.clone(),
            timestamp,
        })?;
    let close_time = DateTime::from_timestamp(close_seconds, 0)
        .map(|value| value.naive_utc())
        .ok_or_else(|| SeriesError::BoundaryOverflow {
            series_id: series_id.clone(),
            timestamp,
        })?;
    Ok((open_time, close_time))
}
