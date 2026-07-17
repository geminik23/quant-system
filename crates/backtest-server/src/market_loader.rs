//! Reopenable Parquet market streams for FutureQuote replay.

use std::path::Path;
use std::sync::Arc;

use chrono::NaiveDateTime;
use data_preprocess::scanner::{ParquetBarScan, ParquetTickScan};
use data_preprocess::{Bar, DataError, ParquetScanBounds, Tick, Timeframe};
use qs_backtest::data_feed::{
    EventBatchFeed, EventBatchFeedError, KWayMergeError, KWayMergeFeed, MarketEvent,
    SequencedMarketEvent, SeriesRoles,
};

use crate::error::{BacktestServerError, Result};

pub(crate) type CancellationCheck = Arc<dyn Fn() -> bool>;
type EventSource = Box<dyn FnMut() -> Result<Option<SequencedMarketEvent>>>;
type SeriesFeed = EventBatchFeed<EventSource, SequencedMarketEvent>;
pub(crate) type MarketStream = KWayMergeFeed<SeriesFeed>;
pub(crate) type MarketStreamError = KWayMergeError<EventBatchFeedError<BacktestServerError>>;

#[derive(Debug, Clone)]
enum MarketSeriesSource {
    Tick { scan: ParquetTickScan },
    Bar { scan: ParquetBarScan },
    Unavailable { message: String },
    Empty,
}

#[derive(Debug, Clone)]
pub(crate) struct MarketSeriesDescription {
    canonical_symbol: String,
    roles: SeriesRoles,
    source: MarketSeriesSource,
}

impl MarketSeriesDescription {
    fn tick(scan: ParquetTickScan, canonical_symbol: String, roles: SeriesRoles) -> Self {
        Self {
            canonical_symbol,
            roles,
            source: MarketSeriesSource::Tick { scan },
        }
    }

    fn bar(scan: ParquetBarScan, canonical_symbol: String, roles: SeriesRoles) -> Self {
        Self {
            canonical_symbol,
            roles,
            source: MarketSeriesSource::Bar { scan },
        }
    }

    pub(crate) fn conversion_tick(
        data_dir: &str,
        exchange: String,
        symbol: String,
        canonical_symbol: String,
        bounds: ParquetScanBounds,
    ) -> Self {
        match ParquetTickScan::describe(data_dir, &exchange, &symbol, bounds) {
            Ok(scan) => Self::tick(scan, canonical_symbol, SeriesRoles::CONVERSION),
            Err(error) => Self {
                canonical_symbol,
                roles: SeriesRoles::CONVERSION,
                source: MarketSeriesSource::Unavailable {
                    message: error.to_string(),
                },
            },
        }
    }

    pub(crate) fn empty_conversion(_data_dir: &str, canonical_symbol: String) -> Self {
        Self::empty(canonical_symbol, SeriesRoles::CONVERSION)
    }

    fn empty_primary(_data_dir: &str, canonical_symbol: String) -> Self {
        Self::empty(canonical_symbol, SeriesRoles::PRIMARY)
    }

    fn empty(canonical_symbol: String, roles: SeriesRoles) -> Self {
        Self {
            canonical_symbol,
            roles,
            source: MarketSeriesSource::Empty,
        }
    }

    fn open(&self, is_cancelled: CancellationCheck, series_rank: u32) -> Result<SeriesFeed> {
        ensure_not_cancelled(&is_cancelled)?;
        let source: EventSource = match &self.source {
            MarketSeriesSource::Tick { scan } => {
                let mut cursor = scan.cursor().map_err(map_data_error)?;
                let canonical_symbol = self.canonical_symbol.clone();
                Box::new(move || {
                    loop {
                        ensure_not_cancelled(&is_cancelled)?;
                        let Some(scanned) = cursor
                            .next_tick_with_ordinal_cancellable({
                                let is_cancelled = is_cancelled.clone();
                                move || is_cancelled()
                            })
                            .map_err(map_data_error)?
                        else {
                            return Ok(None);
                        };
                        if let Some(event) = tick_to_valid_event(scanned.row, &canonical_symbol) {
                            return Ok(Some(SequencedMarketEvent::new(
                                event,
                                scanned.source_row_ordinal,
                            )));
                        }
                    }
                })
            }
            MarketSeriesSource::Bar { scan } => {
                let mut cursor = scan.cursor().map_err(map_data_error)?;
                let canonical_symbol = self.canonical_symbol.clone();
                Box::new(move || {
                    ensure_not_cancelled(&is_cancelled)?;
                    cursor
                        .next_bar_with_ordinal_cancellable({
                            let is_cancelled = is_cancelled.clone();
                            move || is_cancelled()
                        })
                        .map_err(map_data_error)
                        .map(|bar| {
                            bar.map(|scanned| {
                                SequencedMarketEvent::new(
                                    bar_to_event(scanned.row, &canonical_symbol),
                                    scanned.source_row_ordinal,
                                )
                            })
                        })
                })
            }
            MarketSeriesSource::Unavailable { message } => {
                return Err(BacktestServerError::Database(DataError::Other(
                    message.clone(),
                )));
            }
            MarketSeriesSource::Empty => Box::new(move || {
                ensure_not_cancelled(&is_cancelled)?;
                Ok(None)
            }),
        };
        Ok(EventBatchFeed::new(source, self.roles, series_rank))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MarketStreamDescription {
    series: Vec<MarketSeriesDescription>,
    primary_start: Option<NaiveDateTime>,
    primary_eod: Option<NaiveDateTime>,
    requested_to: Option<NaiveDateTime>,
}

impl MarketStreamDescription {
    fn new(
        series: Vec<MarketSeriesDescription>,
        primary_start: Option<NaiveDateTime>,
        primary_eod: Option<NaiveDateTime>,
        requested_to: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            series,
            primary_start,
            primary_eod,
            requested_to,
        }
    }

    pub(crate) fn primary_start(&self) -> Option<NaiveDateTime> {
        self.primary_start
    }

    pub(crate) fn primary_eod(&self) -> Option<NaiveDateTime> {
        self.primary_eod
    }

    pub(crate) fn conversion_end(&self) -> Option<NaiveDateTime> {
        self.primary_eod.or(self.requested_to)
    }

    pub(crate) fn primary_series_count(&self) -> usize {
        self.series.len()
    }

    pub(crate) fn mark_shared_conversion_symbols(
        &mut self,
        shared_symbols: &std::collections::BTreeSet<String>,
    ) {
        for series in &mut self.series {
            if shared_symbols.contains(&series.canonical_symbol) {
                series.roles = SeriesRoles::PRIMARY_AND_CONVERSION;
            }
        }
    }

    pub(crate) fn push_series(&mut self, series: MarketSeriesDescription) {
        self.series.push(series);
    }

    pub(crate) fn open(&self, is_cancelled: CancellationCheck) -> Result<MarketStream> {
        let feeds = self
            .series
            .iter()
            .enumerate()
            .map(|(rank, series)| {
                let rank = u32::try_from(rank).map_err(|_| {
                    BacktestServerError::InvalidRequest(
                        "FutureQuote stream supports at most u32::MAX series".into(),
                    )
                })?;
                series.open(is_cancelled.clone(), rank)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(KWayMergeFeed::new(feeds))
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn describe_primary_market_stream(
    data_dir: &str,
    exchange: &str,
    symbols: &[String],
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    is_cancelled: &mut dyn FnMut() -> bool,
    progress: &mut dyn FnMut(u64),
) -> Result<MarketStreamDescription> {
    ensure_not_cancelled_mut(is_cancelled)?;
    if symbols.is_empty() {
        return Ok(MarketStreamDescription::new(Vec::new(), None, None, to));
    }
    if from.zip(to).is_some_and(|(from, to)| from > to) {
        progress(symbols.len() as u64);
        return Ok(MarketStreamDescription::new(
            symbols
                .iter()
                .cloned()
                .map(|symbol| MarketSeriesDescription::empty_primary(data_dir, symbol))
                .collect(),
            None,
            None,
            to,
        ));
    }

    let bounds = ParquetScanBounds::new(from, to);
    let data_type = data_type.to_ascii_lowercase();
    let mut series = Vec::with_capacity(symbols.len());
    let mut primary_start: Option<NaiveDateTime> = None;
    let mut primary_eod: Option<NaiveDateTime> = None;

    for (index, canonical_symbol) in symbols.iter().enumerate() {
        ensure_not_cancelled_mut(is_cancelled)?;
        let (description, first, last) = if data_type == "tick" {
            let disk_exchange =
                resolve_partition_value(data_dir, "ticks", "exchange", exchange, "", is_cancelled)?;
            let disk_symbol = resolve_partition_value(
                data_dir,
                "ticks",
                "symbol",
                canonical_symbol,
                &format!("exchange={disk_exchange}"),
                is_cancelled,
            )?;
            let scan = ParquetTickScan::describe_cancellable(
                data_dir,
                &disk_exchange,
                &disk_symbol,
                bounds,
                &mut *is_cancelled,
            )
            .map_err(map_data_error)?;
            let (first, last) = primary_tick_edges(&scan, canonical_symbol, is_cancelled)?;
            (
                MarketSeriesDescription::tick(scan, canonical_symbol.clone(), SeriesRoles::PRIMARY),
                first,
                last,
            )
        } else if data_type == "bar" {
            let timeframe = timeframe.ok_or_else(|| {
                BacktestServerError::InvalidRequest("timeframe is required for bar data".into())
            })?;
            let parsed = Timeframe::parse(timeframe).map_err(|_| {
                BacktestServerError::InvalidRequest(format!("Invalid timeframe: '{timeframe}'"))
            })?;
            let disk_exchange =
                resolve_partition_value(data_dir, "bars", "exchange", exchange, "", is_cancelled)?;
            let disk_symbol = resolve_partition_value(
                data_dir,
                "bars",
                "symbol",
                canonical_symbol,
                &format!("exchange={disk_exchange}"),
                is_cancelled,
            )?;
            let disk_timeframe = resolve_partition_value(
                data_dir,
                "bars",
                "timeframe",
                parsed.as_str(),
                &format!("exchange={disk_exchange}/symbol={disk_symbol}"),
                is_cancelled,
            )?;
            let scan = ParquetBarScan::describe_cancellable(
                data_dir,
                &disk_exchange,
                &disk_symbol,
                &disk_timeframe,
                bounds,
                &mut *is_cancelled,
            )
            .map_err(map_data_error)?;
            let (first, last) = primary_bar_edges(&scan, canonical_symbol, is_cancelled)?;
            (
                MarketSeriesDescription::bar(scan, canonical_symbol.clone(), SeriesRoles::PRIMARY),
                first,
                last,
            )
        } else {
            return Err(BacktestServerError::InvalidRequest(format!(
                "Invalid data_type: '{data_type}'. Must be 'tick' or 'bar'."
            )));
        };

        let Some(first) = first else {
            return Err(BacktestServerError::NoDataFound {
                symbol: canonical_symbol.clone(),
                exchange: exchange.to_owned(),
                data_type: data_type.clone(),
            });
        };
        let last = last.expect("a first valid primary quote has a last quote");
        primary_start = Some(primary_start.map_or(first, |current| current.min(first)));
        primary_eod = Some(primary_eod.map_or(last, |current| current.max(last)));
        series.push(description);
        progress((index + 1) as u64);
    }

    Ok(MarketStreamDescription::new(
        series,
        primary_start,
        primary_eod,
        to,
    ))
}

fn primary_tick_edges(
    scan: &ParquetTickScan,
    canonical_symbol: &str,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<(Option<NaiveDateTime>, Option<NaiveDateTime>)> {
    let mut cursor = scan.cursor().map_err(map_data_error)?;
    let mut first = None;
    while let Some(tick) = cursor
        .next_tick_cancellable(&mut *is_cancelled)
        .map_err(map_data_error)?
    {
        let timestamp = tick.ts;
        if tick_to_valid_event(tick, canonical_symbol).is_some() {
            first = Some(timestamp);
            break;
        }
    }
    let Some(first) = first else {
        return Ok((None, None));
    };

    let last = scan
        .latest_valid_tick_cancellable(&mut *is_cancelled)
        .map_err(map_data_error)?
        .map_or(first, |tick| tick.row.ts);
    Ok((Some(first), Some(last)))
}

fn primary_bar_edges(
    scan: &ParquetBarScan,
    canonical_symbol: &str,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<(Option<NaiveDateTime>, Option<NaiveDateTime>)> {
    let mut cursor = scan.cursor().map_err(map_data_error)?;
    let mut first = None;
    while let Some(bar) = cursor
        .next_bar_cancellable(&mut *is_cancelled)
        .map_err(map_data_error)?
    {
        let event = bar_to_event(bar, canonical_symbol);
        if event.to_valid_quote().is_some() {
            first = Some(event.ts());
            break;
        }
    }
    let Some(first) = first else {
        return Ok((None, None));
    };

    let last = scan
        .latest_valid_bar_cancellable(&mut *is_cancelled)
        .map_err(map_data_error)?
        .map_or(first, |bar| bar.row.ts);
    Ok((Some(first), Some(last)))
}

fn tick_to_valid_event(tick: Tick, canonical_symbol: &str) -> Option<MarketEvent> {
    let event = MarketEvent::Tick {
        symbol: canonical_symbol.to_owned(),
        ts: tick.ts,
        bid: tick.bid?,
        ask: tick.ask?,
    };
    event.to_valid_quote().map(|_| event)
}

fn bar_to_event(bar: Bar, canonical_symbol: &str) -> MarketEvent {
    MarketEvent::Bar {
        symbol: canonical_symbol.to_owned(),
        ts: bar.ts,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
    }
}

fn resolve_partition_value(
    data_dir: &str,
    data_subdir: &str,
    key: &str,
    requested: &str,
    parent: &str,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<String> {
    let dir = if parent.is_empty() {
        Path::new(data_dir).join(data_subdir)
    } else {
        Path::new(data_dir).join(data_subdir).join(parent)
    };
    let prefix = format!("{key}=");
    let mut matches = Vec::new();

    ensure_not_cancelled_mut(is_cancelled)?;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            ensure_not_cancelled_mut(is_cancelled)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(value) = name.strip_prefix(&prefix) else {
                continue;
            };
            if value.eq_ignore_ascii_case(requested) {
                matches.push(value.to_owned());
            }
        }
    }
    matches.sort();
    ensure_not_cancelled_mut(is_cancelled)?;
    Ok(matches
        .iter()
        .find(|value| value.as_str() == requested)
        .cloned()
        .or_else(|| matches.into_iter().next())
        .unwrap_or_else(|| requested.to_owned()))
}

fn ensure_not_cancelled(is_cancelled: &CancellationCheck) -> Result<()> {
    if is_cancelled() {
        Err(BacktestServerError::Cancelled)
    } else {
        Ok(())
    }
}

fn ensure_not_cancelled_mut(is_cancelled: &mut dyn FnMut() -> bool) -> Result<()> {
    if is_cancelled() {
        Err(BacktestServerError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_data_error(error: DataError) -> BacktestServerError {
    match error {
        DataError::Cancelled => BacktestServerError::Cancelled,
        other => BacktestServerError::Database(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use data_preprocess::ParquetStore;
    use qs_backtest::data_feed::FallibleBatchFeed;

    fn ts(second: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 2, 3)
            .unwrap()
            .and_hms_opt(10, 0, second)
            .unwrap()
    }

    fn temp_data_dir() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("qs-market-stream-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn tick(symbol: &str, second: u32) -> Tick {
        Tick {
            exchange: "Fixture".into(),
            symbol: symbol.into(),
            ts: ts(second),
            bid: Some(1.1 + second as f64 / 10_000.0),
            ask: Some(1.2 + second as f64 / 10_000.0),
            last: None,
            volume: None,
            flags: None,
        }
    }

    fn collect(mut stream: MarketStream) -> Vec<(NaiveDateTime, String, u32, SeriesRoles)> {
        let mut events = Vec::new();
        while let Some(batch) = FallibleBatchFeed::next_batch(&mut stream).unwrap() {
            events.extend(batch.events.into_iter().map(|event| {
                (
                    event.event.ts(),
                    event.event.symbol().to_owned(),
                    event.metadata.series_rank,
                    event.metadata.roles,
                )
            }));
        }
        events
    }

    fn collect_ordinals(mut stream: MarketStream) -> Vec<(NaiveDateTime, u64)> {
        let mut events = Vec::new();
        while let Some(batch) = FallibleBatchFeed::next_batch(&mut stream).unwrap() {
            events.extend(
                batch
                    .events
                    .into_iter()
                    .map(|event| (event.event.ts(), event.metadata.row_sequence)),
            );
        }
        events
    }

    #[test]
    fn description_reopens_active_symbol_cursors_with_inclusive_bounds() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        store
            .insert_ticks(&[
                tick("EURUSD", 0),
                tick("EURUSD", 1),
                tick("EURUSD", 2),
                tick("GBPUSD", 1),
            ])
            .unwrap();
        let mut never_cancelled = || false;
        let description = describe_primary_market_stream(
            data_dir.to_str().unwrap(),
            "fixture",
            &["eurusd".into()],
            "tick",
            None,
            Some(ts(0)),
            Some(ts(1)),
            &mut never_cancelled,
            &mut |_| {},
        )
        .unwrap();

        assert_eq!(description.primary_start(), Some(ts(0)));
        assert_eq!(description.primary_eod(), Some(ts(1)));
        let expected = vec![
            (ts(0), "eurusd".into(), 0, SeriesRoles::PRIMARY),
            (ts(1), "eurusd".into(), 0, SeriesRoles::PRIMARY),
        ];
        let first = collect(description.open(Arc::new(|| false)).unwrap());
        let reopened = collect(description.clone().open(Arc::new(|| false)).unwrap());
        assert_eq!(first, expected);
        assert_eq!(reopened, expected);

        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn stream_metadata_retains_physical_ordinals_across_invalid_ticks() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        let mut invalid = tick("EURUSD", 0);
        invalid.ask = None;
        store
            .insert_ticks(&[invalid, tick("EURUSD", 1), tick("EURUSD", 2)])
            .unwrap();
        let mut never_cancelled = || false;
        let description = describe_primary_market_stream(
            data_dir.to_str().unwrap(),
            "fixture",
            &["eurusd".into()],
            "tick",
            None,
            Some(ts(0)),
            Some(ts(2)),
            &mut never_cancelled,
            &mut |_| {},
        )
        .unwrap();

        assert_eq!(
            collect_ordinals(description.open(Arc::new(|| false)).unwrap()),
            vec![(ts(1), 1), (ts(2), 2)]
        );

        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn described_stream_rejects_a_replaced_partition_before_reopen() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        store
            .insert_ticks(&[tick("EURUSD", 0), tick("EURUSD", 1)])
            .unwrap();
        let mut never_cancelled = || false;
        let description = describe_primary_market_stream(
            data_dir.to_str().unwrap(),
            "fixture",
            &["eurusd".into()],
            "tick",
            None,
            Some(ts(0)),
            Some(ts(2)),
            &mut never_cancelled,
            &mut |_| {},
        )
        .unwrap();

        store.insert_ticks(&[tick("EURUSD", 2)]).unwrap();
        assert!(matches!(
            description.open(Arc::new(|| false)),
            Err(BacktestServerError::Database(
                DataError::ParquetPartitionChanged { .. }
            ))
        ));

        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn description_scan_honors_cancellation_before_storage_access() {
        let mut cancelled = || true;
        let error = describe_primary_market_stream(
            "/path/that/does/not/exist",
            "fixture",
            &["eurusd".into()],
            "tick",
            None,
            None,
            None,
            &mut cancelled,
            &mut |_| {},
        )
        .unwrap_err();
        assert!(matches!(error, BacktestServerError::Cancelled));
    }
}
