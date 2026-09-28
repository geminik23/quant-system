//! Reopenable stored-market-data streams for historical replay.
//!
//! This crate is the bridge between stored market data and the replay engine. `qs-data-preprocess` owns the storage layout and its chronological cursors, `qs-backtest` consumes ordered market events, and nothing in either crate reads the other's world. The bridge lives here so that both the backtest service and an in-process parameter search open the same streams through the same code, instead of each growing its own loader that could silently diverge.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use chrono::NaiveDateTime;
use data_preprocess::scanner::{ParquetBarScan, ParquetTickScan};
use data_preprocess::{Bar, DataError, ParquetScanBounds, ParquetStore, PriceBar, Tick, Timeframe};
use qs_backtest::currency::{ConversionRoute, resolve_conversion_route};
use qs_backtest::data_feed::{
    EventBatchFeed, EventBatchFeedError, FeedEvent, KWayMergeError, KWayMergeFeed, MarketEvent,
    SequencedMarketEvent, SeriesRoles,
};
use qs_backtest::{
    HistoricalNamedInputProjector, NamedInputProjectionContext, NamedInputProjectionError,
    ProjectedNamedInput, ReplayInstrumentManifest, SeriesId,
};
use qs_strategy::{ScalarType, Value, ValueType};
use qs_symbols::SymbolRegistry;

mod error;

pub use data_preprocess::{
    CountCapability, PriceBins, QuoteStatistics, SeriesDescriptor, StoredPriceBasis, StoredTick,
};
pub use error::{MarketLoadError, Result};

pub type CancellationCheck = Arc<dyn Fn() -> bool>;
type EventSource = Box<dyn FnMut() -> Result<Option<SequencedMarketEvent>>>;
type SeriesFeed = EventBatchFeed<EventSource, SequencedMarketEvent>;
pub type MarketStream = KWayMergeFeed<SeriesFeed>;
pub type MarketStreamError = KWayMergeError<EventBatchFeedError<MarketLoadError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteStatisticKind {
    Accepted,
    Rejected,
    DuplicateProviderRows,
    CoverageMillis,
    SpreadLast,
    SpreadMean,
    SpreadMax,
    SpreadP90,
    SpreadTimeMean,
    SpreadBpsMean,
    QuoteActivity,
    QuoteRatePerSecond,
    MidChangeCount,
    InterarrivalMeanMillis,
    InterarrivalMaxMillis,
    InterarrivalCv,
    DirectionChanges,
    LongestDirectionStreak,
    PathLength,
    PathEfficiency,
    MidChangeVariance,
    FirstHighAt,
    LastHighAt,
    FirstLowAt,
    LastLowAt,
    Twap,
    Crossings,
    CumulativeAboveMillis,
    ContinuousAboveMillis,
    ElapsedSinceBreakoutMillis,
    DominantBin,
    DominantBinCenter,
    DistanceFromDominantCenter,
}

impl QuoteStatisticKind {
    fn scalar_type(self) -> ScalarType {
        match self {
            Self::Accepted
            | Self::Rejected
            | Self::DuplicateProviderRows
            | Self::CoverageMillis
            | Self::QuoteActivity
            | Self::MidChangeCount
            | Self::InterarrivalMaxMillis
            | Self::DirectionChanges
            | Self::LongestDirectionStreak
            | Self::Crossings
            | Self::CumulativeAboveMillis
            | Self::ContinuousAboveMillis
            | Self::ElapsedSinceBreakoutMillis => ScalarType::Integer,
            Self::SpreadBpsMean
            | Self::QuoteRatePerSecond
            | Self::InterarrivalCv
            | Self::PathEfficiency => ScalarType::Ratio,
            Self::Twap | Self::DominantBinCenter => ScalarType::Price,
            Self::FirstHighAt | Self::LastHighAt | Self::FirstLowAt | Self::LastLowAt => {
                ScalarType::Timestamp
            }
            _ => ScalarType::Number,
        }
    }
}

#[derive(Default)]
struct QuoteProjectorState {
    last_open: Option<NaiveDateTime>,
    retained: Option<Value>,
}

/// Projects exact quote-window statistics from an immutable, source-ordered tick slice.
pub struct QuoteStatisticsProjector {
    series_id: SeriesId,
    ticks: Arc<[StoredTick]>,
    kind: QuoteStatisticKind,
    captured_level: Option<f64>,
    bins: Option<PriceBins>,
    maximum_rows_per_query: usize,
    state: RefCell<QuoteProjectorState>,
}

impl QuoteStatisticsProjector {
    pub fn new(
        series_id: SeriesId,
        ticks: Arc<[StoredTick]>,
        kind: QuoteStatisticKind,
        captured_level: Option<f64>,
        bins: Option<PriceBins>,
        maximum_rows_per_query: usize,
    ) -> Result<Self> {
        if maximum_rows_per_query == 0 {
            return Err(MarketLoadError::InvalidSeries(
                "quote projector row limit must be positive".into(),
            ));
        }
        if ticks.windows(2).any(|rows| {
            (rows[0].tick.ts, rows[0].source_ordinal) > (rows[1].tick.ts, rows[1].source_ordinal)
        }) {
            return Err(MarketLoadError::InvalidSeries(
                "quote projector ticks are not source ordered".into(),
            ));
        }
        Ok(Self {
            series_id,
            ticks,
            kind,
            captured_level,
            bins,
            maximum_rows_per_query,
            state: RefCell::new(QuoteProjectorState::default()),
        })
    }

    fn value(&self, statistics: &QuoteStatistics) -> Value {
        let integer = |value: Option<i64>| {
            value
                .map(Value::Integer)
                .unwrap_or(Value::Missing(ScalarType::Integer))
        };
        let number = |value: Option<f64>| {
            value
                .map(Value::Number)
                .unwrap_or(Value::Missing(ScalarType::Number))
        };
        let ratio = |value: Option<f64>| {
            value
                .map(Value::Ratio)
                .unwrap_or(Value::Missing(ScalarType::Ratio))
        };
        match self.kind {
            QuoteStatisticKind::Accepted => integer(i64::try_from(statistics.accepted).ok()),
            QuoteStatisticKind::Rejected => integer(i64::try_from(statistics.rejected).ok()),
            QuoteStatisticKind::DuplicateProviderRows => {
                integer(i64::try_from(statistics.duplicate_provider_rows).ok())
            }
            QuoteStatisticKind::CoverageMillis => integer(Some(statistics.coverage_millis)),
            QuoteStatisticKind::SpreadLast => number(statistics.spread_last),
            QuoteStatisticKind::SpreadMean => number(statistics.spread_mean),
            QuoteStatisticKind::SpreadMax => number(statistics.spread_max),
            QuoteStatisticKind::SpreadP90 => number(statistics.spread_p90),
            QuoteStatisticKind::SpreadTimeMean => number(statistics.spread_time_mean),
            QuoteStatisticKind::SpreadBpsMean => ratio(statistics.spread_bps_mean),
            QuoteStatisticKind::QuoteActivity => {
                integer(i64::try_from(statistics.quote_activity).ok())
            }
            QuoteStatisticKind::QuoteRatePerSecond => ratio(statistics.quote_rate_per_second),
            QuoteStatisticKind::MidChangeCount => {
                integer(i64::try_from(statistics.mid_change_count).ok())
            }
            QuoteStatisticKind::InterarrivalMeanMillis => {
                number(statistics.interarrival_mean_millis)
            }
            QuoteStatisticKind::InterarrivalMaxMillis => {
                integer(statistics.interarrival_max_millis)
            }
            QuoteStatisticKind::InterarrivalCv => ratio(statistics.interarrival_cv),
            QuoteStatisticKind::DirectionChanges => {
                integer(i64::try_from(statistics.direction_changes).ok())
            }
            QuoteStatisticKind::LongestDirectionStreak => {
                integer(i64::try_from(statistics.longest_direction_streak).ok())
            }
            QuoteStatisticKind::PathLength => Value::Number(statistics.path_length),
            QuoteStatisticKind::PathEfficiency => ratio(statistics.path_efficiency),
            QuoteStatisticKind::MidChangeVariance => number(statistics.mid_change_variance),
            QuoteStatisticKind::FirstHighAt => statistics
                .first_high_at
                .map(Value::Timestamp)
                .unwrap_or(Value::Missing(ScalarType::Timestamp)),
            QuoteStatisticKind::LastHighAt => statistics
                .last_high_at
                .map(Value::Timestamp)
                .unwrap_or(Value::Missing(ScalarType::Timestamp)),
            QuoteStatisticKind::FirstLowAt => statistics
                .first_low_at
                .map(Value::Timestamp)
                .unwrap_or(Value::Missing(ScalarType::Timestamp)),
            QuoteStatisticKind::LastLowAt => statistics
                .last_low_at
                .map(Value::Timestamp)
                .unwrap_or(Value::Missing(ScalarType::Timestamp)),
            QuoteStatisticKind::Twap => statistics
                .twap
                .map(Value::Price)
                .unwrap_or(Value::Missing(ScalarType::Price)),
            QuoteStatisticKind::Crossings => integer(i64::try_from(statistics.crossings).ok()),
            QuoteStatisticKind::CumulativeAboveMillis => {
                integer(Some(statistics.cumulative_above_millis))
            }
            QuoteStatisticKind::ContinuousAboveMillis => {
                integer(Some(statistics.continuous_above_millis))
            }
            QuoteStatisticKind::ElapsedSinceBreakoutMillis => {
                integer(statistics.elapsed_since_breakout_millis)
            }
            QuoteStatisticKind::DominantBin => integer(
                statistics
                    .dominant_bin
                    .and_then(|value| i64::try_from(value).ok()),
            ),
            QuoteStatisticKind::DominantBinCenter => statistics
                .dominant_bin_center
                .map(Value::Price)
                .unwrap_or(Value::Missing(ScalarType::Price)),
            QuoteStatisticKind::DistanceFromDominantCenter => {
                number(statistics.distance_from_dominant_center)
            }
        }
    }
}

impl HistoricalNamedInputProjector for QuoteStatisticsProjector {
    fn output_type(&self) -> ValueType {
        ValueType::optional(self.kind.scalar_type())
    }

    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> std::result::Result<ProjectedNamedInput, NamedInputProjectionError> {
        let Some(bar) = context
            .closed_bars
            .iter()
            .find(|bar| bar.series_id() == &self.series_id)
        else {
            return Ok(ProjectedNamedInput {
                value: Value::Missing(self.kind.scalar_type()),
                updated: false,
            });
        };
        let mut state = self.state.borrow_mut();
        if state.last_open == Some(bar.open_time()) {
            return Ok(ProjectedNamedInput {
                value: state
                    .retained
                    .clone()
                    .unwrap_or(Value::Missing(self.kind.scalar_type())),
                updated: false,
            });
        }
        let start = self
            .ticks
            .partition_point(|tick| tick.tick.ts < bar.open_time());
        let end = self
            .ticks
            .partition_point(|tick| tick.tick.ts < bar.close_time());
        let rows = end.saturating_sub(start);
        if rows > self.maximum_rows_per_query {
            return Err(NamedInputProjectionError::new(
                "quote projector query exceeds its row limit",
            ));
        }
        let statistics = data_preprocess::aggregate_quote_statistics(
            &self.ticks[start..end],
            bar.open_time(),
            bar.close_time(),
            bar.close(),
            self.captured_level,
            self.bins,
            None,
        )
        .map_err(|error| NamedInputProjectionError::new(error.to_string()))?;
        let value = self.value(&statistics);
        state.last_open = Some(bar.open_time());
        state.retained = Some(value.clone());
        Ok(ProjectedNamedInput {
            value,
            updated: true,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketLoadLimits {
    pub max_rows: usize,
    pub max_resident_bytes: usize,
}
impl MarketLoadLimits {
    pub fn new(max_rows: usize, max_resident_bytes: usize) -> Result<Self> {
        if max_rows == 0 || max_resident_bytes == 0 {
            Err(MarketLoadError::InvalidSeries(
                "market load limits must be positive".into(),
            ))
        } else {
            Ok(Self {
                max_rows,
                max_resident_bytes,
            })
        }
    }
}

pub fn load_ordered_stored_ticks(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    canonical_symbol: &str,
    limits: MarketLoadLimits,
) -> Result<Arc<[FeedEvent]>> {
    load_ordered_stored_ticks_controlled(
        data_dir,
        exchange,
        symbol,
        canonical_symbol,
        limits,
        Arc::new(|| false),
    )
}

pub fn load_ordered_stored_ticks_controlled(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    canonical_symbol: &str,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<Arc<[FeedEvent]>> {
    let query_bytes = limits.max_resident_bytes / 2;
    if is_cancelled() {
        return Err(MarketLoadError::Cancelled);
    }
    if query_bytes == 0 {
        return Err(MarketLoadError::InvalidSeries(
            "enhanced tick load has no query budget".into(),
        ));
    }
    let rows = ParquetStore::open(data_dir)?.query_stored_ticks_bounded(
        exchange,
        symbol,
        limits.max_rows,
        query_bytes,
        || is_cancelled(),
    )?;
    check_materialized_bound(
        rows.len(),
        std::mem::size_of::<FeedEvent>() + canonical_symbol.len(),
        MarketLoadLimits {
            max_rows: limits.max_rows,
            max_resident_bytes: limits.max_resident_bytes - query_bytes,
        },
    )?;
    let events = rows
        .into_iter()
        .filter_map(|row| {
            tick_to_valid_event(row.tick, canonical_symbol).map(|event| {
                FeedEvent::new(
                    event,
                    qs_backtest::data_feed::EventMetadata::new(
                        SeriesRoles::PRIMARY,
                        0,
                        row.source_ordinal,
                    ),
                )
            })
        })
        .collect::<Vec<_>>();
    if is_cancelled() {
        return Err(MarketLoadError::Cancelled);
    }
    Ok(Arc::from(events))
}

pub fn load_price_only_bars(
    data_dir: &str,
    descriptor: &SeriesDescriptor,
    canonical_symbol: &str,
    limits: MarketLoadLimits,
) -> Result<Arc<[FeedEvent]>> {
    load_price_only_bars_controlled(
        data_dir,
        descriptor,
        canonical_symbol,
        limits,
        Arc::new(|| false),
    )
}

pub fn load_price_only_bars_controlled(
    data_dir: &str,
    descriptor: &SeriesDescriptor,
    canonical_symbol: &str,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<Arc<[FeedEvent]>> {
    let query_bytes = limits.max_resident_bytes / 2;
    if is_cancelled() {
        return Err(MarketLoadError::Cancelled);
    }
    if query_bytes == 0 {
        return Err(MarketLoadError::InvalidSeries(
            "price-bar load has no query budget".into(),
        ));
    }
    let rows = ParquetStore::open(data_dir)?.query_price_bars_bounded(
        descriptor,
        limits.max_rows,
        query_bytes,
        || is_cancelled(),
    )?;
    check_materialized_bound(
        rows.len(),
        std::mem::size_of::<FeedEvent>() + canonical_symbol.len(),
        MarketLoadLimits {
            max_rows: limits.max_rows,
            max_resident_bytes: limits.max_resident_bytes - query_bytes,
        },
    )?;
    let mut events = Vec::with_capacity(rows.len());
    for (ordinal, row) in rows.into_iter().enumerate() {
        let available_at = row.available_at;
        let metadata = qs_backtest::data_feed::EventMetadata::new(
            SeriesRoles::PRIMARY,
            0,
            u64::try_from(ordinal).map_err(|_| {
                MarketLoadError::InvalidSeries("price-bar ordinal overflowed".into())
            })?,
        )
        .with_available_at(available_at);
        events.push(FeedEvent::new(
            price_bar_to_event(row, canonical_symbol, descriptor.point_size),
            metadata,
        ));
    }
    if is_cancelled() {
        return Err(MarketLoadError::Cancelled);
    }
    Ok(Arc::from(events))
}

pub fn open_ordered_stored_tick_stream(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    canonical_symbol: &str,
    bounds: ParquetScanBounds,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<MarketStream> {
    let rows_per_read = streaming_rows_per_read::<StoredTick>(limits)?;
    let store = ParquetStore::open(data_dir)?;
    let mut cursor =
        store.scan_stored_ticks_cancellable(exchange, symbol, bounds, rows_per_read, {
            let is_cancelled = is_cancelled.clone();
            move || is_cancelled()
        })?;
    let canonical_symbol = canonical_symbol.to_owned();
    let mut observed = 0usize;
    let source: EventSource = Box::new(move || {
        loop {
            ensure_not_cancelled(&is_cancelled)?;
            let Some(scanned) = cursor
                .next_stored_tick_with_ordinal_cancellable({
                    let is_cancelled = is_cancelled.clone();
                    move || is_cancelled()
                })
                .map_err(map_data_error)?
            else {
                return Ok(None);
            };
            observed = observed.checked_add(1).ok_or_else(|| {
                MarketLoadError::InvalidSeries("stream row count overflowed".into())
            })?;
            if observed > limits.max_rows {
                return Err(MarketLoadError::InvalidSeries(
                    "ordered-tick stream exceeds its row limit".into(),
                ));
            }
            if let Some(event) = tick_to_valid_event(scanned.row.tick, &canonical_symbol) {
                return Ok(Some(SequencedMarketEvent::new(
                    event,
                    scanned.row.source_ordinal,
                )));
            }
        }
    });
    Ok(KWayMergeFeed::new(vec![EventBatchFeed::new(
        source,
        SeriesRoles::PRIMARY,
        0,
    )]))
}

pub fn open_price_bar_stream(
    data_dir: &str,
    descriptor: &SeriesDescriptor,
    canonical_symbol: &str,
    bounds: ParquetScanBounds,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<MarketStream> {
    let rows_per_read = streaming_rows_per_read::<PriceBar>(limits)?;
    let store = ParquetStore::open(data_dir)?;
    let mut cursor = store.scan_price_bars_cancellable(descriptor, bounds, rows_per_read, {
        let is_cancelled = is_cancelled.clone();
        move || is_cancelled()
    })?;
    let canonical_symbol = canonical_symbol.to_owned();
    let point_size = descriptor.point_size;
    let mut observed = 0usize;
    let source: EventSource = Box::new(move || {
        ensure_not_cancelled(&is_cancelled)?;
        let Some(scanned) = cursor
            .next_price_bar_with_ordinal_cancellable({
                let is_cancelled = is_cancelled.clone();
                move || is_cancelled()
            })
            .map_err(map_data_error)?
        else {
            return Ok(None);
        };
        observed = observed
            .checked_add(1)
            .ok_or_else(|| MarketLoadError::InvalidSeries("stream row count overflowed".into()))?;
        if observed > limits.max_rows {
            return Err(MarketLoadError::InvalidSeries(
                "price-bar stream exceeds its row limit".into(),
            ));
        }
        let available_at = scanned.row.available_at;
        Ok(Some(
            SequencedMarketEvent::new(
                price_bar_to_event(scanned.row, &canonical_symbol, point_size),
                scanned.source_row_ordinal,
            )
            .with_available_at(available_at),
        ))
    });
    Ok(KWayMergeFeed::new(vec![EventBatchFeed::new(
        source,
        SeriesRoles::PRIMARY,
        0,
    )]))
}

fn streaming_rows_per_read<T>(limits: MarketLoadLimits) -> Result<usize> {
    let row_bytes = std::mem::size_of::<T>()
        .checked_add(std::mem::size_of::<FeedEvent>())
        .ok_or_else(|| MarketLoadError::InvalidSeries("stream row byte count overflowed".into()))?;
    let byte_rows = limits.max_resident_bytes / row_bytes;
    let rows = limits.max_rows.min(byte_rows);
    if rows == 0 {
        Err(MarketLoadError::InvalidSeries(
            "stream limits cannot hold one decoded row".into(),
        ))
    } else {
        Ok(rows)
    }
}

fn check_materialized_bound(rows: usize, row_bytes: usize, limits: MarketLoadLimits) -> Result<()> {
    if rows > limits.max_rows
        || rows
            .checked_mul(row_bytes)
            .is_none_or(|bytes| bytes > limits.max_resident_bytes)
    {
        Err(MarketLoadError::InvalidSeries(
            "materialized market input exceeds its declared row or resident-byte bound".into(),
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrencyStreamPlan {
    pub pnl_currency_by_primary_symbol: BTreeMap<String, String>,
    pub routes: BTreeMap<String, ConversionRoute>,
    pub conversion_symbols: std::collections::BTreeSet<String>,
}

pub fn plan_currency_streams(
    registry: &SymbolRegistry,
    account_currency: &str,
    exchange: &str,
    primary_symbols: &std::collections::BTreeSet<String>,
    available_symbols: &std::collections::BTreeSet<String>,
) -> Result<CurrencyStreamPlan> {
    let pnl_currency_by_primary_symbol = primary_symbols
        .iter()
        .map(|symbol| {
            let metadata = registry.currency_metadata(symbol).ok_or_else(|| {
                MarketLoadError::InvalidSeries(format!(
                    "primary symbol '{symbol}' has no currency metadata"
                ))
            })?;
            if metadata.pnl_currency.is_empty() {
                return Err(MarketLoadError::InvalidSeries(format!(
                    "primary symbol '{symbol}' has no P&L currency"
                )));
            }
            Ok((symbol.clone(), metadata.pnl_currency.clone()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    plan_currency_routes(
        registry,
        account_currency,
        exchange,
        pnl_currency_by_primary_symbol,
        available_symbols,
    )
}

pub fn plan_currency_routes(
    registry: &SymbolRegistry,
    account_currency: &str,
    exchange: &str,
    pnl_currency_by_primary_symbol: BTreeMap<String, String>,
    available_symbols: &std::collections::BTreeSet<String>,
) -> Result<CurrencyStreamPlan> {
    let routes = pnl_currency_by_primary_symbol.values().cloned().collect::<std::collections::BTreeSet<_>>().into_iter().map(|source_currency| {
        let route = resolve_conversion_route(registry, &source_currency, account_currency, available_symbols)
            .map_err(|error| MarketLoadError::InvalidSeries(format!("cannot resolve {source_currency} to {account_currency} on exchange '{exchange}': {error}")))?;
        Ok((source_currency, route))
    }).collect::<Result<BTreeMap<_, _>>>()?;
    let conversion_symbols = routes
        .values()
        .flat_map(ConversionRoute::symbols)
        .map(ToOwned::to_owned)
        .collect();
    Ok(CurrencyStreamPlan {
        pnl_currency_by_primary_symbol,
        routes,
        conversion_symbols,
    })
}

#[derive(Debug, Clone)]
enum MarketSeriesSource {
    Tick { scan: ParquetTickScan },
    Bar { scan: ParquetBarScan },
    Unavailable { message: String },
    Empty,
}

#[derive(Debug, Clone)]
pub struct MarketSeriesDescription {
    canonical_symbol: String,
    roles: SeriesRoles,
    source_partition: Option<String>,
    source_symbol: Option<String>,
    source: MarketSeriesSource,
    /// One price point for this symbol, used to turn a stored bar spread in points into price units.
    bar_point_size: Option<f64>,
}

impl MarketSeriesDescription {
    fn tick(
        scan: ParquetTickScan,
        canonical_symbol: String,
        roles: SeriesRoles,
        source_partition: String,
        source_symbol: String,
    ) -> Self {
        Self {
            canonical_symbol,
            roles,
            source_partition: Some(source_partition),
            source_symbol: Some(source_symbol),
            source: MarketSeriesSource::Tick { scan },
            bar_point_size: None,
        }
    }

    fn bar(
        scan: ParquetBarScan,
        canonical_symbol: String,
        roles: SeriesRoles,
        source_partition: String,
        source_symbol: String,
    ) -> Self {
        Self {
            canonical_symbol,
            roles,
            source_partition: Some(source_partition),
            source_symbol: Some(source_symbol),
            source: MarketSeriesSource::Bar { scan },
            bar_point_size: None,
        }
    }

    pub fn conversion_tick(
        data_dir: &str,
        exchange: String,
        symbol: String,
        canonical_symbol: String,
        bounds: ParquetScanBounds,
    ) -> Self {
        match ParquetTickScan::describe(data_dir, &exchange, &symbol, bounds) {
            Ok(scan) => Self::tick(
                scan,
                canonical_symbol,
                SeriesRoles::CONVERSION,
                exchange.clone(),
                symbol.clone(),
            ),
            Err(error) => Self {
                canonical_symbol,
                roles: SeriesRoles::CONVERSION,
                source_partition: Some(exchange),
                source_symbol: Some(symbol),
                source: MarketSeriesSource::Unavailable {
                    message: error.to_string(),
                },
                bar_point_size: None,
            },
        }
    }

    pub fn empty_conversion(_data_dir: &str, canonical_symbol: String) -> Self {
        Self::empty(canonical_symbol, SeriesRoles::CONVERSION)
    }

    fn empty_primary(_data_dir: &str, canonical_symbol: String) -> Self {
        Self::empty(canonical_symbol, SeriesRoles::PRIMARY)
    }

    fn empty(canonical_symbol: String, roles: SeriesRoles) -> Self {
        Self {
            canonical_symbol,
            roles,
            source_partition: None,
            source_symbol: None,
            source: MarketSeriesSource::Empty,
            bar_point_size: None,
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
                let bar_point_size = self.bar_point_size;
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
                                    bar_to_event(scanned.row, &canonical_symbol, bar_point_size),
                                    scanned.source_row_ordinal,
                                )
                            })
                        })
                })
            }
            MarketSeriesSource::Unavailable { message } => {
                return Err(MarketLoadError::Data(DataError::Other(message.clone())));
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
pub struct MarketStreamDescription {
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

    pub fn primary_start(&self) -> Option<NaiveDateTime> {
        self.primary_start
    }

    pub fn primary_eod(&self) -> Option<NaiveDateTime> {
        self.primary_eod
    }

    pub fn conversion_end(&self) -> Option<NaiveDateTime> {
        self.primary_eod.or(self.requested_to)
    }

    pub fn primary_series_count(&self) -> usize {
        self.series.len()
    }

    pub fn stored_series_coordinates(&self) -> Vec<(String, String, String)> {
        self.series
            .iter()
            .filter_map(|series| {
                Some((
                    series.canonical_symbol.clone(),
                    series.source_partition.clone()?,
                    series.source_symbol.clone()?,
                ))
            })
            .collect()
    }

    pub fn validate_stored_series_bindings(
        &self,
        manifest: &ReplayInstrumentManifest,
    ) -> Result<()> {
        let mut expected = self.stored_series_coordinates();
        expected.sort();
        expected.dedup();
        let mut actual = manifest
            .stored_series
            .iter()
            .map(|binding| {
                let symbol = manifest
                    .instruments
                    .iter()
                    .find(|(_, artifact)| artifact.resolved == binding.instrument)
                    .map(|(symbol, _)| symbol.clone())
                    .ok_or_else(|| {
                        MarketLoadError::InvalidSeries(format!(
                            "stored series {}:{} references an unresolved instrument",
                            binding.source_partition, binding.source_symbol
                        ))
                    })?;
                Ok((
                    symbol,
                    binding.source_partition.clone(),
                    binding.source_symbol.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        actual.sort();
        actual.dedup();
        if actual != expected {
            return Err(MarketLoadError::InvalidSeries(format!(
                "stored-series bindings do not match the planned market streams: expected {expected:?}, got {actual:?}"
            )));
        }
        Ok(())
    }

    pub fn mark_shared_conversion_symbols(
        &mut self,
        shared_symbols: &std::collections::BTreeSet<String>,
    ) {
        for series in &mut self.series {
            if shared_symbols.contains(&series.canonical_symbol) {
                series.roles = SeriesRoles::PRIMARY_AND_CONVERSION;
            }
        }
    }

    /// Attach price point sizes so stored bar spreads become executable quote spreads.
    ///
    /// Symbols without an entry keep the historical zero-spread bar approximation.
    pub fn apply_bar_point_sizes(&mut self, point_sizes: &BTreeMap<String, f64>) {
        for series in &mut self.series {
            if matches!(series.source, MarketSeriesSource::Bar { .. }) {
                series.bar_point_size = point_sizes.get(&series.canonical_symbol).copied();
            }
        }
    }

    pub fn push_series(&mut self, series: MarketSeriesDescription) {
        self.series.push(series);
    }

    pub fn open(&self, is_cancelled: CancellationCheck) -> Result<MarketStream> {
        let feeds = self
            .series
            .iter()
            .enumerate()
            .map(|(rank, series)| {
                let rank = u32::try_from(rank).map_err(|_| {
                    MarketLoadError::InvalidSeries(
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
pub fn describe_primary_market_stream(
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
                MarketSeriesDescription::tick(
                    scan,
                    canonical_symbol.clone(),
                    SeriesRoles::PRIMARY,
                    disk_exchange,
                    disk_symbol,
                ),
                first,
                last,
            )
        } else if data_type == "bar" {
            let timeframe = timeframe.ok_or_else(|| {
                MarketLoadError::InvalidSeries("timeframe is required for bar data".into())
            })?;
            let parsed = Timeframe::parse(timeframe).map_err(|_| {
                MarketLoadError::InvalidSeries(format!("Invalid timeframe: '{timeframe}'"))
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
                MarketSeriesDescription::bar(
                    scan,
                    canonical_symbol.clone(),
                    SeriesRoles::PRIMARY,
                    disk_exchange,
                    disk_symbol,
                ),
                first,
                last,
            )
        } else {
            return Err(MarketLoadError::InvalidSeries(format!(
                "Invalid data_type: '{data_type}'. Must be 'tick' or 'bar'."
            )));
        };

        let Some(first) = first else {
            return Err(MarketLoadError::NoDataFound {
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
        let event = bar_to_event(bar, canonical_symbol, None);
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

/// Convert one stored bar into a feed event, expressing its recorded spread in price units.
///
/// The stored spread is a point count, so it becomes a price only when the caller knows the symbol's price point size. Without one the bar keeps the historical zero-spread approximation.
fn price_bar_to_event(bar: PriceBar, canonical_symbol: &str, point_size: f64) -> MarketEvent {
    MarketEvent::Bar {
        symbol: canonical_symbol.to_owned(),
        ts: bar.ts,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: 0,
        spread: bar
            .spread
            .map(|spread| f64::from(spread.max(0)) * point_size)
            .filter(|spread| *spread > 0.0),
        timeframe_seconds: bar
            .timeframe
            .fixed_duration_seconds()
            .and_then(|value| u64::try_from(value).ok()),
        tick_count: bar.tick_count,
    }
}

fn bar_to_event(bar: Bar, canonical_symbol: &str, point_size: Option<f64>) -> MarketEvent {
    let spread = point_size
        .filter(|size| size.is_finite() && *size > 0.0)
        .map(|size| f64::from(bar.spread.max(0)) * size)
        .filter(|spread| *spread > 0.0);
    MarketEvent::Bar {
        symbol: canonical_symbol.to_owned(),
        ts: bar.ts,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        spread,
        timeframe_seconds: bar
            .timeframe
            .fixed_duration_seconds()
            .and_then(|seconds| u64::try_from(seconds).ok()),
        tick_count: u64::try_from(bar.tick_vol).ok().filter(|count| *count > 0),
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
        Err(MarketLoadError::Cancelled)
    } else {
        Ok(())
    }
}

fn ensure_not_cancelled_mut(is_cancelled: &mut dyn FnMut() -> bool) -> Result<()> {
    if is_cancelled() {
        Err(MarketLoadError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_data_error(error: DataError) -> MarketLoadError {
    match error {
        DataError::Cancelled => MarketLoadError::Cancelled,
        other => MarketLoadError::Data(other),
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
    fn stored_bars_carry_their_timeframe_and_tick_count_into_feed_events() {
        let bar = Bar {
            exchange: "Fixture".into(),
            symbol: "eurusd".into(),
            timeframe: data_preprocess::models::Timeframe::M5,
            ts: ts(0),
            open: 1.1,
            high: 1.3,
            low: 1.0,
            close: 1.2,
            tick_vol: 42,
            volume: 0,
            spread: 3,
        };
        let MarketEvent::Bar {
            timeframe_seconds,
            tick_count,
            spread,
            ..
        } = bar_to_event(bar.clone(), "EURUSD", Some(1.0e-5))
        else {
            panic!("a stored bar becomes a bar event");
        };
        assert_eq!(timeframe_seconds, Some(300));
        assert_eq!(tick_count, Some(42));
        assert!(spread.is_some_and(|value| (value - 3.0e-5).abs() < 1e-12));

        let without_ticks = Bar { tick_vol: 0, ..bar };
        let MarketEvent::Bar { tick_count, .. } = bar_to_event(without_ticks, "EURUSD", None)
        else {
            panic!("a stored bar becomes a bar event");
        };
        assert_eq!(
            tick_count, None,
            "a zero tick count is unknown, not an empty bar"
        );
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
            Err(MarketLoadError::Data(
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
        assert!(matches!(error, MarketLoadError::Cancelled));
    }
}
