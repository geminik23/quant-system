use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_backtest::data_feed::FallibleBatchFeed;
use qs_market_loader::{
    CancellationCheck, MarketLoadLimits, SeriesDescriptor, SymbolPartitionResolver,
    describe_primary_market_stream, describe_primary_market_stream_with_resolver,
    load_ordered_stored_ticks, load_ordered_stored_ticks_controlled, load_price_only_bars,
    load_price_only_bars_controlled,
};

use crate::error::ResearchError;
use crate::runner::SymbolEvents;

pub fn load_symbol_ordered_ticks(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    limits: MarketLoadLimits,
) -> Result<SymbolEvents, ResearchError> {
    load_ordered_stored_ticks(data_dir, exchange, symbol, symbol, limits)
        .map_err(ResearchError::from)
}

pub fn load_symbol_ordered_ticks_controlled(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<SymbolEvents, ResearchError> {
    load_ordered_stored_ticks_controlled(data_dir, exchange, symbol, symbol, limits, is_cancelled)
        .map_err(ResearchError::from)
}

pub fn load_symbol_price_bars(
    data_dir: &str,
    descriptor: &SeriesDescriptor,
    limits: MarketLoadLimits,
) -> Result<SymbolEvents, ResearchError> {
    load_price_only_bars(data_dir, descriptor, &descriptor.symbol, limits)
        .map_err(ResearchError::from)
}

pub fn load_symbol_price_bars_controlled(
    data_dir: &str,
    descriptor: &SeriesDescriptor,
    limits: MarketLoadLimits,
    is_cancelled: CancellationCheck,
) -> Result<SymbolEvents, ResearchError> {
    load_price_only_bars_controlled(
        data_dir,
        descriptor,
        &descriptor.symbol,
        limits,
        is_cancelled,
    )
    .map_err(ResearchError::from)
}

/// Read one symbol's stored ticks into memory once, for a batch to share across every run.
///
/// The whole range is materialized rather than streamed because a parameter search reads it many times and slices it per window; a cursor that could only be walked once would have to be reopened for every configuration.
pub fn load_symbol_ticks(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    load_symbol_events(data_dir, exchange, symbol, "tick", None, from, to)
}

/// Read one symbol's stored bars of one timeframe into memory once, for a batch to share across every run.
///
/// Each bar keeps its storage timestamp, the open of its bucket, and carries its timeframe and tick count, so a strategy series accepts it only for a source declared with the same timeframe and shows it only after the bucket closes. A batch over these events records `bars` as its data mode.
pub fn load_symbol_bars(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    timeframe: &str,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    load_symbol_events(data_dir, exchange, symbol, "bar", Some(timeframe), from, to)
}

/// Load ticks using caller-owned instrument aliases and source bindings.
pub fn load_symbol_ticks_with_resolver(
    resolver: &SymbolPartitionResolver<'_>,
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    load_symbol_events_impl(
        data_dir,
        exchange,
        symbol,
        "tick",
        None,
        from,
        to,
        Some(resolver),
    )
}

/// Load stored bars using caller-owned instrument aliases and source bindings.
pub fn load_symbol_bars_with_resolver(
    resolver: &SymbolPartitionResolver<'_>,
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    timeframe: &str,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    load_symbol_events_impl(
        data_dir,
        exchange,
        symbol,
        "bar",
        Some(timeframe),
        from,
        to,
        Some(resolver),
    )
}

fn load_symbol_events(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    load_symbol_events_impl(
        data_dir, exchange, symbol, data_type, timeframe, from, to, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn load_symbol_events_impl(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    resolver: Option<&SymbolPartitionResolver<'_>>,
) -> Result<SymbolEvents, ResearchError> {
    let mut never_cancelled = || false;
    let symbols = [symbol.to_owned()];
    let description = match resolver {
        Some(resolver) => describe_primary_market_stream_with_resolver(
            resolver,
            data_dir,
            exchange,
            &symbols,
            data_type,
            timeframe,
            from,
            to,
            &mut never_cancelled,
            &mut |_| {},
        )?,
        None => describe_primary_market_stream(
            data_dir,
            exchange,
            &symbols,
            data_type,
            timeframe,
            from,
            to,
            &mut never_cancelled,
            &mut |_| {},
        )?,
    };

    let mut stream = description.open(Arc::new(|| false))?;
    let mut events = Vec::new();
    while let Some(batch) = stream
        .next_batch()
        .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?
    {
        events.extend(batch.events);
    }
    Ok(events.into())
}
