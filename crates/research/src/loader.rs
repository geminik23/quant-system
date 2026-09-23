use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_backtest::data_feed::FallibleBatchFeed;
use qs_market_loader::describe_primary_market_stream;

use crate::error::ResearchError;
use crate::runner::SymbolEvents;

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

fn load_symbol_events(
    data_dir: &str,
    exchange: &str,
    symbol: &str,
    data_type: &str,
    timeframe: Option<&str>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<SymbolEvents, ResearchError> {
    let mut never_cancelled = || false;
    let symbols = [symbol.to_owned()];
    let description = describe_primary_market_stream(
        data_dir,
        exchange,
        &symbols,
        data_type,
        timeframe,
        from,
        to,
        &mut never_cancelled,
        &mut |_| {},
    )?;

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
