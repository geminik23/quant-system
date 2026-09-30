//! Bidirectional conversion between Polars DataFrames and domain types (Tick, Bar).

use chrono::NaiveDateTime;
use polars::prelude::*;

use crate::error::Result;
use crate::models::{Bar, PriceBar, StoredTick, Tick, Timeframe};

/// Convert a slice of Ticks into a Polars DataFrame.
pub fn ticks_to_dataframe(ticks: &[Tick]) -> Result<DataFrame> {
    let exchanges: Vec<&str> = ticks.iter().map(|t| t.exchange.as_str()).collect();
    let symbols: Vec<&str> = ticks.iter().map(|t| t.symbol.as_str()).collect();
    let timestamps: Vec<i64> = ticks
        .iter()
        .map(|t| t.ts.and_utc().timestamp_micros())
        .collect();
    let bids: Vec<Option<f64>> = ticks.iter().map(|t| t.bid).collect();
    let asks: Vec<Option<f64>> = ticks.iter().map(|t| t.ask).collect();
    let lasts: Vec<Option<f64>> = ticks.iter().map(|t| t.last).collect();
    let volumes: Vec<Option<f64>> = ticks.iter().map(|t| t.volume).collect();
    let flags: Vec<Option<i32>> = ticks.iter().map(|t| t.flags).collect();

    let df = DataFrame::new(vec![
        Column::new("exchange".into(), &exchanges),
        Column::new("symbol".into(), &symbols),
        Column::new("ts".into(), &timestamps)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .map_err(crate::error::DataError::Polars)?,
        Column::new("bid".into(), &bids),
        Column::new("ask".into(), &asks),
        Column::new("last".into(), &lasts),
        Column::new("volume".into(), &volumes),
        Column::new("flags".into(), &flags),
    ])?;

    Ok(df)
}

/// Convert a Polars DataFrame back into `Vec<Tick>`.
pub fn dataframe_to_ticks(df: &DataFrame) -> Result<Vec<Tick>> {
    let exchanges = df.column("exchange")?.str()?;
    let symbols = df.column("symbol")?.str()?;
    let ts_col = df.column("ts")?.datetime()?;
    let bids = df.column("bid")?.f64()?;
    let asks = df.column("ask")?.f64()?;
    let lasts = df.column("last")?.f64()?;
    let volumes = df.column("volume")?.f64()?;
    let flags = df.column("flags")?.i32()?;

    let mut ticks = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let ts_micros = ts_col.get(i).ok_or_else(|| {
            crate::error::DataError::InvalidTimestamp("null timestamp in dataframe".into())
        })?;
        let ts = micros_to_ndt(ts_micros);

        ticks.push(Tick {
            exchange: exchanges.get(i).unwrap_or("").to_string(),
            symbol: symbols.get(i).unwrap_or("").to_string(),
            ts,
            bid: bids.get(i),
            ask: asks.get(i),
            last: lasts.get(i),
            volume: volumes.get(i),
            flags: flags.get(i),
        });
    }
    Ok(ticks)
}

pub fn stored_ticks_to_dataframe(ticks: &[StoredTick]) -> Result<DataFrame> {
    for tick in ticks {
        tick.validate()?;
    }
    let plain = ticks
        .iter()
        .map(|value| value.tick.clone())
        .collect::<Vec<_>>();
    let mut frame = ticks_to_dataframe(&plain)?;
    frame.with_column(Column::new(
        "source_ordinal".into(),
        ticks
            .iter()
            .map(|value| value.source_ordinal)
            .collect::<Vec<_>>(),
    ))?;
    frame.with_column(Column::new(
        "source_identity".into(),
        ticks
            .iter()
            .map(|value| value.source_identity.as_deref())
            .collect::<Vec<_>>(),
    ))?;
    frame.with_column(Column::new(
        "provider_sequence".into(),
        ticks
            .iter()
            .map(|value| value.provider_sequence)
            .collect::<Vec<_>>(),
    ))?;
    Ok(frame)
}

pub fn dataframe_to_stored_ticks(df: &DataFrame) -> Result<Vec<StoredTick>> {
    let ticks = dataframe_to_ticks(df)?;
    let ordinals = df.column("source_ordinal")?.u64()?;
    let identities = df.column("source_identity")?.str()?;
    let sequences = df.column("provider_sequence")?.u64()?;
    ticks
        .into_iter()
        .enumerate()
        .map(|(index, tick)| {
            let value = StoredTick {
                tick,
                source_ordinal: ordinals
                    .get(index)
                    .ok_or_else(|| crate::DataError::Other("stored tick ordinal is null".into()))?,
                source_identity: identities.get(index).map(str::to_owned),
                provider_sequence: sequences.get(index),
            };
            value.validate()?;
            Ok(value)
        })
        .collect()
}

/// Convert a slice of Bars into a Polars DataFrame.
pub fn bars_to_dataframe(bars: &[Bar]) -> Result<DataFrame> {
    let exchanges: Vec<&str> = bars.iter().map(|b| b.exchange.as_str()).collect();
    let symbols: Vec<&str> = bars.iter().map(|b| b.symbol.as_str()).collect();
    let timeframes: Vec<&str> = bars.iter().map(|b| b.timeframe.as_str()).collect();
    let timestamps: Vec<i64> = bars
        .iter()
        .map(|b| b.ts.and_utc().timestamp_micros())
        .collect();
    let opens: Vec<f64> = bars.iter().map(|b| b.open).collect();
    let highs: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let lows: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let closes: Vec<f64> = bars.iter().map(|b| b.close).collect();
    let tick_vols: Vec<i64> = bars.iter().map(|b| b.tick_vol).collect();
    let volumes: Vec<i64> = bars.iter().map(|b| b.volume).collect();
    let spreads: Vec<i32> = bars.iter().map(|b| b.spread).collect();

    let df = DataFrame::new(vec![
        Column::new("exchange".into(), &exchanges),
        Column::new("symbol".into(), &symbols),
        Column::new("timeframe".into(), &timeframes),
        Column::new("ts".into(), &timestamps)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .map_err(crate::error::DataError::Polars)?,
        Column::new("open".into(), &opens),
        Column::new("high".into(), &highs),
        Column::new("low".into(), &lows),
        Column::new("close".into(), &closes),
        Column::new("tick_vol".into(), &tick_vols),
        Column::new("volume".into(), &volumes),
        Column::new("spread".into(), &spreads),
    ])?;

    Ok(df)
}

/// Convert a Polars DataFrame back into `Vec<Bar>`.
pub fn dataframe_to_bars(df: &DataFrame) -> Result<Vec<Bar>> {
    let exchanges = df.column("exchange")?.str()?;
    let symbols = df.column("symbol")?.str()?;
    let timeframes = df.column("timeframe")?.str()?;
    let ts_col = df.column("ts")?.datetime()?;
    let opens = df.column("open")?.f64()?;
    let highs = df.column("high")?.f64()?;
    let lows = df.column("low")?.f64()?;
    let closes = df.column("close")?.f64()?;
    let tick_vols = df.column("tick_vol")?.i64()?;
    let volumes = df.column("volume")?.i64()?;
    let spreads = df.column("spread")?.i32()?;

    let mut bars = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        let ts_micros = ts_col.get(i).ok_or_else(|| {
            crate::error::DataError::InvalidTimestamp("null timestamp in dataframe".into())
        })?;
        let ts = micros_to_ndt(ts_micros);
        let tf_str = timeframes.get(i).unwrap_or("1m");
        let timeframe = Timeframe::parse(tf_str).unwrap_or(Timeframe::M1);

        bars.push(Bar {
            exchange: exchanges.get(i).unwrap_or("").to_string(),
            symbol: symbols.get(i).unwrap_or("").to_string(),
            timeframe,
            ts,
            open: opens.get(i).unwrap_or(0.0),
            high: highs.get(i).unwrap_or(0.0),
            low: lows.get(i).unwrap_or(0.0),
            close: closes.get(i).unwrap_or(0.0),
            tick_vol: tick_vols.get(i).unwrap_or(0),
            volume: volumes.get(i).unwrap_or(0),
            spread: spreads.get(i).unwrap_or(0),
        });
    }
    Ok(bars)
}

pub fn price_bars_to_dataframe(bars: &[PriceBar]) -> Result<DataFrame> {
    for bar in bars {
        bar.validate()?;
    }
    let timestamps = bars
        .iter()
        .map(|bar| bar.ts.and_utc().timestamp_micros())
        .collect::<Vec<_>>();
    let available = bars
        .iter()
        .map(|bar| bar.available_at.and_utc().timestamp_micros())
        .collect::<Vec<_>>();
    Ok(DataFrame::new(vec![
        Column::new(
            "exchange".into(),
            bars.iter()
                .map(|bar| bar.exchange.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "symbol".into(),
            bars.iter()
                .map(|bar| bar.symbol.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new(
            "timeframe".into(),
            bars.iter()
                .map(|bar| bar.timeframe.as_str())
                .collect::<Vec<_>>(),
        ),
        Column::new("ts".into(), &timestamps)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
        Column::new("available_at".into(), &available)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
        Column::new(
            "open".into(),
            bars.iter().map(|bar| bar.open).collect::<Vec<_>>(),
        ),
        Column::new(
            "high".into(),
            bars.iter().map(|bar| bar.high).collect::<Vec<_>>(),
        ),
        Column::new(
            "low".into(),
            bars.iter().map(|bar| bar.low).collect::<Vec<_>>(),
        ),
        Column::new(
            "close".into(),
            bars.iter().map(|bar| bar.close).collect::<Vec<_>>(),
        ),
        Column::new(
            "tick_count".into(),
            bars.iter().map(|bar| bar.tick_count).collect::<Vec<_>>(),
        ),
        Column::new(
            "spread".into(),
            bars.iter().map(|bar| bar.spread).collect::<Vec<_>>(),
        ),
    ])?)
}

pub fn dataframe_to_price_bars(df: &DataFrame) -> Result<Vec<PriceBar>> {
    if df.column("tick_vol").is_ok() {
        return Err(crate::DataError::Other(
            "price-only frame conflicts with legacy tick_vol".into(),
        ));
    }
    let exchanges = df.column("exchange")?.str()?;
    let symbols = df.column("symbol")?.str()?;
    let timeframes = df.column("timeframe")?.str()?;
    let timestamps = df.column("ts")?.datetime()?;
    let available = df.column("available_at")?.datetime()?;
    let opens = df.column("open")?.f64()?;
    let highs = df.column("high")?.f64()?;
    let lows = df.column("low")?.f64()?;
    let closes = df.column("close")?.f64()?;
    let counts = df.column("tick_count")?.u64()?;
    let spreads = df.column("spread")?.i32()?;
    (0..df.height())
        .map(|index| {
            let value = PriceBar {
                exchange: exchanges.get(index).unwrap_or("").into(),
                symbol: symbols.get(index).unwrap_or("").into(),
                timeframe: Timeframe::parse(timeframes.get(index).unwrap_or("1m"))?,
                ts: micros_to_ndt(timestamps.get(index).ok_or_else(|| {
                    crate::DataError::InvalidTimestamp("null price bar timestamp".into())
                })?),
                available_at: micros_to_ndt(available.get(index).ok_or_else(|| {
                    crate::DataError::InvalidTimestamp("null availability timestamp".into())
                })?),
                open: opens.get(index).unwrap_or(0.0),
                high: highs.get(index).unwrap_or(0.0),
                low: lows.get(index).unwrap_or(0.0),
                close: closes.get(index).unwrap_or(0.0),
                tick_count: counts.get(index),
                spread: spreads.get(index),
            };
            value.validate()?;
            Ok(value)
        })
        .collect()
}

/// Convert microsecond epoch to NaiveDateTime.
fn micros_to_ndt(micros: i64) -> NaiveDateTime {
    let secs = micros / 1_000_000;
    let nsecs = ((micros % 1_000_000) * 1_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs)
        .map(|dt| dt.naive_utc())
        .unwrap_or_default()
}

/// Extract the date portion from a NaiveDateTime as a formatted string.
pub fn ndt_to_date_string(ndt: &NaiveDateTime) -> String {
    ndt.format("%Y-%m-%d").to_string()
}
