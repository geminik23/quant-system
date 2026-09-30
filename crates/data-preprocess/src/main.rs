use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};

use data_preprocess::display::{
    print_bars, print_delete_result, print_import_result, print_stats, print_ticks,
};
use data_preprocess::models::{BarQueryOpts, ImportResult, QueryOpts, Timeframe};
use data_preprocess::parser::bar_csv::parse_bar_csv;
use data_preprocess::parser::tick_csv::parse_tick_csv;
use data_preprocess::parser::{
    extract_symbol_from_filename, normalize_exchange, parse_datetime_arg, parse_tz_offset,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Backend {
    Parquet,
    Duckdb,
}

#[derive(Parser)]
#[command(name = "data-preprocess", about = "Historical market data CLI")]
struct Cli {
    /// Storage backend to use
    #[arg(long, default_value = "parquet", value_enum)]
    backend: Backend,

    /// Root directory for Parquet files (parquet backend)
    #[arg(long, default_value = "market_data", env = "DATA_PREPROCESS_DIR")]
    data_dir: PathBuf,

    /// Path to DuckDB database file (duckdb backend)
    #[arg(long, default_value = "market_data.duckdb", env = "DATA_PREPROCESS_DB")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build stored bars from stored ticks using the replay engine's bucket rules
    Resample {
        /// Exchange / broker name
        #[arg(long, short)]
        exchange: String,
        /// Symbol to resample
        #[arg(long, short)]
        symbol: String,
        /// Target bar timeframe (1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, 1w)
        #[arg(long, short)]
        timeframe: String,
        /// Quote side used to derive each bar price. Replay reconstructs a two-sided quote around the stored close, so `mid` is the basis that executes without a half-spread offset
        #[arg(long, default_value = "mid")]
        price_basis: String,
        /// Bucket alignment offset in seconds, for example 79200 to close daily bars at 22:00
        #[arg(long, default_value_t = 0)]
        align_offset_seconds: i64,
        /// Price decimal digits, used to express the average spread in points
        #[arg(long, default_value_t = 5)]
        digits: u32,
        /// Inclusive start timestamp (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
        #[arg(long)]
        from: Option<String>,
        /// Inclusive end timestamp (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
        #[arg(long)]
        to: Option<String>,
        /// Also write the final, still-incomplete bucket
        #[arg(long)]
        flush_partial: bool,
    },

    /// Import market data from CSV file(s)
    Input {
        #[command(subcommand)]
        data_type: InputType,
    },
    /// Remove data by exchange / symbol / type / date range
    Remove {
        #[command(subcommand)]
        data_type: RemoveType,
    },
    /// Show summary statistics
    Stats {
        /// Filter to a specific exchange
        #[arg(long)]
        exchange: Option<String>,
        /// Filter to a specific symbol
        #[arg(long)]
        symbol: Option<String>,
    },
    /// Query and display stored data
    View {
        #[command(subcommand)]
        data_type: ViewType,
    },
}

#[derive(Subcommand)]
enum InputType {
    /// Import tick data (bid/ask/last)
    Tick {
        /// CSV file(s) to import
        files: Vec<PathBuf>,
        /// Exchange / broker name (e.g. ctrader, binance)
        #[arg(long, short)]
        exchange: String,
        /// Override symbol (default: extracted from filename)
        #[arg(long)]
        symbol: Option<String>,
        /// Source timezone offset [default: +02:00]
        #[arg(long, default_value = "+02:00")]
        tz_offset: String,
    },
    /// Import bar/OHLCV data
    Bar {
        /// CSV file(s) to import
        files: Vec<PathBuf>,
        /// Exchange / broker name
        #[arg(long, short)]
        exchange: String,
        /// Bar timeframe (1m, 3m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M)
        #[arg(long, short)]
        timeframe: String,
        /// Override symbol (default: extracted from filename)
        #[arg(long)]
        symbol: Option<String>,
        /// Source timezone offset [default: +02:00]
        #[arg(long, default_value = "+02:00")]
        tz_offset: String,
    },
}

#[derive(Subcommand)]
enum RemoveType {
    /// Remove tick data
    Tick {
        #[arg(long, short)]
        exchange: String,
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
    /// Remove bar data
    Bar {
        #[arg(long, short)]
        exchange: String,
        #[arg(long)]
        symbol: String,
        #[arg(long, short)]
        timeframe: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
    /// Remove ALL data (ticks + bars) for an exchange+symbol pair
    Symbol {
        #[arg(long, short)]
        exchange: String,
        /// Symbol to remove
        symbol: String,
    },
    /// Remove ALL data for an entire exchange
    Exchange {
        /// Exchange to remove
        exchange: String,
    },
}

#[derive(Subcommand)]
enum ViewType {
    /// View tick data
    Tick {
        #[arg(long, short)]
        exchange: String,
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        tail: bool,
        #[arg(long)]
        desc: bool,
    },
    /// View bar data
    Bar {
        #[arg(long, short)]
        exchange: String,
        #[arg(long)]
        symbol: String,
        #[arg(long, short)]
        timeframe: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        tail: bool,
        #[arg(long)]
        desc: bool,
    },
}

fn main() -> data_preprocess::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.backend {
        #[cfg(feature = "parquet")]
        Backend::Parquet => {
            let store = data_preprocess::ParquetStore::open(&cli.data_dir)?;
            match cli.command {
                Commands::Input { data_type } => handle_input_parquet(&store, data_type)?,
                Commands::Remove { data_type } => handle_remove_parquet(&store, data_type)?,
                Commands::Stats { exchange, symbol } => {
                    handle_stats_parquet(&store, exchange, symbol)?
                }
                Commands::View { data_type } => handle_view_parquet(&store, data_type)?,
                Commands::Resample {
                    exchange,
                    symbol,
                    timeframe,
                    price_basis,
                    align_offset_seconds,
                    digits,
                    from,
                    to,
                    flush_partial,
                } => handle_resample_parquet(
                    &store,
                    &cli.data_dir,
                    ResampleArgs {
                        exchange,
                        symbol,
                        timeframe,
                        price_basis,
                        align_offset_seconds,
                        digits,
                        from,
                        to,
                        flush_partial,
                    },
                )?,
            }
        }

        #[cfg(not(feature = "parquet"))]
        Backend::Parquet => {
            eprintln!("Error: parquet backend not compiled. Rebuild with `--features parquet`.");
            std::process::exit(1);
        }

        #[cfg(feature = "duckdb-backend")]
        Backend::Duckdb => {
            let db = data_preprocess::Database::open(&cli.db)?;
            match cli.command {
                Commands::Input { data_type } => handle_input_duckdb(&db, data_type)?,
                Commands::Remove { data_type } => handle_remove_duckdb(&db, data_type)?,
                Commands::Stats { exchange, symbol } => handle_stats_duckdb(&db, exchange, symbol)?,
                Commands::View { data_type } => handle_view_duckdb(&db, data_type)?,
                Commands::Resample { .. } => {
                    eprintln!(
                        "Error: resample reads the Parquet tick store. Rerun with `--backend parquet`."
                    );
                    std::process::exit(1);
                }
            }
        }

        #[cfg(not(feature = "duckdb-backend"))]
        Backend::Duckdb => {
            eprintln!(
                "Error: duckdb backend not compiled. Rebuild with `--features duckdb-backend`."
            );
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Arguments accepted by the tick-to-bar resampler.
struct ResampleArgs {
    exchange: String,
    symbol: String,
    timeframe: String,
    price_basis: String,
    align_offset_seconds: i64,
    digits: u32,
    from: Option<String>,
    to: Option<String>,
    flush_partial: bool,
}

/// Rows buffered before one write, keeping memory bounded on multi-year ranges.
#[cfg(feature = "parquet")]
const RESAMPLE_WRITE_BATCH: usize = 20_000;

#[cfg(feature = "parquet")]
fn handle_resample_parquet(
    store: &data_preprocess::ParquetStore,
    data_dir: &std::path::Path,
    args: ResampleArgs,
) -> data_preprocess::Result<()> {
    use data_preprocess::error::DataError;
    use data_preprocess::resample::{BarAggregator, BucketSpec, PriceBasis};
    use data_preprocess::scanner::{ParquetScanBounds, ParquetTickCursor};

    let start = Instant::now();
    let exchange = normalize_exchange(&args.exchange);
    let symbol = args.symbol.to_uppercase();
    let timeframe = Timeframe::parse(&args.timeframe)?;
    let duration = timeframe.fixed_duration_seconds().ok_or_else(|| {
        DataError::InvalidResample(format!(
            "timeframe {} has no fixed duration and cannot be resampled",
            timeframe.as_str()
        ))
    })?;
    let spec = BucketSpec::new(duration, args.align_offset_seconds).ok_or_else(|| {
        DataError::InvalidResample(format!("timeframe {} has no positive duration", timeframe))
    })?;
    let basis = PriceBasis::parse(&args.price_basis).ok_or_else(|| {
        DataError::InvalidResample(format!(
            "price basis must be bid, ask, or mid, got '{}'",
            args.price_basis
        ))
    })?;
    if args.digits > 10 {
        return Err(DataError::InvalidResample(format!(
            "digits must not exceed 10, got {}",
            args.digits
        )));
    }
    let point_size = 10f64.powi(-(args.digits as i32));
    let from = args.from.as_deref().map(parse_datetime_arg).transpose()?;
    let to = args.to.as_deref().map(parse_datetime_arg).transpose()?;

    let mut cursor = ParquetTickCursor::open(
        data_dir,
        &exchange,
        &symbol,
        ParquetScanBounds::new(from, to),
    )?;
    let mut aggregator = BarAggregator::new(
        exchange.clone(),
        symbol.clone(),
        timeframe,
        spec,
        basis,
        point_size,
    );

    let mut buffer = Vec::with_capacity(RESAMPLE_WRITE_BATCH);
    let mut ticks_read = 0usize;
    let mut bars_built = 0usize;
    let mut bars_written = 0usize;
    while let Some(tick) = cursor.next_tick()? {
        ticks_read += 1;
        if let Some(bar) = aggregator.push(tick.ts, tick.bid, tick.ask) {
            buffer.push(bar);
            bars_built += 1;
            if buffer.len() >= RESAMPLE_WRITE_BATCH {
                bars_written += store.insert_bars(&buffer)?;
                buffer.clear();
            }
        }
    }
    if args.flush_partial
        && let Some(bar) = aggregator.flush()
    {
        buffer.push(bar);
        bars_built += 1;
    }
    if !buffer.is_empty() {
        bars_written += store.insert_bars(&buffer)?;
    }

    let out_of_order = aggregator.rejected_out_of_order();
    if out_of_order > 0 {
        eprintln!(
            "warning: dropped {out_of_order} tick(s) that arrived after their bucket had closed; the tick source is not chronological"
        );
    }

    print_import_result(&ImportResult {
        file: format!(
            "{exchange}/{symbol} ticks -> {} {} bars (offset {}s)",
            timeframe.as_str(),
            basis.as_str(),
            spec.alignment_offset_seconds()
        ),
        exchange,
        symbol,
        rows_parsed: ticks_read,
        rows_inserted: bars_written,
        rows_skipped: bars_built.saturating_sub(bars_written),
        elapsed: start.elapsed(),
    });
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
//  Parquet backend handlers
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "parquet")]
fn handle_input_parquet(
    store: &data_preprocess::ParquetStore,
    data_type: InputType,
) -> data_preprocess::Result<()> {
    match data_type {
        InputType::Tick {
            files,
            exchange,
            symbol,
            tz_offset,
        } => {
            let exchange = normalize_exchange(&exchange);
            let offset = parse_tz_offset(&tz_offset)?;
            for file in &files {
                let start = Instant::now();
                let sym = match &symbol {
                    Some(s) => s.to_uppercase(),
                    None => extract_symbol_from_filename(file)?,
                };
                let (ticks, warnings) = parse_tick_csv(file, &exchange, &sym, &offset)?;
                for w in &warnings {
                    tracing::warn!("{}: {}", file.display(), w);
                }
                let inserted = store.insert_ticks(&ticks)?;
                print_import_result(&ImportResult {
                    file: file.display().to_string(),
                    exchange: exchange.clone(),
                    symbol: sym,
                    rows_parsed: ticks.len(),
                    rows_inserted: inserted,
                    rows_skipped: ticks.len().saturating_sub(inserted),
                    elapsed: start.elapsed(),
                });
            }
        }
        InputType::Bar {
            files,
            exchange,
            timeframe,
            symbol,
            tz_offset,
        } => {
            let exchange = normalize_exchange(&exchange);
            let tf = Timeframe::parse(&timeframe)?;
            let offset = parse_tz_offset(&tz_offset)?;
            for file in &files {
                let start = Instant::now();
                let sym = match &symbol {
                    Some(s) => s.to_uppercase(),
                    None => extract_symbol_from_filename(file)?,
                };
                let (bars, warnings) = parse_bar_csv(file, &exchange, &sym, tf, &offset)?;
                for w in &warnings {
                    tracing::warn!("{}: {}", file.display(), w);
                }
                let inserted = store.insert_bars(&bars)?;
                print_import_result(&ImportResult {
                    file: file.display().to_string(),
                    exchange: exchange.clone(),
                    symbol: sym,
                    rows_parsed: bars.len(),
                    rows_inserted: inserted,
                    rows_skipped: bars.len().saturating_sub(inserted),
                    elapsed: start.elapsed(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "parquet")]
fn handle_remove_parquet(
    store: &data_preprocess::ParquetStore,
    data_type: RemoveType,
) -> data_preprocess::Result<()> {
    match data_type {
        RemoveType::Tick {
            exchange,
            symbol,
            from,
            to,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let count = store.delete_ticks(&exchange, &symbol, from, to)?;
            print_delete_result("tick", &exchange, &symbol, count);
        }
        RemoveType::Bar {
            exchange,
            symbol,
            timeframe,
            from,
            to,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let count = store.delete_bars(&exchange, &symbol, &timeframe, from, to)?;
            print_delete_result("bar", &exchange, &format!("{symbol} ({timeframe})"), count);
        }
        RemoveType::Symbol { exchange, symbol } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let (t, b) = store.delete_symbol(&exchange, &symbol)?;
            println!("Removed {t} ticks + {b} bars for {exchange}/{symbol}");
        }
        RemoveType::Exchange { exchange } => {
            let exchange = normalize_exchange(&exchange);
            let (t, b) = store.delete_exchange(&exchange)?;
            println!("Removed {t} ticks + {b} bars for exchange '{exchange}'");
        }
    }
    Ok(())
}

#[cfg(feature = "parquet")]
fn handle_stats_parquet(
    store: &data_preprocess::ParquetStore,
    exchange: Option<String>,
    symbol: Option<String>,
) -> data_preprocess::Result<()> {
    let exchange = exchange.map(|e| normalize_exchange(&e));
    let symbol = symbol.map(|s| s.to_uppercase());
    let rows = store.stats(exchange.as_deref(), symbol.as_deref())?;
    print_stats(&rows, store.total_size());
    Ok(())
}

#[cfg(feature = "parquet")]
fn handle_view_parquet(
    store: &data_preprocess::ParquetStore,
    data_type: ViewType,
) -> data_preprocess::Result<()> {
    match data_type {
        ViewType::Tick {
            exchange,
            symbol,
            from,
            to,
            limit,
            tail,
            desc,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let (ticks, total) = store.query_ticks(&QueryOpts {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                from,
                to,
                limit,
                tail,
                descending: desc,
            })?;
            print_ticks(&exchange, &symbol, &ticks, total);
        }
        ViewType::Bar {
            exchange,
            symbol,
            timeframe,
            from,
            to,
            limit,
            tail,
            desc,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let (bars, total) = store.query_bars(&BarQueryOpts {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                timeframe: timeframe.clone(),
                from,
                to,
                limit,
                tail,
                descending: desc,
            })?;
            print_bars(&exchange, &symbol, &timeframe, &bars, total);
        }
    }
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
//  DuckDB backend handlers
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "duckdb-backend")]
fn handle_input_duckdb(
    db: &data_preprocess::Database,
    data_type: InputType,
) -> data_preprocess::Result<()> {
    match data_type {
        InputType::Tick {
            files,
            exchange,
            symbol,
            tz_offset,
        } => {
            let exchange = normalize_exchange(&exchange);
            let offset = parse_tz_offset(&tz_offset)?;
            for file in &files {
                let start = Instant::now();
                let sym = match &symbol {
                    Some(s) => s.to_uppercase(),
                    None => extract_symbol_from_filename(file)?,
                };
                let (ticks, warnings) = parse_tick_csv(file, &exchange, &sym, &offset)?;
                for w in &warnings {
                    tracing::warn!("{}: {}", file.display(), w);
                }
                let inserted = db.insert_ticks(&ticks)?;
                print_import_result(&ImportResult {
                    file: file.display().to_string(),
                    exchange: exchange.clone(),
                    symbol: sym,
                    rows_parsed: ticks.len(),
                    rows_inserted: inserted,
                    rows_skipped: ticks.len() - inserted,
                    elapsed: start.elapsed(),
                });
            }
        }
        InputType::Bar {
            files,
            exchange,
            timeframe,
            symbol,
            tz_offset,
        } => {
            let exchange = normalize_exchange(&exchange);
            let tf = Timeframe::parse(&timeframe)?;
            let offset = parse_tz_offset(&tz_offset)?;
            for file in &files {
                let start = Instant::now();
                let sym = match &symbol {
                    Some(s) => s.to_uppercase(),
                    None => extract_symbol_from_filename(file)?,
                };
                let (bars, warnings) = parse_bar_csv(file, &exchange, &sym, tf, &offset)?;
                for w in &warnings {
                    tracing::warn!("{}: {}", file.display(), w);
                }
                let inserted = db.insert_bars(&bars)?;
                print_import_result(&ImportResult {
                    file: file.display().to_string(),
                    exchange: exchange.clone(),
                    symbol: sym,
                    rows_parsed: bars.len(),
                    rows_inserted: inserted,
                    rows_skipped: bars.len() - inserted,
                    elapsed: start.elapsed(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "duckdb-backend")]
fn handle_remove_duckdb(
    db: &data_preprocess::Database,
    data_type: RemoveType,
) -> data_preprocess::Result<()> {
    match data_type {
        RemoveType::Tick {
            exchange,
            symbol,
            from,
            to,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let count = db.delete_ticks(&exchange, &symbol, from, to)?;
            print_delete_result("tick", &exchange, &symbol, count);
        }
        RemoveType::Bar {
            exchange,
            symbol,
            timeframe,
            from,
            to,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let count = db.delete_bars(&exchange, &symbol, &timeframe, from, to)?;
            print_delete_result("bar", &exchange, &format!("{symbol} ({timeframe})"), count);
        }
        RemoveType::Symbol { exchange, symbol } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let (t, b) = db.delete_symbol(&exchange, &symbol)?;
            println!("Removed {t} ticks + {b} bars for {exchange}/{symbol}");
        }
        RemoveType::Exchange { exchange } => {
            let exchange = normalize_exchange(&exchange);
            let (t, b) = db.delete_exchange(&exchange)?;
            println!("Removed {t} ticks + {b} bars for exchange '{exchange}'");
        }
    }
    Ok(())
}

#[cfg(feature = "duckdb-backend")]
fn handle_stats_duckdb(
    db: &data_preprocess::Database,
    exchange: Option<String>,
    symbol: Option<String>,
) -> data_preprocess::Result<()> {
    let exchange = exchange.map(|e| normalize_exchange(&e));
    let symbol = symbol.map(|s| s.to_uppercase());
    let rows = db.stats(exchange.as_deref(), symbol.as_deref())?;
    print_stats(&rows, db.file_size());
    Ok(())
}

#[cfg(feature = "duckdb-backend")]
fn handle_view_duckdb(
    db: &data_preprocess::Database,
    data_type: ViewType,
) -> data_preprocess::Result<()> {
    match data_type {
        ViewType::Tick {
            exchange,
            symbol,
            from,
            to,
            limit,
            tail,
            desc,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let (ticks, total) = db.query_ticks(&QueryOpts {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                from,
                to,
                limit,
                tail,
                descending: desc,
            })?;
            print_ticks(&exchange, &symbol, &ticks, total);
        }
        ViewType::Bar {
            exchange,
            symbol,
            timeframe,
            from,
            to,
            limit,
            tail,
            desc,
        } => {
            let exchange = normalize_exchange(&exchange);
            let symbol = symbol.to_uppercase();
            let from = from.map(|s| parse_datetime_arg(&s)).transpose()?;
            let to = to.map(|s| parse_datetime_arg(&s)).transpose()?;
            let (bars, total) = db.query_bars(&BarQueryOpts {
                exchange: exchange.clone(),
                symbol: symbol.clone(),
                timeframe: timeframe.clone(),
                from,
                to,
                limit,
                tail,
                descending: desc,
            })?;
            print_bars(&exchange, &symbol, &timeframe, &bars, total);
        }
    }
    Ok(())
}
