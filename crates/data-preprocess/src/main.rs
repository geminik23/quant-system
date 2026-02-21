use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand};

use data_preprocess::db::{BarQueryOpts, Database, QueryOpts};
use data_preprocess::display::{
    print_bars, print_delete_result, print_import_result, print_stats, print_ticks,
};
use data_preprocess::models::{ImportResult, Timeframe};
use data_preprocess::parser::bar_csv::parse_bar_csv;
use data_preprocess::parser::tick_csv::parse_tick_csv;
use data_preprocess::parser::{
    extract_symbol_from_filename, normalize_exchange, parse_datetime_arg, parse_tz_offset,
};

#[derive(Parser)]
#[command(name = "data-preprocess", about = "Historical market data CLI")]
struct Cli {
    /// Path to DuckDB database file
    #[arg(long, default_value = "market_data.duckdb", env = "DATA_PREPROCESS_DB")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
        /// Bar timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M)
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
    let db = Database::open(&cli.db)?;

    match cli.command {
        Commands::Input { data_type } => handle_input(&db, data_type)?,
        Commands::Remove { data_type } => handle_remove(&db, data_type)?,
        Commands::Stats { exchange, symbol } => handle_stats(&db, exchange, symbol)?,
        Commands::View { data_type } => handle_view(&db, data_type)?,
    }
    Ok(())
}

fn handle_input(db: &Database, data_type: InputType) -> data_preprocess::Result<()> {
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

fn handle_remove(db: &Database, data_type: RemoveType) -> data_preprocess::Result<()> {
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
            println!(
                "Removed {} ticks + {} bars for {}/{}",
                t, b, exchange, symbol
            );
        }
        RemoveType::Exchange { exchange } => {
            let exchange = normalize_exchange(&exchange);
            let (t, b) = db.delete_exchange(&exchange)?;
            println!(
                "Removed {} ticks + {} bars for exchange '{}'",
                t, b, exchange
            );
        }
    }
    Ok(())
}

fn handle_stats(
    db: &Database,
    exchange: Option<String>,
    symbol: Option<String>,
) -> data_preprocess::Result<()> {
    let exchange = exchange.map(|e| normalize_exchange(&e));
    let symbol = symbol.map(|s| s.to_uppercase());
    let rows = db.stats(exchange.as_deref(), symbol.as_deref())?;
    print_stats(&rows, db.file_size());
    Ok(())
}

fn handle_view(db: &Database, data_type: ViewType) -> data_preprocess::Result<()> {
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
