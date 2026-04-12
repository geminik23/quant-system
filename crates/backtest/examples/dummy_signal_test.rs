//! Standalone smoke-test: load Parquet data, inject dummy signals, print results.
//!
//! # Usage
//!
//! ```bash
//! # Tick data (direct mode — signals with inline SL/TP/rules)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16"
//!
//! # Bar data (1-minute)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --data-type bar \
//!     --timeframe 1m \
//!     --from "2026-01-15" \
//!     --to "2026-01-16"
//!
//! # With a management profile (profile mode — raw entries transformed by profile)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16" \
//!     --profiles-path crates/backtest/profiles.toml \
//!     --profile aggressive
//!
//! # F14 raw-signals mode (full signal actions: entries + management signals)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16" \
//!     --mode raw-signals
//!
//! # F14 raw-signals with profile (entries transformed by profile, management
//! # signals pass through untouched)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16" \
//!     --mode raw-signals-profile \
//!     --profiles-path crates/backtest/profiles.toml \
//!     --profile aggressive
//! ```
//!
//! The example will:
//! 1. Load data from Parquet store
//! 2. Print a summary of loaded events (count, time range, price range)
//! 3. Generate dummy signals from the actual data (buy near the start, sell later)
//! 4. Run the backtest using the selected mode
//! 5. Print the full BacktestResult report
//!
//! ## Modes
//!
//! - **direct** (default): Generates `Signal` (Action::Open) with inline SL, TP,
//!   and trailing stop rules. Uses `run_signals()`.
//! - **profile**: Generates `RawSignalEntry` with multiple targets, transforms
//!   them through a management profile. Uses `run_signals()`.
//! - **raw-signals** (F14): Generates `RawSignal` with full action vocabulary —
//!   entry signals plus management signals (modify SL, partial close, move SL to
//!   entry, scale-in, close all in group, etc.). Uses `run_raw_signals()`.
//! - **raw-signals-profile** (F14 + F09): Same as raw-signals but entry signals
//!   are transformed through a management profile while management signals pass
//!   through untouched. Uses `run_raw_signals()` with a profile.

use chrono::NaiveDateTime;
use data_preprocess::{BarQueryOpts, ParquetStore, QueryOpts, Timeframe};
use qs_backtest::data_feed::{DataFeed, MarketEvent, VecFeed, bars_to_feed, ticks_to_feed};
use qs_backtest::profile::{PositionRef, ProfileRegistry, RawSignal, RawSignalEntry};
use qs_backtest::runner::{BacktestConfig, BacktestRunner};
use qs_core::types::{Action, OrderType, RuleConfig, Side, Signal, TargetSpec};
use qs_symbols::SymbolRegistry;

use std::collections::HashMap;
use std::path::Path;
use std::process;

// ── CLI Args (manual parsing to avoid adding clap as a dep) ─────────────────

struct Args {
    data_dir: String,
    exchange: String,
    symbol: String,
    data_type: String,         // "tick" or "bar"
    timeframe: Option<String>, // required when data_type == "bar"
    from: Option<String>,
    to: Option<String>,
    profiles_path: Option<String>,
    profile: Option<String>,
    symbols_path: Option<String>,
    initial_balance: f64,
    mode: String, // "direct", "profile", "raw-signals", "raw-signals-profile"
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut data_dir = String::new();
    let mut exchange = String::new();
    let mut symbol = String::new();
    let mut data_type = String::from("tick");
    let mut timeframe = None;
    let mut from = None;
    let mut to = None;
    let mut profiles_path = None;
    let mut profile = None;
    let mut symbols_path = None;
    let mut initial_balance = 10_000.0;
    let mut mode = String::new(); // empty means auto-detect

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                i += 1;
                data_dir = args[i].clone();
            }
            "--exchange" => {
                i += 1;
                exchange = args[i].clone();
            }
            "--symbol" => {
                i += 1;
                symbol = args[i].clone();
            }
            "--data-type" => {
                i += 1;
                data_type = args[i].clone();
            }
            "--timeframe" => {
                i += 1;
                timeframe = Some(args[i].clone());
            }
            "--from" => {
                i += 1;
                from = Some(args[i].clone());
            }
            "--to" => {
                i += 1;
                to = Some(args[i].clone());
            }
            "--profiles-path" => {
                i += 1;
                profiles_path = Some(args[i].clone());
            }
            "--profile" => {
                i += 1;
                profile = Some(args[i].clone());
            }
            "--balance" => {
                i += 1;
                initial_balance = args[i].parse().expect("invalid --balance");
            }
            "--symbols-path" => {
                i += 1;
                symbols_path = Some(args[i].clone());
            }
            "--mode" => {
                i += 1;
                mode = args[i].clone();
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: dummy_signal_test --data-dir <DIR> --exchange <EX> --symbol <SYM> [OPTIONS]"
                );
                eprintln!();
                eprintln!("Required:");
                eprintln!("  --data-dir <DIR>       Parquet root directory");
                eprintln!("  --exchange <NAME>      Exchange partition (e.g. ctrader)");
                eprintln!("  --symbol <NAME>        Symbol name (e.g. eurusd)");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --data-type <TYPE>     tick (default) or bar");
                eprintln!("  --timeframe <TF>       Timeframe for bars (1m, 5m, 1h, etc.)");
                eprintln!(
                    "  --from <DATETIME>      Start filter (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
                );
                eprintln!("  --to <DATETIME>        End filter");
                eprintln!("  --profiles-path <FILE> Path to profiles.toml");
                eprintln!("  --profile <NAME>       Profile name to apply");
                eprintln!(
                    "  --symbols-path <FILE>  Path to symbols.toml (for contract sizes / P&L)"
                );
                eprintln!("  --balance <AMOUNT>     Initial balance (default: 10000)");
                eprintln!("  --mode <MODE>          Signal mode:");
                eprintln!(
                    "                           direct            - Action::Open with SL/TP/rules (default)"
                );
                eprintln!(
                    "                           profile           - RawSignalEntry + profile transform"
                );
                eprintln!(
                    "                           raw-signals       - F14 full signal actions (entry + management)"
                );
                eprintln!(
                    "                           raw-signals-profile - F14 signals + profile for entries"
                );
                process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {other}");
                process::exit(1);
            }
        }
        i += 1;
    }

    if data_dir.is_empty() || exchange.is_empty() || symbol.is_empty() {
        eprintln!("Error: --data-dir, --exchange, and --symbol are required.");
        eprintln!("Run with --help for usage.");
        process::exit(1);
    }

    // Auto-detect mode if not explicitly set
    if mode.is_empty() {
        if profiles_path.is_some() && profile.is_some() {
            mode = "profile".into();
        } else {
            mode = "direct".into();
        }
    }

    // Validate mode
    match mode.as_str() {
        "direct" | "profile" | "raw-signals" | "raw-signals-profile" => {}
        other => {
            eprintln!(
                "Error: invalid --mode '{other}'. Must be one of: direct, profile, raw-signals, raw-signals-profile"
            );
            process::exit(1);
        }
    }

    // Validate mode + profile requirements
    if (mode == "profile" || mode == "raw-signals-profile")
        && (profiles_path.is_none() || profile.is_none())
    {
        eprintln!("Error: --profiles-path and --profile are required for mode '{mode}'.");
        process::exit(1);
    }

    Args {
        data_dir,
        exchange,
        symbol,
        data_type,
        timeframe,
        from,
        to,
        profiles_path,
        profile,
        symbols_path,
        initial_balance,
        mode,
    }
}

// ── Datetime parsing ────────────────────────────────────────────────────────

fn parse_dt(s: &str) -> NaiveDateTime {
    // Try ISO with T separator
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return dt;
    }
    // Try space separator
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return dt;
    }
    // Date only → midnight
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0).unwrap();
    }
    eprintln!("Error: cannot parse datetime '{s}' (expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)");
    process::exit(1);
}

// ── Data loading ────────────────────────────────────────────────────────────

/// Resolve the actual on-disk partition name for a Hive key.
///
/// Parquet stores use Hive-style directories like `exchange=icmarkets/symbol=EURUSD`.
/// The user might type `eurusd`, `EURUSD`, or `EurUsd` — we scan the parent directory
/// for a case-insensitive match and return the exact on-disk value.
fn resolve_partition(
    data_dir: &str,
    data_type: &str,
    key: &str,
    value: &str,
    parent: &str,
) -> String {
    let dir = Path::new(data_dir).join(data_type).join(parent);
    let prefix = format!("{}=", key);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let lower = value.to_lowercase();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(val) = name_str.strip_prefix(&prefix) {
                if val.to_lowercase() == lower {
                    return val.to_string();
                }
            }
        }
    }
    // Fallback: return as-is (will fail naturally downstream with a clear "no data" message)
    value.to_string()
}

fn load_events(args: &Args) -> Vec<MarketEvent> {
    let store = ParquetStore::open(&args.data_dir).unwrap_or_else(|e| {
        eprintln!("Error opening Parquet store at '{}': {e}", args.data_dir);
        process::exit(1);
    });

    let from = args.from.as_deref().map(parse_dt);
    let to = args.to.as_deref().map(parse_dt);

    let dt = args.data_type.to_lowercase();
    let data_subdir = if dt == "tick" { "ticks" } else { "bars" };

    // Resolve exchange and symbol to their actual on-disk case
    let exchange = resolve_partition(&args.data_dir, data_subdir, "exchange", &args.exchange, "");
    let symbol = resolve_partition(
        &args.data_dir,
        data_subdir,
        "symbol",
        &args.symbol,
        &format!("exchange={exchange}"),
    );

    if exchange != args.exchange || symbol != args.symbol {
        println!(
            "Resolved partition names: exchange={} symbol={} (input: {} {})",
            exchange, symbol, args.exchange, args.symbol
        );
    }

    if dt == "tick" {
        let opts = QueryOpts {
            exchange: exchange.clone(),
            symbol: symbol.clone(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (ticks, total) = store.query_ticks(&opts).unwrap_or_else(|e| {
            eprintln!("Error querying ticks: {e}");
            process::exit(1);
        });
        println!("Loaded {total} ticks for {exchange}/{symbol}");
        let feed = ticks_to_feed(ticks);
        extract_events(feed)
    } else if dt == "bar" {
        let tf_str = args.timeframe.as_deref().unwrap_or_else(|| {
            eprintln!("Error: --timeframe is required when --data-type is bar");
            process::exit(1);
        });
        let tf = Timeframe::parse(tf_str).unwrap_or_else(|e| {
            eprintln!("Error parsing timeframe '{tf_str}': {e}");
            process::exit(1);
        });
        let opts = BarQueryOpts {
            exchange: exchange.clone(),
            symbol: symbol.clone(),
            timeframe: tf.as_str().to_string(),
            from,
            to,
            limit: 0,
            tail: false,
            descending: false,
        };
        let (bars, total) = store.query_bars(&opts).unwrap_or_else(|e| {
            eprintln!("Error querying bars: {e}");
            process::exit(1);
        });
        println!("Loaded {total} bars ({tf}) for {exchange}/{symbol}");
        let feed = bars_to_feed(bars);
        extract_events(feed)
    } else {
        eprintln!("Error: --data-type must be 'tick' or 'bar', got '{dt}'");
        process::exit(1);
    }
}

fn extract_events(mut feed: VecFeed) -> Vec<MarketEvent> {
    let mut events = Vec::with_capacity(feed.total());
    while let Some(ev) = feed.next_event() {
        events.push(ev);
    }
    events
}

// ── Data summary ────────────────────────────────────────────────────────────

fn print_data_summary(events: &[MarketEvent]) {
    if events.is_empty() {
        eprintln!("No data loaded — check your --exchange, --symbol, --from, --to filters.");
        process::exit(1);
    }

    let first_ts = events.first().unwrap().ts();
    let last_ts = events.last().unwrap().ts();

    let (mut min_price, mut max_price) = (f64::MAX, f64::MIN);
    for ev in events {
        let q = ev.to_quote();
        min_price = min_price.min(q.bid);
        max_price = max_price.max(q.ask);
    }

    println!();
    println!("╔══════════════════════════════════════════════╗");
    println!("║             DATA SUMMARY                    ║");
    println!("╠══════════════════════════════════════════════╣");
    println!("║ Events:     {:<32} ║", events.len());
    println!("║ First:      {:<32} ║", first_ts);
    println!("║ Last:       {:<32} ║", last_ts);
    println!("║ Price low:  {:<32.5} ║", min_price);
    println!("║ Price high: {:<32.5} ║", max_price);
    println!("╚══════════════════════════════════════════════╝");
    println!();
}

// ── Dummy signal generation: direct mode ────────────────────────────────────

/// Generates dummy signals from the actual loaded data:
///   - BUY at ~10% into the data, with SL 50 pips below and TP 100 pips above
///   - SELL at ~60% into the data, with SL 50 pips above and TP 100 pips below
///
/// "pips" here are approximated based on the price magnitude.
fn generate_dummy_signals(events: &[MarketEvent], symbol: &str) -> Vec<Signal> {
    let n = events.len();
    if n < 20 {
        eprintln!(
            "Warning: very few data points ({}), generating minimal signals",
            n
        );
    }

    // Pick entry points at ~10% and ~60% of the data
    let buy_idx = n / 10;
    let sell_idx = n * 6 / 10;

    let buy_event = &events[buy_idx];
    let sell_event = &events[sell_idx];

    let buy_quote = buy_event.to_quote();
    let sell_quote = sell_event.to_quote();

    // Estimate pip size from price magnitude
    let pip = estimate_pip(buy_quote.ask);

    let buy_entry = buy_quote.ask; // buy at ask
    let buy_sl = buy_entry - 50.0 * pip;
    let buy_tp = buy_entry + 100.0 * pip;

    let sell_entry = sell_quote.bid; // sell at bid
    let sell_sl = sell_entry + 50.0 * pip;
    let sell_tp = sell_entry - 100.0 * pip;

    println!("Generated dummy signals:");
    println!(
        "  BUY  at {} | price={:.5} sl={:.5} tp={:.5}",
        buy_event.ts(),
        buy_entry,
        buy_sl,
        buy_tp
    );
    println!(
        "  SELL at {} | price={:.5} sl={:.5} tp={:.5}",
        sell_event.ts(),
        sell_entry,
        sell_sl,
        sell_tp
    );
    println!();

    vec![
        Signal {
            ts: buy_event.ts(),
            action: Action::Open {
                symbol: symbol.to_string(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: None,
                size: 0.10,
                stoploss: Some(buy_sl),
                targets: vec![TargetSpec {
                    price: buy_tp,
                    close_ratio: 1.0,
                }],
                rules: vec![RuleConfig::TrailingStop {
                    distance: 30.0 * pip,
                }],
                group: Some("dummy_buy".into()),
            },
        },
        Signal {
            ts: sell_event.ts(),
            action: Action::Open {
                symbol: symbol.to_string(),
                side: Side::Sell,
                order_type: OrderType::Market,
                price: None,
                size: 0.10,
                stoploss: Some(sell_sl),
                targets: vec![TargetSpec {
                    price: sell_tp,
                    close_ratio: 1.0,
                }],
                rules: vec![RuleConfig::TrailingStop {
                    distance: 30.0 * pip,
                }],
                group: Some("dummy_sell".into()),
            },
        },
    ]
}

/// Rough pip estimator from price magnitude.
fn estimate_pip(price: f64) -> f64 {
    if price > 500.0 {
        // Indices / gold range: pip ~ 0.01 or 0.1
        0.10
    } else if price > 10.0 {
        // JPY pairs / commodities
        0.01
    } else {
        // Standard forex
        0.0001
    }
}

// ── Raw signal entries (for profile mode) ───────────────────────────────────

fn generate_raw_signals(events: &[MarketEvent], symbol: &str) -> Vec<RawSignalEntry> {
    let n = events.len();
    let buy_idx = n / 10;
    let sell_idx = n * 6 / 10;

    let buy_event = &events[buy_idx];
    let sell_event = &events[sell_idx];

    let buy_quote = buy_event.to_quote();
    let sell_quote = sell_event.to_quote();

    let pip = estimate_pip(buy_quote.ask);

    let buy_entry = buy_quote.ask;
    let buy_sl = buy_entry - 50.0 * pip;
    let buy_tp1 = buy_entry + 50.0 * pip;
    let buy_tp2 = buy_entry + 100.0 * pip;
    let buy_tp3 = buy_entry + 150.0 * pip;

    let sell_entry = sell_quote.bid;
    let sell_sl = sell_entry + 50.0 * pip;
    let sell_tp1 = sell_entry - 50.0 * pip;
    let sell_tp2 = sell_entry - 100.0 * pip;
    let sell_tp3 = sell_entry - 150.0 * pip;

    println!("Generated raw signals (for profile transform):");
    println!(
        "  BUY  at {} | price={:.5} sl={:.5} tp1={:.5} tp2={:.5} tp3={:.5}",
        buy_event.ts(),
        buy_entry,
        buy_sl,
        buy_tp1,
        buy_tp2,
        buy_tp3
    );
    println!(
        "  SELL at {} | price={:.5} sl={:.5} tp1={:.5} tp2={:.5} tp3={:.5}",
        sell_event.ts(),
        sell_entry,
        sell_sl,
        sell_tp1,
        sell_tp2,
        sell_tp3
    );
    println!();

    vec![
        RawSignalEntry {
            ts: buy_event.ts(),
            symbol: symbol.to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            size: 0.10,
            stoploss: Some(buy_sl),
            targets: vec![buy_tp1, buy_tp2, buy_tp3],
            group: Some("dummy_buy".into()),
        },
        RawSignalEntry {
            ts: sell_event.ts(),
            symbol: symbol.to_string(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: None,
            size: 0.10,
            stoploss: Some(sell_sl),
            targets: vec![sell_tp1, sell_tp2, sell_tp3],
            group: Some("dummy_sell".into()),
        },
    ]
}

// ── F14 full signal generation (raw-signals mode) ───────────────────────────

/// Generates a full F14 signal stream with entry + management signals.
///
/// The stream demonstrates:
///   1. Open a BUY position (entry) in group "alpha"
///   2. Tighten stoploss (ModifyStoploss)
///   3. Take partial profits (ClosePartial 50%)
///   4. Move stoploss to entry / breakeven (MoveStoplossToEntry)
///   5. Open a SELL position (entry) in group "beta"
///   6. Scale into the SELL position (ScaleIn)
///   7. Add a trailing stop rule to the SELL (AddRule)
///   8. Open another BUY in group "alpha"
///   9. Close all positions in group "beta" (CloseAllInGroup)
///  10. Modify stoploss for all on the symbol (ModifyAllStoploss — not available
///      as a RawSignal; we use CloseAllOf as a close-all-on-symbol fallback,
///      or just close everything)
///  11. Close all remaining positions (CloseAll)
fn generate_f14_raw_signals(events: &[MarketEvent], symbol: &str) -> Vec<RawSignal> {
    let n = events.len();
    if n < 100 {
        eprintln!(
            "Warning: very few data points ({}), F14 signals may not all fire meaningfully",
            n
        );
    }

    let pip = estimate_pip(events[0].to_quote().ask);

    // Pick timestamps at various points through the data
    let idx_entry1 = n / 20; // ~5% — first BUY entry
    let idx_modify_sl = n * 3 / 20; // ~15% — tighten SL
    let idx_partial = n * 5 / 20; // ~25% — partial close
    let idx_breakeven = idx_partial + 1; // right after partial close
    let idx_entry2 = n * 7 / 20; // ~35% — SELL entry
    let idx_scale_in = n * 8 / 20; // ~40% — scale into SELL
    let idx_add_rule = idx_scale_in + 1; // right after scale-in
    let idx_entry3 = n * 10 / 20; // ~50% — second BUY
    let idx_close_group = n * 13 / 20; // ~65% — close group "beta"
    let idx_close_all = n * 17 / 20; // ~85% — close everything

    let ev1 = &events[idx_entry1];
    let ev_mod = &events[idx_modify_sl];
    let ev_partial = &events[idx_partial];
    let ev_be = &events[idx_breakeven];
    let ev2 = &events[idx_entry2];
    let ev_scale = &events[idx_scale_in];
    let ev_rule = &events[idx_add_rule];
    let ev3 = &events[idx_entry3];
    let ev_close_g = &events[idx_close_group];
    let ev_close_all = &events[idx_close_all];

    let buy_ask = ev1.to_quote().ask;
    let buy_sl = buy_ask - 50.0 * pip;
    let buy_tp1 = buy_ask + 50.0 * pip;
    let buy_tp2 = buy_ask + 100.0 * pip;
    let buy_tp3 = buy_ask + 150.0 * pip;

    let sell_bid = ev2.to_quote().bid;
    let sell_sl = sell_bid + 50.0 * pip;
    let sell_tp1 = sell_bid - 50.0 * pip;

    let buy3_ask = ev3.to_quote().ask;
    let buy3_sl = buy3_ask - 40.0 * pip;
    let buy3_tp1 = buy3_ask + 60.0 * pip;
    let buy3_tp2 = buy3_ask + 120.0 * pip;

    let tightened_sl = buy_ask - 25.0 * pip;

    let signals = vec![
        // 1. Open BUY in group "alpha"
        RawSignal::Entry {
            ts: ev1.ts(),
            symbol: symbol.to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            size: 0.10,
            stoploss: Some(buy_sl),
            targets: vec![buy_tp1, buy_tp2, buy_tp3],
            group: Some("alpha".into()),
        },
        // 2. Tighten stoploss on the last opened position on this symbol
        RawSignal::ModifyStoploss {
            ts: ev_mod.ts(),
            position: PositionRef::LastOnSymbol {
                symbol: symbol.to_string(),
            },
            price: tightened_sl,
        },
        // 3. Partial close 50% of the position
        RawSignal::ClosePartial {
            ts: ev_partial.ts(),
            position: PositionRef::LastOnSymbol {
                symbol: symbol.to_string(),
            },
            ratio: 0.5,
        },
        // 4. Move stoploss to entry (breakeven)
        RawSignal::MoveStoplossToEntry {
            ts: ev_be.ts(),
            position: PositionRef::LastOnSymbol {
                symbol: symbol.to_string(),
            },
        },
        // 5. Open SELL in group "beta"
        RawSignal::Entry {
            ts: ev2.ts(),
            symbol: symbol.to_string(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: None,
            size: 0.05,
            stoploss: Some(sell_sl),
            targets: vec![sell_tp1],
            group: Some("beta".into()),
        },
        // 6. Scale into the SELL position (add more size)
        RawSignal::ScaleIn {
            ts: ev_scale.ts(),
            position: PositionRef::LastInGroup {
                group_id: "beta".into(),
            },
            price: None,
            size: 0.03,
        },
        // 7. Add a trailing stop rule to the SELL
        RawSignal::AddRule {
            ts: ev_rule.ts(),
            position: PositionRef::LastInGroup {
                group_id: "beta".into(),
            },
            rule: qs_backtest::profile::RuleConfigDef::TrailingStop {
                distance: 30.0 * pip,
            },
        },
        // 8. Open another BUY in group "alpha"
        RawSignal::Entry {
            ts: ev3.ts(),
            symbol: symbol.to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            size: 0.08,
            stoploss: Some(buy3_sl),
            targets: vec![buy3_tp1, buy3_tp2],
            group: Some("alpha".into()),
        },
        // 9. Close all positions in group "beta"
        RawSignal::CloseAllInGroup {
            ts: ev_close_g.ts(),
            group_id: "beta".into(),
        },
        // 10. Close all remaining positions
        RawSignal::CloseAll {
            ts: ev_close_all.ts(),
        },
    ];

    println!(
        "Generated F14 raw signal stream ({} signals):",
        signals.len()
    );
    for (i, sig) in signals.iter().enumerate() {
        let desc = match sig {
            RawSignal::Entry {
                side, size, group, ..
            } => {
                format!("Entry {:?} size={:.2} group={:?}", side, size, group)
            }
            RawSignal::ModifyStoploss { price, .. } => {
                format!("ModifyStoploss price={:.5}", price)
            }
            RawSignal::ClosePartial { ratio, .. } => {
                format!("ClosePartial ratio={:.0}%", ratio * 100.0)
            }
            RawSignal::MoveStoplossToEntry { .. } => "MoveStoplossToEntry (breakeven)".to_string(),
            RawSignal::ScaleIn { size, .. } => {
                format!("ScaleIn size={:.2}", size)
            }
            RawSignal::AddRule { rule, .. } => {
                format!("AddRule {:?}", rule)
            }
            RawSignal::CloseAllInGroup { group_id, .. } => {
                format!("CloseAllInGroup \"{}\"", group_id)
            }
            RawSignal::CloseAll { .. } => "CloseAll".to_string(),
            other => format!("{:?}", other),
        };
        println!("  {:>2}. [{}] {}", i + 1, sig.ts(), desc);
    }
    println!();

    signals
}

// ── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let args = parse_args();

    println!("Mode: {}", args.mode);
    println!();

    // 1. Load market data
    let events = load_events(&args);
    print_data_summary(&events);

    // Use the symbol name from the actual data (may differ in case from CLI input).
    let data_symbol = events
        .first()
        .map(|ev| ev.to_quote().symbol.clone())
        .unwrap_or_else(|| args.symbol.clone());

    // 2. Load symbol registry for contract sizes (P&L calculation).
    //    Without this, P&L = price_diff × lots (meaningless tiny numbers).
    //    With it,    P&L = price_diff × lots × contract_size (real USD values).
    let contract_sizes: HashMap<String, f64> = if let Some(ref sp) = args.symbols_path {
        let registry = SymbolRegistry::load(sp).unwrap_or_else(|e| {
            eprintln!("Error loading symbol registry from '{sp}': {e}");
            process::exit(1);
        });
        // Build contract_sizes for every known symbol.
        // Key must match the data_symbol (on-disk case) since that's what
        // the engine and executor see.
        let mut sizes = HashMap::new();
        let canonical = registry.normalize_or_passthrough(&data_symbol);
        if let Some(spec) = registry.spec(&canonical) {
            // Map both the canonical name and the data symbol name
            sizes.insert(data_symbol.clone(), spec.lot_base_units as f64);
            if canonical != data_symbol {
                sizes.insert(canonical.to_string(), spec.lot_base_units as f64);
            }
            println!(
                "Contract size for {}: {} (from {})",
                data_symbol, spec.lot_base_units, sp
            );
        } else {
            eprintln!(
                "Warning: symbol '{}' not found in registry, P&L will use raw multiplier 1.0",
                data_symbol
            );
        }
        sizes
    } else {
        println!("Tip: use --symbols-path crates/symbols/symbols.toml for correct P&L values");
        HashMap::new()
    };

    // 3. Build config
    let config = BacktestConfig {
        initial_balance: args.initial_balance,
        close_on_finish: true,
        contract_sizes,
        ..Default::default()
    };

    // 3. Run backtest based on mode
    let result = match args.mode.as_str() {
        "direct" => {
            // Direct mode: generate Signal with inline SL/TP/rules → run_signals
            let signals = generate_dummy_signals(&events, &data_symbol);
            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!(
                "Running backtest (direct mode, {} signals)...",
                signals.len()
            );
            println!();
            runner.run_signals(&mut feed, signals)
        }
        "profile" => {
            // Profile mode: RawSignalEntry → profile.apply_batch → run_signals
            let profiles_path = args.profiles_path.as_ref().unwrap();
            let profile_name = args.profile.as_ref().unwrap();

            let registry = ProfileRegistry::load(profiles_path).unwrap_or_else(|e| {
                eprintln!("Error loading profiles from '{profiles_path}': {e}");
                process::exit(1);
            });
            let profile = registry.get(profile_name).unwrap_or_else(|| {
                let available = registry.names();
                eprintln!("Error: profile '{profile_name}' not found. Available: {available:?}");
                process::exit(1);
            });
            let raw = generate_raw_signals(&events, &data_symbol);
            let transformed = profile.apply_batch(&raw);
            println!(
                "Applied profile '{profile_name}' → {} signals",
                transformed.len()
            );
            for (i, sig) in transformed.iter().enumerate() {
                println!("  Signal {}: ts={} action={:?}", i, sig.ts, sig.action);
            }
            println!();

            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!("Running backtest (profile mode)...");
            println!();
            runner.run_signals(&mut feed, transformed)
        }
        "raw-signals" => {
            // F14 mode: RawSignal stream (entries + management) → run_raw_signals
            let raw_signals = generate_f14_raw_signals(&events, &data_symbol);
            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!(
                "Running backtest (F14 raw-signals mode, {} signals, no profile)...",
                raw_signals.len()
            );
            println!();
            runner.run_raw_signals(&mut feed, raw_signals, None)
        }
        "raw-signals-profile" => {
            // F14 + profile mode: entries go through profile transform,
            // management signals pass through untouched → run_raw_signals
            let profiles_path = args.profiles_path.as_ref().unwrap();
            let profile_name = args.profile.as_ref().unwrap();

            let registry = ProfileRegistry::load(profiles_path).unwrap_or_else(|e| {
                eprintln!("Error loading profiles from '{profiles_path}': {e}");
                process::exit(1);
            });
            let profile = registry.get(profile_name).unwrap_or_else(|| {
                let available = registry.names();
                eprintln!("Error: profile '{profile_name}' not found. Available: {available:?}");
                process::exit(1);
            });

            let raw_signals = generate_f14_raw_signals(&events, &data_symbol);
            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!(
                "Running backtest (F14 raw-signals + profile '{}', {} signals)...",
                profile_name,
                raw_signals.len()
            );
            println!();
            runner.run_raw_signals(&mut feed, raw_signals, Some(profile))
        }
        _ => unreachable!(),
    };

    // 4. Print the full report
    println!("{result}");

    // 5. Print trade log details
    if !result.trade_log.is_empty() {
        println!();
        println!("═══ TRADE LOG ══════════════════════════════════════════════════");
        for (i, trade) in result.trade_log.iter().enumerate() {
            println!(
                "  #{:<3} {} {:<4} | entry={:.5} exit={:.5} size={:.2} pnl={:+.2} reason={:?} group={:?}",
                i + 1,
                trade.symbol,
                format!("{:?}", trade.side),
                trade.entry_price,
                trade.exit_price,
                trade.size,
                trade.pnl,
                trade.close_reason,
                trade.group,
            );
        }
    }

    // 6. Print position summaries
    if !result.positions.is_empty() {
        println!();
        println!("═══ POSITION SUMMARIES ═════════════════════════════════════════");
        for pos in &result.positions {
            println!(
                "  {} {:<4} | entry={:.5} avg_exit={:.5} closes={} net_pnl={:+.2} reasons={:?} group={:?}",
                pos.symbol,
                format!("{:?}", pos.side),
                pos.entry_price,
                pos.avg_exit_price,
                pos.close_count,
                pos.net_pnl,
                pos.close_reasons,
                pos.group,
            );
        }
    }

    // 7. Per-group breakdown (relevant for F14 modes with groups)
    if !result.per_group.is_empty() {
        println!();
        println!("═══ PER-GROUP BREAKDOWN ════════════════════════════════════════");
        for (group, stats) in &result.per_group {
            println!(
                "  {:<15} trades={:<4} pnl={:+.2} win_rate={:.1}% pf={:.2}",
                group,
                stats.total_trades,
                stats.total_pnl,
                stats.win_rate * 100.0,
                stats.profit_factor,
            );
        }
    }

    // 8. Per-close-reason breakdown
    if !result.per_close_reason.is_empty() {
        println!();
        println!("═══ CLOSE REASONS ═════════════════════════════════════════════");
        for cr in &result.per_close_reason {
            println!(
                "  {:<20} count={:<4} pnl={:+.2} avg={:+.2} ({:.1}%)",
                cr.reason, cr.count, cr.total_pnl, cr.avg_pnl, cr.percentage,
            );
        }
    }

    // 9. Quick verdict
    println!();
    if result.total_trades == 0 {
        println!("⚠  No trades were executed. Possible causes:");
        println!("   - Signals are outside the data time range");
        println!("   - The symbol name in signals doesn't match the data");
        println!("   - Data has no bid/ask (all filtered out)");
    } else {
        let emoji = if result.total_pnl >= 0.0 {
            "✅"
        } else {
            "❌"
        };
        println!(
            "{emoji} Backtest complete: {} trades, P&L={:+.2}, win_rate={:.1}%, max_dd={:.2}",
            result.total_trades,
            result.total_pnl,
            result.win_rate * 100.0,
            result.max_drawdown,
        );
    }
}
