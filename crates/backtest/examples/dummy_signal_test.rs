//! Standalone smoke-test: load Parquet data, inject dummy signals, print results.
//!
//! # Usage
//!
//! ```bash
//! # Raw-signals mode with entry + management signals (default)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --symbols-path crates/symbols/symbols.toml \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16"
//!
//! # With a management profile
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --symbols-path crates/symbols/symbols.toml \
//!     --data-type tick \
//!     --from "2026-01-15" \
//!     --to "2026-01-16" \
//!     --profiles-path crates/backtest/profiles.toml \
//!     --profile aggressive
//!
//! # Bar data (1-minute)
//! cargo run -p qs-backtest --example dummy_signal_test -- \
//!     --data-dir /path/to/parquet/root \
//!     --exchange ctrader \
//!     --symbol eurusd \
//!     --symbols-path crates/symbols/symbols.toml \
//!     --data-type bar \
//!     --timeframe 1m \
//!     --from "2026-01-15" \
//!     --to "2026-01-16"
//! ```
//!
//! The example will:
//! 1. Load data from Parquet store
//! 2. Print a summary of loaded events (count, time range, price range)
//! 3. Generate dummy raw signals from the actual data (risk-multiplier entries + management)
//! 4. Size entries with a fixed-lot policy and run `run_raw_signals()` (with optional profile)
//! 5. Print the full BacktestResult report, including each executed final lot
//!
//! ## Modes
//!
//! - **raw-signals** (default): Generates `RawSignal` with entry signals plus
//!   management signals (modify SL, partial close, move SL to entry, scale-in,
//!   close all in group, etc.). Uses `run_raw_signals()`.
//! - **raw-signals-profile**: Same as raw-signals but entry signals are
//!   transformed through a management profile while management signals pass
//!   through untouched. Uses `run_raw_signals()` with a profile.

use chrono::NaiveDateTime;
use data_preprocess::{BarQueryOpts, ParquetStore, QueryOpts, Timeframe};
use qs_backtest::data_feed::{DataFeed, MarketEvent, VecFeed, bars_to_feed, ticks_to_feed};
use qs_backtest::profile::{PositionRef, ProfileRegistry, RawSignal};
use qs_backtest::runner::{BacktestConfig, BacktestRunner};
use qs_backtest::sizing::SizingPolicy;
use qs_core::types::{OrderType, Side};
use qs_symbols::SymbolRegistry;

use std::collections::HashMap;
use std::path::Path;
use std::process;

const ENTRY_FIXED_LOT: f64 = 0.10;

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
    mode: String, // "raw-signals" (default) or "raw-signals-profile"
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
                eprintln!("  --symbols-path <FILE>  Path to symbols.toml for sizing and P&L");
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

                eprintln!("  --balance <AMOUNT>     Initial balance (default: 10000)");
                eprintln!("  --mode <MODE>          Signal mode:");
                eprintln!(
                    "                           raw-signals         - entry + management signals (default)"
                );
                eprintln!(
                    "                           raw-signals-profile - raw-signals + profile for entries"
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

    if data_dir.is_empty() || exchange.is_empty() || symbol.is_empty() || symbols_path.is_none() {
        eprintln!("Error: --data-dir, --exchange, --symbol, and --symbols-path are required.");
        eprintln!("Run with --help for usage.");
        process::exit(1);
    }

    // Auto-detect mode if not explicitly set
    if mode.is_empty() {
        mode = "raw-signals".into();
    }

    // Validate mode
    match mode.as_str() {
        "raw-signals" | "raw-signals-profile" => {}
        other => {
            eprintln!(
                "Error: invalid --mode '{other}'. Must be one of: raw-signals, raw-signals-profile"
            );
            process::exit(1);
        }
    }

    // Validate mode + profile requirements
    if mode == "raw-signals-profile" && (profiles_path.is_none() || profile.is_none()) {
        eprintln!(
            "Error: --profiles-path and --profile are required for mode 'raw-signals-profile'."
        );
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
            if let Some(val) = name_str.strip_prefix(&prefix)
                && val.to_lowercase() == lower
            {
                return val.to_string();
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

// ── Dummy signal generation ──────────────────────────────────────────

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

// Generate a complete raw-signal lifecycle.

/// Generates a full signal stream with entry and management signals.
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
///  10. Modify stoploss for all on the symbol when supported, otherwise close the applicable scope.
///  11. Close all remaining positions (CloseAll)
fn generate_full_raw_signals(events: &[MarketEvent], symbol: &str) -> Vec<RawSignal> {
    let n = events.len();
    if n < 100 {
        eprintln!(
            "Warning: very few data points ({}), lifecycle signals may not all fire meaningfully",
            n
        );
    }

    let pip = estimate_pip(events[0].to_quote().ask);

    // Pick timestamps at various points through the data
    let idx_entry1 = n / 20; // About 5%, first BUY entry.
    let idx_modify_sl = n * 3 / 20; // About 15%, tighten SL.
    let idx_partial = n * 5 / 20; // About 25%, partial close.
    let idx_breakeven = idx_partial + 1; // Immediately after partial close.
    let idx_entry2 = n * 7 / 20; // About 35%, SELL entry.
    let idx_scale_in = n * 8 / 20; // About 40%, scale into SELL.
    let idx_add_rule = idx_scale_in + 1; // Immediately after scale-in.
    let idx_entry3 = n * 10 / 20; // About 50%, second BUY.
    let idx_close_group = n * 13 / 20; // About 65%, close group beta.
    let idx_close_all = n * 17 / 20; // About 85%, close everything.

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
        // 1. Open BUY in group "alpha" with trade_id "alpha-buy-1"
        RawSignal::Entry {
            ts: ev1.ts(),
            symbol: symbol.to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: Some(buy_sl),
            targets: vec![buy_tp1, buy_tp2, buy_tp3],
            group: Some("alpha".into()),
            trade_id: Some("alpha-buy-1".into()),
        },
        // 2. Tighten stoploss on trade "alpha-buy-1"
        RawSignal::ModifyStoploss {
            ts: ev_mod.ts(),
            position: PositionRef::ByTradeId {
                trade_id: "alpha-buy-1".into(),
            },
            price: tightened_sl,
        },
        // 3. Partial close 50% of trade "alpha-buy-1"
        RawSignal::ClosePartial {
            ts: ev_partial.ts(),
            position: PositionRef::ByTradeId {
                trade_id: "alpha-buy-1".into(),
            },
            ratio: 0.5,
        },
        // 4. Move stoploss to entry (breakeven) on "alpha-buy-1"
        RawSignal::MoveStoplossToEntry {
            ts: ev_be.ts(),
            position: PositionRef::ByTradeId {
                trade_id: "alpha-buy-1".into(),
            },
        },
        // 5. Open SELL in group "beta" with trade_id "beta-sell-1"
        RawSignal::Entry {
            ts: ev2.ts(),
            symbol: symbol.to_string(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 0.5,
            stoploss: Some(sell_sl),
            targets: vec![sell_tp1],
            group: Some("beta".into()),
            trade_id: Some("beta-sell-1".into()),
        },
        // 6. Scale into "beta-sell-1"
        RawSignal::ScaleIn {
            ts: ev_scale.ts(),
            position: PositionRef::ByTradeId {
                trade_id: "beta-sell-1".into(),
            },
            price: None,
            size: 0.03,
        },
        // 7. Add a trailing stop rule to "beta-sell-1"
        RawSignal::AddRule {
            ts: ev_rule.ts(),
            position: PositionRef::ByTradeId {
                trade_id: "beta-sell-1".into(),
            },
            rule: qs_backtest::profile::RuleConfigDef::TrailingStop {
                distance: 30.0 * pip,
            },
        },
        // 8. Open another BUY in group "alpha" with trade_id "alpha-buy-2"
        RawSignal::Entry {
            ts: ev3.ts(),
            symbol: symbol.to_string(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 0.8,
            stoploss: Some(buy3_sl),
            targets: vec![buy3_tp1, buy3_tp2],
            group: Some("alpha".into()),
            trade_id: Some("alpha-buy-2".into()),
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

    println!("Generated raw signal stream ({} signals):", signals.len());
    for (i, sig) in signals.iter().enumerate() {
        let desc = match sig {
            RawSignal::Entry {
                side,
                risk_multiplier,
                group,
                ..
            } => {
                format!(
                    "Entry {:?} risk_multiplier={:.2} group={:?}",
                    side, risk_multiplier, group
                )
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

    // 2. Load symbol metadata for entry sizing and P&L calculation.
    let symbols_path = args.symbols_path.as_ref().unwrap();
    let registry = SymbolRegistry::load(symbols_path).unwrap_or_else(|e| {
        eprintln!("Error loading symbol registry from '{symbols_path}': {e}");
        process::exit(1);
    });
    let canonical = registry.normalize_or_passthrough(&data_symbol);
    let spec = registry.spec(&canonical).cloned().unwrap_or_else(|| {
        eprintln!("Error: symbol '{data_symbol}' not found in '{symbols_path}'");
        process::exit(1);
    });
    let economics = qs_backtest::resolve_legacy_economics(&spec).unwrap_or_else(|error| {
        eprintln!("Error: symbol '{data_symbol}' is not economically supported: {error}");
        process::exit(1);
    });
    let mut contract_sizes = HashMap::from([(data_symbol.clone(), economics.contract_multiplier)]);
    let mut symbol_specs = HashMap::from([(data_symbol.clone(), spec.clone())]);
    if canonical != data_symbol {
        contract_sizes.insert(canonical.clone(), economics.contract_multiplier);
        symbol_specs.insert(canonical.clone(), spec.clone());
    }
    println!(
        "Symbol metadata for {}: contract_size={} lot_step={} (from {})",
        data_symbol,
        economics.contract_multiplier,
        spec.lot_step_units as f64 / spec.lot_base_units as f64,
        symbols_path
    );
    println!(
        "Entry sizing: fixed_lot={ENTRY_FIXED_LOT:.2}; final lot is computed from the signal risk multiplier"
    );

    // 3. Build config
    let config = BacktestConfig {
        initial_balance: args.initial_balance,
        close_on_finish: true,
        contract_sizes,
        sizing: Some(SizingPolicy::FixedLot {
            lots: ENTRY_FIXED_LOT,
        }),
        symbol_specs,
        ..Default::default()
    };

    // 3. Run backtest based on mode
    let result = match args.mode.as_str() {
        "raw-signals" => {
            // RawSignal stream -> run_raw_signals
            let raw_signals = generate_full_raw_signals(&events, &data_symbol);
            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!(
                "Running backtest (raw-signals mode, {} signals)...",
                raw_signals.len()
            );
            println!();
            runner.run_raw_signals(&mut feed, raw_signals, None)
        }
        "raw-signals-profile" => {
            // Entry signals -> profile transform, management pass through
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
            let raw_signals = generate_full_raw_signals(&events, &data_symbol);
            let mut feed = VecFeed::new(events);
            let runner = BacktestRunner::new(config);
            println!(
                "Running backtest (raw-signals + profile '{}', {} signals)...",
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
                "  #{:<3} {} {:<4} | entry={:.5} exit={:.5} final_lot={:.2} pnl={:+.2} reason={:?} group={:?}",
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

    // 7. Per-group breakdown for modes that use groups.
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
