use crate::models::{Bar, ImportResult, StatRow, Tick};

/// Print stats table to stdout.
pub fn print_stats(rows: &[StatRow], db_size: Option<u64>) {
    if rows.is_empty() {
        println!("No data found.");
        return;
    }

    // Column widths
    let w_ex = rows
        .iter()
        .map(|r| r.exchange.len())
        .max()
        .unwrap_or(8)
        .max(8);
    let w_sym = rows
        .iter()
        .map(|r| r.symbol.len())
        .max()
        .unwrap_or(6)
        .max(6);
    let w_type = rows
        .iter()
        .map(|r| r.data_type.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let w_count = 12;
    let w_ts = 19;

    let header = format!(
        " {:w_ex$} │ {:w_sym$} │ {:w_type$} │ {:>w_count$} │ {:w_ts$} │ {:w_ts$}",
        "Exchange", "Symbol", "Type", "Count", "From", "To",
    );
    let sep = format!(
        "─{:─>w_ex$}─┼─{:─>w_sym$}─┼─{:─>w_type$}─┼─{:─>w_count$}─┼─{:─>w_ts$}─┼─{:─>w_ts$}─",
        "", "", "", "", "", "",
    );

    println!();
    println!("{header}");
    println!("{sep}");

    for row in rows {
        let ts_min = row.ts_min.format("%Y-%m-%d %H:%M:%S").to_string();
        let ts_max = row.ts_max.format("%Y-%m-%d %H:%M:%S").to_string();
        println!(
            " {:w_ex$} │ {:w_sym$} │ {:w_type$} │ {:>w_count$} │ {:w_ts$} │ {:w_ts$}",
            row.exchange,
            row.symbol,
            row.data_type,
            format_count(row.count),
            ts_min,
            ts_max,
        );
    }

    // Summary footer
    let exchanges: std::collections::HashSet<&str> =
        rows.iter().map(|r| r.exchange.as_str()).collect();
    let symbols: std::collections::HashSet<&str> = rows.iter().map(|r| r.symbol.as_str()).collect();
    println!();
    print!(
        " Total: {} dataset(s), {} exchange(s), {} symbol(s)",
        rows.len(),
        exchanges.len(),
        symbols.len(),
    );
    if let Some(bytes) = db_size {
        print!("  │  Database size: {}", format_bytes(bytes));
    }
    println!();
}

/// Print tick query results to stdout.
pub fn print_ticks(exchange: &str, symbol: &str, ticks: &[Tick], total_count: u64) {
    println!(
        "\nExchange: {} │ Symbol: {} │ Ticks │ Showing {} of {}\n",
        exchange,
        symbol,
        ticks.len(),
        format_count(total_count),
    );

    if ticks.is_empty() {
        println!("  (no data)");
        return;
    }

    println!(
        " {:26} │ {:>12} │ {:>12} │ {:>12} │ {:>10} │ {:>5}",
        "Timestamp (UTC)", "Bid", "Ask", "Last", "Volume", "Flags",
    );
    println!(
        "─{:─>26}─┼─{:─>12}─┼─{:─>12}─┼─{:─>12}─┼─{:─>10}─┼─{:─>5}─",
        "", "", "", "", "", "",
    );

    for tick in ticks {
        let ts = tick.ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        println!(
            " {:26} │ {:>12} │ {:>12} │ {:>12} │ {:>10} │ {:>5}",
            ts,
            fmt_opt_f64(tick.bid),
            fmt_opt_f64(tick.ask),
            fmt_opt_f64(tick.last),
            fmt_opt_f64(tick.volume),
            fmt_opt_i32(tick.flags),
        );
    }
}

/// Print bar query results to stdout.
pub fn print_bars(exchange: &str, symbol: &str, tf: &str, bars: &[Bar], total_count: u64) {
    println!(
        "\nExchange: {} │ Symbol: {} │ Bars ({}) │ Showing {} of {}\n",
        exchange,
        symbol,
        tf,
        bars.len(),
        format_count(total_count),
    );

    if bars.is_empty() {
        println!("  (no data)");
        return;
    }

    println!(
        " {:19} │ {:>12} │ {:>12} │ {:>12} │ {:>12} │ {:>8} │ {:>8} │ {:>6}",
        "Timestamp (UTC)", "Open", "High", "Low", "Close", "TickVol", "Vol", "Spread",
    );
    println!(
        "─{:─>19}─┼─{:─>12}─┼─{:─>12}─┼─{:─>12}─┼─{:─>12}─┼─{:─>8}─┼─{:─>8}─┼─{:─>6}─",
        "", "", "", "", "", "", "", "",
    );

    for bar in bars {
        let ts = bar.ts.format("%Y-%m-%d %H:%M:%S").to_string();
        println!(
            " {:19} │ {:>12.2} │ {:>12.2} │ {:>12.2} │ {:>12.2} │ {:>8} │ {:>8} │ {:>6}",
            ts, bar.open, bar.high, bar.low, bar.close, bar.tick_vol, bar.volume, bar.spread,
        );
    }
}

/// Print import result summary.
pub fn print_import_result(result: &ImportResult) {
    let elapsed = if result.elapsed.as_secs() >= 1 {
        format!("{:.1}s", result.elapsed.as_secs_f64())
    } else {
        format!("{}ms", result.elapsed.as_millis())
    };

    println!("  Imported {}", result.file);
    println!(
        "  Exchange: {} │ Symbol: {}",
        result.exchange, result.symbol,
    );
    println!(
        "  Parsed: {} │ Inserted: {} │ Skipped (dup): {}",
        format_count(result.rows_parsed as u64),
        format_count(result.rows_inserted as u64),
        format_count(result.rows_skipped as u64),
    );
    println!("  Elapsed: {elapsed}");
}

/// Print delete result.
pub fn print_delete_result(data_type: &str, exchange: &str, detail: &str, count: usize) {
    println!(
        "Removed {} {} row(s) for {}/{}",
        format_count(count as u64),
        data_type,
        exchange,
        detail,
    );
}

/// Format a byte count as human-readable (e.g. "42.3 MB").
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Format a count with thousands separators.
fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn fmt_opt_f64(v: Option<f64>) -> String {
    v.map_or(String::new(), |f| format!("{f:.2}"))
}

fn fmt_opt_i32(v: Option<i32>) -> String {
    v.map_or(String::new(), |i| i.to_string())
}
