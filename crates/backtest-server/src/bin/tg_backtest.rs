//! Telegram signal backtest client — loads parsed JSONL and submits to backtest server.
//!
//! Reads pre-parsed raw signal JSONL (RawSignalMsg format) produced by the
//! `parse_signals` binary, connects to the backtest server over SHM, and
//! prints the backtest results.

use std::io::{self, BufRead};
use std::sync::Arc;

use chrono::NaiveDateTime;
use clap::Parser;

use backtest_server::rpc_types::*;
use xrpc::{
    JsonCodec, MessageChannelAdapter, RpcClient, SharedMemoryConfig, SharedMemoryFrameTransport,
};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "tg_backtest",
    about = "Load parsed Telegram signal JSONL and run backtest via SHM server"
)]
struct Args {
    /// Path to parsed signals JSONL file (use "-" for stdin).
    #[arg(short, long)]
    input: String,

    /// Shared memory base name (must match server config).
    #[arg(long, default_value = "backtest")]
    shm_name: String,

    /// Single symbol to backtest (e.g. EURUSD, XAUUSD).
    #[arg(long)]
    symbol: Option<String>,

    /// Comma-separated symbols to backtest as one portfolio (e.g. XAUUSD,GBPJPY).
    #[arg(long)]
    symbols: Option<String>,

    /// Derive all backtest symbols from Entry signals in the parsed JSONL.
    #[arg(long, default_value_t = false)]
    all_symbols: bool,

    /// Exchange / data source name (e.g. icmarkets, oanda).
    #[arg(long)]
    exchange: String,

    /// Data type: "tick" or "bar".
    #[arg(long, default_value = "tick")]
    data_type: String,

    /// Timeframe for bar data (e.g. "1m", "1h"). Required when data-type is "bar".
    #[arg(long)]
    timeframe: Option<String>,

    /// Start date filter (ISO date, e.g. "2024-01-01").
    #[arg(long)]
    from: Option<String>,

    /// End date filter (ISO date, e.g. "2024-12-31").
    #[arg(long)]
    to: Option<String>,

    /// Named management profile to apply (must exist on server).
    #[arg(long)]
    profile: Option<String>,

    /// Initial account balance.
    #[arg(long, default_value_t = 10_000.0)]
    balance: f64,

    /// Write full result JSON to this file.
    #[arg(long)]
    output: Option<String>,

    /// Use async job submission mode (Issue 2).
    /// Submits job, polls status, then fetches result.
    #[arg(long, default_value_t = false)]
    r#async: bool,

    /// Risk per trade in account currency (Issue 3).  When set, uses
    /// RRValue sizing policy with the given risk amount.
    #[arg(long)]
    risk_per_trade: Option<f64>,
}

// ── Connection ──────────────────────────────────────────────────────────────

/// Connect via the SHM acceptor pattern and return an RPC client on a dedicated slot.
async fn connect(
    shm_name: &str,
) -> Result<
    RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport, JsonCodec>, JsonCodec>,
    Box<dyn std::error::Error>,
> {
    let accept_name = format!("{}-accept", shm_name);
    eprintln!("[connect] acceptor shm://{}", accept_name);

    // Handshake on the well-known acceptor endpoint.
    let acceptor_transport = SharedMemoryFrameTransport::connect_client_with_config(
        &accept_name,
        SharedMemoryConfig::default()
            .with_read_timeout(std::time::Duration::from_secs(30))
            .with_write_timeout(std::time::Duration::from_secs(10)),
    )?;
    let acceptor_channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(acceptor_transport);
    let acceptor_client = RpcClient::with_codec(acceptor_channel, JsonCodec);
    let _handle = acceptor_client.start();

    let resp: ConnectResponse = acceptor_client
        .call(
            "connect",
            &ConnectRequest {
                client_name: "tg-backtest".into(),
            },
        )
        .await?;

    eprintln!(
        "[connect] assigned client_id={}, slot=shm://{}",
        resp.client_id, resp.slot_name
    );

    // Brief pause for the server to create the dedicated SHM slot.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    acceptor_client.close().await?;

    // Reconnect on the dedicated per-client slot.
    let transport = SharedMemoryFrameTransport::connect_client_with_config(
        &resp.slot_name,
        SharedMemoryConfig::default()
            .with_buffer_size(16 * 1024 * 1024)
            .with_read_timeout(std::time::Duration::from_secs(300))
            .with_write_timeout(std::time::Duration::from_secs(30)),
    )?;
    let channel = MessageChannelAdapter::<_, JsonCodec>::with_codec(transport);
    let client = RpcClient::with_codec(channel, JsonCodec);
    let _handle = client.start();

    Ok(client)
}

// ── Signal Loading ──────────────────────────────────────────────────────────

/// Read parsed raw signal JSONL from a file or stdin.
fn load_raw_signals(path: &str) -> Result<Vec<RawSignalMsg>, Box<dyn std::error::Error>> {
    let reader: Box<dyn BufRead> = if path == "-" {
        Box::new(io::BufReader::new(io::stdin()))
    } else {
        let file = std::fs::File::open(path)?;
        Box::new(io::BufReader::new(file))
    };

    let mut signals = Vec::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: RawSignalMsg = serde_json::from_str(trimmed)
            .map_err(|e| format!("line {}: failed to parse raw signal: {}", lineno + 1, e))?;
        signals.push(msg);
    }
    Ok(signals)
}

/// Filter raw signal messages to only those within the requested date range.
///
/// This is a client-side optimisation that reduces request payload size.
/// The server also applies authoritative filtering, so correctness does not
/// depend on this function.
fn filter_signals_by_date(
    signals: Vec<RawSignalMsg>,
    from: &Option<String>,
    to: &Option<String>,
) -> Vec<RawSignalMsg> {
    if from.is_none() && to.is_none() {
        return signals;
    }
    let parse_ts = |s: &str| -> Option<NaiveDateTime> {
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ];
        for fmt in &formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Some(dt);
            }
        }
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
    };
    let from_dt = from.as_deref().and_then(parse_ts);
    let to_dt = to.as_deref().and_then(parse_ts);
    signals
        .into_iter()
        .filter(|s| {
            let Some(ts) = parse_ts(s.ts()) else {
                return true; // keep signals with unparseable timestamps
            };
            let after_from = from_dt.map_or(true, |f| ts >= f);
            let before_to = to_dt.map_or(true, |t| ts <= t);
            after_from && before_to
        })
        .collect()
}

fn parse_symbols_arg(value: Option<&str>) -> Vec<String> {
    value
        .into_iter()
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn resolve_cli_symbol_request(
    symbol: &Option<String>,
    symbols: &Option<String>,
    all_symbols: bool,
) -> Result<(String, Vec<String>, String), Box<dyn std::error::Error>> {
    let parsed_symbols = parse_symbols_arg(symbols.as_deref());
    let mode_count = usize::from(symbol.as_ref().is_some_and(|s| !s.trim().is_empty()))
        + usize::from(!parsed_symbols.is_empty())
        + usize::from(all_symbols);

    if mode_count != 1 {
        return Err(
            "provide exactly one of --symbol <SYMBOL>, --symbols <A,B>, or --all-symbols".into(),
        );
    }

    if all_symbols {
        return Ok((
            String::new(),
            Vec::new(),
            "all symbols from entries".to_string(),
        ));
    }

    if !parsed_symbols.is_empty() {
        return Ok((
            String::new(),
            parsed_symbols.clone(),
            parsed_symbols.join(","),
        ));
    }

    let single = symbol.as_ref().unwrap().trim().to_string();
    Ok((single.clone(), Vec::new(), single))
}

// ── Display Helpers ─────────────────────────────────────────────────────────

fn print_header(title: &str) {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  {:<59}║", title);
    println!("╚══════════════════════════════════════════════════════════════╝");
}

fn print_section(title: &str) {
    println!();
    println!("── {} ──────────────────────────────────────────", title);
}

fn print_result_summary(result: &BacktestResultMsg) {
    print_section("Performance Summary");
    println!("  Initial Balance:   ${:>12.2}", result.initial_balance);
    println!("  Final Balance:     ${:>12.2}", result.final_balance);
    println!(
        "  Total PnL:         ${:>12.2}  ({:>+.2}%)",
        result.total_pnl,
        if result.initial_balance != 0.0 {
            (result.total_pnl / result.initial_balance) * 100.0
        } else {
            0.0
        }
    );
    println!();
    println!("  Total Trades:      {:>6}", result.total_trades);
    println!("  Winning:           {:>6}", result.winning_trades);
    println!("  Losing:            {:>6}", result.losing_trades);
    println!("  Win Rate:          {:>6.1}%", result.win_rate * 100.0);
    println!("  Profit Factor:     {:>9.2}", result.profit_factor);
    println!();
    println!(
        "  Max Drawdown:      ${:>12.2}  ({:.2}%)",
        result.max_drawdown, result.max_drawdown_pct
    );

    // Position-level stats.
    print_section("Position Summary");
    println!("  Total Positions:   {:>6}", result.total_positions);
    println!("  Winning:           {:>6}", result.winning_positions);
    println!("  Losing:            {:>6}", result.losing_positions);
    println!(
        "  Position Win Rate: {:>6.1}%",
        result.position_win_rate * 100.0
    );

    // Risk metrics.
    let rm = &result.risk_metrics;
    print_section("Risk Metrics");
    if let Some(sharpe) = rm.sharpe_ratio {
        println!("  Sharpe Ratio:      {:>9.3}", sharpe);
    }
    if let Some(sortino) = rm.sortino_ratio {
        println!("  Sortino Ratio:     {:>9.3}", sortino);
    }
    if let Some(calmar) = rm.calmar_ratio {
        println!("  Calmar Ratio:      {:>9.3}", calmar);
    }

    // Streak stats.
    let st = &result.streaks;
    print_section("Streak Stats");
    println!("  Max Consec. Wins:  {:>6}", st.max_consecutive_wins);
    println!("  Max Consec. Losses:{:>6}", st.max_consecutive_losses);
    println!("  Current Streak:    {:>6}", st.current_streak);

    // Long / Short breakdown.
    print_section("Long vs Short");
    println!(
        "  Long  — trades: {}, pnl: ${:.2}, win rate: {:.1}%",
        result.long_stats.total_trades,
        result.long_stats.total_pnl,
        result.long_stats.win_rate * 100.0
    );
    println!(
        "  Short — trades: {}, pnl: ${:.2}, win rate: {:.1}%",
        result.short_stats.total_trades,
        result.short_stats.total_pnl,
        result.short_stats.win_rate * 100.0
    );

    // Close reason breakdown.
    if !result.per_close_reason.is_empty() {
        print_section("Close Reasons");
        for cr in &result.per_close_reason {
            println!(
                "  {:<20} count={:<4} pnl=${:<10.2} avg=${:<10.2} ({:.1}%)",
                cr.reason, cr.count, cr.total_pnl, cr.avg_pnl, cr.percentage
            );
        }
    }

    // Per-group breakdown.
    if !result.per_group.is_empty() {
        print_section("Per-Group");
        for (group, stats) in &result.per_group {
            println!(
                "  {:<24} trades={:<4} pnl=${:<10.2} win_rate={:.1}%",
                group,
                stats.total_trades,
                stats.total_pnl,
                stats.win_rate * 100.0
            );
        }
    }

    // Monthly returns (first 12).
    if !result.monthly_returns.is_empty() {
        print_section("Monthly Returns");
        let limit = result.monthly_returns.len().min(12);
        for mr in &result.monthly_returns[..limit] {
            println!(
                "  {}-{:02}:  pnl=${:>10.2}  trades={:<4} balance=${:.2}",
                mr.year, mr.month, mr.pnl, mr.trade_count, mr.ending_balance
            );
        }
        if result.monthly_returns.len() > limit {
            println!(
                "  ... and {} more months",
                result.monthly_returns.len() - limit
            );
        }
    }
}

fn print_trade_log(trades: &[TradeResultMsg], max: usize) {
    print_section("Trade Log");
    if trades.is_empty() {
        println!("  (no trades)");
        return;
    }
    let show = trades.len().min(max);
    println!(
        "  {:<12} {:<8} {:<6} {:>12} {:>12} {:>8} {:>12} {:<15}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "EXIT", "SIZE", "PNL", "CLOSE_REASON"
    );
    println!("  {}", "-".repeat(100));
    for t in &trades[..show] {
        let pnl_marker = if t.pnl >= 0.0 { "+" } else { "" };
        println!(
            "  {:<12} {:<8} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>4}{:<8.2} {:<15}",
            &t.position_id[..t.position_id.len().min(12)],
            t.symbol,
            t.side,
            t.entry_price,
            t.exit_price,
            t.size,
            pnl_marker,
            t.pnl,
            t.close_reason,
        );
    }
    if trades.len() > show {
        println!("  ... and {} more trades", trades.len() - show);
    }
}

fn print_positions(positions: &[PositionSummaryMsg], max: usize) {
    print_section("Position Summaries");
    if positions.is_empty() {
        println!("  (no positions)");
        return;
    }
    let show = positions.len().min(max);
    println!(
        "  {:<12} {:<8} {:<6} {:>12} {:>12} {:>8} {:>12} {:<20}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "AVG_EXIT", "SIZE", "NET_PNL", "CLOSE_REASONS"
    );
    println!("  {}", "-".repeat(105));
    for p in &positions[..show] {
        let pnl_marker = if p.net_pnl >= 0.0 { "+" } else { "" };
        let reasons = p.close_reasons.join(",");
        println!(
            "  {:<12} {:<8} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>4}{:<8.2} {:<20}",
            &p.position_id[..p.position_id.len().min(12)],
            p.symbol,
            p.side,
            p.entry_price,
            p.avg_exit_price,
            p.original_size,
            pnl_marker,
            p.net_pnl,
            reasons,
        );
    }
    if positions.len() > show {
        println!("  ... and {} more positions", positions.len() - show);
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 1. Load parsed signals from JSONL.
    print_header("Loading Parsed Signals");
    let mut raw_signals = load_raw_signals(&args.input)?;
    println!(
        "  Loaded {} raw signals from {}",
        raw_signals.len(),
        args.input
    );

    if raw_signals.is_empty() {
        eprintln!("  No signals to backtest — exiting.");
        return Ok(());
    }

    // 1b. Client-side date filtering (reduces payload; server also filters).
    raw_signals = filter_signals_by_date(raw_signals, &args.from, &args.to);
    println!("  After date filtering: {} signals", raw_signals.len());
    if raw_signals.is_empty() {
        eprintln!("  No signals in date range — exiting.");
        return Ok(());
    }

    let (request_symbol, request_symbols, symbol_label) =
        resolve_cli_symbol_request(&args.symbol, &args.symbols, args.all_symbols)?;

    // Show first few signals as preview.
    let preview = raw_signals.len().min(5);
    for s in &raw_signals[..preview] {
        println!("    {:?}", s);
    }
    if raw_signals.len() > preview {
        println!("    ... and {} more", raw_signals.len() - preview);
    }

    // 2. Connect to backtest server via SHM.
    print_header("Connecting to Backtest Server");
    let client = connect(&args.shm_name).await?;
    let client = Arc::new(client);
    println!("  ✓ Connected");

    // 3. Ping server to confirm it's alive.
    let ping: PingResponse = client.call("ping", &()).await?;
    println!(
        "  Server status: {}, uptime: {}s",
        ping.status, ping.uptime_secs
    );

    // 4. Build and submit the backtest request.
    print_header("Running Backtest");
    println!(
        "  Symbols: {}, Exchange: {}, DataType: {}, Timeframe: {:?}",
        symbol_label, args.exchange, args.data_type, args.timeframe
    );
    println!("  Date range: {:?} → {:?}", args.from, args.to);
    println!(
        "  Profile: {:?}, Balance: ${:.2}, Raw signals: {}",
        args.profile,
        args.balance,
        raw_signals.len()
    );

    let sizing = args.risk_per_trade.map(|v| SizingPolicyMsg::RRValue {
        qty: "all=0.01".into(),
        value: v,
    });

    let request = RunBacktestRequest {
        symbol: request_symbol,
        symbols: request_symbols,
        all_symbols: args.all_symbols,
        exchange: args.exchange.clone(),
        data_type: args.data_type.clone(),
        timeframe: args.timeframe.clone(),
        from: args.from.clone(),
        to: args.to.clone(),
        raw_signals,
        profile: args.profile.clone(),
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: Some(args.balance),
            close_on_finish: Some(true),
            fill_model: Some("BidAsk".into()),
            sizing,
        },
    };

    if args.r#async {
        // Async mode: submit job, poll status, fetch result.
        let submit: SubmitBacktestResponse = client
            .call(
                "submit_backtest",
                &SubmitBacktestRequest {
                    request: request.clone(),
                },
            )
            .await?;
        if !submit.success || submit.job_id.is_none() {
            eprintln!(
                "  ✗ Failed to submit job: {}",
                submit.error.as_deref().unwrap_or("unknown error")
            );
            client.close().await?;
            return Ok(());
        }
        let job_id = submit.job_id.unwrap();
        println!("  Job submitted: {}", job_id);

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let status: BacktestStatusResponse = client
                .call(
                    "get_backtest_status",
                    &GetBacktestStatusRequest {
                        job_id: job_id.clone(),
                    },
                )
                .await?;
            println!("  Status: {}", status.status);
            if status.status == "Completed"
                || status.status == "Failed"
                || status.status == "Cancelled"
            {
                break;
            }
        }

        let result_resp: GetBacktestResultResponse = client
            .call(
                "get_backtest_result",
                &GetBacktestResultRequest {
                    job_id: job_id.clone(),
                },
            )
            .await?;
        if result_resp.success {
            if let Some(ref result) = result_resp.result {
                print_result_summary(result);
                print_trade_log(&result.trade_log, 30);
                print_positions(&result.positions, 15);
                if let Some(ref output_path) = args.output {
                    let json = serde_json::to_string_pretty(result)?;
                    std::fs::write(output_path, json)?;
                    println!();
                    println!("  Full result written to {}", output_path);
                }
            }
        } else {
            eprintln!(
                "  ✗ Job failed: {}",
                result_resp.error.as_deref().unwrap_or("unknown error")
            );
        }
    } else {
        let resp: RunBacktestResponse = client
            .call_with_timeout(
                "run_backtest",
                &request,
                std::time::Duration::from_secs(300),
            )
            .await?;
        println!("  Elapsed: {}ms", resp.elapsed_ms);

        if resp.success {
            if let Some(ref result) = resp.result {
                print_result_summary(result);
                print_trade_log(&result.trade_log, 30);
                print_positions(&result.positions, 15);

                if let Some(ref output_path) = args.output {
                    let json = serde_json::to_string_pretty(result)?;
                    std::fs::write(output_path, json)?;
                    println!();
                    println!("  Full result written to {}", output_path);
                }
            }
        } else {
            eprintln!(
                "  ✗ Backtest failed: {}",
                resp.error.as_deref().unwrap_or("unknown error")
            );
        }
    }

    // 6. Disconnect.
    print_header("Done");
    client.close().await?;
    println!("  ✓ Disconnected");

    Ok(())
}
