//! Backtest server client example.
//!
//! Connects to the backtest server over shared memory, demonstrates the full
//! client workflow: handshake → ping → list profiles → list symbols → run
//! backtest with legacy entry signals → run backtest with F14 raw signals.
//!
//! # Usage
//!
//! ```bash
//! # Start the backtest server first, then:
//! cargo run -p qs-backtest-server --example backtest_client
//!
//! # With custom options:
//! cargo run -p qs-backtest-server --example backtest_client -- \
//!     --shm-name backtest \
//!     --symbol EURUSD \
//!     --exchange oanda \
//!     --data-type tick \
//!     --balance 50000 \
//!     --profile default
//!
//! # With date range filter:
//! cargo run -p qs-backtest-server --example backtest_client -- \
//!     --symbol XAUUSD \
//!     --exchange oanda \
//!     --data-type bar \
//!     --timeframe 1h \
//!     --from 2024-01-01 \
//!     --to 2024-06-01
//! ```

use std::sync::Arc;

use clap::Parser;

use backtest_server::rpc_types::*;
use xrpc::{MessageChannelAdapter, RpcClient, SharedMemoryFrameTransport};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "backtest-client",
    about = "Example client for the backtest RPC server over shared memory"
)]
struct Args {
    /// Shared memory base name (must match server config).
    #[arg(long, default_value = "backtest")]
    shm_name: String,

    /// Symbol to backtest (e.g. EURUSD, XAUUSD).
    #[arg(long, default_value = "EURUSD")]
    symbol: String,

    /// Exchange / data source (e.g. oanda, binance).
    #[arg(long, default_value = "icmarkets")]
    exchange: String,

    /// Data type: "tick" or "bar".
    #[arg(long, default_value = "tick")]
    data_type: String,

    /// Timeframe for bar data (e.g. "1m", "5m", "1h"). Required when data-type is "bar".
    #[arg(long)]
    timeframe: Option<String>,

    /// Start date filter (ISO date, e.g. "2024-01-01").
    #[arg(long)]
    from: Option<String>,

    /// End date filter (ISO date, e.g. "2024-12-31").
    #[arg(long)]
    to: Option<String>,

    /// Named management profile to use (must exist on server).
    #[arg(long)]
    profile: Option<String>,

    /// Initial account balance.
    #[arg(long, default_value_t = 10_000.0)]
    balance: f64,
}

// ── Connection Helper ───────────────────────────────────────────────────────

/// Connect to the acceptor endpoint, receive a dedicated slot, and return an
/// `RpcClient` bound to that slot.
async fn connect(
    shm_name: &str,
    client_name: &str,
) -> Result<
    (
        RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport>>,
        ConnectResponse,
    ),
    Box<dyn std::error::Error>,
> {
    let accept_name = format!("{}-accept", shm_name);
    println!("  Connecting to acceptor shm://{} ...", accept_name);

    // Step 1: Connect to the well-known acceptor endpoint.
    let acceptor_transport = SharedMemoryFrameTransport::connect_client(&accept_name)?;
    let acceptor_channel = MessageChannelAdapter::new(acceptor_transport);
    let acceptor_client = RpcClient::new(acceptor_channel);
    let _handle = acceptor_client.start();

    let resp: ConnectResponse = acceptor_client
        .call(
            "connect",
            &ConnectRequest {
                client_name: client_name.into(),
            },
        )
        .await?;

    println!(
        "  Assigned client_id={}, slot=shm://{}",
        resp.client_id, resp.slot_name
    );

    // Give the server a moment to create the dedicated SHM slot.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Close the acceptor connection — we no longer need it.
    acceptor_client.close().await?;

    // Step 2: Connect to the dedicated per-client slot.
    let transport = SharedMemoryFrameTransport::connect_client(&resp.slot_name)?;
    let channel = MessageChannelAdapter::new(transport);
    let client = RpcClient::new(channel);
    let _handle = client.start();

    Ok((client, resp))
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
        (result.total_pnl / result.initial_balance) * 100.0
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

    // Position-level stats
    print_section("Position Summary");
    println!("  Total Positions:   {:>6}", result.total_positions);
    println!("  Winning:           {:>6}", result.winning_positions);
    println!("  Losing:            {:>6}", result.losing_positions);
    println!(
        "  Position Win Rate: {:>6.1}%",
        result.position_win_rate * 100.0
    );

    // Risk metrics
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

    // Streak stats
    let st = &result.streaks;
    print_section("Streak Stats");
    println!("  Max Consec. Wins:  {:>6}", st.max_consecutive_wins);
    println!("  Max Consec. Losses:{:>6}", st.max_consecutive_losses);
    println!("  Current Streak:    {:>6}", st.current_streak);

    // Long / Short breakdown
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

    // Close reason breakdown
    if !result.per_close_reason.is_empty() {
        print_section("Close Reasons");
        for cr in &result.per_close_reason {
            println!(
                "  {:<20} count={:<4} pnl=${:<10.2} avg=${:<10.2} ({:.1}%)",
                cr.reason, cr.count, cr.total_pnl, cr.avg_pnl, cr.percentage
            );
        }
    }

    // Monthly returns (first 6)
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
        "  {:<12} {:<6} {:<6} {:>12} {:>12} {:>8} {:>12} {:<15}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "EXIT", "SIZE", "PNL", "CLOSE_REASON"
    );
    println!("  {}", "-".repeat(100));
    for t in &trades[..show] {
        let pnl_marker = if t.pnl >= 0.0 { "+" } else { "" };
        println!(
            "  {:<12} {:<6} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>4}{:<8.2} {:<15}",
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
        "  {:<12} {:<6} {:<6} {:>12} {:>12} {:>8} {:>12} {:<20}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "AVG_EXIT", "SIZE", "NET_PNL", "CLOSE_REASONS"
    );
    println!("  {}", "-".repeat(105));
    for p in &positions[..show] {
        let pnl_marker = if p.net_pnl >= 0.0 { "+" } else { "" };
        let reasons = p.close_reasons.join(",");
        println!(
            "  {:<12} {:<6} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>4}{:<8.2} {:<20}",
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

// ── Dummy Signal Generators ─────────────────────────────────────────────────

/// Generate dummy legacy entry signals (`RawSignalEntryMsg`).
///
/// These use hardcoded timestamps and prices. In a real scenario you would
/// generate signals from your strategy logic; the server has the market data
/// and will match fills against it.
fn generate_legacy_signals(symbol: &str) -> Vec<RawSignalEntryMsg> {
    vec![
        RawSignalEntryMsg {
            ts: "2024-01-02T10:00:00Z".into(),
            symbol: symbol.into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.1050, 1.1100],
            group: Some("demo-group-A".into()),
        },
        RawSignalEntryMsg {
            ts: "2024-01-05T14:30:00Z".into(),
            symbol: symbol.into(),
            side: "Sell".into(),
            order_type: "Market".into(),
            price: None,
            size: 0.5,
            stoploss: Some(1.1200),
            targets: vec![1.0950, 1.0900],
            group: Some("demo-group-A".into()),
        },
        RawSignalEntryMsg {
            ts: "2024-01-10T09:15:00Z".into(),
            symbol: symbol.into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 2.0,
            stoploss: Some(1.0750),
            targets: vec![1.1000, 1.1050, 1.1100],
            group: Some("demo-group-B".into()),
        },
        RawSignalEntryMsg {
            ts: "2024-01-15T16:00:00Z".into(),
            symbol: symbol.into(),
            side: "Sell".into(),
            order_type: "Market".into(),
            price: None,
            size: 1.0,
            stoploss: Some(1.1150),
            targets: vec![1.0850],
            group: None,
        },
    ]
}

/// Generate F14 raw signals (`RawSignalMsg`) demonstrating the full signal
/// action vocabulary: entries, management actions (modify SL, partial close,
/// move SL to breakeven), and bulk operations.
///
/// This showcases the key advantage of F14: you can interleave entry and
/// management signals in the same stream, giving your strategy full control
/// over position lifecycle without relying on server-side profiles.
fn generate_f14_signals(symbol: &str) -> Vec<RawSignalMsg> {
    vec![
        // 1. Open a long position with stoploss and targets.
        RawSignalMsg::Entry {
            ts: "2024-02-01T09:00:00Z".into(),
            symbol: symbol.into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.1050, 1.1100],
            group: Some("f14-demo".into()),
        },
        // 2. After some time, tighten the stoploss on the last opened position.
        RawSignalMsg::ModifyStoploss {
            ts: "2024-02-01T12:00:00Z".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: symbol.into(),
            },
            price: 1.0850,
        },
        // 3. Take partial profits — close 50% of the position.
        RawSignalMsg::ClosePartial {
            ts: "2024-02-02T10:00:00Z".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: symbol.into(),
            },
            ratio: 0.5,
        },
        // 4. Move stoploss to entry (breakeven) for the remainder.
        RawSignalMsg::MoveStoplossToEntry {
            ts: "2024-02-02T10:01:00Z".into(),
            position: PositionRefMsg::LastOnSymbol {
                symbol: symbol.into(),
            },
        },
        // 5. Open a second position (short) in a different group.
        RawSignalMsg::Entry {
            ts: "2024-02-05T14:00:00Z".into(),
            symbol: symbol.into(),
            side: "Sell".into(),
            order_type: "Market".into(),
            price: None,
            size: 0.75,
            stoploss: Some(1.1150),
            targets: vec![1.0900],
            group: Some("f14-hedge".into()),
        },
        // 6. Scale into the short position with additional size.
        RawSignalMsg::ScaleIn {
            ts: "2024-02-06T09:30:00Z".into(),
            position: PositionRefMsg::LastInGroup {
                group_id: "f14-hedge".into(),
            },
            price: None,
            size: 0.25,
        },
        // 7. Add a trailing stop rule to the short position.
        RawSignalMsg::AddRule {
            ts: "2024-02-06T09:31:00Z".into(),
            position: PositionRefMsg::LastInGroup {
                group_id: "f14-hedge".into(),
            },
            rule: RuleConfigDefMsg::TrailingStop { distance: 0.0050 },
        },
        // 8. Open a third position.
        RawSignalMsg::Entry {
            ts: "2024-02-10T08:00:00Z".into(),
            symbol: symbol.into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            size: 1.5,
            stoploss: Some(1.0700),
            targets: vec![1.1000, 1.1050],
            group: Some("f14-demo".into()),
        },
        // 9. Close all positions in the hedge group.
        RawSignalMsg::CloseAllInGroup {
            ts: "2024-02-12T16:00:00Z".into(),
            group_id: "f14-hedge".into(),
        },
        // 10. Modify stoploss for all remaining positions on the symbol.
        RawSignalMsg::ModifyAllStoploss {
            ts: "2024-02-14T10:00:00Z".into(),
            symbol: symbol.into(),
            price: 1.0900,
        },
        // 11. Close everything at the end.
        RawSignalMsg::CloseAll {
            ts: "2024-02-15T17:00:00Z".into(),
        },
    ]
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // ── 1. Connect ──────────────────────────────────────────────────────
    print_header("Connecting to Backtest Server");
    let (client, _info) = connect(&args.shm_name, "example-backtest-client").await?;
    let client = Arc::new(client);
    println!("  ✓ Connected successfully");

    // ── 2. Ping ─────────────────────────────────────────────────────────
    print_header("Ping");
    let ping: PingResponse = client.call("ping", &()).await?;
    println!("  Status:   {}", ping.status);
    println!("  Uptime:   {}s", ping.uptime_secs);
    println!("  Data Dir: {}", ping.data_dir);

    // ── 3. List Profiles ────────────────────────────────────────────────
    print_header("Available Profiles");
    let profiles_resp: ListProfilesResponse = client.call("list_profiles", &()).await?;
    if profiles_resp.profiles.is_empty() {
        println!("  (no profiles loaded on server)");
    } else {
        println!(
            "  {:<20} {:<15} {:<15} {:<12} {:<6}",
            "NAME", "TARGETS", "RATIOS", "SL_MODE", "RULES"
        );
        println!("  {}", "-".repeat(70));
        for p in &profiles_resp.profiles {
            println!(
                "  {:<20} {:<15} {:<15} {:<12} {:<6}",
                p.name,
                format!("{:?}", p.use_targets),
                format!("{:?}", p.close_ratios),
                p.stoploss_mode,
                p.rules_count,
            );
        }
    }

    // ── 4. List Symbols ─────────────────────────────────────────────────
    print_header("Available Data");
    let symbols_resp: ListSymbolsResponse = client
        .call(
            "list_symbols",
            &ListSymbolsRequest {
                exchange: Some(args.exchange.clone()),
                data_type: None,
            },
        )
        .await?;

    if symbols_resp.symbols.is_empty() {
        println!("  (no data found for exchange '{}')", args.exchange);
    } else {
        let show_count = symbols_resp.symbols.len().min(20);
        println!(
            "  Found {} datasets (showing first {}):",
            symbols_resp.symbols.len(),
            show_count
        );
        println!(
            "  {:<10} {:<10} {:<6} {:<6} {:>10} {:<22} {:<22}",
            "EXCHANGE", "SYMBOL", "TYPE", "TF", "ROWS", "EARLIEST", "LATEST"
        );
        println!("  {}", "-".repeat(90));
        for s in &symbols_resp.symbols[..show_count] {
            println!(
                "  {:<10} {:<10} {:<6} {:<6} {:>10} {:<22} {:<22}",
                s.exchange,
                s.symbol,
                s.data_type,
                s.timeframe.as_deref().unwrap_or("-"),
                s.row_count,
                s.earliest,
                s.latest,
            );
        }
        if symbols_resp.symbols.len() > show_count {
            println!("  ... and {} more", symbols_resp.symbols.len() - show_count);
        }
    }

    // ── 5. Run Backtest — Legacy Signals ────────────────────────────────
    print_header("Run Backtest — Legacy Entry Signals");

    let legacy_signals = generate_legacy_signals(&args.symbol);
    println!(
        "  Sending {} legacy entry signals for {} on {} ({}) ...",
        legacy_signals.len(),
        args.symbol,
        args.exchange,
        args.data_type
    );

    let request = RunBacktestRequest {
        symbol: args.symbol.clone(),
        exchange: args.exchange.clone(),
        data_type: args.data_type.clone(),
        timeframe: args.timeframe.clone(),
        from: args.from.clone(),
        to: args.to.clone(),
        signals: legacy_signals,
        raw_signals: vec![],
        profile: args.profile.clone(),
        profile_def: None,
        config: BacktestConfigMsg {
            initial_balance: Some(args.balance),
            close_on_finish: Some(true),
            fill_model: Some("BidAsk".into()),
        },
    };

    let resp: RunBacktestResponse = client.call("run_backtest", &request).await?;
    println!("  Elapsed: {}ms", resp.elapsed_ms);

    if resp.success {
        if let Some(ref result) = resp.result {
            print_result_summary(result);
            print_trade_log(&result.trade_log, 20);
            print_positions(&result.positions, 10);

            // Show a few equity curve points
            if !result.equity_curve.is_empty() {
                print_section("Equity Curve (sample)");
                let step = (result.equity_curve.len() / 5).max(1);
                for (i, pt) in result.equity_curve.iter().enumerate() {
                    if i % step == 0 || i == result.equity_curve.len() - 1 {
                        println!("  {} => ${:.2}", pt.ts, pt.balance);
                    }
                }
            }
        }
    } else {
        println!(
            "  ✗ Backtest failed: {}",
            resp.error.as_deref().unwrap_or("unknown error")
        );
    }

    // ── 6. Run Backtest — F14 Raw Signals ───────────────────────────────
    print_header("Run Backtest — F14 Full Signal Actions");

    let f14_signals = generate_f14_signals(&args.symbol);
    println!(
        "  Sending {} F14 raw signals (entries + management) ...",
        f14_signals.len()
    );

    // Show what signals we're sending
    print_section("F14 Signal Stream");
    for (i, sig) in f14_signals.iter().enumerate() {
        let desc = match sig {
            RawSignalMsg::Entry { side, size, .. } => {
                format!("Entry {} size={}", side, size)
            }
            RawSignalMsg::ModifyStoploss { ts: _, price, .. } => {
                format!("ModifyStoploss price={}", price)
            }
            RawSignalMsg::ClosePartial { ts: _, ratio, .. } => {
                format!("ClosePartial ratio={}", ratio)
            }
            RawSignalMsg::MoveStoplossToEntry { .. } => "MoveStoplossToEntry".into(),
            RawSignalMsg::ScaleIn { ts: _, size, .. } => {
                format!("ScaleIn size={}", size)
            }
            RawSignalMsg::AddRule { rule, .. } => {
                format!("AddRule {:?}", rule)
            }
            RawSignalMsg::CloseAllInGroup { group_id, .. } => {
                format!("CloseAllInGroup {}", group_id)
            }
            RawSignalMsg::ModifyAllStoploss { ts: _, price, .. } => {
                format!("ModifyAllStoploss price={}", price)
            }
            RawSignalMsg::CloseAll { .. } => "CloseAll".into(),
            other => format!("{:?}", other),
        };

        // Extract ts from each variant for display
        let ts = match sig {
            RawSignalMsg::Entry { ts, .. }
            | RawSignalMsg::Close { ts, .. }
            | RawSignalMsg::ClosePartial { ts, .. }
            | RawSignalMsg::ModifyStoploss { ts, .. }
            | RawSignalMsg::MoveStoplossToEntry { ts, .. }
            | RawSignalMsg::AddTarget { ts, .. }
            | RawSignalMsg::RemoveTarget { ts, .. }
            | RawSignalMsg::AddRule { ts, .. }
            | RawSignalMsg::RemoveRule { ts, .. }
            | RawSignalMsg::ScaleIn { ts, .. }
            | RawSignalMsg::CancelPending { ts, .. }
            | RawSignalMsg::CloseAllOf { ts, .. }
            | RawSignalMsg::CloseAll { ts }
            | RawSignalMsg::CancelAllPending { ts }
            | RawSignalMsg::ModifyAllStoploss { ts, .. }
            | RawSignalMsg::CloseAllInGroup { ts, .. }
            | RawSignalMsg::ModifyAllStoplossInGroup { ts, .. } => ts.as_str(),
        };
        println!("  {:>2}. [{}] {}", i + 1, ts, desc);
    }

    // Use an inline profile definition to demonstrate profile_def (no server
    // profile needed).
    let inline_profile = ManagementProfileMsg {
        name: "inline-demo".into(),
        use_targets: vec![0, 1],
        close_ratios: vec![0.5, 0.5],
        stoploss_mode: Some(StoplossModeMsg::FromSignal),
        rules: vec![RuleConfigDefMsg::BreakevenAfterTargets { after_n: 1 }],
        group_override: None,
        let_remainder_run: false,
    };

    let f14_request = RunBacktestRequest {
        symbol: args.symbol.clone(),
        exchange: args.exchange.clone(),
        data_type: args.data_type.clone(),
        timeframe: args.timeframe.clone(),
        from: args.from.clone(),
        to: args.to.clone(),
        signals: vec![], // no legacy signals
        raw_signals: f14_signals,
        profile: None,
        profile_def: Some(inline_profile),
        config: BacktestConfigMsg {
            initial_balance: Some(args.balance),
            close_on_finish: Some(true),
            fill_model: Some("BidAsk".into()),
        },
    };

    let f14_resp: RunBacktestResponse = client.call("run_backtest", &f14_request).await?;
    println!("  Elapsed: {}ms", f14_resp.elapsed_ms);

    if f14_resp.success {
        if let Some(ref result) = f14_resp.result {
            print_result_summary(result);
            print_trade_log(&result.trade_log, 20);
            print_positions(&result.positions, 10);
        }
    } else {
        println!(
            "  ✗ Backtest failed: {}",
            f14_resp.error.as_deref().unwrap_or("unknown error")
        );
    }

    // ── Done ────────────────────────────────────────────────────────────
    print_header("Done");
    println!("  Closing connection...");
    client.close().await?;
    println!("  ✓ Client disconnected cleanly");

    Ok(())
}
