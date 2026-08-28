//! Backtest server client example.
//!
//! Connects to the backtest service and demonstrates ping, profile/symbol
//! discovery, and a backtest with raw signals.
//!
//! # Usage
//!
//! ```bash
//! # Start the backtest server first, then:
//! cargo run -p qs-backtest-server --example backtest_client
//!
//! # With custom options:
//! cargo run -p qs-backtest-server --example backtest_client -- \
//!     --endpoint shm://backtest \
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

use clap::Parser;

use backtest_server::rpc_types::*;
use qs_backtest_api::provider::xrpc::BacktestXrpcClient;
use qs_backtest_api::{BacktestClient, BacktestDiscoveryClient, BacktestSyncClient};
use qs_service::ServiceEndpoint;
use qs_service_xrpc::XrpcTransportConfig;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "backtest-client",
    about = "Example client for the backtest service"
)]
struct Args {
    /// Shared memory base name (must match server config).
    #[arg(long, default_value = "backtest")]
    shm_name: String,

    /// Transport endpoint. When omitted, `--shm-name` is interpreted as `shm://NAME`.
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,

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

/// Connect through the configured logical service endpoint.
async fn connect(
    endpoint: &ServiceEndpoint,
    client_name: &str,
) -> Result<BacktestXrpcClient, Box<dyn std::error::Error>> {
    println!("  Connecting to {endpoint} ...");
    Ok(BacktestXrpcClient::connect(endpoint, client_name, &XrpcTransportConfig::default()).await?)
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
        "  Long  - trades: {}, pnl: ${:.2}, win rate: {:.1}%",
        result.long_stats.total_trades,
        result.long_stats.total_pnl,
        result.long_stats.win_rate * 100.0
    );
    println!(
        "  Short - trades: {}, pnl: ${:.2}, win rate: {:.1}%",
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
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "EXIT", "EXEC_LOT", "PNL", "CLOSE_REASON"
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
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "AVG_EXIT", "ORIG_LOT", "NET_PNL", "CLOSE_REASONS"
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

// Dummy signal generators.

/// Generate raw signals that demonstrate the complete action vocabulary.
/// Entries, management actions, and bulk operations can share one stream without relying on server-side profiles.
fn generate_full_signal_actions(symbol: &str) -> Vec<RawSignalMsg> {
    vec![
        // 1. Open a long position with stoploss and targets. Use a stable
        //    trade_id so later management signals can target this trade.
        RawSignalMsg::Entry {
            ts: "2024-02-01T09:00:00Z".into(),
            symbol: symbol.into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.1050, 1.1100],
            group: Some("lifecycle-demo".into()),
            trade_id: Some("lifecycle-demo-buy-1".into()),
        },
        // 2. Tighten the stoploss on the trade by trade_id.
        RawSignalMsg::ModifyStoploss {
            ts: "2024-02-01T12:00:00Z".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "lifecycle-demo-buy-1".into(),
            },
            price: 1.0850,
        },
        // 3. Take partial profits - close 50% of the same trade.
        RawSignalMsg::ClosePartial {
            ts: "2024-02-02T10:00:00Z".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "lifecycle-demo-buy-1".into(),
            },
            ratio: 0.5,
        },
        // 4. Move stoploss to entry (breakeven) for the remainder.
        RawSignalMsg::MoveStoplossToEntry {
            ts: "2024-02-02T10:01:00Z".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "lifecycle-demo-buy-1".into(),
            },
        },
        // 5. Open a second position (short) in a different group.
        RawSignalMsg::Entry {
            ts: "2024-02-05T14:00:00Z".into(),
            symbol: symbol.into(),
            side: "Sell".into(),
            order_type: "Market".into(),
            price: None,
            risk: 1.0,
            stoploss: Some(1.1150),
            targets: vec![1.0900],
            group: Some("lifecycle-hedge".into()),
            trade_id: Some("lifecycle-hedge-sell-1".into()),
        },
        // 6. Scale into the short trade by trade_id.
        RawSignalMsg::ScaleIn {
            ts: "2024-02-06T09:30:00Z".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "lifecycle-hedge-sell-1".into(),
            },
            price: None,
            size: 0.25,
        },
        // 7. Add a trailing stop rule to the short trade.
        RawSignalMsg::AddRule {
            ts: "2024-02-06T09:31:00Z".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "lifecycle-hedge-sell-1".into(),
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
            risk: 1.0,
            stoploss: Some(1.0700),
            targets: vec![1.1000, 1.1050],
            group: Some("lifecycle-demo".into()),
            trade_id: Some("lifecycle-demo-buy-2".into()),
        },
        // 9. Close all positions in the hedge group.
        RawSignalMsg::CloseAllInGroup {
            ts: "2024-02-12T16:00:00Z".into(),
            group_id: "lifecycle-hedge".into(),
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

// Example client entry point.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // ── 1. Connect ──────────────────────────────────────────────────────
    print_header("Connecting to Backtest Server");
    let endpoint = match args.endpoint.clone() {
        Some(endpoint) => endpoint,
        None => format!("shm://{}", args.shm_name).parse()?,
    };
    let client = connect(&endpoint, "example-backtest-client").await?;

    let operation_result: Result<(), Box<dyn std::error::Error>> = async {
        println!("  ✓ Connected successfully");

        // ── 2. Ping ─────────────────────────────────────────────────────────
        print_header("Ping");
        let ping = client.ping().await?;
        println!("  Status:   {}", ping.status);
        println!("  Uptime:   {}s", ping.uptime_secs);
        println!("  Data Dir: {}", ping.data_dir);

        // ── 3. List Profiles ────────────────────────────────────────────────
        print_header("Available Profiles");
        let profiles_resp = client.list_profiles().await?;
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
        let symbols_resp = client
            .list_symbols(ListSymbolsRequest {
                exchange: Some(args.exchange.clone()),
                data_type: None,
            })
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

        // 5. Run a backtest with the complete raw-signal action vocabulary.
        print_header("Run Backtest - Full Signal Actions");

        let raw_signals = generate_full_signal_actions(&args.symbol);
        println!(
            "  Sending {} raw signals (entries + management) ...",
            raw_signals.len()
        );

        // Show the signals being submitted.
        print_section("Raw Signal Stream");
        for (i, sig) in raw_signals.iter().enumerate() {
            let desc = match sig {
                RawSignalMsg::Entry { side, risk, .. } => {
                    format!("Entry {} risk={}", side, risk)
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
                | RawSignalMsg::ModifyTarget { ts, .. }
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
            target_selection: Some(TargetSelectionMsg::Selected(vec![1, 2])),
            use_targets: vec![1, 2],
            close_ratios: vec![0.5, 0.5],
            stoploss_mode: Some(StoplossModeMsg::FromSignal),
            rules: vec![RuleConfigDefMsg::BreakevenAfterTargets { after_n: 1 }],
            group_override: None,
            let_remainder_run: false,
        };

        let raw_signal_request = BacktestRunSpec {
            symbol: args.symbol.clone(),
            symbols: Vec::new(),
            all_symbols: false,
            exchange: args.exchange.clone(),
            data_type: args.data_type.clone(),
            timeframe: args.timeframe.clone(),
            from: args.from.clone(),
            to: args.to.clone(),
            raw_signals,
            profile: None,
            profile_def: Some(inline_profile),
            config: BacktestConfigMsg {
                initial_balance: Some(args.balance),
                close_on_finish: Some(true),
                fill_model: Some("BidAsk".into()),
                sizing: Some(SizingPolicyMsg::FixedLot { lots: 1.0 }),
            },
        };

        let raw_signal_response = client
            .run_backtest(RunBacktestRequest {
                request: raw_signal_request,
                future: FutureQuoteConfigMsg {
                    account_currency: "USD".into(),
                    ..FutureQuoteConfigMsg::default()
                },
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Inline,
            })
            .await?;
        println!("  Elapsed: {}ms", raw_signal_response.elapsed_ms);

        if raw_signal_response.success {
            if let Some(ref result) = raw_signal_response.result {
                print_result_summary(result);
                print_trade_log(&result.trade_log, 20);
                print_positions(&result.positions, 10);
            }
        } else {
            println!(
                "  ✗ Backtest failed: {}",
                raw_signal_response
                    .error
                    .as_deref()
                    .unwrap_or("unknown error")
            );
        }

        Ok(())
    }
    .await;

    print_header("Done");
    println!("  Closing connection...");
    let shutdown_result: Result<(), Box<dyn std::error::Error>> = client
        .close()
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>);

    match (operation_result, shutdown_result) {
        (Ok(()), Ok(())) => {
            println!("  ✓ Client disconnected cleanly");
            Ok(())
        }
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(operation_error), Ok(())) => Err(operation_error),
        (Err(operation_error), Err(shutdown_error)) => {
            eprintln!("  Client shutdown also failed: {shutdown_error}");
            Err(operation_error)
        }
    }
}
