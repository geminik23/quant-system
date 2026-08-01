//! Telegram signal backtest client — loads parsed JSONL and submits to backtest server.
//!
//! Reads pre-parsed raw signal JSONL (RawSignalMsg format) produced by the
//! `parse_signals` binary, connects to the backtest server over SHM, and
//! prints the backtest results.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::NaiveDateTime;
use clap::{Parser, ValueEnum};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use backtest_server::convert::{
    config_from_msg, evaluation_options_from_msg_for_symbols, future_config_from_msg, result_to_msg,
};
use backtest_server::rpc_types::*;
use qs_backtest::currency::{ConversionRoute, RunCurrencyPlan};
use qs_backtest::runner::{BacktestConfig, BacktestRunner};
use qs_backtest::{DEFAULT_MTM_MAX_POINTS, MAX_MTM_MAX_POINTS, MIN_MTM_MAX_POINTS, VecFeed};
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{DynRpcClient, JsonCodec, XrpcClientSession, XrpcTransportConfig};
use qs_symbols::SymbolRegistry;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ReportMode {
    Standard,
    Provider,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum EvaluationJsonFormat {
    Json,
    JsonPretty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum MtmOutputMode {
    None,
    Bounded,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ResultDeliveryMode {
    Auto,
    Inline,
    Artifact,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum ExecutionMode {
    /// Submit a retained job and follow it through server-streaming.
    #[default]
    Stream,
    /// Submit a retained job and poll its status with a total deadline.
    Poll,
    /// Execute the legacy finite unary RPC.
    Sync,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CliPositionFilter {
    Symbol(String),
    Side(EvaluationPositionSideMsg),
    Group(EvaluationGroupFilterMsg),
    CloseReason(String),
}

fn normalized_selector(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn parse_report_section(value: &str) -> Result<EvaluationSectionMsg, String> {
    match normalized_selector(value).as_str() {
        "coverage" => Ok(EvaluationSectionMsg::Coverage),
        "position_performance" | "performance" => Ok(EvaluationSectionMsg::PositionPerformance),
        "r_metrics" | "r" => Ok(EvaluationSectionMsg::RMetrics),
        "excursions" => Ok(EvaluationSectionMsg::Excursions),
        "execution" => Ok(EvaluationSectionMsg::Execution),
        "robustness" => Ok(EvaluationSectionMsg::Robustness),
        "breakdowns" | "breakdown" => Ok(EvaluationSectionMsg::Breakdowns),
        _ => Err(format!(
            "unknown report section `{value}`; expected coverage, position-performance, r-metrics, excursions, execution, robustness, or breakdowns"
        )),
    }
}

fn parse_breakdown(value: &str) -> Result<BreakdownDimensionMsg, String> {
    let normalized = normalized_selector(value);
    match normalized.as_str() {
        "symbol" => Ok(BreakdownDimensionMsg::Symbol),
        "side" => Ok(BreakdownDimensionMsg::Side),
        "group" => Ok(BreakdownDimensionMsg::Group),
        "close_reason" => Ok(BreakdownDimensionMsg::CloseReason),
        _ if value.trim().to_ascii_lowercase().starts_with("tag:") => Err(
            "unsupported evaluation selector: tag breakdowns are not supported by integrated backtests because completed positions have no tags".into(),
        ),
        _ => Err(format!(
            "unknown breakdown `{value}`; expected symbol, side, group, or close-reason"
        )),
    }
}

fn parse_filter(value: &str) -> Result<CliPositionFilter, String> {
    let Some((selector, raw_value)) = value.split_once('=') else {
        return Err(format!("invalid filter `{value}`; expected selector=value"));
    };
    let raw_value = raw_value.trim();
    if raw_value.is_empty() {
        return Err(format!("filter `{selector}` value must not be empty"));
    }
    let selector = selector.trim();
    if selector
        .split_once(':')
        .is_some_and(|(prefix, _)| prefix.eq_ignore_ascii_case("tag"))
    {
        return Err(
            "unsupported evaluation selector: tag filters are not supported by integrated backtests because completed positions have no tags".into(),
        );
    }

    let selector = normalized_selector(selector);
    match selector.as_str() {
        "symbol" => Ok(CliPositionFilter::Symbol(raw_value.to_owned())),
        "side" => match normalized_selector(raw_value).as_str() {
            "long" | "buy" => Ok(CliPositionFilter::Side(EvaluationPositionSideMsg::Long)),
            "short" | "sell" => Ok(CliPositionFilter::Side(EvaluationPositionSideMsg::Short)),
            _ => Err(format!(
                "unknown side filter `{raw_value}`; expected long or short"
            )),
        },
        "group" if raw_value.eq_ignore_ascii_case("ungrouped") => Ok(CliPositionFilter::Group(
            EvaluationGroupFilterMsg::Ungrouped,
        )),
        "group" => Ok(CliPositionFilter::Group(EvaluationGroupFilterMsg::Named(
            raw_value.to_owned(),
        ))),
        "close_reason" => Ok(CliPositionFilter::CloseReason(raw_value.to_owned())),
        _ => Err(format!(
            "unknown filter selector `{selector}`; expected symbol, side, group, or close-reason"
        )),
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "tg_backtest",
    about = "Load parsed Telegram signal JSONL and run backtest via SHM server",
    after_help = "Entry JSONL objects require `risk`. When any loaded signal is an Entry, provide exactly one of --base-lot, --risk-per-trade, or --risk-percent plus --account-currency.\n\nExample:\n  tg_backtest --input signals.jsonl --all-symbols --exchange ctrader --risk-per-trade 100 --account-currency USD"
)]
struct Args {
    /// Path to parsed signals JSONL file (use "-" for stdin).
    #[arg(short, long)]
    input: String,

    /// Optional parser outcomes JSONL produced by fx-provider-parser.
    #[arg(long)]
    outcomes_input: Option<String>,

    /// Shared memory base name (must match server config).
    #[arg(long, default_value = "backtest")]
    shm_name: String,

    /// Transport endpoint. When omitted, `--shm-name` is interpreted as `shm://NAME`.
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,

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

    /// Deliver results automatically, inline, or through an artifact.
    #[arg(long, value_enum, default_value_t = ResultDeliveryMode::Auto)]
    result_delivery: ResultDeliveryMode,

    /// Console report mode.
    #[arg(long, value_enum, default_value_t = ReportMode::Standard)]
    report: ReportMode,

    /// Provider identifier attached to provider-evaluation output.
    #[arg(long)]
    provider_id: Option<String>,

    /// Source identifier attached to provider-evaluation output.
    #[arg(long)]
    source_id: Option<String>,

    /// Comma-separated provider report sections (default: all).
    #[arg(long, value_delimiter = ',', value_parser = parse_report_section)]
    report_sections: Option<Vec<EvaluationSectionMsg>>,

    /// Repeatable breakdown: symbol, side, group, or close-reason.
    #[arg(long, value_parser = parse_breakdown)]
    breakdown: Vec<BreakdownDimensionMsg>,

    /// Repeatable typed filter, e.g. symbol=ES, side=long, group=trend,
    /// or close-reason=Target.
    #[arg(long, value_parser = parse_filter)]
    filter: Vec<CliPositionFilter>,

    /// Write provider evaluation JSON to this file.
    #[arg(long)]
    evaluation_output: Option<String>,

    /// JSON encoding used for console provider reports and evaluation output.
    #[arg(long, value_enum, default_value_t = EvaluationJsonFormat::JsonPretty)]
    evaluation_format: EvaluationJsonFormat,

    /// Include normalized position rows selected by the evaluation filter.
    #[arg(long, default_value_t = false)]
    include_positions: bool,

    /// Deterministic cap for included normalized position rows.
    #[arg(long, requires = "include_positions")]
    max_position_rows: Option<usize>,

    /// Deterministic global cap across breakdown bucket rows.
    #[arg(long)]
    max_breakdown_rows: Option<usize>,

    /// Minimum selected positions required for a breakdown bucket.
    #[arg(long, default_value_t = 1)]
    min_breakdown_bucket_count: usize,

    /// Number of deterministic bootstrap samples.
    #[arg(long, default_value_t = 2_000)]
    bootstrap_samples: usize,

    /// Bootstrap confidence level in the open interval (0, 1).
    #[arg(long, default_value_t = 0.95)]
    bootstrap_confidence: f64,

    /// Deterministic bootstrap seed.
    #[arg(long, default_value_t = 0xA076_1D64_78BD_642F)]
    bootstrap_seed: u64,

    /// Minimum observations required for bootstrap confidence intervals.
    #[arg(long, default_value_t = 5)]
    bootstrap_minimum_sample_size: usize,

    /// Completed positions per rolling robustness window.
    #[arg(long, default_value_t = 20)]
    rolling_window: usize,

    /// Remote execution mode. Streaming is retained, reconnectable, and has no total job deadline.
    #[arg(long, value_enum, default_value_t = ExecutionMode::Stream)]
    execution_mode: ExecutionMode,

    /// Deprecated compatibility alias for --execution-mode poll.
    #[arg(long = "async", default_value_t = false)]
    legacy_async: bool,

    /// Maximum time to poll an async job before returning a process error.
    #[arg(long, default_value_t = 300)]
    poll_timeout_secs: u64,

    /// Explicitly cancel a streamed job when Ctrl-C interrupts the client; default is detach.
    #[arg(long, default_value_t = false)]
    cancel_on_interrupt: bool,

    /// Base lot quantity scaled by each Entry risk multiplier.
    #[arg(long, group = "sizing")]
    base_lot: Option<f64>,

    /// Fixed risk amount per trade in account currency.
    #[arg(long, group = "sizing")]
    risk_per_trade: Option<f64>,

    /// Percentage of realized balance risked per trade, where 1 means 1 percent.
    #[arg(long, group = "sizing")]
    risk_percent: Option<f64>,

    /// Signal latency applied by FutureQuoteV1.
    #[arg(long, default_value_t = 0)]
    signal_latency_ms: i64,

    /// Fixed signed slippage in pips. Positive values are adverse.
    #[arg(long, default_value_t = 0.0)]
    slippage_pips: f64,

    /// Quote age threshold for stale-position diagnostics.
    #[arg(long)]
    stale_quote_after_ms: Option<i64>,

    /// Mark-to-market curve output policy.
    #[arg(long, value_enum, default_value_t = MtmOutputMode::Bounded)]
    mtm_output: MtmOutputMode,

    /// Maximum retained MTM points for bounded output. Default: 4096.
    #[arg(long)]
    mtm_max_points: Option<usize>,

    /// Maximum age of an FX conversion quote used for sizing and accounting.
    #[arg(long, default_value_t = 300_000)]
    conversion_stale_after_ms: i64,

    /// Account-currency P&L epsilon used for breakeven classification.
    #[arg(long, default_value_t = 1.0e-9)]
    pnl_epsilon: f64,

    /// Three-letter account currency used for sizing, accounting, and reporting.
    #[arg(long)]
    account_currency: Option<String>,
}

// ── Connection ──────────────────────────────────────────────────────────────

type BacktestRpcClient = DynRpcClient<JsonCodec>;

struct BacktestClientSession {
    client: Arc<BacktestRpcClient>,
    session: XrpcClientSession<JsonCodec>,
}

impl BacktestClientSession {
    fn client(&self) -> Arc<BacktestRpcClient> {
        Arc::clone(&self.client)
    }

    async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.session.close().await?;
        Ok(())
    }
}

async fn connect(
    endpoint: &ServiceEndpoint,
) -> Result<BacktestClientSession, Box<dyn std::error::Error>> {
    eprintln!("[connect] endpoint={endpoint}");
    let session = qs_service_xrpc::connect(
        endpoint,
        "tg-backtest",
        &XrpcTransportConfig::default(),
        JsonCodec,
    )
    .await?;
    let client = session.raw_client();
    Ok(BacktestClientSession { client, session })
}

// ── Signal Loading ──────────────────────────────────────────────────────────

fn parse_raw_signal_line(
    line: &str,
    line_number: usize,
) -> Result<RawSignalMsg, Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_str(line)
        .map_err(|error| format!("line {line_number}: failed to parse raw signal: {error}"))?;
    if value.get("action").and_then(serde_json::Value::as_str) == Some("Entry") {
        let fields = value
            .as_object()
            .ok_or_else(|| format!("line {line_number}: Entry signal must be a JSON object"))?;
        if fields.contains_key("size") {
            return Err(format!(
                "line {line_number}: Entry field `size` is obsolete; provide required `risk` instead"
            )
            .into());
        }
        if !fields.contains_key("risk") {
            return Err(format!("line {line_number}: Entry requires field `risk`").into());
        }
    }

    serde_json::from_value(value)
        .map_err(|error| format!("line {line_number}: failed to parse raw signal: {error}").into())
}

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
        signals.push(parse_raw_signal_line(trimmed, lineno + 1)?);
    }
    Ok(signals)
}

/// Filter raw signal messages to only those within the requested date range.
///
/// This is a client-side optimisation that reduces request payload size.
/// The server also applies authoritative filtering, so correctness does not
/// depend on this function.
/// Parse a CLI or source timestamp: RFC 3339 first (normalized to UTC), then the
/// naive ISO forms, then a bare date.
///
/// RFC 3339 support is required for `--outcomes-input` date filtering, not merely
/// convenient: `RawTgMessage.ts` in parser outcome files is RFC 3339 with an offset
/// (for example `2026-01-01T19:51:04Z`), which is the parser framework's documented
/// input contract and what `signal_parser::parse_iso_datetime` accepts. Without this
/// branch, combining `--outcomes-input` with `--from`/`--to` failed on every real
/// parser output while passing on naive-timestamp test fixtures.
fn parse_cli_timestamp(value: &str) -> Option<NaiveDateTime> {
    if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.naive_utc());
    }
    let formats = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for format in formats {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Some(timestamp);
        }
    }
    chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
}

fn filter_signals_by_date(
    signals: Vec<RawSignalMsg>,
    from: &Option<String>,
    to: &Option<String>,
) -> Vec<RawSignalMsg> {
    if from.is_none() && to.is_none() {
        return signals;
    }
    let from_dt = from.as_deref().and_then(parse_cli_timestamp);
    let to_dt = to.as_deref().and_then(parse_cli_timestamp);
    signals
        .into_iter()
        .filter(|s| {
            let Some(ts) = parse_cli_timestamp(s.ts()) else {
                return true; // keep signals with unparseable timestamps
            };
            let after_from = from_dt.is_none_or(|f| ts >= f);
            let before_to = to_dt.is_none_or(|t| ts <= t);
            after_from && before_to
        })
        .collect()
}

/// Typed JSONL row compatible with `signal_parser::MessageParseOutcome` output.
#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum MessageParseOutcomeRow {
    Parsed {
        source: ParserSourceMessage,
        #[serde(rename = "parser")]
        _parser: String,
        signals: Vec<qs_backtest::RawSignal>,
    },
    Skipped {
        source: ParserSourceMessage,
        #[serde(rename = "parser")]
        _parser: Option<String>,
        #[serde(rename = "reason")]
        _reason: serde_json::Value,
    },
    Failed {
        source: ParserSourceMessage,
        #[serde(rename = "parser")]
        _parser: Option<String>,
        #[serde(rename = "failure")]
        _failure: serde_json::Value,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParserSourceMessage {
    chat_id: i64,
    msg_id: i64,
    ts: String,
    #[serde(rename = "message")]
    _message: String,
    #[serde(rename = "reply_to")]
    _reply_to: Option<i64>,
}

impl MessageParseOutcomeRow {
    fn source(&self) -> &ParserSourceMessage {
        match self {
            Self::Parsed { source, .. }
            | Self::Skipped { source, .. }
            | Self::Failed { source, .. } => source,
        }
    }
}

fn load_source_coverage(
    path: &str,
    from: &Option<String>,
    to: &Option<String>,
) -> Result<SourceCoverageCountsMsg, Box<dyn std::error::Error>> {
    let reader: Box<dyn BufRead> = if path == "-" {
        Box::new(io::BufReader::new(io::stdin()))
    } else {
        Box::new(io::BufReader::new(std::fs::File::open(path)?))
    };
    let filter_by_date = from.is_some() || to.is_some();
    let from = from.as_deref().and_then(parse_cli_timestamp);
    let to = to.as_deref().and_then(parse_cli_timestamp);
    let mut sources = HashSet::new();
    let mut coverage = SourceCoverageCountsMsg::default();

    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let outcome: MessageParseOutcomeRow = serde_json::from_str(line).map_err(|error| {
            format!(
                "outcomes line {}: expected exactly one parser source outcome object: {error}",
                line_number + 1
            )
        })?;
        let source = outcome.source();
        if !sources.insert((source.chat_id, source.msg_id)) {
            return Err(format!(
                "outcomes line {}: duplicate source outcome for chat_id={} msg_id={}",
                line_number + 1,
                source.chat_id,
                source.msg_id
            )
            .into());
        }
        if filter_by_date {
            let timestamp = parse_cli_timestamp(&source.ts).ok_or_else(|| {
                format!(
                    "outcomes line {}: invalid source timestamp `{}` required for date filtering",
                    line_number + 1,
                    source.ts
                )
            })?;
            if from.is_some_and(|start| timestamp < start) || to.is_some_and(|end| timestamp > end)
            {
                continue;
            }
        }

        coverage.raw_messages += 1;
        match outcome {
            MessageParseOutcomeRow::Parsed { signals, .. } => {
                coverage.parsed_messages += 1;
                coverage.emitted_signals += signals.len() as u64;
                coverage.emitted_entry_signals +=
                    signals.iter().filter(|signal| signal.is_entry()).count() as u64;
            }
            MessageParseOutcomeRow::Skipped { .. } => coverage.skipped_messages += 1,
            MessageParseOutcomeRow::Failed { .. } => coverage.failed_messages += 1,
        }
    }

    Ok(coverage)
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

fn sizing_policy(args: &Args) -> Option<SizingPolicyMsg> {
    match (args.base_lot, args.risk_per_trade, args.risk_percent) {
        (Some(lots), None, None) => Some(SizingPolicyMsg::FixedLot { lots }),
        (None, Some(amount), None) => Some(SizingPolicyMsg::FixedRiskAmount { amount }),
        (None, None, Some(percent)) => Some(SizingPolicyMsg::BalanceRiskPercent { percent }),
        _ => None,
    }
}

fn loaded_signals_have_entry(signals: &[RawSignalMsg]) -> bool {
    signals
        .iter()
        .any(|signal| matches!(signal, RawSignalMsg::Entry { .. }))
}

fn validate_loaded_signal_contract(args: &Args, signals: &[RawSignalMsg]) -> Result<(), String> {
    if !loaded_signals_have_entry(signals) {
        return Ok(());
    }

    let sizing_count = usize::from(args.base_lot.is_some())
        + usize::from(args.risk_per_trade.is_some())
        + usize::from(args.risk_percent.is_some());
    if sizing_count != 1 {
        return Err(
            "Entry signals require exactly one of --base-lot, --risk-per-trade, or --risk-percent"
                .into(),
        );
    }
    if args
        .account_currency
        .as_deref()
        .is_none_or(|currency| currency.trim().is_empty())
    {
        return Err("Entry signals require --account-currency".into());
    }
    Ok(())
}

fn result_delivery_message(args: &Args) -> ResultDeliveryMsg {
    match args.result_delivery {
        ResultDeliveryMode::Auto => ResultDeliveryMsg::Auto,
        ResultDeliveryMode::Inline => ResultDeliveryMsg::Inline,
        ResultDeliveryMode::Artifact => ResultDeliveryMsg::Artifact,
    }
}

fn mtm_output_policy(args: &Args) -> MtmOutputPolicyMsg {
    match args.mtm_output {
        MtmOutputMode::None => MtmOutputPolicyMsg::None,
        MtmOutputMode::Bounded => MtmOutputPolicyMsg::Bounded {
            max_points: args.mtm_max_points.unwrap_or(DEFAULT_MTM_MAX_POINTS),
        },
        MtmOutputMode::Full => MtmOutputPolicyMsg::Full,
    }
}

fn future_config_message(args: &Args) -> FutureQuoteConfigMsg {
    FutureQuoteConfigMsg {
        signal_latency_ms: args.signal_latency_ms,
        slippage_pips: args.slippage_pips,
        stale_quote_after_ms: args.stale_quote_after_ms,
        pnl_epsilon: args.pnl_epsilon,
        account_currency: args.account_currency.clone().unwrap_or_default(),
        conversion_stale_after_ms: args.conversion_stale_after_ms,
        mtm_output: mtm_output_policy(args),
    }
}

fn uses_provider_options(args: &Args) -> bool {
    args.report == ReportMode::Provider
        || args.provider_id.is_some()
        || args.source_id.is_some()
        || args.outcomes_input.is_some()
        || args.report_sections.is_some()
        || !args.breakdown.is_empty()
        || !args.filter.is_empty()
        || args.evaluation_output.is_some()
        || args.include_positions
        || args.max_position_rows.is_some()
        || args.max_breakdown_rows.is_some()
        || args.min_breakdown_bucket_count != 1
        || args.bootstrap_samples != 2_000
        || (args.bootstrap_confidence - 0.95).abs() > f64::EPSILON
        || args.bootstrap_seed != 0xA076_1D64_78BD_642F
        || args.bootstrap_minimum_sample_size != 5
        || args.rolling_window != 20
}

fn execution_mode(args: &Args) -> ExecutionMode {
    if args.legacy_async {
        ExecutionMode::Poll
    } else {
        args.execution_mode
    }
}

fn validate_evaluation_args(args: &Args) -> Result<(), String> {
    if args.poll_timeout_secs == 0 {
        return Err("--poll-timeout-secs must be positive".into());
    }
    if args
        .provider_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("--provider-id must not be empty".into());
    }
    if args
        .source_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err("--source-id must not be empty".into());
    }
    if args.input == "-" && args.outcomes_input.as_deref() == Some("-") {
        return Err("--input and --outcomes-input cannot both read from stdin".into());
    }
    if args.min_breakdown_bucket_count == 0 {
        return Err("--min-breakdown-bucket-count must be positive".into());
    }
    if args.bootstrap_samples == 0 {
        return Err("--bootstrap-samples must be positive".into());
    }
    if !args.bootstrap_confidence.is_finite()
        || args.bootstrap_confidence <= 0.0
        || args.bootstrap_confidence >= 1.0
    {
        return Err("--bootstrap-confidence must be finite and between 0 and 1".into());
    }
    if args.bootstrap_minimum_sample_size == 0 {
        return Err("--bootstrap-minimum-sample-size must be positive".into());
    }
    if args.rolling_window == 0 {
        return Err("--rolling-window must be positive".into());
    }
    for (option, value) in [
        ("--base-lot", args.base_lot),
        ("--risk-per-trade", args.risk_per_trade),
        ("--risk-percent", args.risk_percent),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value <= 0.0) {
            return Err(format!("{option} must be finite and positive"));
        }
    }
    if args.conversion_stale_after_ms < 0 {
        return Err("--conversion-stale-after-ms must be non-negative".into());
    }
    if args.mtm_output == MtmOutputMode::Full && args.result_delivery == ResultDeliveryMode::Inline
    {
        return Err("--mtm-output full cannot be used with --result-delivery inline".into());
    }
    match (args.mtm_output, args.mtm_max_points) {
        (MtmOutputMode::Bounded, max_points)
            if !(MIN_MTM_MAX_POINTS..=MAX_MTM_MAX_POINTS)
                .contains(&max_points.unwrap_or(DEFAULT_MTM_MAX_POINTS)) =>
        {
            return Err(format!(
                "--mtm-max-points must be between {MIN_MTM_MAX_POINTS} and {MAX_MTM_MAX_POINTS}, got {}",
                max_points.unwrap_or(DEFAULT_MTM_MAX_POINTS)
            ));
        }
        (MtmOutputMode::None | MtmOutputMode::Full, Some(_)) => {
            return Err("--mtm-max-points requires --mtm-output bounded".into());
        }
        _ => {}
    }
    if args.mtm_output == MtmOutputMode::Full && args.output.is_none() {
        return Err("--mtm-output full requires --output".into());
    }
    if args
        .account_currency
        .as_deref()
        .is_some_and(|currency| currency.trim().is_empty())
    {
        return Err("--account-currency must not be empty".into());
    }
    if !args.breakdown.is_empty()
        && args
            .report_sections
            .as_ref()
            .is_some_and(|sections| !sections.contains(&EvaluationSectionMsg::Breakdowns))
    {
        return Err("--breakdown requires the breakdowns report section".into());
    }
    Ok(())
}

fn provider_evaluation_options(
    args: &Args,
    source_coverage: Option<SourceCoverageCountsMsg>,
) -> ProviderEvaluationOptionsMsg {
    let mut filter = PositionFilterMsg::default();
    for selector in &args.filter {
        match selector {
            CliPositionFilter::Symbol(symbol) => filter.symbols.push(symbol.clone()),
            CliPositionFilter::Side(side) => filter.sides.push(*side),
            CliPositionFilter::Group(group) => filter.groups.push(group.clone()),
            CliPositionFilter::CloseReason(reason) => {
                filter.close_reasons.push(reason.clone());
            }
        }
    }

    ProviderEvaluationOptionsMsg {
        context: EvaluationContextMsg {
            provider_id: args.provider_id.clone(),
            source_id: args.source_id.clone(),
        },
        source_coverage,
        sections: args
            .report_sections
            .clone()
            .unwrap_or_else(|| EvaluationSectionMsg::ALL.to_vec()),
        filter,
        breakdowns: args.breakdown.clone(),
        bootstrap: BootstrapConfigMsg {
            samples: args.bootstrap_samples,
            confidence_level: args.bootstrap_confidence,
            seed: args.bootstrap_seed,
            minimum_sample_size: args.bootstrap_minimum_sample_size,
        },
        rolling_window: args.rolling_window,
        minimum_breakdown_bucket_count: args.min_breakdown_bucket_count,
        maximum_breakdown_rows: args.max_breakdown_rows,
        include_positions: args.include_positions,
        maximum_position_rows: args.max_position_rows,
    }
}

// ── Async Status Handling ───────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncStatusDecision {
    Continue,
    Completed,
}

fn evaluate_async_status(status: &BacktestStatusResponse) -> Result<AsyncStatusDecision, String> {
    if !status.success || status.status == "NotFound" {
        return Err(status
            .error
            .clone()
            .unwrap_or_else(|| format!("Job '{}' was not found", status.job_id)));
    }
    match status.status.as_str() {
        "Queued" | "LoadingData" | "Running" => Ok(AsyncStatusDecision::Continue),
        "Completed" => Ok(AsyncStatusDecision::Completed),
        "Failed" => Err(status
            .error
            .clone()
            .unwrap_or_else(|| format!("Job '{}' failed", status.job_id))),
        "Cancelled" => Err(status
            .error
            .clone()
            .unwrap_or_else(|| format!("Job '{}' was cancelled", status.job_id))),
        other => Err(format!(
            "Job '{}' returned unknown status '{other}'",
            status.job_id
        )),
    }
}

fn server_response_error(error: Option<String>, fallback: &str) -> io::Error {
    io::Error::other(error.unwrap_or_else(|| fallback.to_string()))
}

async fn poll_async_job<GetStatus, StatusFuture, Cancel, CancelFuture>(
    job_id: &str,
    poll_timeout: std::time::Duration,
    poll_interval: std::time::Duration,
    mut get_status: GetStatus,
    mut cancel: Cancel,
) -> Result<(), Box<dyn std::error::Error>>
where
    GetStatus: FnMut(std::time::Duration) -> StatusFuture,
    StatusFuture: Future<Output = Result<BacktestStatusResponse, Box<dyn std::error::Error>>>,
    Cancel: FnMut() -> CancelFuture,
    CancelFuture: Future<Output = ()>,
{
    let poll_started = tokio::time::Instant::now();
    loop {
        let elapsed = poll_started.elapsed();
        if elapsed >= poll_timeout {
            let original: Box<dyn std::error::Error> = Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "Timed out waiting {}s for job '{}'",
                    poll_timeout.as_secs(),
                    job_id
                ),
            ));
            cancel().await;
            return Err(original);
        }

        let remaining = poll_timeout - elapsed;
        let status = match get_status(remaining.min(std::time::Duration::from_secs(10))).await {
            Ok(status) => status,
            Err(original) => {
                cancel().await;
                return Err(original);
            }
        };
        println!(
            "  Status: {} [{} events {}/{}, signals {}/{}, symbols {}/{}]",
            status.status,
            status.progress.stage,
            status.progress.processed_events,
            status.progress.total_events,
            status.progress.processed_signals,
            status.progress.total_signals,
            status.progress.processed_symbols,
            status.progress.total_symbols,
        );
        match evaluate_async_status(&status) {
            Ok(AsyncStatusDecision::Continue) => {}
            Ok(AsyncStatusDecision::Completed) => return Ok(()),
            Err(error) => {
                let original: Box<dyn std::error::Error> = Box::new(io::Error::other(error));
                cancel().await;
                return Err(original);
            }
        }

        let remaining = poll_timeout.saturating_sub(poll_started.elapsed());
        tokio::time::sleep(poll_interval.min(remaining)).await;
    }
}

#[derive(Debug)]
enum StreamEventDecision {
    Continue,
    Completed,
}

fn evaluate_stream_event(
    expected_job_id: &str,
    event: &BacktestEvent,
) -> Result<StreamEventDecision, String> {
    match event {
        BacktestEvent::Heartbeat { job_id, .. } => {
            if job_id != expected_job_id {
                return Err(format!(
                    "Backtest stream job mismatch: expected '{expected_job_id}', received '{job_id}'"
                ));
            }
            Ok(StreamEventDecision::Continue)
        }
        BacktestEvent::Snapshot { status } => {
            if status.job_id != expected_job_id {
                return Err(format!(
                    "Backtest stream job mismatch: expected '{expected_job_id}', received '{}'",
                    status.job_id
                ));
            }
            match evaluate_async_status(status)? {
                AsyncStatusDecision::Continue => Ok(StreamEventDecision::Continue),
                AsyncStatusDecision::Completed => Ok(StreamEventDecision::Completed),
            }
        }
    }
}

enum WatchAttempt {
    Completed,
    Retry(String),
    Failed(String),
    Interrupted,
}

async fn watch_backtest_attempt(client: &Arc<BacktestRpcClient>, job_id: &str) -> WatchAttempt {
    let mut stream = match client
        .call_server_stream::<_, BacktestEvent>(
            "watch_backtest",
            &WatchBacktestRequest {
                job_id: job_id.to_owned(),
            },
        )
        .await
    {
        Ok(stream) => stream,
        Err(error) if !client.is_connected() => return WatchAttempt::Retry(error.to_string()),
        Err(error) => return WatchAttempt::Failed(error.to_string()),
    };

    loop {
        let event = tokio::select! {
            event = stream.recv() => event,
            signal = tokio::signal::ctrl_c() => {
                return match signal {
                    Ok(()) => WatchAttempt::Interrupted,
                    Err(error) => WatchAttempt::Failed(format!(
                        "Failed to listen for Ctrl-C while watching job '{job_id}': {error}"
                    )),
                };
            }
        };

        let event = match event {
            Some(Ok(event)) => event,
            Some(Err(error)) if !client.is_connected() => {
                return WatchAttempt::Retry(error.to_string());
            }
            Some(Err(error)) => return WatchAttempt::Failed(error.to_string()),
            None => {
                return WatchAttempt::Retry(format!(
                    "Backtest stream for job '{job_id}' ended before a terminal snapshot"
                ));
            }
        };

        if let BacktestEvent::Snapshot { status } = &event {
            println!(
                "  Status: {} [{} events {}/{}, signals {}/{}, symbols {}/{}]",
                status.status,
                status.progress.stage,
                status.progress.processed_events,
                status.progress.total_events,
                status.progress.processed_signals,
                status.progress.total_signals,
                status.progress.processed_symbols,
                status.progress.total_symbols,
            );
        }

        match evaluate_stream_event(job_id, &event) {
            Ok(StreamEventDecision::Continue) => {}
            Ok(StreamEventDecision::Completed) => return WatchAttempt::Completed,
            Err(error) => return WatchAttempt::Failed(error),
        }
    }
}

async fn best_effort_cancel_streamed_job(client: &Arc<BacktestRpcClient>, job_id: &str) {
    let response: Result<CancelBacktestResponse, _> = client
        .call_with_timeout(
            "cancel_backtest",
            &CancelBacktestRequest {
                job_id: job_id.to_owned(),
            },
            std::time::Duration::from_secs(2),
        )
        .await;
    match response {
        Ok(response) if response.success => {
            eprintln!("  Cancellation requested for job '{}'.", response.job_id);
        }
        Ok(response) => {
            eprintln!(
                "  Cancellation for job '{}' was not accepted: {}",
                response.job_id,
                response.error.as_deref().unwrap_or("unknown error")
            );
        }
        Err(error) => {
            eprintln!("  Cancellation request for job '{job_id}' failed: {error}");
        }
    }
}

async fn interrupt_streamed_job(
    client: &Arc<BacktestRpcClient>,
    job_id: &str,
    cancel_on_interrupt: bool,
) -> Box<dyn std::error::Error> {
    if cancel_on_interrupt && client.is_connected() {
        best_effort_cancel_streamed_job(client, job_id).await;
    } else if cancel_on_interrupt {
        eprintln!(
            "  Job '{job_id}' could not be cancelled because no server connection is active."
        );
    } else {
        eprintln!("  Detached from job '{job_id}'; the retained server job was not cancelled.");
    }
    Box::new(io::Error::new(
        io::ErrorKind::Interrupted,
        format!("Interrupted while watching backtest job '{job_id}'"),
    ))
}

async fn watch_backtest_with_reconnect(
    session: &mut BacktestClientSession,
    endpoint: &ServiceEndpoint,
    job_id: &str,
    cancel_on_interrupt: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let client = session.client();
        match watch_backtest_attempt(&client, job_id).await {
            WatchAttempt::Completed => return Ok(()),
            WatchAttempt::Failed(error) => return Err(io::Error::other(error).into()),
            WatchAttempt::Interrupted => {
                return Err(interrupt_streamed_job(&client, job_id, cancel_on_interrupt).await);
            }
            WatchAttempt::Retry(error) => {
                eprintln!(
                    "  Backtest stream for job '{job_id}' was interrupted: {error}. Reconnecting..."
                );
            }
        }

        let reconnect_delay = tokio::time::sleep(std::time::Duration::from_secs(2));
        tokio::pin!(reconnect_delay);
        tokio::select! {
            _ = &mut reconnect_delay => {}
            signal = tokio::signal::ctrl_c() => {
                return match signal {
                    Ok(()) => Err(interrupt_streamed_job(
                        &session.client(),
                        job_id,
                        cancel_on_interrupt,
                    ).await),
                    Err(error) => Err(io::Error::other(format!(
                        "Failed to listen for Ctrl-C while reconnecting job '{job_id}': {error}"
                    )).into()),
                };
            }
        }

        let replacement = tokio::select! {
            result = connect(endpoint) => result,
            signal = tokio::signal::ctrl_c() => {
                return match signal {
                    Ok(()) => Err(interrupt_streamed_job(
                        &session.client(),
                        job_id,
                        cancel_on_interrupt,
                    ).await),
                    Err(error) => Err(io::Error::other(format!(
                        "Failed to listen for Ctrl-C while reconnecting job '{job_id}': {error}"
                    )).into()),
                };
            }
        };

        match replacement {
            Ok(replacement) => {
                let previous = std::mem::replace(session, replacement);
                match tokio::time::timeout(std::time::Duration::from_secs(2), previous.close())
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!("  Previous backtest session close failed: {error}");
                    }
                    Err(_) => {
                        eprintln!("  Previous backtest session did not close within 2 seconds.");
                    }
                }
                eprintln!("  Reconnected; resubscribing to job '{job_id}'.");
            }
            Err(error) => {
                eprintln!("  Reconnect attempt for job '{job_id}' failed: {error}");
            }
        }
    }
}

fn artifact_part_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".part");
    PathBuf::from(value)
}

fn ensure_output_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    ensure_output_parent(path)?;
    let part_path = artifact_part_path(path);
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&part_path)?;
        file.write_all(bytes)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&part_path, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(part_path);
    }
    result
}

async fn download_artifact_chunks<W, Fetch, FetchFuture>(
    reference: &ResultArtifactRefMsg,
    writer: &mut W,
    mut fetch: Fetch,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: Write,
    Fetch: FnMut(GetResultArtifactChunkRequest) -> FetchFuture,
    FetchFuture:
        Future<Output = Result<GetResultArtifactChunkResponse, Box<dyn std::error::Error>>>,
{
    if reference.format_version != RESULT_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported result artifact format_version: {}",
                reference.format_version
            ),
        )
        .into());
    }
    if reference.chunk_size == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "result artifact reference has a zero chunk size",
        )
        .into());
    }

    let mut next_offset = 0_u64;
    let mut hasher = Sha256::new();
    loop {
        let response = fetch(GetResultArtifactChunkRequest {
            artifact_id: reference.artifact_id.clone(),
            offset: next_offset,
        })
        .await?;
        if !response.success {
            return Err(server_response_error(
                response.error,
                "result artifact chunk request failed",
            )
            .into());
        }
        if response.artifact_id != reference.artifact_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "result artifact chunk returned a different artifact id",
            )
            .into());
        }
        if response.offset != next_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "result artifact chunk offset mismatch: expected {next_offset}, got {}",
                    response.offset
                ),
            )
            .into());
        }

        let bytes = BASE64_STANDARD.decode(response.data_base64.as_bytes())?;
        if bytes.len() as u64 > reference.chunk_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "result artifact chunk exceeded its advertised chunk size",
            )
            .into());
        }
        if bytes.is_empty() && !response.eof {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "result artifact returned an empty non-final chunk",
            )
            .into());
        }
        if next_offset.saturating_add(bytes.len() as u64) > reference.byte_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "result artifact exceeded its advertised byte length",
            )
            .into());
        }

        writer.write_all(&bytes)?;
        hasher.update(&bytes);
        next_offset += bytes.len() as u64;
        if response.eof {
            break;
        }
        if next_offset >= reference.byte_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "result artifact reached its advertised byte length without eof",
            )
            .into());
        }
    }
    writer.flush()?;

    if next_offset != reference.byte_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "result artifact byte length mismatch: expected {}, got {next_offset}",
                reference.byte_len
            ),
        )
        .into());
    }
    let actual_sha256 = format!("{:x}", hasher.finalize());
    if !actual_sha256.eq_ignore_ascii_case(&reference.sha256) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "result artifact SHA-256 mismatch: expected {}, got {actual_sha256}",
                reference.sha256
            ),
        )
        .into());
    }
    Ok(())
}

async fn download_artifact_from_rpc<W: Write>(
    client: &Arc<BacktestRpcClient>,
    reference: &ResultArtifactRefMsg,
    writer: &mut W,
) -> Result<(), Box<dyn std::error::Error>> {
    download_artifact_chunks(reference, writer, |request| {
        let client = client.clone();
        async move {
            client
                .call_with_timeout(
                    "get_result_artifact_chunk",
                    &request,
                    std::time::Duration::from_secs(30),
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
        }
    })
    .await
}

async fn delete_downloaded_artifact(
    client: &Arc<BacktestRpcClient>,
    reference: &ResultArtifactRefMsg,
) {
    let response: Result<DeleteResultArtifactResponse, _> = client
        .call_with_timeout(
            "delete_result_artifact",
            &DeleteResultArtifactRequest {
                artifact_id: reference.artifact_id.clone(),
            },
            std::time::Duration::from_secs(10),
        )
        .await;
    match response {
        Ok(response) if response.success => {}
        Ok(response) => eprintln!(
            "  Result artifact deletion was not accepted: {}",
            response.error.as_deref().unwrap_or("unknown error")
        ),
        Err(error) => eprintln!("  Result artifact deletion failed: {error}"),
    }
}

async fn download_result_artifact(
    client: &Arc<BacktestRpcClient>,
    reference: &ResultArtifactRefMsg,
    summary: Option<BacktestResultMsg>,
    output_path: Option<&str>,
) -> Result<(BacktestResultMsg, bool), Box<dyn std::error::Error>> {
    let delivered = if let Some(output_path) = output_path {
        let output_path = PathBuf::from(output_path);
        let part_path = artifact_part_path(&output_path);
        let operation: Result<(BacktestResultMsg, bool), Box<dyn std::error::Error>> = async {
            ensure_output_parent(&output_path)?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&part_path)?;
            download_artifact_from_rpc(client, reference, &mut file).await?;
            file.sync_all()?;
            drop(file);
            let result = match summary {
                Some(summary) => summary,
                None => {
                    serde_json::from_reader(io::BufReader::new(std::fs::File::open(&part_path)?))?
                }
            };
            std::fs::rename(&part_path, &output_path)?;
            Ok((result, true))
        }
        .await;
        if operation.is_err() {
            let _ = std::fs::remove_file(&part_path);
        }
        operation?
    } else if let Some(summary) = summary {
        (summary, false)
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--output is required when an artifact response omits its compact summary",
        )
        .into());
    };

    delete_downloaded_artifact(client, reference).await;
    Ok(delivered)
}

async fn receive_delivered_result(
    client: &Arc<BacktestRpcClient>,
    result: Option<BacktestResultMsg>,
    artifact: Option<ResultArtifactRefMsg>,
    inline_complete: bool,
    output_path: Option<&str>,
) -> Result<(BacktestResultMsg, bool), Box<dyn std::error::Error>> {
    if let Some(artifact) = artifact {
        return download_result_artifact(client, &artifact, result, output_path).await;
    }
    if !inline_complete {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "server returned an incomplete inline result without an artifact reference",
        )
        .into());
    }
    let result = result.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "successful backtest omitted its result",
        )
    })?;
    Ok((result, false))
}

// ── Display Helpers ─────────────────────────────────────────────────────────

fn render_percentage(fraction: f64, decimal_places: usize) -> String {
    format!("{:.*}%", decimal_places, fraction * 100.0)
}

fn format_currency(value: f64, currency: Option<&str>) -> String {
    match currency {
        Some(currency) => format!("{} {:.2}", currency.trim().to_ascii_uppercase(), value),
        None => format!("{value:.2}"),
    }
}

fn format_signed_currency(value: f64, currency: Option<&str>) -> String {
    match currency {
        Some(currency) => format!("{} {value:+.2}", currency.trim().to_ascii_uppercase()),
        None => format!("{value:+.2}"),
    }
}

fn result_currency_code(result: &BacktestResultMsg, args: &Args) -> Option<String> {
    result
        .future
        .as_ref()
        .and_then(|future| future.execution_metadata.get("account_currency"))
        .and_then(serde_json::Value::as_str)
        .or(args.account_currency.as_deref())
        .map(str::trim)
        .filter(|currency| !currency.is_empty())
        .map(str::to_ascii_uppercase)
}

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

fn print_result_summary(result: &BacktestResultMsg, currency: Option<&str>) {
    print_section("Performance Summary");
    println!(
        "  Initial Balance:   {:>16}",
        format_currency(result.initial_balance, currency)
    );
    println!(
        "  Final Balance:     {:>16}",
        format_currency(result.final_balance, currency)
    );
    println!(
        "  Total PnL:         {:>16}  ({:>+.2}%)",
        format_signed_currency(result.total_pnl, currency),
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
        "  Max Drawdown:      {:>16}  ({})",
        format_currency(result.max_drawdown, currency),
        render_percentage(result.max_drawdown_pct, 2)
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
        "  Long  - trades: {}, pnl: {}, win rate: {:.1}%",
        result.long_stats.total_trades,
        format_signed_currency(result.long_stats.total_pnl, currency),
        result.long_stats.win_rate * 100.0
    );
    println!(
        "  Short - trades: {}, pnl: {}, win rate: {:.1}%",
        result.short_stats.total_trades,
        format_signed_currency(result.short_stats.total_pnl, currency),
        result.short_stats.win_rate * 100.0
    );

    // Close reason breakdown.
    if !result.per_close_reason.is_empty() {
        print_section("Close Reasons");
        for cr in &result.per_close_reason {
            println!(
                "  {:<20} count={:<4} pnl={} avg={} ({})",
                cr.reason,
                cr.count,
                format_signed_currency(cr.total_pnl, currency),
                format_signed_currency(cr.avg_pnl, currency),
                render_percentage(cr.percentage, 1)
            );
        }
    }

    // Per-group breakdown.
    if !result.per_group.is_empty() {
        print_section("Per-Group");
        for (group, stats) in &result.per_group {
            println!(
                "  {:<24} trades={:<4} pnl={} win_rate={:.1}%",
                group,
                stats.total_trades,
                format_signed_currency(stats.total_pnl, currency),
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
                "  {}-{:02}:  pnl={}  trades={:<4} balance={}",
                mr.year,
                mr.month,
                format_signed_currency(mr.pnl, currency),
                mr.trade_count,
                format_currency(mr.ending_balance, currency)
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

fn print_trade_log(
    trades: &[TradeResultMsg],
    total_trades: usize,
    max: usize,
    currency: Option<&str>,
) {
    print_section("Close Events");
    if trades.is_empty() {
        println!("  (no trades)");
        return;
    }
    let show = trades.len().min(max);
    println!(
        "  {:<12} {:<8} {:<6} {:>12} {:>12} {:>8} {:>16} {:<15}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "EXIT", "SIZE", "PNL", "CLOSE_REASON"
    );
    println!("  {}", "-".repeat(104));
    for t in &trades[..show] {
        println!(
            "  {:<12} {:<8} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>16} {:<15}",
            &t.position_id[..t.position_id.len().min(12)],
            t.symbol,
            t.side,
            t.entry_price,
            t.exit_price,
            t.size,
            format_signed_currency(t.pnl, currency),
            t.close_reason,
        );
    }
    if total_trades > show {
        println!("  ... and {} more trades", total_trades - show);
    }
}

fn print_positions(
    positions: &[PositionSummaryMsg],
    total_positions: usize,
    max: usize,
    currency: Option<&str>,
) {
    print_section("Position Summaries");
    if positions.is_empty() {
        println!("  (no positions)");
        return;
    }
    let show = positions.len().min(max);
    println!(
        "  {:<12} {:<8} {:<6} {:>12} {:>12} {:>8} {:>16} {:<20}",
        "POS_ID", "SYMBOL", "SIDE", "ENTRY", "AVG_EXIT", "SIZE", "NET_PNL", "CLOSE_REASONS"
    );
    println!("  {}", "-".repeat(109));
    for p in &positions[..show] {
        let reasons = p.close_reasons.join(",");
        println!(
            "  {:<12} {:<8} {:<6} {:>12.5} {:>12.5} {:>8.4} {:>16} {:<20}",
            &p.position_id[..p.position_id.len().min(12)],
            p.symbol,
            p.side,
            p.entry_price,
            p.avg_exit_price,
            p.original_size,
            format_signed_currency(p.net_pnl, currency),
            reasons,
        );
    }
    if total_positions > show {
        println!("  ... and {} more positions", total_positions - show);
    }
}

fn provider_evaluation_value(
    result: &BacktestResultMsg,
) -> Result<&serde_json::Value, Box<dyn std::error::Error>> {
    let future = result
        .future
        .as_ref()
        .ok_or("provider evaluation is only available for FutureQuoteV1 results")?;
    if future.provider_evaluation.is_null() {
        return Err("FutureQuoteV1 result did not contain provider evaluation".into());
    }
    Ok(&future.provider_evaluation)
}

fn serialize_evaluation(
    result: &BacktestResultMsg,
    args: &Args,
) -> Result<String, Box<dyn std::error::Error>> {
    let evaluation = provider_evaluation_value(result)?;
    Ok(match args.evaluation_format {
        EvaluationJsonFormat::Json => serde_json::to_string(evaluation)?,
        EvaluationJsonFormat::JsonPretty => serde_json::to_string_pretty(evaluation)?,
    })
}

fn zero_trade_provider_result(
    args: &Args,
    source_coverage: Option<SourceCoverageCountsMsg>,
    request_symbol: &str,
    request_symbols: &[String],
) -> Result<BacktestResultMsg, Box<dyn std::error::Error>> {
    let evaluation = provider_evaluation_options(args, source_coverage);
    let registry = SymbolRegistry::empty();
    let mut evaluation_symbols = request_symbols
        .iter()
        .map(|symbol| registry.normalize_or_passthrough(symbol))
        .collect::<Vec<_>>();
    if !request_symbol.is_empty() {
        evaluation_symbols.push(registry.normalize_or_passthrough(request_symbol));
    }
    evaluation_symbols.extend(
        evaluation
            .filter
            .symbols
            .iter()
            .map(|symbol| registry.normalize_or_passthrough(symbol)),
    );
    evaluation_symbols.sort();
    evaluation_symbols.dedup();

    let options =
        evaluation_options_from_msg_for_symbols(&evaluation, &registry, &evaluation_symbols)?;
    let config_msg = BacktestConfigMsg {
        initial_balance: Some(args.balance),
        close_on_finish: Some(true),
        fill_model: Some("BidAsk".into()),
        sizing: None,
    };
    let future_msg = future_config_message(args);
    let account_currency = args
        .account_currency
        .as_deref()
        .ok_or("zero-trade FutureQuoteV1 reports require --account-currency")?;
    let identity_routes = BTreeMap::from([(
        account_currency.to_owned(),
        ConversionRoute::Identity {
            currency: account_currency.to_owned(),
        },
    )]);
    let currency_plan = RunCurrencyPlan::new(
        account_currency,
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeMap::new(),
        identity_routes,
        Vec::new(),
    )?;
    let config: BacktestConfig = config_from_msg(&config_msg, &registry, &[])?;
    let future_config = future_config_from_msg(&future_msg, currency_plan)?;
    let runner = BacktestRunner::new_future(config, future_config).with_evaluation_options(options);
    let mut feed = VecFeed::new(Vec::new());
    let result = runner.run_raw_signals_future(&mut feed, Vec::new(), None);
    Ok(result_to_msg(&result))
}

fn present_result(
    result: &BacktestResultMsg,
    args: &Args,
    output_written: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let currency = result_currency_code(result, args);
    match args.report {
        ReportMode::Standard => {
            print_result_summary(result, currency.as_deref());
            print_trade_log(
                &result.trade_log,
                result.total_trades,
                30,
                currency.as_deref(),
            );
            print_positions(
                &result.positions,
                result.total_positions,
                15,
                currency.as_deref(),
            );
        }
        ReportMode::Provider => {
            print_section("Provider Evaluation");
            println!("{}", serialize_evaluation(result, args)?);
        }
    }

    if let Some(ref output_path) = args.output
        && !output_written
    {
        let json = serde_json::to_vec(result)?;
        write_bytes_atomically(Path::new(output_path), &json)?;
        println!();
        println!("  Full result written to {}", output_path);
    } else if let Some(ref output_path) = args.output {
        println!();
        println!("  Full result written to {}", output_path);
    }
    if let Some(ref output_path) = args.evaluation_output {
        let json = serialize_evaluation(result, args)?;
        write_bytes_atomically(Path::new(output_path), json.as_bytes())?;
        println!();
        println!("  Provider evaluation written to {}", output_path);
    }
    Ok(())
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_evaluation_args(&args)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

    // 1. Load parsed signals from JSONL.
    print_header("Loading Parsed Signals");
    let mut raw_signals = load_raw_signals(&args.input)?;
    validate_loaded_signal_contract(&args, &raw_signals)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let loaded_signal_count = raw_signals.len();
    println!(
        "  Loaded {} raw signals from {}",
        loaded_signal_count, args.input
    );

    // Load source outcomes before deciding whether an empty signal stream is reportable.
    let source_coverage = args
        .outcomes_input
        .as_deref()
        .map(|path| load_source_coverage(path, &args.from, &args.to))
        .transpose()?;

    // 1b. Client-side date filtering (reduces payload; server also filters).
    raw_signals = filter_signals_by_date(raw_signals, &args.from, &args.to);
    println!("  After date filtering: {} signals", raw_signals.len());
    if let Some(coverage) = source_coverage {
        println!(
            "  Source coverage: raw={}, parsed={}, skipped={}, failed={}, emitted={}, entries={}",
            coverage.raw_messages,
            coverage.parsed_messages,
            coverage.skipped_messages,
            coverage.failed_messages,
            coverage.emitted_signals,
            coverage.emitted_entry_signals
        );
    }

    let (request_symbol, request_symbols, symbol_label) =
        resolve_cli_symbol_request(&args.symbol, &args.symbols, args.all_symbols)?;

    if raw_signals.is_empty() {
        if uses_provider_options(&args) {
            eprintln!("  No signals remain; producing a zero-trade provider/coverage report.");
            let result = zero_trade_provider_result(
                &args,
                source_coverage,
                &request_symbol,
                &request_symbols,
            )?;
            present_result(&result, &args, false)?;
            print_header("Done");
            return Ok(());
        }

        let message = if loaded_signal_count == 0 {
            format!(
                "no signals were loaded from '{}'; provide a non-empty parsed signal JSONL file or request a provider/coverage report",
                args.input
            )
        } else {
            format!(
                "date filters removed all {loaded_signal_count} loaded signals; adjust --from/--to or request a provider/coverage report"
            )
        };
        return Err(io::Error::new(io::ErrorKind::InvalidInput, message).into());
    }

    if args
        .account_currency
        .as_deref()
        .is_none_or(|currency| currency.trim().is_empty())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FutureQuoteV1 requires --account-currency",
        )
        .into());
    }

    // Show first few signals as preview.
    let preview = raw_signals.len().min(5);
    for s in &raw_signals[..preview] {
        println!("    {:?}", s);
    }
    if raw_signals.len() > preview {
        println!("    ... and {} more", raw_signals.len() - preview);
    }

    let endpoint = match args.endpoint.clone() {
        Some(endpoint) => endpoint,
        None => format!("shm://{}", args.shm_name).parse()?,
    };

    // 2. Connect to the backtest service.
    print_header("Connecting to Backtest Server");
    let mut session = connect(&endpoint).await?;
    let client = session.client();
    println!("  ✓ Connected");

    let operation_result: Result<(), Box<dyn std::error::Error>> = async {
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
            "  Profile: {:?}, Balance: {}, Raw signals: {}",
            args.profile,
            format_currency(args.balance, args.account_currency.as_deref()),
            raw_signals.len()
        );

        let sizing = sizing_policy(&args);

        let request = BacktestRunSpec {
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

        let future_request = RunBacktestRequest {
            request: request.clone(),
            future: future_config_message(&args),
            evaluation: provider_evaluation_options(&args, source_coverage),
            result_delivery: result_delivery_message(&args),
        };

        match execution_mode(&args) {
            ExecutionMode::Stream | ExecutionMode::Poll => {
                let submit: SubmitBacktestResponse = client
                    .call(
                        "submit_backtest",
                        &SubmitBacktestRequest {
                            request: future_request.clone(),
                        },
                    )
                    .await?;
                if !submit.success {
                    return Err(
                        server_response_error(submit.error, "backtest submission failed").into(),
                    );
                }
                let job_id = submit.job_id.ok_or_else(|| {
                    server_response_error(submit.error, "successful submission omitted job_id")
                })?;
                println!("  Job submitted: {job_id}");

                if execution_mode(&args) == ExecutionMode::Stream {
                    watch_backtest_with_reconnect(
                        &mut session,
                        &endpoint,
                        &job_id,
                        args.cancel_on_interrupt,
                    )
                    .await?;
                } else {
                    let status_client = client.clone();
                    let status_job_id = job_id.clone();
                    let cancel_client = client.clone();
                    let cancel_job_id = job_id.clone();
                    poll_async_job(
                        &job_id,
                        std::time::Duration::from_secs(args.poll_timeout_secs),
                        std::time::Duration::from_secs(2),
                        move |remaining| {
                            let client = status_client.clone();
                            let job_id = status_job_id.clone();
                            async move {
                                client
                                    .call_with_timeout(
                                        "get_backtest_status",
                                        &GetBacktestStatusRequest { job_id },
                                        remaining,
                                    )
                                    .await
                                    .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
                            }
                        },
                        move || {
                            let client = cancel_client.clone();
                            let job_id = cancel_job_id.clone();
                            async move {
                                best_effort_cancel_streamed_job(&client, &job_id).await;
                            }
                        },
                    )
                    .await?;
                }

                let result_client = session.client();
                let result_resp: GetBacktestResultResponse = result_client
                    .call(
                        "get_backtest_result",
                        &GetBacktestResultRequest {
                            job_id: job_id.clone(),
                        },
                    )
                    .await?;
                if !result_resp.success {
                    return Err(server_response_error(
                        result_resp.error,
                        "completed job result request failed",
                    )
                    .into());
                }
                let (result, output_written) = receive_delivered_result(
                    &result_client,
                    result_resp.result,
                    result_resp.artifact,
                    result_resp.inline_complete,
                    args.output.as_deref(),
                )
                .await?;
                present_result(&result, &args, output_written)?;
            }
            ExecutionMode::Sync => {
                let resp: RunBacktestResponse = client
                    .call_with_timeout(
                        "run_backtest",
                        &future_request,
                        std::time::Duration::from_secs(300),
                    )
                    .await?;
                println!("  Elapsed: {}ms", resp.elapsed_ms);

                if !resp.success {
                    return Err(server_response_error(resp.error, "backtest failed").into());
                }
                let (result, output_written) = receive_delivered_result(
                    &client,
                    resp.result,
                    resp.artifact,
                    resp.inline_complete,
                    args.output.as_deref(),
                )
                .await?;
                present_result(&result, &args, output_written)?;
            }
        }

        Ok(())
    }
    .await;

    if operation_result.is_ok() {
        print_header("Done");
    }
    let shutdown_result = session.close().await;

    match (operation_result, shutdown_result) {
        (Ok(()), Ok(())) => {
            println!("  ✓ Disconnected");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_json(risk_field: &str) -> String {
        format!(
            r#"{{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,{risk_field}"stoploss":null,"targets":[],"group":null,"trade_id":null}}"#
        )
    }

    #[test]
    fn strict_entry_jsonl_requires_risk_and_rejects_only_entry_size_with_line_numbers() {
        let parsed = parse_raw_signal_line(&entry_json(r#""risk":1.5,"#), 4).unwrap();
        assert!(matches!(parsed, RawSignalMsg::Entry { risk, .. } if risk == 1.5));

        let missing = parse_raw_signal_line(&entry_json(""), 7).unwrap_err();
        assert_eq!(missing.to_string(), "line 7: Entry requires field `risk`");

        let obsolete = parse_raw_signal_line(&entry_json(r#""size":0.1,"#), 9).unwrap_err();
        assert!(obsolete.to_string().contains("line 9"));
        assert!(
            obsolete
                .to_string()
                .contains("Entry field `size` is obsolete")
        );

        let scale_in = parse_raw_signal_line(
            r#"{"action":"ScaleIn","ts":"2026-01-15T10:00:00","position":{"type":"ByTradeId","trade_id":"trade-1"},"price":null,"size":0.1}"#,
            11,
        )
        .unwrap();
        assert!(matches!(scale_in, RawSignalMsg::ScaleIn { size, .. } if size == 0.1));
    }

    #[test]
    fn server_conversion_applies_shared_risk_and_scale_in_validation() {
        let registry = SymbolRegistry::empty();
        let invalid_entry = RawSignalMsg::Entry {
            ts: "2026-01-15T10:00:00".into(),
            symbol: "EURUSD".into(),
            side: "Buy".into(),
            order_type: "Market".into(),
            price: None,
            risk: 0.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: None,
        };
        let entry_error =
            backtest_server::convert::raw_signal_from_msg(&invalid_entry, "EURUSD", &registry)
                .unwrap_err();
        assert!(
            entry_error
                .to_string()
                .contains("entry risk multiplier must be finite and positive"),
            "{entry_error}"
        );

        let invalid_scale_in = RawSignalMsg::ScaleIn {
            ts: "2026-01-15T10:01:00".into(),
            position: PositionRefMsg::ByTradeId {
                trade_id: "trade-1".into(),
            },
            price: None,
            size: 0.0,
        };
        let scale_in_error =
            backtest_server::convert::raw_signal_from_msg(&invalid_scale_in, "EURUSD", &registry)
                .unwrap_err();
        assert!(
            scale_in_error
                .to_string()
                .contains("scale-in size/price is invalid"),
            "{scale_in_error}"
        );
    }

    #[test]
    fn sizing_options_are_mutually_exclusive_and_map_to_current_policies() {
        for (option, value, expected) in [
            ("--base-lot", "0.25", "fixed_lot"),
            ("--risk-per-trade", "125", "fixed_risk_amount"),
            ("--risk-percent", "1.5", "balance_risk_percent"),
        ] {
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                option,
                value,
            ])
            .unwrap();
            match (expected, sizing_policy(&args).unwrap()) {
                ("fixed_lot", SizingPolicyMsg::FixedLot { lots }) => assert_eq!(lots, 0.25),
                ("fixed_risk_amount", SizingPolicyMsg::FixedRiskAmount { amount }) => {
                    assert_eq!(amount, 125.0);
                }
                ("balance_risk_percent", SizingPolicyMsg::BalanceRiskPercent { percent }) => {
                    assert_eq!(percent, 1.5);
                }
                (expected, actual) => panic!("expected {expected}, got {actual:?}"),
            }
        }

        let conflict = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--base-lot",
            "0.1",
            "--risk-percent",
            "1",
        ])
        .unwrap_err();
        assert_eq!(conflict.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn entries_require_one_sizing_option_and_account_currency() {
        let entry = parse_raw_signal_line(&entry_json(r#""risk":1.0,"#), 1).unwrap();

        let no_sizing = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--account-currency",
            "USD",
        ])
        .unwrap();
        assert_eq!(
            validate_loaded_signal_contract(&no_sizing, std::slice::from_ref(&entry)),
            Err(
                "Entry signals require exactly one of --base-lot, --risk-per-trade, or --risk-percent"
                    .into()
            )
        );

        let no_currency = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--base-lot",
            "0.1",
        ])
        .unwrap();
        assert_eq!(
            validate_loaded_signal_contract(&no_currency, std::slice::from_ref(&entry)),
            Err("Entry signals require --account-currency".into())
        );

        let valid = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--risk-per-trade",
            "100",
            "--account-currency",
            "USD",
        ])
        .unwrap();
        assert_eq!(
            validate_loaded_signal_contract(&valid, std::slice::from_ref(&entry)),
            Ok(())
        );
    }

    #[test]
    fn future_quote_and_conversion_staleness_are_cli_defaults() {
        use clap::CommandFactory;

        let args = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
        ])
        .unwrap();
        assert_eq!(args.conversion_stale_after_ms, 300_000);
        assert_eq!(args.mtm_output, MtmOutputMode::Bounded);
        assert_eq!(args.mtm_max_points, None);
        assert_eq!(args.result_delivery, ResultDeliveryMode::Auto);
        assert_eq!(result_delivery_message(&args), ResultDeliveryMsg::Auto);
        assert_eq!(
            serde_json::to_value(result_delivery_message(&args)).unwrap(),
            serde_json::json!("auto")
        );
        let future = future_config_message(&args);
        assert_eq!(
            future.mtm_output,
            MtmOutputPolicyMsg::Bounded {
                max_points: DEFAULT_MTM_MAX_POINTS
            }
        );
        assert_eq!(
            serde_json::to_value(future).unwrap()["mtm_output"],
            serde_json::json!({ "bounded": { "max_points": 4_096 } })
        );

        let help = Args::command().render_long_help().to_string();
        for option in [
            "--base-lot",
            "--risk-per-trade",
            "--risk-percent",
            "--account-currency",
            "--conversion-stale-after-ms",
            "--mtm-output",
            "--mtm-max-points",
            "--result-delivery",
        ] {
            assert!(help.contains(option), "help omitted {option}: {help}");
        }
        assert!(!help.contains("--execution-convention"));
        assert!(help.contains("[default: bounded]"));
        assert!(help.contains("Default: 4096"));
        assert!(help.contains("--risk-per-trade 100 --account-currency USD"));
    }

    #[test]
    fn mtm_output_cli_maps_policies_and_validates_bounds() {
        for (option, expected) in [
            ("none", MtmOutputPolicyMsg::None),
            (
                "bounded",
                MtmOutputPolicyMsg::Bounded {
                    max_points: DEFAULT_MTM_MAX_POINTS,
                },
            ),
            ("full", MtmOutputPolicyMsg::Full),
        ] {
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                "--mtm-output",
                option,
                "--output",
                "result.json",
            ])
            .unwrap();
            validate_evaluation_args(&args).unwrap();
            assert_eq!(mtm_output_policy(&args), expected);
        }

        for max_points in [MIN_MTM_MAX_POINTS, MAX_MTM_MAX_POINTS] {
            let max_points = max_points.to_string();
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                "--mtm-output",
                "bounded",
                "--mtm-max-points",
                &max_points,
            ])
            .unwrap();
            validate_evaluation_args(&args).unwrap();
            assert_eq!(
                mtm_output_policy(&args),
                MtmOutputPolicyMsg::Bounded {
                    max_points: max_points.parse().unwrap()
                }
            );
        }

        for max_points in [MIN_MTM_MAX_POINTS - 1, MAX_MTM_MAX_POINTS + 1] {
            let max_points = max_points.to_string();
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                "--mtm-max-points",
                &max_points,
            ])
            .unwrap();
            let error = validate_evaluation_args(&args).unwrap_err();
            assert!(error.contains("--mtm-max-points must be between"));
        }

        for output in ["none", "full"] {
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                "--mtm-output",
                output,
                "--mtm-max-points",
                "64",
            ])
            .unwrap();
            assert_eq!(
                validate_evaluation_args(&args),
                Err("--mtm-max-points requires --mtm-output bounded".into())
            );
        }
    }

    #[test]
    fn full_mtm_rejects_forced_inline_delivery() {
        let no_output = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--mtm-output",
            "full",
        ])
        .unwrap();
        assert_eq!(
            validate_evaluation_args(&no_output),
            Err("--mtm-output full requires --output".into())
        );

        let args = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--mtm-output",
            "full",
            "--result-delivery",
            "inline",
        ])
        .unwrap();
        assert_eq!(
            validate_evaluation_args(&args),
            Err("--mtm-output full cannot be used with --result-delivery inline".into())
        );

        let artifact = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--mtm-output",
            "full",
            "--result-delivery",
            "artifact",
            "--output",
            "result.json",
        ])
        .unwrap();
        validate_evaluation_args(&artifact).unwrap();
        assert_eq!(
            result_delivery_message(&artifact),
            ResultDeliveryMsg::Artifact
        );
    }

    #[tokio::test]
    async fn artifact_download_helper_reconstructs_and_verifies_chunks() {
        use std::collections::VecDeque;

        let payload = br#"{"result":"complete","values":[1,2,3,4]}"#;
        let chunk_size = 9_usize;
        let reference = ResultArtifactRefMsg {
            format_version: RESULT_FORMAT_VERSION,
            artifact_id: "result_test_download".into(),
            byte_len: payload.len() as u64,
            sha256: backtest_server::artifact_store::sha256_hex(payload),
            chunk_size: chunk_size as u64,
        };
        let mut offset = 0_u64;
        let mut responses = VecDeque::new();
        for chunk in payload.chunks(chunk_size) {
            offset += chunk.len() as u64;
            responses.push_back(GetResultArtifactChunkResponse {
                success: true,
                artifact_id: reference.artifact_id.clone(),
                offset: offset - chunk.len() as u64,
                data_base64: BASE64_STANDARD.encode(chunk),
                eof: offset == payload.len() as u64,
                error: None,
            });
        }

        let mut requested_offsets = Vec::new();
        let mut reconstructed = Vec::new();
        download_artifact_chunks(&reference, &mut reconstructed, |request| {
            requested_offsets.push(request.offset);
            let response = responses.pop_front().unwrap();
            std::future::ready(Ok::<_, Box<dyn std::error::Error>>(response))
        })
        .await
        .unwrap();
        assert_eq!(reconstructed, payload);
        assert_eq!(requested_offsets, [0, 9, 18, 27, 36]);

        let output = std::env::temp_dir().join(format!(
            "tg_backtest_atomic_output_{}_{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        write_bytes_atomically(&output, payload).unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), payload);
        assert!(!artifact_part_path(&output).exists());
        let _ = std::fs::remove_file(output);
    }

    #[test]
    fn atomic_output_creates_missing_parent_directories() {
        let root = std::env::temp_dir().join(format!(
            "tg_backtest_missing_output_parent_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let output = root.join("nested/results/backtest.json");
        let payload = br#"{"success":true}"#;

        assert!(!root.exists());
        write_bytes_atomically(&output, payload).unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), payload);
        assert!(!artifact_part_path(&output).exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn artifact_download_helper_rejects_checksum_mismatch() {
        let payload = b"payload";
        let reference = ResultArtifactRefMsg {
            format_version: RESULT_FORMAT_VERSION,
            artifact_id: "result_bad_checksum".into(),
            byte_len: payload.len() as u64,
            sha256: "00".repeat(32),
            chunk_size: payload.len() as u64,
        };
        let mut output = Vec::new();
        let error = download_artifact_chunks(&reference, &mut output, |request| {
            std::future::ready(Ok::<_, Box<dyn std::error::Error>>(
                GetResultArtifactChunkResponse {
                    success: true,
                    artifact_id: request.artifact_id,
                    offset: request.offset,
                    data_base64: BASE64_STANDARD.encode(payload),
                    eof: true,
                    error: None,
                },
            ))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("SHA-256 mismatch"));
    }

    #[test]
    fn parses_provider_report_selection_and_repeatable_typed_filters() {
        let args = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--report",
            "provider",
            "--provider-id",
            "provider-7",
            "--source-id",
            "telegram:channel-3",
            "--report-sections",
            "coverage,position-performance,breakdowns",
            "--breakdown",
            "symbol",
            "--filter",
            "symbol=ES",
            "--filter",
            "symbol=NQ",
            "--filter",
            "side=long",
            "--filter",
            "group=trend",
            "--evaluation-output",
            "evaluation.json",
            "--max-breakdown-rows",
            "25",
            "--include-positions",
        ])
        .expect("provider CLI options parse");
        validate_evaluation_args(&args).expect("provider CLI options validate");

        assert_eq!(args.report, ReportMode::Provider);
        assert_eq!(args.provider_id.as_deref(), Some("provider-7"));
        assert_eq!(args.breakdown.len(), 1);
        assert_eq!(args.filter.len(), 4);
        assert_eq!(args.max_breakdown_rows, Some(25));
        let options = provider_evaluation_options(&args, None);
        assert_eq!(
            options.sections,
            [
                EvaluationSectionMsg::Coverage,
                EvaluationSectionMsg::PositionPerformance,
                EvaluationSectionMsg::Breakdowns,
            ]
        );
        assert_eq!(options.filter.symbols, ["ES", "NQ"]);
        assert_eq!(options.filter.sides, [EvaluationPositionSideMsg::Long]);
        assert_eq!(
            options.filter.groups,
            [EvaluationGroupFilterMsg::Named("trend".into())]
        );
        assert!(options.filter.tags.is_empty());
        assert_eq!(options.breakdowns, [BreakdownDimensionMsg::Symbol]);
        assert_eq!(options.source_coverage, None);
        assert_eq!(options.maximum_breakdown_rows, Some(25));
    }

    #[test]
    fn cli_rejects_unknown_provider_selectors() {
        let section_error = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--report-sections",
            "coverage,mystery",
        ])
        .expect_err("unknown report section should fail CLI parsing");
        assert_eq!(
            section_error.kind(),
            clap::error::ErrorKind::ValueValidation
        );
        assert!(section_error.to_string().contains("unknown report section"));

        let filter_error = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--filter",
            "mystery=value",
        ])
        .expect_err("unknown filter selector should fail CLI parsing");
        assert_eq!(filter_error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(filter_error.to_string().contains("unknown filter selector"));
    }

    #[test]
    fn cli_rejects_integrated_tag_filters_and_breakdowns() {
        let breakdown_error = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--breakdown",
            "tag:session",
        ])
        .expect_err("tag breakdowns must fail CLI parsing");
        assert_eq!(
            breakdown_error.kind(),
            clap::error::ErrorKind::ValueValidation
        );
        assert!(breakdown_error.to_string().contains("tag breakdowns"));

        let filter_error = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--filter",
            "tag:session=us",
        ])
        .expect_err("tag filters must fail CLI parsing");
        assert_eq!(filter_error.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(filter_error.to_string().contains("tag filters"));
    }

    /// RFC 3339 must parse, because parser outcome files carry `Z`-suffixed source
    /// timestamps per the parser framework's input contract. This failed in the field:
    /// `--outcomes-input` plus `--from`/`--to` rejected every real parser output while
    /// naive-timestamp fixtures passed.
    #[test]
    fn cli_timestamps_accept_rfc3339_and_normalize_offsets_to_utc() {
        // The exact shape emitted by the parser framework.
        assert_eq!(
            parse_cli_timestamp("2026-01-01T19:51:04Z"),
            Some(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(19, 51, 4)
                    .unwrap()
            )
        );
        // A non-UTC offset must normalize to UTC, matching parse_iso_datetime.
        assert_eq!(
            parse_cli_timestamp("2026-01-01T19:51:04+02:00"),
            Some(
                chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                    .unwrap()
                    .and_hms_opt(17, 51, 4)
                    .unwrap()
            )
        );
        // The pre-existing naive forms keep working.
        assert!(parse_cli_timestamp("2026-01-01T19:51:04").is_some());
        assert!(parse_cli_timestamp("2026-01-01 19:51:04").is_some());
        assert!(parse_cli_timestamp("2026-01-01").is_some());
        // Garbage still fails, so the invalid-timestamp error path is intact.
        assert!(parse_cli_timestamp("not-a-timestamp").is_none());
    }

    fn outcome_fixture_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "tg_backtest_{label}_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn load_outcome_error(label: &str, outcomes: &[serde_json::Value]) -> String {
        let path = outcome_fixture_path(label);
        let jsonl = outcomes
            .iter()
            .map(|outcome| serde_json::to_string(outcome).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, jsonl).unwrap();
        let error = load_source_coverage(path.to_str().unwrap(), &None, &None).unwrap_err();
        let _ = std::fs::remove_file(path);
        error.to_string()
    }

    fn skipped_outcome(msg_id: i64) -> serde_json::Value {
        serde_json::json!({
            "status": "skipped",
            "source": {
                "chat_id": 42,
                "msg_id": msg_id,
                "ts": "2026-01-15T10:00:00Z",
                "message": "fixture",
                "reply_to": null
            },
            "parser": "fixture",
            "reason": "parser_returned_skip"
        })
    }

    #[test]
    fn outcomes_jsonl_rejects_unknown_envelope_and_source_fields_and_duplicates() {
        let mut unknown_envelope = skipped_outcome(1);
        unknown_envelope["unexpected"] = serde_json::json!(true);
        let error = load_outcome_error("unknown_envelope", &[unknown_envelope]);
        assert!(error.contains("unknown field `unexpected`"), "{error}");

        let mut unknown_source = skipped_outcome(1);
        unknown_source["source"]["unexpected"] = serde_json::json!(true);
        let error = load_outcome_error("unknown_source", &[unknown_source]);
        assert!(error.contains("unknown field `unexpected`"), "{error}");

        let duplicate = skipped_outcome(1);
        let error = load_outcome_error("duplicate_source", &[duplicate.clone(), duplicate]);
        assert!(
            error.contains("outcomes line 2: duplicate source outcome"),
            "{error}"
        );
        assert!(error.contains("chat_id=42 msg_id=1"), "{error}");
    }

    #[test]
    fn outcomes_jsonl_treats_reason_and_failure_as_opaque_json() {
        let path = outcome_fixture_path("opaque_details");
        let mut skipped = skipped_outcome(1);
        skipped["reason"] = serde_json::json!(["provider", {"code": 7}]);
        let failed = serde_json::json!({
            "status": "failed",
            "source": {
                "chat_id": 42,
                "msg_id": 2,
                "ts": "2026-01-15T10:00:01Z",
                "message": "fixture",
                "reply_to": null
            },
            "parser": null,
            "failure": false
        });
        std::fs::write(&path, format!("{skipped}\n{failed}\n")).unwrap();

        let coverage = load_source_coverage(path.to_str().unwrap(), &None, &None).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(coverage.raw_messages, 2);
        assert_eq!(coverage.skipped_messages, 1);
        assert_eq!(coverage.failed_messages, 1);
    }

    #[test]
    fn outcomes_jsonl_supplies_typed_source_and_entry_coverage() {
        use qs_backtest::RawSignal;
        use qs_core::types::{OrderType, Side};

        let source = |msg_id, ts: &str| {
            serde_json::json!({
                "chat_id": 42,
                "msg_id": msg_id,
                "ts": ts,
                "message": format!("message {msg_id}"),
                "reply_to": null
            })
        };
        let signal_ts =
            NaiveDateTime::parse_from_str("2026-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S").unwrap();
        let outcomes = [
            serde_json::json!({
                "status": "parsed",
                "source": source(1, "2026-01-15T10:00:00"),
                "parser": "fixture",
                "signals": [
                    RawSignal::Entry {
                        ts: signal_ts,
                        symbol: "EURUSD".into(),
                        side: Side::Buy,
                        order_type: OrderType::Market,
                        price: None,
                        risk_multiplier: 1.0,
                        stoploss: None,
                        targets: Vec::new(),
                        group: None,
                        trade_id: Some("entry-1".into()),
                    },
                    RawSignal::CloseAll { ts: signal_ts },
                ]
            }),
            serde_json::json!({
                "status": "skipped",
                "source": source(2, "2026-01-15T10:00:01"),
                "parser": "fixture",
                "reason": "parser_returned_skip"
            }),
            serde_json::json!({
                "status": "failed",
                "source": source(3, "2026-01-15T10:00:02"),
                "parser": "fixture",
                "failure": {
                    "kind": "parser",
                    "reason": "invalid provider message"
                }
            }),
            serde_json::json!({
                "status": "failed",
                "source": source(4, "not-a-timestamp"),
                "parser": null,
                "failure": {
                    "kind": "invalid_timestamp",
                    "value": "not-a-timestamp",
                    "reason": "unsupported source timestamp"
                }
            }),
        ];
        let path = std::env::temp_dir().join(format!(
            "tg_backtest_outcomes_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let jsonl = outcomes
            .iter()
            .map(|outcome| serde_json::to_string(outcome).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, jsonl).unwrap();

        let coverage = load_source_coverage(path.to_str().unwrap(), &None, &None).unwrap();
        let date_filter_error =
            load_source_coverage(path.to_str().unwrap(), &Some("2026-01-01".into()), &None)
                .expect_err("an invalid source timestamp cannot be classified by date");
        let _ = std::fs::remove_file(path);

        assert!(
            date_filter_error
                .to_string()
                .contains("required for date filtering")
        );
        assert_eq!(coverage.raw_messages, 4);
        assert_eq!(coverage.parsed_messages, 1);
        assert_eq!(coverage.skipped_messages, 1);
        assert_eq!(coverage.failed_messages, 2);
        assert_eq!(coverage.emitted_signals, 2);
        assert_eq!(coverage.emitted_entry_signals, 1);

        let args = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--outcomes-input",
            "outcomes.jsonl",
            "--exchange",
            "test",
        ])
        .unwrap();
        let options = provider_evaluation_options(&args, Some(coverage));
        assert_eq!(options.source_coverage, Some(coverage));
    }

    #[test]
    fn outcomes_jsonl_date_filters_classify_valid_source_timestamps() {
        let path = std::env::temp_dir().join(format!(
            "tg_backtest_outcome_dates_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source = |msg_id, ts| {
            serde_json::json!({
                "status": "skipped",
                "source": {
                    "chat_id": 42,
                    "msg_id": msg_id,
                    "ts": ts,
                    "message": "fixture",
                    "reply_to": null
                },
                "parser": "fixture",
                "reason": "parser_returned_skip"
            })
        };
        std::fs::write(
            &path,
            format!(
                "{}\n{}\n",
                source(1, "2026-01-14T10:00:00"),
                source(2, "2026-01-15T10:00:00")
            ),
        )
        .unwrap();

        let coverage = load_source_coverage(
            path.to_str().unwrap(),
            &Some("2026-01-15".into()),
            &Some("2026-01-15T23:59:59".into()),
        )
        .unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(coverage.raw_messages, 1);
        assert_eq!(coverage.skipped_messages, 1);
    }

    fn async_status(status: &str, success: bool, error: Option<&str>) -> BacktestStatusResponse {
        BacktestStatusResponse {
            success,
            job_id: "job-test".into(),
            status: status.into(),
            error: error.map(str::to_owned),
            elapsed_ms: None,
            progress: BacktestProgress::default(),
        }
    }

    #[test]
    fn async_status_failures_preserve_server_errors_for_process_failure() {
        for (status, success, server_error) in [
            ("NotFound", false, "job expired"),
            ("Failed", true, "parquet read failed"),
            ("Cancelled", true, "operator cancelled job"),
        ] {
            let error = evaluate_async_status(&async_status(status, success, Some(server_error)))
                .expect_err("terminal failure must propagate as a main error");
            assert_eq!(error, server_error);
        }
        assert_eq!(
            evaluate_async_status(&async_status("Completed", true, None)),
            Ok(AsyncStatusDecision::Completed)
        );
        assert_eq!(
            evaluate_async_status(&async_status("Running", true, None)),
            Ok(AsyncStatusDecision::Continue)
        );
    }

    #[test]
    fn stream_events_preserve_job_identity_and_terminal_status_errors() {
        let running = BacktestEvent::Snapshot {
            status: async_status("Running", true, None),
        };
        assert!(matches!(
            evaluate_stream_event("job-test", &running),
            Ok(StreamEventDecision::Continue)
        ));

        let completed = BacktestEvent::Snapshot {
            status: async_status("Completed", true, None),
        };
        assert!(matches!(
            evaluate_stream_event("job-test", &completed),
            Ok(StreamEventDecision::Completed)
        ));

        let failed = BacktestEvent::Snapshot {
            status: async_status("Failed", true, Some("replay failed")),
        };
        assert_eq!(
            evaluate_stream_event("job-test", &failed).unwrap_err(),
            "replay failed"
        );

        let wrong_job = BacktestEvent::Heartbeat {
            job_id: "other-job".into(),
            elapsed_ms: 1,
        };
        assert!(
            evaluate_stream_event("job-test", &wrong_job)
                .unwrap_err()
                .contains("job mismatch")
        );
    }

    #[tokio::test]
    async fn poll_queries_already_completed_job_before_first_sleep() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let status_queries = Arc::new(AtomicUsize::new(0));
        let query_counter = status_queries.clone();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let cancellation_counter = cancellations.clone();
        let poll = poll_async_job(
            "job-completed",
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(2),
            move |_| {
                query_counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(async_status("Completed", true, None)) }
            },
            move || {
                let cancellation_counter = cancellation_counter.clone();
                async move {
                    cancellation_counter.fetch_add(1, Ordering::SeqCst);
                }
            },
        );

        tokio::select! {
            biased;
            result = poll => result.expect("completed job must be observed immediately"),
            () = std::future::ready(()) => panic!("polling slept before its first status query"),
        }
        assert_eq!(status_queries.load(Ordering::SeqCst), 1);
        assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn poll_observes_completed_job_when_timeout_is_shorter_than_interval() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let status_queries = Arc::new(AtomicUsize::new(0));
        let query_counter = status_queries.clone();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let cancellation_counter = cancellations.clone();
        let poll = poll_async_job(
            "job-completed",
            std::time::Duration::from_millis(1),
            std::time::Duration::from_secs(2),
            move |_| {
                query_counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(async_status("Completed", true, None)) }
            },
            move || {
                let cancellation_counter = cancellation_counter.clone();
                async move {
                    cancellation_counter.fetch_add(1, Ordering::SeqCst);
                }
            },
        );

        tokio::select! {
            biased;
            result = poll => result.expect("short timeout must still permit an immediate poll"),
            () = std::future::ready(()) => panic!("polling slept for the longer poll interval"),
        }
        assert_eq!(status_queries.load(Ordering::SeqCst), 1);
        assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn poll_failures_attempt_cancellation_and_preserve_the_original_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for (status, success, server_error) in [
            ("NotFound", false, "job expired"),
            ("Failed", true, "parquet read failed"),
            ("Cancelled", true, "operator cancelled job"),
        ] {
            let cancellations = Arc::new(AtomicUsize::new(0));
            let cancellation_counter = cancellations.clone();
            let response = async_status(status, success, Some(server_error));
            let error = poll_async_job(
                "job-test",
                std::time::Duration::from_secs(1),
                std::time::Duration::ZERO,
                move |_| {
                    let response = response.clone();
                    async move { Ok(response) }
                },
                move || {
                    let cancellation_counter = cancellation_counter.clone();
                    async move {
                        cancellation_counter.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await
            .expect_err("terminal status must fail polling");
            assert_eq!(error.to_string(), server_error);
            assert_eq!(cancellations.load(Ordering::SeqCst), 1);
        }

        let cancellations = Arc::new(AtomicUsize::new(0));
        let cancellation_counter = cancellations.clone();
        let error = poll_async_job(
            "job-timeout",
            std::time::Duration::from_millis(5),
            std::time::Duration::from_millis(1),
            |_| async { Ok(async_status("Running", true, None)) },
            move || {
                let cancellation_counter = cancellation_counter.clone();
                async move {
                    cancellation_counter.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await
        .expect_err("poll timeout must fail");
        assert!(error.to_string().contains("Timed out waiting"));
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);

        let cancellations = Arc::new(AtomicUsize::new(0));
        let cancellation_counter = cancellations.clone();
        let error = poll_async_job(
            "job-transport",
            std::time::Duration::from_secs(1),
            std::time::Duration::ZERO,
            |_| async {
                Err(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "status transport failed",
                )) as Box<dyn std::error::Error>)
            },
            move || {
                let cancellation_counter = cancellation_counter.clone();
                async move {
                    cancellation_counter.fetch_add(1, Ordering::SeqCst);
                }
            },
        )
        .await
        .expect_err("status transport failure must fail polling");
        assert_eq!(error.to_string(), "status transport failed");
        assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cli_defaults_to_stream_and_retains_poll_and_sync_fallbacks() {
        let args = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
        ])
        .unwrap();
        assert_eq!(execution_mode(&args), ExecutionMode::Stream);
        assert_eq!(args.poll_timeout_secs, 300);
        assert!(!args.cancel_on_interrupt);

        for (mode, expected) in [
            ("stream", ExecutionMode::Stream),
            ("poll", ExecutionMode::Poll),
            ("sync", ExecutionMode::Sync),
        ] {
            let args = Args::try_parse_from([
                "tg_backtest",
                "--input",
                "signals.jsonl",
                "--exchange",
                "test",
                "--execution-mode",
                mode,
            ])
            .unwrap();
            assert_eq!(execution_mode(&args), expected);
        }

        let legacy = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--async",
        ])
        .unwrap();
        assert_eq!(execution_mode(&legacy), ExecutionMode::Poll);

        let zero = Args::try_parse_from([
            "tg_backtest",
            "--input",
            "signals.jsonl",
            "--exchange",
            "test",
            "--poll-timeout-secs",
            "0",
        ])
        .unwrap();
        assert_eq!(
            validate_evaluation_args(&zero),
            Err("--poll-timeout-secs must be positive".into())
        );
    }

    #[test]
    fn response_error_keeps_the_exact_server_message() {
        let error = server_response_error(Some("server rejected request".into()), "fallback");
        assert_eq!(error.to_string(), "server rejected request");
    }

    #[test]
    fn renders_fractional_percentages_and_currency_codes() {
        assert_eq!(render_percentage(0.1234, 2), "12.34%");
        assert_eq!(render_percentage(0.375, 1), "37.5%");
        assert_eq!(format_currency(12.5, Some("usd")), "USD 12.50");
        assert_eq!(format_signed_currency(12.5, Some("EUR")), "EUR +12.50");
        assert!(!format_currency(12.5, Some("USD")).contains('$'));
    }
}
