//! Logical request, response, and event types for the backtest service.
//!
//! All structs use serde and remain independent of the selected transport/provider codec.
//! Enums are represented as strings at the service boundary for readability where retained compatibility requires it.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

fn default_true() -> bool {
    true
}

/// Current format version for complete result JSON artifacts.
pub const RESULT_FORMAT_VERSION: u32 = 1;

// ── Ping ────────────────────────────────────────────────────────────────────

/// Health check response with server status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResponse {
    pub status: String,
    pub uptime_secs: u64,
    pub data_dir: String,
}

// ── List Profiles ───────────────────────────────────────────────────────────

/// Summary of a management profile available on the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileInfo {
    pub name: String,
    pub use_targets: Vec<usize>,
    pub close_ratios: Vec<f64>,
    pub stoploss_mode: String,
    pub rules_count: usize,
    pub let_remainder_run: bool,
}

/// Response listing all loaded management profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListProfilesResponse {
    pub profiles: Vec<ProfileInfo>,
}

// ── List Symbols ────────────────────────────────────────────────────────────

/// Request to list available data, optionally filtered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSymbolsRequest {
    pub exchange: Option<String>,
    pub data_type: Option<String>,
}

/// One row of data availability info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolAvailability {
    pub exchange: String,
    pub symbol: String,
    pub data_type: String,
    pub timeframe: Option<String>,
    pub row_count: u64,
    pub earliest: String,
    pub latest: String,
}

/// Response listing available market data in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSymbolsResponse {
    pub symbols: Vec<SymbolAvailability>,
}

// ── Backtest Config ─────────────────────────────────────────────────────────

/// Serializable backtest configuration sent by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfigMsg {
    /// Starting account balance. Default: 10000.0
    pub initial_balance: Option<f64>,
    /// Whether to force-close open positions at end of data. Default: true
    pub close_on_finish: Option<bool>,
    /// Fill model: "BidAsk", "AskOnly", or "MidPrice". Default: "BidAsk"
    pub fill_model: Option<String>,
    /// Account sizing policy. Required when the request contains an Entry signal.
    #[serde(default)]
    pub sizing: Option<SizingPolicyMsg>,
}

/// Wire-safe in-place account sizing policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum SizingPolicyMsg {
    /// Use a fixed lot quantity scaled by the entry risk multiplier.
    FixedLot { lots: f64 },
    /// Risk a fixed amount in account currency.
    FixedRiskAmount { amount: f64 },
    /// Risk a percentage of the realized balance before the entry.
    BalanceRiskPercent { percent: f64 },
}

// ── Async Job API (Issue 2) ─────────────────────────────────────────────────

/// Response from submitting a backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitBacktestResponse {
    pub success: bool,
    pub job_id: Option<String>,
    pub error: Option<String>,
}

/// Request the current status of a backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBacktestStatusRequest {
    pub job_id: String,
}

/// Subscribe to status snapshots for a retained backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchBacktestRequest {
    pub job_id: String,
}

/// Structured progress for an asynchronous backtest job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BacktestProgress {
    pub stage: String,
    pub processed_events: u64,
    pub total_events: u64,
    pub processed_signals: u64,
    pub total_signals: u64,
    pub processed_symbols: u64,
    pub total_symbols: u64,
}

/// Status of a backtest job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BacktestStatusResponse {
    pub success: bool,
    pub job_id: String,
    pub status: String,
    pub error: Option<String>,
    pub elapsed_ms: Option<u64>,
    /// Missing in older responses; defaults to an empty progress snapshot.
    #[serde(default)]
    pub progress: BacktestProgress,
}

impl BacktestStatusResponse {
    /// Whether this snapshot represents a terminal retained-job state.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status.as_str(), "Completed" | "Failed" | "Cancelled")
    }
}

/// Server-streamed retained-job status and liveness events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BacktestEvent {
    Snapshot { status: BacktestStatusResponse },
    Heartbeat { job_id: String, elapsed_ms: u64 },
}

/// Request the result of a completed backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBacktestResultRequest {
    pub job_id: String,
}

/// Response containing the result of a completed backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBacktestResultResponse {
    pub success: bool,
    pub job_id: String,
    pub result: Option<BacktestResultMsg>,
    pub error: Option<String>,
    #[serde(default)]
    pub artifact: Option<ResultArtifactRefMsg>,
    #[serde(default = "default_true")]
    pub inline_complete: bool,
    /// True when an async result artifact was deleted after delivery.
    #[serde(default)]
    pub artifact_consumed: bool,
}

/// Request cancellation of a backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelBacktestRequest {
    pub job_id: String,
}

/// Response from cancelling a backtest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelBacktestResponse {
    pub success: bool,
    pub job_id: String,
    pub error: Option<String>,
}

// ── FutureQuote execution ───────────────────────────────────────────────────

/// Requested delivery mode for a complete result JSON payload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultDeliveryMsg {
    #[default]
    Auto,
    Inline,
    Artifact,
}

/// Reference to a complete result JSON artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultArtifactRefMsg {
    pub format_version: u32,
    pub artifact_id: String,
    pub byte_len: u64,
    pub sha256: String,
    pub chunk_size: u64,
}

/// Request a raw byte chunk from a result artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetResultArtifactChunkRequest {
    pub artifact_id: String,
    pub offset: u64,
}

/// Base64-encoded raw artifact bytes at the requested offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetResultArtifactChunkResponse {
    pub success: bool,
    pub artifact_id: String,
    pub offset: u64,
    pub data_base64: String,
    pub eof: bool,
    pub error: Option<String>,
}

/// Delete a result artifact after a successful download.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteResultArtifactRequest {
    pub artifact_id: String,
}

/// Response from deleting a result artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResultArtifactResponse {
    pub success: bool,
    pub artifact_id: String,
    pub error: Option<String>,
}

/// Wire-safe mark-to-market output retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MtmOutputPolicyMsg {
    None,
    Bounded { max_points: usize },
    Full,
}

impl Default for MtmOutputPolicyMsg {
    fn default() -> Self {
        Self::Bounded { max_points: 4_096 }
    }
}

/// FutureQuoteV1 execution settings used by the backtest RPC methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FutureQuoteConfigMsg {
    pub signal_latency_ms: i64,
    pub slippage_pips: f64,
    pub stale_quote_after_ms: Option<i64>,
    pub pnl_epsilon: f64,
    pub account_currency: String,
    pub conversion_stale_after_ms: i64,
    pub mtm_output: MtmOutputPolicyMsg,
}

impl Default for FutureQuoteConfigMsg {
    fn default() -> Self {
        Self {
            signal_latency_ms: 0,
            slippage_pips: 0.0,
            stale_quote_after_ms: None,
            pnl_epsilon: 1.0e-9,
            account_currency: String::new(),
            conversion_stale_after_ms: 300_000,
            mtm_output: MtmOutputPolicyMsg::default(),
        }
    }
}

/// Provider/source identifiers carried into the evaluation result.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvaluationContextMsg {
    pub provider_id: Option<String>,
    pub source_id: Option<String>,
}

/// Selectable provider-evaluation report sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationSectionMsg {
    Coverage,
    PositionPerformance,
    RMetrics,
    Excursions,
    Execution,
    Robustness,
    Breakdowns,
}

impl EvaluationSectionMsg {
    pub const ALL: [Self; 7] = [
        Self::Coverage,
        Self::PositionPerformance,
        Self::RMetrics,
        Self::Excursions,
        Self::Execution,
        Self::Robustness,
        Self::Breakdowns,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationPositionSideMsg {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationGroupFilterMsg {
    Named(String),
    Ungrouped,
}

/// Typed position filters. Values within a field are ORed; populated fields and
/// individual tag keys are ANDed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PositionFilterMsg {
    pub symbols: Vec<String>,
    pub sides: Vec<EvaluationPositionSideMsg>,
    pub groups: Vec<EvaluationGroupFilterMsg>,
    pub close_reasons: Vec<String>,
    pub tags: BTreeMap<String, Vec<String>>,
}

/// Typed deterministic breakdown selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakdownDimensionMsg {
    Symbol,
    Side,
    Group,
    CloseReason,
    Tag(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BootstrapConfigMsg {
    pub samples: usize,
    pub confidence_level: f64,
    pub seed: u64,
    pub minimum_sample_size: usize,
}

impl Default for BootstrapConfigMsg {
    fn default() -> Self {
        Self {
            samples: 2_000,
            confidence_level: 0.95,
            seed: 0xA076_1D64_78BD_642F,
            minimum_sample_size: 5,
        }
    }
}

/// Typed parser/source coverage supplied by clients that retain parse outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceCoverageCountsMsg {
    pub raw_messages: u64,
    pub parsed_messages: u64,
    pub skipped_messages: u64,
    pub failed_messages: u64,
    pub emitted_signals: u64,
    pub emitted_entry_signals: u64,
}

/// Typed provider-evaluation selection for canonical requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProviderEvaluationOptionsMsg {
    pub context: EvaluationContextMsg,
    pub source_coverage: Option<SourceCoverageCountsMsg>,
    /// Missing selectors request all sections; an explicit empty list requests none.
    pub sections: Vec<EvaluationSectionMsg>,
    pub filter: PositionFilterMsg,
    pub breakdowns: Vec<BreakdownDimensionMsg>,
    pub bootstrap: BootstrapConfigMsg,
    pub rolling_window: usize,
    pub minimum_breakdown_bucket_count: usize,
    pub maximum_breakdown_rows: Option<usize>,
    pub include_positions: bool,
    pub maximum_position_rows: Option<usize>,
}

impl Default for ProviderEvaluationOptionsMsg {
    fn default() -> Self {
        Self {
            context: EvaluationContextMsg::default(),
            source_coverage: None,
            sections: EvaluationSectionMsg::ALL.to_vec(),
            filter: PositionFilterMsg::default(),
            breakdowns: Vec::new(),
            bootstrap: BootstrapConfigMsg::default(),
            rolling_window: 20,
            minimum_breakdown_bucket_count: 1,
            maximum_breakdown_rows: None,
            include_positions: false,
            maximum_position_rows: None,
        }
    }
}

// ── Run Backtest ────────────────────────────────────────────────────────────

/// Execution scope and inputs for one backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestRunSpec {
    pub symbol: String,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub all_symbols: bool,
    pub exchange: String,
    pub data_type: String,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// Full signal stream (entry + management).
    #[serde(default)]
    pub raw_signals: Vec<RawSignalMsg>,
    pub profile: Option<String>,
    #[serde(default)]
    pub profile_def: Option<ManagementProfileMsg>,
    pub config: BacktestConfigMsg,
}

/// Response from a single backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunBacktestResponse {
    pub success: bool,
    pub error: Option<String>,
    /// Complete inline result or compact summary when `inline_complete` is false.
    pub result: Option<BacktestResultMsg>,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub artifact: Option<ResultArtifactRefMsg>,
    #[serde(default = "default_true")]
    pub inline_complete: bool,
}

/// Strict FutureQuoteV1 request accepted by `run_backtest`.
#[derive(Debug, Clone, Serialize)]
pub struct RunBacktestRequest {
    pub request: BacktestRunSpec,
    #[serde(default)]
    pub future: FutureQuoteConfigMsg,
    #[serde(default)]
    pub evaluation: ProviderEvaluationOptionsMsg,
    #[serde(default)]
    pub result_delivery: ResultDeliveryMsg,
}

/// Asynchronous FutureQuoteV1 submission.
#[derive(Debug, Clone, Serialize)]
pub struct SubmitBacktestRequest {
    pub request: RunBacktestRequest,
}

// ── Run Backtest Multi ──────────────────────────────────────────────────────

/// Execution scope and inputs for a multi-profile comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestMultiRunSpec {
    pub symbol: String,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub all_symbols: bool,
    pub exchange: String,
    pub data_type: String,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    /// Full signal stream (entry + management).
    #[serde(default)]
    pub raw_signals: Vec<RawSignalMsg>,
    pub profiles: Vec<ProfileRef>,
    pub config: BacktestConfigMsg,
}

/// Result for one profile in a multi-profile comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileResult {
    pub profile: String,
    pub success: bool,
    pub error: Option<String>,
    pub result: Option<BacktestResultMsg>,
}

/// Response from a multi-profile comparison run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunBacktestMultiResponse {
    /// Defaults to true when decoding responses from older servers.
    #[serde(default = "default_true")]
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    /// Complete inline results or compact summaries when `inline_complete` is false.
    pub results: Vec<ProfileResult>,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub artifact: Option<ResultArtifactRefMsg>,
    #[serde(default = "default_true")]
    pub inline_complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunBacktestMultiRequest {
    pub request: BacktestMultiRunSpec,
    #[serde(default)]
    pub future: FutureQuoteConfigMsg,
    #[serde(default)]
    pub evaluation: ProviderEvaluationOptionsMsg,
    #[serde(default)]
    pub result_delivery: ResultDeliveryMsg,
}

// ── Backtest Result Message ─────────────────────────────────────────────────

/// Serializable mirror of `BacktestResult` for wire transport.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResultMsg {
    pub initial_balance: f64,
    pub final_balance: f64,
    pub total_pnl: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,

    // Extended result breakdowns.
    pub summary: SubsetStatsMsg,
    pub per_symbol: HashMap<String, SubsetStatsMsg>,
    pub per_group: HashMap<String, SubsetStatsMsg>,
    pub long_stats: SubsetStatsMsg,
    pub short_stats: SubsetStatsMsg,
    pub per_close_reason: Vec<CloseReasonStatsMsg>,
    pub streaks: StreakStatsMsg,
    pub risk_metrics: RiskMetricsMsg,
    pub duration_stats: Option<DurationStatsMsg>,
    pub monthly_returns: Vec<MonthlyReturnMsg>,

    pub equity_curve: Vec<EquityPoint>,
    pub trade_log: Vec<TradeResultMsg>,

    // Position-level aggregation
    pub positions: Vec<PositionSummaryMsg>,
    pub total_positions: usize,
    pub winning_positions: usize,
    pub losing_positions: usize,
    pub position_win_rate: f64,

    /// Additive FutureQuoteV1 artifacts.
    #[serde(default)]
    pub future: Option<FutureBacktestResultMsg>,
}

/// Wire-safe mark-to-market output counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MtmOutputSummaryMsg {
    pub policy: MtmOutputPolicyMsg,
    pub observed_points: u64,
    pub retained_points: u64,
    pub omitted_points: u64,
}

impl Default for MtmOutputSummaryMsg {
    fn default() -> Self {
        Self {
            policy: MtmOutputPolicyMsg::Full,
            observed_points: 0,
            retained_points: 0,
            omitted_points: 0,
        }
    }
}

/// Format-versioned execution/accounting payload. Complex internal records remain
/// JSON values so new additive fields do not require synchronized positional
/// wire changes; the transport itself uses JsonCodec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FutureBacktestResultMsg {
    pub format_version: u32,
    pub execution_metadata: serde_json::Value,
    pub recorded_fills: serde_json::Value,
    pub action_dispositions: serde_json::Value,
    pub close_events: serde_json::Value,
    pub completed_positions: serde_json::Value,
    pub open_positions: serde_json::Value,
    pub pending_orders: serde_json::Value,
    /// Typed FutureQuote pending-order transition stream.
    #[serde(default)]
    pub pending_order_lifecycle: Vec<PendingOrderLifecycleEventMsg>,
    pub mtm_equity_curve: serde_json::Value,
    #[serde(default)]
    pub mtm_output_summary: MtmOutputSummaryMsg,
    pub mtm_max_drawdown: Option<f64>,
    pub mtm_max_drawdown_pct: Option<f64>,
    pub provider_evaluation: serde_json::Value,
}

/// Wire-safe state for one pending-order lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PendingOrderLifecycleStateMsg {
    #[default]
    Placed,
    Filled,
    Cancelled,
    UnfilledAtEnd,
}

/// Typed wire mirror of a FutureQuote pending-order lifecycle event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PendingOrderLifecycleEventMsg {
    pub id: String,
    pub sequence: u64,
    pub position_id: String,
    pub placement_action_id: Option<String>,
    pub terminal_action_id: Option<String>,
    pub state: PendingOrderLifecycleStateMsg,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub requested_size: f64,
    pub filled_size: Option<f64>,
    pub requested_price: Option<f64>,
    pub fill_price: Option<f64>,
    pub signal_ts: Option<String>,
    pub placed_ts: Option<String>,
    pub effective_ts: Option<String>,
    pub terminal_ts: Option<String>,
    pub wait_latency_ms: Option<i64>,
    pub fill_ratio: Option<f64>,
}

// ── Sub-message types ───────────────────────────────────────────────────────

/// Wire-safe mirror of `SubsetStats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsetStatsMsg {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub breakeven_trades: usize,
    pub total_pnl: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub win_loss_ratio: f64,
    pub expectancy: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
}

/// Wire-safe mirror of `StreakStats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreakStatsMsg {
    pub max_consecutive_wins: u32,
    pub max_consecutive_losses: u32,
    pub current_streak: i32,
}

/// Wire-safe mirror of `RiskMetrics`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetricsMsg {
    pub sharpe_ratio: Option<f64>,
    pub sortino_ratio: Option<f64>,
    pub calmar_ratio: Option<f64>,
    pub return_on_max_drawdown: Option<f64>,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
    pub max_drawdown_duration_secs: Option<i64>,
}

/// Wire-safe mirror of `DurationStats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationStatsMsg {
    pub avg_duration_secs: i64,
    pub min_duration_secs: i64,
    pub max_duration_secs: i64,
    pub avg_winner_duration_secs: i64,
    pub avg_loser_duration_secs: i64,
}

/// Wire-safe mirror of `MonthlyReturn`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonthlyReturnMsg {
    pub year: i32,
    pub month: u32,
    pub pnl: f64,
    pub trade_count: usize,
    pub ending_balance: f64,
}

/// Wire-safe mirror of `CloseReasonStats`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseReasonStatsMsg {
    pub reason: String,
    pub count: usize,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub percentage: f64,
}

/// Wire-safe mirror of `PositionSummary`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummaryMsg {
    pub position_id: String,
    pub symbol: String,
    pub side: String,
    pub group: Option<String>,
    pub entry_price: f64,
    pub avg_exit_price: f64,
    pub original_size: f64,
    pub close_count: usize,
    pub net_pnl: f64,
    pub close_reasons: Vec<String>,
    pub open_ts: String,
    pub final_close_ts: Option<String>,
    pub duration_seconds: i64,
}

/// A single point on the equity curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPoint {
    pub ts: String,
    pub balance: f64,
}

/// Wire-safe mirror of `TradeResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResultMsg {
    pub position_id: String,
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub pnl: f64,
    pub open_ts: String,
    pub close_ts: String,
    pub close_reason: String,
    pub group: Option<String>,
}

// Dynamic management profile messages.

/// Wire-safe strict target selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetSelectionMsg {
    /// Use every target supplied by the entry signal, in signal order.
    All,
    /// Do not attach any targets.
    None,
    /// Use the listed 1-based signal target indices, in the listed order.
    Selected(Vec<usize>),
}

/// Wire-safe management profile definition sent inline with a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementProfileMsg {
    pub name: String,
    /// Explicit strict selection. This wins over `use_targets` when present;
    /// omission preserves the legacy `use_targets`-derived default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_selection: Option<TargetSelectionMsg>,
    /// Compatibility selection retained for older serialized profiles.
    pub use_targets: Vec<usize>,
    pub close_ratios: Vec<f64>,
    #[serde(default)]
    pub stoploss_mode: Option<StoplossModeMsg>,
    #[serde(default)]
    pub rules: Vec<RuleConfigDefMsg>,
    #[serde(default)]
    pub group_override: Option<String>,
    #[serde(default)]
    pub let_remainder_run: bool,
}

/// Wire-safe stoploss mode enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StoplossModeMsg {
    FromSignal,
    None,
    FixedDistance { distance: f64 },
    FixedPrice { price: f64 },
}

/// Wire-safe rule configuration definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RuleConfigDefMsg {
    FixedStoploss { price: f64 },
    TrailingStop { distance: f64 },
    TakeProfit { price: f64, close_ratio: f64 },
    BreakevenWhen { trigger_price: f64 },
    BreakevenWhenOffset { trigger_price_offset: f64 },
    BreakevenAfterTargets { after_n: u32 },
    TimeExit { max_seconds: u64 },
}

/// A profile reference: either by name or inline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProfileRef {
    Named(String),
    Inline(ManagementProfileMsg),
}

// Full raw-signal action messages.

/// Wire-safe signal that can represent any action, not just entries.
///
/// Uses `#[serde(tag = "action")]` so each variant is distinguished by an
/// `"action"` field in the JSON/Bincode representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub enum RawSignalMsg {
    /// Open a new position (same semantics as `RawSignalEntryMsg`).
    Entry {
        ts: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        risk: f64,
        stoploss: Option<f64>,
        #[serde(default)]
        targets: Vec<f64>,
        #[serde(default)]
        group: Option<String>,
        /// Application-defined trade id. Required for `ByTradeId` resolution.
        #[serde(default)]
        trade_id: Option<String>,
    },
    /// Close position(s) at market.
    Close {
        ts: String,
        position: PositionRefMsg,
    },
    /// Close a fraction of position(s).
    ClosePartial {
        ts: String,
        position: PositionRefMsg,
        ratio: f64,
    },
    /// Set or replace the stoploss price.
    ModifyStoploss {
        ts: String,
        position: PositionRefMsg,
        price: f64,
    },
    /// Move stoploss to the average entry price.
    MoveStoplossToEntry {
        ts: String,
        position: PositionRefMsg,
    },
    /// Add a take-profit level.
    AddTarget {
        ts: String,
        position: PositionRefMsg,
        price: f64,
        close_ratio: f64,
    },
    /// Remove a take-profit level at a specific price.
    RemoveTarget {
        ts: String,
        position: PositionRefMsg,
        price: f64,
    },
    /// Atomically change an existing take-profit price while retaining its ratio.
    ModifyTarget {
        ts: String,
        position: PositionRefMsg,
        old_price: f64,
        new_price: f64,
    },
    /// Attach a management rule.
    AddRule {
        ts: String,
        position: PositionRefMsg,
        rule: RuleConfigDefMsg,
    },
    /// Remove a management rule by name.
    RemoveRule {
        ts: String,
        position: PositionRefMsg,
        rule_name: String,
    },
    /// Scale into an existing position.
    ScaleIn {
        ts: String,
        position: PositionRefMsg,
        price: Option<f64>,
        size: f64,
    },
    /// Cancel a pending order.
    CancelPending {
        ts: String,
        position: PositionRefMsg,
    },
    /// Close all open positions on a symbol.
    CloseAllOf { ts: String, symbol: String },
    /// Close all open positions.
    CloseAll { ts: String },
    /// Cancel all pending orders.
    CancelAllPending { ts: String },
    /// Modify stoploss for all open positions on a symbol.
    ModifyAllStoploss {
        ts: String,
        symbol: String,
        price: f64,
    },
    /// Close all open positions in a group.
    CloseAllInGroup { ts: String, group_id: String },
    /// Modify stoploss for all open positions in a group.
    ModifyAllStoplossInGroup {
        ts: String,
        group_id: String,
        price: f64,
    },
}

impl RawSignalMsg {
    /// Extract the timestamp string from any variant.
    pub fn ts(&self) -> &str {
        match self {
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
            | RawSignalMsg::ModifyAllStoplossInGroup { ts, .. } => ts,
        }
    }
}

/// Wire-safe position reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PositionRefMsg {
    /// Target the position with the given application-defined trade id.
    ByTradeId { trade_id: String },
    /// All open positions on this symbol.
    AllOnSymbol { symbol: String },
    /// All open positions in this group.
    AllInGroup { group_id: String },
}

// ── Strict request decoding ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum StrictSizingPolicyMsg {
    FixedLot { lots: f64 },
    FixedRiskAmount { amount: f64 },
    BalanceRiskPercent { percent: f64 },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictBacktestConfigMsg {
    initial_balance: Option<f64>,
    close_on_finish: Option<bool>,
    fill_model: Option<String>,
    #[serde(default)]
    sizing: Option<StrictSizingPolicyMsg>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum StrictStoplossModeMsg {
    FromSignal,
    None,
    FixedDistance { distance: f64 },
    FixedPrice { price: f64 },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum StrictRuleConfigDefMsg {
    FixedStoploss { price: f64 },
    TrailingStop { distance: f64 },
    TakeProfit { price: f64, close_ratio: f64 },
    BreakevenWhen { trigger_price: f64 },
    BreakevenWhenOffset { trigger_price_offset: f64 },
    BreakevenAfterTargets { after_n: u32 },
    TimeExit { max_seconds: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictManagementProfileMsg {
    name: String,
    #[serde(default)]
    target_selection: Option<TargetSelectionMsg>,
    use_targets: Vec<usize>,
    close_ratios: Vec<f64>,
    #[serde(default)]
    stoploss_mode: Option<StrictStoplossModeMsg>,
    #[serde(default)]
    rules: Vec<StrictRuleConfigDefMsg>,
    #[serde(default)]
    group_override: Option<String>,
    #[serde(default)]
    let_remainder_run: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum StrictPositionRefMsg {
    ByTradeId { trade_id: String },
    AllOnSymbol { symbol: String },
    AllInGroup { group_id: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
enum StrictRawSignalMsg {
    Entry {
        ts: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        risk: f64,
        stoploss: Option<f64>,
        #[serde(default)]
        targets: Vec<f64>,
        #[serde(default)]
        group: Option<String>,
        #[serde(default)]
        trade_id: Option<String>,
    },
    Close {
        ts: String,
        position: StrictPositionRefMsg,
    },
    ClosePartial {
        ts: String,
        position: StrictPositionRefMsg,
        ratio: f64,
    },
    ModifyStoploss {
        ts: String,
        position: StrictPositionRefMsg,
        price: f64,
    },
    MoveStoplossToEntry {
        ts: String,
        position: StrictPositionRefMsg,
    },
    AddTarget {
        ts: String,
        position: StrictPositionRefMsg,
        price: f64,
        close_ratio: f64,
    },
    RemoveTarget {
        ts: String,
        position: StrictPositionRefMsg,
        price: f64,
    },
    ModifyTarget {
        ts: String,
        position: StrictPositionRefMsg,
        old_price: f64,
        new_price: f64,
    },
    AddRule {
        ts: String,
        position: StrictPositionRefMsg,
        rule: StrictRuleConfigDefMsg,
    },
    RemoveRule {
        ts: String,
        position: StrictPositionRefMsg,
        rule_name: String,
    },
    ScaleIn {
        ts: String,
        position: StrictPositionRefMsg,
        price: Option<f64>,
        size: f64,
    },
    CancelPending {
        ts: String,
        position: StrictPositionRefMsg,
    },
    CloseAllOf {
        ts: String,
        symbol: String,
    },
    CloseAll {
        ts: String,
    },
    CancelAllPending {
        ts: String,
    },
    ModifyAllStoploss {
        ts: String,
        symbol: String,
        price: f64,
    },
    CloseAllInGroup {
        ts: String,
        group_id: String,
    },
    ModifyAllStoplossInGroup {
        ts: String,
        group_id: String,
        price: f64,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictBacktestRunSpec {
    symbol: String,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default)]
    all_symbols: bool,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    from: Option<String>,
    to: Option<String>,
    #[serde(default)]
    raw_signals: Vec<StrictRawSignalMsg>,
    profile: Option<String>,
    #[serde(default)]
    profile_def: Option<StrictManagementProfileMsg>,
    config: StrictBacktestConfigMsg,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StrictProfileRef {
    Named(String),
    Inline(StrictManagementProfileMsg),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictBacktestMultiRunSpec {
    symbol: String,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default)]
    all_symbols: bool,
    exchange: String,
    data_type: String,
    timeframe: Option<String>,
    from: Option<String>,
    to: Option<String>,
    #[serde(default)]
    raw_signals: Vec<StrictRawSignalMsg>,
    profiles: Vec<StrictProfileRef>,
    config: StrictBacktestConfigMsg,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRunBacktestRequest {
    request: StrictBacktestRunSpec,
    #[serde(default)]
    future: FutureQuoteConfigMsg,
    #[serde(default)]
    evaluation: ProviderEvaluationOptionsMsg,
    #[serde(default)]
    result_delivery: ResultDeliveryMsg,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictSubmitBacktestRequest {
    request: RunBacktestRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRunBacktestMultiRequest {
    request: StrictBacktestMultiRunSpec,
    #[serde(default)]
    future: FutureQuoteConfigMsg,
    #[serde(default)]
    evaluation: ProviderEvaluationOptionsMsg,
    #[serde(default)]
    result_delivery: ResultDeliveryMsg,
}

fn strict_into_wire<T>(value: impl Serialize) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    serde_json::from_value(serde_json::to_value(value)?)
}

/// Error returned by strict standalone raw-signal decoding.
#[derive(Debug, Error)]
pub enum RawSignalDecodeError {
    #[error("invalid strict raw signal JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Entry risk must be finite and positive")]
    InvalidEntryRisk,
}

/// Decode one standalone raw signal with recursively strict nested fields.
pub fn decode_raw_signal_json_strict(input: &str) -> Result<RawSignalMsg, RawSignalDecodeError> {
    let mut deserializer = serde_json::Deserializer::from_str(input);
    let strict = StrictRawSignalMsg::deserialize(&mut deserializer)?;
    deserializer.end()?;
    let signal = strict_into_wire(strict)?;
    if let RawSignalMsg::Entry { risk, .. } = &signal
        && (!risk.is_finite() || *risk <= 0.0)
    {
        return Err(RawSignalDecodeError::InvalidEntryRisk);
    }
    Ok(signal)
}

impl<'de> Deserialize<'de> for RunBacktestRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let strict = StrictRunBacktestRequest::deserialize(deserializer)?;
        Ok(Self {
            request: strict_into_wire(strict.request).map_err(serde::de::Error::custom)?,
            future: strict.future,
            evaluation: strict.evaluation,
            result_delivery: strict.result_delivery,
        })
    }
}

impl<'de> Deserialize<'de> for SubmitBacktestRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let strict = StrictSubmitBacktestRequest::deserialize(deserializer)?;
        Ok(Self {
            request: strict.request,
        })
    }
}

impl<'de> Deserialize<'de> for RunBacktestMultiRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let strict = StrictRunBacktestMultiRequest::deserialize(deserializer)?;
        Ok(Self {
            request: strict_into_wire(strict.request).map_err(serde::de::Error::custom)?,
            future: strict.future,
            evaluation: strict.evaluation,
            result_delivery: strict.result_delivery,
        })
    }
}

// ── Phase 2: Profile Management ─────────────────────────────────────────────

/// Request to add (or overwrite) a management profile at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddProfileRequest {
    pub profile: ManagementProfileMsg,
    #[serde(default)]
    pub overwrite: bool,
}

/// Response from adding a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddProfileResponse {
    pub success: bool,
    pub error: Option<String>,
    pub profile_count: usize,
}

/// Request to remove a management profile by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveProfileRequest {
    pub name: String,
}

/// Response from removing a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveProfileResponse {
    pub success: bool,
    pub error: Option<String>,
    pub profile_count: usize,
}

/// Response from reloading profiles from disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReloadProfilesResponse {
    pub success: bool,
    pub error: Option<String>,
    pub profile_count: usize,
    pub loaded_from: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry_fields() -> serde_json::Value {
        json!({
            "action": "Entry",
            "ts": "2026-01-02T10:00:00",
            "symbol": "eurusd",
            "side": "Buy",
            "order_type": "Market",
            "price": null,
            "risk": 1.5,
            "stoploss": 1.08
        })
    }

    #[test]
    fn entry_requires_risk_and_rejects_size() {
        let entry: RawSignalMsg = serde_json::from_value(entry_fields()).unwrap();
        assert!(matches!(entry, RawSignalMsg::Entry { risk, .. } if risk == 1.5));

        let mut with_size = entry_fields();
        with_size.as_object_mut().unwrap().remove("risk");
        with_size["size"] = json!(0.1);
        let error = serde_json::from_value::<RawSignalMsg>(with_size).unwrap_err();
        assert!(error.to_string().contains("unknown field `size`"));

        let mut missing_risk = entry_fields();
        missing_risk.as_object_mut().unwrap().remove("risk");
        let error = serde_json::from_value::<RawSignalMsg>(missing_risk).unwrap_err();
        assert!(error.to_string().contains("missing field `risk`"));
    }

    #[test]
    fn scale_in_keeps_size_field() {
        let signal: RawSignalMsg = serde_json::from_value(json!({
            "action": "ScaleIn",
            "ts": "2026-01-02T10:01:00",
            "position": { "type": "ByTradeId", "trade_id": "trade-1" },
            "price": null,
            "size": 0.25
        }))
        .unwrap();
        assert!(matches!(signal, RawSignalMsg::ScaleIn { size, .. } if size == 0.25));
    }

    #[test]
    fn current_sizing_and_future_fields_are_strict() {
        let sizing: SizingPolicyMsg = serde_json::from_value(json!({
            "type": "FixedRiskAmount",
            "amount": 100.0
        }))
        .unwrap();
        assert!(matches!(
            sizing,
            SizingPolicyMsg::FixedRiskAmount { amount } if amount == 100.0
        ));
        assert!(
            serde_json::from_value::<SizingPolicyMsg>(json!({
                "type": "FixedLot",
                "qty": "all=0.01"
            }))
            .is_err()
        );

        let future: FutureQuoteConfigMsg = serde_json::from_value(json!({
            "account_currency": "USD",
            "conversion_stale_after_ms": 15_000
        }))
        .unwrap();
        assert_eq!(future.account_currency, "USD");
        assert_eq!(future.conversion_stale_after_ms, 15_000);
        assert_eq!(
            future.mtm_output,
            MtmOutputPolicyMsg::Bounded { max_points: 4_096 }
        );
        assert!(
            serde_json::from_value::<FutureQuoteConfigMsg>(json!({
                "account_currency": "USD",
                "conversion_rates": {}
            }))
            .is_err()
        );
    }

    #[test]
    fn mtm_output_policy_uses_strict_snake_case_wire_values() {
        for (value, expected) in [
            (json!("none"), MtmOutputPolicyMsg::None),
            (
                json!({ "bounded": { "max_points": 512 } }),
                MtmOutputPolicyMsg::Bounded { max_points: 512 },
            ),
            (json!("full"), MtmOutputPolicyMsg::Full),
        ] {
            let policy: MtmOutputPolicyMsg = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(policy, expected);
            assert_eq!(serde_json::to_value(policy).unwrap(), value);
        }

        assert!(
            serde_json::from_value::<MtmOutputPolicyMsg>(json!({
                "bounded": { "max_points": 512, "unexpected": true }
            }))
            .is_err()
        );
    }

    #[test]
    fn future_result_defaults_missing_mtm_output_summary() {
        let result: FutureBacktestResultMsg = serde_json::from_value(json!({
            "format_version": 1,
            "execution_metadata": null,
            "recorded_fills": null,
            "action_dispositions": null,
            "close_events": null,
            "completed_positions": null,
            "open_positions": null,
            "pending_orders": null,
            "pending_order_lifecycle": [],
            "mtm_equity_curve": null,
            "mtm_max_drawdown": null,
            "mtm_max_drawdown_pct": null,
            "provider_evaluation": null
        }))
        .unwrap();

        assert_eq!(result.mtm_output_summary, MtmOutputSummaryMsg::default());
        assert_eq!(result.mtm_output_summary.policy, MtmOutputPolicyMsg::Full);

        let summary: MtmOutputSummaryMsg = serde_json::from_value(json!({
            "policy": "full",
            "observed_points": 4,
            "retained_points": 4,
            "omitted_points": 0,
            "future_addition": true
        }))
        .unwrap();
        assert_eq!(summary.policy, MtmOutputPolicyMsg::Full);
    }

    fn minimal_request_json() -> serde_json::Value {
        json!({
            "request": {
                "symbol": "EURUSD",
                "exchange": "fixture",
                "data_type": "tick",
                "timeframe": null,
                "from": null,
                "to": null,
                "raw_signals": [],
                "profile": null,
                "config": {
                    "initial_balance": null,
                    "close_on_finish": null,
                    "fill_model": null
                }
            },
            "future": {
                "account_currency": "USD"
            }
        })
    }

    #[test]
    fn omitted_delivery_and_mtm_fields_use_current_defaults() {
        let request: RunBacktestRequest = serde_json::from_value(minimal_request_json()).unwrap();
        assert_eq!(request.result_delivery, ResultDeliveryMsg::Auto);
        assert_eq!(
            request.future.mtm_output,
            MtmOutputPolicyMsg::Bounded { max_points: 4_096 }
        );

        let submitted: SubmitBacktestRequest = serde_json::from_value(json!({
            "request": minimal_request_json()
        }))
        .unwrap();
        assert_eq!(submitted.request.result_delivery, ResultDeliveryMsg::Auto);
        assert_eq!(
            submitted.request.future.mtm_output,
            MtmOutputPolicyMsg::Bounded { max_points: 4_096 }
        );

        let mut explicit = minimal_request_json();
        explicit["result_delivery"] = json!("auto");
        explicit["future"]["mtm_output"] = json!({
            "bounded": { "max_points": 4_096 }
        });
        let explicit: RunBacktestRequest = serde_json::from_value(explicit).unwrap();
        assert_eq!(explicit.result_delivery, ResultDeliveryMsg::Auto);
        assert_eq!(
            explicit.future.mtm_output,
            MtmOutputPolicyMsg::Bounded { max_points: 4_096 }
        );

        let mut multi_request = minimal_request_json()["request"].clone();
        let fields = multi_request.as_object_mut().unwrap();
        fields.remove("profile");
        fields.insert("profiles".into(), json!([]));
        let multi: RunBacktestMultiRequest = serde_json::from_value(json!({
            "request": multi_request,
            "future": { "account_currency": "USD" }
        }))
        .unwrap();
        assert_eq!(multi.result_delivery, ResultDeliveryMsg::Auto);
        assert_eq!(
            multi.future.mtm_output,
            MtmOutputPolicyMsg::Bounded { max_points: 4_096 }
        );
    }

    #[test]
    fn older_async_result_responses_default_artifact_consumption_state() {
        let response: GetBacktestResultResponse = serde_json::from_value(json!({
            "success": true,
            "job_id": "job-old",
            "result": null,
            "error": null,
            "inline_complete": true
        }))
        .unwrap();
        assert!(!response.artifact_consumed);
    }
}
