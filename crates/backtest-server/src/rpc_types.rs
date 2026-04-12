//! RPC message types for the backtest server.
//!
//! All structs use serde for Bincode serialization over shared memory.
//! Enums are represented as strings at the RPC boundary for readability.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ── Connection Handshake ────────────────────────────────────────────────────

/// Client sends this to the acceptor endpoint to get a dedicated slot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectRequest {
    pub client_name: String,
}

/// Server responds with the assigned client ID and dedicated SHM slot name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectResponse {
    pub client_id: usize,
    pub slot_name: String,
}

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
}

// ── Raw Signal Entry (RPC transport version) ────────────────────────────────

/// A raw trade signal as sent over the wire.
///
/// Uses strings for enums (side, order_type) and ISO strings for timestamps.
/// The server converts these to the internal typed representations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSignalEntryMsg {
    pub ts: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: Option<f64>,
    pub size: f64,
    pub stoploss: Option<f64>,
    #[serde(default)]
    pub targets: Vec<f64>,
    #[serde(default)]
    pub group: Option<String>,
}

// ── Run Backtest ────────────────────────────────────────────────────────────

/// Request to execute a single backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunBacktestRequest {
    pub symbol: String,
    pub exchange: String,
    pub data_type: String,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub signals: Vec<RawSignalEntryMsg>,
    /// Full signal stream (entry + management). When non-empty, takes
    /// precedence over `signals`. Backward-compatible: existing clients
    /// that omit this field get an empty vec via `#[serde(default)]`.
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
    pub result: Option<BacktestResultMsg>,
    pub elapsed_ms: u64,
}

// ── Run Backtest Multi ──────────────────────────────────────────────────────

/// Request to compare multiple profiles on the same data and signals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunBacktestMultiRequest {
    pub symbol: String,
    pub exchange: String,
    pub data_type: String,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub signals: Vec<RawSignalEntryMsg>,
    /// Full signal stream (entry + management). When non-empty, takes
    /// precedence over `signals`.
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
    pub results: Vec<ProfileResult>,
    pub elapsed_ms: u64,
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

    // F08 enhanced fields
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

// ── Dynamic Profiles (F13) ──────────────────────────────────────────────────

/// Wire-safe management profile definition sent inline with a request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementProfileMsg {
    pub name: String,
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

// ── Full Signal Actions (F14) ───────────────────────────────────────────────

/// Wire-safe signal that can represent any action, not just entries.
///
/// Uses `#[serde(tag = "action")]` so each variant is distinguished by an
/// `"action"` field in the JSON/Bincode representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum RawSignalMsg {
    /// Open a new position (same semantics as `RawSignalEntryMsg`).
    Entry {
        ts: String,
        symbol: String,
        side: String,
        order_type: String,
        price: Option<f64>,
        size: f64,
        stoploss: Option<f64>,
        #[serde(default)]
        targets: Vec<f64>,
        #[serde(default)]
        group: Option<String>,
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

/// Wire-safe position reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PositionRefMsg {
    /// Explicit position ID.
    Id { id: String },
    /// The most recently opened position on this symbol.
    LastOnSymbol { symbol: String },
    /// The most recently opened position in this group.
    LastInGroup { group_id: String },
    /// All open positions on this symbol.
    AllOnSymbol { symbol: String },
    /// All open positions in this group.
    AllInGroup { group_id: String },
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
