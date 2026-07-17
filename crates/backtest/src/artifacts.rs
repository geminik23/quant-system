//! Additive, serializable artifacts for future backtest runners.
//!
//! These types deliberately consume normalized runner observations rather than
//! depending on `TradeEngine`. This keeps artifact collection usable by the
//! current engine, future execution paths, and external replay integrations.

use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use qs_core::{
    CloseReason, EffectiveStop, ExecutionFill, ExecutionModel, OrderType, PriceQuote, Side,
};
use serde::{Deserialize, Serialize};

use crate::currency::{ConversionResult, RunCurrencyPlan};
use crate::ledger::LifecycleLedger;
use crate::mtm::MtmOutputSummary;
use crate::portfolio::EquityPoint;

/// Current on-disk schema version for [`FutureBacktestArtifacts`].
pub const ARTIFACT_SCHEMA_VERSION: u32 = 2;

/// Default absolute tolerance used to classify net P&L as breakeven.
pub const DEFAULT_PNL_EPSILON: f64 = 1.0e-9;

fn default_schema_version() -> u32 {
    ARTIFACT_SCHEMA_VERSION
}

fn default_pnl_epsilon() -> f64 {
    DEFAULT_PNL_EPSILON
}

/// Reproducibility metadata for the execution/accounting model used by a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ExecutionMetadata {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    pub run_id: Option<String>,
    pub execution_model: ExecutionModel,
    pub initial_balance: f64,
    pub account_currency: Option<String>,
    /// Immutable primary/conversion universe, P&L currencies, routes, and warmup quotes.
    pub currency_plan: Option<RunCurrencyPlan>,
    /// Monetary point-value multiplier by symbol.
    pub contract_sizes: BTreeMap<String, f64>,
    /// Quotes older than this at an equity observation are counted as stale.
    pub stale_quote_after_millis: Option<i64>,
    #[serde(default = "default_pnl_epsilon")]
    pub pnl_epsilon: f64,
    /// Application-defined, deterministically ordered metadata.
    pub tags: BTreeMap<String, String>,
}

impl Default for ExecutionMetadata {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            run_id: None,
            execution_model: ExecutionModel::default(),
            initial_balance: 0.0,
            account_currency: None,
            currency_plan: None,
            contract_sizes: BTreeMap::new(),
            stale_quote_after_millis: None,
            pnl_epsilon: DEFAULT_PNL_EPSILON,
            tags: BTreeMap::new(),
        }
    }
}

/// Return a stable, human-readable event id for a scoped zero-based sequence.
///
/// Determinism comes from caller-supplied stable scope and sequence values; no
/// process-randomized hash, wall clock, or global counter is used.
pub fn deterministic_event_id(scope: &str, kind: &str, sequence: u64) -> String {
    format!("{scope}:{kind}:{sequence:08}")
}

/// An execution fill enriched with all timing and quote context needed for an
/// execution audit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordedFill {
    pub id: String,
    #[serde(default)]
    pub action_id: Option<String>,
    pub position_id: String,
    pub symbol: String,
    /// Source signal time. Engine-generated exits may have no source signal.
    #[serde(default)]
    pub signal_ts: Option<NaiveDateTime>,
    /// Time at which the action became eligible for execution.
    pub effective_ts: NaiveDateTime,
    /// Timestamp at which the fill changed execution/account state.
    #[serde(default)]
    pub execution_ts: Option<NaiveDateTime>,
    /// Timestamp of the source quote used by the execution pricer.
    pub quote_ts: NaiveDateTime,
    /// Age of the source quote at execution. Older payloads omit this field.
    #[serde(default)]
    pub quote_age_millis: Option<i64>,
    pub size: f64,
    pub bid: f64,
    pub ask: f64,
    /// Price-selection and slippage result from `qs_core`.
    pub fill: ExecutionFill,
}

impl RecordedFill {
    #[allow(clippy::too_many_arguments)]
    pub fn from_quote(
        position_id: impl Into<String>,
        action_id: Option<String>,
        sequence: u64,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        size: f64,
        quote: &PriceQuote,
        fill: ExecutionFill,
    ) -> Self {
        Self::from_quote_at(
            position_id,
            action_id,
            sequence,
            signal_ts,
            effective_ts,
            quote.ts,
            size,
            quote,
            fill,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_quote_at(
        position_id: impl Into<String>,
        action_id: Option<String>,
        sequence: u64,
        signal_ts: Option<NaiveDateTime>,
        effective_ts: NaiveDateTime,
        execution_ts: NaiveDateTime,
        size: f64,
        quote: &PriceQuote,
        fill: ExecutionFill,
    ) -> Self {
        let position_id = position_id.into();
        Self {
            id: deterministic_event_id(&position_id, "fill", sequence),
            action_id,
            position_id,
            symbol: quote.symbol.clone(),
            signal_ts,
            effective_ts,
            execution_ts: Some(execution_ts),
            quote_ts: quote.ts,
            quote_age_millis: Some(
                execution_ts
                    .signed_duration_since(quote.ts)
                    .num_milliseconds(),
            ),
            size,
            bid: quote.bid,
            ask: quote.ask,
            fill,
        }
    }
}

/// A realized close, including partial closes, in additive account currency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CloseEvent {
    pub id: String,
    pub action_id: Option<String>,
    pub fill_id: Option<String>,
    pub position_id: String,
    pub symbol: String,
    pub side: Side,
    pub ts: NaiveDateTime,
    pub size: f64,
    pub price: f64,
    /// Remaining-inventory average cost used to realize this close.
    #[serde(default)]
    pub entry_price: Option<f64>,
    pub pnl: f64,
    #[serde(default)]
    pub native_pnl: Option<f64>,
    #[serde(default)]
    pub native_currency: Option<String>,
    #[serde(default)]
    pub pnl_conversion: Option<ConversionResult>,
    pub reason: CloseReason,
    /// Remaining size after this close when known.
    pub remaining_size: Option<f64>,
}

impl Default for CloseEvent {
    fn default() -> Self {
        Self {
            id: String::new(),
            action_id: None,
            fill_id: None,
            position_id: String::new(),
            symbol: String::new(),
            side: Side::Buy,
            ts: NaiveDateTime::default(),
            size: 0.0,
            price: 0.0,
            entry_price: None,
            pnl: 0.0,
            native_pnl: None,
            native_currency: None,
            pnl_conversion: None,
            reason: CloseReason::Manual,
            remaining_size: None,
        }
    }
}

impl CloseEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        position_id: impl Into<String>,
        sequence: u64,
        symbol: impl Into<String>,
        side: Side,
        ts: NaiveDateTime,
        size: f64,
        price: f64,
        pnl: f64,
        reason: CloseReason,
    ) -> Self {
        let position_id = position_id.into();
        Self {
            id: deterministic_event_id(&position_id, "close", sequence),
            position_id,
            symbol: symbol.into(),
            side,
            ts,
            size,
            price,
            pnl,
            native_pnl: Some(pnl),
            reason,
            ..Self::default()
        }
    }
}

/// Availability and validity of a position's initial monetary risk basis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RiskBasisStatus {
    Available,
    /// Some, but not all, entry tranches have a valid risk basis.
    Partial,
    #[default]
    MissingStop,
    InvalidInput,
    NonProtectiveStop,
    ZeroRisk,
}

/// Initial risk attached to one entry/scale-in tranche.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskTranche {
    pub fill_id: Option<String>,
    pub size: f64,
    pub entry_price: f64,
    pub initial_stop: Option<f64>,
    pub contract_size: f64,
    pub risk_per_unit: Option<f64>,
    pub risk_amount: Option<f64>,
    #[serde(default)]
    pub native_risk_amount: Option<f64>,
    #[serde(default)]
    pub native_currency: Option<String>,
    #[serde(default)]
    pub risk_conversion: Option<ConversionResult>,
    pub status: RiskBasisStatus,
}

impl Default for RiskTranche {
    fn default() -> Self {
        Self {
            fill_id: None,
            size: 0.0,
            entry_price: 0.0,
            initial_stop: None,
            contract_size: 1.0,
            risk_per_unit: None,
            risk_amount: None,
            native_risk_amount: None,
            native_currency: None,
            risk_conversion: None,
            status: RiskBasisStatus::MissingStop,
        }
    }
}

impl RiskTranche {
    pub fn calculate(
        fill_id: Option<String>,
        side: Side,
        size: f64,
        entry_price: f64,
        initial_stop: Option<f64>,
        contract_size: f64,
        epsilon: f64,
    ) -> Self {
        let mut tranche = Self {
            fill_id,
            size,
            entry_price,
            initial_stop,
            contract_size,
            ..Self::default()
        };
        let epsilon = normalized_epsilon(epsilon);

        if !size.is_finite()
            || size <= 0.0
            || !entry_price.is_finite()
            || !contract_size.is_finite()
            || contract_size <= 0.0
        {
            tranche.status = RiskBasisStatus::InvalidInput;
            return tranche;
        }

        let Some(stop) = initial_stop else {
            return tranche;
        };
        if !stop.is_finite() {
            tranche.status = RiskBasisStatus::InvalidInput;
            return tranche;
        }

        let signed_distance = match side {
            Side::Buy => entry_price - stop,
            Side::Sell => stop - entry_price,
        };
        if signed_distance < -epsilon {
            tranche.status = RiskBasisStatus::NonProtectiveStop;
            return tranche;
        }
        if signed_distance.abs() <= epsilon {
            tranche.status = RiskBasisStatus::ZeroRisk;
            tranche.risk_per_unit = Some(0.0);
            tranche.risk_amount = Some(0.0);
            tranche.native_risk_amount = Some(0.0);
            return tranche;
        }

        tranche.status = RiskBasisStatus::Available;
        tranche.risk_per_unit = Some(signed_distance);
        let native_risk = signed_distance * size * contract_size;
        tranche.risk_amount = Some(native_risk);
        tranche.native_risk_amount = Some(native_risk);
        tranche
    }
}

/// Epsilon-aware classification of additive net P&L.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NetPnlOutcome {
    Win,
    Loss,
    #[default]
    Breakeven,
}

/// Complete campaign-level outcome for one position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompletedPosition {
    pub position_id: String,
    pub symbol: String,
    pub side: Side,
    pub group: Option<String>,
    pub trade_id: Option<String>,
    pub open_ts: NaiveDateTime,
    pub close_ts: NaiveDateTime,
    pub entry_size: f64,
    pub average_entry_price: f64,
    pub net_pnl: f64,
    #[serde(default)]
    pub native_net_pnl: Option<f64>,
    #[serde(default)]
    pub native_currency: Option<String>,
    pub outcome: NetPnlOutcome,
    #[serde(default = "default_pnl_epsilon")]
    pub pnl_epsilon: f64,
    pub initial_stop: Option<f64>,
    pub effective_stop: Option<EffectiveStop>,
    pub risk_basis_status: RiskBasisStatus,
    pub risk_tranches: Vec<RiskTranche>,
    /// Net P&L divided by total valid initial monetary risk.
    pub realized_r: Option<f64>,
    /// Minimum campaign P&L observed from a zero baseline (normally <= 0).
    pub mae: Option<f64>,
    /// Maximum campaign P&L observed from a zero baseline (normally >= 0).
    pub mfe: Option<f64>,
    /// Distinct close reasons in first-observed order.
    pub close_reasons: Vec<CloseReason>,
    pub close_events: Vec<CloseEvent>,
}

impl Default for CompletedPosition {
    fn default() -> Self {
        Self {
            position_id: String::new(),
            symbol: String::new(),
            side: Side::Buy,
            group: None,
            trade_id: None,
            open_ts: NaiveDateTime::default(),
            close_ts: NaiveDateTime::default(),
            entry_size: 0.0,
            average_entry_price: 0.0,
            net_pnl: 0.0,
            native_net_pnl: None,
            native_currency: None,
            outcome: NetPnlOutcome::Breakeven,
            pnl_epsilon: DEFAULT_PNL_EPSILON,
            initial_stop: None,
            effective_stop: None,
            risk_basis_status: RiskBasisStatus::MissingStop,
            risk_tranches: Vec::new(),
            realized_r: None,
            mae: None,
            mfe: None,
            close_reasons: Vec::new(),
            close_events: Vec::new(),
        }
    }
}

impl CompletedPosition {
    #[allow(clippy::too_many_arguments)]
    pub fn from_close_events(
        position_id: impl Into<String>,
        symbol: impl Into<String>,
        side: Side,
        open_ts: NaiveDateTime,
        close_ts: NaiveDateTime,
        entry_size: f64,
        average_entry_price: f64,
        initial_stop: Option<f64>,
        effective_stop: Option<EffectiveStop>,
        risk_tranches: Vec<RiskTranche>,
        close_events: Vec<CloseEvent>,
        mae: Option<f64>,
        mfe: Option<f64>,
        epsilon: f64,
    ) -> Self {
        let epsilon = normalized_epsilon(epsilon);
        let net_pnl = close_events.iter().map(|event| event.pnl).sum();
        let native_net_pnl = close_events.iter().try_fold(0.0, |total, event| {
            event.native_pnl.map(|native_pnl| total + native_pnl)
        });
        let native_currency = close_events
            .first()
            .and_then(|event| event.native_currency.clone())
            .filter(|currency| {
                close_events
                    .iter()
                    .all(|event| event.native_currency.as_ref() == Some(currency))
            });
        let close_reasons = distinct_close_reasons(&close_events);
        let (risk_basis_status, initial_risk) = summarize_risk(&risk_tranches, epsilon);
        let realized_r = initial_risk
            .filter(|risk| *risk > epsilon)
            .map(|risk| net_pnl / risk);

        Self {
            position_id: position_id.into(),
            symbol: symbol.into(),
            side,
            open_ts,
            close_ts,
            entry_size,
            average_entry_price,
            net_pnl,
            native_net_pnl,
            native_currency,
            outcome: Self::classify(net_pnl, epsilon),
            pnl_epsilon: epsilon,
            initial_stop,
            effective_stop,
            risk_basis_status,
            risk_tranches,
            realized_r,
            mae,
            mfe,
            close_reasons,
            close_events,
            ..Self::default()
        }
    }

    pub fn classify(net_pnl: f64, epsilon: f64) -> NetPnlOutcome {
        let epsilon = normalized_epsilon(epsilon);
        if net_pnl > epsilon {
            NetPnlOutcome::Win
        } else if net_pnl < -epsilon {
            NetPnlOutcome::Loss
        } else {
            NetPnlOutcome::Breakeven
        }
    }

    pub fn initial_risk(&self) -> Option<f64> {
        summarize_risk(&self.risk_tranches, self.pnl_epsilon).1
    }
}

fn normalized_epsilon(epsilon: f64) -> f64 {
    if epsilon.is_finite() {
        epsilon.abs()
    } else {
        DEFAULT_PNL_EPSILON
    }
}

fn distinct_close_reasons(events: &[CloseEvent]) -> Vec<CloseReason> {
    let mut reasons = Vec::new();
    for event in events {
        if !reasons.contains(&event.reason) {
            reasons.push(event.reason);
        }
    }
    reasons
}

fn summarize_risk(tranches: &[RiskTranche], epsilon: f64) -> (RiskBasisStatus, Option<f64>) {
    if tranches.is_empty() {
        return (RiskBasisStatus::MissingStop, None);
    }

    let available = tranches
        .iter()
        .filter(|tranche| tranche.status == RiskBasisStatus::Available)
        .count();
    if available == tranches.len() {
        let total: f64 = tranches
            .iter()
            .filter_map(|tranche| tranche.risk_amount)
            .sum();
        if !total.is_finite() {
            return (RiskBasisStatus::InvalidInput, None);
        }
        if total <= epsilon {
            return (RiskBasisStatus::ZeroRisk, None);
        }
        return (RiskBasisStatus::Available, Some(total));
    }
    if available > 0 {
        return (RiskBasisStatus::Partial, None);
    }

    let status = tranches
        .iter()
        .map(|tranche| tranche.status)
        .find(|status| *status != RiskBasisStatus::MissingStop)
        .unwrap_or(RiskBasisStatus::MissingStop);
    (status, None)
}

/// Serializable runner-supplied state for one currently open position.
///
/// Fields populated by [`crate::portfolio::PortfolioRecorder`] are optional so
/// an unpriced or partially priced snapshot remains representable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct OpenPositionSnapshot {
    pub position_id: String,
    pub symbol: String,
    pub side: Side,
    pub group: Option<String>,
    pub trade_id: Option<String>,
    pub open_ts: Option<NaiveDateTime>,
    pub average_entry_price: f64,
    pub remaining_size: f64,
    pub initial_stop: Option<f64>,
    pub effective_stop: Option<EffectiveStop>,
    /// Campaign realized P&L before the current mark (for partial closes).
    pub realized_pnl: f64,
    #[serde(default)]
    pub native_realized_pnl: Option<f64>,
    #[serde(default)]
    pub native_currency: Option<String>,
    #[serde(default)]
    pub account_currency: Option<String>,
    pub quote_ts: Option<NaiveDateTime>,
    pub mark_price: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    #[serde(default)]
    pub native_unrealized_pnl: Option<f64>,
    #[serde(default)]
    pub unrealized_pnl_conversion: Option<ConversionResult>,
    pub gross_exposure: Option<f64>,
    #[serde(default)]
    pub native_signed_exposure: Option<f64>,
    #[serde(default)]
    pub gross_exposure_conversion: Option<ConversionResult>,
    pub open_risk: Option<f64>,
    #[serde(default)]
    pub native_open_risk: Option<f64>,
    #[serde(default)]
    pub open_risk_conversion: Option<ConversionResult>,
    pub campaign_mae: Option<f64>,
    pub campaign_mfe: Option<f64>,
}

impl Default for OpenPositionSnapshot {
    fn default() -> Self {
        Self {
            position_id: String::new(),
            symbol: String::new(),
            side: Side::Buy,
            group: None,
            trade_id: None,
            open_ts: None,
            average_entry_price: 0.0,
            remaining_size: 0.0,
            initial_stop: None,
            effective_stop: None,
            realized_pnl: 0.0,
            native_realized_pnl: None,
            native_currency: None,
            account_currency: None,
            quote_ts: None,
            mark_price: None,
            unrealized_pnl: None,
            native_unrealized_pnl: None,
            unrealized_pnl_conversion: None,
            gross_exposure: None,
            native_signed_exposure: None,
            gross_exposure_conversion: None,
            open_risk: None,
            native_open_risk: None,
            open_risk_conversion: None,
            campaign_mae: None,
            campaign_mfe: None,
        }
    }
}

impl OpenPositionSnapshot {
    pub fn new(
        position_id: impl Into<String>,
        symbol: impl Into<String>,
        side: Side,
        average_entry_price: f64,
        remaining_size: f64,
    ) -> Self {
        Self {
            position_id: position_id.into(),
            symbol: symbol.into(),
            side,
            average_entry_price,
            remaining_size,
            ..Self::default()
        }
    }

    pub(crate) fn clear_mark(&mut self) {
        self.quote_ts = None;
        self.mark_price = None;
        self.unrealized_pnl = None;
        self.native_unrealized_pnl = None;
        self.unrealized_pnl_conversion = None;
        self.gross_exposure = None;
        self.native_signed_exposure = None;
        self.gross_exposure_conversion = None;
        self.open_risk = None;
        self.native_open_risk = None;
        self.open_risk_conversion = None;
        self.campaign_mae = None;
        self.campaign_mfe = None;
    }
}

/// State transition emitted by the FutureQuote pending-order lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PendingOrderLifecycleState {
    #[default]
    Placed,
    Filled,
    Cancelled,
    UnfilledAtEnd,
}

impl PendingOrderLifecycleState {
    /// Whether this state permanently terminates a placed pending order.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Placed)
    }
}

/// One append-only transition in the FutureQuote pending-order lifecycle.
///
/// A successfully placed order emits one `Placed` event and exactly one terminal
/// event. Terminal metrics are absent on `Placed`; cancelled and end-of-run
/// orders report a zero filled size and fill ratio.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PendingOrderLifecycleEvent {
    pub id: String,
    pub sequence: u64,
    pub position_id: String,
    pub placement_action_id: Option<String>,
    pub terminal_action_id: Option<String>,
    pub state: PendingOrderLifecycleState,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub requested_size: f64,
    pub filled_size: Option<f64>,
    pub requested_price: Option<f64>,
    pub fill_price: Option<f64>,
    pub signal_ts: Option<NaiveDateTime>,
    pub placed_ts: Option<NaiveDateTime>,
    pub effective_ts: Option<NaiveDateTime>,
    pub terminal_ts: Option<NaiveDateTime>,
    pub wait_latency_ms: Option<i64>,
    pub fill_ratio: Option<f64>,
}

impl Default for PendingOrderLifecycleEvent {
    fn default() -> Self {
        Self {
            id: String::new(),
            sequence: 0,
            position_id: String::new(),
            placement_action_id: None,
            terminal_action_id: None,
            state: PendingOrderLifecycleState::Placed,
            symbol: String::new(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            requested_size: 0.0,
            filled_size: None,
            requested_price: None,
            fill_price: None,
            signal_ts: None,
            placed_ts: None,
            effective_ts: None,
            terminal_ts: None,
            wait_latency_ms: None,
            fill_ratio: None,
        }
    }
}

/// Serializable state for an order that has not filled at the end of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PendingOrderSnapshot {
    pub position_id: String,
    pub action_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub requested_price: Option<f64>,
    pub size: f64,
    pub signal_ts: Option<NaiveDateTime>,
    pub effective_ts: Option<NaiveDateTime>,
    pub initial_stop: Option<f64>,
    pub group: Option<String>,
    pub trade_id: Option<String>,
}

impl Default for PendingOrderSnapshot {
    fn default() -> Self {
        Self {
            position_id: String::new(),
            action_id: None,
            symbol: String::new(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            requested_price: None,
            size: 0.0,
            signal_ts: None,
            effective_ts: None,
            initial_stop: None,
            group: None,
            trade_id: None,
        }
    }
}

/// Complete additive artifact payload for a future backtest run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct FutureBacktestArtifacts {
    pub execution: ExecutionMetadata,
    pub fills: Vec<RecordedFill>,
    pub close_events: Vec<CloseEvent>,
    pub completed_positions: Vec<CompletedPosition>,
    pub open_positions: Vec<OpenPositionSnapshot>,
    pub pending_orders: Vec<PendingOrderSnapshot>,
    pub pending_order_lifecycle: Vec<PendingOrderLifecycleEvent>,
    pub lifecycle: LifecycleLedger,
    pub equity_curve: Vec<EquityPoint>,
    pub mtm_output_summary: MtmOutputSummary,
    pub max_drawdown: Option<f64>,
    pub max_drawdown_pct: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_core::{ExecutionConvention, FillModel, FillPurpose, SlippageModel, StopOrigin};

    fn ts(second: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, second)
            .unwrap()
    }

    fn execution_fill(side: Side, price: f64) -> ExecutionFill {
        ExecutionFill {
            purpose: FillPurpose::MarketEntry,
            side,
            price,
            quote_price: price,
            requested_price: None,
            slippage_pips: 0.0,
        }
    }

    #[test]
    fn execution_metadata_is_serializable_and_defaults_new_fields() {
        let decoded: ExecutionMetadata = serde_json::from_str("{}").unwrap();
        assert_eq!(decoded.schema_version, ARTIFACT_SCHEMA_VERSION);
        assert_eq!(decoded.pnl_epsilon, DEFAULT_PNL_EPSILON);
        assert_eq!(decoded.execution_model, ExecutionModel::default());

        let metadata = ExecutionMetadata {
            execution_model: ExecutionModel::new(
                ExecutionConvention::FutureQuoteV1,
                FillModel::BidAsk,
                SlippageModel::adverse(0.2),
            ),
            initial_balance: 50_000.0,
            account_currency: Some("USD".into()),
            ..ExecutionMetadata::default()
        };
        let roundtrip: ExecutionMetadata =
            serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
        assert_eq!(roundtrip, metadata);
    }

    #[test]
    fn recorded_fill_has_stable_id_and_quote_context() {
        let quote = PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(2),
            bid: 1.0998,
            ask: 1.1000,
        };
        let first = RecordedFill::from_quote(
            "position-7",
            Some("action-3".into()),
            4,
            Some(ts(0)),
            ts(1),
            0.5,
            &quote,
            execution_fill(Side::Buy, 1.1000),
        );
        let second = RecordedFill::from_quote(
            "position-7",
            Some("action-3".into()),
            4,
            Some(ts(0)),
            ts(1),
            0.5,
            &quote,
            execution_fill(Side::Buy, 1.1000),
        );

        assert_eq!(first.id, "position-7:fill:00000004");
        assert_eq!(first, second);
        assert_eq!(first.symbol, "EURUSD");
        assert_eq!(first.quote_ts, ts(2));
        assert_eq!((first.ask - 1.1000).abs(), 0.0);
    }

    #[test]
    fn risk_tranches_validate_direction_and_calculate_money_risk() {
        let long = RiskTranche::calculate(
            Some("fill-1".into()),
            Side::Buy,
            2.0,
            100.0,
            Some(95.0),
            10.0,
            DEFAULT_PNL_EPSILON,
        );
        assert_eq!(long.status, RiskBasisStatus::Available);
        assert_eq!(long.risk_per_unit, Some(5.0));
        assert_eq!(long.risk_amount, Some(100.0));

        let short = RiskTranche::calculate(
            None,
            Side::Sell,
            1.0,
            100.0,
            Some(105.0),
            10.0,
            DEFAULT_PNL_EPSILON,
        );
        assert_eq!(short.risk_amount, Some(50.0));

        let non_protective = RiskTranche::calculate(
            None,
            Side::Buy,
            1.0,
            100.0,
            Some(101.0),
            1.0,
            DEFAULT_PNL_EPSILON,
        );
        assert_eq!(non_protective.status, RiskBasisStatus::NonProtectiveStop);
        assert_eq!(non_protective.risk_amount, None);
    }

    #[test]
    fn completed_position_sums_closes_classifies_and_realizes_r() {
        let closes = vec![
            CloseEvent::new(
                "p1",
                0,
                "XAUUSD",
                Side::Buy,
                ts(3),
                0.5,
                101.0,
                50.0,
                CloseReason::Target,
            ),
            CloseEvent::new(
                "p1",
                1,
                "XAUUSD",
                Side::Buy,
                ts(4),
                0.5,
                99.0,
                -20.0,
                CloseReason::Manual,
            ),
            CloseEvent::new(
                "p1",
                2,
                "XAUUSD",
                Side::Buy,
                ts(5),
                0.1,
                99.0,
                0.0,
                CloseReason::Manual,
            ),
        ];
        let risk = RiskTranche::calculate(
            Some("entry".into()),
            Side::Buy,
            1.0,
            100.0,
            Some(99.0),
            100.0,
            DEFAULT_PNL_EPSILON,
        );
        let completed = CompletedPosition::from_close_events(
            "p1",
            "XAUUSD",
            Side::Buy,
            ts(0),
            ts(5),
            1.0,
            100.0,
            Some(99.0),
            Some(EffectiveStop::new(100.0, StopOrigin::Breakeven)),
            vec![risk],
            closes,
            Some(-40.0),
            Some(70.0),
            DEFAULT_PNL_EPSILON,
        );

        assert_eq!(completed.net_pnl, 30.0);
        assert_eq!(completed.outcome, NetPnlOutcome::Win);
        assert_eq!(completed.initial_risk(), Some(100.0));
        assert_eq!(completed.realized_r, Some(0.3));
        assert_eq!(
            completed.close_reasons,
            vec![CloseReason::Target, CloseReason::Manual]
        );
        assert_eq!(completed.mae, Some(-40.0));
        assert_eq!(completed.mfe, Some(70.0));
    }

    #[test]
    fn net_pnl_outcome_uses_absolute_epsilon() {
        assert_eq!(
            CompletedPosition::classify(0.0005, 0.001),
            NetPnlOutcome::Breakeven
        );
        assert_eq!(
            CompletedPosition::classify(-0.002, -0.001),
            NetPnlOutcome::Loss
        );
        assert_eq!(
            CompletedPosition::classify(0.002, 0.001),
            NetPnlOutcome::Win
        );
    }

    #[test]
    fn partial_risk_basis_does_not_report_misleading_r() {
        let valid = RiskTranche::calculate(
            None,
            Side::Buy,
            1.0,
            10.0,
            Some(9.0),
            1.0,
            DEFAULT_PNL_EPSILON,
        );
        let missing =
            RiskTranche::calculate(None, Side::Buy, 1.0, 10.0, None, 1.0, DEFAULT_PNL_EPSILON);
        let completed = CompletedPosition::from_close_events(
            "p",
            "S",
            Side::Buy,
            ts(0),
            ts(1),
            2.0,
            10.0,
            Some(9.0),
            None,
            vec![valid, missing],
            vec![CloseEvent::new(
                "p",
                0,
                "S",
                Side::Buy,
                ts(1),
                2.0,
                11.0,
                2.0,
                CloseReason::Manual,
            )],
            None,
            None,
            DEFAULT_PNL_EPSILON,
        );
        assert_eq!(completed.risk_basis_status, RiskBasisStatus::Partial);
        assert_eq!(completed.realized_r, None);
    }

    #[test]
    fn aggregate_deserializes_additive_fields_from_empty_object() {
        let artifacts: FutureBacktestArtifacts = serde_json::from_str("{}").unwrap();
        assert_eq!(artifacts.execution.schema_version, ARTIFACT_SCHEMA_VERSION);
        assert!(artifacts.fills.is_empty());
        assert!(artifacts.completed_positions.is_empty());
        assert!(artifacts.equity_curve.is_empty());
        assert_eq!(artifacts.mtm_output_summary, MtmOutputSummary::default());
        assert_eq!(artifacts.execution.schema_version, 2);
        assert_eq!(artifacts.max_drawdown, None);
    }

    #[test]
    fn snapshots_preserve_defaults_for_forward_compatible_fields() {
        let open: OpenPositionSnapshot = serde_json::from_str(
            r#"{"position_id":"p","symbol":"EURUSD","side":"Buy","average_entry_price":1.1,"remaining_size":1.0}"#,
        )
        .unwrap();
        assert_eq!(open.realized_pnl, 0.0);
        assert_eq!(open.mark_price, None);
        assert_eq!(open.campaign_mae, None);

        let pending: PendingOrderSnapshot = serde_json::from_str("{}").unwrap();
        assert_eq!(pending.order_type, OrderType::Limit);
        assert_eq!(pending.initial_stop, None);

        let lifecycle: PendingOrderLifecycleEvent = serde_json::from_str("{}").unwrap();
        assert_eq!(lifecycle.state, PendingOrderLifecycleState::Placed);
        assert_eq!(lifecycle.filled_size, None);
        assert_eq!(lifecycle.terminal_ts, None);
    }
}
