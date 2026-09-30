use chrono::NaiveDateTime;
use qs_core::{
    EntryLevelResolution, PositionRef, PriceGridSource, RawSignal, RuleConfig, Side, SizingResult,
};
use qs_instruments::{DecimalGrid, InstrumentId, QuantityUnit};
use qs_risk::{ExposureIntent, IntentKind, Verdict};
use qs_strategy::{CommandFeedback, ConfiguredActionKind};

use crate::{IntentError, PreparationError, RequestValidationError};

pub(crate) const MAX_EXECUTION_ID_BYTES: usize = 128;
pub(crate) const MAX_REASON_BYTES: usize = 512;

pub(crate) fn validate_id(field: &'static str, value: &str) -> Result<(), IntentError> {
    if value.is_empty() || value.len() > MAX_EXECUTION_ID_BYTES || !value.is_ascii() {
        return Err(IntentError::InvalidIdentifier {
            field,
            maximum: MAX_EXECUTION_ID_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn bounded_reason(reason: impl Into<String>) -> String {
    let mut reason = reason.into();
    if reason.len() <= MAX_REASON_BYTES {
        return reason;
    }
    let mut boundary = MAX_REASON_BYTES;
    while !reason.is_char_boundary(boundary) {
        boundary -= 1;
    }
    reason.truncate(boundary);
    reason
}

/// One strategy or application command awaiting preparation.
#[derive(Debug, Clone)]
pub struct ExecutionIntent {
    pub command_id: String,
    pub instance_id: String,
    pub account_id: String,
    pub decision_time: NaiveDateTime,
    pub signal: RawSignal,
}

impl ExecutionIntent {
    pub fn new(
        command_id: impl Into<String>,
        instance_id: impl Into<String>,
        account_id: impl Into<String>,
        decision_time: NaiveDateTime,
        signal: RawSignal,
    ) -> Result<Self, IntentError> {
        let command_id = command_id.into();
        let instance_id = instance_id.into();
        let account_id = account_id.into();
        validate_id("command_id", &command_id)?;
        validate_id("instance_id", &instance_id)?;
        validate_id("account_id", &account_id)?;
        if signal.ts() != decision_time {
            return Err(IntentError::TimestampMismatch {
                decision_time,
                signal_time: signal.ts(),
            });
        }
        qs_core::validate_raw_signal(&signal)
            .map_err(|error| IntentError::InvalidSignal(error.to_string()))?;
        Ok(Self {
            command_id,
            instance_id,
            account_id,
            decision_time,
            signal,
        })
    }
}

/// Identity shared by every provider operation derived from one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIdentity {
    pub request_id: String,
    pub parent_command_id: String,
    pub instance_id: String,
    pub account_id: String,
    pub decision_time: NaiveDateTime,
}

impl RequestIdentity {
    pub(crate) fn child(
        intent: &ExecutionIntent,
        ordinal: usize,
    ) -> Result<Self, PreparationError> {
        let request_id = format!("{}:{ordinal}", intent.command_id);
        validate_id("request_id", &request_id)
            .map_err(|error| PreparationError::RequestIdentity(error.to_string()))?;
        Ok(Self {
            request_id,
            parent_command_id: intent.command_id.clone(),
            instance_id: intent.instance_id.clone(),
            account_id: intent.account_id.clone(),
            decision_time: intent.decision_time,
        })
    }
}

/// Concrete quantity in the instrument's declared provider-neutral unit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConcreteQuantity {
    pub steps: u64,
    pub amount: f64,
    pub step_amount: f64,
    pub unit: QuantityUnit,
}

/// Initial target carried with an entry request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TargetInstruction {
    pub price: f64,
    pub close_ratio: f64,
    pub quantity: ConcreteQuantity,
}

/// Owner of ongoing rules after an entry request is submitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagementOwner {
    Unsupported,
    Provider,
    ExternalRuntime,
}

/// Provider capabilities used before dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCapabilities {
    pub market_entry: bool,
    pub limit_entry: bool,
    pub stop_entry: bool,
    pub full_close: bool,
    pub partial_close: bool,
    pub cancel_pending: bool,
    pub modify_stop: bool,
    pub initial_stop: bool,
    pub initial_targets: bool,
    pub ongoing_management: ManagementOwner,
}

impl ExecutionCapabilities {
    pub const fn all_with_external_management() -> Self {
        Self {
            market_entry: true,
            limit_entry: true,
            stop_entry: true,
            full_close: true,
            partial_close: true,
            cancel_pending: true,
            modify_stop: true,
            initial_stop: true,
            initial_targets: true,
            ongoing_management: ManagementOwner::ExternalRuntime,
        }
    }
}

impl Default for ExecutionCapabilities {
    fn default() -> Self {
        Self {
            market_entry: false,
            limit_entry: false,
            stop_entry: false,
            full_close: false,
            partial_close: false,
            cancel_pending: false,
            modify_stop: false,
            initial_stop: false,
            initial_targets: false,
            ongoing_management: ManagementOwner::Unsupported,
        }
    }
}

/// Concrete entry operation supplied to a provider.
#[derive(Debug, Clone)]
pub struct EntryRequest {
    pub instrument: InstrumentId,
    pub symbol: String,
    pub side: Side,
    pub order_type: qs_core::OrderType,
    pub level: Option<f64>,
    pub price_grid: DecimalGrid,
    pub quantity: ConcreteQuantity,
    pub initial_stop: Option<f64>,
    pub targets: Vec<TargetInstruction>,
    /// Rules included only when the provider explicitly owns their execution.
    pub provider_rules: Vec<RuleConfig>,
    pub group: Option<String>,
    pub trade_id: Option<String>,
}

/// Concrete provider operation.
#[derive(Debug, Clone)]
pub enum ExecutionOperation {
    Entry(EntryRequest),
    Close {
        instrument: InstrumentId,
        position_id: String,
        quantity: ConcreteQuantity,
        full: bool,
    },
    CancelPending {
        instrument: InstrumentId,
        position_id: String,
        order_id: String,
    },
    ModifyStop {
        instrument: InstrumentId,
        position_id: String,
        stoploss: f64,
        price_grid: DecimalGrid,
    },
}

/// One provider request with stable application identity.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub identity: RequestIdentity,
    pub operation: ExecutionOperation,
}

impl ExecutionRequest {
    pub fn validate(&self) -> Result<(), RequestValidationError> {
        validate_id("request_id", &self.identity.request_id)
            .map_err(|error| RequestValidationError(error.to_string()))?;
        validate_id("parent_command_id", &self.identity.parent_command_id)
            .map_err(|error| RequestValidationError(error.to_string()))?;
        validate_id("instance_id", &self.identity.instance_id)
            .map_err(|error| RequestValidationError(error.to_string()))?;
        validate_id("account_id", &self.identity.account_id)
            .map_err(|error| RequestValidationError(error.to_string()))?;
        let prefix = format!("{}:", self.identity.parent_command_id);
        let Some(ordinal) = self.identity.request_id.strip_prefix(&prefix) else {
            return Err(RequestValidationError(
                "request_id must be parent_command_id plus a child ordinal".into(),
            ));
        };
        if ordinal.is_empty() || ordinal.parse::<usize>().is_err() {
            return Err(RequestValidationError(
                "request_id child ordinal must be an unsigned integer".into(),
            ));
        }
        match &self.operation {
            ExecutionOperation::Entry(entry) => {
                validate_id("entry symbol", &entry.symbol)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
                if entry.symbol != entry.instrument.listing.as_str() {
                    return Err(RequestValidationError(
                        "entry symbol does not match instrument listing".into(),
                    ));
                }
                validate_quantity(entry.quantity)?;
                match (entry.order_type, entry.level) {
                    (qs_core::OrderType::Market, None)
                    | (qs_core::OrderType::Limit | qs_core::OrderType::Stop, Some(_)) => {}
                    _ => {
                        return Err(RequestValidationError(
                            "entry level does not match its order type".into(),
                        ));
                    }
                }
                if let Some(level) = entry.level {
                    validate_grid_price("entry level", level, entry.price_grid)?;
                }
                if let Some(stop) = entry.initial_stop {
                    validate_grid_price("initial stop", stop, entry.price_grid)?;
                }
                let mut target_steps = 0_u64;
                for target in &entry.targets {
                    validate_grid_price("target", target.price, entry.price_grid)?;
                    if !target.close_ratio.is_finite()
                        || target.close_ratio <= 0.0
                        || target.close_ratio > 1.0
                    {
                        return Err(RequestValidationError(
                            "target close_ratio must be finite in (0, 1]".into(),
                        ));
                    }
                    validate_quantity(target.quantity)?;
                    if target.quantity.unit != entry.quantity.unit
                        || (target.quantity.step_amount - entry.quantity.step_amount).abs()
                            > entry.quantity.step_amount.abs().max(1.0) * 1.0e-12
                    {
                        return Err(RequestValidationError(
                            "target quantity unit and step must match the entry quantity".into(),
                        ));
                    }
                    target_steps = target_steps
                        .checked_add(target.quantity.steps)
                        .ok_or_else(|| RequestValidationError("target steps overflow".into()))?;
                }
                if target_steps > entry.quantity.steps {
                    return Err(RequestValidationError(
                        "target quantities exceed entry quantity".into(),
                    ));
                }
            }
            ExecutionOperation::Close {
                position_id,
                quantity,
                ..
            } => {
                validate_id("position_id", position_id)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
                validate_quantity(*quantity)?;
            }
            ExecutionOperation::CancelPending {
                position_id,
                order_id,
                ..
            } => {
                validate_id("position_id", position_id)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
                validate_id("order_id", order_id)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
            }
            ExecutionOperation::ModifyStop {
                position_id,
                stoploss,
                price_grid,
                ..
            } => {
                validate_id("position_id", position_id)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
                validate_positive_price("stoploss", *stoploss)?;
                let decimal = qs_instruments::Decimal::checked_from_f64(*stoploss)
                    .map_err(|error| RequestValidationError(error.to_string()))?;
                if !price_grid
                    .contains(decimal)
                    .map_err(|error| RequestValidationError(error.to_string()))?
                {
                    return Err(RequestValidationError(
                        "stoploss is outside the request price grid".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn kind(&self) -> RequestKind {
        match &self.operation {
            ExecutionOperation::Entry(_) => RequestKind::Entry,
            ExecutionOperation::Close { full: true, .. } => RequestKind::FullClose,
            ExecutionOperation::Close { full: false, .. } => RequestKind::PartialClose,
            ExecutionOperation::CancelPending { .. } => RequestKind::CancelPending,
            ExecutionOperation::ModifyStop { .. } => RequestKind::ModifyStop,
        }
    }

    pub fn expected_steps(&self) -> Option<u64> {
        match &self.operation {
            ExecutionOperation::Entry(entry) => Some(entry.quantity.steps),
            ExecutionOperation::Close { quantity, .. } => Some(quantity.steps),
            ExecutionOperation::CancelPending { .. } | ExecutionOperation::ModifyStop { .. } => {
                None
            }
        }
    }
}

fn validate_quantity(quantity: ConcreteQuantity) -> Result<(), RequestValidationError> {
    if quantity.steps == 0
        || !quantity.amount.is_finite()
        || quantity.amount <= 0.0
        || !quantity.step_amount.is_finite()
        || quantity.step_amount <= 0.0
    {
        return Err(RequestValidationError(
            "quantity needs positive finite steps, amount, and step_amount".into(),
        ));
    }
    let expected = quantity.step_amount * quantity.steps as f64;
    let tolerance = expected.abs().max(1.0) * 1.0e-12;
    if !expected.is_finite() || (quantity.amount - expected).abs() > tolerance {
        return Err(RequestValidationError(
            "quantity amount must equal steps times step_amount".into(),
        ));
    }
    Ok(())
}

fn validate_positive_price(field: &str, value: f64) -> Result<(), RequestValidationError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(RequestValidationError(format!(
            "{field} must be finite and positive"
        )));
    }
    Ok(())
}

fn validate_grid_price(
    field: &str,
    value: f64,
    grid: DecimalGrid,
) -> Result<(), RequestValidationError> {
    validate_positive_price(field, value)?;
    let decimal = qs_instruments::Decimal::checked_from_f64(value)
        .map_err(|error| RequestValidationError(error.to_string()))?;
    if !grid
        .contains(decimal)
        .map_err(|error| RequestValidationError(error.to_string()))?
    {
        return Err(RequestValidationError(format!(
            "{field} is outside the instrument price grid"
        )));
    }
    Ok(())
}

/// Request class used for report validation and configured feedback projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Entry,
    FullClose,
    PartialClose,
    CancelPending,
    ModifyStop,
}

impl RequestKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::FullClose => "full_close",
            Self::PartialClose => "partial_close",
            Self::CancelPending => "cancel_pending",
            Self::ModifyStop => "modify_stop",
        }
    }

    pub const fn configured_action(self) -> ConfiguredActionKind {
        match self {
            Self::Entry => ConfiguredActionKind::Entry,
            Self::FullClose => ConfiguredActionKind::Close,
            Self::PartialClose => ConfiguredActionKind::ClosePartial,
            Self::CancelPending => ConfiguredActionKind::CancelPending,
            Self::ModifyStop => ConfiguredActionKind::ModifyStoploss,
        }
    }
}

/// Entry sizing reference selected during preparation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PreparationSizingBasis {
    #[default]
    CurrentQuote,
    SignalEntryPriceWithQuoteFallback,
}

/// Auditable preparation facts. Actual fills are deliberately absent.
#[derive(Debug, Clone)]
pub struct PreparationAudit {
    pub instrument: InstrumentId,
    pub prepared_at: NaiveDateTime,
    pub price_grid_source: PriceGridSource,
    pub original_signal_price: Option<f64>,
    pub preparation_price: f64,
    pub sizing_reference_price: f64,
    pub sizing_basis: PreparationSizingBasis,
    pub protective_stop: Option<f64>,
    pub sizing: SizingResult,
    pub level_resolution: EntryLevelResolution,
}

/// Owned exposure request that can be reviewed after approval and fresh preparation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExposureRequest {
    pub symbol: String,
    pub side: Side,
    pub kind: IntentKind,
    pub requested_risk: Option<f64>,
}

impl ExposureRequest {
    pub fn as_intent(&self) -> ExposureIntent<'_> {
        ExposureIntent {
            symbol: &self.symbol,
            side: self.side,
            kind: self.kind,
            requested_risk: self.requested_risk,
        }
    }
}

/// Fully prepared requests and application-owned ongoing rules.
#[derive(Debug, Clone)]
pub struct PreparedExecution {
    pub command_id: String,
    pub action: ConfiguredActionKind,
    pub requests: Vec<ExecutionRequest>,
    pub audit: Option<PreparationAudit>,
    pub external_management: Vec<RuleConfig>,
    pub exposure: Option<ExposureRequest>,
}

impl PreparedExecution {
    /// Apply the application's current risk verdict without mutating requests.
    pub fn apply_risk_verdict(self, verdict: Verdict) -> Result<Self, PreparationError> {
        match verdict {
            Verdict::Approve => Ok(self),
            Verdict::Reject { policy, reason } => {
                Err(PreparationError::RiskRejected { policy, reason })
            }
        }
    }
}

/// Lifecycle result known without provider submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedIntent {
    pub command_id: String,
    pub kind: CompletionKind,
    pub feedback: Vec<CommandFeedback>,
}

/// Reason an intent completed before provider dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKind {
    Skipped,
    Rejected,
}

/// Result of pure preparation.
#[derive(Debug, Clone)]
pub enum PreparationOutcome {
    Prepared(Box<PreparedExecution>),
    Completed(CompletedIntent),
}

/// Application snapshot status relevant to concrete management operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSnapshotStatus {
    Pending,
    Open,
}

impl PositionSnapshotStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Open => "open",
        }
    }
}

/// Authoritative application-owned mapping to provider position/order facts.
#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    pub position_id: String,
    pub order_id: Option<String>,
    pub instrument: InstrumentId,
    pub symbol: String,
    pub side: Side,
    pub status: PositionSnapshotStatus,
    pub total_entered_steps: u64,
    pub remaining_steps: u64,
    pub quantity_step: f64,
    pub quantity_unit: QuantityUnit,
    pub average_entry_price: Option<f64>,
    pub price_grid: DecimalGrid,
    pub trade_id: Option<String>,
    pub group_id: Option<String>,
}

/// Position reference lookup distinguishes known absence from unavailable state.
#[derive(Debug, Clone)]
pub enum PositionLookup {
    Found(Box<PositionSnapshot>),
    KnownAbsent,
    Unknown,
    Ambiguous { count: usize },
}

/// Caller-owned position/order lookup boundary.
pub trait StateResolver {
    fn resolve(&self, reference: &PositionRef) -> PositionLookup;
}

/// Provider response to request submission. Acceptance is not a fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub request_id: String,
    pub provider_reference: Option<String>,
}

/// One observed provider outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum ReportOutcome {
    Accepted,
    Resting,
    Fill {
        incremental_steps: u64,
        cumulative_steps: u64,
        price: f64,
    },
    Cancelled {
        filled_steps: u64,
        reason: String,
    },
    Modified {
        stoploss: f64,
    },
    Rejected {
        reason: String,
    },
    Failed {
        reason: String,
    },
}

impl ReportOutcome {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Resting => "resting",
            Self::Fill { .. } => "fill",
            Self::Cancelled { .. } => "cancelled",
            Self::Modified { .. } => "modified",
            Self::Rejected { .. } => "rejected",
            Self::Failed { .. } => "failed",
        }
    }
}

/// Provider observation used by the report projector.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionReport {
    pub observation_id: String,
    pub request_id: String,
    pub parent_command_id: String,
    pub observed_at: NaiveDateTime,
    pub outcome: ReportOutcome,
}

/// Final provider outcome retained independently from strategy feedback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderOutcome {
    Filled {
        filled_steps: u64,
    },
    PartiallyFilledThenCancelled {
        filled_steps: u64,
        cancelled_steps: u64,
    },
    Cancelled,
    Modified,
    Rejected {
        reason: String,
    },
    Failed {
        reason: String,
    },
}
