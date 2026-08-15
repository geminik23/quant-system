use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::str::FromStr;

use qs_instruments::{AssetId, Decimal, ExecutionVenueId, InstrumentId, Price, Quantity};
use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::canonical::{
    DateTimeUtc, ExecutionCommandId, FillId, OpaquePayloadRef, TradeIntentId, VenueOrderRef,
    VenuePositionRef,
};
use crate::types::{CloseReason, Effect, FutureEffect, Side};

pub const EXECUTION_SCHEMA_VERSION: u32 = 1;
pub const MAX_BOUNDED_TEXT_BYTES: usize = 512;
pub const MAX_VENUE_SEQUENCE_BYTES: usize = 160;
pub const MAX_REPORT_NAMESPACE_BYTES: usize = 96;
pub const MAX_FILL_FEES: usize = 32;
pub const MAX_POSITION_TARGETS: usize = 64;
pub const MAX_CLOSE_FEES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutionEventError {
    #[error("schema version must be {EXECUTION_SCHEMA_VERSION}, got {0}")]
    InvalidSchemaVersion(u32),
    #[error("{kind} length must be between 1 and {maximum} bytes, got {actual}")]
    InvalidTextLength {
        kind: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{kind} contains an unsupported character")]
    InvalidTextCharacter { kind: &'static str },
    #[error("{kind} quantity must be positive")]
    QuantityMustBePositive { kind: &'static str },
    #[error("fee amount must be nonzero")]
    ZeroFeeAmount,
    #[error("{kind} count exceeds maximum {maximum}, got {actual}")]
    CollectionTooLarge {
        kind: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("order cumulative and remaining quantities must equal order quantity")]
    InconsistentOrderQuantities,
    #[error("fill cumulative quantity cannot be smaller than incremental quantity")]
    InconsistentFillQuantities,
    #[error("partial fill must have a positive remaining quantity")]
    PartialFillWithoutRemainingQuantity,
    #[error("final fill must have zero remaining quantity")]
    FinalFillWithRemainingQuantity,
    #[error("fill and order snapshots do not describe the same order state")]
    FillOrderMismatch,
    #[error("event and order snapshot references do not match")]
    OrderReferenceMismatch,
    #[error("reconciliation must include an order or position snapshot")]
    EmptyReconciliation,
    #[error("partial close ratio must be finite, greater than zero, and less than one")]
    InvalidPartialCloseRatio,
    #[error("report counter overflow")]
    ReportCounterOverflow,
    #[error("invalid exact decimal value: {0}")]
    InvalidDecimal(String),
    #[error("invalid canonical identity: {0}")]
    InvalidIdentity(String),
}

fn validate_text(
    kind: &'static str,
    value: &str,
    maximum: usize,
    ascii_only: bool,
    reject_whitespace: bool,
) -> Result<(), ExecutionEventError> {
    if value.is_empty() || value.len() > maximum {
        return Err(ExecutionEventError::InvalidTextLength {
            kind,
            maximum,
            actual: value.len(),
        });
    }
    if (ascii_only && !value.is_ascii())
        || value.bytes().any(|byte| {
            byte.is_ascii_control() || (reject_whitespace && byte.is_ascii_whitespace())
        })
    {
        return Err(ExecutionEventError::InvalidTextCharacter { kind });
    }
    Ok(())
}

macro_rules! bounded_string {
    ($name:ident, $kind:literal, $maximum:expr, $ascii_only:expr, $reject_whitespace:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ExecutionEventError> {
                let value = value.into();
                validate_text($kind, &value, $maximum, $ascii_only, $reject_whitespace)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = ExecutionEventError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

bounded_string!(
    BoundedText,
    "bounded text",
    MAX_BOUNDED_TEXT_BYTES,
    false,
    false
);
bounded_string!(
    VenueSequence,
    "venue sequence",
    MAX_VENUE_SEQUENCE_BYTES,
    true,
    true
);
bounded_string!(
    ReportNamespace,
    "report namespace",
    MAX_REPORT_NAMESPACE_BYTES,
    true,
    true
);

fn deserialize_schema_version<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let version = u32::deserialize(deserializer)?;
    if version != EXECUTION_SCHEMA_VERSION {
        return Err(serde::de::Error::custom(
            ExecutionEventError::InvalidSchemaVersion(version),
        ));
    }
    Ok(version)
}

fn deserialize_bounded_vec<'de, D, T, const MAXIMUM: usize>(
    deserializer: D,
    collection: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    struct BoundedVecVisitor<T, const MAXIMUM: usize> {
        collection: &'static str,
        marker: PhantomData<T>,
    }

    impl<'de, T, const MAXIMUM: usize> Visitor<'de> for BoundedVecVisitor<T, MAXIMUM>
    where
        T: DeserializeOwned,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "at most {MAXIMUM} values in {}", self.collection)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence.size_hint().is_some_and(|size| size > MAXIMUM) {
                return Err(serde::de::Error::custom(format!(
                    "{} exceeds maximum length {MAXIMUM}",
                    self.collection
                )));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAXIMUM));
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAXIMUM {
                    return Err(serde::de::Error::custom(format!(
                        "{} exceeds maximum length {MAXIMUM}",
                        self.collection
                    )));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor::<T, MAXIMUM> {
        collection,
        marker: PhantomData,
    })
}

fn deserialize_fill_fees<'de, D>(deserializer: D) -> Result<Vec<FeeAmount>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec::<D, _, MAX_FILL_FEES>(deserializer, "fill fees")
}

fn deserialize_position_targets<'de, D>(deserializer: D) -> Result<Vec<CanonicalTarget>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec::<D, _, MAX_POSITION_TARGETS>(deserializer, "position targets")
}

fn deserialize_close_fees<'de, D>(deserializer: D) -> Result<Vec<FeeAmount>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec::<D, _, MAX_CLOSE_FEES>(deserializer, "close fees")
}

fn deserialize_positive_quantity<'de, D>(deserializer: D) -> Result<Quantity, D::Error>
where
    D: Deserializer<'de>,
{
    let quantity = Quantity::deserialize(deserializer)?;
    require_positive_quantity("canonical", quantity).map_err(serde::de::Error::custom)?;
    Ok(quantity)
}

fn deserialize_nonzero_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let amount = Decimal::deserialize(deserializer)?;
    if amount.is_zero() {
        return Err(serde::de::Error::custom(ExecutionEventError::ZeroFeeAmount));
    }
    Ok(amount)
}

fn require_positive_quantity(
    kind: &'static str,
    quantity: Quantity,
) -> Result<(), ExecutionEventError> {
    quantity
        .require_positive()
        .map(|_| ())
        .map_err(|_| ExecutionEventError::QuantityMustBePositive { kind })
}

fn validate_collection_bound(
    kind: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), ExecutionEventError> {
    if actual > maximum {
        return Err(ExecutionEventError::CollectionTooLarge {
            kind,
            maximum,
            actual,
        });
    }
    Ok(())
}

mod side_serde {
    use super::*;

    pub fn serialize<S>(side: &Side, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        })
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Side, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer)?.as_str() {
            "buy" => Ok(Side::Buy),
            "sell" => Ok(Side::Sell),
            value => Err(serde::de::Error::unknown_variant(value, &["buy", "sell"])),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionCommandEnvelope<T> {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub command_id: ExecutionCommandId,
    pub intent_id: TradeIntentId,
    pub created_at: DateTimeUtc,
    pub payload: T,
}

impl<T> ExecutionCommandEnvelope<T> {
    pub fn new(
        command_id: ExecutionCommandId,
        intent_id: TradeIntentId,
        created_at: DateTimeUtc,
        payload: T,
    ) -> Self {
        Self {
            schema_version: EXECUTION_SCHEMA_VERSION,
            command_id,
            intent_id,
            created_at,
            payload,
        }
    }

    pub fn with_deterministic_id(
        intent_id: TradeIntentId,
        command_ordinal: u64,
        created_at: DateTimeUtc,
        payload: T,
    ) -> Self {
        let command_id = deterministic_execution_command_id(&intent_id, command_ordinal);
        Self::new(command_id, intent_id, created_at, payload)
    }
}

impl<T: PartialEq> ExecutionCommandEnvelope<T> {
    pub fn compare(&self, other: &Self) -> CommandComparison {
        if self.command_id != other.command_id {
            CommandComparison::Distinct
        } else if self == other {
            CommandComparison::Duplicate
        } else {
            CommandComparison::Conflict
        }
    }
}

fn deterministic_execution_command_id(
    intent_id: &TradeIntentId,
    command_ordinal: u64,
) -> ExecutionCommandId {
    ExecutionCommandId::new(format!("command:{intent_id}:{command_ordinal}"))
        .expect("deterministic execution command ID is valid")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CommandComparison {
    Distinct,
    Duplicate,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandDispatchAttempt<T> {
    pub command: ExecutionCommandEnvelope<T>,
    pub attempt: NonZeroU32,
    pub dispatched_at: DateTimeUtc,
}

impl<T> CommandDispatchAttempt<T> {
    pub fn new(
        command: ExecutionCommandEnvelope<T>,
        attempt: NonZeroU32,
        dispatched_at: DateTimeUtc,
    ) -> Self {
        Self {
            command,
            attempt,
            dispatched_at,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchFailureCategory {
    Timeout,
    Connection,
    Authentication,
    Authorization,
    RateLimited,
    Serialization,
    Protocol,
    GatewayUnavailable,
    Internal,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CommandDispatchEvent {
    TransportAcknowledged {
        gateway_reference: Option<BoundedText>,
    },
    TransportFailed {
        category: DispatchFailureCategory,
        message: BoundedText,
    },
    UnknownOutcome {
        category: DispatchFailureCategory,
        message: BoundedText,
    },
    ReconciliationRequired {
        reason: BoundedText,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandDispatchReport {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub intent_id: TradeIntentId,
    pub command_id: ExecutionCommandId,
    pub execution_venue: ExecutionVenueId,
    pub observed_at: DateTimeUtc,
    pub event: CommandDispatchEvent,
}

impl CommandDispatchReport {
    pub fn new(
        intent_id: TradeIntentId,
        command_id: ExecutionCommandId,
        execution_venue: ExecutionVenueId,
        observed_at: DateTimeUtc,
        event: CommandDispatchEvent,
    ) -> Self {
        Self {
            schema_version: EXECUTION_SCHEMA_VERSION,
            intent_id,
            command_id,
            execution_venue,
            observed_at,
            event,
        }
    }

    pub fn compare(&self, other: &Self) -> ReportComparison {
        let same_identity = self.intent_id == other.intent_id
            && self.command_id == other.command_id
            && self.execution_venue == other.execution_venue
            && self.observed_at == other.observed_at;
        compare_report_parts(same_identity, self == other)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
    Auction,
    Internalized,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeType {
    Commission,
    Exchange,
    Clearing,
    Regulatory,
    Financing,
    Tax,
    Rebate,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionCategory {
    InvalidOrder,
    InsufficientFunds,
    RiskLimit,
    MarketClosed,
    InstrumentUnavailable,
    PriceOutOfRange,
    QuantityOutOfRange,
    Duplicate,
    PermissionDenied,
    VenueUnavailable,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationSource {
    VenueSnapshot,
    VenueHistory,
    DropCopy,
    Replay,
    PaperLedger,
    Operator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalCloseReason {
    StopLoss,
    Target,
    TrailingStop,
    TimeExit,
    BreakevenStop,
    Manual,
    EndOfData,
    GroupRule,
    Cancelled,
    Liquidation,
    Venue,
    Reconciliation,
    Other,
}

/// Signed nonzero amount for a charge or rebate. Positive values debit the account and negative values credit it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeAmount {
    pub asset: AssetId,
    #[serde(deserialize_with = "deserialize_nonzero_decimal")]
    pub amount: Decimal,
    pub fee_type: FeeType,
}

impl FeeAmount {
    pub fn new(
        asset: AssetId,
        amount: Decimal,
        fee_type: FeeType,
    ) -> Result<Self, ExecutionEventError> {
        if amount.is_zero() {
            return Err(ExecutionEventError::ZeroFeeAmount);
        }
        Ok(Self {
            asset,
            amount,
            fee_type,
        })
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        if self.amount.is_zero() {
            return Err(ExecutionEventError::ZeroFeeAmount);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalOrderSnapshot {
    pub venue_order_ref: VenueOrderRef,
    #[serde(with = "side_serde")]
    pub side: Side,
    pub order_quantity: Quantity,
    pub cumulative_quantity: Quantity,
    pub remaining_quantity: Quantity,
    pub average_fill_price: Option<Price>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalOrderSnapshotWire {
    venue_order_ref: VenueOrderRef,
    #[serde(with = "side_serde")]
    side: Side,
    #[serde(deserialize_with = "deserialize_positive_quantity")]
    order_quantity: Quantity,
    cumulative_quantity: Quantity,
    remaining_quantity: Quantity,
    average_fill_price: Option<Price>,
}

impl CanonicalOrderSnapshot {
    pub fn new(
        venue_order_ref: VenueOrderRef,
        side: Side,
        order_quantity: Quantity,
        cumulative_quantity: Quantity,
        remaining_quantity: Quantity,
        average_fill_price: Option<Price>,
    ) -> Result<Self, ExecutionEventError> {
        require_positive_quantity("order", order_quantity)?;
        let total = cumulative_quantity
            .get()
            .checked_add(remaining_quantity.get())
            .map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))?;
        if total != order_quantity.get() {
            return Err(ExecutionEventError::InconsistentOrderQuantities);
        }
        Ok(Self {
            venue_order_ref,
            side,
            order_quantity,
            cumulative_quantity,
            remaining_quantity,
            average_fill_price,
        })
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        require_positive_quantity("order", self.order_quantity)?;
        let total = self
            .cumulative_quantity
            .get()
            .checked_add(self.remaining_quantity.get())
            .map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))?;
        if total != self.order_quantity.get() {
            return Err(ExecutionEventError::InconsistentOrderQuantities);
        }
        Ok(())
    }
}

impl TryFrom<CanonicalOrderSnapshotWire> for CanonicalOrderSnapshot {
    type Error = ExecutionEventError;

    fn try_from(value: CanonicalOrderSnapshotWire) -> Result<Self, Self::Error> {
        Self::new(
            value.venue_order_ref,
            value.side,
            value.order_quantity,
            value.cumulative_quantity,
            value.remaining_quantity,
            value.average_fill_price,
        )
    }
}

impl<'de> Deserialize<'de> for CanonicalOrderSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CanonicalOrderSnapshotWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalFill {
    pub fill_id: Option<FillId>,
    pub venue_order_ref: VenueOrderRef,
    #[serde(with = "side_serde")]
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
    pub cumulative_quantity: Quantity,
    pub remaining_quantity: Quantity,
    pub liquidity_role: LiquidityRole,
    pub fees: Vec<FeeAmount>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalFillWire {
    fill_id: Option<FillId>,
    venue_order_ref: VenueOrderRef,
    #[serde(with = "side_serde")]
    side: Side,
    price: Price,
    #[serde(deserialize_with = "deserialize_positive_quantity")]
    quantity: Quantity,
    #[serde(deserialize_with = "deserialize_positive_quantity")]
    cumulative_quantity: Quantity,
    remaining_quantity: Quantity,
    liquidity_role: LiquidityRole,
    #[serde(deserialize_with = "deserialize_fill_fees")]
    fees: Vec<FeeAmount>,
}

impl CanonicalFill {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fill_id: Option<FillId>,
        venue_order_ref: VenueOrderRef,
        side: Side,
        price: Price,
        quantity: Quantity,
        cumulative_quantity: Quantity,
        remaining_quantity: Quantity,
        liquidity_role: LiquidityRole,
        fees: Vec<FeeAmount>,
    ) -> Result<Self, ExecutionEventError> {
        let value = Self {
            fill_id,
            venue_order_ref,
            side,
            price,
            quantity,
            cumulative_quantity,
            remaining_quantity,
            liquidity_role,
            fees,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        require_positive_quantity("fill", self.quantity)?;
        require_positive_quantity("cumulative fill", self.cumulative_quantity)?;
        if self.cumulative_quantity < self.quantity {
            return Err(ExecutionEventError::InconsistentFillQuantities);
        }
        validate_collection_bound("fill fee", self.fees.len(), MAX_FILL_FEES)?;
        for fee in &self.fees {
            fee.validate()?;
        }
        Ok(())
    }
}

impl TryFrom<CanonicalFillWire> for CanonicalFill {
    type Error = ExecutionEventError;

    fn try_from(value: CanonicalFillWire) -> Result<Self, Self::Error> {
        Self::new(
            value.fill_id,
            value.venue_order_ref,
            value.side,
            value.price,
            value.quantity,
            value.cumulative_quantity,
            value.remaining_quantity,
            value.liquidity_role,
            value.fees,
        )
    }
}

impl<'de> Deserialize<'de> for CanonicalFill {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CanonicalFillWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CanonicalProtection {
    pub stop_loss: Option<Price>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalTarget {
    pub target_ref: Option<BoundedText>,
    pub price: Price,
    #[serde(deserialize_with = "deserialize_positive_quantity")]
    pub quantity: Quantity,
}

impl CanonicalTarget {
    pub fn new(
        target_ref: Option<BoundedText>,
        price: Price,
        quantity: Quantity,
    ) -> Result<Self, ExecutionEventError> {
        require_positive_quantity("target", quantity)?;
        Ok(Self {
            target_ref,
            price,
            quantity,
        })
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        require_positive_quantity("target", self.quantity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalPositionSnapshot {
    pub venue_position_ref: VenuePositionRef,
    #[serde(with = "side_serde")]
    pub side: Side,
    pub quantity_before: Quantity,
    pub quantity_after: Quantity,
    pub average_open_price: Option<Price>,
    pub protection: CanonicalProtection,
    pub targets: Vec<CanonicalTarget>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalPositionSnapshotWire {
    venue_position_ref: VenuePositionRef,
    #[serde(with = "side_serde")]
    side: Side,
    quantity_before: Quantity,
    quantity_after: Quantity,
    average_open_price: Option<Price>,
    protection: CanonicalProtection,
    #[serde(deserialize_with = "deserialize_position_targets")]
    targets: Vec<CanonicalTarget>,
}

impl CanonicalPositionSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        venue_position_ref: VenuePositionRef,
        side: Side,
        quantity_before: Quantity,
        quantity_after: Quantity,
        average_open_price: Option<Price>,
        protection: CanonicalProtection,
        targets: Vec<CanonicalTarget>,
    ) -> Result<Self, ExecutionEventError> {
        let value = Self {
            venue_position_ref,
            side,
            quantity_before,
            quantity_after,
            average_open_price,
            protection,
            targets,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        validate_collection_bound("position target", self.targets.len(), MAX_POSITION_TARGETS)?;
        for target in &self.targets {
            target.validate()?;
        }
        Ok(())
    }
}

impl TryFrom<CanonicalPositionSnapshotWire> for CanonicalPositionSnapshot {
    type Error = ExecutionEventError;

    fn try_from(value: CanonicalPositionSnapshotWire) -> Result<Self, Self::Error> {
        Self::new(
            value.venue_position_ref,
            value.side,
            value.quantity_before,
            value.quantity_after,
            value.average_open_price,
            value.protection,
            value.targets,
        )
    }
}

impl<'de> Deserialize<'de> for CanonicalPositionSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CanonicalPositionSnapshotWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalClose {
    pub venue_position_ref: VenuePositionRef,
    #[serde(with = "side_serde")]
    pub side: Side,
    pub quantity: Quantity,
    pub price: Price,
    pub reason: CanonicalCloseReason,
    pub fees: Vec<FeeAmount>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalCloseWire {
    venue_position_ref: VenuePositionRef,
    #[serde(with = "side_serde")]
    side: Side,
    #[serde(deserialize_with = "deserialize_positive_quantity")]
    quantity: Quantity,
    price: Price,
    reason: CanonicalCloseReason,
    #[serde(deserialize_with = "deserialize_close_fees")]
    fees: Vec<FeeAmount>,
}

impl CanonicalClose {
    pub fn new(
        venue_position_ref: VenuePositionRef,
        side: Side,
        quantity: Quantity,
        price: Price,
        reason: CanonicalCloseReason,
        fees: Vec<FeeAmount>,
    ) -> Result<Self, ExecutionEventError> {
        let value = Self {
            venue_position_ref,
            side,
            quantity,
            price,
            reason,
            fees,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ExecutionEventError> {
        require_positive_quantity("close", self.quantity)?;
        validate_collection_bound("close fee", self.fees.len(), MAX_CLOSE_FEES)?;
        for fee in &self.fees {
            fee.validate()?;
        }
        Ok(())
    }
}

impl TryFrom<CanonicalCloseWire> for CanonicalClose {
    type Error = ExecutionEventError;

    fn try_from(value: CanonicalCloseWire) -> Result<Self, Self::Error> {
        Self::new(
            value.venue_position_ref,
            value.side,
            value.quantity,
            value.price,
            value.reason,
            value.fees,
        )
    }
}

impl<'de> Deserialize<'de> for CanonicalClose {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CanonicalCloseWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExecutionEvent {
    VenueAccepted {
        order: CanonicalOrderSnapshot,
    },
    VenueRejected {
        category: RejectionCategory,
        message: BoundedText,
        venue_order_ref: Option<VenueOrderRef>,
    },
    OrderPartiallyFilled {
        fill: CanonicalFill,
        order: CanonicalOrderSnapshot,
    },
    OrderFilled {
        fill: CanonicalFill,
        order: CanonicalOrderSnapshot,
    },
    OrderCancelled {
        venue_order_ref: VenueOrderRef,
        order: Option<CanonicalOrderSnapshot>,
        reason: Option<BoundedText>,
    },
    OrderExpired {
        venue_order_ref: VenueOrderRef,
        order: Option<CanonicalOrderSnapshot>,
    },
    ProtectionChanged {
        venue_position_ref: VenuePositionRef,
        protection: CanonicalProtection,
    },
    TargetsChanged {
        venue_position_ref: VenuePositionRef,
        targets: Vec<CanonicalTarget>,
    },
    PositionChanged {
        position: CanonicalPositionSnapshot,
        fill: Option<CanonicalFill>,
    },
    PositionClosed {
        close: CanonicalClose,
    },
    Reconciled {
        source: ReconciliationSource,
        order: Option<CanonicalOrderSnapshot>,
        position: Option<CanonicalPositionSnapshot>,
        note: Option<BoundedText>,
    },
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ExecutionEventWire {
    VenueAccepted {
        order: CanonicalOrderSnapshot,
    },
    VenueRejected {
        category: RejectionCategory,
        message: BoundedText,
        venue_order_ref: Option<VenueOrderRef>,
    },
    OrderPartiallyFilled {
        fill: CanonicalFill,
        order: CanonicalOrderSnapshot,
    },
    OrderFilled {
        fill: CanonicalFill,
        order: CanonicalOrderSnapshot,
    },
    OrderCancelled {
        venue_order_ref: VenueOrderRef,
        order: Option<CanonicalOrderSnapshot>,
        reason: Option<BoundedText>,
    },
    OrderExpired {
        venue_order_ref: VenueOrderRef,
        order: Option<CanonicalOrderSnapshot>,
    },
    ProtectionChanged {
        venue_position_ref: VenuePositionRef,
        protection: CanonicalProtection,
    },
    TargetsChanged {
        venue_position_ref: VenuePositionRef,
        #[serde(deserialize_with = "deserialize_position_targets")]
        targets: Vec<CanonicalTarget>,
    },
    PositionChanged {
        position: CanonicalPositionSnapshot,
        fill: Option<CanonicalFill>,
    },
    PositionClosed {
        close: CanonicalClose,
    },
    Reconciled {
        source: ReconciliationSource,
        order: Option<CanonicalOrderSnapshot>,
        position: Option<CanonicalPositionSnapshot>,
        note: Option<BoundedText>,
    },
}

impl ExecutionEvent {
    pub fn validate(&self) -> Result<(), ExecutionEventError> {
        match self {
            Self::VenueAccepted { order } => order.validate(),
            Self::VenueRejected { .. } | Self::ProtectionChanged { .. } => Ok(()),
            Self::OrderPartiallyFilled { fill, order } => {
                fill.validate()?;
                order.validate()?;
                validate_fill_order(fill, order)?;
                if fill.remaining_quantity.get().is_zero() {
                    return Err(ExecutionEventError::PartialFillWithoutRemainingQuantity);
                }
                Ok(())
            }
            Self::OrderFilled { fill, order } => {
                fill.validate()?;
                order.validate()?;
                validate_fill_order(fill, order)?;
                if !fill.remaining_quantity.get().is_zero() {
                    return Err(ExecutionEventError::FinalFillWithRemainingQuantity);
                }
                Ok(())
            }
            Self::OrderCancelled {
                venue_order_ref,
                order,
                ..
            }
            | Self::OrderExpired {
                venue_order_ref,
                order,
            } => {
                if let Some(order) = order {
                    order.validate()?;
                    if &order.venue_order_ref != venue_order_ref {
                        return Err(ExecutionEventError::OrderReferenceMismatch);
                    }
                }
                Ok(())
            }
            Self::TargetsChanged { targets, .. } => {
                validate_collection_bound("position target", targets.len(), MAX_POSITION_TARGETS)?;
                for target in targets {
                    target.validate()?;
                }
                Ok(())
            }
            Self::PositionChanged { position, fill } => {
                position.validate()?;
                if let Some(fill) = fill {
                    fill.validate()?;
                }
                Ok(())
            }
            Self::PositionClosed { close } => close.validate(),
            Self::Reconciled {
                order, position, ..
            } => {
                if order.is_none() && position.is_none() {
                    return Err(ExecutionEventError::EmptyReconciliation);
                }
                if let Some(order) = order {
                    order.validate()?;
                }
                if let Some(position) = position {
                    position.validate()?;
                }
                Ok(())
            }
        }
    }
}

impl TryFrom<ExecutionEventWire> for ExecutionEvent {
    type Error = ExecutionEventError;

    fn try_from(value: ExecutionEventWire) -> Result<Self, Self::Error> {
        let event = match value {
            ExecutionEventWire::VenueAccepted { order } => Self::VenueAccepted { order },
            ExecutionEventWire::VenueRejected {
                category,
                message,
                venue_order_ref,
            } => Self::VenueRejected {
                category,
                message,
                venue_order_ref,
            },
            ExecutionEventWire::OrderPartiallyFilled { fill, order } => {
                Self::OrderPartiallyFilled { fill, order }
            }
            ExecutionEventWire::OrderFilled { fill, order } => Self::OrderFilled { fill, order },
            ExecutionEventWire::OrderCancelled {
                venue_order_ref,
                order,
                reason,
            } => Self::OrderCancelled {
                venue_order_ref,
                order,
                reason,
            },
            ExecutionEventWire::OrderExpired {
                venue_order_ref,
                order,
            } => Self::OrderExpired {
                venue_order_ref,
                order,
            },
            ExecutionEventWire::ProtectionChanged {
                venue_position_ref,
                protection,
            } => Self::ProtectionChanged {
                venue_position_ref,
                protection,
            },
            ExecutionEventWire::TargetsChanged {
                venue_position_ref,
                targets,
            } => Self::TargetsChanged {
                venue_position_ref,
                targets,
            },
            ExecutionEventWire::PositionChanged { position, fill } => {
                Self::PositionChanged { position, fill }
            }
            ExecutionEventWire::PositionClosed { close } => Self::PositionClosed { close },
            ExecutionEventWire::Reconciled {
                source,
                order,
                position,
                note,
            } => Self::Reconciled {
                source,
                order,
                position,
                note,
            },
        };
        event.validate()?;
        Ok(event)
    }
}

impl<'de> Deserialize<'de> for ExecutionEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ExecutionEventWire::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

fn validate_fill_order(
    fill: &CanonicalFill,
    order: &CanonicalOrderSnapshot,
) -> Result<(), ExecutionEventError> {
    if fill.venue_order_ref != order.venue_order_ref
        || fill.side != order.side
        || fill.cumulative_quantity != order.cumulative_quantity
        || fill.remaining_quantity != order.remaining_quantity
    {
        return Err(ExecutionEventError::FillOrderMismatch);
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionReport {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub intent_id: Option<TradeIntentId>,
    pub command_id: Option<ExecutionCommandId>,
    pub execution_venue: ExecutionVenueId,
    pub instrument_id: InstrumentId,
    pub event_time: DateTimeUtc,
    pub received_at: DateTimeUtc,
    pub venue_sequence: Option<VenueSequence>,
    pub event: ExecutionEvent,
    pub opaque_payload_ref: Option<OpaquePayloadRef>,
}

impl PartialEq for ExecutionReport {
    fn eq(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.execution_venue == other.execution_venue
            && self.instrument_id == other.instrument_id
            && self.event_time == other.event_time
            && self.venue_sequence == other.venue_sequence
            && self.event == other.event
            && self.opaque_payload_ref == other.opaque_payload_ref
    }
}

impl Eq for ExecutionReport {}

impl ExecutionReport {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        intent_id: Option<TradeIntentId>,
        command_id: Option<ExecutionCommandId>,
        execution_venue: ExecutionVenueId,
        instrument_id: InstrumentId,
        event_time: DateTimeUtc,
        received_at: DateTimeUtc,
        venue_sequence: Option<VenueSequence>,
        event: ExecutionEvent,
        opaque_payload_ref: Option<OpaquePayloadRef>,
    ) -> Result<Self, ExecutionEventError> {
        event.validate()?;
        Ok(Self {
            schema_version: EXECUTION_SCHEMA_VERSION,
            intent_id,
            command_id,
            execution_venue,
            instrument_id,
            event_time,
            received_at,
            venue_sequence,
            event,
            opaque_payload_ref,
        })
    }

    pub fn venue_fallback_key(&self) -> VenueEventDedupKey {
        match &self.venue_sequence {
            Some(venue_sequence) => VenueEventDedupKey::Sequenced {
                execution_venue: self.execution_venue.clone(),
                instrument_id: self.instrument_id.clone(),
                venue_sequence: venue_sequence.clone(),
            },
            None => VenueEventDedupKey::Unsequenced {
                execution_venue: self.execution_venue.clone(),
                instrument_id: self.instrument_id.clone(),
                event_time: self.event_time,
                event: Box::new(self.event.clone()),
                opaque_payload_ref: self.opaque_payload_ref.clone(),
            },
        }
    }

    pub fn compare(&self, other: &Self) -> ReportComparison {
        compare_report_parts(
            self.venue_fallback_key() == other.venue_fallback_key(),
            self == other,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum VenueEventDedupKey {
    Sequenced {
        execution_venue: ExecutionVenueId,
        instrument_id: InstrumentId,
        venue_sequence: VenueSequence,
    },
    Unsequenced {
        execution_venue: ExecutionVenueId,
        instrument_id: InstrumentId,
        event_time: DateTimeUtc,
        event: Box<ExecutionEvent>,
        opaque_payload_ref: Option<OpaquePayloadRef>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ReportComparison {
    Distinct,
    Duplicate,
    Conflict,
}

fn compare_report_parts(same_id: bool, same_report: bool) -> ReportComparison {
    if !same_id {
        ReportComparison::Distinct
    } else if same_report {
        ReportComparison::Duplicate
    } else {
        ReportComparison::Conflict
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FutureEffectReportContext {
    pub report_namespace: ReportNamespace,
    pub report_counter: u64,
    pub execution_venue: ExecutionVenueId,
    pub instrument_id: InstrumentId,
    pub event_time: DateTimeUtc,
    pub received_at: DateTimeUtc,
    pub command_id: Option<ExecutionCommandId>,
    pub intent_id: Option<TradeIntentId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FutureEffectNoReportReason {
    InformationalRuleEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum UnsupportedFutureEffectReason {
    NonAuthoritativeOrderPlacement,
    MissingAuthoritativeFill,
    UnexpectedAuthoritativeFill,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FutureEffectReportDisposition {
    Reports(Vec<ExecutionReport>),
    NoReport(FutureEffectNoReportReason),
    Unsupported(UnsupportedFutureEffectReason),
}

pub fn execution_reports_from_future_effect(
    effect: &FutureEffect,
    context: &FutureEffectReportContext,
) -> Result<FutureEffectReportDisposition, ExecutionEventError> {
    match effect {
        FutureEffect::Plain {
            effect,
            requested_price,
            ..
        } => bridge_plain_effect(effect, *requested_price, context),
        FutureEffect::Filled { effect, fill, .. } => bridge_filled_effect(
            effect,
            fill.execution.side,
            fill.execution.price,
            fill.size,
            context,
        ),
    }
}

fn bridge_plain_effect(
    effect: &Effect,
    requested_price: Option<f64>,
    context: &FutureEffectReportContext,
) -> Result<FutureEffectReportDisposition, ExecutionEventError> {
    let event = match effect {
        Effect::OrderCancelled { id } => ExecutionEvent::OrderCancelled {
            venue_order_ref: venue_order_ref(id)?,
            order: None,
            reason: None,
        },
        Effect::StoplossModified { id, new_price, .. } => ExecutionEvent::ProtectionChanged {
            venue_position_ref: venue_position_ref(id)?,
            protection: CanonicalProtection {
                stop_loss: Some(price_from_f64(requested_price.unwrap_or(*new_price))?),
            },
        },
        Effect::StoplossRemoved { id, .. } => ExecutionEvent::ProtectionChanged {
            venue_position_ref: venue_position_ref(id)?,
            protection: CanonicalProtection::default(),
        },
        Effect::RuleTriggered { .. } => {
            return Ok(FutureEffectReportDisposition::NoReport(
                FutureEffectNoReportReason::InformationalRuleEffect,
            ));
        }
        Effect::OrderPlaced { .. } => {
            return Ok(FutureEffectReportDisposition::Unsupported(
                UnsupportedFutureEffectReason::NonAuthoritativeOrderPlacement,
            ));
        }
        Effect::PositionOpened { .. }
        | Effect::PositionClosed { .. }
        | Effect::PartialClose { .. }
        | Effect::ScaledIn { .. } => {
            return Ok(FutureEffectReportDisposition::Unsupported(
                UnsupportedFutureEffectReason::MissingAuthoritativeFill,
            ));
        }
    };
    Ok(FutureEffectReportDisposition::Reports(vec![bridge_report(
        context, 0, event,
    )?]))
}

fn bridge_filled_effect(
    effect: &Effect,
    side: Side,
    fill_price: f64,
    fill_size: f64,
    context: &FutureEffectReportContext,
) -> Result<FutureEffectReportDisposition, ExecutionEventError> {
    let price = price_from_f64(fill_price)?;
    let quantity = positive_quantity_from_f64("future fill", fill_size)?;
    match effect {
        Effect::PositionOpened { id } => {
            let order_ref = venue_order_ref(id)?;
            let fill = completed_fill(order_ref.clone(), side, price, quantity)?;
            let order = completed_order(order_ref, side, price, quantity)?;
            let position = CanonicalPositionSnapshot::new(
                venue_position_ref(id)?,
                side,
                Quantity::new(Decimal::ZERO)
                    .map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))?,
                quantity,
                Some(price),
                CanonicalProtection::default(),
                Vec::new(),
            )?;
            Ok(FutureEffectReportDisposition::Reports(vec![
                bridge_report(context, 0, ExecutionEvent::OrderFilled { fill, order })?,
                bridge_report(
                    context,
                    1,
                    ExecutionEvent::PositionChanged {
                        position,
                        fill: None,
                    },
                )?,
            ]))
        }
        Effect::ScaledIn { id, .. } => {
            let order_ref = venue_order_ref(id)?;
            let fill = completed_fill(order_ref.clone(), side, price, quantity)?;
            let order = completed_order(order_ref, side, price, quantity)?;
            Ok(FutureEffectReportDisposition::Reports(vec![bridge_report(
                context,
                0,
                ExecutionEvent::OrderFilled { fill, order },
            )?]))
        }
        Effect::PositionClosed { id, reason } => {
            let close = CanonicalClose::new(
                venue_position_ref(id)?,
                side,
                quantity,
                price,
                canonical_close_reason(*reason),
                Vec::new(),
            )?;
            Ok(FutureEffectReportDisposition::Reports(vec![bridge_report(
                context,
                0,
                ExecutionEvent::PositionClosed { close },
            )?]))
        }
        Effect::PartialClose { id, ratio, .. } => {
            if !ratio.is_finite() || *ratio <= 0.0 || *ratio >= 1.0 {
                return Err(ExecutionEventError::InvalidPartialCloseRatio);
            }
            let quantity_before =
                positive_quantity_from_f64("position before partial close", fill_size / ratio)?;
            let quantity_after = quantity_from_f64(fill_size * (1.0 - ratio) / ratio)?;
            let position = CanonicalPositionSnapshot::new(
                venue_position_ref(id)?,
                side,
                quantity_before,
                quantity_after,
                None,
                CanonicalProtection::default(),
                Vec::new(),
            )?;
            let order_ref = venue_order_ref(id)?;
            let fill = completed_fill(order_ref, side, price, quantity)?;
            Ok(FutureEffectReportDisposition::Reports(vec![bridge_report(
                context,
                0,
                ExecutionEvent::PositionChanged {
                    position,
                    fill: Some(fill),
                },
            )?]))
        }
        Effect::OrderPlaced { .. }
        | Effect::OrderCancelled { .. }
        | Effect::StoplossModified { .. }
        | Effect::StoplossRemoved { .. }
        | Effect::RuleTriggered { .. } => Ok(FutureEffectReportDisposition::Unsupported(
            UnsupportedFutureEffectReason::UnexpectedAuthoritativeFill,
        )),
    }
}

fn bridge_report(
    context: &FutureEffectReportContext,
    offset: u64,
    event: ExecutionEvent,
) -> Result<ExecutionReport, ExecutionEventError> {
    let counter = context
        .report_counter
        .checked_add(offset)
        .ok_or(ExecutionEventError::ReportCounterOverflow)?;
    let venue_sequence =
        VenueSequence::new(format!("{}:{counter}", context.report_namespace.as_str()))?;
    ExecutionReport::new(
        context.intent_id.clone(),
        context.command_id.clone(),
        context.execution_venue.clone(),
        context.instrument_id.clone(),
        context.event_time,
        context.received_at,
        Some(venue_sequence),
        event,
        None,
    )
}

fn completed_fill(
    venue_order_ref: VenueOrderRef,
    side: Side,
    price: Price,
    quantity: Quantity,
) -> Result<CanonicalFill, ExecutionEventError> {
    CanonicalFill::new(
        None,
        venue_order_ref,
        side,
        price,
        quantity,
        quantity,
        quantity_from_decimal(Decimal::ZERO)?,
        LiquidityRole::Unknown,
        Vec::new(),
    )
}

fn completed_order(
    venue_order_ref: VenueOrderRef,
    side: Side,
    price: Price,
    quantity: Quantity,
) -> Result<CanonicalOrderSnapshot, ExecutionEventError> {
    CanonicalOrderSnapshot::new(
        venue_order_ref,
        side,
        quantity,
        quantity,
        quantity_from_decimal(Decimal::ZERO)?,
        Some(price),
    )
}

fn quantity_from_decimal(value: Decimal) -> Result<Quantity, ExecutionEventError> {
    Quantity::new(value).map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))
}

fn quantity_from_f64(value: f64) -> Result<Quantity, ExecutionEventError> {
    let decimal = Decimal::checked_from_f64(value)
        .map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))?;
    quantity_from_decimal(decimal)
}

fn positive_quantity_from_f64(
    kind: &'static str,
    value: f64,
) -> Result<Quantity, ExecutionEventError> {
    let quantity = quantity_from_f64(value)?;
    require_positive_quantity(kind, quantity)?;
    Ok(quantity)
}

fn price_from_f64(value: f64) -> Result<Price, ExecutionEventError> {
    let decimal = Decimal::checked_from_f64(value)
        .map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))?;
    Price::new(decimal).map_err(|error| ExecutionEventError::InvalidDecimal(error.to_string()))
}

fn venue_order_ref(value: &str) -> Result<VenueOrderRef, ExecutionEventError> {
    VenueOrderRef::new(value)
        .map_err(|error| ExecutionEventError::InvalidIdentity(error.to_string()))
}

fn venue_position_ref(value: &str) -> Result<VenuePositionRef, ExecutionEventError> {
    VenuePositionRef::new(value)
        .map_err(|error| ExecutionEventError::InvalidIdentity(error.to_string()))
}

fn canonical_close_reason(reason: CloseReason) -> CanonicalCloseReason {
    match reason {
        CloseReason::Stoploss => CanonicalCloseReason::StopLoss,
        CloseReason::Target => CanonicalCloseReason::Target,
        CloseReason::TrailingStop => CanonicalCloseReason::TrailingStop,
        CloseReason::TimeExit => CanonicalCloseReason::TimeExit,
        CloseReason::BreakevenStop => CanonicalCloseReason::BreakevenStop,
        CloseReason::Manual => CanonicalCloseReason::Manual,
        CloseReason::EndOfData => CanonicalCloseReason::EndOfData,
        CloseReason::GroupRule => CanonicalCloseReason::GroupRule,
        CloseReason::Cancelled => CanonicalCloseReason::Cancelled,
    }
}
