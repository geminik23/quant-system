use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;

use chrono::{FixedOffset, NaiveDateTime, TimeZone, Utc};
use qs_instruments::{Decimal, Money, PositiveDecimal, Price, Quantity, ResolvedInstrumentRef};
use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::canonical::{
    DateTimeUtc, DurationMillis, ExecutionCapability, IntentCampaignRef, IntentCorrelationId,
    IntentIdentityNamespace, IntentPositionRef, IntentProducerId, IntentStateRef,
    OpaqueProvenanceRef, OperatingMode, PositiveFraction, PriceDistance, TradeIntentId,
};
use crate::profile::{PositionRef, RawSignal};
use crate::types::{GroupId, OrderType, Side, TradeId};

pub const TRADE_INTENT_SCHEMA_VERSION: u32 = 1;
pub const MAX_PROVENANCE_REFS: usize = 32;
pub const MAX_TARGET_HINTS: usize = 32;
pub const MAX_SUPERSEDES: usize = 32;
pub const MAX_ALLOWED_MODES: usize = 8;
pub const MAX_REQUIRED_CAPABILITIES: usize = 32;
pub const MAX_RESOLVED_TARGETS: usize = 256;
pub const MAX_PRODUCER_REVISION_BYTES: usize = 64;
pub const MAX_DERIVED_FIELD_BYTES: usize = 96;
pub const MAX_COMPATIBILITY_TEXT_BYTES: usize = 160;

const LEGACY_GROUP_PREFIX: &str = "raw-signal-group:";

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IntentValidationError {
    #[error("unsupported trade intent schema version {0}")]
    UnsupportedSchema(u32),
    #[error("expiration must be after the effective time")]
    InvalidExpiration,
    #[error("{kind} must contain between 1 and {maximum} bytes of bounded ASCII text")]
    InvalidText { kind: &'static str, maximum: usize },
    #[error("{collection} contains {actual} values, maximum is {maximum}")]
    CollectionTooLarge {
        collection: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{collection} contains duplicate values")]
    DuplicateCollectionValue { collection: &'static str },
    #[error("an intent cannot supersede itself")]
    SelfSupersession,
    #[error("target hints contain duplicate price references")]
    DuplicateTargetPriceReference,
    #[error("specified target close fractions exceed one")]
    TargetCloseFractionsExceedOne,
    #[error("account risk amount must be positive")]
    NonPositiveAccountAmount,
    #[error("explicit quantity must be positive")]
    NonPositiveQuantity,
    #[error("expected state reference and revision must either both be present or both be absent")]
    IncompleteExpectedState,
    #[error("position selector is invalid for scope {0:?}")]
    InvalidSelector(SelectorScope),
    #[error("position selector instrument differs from the intent envelope instrument")]
    SelectorInstrumentMismatch,
    #[error("catalog snapshot version is invalid")]
    InvalidCatalogVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentProducerKind {
    ExternalSignal,
    Strategy,
    Manual,
    Api,
    Reconciliation,
    System,
    LegacyUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentProvenance {
    pub producer: IntentProducerId,
    pub producer_kind: IntentProducerKind,
    #[serde(deserialize_with = "deserialize_producer_revision")]
    pub producer_revision: String,
    pub correlation: Option<IntentCorrelationId>,
    #[serde(deserialize_with = "deserialize_provenance_refs")]
    pub source_refs: Vec<OpaqueProvenanceRef>,
}

impl IntentProvenance {
    pub fn legacy_unknown(producer: IntentProducerId, producer_revision: String) -> Self {
        Self {
            producer,
            producer_kind: IntentProducerKind::LegacyUnknown,
            producer_revision,
            correlation: None,
            source_refs: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), IntentValidationError> {
        validate_bounded_text(
            "producer revision",
            &self.producer_revision,
            MAX_PRODUCER_REVISION_BYTES,
        )?;
        validate_collection_bound(
            "provenance source references",
            self.source_refs.len(),
            MAX_PROVENANCE_REFS,
        )?;
        if has_duplicates(&self.source_refs) {
            return Err(IntentValidationError::DuplicateCollectionValue {
                collection: "provenance source references",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedStateRevision {
    pub state: Option<IntentStateRef>,
    pub revision: Option<u64>,
}

impl ExpectedStateRevision {
    pub fn validate(&self) -> Result<(), IntentValidationError> {
        if self.state.is_some() != self.revision.is_some() {
            return Err(IntentValidationError::IncompleteExpectedState);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentConstraints {
    #[serde(deserialize_with = "deserialize_allowed_modes")]
    pub allowed_modes: BTreeSet<OperatingMode>,
    pub maximum_slippage: Option<PriceDistance>,
    pub maximum_age: Option<DurationMillis>,
    pub reduce_only: bool,
    #[serde(deserialize_with = "deserialize_required_capabilities")]
    pub required_capabilities: BTreeSet<ExecutionCapability>,
    #[serde(deserialize_with = "deserialize_supersedes")]
    pub supersedes: Vec<TradeIntentId>,
}

impl IntentConstraints {
    pub fn validate(&self) -> Result<(), IntentValidationError> {
        validate_collection_bound(
            "allowed operating modes",
            self.allowed_modes.len(),
            MAX_ALLOWED_MODES,
        )?;
        validate_collection_bound(
            "required capabilities",
            self.required_capabilities.len(),
            MAX_REQUIRED_CAPABILITIES,
        )?;
        validate_collection_bound(
            "superseded intent IDs",
            self.supersedes.len(),
            MAX_SUPERSEDES,
        )?;
        if has_duplicates(&self.supersedes) {
            return Err(IntentValidationError::DuplicateCollectionValue {
                collection: "superseded intent IDs",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectorScope {
    ExactPosition,
    Campaign,
    Instrument,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionSelector {
    pub position: Option<IntentPositionRef>,
    pub campaign: Option<IntentCampaignRef>,
    pub instrument: Option<ResolvedInstrumentRef>,
    pub scope: SelectorScope,
}

impl PositionSelector {
    pub fn exact(
        position: IntentPositionRef,
        campaign: Option<IntentCampaignRef>,
        instrument: ResolvedInstrumentRef,
    ) -> Self {
        Self {
            position: Some(position),
            campaign,
            instrument: Some(instrument),
            scope: SelectorScope::ExactPosition,
        }
    }

    pub fn validate(&self) -> Result<(), IntentValidationError> {
        let valid = match self.scope {
            SelectorScope::ExactPosition => self.position.is_some(),
            SelectorScope::Campaign => self.position.is_none() && self.campaign.is_some(),
            SelectorScope::Instrument => self.position.is_none() && self.campaign.is_none(),
        };
        if !valid || self.instrument.is_none() {
            return Err(IntentValidationError::InvalidSelector(self.scope));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RiskRequest {
    UnitMultiplier(PositiveDecimal),
    AccountAmount(Money),
    BalanceFraction(PositiveFraction),
    ExplicitQuantity(Quantity),
    Delegated,
}

impl RiskRequest {
    pub fn validate(&self) -> Result<(), IntentValidationError> {
        match self {
            Self::AccountAmount(amount) if !amount.amount.is_positive() => {
                Err(IntentValidationError::NonPositiveAccountAmount)
            }
            Self::ExplicitQuantity(quantity) if quantity.require_positive().is_err() => {
                Err(IntentValidationError::NonPositiveQuantity)
            }
            _ => Ok(()),
        }
    }
}

impl Serialize for RiskRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum Wire<'a> {
            UnitMultiplier { value: &'a PositiveDecimal },
            AccountAmount { value: &'a Money },
            BalanceFraction { value: &'a PositiveFraction },
            ExplicitQuantity { value: &'a Quantity },
            Delegated,
        }

        match self {
            Self::UnitMultiplier(value) => Wire::UnitMultiplier { value }.serialize(serializer),
            Self::AccountAmount(value) => Wire::AccountAmount { value }.serialize(serializer),
            Self::BalanceFraction(value) => Wire::BalanceFraction { value }.serialize(serializer),
            Self::ExplicitQuantity(value) => Wire::ExplicitQuantity { value }.serialize(serializer),
            Self::Delegated => Wire::Delegated.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RiskRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            UnitMultiplier { value: PositiveDecimal },
            AccountAmount { value: Money },
            BalanceFraction { value: PositiveFraction },
            ExplicitQuantity { value: Quantity },
            Delegated,
        }

        let value = match Wire::deserialize(deserializer)? {
            Wire::UnitMultiplier { value } => Self::UnitMultiplier(value),
            Wire::AccountAmount { value } => Self::AccountAmount(value),
            Wire::BalanceFraction { value } => Self::BalanceFraction(value),
            Wire::ExplicitQuantity { value } => Self::ExplicitQuantity(value),
            Wire::Delegated => Self::Delegated,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PriceReference {
    Absolute(Price),
    EntryAverage,
    DerivedArtifact {
        artifact_ref: OpaqueProvenanceRef,
        field: String,
    },
}

impl PriceReference {
    pub fn validate(&self) -> Result<(), IntentValidationError> {
        if let Self::DerivedArtifact { field, .. } = self {
            validate_bounded_text("derived price field", field, MAX_DERIVED_FIELD_BYTES)?;
        }
        Ok(())
    }
}

impl Serialize for PriceReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum Wire<'a> {
            Absolute {
                value: &'a Price,
            },
            EntryAverage,
            DerivedArtifact {
                artifact_ref: &'a OpaqueProvenanceRef,
                field: &'a str,
            },
        }

        match self {
            Self::Absolute(value) => Wire::Absolute { value }.serialize(serializer),
            Self::EntryAverage => Wire::EntryAverage.serialize(serializer),
            Self::DerivedArtifact {
                artifact_ref,
                field,
            } => Wire::DerivedArtifact {
                artifact_ref,
                field,
            }
            .serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for PriceReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Absolute {
                value: Price,
            },
            EntryAverage,
            DerivedArtifact {
                artifact_ref: OpaqueProvenanceRef,
                #[serde(deserialize_with = "deserialize_derived_field")]
                field: String,
            },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Absolute { value } => Self::Absolute(value),
            Wire::EntryAverage => Self::EntryAverage,
            Wire::DerivedArtifact {
                artifact_ref,
                field,
            } => Self::DerivedArtifact {
                artifact_ref,
                field,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum OrderPreference {
    Market,
    Limit {
        limit: PriceReference,
    },
    Stop {
        trigger: PriceReference,
    },
    StopLimit {
        trigger: PriceReference,
        limit: PriceReference,
    },
}

impl OrderPreference {
    fn validate(&self) -> Result<(), IntentValidationError> {
        match self {
            Self::Market => Ok(()),
            Self::Limit { limit } => limit.validate(),
            Self::Stop { trigger } => trigger.validate(),
            Self::StopLimit { trigger, limit } => {
                trigger.validate()?;
                limit.validate()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetHint {
    pub price: PriceReference,
    pub close_fraction: Option<PositiveFraction>,
}

impl TargetHint {
    fn validate(&self) -> Result<(), IntentValidationError> {
        self.price.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProtectionRequest {
    Clear,
    StopLoss { stop: PriceReference },
    Breakeven,
}

impl ProtectionRequest {
    fn validate(&self) -> Result<(), IntentValidationError> {
        match self {
            Self::StopLoss { stop } => stop.validate(),
            Self::Clear | Self::Breakeven => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReductionRequest {
    Quantity(Quantity),
    Fraction(PositiveFraction),
    AllRemaining,
}

impl ReductionRequest {
    pub fn validate(&self) -> Result<(), IntentValidationError> {
        if let Self::Quantity(quantity) = self
            && quantity.require_positive().is_err()
        {
            return Err(IntentValidationError::NonPositiveQuantity);
        }
        Ok(())
    }
}

impl Serialize for ReductionRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum Wire<'a> {
            Quantity { value: &'a Quantity },
            Fraction { value: &'a PositiveFraction },
            AllRemaining,
        }

        match self {
            Self::Quantity(value) => Wire::Quantity { value }.serialize(serializer),
            Self::Fraction(value) => Wire::Fraction { value }.serialize(serializer),
            Self::AllRemaining => Wire::AllRemaining.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ReductionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Quantity { value: Quantity },
            Fraction { value: PositiveFraction },
            AllRemaining,
        }

        let value = match Wire::deserialize(deserializer)? {
            Wire::Quantity { value } => Self::Quantity(value),
            Wire::Fraction { value } => Self::Fraction(value),
            Wire::AllRemaining => Self::AllRemaining,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryIntent {
    #[serde(with = "intent_side_serde")]
    pub side: Side,
    pub order: OrderPreference,
    pub entry_reference: Option<PriceReference>,
    pub invalidation: Option<PriceReference>,
    #[serde(deserialize_with = "deserialize_target_hints")]
    pub target_hints: Vec<TargetHint>,
    pub risk_request: RiskRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReduceIntent {
    pub position: PositionSelector,
    pub reduction: ReductionRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitIntent {
    pub position: PositionSelector,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceProtectionIntent {
    pub position: PositionSelector,
    pub protection: ProtectionRequest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceTargetsIntent {
    pub position: PositionSelector,
    #[serde(deserialize_with = "deserialize_target_hints")]
    pub targets: Vec<TargetHint>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddTrancheIntent {
    pub position: PositionSelector,
    pub order: OrderPreference,
    pub entry_reference: Option<PriceReference>,
    pub quantity: Quantity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelEntryIntent {
    pub position: PositionSelector,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlattenScopeIntent {
    pub position: PositionSelector,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntentAction {
    Enter(EntryIntent),
    Reduce(ReduceIntent),
    Exit(ExitIntent),
    ReplaceProtection(ReplaceProtectionIntent),
    ReplaceTargets(ReplaceTargetsIntent),
    AddTranche(AddTrancheIntent),
    CancelEntry(CancelEntryIntent),
    FlattenScope(FlattenScopeIntent),
}

impl IntentAction {
    fn validate(&self, envelope: &ResolvedInstrumentRef) -> Result<(), IntentValidationError> {
        match self {
            Self::Enter(entry) => {
                entry.order.validate()?;
                if let Some(reference) = &entry.entry_reference {
                    reference.validate()?;
                }
                if let Some(invalidation) = &entry.invalidation {
                    invalidation.validate()?;
                }
                validate_target_hints(&entry.target_hints)?;
                entry.risk_request.validate()
            }
            Self::Reduce(intent) => {
                validate_selector(&intent.position, envelope)?;
                intent.reduction.validate()
            }
            Self::Exit(intent) => validate_selector(&intent.position, envelope),
            Self::ReplaceProtection(intent) => {
                validate_selector(&intent.position, envelope)?;
                intent.protection.validate()
            }
            Self::ReplaceTargets(intent) => {
                validate_selector(&intent.position, envelope)?;
                validate_target_hints(&intent.targets)
            }
            Self::AddTranche(intent) => {
                validate_selector(&intent.position, envelope)?;
                intent.order.validate()?;
                if let Some(reference) = &intent.entry_reference {
                    reference.validate()?;
                }
                if intent.quantity.require_positive().is_err() {
                    return Err(IntentValidationError::NonPositiveQuantity);
                }
                Ok(())
            }
            Self::CancelEntry(intent) => validate_selector(&intent.position, envelope),
            Self::FlattenScope(intent) => validate_selector(&intent.position, envelope),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TradeIntent {
    pub schema_version: u32,
    pub intent_id: TradeIntentId,
    pub instrument: ResolvedInstrumentRef,
    pub created_at: DateTimeUtc,
    pub effective_at: DateTimeUtc,
    pub expires_at: Option<DateTimeUtc>,
    pub expected_state: ExpectedStateRevision,
    pub provenance: IntentProvenance,
    pub action: IntentAction,
    pub constraints: IntentConstraints,
}

impl<'de> Deserialize<'de> for TradeIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema_version: u32,
            intent_id: TradeIntentId,
            instrument: ResolvedInstrumentRef,
            created_at: DateTimeUtc,
            effective_at: DateTimeUtc,
            expires_at: Option<DateTimeUtc>,
            expected_state: ExpectedStateRevision,
            provenance: IntentProvenance,
            action: IntentAction,
            constraints: IntentConstraints,
        }

        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            intent_id: wire.intent_id,
            instrument: wire.instrument,
            created_at: wire.created_at,
            effective_at: wire.effective_at,
            expires_at: wire.expires_at,
            expected_state: wire.expected_state,
            provenance: wire.provenance,
            action: wire.action,
            constraints: wire.constraints,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

impl TradeIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        intent_id: TradeIntentId,
        instrument: ResolvedInstrumentRef,
        created_at: DateTimeUtc,
        effective_at: DateTimeUtc,
        expires_at: Option<DateTimeUtc>,
        expected_state: ExpectedStateRevision,
        provenance: IntentProvenance,
        action: IntentAction,
        constraints: IntentConstraints,
    ) -> Result<Self, IntentValidationError> {
        let value = Self {
            schema_version: TRADE_INTENT_SCHEMA_VERSION,
            intent_id,
            instrument,
            created_at,
            effective_at,
            expires_at,
            expected_state,
            provenance,
            action,
            constraints,
        };
        value.validate()?;
        Ok(value)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_deterministic_id(
        namespace: &IntentIdentityNamespace,
        action_ordinal: u32,
        instrument: ResolvedInstrumentRef,
        created_at: DateTimeUtc,
        effective_at: DateTimeUtc,
        expires_at: Option<DateTimeUtc>,
        expected_state: ExpectedStateRevision,
        provenance: IntentProvenance,
        action: IntentAction,
        constraints: IntentConstraints,
    ) -> Result<Self, IntentValidationError> {
        let intent_id = deterministic_trade_intent_id(namespace, action_ordinal);
        Self::new(
            intent_id,
            instrument,
            created_at,
            effective_at,
            expires_at,
            expected_state,
            provenance,
            action,
            constraints,
        )
    }

    pub fn validate(&self) -> Result<(), IntentValidationError> {
        if self.schema_version != TRADE_INTENT_SCHEMA_VERSION {
            return Err(IntentValidationError::UnsupportedSchema(
                self.schema_version,
            ));
        }
        if self
            .expires_at
            .is_some_and(|expires| expires <= self.effective_at)
        {
            return Err(IntentValidationError::InvalidExpiration);
        }
        validate_catalog_version(&self.instrument)?;
        self.expected_state.validate()?;
        self.provenance.validate()?;
        self.constraints.validate()?;
        if self.constraints.supersedes.contains(&self.intent_id) {
            return Err(IntentValidationError::SelfSupersession);
        }
        self.action.validate(&self.instrument)
    }
}

fn deterministic_trade_intent_id(
    namespace: &IntentIdentityNamespace,
    action_ordinal: u32,
) -> TradeIntentId {
    TradeIntentId::new(format!("intent:{namespace}:{action_ordinal}"))
        .expect("deterministic trade intent IDs satisfy canonical ID validation")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntentIdentityDisposition {
    Distinct,
    Duplicate,
    Conflict,
}

pub fn classify_intent_identity(
    existing: &TradeIntent,
    candidate: &TradeIntent,
) -> IntentIdentityDisposition {
    if existing.intent_id != candidate.intent_id {
        IntentIdentityDisposition::Distinct
    } else if existing == candidate {
        IntentIdentityDisposition::Duplicate
    } else {
        IntentIdentityDisposition::Conflict
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementTargetDesiredState {
    pub protection: Option<ProtectionRequest>,
    #[serde(deserialize_with = "deserialize_target_hints")]
    pub targets: Vec<TargetHint>,
}

impl ManagementTargetDesiredState {
    fn validate(&self) -> Result<(), IntentValidationError> {
        if let Some(protection) = &self.protection {
            protection.validate()?;
        }
        validate_target_hints(&self.targets)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRawSignalTarget {
    pub position: IntentPositionRef,
    pub campaign: Option<IntentCampaignRef>,
    pub instrument: ResolvedInstrumentRef,
    pub desired_state: Option<ManagementTargetDesiredState>,
    pub pending_entry: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RawSignalTimestampPolicy {
    AssumeUtc,
    FixedOffsetSeconds { seconds: i32 },
}

impl RawSignalTimestampPolicy {
    pub fn resolve(self, value: NaiveDateTime) -> Result<DateTimeUtc, RawSignalAdaptationError> {
        match self {
            Self::AssumeUtc => Ok(DateTimeUtc::from_naive_utc(value)),
            Self::FixedOffsetSeconds { seconds } => {
                let offset = FixedOffset::east_opt(seconds)
                    .ok_or(RawSignalAdaptationError::InvalidTimestampOffset(seconds))?;
                let timestamp = offset
                    .from_local_datetime(&value)
                    .single()
                    .ok_or(RawSignalAdaptationError::InvalidTimestamp)?;
                Ok(DateTimeUtc::new(timestamp.with_timezone(&Utc)))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSignalAdaptationContext {
    pub timestamp_policy: RawSignalTimestampPolicy,
    pub base_provenance: IntentProvenance,
    pub identity_namespace: IntentIdentityNamespace,
    pub constraints: IntentConstraints,
    pub resolved_entry_instrument: Option<ResolvedInstrumentRef>,
    pub management_targets: Vec<ResolvedRawSignalTarget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawSignalActionClassification {
    Entry,
    Close,
    ClosePartial,
    ModifyStoploss,
    MoveStoplossToEntry,
    AddTarget,
    RemoveTarget,
    ModifyTarget,
    AddRule,
    RemoveRule,
    ScaleIn,
    CancelPending,
    CloseAllOf,
    CloseAll,
    CancelAllPending,
    ModifyAllStoploss,
    CloseAllInGroup,
    ModifyAllStoplossInGroup,
}

impl RawSignalActionClassification {
    pub fn of(signal: &RawSignal) -> Self {
        match signal {
            RawSignal::Entry { .. } => Self::Entry,
            RawSignal::Close { .. } => Self::Close,
            RawSignal::ClosePartial { .. } => Self::ClosePartial,
            RawSignal::ModifyStoploss { .. } => Self::ModifyStoploss,
            RawSignal::MoveStoplossToEntry { .. } => Self::MoveStoplossToEntry,
            RawSignal::AddTarget { .. } => Self::AddTarget,
            RawSignal::RemoveTarget { .. } => Self::RemoveTarget,
            RawSignal::ModifyTarget { .. } => Self::ModifyTarget,
            RawSignal::AddRule { .. } => Self::AddRule,
            RawSignal::RemoveRule { .. } => Self::RemoveRule,
            RawSignal::ScaleIn { .. } => Self::ScaleIn,
            RawSignal::CancelPending { .. } => Self::CancelPending,
            RawSignal::CloseAllOf { .. } => Self::CloseAllOf,
            RawSignal::CloseAll { .. } => Self::CloseAll,
            RawSignal::CancelAllPending { .. } => Self::CancelAllPending,
            RawSignal::ModifyAllStoploss { .. } => Self::ModifyAllStoploss,
            RawSignal::CloseAllInGroup { .. } => Self::CloseAllInGroup,
            RawSignal::ModifyAllStoplossInGroup { .. } => Self::ModifyAllStoplossInGroup,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityOnlyReason {
    RuleMutationHasNoCanonicalIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawSignalAdaptationOutcome {
    Intents(Vec<TradeIntent>),
    CompatibilityOnly {
        action: RawSignalActionClassification,
        reason: CompatibilityOnlyReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RawSignalAdaptationError {
    #[error("entry instrument was not resolved")]
    UnresolvedEntryInstrument,
    #[error("management target was not resolved")]
    UnresolvedManagementTarget,
    #[error("resolved target count {actual} exceeds maximum {maximum}")]
    TooManyResolvedTargets { maximum: usize, actual: usize },
    #[error("resolved target desired state is required for target changes")]
    MissingTargetState,
    #[error("target price was not found in current desired state")]
    MissingTarget,
    #[error("target price is ambiguous in current desired state")]
    AmbiguousTarget,
    #[error("cancel pending requires a pending entry target")]
    TargetIsNotPending,
    #[error("raw signal timestamp offset {0} is invalid")]
    InvalidTimestampOffset(i32),
    #[error("raw signal timestamp cannot be represented")]
    InvalidTimestamp,
    #[error("{field} must be finite and positive")]
    InvalidPositiveNumber { field: &'static str },
    #[error("{field} must be greater than zero and at most one")]
    InvalidFraction { field: &'static str },
    #[error("raw signal field cannot be represented canonically: {0}")]
    InvalidCompatibilityValue(String),
    #[error(transparent)]
    InvalidIntent(#[from] IntentValidationError),
}

pub trait RawSignalIntentAdapter {
    fn adapt(
        &self,
        signal: &RawSignal,
        context: &RawSignalAdaptationContext,
    ) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CanonicalRawSignalIntentAdapter;

impl RawSignalIntentAdapter for CanonicalRawSignalIntentAdapter {
    fn adapt(
        &self,
        signal: &RawSignal,
        context: &RawSignalAdaptationContext,
    ) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
        adapt_raw_signal(signal, context)
    }
}

pub fn adapt_raw_signal(
    signal: &RawSignal,
    context: &RawSignalAdaptationContext,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    match signal {
        RawSignal::AddRule { .. } => {
            return Ok(RawSignalAdaptationOutcome::CompatibilityOnly {
                action: RawSignalActionClassification::AddRule,
                reason: CompatibilityOnlyReason::RuleMutationHasNoCanonicalIntent,
            });
        }
        RawSignal::RemoveRule { .. } => {
            return Ok(RawSignalAdaptationOutcome::CompatibilityOnly {
                action: RawSignalActionClassification::RemoveRule,
                reason: CompatibilityOnlyReason::RuleMutationHasNoCanonicalIntent,
            });
        }
        RawSignal::Entry { .. }
        | RawSignal::Close { .. }
        | RawSignal::ClosePartial { .. }
        | RawSignal::ModifyStoploss { .. }
        | RawSignal::MoveStoplossToEntry { .. }
        | RawSignal::AddTarget { .. }
        | RawSignal::RemoveTarget { .. }
        | RawSignal::ModifyTarget { .. }
        | RawSignal::ScaleIn { .. }
        | RawSignal::CancelPending { .. }
        | RawSignal::CloseAllOf { .. }
        | RawSignal::CloseAll { .. }
        | RawSignal::CancelAllPending { .. }
        | RawSignal::ModifyAllStoploss { .. }
        | RawSignal::CloseAllInGroup { .. }
        | RawSignal::ModifyAllStoplossInGroup { .. } => {}
    }

    validate_adaptation_context(context)?;
    let timestamp = context.timestamp_policy.resolve(signal.ts())?;

    match signal {
        RawSignal::Entry {
            side,
            order_type,
            price,
            risk_multiplier,
            stoploss,
            targets,
            group,
            trade_id,
            ..
        } => adapt_entry(
            *side,
            *order_type,
            *price,
            *risk_multiplier,
            *stoploss,
            targets,
            group.as_deref(),
            trade_id.as_deref(),
            timestamp,
            context,
        ),
        RawSignal::Close { .. } => adapt_direct_targets(context, timestamp, |target| {
            IntentAction::Exit(ExitIntent {
                position: selector_for(target),
            })
        }),
        RawSignal::ClosePartial {
            position: _, ratio, ..
        } => {
            let reduction = ReductionRequest::Fraction(fraction_from_f64(*ratio, "close ratio")?);
            adapt_direct_targets(context, timestamp, |target| {
                IntentAction::Reduce(ReduceIntent {
                    position: selector_for(target),
                    reduction: reduction.clone(),
                })
            })
        }
        RawSignal::ModifyStoploss {
            position: _, price, ..
        } => {
            let stop = absolute_price(*price, "stoploss price")?;
            adapt_direct_targets(context, timestamp, |target| {
                IntentAction::ReplaceProtection(ReplaceProtectionIntent {
                    position: selector_for(target),
                    protection: ProtectionRequest::StopLoss { stop: stop.clone() },
                })
            })
        }
        RawSignal::MoveStoplossToEntry { .. } => {
            adapt_direct_targets(context, timestamp, |target| {
                IntentAction::ReplaceProtection(ReplaceProtectionIntent {
                    position: selector_for(target),
                    protection: ProtectionRequest::Breakeven,
                })
            })
        }
        RawSignal::AddTarget {
            position: _,
            price,
            close_ratio,
            ..
        } => {
            let price = absolute_price(*price, "target price")?;
            let close_fraction = fraction_from_f64(*close_ratio, "target close ratio")?;
            adapt_target_delta(context, timestamp, |state| {
                let mut targets = state.targets.clone();
                targets.push(TargetHint {
                    price: price.clone(),
                    close_fraction: Some(close_fraction),
                });
                Ok(targets)
            })
        }
        RawSignal::RemoveTarget {
            position: _, price, ..
        } => {
            let price = price_from_f64(*price, "target price")?;
            adapt_target_delta(context, timestamp, |state| {
                let index = unique_target_index(&state.targets, price)?;
                let mut targets = state.targets.clone();
                targets.remove(index);
                Ok(targets)
            })
        }
        RawSignal::ModifyTarget {
            position: _,
            old_price,
            new_price,
            ..
        } => {
            let old_price = price_from_f64(*old_price, "old target price")?;
            let new_price = absolute_price(*new_price, "new target price")?;
            adapt_target_delta(context, timestamp, |state| {
                let index = unique_target_index(&state.targets, old_price)?;
                let mut targets = state.targets.clone();
                targets[index].price = new_price.clone();
                Ok(targets)
            })
        }
        RawSignal::ScaleIn {
            position: _,
            price,
            size,
            ..
        } => {
            let quantity = quantity_from_f64(*size, "scale-in size")?;
            let entry_reference = price
                .map(|price| absolute_price(price, "scale-in price"))
                .transpose()?;
            adapt_direct_targets(context, timestamp, |target| {
                IntentAction::AddTranche(AddTrancheIntent {
                    position: selector_for(target),
                    order: OrderPreference::Market,
                    entry_reference: entry_reference.clone(),
                    quantity,
                })
            })
        }
        RawSignal::CancelPending { .. } => {
            let targets = sorted_targets(context);
            if targets.is_empty() {
                return Err(RawSignalAdaptationError::UnresolvedManagementTarget);
            }
            if targets.iter().any(|target| !target.pending_entry) {
                return Err(RawSignalAdaptationError::TargetIsNotPending);
            }
            build_target_intents(context, timestamp, targets, |target| {
                IntentAction::CancelEntry(CancelEntryIntent {
                    position: selector_for(target),
                })
            })
        }
        RawSignal::CloseAllOf { .. }
        | RawSignal::CloseAll { .. }
        | RawSignal::CloseAllInGroup { .. } => adapt_bulk_targets(context, timestamp, |target| {
            IntentAction::FlattenScope(FlattenScopeIntent {
                position: selector_for(target),
            })
        }),
        RawSignal::CancelAllPending { .. } => {
            let targets = sorted_targets(context)
                .into_iter()
                .filter(|target| target.pending_entry)
                .collect();
            build_target_intents(context, timestamp, targets, |target| {
                IntentAction::CancelEntry(CancelEntryIntent {
                    position: selector_for(target),
                })
            })
        }
        RawSignal::ModifyAllStoploss { price, .. }
        | RawSignal::ModifyAllStoplossInGroup { price, .. } => {
            let stop = absolute_price(*price, "stoploss price")?;
            adapt_bulk_targets(context, timestamp, |target| {
                IntentAction::ReplaceProtection(ReplaceProtectionIntent {
                    position: selector_for(target),
                    protection: ProtectionRequest::StopLoss { stop: stop.clone() },
                })
            })
        }
        RawSignal::AddRule { .. } | RawSignal::RemoveRule { .. } => {
            unreachable!("compatibility-only actions returned before adaptation")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn adapt_entry(
    side: Side,
    order_type: OrderType,
    price: Option<f64>,
    risk_multiplier: f64,
    stoploss: Option<f64>,
    targets: &[f64],
    group: Option<&str>,
    trade_id: Option<&str>,
    timestamp: DateTimeUtc,
    context: &RawSignalAdaptationContext,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    let instrument = context
        .resolved_entry_instrument
        .clone()
        .ok_or(RawSignalAdaptationError::UnresolvedEntryInstrument)?;
    validate_collection_bound("entry target hints", targets.len(), MAX_TARGET_HINTS)?;

    let entry_reference = match (order_type, price) {
        (OrderType::Market, Some(price)) => {
            Some(absolute_price(price, "market entry reference price")?)
        }
        _ => None,
    };
    let order = match (order_type, price) {
        (OrderType::Market, _) => OrderPreference::Market,
        (OrderType::Limit, Some(price)) => OrderPreference::Limit {
            limit: absolute_price(price, "entry limit price")?,
        },
        (OrderType::Stop, Some(price)) => OrderPreference::Stop {
            trigger: absolute_price(price, "entry stop price")?,
        },
        (OrderType::Limit, None) => {
            return Err(RawSignalAdaptationError::InvalidCompatibilityValue(
                "limit entry requires a price".to_owned(),
            ));
        }
        (OrderType::Stop, None) => {
            return Err(RawSignalAdaptationError::InvalidCompatibilityValue(
                "stop entry requires a price".to_owned(),
            ));
        }
    };

    let mut provenance = context.base_provenance.clone();
    if provenance.correlation.is_none()
        && let Some(trade_id) = trade_id
    {
        provenance.correlation = Some(IntentCorrelationId::new(trade_id).map_err(|error| {
            RawSignalAdaptationError::InvalidCompatibilityValue(error.to_string())
        })?);
    }
    if let Some(group) = group {
        provenance.source_refs.push(encode_legacy_group(group)?);
    }
    provenance.source_refs.sort();
    provenance.validate()?;

    let action = IntentAction::Enter(EntryIntent {
        side,
        order,
        entry_reference,
        invalidation: stoploss
            .map(|price| absolute_price(price, "entry invalidation price"))
            .transpose()?,
        target_hints: targets
            .iter()
            .map(|price| {
                Ok(TargetHint {
                    price: absolute_price(*price, "entry target price")?,
                    close_fraction: None,
                })
            })
            .collect::<Result<Vec<_>, RawSignalAdaptationError>>()?,
        risk_request: RiskRequest::UnitMultiplier(positive_decimal_from_f64(
            risk_multiplier,
            "entry risk multiplier",
        )?),
    });
    let intent = TradeIntent::with_deterministic_id(
        &context.identity_namespace,
        0,
        instrument,
        timestamp,
        timestamp,
        None,
        ExpectedStateRevision::default(),
        provenance,
        action,
        context.constraints.clone(),
    )?;
    Ok(RawSignalAdaptationOutcome::Intents(vec![intent]))
}

fn adapt_direct_targets(
    context: &RawSignalAdaptationContext,
    timestamp: DateTimeUtc,
    action: impl Fn(&ResolvedRawSignalTarget) -> IntentAction,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    let targets = sorted_targets(context);
    if targets.is_empty() {
        return Err(RawSignalAdaptationError::UnresolvedManagementTarget);
    }
    build_target_intents(context, timestamp, targets, action)
}

fn adapt_bulk_targets(
    context: &RawSignalAdaptationContext,
    timestamp: DateTimeUtc,
    action: impl Fn(&ResolvedRawSignalTarget) -> IntentAction,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    build_target_intents(context, timestamp, sorted_targets(context), action)
}

fn adapt_target_delta(
    context: &RawSignalAdaptationContext,
    timestamp: DateTimeUtc,
    transform: impl Fn(
        &ManagementTargetDesiredState,
    ) -> Result<Vec<TargetHint>, RawSignalAdaptationError>,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    let targets = sorted_targets(context);
    if targets.is_empty() {
        return Err(RawSignalAdaptationError::UnresolvedManagementTarget);
    }
    let mut actions = Vec::with_capacity(targets.len());
    for target in &targets {
        let state = target
            .desired_state
            .as_ref()
            .ok_or(RawSignalAdaptationError::MissingTargetState)?;
        let replacement = transform(state)?;
        validate_target_hints(&replacement)?;
        actions.push(IntentAction::ReplaceTargets(ReplaceTargetsIntent {
            position: selector_for(target),
            targets: replacement,
        }));
    }
    build_target_intents_from_actions(context, timestamp, targets, actions)
}

fn build_target_intents(
    context: &RawSignalAdaptationContext,
    timestamp: DateTimeUtc,
    targets: Vec<&ResolvedRawSignalTarget>,
    action: impl Fn(&ResolvedRawSignalTarget) -> IntentAction,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    let actions = targets.iter().map(|target| action(target)).collect();
    build_target_intents_from_actions(context, timestamp, targets, actions)
}

fn build_target_intents_from_actions(
    context: &RawSignalAdaptationContext,
    timestamp: DateTimeUtc,
    targets: Vec<&ResolvedRawSignalTarget>,
    actions: Vec<IntentAction>,
) -> Result<RawSignalAdaptationOutcome, RawSignalAdaptationError> {
    let mut intents = Vec::with_capacity(targets.len());
    for (ordinal, (target, action)) in targets.into_iter().zip(actions).enumerate() {
        let ordinal = u32::try_from(ordinal).expect("resolved target bound fits in u32");
        intents.push(TradeIntent::with_deterministic_id(
            &context.identity_namespace,
            ordinal,
            target.instrument.clone(),
            timestamp,
            timestamp,
            None,
            ExpectedStateRevision::default(),
            context.base_provenance.clone(),
            action,
            context.constraints.clone(),
        )?);
    }
    Ok(RawSignalAdaptationOutcome::Intents(intents))
}

fn validate_adaptation_context(
    context: &RawSignalAdaptationContext,
) -> Result<(), RawSignalAdaptationError> {
    context.base_provenance.validate()?;
    context.constraints.validate()?;
    if context.management_targets.len() > MAX_RESOLVED_TARGETS {
        return Err(RawSignalAdaptationError::TooManyResolvedTargets {
            maximum: MAX_RESOLVED_TARGETS,
            actual: context.management_targets.len(),
        });
    }
    if let Some(instrument) = &context.resolved_entry_instrument {
        validate_catalog_version(instrument)?;
    }
    for target in &context.management_targets {
        validate_catalog_version(&target.instrument)?;
        if let Some(state) = &target.desired_state {
            state.validate()?;
        }
    }
    Ok(())
}

fn sorted_targets(context: &RawSignalAdaptationContext) -> Vec<&ResolvedRawSignalTarget> {
    let mut targets = context.management_targets.iter().collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.position
            .cmp(&right.position)
            .then(left.instrument.instrument.cmp(&right.instrument.instrument))
            .then(
                left.instrument
                    .catalog
                    .version
                    .cmp(&right.instrument.catalog.version),
            )
            .then(
                left.instrument
                    .spec_revision
                    .cmp(&right.instrument.spec_revision),
            )
            .then(left.campaign.cmp(&right.campaign))
    });
    targets
}

fn selector_for(target: &ResolvedRawSignalTarget) -> PositionSelector {
    PositionSelector::exact(
        target.position.clone(),
        target.campaign.clone(),
        target.instrument.clone(),
    )
}

fn unique_target_index(
    targets: &[TargetHint],
    price: Price,
) -> Result<usize, RawSignalAdaptationError> {
    let matches = targets
        .iter()
        .enumerate()
        .filter_map(|(index, target)| {
            matches!(target.price, PriceReference::Absolute(candidate) if candidate == price)
                .then_some(index)
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(RawSignalAdaptationError::MissingTarget),
        [index] => Ok(*index),
        _ => Err(RawSignalAdaptationError::AmbiguousTarget),
    }
}

#[derive(Clone, Debug, Default)]
pub struct TradeIntentRawSignalProjectionContext {
    pub symbol: Option<String>,
    pub position: Option<PositionRef>,
    pub trade_id: Option<TradeId>,
    pub group: Option<GroupId>,
    pub current_target_state: Option<ManagementTargetDesiredState>,
}

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum TradeIntentProjectionError {
    #[error(transparent)]
    InvalidIntent(#[from] IntentValidationError),
    #[error("compatibility projection requires {0}")]
    MissingContext(&'static str),
    #[error("intent action is not representable as one RawSignal: {0}")]
    Unsupported(&'static str),
    #[error("canonical value cannot be represented by RawSignal: {0}")]
    InvalidValue(String),
}

pub fn project_trade_intent_to_raw_signal(
    intent: &TradeIntent,
    context: &TradeIntentRawSignalProjectionContext,
) -> Result<RawSignal, TradeIntentProjectionError> {
    intent.validate()?;
    let ts = intent.effective_at.as_inner().naive_utc();
    match &intent.action {
        IntentAction::Enter(entry) => {
            let symbol = required_compatibility_text("entry symbol", context.symbol.as_deref())?;
            let risk_multiplier = match &entry.risk_request {
                RiskRequest::UnitMultiplier(value) => decimal_to_f64(value.get())?,
                _ => {
                    return Err(TradeIntentProjectionError::Unsupported(
                        "entry risk request is not a unit multiplier",
                    ));
                }
            };
            let (order_type, price) = project_entry_order(&entry.order, &entry.entry_reference)?;
            let stoploss = entry
                .invalidation
                .as_ref()
                .map(project_absolute_price)
                .transpose()?;
            let targets = entry
                .target_hints
                .iter()
                .map(|target| project_absolute_price(&target.price))
                .collect::<Result<Vec<_>, _>>()?;
            let trade_id = context.trade_id.clone().or_else(|| {
                intent
                    .provenance
                    .correlation
                    .as_ref()
                    .map(|correlation| correlation.as_str().to_owned())
            });
            let group = match &context.group {
                Some(group) => Some(group.clone()),
                None => decode_legacy_group(&intent.provenance.source_refs)?,
            };
            Ok(RawSignal::Entry {
                ts,
                symbol,
                side: entry.side,
                order_type,
                price,
                risk_multiplier,
                stoploss,
                targets,
                group,
                trade_id,
            })
        }
        IntentAction::Exit(_) => Ok(RawSignal::Close {
            ts,
            position: required_position(context)?,
        }),
        IntentAction::Reduce(reduce) => match &reduce.reduction {
            ReductionRequest::Fraction(fraction) => Ok(RawSignal::ClosePartial {
                ts,
                position: required_position(context)?,
                ratio: decimal_to_f64(fraction.get().get())?,
            }),
            ReductionRequest::AllRemaining => Ok(RawSignal::Close {
                ts,
                position: required_position(context)?,
            }),
            ReductionRequest::Quantity(_) => Err(TradeIntentProjectionError::Unsupported(
                "quantity reduction has no RawSignal equivalent",
            )),
        },
        IntentAction::ReplaceProtection(replacement) => match &replacement.protection {
            ProtectionRequest::StopLoss { stop } => Ok(RawSignal::ModifyStoploss {
                ts,
                position: required_position(context)?,
                price: project_absolute_price(stop)?,
            }),
            ProtectionRequest::Breakeven => Ok(RawSignal::MoveStoplossToEntry {
                ts,
                position: required_position(context)?,
            }),
            ProtectionRequest::Clear => Err(TradeIntentProjectionError::Unsupported(
                "clearing protection has no RawSignal equivalent",
            )),
        },
        IntentAction::ReplaceTargets(replacement) => {
            project_target_replacement(ts, &replacement.targets, context)
        }
        IntentAction::AddTranche(tranche) => {
            let price = match &tranche.order {
                OrderPreference::Market => tranche
                    .entry_reference
                    .as_ref()
                    .map(project_absolute_price)
                    .transpose()?,
                OrderPreference::Limit { .. }
                | OrderPreference::Stop { .. }
                | OrderPreference::StopLimit { .. } => {
                    return Err(TradeIntentProjectionError::Unsupported(
                        "tranche order preference has no RawSignal equivalent",
                    ));
                }
            };
            Ok(RawSignal::ScaleIn {
                ts,
                position: required_position(context)?,
                price,
                size: decimal_to_f64(tranche.quantity.get())?,
            })
        }
        IntentAction::CancelEntry(_) => Ok(RawSignal::CancelPending {
            ts,
            position: required_position(context)?,
        }),
        IntentAction::FlattenScope(flatten) => match flatten.position.scope {
            SelectorScope::ExactPosition => Ok(RawSignal::Close {
                ts,
                position: required_position(context)?,
            }),
            SelectorScope::Campaign | SelectorScope::Instrument => {
                Err(TradeIntentProjectionError::Unsupported(
                    "campaign or instrument flatten scope requires caller expansion",
                ))
            }
        },
    }
}

fn project_entry_order(
    order: &OrderPreference,
    entry_reference: &Option<PriceReference>,
) -> Result<(OrderType, Option<f64>), TradeIntentProjectionError> {
    match order {
        OrderPreference::Market => Ok((
            OrderType::Market,
            entry_reference
                .as_ref()
                .map(project_absolute_price)
                .transpose()?,
        )),
        OrderPreference::Limit { limit } => {
            Ok((OrderType::Limit, Some(project_absolute_price(limit)?)))
        }
        OrderPreference::Stop { trigger } => {
            Ok((OrderType::Stop, Some(project_absolute_price(trigger)?)))
        }
        OrderPreference::StopLimit { .. } => Err(TradeIntentProjectionError::Unsupported(
            "stop-limit entry has no RawSignal equivalent",
        )),
    }
}

fn project_target_replacement(
    ts: NaiveDateTime,
    replacement: &[TargetHint],
    context: &TradeIntentRawSignalProjectionContext,
) -> Result<RawSignal, TradeIntentProjectionError> {
    let current =
        context
            .current_target_state
            .as_ref()
            .ok_or(TradeIntentProjectionError::MissingContext(
                "current target desired state",
            ))?;
    current.validate()?;
    let position = required_position(context)?;
    let removed = unmatched_targets(&current.targets, replacement);
    let added = unmatched_targets(replacement, &current.targets);

    match (removed.as_slice(), added.as_slice()) {
        ([], [target]) => {
            let close_fraction =
                target
                    .close_fraction
                    .ok_or(TradeIntentProjectionError::Unsupported(
                        "added target requires a close fraction for RawSignal",
                    ))?;
            Ok(RawSignal::AddTarget {
                ts,
                position,
                price: project_absolute_price(&target.price)?,
                close_ratio: decimal_to_f64(close_fraction.get().get())?,
            })
        }
        ([target], []) => Ok(RawSignal::RemoveTarget {
            ts,
            position,
            price: project_absolute_price(&target.price)?,
        }),
        ([old], [new]) if old.close_fraction == new.close_fraction => Ok(RawSignal::ModifyTarget {
            ts,
            position,
            old_price: project_absolute_price(&old.price)?,
            new_price: project_absolute_price(&new.price)?,
        }),
        _ => Err(TradeIntentProjectionError::Unsupported(
            "target replacement is not one add, remove, or price modification",
        )),
    }
}

fn unmatched_targets<'a>(left: &'a [TargetHint], right: &[TargetHint]) -> Vec<&'a TargetHint> {
    let mut used = vec![false; right.len()];
    let mut unmatched = Vec::new();
    for target in left {
        if let Some((index, _)) = right
            .iter()
            .enumerate()
            .find(|(index, candidate)| !used[*index] && *candidate == target)
        {
            used[index] = true;
        } else {
            unmatched.push(target);
        }
    }
    unmatched
}

fn required_position(
    context: &TradeIntentRawSignalProjectionContext,
) -> Result<PositionRef, TradeIntentProjectionError> {
    context
        .position
        .clone()
        .ok_or(TradeIntentProjectionError::MissingContext(
            "legacy position reference",
        ))
}

fn required_compatibility_text(
    kind: &'static str,
    value: Option<&str>,
) -> Result<String, TradeIntentProjectionError> {
    let value = value.ok_or(TradeIntentProjectionError::MissingContext(kind))?;
    validate_bounded_text(kind, value, MAX_COMPATIBILITY_TEXT_BYTES)?;
    Ok(value.to_owned())
}

fn project_absolute_price(reference: &PriceReference) -> Result<f64, TradeIntentProjectionError> {
    match reference {
        PriceReference::Absolute(price) => decimal_to_f64(price.get()),
        PriceReference::EntryAverage | PriceReference::DerivedArtifact { .. } => {
            Err(TradeIntentProjectionError::Unsupported(
                "non-absolute price reference has no RawSignal equivalent",
            ))
        }
    }
}

fn decimal_to_f64(value: Decimal) -> Result<f64, TradeIntentProjectionError> {
    let parsed = value
        .to_string()
        .parse::<f64>()
        .map_err(|error| TradeIntentProjectionError::InvalidValue(error.to_string()))?;
    if !parsed.is_finite() {
        return Err(TradeIntentProjectionError::InvalidValue(
            "decimal exceeds finite f64 range".to_owned(),
        ));
    }
    Ok(parsed)
}

fn encode_legacy_group(group: &str) -> Result<OpaqueProvenanceRef, RawSignalAdaptationError> {
    if group.len() > MAX_COMPATIBILITY_TEXT_BYTES {
        return Err(RawSignalAdaptationError::InvalidCompatibilityValue(
            "legacy group exceeds compatibility bound".to_owned(),
        ));
    }
    let mut encoded = String::with_capacity(LEGACY_GROUP_PREFIX.len() + group.len() * 2);
    encoded.push_str(LEGACY_GROUP_PREFIX);
    for byte in group.as_bytes() {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    OpaqueProvenanceRef::new(encoded)
        .map_err(|error| RawSignalAdaptationError::InvalidCompatibilityValue(error.to_string()))
}

fn decode_legacy_group(
    source_refs: &[OpaqueProvenanceRef],
) -> Result<Option<String>, TradeIntentProjectionError> {
    let Some(reference) = source_refs
        .iter()
        .find_map(|reference| reference.as_str().strip_prefix(LEGACY_GROUP_PREFIX))
    else {
        return Ok(None);
    };
    if reference.len() % 2 != 0 || !reference.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TradeIntentProjectionError::InvalidValue(
            "legacy group provenance is not valid hexadecimal".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(reference.len() / 2);
    for pair in reference.as_bytes().chunks_exact(2) {
        let text = std::str::from_utf8(pair).expect("hexadecimal pairs are ASCII");
        bytes.push(u8::from_str_radix(text, 16).expect("hexadecimal pairs were validated"));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| TradeIntentProjectionError::InvalidValue(error.to_string()))
}

fn positive_decimal_from_f64(
    value: f64,
    field: &'static str,
) -> Result<PositiveDecimal, RawSignalAdaptationError> {
    let decimal = Decimal::checked_from_f64(value)
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })?;
    PositiveDecimal::new(decimal)
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })
}

fn price_from_f64(value: f64, field: &'static str) -> Result<Price, RawSignalAdaptationError> {
    let decimal = Decimal::checked_from_f64(value)
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })?;
    Price::new(decimal).map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })
}

fn absolute_price(
    value: f64,
    field: &'static str,
) -> Result<PriceReference, RawSignalAdaptationError> {
    Ok(PriceReference::Absolute(price_from_f64(value, field)?))
}

fn quantity_from_f64(
    value: f64,
    field: &'static str,
) -> Result<Quantity, RawSignalAdaptationError> {
    let decimal = Decimal::checked_from_f64(value)
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })?;
    let quantity = Quantity::new(decimal)
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })?;
    quantity
        .require_positive()
        .map_err(|_| RawSignalAdaptationError::InvalidPositiveNumber { field })?;
    Ok(quantity)
}

fn fraction_from_f64(
    value: f64,
    field: &'static str,
) -> Result<PositiveFraction, RawSignalAdaptationError> {
    let value = positive_decimal_from_f64(value, field)
        .map_err(|_| RawSignalAdaptationError::InvalidFraction { field })?;
    PositiveFraction::new(value).map_err(|_| RawSignalAdaptationError::InvalidFraction { field })
}

fn validate_selector(
    selector: &PositionSelector,
    envelope: &ResolvedInstrumentRef,
) -> Result<(), IntentValidationError> {
    selector.validate()?;
    if selector.instrument.as_ref() != Some(envelope) {
        return Err(IntentValidationError::SelectorInstrumentMismatch);
    }
    Ok(())
}

fn validate_target_hints(targets: &[TargetHint]) -> Result<(), IntentValidationError> {
    validate_collection_bound("target hints", targets.len(), MAX_TARGET_HINTS)?;
    let mut specified_fraction = Decimal::ZERO;
    for (index, target) in targets.iter().enumerate() {
        target.validate()?;
        if targets[..index]
            .iter()
            .any(|existing| existing.price == target.price)
        {
            return Err(IntentValidationError::DuplicateTargetPriceReference);
        }
        if let Some(fraction) = target.close_fraction {
            specified_fraction = specified_fraction
                .checked_add(fraction.get().get())
                .expect("bounded positive fractions cannot overflow Decimal");
        }
    }
    let one = Decimal::new(1, 0).expect("one is a valid decimal");
    if specified_fraction > one {
        return Err(IntentValidationError::TargetCloseFractionsExceedOne);
    }
    Ok(())
}

fn validate_catalog_version(
    instrument: &ResolvedInstrumentRef,
) -> Result<(), IntentValidationError> {
    let version = &instrument.catalog.version;
    if version.is_empty()
        || version.len() > 64
        || !version.is_ascii()
        || version.trim() != version
        || version.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(IntentValidationError::InvalidCatalogVersion);
    }
    Ok(())
}

fn validate_bounded_text(
    kind: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), IntentValidationError> {
    if value.is_empty()
        || value.len() > maximum
        || !value.is_ascii()
        || value.trim() != value
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(IntentValidationError::InvalidText { kind, maximum });
    }
    Ok(())
}

fn validate_collection_bound(
    collection: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), IntentValidationError> {
    if actual > maximum {
        return Err(IntentValidationError::CollectionTooLarge {
            collection,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn has_duplicates<T: Eq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
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

fn deserialize_producer_revision<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_bounded_text("producer revision", &value, MAX_PRODUCER_REVISION_BYTES)
        .map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_derived_field<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    validate_bounded_text("derived price field", &value, MAX_DERIVED_FIELD_BYTES)
        .map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_provenance_refs<'de, D>(
    deserializer: D,
) -> Result<Vec<OpaqueProvenanceRef>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = deserialize_bounded_vec::<D, _, MAX_PROVENANCE_REFS>(
        deserializer,
        "provenance source references",
    )?;
    if has_duplicates(&values) {
        return Err(serde::de::Error::custom(
            "provenance source references contain duplicates",
        ));
    }
    Ok(values)
}

fn deserialize_target_hints<'de, D>(deserializer: D) -> Result<Vec<TargetHint>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = deserialize_bounded_vec::<D, _, MAX_TARGET_HINTS>(deserializer, "target hints")?;
    validate_target_hints(&values).map_err(serde::de::Error::custom)?;
    Ok(values)
}

fn deserialize_supersedes<'de, D>(deserializer: D) -> Result<Vec<TradeIntentId>, D::Error>
where
    D: Deserializer<'de>,
{
    let values =
        deserialize_bounded_vec::<D, _, MAX_SUPERSEDES>(deserializer, "superseded intent IDs")?;
    if has_duplicates(&values) {
        return Err(serde::de::Error::custom(
            "superseded intent IDs contain duplicates",
        ));
    }
    Ok(values)
}

fn deserialize_allowed_modes<'de, D>(deserializer: D) -> Result<BTreeSet<OperatingMode>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_set::<D, OperatingMode, MAX_ALLOWED_MODES>(
        deserializer,
        "allowed operating modes",
    )
}

fn deserialize_required_capabilities<'de, D>(
    deserializer: D,
) -> Result<BTreeSet<ExecutionCapability>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_set::<D, ExecutionCapability, MAX_REQUIRED_CAPABILITIES>(
        deserializer,
        "required capabilities",
    )
}

fn deserialize_bounded_set<'de, D, T, const MAXIMUM: usize>(
    deserializer: D,
    collection: &'static str,
) -> Result<BTreeSet<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned + Ord,
{
    let values = deserialize_bounded_vec::<D, T, MAXIMUM>(deserializer, collection)?;
    let length = values.len();
    let values = values.into_iter().collect::<BTreeSet<_>>();
    if values.len() != length {
        return Err(serde::de::Error::custom(format!(
            "{collection} contains duplicates"
        )));
    }
    Ok(values)
}

mod intent_side_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::types::Side;

    pub fn serialize<S>(value: &Side, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match value {
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
