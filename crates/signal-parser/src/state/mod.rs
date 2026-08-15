//! Restart-safe source application, committed lifecycle, checkpoints, and outbox state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;

use qs_core::RawSignal;
use rusqlite::{Connection, OptionalExtension as _, params};
use serde::{Deserialize, Serialize};

use crate::ingestion::{
    DateTimeUtc, ExternalEventId, SourceEvent, SourceEventKey, SourceEventRef, SourceId,
    SourceOperation, SourcePayload, SourceRevision,
};
use crate::normalization::{
    BaseContextSnapshot, CanonicalIdentityBytes, CompletionKnowledge, ContractBytes,
    EvaluationClock, EvaluationFailure, EvaluationFailureClass, EvaluationIdentity,
    EvaluationRetrySafety, HistoricalSourceFact, HistoryView, IdentityError, InstrumentHint,
    NormalizationEvaluationReport, NormalizationOutcome, ParentRequirement, ParentView,
    PipelineContextRequirements, PipelineIdentity, SourceAdapterIdentity,
    normalized_signal_semantic_projection,
};

const APPLIED_EVENT_ID_PREFIX: &str = "ae2_";
const NORMALIZED_SIGNAL_ID_PREFIX: &str = "ns1_";
const COMMITTED_BATCH_ID_PREFIX: &str = "nb3_";
const PUBLICATION_DELIVERY_ID_PREFIX: &str = "pd2_";
const MAX_DURABLE_DELIVERY_ID_BYTES: usize = 1_024;
const MAX_APPLIED_EVENT_ID_BYTES: usize = 2_048;
const PUBLICATION_SINK_MAX_BYTES: usize = 128;
const SQLITE_SCHEMA_VERSION: u32 = 2;
pub const DEFAULT_ACTIVE_OUTPUT_LIMIT: usize = 32;
pub const MAX_ACTIVE_OUTPUT_LIMIT: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} identity")]
pub struct DirectIdParseError {
    kind: &'static str,
}

impl DirectIdParseError {
    fn new(kind: &'static str) -> Self {
        Self { kind }
    }
}

fn boxed_direct_id(
    value: String,
    maximum: usize,
    kind: &'static str,
) -> Result<Box<str>, DirectIdParseError> {
    validate_direct_component(&value, maximum, false).map_err(|_| DirectIdParseError::new(kind))?;
    Ok(value.into_boxed_str())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedEventId(Box<str>);

impl AppliedEventId {
    pub fn for_monotonic(key: &SourceEventKey, revision: u64) -> Result<Self, DirectIdParseError> {
        let source = key.source().as_str();
        let external = key.external_id().as_str();
        Self::from_canonical(format!(
            "{APPLIED_EVENT_ID_PREFIX}m:{}:{source}:{}:{external}:{revision}",
            source.len(),
            external.len()
        ))
    }

    pub fn for_unversioned(
        key: &SourceEventKey,
        delivery: &DurableDeliveryIdentity,
    ) -> Result<Self, DirectIdParseError> {
        let source = key.source().as_str();
        let external = key.external_id().as_str();
        let delivery = encode_delivery_identity(delivery)?;
        Self::from_canonical(format!(
            "{APPLIED_EVENT_ID_PREFIX}u:{}:{source}:{}:{external}:{delivery}",
            source.len(),
            external.len()
        ))
    }

    pub fn from_string_id(value: &str) -> Result<Self, DirectIdParseError> {
        parse_applied_identity(value)?;
        Self::from_canonical(value.to_string())
    }

    pub fn to_string_id(&self) -> String {
        self.0.to_string()
    }

    pub fn matches_source_ref(&self, source: &SourceEventRef) -> bool {
        let Ok(parsed) = parse_applied_identity(&self.0) else {
            return false;
        };
        parsed.source == source.key().source().as_str()
            && parsed.external == source.key().external_id().as_str()
            && match (parsed.revision, source.revision()) {
                (ParsedAppliedRevision::Monotonic(left), SourceRevision::Monotonic(right)) => {
                    left == *right
                }
                (ParsedAppliedRevision::Unversioned, SourceRevision::Unversioned) => true,
                _ => false,
            }
    }

    fn from_canonical(value: String) -> Result<Self, DirectIdParseError> {
        Ok(Self(boxed_direct_id(
            value,
            MAX_APPLIED_EVENT_ID_BYTES,
            "applied event",
        )?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedSignalId {
    applied_event_id: AppliedEventId,
    candidate_ordinal: u32,
}

impl NormalizedSignalId {
    pub fn from_applied_event(applied_event_id: AppliedEventId, candidate_ordinal: u32) -> Self {
        Self {
            applied_event_id,
            candidate_ordinal,
        }
    }

    pub fn from_string_id(value: &str) -> Result<Self, DirectIdParseError> {
        let encoded = value
            .strip_prefix(NORMALIZED_SIGNAL_ID_PREFIX)
            .ok_or_else(|| DirectIdParseError::new("normalized signal"))?;
        let (applied, ordinal) = take_length_framed(encoded, "normalized signal")?;
        let ordinal = parse_canonical_u32(ordinal, "normalized signal")?;
        Ok(Self::from_applied_event(
            AppliedEventId::from_string_id(applied)?,
            ordinal,
        ))
    }

    pub fn applied_event_id(&self) -> &AppliedEventId {
        &self.applied_event_id
    }

    pub fn candidate_ordinal(&self) -> u32 {
        self.candidate_ordinal
    }

    pub fn to_string_id(&self) -> String {
        let applied = self.applied_event_id.to_string_id();
        format!(
            "{NORMALIZED_SIGNAL_ID_PREFIX}{}:{applied}:{}",
            applied.len(),
            self.candidate_ordinal
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommittedBatchId {
    applied_event_id: AppliedEventId,
    commit_index: u64,
}

impl CommittedBatchId {
    pub fn from_applied_event(applied_event_id: AppliedEventId, commit_index: u64) -> Self {
        Self {
            applied_event_id,
            commit_index,
        }
    }

    pub fn from_string_id(value: &str) -> Result<Self, DirectIdParseError> {
        let encoded = value
            .strip_prefix(COMMITTED_BATCH_ID_PREFIX)
            .ok_or_else(|| DirectIdParseError::new("committed batch"))?;
        let (applied, commit_index) = take_length_framed(encoded, "committed batch")?;
        let commit_index = parse_canonical_u64(commit_index, "committed batch")?;
        Ok(Self::from_applied_event(
            AppliedEventId::from_string_id(applied)?,
            commit_index,
        ))
    }

    pub fn applied_event_id(&self) -> &AppliedEventId {
        &self.applied_event_id
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn to_string_id(&self) -> String {
        let applied = self.applied_event_id.to_string_id();
        format!(
            "{COMMITTED_BATCH_ID_PREFIX}{}:{applied}:{}",
            applied.len(),
            self.commit_index
        )
    }
}

macro_rules! serialize_string_id {
    ($type:ty) => {
        impl Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_string_id())
            }
        }

        impl<'de> Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::from_string_id(&value).map_err(serde::de::Error::custom)
            }
        }
    };
}

serialize_string_id!(AppliedEventId);
serialize_string_id!(NormalizedSignalId);
serialize_string_id!(CommittedBatchId);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationDeliveryId {
    batch_id: CommittedBatchId,
    sink: Box<str>,
}

impl PublicationDeliveryId {
    pub fn try_new(
        batch_id: CommittedBatchId,
        sink: &str,
    ) -> Result<Self, PublicationDeliveryIdParseError> {
        validate_direct_component(sink, PUBLICATION_SINK_MAX_BYTES, false)
            .map_err(|_| PublicationDeliveryIdParseError)?;
        Ok(Self {
            batch_id,
            sink: sink.into(),
        })
    }

    pub fn from_string_id(value: &str) -> Result<Self, PublicationDeliveryIdParseError> {
        let encoded = value
            .strip_prefix(PUBLICATION_DELIVERY_ID_PREFIX)
            .ok_or(PublicationDeliveryIdParseError)?;
        let (batch_id, framed_sink) = take_length_framed(encoded, "publication delivery")
            .map_err(|_| PublicationDeliveryIdParseError)?;
        let sink = take_final_length_framed(framed_sink, "publication delivery")
            .map_err(|_| PublicationDeliveryIdParseError)?;
        Self::try_new(
            CommittedBatchId::from_string_id(batch_id)
                .map_err(|_| PublicationDeliveryIdParseError)?,
            sink,
        )
    }

    pub fn batch_id(&self) -> &CommittedBatchId {
        &self.batch_id
    }

    pub fn sink(&self) -> &str {
        &self.sink
    }

    pub fn to_string_id(&self) -> String {
        let batch = self.batch_id.to_string_id();
        format!(
            "{PUBLICATION_DELIVERY_ID_PREFIX}{}:{batch}:{}:{}",
            batch.len(),
            self.sink.len(),
            self.sink()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid publication delivery identity")]
pub struct PublicationDeliveryIdParseError;

impl Serialize for PublicationDeliveryId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string_id())
    }
}

impl<'de> Deserialize<'de> for PublicationDeliveryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_string_id(&value).map_err(serde::de::Error::custom)
    }
}

struct ParsedAppliedIdentity<'a> {
    source: &'a str,
    external: &'a str,
    revision: ParsedAppliedRevision,
}

#[derive(Debug, Clone, Copy)]
enum ParsedAppliedRevision {
    Monotonic(u64),
    Unversioned,
}

fn parse_applied_identity(value: &str) -> Result<ParsedAppliedIdentity<'_>, DirectIdParseError> {
    let kind = "applied event";
    let encoded = value
        .strip_prefix(APPLIED_EVENT_ID_PREFIX)
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    let (revision_kind, framed) = encoded
        .split_once(':')
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    let (source, framed) = take_length_framed(framed, kind)?;
    let (external, revision) = take_length_framed(framed, kind)?;
    SourceId::new(source).map_err(|_| DirectIdParseError::new(kind))?;
    ExternalEventId::new(external).map_err(|_| DirectIdParseError::new(kind))?;
    let revision = match revision_kind {
        "m" => ParsedAppliedRevision::Monotonic(parse_canonical_u64(revision, kind)?),
        "u" => {
            validate_delivery_identity(revision, kind)?;
            ParsedAppliedRevision::Unversioned
        }
        _ => return Err(DirectIdParseError::new(kind)),
    };
    Ok(ParsedAppliedIdentity {
        source,
        external,
        revision,
    })
}

fn encode_delivery_identity(
    delivery: &DurableDeliveryIdentity,
) -> Result<String, DirectIdParseError> {
    Ok(match delivery {
        DurableDeliveryIdentity::Stable(value) => {
            validate_direct_component(value, MAX_DURABLE_DELIVERY_ID_BYTES, true)?;
            format!("s:{}:{value}", value.len())
        }
        DurableDeliveryIdentity::OfflinePosition { artifact, ordinal } => {
            validate_direct_component(artifact, MAX_DURABLE_DELIVERY_ID_BYTES, true)?;
            format!("o:{}:{artifact}:{ordinal}", artifact.len())
        }
        DurableDeliveryIdentity::StoreReceipt(receipt) => format!("r:{receipt}"),
    })
}

fn validate_delivery_identity(value: &str, kind: &'static str) -> Result<(), DirectIdParseError> {
    let (delivery_kind, encoded) = value
        .split_once(':')
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    match delivery_kind {
        "s" => {
            let value = take_final_length_framed(encoded, kind)?;
            validate_direct_component(value, MAX_DURABLE_DELIVERY_ID_BYTES, true)
        }
        "o" => {
            let (artifact, ordinal) = take_length_framed(encoded, kind)?;
            validate_direct_component(artifact, MAX_DURABLE_DELIVERY_ID_BYTES, true)?;
            parse_canonical_u64(ordinal, kind).map(|_| ())
        }
        "r" => parse_canonical_u64(encoded, kind).map(|_| ()),
        _ => Err(DirectIdParseError::new(kind)),
    }
}

fn take_length_framed<'a>(
    value: &'a str,
    kind: &'static str,
) -> Result<(&'a str, &'a str), DirectIdParseError> {
    let (length, framed) = value
        .split_once(':')
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    let length = parse_canonical_usize(length, kind)?;
    if framed.len() <= length || !framed.is_char_boundary(length) {
        return Err(DirectIdParseError::new(kind));
    }
    let (field, remainder) = framed.split_at(length);
    let remainder = remainder
        .strip_prefix(':')
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    Ok((field, remainder))
}

fn take_final_length_framed<'a>(
    value: &'a str,
    kind: &'static str,
) -> Result<&'a str, DirectIdParseError> {
    let (length, field) = value
        .split_once(':')
        .ok_or_else(|| DirectIdParseError::new(kind))?;
    let length = parse_canonical_usize(length, kind)?;
    if field.len() != length {
        return Err(DirectIdParseError::new(kind));
    }
    Ok(field)
}

fn parse_canonical_usize(value: &str, kind: &'static str) -> Result<usize, DirectIdParseError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| DirectIdParseError::new(kind))?;
    if parsed.to_string() != value {
        return Err(DirectIdParseError::new(kind));
    }
    Ok(parsed)
}

fn parse_canonical_u64(value: &str, kind: &'static str) -> Result<u64, DirectIdParseError> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| DirectIdParseError::new(kind))?;
    if parsed.to_string() != value {
        return Err(DirectIdParseError::new(kind));
    }
    Ok(parsed)
}

fn parse_canonical_u32(value: &str, kind: &'static str) -> Result<u32, DirectIdParseError> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| DirectIdParseError::new(kind))?;
    if parsed.to_string() != value {
        return Err(DirectIdParseError::new(kind));
    }
    Ok(parsed)
}

fn validate_direct_component(
    value: &str,
    maximum: usize,
    allow_empty: bool,
) -> Result<(), DirectIdParseError> {
    if (!allow_empty && value.is_empty())
        || value.len() > maximum
        || value.chars().any(char::is_control)
    {
        return Err(DirectIdParseError::new("direct component"));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableDeliveryIdentity {
    Stable(String),
    OfflinePosition { artifact: String, ordinal: u64 },
    StoreReceipt(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationFence {
    pub reservation_id: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightCompareBasis {
    pub source_state_version: u64,
    pub checkpoint_version: u64,
    pub partition_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedSourceAdapter {
    pub id: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub version_prerelease: String,
    pub version_build: String,
    pub config_identity: Option<CanonicalIdentityBytes>,
}

impl From<&SourceAdapterIdentity> for AdmittedSourceAdapter {
    fn from(value: &SourceAdapterIdentity) -> Self {
        Self {
            id: value.id().as_str().to_string(),
            version_major: value.version().major(),
            version_minor: value.version().minor(),
            version_patch: value.version().patch(),
            version_prerelease: value.version().prerelease().to_string(),
            version_build: value.version().build().to_string(),
            config_identity: value.config_identity().cloned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreflightRequest {
    pub event: SourceEvent,
    pub delivery_identity: Option<DurableDeliveryIdentity>,
    pub source_adapter: SourceAdapterIdentity,
    pub execution_identity: Option<AdmittedExecutionIdentity>,
    pub adapter_evidence: Option<Vec<u8>>,
    pub requested_at: DateTimeUtc,
    pub expires_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReservation {
    pub applied_event_id: AppliedEventId,
    pub delivery_identity: DurableDeliveryIdentity,
    pub fence: ReservationFence,
    pub compare_basis: PreflightCompareBasis,
    pub source_key: SourceEventKey,
    pub intake_index: u64,
    pub expires_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreflightResult {
    Reserved(PreflightReservation),
    ExistingCommitted(CommittedBatchId),
    Conflict { existing: Box<SourceEvent> },
    Stale { latest_revision: u64 },
}

#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    pub applied_event_id: AppliedEventId,
    pub fence: ReservationFence,
    pub selected_pipeline: PipelineIdentity,
    pub requirements: PipelineContextRequirements,
    pub requested_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationCompareToken {
    pub applied_event_id: AppliedEventId,
    pub fence: ReservationFence,
    pub source_state_version: u64,
    pub checkpoint_version: u64,
    pub partition_version: u64,
    pub snapshot_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PipelineStateSnapshot {
    pub snapshot_id: u64,
    pub selected_pipeline: PipelineIdentity,
    pub base_context: BaseContextSnapshot,
    pub compare_token: ApplicationCompareToken,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplacementPolicy {
    Patch,
    ReplaceCurrentSourceKey,
}

#[derive(Debug)]
pub enum ApplicationCommitInput<'a> {
    CompletedEvaluation(&'a NormalizationEvaluationReport),
    LifecycleOnlyDelete,
}

#[derive(Debug)]
pub struct CompareAndCommitRequest<'a> {
    pub compare_token: ApplicationCompareToken,
    pub input: ApplicationCommitInput<'a>,
    pub replacement_policy: ReplacementPolicy,
    pub maximum_active_outputs: usize,
    pub publication_sink: Option<String>,
    pub committed_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareAndCommitResult {
    Committed(CommittedBatchId),
    AlreadyCommitted(CommittedBatchId),
    RetryRequired,
    FenceLost,
    ApplicationRejected(CommittedBatchId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceLifecycleState {
    Active,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceState {
    pub key: SourceEventKey,
    pub state_version: u64,
    pub latest_applied_event: Option<AppliedEventId>,
    pub latest_revision: Option<SourceRevision>,
    pub latest_event: Option<SourceEvent>,
    pub lifecycle: SourceLifecycleState,
    pub active_outputs: Vec<NormalizedSignalId>,
    pub updated_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedEvaluationIdentity {
    pub routing_graph: CanonicalIdentityBytes,
    pub selected_pipeline: Option<CanonicalIdentityBytes>,
}

impl From<&EvaluationIdentity> for CommittedEvaluationIdentity {
    fn from(value: &EvaluationIdentity) -> Self {
        Self {
            routing_graph: value.routing_graph().canonical_bytes().clone(),
            selected_pipeline: value
                .selected_pipeline()
                .map(PipelineIdentity::canonical_bytes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedInstrumentHint {
    pub symbol: String,
    pub venue: Option<String>,
    pub market_kind: Option<String>,
}

impl From<&InstrumentHint> for CommittedInstrumentHint {
    fn from(value: &InstrumentHint) -> Self {
        Self {
            symbol: value.symbol().as_str().to_string(),
            venue: value.venue_hint().map(str::to_string),
            market_kind: value.market_kind_hint().map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizationCommitRef {
    pub batch_id: CommittedBatchId,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedNormalizedSignalEnvelope {
    pub commit: NormalizationCommitRef,
    pub normalized_id: NormalizedSignalId,
    pub applied_event_id: AppliedEventId,
    pub signal: RawSignal,
    pub source: SourceEventRef,
    pub evaluation_identity: CommittedEvaluationIdentity,
    pub instrument_hint: Option<CommittedInstrumentHint>,
    pub candidate_ordinal: u32,
    pub correlation_hints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NormalizedLifecycleEvent {
    Added {
        output: NormalizedSignalId,
    },
    Superseded {
        prior: NormalizedSignalId,
        replacement: NormalizedSignalId,
    },
    Equivalent {
        evaluated: NormalizedSignalId,
        active: NormalizedSignalId,
    },
    Withdrawn {
        output: NormalizedSignalId,
        cause: AppliedEventId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedNormalizationOutcome {
    Accepted { outputs: Vec<NormalizedSignalId> },
    Ignored { reason: String },
    Ambiguous { alternatives: u32 },
    Rejected { reason: String },
    ApplicationRejected { reason: String },
    LifecycleOnlyDelete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedNormalizationBatch {
    pub batch_id: CommittedBatchId,
    pub applied_event_id: AppliedEventId,
    pub source: SourceEventRef,
    pub evaluation_identity: Option<CommittedEvaluationIdentity>,
    pub outcome: CommittedNormalizationOutcome,
    pub envelopes: Vec<CommittedNormalizedSignalEnvelope>,
    pub lifecycle: Vec<NormalizedLifecycleEvent>,
    pub commit_index: u64,
    pub committed_at: DateTimeUtc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    pub source: String,
    pub applied_event_id: AppliedEventId,
    pub batch_id: CommittedBatchId,
    pub commit_index: u64,
    pub checkpoint_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedComponentIdentity {
    pub id: String,
    pub kind: u16,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub version_prerelease: String,
    pub version_build: String,
    pub contract_version: u32,
    pub config_identity: CanonicalIdentityBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedExecutionIdentity {
    pub routing_graph: CanonicalIdentityBytes,
    pub pipeline: CanonicalIdentityBytes,
    pub decoder: AdmittedComponentIdentity,
    pub finalizer: AdmittedComponentIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedReceipt {
    pub intake_index: u64,
    pub available_at: DateTimeUtc,
    pub applied_event_id: AppliedEventId,
    pub delivery_identity: DurableDeliveryIdentity,
    pub source_adapter: AdmittedSourceAdapter,
    pub execution_identity: Option<AdmittedExecutionIdentity>,
    pub adapter_evidence: Option<Vec<u8>>,
    pub event: SourceEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvaluationAttemptRecord {
    pub applied_event_id: AppliedEventId,
    pub failure_class: String,
    pub retry_safety: String,
    pub completion_knowledge: String,
    pub diagnostic_codes: Vec<String>,
    pub observed_at: DateTimeUtc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationLeaseFence {
    pub delivery_id: PublicationDeliveryId,
    pub generation: u64,
    pub lease_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicationState {
    Pending,
    Leased {
        fence: PublicationLeaseFence,
        expires_at: DateTimeUtc,
    },
    RetryPending,
    RetryScheduled {
        available_at: DateTimeUtc,
    },
    Published {
        acknowledged_at: DateTimeUtc,
    },
    DeadLetter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationOutboxRecord {
    pub delivery_id: PublicationDeliveryId,
    pub batch_id: CommittedBatchId,
    pub sink: String,
    pub state: PublicationState,
    pub attempts: u32,
    #[serde(default)]
    pub first_attempt_at: Option<DateTimeUtc>,
}

#[derive(Debug, Clone)]
pub struct PublicationLease {
    pub record: NormalizationOutboxRecord,
    pub fence: PublicationLeaseFence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationNackDisposition {
    Retry { available_at: DateTimeUtc },
    DeadLetter,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceStateError {
    #[error("unsupported source revision policy")]
    UnsupportedRevision,
    #[error("unversioned delivery requires a durable delivery identity")]
    MissingDeliveryIdentity,
    #[error("adapter evidence exceeds the 65536-byte contract limit")]
    AdapterEvidenceTooLarge,
    #[error("reservation fence was lost")]
    FenceLost,
    #[error("application compare token is stale")]
    CompareConflict,
    #[error("selected pipeline snapshot is invalid: {0}")]
    Snapshot(String),
    #[error("completed evaluation violates the normalization contract")]
    InvalidEvaluation,
    #[error("active output limit must be between 1 and {MAX_ACTIVE_OUTPUT_LIMIT}")]
    InvalidActiveOutputLimit,
    #[error(
        "publication sink identity must contain 1 through {PUBLICATION_SINK_MAX_BYTES} non-control UTF-8 bytes"
    )]
    InvalidPublicationSink,
    #[error("publication lease is stale or foreign")]
    InvalidPublicationLease,
    #[error("direct identity construction failed: {0}")]
    DirectIdentity(#[from] DirectIdParseError),
    #[error("canonical semantic projection failed: {0}")]
    Identity(#[from] IdentityError),
    #[error("state serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("SQLite state backend failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("state lock was poisoned")]
    Poisoned,
    #[error("unsupported SQLite schema version {0}")]
    UnsupportedSchemaVersion(u32),
}

pub trait SourceStateStore: Send + Sync {
    fn preflight(&self, request: PreflightRequest) -> Result<PreflightResult, SourceStateError>;

    fn snapshot(&self, request: SnapshotRequest)
    -> Result<PipelineStateSnapshot, SourceStateError>;

    fn route_only_compare_token(
        &self,
        reservation: &PreflightReservation,
    ) -> Result<ApplicationCompareToken, SourceStateError>;

    fn record_evaluation_failure(
        &self,
        applied_event_id: AppliedEventId,
        fence: ReservationFence,
        failure: &EvaluationFailure,
        observed_at: DateTimeUtc,
    ) -> Result<(), SourceStateError>;

    fn compare_and_commit(
        &self,
        request: CompareAndCommitRequest<'_>,
    ) -> Result<CompareAndCommitResult, SourceStateError>;

    fn source_state(&self, key: &SourceEventKey) -> Result<Option<SourceState>, SourceStateError>;

    fn checkpoint(&self, source: &str) -> Result<Option<SourceCheckpoint>, SourceStateError>;

    fn committed_batch(
        &self,
        id: CommittedBatchId,
    ) -> Result<Option<CommittedNormalizationBatch>, SourceStateError>;

    fn recorded_receipts(&self) -> Result<Vec<RecordedReceipt>, SourceStateError>;

    fn evaluation_attempts(&self) -> Result<Vec<EvaluationAttemptRecord>, SourceStateError>;

    fn lease_publications(
        &self,
        maximum_records: usize,
        now: DateTimeUtc,
        expires_at: DateTimeUtc,
    ) -> Result<Vec<PublicationLease>, SourceStateError>;

    fn acknowledge_publication(
        &self,
        fence: PublicationLeaseFence,
        acknowledged_at: DateTimeUtc,
    ) -> Result<PublicationState, SourceStateError>;

    fn reject_publication(
        &self,
        fence: PublicationLeaseFence,
        disposition: PublicationNackDisposition,
    ) -> Result<PublicationState, SourceStateError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveOutputRecord {
    normalized_id: NormalizedSignalId,
    semantic_projection: Vec<u8>,
    correlation_hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReservationRecord {
    reservation: PreflightReservation,
    event: SourceEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    schema_version: u32,
    next_receipt: u64,
    next_reservation: u64,
    next_snapshot: u64,
    next_commit: u64,
    next_lease: u64,
    receipts: Vec<RecordedReceipt>,
    reservations: BTreeMap<String, ReservationRecord>,
    committed_by_applied: BTreeMap<String, CommittedBatchId>,
    batches: BTreeMap<String, CommittedNormalizationBatch>,
    source_states: BTreeMap<String, SourceState>,
    active_outputs: BTreeMap<String, Vec<ActiveOutputRecord>>,
    checkpoints: BTreeMap<String, SourceCheckpoint>,
    attempts: Vec<EvaluationAttemptRecord>,
    outbox: BTreeMap<String, NormalizationOutboxRecord>,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            schema_version: SQLITE_SCHEMA_VERSION,
            next_receipt: 1,
            next_reservation: 1,
            next_snapshot: 1,
            next_commit: 1,
            next_lease: 1,
            receipts: Vec::new(),
            reservations: BTreeMap::new(),
            committed_by_applied: BTreeMap::new(),
            batches: BTreeMap::new(),
            source_states: BTreeMap::new(),
            active_outputs: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            attempts: Vec::new(),
            outbox: BTreeMap::new(),
        }
    }
}

impl PersistedState {
    fn preflight(
        &mut self,
        request: PreflightRequest,
    ) -> Result<PreflightResult, SourceStateError> {
        if request
            .adapter_evidence
            .as_ref()
            .is_some_and(|evidence| evidence.len() > 65_536)
        {
            return Err(SourceStateError::AdapterEvidenceTooLarge);
        }
        let source_adapter = AdmittedSourceAdapter::from(&request.source_adapter);
        let execution_identity = request.execution_identity.clone();
        let adapter_evidence = request.adapter_evidence.clone();
        let source_key = source_key(request.event.key());
        let delivery_identity = match (&request.event.revision(), request.delivery_identity) {
            (SourceRevision::Unversioned, Some(delivery)) => delivery,
            (SourceRevision::Unversioned, None) => {
                let receipt = self.next_receipt;
                self.next_receipt = self.next_receipt.saturating_add(1);
                DurableDeliveryIdentity::StoreReceipt(receipt)
            }
            (SourceRevision::Monotonic(_), Some(delivery)) => delivery,
            (SourceRevision::Monotonic(_), None) => {
                let receipt = self.next_receipt;
                self.next_receipt = self.next_receipt.saturating_add(1);
                DurableDeliveryIdentity::StoreReceipt(receipt)
            }
            (SourceRevision::Opaque(_), _) => return Err(SourceStateError::UnsupportedRevision),
        };
        let applied_event_id = applied_event_id(&request.event, &delivery_identity)?;
        let applied_key = applied_event_id.to_string_id();

        if let Some(existing) = self.reservations.get_mut(&source_key)
            && existing.reservation.applied_event_id == applied_event_id
        {
            if existing.event != request.event {
                return Ok(PreflightResult::Conflict {
                    existing: Box::new(existing.event.clone()),
                });
            }
            if existing.reservation.expires_at <= request.requested_at {
                existing.reservation.fence.generation =
                    existing.reservation.fence.generation.saturating_add(1);
                existing.reservation.expires_at = request.expires_at;
            }
            return Ok(PreflightResult::Reserved(existing.reservation.clone()));
        }

        if self
            .reservations
            .get(&source_key)
            .is_some_and(|existing| existing.reservation.expires_at <= request.requested_at)
        {
            self.reservations.remove(&source_key);
        }

        if let Some(batch_id) = self.committed_by_applied.get(&applied_key).cloned()
            && self.receipts.iter().any(|receipt| {
                receipt.applied_event_id == applied_event_id && receipt.event == request.event
            })
        {
            return Ok(PreflightResult::ExistingCommitted(batch_id));
        }

        if let SourceRevision::Monotonic(incoming) = request.event.revision()
            && let Some(current) = self.source_states.get(&source_key)
            && let Some(SourceRevision::Monotonic(latest)) = current.latest_revision.as_ref()
        {
            if incoming < latest {
                return Ok(PreflightResult::Stale {
                    latest_revision: *latest,
                });
            }
            if incoming == latest {
                if current.latest_event.as_ref() == Some(&request.event)
                    && let Some(batch_id) = current
                        .latest_applied_event
                        .as_ref()
                        .and_then(|id| self.committed_by_applied.get(&id.to_string_id()).cloned())
                {
                    return Ok(PreflightResult::ExistingCommitted(batch_id));
                }
                if let Some(existing) = current.latest_event.as_ref() {
                    return Ok(PreflightResult::Conflict {
                        existing: Box::new(existing.clone()),
                    });
                }
            }
        }

        if self.committed_by_applied.contains_key(&applied_key)
            && let Some(existing) = self
                .receipts
                .iter()
                .find(|receipt| receipt.applied_event_id == applied_event_id)
        {
            return Ok(PreflightResult::Conflict {
                existing: Box::new(existing.event.clone()),
            });
        }

        if let Some(existing) = self.reservations.get(&source_key)
            && existing.event != request.event
        {
            return Ok(PreflightResult::Conflict {
                existing: Box::new(existing.event.clone()),
            });
        }

        let intake_index = self.receipts.len() as u64 + 1;
        let source_version = self
            .source_states
            .get(&source_key)
            .map_or(0, |state| state.state_version);
        let source = request.event.key().source().as_str();
        let checkpoint_version = self
            .checkpoints
            .get(source)
            .map_or(0, |checkpoint| checkpoint.checkpoint_version);
        let reservation = PreflightReservation {
            applied_event_id: applied_event_id.clone(),
            delivery_identity: delivery_identity.clone(),
            fence: ReservationFence {
                reservation_id: self.next_reservation,
                generation: 1,
            },
            compare_basis: PreflightCompareBasis {
                source_state_version: source_version,
                checkpoint_version,
                partition_version: source_version,
            },
            source_key: request.event.key().clone(),
            intake_index,
            expires_at: request.expires_at,
        };
        self.next_reservation = self.next_reservation.saturating_add(1);
        self.receipts.push(RecordedReceipt {
            intake_index,
            available_at: request.requested_at,
            applied_event_id,
            delivery_identity,
            source_adapter,
            execution_identity,
            adapter_evidence,
            event: request.event.clone(),
        });
        self.reservations.insert(
            source_key,
            ReservationRecord {
                reservation: reservation.clone(),
                event: request.event,
            },
        );
        Ok(PreflightResult::Reserved(reservation))
    }

    fn reservation(
        &self,
        applied_event_id: &AppliedEventId,
        fence: ReservationFence,
    ) -> Result<&ReservationRecord, SourceStateError> {
        self.reservations
            .values()
            .find(|record| {
                &record.reservation.applied_event_id == applied_event_id
                    && record.reservation.fence == fence
            })
            .ok_or(SourceStateError::FenceLost)
    }

    fn compare_token(
        &self,
        reservation: &PreflightReservation,
        snapshot_id: Option<u64>,
    ) -> Result<ApplicationCompareToken, SourceStateError> {
        self.reservation(&reservation.applied_event_id, reservation.fence)?;
        Ok(ApplicationCompareToken {
            applied_event_id: reservation.applied_event_id.clone(),
            fence: reservation.fence,
            source_state_version: reservation.compare_basis.source_state_version,
            checkpoint_version: reservation.compare_basis.checkpoint_version,
            partition_version: reservation.compare_basis.partition_version,
            snapshot_id,
        })
    }

    fn validate_compare(
        &self,
        token: &ApplicationCompareToken,
    ) -> Result<&ReservationRecord, SourceStateError> {
        let reservation = self.reservation(&token.applied_event_id, token.fence)?;
        let key = source_key(&reservation.event.key().clone());
        let source_version = self
            .source_states
            .get(&key)
            .map_or(0, |state| state.state_version);
        let checkpoint_version = self
            .checkpoints
            .get(reservation.event.key().source().as_str())
            .map_or(0, |checkpoint| checkpoint.checkpoint_version);
        if source_version != token.source_state_version
            || checkpoint_version != token.checkpoint_version
            || source_version != token.partition_version
        {
            return Err(SourceStateError::CompareConflict);
        }
        Ok(reservation)
    }

    fn commit(
        &mut self,
        request: CompareAndCommitRequest<'_>,
    ) -> Result<CompareAndCommitResult, SourceStateError> {
        let applied_key = request.compare_token.applied_event_id.to_string_id();
        if let Some(existing) = self.committed_by_applied.get(&applied_key) {
            return Ok(CompareAndCommitResult::AlreadyCommitted(existing.clone()));
        }
        let reservation = match self.validate_compare(&request.compare_token) {
            Ok(value) => value.clone(),
            Err(SourceStateError::FenceLost) => return Ok(CompareAndCommitResult::FenceLost),
            Err(SourceStateError::CompareConflict) => {
                return Ok(CompareAndCommitResult::RetryRequired);
            }
            Err(error) => return Err(error),
        };
        if request.maximum_active_outputs == 0
            || request.maximum_active_outputs > MAX_ACTIVE_OUTPUT_LIMIT
        {
            return Err(SourceStateError::InvalidActiveOutputLimit);
        }
        if let Some(sink) = request.publication_sink.as_deref() {
            validate_direct_component(sink, PUBLICATION_SINK_MAX_BYTES, false)
                .map_err(|_| SourceStateError::InvalidPublicationSink)?;
        }

        let source_key_value = source_key(reservation.event.key());
        let prior_active = self
            .active_outputs
            .get(&source_key_value)
            .cloned()
            .unwrap_or_default();
        let mut next_active = prior_active.clone();
        let mut envelopes = Vec::new();
        let mut lifecycle = Vec::new();
        let mut evaluation_identity = None;
        let mut application_rejected = false;

        let outcome = match request.input {
            ApplicationCommitInput::CompletedEvaluation(report) => {
                validate_report(report, &reservation.event)?;
                evaluation_identity = Some(CommittedEvaluationIdentity::from(report.identity()));
                match report.outcome() {
                    NormalizationOutcome::Accepted { candidates } => {
                        let mut evaluated_ids = Vec::with_capacity(candidates.as_slice().len());
                        let mut matched_prior = BTreeSet::new();
                        for candidate in candidates.as_slice() {
                            let normalized_id = NormalizedSignalId::from_applied_event(
                                request.compare_token.applied_event_id.clone(),
                                candidate.candidate_ordinal(),
                            );
                            let semantic_projection = normalized_signal_semantic_projection(
                                candidate.signal(),
                                candidate.instrument_hint(),
                            )?
                            .into_bytes();
                            let correlation_hints = candidate
                                .correlation_hints()
                                .iter()
                                .map(|hint| hint.key().as_str().to_string())
                                .collect::<Vec<_>>();
                            let matches = prior_active
                                .iter()
                                .enumerate()
                                .filter(|(_, prior)| {
                                    (!correlation_hints.is_empty()
                                        && prior.correlation_hints.iter().any(|prior_hint| {
                                            correlation_hints.contains(prior_hint)
                                        }))
                                        || prior.semantic_projection == semantic_projection
                                })
                                .map(|(index, _)| index)
                                .collect::<Vec<_>>();
                            if matches.len() > 1 {
                                return Err(SourceStateError::InvalidEvaluation);
                            }
                            if let Some(index) = matches.first().copied() {
                                matched_prior.insert(index);
                                let prior = &prior_active[index];
                                if prior.semantic_projection == semantic_projection {
                                    lifecycle.push(NormalizedLifecycleEvent::Equivalent {
                                        evaluated: normalized_id.clone(),
                                        active: prior.normalized_id.clone(),
                                    });
                                } else {
                                    lifecycle.push(NormalizedLifecycleEvent::Superseded {
                                        prior: prior.normalized_id.clone(),
                                        replacement: normalized_id.clone(),
                                    });
                                    next_active
                                        .retain(|value| value.normalized_id != prior.normalized_id);
                                    next_active.push(ActiveOutputRecord {
                                        normalized_id: normalized_id.clone(),
                                        semantic_projection: semantic_projection.clone(),
                                        correlation_hints: correlation_hints.clone(),
                                    });
                                }
                            } else {
                                lifecycle.push(NormalizedLifecycleEvent::Added {
                                    output: normalized_id.clone(),
                                });
                                next_active.push(ActiveOutputRecord {
                                    normalized_id: normalized_id.clone(),
                                    semantic_projection: semantic_projection.clone(),
                                    correlation_hints: correlation_hints.clone(),
                                });
                            }
                            evaluated_ids.push(normalized_id.clone());
                            envelopes.push(PendingEnvelope {
                                normalized_id,
                                signal: candidate.signal().clone(),
                                source: candidate.provenance().source().clone(),
                                instrument_hint: candidate
                                    .instrument_hint()
                                    .map(CommittedInstrumentHint::from),
                                candidate_ordinal: candidate.candidate_ordinal(),
                                correlation_hints,
                            });
                        }
                        if request.replacement_policy == ReplacementPolicy::ReplaceCurrentSourceKey
                        {
                            for (index, prior) in prior_active.iter().enumerate() {
                                if !matched_prior.contains(&index)
                                    && next_active
                                        .iter()
                                        .any(|value| value.normalized_id == prior.normalized_id)
                                {
                                    lifecycle.push(NormalizedLifecycleEvent::Withdrawn {
                                        output: prior.normalized_id.clone(),
                                        cause: request.compare_token.applied_event_id.clone(),
                                    });
                                    next_active
                                        .retain(|value| value.normalized_id != prior.normalized_id);
                                }
                            }
                        }
                        if next_active.len() > request.maximum_active_outputs {
                            application_rejected = true;
                            next_active = prior_active.clone();
                            envelopes.clear();
                            lifecycle.clear();
                            CommittedNormalizationOutcome::ApplicationRejected {
                                reason: "active_output_limit".to_string(),
                            }
                        } else {
                            CommittedNormalizationOutcome::Accepted {
                                outputs: evaluated_ids,
                            }
                        }
                    }
                    NormalizationOutcome::Ignored { reason } => {
                        CommittedNormalizationOutcome::Ignored {
                            reason: reason.as_str().to_string(),
                        }
                    }
                    NormalizationOutcome::Ambiguous { evidence } => {
                        CommittedNormalizationOutcome::Ambiguous {
                            alternatives: evidence.alternatives().len() as u32,
                        }
                    }
                    NormalizationOutcome::Rejected { reason } => {
                        CommittedNormalizationOutcome::Rejected {
                            reason: reason.as_str().to_string(),
                        }
                    }
                }
            }
            ApplicationCommitInput::LifecycleOnlyDelete => {
                if reservation.event.operation() != SourceOperation::Delete {
                    return Err(SourceStateError::InvalidEvaluation);
                }
                for prior in &prior_active {
                    lifecycle.push(NormalizedLifecycleEvent::Withdrawn {
                        output: prior.normalized_id.clone(),
                        cause: request.compare_token.applied_event_id.clone(),
                    });
                }
                next_active.clear();
                CommittedNormalizationOutcome::LifecycleOnlyDelete
            }
        };

        let commit_index = self.next_commit;
        let batch_id = CommittedBatchId::from_applied_event(
            request.compare_token.applied_event_id.clone(),
            commit_index,
        );
        let commit = NormalizationCommitRef {
            batch_id: batch_id.clone(),
            commit_index,
        };
        let identity = evaluation_identity.clone();
        let committed_envelopes = envelopes
            .into_iter()
            .map(|pending| CommittedNormalizedSignalEnvelope {
                commit: commit.clone(),
                normalized_id: pending.normalized_id,
                applied_event_id: request.compare_token.applied_event_id.clone(),
                signal: pending.signal,
                source: pending.source,
                evaluation_identity: identity
                    .clone()
                    .expect("completed evaluation envelope has identity"),
                instrument_hint: pending.instrument_hint,
                candidate_ordinal: pending.candidate_ordinal,
                correlation_hints: pending.correlation_hints,
            })
            .collect::<Vec<_>>();
        let batch = CommittedNormalizationBatch {
            batch_id: batch_id.clone(),
            applied_event_id: request.compare_token.applied_event_id.clone(),
            source: SourceEventRef::from(&reservation.event),
            evaluation_identity,
            outcome,
            envelopes: committed_envelopes,
            lifecycle,
            commit_index,
            committed_at: request.committed_at,
        };
        if derive_committed_batch_id(&batch)? != batch_id {
            return Err(SourceStateError::InvalidEvaluation);
        }
        self.next_commit = self.next_commit.saturating_add(1);
        self.batches.insert(batch_id.to_string_id(), batch);
        self.committed_by_applied
            .insert(applied_key, batch_id.clone());
        self.active_outputs
            .insert(source_key_value.clone(), next_active.clone());
        let previous_version = self
            .source_states
            .get(&source_key_value)
            .map_or(0, |state| state.state_version);
        self.source_states.insert(
            source_key_value.clone(),
            SourceState {
                key: reservation.event.key().clone(),
                state_version: previous_version.saturating_add(1),
                latest_applied_event: Some(request.compare_token.applied_event_id.clone()),
                latest_revision: Some(reservation.event.revision().clone()),
                latest_event: Some(reservation.event.clone()),
                lifecycle: if matches!(request.input, ApplicationCommitInput::LifecycleOnlyDelete) {
                    SourceLifecycleState::Deleted
                } else {
                    SourceLifecycleState::Active
                },
                active_outputs: next_active
                    .iter()
                    .map(|value| value.normalized_id.clone())
                    .collect(),
                updated_at: request.committed_at,
            },
        );
        let source = reservation.event.key().source().as_str().to_string();
        let checkpoint_version = self.checkpoints.get(&source).map_or(1, |checkpoint| {
            checkpoint.checkpoint_version.saturating_add(1)
        });
        self.checkpoints.insert(
            source.clone(),
            SourceCheckpoint {
                source,
                applied_event_id: request.compare_token.applied_event_id.clone(),
                batch_id: batch_id.clone(),
                commit_index,
                checkpoint_version,
            },
        );
        self.reservations.remove(&source_key_value);
        if let Some(sink) = request.publication_sink {
            let delivery_id = publication_delivery_id(batch_id.clone(), &sink)?;
            self.outbox.insert(
                delivery_id.to_string_id(),
                NormalizationOutboxRecord {
                    delivery_id,
                    batch_id: batch_id.clone(),
                    sink,
                    state: PublicationState::Pending,
                    attempts: 0,
                    first_attempt_at: None,
                },
            );
        }
        if application_rejected {
            Ok(CompareAndCommitResult::ApplicationRejected(batch_id))
        } else {
            Ok(CompareAndCommitResult::Committed(batch_id))
        }
    }
}

#[derive(Debug)]
struct PendingEnvelope {
    normalized_id: NormalizedSignalId,
    signal: RawSignal,
    source: SourceEventRef,
    instrument_hint: Option<CommittedInstrumentHint>,
    candidate_ordinal: u32,
    correlation_hints: Vec<String>,
}

#[derive(Debug, Default)]
pub struct MemorySourceStateStore {
    state: Mutex<PersistedState>,
}

impl MemorySourceStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn read<T>(
        &self,
        operation: impl FnOnce(&PersistedState) -> Result<T, SourceStateError>,
    ) -> Result<T, SourceStateError> {
        let state = self.state.lock().map_err(|_| SourceStateError::Poisoned)?;
        operation(&state)
    }

    fn write<T>(
        &self,
        operation: impl FnOnce(&mut PersistedState) -> Result<T, SourceStateError>,
    ) -> Result<T, SourceStateError> {
        let mut state = self.state.lock().map_err(|_| SourceStateError::Poisoned)?;
        operation(&mut state)
    }
}

pub struct SqliteSourceStateStore {
    connection: Mutex<Connection>,
}

impl std::fmt::Debug for SqliteSourceStateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SqliteSourceStateStore").finish()
    }
}

impl SqliteSourceStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SourceStateError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS ingestion_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                payload TEXT NOT NULL
            );",
        )?;
        let existing = connection
            .query_row(
                "SELECT schema_version FROM ingestion_state WHERE singleton = 1",
                [],
                |row| row.get::<_, u32>(0),
            )
            .optional()?;
        match existing {
            Some(version) if version != SQLITE_SCHEMA_VERSION => {
                return Err(SourceStateError::UnsupportedSchemaVersion(version));
            }
            Some(_) => {}
            None => {
                let payload = serde_json::to_string(&PersistedState::default())?;
                connection.execute(
                    "INSERT INTO ingestion_state (singleton, schema_version, payload) VALUES (1, ?1, ?2)",
                    params![SQLITE_SCHEMA_VERSION, payload],
                )?;
            }
        }
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn quick_check(&self) -> Result<(), SourceStateError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| SourceStateError::Poisoned)?;
        let result: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if result == "ok" {
            Ok(())
        } else {
            Err(SourceStateError::Snapshot(result))
        }
    }

    fn load(connection: &Connection) -> Result<PersistedState, SourceStateError> {
        let payload: String = connection.query_row(
            "SELECT payload FROM ingestion_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let state: PersistedState = serde_json::from_str(&payload)?;
        if state.schema_version != SQLITE_SCHEMA_VERSION {
            return Err(SourceStateError::UnsupportedSchemaVersion(
                state.schema_version,
            ));
        }
        Ok(state)
    }

    fn read<T>(
        &self,
        operation: impl FnOnce(&PersistedState) -> Result<T, SourceStateError>,
    ) -> Result<T, SourceStateError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| SourceStateError::Poisoned)?;
        operation(&Self::load(&connection)?)
    }

    fn write<T>(
        &self,
        operation: impl FnOnce(&mut PersistedState) -> Result<T, SourceStateError>,
    ) -> Result<T, SourceStateError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SourceStateError::Poisoned)?;
        let transaction = connection.transaction()?;
        let mut state = Self::load(&transaction)?;
        let result = operation(&mut state)?;
        let payload = serde_json::to_string(&state)?;
        transaction.execute(
            "UPDATE ingestion_state SET schema_version = ?1, payload = ?2 WHERE singleton = 1",
            params![SQLITE_SCHEMA_VERSION, payload],
        )?;
        transaction.commit()?;
        Ok(result)
    }
}

macro_rules! impl_store {
    ($store:ty) => {
        impl SourceStateStore for $store {
            fn preflight(
                &self,
                request: PreflightRequest,
            ) -> Result<PreflightResult, SourceStateError> {
                self.write(|state| state.preflight(request))
            }

            fn snapshot(
                &self,
                request: SnapshotRequest,
            ) -> Result<PipelineStateSnapshot, SourceStateError> {
                self.write(|state| {
                    let reservation = state
                        .reservation(&request.applied_event_id, request.fence)?
                        .clone();
                    let prior = state
                        .receipts
                        .iter()
                        .filter(|receipt| {
                            receipt.intake_index < reservation.reservation.intake_index
                                && receipt.event.key().source()
                                    == reservation.event.key().source()
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let history = if let Some(required) = request.requirements.history() {
                        let maximum = required.maximum_items().get() as usize;
                        let selected = prior
                            .iter()
                            .rev()
                            .take(maximum)
                            .cloned()
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>();
                        let mut encoded_bytes = 0_u64;
                        let mut facts = Vec::with_capacity(selected.len());
                        for receipt in selected {
                            let event = project_history_event(
                                &receipt.event,
                                required.include_payload(),
                            );
                            let adapter_evidence = if required.include_adapter_evidence() {
                                receipt
                                    .adapter_evidence
                                    .map(|evidence| {
                                        ContractBytes::try_new(evidence, "adapter evidence")
                                    })
                                    .transpose()
                                    .map_err(|error| {
                                        SourceStateError::Snapshot(error.to_string())
                                    })?
                            } else {
                                None
                            };
                            encoded_bytes = encoded_bytes
                                .saturating_add(serde_json::to_vec(&event)?.len() as u64)
                                .saturating_add(
                                    adapter_evidence
                                        .as_ref()
                                        .map_or(0, |evidence| evidence.as_slice().len() as u64),
                                );
                            facts.push(HistoricalSourceFact::new(event, adapter_evidence));
                        }
                        if encoded_bytes > required.maximum_bytes().get() {
                            return Err(SourceStateError::Snapshot(
                                "history exceeds declared byte limit".to_string(),
                            ));
                        }
                        Some(HistoryView::try_new(
                            facts,
                            encoded_bytes,
                            prior.len() > maximum,
                        ).map_err(|error| SourceStateError::Snapshot(error.to_string()))?)
                    } else {
                        None
                    };
                    let parent = match request.requirements.parent() {
                        ParentRequirement::None => None,
                        ParentRequirement::Optional | ParentRequirement::Required => reservation
                            .event
                            .parent()
                            .and_then(|parent_key| {
                                prior
                                    .iter()
                                    .rev()
                                    .find(|receipt| receipt.event.key() == parent_key)
                            })
                            .cloned()
                            .map(|receipt| {
                                ParentView::new(HistoricalSourceFact::new(receipt.event, None))
                            }),
                    };
                    if request.requirements.parent() == ParentRequirement::Required
                        && parent.is_none()
                    {
                        return Err(SourceStateError::Snapshot(
                            "required parent is unavailable at the cutoff".to_string(),
                        ));
                    }
                    let cutoff = crate::normalization::ContextCutoff::new(request.requested_at);
                    let base_context = BaseContextSnapshot::try_new(
                        cutoff,
                        history,
                        parent,
                        EvaluationClock::new(request.requested_at),
                        &request.requirements,
                    )
                    .map_err(|error| SourceStateError::Snapshot(error.to_string()))?;
                    let encoded_bytes = base_context
                        .history()
                        .map_or(0, HistoryView::encoded_bytes);
                    let snapshot_id = state.next_snapshot;
                    state.next_snapshot = state.next_snapshot.saturating_add(1);
                    let compare_token = state.compare_token(
                        &reservation.reservation,
                        Some(snapshot_id),
                    )?;
                    Ok(PipelineStateSnapshot {
                        snapshot_id,
                        selected_pipeline: request.selected_pipeline,
                        base_context,
                        compare_token,
                        encoded_bytes,
                    })
                })
            }

            fn route_only_compare_token(
                &self,
                reservation: &PreflightReservation,
            ) -> Result<ApplicationCompareToken, SourceStateError> {
                self.read(|state| state.compare_token(reservation, None))
            }

            fn record_evaluation_failure(
                &self,
                applied_event_id: AppliedEventId,
                fence: ReservationFence,
                failure: &EvaluationFailure,
                observed_at: DateTimeUtc,
            ) -> Result<(), SourceStateError> {
                self.write(|state| {
                    state.reservation(&applied_event_id, fence)?;
                    state.attempts.push(EvaluationAttemptRecord {
                        applied_event_id,
                        failure_class: failure_class_name(failure.class()).to_string(),
                        retry_safety: retry_safety_name(failure.retry_safety()).to_string(),
                        completion_knowledge: completion_name(failure.completion_knowledge())
                            .to_string(),
                        diagnostic_codes: failure
                            .diagnostics()
                            .items()
                            .iter()
                            .map(|diagnostic| diagnostic.code().as_str().to_string())
                            .collect(),
                        observed_at,
                    });
                    Ok(())
                })
            }

            fn compare_and_commit(
                &self,
                request: CompareAndCommitRequest<'_>,
            ) -> Result<CompareAndCommitResult, SourceStateError> {
                self.write(|state| state.commit(request))
            }

            fn source_state(
                &self,
                key: &SourceEventKey,
            ) -> Result<Option<SourceState>, SourceStateError> {
                self.read(|state| Ok(state.source_states.get(&source_key(key)).cloned()))
            }

            fn checkpoint(
                &self,
                source: &str,
            ) -> Result<Option<SourceCheckpoint>, SourceStateError> {
                self.read(|state| Ok(state.checkpoints.get(source).cloned()))
            }

            fn committed_batch(
                &self,
                id: CommittedBatchId,
            ) -> Result<Option<CommittedNormalizationBatch>, SourceStateError> {
                self.read(|state| Ok(state.batches.get(&id.to_string_id()).cloned()))
            }

            fn recorded_receipts(&self) -> Result<Vec<RecordedReceipt>, SourceStateError> {
                self.read(|state| Ok(state.receipts.clone()))
            }

            fn evaluation_attempts(
                &self,
            ) -> Result<Vec<EvaluationAttemptRecord>, SourceStateError> {
                self.read(|state| Ok(state.attempts.clone()))
            }

            fn lease_publications(
                &self,
                maximum_records: usize,
                now: DateTimeUtc,
                expires_at: DateTimeUtc,
            ) -> Result<Vec<PublicationLease>, SourceStateError> {
                self.write(|state| {
                    let ids = state
                        .outbox
                        .iter()
                        .filter(|(_, record)| {
                            matches!(
                                record.state,
                                PublicationState::Pending
                            ) || matches!(
                                record.state,
                                PublicationState::RetryPending
                            ) || matches!(
                                record.state,
                                PublicationState::RetryScheduled { available_at } if available_at <= now
                            ) || matches!(
                                record.state,
                                PublicationState::Leased { expires_at, .. } if expires_at <= now
                            )
                        })
                        .map(|(id, _)| id.clone())
                        .take(maximum_records.min(128))
                        .collect::<Vec<_>>();
                    let mut leases = Vec::with_capacity(ids.len());
                    for id in ids {
                        let record = state.outbox.get_mut(&id).expect("outbox ID exists");
                        let generation = match &record.state {
                            PublicationState::Leased { fence, .. } => {
                                fence.generation.saturating_add(1)
                            }
                            _ => record.attempts as u64 + 1,
                        };
                        let fence = PublicationLeaseFence {
                            delivery_id: record.delivery_id.clone(),
                            generation,
                            lease_id: state.next_lease,
                        };
                        state.next_lease = state.next_lease.saturating_add(1);
                        record.attempts = record.attempts.saturating_add(1);
                        if record.first_attempt_at.is_none() {
                            record.first_attempt_at = Some(now);
                        }
                        record.state = PublicationState::Leased {
                            fence: fence.clone(),
                            expires_at,
                        };
                        leases.push(PublicationLease {
                            record: record.clone(),
                            fence,
                        });
                    }
                    Ok(leases)
                })
            }

            fn acknowledge_publication(
                &self,
                fence: PublicationLeaseFence,
                acknowledged_at: DateTimeUtc,
            ) -> Result<PublicationState, SourceStateError> {
                self.write(|state| {
                    let record = state
                        .outbox
                        .get_mut(&fence.delivery_id.to_string_id())
                        .ok_or(SourceStateError::InvalidPublicationLease)?;
                    if !matches!(&record.state, PublicationState::Leased { fence: current, .. } if current == &fence)
                    {
                        return Err(SourceStateError::InvalidPublicationLease);
                    }
                    record.state = PublicationState::Published { acknowledged_at };
                    Ok(record.state.clone())
                })
            }

            fn reject_publication(
                &self,
                fence: PublicationLeaseFence,
                disposition: PublicationNackDisposition,
            ) -> Result<PublicationState, SourceStateError> {
                self.write(|state| {
                    let record = state
                        .outbox
                        .get_mut(&fence.delivery_id.to_string_id())
                        .ok_or(SourceStateError::InvalidPublicationLease)?;
                    if !matches!(&record.state, PublicationState::Leased { fence: current, .. } if current == &fence)
                    {
                        return Err(SourceStateError::InvalidPublicationLease);
                    }
                    record.state = match disposition {
                        PublicationNackDisposition::Retry { available_at } => {
                            PublicationState::RetryScheduled { available_at }
                        }
                        PublicationNackDisposition::DeadLetter => PublicationState::DeadLetter,
                    };
                    Ok(record.state.clone())
                })
            }
        }
    };
}

impl_store!(MemorySourceStateStore);
impl_store!(SqliteSourceStateStore);

fn validate_report(
    report: &NormalizationEvaluationReport,
    event: &SourceEvent,
) -> Result<(), SourceStateError> {
    if let NormalizationOutcome::Accepted { candidates } = report.outcome() {
        let selected = report
            .identity()
            .selected_pipeline()
            .ok_or(SourceStateError::InvalidEvaluation)?;
        for (ordinal, candidate) in candidates.as_slice().iter().enumerate() {
            if candidate.candidate_ordinal() != ordinal as u32
                || candidate.evidence().pipeline() != selected
                || candidate.provenance().source() != &SourceEventRef::from(event)
            {
                return Err(SourceStateError::InvalidEvaluation);
            }
        }
    }
    Ok(())
}

fn project_history_event(event: &SourceEvent, include_payload: bool) -> SourceEvent {
    let payload = if include_payload {
        event.payload().clone()
    } else {
        SourcePayload::Empty
    };
    let mut projected = SourceEvent::new(
        event.key().clone(),
        event.operation(),
        event.revision().clone(),
        event.occurred_at(),
        event.received_at(),
        payload,
    );
    if let Some(thread) = event.thread() {
        projected = projected.with_thread(thread.clone());
    }
    if let Some(parent) = event.parent() {
        projected = projected.with_parent(parent.clone());
    }
    if let Some(author) = event.author() {
        projected = projected.with_author(author.clone());
    }
    if let Some(correlation) = event.correlation() {
        projected = projected.with_correlation(correlation.clone());
    }
    if let Some(sequence) = event.sequence() {
        projected = projected.with_sequence(sequence.clone());
    }
    projected.with_metadata(event.metadata().clone())
}

fn applied_event_id(
    event: &SourceEvent,
    delivery: &DurableDeliveryIdentity,
) -> Result<AppliedEventId, SourceStateError> {
    match event.revision() {
        SourceRevision::Monotonic(revision) => {
            Ok(AppliedEventId::for_monotonic(event.key(), *revision)?)
        }
        SourceRevision::Unversioned => Ok(AppliedEventId::for_unversioned(event.key(), delivery)?),
        SourceRevision::Opaque(_) => Err(SourceStateError::UnsupportedRevision),
    }
}

pub fn derive_committed_batch_id(
    batch: &CommittedNormalizationBatch,
) -> Result<CommittedBatchId, DirectIdParseError> {
    if batch.commit_index == 0 || !batch.applied_event_id.matches_source_ref(&batch.source) {
        return Err(DirectIdParseError::new("committed batch"));
    }
    Ok(CommittedBatchId::from_applied_event(
        batch.applied_event_id.clone(),
        batch.commit_index,
    ))
}

fn publication_delivery_id(
    batch_id: CommittedBatchId,
    sink: &str,
) -> Result<PublicationDeliveryId, SourceStateError> {
    PublicationDeliveryId::try_new(batch_id, sink)
        .map_err(|_| SourceStateError::InvalidPublicationSink)
}

fn source_key(key: &SourceEventKey) -> String {
    format!(
        "{}\u{0}{}",
        key.source().as_str(),
        key.external_id().as_str()
    )
}

fn failure_class_name(value: EvaluationFailureClass) -> &'static str {
    match value {
        EvaluationFailureClass::ContextReadFailed => "context_read_failed",
        EvaluationFailureClass::HostUnavailable => "host_unavailable",
        EvaluationFailureClass::ComponentUnavailable => "component_unavailable",
        EvaluationFailureClass::DeadlineExceeded => "deadline_exceeded",
        EvaluationFailureClass::Cancelled => "cancelled",
        EvaluationFailureClass::ResourceExhausted => "resource_exhausted",
        EvaluationFailureClass::ExternalProtocolFailed => "external_protocol_failed",
        EvaluationFailureClass::InternalContractFailed => "internal_contract_failed",
    }
}

fn retry_safety_name(value: EvaluationRetrySafety) -> &'static str {
    match value {
        EvaluationRetrySafety::SafeToRetry => "safe_to_retry",
        EvaluationRetrySafety::UnsafeToRetry => "unsafe_to_retry",
        EvaluationRetrySafety::RequiresIdempotencyEvidence => "requires_idempotency_evidence",
    }
}

fn completion_name(value: CompletionKnowledge) -> &'static str {
    match value {
        CompletionKnowledge::NotStarted => "not_started",
        CompletionKnowledge::StartedMayHaveCompleted => "started_may_have_completed",
        CompletionKnowledge::CompletedWithoutSemanticReport => "completed_without_semantic_report",
        CompletionKnowledge::Unknown => "unknown",
    }
}
