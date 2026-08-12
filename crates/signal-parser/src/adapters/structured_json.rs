use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use qs_core::{RawSignal, validate_raw_signal};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::ingestion::{DateTimeUtc, SourceEvent, SourceEventKey, SourceEventRef, SourceRevision};
use crate::normalization::{
    ContractText, InstrumentHint, SymbolText, normalized_signal_semantic_digest,
};
use crate::state::{
    AppliedEventId, CommittedBatchId, CommittedEvaluationIdentity, CommittedInstrumentHint,
    CommittedNormalizationBatch, CommittedNormalizationOutcome, CommittedNormalizedSignalEnvelope,
    DurableDeliveryIdentity, NormalizationCommitRef, NormalizedLifecycleEvent, NormalizedSignalId,
};

pub const SOURCE_EVENT_JSONL_CODEC: &str = "source-event-jsonl@1";
pub const COMMITTED_NORMALIZATION_JSONL_CODEC: &str = "committed-normalization-jsonl@1";
pub const COMMITTED_NORMALIZATION_BATCH_SCHEMA: &str =
    "quant-system/committed-normalization-batch@1";
pub const COMMITTED_NORMALIZATION_BATCH_SCHEMA_VERSION: u32 = 1;
pub const COMMITTED_NORMALIZATION_BATCH_ARTIFACT_TYPE: &str = COMMITTED_NORMALIZATION_BATCH_SCHEMA;
pub const MAX_STRUCTURED_JSONL_RECORD_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_STRUCTURED_JSON_ERROR_BYTES: usize = 512;
pub const SOURCE_EVENT_JSONL_ARTIFACT_ID_PREFIX: &str = "source-event-jsonl@1:sha256:";

const SHA256_WIRE_PREFIX: &str = "sha256:";
const MAX_COMMITTED_ENVELOPES: usize = 32;
const MAX_COMMITTED_LIFECYCLE_EVENTS: usize = 288;
const MAX_CORRELATION_HINTS: usize = 32;
const MAX_REASON_BYTES: usize = 128;
const MAX_INSTRUMENT_HINT_BYTES: usize = 128;
const MAX_CORRELATION_HINT_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredJsonErrorKind {
    RecordTooLarge,
    ByteOrderMark,
    InvalidJson,
    InvalidSchema,
    InvalidIdentifier,
    InvalidRecord,
    Serialization,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct StructuredJsonError {
    kind: StructuredJsonErrorKind,
    message: String,
}

impl StructuredJsonError {
    fn new(kind: StructuredJsonErrorKind, message: impl std::fmt::Display) -> Self {
        Self {
            kind,
            message: bounded_error_message(message.to_string()),
        }
    }

    pub fn kind(&self) -> StructuredJsonErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("physical line {physical_line}: {error}")]
pub struct JsonlRecordError {
    physical_line: u64,
    error: StructuredJsonError,
}

impl JsonlRecordError {
    fn new(physical_line: u64, mut error: StructuredJsonError) -> Self {
        let prefix_bytes = format!("physical line {physical_line}: ").len();
        error.message = bounded_error_message_with_limit(
            error.message,
            MAX_STRUCTURED_JSON_ERROR_BYTES.saturating_sub(prefix_bytes),
        );
        Self {
            physical_line,
            error,
        }
    }

    pub fn physical_line(&self) -> u64 {
        self.physical_line
    }

    pub fn error(&self) -> &StructuredJsonError {
        &self.error
    }
}

#[derive(Debug)]
pub struct SourceEventJsonlRecord {
    physical_line: u64,
    event: SourceEvent,
    delivery_identity: DurableDeliveryIdentity,
}

impl SourceEventJsonlRecord {
    pub fn physical_line(&self) -> u64 {
        self.physical_line
    }

    pub fn event(&self) -> &SourceEvent {
        &self.event
    }

    pub fn into_event(self) -> SourceEvent {
        self.event
    }

    pub fn delivery_identity(&self) -> &DurableDeliveryIdentity {
        &self.delivery_identity
    }
}

#[derive(Debug)]
pub struct SourceEventJsonlArtifact {
    artifact_identity: String,
    records: Vec<Result<SourceEventJsonlRecord, JsonlRecordError>>,
}

impl SourceEventJsonlArtifact {
    pub fn artifact_identity(&self) -> &str {
        &self.artifact_identity
    }

    pub fn records(&self) -> &[Result<SourceEventJsonlRecord, JsonlRecordError>] {
        &self.records
    }

    pub fn into_records(self) -> Vec<Result<SourceEventJsonlRecord, JsonlRecordError>> {
        self.records
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SourceEventJsonlCodec;

impl SourceEventJsonlCodec {
    pub fn artifact_identity(artifact_bytes: &[u8]) -> String {
        source_event_jsonl_artifact_identity(artifact_bytes)
    }

    pub fn decode(artifact_bytes: &[u8]) -> SourceEventJsonlArtifact {
        decode_source_event_jsonl(artifact_bytes)
    }

    pub fn encode_record(event: &SourceEvent) -> Result<Vec<u8>, StructuredJsonError> {
        encode_source_event_jsonl_record(event)
    }
}

pub fn source_event_jsonl_artifact_identity(artifact_bytes: &[u8]) -> String {
    let digest = Sha256::digest(artifact_bytes);
    format!(
        "{SOURCE_EVENT_JSONL_ARTIFACT_ID_PREFIX}{}",
        lowercase_hex(digest.as_slice())
    )
}

pub fn decode_source_event_jsonl(artifact_bytes: &[u8]) -> SourceEventJsonlArtifact {
    let artifact_identity = source_event_jsonl_artifact_identity(artifact_bytes);
    let mut records = Vec::new();

    for (index, physical_bytes) in artifact_bytes
        .split_inclusive(|byte| *byte == b'\n')
        .enumerate()
    {
        let physical_line = index as u64 + 1;
        let parse_bytes = strip_jsonl_terminator(physical_bytes);
        if parse_bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let decoded = decode_source_event_record(parse_bytes).map(|event| SourceEventJsonlRecord {
            physical_line,
            event,
            delivery_identity: DurableDeliveryIdentity::OfflinePosition {
                artifact: artifact_identity.clone(),
                ordinal: physical_line,
            },
        });
        records.push(decoded.map_err(|error| JsonlRecordError::new(physical_line, error)));
    }

    SourceEventJsonlArtifact {
        artifact_identity,
        records,
    }
}

pub fn encode_source_event_jsonl_record(
    event: &SourceEvent,
) -> Result<Vec<u8>, StructuredJsonError> {
    let mut encoded = serde_json::to_vec(event).map_err(|error| {
        StructuredJsonError::new(
            StructuredJsonErrorKind::Serialization,
            format_args!("SourceEvent serialization failed: {error}"),
        )
    })?;
    enforce_record_bound(encoded.len())?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn decode_source_event_record(parse_bytes: &[u8]) -> Result<SourceEvent, StructuredJsonError> {
    enforce_record_bound(parse_bytes.len())?;
    reject_bom(parse_bytes)?;
    serde_json::from_slice(parse_bytes).map_err(|error| {
        StructuredJsonError::new(
            StructuredJsonErrorKind::InvalidJson,
            format_args!("invalid SourceEvent JSON: {error}"),
        )
    })
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CommittedNormalizationBatchJsonlCodec;

impl CommittedNormalizationBatchJsonlCodec {
    pub fn decode(
        artifact_bytes: &[u8],
    ) -> Vec<Result<CommittedNormalizationBatch, JsonlRecordError>> {
        decode_committed_normalization_batch_jsonl(artifact_bytes)
    }

    pub fn decode_record(
        record_bytes: &[u8],
    ) -> Result<CommittedNormalizationBatch, StructuredJsonError> {
        decode_committed_normalization_batch_jsonl_record(record_bytes)
    }

    pub fn encode<'a, I>(batches: I) -> Result<Vec<u8>, StructuredJsonError>
    where
        I: IntoIterator<Item = &'a CommittedNormalizationBatch>,
    {
        encode_committed_normalization_batch_jsonl(batches)
    }

    pub fn encode_record(
        batch: &CommittedNormalizationBatch,
    ) -> Result<Vec<u8>, StructuredJsonError> {
        encode_committed_normalization_batch_jsonl_record(batch)
    }
}

pub fn decode_committed_normalization_batch_jsonl(
    artifact_bytes: &[u8],
) -> Vec<Result<CommittedNormalizationBatch, JsonlRecordError>> {
    let mut records = Vec::new();
    for (index, physical_bytes) in artifact_bytes
        .split_inclusive(|byte| *byte == b'\n')
        .enumerate()
    {
        let physical_line = index as u64 + 1;
        let parse_bytes = strip_jsonl_terminator(physical_bytes);
        if parse_bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        records.push(
            decode_committed_normalization_batch_jsonl_record(parse_bytes)
                .map_err(|error| JsonlRecordError::new(physical_line, error)),
        );
    }
    records
}

pub fn encode_committed_normalization_batch_jsonl<'a, I>(
    batches: I,
) -> Result<Vec<u8>, StructuredJsonError>
where
    I: IntoIterator<Item = &'a CommittedNormalizationBatch>,
{
    let mut encoded = Vec::new();
    for batch in batches {
        encoded.extend_from_slice(&encode_committed_normalization_batch_jsonl_record(batch)?);
    }
    Ok(encoded)
}

pub fn decode_committed_normalization_batch_jsonl_record(
    record_bytes: &[u8],
) -> Result<CommittedNormalizationBatch, StructuredJsonError> {
    let parse_bytes = strip_jsonl_terminator(record_bytes);
    enforce_record_bound(parse_bytes.len())?;
    reject_bom(parse_bytes)?;
    if parse_bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(invalid_record(
            "committed normalization batch record is blank",
        ));
    }

    let wire: CommittedBatchWire = serde_json::from_slice(parse_bytes).map_err(|error| {
        StructuredJsonError::new(
            StructuredJsonErrorKind::InvalidJson,
            format_args!("invalid committed normalization batch JSON: {error}"),
        )
    })?;
    let batch = wire.into_domain()?;
    validate_committed_normalization_batch(&batch)?;
    Ok(batch)
}

pub fn encode_committed_normalization_batch_jsonl_record(
    batch: &CommittedNormalizationBatch,
) -> Result<Vec<u8>, StructuredJsonError> {
    validate_committed_normalization_batch(batch)?;
    let wire = CommittedBatchWire::from_domain(batch)?;
    let mut encoded = serde_json::to_vec(&wire).map_err(|error| {
        StructuredJsonError::new(
            StructuredJsonErrorKind::Serialization,
            format_args!("committed normalization batch serialization failed: {error}"),
        )
    })?;
    enforce_record_bound(encoded.len())?;
    encoded.push(b'\n');
    Ok(encoded)
}

pub fn validate_committed_normalization_batch(
    batch: &CommittedNormalizationBatch,
) -> Result<(), StructuredJsonError> {
    if batch.commit_index == 0 {
        return Err(invalid_record("commit_index must be greater than zero"));
    }
    if batch.envelopes.len() > MAX_COMMITTED_ENVELOPES {
        return Err(invalid_record(format_args!(
            "envelopes exceeds {MAX_COMMITTED_ENVELOPES} items"
        )));
    }
    if batch.lifecycle.len() > MAX_COMMITTED_LIFECYCLE_EVENTS {
        return Err(invalid_record(format_args!(
            "lifecycle exceeds {MAX_COMMITTED_LIFECYCLE_EVENTS} items"
        )));
    }

    let lifecycle_only = matches!(
        batch.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    );
    match (
        lifecycle_only,
        batch.evaluation_identity.as_ref(),
        batch.evaluation_semantic_digest.as_ref(),
    ) {
        (true, None, None) | (false, Some(_), Some(_)) => {}
        (true, _, _) => {
            return Err(invalid_record(
                "lifecycle-only batches must not contain evaluation identity or semantic digest",
            ));
        }
        (false, _, _) => {
            return Err(invalid_record(
                "completed batches require evaluation identity and semantic digest",
            ));
        }
    }

    let expected_identity = batch.evaluation_identity.as_ref();
    let mut envelope_ids = Vec::with_capacity(batch.envelopes.len());
    let mut unique_envelope_ids = BTreeSet::new();
    for (index, envelope) in batch.envelopes.iter().enumerate() {
        if envelope.commit.batch_id != batch.batch_id
            || envelope.commit.commit_index != batch.commit_index
        {
            return Err(invalid_record(format_args!(
                "envelope {index} commit reference does not own the batch"
            )));
        }
        if envelope.applied_event_id != batch.applied_event_id {
            return Err(invalid_record(format_args!(
                "envelope {index} applied_event_id does not match the batch"
            )));
        }
        if envelope.source != batch.source {
            return Err(invalid_record(format_args!(
                "envelope {index} source reference does not match the batch"
            )));
        }
        if Some(&envelope.evaluation_identity) != expected_identity {
            return Err(invalid_record(format_args!(
                "envelope {index} evaluation identity does not match the batch"
            )));
        }
        if envelope.candidate_ordinal != index as u32 {
            return Err(invalid_record(format_args!(
                "envelope {index} candidate ordinal must be {index}"
            )));
        }
        if !unique_envelope_ids.insert(envelope.normalized_id) {
            return Err(invalid_record("envelope normalized IDs must be unique"));
        }
        validate_envelope(index, envelope)?;
        envelope_ids.push(envelope.normalized_id);
    }

    match &batch.outcome {
        CommittedNormalizationOutcome::Accepted { outputs } => {
            if outputs.is_empty() || outputs.len() > MAX_COMMITTED_ENVELOPES {
                return Err(invalid_record(format_args!(
                    "accepted outputs must contain 1 through {MAX_COMMITTED_ENVELOPES} IDs"
                )));
            }
            if outputs.as_slice() != envelope_ids.as_slice() {
                return Err(invalid_record(
                    "accepted outputs must equal envelope IDs in candidate order",
                ));
            }
            if batch
                .evaluation_identity
                .as_ref()
                .is_some_and(|identity| identity.selected_pipeline.is_none())
            {
                return Err(invalid_record(
                    "accepted batches require a selected pipeline identity",
                ));
            }
        }
        CommittedNormalizationOutcome::Ignored { reason }
        | CommittedNormalizationOutcome::Rejected { reason } => {
            validate_bounded_text(reason, "outcome reason", MAX_REASON_BYTES, false)?;
            require_no_outputs(batch)?;
        }
        CommittedNormalizationOutcome::Ambiguous { alternatives } => {
            if !(2..=8).contains(alternatives) {
                return Err(invalid_record(
                    "ambiguous alternatives must be between 2 and 8",
                ));
            }
            require_no_outputs(batch)?;
        }
        CommittedNormalizationOutcome::ApplicationRejected { reason } => {
            if reason != "active_output_limit" {
                return Err(invalid_record(
                    "application-rejected reason is not recognized by this schema",
                ));
            }
            require_no_outputs(batch)?;
        }
        CommittedNormalizationOutcome::LifecycleOnlyDelete => {
            if !batch.envelopes.is_empty() {
                return Err(invalid_record(
                    "lifecycle-only batches must not contain envelopes",
                ));
            }
        }
    }

    validate_lifecycle(batch, &unique_envelope_ids)
}

fn validate_envelope(
    index: usize,
    envelope: &CommittedNormalizedSignalEnvelope,
) -> Result<(), StructuredJsonError> {
    validate_raw_signal(&envelope.signal).map_err(|error| {
        invalid_record(format_args!(
            "envelope {index} contains an invalid RawSignal: {error}"
        ))
    })?;
    let signal_value = encode_raw_signal_value_v1(&envelope.signal)?;
    crate::normalization::decode_raw_signal_value_v1(signal_value).map_err(|error| {
            invalid_record(format_args!(
                "envelope {index} RawSignal does not satisfy the strict version 1 wire contract: {error}"
            ))
        },
    )?;

    if envelope.correlation_hints.len() > MAX_CORRELATION_HINTS {
        return Err(invalid_record(format_args!(
            "envelope {index} correlation_hints exceeds {MAX_CORRELATION_HINTS} items"
        )));
    }
    for hint in &envelope.correlation_hints {
        validate_bounded_text(hint, "correlation hint", MAX_CORRELATION_HINT_BYTES, false)?;
    }

    let hint = envelope
        .instrument_hint
        .as_ref()
        .map(instrument_hint_from_committed)
        .transpose()?;
    if let (RawSignal::Entry { symbol, .. }, Some(instrument_hint)) =
        (&envelope.signal, hint.as_ref())
        && symbol != instrument_hint.symbol().as_str()
    {
        return Err(invalid_record(format_args!(
            "envelope {index} instrument hint symbol does not match its Entry"
        )));
    }
    let semantic =
        normalized_signal_semantic_digest(&envelope.signal, hint.as_ref()).map_err(|error| {
            invalid_record(format_args!(
                "envelope {index} semantic digest calculation failed: {error}"
            ))
        })?;
    if semantic.digest().as_bytes() != &envelope.semantic_digest {
        return Err(invalid_record(format_args!(
            "envelope {index} semantic digest does not match its signal and instrument hint"
        )));
    }
    Ok(())
}

fn instrument_hint_from_committed(
    hint: &CommittedInstrumentHint,
) -> Result<InstrumentHint, StructuredJsonError> {
    let symbol = SymbolText::try_new(hint.symbol.clone(), "instrument symbol")
        .map_err(invalid_contract_value)?;
    let venue_hint = hint
        .venue
        .clone()
        .map(|value| ContractText::<MAX_INSTRUMENT_HINT_BYTES>::try_new(value, "venue hint"))
        .transpose()
        .map_err(invalid_contract_value)?;
    let market_kind_hint = hint
        .market_kind
        .clone()
        .map(|value| ContractText::<MAX_INSTRUMENT_HINT_BYTES>::try_new(value, "market kind hint"))
        .transpose()
        .map_err(invalid_contract_value)?;
    Ok(InstrumentHint::new(symbol, venue_hint, market_kind_hint))
}

fn require_no_outputs(batch: &CommittedNormalizationBatch) -> Result<(), StructuredJsonError> {
    if !batch.envelopes.is_empty() || !batch.lifecycle.is_empty() {
        return Err(invalid_record(
            "non-accepted completed outcome must not contain envelopes or lifecycle events",
        ));
    }
    Ok(())
}

fn validate_lifecycle(
    batch: &CommittedNormalizationBatch,
    envelope_ids: &BTreeSet<NormalizedSignalId>,
) -> Result<(), StructuredJsonError> {
    let mut current_references = BTreeMap::<NormalizedSignalId, usize>::new();
    for lifecycle in &batch.lifecycle {
        match lifecycle {
            NormalizedLifecycleEvent::Added { output } => {
                require_current_id(*output, envelope_ids, "added output")?;
                *current_references.entry(*output).or_default() += 1;
            }
            NormalizedLifecycleEvent::Superseded { prior, replacement } => {
                require_prior_id(*prior, envelope_ids, "superseded prior")?;
                require_current_id(*replacement, envelope_ids, "superseded replacement")?;
                *current_references.entry(*replacement).or_default() += 1;
            }
            NormalizedLifecycleEvent::Equivalent { evaluated, active } => {
                require_current_id(*evaluated, envelope_ids, "equivalent evaluated")?;
                require_prior_id(*active, envelope_ids, "equivalent active")?;
                *current_references.entry(*evaluated).or_default() += 1;
            }
            NormalizedLifecycleEvent::Withdrawn { output, cause } => {
                require_prior_id(*output, envelope_ids, "withdrawn output")?;
                if *cause != batch.applied_event_id {
                    return Err(invalid_record(
                        "withdrawn lifecycle cause does not match batch applied_event_id",
                    ));
                }
            }
        }
    }

    if matches!(
        batch.outcome,
        CommittedNormalizationOutcome::Accepted { .. }
    ) {
        for id in envelope_ids {
            if current_references.get(id).copied() != Some(1) {
                return Err(invalid_record(
                    "each current envelope ID must have exactly one lifecycle reference",
                ));
            }
        }
    } else if !current_references.is_empty() {
        return Err(invalid_record(
            "a batch without envelopes has current-envelope lifecycle references",
        ));
    }

    if matches!(
        batch.outcome,
        CommittedNormalizationOutcome::LifecycleOnlyDelete
    ) && batch
        .lifecycle
        .iter()
        .any(|event| !matches!(event, NormalizedLifecycleEvent::Withdrawn { .. }))
    {
        return Err(invalid_record(
            "lifecycle-only batches may contain only withdrawn events",
        ));
    }
    Ok(())
}

fn require_current_id(
    id: NormalizedSignalId,
    envelope_ids: &BTreeSet<NormalizedSignalId>,
    field: &'static str,
) -> Result<(), StructuredJsonError> {
    if !envelope_ids.contains(&id) {
        return Err(invalid_record(format_args!(
            "{field} does not reference a current envelope"
        )));
    }
    Ok(())
}

fn require_prior_id(
    id: NormalizedSignalId,
    envelope_ids: &BTreeSet<NormalizedSignalId>,
    field: &'static str,
) -> Result<(), StructuredJsonError> {
    if envelope_ids.contains(&id) {
        return Err(invalid_record(format_args!(
            "{field} incorrectly references a current envelope"
        )));
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommittedBatchWire {
    schema_version: u32,
    artifact_type: String,
    batch_id: String,
    applied_event_id: String,
    source: SourceEventRefWire,
    #[serde(deserialize_with = "deserialize_nullable")]
    evaluation_identity: Option<EvaluationIdentityWire>,
    #[serde(deserialize_with = "deserialize_nullable")]
    evaluation_semantic_digest: Option<String>,
    semantic_basis: CommitSemanticBasisWire,
    outcome: OutcomeWire,
    envelopes: Vec<EnvelopeWire>,
    lifecycle: Vec<LifecycleWire>,
    commit_index: u64,
    committed_at: DateTimeUtc,
}

impl CommittedBatchWire {
    fn from_domain(batch: &CommittedNormalizationBatch) -> Result<Self, StructuredJsonError> {
        Ok(Self {
            schema_version: COMMITTED_NORMALIZATION_BATCH_SCHEMA_VERSION,
            artifact_type: COMMITTED_NORMALIZATION_BATCH_ARTIFACT_TYPE.to_string(),
            batch_id: batch.batch_id.to_string_id(),
            applied_event_id: batch.applied_event_id.to_string_id(),
            source: SourceEventRefWire::from_domain(&batch.source),
            evaluation_identity: batch
                .evaluation_identity
                .as_ref()
                .map(EvaluationIdentityWire::from_domain),
            evaluation_semantic_digest: batch.evaluation_semantic_digest.map(encode_sha256),
            semantic_basis: CommitSemanticBasisWire::from_domain(batch),
            outcome: OutcomeWire::from_domain(&batch.outcome),
            envelopes: batch
                .envelopes
                .iter()
                .map(EnvelopeWire::from_domain)
                .collect::<Result<Vec<_>, _>>()?,
            lifecycle: batch
                .lifecycle
                .iter()
                .map(LifecycleWire::from_domain)
                .collect(),
            commit_index: batch.commit_index,
            committed_at: batch.committed_at,
        })
    }

    fn into_domain(self) -> Result<CommittedNormalizationBatch, StructuredJsonError> {
        if self.artifact_type != COMMITTED_NORMALIZATION_BATCH_ARTIFACT_TYPE {
            return Err(StructuredJsonError::new(
                StructuredJsonErrorKind::InvalidSchema,
                format_args!(
                    "unsupported committed normalization artifact type '{}'",
                    self.artifact_type
                ),
            ));
        }
        if self.schema_version != COMMITTED_NORMALIZATION_BATCH_SCHEMA_VERSION {
            return Err(StructuredJsonError::new(
                StructuredJsonErrorKind::InvalidSchema,
                format_args!(
                    "unsupported committed normalization batch schema version {}",
                    self.schema_version
                ),
            ));
        }
        self.semantic_basis.validate_wire(
            self.evaluation_identity.as_ref(),
            self.evaluation_semantic_digest.as_ref(),
            &self.outcome,
        )?;
        Ok(CommittedNormalizationBatch {
            batch_id: parse_committed_batch_id(&self.batch_id, "batch_id")?,
            applied_event_id: parse_applied_event_id(&self.applied_event_id, "applied_event_id")?,
            source: self.source.into_domain(),
            evaluation_identity: self
                .evaluation_identity
                .map(EvaluationIdentityWire::into_domain)
                .transpose()?,
            evaluation_semantic_digest: self
                .evaluation_semantic_digest
                .map(|value| parse_sha256(&value, "evaluation_semantic_digest"))
                .transpose()?,
            outcome: self.outcome.into_domain()?,
            envelopes: self
                .envelopes
                .into_iter()
                .map(EnvelopeWire::into_domain)
                .collect::<Result<Vec<_>, _>>()?,
            lifecycle: self
                .lifecycle
                .into_iter()
                .map(LifecycleWire::into_domain)
                .collect::<Result<Vec<_>, _>>()?,
            commit_index: self.commit_index,
            committed_at: self.committed_at,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceEventRefWire {
    key: SourceEventKey,
    revision: SourceRevision,
}

impl SourceEventRefWire {
    fn from_domain(source: &SourceEventRef) -> Self {
        Self {
            key: source.key().clone(),
            revision: source.revision().clone(),
        }
    }

    fn into_domain(self) -> SourceEventRef {
        SourceEventRef::new(self.key, self.revision)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvaluationIdentityWire {
    routing_graph: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    selected_pipeline: Option<String>,
}

impl EvaluationIdentityWire {
    fn from_domain(identity: &CommittedEvaluationIdentity) -> Self {
        Self {
            routing_graph: encode_sha256(identity.routing_graph),
            selected_pipeline: identity.selected_pipeline.map(encode_sha256),
        }
    }

    fn into_domain(self) -> Result<CommittedEvaluationIdentity, StructuredJsonError> {
        Ok(CommittedEvaluationIdentity {
            routing_graph: parse_sha256(&self.routing_graph, "routing_graph")?,
            selected_pipeline: self
                .selected_pipeline
                .map(|value| parse_sha256(&value, "selected_pipeline"))
                .transpose()?,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum CommitSemanticBasisWire {
    CompletedEvaluation,
    LifecycleOnlyDelete,
}

impl CommitSemanticBasisWire {
    fn from_domain(batch: &CommittedNormalizationBatch) -> Self {
        if matches!(
            batch.outcome,
            CommittedNormalizationOutcome::LifecycleOnlyDelete
        ) {
            Self::LifecycleOnlyDelete
        } else {
            Self::CompletedEvaluation
        }
    }

    fn validate_wire(
        self,
        identity: Option<&EvaluationIdentityWire>,
        digest: Option<&String>,
        outcome: &OutcomeWire,
    ) -> Result<(), StructuredJsonError> {
        match self {
            Self::CompletedEvaluation
                if identity.is_some()
                    && digest.is_some()
                    && !matches!(outcome, OutcomeWire::LifecycleOnlyDelete) =>
            {
                Ok(())
            }
            Self::LifecycleOnlyDelete
                if identity.is_none()
                    && digest.is_none()
                    && matches!(outcome, OutcomeWire::LifecycleOnlyDelete) =>
            {
                Ok(())
            }
            _ => Err(invalid_record(
                "commit semantic basis conflicts with evaluation evidence or outcome",
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum OutcomeWire {
    Accepted { outputs: Vec<String> },
    Ignored { reason: String },
    Ambiguous { alternatives: u32 },
    Rejected { reason: String },
    ApplicationRejected { reason: String },
    LifecycleOnlyDelete,
}

impl OutcomeWire {
    fn from_domain(outcome: &CommittedNormalizationOutcome) -> Self {
        match outcome {
            CommittedNormalizationOutcome::Accepted { outputs } => Self::Accepted {
                outputs: outputs.iter().map(|id| id.to_string_id()).collect(),
            },
            CommittedNormalizationOutcome::Ignored { reason } => Self::Ignored {
                reason: reason.clone(),
            },
            CommittedNormalizationOutcome::Ambiguous { alternatives } => Self::Ambiguous {
                alternatives: *alternatives,
            },
            CommittedNormalizationOutcome::Rejected { reason } => Self::Rejected {
                reason: reason.clone(),
            },
            CommittedNormalizationOutcome::ApplicationRejected { reason } => {
                Self::ApplicationRejected {
                    reason: reason.clone(),
                }
            }
            CommittedNormalizationOutcome::LifecycleOnlyDelete => Self::LifecycleOnlyDelete,
        }
    }

    fn into_domain(self) -> Result<CommittedNormalizationOutcome, StructuredJsonError> {
        Ok(match self {
            Self::Accepted { outputs } => CommittedNormalizationOutcome::Accepted {
                outputs: outputs
                    .into_iter()
                    .map(|value| parse_normalized_id(&value, "outcome output"))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Self::Ignored { reason } => CommittedNormalizationOutcome::Ignored { reason },
            Self::Ambiguous { alternatives } => {
                CommittedNormalizationOutcome::Ambiguous { alternatives }
            }
            Self::Rejected { reason } => CommittedNormalizationOutcome::Rejected { reason },
            Self::ApplicationRejected { reason } => {
                CommittedNormalizationOutcome::ApplicationRejected { reason }
            }
            Self::LifecycleOnlyDelete => CommittedNormalizationOutcome::LifecycleOnlyDelete,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeWire {
    commit: CommitRefWire,
    normalized_id: String,
    applied_event_id: String,
    signal: Value,
    source: SourceEventRefWire,
    evaluation_identity: EvaluationIdentityWire,
    #[serde(deserialize_with = "deserialize_nullable")]
    instrument_hint: Option<InstrumentHintWire>,
    candidate_ordinal: u32,
    semantic_digest: String,
    correlation_hints: Vec<String>,
}

impl EnvelopeWire {
    fn from_domain(
        envelope: &CommittedNormalizedSignalEnvelope,
    ) -> Result<Self, StructuredJsonError> {
        Ok(Self {
            commit: CommitRefWire::from_domain(&envelope.commit),
            normalized_id: envelope.normalized_id.to_string_id(),
            applied_event_id: envelope.applied_event_id.to_string_id(),
            signal: encode_raw_signal_value_v1(&envelope.signal)?,
            source: SourceEventRefWire::from_domain(&envelope.source),
            evaluation_identity: EvaluationIdentityWire::from_domain(&envelope.evaluation_identity),
            instrument_hint: envelope
                .instrument_hint
                .as_ref()
                .map(InstrumentHintWire::from_domain),
            candidate_ordinal: envelope.candidate_ordinal,
            semantic_digest: encode_sha256(envelope.semantic_digest),
            correlation_hints: envelope.correlation_hints.clone(),
        })
    }

    fn into_domain(self) -> Result<CommittedNormalizedSignalEnvelope, StructuredJsonError> {
        let signal =
            crate::normalization::decode_raw_signal_value_v1(self.signal).map_err(|error| {
                invalid_record(format_args!(
                    "signal does not satisfy the strict version 1 wire contract: {error}"
                ))
            })?;
        Ok(CommittedNormalizedSignalEnvelope {
            commit: self.commit.into_domain()?,
            normalized_id: parse_normalized_id(&self.normalized_id, "normalized_id")?,
            applied_event_id: parse_applied_event_id(
                &self.applied_event_id,
                "envelope applied_event_id",
            )?,
            signal,
            source: self.source.into_domain(),
            evaluation_identity: self.evaluation_identity.into_domain()?,
            instrument_hint: self.instrument_hint.map(InstrumentHintWire::into_domain),
            candidate_ordinal: self.candidate_ordinal,
            semantic_digest: parse_sha256(&self.semantic_digest, "semantic_digest")?,
            correlation_hints: self.correlation_hints,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitRefWire {
    batch_id: String,
    commit_index: u64,
}

impl CommitRefWire {
    fn from_domain(commit: &NormalizationCommitRef) -> Self {
        Self {
            batch_id: commit.batch_id.to_string_id(),
            commit_index: commit.commit_index,
        }
    }

    fn into_domain(self) -> Result<NormalizationCommitRef, StructuredJsonError> {
        Ok(NormalizationCommitRef {
            batch_id: parse_committed_batch_id(&self.batch_id, "commit batch_id")?,
            commit_index: self.commit_index,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentHintWire {
    symbol: String,
    #[serde(deserialize_with = "deserialize_nullable")]
    venue: Option<String>,
    #[serde(deserialize_with = "deserialize_nullable")]
    market_kind: Option<String>,
}

impl InstrumentHintWire {
    fn from_domain(hint: &CommittedInstrumentHint) -> Self {
        Self {
            symbol: hint.symbol.clone(),
            venue: hint.venue.clone(),
            market_kind: hint.market_kind.clone(),
        }
    }

    fn into_domain(self) -> CommittedInstrumentHint {
        CommittedInstrumentHint {
            symbol: self.symbol,
            venue: self.venue,
            market_kind: self.market_kind,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum LifecycleWire {
    Added { output: String },
    Superseded { prior: String, replacement: String },
    Equivalent { evaluated: String, active: String },
    Withdrawn { output: String, cause: String },
}

impl LifecycleWire {
    fn from_domain(event: &NormalizedLifecycleEvent) -> Self {
        match event {
            NormalizedLifecycleEvent::Added { output } => Self::Added {
                output: output.to_string_id(),
            },
            NormalizedLifecycleEvent::Superseded { prior, replacement } => Self::Superseded {
                prior: prior.to_string_id(),
                replacement: replacement.to_string_id(),
            },
            NormalizedLifecycleEvent::Equivalent { evaluated, active } => Self::Equivalent {
                evaluated: evaluated.to_string_id(),
                active: active.to_string_id(),
            },
            NormalizedLifecycleEvent::Withdrawn { output, cause } => Self::Withdrawn {
                output: output.to_string_id(),
                cause: cause.to_string_id(),
            },
        }
    }

    fn into_domain(self) -> Result<NormalizedLifecycleEvent, StructuredJsonError> {
        Ok(match self {
            Self::Added { output } => NormalizedLifecycleEvent::Added {
                output: parse_normalized_id(&output, "added output")?,
            },
            Self::Superseded { prior, replacement } => NormalizedLifecycleEvent::Superseded {
                prior: parse_normalized_id(&prior, "superseded prior")?,
                replacement: parse_normalized_id(&replacement, "superseded replacement")?,
            },
            Self::Equivalent { evaluated, active } => NormalizedLifecycleEvent::Equivalent {
                evaluated: parse_normalized_id(&evaluated, "equivalent evaluated")?,
                active: parse_normalized_id(&active, "equivalent active")?,
            },
            Self::Withdrawn { output, cause } => NormalizedLifecycleEvent::Withdrawn {
                output: parse_normalized_id(&output, "withdrawn output")?,
                cause: parse_applied_event_id(&cause, "withdrawn cause")?,
            },
        })
    }
}

fn parse_committed_batch_id(
    value: &str,
    field: &'static str,
) -> Result<CommittedBatchId, StructuredJsonError> {
    CommittedBatchId::from_string_id(value).map_err(|_| {
        invalid_identifier(format_args!(
            "{field} must use the nb2_ prefix followed by 64 lowercase hexadecimal digits"
        ))
    })
}

fn parse_applied_event_id(
    value: &str,
    field: &'static str,
) -> Result<AppliedEventId, StructuredJsonError> {
    AppliedEventId::from_string_id(value).map_err(|_| {
        invalid_identifier(format_args!(
            "{field} must use the ae1_ prefix followed by 64 lowercase hexadecimal digits"
        ))
    })
}

fn parse_normalized_id(
    value: &str,
    field: &'static str,
) -> Result<NormalizedSignalId, StructuredJsonError> {
    NormalizedSignalId::from_string_id(value).map_err(|_| {
        invalid_identifier(format_args!(
            "{field} must use the ns1_ prefix followed by 64 lowercase hexadecimal digits"
        ))
    })
}

fn encode_raw_signal_value_v1(signal: &RawSignal) -> Result<Value, StructuredJsonError> {
    let mut value = serde_json::to_value(signal).map_err(|error| {
        StructuredJsonError::new(
            StructuredJsonErrorKind::Serialization,
            format_args!("RawSignal serialization failed: {error}"),
        )
    })?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid_record("RawSignal serialization did not produce an object"))?;
    let timestamp = object
        .get_mut("ts")
        .ok_or_else(|| invalid_record("RawSignal serialization omitted ts"))?;
    let timestamp_text = timestamp
        .as_str()
        .ok_or_else(|| invalid_record("RawSignal serialization produced a non-string ts"))?;
    *timestamp = Value::String(format!("{timestamp_text}Z"));
    Ok(value)
}

fn encode_sha256(bytes: [u8; 32]) -> String {
    format!("{SHA256_WIRE_PREFIX}{}", lowercase_hex(&bytes))
}

fn parse_sha256(value: &str, field: &'static str) -> Result<[u8; 32], StructuredJsonError> {
    let Some(hex) = value.strip_prefix(SHA256_WIRE_PREFIX) else {
        return Err(invalid_identifier(format_args!(
            "{field} must use the {SHA256_WIRE_PREFIX} prefix"
        )));
    };
    parse_lowercase_hex_digest(hex, field)
}

fn parse_lowercase_hex_digest(
    hex: &str,
    field: &'static str,
) -> Result<[u8; 32], StructuredJsonError> {
    if hex.len() != 64
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid_identifier(format_args!(
            "{field} must contain exactly 64 lowercase hexadecimal digits"
        )));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("hex input was validated"),
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

fn strip_jsonl_terminator(bytes: &[u8]) -> &[u8] {
    let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    bytes.strip_suffix(b"\r").unwrap_or(bytes)
}

fn reject_bom(bytes: &[u8]) -> Result<(), StructuredJsonError> {
    if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        return Err(StructuredJsonError::new(
            StructuredJsonErrorKind::ByteOrderMark,
            "UTF-8 byte order marks are not accepted",
        ));
    }
    Ok(())
}

fn enforce_record_bound(actual: usize) -> Result<(), StructuredJsonError> {
    if actual > MAX_STRUCTURED_JSONL_RECORD_BYTES {
        return Err(StructuredJsonError::new(
            StructuredJsonErrorKind::RecordTooLarge,
            format_args!(
                "JSONL record exceeds {MAX_STRUCTURED_JSONL_RECORD_BYTES} bytes (got {actual})"
            ),
        ));
    }
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    field: &'static str,
    maximum: usize,
    allow_empty: bool,
) -> Result<(), StructuredJsonError> {
    if !allow_empty && value.is_empty() {
        return Err(invalid_record(format_args!("{field} must not be empty")));
    }
    if value.len() > maximum {
        return Err(invalid_record(format_args!(
            "{field} exceeds {maximum} bytes (got {})",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid_record(format_args!(
            "{field} contains prohibited control characters"
        )));
    }
    Ok(())
}

fn invalid_contract_value(error: impl std::fmt::Display) -> StructuredJsonError {
    invalid_record(error)
}

fn invalid_identifier(message: impl std::fmt::Display) -> StructuredJsonError {
    StructuredJsonError::new(StructuredJsonErrorKind::InvalidIdentifier, message)
}

fn invalid_record(message: impl std::fmt::Display) -> StructuredJsonError {
    StructuredJsonError::new(StructuredJsonErrorKind::InvalidRecord, message)
}

fn bounded_error_message(message: String) -> String {
    bounded_error_message_with_limit(message, MAX_STRUCTURED_JSON_ERROR_BYTES)
}

fn bounded_error_message_with_limit(mut message: String, maximum: usize) -> String {
    if message.len() <= maximum {
        return message;
    }
    let mut boundary = maximum;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message.truncate(boundary);
    message
}
