//! Pure authenticated webhook admission with persistent replay protection.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::ingestion::{ExternalEventId, SourceEvent, SourceEventKey, SourceId};
use crate::state::DurableDeliveryIdentity;

pub const WEBHOOK_METHOD: &str = "POST";
pub const WEBHOOK_PATH: &str = "/v1/source-events";
pub const WEBHOOK_RESPONSE_SCHEMA_VERSION: u32 = 1;
pub const MAX_WEBHOOK_BODY_BYTES: usize = 1_048_576;
pub const MAX_WEBHOOK_KEY_ID_BYTES: usize = 128;
pub const MAX_WEBHOOK_TIMESTAMP_BYTES: usize = 20;
pub const MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES: usize = 256;
pub const MAX_WEBHOOK_SIGNATURE_BYTES: usize = 67;
pub const MAX_WEBHOOK_OUTCOME_REF_BYTES: usize = 512;
pub const MIN_WEBHOOK_SECRET_BYTES: usize = 32;
pub const MAX_WEBHOOK_SECRET_BYTES: usize = 4_096;
pub const MAX_WEBHOOK_REPLAY_RECORDS: usize = 10_000;

const MAX_METHOD_BYTES: usize = 16;
const MAX_PATH_BYTES: usize = 128;
const MAX_QUERY_BYTES: usize = 256;
const MAX_CONTENT_TYPE_BYTES: usize = 128;
const MAX_CONTENT_ENCODING_BYTES: usize = 64;
const SIGNING_DOMAIN: &str = "quant-system-webhook-v1";
const SUBMISSION_ID_PREFIX: &str = "webhook-submission:v2:";
const ADMISSION_ID_PREFIX: &str = "admission:v2:";
const SQLITE_REPLAY_STATE_PENDING: &str = "pending_admission";
const SQLITE_REPLAY_STATE_ACCEPTED: &str = "accepted";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct WebhookRequestParts {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub key_id: Option<String>,
    pub timestamp: Option<String>,
    pub idempotency_key: Option<String>,
    pub signature: Option<String>,
    pub body: Vec<u8>,
}

impl fmt::Debug for WebhookRequestParts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookRequestParts")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("content_type", &self.content_type)
            .field("content_encoding", &self.content_encoding)
            .field("key_id", &self.key_id)
            .field("timestamp", &self.timestamp)
            .field("idempotency_key", &self.idempotency_key)
            .field("signature", &self.signature.as_ref().map(|_| "[REDACTED]"))
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

#[derive(Clone)]
pub struct WebhookRequest {
    method: String,
    path: String,
    query: Option<String>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    key_id: Option<String>,
    timestamp: Option<String>,
    idempotency_key: Option<String>,
    signature: Option<String>,
    body: Vec<u8>,
}

impl fmt::Debug for WebhookRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookRequest")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("query", &self.query)
            .field("content_type", &self.content_type)
            .field("content_encoding", &self.content_encoding)
            .field("key_id", &self.key_id)
            .field("timestamp", &self.timestamp)
            .field("idempotency_key", &self.idempotency_key)
            .field("signature", &self.signature.as_ref().map(|_| "[REDACTED]"))
            .field("body_bytes", &self.body.len())
            .finish()
    }
}

impl WebhookRequest {
    pub fn try_new(parts: WebhookRequestParts) -> Result<Self, WebhookRequestBuildError> {
        validate_ascii(&parts.method, "method", MAX_METHOD_BYTES, false)?;
        validate_ascii(&parts.path, "path", MAX_PATH_BYTES, false)?;
        validate_optional_ascii(&parts.query, "query", MAX_QUERY_BYTES, false)?;
        let content_type =
            normalize_optional_header(parts.content_type, "content type", MAX_CONTENT_TYPE_BYTES)?;
        let content_encoding = normalize_optional_header(
            parts.content_encoding,
            "content encoding",
            MAX_CONTENT_ENCODING_BYTES,
        )?;
        let key_id = normalize_optional_header(parts.key_id, "key ID", MAX_WEBHOOK_KEY_ID_BYTES)?;
        let timestamp =
            normalize_optional_header(parts.timestamp, "timestamp", MAX_WEBHOOK_TIMESTAMP_BYTES)?;
        let idempotency_key = normalize_optional_header(
            parts.idempotency_key,
            "idempotency key",
            MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
        )?;
        let signature =
            normalize_optional_header(parts.signature, "signature", MAX_WEBHOOK_SIGNATURE_BYTES)?;
        if parts.body.len() > MAX_WEBHOOK_BODY_BYTES {
            return Err(WebhookRequestBuildError::BodyTooLarge {
                maximum: MAX_WEBHOOK_BODY_BYTES,
                actual: parts.body.len(),
            });
        }
        Ok(Self {
            method: parts.method,
            path: parts.path,
            query: parts.query,
            content_type,
            content_encoding,
            key_id,
            timestamp,
            idempotency_key,
            signature,
            body: parts.body,
        })
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WebhookRequestBuildError {
    #[error("{field} must contain only printable ASCII")]
    NonAscii { field: &'static str },
    #[error("{field} exceeds {maximum} bytes")]
    FieldTooLarge { field: &'static str, maximum: usize },
    #[error("request body exceeds {maximum} bytes")]
    BodyTooLarge { maximum: usize, actual: usize },
}

#[derive(Clone)]
pub struct WebhookKeyBinding {
    key_id: String,
    source_id: SourceId,
    secret: Vec<u8>,
}

impl WebhookKeyBinding {
    pub fn try_new(
        key_id: impl Into<String>,
        source_id: SourceId,
        secret: impl Into<Vec<u8>>,
    ) -> Result<Self, WebhookProfileError> {
        let key_id = key_id.into();
        validate_ascii(&key_id, "key ID", MAX_WEBHOOK_KEY_ID_BYTES, true)
            .map_err(WebhookProfileError::InvalidField)?;
        let secret = secret.into();
        if !(MIN_WEBHOOK_SECRET_BYTES..=MAX_WEBHOOK_SECRET_BYTES).contains(&secret.len()) {
            return Err(WebhookProfileError::InvalidSecretLength);
        }
        Ok(Self {
            key_id,
            source_id,
            secret,
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
}

impl fmt::Debug for WebhookKeyBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookKeyBinding")
            .field("key_id", &self.key_id)
            .field("source_id", &self.source_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct WebhookProfile {
    maximum_body_bytes: usize,
    maximum_clock_skew_seconds: u64,
    replay_retention_seconds: u64,
    bindings: BTreeMap<String, WebhookKeyBinding>,
}

impl WebhookProfile {
    pub fn try_new(
        maximum_body_bytes: usize,
        maximum_clock_skew_seconds: u64,
        replay_retention_seconds: u64,
        bindings: Vec<WebhookKeyBinding>,
    ) -> Result<Self, WebhookProfileError> {
        if maximum_body_bytes == 0 || maximum_body_bytes > MAX_WEBHOOK_BODY_BYTES {
            return Err(WebhookProfileError::InvalidBodyLimit);
        }
        if replay_retention_seconds == 0 {
            return Err(WebhookProfileError::InvalidReplayRetention);
        }
        if bindings.is_empty() {
            return Err(WebhookProfileError::MissingKeyBinding);
        }
        let mut indexed = BTreeMap::new();
        for binding in bindings {
            if indexed.insert(binding.key_id.clone(), binding).is_some() {
                return Err(WebhookProfileError::DuplicateKeyId);
            }
        }
        Ok(Self {
            maximum_body_bytes,
            maximum_clock_skew_seconds,
            replay_retention_seconds,
            bindings: indexed,
        })
    }

    pub fn maximum_body_bytes(&self) -> usize {
        self.maximum_body_bytes
    }

    pub fn maximum_clock_skew_seconds(&self) -> u64 {
        self.maximum_clock_skew_seconds
    }

    pub fn replay_retention_seconds(&self) -> u64 {
        self.replay_retention_seconds
    }
}

impl fmt::Debug for WebhookProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookProfile")
            .field("maximum_body_bytes", &self.maximum_body_bytes)
            .field(
                "maximum_clock_skew_seconds",
                &self.maximum_clock_skew_seconds,
            )
            .field("replay_retention_seconds", &self.replay_retention_seconds)
            .field("key_ids", &self.bindings.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookProfileError {
    #[error("invalid webhook profile field: {0}")]
    InvalidField(WebhookRequestBuildError),
    #[error("maximum body bytes must be within the supported bound")]
    InvalidBodyLimit,
    #[error("replay retention must be positive")]
    InvalidReplayRetention,
    #[error("at least one key binding is required")]
    MissingKeyBinding,
    #[error("key IDs must be unique")]
    DuplicateKeyId,
    #[error("HMAC secret length is outside the supported bound")]
    InvalidSecretLength,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SubmissionId(String);

impl SubmissionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_stored(value: String) -> Result<Self, WebhookReplayStoreError> {
        validate_direct_identity(&value, SUBMISSION_ID_PREFIX)?;
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AdmissionIdentity(String);

impl AdmissionIdentity {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn durable_delivery_identity(&self) -> DurableDeliveryIdentity {
        DurableDeliveryIdentity::Stable(self.0.clone())
    }

    fn from_stored(value: String) -> Result<Self, WebhookReplayStoreError> {
        validate_direct_identity(&value, ADMISSION_ID_PREFIX)?;
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OutcomeRef(String);

impl OutcomeRef {
    pub fn try_new(value: impl Into<String>) -> Result<Self, OutcomeRefError> {
        let value = value.into();
        validate_ascii(
            &value,
            "outcome reference",
            MAX_WEBHOOK_OUTCOME_REF_BYTES,
            true,
        )
        .map_err(|_| OutcomeRefError)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("outcome reference is invalid")]
pub struct OutcomeRefError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDisposition {
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpSourceSubmissionResponseV1 {
    pub schema_version: u32,
    pub submission_id: SubmissionId,
    pub source_key: SourceEventKey,
    pub disposition: SubmissionDisposition,
    pub outcome_ref: Option<OutcomeRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookErrorCode {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    ReplayConflict,
    ContentTooLarge,
    TooManyRequests,
    ServiceUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookErrorResponseV1 {
    pub schema_version: u32,
    pub code: WebhookErrorCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookHttpStatus {
    Ok,
    Accepted,
    BadRequest,
    Unauthorized,
    Forbidden,
    Conflict,
    ContentTooLarge,
    TooManyRequests,
    ServiceUnavailable,
}

impl WebhookHttpStatus {
    pub fn as_u16(self) -> u16 {
        match self {
            Self::Ok => 200,
            Self::Accepted => 202,
            Self::BadRequest => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::Conflict => 409,
            Self::ContentTooLarge => 413,
            Self::TooManyRequests => 429,
            Self::ServiceUnavailable => 503,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookResponseBody {
    Submission(HttpSourceSubmissionResponseV1),
    Error(WebhookErrorResponseV1),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookHttpResponse {
    pub status: WebhookHttpStatus,
    pub body: WebhookResponseBody,
}

impl WebhookHttpResponse {
    fn submission(status: WebhookHttpStatus, record: WebhookReplayRecord) -> WebhookHttpResponse {
        Self {
            status,
            body: WebhookResponseBody::Submission(HttpSourceSubmissionResponseV1 {
                schema_version: WEBHOOK_RESPONSE_SCHEMA_VERSION,
                submission_id: record.submission_id,
                source_key: record.source_key,
                disposition: SubmissionDisposition::Accepted,
                outcome_ref: record.outcome_ref,
            }),
        }
    }

    fn error(status: WebhookHttpStatus, code: WebhookErrorCode) -> WebhookHttpResponse {
        Self {
            status,
            body: WebhookResponseBody::Error(WebhookErrorResponseV1 {
                schema_version: WEBHOOK_RESPONSE_SCHEMA_VERSION,
                code,
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdmissionSubmission {
    pub identity: AdmissionIdentity,
    pub delivery_identity: DurableDeliveryIdentity,
    pub submission_id: SubmissionId,
    pub event: SourceEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DurableAdmissionAcknowledgement {
    pub outcome_ref: Option<OutcomeRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("admission conflict")]
    Conflict,
    #[error("admission capacity is currently exhausted")]
    TooManyRequests,
    #[error("admission service is unavailable")]
    Unavailable,
}

/// Synchronous test seam for durable provider-neutral application admission.
/// Returning success acknowledges that the identity and event were durably admitted.
pub trait SourceEventAdmissionPort: Send + Sync {
    fn admit(
        &self,
        submission: AdmissionSubmission,
    ) -> Result<DurableAdmissionAcknowledgement, AdmissionError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookReplayState {
    PendingAdmission,
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookReplayRecord {
    source_id: SourceId,
    idempotency_key: String,
    body_sha256: [u8; 32],
    submission_id: SubmissionId,
    admission_identity: AdmissionIdentity,
    source_key: SourceEventKey,
    state: WebhookReplayState,
    outcome_ref: Option<OutcomeRef>,
    first_seen_unix_seconds: u64,
    expires_at_unix_seconds: u64,
}

impl WebhookReplayRecord {
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn body_sha256(&self) -> &[u8; 32] {
        &self.body_sha256
    }

    pub fn submission_id(&self) -> &SubmissionId {
        &self.submission_id
    }

    pub fn admission_identity(&self) -> &AdmissionIdentity {
        &self.admission_identity
    }

    pub fn source_key(&self) -> &SourceEventKey {
        &self.source_key
    }

    pub fn state(&self) -> WebhookReplayState {
        self.state
    }

    pub fn outcome_ref(&self) -> Option<&OutcomeRef> {
        self.outcome_ref.as_ref()
    }

    pub fn first_seen_unix_seconds(&self) -> u64 {
        self.first_seen_unix_seconds
    }

    pub fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }
}

#[derive(Debug, Clone)]
pub struct WebhookReplayReservation {
    pub source_id: SourceId,
    pub idempotency_key: String,
    pub body_sha256: [u8; 32],
    pub source_key: SourceEventKey,
    pub now_unix_seconds: u64,
    pub expires_at_unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookReplayReservationResult {
    Reserved(WebhookReplayRecord),
    ExistingPending(WebhookReplayRecord),
    ExistingAccepted(WebhookReplayRecord),
    Conflict,
}

#[derive(Debug, Clone)]
pub struct WebhookReplayAcceptance {
    pub source_id: SourceId,
    pub idempotency_key: String,
    pub admission_identity: AdmissionIdentity,
    pub outcome_ref: Option<OutcomeRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookReplayAcceptanceResult {
    AcceptedNow(WebhookReplayRecord),
    AlreadyAccepted(WebhookReplayRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WebhookReplayStoreError {
    #[error("webhook replay store is unavailable")]
    Unavailable,
    #[error("webhook replay store reached its record limit")]
    CapacityExceeded,
    #[error("webhook replay state is invalid")]
    Corrupt,
    #[error("webhook replay acceptance lost its reservation")]
    ReservationLost,
}

/// Persistent replay state is separate from source-event application state.
pub trait WebhookReplayStore: Send + Sync {
    fn reserve(
        &self,
        reservation: WebhookReplayReservation,
    ) -> Result<WebhookReplayReservationResult, WebhookReplayStoreError>;

    fn mark_accepted(
        &self,
        acceptance: WebhookReplayAcceptance,
    ) -> Result<WebhookReplayAcceptanceResult, WebhookReplayStoreError>;
}

#[derive(Debug)]
pub struct MemoryWebhookReplayStore {
    records: Mutex<BTreeMap<(SourceId, String), WebhookReplayRecord>>,
    maximum_records: NonZeroUsize,
}

impl Default for MemoryWebhookReplayStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryWebhookReplayStore {
    pub fn new() -> Self {
        Self::with_maximum_records(
            NonZeroUsize::new(MAX_WEBHOOK_REPLAY_RECORDS).expect("replay record limit is nonzero"),
        )
    }

    pub fn with_maximum_records(maximum_records: NonZeroUsize) -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            maximum_records,
        }
    }
}

impl WebhookReplayStore for MemoryWebhookReplayStore {
    fn reserve(
        &self,
        reservation: WebhookReplayReservation,
    ) -> Result<WebhookReplayReservationResult, WebhookReplayStoreError> {
        validate_reservation(&reservation)?;
        let key = (
            reservation.source_id.clone(),
            reservation.idempotency_key.clone(),
        );
        let mut records = self
            .records
            .lock()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        records.retain(|_, record| record.expires_at_unix_seconds > reservation.now_unix_seconds);
        if let Some(record) = records.get(&key) {
            return Ok(if record.body_sha256 != reservation.body_sha256 {
                WebhookReplayReservationResult::Conflict
            } else if record.state == WebhookReplayState::Accepted {
                WebhookReplayReservationResult::ExistingAccepted(record.clone())
            } else {
                WebhookReplayReservationResult::ExistingPending(record.clone())
            });
        }
        if records.len() >= self.maximum_records.get() {
            return Err(WebhookReplayStoreError::CapacityExceeded);
        }
        let (submission_id, admission_identity) = deterministic_identities(
            &reservation.source_id,
            &reservation.idempotency_key,
            reservation.now_unix_seconds,
        );
        let record = WebhookReplayRecord {
            source_id: reservation.source_id,
            idempotency_key: reservation.idempotency_key,
            body_sha256: reservation.body_sha256,
            submission_id,
            admission_identity,
            source_key: reservation.source_key,
            state: WebhookReplayState::PendingAdmission,
            outcome_ref: None,
            first_seen_unix_seconds: reservation.now_unix_seconds,
            expires_at_unix_seconds: reservation.expires_at_unix_seconds,
        };
        records.insert(key, record.clone());
        Ok(WebhookReplayReservationResult::Reserved(record))
    }

    fn mark_accepted(
        &self,
        acceptance: WebhookReplayAcceptance,
    ) -> Result<WebhookReplayAcceptanceResult, WebhookReplayStoreError> {
        validate_ascii(
            &acceptance.idempotency_key,
            "idempotency key",
            MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
            true,
        )
        .map_err(|_| WebhookReplayStoreError::Corrupt)?;
        let mut records = self
            .records
            .lock()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let record = records
            .get_mut(&(acceptance.source_id, acceptance.idempotency_key))
            .ok_or(WebhookReplayStoreError::ReservationLost)?;
        if record.admission_identity != acceptance.admission_identity {
            return Err(WebhookReplayStoreError::ReservationLost);
        }
        if record.state == WebhookReplayState::Accepted {
            if record.outcome_ref != acceptance.outcome_ref {
                return Err(WebhookReplayStoreError::Corrupt);
            }
            return Ok(WebhookReplayAcceptanceResult::AlreadyAccepted(
                record.clone(),
            ));
        }
        record.state = WebhookReplayState::Accepted;
        record.outcome_ref = acceptance.outcome_ref;
        Ok(WebhookReplayAcceptanceResult::AcceptedNow(record.clone()))
    }
}

pub struct SqliteWebhookReplayStore {
    connection: Mutex<Connection>,
    maximum_records: NonZeroUsize,
}

impl fmt::Debug for SqliteWebhookReplayStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SqliteWebhookReplayStore").finish()
    }
}

impl SqliteWebhookReplayStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WebhookReplayStoreError> {
        Self::open_with_maximum_records(
            path,
            NonZeroUsize::new(MAX_WEBHOOK_REPLAY_RECORDS).expect("replay record limit is nonzero"),
        )
    }

    pub fn open_with_maximum_records(
        path: impl AsRef<Path>,
        maximum_records: NonZeroUsize,
    ) -> Result<Self, WebhookReplayStoreError> {
        let connection =
            Connection::open(path).map_err(|_| WebhookReplayStoreError::Unavailable)?;
        Self::initialize(connection, maximum_records)
    }

    pub fn open_in_memory() -> Result<Self, WebhookReplayStoreError> {
        Self::open_in_memory_with_maximum_records(
            NonZeroUsize::new(MAX_WEBHOOK_REPLAY_RECORDS).expect("replay record limit is nonzero"),
        )
    }

    pub fn open_in_memory_with_maximum_records(
        maximum_records: NonZeroUsize,
    ) -> Result<Self, WebhookReplayStoreError> {
        let connection =
            Connection::open_in_memory().map_err(|_| WebhookReplayStoreError::Unavailable)?;
        Self::initialize(connection, maximum_records)
    }

    fn initialize(
        connection: Connection,
        maximum_records: NonZeroUsize,
    ) -> Result<Self, WebhookReplayStoreError> {
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS webhook_replay_v1 (
                    source_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    body_sha256 BLOB NOT NULL CHECK(length(body_sha256) = 32),
                    submission_id TEXT NOT NULL,
                    admission_identity TEXT NOT NULL,
                    external_event_id TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('pending_admission', 'accepted')),
                    outcome_ref TEXT,
                    first_seen_unix_seconds INTEGER NOT NULL CHECK(first_seen_unix_seconds >= 0),
                    expires_at_unix_seconds INTEGER NOT NULL CHECK(expires_at_unix_seconds >= 0),
                    PRIMARY KEY (source_id, idempotency_key)
                );",
            )
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        Ok(Self {
            connection: Mutex::new(connection),
            maximum_records,
        })
    }

    pub fn quick_check(&self) -> Result<(), WebhookReplayStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let result: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        if result == "ok" {
            Ok(())
        } else {
            Err(WebhookReplayStoreError::Corrupt)
        }
    }
}

impl WebhookReplayStore for SqliteWebhookReplayStore {
    fn reserve(
        &self,
        reservation: WebhookReplayReservation,
    ) -> Result<WebhookReplayReservationResult, WebhookReplayStoreError> {
        validate_reservation(&reservation)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        transaction
            .execute(
                "DELETE FROM webhook_replay_v1 WHERE expires_at_unix_seconds <= ?1",
                params![to_sqlite_integer(reservation.now_unix_seconds)?],
            )
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let existing = query_replay_record(
            &transaction,
            reservation.source_id.as_str(),
            &reservation.idempotency_key,
        )?;
        let result = if let Some(record) = existing {
            if record.body_sha256 != reservation.body_sha256 {
                WebhookReplayReservationResult::Conflict
            } else if record.state == WebhookReplayState::Accepted {
                WebhookReplayReservationResult::ExistingAccepted(record)
            } else {
                WebhookReplayReservationResult::ExistingPending(record)
            }
        } else {
            let record_count: usize = transaction
                .query_row("SELECT COUNT(*) FROM webhook_replay_v1", [], |row| {
                    row.get(0)
                })
                .map_err(|_| WebhookReplayStoreError::Unavailable)?;
            if record_count >= self.maximum_records.get() {
                return Err(WebhookReplayStoreError::CapacityExceeded);
            }
            let (submission_id, admission_identity) = deterministic_identities(
                &reservation.source_id,
                &reservation.idempotency_key,
                reservation.now_unix_seconds,
            );
            transaction
                .execute(
                    "INSERT INTO webhook_replay_v1 (
                        source_id, idempotency_key, body_sha256, submission_id,
                        admission_identity, external_event_id, state, outcome_ref,
                        first_seen_unix_seconds, expires_at_unix_seconds
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9)",
                    params![
                        reservation.source_id.as_str(),
                        &reservation.idempotency_key,
                        reservation.body_sha256.as_slice(),
                        submission_id.as_str(),
                        admission_identity.as_str(),
                        reservation.source_key.external_id().as_str(),
                        SQLITE_REPLAY_STATE_PENDING,
                        to_sqlite_integer(reservation.now_unix_seconds)?,
                        to_sqlite_integer(reservation.expires_at_unix_seconds)?,
                    ],
                )
                .map_err(|_| WebhookReplayStoreError::Unavailable)?;
            WebhookReplayReservationResult::Reserved(WebhookReplayRecord {
                source_id: reservation.source_id,
                idempotency_key: reservation.idempotency_key,
                body_sha256: reservation.body_sha256,
                submission_id,
                admission_identity,
                source_key: reservation.source_key,
                state: WebhookReplayState::PendingAdmission,
                outcome_ref: None,
                first_seen_unix_seconds: reservation.now_unix_seconds,
                expires_at_unix_seconds: reservation.expires_at_unix_seconds,
            })
        };
        transaction
            .commit()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        Ok(result)
    }

    fn mark_accepted(
        &self,
        acceptance: WebhookReplayAcceptance,
    ) -> Result<WebhookReplayAcceptanceResult, WebhookReplayStoreError> {
        validate_ascii(
            &acceptance.idempotency_key,
            "idempotency key",
            MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
            true,
        )
        .map_err(|_| WebhookReplayStoreError::Corrupt)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let existing = query_replay_record(
            &transaction,
            acceptance.source_id.as_str(),
            &acceptance.idempotency_key,
        )?
        .ok_or(WebhookReplayStoreError::ReservationLost)?;
        if existing.admission_identity != acceptance.admission_identity {
            return Err(WebhookReplayStoreError::ReservationLost);
        }
        if existing.state == WebhookReplayState::Accepted {
            if existing.outcome_ref != acceptance.outcome_ref {
                return Err(WebhookReplayStoreError::Corrupt);
            }
            transaction
                .commit()
                .map_err(|_| WebhookReplayStoreError::Unavailable)?;
            return Ok(WebhookReplayAcceptanceResult::AlreadyAccepted(existing));
        }
        transaction
            .execute(
                "UPDATE webhook_replay_v1
                 SET state = ?1, outcome_ref = ?2
                 WHERE source_id = ?3 AND idempotency_key = ?4 AND admission_identity = ?5",
                params![
                    SQLITE_REPLAY_STATE_ACCEPTED,
                    acceptance.outcome_ref.as_ref().map(OutcomeRef::as_str),
                    acceptance.source_id.as_str(),
                    &acceptance.idempotency_key,
                    acceptance.admission_identity.as_str(),
                ],
            )
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        let mut accepted = existing;
        accepted.state = WebhookReplayState::Accepted;
        accepted.outcome_ref = acceptance.outcome_ref;
        transaction
            .commit()
            .map_err(|_| WebhookReplayStoreError::Unavailable)?;
        Ok(WebhookReplayAcceptanceResult::AcceptedNow(accepted))
    }
}

pub struct AuthenticatedWebhookAdapter {
    profile: WebhookProfile,
    replay_store: Arc<dyn WebhookReplayStore>,
    admission_port: Arc<dyn SourceEventAdmissionPort>,
}

impl fmt::Debug for AuthenticatedWebhookAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedWebhookAdapter")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

impl AuthenticatedWebhookAdapter {
    pub fn new(
        profile: WebhookProfile,
        replay_store: Arc<dyn WebhookReplayStore>,
        admission_port: Arc<dyn SourceEventAdmissionPort>,
    ) -> Self {
        Self {
            profile,
            replay_store,
            admission_port,
        }
    }

    pub fn maximum_body_bytes(&self) -> usize {
        self.profile.maximum_body_bytes()
    }

    pub fn handle_parts(
        &self,
        parts: WebhookRequestParts,
        trusted_now_unix_seconds: u64,
    ) -> WebhookHttpResponse {
        match WebhookRequest::try_new(parts) {
            Ok(request) => self.handle(request, trusted_now_unix_seconds),
            Err(WebhookRequestBuildError::BodyTooLarge { .. }) => WebhookHttpResponse::error(
                WebhookHttpStatus::ContentTooLarge,
                WebhookErrorCode::ContentTooLarge,
            ),
            Err(_) => WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ),
        }
    }

    pub fn handle(
        &self,
        request: WebhookRequest,
        trusted_now_unix_seconds: u64,
    ) -> WebhookHttpResponse {
        let validation = match self.authenticate_and_decode(&request, trusted_now_unix_seconds) {
            Ok(validation) => validation,
            Err(response) => return response,
        };
        let expires_at_unix_seconds = match trusted_now_unix_seconds
            .checked_add(self.profile.replay_retention_seconds)
            .filter(|value| *value <= i64::MAX as u64)
        {
            Some(value) => value,
            None => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::ServiceUnavailable,
                    WebhookErrorCode::ServiceUnavailable,
                );
            }
        };
        let reservation = WebhookReplayReservation {
            source_id: validation.binding.source_id.clone(),
            idempotency_key: validation.idempotency_key.clone(),
            body_sha256: validation.body_sha256,
            source_key: validation.event.key().clone(),
            now_unix_seconds: trusted_now_unix_seconds,
            expires_at_unix_seconds,
        };
        let record = match self.replay_store.reserve(reservation) {
            Ok(WebhookReplayReservationResult::ExistingAccepted(record)) => {
                return WebhookHttpResponse::submission(WebhookHttpStatus::Ok, record);
            }
            Ok(WebhookReplayReservationResult::Conflict) => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::Conflict,
                    WebhookErrorCode::ReplayConflict,
                );
            }
            Ok(WebhookReplayReservationResult::Reserved(record))
            | Ok(WebhookReplayReservationResult::ExistingPending(record)) => record,
            Err(_) => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::ServiceUnavailable,
                    WebhookErrorCode::ServiceUnavailable,
                );
            }
        };
        let acknowledgement = match self.admission_port.admit(AdmissionSubmission {
            identity: record.admission_identity.clone(),
            delivery_identity: record.admission_identity.durable_delivery_identity(),
            submission_id: record.submission_id.clone(),
            event: validation.event,
        }) {
            Ok(acknowledgement) => acknowledgement,
            Err(AdmissionError::Conflict) => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::Conflict,
                    WebhookErrorCode::ReplayConflict,
                );
            }
            Err(AdmissionError::TooManyRequests) => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::TooManyRequests,
                    WebhookErrorCode::TooManyRequests,
                );
            }
            Err(AdmissionError::Unavailable) => {
                return WebhookHttpResponse::error(
                    WebhookHttpStatus::ServiceUnavailable,
                    WebhookErrorCode::ServiceUnavailable,
                );
            }
        };
        let acceptance = WebhookReplayAcceptance {
            source_id: record.source_id,
            idempotency_key: record.idempotency_key,
            admission_identity: record.admission_identity,
            outcome_ref: acknowledgement.outcome_ref,
        };
        match self.replay_store.mark_accepted(acceptance) {
            Ok(WebhookReplayAcceptanceResult::AcceptedNow(record)) => {
                WebhookHttpResponse::submission(WebhookHttpStatus::Accepted, record)
            }
            Ok(WebhookReplayAcceptanceResult::AlreadyAccepted(record)) => {
                WebhookHttpResponse::submission(WebhookHttpStatus::Ok, record)
            }
            Err(_) => WebhookHttpResponse::error(
                WebhookHttpStatus::ServiceUnavailable,
                WebhookErrorCode::ServiceUnavailable,
            ),
        }
    }

    fn authenticate_and_decode<'a>(
        &'a self,
        request: &WebhookRequest,
        trusted_now_unix_seconds: u64,
    ) -> Result<AuthenticatedRequest<'a>, WebhookHttpResponse> {
        if request.method != WEBHOOK_METHOD
            || request.path != WEBHOOK_PATH
            || request.query.is_some()
            || request.content_encoding.is_some()
            || !request
                .content_type
                .as_deref()
                .is_some_and(is_application_json)
        {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        }
        if request.body.len() > self.profile.maximum_body_bytes {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::ContentTooLarge,
                WebhookErrorCode::ContentTooLarge,
            ));
        }
        let Some(key_id) = request.key_id.as_deref().filter(|value| !value.is_empty()) else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        };
        let Some(binding) = self.profile.bindings.get(key_id) else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::Unauthorized,
                WebhookErrorCode::Unauthorized,
            ));
        };
        let Some(timestamp_text) = request.timestamp.as_deref() else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        };
        let Some(timestamp) = parse_canonical_unix_seconds(timestamp_text) else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        };
        if timestamp.abs_diff(trusted_now_unix_seconds) > self.profile.maximum_clock_skew_seconds {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::Unauthorized,
                WebhookErrorCode::Unauthorized,
            ));
        }
        let Some(idempotency_key) = request
            .idempotency_key
            .as_deref()
            .filter(|value| !value.is_empty())
        else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        };
        let Some(signature) = request.signature.as_deref() else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            ));
        };
        let Some(signature_bytes) = decode_signature(signature) else {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::Unauthorized,
                WebhookErrorCode::Unauthorized,
            ));
        };
        let body_sha256 = sha256(&request.body);
        let material =
            canonical_material_from_digest(key_id, timestamp_text, idempotency_key, &body_sha256);
        let mut verifier = HmacSha256::new_from_slice(&binding.secret)
            .expect("HMAC accepts every supported secret length");
        verifier.update(&material);
        if verifier.verify_slice(&signature_bytes).is_err() {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::Unauthorized,
                WebhookErrorCode::Unauthorized,
            ));
        }
        let event: SourceEvent = serde_json::from_slice(&request.body).map_err(|_| {
            WebhookHttpResponse::error(
                WebhookHttpStatus::BadRequest,
                WebhookErrorCode::InvalidRequest,
            )
        })?;
        if event.key().source() != &binding.source_id {
            return Err(WebhookHttpResponse::error(
                WebhookHttpStatus::Forbidden,
                WebhookErrorCode::Forbidden,
            ));
        }
        Ok(AuthenticatedRequest {
            binding,
            idempotency_key: idempotency_key.to_string(),
            body_sha256,
            event,
        })
    }
}

struct AuthenticatedRequest<'a> {
    binding: &'a WebhookKeyBinding,
    idempotency_key: String,
    body_sha256: [u8; 32],
    event: SourceEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WebhookSigningError {
    #[error("signing field is invalid")]
    InvalidField,
    #[error("request body exceeds the webhook signing bound")]
    BodyTooLarge,
    #[error("HMAC secret length is outside the supported bound")]
    InvalidSecret,
}

pub fn webhook_signing_material(
    key_id: &str,
    unix_seconds: &str,
    idempotency_key: &str,
    body: &[u8],
) -> Result<Vec<u8>, WebhookSigningError> {
    validate_ascii(key_id, "key ID", MAX_WEBHOOK_KEY_ID_BYTES, true)
        .map_err(|_| WebhookSigningError::InvalidField)?;
    validate_ascii(
        idempotency_key,
        "idempotency key",
        MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
        true,
    )
    .map_err(|_| WebhookSigningError::InvalidField)?;
    if parse_canonical_unix_seconds(unix_seconds).is_none() {
        return Err(WebhookSigningError::InvalidField);
    }
    if body.len() > MAX_WEBHOOK_BODY_BYTES {
        return Err(WebhookSigningError::BodyTooLarge);
    }
    Ok(canonical_material_from_digest(
        key_id,
        unix_seconds,
        idempotency_key,
        &sha256(body),
    ))
}

pub fn sign_webhook_v1(
    secret: &[u8],
    key_id: &str,
    unix_seconds: &str,
    idempotency_key: &str,
    body: &[u8],
) -> Result<String, WebhookSigningError> {
    if !(MIN_WEBHOOK_SECRET_BYTES..=MAX_WEBHOOK_SECRET_BYTES).contains(&secret.len()) {
        return Err(WebhookSigningError::InvalidSecret);
    }
    let material = webhook_signing_material(key_id, unix_seconds, idempotency_key, body)?;
    let mut signer =
        HmacSha256::new_from_slice(secret).map_err(|_| WebhookSigningError::InvalidSecret)?;
    signer.update(&material);
    Ok(format!("v1={}", lower_hex(&signer.finalize().into_bytes())))
}

fn validate_reservation(
    reservation: &WebhookReplayReservation,
) -> Result<(), WebhookReplayStoreError> {
    validate_ascii(
        &reservation.idempotency_key,
        "idempotency key",
        MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
        true,
    )
    .map_err(|_| WebhookReplayStoreError::Corrupt)?;
    if reservation.source_key.source() != &reservation.source_id
        || reservation.expires_at_unix_seconds <= reservation.now_unix_seconds
        || reservation.expires_at_unix_seconds > i64::MAX as u64
        || reservation.now_unix_seconds > i64::MAX as u64
    {
        return Err(WebhookReplayStoreError::Corrupt);
    }
    Ok(())
}

fn query_replay_record(
    transaction: &Transaction<'_>,
    source_id: &str,
    idempotency_key: &str,
) -> Result<Option<WebhookReplayRecord>, WebhookReplayStoreError> {
    let stored = transaction
        .query_row(
            "SELECT source_id, idempotency_key, body_sha256, submission_id,
                    admission_identity, external_event_id, state, outcome_ref,
                    first_seen_unix_seconds, expires_at_unix_seconds
             FROM webhook_replay_v1
             WHERE source_id = ?1 AND idempotency_key = ?2",
            params![source_id, idempotency_key],
            |row| {
                Ok(StoredReplayRecord {
                    source_id: row.get(0)?,
                    idempotency_key: row.get(1)?,
                    body_sha256: row.get(2)?,
                    submission_id: row.get(3)?,
                    admission_identity: row.get(4)?,
                    external_event_id: row.get(5)?,
                    state: row.get(6)?,
                    outcome_ref: row.get(7)?,
                    first_seen_unix_seconds: row.get(8)?,
                    expires_at_unix_seconds: row.get(9)?,
                })
            },
        )
        .optional()
        .map_err(|_| WebhookReplayStoreError::Unavailable)?;
    stored.map(TryInto::try_into).transpose()
}

struct StoredReplayRecord {
    source_id: String,
    idempotency_key: String,
    body_sha256: Vec<u8>,
    submission_id: String,
    admission_identity: String,
    external_event_id: String,
    state: String,
    outcome_ref: Option<String>,
    first_seen_unix_seconds: i64,
    expires_at_unix_seconds: i64,
}

impl TryFrom<StoredReplayRecord> for WebhookReplayRecord {
    type Error = WebhookReplayStoreError;

    fn try_from(stored: StoredReplayRecord) -> Result<Self, Self::Error> {
        let source_id = SourceId::new(stored.source_id).map_err(|_| Self::Error::Corrupt)?;
        validate_ascii(
            &stored.idempotency_key,
            "idempotency key",
            MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
            true,
        )
        .map_err(|_| Self::Error::Corrupt)?;
        let body_sha256: [u8; 32] = stored
            .body_sha256
            .try_into()
            .map_err(|_| Self::Error::Corrupt)?;
        let external_event_id =
            ExternalEventId::new(stored.external_event_id).map_err(|_| Self::Error::Corrupt)?;
        let state = match stored.state.as_str() {
            SQLITE_REPLAY_STATE_PENDING => WebhookReplayState::PendingAdmission,
            SQLITE_REPLAY_STATE_ACCEPTED => WebhookReplayState::Accepted,
            _ => return Err(Self::Error::Corrupt),
        };
        let outcome_ref = stored
            .outcome_ref
            .map(OutcomeRef::try_new)
            .transpose()
            .map_err(|_| Self::Error::Corrupt)?;
        if state == WebhookReplayState::PendingAdmission && outcome_ref.is_some() {
            return Err(Self::Error::Corrupt);
        }
        let first_seen_unix_seconds =
            u64::try_from(stored.first_seen_unix_seconds).map_err(|_| Self::Error::Corrupt)?;
        let expires_at_unix_seconds =
            u64::try_from(stored.expires_at_unix_seconds).map_err(|_| Self::Error::Corrupt)?;
        if expires_at_unix_seconds <= first_seen_unix_seconds {
            return Err(Self::Error::Corrupt);
        }
        let submission_id = SubmissionId::from_stored(stored.submission_id)?;
        let admission_identity = AdmissionIdentity::from_stored(stored.admission_identity)?;
        let (expected_submission_id, expected_admission_identity) =
            deterministic_identities(&source_id, &stored.idempotency_key, first_seen_unix_seconds);
        if submission_id != expected_submission_id
            || admission_identity != expected_admission_identity
        {
            return Err(Self::Error::Corrupt);
        }
        Ok(Self {
            source_key: SourceEventKey::new(source_id.clone(), external_event_id),
            source_id,
            idempotency_key: stored.idempotency_key,
            body_sha256,
            submission_id,
            admission_identity,
            state,
            outcome_ref,
            first_seen_unix_seconds,
            expires_at_unix_seconds,
        })
    }
}

fn deterministic_identities(
    source_id: &SourceId,
    idempotency_key: &str,
    first_seen_unix_seconds: u64,
) -> (SubmissionId, AdmissionIdentity) {
    let first_seen = first_seen_unix_seconds.to_string();
    let coordinate = format!(
        "{}:{}:{}:{}:{}:{first_seen}",
        source_id.as_str().len(),
        source_id.as_str(),
        idempotency_key.len(),
        idempotency_key,
        first_seen.len(),
    );
    (
        SubmissionId(format!("{SUBMISSION_ID_PREFIX}{coordinate}")),
        AdmissionIdentity(format!("{ADMISSION_ID_PREFIX}{coordinate}")),
    )
}

fn validate_direct_identity(value: &str, prefix: &str) -> Result<(), WebhookReplayStoreError> {
    let coordinate = value
        .strip_prefix(prefix)
        .ok_or(WebhookReplayStoreError::Corrupt)?;
    let (source_id, coordinate) = take_length_framed_identity_part(coordinate)?;
    let (idempotency_key, coordinate) = take_length_framed_identity_part(coordinate)?;
    let first_seen = take_final_length_framed_identity_part(coordinate)?;
    SourceId::new(source_id).map_err(|_| WebhookReplayStoreError::Corrupt)?;
    validate_ascii(
        idempotency_key,
        "idempotency key",
        MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES,
        true,
    )
    .map_err(|_| WebhookReplayStoreError::Corrupt)?;
    parse_canonical_unix_seconds(first_seen).ok_or(WebhookReplayStoreError::Corrupt)?;
    Ok(())
}

fn take_length_framed_identity_part(value: &str) -> Result<(&str, &str), WebhookReplayStoreError> {
    let (length, framed) = value
        .split_once(':')
        .ok_or(WebhookReplayStoreError::Corrupt)?;
    let length = parse_canonical_usize(length)?;
    if framed.len() <= length || !framed.is_char_boundary(length) {
        return Err(WebhookReplayStoreError::Corrupt);
    }
    let (part, remainder) = framed.split_at(length);
    let remainder = remainder
        .strip_prefix(':')
        .ok_or(WebhookReplayStoreError::Corrupt)?;
    Ok((part, remainder))
}

fn take_final_length_framed_identity_part(value: &str) -> Result<&str, WebhookReplayStoreError> {
    let (length, part) = value
        .split_once(':')
        .ok_or(WebhookReplayStoreError::Corrupt)?;
    if parse_canonical_usize(length)? != part.len() {
        return Err(WebhookReplayStoreError::Corrupt);
    }
    Ok(part)
}

fn parse_canonical_usize(value: &str) -> Result<usize, WebhookReplayStoreError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| WebhookReplayStoreError::Corrupt)?;
    if parsed.to_string() != value {
        return Err(WebhookReplayStoreError::Corrupt);
    }
    Ok(parsed)
}

fn canonical_material_from_digest(
    key_id: &str,
    unix_seconds: &str,
    idempotency_key: &str,
    body_sha256: &[u8; 32],
) -> Vec<u8> {
    format!(
        "{SIGNING_DOMAIN}\n{key_id}\n{unix_seconds}\n{WEBHOOK_METHOD}\n{WEBHOOK_PATH}\n{idempotency_key}\n{}",
        lower_hex(body_sha256)
    )
    .into_bytes()
}

fn parse_canonical_unix_seconds(value: &str) -> Option<u64> {
    if value == "0" {
        return Some(0);
    }
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn decode_signature(value: &str) -> Option<[u8; 32]> {
    let hex = value.strip_prefix("v1=")?;
    if hex.len() != 64 || !hex.bytes().all(is_lower_hex) {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = decode_nibble(pair[0])? << 4 | decode_nibble(pair[1])?;
    }
    Some(decoded)
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn sha256(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

fn lower_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn is_application_json(value: &str) -> bool {
    value.eq_ignore_ascii_case("application/json")
}

fn normalize_optional_header(
    value: Option<String>,
    field: &'static str,
    maximum: usize,
) -> Result<Option<String>, WebhookRequestBuildError> {
    value
        .map(|value| {
            let trimmed = value.trim_matches([' ', '\t']).to_string();
            validate_ascii(&trimmed, field, maximum, false)?;
            Ok(trimmed)
        })
        .transpose()
}

fn validate_optional_ascii(
    value: &Option<String>,
    field: &'static str,
    maximum: usize,
    non_empty: bool,
) -> Result<(), WebhookRequestBuildError> {
    if let Some(value) = value {
        validate_ascii(value, field, maximum, non_empty)?;
    }
    Ok(())
}

fn validate_ascii(
    value: &str,
    field: &'static str,
    maximum: usize,
    non_empty: bool,
) -> Result<(), WebhookRequestBuildError> {
    if value.len() > maximum {
        return Err(WebhookRequestBuildError::FieldTooLarge { field, maximum });
    }
    if (non_empty && value.is_empty()) || !value.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
        return Err(WebhookRequestBuildError::NonAscii { field });
    }
    Ok(())
}

fn to_sqlite_integer(value: u64) -> Result<i64, WebhookReplayStoreError> {
    i64::try_from(value).map_err(|_| WebhookReplayStoreError::Corrupt)
}
