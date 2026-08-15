use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;
use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use sha2::Sha256;

use signal_parser::adapters::structured_json::{
    SourceEventJsonlArtifactIdentity, decode_source_event_jsonl,
};
use signal_parser::adapters::webhook::{
    AdmissionError, AdmissionIdentity, AdmissionSubmission, AuthenticatedWebhookAdapter,
    DurableAdmissionAcknowledgement, HttpSourceSubmissionResponseV1, MAX_WEBHOOK_BODY_BYTES,
    MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES, MAX_WEBHOOK_KEY_ID_BYTES, MAX_WEBHOOK_SIGNATURE_BYTES,
    MAX_WEBHOOK_TIMESTAMP_BYTES, MemoryWebhookReplayStore, OutcomeRef, SourceEventAdmissionPort,
    SqliteWebhookReplayStore, SubmissionId, WebhookErrorCode, WebhookHttpResponse,
    WebhookHttpStatus, WebhookKeyBinding, WebhookProfile, WebhookProfileError,
    WebhookReplayReservation, WebhookReplayReservationResult, WebhookReplayStore,
    WebhookReplayStoreError, WebhookRequest, WebhookRequestParts, WebhookResponseBody,
    WebhookSigningError, sign_webhook_v1, webhook_signing_material,
};
use signal_parser::ingestion::{
    DateTimeUtc, ExternalEventId, SourceEvent, SourceEventKey, SourceId, SourceOperation,
    SourcePayload, SourceRevision, SourceTimestamp, SourceTimestampQuality,
};
use signal_parser::state::DurableDeliveryIdentity;

const NOW: u64 = 1_786_200_000;
const SOURCE_A: &str = "webhook:synthetic-a";
const SOURCE_B: &str = "webhook:synthetic-b";
const KEY_A: &str = "synthetic-primary";
const KEY_A_ROTATED: &str = "synthetic-rotated";
const KEY_B: &str = "synthetic-secondary";
const SECRET_A: &[u8] = b"synthetic-secret-a-0123456789abcde";
const SECRET_B: &[u8] = b"synthetic-secret-b-0123456789abcde";
const KNOWN_SECRET: &[u8] = b"known-secret-0123456789abcdefghi";

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn event(source: &str, external_id: &str) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new(source).unwrap(),
            ExternalEventId::new(external_id).unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-08T12:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-08T12:00:01Z"),
        SourcePayload::Empty,
    )
}

fn body(source: &str, external_id: &str) -> Vec<u8> {
    serde_json::to_vec(&event(source, external_id)).unwrap()
}

fn binding(key_id: &str, source: &str, secret: &[u8]) -> WebhookKeyBinding {
    WebhookKeyBinding::try_new(key_id, SourceId::new(source).unwrap(), secret.to_vec()).unwrap()
}

fn profile(bindings: Vec<WebhookKeyBinding>) -> WebhookProfile {
    profile_with_body_limit(65_536, bindings)
}

fn profile_with_body_limit(
    maximum_body_bytes: usize,
    bindings: Vec<WebhookKeyBinding>,
) -> WebhookProfile {
    WebhookProfile::try_new(maximum_body_bytes, 60, 86_400, bindings).unwrap()
}

fn request(
    key_id: &str,
    secret: &[u8],
    unix_seconds: u64,
    idempotency_key: &str,
    body: Vec<u8>,
) -> WebhookRequestParts {
    let timestamp = unix_seconds.to_string();
    let signature = sign_webhook_v1(secret, key_id, &timestamp, idempotency_key, &body).unwrap();
    WebhookRequestParts {
        method: "POST".to_string(),
        path: "/v1/source-events".to_string(),
        query: None,
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        key_id: Some(key_id.to_string()),
        timestamp: Some(timestamp),
        idempotency_key: Some(idempotency_key.to_string()),
        signature: Some(signature),
        body,
    }
}

fn submission(response: WebhookHttpResponse) -> HttpSourceSubmissionResponseV1 {
    match response.body {
        WebhookResponseBody::Submission(body) => body,
        WebhookResponseBody::Error(error) => panic!("expected submission, got {error:?}"),
    }
}

fn error_code(response: WebhookHttpResponse) -> WebhookErrorCode {
    match response.body {
        WebhookResponseBody::Error(error) => error.code,
        WebhookResponseBody::Submission(body) => {
            panic!("expected error, got submission {body:?}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdmissionCall {
    identity: AdmissionIdentity,
    delivery_identity: DurableDeliveryIdentity,
    submission_id: SubmissionId,
    source_key: SourceEventKey,
    event: SourceEvent,
}

#[derive(Debug, Default)]
struct ScriptedAdmissionPort {
    outcomes: Mutex<VecDeque<Result<DurableAdmissionAcknowledgement, AdmissionError>>>,
    calls: Mutex<Vec<AdmissionCall>>,
}

impl ScriptedAdmissionPort {
    fn new(outcomes: Vec<Result<DurableAdmissionAcknowledgement, AdmissionError>>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<AdmissionCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl SourceEventAdmissionPort for ScriptedAdmissionPort {
    fn admit(
        &self,
        submission: AdmissionSubmission,
    ) -> Result<DurableAdmissionAcknowledgement, AdmissionError> {
        let AdmissionSubmission {
            identity,
            delivery_identity,
            submission_id,
            event,
        } = submission;
        self.calls.lock().unwrap().push(AdmissionCall {
            identity,
            delivery_identity,
            submission_id,
            source_key: event.key().clone(),
            event,
        });
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(DurableAdmissionAcknowledgement::default()))
    }
}

fn adapter(
    profile: WebhookProfile,
    replay: Arc<dyn WebhookReplayStore>,
    admission: Arc<ScriptedAdmissionPort>,
) -> AuthenticatedWebhookAdapter {
    AuthenticatedWebhookAdapter::new(profile, replay, admission)
}

fn memory_adapter(
    profile: WebhookProfile,
    admission: Arc<ScriptedAdmissionPort>,
) -> AuthenticatedWebhookAdapter {
    adapter(
        profile,
        Arc::new(MemoryWebhookReplayStore::new()),
        admission,
    )
}

fn hmac_hex(secret: &[u8], material: &[u8]) -> String {
    let mut signer = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    signer.update(material);
    signer
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn replay_reservation(
    idempotency_key: &str,
    now: u64,
    expires_at: u64,
) -> WebhookReplayReservation {
    WebhookReplayReservation {
        source_id: SourceId::new(SOURCE_A).unwrap(),
        idempotency_key: idempotency_key.to_string(),
        body_sha256: [idempotency_key.len() as u8; 32],
        source_key: SourceEventKey::new(
            SourceId::new(SOURCE_A).unwrap(),
            ExternalEventId::new(format!("event-{idempotency_key}")).unwrap(),
        ),
        now_unix_seconds: now,
        expires_at_unix_seconds: expires_at,
    }
}

fn assert_replay_store_purges_and_bounds(store: &dyn WebhookReplayStore) {
    for key in ["expired-one", "expired-two"] {
        assert!(matches!(
            store.reserve(replay_reservation(key, NOW - 10, NOW)),
            Ok(WebhookReplayReservationResult::Reserved(_))
        ));
    }
    assert!(matches!(
        store.reserve(replay_reservation("fresh-one", NOW, NOW + 10)),
        Ok(WebhookReplayReservationResult::Reserved(_))
    ));
    assert!(matches!(
        store.reserve(replay_reservation("fresh-two", NOW, NOW + 10)),
        Ok(WebhookReplayReservationResult::Reserved(_))
    ));
    assert_eq!(
        store.reserve(replay_reservation("over-capacity", NOW, NOW + 10)),
        Err(WebhookReplayStoreError::CapacityExceeded)
    );
}

#[test]
fn canonical_material_and_known_hmac_signature_are_exact() {
    let raw_body = br#"{"schema_version":1}"#;
    let material =
        webhook_signing_material("demo-primary", "1786200000", "delivery-1", raw_body).unwrap();
    let expected = concat!(
        "quant-system-webhook-v1\n",
        "demo-primary\n",
        "1786200000\n",
        "POST\n",
        "/v1/source-events\n",
        "delivery-1\n",
        "a9d5f6d002d956b8af5787a05e0ca000d45c03977ffa54ee8fbed719fed5fd23"
    );
    assert_eq!(material, expected.as_bytes());
    assert!(!material.ends_with(b"\n"));
    assert_eq!(material.split(|byte| *byte == b'\n').count(), 7);
    assert_eq!(
        sign_webhook_v1(
            KNOWN_SECRET,
            "demo-primary",
            "1786200000",
            "delivery-1",
            raw_body,
        )
        .unwrap(),
        "v1=63fc19917157accbd1f652738895b8fe1306c8a80dae045bfe388c61d63c01ca"
    );

    let original = sign_webhook_v1(
        KNOWN_SECRET,
        "demo-primary",
        "1786200000",
        "delivery-1",
        raw_body,
    )
    .unwrap();
    assert_ne!(
        original,
        sign_webhook_v1(
            KNOWN_SECRET,
            "demo-secondary",
            "1786200000",
            "delivery-1",
            raw_body,
        )
        .unwrap()
    );
    assert_ne!(
        original,
        sign_webhook_v1(
            KNOWN_SECRET,
            "demo-primary",
            "1786200001",
            "delivery-1",
            raw_body,
        )
        .unwrap()
    );
    assert_ne!(
        original,
        sign_webhook_v1(
            KNOWN_SECRET,
            "demo-primary",
            "1786200000",
            "delivery-2",
            raw_body,
        )
        .unwrap()
    );
    assert_ne!(
        original,
        sign_webhook_v1(
            KNOWN_SECRET,
            "demo-primary",
            "1786200000",
            "delivery-1",
            br#"{"schema_version":1} "#,
        )
        .unwrap()
    );

    let changed_path_material = expected.replace("/v1/source-events", "/v1/source-events/");
    assert_ne!(
        hmac_hex(KNOWN_SECRET, expected.as_bytes()),
        hmac_hex(KNOWN_SECRET, changed_path_material.as_bytes())
    );
}

#[test]
fn signing_and_bindings_reject_short_hmac_secrets() {
    let short_secret = [0_u8; 31];
    assert!(matches!(
        WebhookKeyBinding::try_new(KEY_A, SourceId::new(SOURCE_A).unwrap(), short_secret),
        Err(WebhookProfileError::InvalidSecretLength)
    ));
    assert_eq!(
        sign_webhook_v1(
            &short_secret,
            KEY_A,
            &NOW.to_string(),
            "short-secret",
            b"{}"
        ),
        Err(WebhookSigningError::InvalidSecret)
    );
}

#[test]
fn request_debug_output_redacts_signature_and_raw_body() {
    let parts = request(
        KEY_A,
        SECRET_A,
        NOW,
        "redacted-debug",
        body(SOURCE_A, "sensitive-event"),
    );
    let signature = parts.signature.clone().unwrap();
    let parts_debug = format!("{parts:?}");
    assert!(parts_debug.contains("[REDACTED]"));
    assert!(parts_debug.contains("body_bytes"));
    assert!(!parts_debug.contains(&signature));
    assert!(!parts_debug.contains("sensitive-event"));

    let request = WebhookRequest::try_new(parts).unwrap();
    let request_debug = format!("{request:?}");
    assert!(request_debug.contains("[REDACTED]"));
    assert!(request_debug.contains("body_bytes"));
    assert!(!request_debug.contains(&signature));
    assert!(!request_debug.contains("sensitive-event"));
}

#[test]
fn authentication_precedes_strict_decode_and_canonical_mutations_fail() {
    let admission = Arc::new(ScriptedAdmissionPort::default());
    let adapter = memory_adapter(
        profile(vec![
            binding(KEY_A, SOURCE_A, SECRET_A),
            binding(KEY_A_ROTATED, SOURCE_A, SECRET_A),
        ]),
        admission.clone(),
    );
    let valid_body = body(SOURCE_A, "canonical-event");
    let original = request(
        KEY_A,
        SECRET_A,
        NOW,
        "canonical-delivery",
        valid_body.clone(),
    );

    let mut changed_body = original.clone();
    changed_body.body.push(b' ');
    let response = adapter.handle_parts(changed_body, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let mut changed_timestamp = original.clone();
    changed_timestamp.timestamp = Some((NOW + 1).to_string());
    let response = adapter.handle_parts(changed_timestamp, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let mut changed_key_id = original.clone();
    changed_key_id.key_id = Some(KEY_A_ROTATED.to_string());
    let response = adapter.handle_parts(changed_key_id, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let mut changed_idempotency_key = original.clone();
    changed_idempotency_key.idempotency_key = Some("canonical-delivery-2".to_string());
    let response = adapter.handle_parts(changed_idempotency_key, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let mut changed_path = original;
    changed_path.path.push('/');
    let response = adapter.handle_parts(changed_path, NOW);
    assert_eq!(response.status, WebhookHttpStatus::BadRequest);

    let malformed = br#"{"schema_version":1,"not":"a source event"}"#.to_vec();
    let mut invalid_signature = request(KEY_A, SECRET_A, NOW, "auth-order", malformed.clone());
    invalid_signature.signature = Some(format!("v1={}", "0".repeat(64)));
    let response = adapter.handle_parts(invalid_signature, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);
    assert_eq!(error_code(response), WebhookErrorCode::Unauthorized);

    let mut uppercase_signature = request(KEY_A, SECRET_A, NOW, "auth-order", malformed.clone());
    let signature = uppercase_signature.signature.take().unwrap();
    uppercase_signature.signature = Some(format!("v1={}", signature[3..].to_uppercase()));
    let response = adapter.handle_parts(uppercase_signature, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let mut malformed_signature = request(KEY_A, SECRET_A, NOW, "auth-order", malformed.clone());
    malformed_signature.signature = Some("v1=abc".to_string());
    let response = adapter.handle_parts(malformed_signature, NOW);
    assert_eq!(response.status, WebhookHttpStatus::Unauthorized);

    let response =
        adapter.handle_parts(request(KEY_A, SECRET_A, NOW, "auth-order", malformed), NOW);
    assert_eq!(response.status, WebhookHttpStatus::BadRequest);
    assert!(admission.calls().is_empty());
}

#[test]
fn request_profile_enforces_route_media_headers_clock_and_bounds() {
    let admission = Arc::new(ScriptedAdmissionPort::default());
    let adapter = memory_adapter(
        profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
        admission.clone(),
    );

    let mut wrong_method = request(KEY_A, SECRET_A, NOW, "wrong-method", body(SOURCE_A, "1"));
    wrong_method.method = "PUT".to_string();
    assert_eq!(
        adapter.handle_parts(wrong_method, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut query = request(KEY_A, SECRET_A, NOW, "query", body(SOURCE_A, "2"));
    query.query = Some(String::new());
    assert_eq!(
        adapter.handle_parts(query, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut compressed = request(KEY_A, SECRET_A, NOW, "compressed", body(SOURCE_A, "3"));
    compressed.content_encoding = Some("gzip".to_string());
    assert_eq!(
        adapter.handle_parts(compressed, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut parameterized = request(KEY_A, SECRET_A, NOW, "media-param", body(SOURCE_A, "4"));
    parameterized.content_type = Some("application/json; charset=utf-8".to_string());
    assert_eq!(
        adapter.handle_parts(parameterized, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut alternate_media = request(KEY_A, SECRET_A, NOW, "wrong-media", body(SOURCE_A, "5"));
    alternate_media.content_type = Some("application/problem+json".to_string());
    assert_eq!(
        adapter.handle_parts(alternate_media, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut case_insensitive_media =
        request(KEY_A, SECRET_A, NOW, "media-case", body(SOURCE_A, "6"));
    case_insensitive_media.content_type = Some("APPLICATION/JSON".to_string());
    assert_eq!(
        adapter.handle_parts(case_insensitive_media, NOW).status,
        WebhookHttpStatus::Accepted
    );

    let unknown_key = request(
        "unknown-key",
        b"unknown-secret-0123456789abcdefghi",
        NOW,
        "unknown-key",
        body(SOURCE_A, "7"),
    );
    assert_eq!(
        adapter.handle_parts(unknown_key, NOW).status,
        WebhookHttpStatus::Unauthorized
    );

    let expired = request(KEY_A, SECRET_A, NOW - 61, "expired", body(SOURCE_A, "8"));
    assert_eq!(
        adapter.handle_parts(expired, NOW).status,
        WebhookHttpStatus::Unauthorized
    );

    let future = request(KEY_A, SECRET_A, NOW + 61, "future", body(SOURCE_A, "9"));
    assert_eq!(
        adapter.handle_parts(future, NOW).status,
        WebhookHttpStatus::Unauthorized
    );

    let mut noncanonical_timestamp =
        request(KEY_A, SECRET_A, NOW, "timestamp", body(SOURCE_A, "10"));
    noncanonical_timestamp.timestamp = Some(format!("0{NOW}"));
    assert_eq!(
        adapter.handle_parts(noncanonical_timestamp, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut missing_key = request(KEY_A, SECRET_A, NOW, "missing-key", body(SOURCE_A, "11"));
    missing_key.key_id = None;
    assert_eq!(
        adapter.handle_parts(missing_key, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut missing_timestamp = request(
        KEY_A,
        SECRET_A,
        NOW,
        "missing-timestamp",
        body(SOURCE_A, "12"),
    );
    missing_timestamp.timestamp = None;
    assert_eq!(
        adapter.handle_parts(missing_timestamp, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut missing_idempotency = request(
        KEY_A,
        SECRET_A,
        NOW,
        "missing-idempotency",
        body(SOURCE_A, "13"),
    );
    missing_idempotency.idempotency_key = None;
    assert_eq!(
        adapter.handle_parts(missing_idempotency, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut missing_signature = request(
        KEY_A,
        SECRET_A,
        NOW,
        "missing-signature",
        body(SOURCE_A, "14"),
    );
    missing_signature.signature = None;
    assert_eq!(
        adapter.handle_parts(missing_signature, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut oversized_key = request(KEY_A, SECRET_A, NOW, "key-bound", body(SOURCE_A, "15"));
    oversized_key.key_id = Some("k".repeat(MAX_WEBHOOK_KEY_ID_BYTES + 1));
    assert_eq!(
        adapter.handle_parts(oversized_key, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut oversized_timestamp = request(
        KEY_A,
        SECRET_A,
        NOW,
        "timestamp-bound",
        body(SOURCE_A, "16"),
    );
    oversized_timestamp.timestamp = Some("1".repeat(MAX_WEBHOOK_TIMESTAMP_BYTES + 1));
    assert_eq!(
        adapter.handle_parts(oversized_timestamp, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut oversized_idempotency = request(
        KEY_A,
        SECRET_A,
        NOW,
        "idempotency-bound",
        body(SOURCE_A, "17"),
    );
    oversized_idempotency.idempotency_key = Some("i".repeat(MAX_WEBHOOK_IDEMPOTENCY_KEY_BYTES + 1));
    assert_eq!(
        adapter.handle_parts(oversized_idempotency, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut oversized_signature = request(
        KEY_A,
        SECRET_A,
        NOW,
        "signature-bound",
        body(SOURCE_A, "18"),
    );
    oversized_signature.signature = Some("s".repeat(MAX_WEBHOOK_SIGNATURE_BYTES + 1));
    assert_eq!(
        adapter.handle_parts(oversized_signature, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let mut non_ascii_header = request(KEY_A, SECRET_A, NOW, "ascii-bound", body(SOURCE_A, "19"));
    non_ascii_header.idempotency_key = Some("delivery-Ä".to_string());
    assert_eq!(
        adapter.handle_parts(non_ascii_header, NOW).status,
        WebhookHttpStatus::BadRequest
    );

    let profile_limited_adapter = memory_adapter(
        profile_with_body_limit(1_024, vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
        Arc::new(ScriptedAdmissionPort::default()),
    );
    let profile_oversized = request(
        KEY_A,
        SECRET_A,
        NOW,
        "profile-body-bound",
        vec![b' '; 1_025],
    );
    let response = profile_limited_adapter.handle_parts(profile_oversized, NOW);
    assert_eq!(response.status, WebhookHttpStatus::ContentTooLarge);
    assert_eq!(error_code(response), WebhookErrorCode::ContentTooLarge);

    let absolute_oversized = WebhookRequestParts {
        method: "POST".to_string(),
        path: "/v1/source-events".to_string(),
        query: None,
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        key_id: Some(KEY_A.to_string()),
        timestamp: Some(NOW.to_string()),
        idempotency_key: Some("absolute-body-bound".to_string()),
        signature: None,
        body: vec![0; MAX_WEBHOOK_BODY_BYTES + 1],
    };
    assert_eq!(
        adapter.handle_parts(absolute_oversized, NOW).status,
        WebhookHttpStatus::ContentTooLarge
    );

    assert_eq!(admission.calls().len(), 1);
}

#[test]
fn malformed_and_source_mismatch_do_not_consume_replay_keys() {
    let admission = Arc::new(ScriptedAdmissionPort::default());
    let adapter = memory_adapter(
        profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
        admission.clone(),
    );

    let malformed = br#"{"schema_version":1}"#.to_vec();
    let response = adapter.handle_parts(
        request(KEY_A, SECRET_A, NOW, "reusable-malformed", malformed),
        NOW,
    );
    assert_eq!(response.status, WebhookHttpStatus::BadRequest);
    let response = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "reusable-malformed",
            body(SOURCE_A, "after-malformed"),
        ),
        NOW,
    );
    assert_eq!(response.status, WebhookHttpStatus::Accepted);

    let response = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "reusable-mismatch",
            body(SOURCE_B, "source-mismatch"),
        ),
        NOW,
    );
    assert_eq!(response.status, WebhookHttpStatus::Forbidden);
    assert_eq!(error_code(response), WebhookErrorCode::Forbidden);
    let response = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "reusable-mismatch",
            body(SOURCE_A, "after-mismatch"),
        ),
        NOW,
    );
    assert_eq!(response.status, WebhookHttpStatus::Accepted);
    assert_eq!(admission.calls().len(), 2);
}

#[test]
fn replay_identities_share_a_direct_length_framed_body_independent_coordinate() {
    let reservation = replay_reservation("identity:coordinate", NOW, NOW + 10);
    let first = match MemoryWebhookReplayStore::new().reserve(reservation.clone()) {
        Ok(WebhookReplayReservationResult::Reserved(record)) => record,
        result => panic!("expected reservation, got {result:?}"),
    };
    let mut changed_body = reservation;
    changed_body.body_sha256 = [0x7f; 32];
    let second = match MemoryWebhookReplayStore::new().reserve(changed_body) {
        Ok(WebhookReplayReservationResult::Reserved(record)) => record,
        result => panic!("expected reservation, got {result:?}"),
    };
    let coordinate = "19:webhook:synthetic-a:19:identity:coordinate:10:1786200000";

    assert_eq!(
        first.submission_id().as_str(),
        format!("webhook-submission:v2:{coordinate}")
    );
    assert_eq!(
        first.admission_identity().as_str(),
        format!("admission:v2:{coordinate}")
    );
    assert_eq!(first.submission_id(), second.submission_id());
    assert_eq!(first.admission_identity(), second.admission_identity());
    assert_ne!(first.body_sha256(), second.body_sha256());
}

#[test]
fn memory_replay_is_stable_conflicting_source_scoped_and_rotation_safe() {
    let admission = Arc::new(ScriptedAdmissionPort::default());
    let replay: Arc<dyn WebhookReplayStore> = Arc::new(MemoryWebhookReplayStore::new());
    let adapter = adapter(
        profile(vec![
            binding(KEY_A, SOURCE_A, SECRET_A),
            binding(KEY_A_ROTATED, SOURCE_A, SECRET_A),
            binding(KEY_B, SOURCE_B, SECRET_B),
        ]),
        replay,
        admission.clone(),
    );

    let raw_body = body(SOURCE_A, "stable-event");
    let first_response = adapter.handle_parts(
        request(KEY_A, SECRET_A, NOW, "stable-delivery", raw_body.clone()),
        NOW,
    );
    assert_eq!(first_response.status, WebhookHttpStatus::Accepted);
    let first = submission(first_response);

    let retry_response = adapter.handle_parts(
        request(
            KEY_A_ROTATED,
            SECRET_A,
            NOW + 1,
            "stable-delivery",
            raw_body,
        ),
        NOW + 1,
    );
    assert_eq!(retry_response.status, WebhookHttpStatus::Ok);
    let retry = submission(retry_response);
    assert_eq!(retry.submission_id, first.submission_id);
    assert_eq!(retry.source_key, first.source_key);
    assert_eq!(admission.calls().len(), 1);

    let conflict = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW + 2,
            "stable-delivery",
            body(SOURCE_A, "different-event"),
        ),
        NOW + 2,
    );
    assert_eq!(conflict.status, WebhookHttpStatus::Conflict);
    assert_eq!(error_code(conflict), WebhookErrorCode::ReplayConflict);
    assert_eq!(admission.calls().len(), 1);

    let source_a = submission(adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "shared-delivery",
            body(SOURCE_A, "scope-a"),
        ),
        NOW,
    ));
    let source_b_response = adapter.handle_parts(
        request(
            KEY_B,
            SECRET_B,
            NOW,
            "shared-delivery",
            body(SOURCE_B, "scope-b"),
        ),
        NOW,
    );
    assert_eq!(source_b_response.status, WebhookHttpStatus::Accepted);
    let source_b = submission(source_b_response);
    assert_ne!(source_a.submission_id, source_b.submission_id);
    assert_eq!(admission.calls().len(), 3);
}

#[test]
fn sqlite_accepted_replay_survives_restart_without_resubmission() {
    let database = TemporaryDatabase::new("webhook-accepted-restart");
    let raw_body = body(SOURCE_A, "accepted-restart");
    let first;
    {
        let admission = Arc::new(ScriptedAdmissionPort::new(vec![Ok(
            DurableAdmissionAcknowledgement {
                outcome_ref: Some(OutcomeRef::try_new("outcome:accepted-restart").unwrap()),
            },
        )]));
        let replay: Arc<dyn WebhookReplayStore> =
            Arc::new(SqliteWebhookReplayStore::open(database.path()).unwrap());
        let adapter = adapter(
            profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
            replay,
            admission.clone(),
        );
        let response = adapter.handle_parts(
            request(KEY_A, SECRET_A, NOW, "accepted-restart", raw_body.clone()),
            NOW,
        );
        assert_eq!(response.status, WebhookHttpStatus::Accepted);
        first = submission(response);
        assert_eq!(admission.calls().len(), 1);
    }
    {
        let replay_store = SqliteWebhookReplayStore::open(database.path()).unwrap();
        replay_store.quick_check().unwrap();
        let admission = Arc::new(ScriptedAdmissionPort::default());
        let adapter = adapter(
            profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
            Arc::new(replay_store),
            admission.clone(),
        );
        let response = adapter.handle_parts(
            request(KEY_A, SECRET_A, NOW + 1, "accepted-restart", raw_body),
            NOW + 1,
        );
        assert_eq!(response.status, WebhookHttpStatus::Ok);
        let retried = submission(response);
        assert_eq!(retried.submission_id, first.submission_id);
        assert_eq!(
            retried.outcome_ref.as_ref().map(OutcomeRef::as_str),
            Some("outcome:accepted-restart")
        );
        assert!(admission.calls().is_empty());
    }
}

#[test]
fn pending_restart_resubmits_same_stable_identity_and_waits_for_durable_ack() {
    let database = TemporaryDatabase::new("webhook-pending-restart");
    let raw_body = body(SOURCE_A, "pending-restart");
    let first_call;
    {
        let admission = Arc::new(ScriptedAdmissionPort::new(vec![Err(
            AdmissionError::Unavailable,
        )]));
        let adapter = adapter(
            profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
            Arc::new(SqliteWebhookReplayStore::open(database.path()).unwrap()),
            admission.clone(),
        );
        let response = adapter.handle_parts(
            request(KEY_A, SECRET_A, NOW, "pending-restart", raw_body.clone()),
            NOW,
        );
        assert_eq!(response.status, WebhookHttpStatus::ServiceUnavailable);
        assert_eq!(error_code(response), WebhookErrorCode::ServiceUnavailable);
        first_call = admission.calls().into_iter().next().unwrap();
        assert_eq!(
            first_call.delivery_identity,
            DurableDeliveryIdentity::Stable(first_call.identity.as_str().to_string())
        );
    }
    {
        let admission = Arc::new(ScriptedAdmissionPort::new(vec![Ok(
            DurableAdmissionAcknowledgement {
                outcome_ref: Some(OutcomeRef::try_new("outcome:pending-restart").unwrap()),
            },
        )]));
        let adapter = adapter(
            profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
            Arc::new(SqliteWebhookReplayStore::open(database.path()).unwrap()),
            admission.clone(),
        );
        let response = adapter.handle_parts(
            request(
                KEY_A,
                SECRET_A,
                NOW + 1,
                "pending-restart",
                raw_body.clone(),
            ),
            NOW + 1,
        );
        assert_eq!(response.status, WebhookHttpStatus::Accepted);
        let accepted = submission(response);
        let second_call = admission.calls().into_iter().next().unwrap();
        assert_eq!(second_call.identity, first_call.identity);
        assert_eq!(second_call.delivery_identity, first_call.delivery_identity);
        assert_eq!(second_call.submission_id, first_call.submission_id);
        assert_eq!(accepted.submission_id, first_call.submission_id);

        let response = adapter.handle_parts(
            request(KEY_A, SECRET_A, NOW + 2, "pending-restart", raw_body),
            NOW + 2,
        );
        assert_eq!(response.status, WebhookHttpStatus::Ok);
        assert_eq!(admission.calls().len(), 1);
    }
}

#[test]
fn admission_failures_map_to_bounded_provider_statuses() {
    let admission = Arc::new(ScriptedAdmissionPort::new(vec![
        Err(AdmissionError::TooManyRequests),
        Err(AdmissionError::Conflict),
        Err(AdmissionError::Unavailable),
    ]));
    let adapter = memory_adapter(profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]), admission);

    let too_many = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "admission-rate",
            body(SOURCE_A, "rate"),
        ),
        NOW,
    );
    assert_eq!(too_many.status, WebhookHttpStatus::TooManyRequests);
    assert_eq!(error_code(too_many), WebhookErrorCode::TooManyRequests);

    let conflict = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "admission-conflict",
            body(SOURCE_A, "conflict"),
        ),
        NOW,
    );
    assert_eq!(conflict.status, WebhookHttpStatus::Conflict);
    assert_eq!(error_code(conflict), WebhookErrorCode::ReplayConflict);

    let unavailable = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "admission-unavailable",
            body(SOURCE_A, "unavailable"),
        ),
        NOW,
    );
    assert_eq!(unavailable.status, WebhookHttpStatus::ServiceUnavailable);
    assert_eq!(
        error_code(unavailable),
        WebhookErrorCode::ServiceUnavailable
    );

    let statuses = [
        WebhookHttpStatus::Ok,
        WebhookHttpStatus::Accepted,
        WebhookHttpStatus::BadRequest,
        WebhookHttpStatus::Unauthorized,
        WebhookHttpStatus::Forbidden,
        WebhookHttpStatus::Conflict,
        WebhookHttpStatus::ContentTooLarge,
        WebhookHttpStatus::TooManyRequests,
        WebhookHttpStatus::ServiceUnavailable,
    ];
    assert_eq!(
        statuses.map(WebhookHttpStatus::as_u16),
        [200, 202, 400, 401, 403, 409, 413, 429, 503]
    );
}

#[test]
fn jsonl_and_webhook_admit_the_same_strict_source_event() {
    let source = event(SOURCE_A, "cross-adapter");
    let body = serde_json::to_vec(&source).unwrap();
    let mut artifact = body.clone();
    artifact.push(b'\n');
    let decoded = decode_source_event_jsonl(
        SourceEventJsonlArtifactIdentity::try_new("cross-adapter-source-run").unwrap(),
        &artifact,
    );
    let jsonl_record = decoded.records()[0].as_ref().unwrap();

    let admission = Arc::new(ScriptedAdmissionPort::default());
    let adapter = memory_adapter(
        profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]),
        admission.clone(),
    );
    let response = adapter.handle_parts(request(KEY_A, SECRET_A, NOW, "cross-adapter", body), NOW);
    assert_eq!(response.status, WebhookHttpStatus::Accepted);

    let calls = admission.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].event, *jsonl_record.event());
    assert_ne!(
        calls[0].delivery_identity,
        *jsonl_record.delivery_identity()
    );
}

#[test]
fn asynchronous_response_contains_no_candidate_or_envelope_projection() {
    let admission = Arc::new(ScriptedAdmissionPort::new(vec![Ok(
        DurableAdmissionAcknowledgement {
            outcome_ref: Some(OutcomeRef::try_new("outcome:lookup-only").unwrap()),
        },
    )]));
    let adapter = memory_adapter(profile(vec![binding(KEY_A, SOURCE_A, SECRET_A)]), admission);
    let response = adapter.handle_parts(
        request(
            KEY_A,
            SECRET_A,
            NOW,
            "projection",
            body(SOURCE_A, "projection"),
        ),
        NOW,
    );
    assert_eq!(response.status, WebhookHttpStatus::Accepted);
    let value = serde_json::to_value(submission(response)).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(object.len(), 5);
    assert_eq!(object["schema_version"], 1);
    assert_eq!(object["disposition"], "accepted");
    assert_eq!(object["outcome_ref"], "outcome:lookup-only");
    assert!(!object.contains_key("candidate"));
    assert!(!object.contains_key("candidates"));
    assert!(!object.contains_key("envelope"));
    assert!(!object.contains_key("envelopes"));
}

#[test]
fn replay_stores_purge_expired_records_and_enforce_capacity() {
    let maximum_records = NonZeroUsize::new(2).unwrap();
    let memory = MemoryWebhookReplayStore::with_maximum_records(maximum_records);
    assert_replay_store_purges_and_bounds(&memory);

    let sqlite =
        SqliteWebhookReplayStore::open_in_memory_with_maximum_records(maximum_records).unwrap();
    assert_replay_store_purges_and_bounds(&sqlite);
}

#[test]
fn sqlite_restore_recomputes_deterministic_ids() {
    let database = TemporaryDatabase::new("webhook-corrupt-identities");
    let reservation = replay_reservation("restore-corrupt", NOW, NOW + 10);
    {
        let store = SqliteWebhookReplayStore::open(database.path()).unwrap();
        assert!(matches!(
            store.reserve(reservation.clone()),
            Ok(WebhookReplayReservationResult::Reserved(_))
        ));
    }
    let connection = Connection::open(database.path()).unwrap();
    let wrong_first_seen = (NOW + 1).to_string();
    let mismatched_submission_id = format!(
        "webhook-submission:v2:{}:{SOURCE_A}:{}:restore-corrupt:{}:{wrong_first_seen}",
        SOURCE_A.len(),
        "restore-corrupt".len(),
        wrong_first_seen.len(),
    );
    connection
        .execute(
            "UPDATE webhook_replay_v1 SET submission_id = ?1",
            [mismatched_submission_id],
        )
        .unwrap();
    drop(connection);

    let store = SqliteWebhookReplayStore::open(database.path()).unwrap();
    assert_eq!(
        store.reserve(reservation),
        Err(WebhookReplayStoreError::Corrupt)
    );
}

static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(1);

struct TemporaryDatabase {
    path: PathBuf,
}

impl TemporaryDatabase {
    fn new(label: &str) -> Self {
        let sequence = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quant-system-{label}-{}-{sequence}.sqlite",
            std::process::id()
        ));
        remove_sqlite_files(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        remove_sqlite_files(&self.path);
    }
}

fn remove_sqlite_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let path_text = path.to_string_lossy();
    let _ = std::fs::remove_file(format!("{path_text}-shm"));
    let _ = std::fs::remove_file(format!("{path_text}-wal"));
}
