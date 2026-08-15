#![cfg(feature = "provider-http")]

use std::num::NonZeroUsize;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use signal_parser::adapters::webhook::{
    MemoryWebhookReplayStore, WebhookKeyBinding, WebhookProfile, sign_webhook_v1,
};
use signal_parser::ingestion::{
    DateTimeUtc, ExternalEventId, SourceEvent, SourceEventKey, SourceId, SourceOperation,
    SourcePayload, SourceRevision, SourceTimestamp, SourceTimestampQuality,
};
use signal_parser::runner::{
    IngestionServiceConfig,
    runtime::AdmissionRuntime,
    service::{DurableIngestionService, IngestionService},
    structured_jsonl_service,
};
use signal_parser::state::{MemorySourceStateStore, SourceStateStore};
use tower::Service;

const NOW: u64 = 1_786_200_000;
const SOURCE: &str = "webhook:http-binding";
const KEY_ID: &str = "http-binding-key";
const SECRET: &[u8] = b"http-binding-secret-0123456789abcdef";

fn source_event() -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new(SOURCE).unwrap(),
            ExternalEventId::new("event-1").unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            DateTimeUtc::parse("2026-08-08T12:00:00Z").unwrap(),
            SourceTimestampQuality::SourceProvided,
        ),
        DateTimeUtc::parse("2026-08-08T12:00:01Z").unwrap(),
        SourcePayload::Empty,
    )
}

#[tokio::test]
async fn hosted_binding_durably_deduplicates_admission_identity_before_accepting_retry() {
    let profile = WebhookProfile::try_new(
        65_536,
        60,
        86_400,
        vec![
            WebhookKeyBinding::try_new(KEY_ID, SourceId::new(SOURCE).unwrap(), SECRET.to_vec())
                .unwrap(),
        ],
    )
    .unwrap();
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let runner = Arc::new(
        structured_jsonl_service(
            SourceId::new(SOURCE).unwrap(),
            state.clone(),
            IngestionServiceConfig::default(),
        )
        .unwrap(),
    );
    let service: Arc<dyn IngestionService> = Arc::new(AdmissionRuntime::new(
        Arc::new(DurableIngestionService::new(runner, state.clone())),
        NonZeroUsize::new(1).unwrap(),
    ));
    let request_body = serde_json::to_vec(&source_event()).unwrap();
    let timestamp = NOW.to_string();
    let coordinate = format!(
        "{}:{SOURCE}:{}:delivery-1:{}:{timestamp}",
        SOURCE.len(),
        "delivery-1".len(),
        timestamp.len(),
    );
    let expected_submission_id = format!("webhook-submission:v2:{coordinate}");
    let expected_admission_identity = format!("admission:v2:{coordinate}");
    let signature =
        sign_webhook_v1(SECRET, KEY_ID, &timestamp, "delivery-1", &request_body).unwrap();
    let mut app = signal_parser::runner::http::webhook_router(
        profile,
        Arc::new(MemoryWebhookReplayStore::new()),
        service,
        Arc::new(|| NOW),
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/source-events")
        .header("content-type", "application/json")
        .header("x-webhook-key-id", KEY_ID)
        .header("x-webhook-timestamp", &timestamp)
        .header("idempotency-key", "delivery-1")
        .header("x-webhook-signature", signature)
        .body(Body::from(request_body.clone()))
        .unwrap();
    let response = app.call(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json"
    );
    let response_body = to_bytes(response.into_body(), 65_536).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["source_key"]["source"], SOURCE);
    assert_eq!(body["source_key"]["external_id"], "event-1");
    assert_eq!(body["disposition"], "accepted");
    assert!(
        body["outcome_ref"]
            .as_str()
            .is_some_and(|value| value.starts_with("admission:"))
    );
    assert_eq!(body["submission_id"], expected_submission_id);

    let retry = Request::builder()
        .method("POST")
        .uri("/v1/source-events")
        .header("content-type", "application/json")
        .header("x-webhook-key-id", KEY_ID)
        .header("x-webhook-timestamp", &timestamp)
        .header("idempotency-key", "delivery-1")
        .header(
            "x-webhook-signature",
            sign_webhook_v1(SECRET, KEY_ID, &timestamp, "delivery-1", &request_body).unwrap(),
        )
        .body(Body::from(request_body))
        .unwrap();
    let retry = app.call(retry).await.unwrap();
    assert_eq!(retry.status(), StatusCode::OK);
    assert_eq!(state.recorded_receipts().unwrap().len(), 1);
    assert!(matches!(
        state.recorded_receipts().unwrap()[0].delivery_identity,
        signal_parser::state::DurableDeliveryIdentity::Stable(ref identity)
            if identity == &expected_admission_identity
    ));
}
