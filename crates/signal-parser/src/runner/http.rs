#![cfg(feature = "provider-http")]

use std::sync::Arc;

use axum::body::to_bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

use crate::adapters::webhook::{
    AdmissionError, AdmissionSubmission, AuthenticatedWebhookAdapter,
    DurableAdmissionAcknowledgement, OutcomeRef, SourceEventAdmissionPort,
    WEBHOOK_RESPONSE_SCHEMA_VERSION, WebhookErrorCode, WebhookErrorResponseV1, WebhookHttpResponse,
    WebhookProfile, WebhookReplayStore, WebhookRequestParts, WebhookResponseBody,
};
use crate::runner::service::{
    AdmissionIdentity, AuthenticatedSourceContext, IngestionService, LogicalIngestionServiceError,
    SourceSubmission,
};

pub type TrustedClock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone)]
struct WebhookHttpState {
    adapter: Arc<AuthenticatedWebhookAdapter>,
    trusted_clock: TrustedClock,
}

/// Bridges authenticated webhook admission to the provider-neutral logical service.
pub struct LogicalWebhookAdmissionPort {
    service: Arc<dyn IngestionService>,
}

impl LogicalWebhookAdmissionPort {
    pub fn new(service: Arc<dyn IngestionService>) -> Self {
        Self { service }
    }
}

impl SourceEventAdmissionPort for LogicalWebhookAdmissionPort {
    fn admit(
        &self,
        submission: AdmissionSubmission,
    ) -> Result<DurableAdmissionAcknowledgement, AdmissionError> {
        let admission_identity = AdmissionIdentity::try_new(submission.identity.as_str())
            .map_err(|_| AdmissionError::Unavailable)?;
        let authenticated_context = AuthenticatedSourceContext::try_new(
            submission.event.key().source().clone(),
            "webhook-hmac",
            None,
        )
        .map_err(|_| AdmissionError::Unavailable)?;
        let response = futures::executor::block_on(self.service.submit(SourceSubmission {
            admission_identity,
            delivery_identity: submission.delivery_identity,
            authenticated_context,
            event: submission.event,
        }))
        .map_err(map_admission_error)?;
        let outcome_ref = OutcomeRef::try_new(response.outcome_reference.as_str())
            .map_err(|_| AdmissionError::Unavailable)?;
        Ok(DurableAdmissionAcknowledgement {
            outcome_ref: Some(outcome_ref),
        })
    }
}

/// Hosts an authenticated webhook through the logical ingestion service.
pub fn webhook_router(
    profile: WebhookProfile,
    replay_store: Arc<dyn WebhookReplayStore>,
    service: Arc<dyn IngestionService>,
    trusted_clock: TrustedClock,
) -> Router {
    let adapter = Arc::new(AuthenticatedWebhookAdapter::new(
        profile,
        replay_store,
        Arc::new(LogicalWebhookAdmissionPort::new(service)),
    ));
    webhook_router_with_adapter(adapter, trusted_clock)
}

/// Hosts a pre-built adapter for provider-edge integration tests.
pub fn webhook_router_with_adapter(
    adapter: Arc<AuthenticatedWebhookAdapter>,
    trusted_clock: TrustedClock,
) -> Router {
    Router::new()
        .route("/v1/source-events", post(handle_source_event))
        .with_state(WebhookHttpState {
            adapter,
            trusted_clock,
        })
}

fn map_admission_error(error: LogicalIngestionServiceError) -> AdmissionError {
    match error {
        LogicalIngestionServiceError::RetryRequired => AdmissionError::TooManyRequests,
        LogicalIngestionServiceError::SourceMismatch
        | LogicalIngestionServiceError::Unavailable
        | LogicalIngestionServiceError::InvalidOutcomeReference(_) => AdmissionError::Unavailable,
    }
}

async fn handle_source_event(
    State(state): State<WebhookHttpState>,
    request: Request<axum::body::Body>,
) -> Response {
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, state.adapter.maximum_body_bytes() + 1).await {
        Ok(body) => body.to_vec(),
        Err(_) => return content_too_large_response(),
    };
    let response = state.adapter.handle_parts(
        WebhookRequestParts {
            method: parts.method.as_str().to_string(),
            path: parts.uri.path().to_string(),
            query: parts.uri.query().map(str::to_string),
            content_type: header_value(&parts.headers, "content-type"),
            content_encoding: header_value(&parts.headers, "content-encoding"),
            key_id: header_value(&parts.headers, "x-webhook-key-id"),
            timestamp: header_value(&parts.headers, "x-webhook-timestamp"),
            idempotency_key: header_value(&parts.headers, "idempotency-key"),
            signature: header_value(&parts.headers, "x-webhook-signature"),
            body,
        },
        (state.trusted_clock)(),
    );
    webhook_response(response)
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn webhook_response(response: WebhookHttpResponse) -> Response {
    let status = StatusCode::from_u16(response.status.as_u16())
        .expect("webhook response statuses are valid HTTP status codes");
    match response.body {
        WebhookResponseBody::Submission(body) => (status, Json(body)).into_response(),
        WebhookResponseBody::Error(body) => (status, Json(body)).into_response(),
    }
}

fn content_too_large_response() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(WebhookErrorResponseV1 {
            schema_version: WEBHOOK_RESPONSE_SCHEMA_VERSION,
            code: WebhookErrorCode::ContentTooLarge,
        }),
    )
        .into_response()
}
