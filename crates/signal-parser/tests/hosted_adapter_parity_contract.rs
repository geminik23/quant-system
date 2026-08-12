#![cfg(feature = "online")]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use chrono::NaiveDateTime;
use signal_parser::adapters::telegram::{
    TelegramAdaptationOutcome, TelegramAdapterConfig, TelegramRelayInput, TelegramRelayOperation,
    TelegramRelaySourceAdapter, bind_legacy_telegram_producer,
};
use signal_parser::adapters::webhook::{
    MemoryWebhookReplayStore, WebhookKeyBinding, WebhookProfile, sign_webhook_v1,
};
use signal_parser::ingestion::{DateTimeUtc, SourceEvent, SourceId, TextFormat};
use signal_parser::normalization::{
    CompiledPipeline, CompiledRoutingGraph, PayloadKind, PipelineId, RouteSelector, RouteSpec,
    SemanticVersion, Sha256Digest,
};
use signal_parser::runner::{
    IngestionService as RunnerIngestionService, IngestionServiceConfig,
    runtime::AdmissionRuntime,
    service::{DurableIngestionService, IngestionService as _, SourceSubmissionDisposition},
    telegram::{TelegramIngestionOutcome, TelegramRelayIngestionBinding},
};
use signal_parser::state::{
    CommittedNormalizationOutcome, DurableDeliveryIdentity, MemorySourceStateStore,
    SourceStateStore,
};
use signal_parser::{ChannelParser, ParseContext, ParsedAction, ParserRegistry, RawSignal};
use tower::Service;

const NOW: u64 = 1_786_200_000;
const SOURCE: &str = "telegram:hosted-adapter-parity";
const KEY_ID: &str = "hosted-adapter-parity";
const SECRET: &[u8] = b"hosted-adapter-parity-secret-012345";
const CHAT_ID: i64 = -100_246;
const MESSAGE_ID: i64 = 73;
const DELIVERY_ID: &str = "telegram-delivery-73";

struct CloseAllParser {
    channels: [i64; 1],
}

impl ChannelParser for CloseAllParser {
    fn name(&self) -> &str {
        "hosted-adapter-parity"
    }

    fn channel_ids(&self) -> &[i64] {
        &self.channels
    }

    fn max_history(&self) -> usize {
        0
    }

    fn parse_root(
        &self,
        message: &str,
        ts: NaiveDateTime,
        _context: &ParseContext,
    ) -> ParsedAction {
        assert_eq!(message, "close all");
        ParsedAction::one(RawSignal::CloseAll { ts })
    }

    fn parse_reply(
        &self,
        _message: &str,
        _ts: NaiveDateTime,
        _parent: Option<&signal_parser::RawTgMessage>,
        _context: &ParseContext,
    ) -> ParsedAction {
        panic!("the parity event is a root message")
    }
}

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn relay_input() -> TelegramRelayInput {
    TelegramRelayInput::try_new_message(
        TelegramRelayOperation::New,
        CHAT_ID,
        Some(MESSAGE_ID),
        Some("close all".to_string()),
        Some(NOW as f64),
        None,
        DELIVERY_ID,
    )
    .unwrap()
}

fn relay_adapter() -> TelegramRelaySourceAdapter {
    TelegramRelaySourceAdapter::try_new(TelegramAdapterConfig::new(
        SourceId::new(SOURCE).unwrap(),
        false,
        false,
    ))
    .unwrap()
}

fn adapted_event(adapter: &TelegramRelaySourceAdapter) -> SourceEvent {
    let outcomes = adapter
        .adapt(relay_input(), timestamp("2026-08-09T12:00:00Z"))
        .unwrap();
    match outcomes.into_iter().next().unwrap() {
        TelegramAdaptationOutcome::Accepted { event, .. } => *event,
        other => panic!("expected an accepted Telegram adaptation, got {other:?}"),
    }
}

fn runner_service(
    state: Arc<dyn SourceStateStore>,
    adapter: &TelegramRelaySourceAdapter,
) -> Arc<RunnerIngestionService> {
    let mut registry = ParserRegistry::new();
    registry.register(Box::new(CloseAllParser {
        channels: [CHAT_ID],
    }));
    let producer =
        bind_legacy_telegram_producer(Arc::new(registry), Sha256Digest::new([0x73; 32])).unwrap();
    let pipeline = CompiledPipeline::compile_compatibility(
        PipelineId::try_new("hosted-adapter-parity", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        producer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "hosted-adapter-parity",
        1,
        RouteSelector::try_new(
            Some(SourceId::new(SOURCE).unwrap()),
            None,
            Some(PayloadKind::Text),
            None,
            None,
            Some(TextFormat::Plain),
            None,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
        pipeline.identity().clone(),
    )
    .unwrap();
    let graph = CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap();
    Arc::new(RunnerIngestionService::new(
        graph,
        state,
        adapter.source_adapter_identity().clone(),
        IngestionServiceConfig::default(),
    ))
}

#[tokio::test]
async fn hosted_webhook_and_telegram_bindings_commit_identical_semantics_with_distinct_identities()
{
    let adapter = relay_adapter();
    let event = adapted_event(&adapter);
    let webhook_state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let telegram_state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());

    let webhook_runner = runner_service(Arc::clone(&webhook_state), &adapter);
    let webhook_runtime = Arc::new(AdmissionRuntime::new(
        Arc::new(DurableIngestionService::new(
            webhook_runner,
            Arc::clone(&webhook_state),
        )),
        NonZeroUsize::new(1).unwrap(),
    ));
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
    let body = serde_json::to_vec(&event).unwrap();
    let webhook_timestamp = NOW.to_string();
    let signature = sign_webhook_v1(
        SECRET,
        KEY_ID,
        &webhook_timestamp,
        "webhook-delivery-73",
        &body,
    )
    .unwrap();
    let mut app = signal_parser::runner::http::webhook_router(
        profile,
        Arc::new(MemoryWebhookReplayStore::new()),
        webhook_runtime.clone(),
        Arc::new(|| NOW),
    );
    let response = app
        .call(
            Request::builder()
                .method("POST")
                .uri("/v1/source-events")
                .header("content-type", "application/json")
                .header("x-webhook-key-id", KEY_ID)
                .header("x-webhook-timestamp", webhook_timestamp)
                .header("idempotency-key", "webhook-delivery-73")
                .header("x-webhook-signature", signature)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let webhook_response: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap();
    assert_eq!(webhook_response["source_key"]["source"], SOURCE);
    assert_eq!(
        webhook_response["source_key"]["external_id"],
        "tgmsg:v1:-100246:73"
    );
    assert_eq!(webhook_response["disposition"], "accepted");
    assert!(webhook_response["outcome_ref"].as_str().is_some());

    let telegram_runner = runner_service(Arc::clone(&telegram_state), &adapter);
    let durable_service = Arc::new(DurableIngestionService::new(
        telegram_runner,
        Arc::clone(&telegram_state),
    ));
    let runtime = Arc::new(AdmissionRuntime::new(
        durable_service,
        NonZeroUsize::new(1).unwrap(),
    ));
    let telegram = TelegramRelayIngestionBinding::new(adapter, runtime.clone());
    let outcomes = telegram
        .submit(relay_input(), timestamp("2026-08-09T12:00:00Z"))
        .await
        .unwrap();
    let TelegramIngestionOutcome::Submitted { evidence, response } =
        outcomes.into_iter().next().unwrap()
    else {
        panic!("expected the Telegram binding to submit the adapted event");
    };
    assert_eq!(response.disposition, SourceSubmissionDisposition::Accepted);
    assert_eq!(response.source, (&event).into());
    assert!(
        response
            .admission_identity
            .as_str()
            .starts_with("telegram-relay-v1:")
    );
    assert_eq!(evidence.ingress_delivery_id(), Some(DELIVERY_ID));
    let _ = webhook_runtime.drain();
    let _ = runtime.drain();

    let webhook_receipt = webhook_state.recorded_receipts().unwrap().pop().unwrap();
    let telegram_receipt = telegram_state.recorded_receipts().unwrap().pop().unwrap();
    assert_eq!(webhook_receipt.event, event);
    assert_eq!(telegram_receipt.event, event);
    assert!(matches!(
        webhook_receipt.delivery_identity,
        DurableDeliveryIdentity::Stable(ref value) if value.starts_with("admission:v1:")
    ));
    assert!(matches!(
        telegram_receipt.delivery_identity,
        DurableDeliveryIdentity::Stable(ref value) if value.starts_with("telegram-relay-v1:")
    ));

    let webhook_batch = webhook_state
        .committed_batch(
            webhook_state
                .checkpoint(SOURCE)
                .unwrap()
                .expect("webhook application must commit after drain")
                .batch_id,
        )
        .unwrap()
        .unwrap();
    let telegram_reference = response.outcome_reference.clone();
    let telegram_outcome = runtime
        .outcome(telegram_reference.clone())
        .await
        .unwrap()
        .unwrap();
    let telegram_batch = telegram_state
        .committed_batch(
            telegram_state
                .checkpoint(SOURCE)
                .unwrap()
                .expect("Telegram application must commit after drain")
                .batch_id,
        )
        .unwrap()
        .unwrap();
    assert_eq!(webhook_batch.source, telegram_batch.source);
    assert!(matches!(
        webhook_batch.outcome,
        CommittedNormalizationOutcome::Accepted { ref outputs } if outputs.len() == 1
    ));
    assert!(matches!(
        telegram_outcome,
        signal_parser::runner::service::SourceSubmissionOutcome::Committed {
            outcome: CommittedNormalizationOutcome::Accepted { ref outputs },
            ..
        } if outputs.len() == 1
    ));
    assert_eq!(webhook_batch.envelopes.len(), 1);
    assert_eq!(telegram_batch.envelopes.len(), 1);
    assert_eq!(
        serde_json::to_value(&webhook_batch.envelopes[0].signal).unwrap(),
        serde_json::to_value(&telegram_batch.envelopes[0].signal).unwrap()
    );
    assert_eq!(
        webhook_batch.envelopes[0].source,
        telegram_batch.envelopes[0].source
    );
    assert_eq!(
        webhook_batch.envelopes[0].candidate_ordinal,
        telegram_batch.envelopes[0].candidate_ordinal
    );

    let webhook_report = webhook_runtime.drain();
    assert_eq!(webhook_report.admitted_submissions, 1);
    assert_eq!(webhook_report.completed_submissions, 1);
    let report = runtime.drain();
    assert_eq!(report.admitted_submissions, 1);
    assert_eq!(report.completed_submissions, 1);
}
