use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use signal_parser::adapters::structured_json::encode_source_event_jsonl_record;
use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::{
    CanonicalRawSignalsDecoder, CompiledPipeline, CompiledRoutingGraph, ComponentConfigSchemaRef,
    ComponentDescriptor, ComponentId, ComponentKind, DraftValidationStep, EmptyOutputPolicy,
    NoConfig, PayloadKind, PipelineContextRequirements, PipelineId, RouteSelector, RouteSpec,
    SemanticVersion, Sha256Digest, SourceAdapterIdentity, StandardSignalFinalizer,
    StructuredInputCapability, bind_decoder, bind_finalizer, raw_signals_v1_schema,
};
use signal_parser::runner::{
    CommittedBatchSink, IngestionService, IngestionServiceConfig, OfflineErrorPolicy,
    OfflineIngestionRunner, PublicationSinkError,
    publication::{
        DeliveryAcknowledgementPolicy, PublicationDeliveryReceipt, PublicationOrchestrator,
        PublicationRetryPolicy,
    },
};
use signal_parser::state::{MemorySourceStateStore, ReplacementPolicy, SourceStateStore};

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn source_event(external_id: &str) -> SourceEvent {
    SourceEvent::new(
        SourceEventKey::new(
            SourceId::new("jsonl:runner-test").unwrap(),
            ExternalEventId::new(external_id).unwrap(),
        ),
        SourceOperation::Create,
        SourceRevision::Monotonic(1),
        SourceTimestamp::new(
            timestamp("2026-08-09T00:00:00Z"),
            SourceTimestampQuality::SourceProvided,
        ),
        timestamp("2026-08-09T00:00:01Z"),
        SourcePayload::Structured(StructuredPayload::new(
            raw_signals_v1_schema(),
            PayloadEncoding::Json,
            BoundedBytes::new(
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "signals": [{
                        "action": "CloseAll",
                        "ts": "2026-08-09T00:00:00Z"
                    }]
                }))
                .unwrap(),
            )
            .unwrap(),
        )),
    )
}

fn descriptor(kind: ComponentKind, id: &str) -> ComponentDescriptor {
    ComponentDescriptor::try_new(
        ComponentId::try_new(id, "component ID").unwrap(),
        kind,
        SemanticVersion::new(1, 0, 0),
        1,
        ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap(),
        PipelineContextRequirements::none(),
        EmptyOutputPolicy::Reject,
        if kind == ComponentKind::Decoder {
            vec![StructuredInputCapability::new(
                raw_signals_v1_schema(),
                PayloadEncoding::Json,
            )]
        } else {
            vec![]
        },
        vec![],
        vec![],
    )
    .unwrap()
}

fn service(state: Arc<dyn SourceStateStore>) -> Arc<IngestionService> {
    let config =
        NoConfig::new(ComponentConfigSchemaRef::try_new("quant-system/no-config@1").unwrap());
    let decoder = bind_decoder(
        descriptor(ComponentKind::Decoder, "runner-decoder"),
        &config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )
    .unwrap();
    let finalizer = bind_finalizer(
        descriptor(ComponentKind::Finalizer, "runner-finalizer"),
        &config,
        |_| Ok(StandardSignalFinalizer),
    )
    .unwrap();
    let pipeline = CompiledPipeline::compile_structured(
        PipelineId::try_new("runner", "pipeline ID").unwrap(),
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )
    .unwrap();
    let route = RouteSpec::try_new(
        "runner",
        1,
        RouteSelector::try_new(
            Some(SourceId::new("jsonl:runner-test").unwrap()),
            None,
            Some(PayloadKind::Structured),
            Some(raw_signals_v1_schema()),
            Some(PayloadEncoding::Json),
            None,
            None,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
        pipeline.identity().clone(),
    )
    .unwrap();
    Arc::new(IngestionService::new(
        CompiledRoutingGraph::compile(vec![route], vec![pipeline]).unwrap(),
        state,
        SourceAdapterIdentity::new(
            ComponentId::try_new("source-event-jsonl", "adapter ID").unwrap(),
            SemanticVersion::new(1, 0, 0),
            Sha256Digest::new([9; 32]),
        ),
        IngestionServiceConfig {
            replacement_policy: ReplacementPolicy::Patch,
            publication_sink: Some("committed-jsonl".to_string()),
            ..IngestionServiceConfig::default()
        },
    ))
}

#[derive(Default)]
struct RecordingSink {
    batches: Mutex<Vec<String>>,
}

impl CommittedBatchSink for RecordingSink {
    fn acknowledgement_policy(&self) -> DeliveryAcknowledgementPolicy {
        DeliveryAcknowledgementPolicy::IdempotentByDeliveryId
    }

    fn publish(
        &self,
        delivery: signal_parser::runner::publication::CommittedDelivery<'_>,
    ) -> Result<PublicationDeliveryReceipt, PublicationSinkError> {
        self.batches
            .lock()
            .unwrap()
            .push(delivery.batch.batch_id.to_string_id());
        Ok(PublicationDeliveryReceipt {
            delivery_id: delivery.delivery_id,
            batch_id: delivery.batch.batch_id,
        })
    }
}

#[test]
fn offline_runner_commits_valid_records_continues_after_malformed_input_and_publishes_batch() {
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let runner = OfflineIngestionRunner::new(service(state.clone()), OfflineErrorPolicy::Continue);
    let mut artifact = encode_source_event_jsonl_record(&source_event("event-1")).unwrap();
    artifact.extend_from_slice(b"{not-json}\n");

    let report = runner.run(&artifact).unwrap();
    assert_eq!(report.admitted_records, 1);
    assert_eq!(report.malformed_records, 1);
    assert_eq!(report.retry_required_records, 0);
    assert!(
        report
            .artifact_identity
            .starts_with("source-event-jsonl@1:sha256:")
    );

    let checkpoint = state.checkpoint("jsonl:runner-test").unwrap().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let orchestrator = PublicationOrchestrator::new(
        state,
        sink.clone(),
        PublicationRetryPolicy::new(1, chrono::Duration::minutes(1)).unwrap(),
    );
    assert_eq!(orchestrator.run_once(8).unwrap().acknowledged, 1);
    assert_eq!(
        sink.batches.lock().unwrap().as_slice(),
        &[checkpoint.batch_id.to_string_id()]
    );
}

#[test]
fn offline_runner_stop_policy_returns_the_physical_line_for_invalid_input() {
    let state: Arc<dyn SourceStateStore> = Arc::new(MemorySourceStateStore::new());
    let runner = OfflineIngestionRunner::new(service(state), OfflineErrorPolicy::Stop);
    let error = runner.run(b"\n{not-json}\n").unwrap_err();
    assert!(error.to_string().contains("physical line 2"));
}
