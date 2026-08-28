//! Provider-neutral ingestion application composition and committed-batch publication.

pub mod composition;
pub mod manifest;
pub mod publication;
pub mod replay;
pub mod runtime;
pub mod service;
#[cfg(feature = "telegram-compat")]
pub mod telegram;

#[cfg(feature = "provider-http")]
pub mod http;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use chrono::Duration;

use crate::adapters::structured_json::{
    SourceEventJsonlArtifactIdentity, decode_source_event_jsonl,
    encode_committed_normalization_batch_jsonl_record,
};

use crate::ingestion::{DateTimeUtc, PayloadEncoding, SourceEvent, SourceId, SourceOperation};
use crate::normalization::{
    AuthorClass, CanonicalRawSignalsDecoder, CompiledPipeline, CompiledRoutingGraph,
    ComponentBindError, ComponentConfigSchemaRef, ComponentDescriptor, ComponentId, ComponentKind,
    DraftValidationStep, EmptyOutputPolicy, EvaluationInput, NoConfig, PayloadKind,
    PipelineContextRequirements, PipelineEvaluationResult, PipelineId, ResolvedComponentRef,
    RouteEvaluation, RouteSelector, RouteSpec, SemanticVersion, SourceAdapterIdentity,
    StandardSignalFinalizer, StructuredInputCapability, bind_decoder, bind_finalizer,
    raw_signals_v1_schema,
};
use crate::state::{
    AdmittedComponentIdentity, AdmittedExecutionIdentity, ApplicationCommitInput,
    CompareAndCommitRequest, CompareAndCommitResult, DEFAULT_ACTIVE_OUTPUT_LIMIT,
    DurableDeliveryIdentity, PreflightRequest, PreflightResult, ReplacementPolicy, SnapshotRequest,
    SourceStateError, SourceStateStore,
};

/// Policy used while composing durable source application with publication.
#[derive(Debug, Clone)]
pub struct IngestionServiceConfig {
    pub reservation_ttl: Duration,
    pub replacement_policy: ReplacementPolicy,
    pub maximum_active_outputs: usize,
    pub publication_sink: Option<String>,
    pub compare_and_commit_retries: usize,
}

impl Default for IngestionServiceConfig {
    fn default() -> Self {
        Self {
            reservation_ttl: Duration::minutes(5),
            replacement_policy: ReplacementPolicy::Patch,
            maximum_active_outputs: DEFAULT_ACTIVE_OUTPUT_LIMIT,
            publication_sink: None,
            compare_and_commit_retries: 0,
        }
    }
}

/// The durable disposition observed for one source submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionOutcomeKind {
    Committed,
    Existing,
    RetryRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestionSubmissionOutcome {
    Committed {
        batch_id: crate::state::CommittedBatchId,
    },
    Existing {
        batch_id: crate::state::CommittedBatchId,
    },
    RetryRequired,
}

impl IngestionSubmissionOutcome {
    fn kind(&self) -> IngestionOutcomeKind {
        match self {
            Self::Committed { .. } => IngestionOutcomeKind::Committed,
            Self::Existing { .. } => IngestionOutcomeKind::Existing,
            Self::RetryRequired => IngestionOutcomeKind::RetryRequired,
        }
    }
}

/// Count-only stage observations for one ingestion pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestionExecutionReport {
    pub preflight_reserved: usize,
    pub preflight_existing: usize,
    pub preflight_conflicts: usize,
    pub preflight_stale: usize,
    pub evaluations_completed: usize,
    pub evaluations_failed: usize,
    pub commits_committed: usize,
    pub commits_existing: usize,
    pub commits_retry_required: usize,
    pub commits_fence_lost: usize,
    pub commits_application_rejected: usize,
}

impl IngestionExecutionReport {
    fn merge(&mut self, other: Self) {
        self.preflight_reserved += other.preflight_reserved;
        self.preflight_existing += other.preflight_existing;
        self.preflight_conflicts += other.preflight_conflicts;
        self.preflight_stale += other.preflight_stale;
        self.evaluations_completed += other.evaluations_completed;
        self.evaluations_failed += other.evaluations_failed;
        self.commits_committed += other.commits_committed;
        self.commits_existing += other.commits_existing;
        self.commits_retry_required += other.commits_retry_required;
        self.commits_fence_lost += other.commits_fence_lost;
        self.commits_application_rejected += other.commits_application_rejected;
    }

    pub(crate) fn saturating_difference(self, before: Self) -> Self {
        Self {
            preflight_reserved: self
                .preflight_reserved
                .saturating_sub(before.preflight_reserved),
            preflight_existing: self
                .preflight_existing
                .saturating_sub(before.preflight_existing),
            preflight_conflicts: self
                .preflight_conflicts
                .saturating_sub(before.preflight_conflicts),
            preflight_stale: self.preflight_stale.saturating_sub(before.preflight_stale),
            evaluations_completed: self
                .evaluations_completed
                .saturating_sub(before.evaluations_completed),
            evaluations_failed: self
                .evaluations_failed
                .saturating_sub(before.evaluations_failed),
            commits_committed: self
                .commits_committed
                .saturating_sub(before.commits_committed),
            commits_existing: self
                .commits_existing
                .saturating_sub(before.commits_existing),
            commits_retry_required: self
                .commits_retry_required
                .saturating_sub(before.commits_retry_required),
            commits_fence_lost: self
                .commits_fence_lost
                .saturating_sub(before.commits_fence_lost),
            commits_application_rejected: self
                .commits_application_rejected
                .saturating_sub(before.commits_application_rejected),
        }
    }

    fn observe_preflight(&mut self, result: &PreflightResult) {
        match result {
            PreflightResult::Reserved(_) => self.preflight_reserved += 1,
            PreflightResult::ExistingCommitted(_) => self.preflight_existing += 1,
            PreflightResult::Conflict { .. } => self.preflight_conflicts += 1,
            PreflightResult::Stale { .. } => self.preflight_stale += 1,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IngestionServiceError {
    #[error("source application state failed: {0}")]
    State(#[from] SourceStateError),
    #[error("the committed outcome reference is invalid")]
    InvalidOutcomeReference,
}

/// Composes durable source application around a compiled routing graph.
pub struct IngestionService {
    graph: CompiledRoutingGraph,
    state: Arc<dyn SourceStateStore>,
    source_adapter: SourceAdapterIdentity,
    execution_identity: Option<AdmittedExecutionIdentity>,
    config: IngestionServiceConfig,
    execution: std::sync::Mutex<IngestionExecutionReport>,
}

impl std::fmt::Debug for IngestionService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IngestionService")
            .field("source_adapter", &self.source_adapter)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl IngestionService {
    pub fn new(
        graph: CompiledRoutingGraph,
        state: Arc<dyn SourceStateStore>,
        source_adapter: SourceAdapterIdentity,
        config: IngestionServiceConfig,
    ) -> Self {
        Self {
            graph,
            state,
            source_adapter,
            execution_identity: None,
            config,
            execution: std::sync::Mutex::new(IngestionExecutionReport::default()),
        }
    }

    pub fn source_adapter_identity(&self) -> &SourceAdapterIdentity {
        &self.source_adapter
    }

    pub fn execution_identity(&self) -> Option<&AdmittedExecutionIdentity> {
        self.execution_identity.as_ref()
    }

    pub fn with_execution_identity(
        mut self,
        execution_identity: AdmittedExecutionIdentity,
    ) -> Self {
        self.execution_identity = Some(execution_identity);
        self
    }

    pub fn with_source_adapter(mut self, source_adapter: SourceAdapterIdentity) -> Self {
        self.source_adapter = source_adapter;
        self
    }

    /// Returns aggregate count-only execution observations for this service instance.
    pub fn execution_report(&self) -> IngestionExecutionReport {
        *self
            .execution
            .lock()
            .expect("ingestion execution lock poisoned")
    }

    pub fn submit(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
    ) -> Result<IngestionSubmissionOutcome, IngestionServiceError> {
        self.submit_with_author_class(event, delivery_identity, None)
    }

    /// Applies an event with authenticated provider-neutral routing context.
    pub fn submit_with_author_class(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
        author_class: Option<AuthorClass>,
    ) -> Result<IngestionSubmissionOutcome, IngestionServiceError> {
        self.submit_with_report_and_author_class(event, delivery_identity, author_class)
            .map(|(outcome, _)| outcome)
    }

    /// Applies one event and returns count-only observations for its durable stages.
    pub fn submit_with_report(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
    ) -> Result<(IngestionSubmissionOutcome, IngestionExecutionReport), IngestionServiceError> {
        self.submit_with_report_and_author_class(event, delivery_identity, None)
    }

    /// Applies one event with authenticated routing context and returns count-only observations.
    pub fn submit_with_report_and_author_class(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
        author_class: Option<AuthorClass>,
    ) -> Result<(IngestionSubmissionOutcome, IngestionExecutionReport), IngestionServiceError> {
        let result = self.submit_with_report_inner(event, delivery_identity, author_class);
        if let Ok((_, report)) = &result {
            self.execution
                .lock()
                .expect("ingestion execution lock poisoned")
                .merge(*report);
        }
        result
    }

    fn submit_with_report_inner(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
        author_class: Option<AuthorClass>,
    ) -> Result<(IngestionSubmissionOutcome, IngestionExecutionReport), IngestionServiceError> {
        let mut report = IngestionExecutionReport::default();
        for attempt in 0..=self.config.compare_and_commit_retries {
            let (outcome, attempt_report) = self.submit_once_with_report(
                event.clone(),
                delivery_identity.clone(),
                author_class.clone(),
            )?;
            let compare_and_commit_retry = attempt_report.commits_retry_required != 0
                || attempt_report.commits_fence_lost != 0;
            report.merge(attempt_report);
            if outcome != IngestionSubmissionOutcome::RetryRequired
                || !compare_and_commit_retry
                || attempt == self.config.compare_and_commit_retries
            {
                return Ok((outcome, report));
            }
        }
        unreachable!("the inclusive retry loop always returns")
    }

    fn submit_once_with_report(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
        author_class: Option<AuthorClass>,
    ) -> Result<(IngestionSubmissionOutcome, IngestionExecutionReport), IngestionServiceError> {
        let requested_at = event.received_at();
        let expires_at = DateTimeUtc::new(requested_at.into_inner() + self.config.reservation_ttl);
        let mut report = IngestionExecutionReport::default();
        let preflight = self.state.preflight(PreflightRequest {
            event: event.clone(),
            delivery_identity: Some(delivery_identity),
            source_adapter: self.source_adapter.clone(),
            execution_identity: self.execution_identity.clone(),
            adapter_evidence: None,
            requested_at,
            expires_at,
        })?;
        report.observe_preflight(&preflight);
        let reservation = match preflight {
            PreflightResult::ExistingCommitted(batch_id) => {
                return Ok((IngestionSubmissionOutcome::Existing { batch_id }, report));
            }
            PreflightResult::Conflict { .. } | PreflightResult::Stale { .. } => {
                return Ok((IngestionSubmissionOutcome::RetryRequired, report));
            }
            PreflightResult::Reserved(reservation) => reservation,
        };

        if event.operation() == SourceOperation::Delete {
            let compare_token = self.state.route_only_compare_token(&reservation)?;
            return self
                .commit(
                    compare_token,
                    ApplicationCommitInput::LifecycleOnlyDelete,
                    requested_at,
                    &mut report,
                )
                .map(|outcome| (outcome, report));
        }

        match self.graph.route(EvaluationInput::new(
            event.clone(),
            self.source_adapter.clone(),
            author_class,
        )) {
            RouteEvaluation::Completed(evaluation) => {
                report.evaluations_completed += 1;
                self.commit(
                    self.state.route_only_compare_token(&reservation)?,
                    ApplicationCommitInput::CompletedEvaluation(&evaluation),
                    requested_at,
                    &mut report,
                )
                .map(|outcome| (outcome, report))
            }
            RouteEvaluation::Prepared(prepared) => {
                let selected_pipeline = prepared
                    .identity()
                    .selected_pipeline()
                    .expect("prepared evaluation always selects a pipeline")
                    .clone();
                let snapshot = self.state.snapshot(SnapshotRequest {
                    applied_event_id: reservation.applied_event_id.clone(),
                    fence: reservation.fence,
                    selected_pipeline,
                    requirements: prepared.requirements().clone(),
                    requested_at,
                })?;
                match prepared.evaluate(&snapshot.base_context) {
                    PipelineEvaluationResult::Completed(evaluation) => {
                        report.evaluations_completed += 1;
                        self.commit(
                            snapshot.compare_token,
                            ApplicationCommitInput::CompletedEvaluation(&evaluation),
                            requested_at,
                            &mut report,
                        )
                        .map(|outcome| (outcome, report))
                    }
                    PipelineEvaluationResult::Failed(failure) => {
                        report.evaluations_failed += 1;
                        self.state.record_evaluation_failure(
                            reservation.applied_event_id,
                            reservation.fence,
                            &failure,
                            requested_at,
                        )?;
                        Ok((IngestionSubmissionOutcome::RetryRequired, report))
                    }
                }
            }
        }
    }

    fn commit(
        &self,
        compare_token: crate::state::ApplicationCompareToken,
        input: ApplicationCommitInput<'_>,
        committed_at: DateTimeUtc,
        report: &mut IngestionExecutionReport,
    ) -> Result<IngestionSubmissionOutcome, IngestionServiceError> {
        match self.state.compare_and_commit(CompareAndCommitRequest {
            compare_token,
            input,
            replacement_policy: self.config.replacement_policy,
            maximum_active_outputs: self.config.maximum_active_outputs,
            publication_sink: self.config.publication_sink.clone(),
            committed_at,
        })? {
            CompareAndCommitResult::Committed(batch_id) => {
                report.commits_committed += 1;
                Ok(IngestionSubmissionOutcome::Committed { batch_id })
            }
            CompareAndCommitResult::AlreadyCommitted(batch_id) => {
                report.commits_existing += 1;
                Ok(IngestionSubmissionOutcome::Committed { batch_id })
            }
            CompareAndCommitResult::ApplicationRejected(batch_id) => {
                report.commits_application_rejected += 1;
                Ok(IngestionSubmissionOutcome::Committed { batch_id })
            }
            CompareAndCommitResult::RetryRequired => {
                report.commits_retry_required += 1;
                Ok(IngestionSubmissionOutcome::RetryRequired)
            }
            CompareAndCommitResult::FenceLost => {
                report.commits_fence_lost += 1;
                Ok(IngestionSubmissionOutcome::RetryRequired)
            }
        }
    }
}

/// Explicit malformed-record behavior for a source-event JSONL artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfflineErrorPolicy {
    Stop,
    Continue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfflineIngestionReport {
    pub artifact_identity: SourceEventJsonlArtifactIdentity,
    pub admitted_records: usize,
    pub malformed_records: usize,
    pub committed_records: usize,
    pub existing_records: usize,
    pub retry_required_records: usize,
    pub execution: IngestionExecutionReport,
}

#[derive(Debug, thiserror::Error)]
pub enum OfflineIngestionError {
    #[error("source-event JSONL record failed at physical line {physical_line}: {message}")]
    MalformedRecord { physical_line: u64, message: String },
    #[error(transparent)]
    Service(#[from] IngestionServiceError),
    #[error(transparent)]
    Logical(#[from] service::LogicalIngestionServiceError),
    #[error("offline delivery identity cannot form an admission identity")]
    InvalidAdmissionIdentity,
    #[error("logical service returned an invalid outcome reference")]
    InvalidOutcomeReference,
}

/// Runs a source-event JSONL artifact through the durable application service.
enum OfflineIngestionBackend {
    Direct(Arc<IngestionService>),
    Runtime {
        runtime: Arc<runtime::AdmissionRuntime>,
        event_deadline: std::time::Duration,
    },
}

/// Runs a source-event JSONL artifact through a durable application boundary.
pub struct OfflineIngestionRunner {
    backend: OfflineIngestionBackend,
    error_policy: OfflineErrorPolicy,
}

impl OfflineIngestionRunner {
    pub fn new(service: Arc<IngestionService>, error_policy: OfflineErrorPolicy) -> Self {
        Self {
            backend: OfflineIngestionBackend::Direct(service),
            error_policy,
        }
    }

    /// Uses the shared bounded admission runtime for each offline record.
    pub fn new_with_runtime(
        runtime: Arc<runtime::AdmissionRuntime>,
        event_deadline: std::time::Duration,
        error_policy: OfflineErrorPolicy,
    ) -> Self {
        Self {
            backend: OfflineIngestionBackend::Runtime {
                runtime,
                event_deadline,
            },
            error_policy,
        }
    }

    pub fn run(
        &self,
        artifact_identity: SourceEventJsonlArtifactIdentity,
        artifact_bytes: &[u8],
    ) -> Result<OfflineIngestionReport, OfflineIngestionError> {
        let artifact = decode_source_event_jsonl(artifact_identity, artifact_bytes);
        let mut report = OfflineIngestionReport {
            artifact_identity: artifact.artifact_identity().clone(),
            admitted_records: 0,
            malformed_records: 0,
            committed_records: 0,
            existing_records: 0,
            retry_required_records: 0,
            execution: IngestionExecutionReport::default(),
        };
        for record in artifact.into_records() {
            match record {
                Ok(record) => {
                    let delivery_identity = record.delivery_identity().clone();
                    let (outcome, execution) =
                        self.submit(record.into_event(), delivery_identity)?;
                    report.admitted_records += 1;
                    report.execution.merge(execution);
                    match outcome.kind() {
                        IngestionOutcomeKind::Committed => report.committed_records += 1,
                        IngestionOutcomeKind::Existing => report.existing_records += 1,
                        IngestionOutcomeKind::RetryRequired => report.retry_required_records += 1,
                    }
                }
                Err(error) => {
                    report.malformed_records += 1;
                    if self.error_policy == OfflineErrorPolicy::Stop {
                        return Err(OfflineIngestionError::MalformedRecord {
                            physical_line: error.physical_line(),
                            message: error.error().message().to_string(),
                        });
                    }
                }
            }
        }
        Ok(report)
    }

    fn submit(
        &self,
        event: SourceEvent,
        delivery_identity: DurableDeliveryIdentity,
    ) -> Result<(IngestionSubmissionOutcome, IngestionExecutionReport), OfflineIngestionError> {
        match &self.backend {
            OfflineIngestionBackend::Direct(service) => {
                Ok(service.submit_with_report(event, delivery_identity)?)
            }
            OfflineIngestionBackend::Runtime {
                runtime,
                event_deadline,
            } => {
                let admission_identity =
                    service::AdmissionIdentity::from_delivery_identity(&delivery_identity)
                        .map_err(|_| OfflineIngestionError::InvalidAdmissionIdentity)?;
                let authenticated_context = service::AuthenticatedSourceContext::try_new(
                    event.key().source().clone(),
                    "source-event-jsonl",
                    None,
                )
                .map_err(|_| OfflineIngestionError::InvalidAdmissionIdentity)?;
                match runtime.submit_controlled(
                    service::SourceSubmission {
                        admission_identity,
                        delivery_identity,
                        authenticated_context,
                        event,
                    },
                    runtime::RuntimeDeadline::from_now(*event_deadline),
                    runtime::CancellationToken::new(),
                ) {
                    runtime::ControlledSubmissionResult::Completed(Ok(response)) => Ok((
                        match response.disposition {
                            service::SourceSubmissionDisposition::Committed => {
                                IngestionSubmissionOutcome::Committed {
                                    batch_id: response
                                        .outcome_reference
                                        .committed_batch_id()
                                        .map_err(|_| {
                                            OfflineIngestionError::InvalidOutcomeReference
                                        })?,
                                }
                            }
                            service::SourceSubmissionDisposition::Existing => {
                                IngestionSubmissionOutcome::Existing {
                                    batch_id: response
                                        .outcome_reference
                                        .committed_batch_id()
                                        .map_err(|_| {
                                            OfflineIngestionError::InvalidOutcomeReference
                                        })?,
                                }
                            }
                            service::SourceSubmissionDisposition::Accepted => {
                                IngestionSubmissionOutcome::RetryRequired
                            }
                        },
                        IngestionExecutionReport::default(),
                    )),
                    runtime::ControlledSubmissionResult::Completed(Err(
                        service::LogicalIngestionServiceError::RetryRequired,
                    ))
                    | runtime::ControlledSubmissionResult::DeadlineExceeded { .. }
                    | runtime::ControlledSubmissionResult::Cancelled { .. } => Ok((
                        IngestionSubmissionOutcome::RetryRequired,
                        IngestionExecutionReport::default(),
                    )),
                    runtime::ControlledSubmissionResult::Completed(Err(error)) => {
                        Err(OfflineIngestionError::Logical(error))
                    }
                }
            }
        }
    }
}

/// Builds the shipped structured JSONL pipeline for one configured source.
pub fn structured_jsonl_execution_identity(
    source_id: SourceId,
) -> Result<AdmittedExecutionIdentity, IngestionBuildError> {
    let state: Arc<dyn SourceStateStore> = Arc::new(crate::state::MemorySourceStateStore::new());
    structured_jsonl_service(source_id, state, IngestionServiceConfig::default()).map(|service| {
        service
            .execution_identity()
            .cloned()
            .expect("built-in service pins execution identity")
    })
}

pub fn structured_jsonl_service(
    source_id: SourceId,
    state: Arc<dyn SourceStateStore>,
    config: IngestionServiceConfig,
) -> Result<IngestionService, IngestionBuildError> {
    let config_schema = ComponentConfigSchemaRef::try_new("quant-system/no-config@1")?;
    let component_config = NoConfig::new(config_schema);
    let decoder = bind_decoder(
        structured_component_descriptor(ComponentKind::Decoder, "canonical-raw-signals")?,
        &component_config,
        |_| Ok(CanonicalRawSignalsDecoder),
    )?;
    let finalizer = bind_finalizer(
        structured_component_descriptor(ComponentKind::Finalizer, "standard-signal-finalizer")?,
        &component_config,
        |_| Ok(StandardSignalFinalizer),
    )?;
    let decoder_identity = admitted_component_identity(decoder.resolved());
    let finalizer_identity = admitted_component_identity(finalizer.resolved());
    let pipeline = CompiledPipeline::compile_structured(
        PipelineId::try_new("structured-raw-signals", "pipeline ID")?,
        SemanticVersion::new(1, 0, 0),
        decoder,
        DraftValidationStep::NoneDeclared,
        finalizer,
    )?;
    let pipeline_identity = pipeline.identity().clone();
    let route = RouteSpec::try_new(
        "structured-raw-signals",
        1,
        RouteSelector::try_new(
            Some(source_id.clone()),
            None,
            Some(PayloadKind::Structured),
            Some(raw_signals_v1_schema()),
            Some(PayloadEncoding::Json),
            None,
            None,
            None,
            std::collections::BTreeMap::new(),
        )?,
        pipeline_identity.clone(),
    )?;
    let graph = CompiledRoutingGraph::compile(vec![route], vec![pipeline])?;
    let execution_identity = AdmittedExecutionIdentity {
        routing_graph: graph.identity().canonical_bytes().clone(),
        pipeline: pipeline_identity.canonical_bytes(),
        decoder: decoder_identity,
        finalizer: finalizer_identity,
    };
    let source_adapter = SourceAdapterIdentity::without_config(
        ComponentId::try_new("source-event-jsonl", "adapter ID")?,
        SemanticVersion::new(1, 0, 0),
    );
    Ok(IngestionService::new(graph, state, source_adapter, config)
        .with_execution_identity(execution_identity))
}

fn admitted_component_identity(component: &ResolvedComponentRef) -> AdmittedComponentIdentity {
    AdmittedComponentIdentity {
        id: component.id().as_str().to_string(),
        kind: component.kind().tag(),
        version_major: component.implementation_version().major(),
        version_minor: component.implementation_version().minor(),
        version_patch: component.implementation_version().patch(),
        version_prerelease: component.implementation_version().prerelease().to_string(),
        version_build: component.implementation_version().build().to_string(),
        contract_version: component.contract_version(),
        config_identity: component.config_identity().canonical_bytes().clone(),
    }
}

fn structured_component_descriptor(
    kind: ComponentKind,
    id: &str,
) -> Result<ComponentDescriptor, crate::normalization::ContractValueError> {
    ComponentDescriptor::try_new(
        ComponentId::try_new(id, "component ID")?,
        kind,
        SemanticVersion::new(1, 0, 0),
        1,
        ComponentConfigSchemaRef::try_new("quant-system/no-config@1")?,
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
}

#[derive(Debug, thiserror::Error)]
pub enum IngestionBuildError {
    #[error(transparent)]
    Value(#[from] crate::normalization::ContractValueError),
    #[error(transparent)]
    Bind(#[from] ComponentBindError),
    #[error(transparent)]
    Graph(#[from] crate::normalization::GraphCompileError),
}

/// One durable committed-batch delivery target.
pub trait CommittedBatchSink: Send + Sync {
    fn acknowledgement_policy(&self) -> publication::DeliveryAcknowledgementPolicy;

    fn publish(
        &self,
        delivery: publication::CommittedDelivery<'_>,
    ) -> Result<publication::PublicationDeliveryReceipt, PublicationSinkError>;
}

#[derive(Debug, thiserror::Error)]
pub enum PublicationSinkError {
    #[error("committed batch serialization failed: {0}")]
    Serialization(String),
    #[error("committed batch sink IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("committed batch sink outcome is unknown: {0}")]
    UnknownOutcome(String),
}

/// Append-only strict JSONL sink for authoritative committed batches.
pub struct CommittedBatchJsonlSink {
    writer: std::sync::Mutex<BufWriter<File>>,
}

impl std::fmt::Debug for CommittedBatchJsonlSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedBatchJsonlSink")
            .finish_non_exhaustive()
    }
}

impl CommittedBatchJsonlSink {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: std::sync::Mutex::new(BufWriter::new(file)),
        })
    }
}

impl CommittedBatchSink for CommittedBatchJsonlSink {
    fn acknowledgement_policy(&self) -> publication::DeliveryAcknowledgementPolicy {
        publication::DeliveryAcknowledgementPolicy::DuplicateTolerant
    }

    fn publish(
        &self,
        delivery: publication::CommittedDelivery<'_>,
    ) -> Result<publication::PublicationDeliveryReceipt, PublicationSinkError> {
        let bytes = encode_committed_normalization_batch_jsonl_record(delivery.batch)
            .map_err(|error| PublicationSinkError::Serialization(error.to_string()))?;
        let mut writer = self.writer.lock().map_err(|_| {
            PublicationSinkError::Io(std::io::Error::other(
                "committed batch writer lock poisoned",
            ))
        })?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(publication::PublicationDeliveryReceipt {
            delivery_id: delivery.delivery_id,
            batch_id: delivery.batch.batch_id.clone(),
        })
    }
}
