//! Manifest-derived local composition for structured JSONL ingestion.

use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;

use crate::adapters::structured_json::{
    SourceEventJsonlArtifactIdentity, SourceEventJsonlArtifactIdentityError,
};
use crate::ingestion::DateTimeUtc;
use crate::state::{SourceStateError, SourceStateStore, SqliteSourceStateStore};

use crate::runner::manifest::{ManifestError, ResolvedRunnerManifest, RunnerManifest};
use crate::runner::publication::{PublicationOrchestrator, PublicationRunReport};
use crate::runner::runtime::{AdmissionRuntime, RuntimeRunArtifact};
use crate::runner::service::DurableIngestionService;
use crate::runner::{
    CommittedBatchJsonlSink, CommittedBatchSink, IngestionBuildError, IngestionExecutionReport,
    IngestionOutcomeKind, OfflineIngestionError, OfflineIngestionReport, OfflineIngestionRunner,
    structured_jsonl_service,
};

/// A local, manifest-configured ingestion and publication composition.
pub struct LocalIngestionComposition {
    manifest: ResolvedRunnerManifest,
    source_path: PathBuf,
    source_artifact_identity: SourceEventJsonlArtifactIdentity,
    ingestion_runner: OfflineIngestionRunner,
    execution_service: Arc<crate::runner::IngestionService>,
    admission_runtime: Arc<AdmissionRuntime>,
    publication_orchestrator: PublicationOrchestrator,
    publication_batch_size: usize,
}

impl LocalIngestionComposition {
    /// Loads a runner manifest and constructs its local durable adapters.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, LocalIngestionCompositionError> {
        let manifest = fs::read_to_string(path)?;
        Self::build(RunnerManifest::from_toml_str(&manifest)?)
    }

    /// Builds local SQLite, JSONL, ingestion, and publication adapters from a manifest.
    pub fn build(manifest: RunnerManifest) -> Result<Self, LocalIngestionCompositionError> {
        let manifest = manifest.compile()?;
        let wiring = manifest.wiring()?;
        let state: Arc<dyn SourceStateStore> =
            Arc::new(SqliteSourceStateStore::open(&wiring.state_path)?);
        let sink = Arc::new(CommittedBatchJsonlSink::open(&wiring.sink_path)?);
        Self::from_resolved(manifest, state, sink)
    }

    /// Builds local ingestion with a supplied sink that satisfies the manifest acknowledgement policy.
    pub fn build_with_sink(
        manifest: RunnerManifest,
        sink: Arc<dyn CommittedBatchSink>,
    ) -> Result<Self, LocalIngestionCompositionError> {
        let manifest = manifest.compile()?;
        let wiring = manifest.wiring()?;
        let state: Arc<dyn SourceStateStore> =
            Arc::new(SqliteSourceStateStore::open(&wiring.state_path)?);
        Self::from_resolved(manifest, state, sink)
    }

    fn from_resolved(
        manifest: ResolvedRunnerManifest,
        state: Arc<dyn SourceStateStore>,
        sink: Arc<dyn CommittedBatchSink>,
    ) -> Result<Self, LocalIngestionCompositionError> {
        let wiring = manifest.wiring()?;
        if sink.acknowledgement_policy() != manifest.sink.acknowledgement {
            return Err(LocalIngestionCompositionError::SinkAcknowledgementPolicy);
        }
        let runner_service = Arc::new(structured_jsonl_service(
            wiring.source_id,
            Arc::clone(&state),
            wiring.ingestion_config,
        )?);
        let ManifestAdmissionRuntime {
            runtime: admission_runtime,
            event_deadline,
        } = ManifestAdmissionRuntime::new(
            &manifest,
            Arc::clone(&runner_service),
            Arc::clone(&state),
        )?;
        let source_artifact_identity = SourceEventJsonlArtifactIdentity::try_new(format!(
            "source-event-jsonl@1:path:{}",
            wiring.source_path.display()
        ))?;
        Ok(Self {
            manifest,
            source_path: wiring.source_path,
            source_artifact_identity,
            ingestion_runner: OfflineIngestionRunner::new_with_runtime(
                Arc::clone(&admission_runtime),
                event_deadline,
                wiring.malformed_records,
            ),
            execution_service: runner_service,
            admission_runtime,
            publication_orchestrator: PublicationOrchestrator::new(
                state,
                sink,
                wiring.publication_retry_policy,
            ),
            publication_batch_size: wiring.publication_batch_size,
        })
    }

    /// Ingests the configured source artifact and publishes its committed batches.
    pub fn run_offline(&self) -> Result<LocalIngestionReport, LocalIngestionCompositionError> {
        self.manifest.validate()?;
        let started_at = DateTimeUtc::new(Utc::now());
        let runtime_before = self.admission_runtime.run_artifact();
        let execution_before = self.execution_service.execution_report();
        let source = fs::read(&self.source_path)?;
        let ingestion = self
            .ingestion_runner
            .run(self.source_artifact_identity.clone(), &source)?;
        let publication = self
            .publication_orchestrator
            .run_once(self.publication_batch_size)?;
        let completed_at = DateTimeUtc::new(Utc::now());
        let runtime_after = self.admission_runtime.run_artifact();
        let execution = self
            .execution_service
            .execution_report()
            .saturating_difference(execution_before);
        let artifact = IngestionRunArtifact::from_reports(
            &self.manifest,
            started_at,
            completed_at,
            &ingestion,
            execution,
            publication,
            runtime_after
                .metrics
                .deadline_expired_submissions
                .saturating_sub(runtime_before.metrics.deadline_expired_submissions),
        );
        Ok(LocalIngestionReport {
            ingestion,
            published_batches: publication.acknowledged,
            publication,
            runtime: runtime_after,
            artifact,
        })
    }
}

/// Shared manifest-derived admission boundary for offline, online, and replay hosts.
pub struct ManifestAdmissionRuntime {
    pub runtime: Arc<AdmissionRuntime>,
    pub event_deadline: std::time::Duration,
}

impl ManifestAdmissionRuntime {
    /// Builds the logical service boundary with the resolved queue and deadline limits.
    pub fn new(
        manifest: &ResolvedRunnerManifest,
        runner_service: Arc<crate::runner::IngestionService>,
        state: Arc<dyn SourceStateStore>,
    ) -> Result<Self, ManifestError> {
        let wiring = manifest.wiring()?;
        Ok(Self {
            runtime: Arc::new(AdmissionRuntime::new(
                Arc::new(DurableIngestionService::new(runner_service, state)),
                NonZeroUsize::new(wiring.admission_queue_depth)
                    .expect("validated admission queue depth"),
            )),
            event_deadline: wiring.event_deadline,
        })
    }
}

/// The result of one local ingestion and publication pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalIngestionReport {
    pub ingestion: OfflineIngestionReport,
    pub published_batches: usize,
    pub publication: PublicationRunReport,
    pub runtime: RuntimeRunArtifact,
    pub artifact: IngestionRunArtifact,
}

/// Maximum number of count-only outcome categories retained in a run artifact.
pub const MAX_INGESTION_OUTCOME_SUMMARIES: usize = 3;

/// Bounded count for one outcome category without source payloads or identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestionOutcomeSummary {
    pub kind: IngestionOutcomeKind,
    pub count: usize,
}

/// Count-only source artifact observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRunCounts {
    pub records: usize,
    pub malformed_records: usize,
}

/// Count-only durable preflight observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreflightRunCounts {
    pub reserved: usize,
    pub existing: usize,
    pub conflicts: usize,
    pub stale: usize,
}

/// Count-only normalization evaluation observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationRunCounts {
    pub completed: usize,
    pub failed: usize,
}

/// Count-only compare-and-commit observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRunCounts {
    pub committed: usize,
    pub existing: usize,
    pub retry_required: usize,
    pub fence_lost: usize,
    pub application_rejected: usize,
}

/// Count-only deadline observations from the bounded admission runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutRunCounts {
    pub expired: usize,
}

/// Count-only aggregate for one completed local ingestion run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestionRunArtifact {
    pub schema_version: u32,
    pub manifest_id: String,
    pub manifest_version: String,
    pub started_at: DateTimeUtc,
    pub completed_at: DateTimeUtc,
    pub source: SourceRunCounts,
    pub preflight: PreflightRunCounts,
    pub evaluation: EvaluationRunCounts,
    pub commit: CommitRunCounts,
    pub delivery: PublicationRunReport,
    pub dead_lettered: usize,
    pub timeout: TimeoutRunCounts,
    pub outcome_summaries: [IngestionOutcomeSummary; MAX_INGESTION_OUTCOME_SUMMARIES],
}

impl IngestionRunArtifact {
    fn from_reports(
        manifest: &ResolvedRunnerManifest,
        started_at: DateTimeUtc,
        completed_at: DateTimeUtc,
        ingestion: &OfflineIngestionReport,
        execution: IngestionExecutionReport,
        delivery: PublicationRunReport,
        timed_out: usize,
    ) -> Self {
        let outcome_summaries = [
            IngestionOutcomeSummary {
                kind: IngestionOutcomeKind::Committed,
                count: ingestion.committed_records,
            },
            IngestionOutcomeSummary {
                kind: IngestionOutcomeKind::Existing,
                count: ingestion.existing_records,
            },
            IngestionOutcomeSummary {
                kind: IngestionOutcomeKind::RetryRequired,
                count: ingestion.retry_required_records,
            },
        ];
        Self {
            schema_version: 1,
            manifest_id: manifest.id.clone(),
            manifest_version: manifest.version.clone(),
            started_at,
            completed_at,
            source: SourceRunCounts {
                records: ingestion.admitted_records + ingestion.malformed_records,
                malformed_records: ingestion.malformed_records,
            },
            preflight: PreflightRunCounts {
                reserved: execution.preflight_reserved,
                existing: execution.preflight_existing,
                conflicts: execution.preflight_conflicts,
                stale: execution.preflight_stale,
            },
            evaluation: EvaluationRunCounts {
                completed: execution.evaluations_completed,
                failed: execution.evaluations_failed,
            },
            commit: CommitRunCounts {
                committed: execution.commits_committed,
                existing: execution.commits_existing,
                retry_required: execution.commits_retry_required,
                fence_lost: execution.commits_fence_lost,
                application_rejected: execution.commits_application_rejected,
            },
            delivery,
            dead_lettered: delivery.dead_lettered,
            timeout: TimeoutRunCounts { expired: timed_out },
            outcome_summaries,
        }
    }
}

/// Failures while constructing or running the local composition.
#[derive(Debug, thiserror::Error)]
pub enum LocalIngestionCompositionError {
    #[error("local composition IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    State(#[from] SourceStateError),
    #[error(transparent)]
    Build(#[from] IngestionBuildError),
    #[error(transparent)]
    SourceArtifactIdentity(#[from] SourceEventJsonlArtifactIdentityError),
    #[error("configured sink acknowledgement policy does not match the manifest")]
    SinkAcknowledgementPolicy,
    #[error(transparent)]
    Offline(#[from] OfflineIngestionError),
}
