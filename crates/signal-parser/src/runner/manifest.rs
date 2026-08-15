//! Strict authoring and resolved manifests for local structured JSONL ingestion.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::Duration;
use serde::Deserialize;

use crate::ingestion::{MAX_PAYLOAD_BYTES, SourceEvent, SourceId, SourceRevision};
use crate::normalization::{ComponentId, SemanticVersion, SourceAdapterIdentity};
use crate::state::{MAX_ACTIVE_OUTPUT_LIMIT, ReplacementPolicy};

use super::publication::{DeliveryAcknowledgementPolicy, PublicationRetryPolicy};
use super::{IngestionServiceConfig, OfflineErrorPolicy, structured_jsonl_execution_identity};
use crate::state::AdmittedExecutionIdentity;

pub const RUNNER_MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const SOURCE_EVENT_JSONL_CODEC: &str = "source-event-jsonl@1";
pub const COMMITTED_NORMALIZATION_JSONL_CODEC: &str = "committed-normalization-jsonl@1";
pub const BUILTIN_SOURCE_EVENT_JSONL_ADAPTER: &str = "source-event-jsonl@1.0.0";
pub const BUILTIN_CANONICAL_RAW_SIGNALS_DECODER: &str = "canonical-raw-signals@1.0.0";
pub const BUILTIN_STANDARD_SIGNAL_FINALIZER: &str = "standard-signal-finalizer@1.0.0";
pub const BUILTIN_SQLITE_STATE: &str = "sqlite-source-state@1.0.0";
pub const BUILTIN_COMMITTED_JSONL_SINK: &str = "committed-normalization-jsonl@1.0.0";
pub const MAX_PUBLICATION_BATCH_SIZE: usize = 256;
pub const MAX_DURATION_SECONDS: u64 = 86_400;
pub const MAX_DEADLINE_MILLISECONDS: u64 = MAX_DURATION_SECONDS * 1_000;
pub const MAX_QUEUE_DEPTH: usize = 65_536;
pub const MAX_COMPARE_COMMIT_RETRIES: usize = 16;
pub const MAX_PUBLICATION_ATTEMPTS: usize = 100;

/// Strict authoring input that must be compiled before a runner uses it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerManifest {
    pub schema_version: u32,
    pub id: String,
    pub version: String,
    pub sources: Vec<StructuredJsonlSource>,
    pub pipelines: BTreeMap<String, StructuredPipeline>,
    pub state: SqliteState,
    pub sinks: Vec<CommittedJsonlSink>,
    pub limits: RunnerLimits,
    pub publication: PublicationPolicy,
    pub malformed_records: MalformedRecordPolicy,
}

impl RunnerManifest {
    pub fn from_toml_str(input: &str) -> Result<Self, ManifestError> {
        let manifest: Self =
            toml::from_str(input).map_err(|error| ManifestError::Toml(error.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        self.compile().map(|_| ())
    }

    pub fn compile(&self) -> Result<ResolvedRunnerManifest, ManifestError> {
        if self.schema_version != RUNNER_MANIFEST_SCHEMA_VERSION {
            return Err(ManifestError::UnsupportedSchemaVersion(self.schema_version));
        }
        validate_identifier(&self.id, "manifest ID")?;
        validate_semantic_version(&self.version, "manifest version")?;
        if self.sources.is_empty() {
            return Err(ManifestError::MissingSection("sources"));
        }
        if self.pipelines.is_empty() {
            return Err(ManifestError::MissingSection("pipelines"));
        }
        if self.sinks.len() != 1 {
            return Err(ManifestError::LocalCompositionRequiresOneSink(
                self.sinks.len(),
            ));
        }
        self.state.validate()?;
        self.limits.validate()?;
        self.publication.validate()?;

        let mut source_ids = std::collections::BTreeSet::new();
        let mut configured_source_ids = std::collections::BTreeSet::new();
        let mut sources = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            source.validate(&self.pipelines)?;
            if !source_ids.insert(source.id.clone()) {
                return Err(ManifestError::DuplicateBindingId {
                    section: "sources",
                    id: source.id.clone(),
                });
            }
            if !configured_source_ids.insert(source.source_id.clone()) {
                return Err(ManifestError::DuplicateSourceId(source.source_id.clone()));
            }
            sources.push(ResolvedSourceBinding {
                id: source.id.clone(),
                adapter: source.adapter.clone(),
                source_path: source.path.clone(),
                source_id: SourceId::new(source.source_id.clone())
                    .map_err(|error| ManifestError::InvalidSourceId(error.to_string()))?,
                pipeline: source.pipeline.clone(),
            });
        }

        let mut pipelines = BTreeMap::new();
        for (id, pipeline) in &self.pipelines {
            validate_identifier(id, "pipeline ID")?;
            pipeline.validate(id)?;
            pipelines.insert(
                id.clone(),
                ResolvedPipelineBinding {
                    id: id.clone(),
                    kind: pipeline.kind.clone(),
                    decoder: pipeline.decoder.clone(),
                    draft_validation: pipeline.draft_validation.clone(),
                    finalizer: pipeline.finalizer.clone(),
                },
            );
        }

        let sink = &self.sinks[0];
        sink.validate()?;
        let execution_identity = structured_jsonl_execution_identity(sources[0].source_id.clone())
            .map_err(|error| ManifestError::CrossSection {
                message: format!("built-in execution identity could not resolve: {error}"),
            })?;
        let resolved = ResolvedRunnerManifest {
            schema_version: self.schema_version,
            id: self.id.clone(),
            version: self.version.clone(),
            sources,
            pipelines,
            state_backend: self.state.backend.clone(),
            state_path: self.state.path.clone(),
            sink: ResolvedSinkBinding {
                id: sink.id.clone(),
                component: sink.component.clone(),
                codec: sink.codec.clone(),
                acknowledgement: sink.acknowledgement,
                path: sink.path.clone(),
            },
            limits: self.limits.clone(),
            publication: self.publication.clone(),
            malformed_records: self.malformed_records,
            execution_identity,
        };
        resolved.validate_cross_section()?;
        Ok(resolved)
    }

    pub fn wiring(&self) -> Result<RunnerManifestWiring, ManifestError> {
        self.compile()?.wiring()
    }

    pub fn validate_source_event(&self, event: &SourceEvent) -> Result<(), ManifestError> {
        self.compile()?.validate_source_event(event)
    }
}

/// One exact built-in SourceEvent JSONL source binding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredJsonlSource {
    pub id: String,
    pub adapter: String,
    pub codec: String,
    pub path: PathBuf,
    pub source_id: String,
    pub pipeline: String,
    pub revision_policy: SourceRevisionPolicy,
}

impl StructuredJsonlSource {
    fn validate(
        &self,
        pipelines: &BTreeMap<String, StructuredPipeline>,
    ) -> Result<(), ManifestError> {
        validate_identifier(&self.id, "source binding ID")?;
        validate_exact_component(
            "source adapter",
            &self.adapter,
            BUILTIN_SOURCE_EVENT_JSONL_ADAPTER,
        )?;
        if self.codec != SOURCE_EVENT_JSONL_CODEC {
            return Err(ManifestError::UnsupportedSourceCodec(self.codec.clone()));
        }
        validate_local_path(&self.path, "source path")?;
        SourceId::new(self.source_id.clone())
            .map_err(|error| ManifestError::InvalidSourceId(error.to_string()))?;
        if !pipelines.contains_key(&self.pipeline) {
            return Err(ManifestError::UnresolvedPipeline(self.pipeline.clone()));
        }
        if self.revision_policy != SourceRevisionPolicy::Monotonic {
            return Err(ManifestError::UnsupportedRevisionPolicy);
        }
        Ok(())
    }
}

/// The only structured pipeline supported by the local built-in composition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredPipeline {
    pub kind: String,
    pub decoder: String,
    pub draft_validation: String,
    pub finalizer: String,
}

impl StructuredPipeline {
    fn validate(&self, _id: &str) -> Result<(), ManifestError> {
        if self.kind != "structured" {
            return Err(ManifestError::UnsupportedPipelineKind(self.kind.clone()));
        }
        validate_exact_component(
            "decoder",
            &self.decoder,
            BUILTIN_CANONICAL_RAW_SIGNALS_DECODER,
        )?;
        if self.draft_validation != "none" {
            return Err(ManifestError::UnsupportedDraftValidation(
                self.draft_validation.clone(),
            ));
        }
        validate_exact_component(
            "finalizer",
            &self.finalizer,
            BUILTIN_STANDARD_SIGNAL_FINALIZER,
        )
    }
}

/// The revision evidence required for structured JSONL records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRevisionPolicy {
    Monotonic,
}

/// Local SQLite state-store configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteState {
    pub backend: String,
    pub path: PathBuf,
}

impl SqliteState {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_exact_component("state backend", &self.backend, BUILTIN_SQLITE_STATE)?;
        validate_local_path(&self.path, "SQLite state path")
    }
}

/// Local append-only committed-normalization JSONL sink configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedJsonlSink {
    pub id: String,
    pub component: String,
    pub codec: String,
    pub path: PathBuf,
    pub acknowledgement: DeliveryAcknowledgementPolicy,
}

impl CommittedJsonlSink {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_identifier(&self.id, "sink binding ID")?;
        validate_exact_component("sink", &self.component, BUILTIN_COMMITTED_JSONL_SINK)?;
        if self.codec != COMMITTED_NORMALIZATION_JSONL_CODEC {
            return Err(ManifestError::UnsupportedSinkCodec(self.codec.clone()));
        }
        validate_local_path(&self.path, "committed JSONL sink path")?;
        Ok(())
    }
}

/// Bounded admission, execution, commit, and delivery configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerLimits {
    pub admission_queue_depth: usize,
    pub publication_queue_depth: usize,
    pub maximum_payload_bytes: usize,
    pub admission_deadline_ms: u64,
    pub event_deadline_ms: u64,
    pub stage_deadline_ms: u64,
    pub compare_commit_retries: usize,
    pub reservation_ttl_seconds: u64,
    pub maximum_active_outputs: usize,
    pub replacement_policy: ManifestReplacementPolicy,
}

impl RunnerLimits {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_limit(
            self.admission_queue_depth,
            "admission_queue_depth",
            1,
            MAX_QUEUE_DEPTH,
        )?;
        validate_limit(
            self.publication_queue_depth,
            "publication_queue_depth",
            1,
            MAX_QUEUE_DEPTH,
        )?;
        validate_limit(
            self.maximum_payload_bytes,
            "maximum_payload_bytes",
            1,
            MAX_PAYLOAD_BYTES,
        )?;
        validate_duration_ms(self.admission_deadline_ms, "admission_deadline_ms")?;
        validate_duration_ms(self.event_deadline_ms, "event_deadline_ms")?;
        validate_duration_ms(self.stage_deadline_ms, "stage_deadline_ms")?;
        if self.stage_deadline_ms > self.event_deadline_ms {
            return Err(ManifestError::CrossSection {
                message: "stage_deadline_ms must not exceed event_deadline_ms".to_string(),
            });
        }
        validate_limit(
            self.compare_commit_retries,
            "compare_commit_retries",
            0,
            MAX_COMPARE_COMMIT_RETRIES,
        )?;
        validate_duration(self.reservation_ttl_seconds, "reservation_ttl_seconds")?;
        validate_limit(
            self.maximum_active_outputs,
            "maximum_active_outputs",
            1,
            MAX_ACTIVE_OUTPUT_LIMIT,
        )
    }
}

/// Publication retry and lease limits for the configured committed-batch sink.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationPolicy {
    pub lease_ttl_seconds: u64,
    pub batch_size: usize,
    pub attempt_deadline_ms: u64,
    pub initial_backoff_ms: u64,
    pub maximum_backoff_ms: u64,
    pub maximum_attempts: usize,
    pub dead_letter_after_ms: u64,
}

impl PublicationPolicy {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_duration(self.lease_ttl_seconds, "publication.lease_ttl_seconds")?;
        validate_limit(
            self.batch_size,
            "publication.batch_size",
            1,
            MAX_PUBLICATION_BATCH_SIZE,
        )?;
        validate_duration_ms(self.attempt_deadline_ms, "publication.attempt_deadline_ms")?;
        if self.attempt_deadline_ms > self.lease_ttl_seconds.saturating_mul(1_000) {
            return Err(ManifestError::CrossSection {
                message:
                    "publication.attempt_deadline_ms must not exceed publication.lease_ttl_seconds"
                        .to_string(),
            });
        }
        validate_duration_ms(self.initial_backoff_ms, "publication.initial_backoff_ms")?;
        validate_duration_ms(self.maximum_backoff_ms, "publication.maximum_backoff_ms")?;
        if self.initial_backoff_ms > self.maximum_backoff_ms {
            return Err(ManifestError::CrossSection {
                message:
                    "publication.initial_backoff_ms must not exceed publication.maximum_backoff_ms"
                        .to_string(),
            });
        }
        validate_limit(
            self.maximum_attempts,
            "publication.maximum_attempts",
            1,
            MAX_PUBLICATION_ATTEMPTS,
        )?;
        validate_duration_ms(
            self.dead_letter_after_ms,
            "publication.dead_letter_after_ms",
        )
    }
}

/// Replacement semantics passed to the durable state store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestReplacementPolicy {
    Patch,
    ReplaceCurrentSourceKey,
}

impl From<ManifestReplacementPolicy> for ReplacementPolicy {
    fn from(value: ManifestReplacementPolicy) -> Self {
        match value {
            ManifestReplacementPolicy::Patch => Self::Patch,
            ManifestReplacementPolicy::ReplaceCurrentSourceKey => Self::ReplaceCurrentSourceKey,
        }
    }
}

/// The required response when a JSONL physical record cannot be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MalformedRecordPolicy {
    Stop,
    Continue,
}

impl From<MalformedRecordPolicy> for OfflineErrorPolicy {
    fn from(value: MalformedRecordPolicy) -> Self {
        match value {
            MalformedRecordPolicy::Stop => Self::Stop,
            MalformedRecordPolicy::Continue => Self::Continue,
        }
    }
}

/// Immutable, fully resolved local runner manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRunnerManifest {
    pub schema_version: u32,
    pub id: String,
    pub version: String,
    pub sources: Vec<ResolvedSourceBinding>,
    pub pipelines: BTreeMap<String, ResolvedPipelineBinding>,
    pub state_backend: String,
    pub state_path: PathBuf,
    pub sink: ResolvedSinkBinding,
    pub limits: RunnerLimits,
    pub publication: PublicationPolicy,
    pub malformed_records: MalformedRecordPolicy,
    pub execution_identity: AdmittedExecutionIdentity,
}

impl ResolvedRunnerManifest {
    pub fn validate(&self) -> Result<(), ManifestError> {
        self.validate_cross_section()
    }

    pub fn wiring(&self) -> Result<RunnerManifestWiring, ManifestError> {
        self.validate_cross_section()?;
        let source = self.local_source()?;
        Ok(RunnerManifestWiring {
            source_path: source.source_path.clone(),
            source_id: source.source_id.clone(),
            state_path: self.state_path.clone(),
            sink_path: self.sink.path.clone(),
            sink_binding_id: self.sink.id.clone(),
            ingestion_config: IngestionServiceConfig {
                reservation_ttl: Duration::seconds(self.limits.reservation_ttl_seconds as i64),
                replacement_policy: self.limits.replacement_policy.into(),
                maximum_active_outputs: self.limits.maximum_active_outputs,
                publication_sink: Some(self.sink.id.clone()),
                compare_and_commit_retries: self.limits.compare_commit_retries,
            },
            malformed_records: self.malformed_records.into(),
            admission_queue_depth: self.limits.admission_queue_depth,
            event_deadline: std::time::Duration::from_millis(self.limits.event_deadline_ms),
            publication_batch_size: self.publication.batch_size,
            publication_retry_policy: PublicationRetryPolicy::with_operational_limits(
                self.publication.maximum_attempts as u32,
                Duration::seconds(self.publication.lease_ttl_seconds as i64),
                Duration::milliseconds(self.publication.attempt_deadline_ms as i64),
                Duration::milliseconds(self.publication.initial_backoff_ms as i64),
                Duration::milliseconds(self.publication.maximum_backoff_ms as i64),
                Duration::milliseconds(self.publication.dead_letter_after_ms as i64),
            )
            .expect("validated publication policy"),
            source_adapter: self.source_adapter_identity()?,
            execution_identity: self.execution_identity.clone(),
        })
    }

    pub fn source_adapter_identity(&self) -> Result<SourceAdapterIdentity, ManifestError> {
        self.validate_cross_section()?;
        Ok(SourceAdapterIdentity::without_config(
            ComponentId::try_new("source-event-jsonl", "adapter ID")
                .expect("the built-in adapter ID is valid"),
            SemanticVersion::new(1, 0, 0),
        ))
    }

    pub fn validate_source_event(&self, event: &SourceEvent) -> Result<(), ManifestError> {
        self.validate_cross_section()?;
        let source = self.local_source()?;
        if event.key().source() != &source.source_id {
            return Err(ManifestError::UnexpectedSource {
                expected: source.source_id.as_str().to_string(),
                actual: event.key().source().as_str().to_string(),
            });
        }
        match event.revision() {
            SourceRevision::Monotonic(_) => Ok(()),
            SourceRevision::Opaque(_) => Err(ManifestError::OpaqueRevision),
            SourceRevision::Unversioned => Err(ManifestError::UnversionedRevision),
        }
    }

    fn local_source(&self) -> Result<&ResolvedSourceBinding, ManifestError> {
        if self.sources.len() != 1 {
            return Err(ManifestError::LocalCompositionRequiresOneSource(
                self.sources.len(),
            ));
        }
        Ok(&self.sources[0])
    }

    fn validate_cross_section(&self) -> Result<(), ManifestError> {
        if self.sources.len() != 1 {
            return Err(ManifestError::LocalCompositionRequiresOneSource(
                self.sources.len(),
            ));
        }
        let source = &self.sources[0];
        validate_exact_component(
            "source adapter",
            &source.adapter,
            BUILTIN_SOURCE_EVENT_JSONL_ADAPTER,
        )?;
        let pipeline = self
            .pipelines
            .get(&source.pipeline)
            .ok_or_else(|| ManifestError::UnresolvedPipeline(source.pipeline.clone()))?;
        if pipeline.kind != "structured" {
            return Err(ManifestError::UnsupportedPipelineKind(
                pipeline.kind.clone(),
            ));
        }
        validate_exact_component(
            "decoder",
            &pipeline.decoder,
            BUILTIN_CANONICAL_RAW_SIGNALS_DECODER,
        )?;
        if pipeline.draft_validation != "none" {
            return Err(ManifestError::UnsupportedDraftValidation(
                pipeline.draft_validation.clone(),
            ));
        }
        validate_exact_component(
            "finalizer",
            &pipeline.finalizer,
            BUILTIN_STANDARD_SIGNAL_FINALIZER,
        )?;
        validate_exact_component("state backend", &self.state_backend, BUILTIN_SQLITE_STATE)?;
        validate_exact_component("sink", &self.sink.component, BUILTIN_COMMITTED_JSONL_SINK)?;
        if self.sink.codec != COMMITTED_NORMALIZATION_JSONL_CODEC {
            return Err(ManifestError::UnsupportedSinkCodec(self.sink.codec.clone()));
        }
        if self.limits.maximum_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(ManifestError::CrossSection {
                message: "maximum_payload_bytes exceeds the source-event payload contract"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// A source binding after exact built-in adapter and pipeline resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSourceBinding {
    pub id: String,
    pub adapter: String,
    pub source_path: PathBuf,
    pub source_id: SourceId,
    pub pipeline: String,
}

/// A pipeline binding after exact built-in component resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPipelineBinding {
    pub id: String,
    pub kind: String,
    pub decoder: String,
    pub draft_validation: String,
    pub finalizer: String,
}

/// A sink binding after exact built-in sink resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSinkBinding {
    pub id: String,
    pub component: String,
    pub codec: String,
    pub acknowledgement: DeliveryAcknowledgementPolicy,
    pub path: PathBuf,
}

/// Validated values that the local composition uses to construct its adapters.
#[derive(Debug, Clone)]
pub struct RunnerManifestWiring {
    pub source_path: PathBuf,
    pub source_id: SourceId,
    pub source_adapter: SourceAdapterIdentity,
    pub execution_identity: AdmittedExecutionIdentity,
    pub state_path: PathBuf,
    pub sink_path: PathBuf,
    pub sink_binding_id: String,
    pub ingestion_config: IngestionServiceConfig,
    pub malformed_records: OfflineErrorPolicy,
    pub admission_queue_depth: usize,
    pub event_deadline: std::time::Duration,
    pub publication_batch_size: usize,
    pub publication_retry_policy: PublicationRetryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("invalid runner manifest TOML: {0}")]
    Toml(String),
    #[error("unsupported runner manifest schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("{0} is required")]
    MissingSection(&'static str),
    #[error("invalid {field}")]
    InvalidIdentifier { field: &'static str },
    #[error("invalid {field}: expected major.minor.patch")]
    InvalidSemanticVersion { field: &'static str },
    #[error("unsupported structured JSONL source codec {0}")]
    UnsupportedSourceCodec(String),
    #[error("unsupported committed JSONL sink codec {0}")]
    UnsupportedSinkCodec(String),
    #[error("unsupported {kind} component reference {actual}; expected {expected}")]
    UnsupportedComponentReference {
        kind: &'static str,
        actual: String,
        expected: &'static str,
    },
    #[error("pipeline {0} is not configured")]
    UnresolvedPipeline(String),
    #[error("unsupported pipeline kind {0}")]
    UnsupportedPipelineKind(String),
    #[error("unsupported draft validation {0}")]
    UnsupportedDraftValidation(String),
    #[error("unsupported sink acknowledgement {0}")]
    UnsupportedSinkAcknowledgement(String),
    #[error("duplicate {section} binding ID {id}")]
    DuplicateBindingId { section: &'static str, id: String },
    #[error("duplicate configured source ID {0}")]
    DuplicateSourceId(String),
    #[error("local composition requires exactly one source, got {0}")]
    LocalCompositionRequiresOneSource(usize),
    #[error("local composition requires exactly one sink, got {0}")]
    LocalCompositionRequiresOneSink(usize),
    #[error("invalid configured source ID: {0}")]
    InvalidSourceId(String),
    #[error("unsupported source revision policy")]
    UnsupportedRevisionPolicy,
    #[error("source event uses an unsupported opaque revision")]
    OpaqueRevision,
    #[error("source event must use a monotonic revision")]
    UnversionedRevision,
    #[error("source event belongs to {actual}, but the manifest requires {expected}")]
    UnexpectedSource { expected: String, actual: String },
    #[error("{field} must be a local non-empty path")]
    InvalidLocalPath { field: &'static str },
    #[error("{field} must be between {minimum} and {maximum}, got {actual}")]
    LimitOutOfRange {
        field: &'static str,
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    #[error("manifest cross-section validation failed: {message}")]
    CrossSection { message: String },
}

fn validate_exact_component(
    kind: &'static str,
    actual: &str,
    expected: &'static str,
) -> Result<(), ManifestError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ManifestError::UnsupportedComponentReference {
            kind,
            actual: actual.to_string(),
            expected,
        })
    }
}

fn validate_identifier(value: &str, field: &'static str) -> Result<(), ManifestError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ManifestError::InvalidIdentifier { field });
    }
    Ok(())
}

fn validate_semantic_version(value: &str, field: &'static str) -> Result<(), ManifestError> {
    if value.split('.').count() == 3
        && value
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    {
        Ok(())
    } else {
        Err(ManifestError::InvalidSemanticVersion { field })
    }
}

fn validate_duration(value: u64, field: &'static str) -> Result<(), ManifestError> {
    validate_limit(
        usize::try_from(value).unwrap_or(usize::MAX),
        field,
        1,
        MAX_DURATION_SECONDS as usize,
    )
}

fn validate_duration_ms(value: u64, field: &'static str) -> Result<(), ManifestError> {
    validate_limit(
        usize::try_from(value).unwrap_or(usize::MAX),
        field,
        1,
        MAX_DEADLINE_MILLISECONDS as usize,
    )
}

fn validate_limit(
    value: usize,
    field: &'static str,
    minimum: usize,
    maximum: usize,
) -> Result<(), ManifestError> {
    if !(minimum..=maximum).contains(&value) {
        return Err(ManifestError::LimitOutOfRange {
            field,
            minimum,
            maximum,
            actual: value,
        });
    }
    Ok(())
}

fn validate_local_path(path: &Path, field: &'static str) -> Result<(), ManifestError> {
    let value = path.as_os_str().to_string_lossy();
    if value.is_empty()
        || value.contains("\0")
        || value.bytes().any(|byte| byte.is_ascii_control())
        || value.contains("://")
        || value.starts_with("file:")
    {
        return Err(ManifestError::InvalidLocalPath { field });
    }
    Ok(())
}
