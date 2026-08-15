use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::ingestion::{
    BoundedText, DateTimeUtc, ExternalEventId, ExternalThreadId, MetadataKey, MetadataValue,
    SourceEvent, SourceEventKey, SourceId, SourceMetadata, SourceOperation, SourcePayload,
    SourceRevision, SourceTimestamp, SourceTimestampQuality, SourceValidationError, TextFormat,
    TextPayload,
};
use crate::normalization::{
    BaseContextSnapshot, ByteLimit, CanonicalComponentConfig, CanonicalWriter, ComponentBindError,
    ComponentConfigSchemaRef, ComponentDescriptor, ComponentId, ComponentKind, ComponentReport,
    ComponentResult, ContractValueError, Diagnostic, DiagnosticRedaction, DiagnosticSet,
    DiagnosticSeverity, EmptyOutputPolicy, EvaluationFailureClass, EvaluationRetrySafety,
    HistoryRequirement, IgnoreReason, ItemLimit, ParentRequirement, PipelineContextRequirements,
    PreNormalizedProducer, PreNormalizedProducerBinding, PreNormalizedSignalBatch, RejectionReason,
    SemanticVersion, SourceAdapterIdentity, StageExecutionFailure, bind_pre_normalized_producer,
};
use crate::registry::ParserRegistry;
use crate::state::DurableDeliveryIdentity;
use crate::types::{ParseContext, ParsedAction, RawTgMessage};

const TELEGRAM_EVIDENCE_SCHEMA_VERSION: u32 = 1;
const TELEGRAM_EVIDENCE_MAX_BYTES: usize = 65_536;
const TELEGRAM_TEXT_EVIDENCE_MAX_BYTES: usize = 1_024;
const TELEGRAM_DELIVERY_ID_MAX_BYTES: usize = 512;
const TELEGRAM_DELETE_MAX_ITEMS: usize = 256;
const TELEGRAM_HISTORY_MAX_ITEMS: usize = 64;
const TELEGRAM_HISTORY_FACT_MAX_BYTES: u64 = 1_114_112;
const TELEGRAM_THREAD_LABEL: &str = "telegram-thread";

const TELEGRAM_LEGACY_CONFIG_SCHEMA: &str = "quant-system/telegram-legacy-producer-config@1";

#[derive(Debug, thiserror::Error)]
pub enum TelegramAdapterError {
    #[error(transparent)]
    Source(#[from] SourceValidationError),
    #[error(transparent)]
    Contract(#[from] ContractValueError),
    #[error(transparent)]
    Binding(#[from] ComponentBindError),
    #[error("Telegram evidence serialization failed: {0}")]
    EvidenceSerialization(#[from] serde_json::Error),
    #[error("Telegram evidence schema version {0} is unsupported")]
    UnsupportedEvidenceVersion(u32),
    #[error("Telegram evidence exceeds {maximum} bytes (got {actual})")]
    EvidenceTooLarge { maximum: usize, actual: usize },
    #[error("{field} exceeds {maximum} bytes (got {actual})")]
    FieldTooLarge {
        field: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("Telegram delete contains more than {maximum} message IDs (got {actual})")]
    DeleteLimitExceeded { maximum: usize, actual: usize },
    #[error("Telegram parser history requirement exceeds {maximum} items (got {actual})")]
    HistoryLimitExceeded { maximum: usize, actual: usize },
    #[error("invalid Telegram component configuration: {0}")]
    ComponentConfiguration(String),
}

#[derive(Debug, Clone)]
pub struct TelegramAdapterConfig {
    source_id: SourceId,
    allow_legacy_naive_utc: bool,
    allow_legacy_zero_message_id: bool,
}

impl TelegramAdapterConfig {
    pub fn new(
        source_id: SourceId,
        allow_legacy_naive_utc: bool,
        allow_legacy_zero_message_id: bool,
    ) -> Self {
        Self {
            source_id,
            allow_legacy_naive_utc,
            allow_legacy_zero_message_id,
        }
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramAdapterPath {
    Batch,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramTimestampRule {
    ExplicitOffset,
    LegacyNaiveUtc,
    RelayEpoch,
    ReceptionFallbackMissing,
    ReceptionFallbackInvalid,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramTimestampEvidence {
    original_text: Option<String>,
    relay_epoch_bits: Option<u64>,
    rule: TelegramTimestampRule,
}

impl TelegramTimestampEvidence {
    pub fn original_text(&self) -> Option<&str> {
        self.original_text.as_deref()
    }

    pub fn relay_epoch_bits(&self) -> Option<u64> {
        self.relay_epoch_bits
    }

    pub fn rule(&self) -> TelegramTimestampRule {
        self.rule
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramSourceEvidenceV1 {
    schema_version: u32,
    path: TelegramAdapterPath,
    chat_id: i64,
    message_id: Option<i64>,
    reply_to: Option<i64>,
    operation: SourceOperation,
    timestamp: TelegramTimestampEvidence,
    ingress_delivery_id: Option<String>,
}

impl TelegramSourceEvidenceV1 {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        path: TelegramAdapterPath,
        chat_id: i64,
        message_id: Option<i64>,
        reply_to: Option<i64>,
        operation: SourceOperation,
        original_timestamp: Option<String>,
        relay_epoch_bits: Option<u64>,
        timestamp_rule: TelegramTimestampRule,
        ingress_delivery_id: Option<String>,
    ) -> Result<Self, TelegramAdapterError> {
        if let Some(value) = original_timestamp.as_ref() {
            validate_adapter_field(
                value,
                "Telegram timestamp evidence",
                TELEGRAM_TEXT_EVIDENCE_MAX_BYTES,
            )?;
        }
        if let Some(value) = ingress_delivery_id.as_ref() {
            validate_adapter_field(
                value,
                "Telegram delivery identity",
                TELEGRAM_DELIVERY_ID_MAX_BYTES,
            )?;
        }
        Ok(Self {
            schema_version: TELEGRAM_EVIDENCE_SCHEMA_VERSION,
            path,
            chat_id,
            message_id,
            reply_to,
            operation,
            timestamp: TelegramTimestampEvidence {
                original_text: original_timestamp,
                relay_epoch_bits,
                rule: timestamp_rule,
            },
            ingress_delivery_id,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, TelegramAdapterError> {
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > TELEGRAM_EVIDENCE_MAX_BYTES {
            return Err(TelegramAdapterError::EvidenceTooLarge {
                maximum: TELEGRAM_EVIDENCE_MAX_BYTES,
                actual: encoded.len(),
            });
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, TelegramAdapterError> {
        if encoded.len() > TELEGRAM_EVIDENCE_MAX_BYTES {
            return Err(TelegramAdapterError::EvidenceTooLarge {
                maximum: TELEGRAM_EVIDENCE_MAX_BYTES,
                actual: encoded.len(),
            });
        }
        let value: Self = serde_json::from_slice(encoded)?;
        if value.schema_version != TELEGRAM_EVIDENCE_SCHEMA_VERSION {
            return Err(TelegramAdapterError::UnsupportedEvidenceVersion(
                value.schema_version,
            ));
        }
        Ok(value)
    }

    pub fn path(&self) -> TelegramAdapterPath {
        self.path
    }

    pub fn chat_id(&self) -> i64 {
        self.chat_id
    }

    pub fn message_id(&self) -> Option<i64> {
        self.message_id
    }

    pub fn reply_to(&self) -> Option<i64> {
        self.reply_to
    }

    pub fn operation(&self) -> SourceOperation {
        self.operation
    }

    pub fn timestamp(&self) -> &TelegramTimestampEvidence {
        &self.timestamp
    }

    pub fn ingress_delivery_id(&self) -> Option<&str> {
        self.ingress_delivery_id.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramIgnoreReason {
    MissingMessage,
    EmptyDelete,
}

#[derive(Debug)]
pub enum TelegramAdaptationOutcome {
    Accepted {
        event: Box<SourceEvent>,
        evidence: TelegramSourceEvidenceV1,
        delivery_identity: DurableDeliveryIdentity,
    },
    Ignored {
        evidence: TelegramSourceEvidenceV1,
        reason: TelegramIgnoreReason,
    },
    Rejected {
        evidence: TelegramSourceEvidenceV1,
        diagnostic: Diagnostic,
    },
}

#[derive(Debug, Clone)]
pub struct TelegramBatchPosition {
    artifact: String,
    ordinal: u64,
}

impl TelegramBatchPosition {
    pub fn try_new(
        artifact: impl Into<String>,
        ordinal: u64,
    ) -> Result<Self, TelegramAdapterError> {
        let artifact = artifact.into();
        validate_adapter_field(
            &artifact,
            "Telegram batch artifact identity",
            TELEGRAM_DELIVERY_ID_MAX_BYTES,
        )?;
        Ok(Self { artifact, ordinal })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramRelayOperation {
    New,
    Edit,
    Delete,
}

#[derive(Debug, Clone)]
pub struct TelegramRelayInput {
    pub operation: TelegramRelayOperation,
    pub chat_id: i64,
    pub message_id: Option<i64>,
    pub message: Option<String>,
    pub epoch_seconds: Option<f64>,
    pub reply_to: Option<i64>,
    pub delete_ids: Vec<i64>,
    pub delivery_id: String,
}

impl TelegramRelayInput {
    pub fn try_new_message(
        operation: TelegramRelayOperation,
        chat_id: i64,
        message_id: Option<i64>,
        message: Option<String>,
        epoch_seconds: Option<f64>,
        reply_to: Option<i64>,
        delivery_id: impl Into<String>,
    ) -> Result<Self, TelegramAdapterError> {
        if operation == TelegramRelayOperation::Delete {
            return Err(TelegramAdapterError::ComponentConfiguration(
                "message input cannot use the delete operation".to_string(),
            ));
        }
        let delivery_id = delivery_id.into();
        validate_adapter_field(
            &delivery_id,
            "Telegram relay delivery identity",
            TELEGRAM_DELIVERY_ID_MAX_BYTES,
        )?;
        Ok(Self {
            operation,
            chat_id,
            message_id,
            message,
            epoch_seconds,
            reply_to,
            delete_ids: Vec::new(),
            delivery_id,
        })
    }

    pub fn try_new_delete(
        chat_id: i64,
        delete_ids: Vec<i64>,
        delivery_id: impl Into<String>,
    ) -> Result<Self, TelegramAdapterError> {
        if delete_ids.len() > TELEGRAM_DELETE_MAX_ITEMS {
            return Err(TelegramAdapterError::DeleteLimitExceeded {
                maximum: TELEGRAM_DELETE_MAX_ITEMS,
                actual: delete_ids.len(),
            });
        }
        let delivery_id = delivery_id.into();
        validate_adapter_field(
            &delivery_id,
            "Telegram relay delivery identity",
            TELEGRAM_DELIVERY_ID_MAX_BYTES,
        )?;
        Ok(Self {
            operation: TelegramRelayOperation::Delete,
            chat_id,
            message_id: None,
            message: None,
            epoch_seconds: None,
            reply_to: None,
            delete_ids,
            delivery_id,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TelegramBatchSourceAdapter {
    config: TelegramAdapterConfig,
    source_adapter: SourceAdapterIdentity,
}

impl TelegramBatchSourceAdapter {
    pub fn try_new(config: TelegramAdapterConfig) -> Result<Self, TelegramAdapterError> {
        let source_adapter = source_adapter_identity(&config, TelegramAdapterPath::Batch)?;
        Ok(Self {
            config,
            source_adapter,
        })
    }

    pub fn source_adapter_identity(&self) -> &SourceAdapterIdentity {
        &self.source_adapter
    }

    pub fn adapt(
        &self,
        message: &RawTgMessage,
        received_at: DateTimeUtc,
        position: TelegramBatchPosition,
    ) -> Result<TelegramAdaptationOutcome, TelegramAdapterError> {
        let parsed = parse_batch_timestamp(&message.ts, self.config.allow_legacy_naive_utc);
        let (occurred_at, quality, rule) = match parsed {
            Ok(value) => value,
            Err(reason) => {
                let evidence = TelegramSourceEvidenceV1::try_new(
                    TelegramAdapterPath::Batch,
                    message.chat_id,
                    Some(message.msg_id),
                    message.reply_to,
                    SourceOperation::Upsert,
                    Some(message.ts.clone()),
                    None,
                    TelegramTimestampRule::Rejected,
                    Some(format!("{}:{}", position.artifact, position.ordinal)),
                )?;
                return Ok(TelegramAdaptationOutcome::Rejected {
                    evidence,
                    diagnostic: adaptation_diagnostic("telegram_invalid_timestamp", &reason)?,
                });
            }
        };
        let evidence = TelegramSourceEvidenceV1::try_new(
            TelegramAdapterPath::Batch,
            message.chat_id,
            Some(message.msg_id),
            message.reply_to,
            SourceOperation::Upsert,
            Some(message.ts.clone()),
            None,
            rule,
            Some(format!("{}:{}", position.artifact, position.ordinal)),
        )?;
        let event = build_event(
            &self.config,
            TelegramEventInput {
                chat_id: message.chat_id,
                message_id: message.msg_id,
                reply_to: message.reply_to,
                operation: SourceOperation::Upsert,
                occurred_at: SourceTimestamp::new(occurred_at, quality),
                received_at,
                payload: SourcePayload::Text(TextPayload::new(
                    BoundedText::new(message.message.clone())?,
                    TextFormat::Plain,
                    None,
                )),
            },
        )?;
        Ok(TelegramAdaptationOutcome::Accepted {
            event: Box::new(event),
            evidence,
            delivery_identity: DurableDeliveryIdentity::OfflinePosition {
                artifact: position.artifact,
                ordinal: position.ordinal,
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct TelegramRelaySourceAdapter {
    config: TelegramAdapterConfig,
    source_adapter: SourceAdapterIdentity,
}

impl TelegramRelaySourceAdapter {
    pub fn try_new(config: TelegramAdapterConfig) -> Result<Self, TelegramAdapterError> {
        let source_adapter = source_adapter_identity(&config, TelegramAdapterPath::Relay)?;
        Ok(Self {
            config,
            source_adapter,
        })
    }

    pub fn source_adapter_identity(&self) -> &SourceAdapterIdentity {
        &self.source_adapter
    }

    pub fn adapt(
        &self,
        input: TelegramRelayInput,
        received_at: DateTimeUtc,
    ) -> Result<Vec<TelegramAdaptationOutcome>, TelegramAdapterError> {
        match input.operation {
            TelegramRelayOperation::New | TelegramRelayOperation::Edit => self
                .adapt_message(input, received_at)
                .map(|value| vec![value]),
            TelegramRelayOperation::Delete => self.adapt_delete(input, received_at),
        }
    }

    fn adapt_message(
        &self,
        input: TelegramRelayInput,
        received_at: DateTimeUtc,
    ) -> Result<TelegramAdaptationOutcome, TelegramAdapterError> {
        let operation = match input.operation {
            TelegramRelayOperation::New => SourceOperation::Create,
            TelegramRelayOperation::Edit => SourceOperation::Update,
            TelegramRelayOperation::Delete => unreachable!("delete is dispatched separately"),
        };
        let message_id = match input.message_id {
            Some(value) => value,
            None if self.config.allow_legacy_zero_message_id => 0,
            None => {
                let evidence =
                    relay_evidence(&input, operation, None, TelegramTimestampRule::Rejected)?;
                return Ok(TelegramAdaptationOutcome::Rejected {
                    evidence,
                    diagnostic: adaptation_diagnostic(
                        "telegram_missing_message_id",
                        "relay NEW or EDIT requires a message ID",
                    )?,
                });
            }
        };
        let Some(message) = input.message.as_ref().filter(|value| !value.is_empty()) else {
            let evidence = relay_evidence(
                &input,
                operation,
                Some(message_id),
                TelegramTimestampRule::Rejected,
            )?;
            return Ok(TelegramAdaptationOutcome::Ignored {
                evidence,
                reason: TelegramIgnoreReason::MissingMessage,
            });
        };
        let (occurred_at, quality, rule) = relay_timestamp(input.epoch_seconds, received_at);
        let evidence = relay_evidence(&input, operation, Some(message_id), rule)?;
        let event = build_event(
            &self.config,
            TelegramEventInput {
                chat_id: input.chat_id,
                message_id,
                reply_to: input.reply_to,
                operation,
                occurred_at: SourceTimestamp::new(occurred_at, quality),
                received_at,
                payload: SourcePayload::Text(TextPayload::new(
                    BoundedText::new(message.clone())?,
                    TextFormat::Plain,
                    None,
                )),
            },
        )?;
        Ok(TelegramAdaptationOutcome::Accepted {
            event: Box::new(event),
            evidence,
            delivery_identity: DurableDeliveryIdentity::Stable(format!(
                "telegram-relay-v1:{}:0",
                input.delivery_id
            )),
        })
    }

    fn adapt_delete(
        &self,
        input: TelegramRelayInput,
        received_at: DateTimeUtc,
    ) -> Result<Vec<TelegramAdaptationOutcome>, TelegramAdapterError> {
        if input.delete_ids.len() > TELEGRAM_DELETE_MAX_ITEMS {
            return Err(TelegramAdapterError::DeleteLimitExceeded {
                maximum: TELEGRAM_DELETE_MAX_ITEMS,
                actual: input.delete_ids.len(),
            });
        }
        let mut seen = BTreeSet::new();
        let ids = input
            .delete_ids
            .iter()
            .copied()
            .filter(|value| seen.insert(*value))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(vec![TelegramAdaptationOutcome::Ignored {
                evidence: relay_evidence(
                    &input,
                    SourceOperation::Delete,
                    None,
                    TelegramTimestampRule::ReceptionFallbackMissing,
                )?,
                reason: TelegramIgnoreReason::EmptyDelete,
            }]);
        }
        let mut outcomes = Vec::with_capacity(ids.len());
        for (ordinal, message_id) in ids.into_iter().enumerate() {
            let evidence = TelegramSourceEvidenceV1::try_new(
                TelegramAdapterPath::Relay,
                input.chat_id,
                Some(message_id),
                None,
                SourceOperation::Delete,
                None,
                None,
                TelegramTimestampRule::ReceptionFallbackMissing,
                Some(input.delivery_id.clone()),
            )?;
            let event = build_event(
                &self.config,
                TelegramEventInput {
                    chat_id: input.chat_id,
                    message_id,
                    reply_to: None,
                    operation: SourceOperation::Delete,
                    occurred_at: SourceTimestamp::new(
                        received_at,
                        SourceTimestampQuality::ReceptionFallback,
                    ),
                    received_at,
                    payload: SourcePayload::Empty,
                },
            )?;
            outcomes.push(TelegramAdaptationOutcome::Accepted {
                event: Box::new(event),
                evidence,
                delivery_identity: DurableDeliveryIdentity::Stable(format!(
                    "telegram-relay-v1:{}:{ordinal}",
                    input.delivery_id
                )),
            });
        }
        Ok(outcomes)
    }
}

#[derive(Debug, Clone)]
pub struct TelegramLegacyProducerConfig {
    schema: ComponentConfigSchemaRef,
    maximum_history: u32,
}

impl TelegramLegacyProducerConfig {
    pub fn try_new(maximum_history: usize) -> Result<Self, TelegramAdapterError> {
        if maximum_history > TELEGRAM_HISTORY_MAX_ITEMS {
            return Err(TelegramAdapterError::HistoryLimitExceeded {
                maximum: TELEGRAM_HISTORY_MAX_ITEMS,
                actual: maximum_history,
            });
        }
        Ok(Self {
            schema: ComponentConfigSchemaRef::try_new(TELEGRAM_LEGACY_CONFIG_SCHEMA)?,
            maximum_history: maximum_history as u32,
        })
    }
}

impl CanonicalComponentConfig for TelegramLegacyProducerConfig {
    fn schema(&self) -> &ComponentConfigSchemaRef {
        &self.schema
    }

    fn encode_config(
        &self,
        writer: &mut CanonicalWriter,
    ) -> Result<(), crate::normalization::IdentityError> {
        writer.u32(self.maximum_history);
        Ok(())
    }
}

pub struct LegacyTelegramParserAdapter {
    registry: Arc<ParserRegistry>,
}

impl LegacyTelegramParserAdapter {
    pub fn new(registry: Arc<ParserRegistry>) -> Self {
        Self { registry }
    }
}

impl PreNormalizedProducer for LegacyTelegramParserAdapter {
    fn produce(
        &self,
        event: &SourceEvent,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<PreNormalizedSignalBatch> {
        if event.operation() == SourceOperation::Delete {
            return rejected_component("telegram_delete_requires_lifecycle_only_commit");
        }
        let current = match project_message(event) {
            Ok(value) => value,
            Err(reason) => return Err(internal_contract_failure(&reason)),
        };
        let Some(parser) = self.registry.get(current.chat_id) else {
            return ignored_component("telegram_unregistered_channel");
        };
        let maximum_history = parser.max_history().min(TELEGRAM_HISTORY_MAX_ITEMS);
        let mut history = Vec::new();
        if let Some(view) = context.history() {
            for fact in view.facts() {
                if fact.event().operation() == SourceOperation::Delete {
                    continue;
                }
                let projected = match project_message(fact.event()) {
                    Ok(value) => value,
                    Err(reason) => return Err(internal_contract_failure(&reason)),
                };
                if projected.chat_id == current.chat_id {
                    history.push(projected);
                }
            }
        }
        if history.len() > maximum_history {
            history.drain(..history.len() - maximum_history);
        }
        let parent_from_context = match context.parent() {
            Some(parent) => match project_message(parent.fact().event()) {
                Ok(value) => Some(value),
                Err(reason) => return Err(internal_contract_failure(&reason)),
            },
            None => None,
        };
        let parent = current.reply_to.and_then(|reply_to| {
            parent_from_context
                .as_ref()
                .filter(|value| value.chat_id == current.chat_id && value.msg_id == reply_to)
                .or_else(|| {
                    history
                        .iter()
                        .rev()
                        .find(|value| value.chat_id == current.chat_id && value.msg_id == reply_to)
                })
        });
        let parse_context = ParseContext {
            market: None,
            llm: None,
            history: &history,
        };
        let timestamp = event.occurred_at().value().as_datetime().naive_utc();
        let action = if current.reply_to.is_some() {
            parser.parse_reply_message(&current, timestamp, parent, &parse_context)
        } else {
            parser.parse_root_message(&current, timestamp, &parse_context)
        };
        match action {
            ParsedAction::Signals(signals) if signals.is_empty() => {
                ignored_component("telegram_parser_returned_empty_signals")
            }
            ParsedAction::Signals(signals) => match PreNormalizedSignalBatch::try_new(signals) {
                Ok(batch) => Ok(ComponentReport::accepted(batch)),
                Err(_) => rejected_component("telegram_signal_batch_limit_exceeded"),
            },
            ParsedAction::Skip => ignored_component("telegram_parser_returned_skip"),
            ParsedAction::Rejected(_) => rejected_component("telegram_parser_rejected"),
        }
    }
}

pub fn bind_legacy_telegram_producer(
    registry: Arc<ParserRegistry>,
) -> Result<PreNormalizedProducerBinding, TelegramAdapterError> {
    let maximum_history = registry.maximum_history();
    let config = TelegramLegacyProducerConfig::try_new(maximum_history)?;
    let history = if maximum_history == 0 {
        None
    } else {
        Some(HistoryRequirement::new(
            ItemLimit::new(maximum_history as u32),
            ByteLimit::new(
                (maximum_history as u64).saturating_mul(TELEGRAM_HISTORY_FACT_MAX_BYTES),
            ),
            true,
            false,
        ))
    };
    let descriptor = ComponentDescriptor::try_new(
        ComponentId::try_new("telegram-legacy-producer", "component ID")?,
        ComponentKind::PreNormalizedProducer,
        SemanticVersion::new(1, 0, 0),
        1,
        config.schema().clone(),
        PipelineContextRequirements::new(
            history,
            ParentRequirement::Optional,
            ItemLimit::new(maximum_history as u32),
            ByteLimit::new(
                (maximum_history as u64).saturating_mul(TELEGRAM_HISTORY_FACT_MAX_BYTES),
            ),
        ),
        EmptyOutputPolicy::Ignore,
        vec![],
        vec![],
        vec![],
    )?;
    Ok(bind_pre_normalized_producer(descriptor, &config, |_| {
        Ok(LegacyTelegramParserAdapter::new(registry))
    })?)
}

pub fn telegram_thread_label(
    chat_id: i64,
) -> Result<(MetadataKey, MetadataValue), SourceValidationError> {
    Ok((
        MetadataKey::new(TELEGRAM_THREAD_LABEL)?,
        MetadataValue::new(telegram_thread_id(chat_id))?,
    ))
}

pub fn telegram_event_id(chat_id: i64, message_id: i64) -> String {
    format!("tgmsg:v1:{chat_id}:{message_id}")
}

pub fn telegram_thread_id(chat_id: i64) -> String {
    format!("tgchat:v1:{chat_id}")
}

fn source_adapter_identity(
    config: &TelegramAdapterConfig,
    path: TelegramAdapterPath,
) -> Result<SourceAdapterIdentity, TelegramAdapterError> {
    let mut writer = CanonicalWriter::new();
    writer
        .text(config.source_id.as_str())
        .map_err(|error| TelegramAdapterError::ComponentConfiguration(error.to_string()))?;
    writer.bool(config.allow_legacy_naive_utc);
    writer.bool(config.allow_legacy_zero_message_id);
    writer.u16(match path {
        TelegramAdapterPath::Batch => 1,
        TelegramAdapterPath::Relay => 2,
    });
    let id = match path {
        TelegramAdapterPath::Batch => "telegram-batch-source-adapter",
        TelegramAdapterPath::Relay => "telegram-relay-source-adapter",
    };
    Ok(SourceAdapterIdentity::new(
        ComponentId::try_new(id, "adapter ID")?,
        SemanticVersion::new(1, 0, 0),
        writer
            .into_identity_bytes()
            .map_err(|error| TelegramAdapterError::ComponentConfiguration(error.to_string()))?,
    ))
}

struct TelegramEventInput {
    chat_id: i64,
    message_id: i64,
    reply_to: Option<i64>,
    operation: SourceOperation,
    occurred_at: SourceTimestamp,
    received_at: DateTimeUtc,
    payload: SourcePayload,
}

fn build_event(
    config: &TelegramAdapterConfig,
    input: TelegramEventInput,
) -> Result<SourceEvent, TelegramAdapterError> {
    let source = config.source_id.clone();
    let key = SourceEventKey::new(
        source.clone(),
        ExternalEventId::new(telegram_event_id(input.chat_id, input.message_id))?,
    );
    let mut labels = BTreeMap::new();
    let (thread_key, thread_value) = telegram_thread_label(input.chat_id)?;
    labels.insert(thread_key, thread_value);
    let mut event = SourceEvent::new(
        key,
        input.operation,
        SourceRevision::Unversioned,
        input.occurred_at,
        input.received_at,
        input.payload,
    )
    .with_thread(ExternalThreadId::new(telegram_thread_id(input.chat_id))?)
    .with_metadata(SourceMetadata::new(labels)?);
    if let Some(parent) = input.reply_to {
        event = event.with_parent(SourceEventKey::new(
            source,
            ExternalEventId::new(telegram_event_id(input.chat_id, parent))?,
        ));
    }
    Ok(event)
}

fn relay_evidence(
    input: &TelegramRelayInput,
    operation: SourceOperation,
    message_id: Option<i64>,
    rule: TelegramTimestampRule,
) -> Result<TelegramSourceEvidenceV1, TelegramAdapterError> {
    TelegramSourceEvidenceV1::try_new(
        TelegramAdapterPath::Relay,
        input.chat_id,
        message_id,
        input.reply_to,
        operation,
        None,
        input.epoch_seconds.map(f64::to_bits),
        rule,
        Some(input.delivery_id.clone()),
    )
}

fn parse_batch_timestamp(
    value: &str,
    allow_legacy_naive_utc: bool,
) -> Result<(DateTimeUtc, SourceTimestampQuality, TelegramTimestampRule), String> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok((
            DateTimeUtc::new(timestamp.with_timezone(&Utc)),
            SourceTimestampQuality::SourceProvided,
            TelegramTimestampRule::ExplicitOffset,
        ));
    }
    if allow_legacy_naive_utc {
        for format in [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
        ] {
            if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
                return Ok((
                    DateTimeUtc::new(DateTime::<Utc>::from_naive_utc_and_offset(timestamp, Utc)),
                    SourceTimestampQuality::AdapterDerived,
                    TelegramTimestampRule::LegacyNaiveUtc,
                ));
            }
        }
    }
    Err("Telegram batch timestamp is invalid under the configured policy".to_string())
}

fn relay_timestamp(
    epoch_seconds: Option<f64>,
    received_at: DateTimeUtc,
) -> (DateTimeUtc, SourceTimestampQuality, TelegramTimestampRule) {
    match epoch_seconds.and_then(epoch_to_utc) {
        Some(timestamp) => (
            timestamp,
            SourceTimestampQuality::SourceProvided,
            TelegramTimestampRule::RelayEpoch,
        ),
        None if epoch_seconds.is_none() => (
            received_at,
            SourceTimestampQuality::ReceptionFallback,
            TelegramTimestampRule::ReceptionFallbackMissing,
        ),
        None => (
            received_at,
            SourceTimestampQuality::ReceptionFallback,
            TelegramTimestampRule::ReceptionFallbackInvalid,
        ),
    }
}

fn epoch_to_utc(value: f64) -> Option<DateTimeUtc> {
    if !value.is_finite() {
        return None;
    }
    let seconds_floor = value.floor();
    if seconds_floor < i64::MIN as f64 || seconds_floor > i64::MAX as f64 {
        return None;
    }
    let mut seconds = seconds_floor as i64;
    let mut nanos = ((value - seconds_floor) * 1_000_000_000.0).round() as u32;
    if nanos == 1_000_000_000 {
        seconds = seconds.checked_add(1)?;
        nanos = 0;
    }
    DateTime::<Utc>::from_timestamp(seconds, nanos).map(DateTimeUtc::new)
}

fn project_message(event: &SourceEvent) -> Result<RawTgMessage, String> {
    let (chat_id, message_id) = parse_telegram_event_id(event.key().external_id().as_str())?;
    if let Some(thread) = event.thread() {
        let thread_chat = parse_telegram_thread_id(thread.as_str())?;
        if thread_chat != chat_id {
            return Err("Telegram event and thread identities disagree".to_string());
        }
    }
    let message = match event.payload() {
        SourcePayload::Text(payload) => payload.text().as_str().to_string(),
        SourcePayload::Structured(_) | SourcePayload::Empty => {
            return Err("Telegram parser input requires a text payload".to_string());
        }
    };
    let reply_to = event
        .parent()
        .map(|parent| {
            if parent.source() != event.key().source() {
                return Err("Telegram parent belongs to another source".to_string());
            }
            let (parent_chat, parent_message) =
                parse_telegram_event_id(parent.external_id().as_str())?;
            if parent_chat != chat_id {
                return Err("Telegram parent belongs to another thread".to_string());
            }
            Ok(parent_message)
        })
        .transpose()?;
    Ok(RawTgMessage {
        chat_id,
        msg_id: message_id,
        ts: event
            .occurred_at()
            .value()
            .as_datetime()
            .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        message,
        reply_to,
    })
}

fn parse_telegram_event_id(value: &str) -> Result<(i64, i64), String> {
    let rest = value
        .strip_prefix("tgmsg:v1:")
        .ok_or_else(|| "Telegram event identity has an unsupported format".to_string())?;
    let (chat, message) = rest
        .split_once(':')
        .ok_or_else(|| "Telegram event identity is incomplete".to_string())?;
    if message.contains(':') {
        return Err("Telegram event identity has too many fields".to_string());
    }
    Ok((parse_canonical_i64(chat)?, parse_canonical_i64(message)?))
}

fn parse_telegram_thread_id(value: &str) -> Result<i64, String> {
    let value = value
        .strip_prefix("tgchat:v1:")
        .ok_or_else(|| "Telegram thread identity has an unsupported format".to_string())?;
    parse_canonical_i64(value)
}

fn parse_canonical_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| "Telegram identity contains an invalid i64".to_string())?;
    if parsed.to_string() != value {
        return Err("Telegram identity is not canonically encoded".to_string());
    }
    Ok(parsed)
}

fn validate_adapter_field(
    value: &str,
    field: &'static str,
    maximum: usize,
) -> Result<(), TelegramAdapterError> {
    if value.is_empty() {
        return Err(TelegramAdapterError::Empty { field });
    }
    if value.len() > maximum {
        return Err(TelegramAdapterError::FieldTooLarge {
            field,
            maximum,
            actual: value.len(),
        });
    }
    Ok(())
}

fn adaptation_diagnostic(
    code: &'static str,
    message: &str,
) -> Result<Diagnostic, TelegramAdapterError> {
    Ok(Diagnostic::try_new(
        code,
        DiagnosticSeverity::Error,
        DiagnosticRedaction::SensitiveValuesRedacted,
        message,
    )?)
}

fn ignored_component(reason: &'static str) -> ComponentResult<PreNormalizedSignalBatch> {
    match IgnoreReason::try_new(reason) {
        Ok(reason) => Ok(ComponentReport::ignored(reason)),
        Err(error) => Err(internal_contract_failure(&error.to_string())),
    }
}

fn rejected_component(reason: &'static str) -> ComponentResult<PreNormalizedSignalBatch> {
    match RejectionReason::try_new(reason) {
        Ok(reason) => Ok(ComponentReport::rejected(reason)),
        Err(error) => Err(internal_contract_failure(&error.to_string())),
    }
}

fn internal_contract_failure(message: &str) -> StageExecutionFailure {
    let diagnostics = Diagnostic::try_new(
        "telegram_adapter_contract_failure",
        DiagnosticSeverity::Error,
        DiagnosticRedaction::SensitiveValuesRedacted,
        message,
    )
    .ok()
    .and_then(|diagnostic| DiagnosticSet::try_new(vec![diagnostic]).ok())
    .unwrap_or_else(DiagnosticSet::empty);
    StageExecutionFailure::new(
        EvaluationFailureClass::InternalContractFailed,
        EvaluationRetrySafety::UnsafeToRetry,
        crate::normalization::CompletionKnowledge::NotStarted,
        diagnostics,
    )
}
