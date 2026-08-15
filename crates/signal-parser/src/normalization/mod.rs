//! Stateless source-event routing, decoding, parsing, and signal normalization.

mod component;
mod context;
mod diagnostic;
mod graph;
mod identity;
mod projection;
mod raw_signals_v1;
mod report;
mod signal;
mod value;

pub use component::{
    CanonicalComponentConfig, ComponentBindError, ComponentDescriptor, DecoderBinding, DraftBatch,
    DraftValidator, DraftValidatorBinding, EmptyOutputPolicy, FinalizedBatch, FinalizerBinding,
    MeaningBatch, MeaningContract, MeaningEncoding, MeaningNormalizer, MeaningNormalizerBinding,
    MeaningSchemaRef, MessageParser, NoConfig, ParsedMeaning, ParserBinding, PreNormalizedProducer,
    PreNormalizedProducerBinding, SignalDecoder, SignalFinalizer, StructuredInputCapability,
    VersionedMeaning, bind_decoder, bind_draft_validator, bind_finalizer, bind_meaning_normalizer,
    bind_parser, bind_pre_normalized_producer,
};
pub use context::{
    BaseContextSnapshot, ContextCutoff, ContextValidationError, EvaluationClock,
    HistoricalSourceFact, HistoryRequirement, HistoryView, ParentRequirement, ParentView,
    PipelineContextRequirements,
};
pub use diagnostic::{
    CompletionKnowledge, ComponentReport, ComponentResult, Diagnostic, DiagnosticRedaction,
    DiagnosticSet, DiagnosticSeverity, EvaluationFailureClass, EvaluationRetrySafety,
    EvaluationStage, EvidenceFact, IgnoreReason, RejectionReason, StageDisposition, StageEvidence,
    StageExecutionFailure,
};
pub use graph::{
    AuthorClass, CompiledPipeline, CompiledPipelineKind, CompiledRoutingGraph, DraftValidationStep,
    EvaluationInput, GraphCompileError, PayloadKind, PreparedEvaluation, RouteEvaluation,
    RouteSelector, RouteSpec,
};
pub use identity::{
    CanonicalEncode, CanonicalWriter, ComponentConfigIdentity, ComponentConfigSchemaRef,
    ComponentKind, IdentityError, PipelineIdentity, ResolvedComponentRef, ResolvedGraphIdentity,
    RoutingGraphIdentity, SemanticVersion,
};
pub use projection::{
    NORMALIZED_SIGNAL_SEMANTIC_MAX_BYTES, NormalizedSignalSemanticProjection,
    normalized_signal_semantic_projection,
};
pub(crate) use raw_signals_v1::decode_raw_signal_value_v1;
pub use raw_signals_v1::{
    CanonicalRawSignalsDecoder, RAW_SIGNALS_V1_SCHEMA, StandardSignalFinalizer,
    raw_signals_v1_schema,
};
pub use report::{
    AmbiguityAlternativeEvidence, AmbiguityEvidence, EvaluationEvidence, EvaluationFailure,
    EvaluationIdentity, NormalizationEvaluationReport, NormalizationOutcome,
    PipelineEvaluationResult,
};
pub use signal::{
    CorrelationHint, InstrumentHint, NormalizationCandidate, PipelineCandidateEvidence,
    PositionDraftRef, PreNormalizedSignalBatch, RuleDraft, SignalContractError, SignalDraft,
    SignalDraftAction, SourceAdapterIdentity, SourceProvenanceDraft,
};
pub use value::{
    ByteLimit, CanonicalIdentityBytes, ComponentId, ContractBytes, ContractList, ContractMap,
    ContractText, ContractValueError, DiagnosticCode, DiagnosticText, FiniteF64, GroupText,
    ItemLimit, MAX_CANONICAL_IDENTITY_BYTES, NonEmptyContractList, NonEmptyContractText,
    PipelineId, PositiveFiniteF64, RuleNameText, SymbolText, TradeKeyText, UnitInterval,
};
