use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::ingestion::{
    LanguageTag, MetadataKey, MetadataValue, PayloadEncoding, PayloadSchemaRef, SourceEvent,
    SourceId, SourceOperation, SourcePayload, TextFormat,
};

use super::component::{
    DecoderBinding, DraftValidatorBinding, EmptyOutputPolicy, FinalizerBinding, MeaningContract,
    MeaningNormalizerBinding, ParserBinding, PreNormalizedProducerBinding, encode_descriptor_ref,
};
use super::context::{BaseContextSnapshot, PipelineContextRequirements};
use super::diagnostic::{
    ComponentReport, Diagnostic, DiagnosticRedaction, DiagnosticSet, DiagnosticSeverity,
    EvaluationStage, IgnoreReason, RejectionReason, RouteMatchEvidence, StageDisposition,
    StageEvidence, StageExecutionFailure,
};
use super::identity::{
    CanonicalEncode, CanonicalWriter, IdentityError, PipelineIdentity, ResolvedGraphIdentity,
    RoutingGraphIdentity, SemanticVersion,
};
use super::report::{
    AmbiguityAlternativeEvidence, AmbiguityEvidence, EvaluationEvidence, EvaluationFailure,
    EvaluationIdentity, NormalizationEvaluationReport, NormalizationOutcome,
    PipelineEvaluationResult,
};
use super::signal::{
    NormalizationCandidate, PipelineCandidateEvidence, SourceAdapterIdentity,
    SourceProvenanceDraft, into_candidate_parts, validate_finalized_batch,
    validate_pre_normalized_batch,
};
use super::value::{
    ContractList, ContractMap, ContractText, ContractValueError, NonEmptyContractList, PipelineId,
};

#[derive(Debug, Clone)]
pub enum DraftValidationStep {
    NoneDeclared,
    Component(Box<DraftValidatorBinding>),
}

#[derive(Debug, Clone)]
pub enum CompiledPipelineKind {
    Structured {
        decoder: Box<DecoderBinding>,
        draft_validation: DraftValidationStep,
        finalizer: Box<FinalizerBinding>,
    },
    Text {
        parser: Box<ParserBinding>,
        normalizer: Box<MeaningNormalizerBinding>,
        draft_validation: DraftValidationStep,
        finalizer: Box<FinalizerBinding>,
        meaning_contracts: ContractList<MeaningContract, 16>,
    },
    CompatibilityPreNormalized {
        producer: Box<PreNormalizedProducerBinding>,
    },
}

impl CompiledPipelineKind {
    fn tag(&self) -> u16 {
        match self {
            Self::Structured { .. } => 1,
            Self::Text { .. } => 2,
            Self::CompatibilityPreNormalized { .. } => 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    identity: PipelineIdentity,
    requirements: PipelineContextRequirements,
    kind: CompiledPipelineKind,
}

impl CompiledPipeline {
    pub fn compile_structured(
        id: PipelineId,
        version: SemanticVersion,
        decoder: DecoderBinding,
        draft_validation: DraftValidationStep,
        finalizer: FinalizerBinding,
    ) -> Result<Self, GraphCompileError> {
        let requirements = merge_requirements(
            decoder.descriptor().requirements(),
            &draft_validation,
            finalizer.descriptor().requirements(),
        );
        let kind = CompiledPipelineKind::Structured {
            decoder: Box::new(decoder),
            draft_validation,
            finalizer: Box::new(finalizer),
        };
        Self::finish(id, version, requirements, kind)
    }

    pub fn compile_text(
        id: PipelineId,
        version: SemanticVersion,
        parser: ParserBinding,
        normalizer: MeaningNormalizerBinding,
        draft_validation: DraftValidationStep,
        finalizer: FinalizerBinding,
    ) -> Result<Self, GraphCompileError> {
        let parser_outputs = parser.descriptor().meaning_outputs();
        let normalizer_inputs = normalizer.descriptor().meaning_inputs();
        let selected = parser_outputs
            .iter()
            .filter(|contract| normalizer_inputs.contains(contract))
            .cloned()
            .collect::<Vec<_>>();
        if selected.is_empty() || selected.len() != parser_outputs.len() {
            return Err(GraphCompileError::MeaningContractMismatch);
        }
        let requirements = parser
            .descriptor()
            .requirements()
            .merge(normalizer.descriptor().requirements());
        let requirements = merge_requirements(
            &requirements,
            &draft_validation,
            finalizer.descriptor().requirements(),
        );
        let kind = CompiledPipelineKind::Text {
            parser: Box::new(parser),
            normalizer: Box::new(normalizer),
            draft_validation,
            finalizer: Box::new(finalizer),
            meaning_contracts: ContractList::try_new(selected, "meaning contracts")?,
        };
        Self::finish(id, version, requirements, kind)
    }

    pub fn compile_compatibility(
        id: PipelineId,
        version: SemanticVersion,
        producer: PreNormalizedProducerBinding,
    ) -> Result<Self, GraphCompileError> {
        let requirements = producer.descriptor().requirements().clone();
        let kind = CompiledPipelineKind::CompatibilityPreNormalized {
            producer: Box::new(producer),
        };
        Self::finish(id, version, requirements, kind)
    }

    fn finish(
        id: PipelineId,
        version: SemanticVersion,
        requirements: PipelineContextRequirements,
        kind: CompiledPipelineKind,
    ) -> Result<Self, GraphCompileError> {
        let graph = ResolvedGraphIdentity::from_payload(encode_pipeline_kind(&kind)?)?;
        let identity = PipelineIdentity::new(id, version, graph)?;
        Ok(Self {
            identity,
            requirements,
            kind,
        })
    }

    pub fn identity(&self) -> &PipelineIdentity {
        &self.identity
    }

    pub fn requirements(&self) -> &PipelineContextRequirements {
        &self.requirements
    }
}

fn merge_requirements(
    first: &PipelineContextRequirements,
    validation: &DraftValidationStep,
    finalizer: &PipelineContextRequirements,
) -> PipelineContextRequirements {
    let requirements = first.merge(finalizer);
    match validation {
        DraftValidationStep::NoneDeclared => requirements,
        DraftValidationStep::Component(binding) => {
            requirements.merge(binding.descriptor().requirements())
        }
    }
}

fn encode_pipeline_kind(kind: &CompiledPipelineKind) -> Result<Vec<u8>, IdentityError> {
    let mut writer = CanonicalWriter::new();
    writer.u16(1);
    writer.u16(kind.tag());
    match kind {
        CompiledPipelineKind::Structured {
            decoder,
            draft_validation,
            finalizer,
        } => {
            encode_descriptor_ref(decoder.descriptor(), decoder.resolved(), &mut writer)?;
            encode_validation(draft_validation, &mut writer)?;
            encode_descriptor_ref(finalizer.descriptor(), finalizer.resolved(), &mut writer)?;
        }
        CompiledPipelineKind::Text {
            parser,
            normalizer,
            draft_validation,
            finalizer,
            meaning_contracts,
        } => {
            encode_descriptor_ref(parser.descriptor(), parser.resolved(), &mut writer)?;
            encode_descriptor_ref(normalizer.descriptor(), normalizer.resolved(), &mut writer)?;
            writer.u32(meaning_contracts.len() as u32);
            for contract in meaning_contracts.iter() {
                writer.text(contract.schema().id().as_str())?;
                writer.u32(contract.schema().version());
                writer.u16(contract.encoding().tag());
            }
            encode_validation(draft_validation, &mut writer)?;
            encode_descriptor_ref(finalizer.descriptor(), finalizer.resolved(), &mut writer)?;
        }
        CompiledPipelineKind::CompatibilityPreNormalized { producer } => {
            encode_descriptor_ref(producer.descriptor(), producer.resolved(), &mut writer)?;
        }
    }
    Ok(writer.into_bytes())
}

fn encode_validation(
    validation: &DraftValidationStep,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match validation {
        DraftValidationStep::NoneDeclared => writer.u16(1),
        DraftValidationStep::Component(binding) => {
            writer.u16(2);
            encode_descriptor_ref(binding.descriptor(), binding.resolved(), writer)?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AuthorClass(ContractText<128>);

impl AuthorClass {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ContractValueError> {
        let value = ContractText::try_new(value, "author class")?;
        if value.as_str().is_empty() {
            return Err(ContractValueError::Empty {
                field: "author class",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Text,
    Structured,
    Empty,
}

impl PayloadKind {
    fn tag(self) -> u16 {
        match self {
            Self::Text => 1,
            Self::Structured => 2,
            Self::Empty => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSelector {
    source: Option<SourceId>,
    operation: Option<SourceOperation>,
    payload_kind: Option<PayloadKind>,
    schema: Option<PayloadSchemaRef>,
    encoding: Option<PayloadEncoding>,
    text_format: Option<TextFormat>,
    language: Option<LanguageTag>,
    author_class: Option<AuthorClass>,
    labels: ContractMap<MetadataKey, MetadataValue, 64>,
}

impl RouteSelector {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        source: Option<SourceId>,
        operation: Option<SourceOperation>,
        payload_kind: Option<PayloadKind>,
        schema: Option<PayloadSchemaRef>,
        encoding: Option<PayloadEncoding>,
        text_format: Option<TextFormat>,
        language: Option<LanguageTag>,
        author_class: Option<AuthorClass>,
        labels: BTreeMap<MetadataKey, MetadataValue>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            source,
            operation,
            payload_kind,
            schema,
            encoding,
            text_format,
            language,
            author_class,
            labels: ContractMap::try_new(labels, "route labels")?,
        })
    }

    pub fn any() -> Self {
        Self {
            source: None,
            operation: None,
            payload_kind: None,
            schema: None,
            encoding: None,
            text_format: None,
            language: None,
            author_class: None,
            labels: ContractMap::empty(),
        }
    }

    fn matches(&self, input: &EvaluationInput) -> bool {
        let event = input.event();
        if self
            .source
            .as_ref()
            .is_some_and(|value| value != event.key().source())
            || self
                .operation
                .is_some_and(|value| value != event.operation())
            || self
                .author_class
                .as_ref()
                .is_some_and(|value| Some(value) != input.author_class())
        {
            return false;
        }
        let payload_matches = match event.payload() {
            SourcePayload::Text(payload) => {
                self.payload_kind
                    .is_none_or(|kind| kind == PayloadKind::Text)
                    && self.schema.is_none()
                    && self.encoding.is_none()
                    && self
                        .text_format
                        .is_none_or(|value| value == payload.format())
                    && self
                        .language
                        .as_ref()
                        .is_none_or(|value| Some(value) == payload.language())
            }
            SourcePayload::Structured(payload) => {
                self.payload_kind
                    .is_none_or(|kind| kind == PayloadKind::Structured)
                    && self
                        .schema
                        .as_ref()
                        .is_none_or(|value| value == payload.schema())
                    && self
                        .encoding
                        .is_none_or(|value| value == payload.encoding())
                    && self.text_format.is_none()
                    && self.language.is_none()
            }
            SourcePayload::Empty => {
                self.payload_kind
                    .is_none_or(|kind| kind == PayloadKind::Empty)
                    && self.schema.is_none()
                    && self.encoding.is_none()
                    && self.text_format.is_none()
                    && self.language.is_none()
            }
        };
        payload_matches
            && self
                .labels
                .as_map()
                .iter()
                .all(|(key, value)| event.metadata().labels().get(key) == Some(value))
    }

    fn encode(&self, writer: &mut CanonicalWriter) -> Result<(), IdentityError> {
        encode_option_text(self.source.as_ref().map(SourceId::as_str), writer)?;
        encode_option_u16(self.operation.map(operation_tag), writer);
        encode_option_u16(self.payload_kind.map(PayloadKind::tag), writer);
        encode_option_text(self.schema.as_ref().map(PayloadSchemaRef::as_str), writer)?;
        encode_option_u16(self.encoding.map(encoding_tag), writer);
        encode_option_u16(self.text_format.map(text_format_tag), writer);
        encode_option_text(self.language.as_ref().map(LanguageTag::as_str), writer)?;
        encode_option_text(self.author_class.as_ref().map(AuthorClass::as_str), writer)?;
        writer.u32(self.labels.as_map().len() as u32);
        for (key, value) in self.labels.as_map() {
            writer.text(key.as_str())?;
            writer.text(value.as_str())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RouteSpec {
    id: ContractText<128>,
    priority: i64,
    selector: RouteSelector,
    target: PipelineIdentity,
}

impl RouteSpec {
    pub fn try_new(
        id: impl Into<String>,
        priority: i64,
        selector: RouteSelector,
        target: PipelineIdentity,
    ) -> Result<Self, ContractValueError> {
        let id = ContractText::try_new(id, "route id")?;
        if id.as_str().is_empty() {
            return Err(ContractValueError::Empty { field: "route id" });
        }
        Ok(Self {
            id,
            priority,
            selector,
            target,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EvaluationInput {
    event: SourceEvent,
    source_adapter: SourceAdapterIdentity,
    author_class: Option<AuthorClass>,
}

impl EvaluationInput {
    pub fn new(
        event: SourceEvent,
        source_adapter: SourceAdapterIdentity,
        author_class: Option<AuthorClass>,
    ) -> Self {
        Self {
            event,
            source_adapter,
            author_class,
        }
    }

    pub fn event(&self) -> &SourceEvent {
        &self.event
    }

    pub fn source_adapter(&self) -> &SourceAdapterIdentity {
        &self.source_adapter
    }

    pub fn author_class(&self) -> Option<&AuthorClass> {
        self.author_class.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct CompiledRoutingGraph {
    identity: RoutingGraphIdentity,
    routes: Vec<RouteSpec>,
    pipelines: BTreeMap<PipelineIdentity, Arc<CompiledPipeline>>,
}

impl CompiledRoutingGraph {
    pub fn compile(
        mut routes: Vec<RouteSpec>,
        pipelines: Vec<CompiledPipeline>,
    ) -> Result<Self, GraphCompileError> {
        if routes.len() > 16 {
            return Err(GraphCompileError::TooManyRoutes);
        }
        let mut pipeline_map = BTreeMap::new();
        for pipeline in pipelines {
            if pipeline_map
                .insert(pipeline.identity().clone(), Arc::new(pipeline))
                .is_some()
            {
                return Err(GraphCompileError::DuplicatePipeline);
            }
        }
        let mut route_ids = BTreeSet::new();
        for route in &routes {
            if !route_ids.insert(route.id.as_str().to_string()) {
                return Err(GraphCompileError::DuplicateRoute);
            }
            let pipeline = pipeline_map
                .get(&route.target)
                .ok_or(GraphCompileError::UnknownPipeline)?;
            if !route_matches_pipeline(route, pipeline) {
                return Err(GraphCompileError::RoutePipelineMismatch);
            }
        }
        for (index, route) in routes.iter().enumerate() {
            if routes[index + 1..]
                .iter()
                .any(|other| route.selector == other.selector && route.target != other.target)
            {
                return Err(GraphCompileError::ConflictingSelector);
            }
        }
        routes.sort_by(|left, right| left.id.cmp(&right.id));
        let identity = RoutingGraphIdentity::from_payload(encode_routes(&routes)?)?;
        Ok(Self {
            identity,
            routes,
            pipelines: pipeline_map,
        })
    }

    pub fn identity(&self) -> RoutingGraphIdentity {
        self.identity.clone()
    }

    pub fn route(&self, input: EvaluationInput) -> RouteEvaluation {
        let mut matches = self
            .routes
            .iter()
            .filter(|route| route.selector.matches(&input))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return RouteEvaluation::Completed(Box::new(route_terminal_report(
                EvaluationIdentity::new(self.identity.clone(), None),
                NormalizationOutcome::Ignored {
                    reason: ignore_reason("no_route"),
                },
                vec![],
            )));
        }
        let maximum = matches.iter().map(|route| route.priority).max().unwrap();
        matches.retain(|route| route.priority == maximum);
        let route_evidence = matches
            .iter()
            .map(|route| {
                RouteMatchEvidence::new(route.id.clone(), route.priority, route.target.clone())
            })
            .collect::<Vec<_>>();
        let distinct = matches
            .iter()
            .map(|route| route.target.clone())
            .collect::<BTreeSet<_>>();
        if distinct.len() > 1 {
            let alternatives = distinct
                .into_iter()
                .enumerate()
                .map(|(ordinal, pipeline)| {
                    AmbiguityAlternativeEvidence::new(Some(pipeline), ordinal as u32, 1)
                })
                .collect::<Vec<_>>();
            let evidence =
                AmbiguityEvidence::try_new(alternatives.clone(), alternatives.len() as u32)
                    .expect("route and ambiguity bounds share the same ceiling");
            return RouteEvaluation::Completed(Box::new(route_terminal_report(
                EvaluationIdentity::new(self.identity.clone(), None),
                NormalizationOutcome::Ambiguous { evidence },
                route_evidence,
            )));
        }
        let target = matches[0].target.clone();
        let pipeline = self
            .pipelines
            .get(&target)
            .expect("compiled route target exists")
            .clone();
        RouteEvaluation::Prepared(Box::new(PreparedEvaluation {
            input,
            identity: EvaluationIdentity::new(self.identity.clone(), Some(target)),
            route_evidence,
            pipeline,
        }))
    }
}

#[derive(Debug)]
pub enum RouteEvaluation {
    Completed(Box<NormalizationEvaluationReport>),
    Prepared(Box<PreparedEvaluation>),
}

#[derive(Debug)]
pub struct PreparedEvaluation {
    input: EvaluationInput,
    identity: EvaluationIdentity,
    route_evidence: Vec<RouteMatchEvidence>,
    pipeline: Arc<CompiledPipeline>,
}

impl PreparedEvaluation {
    pub fn identity(&self) -> &EvaluationIdentity {
        &self.identity
    }

    pub fn requirements(&self) -> &PipelineContextRequirements {
        self.pipeline.requirements()
    }

    pub fn evaluate(self, context: &BaseContextSnapshot) -> PipelineEvaluationResult {
        let kind = self.pipeline.kind.clone();
        match kind {
            CompiledPipelineKind::Structured {
                decoder,
                draft_validation,
                finalizer,
            } => self.evaluate_structured(&decoder, &draft_validation, &finalizer, context),
            CompiledPipelineKind::Text {
                parser,
                normalizer,
                draft_validation,
                finalizer,
                meaning_contracts,
            } => self.evaluate_text(
                &parser,
                &normalizer,
                &draft_validation,
                &finalizer,
                &meaning_contracts,
                context,
            ),
            CompiledPipelineKind::CompatibilityPreNormalized { producer } => {
                self.evaluate_compatibility(&producer, context)
            }
        }
    }

    fn evaluate_structured(
        self,
        decoder: &DecoderBinding,
        validation: &DraftValidationStep,
        finalizer: &FinalizerBinding,
        context: &BaseContextSnapshot,
    ) -> PipelineEvaluationResult {
        let SourcePayload::Structured(payload) = self.input.event().payload() else {
            return self.rejected("route_payload_mismatch", DiagnosticSet::empty(), vec![]);
        };
        if !decoder
            .descriptor()
            .structured_inputs()
            .iter()
            .any(|capability| {
                capability.schema() == payload.schema()
                    && capability.encoding() == payload.encoding()
            })
        {
            return self.rejected("decoder_schema_mismatch", DiagnosticSet::empty(), vec![]);
        }
        let mut diagnostics = DiagnosticSet::empty();
        let mut evidence = Vec::new();
        let drafts = match stage_output(
            decoder
                .executable()
                .decode(self.input.event(), payload, context),
            decoder,
            EvaluationStage::Decoding,
            &mut diagnostics,
            &mut evidence,
            decoder.descriptor().empty_output(),
        ) {
            StageFlow::Accepted(value) => value,
            StageFlow::Completed(outcome) => return self.completed(outcome, diagnostics, evidence),
            StageFlow::Failed(failure) => return self.failed(failure),
        };
        let drafts =
            match self.run_validation(validation, drafts, context, &mut diagnostics, &mut evidence)
            {
                StageFlow::Accepted(value) => value,
                StageFlow::Completed(outcome) => {
                    return self.completed(outcome, diagnostics, evidence);
                }
                StageFlow::Failed(failure) => return self.failed(failure),
            };
        self.run_finalizer(finalizer, drafts, context, diagnostics, evidence)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_text(
        self,
        parser: &ParserBinding,
        normalizer: &MeaningNormalizerBinding,
        validation: &DraftValidationStep,
        finalizer: &FinalizerBinding,
        meaning_contracts: &ContractList<MeaningContract, 16>,
        context: &BaseContextSnapshot,
    ) -> PipelineEvaluationResult {
        let SourcePayload::Text(payload) = self.input.event().payload() else {
            return self.rejected("route_payload_mismatch", DiagnosticSet::empty(), vec![]);
        };
        let mut diagnostics = DiagnosticSet::empty();
        let mut evidence = Vec::new();
        let meanings = match stage_output(
            parser
                .executable()
                .parse(self.input.event(), payload, context),
            parser,
            EvaluationStage::Parsing,
            &mut diagnostics,
            &mut evidence,
            parser.descriptor().empty_output(),
        ) {
            StageFlow::Accepted(value) => value,
            StageFlow::Completed(outcome) => return self.completed(outcome, diagnostics, evidence),
            StageFlow::Failed(failure) => return self.failed(failure),
        };
        if meanings.iter().any(|meaning| {
            !meaning_contracts
                .as_slice()
                .contains(&meaning.value().contract())
        }) {
            return self.rejected("meaning_contract_mismatch", diagnostics, evidence);
        }
        let drafts = match stage_output(
            normalizer
                .executable()
                .normalize(meanings, self.input.event(), context),
            normalizer,
            EvaluationStage::MeaningNormalization,
            &mut diagnostics,
            &mut evidence,
            normalizer.descriptor().empty_output(),
        ) {
            StageFlow::Accepted(value) => value,
            StageFlow::Completed(outcome) => return self.completed(outcome, diagnostics, evidence),
            StageFlow::Failed(failure) => return self.failed(failure),
        };
        let drafts =
            match self.run_validation(validation, drafts, context, &mut diagnostics, &mut evidence)
            {
                StageFlow::Accepted(value) => value,
                StageFlow::Completed(outcome) => {
                    return self.completed(outcome, diagnostics, evidence);
                }
                StageFlow::Failed(failure) => return self.failed(failure),
            };
        self.run_finalizer(finalizer, drafts, context, diagnostics, evidence)
    }

    fn evaluate_compatibility(
        self,
        producer: &PreNormalizedProducerBinding,
        context: &BaseContextSnapshot,
    ) -> PipelineEvaluationResult {
        let mut diagnostics = DiagnosticSet::empty();
        let mut evidence = Vec::new();
        let signals = match stage_output(
            producer.executable().produce(self.input.event(), context),
            producer,
            EvaluationStage::Finalization,
            &mut diagnostics,
            &mut evidence,
            producer.descriptor().empty_output(),
        ) {
            StageFlow::Accepted(value) => value.into_inner(),
            StageFlow::Completed(outcome) => return self.completed(outcome, diagnostics, evidence),
            StageFlow::Failed(failure) => return self.failed(failure),
        };
        match validate_pre_normalized_batch(signals) {
            Ok(finalized) => self.accept_candidates(finalized, diagnostics, evidence),
            Err(error) => self.core_rejected(error.to_string(), diagnostics, evidence),
        }
    }

    fn run_validation(
        &self,
        validation: &DraftValidationStep,
        drafts: super::component::DraftBatch,
        context: &BaseContextSnapshot,
        diagnostics: &mut DiagnosticSet,
        evidence: &mut Vec<StageEvidence>,
    ) -> StageFlow<super::component::DraftBatch> {
        match validation {
            DraftValidationStep::NoneDeclared => StageFlow::Accepted(drafts),
            DraftValidationStep::Component(binding) => stage_output(
                binding
                    .executable()
                    .validate(drafts, self.input.event(), context),
                binding.as_ref(),
                EvaluationStage::DraftValidation,
                diagnostics,
                evidence,
                binding.descriptor().empty_output(),
            ),
        }
    }

    fn run_finalizer(
        self,
        finalizer: &FinalizerBinding,
        drafts: super::component::DraftBatch,
        context: &BaseContextSnapshot,
        mut diagnostics: DiagnosticSet,
        mut evidence: Vec<StageEvidence>,
    ) -> PipelineEvaluationResult {
        let finalized = match stage_output(
            finalizer
                .executable()
                .finalize(drafts, self.input.event(), context),
            finalizer,
            EvaluationStage::Finalization,
            &mut diagnostics,
            &mut evidence,
            finalizer.descriptor().empty_output(),
        ) {
            StageFlow::Accepted(value) => value.into_inner(),
            StageFlow::Completed(outcome) => return self.completed(outcome, diagnostics, evidence),
            StageFlow::Failed(failure) => return self.failed(failure),
        };
        match validate_finalized_batch(finalized) {
            Ok(finalized) => self.accept_candidates(finalized, diagnostics, evidence),
            Err(error) => self.core_rejected(error.to_string(), diagnostics, evidence),
        }
    }

    fn accept_candidates(
        self,
        finalized: Vec<super::signal::FinalizedSignal>,
        diagnostics: DiagnosticSet,
        evidence: Vec<StageEvidence>,
    ) -> PipelineEvaluationResult {
        let provenance = SourceProvenanceDraft::from_input(
            self.input.event(),
            self.input.source_adapter().clone(),
        );
        let pipeline = self
            .identity
            .selected_pipeline()
            .expect("prepared evaluation has selected pipeline")
            .clone();
        let candidates = finalized
            .into_iter()
            .enumerate()
            .map(|(ordinal, finalized)| {
                let (signal, hint, signal_diagnostics, correlation_hints) =
                    into_candidate_parts(finalized);
                let mut candidate_diagnostics = diagnostics.clone();
                candidate_diagnostics.append(signal_diagnostics);
                let pipeline_evidence =
                    PipelineCandidateEvidence::try_new(pipeline.clone(), evidence.clone())
                        .expect("compiled stage count is bounded");
                NormalizationCandidate::new(
                    signal,
                    provenance.clone(),
                    pipeline_evidence,
                    hint,
                    ordinal as u32,
                    candidate_diagnostics,
                    correlation_hints,
                )
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return self.rejected("empty_finalized_batch", diagnostics, evidence);
        }
        self.completed(
            NormalizationOutcome::Accepted {
                candidates: NonEmptyContractList::try_new(candidates, "candidates")
                    .expect("candidate batch is non-empty and bounded"),
            },
            diagnostics,
            evidence,
        )
    }

    fn core_rejected(
        self,
        message: String,
        mut diagnostics: DiagnosticSet,
        evidence: Vec<StageEvidence>,
    ) -> PipelineEvaluationResult {
        if let Ok(diagnostic) = Diagnostic::try_new(
            "core_validation_failed",
            DiagnosticSeverity::Error,
            DiagnosticRedaction::SafeToPersist,
            message,
        ) {
            diagnostics.append(DiagnosticSet::try_new(vec![diagnostic]).unwrap());
        }
        self.rejected("core_validation_failed", diagnostics, evidence)
    }

    fn rejected(
        self,
        reason: &str,
        diagnostics: DiagnosticSet,
        evidence: Vec<StageEvidence>,
    ) -> PipelineEvaluationResult {
        self.completed(
            NormalizationOutcome::Rejected {
                reason: rejection_reason(reason),
            },
            diagnostics,
            evidence,
        )
    }

    fn completed(
        self,
        outcome: NormalizationOutcome,
        diagnostics: DiagnosticSet,
        stages: Vec<StageEvidence>,
    ) -> PipelineEvaluationResult {
        PipelineEvaluationResult::Completed(NormalizationEvaluationReport::new(
            self.identity,
            outcome,
            diagnostics,
            EvaluationEvidence::try_new(self.route_evidence, stages)
                .expect("compiled evidence ceilings are enforced"),
        ))
    }

    fn failed(self, failure: StageExecutionFailure) -> PipelineEvaluationResult {
        PipelineEvaluationResult::Failed(EvaluationFailure::new(
            self.identity,
            failure.class(),
            failure.retry_safety(),
            failure.completion_knowledge(),
            failure.diagnostics().clone(),
        ))
    }
}

trait BindingView {
    fn resolved(&self) -> &super::identity::ResolvedComponentRef;
}

macro_rules! binding_view {
    ($($binding:ty),+ $(,)?) => {
        $(
            impl BindingView for $binding {
                fn resolved(&self) -> &super::identity::ResolvedComponentRef {
                    self.resolved()
                }
            }
        )+
    };
}

binding_view!(
    DecoderBinding,
    ParserBinding,
    MeaningNormalizerBinding,
    DraftValidatorBinding,
    FinalizerBinding,
    PreNormalizedProducerBinding,
);

enum StageFlow<T> {
    Accepted(T),
    Completed(NormalizationOutcome),
    Failed(StageExecutionFailure),
}

fn stage_output<T, B: BindingView>(
    result: Result<ComponentReport<T>, StageExecutionFailure>,
    binding: &B,
    stage: EvaluationStage,
    diagnostics: &mut DiagnosticSet,
    evidence: &mut Vec<StageEvidence>,
    empty_policy: EmptyOutputPolicy,
) -> StageFlow<T>
where
    T: BoundedStageOutput,
{
    let report = match result {
        Ok(value) => value,
        Err(failure) => return StageFlow::Failed(failure),
    };
    let (disposition, stage_diagnostics, facts) = report.into_parts();
    diagnostics.append(stage_diagnostics);
    evidence.push(StageEvidence::new(stage, binding.resolved().clone(), facts));
    match disposition {
        StageDisposition::Accepted(output) if output.is_empty() => match empty_policy {
            EmptyOutputPolicy::Ignore => StageFlow::Completed(NormalizationOutcome::Ignored {
                reason: ignore_reason("empty_output"),
            }),
            EmptyOutputPolicy::Reject => StageFlow::Completed(NormalizationOutcome::Rejected {
                reason: rejection_reason("empty_output"),
            }),
        },
        StageDisposition::Accepted(output) => StageFlow::Accepted(output),
        StageDisposition::Ignored(reason) => {
            StageFlow::Completed(NormalizationOutcome::Ignored { reason })
        }
        StageDisposition::Rejected(reason) => {
            StageFlow::Completed(NormalizationOutcome::Rejected { reason })
        }
        StageDisposition::Ambiguous(alternatives) => {
            let total = alternatives
                .iter()
                .map(BoundedStageOutput::len)
                .sum::<usize>();
            if total > 64 {
                return StageFlow::Completed(NormalizationOutcome::Rejected {
                    reason: rejection_reason("ambiguity_evidence_overflow"),
                });
            }
            let values = alternatives
                .into_inner()
                .into_iter()
                .enumerate()
                .map(|(ordinal, value)| {
                    AmbiguityAlternativeEvidence::new(None, ordinal as u32, value.len() as u32)
                })
                .collect::<Vec<_>>();
            if values.len() < 2 {
                return StageFlow::Completed(NormalizationOutcome::Rejected {
                    reason: rejection_reason("invalid_ambiguity"),
                });
            }
            StageFlow::Completed(NormalizationOutcome::Ambiguous {
                evidence: AmbiguityEvidence::try_new(values, total as u32)
                    .expect("ambiguity bounds were checked"),
            })
        }
    }
}

trait BoundedStageOutput {
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
}

impl<T, const MAX: usize> BoundedStageOutput for ContractList<T, MAX> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl BoundedStageOutput for super::signal::PreNormalizedSignalBatch {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

fn route_terminal_report(
    identity: EvaluationIdentity,
    outcome: NormalizationOutcome,
    route_matches: Vec<RouteMatchEvidence>,
) -> NormalizationEvaluationReport {
    NormalizationEvaluationReport::new(
        identity,
        outcome,
        DiagnosticSet::empty(),
        EvaluationEvidence::try_new(route_matches, vec![])
            .expect("compiled route evidence is bounded"),
    )
}

fn route_matches_pipeline(route: &RouteSpec, pipeline: &CompiledPipeline) -> bool {
    match &pipeline.kind {
        CompiledPipelineKind::Structured { decoder, .. } => {
            route.selector.payload_kind == Some(PayloadKind::Structured)
                && decoder
                    .descriptor()
                    .structured_inputs()
                    .iter()
                    .any(|capability| {
                        route
                            .selector
                            .schema
                            .as_ref()
                            .is_none_or(|schema| schema == capability.schema())
                            && route
                                .selector
                                .encoding
                                .is_none_or(|encoding| encoding == capability.encoding())
                    })
        }
        CompiledPipelineKind::Text { .. } => {
            route.selector.payload_kind == Some(PayloadKind::Text)
                && route.selector.schema.is_none()
                && route.selector.encoding.is_none()
        }
        CompiledPipelineKind::CompatibilityPreNormalized { .. } => true,
    }
}

fn encode_routes(routes: &[RouteSpec]) -> Result<Vec<u8>, IdentityError> {
    let mut writer = CanonicalWriter::new();
    writer.u16(1);
    writer.u32(routes.len() as u32);
    for route in routes {
        writer.text(route.id.as_str())?;
        writer.i64(route.priority);
        route.selector.encode(&mut writer)?;
        route.target.encode(&mut writer)?;
    }
    Ok(writer.into_bytes())
}

fn encode_option_text(
    value: Option<&str>,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match value {
        None => writer.bool(false),
        Some(value) => {
            writer.bool(true);
            writer.text(value)?;
        }
    }
    Ok(())
}

fn encode_option_u16(value: Option<u16>, writer: &mut CanonicalWriter) {
    match value {
        None => writer.bool(false),
        Some(value) => {
            writer.bool(true);
            writer.u16(value);
        }
    }
}

fn operation_tag(value: SourceOperation) -> u16 {
    match value {
        SourceOperation::Create => 1,
        SourceOperation::Update => 2,
        SourceOperation::Delete => 3,
        SourceOperation::Upsert => 4,
        SourceOperation::Snapshot => 5,
    }
}

fn encoding_tag(value: PayloadEncoding) -> u16 {
    match value {
        PayloadEncoding::Json => 1,
        PayloadEncoding::Cbor => 2,
        PayloadEncoding::MessagePack => 3,
        PayloadEncoding::Binary => 4,
    }
}

fn text_format_tag(value: TextFormat) -> u16 {
    match value {
        TextFormat::Plain => 1,
        TextFormat::Markdown => 2,
        TextFormat::Html => 3,
    }
}

fn ignore_reason(value: &str) -> IgnoreReason {
    IgnoreReason::try_new(value).expect("static ignore code is valid")
}

fn rejection_reason(value: &str) -> RejectionReason {
    RejectionReason::try_new(value).expect("static rejection code is valid")
}

#[derive(Debug, thiserror::Error)]
pub enum GraphCompileError {
    #[error(transparent)]
    Value(#[from] ContractValueError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("parser outputs and normalizer inputs do not form one exact meaning edge")]
    MeaningContractMismatch,
    #[error("routing graph has more than 16 routes")]
    TooManyRoutes,
    #[error("duplicate pipeline identity")]
    DuplicatePipeline,
    #[error("duplicate route identity")]
    DuplicateRoute,
    #[error("route references an unknown pipeline")]
    UnknownPipeline,
    #[error("route payload contract is incompatible with its target pipeline")]
    RoutePipelineMismatch,
    #[error("identical selectors target different pipelines")]
    ConflictingSelector,
}
