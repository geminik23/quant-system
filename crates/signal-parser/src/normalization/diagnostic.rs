use super::identity::{PipelineIdentity, ResolvedComponentRef};
use super::value::{
    ContractList, ContractText, ContractValueError, DiagnosticCode, DiagnosticText, Sha256Digest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Note,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticRedaction {
    SafeToPersist,
    SensitiveValuesRedacted,
    RestrictedToOperator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    code: DiagnosticCode,
    severity: DiagnosticSeverity,
    redaction: DiagnosticRedaction,
    message: DiagnosticText,
}

impl Diagnostic {
    pub fn try_new(
        code: impl Into<String>,
        severity: DiagnosticSeverity,
        redaction: DiagnosticRedaction,
        message: impl Into<String>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            code: DiagnosticCode::try_new(code, "diagnostic code")?,
            severity,
            redaction,
            message: DiagnosticText::try_new(message, "diagnostic message")?,
        })
    }

    pub fn code(&self) -> &DiagnosticCode {
        &self.code
    }

    pub fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    pub fn redaction(&self) -> DiagnosticRedaction {
        self.redaction
    }

    pub fn message(&self) -> &DiagnosticText {
        &self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticSet {
    items: ContractList<Diagnostic, 64>,
    omitted_count: u32,
}

impl DiagnosticSet {
    pub fn empty() -> Self {
        Self {
            items: ContractList::empty(),
            omitted_count: 0,
        }
    }

    pub fn try_new(items: Vec<Diagnostic>) -> Result<Self, ContractValueError> {
        Ok(Self {
            items: ContractList::try_new(items, "diagnostics")?,
            omitted_count: 0,
        })
    }

    pub fn items(&self) -> &[Diagnostic] {
        self.items.as_slice()
    }

    pub fn omitted_count(&self) -> u32 {
        self.omitted_count
    }

    pub(crate) fn append(&mut self, other: Self) {
        for item in other.items.into_inner() {
            if self.items.try_push(item, "diagnostics").is_err() {
                self.omitted_count = self.omitted_count.saturating_add(1);
            }
        }
        self.omitted_count = self.omitted_count.saturating_add(other.omitted_count);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoreReason(ContractText<128>);

impl IgnoreReason {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ContractValueError> {
        let value = ContractText::try_new(value, "ignore reason")?;
        if value.as_str().is_empty() {
            return Err(ContractValueError::Empty {
                field: "ignore reason",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectionReason(ContractText<128>);

impl RejectionReason {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ContractValueError> {
        let value = ContractText::try_new(value, "rejection reason")?;
        if value.as_str().is_empty() {
            return Err(ContractValueError::Empty {
                field: "rejection reason",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceFact {
    code: DiagnosticCode,
    value: ContractText<512>,
}

impl EvidenceFact {
    pub fn try_new(
        code: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            code: DiagnosticCode::try_new(code, "evidence code")?,
            value: ContractText::try_new(value, "evidence value")?,
        })
    }

    pub fn code(&self) -> &DiagnosticCode {
        &self.code
    }

    pub fn value(&self) -> &ContractText<512> {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StageDisposition<T> {
    Accepted(T),
    Ignored(IgnoreReason),
    Ambiguous(ContractList<T, 8>),
    Rejected(RejectionReason),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComponentReport<T> {
    disposition: StageDisposition<T>,
    diagnostics: DiagnosticSet,
    facts: ContractList<EvidenceFact, 16>,
}

impl<T> ComponentReport<T> {
    pub fn accepted(output: T) -> Self {
        Self {
            disposition: StageDisposition::Accepted(output),
            diagnostics: DiagnosticSet::empty(),
            facts: ContractList::empty(),
        }
    }

    pub fn ignored(reason: IgnoreReason) -> Self {
        Self {
            disposition: StageDisposition::Ignored(reason),
            diagnostics: DiagnosticSet::empty(),
            facts: ContractList::empty(),
        }
    }

    pub fn rejected(reason: RejectionReason) -> Self {
        Self {
            disposition: StageDisposition::Rejected(reason),
            diagnostics: DiagnosticSet::empty(),
            facts: ContractList::empty(),
        }
    }

    pub fn try_ambiguous(alternatives: Vec<T>) -> Result<Self, ContractValueError> {
        if alternatives.len() < 2 {
            return Err(ContractValueError::LimitExceeded {
                field: "ambiguity alternatives",
                maximum: 8,
                actual: alternatives.len(),
            });
        }
        Ok(Self {
            disposition: StageDisposition::Ambiguous(ContractList::try_new(
                alternatives,
                "ambiguity alternatives",
            )?),
            diagnostics: DiagnosticSet::empty(),
            facts: ContractList::empty(),
        })
    }

    pub fn with_diagnostics(mut self, diagnostics: DiagnosticSet) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub fn with_facts(mut self, facts: Vec<EvidenceFact>) -> Result<Self, ContractValueError> {
        self.facts = ContractList::try_new(facts, "component facts")?;
        Ok(self)
    }

    pub fn disposition(&self) -> &StageDisposition<T> {
        &self.disposition
    }

    pub fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        StageDisposition<T>,
        DiagnosticSet,
        ContractList<EvidenceFact, 16>,
    ) {
        (self.disposition, self.diagnostics, self.facts)
    }
}

pub type ComponentResult<T> = Result<ComponentReport<T>, StageExecutionFailure>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationFailureClass {
    ContextReadFailed,
    HostUnavailable,
    ComponentUnavailable,
    DeadlineExceeded,
    Cancelled,
    ResourceExhausted,
    ExternalProtocolFailed,
    InternalContractFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationRetrySafety {
    SafeToRetry,
    UnsafeToRetry,
    RequiresIdempotencyEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionKnowledge {
    NotStarted,
    StartedMayHaveCompleted,
    CompletedWithoutSemanticReport,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageExecutionFailure {
    class: EvaluationFailureClass,
    retry_safety: EvaluationRetrySafety,
    completion_knowledge: CompletionKnowledge,
    diagnostics: DiagnosticSet,
}

impl StageExecutionFailure {
    pub fn new(
        class: EvaluationFailureClass,
        retry_safety: EvaluationRetrySafety,
        completion_knowledge: CompletionKnowledge,
        diagnostics: DiagnosticSet,
    ) -> Self {
        Self {
            class,
            retry_safety,
            completion_knowledge,
            diagnostics,
        }
    }

    pub fn class(&self) -> EvaluationFailureClass {
        self.class
    }

    pub fn retry_safety(&self) -> EvaluationRetrySafety {
        self.retry_safety
    }

    pub fn completion_knowledge(&self) -> CompletionKnowledge {
        self.completion_knowledge
    }

    pub fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationStage {
    Decoding,
    Parsing,
    MeaningNormalization,
    DraftValidation,
    Finalization,
    CoreValidation,
    CandidateConstruction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageEvidence {
    stage: EvaluationStage,
    component: ResolvedComponentRef,
    input_digest: Sha256Digest,
    output_digest: Option<Sha256Digest>,
    facts: ContractList<EvidenceFact, 16>,
}

impl StageEvidence {
    pub(crate) fn new(
        stage: EvaluationStage,
        component: ResolvedComponentRef,
        input_digest: Sha256Digest,
        output_digest: Option<Sha256Digest>,
        facts: ContractList<EvidenceFact, 16>,
    ) -> Self {
        Self {
            stage,
            component,
            input_digest,
            output_digest,
            facts,
        }
    }

    pub fn stage(&self) -> EvaluationStage {
        self.stage
    }

    pub fn component(&self) -> &ResolvedComponentRef {
        &self.component
    }

    pub fn input_digest(&self) -> Sha256Digest {
        self.input_digest
    }

    pub fn output_digest(&self) -> Option<Sha256Digest> {
        self.output_digest
    }

    pub fn facts(&self) -> &[EvidenceFact] {
        self.facts.as_slice()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteMatchEvidence {
    route_id: ContractText<128>,
    priority: i64,
    target_pipeline: PipelineIdentity,
}

impl RouteMatchEvidence {
    pub(crate) fn new(
        route_id: ContractText<128>,
        priority: i64,
        target_pipeline: PipelineIdentity,
    ) -> Self {
        Self {
            route_id,
            priority,
            target_pipeline,
        }
    }

    pub fn route_id(&self) -> &str {
        self.route_id.as_str()
    }

    pub fn priority(&self) -> i64 {
        self.priority
    }

    pub fn target_pipeline(&self) -> &PipelineIdentity {
        &self.target_pipeline
    }
}
