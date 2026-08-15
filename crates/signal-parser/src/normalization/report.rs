use super::diagnostic::{
    CompletionKnowledge, DiagnosticSet, EvaluationFailureClass, EvaluationRetrySafety,
    IgnoreReason, RejectionReason, RouteMatchEvidence, StageEvidence,
};
use super::identity::{PipelineIdentity, RoutingGraphIdentity};
use super::signal::NormalizationCandidate;
use super::value::{ContractList, ContractValueError, NonEmptyContractList};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationIdentity {
    routing_graph: RoutingGraphIdentity,
    selected_pipeline: Option<PipelineIdentity>,
}

impl EvaluationIdentity {
    pub fn new(
        routing_graph: RoutingGraphIdentity,
        selected_pipeline: Option<PipelineIdentity>,
    ) -> Self {
        Self {
            routing_graph,
            selected_pipeline,
        }
    }

    pub fn routing_graph(&self) -> RoutingGraphIdentity {
        self.routing_graph.clone()
    }

    pub fn selected_pipeline(&self) -> Option<&PipelineIdentity> {
        self.selected_pipeline.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguityAlternativeEvidence {
    pipeline: Option<PipelineIdentity>,
    alternative_ordinal: u32,
    value_count: u32,
}

impl AmbiguityAlternativeEvidence {
    pub fn new(
        pipeline: Option<PipelineIdentity>,
        alternative_ordinal: u32,
        value_count: u32,
    ) -> Self {
        Self {
            pipeline,
            alternative_ordinal,
            value_count,
        }
    }

    pub fn pipeline(&self) -> Option<&PipelineIdentity> {
        self.pipeline.as_ref()
    }

    pub fn alternative_ordinal(&self) -> u32 {
        self.alternative_ordinal
    }

    pub fn value_count(&self) -> u32 {
        self.value_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguityEvidence {
    alternatives: NonEmptyContractList<AmbiguityAlternativeEvidence, 8>,
    total_alternative_values: u32,
}

impl AmbiguityEvidence {
    pub fn try_new(
        alternatives: Vec<AmbiguityAlternativeEvidence>,
        total_alternative_values: u32,
    ) -> Result<Self, ContractValueError> {
        if alternatives.len() < 2 || total_alternative_values > 64 {
            return Err(ContractValueError::LimitExceeded {
                field: "ambiguity evidence",
                maximum: 64,
                actual: total_alternative_values as usize,
            });
        }
        Ok(Self {
            alternatives: NonEmptyContractList::try_new(alternatives, "ambiguity alternatives")?,
            total_alternative_values,
        })
    }

    pub fn alternatives(&self) -> &[AmbiguityAlternativeEvidence] {
        self.alternatives.as_slice()
    }

    pub fn total_alternative_values(&self) -> u32 {
        self.total_alternative_values
    }
}

#[derive(Debug)]
pub enum NormalizationOutcome {
    Accepted {
        candidates: NonEmptyContractList<NormalizationCandidate, 32>,
    },
    Ignored {
        reason: IgnoreReason,
    },
    Ambiguous {
        evidence: AmbiguityEvidence,
    },
    Rejected {
        reason: RejectionReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationEvidence {
    route_matches: ContractList<RouteMatchEvidence, 16>,
    stages: ContractList<StageEvidence, 8>,
}

impl EvaluationEvidence {
    pub fn try_new(
        route_matches: Vec<RouteMatchEvidence>,
        stages: Vec<StageEvidence>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            route_matches: ContractList::try_new(route_matches, "route evidence")?,
            stages: ContractList::try_new(stages, "stage evidence")?,
        })
    }

    pub fn route_matches(&self) -> &[RouteMatchEvidence] {
        self.route_matches.as_slice()
    }

    pub fn stages(&self) -> &[StageEvidence] {
        self.stages.as_slice()
    }
}

#[derive(Debug)]
pub struct NormalizationEvaluationReport {
    identity: EvaluationIdentity,
    outcome: NormalizationOutcome,
    diagnostics: DiagnosticSet,
    evidence: EvaluationEvidence,
}

impl NormalizationEvaluationReport {
    pub(crate) fn new(
        identity: EvaluationIdentity,
        outcome: NormalizationOutcome,
        diagnostics: DiagnosticSet,
        evidence: EvaluationEvidence,
    ) -> Self {
        Self {
            identity,
            outcome,
            diagnostics,
            evidence,
        }
    }

    pub fn identity(&self) -> &EvaluationIdentity {
        &self.identity
    }

    pub fn outcome(&self) -> &NormalizationOutcome {
        &self.outcome
    }

    pub fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }

    pub fn evidence(&self) -> &EvaluationEvidence {
        &self.evidence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationFailure {
    identity: EvaluationIdentity,
    class: EvaluationFailureClass,
    retry_safety: EvaluationRetrySafety,
    completion_knowledge: CompletionKnowledge,
    diagnostics: DiagnosticSet,
}

impl EvaluationFailure {
    pub(crate) fn new(
        identity: EvaluationIdentity,
        class: EvaluationFailureClass,
        retry_safety: EvaluationRetrySafety,
        completion_knowledge: CompletionKnowledge,
        diagnostics: DiagnosticSet,
    ) -> Self {
        Self {
            identity,
            class,
            retry_safety,
            completion_knowledge,
            diagnostics,
        }
    }

    pub fn identity(&self) -> &EvaluationIdentity {
        &self.identity
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

#[derive(Debug)]
pub enum PipelineEvaluationResult {
    Completed(NormalizationEvaluationReport),
    Failed(EvaluationFailure),
}
