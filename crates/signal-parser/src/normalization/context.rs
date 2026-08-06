use crate::ingestion::{DateTimeUtc, SourceEvent};

use super::value::{ByteLimit, ContractBytes, ContractList, ContractValueError, ItemLimit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentRequirement {
    None,
    Optional,
    Required,
}

impl ParentRequirement {
    pub(crate) fn tag(self) -> u16 {
        match self {
            Self::None => 1,
            Self::Optional => 2,
            Self::Required => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRequirement {
    maximum_items: ItemLimit,
    maximum_bytes: ByteLimit,
    include_payload: bool,
    include_adapter_evidence: bool,
}

impl HistoryRequirement {
    pub fn new(
        maximum_items: ItemLimit,
        maximum_bytes: ByteLimit,
        include_payload: bool,
        include_adapter_evidence: bool,
    ) -> Self {
        Self {
            maximum_items,
            maximum_bytes,
            include_payload,
            include_adapter_evidence,
        }
    }

    pub fn maximum_items(&self) -> ItemLimit {
        self.maximum_items
    }

    pub fn maximum_bytes(&self) -> ByteLimit {
        self.maximum_bytes
    }

    pub fn include_payload(&self) -> bool {
        self.include_payload
    }

    pub fn include_adapter_evidence(&self) -> bool {
        self.include_adapter_evidence
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineContextRequirements {
    history: Option<HistoryRequirement>,
    parent: ParentRequirement,
    maximum_items: ItemLimit,
    maximum_bytes: ByteLimit,
}

impl PipelineContextRequirements {
    pub fn none() -> Self {
        Self {
            history: None,
            parent: ParentRequirement::None,
            maximum_items: ItemLimit::new(0),
            maximum_bytes: ByteLimit::new(0),
        }
    }

    pub fn new(
        history: Option<HistoryRequirement>,
        parent: ParentRequirement,
        maximum_items: ItemLimit,
        maximum_bytes: ByteLimit,
    ) -> Self {
        Self {
            history,
            parent,
            maximum_items,
            maximum_bytes,
        }
    }

    pub fn history(&self) -> Option<&HistoryRequirement> {
        self.history.as_ref()
    }

    pub fn parent(&self) -> ParentRequirement {
        self.parent
    }

    pub fn maximum_items(&self) -> ItemLimit {
        self.maximum_items
    }

    pub fn maximum_bytes(&self) -> ByteLimit {
        self.maximum_bytes
    }

    pub(crate) fn merge(&self, other: &Self) -> Self {
        let history = match (&self.history, &other.history) {
            (None, None) => None,
            (Some(value), None) | (None, Some(value)) => Some(value.clone()),
            (Some(left), Some(right)) => Some(HistoryRequirement::new(
                ItemLimit::new(left.maximum_items().get().max(right.maximum_items().get())),
                ByteLimit::new(left.maximum_bytes().get().max(right.maximum_bytes().get())),
                left.include_payload() || right.include_payload(),
                left.include_adapter_evidence() || right.include_adapter_evidence(),
            )),
        };
        let parent = match (self.parent, other.parent) {
            (ParentRequirement::Required, _) | (_, ParentRequirement::Required) => {
                ParentRequirement::Required
            }
            (ParentRequirement::Optional, _) | (_, ParentRequirement::Optional) => {
                ParentRequirement::Optional
            }
            _ => ParentRequirement::None,
        };
        Self::new(
            history,
            parent,
            ItemLimit::new(self.maximum_items.get().max(other.maximum_items.get())),
            ByteLimit::new(self.maximum_bytes.get().max(other.maximum_bytes.get())),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextCutoff {
    as_of: DateTimeUtc,
}

impl ContextCutoff {
    pub fn new(as_of: DateTimeUtc) -> Self {
        Self { as_of }
    }

    pub fn as_of(&self) -> DateTimeUtc {
        self.as_of
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalSourceFact {
    event: SourceEvent,
    adapter_evidence: Option<ContractBytes<65536>>,
}

impl HistoricalSourceFact {
    pub fn new(event: SourceEvent, adapter_evidence: Option<ContractBytes<65536>>) -> Self {
        Self {
            event,
            adapter_evidence,
        }
    }

    pub fn event(&self) -> &SourceEvent {
        &self.event
    }

    pub fn adapter_evidence(&self) -> Option<&ContractBytes<65536>> {
        self.adapter_evidence.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryView {
    facts: ContractList<HistoricalSourceFact, 64>,
    encoded_bytes: u64,
    truncated: bool,
}

impl HistoryView {
    pub fn try_new(
        facts: Vec<HistoricalSourceFact>,
        encoded_bytes: u64,
        truncated: bool,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            facts: ContractList::try_new(facts, "history facts")?,
            encoded_bytes,
            truncated,
        })
    }

    pub fn facts(&self) -> &[HistoricalSourceFact] {
        self.facts.as_slice()
    }

    pub fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentView {
    fact: HistoricalSourceFact,
}

impl ParentView {
    pub fn new(fact: HistoricalSourceFact) -> Self {
        Self { fact }
    }

    pub fn fact(&self) -> &HistoricalSourceFact {
        &self.fact
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationClock {
    observed_at: DateTimeUtc,
}

impl EvaluationClock {
    pub fn new(observed_at: DateTimeUtc) -> Self {
        Self { observed_at }
    }

    pub fn observed_at(&self) -> DateTimeUtc {
        self.observed_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseContextSnapshot {
    cutoff: ContextCutoff,
    history: Option<HistoryView>,
    parent: Option<ParentView>,
    clock: EvaluationClock,
}

impl BaseContextSnapshot {
    pub fn empty(at: DateTimeUtc) -> Self {
        Self {
            cutoff: ContextCutoff::new(at),
            history: None,
            parent: None,
            clock: EvaluationClock::new(at),
        }
    }

    pub fn try_new(
        cutoff: ContextCutoff,
        history: Option<HistoryView>,
        parent: Option<ParentView>,
        clock: EvaluationClock,
        requirements: &PipelineContextRequirements,
    ) -> Result<Self, ContextValidationError> {
        if requirements.history().is_none() && history.is_some() {
            return Err(ContextValidationError::UnexpectedHistory);
        }
        if let Some(required) = requirements.history() {
            let history = history
                .as_ref()
                .ok_or(ContextValidationError::MissingHistory)?;
            if history.facts().len() > required.maximum_items().get() as usize
                || history.encoded_bytes() > required.maximum_bytes().get()
            {
                return Err(ContextValidationError::HistoryLimitExceeded);
            }
        }
        match requirements.parent() {
            ParentRequirement::None if parent.is_some() => {
                return Err(ContextValidationError::UnexpectedParent);
            }
            ParentRequirement::Required if parent.is_none() => {
                return Err(ContextValidationError::MissingParent);
            }
            _ => {}
        }
        Ok(Self {
            cutoff,
            history,
            parent,
            clock,
        })
    }

    pub fn cutoff(&self) -> ContextCutoff {
        self.cutoff
    }

    pub fn history(&self) -> Option<&HistoryView> {
        self.history.as_ref()
    }

    pub fn parent(&self) -> Option<&ParentView> {
        self.parent.as_ref()
    }

    pub fn clock(&self) -> EvaluationClock {
        self.clock
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ContextValidationError {
    #[error("required history is missing")]
    MissingHistory,
    #[error("history was supplied but not declared")]
    UnexpectedHistory,
    #[error("history exceeds compiled limits")]
    HistoryLimitExceeded,
    #[error("required parent is missing")]
    MissingParent,
    #[error("parent was supplied but not declared")]
    UnexpectedParent,
}
