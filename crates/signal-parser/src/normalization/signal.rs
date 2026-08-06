use qs_core::{
    OrderType, PositionRef, RawSignal, RawSignalValidationError, RuleConfigDef, Side,
    validate_raw_signal,
};

use crate::ingestion::{
    DateTimeUtc, SourceEvent, SourceEventRef, SourceOperation, SourceTimestamp,
};

use super::diagnostic::{DiagnosticSet, StageEvidence};
use super::identity::{PipelineIdentity, SemanticVersion};
use super::value::{
    ContractList, ContractValueError, FiniteF64, GroupText, PositiveFiniteF64, RuleNameText,
    Sha256Digest, SymbolText, TradeKeyText, UnitInterval,
};

#[derive(Debug, Clone, PartialEq)]
pub enum PositionDraftRef {
    ByTradeId { trade_id: TradeKeyText },
    AllOnSymbol { symbol: SymbolText },
    AllInGroup { group_id: GroupText },
}

impl PositionDraftRef {
    fn into_raw(self) -> PositionRef {
        match self {
            Self::ByTradeId { trade_id } => PositionRef::ByTradeId {
                trade_id: trade_id.into_inner(),
            },
            Self::AllOnSymbol { symbol } => PositionRef::AllOnSymbol {
                symbol: symbol.into_inner(),
            },
            Self::AllInGroup { group_id } => PositionRef::AllInGroup {
                group_id: group_id.into_inner(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RuleDraft {
    FixedStoploss {
        price: FiniteF64,
    },
    TrailingStop {
        distance: FiniteF64,
    },
    TakeProfit {
        price: FiniteF64,
        close_ratio: FiniteF64,
    },
    BreakevenWhen {
        trigger_price: FiniteF64,
    },
    BreakevenWhenOffset {
        trigger_price_offset: FiniteF64,
    },
    BreakevenAfterTargets {
        after_n: u32,
    },
    TimeExit {
        max_seconds: u64,
    },
}

impl RuleDraft {
    fn into_raw(self) -> RuleConfigDef {
        match self {
            Self::FixedStoploss { price } => RuleConfigDef::FixedStoploss { price: price.get() },
            Self::TrailingStop { distance } => RuleConfigDef::TrailingStop {
                distance: distance.get(),
            },
            Self::TakeProfit { price, close_ratio } => RuleConfigDef::TakeProfit {
                price: price.get(),
                close_ratio: close_ratio.get(),
            },
            Self::BreakevenWhen { trigger_price } => RuleConfigDef::BreakevenWhen {
                trigger_price: trigger_price.get(),
            },
            Self::BreakevenWhenOffset {
                trigger_price_offset,
            } => RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset: trigger_price_offset.get(),
            },
            Self::BreakevenAfterTargets { after_n } => {
                RuleConfigDef::BreakevenAfterTargets { after_n }
            }
            Self::TimeExit { max_seconds } => RuleConfigDef::TimeExit { max_seconds },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SignalDraftAction {
    Entry {
        symbol: SymbolText,
        side: Side,
        order_type: OrderType,
        price: Option<FiniteF64>,
        risk: PositiveFiniteF64,
        stoploss: Option<FiniteF64>,
        targets: ContractList<FiniteF64, 32>,
        group: Option<GroupText>,
        trade_id: Option<TradeKeyText>,
    },
    Close {
        position: PositionDraftRef,
    },
    ClosePartial {
        position: PositionDraftRef,
        ratio: FiniteF64,
    },
    ModifyStoploss {
        position: PositionDraftRef,
        price: FiniteF64,
    },
    MoveStoplossToEntry {
        position: PositionDraftRef,
    },
    AddTarget {
        position: PositionDraftRef,
        price: FiniteF64,
        close_ratio: FiniteF64,
    },
    RemoveTarget {
        position: PositionDraftRef,
        price: FiniteF64,
    },
    ModifyTarget {
        position: PositionDraftRef,
        old_price: FiniteF64,
        new_price: FiniteF64,
    },
    AddRule {
        position: PositionDraftRef,
        rule: RuleDraft,
    },
    RemoveRule {
        position: PositionDraftRef,
        rule_name: RuleNameText,
    },
    ScaleIn {
        position: PositionDraftRef,
        price: Option<FiniteF64>,
        size: FiniteF64,
    },
    CancelPending {
        position: PositionDraftRef,
    },
    CloseAllOf {
        symbol: SymbolText,
    },
    CloseAll,
    CancelAllPending,
    ModifyAllStoploss {
        symbol: SymbolText,
        price: FiniteF64,
    },
    CloseAllInGroup {
        group_id: GroupText,
    },
    ModifyAllStoplossInGroup {
        group_id: GroupText,
        price: FiniteF64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentHint {
    symbol: SymbolText,
    venue_hint: Option<super::value::ContractText<128>>,
    market_kind_hint: Option<super::value::ContractText<128>>,
}

impl InstrumentHint {
    pub fn new(
        symbol: SymbolText,
        venue_hint: Option<super::value::ContractText<128>>,
        market_kind_hint: Option<super::value::ContractText<128>>,
    ) -> Self {
        Self {
            symbol,
            venue_hint,
            market_kind_hint,
        }
    }

    pub fn symbol(&self) -> &SymbolText {
        &self.symbol
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrelationHint {
    key: TradeKeyText,
    confidence: Option<UnitInterval>,
}

impl CorrelationHint {
    pub fn new(key: TradeKeyText, confidence: Option<UnitInterval>) -> Self {
        Self { key, confidence }
    }

    pub fn key(&self) -> &TradeKeyText {
        &self.key
    }

    pub fn confidence(&self) -> Option<UnitInterval> {
        self.confidence
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalDraft {
    effective_at: DateTimeUtc,
    instrument: Option<InstrumentHint>,
    action: SignalDraftAction,
    diagnostics: DiagnosticSet,
    correlation_hints: ContractList<CorrelationHint, 32>,
}

impl SignalDraft {
    pub fn try_new(
        effective_at: DateTimeUtc,
        instrument: Option<InstrumentHint>,
        action: SignalDraftAction,
        diagnostics: DiagnosticSet,
        correlation_hints: Vec<CorrelationHint>,
    ) -> Result<Self, SignalContractError> {
        if let (Some(hint), SignalDraftAction::Entry { symbol, .. }) = (&instrument, &action)
            && hint.symbol() != symbol
        {
            return Err(SignalContractError::InstrumentSymbolMismatch);
        }
        Ok(Self {
            effective_at,
            instrument,
            action,
            diagnostics,
            correlation_hints: ContractList::try_new(correlation_hints, "correlation hints")?,
        })
    }

    pub fn effective_at(&self) -> DateTimeUtc {
        self.effective_at
    }

    pub fn action(&self) -> &SignalDraftAction {
        &self.action
    }

    pub fn instrument(&self) -> Option<&InstrumentHint> {
        self.instrument.as_ref()
    }
}

#[derive(Debug)]
pub struct FinalizedSignal {
    signal: RawSignal,
    instrument_hint: Option<InstrumentHint>,
    diagnostics: DiagnosticSet,
    correlation_hints: ContractList<CorrelationHint, 32>,
}

impl FinalizedSignal {
    pub fn signal(&self) -> &RawSignal {
        &self.signal
    }
}

#[derive(Debug)]
pub struct PreNormalizedSignalBatch(ContractList<RawSignal, 32>);

impl PreNormalizedSignalBatch {
    pub fn try_new(signals: Vec<RawSignal>) -> Result<Self, ContractValueError> {
        Ok(Self(ContractList::try_new(
            signals,
            "pre-normalized signals",
        )?))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn into_inner(self) -> Vec<RawSignal> {
        self.0.into_inner()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceAdapterIdentity {
    id: super::value::NonEmptyContractText<128>,
    version: SemanticVersion,
    config_identity: Sha256Digest,
}

impl SourceAdapterIdentity {
    pub fn new(
        id: super::value::NonEmptyContractText<128>,
        version: SemanticVersion,
        config_identity: Sha256Digest,
    ) -> Self {
        Self {
            id,
            version,
            config_identity,
        }
    }

    pub fn id(&self) -> &super::value::NonEmptyContractText<128> {
        &self.id
    }

    pub fn version(&self) -> &SemanticVersion {
        &self.version
    }

    pub fn config_identity(&self) -> Sha256Digest {
        self.config_identity
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceProvenanceDraft {
    source: SourceEventRef,
    operation: SourceOperation,
    occurred_at: SourceTimestamp,
    received_at: DateTimeUtc,
    source_adapter: SourceAdapterIdentity,
}

impl SourceProvenanceDraft {
    pub(crate) fn from_input(event: &SourceEvent, source_adapter: SourceAdapterIdentity) -> Self {
        Self {
            source: SourceEventRef::from(event),
            operation: event.operation(),
            occurred_at: event.occurred_at(),
            received_at: event.received_at(),
            source_adapter,
        }
    }

    pub fn source(&self) -> &SourceEventRef {
        &self.source
    }

    pub fn source_adapter(&self) -> &SourceAdapterIdentity {
        &self.source_adapter
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineCandidateEvidence {
    pipeline: PipelineIdentity,
    components: ContractList<StageEvidence, 8>,
}

impl PipelineCandidateEvidence {
    pub(crate) fn try_new(
        pipeline: PipelineIdentity,
        components: Vec<StageEvidence>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            pipeline,
            components: ContractList::try_new(components, "pipeline evidence")?,
        })
    }

    pub fn pipeline(&self) -> &PipelineIdentity {
        &self.pipeline
    }
}

#[derive(Debug)]
pub struct NormalizationCandidate {
    signal: RawSignal,
    provenance: SourceProvenanceDraft,
    evidence: PipelineCandidateEvidence,
    instrument_hint: Option<InstrumentHint>,
    candidate_ordinal: u32,
    diagnostics: DiagnosticSet,
    correlation_hints: ContractList<CorrelationHint, 32>,
}

impl NormalizationCandidate {
    pub(crate) fn new(
        signal: RawSignal,
        provenance: SourceProvenanceDraft,
        evidence: PipelineCandidateEvidence,
        instrument_hint: Option<InstrumentHint>,
        candidate_ordinal: u32,
        diagnostics: DiagnosticSet,
        correlation_hints: ContractList<CorrelationHint, 32>,
    ) -> Self {
        Self {
            signal,
            provenance,
            evidence,
            instrument_hint,
            candidate_ordinal,
            diagnostics,
            correlation_hints,
        }
    }

    pub fn signal(&self) -> &RawSignal {
        &self.signal
    }

    pub fn provenance(&self) -> &SourceProvenanceDraft {
        &self.provenance
    }

    pub fn evidence(&self) -> &PipelineCandidateEvidence {
        &self.evidence
    }

    pub fn instrument_hint(&self) -> Option<&InstrumentHint> {
        self.instrument_hint.as_ref()
    }

    pub fn candidate_ordinal(&self) -> u32 {
        self.candidate_ordinal
    }

    pub fn diagnostics(&self) -> &DiagnosticSet {
        &self.diagnostics
    }

    pub fn correlation_hints(&self) -> &[CorrelationHint] {
        self.correlation_hints.as_slice()
    }
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SignalContractError {
    #[error(transparent)]
    Value(#[from] ContractValueError),
    #[error("entry action symbol and instrument hint symbol differ")]
    InstrumentSymbolMismatch,
    #[error("signal {ordinal} failed shared core validation: {source}")]
    CoreValidation {
        ordinal: usize,
        source: RawSignalValidationError,
    },
}

pub(crate) fn finalize_draft(draft: SignalDraft) -> FinalizedSignal {
    let ts = draft.effective_at.into_inner().naive_utc();
    let signal = match draft.action {
        SignalDraftAction::Entry {
            symbol,
            side,
            order_type,
            price,
            risk,
            stoploss,
            targets,
            group,
            trade_id,
        } => RawSignal::Entry {
            ts,
            symbol: symbol.into_inner(),
            side,
            order_type,
            price: price.map(FiniteF64::get),
            risk_multiplier: risk.get(),
            stoploss: stoploss.map(FiniteF64::get),
            targets: targets
                .into_inner()
                .into_iter()
                .map(FiniteF64::get)
                .collect(),
            group: group.map(GroupText::into_inner),
            trade_id: trade_id.map(TradeKeyText::into_inner),
        },
        SignalDraftAction::Close { position } => RawSignal::Close {
            ts,
            position: position.into_raw(),
        },
        SignalDraftAction::ClosePartial { position, ratio } => RawSignal::ClosePartial {
            ts,
            position: position.into_raw(),
            ratio: ratio.get(),
        },
        SignalDraftAction::ModifyStoploss { position, price } => RawSignal::ModifyStoploss {
            ts,
            position: position.into_raw(),
            price: price.get(),
        },
        SignalDraftAction::MoveStoplossToEntry { position } => RawSignal::MoveStoplossToEntry {
            ts,
            position: position.into_raw(),
        },
        SignalDraftAction::AddTarget {
            position,
            price,
            close_ratio,
        } => RawSignal::AddTarget {
            ts,
            position: position.into_raw(),
            price: price.get(),
            close_ratio: close_ratio.get(),
        },
        SignalDraftAction::RemoveTarget { position, price } => RawSignal::RemoveTarget {
            ts,
            position: position.into_raw(),
            price: price.get(),
        },
        SignalDraftAction::ModifyTarget {
            position,
            old_price,
            new_price,
        } => RawSignal::ModifyTarget {
            ts,
            position: position.into_raw(),
            old_price: old_price.get(),
            new_price: new_price.get(),
        },
        SignalDraftAction::AddRule { position, rule } => RawSignal::AddRule {
            ts,
            position: position.into_raw(),
            rule: rule.into_raw(),
        },
        SignalDraftAction::RemoveRule {
            position,
            rule_name,
        } => RawSignal::RemoveRule {
            ts,
            position: position.into_raw(),
            rule_name: rule_name.into_inner(),
        },
        SignalDraftAction::ScaleIn {
            position,
            price,
            size,
        } => RawSignal::ScaleIn {
            ts,
            position: position.into_raw(),
            price: price.map(FiniteF64::get),
            size: size.get(),
        },
        SignalDraftAction::CancelPending { position } => RawSignal::CancelPending {
            ts,
            position: position.into_raw(),
        },
        SignalDraftAction::CloseAllOf { symbol } => RawSignal::CloseAllOf {
            ts,
            symbol: symbol.into_inner(),
        },
        SignalDraftAction::CloseAll => RawSignal::CloseAll { ts },
        SignalDraftAction::CancelAllPending => RawSignal::CancelAllPending { ts },
        SignalDraftAction::ModifyAllStoploss { symbol, price } => RawSignal::ModifyAllStoploss {
            ts,
            symbol: symbol.into_inner(),
            price: price.get(),
        },
        SignalDraftAction::CloseAllInGroup { group_id } => RawSignal::CloseAllInGroup {
            ts,
            group_id: group_id.into_inner(),
        },
        SignalDraftAction::ModifyAllStoplossInGroup { group_id, price } => {
            RawSignal::ModifyAllStoplossInGroup {
                ts,
                group_id: group_id.into_inner(),
                price: price.get(),
            }
        }
    };
    FinalizedSignal {
        signal,
        instrument_hint: draft.instrument,
        diagnostics: draft.diagnostics,
        correlation_hints: draft.correlation_hints,
    }
}

pub(crate) fn validate_finalized_batch(
    signals: Vec<FinalizedSignal>,
) -> Result<Vec<FinalizedSignal>, SignalContractError> {
    for (ordinal, finalized) in signals.iter().enumerate() {
        validate_raw_signal(&finalized.signal)
            .map_err(|source| SignalContractError::CoreValidation { ordinal, source })?;
    }
    Ok(signals)
}

pub(crate) fn validate_pre_normalized_batch(
    signals: Vec<RawSignal>,
) -> Result<Vec<FinalizedSignal>, SignalContractError> {
    let finalized = signals
        .into_iter()
        .map(|signal| FinalizedSignal {
            signal,
            instrument_hint: None,
            diagnostics: DiagnosticSet::empty(),
            correlation_hints: ContractList::empty(),
        })
        .collect();
    validate_finalized_batch(finalized)
}

pub(crate) fn into_candidate_parts(
    finalized: FinalizedSignal,
) -> (
    RawSignal,
    Option<InstrumentHint>,
    DiagnosticSet,
    ContractList<CorrelationHint, 32>,
) {
    (
        finalized.signal,
        finalized.instrument_hint,
        finalized.diagnostics,
        finalized.correlation_hints,
    )
}
