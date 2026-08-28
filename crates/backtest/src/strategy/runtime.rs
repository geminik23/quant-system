//! Stateful callback contract for causal historical strategies.

use chrono::NaiveDateTime;

use crate::data_feed::FeedEvent;
use crate::profile::RawSignal;

use super::domain::validate_decision_fields;
use super::{
    ClosedBar, StrategyContext, StrategyDecisionKind, StrategyDecisionRecord, StrategyDescriptor,
    StrategyDomainError, StrategyFeedback, StrategyJournalDraft, StrategyJournalError,
    StrategyObservation, StrategyRequirements, StrategyRetentionLimits,
};

/// One complete replay timestamp presented in already committed order.
#[derive(Debug, Clone, Copy)]
pub struct StrategyEvent<'a> {
    primary_events: &'a [FeedEvent],
    closed_bars: &'a [ClosedBar],
    observations: &'a [StrategyObservation],
    feedback: StrategyFeedback<'a>,
}

impl<'a> StrategyEvent<'a> {
    pub const fn new(
        primary_events: &'a [FeedEvent],
        closed_bars: &'a [ClosedBar],
        observations: &'a [StrategyObservation],
        feedback: StrategyFeedback<'a>,
    ) -> Self {
        Self {
            primary_events,
            closed_bars,
            observations,
            feedback,
        }
    }

    pub const fn primary_events(self) -> &'a [FeedEvent] {
        self.primary_events
    }

    pub const fn closed_bars(self) -> &'a [ClosedBar] {
        self.closed_bars
    }

    pub const fn observations(self) -> &'a [StrategyObservation] {
        self.observations
    }

    pub const fn feedback(self) -> StrategyFeedback<'a> {
        self.feedback
    }
}

/// A validated decision that may emit several ordered strict signals.
#[derive(Debug, Clone)]
pub struct StrategyDecisionDraft {
    kind: StrategyDecisionKind,
    reason: String,
    related_trade_id: Option<String>,
    signals: Vec<RawSignal>,
}

impl StrategyDecisionDraft {
    pub fn new(
        kind: StrategyDecisionKind,
        reason: impl Into<String>,
        related_trade_id: Option<String>,
        signals: Vec<RawSignal>,
        limits: StrategyRetentionLimits,
    ) -> Result<Self, StrategyDomainError> {
        let reason = reason.into();
        validate_decision_fields(&reason, related_trade_id.as_deref(), signals.len(), limits)?;
        Ok(Self {
            kind,
            reason,
            related_trade_id,
            signals,
        })
    }

    pub fn kind(&self) -> StrategyDecisionKind {
        self.kind
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn related_trade_id(&self) -> Option<&str> {
        self.related_trade_id.as_deref()
    }

    pub fn signals(&self) -> &[RawSignal] {
        &self.signals
    }

    pub fn into_record(
        self,
        sequence: u64,
        observed_through: NaiveDateTime,
        limits: StrategyRetentionLimits,
    ) -> Result<StrategyDecisionRecord, StrategyRuntimeError> {
        if let Some((signal_index, signal)) = self
            .signals
            .iter()
            .enumerate()
            .find(|(_, signal)| signal.ts() != observed_through)
        {
            return Err(StrategyRuntimeError::SignalTimestampMismatch {
                signal_index,
                signal_ts: signal.ts(),
                observed_through,
            });
        }
        Ok(StrategyDecisionRecord::new(
            sequence,
            observed_through,
            self.kind,
            self.reason,
            self.related_trade_id,
            self.signals,
            limits,
        )?)
    }
}

/// Optional economic decision and ordered non-economic journal drafts from one callback.
#[derive(Debug, Clone, Default)]
pub struct StrategyOutput {
    decision: Option<StrategyDecisionDraft>,
    journal: Vec<StrategyJournalDraft>,
}

impl StrategyOutput {
    pub const fn new(decision: Option<StrategyDecisionDraft>) -> Self {
        Self {
            decision,
            journal: Vec::new(),
        }
    }

    pub const fn none() -> Self {
        Self::new(None)
    }

    pub fn from_decision(decision: StrategyDecisionDraft) -> Self {
        Self::new(Some(decision))
    }

    pub fn from_journal(journal: Vec<StrategyJournalDraft>) -> Self {
        Self {
            decision: None,
            journal,
        }
    }

    pub fn with_journal(mut self, journal: Vec<StrategyJournalDraft>) -> Self {
        self.journal = journal;
        self
    }

    pub fn decision(&self) -> Option<&StrategyDecisionDraft> {
        self.decision.as_ref()
    }

    pub fn journal(&self) -> &[StrategyJournalDraft] {
        &self.journal
    }

    pub fn into_decision(self) -> Option<StrategyDecisionDraft> {
        self.decision
    }

    pub fn into_parts(self) -> (Option<StrategyDecisionDraft>, Vec<StrategyJournalDraft>) {
        (self.decision, self.journal)
    }
}

/// Errors produced while finalizing one callback decision.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyRuntimeError {
    #[error(transparent)]
    Domain(#[from] StrategyDomainError),
    #[error(transparent)]
    Journal(#[from] StrategyJournalError),
    #[error(
        "signal {signal_index} timestamp {signal_ts} does not match strategy boundary {observed_through}"
    )]
    SignalTimestampMismatch {
        signal_index: usize,
        signal_ts: NaiveDateTime,
        observed_through: NaiveDateTime,
    },
}

/// Stateful synchronous strategy evaluated once per complete replay timestamp.
pub trait HistoricalStrategy {
    type Error;

    fn descriptor(&self) -> &StrategyDescriptor;
    fn requirements(&self) -> &StrategyRequirements;

    fn on_event(
        &mut self,
        event: StrategyEvent<'_>,
        context: StrategyContext<'_>,
    ) -> Result<StrategyOutput, Self::Error>;
}
