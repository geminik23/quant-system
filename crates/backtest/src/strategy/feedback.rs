//! Borrowed execution facts supplied to a historical strategy boundary.

use qs_core::types::FutureEffect;

use crate::ledger::ActionDisposition;

/// One committed execution fact in replay commit order.
#[derive(Debug, Clone)]
pub enum StrategyFeedbackEvent {
    Effect {
        action_id: Option<String>,
        effect: FutureEffect,
    },
    Disposition(ActionDisposition),
}

impl StrategyFeedbackEvent {
    pub fn action_id(&self) -> Option<&str> {
        match self {
            Self::Effect { action_id, .. } => action_id.as_deref(),
            Self::Disposition(disposition) => Some(&disposition.action_id),
        }
    }

    pub const fn effect(&self) -> Option<&FutureEffect> {
        match self {
            Self::Effect { effect, .. } => Some(effect),
            Self::Disposition(_) => None,
        }
    }

    pub const fn disposition(&self) -> Option<&ActionDisposition> {
        match self {
            Self::Effect { .. } => None,
            Self::Disposition(disposition) => Some(disposition),
        }
    }
}

/// Existing post-commit effects and newly terminal action dispositions.
///
/// The dynamic replay owner supplies only successfully committed effects and each newly terminal disposition in stable order.
#[derive(Debug, Clone, Copy, Default)]
pub struct StrategyFeedback<'a> {
    effects: &'a [FutureEffect],
    dispositions: &'a [ActionDisposition],
    events: &'a [StrategyFeedbackEvent],
}

impl<'a> StrategyFeedback<'a> {
    pub const fn new(effects: &'a [FutureEffect], dispositions: &'a [ActionDisposition]) -> Self {
        Self {
            effects,
            dispositions,
            events: &[],
        }
    }

    pub(crate) const fn with_events(
        effects: &'a [FutureEffect],
        dispositions: &'a [ActionDisposition],
        events: &'a [StrategyFeedbackEvent],
    ) -> Self {
        Self {
            effects,
            dispositions,
            events,
        }
    }

    pub const fn effects(self) -> &'a [FutureEffect] {
        self.effects
    }

    pub const fn dispositions(self) -> &'a [ActionDisposition] {
        self.dispositions
    }

    pub const fn events(self) -> &'a [StrategyFeedbackEvent] {
        self.events
    }

    pub const fn is_empty(self) -> bool {
        self.effects.is_empty() && self.dispositions.is_empty()
    }
}
