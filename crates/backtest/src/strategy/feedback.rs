//! Borrowed execution facts supplied to a historical strategy boundary.

use qs_core::types::FutureEffect;

use crate::ledger::ActionDisposition;

/// Existing post-commit effects and newly terminal action dispositions.
///
/// The dynamic replay owner supplies only successfully committed effects and each newly terminal disposition in stable order.
#[derive(Debug, Clone, Copy, Default)]
pub struct StrategyFeedback<'a> {
    effects: &'a [FutureEffect],
    dispositions: &'a [ActionDisposition],
}

impl<'a> StrategyFeedback<'a> {
    pub const fn new(effects: &'a [FutureEffect], dispositions: &'a [ActionDisposition]) -> Self {
        Self {
            effects,
            dispositions,
        }
    }

    pub const fn effects(self) -> &'a [FutureEffect] {
        self.effects
    }

    pub const fn dispositions(self) -> &'a [ActionDisposition] {
        self.dispositions
    }

    pub const fn is_empty(self) -> bool {
        self.effects.is_empty() && self.dispositions.is_empty()
    }
}
