use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, NaiveDateTime};
use qs_core::RawSignal;
use qs_strategy::{CommandFeedback, CommandTerminalStatus};

use crate::types::bounded_reason;
use crate::{ApprovalError, CompletedIntent, CompletionKind, ExecutionIntent};

/// Per-instance entry approval policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalMode {
    Auto,
    RequireEntryApproval { timeout: Duration },
}

/// Result of admitting an intent to the approval gate.
#[derive(Debug, Clone)]
pub enum ApprovalAdmission {
    Ready(Box<ExecutionIntent>),
    Awaiting {
        command_id: String,
        expires_at: NaiveDateTime,
    },
    AlreadyAwaiting,
    AlreadyConsumed,
}

/// Result of an explicit approval or rejection decision.
#[derive(Debug, Clone)]
pub enum ApprovalDecision {
    ReadyForValidation(Box<ExecutionIntent>),
    Refused(ApprovalRefusal),
    AlreadyConsumed,
    Unknown,
}

/// Entry refusal that sends no provider request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRefusal {
    pub completion: CompletedIntent,
}

#[derive(Debug, Clone)]
struct PendingApproval {
    intent: ExecutionIntent,
    expires_at: NaiveDateTime,
}

/// Synchronous bounded approval state. It owns no clock, task, UI, or broker order.
pub struct ApprovalGate {
    mode: ApprovalMode,
    maximum_pending: usize,
    maximum_consumed: usize,
    pending: BTreeMap<String, PendingApproval>,
    consumed: BTreeSet<String>,
}

impl ApprovalGate {
    pub fn new(
        mode: Option<ApprovalMode>,
        maximum_pending: usize,
        maximum_consumed: usize,
    ) -> Result<Self, ApprovalError> {
        let mode = mode.ok_or(ApprovalError::MissingMode)?;
        if maximum_pending == 0 || maximum_consumed == 0 {
            return Err(ApprovalError::InvalidCapacity);
        }
        if matches!(mode, ApprovalMode::RequireEntryApproval { timeout } if timeout <= Duration::zero())
        {
            return Err(ApprovalError::InvalidTimeout);
        }
        Ok(Self {
            mode,
            maximum_pending,
            maximum_consumed,
            pending: BTreeMap::new(),
            consumed: BTreeSet::new(),
        })
    }

    pub const fn mode(&self) -> ApprovalMode {
        self.mode
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Release bounded duplicate protection after the application has reached and retained a terminal command outcome.
    /// The application remains responsible for durable command idempotency across cleanup or restart.
    pub fn complete(&mut self, command_id: &str) -> bool {
        self.consumed.remove(command_id)
    }

    pub fn admit(
        &mut self,
        intent: ExecutionIntent,
        now: NaiveDateTime,
    ) -> Result<ApprovalAdmission, ApprovalError> {
        if self.consumed.contains(&intent.command_id) {
            return Ok(ApprovalAdmission::AlreadyConsumed);
        }
        if self.pending.contains_key(&intent.command_id) {
            return Ok(ApprovalAdmission::AlreadyAwaiting);
        }
        let requires_approval = matches!(&intent.signal, RawSignal::Entry { .. })
            && matches!(self.mode, ApprovalMode::RequireEntryApproval { .. });
        if !requires_approval {
            self.mark_consumed(&intent.command_id)?;
            return Ok(ApprovalAdmission::Ready(Box::new(intent)));
        }
        if self.pending.len() >= self.maximum_pending {
            return Err(ApprovalError::CapacityExceeded {
                maximum: self.maximum_pending,
            });
        }
        let ApprovalMode::RequireEntryApproval { timeout } = self.mode else {
            unreachable!("requires_approval is true only for approval mode")
        };
        let expires_at = now
            .checked_add_signed(timeout)
            .ok_or(ApprovalError::TimestampOverflow)?;
        let command_id = intent.command_id.clone();
        self.pending
            .insert(command_id.clone(), PendingApproval { intent, expires_at });
        Ok(ApprovalAdmission::Awaiting {
            command_id,
            expires_at,
        })
    }

    pub fn approve(
        &mut self,
        command_id: &str,
        now: NaiveDateTime,
    ) -> Result<ApprovalDecision, ApprovalError> {
        if self.consumed.contains(command_id) {
            return Ok(ApprovalDecision::AlreadyConsumed);
        }
        if !self.pending.contains_key(command_id) {
            return Ok(ApprovalDecision::Unknown);
        }
        self.ensure_consumed_capacity(command_id)?;
        let pending = self
            .pending
            .remove(command_id)
            .expect("the pending approval was checked");
        self.mark_consumed(command_id)?;
        if now >= pending.expires_at {
            return Ok(ApprovalDecision::Refused(refusal(
                command_id,
                "entry approval expired",
            )));
        }
        Ok(ApprovalDecision::ReadyForValidation(Box::new(
            pending.intent,
        )))
    }

    pub fn reject(
        &mut self,
        command_id: &str,
        reason: impl Into<String>,
    ) -> Result<ApprovalDecision, ApprovalError> {
        if self.consumed.contains(command_id) {
            return Ok(ApprovalDecision::AlreadyConsumed);
        }
        if !self.pending.contains_key(command_id) {
            return Ok(ApprovalDecision::Unknown);
        }
        self.ensure_consumed_capacity(command_id)?;
        self.pending.remove(command_id);
        self.mark_consumed(command_id)?;
        Ok(ApprovalDecision::Refused(refusal(command_id, reason)))
    }

    pub fn expire(&mut self, now: NaiveDateTime) -> Result<Vec<ApprovalRefusal>, ApprovalError> {
        let expired = self
            .pending
            .iter()
            .filter_map(|(command_id, pending)| {
                (now >= pending.expires_at).then_some(command_id.clone())
            })
            .collect::<Vec<_>>();
        if self.consumed.len().saturating_add(expired.len()) > self.maximum_consumed {
            return Err(ApprovalError::ConsumedCapacityExceeded {
                maximum: self.maximum_consumed,
            });
        }
        let mut refusals = Vec::with_capacity(expired.len());
        for command_id in expired {
            self.pending.remove(&command_id);
            self.mark_consumed(&command_id)?;
            refusals.push(refusal(command_id, "entry approval expired"));
        }
        Ok(refusals)
    }

    fn ensure_consumed_capacity(&self, command_id: &str) -> Result<(), ApprovalError> {
        if !self.consumed.contains(command_id) && self.consumed.len() >= self.maximum_consumed {
            return Err(ApprovalError::ConsumedCapacityExceeded {
                maximum: self.maximum_consumed,
            });
        }
        Ok(())
    }

    fn mark_consumed(&mut self, command_id: &str) -> Result<(), ApprovalError> {
        self.ensure_consumed_capacity(command_id)?;
        self.consumed.insert(command_id.to_owned());
        Ok(())
    }
}

fn refusal(command_id: impl Into<String>, reason: impl Into<String>) -> ApprovalRefusal {
    let command_id = command_id.into();
    ApprovalRefusal {
        completion: CompletedIntent {
            command_id: command_id.clone(),
            kind: CompletionKind::Rejected,
            feedback: vec![CommandFeedback::Terminal {
                command_id,
                status: CommandTerminalStatus::Rejected,
                reason: Some(bounded_reason(reason)),
            }],
        },
    }
}
