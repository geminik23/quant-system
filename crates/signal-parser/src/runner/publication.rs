//! Bounded retry and dead-letter orchestration for committed-batch publication.

use std::sync::Arc;
use std::time::Instant;

use chrono::{Duration, Utc};
use serde::Deserialize;

use crate::ingestion::DateTimeUtc;
use crate::state::{PublicationNackDisposition, SourceStateError, SourceStateStore};

use super::{CommittedBatchSink, PublicationSinkError};

/// The duplicate-delivery behavior a committed-batch sink guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryAcknowledgementPolicy {
    DuplicateTolerant,
    IdempotentByDeliveryId,
}

impl DeliveryAcknowledgementPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DuplicateTolerant => "duplicate_tolerant",
            Self::IdempotentByDeliveryId => "idempotent_by_delivery_id",
        }
    }
}

/// Stable runner-owned identity for one logical sink delivery across attempts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationDeliveryId(String);

impl PublicationDeliveryId {
    pub(crate) fn from_state(value: &crate::state::PublicationDeliveryId) -> Self {
        Self(value.to_string_id())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Provider-neutral data supplied to one committed-batch sink attempt.
#[derive(Debug, Clone)]
pub struct CommittedDelivery<'a> {
    pub delivery_id: PublicationDeliveryId,
    pub sink_binding_id: &'a str,
    pub batch: &'a crate::state::CommittedNormalizationBatch,
    pub attempt: u32,
}

/// Receipt returned by a sink after it accepts a committed-batch delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationDeliveryReceipt {
    pub delivery_id: PublicationDeliveryId,
    pub batch_id: crate::state::CommittedBatchId,
}

/// Validated limits for one committed-batch publication runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationRetryPolicy {
    maximum_attempts: u32,
    lease_ttl: Duration,
    attempt_deadline: Duration,
    initial_backoff: Duration,
    maximum_backoff: Duration,
    dead_letter_after: Duration,
}

impl PublicationRetryPolicy {
    pub fn new(maximum_attempts: u32, lease_ttl: Duration) -> Result<Self, PublicationPolicyError> {
        Self::with_operational_limits(
            maximum_attempts,
            lease_ttl,
            Duration::seconds(30),
            Duration::seconds(1),
            Duration::minutes(1),
            Duration::hours(24),
        )
    }

    pub fn with_operational_limits(
        maximum_attempts: u32,
        lease_ttl: Duration,
        attempt_deadline: Duration,
        initial_backoff: Duration,
        maximum_backoff: Duration,
        dead_letter_after: Duration,
    ) -> Result<Self, PublicationPolicyError> {
        if maximum_attempts == 0 {
            return Err(PublicationPolicyError::ZeroMaximumAttempts);
        }
        if lease_ttl <= Duration::zero() {
            return Err(PublicationPolicyError::NonPositiveLeaseTtl);
        }
        if attempt_deadline <= Duration::zero() {
            return Err(PublicationPolicyError::NonPositiveAttemptDeadline);
        }
        if attempt_deadline > lease_ttl {
            return Err(PublicationPolicyError::AttemptDeadlineExceedsLeaseTtl);
        }
        if initial_backoff <= Duration::zero() || maximum_backoff <= Duration::zero() {
            return Err(PublicationPolicyError::NonPositiveBackoff);
        }
        if initial_backoff > maximum_backoff {
            return Err(PublicationPolicyError::BackoffExceedsMaximum);
        }
        if dead_letter_after <= Duration::zero() {
            return Err(PublicationPolicyError::NonPositiveDeadLetterAfter);
        }
        Ok(Self {
            maximum_attempts,
            lease_ttl,
            attempt_deadline,
            initial_backoff,
            maximum_backoff,
            dead_letter_after,
        })
    }

    pub fn maximum_attempts(self) -> u32 {
        self.maximum_attempts
    }
    pub fn lease_ttl(self) -> Duration {
        self.lease_ttl
    }
    pub fn attempt_deadline(self) -> Duration {
        self.attempt_deadline
    }
    pub fn dead_letter_after(self) -> Duration {
        self.dead_letter_after
    }

    /// Returns the deterministic, capped delay after a failed attempt number.
    pub fn backoff_for_attempt(self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(62);
        let factor = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
        let milliseconds = self
            .initial_backoff
            .num_milliseconds()
            .saturating_mul(factor)
            .min(self.maximum_backoff.num_milliseconds());
        Duration::milliseconds(milliseconds)
    }
}

/// Invalid publication retry policy configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublicationPolicyError {
    #[error("publication maximum attempts must be greater than zero")]
    ZeroMaximumAttempts,
    #[error("publication lease TTL must be positive")]
    NonPositiveLeaseTtl,
    #[error("publication attempt deadline must be positive")]
    NonPositiveAttemptDeadline,
    #[error("publication attempt deadline must not exceed the lease TTL")]
    AttemptDeadlineExceedsLeaseTtl,
    #[error("publication retry backoff values must be positive")]
    NonPositiveBackoff,
    #[error("publication initial retry backoff must not exceed its maximum")]
    BackoffExceedsMaximum,
    #[error("publication dead-letter cutoff must be positive")]
    NonPositiveDeadLetterAfter,
}

/// Whether a completed attempt has a known durable outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationFailureClass {
    Retryable,
    Permanent,
    UnknownOutcome,
}

/// Classifies a sink failure without coupling publication to a provider transport.
pub fn classify_sink_error(error: &PublicationSinkError) -> PublicationFailureClass {
    match error {
        PublicationSinkError::Io(_) => PublicationFailureClass::Retryable,
        PublicationSinkError::Serialization(_) => PublicationFailureClass::Permanent,
        PublicationSinkError::UnknownOutcome(_) => PublicationFailureClass::UnknownOutcome,
    }
}

/// Outcome counts for one bounded publication pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublicationRunReport {
    pub leased: usize,
    pub acknowledged: usize,
    pub retries_scheduled: usize,
    pub dead_lettered: usize,
    pub unknown_outcomes: usize,
}

/// Leases committed batches and applies bounded retry or dead-letter decisions.
pub struct PublicationOrchestrator {
    state: Arc<dyn SourceStateStore>,
    sink: Arc<dyn CommittedBatchSink>,
    policy: PublicationRetryPolicy,
}

impl PublicationOrchestrator {
    pub fn new(
        state: Arc<dyn SourceStateStore>,
        sink: Arc<dyn CommittedBatchSink>,
        policy: PublicationRetryPolicy,
    ) -> Self {
        Self {
            state,
            sink,
            policy,
        }
    }

    pub fn run_once(
        &self,
        maximum_records: usize,
    ) -> Result<PublicationRunReport, SourceStateError> {
        self.run_at(maximum_records, DateTimeUtc::new(Utc::now()))
    }

    /// Runs one pass at an explicit clock instant for deterministic scheduling.
    pub fn run_at(
        &self,
        maximum_records: usize,
        now: DateTimeUtc,
    ) -> Result<PublicationRunReport, SourceStateError> {
        let expires_at = DateTimeUtc::new(now.into_inner() + self.policy.lease_ttl());
        let leases = self
            .state
            .lease_publications(maximum_records, now, expires_at)?;
        let mut report = PublicationRunReport {
            leased: leases.len(),
            ..PublicationRunReport::default()
        };

        for lease in leases {
            if lease.record.attempts > self.policy.maximum_attempts()
                || deadline_reached(
                    lease.record.first_attempt_at.unwrap_or(now),
                    now,
                    self.policy.dead_letter_after(),
                )
            {
                self.dead_letter(lease.fence, &mut report)?;
                continue;
            }
            let Some(batch) = self.state.committed_batch(lease.record.batch_id.clone())? else {
                self.dead_letter(lease.fence, &mut report)?;
                continue;
            };

            let delivery_id = PublicationDeliveryId::from_state(&lease.record.delivery_id);
            let started = Instant::now();
            let result = self.sink.publish(CommittedDelivery {
                delivery_id: delivery_id.clone(),
                sink_binding_id: &lease.record.sink,
                batch: &batch,
                attempt: lease.record.attempts,
            });
            let elapsed = elapsed_chrono(started);
            let failure = if elapsed > self.policy.attempt_deadline() {
                Some(PublicationFailureClass::UnknownOutcome)
            } else {
                match result {
                    Ok(receipt)
                        if receipt.delivery_id == delivery_id
                            && receipt.batch_id == lease.record.batch_id =>
                    {
                        self.state
                            .acknowledge_publication(lease.fence.clone(), now)?;
                        report.acknowledged += 1;
                        None
                    }
                    Ok(_) => Some(PublicationFailureClass::Permanent),
                    Err(error) => Some(classify_sink_error(&error)),
                }
            };
            if let Some(failure) = failure {
                self.handle_failure(lease, now, failure, &mut report)?;
            }
        }
        Ok(report)
    }

    fn handle_failure(
        &self,
        lease: crate::state::PublicationLease,
        now: DateTimeUtc,
        failure: PublicationFailureClass,
        report: &mut PublicationRunReport,
    ) -> Result<(), SourceStateError> {
        if failure == PublicationFailureClass::UnknownOutcome {
            report.unknown_outcomes += 1;
        }
        let retry_is_safe = match failure {
            PublicationFailureClass::Permanent => false,
            PublicationFailureClass::Retryable => true,
            PublicationFailureClass::UnknownOutcome => matches!(
                self.sink.acknowledgement_policy(),
                DeliveryAcknowledgementPolicy::DuplicateTolerant
                    | DeliveryAcknowledgementPolicy::IdempotentByDeliveryId
            ),
        };
        let retry_at = DateTimeUtc::new(
            now.into_inner() + self.policy.backoff_for_attempt(lease.record.attempts),
        );
        if retry_is_safe
            && lease.record.attempts < self.policy.maximum_attempts()
            && !deadline_reached(
                lease.record.first_attempt_at.unwrap_or(now),
                retry_at,
                self.policy.dead_letter_after(),
            )
        {
            self.state.reject_publication(
                lease.fence,
                PublicationNackDisposition::Retry {
                    available_at: retry_at,
                },
            )?;
            report.retries_scheduled += 1;
        } else {
            self.dead_letter(lease.fence, report)?;
        }
        Ok(())
    }

    fn dead_letter(
        &self,
        fence: crate::state::PublicationLeaseFence,
        report: &mut PublicationRunReport,
    ) -> Result<(), SourceStateError> {
        self.state
            .reject_publication(fence, PublicationNackDisposition::DeadLetter)?;
        report.dead_lettered += 1;
        Ok(())
    }
}

fn elapsed_chrono(started: Instant) -> Duration {
    Duration::from_std(started.elapsed()).unwrap_or(Duration::MAX)
}

fn deadline_reached(started_at: DateTimeUtc, now: DateTimeUtc, limit: Duration) -> bool {
    now.into_inner() >= started_at.into_inner() + limit
}
