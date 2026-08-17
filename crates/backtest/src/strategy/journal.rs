//! Bounded non-economic research journal output.

use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer, Serialize};

use super::analysis::validate_symbol;
use super::domain::validate_trade_id;
use super::{MAX_INSTRUMENT_BYTES, MAX_TRADE_ID_BYTES};

pub const MAX_JOURNAL_RECORDS: usize = 1_000_000;
pub const MAX_JOURNAL_PER_CALLBACK: usize = 4096;
pub const MAX_JOURNAL_REASON_BYTES: usize = 4096;
pub const MAX_CHART_REF_BYTES: usize = 4096;
pub const MAX_JOURNAL_VALUES: usize = 256;
pub const MAX_JOURNAL_VALUE_KEY_BYTES: usize = 64;
pub const MAX_EXPERIMENT_LABEL_BYTES: usize = 256;

/// Generic category for one non-economic research record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalKind {
    DecisionContext,
    OutcomeReview,
    NoAction,
    Hypothetical,
    PeriodReview,
}

/// Caller-visible limits for journal retention and research text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StrategyResearchLimits {
    max_journal_records: usize,
    max_journal_per_callback: usize,
    max_reason_bytes: usize,
    max_chart_ref_bytes: usize,
    max_values_per_record: usize,
    max_value_key_bytes: usize,
    max_experiment_label_bytes: usize,
}

impl StrategyResearchLimits {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        max_journal_records: usize,
        max_journal_per_callback: usize,
        max_reason_bytes: usize,
        max_chart_ref_bytes: usize,
        max_values_per_record: usize,
        max_value_key_bytes: usize,
        max_experiment_label_bytes: usize,
    ) -> Result<Self, StrategyJournalError> {
        validate_limit(
            "max_journal_records",
            max_journal_records,
            MAX_JOURNAL_RECORDS,
            true,
        )?;
        validate_limit(
            "max_journal_per_callback",
            max_journal_per_callback,
            MAX_JOURNAL_PER_CALLBACK,
            false,
        )?;
        validate_limit(
            "max_reason_bytes",
            max_reason_bytes,
            MAX_JOURNAL_REASON_BYTES,
            false,
        )?;
        validate_limit(
            "max_chart_ref_bytes",
            max_chart_ref_bytes,
            MAX_CHART_REF_BYTES,
            false,
        )?;
        validate_limit(
            "max_values_per_record",
            max_values_per_record,
            MAX_JOURNAL_VALUES,
            true,
        )?;
        validate_limit(
            "max_value_key_bytes",
            max_value_key_bytes,
            MAX_JOURNAL_VALUE_KEY_BYTES,
            false,
        )?;
        validate_limit(
            "max_experiment_label_bytes",
            max_experiment_label_bytes,
            MAX_EXPERIMENT_LABEL_BYTES,
            false,
        )?;
        Ok(Self {
            max_journal_records,
            max_journal_per_callback,
            max_reason_bytes,
            max_chart_ref_bytes,
            max_values_per_record,
            max_value_key_bytes,
            max_experiment_label_bytes,
        })
    }

    pub fn max_journal_records(self) -> usize {
        self.max_journal_records
    }

    pub fn max_journal_per_callback(self) -> usize {
        self.max_journal_per_callback
    }

    pub fn max_reason_bytes(self) -> usize {
        self.max_reason_bytes
    }

    pub fn max_chart_ref_bytes(self) -> usize {
        self.max_chart_ref_bytes
    }

    pub fn max_values_per_record(self) -> usize {
        self.max_values_per_record
    }

    pub fn max_value_key_bytes(self) -> usize {
        self.max_value_key_bytes
    }

    pub fn max_experiment_label_bytes(self) -> usize {
        self.max_experiment_label_bytes
    }
}

impl Default for StrategyResearchLimits {
    fn default() -> Self {
        Self {
            max_journal_records: 10_000,
            max_journal_per_callback: 256,
            max_reason_bytes: 1024,
            max_chart_ref_bytes: 1024,
            max_values_per_record: 32,
            max_value_key_bytes: 64,
            max_experiment_label_bytes: 128,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyResearchLimitsDef {
    max_journal_records: usize,
    max_journal_per_callback: usize,
    max_reason_bytes: usize,
    max_chart_ref_bytes: usize,
    max_values_per_record: usize,
    max_value_key_bytes: usize,
    max_experiment_label_bytes: usize,
}

impl<'de> Deserialize<'de> for StrategyResearchLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyResearchLimitsDef::deserialize(deserializer)?;
        Self::new(
            value.max_journal_records,
            value.max_journal_per_callback,
            value.max_reason_bytes,
            value.max_chart_ref_bytes,
            value.max_values_per_record,
            value.max_value_key_bytes,
            value.max_experiment_label_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Validated journal output returned by one strategy callback.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StrategyJournalDraft {
    kind: JournalKind,
    symbol: String,
    related_trade_id: Option<String>,
    reason: String,
    chart_ref: Option<String>,
    values: BTreeMap<String, f64>,
}

impl StrategyJournalDraft {
    pub fn new(
        kind: JournalKind,
        symbol: impl Into<String>,
        related_trade_id: Option<String>,
        reason: impl Into<String>,
        chart_ref: Option<String>,
        values: BTreeMap<String, f64>,
        limits: StrategyResearchLimits,
    ) -> Result<Self, StrategyJournalError> {
        let draft = Self {
            kind,
            symbol: symbol.into(),
            related_trade_id,
            reason: reason.into(),
            chart_ref,
            values,
        };
        draft.validate(limits)?;
        Ok(draft)
    }

    pub fn kind(&self) -> JournalKind {
        self.kind
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn related_trade_id(&self) -> Option<&str> {
        self.related_trade_id.as_deref()
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn chart_ref(&self) -> Option<&str> {
        self.chart_ref.as_deref()
    }

    pub fn values(&self) -> &BTreeMap<String, f64> {
        &self.values
    }

    fn validate(&self, limits: StrategyResearchLimits) -> Result<(), StrategyJournalError> {
        validate_symbol(&self.symbol).map_err(|_| StrategyJournalError::InvalidSymbol {
            symbol: self.symbol.clone(),
            maximum: MAX_INSTRUMENT_BYTES,
        })?;
        if let Some(trade_id) = self.related_trade_id.as_deref() {
            validate_trade_id(trade_id).map_err(|_| StrategyJournalError::InvalidTradeId)?;
        }
        if !valid_text(&self.reason, limits.max_reason_bytes) {
            return Err(StrategyJournalError::InvalidReason {
                maximum: limits.max_reason_bytes,
            });
        }
        if self
            .chart_ref
            .as_deref()
            .is_some_and(|value| !valid_text(value, limits.max_chart_ref_bytes))
        {
            return Err(StrategyJournalError::InvalidChartRef {
                maximum: limits.max_chart_ref_bytes,
            });
        }
        if self.values.len() > limits.max_values_per_record {
            return Err(StrategyJournalError::TooManyValues {
                actual: self.values.len(),
                maximum: limits.max_values_per_record,
            });
        }
        for (key, value) in &self.values {
            if !valid_identifier(key, limits.max_value_key_bytes) {
                return Err(StrategyJournalError::InvalidValueKey {
                    key: key.clone(),
                    maximum: limits.max_value_key_bytes,
                });
            }
            if !value.is_finite() {
                return Err(StrategyJournalError::NonFiniteValue { key: key.clone() });
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyJournalDraftDef {
    kind: JournalKind,
    symbol: String,
    related_trade_id: Option<String>,
    reason: String,
    chart_ref: Option<String>,
    values: BTreeMap<String, f64>,
}

impl<'de> Deserialize<'de> for StrategyJournalDraft {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyJournalDraftDef::deserialize(deserializer)?;
        Self::new(
            value.kind,
            value.symbol,
            value.related_trade_id,
            value.reason,
            value.chart_ref,
            value.values,
            maximum_research_limits(),
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Runtime-stamped non-economic journal record.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StrategyJournalRecord {
    sequence: u64,
    observed_through: NaiveDateTime,
    kind: JournalKind,
    symbol: String,
    related_trade_id: Option<String>,
    reason: String,
    chart_ref: Option<String>,
    values: BTreeMap<String, f64>,
}

impl StrategyJournalRecord {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn kind(&self) -> JournalKind {
        self.kind
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn related_trade_id(&self) -> Option<&str> {
        self.related_trade_id.as_deref()
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn chart_ref(&self) -> Option<&str> {
        self.chart_ref.as_deref()
    }

    pub fn values(&self) -> &BTreeMap<String, f64> {
        &self.values
    }

    fn from_draft(
        sequence: u64,
        observed_through: NaiveDateTime,
        draft: StrategyJournalDraft,
    ) -> Self {
        Self {
            sequence,
            observed_through,
            kind: draft.kind,
            symbol: draft.symbol,
            related_trade_id: draft.related_trade_id,
            reason: draft.reason,
            chart_ref: draft.chart_ref,
            values: draft.values,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyJournalRecordDef {
    sequence: u64,
    observed_through: NaiveDateTime,
    kind: JournalKind,
    symbol: String,
    related_trade_id: Option<String>,
    reason: String,
    chart_ref: Option<String>,
    values: BTreeMap<String, f64>,
}

impl<'de> Deserialize<'de> for StrategyJournalRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyJournalRecordDef::deserialize(deserializer)?;
        let draft = StrategyJournalDraft::new(
            value.kind,
            value.symbol,
            value.related_trade_id,
            value.reason,
            value.chart_ref,
            value.values,
            maximum_research_limits(),
        )
        .map_err(serde::de::Error::custom)?;
        Ok(Self::from_draft(
            value.sequence,
            value.observed_through,
            draft,
        ))
    }
}

/// Exact retained and omitted journal counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyJournalRetention {
    pub retained: usize,
    pub omitted: usize,
}

/// Ordered retained journal records and exact omission metadata.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct StrategyJournalOutput {
    pub records: Vec<StrategyJournalRecord>,
    pub retention: StrategyJournalRetention,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyJournalOutputDef {
    records: Vec<StrategyJournalRecord>,
    retention: StrategyJournalRetention,
}

impl<'de> Deserialize<'de> for StrategyJournalOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyJournalOutputDef::deserialize(deserializer)?;
        if value.records.len() > MAX_JOURNAL_RECORDS
            || value.retention.retained != value.records.len()
        {
            return Err(serde::de::Error::custom(
                StrategyJournalError::InvalidRetention,
            ));
        }
        for (index, record) in value.records.iter().enumerate() {
            let expected = u64::try_from(index).map_err(serde::de::Error::custom)?;
            if record.sequence() != expected {
                return Err(serde::de::Error::custom(
                    StrategyJournalError::InvalidRecordSequence {
                        expected,
                        actual: record.sequence(),
                    },
                ));
            }
        }
        Ok(Self {
            records: value.records,
            retention: value.retention,
        })
    }
}

/// Bounded recorder for runtime-stamped journal drafts.
pub struct StrategyJournalRecorder {
    limits: StrategyResearchLimits,
    records: Vec<StrategyJournalRecord>,
    omitted: usize,
    next_sequence: u64,
}

impl StrategyJournalRecorder {
    pub fn new(limits: StrategyResearchLimits) -> Self {
        Self {
            limits,
            records: Vec::new(),
            omitted: 0,
            next_sequence: 0,
        }
    }

    pub fn push_callback(
        &mut self,
        observed_through: NaiveDateTime,
        drafts: Vec<StrategyJournalDraft>,
    ) -> Result<(), StrategyJournalError> {
        if drafts.len() > self.limits.max_journal_per_callback {
            return Err(StrategyJournalError::TooManyDrafts {
                actual: drafts.len(),
                maximum: self.limits.max_journal_per_callback,
            });
        }
        for draft in &drafts {
            draft.validate(self.limits)?;
        }

        let draft_count =
            u64::try_from(drafts.len()).map_err(|_| StrategyJournalError::SequenceOverflow)?;
        self.next_sequence
            .checked_add(draft_count)
            .ok_or(StrategyJournalError::SequenceOverflow)?;
        let available = self
            .limits
            .max_journal_records
            .saturating_sub(self.records.len());
        let omitted = drafts.len().saturating_sub(available);
        self.omitted
            .checked_add(omitted)
            .ok_or(StrategyJournalError::OmittedCounterOverflow)?;

        for draft in drafts {
            let sequence = self.next_sequence;
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .expect("journal sequence capacity was prevalidated");
            if self.records.len() < self.limits.max_journal_records {
                self.records.push(StrategyJournalRecord::from_draft(
                    sequence,
                    observed_through,
                    draft,
                ));
            } else {
                self.omitted += 1;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> StrategyJournalOutput {
        StrategyJournalOutput {
            retention: StrategyJournalRetention {
                retained: self.records.len(),
                omitted: self.omitted,
            },
            records: self.records,
        }
    }
}

/// Validation and retention failures for strategy research output.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyJournalError {
    #[error("{field} must be greater than zero")]
    ZeroLimit { field: &'static str },
    #[error("{field} {actual} exceeds maximum {maximum}")]
    LimitTooLarge {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("invalid journal symbol '{symbol}', expected at most {maximum} identifier bytes")]
    InvalidSymbol { symbol: String, maximum: usize },
    #[error("related trade ID must contain 1 to {MAX_TRADE_ID_BYTES} trimmed non-control bytes")]
    InvalidTradeId,
    #[error("journal reason must contain 1 to {maximum} trimmed non-control bytes")]
    InvalidReason { maximum: usize },
    #[error("chart reference must contain 1 to {maximum} trimmed non-control bytes")]
    InvalidChartRef { maximum: usize },
    #[error("journal values count {actual} exceeds maximum {maximum}")]
    TooManyValues { actual: usize, maximum: usize },
    #[error("journal value key '{key}' must contain 1 to {maximum} ASCII identifier bytes")]
    InvalidValueKey { key: String, maximum: usize },
    #[error("journal value '{key}' must be finite")]
    NonFiniteValue { key: String },
    #[error("callback returned {actual} journal drafts, exceeding maximum {maximum}")]
    TooManyDrafts { actual: usize, maximum: usize },
    #[error("journal sequence overflowed")]
    SequenceOverflow,
    #[error("journal omitted counter overflowed")]
    OmittedCounterOverflow,
    #[error("journal retention does not match retained records")]
    InvalidRetention,
    #[error("journal record sequence {actual} does not match expected sequence {expected}")]
    InvalidRecordSequence { expected: u64, actual: u64 },
    #[error("experiment label must contain 1 to {maximum} trimmed non-control bytes")]
    InvalidExperimentLabel { maximum: usize },
}

pub(crate) fn validate_experiment_label(
    value: &str,
    limits: StrategyResearchLimits,
) -> Result<(), StrategyJournalError> {
    if valid_text(value, limits.max_experiment_label_bytes) {
        Ok(())
    } else {
        Err(StrategyJournalError::InvalidExperimentLabel {
            maximum: limits.max_experiment_label_bytes,
        })
    }
}

fn validate_limit(
    field: &'static str,
    actual: usize,
    maximum: usize,
    allow_zero: bool,
) -> Result<(), StrategyJournalError> {
    if actual == 0 && !allow_zero {
        return Err(StrategyJournalError::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(StrategyJournalError::LimitTooLarge {
            field,
            actual,
            maximum,
        });
    }
    Ok(())
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub(crate) fn maximum_research_limits() -> StrategyResearchLimits {
    StrategyResearchLimits {
        max_journal_records: MAX_JOURNAL_RECORDS,
        max_journal_per_callback: MAX_JOURNAL_PER_CALLBACK,
        max_reason_bytes: MAX_JOURNAL_REASON_BYTES,
        max_chart_ref_bytes: MAX_CHART_REF_BYTES,
        max_values_per_record: MAX_JOURNAL_VALUES,
        max_value_key_bytes: MAX_JOURNAL_VALUE_KEY_BYTES,
        max_experiment_label_bytes: MAX_EXPERIMENT_LABEL_BYTES,
    }
}
