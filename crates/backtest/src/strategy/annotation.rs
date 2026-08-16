//! Validated causal and research-only historical annotations.

use std::collections::BTreeSet;
use std::fmt;

use chrono::NaiveDateTime;

use super::SeriesId;
use super::analysis::{
    AnalysisError, MAX_OBSERVATION_SOURCE_SERIES, StrategyObservationValue, validate_source_series,
    validate_symbol,
};

pub const MAX_ANNOTATION_ID_BYTES: usize = 64;
pub const MAX_ANNOTATIONS: usize = 1_000_000;
pub const MAX_ANNOTATION_NOTE_BYTES: usize = 4096;

/// Stable caller-supplied identity for one annotation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AnnotationId(String);

impl AnnotationId {
    pub fn new(value: impl Into<String>) -> Result<Self, AnnotationError> {
        let value = value.into();
        if valid_identifier(&value, MAX_ANNOTATION_ID_BYTES) {
            Ok(Self(value))
        } else {
            Err(AnnotationError::InvalidAnnotationId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AnnotationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Eligibility classification shared with later research output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationUse {
    CausalDecisionInput,
    HindsightLabel,
    JournalOnly,
}

/// Caller-visible annotation storage and text bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnotationLimits {
    max_annotations: usize,
    max_note_bytes: usize,
    max_source_series: usize,
}

impl AnnotationLimits {
    pub fn new(
        max_annotations: usize,
        max_note_bytes: usize,
        max_source_series: usize,
    ) -> Result<Self, AnnotationError> {
        validate_limit("max_annotations", max_annotations, MAX_ANNOTATIONS)?;
        validate_limit("max_note_bytes", max_note_bytes, MAX_ANNOTATION_NOTE_BYTES)?;
        validate_limit(
            "max_source_series",
            max_source_series,
            MAX_OBSERVATION_SOURCE_SERIES,
        )?;
        Ok(Self {
            max_annotations,
            max_note_bytes,
            max_source_series,
        })
    }

    pub fn max_annotations(self) -> usize {
        self.max_annotations
    }

    pub fn max_note_bytes(self) -> usize {
        self.max_note_bytes
    }

    pub fn max_source_series(self) -> usize {
        self.max_source_series
    }
}

impl Default for AnnotationLimits {
    fn default() -> Self {
        Self {
            max_annotations: 10_000,
            max_note_bytes: 1024,
            max_source_series: 16,
        }
    }
}

/// One validated manual causal input or research-only record.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyAnnotation {
    annotation_id: AnnotationId,
    input_sequence: u64,
    created_at: NaiveDateTime,
    observed_through: NaiveDateTime,
    valid_from: Option<NaiveDateTime>,
    use_kind: AnnotationUse,
    symbol: String,
    source_series: Vec<SeriesId>,
    value: StrategyObservationValue,
    note: Option<String>,
}

impl StrategyAnnotation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        annotation_id: AnnotationId,
        input_sequence: u64,
        created_at: NaiveDateTime,
        observed_through: NaiveDateTime,
        valid_from: Option<NaiveDateTime>,
        use_kind: AnnotationUse,
        symbol: impl Into<String>,
        source_series: Vec<SeriesId>,
        value: StrategyObservationValue,
        note: Option<String>,
        limits: AnnotationLimits,
    ) -> Result<Self, AnnotationError> {
        let symbol = symbol.into();
        validate_symbol(&symbol).map_err(|error| AnnotationError::InvalidValue(Box::new(error)))?;
        validate_source_series(&source_series, limits.max_source_series)
            .map_err(|error| AnnotationError::InvalidValue(Box::new(error)))?;
        if created_at < observed_through {
            return Err(AnnotationError::CreatedBeforeObservation {
                created_at,
                observed_through,
            });
        }
        match use_kind {
            AnnotationUse::CausalDecisionInput => {
                let valid_from = valid_from.ok_or(AnnotationError::MissingValidFrom)?;
                if valid_from < observed_through {
                    return Err(AnnotationError::CausalBackdating {
                        observed_through,
                        valid_from,
                    });
                }
            }
            AnnotationUse::HindsightLabel | AnnotationUse::JournalOnly => {
                if valid_from.is_some() {
                    return Err(AnnotationError::UnexpectedValidFrom);
                }
            }
        }
        if note.as_deref().is_some_and(|value| {
            value.len() > limits.max_note_bytes
                || value.trim() != value
                || value.chars().any(char::is_control)
        }) {
            return Err(AnnotationError::InvalidNote {
                maximum: limits.max_note_bytes,
            });
        }
        let value_boundary = match &value {
            StrategyObservationValue::Zone(_) => valid_from.unwrap_or(created_at),
            _ => observed_through,
        };
        value
            .validate_at(value_boundary)
            .map_err(|error| AnnotationError::InvalidValue(Box::new(error)))?;
        Ok(Self {
            annotation_id,
            input_sequence,
            created_at,
            observed_through,
            valid_from,
            use_kind,
            symbol,
            source_series,
            value,
            note,
        })
    }

    pub fn annotation_id(&self) -> &AnnotationId {
        &self.annotation_id
    }

    pub fn input_sequence(&self) -> u64 {
        self.input_sequence
    }

    pub fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn valid_from(&self) -> Option<NaiveDateTime> {
        self.valid_from
    }

    pub fn use_kind(&self) -> AnnotationUse {
        self.use_kind
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn source_series(&self) -> &[SeriesId] {
        &self.source_series
    }

    pub fn value(&self) -> &StrategyObservationValue {
        &self.value
    }

    pub fn note(&self) -> Option<&str> {
        self.note.as_deref()
    }
}

/// Deterministic annotation schedule and research-only retention.
#[derive(Debug, Clone)]
pub struct AnnotationTimeline {
    pending_causal: Vec<StrategyAnnotation>,
    research_only: Vec<StrategyAnnotation>,
    ids: BTreeSet<AnnotationId>,
    input_sequences: BTreeSet<u64>,
    limits: AnnotationLimits,
}

impl AnnotationTimeline {
    pub fn new(limits: AnnotationLimits) -> Self {
        Self {
            pending_causal: Vec::new(),
            research_only: Vec::new(),
            ids: BTreeSet::new(),
            input_sequences: BTreeSet::new(),
            limits,
        }
    }

    pub fn add(
        &mut self,
        annotation: StrategyAnnotation,
        advanced_through: Option<NaiveDateTime>,
    ) -> Result<(), AnnotationError> {
        if annotation
            .note()
            .is_some_and(|note| note.len() > self.limits.max_note_bytes)
        {
            return Err(AnnotationError::InvalidNote {
                maximum: self.limits.max_note_bytes,
            });
        }
        if annotation.source_series().len() > self.limits.max_source_series {
            return Err(AnnotationError::InvalidValue(Box::new(
                AnalysisError::TooManySourceSeries {
                    actual: annotation.source_series().len(),
                    maximum: self.limits.max_source_series,
                },
            )));
        }
        let count = self
            .ids
            .len()
            .checked_add(1)
            .ok_or(AnnotationError::AnnotationCountOverflow)?;
        if count > self.limits.max_annotations {
            return Err(AnnotationError::TooManyAnnotations {
                actual: count,
                maximum: self.limits.max_annotations,
            });
        }
        if self.ids.contains(annotation.annotation_id()) {
            return Err(AnnotationError::DuplicateAnnotationId {
                annotation_id: annotation.annotation_id().clone(),
            });
        }
        if self.input_sequences.contains(&annotation.input_sequence()) {
            return Err(AnnotationError::DuplicateInputSequence {
                input_sequence: annotation.input_sequence(),
            });
        }
        if annotation.use_kind() == AnnotationUse::CausalDecisionInput
            && advanced_through.is_some_and(|advanced| {
                annotation
                    .valid_from()
                    .is_some_and(|valid_from| valid_from <= advanced)
            })
        {
            return Err(AnnotationError::RetroactiveCausalInsertion {
                valid_from: annotation
                    .valid_from()
                    .expect("causal annotations always have valid_from"),
                advanced_through: advanced_through.expect("checked as present"),
            });
        }

        self.ids.insert(annotation.annotation_id().clone());
        self.input_sequences.insert(annotation.input_sequence());
        match annotation.use_kind() {
            AnnotationUse::CausalDecisionInput => {
                self.pending_causal.push(annotation);
                self.pending_causal.sort_by(|left, right| {
                    left.valid_from()
                        .cmp(&right.valid_from())
                        .then_with(|| left.input_sequence().cmp(&right.input_sequence()))
                        .then_with(|| left.annotation_id().cmp(right.annotation_id()))
                });
            }
            AnnotationUse::HindsightLabel | AnnotationUse::JournalOnly => {
                self.research_only.push(annotation);
            }
        }
        Ok(())
    }

    pub fn pending_causal(&self) -> &[StrategyAnnotation] {
        &self.pending_causal
    }

    pub fn research_only(&self) -> &[StrategyAnnotation] {
        &self.research_only
    }

    pub fn total_count(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn activate(
        &mut self,
        observed_through: NaiveDateTime,
    ) -> Result<Vec<StrategyAnnotation>, AnnotationError> {
        let eligible = self.pending_causal.partition_point(|annotation| {
            annotation
                .valid_from()
                .is_some_and(|valid_from| valid_from <= observed_through)
        });
        Ok(self.pending_causal.drain(..eligible).collect())
    }
}

/// Typed validation, conflict, bound, and causality errors for annotations.
#[derive(Debug, thiserror::Error)]
pub enum AnnotationError {
    #[error("annotation ID must contain 1 to {MAX_ANNOTATION_ID_BYTES} ASCII identifier bytes")]
    InvalidAnnotationId,
    #[error("{field} must be greater than zero")]
    ZeroLimit { field: &'static str },
    #[error("{field} {actual} exceeds maximum {maximum}")]
    LimitTooLarge {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("annotation creation time {created_at} precedes observed data time {observed_through}")]
    CreatedBeforeObservation {
        created_at: NaiveDateTime,
        observed_through: NaiveDateTime,
    },
    #[error("causal annotation requires valid_from")]
    MissingValidFrom,
    #[error("research-only annotation must not declare valid_from")]
    UnexpectedValidFrom,
    #[error("causal valid_from {valid_from} precedes observed data time {observed_through}")]
    CausalBackdating {
        observed_through: NaiveDateTime,
        valid_from: NaiveDateTime,
    },
    #[error("annotation note must be trimmed non-control text within {maximum} bytes")]
    InvalidNote { maximum: usize },
    #[error("annotation value is invalid: {0}")]
    InvalidValue(Box<AnalysisError>),
    #[error("annotation ID '{annotation_id}' is already present")]
    DuplicateAnnotationId { annotation_id: AnnotationId },
    #[error("annotation input sequence {input_sequence} is already present")]
    DuplicateInputSequence { input_sequence: u64 },
    #[error("annotation count overflowed")]
    AnnotationCountOverflow,
    #[error("annotation count {actual} exceeds maximum {maximum}")]
    TooManyAnnotations { actual: usize, maximum: usize },
    #[error(
        "causal annotation valid from {valid_from} cannot be inserted after replay advanced through {advanced_through}"
    )]
    RetroactiveCausalInsertion {
        valid_from: NaiveDateTime,
        advanced_through: NaiveDateTime,
    },
}

fn validate_limit(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), AnnotationError> {
    if actual == 0 {
        return Err(AnnotationError::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(AnnotationError::LimitTooLarge {
            field,
            actual,
            maximum,
        });
    }
    Ok(())
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}
