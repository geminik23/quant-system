use std::fmt;

use serde::{Deserialize, Deserializer};

pub const MAX_SOURCE_ID_BYTES: usize = 128;
pub const MAX_EXTERNAL_ID_BYTES: usize = 512;
pub const MAX_OPAQUE_ORDERING_BYTES: usize = 256;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_SCHEMA_REF_BYTES: usize = 128;
pub const MAX_LANGUAGE_TAG_BYTES: usize = 64;
pub const MAX_METADATA_LABELS: usize = 64;
pub const MAX_METADATA_KEY_BYTES: usize = 64;
pub const MAX_METADATA_VALUE_BYTES: usize = 1024;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;

/// Validation failure for a generic source-event value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceValidationError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds {maximum} bytes (got {actual})")]
    TooLong {
        field: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{field} contains prohibited characters")]
    InvalidCharacters { field: &'static str },
    #[error("invalid {field}: {reason}")]
    InvalidFormat { field: &'static str, reason: String },
    #[error("unsupported source-event schema version {actual}")]
    UnsupportedSchemaVersion { actual: u32 },
    #[error("invalid UTC timestamp '{value}': {reason}")]
    InvalidTimestamp { value: String, reason: String },
    #[error("invalid structured payload base64: {reason}")]
    InvalidBase64 { reason: String },
    #[error("source metadata has {actual} labels; maximum is {maximum}")]
    TooManyMetadataLabels { maximum: usize, actual: usize },
    #[error("serialized source metadata exceeds {maximum} bytes (got {actual})")]
    MetadataTooLarge { maximum: usize, actual: usize },
}

pub(crate) fn validate_non_empty_bounded(
    value: &str,
    field: &'static str,
    maximum: usize,
) -> Result<(), SourceValidationError> {
    if value.is_empty() {
        return Err(SourceValidationError::Empty { field });
    }
    validate_bounded(value.len(), field, maximum)
}

pub(crate) fn validate_bounded(
    actual: usize,
    field: &'static str,
    maximum: usize,
) -> Result<(), SourceValidationError> {
    if actual > maximum {
        return Err(SourceValidationError::TooLong {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

pub(crate) fn validate_no_control(
    value: &str,
    field: &'static str,
) -> Result<(), SourceValidationError> {
    if value.chars().any(char::is_control) {
        return Err(SourceValidationError::InvalidCharacters { field });
    }
    Ok(())
}

pub(crate) fn invalid_format(
    field: &'static str,
    reason: impl fmt::Display,
) -> SourceValidationError {
    SourceValidationError::InvalidFormat {
        field,
        reason: reason.to_string(),
    }
}

pub(crate) fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

pub(crate) fn is_namespace_segment(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase() || first.is_ascii_digit())
        && chars.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
}
