//! Source-neutral facts accepted before decoding, parsing, or normalization.

pub mod payload;
pub mod source;
pub mod validation;

pub use payload::{
    BoundedBytes, BoundedText, LanguageTag, MetadataKey, MetadataValue, PayloadEncoding,
    PayloadSchemaRef, SourceMetadata, SourcePayload, StructuredPayload, TextFormat, TextPayload,
};
pub use source::{
    DateTimeUtc, ExternalAuthorId, ExternalCorrelationId, ExternalEventId, ExternalThreadId,
    OpaqueSourceRevision, OpaqueSourceSequence, SOURCE_EVENT_SCHEMA_VERSION, SourceEvent,
    SourceEventKey, SourceEventRef, SourceId, SourceOperation, SourceRevision, SourceSequence,
    SourceTimestamp, SourceTimestampQuality,
};
pub use validation::{
    MAX_EXTERNAL_ID_BYTES, MAX_LANGUAGE_TAG_BYTES, MAX_METADATA_BYTES, MAX_METADATA_KEY_BYTES,
    MAX_METADATA_LABELS, MAX_METADATA_VALUE_BYTES, MAX_OPAQUE_ORDERING_BYTES, MAX_PAYLOAD_BYTES,
    MAX_SCHEMA_REF_BYTES, MAX_SOURCE_ID_BYTES, SourceValidationError,
};
