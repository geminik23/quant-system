use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::payload::{SourceMetadata, SourcePayload};
use super::validation::{
    MAX_EXTERNAL_ID_BYTES, MAX_OPAQUE_ORDERING_BYTES, MAX_SOURCE_ID_BYTES, SourceValidationError,
    deserialize_nullable, is_namespace_segment, validate_no_control, validate_non_empty_bounded,
};

pub const SOURCE_EVENT_SCHEMA_VERSION: u32 = 1;

/// Validated identity of one configured source instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SourceId(String);

impl SourceId {
    pub fn new(value: impl Into<String>) -> Result<Self, SourceValidationError> {
        let value = value.into();
        validate_non_empty_bounded(&value, "source ID", MAX_SOURCE_ID_BYTES)?;
        if !value.is_ascii() || !value.split(':').all(is_namespace_segment) {
            return Err(SourceValidationError::InvalidCharacters { field: "source ID" });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<&str> for SourceId {
    type Error = SourceValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for SourceId {
    type Error = SourceValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

macro_rules! external_id_type {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, SourceValidationError> {
                let value = value.into();
                validate_non_empty_bounded(&value, $field, MAX_EXTERNAL_ID_BYTES)?;
                validate_no_control(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = SourceValidationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = SourceValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

external_id_type!(ExternalEventId, "external event ID");
external_id_type!(ExternalThreadId, "external thread ID");
external_id_type!(ExternalAuthorId, "external author ID");
external_id_type!(ExternalCorrelationId, "external correlation ID");

macro_rules! opaque_ordering_type {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, SourceValidationError> {
                let value = value.into();
                validate_non_empty_bounded(&value, $field, MAX_OPAQUE_ORDERING_BYTES)?;
                validate_no_control(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = SourceValidationError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = SourceValidationError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_ordering_type!(OpaqueSourceSequence, "opaque source sequence");
opaque_ordering_type!(OpaqueSourceRevision, "opaque source revision");

/// Composite event identity scoped by a configured source instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEventKey {
    source: SourceId,
    external_id: ExternalEventId,
}

impl SourceEventKey {
    pub fn new(source: SourceId, external_id: ExternalEventId) -> Self {
        Self {
            source,
            external_id,
        }
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn external_id(&self) -> &ExternalEventId {
        &self.external_id
    }
}

/// UTC timestamp with canonical RFC 3339 serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTimeUtc(DateTime<Utc>);

impl DateTimeUtc {
    pub fn new(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    pub fn parse(value: &str) -> Result<Self, SourceValidationError> {
        DateTime::parse_from_rfc3339(value)
            .map(|timestamp| Self(timestamp.with_timezone(&Utc)))
            .map_err(|error| SourceValidationError::InvalidTimestamp {
                value: value.to_string(),
                reason: error.to_string(),
            })
    }

    pub fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub fn into_inner(self) -> DateTime<Utc> {
        self.0
    }
}

impl Serialize for DateTimeUtc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut value = self.0.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let suffix = value.pop();
        debug_assert_eq!(suffix, Some('Z'));
        while value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        value.push('Z');
        serializer.serialize_str(&value)
    }
}

impl<'de> Deserialize<'de> for DateTimeUtc {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Evidence describing where the occurrence timestamp came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTimestampQuality {
    SourceProvided,
    AdapterDerived,
    ReceptionFallback,
}

/// Source occurrence time and its declared quality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceTimestamp {
    value: DateTimeUtc,
    quality: SourceTimestampQuality,
}

impl SourceTimestamp {
    pub fn new(value: DateTimeUtc, quality: SourceTimestampQuality) -> Self {
        Self { value, quality }
    }

    pub fn value(&self) -> DateTimeUtc {
        self.value
    }

    pub fn quality(&self) -> SourceTimestampQuality {
        self.quality
    }
}

/// Optional ordering evidence supplied by a source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SourceSequence {
    Monotonic(u64),
    Opaque(OpaqueSourceSequence),
}

/// Source lifecycle fact without downstream trading semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceOperation {
    Create,
    Update,
    Delete,
    Upsert,
    Snapshot,
}

/// Revision evidence declared by a source adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SourceRevision {
    Monotonic(u64),
    Opaque(OpaqueSourceRevision),
    Unversioned,
}

/// Validated source fact before decoding, parsing, or normalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceEvent {
    schema_version: u32,
    key: SourceEventKey,
    operation: SourceOperation,
    revision: SourceRevision,
    occurred_at: SourceTimestamp,
    received_at: DateTimeUtc,
    thread: Option<ExternalThreadId>,
    parent: Option<SourceEventKey>,
    author: Option<ExternalAuthorId>,
    correlation: Option<ExternalCorrelationId>,
    sequence: Option<SourceSequence>,
    payload: SourcePayload,
    metadata: SourceMetadata,
}

impl SourceEvent {
    pub fn new(
        key: SourceEventKey,
        operation: SourceOperation,
        revision: SourceRevision,
        occurred_at: SourceTimestamp,
        received_at: DateTimeUtc,
        payload: SourcePayload,
    ) -> Self {
        Self {
            schema_version: SOURCE_EVENT_SCHEMA_VERSION,
            key,
            operation,
            revision,
            occurred_at,
            received_at,
            thread: None,
            parent: None,
            author: None,
            correlation: None,
            sequence: None,
            payload,
            metadata: SourceMetadata::default(),
        }
    }

    pub fn with_thread(mut self, thread: ExternalThreadId) -> Self {
        self.thread = Some(thread);
        self
    }

    pub fn with_parent(mut self, parent: SourceEventKey) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn with_author(mut self, author: ExternalAuthorId) -> Self {
        self.author = Some(author);
        self
    }

    pub fn with_correlation(mut self, correlation: ExternalCorrelationId) -> Self {
        self.correlation = Some(correlation);
        self
    }

    pub fn with_sequence(mut self, sequence: SourceSequence) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn with_metadata(mut self, metadata: SourceMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn key(&self) -> &SourceEventKey {
        &self.key
    }

    pub fn operation(&self) -> SourceOperation {
        self.operation
    }

    pub fn revision(&self) -> &SourceRevision {
        &self.revision
    }

    pub fn occurred_at(&self) -> SourceTimestamp {
        self.occurred_at
    }

    pub fn received_at(&self) -> DateTimeUtc {
        self.received_at
    }

    pub fn thread(&self) -> Option<&ExternalThreadId> {
        self.thread.as_ref()
    }

    pub fn parent(&self) -> Option<&SourceEventKey> {
        self.parent.as_ref()
    }

    pub fn author(&self) -> Option<&ExternalAuthorId> {
        self.author.as_ref()
    }

    pub fn correlation(&self) -> Option<&ExternalCorrelationId> {
        self.correlation.as_ref()
    }

    pub fn sequence(&self) -> Option<&SourceSequence> {
        self.sequence.as_ref()
    }

    pub fn payload(&self) -> &SourcePayload {
        &self.payload
    }

    pub fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceEventWire {
    schema_version: u32,
    key: SourceEventKey,
    operation: SourceOperation,
    revision: SourceRevision,
    occurred_at: SourceTimestamp,
    received_at: DateTimeUtc,
    #[serde(deserialize_with = "deserialize_nullable")]
    thread: Option<ExternalThreadId>,
    #[serde(deserialize_with = "deserialize_nullable")]
    parent: Option<SourceEventKey>,
    #[serde(deserialize_with = "deserialize_nullable")]
    author: Option<ExternalAuthorId>,
    #[serde(deserialize_with = "deserialize_nullable")]
    correlation: Option<ExternalCorrelationId>,
    #[serde(deserialize_with = "deserialize_nullable")]
    sequence: Option<SourceSequence>,
    payload: SourcePayload,
    metadata: SourceMetadata,
}

impl<'de> Deserialize<'de> for SourceEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceEventWire::deserialize(deserializer)?;
        if wire.schema_version != SOURCE_EVENT_SCHEMA_VERSION {
            return Err(serde::de::Error::custom(
                SourceValidationError::UnsupportedSchemaVersion {
                    actual: wire.schema_version,
                },
            ));
        }
        Ok(Self {
            schema_version: SOURCE_EVENT_SCHEMA_VERSION,
            key: wire.key,
            operation: wire.operation,
            revision: wire.revision,
            occurred_at: wire.occurred_at,
            received_at: wire.received_at,
            thread: wire.thread,
            parent: wire.parent,
            author: wire.author,
            correlation: wire.correlation,
            sequence: wire.sequence,
            payload: wire.payload,
            metadata: wire.metadata,
        })
    }
}

/// Stable source identity carried into later normalization provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEventRef {
    key: SourceEventKey,
    revision: SourceRevision,
}

impl SourceEventRef {
    pub fn new(key: SourceEventKey, revision: SourceRevision) -> Self {
        Self { key, revision }
    }

    pub fn key(&self) -> &SourceEventKey {
        &self.key
    }

    pub fn revision(&self) -> &SourceRevision {
        &self.revision
    }
}

impl From<&SourceEvent> for SourceEventRef {
    fn from(event: &SourceEvent) -> Self {
        Self::new(event.key.clone(), event.revision.clone())
    }
}

impl std::str::FromStr for DateTimeUtc {
    type Err = SourceValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl std::str::FromStr for SourceId {
    type Err = SourceValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
