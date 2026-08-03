use std::collections::BTreeMap;

use base64::Engine as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::validation::{
    MAX_LANGUAGE_TAG_BYTES, MAX_METADATA_BYTES, MAX_METADATA_KEY_BYTES, MAX_METADATA_LABELS,
    MAX_METADATA_VALUE_BYTES, MAX_PAYLOAD_BYTES, MAX_SCHEMA_REF_BYTES, SourceValidationError,
    deserialize_nullable, invalid_format, is_namespace_segment, validate_bounded,
    validate_no_control, validate_non_empty_bounded,
};

/// Bounded UTF-8 text retained exactly as received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedText(String);

impl BoundedText {
    pub fn new(value: impl Into<String>) -> Result<Self, SourceValidationError> {
        let value = value.into();
        validate_bounded(value.len(), "text payload", MAX_PAYLOAD_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Serialize for BoundedText {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BoundedText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Bounded arbitrary bytes used by structured source payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedBytes(Vec<u8>);

impl BoundedBytes {
    pub fn new(value: impl Into<Vec<u8>>) -> Result<Self, SourceValidationError> {
        let value = value.into();
        validate_bounded(value.len(), "structured payload", MAX_PAYLOAD_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

macro_rules! validated_string_type {
    ($name:ident, $field:literal, $validator:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, SourceValidationError> {
                let value = value.into();
                ($validator)(&value)?;
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

validated_string_type!(LanguageTag, "language tag", |value: &str| {
    validate_non_empty_bounded(value, "language tag", MAX_LANGUAGE_TAG_BYTES)?;
    let mut characters = value.chars();
    if !matches!(characters.next(), Some(first) if first.is_ascii_alphanumeric())
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '-')
        || value.ends_with('-')
    {
        return Err(SourceValidationError::InvalidCharacters {
            field: "language tag",
        });
    }
    Ok(())
});

validated_string_type!(PayloadSchemaRef, "payload schema", |value: &str| {
    validate_non_empty_bounded(value, "payload schema", MAX_SCHEMA_REF_BYTES)?;
    if !value.is_ascii() {
        return Err(SourceValidationError::InvalidCharacters {
            field: "payload schema",
        });
    }
    let (qualified_name, version) = value
        .rsplit_once('@')
        .ok_or_else(|| invalid_format("payload schema", "expected namespace/name@version"))?;
    let (namespace, name) = qualified_name
        .split_once('/')
        .ok_or_else(|| invalid_format("payload schema", "expected namespace/name@version"))?;
    if namespace.contains('/') || name.contains('/') {
        return Err(invalid_format(
            "payload schema",
            "expected exactly one namespace/name separator",
        ));
    }
    if !is_namespace_segment(namespace) || !is_namespace_segment(name) {
        return Err(SourceValidationError::InvalidCharacters {
            field: "payload schema",
        });
    }
    let parsed = version
        .parse::<u32>()
        .map_err(|error| invalid_format("payload schema version", error))?;
    if parsed == 0 || parsed.to_string() != version {
        return Err(invalid_format(
            "payload schema version",
            "expected a canonical positive u32",
        ));
    }
    Ok(())
});

validated_string_type!(MetadataKey, "metadata key", |value: &str| {
    validate_non_empty_bounded(value, "metadata key", MAX_METADATA_KEY_BYTES)?;
    let mut characters = value.chars();
    if !matches!(characters.next(), Some(first) if first.is_ascii_lowercase())
        || !characters.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-')
        })
    {
        return Err(SourceValidationError::InvalidCharacters {
            field: "metadata key",
        });
    }
    Ok(())
});

validated_string_type!(MetadataValue, "metadata value", |value: &str| {
    validate_bounded(value.len(), "metadata value", MAX_METADATA_VALUE_BYTES)?;
    validate_no_control(value, "metadata value")
});

/// Declared formatting of a text payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextFormat {
    Plain,
    Markdown,
    Html,
}

/// Encoding of the exact structured payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadEncoding {
    Json,
    Cbor,
    MessagePack,
    Binary,
}

/// Bounded source text and its declared presentation facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TextPayload {
    text: BoundedText,
    format: TextFormat,
    language: Option<LanguageTag>,
}

impl TextPayload {
    pub fn new(text: BoundedText, format: TextFormat, language: Option<LanguageTag>) -> Self {
        Self {
            text,
            format,
            language,
        }
    }

    pub fn text(&self) -> &BoundedText {
        &self.text
    }

    pub fn format(&self) -> TextFormat {
        self.format
    }

    pub fn language(&self) -> Option<&LanguageTag> {
        self.language.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextPayloadWire {
    text: BoundedText,
    format: TextFormat,
    #[serde(deserialize_with = "deserialize_nullable")]
    language: Option<LanguageTag>,
}

impl<'de> Deserialize<'de> for TextPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TextPayloadWire::deserialize(deserializer)?;
        Ok(Self::new(wire.text, wire.format, wire.language))
    }
}

/// Bounded exact bytes identified by a strict source schema and encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredPayload {
    schema: PayloadSchemaRef,
    encoding: PayloadEncoding,
    data: BoundedBytes,
}

impl StructuredPayload {
    pub fn new(schema: PayloadSchemaRef, encoding: PayloadEncoding, data: BoundedBytes) -> Self {
        Self {
            schema,
            encoding,
            data,
        }
    }

    pub fn schema(&self) -> &PayloadSchemaRef {
        &self.schema
    }

    pub fn encoding(&self) -> PayloadEncoding {
        self.encoding
    }

    pub fn data(&self) -> &BoundedBytes {
        &self.data
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct StructuredPayloadRef<'a> {
    schema: &'a PayloadSchemaRef,
    encoding: PayloadEncoding,
    data_base64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StructuredPayloadWire {
    schema: PayloadSchemaRef,
    encoding: PayloadEncoding,
    data_base64: String,
}

impl Serialize for StructuredPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        StructuredPayloadRef {
            schema: &self.schema,
            encoding: self.encoding,
            data_base64: base64::engine::general_purpose::STANDARD.encode(self.data.as_slice()),
        }
        .serialize(serializer)
    }
}

fn decode_structured_data(value: &str) -> Result<BoundedBytes, SourceValidationError> {
    let maximum_encoded = MAX_PAYLOAD_BYTES.div_ceil(3) * 4;
    validate_bounded(value.len(), "structured payload base64", maximum_encoded)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|error| SourceValidationError::InvalidBase64 {
            reason: error.to_string(),
        })?;
    if base64::engine::general_purpose::STANDARD.encode(&decoded) != value {
        return Err(SourceValidationError::InvalidBase64 {
            reason: "expected canonical padded RFC 4648 encoding".to_string(),
        });
    }
    BoundedBytes::new(decoded)
}

impl<'de> Deserialize<'de> for StructuredPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StructuredPayloadWire::deserialize(deserializer)?;
        let data = decode_structured_data(&wire.data_base64).map_err(serde::de::Error::custom)?;
        Ok(Self::new(wire.schema, wire.encoding, data))
    }
}

/// Source content category before any decoding or normalization stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourcePayload {
    Text(TextPayload),
    Structured(StructuredPayload),
    Empty,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum TextPayloadType {
    Text,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum StructuredPayloadType {
    Structured,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EmptyPayloadType {
    Empty,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextSourcePayloadWire {
    #[serde(rename = "type")]
    _kind: TextPayloadType,
    text: BoundedText,
    format: TextFormat,
    #[serde(deserialize_with = "deserialize_nullable")]
    language: Option<LanguageTag>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StructuredSourcePayloadWire {
    #[serde(rename = "type")]
    _kind: StructuredPayloadType,
    schema: PayloadSchemaRef,
    encoding: PayloadEncoding,
    data_base64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptySourcePayloadWire {
    #[serde(rename = "type")]
    _kind: EmptyPayloadType,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SourcePayloadWire {
    Text(TextSourcePayloadWire),
    Structured(StructuredSourcePayloadWire),
    Empty(EmptySourcePayloadWire),
}

impl<'de> Deserialize<'de> for SourcePayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match SourcePayloadWire::deserialize(deserializer)? {
            SourcePayloadWire::Text(wire) => Ok(Self::Text(TextPayload::new(
                wire.text,
                wire.format,
                wire.language,
            ))),
            SourcePayloadWire::Structured(wire) => {
                let data =
                    decode_structured_data(&wire.data_base64).map_err(serde::de::Error::custom)?;
                Ok(Self::Structured(StructuredPayload::new(
                    wire.schema,
                    wire.encoding,
                    data,
                )))
            }
            SourcePayloadWire::Empty(_) => Ok(Self::Empty),
        }
    }
}

/// Bounded source facts available for routing and audit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceMetadata {
    labels: BTreeMap<MetadataKey, MetadataValue>,
}

impl SourceMetadata {
    pub fn new(
        labels: BTreeMap<MetadataKey, MetadataValue>,
    ) -> Result<Self, SourceValidationError> {
        if labels.len() > MAX_METADATA_LABELS {
            return Err(SourceValidationError::TooManyMetadataLabels {
                maximum: MAX_METADATA_LABELS,
                actual: labels.len(),
            });
        }
        let metadata = Self { labels };
        let actual = serde_json::to_vec(&SourceMetadataRef {
            labels: &metadata.labels,
        })
        .map_err(|error| invalid_format("source metadata", error))?
        .len();
        if actual > MAX_METADATA_BYTES {
            return Err(SourceValidationError::MetadataTooLarge {
                maximum: MAX_METADATA_BYTES,
                actual,
            });
        }
        Ok(metadata)
    }

    pub fn labels(&self) -> &BTreeMap<MetadataKey, MetadataValue> {
        &self.labels
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SourceMetadataRef<'a> {
    labels: &'a BTreeMap<MetadataKey, MetadataValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceMetadataWire {
    labels: BTreeMap<MetadataKey, MetadataValue>,
}

impl Serialize for SourceMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SourceMetadataRef {
            labels: &self.labels,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SourceMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceMetadataWire::deserialize(deserializer)?;
        Self::new(wire.labels).map_err(serde::de::Error::custom)
    }
}
