use chrono::{Duration, NaiveDateTime};
use qs_core::Side;
use serde::{Deserialize, Serialize};

pub const MAX_ID_BYTES: usize = 64;
pub const MAX_TEXT_BYTES: usize = 256;
pub const MAX_GENERATED_ID_BYTES: usize = 192;

/// A scalar type understood by configured expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarType {
    Bool,
    Integer,
    Number,
    Price,
    Timestamp,
    Duration,
    Text,
    Side,
}

/// A scalar expression type with explicit optionality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueType {
    pub scalar: ScalarType,
    #[serde(default)]
    pub optional: bool,
}

impl ValueType {
    pub const fn required(scalar: ScalarType) -> Self {
        Self {
            scalar,
            optional: false,
        }
    }

    pub const fn optional(scalar: ScalarType) -> Self {
        Self {
            scalar,
            optional: true,
        }
    }
}

/// A typed runtime value. Missing always retains its scalar type.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Missing(ScalarType),
    Bool(bool),
    Integer(i64),
    Number(f64),
    Price(f64),
    Timestamp(NaiveDateTime),
    Duration(Duration),
    Text(String),
    Side(Side),
}

impl Value {
    pub fn scalar_type(&self) -> ScalarType {
        match self {
            Self::Missing(value_type) => *value_type,
            Self::Bool(_) => ScalarType::Bool,
            Self::Integer(_) => ScalarType::Integer,
            Self::Number(_) => ScalarType::Number,
            Self::Price(_) => ScalarType::Price,
            Self::Timestamp(_) => ScalarType::Timestamp,
            Self::Duration(_) => ScalarType::Duration,
            Self::Text(_) => ScalarType::Text,
            Self::Side(_) => ScalarType::Side,
        }
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }

    pub(crate) fn finite(self, path: &str) -> Result<Self, crate::EvaluationError> {
        match self {
            Self::Number(value) if !value.is_finite() => {
                Err(crate::EvaluationError::NonFinite { path: path.into() })
            }
            Self::Price(value) if !value.is_finite() => {
                Err(crate::EvaluationError::NonFinite { path: path.into() })
            }
            value => Ok(value),
        }
    }
}

/// A strict configured literal, including an explicitly typed missing value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Literal {
    Missing(ScalarType),
    Bool(bool),
    Integer(i64),
    Number(f64),
    Price(f64),
    Timestamp(NaiveDateTime),
    DurationMillis(i64),
    Text(String),
    Side(Side),
}

impl Literal {
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Missing(value_type) => ValueType::optional(*value_type),
            Self::Bool(_) => ValueType::required(ScalarType::Bool),
            Self::Integer(_) => ValueType::required(ScalarType::Integer),
            Self::Number(_) => ValueType::required(ScalarType::Number),
            Self::Price(_) => ValueType::required(ScalarType::Price),
            Self::Timestamp(_) => ValueType::required(ScalarType::Timestamp),
            Self::DurationMillis(_) => ValueType::required(ScalarType::Duration),
            Self::Text(_) => ValueType::required(ScalarType::Text),
            Self::Side(_) => ValueType::required(ScalarType::Side),
        }
    }

    pub fn to_value(&self) -> Result<Value, crate::CompileError> {
        let value = match self {
            Self::Missing(value_type) => Value::Missing(*value_type),
            Self::Bool(value) => Value::Bool(*value),
            Self::Integer(value) => Value::Integer(*value),
            Self::Number(value) => Value::Number(*value),
            Self::Price(value) => Value::Price(*value),
            Self::Timestamp(value) => Value::Timestamp(*value),
            Self::DurationMillis(value) => Value::Duration(Duration::milliseconds(*value)),
            Self::Text(value) => {
                validate_text(value, MAX_TEXT_BYTES).map_err(|reason| {
                    crate::CompileError::InvalidConfig {
                        path: "literal.text".into(),
                        reason,
                    }
                })?;
                Value::Text(value.clone())
            }
            Self::Side(value) => Value::Side(*value),
        };
        match value {
            Value::Number(number) | Value::Price(number) if !number.is_finite() => {
                Err(crate::CompileError::InvalidConfig {
                    path: "literal".into(),
                    reason: "number must be finite".into(),
                })
            }
            _ => Ok(value),
        }
    }
}

pub(crate) fn validate_id(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("must not be empty".into());
    }
    if value.len() > MAX_ID_BYTES {
        return Err(format!("must not exceed {MAX_ID_BYTES} bytes"));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err("must contain only ASCII letters, digits, '_', '-', or '.'".into());
    }
    Ok(())
}

pub(crate) fn validate_text(value: &str, limit: usize) -> Result<(), String> {
    if value.is_empty() {
        return Err("must not be empty".into());
    }
    if value != value.trim() {
        return Err("must not have leading or trailing whitespace".into());
    }
    if value.chars().any(char::is_control) {
        return Err("must not contain control characters".into());
    }
    if value.len() > limit {
        return Err(format!("must not exceed {limit} bytes"));
    }
    Ok(())
}
