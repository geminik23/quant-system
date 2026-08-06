use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ContractValueError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds {maximum} bytes or items (got {actual})")]
    LimitExceeded {
        field: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{field} contains prohibited control characters")]
    ControlCharacter { field: &'static str },
    #[error("{field} must be finite, got {value}")]
    NonFinite { field: &'static str, value: String },
    #[error("{field} must be finite and greater than zero, got {value}")]
    NonPositive { field: &'static str, value: f64 },
    #[error("{field} must be between zero and one inclusive, got {value}")]
    OutsideUnitInterval { field: &'static str, value: f64 },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContractText<const MAX_BYTES: usize>(String);

impl<const MAX_BYTES: usize> ContractText<MAX_BYTES> {
    pub fn try_new(
        value: impl Into<String>,
        field: &'static str,
    ) -> Result<Self, ContractValueError> {
        let value = value.into();
        if value.len() > MAX_BYTES {
            return Err(ContractValueError::LimitExceeded {
                field,
                maximum: MAX_BYTES,
                actual: value.len(),
            });
        }
        if value.chars().any(char::is_control) {
            return Err(ContractValueError::ControlCharacter { field });
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonEmptyContractText<const MAX_BYTES: usize>(ContractText<MAX_BYTES>);

impl<const MAX_BYTES: usize> NonEmptyContractText<MAX_BYTES> {
    pub fn try_new(
        value: impl Into<String>,
        field: &'static str,
    ) -> Result<Self, ContractValueError> {
        let value = ContractText::try_new(value, field)?;
        if value.as_str().is_empty() {
            return Err(ContractValueError::Empty { field });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> String {
        self.0.into_inner()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractBytes<const MAX_BYTES: usize>(Vec<u8>);

impl<const MAX_BYTES: usize> ContractBytes<MAX_BYTES> {
    pub fn try_new(
        value: impl Into<Vec<u8>>,
        field: &'static str,
    ) -> Result<Self, ContractValueError> {
        let value = value.into();
        if value.len() > MAX_BYTES {
            return Err(ContractValueError::LimitExceeded {
                field,
                maximum: MAX_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self(value))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractList<T, const MAX_ITEMS: usize>(Vec<T>);

impl<T, const MAX_ITEMS: usize> ContractList<T, MAX_ITEMS> {
    pub fn try_new(value: Vec<T>, field: &'static str) -> Result<Self, ContractValueError> {
        if value.len() > MAX_ITEMS {
            return Err(ContractValueError::LimitExceeded {
                field,
                maximum: MAX_ITEMS,
                actual: value.len(),
            });
        }
        Ok(Self(value))
    }

    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_inner(self) -> Vec<T> {
        self.0
    }

    pub fn try_push(&mut self, value: T, field: &'static str) -> Result<(), ContractValueError> {
        if self.0.len() == MAX_ITEMS {
            return Err(ContractValueError::LimitExceeded {
                field,
                maximum: MAX_ITEMS,
                actual: self.0.len() + 1,
            });
        }
        self.0.push(value);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonEmptyContractList<T, const MAX_ITEMS: usize>(ContractList<T, MAX_ITEMS>);

impl<T, const MAX_ITEMS: usize> NonEmptyContractList<T, MAX_ITEMS> {
    pub fn try_new(value: Vec<T>, field: &'static str) -> Result<Self, ContractValueError> {
        let value = ContractList::try_new(value, field)?;
        if value.is_empty() {
            return Err(ContractValueError::Empty { field });
        }
        Ok(Self(value))
    }

    pub fn as_slice(&self) -> &[T] {
        self.0.as_slice()
    }

    pub fn into_inner(self) -> Vec<T> {
        self.0.into_inner()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractMap<K, V, const MAX_ITEMS: usize>(BTreeMap<K, V>);

impl<K: Ord, V, const MAX_ITEMS: usize> ContractMap<K, V, MAX_ITEMS> {
    pub fn try_new(value: BTreeMap<K, V>, field: &'static str) -> Result<Self, ContractValueError> {
        if value.len() > MAX_ITEMS {
            return Err(ContractValueError::LimitExceeded {
                field,
                maximum: MAX_ITEMS,
                actual: value.len(),
            });
        }
        Ok(Self(value))
    }

    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    pub fn as_map(&self) -> &BTreeMap<K, V> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<K, V> {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FiniteF64(f64);

impl FiniteF64 {
    pub fn try_new(value: f64, field: &'static str) -> Result<Self, ContractValueError> {
        if !value.is_finite() {
            return Err(ContractValueError::NonFinite {
                field,
                value: value.to_string(),
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositiveFiniteF64(FiniteF64);

impl PositiveFiniteF64 {
    pub fn try_new(value: f64, field: &'static str) -> Result<Self, ContractValueError> {
        let value = FiniteF64::try_new(value, field)?;
        if value.get() <= 0.0 {
            return Err(ContractValueError::NonPositive {
                field,
                value: value.get(),
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnitInterval(FiniteF64);

impl UnitInterval {
    pub fn try_new(value: f64, field: &'static str) -> Result<Self, ContractValueError> {
        let value = FiniteF64::try_new(value, field)?;
        if !(0.0..=1.0).contains(&value.get()) {
            return Err(ContractValueError::OutsideUnitInterval {
                field,
                value: value.get(),
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> f64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteLimit(u64);

impl ByteLimit {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ItemLimit(u32);

impl ItemLimit {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

pub type ComponentId = NonEmptyContractText<128>;
pub type PipelineId = NonEmptyContractText<128>;
pub type DiagnosticCode = NonEmptyContractText<64>;
pub type DiagnosticText = ContractText<4096>;
pub type SymbolText = NonEmptyContractText<128>;
pub type RuleNameText = NonEmptyContractText<128>;
pub type GroupText = NonEmptyContractText<512>;
pub type TradeKeyText = NonEmptyContractText<512>;
