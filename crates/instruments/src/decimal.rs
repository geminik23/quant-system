use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::AssetId;

/// Maximum supported number of decimal fractional digits.
pub const MAX_DECIMAL_SCALE: u8 = 18;

/// An exact checked base-10 decimal backed by an `i128` coefficient.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Decimal {
    coefficient: i128,
    scale: u8,
}

impl Decimal {
    pub const ZERO: Self = Self {
        coefficient: 0,
        scale: 0,
    };

    pub fn new(coefficient: i128, scale: u8) -> Result<Self, DecimalError> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge(scale));
        }
        Ok(Self::normalize(coefficient, scale))
    }

    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }

    pub const fn scale(self) -> u8 {
        self.scale
    }

    pub const fn is_zero(self) -> bool {
        self.coefficient == 0
    }

    pub const fn is_positive(self) -> bool {
        self.coefficient > 0
    }

    pub const fn is_negative(self) -> bool {
        self.coefficient < 0
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, DecimalError> {
        let (left, right, scale) = align(self, rhs)?;
        let coefficient = left.checked_add(right).ok_or(DecimalError::Overflow)?;
        Self::new(coefficient, scale)
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self, DecimalError> {
        let (left, right, scale) = align(self, rhs)?;
        let coefficient = left.checked_sub(right).ok_or(DecimalError::Overflow)?;
        Self::new(coefficient, scale)
    }

    pub fn checked_mul(self, rhs: Self) -> Result<Self, DecimalError> {
        let coefficient = self
            .coefficient
            .checked_mul(rhs.coefficient)
            .ok_or(DecimalError::Overflow)?;
        let scale = self
            .scale
            .checked_add(rhs.scale)
            .ok_or(DecimalError::Overflow)?;
        let normalized = Self::normalize(coefficient, scale);
        if normalized.scale > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge(normalized.scale));
        }
        Ok(normalized)
    }

    pub fn checked_from_f64(value: f64) -> Result<Self, DecimalError> {
        if !value.is_finite() {
            return Err(DecimalError::NonFiniteFloat);
        }
        value.to_string().parse()
    }

    pub fn checked_rescale(self, scale: u8) -> Result<Self, DecimalError> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge(scale));
        }
        if scale == self.scale {
            return Ok(self);
        }
        if scale > self.scale {
            let factor = power_of_ten(scale - self.scale)?;
            let coefficient = self
                .coefficient
                .checked_mul(factor)
                .ok_or(DecimalError::Overflow)?;
            return Self::new(coefficient, scale);
        }

        let factor = power_of_ten(self.scale - scale)?;
        if self.coefficient % factor != 0 {
            return Err(DecimalError::InexactRescale {
                from: self.scale,
                to: scale,
            });
        }
        Self::new(self.coefficient / factor, scale)
    }

    pub(crate) fn aligned_coefficients(self, rhs: Self) -> Result<(i128, i128, u8), DecimalError> {
        align(self, rhs)
    }

    fn normalize(mut coefficient: i128, mut scale: u8) -> Self {
        if coefficient == 0 {
            return Self::ZERO;
        }
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Self { coefficient, scale }
    }
}

fn align(left: Decimal, right: Decimal) -> Result<(i128, i128, u8), DecimalError> {
    let scale = left.scale.max(right.scale);
    let left_factor = power_of_ten(scale - left.scale)?;
    let right_factor = power_of_ten(scale - right.scale)?;
    let left = left
        .coefficient
        .checked_mul(left_factor)
        .ok_or(DecimalError::Overflow)?;
    let right = right
        .coefficient
        .checked_mul(right_factor)
        .ok_or(DecimalError::Overflow)?;
    Ok((left, right, scale))
}

fn power_of_ten(power: u8) -> Result<i128, DecimalError> {
    10_i128
        .checked_pow(u32::from(power))
        .ok_or(DecimalError::Overflow)
}

fn compare_magnitude(left: Decimal, right: Decimal) -> Ordering {
    let left_digits = left.coefficient.unsigned_abs().to_string();
    let right_digits = right.coefficient.unsigned_abs().to_string();
    let left_exponent = left_digits.len() as i32 - i32::from(left.scale);
    let right_exponent = right_digits.len() as i32 - i32::from(right.scale);
    match left_exponent.cmp(&right_exponent) {
        Ordering::Equal => {
            let length = left_digits.len().max(right_digits.len());
            left_digits
                .bytes()
                .chain(std::iter::repeat(b'0'))
                .take(length)
                .cmp(
                    right_digits
                        .bytes()
                        .chain(std::iter::repeat(b'0'))
                        .take(length),
                )
        }
        ordering => ordering,
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.coefficient.signum(), other.coefficient.signum()) {
            (left, right) if left != right => left.cmp(&right),
            (-1, -1) => compare_magnitude(*other, *self),
            (0, 0) => Ordering::Equal,
            _ => compare_magnitude(*self, *other),
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.coefficient);
        }

        let negative = self.coefficient < 0;
        let digits = self.coefficient.unsigned_abs().to_string();
        let scale = usize::from(self.scale);
        if negative {
            f.write_str("-")?;
        }
        if digits.len() <= scale {
            f.write_str("0.")?;
            for _ in 0..(scale - digits.len()) {
                f.write_str("0")?;
            }
            f.write_str(&digits)
        } else {
            let split = digits.len() - scale;
            write!(f, "{}.{}", &digits[..split], &digits[split..])
        }
    }
}

impl FromStr for Decimal {
    type Err = DecimalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() || value.trim() != value {
            return Err(DecimalError::InvalidFormat);
        }
        if value.starts_with('+') || value.contains(['e', 'E']) {
            return Err(DecimalError::InvalidFormat);
        }

        let negative = value.starts_with('-');
        let unsigned = value.strip_prefix('-').unwrap_or(value);
        let mut components = unsigned.split('.');
        let integer = components.next().ok_or(DecimalError::InvalidFormat)?;
        let fractional = components.next();
        if components.next().is_some()
            || integer.is_empty()
            || !integer.bytes().all(|byte| byte.is_ascii_digit())
            || fractional.is_some_and(|part| {
                part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit())
            })
        {
            return Err(DecimalError::InvalidFormat);
        }

        let fractional = fractional.unwrap_or("");
        let scale = u8::try_from(fractional.len()).map_err(|_| DecimalError::InvalidFormat)?;
        if scale > MAX_DECIMAL_SCALE {
            return Err(DecimalError::ScaleTooLarge(scale));
        }
        let digits = format!("{integer}{fractional}");
        let magnitude = digits.parse::<u128>().map_err(|_| DecimalError::Overflow)?;
        let coefficient = if negative {
            if magnitude == 0 {
                return Err(DecimalError::NegativeZero);
            }
            if magnitude == i128::MAX as u128 + 1 {
                i128::MIN
            } else {
                let coefficient = i128::try_from(magnitude).map_err(|_| DecimalError::Overflow)?;
                coefficient.checked_neg().ok_or(DecimalError::Overflow)?
            }
        } else {
            i128::try_from(magnitude).map_err(|_| DecimalError::Overflow)?
        };
        Self::new(coefficient, scale)
    }
}

impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

macro_rules! decimal_wrapper {
    ($name:ident, $predicate:expr, $message:literal) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Decimal);

        impl $name {
            pub fn new(value: Decimal) -> Result<Self, DecimalError> {
                if !($predicate)(value) {
                    return Err(DecimalError::ConstraintViolation($message));
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> Decimal {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = DecimalError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value.parse()?)
            }
        }

        impl TryFrom<Decimal> for $name {
            type Error = DecimalError;

            fn try_from(value: Decimal) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for Decimal {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(Decimal::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

decimal_wrapper!(
    PositiveDecimal,
    |value: Decimal| value.is_positive(),
    "value must be positive"
);
decimal_wrapper!(
    NonNegativeDecimal,
    |value: Decimal| !value.is_negative(),
    "value must be nonnegative"
);
decimal_wrapper!(
    Price,
    |value: Decimal| value.is_positive(),
    "price must be positive"
);
decimal_wrapper!(
    Quantity,
    |value: Decimal| !value.is_negative(),
    "quantity must be nonnegative"
);

impl Quantity {
    pub fn require_positive(self) -> Result<PositiveDecimal, DecimalError> {
        PositiveDecimal::new(self.get())
    }
}

/// An exact amount denominated in one asset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Money {
    pub asset: AssetId,
    pub amount: Decimal,
}

/// Exact-decimal parsing and arithmetic failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DecimalError {
    #[error("invalid decimal string")]
    InvalidFormat,
    #[error("decimal scale {0} exceeds maximum scale {MAX_DECIMAL_SCALE}")]
    ScaleTooLarge(u8),
    #[error("decimal arithmetic overflow")]
    Overflow,
    #[error("negative zero is not canonical")]
    NegativeZero,
    #[error("non-finite floating-point values cannot be converted to Decimal")]
    NonFiniteFloat,
    #[error("cannot rescale exactly from scale {from} to scale {to}")]
    InexactRescale { from: u8, to: u8 },
    #[error("{0}")]
    ConstraintViolation(&'static str),
}
