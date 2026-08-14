use serde::{Deserialize, Serialize};

use crate::{Decimal, DecimalError, PositiveDecimal};

/// An exact decimal grid with an origin and positive step.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecimalGrid {
    pub origin: Decimal,
    pub step: PositiveDecimal,
}

impl DecimalGrid {
    pub fn new(origin: Decimal, step: PositiveDecimal) -> Self {
        Self { origin, step }
    }

    pub fn contains(&self, value: Decimal) -> Result<bool, GridError> {
        let delta = value.checked_sub(self.origin)?;
        let (delta, step, _) = delta.aligned_coefficients(self.step.get())?;
        Ok(delta % step == 0)
    }

    pub fn adjust(
        &self,
        value: Decimal,
        rounding: GridRounding,
    ) -> Result<GridAdjustment<Decimal>, GridError> {
        let delta = value.checked_sub(self.origin)?;
        let (delta, step, scale) = delta.aligned_coefficients(self.step.get())?;
        let quotient = delta.div_euclid(step);
        let remainder = delta.rem_euclid(step);
        if remainder == 0 {
            return Ok(GridAdjustment {
                requested: value,
                adjusted: value,
                direction: AdjustmentDirection::Unchanged,
            });
        }
        if rounding == GridRounding::Reject {
            return Err(GridError::OffGrid { value });
        }

        let adjusted_quotient = match rounding {
            GridRounding::Floor => quotient,
            GridRounding::Ceil => quotient.checked_add(1).ok_or(DecimalError::Overflow)?,
            GridRounding::Reject => unreachable!(),
        };
        let offset = adjusted_quotient
            .checked_mul(step)
            .ok_or(DecimalError::Overflow)?;
        let adjusted = self.origin.checked_add(Decimal::new(offset, scale)?)?;
        let direction = match adjusted.cmp(&value) {
            std::cmp::Ordering::Less => AdjustmentDirection::Down,
            std::cmp::Ordering::Equal => AdjustmentDirection::Unchanged,
            std::cmp::Ordering::Greater => AdjustmentDirection::Up,
        };

        Ok(GridAdjustment {
            requested: value,
            adjusted,
            direction,
        })
    }
}

/// Explicit handling for a value outside a declared grid.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridRounding {
    Reject,
    Floor,
    Ceil,
}

/// Direction taken by a grid adjustment.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentDirection {
    Unchanged,
    Down,
    Up,
}

/// Auditable requested and adjusted values.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridAdjustment<T> {
    pub requested: T,
    pub adjusted: T,
    pub direction: AdjustmentDirection,
}

/// Decimal-grid validation and adjustment failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GridError {
    #[error("value {value} is not on the declared decimal grid")]
    OffGrid { value: Decimal },
    #[error(transparent)]
    Decimal(#[from] DecimalError),
}
