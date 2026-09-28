use std::cmp::Ordering;

pub(crate) const MAX_STRICT_PERIOD: usize = 1024;

/// Fixed storage keeps staged clones and rollover within the declared state bound.
#[derive(Clone)]
pub(crate) struct ObservedWindow {
    slots: Box<[Option<f64>]>,
    next: usize,
    len: usize,
}

impl ObservedWindow {
    pub(crate) fn new(period: usize) -> Result<Self, String> {
        Self::state_bytes(period)?;
        Ok(Self {
            slots: vec![None; period].into_boxed_slice(),
            next: 0,
            len: 0,
        })
    }

    pub(crate) fn state_bytes(period: usize) -> Result<usize, String> {
        if !(1..=MAX_STRICT_PERIOD).contains(&period) {
            return Err(format!("strict period must be in 1..={MAX_STRICT_PERIOD}"));
        }
        period
            .checked_mul(std::mem::size_of::<Option<f64>>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| "strict window state bound overflowed".into())
    }

    pub(crate) fn push(&mut self, sample: Option<f64>) -> Result<(), String> {
        if sample.is_some_and(|value| !value.is_finite()) {
            return Err("strict sample must be finite".into());
        }
        self.slots[self.next] = sample;
        self.next = (self.next + 1) % self.slots.len();
        self.len = (self.len + 1).min(self.slots.len());
        Ok(())
    }

    pub(crate) fn reset(&mut self) {
        self.len = 0;
        self.next = 0;
    }

    pub(crate) fn complete(&self) -> bool {
        self.len == self.slots.len()
    }

    pub(crate) fn chronological(&self) -> impl Iterator<Item = Option<f64>> + '_ {
        (0..self.len).rev().map(|age| {
            let index = (self.next + self.slots.len() - 1 - age) % self.slots.len();
            self.slots[index]
        })
    }

    pub(crate) fn mean(&self) -> Result<Option<f64>, String> {
        if self.len != self.slots.len() || self.slots.iter().any(Option::is_none) {
            return Ok(None);
        }
        stable_mean(self.slots.iter().flatten().copied()).map(Some)
    }
}

// Finite binary64 values are integers in units of 2^-1074.
// 34 limbs cover the exact sum of 1024 maximum finite samples without heap allocation.
const LIMBS: usize = 34;

pub(crate) fn stable_mean(values: impl IntoIterator<Item = f64>) -> Result<f64, String> {
    exact_weighted_mean(
        values.into_iter().map(|value| (value, 1)),
        MAX_STRICT_PERIOD as u64,
    )
}

pub(crate) fn weighted_mean(
    values: impl IntoIterator<Item = (f64, u64)>,
    maximum_weight: u64,
) -> Result<f64, String> {
    exact_weighted_mean(values, maximum_weight)
}

pub(crate) fn ema_step(sample: f64, previous: f64, period: usize) -> Result<f64, String> {
    if !(1..=MAX_STRICT_PERIOD).contains(&period) {
        return Err("strict EMA period is out of bounds".into());
    }
    // Rational weights avoid underflow in separately rounded products on subnormal flat input.
    exact_weighted_mean(
        [(sample, 2), (previous, period as u64 - 1)],
        MAX_STRICT_PERIOD as u64 + 1,
    )
}

pub(crate) fn rma_step(sample: f64, previous: f64, period: usize) -> Result<f64, String> {
    if !(1..=MAX_STRICT_PERIOD).contains(&period) {
        return Err("strict RMA period is out of bounds".into());
    }
    exact_weighted_mean(
        [(sample, 1), (previous, period as u64 - 1)],
        MAX_STRICT_PERIOD as u64,
    )
}

fn exact_weighted_mean(
    values: impl IntoIterator<Item = (f64, u64)>,
    limit: u64,
) -> Result<f64, String> {
    let mut positive = [0u64; LIMBS];
    let mut negative = [0u64; LIMBS];
    let mut count = 0u64;
    for (value, weight) in values {
        if !value.is_finite() || weight > limit - count {
            return Err("strict mean exceeds its finite sample weight bound".into());
        }
        count += weight;
        let bits = value.to_bits();
        let exponent = ((bits >> 52) & 0x7ff) as usize;
        let significand = (bits & ((1u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1u64 << 52 };
        let shift = exponent.saturating_sub(1);
        let target = if value.is_sign_negative() {
            &mut negative
        } else {
            &mut positive
        };
        let mut carry = (significand as u128 * weight as u128) << (shift % 64);
        for limb in &mut target[shift / 64..] {
            let sum = *limb as u128 + (carry & u64::MAX as u128);
            *limb = sum as u64;
            carry = (carry >> 64) + (sum >> 64);
            if carry == 0 {
                break;
            }
        }
        debug_assert_eq!(carry, 0);
    }
    if count == 0 {
        return Err("strict mean requires a sample".into());
    }
    let ordering = positive.iter().rev().cmp(negative.iter().rev());
    if ordering == Ordering::Equal {
        return Ok(0.0);
    }
    let (mut magnitude, subtrahend, sign) = if ordering == Ordering::Less {
        (negative, positive, 1u64 << 63)
    } else {
        (positive, negative, 0)
    };
    let mut borrow = false;
    for (limb, other) in magnitude.iter_mut().zip(subtrahend) {
        let (difference, first) = limb.overflowing_sub(other);
        let (difference, second) = difference.overflowing_sub(u64::from(borrow));
        *limb = difference;
        borrow = first || second;
    }
    debug_assert!(!borrow);
    let mut remainder = 0u128;
    for limb in magnitude.iter_mut().rev() {
        let dividend = (remainder << 64) | *limb as u128;
        *limb = (dividend / count as u128) as u64;
        remainder = dividend % count as u128;
    }
    let highest = magnitude
        .iter()
        .rposition(|limb| *limb != 0)
        .map(|index| index * 64 + 63 - magnitude[index].leading_zeros() as usize)
        .unwrap_or(0);
    let mut shift = highest.saturating_sub(52);
    let bit = |index: usize| (magnitude[index / 64] >> (index % 64)) & 1;
    let mut mantissa = (0..53).fold(0u64, |value, index| value | (bit(shift + index) << index));
    let rounding = if shift == 0 {
        (remainder * 2).cmp(&(count as u128))
    } else if bit(shift - 1) == 0 {
        Ordering::Less
    } else if remainder != 0 || (0..shift - 1).any(|index| bit(index) != 0) {
        Ordering::Greater
    } else {
        Ordering::Equal
    };
    if rounding == Ordering::Greater || (rounding == Ordering::Equal && mantissa & 1 != 0) {
        mantissa += 1;
    }
    if mantissa == 1u64 << 53 {
        mantissa >>= 1;
        shift += 1;
    }
    let encoded = if mantissa < 1u64 << 52 {
        mantissa
    } else {
        ((shift as u64 + 1) << 52) | (mantissa - (1u64 << 52))
    };
    let mean = f64::from_bits(sign | encoded);
    if !mean.is_finite() {
        return Err("strict mean overflowed".into());
    }
    Ok(mean)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_mean_preserves_cancellation_and_rounds_ties_once() {
        let tiny = f64::from_bits(1);
        for values in [
            [f64::MAX, -f64::MAX, 3.0 * tiny],
            [3.0 * tiny, f64::MAX, -f64::MAX],
            [-f64::MAX, 3.0 * tiny, f64::MAX],
        ] {
            assert_eq!(stable_mean(values).unwrap(), tiny);
            assert_eq!(stable_mean(values.map(|v| -v)).unwrap(), -tiny);
        }
        assert_eq!(stable_mean([1e308, -1e308, 3e-100]).unwrap(), 1e-100);
        assert_eq!(stable_mean([tiny, 0.0]).unwrap(), 0.0);
        assert_eq!(stable_mean([tiny, 2.0 * tiny]).unwrap(), 2.0 * tiny);
        assert_eq!(
            stable_mean([f64::MIN_POSITIVE, f64::from_bits((1u64 << 52) - 1)]).unwrap(),
            f64::MIN_POSITIVE
        );
        assert_eq!(
            stable_mean([1.0, f64::from_bits(1.0f64.to_bits() + 1)]).unwrap(),
            1.0
        );
        for value in [f64::MAX, -f64::MAX, tiny, -tiny, 1.0, -1.0, 0.0] {
            assert_eq!(
                stable_mean(std::iter::repeat_n(value, MAX_STRICT_PERIOD)).unwrap(),
                value
            );
        }
        assert_eq!(
            stable_mean([1.0, f64::from_bits(1.0f64.to_bits() + 1), 0.0])
                .unwrap()
                .to_bits(),
            0x3fe5_5555_5555_5556
        );
        assert_eq!(
            stable_mean([f64::from_bits(2.0f64.to_bits() - 1), 2.0])
                .unwrap()
                .to_bits(),
            0x4000_0000_0000_0000
        );
        assert_eq!(
            stable_mean([-tiny, 0.0]).unwrap().to_bits(),
            0x8000_0000_0000_0000
        );
        for period in [1, 2, 3, 31, 32, MAX_STRICT_PERIOD] {
            for value in [tiny, -tiny, f64::MAX, -f64::MAX, 1.0, -1.0] {
                assert_eq!(
                    ema_step(value, value, period).unwrap().to_bits(),
                    value.to_bits()
                );
            }
        }
        assert_eq!(ema_step(f64::MAX, -f64::MAX, 3).unwrap(), 0.0);
        assert!(stable_mean([]).is_err());
        assert!(stable_mean([f64::NAN]).is_err());
        assert!(stable_mean(std::iter::repeat_n(1.0, MAX_STRICT_PERIOD + 1)).is_err());
    }

    #[test]
    fn window_keeps_observed_missing_and_fixed_storage_through_staged_clones() {
        for period in [1, 3, 32, MAX_STRICT_PERIOD] {
            let mut window = ObservedWindow::new(period).unwrap();
            for step in 0..period * 4 {
                let mut staged = window.clone();
                staged.push(Some(3.0)).unwrap();
                assert_eq!(
                    std::mem::size_of_val(&staged) + std::mem::size_of_val(staged.slots.as_ref()),
                    ObservedWindow::state_bytes(period).unwrap()
                );
                assert_eq!(staged.mean().unwrap(), (step + 1 >= period).then_some(3.0));
                window = staged;
            }
            let mut fork = window.clone();
            fork.push(None).unwrap();
            assert_eq!(fork.mean().unwrap(), None);
            assert_eq!(window.mean().unwrap(), Some(3.0));
            for _ in 1..period {
                fork.push(Some(6.0)).unwrap();
                assert_eq!(fork.mean().unwrap(), None);
            }
            fork.push(Some(6.0)).unwrap();
            assert_eq!(fork.mean().unwrap(), Some(6.0));
            fork.reset();
            assert_eq!(fork.mean().unwrap(), None);
        }
        assert!(ObservedWindow::new(0).is_err());
        assert!(ObservedWindow::new(MAX_STRICT_PERIOD + 1).is_err());
    }
}
