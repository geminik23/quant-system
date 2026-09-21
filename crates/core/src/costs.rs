//! Pure trading-cost arithmetic for commission and overnight swap.
//!
//! This module owns *how much* a cost is, never *when* it is charged. A replay or live consumer decides the timing, supplies the fill or rollover facts, and applies the returned signed charge to its own ledger.
//!
//! Every charge is signed so that a positive amount reduces the account balance and a negative amount increases it. Commission is always a cost. Swap follows broker convention, where a negative configured value is a nightly charge and a positive value is a nightly credit.
//!
//! Each charge also carries the currency basis it was computed in. Per-lot commission and per-lot swap are expressed directly in the account currency, while notional-rate commission and point-based swap are expressed in the instrument's native profit-and-loss currency and must be converted by the consumer.

use chrono::{Datelike, NaiveDateTime, NaiveTime, Weekday};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::Side;

/// Largest accepted notional commission rate per side.
pub const MAX_NOTIONAL_COMMISSION_RATE: f64 = 0.1;

/// Largest accepted absolute per-lot commission or swap magnitude.
pub const MAX_PER_LOT_COST: f64 = 1.0e6;

/// Largest accepted absolute swap magnitude expressed in price points.
pub const MAX_SWAP_POINTS: f64 = 1.0e6;

/// Which currency a computed charge is denominated in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostBasis {
    /// Already expressed in the account currency; the consumer applies it directly.
    AccountCurrency,
    /// Expressed in the instrument's native profit-and-loss currency; the consumer converts it.
    InstrumentNative,
}

/// What produced one charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CostKind {
    /// Commission charged on the fill that opens or adds to a position.
    EntryCommission,
    /// Commission charged on the fill that reduces or closes a position.
    ExitCommission,
    /// Overnight financing charged or credited at a rollover instant.
    Swap,
}

impl CostKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EntryCommission => "entry_commission",
            Self::ExitCommission => "exit_commission",
            Self::Swap => "swap",
        }
    }
}

/// One computed signed charge before any account conversion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostCharge {
    /// Positive reduces the balance, negative increases it.
    pub amount: f64,
    pub basis: CostBasis,
}

impl CostCharge {
    fn account(amount: f64) -> Self {
        Self {
            amount,
            basis: CostBasis::AccountCurrency,
        }
    }

    fn native(amount: f64) -> Self {
        Self {
            amount,
            basis: CostBasis::InstrumentNative,
        }
    }
}

/// How commission is charged for one instrument.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommissionModel {
    /// Fixed amount per 1.0 lot per side, already expressed in the account currency. The declared currency is validated against the run's account currency when one is known.
    PerLotPerSide { amount: f64, currency: String },
    /// Fraction of traded notional per side, charged in the instrument's native profit-and-loss currency. Buy and sell rates are separate because some venues charge asymmetrically; use equal values for a symmetric venue.
    NotionalRatePerSide { buy_rate: f64, sell_rate: f64 },
}

impl CommissionModel {
    /// Account currency this model is denominated in, when it declares one.
    pub fn declared_currency(&self) -> Option<&str> {
        match self {
            Self::PerLotPerSide { currency, .. } => Some(currency.as_str()),
            Self::NotionalRatePerSide { .. } => None,
        }
    }

    fn rate_for(&self, side: Side) -> Option<f64> {
        match self {
            Self::NotionalRatePerSide {
                buy_rate,
                sell_rate,
            } => Some(match side {
                Side::Buy => *buy_rate,
                Side::Sell => *sell_rate,
            }),
            Self::PerLotPerSide { .. } => None,
        }
    }

    fn validate(&self) -> Result<(), CostValidationError> {
        match self {
            Self::PerLotPerSide { amount, currency } => {
                if !amount.is_finite() || *amount < 0.0 || *amount > MAX_PER_LOT_COST {
                    return Err(CostValidationError::InvalidCommissionAmount { amount: *amount });
                }
                if currency.trim().is_empty() {
                    return Err(CostValidationError::MissingCurrency {
                        field: "commission",
                    });
                }
                Ok(())
            }
            Self::NotionalRatePerSide {
                buy_rate,
                sell_rate,
            } => {
                for rate in [*buy_rate, *sell_rate] {
                    if !rate.is_finite() || !(0.0..=MAX_NOTIONAL_COMMISSION_RATE).contains(&rate) {
                        return Err(CostValidationError::InvalidCommissionRate { rate });
                    }
                }
                Ok(())
            }
        }
    }
}

/// Nightly swap magnitude for one instrument, in broker sign convention where negative charges and positive credits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "unit", rename_all = "snake_case", deny_unknown_fields)]
pub enum SwapAmount {
    /// Price points per 1.0 lot per night, applied in the instrument's native profit-and-loss currency.
    Points { long: f64, short: f64 },
    /// Amount per 1.0 lot per night, already expressed in the account currency.
    Currency {
        long: f64,
        short: f64,
        currency: String,
    },
}

impl SwapAmount {
    /// Account currency this amount is denominated in, when it declares one.
    pub fn declared_currency(&self) -> Option<&str> {
        match self {
            Self::Currency { currency, .. } => Some(currency.as_str()),
            Self::Points { .. } => None,
        }
    }

    fn value_for(&self, side: Side) -> f64 {
        let (long, short) = match self {
            Self::Points { long, short } => (*long, *short),
            Self::Currency { long, short, .. } => (*long, *short),
        };
        match side {
            Side::Buy => long,
            Side::Sell => short,
        }
    }

    fn validate(&self) -> Result<(), CostValidationError> {
        match self {
            Self::Points { long, short } => {
                for value in [*long, *short] {
                    if !value.is_finite() || value.abs() > MAX_SWAP_POINTS {
                        return Err(CostValidationError::InvalidSwapAmount { amount: value });
                    }
                }
                Ok(())
            }
            Self::Currency {
                long,
                short,
                currency,
            } => {
                for value in [*long, *short] {
                    if !value.is_finite() || value.abs() > MAX_PER_LOT_COST {
                        return Err(CostValidationError::InvalidSwapAmount { amount: value });
                    }
                }
                if currency.trim().is_empty() {
                    return Err(CostValidationError::MissingCurrency { field: "swap" });
                }
                Ok(())
            }
        }
    }
}

/// Nightly swap magnitude plus the rollover calendar that charges it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwapSchedule {
    pub amount: SwapAmount,
    /// Rollover instant expressed in the replay's timestamp zone.
    pub rollover: NaiveTime,
    /// Weekday whose rollover charges three nights at once.
    pub triple_weekday: Weekday,
    /// Weekdays whose rollover charges nothing, normally the weekend.
    #[serde(default = "default_skipped_weekdays")]
    pub skipped_weekdays: Vec<Weekday>,
}

fn default_skipped_weekdays() -> Vec<Weekday> {
    vec![Weekday::Sat, Weekday::Sun]
}

impl SwapSchedule {
    /// Number of nights charged by the rollover that occurs on `instant`.
    pub fn nights_at(&self, instant: NaiveDateTime) -> u32 {
        let weekday = instant.weekday();
        if self.skipped_weekdays.contains(&weekday) {
            0
        } else if weekday == self.triple_weekday {
            3
        } else {
            1
        }
    }

    fn validate(&self) -> Result<(), CostValidationError> {
        self.amount.validate()?;
        if self.skipped_weekdays.contains(&self.triple_weekday) {
            return Err(CostValidationError::TripleWeekdaySkipped);
        }
        if self.skipped_weekdays.len() > 7 {
            return Err(CostValidationError::TooManySkippedWeekdays);
        }
        Ok(())
    }
}

/// Complete cost specification for one instrument.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentCosts {
    #[serde(default)]
    pub commission: Option<CommissionModel>,
    #[serde(default)]
    pub swap: Option<SwapSchedule>,
}

impl InstrumentCosts {
    /// True when this specification can never produce a charge.
    pub fn is_empty(&self) -> bool {
        self.commission.is_none() && self.swap.is_none()
    }

    /// Signed commission for one fill of `lots` at `fill_price`.
    ///
    /// `contract_size` is the number of quote-currency units moved by one full lot and one price unit, matching the multiplier the accounting path already applies to profit and loss.
    pub fn commission_for_fill(
        &self,
        side: Side,
        lots: f64,
        fill_price: f64,
        contract_size: f64,
    ) -> Option<CostCharge> {
        let model = self.commission.as_ref()?;
        if !lots.is_finite() || lots <= 0.0 {
            return None;
        }
        match model {
            CommissionModel::PerLotPerSide { amount, .. } => {
                let charge = amount * lots;
                charge.is_finite().then(|| CostCharge::account(charge))
            }
            CommissionModel::NotionalRatePerSide { .. } => {
                let rate = model.rate_for(side)?;
                if !fill_price.is_finite() || !contract_size.is_finite() {
                    return None;
                }
                let charge = fill_price.abs() * lots * contract_size * rate;
                charge.is_finite().then(|| CostCharge::native(charge))
            }
        }
    }

    /// Signed swap for holding `lots` across `nights` rollovers.
    ///
    /// `point_size` is one price point for the instrument, normally `10^-digits`. It is ignored when the schedule is expressed in account currency.
    pub fn swap_for_rollover(
        &self,
        side: Side,
        lots: f64,
        contract_size: f64,
        point_size: f64,
        nights: u32,
    ) -> Option<CostCharge> {
        let schedule = self.swap.as_ref()?;
        if nights == 0 || !lots.is_finite() || lots <= 0.0 {
            return None;
        }
        let nights = f64::from(nights);
        let value = schedule.amount.value_for(side);
        match &schedule.amount {
            SwapAmount::Points { .. } => {
                if !point_size.is_finite() || point_size <= 0.0 || !contract_size.is_finite() {
                    return None;
                }
                let credit = value * point_size * lots * contract_size * nights;
                credit.is_finite().then(|| CostCharge::native(-credit))
            }
            SwapAmount::Currency { .. } => {
                let credit = value * lots * nights;
                credit.is_finite().then(|| CostCharge::account(-credit))
            }
        }
    }

    /// Reject non-finite, negative, or out-of-range configuration before a run starts.
    pub fn validate(&self) -> Result<(), CostValidationError> {
        if let Some(commission) = self.commission.as_ref() {
            commission.validate()?;
        }
        if let Some(swap) = self.swap.as_ref() {
            swap.validate()?;
        }
        Ok(())
    }

    /// Reject account-currency costs that declare a different currency than the run.
    pub fn validate_against_account_currency(
        &self,
        account_currency: &str,
    ) -> Result<(), CostValidationError> {
        let declared = [
            self.commission
                .as_ref()
                .and_then(CommissionModel::declared_currency),
            self.swap
                .as_ref()
                .and_then(|swap| swap.amount.declared_currency()),
        ];
        for currency in declared.into_iter().flatten() {
            if !currency.eq_ignore_ascii_case(account_currency) {
                return Err(CostValidationError::CurrencyMismatch {
                    declared: currency.to_owned(),
                    account: account_currency.to_owned(),
                });
            }
        }
        Ok(())
    }

    /// True when a point-denominated swap requires an instrument point size.
    pub fn requires_point_size(&self) -> bool {
        matches!(
            self.swap.as_ref().map(|swap| &swap.amount),
            Some(SwapAmount::Points { .. })
        )
    }
}

/// Rollover instants strictly after `previous` and at or before `current`.
///
/// `previous` is `None` before the first observed timestamp, where no history exists to charge.
pub fn rollover_instants(
    previous: Option<NaiveDateTime>,
    current: NaiveDateTime,
    rollover: NaiveTime,
) -> Vec<NaiveDateTime> {
    let Some(previous) = previous else {
        return Vec::new();
    };
    if current <= previous {
        return Vec::new();
    }
    let mut instants = Vec::new();
    let mut date = previous.date();
    let last = current.date();
    while date <= last {
        let instant = date.and_time(rollover);
        if instant > previous && instant <= current {
            instants.push(instant);
        }
        let Some(next) = date.succ_opt() else {
            break;
        };
        date = next;
    }
    instants
}

/// Configuration failures detected before a run starts.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum CostValidationError {
    #[error(
        "commission amount {amount} must be finite, non-negative, and within the accepted range"
    )]
    InvalidCommissionAmount { amount: f64 },
    #[error("commission rate {rate} must be finite and within [0, {MAX_NOTIONAL_COMMISSION_RATE}]")]
    InvalidCommissionRate { rate: f64 },
    #[error("swap amount {amount} must be finite and within the accepted range")]
    InvalidSwapAmount { amount: f64 },
    #[error("{field} currency must not be empty")]
    MissingCurrency { field: &'static str },
    #[error("triple swap weekday must not also be skipped")]
    TripleWeekdaySkipped,
    #[error("skipped swap weekdays must not exceed seven entries")]
    TooManySkippedWeekdays,
    #[error("declared cost currency {declared} does not match account currency {account}")]
    CurrencyMismatch { declared: String, account: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn at(year: i32, month: u32, day: u32, hour: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
    }

    fn per_lot() -> InstrumentCosts {
        InstrumentCosts {
            commission: Some(CommissionModel::PerLotPerSide {
                amount: 3.5,
                currency: "USD".into(),
            }),
            swap: None,
        }
    }

    fn notional(buy_rate: f64, sell_rate: f64) -> InstrumentCosts {
        InstrumentCosts {
            commission: Some(CommissionModel::NotionalRatePerSide {
                buy_rate,
                sell_rate,
            }),
            swap: None,
        }
    }

    fn points_swap(long: f64, short: f64) -> InstrumentCosts {
        InstrumentCosts {
            commission: None,
            swap: Some(SwapSchedule {
                amount: SwapAmount::Points { long, short },
                rollover: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
                triple_weekday: Weekday::Wed,
                skipped_weekdays: default_skipped_weekdays(),
            }),
        }
    }

    #[test]
    fn per_lot_commission_scales_with_lots_in_account_currency() {
        let charge = per_lot()
            .commission_for_fill(Side::Buy, 2.0, 1.1, 100_000.0)
            .unwrap();
        assert_eq!(charge.amount, 7.0);
        assert_eq!(charge.basis, CostBasis::AccountCurrency);
    }

    #[test]
    fn notional_commission_uses_side_specific_rate() {
        let costs = notional(0.005, 0.002);
        let buy = costs
            .commission_for_fill(Side::Buy, 1.0, 100.0, 1.0)
            .unwrap();
        let sell = costs
            .commission_for_fill(Side::Sell, 1.0, 100.0, 1.0)
            .unwrap();
        assert_eq!(buy.amount, 0.5);
        assert_eq!(sell.amount, 0.2);
        assert_eq!(buy.basis, CostBasis::InstrumentNative);
    }

    #[test]
    fn point_swap_charges_native_amount_per_night() {
        let charge = points_swap(-6.1, 1.9)
            .swap_for_rollover(Side::Buy, 1.0, 100_000.0, 1.0e-5, 1)
            .unwrap();
        assert!((charge.amount - 6.1).abs() < 1.0e-9);
        assert_eq!(charge.basis, CostBasis::InstrumentNative);
    }

    #[test]
    fn positive_point_swap_is_a_credit() {
        let charge = points_swap(-6.1, 1.9)
            .swap_for_rollover(Side::Sell, 1.0, 100_000.0, 1.0e-5, 1)
            .unwrap();
        assert!((charge.amount + 1.9).abs() < 1.0e-9);
    }

    #[test]
    fn triple_weekday_multiplies_nights() {
        let costs = points_swap(-6.1, 1.9);
        let schedule = costs.swap.as_ref().unwrap();
        assert_eq!(schedule.nights_at(at(2026, 6, 3, 22)), 3);
        assert_eq!(schedule.nights_at(at(2026, 6, 4, 22)), 1);
        assert_eq!(schedule.nights_at(at(2026, 6, 6, 22)), 0);
        assert_eq!(schedule.nights_at(at(2026, 6, 7, 22)), 0);
    }

    #[test]
    fn zero_nights_produce_no_charge() {
        assert!(
            points_swap(-6.1, 1.9)
                .swap_for_rollover(Side::Buy, 1.0, 100_000.0, 1.0e-5, 0)
                .is_none()
        );
    }

    #[test]
    fn account_currency_swap_ignores_point_size() {
        let costs = InstrumentCosts {
            commission: None,
            swap: Some(SwapSchedule {
                amount: SwapAmount::Currency {
                    long: -2.0,
                    short: 0.5,
                    currency: "USD".into(),
                },
                rollover: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
                triple_weekday: Weekday::Wed,
                skipped_weekdays: default_skipped_weekdays(),
            }),
        };
        let charge = costs
            .swap_for_rollover(Side::Buy, 2.0, 100_000.0, f64::NAN, 3)
            .unwrap();
        assert_eq!(charge.amount, 12.0);
        assert_eq!(charge.basis, CostBasis::AccountCurrency);
    }

    #[test]
    fn rollover_instants_are_half_open_on_the_left() {
        let rollover = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        let instants = rollover_instants(Some(at(2026, 6, 1, 22)), at(2026, 6, 3, 22), rollover);
        assert_eq!(instants, vec![at(2026, 6, 2, 22), at(2026, 6, 3, 22)]);
    }

    #[test]
    fn rollover_instants_cover_a_weekend_gap() {
        let rollover = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        let instants = rollover_instants(Some(at(2026, 6, 5, 20)), at(2026, 6, 7, 23), rollover);
        assert_eq!(
            instants,
            vec![at(2026, 6, 5, 22), at(2026, 6, 6, 22), at(2026, 6, 7, 22)]
        );
    }

    #[test]
    fn first_observed_timestamp_charges_nothing() {
        let rollover = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
        assert!(rollover_instants(None, at(2026, 6, 5, 23), rollover).is_empty());
    }

    #[test]
    fn validation_rejects_out_of_range_values() {
        assert!(notional(0.5, 0.0).validate().is_err());
        assert!(notional(-0.1, 0.0).validate().is_err());
        assert!(
            InstrumentCosts {
                commission: Some(CommissionModel::PerLotPerSide {
                    amount: -1.0,
                    currency: "USD".into()
                }),
                swap: None,
            }
            .validate()
            .is_err()
        );
        assert!(per_lot().validate().is_ok());
        assert!(points_swap(-6.1, 1.9).validate().is_ok());
    }

    #[test]
    fn validation_rejects_mismatched_account_currency() {
        assert!(per_lot().validate_against_account_currency("EUR").is_err());
        assert!(per_lot().validate_against_account_currency("usd").is_ok());
        assert!(
            notional(0.005, 0.005)
                .validate_against_account_currency("EUR")
                .is_ok()
        );
    }
}
