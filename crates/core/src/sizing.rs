//! In-place account position sizing for backtests.
//!
//! Sizing consumes the authoritative entry and protective stop after signal resolution. Currency conversion and target allocation are performed by callers.

use crate::types::Side;
use qs_instruments::{
    AdjustmentDirection, Decimal, EconomicsModelId, GridAdjustment, GridRounding,
    InstrumentEconomics, InstrumentSpec, Money, QuantityRules, QuantityUnit,
};
use qs_symbols::SymbolSpec;
use thiserror::Error;

/// Policy for computing the position size of one resolved entry signal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SizingPolicy {
    /// Scale a fixed lot quantity by the signal risk multiplier.
    FixedLot {
        /// Unscaled lot quantity.
        lots: f64,
    },
    /// Risk a fixed amount in account currency.
    FixedRiskAmount {
        /// Unscaled account-currency risk amount.
        amount: f64,
    },
    /// Risk a percentage of the realized balance before the entry.
    BalanceRiskPercent {
        /// Unscaled percentage where 1.0 means one percent.
        percent: f64,
    },
}

/// Indicates whether the symbol maximum reduced the computed lot steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LotCapStatus {
    /// The computed lot steps did not exceed the symbol maximum.
    NotCapped,
    /// The computed lot steps were reduced to the symbol maximum.
    CappedAtMaximum,
}

/// Auditable output from legacy or catalog-backed sizing.
#[derive(Debug, Clone, PartialEq)]
pub struct SizingResult {
    /// Authoritative tradable quantity in legacy lot steps or catalog quantity-grid steps.
    pub final_lot_steps: u64,
    /// Final standard-lot quantity derived from `final_lot_steps` and the active quantity rules.
    pub final_lot: f64,
    /// Raw lot quantity after applying the signal risk multiplier and before lot constraints.
    pub scaled_raw_lot: f64,
    /// Requested account-currency risk before lot constraints for monetary policies.
    pub requested_account_risk: Option<f64>,
    /// Loss in the symbol's native P&L currency for one lot at the protective stop.
    pub native_loss_per_lot: Option<f64>,
    /// Account-currency loss for one lot at the protective stop.
    pub account_loss_per_lot: Option<f64>,
    /// Maximum-lot cap status.
    pub cap_status: LotCapStatus,
    /// Exact requested and final quantity for catalog-backed sizing.
    pub quantity_adjustment: Option<GridAdjustment<Decimal>>,
    /// Exact notional after quantity adjustment for catalog-backed sizing.
    pub final_notional: Option<Money>,
}

/// Stable structured failures from [`compute_size`].
#[derive(Debug, Clone, PartialEq, Error)]
pub enum SizingError {
    /// The signal risk multiplier is not usable.
    #[error("risk_multiplier must be finite and positive, got {value}")]
    InvalidRiskMultiplier {
        /// Invalid multiplier.
        value: f64,
    },
    /// The fixed lot policy value is not usable.
    #[error("fixed lots must be finite and positive, got {value}")]
    InvalidFixedLots {
        /// Invalid lot quantity.
        value: f64,
    },
    /// The fixed account risk policy value is not usable.
    #[error("fixed risk amount must be finite and positive, got {value}")]
    InvalidFixedRiskAmount {
        /// Invalid account-currency amount.
        value: f64,
    },
    /// The balance percentage policy value is not usable.
    #[error("balance risk percent must be finite and positive, got {value}")]
    InvalidBalanceRiskPercent {
        /// Invalid percentage.
        value: f64,
    },
    /// The realized balance required by a balance policy is not usable.
    #[error("balance_before must be finite and positive, got {value}")]
    InvalidBalanceBefore {
        /// Invalid realized balance.
        value: f64,
    },
    /// The authoritative entry price is not usable.
    #[error("entry price must be finite and positive, got {value}")]
    InvalidEntryPrice {
        /// Invalid entry price.
        value: f64,
    },
    /// The supplied protective stop is not usable.
    #[error("protective stop must be finite and positive, got {value}")]
    InvalidProtectiveStop {
        /// Invalid stop price.
        value: f64,
    },
    /// A monetary policy was used without a protective stop.
    #[error("monetary sizing requires a protective stop")]
    MissingProtectiveStop,
    /// A stop is not on the loss side of the authoritative entry.
    #[error("invalid {side} protective stop geometry: entry {entry_price}, stop {stop_price}")]
    InvalidStopGeometry {
        /// Trade side.
        side: Side,
        /// Authoritative entry price.
        entry_price: f64,
        /// Protective stop price.
        stop_price: f64,
    },
    /// Entry and stop collapse to the same tick at the symbol price precision.
    #[error("entry {entry_price} and stop {stop_price} have zero distance at {digits} digits")]
    StopDistanceBelowTick {
        /// Authoritative entry price.
        entry_price: f64,
        /// Protective stop price.
        stop_price: f64,
        /// Symbol price digits.
        digits: u16,
    },
    /// A price cannot be represented safely at the symbol precision.
    #[error("{field} price {value} is out of range at {digits} digits")]
    PriceOutOfRange {
        /// Price field name.
        field: &'static str,
        /// Out-of-range price.
        value: f64,
        /// Symbol price digits.
        digits: u16,
    },
    /// A monetary policy did not receive an account-currency loss per lot.
    #[error("monetary sizing requires account_loss_per_lot")]
    MissingAccountLossPerLot,
    /// The supplied account-currency loss per lot is not usable.
    #[error("account_loss_per_lot must be finite and positive, got {value}")]
    InvalidAccountLossPerLot {
        /// Invalid account-currency loss per lot.
        value: f64,
    },
    /// The symbol price precision is internally inconsistent or unsupported.
    #[error("invalid symbol price precision: digits={digits}, pip_position={pip_position}")]
    InvalidPricePrecision {
        /// Number of symbol price digits.
        digits: u16,
        /// Position of one pip.
        pip_position: u16,
    },
    /// The symbol lot base unit count is invalid.
    #[error("symbol lot_base_units must be positive, got {value}")]
    InvalidLotBaseUnits {
        /// Invalid base unit count.
        value: i64,
    },
    /// The symbol lot step unit count is invalid.
    #[error("symbol lot_step_units must be positive, got {value}")]
    InvalidLotStepUnits {
        /// Invalid lot step unit count.
        value: i64,
    },
    /// The symbol minimum lot step count is invalid.
    #[error("symbol lot_min_steps must be positive, got {value}")]
    InvalidMinimumLotSteps {
        /// Invalid minimum lot step count.
        value: i64,
    },
    /// The symbol maximum lot step count is invalid.
    #[error("symbol lot_max_steps {maximum} must be zero or at least lot_min_steps {minimum}")]
    InvalidMaximumLotSteps {
        /// Invalid maximum lot step count.
        maximum: i64,
        /// Configured minimum lot step count.
        minimum: i64,
    },
    /// Applying the signal multiplier produced an unusable policy value.
    #[error(
        "scaling policy value {base_value} by risk_multiplier {risk_multiplier} did not produce a finite positive value"
    )]
    InvalidScaledPolicyValue {
        /// Unscaled fixed lots or account risk.
        base_value: f64,
        /// Signal risk multiplier.
        risk_multiplier: f64,
    },
    /// Monetary division produced an unusable raw lot quantity.
    #[error("scaled raw lot must be finite and positive, got {value}")]
    InvalidScaledRawLot {
        /// Invalid raw lot quantity.
        value: f64,
    },
    /// The computed native loss for one lot overflowed.
    #[error("native loss per lot is not finite for entry {entry_price} and stop {stop_price}")]
    InvalidNativeLossPerLot {
        /// Authoritative entry price.
        entry_price: f64,
        /// Protective stop price.
        stop_price: f64,
    },
    /// The floored lot quantity does not meet the symbol minimum.
    #[error(
        "scaled raw lot {scaled_raw_lot} floors to {floored_lot_steps} steps below minimum {minimum_lot_steps}"
    )]
    BelowMinimumLot {
        /// Lot quantity before constraints.
        scaled_raw_lot: f64,
        /// Lot steps after flooring.
        floored_lot_steps: u64,
        /// Required minimum lot steps.
        minimum_lot_steps: u64,
    },
    /// The lot quantity cannot be represented by the authoritative step count.
    #[error("scaled raw lot {scaled_raw_lot} exceeds the supported lot step count")]
    LotStepOverflow {
        /// Lot quantity before constraints.
        scaled_raw_lot: f64,
    },
}

/// Catalog-backed sizing failures.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum InstrumentSizingError {
    /// Existing policy, price, or floating-point sizing validation failed.
    #[error(transparent)]
    Sizing(#[from] SizingError),
    /// The instrument quantity and P&L model combination is not supported by this sizing path.
    #[error(
        "unsupported instrument sizing combination: quantity_unit={quantity_unit:?}, pnl_model={pnl_model}"
    )]
    UnsupportedInstrumentSizing {
        /// Instrument quantity unit.
        quantity_unit: QuantityUnit,
        /// Instrument P&L model.
        pnl_model: EconomicsModelId,
    },
    /// Catalog-backed monetary sizing did not receive a native-to-account conversion rate.
    #[error("catalog-backed monetary sizing requires native_to_account_rate")]
    MissingNativeToAccountRate,
    /// The supplied native-to-account conversion rate is not usable.
    #[error("native_to_account_rate must be finite and positive, got {value}")]
    InvalidNativeToAccountRate {
        /// Invalid conversion rate.
        value: f64,
    },
    /// The exact instrument decimal cannot be represented as a finite `f64`.
    #[error("{field} decimal {value} cannot be represented as a finite f64")]
    ExactDecimalOutOfRange {
        /// Instrument field being converted.
        field: &'static str,
        /// Exact value that could not be converted.
        value: Decimal,
    },
    /// The quantity grid origin is incompatible with the retained zero-based lot-step result.
    #[error("quantity grid origin must be zero for standard-lot sizing, got {value}")]
    UnsupportedQuantityGridOrigin {
        /// Unsupported grid origin.
        value: Decimal,
    },
    /// An exact quantity bound is not on the declared quantity grid.
    #[error("{field} quantity {value} is not on the declared quantity grid")]
    QuantityBoundOffGrid {
        /// Quantity bound field.
        field: &'static str,
        /// Off-grid exact quantity.
        value: Decimal,
    },
    /// The exact quantity bounds are inconsistent.
    #[error("maximum quantity {maximum} is below minimum quantity {minimum}")]
    InvalidQuantityBounds {
        /// Minimum standard-lot quantity.
        minimum: Decimal,
        /// Maximum standard-lot quantity.
        maximum: Decimal,
    },
    /// An exact quantity bound cannot be represented as a supported lot-step count.
    #[error("{field} quantity {value} exceeds the supported lot step count")]
    QuantityStepOverflow {
        /// Quantity bound field.
        field: &'static str,
        /// Exact quantity that overflowed.
        value: Decimal,
    },
    /// Exact quantity-grid arithmetic failed.
    #[error("invalid quantity grid: {0}")]
    InvalidQuantityGrid(#[from] qs_instruments::GridError),
    /// A replay price is outside the declared price grid.
    #[error("{field} price {value} is outside the declared price grid")]
    PriceOffGrid {
        /// Price field being validated.
        field: &'static str,
        /// Exact off-grid value.
        value: Decimal,
    },
    /// The notional asset is unsupported by the current quote-linear sizing model.
    #[error("notional asset {notional_asset} must match settlement asset {settlement_asset}")]
    UnsupportedNotionalAsset {
        /// Declared notional asset.
        notional_asset: qs_instruments::AssetId,
        /// Instrument settlement asset.
        settlement_asset: qs_instruments::AssetId,
    },
    /// Exact final notional is below the declared minimum.
    #[error("final notional {notional} is below minimum {minimum}")]
    BelowMinimumNotional {
        /// Calculated final notional.
        notional: Decimal,
        /// Required minimum.
        minimum: Decimal,
    },
    /// Exact final notional exceeds the declared maximum.
    #[error("final notional {notional} exceeds maximum {maximum}")]
    AboveMaximumNotional {
        /// Calculated final notional.
        notional: Decimal,
        /// Allowed maximum.
        maximum: Decimal,
    },
    /// Exact catalog arithmetic failed.
    #[error("invalid exact instrument arithmetic: {0}")]
    ExactArithmetic(#[from] qs_instruments::DecimalError),
}

#[derive(Debug, Clone, Copy)]
struct ValidatedLotSpec {
    lot_base_units: u64,
    lot_step_units: u64,
    lot_min_steps: u64,
    lot_max_steps: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct ValidatedQuantityRules {
    step: Decimal,
    minimum_steps: u64,
    maximum_steps: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum PolicyBasis {
    FixedLots(f64),
    AccountRisk(f64),
}

/// Compute one in-place account position size.
///
/// `balance_before` is the realized account balance immediately before the entry. `entry_price` and `protective_stop` are authoritative resolved prices. Monetary policies require `account_loss_per_lot` to be converted to account currency by the caller. Fixed-lot sizing ignores `balance_before` and `account_loss_per_lot`, and it may omit the protective stop.
#[allow(clippy::too_many_arguments)]
pub fn compute_size(
    policy: &SizingPolicy,
    risk_multiplier: f64,
    balance_before: f64,
    side: Side,
    entry_price: f64,
    protective_stop: Option<f64>,
    spec: &SymbolSpec,
    account_loss_per_lot: Option<f64>,
) -> Result<SizingResult, SizingError> {
    validate_risk_multiplier(risk_multiplier)?;
    validate_entry_price(entry_price)?;
    let lot_spec = validate_lot_spec(spec)?;
    let basis = policy_basis(policy, balance_before)?;
    let native_loss_per_lot = protective_stop
        .map(|stop_price| compute_native_loss_per_lot(side, entry_price, stop_price, spec))
        .transpose()?;
    let sizing = compute_raw_size(
        basis,
        risk_multiplier,
        native_loss_per_lot,
        account_loss_per_lot,
    )?;
    let (final_lot_steps, final_lot, cap_status) =
        apply_lot_constraints(sizing.scaled_raw_lot, lot_spec)?;

    Ok(sizing.into_result(final_lot_steps, final_lot, cap_status, None, None))
}

/// Compute one account position size from catalog-backed quantity and economics contracts.
///
/// This compatibility seam validates exact quantity rules but has no full specification for price-grid or notional validation. New replay consumers should use [`compute_instrument_size_for_spec`].
#[allow(clippy::too_many_arguments)]
pub fn compute_instrument_size(
    policy: &SizingPolicy,
    risk_multiplier: f64,
    balance_before: f64,
    side: Side,
    entry_price: f64,
    protective_stop: Option<f64>,
    price_digits: u16,
    quantity_rules: &QuantityRules,
    economics: &InstrumentEconomics,
    native_to_account_rate: Option<f64>,
) -> Result<SizingResult, InstrumentSizingError> {
    validate_risk_multiplier(risk_multiplier)?;
    validate_entry_price(entry_price)?;
    validate_instrument_economics(economics)?;
    let quantity_rules = validate_quantity_rules(quantity_rules)?;
    let basis = policy_basis(policy, balance_before)?;
    let native_loss_per_lot = protective_stop
        .map(|stop_price| {
            compute_instrument_native_loss_per_lot(
                side,
                entry_price,
                stop_price,
                price_digits,
                economics,
            )
        })
        .transpose()?;
    let account_loss_per_lot =
        instrument_account_loss(basis, native_loss_per_lot, native_to_account_rate)?;
    let sizing = compute_raw_size(
        basis,
        risk_multiplier,
        native_loss_per_lot,
        account_loss_per_lot,
    )?;
    let (final_lot_steps, final_lot, cap_status, quantity_adjustment) =
        apply_quantity_constraints(sizing.scaled_raw_lot, quantity_rules)?;
    Ok(sizing.into_result(
        final_lot_steps,
        final_lot,
        cap_status,
        Some(quantity_adjustment),
        None,
    ))
}

/// Compute catalog-backed sizing with complete price, quantity, economics, and notional validation.
#[allow(clippy::too_many_arguments)]
pub fn compute_instrument_size_for_spec(
    policy: &SizingPolicy,
    risk_multiplier: f64,
    balance_before: f64,
    side: Side,
    entry_price: f64,
    protective_stop: Option<f64>,
    spec: &InstrumentSpec,
    native_to_account_rate: Option<f64>,
) -> Result<SizingResult, InstrumentSizingError> {
    validate_risk_multiplier(risk_multiplier)?;
    validate_entry_price(entry_price)?;
    validate_instrument_economics(&spec.economics)?;
    let quantity_rules = validate_quantity_rules(&spec.quantity)?;
    let entry_decimal = validate_price_grid("entry", entry_price, spec)?;
    if let Some(stop) = protective_stop {
        validate_price_grid("protective stop", stop, spec)?;
    }
    let basis = policy_basis(policy, balance_before)?;
    let native_loss_per_lot = protective_stop
        .map(|stop_price| {
            compute_instrument_native_loss_per_lot(
                side,
                entry_price,
                stop_price,
                u16::from(spec.price.display_scale),
                &spec.economics,
            )
        })
        .transpose()?;
    let account_loss_per_lot =
        instrument_account_loss(basis, native_loss_per_lot, native_to_account_rate)?;
    let sizing = compute_raw_size(
        basis,
        risk_multiplier,
        native_loss_per_lot,
        account_loss_per_lot,
    )?;
    let (final_lot_steps, final_lot, cap_status, quantity_adjustment) =
        apply_quantity_constraints(sizing.scaled_raw_lot, quantity_rules)?;
    let final_notional =
        validate_final_notional(entry_decimal, quantity_adjustment.adjusted, spec)?;

    Ok(sizing.into_result(
        final_lot_steps,
        final_lot,
        cap_status,
        Some(quantity_adjustment),
        final_notional,
    ))
}

#[derive(Debug, Clone, Copy)]
struct RawSizingResult {
    scaled_raw_lot: f64,
    requested_account_risk: Option<f64>,
    native_loss_per_lot: Option<f64>,
    account_loss_per_lot: Option<f64>,
}

impl RawSizingResult {
    fn into_result(
        self,
        final_lot_steps: u64,
        final_lot: f64,
        cap_status: LotCapStatus,
        quantity_adjustment: Option<GridAdjustment<Decimal>>,
        final_notional: Option<Money>,
    ) -> SizingResult {
        SizingResult {
            final_lot_steps,
            final_lot,
            scaled_raw_lot: self.scaled_raw_lot,
            requested_account_risk: self.requested_account_risk,
            native_loss_per_lot: self.native_loss_per_lot,
            account_loss_per_lot: self.account_loss_per_lot,
            cap_status,
            quantity_adjustment,
            final_notional,
        }
    }
}

fn compute_raw_size(
    basis: PolicyBasis,
    risk_multiplier: f64,
    native_loss_per_lot: Option<f64>,
    account_loss_per_lot: Option<f64>,
) -> Result<RawSizingResult, SizingError> {
    let scaled_policy_value = match basis {
        PolicyBasis::FixedLots(base_value) | PolicyBasis::AccountRisk(base_value) => {
            let scaled = base_value * risk_multiplier;
            if !scaled.is_finite() || scaled <= 0.0 {
                return Err(SizingError::InvalidScaledPolicyValue {
                    base_value,
                    risk_multiplier,
                });
            }
            scaled
        }
    };

    let (scaled_raw_lot, requested_account_risk, result_account_loss_per_lot) = match basis {
        PolicyBasis::FixedLots(_) => (scaled_policy_value, None, None),
        PolicyBasis::AccountRisk(_) => {
            native_loss_per_lot.ok_or(SizingError::MissingProtectiveStop)?;
            let account_loss_per_lot = account_loss_per_lot
                .ok_or(SizingError::MissingAccountLossPerLot)
                .and_then(validate_account_loss_per_lot)?;
            let scaled_raw_lot = scaled_policy_value / account_loss_per_lot;
            if !scaled_raw_lot.is_finite() || scaled_raw_lot <= 0.0 {
                return Err(SizingError::InvalidScaledRawLot {
                    value: scaled_raw_lot,
                });
            }
            (
                scaled_raw_lot,
                Some(scaled_policy_value),
                Some(account_loss_per_lot),
            )
        }
    };

    Ok(RawSizingResult {
        scaled_raw_lot,
        requested_account_risk,
        native_loss_per_lot,
        account_loss_per_lot: result_account_loss_per_lot,
    })
}

fn instrument_account_loss(
    basis: PolicyBasis,
    native_loss_per_lot: Option<f64>,
    native_to_account_rate: Option<f64>,
) -> Result<Option<f64>, InstrumentSizingError> {
    match basis {
        PolicyBasis::FixedLots(_) => Ok(None),
        PolicyBasis::AccountRisk(_) => {
            let native_loss_per_lot =
                native_loss_per_lot.ok_or(SizingError::MissingProtectiveStop)?;
            let conversion_rate = native_to_account_rate
                .ok_or(InstrumentSizingError::MissingNativeToAccountRate)
                .and_then(validate_native_to_account_rate)?;
            let account_loss_per_lot = native_loss_per_lot * conversion_rate;
            Ok(Some(validate_account_loss_per_lot(account_loss_per_lot)?))
        }
    }
}

fn validate_risk_multiplier(value: f64) -> Result<(), SizingError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(SizingError::InvalidRiskMultiplier { value })
    }
}

fn validate_entry_price(value: f64) -> Result<(), SizingError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(SizingError::InvalidEntryPrice { value })
    }
}

fn validate_account_loss_per_lot(value: f64) -> Result<f64, SizingError> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(SizingError::InvalidAccountLossPerLot { value })
    }
}

fn validate_native_to_account_rate(value: f64) -> Result<f64, InstrumentSizingError> {
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(InstrumentSizingError::InvalidNativeToAccountRate { value })
    }
}

fn policy_basis(policy: &SizingPolicy, balance_before: f64) -> Result<PolicyBasis, SizingError> {
    match *policy {
        SizingPolicy::FixedLot { lots } => {
            if lots.is_finite() && lots > 0.0 {
                Ok(PolicyBasis::FixedLots(lots))
            } else {
                Err(SizingError::InvalidFixedLots { value: lots })
            }
        }
        SizingPolicy::FixedRiskAmount { amount } => {
            if amount.is_finite() && amount > 0.0 {
                Ok(PolicyBasis::AccountRisk(amount))
            } else {
                Err(SizingError::InvalidFixedRiskAmount { value: amount })
            }
        }
        SizingPolicy::BalanceRiskPercent { percent } => {
            if !percent.is_finite() || percent <= 0.0 {
                return Err(SizingError::InvalidBalanceRiskPercent { value: percent });
            }
            if !balance_before.is_finite() || balance_before <= 0.0 {
                return Err(SizingError::InvalidBalanceBefore {
                    value: balance_before,
                });
            }
            let account_risk = balance_before * (percent / 100.0);
            if account_risk.is_finite() && account_risk > 0.0 {
                Ok(PolicyBasis::AccountRisk(account_risk))
            } else {
                Err(SizingError::InvalidScaledPolicyValue {
                    base_value: balance_before,
                    risk_multiplier: percent / 100.0,
                })
            }
        }
    }
}

fn validate_lot_spec(spec: &SymbolSpec) -> Result<ValidatedLotSpec, SizingError> {
    let lot_base_units = u64::try_from(spec.lot_base_units)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(SizingError::InvalidLotBaseUnits {
            value: spec.lot_base_units,
        })?;
    let lot_step_units = u64::try_from(spec.lot_step_units)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(SizingError::InvalidLotStepUnits {
            value: spec.lot_step_units,
        })?;
    let lot_min_steps = u64::try_from(spec.lot_min_steps)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(SizingError::InvalidMinimumLotSteps {
            value: spec.lot_min_steps,
        })?;
    let lot_max_steps = match spec.lot_max_steps {
        0 => None,
        maximum if maximum >= spec.lot_min_steps => Some(maximum as u64),
        maximum => {
            return Err(SizingError::InvalidMaximumLotSteps {
                maximum,
                minimum: spec.lot_min_steps,
            });
        }
    };

    Ok(ValidatedLotSpec {
        lot_base_units,
        lot_step_units,
        lot_min_steps,
        lot_max_steps,
    })
}

fn validate_instrument_economics(
    economics: &InstrumentEconomics,
) -> Result<(), InstrumentSizingError> {
    let supported_model = matches!(
        economics.pnl_model.as_str(),
        EconomicsModelId::FX_QUOTE_LINEAR_V1 | EconomicsModelId::CFD_QUOTE_LINEAR_V1
    );
    if economics.quantity_unit == QuantityUnit::StandardLot && supported_model {
        Ok(())
    } else {
        Err(InstrumentSizingError::UnsupportedInstrumentSizing {
            quantity_unit: economics.quantity_unit,
            pnl_model: economics.pnl_model.clone(),
        })
    }
}

fn validate_quantity_rules(
    rules: &QuantityRules,
) -> Result<ValidatedQuantityRules, InstrumentSizingError> {
    if !rules.grid.origin.is_zero() {
        return Err(InstrumentSizingError::UnsupportedQuantityGridOrigin {
            value: rules.grid.origin,
        });
    }

    let step = rules.grid.step.get();
    let minimum = rules.minimum.get();
    if !rules.grid.contains(minimum)? {
        return Err(InstrumentSizingError::QuantityBoundOffGrid {
            field: "minimum",
            value: minimum,
        });
    }
    let minimum_steps = exact_quantity_steps("minimum", minimum, rules.grid.step.get())?;

    let maximum_steps = match rules.maximum {
        Some(maximum) => {
            let maximum = maximum.get();
            if maximum < minimum {
                return Err(InstrumentSizingError::InvalidQuantityBounds { minimum, maximum });
            }
            if !rules.grid.contains(maximum)? {
                return Err(InstrumentSizingError::QuantityBoundOffGrid {
                    field: "maximum",
                    value: maximum,
                });
            }
            Some(exact_quantity_steps(
                "maximum",
                maximum,
                rules.grid.step.get(),
            )?)
        }
        None => None,
    };

    Ok(ValidatedQuantityRules {
        step,
        minimum_steps,
        maximum_steps,
    })
}

fn exact_quantity_steps(
    field: &'static str,
    quantity: Decimal,
    step: Decimal,
) -> Result<u64, InstrumentSizingError> {
    let scale = quantity.scale().max(step.scale());
    let quantity_factor = 10_i128
        .checked_pow(u32::from(scale - quantity.scale()))
        .ok_or(InstrumentSizingError::QuantityStepOverflow {
            field,
            value: quantity,
        })?;
    let step_factor = 10_i128.checked_pow(u32::from(scale - step.scale())).ok_or(
        InstrumentSizingError::QuantityStepOverflow {
            field,
            value: quantity,
        },
    )?;
    let quantity_coefficient = quantity.coefficient().checked_mul(quantity_factor).ok_or(
        InstrumentSizingError::QuantityStepOverflow {
            field,
            value: quantity,
        },
    )?;
    let step_coefficient = step.coefficient().checked_mul(step_factor).ok_or(
        InstrumentSizingError::QuantityStepOverflow {
            field,
            value: quantity,
        },
    )?;
    let steps = quantity_coefficient / step_coefficient;
    u64::try_from(steps).map_err(|_| InstrumentSizingError::QuantityStepOverflow {
        field,
        value: quantity,
    })
}

fn exact_decimal_to_f64(field: &'static str, value: Decimal) -> Result<f64, InstrumentSizingError> {
    let converted = value.coefficient() as f64 / 10_f64.powi(i32::from(value.scale()));
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(InstrumentSizingError::ExactDecimalOutOfRange { field, value })
    }
}

fn validate_price_grid(
    field: &'static str,
    price: f64,
    spec: &InstrumentSpec,
) -> Result<Decimal, InstrumentSizingError> {
    let scale = 10_f64.powi(i32::from(spec.price.display_scale));
    let normalized = (price * scale).round() / scale;
    let price = Decimal::checked_from_f64(normalized)?;
    if spec.price.grid.contains(price)? {
        Ok(price)
    } else {
        Err(InstrumentSizingError::PriceOffGrid {
            field,
            value: price,
        })
    }
}

fn validate_final_notional(
    entry_price: Decimal,
    quantity: Decimal,
    spec: &InstrumentSpec,
) -> Result<Option<Money>, InstrumentSizingError> {
    let Some(rules) = &spec.notional else {
        return Ok(None);
    };
    if rules.asset != spec.economics.settlement_asset {
        return Err(InstrumentSizingError::UnsupportedNotionalAsset {
            notional_asset: rules.asset.clone(),
            settlement_asset: spec.economics.settlement_asset.clone(),
        });
    }
    let amount = entry_price
        .checked_mul(quantity)?
        .checked_mul(spec.economics.contract_multiplier.get())?;
    if let Some(minimum) = rules.minimum
        && amount < minimum.get()
    {
        return Err(InstrumentSizingError::BelowMinimumNotional {
            notional: amount,
            minimum: minimum.get(),
        });
    }
    if let Some(maximum) = rules.maximum
        && amount > maximum.get()
    {
        return Err(InstrumentSizingError::AboveMaximumNotional {
            notional: amount,
            maximum: maximum.get(),
        });
    }
    Ok(Some(Money {
        asset: rules.asset.clone(),
        amount,
    }))
}

/// Compute the positive native P&L currency loss for one standard lot at a protective stop.
///
/// Prices are normalized to `SymbolSpec::digits` before distance is measured. The helper uses the same validation and geometry path as [`compute_size`].
pub fn compute_native_loss_per_lot(
    side: Side,
    entry_price: f64,
    protective_stop: f64,
    spec: &SymbolSpec,
) -> Result<f64, SizingError> {
    validate_entry_price(entry_price)?;
    if spec.lot_base_units <= 0 {
        return Err(SizingError::InvalidLotBaseUnits {
            value: spec.lot_base_units,
        });
    }
    compute_native_loss_with_multiplier(
        side,
        entry_price,
        protective_stop,
        spec.digits,
        spec.pip_position,
        spec.lot_base_units as f64,
    )
}

/// Compute native stop loss for one catalog-backed standard lot.
///
/// The contract multiplier is the economic authority. Quantity storage scale does not participate in this calculation.
pub fn compute_instrument_native_loss_per_lot(
    side: Side,
    entry_price: f64,
    protective_stop: f64,
    price_digits: u16,
    economics: &InstrumentEconomics,
) -> Result<f64, InstrumentSizingError> {
    validate_instrument_economics(economics)?;
    let contract_multiplier = exact_decimal_to_f64(
        "instrument contract multiplier",
        economics.contract_multiplier.get(),
    )?;
    Ok(compute_native_loss_with_multiplier(
        side,
        entry_price,
        protective_stop,
        price_digits,
        price_digits,
        contract_multiplier,
    )?)
}

fn compute_native_loss_with_multiplier(
    side: Side,
    entry_price: f64,
    protective_stop: f64,
    digits: u16,
    pip_position: u16,
    contract_multiplier: f64,
) -> Result<f64, SizingError> {
    validate_entry_price(entry_price)?;
    if !protective_stop.is_finite() || protective_stop <= 0.0 {
        return Err(SizingError::InvalidProtectiveStop {
            value: protective_stop,
        });
    }
    let valid_geometry = match side {
        Side::Buy => protective_stop < entry_price,
        Side::Sell => protective_stop > entry_price,
    };
    if !valid_geometry {
        return Err(SizingError::InvalidStopGeometry {
            side,
            entry_price,
            stop_price: protective_stop,
        });
    }
    if digits > 18 || pip_position > digits {
        return Err(SizingError::InvalidPricePrecision {
            digits,
            pip_position,
        });
    }

    let scale = 10_i64.pow(digits as u32) as f64;
    let entry_ticks = price_to_ticks("entry", entry_price, digits, scale)?;
    let stop_ticks = price_to_ticks("protective stop", protective_stop, digits, scale)?;
    let distance_ticks = entry_ticks.abs_diff(stop_ticks);
    if distance_ticks == 0 {
        return Err(SizingError::StopDistanceBelowTick {
            entry_price,
            stop_price: protective_stop,
            digits,
        });
    }

    let native_loss_per_lot = distance_ticks as f64 * contract_multiplier / scale;
    if !native_loss_per_lot.is_finite() || native_loss_per_lot <= 0.0 {
        return Err(SizingError::InvalidNativeLossPerLot {
            entry_price,
            stop_price: protective_stop,
        });
    }

    Ok(native_loss_per_lot)
}

fn price_to_ticks(
    field: &'static str,
    value: f64,
    digits: u16,
    scale: f64,
) -> Result<i64, SizingError> {
    let scaled = value * scale;
    if !scaled.is_finite() || scaled >= i64::MAX as f64 {
        return Err(SizingError::PriceOutOfRange {
            field,
            value,
            digits,
        });
    }
    Ok(scaled.round() as i64)
}

fn apply_lot_constraints(
    scaled_raw_lot: f64,
    spec: ValidatedLotSpec,
) -> Result<(u64, f64, LotCapStatus), SizingError> {
    if !scaled_raw_lot.is_finite() || scaled_raw_lot <= 0.0 {
        return Err(SizingError::InvalidScaledRawLot {
            value: scaled_raw_lot,
        });
    }

    let raw_steps = scaled_raw_lot * spec.lot_base_units as f64 / spec.lot_step_units as f64;
    if !raw_steps.is_finite() || raw_steps >= u64::MAX as f64 {
        return Err(SizingError::LotStepOverflow { scaled_raw_lot });
    }
    let floored_lot_steps = raw_steps.floor() as u64;
    if floored_lot_steps < spec.lot_min_steps {
        return Err(SizingError::BelowMinimumLot {
            scaled_raw_lot,
            floored_lot_steps,
            minimum_lot_steps: spec.lot_min_steps,
        });
    }

    let (final_lot_steps, cap_status) = match spec.lot_max_steps {
        Some(maximum) if floored_lot_steps > maximum => (maximum, LotCapStatus::CappedAtMaximum),
        _ => (floored_lot_steps, LotCapStatus::NotCapped),
    };
    let final_lot =
        final_lot_steps as f64 * spec.lot_step_units as f64 / spec.lot_base_units as f64;

    Ok((final_lot_steps, final_lot, cap_status))
}

fn apply_quantity_constraints(
    scaled_raw_lot: f64,
    rules: ValidatedQuantityRules,
) -> Result<(u64, f64, LotCapStatus, GridAdjustment<Decimal>), InstrumentSizingError> {
    if !scaled_raw_lot.is_finite() || scaled_raw_lot <= 0.0 {
        return Err(SizingError::InvalidScaledRawLot {
            value: scaled_raw_lot,
        }
        .into());
    }

    let requested = Decimal::checked_from_f64(scaled_raw_lot)?;
    let floored = qs_instruments::DecimalGrid::new(Decimal::ZERO, rules.step.try_into()?)
        .adjust(requested, GridRounding::Floor)?;
    let floored_lot_steps = exact_quantity_steps("adjusted", floored.adjusted, rules.step)?;
    if floored_lot_steps < rules.minimum_steps {
        return Err(SizingError::BelowMinimumLot {
            scaled_raw_lot,
            floored_lot_steps,
            minimum_lot_steps: rules.minimum_steps,
        }
        .into());
    }

    let (final_lot_steps, cap_status) = match rules.maximum_steps {
        Some(maximum) if floored_lot_steps > maximum => (maximum, LotCapStatus::CappedAtMaximum),
        _ => (floored_lot_steps, LotCapStatus::NotCapped),
    };
    let final_quantity = rules
        .step
        .checked_mul(Decimal::new(i128::from(final_lot_steps), 0)?)?;
    let final_lot = exact_decimal_to_f64("final quantity", final_quantity)?;
    let direction = match final_quantity.cmp(&requested) {
        std::cmp::Ordering::Less => AdjustmentDirection::Down,
        std::cmp::Ordering::Equal => AdjustmentDirection::Unchanged,
        std::cmp::Ordering::Greater => AdjustmentDirection::Up,
    };

    Ok((
        final_lot_steps,
        final_lot,
        cap_status,
        GridAdjustment {
            requested,
            adjusted: final_quantity,
            direction,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use qs_instruments::{
        AssetId, DecimalGrid, EconomicsModelId, EffectiveInterval, InstrumentAssets, InstrumentId,
        InstrumentSpec, ListingStatus, NotionalRules, PositiveDecimal, PriceRules, QuantityUnit,
    };

    fn decimal(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn positive(value: &str) -> PositiveDecimal {
        value.parse().unwrap()
    }

    fn quantity_rules(storage_scale: u8) -> QuantityRules {
        QuantityRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.01")),
            minimum: positive("0.01"),
            maximum: Some(positive("100")),
            storage_scale,
        }
    }

    fn economics(
        quantity_unit: QuantityUnit,
        model: &str,
        multiplier: &str,
    ) -> InstrumentEconomics {
        InstrumentEconomics {
            pnl_model: EconomicsModelId::new(model).unwrap(),
            quantity_unit,
            contract_multiplier: positive(multiplier),
            settlement_asset: AssetId::new("USD").unwrap(),
            fee_model: None,
            funding_model: None,
            margin_model: None,
        }
    }

    fn standard_lot_economics(multiplier: &str) -> InstrumentEconomics {
        economics(
            QuantityUnit::StandardLot,
            EconomicsModelId::FX_QUOTE_LINEAR_V1,
            multiplier,
        )
    }

    fn instrument_spec(step: &str, notional: Option<NotionalRules>) -> InstrumentSpec {
        let usd = AssetId::new("USD").unwrap();
        InstrumentSpec {
            revision: "1.0.0".parse().unwrap(),
            instrument: InstrumentId::new(
                "broker-a".parse().unwrap(),
                qs_instruments::MarketKind::new(qs_instruments::MarketKind::FX_CFD).unwrap(),
                "EURUSD".parse().unwrap(),
            ),
            effective: EffectiveInterval::new("2026-01-01T00:00:00Z".parse().unwrap(), None)
                .unwrap(),
            status: ListingStatus::Trading,
            assets: InstrumentAssets {
                base: Some("EUR".parse().unwrap()),
                quote: Some(usd.clone()),
                settlement: usd,
                fee_assets: BTreeSet::new(),
            },
            price: PriceRules {
                grid: DecimalGrid::new(Decimal::ZERO, positive("0.00001")),
                display_scale: 5,
            },
            quantity: QuantityRules {
                grid: DecimalGrid::new(Decimal::ZERO, positive(step)),
                minimum: positive(step),
                maximum: Some(positive("100")),
                storage_scale: 2,
            },
            notional,
            economics: standard_lot_economics("100000"),
            aliases: BTreeSet::from(["EURUSD".parse().unwrap()]),
        }
    }

    fn forex_spec() -> SymbolSpec {
        SymbolSpec {
            canonical: "eurusd".into(),
            pip_position: 4,
            digits: 5,
            category: "forex".into(),
            lot_base_units: 100_000,
            lot_step_units: 1_000,
            lot_min_steps: 1,
            lot_max_steps: 0,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn catalog_sizing_uses_quantity_rules_and_contract_multiplier() {
        let result = compute_instrument_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            5,
            &quantity_rules(2),
            &standard_lot_economics("100000"),
            Some(1.0),
        )
        .unwrap();

        assert_eq!(result.native_loss_per_lot, Some(500.0));
        assert_eq!(result.account_loss_per_lot, Some(500.0));
        assert_close(result.scaled_raw_lot, 0.2);
        assert_eq!(result.final_lot_steps, 20);
        assert_close(result.final_lot, 0.2);
    }

    #[test]
    fn storage_scale_does_not_change_native_loss_or_monetary_size() {
        let economics = standard_lot_economics("100000");
        let low_scale = compute_instrument_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            5,
            &quantity_rules(2),
            &economics,
            Some(1.0),
        )
        .unwrap();
        let high_scale = compute_instrument_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            5,
            &quantity_rules(8),
            &economics,
            Some(1.0),
        )
        .unwrap();

        assert_eq!(low_scale, high_scale);
    }

    #[test]
    fn contract_multiplier_changes_native_loss_and_monetary_size() {
        let standard = compute_instrument_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            5,
            &quantity_rules(2),
            &standard_lot_economics("100000"),
            Some(1.0),
        )
        .unwrap();
        let doubled = compute_instrument_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            5,
            &quantity_rules(2),
            &standard_lot_economics("200000"),
            Some(1.0),
        )
        .unwrap();

        assert_eq!(standard.native_loss_per_lot, Some(500.0));
        assert_eq!(doubled.native_loss_per_lot, Some(1_000.0));
        assert_close(standard.scaled_raw_lot, 0.2);
        assert_close(doubled.scaled_raw_lot, 0.1);
        assert_eq!(standard.final_lot_steps, 20);
        assert_eq!(doubled.final_lot_steps, 10);
    }

    #[test]
    fn catalog_fixed_lot_uses_exact_quantity_grid_and_cap() {
        let mut rules = quantity_rules(4);
        rules.maximum = Some(positive("0.05"));
        let result = compute_instrument_size(
            &SizingPolicy::FixedLot { lots: 0.066 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            None,
            5,
            &rules,
            &standard_lot_economics("100000"),
            None,
        )
        .unwrap();

        assert_eq!(result.final_lot_steps, 5);
        assert_close(result.final_lot, 0.05);
        assert_eq!(result.cap_status, LotCapStatus::CappedAtMaximum);
        assert_eq!(result.native_loss_per_lot, None);
    }

    #[test]
    fn full_spec_sizing_uses_exact_grid_and_records_adjustment() {
        let spec = instrument_spec("0.1", None);
        let result = compute_instrument_size_for_spec(
            &SizingPolicy::FixedLot { lots: 0.3 },
            1.0,
            10_000.0,
            Side::Buy,
            1.1,
            None,
            &spec,
            None,
        )
        .unwrap();

        assert_eq!(result.final_lot_steps, 3);
        assert_eq!(result.final_lot, 0.3);
        assert_eq!(
            result.quantity_adjustment,
            Some(GridAdjustment {
                requested: decimal("0.3"),
                adjusted: decimal("0.3"),
                direction: AdjustmentDirection::Unchanged,
            })
        );
    }

    #[test]
    fn full_spec_sizing_validates_price_grid_and_post_rounding_notional() {
        let usd = AssetId::new("USD").unwrap();
        let minimum_spec = instrument_spec(
            "0.01",
            Some(NotionalRules {
                asset: usd.clone(),
                minimum: Some(positive("2200")),
                maximum: None,
            }),
        );
        let minimum_error = compute_instrument_size_for_spec(
            &SizingPolicy::FixedLot { lots: 0.019 },
            1.0,
            10_000.0,
            Side::Buy,
            1.1,
            None,
            &minimum_spec,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            minimum_error,
            InstrumentSizingError::BelowMinimumNotional { .. }
        ));

        let maximum_spec = instrument_spec(
            "0.01",
            Some(NotionalRules {
                asset: usd,
                minimum: None,
                maximum: Some(positive("1000")),
            }),
        );
        let maximum_error = compute_instrument_size_for_spec(
            &SizingPolicy::FixedLot { lots: 0.02 },
            1.0,
            10_000.0,
            Side::Buy,
            1.1,
            None,
            &maximum_spec,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            maximum_error,
            InstrumentSizingError::AboveMaximumNotional { .. }
        ));

        let mut price_spec = instrument_spec("0.01", None);
        price_spec.price.grid = DecimalGrid::new(Decimal::ZERO, positive("0.00005"));
        let price_error = compute_instrument_size_for_spec(
            &SizingPolicy::FixedLot { lots: 0.02 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10003,
            None,
            &price_spec,
            None,
        )
        .unwrap_err();
        assert!(matches!(
            price_error,
            InstrumentSizingError::PriceOffGrid { .. }
        ));
    }

    #[test]
    fn catalog_sizing_rejects_unsupported_quantity_and_model_combinations() {
        for economics in [
            economics(
                QuantityUnit::Contract,
                EconomicsModelId::FX_QUOTE_LINEAR_V1,
                "100000",
            ),
            economics(
                QuantityUnit::StandardLot,
                EconomicsModelId::LINEAR_CONTRACT_V1,
                "100000",
            ),
        ] {
            let error = compute_instrument_size(
                &SizingPolicy::FixedLot { lots: 0.01 },
                1.0,
                10_000.0,
                Side::Buy,
                1.10000,
                None,
                5,
                &quantity_rules(2),
                &economics,
                None,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                InstrumentSizingError::UnsupportedInstrumentSizing { .. }
            ));
        }
    }

    #[test]
    fn catalog_sizing_rejects_nonzero_quantity_grid_origin() {
        let mut rules = quantity_rules(2);
        rules.grid = DecimalGrid::new(decimal("0.01"), positive("0.01"));
        let error = compute_instrument_size(
            &SizingPolicy::FixedLot { lots: 0.01 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            None,
            5,
            &rules,
            &standard_lot_economics("100000"),
            None,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            InstrumentSizingError::UnsupportedQuantityGridOrigin { .. }
        ));
    }

    #[test]
    fn public_native_loss_helper_matches_compute_size_normalization() {
        let spec = forex_spec();
        let native_loss =
            compute_native_loss_per_lot(Side::Buy, 1.100004, 1.095003, &spec).unwrap();
        let result = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.100004,
            Some(1.095003),
            &spec,
            Some(native_loss),
        )
        .unwrap();

        assert_close(native_loss, 500.0);
        assert_eq!(result.native_loss_per_lot, Some(native_loss));

        let helper_error =
            compute_native_loss_per_lot(Side::Buy, 1.000004, 1.000003, &spec).unwrap_err();
        let sizing_error = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.000004,
            Some(1.000003),
            &spec,
            Some(100.0),
        )
        .unwrap_err();
        assert_eq!(helper_error, sizing_error);
    }

    #[test]
    fn fixed_lot_applies_multiplier_before_lot_step() {
        let result = compute_size(
            &SizingPolicy::FixedLot { lots: 0.006 },
            2.0,
            10_000.0,
            Side::Buy,
            1.10000,
            None,
            &forex_spec(),
            None,
        )
        .unwrap();

        assert_close(result.scaled_raw_lot, 0.012);
        assert_eq!(result.final_lot_steps, 1);
        assert_close(result.final_lot, 0.01);
        assert_eq!(result.requested_account_risk, None);
        assert_eq!(result.native_loss_per_lot, None);
        assert_eq!(result.account_loss_per_lot, None);
        assert_eq!(result.cap_status, LotCapStatus::NotCapped);
    }

    #[test]
    fn invalid_risk_multipliers_are_rejected() {
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let error = compute_size(
                &SizingPolicy::FixedLot { lots: 0.01 },
                value,
                10_000.0,
                Side::Buy,
                1.10000,
                None,
                &forex_spec(),
                None,
            )
            .unwrap_err();

            assert!(matches!(error, SizingError::InvalidRiskMultiplier { .. }));
        }
    }

    #[test]
    fn fixed_lot_may_be_stopless_but_monetary_policies_may_not() {
        let fixed = compute_size(
            &SizingPolicy::FixedLot { lots: 0.01 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            None,
            &forex_spec(),
            None,
        )
        .unwrap();
        assert_eq!(fixed.final_lot_steps, 1);

        for policy in [
            SizingPolicy::FixedRiskAmount { amount: 100.0 },
            SizingPolicy::BalanceRiskPercent { percent: 1.0 },
        ] {
            let error = compute_size(
                &policy,
                1.0,
                10_000.0,
                Side::Buy,
                1.10000,
                None,
                &forex_spec(),
                Some(500.0),
            )
            .unwrap_err();
            assert_eq!(error, SizingError::MissingProtectiveStop);
        }
    }

    #[test]
    fn monetary_policy_requires_positive_account_loss_per_lot() {
        let missing = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            &forex_spec(),
            None,
        )
        .unwrap_err();
        assert_eq!(missing, SizingError::MissingAccountLossPerLot);

        for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let error = compute_size(
                &SizingPolicy::FixedRiskAmount { amount: 100.0 },
                1.0,
                10_000.0,
                Side::Buy,
                1.10000,
                Some(1.09500),
                &forex_spec(),
                Some(value),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                SizingError::InvalidAccountLossPerLot { .. }
            ));
        }
    }

    #[test]
    fn fixed_risk_returns_requested_and_per_lot_audit_values() {
        let result = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            2.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            &forex_spec(),
            Some(500.0),
        )
        .unwrap();

        assert_eq!(result.requested_account_risk, Some(200.0));
        assert_eq!(result.native_loss_per_lot, Some(500.0));
        assert_eq!(result.account_loss_per_lot, Some(500.0));
        assert_close(result.scaled_raw_lot, 0.4);
        assert_eq!(result.final_lot_steps, 40);
        assert_close(result.final_lot, 0.4);
    }

    #[test]
    fn balance_percent_uses_realized_balance_before() {
        let result = compute_size(
            &SizingPolicy::BalanceRiskPercent { percent: 1.0 },
            0.5,
            20_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            &forex_spec(),
            Some(500.0),
        )
        .unwrap();

        assert_eq!(result.requested_account_risk, Some(100.0));
        assert_close(result.scaled_raw_lot, 0.2);
        assert_eq!(result.final_lot_steps, 20);
        assert_close(result.final_lot, 0.2);
    }

    #[test]
    fn all_policies_reject_lots_below_the_minimum() {
        let cases = [
            (SizingPolicy::FixedLot { lots: 0.009 }, None),
            (SizingPolicy::FixedRiskAmount { amount: 4.5 }, Some(500.0)),
            (
                SizingPolicy::BalanceRiskPercent { percent: 0.045 },
                Some(500.0),
            ),
        ];

        for (policy, account_loss_per_lot) in cases {
            let error = compute_size(
                &policy,
                1.0,
                10_000.0,
                Side::Buy,
                1.10000,
                Some(1.09500),
                &forex_spec(),
                account_loss_per_lot,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                SizingError::BelowMinimumLot {
                    floored_lot_steps: 0,
                    minimum_lot_steps: 1,
                    ..
                }
            ));
        }
    }

    #[test]
    fn maximum_cap_preserves_raw_lot_and_reports_audit_status() {
        let mut spec = forex_spec();
        spec.lot_max_steps = 5;

        let result = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.10000,
            Some(1.09500),
            &spec,
            Some(100.0),
        )
        .unwrap();

        assert_close(result.scaled_raw_lot, 1.0);
        assert_eq!(result.requested_account_risk, Some(100.0));
        assert_eq!(result.final_lot_steps, 5);
        assert_close(result.final_lot, 0.05);
        assert_eq!(result.cap_status, LotCapStatus::CappedAtMaximum);
    }

    #[test]
    fn geometry_is_checked_before_sub_tick_distance() {
        let invalid_geometry = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.000003,
            Some(1.000004),
            &forex_spec(),
            Some(100.0),
        )
        .unwrap_err();
        assert!(matches!(
            invalid_geometry,
            SizingError::InvalidStopGeometry {
                side: Side::Buy,
                ..
            }
        ));

        let sub_tick = compute_size(
            &SizingPolicy::FixedRiskAmount { amount: 100.0 },
            1.0,
            10_000.0,
            Side::Buy,
            1.000004,
            Some(1.000003),
            &forex_spec(),
            Some(100.0),
        )
        .unwrap_err();
        assert_eq!(
            sub_tick,
            SizingError::StopDistanceBelowTick {
                entry_price: 1.000004,
                stop_price: 1.000003,
                digits: 5,
            }
        );
    }

    #[test]
    fn fixed_lot_validates_a_supplied_stop_but_does_not_require_one() {
        let error = compute_size(
            &SizingPolicy::FixedLot { lots: 0.01 },
            1.0,
            10_000.0,
            Side::Sell,
            1.10000,
            Some(1.09500),
            &forex_spec(),
            None,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SizingError::InvalidStopGeometry {
                side: Side::Sell,
                ..
            }
        ));
    }
}
