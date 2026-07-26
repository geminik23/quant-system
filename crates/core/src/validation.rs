//! Shared raw-signal validation.
//!
//! `RawSignal` is owned by this crate, so its semantic validation belongs here
//! too. Before this module existed the checks were duplicated: `qs-signal-parser`
//! held the full set behind a crate-private function and `qs-backtest-server`
//! re-implemented a narrower subset with a different error type. Neither was
//! reachable by other consumers, and the two could drift.
//!
//! The engine remains the hard backstop for the invariants it owns (a partial
//! close ratio outside `(0, 1]` is rejected at apply time regardless of what
//! reaches it). This module exists so a caller can reject a bad signal *early*,
//! with a useful reason, on every entry path rather than only on the parser path.
//!
//! # Message stability
//!
//! The `Display` text of [`RawSignalValidationError`] is reproduced verbatim in
//! committed parser outcome goldens through `ParseFailure::InvalidSignal`.
//! Changing a message rewrites those goldens, so treat these strings as part of
//! the observable contract and not as free-form diagnostics.

use crate::profile::RawSignal;
use crate::types::{OrderType, Side};

/// A raw signal that violates the common signal contract.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RawSignalValidationError {
    #[error("entry risk multiplier must be finite and positive, got {value}")]
    EntryRisk { value: f64 },

    #[error("{order_type} entry requires a finite positive price")]
    EntryPriceRequired { order_type: OrderType },

    #[error("entry price must be finite and positive, got {value}")]
    EntryPrice { value: f64 },

    #[error("stoploss is not protective for the entry side")]
    StoplossNotProtective,

    #[error("target is on the wrong side of entry")]
    TargetWrongSide,

    #[error("partial close ratio must be in (0, 1], got {value}")]
    PartialCloseRatio { value: f64 },

    #[error("management price must be finite and positive, got {value}")]
    ManagementPrice { value: f64 },

    #[error("target prices must be finite and positive, got {old_price} -> {new_price}")]
    TargetPricePair { old_price: f64, new_price: f64 },

    #[error("scale-in size/price is invalid")]
    ScaleIn,
}

/// Validate one raw signal against the common signal contract.
///
/// Entry geometry is only checked when the entry price is known, because a
/// market Entry may legitimately omit it; the stop and target side rules have no
/// reference point without it.
pub fn validate_raw_signal(signal: &RawSignal) -> Result<(), RawSignalValidationError> {
    match signal {
        RawSignal::Entry {
            side,
            order_type,
            price,
            risk_multiplier,
            stoploss,
            targets,
            ..
        } => {
            if !risk_multiplier.is_finite() || *risk_multiplier <= 0.0 {
                return Err(RawSignalValidationError::EntryRisk {
                    value: *risk_multiplier,
                });
            }
            if matches!(order_type, OrderType::Limit | OrderType::Stop)
                && !price.is_some_and(|value| value.is_finite() && value > 0.0)
            {
                return Err(RawSignalValidationError::EntryPriceRequired {
                    order_type: *order_type,
                });
            }
            if let Some(entry) = price {
                if !entry.is_finite() || *entry <= 0.0 {
                    return Err(RawSignalValidationError::EntryPrice { value: *entry });
                }
                if let Some(stop) = stoploss {
                    let protective = stop.is_finite()
                        && *stop > 0.0
                        && match side {
                            Side::Buy => *stop < *entry,
                            Side::Sell => *stop > *entry,
                        };
                    if !protective {
                        return Err(RawSignalValidationError::StoplossNotProtective);
                    }
                }
                for target in targets {
                    let valid = target.is_finite()
                        && *target > 0.0
                        && match side {
                            Side::Buy => *target > *entry,
                            Side::Sell => *target < *entry,
                        };
                    if !valid {
                        return Err(RawSignalValidationError::TargetWrongSide);
                    }
                }
            }
        }
        RawSignal::ClosePartial { ratio, .. } => {
            if !ratio.is_finite() || *ratio <= 0.0 || *ratio > 1.0 {
                return Err(RawSignalValidationError::PartialCloseRatio { value: *ratio });
            }
        }
        RawSignal::ModifyStoploss { price, .. }
        | RawSignal::AddTarget { price, .. }
        | RawSignal::RemoveTarget { price, .. }
        | RawSignal::ModifyAllStoploss { price, .. }
        | RawSignal::ModifyAllStoplossInGroup { price, .. } => {
            if !price.is_finite() || *price <= 0.0 {
                return Err(RawSignalValidationError::ManagementPrice { value: *price });
            }
        }
        RawSignal::ModifyTarget {
            old_price,
            new_price,
            ..
        } => {
            if !old_price.is_finite()
                || *old_price <= 0.0
                || !new_price.is_finite()
                || *new_price <= 0.0
            {
                return Err(RawSignalValidationError::TargetPricePair {
                    old_price: *old_price,
                    new_price: *new_price,
                });
            }
        }
        RawSignal::ScaleIn { size, price, .. } => {
            if !size.is_finite()
                || *size <= 0.0
                || price.is_some_and(|value| !value.is_finite() || value <= 0.0)
            {
                return Err(RawSignalValidationError::ScaleIn);
            }
        }
        RawSignal::Close { .. }
        | RawSignal::MoveStoplossToEntry { .. }
        | RawSignal::AddRule { .. }
        | RawSignal::RemoveRule { .. }
        | RawSignal::CancelPending { .. }
        | RawSignal::CloseAllOf { .. }
        | RawSignal::CloseAll { .. }
        | RawSignal::CancelAllPending { .. }
        | RawSignal::CloseAllInGroup { .. } => {}
    }
    Ok(())
}

/// Validate a batch, returning the first violation in slice order.
pub fn validate_raw_signals(signals: &[RawSignal]) -> Result<(), RawSignalValidationError> {
    signals.iter().try_for_each(validate_raw_signal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::PositionRef;
    use chrono::NaiveDate;

    fn ts() -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
    }

    fn entry(
        side: Side,
        order_type: OrderType,
        price: Option<f64>,
        risk: f64,
        stoploss: Option<f64>,
        targets: Vec<f64>,
    ) -> RawSignal {
        RawSignal::Entry {
            ts: ts(),
            symbol: "xauusd".into(),
            side,
            order_type,
            price,
            risk_multiplier: risk,
            stoploss,
            targets,
            group: None,
            trade_id: None,
        }
    }

    fn any_position() -> PositionRef {
        PositionRef::AllOnSymbol {
            symbol: "xauusd".into(),
        }
    }

    #[test]
    fn valid_market_entry_passes() {
        let signal = entry(
            Side::Buy,
            OrderType::Market,
            Some(2000.0),
            1.0,
            Some(1990.0),
            vec![2010.0, 2020.0],
        );
        assert_eq!(validate_raw_signal(&signal), Ok(()));
    }

    #[test]
    fn market_entry_without_price_skips_geometry() {
        // A market Entry may omit price; stop/target side rules need a reference
        // point, so they must not fire.
        let signal = entry(
            Side::Buy,
            OrderType::Market,
            None,
            1.0,
            Some(9999.0),
            vec![1.0],
        );
        assert_eq!(validate_raw_signal(&signal), Ok(()));
    }

    #[test]
    fn non_finite_and_non_positive_risk_are_rejected() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let signal = entry(Side::Buy, OrderType::Market, None, bad, None, vec![]);
            assert!(matches!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::EntryRisk { .. })
            ));
        }
    }

    #[test]
    fn limit_and_stop_entries_require_a_price() {
        for order_type in [OrderType::Limit, OrderType::Stop] {
            let signal = entry(Side::Buy, order_type, None, 1.0, None, vec![]);
            assert!(matches!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::EntryPriceRequired { .. })
            ));
        }
    }

    #[test]
    fn non_positive_entry_price_is_rejected() {
        let signal = entry(Side::Buy, OrderType::Market, Some(0.0), 1.0, None, vec![]);
        assert!(matches!(
            validate_raw_signal(&signal),
            Err(RawSignalValidationError::EntryPrice { .. })
        ));
    }

    #[test]
    fn stoploss_must_be_protective_for_each_side() {
        let buy = entry(
            Side::Buy,
            OrderType::Market,
            Some(2000.0),
            1.0,
            Some(2010.0),
            vec![],
        );
        let sell = entry(
            Side::Sell,
            OrderType::Market,
            Some(2000.0),
            1.0,
            Some(1990.0),
            vec![],
        );
        for signal in [buy, sell] {
            assert_eq!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::StoplossNotProtective)
            );
        }
    }

    #[test]
    fn targets_must_be_on_the_profitable_side() {
        let buy = entry(
            Side::Buy,
            OrderType::Market,
            Some(2000.0),
            1.0,
            None,
            vec![1990.0],
        );
        let sell = entry(
            Side::Sell,
            OrderType::Market,
            Some(2000.0),
            1.0,
            None,
            vec![2010.0],
        );
        for signal in [buy, sell] {
            assert_eq!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::TargetWrongSide)
            );
        }
    }

    #[test]
    fn partial_close_ratio_bounds_are_inclusive_at_one() {
        let ok = RawSignal::ClosePartial {
            ts: ts(),
            position: any_position(),
            ratio: 1.0,
        };
        assert_eq!(validate_raw_signal(&ok), Ok(()));

        for bad in [0.0, -0.5, 1.000_001, f64::NAN] {
            let signal = RawSignal::ClosePartial {
                ts: ts(),
                position: any_position(),
                ratio: bad,
            };
            assert!(matches!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::PartialCloseRatio { .. })
            ));
        }
    }

    #[test]
    fn management_prices_must_be_finite_positive() {
        let signal = RawSignal::ModifyStoploss {
            ts: ts(),
            position: any_position(),
            price: -1.0,
        };
        assert!(matches!(
            validate_raw_signal(&signal),
            Err(RawSignalValidationError::ManagementPrice { .. })
        ));
    }

    #[test]
    fn modify_target_rejects_either_bad_price() {
        for (old, new) in [(0.0, 2010.0), (2000.0, f64::NAN)] {
            let signal = RawSignal::ModifyTarget {
                ts: ts(),
                position: any_position(),
                old_price: old,
                new_price: new,
            };
            assert!(matches!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::TargetPricePair { .. })
            ));
        }
    }

    #[test]
    fn scale_in_rejects_bad_size_or_price_but_allows_absent_price() {
        let ok = RawSignal::ScaleIn {
            ts: ts(),
            position: any_position(),
            price: None,
            size: 0.5,
        };
        assert_eq!(validate_raw_signal(&ok), Ok(()));

        let bad_size = RawSignal::ScaleIn {
            ts: ts(),
            position: any_position(),
            price: None,
            size: 0.0,
        };
        let bad_price = RawSignal::ScaleIn {
            ts: ts(),
            position: any_position(),
            price: Some(-1.0),
            size: 0.5,
        };
        for signal in [bad_size, bad_price] {
            assert_eq!(
                validate_raw_signal(&signal),
                Err(RawSignalValidationError::ScaleIn)
            );
        }
    }

    #[test]
    fn variants_without_numeric_payload_always_pass() {
        let signals = vec![
            RawSignal::Close {
                ts: ts(),
                position: any_position(),
            },
            RawSignal::MoveStoplossToEntry {
                ts: ts(),
                position: any_position(),
            },
            RawSignal::CancelPending {
                ts: ts(),
                position: any_position(),
            },
            RawSignal::CloseAll { ts: ts() },
            RawSignal::CancelAllPending { ts: ts() },
        ];
        assert_eq!(validate_raw_signals(&signals), Ok(()));
    }

    #[test]
    fn batch_reports_the_first_violation_in_slice_order() {
        let signals = vec![
            entry(
                Side::Buy,
                OrderType::Market,
                Some(2000.0),
                1.0,
                None,
                vec![],
            ),
            RawSignal::ClosePartial {
                ts: ts(),
                position: any_position(),
                ratio: 2.0,
            },
            RawSignal::ModifyStoploss {
                ts: ts(),
                position: any_position(),
                price: -1.0,
            },
        ];
        assert!(matches!(
            validate_raw_signals(&signals),
            Err(RawSignalValidationError::PartialCloseRatio { .. })
        ));
    }

    #[test]
    fn messages_match_the_strings_embedded_in_parser_goldens() {
        // These exact strings appear in committed outcome goldens through
        // ParseFailure::InvalidSignal. Changing them rewrites those files.
        assert_eq!(
            RawSignalValidationError::TargetWrongSide.to_string(),
            "target is on the wrong side of entry"
        );
        assert_eq!(
            RawSignalValidationError::StoplossNotProtective.to_string(),
            "stoploss is not protective for the entry side"
        );
        assert_eq!(
            RawSignalValidationError::EntryRisk { value: 0.0 }.to_string(),
            "entry risk multiplier must be finite and positive, got 0"
        );
        assert_eq!(
            RawSignalValidationError::PartialCloseRatio { value: 2.0 }.to_string(),
            "partial close ratio must be in (0, 1], got 2"
        );
        assert_eq!(
            RawSignalValidationError::EntryPriceRequired {
                order_type: OrderType::Limit
            }
            .to_string(),
            "Limit entry requires a finite positive price"
        );
        assert_eq!(
            RawSignalValidationError::ScaleIn.to_string(),
            "scale-in size/price is invalid"
        );
        assert_eq!(
            RawSignalValidationError::ManagementPrice { value: -1.0 }.to_string(),
            "management price must be finite and positive, got -1"
        );
        assert_eq!(
            RawSignalValidationError::TargetPricePair {
                old_price: 1.0,
                new_price: 2.0
            }
            .to_string(),
            "target prices must be finite and positive, got 1 -> 2"
        );
    }
}
