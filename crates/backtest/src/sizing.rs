//! Position sizing policies for backtests.
//!
//! Provides policy-based lot sizing compatible with the legacy autotrader
//! model.  Policies are resolved after profile transformation, using the
//! final `Action::Open` entry price and stoploss.
//!
//! # Policies
//!
//! - [`SizingPolicy::FixedLot`] — use configured lot directly.
//! - [`SizingPolicy::FixedValueLot`] — normalize lot by symbol pip value.
//! - [`SizingPolicy::RRLot`] — scale base lot by configured/actual pip ratio.
//! - [`SizingPolicy::RRValue`] — compute lot from fixed money risk.

use std::collections::HashMap;

use qs_core::types::Side;
use qs_symbols::SymbolSpec;

/// Policy for computing position size from a signal.
#[derive(Debug, Clone)]
pub enum SizingPolicy {
    /// Use the configured lot directly.  The `qty` string is a pipe-delimited
    /// map: `all=0.01|xauusd=0.03`.  Exact symbol match wins, then `all`,
    /// then fallback `0.01`.
    FixedLot {
        /// Pipe-delimited symbol-to-lot map.
        qty: String,
    },
    /// Normalize the configured lot by symbol pip value so that exposure
    /// is comparable across instruments.
    FixedValueLot {
        /// Pipe-delimited symbol-to-lot map.
        qty: String,
    },
    /// Scale the base lot by `configured_pips / actual_stop_pips`.
    RRLot {
        /// Pipe-delimited symbol-to-lot map.
        qty: String,
        /// The reference stop distance in pips.
        pips: f64,
    },
    /// Compute lot from a fixed money-risk amount.
    /// `lot = risk / (pip_value * stop_pips)`, rounded to lot step.
    RRValue {
        /// Pipe-delimited symbol-to-lot map (used for fallback/zero).
        qty: String,
        /// Target risk amount in account currency.
        value: f64,
    },
}

impl SizingPolicy {
    /// Get the qty string from any variant.
    pub fn qty(&self) -> &str {
        match self {
            SizingPolicy::FixedLot { qty }
            | SizingPolicy::FixedValueLot { qty }
            | SizingPolicy::RRLot { qty, .. }
            | SizingPolicy::RRValue { qty, .. } => qty,
        }
    }
}

/// Result of a sizing calculation.
#[derive(Debug, Clone, PartialEq)]
pub struct SizingResult {
    /// Final lot size after policy application and rounding.
    pub size: f64,
    /// Whether sizing was skipped (e.g. zero lot or FixedLot with no stop).
    pub skipped: bool,
    /// Error message when sizing cannot be computed in risk mode.
    pub error: Option<String>,
}

/// Parse a pipe-delimited qty map like `all=0.01|xauusd=0.03`.
///
/// Returns a map from symbol to lot.  Invalid entries default to `0.01`.
pub fn parse_qty_map(qty: &str) -> HashMap<String, f64> {
    qty.split('|')
        .filter(|v| !v.is_empty())
        .map(|v| {
            let v = v.trim();
            let parts: Vec<&str> = v.split('=').collect();
            if parts.len() != 2 {
                (parts[0].to_string(), 0.01f64)
            } else {
                let lot = parts[1].parse::<f64>().unwrap_or(0.01f64);
                (parts[0].to_string(), lot)
            }
        })
        .collect()
}

/// Look up the base lot for a symbol from a qty map.
///
/// Exact symbol match wins, then `all`, then fallback `0.01`.
pub fn lookup_base_lot(map: &HashMap<String, f64>, symbol: &str) -> f64 {
    map.get(symbol)
        .copied()
        .or_else(|| map.get("all").copied())
        .unwrap_or(0.01)
}

/// Calculate pip distance between entry and stoploss.
pub fn pip_distance(entry_price: f64, stoploss: f64, spec: &SymbolSpec) -> f64 {
    spec.to_pips(entry_price, stoploss).abs()
}

/// Round a lot to the nearest allowed lot step.
///
/// Uses `floor` to avoid exceeding risk.  Clamps to minimum lot.
/// Clamps to maximum lot when `lot_max_steps > 0`.
pub fn round_to_lot_step(lot: f64, spec: &SymbolSpec) -> f64 {
    let step = spec.lot_step();
    let min_lot = spec.lot_min();
    let max_lot = spec.lot_max();
    if step <= 0.0 {
        return lot.max(min_lot).min(max_lot);
    }
    let rounded = (lot / step).floor() * step;
    let clamped = if rounded < min_lot {
        min_lot
    } else if max_lot > 0.0 && rounded > max_lot {
        max_lot
    } else {
        rounded
    };
    clamped
}

/// Estimate the USD pip value for 0.01 lot of a symbol.
pub fn pip_value_per_001_lot(
    symbol: &str,
    spec: &SymbolSpec,
    conversion: &HashMap<String, f64>,
) -> f64 {
    let base = match spec.category.as_str() {
        "metal" if symbol.contains("xau") => 0.01,
        "metal" if symbol.contains("xag") => 0.5,
        "crypto" => 0.001,
        "index" => 0.001,
        "commodity" => 0.1,
        _ => 0.1,
    };

    if spec.category != "forex" {
        return base;
    }

    let quote = &symbol[3..];
    if quote == "usd" {
        return base;
    }

    let conversion_rate = conversion.get(quote).copied().unwrap_or(1.0);
    base * conversion_rate
}

/// Compute the position size for a given entry signal.
pub fn compute_size(
    policy: &SizingPolicy,
    symbol: &str,
    _side: Side,
    entry_price: Option<f64>,
    stoploss: Option<f64>,
    spec: &SymbolSpec,
    conversion: &HashMap<String, f64>,
) -> SizingResult {
    let map = parse_qty_map(policy.qty());
    let base_lot = lookup_base_lot(&map, symbol);

    if base_lot == 0.0 {
        return SizingResult {
            size: 0.0,
            skipped: true,
            error: None,
        };
    }

    if matches!(policy, SizingPolicy::FixedLot { .. }) {
        return SizingResult {
            size: base_lot,
            skipped: false,
            error: None,
        };
    }

    let is_risk_policy = matches!(
        policy,
        SizingPolicy::RRLot { .. } | SizingPolicy::RRValue { .. }
    );

    let (entry, sl) = match (entry_price, stoploss) {
        (Some(e), Some(s)) => (e, s),
        _ => {
            if is_risk_policy {
                return SizingResult {
                    size: 0.0,
                    skipped: true,
                    error: Some("Risk sizing requires both entry price and stoploss".into()),
                };
            }
            return SizingResult {
                size: base_lot,
                skipped: false,
                error: None,
            };
        }
    };

    let stop_pips = pip_distance(entry, sl, spec);

    if is_risk_policy && stop_pips == 0.0 {
        return SizingResult {
            size: 0.0,
            skipped: true,
            error: Some("Stop distance is zero - cannot compute risk sizing".into()),
        };
    }

    let raw_lot = match policy {
        SizingPolicy::FixedLot { .. } => base_lot,
        SizingPolicy::FixedValueLot { .. } => {
            let pip_val = pip_value_per_001_lot(symbol, spec, conversion);
            if pip_val <= 0.0 {
                base_lot
            } else {
                (0.1 / pip_val) * base_lot
            }
        }
        SizingPolicy::RRLot { pips, .. } => {
            if *pips <= 0.0 {
                return SizingResult {
                    size: 0.0,
                    skipped: true,
                    error: Some("RRLot policy pips must be positive".into()),
                };
            }
            base_lot * (*pips / stop_pips)
        }
        SizingPolicy::RRValue { value, .. } => {
            if *value <= 0.0 {
                return SizingResult {
                    size: 0.0,
                    skipped: true,
                    error: Some("RRValue must be positive".into()),
                };
            }
            let pip_val = pip_value_per_001_lot(symbol, spec, conversion);
            if pip_val <= 0.0 {
                return SizingResult {
                    size: 0.0,
                    skipped: true,
                    error: Some("Cannot determine pip value for symbol".into()),
                };
            }
            *value / (pip_val * stop_pips)
        }
    };

    if raw_lot <= 0.0 {
        return SizingResult {
            size: 0.0,
            skipped: true,
            error: None,
        };
    }

    let rounded = round_to_lot_step(raw_lot, spec);
    SizingResult {
        size: rounded,
        skipped: false,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eurusd_spec() -> SymbolSpec {
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

    fn xauusd_spec() -> SymbolSpec {
        SymbolSpec {
            canonical: "xauusd".into(),
            pip_position: 1,
            digits: 2,
            category: "metal".into(),
            lot_base_units: 100,
            lot_step_units: 1,
            lot_min_steps: 1,
            lot_max_steps: 0,
        }
    }

    #[test]
    fn parse_qty_map_basic() {
        let map = parse_qty_map("all=0.01|xauusd=0.03");
        assert_eq!(map.get("all"), Some(&0.01));
        assert_eq!(map.get("xauusd"), Some(&0.03));
    }

    #[test]
    fn parse_qty_map_invalid_defaults() {
        let map = parse_qty_map("all=bad");
        assert_eq!(map.get("all"), Some(&0.01));
    }

    #[test]
    fn lookup_base_lot_symbol_override() {
        let map = parse_qty_map("all=0.01|xauusd=0.03");
        assert_eq!(lookup_base_lot(&map, "xauusd"), 0.03);
        assert_eq!(lookup_base_lot(&map, "eurusd"), 0.01);
        assert_eq!(lookup_base_lot(&map, "gbpjpy"), 0.01);
    }

    #[test]
    fn fixed_lot_returns_base() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::FixedLot {
            qty: "all=0.05".into(),
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            Some(1.0850),
            Some(1.0800),
            &spec,
            &HashMap::new(),
        );
        assert!(!result.skipped);
        assert!((result.size - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn fixed_lot_zero_skips() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::FixedLot {
            qty: "all=0.0".into(),
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            Some(1.0850),
            Some(1.0800),
            &spec,
            &HashMap::new(),
        );
        assert!(result.skipped);
    }

    #[test]
    fn rr_value_xauusd() {
        let spec = xauusd_spec();
        let policy = SizingPolicy::RRValue {
            qty: "all=0.01".into(),
            value: 100.0,
        };
        let result = compute_size(
            &policy,
            "xauusd",
            Side::Buy,
            Some(4411.50),
            Some(4398.50),
            &spec,
            &HashMap::new(),
        );
        assert!(!result.skipped);
        assert!(result.size > 0.0);
    }

    #[test]
    fn rr_lot_scales_by_pip_ratio() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::RRLot {
            qty: "all=0.02".into(),
            pips: 20.0,
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            Some(1.0850),
            Some(1.0800),
            &spec,
            &HashMap::new(),
        );
        assert!(!result.skipped);
        assert!((result.size - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn missing_stoploss_rejects_in_risk_mode() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::RRLot {
            qty: "all=0.02".into(),
            pips: 20.0,
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            Some(1.0850),
            None,
            &spec,
            &HashMap::new(),
        );
        assert!(result.skipped);
        assert!(result.error.is_some());
    }

    #[test]
    fn missing_entry_price_rejects_in_rrvalue() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::RRValue {
            qty: "all=0.01".into(),
            value: 100.0,
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            None,
            Some(1.0800),
            &spec,
            &HashMap::new(),
        );
        assert!(result.skipped);
        assert!(result.error.is_some());
    }

    #[test]
    fn zero_stop_distance_rejects_in_risk_mode() {
        let spec = eurusd_spec();
        let policy = SizingPolicy::RRValue {
            qty: "all=0.01".into(),
            value: 100.0,
        };
        let result = compute_size(
            &policy,
            "eurusd",
            Side::Buy,
            Some(1.0850),
            Some(1.0850),
            &spec,
            &HashMap::new(),
        );
        assert!(result.skipped);
        assert!(result.error.is_some());
    }

    #[test]
    fn round_to_lot_step_floors() {
        let spec = eurusd_spec();
        assert!((round_to_lot_step(0.035, &spec) - 0.03).abs() < f64::EPSILON);
        assert!((round_to_lot_step(0.019, &spec) - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn lot_max_steps_enforced() {
        let spec = SymbolSpec {
            canonical: "test".into(),
            pip_position: 4,
            digits: 5,
            category: "forex".into(),
            lot_base_units: 100_000,
            lot_step_units: 1_000,
            lot_min_steps: 1,
            lot_max_steps: 50,
        };
        // lot_max_steps=50 => max_lot = 50 * 0.01 = 0.5
        assert!((round_to_lot_step(100.0, &spec) - 0.5).abs() < f64::EPSILON);
        // 0.3 is within min/max range, stays as 0.3
        assert!((round_to_lot_step(0.3, &spec) - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn pip_distance_forex() {
        let spec = eurusd_spec();
        let dist = pip_distance(1.0850, 1.0800, &spec);
        assert!((dist - 50.0).abs() < 0.001);
    }
}
