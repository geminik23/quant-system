//! Immutable server-side planning for raw-signal replay.

use std::collections::BTreeSet;

use chrono::{Duration, NaiveDateTime};
use qs_backtest::profile::RawSignal;

use crate::error::{BacktestServerError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestedSymbolScope {
    Explicit(Vec<String>),
    Inferred,
}

impl RequestedSymbolScope {
    pub(crate) fn explicit(symbols: impl IntoIterator<Item = String>) -> Self {
        Self::Explicit(
            symbols
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    }

    pub(crate) fn default_symbol(&self) -> &str {
        match self {
            Self::Explicit(symbols) if symbols.len() == 1 => &symbols[0],
            Self::Explicit(_) | Self::Inferred => "",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReplayPlan {
    retained_signals: Vec<RawSignal>,
    requested_symbols: Vec<String>,
    active_symbols: Vec<String>,
    idle_explicit_symbols: Vec<String>,
    loading_start: Option<NaiveDateTime>,
}

impl ReplayPlan {
    pub(crate) fn build(
        scope: RequestedSymbolScope,
        signals: Vec<RawSignal>,
        requested_from: Option<NaiveDateTime>,
        requested_to: Option<NaiveDateTime>,
        signal_latency_ms: i64,
    ) -> Result<Self> {
        if signal_latency_ms < 0 {
            return Err(BacktestServerError::InvalidRequest(format!(
                "signal_latency_ms must be non-negative, got {signal_latency_ms}"
            )));
        }

        let retained_signals = signals
            .into_iter()
            .filter(|signal| {
                let ts = signal.ts();
                requested_from.is_none_or(|from| ts >= from)
                    && requested_to.is_none_or(|to| ts <= to)
            })
            .collect::<Vec<_>>();
        let active_symbols = retained_signals
            .iter()
            .filter_map(|signal| match signal {
                RawSignal::Entry { symbol, .. } => Some(symbol.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        if active_symbols.contains("") {
            return Err(BacktestServerError::InvalidRequest(
                "Entry signal symbol is required when it cannot use an explicit single-symbol default"
                    .into(),
            ));
        }

        let latency = Duration::milliseconds(signal_latency_ms);
        let loading_start = retained_signals
            .iter()
            .map(|signal| {
                signal.ts().checked_add_signed(latency).ok_or_else(|| {
                    BacktestServerError::InvalidRequest(format!(
                        "signal latency overflows datetime for signal at {}",
                        signal.ts()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .min();

        let active_symbols = active_symbols.into_iter().collect::<Vec<_>>();
        let (requested_symbols, idle_explicit_symbols) = match scope {
            RequestedSymbolScope::Explicit(requested_symbols) => {
                let requested = requested_symbols
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                if let Some(symbol) = active_symbols
                    .iter()
                    .find(|symbol| !requested.contains(symbol.as_str()))
                {
                    return Err(BacktestServerError::InvalidRequest(format!(
                        "Entry signal symbol '{symbol}' is not included in requested market-data symbols: {}",
                        requested_symbols.join(",")
                    )));
                }
                let active = active_symbols
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                let idle = requested_symbols
                    .iter()
                    .filter(|symbol| !active.contains(symbol.as_str()))
                    .cloned()
                    .collect();
                (requested_symbols, idle)
            }
            RequestedSymbolScope::Inferred => (active_symbols.clone(), Vec::new()),
        };

        Ok(Self {
            retained_signals,
            requested_symbols,
            active_symbols,
            idle_explicit_symbols,
            loading_start,
        })
    }

    pub(crate) fn retained_signals(&self) -> &[RawSignal] {
        &self.retained_signals
    }

    pub(crate) fn requested_symbols(&self) -> &[String] {
        &self.requested_symbols
    }

    pub(crate) fn active_symbols(&self) -> &[String] {
        &self.active_symbols
    }

    pub(crate) fn idle_explicit_symbols(&self) -> &[String] {
        &self.idle_explicit_symbols
    }

    pub(crate) fn loading_start(&self) -> Option<NaiveDateTime> {
        self.loading_start
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.active_symbols.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_core::types::{OrderType, Side};

    fn ts(second: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(10, 0, second)
            .unwrap()
    }

    fn entry(timestamp: NaiveDateTime, symbol: &str) -> RawSignal {
        RawSignal::Entry {
            ts: timestamp,
            symbol: symbol.into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: Vec::new(),
            group: None,
            trade_id: None,
        }
    }

    #[test]
    fn derives_active_and_idle_symbols_from_retained_entries() {
        let plan = ReplayPlan::build(
            RequestedSymbolScope::explicit(["gbpjpy".into(), "xauusd".into()]),
            vec![entry(ts(1), "xauusd")],
            None,
            None,
            500,
        )
        .unwrap();

        assert_eq!(plan.requested_symbols(), ["gbpjpy", "xauusd"]);
        assert_eq!(plan.active_symbols(), ["xauusd"]);
        assert_eq!(plan.idle_explicit_symbols(), ["gbpjpy"]);
        assert_eq!(
            plan.loading_start(),
            Some(ts(1) + Duration::milliseconds(500))
        );
    }

    #[test]
    fn filtered_entries_do_not_activate_or_fail_scope_validation() {
        let plan = ReplayPlan::build(
            RequestedSymbolScope::explicit(["xauusd".into()]),
            vec![entry(ts(0), "gbpjpy"), entry(ts(1), "xauusd")],
            Some(ts(1)),
            None,
            0,
        )
        .unwrap();

        assert_eq!(plan.retained_signals().len(), 1);
        assert_eq!(plan.active_symbols(), ["xauusd"]);
        assert!(plan.idle_explicit_symbols().is_empty());
        assert_eq!(plan.loading_start(), Some(ts(1)));
    }

    #[test]
    fn retained_entry_must_be_in_explicit_scope() {
        let error = ReplayPlan::build(
            RequestedSymbolScope::explicit(["xauusd".into()]),
            vec![entry(ts(0), "gbpjpy")],
            None,
            None,
            0,
        )
        .unwrap_err();

        assert!(error.to_string().contains("not included"));
    }

    #[test]
    fn inclusive_requested_bounds_are_preserved() {
        let plan = ReplayPlan::build(
            RequestedSymbolScope::Inferred,
            vec![entry(ts(0), "xauusd"), entry(ts(1), "xauusd")],
            Some(ts(0)),
            Some(ts(1)),
            0,
        )
        .unwrap();

        assert_eq!(plan.retained_signals().len(), 2);
    }
}
