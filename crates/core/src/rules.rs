//! Composable position management rules.
//!
//! Each [`Rule`] variant carries both its configuration **and** its mutable
//! runtime state (e.g. whether it has already triggered).  Rules are evaluated
//! on every price tick for open positions and produce [`Effect`]s that the
//! engine applies.

use serde::{Deserialize, Serialize};

use crate::types::{CloseReason, Effect, FillModel, PositionStatus, PriceQuote, RuleConfig, Side};

// ─── PositionData view (passed to rules for evaluation) ─────────────────────

/// Read-only snapshot of the position fields that rules need for evaluation.
///
/// This is a subset of `Position` that avoids borrow-checker conflicts when
/// rules (stored inside `Position`) need to inspect the position's data.
pub struct PositionView<'a> {
    pub id: &'a str,
    pub symbol: &'a str,
    pub side: Side,
    pub status: PositionStatus,
    pub average_entry: f64,
    pub remaining_ratio: f64,
    pub target_hits: u32,
    pub open_ts: Option<chrono::NaiveDateTime>,
}

// ─── Rule enum ──────────────────────────────────────────────────────────────

/// A composable management rule that is evaluated on every price update.
///
/// Rules are stored per-position and carry their own mutable state so that
/// trigger-once semantics (e.g. breakeven) work correctly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Rule {
    /// Close the full remaining position when price crosses the stoploss level.
    FixedStoploss { price: f64 },

    /// Stoploss that follows the price at a fixed `distance`.
    ///
    /// For a **Buy** position the stop trails below the highest bid seen;
    /// for a **Sell** position it trails above the lowest ask seen.
    TrailingStop {
        distance: f64,
        #[serde(default)]
        peak_price: f64,
        #[serde(default)]
        initialized: bool,
    },

    /// Close `close_ratio` of the position when price reaches `price`.
    /// Fires at most once.
    TakeProfit {
        price: f64,
        close_ratio: f64,
        #[serde(default)]
        triggered: bool,
    },

    /// When price reaches `trigger_price`, move the stoploss to the entry
    /// price (breakeven).  Fires at most once.
    BreakevenWhen {
        trigger_price: f64,
        #[serde(default)]
        triggered: bool,
    },

    /// After `after_n` take-profit hits, move the stoploss to entry.
    /// Fires at most once.
    BreakevenAfterTargets {
        after_n: u32,
        #[serde(default)]
        triggered: bool,
    },

    /// Close the position if it has been open longer than `max_seconds`.
    TimeExit { max_seconds: u64 },
}

// ─── Constructors ───────────────────────────────────────────────────────────

impl Rule {
    pub fn fixed_stoploss(price: f64) -> Self {
        Rule::FixedStoploss { price }
    }

    pub fn trailing_stop(distance: f64) -> Self {
        Rule::TrailingStop {
            distance,
            peak_price: 0.0,
            initialized: false,
        }
    }

    pub fn take_profit(price: f64, close_ratio: f64) -> Self {
        Rule::TakeProfit {
            price,
            close_ratio,
            triggered: false,
        }
    }

    pub fn breakeven_when(trigger_price: f64) -> Self {
        Rule::BreakevenWhen {
            trigger_price,
            triggered: false,
        }
    }

    pub fn breakeven_after_targets(after_n: u32) -> Self {
        Rule::BreakevenAfterTargets {
            after_n,
            triggered: false,
        }
    }

    pub fn time_exit(max_seconds: u64) -> Self {
        Rule::TimeExit { max_seconds }
    }

    /// Build a live `Rule` from a declarative [`RuleConfig`].
    pub fn from_config(config: RuleConfig) -> Self {
        match config {
            RuleConfig::FixedStoploss { price } => Self::fixed_stoploss(price),
            RuleConfig::TrailingStop { distance } => Self::trailing_stop(distance),
            RuleConfig::TakeProfit { price, close_ratio } => Self::take_profit(price, close_ratio),
            RuleConfig::BreakevenWhen { trigger_price } => Self::breakeven_when(trigger_price),
            RuleConfig::BreakevenAfterTargets { after_n } => Self::breakeven_after_targets(after_n),
            RuleConfig::TimeExit { max_seconds } => Self::time_exit(max_seconds),
        }
    }
}

// ─── Evaluation ─────────────────────────────────────────────────────────────

impl Rule {
    /// Whether this rule requires tick-by-tick evaluation (not indexable as a static alert).
    pub fn is_stateful(&self) -> bool {
        matches!(
            self,
            Rule::TrailingStop { .. } | Rule::TimeExit { .. } | Rule::BreakevenAfterTargets { .. }
        )
    }

    /// Human-readable name used for logging, record keeping, and `RemoveRule`.
    pub fn name(&self) -> &'static str {
        match self {
            Rule::FixedStoploss { .. } => "FixedStoploss",
            Rule::TrailingStop { .. } => "TrailingStop",
            Rule::TakeProfit { .. } => "TakeProfit",
            Rule::BreakevenWhen { .. } => "BreakevenWhen",
            Rule::BreakevenAfterTargets { .. } => "BreakevenAfterTargets",
            Rule::TimeExit { .. } => "TimeExit",
        }
    }

    /// Evaluate the rule against the current quote and position state.
    ///
    /// Returns zero or more [`Effect`]s.  The engine is responsible for
    /// applying these effects to the position afterwards.
    pub fn evaluate(
        &mut self,
        data: &PositionView<'_>,
        quote: &PriceQuote,
        model: FillModel,
    ) -> Vec<Effect> {
        // Only evaluate for open positions.
        if data.status != PositionStatus::Open {
            return vec![];
        }

        match self {
            // ── Fixed stoploss ──────────────────────────────────────────
            Rule::FixedStoploss { price } => {
                let check = quote.eval_price(data.side, model);
                let hit = match data.side {
                    Side::Buy => check <= *price,
                    Side::Sell => check >= *price,
                };
                if hit {
                    vec![Effect::PositionClosed {
                        id: data.id.to_owned(),
                        reason: CloseReason::Stoploss,
                    }]
                } else {
                    vec![]
                }
            }

            // ── Trailing stop ───────────────────────────────────────────
            Rule::TrailingStop {
                distance,
                peak_price,
                initialized,
            } => {
                if !*initialized {
                    *peak_price = data.average_entry;
                    *initialized = true;
                }

                let check = quote.eval_price(data.side, model);

                match data.side {
                    Side::Buy => {
                        if check > *peak_price {
                            *peak_price = check;
                        }
                        let trailing_sl = *peak_price - *distance;
                        if check <= trailing_sl {
                            vec![Effect::PositionClosed {
                                id: data.id.to_owned(),
                                reason: CloseReason::TrailingStop,
                            }]
                        } else {
                            vec![]
                        }
                    }
                    Side::Sell => {
                        if check < *peak_price {
                            *peak_price = check;
                        }
                        let trailing_sl = *peak_price + *distance;
                        if check >= trailing_sl {
                            vec![Effect::PositionClosed {
                                id: data.id.to_owned(),
                                reason: CloseReason::TrailingStop,
                            }]
                        } else {
                            vec![]
                        }
                    }
                }
            }

            // ── Take profit ─────────────────────────────────────────────
            Rule::TakeProfit {
                price,
                close_ratio,
                triggered,
            } => {
                if *triggered {
                    return vec![];
                }
                if data.remaining_ratio <= 0.0 {
                    return vec![];
                }

                let check = quote.eval_price(data.side, model);
                let hit = match data.side {
                    Side::Buy => check >= *price,
                    Side::Sell => check <= *price,
                };

                if hit {
                    *triggered = true;
                    let actual_ratio = close_ratio.min(data.remaining_ratio);

                    // If this closes the remaining position, emit CloseFull.
                    if (data.remaining_ratio - actual_ratio).abs() < f64::EPSILON {
                        vec![Effect::PositionClosed {
                            id: data.id.to_owned(),
                            reason: CloseReason::Target,
                        }]
                    } else {
                        vec![Effect::PartialClose {
                            id: data.id.to_owned(),
                            ratio: actual_ratio,
                            reason: CloseReason::Target,
                        }]
                    }
                } else {
                    vec![]
                }
            }

            // ── Breakeven when price reached ────────────────────────────
            Rule::BreakevenWhen {
                trigger_price,
                triggered,
            } => {
                if *triggered {
                    return vec![];
                }

                let check = quote.eval_price(data.side, model);
                let hit = match data.side {
                    Side::Buy => check >= *trigger_price,
                    Side::Sell => check <= *trigger_price,
                };

                if hit {
                    *triggered = true;
                    vec![Effect::StoplossModified {
                        id: data.id.to_owned(),
                        old_price: 0.0, // filled in by engine
                        new_price: data.average_entry,
                    }]
                } else {
                    vec![]
                }
            }

            // ── Breakeven after N target hits ───────────────────────────
            Rule::BreakevenAfterTargets { after_n, triggered } => {
                if *triggered {
                    return vec![];
                }

                if data.target_hits >= *after_n {
                    *triggered = true;
                    vec![Effect::StoplossModified {
                        id: data.id.to_owned(),
                        old_price: 0.0, // filled in by engine
                        new_price: data.average_entry,
                    }]
                } else {
                    vec![]
                }
            }

            // ── Time-based exit ─────────────────────────────────────────
            Rule::TimeExit { max_seconds } => {
                if let Some(open_ts) = data.open_ts {
                    let elapsed = (quote.ts - open_ts).num_seconds();
                    if elapsed >= *max_seconds as i64 {
                        vec![Effect::PositionClosed {
                            id: data.id.to_owned(),
                            reason: CloseReason::TimeExit,
                        }]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn quote(bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: "EURUSD".into(),
            ts: ts(12, 0, 0),
            bid,
            ask,
        }
    }

    fn view_buy(id: &str, entry: f64) -> PositionView<'_> {
        PositionView {
            id,
            symbol: "EURUSD",
            side: Side::Buy,
            status: PositionStatus::Open,
            average_entry: entry,
            remaining_ratio: 1.0,
            target_hits: 0,
            open_ts: Some(ts(10, 0, 0)),
        }
    }

    fn view_sell(id: &str, entry: f64) -> PositionView<'_> {
        PositionView {
            id,
            symbol: "EURUSD",
            side: Side::Sell,
            status: PositionStatus::Open,
            average_entry: entry,
            remaining_ratio: 1.0,
            target_hits: 0,
            open_ts: Some(ts(10, 0, 0)),
        }
    }

    #[test]
    fn fixed_stoploss_buy_triggers() {
        let mut rule = Rule::fixed_stoploss(1.0800);
        let v = view_buy("p1", 1.0850);
        // bid at SL → triggers
        let effects = rule.evaluate(&v, &quote(1.0800, 1.0802), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        ));
    }

    #[test]
    fn fixed_stoploss_buy_no_trigger() {
        let mut rule = Rule::fixed_stoploss(1.0800);
        let v = view_buy("p1", 1.0850);
        let effects = rule.evaluate(&v, &quote(1.0810, 1.0812), FillModel::BidAsk);
        assert!(effects.is_empty());
    }

    #[test]
    fn fixed_stoploss_sell_triggers() {
        let mut rule = Rule::fixed_stoploss(1.0900);
        let v = view_sell("p1", 1.0850);
        // ask at SL → triggers
        let effects = rule.evaluate(&v, &quote(1.0898, 1.0900), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Stoploss,
                ..
            }
        ));
    }

    #[test]
    fn trailing_stop_buy() {
        let mut rule = Rule::trailing_stop(0.0020);
        let v = view_buy("p1", 1.0850);

        // Price rises → no trigger
        let effects = rule.evaluate(&v, &quote(1.0870, 1.0872), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Price rises more → no trigger, peak updates
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Price drops within distance → no trigger (0.0900 - 0.0020 = 0.0880)
        let effects = rule.evaluate(&v, &quote(1.0882, 1.0884), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Price drops beyond distance → triggers
        let effects = rule.evaluate(&v, &quote(1.0879, 1.0881), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::TrailingStop,
                ..
            }
        ));
    }

    #[test]
    fn take_profit_partial() {
        let mut rule = Rule::take_profit(1.0900, 0.5);
        let v = view_buy("p1", 1.0850);

        // Not reached
        let effects = rule.evaluate(&v, &quote(1.0890, 1.0892), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Reached → partial close
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(
            matches!(&effects[0], Effect::PartialClose { ratio, .. } if (*ratio - 0.5).abs() < f64::EPSILON)
        );

        // Second call → already triggered
        let effects = rule.evaluate(&v, &quote(1.0910, 1.0912), FillModel::BidAsk);
        assert!(effects.is_empty());
    }

    #[test]
    fn take_profit_closes_remaining() {
        let mut rule = Rule::take_profit(1.0900, 1.0);
        let v = view_buy("p1", 1.0850);

        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::Target,
                ..
            }
        ));
    }

    #[test]
    fn breakeven_when_triggers() {
        let mut rule = Rule::breakeven_when(1.0900);
        let v = view_buy("p1", 1.0850);

        // Not reached
        let effects = rule.evaluate(&v, &quote(1.0890, 1.0892), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Reached
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(
            matches!(&effects[0], Effect::StoplossModified { new_price, .. } if (*new_price - 1.0850).abs() < f64::EPSILON)
        );

        // Already triggered
        let effects = rule.evaluate(&v, &quote(1.0910, 1.0912), FillModel::BidAsk);
        assert!(effects.is_empty());
    }

    #[test]
    fn breakeven_after_targets() {
        let mut rule = Rule::breakeven_after_targets(2);

        // Not enough target hits
        let mut v = view_buy("p1", 1.0850);
        v.target_hits = 1;
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert!(effects.is_empty());

        // Enough target hits
        v.target_hits = 2;
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(&effects[0], Effect::StoplossModified { .. }));

        // Already triggered
        v.target_hits = 3;
        let effects = rule.evaluate(&v, &quote(1.0900, 1.0902), FillModel::BidAsk);
        assert!(effects.is_empty());
    }

    #[test]
    fn time_exit_triggers() {
        let mut rule = Rule::time_exit(3600); // 1 hour
        let v = view_buy("p1", 1.0850); // open_ts = 10:00:00

        // 30 minutes later → no trigger
        let mut q = quote(1.0860, 1.0862);
        q.ts = ts(10, 30, 0);
        let effects = rule.evaluate(&v, &q, FillModel::BidAsk);
        assert!(effects.is_empty());

        // 1 hour later → triggers
        q.ts = ts(11, 0, 0);
        let effects = rule.evaluate(&v, &q, FillModel::BidAsk);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PositionClosed {
                reason: CloseReason::TimeExit,
                ..
            }
        ));
    }

    #[test]
    fn rule_skips_closed_position() {
        let mut rule = Rule::fixed_stoploss(1.0800);
        let mut v = view_buy("p1", 1.0850);
        v.status = PositionStatus::Closed;

        let effects = rule.evaluate(&v, &quote(1.0790, 1.0792), FillModel::BidAsk);
        assert!(effects.is_empty());
    }

    #[test]
    fn from_config_roundtrip() {
        let configs = vec![
            RuleConfig::FixedStoploss { price: 1.08 },
            RuleConfig::TrailingStop { distance: 0.002 },
            RuleConfig::TakeProfit {
                price: 1.09,
                close_ratio: 0.5,
            },
            RuleConfig::BreakevenWhen {
                trigger_price: 1.09,
            },
            RuleConfig::BreakevenAfterTargets { after_n: 2 },
            RuleConfig::TimeExit { max_seconds: 3600 },
        ];
        let names: Vec<&str> = configs
            .into_iter()
            .map(|c| Rule::from_config(c).name())
            .collect();
        assert_eq!(
            names,
            vec![
                "FixedStoploss",
                "TrailingStop",
                "TakeProfit",
                "BreakevenWhen",
                "BreakevenAfterTargets",
                "TimeExit",
            ]
        );
    }
}
