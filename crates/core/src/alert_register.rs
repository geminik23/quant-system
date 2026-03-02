//! Price alert register — BTreeMap-indexed rule evaluation for O(log N + K) per tick.
//!
//! Static rules (fixed SL, TP, breakeven trigger, pending fills) are stored as
//! sorted price alerts. On each tick only a range scan is needed to find
//! triggered alerts, instead of iterating every rule on every position.
//!
//! Stateful rules (trailing stop, time exit, breakeven-after-targets) remain
//! tick-by-tick but are tracked in a separate set so only those positions are
//! visited.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::types::{FillModel, OrderType, PositionId, PriceQuote, Side};

// ─── Alert key & entry types ────────────────────────────────────────────────

/// Composite key for sorted storage. Ordered by (symbol, price_micros).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AlertKey {
    symbol: String,
    /// Price stored as integer micros (price × 1_000_000) for exact Ord.
    price_micros: i64,
}

/// What to do when this alert fires.
#[derive(Debug, Clone)]
struct AlertEntry {
    position_id: PositionId,
    kind: AlertKind,
    /// Side of the position that owns this alert.
    side: Side,
}

/// The type of alert — determines what effect to produce when triggered.
#[derive(Debug, Clone, PartialEq)]
pub enum AlertKind {
    /// Close the position fully (fixed stoploss hit).
    Stoploss,
    /// Partial or full close at this take-profit level.
    TakeProfit { close_ratio: f64 },
    /// Move stoploss to entry price (breakeven trigger).
    BreakevenTrigger,
    /// Fill a pending order.
    PendingFill { order_type: OrderType, side: Side },
}

/// An alert that has been triggered by a price movement.
#[derive(Debug, Clone)]
pub struct TriggeredAlert {
    pub position_id: PositionId,
    pub kind: AlertKind,
    pub side: Side,
    pub trigger_price: f64,
}

// ─── Directional alert storage ──────────────────────────────────────────────

/// Two BTreeMaps split by trigger direction for efficient range scans.
#[derive(Debug, Clone, Default)]
struct DirectionalAlerts {
    /// Alerts that fire when eval_price <= threshold (buy SL, sell TP, limit buy fill, stop sell fill).
    fire_on_drop: BTreeMap<AlertKey, Vec<AlertEntry>>,
    /// Alerts that fire when eval_price >= threshold (sell SL, buy TP, limit sell fill, stop buy fill).
    fire_on_rise: BTreeMap<AlertKey, Vec<AlertEntry>>,
}

// ─── PriceAlertRegister ─────────────────────────────────────────────────────

/// BTreeMap-indexed price alert register for O(log N + K) rule evaluation.
///
/// Static rules (SL, TP, breakeven, pending fills) are stored as sorted price
/// alerts. Stateful rules (trailing stop, time exit) are tracked in a separate
/// set for tick-by-tick evaluation on only those positions.
#[derive(Debug, Clone, Default)]
pub struct PriceAlertRegister {
    /// Directional alert storage.
    alerts: DirectionalAlerts,
    /// Reverse index: position_id → set of registered alert keys.
    position_alerts: HashMap<PositionId, HashSet<AlertKey>>,
    /// Positions that require tick-by-tick evaluation (have stateful rules).
    /// symbol → set of position IDs.
    tick_eval_positions: HashMap<String, HashSet<PositionId>>,
}

/// Convert an f64 price to integer micros for BTreeMap keys.
fn price_to_micros(price: f64) -> i64 {
    (price * 1_000_000.0).round() as i64
}

/// Convert integer micros back to f64 price.
fn micros_to_price(micros: i64) -> f64 {
    micros as f64 / 1_000_000.0
}

impl PriceAlertRegister {
    /// Create an empty alert register.
    pub fn new() -> Self {
        Self::default()
    }

    // ── Registration ────────────────────────────────────────────────────

    /// Register a price alert for a position.
    pub fn register(
        &mut self,
        symbol: &str,
        price: f64,
        position_id: PositionId,
        side: Side,
        kind: AlertKind,
    ) {
        let key = AlertKey {
            symbol: symbol.to_owned(),
            price_micros: price_to_micros(price),
        };

        let entry = AlertEntry {
            position_id: position_id.clone(),
            kind: kind.clone(),
            side,
        };

        // Decide which BTreeMap based on trigger direction.
        let map = match Self::trigger_direction(side, &kind) {
            TriggerDirection::FireOnDrop => &mut self.alerts.fire_on_drop,
            TriggerDirection::FireOnRise => &mut self.alerts.fire_on_rise,
        };

        map.entry(key.clone()).or_default().push(entry);

        // Maintain reverse index.
        self.position_alerts
            .entry(position_id)
            .or_default()
            .insert(key);
    }

    /// Register a position for tick-by-tick evaluation (has stateful rules).
    pub fn register_tick_eval(&mut self, symbol: &str, position_id: PositionId) {
        self.tick_eval_positions
            .entry(symbol.to_owned())
            .or_default()
            .insert(position_id);
    }

    /// Unregister a position from tick-by-tick evaluation.
    pub fn unregister_tick_eval(&mut self, symbol: &str, position_id: &str) {
        if let Some(set) = self.tick_eval_positions.get_mut(symbol) {
            set.remove(position_id);
            if set.is_empty() {
                self.tick_eval_positions.remove(symbol);
            }
        }
    }

    // ── Deregistration ──────────────────────────────────────────────────

    /// Remove all alerts for a specific position.
    pub fn deregister_position(&mut self, position_id: &str) {
        if let Some(keys) = self.position_alerts.remove(position_id) {
            for key in keys {
                Self::remove_entry_from_map(&mut self.alerts.fire_on_drop, &key, position_id);
                Self::remove_entry_from_map(&mut self.alerts.fire_on_rise, &key, position_id);
            }
        }
        // Also remove from tick eval sets.
        self.tick_eval_positions.retain(|_, ids| {
            ids.remove(position_id);
            !ids.is_empty()
        });
    }

    /// Remove a specific alert for a position at a given price and kind.
    pub fn deregister_alert(
        &mut self,
        symbol: &str,
        price: f64,
        position_id: &str,
        side: Side,
        kind: &AlertKind,
    ) {
        let key = AlertKey {
            symbol: symbol.to_owned(),
            price_micros: price_to_micros(price),
        };

        let map = match Self::trigger_direction(side, kind) {
            TriggerDirection::FireOnDrop => &mut self.alerts.fire_on_drop,
            TriggerDirection::FireOnRise => &mut self.alerts.fire_on_rise,
        };

        Self::remove_entry_from_map(map, &key, position_id);

        // Clean up reverse index.
        if let Some(keys) = self.position_alerts.get_mut(position_id) {
            keys.remove(&key);
            if keys.is_empty() {
                self.position_alerts.remove(position_id);
            }
        }
    }

    /// Remove all alerts across all positions.
    pub fn clear_all(&mut self) {
        self.alerts.fire_on_drop.clear();
        self.alerts.fire_on_rise.clear();
        self.position_alerts.clear();
        self.tick_eval_positions.clear();
    }

    // ── Querying ────────────────────────────────────────────────────────

    /// Check all alerts for a symbol against the current quote.
    /// Returns triggered alerts and removes them from the register.
    pub fn check(&mut self, quote: &PriceQuote, model: FillModel) -> Vec<TriggeredAlert> {
        let mut triggered = Vec::new();

        // For fire_on_drop: alerts fire when eval_price <= alert_price.
        // We need the eval_price for the side of each alert's position.
        // Since buy and sell positions use different eval prices, we check
        // both sides using the bid (for buy-side eval under BidAsk) and
        // ask (for sell-side eval under BidAsk).

        // Compute all possible eval prices we need.
        let buy_eval = quote.eval_price(Side::Buy, model);
        let sell_eval = quote.eval_price(Side::Sell, model);
        let buy_fill = quote.fill_price(Side::Buy, model);
        let sell_fill = quote.fill_price(Side::Sell, model);

        let buy_eval_micros = price_to_micros(buy_eval);
        let sell_eval_micros = price_to_micros(sell_eval);
        let buy_fill_micros = price_to_micros(buy_fill);
        let sell_fill_micros = price_to_micros(sell_fill);

        // ── fire_on_drop: alert fires when market price ≤ alert_price ──
        // Range: all keys for this symbol with price_micros >= -∞ up to the
        // relevant eval/fill price.
        let sym = &quote.symbol;
        let range_start_drop = AlertKey {
            symbol: sym.clone(),
            price_micros: i64::MIN,
        };

        // We need to scan all fire_on_drop entries for this symbol and check
        // if the appropriate eval/fill price has dropped to or below the
        // alert price. Since different alerts use different prices (buy vs
        // sell, eval vs fill), we scan all entries for this symbol.
        let range_end_drop = AlertKey {
            symbol: sym.clone(),
            price_micros: i64::MAX,
        };

        let mut keys_to_remove_drop: Vec<(AlertKey, Vec<usize>)> = Vec::new();

        // Collect from fire_on_drop.
        for (key, entries) in self
            .alerts
            .fire_on_drop
            .range(range_start_drop..=range_end_drop.clone())
        {
            let mut indices = Vec::new();
            for (i, entry) in entries.iter().enumerate() {
                let check_micros = match &entry.kind {
                    AlertKind::PendingFill { .. } => match entry.side {
                        Side::Buy => buy_fill_micros,
                        Side::Sell => sell_fill_micros,
                    },
                    _ => match entry.side {
                        Side::Buy => buy_eval_micros,
                        Side::Sell => sell_eval_micros,
                    },
                };

                // fire_on_drop: fires when check_price <= alert_price
                if check_micros <= key.price_micros {
                    triggered.push(TriggeredAlert {
                        position_id: entry.position_id.clone(),
                        kind: entry.kind.clone(),
                        side: entry.side,
                        trigger_price: micros_to_price(key.price_micros),
                    });
                    indices.push(i);
                }
            }
            if !indices.is_empty() {
                keys_to_remove_drop.push((key.clone(), indices));
            }
        }

        // ── fire_on_rise: alert fires when market price ≥ alert_price ──
        let range_start_rise = AlertKey {
            symbol: sym.clone(),
            price_micros: i64::MIN,
        };
        let range_end_rise = AlertKey {
            symbol: sym.clone(),
            price_micros: i64::MAX,
        };

        let mut keys_to_remove_rise: Vec<(AlertKey, Vec<usize>)> = Vec::new();

        for (key, entries) in self
            .alerts
            .fire_on_rise
            .range(range_start_rise..=range_end_rise)
        {
            let mut indices = Vec::new();
            for (i, entry) in entries.iter().enumerate() {
                let check_micros = match &entry.kind {
                    AlertKind::PendingFill { .. } => match entry.side {
                        Side::Buy => buy_fill_micros,
                        Side::Sell => sell_fill_micros,
                    },
                    _ => match entry.side {
                        Side::Buy => buy_eval_micros,
                        Side::Sell => sell_eval_micros,
                    },
                };

                // fire_on_rise: fires when check_price >= alert_price
                if check_micros >= key.price_micros {
                    triggered.push(TriggeredAlert {
                        position_id: entry.position_id.clone(),
                        kind: entry.kind.clone(),
                        side: entry.side,
                        trigger_price: micros_to_price(key.price_micros),
                    });
                    indices.push(i);
                }
            }
            if !indices.is_empty() {
                keys_to_remove_rise.push((key.clone(), indices));
            }
        }

        // Remove triggered entries (in reverse index order to preserve indices).
        for (key, mut indices) in keys_to_remove_drop {
            indices.sort_unstable_by(|a, b| b.cmp(a));
            if let Some(entries) = self.alerts.fire_on_drop.get_mut(&key) {
                for i in &indices {
                    let removed = entries.remove(*i);
                    // Clean up reverse index.
                    if let Some(keys) = self.position_alerts.get_mut(&removed.position_id) {
                        // Only remove the key from reverse index if no more entries
                        // for this position at this key.
                        let still_present =
                            entries.iter().any(|e| e.position_id == removed.position_id);
                        if !still_present {
                            // Also check fire_on_rise for this key.
                            let in_rise = self.alerts.fire_on_rise.get(&key).map_or(false, |v| {
                                v.iter().any(|e| e.position_id == removed.position_id)
                            });
                            if !in_rise {
                                keys.remove(&key);
                            }
                        }
                        if keys.is_empty() {
                            self.position_alerts.remove(&removed.position_id);
                        }
                    }
                }
                if entries.is_empty() {
                    self.alerts.fire_on_drop.remove(&key);
                }
            }
        }

        for (key, mut indices) in keys_to_remove_rise {
            indices.sort_unstable_by(|a, b| b.cmp(a));
            if let Some(entries) = self.alerts.fire_on_rise.get_mut(&key) {
                for i in &indices {
                    let removed = entries.remove(*i);
                    if let Some(keys) = self.position_alerts.get_mut(&removed.position_id) {
                        let still_present =
                            entries.iter().any(|e| e.position_id == removed.position_id);
                        if !still_present {
                            let in_drop = self.alerts.fire_on_drop.get(&key).map_or(false, |v| {
                                v.iter().any(|e| e.position_id == removed.position_id)
                            });
                            if !in_drop {
                                keys.remove(&key);
                            }
                        }
                        if keys.is_empty() {
                            self.position_alerts.remove(&removed.position_id);
                        }
                    }
                }
                if entries.is_empty() {
                    self.alerts.fire_on_rise.remove(&key);
                }
            }
        }

        triggered
    }

    /// Get position IDs that need tick-by-tick evaluation for a symbol.
    pub fn tick_eval_ids(&self, symbol: &str) -> Vec<PositionId> {
        self.tick_eval_positions
            .get(symbol)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if a position has any registered alerts.
    pub fn has_alerts(&self, position_id: &str) -> bool {
        self.position_alerts
            .get(position_id)
            .map_or(false, |k| !k.is_empty())
    }

    /// Check if a position is registered for tick evaluation.
    pub fn is_tick_eval(&self, symbol: &str, position_id: &str) -> bool {
        self.tick_eval_positions
            .get(symbol)
            .map_or(false, |s| s.contains(position_id))
    }

    /// Total number of alert entries across both directional maps.
    pub fn alert_count(&self) -> usize {
        let drop_count: usize = self.alerts.fire_on_drop.values().map(|v| v.len()).sum();
        let rise_count: usize = self.alerts.fire_on_rise.values().map(|v| v.len()).sum();
        drop_count + rise_count
    }

    /// Number of positions with registered alerts.
    pub fn position_count(&self) -> usize {
        self.position_alerts.len()
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /// Determine which BTreeMap to use based on position side and alert kind.
    fn trigger_direction(side: Side, kind: &AlertKind) -> TriggerDirection {
        match kind {
            // Buy SL fires when price drops to or below SL → fire_on_drop
            AlertKind::Stoploss => match side {
                Side::Buy => TriggerDirection::FireOnDrop,
                Side::Sell => TriggerDirection::FireOnRise,
            },
            // Buy TP fires when price rises to or above TP → fire_on_rise
            AlertKind::TakeProfit { .. } => match side {
                Side::Buy => TriggerDirection::FireOnRise,
                Side::Sell => TriggerDirection::FireOnDrop,
            },
            // Buy breakeven fires when price rises to trigger → fire_on_rise
            AlertKind::BreakevenTrigger => match side {
                Side::Buy => TriggerDirection::FireOnRise,
                Side::Sell => TriggerDirection::FireOnDrop,
            },
            // Pending fills depend on order type and side.
            AlertKind::PendingFill {
                order_type,
                side: pending_side,
            } => {
                match (order_type, pending_side) {
                    // Limit Buy: fill when price drops to limit → fire_on_drop
                    (OrderType::Limit, Side::Buy) => TriggerDirection::FireOnDrop,
                    // Limit Sell: fill when price rises to limit → fire_on_rise
                    (OrderType::Limit, Side::Sell) => TriggerDirection::FireOnRise,
                    // Stop Buy: fill when price rises to stop → fire_on_rise
                    (OrderType::Stop, Side::Buy) => TriggerDirection::FireOnRise,
                    // Stop Sell: fill when price drops to stop → fire_on_drop
                    (OrderType::Stop, Side::Sell) => TriggerDirection::FireOnDrop,
                    // Market orders should never be pending.
                    (OrderType::Market, _) => TriggerDirection::FireOnRise,
                }
            }
        }
    }

    /// Remove entries for a position from a BTreeMap at a specific key.
    fn remove_entry_from_map(
        map: &mut BTreeMap<AlertKey, Vec<AlertEntry>>,
        key: &AlertKey,
        position_id: &str,
    ) {
        if let Some(entries) = map.get_mut(key) {
            entries.retain(|e| e.position_id != position_id);
            if entries.is_empty() {
                map.remove(key);
            }
        }
    }
}

/// Which directional BTreeMap to store/check the alert in.
enum TriggerDirection {
    FireOnDrop,
    FireOnRise,
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FillModel, PriceQuote, Side};
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn quote(sym: &str, bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: sym.into(),
            ts: ts(10, 0, 0),
            bid,
            ask,
        }
    }

    #[test]
    fn register_and_check_buy_stoploss() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );

        // Price above SL — no trigger.
        let q = quote("EURUSD", 1.0850, 1.0852);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price drops to SL — triggers.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].position_id, "p1");
        assert!(matches!(t[0].kind, AlertKind::Stoploss));

        // Already removed — no trigger again.
        let q = quote("EURUSD", 1.0750, 1.0752);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());
    }

    #[test]
    fn register_and_check_sell_stoploss() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Sell,
            AlertKind::Stoploss,
        );

        // Price below SL — no trigger.
        let q = quote("EURUSD", 1.0848, 1.0850);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price rises to SL — triggers (sell SL checks ask).
        let q = quote("EURUSD", 1.0898, 1.0900);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].position_id, "p1");
    }

    #[test]
    fn register_and_check_take_profit() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Buy,
            AlertKind::TakeProfit { close_ratio: 0.5 },
        );

        // Price below TP — no trigger.
        let q = quote("EURUSD", 1.0848, 1.0850);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price reaches TP — triggers (buy TP checks bid for BidAsk).
        let q = quote("EURUSD", 1.0900, 1.0905);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(
            matches!(t[0].kind, AlertKind::TakeProfit { close_ratio } if (close_ratio - 0.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn multiple_alerts_same_price() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0800,
            "p2".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0800,
            "p3".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );

        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 3);
    }

    #[test]
    fn alerts_different_symbols_independent() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "XAUUSD",
            2300.00,
            "p2".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );

        // Only EURUSD tick — only p1 triggers.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].position_id, "p1");

        // XAUUSD alert still alive.
        let q = quote("XAUUSD", 2300.00, 2301.00);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].position_id, "p2");
    }

    #[test]
    fn deregister_position_removes_all_alerts() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Buy,
            AlertKind::TakeProfit { close_ratio: 0.5 },
        );
        reg.register(
            "EURUSD",
            1.0880,
            "p1".into(),
            Side::Buy,
            AlertKind::BreakevenTrigger,
        );
        assert_eq!(reg.alert_count(), 3);

        reg.deregister_position("p1");
        assert_eq!(reg.alert_count(), 0);
        assert!(!reg.has_alerts("p1"));

        // Nothing triggers.
        let q = quote("EURUSD", 1.0700, 1.0702);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        let q = quote("EURUSD", 1.0950, 1.0952);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());
    }

    #[test]
    fn deregister_single_alert() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Buy,
            AlertKind::TakeProfit { close_ratio: 0.5 },
        );

        // Deregister only the SL.
        reg.deregister_alert("EURUSD", 1.0800, "p1", Side::Buy, &AlertKind::Stoploss);

        // SL price — no trigger.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // TP price — still triggers.
        let q = quote("EURUSD", 1.0900, 1.0902);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(matches!(t[0].kind, AlertKind::TakeProfit { .. }));
    }

    #[test]
    fn pending_fill_alert_limit_buy() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::PendingFill {
                order_type: OrderType::Limit,
                side: Side::Buy,
            },
        );

        // Price above limit — no fill.
        let q = quote("EURUSD", 1.0848, 1.0850);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price drops to limit — fills.
        let q = quote("EURUSD", 1.0798, 1.0800);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(matches!(
            t[0].kind,
            AlertKind::PendingFill {
                order_type: OrderType::Limit,
                side: Side::Buy
            }
        ));
    }

    #[test]
    fn pending_fill_alert_stop_buy() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Buy,
            AlertKind::PendingFill {
                order_type: OrderType::Stop,
                side: Side::Buy,
            },
        );

        // Price below stop — no fill.
        let q = quote("EURUSD", 1.0848, 1.0850);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price rises to stop — fills (Stop Buy checks ask for BidAsk fill_price).
        let q = quote("EURUSD", 1.0898, 1.0900);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn clear_all_removes_everything() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "XAUUSD",
            2300.00,
            "p2".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register_tick_eval("EURUSD", "p3".into());

        reg.clear_all();

        assert_eq!(reg.alert_count(), 0);
        assert_eq!(reg.position_count(), 0);
        assert!(reg.tick_eval_ids("EURUSD").is_empty());

        let q = quote("EURUSD", 1.0700, 1.0702);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());
    }

    #[test]
    fn tick_eval_ids_returns_stateful_only() {
        let mut reg = PriceAlertRegister::new();
        // p1 has trailing stop → tick eval.
        reg.register_tick_eval("EURUSD", "p1".into());
        // p2 has only SL+TP → alert register only.
        reg.register(
            "EURUSD",
            1.0800,
            "p2".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0900,
            "p2".into(),
            Side::Buy,
            AlertKind::TakeProfit { close_ratio: 1.0 },
        );

        let tick_ids = reg.tick_eval_ids("EURUSD");
        assert_eq!(tick_ids.len(), 1);
        assert!(tick_ids.contains(&"p1".to_owned()));
    }

    #[test]
    fn is_tick_eval_check() {
        let mut reg = PriceAlertRegister::new();
        reg.register_tick_eval("EURUSD", "p1".into());

        assert!(reg.is_tick_eval("EURUSD", "p1"));
        assert!(!reg.is_tick_eval("EURUSD", "p2"));
        assert!(!reg.is_tick_eval("XAUUSD", "p1"));
    }

    #[test]
    fn deregister_position_also_removes_tick_eval() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register_tick_eval("EURUSD", "p1".into());

        assert!(reg.is_tick_eval("EURUSD", "p1"));
        reg.deregister_position("p1");
        assert!(!reg.is_tick_eval("EURUSD", "p1"));
        assert_eq!(reg.alert_count(), 0);
    }

    #[test]
    fn breakeven_trigger_buy() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0900,
            "p1".into(),
            Side::Buy,
            AlertKind::BreakevenTrigger,
        );

        // Price below trigger — no fire.
        let q = quote("EURUSD", 1.0850, 1.0852);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price reaches trigger — fires.
        let q = quote("EURUSD", 1.0900, 1.0902);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(matches!(t[0].kind, AlertKind::BreakevenTrigger));
    }

    #[test]
    fn breakeven_trigger_sell() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0750,
            "p1".into(),
            Side::Sell,
            AlertKind::BreakevenTrigger,
        );

        // Price above trigger — no fire.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price drops to trigger — fires (sell breakeven checks ask dropping to level).
        let q = quote("EURUSD", 1.0748, 1.0750);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(matches!(t[0].kind, AlertKind::BreakevenTrigger));
    }

    #[test]
    fn sell_take_profit_fires_on_drop() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0750,
            "p1".into(),
            Side::Sell,
            AlertKind::TakeProfit { close_ratio: 1.0 },
        );

        // Price above TP — no trigger.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert!(t.is_empty());

        // Price drops to TP — triggers (sell TP checks ask).
        let q = quote("EURUSD", 1.0748, 1.0750);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn price_to_micros_precision() {
        assert_eq!(price_to_micros(1.08500), 1_085_000);
        assert_eq!(price_to_micros(2350.50), 2_350_500_000);
        assert_eq!(price_to_micros(0.0), 0);
        assert_eq!(price_to_micros(100_000.0), 100_000_000_000);
    }

    #[test]
    fn micros_roundtrip() {
        let prices = [1.08500, 2350.50, 154.325, 0.00001, 100000.0];
        for p in prices {
            let m = price_to_micros(p);
            let back = micros_to_price(m);
            assert!(
                (back - p).abs() < 1e-6,
                "roundtrip failed for {p}: got {back}"
            );
        }
    }

    #[test]
    fn fill_model_ask_only_buy_sl() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );

        // AskOnly: buy eval price is ask.
        // Ask at 1.0810 > 1.0800 — no trigger.
        let q = quote("EURUSD", 1.0790, 1.0810);
        let t = reg.check(&q, FillModel::AskOnly);
        assert!(t.is_empty());

        // Ask drops to 1.0800 — triggers.
        let q = quote("EURUSD", 1.0790, 1.0800);
        let t = reg.check(&q, FillModel::AskOnly);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn multiple_positions_partial_trigger() {
        let mut reg = PriceAlertRegister::new();
        reg.register(
            "EURUSD",
            1.0800,
            "p1".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0700,
            "p2".into(),
            Side::Buy,
            AlertKind::Stoploss,
        );

        // Price drops to 1.0800 — only p1 triggers.
        let q = quote("EURUSD", 1.0800, 1.0802);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].position_id, "p1");

        // p2 alert still alive.
        assert!(reg.has_alerts("p2"));
        assert!(!reg.has_alerts("p1"));
    }

    #[test]
    fn mixed_alert_types_same_position() {
        let mut reg = PriceAlertRegister::new();
        let pid: PositionId = "p1".into();

        // Buy position: SL at 1.0800 (fire_on_drop), TP at 1.0900 (fire_on_rise),
        // breakeven at 1.0870 (fire_on_rise).
        reg.register(
            "EURUSD",
            1.0800,
            pid.clone(),
            Side::Buy,
            AlertKind::Stoploss,
        );
        reg.register(
            "EURUSD",
            1.0900,
            pid.clone(),
            Side::Buy,
            AlertKind::TakeProfit { close_ratio: 0.5 },
        );
        reg.register(
            "EURUSD",
            1.0870,
            pid.clone(),
            Side::Buy,
            AlertKind::BreakevenTrigger,
        );

        assert_eq!(reg.alert_count(), 3);

        // Price at 1.0870 — breakeven fires.
        let q = quote("EURUSD", 1.0870, 1.0872);
        let t = reg.check(&q, FillModel::BidAsk);
        assert_eq!(t.len(), 1);
        assert!(matches!(t[0].kind, AlertKind::BreakevenTrigger));

        // SL and TP still registered.
        assert_eq!(reg.alert_count(), 2);
    }

    #[test]
    fn unregister_tick_eval() {
        let mut reg = PriceAlertRegister::new();
        reg.register_tick_eval("EURUSD", "p1".into());
        reg.register_tick_eval("EURUSD", "p2".into());

        assert_eq!(reg.tick_eval_ids("EURUSD").len(), 2);

        reg.unregister_tick_eval("EURUSD", "p1");
        let ids = reg.tick_eval_ids("EURUSD");
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"p2".to_owned()));
    }
}
