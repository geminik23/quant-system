//! Position manager — collection of positions with lookup helpers.
//!
//! `PositionManager` owns all positions and provides query/filter methods
//! used by the [`TradeEngine`](crate::engine::TradeEngine).

use std::collections::HashMap;

use crate::position::Position;
use crate::types::{GroupId, PositionId, PositionStatus, Side};

/// Optional grouping of positions for coordinated management.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PositionGroup {
    pub id: GroupId,
    pub positions: Vec<PositionId>,
}

/// Owns all positions and provides indexed access.
#[derive(Debug, Clone, Default)]
pub struct PositionManager {
    positions: HashMap<PositionId, Position>,
    groups: HashMap<GroupId, PositionGroup>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self::default()
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    /// Insert a position.  Overwrites if the id already exists.
    pub fn add(&mut self, position: Position) {
        let id = position.data.id.clone();
        self.positions.insert(id, position);
    }

    /// Remove a position by id.
    pub fn remove(&mut self, id: &str) -> Option<Position> {
        self.positions.remove(id)
    }

    /// Immutable lookup.
    pub fn get(&self, id: &str) -> Option<&Position> {
        self.positions.get(id)
    }

    /// Mutable lookup.
    pub fn get_mut(&mut self, id: &str) -> Option<&mut Position> {
        self.positions.get_mut(id)
    }

    /// Total number of tracked positions (all statuses).
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    // ── Iterators ───────────────────────────────────────────────────────

    /// Iterate over all positions.
    pub fn iter(&self) -> impl Iterator<Item = (&PositionId, &Position)> {
        self.positions.iter()
    }

    /// Iterate mutably over all positions.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&PositionId, &mut Position)> {
        self.positions.iter_mut()
    }

    // ── Filtered queries ────────────────────────────────────────────────

    /// Collect ids of positions matching the given status and symbol.
    pub fn ids_by_symbol_status(&self, symbol: &str, status: PositionStatus) -> Vec<PositionId> {
        self.positions
            .iter()
            .filter(|(_, p)| p.data.symbol == symbol && p.data.status == status)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Collect ids of all positions with the given status.
    pub fn ids_by_status(&self, status: PositionStatus) -> Vec<PositionId> {
        self.positions
            .iter()
            .filter(|(_, p)| p.data.status == status)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Collect ids of open positions on a given symbol with a given side.
    pub fn ids_by_symbol_side(&self, symbol: &str, side: Side) -> Vec<PositionId> {
        self.positions
            .iter()
            .filter(|(_, p)| {
                p.data.symbol == symbol
                    && p.data.side == side
                    && p.data.status == PositionStatus::Open
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// All open (active) positions.
    pub fn open_positions(&self) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.data.status == PositionStatus::Open)
            .collect()
    }

    /// All pending (unfilled) positions.
    pub fn pending_positions(&self) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.data.status == PositionStatus::Pending)
            .collect()
    }

    /// All closed positions.
    pub fn closed_positions(&self) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|p| p.data.status == PositionStatus::Closed)
            .collect()
    }

    /// Collect ids of all open positions on a given symbol.
    pub fn open_ids_by_symbol(&self, symbol: &str) -> Vec<PositionId> {
        self.ids_by_symbol_status(symbol, PositionStatus::Open)
    }

    /// Collect ids of all pending positions on a given symbol.
    pub fn pending_ids_by_symbol(&self, symbol: &str) -> Vec<PositionId> {
        self.ids_by_symbol_status(symbol, PositionStatus::Pending)
    }

    // ── Groups ──────────────────────────────────────────────────────────

    /// Create or update a position group.
    pub fn add_group(&mut self, group: PositionGroup) {
        self.groups.insert(group.id.clone(), group);
    }

    /// Get a group by id.
    pub fn get_group(&self, id: &str) -> Option<&PositionGroup> {
        self.groups.get(id)
    }

    /// Add a position to an existing group.  Creates the group if it does not
    /// exist.
    pub fn add_to_group(&mut self, group_id: &str, position_id: PositionId) {
        self.groups
            .entry(group_id.to_owned())
            .or_insert_with(|| PositionGroup {
                id: group_id.to_owned(),
                positions: Vec::new(),
            })
            .positions
            .push(position_id);
    }

    /// Return all position ids belonging to a group.
    pub fn group_position_ids(&self, group_id: &str) -> Vec<PositionId> {
        self.groups
            .get(group_id)
            .map(|g| g.positions.clone())
            .unwrap_or_default()
    }

    /// Remove a group (does **not** close or remove the positions).
    pub fn remove_group(&mut self, group_id: &str) -> Option<PositionGroup> {
        self.groups.remove(group_id)
    }

    /// All open position IDs in a group.
    pub fn open_ids_by_group(&self, group_id: &str) -> Vec<PositionId> {
        self.group_position_ids(group_id)
            .into_iter()
            .filter(|id| {
                self.positions
                    .get(id)
                    .map_or(false, |p| p.data.status == PositionStatus::Open)
            })
            .collect()
    }

    /// All pending position IDs in a group.
    pub fn pending_ids_by_group(&self, group_id: &str) -> Vec<PositionId> {
        self.group_position_ids(group_id)
            .into_iter()
            .filter(|id| {
                self.positions
                    .get(id)
                    .map_or(false, |p| p.data.status == PositionStatus::Pending)
            })
            .collect()
    }

    /// All group IDs that currently exist.
    pub fn all_group_ids(&self) -> Vec<&GroupId> {
        self.groups.keys().collect()
    }

    // ── Bulk helpers (used by engine) ───────────────────────────────────

    /// Collect ids of all open positions matching a side filter.
    pub fn open_ids_by_side(&self, side: Side) -> Vec<PositionId> {
        self.positions
            .iter()
            .filter(|(_, p)| p.data.status == PositionStatus::Open && p.data.side == side)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Collect ids of **all** open positions regardless of symbol.
    pub fn all_open_ids(&self) -> Vec<PositionId> {
        self.ids_by_status(PositionStatus::Open)
    }

    /// Collect ids of **all** pending positions regardless of symbol.
    pub fn all_pending_ids(&self) -> Vec<PositionId> {
        self.ids_by_status(PositionStatus::Pending)
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Position;
    use crate::types::{Fill, OrderType, Side};
    use chrono::NaiveDate;

    fn ts(h: u32, m: u32, s: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    fn make_open(id: &str, symbol: &str, side: Side) -> Position {
        Position::new_market(
            id.into(),
            symbol.into(),
            side,
            Fill {
                price: 1.0850,
                size: 1.0,
                ts: ts(10, 0, 0),
            },
            vec![],
        )
    }

    fn make_pending(id: &str, symbol: &str, side: Side) -> Position {
        Position::new_pending(
            id.into(),
            symbol.into(),
            side,
            OrderType::Limit,
            1.0800,
            1.0,
            ts(9, 0, 0),
            vec![],
        )
    }

    #[test]
    fn add_and_get() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("p1").is_some());
        assert!(mgr.get("p2").is_none());
    }

    #[test]
    fn remove_position() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        let removed = mgr.remove("p1");
        assert!(removed.is_some());
        assert!(mgr.is_empty());
    }

    #[test]
    fn filter_by_status() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        mgr.add(make_pending("p2", "EURUSD", Side::Buy));
        mgr.add(make_open("p3", "XAUUSD", Side::Sell));

        assert_eq!(mgr.open_positions().len(), 2);
        assert_eq!(mgr.pending_positions().len(), 1);
        assert_eq!(mgr.closed_positions().len(), 0);
    }

    #[test]
    fn filter_by_symbol_status() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        mgr.add(make_open("p2", "XAUUSD", Side::Buy));
        mgr.add(make_pending("p3", "EURUSD", Side::Sell));

        let ids = mgr.open_ids_by_symbol("EURUSD");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], "p1");

        let ids = mgr.pending_ids_by_symbol("EURUSD");
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], "p3");
    }

    #[test]
    fn filter_by_side() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        mgr.add(make_open("p2", "EURUSD", Side::Sell));
        mgr.add(make_open("p3", "XAUUSD", Side::Buy));

        let buy_ids = mgr.open_ids_by_side(Side::Buy);
        assert_eq!(buy_ids.len(), 2);

        let sell_ids = mgr.open_ids_by_side(Side::Sell);
        assert_eq!(sell_ids.len(), 1);
    }

    #[test]
    fn group_operations() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        mgr.add(make_open("p2", "EURUSD", Side::Buy));

        mgr.add_to_group("g1", "p1".into());
        mgr.add_to_group("g1", "p2".into());

        let group_ids = mgr.group_position_ids("g1");
        assert_eq!(group_ids.len(), 2);

        assert!(mgr.get_group("g1").is_some());
        assert!(mgr.get_group("g2").is_none());

        mgr.remove_group("g1");
        assert!(mgr.get_group("g1").is_none());
        // Positions are still there
        assert_eq!(mgr.len(), 2);
    }

    #[test]
    fn all_open_and_pending_ids() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p1", "EURUSD", Side::Buy));
        mgr.add(make_open("p2", "XAUUSD", Side::Sell));
        mgr.add(make_pending("p3", "EURUSD", Side::Buy));

        assert_eq!(mgr.all_open_ids().len(), 2);
        assert_eq!(mgr.all_pending_ids().len(), 1);
    }
}
