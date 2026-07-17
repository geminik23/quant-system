//! Position manager — collection of positions with lookup helpers.
//!
//! `PositionManager` owns all positions and provides query/filter methods
//! used by the [`TradeEngine`](crate::engine::TradeEngine).

use std::collections::{HashMap, HashSet};

use crate::position::Position;
use crate::types::{GroupId, PositionId, PositionStatus, Side, TradeId};

/// Optional grouping of positions for coordinated management.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PositionGroup {
    pub id: GroupId,
    pub positions: Vec<PositionId>,
}

/// Errors from atomic checked manager mutations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PositionManagerError {
    #[error("position not found: {0}")]
    PositionNotFound(PositionId),
    #[error("trade id {trade_id:?} is already assigned to position {existing_position_id}")]
    DuplicateTradeId {
        trade_id: TradeId,
        existing_position_id: PositionId,
    },
}

/// Owns all positions and provides indexed access.
#[derive(Debug, Clone, Default)]
pub struct PositionManager {
    positions: HashMap<PositionId, Position>,
    groups: HashMap<GroupId, PositionGroup>,
    /// Secondary index: symbol → position IDs (all statuses).
    symbol_index: HashMap<String, HashSet<PositionId>>,
    /// Secondary index: trade_id → position ID (current/last position per trade id).
    trade_index: HashMap<TradeId, PositionId>,
}

#[derive(Debug)]
pub(crate) struct PositionManagerCheckpoint {
    positions: HashMap<PositionId, Position>,
    existing_position_ids: HashSet<PositionId>,
    groups: HashMap<GroupId, PositionGroup>,
    symbol_index: HashMap<String, HashSet<PositionId>>,
    trade_index: HashMap<TradeId, PositionId>,
}

#[derive(Debug)]
pub(crate) struct PositionManagerQuoteCheckpoint {
    positions: HashMap<PositionId, Position>,
}

impl PositionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn checkpoint(
        &self,
        position_ids: impl IntoIterator<Item = PositionId>,
    ) -> PositionManagerCheckpoint {
        let positions = position_ids
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .filter_map(|id| {
                self.positions
                    .get(&id)
                    .cloned()
                    .map(|position| (id, position))
            })
            .collect();
        PositionManagerCheckpoint {
            positions,
            existing_position_ids: self.positions.keys().cloned().collect(),
            groups: self.groups.clone(),
            symbol_index: self.symbol_index.clone(),
            trade_index: self.trade_index.clone(),
        }
    }

    pub(crate) fn restore(&mut self, checkpoint: PositionManagerCheckpoint) {
        self.positions
            .retain(|id, _| checkpoint.existing_position_ids.contains(id));
        self.positions.extend(checkpoint.positions);
        self.groups = checkpoint.groups;
        self.symbol_index = checkpoint.symbol_index;
        self.trade_index = checkpoint.trade_index;
    }

    pub(crate) fn checkpoint_for_quote(&self, symbol: &str) -> PositionManagerQuoteCheckpoint {
        let positions = self
            .symbol_index
            .get(symbol)
            .into_iter()
            .flatten()
            .filter_map(|id| {
                let position = self.positions.get(id)?;
                matches!(
                    position.data.status,
                    PositionStatus::Open | PositionStatus::Pending
                )
                .then(|| (id.clone(), position.clone()))
            })
            .collect();
        PositionManagerQuoteCheckpoint { positions }
    }

    pub(crate) fn restore_quote(&mut self, checkpoint: PositionManagerQuoteCheckpoint) {
        self.positions.extend(checkpoint.positions);
    }

    // ── CRUD ────────────────────────────────────────────────────────────

    /// Insert a position.  Overwrites if the id already exists.
    pub fn add(&mut self, position: Position) {
        let id = position.data.id.clone();
        let symbol = position.data.symbol.clone();
        let trade_id = position.data.trade_id.clone();
        self.symbol_index
            .entry(symbol)
            .or_default()
            .insert(id.clone());
        if let Some(tid) = trade_id {
            self.trade_index.insert(tid, id.clone());
        }
        self.positions.insert(id, position);
    }

    /// Atomically insert a position after rejecting a duplicate non-empty trade ID.
    ///
    /// Validation happens before any primary or secondary index mutation. The
    /// legacy [`Self::add`] API remains available for callers that require its
    /// historical last-write-wins behavior.
    pub fn add_checked(
        &mut self,
        position: Position,
    ) -> std::result::Result<(), PositionManagerError> {
        if let Some(trade_id) = position.data.trade_id.as_deref() {
            self.ensure_trade_id_available(trade_id, Some(&position.data.id))?;
        }
        let id = position.data.id.clone();
        if self.positions.contains_key(&id) {
            self.remove(&id);
        }
        self.add(position);
        Ok(())
    }

    /// Remove a position by id.
    pub fn remove(&mut self, id: &str) -> Option<Position> {
        let pos = self.positions.remove(id);
        if let Some(ref p) = pos {
            if let Some(set) = self.symbol_index.get_mut(&p.data.symbol) {
                set.remove(id);
                if set.is_empty() {
                    self.symbol_index.remove(&p.data.symbol);
                }
            }
            if let Some(tid) = p.data.trade_id.as_ref()
                && self.trade_index.get(tid).is_some_and(|x| x == id)
            {
                self.trade_index.remove(tid);
            }
        }
        pos
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

    /// All position IDs in stable lexicographic order.
    pub fn position_ids_sorted(&self) -> Vec<PositionId> {
        sorted_ids(self.positions.keys().cloned().collect())
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

    /// Stable lexicographic variant of [`Self::ids_by_symbol_status`].
    pub fn ids_by_symbol_status_sorted(
        &self,
        symbol: &str,
        status: PositionStatus,
    ) -> Vec<PositionId> {
        sorted_ids(self.ids_by_symbol_status(symbol, status))
    }

    /// Collect ids of all positions with the given status.
    pub fn ids_by_status(&self, status: PositionStatus) -> Vec<PositionId> {
        self.positions
            .iter()
            .filter(|(_, p)| p.data.status == status)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Stable lexicographic variant of [`Self::ids_by_status`].
    pub fn ids_by_status_sorted(&self, status: PositionStatus) -> Vec<PositionId> {
        sorted_ids(self.ids_by_status(status))
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

    /// Stable lexicographic variant of [`Self::ids_by_symbol_side`].
    pub fn ids_by_symbol_side_sorted(&self, symbol: &str, side: Side) -> Vec<PositionId> {
        sorted_ids(self.ids_by_symbol_side(symbol, side))
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
        self.ids_for_symbol(symbol)
            .into_iter()
            .filter(|id| {
                self.positions
                    .get(id)
                    .is_some_and(|p| p.data.status == PositionStatus::Open)
            })
            .collect()
    }

    /// Stable lexicographic variant of [`Self::open_ids_by_symbol`].
    pub fn open_ids_by_symbol_sorted(&self, symbol: &str) -> Vec<PositionId> {
        sorted_ids(self.open_ids_by_symbol(symbol))
    }

    /// Collect ids of all pending positions on a given symbol.
    pub fn pending_ids_by_symbol(&self, symbol: &str) -> Vec<PositionId> {
        self.ids_for_symbol(symbol)
            .into_iter()
            .filter(|id| {
                self.positions
                    .get(id)
                    .is_some_and(|p| p.data.status == PositionStatus::Pending)
            })
            .collect()
    }

    /// Stable lexicographic variant of [`Self::pending_ids_by_symbol`].
    pub fn pending_ids_by_symbol_sorted(&self, symbol: &str) -> Vec<PositionId> {
        sorted_ids(self.pending_ids_by_symbol(symbol))
    }

    /// All position IDs for a symbol (any status), via the symbol index.
    pub fn ids_for_symbol(&self, symbol: &str) -> Vec<PositionId> {
        self.symbol_index
            .get(symbol)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Stable lexicographic variant of [`Self::ids_for_symbol`].
    pub fn ids_for_symbol_sorted(&self, symbol: &str) -> Vec<PositionId> {
        sorted_ids(self.ids_for_symbol(symbol))
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
                    .is_some_and(|p| p.data.status == PositionStatus::Open)
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
                    .is_some_and(|p| p.data.status == PositionStatus::Pending)
            })
            .collect()
    }

    /// All group IDs that currently exist.
    pub fn all_group_ids(&self) -> Vec<&GroupId> {
        self.groups.keys().collect()
    }

    /// All group IDs in stable lexicographic order.
    pub fn all_group_ids_sorted(&self) -> Vec<&GroupId> {
        let mut ids: Vec<_> = self.groups.keys().collect();
        ids.sort();
        ids
    }

    // ── Trade ID helpers ─────────────────────────────────────────────────

    /// Look up the position ID for a given application-defined trade id.
    pub fn id_by_trade_id(&self, trade_id: &str) -> Option<PositionId> {
        self.trade_index.get(trade_id).cloned()
    }

    /// Return the current owner when adding `position` would duplicate a trade
    /// ID already assigned to a different position.
    ///
    /// This is opt-in validation; [`Self::add`] intentionally retains its
    /// legacy last-write-wins trade-index behavior.
    pub fn would_duplicate_trade_id(&self, position: &Position) -> Option<PositionId> {
        let trade_id = position.data.trade_id.as_deref()?;
        self.conflicting_trade_id_owner(trade_id, Some(&position.data.id))
    }

    /// Validate a non-empty trade ID without mutating manager state.
    pub fn ensure_trade_id_available(
        &self,
        trade_id: &str,
        position_id: Option<&str>,
    ) -> std::result::Result<(), PositionManagerError> {
        if let Some(existing_position_id) = self.conflicting_trade_id_owner(trade_id, position_id) {
            return Err(PositionManagerError::DuplicateTradeId {
                trade_id: trade_id.to_owned(),
                existing_position_id,
            });
        }
        Ok(())
    }

    fn conflicting_trade_id_owner(
        &self,
        trade_id: &str,
        position_id: Option<&str>,
    ) -> Option<PositionId> {
        if trade_id.is_empty() {
            return None;
        }
        self.positions.iter().find_map(|(id, position)| {
            (position.data.trade_id.as_deref() == Some(trade_id)
                && position_id != Some(id.as_str()))
            .then(|| id.clone())
        })
    }

    /// Find every trade ID currently present on more than one stored position.
    /// Results are stable and lexicographically sorted.
    pub fn duplicate_trade_ids(&self) -> Vec<TradeId> {
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for trade_id in self
            .positions
            .values()
            .filter_map(|position| position.data.trade_id.as_deref())
        {
            *counts.entry(trade_id).or_default() += 1;
        }

        let mut duplicates: Vec<_> = counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(trade_id, _)| trade_id.to_owned())
            .collect();
        duplicates.sort();
        duplicates
    }

    /// Update the trade_id mapping for a position (used when trade_id
    /// is attached after position creation, e.g. during scale-in).
    pub fn set_trade_id(&mut self, position_id: &str, trade_id: TradeId) {
        self.trade_index.insert(trade_id, position_id.to_owned());
    }

    /// Atomically attach a trade ID to a stored position and update the index.
    pub fn set_trade_id_checked(
        &mut self,
        position_id: &str,
        trade_id: TradeId,
    ) -> std::result::Result<(), PositionManagerError> {
        if !self.positions.contains_key(position_id) {
            return Err(PositionManagerError::PositionNotFound(
                position_id.to_owned(),
            ));
        }
        self.ensure_trade_id_available(&trade_id, Some(position_id))?;

        let previous = self
            .positions
            .get(position_id)
            .and_then(|position| position.data.trade_id.clone());
        if let Some(previous) = previous
            && self.trade_index.get(&previous).map(String::as_str) == Some(position_id)
        {
            self.trade_index.remove(&previous);
        }

        self.positions
            .get_mut(position_id)
            .expect("position existence checked above")
            .set_trade_id(Some(trade_id.clone()));
        self.trade_index.insert(trade_id, position_id.to_owned());
        Ok(())
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

    /// Stable lexicographic variant of [`Self::open_ids_by_side`].
    pub fn open_ids_by_side_sorted(&self, side: Side) -> Vec<PositionId> {
        sorted_ids(self.open_ids_by_side(side))
    }

    /// Collect ids of **all** open positions regardless of symbol.
    pub fn all_open_ids(&self) -> Vec<PositionId> {
        self.ids_by_status(PositionStatus::Open)
    }

    /// All open position IDs in stable lexicographic order.
    pub fn all_open_ids_sorted(&self) -> Vec<PositionId> {
        self.ids_by_status_sorted(PositionStatus::Open)
    }

    /// Collect ids of **all** pending positions regardless of symbol.
    pub fn all_pending_ids(&self) -> Vec<PositionId> {
        self.ids_by_status(PositionStatus::Pending)
    }

    /// All pending position IDs in stable lexicographic order.
    pub fn all_pending_ids_sorted(&self) -> Vec<PositionId> {
        self.ids_by_status_sorted(PositionStatus::Pending)
    }
}

fn sorted_ids(mut ids: Vec<PositionId>) -> Vec<PositionId> {
    ids.sort();
    ids
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Position;
    use crate::types::{CloseReason, Fill, OrderType, Side};
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
    fn quote_checkpoint_excludes_closed_history_and_other_symbols() {
        let mut mgr = PositionManager::new();
        for index in 0..1_000 {
            let id = format!("closed-{index:04}");
            let mut position = make_open(&id, "EURUSD", Side::Buy);
            position
                .data
                .apply_full_close(CloseReason::Manual, ts(10, 1, 0));
            mgr.add(position);
        }
        mgr.add(make_open("open", "EURUSD", Side::Buy));
        mgr.add(make_pending("pending", "EURUSD", Side::Sell));
        mgr.add(make_open("other-symbol", "XAUUSD", Side::Buy));

        let checkpoint = mgr.checkpoint_for_quote("EURUSD");

        assert_eq!(checkpoint.positions.len(), 2);
        assert!(checkpoint.positions.contains_key("open"));
        assert!(checkpoint.positions.contains_key("pending"));
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

    #[test]
    fn sorted_queries_are_stable_and_lexicographic() {
        let mut mgr = PositionManager::new();
        mgr.add(make_open("p20", "EURUSD", Side::Buy));
        mgr.add(make_pending("p03", "EURUSD", Side::Sell));
        mgr.add(make_open("p10", "EURUSD", Side::Buy));
        mgr.add(make_open("p01", "XAUUSD", Side::Sell));
        mgr.add_group(PositionGroup {
            id: "group-z".into(),
            positions: vec![],
        });
        mgr.add_group(PositionGroup {
            id: "group-a".into(),
            positions: vec![],
        });

        assert_eq!(mgr.position_ids_sorted(), vec!["p01", "p03", "p10", "p20"]);
        assert_eq!(
            mgr.ids_for_symbol_sorted("EURUSD"),
            vec!["p03", "p10", "p20"]
        );
        assert_eq!(
            mgr.ids_by_symbol_status_sorted("EURUSD", PositionStatus::Open),
            vec!["p10", "p20"]
        );
        assert_eq!(
            mgr.ids_by_symbol_side_sorted("EURUSD", Side::Buy),
            vec!["p10", "p20"]
        );
        assert_eq!(mgr.open_ids_by_symbol_sorted("EURUSD"), vec!["p10", "p20"]);
        assert_eq!(mgr.pending_ids_by_symbol_sorted("EURUSD"), vec!["p03"]);
        assert_eq!(mgr.open_ids_by_side_sorted(Side::Buy), vec!["p10", "p20"]);
        assert_eq!(mgr.all_open_ids_sorted(), vec!["p01", "p10", "p20"]);
        assert_eq!(mgr.all_pending_ids_sorted(), vec!["p03"]);
        assert_eq!(
            mgr.all_group_ids_sorted()
                .into_iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["group-a", "group-z"]
        );
    }

    #[test]
    fn duplicate_trade_id_can_be_checked_before_add() {
        let mut mgr = PositionManager::new();
        let mut first = make_open("p1", "EURUSD", Side::Buy);
        first.set_trade_id(Some("trade-7".into()));
        mgr.add(first);

        let mut duplicate = make_open("p2", "EURUSD", Side::Sell);
        duplicate.set_trade_id(Some("trade-7".into()));
        assert_eq!(mgr.would_duplicate_trade_id(&duplicate), Some("p1".into()));

        let no_trade_id = make_open("p3", "EURUSD", Side::Buy);
        assert_eq!(mgr.would_duplicate_trade_id(&no_trade_id), None);

        let mut same_position = make_open("p1", "EURUSD", Side::Buy);
        same_position.set_trade_id(Some("trade-7".into()));
        assert_eq!(mgr.would_duplicate_trade_id(&same_position), None);
    }

    #[test]
    fn legacy_add_still_allows_duplicates_and_audit_is_sorted() {
        let mut mgr = PositionManager::new();
        for (position_id, trade_id) in [
            ("p1", "trade-z"),
            ("p2", "trade-a"),
            ("p3", "trade-z"),
            ("p4", "trade-a"),
            ("p5", "trade-ok"),
        ] {
            let mut position = make_open(position_id, "EURUSD", Side::Buy);
            position.set_trade_id(Some(trade_id.into()));
            mgr.add(position);
        }

        assert_eq!(mgr.len(), 5);
        assert_eq!(mgr.id_by_trade_id("trade-z"), Some("p3".into()));
        assert_eq!(mgr.duplicate_trade_ids(), vec!["trade-a", "trade-z"]);
    }

    #[test]
    fn checked_add_rejects_duplicate_trade_id_without_corrupting_index() {
        let mut mgr = PositionManager::new();
        let mut first = make_open("p1", "EURUSD", Side::Buy);
        first.set_trade_id(Some("trade-7".into()));
        mgr.add_checked(first).unwrap();

        let mut duplicate = make_open("p2", "XAUUSD", Side::Sell);
        duplicate.set_trade_id(Some("trade-7".into()));
        assert_eq!(
            mgr.add_checked(duplicate),
            Err(PositionManagerError::DuplicateTradeId {
                trade_id: "trade-7".into(),
                existing_position_id: "p1".into(),
            })
        );

        assert_eq!(mgr.len(), 1);
        assert!(mgr.get("p2").is_none());
        assert!(mgr.ids_for_symbol("XAUUSD").is_empty());
        assert_eq!(mgr.id_by_trade_id("trade-7"), Some("p1".into()));
    }

    #[test]
    fn checked_add_overwrite_cleans_old_secondary_indexes() {
        let mut mgr = PositionManager::new();
        let mut first = make_open("p1", "EURUSD", Side::Buy);
        first.set_trade_id(Some("trade-old".into()));
        mgr.add_checked(first).unwrap();

        let mut replacement = make_open("p1", "XAUUSD", Side::Sell);
        replacement.set_trade_id(Some("trade-new".into()));
        mgr.add_checked(replacement).unwrap();

        assert!(mgr.ids_for_symbol("EURUSD").is_empty());
        assert_eq!(mgr.ids_for_symbol("XAUUSD"), vec!["p1"]);
        assert_eq!(mgr.id_by_trade_id("trade-old"), None);
        assert_eq!(mgr.id_by_trade_id("trade-new"), Some("p1".into()));
    }

    #[test]
    fn checked_trade_id_assignment_is_atomic_and_allows_empty_duplicates() {
        let mut mgr = PositionManager::new();
        let mut first = make_open("p1", "EURUSD", Side::Buy);
        first.set_trade_id(Some("trade-7".into()));
        mgr.add_checked(first).unwrap();
        mgr.add_checked(make_open("p2", "EURUSD", Side::Sell))
            .unwrap();

        assert_eq!(
            mgr.set_trade_id_checked("p2", "trade-7".into()),
            Err(PositionManagerError::DuplicateTradeId {
                trade_id: "trade-7".into(),
                existing_position_id: "p1".into(),
            })
        );
        assert_eq!(mgr.get("p2").unwrap().data.trade_id, None);
        assert_eq!(mgr.id_by_trade_id("trade-7"), Some("p1".into()));

        mgr.set_trade_id_checked("p2", String::new()).unwrap();
        let mut third = make_open("p3", "EURUSD", Side::Buy);
        third.set_trade_id(Some(String::new()));
        mgr.add_checked(third).unwrap();
        assert_eq!(mgr.len(), 3);
    }
}
