//! Terminal action lifecycle accounting.
//!
//! A [`LifecycleLedger`] accepts only terminal dispositions and rejects a
//! second disposition for the same action id. Its custom deserializer applies
//! the same validation, so the one-terminal-record invariant also holds for
//! artifacts loaded from storage.

use std::error::Error;
use std::fmt;

use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer, Serialize};

/// Terminal result of applying an action to a runner/engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ActionDispositionStatus {
    /// The action was accepted and its effects were processed.
    #[default]
    Applied,
    /// The action was valid but intentionally had no effect.
    Skipped,
    /// Validation or current lifecycle state rejected the action.
    Rejected,
    /// An unexpected execution/integration error prevented application.
    Failed,
}

/// Exactly one terminal accounting record for one runner action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ActionDisposition {
    pub action_id: String,
    /// Stable caller-defined action category (for example, `open` or `close`).
    pub action_kind: Option<String>,
    pub signal_ts: Option<NaiveDateTime>,
    pub effective_ts: Option<NaiveDateTime>,
    pub status: ActionDispositionStatus,
    /// Machine-stable or human-readable reason. Usually absent for `Applied`.
    pub reason: Option<String>,
    /// Position ids affected by bulk actions are retained in deterministic order.
    pub position_ids: Vec<String>,
}

impl Default for ActionDisposition {
    fn default() -> Self {
        Self {
            action_id: String::new(),
            action_kind: None,
            signal_ts: None,
            effective_ts: None,
            status: ActionDispositionStatus::Applied,
            reason: None,
            position_ids: Vec::new(),
        }
    }
}

impl ActionDisposition {
    pub fn new(action_id: impl Into<String>, status: ActionDispositionStatus) -> Self {
        Self {
            action_id: action_id.into(),
            status,
            ..Self::default()
        }
    }

    pub fn applied(action_id: impl Into<String>) -> Self {
        Self::new(action_id, ActionDispositionStatus::Applied)
    }

    pub fn skipped(action_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::with_reason(action_id, ActionDispositionStatus::Skipped, reason)
    }

    pub fn rejected(action_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::with_reason(action_id, ActionDispositionStatus::Rejected, reason)
    }

    pub fn failed(action_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::with_reason(action_id, ActionDispositionStatus::Failed, reason)
    }

    fn with_reason(
        action_id: impl Into<String>,
        status: ActionDispositionStatus,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            action_id: action_id.into(),
            status,
            reason: Some(reason.into()),
            ..Self::default()
        }
    }

    /// All statuses represented by this type are terminal by construction.
    pub const fn is_terminal(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    EmptyActionId,
    DuplicateTerminalRecord { action_id: String },
}

impl fmt::Display for LedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyActionId => write!(formatter, "action id must not be empty"),
            Self::DuplicateTerminalRecord { action_id } => {
                write!(
                    formatter,
                    "action '{action_id}' already has a terminal record"
                )
            }
        }
    }
}

impl Error for LedgerError {}

/// Ordered terminal action records with a uniqueness invariant on `action_id`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LifecycleLedger {
    records: Vec<ActionDisposition>,
}

impl LifecycleLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_records(
        records: impl IntoIterator<Item = ActionDisposition>,
    ) -> Result<Self, LedgerError> {
        let mut ledger = Self::new();
        for record in records {
            ledger.record(record)?;
        }
        Ok(ledger)
    }

    /// Append a terminal disposition, rejecting empty or previously finalized ids.
    pub fn record(&mut self, disposition: ActionDisposition) -> Result<(), LedgerError> {
        if disposition.action_id.is_empty() {
            return Err(LedgerError::EmptyActionId);
        }
        if self.contains(&disposition.action_id) {
            return Err(LedgerError::DuplicateTerminalRecord {
                action_id: disposition.action_id,
            });
        }
        self.records.push(disposition);
        Ok(())
    }

    pub fn contains(&self, action_id: &str) -> bool {
        self.records
            .iter()
            .any(|record| record.action_id == action_id)
    }

    pub fn get(&self, action_id: &str) -> Option<&ActionDisposition> {
        self.records
            .iter()
            .find(|record| record.action_id == action_id)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ActionDisposition> {
        self.records.iter()
    }

    pub fn as_slice(&self) -> &[ActionDisposition] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn count(&self, status: ActionDispositionStatus) -> usize {
        self.records
            .iter()
            .filter(|record| record.status == status)
            .count()
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct LifecycleLedgerWire {
    records: Vec<ActionDisposition>,
}

impl<'de> Deserialize<'de> for LifecycleLedger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LifecycleLedgerWire::deserialize(deserializer)?;
        Self::from_records(wire.records).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap()
    }

    #[test]
    fn records_exactly_one_terminal_disposition_per_action() {
        let mut ledger = LifecycleLedger::new();
        let mut applied = ActionDisposition::applied("action-1");
        applied.action_kind = Some("open".into());
        applied.signal_ts = Some(ts());
        applied.effective_ts = Some(ts());
        applied.position_ids.push("position-1".into());

        ledger.record(applied).unwrap();
        let duplicate = ledger.record(ActionDisposition::rejected("action-1", "too late"));

        assert_eq!(
            duplicate,
            Err(LedgerError::DuplicateTerminalRecord {
                action_id: "action-1".into()
            })
        );
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger.get("action-1").unwrap().position_ids, ["position-1"]);
    }

    #[test]
    fn rejects_empty_action_ids() {
        let mut ledger = LifecycleLedger::new();
        assert_eq!(
            ledger.record(ActionDisposition::applied("")),
            Err(LedgerError::EmptyActionId)
        );
        assert!(ledger.is_empty());
    }

    #[test]
    fn status_helpers_capture_reasons_and_counts() {
        let mut ledger = LifecycleLedger::new();
        ledger.record(ActionDisposition::applied("a")).unwrap();
        ledger
            .record(ActionDisposition::skipped("b", "no matching position"))
            .unwrap();
        ledger
            .record(ActionDisposition::rejected("c", "invalid size"))
            .unwrap();
        ledger
            .record(ActionDisposition::failed("d", "adapter failure"))
            .unwrap();

        assert_eq!(ledger.count(ActionDispositionStatus::Applied), 1);
        assert_eq!(ledger.count(ActionDispositionStatus::Skipped), 1);
        assert_eq!(ledger.count(ActionDispositionStatus::Rejected), 1);
        assert_eq!(ledger.count(ActionDispositionStatus::Failed), 1);
        assert_eq!(
            ledger.get("c").unwrap().reason.as_deref(),
            Some("invalid size")
        );
        assert!(ledger.iter().all(ActionDisposition::is_terminal));
    }

    #[test]
    fn serde_roundtrip_preserves_order() {
        let ledger = LifecycleLedger::from_records([
            ActionDisposition::applied("z"),
            ActionDisposition::rejected("a", "bad request"),
        ])
        .unwrap();
        let json = serde_json::to_string(&ledger).unwrap();
        let decoded: LifecycleLedger = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, ledger);
        let ids: Vec<_> = decoded
            .iter()
            .map(|record| record.action_id.as_str())
            .collect();
        assert_eq!(ids, ["z", "a"]);
    }

    #[test]
    fn deserialization_cannot_bypass_uniqueness_invariant() {
        let json = r#"{
            "records": [
                {"action_id":"a","status":"applied"},
                {"action_id":"a","status":"rejected","reason":"duplicate"}
            ]
        }"#;
        let error = serde_json::from_str::<LifecycleLedger>(json).unwrap_err();
        assert!(error.to_string().contains("already has a terminal record"));
    }

    #[test]
    fn additive_disposition_fields_have_serde_defaults() {
        let disposition: ActionDisposition = serde_json::from_str(r#"{"action_id":"a"}"#).unwrap();
        assert_eq!(disposition.status, ActionDispositionStatus::Applied);
        assert_eq!(disposition.reason, None);
        assert!(disposition.position_ids.is_empty());

        let ledger: LifecycleLedger = serde_json::from_str("{}").unwrap();
        assert!(ledger.is_empty());
    }
}
