//! Management profiles — decouple entry signals from trade management.
//!
//! A [`ManagementProfile`] transforms raw signals ([`RawSignalEntry`]) into
//! fully-specified [`Action::Open`] calls.  Profiles are defined in TOML and
//! loaded via [`ProfileRegistry`], enabling same-signals-different-management
//! comparison without recompilation.

use std::collections::HashMap;
use std::path::Path;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use qs_core::types::{
    Action, GroupId, OrderType, PositionId, RuleConfig, Side, Signal, TargetSpec,
};

// ─── Errors ─────────────────────────────────────────────────────────────────

/// Errors that can occur when loading or validating management profiles.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("Failed to read profile file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse profile TOML: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Duplicate profile name: '{0}'")]
    DuplicateName(String),

    #[error(
        "Profile '{profile}': use_targets length ({targets}) does not match close_ratios length ({ratios})"
    )]
    TargetRatioMismatch {
        profile: String,
        targets: usize,
        ratios: usize,
    },

    #[error("Profile '{profile}': close_ratios sum to {sum:.4}, which exceeds 1.0")]
    RatioSumExceeded { profile: String, sum: f64 },

    #[error("Profile '{profile}': close_ratios contains a value <= 0.0")]
    ZeroRatio { profile: String },

    #[error("Profile '{profile}': use_targets contains a 0 index (must be 1-indexed)")]
    ZeroTargetIndex { profile: String },

    #[error("Profile not found: '{0}'")]
    NotFound(String),
}

// ─── RawSignalEntry ─────────────────────────────────────────────────────────

/// A raw trade signal from an external source.
///
/// Contains the entry decision and suggested price levels.
/// The management profile decides how to use these levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawSignalEntry {
    /// Timestamp of the signal.
    pub ts: NaiveDateTime,
    /// Symbol to trade.
    pub symbol: String,
    /// Trade direction.
    pub side: Side,
    /// Order type (Market, Limit, Stop).
    pub order_type: OrderType,
    /// Entry price (None for market orders that use current quote).
    pub price: Option<f64>,
    /// Position size.
    pub size: f64,
    /// Suggested stoploss price.
    pub stoploss: Option<f64>,
    /// Suggested target prices (ordered by distance from entry).
    pub targets: Vec<f64>,
    /// Optional group/source tag.
    #[serde(default)]
    pub group: Option<String>,
}

// ─── PositionRef ────────────────────────────────────────────────────────────

/// How a management signal references its target position(s).
///
/// Resolved at runtime by the backtest runner, which has access to
/// engine state for lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PositionRef {
    /// Explicit position ID (for strategy mode or known IDs).
    Id { id: PositionId },
    /// The most recently opened position on this symbol.
    LastOnSymbol { symbol: String },
    /// The most recently opened position in this group.
    LastInGroup { group_id: GroupId },
    /// All open positions on this symbol.
    AllOnSymbol { symbol: String },
    /// All open positions in this group.
    AllInGroup { group_id: GroupId },
}

// ─── RawSignal ──────────────────────────────────────────────────────────────

/// A raw signal from an external source — entry or management.
///
/// Covers both entry signals (which can be profile-transformed) and
/// management signals (which pass through to the engine as-is after
/// position resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum RawSignal {
    // ── Entry (profile-transformable) ───────────────────────────────
    Entry {
        ts: NaiveDateTime,
        symbol: String,
        side: Side,
        order_type: OrderType,
        price: Option<f64>,
        size: f64,
        stoploss: Option<f64>,
        #[serde(default)]
        targets: Vec<f64>,
        #[serde(default)]
        group: Option<String>,
    },

    // ── Per-position management ─────────────────────────────────────
    Close {
        ts: NaiveDateTime,
        position: PositionRef,
    },
    ClosePartial {
        ts: NaiveDateTime,
        position: PositionRef,
        ratio: f64,
    },
    ModifyStoploss {
        ts: NaiveDateTime,
        position: PositionRef,
        price: f64,
    },
    MoveStoplossToEntry {
        ts: NaiveDateTime,
        position: PositionRef,
    },
    AddTarget {
        ts: NaiveDateTime,
        position: PositionRef,
        price: f64,
        close_ratio: f64,
    },
    RemoveTarget {
        ts: NaiveDateTime,
        position: PositionRef,
        price: f64,
    },
    AddRule {
        ts: NaiveDateTime,
        position: PositionRef,
        rule: RuleConfigDef,
    },
    RemoveRule {
        ts: NaiveDateTime,
        position: PositionRef,
        rule_name: String,
    },
    ScaleIn {
        ts: NaiveDateTime,
        position: PositionRef,
        price: Option<f64>,
        size: f64,
    },
    CancelPending {
        ts: NaiveDateTime,
        position: PositionRef,
    },

    // ── Bulk actions ────────────────────────────────────────────────
    CloseAllOf {
        ts: NaiveDateTime,
        symbol: String,
    },
    CloseAll {
        ts: NaiveDateTime,
    },
    CancelAllPending {
        ts: NaiveDateTime,
    },
    ModifyAllStoploss {
        ts: NaiveDateTime,
        symbol: String,
        price: f64,
    },
    CloseAllInGroup {
        ts: NaiveDateTime,
        group_id: GroupId,
    },
    ModifyAllStoplossInGroup {
        ts: NaiveDateTime,
        group_id: GroupId,
        price: f64,
    },
}

impl RawSignal {
    /// Extract the timestamp from any signal variant.
    pub fn ts(&self) -> NaiveDateTime {
        match self {
            Self::Entry { ts, .. } => *ts,
            Self::Close { ts, .. } => *ts,
            Self::ClosePartial { ts, .. } => *ts,
            Self::ModifyStoploss { ts, .. } => *ts,
            Self::MoveStoplossToEntry { ts, .. } => *ts,
            Self::AddTarget { ts, .. } => *ts,
            Self::RemoveTarget { ts, .. } => *ts,
            Self::AddRule { ts, .. } => *ts,
            Self::RemoveRule { ts, .. } => *ts,
            Self::ScaleIn { ts, .. } => *ts,
            Self::CancelPending { ts, .. } => *ts,
            Self::CloseAllOf { ts, .. } => *ts,
            Self::CloseAll { ts, .. } => *ts,
            Self::CancelAllPending { ts, .. } => *ts,
            Self::ModifyAllStoploss { ts, .. } => *ts,
            Self::CloseAllInGroup { ts, .. } => *ts,
            Self::ModifyAllStoplossInGroup { ts, .. } => *ts,
        }
    }

    /// Returns `true` if this is an `Entry` variant.
    pub fn is_entry(&self) -> bool {
        matches!(self, Self::Entry { .. })
    }

    /// Convert to a `RawSignalEntry` if this is an `Entry` variant.
    pub fn as_entry(&self) -> Option<RawSignalEntry> {
        match self {
            Self::Entry {
                ts,
                symbol,
                side,
                order_type,
                price,
                size,
                stoploss,
                targets,
                group,
            } => Some(RawSignalEntry {
                ts: *ts,
                symbol: symbol.clone(),
                side: *side,
                order_type: *order_type,
                price: *price,
                size: *size,
                stoploss: *stoploss,
                targets: targets.clone(),
                group: group.clone(),
            }),
            _ => None,
        }
    }
}

impl From<RawSignalEntry> for RawSignal {
    fn from(e: RawSignalEntry) -> Self {
        RawSignal::Entry {
            ts: e.ts,
            symbol: e.symbol,
            side: e.side,
            order_type: e.order_type,
            price: e.price,
            size: e.size,
            stoploss: e.stoploss,
            targets: e.targets,
            group: e.group,
        }
    }
}

// ─── Position Resolution ────────────────────────────────────────────────────

/// Resolves a `PositionRef` to concrete position ID(s) using engine state.
pub trait PositionResolver {
    /// Resolve a position reference to zero or more concrete position IDs.
    fn resolve(&self, pr: &PositionRef) -> Vec<PositionId>;
    /// Get entry info (average_entry, side) for an open position.
    fn position_entry_info(&self, id: &PositionId) -> Option<(f64, Side)>;
}

/// Resolve a non-entry `RawSignal` into concrete `Action`(s).
///
/// Entry signals are not handled here — they go through the profile path.
/// Returns an empty vec for `Entry` variants.
pub fn resolve_signal(signal: &RawSignal, resolver: &impl PositionResolver) -> Vec<Action> {
    match signal {
        RawSignal::Entry { .. } => vec![],

        RawSignal::Close { position, .. } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::ClosePosition { position_id: id })
            .collect(),

        RawSignal::ClosePartial {
            position, ratio, ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::ClosePartial {
                position_id: id,
                ratio: *ratio,
            })
            .collect(),

        RawSignal::ModifyStoploss {
            position, price, ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::ModifyStoploss {
                position_id: id,
                price: *price,
            })
            .collect(),

        RawSignal::MoveStoplossToEntry { position, .. } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::MoveStoplossToEntry { position_id: id })
            .collect(),

        RawSignal::AddTarget {
            position,
            price,
            close_ratio,
            ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::AddTarget {
                position_id: id,
                price: *price,
                close_ratio: *close_ratio,
            })
            .collect(),

        RawSignal::RemoveTarget {
            position, price, ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::RemoveTarget {
                position_id: id,
                price: *price,
            })
            .collect(),

        RawSignal::AddRule { position, rule, .. } => {
            resolver
                .resolve(position)
                .into_iter()
                .filter_map(|id| {
                    let info = resolver.position_entry_info(&id);
                    let (entry_price, side) = match info {
                        Some((ep, s)) => (Some(ep), s),
                        None => (None, Side::Buy), // fallback side; resolve may return None
                    };
                    rule.resolve(entry_price, side)
                        .map(|resolved_rule| Action::AddRule {
                            position_id: id,
                            rule: resolved_rule,
                        })
                })
                .collect()
        }

        RawSignal::RemoveRule {
            position,
            rule_name,
            ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::RemoveRule {
                position_id: id,
                rule_name: rule_name.clone(),
            })
            .collect(),

        RawSignal::ScaleIn {
            position,
            price,
            size,
            ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::ScaleIn {
                position_id: id,
                price: *price,
                size: *size,
            })
            .collect(),

        RawSignal::CancelPending { position, .. } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::CancelPending { position_id: id })
            .collect(),

        // ── Bulk actions — no resolution needed ─────────────────────
        RawSignal::CloseAllOf { symbol, .. } => {
            vec![Action::CloseAllOf {
                symbol: symbol.clone(),
            }]
        }
        RawSignal::CloseAll { .. } => {
            vec![Action::CloseAll]
        }
        RawSignal::CancelAllPending { .. } => {
            vec![Action::CancelAllPending]
        }
        RawSignal::ModifyAllStoploss { symbol, price, .. } => {
            vec![Action::ModifyAllStoploss {
                symbol: symbol.clone(),
                price: *price,
            }]
        }
        RawSignal::CloseAllInGroup { group_id, .. } => {
            vec![Action::CloseAllInGroup {
                group_id: group_id.clone(),
            }]
        }
        RawSignal::ModifyAllStoplossInGroup {
            group_id, price, ..
        } => {
            vec![Action::ModifyAllStoplossInGroup {
                group_id: group_id.clone(),
                price: *price,
            }]
        }
    }
}

// ─── StoplossMode ───────────────────────────────────────────────────────────

/// How the profile handles the stoploss from the raw signal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StoplossMode {
    /// Use the stoploss price from the signal as-is.
    FromSignal,
    /// No fixed stoploss (rely on trailing stop or time exit instead).
    None,
    /// Override with a fixed distance from entry price.
    FixedDistance { distance: f64 },
    /// Override with a specific absolute price.
    FixedPrice { price: f64 },
}

// ─── TOML-friendly rule definition ──────────────────────────────────────────

/// Profile-specific rule definition with `#[serde(tag = "type")]` for TOML.
///
/// Converts to the core `RuleConfig` enum. Includes an offset-based
/// `BreakevenWhenOffset` variant that computes the absolute trigger price
/// from the signal's entry price at apply time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RuleConfigDef {
    /// Fixed stoploss at an absolute price.
    FixedStoploss { price: f64 },
    /// Trailing stop with a fixed distance.
    TrailingStop { distance: f64 },
    /// Take profit at an absolute price with a close ratio.
    TakeProfit { price: f64, close_ratio: f64 },
    /// Breakeven trigger at an absolute price.
    BreakevenWhen { trigger_price: f64 },
    /// Breakeven trigger as an offset from the entry price (profile-specific).
    BreakevenWhenOffset { trigger_price_offset: f64 },
    /// Breakeven after N targets have been hit.
    BreakevenAfterTargets { after_n: u32 },
    /// Time-based exit after N seconds.
    TimeExit { max_seconds: u64 },
}

impl RuleConfigDef {
    /// Resolve this definition into a core `RuleConfig`.
    ///
    /// For offset-based variants, `entry_price` and `side` are needed
    /// to compute the absolute trigger price. Returns `None` when the
    /// offset variant is used but no entry price is available.
    pub fn resolve(&self, entry_price: Option<f64>, side: Side) -> Option<RuleConfig> {
        match self {
            Self::FixedStoploss { price } => Some(RuleConfig::FixedStoploss { price: *price }),
            Self::TrailingStop { distance } => Some(RuleConfig::TrailingStop {
                distance: *distance,
            }),
            Self::TakeProfit { price, close_ratio } => Some(RuleConfig::TakeProfit {
                price: *price,
                close_ratio: *close_ratio,
            }),
            Self::BreakevenWhen { trigger_price } => Some(RuleConfig::BreakevenWhen {
                trigger_price: *trigger_price,
            }),
            Self::BreakevenWhenOffset {
                trigger_price_offset,
            } => {
                let entry = entry_price?;
                let trigger = match side {
                    Side::Buy => entry + trigger_price_offset,
                    Side::Sell => entry - trigger_price_offset,
                };
                Some(RuleConfig::BreakevenWhen {
                    trigger_price: trigger,
                })
            }
            Self::BreakevenAfterTargets { after_n } => {
                Some(RuleConfig::BreakevenAfterTargets { after_n: *after_n })
            }
            Self::TimeExit { max_seconds } => Some(RuleConfig::TimeExit {
                max_seconds: *max_seconds,
            }),
        }
    }
}

// ─── ManagementProfile ──────────────────────────────────────────────────────

/// A named management profile that transforms raw signals into Actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementProfile {
    /// Profile name (e.g. "conservative", "aggressive", "runner").
    pub name: String,

    /// Which targets from the signal to use (1-indexed).
    pub use_targets: Vec<usize>,

    /// Close ratio for each used target. Must match `use_targets` length.
    pub close_ratios: Vec<f64>,

    /// How to handle the stoploss from the signal.
    #[serde(default = "default_stoploss_mode")]
    pub stoploss_mode: StoplossMode,

    /// Additional rules to attach to every position opened with this profile.
    #[serde(default)]
    pub rules: Vec<RuleConfigDef>,

    /// If set, override the signal's group tag with this value.
    #[serde(default)]
    pub group_override: Option<String>,

    /// When true and ratios sum < 1.0, the remainder rides with just SL/rules.
    #[serde(default)]
    pub let_remainder_run: bool,
}

fn default_stoploss_mode() -> StoplossMode {
    StoplossMode::FromSignal
}

impl ManagementProfile {
    /// Validate this profile's configuration.
    pub fn validate(&self) -> Result<(), ProfileError> {
        ProfileRegistry::validate_profile(self)
    }

    /// Transform a raw signal into a fully-specified `Action::Open`.
    pub fn apply(&self, signal: &RawSignalEntry) -> Option<Action> {
        // 1. Build target specs from selected targets + close ratios.
        let mut targets = Vec::new();
        for (i, &target_idx) in self.use_targets.iter().enumerate() {
            if let Some(&target_price) = signal.targets.get(target_idx - 1) {
                let ratio = self.close_ratios.get(i).copied().unwrap_or(0.0);
                if ratio > 0.0 {
                    targets.push(TargetSpec {
                        price: target_price,
                        close_ratio: ratio,
                    });
                }
            }
            // If signal doesn't have this target index, skip silently.
        }

        // 2. Determine stoploss.
        let stoploss = match &self.stoploss_mode {
            StoplossMode::FromSignal => signal.stoploss,
            StoplossMode::None => Option::None,
            StoplossMode::FixedDistance { distance } => {
                signal.price.map(|entry| match signal.side {
                    Side::Buy => entry - distance,
                    Side::Sell => entry + distance,
                })
            }
            StoplossMode::FixedPrice { price } => Some(*price),
        };

        // 3. Build rules (resolve offset-based variants using entry price).
        let rules: Vec<RuleConfig> = self
            .rules
            .iter()
            .filter_map(|def| def.resolve(signal.price, signal.side))
            .collect();

        // 4. Determine group (profile override takes precedence).
        let group: Option<GroupId> = self.group_override.clone().or(signal.group.clone());

        // 5. Construct the action.
        Some(Action::Open {
            symbol: signal.symbol.clone(),
            side: signal.side,
            order_type: signal.order_type,
            price: signal.price,
            size: signal.size,
            stoploss,
            targets,
            rules,
            group,
        })
    }

    /// Transform a batch of raw signals into `Signal`s for `BacktestRunner::run_signals()`.
    pub fn apply_batch(&self, raw_signals: &[RawSignalEntry]) -> Vec<Signal> {
        raw_signals
            .iter()
            .filter_map(|raw| self.apply(raw).map(|action| Signal { ts: raw.ts, action }))
            .collect()
    }

    /// Transform a `RawSignal` — entry signals are profile-transformed,
    /// management signals pass through unchanged.
    pub fn apply_raw(&self, signal: &RawSignal) -> Option<RawSignal> {
        match signal {
            RawSignal::Entry { .. } => {
                // Convert to RawSignalEntry, apply profile, then wrap back.
                // If apply() returns None we filter it out.
                let entry = signal.as_entry()?;
                let _action = self.apply(&entry)?;
                // Profile accepted it — return the original entry signal
                // (the actual Action conversion happens later in the runner).
                Some(signal.clone())
            }
            _ => Some(signal.clone()),
        }
    }

    /// Transform a batch of `RawSignal`s — entry signals are profile-filtered,
    /// management signals pass through unchanged.
    pub fn apply_batch_raw(&self, raw_signals: &[RawSignal]) -> Vec<RawSignal> {
        raw_signals
            .iter()
            .filter_map(|s| self.apply_raw(s))
            .collect()
    }
}

// ─── ProfileRegistry ────────────────────────────────────────────────────────

/// TOML document structure: `[[profile]]` array.
#[derive(Debug, Deserialize)]
struct ProfileFile {
    profile: Vec<ManagementProfile>,
}

/// A collection of named management profiles loaded from TOML.
pub struct ProfileRegistry {
    profiles: HashMap<String, ManagementProfile>,
}

impl ProfileRegistry {
    /// Load profiles from a TOML file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProfileError> {
        let content = std::fs::read_to_string(path)?;
        Self::from_toml(&content)
    }

    /// Load profiles from a TOML string.
    pub fn from_toml(content: &str) -> Result<Self, ProfileError> {
        let file: ProfileFile = toml::from_str(content)?;
        let mut profiles = HashMap::new();

        for p in file.profile {
            // Validate before inserting.
            Self::validate(&p)?;

            if profiles.contains_key(&p.name) {
                return Err(ProfileError::DuplicateName(p.name.clone()));
            }
            profiles.insert(p.name.clone(), p);
        }

        Ok(Self { profiles })
    }

    /// Create an empty registry.
    pub fn empty() -> Self {
        Self {
            profiles: HashMap::new(),
        }
    }

    /// Get a profile by name.
    pub fn get(&self, name: &str) -> Option<&ManagementProfile> {
        self.profiles.get(name)
    }

    /// List all profile names (sorted).
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.profiles.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }

    /// Number of registered profiles.
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Validate a single profile's configuration (public static method).
    pub fn validate_profile(p: &ManagementProfile) -> Result<(), ProfileError> {
        // use_targets and close_ratios must be same length.
        if p.use_targets.len() != p.close_ratios.len() {
            return Err(ProfileError::TargetRatioMismatch {
                profile: p.name.clone(),
                targets: p.use_targets.len(),
                ratios: p.close_ratios.len(),
            });
        }

        // No zero-index in use_targets (1-indexed).
        if p.use_targets.iter().any(|&idx| idx == 0) {
            return Err(ProfileError::ZeroTargetIndex {
                profile: p.name.clone(),
            });
        }

        // All close_ratios must be > 0.0.
        if p.close_ratios.iter().any(|&r| r <= 0.0) {
            return Err(ProfileError::ZeroRatio {
                profile: p.name.clone(),
            });
        }

        // Sum of close_ratios must not exceed 1.0.
        let sum: f64 = p.close_ratios.iter().sum();
        if sum > 1.0 + f64::EPSILON {
            return Err(ProfileError::RatioSumExceeded {
                profile: p.name.clone(),
                sum,
            });
        }

        Ok(())
    }

    /// Validate a single profile at load time (delegates to public method).
    fn validate(p: &ManagementProfile) -> Result<(), ProfileError> {
        Self::validate_profile(p)
    }

    /// Insert a profile into the registry. If `overwrite` is false, returns
    /// an error when a profile with the same name already exists.
    pub fn insert(
        &mut self,
        profile: ManagementProfile,
        overwrite: bool,
    ) -> Result<(), ProfileError> {
        Self::validate_profile(&profile)?;
        if !overwrite && self.profiles.contains_key(&profile.name) {
            return Err(ProfileError::DuplicateName(profile.name.clone()));
        }
        self.profiles.insert(profile.name.clone(), profile);
        Ok(())
    }

    /// Remove a profile by name. Returns `true` if the profile existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.profiles.remove(name).is_some()
    }
}

impl std::fmt::Debug for ProfileRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileRegistry")
            .field("count", &self.profiles.len())
            .field("names", &self.names())
            .finish()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use qs_core::types::{CloseReason, OrderType, PositionId, Side, TargetSpec};

    // ── Helpers ─────────────────────────────────────────────────────────

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    /// Convenience: build a standard Buy signal with 2 targets.
    fn buy_signal() -> RawSignalEntry {
        RawSignalEntry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900, 1.0950],
            group: None,
        }
    }

    /// Convenience: build a standard Sell signal with 2 targets.
    fn sell_signal() -> RawSignalEntry {
        RawSignalEntry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0900),
            targets: vec![1.0800, 1.0750],
            group: None,
        }
    }

    /// Extract fields from an Action::Open, panicking if it isn't Open.
    fn unwrap_open(
        action: &Action,
    ) -> (
        &str,
        Side,
        OrderType,
        Option<f64>,
        f64,
        Option<f64>,
        &[TargetSpec],
        &[RuleConfig],
        &Option<String>,
    ) {
        match action {
            Action::Open {
                symbol,
                side,
                order_type,
                price,
                size,
                stoploss,
                targets,
                rules,
                group,
            } => (
                symbol.as_str(),
                *side,
                *order_type,
                *price,
                *size,
                *stoploss,
                targets.as_slice(),
                rules.as_slice(),
                group,
            ),
            _ => panic!("Expected Action::Open"),
        }
    }

    // ── ProfileRegistry tests ───────────────────────────────────────────

    #[test]
    fn load_from_toml_string() {
        let toml = r#"
[[profile]]
name = "basic"
use_targets = [1]
close_ratios = [1.0]
stoploss_mode = { type = "FromSignal" }
let_remainder_run = false
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        assert_eq!(reg.len(), 1);
        assert!(reg.get("basic").is_some());
    }

    #[test]
    fn load_multiple_profiles() {
        let toml = r#"
[[profile]]
name = "a"
use_targets = [1]
close_ratios = [1.0]

[[profile]]
name = "b"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]

[[profile]]
name = "c"
use_targets = []
close_ratios = []
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        assert_eq!(reg.len(), 3);
        assert!(reg.get("a").is_some());
        assert!(reg.get("b").is_some());
        assert!(reg.get("c").is_some());
    }

    #[test]
    fn duplicate_name_error() {
        let toml = r#"
[[profile]]
name = "dup"
use_targets = [1]
close_ratios = [1.0]

[[profile]]
name = "dup"
use_targets = [1]
close_ratios = [1.0]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(
            matches!(err, ProfileError::DuplicateName(ref n) if n == "dup"),
            "Expected DuplicateName, got: {err:?}"
        );
    }

    #[test]
    fn target_ratio_mismatch_error() {
        let toml = r#"
[[profile]]
name = "bad"
use_targets = [1, 2]
close_ratios = [0.5]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, ProfileError::TargetRatioMismatch { .. }));
    }

    #[test]
    fn ratio_sum_exceeded_error() {
        let toml = r#"
[[profile]]
name = "bad"
use_targets = [1, 2]
close_ratios = [0.6, 0.6]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, ProfileError::RatioSumExceeded { .. }));
    }

    #[test]
    fn zero_ratio_error() {
        let toml = r#"
[[profile]]
name = "bad"
use_targets = [1]
close_ratios = [0.0]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, ProfileError::ZeroRatio { .. }));
    }

    #[test]
    fn zero_target_index_error() {
        let toml = r#"
[[profile]]
name = "bad"
use_targets = [0]
close_ratios = [1.0]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, ProfileError::ZeroTargetIndex { .. }));
    }

    #[test]
    fn empty_registry() {
        let reg = ProfileRegistry::empty();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        assert!(reg.get("anything").is_none());
    }

    #[test]
    fn names_list_sorted() {
        let toml = r#"
[[profile]]
name = "charlie"
use_targets = []
close_ratios = []

[[profile]]
name = "alpha"
use_targets = []
close_ratios = []

[[profile]]
name = "bravo"
use_targets = []
close_ratios = []
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        assert_eq!(reg.names(), vec!["alpha", "bravo", "charlie"]);
    }

    // ── ManagementProfile::apply() tests ────────────────────────────────

    #[test]
    fn apply_conservative_single_target() {
        let toml = r#"
[[profile]]
name = "conservative"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("conservative").unwrap();

        let signal = buy_signal();
        let action = profile.apply(&signal).unwrap();
        let (sym, side, _, price, size, sl, targets, _, _) = unwrap_open(&action);

        assert_eq!(sym, "eurusd");
        assert_eq!(side, Side::Buy);
        assert_eq!(price, Some(1.0850));
        assert_eq!(size, 1.0);
        assert_eq!(sl, Some(1.0800));
        assert_eq!(targets.len(), 1);
        assert!((targets[0].price - 1.0900).abs() < f64::EPSILON);
        assert!((targets[0].close_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn apply_aggressive_both_targets() {
        let toml = r#"
[[profile]]
name = "aggressive"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("aggressive").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, targets, _, _) = unwrap_open(&action);

        assert_eq!(targets.len(), 2);
        assert!((targets[0].price - 1.0900).abs() < f64::EPSILON);
        assert!((targets[0].close_ratio - 0.5).abs() < f64::EPSILON);
        assert!((targets[1].price - 1.0950).abs() < f64::EPSILON);
        assert!((targets[1].close_ratio - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn apply_runner_partial_target() {
        let toml = r#"
[[profile]]
name = "runner"
use_targets = [1]
close_ratios = [0.3]
let_remainder_run = true
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("runner").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, targets, _, _) = unwrap_open(&action);

        assert_eq!(targets.len(), 1);
        assert!((targets[0].close_ratio - 0.3).abs() < f64::EPSILON);
        assert!(profile.let_remainder_run);
    }

    #[test]
    fn apply_stoploss_from_signal() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
stoploss_mode = { type = "FromSignal" }
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        assert_eq!(sl, Some(1.0800));
    }

    #[test]
    fn apply_stoploss_none() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
stoploss_mode = { type = "None" }
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        assert_eq!(sl, None);
    }

    #[test]
    fn apply_stoploss_fixed_distance_buy() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[profile.stoploss_mode]
type = "FixedDistance"
distance = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        // Buy at 1.0850, distance 0.0020 → SL at 1.0830
        assert!((sl.unwrap() - 1.0830).abs() < 1e-10);
    }

    #[test]
    fn apply_stoploss_fixed_distance_sell() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[profile.stoploss_mode]
type = "FixedDistance"
distance = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&sell_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        // Sell at 1.0850, distance 0.0020 → SL at 1.0870
        assert!((sl.unwrap() - 1.0870).abs() < 1e-10);
    }

    #[test]
    fn apply_stoploss_fixed_price() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[profile.stoploss_mode]
type = "FixedPrice"
price = 1.0780
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        assert!((sl.unwrap() - 1.0780).abs() < f64::EPSILON);
    }

    #[test]
    fn apply_with_breakeven_after_targets_rule() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]

[[profile.rules]]
type = "BreakevenAfterTargets"
after_n = 1

[[profile.rules]]
type = "TrailingStop"
distance = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        assert_eq!(rules.len(), 2);
        assert!(matches!(
            rules[0],
            RuleConfig::BreakevenAfterTargets { after_n: 1 }
        ));
        assert!(matches!(
            rules[1],
            RuleConfig::TrailingStop { distance } if (distance - 0.0020).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn apply_group_override() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []
group_override = "scalp"
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.group = Some("momentum".into());

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, _, _, _, _, _, group) = unwrap_open(&action);
        // Profile override takes precedence.
        assert_eq!(group.as_deref(), Some("scalp"));
    }

    #[test]
    fn apply_group_from_signal() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.group = Some("momentum".into());

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, _, _, _, _, _, group) = unwrap_open(&action);
        assert_eq!(group.as_deref(), Some("momentum"));
    }

    #[test]
    fn apply_group_both_none() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, _, group) = unwrap_open(&action);
        assert!(group.is_none());
    }

    #[test]
    fn apply_missing_target_index() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [3]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        // Signal only has 2 targets, profile asks for 3rd → empty targets.
        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, targets, _, _) = unwrap_open(&action);
        assert!(targets.is_empty());
    }

    #[test]
    fn apply_no_targets() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, targets, _, _) = unwrap_open(&action);
        assert!(targets.is_empty());
    }

    #[test]
    fn apply_market_order_no_price() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.price = None;

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, price, _, _, _, _, _) = unwrap_open(&action);
        assert_eq!(price, None);
    }

    #[test]
    fn apply_limit_order_with_price() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.order_type = OrderType::Limit;
        signal.price = Some(1.0800);

        let action = profile.apply(&signal).unwrap();
        let (_, _, ot, price, _, _, _, _, _) = unwrap_open(&action);
        assert_eq!(ot, OrderType::Limit);
        assert_eq!(price, Some(1.0800));
    }

    #[test]
    fn apply_fixed_distance_no_price_returns_none_sl() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []

[profile.stoploss_mode]
type = "FixedDistance"
distance = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.price = None;

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        // No entry price → can't compute SL from distance.
        assert_eq!(sl, None);
    }

    // ── BreakevenWhenOffset tests ───────────────────────────────────────

    #[test]
    fn breakeven_offset_buy() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[[profile.rules]]
type = "BreakevenWhenOffset"
trigger_price_offset = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        assert_eq!(rules.len(), 1);
        match &rules[0] {
            RuleConfig::BreakevenWhen { trigger_price } => {
                // Buy at 1.0850 + offset 0.0020 → trigger at 1.0870.
                assert!(
                    (trigger_price - 1.0870).abs() < 1e-10,
                    "Expected ~1.0870, got {trigger_price}"
                );
            }
            other => panic!("Expected BreakevenWhen, got {other:?}"),
        }
    }

    #[test]
    fn breakeven_offset_sell() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[[profile.rules]]
type = "BreakevenWhenOffset"
trigger_price_offset = 2.0
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = sell_signal();
        signal.price = Some(2010.0);

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        match &rules[0] {
            RuleConfig::BreakevenWhen { trigger_price } => {
                // Sell at 2010.0 - offset 2.0 → trigger at 2008.0.
                assert!(
                    (trigger_price - 2008.0).abs() < 1e-10,
                    "Expected ~2008.0, got {trigger_price}"
                );
            }
            other => panic!("Expected BreakevenWhen, got {other:?}"),
        }
    }

    #[test]
    fn breakeven_offset_no_entry_price_skips_rule() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = []
close_ratios = []

[[profile.rules]]
type = "BreakevenWhenOffset"
trigger_price_offset = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let mut signal = buy_signal();
        signal.price = None;

        let action = profile.apply(&signal).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);
        // Rule is skipped because no entry price to compute offset.
        assert!(rules.is_empty());
    }

    // ── apply_batch() tests ─────────────────────────────────────────────

    #[test]
    fn apply_batch_transforms_all() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let signals: Vec<RawSignalEntry> = (0..5)
            .map(|i| {
                let mut s = buy_signal();
                s.ts = ts(10, i, 0);
                s
            })
            .collect();

        let result = profile.apply_batch(&signals);
        assert_eq!(result.len(), 5);

        // Verify timestamps are preserved in order.
        for (i, sig) in result.iter().enumerate() {
            assert_eq!(sig.ts, ts(10, i as u32, 0));
        }
    }

    #[test]
    fn apply_batch_preserves_order() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let signals = vec![
            {
                let mut s = buy_signal();
                s.ts = ts(10, 0, 0);
                s.symbol = "eurusd".into();
                s
            },
            {
                let mut s = buy_signal();
                s.ts = ts(10, 1, 0);
                s.symbol = "gbpusd".into();
                s
            },
            {
                let mut s = buy_signal();
                s.ts = ts(10, 2, 0);
                s.symbol = "usdjpy".into();
                s
            },
        ];

        let result = profile.apply_batch(&signals);
        assert_eq!(result.len(), 3);

        let symbols: Vec<&str> = result
            .iter()
            .map(|s| match &s.action {
                Action::Open { symbol, .. } => symbol.as_str(),
                _ => panic!("Expected Open"),
            })
            .collect();
        assert_eq!(symbols, vec!["eurusd", "gbpusd", "usdjpy"]);
    }

    #[test]
    fn apply_batch_empty_input() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let result = profile.apply_batch(&[]);
        assert!(result.is_empty());
    }

    // ── TimeExit rule in profile ────────────────────────────────────────

    #[test]
    fn apply_with_time_exit_rule() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[[profile.rules]]
type = "TimeExit"
max_seconds = 3600
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        assert_eq!(rules.len(), 1);
        assert!(matches!(
            rules[0],
            RuleConfig::TimeExit { max_seconds: 3600 }
        ));
    }

    // ── Full profile TOML with multiple complex profiles ────────────────

    #[test]
    fn full_profiles_toml_loads() {
        let toml = r#"
[[profile]]
name = "conservative"
use_targets = [1]
close_ratios = [1.0]
stoploss_mode = { type = "FromSignal" }
let_remainder_run = false

[[profile]]
name = "aggressive"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
stoploss_mode = { type = "FromSignal" }
let_remainder_run = false

[[profile.rules]]
type = "BreakevenAfterTargets"
after_n = 1

[[profile.rules]]
type = "TrailingStop"
distance = 0.0020

[[profile]]
name = "runner"
use_targets = [1]
close_ratios = [0.3]
stoploss_mode = { type = "FromSignal" }
let_remainder_run = true

[[profile.rules]]
type = "BreakevenAfterTargets"
after_n = 1

[[profile.rules]]
type = "TrailingStop"
distance = 0.0030

[[profile]]
name = "scalp_tight"
use_targets = [1]
close_ratios = [1.0]
let_remainder_run = false

[profile.stoploss_mode]
type = "FixedDistance"
distance = 0.0010

[[profile]]
name = "time_limited"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
stoploss_mode = { type = "FromSignal" }
let_remainder_run = false

[[profile.rules]]
type = "TimeExit"
max_seconds = 3600

[[profile.rules]]
type = "BreakevenWhenOffset"
trigger_price_offset = 0.0020
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        assert_eq!(reg.len(), 5);
        assert_eq!(
            reg.names(),
            vec![
                "aggressive",
                "conservative",
                "runner",
                "scalp_tight",
                "time_limited"
            ]
        );
    }

    // ── Same signals, different profiles produce different results ───────

    #[test]
    fn same_signal_different_profiles() {
        let toml = r#"
[[profile]]
name = "conservative"
use_targets = [1]
close_ratios = [1.0]

[[profile]]
name = "aggressive"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let signal = buy_signal();

        let conservative = reg.get("conservative").unwrap();
        let aggressive = reg.get("aggressive").unwrap();

        let action_c = conservative.apply(&signal).unwrap();
        let action_a = aggressive.apply(&signal).unwrap();

        let (_, _, _, _, _, _, targets_c, _, _) = unwrap_open(&action_c);
        let (_, _, _, _, _, _, targets_a, _, _) = unwrap_open(&action_a);

        // Conservative: 1 target at 100%.
        assert_eq!(targets_c.len(), 1);
        assert!((targets_c[0].close_ratio - 1.0).abs() < f64::EPSILON);

        // Aggressive: 2 targets at 50% each.
        assert_eq!(targets_a.len(), 2);
        assert!((targets_a[0].close_ratio - 0.5).abs() < f64::EPSILON);
        assert!((targets_a[1].close_ratio - 0.5).abs() < f64::EPSILON);
    }

    // ── Serde roundtrip tests ───────────────────────────────────────────

    #[test]
    fn serde_roundtrip_raw_signal() {
        let signal = buy_signal();
        let json = serde_json::to_string(&signal).unwrap();
        let back: RawSignalEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(back.symbol, signal.symbol);
        assert_eq!(back.side, signal.side);
        assert_eq!(back.price, signal.price);
        assert_eq!(back.size, signal.size);
        assert_eq!(back.stoploss, signal.stoploss);
        assert_eq!(back.targets, signal.targets);
        assert_eq!(back.group, signal.group);
    }

    #[test]
    fn serde_roundtrip_profile() {
        let toml_input = r#"
[[profile]]
name = "test"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
stoploss_mode = { type = "FromSignal" }
group_override = "my_group"
let_remainder_run = true

[[profile.rules]]
type = "BreakevenAfterTargets"
after_n = 1
"#;
        let reg = ProfileRegistry::from_toml(toml_input).unwrap();
        let profile = reg.get("test").unwrap();

        assert_eq!(profile.name, "test");
        assert_eq!(profile.use_targets, vec![1, 2]);
        assert_eq!(profile.close_ratios, vec![0.5, 0.5]);
        assert_eq!(profile.group_override.as_deref(), Some("my_group"));
        assert!(profile.let_remainder_run);
        assert_eq!(profile.rules.len(), 1);
    }

    // ── Integration: profile produces valid signals for runner ───────────

    #[test]
    fn profile_produces_valid_signals_for_runner() {
        use crate::data_feed::{MarketEvent, VecFeed};
        use crate::runner::{BacktestConfig, BacktestRunner};

        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
stoploss_mode = { type = "FromSignal" }
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let raw_signals = vec![buy_signal()];
        let signals = profile.apply_batch(&raw_signals);

        // Create a price feed that triggers the TP.
        let events = vec![
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(9, 59, 59),
                bid: 1.0848,
                ask: 1.0850,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 0),
                bid: 1.0848,
                ask: 1.0850,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 1),
                bid: 1.0860,
                ask: 1.0862,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 2),
                bid: 1.0900,
                ask: 1.0902,
            },
        ];

        let mut feed = VecFeed::new(events);
        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let runner = BacktestRunner::new(config);
        let result = runner.run_signals(&mut feed, signals);

        // The trade should have been opened and TP should trigger.
        assert_eq!(result.total_trades, 1);
        assert!(result.total_pnl > 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn same_signals_different_profiles_different_results() {
        use crate::data_feed::{MarketEvent, VecFeed};
        use crate::runner::{BacktestConfig, BacktestRunner};

        let toml = r#"
[[profile]]
name = "tp1_only"
use_targets = [1]
close_ratios = [1.0]

[[profile]]
name = "tp1_tp2"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();

        let raw = vec![buy_signal()];

        // Feed that hits TP1 (1.0900) but not TP2 (1.0950).
        let events = vec![
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(9, 59, 59),
                bid: 1.0848,
                ask: 1.0850,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 0),
                bid: 1.0848,
                ask: 1.0850,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 1),
                bid: 1.0900,
                ask: 1.0902,
            },
            MarketEvent::Tick {
                symbol: "eurusd".into(),
                ts: ts(10, 0, 2),
                bid: 1.0910,
                ask: 1.0912,
            },
        ];

        // Profile A: close 100% at TP1. Trade should fully close.
        let signals_a = reg.get("tp1_only").unwrap().apply_batch(&raw);
        let mut feed_a = VecFeed::new(events.clone());
        let config = BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            ..Default::default()
        };
        let result_a = BacktestRunner::new(config.clone()).run_signals(&mut feed_a, signals_a);

        // Profile B: close 50% at TP1, 50% at TP2.
        // TP2 never hit, so remaining closes at end (close_on_finish).
        let signals_b = reg.get("tp1_tp2").unwrap().apply_batch(&raw);
        let mut feed_b = VecFeed::new(events);
        let result_b = BacktestRunner::new(config).run_signals(&mut feed_b, signals_b);

        // Both produced trades but with different P&L due to different management.
        assert!(result_a.total_trades >= 1);
        assert!(result_b.total_trades >= 1);
        // They should differ since profile B only partially closes at TP1.
        // (The remaining 50% is closed by close_on_finish at a different price.)
        assert!(
            (result_a.total_pnl - result_b.total_pnl).abs() > 1e-10
                || result_a.total_trades != result_b.total_trades,
            "Profiles should produce different results"
        );
    }

    // ── Negative ratio test ─────────────────────────────────────────────

    #[test]
    fn negative_ratio_error() {
        let toml = r#"
[[profile]]
name = "bad"
use_targets = [1]
close_ratios = [-0.5]
"#;
        let err = ProfileRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, ProfileError::ZeroRatio { .. }));
    }

    // ── Ratios exactly 1.0 is valid ─────────────────────────────────────

    #[test]
    fn ratios_sum_exactly_1_0_is_valid() {
        let toml = r#"
[[profile]]
name = "ok"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
        let reg = ProfileRegistry::from_toml(toml);
        assert!(reg.is_ok());
    }

    // ── Ratios sum less than 1.0 is valid (remainder runs) ──────────────

    #[test]
    fn ratios_sum_less_than_1_0_is_valid() {
        let toml = r#"
[[profile]]
name = "ok"
use_targets = [1]
close_ratios = [0.3]
let_remainder_run = true
"#;
        let reg = ProfileRegistry::from_toml(toml);
        assert!(reg.is_ok());
    }

    // ── Profile with only trailing stop (no targets) ────────────────────

    #[test]
    fn profile_with_trailing_stop_only() {
        let toml = r#"
[[profile]]
name = "trail_only"
use_targets = []
close_ratios = []
stoploss_mode = { type = "None" }

[[profile.rules]]
type = "TrailingStop"
distance = 0.0030
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("trail_only").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, targets, rules, _) = unwrap_open(&action);

        assert!(targets.is_empty());
        assert_eq!(sl, None);
        assert_eq!(rules.len(), 1);
        assert!(matches!(rules[0], RuleConfig::TrailingStop { .. }));
    }

    // ── Multiple rules of different types ───────────────────────────────

    #[test]
    fn profile_multiple_mixed_rules() {
        let toml = r#"
[[profile]]
name = "complex"
use_targets = [1]
close_ratios = [0.5]
let_remainder_run = true

[[profile.rules]]
type = "BreakevenAfterTargets"
after_n = 1

[[profile.rules]]
type = "TrailingStop"
distance = 0.0025

[[profile.rules]]
type = "TimeExit"
max_seconds = 7200
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("complex").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        assert_eq!(rules.len(), 3);
        assert!(matches!(
            rules[0],
            RuleConfig::BreakevenAfterTargets { after_n: 1 }
        ));
        assert!(matches!(rules[1], RuleConfig::TrailingStop { .. }));
        assert!(matches!(
            rules[2],
            RuleConfig::TimeExit { max_seconds: 7200 }
        ));
    }

    // ── Debug output for ProfileRegistry ────────────────────────────────

    #[test]
    fn debug_output_does_not_panic() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let debug = format!("{:?}", reg);
        assert!(debug.contains("ProfileRegistry"));
        assert!(debug.contains("test"));
    }

    // ── Load from file ──────────────────────────────────────────────────

    #[test]
    fn load_from_missing_file_returns_io_error() {
        let result = ProfileRegistry::load("/nonexistent/path/profiles.toml");
        assert!(matches!(result, Err(ProfileError::Io(_))));
    }

    // ── FixedStoploss rule in profile ───────────────────────────────────

    #[test]
    fn apply_with_fixed_stoploss_rule() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]

[[profile.rules]]
type = "FixedStoploss"
price = 1.0750
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);

        assert_eq!(rules.len(), 1);
        assert!(matches!(
            rules[0],
            RuleConfig::FixedStoploss { price } if (price - 1.0750).abs() < f64::EPSILON
        ));
    }

    // ── Sell signal with FixedDistance stoploss applies correctly ────────

    #[test]
    fn apply_sell_targets_correctly_selected() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1, 2]
close_ratios = [0.5, 0.5]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let action = profile.apply(&sell_signal()).unwrap();
        let (_, side, _, _, _, sl, targets, _, _) = unwrap_open(&action);

        assert_eq!(side, Side::Sell);
        assert_eq!(sl, Some(1.0900)); // From signal.
        assert_eq!(targets.len(), 2);
        assert!((targets[0].price - 1.0800).abs() < f64::EPSILON);
        assert!((targets[1].price - 1.0750).abs() < f64::EPSILON);
    }

    // ── Phase 2: insert / remove / validate_profile ─────────────────

    #[test]
    fn insert_new_profile() {
        let mut reg = ProfileRegistry::empty();
        let p = ManagementProfile {
            name: "new".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        assert!(reg.insert(p, false).is_ok());
        assert_eq!(reg.len(), 1);
        assert!(reg.get("new").is_some());
    }

    #[test]
    fn insert_duplicate_no_overwrite() {
        let mut reg = ProfileRegistry::empty();
        let p = ManagementProfile {
            name: "dup".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        reg.insert(p.clone(), false).unwrap();
        let result = reg.insert(p, false);
        assert!(result.is_err());
        match result.unwrap_err() {
            ProfileError::DuplicateName(n) => assert_eq!(n, "dup"),
            other => panic!("Expected DuplicateName, got: {other:?}"),
        }
    }

    #[test]
    fn insert_duplicate_with_overwrite() {
        let mut reg = ProfileRegistry::empty();
        let p1 = ManagementProfile {
            name: "ow".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        reg.insert(p1, false).unwrap();

        let p2 = ManagementProfile {
            name: "ow".into(),
            use_targets: vec![1, 2],
            close_ratios: vec![0.5, 0.5],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        assert!(reg.insert(p2, true).is_ok());
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get("ow").unwrap().use_targets, vec![1, 2]);
    }

    #[test]
    fn insert_validates_profile() {
        let mut reg = ProfileRegistry::empty();
        let bad = ManagementProfile {
            name: "bad".into(),
            use_targets: vec![1, 2],
            close_ratios: vec![1.0], // mismatch
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        assert!(reg.insert(bad, false).is_err());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn remove_existing() {
        let mut reg = ProfileRegistry::empty();
        let p = ManagementProfile {
            name: "rm".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        reg.insert(p, false).unwrap();
        assert!(reg.remove("rm"));
        assert_eq!(reg.len(), 0);
        assert!(reg.get("rm").is_none());
    }

    #[test]
    fn remove_nonexistent() {
        let mut reg = ProfileRegistry::empty();
        assert!(!reg.remove("nope"));
    }

    #[test]
    fn validate_profile_public() {
        let good = ManagementProfile {
            name: "ok".into(),
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        assert!(good.validate().is_ok());

        let bad = ManagementProfile {
            name: "bad".into(),
            use_targets: vec![0], // zero index
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };
        assert!(bad.validate().is_err());
    }

    // ── Default stoploss_mode when omitted from TOML ────────────────────

    #[test]
    fn default_stoploss_mode_is_from_signal() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        // Default should be FromSignal — signal's SL should pass through.
        let action = profile.apply(&buy_signal()).unwrap();
        let (_, _, _, _, _, sl, _, _, _) = unwrap_open(&action);
        assert_eq!(sl, Some(1.0800));
    }

    // ── Phase 1: RawSignal & PositionRef tests ──────────────────────────

    #[test]
    fn raw_signal_entry_has_correct_ts() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
        };
        assert_eq!(sig.ts(), ts(10, 0, 0));
    }

    #[test]
    fn raw_signal_close_has_correct_ts() {
        let sig = RawSignal::Close {
            ts: ts(11, 30, 0),
            position: PositionRef::Id { id: "pos1".into() },
        };
        assert_eq!(sig.ts(), ts(11, 30, 0));
    }

    #[test]
    fn raw_signal_is_entry_true() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
        };
        assert!(sig.is_entry());
    }

    #[test]
    fn raw_signal_is_entry_false_for_close() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::LastOnSymbol {
                symbol: "eurusd".into(),
            },
        };
        assert!(!sig.is_entry());
    }

    #[test]
    fn raw_signal_as_entry_converts() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(1.0850),
            size: 2.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900, 1.0950],
            group: Some("grp1".into()),
        };
        let entry = sig.as_entry().unwrap();
        assert_eq!(entry.ts, ts(10, 0, 0));
        assert_eq!(entry.symbol, "eurusd");
        assert_eq!(entry.side, Side::Buy);
        assert_eq!(entry.order_type, OrderType::Limit);
        assert_eq!(entry.price, Some(1.0850));
        assert!((entry.size - 2.0).abs() < f64::EPSILON);
        assert_eq!(entry.stoploss, Some(1.0800));
        assert_eq!(entry.targets, vec![1.0900, 1.0950]);
        assert_eq!(entry.group, Some("grp1".into()));
    }

    #[test]
    fn raw_signal_as_entry_returns_none_for_non_entry() {
        let sig = RawSignal::CloseAll { ts: ts(10, 0, 0) };
        assert!(sig.as_entry().is_none());
    }

    #[test]
    fn from_raw_signal_entry_converts() {
        let entry = RawSignalEntry {
            ts: ts(10, 0, 0),
            symbol: "gbpusd".into(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: Some(1.2500),
            size: 1.5,
            stoploss: Some(1.2550),
            targets: vec![1.2450],
            group: Some("g1".into()),
        };
        let sig: RawSignal = entry.into();
        assert!(sig.is_entry());
        assert_eq!(sig.ts(), ts(10, 0, 0));
        let back = sig.as_entry().unwrap();
        assert_eq!(back.symbol, "gbpusd");
        assert_eq!(back.side, Side::Sell);
        assert!((back.size - 1.5).abs() < f64::EPSILON);
        assert_eq!(back.group, Some("g1".into()));
    }

    #[test]
    fn serde_roundtrip_raw_signal_entry_variant() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
        };
        let json = serde_json::to_string(&sig).unwrap();
        let back: RawSignal = serde_json::from_str(&json).unwrap();
        assert!(back.is_entry());
        assert_eq!(back.ts(), ts(10, 0, 0));
    }

    #[test]
    fn serde_roundtrip_raw_signal_close() {
        let sig = RawSignal::Close {
            ts: ts(11, 0, 0),
            position: PositionRef::Id {
                id: "pos123".into(),
            },
        };
        let json = serde_json::to_string(&sig).unwrap();
        let back: RawSignal = serde_json::from_str(&json).unwrap();
        assert!(!back.is_entry());
        assert_eq!(back.ts(), ts(11, 0, 0));
    }

    #[test]
    fn serde_roundtrip_position_ref_all_variants() {
        let variants: Vec<PositionRef> = vec![
            PositionRef::Id { id: "abc".into() },
            PositionRef::LastOnSymbol {
                symbol: "eurusd".into(),
            },
            PositionRef::LastInGroup {
                group_id: "g1".into(),
            },
            PositionRef::AllOnSymbol {
                symbol: "gbpusd".into(),
            },
            PositionRef::AllInGroup {
                group_id: "g2".into(),
            },
        ];
        for pr in &variants {
            let json = serde_json::to_string(pr).unwrap();
            let back: PositionRef = serde_json::from_str(&json).unwrap();
            // Just verify it round-trips without panic
            let _debug = format!("{:?}", back);
        }
    }

    // ── Phase 2: resolve_signal tests ───────────────────────────────────

    /// A mock resolver for unit testing resolve_signal.
    struct MockResolver {
        ids: Vec<PositionId>,
        entry_info: Option<(f64, Side)>,
    }

    impl MockResolver {
        fn with_ids(ids: Vec<&str>) -> Self {
            Self {
                ids: ids.into_iter().map(String::from).collect(),
                entry_info: None,
            }
        }

        fn with_ids_and_info(ids: Vec<&str>, entry_price: f64, side: Side) -> Self {
            Self {
                ids: ids.into_iter().map(String::from).collect(),
                entry_info: Some((entry_price, side)),
            }
        }

        fn empty() -> Self {
            Self {
                ids: vec![],
                entry_info: None,
            }
        }
    }

    impl PositionResolver for MockResolver {
        fn resolve(&self, _pr: &PositionRef) -> Vec<PositionId> {
            self.ids.clone()
        }

        fn position_entry_info(&self, _id: &PositionId) -> Option<(f64, Side)> {
            self.entry_info
        }
    }

    #[test]
    fn resolve_signal_entry_returns_empty() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert!(actions.is_empty());
    }

    #[test]
    fn resolve_signal_close_single() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::ClosePosition { position_id } if position_id == "pos1"
        ));
    }

    #[test]
    fn resolve_signal_close_multiple() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::AllOnSymbol {
                symbol: "eurusd".into(),
            },
        };
        let resolver = MockResolver::with_ids(vec!["pos1", "pos2", "pos3"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 3);
    }

    #[test]
    fn resolve_signal_close_empty_resolver() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::LastOnSymbol {
                symbol: "eurusd".into(),
            },
        };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert!(actions.is_empty());
    }

    #[test]
    fn resolve_signal_close_partial() {
        let sig = RawSignal::ClosePartial {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            ratio: 0.5,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::ClosePartial { position_id, ratio } => {
                assert_eq!(position_id, "pos1");
                assert!((ratio - 0.5).abs() < f64::EPSILON);
            }
            other => panic!("Expected ClosePartial, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_modify_stoploss() {
        let sig = RawSignal::ModifyStoploss {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            price: 1.0820,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::ModifyStoploss { position_id, price } => {
                assert_eq!(position_id, "pos1");
                assert!((price - 1.0820).abs() < f64::EPSILON);
            }
            other => panic!("Expected ModifyStoploss, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_move_sl_to_entry() {
        let sig = RawSignal::MoveStoplossToEntry {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::MoveStoplossToEntry { position_id } if position_id == "pos1"
        ));
    }

    #[test]
    fn resolve_signal_add_target() {
        let sig = RawSignal::AddTarget {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            price: 1.0950,
            close_ratio: 0.5,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::AddTarget {
                position_id,
                price,
                close_ratio,
            } => {
                assert_eq!(position_id, "pos1");
                assert!((price - 1.0950).abs() < f64::EPSILON);
                assert!((close_ratio - 0.5).abs() < f64::EPSILON);
            }
            other => panic!("Expected AddTarget, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_remove_target() {
        let sig = RawSignal::RemoveTarget {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            price: 1.0950,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::RemoveTarget { position_id, price } => {
                assert_eq!(position_id, "pos1");
                assert!((price - 1.0950).abs() < f64::EPSILON);
            }
            other => panic!("Expected RemoveTarget, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_add_rule_with_entry_info() {
        let sig = RawSignal::AddRule {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            rule: RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset: 0.0050,
            },
        };
        let resolver = MockResolver::with_ids_and_info(vec!["pos1"], 1.0850, Side::Buy);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::AddRule { position_id, rule } => {
                assert_eq!(position_id, "pos1");
                match rule {
                    RuleConfig::BreakevenWhen { trigger_price } => {
                        assert!((trigger_price - 1.0900).abs() < 1e-10);
                    }
                    other => panic!("Expected BreakevenWhen, got {other:?}"),
                }
            }
            other => panic!("Expected AddRule, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_add_rule_no_entry_info_skips() {
        let sig = RawSignal::AddRule {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            rule: RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset: 0.0050,
            },
        };
        // Resolver returns an ID but no entry info — offset can't resolve
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert!(actions.is_empty());
    }

    #[test]
    fn resolve_signal_add_rule_trailing_stop() {
        let sig = RawSignal::AddRule {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            rule: RuleConfigDef::TrailingStop { distance: 0.0030 },
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::AddRule { rule, .. } => {
                assert!(
                    matches!(rule, RuleConfig::TrailingStop { distance } if (*distance - 0.0030).abs() < f64::EPSILON)
                );
            }
            other => panic!("Expected AddRule, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_remove_rule() {
        let sig = RawSignal::RemoveRule {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            rule_name: "TrailingStop".into(),
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::RemoveRule {
                position_id,
                rule_name,
            } => {
                assert_eq!(position_id, "pos1");
                assert_eq!(rule_name, "TrailingStop");
            }
            other => panic!("Expected RemoveRule, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_scale_in() {
        let sig = RawSignal::ScaleIn {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
            price: Some(1.0860),
            size: 0.5,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::ScaleIn {
                position_id,
                price,
                size,
            } => {
                assert_eq!(position_id, "pos1");
                assert_eq!(*price, Some(1.0860));
                assert!((size - 0.5).abs() < f64::EPSILON);
            }
            other => panic!("Expected ScaleIn, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_cancel_pending() {
        let sig = RawSignal::CancelPending {
            ts: ts(10, 0, 0),
            position: PositionRef::Id { id: "pos1".into() },
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::CancelPending { position_id } if position_id == "pos1"
        ));
    }

    #[test]
    fn resolve_signal_bulk_close_all_of() {
        let sig = RawSignal::CloseAllOf {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
        };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::CloseAllOf { symbol } if symbol == "eurusd"
        ));
    }

    #[test]
    fn resolve_signal_bulk_close_all() {
        let sig = RawSignal::CloseAll { ts: ts(10, 0, 0) };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], Action::CloseAll));
    }

    #[test]
    fn resolve_signal_bulk_cancel_all_pending() {
        let sig = RawSignal::CancelAllPending { ts: ts(10, 0, 0) };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], Action::CancelAllPending));
    }

    #[test]
    fn resolve_signal_bulk_modify_all_stoploss() {
        let sig = RawSignal::ModifyAllStoploss {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            price: 1.0780,
        };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::ModifyAllStoploss { symbol, price } => {
                assert_eq!(symbol, "eurusd");
                assert!((price - 1.0780).abs() < f64::EPSILON);
            }
            other => panic!("Expected ModifyAllStoploss, got {other:?}"),
        }
    }

    #[test]
    fn resolve_signal_bulk_close_all_in_group() {
        let sig = RawSignal::CloseAllInGroup {
            ts: ts(10, 0, 0),
            group_id: "g1".into(),
        };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        assert!(matches!(
            &actions[0],
            Action::CloseAllInGroup { group_id } if group_id == "g1"
        ));
    }

    #[test]
    fn resolve_signal_bulk_modify_all_sl_in_group() {
        let sig = RawSignal::ModifyAllStoplossInGroup {
            ts: ts(10, 0, 0),
            group_id: "g1".into(),
            price: 1.0780,
        };
        let resolver = MockResolver::empty();
        let actions = resolve_signal(&sig, &resolver);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::ModifyAllStoplossInGroup { group_id, price } => {
                assert_eq!(group_id, "g1");
                assert!((price - 1.0780).abs() < f64::EPSILON);
            }
            other => panic!("Expected ModifyAllStoplossInGroup, got {other:?}"),
        }
    }

    // ── Phase 3: apply_raw / apply_batch_raw tests ──────────────────────

    #[test]
    fn apply_raw_entry_transforms() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            size: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
        };
        let result = profile.apply_raw(&sig);
        assert!(result.is_some());
        assert!(result.unwrap().is_entry());
    }

    #[test]
    fn apply_raw_close_passes_through() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let sig = RawSignal::Close {
            ts: ts(10, 30, 0),
            position: PositionRef::LastOnSymbol {
                symbol: "eurusd".into(),
            },
        };
        let result = profile.apply_raw(&sig);
        assert!(result.is_some());
        let result = result.unwrap();
        assert!(!result.is_entry());
        assert_eq!(result.ts(), ts(10, 30, 0));
    }

    #[test]
    fn apply_batch_raw_mixed() {
        let toml = r#"
[[profile]]
name = "test"
use_targets = [1]
close_ratios = [1.0]
"#;
        let reg = ProfileRegistry::from_toml(toml).unwrap();
        let profile = reg.get("test").unwrap();

        let signals = vec![
            RawSignal::Entry {
                ts: ts(10, 0, 0),
                symbol: "eurusd".into(),
                side: Side::Buy,
                order_type: OrderType::Market,
                price: Some(1.0850),
                size: 1.0,
                stoploss: Some(1.0800),
                targets: vec![1.0900],
                group: None,
            },
            RawSignal::Close {
                ts: ts(10, 30, 0),
                position: PositionRef::LastOnSymbol {
                    symbol: "eurusd".into(),
                },
            },
            RawSignal::ModifyStoploss {
                ts: ts(10, 15, 0),
                position: PositionRef::Id { id: "pos1".into() },
                price: 1.0820,
            },
        ];

        let result = profile.apply_batch_raw(&signals);
        assert_eq!(result.len(), 3);
        assert!(result[0].is_entry());
        assert!(!result[1].is_entry());
        assert!(!result[2].is_entry());
    }
}
