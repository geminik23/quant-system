//! Management profiles — decouple entry signals from trade management.
//!
//! A [`ManagementProfile`] resolves [`RawSignal::Entry`] fields before sizing.
//! Resolved entries can be finalized into [`Action::Open`] calls after a concrete lot size is known.
//! Profiles are loaded through [`ProfileRegistry`] for comparison without recompilation.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use qs_core::types::{
    Action, GroupId, OrderType, PositionId, RuleConfig, Side, TargetSpec, TradeId,
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
        "Profile '{profile}': selected target count ({targets}) does not match close_ratios length ({ratios})"
    )]
    TargetRatioMismatch {
        profile: String,
        targets: usize,
        ratios: usize,
    },

    #[error("Profile '{profile}': close_ratios sum to {sum:.4}, which exceeds 1.0")]
    RatioSumExceeded { profile: String, sum: f64 },

    #[error(
        "Profile '{profile}': close_ratios sum to {sum:.4}; they must sum to 1.0 when let_remainder_run is false"
    )]
    RatioSumIncomplete { profile: String, sum: f64 },

    #[error("Profile '{profile}': close_ratios contains a non-finite or non-positive value")]
    ZeroRatio { profile: String },

    #[error("Profile '{profile}': target selection contains a 0 index (must be 1-indexed)")]
    ZeroTargetIndex { profile: String },

    #[error("Profile '{profile}': target index {index} is selected more than once")]
    DuplicateTargetIndex { profile: String, index: usize },

    #[error("Profile '{profile}': {reason}")]
    InvalidConfiguration { profile: String, reason: String },

    #[error("Profile not found: '{0}'")]
    NotFound(String),
}

/// Strict validation failures returned by the additive V2 entry resolvers.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ProfileApplicationError {
    #[error("{field} must be finite and greater than zero, got {value}")]
    InvalidNumericInput { field: String, value: f64 },

    #[error("target indices are 1-based; index 0 is invalid")]
    ZeroTargetIndex,

    #[error("target index {index} is selected more than once")]
    DuplicateTargetIndex { index: usize },

    #[error("target price {price} is selected more than once")]
    DuplicateTargetPrice { price: f64 },

    #[error("target index {index} is missing; signal provides {available} target(s)")]
    MissingTargetIndex { index: usize, available: usize },

    #[error("selected target count ({targets}) does not match explicit weight count ({weights})")]
    TargetWeightCountMismatch { targets: usize, weights: usize },

    #[error("target weight {position} must be finite and greater than zero, got {weight}")]
    InvalidTargetWeight { position: usize, weight: f64 },

    #[error("target weights sum to {sum}, which exceeds 1.0")]
    TargetWeightSumExceeded { sum: f64 },

    #[error("target weights sum to {sum}; they must sum to 1.0 when no remainder runs")]
    TargetWeightSumIncomplete { sum: f64 },

    #[error(
        "target {index} at {target} is invalid for {side} entry at {entry}: buy targets must be above entry and sell targets below entry"
    )]
    InvalidTargetGeometry {
        index: usize,
        side: Side,
        entry: f64,
        target: f64,
    },

    #[error(
        "stop {stoploss} is invalid for {side} entry at {entry}: buy stops must be below entry and sell stops above entry"
    )]
    InvalidStopGeometry {
        side: Side,
        entry: f64,
        stoploss: f64,
    },

    #[error("size {size} is not an integer multiple of lot_step {lot_step}")]
    SizeNotMultipleOfLotStep { size: f64, lot_step: f64 },

    #[error("size {size} and lot_step {lot_step} produce a lot count outside u64 range")]
    LotUnitCountOverflow { size: f64, lot_step: f64 },

    #[error("target allocation {position} rounds to zero lot units")]
    ZeroUnitAllocation { position: usize },

    #[error("allocation remainder must be finite and non-negative, got {remainder}")]
    InvalidRemainder { remainder: f64 },

    #[error(
        "target weights sum to {sum}, but allocation remainder is {remainder}; together they must equal 1.0"
    )]
    TargetWeightRemainderMismatch { sum: f64, remainder: f64 },

    #[error("{field} must be greater than zero, got {value}")]
    InvalidCountInput { field: String, value: u64 },
}

// ─── PositionRef ────────────────────────────────────────────────────────────

/// How a management signal references its target position(s).
///
/// Resolved at runtime by the backtest runner, which has access to
/// engine state for lookup.
///
/// The minimal set is:
/// - `ByTradeId`: the canonical parser path. Each entry carries an
///   application-defined `trade_id`; management signals reference it.
/// - `AllOnSymbol`: bulk close by symbol.
/// - `AllInGroup`: bulk close by group.
///
/// `group` is a reporting tag (channel-level), while `trade_id` is the
/// per-trade identity used for addressing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PositionRef {
    /// Target the position with the given application-defined trade id.
    ByTradeId { trade_id: TradeId },
    /// All open positions on a symbol.
    AllOnSymbol { symbol: String },
    /// All open positions in a group.
    AllInGroup { group_id: GroupId },
}

// ─── RawSignal ──────────────────────────────────────────────────────────────

fn deserialize_risk_multiplier<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = f64::deserialize(deserializer)?;
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(serde::de::Error::custom(format!(
            "risk must be finite and greater than zero, got {value}"
        )))
    }
}

/// A raw signal from an external source — entry or management.
///
/// Covers both entry signals (which can be profile-transformed) and
/// management signals (which pass through to the engine as-is after
/// position resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub enum RawSignal {
    // ── Entry (profile-transformable) ───────────────────────────────
    Entry {
        ts: NaiveDateTime,
        symbol: String,
        side: Side,
        order_type: OrderType,
        price: Option<f64>,
        #[serde(rename = "risk", deserialize_with = "deserialize_risk_multiplier")]
        risk_multiplier: f64,
        stoploss: Option<f64>,
        #[serde(default)]
        targets: Vec<f64>,
        #[serde(default)]
        group: Option<String>,
        /// Application-defined trade identity. Required for `ByTradeId`
        /// resolution. Older JSONL without this field is still accepted.
        #[serde(default)]
        trade_id: Option<TradeId>,
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
    ModifyTarget {
        ts: NaiveDateTime,
        position: PositionRef,
        old_price: f64,
        new_price: f64,
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
            Self::ModifyTarget { ts, .. } => *ts,
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
}

// ─── Position Resolution ────────────────────────────────────────────────────

/// Resolves a `PositionRef` to concrete position ID(s) using engine state.
pub trait PositionResolver {
    /// Resolve a position reference to zero or more concrete position IDs.
    fn resolve(&self, pr: &PositionRef) -> Vec<PositionId>;
    /// Get entry info (average_entry, side) for a position.
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

        RawSignal::ModifyTarget {
            position,
            old_price,
            new_price,
            ..
        } => resolver
            .resolve(position)
            .into_iter()
            .map(|id| Action::ModifyTarget {
                position_id: id,
                old_price: *old_price,
                new_price: *new_price,
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
                trade_id: None,
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

// ─── Strict V2 target resolution ─────────────────────────────────────────────

/// Which 1-based target indices participate in strict target resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetSelection {
    /// Use every target supplied by the entry signal, in signal order.
    All,
    /// Do not attach any targets.
    None,
    /// Use the listed 1-based signal target indices, in the listed order.
    Selected(Vec<usize>),
}

/// Metadata describing how signal targets were selected and weighted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetResolution {
    pub selection: TargetSelection,
    /// Resolved 1-based indices in output order.
    pub selected_indices: Vec<usize>,
    /// Close weights corresponding one-to-one with `selected_indices`.
    pub weights: Vec<f64>,
    /// Fraction of the original position not assigned to a target.
    pub remainder: f64,
}

/// A resolved entry that retains risk intent without assigning concrete lots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedEntry {
    pub risk_multiplier: f64,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub stoploss: Option<f64>,
    pub targets: Vec<TargetSpec>,
    pub rules: Vec<RuleConfig>,
    pub group: Option<GroupId>,
    pub trade_id: Option<TradeId>,
    pub target_resolution: TargetResolution,
}

impl ResolvedEntry {
    /// Finalize the resolved entry with a concrete lot size.
    pub fn into_action(self, lot_size: f64) -> Action {
        Action::Open {
            symbol: self.symbol,
            side: self.side,
            order_type: self.order_type,
            price: self.price,
            size: lot_size,
            stoploss: self.stoploss,
            targets: self.targets,
            rules: self.rules,
            group: self.group,
            trade_id: self.trade_id,
        }
    }
}

// ─── ManagementProfile ──────────────────────────────────────────────────────

/// A named management profile that resolves raw entry signals before sizing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagementProfile {
    /// Profile name (e.g. "conservative", "aggressive", "runner").
    pub name: String,

    /// Explicit current target selection. When present, this takes precedence
    /// over `use_targets` for [`Self::apply_entry_signal`]. When omitted,
    /// schema-2 compatibility decoding derives the prior behavior from `use_targets`: an empty vector means
    /// [`TargetSelection::None`], otherwise it means [`TargetSelection::Selected`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_selection: Option<TargetSelection>,

    /// Compatibility target selection (1-indexed), retained so existing schema-2
    /// serialized profiles remain readable.
    pub use_targets: Vec<usize>,

    /// Close ratio for each selected target. In current application, an empty
    /// vector assigns equal weights to all selected targets; otherwise its
    /// length must match the effective target selection.
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
    /// Return the target selection used by current application.
    ///
    /// The explicit `target_selection` field wins when present. Otherwise this
    /// preserves existing schema-2 profile behavior by deriving `None`/`Selected` from
    /// `use_targets`.
    pub fn effective_target_selection(&self) -> TargetSelection {
        self.target_selection.clone().unwrap_or_else(|| {
            if self.use_targets.is_empty() {
                TargetSelection::None
            } else {
                TargetSelection::Selected(self.use_targets.clone())
            }
        })
    }

    /// Validate this profile's configuration.
    pub fn validate(&self) -> Result<(), ProfileError> {
        ProfileRegistry::validate_profile(self)
    }

    /// Transform a `RawSignal::Entry` while retaining target-resolution metadata.
    ///
    /// This canonical path rejects malformed numeric values, target selections,
    /// geometry, and weights. An explicit `target_selection` takes precedence; when it is
    /// omitted, an empty `use_targets` means [`TargetSelection::None`] and non-empty
    /// `use_targets` means [`TargetSelection::Selected`]. When targets are selected and
    /// `close_ratios` is empty, equal `1 / N` weights are synthesized.
    pub fn apply_entry_signal(
        &self,
        signal: &RawSignal,
    ) -> Result<Option<ResolvedEntry>, ProfileApplicationError> {
        let (
            symbol,
            side,
            order_type,
            price,
            risk_multiplier,
            signal_stoploss,
            signal_targets,
            group,
            trade_id,
        ) = match signal {
            RawSignal::Entry {
                symbol,
                side,
                order_type,
                price,
                risk_multiplier,
                stoploss,
                targets,
                group,
                trade_id,
                ..
            } => (
                symbol,
                side,
                order_type,
                price,
                risk_multiplier,
                stoploss,
                targets,
                group,
                trade_id,
            ),
            _ => return Ok(None),
        };

        validate_entry_numbers(*price, *risk_multiplier, *signal_stoploss, signal_targets)?;

        let selection = self.effective_target_selection();
        let (targets, target_resolution) = resolve_targets_v2(
            signal_targets,
            *side,
            *price,
            selection,
            &self.close_ratios,
            self.let_remainder_run,
        )?;
        let stoploss = resolve_stoploss_v2(&self.stoploss_mode, *signal_stoploss, *price, *side)?;
        let rules = resolve_rules_v2(&self.rules, *price, *side)?;

        Ok(Some(ResolvedEntry {
            risk_multiplier: *risk_multiplier,
            symbol: symbol.clone(),
            side: *side,
            order_type: *order_type,
            price: *price,
            stoploss,
            targets,
            rules,
            group: self.group_override.clone().or(group.clone()),
            trade_id: trade_id.clone(),
            target_resolution,
        }))
    }
}

const WEIGHT_TOLERANCE: f64 = 1e-12;
const LOT_ALIGNMENT_TOLERANCE: f64 = 1e-9;

fn require_positive_finite(
    field: impl Into<String>,
    value: f64,
) -> Result<(), ProfileApplicationError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ProfileApplicationError::InvalidNumericInput {
            field: field.into(),
            value,
        })
    }
}

fn validate_entry_numbers(
    price: Option<f64>,
    risk_multiplier: f64,
    stoploss: Option<f64>,
    targets: &[f64],
) -> Result<(), ProfileApplicationError> {
    require_positive_finite("risk_multiplier", risk_multiplier)?;
    if let Some(price) = price {
        require_positive_finite("price", price)?;
    }
    if let Some(stoploss) = stoploss {
        require_positive_finite("stoploss", stoploss)?;
    }
    for (offset, &target) in targets.iter().enumerate() {
        require_positive_finite(format!("target {}", offset + 1), target)?;
    }
    Ok(())
}

fn weights_sum_to_one(sum: f64) -> bool {
    (sum - 1.0).abs() <= WEIGHT_TOLERANCE
}

fn validate_weights(
    weights: &[f64],
    let_remainder_run: bool,
) -> Result<f64, ProfileApplicationError> {
    for (offset, &weight) in weights.iter().enumerate() {
        if !weight.is_finite() || weight <= 0.0 {
            return Err(ProfileApplicationError::InvalidTargetWeight {
                position: offset + 1,
                weight,
            });
        }
    }

    let sum: f64 = weights.iter().sum();
    if !sum.is_finite() || sum > 1.0 + WEIGHT_TOLERANCE {
        return Err(ProfileApplicationError::TargetWeightSumExceeded { sum });
    }
    if !let_remainder_run && !weights_sum_to_one(sum) {
        return Err(ProfileApplicationError::TargetWeightSumIncomplete { sum });
    }

    Ok(if weights_sum_to_one(sum) {
        0.0
    } else {
        1.0 - sum
    })
}

fn resolve_targets_v2(
    signal_targets: &[f64],
    side: Side,
    entry_price: Option<f64>,
    selection: TargetSelection,
    explicit_weights: &[f64],
    let_remainder_run: bool,
) -> Result<(Vec<TargetSpec>, TargetResolution), ProfileApplicationError> {
    let selected_indices = match &selection {
        TargetSelection::All => (1..=signal_targets.len()).collect(),
        TargetSelection::None => Vec::new(),
        TargetSelection::Selected(indices) => {
            let mut seen = HashSet::with_capacity(indices.len());
            for &index in indices {
                if index == 0 {
                    return Err(ProfileApplicationError::ZeroTargetIndex);
                }
                if !seen.insert(index) {
                    return Err(ProfileApplicationError::DuplicateTargetIndex { index });
                }
                if index > signal_targets.len() {
                    return Err(ProfileApplicationError::MissingTargetIndex {
                        index,
                        available: signal_targets.len(),
                    });
                }
            }
            indices.clone()
        }
    };

    if selected_indices.is_empty() {
        if !explicit_weights.is_empty() {
            return Err(ProfileApplicationError::TargetWeightCountMismatch {
                targets: 0,
                weights: explicit_weights.len(),
            });
        }
        return Ok((
            Vec::new(),
            TargetResolution {
                selection,
                selected_indices,
                weights: Vec::new(),
                remainder: 1.0,
            },
        ));
    }

    let weights = if explicit_weights.is_empty() {
        vec![1.0 / selected_indices.len() as f64; selected_indices.len()]
    } else {
        if explicit_weights.len() != selected_indices.len() {
            return Err(ProfileApplicationError::TargetWeightCountMismatch {
                targets: selected_indices.len(),
                weights: explicit_weights.len(),
            });
        }
        explicit_weights.to_vec()
    };
    let remainder = validate_weights(&weights, let_remainder_run)?;

    let mut targets = Vec::with_capacity(selected_indices.len());
    let mut target_price_keys = HashSet::with_capacity(selected_indices.len());
    for (&index, &weight) in selected_indices.iter().zip(&weights) {
        let target = signal_targets[index - 1];
        let target_key = (target * 1_000_000.0).round() as i64;
        if !target_price_keys.insert(target_key) {
            return Err(ProfileApplicationError::DuplicateTargetPrice { price: target });
        }
        if let Some(entry) = entry_price {
            let valid_geometry = match side {
                Side::Buy => target > entry,
                Side::Sell => target < entry,
            };
            if !valid_geometry {
                return Err(ProfileApplicationError::InvalidTargetGeometry {
                    index,
                    side,
                    entry,
                    target,
                });
            }
        }
        targets.push(TargetSpec {
            price: target,
            close_ratio: weight,
        });
    }

    Ok((
        targets,
        TargetResolution {
            selection,
            selected_indices,
            weights,
            remainder,
        },
    ))
}

fn validate_stop_geometry_v2(
    side: Side,
    entry: f64,
    stoploss: f64,
) -> Result<(), ProfileApplicationError> {
    let valid = match side {
        Side::Buy => stoploss < entry,
        Side::Sell => stoploss > entry,
    };
    if valid {
        Ok(())
    } else {
        Err(ProfileApplicationError::InvalidStopGeometry {
            side,
            entry,
            stoploss,
        })
    }
}

fn validate_target_geometry_v2(
    index: usize,
    side: Side,
    entry: f64,
    target: f64,
) -> Result<(), ProfileApplicationError> {
    let valid = match side {
        Side::Buy => target > entry,
        Side::Sell => target < entry,
    };
    if valid {
        Ok(())
    } else {
        Err(ProfileApplicationError::InvalidTargetGeometry {
            index,
            side,
            entry,
            target,
        })
    }
}

fn resolve_stoploss_v2(
    mode: &StoplossMode,
    signal_stoploss: Option<f64>,
    entry_price: Option<f64>,
    side: Side,
) -> Result<Option<f64>, ProfileApplicationError> {
    let stoploss = match mode {
        StoplossMode::FromSignal => signal_stoploss,
        StoplossMode::None => None,
        StoplossMode::FixedDistance { distance } => {
            require_positive_finite("stoploss fixed distance", *distance)?;
            entry_price.map(|entry| match side {
                Side::Buy => entry - distance,
                Side::Sell => entry + distance,
            })
        }
        StoplossMode::FixedPrice { price } => {
            require_positive_finite("stoploss fixed price", *price)?;
            Some(*price)
        }
    };
    if let Some(stoploss) = stoploss {
        require_positive_finite("resolved stoploss", stoploss)?;
        if let Some(entry) = entry_price {
            validate_stop_geometry_v2(side, entry, stoploss)?;
        }
    }
    Ok(stoploss)
}

fn resolve_rules_v2(
    definitions: &[RuleConfigDef],
    entry_price: Option<f64>,
    side: Side,
) -> Result<Vec<RuleConfig>, ProfileApplicationError> {
    let mut rules = Vec::with_capacity(definitions.len());
    for (offset, definition) in definitions.iter().enumerate() {
        let position = offset + 1;
        match definition {
            RuleConfigDef::FixedStoploss { price } => {
                require_positive_finite(format!("rule {position} fixed stoploss price"), *price)?;
                if let Some(entry) = entry_price {
                    validate_stop_geometry_v2(side, entry, *price)?;
                }
            }
            RuleConfigDef::TrailingStop { distance } => {
                require_positive_finite(format!("rule {position} trailing distance"), *distance)?;
                if let Some(entry) = entry_price {
                    let initial_stop = match side {
                        Side::Buy => entry - distance,
                        Side::Sell => entry + distance,
                    };
                    require_positive_finite(
                        format!("rule {position} initial trailing stop"),
                        initial_stop,
                    )?;
                    validate_stop_geometry_v2(side, entry, initial_stop)?;
                }
            }
            RuleConfigDef::TakeProfit { price, close_ratio } => {
                require_positive_finite(format!("rule {position} take-profit price"), *price)?;
                require_positive_finite(
                    format!("rule {position} take-profit close ratio"),
                    *close_ratio,
                )?;
                if *close_ratio > 1.0 {
                    return Err(ProfileApplicationError::InvalidTargetWeight {
                        position,
                        weight: *close_ratio,
                    });
                }
                if let Some(entry) = entry_price {
                    validate_target_geometry_v2(position, side, entry, *price)?;
                }
            }
            RuleConfigDef::BreakevenWhen { trigger_price } => {
                require_positive_finite(
                    format!("rule {position} breakeven trigger price"),
                    *trigger_price,
                )?;
                if let Some(entry) = entry_price {
                    validate_target_geometry_v2(position, side, entry, *trigger_price)?;
                }
            }
            RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset,
            } => {
                require_positive_finite(
                    format!("rule {position} breakeven trigger offset"),
                    *trigger_price_offset,
                )?;
            }
            RuleConfigDef::BreakevenAfterTargets { after_n } => {
                if *after_n == 0 {
                    return Err(ProfileApplicationError::InvalidCountInput {
                        field: format!("rule {position} breakeven target count"),
                        value: 0,
                    });
                }
            }
            RuleConfigDef::TimeExit { max_seconds } => {
                if *max_seconds == 0 {
                    return Err(ProfileApplicationError::InvalidCountInput {
                        field: format!("rule {position} maximum seconds"),
                        value: 0,
                    });
                }
            }
        }

        if let Some(rule) = definition.resolve(entry_price, side) {
            if let RuleConfig::BreakevenWhen { trigger_price } = &rule {
                require_positive_finite(
                    format!("rule {position} resolved breakeven trigger"),
                    *trigger_price,
                )?;
                if let Some(entry) = entry_price {
                    validate_target_geometry_v2(position, side, entry, *trigger_price)?;
                }
            }
            rules.push(rule);
        }
    }
    Ok(rules)
}

/// Strictly resolve an entry without a management profile.
///
/// Every signal target is retained and receives an equal `1 / N` close weight.
/// Non-entry signals return `Ok(None)`.
pub fn resolve_unprofiled_entry_v2(
    signal: &RawSignal,
) -> Result<Option<ResolvedEntry>, ProfileApplicationError> {
    let (
        symbol,
        side,
        order_type,
        price,
        risk_multiplier,
        stoploss,
        signal_targets,
        group,
        trade_id,
    ) = match signal {
        RawSignal::Entry {
            symbol,
            side,
            order_type,
            price,
            risk_multiplier,
            stoploss,
            targets,
            group,
            trade_id,
            ..
        } => (
            symbol,
            side,
            order_type,
            price,
            risk_multiplier,
            stoploss,
            targets,
            group,
            trade_id,
        ),
        _ => return Ok(None),
    };

    validate_entry_numbers(*price, *risk_multiplier, *stoploss, signal_targets)?;
    let (targets, target_resolution) = resolve_targets_v2(
        signal_targets,
        *side,
        *price,
        TargetSelection::All,
        &[],
        false,
    )?;

    Ok(Some(ResolvedEntry {
        risk_multiplier: *risk_multiplier,
        symbol: symbol.clone(),
        side: *side,
        order_type: *order_type,
        price: *price,
        stoploss: *stoploss,
        targets,
        rules: Vec::new(),
        group: group.clone(),
        trade_id: trade_id.clone(),
        target_resolution,
    }))
}

/// Allocate target close weights from authoritative integer lot steps.
/// Each non-final target rounds down, while a fully allocated final target receives all remaining steps.
/// A positive runner remainder is intentionally left unallocated, and `weights + remainder` must equal one.
pub fn allocate_target_steps(
    total_steps: u64,
    weights: &[f64],
    remainder: f64,
) -> Result<Vec<u64>, ProfileApplicationError> {
    if total_steps == 0 {
        return Err(ProfileApplicationError::InvalidCountInput {
            field: "total_steps".into(),
            value: total_steps,
        });
    }
    if !remainder.is_finite() || remainder < 0.0 {
        return Err(ProfileApplicationError::InvalidRemainder { remainder });
    }
    if weights.is_empty() {
        if weights_sum_to_one(remainder) {
            return Ok(Vec::new());
        }
        return Err(ProfileApplicationError::TargetWeightRemainderMismatch {
            sum: 0.0,
            remainder,
        });
    }

    let computed_remainder = validate_weights(weights, true)?;
    let weight_sum = 1.0 - computed_remainder;
    if !weights_sum_to_one(weight_sum + remainder) {
        return Err(ProfileApplicationError::TargetWeightRemainderMismatch {
            sum: weight_sum,
            remainder,
        });
    }
    let assign_residue_to_final = weights_sum_to_one(weight_sum);

    let mut allocations = Vec::with_capacity(weights.len());
    let mut allocated = 0_u64;
    for (offset, &weight) in weights.iter().enumerate() {
        let is_final = offset + 1 == weights.len();
        let steps = if is_final && assign_residue_to_final {
            total_steps.saturating_sub(allocated)
        } else {
            ((total_steps as f64) * weight).floor() as u64
        };
        if steps == 0 {
            return Err(ProfileApplicationError::ZeroUnitAllocation {
                position: offset + 1,
            });
        }
        allocated = allocated.saturating_add(steps);
        allocations.push(steps);
    }

    Ok(allocations)
}

/// Convert an aligned floating lot size to steps and delegate to [`allocate_target_steps`].
pub fn allocate_target_units(
    size: f64,
    lot_step: f64,
    weights: &[f64],
    remainder: f64,
) -> Result<Vec<u64>, ProfileApplicationError> {
    require_positive_finite("size", size)?;
    require_positive_finite("lot_step", lot_step)?;

    let raw_units = size / lot_step;
    if !raw_units.is_finite() || raw_units >= u64::MAX as f64 {
        return Err(ProfileApplicationError::LotUnitCountOverflow { size, lot_step });
    }
    let rounded_units = raw_units.round();
    let alignment_tolerance = LOT_ALIGNMENT_TOLERANCE * raw_units.abs().max(1.0);
    if (raw_units - rounded_units).abs() > alignment_tolerance || rounded_units < 1.0 {
        return Err(ProfileApplicationError::SizeNotMultipleOfLotStep { size, lot_step });
    }

    allocate_target_steps(rounded_units as u64, weights, remainder)
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
        let selection = p.effective_target_selection();

        // Empty ratios are the strict V2 sentinel for equal target weights.
        // Explicit ratios must correspond one-to-one when the selected target
        // count is profile-known. `All` is signal-dependent and is checked by
        // `apply_entry_signal` once the signal targets are available.
        let selected_count = match &selection {
            TargetSelection::All => None,
            TargetSelection::None => Some(0),
            TargetSelection::Selected(indices) => Some(indices.len()),
        };
        if let Some(targets) = selected_count
            && !p.close_ratios.is_empty()
            && targets != p.close_ratios.len()
        {
            return Err(ProfileError::TargetRatioMismatch {
                profile: p.name.clone(),
                targets,
                ratios: p.close_ratios.len(),
            });
        }

        // Keep both the legacy field and the effective V2 selection safe even
        // when an explicit selection takes precedence.
        let mut seen = HashSet::new();
        for &index in &p.use_targets {
            if index == 0 {
                return Err(ProfileError::ZeroTargetIndex {
                    profile: p.name.clone(),
                });
            }
            if !seen.insert(index) {
                return Err(ProfileError::DuplicateTargetIndex {
                    profile: p.name.clone(),
                    index,
                });
            }
        }
        if let TargetSelection::Selected(indices) = &selection {
            seen.clear();
            for &index in indices {
                if index == 0 {
                    return Err(ProfileError::ZeroTargetIndex {
                        profile: p.name.clone(),
                    });
                }
                if !seen.insert(index) {
                    return Err(ProfileError::DuplicateTargetIndex {
                        profile: p.name.clone(),
                        index,
                    });
                }
            }
        }

        resolve_stoploss_v2(&p.stoploss_mode, None, None, Side::Buy).map_err(|error| {
            ProfileError::InvalidConfiguration {
                profile: p.name.clone(),
                reason: error.to_string(),
            }
        })?;
        resolve_rules_v2(&p.rules, None, Side::Buy).map_err(|error| {
            ProfileError::InvalidConfiguration {
                profile: p.name.clone(),
                reason: error.to_string(),
            }
        })?;

        if p.close_ratios.is_empty() {
            return Ok(());
        }

        match validate_weights(&p.close_ratios, p.let_remainder_run) {
            Ok(_) => Ok(()),
            Err(ProfileApplicationError::InvalidTargetWeight { .. }) => {
                Err(ProfileError::ZeroRatio {
                    profile: p.name.clone(),
                })
            }
            Err(ProfileApplicationError::TargetWeightSumExceeded { sum }) => {
                Err(ProfileError::RatioSumExceeded {
                    profile: p.name.clone(),
                    sum,
                })
            }
            Err(ProfileApplicationError::TargetWeightSumIncomplete { sum }) => {
                Err(ProfileError::RatioSumIncomplete {
                    profile: p.name.clone(),
                    sum,
                })
            }
            Err(error) => unreachable!("unexpected profile weight validation error: {error}"),
        }
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
    use crate::runner::BacktestConfig;
    use crate::sizing::SizingPolicy;
    use chrono::NaiveDate;
    use qs_core::types::{CloseReason, OrderType, PositionId, Side, TargetSpec};
    use qs_symbols::SymbolSpec;

    // ── Helpers ─────────────────────────────────────────────────────────

    fn ts(h: u32, m: u32, s: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(h, m, s)
            .unwrap()
    }

    /// Convenience: build a standard Buy signal with 2 targets.
    fn buy_signal() -> RawSignal {
        RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900, 1.0950],
            group: None,
            trade_id: None,
        }
    }

    /// Convenience: build a standard Sell signal with 2 targets.
    fn sell_signal() -> RawSignal {
        RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Sell,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0900),
            targets: vec![1.0800, 1.0750],
            group: None,
            trade_id: None,
        }
    }

    fn entry_replay_config() -> BacktestConfig {
        BacktestConfig {
            initial_balance: 10_000.0,
            close_on_finish: true,
            sizing: Some(SizingPolicy::FixedLot { lots: 0.02 }),
            symbol_specs: HashMap::from([(
                "eurusd".into(),
                SymbolSpec {
                    canonical: "eurusd".into(),
                    pip_position: 4,
                    digits: 5,
                    category: "forex".into(),
                    lot_base_units: 100_000,
                    lot_step_units: 1_000,
                    lot_min_steps: 1,
                    lot_max_steps: 0,
                },
            )]),
            ..BacktestConfig::default()
        }
    }

    type ResolvedEntryFields<'a> = (
        &'a str,
        Side,
        OrderType,
        Option<f64>,
        f64,
        Option<f64>,
        &'a [TargetSpec],
        &'a [RuleConfig],
        &'a Option<String>,
    );

    trait ResolvedEntryRef {
        fn resolved_entry(&self) -> &ResolvedEntry;
    }

    impl ResolvedEntryRef for ResolvedEntry {
        fn resolved_entry(&self) -> &ResolvedEntry {
            self
        }
    }

    impl ResolvedEntryRef for Option<ResolvedEntry> {
        fn resolved_entry(&self) -> &ResolvedEntry {
            self.as_ref().expect("expected resolved Entry signal")
        }
    }

    fn unwrap_open(entry: &impl ResolvedEntryRef) -> ResolvedEntryFields<'_> {
        let entry = entry.resolved_entry();
        (
            entry.symbol.as_str(),
            entry.side,
            entry.order_type,
            entry.price,
            entry.risk_multiplier,
            entry.stoploss,
            entry.targets.as_slice(),
            entry.rules.as_slice(),
            &entry.group,
        )
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
    fn selected_single_target_with_empty_ratios_resolves_to_full_weight() {
        let toml = r#"
[[profile]]
name = "equal_one"
use_targets = [1]
close_ratios = []
"#;
        let registry = ProfileRegistry::from_toml(toml).unwrap();
        let resolved = registry
            .get("equal_one")
            .unwrap()
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(resolved.target_resolution.weights, vec![1.0]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert_eq!(resolved_targets(&resolved)[0].close_ratio, 1.0);
    }

    #[test]
    fn legacy_empty_close_ratios_resolve_to_equal_weights() {
        let profile = v2_profile(vec![1, 2], vec![], false);
        let resolved = profile.apply_entry_signal(&buy_signal()).unwrap().unwrap();

        assert_eq!(resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert_eq!(resolved.targets.len(), 2);
        assert_eq!(resolved.targets[0].close_ratio, 0.5);
        assert_eq!(resolved.targets[1].close_ratio, 0.5);
    }

    #[test]
    fn selected_three_targets_with_empty_ratios_resolve_to_equal_weights() {
        let toml = r#"
[[profile]]
name = "equal_three"
use_targets = [1, 2, 3]
close_ratios = []
"#;
        let registry = ProfileRegistry::from_toml(toml).unwrap();
        let mut signal = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut signal {
            targets.push(1.1000);
        }

        let resolved = registry
            .get("equal_three")
            .unwrap()
            .apply_entry_signal(&signal)
            .unwrap()
            .unwrap();
        let expected_weight = 1.0 / 3.0;

        assert_eq!(resolved.target_resolution.selected_indices, vec![1, 2, 3]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert!(
            resolved
                .target_resolution
                .weights
                .iter()
                .all(|weight| (weight - expected_weight).abs() <= WEIGHT_TOLERANCE)
        );
        assert!(
            resolved_targets(&resolved)
                .iter()
                .all(|target| (target.close_ratio - expected_weight).abs() <= WEIGHT_TOLERANCE)
        );
    }

    #[test]
    fn explicit_partial_ratios_require_and_honor_remainder_flag() {
        let rejected = r#"
[[profile]]
name = "no_runner"
use_targets = [1, 2]
close_ratios = [0.3, 0.3]
let_remainder_run = false
"#;
        assert!(matches!(
            ProfileRegistry::from_toml(rejected),
            Err(ProfileError::RatioSumIncomplete { .. })
        ));

        let accepted = r#"
[[profile]]
name = "runner"
use_targets = [1, 2]
close_ratios = [0.3, 0.3]
let_remainder_run = true
"#;
        let registry = ProfileRegistry::from_toml(accepted).unwrap();
        let resolved = registry
            .get("runner")
            .unwrap()
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(resolved.target_resolution.weights, vec![0.3, 0.3]);
        assert!((resolved.target_resolution.remainder - 0.4).abs() <= WEIGHT_TOLERANCE);
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
    fn non_finite_ratios_are_rejected() {
        for ratio in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let profile = v2_profile(vec![1], vec![ratio], false);
            assert!(matches!(
                profile.validate(),
                Err(ProfileError::ZeroRatio { .. })
            ));
        }
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
        let action = profile.apply_entry_signal(&signal).unwrap();
        let (sym, side, _, price, risk_multiplier, sl, targets, _, _) = unwrap_open(&action);

        assert_eq!(sym, "eurusd");
        assert_eq!(side, Side::Buy);
        assert_eq!(price, Some(1.0850));
        assert_eq!(risk_multiplier, 1.0);
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&sell_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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
        if let RawSignal::Entry { ref mut group, .. } = signal {
            *group = Some("momentum".into());
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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
        if let RawSignal::Entry { ref mut group, .. } = signal {
            *group = Some("momentum".into());
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::MissingTargetIndex {
                index: 3,
                available: 2
            })
        ));
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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
        if let RawSignal::Entry { ref mut price, .. } = signal {
            *price = None;
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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
        if let RawSignal::Entry {
            ref mut order_type,
            ref mut price,
            ref mut stoploss,
            ..
        } = signal
        {
            *order_type = OrderType::Limit;
            *price = Some(1.0800);
            *stoploss = Some(1.0750);
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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
        if let RawSignal::Entry { ref mut price, .. } = signal {
            *price = None;
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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
        if let RawSignal::Entry {
            ref mut price,
            ref mut stoploss,
            ref mut targets,
            ..
        } = signal
        {
            *price = Some(2010.0);
            *stoploss = Some(2020.0);
            *targets = vec![2000.0];
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
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
        if let RawSignal::Entry { ref mut price, .. } = signal {
            *price = None;
        }

        let action = profile.apply_entry_signal(&signal).unwrap();
        let (_, _, _, _, _, _, _, rules, _) = unwrap_open(&action);
        // Rule is skipped because no entry price to compute offset.
        assert!(rules.is_empty());
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

    #[test]
    fn shipped_profiles_have_explicit_current_target_selection() {
        let registry = ProfileRegistry::from_toml(include_str!("../profiles.toml")).unwrap();
        assert!(!registry.is_empty());
        for name in registry.names() {
            let profile = registry.get(name).unwrap();
            assert!(
                profile.target_selection.is_some(),
                "shipped profile `{name}` must state target_selection explicitly"
            );
        }
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

        let action_c = conservative.apply_entry_signal(&signal).unwrap();
        let action_a = aggressive.apply_entry_signal(&signal).unwrap();

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
        let back: RawSignal = serde_json::from_str(&json).unwrap();

        assert!(back.is_entry());
        assert_eq!(back.ts(), ts(10, 0, 0));
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
        assert_eq!(profile.target_selection, None);
        assert_eq!(
            profile.effective_target_selection(),
            TargetSelection::Selected(vec![1, 2])
        );
        assert_eq!(profile.use_targets, vec![1, 2]);
        assert_eq!(profile.close_ratios, vec![0.5, 0.5]);
        assert_eq!(profile.group_override.as_deref(), Some("my_group"));
        assert!(profile.let_remainder_run);
        assert_eq!(profile.rules.len(), 1);
    }

    #[test]
    fn serde_target_selection_variants_are_actual_profile_fields() {
        let toml_input = r#"
[[profile]]
name = "all"
target_selection = "All"
use_targets = [1]
close_ratios = []

[[profile]]
name = "none"
target_selection = "None"
use_targets = [1]
close_ratios = []

[[profile]]
name = "selected"
target_selection = { Selected = [2, 1] }
use_targets = [1]
close_ratios = [0.6, 0.4]
"#;
        let registry = ProfileRegistry::from_toml(toml_input).unwrap();

        assert_eq!(
            registry.get("all").unwrap().target_selection,
            Some(TargetSelection::All)
        );
        assert_eq!(
            registry.get("none").unwrap().target_selection,
            Some(TargetSelection::None)
        );
        assert_eq!(
            registry.get("selected").unwrap().target_selection,
            Some(TargetSelection::Selected(vec![2, 1]))
        );

        let json = serde_json::to_value(registry.get("selected").unwrap()).unwrap();
        assert_eq!(
            json["target_selection"]["Selected"],
            serde_json::json!([2, 1])
        );
        assert_eq!(json["use_targets"], serde_json::json!([1]));
    }

    #[test]
    fn legacy_profile_omits_target_selection_and_keeps_legacy_default() {
        let toml_input = r#"
[[profile]]
name = "legacy"
use_targets = [1]
close_ratios = [1.0]
"#;
        let registry = ProfileRegistry::from_toml(toml_input).unwrap();
        let profile = registry.get("legacy").unwrap();

        assert_eq!(profile.target_selection, None);
        assert_eq!(
            profile.effective_target_selection(),
            TargetSelection::Selected(vec![1])
        );
        let json = serde_json::to_value(profile).unwrap();
        assert!(json.get("target_selection").is_none());
        assert_eq!(
            unwrap_open(&profile.apply_entry_signal(&buy_signal()).unwrap())
                .6
                .len(),
            1
        );
    }

    // ── Integration: profile produces valid signals for runner ───────────

    #[test]
    fn profile_produces_valid_signals_for_runner() {
        use crate::data_feed::{MarketEvent, VecFeed};
        use crate::runner::BacktestRunner;

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
        let runner = BacktestRunner::new(entry_replay_config());
        let result = runner.run_raw_signals(&mut feed, raw_signals, Some(profile));

        // The trade should have been opened and TP should trigger.
        assert_eq!(result.total_trades, 1);
        assert!(result.total_pnl > 0.0);
        assert_eq!(result.trade_log[0].close_reason, CloseReason::Target);
    }

    #[test]
    fn same_signals_different_profiles_different_results() {
        use crate::data_feed::{MarketEvent, VecFeed};
        use crate::runner::BacktestRunner;

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

        let raw_signals = vec![buy_signal()];

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
        let profile_a = reg.get("tp1_only").unwrap();
        let mut feed_a = VecFeed::new(events.clone());
        let config = entry_replay_config();
        let result_a = BacktestRunner::new(config.clone()).run_raw_signals(
            &mut feed_a,
            raw_signals.clone(),
            Some(profile_a),
        );

        // Profile B: close 50% at TP1, 50% at TP2.
        // TP2 never hit, so remaining closes at end (close_on_finish).
        let profile_b = reg.get("tp1_tp2").unwrap();
        let mut feed_b = VecFeed::new(events);
        let result_b = BacktestRunner::new(config).run_raw_signals(
            &mut feed_b,
            raw_signals.clone(),
            Some(profile_b),
        );

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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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

        let action = profile.apply_entry_signal(&sell_signal()).unwrap();
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
            target_selection: None,
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
        let action = profile.apply_entry_signal(&buy_signal()).unwrap();
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
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: Some("t1".into()),
        };
        assert_eq!(sig.ts(), ts(10, 0, 0));
    }

    #[test]
    fn raw_signal_close_has_correct_ts() {
        let sig = RawSignal::Close {
            ts: ts(11, 30, 0),
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        };
        assert!(sig.is_entry());
    }

    #[test]
    fn raw_signal_is_entry_false_for_close() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "eurusd".into(),
            },
        };
        assert!(!sig.is_entry());
    }

    #[test]
    fn serde_roundtrip_raw_signal_entry_variant() {
        let sig = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.25,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: Some("t1".into()),
        };
        let json = serde_json::to_value(&sig).unwrap();
        assert_eq!(json["risk"], 1.25);
        assert!(json.get("risk_multiplier").is_none());
        assert!(json.get("size").is_none());

        let back: RawSignal = serde_json::from_value(json).unwrap();
        assert!(matches!(
            back,
            RawSignal::Entry {
                risk_multiplier: 1.25,
                ..
            }
        ));
    }

    #[test]
    fn raw_signal_entry_requires_risk() {
        let mut json = serde_json::to_value(buy_signal()).unwrap();
        json.as_object_mut().unwrap().remove("risk");

        let error = serde_json::from_value::<RawSignal>(json).unwrap_err();
        assert!(error.to_string().contains("missing field `risk`"));
    }

    #[test]
    fn raw_signal_entry_rejects_obsolete_size_field() {
        let mut json = serde_json::to_value(buy_signal()).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("size".into(), serde_json::json!(0.1));

        let error = serde_json::from_value::<RawSignal>(json).unwrap_err();
        assert!(error.to_string().contains("unknown field `size`"));
    }

    #[test]
    fn raw_signal_entry_rejects_non_positive_risk() {
        for risk in [0.0, -1.0] {
            let mut json = serde_json::to_value(buy_signal()).unwrap();
            json.as_object_mut()
                .unwrap()
                .insert("risk".into(), serde_json::json!(risk));

            let error = serde_json::from_value::<RawSignal>(json).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("risk must be finite and greater than zero")
            );
        }
    }

    #[test]
    fn raw_signal_scale_in_keeps_size_wire_field() {
        let signal = RawSignal::ScaleIn {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "t1".into(),
            },
            price: Some(1.0860),
            size: 0.5,
        };
        let json = serde_json::to_value(&signal).unwrap();
        assert_eq!(json["size"], 0.5);
        assert!(json.get("risk").is_none());
        assert!(matches!(
            serde_json::from_value::<RawSignal>(json).unwrap(),
            RawSignal::ScaleIn { size: 0.5, .. }
        ));
    }

    #[test]
    fn serde_roundtrip_raw_signal_close() {
        let sig = RawSignal::Close {
            ts: ts(11, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "pos123".into(),
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
            PositionRef::ByTradeId {
                trade_id: "abc".into(),
            },
            PositionRef::AllOnSymbol {
                symbol: "eurusd".into(),
            },
            PositionRef::AllInGroup {
                group_id: "g1".into(),
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
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: None,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);
        let actions = resolve_signal(&sig, &resolver);
        assert!(actions.is_empty());
    }

    #[test]
    fn resolve_signal_close_single() {
        let sig = RawSignal::Close {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "eurusd".into(),
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
    fn resolve_signal_modify_target() {
        let sig = RawSignal::ModifyTarget {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
            old_price: 1.0950,
            new_price: 1.0975,
        };
        let resolver = MockResolver::with_ids(vec!["pos1"]);

        let actions = resolve_signal(&sig, &resolver);

        assert!(matches!(
            actions.as_slice(),
            [Action::ModifyTarget {
                position_id,
                old_price,
                new_price,
            }] if position_id == "pos1"
                && (*old_price - 1.0950).abs() < f64::EPSILON
                && (*new_price - 1.0975).abs() < f64::EPSILON
        ));
    }

    #[test]
    fn resolve_signal_add_rule_with_entry_info() {
        let sig = RawSignal::AddRule {
            ts: ts(10, 0, 0),
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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
                ..
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
            position: PositionRef::ByTradeId {
                trade_id: "pos1".into(),
            },
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

    #[test]
    fn apply_entry_signal_preserves_trade_id() {
        let profile = ManagementProfile {
            name: "test".into(),
            target_selection: None,
            use_targets: vec![1],
            close_ratios: vec![1.0],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let signal = RawSignal::Entry {
            ts: ts(10, 0, 0),
            symbol: "eurusd".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: Some(1.0850),
            risk_multiplier: 1.0,
            stoploss: Some(1.0800),
            targets: vec![1.0900],
            group: None,
            trade_id: Some("t1".into()),
        };

        let resolved = profile
            .apply_entry_signal(&signal)
            .expect("valid profile application")
            .expect("Expected resolved entry");
        assert_eq!(resolved.trade_id.as_deref(), Some("t1"));
    }

    // ── Strict V2 profile and target resolution ─────────────────────────

    fn v2_profile(
        use_targets: Vec<usize>,
        close_ratios: Vec<f64>,
        let_remainder_run: bool,
    ) -> ManagementProfile {
        ManagementProfile {
            name: "v2".into(),
            target_selection: None,
            use_targets,
            close_ratios,
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run,
        }
    }

    fn resolved_targets(resolved: &ResolvedEntry) -> &[TargetSpec] {
        &resolved.targets
    }

    #[test]
    fn v2_non_entry_returns_none() {
        let signal = RawSignal::CloseAll { ts: ts(10, 0, 0) };
        assert!(
            v2_profile(vec![1], vec![1.0], false)
                .apply_entry_signal(&signal)
                .unwrap()
                .is_none()
        );
        assert!(resolve_unprofiled_entry_v2(&signal).unwrap().is_none());
    }

    #[test]
    fn explicit_selection_wins_over_compatibility_field() {
        let mut profile = v2_profile(vec![1], vec![1.0], false);
        profile.target_selection = Some(TargetSelection::Selected(vec![2]));

        let resolved = profile.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            resolved.target_resolution.selection,
            TargetSelection::Selected(vec![2])
        );
        assert_eq!(resolved_targets(&resolved)[0].price, 1.0950);
    }

    #[test]
    fn v2_explicit_all_and_none_are_honored() {
        let mut all = v2_profile(vec![1], vec![], false);
        all.target_selection = Some(TargetSelection::All);
        let all_resolved = all.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            all_resolved.target_resolution.selection,
            TargetSelection::All
        );
        assert_eq!(all_resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved_targets(&all_resolved).len(), 2);

        let mut none = v2_profile(vec![1], vec![], false);
        none.target_selection = Some(TargetSelection::None);
        let none_resolved = none.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            none_resolved.target_resolution.selection,
            TargetSelection::None
        );
        assert!(resolved_targets(&none_resolved).is_empty());
    }

    #[test]
    fn v2_selected_targets_preserve_selection_order_and_metadata() {
        let resolved = v2_profile(vec![2, 1], vec![0.6, 0.4], false)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(
            resolved.target_resolution.selection,
            TargetSelection::Selected(vec![2, 1])
        );
        assert_eq!(resolved.target_resolution.selected_indices, vec![2, 1]);
        assert_eq!(resolved.target_resolution.weights, vec![0.6, 0.4]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        let targets = resolved_targets(&resolved);
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].price, 1.0950);
        assert_eq!(targets[1].price, 1.0900);
    }

    #[test]
    fn v2_empty_explicit_weights_default_to_equal_selected_weights() {
        let resolved = v2_profile(vec![1, 2], vec![], false)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert_eq!(resolved_targets(&resolved)[0].close_ratio, 0.5);
        assert_eq!(resolved_targets(&resolved)[1].close_ratio, 0.5);
    }

    #[test]
    fn v2_empty_profile_selection_means_none() {
        let resolved = v2_profile(vec![], vec![], false)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(resolved.target_resolution.selection, TargetSelection::None);
        assert!(resolved.target_resolution.selected_indices.is_empty());
        assert!(resolved.target_resolution.weights.is_empty());
        assert_eq!(resolved.target_resolution.remainder, 1.0);
        assert!(resolved_targets(&resolved).is_empty());
    }

    #[test]
    fn v2_unprofiled_uses_all_targets_with_equal_weights() {
        let mut signal = buy_signal();
        if let RawSignal::Entry {
            group, trade_id, ..
        } = &mut signal
        {
            *group = Some("source".into());
            *trade_id = Some("trade-1".into());
        }
        let resolved = resolve_unprofiled_entry_v2(&signal).unwrap().unwrap();

        assert_eq!(resolved.target_resolution.selection, TargetSelection::All);
        assert_eq!(resolved.target_resolution.selected_indices, vec![1, 2]);
        assert_eq!(resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert_eq!(resolved.risk_multiplier, 1.0);
        assert_eq!(resolved.group.as_deref(), Some("source"));
        assert_eq!(resolved.trade_id.as_deref(), Some("trade-1"));
        assert_eq!(resolved.targets.len(), 2);

        match resolved.into_action(0.25) {
            Action::Open {
                size,
                group,
                trade_id,
                targets,
                ..
            } => {
                assert_eq!(size, 0.25);
                assert_eq!(group.as_deref(), Some("source"));
                assert_eq!(trade_id.as_deref(), Some("trade-1"));
                assert_eq!(targets.len(), 2);
            }
            _ => panic!("Expected Action::Open"),
        }
    }

    #[test]
    fn v2_unprofiled_with_no_targets_is_valid() {
        let mut signal = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut signal {
            targets.clear();
        }
        let resolved = resolve_unprofiled_entry_v2(&signal).unwrap().unwrap();
        assert_eq!(resolved.target_resolution.selection, TargetSelection::All);
        assert_eq!(resolved.target_resolution.remainder, 1.0);
        assert!(resolved_targets(&resolved).is_empty());
    }

    #[test]
    fn v2_rejects_zero_duplicate_and_missing_target_indices() {
        let zero = v2_profile(vec![0], vec![1.0], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(zero, ProfileApplicationError::ZeroTargetIndex);

        let duplicate = v2_profile(vec![1, 1], vec![0.5, 0.5], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(
            duplicate,
            ProfileApplicationError::DuplicateTargetIndex { index: 1 }
        );

        let missing = v2_profile(vec![3], vec![1.0], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(
            missing,
            ProfileApplicationError::MissingTargetIndex {
                index: 3,
                available: 2
            }
        );
    }

    #[test]
    fn v2_rejects_explicit_weight_count_mismatch() {
        let error = v2_profile(vec![1, 2], vec![1.0], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(
            error,
            ProfileApplicationError::TargetWeightCountMismatch {
                targets: 2,
                weights: 1
            }
        );
    }

    #[test]
    fn v2_rejects_non_positive_and_non_finite_weights() {
        for weight in [0.0, -0.1, f64::NAN, f64::INFINITY] {
            let error = v2_profile(vec![1], vec![weight], false)
                .apply_entry_signal(&buy_signal())
                .unwrap_err();
            assert!(matches!(
                error,
                ProfileApplicationError::InvalidTargetWeight { position: 1, .. }
            ));
        }
    }

    #[test]
    fn v2_enforces_weight_sum_and_reports_remainder() {
        let exceeded = v2_profile(vec![1, 2], vec![0.6, 0.5], true)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert!(matches!(
            exceeded,
            ProfileApplicationError::TargetWeightSumExceeded { .. }
        ));

        let incomplete = v2_profile(vec![1, 2], vec![0.3, 0.3], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert!(matches!(
            incomplete,
            ProfileApplicationError::TargetWeightSumIncomplete { .. }
        ));

        let resolved = v2_profile(vec![1, 2], vec![0.3, 0.3], true)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();
        assert!((resolved.target_resolution.remainder - 0.4).abs() < 1e-12);
    }

    #[test]
    fn v2_validates_buy_and_sell_target_geometry_when_entry_known() {
        let mut buy = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut buy {
            targets[0] = 1.0800;
        }
        let buy_error = v2_profile(vec![1], vec![1.0], false)
            .apply_entry_signal(&buy)
            .unwrap_err();
        assert!(matches!(
            buy_error,
            ProfileApplicationError::InvalidTargetGeometry {
                index: 1,
                side: Side::Buy,
                ..
            }
        ));

        let mut sell = sell_signal();
        if let RawSignal::Entry { targets, .. } = &mut sell {
            targets[0] = 1.0900;
        }
        let sell_error = v2_profile(vec![1], vec![1.0], false)
            .apply_entry_signal(&sell)
            .unwrap_err();
        assert!(matches!(
            sell_error,
            ProfileApplicationError::InvalidTargetGeometry {
                index: 1,
                side: Side::Sell,
                ..
            }
        ));
    }

    #[test]
    fn v2_skips_geometry_check_when_entry_price_is_unknown() {
        let mut signal = buy_signal();
        if let RawSignal::Entry { price, targets, .. } = &mut signal {
            *price = None;
            targets[0] = 1.0;
        }
        let resolved = v2_profile(vec![1], vec![1.0], false)
            .apply_entry_signal(&signal)
            .unwrap()
            .unwrap();
        assert_eq!(resolved_targets(&resolved)[0].price, 1.0);
    }

    #[test]
    fn v2_rejects_invalid_entry_numeric_inputs() {
        for risk_multiplier in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let mut signal = buy_signal();
            if let RawSignal::Entry {
                risk_multiplier: value,
                ..
            } = &mut signal
            {
                *value = risk_multiplier;
            }
            assert!(matches!(
                resolve_unprofiled_entry_v2(&signal),
                Err(ProfileApplicationError::InvalidNumericInput { .. })
            ));
        }

        let mut signal = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut signal {
            targets[1] = f64::NAN;
        }
        assert!(matches!(
            v2_profile(vec![1], vec![1.0], false).apply_entry_signal(&signal),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));
    }

    #[test]
    fn v2_rejects_invalid_profile_numeric_inputs() {
        let mut profile = v2_profile(vec![1], vec![1.0], false);
        profile.stoploss_mode = StoplossMode::FixedDistance { distance: 0.0 };
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));

        let mut profile = v2_profile(vec![1], vec![1.0], false);
        profile.rules = vec![RuleConfigDef::TrailingStop { distance: f64::NAN }];
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));

        let mut profile = v2_profile(vec![1], vec![1.0], false);
        profile.rules = vec![RuleConfigDef::TimeExit { max_seconds: 0 }];
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidCountInput { .. })
        ));
    }

    #[test]
    fn canonical_apply_rejects_missing_target() {
        let profile = v2_profile(vec![3], vec![1.0], false);
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::MissingTargetIndex { index: 3, .. })
        ));
    }

    // ── Deterministic target lot allocation ─────────────────────────────

    #[test]
    fn allocate_target_steps_rejects_one_step_for_two_equal_targets() {
        assert_eq!(
            allocate_target_steps(1, &[0.5, 0.5], 0.0).unwrap_err(),
            ProfileApplicationError::ZeroUnitAllocation { position: 1 }
        );
    }

    #[test]
    fn allocate_target_steps_splits_two_steps_between_equal_targets() {
        assert_eq!(
            allocate_target_steps(2, &[0.5, 0.5], 0.0).unwrap(),
            vec![1, 1]
        );
    }

    #[test]
    fn allocate_target_steps_assigns_final_residue() {
        let steps = allocate_target_steps(10, &[0.333, 0.333, 0.334], 0.0).unwrap();
        assert_eq!(steps, vec![3, 3, 4]);
        assert_eq!(steps.iter().sum::<u64>(), 10);
    }

    #[test]
    fn allocate_target_steps_leaves_runner_remainder_unallocated() {
        let steps = allocate_target_steps(10, &[0.3, 0.3], 0.4).unwrap();
        assert_eq!(steps, vec![3, 3]);
        assert_eq!(steps.iter().sum::<u64>(), 6);
    }

    #[test]
    fn allocate_target_units_assigns_full_sum_residue_to_final_target() {
        let units = allocate_target_units(1.0, 0.1, &[0.333, 0.333, 0.334], 0.0).unwrap();
        assert_eq!(units, vec![3, 3, 4]);
        assert_eq!(units.iter().sum::<u64>(), 10);
    }

    #[test]
    fn allocate_equal_weights_is_deterministic_at_lot_step_edges() {
        let equal_weights = vec![1.0 / 3.0; 3];

        // Floating-point division produces a value just below three, but the
        // aligned size still represents exactly three lot units.
        assert_eq!(
            allocate_target_units(0.3, 0.1, &equal_weights, 0.0).unwrap(),
            vec![1, 1, 1]
        );

        // Five units cannot be split evenly. Earlier targets round down and
        // the deterministic final target receives the full-unit residue.
        assert_eq!(
            allocate_target_units(0.05, 0.01, &equal_weights, 0.0).unwrap(),
            vec![1, 1, 3]
        );
    }

    #[test]
    fn allocate_target_units_leaves_intentional_remainder_unallocated() {
        let units = allocate_target_units(1.0, 0.1, &[0.3, 0.3], 0.4).unwrap();
        assert_eq!(units, vec![3, 3]);
        assert_eq!(units.iter().sum::<u64>(), 6);
    }

    #[test]
    fn allocate_target_units_assigns_residue_even_when_remainder_is_allowed() {
        let units = allocate_target_units(1.0, 0.1, &[0.5, 0.5], 0.0).unwrap();
        assert_eq!(units, vec![5, 5]);
    }

    #[test]
    fn allocate_target_units_rejects_invalid_size_step_and_alignment() {
        for (size, step) in [(0.0, 0.1), (1.0, 0.0), (f64::NAN, 0.1)] {
            assert!(matches!(
                allocate_target_units(size, step, &[1.0], 0.0),
                Err(ProfileApplicationError::InvalidNumericInput { .. })
            ));
        }
        assert!(matches!(
            allocate_target_units(1.0, 0.3, &[1.0], 0.0),
            Err(ProfileApplicationError::SizeNotMultipleOfLotStep { .. })
        ));
    }

    #[test]
    fn allocate_target_units_rejects_zero_unit_allocations() {
        let error = allocate_target_units(0.02, 0.01, &[0.1, 0.9], 0.0).unwrap_err();
        assert_eq!(
            error,
            ProfileApplicationError::ZeroUnitAllocation { position: 1 }
        );
    }

    #[test]
    fn profile_load_and_insert_validate_duplicates_and_all_numeric_fields_without_ratios() {
        let duplicate = r#"
[[profile]]
name = "duplicate"
use_targets = [1, 1]
close_ratios = []
"#;
        assert!(matches!(
            ProfileRegistry::from_toml(duplicate),
            Err(ProfileError::DuplicateTargetIndex { index: 1, .. })
        ));

        let base_profile = || ManagementProfile {
            name: "invalid".into(),
            target_selection: Some(TargetSelection::Selected(vec![1])),
            use_targets: vec![1],
            close_ratios: vec![],
            stoploss_mode: StoplossMode::FromSignal,
            rules: vec![],
            group_override: None,
            let_remainder_run: false,
        };

        let mut duplicate_selection = base_profile();
        duplicate_selection.target_selection = Some(TargetSelection::Selected(vec![1, 1]));
        assert!(matches!(
            ProfileRegistry::empty().insert(duplicate_selection, false),
            Err(ProfileError::DuplicateTargetIndex { index: 1, .. })
        ));

        for mode in [
            StoplossMode::FixedDistance { distance: 0.0 },
            StoplossMode::FixedDistance { distance: f64::NAN },
            StoplossMode::FixedPrice { price: 0.0 },
            StoplossMode::FixedPrice {
                price: f64::INFINITY,
            },
        ] {
            let mut profile = base_profile();
            profile.stoploss_mode = mode;
            assert!(matches!(
                ProfileRegistry::empty().insert(profile, false),
                Err(ProfileError::InvalidConfiguration { .. })
            ));
        }

        for rule in [
            RuleConfigDef::FixedStoploss { price: 0.0 },
            RuleConfigDef::TrailingStop { distance: f64::NAN },
            RuleConfigDef::TakeProfit {
                price: f64::INFINITY,
                close_ratio: 1.0,
            },
            RuleConfigDef::TakeProfit {
                price: 2.0,
                close_ratio: 1.1,
            },
            RuleConfigDef::BreakevenWhen { trigger_price: 0.0 },
            RuleConfigDef::BreakevenWhenOffset {
                trigger_price_offset: -1.0,
            },
            RuleConfigDef::BreakevenAfterTargets { after_n: 0 },
            RuleConfigDef::TimeExit { max_seconds: 0 },
        ] {
            let mut profile = base_profile();
            profile.rules.push(rule);
            assert!(matches!(
                ProfileRegistry::empty().insert(profile, false),
                Err(ProfileError::InvalidConfiguration { .. })
            ));
        }
    }

    #[test]
    fn allocate_target_units_validates_weights_and_empty_input() {
        assert_eq!(
            allocate_target_units(1.0, 0.1, &[], 1.0).unwrap(),
            Vec::<u64>::new()
        );
        assert!(matches!(
            allocate_target_units(1.0, 0.1, &[0.4, 0.4], 0.0),
            Err(ProfileApplicationError::TargetWeightRemainderMismatch { .. })
        ));
        assert!(matches!(
            allocate_target_units(1.0, 0.1, &[0.6, 0.6], 0.0),
            Err(ProfileApplicationError::TargetWeightSumExceeded { .. })
        ));
        assert!(matches!(
            allocate_target_units(1.0, 0.1, &[f64::NAN], 0.0),
            Err(ProfileApplicationError::InvalidTargetWeight { .. })
        ));
        assert!(matches!(
            allocate_target_units(1.0, 0.1, &[0.5], -0.5),
            Err(ProfileApplicationError::InvalidRemainder { .. })
        ));
        assert!(matches!(
            allocate_target_units(1.0, 0.1, &[0.5], 0.4),
            Err(ProfileApplicationError::TargetWeightRemainderMismatch { .. })
        ));
    }
}
