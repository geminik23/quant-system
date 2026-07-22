//! Management profiles — decouple entry signals from trade management.
//!
//! A [`ManagementProfile`] resolves [`RawSignal::Entry`] fields before sizing.
//! Resolved entries can be finalized into [`Action::Open`] calls after a concrete lot size is known.
//! Profile definitions are loaded by an application-owned registry for comparison without recompilation.

use std::collections::HashSet;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::TradeEngine;
use crate::types::{
    Action, GroupId, OrderType, PositionId, PositionStatus, RuleConfig, Side, TargetSpec, TradeId,
};

// ─── Errors ─────────────────────────────────────────────────────────────────

/// Errors returned while validating a management profile definition.
#[derive(Debug, thiserror::Error)]
pub enum ProfileValidationError {
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
}

/// Strict validation failures returned by the canonical entry resolvers.
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

impl PositionResolver for TradeEngine {
    fn resolve(&self, position: &PositionRef) -> Vec<PositionId> {
        match position {
            PositionRef::ByTradeId { trade_id } => {
                self.manager.id_by_trade_id(trade_id).into_iter().collect()
            }
            PositionRef::AllOnSymbol { symbol } => self.manager.open_ids_by_symbol_sorted(symbol),
            PositionRef::AllInGroup { group_id } => {
                let mut ids = self.manager.open_ids_by_group(group_id);
                ids.sort();
                ids
            }
        }
    }

    fn position_entry_info(&self, id: &PositionId) -> Option<(f64, Side)> {
        self.get_position(id).and_then(|position| {
            if position.data.status == PositionStatus::Open {
                Some((position.data.average_entry(), position.data.side))
            } else {
                None
            }
        })
    }
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

// ─── Strict target resolution ────────────────────────────────────────────────

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
    /// compatibility decoding derives the prior behavior from `use_targets`: an empty vector means
    /// [`TargetSelection::None`], otherwise it means [`TargetSelection::Selected`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_selection: Option<TargetSelection>,

    /// Compatibility target selection (1-indexed), retained so existing serialized
    /// profiles remain readable.
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
    /// preserves existing profile behavior by deriving `None`/`Selected` from
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
    pub fn validate(&self) -> Result<(), ProfileValidationError> {
        validate_profile(self)
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
        let (targets, target_resolution) = resolve_targets(
            signal_targets,
            *side,
            *price,
            selection,
            &self.close_ratios,
            self.let_remainder_run,
        )?;
        let stoploss = resolve_stoploss(&self.stoploss_mode, *signal_stoploss, *price, *side)?;
        let rules = resolve_rules(&self.rules, *price, *side)?;

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

fn resolve_targets(
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

fn validate_stop_geometry(
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

fn validate_target_geometry(
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

fn resolve_stoploss(
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
            validate_stop_geometry(side, entry, stoploss)?;
        }
    }
    Ok(stoploss)
}

fn resolve_rules(
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
                    validate_stop_geometry(side, entry, *price)?;
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
                    validate_stop_geometry(side, entry, initial_stop)?;
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
                    validate_target_geometry(position, side, entry, *price)?;
                }
            }
            RuleConfigDef::BreakevenWhen { trigger_price } => {
                require_positive_finite(
                    format!("rule {position} breakeven trigger price"),
                    *trigger_price,
                )?;
                if let Some(entry) = entry_price {
                    validate_target_geometry(position, side, entry, *trigger_price)?;
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
                    validate_target_geometry(position, side, entry, *trigger_price)?;
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
pub fn resolve_unprofiled_entry(
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
    let (targets, target_resolution) = resolve_targets(
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

/// Validate a management profile without performing configuration I/O.
pub fn validate_profile(p: &ManagementProfile) -> Result<(), ProfileValidationError> {
    let selection = p.effective_target_selection();

    // Empty ratios are the strict sentinel for equal target weights.
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
        return Err(ProfileValidationError::TargetRatioMismatch {
            profile: p.name.clone(),
            targets,
            ratios: p.close_ratios.len(),
        });
    }

    // Keep both the legacy field and the effective strict selection safe even
    // when an explicit selection takes precedence.
    let mut seen = HashSet::new();
    for &index in &p.use_targets {
        if index == 0 {
            return Err(ProfileValidationError::ZeroTargetIndex {
                profile: p.name.clone(),
            });
        }
        if !seen.insert(index) {
            return Err(ProfileValidationError::DuplicateTargetIndex {
                profile: p.name.clone(),
                index,
            });
        }
    }
    if let TargetSelection::Selected(indices) = &selection {
        seen.clear();
        for &index in indices {
            if index == 0 {
                return Err(ProfileValidationError::ZeroTargetIndex {
                    profile: p.name.clone(),
                });
            }
            if !seen.insert(index) {
                return Err(ProfileValidationError::DuplicateTargetIndex {
                    profile: p.name.clone(),
                    index,
                });
            }
        }
    }

    resolve_stoploss(&p.stoploss_mode, None, None, Side::Buy).map_err(|error| {
        ProfileValidationError::InvalidConfiguration {
            profile: p.name.clone(),
            reason: error.to_string(),
        }
    })?;
    resolve_rules(&p.rules, None, Side::Buy).map_err(|error| {
        ProfileValidationError::InvalidConfiguration {
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
            Err(ProfileValidationError::ZeroRatio {
                profile: p.name.clone(),
            })
        }
        Err(ProfileApplicationError::TargetWeightSumExceeded { sum }) => {
            Err(ProfileValidationError::RatioSumExceeded {
                profile: p.name.clone(),
                sum,
            })
        }
        Err(ProfileApplicationError::TargetWeightSumIncomplete { sum }) => {
            Err(ProfileValidationError::RatioSumIncomplete {
                profile: p.name.clone(),
                sum,
            })
        }
        Err(error) => unreachable!("unexpected profile weight validation error: {error}"),
    }
}
