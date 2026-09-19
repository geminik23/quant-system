//! Management profiles — decouple entry signals from trade management.
//!
//! A [`ManagementProfile`] resolves [`RawSignal::Entry`] fields before sizing.
//! Resolved entries can be finalized into [`Action::Open`] calls after a concrete lot size is known.
//! Profile definitions are loaded by an application-owned registry for comparison without recompilation.

use std::collections::HashSet;

use chrono::NaiveDateTime;
use qs_instruments::{AdjustmentDirection, Decimal, DecimalGrid, GridRounding};
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

    #[error("entry resolution requires a price grid for {mode}")]
    MissingPriceGrid { mode: &'static str },

    #[error("entry resolution requires an entry price for {mode}")]
    MissingEntryPrice { mode: &'static str },

    #[error("entry resolution requires a signal stoploss for {mode}")]
    MissingSignalStoploss { mode: &'static str },

    #[error("price-grid resolution failed: {reason}")]
    PriceGrid { reason: String },
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
        /// Optional semantic class used by replay-owned profile routing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        entry_class: Option<String>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Scale the directional distance from the applied entry to the signal stop.
    FromSignalDistance { multiplier: f64 },
}

/// Directional geometry policy for signal stoploss and targets resolved
/// against the execution price.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryGeometryPolicy {
    /// Reject entry resolution when a signal level sits on the wrong side
    /// of the entry price.
    #[default]
    Strict,
    /// Retain crossed signal levels at profile resolution. The engine may still
    /// reject ordinary Open geometry before a position is created.
    Permissive,
}

// ─── TOML-friendly rule definition ──────────────────────────────────────────

/// Profile-specific rule definition with `#[serde(tag = "type")]` for TOML.
///
/// Converts to the core `RuleConfig` enum. Includes an offset-based
/// `BreakevenWhenOffset` variant that computes the absolute trigger price
/// from the signal's entry price at apply time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Source used to produce initial targets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum TargetSource {
    /// Select targets supplied by the entry signal.
    #[default]
    FromSignal,
    /// Generate targets from multiples of the final protective-stop distance.
    StopDistanceMultiples { multiples: Vec<f64> },
}

/// Source recorded for resolved target metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetResolutionSource {
    #[default]
    FromSignal,
    StopDistanceMultiples,
}

/// Metadata for one generated target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratedTargetResolution {
    pub ordinal: usize,
    pub multiple: f64,
    pub multiple_decimal: String,
    pub requested_price: f64,
    pub resolved_price: f64,
}

/// Origin of the price grid supplied by the replay adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceGridSource {
    InstrumentPriceGrid,
    LegacyDigitsFallback,
}

/// Instrument-aware context for profile-generated price levels.
#[derive(Debug, Clone, Copy)]
pub struct EntryResolutionContext {
    pub price_grid: DecimalGrid,
    pub price_grid_source: PriceGridSource,
}

/// Auditable requested and resolved profile levels.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EntryLevelResolution {
    pub price_grid_source: Option<PriceGridSource>,
    pub original_signal_stoploss: Option<f64>,
    pub stop_distance_multiplier: Option<f64>,
    pub stop_distance_multiplier_decimal: Option<String>,
    pub source_stop_distance: Option<f64>,
    pub final_stop_distance: Option<f64>,
    pub requested_stoploss: Option<f64>,
    pub resolved_stoploss: Option<f64>,
    pub stop_adjustment: Option<AdjustmentDirection>,
    #[serde(default)]
    pub requested_targets: Vec<f64>,
    #[serde(default)]
    pub resolved_targets: Vec<f64>,
    #[serde(default)]
    pub target_adjustments: Vec<AdjustmentDirection>,
}

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
    #[serde(default)]
    pub source: TargetResolutionSource,
    pub selection: TargetSelection,
    /// Resolved 1-based signal indices in output order.
    pub selected_indices: Vec<usize>,
    #[serde(default)]
    pub generated: Vec<GeneratedTargetResolution>,
    /// Close weights corresponding one-to-one with resolved targets.
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
    #[serde(default)]
    pub level_resolution: EntryLevelResolution,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Source used to select or generate initial targets.
    #[serde(default, skip_serializing_if = "target_source_is_default")]
    pub target_source: TargetSource,

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

    /// Directional geometry policy for the signal stoploss and targets when
    /// they are validated against the execution price. Profile rule levels
    /// and numeric validation are always strict.
    #[serde(default)]
    pub entry_geometry: EntryGeometryPolicy,
}

fn default_stoploss_mode() -> StoplossMode {
    StoplossMode::FromSignal
}

fn target_source_is_default(source: &TargetSource) -> bool {
    matches!(source, TargetSource::FromSignal)
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

    /// Transform an Entry using compatibility behavior for signal-backed levels.
    pub fn apply_entry_signal(
        &self,
        signal: &RawSignal,
    ) -> Result<Option<ResolvedEntry>, ProfileApplicationError> {
        self.apply_entry_signal_internal(signal, None)
    }

    /// Transform an Entry with the instrument price grid required by generated levels.
    pub fn apply_entry_signal_with_context(
        &self,
        signal: &RawSignal,
        context: EntryResolutionContext,
    ) -> Result<Option<ResolvedEntry>, ProfileApplicationError> {
        self.apply_entry_signal_internal(signal, Some(context))
    }

    fn apply_entry_signal_internal(
        &self,
        signal: &RawSignal,
        context: Option<EntryResolutionContext>,
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

        let (stoploss, mut level_resolution) = resolve_stoploss(
            &self.stoploss_mode,
            *signal_stoploss,
            *price,
            *side,
            self.entry_geometry,
            context,
        )?;
        let (targets, target_resolution, target_level_resolution) = resolve_target_source(
            &self.target_source,
            signal_targets,
            stoploss,
            *side,
            *price,
            self.effective_target_selection(),
            &self.close_ratios,
            self.let_remainder_run,
            self.entry_geometry,
            context,
        )?;
        level_resolution.requested_targets = target_level_resolution.requested_targets;
        level_resolution.resolved_targets = target_level_resolution.resolved_targets;
        level_resolution.target_adjustments = target_level_resolution.target_adjustments;
        if level_resolution.price_grid_source.is_none() {
            level_resolution.price_grid_source = target_level_resolution.price_grid_source;
        }
        if level_resolution.final_stop_distance.is_none() {
            level_resolution.final_stop_distance = target_level_resolution.final_stop_distance;
        }
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
            level_resolution,
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
    geometry_policy: EntryGeometryPolicy,
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
                source: TargetResolutionSource::FromSignal,
                selection,
                selected_indices,
                generated: Vec::new(),
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
            if !valid_geometry && geometry_policy == EntryGeometryPolicy::Strict {
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
            source: TargetResolutionSource::FromSignal,
            selection,
            selected_indices,
            generated: Vec::new(),
            weights,
            remainder,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn resolve_target_source(
    source: &TargetSource,
    signal_targets: &[f64],
    stoploss: Option<f64>,
    side: Side,
    entry_price: Option<f64>,
    selection: TargetSelection,
    explicit_weights: &[f64],
    let_remainder_run: bool,
    geometry_policy: EntryGeometryPolicy,
    context: Option<EntryResolutionContext>,
) -> Result<(Vec<TargetSpec>, TargetResolution, EntryLevelResolution), ProfileApplicationError> {
    match source {
        TargetSource::FromSignal => {
            let (targets, resolution) = resolve_targets(
                signal_targets,
                side,
                entry_price,
                selection,
                explicit_weights,
                let_remainder_run,
                geometry_policy,
            )?;
            let resolved_targets = targets
                .iter()
                .map(|target| target.price)
                .collect::<Vec<_>>();
            Ok((
                targets,
                resolution,
                EntryLevelResolution {
                    requested_targets: resolved_targets.clone(),
                    resolved_targets,
                    ..EntryLevelResolution::default()
                },
            ))
        }
        TargetSource::StopDistanceMultiples { multiples } => resolve_generated_targets(
            multiples,
            stoploss,
            side,
            entry_price,
            explicit_weights,
            let_remainder_run,
            context,
        ),
    }
}

fn resolve_generated_targets(
    multiples: &[f64],
    stoploss: Option<f64>,
    side: Side,
    entry_price: Option<f64>,
    explicit_weights: &[f64],
    let_remainder_run: bool,
    context: Option<EntryResolutionContext>,
) -> Result<(Vec<TargetSpec>, TargetResolution, EntryLevelResolution), ProfileApplicationError> {
    if multiples.is_empty() {
        return Err(ProfileApplicationError::PriceGrid {
            reason: "generated target multiples cannot be empty".to_owned(),
        });
    }
    let entry = entry_price.ok_or(ProfileApplicationError::MissingEntryPrice {
        mode: "stop-distance targets",
    })?;
    let stop = stoploss.ok_or(ProfileApplicationError::MissingSignalStoploss {
        mode: "stop-distance targets",
    })?;
    validate_stop_geometry(side, entry, stop)?;
    let context = context.ok_or(ProfileApplicationError::MissingPriceGrid {
        mode: "stop-distance targets",
    })?;

    let entry_decimal = decimal_from_f64(entry)?;
    let stop_decimal = decimal_from_f64(stop)?;
    context
        .price_grid
        .adjust(stop_decimal, GridRounding::Reject)
        .map_err(price_grid_error)?;
    let distance = directional_stop_distance(side, entry_decimal, stop_decimal)?;

    let weights = if explicit_weights.is_empty() {
        vec![1.0 / multiples.len() as f64; multiples.len()]
    } else {
        if explicit_weights.len() != multiples.len() {
            return Err(ProfileApplicationError::TargetWeightCountMismatch {
                targets: multiples.len(),
                weights: explicit_weights.len(),
            });
        }
        explicit_weights.to_vec()
    };
    let remainder = validate_weights(&weights, let_remainder_run)?;

    let mut targets = Vec::with_capacity(multiples.len());
    let mut generated = Vec::with_capacity(multiples.len());
    let mut requested_targets = Vec::with_capacity(multiples.len());
    let mut resolved_targets = Vec::with_capacity(multiples.len());
    let mut target_adjustments = Vec::with_capacity(multiples.len());
    let mut seen = HashSet::with_capacity(multiples.len());
    let mut seen_engine_prices = HashSet::with_capacity(multiples.len());
    for (offset, (&multiple, &weight)) in multiples.iter().zip(&weights).enumerate() {
        require_positive_finite(format!("target multiple {}", offset + 1), multiple)?;
        let multiple_decimal = decimal_from_f64(multiple)?;
        let offset_decimal = distance
            .checked_mul(multiple_decimal)
            .map_err(price_grid_error)?;
        let requested = match side {
            Side::Buy => entry_decimal.checked_add(offset_decimal),
            Side::Sell => entry_decimal.checked_sub(offset_decimal),
        }
        .map_err(price_grid_error)?;
        let rounding = match side {
            Side::Buy => GridRounding::Ceil,
            Side::Sell => GridRounding::Floor,
        };
        let adjustment = context
            .price_grid
            .adjust(requested, rounding)
            .map_err(price_grid_error)?;
        if !seen.insert(adjustment.adjusted) {
            return Err(ProfileApplicationError::DuplicateTargetPrice {
                price: decimal_to_f64(adjustment.adjusted),
            });
        }
        let requested_price = decimal_to_f64(requested);
        let resolved_price = decimal_to_f64(adjustment.adjusted);
        let engine_price_key = (resolved_price * 1_000_000.0).round() as i64;
        if !seen_engine_prices.insert(engine_price_key) {
            return Err(ProfileApplicationError::DuplicateTargetPrice {
                price: resolved_price,
            });
        }
        require_positive_finite(format!("generated target {}", offset + 1), resolved_price)?;
        validate_target_geometry(offset + 1, side, entry, resolved_price)?;
        targets.push(TargetSpec {
            price: resolved_price,
            close_ratio: weight,
        });
        generated.push(GeneratedTargetResolution {
            ordinal: offset + 1,
            multiple,
            multiple_decimal: multiple_decimal.to_string(),
            requested_price,
            resolved_price,
        });
        requested_targets.push(requested_price);
        resolved_targets.push(resolved_price);
        target_adjustments.push(adjustment.direction);
    }

    Ok((
        targets,
        TargetResolution {
            source: TargetResolutionSource::StopDistanceMultiples,
            selection: TargetSelection::None,
            selected_indices: Vec::new(),
            generated,
            weights,
            remainder,
        },
        EntryLevelResolution {
            price_grid_source: Some(context.price_grid_source),
            final_stop_distance: Some(decimal_to_f64(distance)),
            requested_targets,
            resolved_targets,
            target_adjustments,
            ..EntryLevelResolution::default()
        },
    ))
}

fn decimal_from_f64(value: f64) -> Result<Decimal, ProfileApplicationError> {
    Decimal::checked_from_f64(value).map_err(price_grid_error)
}

fn decimal_to_f64(value: Decimal) -> f64 {
    value.coefficient() as f64 / 10_f64.powi(i32::from(value.scale()))
}

fn price_grid_error(error: impl std::fmt::Display) -> ProfileApplicationError {
    ProfileApplicationError::PriceGrid {
        reason: error.to_string(),
    }
}

fn directional_stop_distance(
    side: Side,
    entry: Decimal,
    stop: Decimal,
) -> Result<Decimal, ProfileApplicationError> {
    let distance = match side {
        Side::Buy => entry.checked_sub(stop),
        Side::Sell => stop.checked_sub(entry),
    }
    .map_err(price_grid_error)?;
    if distance.is_positive() {
        Ok(distance)
    } else {
        Err(ProfileApplicationError::PriceGrid {
            reason: "stop distance must be strictly protective".to_owned(),
        })
    }
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
    geometry_policy: EntryGeometryPolicy,
    context: Option<EntryResolutionContext>,
) -> Result<(Option<f64>, EntryLevelResolution), ProfileApplicationError> {
    let mut resolution = EntryLevelResolution {
        original_signal_stoploss: signal_stoploss,
        ..EntryLevelResolution::default()
    };
    let (stoploss, signal_level) = match mode {
        StoplossMode::FromSignal => (signal_stoploss, true),
        StoplossMode::None => (None, false),
        StoplossMode::FixedDistance { distance } => {
            require_positive_finite("stoploss fixed distance", *distance)?;
            (
                entry_price.map(|entry| match side {
                    Side::Buy => entry - distance,
                    Side::Sell => entry + distance,
                }),
                false,
            )
        }
        StoplossMode::FixedPrice { price } => {
            require_positive_finite("stoploss fixed price", *price)?;
            (Some(*price), false)
        }
        StoplossMode::FromSignalDistance { multiplier } => {
            require_positive_finite("stoploss signal-distance multiplier", *multiplier)?;
            let entry = entry_price.ok_or(ProfileApplicationError::MissingEntryPrice {
                mode: "signal-distance stoploss",
            })?;
            let signal_stop =
                signal_stoploss.ok_or(ProfileApplicationError::MissingSignalStoploss {
                    mode: "signal-distance stoploss",
                })?;
            validate_stop_geometry(side, entry, signal_stop)?;
            let context = context.ok_or(ProfileApplicationError::MissingPriceGrid {
                mode: "signal-distance stoploss",
            })?;
            let entry_decimal = decimal_from_f64(entry)?;
            let stop_decimal = decimal_from_f64(signal_stop)?;
            let distance = directional_stop_distance(side, entry_decimal, stop_decimal)?;
            let multiplier_decimal = decimal_from_f64(*multiplier)?;
            let scaled = distance
                .checked_mul(multiplier_decimal)
                .map_err(price_grid_error)?;
            let requested = match side {
                Side::Buy => entry_decimal.checked_sub(scaled),
                Side::Sell => entry_decimal.checked_add(scaled),
            }
            .map_err(price_grid_error)?;
            let rounding = match side {
                Side::Buy => GridRounding::Floor,
                Side::Sell => GridRounding::Ceil,
            };
            let adjustment = context
                .price_grid
                .adjust(requested, rounding)
                .map_err(price_grid_error)?;
            let requested_price = decimal_to_f64(requested);
            let resolved_price = decimal_to_f64(adjustment.adjusted);
            resolution.price_grid_source = Some(context.price_grid_source);
            let final_distance =
                directional_stop_distance(side, entry_decimal, adjustment.adjusted)?;
            resolution.stop_distance_multiplier = Some(*multiplier);
            resolution.stop_distance_multiplier_decimal = Some(multiplier_decimal.to_string());
            resolution.source_stop_distance = Some(decimal_to_f64(distance));
            resolution.final_stop_distance = Some(decimal_to_f64(final_distance));
            resolution.requested_stoploss = Some(requested_price);
            resolution.resolved_stoploss = Some(resolved_price);
            resolution.stop_adjustment = Some(adjustment.direction);
            (Some(resolved_price), false)
        }
    };
    if let Some(stoploss) = stoploss {
        require_positive_finite("resolved stoploss", stoploss)?;
        if let Some(entry) = entry_price
            && (!signal_level || geometry_policy == EntryGeometryPolicy::Strict)
        {
            validate_stop_geometry(side, entry, stoploss)?;
        }
        resolution.resolved_stoploss.get_or_insert(stoploss);
        resolution.requested_stoploss.get_or_insert(stoploss);
    }
    Ok((stoploss, resolution))
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
        EntryGeometryPolicy::Strict,
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
        level_resolution: EntryLevelResolution {
            original_signal_stoploss: *stoploss,
            requested_stoploss: *stoploss,
            resolved_stoploss: *stoploss,
            requested_targets: signal_targets.clone(),
            resolved_targets: signal_targets.clone(),
            ..EntryLevelResolution::default()
        },
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
    let invalid = |reason: String| ProfileValidationError::InvalidConfiguration {
        profile: p.name.clone(),
        reason,
    };
    let selection = p.effective_target_selection();

    let selected_count = match &p.target_source {
        TargetSource::FromSignal => match &selection {
            TargetSelection::All => None,
            TargetSelection::None => Some(0),
            TargetSelection::Selected(indices) => Some(indices.len()),
        },
        TargetSource::StopDistanceMultiples { multiples } => {
            if p.target_selection.is_some() || !p.use_targets.is_empty() {
                return Err(invalid(
                    "generated targets cannot be combined with signal target selection".into(),
                ));
            }
            if multiples.is_empty() {
                return Err(invalid("generated target multiples cannot be empty".into()));
            }
            let mut previous = None;
            for (offset, &multiple) in multiples.iter().enumerate() {
                if !multiple.is_finite() || multiple <= 0.0 {
                    return Err(invalid(format!(
                        "target multiple {} must be finite and positive",
                        offset + 1
                    )));
                }
                if previous.is_some_and(|value| multiple <= value) {
                    return Err(invalid(
                        "generated target multiples must be strictly increasing".into(),
                    ));
                }
                previous = Some(multiple);
            }
            if matches!(p.stoploss_mode, StoplossMode::None) {
                return Err(invalid(
                    "generated targets require a protective stoploss mode".into(),
                ));
            }
            if p.rules
                .iter()
                .any(|rule| matches!(rule, RuleConfigDef::TakeProfit { .. }))
            {
                return Err(invalid(
                    "generated targets cannot be combined with take-profit rules".into(),
                ));
            }
            for rule in &p.rules {
                if let RuleConfigDef::BreakevenAfterTargets { after_n } = rule
                    && *after_n as usize > multiples.len()
                {
                    return Err(invalid(format!(
                        "breakeven target count {after_n} exceeds generated target count {}",
                        multiples.len()
                    )));
                }
            }
            Some(multiples.len())
        }
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

    match &p.stoploss_mode {
        StoplossMode::FixedDistance { distance } => {
            require_positive_finite("stoploss fixed distance", *distance)
                .map_err(|error| invalid(error.to_string()))?;
        }
        StoplossMode::FixedPrice { price } => {
            require_positive_finite("stoploss fixed price", *price)
                .map_err(|error| invalid(error.to_string()))?;
        }
        StoplossMode::FromSignalDistance { multiplier } => {
            require_positive_finite("stoploss signal-distance multiplier", *multiplier)
                .map_err(|error| invalid(error.to_string()))?;
        }
        StoplossMode::FromSignal | StoplossMode::None => {}
    }
    resolve_rules(&p.rules, None, Side::Buy).map_err(|error| invalid(error.to_string()))?;

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
