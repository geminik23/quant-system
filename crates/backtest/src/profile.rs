//! Backtest compatibility surface and configuration loader for core management profiles.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

pub use qs_core::profile::*;

/// Configuration-loading and registry failures for management profiles.
#[derive(Debug, thiserror::Error)]
pub enum ProfileRegistryError {
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

/// Compatibility name retained for existing `qs_backtest::profile` callers.
pub type ProfileError = ProfileRegistryError;

impl From<ProfileValidationError> for ProfileRegistryError {
    fn from(error: ProfileValidationError) -> Self {
        match error {
            ProfileValidationError::TargetRatioMismatch {
                profile,
                targets,
                ratios,
            } => Self::TargetRatioMismatch {
                profile,
                targets,
                ratios,
            },
            ProfileValidationError::RatioSumExceeded { profile, sum } => {
                Self::RatioSumExceeded { profile, sum }
            }
            ProfileValidationError::RatioSumIncomplete { profile, sum } => {
                Self::RatioSumIncomplete { profile, sum }
            }
            ProfileValidationError::ZeroRatio { profile } => Self::ZeroRatio { profile },
            ProfileValidationError::ZeroTargetIndex { profile } => {
                Self::ZeroTargetIndex { profile }
            }
            ProfileValidationError::DuplicateTargetIndex { profile, index } => {
                Self::DuplicateTargetIndex { profile, index }
            }
            ProfileValidationError::InvalidConfiguration { profile, reason } => {
                Self::InvalidConfiguration { profile, reason }
            }
        }
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
        qs_core::profile::validate_profile(p).map_err(Into::into)
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
    use chrono::{NaiveDate, NaiveDateTime};
    use qs_core::types::{
        Action, CloseReason, OrderType, PositionId, RuleConfig, Side, TargetSpec,
    };
    use qs_symbols::SymbolSpec;

    const WEIGHT_TOLERANCE: f64 = 1e-12;

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
        let profile = strict_profile(vec![1, 2], vec![], false);
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
            let profile = strict_profile(vec![1], vec![ratio], false);
            assert!(matches!(
                profile.validate(),
                Err(ProfileValidationError::ZeroRatio { .. })
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

    // ── Strict strict profile and target resolution ─────────────────────────

    fn strict_profile(
        use_targets: Vec<usize>,
        close_ratios: Vec<f64>,
        let_remainder_run: bool,
    ) -> ManagementProfile {
        ManagementProfile {
            name: "strict".into(),
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
    fn non_entry_returns_none() {
        let signal = RawSignal::CloseAll { ts: ts(10, 0, 0) };
        assert!(
            strict_profile(vec![1], vec![1.0], false)
                .apply_entry_signal(&signal)
                .unwrap()
                .is_none()
        );
        assert!(resolve_unprofiled_entry(&signal).unwrap().is_none());
    }

    #[test]
    fn explicit_selection_wins_over_compatibility_field() {
        let mut profile = strict_profile(vec![1], vec![1.0], false);
        profile.target_selection = Some(TargetSelection::Selected(vec![2]));

        let resolved = profile.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            resolved.target_resolution.selection,
            TargetSelection::Selected(vec![2])
        );
        assert_eq!(resolved_targets(&resolved)[0].price, 1.0950);
    }

    #[test]
    fn explicit_all_and_none_are_honored() {
        let mut all = strict_profile(vec![1], vec![], false);
        all.target_selection = Some(TargetSelection::All);
        let all_resolved = all.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            all_resolved.target_resolution.selection,
            TargetSelection::All
        );
        assert_eq!(all_resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved_targets(&all_resolved).len(), 2);

        let mut none = strict_profile(vec![1], vec![], false);
        none.target_selection = Some(TargetSelection::None);
        let none_resolved = none.apply_entry_signal(&buy_signal()).unwrap().unwrap();
        assert_eq!(
            none_resolved.target_resolution.selection,
            TargetSelection::None
        );
        assert!(resolved_targets(&none_resolved).is_empty());
    }

    #[test]
    fn selected_targets_preserve_selection_order_and_metadata() {
        let resolved = strict_profile(vec![2, 1], vec![0.6, 0.4], false)
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
    fn empty_explicit_weights_default_to_equal_selected_weights() {
        let resolved = strict_profile(vec![1, 2], vec![], false)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();

        assert_eq!(resolved.target_resolution.weights, vec![0.5, 0.5]);
        assert_eq!(resolved.target_resolution.remainder, 0.0);
        assert_eq!(resolved_targets(&resolved)[0].close_ratio, 0.5);
        assert_eq!(resolved_targets(&resolved)[1].close_ratio, 0.5);
    }

    #[test]
    fn empty_profile_selection_means_none() {
        let resolved = strict_profile(vec![], vec![], false)
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
    fn unprofiled_uses_all_targets_with_equal_weights() {
        let mut signal = buy_signal();
        if let RawSignal::Entry {
            group, trade_id, ..
        } = &mut signal
        {
            *group = Some("source".into());
            *trade_id = Some("trade-1".into());
        }
        let resolved = resolve_unprofiled_entry(&signal).unwrap().unwrap();

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
    fn unprofiled_with_no_targets_is_valid() {
        let mut signal = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut signal {
            targets.clear();
        }
        let resolved = resolve_unprofiled_entry(&signal).unwrap().unwrap();
        assert_eq!(resolved.target_resolution.selection, TargetSelection::All);
        assert_eq!(resolved.target_resolution.remainder, 1.0);
        assert!(resolved_targets(&resolved).is_empty());
    }

    #[test]
    fn rejects_zero_duplicate_and_missing_target_indices() {
        let zero = strict_profile(vec![0], vec![1.0], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(zero, ProfileApplicationError::ZeroTargetIndex);

        let duplicate = strict_profile(vec![1, 1], vec![0.5, 0.5], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert_eq!(
            duplicate,
            ProfileApplicationError::DuplicateTargetIndex { index: 1 }
        );

        let missing = strict_profile(vec![3], vec![1.0], false)
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
    fn rejects_explicit_weight_count_mismatch() {
        let error = strict_profile(vec![1, 2], vec![1.0], false)
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
    fn rejects_non_positive_and_non_finite_weights() {
        for weight in [0.0, -0.1, f64::NAN, f64::INFINITY] {
            let error = strict_profile(vec![1], vec![weight], false)
                .apply_entry_signal(&buy_signal())
                .unwrap_err();
            assert!(matches!(
                error,
                ProfileApplicationError::InvalidTargetWeight { position: 1, .. }
            ));
        }
    }

    #[test]
    fn enforces_weight_sum_and_reports_remainder() {
        let exceeded = strict_profile(vec![1, 2], vec![0.6, 0.5], true)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert!(matches!(
            exceeded,
            ProfileApplicationError::TargetWeightSumExceeded { .. }
        ));

        let incomplete = strict_profile(vec![1, 2], vec![0.3, 0.3], false)
            .apply_entry_signal(&buy_signal())
            .unwrap_err();
        assert!(matches!(
            incomplete,
            ProfileApplicationError::TargetWeightSumIncomplete { .. }
        ));

        let resolved = strict_profile(vec![1, 2], vec![0.3, 0.3], true)
            .apply_entry_signal(&buy_signal())
            .unwrap()
            .unwrap();
        assert!((resolved.target_resolution.remainder - 0.4).abs() < 1e-12);
    }

    #[test]
    fn validates_buy_and_sell_target_geometry_when_entry_known() {
        let mut buy = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut buy {
            targets[0] = 1.0800;
        }
        let buy_error = strict_profile(vec![1], vec![1.0], false)
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
        let sell_error = strict_profile(vec![1], vec![1.0], false)
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
    fn skips_geometry_check_when_entry_price_is_unknown() {
        let mut signal = buy_signal();
        if let RawSignal::Entry { price, targets, .. } = &mut signal {
            *price = None;
            targets[0] = 1.0;
        }
        let resolved = strict_profile(vec![1], vec![1.0], false)
            .apply_entry_signal(&signal)
            .unwrap()
            .unwrap();
        assert_eq!(resolved_targets(&resolved)[0].price, 1.0);
    }

    #[test]
    fn rejects_invalid_entry_numeric_inputs() {
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
                resolve_unprofiled_entry(&signal),
                Err(ProfileApplicationError::InvalidNumericInput { .. })
            ));
        }

        let mut signal = buy_signal();
        if let RawSignal::Entry { targets, .. } = &mut signal {
            targets[1] = f64::NAN;
        }
        assert!(matches!(
            strict_profile(vec![1], vec![1.0], false).apply_entry_signal(&signal),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));
    }

    #[test]
    fn rejects_invalid_profile_numeric_inputs() {
        let mut profile = strict_profile(vec![1], vec![1.0], false);
        profile.stoploss_mode = StoplossMode::FixedDistance { distance: 0.0 };
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));

        let mut profile = strict_profile(vec![1], vec![1.0], false);
        profile.rules = vec![RuleConfigDef::TrailingStop { distance: f64::NAN }];
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidNumericInput { .. })
        ));

        let mut profile = strict_profile(vec![1], vec![1.0], false);
        profile.rules = vec![RuleConfigDef::TimeExit { max_seconds: 0 }];
        assert!(matches!(
            profile.apply_entry_signal(&buy_signal()),
            Err(ProfileApplicationError::InvalidCountInput { .. })
        ));
    }

    #[test]
    fn canonical_apply_rejects_missing_target() {
        let profile = strict_profile(vec![3], vec![1.0], false);
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
