//! `qs-symbols` — Symbol registry for the quant-system workspace.
//!
//! Provides a TOML-driven symbol registry with canonical name normalization,
//! price precision metadata (pip/digit), and lot specification. Shared across
//! all crates to replace hardcoded symbol mappings.

pub mod error;

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub use error::SymbolError;

/// Convenience result type for this crate.
pub type Result<T> = std::result::Result<T, SymbolError>;

// ─── TOML deserialization helpers ───────────────────────────────────────────

/// Raw TOML file layout: `{ symbol: Vec<SymbolEntry> }`.
#[derive(Deserialize)]
struct TomlFile {
    symbol: Vec<SymbolEntry>,
}

/// One `[[symbol]]` entry in the TOML file.
#[derive(Deserialize)]
struct SymbolEntry {
    canonical: String,
    #[serde(default)]
    aliases: Vec<String>,
    pip_position: u16,
    digits: u16,
    category: String,
    lot_base_units: i64,
    lot_step_units: i64,
    #[serde(default = "default_lot_min_steps")]
    lot_min_steps: i64,
    #[serde(default)]
    lot_max_steps: i64,
}

fn default_lot_min_steps() -> i64 {
    1
}

// ─── SymbolSpec ─────────────────────────────────────────────────────────────

/// Full symbol specification — price precision, lot specification, and category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpec {
    /// Canonical (normalized) symbol name. Always lowercase, no separators.
    pub canonical: String,
    /// Pip position (e.g. 4 for forex majors, 2 for JPY pairs).
    pub pip_position: u16,
    /// Number of decimal digits in the price (e.g. 5 for EURUSD, 2 for XAUUSD).
    pub digits: u16,
    /// Instrument category: "forex", "metal", "index", "commodity", "crypto".
    pub category: String,
    /// How many base units make 1.0 standard lot (e.g. 100_000 for forex).
    pub lot_base_units: i64,
    /// How many base units per minimum lot step (e.g. 1_000 for forex = 0.01 lot).
    pub lot_step_units: i64,
    /// Minimum allowed lot steps (e.g. 1 means minimum is 1 × lot_step).
    pub lot_min_steps: i64,
    /// Maximum allowed lot steps (0 = no limit).
    pub lot_max_steps: i64,
}

impl SymbolSpec {
    /// Lot step as f64: `lot_step_units / lot_base_units`.
    pub fn lot_step(&self) -> f64 {
        self.lot_step_units as f64 / self.lot_base_units as f64
    }

    /// Minimum lot as f64.
    pub fn lot_min(&self) -> f64 {
        self.lot_min_steps as f64 * self.lot_step()
    }

    /// Maximum lot as f64 (returns 0.0 when there is no limit).
    pub fn lot_max(&self) -> f64 {
        if self.lot_max_steps == 0 {
            0.0
        } else {
            self.lot_max_steps as f64 * self.lot_step()
        }
    }

    /// Convert a raw price difference to pips.
    pub fn to_pips(&self, p1: f64, p2: f64) -> f64 {
        let scale = 10i64.pow(self.digits as u32);
        let pip_scale = 10i64.pow((self.digits - self.pip_position) as u32);
        let d1 = (p1 * scale as f64).round() as i64;
        let d2 = (p2 * scale as f64).round() as i64;
        (d1 - d2) as f64 / pip_scale as f64
    }

    /// Add N pips to a price and return the adjusted price.
    pub fn add_pips(&self, price: f64, pips: f64) -> f64 {
        let scale = 10i64.pow(self.digits as u32);
        let pip_scale = 10i64.pow((self.digits - self.pip_position) as u32);
        let d_price = (price * scale as f64).round();
        let d_pips = pip_scale as f64 * pips;
        (d_price + d_pips) / scale as f64
    }
}

// ─── SymbolRegistry ─────────────────────────────────────────────────────────

/// Central symbol registry. Loaded from TOML, provides normalization and metadata lookup.
#[derive(Debug)]
pub struct SymbolRegistry {
    /// canonical_name → SymbolSpec
    symbols: HashMap<String, SymbolSpec>,
    /// alias (lowercase, stripped) → canonical_name
    alias_map: HashMap<String, String>,
}

impl SymbolRegistry {
    /// Load a registry from a TOML file path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::from_toml(&content)
    }

    /// Load a registry from a TOML string (useful for testing / embedding).
    pub fn from_toml(content: &str) -> Result<Self> {
        let file: TomlFile = toml::from_str(content)?;

        let mut symbols = HashMap::with_capacity(file.symbol.len());
        let mut alias_map = HashMap::new();

        for entry in file.symbol {
            let canonical = entry.canonical.to_lowercase();

            // Validate: no duplicate canonical names
            if symbols.contains_key(&canonical) {
                return Err(SymbolError::DuplicateCanonical(canonical));
            }

            // Validate: pip_position <= digits
            if entry.pip_position > entry.digits {
                return Err(SymbolError::InvalidDigits {
                    symbol: canonical,
                    pip: entry.pip_position,
                    digits: entry.digits,
                });
            }

            // Validate: lot_step_units > 0 and <= lot_base_units
            if entry.lot_step_units <= 0 || entry.lot_step_units > entry.lot_base_units {
                return Err(SymbolError::InvalidLotSpec {
                    symbol: canonical,
                    step: entry.lot_step_units,
                    base: entry.lot_base_units,
                });
            }

            // Register the canonical name itself as an alias
            let stripped_canonical = strip_separators(&canonical);
            if stripped_canonical != canonical {
                alias_map.insert(stripped_canonical, canonical.clone());
            }

            // Register all explicit aliases
            for alias in &entry.aliases {
                let key = strip_separators(&alias.to_lowercase());
                if let Some(existing) = alias_map.get(&key) {
                    if *existing != canonical {
                        return Err(SymbolError::DuplicateAlias {
                            alias: key,
                            existing: existing.clone(),
                            new: canonical.clone(),
                        });
                    }
                    // Same canonical — harmless duplicate, skip
                    continue;
                }
                alias_map.insert(key, canonical.clone());
            }

            let spec = SymbolSpec {
                canonical: canonical.clone(),
                pip_position: entry.pip_position,
                digits: entry.digits,
                category: entry.category,
                lot_base_units: entry.lot_base_units,
                lot_step_units: entry.lot_step_units,
                lot_min_steps: entry.lot_min_steps,
                lot_max_steps: entry.lot_max_steps,
            };

            symbols.insert(canonical, spec);
        }

        Ok(Self { symbols, alias_map })
    }

    /// Create an empty registry (for tests that don't need normalization).
    pub fn empty() -> Self {
        Self {
            symbols: HashMap::new(),
            alias_map: HashMap::new(),
        }
    }

    /// Normalize a raw symbol string to its canonical name.
    pub fn normalize(&self, raw: &str) -> Option<&str> {
        let key = strip_separators(&raw.to_lowercase());

        // Direct canonical match
        if let Some(spec) = self.symbols.get(&key) {
            return Some(&spec.canonical);
        }

        // Alias lookup
        if let Some(canonical) = self.alias_map.get(&key) {
            return Some(canonical.as_str());
        }

        None
    }

    /// Normalize, returning the input lowercased+stripped if no mapping is found.
    pub fn normalize_or_passthrough(&self, raw: &str) -> String {
        match self.normalize(raw) {
            Some(canonical) => canonical.to_owned(),
            None => strip_separators(&raw.to_lowercase()),
        }
    }

    /// Get symbol spec by canonical name.
    pub fn spec(&self, canonical: &str) -> Option<&SymbolSpec> {
        self.symbols.get(canonical)
    }

    /// Get symbol spec by any name (normalizes first, then looks up).
    pub fn spec_by_any(&self, raw: &str) -> Option<&SymbolSpec> {
        let canonical = self.normalize(raw)?;
        self.symbols.get(canonical)
    }

    /// Get digits for a symbol by canonical name.
    pub fn digits(&self, canonical: &str) -> Option<u16> {
        self.symbols.get(canonical).map(|s| s.digits)
    }

    /// Get lot step as f64 for a symbol by canonical name.
    pub fn lot_step(&self, canonical: &str) -> Option<f64> {
        self.symbols.get(canonical).map(|s| s.lot_step())
    }

    /// List all canonical symbol names (sorted).
    pub fn canonical_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.symbols.keys().map(|s| s.as_str()).collect();
        names.sort();
        names
    }

    /// Check if a symbol (canonical or alias) is known.
    pub fn is_known(&self, raw: &str) -> bool {
        self.normalize(raw).is_some()
    }

    /// Number of registered symbols.
    pub fn len(&self) -> usize {
        self.symbols.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    /// List all symbols in a given category.
    pub fn symbols_in_category(&self, category: &str) -> Vec<&SymbolSpec> {
        let cat = category.to_lowercase();
        self.symbols
            .values()
            .filter(|s| s.category == cat)
            .collect()
    }

    /// Suggest closest canonical names for an unrecognized symbol using edit distance.
    ///
    /// Returns up to `limit` matches within `max_distance`, sorted by distance
    /// then alphabetically. Each entry is `(canonical_name, distance)`.
    pub fn suggest(&self, raw: &str, max_distance: usize, limit: usize) -> Vec<(&str, usize)> {
        if limit == 0 {
            return Vec::new();
        }

        let key = strip_separators(&raw.to_lowercase());
        if key.is_empty() {
            return Vec::new();
        }

        // Already known — no suggestions needed
        if self.normalize(raw).is_some() {
            return Vec::new();
        }

        let mut candidates: Vec<(&str, usize)> = self
            .symbols
            .keys()
            .filter_map(|canonical| {
                let d = levenshtein(&key, canonical);
                if d <= max_distance {
                    Some((canonical.as_str(), d))
                } else {
                    None
                }
            })
            .collect();

        // Also check aliases — map back to canonical
        for (alias, canonical) in &self.alias_map {
            let d = levenshtein(&key, alias);
            if d <= max_distance && !candidates.iter().any(|(c, _)| *c == canonical.as_str()) {
                candidates.push((canonical.as_str(), d));
            }
        }

        candidates.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));
        candidates.truncate(limit);
        candidates
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Strip `/`, `-`, `_`, and spaces from a string for normalization.
fn strip_separators(s: &str) -> String {
    s.chars()
        .filter(|c| !matches!(c, '/' | '-' | '_' | ' '))
        .collect()
}

/// Levenshtein edit distance between two strings (insert, delete, substitute).
fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (m, n) = (a.len(), b.len());

    // Single-row DP: prev[j] holds distance for (i-1, j)
    let mut prev: Vec<usize> = (0..=n).collect();
    let mut curr = vec![0usize; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1) // deletion
                .min(curr[j - 1] + 1) // insertion
                .min(prev[j - 1] + cost); // substitution
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_toml() -> &'static str {
        r#"
[[symbol]]
canonical = "eurusd"
aliases = ["eur/usd", "eur-usd", "eur_usd"]
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
lot_min_steps = 1
lot_max_steps = 0

[[symbol]]
canonical = "xauusd"
aliases = ["gold", "xau/usd", "xau-usd"]
pip_position = 1
digits = 2
category = "metal"
lot_base_units = 100
lot_step_units = 1
lot_min_steps = 1
lot_max_steps = 0

[[symbol]]
canonical = "us100"
aliases = ["nas100", "nasdaq", "us tech 100"]
pip_position = 1
digits = 2
category = "index"
lot_base_units = 1
lot_step_units = 1
lot_min_steps = 1
lot_max_steps = 0
"#
    }

    #[test]
    fn load_from_toml_string() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.len(), 3);
        assert!(reg.spec("eurusd").is_some());
        assert!(reg.spec("xauusd").is_some());
        assert!(reg.spec("us100").is_some());
    }

    #[test]
    fn normalize_canonical_name() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize("eurusd"), Some("eurusd"));
    }

    #[test]
    fn normalize_alias() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize("nasdaq"), Some("us100"));
        assert_eq!(reg.normalize("gold"), Some("xauusd"));
        assert_eq!(reg.normalize("nas100"), Some("us100"));
    }

    #[test]
    fn normalize_with_separators() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize("EUR/USD"), Some("eurusd"));
        assert_eq!(reg.normalize("XAU-USD"), Some("xauusd"));
        assert_eq!(reg.normalize("eur_usd"), Some("eurusd"));
        assert_eq!(reg.normalize("us tech 100"), Some("us100"));
    }

    #[test]
    fn normalize_case_insensitive() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize("EURUSD"), Some("eurusd"));
        assert_eq!(reg.normalize("EurUsd"), Some("eurusd"));
        assert_eq!(reg.normalize("GOLD"), Some("xauusd"));
        assert_eq!(reg.normalize("NAS100"), Some("us100"));
    }

    #[test]
    fn normalize_unknown_returns_none() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize("foobar"), None);
    }

    #[test]
    fn passthrough_unknown() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.normalize_or_passthrough("foobar"), "foobar");
        assert_eq!(reg.normalize_or_passthrough("FOO/BAR"), "foobar");
    }

    #[test]
    fn spec_lookup() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        assert_eq!(spec.pip_position, 4);
        assert_eq!(spec.digits, 5);
        assert_eq!(spec.category, "forex");
        assert_eq!(spec.lot_base_units, 100_000);
        assert_eq!(spec.lot_step_units, 1_000);
    }

    #[test]
    fn spec_by_any_alias() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec_by_any("gold").unwrap();
        assert_eq!(spec.canonical, "xauusd");
        assert_eq!(spec.category, "metal");
    }

    #[test]
    fn empty_registry() {
        let reg = SymbolRegistry::empty();
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
        assert_eq!(reg.normalize("eurusd"), None);
    }

    #[test]
    fn canonical_names_list() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let names = reg.canonical_names();
        assert_eq!(names, vec!["eurusd", "us100", "xauusd"]);
    }

    #[test]
    fn symbols_in_category() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let forex = reg.symbols_in_category("forex");
        assert_eq!(forex.len(), 1);
        assert_eq!(forex[0].canonical, "eurusd");
    }

    #[test]
    fn is_known_check() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert!(reg.is_known("eurusd"));
        assert!(reg.is_known("GOLD"));
        assert!(reg.is_known("NAS/100"));
        assert!(!reg.is_known("foobar"));
    }

    #[test]
    fn duplicate_canonical_error() {
        let toml = r#"
[[symbol]]
canonical = "eurusd"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000

[[symbol]]
canonical = "eurusd"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
"#;
        let err = SymbolRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, SymbolError::DuplicateCanonical(ref s) if s == "eurusd"));
    }

    #[test]
    fn duplicate_alias_error() {
        let toml = r#"
[[symbol]]
canonical = "us100"
aliases = ["nasdaq"]
pip_position = 1
digits = 2
category = "index"
lot_base_units = 1
lot_step_units = 1

[[symbol]]
canonical = "us500"
aliases = ["nasdaq"]
pip_position = 1
digits = 2
category = "index"
lot_base_units = 1
lot_step_units = 1
"#;
        let err = SymbolRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, SymbolError::DuplicateAlias { .. }));
    }

    #[test]
    fn invalid_lot_spec_error() {
        let toml = r#"
[[symbol]]
canonical = "bad"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 0
"#;
        let err = SymbolRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, SymbolError::InvalidLotSpec { .. }));
    }

    #[test]
    fn invalid_digits_error() {
        let toml = r#"
[[symbol]]
canonical = "bad"
aliases = []
pip_position = 6
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
"#;
        let err = SymbolRegistry::from_toml(toml).unwrap_err();
        assert!(matches!(err, SymbolError::InvalidDigits { .. }));
    }

    #[test]
    fn lot_step_forex() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        assert!((spec.lot_step() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn lot_step_gold() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("xauusd").unwrap();
        assert!((spec.lot_step() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn lot_min_derived() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        assert!((spec.lot_min() - 0.01).abs() < f64::EPSILON);
    }

    #[test]
    fn lot_max_no_limit() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        assert!((spec.lot_max() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn to_pips_eurusd() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        // 1.10500 - 1.10000 = 50.0 pips
        let pips = spec.to_pips(1.10500, 1.10000);
        assert!((pips - 50.0).abs() < 0.001);
    }

    #[test]
    fn to_pips_gold() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("xauusd").unwrap();
        // 2050.50 - 2050.00 = 5.0 pips (pip_position=1, digits=2)
        let pips = spec.to_pips(2050.50, 2050.00);
        assert!((pips - 5.0).abs() < 0.001);
    }

    #[test]
    fn add_pips_eurusd() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        let result = spec.add_pips(1.10000, 30.0);
        assert!((result - 1.10300).abs() < 1e-10);
    }

    #[test]
    fn add_pips_negative() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let spec = reg.spec("eurusd").unwrap();
        let result = spec.add_pips(1.10000, -15.0);
        assert!((result - 1.09850).abs() < 1e-10);
    }

    #[test]
    fn digits_convenience() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert_eq!(reg.digits("eurusd"), Some(5));
        assert_eq!(reg.digits("xauusd"), Some(2));
        assert_eq!(reg.digits("unknown"), None);
    }

    #[test]
    fn lot_step_convenience() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        assert!((reg.lot_step("eurusd").unwrap() - 0.01).abs() < f64::EPSILON);
        assert!(reg.lot_step("unknown").is_none());
    }

    // ─── Levenshtein ────────────────────────────────────────────────────

    #[test]
    fn levenshtein_identical() {
        assert_eq!(super::levenshtein("eurusd", "eurusd"), 0);
    }

    #[test]
    fn levenshtein_one_deletion() {
        // "adusd" → "audusd" needs 1 insertion (or viewed from other side, 1 deletion)
        assert_eq!(super::levenshtein("adusd", "audusd"), 1);
    }

    #[test]
    fn levenshtein_one_substitution() {
        // "euruds" → "eurusd" needs 1 transposition modeled as sub+sub or 2, actually:
        // e u r u d s
        // e u r u s d  — swap last two = 2 substitutions in pure Levenshtein
        assert_eq!(super::levenshtein("euruds", "eurusd"), 2);
        // single char substitution
        assert_eq!(super::levenshtein("eurxsd", "eurusd"), 1);
    }

    #[test]
    fn levenshtein_empty() {
        assert_eq!(super::levenshtein("", "abc"), 3);
        assert_eq!(super::levenshtein("abc", ""), 3);
        assert_eq!(super::levenshtein("", ""), 0);
    }

    // ─── suggest() ──────────────────────────────────────────────────────

    #[test]
    fn suggest_single_deletion_typo() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        // "adusd" is 1 edit from "audusd" (missing 'u') — but we don't have
        // audusd in minimal_toml. Let's test with "euusd" → "eurusd" (missing 'r')
        let suggestions = reg.suggest("euusd", 2, 5);
        assert!(suggestions.iter().any(|(name, _)| *name == "eurusd"));
    }

    #[test]
    fn suggest_substitution_typo() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        // "eurxsd" → 1 sub from "eurusd"
        let suggestions = reg.suggest("eurxsd", 1, 5);
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0], ("eurusd", 1));
    }

    #[test]
    fn suggest_returns_empty_for_known_symbol() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("eurusd", 2, 5);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_returns_empty_for_known_alias() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("gold", 2, 5);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_returns_empty_when_too_distant() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("zzzzzzzzz", 1, 5);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_respects_limit() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("usd", 3, 1);
        assert!(suggestions.len() <= 1);
    }

    #[test]
    fn suggest_sorted_by_distance_then_name() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        // "euusd" is 1 edit from "eurusd" (missing 'r')
        let suggestions = reg.suggest("euusd", 2, 10);
        assert!(!suggestions.is_empty());
        assert_eq!(suggestions[0].0, "eurusd");
        assert_eq!(suggestions[0].1, 1);
        // All entries should be sorted by distance then name
        for w in suggestions.windows(2) {
            assert!(w[0].1 < w[1].1 || (w[0].1 == w[1].1 && w[0].0 <= w[1].0),);
        }
    }

    #[test]
    fn suggest_empty_input() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("", 2, 5);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_zero_limit() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        let suggestions = reg.suggest("eurxsd", 2, 0);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn suggest_matches_via_alias_proximity() {
        let reg = SymbolRegistry::from_toml(minimal_toml()).unwrap();
        // "golds" is 1 edit from alias "gold" → should suggest "xauusd"
        let suggestions = reg.suggest("golds", 1, 5);
        assert!(suggestions.iter().any(|(name, _)| *name == "xauusd"));
    }
}
