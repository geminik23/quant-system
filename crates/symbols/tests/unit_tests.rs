use qs_symbols::{SymbolError, SymbolRegistry, SymbolSpec};

// ─── Helpers ────────────────────────────────────────────────────────────────

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

[[symbol]]
canonical = "usdjpy"
aliases = ["usd/jpy"]
pip_position = 2
digits = 3
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
lot_min_steps = 1
lot_max_steps = 0

[[symbol]]
canonical = "btcusd"
aliases = ["bitcoin", "btc/usd"]
pip_position = 1
digits = 2
category = "crypto"
lot_base_units = 100000000
lot_step_units = 100000
lot_min_steps = 1
lot_max_steps = 0
"#
}

fn registry() -> SymbolRegistry {
    SymbolRegistry::from_toml(minimal_toml()).unwrap()
}

// ─── Loading ────────────────────────────────────────────────────────────────

#[test]
fn load_from_toml_string() {
    let reg = registry();
    assert_eq!(reg.len(), 5);
    assert!(reg.spec("eurusd").is_some());
    assert!(reg.spec("xauusd").is_some());
    assert!(reg.spec("us100").is_some());
    assert!(reg.spec("usdjpy").is_some());
    assert!(reg.spec("btcusd").is_some());
}

#[test]
fn load_from_file() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("symbols.toml");
    let reg = SymbolRegistry::load(&path).unwrap();
    // The default file has many symbols
    assert!(reg.len() > 50);
    assert!(reg.spec("eurusd").is_some());
    assert!(reg.spec("xauusd").is_some());
    assert!(reg.spec("us100").is_some());
}

#[test]
fn empty_registry() {
    let reg = SymbolRegistry::empty();
    assert_eq!(reg.len(), 0);
    assert!(reg.is_empty());
    assert_eq!(reg.normalize("eurusd"), None);
    assert!(reg.spec("eurusd").is_none());
    assert!(reg.canonical_names().is_empty());
}

// ─── Normalization ──────────────────────────────────────────────────────────

#[test]
fn normalize_canonical_name() {
    let reg = registry();
    assert_eq!(reg.normalize("eurusd"), Some("eurusd"));
    assert_eq!(reg.normalize("xauusd"), Some("xauusd"));
    assert_eq!(reg.normalize("us100"), Some("us100"));
}

#[test]
fn normalize_alias() {
    let reg = registry();
    assert_eq!(reg.normalize("nasdaq"), Some("us100"));
    assert_eq!(reg.normalize("gold"), Some("xauusd"));
    assert_eq!(reg.normalize("nas100"), Some("us100"));
    assert_eq!(reg.normalize("bitcoin"), Some("btcusd"));
}

#[test]
fn normalize_with_separators() {
    let reg = registry();
    assert_eq!(reg.normalize("EUR/USD"), Some("eurusd"));
    assert_eq!(reg.normalize("XAU-USD"), Some("xauusd"));
    assert_eq!(reg.normalize("eur_usd"), Some("eurusd"));
    assert_eq!(reg.normalize("us tech 100"), Some("us100"));
    assert_eq!(reg.normalize("USD/JPY"), Some("usdjpy"));
    assert_eq!(reg.normalize("BTC/USD"), Some("btcusd"));
}

#[test]
fn normalize_case_insensitive() {
    let reg = registry();
    assert_eq!(reg.normalize("EURUSD"), Some("eurusd"));
    assert_eq!(reg.normalize("EurUsd"), Some("eurusd"));
    assert_eq!(reg.normalize("GOLD"), Some("xauusd"));
    assert_eq!(reg.normalize("NAS100"), Some("us100"));
    assert_eq!(reg.normalize("BITCOIN"), Some("btcusd"));
    assert_eq!(reg.normalize("Nasdaq"), Some("us100"));
}

#[test]
fn normalize_unknown_returns_none() {
    let reg = registry();
    assert_eq!(reg.normalize("foobar"), None);
    assert_eq!(reg.normalize("unknown"), None);
    assert_eq!(reg.normalize(""), None);
}

#[test]
fn passthrough_unknown() {
    let reg = registry();
    assert_eq!(reg.normalize_or_passthrough("foobar"), "foobar");
    assert_eq!(reg.normalize_or_passthrough("FOO/BAR"), "foobar");
    assert_eq!(reg.normalize_or_passthrough("SOME_THING"), "something");
}

#[test]
fn passthrough_known_returns_canonical() {
    let reg = registry();
    assert_eq!(reg.normalize_or_passthrough("GOLD"), "xauusd");
    assert_eq!(reg.normalize_or_passthrough("NAS/100"), "us100");
    assert_eq!(reg.normalize_or_passthrough("eurusd"), "eurusd");
}

#[test]
fn is_known_check() {
    let reg = registry();
    assert!(reg.is_known("eurusd"));
    assert!(reg.is_known("EURUSD"));
    assert!(reg.is_known("GOLD"));
    assert!(reg.is_known("NAS/100"));
    assert!(reg.is_known("bitcoin"));
    assert!(!reg.is_known("foobar"));
    assert!(!reg.is_known(""));
}

// ─── Spec lookup ────────────────────────────────────────────────────────────

#[test]
fn spec_lookup_eurusd() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    assert_eq!(spec.canonical, "eurusd");
    assert_eq!(spec.pip_position, 4);
    assert_eq!(spec.digits, 5);
    assert_eq!(spec.category, "forex");
    assert_eq!(spec.lot_base_units, 100_000);
    assert_eq!(spec.lot_step_units, 1_000);
    assert_eq!(spec.lot_min_steps, 1);
    assert_eq!(spec.lot_max_steps, 0);
}

#[test]
fn spec_lookup_gold() {
    let reg = registry();
    let spec = reg.spec("xauusd").unwrap();
    assert_eq!(spec.canonical, "xauusd");
    assert_eq!(spec.pip_position, 1);
    assert_eq!(spec.digits, 2);
    assert_eq!(spec.category, "metal");
    assert_eq!(spec.lot_base_units, 100);
    assert_eq!(spec.lot_step_units, 1);
}

#[test]
fn spec_lookup_crypto() {
    let reg = registry();
    let spec = reg.spec("btcusd").unwrap();
    assert_eq!(spec.canonical, "btcusd");
    assert_eq!(spec.category, "crypto");
    assert_eq!(spec.lot_base_units, 100_000_000);
    assert_eq!(spec.lot_step_units, 100_000);
}

#[test]
fn spec_lookup_unknown() {
    let reg = registry();
    assert!(reg.spec("foobar").is_none());
}

#[test]
fn spec_by_any_alias() {
    let reg = registry();
    let spec = reg.spec_by_any("gold").unwrap();
    assert_eq!(spec.canonical, "xauusd");
    assert_eq!(spec.category, "metal");

    let spec = reg.spec_by_any("NASDAQ").unwrap();
    assert_eq!(spec.canonical, "us100");

    let spec = reg.spec_by_any("EUR/USD").unwrap();
    assert_eq!(spec.canonical, "eurusd");
}

#[test]
fn spec_by_any_unknown() {
    let reg = registry();
    assert!(reg.spec_by_any("foobar").is_none());
}

// ─── Convenience methods ────────────────────────────────────────────────────

#[test]
fn digits_convenience() {
    let reg = registry();
    assert_eq!(reg.digits("eurusd"), Some(5));
    assert_eq!(reg.digits("xauusd"), Some(2));
    assert_eq!(reg.digits("usdjpy"), Some(3));
    assert_eq!(reg.digits("unknown"), None);
}

#[test]
fn lot_step_convenience() {
    let reg = registry();
    let step = reg.lot_step("eurusd").unwrap();
    assert!((step - 0.01).abs() < f64::EPSILON);

    assert!(reg.lot_step("unknown").is_none());
}

#[test]
fn canonical_names_list() {
    let reg = registry();
    let names = reg.canonical_names();
    assert_eq!(names.len(), 5);
    // Should be sorted
    assert_eq!(names, vec!["btcusd", "eurusd", "us100", "usdjpy", "xauusd"]);
}

#[test]
fn symbols_in_category() {
    let reg = registry();

    let forex = reg.symbols_in_category("forex");
    assert_eq!(forex.len(), 2);
    let forex_names: Vec<&str> = {
        let mut v: Vec<&str> = forex.iter().map(|s| s.canonical.as_str()).collect();
        v.sort();
        v
    };
    assert_eq!(forex_names, vec!["eurusd", "usdjpy"]);

    let metal = reg.symbols_in_category("metal");
    assert_eq!(metal.len(), 1);
    assert_eq!(metal[0].canonical, "xauusd");

    let index = reg.symbols_in_category("index");
    assert_eq!(index.len(), 1);

    let crypto = reg.symbols_in_category("crypto");
    assert_eq!(crypto.len(), 1);

    let none = reg.symbols_in_category("nonexistent");
    assert!(none.is_empty());
}

#[test]
fn symbols_in_category_case_insensitive() {
    let reg = registry();
    let forex = reg.symbols_in_category("FOREX");
    assert_eq!(forex.len(), 2);
}

// ─── Lot spec derived values ────────────────────────────────────────────────

#[test]
fn lot_step_forex() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    // 1000 / 100000 = 0.01
    assert!((spec.lot_step() - 0.01).abs() < f64::EPSILON);
}

#[test]
fn lot_step_gold() {
    let reg = registry();
    let spec = reg.spec("xauusd").unwrap();
    // 1 / 100 = 0.01
    assert!((spec.lot_step() - 0.01).abs() < f64::EPSILON);
}

#[test]
fn lot_step_index() {
    let reg = registry();
    let spec = reg.spec("us100").unwrap();
    // 1 / 1 = 1.0
    assert!((spec.lot_step() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn lot_step_crypto() {
    let reg = registry();
    let spec = reg.spec("btcusd").unwrap();
    // 100000 / 100000000 = 0.001
    assert!((spec.lot_step() - 0.001).abs() < f64::EPSILON);
}

#[test]
fn lot_min_derived() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    // 1 * 0.01 = 0.01
    assert!((spec.lot_min() - 0.01).abs() < f64::EPSILON);
}

#[test]
fn lot_max_no_limit() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    assert!((spec.lot_max() - 0.0).abs() < f64::EPSILON);
}

#[test]
fn lot_max_with_limit() {
    let toml = r#"
[[symbol]]
canonical = "limited"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
lot_min_steps = 1
lot_max_steps = 500
"#;
    let reg = SymbolRegistry::from_toml(toml).unwrap();
    let spec = reg.spec("limited").unwrap();
    // 500 * 0.01 = 5.0
    assert!((spec.lot_max() - 5.0).abs() < f64::EPSILON);
}

#[test]
fn lot_base_units_forex() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    assert_eq!(spec.lot_base_units, 100_000);
}

#[test]
fn lot_base_units_crypto() {
    let reg = registry();
    let spec = reg.spec("btcusd").unwrap();
    assert_eq!(spec.lot_base_units, 100_000_000);
}

// ─── Pip calculations ───────────────────────────────────────────────────────

#[test]
fn to_pips_eurusd() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    // 1.10500 - 1.10000 = 50.0 pips (digits=5, pip_position=4, pip_scale=10)
    let pips = spec.to_pips(1.10500, 1.10000);
    assert!((pips - 50.0).abs() < 0.001);
}

#[test]
fn to_pips_eurusd_negative() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    // 1.10000 - 1.10500 = -50.0 pips
    let pips = spec.to_pips(1.10000, 1.10500);
    assert!((pips - (-50.0)).abs() < 0.001);
}

#[test]
fn to_pips_usdjpy() {
    let reg = registry();
    let spec = reg.spec("usdjpy").unwrap();
    // 150.500 - 150.000 = 50.0 pips (digits=3, pip_position=2, pip_scale=10)
    let pips = spec.to_pips(150.500, 150.000);
    assert!((pips - 50.0).abs() < 0.001);
}

#[test]
fn to_pips_gold() {
    let reg = registry();
    let spec = reg.spec("xauusd").unwrap();
    // 2050.50 - 2050.00 = 5.0 pips (digits=2, pip_position=1, pip_scale=10)
    let pips = spec.to_pips(2050.50, 2050.00);
    assert!((pips - 5.0).abs() < 0.001);
}

#[test]
fn to_pips_zero() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    let pips = spec.to_pips(1.10000, 1.10000);
    assert!((pips - 0.0).abs() < 0.001);
}

#[test]
fn add_pips_eurusd() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    let result = spec.add_pips(1.10000, 30.0);
    assert!((result - 1.10300).abs() < 1e-10);
}

#[test]
fn add_pips_negative() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    let result = spec.add_pips(1.10000, -15.0);
    assert!((result - 1.09850).abs() < 1e-10);
}

#[test]
fn add_pips_usdjpy() {
    let reg = registry();
    let spec = reg.spec("usdjpy").unwrap();
    let result = spec.add_pips(150.000, 20.0);
    assert!((result - 150.200).abs() < 1e-10);
}

#[test]
fn add_pips_gold() {
    let reg = registry();
    let spec = reg.spec("xauusd").unwrap();
    let result = spec.add_pips(2050.00, 10.0);
    assert!((result - 2051.00).abs() < 1e-10);
}

#[test]
fn add_pips_roundtrip() {
    let reg = registry();
    let spec = reg.spec("eurusd").unwrap();
    let base = 1.10000;
    let pips = 42.5;
    let adjusted = spec.add_pips(base, pips);
    let computed_pips = spec.to_pips(adjusted, base);
    assert!((computed_pips - pips).abs() < 0.1);
}

// ─── Error cases ────────────────────────────────────────────────────────────

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
    match err {
        SymbolError::DuplicateAlias {
            alias,
            existing,
            new,
        } => {
            assert_eq!(alias, "nasdaq");
            assert_eq!(existing, "us100");
            assert_eq!(new, "us500");
        }
        other => panic!("Expected DuplicateAlias, got: {other:?}"),
    }
}

#[test]
fn invalid_lot_spec_zero_step() {
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
    match err {
        SymbolError::InvalidLotSpec { symbol, step, .. } => {
            assert_eq!(symbol, "bad");
            assert_eq!(step, 0);
        }
        other => panic!("Expected InvalidLotSpec, got: {other:?}"),
    }
}

#[test]
fn invalid_lot_spec_negative_step() {
    let toml = r#"
[[symbol]]
canonical = "bad"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = -1
"#;
    let err = SymbolRegistry::from_toml(toml).unwrap_err();
    assert!(matches!(err, SymbolError::InvalidLotSpec { .. }));
}

#[test]
fn invalid_lot_spec_step_exceeds_base() {
    let toml = r#"
[[symbol]]
canonical = "bad"
aliases = []
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100
lot_step_units = 200
"#;
    let err = SymbolRegistry::from_toml(toml).unwrap_err();
    assert!(matches!(err, SymbolError::InvalidLotSpec { .. }));
}

#[test]
fn invalid_digits_pip_exceeds_digits() {
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
    match err {
        SymbolError::InvalidDigits {
            symbol,
            pip,
            digits,
        } => {
            assert_eq!(symbol, "bad");
            assert_eq!(pip, 6);
            assert_eq!(digits, 5);
        }
        other => panic!("Expected InvalidDigits, got: {other:?}"),
    }
}

#[test]
fn invalid_toml_parse_error() {
    let err = SymbolRegistry::from_toml("this is not valid toml {{{{").unwrap_err();
    assert!(matches!(err, SymbolError::Parse(_)));
}

#[test]
fn load_nonexistent_file_error() {
    let err = SymbolRegistry::load("/tmp/definitely_nonexistent_file_abc123.toml").unwrap_err();
    assert!(matches!(err, SymbolError::Io(_)));
}

// ─── Same alias on same canonical is harmless ───────────────────────────────

#[test]
fn same_alias_same_canonical_ok() {
    // If two aliases for the same symbol normalize to the same key, it's fine
    let toml = r#"
[[symbol]]
canonical = "eurusd"
aliases = ["eur/usd", "eur-usd"]
pip_position = 4
digits = 5
category = "forex"
lot_base_units = 100000
lot_step_units = 1000
"#;
    // "eur/usd" and "eur-usd" both strip to "eurusd" which is the canonical,
    // so both map to the same target. This should not error.
    let reg = SymbolRegistry::from_toml(toml).unwrap();
    assert_eq!(reg.normalize("eur/usd"), Some("eurusd"));
    assert_eq!(reg.normalize("eur-usd"), Some("eurusd"));
}

// ─── Default symbols.toml validation ────────────────────────────────────────

#[test]
fn default_toml_loads_without_errors() {
    let content = include_str!("../symbols.toml");
    let reg = SymbolRegistry::from_toml(content).unwrap();
    assert!(reg.len() > 0);
}

#[test]
fn default_toml_has_all_existing_market_data_symbols() {
    // Every symbol that was in market-data's symbol_info() must be in the TOML
    let content = include_str!("../symbols.toml");
    let reg = SymbolRegistry::from_toml(content).unwrap();

    let expected = [
        "btcusd", "ethusd", "us30", "us100", "us500", "xtiusd", "xbrusd", "xngusd", "xauusd",
        "xagusd", "audcad", "audchf", "audjpy", "audnzd", "audusd", "cadchf", "cadjpy", "chfjpy",
        "chfpln", "euraud", "eurcad", "eurchf", "eurczk", "eurdkk", "eurgbp", "eurhkd", "eurhuf",
        "eurjpy", "eurmxn", "eurnok", "eurnzd", "eurpln", "eurrub", "eursek", "eursgd", "eurtry",
        "eurusd", "eurzar", "gbpaud", "gbpcad", "gbpchf", "gbphkd", "gbpjpy", "gbpnzd", "gbpusd",
        "nzdcad", "nzdchf", "nzdjpy", "nzdusd", "usdcad", "usdchf", "usdcnh", "usdczk", "usddkk",
        "usdhkd", "usdhuf", "usdils", "usdjpy", "usdkrw", "usdmxn", "usdnok", "usdpln", "usdrub",
        "usdsek", "usdsgd", "usdtry", "usdzar",
    ];

    for sym in &expected {
        assert!(
            reg.spec(sym).is_some(),
            "Symbol '{sym}' missing from symbols.toml"
        );
    }
}

#[test]
fn default_toml_convert_symbol_compatibility() {
    // Every alias that was in market-data's convert_symbol() must normalize correctly
    let content = include_str!("../symbols.toml");
    let reg = SymbolRegistry::from_toml(content).unwrap();

    assert_eq!(reg.normalize("spx500"), Some("us500"));
    assert_eq!(reg.normalize("nas100"), Some("us100"));
    assert_eq!(reg.normalize("ger30"), Some("de40"));
    assert_eq!(reg.normalize("ger40"), Some("de40"));
    assert_eq!(reg.normalize("de30"), Some("de40"));
    assert_eq!(reg.normalize("nasdaq"), Some("us100"));
    assert_eq!(reg.normalize("gold"), Some("xauusd"));
    assert_eq!(reg.normalize("silver"), Some("xagusd"));
    assert_eq!(reg.normalize("oil"), Some("xtiusd"));
    assert_eq!(reg.normalize("usoil"), Some("xtiusd"));
}

#[test]
fn default_toml_no_duplicate_aliases() {
    // Implicitly tested by successful load, but explicit for clarity
    let content = include_str!("../symbols.toml");
    let result = SymbolRegistry::from_toml(content);
    assert!(
        result.is_ok(),
        "Default TOML has duplicate aliases: {result:?}"
    );
}

// ─── SymbolSpec serde roundtrip ─────────────────────────────────────────────

#[test]
fn symbol_spec_serde_roundtrip() {
    let spec = SymbolSpec {
        canonical: "eurusd".into(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    };
    let json = serde_json::to_string(&spec).unwrap();
    let back: SymbolSpec = serde_json::from_str(&json).unwrap();
    assert_eq!(back.canonical, "eurusd");
    assert_eq!(back.digits, 5);
    assert_eq!(back.lot_base_units, 100_000);
}

// ─── Edge cases ─────────────────────────────────────────────────────────────

#[test]
fn normalize_empty_string() {
    let reg = registry();
    assert_eq!(reg.normalize(""), None);
}

#[test]
fn normalize_only_separators() {
    let reg = registry();
    assert_eq!(reg.normalize("/ -_"), None);
}

#[test]
fn passthrough_empty_string() {
    let reg = registry();
    assert_eq!(reg.normalize_or_passthrough(""), "");
}

#[test]
fn pip_position_equals_digits_ok() {
    // pip_position == digits is valid (pip_scale = 10^0 = 1)
    let toml = r#"
[[symbol]]
canonical = "test"
aliases = []
pip_position = 2
digits = 2
category = "index"
lot_base_units = 1
lot_step_units = 1
"#;
    let reg = SymbolRegistry::from_toml(toml).unwrap();
    let spec = reg.spec("test").unwrap();
    // to_pips with pip_position == digits: pip_scale = 1
    let pips = spec.to_pips(100.50, 100.00);
    assert!((pips - 50.0).abs() < 0.001);
}

#[test]
fn lot_step_equal_to_base() {
    // lot_step_units == lot_base_units means lot_step = 1.0
    let toml = r#"
[[symbol]]
canonical = "whole"
aliases = []
pip_position = 0
digits = 0
category = "index"
lot_base_units = 1
lot_step_units = 1
"#;
    let reg = SymbolRegistry::from_toml(toml).unwrap();
    let spec = reg.spec("whole").unwrap();
    assert!((spec.lot_step() - 1.0).abs() < f64::EPSILON);
    assert!((spec.lot_min() - 1.0).abs() < f64::EPSILON);
}

// ─── suggest() — typo correction ───────────────────────────────────────────

#[test]
fn suggest_missing_char_adusd() {
    // "adusd" is missing 'u' from "audusd" — audusd isn't in minimal_toml,
    // so test with full TOML
    let content = include_str!("../symbols.toml");
    let reg = SymbolRegistry::from_toml(content).unwrap();
    let suggestions = reg.suggest("adusd", 2, 5);
    assert!(
        suggestions.iter().any(|(name, _)| *name == "audusd"),
        "Expected 'audusd' in suggestions, got: {suggestions:?}"
    );
    // Should be distance 1 (one insertion)
    let audusd = suggestions.iter().find(|(n, _)| *n == "audusd").unwrap();
    assert_eq!(audusd.1, 1);
}

#[test]
fn suggest_missing_char_euusd() {
    let reg = registry();
    // "euusd" missing 'r' → "eurusd" at distance 1
    let suggestions = reg.suggest("euusd", 2, 5);
    assert!(suggestions.iter().any(|(name, _)| *name == "eurusd"));
    assert_eq!(suggestions[0], ("eurusd", 1));
}

#[test]
fn suggest_substitution_eurxsd() {
    let reg = registry();
    // "eurxsd" has 'x' instead of 'u' → "eurusd" at distance 1
    let suggestions = reg.suggest("eurxsd", 1, 5);
    assert_eq!(suggestions.len(), 1);
    assert_eq!(suggestions[0], ("eurusd", 1));
}

#[test]
fn suggest_extra_char() {
    let reg = registry();
    // "eurusdd" has an extra 'd' → "eurusd" at distance 1
    let suggestions = reg.suggest("eurusdd", 1, 5);
    assert!(suggestions.iter().any(|(name, _)| *name == "eurusd"));
}

#[test]
fn suggest_transposition() {
    let reg = registry();
    // "euruds" swaps last two chars → distance 2 in pure Levenshtein
    let suggestions = reg.suggest("euruds", 2, 5);
    assert!(suggestions.iter().any(|(name, _)| *name == "eurusd"));
}

#[test]
fn suggest_returns_empty_for_exact_canonical() {
    let reg = registry();
    assert!(reg.suggest("eurusd", 2, 5).is_empty());
}

#[test]
fn suggest_returns_empty_for_exact_alias() {
    let reg = registry();
    assert!(reg.suggest("gold", 2, 5).is_empty());
    assert!(reg.suggest("NASDAQ", 2, 5).is_empty());
}

#[test]
fn suggest_returns_empty_when_too_far() {
    let reg = registry();
    assert!(reg.suggest("zzzzzzzzz", 1, 5).is_empty());
}

#[test]
fn suggest_empty_input() {
    let reg = registry();
    assert!(reg.suggest("", 2, 5).is_empty());
}

#[test]
fn suggest_zero_limit() {
    let reg = registry();
    assert!(reg.suggest("eurxsd", 2, 0).is_empty());
}

#[test]
fn suggest_respects_limit() {
    let reg = registry();
    let suggestions = reg.suggest("usd", 3, 2);
    assert!(suggestions.len() <= 2);
}

#[test]
fn suggest_sorted_by_distance_then_name() {
    let reg = registry();
    let suggestions = reg.suggest("usdjp", 2, 10);
    assert!(!suggestions.is_empty());
    assert_eq!(suggestions[0].0, "usdjpy");
    assert_eq!(suggestions[0].1, 1);
    for w in suggestions.windows(2) {
        assert!(
            w[0].1 < w[1].1 || (w[0].1 == w[1].1 && w[0].0 <= w[1].0),
            "Not sorted: {:?} should come before {:?}",
            w[0],
            w[1]
        );
    }
}

#[test]
fn suggest_alias_proximity() {
    let reg = registry();
    // "golds" is 1 edit from alias "gold" → should suggest "xauusd"
    let suggestions = reg.suggest("golds", 1, 5);
    assert!(
        suggestions.iter().any(|(name, _)| *name == "xauusd"),
        "Expected 'xauusd' via alias proximity, got: {suggestions:?}"
    );
}

#[test]
fn suggest_alias_proximity_nasda() {
    let reg = registry();
    // "nasda" is 1 edit from alias "nasdaq" (missing 'q') → should suggest "us100"
    let suggestions = reg.suggest("nasda", 2, 5);
    assert!(
        suggestions.iter().any(|(name, _)| *name == "us100"),
        "Expected 'us100' via alias 'nasdaq' proximity, got: {suggestions:?}"
    );
}

#[test]
fn suggest_full_toml_common_typos() {
    let content = include_str!("../symbols.toml");
    let reg = SymbolRegistry::from_toml(content).unwrap();

    // gbpusd typos
    let s = reg.suggest("gbpud", 2, 3);
    assert!(s.iter().any(|(n, _)| *n == "gbpusd"), "gbpud: {s:?}");

    // xauusd typos
    let s = reg.suggest("xauud", 2, 3);
    assert!(s.iter().any(|(n, _)| *n == "xauusd"), "xauud: {s:?}");

    // usdjpy typos
    let s = reg.suggest("usdjp", 2, 3);
    assert!(s.iter().any(|(n, _)| *n == "usdjpy"), "usdjp: {s:?}");

    // btcusd typos
    let s = reg.suggest("btcud", 2, 3);
    assert!(s.iter().any(|(n, _)| *n == "btcusd"), "btcud: {s:?}");
}

#[test]
fn suggest_does_not_duplicate_canonical_from_alias_and_direct() {
    let reg = registry();
    // "xauuds" is close to canonical "xauusd" (distance 2) and possibly aliases
    // Each canonical should appear at most once
    let suggestions = reg.suggest("xauuds", 2, 10);
    let xauusd_count = suggestions.iter().filter(|(n, _)| *n == "xauusd").count();
    assert!(
        xauusd_count <= 1,
        "xauusd appears {xauusd_count} times in suggestions"
    );
}
