use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use signal_parser::{
    OfflineArgs, OfflineRunner, RawTgMessage, SignalParserError, load_parsers, parse_messages,
};

#[derive(Deserialize)]
struct Manifest {
    manifest_schema_version: u32,
    root_reexport_inventory: Artifact,
    fixtures: Vec<Fixture>,
}

#[derive(Deserialize)]
struct SurfaceInventory {
    inventory_schema_version: u32,
    inventory_scope: String,
    package: String,
    surfaces: Vec<Surface>,
}

#[derive(Deserialize)]
struct Surface {
    symbol: String,
    owner: String,
    disposition: String,
    replacement_dependency: String,
    removal_condition: String,
}

#[derive(Deserialize)]
struct Fixture {
    fixture_id: String,
    classification: String,
    execution_path: String,
    required_features: Vec<String>,
    inputs: Vec<Artifact>,
    expectations: Vec<Expectation>,
    notes: String,
}

#[derive(Deserialize)]
struct Artifact {
    role: String,
    path: String,
    records: usize,
}

#[derive(Deserialize)]
struct Expectation {
    role: String,
    path: Option<String>,
    records: usize,
    comparison_mode: String,
    assertions: Vec<String>,
}

struct TempOutputs {
    signals: PathBuf,
    outcomes: PathBuf,
}

impl TempOutputs {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "qs_signal_parser_ingestion_compatibility_{}_{}",
            std::process::id(),
            unique
        ));
        Self {
            signals: base.with_extension("signals.jsonl"),
            outcomes: base.with_extension("outcomes.jsonl"),
        }
    }
}

impl Drop for TempOutputs {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.signals);
        let _ = std::fs::remove_file(&self.outcomes);
    }
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/telegram_ingestion_compatibility")
}

fn read_manifest() -> Manifest {
    serde_json::from_slice(&std::fs::read(fixture_root().join("manifest.json")).unwrap()).unwrap()
}

fn jsonl_records(bytes: &[u8]) -> usize {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

fn input_path(fixture: &Fixture, role: &str) -> PathBuf {
    let artifact = fixture
        .inputs
        .iter()
        .find(|artifact| artifact.role == role)
        .unwrap();
    fixture_root().join(&artifact.path)
}

fn expectation_path(fixture: &Fixture, role: &str) -> PathBuf {
    let expectation = fixture
        .expectations
        .iter()
        .find(|expectation| expectation.role == role)
        .unwrap();
    fixture_root().join(expectation.path.as_ref().unwrap())
}

fn load_messages(path: &Path) -> Vec<RawTgMessage> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn exposed_name(item: &str) -> String {
    let item = item.trim();
    item.split_once(" as ")
        .map(|(_, alias)| alias)
        .unwrap_or_else(|| item.rsplit("::").next().unwrap())
        .trim()
        .to_string()
}

fn root_reexport_symbols(source: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();

    for line in source.lines() {
        let line = line.trim();
        if current.is_empty() {
            if !line.starts_with("pub use ") {
                continue;
            }
            current.push_str(line);
        } else {
            current.push(' ');
            current.push_str(line);
        }

        if current.ends_with(';') {
            statements.push(std::mem::take(&mut current));
        }
    }
    assert!(current.is_empty(), "unterminated pub use statement");

    statements
        .into_iter()
        .flat_map(|statement| {
            let body = statement
                .strip_prefix("pub use ")
                .unwrap()
                .strip_suffix(';')
                .unwrap();
            match (body.find('{'), body.rfind('}')) {
                (Some(open), Some(close)) => body[open + 1..close]
                    .split(',')
                    .filter(|item| !item.trim().is_empty())
                    .map(exposed_name)
                    .collect(),
                _ => vec![exposed_name(body)],
            }
        })
        .collect()
}

#[test]
fn fixture_manifest_and_root_reexports_are_consistent() {
    let manifest = read_manifest();
    assert_eq!(manifest.manifest_schema_version, 1);
    assert_eq!(manifest.fixtures.len(), 2);
    assert_eq!(
        manifest.root_reexport_inventory.role,
        "root_reexport_inventory"
    );

    let inventory_bytes =
        std::fs::read(fixture_root().join(&manifest.root_reexport_inventory.path)).unwrap();
    let inventory: SurfaceInventory = serde_json::from_slice(&inventory_bytes).unwrap();
    assert_eq!(inventory.inventory_schema_version, 1);
    assert_eq!(inventory.inventory_scope, "crate_root_reexports");
    assert_eq!(inventory.package, "qs-signal-parser");
    assert_eq!(
        inventory.surfaces.len(),
        manifest.root_reexport_inventory.records
    );

    let inventoried_symbols: BTreeSet<_> = inventory
        .surfaces
        .iter()
        .map(|surface| surface.symbol.as_str())
        .collect();
    assert_eq!(inventoried_symbols.len(), inventory.surfaces.len());

    let public_exports =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    let exported_symbols = root_reexport_symbols(&public_exports);
    let exported_symbol_set: BTreeSet<_> = exported_symbols.iter().map(String::as_str).collect();
    assert_eq!(exported_symbol_set.len(), exported_symbols.len());
    assert_eq!(inventoried_symbols, exported_symbol_set);

    for surface in &inventory.surfaces {
        assert!(!surface.owner.is_empty());
        assert!(matches!(
            surface.disposition.as_str(),
            "stable" | "compatibility_wrapper" | "deprecate_after_replacement"
        ));
        assert!(!surface.replacement_dependency.is_empty());
        assert!(!surface.removal_condition.is_empty());
    }

    for fixture in &manifest.fixtures {
        assert!(!fixture.fixture_id.is_empty());
        assert!(matches!(
            fixture.classification.as_str(),
            "normative" | "retained_compatibility" | "known_divergence" | "replacement_candidate"
        ));
        assert!(!fixture.execution_path.is_empty());
        assert!(
            fixture
                .required_features
                .iter()
                .all(|feature| feature == "offline")
        );
        assert!(!fixture.notes.is_empty());

        for artifact in &fixture.inputs {
            let bytes = std::fs::read(fixture_root().join(&artifact.path)).unwrap();
            assert!(!bytes.is_empty(), "{}", artifact.path);
            if artifact.path.ends_with(".jsonl") {
                assert_eq!(jsonl_records(&bytes), artifact.records, "{}", artifact.path);
            }
        }
        for expectation in &fixture.expectations {
            assert!(matches!(
                expectation.comparison_mode.as_str(),
                "exact_bytes" | "semantic_assertions"
            ));
            if let Some(path) = &expectation.path {
                assert_eq!(expectation.comparison_mode, "exact_bytes");
                let bytes = std::fs::read(fixture_root().join(path)).unwrap();
                assert_eq!(jsonl_records(&bytes), expectation.records);
            } else {
                assert_eq!(expectation.comparison_mode, "semantic_assertions");
                assert!(!expectation.assertions.is_empty());
            }
        }
    }
}

#[test]
fn structured_offline_fixture_matches_exact_signal_and_outcome_bytes() {
    let manifest = read_manifest();
    let fixture = manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.fixture_id == "structured-v2-basic")
        .unwrap();
    let outputs = TempOutputs::new();
    let config = input_path(fixture, "parser_config");
    let messages = input_path(fixture, "messages");
    let registry = load_parsers(config.to_str().unwrap()).unwrap();

    OfflineRunner::new(registry)
        .run_with_args_and_outcomes(
            OfflineArgs {
                input: messages.to_str().unwrap().to_string(),
                output: Some(outputs.signals.to_str().unwrap().to_string()),
            },
            outputs.outcomes.to_str().unwrap().to_string(),
        )
        .unwrap();

    assert_eq!(
        std::fs::read(&outputs.signals).unwrap(),
        std::fs::read(expectation_path(fixture, "signals")).unwrap()
    );
    assert_eq!(
        std::fs::read(&outputs.outcomes).unwrap(),
        std::fs::read(expectation_path(fixture, "outcomes")).unwrap()
    );
}

#[test]
fn compatibility_fixture_returns_the_legacy_overall_timestamp_error() {
    let manifest = read_manifest();
    let fixture = manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.fixture_id == "compatibility-batch-stop")
        .unwrap();
    let config = input_path(fixture, "parser_config");
    let messages = load_messages(&input_path(fixture, "messages"));
    let registry = load_parsers(config.to_str().unwrap()).unwrap();

    let error = parse_messages(&registry, &messages).unwrap_err();

    assert!(matches!(
        error,
        SignalParserError::TimestampParse(value, reason)
            if value == "invalid-registered-source" && reason == "unrecognized format"
    ));
}
