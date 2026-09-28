use chrono::{Duration, NaiveDate};
use qs_backtest::evaluation::PositionOutcome;
use qs_research::*;
use std::collections::{BTreeMap, BTreeSet};
fn start() -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}
fn key(output: &str) -> FeatureCacheKey {
    FeatureCacheKey {
        dataset_reference: "synthetic".into(),
        symbol: "EURUSD".into(),
        from: start(),
        to: start() + Duration::days(1),
        source: "m1".into(),
        price_basis: "mid".into(),
        alignment_offset_seconds: 0,
        output: output.into(),
        parameters: BTreeMap::from([("period".into(), "3".into())]),
        seed_policy: "sma".into(),
        missing_policy: "observed".into(),
        clock: "m1".into(),
        availability_policy: "revealed".into(),
    }
}
#[test]
fn bounded_cache_uses_complete_typed_equality_and_deterministic_eviction() {
    let entry_bytes = FeatureCache::entry_bytes_upper_bound(&key("a"), 3).unwrap();
    let mut cache = FeatureCache::new(entry_bytes * 2).unwrap();
    cache.insert(key("a"), vec![Some(1.0); 3]).unwrap();
    cache.insert(key("b"), vec![Some(2.0); 3]).unwrap();
    cache.insert(key("c"), vec![Some(3.0); 3]).unwrap();
    assert!(cache.get(&key("a")).is_none());
    assert_eq!(cache.get(&key("b")).unwrap(), [Some(2.0); 3]);
    assert!(cache.used_bytes() <= entry_bytes * 2);
    assert!(
        cache
            .insert(key("large"), vec![Some(1.0); entry_bytes])
            .is_err()
    );
}

#[test]
fn empty_cache_values_still_consume_key_and_container_budget() {
    let one = FeatureCache::entry_bytes_upper_bound(&key("a"), 0).unwrap();
    let mut cache = FeatureCache::new(one).unwrap();
    cache.insert(key("a"), vec![]).unwrap();
    cache.insert(key("b"), vec![]).unwrap();
    assert!(cache.get(&key("a")).is_none());
    assert!(cache.get(&key("b")).is_some());
    assert_eq!(cache.len(), 1);
    assert!(cache.used_bytes() <= one);

    let mut oversized = key("oversized");
    oversized.output = "x".repeat(257);
    assert!(cache.insert(oversized, vec![]).is_err());
}
#[test]
fn traces_bound_records_and_bytes_without_changing_economic_state() {
    let limits = TraceLimits::new(1, 4096).unwrap();
    let mut trace = BoundedTrace::default();
    let record = TraceRecord {
        feature: "rsi".into(),
        value: Some(55.0),
        valid: true,
        source: "m1".into(),
        sample_at: start(),
        available_at: start() + Duration::minutes(1),
        predicate: Some("rsi_gt".into()),
        event: None,
        capture: None,
        decision_id: Some("d1".into()),
        command_id: Some("c1".into()),
        position_id: Some("p1".into()),
        entry_regime: Some("trend".into()),
        fill_regime: Some("trend".into()),
        hindsight_regime: None,
    };
    trace.push(record.clone(), limits).unwrap();
    trace.push(record, limits).unwrap();
    assert_eq!(trace.records().len(), 1);
    assert_eq!(trace.omitted_records(), 1);
}
#[test]
fn checkpoint_resume_rejects_dependency_changes_and_overlap() {
    let dependency = CheckpointDependency {
        experiment_id: Some("exp".into()),
        caller_revision: "r1".into(),
        dataset_reference: "synthetic".into(),
        factory_revision: Some("f1".into()),
    };
    let checkpoint = SearchCheckpoint {
        dependency: dependency.clone(),
        experiment_recipe: None,
        candidate_recipes: BTreeMap::new(),
        frozen_selection: None,
        frontier: 7,
        completed_candidates: BTreeSet::from([1, 2]),
        completed_runs: BTreeSet::from([1, 2]),
        committed_runs: BTreeMap::new(),
        failures: BTreeMap::from([(3, "failed".into())]),
        generated: 8,
        executed: 3,
        generation_exhaustive: false,
        split_access: vec![],
    };
    let limits = CheckpointLimits::new(8192, 8).unwrap();
    let bytes = checkpoint.encode(limits).unwrap();
    assert_eq!(
        SearchCheckpoint::decode(&bytes, limits, &dependency).unwrap(),
        checkpoint
    );
    let mut changed = dependency.clone();
    changed.caller_revision = "r2".into();
    assert!(SearchCheckpoint::decode(&bytes, limits, &changed).is_err());
    let mut overlap = checkpoint;
    overlap.failures.insert(1, "duplicate".into());
    assert!(overlap.validate(limits).is_err());
}
fn position(
    id: &str,
    symbol: &str,
    outcome: f64,
    regime: Option<&str>,
    month: i64,
) -> PositionOutcome {
    serde_json::from_value(serde_json::json!({"id":id,"trade_id":null,"ordinal":month,"dimensions":{"symbol":symbol,"side":"long","group":null,"close_reasons":[],"tags":regime.map(|value|serde_json::json!({"regime":value})).unwrap_or_else(||serde_json::json!({}))},"outcome":outcome,"outcome_classification":null,"r_multiple":null,"excursions":null,"execution":null,"costs":null})).unwrap()
}
#[test]
fn selected_evidence_reports_symbol_period_regime_concentration_and_explicit_uncertainty_status() {
    let jan = start().and_utc().timestamp_millis();
    let feb = (start() + Duration::days(35)).and_utc().timestamp_millis();
    let evidence = selected_candidate_evidence(
        &[
            position("a", "EURUSD", 10.0, Some("trend"), jan),
            position("b", "GBPUSD", -5.0, None, feb),
        ],
        "regime",
        UncertaintyAssessment::Incomplete {
            reason: "block length not selected".into(),
        },
    )
    .unwrap();
    assert_eq!(evidence.by_symbol.len(), 2);
    assert_eq!(evidence.by_period.len(), 2);
    assert!(
        evidence
            .by_regime
            .iter()
            .any(|bucket| bucket.key == "unavailable")
    );
    assert!(matches!(
        evidence.uncertainty,
        UncertaintyAssessment::Incomplete { .. }
    ));
}
