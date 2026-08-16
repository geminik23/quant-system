use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::{
    MAX_DECISION_LATENCY_MS, PriceBasis, RawSignal, SeriesId, SeriesRequirement,
    StrategyDecisionKind, StrategyDecisionRecord, StrategyDecisionRecorder, StrategyDescriptor,
    StrategyDomainError, StrategyId, StrategyRequirements, StrategyRetentionLimits, Timeframe,
    WarmupRequirement,
};
use serde::Deserialize;

fn ts(second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(3, 4, second)
        .unwrap()
}

fn series(id: &str, timeframe: Timeframe, warmup: usize) -> SeriesRequirement {
    SeriesRequirement::new(
        SeriesId::new(id).unwrap(),
        "EURUSD",
        timeframe,
        PriceBasis::Mid,
        WarmupRequirement::bars(warmup).unwrap(),
    )
    .unwrap()
}

fn requirements(series: Vec<SeriesRequirement>) -> StrategyRequirements {
    StrategyRequirements::new(vec!["EURUSD".to_string()], series, 25, true, true).unwrap()
}

#[test]
fn validated_identity_and_requirements_round_trip_strictly() {
    let descriptor = StrategyDescriptor::new(
        StrategyId::new("ema-crossover").unwrap(),
        "v1.2",
        "EMA crossover",
    )
    .unwrap();
    let requirements = requirements(vec![
        series("context-h1", Timeframe::hours(1).unwrap(), 50),
        series("decision-m5", Timeframe::minutes(5).unwrap(), 20),
    ]);

    let descriptor_json = serde_json::to_string(&descriptor).unwrap();
    let decoded_descriptor: StrategyDescriptor = serde_json::from_str(&descriptor_json).unwrap();
    assert_eq!(decoded_descriptor, descriptor);

    let requirements_json = serde_json::to_string(&requirements).unwrap();
    let decoded_requirements: StrategyRequirements =
        serde_json::from_str(&requirements_json).unwrap();
    assert_eq!(decoded_requirements, requirements);
    assert_eq!(decoded_requirements.instruments(), ["EURUSD"]);
    assert_eq!(decoded_requirements.series()[0].id().as_str(), "context-h1");
    assert_eq!(
        decoded_requirements.effective_timestamp(ts(1)).unwrap(),
        ts(1) + Duration::milliseconds(25)
    );
}

#[test]
fn strict_strategy_owned_config_rejects_unknown_fields() {
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ExampleStrategyConfig {
        context_timeframe: Timeframe,
        execution_timeframe: Timeframe,
        risk: f64,
    }

    let valid: ExampleStrategyConfig = serde_json::from_str(
        r#"{
            "context_timeframe":{"hours":4},
            "execution_timeframe":{"minutes":15},
            "risk":1.0
        }"#,
    )
    .unwrap();
    assert_eq!(valid.context_timeframe, Timeframe::hours(4).unwrap());
    assert_eq!(valid.execution_timeframe, Timeframe::minutes(15).unwrap());
    assert_eq!(valid.risk, 1.0);

    let error = serde_json::from_str::<ExampleStrategyConfig>(
        r#"{
            "context_timeframe":{"hours":4},
            "execution_timeframe":{"minutes":15},
            "risk":1.0,
            "registry":"global"
        }"#,
    )
    .unwrap_err();
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn invalid_domain_values_reject_at_construction_or_decode() {
    assert!(StrategyId::new("").is_err());
    assert!(StrategyId::new("content digest").is_err());
    assert!(SeriesId::new("bad/id").is_err());
    assert!(Timeframe::minutes(0).is_err());
    assert!(WarmupRequirement::bars(1_000_001).is_err());
    assert!(StrategyRetentionLimits::new(1, 0, 10).is_err());
    assert!(StrategyRetentionLimits::new(1, 1, 0).is_err());
    assert!(
        serde_json::from_str::<StrategyRetentionLimits>(
            r#"{
                "max_decisions":1,
                "max_signals_per_callback":1,
                "max_reason_bytes":10,
                "unknown":true
            }"#,
        )
        .is_err()
    );
    assert!(
        StrategyDescriptor::new(StrategyId::new("valid").unwrap(), "bad revision", "Title")
            .is_err()
    );

    let error = serde_json::from_str::<StrategyRequirements>(
        r#"{
            "instruments":["EURUSD"],
            "series":[],
            "decision_latency_ms":0,
            "needs_tick_execution":true,
            "needs_execution_feedback":false,
            "extra":true
        }"#,
    )
    .unwrap_err();
    assert!(error.to_string().contains("unknown field"));
}

#[test]
fn requirement_validation_rejects_duplicates_and_undeclared_symbols() {
    let h1 = series("h1", Timeframe::hours(1).unwrap(), 10);
    let duplicate_id = series("h1", Timeframe::minutes(15).unwrap(), 10);
    assert!(matches!(
        StrategyRequirements::new(
            vec!["EURUSD".to_string()],
            vec![h1.clone(), duplicate_id],
            0,
            true,
            false,
        ),
        Err(StrategyDomainError::DuplicateSeriesId { .. })
    ));

    let duplicate_definition = series("h1-copy", Timeframe::hours(1).unwrap(), 20);
    assert!(matches!(
        StrategyRequirements::new(
            vec!["EURUSD".to_string()],
            vec![h1.clone(), duplicate_definition],
            0,
            true,
            false,
        ),
        Err(StrategyDomainError::DuplicateSeriesDefinition { .. })
    ));

    assert!(matches!(
        StrategyRequirements::new(
            vec!["EURUSD".to_string(), "EURUSD".to_string()],
            vec![h1.clone()],
            0,
            true,
            false,
        ),
        Err(StrategyDomainError::DuplicateInstrument { .. })
    ));

    let gbp = SeriesRequirement::new(
        SeriesId::new("gbp-h1").unwrap(),
        "GBPUSD",
        Timeframe::hours(1).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(10).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        StrategyRequirements::new(vec!["EURUSD".to_string()], vec![gbp], 0, true, false,),
        Err(StrategyDomainError::UndeclaredSeriesInstrument { .. })
    ));
}

#[test]
fn latency_bounds_and_timestamp_overflow_reject() {
    let h1 = series("h1", Timeframe::hours(1).unwrap(), 2);
    assert!(matches!(
        StrategyRequirements::new(
            vec!["EURUSD".to_string()],
            vec![h1],
            MAX_DECISION_LATENCY_MS + 1,
            true,
            false,
        ),
        Err(StrategyDomainError::DecisionLatencyTooLarge { .. })
    ));

    let requirements = requirements(vec![series("h1", Timeframe::hours(1).unwrap(), 2)]);
    assert!(matches!(
        requirements.effective_timestamp(NaiveDateTime::MAX),
        Err(StrategyDomainError::DecisionTimestampOverflow { .. })
    ));
}

#[test]
fn warmup_is_derived_from_all_declared_series() {
    let requirements = requirements(vec![
        series("h1", Timeframe::hours(1).unwrap(), 2),
        series("m5", Timeframe::minutes(5).unwrap(), 3),
    ]);

    assert!(!requirements.warmup_complete(|id| match id.as_str() {
        "h1" => 2,
        "m5" => 2,
        _ => 0,
    }));
    assert!(requirements.warmup_complete(|id| match id.as_str() {
        "h1" => 2,
        "m5" => 3,
        _ => 0,
    }));
}

#[test]
fn decision_retention_preserves_executable_signals_and_exact_counts() {
    let limits = StrategyRetentionLimits::new(1, 2, 64).unwrap();
    let first = StrategyDecisionRecord::new(
        10,
        ts(1),
        StrategyDecisionKind::Exit,
        "close the active campaign",
        None,
        vec![
            RawSignal::CloseAll { ts: ts(1) },
            RawSignal::CancelAllPending { ts: ts(1) },
        ],
        limits,
    )
    .unwrap();
    let second = StrategyDecisionRecord::new(
        11,
        ts(2),
        StrategyDecisionKind::Management,
        "cancel pending entries",
        None,
        vec![RawSignal::CloseAll { ts: ts(2) }],
        limits,
    )
    .unwrap();

    let mut recorder = StrategyDecisionRecorder::new(limits);
    let first_executable = recorder.push(first).unwrap();
    let second_executable = recorder.push(second).unwrap();
    assert_eq!(first_executable.len(), 2);
    assert!(matches!(first_executable[0], RawSignal::CloseAll { .. }));
    assert!(matches!(
        first_executable[1],
        RawSignal::CancelAllPending { .. }
    ));
    assert_eq!(first_executable[0].ts(), ts(1));
    assert_eq!(second_executable.len(), 1);
    assert_eq!(second_executable[0].ts(), ts(2));

    let output = recorder.finish();
    assert_eq!(output.records.len(), 1);
    assert_eq!(output.records[0].sequence(), 10);
    assert_eq!(output.retention.retained, 1);
    assert_eq!(output.retention.omitted, 1);
}

#[test]
fn decision_validation_enforces_sequence_reason_trade_id_and_callback_bound() {
    let limits = StrategyRetentionLimits::new(2, 1, 16).unwrap();
    let first = StrategyDecisionRecord::new(
        4,
        ts(1),
        StrategyDecisionKind::NoAction,
        "wait",
        Some("trade-1".to_string()),
        vec![],
        limits,
    )
    .unwrap();
    let repeated = StrategyDecisionRecord::new(
        4,
        ts(2),
        StrategyDecisionKind::NoAction,
        "wait again",
        None,
        vec![],
        limits,
    )
    .unwrap();

    let mut recorder = StrategyDecisionRecorder::new(limits);
    recorder.push(first).unwrap();
    assert!(matches!(
        recorder.push(repeated),
        Err(StrategyDomainError::NonMonotonicDecisionSequence { .. })
    ));

    assert!(
        StrategyDecisionRecord::new(
            5,
            ts(2),
            StrategyDecisionKind::Entry,
            "reason text is too long",
            None,
            vec![],
            limits,
        )
        .is_err()
    );
    assert!(
        StrategyDecisionRecord::new(
            5,
            ts(2),
            StrategyDecisionKind::Entry,
            "enter",
            Some(" bad ".to_string()),
            vec![],
            limits,
        )
        .is_err()
    );
    assert!(
        StrategyDecisionRecord::new(
            5,
            ts(2),
            StrategyDecisionKind::Entry,
            "enter",
            None,
            vec![
                RawSignal::CloseAll { ts: ts(2) },
                RawSignal::CloseAll { ts: ts(2) }
            ],
            limits,
        )
        .is_err()
    );
}

#[test]
fn explicit_identity_is_not_content_derived() {
    let first = StrategyDescriptor::new(StrategyId::new("manual-id").unwrap(), "r1", "First title")
        .unwrap();
    let second =
        StrategyDescriptor::new(StrategyId::new("manual-id").unwrap(), "r1", "Second title")
            .unwrap();

    assert_eq!(first.id(), second.id());
    assert_ne!(first.title(), second.title());
}
