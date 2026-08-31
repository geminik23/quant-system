use std::fs;

use qs_backtest_api::BacktestResultMsg;
use qs_backtest_client::{
    AnalysisDatasetState, AnalysisUnavailableReason, BacktestRequestSummary,
    BacktestResultDocument, FillModel, HistoricalDataType, OpenedResultFile,
    PersistedExecutionDatasetState, ProfileSelectionSummary, ResultInputMetadata, ResultIoLimits,
    SymbolScopeSummary, decode_result_bytes, decode_result_reader,
};

#[test]
fn document_roundtrips_without_analysis_feature() {
    let document = document();
    let bytes = serde_json::to_vec(&document).unwrap();
    let opened = decode_result_bytes(&bytes).unwrap();
    let OpenedResultFile::Document(opened) = opened else {
        panic!("expected document");
    };
    assert_eq!(opened.document_type(), "quant-system-backtest-result");
    assert_eq!(opened.format_version(), 1);
    assert!(matches!(
        opened.analysis,
        AnalysisDatasetState::Unavailable {
            reason: AnalysisUnavailableReason::AnalysisFeatureDisabled
        }
    ));
}

#[test]
fn legacy_result_loads_only_without_a_discriminator() {
    let legacy = BacktestResultMsg::default();
    let bytes = serde_json::to_vec(&legacy).unwrap();
    assert!(matches!(
        decode_result_bytes(&bytes).unwrap(),
        OpenedResultFile::Legacy(_)
    ));

    let wrong = br#"{"document_type":"other","format_version":1}"#;
    assert!(decode_result_bytes(wrong).is_err());
    let duplicate = br#"{"document_type":"quant-system-backtest-result","document_type":"quant-system-backtest-result","format_version":1}"#;
    assert!(decode_result_bytes(duplicate).is_err());
    let null_discriminator = serde_json::to_vec(&serde_json::json!({
        "document_type": null,
        "initial_balance": 0.0
    }))
    .unwrap();
    assert!(decode_result_bytes(&null_discriminator).is_err());
}

#[test]
fn document_allows_outer_additions_but_rejects_trailing_values_and_bounded_overflow() {
    let mut value = serde_json::to_value(document()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("future_addition".into(), serde_json::json!(true));
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(matches!(
        decode_result_bytes(&bytes).unwrap(),
        OpenedResultFile::Document(_)
    ));
    let mut trailing = bytes.clone();
    trailing.extend_from_slice(b" {}");
    assert!(decode_result_bytes(&trailing).is_err());
    assert!(
        decode_result_reader(std::io::Cursor::new(bytes.clone()), bytes.len() as u64 - 1).is_err()
    );
}

#[test]
fn older_v1_without_execution_field_opens_with_explicit_unavailable_state() {
    let mut value = serde_json::to_value(document()).unwrap();
    value.as_object_mut().unwrap().remove("execution");
    let OpenedResultFile::Document(document) =
        decode_result_bytes(&serde_json::to_vec(&value).unwrap()).unwrap()
    else {
        panic!("expected document");
    };
    assert!(matches!(
        document.execution,
        PersistedExecutionDatasetState::Unavailable {
            reason: qs_backtest_client::ExecutionDatasetUnavailableReason::LegacyOrOmitted
        }
    ));
}

#[test]
fn unsupported_embedded_version_is_rejected() {
    let result = BacktestResultMsg {
        future: Some(qs_backtest_api::FutureBacktestResultMsg {
            format_version: 999,
            ..qs_backtest_api::FutureBacktestResultMsg::default()
        }),
        ..BacktestResultMsg::default()
    };
    let mut document = document();
    document.result = result;
    assert!(decode_result_bytes(&serde_json::to_vec(&document).unwrap()).is_err());
}

#[test]
fn result_io_limits_validate_relationships() {
    assert!(ResultIoLimits::default().validate().is_ok());
    let mut limits = ResultIoLimits::default();
    limits.maximum_artifact_chunk_bytes = limits.maximum_artifact_bytes + 1;
    assert!(limits.validate().is_err());
}

#[test]
fn representative_fifty_thousand_position_document_is_bounded() {
    let mut document = document();
    let positions = (0..50_000)
        .map(|ordinal| qs_backtest_client::PersistedPositionOutcome {
            id: format!("position-{ordinal}"),
            trade_id: None,
            ordinal,
            symbol: "EURUSD".into(),
            side: qs_backtest_client::PersistedPositionSide::Long,
            group: None,
            close_reasons: vec!["Target".into()],
            tags: std::collections::BTreeMap::new(),
            outcome: 1.0,
            outcome_classification: Some(qs_backtest_client::PersistedOutcomeClassification::Win),
            r_multiple: Some(1.0),
            favorable_r: Some(1.5),
            adverse_r: Some(-0.5),
            slippage_bps: Some(0.1),
            latency_ms: Some(2.0),
            fill_ratio: Some(1.0),
        })
        .collect();
    document.analysis =
        AnalysisDatasetState::Complete(Box::new(qs_backtest_client::PersistedAnalysisDataset {
            format_version: 1,

            positions,
            lifecycle: None,
            source_coverage: None,
            default_options: qs_backtest_client::PersistedEvaluationOptions {
                provider_id: None,
                source_id: None,
                sections: vec!["position_performance".into()],
                filter: qs_backtest_client::PersistedPositionFilter::default(),
                breakdowns: vec![],
                bootstrap_samples: 2_000,
                bootstrap_confidence_level: 0.95,
                bootstrap_seed: 1,
                bootstrap_minimum_sample_size: 5,
                rolling_window: 20,
                minimum_breakdown_bucket_count: 1,
                maximum_breakdown_rows: None,
            },
        }));
    let bytes = serde_json::to_vec(&document).unwrap();
    assert!(bytes.len() as u64 <= ResultIoLimits::default().maximum_result_document_bytes);
    assert!(decode_result_bytes(&bytes).is_ok());
}

#[test]
fn document_does_not_store_an_absolute_input_path() {
    let bytes = serde_json::to_string(&document()).unwrap();
    assert!(!bytes.contains("C:\\private"));
    assert!(!bytes.contains("/home/private"));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("result.json");
    fs::write(path, bytes).unwrap();
}

fn document() -> BacktestResultDocument {
    BacktestResultDocument::new(
        chrono::Utc::now(),
        Some("job-1".into()),
        ResultInputMetadata {
            display_name: "signals.jsonl".into(),
            byte_len: 0,

            signal_count: 0,
            retained_signal_count: 0,
            entry_count: 0,
            symbols: vec![],
            minimum_timestamp: None,
            maximum_timestamp: None,
        },
        BacktestRequestSummary {
            symbol_scope: SymbolScopeSummary::Single {
                symbol: "EURUSD".into(),
            },
            exchange: "fixture".into(),
            data_type: HistoricalDataType::Tick,
            timeframe: None,
            from: None,
            to: None,
            profile: ProfileSelectionSummary::None,
            account_currency: "USD".into(),
            initial_balance: 10_000.0,
            close_on_finish: true,
            fill_model: FillModel::BidAsk,
            signal_count: 0,
            signal_latency_ms: 0,
            slippage_pips: 0.0,
            stale_quote_after_ms: None,
            conversion_stale_after_ms: 300_000,
            result_delivery: qs_backtest_client::ResultDeliverySummary::Auto,
            evaluation: qs_backtest_api::ProviderEvaluationOptionsMsg::default(),
        },
        BacktestResultMsg::default(),
        AnalysisDatasetState::Unavailable {
            reason: AnalysisUnavailableReason::AnalysisFeatureDisabled,
        },
        PersistedExecutionDatasetState::default(),
    )
    .unwrap()
}
