#![cfg(feature = "analysis")]

use qs_backtest::evaluation::{EvaluationOptions, evaluate};
use qs_backtest::{FutureBacktestArtifacts, evaluation_request_from_future_artifacts};
use qs_backtest_api::{BacktestResultMsg, FutureBacktestResultMsg, MtmOutputSummaryMsg};
use qs_backtest_client::{
    AnalysisCancellation, AnalysisCoordinator, AnalysisDatasetState, AnalysisRecomputeRequest,
    PersistedExecutionDatasetState, PersistedPositionFilter, evaluate_persisted_dataset,
    project_result_datasets, recompute_analysis,
};

#[test]
fn complete_future_result_projects_and_reevaluates_without_replay() {
    let artifacts = FutureBacktestArtifacts::default();
    let options = EvaluationOptions {
        include_position_rows: true,
        ..EvaluationOptions::default()
    };
    let request = evaluation_request_from_future_artifacts(&artifacts, options);
    let report = evaluate(&request);
    let result = BacktestResultMsg {
        future: Some(FutureBacktestResultMsg {
            format_version: 1,
            execution_metadata: serde_json::to_value(&artifacts.execution).unwrap(),
            recorded_fills: serde_json::to_value(&artifacts.fills).unwrap(),
            action_dispositions: serde_json::to_value(artifacts.lifecycle.as_slice()).unwrap(),
            close_events: serde_json::to_value(&artifacts.close_events).unwrap(),
            completed_positions: serde_json::to_value(&artifacts.completed_positions).unwrap(),
            open_positions: serde_json::to_value(&artifacts.open_positions).unwrap(),
            pending_orders: serde_json::to_value(&artifacts.pending_orders).unwrap(),
            pending_order_lifecycle: vec![],
            mtm_equity_curve: serde_json::to_value(&artifacts.equity_curve).unwrap(),
            mtm_output_summary: MtmOutputSummaryMsg::default(),
            mtm_max_drawdown: None,
            mtm_max_drawdown_pct: None,
            provider_evaluation: serde_json::to_value(&report).unwrap(),
        }),
        ..BacktestResultMsg::default()
    };

    let requested_options = qs_backtest_api::ProviderEvaluationOptionsMsg {
        include_positions: true,
        ..qs_backtest_api::ProviderEvaluationOptionsMsg::default()
    };
    let (analysis, execution) = project_result_datasets(&result, &requested_options).unwrap();
    let AnalysisDatasetState::Complete(dataset) = analysis else {
        panic!("expected complete analysis dataset");
    };
    assert!(dataset.positions.is_empty());
    assert!(matches!(
        execution,
        PersistedExecutionDatasetState::Complete(_)
    ));
    let reevaluated =
        evaluate_persisted_dataset(&dataset, PersistedPositionFilter::default(), vec![]).unwrap();
    assert_eq!(reevaluated, report);
}

#[test]
fn nondefault_evaluation_options_are_preserved_for_local_recompute() {
    let artifacts = FutureBacktestArtifacts::default();
    let requested = qs_backtest_api::ProviderEvaluationOptionsMsg {
        sections: vec![qs_backtest_api::EvaluationSectionMsg::Coverage],
        filter: qs_backtest_api::PositionFilterMsg {
            symbols: vec!["EURUSD".into()],
            ..qs_backtest_api::PositionFilterMsg::default()
        },
        breakdowns: vec![qs_backtest_api::BreakdownDimensionMsg::Side],
        bootstrap: qs_backtest_api::BootstrapConfigMsg {
            samples: 17,
            confidence_level: 0.9,
            seed: 99,
            minimum_sample_size: 2,
        },
        rolling_window: 7,
        minimum_breakdown_bucket_count: 3,
        maximum_breakdown_rows: Some(11),
        include_positions: true,
        ..qs_backtest_api::ProviderEvaluationOptionsMsg::default()
    };
    let report = evaluate(&evaluation_request_from_future_artifacts(
        &artifacts,
        EvaluationOptions {
            sections: [qs_backtest::evaluation::EvaluationSection::Coverage]
                .into_iter()
                .collect(),
            filter: qs_backtest::evaluation::PositionFilter {
                symbols: vec!["EURUSD".into()],
                ..qs_backtest::evaluation::PositionFilter::default()
            },
            breakdowns: vec![qs_backtest::evaluation::BreakdownDimension::Side],
            bootstrap: qs_backtest::evaluation::BootstrapConfig {
                samples: 17,
                confidence_level: 0.9,
                seed: 99,
                minimum_sample_size: 2,
            },
            rolling_window: 7,
            minimum_breakdown_bucket_count: 3,
            maximum_breakdown_rows: Some(11),
            include_position_rows: true,
            ..EvaluationOptions::default()
        },
    ));
    let result = BacktestResultMsg {
        future: Some(FutureBacktestResultMsg {
            format_version: 1,
            execution_metadata: serde_json::to_value(&artifacts.execution).unwrap(),
            recorded_fills: serde_json::to_value(&artifacts.fills).unwrap(),
            action_dispositions: serde_json::to_value(artifacts.lifecycle.as_slice()).unwrap(),
            close_events: serde_json::to_value(&artifacts.close_events).unwrap(),
            completed_positions: serde_json::to_value(&artifacts.completed_positions).unwrap(),
            open_positions: serde_json::to_value(&artifacts.open_positions).unwrap(),
            pending_orders: serde_json::to_value(&artifacts.pending_orders).unwrap(),
            pending_order_lifecycle: vec![],
            mtm_equity_curve: serde_json::to_value(&artifacts.equity_curve).unwrap(),
            mtm_output_summary: MtmOutputSummaryMsg::default(),
            mtm_max_drawdown: None,
            mtm_max_drawdown_pct: None,
            provider_evaluation: serde_json::to_value(&report).unwrap(),
        }),
        ..BacktestResultMsg::default()
    };
    let (AnalysisDatasetState::Complete(dataset), _) =
        project_result_datasets(&result, &requested).unwrap()
    else {
        panic!("expected complete dataset");
    };
    assert_eq!(dataset.default_options.bootstrap_samples, 17);
    assert_eq!(dataset.default_options.rolling_window, 7);
    assert_eq!(dataset.default_options.filter.symbols, vec!["EURUSD"]);
    assert_eq!(dataset.default_options.breakdowns, vec!["side"]);
}

#[tokio::test]
async fn background_analysis_preserves_generation_and_honors_cancellation() {
    let artifacts = FutureBacktestArtifacts::default();
    let request =
        evaluation_request_from_future_artifacts(&artifacts, EvaluationOptions::default());
    let report = evaluate(&request);
    let result = BacktestResultMsg {
        future: Some(FutureBacktestResultMsg {
            format_version: 1,
            execution_metadata: serde_json::to_value(&artifacts.execution).unwrap(),
            recorded_fills: serde_json::to_value(&artifacts.fills).unwrap(),
            action_dispositions: serde_json::to_value(artifacts.lifecycle.as_slice()).unwrap(),
            close_events: serde_json::to_value(&artifacts.close_events).unwrap(),
            completed_positions: serde_json::to_value(&artifacts.completed_positions).unwrap(),
            open_positions: serde_json::to_value(&artifacts.open_positions).unwrap(),
            pending_orders: serde_json::to_value(&artifacts.pending_orders).unwrap(),
            pending_order_lifecycle: vec![],
            mtm_equity_curve: serde_json::to_value(&artifacts.equity_curve).unwrap(),
            mtm_output_summary: MtmOutputSummaryMsg::default(),
            mtm_max_drawdown: None,
            mtm_max_drawdown_pct: None,
            provider_evaluation: serde_json::to_value(report).unwrap(),
        }),
        ..BacktestResultMsg::default()
    };
    let (AnalysisDatasetState::Complete(dataset), _) = project_result_datasets(
        &result,
        &qs_backtest_api::ProviderEvaluationOptionsMsg::default(),
    )
    .unwrap() else {
        panic!("expected complete dataset");
    };
    let snapshot = recompute_analysis(
        std::sync::Arc::new((*dataset).clone()),
        AnalysisRecomputeRequest {
            generation: 7,
            filter: PersistedPositionFilter::default(),
            breakdowns: vec![],
            tail_fraction: 0.05,
        },
        AnalysisCancellation::default(),
    )
    .await
    .unwrap();
    assert_eq!(snapshot.generation, 7);
    assert_eq!(snapshot.additional.selected_positions, 0);

    let cancellation = AnalysisCancellation::default();
    cancellation.cancel();
    assert!(
        recompute_analysis(
            std::sync::Arc::new(*dataset),
            AnalysisRecomputeRequest {
                generation: 8,
                filter: PersistedPositionFilter::default(),
                breakdowns: vec![],
                tail_fraction: 0.05,
            },
            cancellation,
        )
        .await
        .is_err()
    );
}

#[test]
fn analysis_coordinator_marks_older_generations_stale() {
    let coordinator = AnalysisCoordinator::default();
    let older = coordinator.next_generation();
    let current = coordinator.next_generation();
    assert!(!coordinator.is_current(older));
    assert!(coordinator.is_current(current));
}

#[test]
fn legacy_result_has_explicit_unavailable_states() {
    let (analysis, execution) = project_result_datasets(
        &BacktestResultMsg::default(),
        &qs_backtest_api::ProviderEvaluationOptionsMsg::default(),
    )
    .unwrap();
    assert!(matches!(analysis, AnalysisDatasetState::Unavailable { .. }));
    assert!(matches!(
        execution,
        PersistedExecutionDatasetState::Unavailable { .. }
    ));
}
