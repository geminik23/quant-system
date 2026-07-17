use std::collections::{BTreeMap, BTreeSet};

use super::*;

fn position(
    id: &str,
    ordinal: i64,
    symbol: &str,
    side: PositionSide,
    group: Option<&str>,
    outcome: f64,
    r_multiple: Option<f64>,
) -> PositionOutcome {
    PositionOutcome {
        id: id.to_owned(),
        trade_id: None,
        ordinal,
        dimensions: PositionDimensions {
            symbol: symbol.to_owned(),
            side,
            group: group.map(str::to_owned),
            close_reasons: Vec::new(),
            tags: BTreeMap::new(),
        },
        outcome,
        outcome_classification: None,
        r_multiple,
        excursions: None,
        execution: None,
    }
}

fn test_bootstrap() -> BootstrapConfig {
    BootstrapConfig {
        samples: 500,
        confidence_level: 0.95,
        seed: 42,
        minimum_sample_size: 2,
    }
}

fn request(positions: Vec<PositionOutcome>) -> EvaluationRequest {
    EvaluationRequest {
        positions,
        options: EvaluationOptions {
            bootstrap: test_bootstrap(),
            rolling_window: 2,
            ..EvaluationOptions::default()
        },
        ..EvaluationRequest::default()
    }
}

fn available(metric: &MetricValue<f64>) -> f64 {
    assert_eq!(metric.status, MetricStatus::Available);
    metric.value.expect("available metric has a value")
}

#[test]
fn metric_value_makes_availability_explicit() {
    let metric = MetricValue::available(12.5);
    assert_eq!(metric.status, MetricStatus::Available);
    assert_eq!(metric.value, Some(12.5));
    assert_eq!(metric.reason, None);

    let metric = MetricValue::<f64>::not_applicable("no losses");
    assert_eq!(metric.status, MetricStatus::NotApplicable);
    assert_eq!(metric.value, None);
    assert_eq!(metric.reason.as_deref(), Some("no losses"));
}

#[test]
fn filter_ors_within_dimensions_and_ands_across_dimensions() {
    let mut matching = position("one", 1, "ES", PositionSide::Long, Some("trend"), 1.0, None);
    matching.dimensions.close_reasons = vec!["partial".into(), "target".into()];
    matching
        .dimensions
        .tags
        .insert("session".into(), "us".into());
    matching
        .dimensions
        .tags
        .insert("regime".into(), "volatile".into());

    let filter = PositionFilter {
        symbols: vec!["NQ".into(), "ES".into()],
        sides: vec![PositionSide::Long],
        groups: vec![GroupFilter::Named("trend".into())],
        close_reasons: vec!["stop".into(), "target".into()],
        tags: BTreeMap::from([
            ("session".into(), vec!["eu".into(), "us".into()]),
            ("regime".into(), vec!["volatile".into()]),
        ]),
    };

    assert!(filter.matches(&matching));

    let mut wrong_regime = matching.clone();
    wrong_regime
        .dimensions
        .tags
        .insert("regime".into(), "quiet".into());
    assert!(!filter.matches(&wrong_regime));

    let mut wrong_side = matching.clone();
    wrong_side.dimensions.side = PositionSide::Short;
    assert!(!filter.matches(&wrong_side));
}

#[test]
fn group_filter_can_select_ungrouped_positions() {
    let grouped = position("grouped", 1, "ES", PositionSide::Long, Some("a"), 1.0, None);
    let ungrouped = position("ungrouped", 2, "ES", PositionSide::Long, None, 1.0, None);
    let filter = PositionFilter {
        groups: vec![GroupFilter::Ungrouped],
        ..PositionFilter::default()
    };

    assert!(!filter.matches(&grouped));
    assert!(filter.matches(&ungrouped));
}

#[test]
fn wilson_interval_matches_known_half_success_case() {
    let interval = wilson_interval(5, 10, 0.95)
        .value
        .expect("valid Wilson interval");
    assert!((interval.estimate - 0.5).abs() < 1e-12);
    assert!((interval.lower - 0.236_593).abs() < 1e-5);
    assert!((interval.upper - 0.763_407).abs() < 1e-5);

    assert_eq!(
        wilson_interval(0, 0, 0.95).status,
        MetricStatus::InsufficientData
    );
    assert_eq!(
        wilson_interval(11, 10, 0.95).status,
        MetricStatus::InvalidInput
    );
    assert_eq!(
        wilson_interval(1, 2, 1.0).status,
        MetricStatus::InvalidInput
    );
}

#[test]
fn bootstrap_is_fixed_seed_deterministic() {
    let values = [-2.0, 1.0, 3.0, 8.0, 13.0];
    let first = bootstrap_mean_confidence(&values, test_bootstrap());
    let second = bootstrap_mean_confidence(&values, test_bootstrap());
    assert_eq!(first, second);

    let interval = first.value.expect("sufficient bootstrap observations");
    assert_eq!(interval.estimate, 4.6);
    assert!(interval.lower <= interval.estimate);
    assert!(interval.upper >= interval.estimate);

    let invalid = bootstrap_mean_confidence(
        &values,
        BootstrapConfig {
            samples: 0,
            ..test_bootstrap()
        },
    );
    assert_eq!(invalid.status, MetricStatus::InvalidInput);
    assert_eq!(
        bootstrap_mean_confidence(&[1.0], test_bootstrap()).status,
        MetricStatus::InsufficientData
    );
    assert_eq!(
        bootstrap_mean_confidence(&[1.0, f64::NAN], test_bootstrap()).status,
        MetricStatus::InvalidInput
    );
}

#[test]
fn position_performance_uses_completed_position_outcomes() {
    let report = evaluate(&request(vec![
        position("a", 1, "ES", PositionSide::Long, None, 10.0, None),
        position("b", 2, "ES", PositionSide::Long, None, -4.0, None),
        position("c", 3, "ES", PositionSide::Long, None, 0.0, None),
        position("d", 4, "ES", PositionSide::Long, None, 6.0, None),
    ]));
    let performance = report
        .position_performance
        .expect("position performance requested");

    assert_eq!(performance.position_count, 4);
    assert_eq!(
        (performance.wins, performance.losses, performance.breakeven),
        (2, 1, 1)
    );
    assert_eq!(available(&performance.total_outcome), 12.0);
    assert_eq!(available(&performance.mean_outcome), 3.0);
    assert_eq!(available(&performance.median_outcome), 3.0);
    assert_eq!(available(&performance.win_rate), 0.5);
    assert_eq!(available(&performance.profit_factor), 4.0);
    assert_eq!(available(&performance.payoff_ratio), 2.0);
    assert_eq!(available(&performance.best_outcome), 10.0);
    assert_eq!(available(&performance.worst_outcome), -4.0);
    assert_eq!(
        performance.mean_outcome_confidence.status,
        MetricStatus::Available
    );
}

#[test]
fn supplied_outcome_classification_preserves_provider_breakeven_tolerance() {
    let mut tiny_breakeven = position("tiny", 1, "ES", PositionSide::Long, None, 0.0005, None);
    tiny_breakeven.outcome_classification = Some(OutcomeClassification::Breakeven);
    let report = evaluate(&request(vec![
        tiny_breakeven,
        position("win", 2, "ES", PositionSide::Long, None, 2.0, None),
    ]));
    let performance = report
        .position_performance
        .expect("position performance requested");

    assert_eq!(
        (performance.wins, performance.losses, performance.breakeven),
        (1, 0, 1)
    );
    assert!((available(&performance.total_outcome) - 2.0005).abs() < 1e-12);
    assert_eq!(available(&performance.gross_positive), 2.0);
    assert_eq!(
        available(
            &report
                .robustness
                .as_ref()
                .expect("robustness requested")
                .best_one_positive_concentration,
        ),
        1.0
    );
}

#[test]
fn no_losses_produce_not_applicable_ratios_instead_of_infinity() {
    let report = evaluate(&request(vec![
        position("a", 1, "ES", PositionSide::Long, None, 2.0, None),
        position("b", 2, "ES", PositionSide::Long, None, 1.0, None),
    ]));

    let performance = report
        .position_performance
        .expect("position performance requested");
    assert_eq!(
        performance.profit_factor.status,
        MetricStatus::NotApplicable
    );
    assert_eq!(performance.payoff_ratio.status, MetricStatus::NotApplicable);
}

#[test]
fn coverage_reports_filters_lifecycle_and_optional_observations() {
    let mut observed = position(
        "observed",
        1,
        "ES",
        PositionSide::Long,
        None,
        1.0,
        Some(1.0),
    );
    observed.excursions = Some(ExcursionInput {
        favorable_r: Some(2.0),
        adverse_r: None,
    });
    observed.execution = Some(ExecutionDiagnosticsInput {
        slippage_bps: Some(0.5),
        latency_ms: None,
        fill_ratio: None,
    });
    let invalid = position("invalid", 2, "ES", PositionSide::Long, None, f64::NAN, None);
    let filtered = position("filtered", 3, "NQ", PositionSide::Short, None, 2.0, None);
    let mut evaluation_request = request(vec![observed, invalid, filtered]);
    evaluation_request.filter.symbols = vec!["ES".into()];
    evaluation_request.lifecycle = Some(LifecycleCounts {
        candidates: 10,
        accepted: 8,
        opened: 6,
        completed: 5,
        rejected: 2,
        filled: 4,
        cancelled: 1,
        unfilled_at_end: 1,
        open_at_end: 1,
    });

    let report = evaluate(&evaluation_request);
    let coverage = report.coverage.as_ref().expect("coverage requested");
    assert_eq!(coverage.provided_positions, 3);
    assert_eq!(coverage.selected_positions, 2);
    assert_eq!(coverage.filtered_out_positions, 1);
    assert_eq!(coverage.valid_outcomes, 1);
    assert_eq!(coverage.invalid_outcomes, 1);
    assert_eq!(available(&coverage.acceptance_rate), 0.8);
    assert_eq!(available(&coverage.open_rate), 0.75);
    assert!((available(&coverage.completion_rate) - 5.0 / 6.0).abs() < 1e-12);
    assert_eq!(available(&coverage.r_coverage), 0.5);
    assert_eq!(available(&coverage.excursion_coverage), 0.5);
    assert_eq!(available(&coverage.execution_coverage), 0.5);
    assert_eq!(
        report
            .position_performance
            .as_ref()
            .expect("position performance requested")
            .position_count,
        1
    );
}

#[test]
fn included_position_rows_use_the_evaluation_filter_and_preserve_outcome_data() {
    let mut selected = position(
        "selected",
        2,
        "ES",
        PositionSide::Long,
        None,
        12.0,
        Some(1.5),
    );
    selected.trade_id = Some("provider-trade-7".into());
    let filtered = position(
        "filtered",
        1,
        "NQ",
        PositionSide::Short,
        None,
        -4.0,
        Some(-0.5),
    );
    let mut evaluation_request = request(vec![filtered, selected]);
    evaluation_request.filter.symbols = vec!["ES".into()];
    evaluation_request.include_position_rows = true;

    let rows = evaluate(&evaluation_request)
        .position_rows
        .expect("position rows requested");

    assert_eq!(rows.available_rows, 1);
    assert_eq!(rows.included_rows, 1);
    assert!(!rows.truncated);
    assert_eq!(rows.rows[0].id, "selected");
    assert_eq!(rows.rows[0].trade_id.as_deref(), Some("provider-trade-7"));
    assert_eq!(rows.rows[0].outcome, 12.0);
    assert_eq!(rows.rows[0].r_multiple, Some(1.5));
    assert_eq!(rows.rows[0].classification(), OutcomeClassification::Win);
}

#[test]
fn inconsistent_lifecycle_counts_are_invalid_input() {
    let evaluation_request = EvaluationRequest {
        lifecycle: Some(LifecycleCounts {
            candidates: 1,
            accepted: 2,
            opened: 3,
            completed: 4,
            ..LifecycleCounts::default()
        }),
        ..EvaluationRequest::default()
    };
    let coverage = evaluate(&evaluation_request)
        .coverage
        .expect("coverage requested");

    assert_eq!(coverage.acceptance_rate.status, MetricStatus::InvalidInput);
    assert_eq!(coverage.open_rate.status, MetricStatus::InvalidInput);
    assert_eq!(coverage.completion_rate.status, MetricStatus::InvalidInput);
}

#[test]
fn r_excursion_and_execution_metrics_ignore_only_their_invalid_observations() {
    let mut first = position("a", 1, "ES", PositionSide::Long, None, 2.0, Some(1.0));
    first.excursions = Some(ExcursionInput {
        favorable_r: Some(3.0),
        adverse_r: Some(0.5),
    });
    first.execution = Some(ExecutionDiagnosticsInput {
        slippage_bps: Some(2.0),
        latency_ms: Some(10.0),
        fill_ratio: Some(1.0),
    });
    let mut second = position("b", 2, "ES", PositionSide::Long, None, -1.0, Some(-0.5));
    second.excursions = Some(ExcursionInput {
        favorable_r: Some(1.0),
        adverse_r: Some(1.5),
    });
    second.execution = Some(ExecutionDiagnosticsInput {
        slippage_bps: Some(-1.0),
        latency_ms: Some(30.0),
        fill_ratio: Some(0.5),
    });
    let third = position(
        "c",
        3,
        "ES",
        PositionSide::Long,
        None,
        0.0,
        Some(f64::INFINITY),
    );

    let report = evaluate(&request(vec![first, second, third]));
    let r_metrics = report.r_metrics.as_ref().expect("R metrics requested");
    let excursions = report.excursions.as_ref().expect("excursions requested");
    let execution = report.execution.as_ref().expect("execution requested");
    assert_eq!(r_metrics.observed_count, 2);
    assert_eq!(r_metrics.missing_or_invalid_count, 1);
    assert_eq!(available(&r_metrics.total_r), 0.5);
    assert_eq!(available(&r_metrics.mean_r), 0.25);
    assert_eq!(available(&r_metrics.median_r), 0.25);
    assert_eq!(available(&r_metrics.positive_r_rate), 0.5);
    assert_eq!(available(&r_metrics.profit_factor), 2.0);
    assert_eq!(available(&r_metrics.average_winner_r), 1.0);
    assert_eq!(available(&r_metrics.average_loser_r), -0.5);
    assert_eq!(available(&r_metrics.best_r), 1.0);
    assert_eq!(available(&r_metrics.worst_r), -0.5);
    let quantiles = r_metrics
        .quantiles
        .value
        .expect("finite R values have quantiles");
    assert!((quantiles.p05 - -0.425).abs() < 1e-12);
    assert!((quantiles.p25 - -0.125).abs() < 1e-12);
    assert!((quantiles.p50 - 0.25).abs() < 1e-12);
    assert!((quantiles.p75 - 0.625).abs() < 1e-12);
    assert!((quantiles.p95 - 0.925).abs() < 1e-12);
    let curve = r_metrics
        .cumulative_r_curve
        .value
        .as_ref()
        .expect("finite R values have a cumulative curve");
    assert_eq!(curve.len(), 2);
    assert_eq!(curve[0].position_id, "a");
    assert_eq!(curve[0].cumulative_r, 1.0);
    assert_eq!(curve[1].position_id, "b");
    assert_eq!(curve[1].cumulative_r, 0.5);
    assert_eq!(available(&r_metrics.max_realized_r_drawdown), 0.5);

    assert_eq!(excursions.favorable_observed_count, 2);
    assert_eq!(available(&excursions.mean_favorable_r), 2.0);
    assert_eq!(available(&excursions.mean_adverse_r), 1.0);

    assert_eq!(execution.positions_with_diagnostics, 2);
    assert_eq!(available(&execution.mean_slippage_bps), 0.5);
    assert_eq!(available(&execution.adverse_slippage_rate), 0.5);
    assert_eq!(available(&execution.mean_latency_ms), 20.0);
    assert_eq!(available(&execution.mean_fill_ratio), 0.75);
}

#[test]
fn r_metrics_are_chronological_and_keep_undefined_ratios_explicit() {
    let mut positions = vec![
        position("d", 4, "ES", PositionSide::Long, None, 1.0, Some(3.0)),
        position("b", 2, "ES", PositionSide::Long, None, -1.0, Some(-2.0)),
        position("e", 5, "ES", PositionSide::Long, None, -1.0, Some(-1.0)),
        position("a", 1, "ES", PositionSide::Long, None, 1.0, Some(1.0)),
        position("c", 3, "ES", PositionSide::Long, None, 1.0, Some(0.5)),
    ];
    let first = evaluate(&request(positions.clone()))
        .r_metrics
        .expect("R metrics requested");
    positions.reverse();
    let second = evaluate(&request(positions))
        .r_metrics
        .expect("R metrics requested");

    assert_eq!(first, second);
    assert_eq!(available(&first.total_r), 1.5);
    assert_eq!(available(&first.profit_factor), 1.5);
    assert_eq!(available(&first.average_winner_r), 1.5);
    assert_eq!(available(&first.average_loser_r), -1.5);
    assert_eq!(available(&first.best_r), 3.0);
    assert_eq!(available(&first.worst_r), -2.0);
    assert_eq!(available(&first.max_realized_r_drawdown), 2.0);
    assert_eq!(
        first
            .cumulative_r_curve
            .value
            .expect("curve is available")
            .iter()
            .map(|point| point.cumulative_r)
            .collect::<Vec<_>>(),
        vec![1.0, -1.0, -0.5, 2.5, 1.5]
    );

    let all_positive = evaluate(&request(vec![
        position("a", 1, "ES", PositionSide::Long, None, 1.0, Some(1.0)),
        position("b", 2, "ES", PositionSide::Long, None, 2.0, Some(2.0)),
    ]))
    .r_metrics
    .expect("R metrics requested");
    assert_eq!(
        all_positive.profit_factor.status,
        MetricStatus::NotApplicable
    );
    assert_eq!(
        all_positive.average_loser_r.status,
        MetricStatus::NotApplicable
    );
    assert_eq!(available(&all_positive.max_realized_r_drawdown), 0.0);
    assert!(
        all_positive.profit_factor.value.is_none(),
        "undefined R profit factor must not be infinity"
    );
}

#[test]
fn robustness_removes_best_results_and_measures_positive_concentration() {
    let report = evaluate(&request(vec![
        position("a", 1, "ES", PositionSide::Long, None, 10.0, None),
        position("b", 2, "ES", PositionSide::Long, None, 5.0, None),
        position("c", 3, "ES", PositionSide::Long, None, -4.0, None),
        position("d", 4, "ES", PositionSide::Long, None, 1.0, None),
    ]));
    let robustness = report.robustness.expect("robustness requested");
    let removed = robustness
        .best_one_removed
        .value
        .expect("enough outcomes for removal");
    assert_eq!(removed.removed_count, 1);
    assert_eq!(removed.original_total, 12.0);
    assert_eq!(removed.removed_total, 10.0);
    assert_eq!(removed.remaining_total, 2.0);
    assert!((removed.remaining_mean - 2.0 / 3.0).abs() < 1e-12);
    assert_eq!(
        available(&robustness.best_one_positive_concentration),
        0.625
    );
    assert_eq!(
        available(&robustness.best_five_percent_positive_concentration),
        0.625
    );
    assert_eq!(available(&robustness.pnl_concentration.top_1), 0.625);
    assert_eq!(available(&robustness.pnl_concentration.top_3), 1.0);
    assert_eq!(available(&robustness.pnl_concentration.top_5), 1.0);
    assert_eq!(available(&robustness.pnl_concentration.top_10), 1.0);
}

#[test]
fn fixed_count_pnl_concentration_uses_completed_position_outcomes() {
    let mut positions: Vec<PositionOutcome> = (1..=10)
        .map(|value| {
            position(
                &format!("p{value}"),
                value,
                "ES",
                PositionSide::Long,
                None,
                value as f64,
                None,
            )
        })
        .collect();
    positions.push(position(
        "loss",
        11,
        "ES",
        PositionSide::Long,
        None,
        -100.0,
        None,
    ));

    let concentration = evaluate(&request(positions))
        .robustness
        .expect("robustness requested")
        .pnl_concentration;
    assert_eq!(available(&concentration.top_1), 10.0 / 55.0);
    assert_eq!(available(&concentration.top_3), 27.0 / 55.0);
    assert_eq!(available(&concentration.top_5), 40.0 / 55.0);
    assert_eq!(available(&concentration.top_10), 1.0);
}

#[test]
fn best_five_percent_uses_ceiling_and_rolling_outcomes_use_ordinal_order() {
    let mut positions: Vec<PositionOutcome> = (1..=21)
        .map(|ordinal| {
            position(
                &format!("p{ordinal}"),
                ordinal,
                "ES",
                PositionSide::Long,
                None,
                ordinal as f64,
                None,
            )
        })
        .collect();
    positions.reverse();
    let mut evaluation_request = request(positions);
    evaluation_request.rolling_window = 20;
    let robustness = evaluate(&evaluation_request)
        .robustness
        .expect("robustness requested");

    assert_eq!(
        robustness
            .best_five_percent_removed
            .value
            .expect("enough outcomes")
            .removed_count,
        2
    );
    assert_eq!(robustness.rolling_outcomes.windows.len(), 2);
    assert_eq!(robustness.rolling_outcomes.windows[0].start_ordinal, 1);
    assert_eq!(robustness.rolling_outcomes.windows[0].end_ordinal, 20);
    assert_eq!(robustness.rolling_outcomes.windows[1].start_ordinal, 2);
    assert_eq!(robustness.rolling_outcomes.windows[1].end_ordinal, 21);
}

#[test]
fn rolling_outcomes_report_invalid_and_insufficient_configuration() {
    let positions = vec![position("a", 1, "ES", PositionSide::Long, None, 1.0, None)];
    let mut zero_window = request(positions.clone());
    zero_window.rolling_window = 0;
    assert_eq!(
        evaluate(&zero_window)
            .robustness
            .expect("robustness requested")
            .rolling_outcomes
            .worst_window_mean
            .status,
        MetricStatus::InvalidInput
    );

    let mut large_window = request(positions);
    large_window.rolling_window = 2;
    assert_eq!(
        evaluate(&large_window)
            .robustness
            .expect("robustness requested")
            .rolling_outcomes
            .worst_window_mean
            .status,
        MetricStatus::InsufficientData
    );
}

#[test]
fn breakdown_dimensions_and_buckets_are_sorted_and_deduplicated() {
    let mut z = position(
        "z",
        1,
        "ZB",
        PositionSide::Short,
        Some("macro"),
        1.0,
        Some(1.0),
    );
    z.dimensions.close_reasons = vec!["target".into(), "partial".into(), "target".into()];
    z.dimensions.tags.insert("session".into(), "us".into());
    let a = position("a", 2, "AL", PositionSide::Long, None, -1.0, Some(-1.0));
    let mut evaluation_request = request(vec![z, a]);
    evaluation_request.breakdowns = vec![
        BreakdownDimension::Tag("session".into()),
        BreakdownDimension::CloseReason,
        BreakdownDimension::Symbol,
        BreakdownDimension::Symbol,
        BreakdownDimension::Side,
    ];

    let report = evaluate(&evaluation_request);
    let breakdowns = report.breakdowns.expect("breakdowns requested");
    assert_eq!(breakdowns.len(), 4);
    assert_eq!(breakdowns[0].dimension, BreakdownDimension::Symbol);
    assert_eq!(breakdowns[1].dimension, BreakdownDimension::Side);
    assert_eq!(breakdowns[2].dimension, BreakdownDimension::CloseReason);
    assert_eq!(
        breakdowns[3].dimension,
        BreakdownDimension::Tag("session".into())
    );
    assert_eq!(
        breakdowns[0]
            .buckets
            .iter()
            .map(|bucket| bucket.value.clone())
            .collect::<Vec<_>>(),
        vec![
            BreakdownValue::Text("AL".into()),
            BreakdownValue::Text("ZB".into()),
        ]
    );

    let close_reason = &breakdowns[2];
    assert_eq!(close_reason.buckets.len(), 3);
    let target = close_reason
        .buckets
        .iter()
        .find(|bucket| bucket.value == BreakdownValue::Text("target".into()))
        .expect("target bucket");
    assert_eq!(target.performance.position_count, 1);
}

#[test]
fn section_selection_omits_unrequested_sections_without_changing_selected_shapes() {
    let mut evaluation_request = request(vec![position(
        "a",
        1,
        "ES",
        PositionSide::Long,
        None,
        1.0,
        Some(1.0),
    )]);
    evaluation_request.context = EvaluationContext {
        provider_id: Some("provider-7".into()),
        source_id: Some("telegram:channel-3".into()),
    };
    evaluation_request.sections =
        BTreeSet::from([EvaluationSection::Coverage, EvaluationSection::RMetrics]);

    let report = evaluate(&evaluation_request);
    assert_eq!(report.context, evaluation_request.context);
    assert_eq!(report.requested_sections, evaluation_request.sections);
    assert!(report.coverage.is_some());
    assert!(report.r_metrics.is_some());
    assert!(report.position_performance.is_none());
    assert!(report.excursions.is_none());
    assert!(report.execution.is_none());
    assert!(report.robustness.is_none());
    assert!(report.breakdowns.is_none());

    let json = serde_json::to_value(&report).expect("selected report serializes");
    assert!(json.get("coverage").is_some());
    assert!(json.get("r_metrics").is_some());
    assert!(json.get("position_performance").is_none());
    assert!(json.get("breakdowns").is_none());
}

#[test]
fn breakdown_minimum_count_and_global_row_limit_are_deterministic() {
    let mut evaluation_request = request(vec![
        position("a1", 1, "A", PositionSide::Long, None, 1.0, None),
        position("a2", 2, "A", PositionSide::Long, None, 2.0, None),
        position("b", 3, "B", PositionSide::Short, None, 3.0, None),
    ]);
    evaluation_request.breakdowns = vec![BreakdownDimension::Symbol, BreakdownDimension::Side];
    evaluation_request.minimum_breakdown_bucket_count = 2;
    evaluation_request.maximum_breakdown_rows = Some(1);

    let report = evaluate(&evaluation_request);
    let breakdowns = report.breakdowns.expect("breakdowns requested");
    assert_eq!(breakdowns.len(), 2);
    assert_eq!(breakdowns[0].dimension, BreakdownDimension::Symbol);
    assert_eq!(breakdowns[0].buckets.len(), 1);
    assert_eq!(
        breakdowns[0].buckets[0].value,
        BreakdownValue::Text("A".into())
    );
    assert_eq!(breakdowns[1].dimension, BreakdownDimension::Side);
    assert!(breakdowns[1].buckets.is_empty());
    assert_eq!(
        report.breakdown_rows,
        BreakdownRowSummary {
            available_rows: 2,
            included_rows: 1,
            truncated: true,
        }
    );
}

#[test]
fn evaluation_request_keeps_pre_options_serde_shape() {
    let json = serde_json::json!({
        "positions": [],
        "lifecycle": null,
        "filter": {"symbols": ["ES"]},
        "breakdowns": ["symbol"],
        "rolling_window": 4
    });
    let decoded: EvaluationRequest =
        serde_json::from_value(json.clone()).expect("legacy request shape deserializes");
    assert_eq!(decoded.filter.symbols, ["ES"]);
    assert_eq!(decoded.breakdowns, [BreakdownDimension::Symbol]);
    assert_eq!(decoded.rolling_window, 4);
    assert_eq!(decoded.sections, EvaluationSection::all());

    let encoded = serde_json::to_value(decoded).expect("request serializes");
    assert!(encoded.get("options").is_none());
    assert_eq!(encoded["filter"]["symbols"][0], "ES");
}

#[test]
fn additive_evaluation_fields_default_when_deserializing_older_payloads() {
    let report = evaluate(&request(vec![position(
        "a",
        1,
        "ES",
        PositionSide::Long,
        None,
        1.0,
        Some(1.0),
    )]));
    let mut json = serde_json::to_value(report).expect("evaluation serializes");
    let r_metrics = json
        .get_mut("r_metrics")
        .and_then(serde_json::Value::as_object_mut)
        .expect("R metrics object");
    for field in [
        "profit_factor",
        "average_winner_r",
        "average_loser_r",
        "best_r",
        "worst_r",
        "quantiles",
        "cumulative_r_curve",
        "max_realized_r_drawdown",
    ] {
        r_metrics.remove(field);
    }
    json.get_mut("robustness")
        .and_then(serde_json::Value::as_object_mut)
        .expect("robustness object")
        .remove("pnl_concentration");

    let restored: EvaluationReport =
        serde_json::from_value(json).expect("older evaluation payload deserializes");
    let r_metrics = restored.r_metrics.expect("old R metrics remain present");
    let robustness = restored.robustness.expect("old robustness remains present");
    assert_eq!(
        r_metrics.profit_factor.status,
        MetricStatus::InsufficientData
    );
    assert_eq!(
        r_metrics.cumulative_r_curve.status,
        MetricStatus::InsufficientData
    );
    assert_eq!(
        robustness.pnl_concentration.top_1.status,
        MetricStatus::InsufficientData
    );
}

#[test]
fn empty_evaluation_has_explicit_unavailable_metrics_and_no_score() {
    let report = evaluate(&EvaluationRequest::default());

    let coverage = report.coverage.expect("coverage requested");
    let performance = report
        .position_performance
        .expect("position performance requested");
    let r_metrics = report.r_metrics.expect("R metrics requested");
    assert_eq!(coverage.selected_positions, 0);
    assert_eq!(
        performance.total_outcome.status,
        MetricStatus::InsufficientData
    );
    assert_eq!(coverage.acceptance_rate.status, MetricStatus::NotApplicable);
    assert_eq!(r_metrics.mean_r.status, MetricStatus::InsufficientData);
    assert!(report.breakdowns.expect("breakdowns requested").is_empty());
}
