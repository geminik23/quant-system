use std::collections::{BTreeMap, BTreeSet};

use super::stats::{mean, median, quantile_sorted, sample_standard_deviation};
use super::{
    BootstrapConfig, BreakdownBucket, BreakdownDimension, BreakdownRowSummary, BreakdownValue,
    CoverageSection, CumulativeRPoint, EvaluationBreakdown, EvaluationPositionRows,
    EvaluationReport, EvaluationRequest, EvaluationSection, ExcursionMetricsSection,
    ExecutionDiagnosticsSection, IntrinsicRobustnessSection, LifecycleCounts, MetricValue,
    OutcomeClassification, PnlConcentrationSection, PositionOutcome, PositionPerformanceSection,
    RMetricsSection, RQuantiles, RemovalImpact, RollingOutcome, RollingOutcomes,
    bootstrap_mean_confidence, wilson_interval,
};

/// Evaluates normalized provider outcomes without coupling to a backtest report.
///
/// Invalid non-finite `outcome` values remain visible in coverage but are omitted
/// from outcome-dependent calculations. Optional R, excursion, and execution
/// observations are independently included when finite.
pub fn evaluate(request: &EvaluationRequest) -> EvaluationReport {
    let selected: Vec<&PositionOutcome> = request
        .positions
        .iter()
        .filter(|position| request.filter.matches(position))
        .collect();

    let section_requested = |section| request.sections.contains(&section);
    let (breakdowns, breakdown_rows) = if section_requested(EvaluationSection::Breakdowns) {
        let (breakdowns, summary) = breakdowns(request, &selected);
        (Some(breakdowns), summary)
    } else {
        (None, BreakdownRowSummary::default())
    };

    EvaluationReport {
        context: request.context.clone(),
        requested_sections: request.sections.clone(),
        coverage: section_requested(EvaluationSection::Coverage)
            .then(|| coverage(request, &selected)),
        position_performance: section_requested(EvaluationSection::PositionPerformance)
            .then(|| performance(&selected, request.bootstrap)),
        r_metrics: section_requested(EvaluationSection::RMetrics)
            .then(|| r_metrics(&selected, request.bootstrap)),
        excursions: section_requested(EvaluationSection::Excursions)
            .then(|| excursion_metrics(&selected)),
        execution: section_requested(EvaluationSection::Execution)
            .then(|| execution_metrics(&selected)),
        robustness: section_requested(EvaluationSection::Robustness)
            .then(|| robustness(&selected, request.rolling_window)),
        breakdowns,
        breakdown_rows,
        position_rows: request
            .include_position_rows
            .then(|| selected_position_rows(&selected, request.maximum_position_rows)),
    }
}

fn selected_position_rows(
    positions: &[&PositionOutcome],
    maximum_rows: Option<usize>,
) -> EvaluationPositionRows {
    let mut rows: Vec<_> = positions
        .iter()
        .map(|position| (*position).clone())
        .collect();
    rows.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
    });
    let available_rows = rows.len();
    if let Some(maximum_rows) = maximum_rows {
        rows.truncate(maximum_rows);
    }
    EvaluationPositionRows {
        available_rows,
        included_rows: rows.len(),
        truncated: rows.len() < available_rows,
        rows,
    }
}

fn coverage(request: &EvaluationRequest, positions: &[&PositionOutcome]) -> CoverageSection {
    let valid_outcomes = positions
        .iter()
        .filter(|position| position.outcome.is_finite())
        .count();
    let r_count = positions
        .iter()
        .filter(|position| position.r_multiple.is_some_and(f64::is_finite))
        .count();
    let excursion_count = positions
        .iter()
        .filter(|position| {
            position.excursions.is_some_and(|excursion| {
                excursion.favorable_r.is_some_and(f64::is_finite)
                    || excursion.adverse_r.is_some_and(f64::is_finite)
            })
        })
        .count();
    let execution_count = positions
        .iter()
        .filter(|position| {
            position.execution.is_some_and(|execution| {
                execution.slippage_bps.is_some_and(f64::is_finite)
                    || execution.latency_ms.is_some_and(f64::is_finite)
                    || execution.fill_ratio.is_some_and(f64::is_finite)
            })
        })
        .count();

    let (acceptance_rate, open_rate, completion_rate) = match request.lifecycle {
        Some(lifecycle) => lifecycle_rates(lifecycle),
        None => (
            MetricValue::not_applicable("lifecycle counts were not provided"),
            MetricValue::not_applicable("lifecycle counts were not provided"),
            MetricValue::not_applicable("lifecycle counts were not provided"),
        ),
    };

    CoverageSection {
        provided_positions: request.positions.len(),
        selected_positions: positions.len(),
        filtered_out_positions: request.positions.len() - positions.len(),
        valid_outcomes,
        invalid_outcomes: positions.len() - valid_outcomes,
        source: request.source_coverage,
        lifecycle: request.lifecycle,
        acceptance_rate,
        open_rate,
        completion_rate,
        r_coverage: observation_coverage(r_count, positions.len(), "R observations"),
        excursion_coverage: observation_coverage(
            excursion_count,
            positions.len(),
            "excursion observations",
        ),
        execution_coverage: observation_coverage(
            execution_count,
            positions.len(),
            "execution observations",
        ),
    }
}

fn lifecycle_rates(
    lifecycle: LifecycleCounts,
) -> (MetricValue<f64>, MetricValue<f64>, MetricValue<f64>) {
    (
        bounded_rate(
            lifecycle.accepted,
            lifecycle.candidates,
            "accepted",
            "candidates",
        ),
        bounded_rate(lifecycle.opened, lifecycle.accepted, "opened", "accepted"),
        bounded_rate(lifecycle.completed, lifecycle.opened, "completed", "opened"),
    )
}

fn bounded_rate(
    numerator: u64,
    denominator: u64,
    numerator_name: &str,
    denominator_name: &str,
) -> MetricValue<f64> {
    if numerator > denominator {
        return MetricValue::invalid_input(format!(
            "{numerator_name} cannot exceed {denominator_name}"
        ));
    }
    if denominator == 0 {
        return MetricValue::insufficient_data(format!(
            "{denominator_name} must be greater than zero"
        ));
    }
    MetricValue::available(numerator as f64 / denominator as f64)
}

fn observation_coverage(count: usize, total: usize, name: &str) -> MetricValue<f64> {
    if total == 0 {
        MetricValue::insufficient_data(format!(
            "at least one selected position is required for {name} coverage"
        ))
    } else {
        MetricValue::available(count as f64 / total as f64)
    }
}

fn performance(
    positions: &[&PositionOutcome],
    bootstrap: BootstrapConfig,
) -> PositionPerformanceSection {
    let positions: Vec<&PositionOutcome> = positions
        .iter()
        .copied()
        .filter(|position| position.outcome.is_finite())
        .collect();
    let wins = positions
        .iter()
        .filter(|position| position.classification() == OutcomeClassification::Win)
        .count();
    let losses = positions
        .iter()
        .filter(|position| position.classification() == OutcomeClassification::Loss)
        .count();
    let breakeven = positions.len() - wins - losses;

    if positions.is_empty() {
        return PositionPerformanceSection {
            position_count: 0,
            wins,
            losses,
            breakeven,
            total_outcome: no_outcomes(),
            mean_outcome: no_outcomes(),
            median_outcome: no_outcomes(),
            win_rate: no_outcomes(),
            win_rate_confidence: MetricValue::insufficient_data(
                "at least one finite outcome is required",
            ),
            gross_positive: no_outcomes(),
            gross_negative: no_outcomes(),
            profit_factor: no_outcomes(),
            payoff_ratio: no_outcomes(),
            best_outcome: no_outcomes(),
            worst_outcome: no_outcomes(),
            mean_outcome_confidence: bootstrap_mean_confidence(&[], bootstrap),
        };
    }

    let outcomes: Vec<f64> = positions.iter().map(|position| position.outcome).collect();
    let total = outcomes.iter().sum::<f64>();
    let gross_positive = positions
        .iter()
        .filter(|position| position.classification() == OutcomeClassification::Win)
        .map(|position| position.outcome.abs())
        .sum::<f64>();
    let gross_negative = positions
        .iter()
        .filter(|position| position.classification() == OutcomeClassification::Loss)
        .map(|position| position.outcome.abs())
        .sum::<f64>();
    let average_win = (wins > 0).then(|| gross_positive / wins as f64);
    let average_loss = (losses > 0).then(|| gross_negative / losses as f64);

    PositionPerformanceSection {
        position_count: positions.len(),
        wins,
        losses,
        breakeven,
        total_outcome: MetricValue::available(total),
        mean_outcome: MetricValue::available(total / positions.len() as f64),
        median_outcome: MetricValue::available(
            median(&outcomes).expect("non-empty outcomes checked above"),
        ),
        win_rate: MetricValue::available(wins as f64 / outcomes.len() as f64),
        win_rate_confidence: wilson_interval(wins, outcomes.len(), bootstrap.confidence_level),
        gross_positive: MetricValue::available(gross_positive),
        gross_negative: MetricValue::available(gross_negative),
        profit_factor: if gross_negative > 0.0 {
            MetricValue::available(gross_positive / gross_negative)
        } else {
            MetricValue::not_applicable("profit factor requires at least one losing position")
        },
        payoff_ratio: match (average_win, average_loss) {
            (Some(win), Some(loss)) => MetricValue::available(win / loss),
            _ => MetricValue::not_applicable(
                "payoff ratio requires both winning and losing positions",
            ),
        },
        best_outcome: MetricValue::available(
            outcomes
                .iter()
                .copied()
                .max_by(f64::total_cmp)
                .expect("non-empty outcomes checked above"),
        ),
        worst_outcome: MetricValue::available(
            outcomes
                .iter()
                .copied()
                .min_by(f64::total_cmp)
                .expect("non-empty outcomes checked above"),
        ),
        mean_outcome_confidence: bootstrap_mean_confidence(&outcomes, bootstrap),
    }
}

fn no_outcomes<T>() -> MetricValue<T> {
    MetricValue::insufficient_data("at least one finite outcome is required")
}

fn r_metrics(positions: &[&PositionOutcome], bootstrap: BootstrapConfig) -> RMetricsSection {
    let mut observed: Vec<(&PositionOutcome, f64)> = positions
        .iter()
        .filter_map(|position| {
            position
                .r_multiple
                .filter(|value| value.is_finite())
                .map(|value| (*position, value))
        })
        .collect();
    observed.sort_by(|(left, left_r), (right, right_r)| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left_r.total_cmp(right_r))
    });
    let values: Vec<f64> = observed.iter().map(|(_, value)| *value).collect();
    let missing_or_invalid_count = positions.len() - values.len();

    if values.is_empty() {
        return RMetricsSection {
            observed_count: 0,
            missing_or_invalid_count,
            total_r: no_r(),
            mean_r: no_r(),
            median_r: no_r(),
            standard_deviation_r: no_r(),
            positive_r_rate: no_r(),
            positive_r_rate_confidence: no_r(),
            mean_r_confidence: bootstrap_mean_confidence(&values, bootstrap),
            profit_factor: no_r(),
            average_winner_r: no_r(),
            average_loser_r: no_r(),
            best_r: no_r(),
            worst_r: no_r(),
            quantiles: no_r(),
            cumulative_r_curve: no_r(),
            max_realized_r_drawdown: no_r(),
        };
    }

    let positive_values: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .collect();
    let negative_values: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .collect();
    let gross_positive = positive_values.iter().sum::<f64>();
    let gross_negative = negative_values.iter().map(|value| value.abs()).sum::<f64>();
    let (cumulative_r_curve, max_realized_r_drawdown) = cumulative_r_metrics(&observed);

    RMetricsSection {
        observed_count: values.len(),
        missing_or_invalid_count,
        total_r: finite_r_metric(values.iter().sum(), "total R"),
        mean_r: finite_r_metric(
            mean(&values).expect("non-empty R values checked above"),
            "mean R",
        ),
        median_r: finite_r_metric(
            median(&values).expect("non-empty R values checked above"),
            "median R",
        ),
        standard_deviation_r: sample_standard_deviation(&values).map_or_else(
            || MetricValue::insufficient_data("at least two R observations are required"),
            |value| finite_r_metric(value, "R standard deviation"),
        ),
        positive_r_rate: MetricValue::available(positive_values.len() as f64 / values.len() as f64),
        positive_r_rate_confidence: wilson_interval(
            positive_values.len(),
            values.len(),
            bootstrap.confidence_level,
        ),
        mean_r_confidence: bootstrap_mean_confidence(&values, bootstrap),
        profit_factor: if !gross_positive.is_finite() || !gross_negative.is_finite() {
            MetricValue::invalid_input("R profit-factor totals exceed the finite f64 range")
        } else if gross_negative > 0.0 {
            finite_r_metric(gross_positive / gross_negative, "R profit factor")
        } else {
            MetricValue::not_applicable(
                "R profit factor requires at least one negative R observation",
            )
        },
        average_winner_r: observed_r_average(&positive_values, "positive"),
        average_loser_r: observed_r_average(&negative_values, "negative"),
        best_r: finite_r_metric(
            values
                .iter()
                .copied()
                .max_by(f64::total_cmp)
                .expect("non-empty R values checked above"),
            "best R",
        ),
        worst_r: finite_r_metric(
            values
                .iter()
                .copied()
                .min_by(f64::total_cmp)
                .expect("non-empty R values checked above"),
            "worst R",
        ),
        quantiles: r_quantiles(&values),
        cumulative_r_curve,
        max_realized_r_drawdown,
    }
}

fn no_r<T>() -> MetricValue<T> {
    MetricValue::insufficient_data("at least one finite R observation is required")
}

fn finite_r_metric(value: f64, name: &str) -> MetricValue<f64> {
    if value.is_finite() {
        MetricValue::available(value)
    } else {
        MetricValue::invalid_input(format!("{name} exceeds the finite f64 range"))
    }
}

fn observed_r_average(values: &[f64], sign: &str) -> MetricValue<f64> {
    if values.is_empty() {
        MetricValue::not_applicable(format!(
            "average {sign} R requires at least one {sign} R observation"
        ))
    } else {
        finite_r_metric(
            mean(values).expect("non-empty R values checked above"),
            &format!("average {sign} R"),
        )
    }
}

fn r_quantiles(values: &[f64]) -> MetricValue<RQuantiles> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let quantiles = RQuantiles {
        p05: quantile_sorted(&sorted, 0.05),
        p10: quantile_sorted(&sorted, 0.10),
        p25: quantile_sorted(&sorted, 0.25),
        p50: quantile_sorted(&sorted, 0.50),
        p75: quantile_sorted(&sorted, 0.75),
        p90: quantile_sorted(&sorted, 0.90),
        p95: quantile_sorted(&sorted, 0.95),
    };
    let values = [
        quantiles.p05,
        quantiles.p10,
        quantiles.p25,
        quantiles.p50,
        quantiles.p75,
        quantiles.p90,
        quantiles.p95,
    ];
    if values.into_iter().all(f64::is_finite) {
        MetricValue::available(quantiles)
    } else {
        MetricValue::invalid_input("R quantiles exceed the finite f64 range")
    }
}

fn cumulative_r_metrics(
    observed: &[(&PositionOutcome, f64)],
) -> (MetricValue<Vec<CumulativeRPoint>>, MetricValue<f64>) {
    let mut cumulative_r = 0.0_f64;
    let mut peak_r = 0.0_f64;
    let mut max_drawdown_r = 0.0_f64;
    let mut drawdown_overflowed = false;
    let mut curve = Vec::with_capacity(observed.len());

    for (position, realized_r) in observed {
        cumulative_r += realized_r;
        if !cumulative_r.is_finite() {
            let reason = "cumulative realized R exceeds the finite f64 range";
            return (
                MetricValue::invalid_input(reason),
                MetricValue::invalid_input(reason),
            );
        }
        peak_r = peak_r.max(cumulative_r);
        let drawdown_r = peak_r - cumulative_r;
        if drawdown_r.is_finite() {
            max_drawdown_r = max_drawdown_r.max(drawdown_r);
        } else {
            drawdown_overflowed = true;
        }
        curve.push(CumulativeRPoint {
            position_id: position.id.clone(),
            ordinal: position.ordinal,
            realized_r: *realized_r,
            cumulative_r,
        });
    }

    let drawdown = if drawdown_overflowed {
        MetricValue::invalid_input("realized-R drawdown exceeds the finite f64 range")
    } else {
        MetricValue::available(max_drawdown_r)
    };
    (MetricValue::available(curve), drawdown)
}

fn excursion_metrics(positions: &[&PositionOutcome]) -> ExcursionMetricsSection {
    let favorable: Vec<f64> = positions
        .iter()
        .filter_map(|position| {
            position
                .excursions
                .and_then(|value| value.favorable_r)
                .filter(|value| value.is_finite())
        })
        .collect();
    let adverse: Vec<f64> = positions
        .iter()
        .filter_map(|position| {
            position
                .excursions
                .and_then(|value| value.adverse_r)
                .filter(|value| value.is_finite())
        })
        .collect();

    ExcursionMetricsSection {
        favorable_observed_count: favorable.len(),
        adverse_observed_count: adverse.len(),
        mean_favorable_r: observed_mean(&favorable, "favorable excursion"),
        median_favorable_r: observed_median(&favorable, "favorable excursion"),
        mean_adverse_r: observed_mean(&adverse, "adverse excursion"),
        median_adverse_r: observed_median(&adverse, "adverse excursion"),
    }
}

fn execution_metrics(positions: &[&PositionOutcome]) -> ExecutionDiagnosticsSection {
    let positions_with_diagnostics = positions
        .iter()
        .filter(|position| position.execution.is_some())
        .count();
    let slippage: Vec<f64> = positions
        .iter()
        .filter_map(|position| {
            position
                .execution
                .and_then(|value| value.slippage_bps)
                .filter(|value| value.is_finite())
        })
        .collect();
    let latency: Vec<f64> = positions
        .iter()
        .filter_map(|position| {
            position
                .execution
                .and_then(|value| value.latency_ms)
                .filter(|value| value.is_finite())
        })
        .collect();
    let fill_ratio: Vec<f64> = positions
        .iter()
        .filter_map(|position| {
            position
                .execution
                .and_then(|value| value.fill_ratio)
                .filter(|value| value.is_finite())
        })
        .collect();
    let adverse_slippage = slippage.iter().filter(|value| **value > 0.0).count();

    ExecutionDiagnosticsSection {
        positions_with_diagnostics,
        slippage_observed_count: slippage.len(),
        latency_observed_count: latency.len(),
        fill_ratio_observed_count: fill_ratio.len(),
        mean_slippage_bps: observed_mean(&slippage, "slippage"),
        median_slippage_bps: observed_median(&slippage, "slippage"),
        adverse_slippage_rate: if slippage.is_empty() {
            MetricValue::insufficient_data("at least one finite slippage observation is required")
        } else {
            MetricValue::available(adverse_slippage as f64 / slippage.len() as f64)
        },
        mean_latency_ms: observed_mean(&latency, "latency"),
        median_latency_ms: observed_median(&latency, "latency"),
        mean_fill_ratio: observed_mean(&fill_ratio, "fill ratio"),
    }
}

fn observed_mean(values: &[f64], name: &str) -> MetricValue<f64> {
    mean(values).map_or_else(
        || {
            MetricValue::insufficient_data(format!(
                "at least one finite {name} observation is required"
            ))
        },
        MetricValue::available,
    )
}

fn observed_median(values: &[f64], name: &str) -> MetricValue<f64> {
    median(values).map_or_else(
        || {
            MetricValue::insufficient_data(format!(
                "at least one finite {name} observation is required"
            ))
        },
        MetricValue::available,
    )
}

fn robustness(positions: &[&PositionOutcome], window_size: usize) -> IntrinsicRobustnessSection {
    let finite_positions: Vec<&PositionOutcome> = positions
        .iter()
        .copied()
        .filter(|position| position.outcome.is_finite())
        .collect();
    let outcomes: Vec<f64> = finite_positions
        .iter()
        .map(|position| position.outcome)
        .collect();
    let removal_count = five_percent_count(outcomes.len());

    let top_one = positive_concentration(&finite_positions, 1);
    let pnl_concentration = PnlConcentrationSection {
        top_1: top_one.clone(),
        top_3: positive_concentration(&finite_positions, 3),
        top_5: positive_concentration(&finite_positions, 5),
        top_10: positive_concentration(&finite_positions, 10),
    };

    IntrinsicRobustnessSection {
        best_one_removed: removal_impact(&outcomes, 1),
        best_five_percent_removed: removal_impact(&outcomes, removal_count),
        best_one_positive_concentration: top_one,
        best_five_percent_positive_concentration: positive_concentration(
            &finite_positions,
            removal_count,
        ),
        pnl_concentration,
        rolling_outcomes: rolling_outcomes(positions, window_size),
    }
}

fn five_percent_count(position_count: usize) -> usize {
    if position_count == 0 {
        0
    } else {
        position_count.div_ceil(20)
    }
}

fn removal_impact(outcomes: &[f64], remove_count: usize) -> MetricValue<RemovalImpact> {
    if outcomes.len() < 2 || remove_count == 0 || remove_count >= outcomes.len() {
        return MetricValue::insufficient_data(
            "at least two finite outcomes with a non-empty remainder are required",
        );
    }

    let mut sorted = outcomes.to_vec();
    sorted.sort_by(|left, right| right.total_cmp(left));
    let original_total = sorted.iter().sum::<f64>();
    let removed_total = sorted[..remove_count].iter().sum::<f64>();
    let remaining_total = original_total - removed_total;

    MetricValue::available(RemovalImpact {
        removed_count: remove_count,
        original_total,
        removed_total,
        remaining_total,
        remaining_mean: remaining_total / (outcomes.len() - remove_count) as f64,
    })
}

fn positive_concentration(positions: &[&PositionOutcome], take_count: usize) -> MetricValue<f64> {
    if positions.is_empty() || take_count == 0 {
        return MetricValue::insufficient_data("at least one finite outcome is required");
    }

    let mut positives: Vec<f64> = positions
        .iter()
        .filter(|position| position.classification() == OutcomeClassification::Win)
        .map(|position| position.outcome.abs())
        .collect();
    if positives.is_empty() {
        return MetricValue::not_applicable("positive concentration requires a winning position");
    }

    positives.sort_by(|left, right| right.total_cmp(left));
    let gross_positive = positives.iter().sum::<f64>();
    let concentrated = positives.iter().take(take_count).sum::<f64>();
    if !gross_positive.is_finite() || !concentrated.is_finite() {
        MetricValue::invalid_input("positive P&L concentration exceeds the finite f64 range")
    } else if gross_positive > 0.0 {
        MetricValue::available(concentrated / gross_positive)
    } else {
        MetricValue::not_applicable("positive concentration requires positive gross P&L")
    }
}

fn rolling_outcomes(positions: &[&PositionOutcome], window_size: usize) -> RollingOutcomes {
    if window_size == 0 {
        return RollingOutcomes {
            window_size,
            windows: Vec::new(),
            worst_window_mean: MetricValue::invalid_input(
                "rolling_window must be greater than zero",
            ),
            best_window_mean: MetricValue::invalid_input(
                "rolling_window must be greater than zero",
            ),
            positive_window_rate: MetricValue::invalid_input(
                "rolling_window must be greater than zero",
            ),
        };
    }

    let mut ordered: Vec<&PositionOutcome> = positions
        .iter()
        .copied()
        .filter(|position| position.outcome.is_finite())
        .collect();
    ordered.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left.outcome.total_cmp(&right.outcome))
    });

    if ordered.len() < window_size {
        let metric = || {
            MetricValue::insufficient_data(format!(
                "at least {window_size} finite outcomes are required"
            ))
        };
        return RollingOutcomes {
            window_size,
            windows: Vec::new(),
            worst_window_mean: metric(),
            best_window_mean: metric(),
            positive_window_rate: metric(),
        };
    }

    let windows: Vec<RollingOutcome> = ordered
        .windows(window_size)
        .map(|window| {
            let total_outcome = window.iter().map(|position| position.outcome).sum::<f64>();
            RollingOutcome {
                start_ordinal: window.first().expect("window is non-empty").ordinal,
                end_ordinal: window.last().expect("window is non-empty").ordinal,
                position_count: window_size,
                total_outcome,
                mean_outcome: total_outcome / window_size as f64,
            }
        })
        .collect();
    let positive_windows = windows
        .iter()
        .filter(|window| window.total_outcome > 0.0)
        .count();
    let worst = windows
        .iter()
        .map(|window| window.mean_outcome)
        .min_by(f64::total_cmp)
        .expect("at least one rolling window exists");
    let best = windows
        .iter()
        .map(|window| window.mean_outcome)
        .max_by(f64::total_cmp)
        .expect("at least one rolling window exists");

    RollingOutcomes {
        window_size,
        positive_window_rate: MetricValue::available(
            positive_windows as f64 / windows.len() as f64,
        ),
        worst_window_mean: MetricValue::available(worst),
        best_window_mean: MetricValue::available(best),
        windows,
    }
}

fn breakdowns(
    request: &EvaluationRequest,
    positions: &[&PositionOutcome],
) -> (Vec<EvaluationBreakdown>, BreakdownRowSummary) {
    let dimensions: BTreeSet<BreakdownDimension> = request.breakdowns.iter().cloned().collect();
    let minimum_count = request.minimum_breakdown_bucket_count;
    let maximum_rows = request.maximum_breakdown_rows.unwrap_or(usize::MAX);
    let mut available_rows = 0;
    let mut included_rows = 0;
    let mut breakdowns = Vec::with_capacity(dimensions.len());

    for dimension in dimensions {
        let mut grouped: BTreeMap<BreakdownValue, Vec<&PositionOutcome>> = BTreeMap::new();
        for position in positions {
            for value in breakdown_values(position, &dimension) {
                grouped.entry(value).or_default().push(position);
            }
        }

        let eligible: Vec<_> = grouped
            .into_iter()
            .filter(|(_, bucket_positions)| bucket_positions.len() >= minimum_count)
            .collect();
        available_rows += eligible.len();
        let remaining = maximum_rows.saturating_sub(included_rows);
        let buckets = eligible
            .into_iter()
            .take(remaining)
            .map(|(value, bucket_positions)| BreakdownBucket {
                value,
                performance: performance(&bucket_positions, request.bootstrap),
                r_metrics: r_metrics(&bucket_positions, request.bootstrap),
            })
            .collect::<Vec<_>>();
        included_rows += buckets.len();
        breakdowns.push(EvaluationBreakdown { dimension, buckets });
    }

    (
        breakdowns,
        BreakdownRowSummary {
            available_rows,
            included_rows,
            truncated: included_rows < available_rows,
        },
    )
}

fn breakdown_values(
    position: &PositionOutcome,
    dimension: &BreakdownDimension,
) -> Vec<BreakdownValue> {
    match dimension {
        BreakdownDimension::Symbol => {
            vec![BreakdownValue::Text(position.dimensions.symbol.clone())]
        }
        BreakdownDimension::Side => vec![BreakdownValue::Side(position.dimensions.side)],
        BreakdownDimension::Group => vec![
            position
                .dimensions
                .group
                .clone()
                .map_or(BreakdownValue::Missing, BreakdownValue::Text),
        ],
        BreakdownDimension::CloseReason => {
            let values: BTreeSet<BreakdownValue> = position
                .dimensions
                .close_reasons
                .iter()
                .cloned()
                .map(BreakdownValue::Text)
                .collect();
            if values.is_empty() {
                vec![BreakdownValue::Missing]
            } else {
                values.into_iter().collect()
            }
        }
        BreakdownDimension::Tag(key) => vec![
            position
                .dimensions
                .tags
                .get(key)
                .cloned()
                .map_or(BreakdownValue::Missing, BreakdownValue::Text),
        ],
    }
}
