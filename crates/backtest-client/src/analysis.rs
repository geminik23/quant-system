#![cfg(feature = "analysis")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use qs_backtest::evaluation::{
    BootstrapConfig, BreakdownDimension, EvaluationContext, EvaluationOptions, EvaluationReport,
    EvaluationRequest, EvaluationSection, ExcursionInput, ExecutionDiagnosticsInput, GroupFilter,
    LifecycleCounts, OutcomeClassification, PositionDimensions, PositionFilter, PositionOutcome,
    PositionSide, SourceCoverageCounts, evaluate,
};
use qs_backtest::ledger::{ActionDisposition, LifecycleLedger};
use qs_backtest::{
    CloseEvent, CompletedPosition, ConversionResult, FutureBacktestArtifacts, MtmOutputSummary,
    OpenPositionSnapshot, PendingOrderLifecycleEvent, PendingOrderSnapshot, RecordedFill,
    RiskTranche, evaluation_request_from_future_artifacts,
};
use qs_backtest_api::{
    BacktestResultMsg, BreakdownDimensionMsg, EvaluationGroupFilterMsg, EvaluationPositionSideMsg,
    EvaluationSectionMsg, ProviderEvaluationOptionsMsg,
};

use thiserror::Error;

use crate::{
    ANALYSIS_DATASET_FORMAT_VERSION, AnalysisDatasetState, EXECUTION_DATASET_FORMAT_VERSION,
    ExecutionDatasetUnavailableReason, PersistedActionDisposition, PersistedAnalysisDataset,
    PersistedCloseEvent, PersistedCloseReason, PersistedCollectionCompleteness,
    PersistedCompletedPosition, PersistedConversionAudit, PersistedConversionLeg,
    PersistedConversionPriceSide, PersistedConversionRoute, PersistedConversionRouteLeg,
    PersistedDispositionStatus, PersistedEffectiveStop, PersistedEvaluationOptions,
    PersistedExecutionCompleteness, PersistedExecutionDataset, PersistedExecutionDatasetState,
    PersistedFill, PersistedFillPurpose, PersistedFxPair, PersistedFxPairDirection,
    PersistedGroupFilter, PersistedLifecycleCounts, PersistedMetricPopulation,
    PersistedNetPnlOutcome, PersistedOpenPosition, PersistedOrderType,
    PersistedOutcomeClassification, PersistedPendingLifecycleEvent, PersistedPendingLifecycleState,
    PersistedPendingOrder, PersistedPopulationUnit, PersistedPositionFilter,
    PersistedPositionOutcome, PersistedPositionSide, PersistedRiskBasisStatus,
    PersistedRiskTranche, PersistedSourceCoverageCounts, PersistedStopOrigin, PersistedTradeSide,
    SUPPORTED_FUTURE_RESULT_FORMAT_VERSION,
};

pub fn project_result_datasets(
    result: &BacktestResultMsg,
    requested_options: &ProviderEvaluationOptionsMsg,
) -> Result<(AnalysisDatasetState, PersistedExecutionDatasetState), AnalysisError> {
    let Some(future) = result.future.as_ref() else {
        return Ok((
            AnalysisDatasetState::Unavailable {
                reason: crate::AnalysisUnavailableReason::LegacyResult,
            },
            PersistedExecutionDatasetState::Unavailable {
                reason: ExecutionDatasetUnavailableReason::LegacyOrOmitted,
            },
        ));
    };
    if future.format_version != SUPPORTED_FUTURE_RESULT_FORMAT_VERSION {
        return Err(AnalysisError::UnsupportedFutureVersion {
            actual: future.format_version,
        });
    }

    let execution = decode_value(&future.execution_metadata, "execution metadata")?;
    let fills: Vec<RecordedFill> = decode_value(&future.recorded_fills, "recorded fills")?;
    let dispositions: Vec<ActionDisposition> =
        decode_value(&future.action_dispositions, "action dispositions")?;
    let close_events: Vec<CloseEvent> = decode_value(&future.close_events, "close events")?;
    let completed_positions: Vec<CompletedPosition> =
        decode_value(&future.completed_positions, "completed positions")?;
    let open_positions: Vec<OpenPositionSnapshot> =
        decode_value(&future.open_positions, "open positions")?;
    let pending_orders: Vec<PendingOrderSnapshot> =
        decode_value(&future.pending_orders, "pending orders")?;
    let pending_order_lifecycle: Vec<PendingOrderLifecycleEvent> =
        decode_serializable(&future.pending_order_lifecycle, "pending lifecycle")?;
    let equity_curve = decode_value(&future.mtm_equity_curve, "MTM equity curve")?;
    let mtm_output_summary: MtmOutputSummary =
        decode_serializable(&future.mtm_output_summary, "MTM output summary")?;
    let lifecycle = LifecycleLedger::from_records(dispositions.clone())
        .map_err(|error| AnalysisError::InvalidExecution(error.to_string()))?;
    let artifacts = FutureBacktestArtifacts {
        format_version: future.format_version,
        execution,
        fills: fills.clone(),
        close_events: close_events.clone(),
        completed_positions: completed_positions.clone(),
        open_positions: open_positions.clone(),
        pending_orders: pending_orders.clone(),
        pending_order_lifecycle: pending_order_lifecycle.clone(),
        lifecycle,
        equity_curve,
        mtm_output_summary,
        max_drawdown: future.mtm_max_drawdown,
        max_drawdown_pct: future.mtm_max_drawdown_pct,
    };

    if !future.provider_evaluation.is_null() {
        let _: EvaluationReport = decode_value(&future.provider_evaluation, "provider evaluation")?;
    }
    let mut options = options_from_msg(requested_options);
    options.include_position_rows = true;
    options.maximum_position_rows = None;
    let request = evaluation_request_from_future_artifacts(&artifacts, options.clone());
    let analysis = PersistedAnalysisDataset {
        format_version: ANALYSIS_DATASET_FORMAT_VERSION,

        positions: request.positions.iter().map(persist_position).collect(),
        lifecycle: request.lifecycle.map(persist_lifecycle),
        source_coverage: options.source_coverage.map(persist_source_coverage),
        default_options: persist_options(&options),
    };
    let execution = persist_execution(
        fills,
        dispositions,
        close_events,
        completed_positions,
        open_positions,
        pending_orders,
        pending_order_lifecycle,
    );
    Ok((
        AnalysisDatasetState::Complete(Box::new(analysis)),
        PersistedExecutionDatasetState::Complete(Box::new(execution)),
    ))
}

#[derive(Debug, Clone)]
pub struct AnalysisRecomputeRequest {
    pub generation: u64,
    pub filter: PersistedPositionFilter,
    pub breakdowns: Vec<String>,
    pub tail_fraction: f64,
}

#[derive(Debug, Clone, Default)]
pub struct AnalysisCoordinator {
    current_generation: Arc<std::sync::atomic::AtomicU64>,
}

impl AnalysisCoordinator {
    pub fn next_generation(&self) -> u64 {
        self.current_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn is_current(&self, generation: u64) -> bool {
        self.current_generation.load(Ordering::Acquire) == generation
    }

    pub async fn recompute(
        &self,
        dataset: Arc<PersistedAnalysisDataset>,
        mut request: AnalysisRecomputeRequest,
        cancellation: AnalysisCancellation,
    ) -> Result<Arc<AnalysisSnapshot>, AnalysisError> {
        let generation = self.next_generation();
        request.generation = generation;
        let snapshot = recompute_analysis(dataset, request, cancellation).await?;
        if !self.is_current(snapshot.generation) {
            return Err(AnalysisError::StaleGeneration);
        }
        Ok(Arc::new(snapshot))
    }
}

#[derive(Debug, Clone)]
pub struct AnalysisSnapshot {
    pub generation: u64,
    pub report: EvaluationReport,
    pub additional: AdditionalAnalytics,
}

#[derive(Debug, Clone, Default)]
pub struct AnalysisCancellation {
    cancelled: Arc<AtomicBool>,
}

impl AnalysisCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdditionalAnalytics {
    pub selected_positions: usize,
    pub expected_shortfall: LocalMetric,
    pub lag_one_same_outcome_rate: LocalMetric,
    pub transitions: OutcomeTransitions,
    pub calendar: Vec<CalendarOutcomeBucket>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalMetric {
    pub value: Option<f64>,
    pub status: LocalMetricStatus,
    pub population: PersistedMetricPopulation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalMetricStatus {
    Available,
    InsufficientData,
    InvalidInput,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutcomeTransitions {
    pub win_to_win: u64,
    pub win_to_non_win: u64,
    pub non_win_to_win: u64,
    pub non_win_to_non_win: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CalendarOutcomeBucket {
    pub year: i32,
    pub month: u32,
    pub observed_positions: usize,
    pub total_outcome: f64,
}

pub async fn recompute_analysis(
    dataset: Arc<PersistedAnalysisDataset>,
    request: AnalysisRecomputeRequest,
    cancellation: AnalysisCancellation,
) -> Result<AnalysisSnapshot, AnalysisError> {
    if cancellation.is_cancelled() {
        return Err(AnalysisError::Cancelled);
    }
    tokio::task::spawn_blocking(move || {
        if cancellation.is_cancelled() {
            return Err(AnalysisError::Cancelled);
        }
        let report =
            evaluate_persisted_dataset(&dataset, request.filter.clone(), request.breakdowns)?;
        let additional = additional_analytics(&dataset, &request.filter, request.tail_fraction)?;
        if cancellation.is_cancelled() {
            return Err(AnalysisError::Cancelled);
        }
        Ok(AnalysisSnapshot {
            generation: request.generation,
            report,
            additional,
        })
    })
    .await
    .map_err(|error| AnalysisError::Task(error.to_string()))?
}

pub fn evaluate_persisted_dataset(
    dataset: &PersistedAnalysisDataset,
    filter: PersistedPositionFilter,
    breakdowns: Vec<String>,
) -> Result<EvaluationReport, AnalysisError> {
    if dataset.format_version != ANALYSIS_DATASET_FORMAT_VERSION {
        return Err(AnalysisError::UnsupportedAnalysisVersion {
            actual: dataset.format_version,
        });
    }
    let mut options = restore_options(&dataset.default_options)?;
    options.source_coverage = dataset.source_coverage.map(restore_source_coverage);
    options.filter = restore_filter(&filter);
    options.breakdowns = breakdowns
        .iter()
        .map(|value| parse_breakdown(value))
        .collect::<Result<_, _>>()?;
    options.include_position_rows = true;
    options.maximum_position_rows = None;
    let request = EvaluationRequest {
        positions: dataset.positions.iter().map(restore_position).collect(),
        lifecycle: dataset.lifecycle.map(restore_lifecycle),
        options,
    };
    Ok(evaluate(&request))
}

fn additional_analytics(
    dataset: &PersistedAnalysisDataset,
    filter: &PersistedPositionFilter,
    tail_fraction: f64,
) -> Result<AdditionalAnalytics, AnalysisError> {
    if !tail_fraction.is_finite() || !(0.0..=1.0).contains(&tail_fraction) || tail_fraction == 0.0 {
        return Err(AnalysisError::InvalidTailFraction);
    }
    let mut selected: Vec<_> = dataset
        .positions
        .iter()
        .filter(|position| persisted_filter_matches(filter, position))
        .collect();
    selected.sort_by_key(|position| position.ordinal);
    let mut outcomes: Vec<_> = selected
        .iter()
        .map(|position| position.outcome)
        .filter(|value| value.is_finite())
        .collect();
    outcomes.sort_by(f64::total_cmp);
    let tail_count = ((outcomes.len() as f64 * tail_fraction).ceil() as usize).min(outcomes.len());
    let expected_shortfall =
        (tail_count > 0).then(|| outcomes[..tail_count].iter().sum::<f64>() / tail_count as f64);

    let mut transitions = OutcomeTransitions::default();
    let mut same = 0_u64;
    for pair in selected.windows(2) {
        let left = pair[0].outcome > 0.0;
        let right = pair[1].outcome > 0.0;
        if left == right {
            same += 1;
        }
        match (left, right) {
            (true, true) => transitions.win_to_win += 1,
            (true, false) => transitions.win_to_non_win += 1,
            (false, true) => transitions.non_win_to_win += 1,
            (false, false) => transitions.non_win_to_non_win += 1,
        }
    }
    let lag_one_same_outcome_rate =
        (selected.len() > 1).then(|| same as f64 / (selected.len() - 1) as f64);
    let mut calendar = BTreeMap::<(i32, u32), (usize, f64)>::new();
    for position in &selected {
        if let Some(timestamp) = chrono::DateTime::from_timestamp_millis(position.ordinal) {
            use chrono::Datelike;
            let entry = calendar
                .entry((timestamp.year(), timestamp.month()))
                .or_default();
            entry.0 += 1;
            entry.1 += position.outcome;
        }
    }
    let population = PersistedMetricPopulation {
        unit: PersistedPopulationUnit::CompletedPosition,

        filter: filter.clone(),
        provided_count: dataset.positions.len() as u64,
        eligible_count: selected.len() as u64,
        observed_count: outcomes.len() as u64,
        excluded_count: dataset.positions.len().saturating_sub(selected.len()) as u64,
        invalid_count: selected.len().saturating_sub(outcomes.len()) as u64,
    };
    Ok(AdditionalAnalytics {
        selected_positions: selected.len(),
        expected_shortfall: LocalMetric {
            value: expected_shortfall,
            status: if expected_shortfall.is_some() {
                LocalMetricStatus::Available
            } else {
                LocalMetricStatus::InsufficientData
            },
            population: population.clone(),
        },
        lag_one_same_outcome_rate: LocalMetric {
            value: lag_one_same_outcome_rate,
            status: if lag_one_same_outcome_rate.is_some() {
                LocalMetricStatus::Available
            } else {
                LocalMetricStatus::InsufficientData
            },
            population,
        },
        transitions,
        calendar: calendar
            .into_iter()
            .map(
                |((year, month), (observed_positions, total_outcome))| CalendarOutcomeBucket {
                    year,
                    month,
                    observed_positions,
                    total_outcome,
                },
            )
            .collect(),
    })
}

fn persisted_filter_matches(
    filter: &PersistedPositionFilter,
    position: &PersistedPositionOutcome,
) -> bool {
    let symbol = filter.symbols.is_empty() || filter.symbols.contains(&position.symbol);
    let side = filter.sides.is_empty() || filter.sides.contains(&position.side);
    let group = filter.groups.is_empty()
        || filter.groups.iter().any(|expected| match expected {
            PersistedGroupFilter::Named(name) => position.group.as_ref() == Some(name),
            PersistedGroupFilter::Ungrouped => position.group.is_none(),
        });
    let close_reason = filter.close_reasons.is_empty()
        || filter
            .close_reasons
            .iter()
            .any(|expected| position.close_reasons.contains(expected));
    let tags = filter.tags.iter().all(|(key, values)| {
        values.is_empty()
            || position
                .tags
                .get(key)
                .is_some_and(|actual| values.contains(actual))
    });
    symbol && side && group && close_reason && tags
}

fn options_from_msg(message: &ProviderEvaluationOptionsMsg) -> EvaluationOptions {
    EvaluationOptions {
        context: EvaluationContext {
            provider_id: message.context.provider_id.clone(),
            source_id: message.context.source_id.clone(),
        },
        source_coverage: message.source_coverage.map(|value| SourceCoverageCounts {
            raw_messages: value.raw_messages,
            parsed_messages: value.parsed_messages,
            skipped_messages: value.skipped_messages,
            failed_messages: value.failed_messages,
            emitted_signals: value.emitted_signals,
            emitted_entry_signals: value.emitted_entry_signals,
        }),
        sections: message
            .sections
            .iter()
            .map(|value| match value {
                EvaluationSectionMsg::Coverage => EvaluationSection::Coverage,
                EvaluationSectionMsg::PositionPerformance => EvaluationSection::PositionPerformance,
                EvaluationSectionMsg::RMetrics => EvaluationSection::RMetrics,
                EvaluationSectionMsg::Excursions => EvaluationSection::Excursions,
                EvaluationSectionMsg::Execution => EvaluationSection::Execution,
                EvaluationSectionMsg::Robustness => EvaluationSection::Robustness,
                EvaluationSectionMsg::Breakdowns => EvaluationSection::Breakdowns,
            })
            .collect(),
        filter: PositionFilter {
            symbols: message.filter.symbols.clone(),
            sides: message
                .filter
                .sides
                .iter()
                .map(|value| match value {
                    EvaluationPositionSideMsg::Long => PositionSide::Long,
                    EvaluationPositionSideMsg::Short => PositionSide::Short,
                })
                .collect(),
            groups: message
                .filter
                .groups
                .iter()
                .map(|value| match value {
                    EvaluationGroupFilterMsg::Named(name) => GroupFilter::Named(name.clone()),
                    EvaluationGroupFilterMsg::Ungrouped => GroupFilter::Ungrouped,
                })
                .collect(),
            close_reasons: message.filter.close_reasons.clone(),
            tags: message.filter.tags.clone(),
        },
        breakdowns: message
            .breakdowns
            .iter()
            .map(|value| match value {
                BreakdownDimensionMsg::Symbol => BreakdownDimension::Symbol,
                BreakdownDimensionMsg::Side => BreakdownDimension::Side,
                BreakdownDimensionMsg::Group => BreakdownDimension::Group,
                BreakdownDimensionMsg::CloseReason => BreakdownDimension::CloseReason,
                BreakdownDimensionMsg::Tag(tag) => BreakdownDimension::Tag(tag.clone()),
            })
            .collect(),
        bootstrap: BootstrapConfig {
            samples: message.bootstrap.samples,
            confidence_level: message.bootstrap.confidence_level,
            seed: message.bootstrap.seed,
            minimum_sample_size: message.bootstrap.minimum_sample_size,
        },
        rolling_window: message.rolling_window,
        minimum_breakdown_bucket_count: message.minimum_breakdown_bucket_count,
        maximum_breakdown_rows: message.maximum_breakdown_rows,
        include_position_rows: message.include_positions,
        maximum_position_rows: message.maximum_position_rows,
    }
}

fn decode_value<T: serde::de::DeserializeOwned>(
    value: &serde_json::Value,
    field: &'static str,
) -> Result<T, AnalysisError> {
    if value.is_null() {
        return Err(AnalysisError::MissingExecutionField { field });
    }
    serde_json::from_value(value.clone()).map_err(|error| AnalysisError::InvalidExecutionField {
        field,
        detail: error.to_string(),
    })
}

fn decode_serializable<T, S>(value: &S, field: &'static str) -> Result<T, AnalysisError>
where
    T: serde::de::DeserializeOwned,
    S: serde::Serialize,
{
    let value =
        serde_json::to_value(value).map_err(|error| AnalysisError::InvalidExecutionField {
            field,
            detail: error.to_string(),
        })?;
    serde_json::from_value(value).map_err(|error| AnalysisError::InvalidExecutionField {
        field,
        detail: error.to_string(),
    })
}

fn persist_position(position: &PositionOutcome) -> PersistedPositionOutcome {
    PersistedPositionOutcome {
        id: position.id.clone(),
        trade_id: position.trade_id.clone(),
        ordinal: position.ordinal,
        symbol: position.dimensions.symbol.clone(),
        side: persist_side(position.dimensions.side),
        group: position.dimensions.group.clone(),
        close_reasons: position.dimensions.close_reasons.clone(),
        tags: position.dimensions.tags.clone(),
        outcome: position.outcome,
        outcome_classification: position.outcome_classification.map(|value| match value {
            OutcomeClassification::Win => PersistedOutcomeClassification::Win,
            OutcomeClassification::Loss => PersistedOutcomeClassification::Loss,
            OutcomeClassification::Breakeven => PersistedOutcomeClassification::Breakeven,
        }),
        r_multiple: position.r_multiple,
        favorable_r: position.excursions.and_then(|value| value.favorable_r),
        adverse_r: position.excursions.and_then(|value| value.adverse_r),
        slippage_bps: position.execution.and_then(|value| value.slippage_bps),
        latency_ms: position.execution.and_then(|value| value.latency_ms),
        fill_ratio: position.execution.and_then(|value| value.fill_ratio),
    }
}

fn restore_position(position: &PersistedPositionOutcome) -> PositionOutcome {
    PositionOutcome {
        id: position.id.clone(),
        trade_id: position.trade_id.clone(),
        ordinal: position.ordinal,
        dimensions: PositionDimensions {
            symbol: position.symbol.clone(),
            side: restore_side(position.side),
            group: position.group.clone(),
            close_reasons: position.close_reasons.clone(),
            tags: position.tags.clone(),
        },
        outcome: position.outcome,
        outcome_classification: position.outcome_classification.map(|value| match value {
            PersistedOutcomeClassification::Win => OutcomeClassification::Win,
            PersistedOutcomeClassification::Loss => OutcomeClassification::Loss,
            PersistedOutcomeClassification::Breakeven => OutcomeClassification::Breakeven,
        }),
        r_multiple: position.r_multiple,
        excursions: (position.favorable_r.is_some() || position.adverse_r.is_some()).then_some(
            ExcursionInput {
                favorable_r: position.favorable_r,
                adverse_r: position.adverse_r,
            },
        ),
        execution: (position.slippage_bps.is_some()
            || position.latency_ms.is_some()
            || position.fill_ratio.is_some())
        .then_some(ExecutionDiagnosticsInput {
            slippage_bps: position.slippage_bps,
            latency_ms: position.latency_ms,
            fill_ratio: position.fill_ratio,
        }),
    }
}

fn persist_lifecycle(value: LifecycleCounts) -> PersistedLifecycleCounts {
    PersistedLifecycleCounts {
        candidates: value.candidates,
        accepted: value.accepted,
        opened: value.opened,
        completed: value.completed,
        rejected: value.rejected,
        filled: value.filled,
        cancelled: value.cancelled,
        unfilled_at_end: value.unfilled_at_end,
        open_at_end: value.open_at_end,
    }
}

fn restore_lifecycle(value: PersistedLifecycleCounts) -> LifecycleCounts {
    LifecycleCounts {
        candidates: value.candidates,
        accepted: value.accepted,
        opened: value.opened,
        completed: value.completed,
        rejected: value.rejected,
        filled: value.filled,
        cancelled: value.cancelled,
        unfilled_at_end: value.unfilled_at_end,
        open_at_end: value.open_at_end,
    }
}

fn persist_source_coverage(value: SourceCoverageCounts) -> PersistedSourceCoverageCounts {
    PersistedSourceCoverageCounts {
        raw_messages: value.raw_messages,
        parsed_messages: value.parsed_messages,
        skipped_messages: value.skipped_messages,
        failed_messages: value.failed_messages,
        emitted_signals: value.emitted_signals,
        emitted_entry_signals: value.emitted_entry_signals,
    }
}

fn restore_source_coverage(value: PersistedSourceCoverageCounts) -> SourceCoverageCounts {
    SourceCoverageCounts {
        raw_messages: value.raw_messages,
        parsed_messages: value.parsed_messages,
        skipped_messages: value.skipped_messages,
        failed_messages: value.failed_messages,
        emitted_signals: value.emitted_signals,
        emitted_entry_signals: value.emitted_entry_signals,
    }
}

fn persist_options(options: &EvaluationOptions) -> PersistedEvaluationOptions {
    PersistedEvaluationOptions {
        provider_id: options.context.provider_id.clone(),
        source_id: options.context.source_id.clone(),
        sections: options
            .sections
            .iter()
            .map(section_name)
            .map(str::to_owned)
            .collect(),
        filter: persist_filter(&options.filter),
        breakdowns: options.breakdowns.iter().map(breakdown_name).collect(),
        bootstrap_samples: options.bootstrap.samples,
        bootstrap_confidence_level: options.bootstrap.confidence_level,
        bootstrap_seed: options.bootstrap.seed,
        bootstrap_minimum_sample_size: options.bootstrap.minimum_sample_size,
        rolling_window: options.rolling_window,
        minimum_breakdown_bucket_count: options.minimum_breakdown_bucket_count,
        maximum_breakdown_rows: options.maximum_breakdown_rows,
    }
}

fn restore_options(
    options: &PersistedEvaluationOptions,
) -> Result<EvaluationOptions, AnalysisError> {
    Ok(EvaluationOptions {
        context: EvaluationContext {
            provider_id: options.provider_id.clone(),
            source_id: options.source_id.clone(),
        },
        source_coverage: None,
        sections: options
            .sections
            .iter()
            .map(|value| parse_section(value))
            .collect::<Result<BTreeSet<_>, _>>()?,
        filter: restore_filter(&options.filter),
        breakdowns: options
            .breakdowns
            .iter()
            .map(|value| parse_breakdown(value))
            .collect::<Result<_, _>>()?,
        bootstrap: BootstrapConfig {
            samples: options.bootstrap_samples,
            confidence_level: options.bootstrap_confidence_level,
            seed: options.bootstrap_seed,
            minimum_sample_size: options.bootstrap_minimum_sample_size,
        },
        rolling_window: options.rolling_window,
        minimum_breakdown_bucket_count: options.minimum_breakdown_bucket_count,
        maximum_breakdown_rows: options.maximum_breakdown_rows,
        include_position_rows: true,
        maximum_position_rows: None,
    })
}

fn persist_filter(filter: &PositionFilter) -> PersistedPositionFilter {
    PersistedPositionFilter {
        symbols: filter.symbols.clone(),
        sides: filter.sides.iter().copied().map(persist_side).collect(),
        groups: filter
            .groups
            .iter()
            .map(|group| match group {
                GroupFilter::Named(name) => PersistedGroupFilter::Named(name.clone()),
                GroupFilter::Ungrouped => PersistedGroupFilter::Ungrouped,
            })
            .collect(),
        close_reasons: filter.close_reasons.clone(),
        tags: filter.tags.clone(),
    }
}

fn restore_filter(filter: &PersistedPositionFilter) -> PositionFilter {
    PositionFilter {
        symbols: filter.symbols.clone(),
        sides: filter.sides.iter().copied().map(restore_side).collect(),
        groups: filter
            .groups
            .iter()
            .map(|group| match group {
                PersistedGroupFilter::Named(name) => GroupFilter::Named(name.clone()),
                PersistedGroupFilter::Ungrouped => GroupFilter::Ungrouped,
            })
            .collect(),
        close_reasons: filter.close_reasons.clone(),
        tags: filter.tags.clone(),
    }
}

fn persist_side(side: PositionSide) -> PersistedPositionSide {
    match side {
        PositionSide::Long => PersistedPositionSide::Long,
        PositionSide::Short => PersistedPositionSide::Short,
    }
}

fn restore_side(side: PersistedPositionSide) -> PositionSide {
    match side {
        PersistedPositionSide::Long => PositionSide::Long,
        PersistedPositionSide::Short => PositionSide::Short,
    }
}

fn section_name(section: &EvaluationSection) -> &'static str {
    match section {
        EvaluationSection::Coverage => "coverage",
        EvaluationSection::PositionPerformance => "position_performance",
        EvaluationSection::RMetrics => "r_metrics",
        EvaluationSection::Excursions => "excursions",
        EvaluationSection::Execution => "execution",
        EvaluationSection::Robustness => "robustness",
        EvaluationSection::Breakdowns => "breakdowns",
    }
}

fn parse_section(value: &str) -> Result<EvaluationSection, AnalysisError> {
    match value {
        "coverage" => Ok(EvaluationSection::Coverage),
        "position_performance" => Ok(EvaluationSection::PositionPerformance),
        "r_metrics" => Ok(EvaluationSection::RMetrics),
        "excursions" => Ok(EvaluationSection::Excursions),
        "execution" => Ok(EvaluationSection::Execution),
        "robustness" => Ok(EvaluationSection::Robustness),
        "breakdowns" => Ok(EvaluationSection::Breakdowns),
        _ => Err(AnalysisError::UnknownOption(value.into())),
    }
}

fn breakdown_name(value: &BreakdownDimension) -> String {
    match value {
        BreakdownDimension::Symbol => "symbol".into(),
        BreakdownDimension::Side => "side".into(),
        BreakdownDimension::Group => "group".into(),
        BreakdownDimension::CloseReason => "close_reason".into(),
        BreakdownDimension::Tag(tag) => format!("tag:{tag}"),
    }
}

fn parse_breakdown(value: &str) -> Result<BreakdownDimension, AnalysisError> {
    match value {
        "symbol" => Ok(BreakdownDimension::Symbol),
        "side" => Ok(BreakdownDimension::Side),
        "group" => Ok(BreakdownDimension::Group),
        "close_reason" => Ok(BreakdownDimension::CloseReason),
        _ if value.starts_with("tag:") => Ok(BreakdownDimension::Tag(value[4..].into())),
        _ => Err(AnalysisError::UnknownOption(value.into())),
    }
}

#[allow(clippy::too_many_arguments)]
fn persist_execution(
    fills: Vec<RecordedFill>,
    dispositions: Vec<ActionDisposition>,
    close_events: Vec<CloseEvent>,
    completed_positions: Vec<CompletedPosition>,
    open_positions: Vec<OpenPositionSnapshot>,
    pending_orders: Vec<PendingOrderSnapshot>,
    pending_lifecycle: Vec<PendingOrderLifecycleEvent>,
) -> PersistedExecutionDataset {
    let mut conversion_audits = Vec::new();
    let mut risk_tranches = Vec::new();
    for event in &close_events {
        if let Some(conversion) = event.pnl_conversion.as_ref() {
            conversion_audits.push(persist_conversion(
                "close_pnl",
                &event.position_id,
                conversion,
            ));
        }
    }
    for position in &completed_positions {
        for tranche in &position.risk_tranches {
            risk_tranches.push(persist_risk_tranche(&position.position_id, tranche));
            if let Some(conversion) = tranche.risk_conversion.as_ref() {
                conversion_audits.push(persist_conversion(
                    "initial_risk",
                    &position.position_id,
                    conversion,
                ));
            }
        }
    }
    for position in &open_positions {
        for (context, conversion) in [
            (
                "unrealized_pnl",
                position.unrealized_pnl_conversion.as_ref(),
            ),
            (
                "gross_exposure",
                position.gross_exposure_conversion.as_ref(),
            ),
            ("open_risk", position.open_risk_conversion.as_ref()),
        ] {
            if let Some(conversion) = conversion {
                conversion_audits.push(persist_conversion(
                    context,
                    &position.position_id,
                    conversion,
                ));
            }
        }
    }
    let risk_tranche_completeness = PersistedCollectionCompleteness {
        available: true,
        source_count: risk_tranches.len().saturating_add(open_positions.len()),
        included_count: risk_tranches.len(),
        truncated: !open_positions.is_empty(),
    };
    let completeness = PersistedExecutionCompleteness {
        fills: PersistedCollectionCompleteness::complete(fills.len()),
        action_dispositions: PersistedCollectionCompleteness::complete(dispositions.len()),
        close_events: PersistedCollectionCompleteness::complete(close_events.len()),
        completed_positions: PersistedCollectionCompleteness::complete(completed_positions.len()),
        open_positions: PersistedCollectionCompleteness::complete(open_positions.len()),
        pending_orders: PersistedCollectionCompleteness::complete(pending_orders.len()),
        pending_lifecycle: PersistedCollectionCompleteness::complete(pending_lifecycle.len()),
        risk_tranches: risk_tranche_completeness,
        conversion_audits: PersistedCollectionCompleteness::complete(conversion_audits.len()),
    };
    PersistedExecutionDataset {
        format_version: EXECUTION_DATASET_FORMAT_VERSION,
        fills: fills.iter().map(persist_fill).collect(),
        action_dispositions: dispositions.iter().map(persist_disposition).collect(),
        close_events: close_events.iter().map(persist_close_event).collect(),
        completed_positions: completed_positions.iter().map(persist_completed).collect(),
        open_positions: open_positions.iter().map(persist_open).collect(),
        pending_orders: pending_orders.iter().map(persist_pending).collect(),
        pending_lifecycle: pending_lifecycle
            .iter()
            .map(persist_pending_event)
            .collect(),
        risk_tranches,
        conversion_audits,
        completeness,
    }
}

fn persist_trade_side(value: qs_backtest::Side) -> PersistedTradeSide {
    match value {
        qs_backtest::Side::Buy => PersistedTradeSide::Buy,
        qs_backtest::Side::Sell => PersistedTradeSide::Sell,
    }
}

fn persist_order_type(value: qs_backtest::OrderType) -> PersistedOrderType {
    match value {
        qs_backtest::OrderType::Market => PersistedOrderType::Market,
        qs_backtest::OrderType::Limit => PersistedOrderType::Limit,
        qs_backtest::OrderType::Stop => PersistedOrderType::Stop,
    }
}

fn persist_fill_purpose(value: qs_backtest::FillPurpose) -> PersistedFillPurpose {
    match value {
        qs_backtest::FillPurpose::MarketEntry => PersistedFillPurpose::MarketEntry,
        qs_backtest::FillPurpose::MarketExit => PersistedFillPurpose::MarketExit,
        qs_backtest::FillPurpose::LimitEntry => PersistedFillPurpose::LimitEntry,
        qs_backtest::FillPurpose::StopEntry => PersistedFillPurpose::StopEntry,
        qs_backtest::FillPurpose::StopLoss => PersistedFillPurpose::StopLoss,
        qs_backtest::FillPurpose::TakeProfit => PersistedFillPurpose::TakeProfit,
    }
}

fn persist_disposition_status(
    value: qs_backtest::ledger::ActionDispositionStatus,
) -> PersistedDispositionStatus {
    match value {
        qs_backtest::ledger::ActionDispositionStatus::Applied => {
            PersistedDispositionStatus::Applied
        }
        qs_backtest::ledger::ActionDispositionStatus::Skipped => {
            PersistedDispositionStatus::Skipped
        }
        qs_backtest::ledger::ActionDispositionStatus::Rejected => {
            PersistedDispositionStatus::Rejected
        }
        qs_backtest::ledger::ActionDispositionStatus::Failed => PersistedDispositionStatus::Failed,
    }
}

fn persist_close_reason(value: qs_backtest::CloseReason) -> PersistedCloseReason {
    match value {
        qs_backtest::CloseReason::Stoploss => PersistedCloseReason::Stoploss,
        qs_backtest::CloseReason::Target => PersistedCloseReason::Target,
        qs_backtest::CloseReason::TrailingStop => PersistedCloseReason::TrailingStop,
        qs_backtest::CloseReason::TimeExit => PersistedCloseReason::TimeExit,
        qs_backtest::CloseReason::BreakevenStop => PersistedCloseReason::BreakevenStop,
        qs_backtest::CloseReason::Manual => PersistedCloseReason::Manual,
        qs_backtest::CloseReason::EndOfData => PersistedCloseReason::EndOfData,
        qs_backtest::CloseReason::GroupRule => PersistedCloseReason::GroupRule,
        qs_backtest::CloseReason::Cancelled => PersistedCloseReason::Cancelled,
    }
}

fn persist_net_pnl_outcome(value: qs_backtest::NetPnlOutcome) -> PersistedNetPnlOutcome {
    match value {
        qs_backtest::NetPnlOutcome::Win => PersistedNetPnlOutcome::Win,
        qs_backtest::NetPnlOutcome::Loss => PersistedNetPnlOutcome::Loss,
        qs_backtest::NetPnlOutcome::Breakeven => PersistedNetPnlOutcome::Breakeven,
    }
}

fn persist_risk_status(value: qs_backtest::RiskBasisStatus) -> PersistedRiskBasisStatus {
    match value {
        qs_backtest::RiskBasisStatus::Available => PersistedRiskBasisStatus::Available,
        qs_backtest::RiskBasisStatus::Partial => PersistedRiskBasisStatus::Partial,
        qs_backtest::RiskBasisStatus::MissingStop => PersistedRiskBasisStatus::MissingStop,
        qs_backtest::RiskBasisStatus::InvalidInput => PersistedRiskBasisStatus::InvalidInput,
        qs_backtest::RiskBasisStatus::NonProtectiveStop => {
            PersistedRiskBasisStatus::NonProtectiveStop
        }
        qs_backtest::RiskBasisStatus::ZeroRisk => PersistedRiskBasisStatus::ZeroRisk,
    }
}

fn persist_pending_state(
    value: qs_backtest::PendingOrderLifecycleState,
) -> PersistedPendingLifecycleState {
    match value {
        qs_backtest::PendingOrderLifecycleState::Placed => PersistedPendingLifecycleState::Placed,
        qs_backtest::PendingOrderLifecycleState::Filled => PersistedPendingLifecycleState::Filled,
        qs_backtest::PendingOrderLifecycleState::Cancelled => {
            PersistedPendingLifecycleState::Cancelled
        }
        qs_backtest::PendingOrderLifecycleState::UnfilledAtEnd => {
            PersistedPendingLifecycleState::UnfilledAtEnd
        }
    }
}

fn persist_fx_pair(value: &qs_backtest::FxPair) -> PersistedFxPair {
    PersistedFxPair {
        symbol: value.symbol.clone(),
        base_currency: value.base_currency.clone(),
        quote_currency: value.quote_currency.clone(),
    }
}

fn persist_fx_direction(value: qs_backtest::FxPairDirection) -> PersistedFxPairDirection {
    match value {
        qs_backtest::FxPairDirection::Direct => PersistedFxPairDirection::Direct,
        qs_backtest::FxPairDirection::Inverse => PersistedFxPairDirection::Inverse,
    }
}

fn persist_route_leg(value: &qs_backtest::ConversionLeg) -> PersistedConversionRouteLeg {
    PersistedConversionRouteLeg {
        pair: persist_fx_pair(&value.pair),
        direction: persist_fx_direction(value.direction),
    }
}

fn persist_conversion_route(value: &qs_backtest::ConversionRoute) -> PersistedConversionRoute {
    match value {
        qs_backtest::ConversionRoute::Identity { currency } => PersistedConversionRoute::Identity {
            currency: currency.clone(),
        },
        qs_backtest::ConversionRoute::Direct { pair } => PersistedConversionRoute::Direct {
            pair: persist_fx_pair(pair),
        },
        qs_backtest::ConversionRoute::Inverse { pair } => PersistedConversionRoute::Inverse {
            pair: persist_fx_pair(pair),
        },
        qs_backtest::ConversionRoute::TwoLeg {
            pivot_currency,
            first,
            second,
        } => PersistedConversionRoute::TwoLeg {
            pivot_currency: pivot_currency.clone(),
            first: persist_route_leg(first),
            second: persist_route_leg(second),
        },
    }
}

fn persist_conversion_price_side(
    value: qs_backtest::ConversionPriceSide,
) -> PersistedConversionPriceSide {
    match value {
        qs_backtest::ConversionPriceSide::Bid => PersistedConversionPriceSide::Bid,
        qs_backtest::ConversionPriceSide::Ask => PersistedConversionPriceSide::Ask,
    }
}

fn persist_fill(value: &RecordedFill) -> PersistedFill {
    PersistedFill {
        id: value.id.clone(),
        action_id: value.action_id.clone(),
        position_id: value.position_id.clone(),
        symbol: value.symbol.clone(),
        signal_ts: value.signal_ts.map(|value| value.to_string()),
        effective_ts: value.effective_ts.to_string(),
        execution_ts: value.execution_ts.map(|value| value.to_string()),
        quote_ts: value.quote_ts.to_string(),
        quote_age_millis: value.quote_age_millis,
        size: value.size,
        bid: value.bid,
        ask: value.ask,
        purpose: persist_fill_purpose(value.fill.purpose),
        side: persist_trade_side(value.fill.side),
        price: value.fill.price,
        quote_price: value.fill.quote_price,
        requested_price: value.fill.requested_price,
        slippage_pips: value.fill.slippage_pips,
    }
}

fn persist_disposition(value: &ActionDisposition) -> PersistedActionDisposition {
    PersistedActionDisposition {
        action_id: value.action_id.clone(),
        action_kind: value.action_kind.clone(),
        signal_ts: value.signal_ts.map(|value| value.to_string()),
        effective_ts: value.effective_ts.map(|value| value.to_string()),
        status: persist_disposition_status(value.status),
        reason: value.reason.clone(),
        position_ids: value.position_ids.clone(),
    }
}

fn persist_close_event(value: &CloseEvent) -> PersistedCloseEvent {
    PersistedCloseEvent {
        id: value.id.clone(),
        action_id: value.action_id.clone(),
        fill_id: value.fill_id.clone(),
        position_id: value.position_id.clone(),
        symbol: value.symbol.clone(),
        side: persist_trade_side(value.side),
        ts: value.ts.to_string(),
        size: value.size,
        price: value.price,
        entry_price: value.entry_price,
        pnl: value.pnl,
        native_pnl: value.native_pnl,
        native_currency: value.native_currency.clone(),
        reason: persist_close_reason(value.reason),
        remaining_size: value.remaining_size,
    }
}

fn persist_completed(value: &CompletedPosition) -> PersistedCompletedPosition {
    PersistedCompletedPosition {
        position_id: value.position_id.clone(),
        symbol: value.symbol.clone(),
        side: persist_trade_side(value.side),
        group: value.group.clone(),
        trade_id: value.trade_id.clone(),
        open_ts: value.open_ts.to_string(),
        close_ts: value.close_ts.to_string(),
        entry_size: value.entry_size,
        average_entry_price: value.average_entry_price,
        net_pnl: value.net_pnl,
        native_net_pnl: value.native_net_pnl,
        native_currency: value.native_currency.clone(),
        outcome: persist_net_pnl_outcome(value.outcome),
        initial_stop: value.initial_stop,
        effective_stop: value.effective_stop.map(persist_effective_stop),
        risk_basis_status: persist_risk_status(value.risk_basis_status),
        realized_r: value.realized_r,
        mae: value.mae,
        mfe: value.mfe,
        close_reasons: value
            .close_reasons
            .iter()
            .copied()
            .map(persist_close_reason)
            .collect(),
    }
}

fn persist_open(value: &OpenPositionSnapshot) -> PersistedOpenPosition {
    PersistedOpenPosition {
        position_id: value.position_id.clone(),
        symbol: value.symbol.clone(),
        side: persist_trade_side(value.side),
        group: value.group.clone(),
        trade_id: value.trade_id.clone(),
        open_ts: value.open_ts.map(|value| value.to_string()),
        average_entry_price: value.average_entry_price,
        remaining_size: value.remaining_size,
        initial_stop: value.initial_stop,
        effective_stop: value.effective_stop.map(persist_effective_stop),
        realized_pnl: value.realized_pnl,
        unrealized_pnl: value.unrealized_pnl,
        gross_exposure: value.gross_exposure,
        open_risk: value.open_risk,
        campaign_mae: value.campaign_mae,
        campaign_mfe: value.campaign_mfe,
    }
}

fn persist_effective_stop(value: qs_backtest::EffectiveStop) -> PersistedEffectiveStop {
    PersistedEffectiveStop {
        price: value.price,
        origin: match value.origin {
            qs_backtest::StopOrigin::Initial => PersistedStopOrigin::Initial,
            qs_backtest::StopOrigin::Modified => PersistedStopOrigin::Modified,
            qs_backtest::StopOrigin::Breakeven => PersistedStopOrigin::Breakeven,
            qs_backtest::StopOrigin::Trailing => PersistedStopOrigin::Trailing,
        },
    }
}

fn persist_pending(value: &PendingOrderSnapshot) -> PersistedPendingOrder {
    PersistedPendingOrder {
        position_id: value.position_id.clone(),
        action_id: value.action_id.clone(),
        symbol: value.symbol.clone(),
        side: persist_trade_side(value.side),
        order_type: persist_order_type(value.order_type),
        requested_price: value.requested_price,
        size: value.size,
        signal_ts: value.signal_ts.map(|value| value.to_string()),
        effective_ts: value.effective_ts.map(|value| value.to_string()),
        initial_stop: value.initial_stop,
        group: value.group.clone(),
        trade_id: value.trade_id.clone(),
    }
}

fn persist_pending_event(value: &PendingOrderLifecycleEvent) -> PersistedPendingLifecycleEvent {
    PersistedPendingLifecycleEvent {
        id: value.id.clone(),
        sequence: value.sequence,
        position_id: value.position_id.clone(),
        placement_action_id: value.placement_action_id.clone(),
        terminal_action_id: value.terminal_action_id.clone(),
        state: persist_pending_state(value.state),
        symbol: value.symbol.clone(),
        side: persist_trade_side(value.side),
        order_type: persist_order_type(value.order_type),
        requested_size: value.requested_size,
        filled_size: value.filled_size,
        requested_price: value.requested_price,
        fill_price: value.fill_price,
        signal_ts: value.signal_ts.map(|value| value.to_string()),
        placed_ts: value.placed_ts.map(|value| value.to_string()),
        effective_ts: value.effective_ts.map(|value| value.to_string()),
        terminal_ts: value.terminal_ts.map(|value| value.to_string()),
        wait_latency_ms: value.wait_latency_ms,
        fill_ratio: value.fill_ratio,
    }
}

fn persist_risk_tranche(position_id: &str, value: &RiskTranche) -> PersistedRiskTranche {
    PersistedRiskTranche {
        position_id: position_id.into(),
        fill_id: value.fill_id.clone(),
        size: value.size,
        entry_price: value.entry_price,
        initial_stop: value.initial_stop,
        contract_size: value.contract_size,
        risk_per_unit: value.risk_per_unit,
        risk_amount: value.risk_amount,
        native_risk_amount: value.native_risk_amount,
        native_currency: value.native_currency.clone(),
        status: persist_risk_status(value.status),
    }
}

fn persist_conversion(
    context: &str,
    position_id: &str,
    value: &ConversionResult,
) -> PersistedConversionAudit {
    PersistedConversionAudit {
        context: context.into(),
        position_id: position_id.into(),
        from_currency: value.from_currency.clone(),
        to_currency: value.to_currency.clone(),
        input_amount: value.input_amount,
        output_amount: value.output_amount,
        operation_ts: value.operation_ts.to_string(),
        route: persist_conversion_route(&value.route),
        legs: value
            .legs
            .iter()
            .map(|leg| PersistedConversionLeg {
                sequence: leg.sequence,
                symbol: leg.symbol.clone(),
                direction: persist_fx_direction(leg.direction),
                from_currency: leg.from_currency.clone(),
                to_currency: leg.to_currency.clone(),
                input_amount: leg.input_amount,
                output_amount: leg.output_amount,
                quote_ts: leg.quote_ts.to_string(),
                quote_age_millis: leg.quote_age_millis,
                bid: leg.bid,
                ask: leg.ask,
                price_side: persist_conversion_price_side(leg.price_side),
                executable_price: leg.executable_price,
                conversion_rate: leg.conversion_rate,
            })
            .collect(),
    }
}

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("unsupported FutureQuote result version {actual}")]
    UnsupportedFutureVersion { actual: u32 },
    #[error("unsupported persisted analysis version {actual}")]
    UnsupportedAnalysisVersion { actual: u32 },
    #[error("full execution result omitted required field '{field}'")]
    MissingExecutionField { field: &'static str },
    #[error("invalid execution field '{field}': {detail}")]
    InvalidExecutionField { field: &'static str, detail: String },
    #[error("invalid execution lifecycle: {0}")]
    InvalidExecution(String),
    #[error("unknown persisted analysis option '{0}'")]
    UnknownOption(String),
    #[error("analysis tail fraction must be finite and in (0, 1]")]
    InvalidTailFraction,
    #[error("analysis was cancelled")]
    Cancelled,
    #[error("analysis generation is stale")]
    StaleGeneration,
    #[error("background analysis task failed: {0}")]
    Task(String),
}
