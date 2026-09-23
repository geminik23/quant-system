use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_backtest::data_feed::{FeedEvent, MarketEvent};
use qs_backtest::evaluation::{
    EvaluationOptions, EvaluationReport, EvaluationRequest, PositionOutcome,
};
use qs_backtest::report::BacktestResult;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BacktestConfiguredStrategyAdapter, BacktestRunner,
    ObservationStoreLimits, StrategyDescriptor, StrategyId, VecFeed,
};
use qs_core::CloseReason;
use qs_strategy::{ConfiguredStrategy, ParameterBinding, StrategyConfig, parameter_value_label};

use crate::error::{ResearchError, RunFailure};
use crate::family::StrategyFamily;
use crate::plan::ResearchPlan;
use crate::table::{ResearchRow, ResearchTable, RunStatus};
use crate::window::DataWindow;

/// Data mode recorded for a symbol that supplied no primary events.
const DEFAULT_DATA_MODE: &str = "ticks";
const RESERVED_TAGS: [&str; 3] = ["window", "symbol", "data_mode"];

pub type SymbolEvents = Arc<[FeedEvent]>;

struct RunSpec<'a, P> {
    run_index: usize,
    point: &'a P,
    point_index: usize,
    symbol: &'a str,
    window: &'a DataWindow,
    data_mode: &'static str,
}

struct RunRecord {
    run_index: usize,
    row: ResearchRow,
    positions: Vec<PositionOutcome>,
}

/// All retained outcomes and projections produced by one parameter search.
#[derive(Debug, Clone, PartialEq)]
pub struct ResearchBatch {
    table: ResearchTable,
    positions: Vec<PositionOutcome>,
    bound_documents: Vec<StrategyConfig>,
}

impl ResearchBatch {
    pub fn table(&self) -> &ResearchTable {
        &self.table
    }

    pub fn evaluate(&self, options: EvaluationOptions) -> EvaluationReport {
        qs_backtest::evaluation::evaluate(&EvaluationRequest {
            positions: self.positions.clone(),
            lifecycle: None,
            options,
        })
    }

    pub fn bound_document(&self, point: usize) -> Option<&StrategyConfig> {
        self.bound_documents.get(point)
    }

    pub fn position_outcomes(&self) -> &[PositionOutcome] {
        &self.positions
    }
}

impl Deref for ResearchBatch {
    type Target = ResearchTable;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

pub fn run_batch<F>(
    plan: &ResearchPlan,
    family: &F,
    events: &BTreeMap<String, SymbolEvents>,
) -> Result<ResearchBatch, ResearchError>
where
    F: StrategyFamily,
{
    plan.validate()?;
    let pairs = plan.window_plan.pairs()?;
    if pairs.is_empty() {
        return Err(ResearchError::InvalidWindow(
            "the window plan produced no evaluation periods".into(),
        ));
    }
    let points = family.points();
    if points.is_empty() {
        return Err(ResearchError::InvalidPlan(
            "the family produced no parameter points".into(),
        ));
    }
    let points_total = points.len();
    let bound_documents = points.iter().map(|point| family.config(point)).collect();

    let bindings: Vec<ParameterBinding> = points
        .iter()
        .map(|point| family.parameter_binding(point))
        .collect();
    validate_parameter_labels(family.family_id(), &bindings, &plan.config.run_tags)?;

    let mut data_modes = BTreeMap::new();
    for symbol in &plan.symbols {
        let Some(symbol_events) = events.get(symbol.as_str()) else {
            continue;
        };
        if symbol_events
            .windows(2)
            .any(|pair| pair[0].event.ts() > pair[1].event.ts())
        {
            return Err(ResearchError::InvalidPlan(format!(
                "events for '{symbol}' are not in ascending timestamp order"
            )));
        }
        data_modes.insert(symbol.as_str(), data_mode(symbol, symbol_events)?);
    }

    let mut specs = Vec::new();
    for symbol in &plan.symbols {
        for pair in &pairs {
            for window in [&pair.in_sample, &pair.out_of_sample] {
                for (point_index, point) in points.iter().enumerate() {
                    specs.push(RunSpec {
                        run_index: specs.len(),
                        point,
                        point_index,
                        symbol: symbol.as_str(),
                        window,
                        data_mode: data_modes
                            .get(symbol.as_str())
                            .copied()
                            .unwrap_or(DEFAULT_DATA_MODE),
                    });
                }
            }
        }
    }

    let workers = plan.workers.min(specs.len()).max(1);
    let mut records = if workers == 1 {
        specs
            .iter()
            .map(|spec| evaluate_run(plan, family, spec, events, points_total))
            .collect()
    } else {
        run_in_parallel(plan, family, &specs, events, points_total, workers)
    };
    records.sort_by_key(|record| record.run_index);

    let mut positions = Vec::new();
    let mut rows = Vec::with_capacity(records.len());
    for mut record in records {
        for position in &mut record.positions {
            position.id = format!("run:{}|{}", record.run_index, position.id);
            if let Some(trade_id) = position.trade_id.as_mut() {
                *trade_id = format!("run:{}|{trade_id}", record.run_index);
            }
        }
        positions.extend(record.positions);
        rows.push(record.row);
    }

    Ok(ResearchBatch {
        table: ResearchTable::new(rows),
        positions,
        bound_documents,
    })
}

fn validate_parameter_labels(
    family_id: &str,
    bindings: &[ParameterBinding],
    common_tags: &BTreeMap<String, String>,
) -> Result<(), ResearchError> {
    let mut labels = Vec::with_capacity(bindings.len());
    for binding in bindings {
        let label = binding_labels(binding);
        for key in label.keys().chain(common_tags.keys()) {
            if RESERVED_TAGS.contains(&key.as_str()) {
                return Err(ResearchError::InvalidPlan(format!(
                    "run tag '{key}' is reserved by the research runner"
                )));
            }
        }
        let total = common_tags.len() + label.len() + RESERVED_TAGS.len();
        if total > qs_backtest::runner::MAX_RUN_TAGS {
            return Err(ResearchError::InvalidPlan(format!(
                "composed run tags require {total} entries, exceeding {}",
                qs_backtest::runner::MAX_RUN_TAGS
            )));
        }
        labels.push(label);
    }
    labels.sort();
    let distinct = labels.len();
    labels.dedup();
    if labels.len() != distinct {
        return Err(ResearchError::InvalidPlan(format!(
            "family '{family_id}' binds two parameter points identically"
        )));
    }
    Ok(())
}

fn binding_labels(binding: &ParameterBinding) -> BTreeMap<String, String> {
    binding
        .iter()
        .map(|(name, value)| (name.clone(), parameter_value_label(value)))
        .collect()
}

fn run_in_parallel<F>(
    plan: &ResearchPlan,
    family: &F,
    specs: &[RunSpec<'_, F::Params>],
    events: &BTreeMap<String, SymbolEvents>,
    points_total: usize,
    workers: usize,
) -> Vec<RunRecord>
where
    F: StrategyFamily,
{
    let next = std::sync::atomic::AtomicUsize::new(0);
    let collected = std::sync::Mutex::new(Vec::with_capacity(specs.len()));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(spec) = specs.get(index) else {
                        break;
                    };
                    let record = evaluate_run(plan, family, spec, events, points_total);
                    collected
                        .lock()
                        .expect("a worker panicked while holding the result lock")
                        .push(record);
                }
            });
        }
    });

    collected
        .into_inner()
        .expect("every worker has finished by now")
}

fn evaluate_run<F>(
    plan: &ResearchPlan,
    family: &F,
    spec: &RunSpec<'_, F::Params>,
    events: &BTreeMap<String, SymbolEvents>,
    points_total: usize,
) -> RunRecord
where
    F: StrategyFamily,
{
    let binding = family.parameter_binding(spec.point);
    let params = binding_labels(&binding);
    let tags = run_tags(plan, &params, spec);
    let make_row = |status: RunStatus| ResearchRow {
        family_id: family.family_id().to_owned(),
        symbol: spec.symbol.to_owned(),
        params: params.clone(),
        window: spec.window.label().to_owned(),
        status,
        data_mode: spec.data_mode.to_owned(),
        positions: 0,
        win_rate: None,
        net_pnl: 0.0,
        gross_pnl: None,
        commission: 0.0,
        swap: 0.0,
        expectancy_r: None,
        r_p05: None,
        r_p50: None,
        r_p95: None,
        avg_favorable_r: None,
        avg_adverse_r: None,
        max_drawdown_pct: 0.0,
        forced_closes: 0,
        entries_before_window: 0,
        points_total,
    };

    let Some(symbol_events) = events.get(spec.symbol) else {
        return RunRecord {
            run_index: spec.run_index,
            row: make_row(
                RunFailure::Replay(format!("no market data supplied for '{}'", spec.symbol)).into(),
            ),
            positions: Vec::new(),
        };
    };

    match run_one(plan, family, spec, symbol_events, &tags) {
        Ok(result) => {
            let mut row = make_row(RunStatus::Completed);
            fill_metrics(&mut row, &result, spec.window);
            RunRecord {
                run_index: spec.run_index,
                row,
                positions: result.provider_positions,
            }
        }
        Err(failure) => RunRecord {
            run_index: spec.run_index,
            row: make_row(failure.into()),
            positions: Vec::new(),
        },
    }
}

/// Name how a symbol's primary events were recorded, because a table built from stored bars and one built from ticks do not mean the same thing.
fn data_mode(symbol: &str, events: &SymbolEvents) -> Result<&'static str, ResearchError> {
    let mut ticks = false;
    let mut bars = false;
    for event in events.iter().filter(|event| event.metadata.roles.primary) {
        match event.event {
            MarketEvent::Tick { .. } => ticks = true,
            MarketEvent::Bar { .. } => bars = true,
        }
    }
    match (ticks, bars) {
        (true, true) => Err(ResearchError::InvalidPlan(format!(
            "events for '{symbol}' mix ticks and stored bars"
        ))),
        (false, true) => Ok("bars"),
        _ => Ok(DEFAULT_DATA_MODE),
    }
}

fn run_tags<P>(
    plan: &ResearchPlan,
    params: &BTreeMap<String, String>,
    spec: &RunSpec<'_, P>,
) -> BTreeMap<String, String> {
    let mut tags = plan.config.run_tags.clone();
    tags.extend(params.clone());
    tags.insert("window".into(), spec.window.label().to_owned());
    tags.insert("symbol".into(), spec.symbol.to_owned());
    tags.insert("data_mode".into(), spec.data_mode.to_owned());
    tags
}

fn run_one<F>(
    plan: &ResearchPlan,
    family: &F,
    spec: &RunSpec<'_, F::Params>,
    events: &SymbolEvents,
    tags: &BTreeMap<String, String>,
) -> Result<BacktestResult, RunFailure>
where
    F: StrategyFamily,
{
    let instance_id = format!("p{}", spec.point_index);
    let config_document = family.config(spec.point);
    let strategy_id = config_document.strategy_id.clone();
    let strategy = ConfiguredStrategy::compile(
        config_document,
        &family.library(),
        instance_id.clone(),
        spec.symbol,
    )
    .map_err(|error| RunFailure::Compile(error.to_string()))?;

    let bindings = family
        .bindings(spec.symbol, spec.point, strategy.input_requirements())
        .map_err(RunFailure::Bind)?;
    let warmup_start = warmup_start(&bindings, spec.window.from())?;
    let descriptor = StrategyDescriptor::new(
        StrategyId::new(strategy_id.clone())
            .map_err(|error| RunFailure::Compile(error.to_string()))?,
        instance_id,
        strategy_id,
    )
    .map_err(|error| RunFailure::Compile(error.to_string()))?;

    let mut adapter = BacktestConfiguredStrategyAdapter::new(
        strategy,
        descriptor,
        bindings,
        plan.decision_latency_ms,
    )
    .map_err(|error| RunFailure::Bind(error.to_string()))?;

    let slice = slice_events(events, spec.window, warmup_start);
    let mut feed = VecFeed::from_feed_events(slice);

    let mut config = plan.config.clone();
    config.run_tags = tags.clone();
    config.close_on_finish = true;

    let analysis = AnalysisPipeline::new(
        Vec::new(),
        ObservationStoreLimits::default(),
        AnnotationLimits::default(),
    )
    .map_err(|error| RunFailure::Bind(error.to_string()))?;

    BacktestRunner::new_future(config, plan.future.clone())
        .with_evaluation_options(plan.evaluation.clone())
        .run_configured_strategy_future(&mut feed, &mut adapter, analysis, plan.retention, None)
        .map(|result| result.replay)
        .map_err(|error| RunFailure::Replay(error.to_string()))
}

fn warmup_start(
    bindings: &qs_backtest::ConfiguredHistoricalBindings,
    from: NaiveDateTime,
) -> Result<NaiveDateTime, RunFailure> {
    let from_seconds = from.and_utc().timestamp();
    let mut start = from;
    for binding in bindings.sources() {
        let series = binding.series();
        let requirement = series.requirement();
        let bars = i64::try_from(requirement.warmup().required_bars())
            .map_err(|_| RunFailure::Bind("warmup bars do not fit i64".into()))?;
        if bars == 0 {
            continue;
        }
        let duration = i64::try_from(requirement.timeframe().duration_seconds())
            .map_err(|_| RunFailure::Bind("timeframe duration does not fit i64".into()))?;
        let offset = series.alignment_offset_seconds();
        let bucket_open = (from_seconds - offset).div_euclid(duration) * duration + offset;
        let preceding_intervals = if bucket_open == from_seconds {
            bars
        } else {
            bars - 1
        };
        let source_start = bucket_open
            .checked_sub(
                duration
                    .checked_mul(preceding_intervals)
                    .ok_or_else(|| RunFailure::Bind("warmup span overflowed".into()))?,
            )
            .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
            .map(|value| value.naive_utc())
            .ok_or_else(|| RunFailure::Bind("warmup start is outside timestamp bounds".into()))?;
        start = start.min(source_start);
    }
    Ok(start)
}

fn slice_events(
    events: &SymbolEvents,
    window: &DataWindow,
    warmup_start: NaiveDateTime,
) -> Vec<FeedEvent> {
    let start = warmup_start;
    let end = window.to();
    let lower = partition_point(events, |ts| ts < start);
    let upper = partition_point(events, |ts| ts < end);
    events[lower..upper].to_vec()
}

fn partition_point(
    events: &SymbolEvents,
    mut predicate: impl FnMut(NaiveDateTime) -> bool,
) -> usize {
    let mut low = 0usize;
    let mut high = events.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if predicate(events[mid].event.ts()) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

fn fill_metrics(row: &mut ResearchRow, result: &BacktestResult, window: &DataWindow) {
    row.net_pnl = result.total_pnl;
    row.gross_pnl = result.gross_pnl;
    row.commission = result.total_commission;
    row.swap = result.total_swap;
    row.max_drawdown_pct = result.max_drawdown_pct;
    row.positions = result.completed_positions.len();
    row.forced_closes = result
        .completed_positions
        .iter()
        .filter(|position| position.close_reasons.contains(&CloseReason::EndOfData))
        .count();
    row.entries_before_window = result
        .completed_positions
        .iter()
        .filter(|position| position.open_ts < window.from())
        .count();

    if let Some(evaluation) = result.provider_evaluation.as_ref() {
        if let Some(performance) = evaluation.position_performance.as_ref() {
            row.win_rate = performance.win_rate.value;
        }
        if let Some(r_metrics) = evaluation.r_metrics.as_ref() {
            row.expectancy_r = r_metrics.mean_r.value;
            if let Some(quantiles) = r_metrics.quantiles.value.as_ref() {
                row.r_p05 = Some(quantiles.p05);
                row.r_p50 = Some(quantiles.p50);
                row.r_p95 = Some(quantiles.p95);
            }
        }
        if let Some(excursions) = evaluation.excursions.as_ref() {
            row.avg_favorable_r = excursions.mean_favorable_r.value;
            row.avg_adverse_r = excursions.mean_adverse_r.value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::EmaCrossFamily;
    use crate::window::DataWindow;
    use chrono::{Duration, NaiveDate};
    use qs_backtest::Timeframe;
    use qs_backtest::runner::BacktestConfig;

    fn ts(minute: i64) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 5)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            + Duration::minutes(minute)
    }

    #[test]
    fn a_run_is_labelled_with_the_batch_the_point_and_the_window() {
        let mut config = BacktestConfig::default();
        config.run_tags.insert("dataset".into(), "synthetic".into());
        let plan = crate::plan::ResearchPlan::new(
            vec!["EURUSD".into()],
            crate::window::WindowPlan::Fixed {
                in_sample: DataWindow::new("is", ts(0), ts(100)).unwrap(),
                out_of_sample: DataWindow::new("oos", ts(100), ts(200)).unwrap(),
            },
            config,
        );
        let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap());
        let point = family.points()[0];
        let window = DataWindow::new("is", ts(0), ts(100)).unwrap();
        let spec = RunSpec {
            run_index: 0,
            point: &point,
            point_index: 0,
            symbol: "EURUSD",
            window: &window,
            data_mode: DEFAULT_DATA_MODE,
        };
        let params = binding_labels(&family.parameter_binding(&point));

        let tags = run_tags(&plan, &params, &spec);
        assert_eq!(tags["dataset"], "synthetic");
        assert_eq!(tags["ema_fast"], "3");
        assert_eq!(tags["ema_slow"], "8");
        assert_eq!(tags["atr_stop"], "1");
        assert_eq!(tags["entry"], "cross");
        assert_eq!(tags["window"], "is");
        assert_eq!(tags["symbol"], "EURUSD");
        assert_eq!(tags["data_mode"], "ticks");
    }

    #[test]
    fn unaligned_windows_start_warmup_on_a_complete_bucket_boundary() {
        let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
            .with_atr_period(5);
        let point = family.points()[0];
        let strategy = ConfiguredStrategy::compile(
            family.config(&point),
            &family.library(),
            "instance",
            "EURUSD",
        )
        .unwrap();
        let bindings = family
            .bindings("EURUSD", &point, strategy.input_requirements())
            .unwrap();
        let from = ts(40) + Duration::seconds(30);
        assert_eq!(warmup_start(&bindings, from).unwrap(), ts(33));
    }

    #[test]
    fn a_slice_uses_a_half_open_evaluation_window() {
        let events: SymbolEvents = (0..100)
            .map(|minute| {
                qs_backtest::data_feed::FeedEvent::new(
                    qs_backtest::data_feed::MarketEvent::Tick {
                        symbol: "EURUSD".into(),
                        ts: ts(minute),
                        bid: 1.0,
                        ask: 1.1,
                    },
                    qs_backtest::data_feed::EventMetadata::new(
                        qs_backtest::data_feed::SeriesRoles::PRIMARY,
                        0,
                        minute as u64,
                    ),
                )
            })
            .collect::<Vec<_>>()
            .into();
        let window = DataWindow::new("w", ts(40), ts(60)).unwrap();
        let slice = slice_events(&events, &window, ts(30));

        assert_eq!(slice.first().unwrap().event.ts(), ts(30));
        assert_eq!(slice.last().unwrap().event.ts(), ts(59));
        assert_eq!(slice.len(), 30);
    }
}
