mod support;
use chrono::Duration;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BarSeriesSpec, HistoricalStrategy, MissingIntervalPolicy,
    ObservationStoreLimits, PriceBasis, SeriesId, SeriesRequirement, StrategyContext,
    StrategyDescriptor, StrategyEvent, StrategyId, StrategyOutput, StrategyRequirements, Timeframe,
    WarmupRequirement,
};
use qs_research::*;
use qs_strategy::{ParameterBinding, ParameterValue};
use std::collections::{BTreeMap, BTreeSet};

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use support::{SYMBOL, at, config, synthetic_ticks};
struct Noop {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
}

impl HistoricalStrategy for Noop {
    type Error = DirectStrategyError;
    fn descriptor(&self) -> &StrategyDescriptor {
        &self.descriptor
    }
    fn requirements(&self) -> &StrategyRequirements {
        &self.requirements
    }
    fn on_event(
        &mut self,
        _: StrategyEvent<'_>,
        _: StrategyContext<'_>,
    ) -> Result<StrategyOutput, Self::Error> {
        Ok(StrategyOutput::none())
    }
}
struct Factory {
    created: AtomicUsize,
}
impl Factory {
    fn new() -> Self {
        Self {
            created: AtomicUsize::new(0),
        }
    }
}
impl DirectResearchFactory for Factory {
    fn factory_name(&self) -> &str {
        "noop_direct"
    }
    fn revision(&self) -> &str {
        "r1"
    }
    fn point_count(&self) -> usize {
        1
    }
    fn point(&self, index: usize) -> Option<DirectFactoryPoint> {
        (index == 0).then(|| DirectFactoryPoint {
            binding: ParameterBinding::new([("variant", ParameterValue::Integer(1))]),
        })
    }
    fn create(
        &self,
        _: &DirectFactoryPoint,
        symbol: &str,
        _: &DataWindow,
    ) -> Result<DirectRunCandidate, ResearchError> {
        let instance = self.created.fetch_add(1, Ordering::SeqCst);
        let series = SeriesRequirement::new(
            SeriesId::new("m1").unwrap(),
            symbol,
            Timeframe::minutes(1).unwrap(),
            PriceBasis::Bid,
            WarmupRequirement::bars(3).unwrap(),
        )
        .unwrap();
        let requirements =
            StrategyRequirements::new(vec![symbol.into()], vec![series.clone()], 0, false, false)
                .unwrap();
        let descriptor = StrategyDescriptor::new(
            StrategyId::new("noop").unwrap(),
            format!("instance_{instance}"),
            "Noop direct",
        )
        .unwrap();
        Ok(DirectRunCandidate {
            strategy: Box::new(Noop {
                descriptor,
                requirements,
            }),
            series: vec![BarSeriesSpec::new(series, 16, 0, MissingIntervalPolicy::Skip).unwrap()],
            analysis: AnalysisPipeline::new(
                vec![],
                ObservationStoreLimits::default(),
                AnnotationLimits::default(),
            )
            .unwrap(),
        })
    }
}
fn plan(workers: usize) -> ResearchPlan {
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(100), at(300)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(300), at(500)).unwrap(),
    };
    ResearchPlan::new(vec![SYMBOL.into()], windows, config()).with_workers(workers)
}
#[test]
fn direct_factories_create_fresh_strategy_and_extension_state_for_every_window() {
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(550))]);
    let factory = Factory::new();
    let first = run_direct_factory_batch(
        &plan(1),
        &factory,
        &events,
        ExperimentOptions {
            experiment_id: Some(ExperimentId::new("direct").unwrap()),
            caller_revision: Some("r1".into()),
            dataset_reference: Some("synthetic".into()),
        },
    )
    .unwrap();
    assert_eq!(factory.created.load(Ordering::SeqCst), 2);
    assert_eq!(first.len(), 2);
    assert!(first.rows().iter().all(|row| row.status.is_completed()));
    assert!(first.bound_document(0).is_none());
    assert!(first.candidate_recipes()[0].document.is_none());
    assert_eq!(
        first.candidate_recipes()[0]
            .registered_factory
            .as_ref()
            .unwrap()
            .name,
        "noop_direct"
    );
    let second =
        run_direct_factory_batch(&plan(2), &factory, &events, ExperimentOptions::default())
            .unwrap();
    assert_eq!(factory.created.load(Ordering::SeqCst), 4);
    assert_eq!(first.table(), second.table());

    assert!(first.run_recipes().iter().all(|recipe| {
        recipe
            .coverage
            .as_ref()
            .is_some_and(|coverage| coverage.first_input_at < Some(recipe.from))
    }));
    assert_eq!(
        first.run_recipes()[0].to - first.run_recipes()[0].from,
        Duration::minutes(200)
    );
}

#[test]
fn direct_factory_control_reports_progress_and_publishes_no_batch_after_cancellation() {
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(550))]);
    let factory = Factory::new();
    let cancelled = AtomicBool::new(false);
    let progress = Mutex::new(Vec::new());
    let result = run_direct_factory_batch_controlled(
        &plan(1),
        &factory,
        &events,
        ExperimentOptions::default(),
        ResearchAdmissionLimits::new(1, 2).unwrap(),
        &|| cancelled.load(Ordering::SeqCst),
        &|value| {
            progress.lock().unwrap().push(value);
            cancelled.store(true, Ordering::SeqCst);
        },
    );
    let ResearchError::CancelledWithPartial(partial) = result.unwrap_err() else {
        panic!("direct cancellation must retain completed runs");
    };
    assert_eq!(partial.run_recipes().len(), 1);
    let completed = BTreeSet::from([partial.run_recipes()[0].ordinal]);
    let remaining = run_direct_factory_batch_controlled_resume(
        &plan(1),
        &factory,
        &events,
        ExperimentOptions::default(),
        ResearchAdmissionLimits::new(1, 2).unwrap(),
        &completed,
        &|| false,
        &|_| {},
    )
    .unwrap();
    assert_eq!(remaining.run_recipes().len(), 1);
    assert_ne!(
        partial.run_recipes()[0].ordinal,
        remaining.run_recipes()[0].ordinal
    );
    assert_eq!(
        progress.lock().unwrap().as_slice(),
        &[BatchProgress {
            completed_runs: 1,
            total_runs: 2,
        }]
    );
}
