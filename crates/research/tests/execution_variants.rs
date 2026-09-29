mod support;
use qs_backtest::{
    AnalysisPipeline, AnnotationLimits, BarSeriesSpec, FutureQuoteConfig, HistoricalStrategy,
    MissingIntervalPolicy, ObservationStoreLimits, PriceBasis, SeriesId, SeriesRequirement,
    StrategyContext, StrategyDescriptor, StrategyEvent, StrategyId, StrategyOutput,
    StrategyRequirements, Timeframe, WarmupRequirement,
};
use qs_research::families::EmaCrossFamily;
use qs_research::*;
use qs_strategy::{ParameterBinding, ParameterValue};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use support::{SYMBOL, at, config, synthetic_ticks};

struct DirectNoop {
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
}

impl HistoricalStrategy for DirectNoop {
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

struct DirectNoopFactory;

impl DirectResearchFactory for DirectNoopFactory {
    fn factory_name(&self) -> &str {
        "direct_noop"
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
        let requirement = SeriesRequirement::new(
            SeriesId::new("m1").unwrap(),
            symbol,
            Timeframe::minutes(1).unwrap(),
            PriceBasis::Bid,
            WarmupRequirement::bars(3).unwrap(),
        )
        .unwrap();
        Ok(DirectRunCandidate {
            strategy: Box::new(DirectNoop {
                descriptor: StrategyDescriptor::new(
                    StrategyId::new("direct_noop").unwrap(),
                    "direct_noop",
                    "Direct noop",
                )
                .unwrap(),
                requirements: StrategyRequirements::new(
                    vec![symbol.into()],
                    vec![requirement.clone()],
                    0,
                    false,
                    false,
                )
                .unwrap(),
            }),
            series: vec![
                BarSeriesSpec::new(requirement, 16, 0, MissingIntervalPolicy::Skip).unwrap(),
            ],
            analysis: AnalysisPipeline::new(
                vec![],
                ObservationStoreLimits::default(),
                AnnotationLimits::default(),
            )
            .unwrap(),
        })
    }
}
#[test]
fn typed_execution_variants_expand_before_replay_and_preserve_variant_provenance() {
    let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let plan = ResearchPlan::new(vec![SYMBOL.into()], windows, config());
    let variants = [
        ExecutionVariant {
            id: "baseline".into(),
            backtest: config(),
            future: FutureQuoteConfig::default(),
            profiles: None,
            portfolio: None,
        },
        ExecutionVariant {
            id: "alternate".into(),
            backtest: config(),
            future: FutureQuoteConfig {
                slippage_pips: 1.0,
                ..FutureQuoteConfig::default()
            },
            profiles: None,
            portfolio: None,
        },
    ];
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let batch = run_execution_variants(
        &plan,
        &family,
        &events,
        &variants,
        ExecutionVariantLimits::new(2, 16).unwrap(),
        ExperimentOptions::default(),
    )
    .unwrap();
    assert_eq!(batch.batches().len(), 2);
    assert_eq!(batch.table().len(), 4);
    for (id, result) in batch.batches() {
        assert!(
            result
                .run_recipes()
                .iter()
                .all(|recipe| recipe.run_tags["execution_variant"] == *id)
        );
        assert!(result.rows().iter().all(|row| row.status.is_completed()));
    }
    let combined = batch.combined().unwrap();
    assert_eq!(combined.candidate_recipes().len(), 2);
    assert_eq!(
        combined
            .run_recipes()
            .iter()
            .map(|recipe| recipe.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        combined
            .candidate_recipes()
            .iter()
            .map(|recipe| recipe.ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(
        combined
            .position_outcomes()
            .iter()
            .all(|position| position.id.starts_with("variant:"))
    );
    let mut duplicate = variants.to_vec();
    duplicate[1].id = "baseline".into();
    assert!(
        run_execution_variants(
            &plan,
            &family,
            &events,
            &duplicate,
            ExecutionVariantLimits::new(2, 16).unwrap(),
            ExperimentOptions::default()
        )
        .is_err()
    );
    assert!(
        run_execution_variants(
            &plan,
            &family,
            &events,
            &variants,
            ExecutionVariantLimits::new(1, 16).unwrap(),
            ExperimentOptions::default()
        )
        .is_err()
    );
}

#[test]
fn execution_variant_control_stops_without_a_combined_success_batch() {
    let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let plan = ResearchPlan::new(vec![SYMBOL.into()], windows, config()).with_workers(1);
    let variants = [ExecutionVariant {
        id: "baseline".into(),
        backtest: config(),
        future: FutureQuoteConfig::default(),
        profiles: None,
        portfolio: None,
    }];
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let cancelled = AtomicBool::new(false);
    let progress = Mutex::new(Vec::new());
    let result = run_execution_variants_controlled(
        &plan,
        &family,
        &events,
        &variants,
        ExecutionVariantLimits::new(1, 2).unwrap(),
        ExperimentOptions::default(),
        &|| cancelled.load(Ordering::SeqCst),
        &|value| {
            progress.lock().unwrap().push(value);
            cancelled.store(true, Ordering::SeqCst);
        },
    );
    assert!(matches!(result, Err(ResearchError::Cancelled)));
    assert_eq!(progress.lock().unwrap()[0].completed_runs, 1);
    assert_eq!(progress.lock().unwrap()[0].total_runs, 2);
}

#[test]
fn heterogeneous_candidates_run_distinct_documents_in_one_account() {
    let family = EmaCrossFamily::new(3..=4, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let points = family.points();
    let instances = points
        .iter()
        .take(2)
        .enumerate()
        .map(|(index, point)| HeterogeneousInstanceSpec {
            instance_id: format!("instance_{index}"),
            symbol: SYMBOL.into(),
            document: family.config(point),
            geometry: family.geometry(SYMBOL, point),
            historical_inputs: vec![],
            profiles: None,
        })
        .collect::<Vec<_>>();
    let candidate = HeterogeneousPortfolioCandidate {
        id: "mixed".into(),
        instances,
        portfolio: PortfolioPlan::default(),
    };
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let plan = ResearchPlan::new(vec![SYMBOL.into()], windows, config());
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let batch = run_heterogeneous_portfolios(
        &plan,
        &[candidate],
        &events,
        ExecutionVariantLimits::new(1, 4).unwrap(),
    )
    .unwrap();
    assert_eq!(batch.table().len(), 2);
    let retained = &batch.batches()["mixed"];
    assert!(retained.rows().iter().all(|row| row.status.is_completed()));
    assert!(
        retained.candidate_recipes()[0]
            .document
            .as_ref()
            .unwrap()
            .is_array()
    );
    assert_eq!(retained.candidate_recipes()[0].series_by_instance.len(), 2);
}

#[test]
fn mixed_configured_and_direct_instances_run_in_one_account_with_instance_recipes() {
    let family = EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap())
        .with_atr_period(3);
    let point = family.points().remove(0);
    let direct_factory = DirectNoopFactory;
    let direct_point = direct_factory.point(0).unwrap();
    let candidate = MixedHeterogeneousPortfolioCandidate {
        id: "configured_and_direct".into(),
        configured: vec![HeterogeneousInstanceSpec {
            instance_id: "configured".into(),
            symbol: SYMBOL.into(),
            document: family.config(&point),
            geometry: family.geometry(SYMBOL, &point),
            historical_inputs: vec![],
            profiles: None,
        }],
        direct: vec![HeterogeneousDirectInstanceSpec {
            instance_id: "direct".into(),
            symbol: SYMBOL.into(),
            factory_name: direct_factory.factory_name().into(),
            point: direct_point,
            profiles: None,
        }],
        portfolio: PortfolioPlan::default(),
    };
    let windows = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(500)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(500), at(800)).unwrap(),
    };
    let plan = ResearchPlan::new(vec![SYMBOL.into()], windows, config());
    let events = BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(850))]);
    let factories: BTreeMap<String, &dyn DirectResearchFactory> = BTreeMap::from([(
        direct_factory.factory_name().into(),
        &direct_factory as &dyn DirectResearchFactory,
    )]);
    let result = run_mixed_heterogeneous_portfolios(
        &plan,
        &[candidate],
        &factories,
        &events,
        ExecutionVariantLimits::new(1, 4).unwrap(),
    )
    .unwrap();
    assert_eq!(result.table().len(), 2);
    let retained = &result.batches()["configured_and_direct"];
    assert_eq!(retained.candidate_recipes()[0].series_by_instance.len(), 2);
    assert!(
        retained.candidate_recipes()[0]
            .document
            .as_ref()
            .is_some_and(serde_json::Value::is_object)
    );
    assert!(retained.rows().iter().all(|row| row.status.is_completed()));
}
