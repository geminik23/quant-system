use std::sync::{Arc, Mutex};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, MarketEvent, SeriesRoles, TimestampBatch};
use qs_backtest::{
    AnalysisBoundary, AnalysisContext, AnalysisError, AnalysisPipeline, AnnotationError,
    AnnotationId, AnnotationLimits, AnnotationUse, BarSeriesSpec, ConfirmedPivotAnalyzer,
    HistoricalAnalyzer, HistoricalObservationView, MAX_ANNOTATION_NOTE_BYTES,
    MAX_OBSERVATION_SOURCE_SERIES, MAX_OBSERVATIONS_PER_BOUNDARY, MAX_PIVOT_SIDE_BARS,
    MAX_RETAINED_OBSERVATIONS, MissingIntervalPolicy, MomentumState, MultiTimeframeSeries,
    ObservationOrigin, ObservationStoreLimits, PivotConfig, PriceBasis, PriceZone,
    RejectionPattern, SeriesId, SeriesRequirement, StrategyAnnotation, StrategyObservationDraft,
    StrategyObservationValue, SwingKind, SwingPoint, Timeframe, WarmupRequirement, ZoneId,
    ZoneSide, ZoneSource, ZoneState,
};

fn base_ts() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn series_id(value: &str) -> SeriesId {
    SeriesId::new(value).unwrap()
}

fn series(symbol: &str, ids: &[&str], retained: usize) -> MultiTimeframeSeries {
    let specs = ids
        .iter()
        .map(|id| {
            let requirement = SeriesRequirement::new(
                series_id(id),
                symbol,
                Timeframe::minutes(5).unwrap(),
                PriceBasis::Bid,
                WarmupRequirement::bars(0).unwrap(),
            )
            .unwrap();
            BarSeriesSpec::new(requirement, retained, 0, MissingIntervalPolicy::Skip).unwrap()
        })
        .collect();
    MultiTimeframeSeries::new(specs).unwrap()
}

fn tick_batch(symbol: &str, ts: NaiveDateTime, price: f64) -> TimestampBatch {
    TimestampBatch {
        ts,
        events: vec![FeedEvent::new(
            MarketEvent::Tick {
                symbol: symbol.to_string(),
                ts,
                bid: price,
                ask: price + 0.01,
            },
            EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
        )],
    }
}

fn close_next(
    history: &mut MultiTimeframeSeries,
    symbol: &str,
    ts: NaiveDateTime,
    price: f64,
) -> Vec<qs_backtest::ClosedBar> {
    history.on_batch(&tick_batch(symbol, ts, price)).unwrap()
}

fn limits(retained: usize, per_boundary: usize) -> ObservationStoreLimits {
    ObservationStoreLimits::new(retained, per_boundary).unwrap()
}

fn pipeline(
    analyzers: Vec<Box<dyn HistoricalAnalyzer>>,
    retained: usize,
    per_boundary: usize,
) -> AnalysisPipeline {
    AnalysisPipeline::new(
        analyzers,
        limits(retained, per_boundary),
        AnnotationLimits::default(),
    )
    .unwrap()
}

fn annotation(
    id: &str,
    input_sequence: u64,
    use_kind: AnnotationUse,
    observed_through: NaiveDateTime,
    valid_from: Option<NaiveDateTime>,
    value: StrategyObservationValue,
) -> StrategyAnnotation {
    StrategyAnnotation::new(
        AnnotationId::new(id).unwrap(),
        input_sequence,
        observed_through,
        observed_through,
        valid_from,
        use_kind,
        "EURUSD",
        vec![series_id("m5")],
        value,
        Some("research input".to_string()),
        AnnotationLimits::default(),
    )
    .unwrap()
}

#[test]
fn validates_public_values_and_every_configured_bound() {
    let now = base_ts();
    assert!(ZoneId::new("").is_err());
    assert!(AnnotationId::new("bad/id").is_err());
    assert!(ObservationStoreLimits::new(0, 1).is_err());
    assert!(ObservationStoreLimits::new(1, 0).is_err());
    assert!(ObservationStoreLimits::new(MAX_RETAINED_OBSERVATIONS + 1, 1).is_err());
    assert!(ObservationStoreLimits::new(1, MAX_OBSERVATIONS_PER_BOUNDARY + 1).is_err());
    assert!(AnnotationLimits::new(1, MAX_ANNOTATION_NOTE_BYTES + 1, 1).is_err());
    assert!(AnnotationLimits::new(1, 1, MAX_OBSERVATION_SOURCE_SERIES + 1).is_err());
    assert!(PivotConfig::new(series_id("m5"), 0, 1).is_err());
    assert!(PivotConfig::new(series_id("m5"), 1, 0).is_err());
    assert!(PivotConfig::new(series_id("m5"), MAX_PIVOT_SIDE_BARS + 1, 1).is_err());

    assert!(
        PriceZone::new(
            ZoneId::new("zone-1").unwrap(),
            ZoneSide::Support,
            f64::NAN,
            2.0,
            now,
            0,
            ZoneState::Active,
            ZoneSource::CausalAnnotation,
        )
        .is_err()
    );
    assert!(
        PriceZone::new(
            ZoneId::new("zone-1").unwrap(),
            ZoneSide::Support,
            2.0,
            1.0,
            now,
            0,
            ZoneState::Active,
            ZoneSource::CausalAnnotation,
        )
        .is_err()
    );
    assert!(SwingPoint::new(SwingKind::High, 1.0, now, now, now).is_err());
    assert!(
        SwingPoint::new(
            SwingKind::Low,
            1.0,
            now,
            now + Duration::minutes(5),
            now + Duration::minutes(4),
        )
        .is_err()
    );
    assert!(StrategyObservationValue::rejection(RejectionPattern::LongWick, now, now,).is_err());
    assert!(
        StrategyObservationDraft::new(
            "bad symbol",
            vec![],
            StrategyObservationValue::Momentum(MomentumState::Advancing),
        )
        .is_err()
    );
    assert!(
        StrategyObservationDraft::new(
            "EURUSD",
            vec![series_id("m5"), series_id("m5")],
            StrategyObservationValue::Momentum(MomentumState::Advancing),
        )
        .is_err()
    );
}

struct EchoAnalyzer;

impl HistoricalAnalyzer for EchoAnalyzer {
    fn on_bar(
        &mut self,
        bar: &qs_backtest::ClosedBar,
        _context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        Ok(vec![StrategyObservationDraft::new(
            bar.symbol(),
            vec![bar.series_id().clone()],
            StrategyObservationValue::rejection(
                RejectionPattern::LongWick,
                bar.open_time(),
                bar.close_time(),
            )?,
        )?])
    }
}

#[test]
fn delayed_reveal_stamps_actual_boundary_and_keeps_prior_snapshot_immutable() {
    let start = base_ts();
    let reveal = start + Duration::minutes(17);
    let mut history = series("EURUSD", &["m5"], 8);
    assert!(close_next(&mut history, "EURUSD", start, 1.0).is_empty());
    let bars = close_next(&mut history, "EURUSD", reveal, 2.0);
    assert_eq!(bars[0].close_time(), start + Duration::minutes(5));

    let mut analysis = pipeline(vec![Box::new(EchoAnalyzer)], 8, 8);
    let output = analysis
        .on_boundary(AnalysisBoundary::new(reveal, &bars, &history))
        .unwrap();
    assert_eq!(output.observations().len(), 1);
    assert_eq!(output.observations()[0].observed_through(), reveal);
    assert_eq!(output.observations()[0].valid_from(), reveal);
    let snapshot = output.observations()[0].clone();

    let later = reveal + Duration::minutes(5);
    let later_bars = close_next(&mut history, "EURUSD", later, 3.0);
    analysis
        .on_boundary(AnalysisBoundary::new(later, &later_bars, &history))
        .unwrap();
    assert_eq!(
        analysis.observations().observations(2).iter().next(),
        Some(&snapshot)
    );
}

struct OrderedAnalyzer {
    label: MomentumState,
    calls: Arc<Mutex<Vec<String>>>,
    require_staged: bool,
}

impl HistoricalAnalyzer for OrderedAnalyzer {
    fn on_bar(
        &mut self,
        bar: &qs_backtest::ClosedBar,
        context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        if self.require_staged && context.observations().observations(usize::MAX).is_empty() {
            return Err(AnalysisError::Analyzer {
                message: "missing staged observation".to_string(),
            });
        }
        self.calls
            .lock()
            .unwrap()
            .push(format!("{}:{:?}", bar.series_id(), self.label));
        Ok(vec![StrategyObservationDraft::new(
            bar.symbol(),
            vec![bar.series_id().clone()],
            StrategyObservationValue::Momentum(self.label),
        )?])
    }
}

#[test]
fn preserves_bar_analyzer_output_order_and_exposes_staged_output() {
    let start = base_ts();
    let boundary = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["a-m5", "b-m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", boundary, 2.0);
    assert_eq!(
        bars.iter()
            .map(|bar| bar.series_id().as_str())
            .collect::<Vec<_>>(),
        ["a-m5", "b-m5"]
    );

    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut analysis = pipeline(
        vec![
            Box::new(OrderedAnalyzer {
                label: MomentumState::Advancing,
                calls: Arc::clone(&calls),
                require_staged: false,
            }),
            Box::new(OrderedAnalyzer {
                label: MomentumState::Stalling,
                calls: Arc::clone(&calls),
                require_staged: true,
            }),
        ],
        8,
        8,
    );
    let output = analysis
        .on_boundary(AnalysisBoundary::new(boundary, &bars, &history))
        .unwrap();
    assert_eq!(
        output
            .observations()
            .iter()
            .map(|value| value.sequence())
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "a-m5:Advancing",
            "a-m5:Stalling",
            "b-m5:Advancing",
            "b-m5:Stalling",
        ]
    );
}

struct FailingAnalyzer;

impl HistoricalAnalyzer for FailingAnalyzer {
    fn on_bar(
        &mut self,
        _bar: &qs_backtest::ClosedBar,
        _context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        Err(AnalysisError::Analyzer {
            message: "expected failure".to_string(),
        })
    }
}

#[test]
fn analyzer_failure_discards_boundary_and_terminally_fails_pipeline() {
    let start = base_ts();
    let boundary = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", boundary, 2.0);
    let mut analysis = pipeline(
        vec![Box::new(EchoAnalyzer), Box::new(FailingAnalyzer)],
        8,
        8,
    );
    analysis
        .add_annotation(annotation(
            "prior",
            1,
            AnnotationUse::CausalDecisionInput,
            start,
            Some(start),
            StrategyObservationValue::Momentum(MomentumState::Sideways),
        ))
        .unwrap();
    analysis
        .on_boundary(AnalysisBoundary::new(start, &[], &history))
        .unwrap();
    let prior = analysis
        .observations()
        .observations(1)
        .latest()
        .unwrap()
        .clone();
    analysis
        .add_annotation(annotation(
            "failed-boundary",
            2,
            AnnotationUse::CausalDecisionInput,
            start,
            Some(boundary),
            StrategyObservationValue::Momentum(MomentumState::Reversing),
        ))
        .unwrap();

    let error = analysis
        .on_boundary(AnalysisBoundary::new(boundary, &bars, &history))
        .unwrap_err();
    assert!(matches!(
        error,
        AnalysisError::AnalyzerFailure {
            analyzer_index: 1,
            ..
        }
    ));
    assert_eq!(analysis.observations().len(), 1);
    assert_eq!(
        analysis.observations().observations(1).latest(),
        Some(&prior)
    );
    assert_eq!(analysis.annotations().pending_causal().len(), 1);
    assert_eq!(
        analysis.annotations().pending_causal()[0]
            .annotation_id()
            .as_str(),
        "failed-boundary"
    );
    assert!(analysis.is_failed());
    assert!(matches!(
        analysis.on_boundary(AnalysisBoundary::new(boundary, &bars, &history)),
        Err(AnalysisError::PipelineFailed)
    ));
}

#[test]
fn annotation_timeline_orders_activation_and_separates_research_records() {
    let start = base_ts();
    let activation = start + Duration::minutes(10);
    let history = series("EURUSD", &["m5"], 4);
    let mut analysis = pipeline(vec![], 8, 8);
    analysis
        .add_annotation(annotation(
            "causal-b",
            2,
            AnnotationUse::CausalDecisionInput,
            start,
            Some(activation),
            StrategyObservationValue::Momentum(MomentumState::Stalling),
        ))
        .unwrap();
    analysis
        .add_annotation(annotation(
            "causal-a",
            1,
            AnnotationUse::CausalDecisionInput,
            start,
            Some(activation),
            StrategyObservationValue::Momentum(MomentumState::Advancing),
        ))
        .unwrap();
    analysis
        .add_annotation(annotation(
            "hindsight",
            3,
            AnnotationUse::HindsightLabel,
            activation,
            None,
            StrategyObservationValue::Momentum(MomentumState::Reversing),
        ))
        .unwrap();
    analysis
        .add_annotation(annotation(
            "journal",
            4,
            AnnotationUse::JournalOnly,
            activation,
            None,
            StrategyObservationValue::Momentum(MomentumState::Sideways),
        ))
        .unwrap();

    let before = activation - Duration::seconds(1);
    assert!(
        analysis
            .on_boundary(AnalysisBoundary::new(before, &[], &history))
            .unwrap()
            .observations()
            .is_empty()
    );
    let output = analysis
        .on_boundary(AnalysisBoundary::new(activation, &[], &history))
        .unwrap();
    let ids = output
        .observations()
        .iter()
        .map(|observation| match observation.origin() {
            ObservationOrigin::CausalAnnotation { annotation_id } => annotation_id.as_str(),
            ObservationOrigin::Analyzer => panic!("unexpected analyzer observation"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["causal-a", "causal-b"]);
    assert_eq!(analysis.annotations().research_only().len(), 2);
    assert_eq!(analysis.observations().len(), 2);
}

#[test]
fn annotations_reject_backdating_duplicates_and_retroactive_insertion() {
    let start = base_ts();
    let limits = AnnotationLimits::default();
    let causal = StrategyAnnotation::new(
        AnnotationId::new("backdated").unwrap(),
        1,
        start,
        start,
        Some(start - Duration::seconds(1)),
        AnnotationUse::CausalDecisionInput,
        "EURUSD",
        vec![],
        StrategyObservationValue::Momentum(MomentumState::Advancing),
        None,
        limits,
    );
    assert!(matches!(
        causal,
        Err(AnnotationError::CausalBackdating { .. })
    ));
    assert!(
        StrategyAnnotation::new(
            AnnotationId::new("hindsight").unwrap(),
            2,
            start,
            start,
            Some(start),
            AnnotationUse::HindsightLabel,
            "EURUSD",
            vec![],
            StrategyObservationValue::Momentum(MomentumState::Advancing),
            None,
            limits,
        )
        .is_err()
    );
    let created_at = start + Duration::minutes(1);
    let valid_from = start + Duration::minutes(2);
    let manual_zone = PriceZone::new(
        ZoneId::new("manual-zone").unwrap(),
        ZoneSide::Support,
        1.0,
        2.0,
        created_at,
        0,
        ZoneState::Active,
        ZoneSource::CausalAnnotation,
    )
    .unwrap();
    assert!(
        StrategyAnnotation::new(
            AnnotationId::new("manual-zone-input").unwrap(),
            3,
            created_at,
            start,
            Some(valid_from),
            AnnotationUse::CausalDecisionInput,
            "EURUSD",
            vec![],
            StrategyObservationValue::Zone(manual_zone),
            None,
            limits,
        )
        .is_ok()
    );

    let history = series("EURUSD", &["m5"], 4);
    let mut analysis = pipeline(vec![], 8, 8);
    let first = annotation(
        "first",
        10,
        AnnotationUse::CausalDecisionInput,
        start,
        Some(start + Duration::minutes(1)),
        StrategyObservationValue::Momentum(MomentumState::Advancing),
    );
    analysis.add_annotation(first.clone()).unwrap();
    assert!(analysis.add_annotation(first).is_err());
    let duplicate_sequence = annotation(
        "second",
        10,
        AnnotationUse::JournalOnly,
        start,
        None,
        StrategyObservationValue::Momentum(MomentumState::Sideways),
    );
    assert!(analysis.add_annotation(duplicate_sequence).is_err());

    let advanced = start + Duration::minutes(5);
    analysis
        .on_boundary(AnalysisBoundary::new(advanced, &[], &history))
        .unwrap();
    let retroactive = annotation(
        "retroactive",
        11,
        AnnotationUse::CausalDecisionInput,
        start,
        Some(advanced),
        StrategyObservationValue::Momentum(MomentumState::Reversing),
    );
    assert!(matches!(
        analysis.add_annotation(retroactive),
        Err(AnalysisError::Annotation(
            AnnotationError::RetroactiveCausalInsertion { .. }
        ))
    ));
}

#[test]
fn bounded_store_wraps_with_exact_omitted_count_and_deterministic_lookup() {
    let start = base_ts();
    let history = series("EURUSD", &["m5"], 4);
    let mut analysis = pipeline(vec![], 2, 2);
    for index in 0..4_u64 {
        let boundary = start + Duration::minutes(index as i64 + 1);
        let zone = PriceZone::new(
            ZoneId::new("shared-zone").unwrap(),
            ZoneSide::Resistance,
            1.0 + index as f64,
            1.5 + index as f64,
            start,
            index as u32,
            ZoneState::Active,
            ZoneSource::CausalAnnotation,
        )
        .unwrap();
        analysis
            .add_annotation(annotation(
                &format!("zone-{index}"),
                index,
                AnnotationUse::CausalDecisionInput,
                start,
                Some(boundary),
                StrategyObservationValue::Zone(zone),
            ))
            .unwrap();
        analysis
            .on_boundary(AnalysisBoundary::new(boundary, &[], &history))
            .unwrap();
    }
    let store = analysis.observations();
    assert_eq!(store.len(), 2);
    assert_eq!(store.omitted(), 2);
    assert_eq!(
        store
            .observations(10)
            .iter()
            .map(|value| value.sequence())
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(store.observations(1).latest().unwrap().sequence(), 3);
    assert_eq!(
        store
            .for_symbol("EURUSD", 1)
            .iter()
            .next()
            .unwrap()
            .sequence(),
        3
    );
    assert_eq!(
        store
            .latest_zone(&ZoneId::new("shared-zone").unwrap())
            .unwrap()
            .sequence(),
        3
    );

    fn object_safe_len(view: &dyn HistoricalObservationView) -> usize {
        view.observations(usize::MAX).len()
    }
    assert_eq!(object_safe_len(store), 2);
}

fn run_pivot(prices: &[f64], delayed_last: bool) -> Vec<qs_backtest::StrategyObservation> {
    let start = base_ts();
    let mut history = series("EURUSD", &["m5"], 8);
    let mut analysis = pipeline(
        vec![Box::new(ConfirmedPivotAnalyzer::new(
            PivotConfig::new(series_id("m5"), 1, 1).unwrap(),
        ))],
        8,
        8,
    );
    let mut observations = Vec::new();
    for (index, price) in prices.iter().enumerate() {
        let mut timestamp = start + Duration::minutes(index as i64 * 5);
        if delayed_last && index + 1 == prices.len() {
            timestamp += Duration::minutes(2);
        }
        let bars = close_next(&mut history, "EURUSD", timestamp, *price);
        let output = analysis
            .on_boundary(AnalysisBoundary::new(timestamp, &bars, &history))
            .unwrap();
        observations.extend_from_slice(output.observations());
    }
    observations
}

#[test]
fn confirmed_pivot_emits_high_and_low_only_after_actual_reveal() {
    let high = run_pivot(&[1.0, 3.0, 1.0, 2.0], true);
    assert_eq!(high.len(), 1);
    let StrategyObservationValue::Swing(swing) = high[0].value() else {
        panic!("expected swing");
    };
    assert_eq!(swing.kind(), SwingKind::High);
    assert_eq!(swing.price(), 3.0);
    assert_eq!(swing.anchor_open_time(), base_ts() + Duration::minutes(5));
    assert_eq!(swing.anchor_close_time(), base_ts() + Duration::minutes(10));
    assert_eq!(swing.confirmed_at(), base_ts() + Duration::minutes(17));
    assert_eq!(high[0].valid_from(), base_ts() + Duration::minutes(17));

    let low = run_pivot(&[3.0, 1.0, 3.0, 2.0], false);
    let StrategyObservationValue::Swing(swing) = low[0].value() else {
        panic!("expected swing");
    };
    assert_eq!(swing.kind(), SwingKind::Low);
    assert_eq!(swing.price(), 1.0);
    assert!(run_pivot(&[1.0, 3.0, 3.0, 2.0], false).is_empty());
    assert!(run_pivot(&[1.0, 3.0], false).is_empty());
}

#[test]
fn unknown_pivot_series_is_typed_and_terminal_after_callback_error() {
    let start = base_ts();
    let boundary = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", boundary, 2.0);
    let mut analysis = pipeline(
        vec![Box::new(ConfirmedPivotAnalyzer::new(
            PivotConfig::new(series_id("missing"), 1, 1).unwrap(),
        ))],
        4,
        4,
    );
    let error = analysis
        .on_boundary(AnalysisBoundary::new(boundary, &bars, &history))
        .unwrap_err();
    assert!(matches!(
        error,
        AnalysisError::AnalyzerFailure {
            source,
            ..
        } if matches!(*source, AnalysisError::SeriesView(_))
    ));
    assert!(analysis.is_failed());
}

#[test]
fn boundary_validation_rejects_regression_and_future_bar_without_mutation() {
    let start = base_ts();
    let close = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", close, 2.0);
    let mut analysis = pipeline(vec![], 4, 4);
    assert!(matches!(
        analysis.on_boundary(AnalysisBoundary::new(start, &bars, &history)),
        Err(AnalysisError::BarAfterBoundary { .. })
    ));
    assert!(analysis.observations().is_empty());
    analysis
        .on_boundary(AnalysisBoundary::new(close, &bars, &history))
        .unwrap();
    assert!(matches!(
        analysis.on_boundary(AnalysisBoundary::new(start, &[], &history)),
        Err(AnalysisError::BoundaryRegression { .. })
    ));
}

#[test]
fn analyzer_annotation_and_boundary_output_bounds_reject_atomically() {
    struct NoopAnalyzer;

    impl HistoricalAnalyzer for NoopAnalyzer {
        fn on_bar(
            &mut self,
            _bar: &qs_backtest::ClosedBar,
            _context: AnalysisContext<'_>,
        ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
            Ok(Vec::new())
        }
    }

    let too_many = (0..=qs_backtest::MAX_ANALYZERS)
        .map(|_| Box::new(NoopAnalyzer) as Box<dyn HistoricalAnalyzer>)
        .collect();
    assert!(matches!(
        AnalysisPipeline::new(
            too_many,
            ObservationStoreLimits::default(),
            AnnotationLimits::default(),
        ),
        Err(AnalysisError::TooManyAnalyzers { .. })
    ));

    let start = base_ts();
    let annotation_limits = AnnotationLimits::new(1, 32, 1).unwrap();
    let mut annotation_bound =
        AnalysisPipeline::new(vec![], ObservationStoreLimits::default(), annotation_limits)
            .unwrap();
    for index in 0..2 {
        let result = annotation_bound.add_annotation(
            StrategyAnnotation::new(
                AnnotationId::new(format!("bounded-{index}")).unwrap(),
                index,
                start,
                start,
                None,
                AnnotationUse::JournalOnly,
                "EURUSD",
                vec![],
                StrategyObservationValue::Momentum(MomentumState::Sideways),
                None,
                annotation_limits,
            )
            .unwrap(),
        );
        if index == 0 {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(AnalysisError::Annotation(
                    AnnotationError::TooManyAnnotations { .. }
                ))
            ));
        }
    }
    assert_eq!(annotation_bound.annotations().total_count(), 1);

    let narrow_limits = AnnotationLimits::new(4, 8, 1).unwrap();
    let mut narrow_pipeline =
        AnalysisPipeline::new(vec![], ObservationStoreLimits::default(), narrow_limits).unwrap();
    let permissive_limits = AnnotationLimits::default();
    let excessive_note = StrategyAnnotation::new(
        AnnotationId::new("excessive-note").unwrap(),
        10,
        start,
        start,
        None,
        AnnotationUse::JournalOnly,
        "EURUSD",
        vec![],
        StrategyObservationValue::Momentum(MomentumState::Sideways),
        Some("123456789".to_string()),
        permissive_limits,
    )
    .unwrap();
    assert!(matches!(
        narrow_pipeline.add_annotation(excessive_note),
        Err(AnalysisError::Annotation(AnnotationError::InvalidNote {
            maximum: 8
        }))
    ));
    let excessive_sources = StrategyAnnotation::new(
        AnnotationId::new("excessive-sources").unwrap(),
        11,
        start,
        start,
        None,
        AnnotationUse::JournalOnly,
        "EURUSD",
        vec![series_id("m5"), series_id("h1")],
        StrategyObservationValue::Momentum(MomentumState::Sideways),
        None,
        permissive_limits,
    )
    .unwrap();
    assert!(matches!(
        narrow_pipeline.add_annotation(excessive_sources),
        Err(AnalysisError::Annotation(AnnotationError::InvalidValue(
            source
        ))) if matches!(
            *source,
            AnalysisError::TooManySourceSeries {
                actual: 2,
                maximum: 1
            }
        )
    ));
    assert_eq!(narrow_pipeline.annotations().total_count(), 0);

    struct BurstAnalyzer;

    impl HistoricalAnalyzer for BurstAnalyzer {
        fn on_bar(
            &mut self,
            bar: &qs_backtest::ClosedBar,
            _context: AnalysisContext<'_>,
        ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
            Ok(vec![
                StrategyObservationDraft::new(
                    bar.symbol(),
                    vec![],
                    StrategyObservationValue::Momentum(MomentumState::Advancing),
                )?,
                StrategyObservationDraft::new(
                    bar.symbol(),
                    vec![],
                    StrategyObservationValue::Momentum(MomentumState::Stalling),
                )?,
            ])
        }
    }

    let boundary = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", boundary, 2.0);
    let mut output_bound = pipeline(vec![Box::new(BurstAnalyzer)], 4, 1);
    assert!(matches!(
        output_bound.on_boundary(AnalysisBoundary::new(boundary, &bars, &history)),
        Err(AnalysisError::TooManyBoundaryObservations { .. })
    ));
    assert!(output_bound.observations().is_empty());
    assert!(output_bound.is_failed());
}

#[test]
fn custom_analyzer_and_private_indicator_need_no_common_enum_extension() {
    struct PrivateIndicator {
        running_close: f64,
    }

    impl HistoricalAnalyzer for PrivateIndicator {
        fn on_bar(
            &mut self,
            bar: &qs_backtest::ClosedBar,
            _context: AnalysisContext<'_>,
        ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
            self.running_close += bar.close();
            Ok(Vec::new())
        }
    }

    let start = base_ts();
    let boundary = start + Duration::minutes(5);
    let mut history = series("EURUSD", &["m5"], 4);
    close_next(&mut history, "EURUSD", start, 1.0);
    let bars = close_next(&mut history, "EURUSD", boundary, 2.0);
    let mut analysis = pipeline(
        vec![Box::new(PrivateIndicator { running_close: 0.0 })],
        4,
        4,
    );
    assert!(
        analysis
            .on_boundary(AnalysisBoundary::new(boundary, &bars, &history))
            .unwrap()
            .observations()
            .is_empty()
    );
}
