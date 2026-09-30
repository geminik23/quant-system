//! A replay's adapter and analysis extension points must be movable between threads.
//!
//! A batch parameter search runs one configuration per worker thread, and a later live instance runs inside its own task. Both reuse the same adapter components, so the contract is asserted here rather than discovered when either is written.

use std::marker::PhantomData;

use qs_backtest::{
    AnalysisContext, AnalysisError, AnalysisPipeline, BacktestConfiguredStrategyAdapter,
    BacktestRunner, ClosedBar, HistoricalAnalyzer, HistoricalNamedInputProjector,
    NamedInputProjectionContext, NamedInputProjectionError, ProjectedNamedInput,
    StrategyBacktestResult, StrategyObservationDraft, VecFeed,
};
use qs_strategy::{ScalarType, ValueType};

struct Probe<T>(PhantomData<T>);

impl<T: Send> Probe<T> {
    fn assert_send() {}
}

#[test]
fn replay_components_move_between_threads() {
    Probe::<BacktestRunner>::assert_send();
    Probe::<qs_backtest::runner::BacktestConfig>::assert_send();
    Probe::<qs_strategy::ConfiguredStrategy>::assert_send();
    Probe::<BacktestConfiguredStrategyAdapter>::assert_send();
    Probe::<AnalysisPipeline>::assert_send();
    Probe::<StrategyBacktestResult>::assert_send();
    Probe::<VecFeed>::assert_send();
}

struct StaticProjector;

impl HistoricalNamedInputProjector for StaticProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Number)
    }

    fn project(
        &self,
        _context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        unreachable!("this probe only exercises the thread contract")
    }
}

struct CountingAnalyzer;

impl HistoricalAnalyzer for CountingAnalyzer {
    fn on_bar(
        &mut self,
        _bar: &ClosedBar,
        _context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        Ok(Vec::new())
    }
}

#[test]
fn boxed_extension_points_move_into_a_worker_thread() {
    let projector: Box<dyn HistoricalNamedInputProjector> = Box::new(StaticProjector);
    let analyzer: Box<dyn HistoricalAnalyzer> = Box::new(CountingAnalyzer);

    // Moving both boxes across the boundary is the property under test; the call just proves the moved value is still usable.
    let output_type = std::thread::scope(|scope| {
        scope
            .spawn(move || {
                drop(analyzer);
                projector.output_type()
            })
            .join()
            .expect("worker thread panicked")
    });

    assert_eq!(output_type, ValueType::required(ScalarType::Number));
}
