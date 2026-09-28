#[path = "../../strategy/tests/support/numeric_catalog_fixture.rs"]
mod numeric_catalog_fixture;
mod support;
use qs_backtest::data_feed::{EventMetadata, FeedEvent, SeriesRoles};
use qs_backtest::*;
use qs_strategy::{ConfiguredStrategy, MaterialLibrary, NamedValue, SourceId, ValueType};
use std::cell::Cell;
use support::configured::{SYMBOL, analysis, runner_config, ts};
struct Projector {
    index: usize,
    value_type: ValueType,
    ordinal: Cell<usize>,
}
impl HistoricalNamedInputProjector for Projector {
    fn output_type(&self) -> ValueType {
        self.value_type
    }
    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        let updated = !context.closed_bars.is_empty();
        if updated {
            self.ordinal.set(self.ordinal.get() + 1)
        }
        let NamedValue { value, .. } =
            numeric_catalog_fixture::named_value(self.index, self.value_type, self.ordinal.get());
        Ok(ProjectedNamedInput { value, updated })
    }
}
fn feed() -> VecFeed {
    let events = (0..702)
        .map(|index| {
            let bar = numeric_catalog_fixture::bar(index);
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol: SYMBOL.into(),
                    ts: ts(index as i64),
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    volume: 0,
                    spread: Some(0.0002),
                    timeframe_seconds: Some(60),
                    tick_count: None,
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )
        })
        .collect();
    VecFeed::from_feed_events(events)
}
#[test]
fn every_numeric_output_runs_through_the_historical_stored_bar_adapter() {
    let catalog = numeric_catalog_fixture::catalog();
    for spec in catalog {
        let document = numeric_catalog_fixture::document(&spec);
        let strategy = ConfiguredStrategy::compile(
            document,
            &MaterialLibrary::builtins(),
            "catalog_instance",
            SYMBOL,
        )
        .unwrap();
        let lookback = strategy.input_requirements().completed_bars[0].required_lookback;
        let requirement = SeriesRequirement::new(
            SeriesId::new("m1").unwrap(),
            SYMBOL,
            Timeframe::minutes(1).unwrap(),
            PriceBasis::Bid,
            WarmupRequirement::bars(lookback).unwrap(),
        )
        .unwrap();
        let series =
            BarSeriesSpec::new(requirement, lookback + 16, 0, MissingIntervalPolicy::Skip).unwrap();
        let named = spec
            .inputs
            .iter()
            .enumerate()
            .map(|(index, value_type)| {
                ConfiguredNamedInputBinding::new(
                    format!("input_{index}"),
                    Box::new(Projector {
                        index,
                        value_type: *value_type,
                        ordinal: Cell::new(0),
                    }),
                )
            })
            .collect();
        let mut adapter = BacktestConfiguredStrategyAdapter::new(
            strategy,
            StrategyDescriptor::new(StrategyId::new("catalog").unwrap(), "catalog", spec.key)
                .unwrap(),
            ConfiguredHistoricalBindings::new(
                vec![ConfiguredSourceBinding::new(
                    SourceId::new("bars").unwrap(),
                    series,
                )],
                named,
                HistoricalVolumeProjection::OptionalTickCount,
            ),
            0,
        )
        .unwrap();
        let mut market = feed();
        let result = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut market,
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("{} replay failed: {error}", spec.key));
        assert_eq!(
            adapter.configured_strategy().state_id(),
            "done",
            "{} never became valid historically",
            spec.key
        );
        let record = result
            .research
            .journal
            .records
            .iter()
            .find(|record| record.values().contains_key("feature"))
            .unwrap_or_else(|| panic!("{} retained no historical numeric value", spec.key));
        let actual = record.values()["feature"];
        let revealed_minute = usize::try_from((record.observed_through() - ts(0)).num_minutes())
            .expect("fixture timestamp is nonnegative");
        let expected = numeric_catalog_fixture::configured_historical_value_through(
            &spec,
            revealed_minute - 1,
        );
        let tolerance = 1e-11_f64.max(expected.abs() * 1e-11);
        assert!(
            (actual - expected).abs() <= tolerance,
            "{} historical value {actual:?} differs from configured value {expected:?}",
            spec.key
        );
    }
}
