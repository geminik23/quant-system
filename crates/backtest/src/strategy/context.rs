//! Read-only historical and engine state exposed at one strategy boundary.

use chrono::NaiveDateTime;
use qs_core::TradeEngine;

use super::{HistoricalObservationView, HistoricalSeriesView};

/// Complete causal state available while evaluating one historical boundary.
#[derive(Clone, Copy)]
pub struct StrategyContext<'a> {
    observed_through: NaiveDateTime,
    series: &'a dyn HistoricalSeriesView,
    observations: &'a dyn HistoricalObservationView,
    engine: &'a TradeEngine,
    warmup_complete: bool,
}

impl<'a> StrategyContext<'a> {
    pub fn new(
        observed_through: NaiveDateTime,
        series: &'a dyn HistoricalSeriesView,
        observations: &'a dyn HistoricalObservationView,
        engine: &'a TradeEngine,
        warmup_complete: bool,
    ) -> Self {
        Self {
            observed_through,
            series,
            observations,
            engine,
            warmup_complete,
        }
    }

    pub fn observed_through(self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn series(self) -> &'a dyn HistoricalSeriesView {
        self.series
    }

    pub fn observations(self) -> &'a dyn HistoricalObservationView {
        self.observations
    }

    pub fn engine(self) -> &'a TradeEngine {
        self.engine
    }

    pub fn warmup_complete(self) -> bool {
        self.warmup_complete
    }
}
