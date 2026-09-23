//! Historical series geometry for configured logical sources and the warmup it implies.
//!
//! A caller declares only which symbol, timeframe, price basis, and alignment back each logical source. Warmup and retention are derived from the compiled strategy's own completed-bar requirements, so an in-process search and a service request bind the same strategy identically.

use chrono::NaiveDateTime;
use qs_strategy::{ConfiguredStrategyRequirements, SourceId};

use super::{
    BarSeriesSpec, ConfiguredHistoricalBindings, ConfiguredSourceBinding,
    HistoricalVolumeProjection, MissingIntervalPolicy, PriceBasis, SeriesId, SeriesRequirement,
    Timeframe, WarmupRequirement,
};

/// Historical series geometry for one configured logical source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesGeometry {
    pub source: SourceId,
    pub symbol: String,
    pub timeframe: Timeframe,
    pub price_basis: PriceBasis,
    pub alignment_offset_seconds: i32,
}

impl SeriesGeometry {
    pub fn new(
        source: SourceId,
        symbol: impl Into<String>,
        timeframe: Timeframe,
        price_basis: PriceBasis,
        alignment_offset_seconds: i32,
    ) -> Self {
        Self {
            source,
            symbol: symbol.into(),
            timeframe,
            price_basis,
            alignment_offset_seconds,
        }
    }
}

/// Geometry that cannot be bound or whose warmup cannot be represented.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SeriesGeometryError(String);

impl ConfiguredHistoricalBindings {
    /// Bind each declared geometry to a series whose warmup and retention come from the compiled requirements, with exact tick-count volume and no named inputs.
    pub fn from_geometry(
        geometry: Vec<SeriesGeometry>,
        requirements: &ConfiguredStrategyRequirements,
    ) -> Result<Self, SeriesGeometryError> {
        let invalid = |error: &dyn std::fmt::Display| SeriesGeometryError(error.to_string());
        let mut sources = Vec::with_capacity(geometry.len());
        for item in geometry {
            let required = requirements
                .completed_bars
                .iter()
                .find(|requirement| requirement.source == item.source)
                .map_or(0, |requirement| requirement.required_lookback);
            let series_id = SeriesId::new(item.source.as_str()).map_err(|error| invalid(&error))?;
            let requirement = SeriesRequirement::new(
                series_id,
                item.symbol,
                item.timeframe,
                item.price_basis,
                WarmupRequirement::bars(required).map_err(|error| invalid(&error))?,
            )
            .map_err(|error| invalid(&error))?;
            let series = BarSeriesSpec::new(
                requirement,
                required.max(1),
                item.alignment_offset_seconds,
                MissingIntervalPolicy::Skip,
            )
            .map_err(|error| invalid(&error))?;
            sources.push(ConfiguredSourceBinding::new(item.source, series));
        }
        Ok(Self::new(
            sources,
            Vec::new(),
            HistoricalVolumeProjection::TickCountExact,
        ))
    }

    /// Earliest time from which every bound series completes its warmup by `from`, starting on each source's aligned bucket boundary so dense data cannot make a strategy ready before `from`.
    pub fn warmup_start(&self, from: NaiveDateTime) -> Result<NaiveDateTime, SeriesGeometryError> {
        let from_seconds = from.and_utc().timestamp();
        let mut start = from;
        for binding in self.sources() {
            let series = binding.series();
            let requirement = series.requirement();
            let bars = i64::try_from(requirement.warmup().required_bars())
                .map_err(|_| SeriesGeometryError("warmup bars do not fit i64".into()))?;
            if bars == 0 {
                continue;
            }
            let duration = i64::try_from(requirement.timeframe().duration_seconds())
                .map_err(|_| SeriesGeometryError("timeframe duration does not fit i64".into()))?;
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
                        .ok_or_else(|| SeriesGeometryError("warmup span overflowed".into()))?,
                )
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
                .map(|value| value.naive_utc())
                .ok_or_else(|| {
                    SeriesGeometryError("warmup start is outside timestamp bounds".into())
                })?;
            start = start.min(source_start);
        }
        Ok(start)
    }
}
