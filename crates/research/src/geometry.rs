use qs_backtest::{
    BarSeriesSpec, ConfiguredHistoricalBindings, ConfiguredSourceBinding,
    HistoricalVolumeProjection, MissingIntervalPolicy, PriceBasis, SeriesId, SeriesRequirement,
    Timeframe, WarmupRequirement,
};
use qs_strategy::{ConfiguredStrategyRequirements, SourceId};

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

pub(crate) fn bindings_from_geometry(
    geometry: Vec<SeriesGeometry>,
    requirements: &ConfiguredStrategyRequirements,
) -> Result<ConfiguredHistoricalBindings, String> {
    let mut sources = Vec::with_capacity(geometry.len());
    for item in geometry {
        let required = requirements
            .completed_bars
            .iter()
            .find(|requirement| requirement.source == item.source)
            .map_or(0, |requirement| requirement.required_lookback);
        let series_id = SeriesId::new(item.source.as_str()).map_err(|error| error.to_string())?;
        let requirement = SeriesRequirement::new(
            series_id,
            item.symbol,
            item.timeframe,
            item.price_basis,
            WarmupRequirement::bars(required).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        let series = BarSeriesSpec::new(
            requirement,
            required.max(1),
            item.alignment_offset_seconds,
            MissingIntervalPolicy::Skip,
        )
        .map_err(|error| error.to_string())?;
        sources.push(ConfiguredSourceBinding::new(item.source, series));
    }
    Ok(ConfiguredHistoricalBindings::new(
        sources,
        Vec::new(),
        HistoricalVolumeProjection::TickCountExact,
    ))
}
