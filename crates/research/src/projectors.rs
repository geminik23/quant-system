use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime};
use qs_backtest::{
    CalendarFeatureKind, CalendarFeatureProjector, ConfiguredHistoricalBindings,
    ConfiguredNamedInputBinding, IanaTradingCalendar, SeriesId,
};
use qs_market_loader::{PriceBins, QuoteStatisticKind, QuoteStatisticsProjector, StoredTick};
use qs_strategy::{
    ConfiguredStrategyRequirements, MaterialLibrary, ParameterBinding, StrategyConfig,
};

use crate::{InputProjectorSnapshot, SeriesGeometry, StrategyFamily};

#[derive(Debug, Clone)]
pub struct CalendarProjectorSelection {
    pub name: String,
    pub source: String,
    pub timezone: String,
    pub session_open: NaiveTime,
    pub session_close: NaiveTime,
    pub holidays: BTreeSet<NaiveDate>,
    pub early_closes: BTreeMap<NaiveDate, NaiveTime>,
    pub kind: CalendarFeatureKind,
    pub opening_range_minutes: u32,
    pub child_seconds: u64,
    pub alignment_offset_seconds: i64,
    pub maximum_history: usize,
}

#[derive(Debug, Clone)]
pub struct QuoteProjectorSelection {
    pub name: String,
    pub source: String,
    pub dataset_reference: String,
    pub ticks: Arc<[StoredTick]>,
    pub kind: QuoteStatisticKind,
    pub captured_level: Option<f64>,
    pub bins: Option<PriceBins>,
    pub maximum_rows_per_query: usize,
}

#[derive(Debug, Clone)]
pub enum NamedProjectorSelection {
    Calendar(CalendarProjectorSelection),
    Quote(QuoteProjectorSelection),
}

impl NamedProjectorSelection {
    fn name(&self) -> &str {
        match self {
            Self::Calendar(selection) => &selection.name,
            Self::Quote(selection) => &selection.name,
        }
    }

    fn binding(&self) -> Result<ConfiguredNamedInputBinding, String> {
        match self {
            Self::Calendar(selection) => {
                let calendar = IanaTradingCalendar::new(
                    &selection.timezone,
                    selection.session_open,
                    selection.session_close,
                    selection.holidays.clone(),
                    selection.early_closes.clone(),
                )
                .map_err(|error| error.to_string())?;
                let projector = CalendarFeatureProjector::new(
                    SeriesId::new(&selection.source).map_err(|error| error.to_string())?,
                    calendar,
                    selection.kind,
                    selection.opening_range_minutes,
                    selection.child_seconds,
                    selection.alignment_offset_seconds,
                    selection.maximum_history,
                )
                .map_err(|error| error.to_string())?;
                Ok(ConfiguredNamedInputBinding::new(
                    selection.name.clone(),
                    Box::new(projector),
                ))
            }
            Self::Quote(selection) => {
                let projector = QuoteStatisticsProjector::new(
                    SeriesId::new(&selection.source).map_err(|error| error.to_string())?,
                    selection.ticks.clone(),
                    selection.kind,
                    selection.captured_level,
                    selection.bins,
                    selection.maximum_rows_per_query,
                )
                .map_err(|error| error.to_string())?;
                Ok(ConfiguredNamedInputBinding::new(
                    selection.name.clone(),
                    Box::new(projector),
                ))
            }
        }
    }

    fn snapshot(&self, symbol: &str) -> InputProjectorSnapshot {
        match self {
            Self::Calendar(selection) => InputProjectorSnapshot {
                symbol: symbol.into(),
                name: selection.name.clone(),
                kind: "calendar".into(),
                configuration: serde_json::json!({
                    "source": selection.source,
                    "timezone": selection.timezone,
                    "session_open": selection.session_open,
                    "session_close": selection.session_close,
                    "holidays": selection.holidays,
                    "early_closes": selection.early_closes,
                    "feature": selection.kind,
                    "opening_range_minutes": selection.opening_range_minutes,
                    "child_seconds": selection.child_seconds,
                    "alignment_offset_seconds": selection.alignment_offset_seconds,
                    "maximum_history": selection.maximum_history,
                }),
            },
            Self::Quote(selection) => InputProjectorSnapshot {
                symbol: symbol.into(),
                name: selection.name.clone(),
                kind: "quote_statistics".into(),
                configuration: serde_json::json!({
                    "source": selection.source,
                    "dataset_reference": selection.dataset_reference,
                    "stored_rows": selection.ticks.len(),
                    "feature": selection.kind,
                    "captured_level": selection.captured_level,
                    "bins": selection.bins,
                    "maximum_rows_per_query": selection.maximum_rows_per_query,
                    "identity_policy": "timestamp_source_ordinal_with_optional_provider_sequence",
                }),
            },
        }
    }
}

/// Adds fresh typed calendar and quote projectors to an existing configured family.
pub struct ProjectedStrategyFamily<F> {
    inner: F,
    selections: Vec<NamedProjectorSelection>,
}

impl<F> ProjectedStrategyFamily<F> {
    pub fn new(inner: F, selections: Vec<NamedProjectorSelection>) -> Result<Self, String> {
        let mut names = BTreeSet::new();
        for selection in &selections {
            let name = selection.name();
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
                || !names.insert(name.to_owned())
            {
                return Err("projector names must be unique bounded ASCII identifiers".into());
            }
        }
        Ok(Self { inner, selections })
    }

    pub fn inner(&self) -> &F {
        &self.inner
    }
}

impl<F> StrategyFamily for ProjectedStrategyFamily<F>
where
    F: StrategyFamily,
{
    type Params = F::Params;

    fn family_id(&self) -> &str {
        self.inner.family_id()
    }

    fn points(&self) -> Vec<Self::Params> {
        self.inner.points()
    }

    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding {
        self.inner.parameter_binding(point)
    }

    fn config(&self, point: &Self::Params) -> StrategyConfig {
        self.inner.config(point)
    }

    fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<SeriesGeometry> {
        self.inner.geometry(symbol, point)
    }

    fn bindings(
        &self,
        symbol: &str,
        point: &Self::Params,
        requirements: &ConfiguredStrategyRequirements,
    ) -> Result<ConfiguredHistoricalBindings, String> {
        let base = self.inner.bindings(symbol, point, requirements)?;
        let (sources, mut named, volume) = base.into_parts();
        let required = requirements
            .named_inputs
            .iter()
            .map(|requirement| requirement.name.as_str())
            .collect::<BTreeSet<_>>();
        for selection in &self.selections {
            if !required.contains(selection.name()) {
                return Err(format!(
                    "named projector '{}' is not required by the strategy",
                    selection.name()
                ));
            }
            if !named
                .iter()
                .any(|binding| binding.name() == selection.name())
            {
                named.push(selection.binding()?);
            }
        }
        Ok(ConfiguredHistoricalBindings::new(sources, named, volume))
    }

    fn library(&self) -> MaterialLibrary {
        self.inner.library()
    }

    fn input_projector_recipe(
        &self,
        symbol: &str,
        _point: &Self::Params,
    ) -> Vec<InputProjectorSnapshot> {
        self.selections
            .iter()
            .map(|selection| selection.snapshot(symbol))
            .collect()
    }
}
