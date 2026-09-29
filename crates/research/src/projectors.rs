use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::NaiveDateTime;
use qs_backtest::{
    CalendarAdmissionLimits, CalendarInputSpec, ConfiguredCalendarFeatureProjector,
    ConfiguredHistoricalBindings, ConfiguredNamedInputBinding, ConfiguredTradingCalendar,
    HistoricalNamedInputProjector, NamedInputProjectionContext, NamedInputProjectionError,
    ProjectedNamedInput, SeriesId, TradingCalendarSpec,
};
use qs_market_loader::{PriceBins, QuoteStatisticKind, QuoteStatisticsProjector, StoredTick};
use qs_strategy::{
    ConfiguredStrategyRequirements, MaterialLibrary, ParameterBinding, ScalarType, StrategyConfig,
    Value, ValueType,
};
use serde::{Deserialize, Serialize};

use crate::{InputProjectorSnapshot, SeriesGeometry, StrategyFamily};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarProjectorSelection {
    pub name: String,
    pub source: String,
    pub calendar: TradingCalendarSpec,
    pub input: CalendarInputSpec,
    #[serde(default)]
    pub limits: CalendarAdmissionLimits,
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
pub struct CachedMidpointProjectorSelection {
    pub name: String,
    pub source: String,
    pub view_reference: String,
    pub samples: Arc<BTreeMap<chrono::NaiveDateTime, crate::CachedFeatureSample>>,
}

#[derive(Debug, Clone)]
pub enum NamedProjectorSelection {
    Calendar(CalendarProjectorSelection),
    Quote(QuoteProjectorSelection),
    CachedMidpoint(CachedMidpointProjectorSelection),
}

#[derive(Clone)]
struct CachedMidpointProjector {
    series_id: SeriesId,
    samples: Arc<BTreeMap<chrono::NaiveDateTime, crate::CachedFeatureSample>>,
}

impl HistoricalNamedInputProjector for CachedMidpointProjector {
    fn output_type(&self) -> ValueType {
        ValueType::optional(ScalarType::Price)
    }

    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        let Some(bar) = context
            .closed_bars
            .iter()
            .find(|bar| bar.series_id() == &self.series_id)
        else {
            return Ok(ProjectedNamedInput {
                value: Value::Missing(ScalarType::Price),
                updated: false,
            });
        };
        let value = self
            .samples
            .get(&bar.open_time())
            .filter(|sample| sample.available_at <= context.observed_through)
            .and_then(|sample| sample.value)
            .map(Value::Price)
            .unwrap_or(Value::Missing(ScalarType::Price));
        Ok(ProjectedNamedInput {
            value,
            updated: true,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalInputsDocument {
    #[serde(default)]
    pub calendars: BTreeMap<String, TradingCalendarSpec>,
    #[serde(default)]
    pub inputs: Vec<CalendarInputDocument>,
    #[serde(default)]
    pub limits: CalendarAdmissionLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarInputDocument {
    pub name: String,
    pub source: String,
    pub calendar: String,
    pub input: CalendarInputSpec,
}

impl HistoricalInputsDocument {
    pub fn calendar_selections(&self) -> Result<Vec<NamedProjectorSelection>, String> {
        if self.calendars.is_empty() && !self.inputs.is_empty() {
            return Err("historical inputs require at least one calendar definition".into());
        }
        let mut ids = BTreeSet::new();
        for (id, calendar) in &self.calendars {
            if id != &calendar.id {
                return Err(format!(
                    "calendar map key '{id}' differs from calendar id '{}'",
                    calendar.id
                ));
            }
        }
        self.inputs
            .iter()
            .map(|document| {
                if !ids.insert(document.name.clone()) {
                    return Err(format!(
                        "duplicate historical input name '{}'",
                        document.name
                    ));
                }
                let calendar = self.calendars.get(&document.calendar).ok_or_else(|| {
                    format!(
                        "historical input '{}' references unknown calendar '{}'",
                        document.name, document.calendar
                    )
                })?;
                if document.input.calendar_id != document.calendar {
                    return Err(format!(
                        "historical input '{}' calendar reference differs from its input calendar_id",
                        document.name
                    ));
                }
                Ok(NamedProjectorSelection::Calendar(
                    CalendarProjectorSelection {
                        name: document.name.clone(),
                        source: document.source.clone(),
                        calendar: calendar.clone(),
                        input: document.input.clone(),
                        limits: self.limits,
                    },
                ))
            })
            .collect()
    }
}

impl NamedProjectorSelection {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Calendar(selection) => &selection.name,
            Self::Quote(selection) => &selection.name,
            Self::CachedMidpoint(selection) => &selection.name,
        }
    }

    pub(crate) fn history_start(
        &self,
        evaluation_start: NaiveDateTime,
    ) -> Result<NaiveDateTime, String> {
        match self {
            Self::Calendar(selection) => qs_backtest::ConfiguredCalendarInput {
                name: selection.name.clone(),
                source: selection.source.clone(),
                calendar: selection.calendar.clone(),
                input: selection.input.clone(),
                limits: selection.limits,
            }
            .history_start(evaluation_start)
            .map_err(|error| error.to_string()),
            Self::Quote(_) | Self::CachedMidpoint(_) => Ok(evaluation_start),
        }
    }

    pub(crate) fn binding(&self) -> Result<ConfiguredNamedInputBinding, String> {
        match self {
            Self::Calendar(selection) => {
                let calendar =
                    ConfiguredTradingCalendar::new(selection.calendar.clone(), selection.limits)
                        .map_err(|error| error.to_string())?;
                let projector = ConfiguredCalendarFeatureProjector::new(
                    SeriesId::new(&selection.source).map_err(|error| error.to_string())?,
                    calendar,
                    selection.input.clone(),
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
            Self::CachedMidpoint(selection) => Ok(ConfiguredNamedInputBinding::new(
                selection.name.clone(),
                Box::new(CachedMidpointProjector {
                    series_id: SeriesId::new(&selection.source)
                        .map_err(|error| error.to_string())?,
                    samples: selection.samples.clone(),
                }),
            )),
        }
    }

    pub(crate) fn snapshot(&self, symbol: &str, owner: &str) -> InputProjectorSnapshot {
        match self {
            Self::Calendar(selection) => InputProjectorSnapshot {
                owner: owner.into(),
                symbol: symbol.into(),
                name: selection.name.clone(),
                kind: "calendar".into(),
                configuration: serde_json::json!({
                    "source": selection.source,
                    "calendar": selection.calendar,
                    "input": selection.input,
                    "limits": selection.limits,
                }),
            },
            Self::Quote(selection) => InputProjectorSnapshot {
                owner: owner.into(),
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
            Self::CachedMidpoint(selection) => InputProjectorSnapshot {
                owner: owner.into(),
                symbol: symbol.into(),
                name: selection.name.clone(),
                kind: "cached_midpoint".into(),
                configuration: serde_json::json!({
                    "source": selection.source,
                    "view_reference": selection.view_reference,
                    "sample_count": selection.samples.len(),
                    "availability_policy": "actual",
                }),
            },
        }
    }

    pub(crate) fn from_snapshot(snapshot: &InputProjectorSnapshot) -> Result<Self, String> {
        match snapshot.kind.as_str() {
            "calendar" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct CalendarSnapshot {
                    source: String,
                    calendar: TradingCalendarSpec,
                    input: CalendarInputSpec,
                    limits: CalendarAdmissionLimits,
                }
                let value: CalendarSnapshot = serde_json::from_value(snapshot.configuration.clone())
                    .map_err(|error| format!("calendar projector snapshot: {error}"))?;
                Ok(Self::Calendar(CalendarProjectorSelection {
                    name: snapshot.name.clone(),
                    source: value.source,
                    calendar: value.calendar,
                    input: value.input,
                    limits: value.limits,
                }))
            }
            "quote_statistics" => Err(
                "quote-statistics recipe rerun requires the admitted ordered dataset and cannot be reconstructed from row metadata alone".into(),
            ),
            "cached_midpoint" => Err(
                "cached-midpoint recipe rerun requires a newly admitted immutable market view".into(),
            ),
            other => Err(format!("unknown input projector kind '{other}'")),
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
                continue;
            }
            if named
                .iter()
                .any(|binding| binding.name() == selection.name())
            {
                return Err(format!(
                    "named input '{}' has more than one projector",
                    selection.name()
                ));
            }
            named.push(selection.binding()?);
        }
        Ok(ConfiguredHistoricalBindings::new(sources, named, volume))
    }

    fn history_start(
        &self,
        symbol: &str,
        point: &Self::Params,
        evaluation_start: NaiveDateTime,
    ) -> Result<NaiveDateTime, String> {
        let mut start = self.inner.history_start(symbol, point, evaluation_start)?;
        for selection in &self.selections {
            start = start.min(selection.history_start(evaluation_start)?);
        }
        Ok(start)
    }

    fn history_start_for_requirements(
        &self,
        symbol: &str,
        point: &Self::Params,
        evaluation_start: NaiveDateTime,
        requirements: &ConfiguredStrategyRequirements,
    ) -> Result<NaiveDateTime, String> {
        let required = requirements
            .named_inputs
            .iter()
            .map(|requirement| requirement.name.as_str())
            .collect::<BTreeSet<_>>();
        let mut start = self.inner.history_start_for_requirements(
            symbol,
            point,
            evaluation_start,
            requirements,
        )?;
        for selection in self
            .selections
            .iter()
            .filter(|selection| required.contains(selection.name()))
        {
            start = start.min(selection.history_start(evaluation_start)?);
        }
        Ok(start)
    }

    fn library(&self) -> MaterialLibrary {
        self.inner.library()
    }

    fn input_projector_recipe(
        &self,
        symbol: &str,
        point: &Self::Params,
    ) -> Vec<InputProjectorSnapshot> {
        let mut snapshots = self.inner.input_projector_recipe(symbol, point);
        snapshots.extend(
            self.selections
                .iter()
                .map(|selection| selection.snapshot(symbol, "configured")),
        );
        snapshots
    }
}
