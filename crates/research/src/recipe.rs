use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use qs_backtest::ConfiguredHistoricalBindings;
use qs_strategy::ParameterValue;
use serde::{Deserialize, Deserializer, Serialize};

use crate::ResearchError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ExperimentId(String);

impl ExperimentId {
    pub fn new(value: impl Into<String>) -> Result<Self, ResearchError> {
        let value = value.into();
        if value.is_empty() || value.len() > 64 || !value.is_ascii() || value.trim() != value {
            return Err(ResearchError::InvalidPlan("invalid experiment ID".into()));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExperimentId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExperimentOptions {
    pub experiment_id: Option<ExperimentId>,
    pub caller_revision: Option<String>,
    pub dataset_reference: Option<String>,
}

impl ExperimentOptions {
    pub(crate) fn validate(&self) -> Result<(), ResearchError> {
        for (name, value) in [
            ("caller revision", self.caller_revision.as_deref()),
            ("dataset reference", self.dataset_reference.as_deref()),
        ] {
            if let Some(value) = value
                && (value.is_empty()
                    || value.len() > 256
                    || value.trim() != value
                    || value.chars().any(char::is_control))
            {
                return Err(ResearchError::InvalidPlan(format!("invalid {name}")));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SeriesBindingSnapshot {
    pub source: String,
    pub symbol: String,
    pub timeframe_seconds: u64,
    pub price_basis: String,
    pub alignment_offset_seconds: i64,
    pub warmup_bars: usize,
    pub retained_bars: usize,
}

pub fn snapshot_bindings(bindings: &ConfiguredHistoricalBindings) -> Vec<SeriesBindingSnapshot> {
    bindings
        .sources()
        .iter()
        .map(|binding| {
            let series = binding.series();
            let requirement = series.requirement();
            SeriesBindingSnapshot {
                source: binding.source().as_str().to_owned(),
                symbol: requirement.symbol().to_owned(),
                timeframe_seconds: requirement.timeframe().duration_seconds(),
                price_basis: format!("{:?}", requirement.price_basis()).to_lowercase(),
                alignment_offset_seconds: series.alignment_offset_seconds(),
                warmup_bars: requirement.warmup().required_bars(),
                retained_bars: series.retained_bars(),
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CandidateRecipe {
    pub ordinal: u64,
    pub family_id: String,
    pub parameters: BTreeMap<String, ParameterValue>,
    pub document: serde_json::Value,
    pub series_by_symbol: BTreeMap<String, Vec<SeriesBindingSnapshot>>,
    pub admission_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EndpointBounds {
    HalfOpen,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ExperimentRecipe {
    pub experiment_id: Option<ExperimentId>,
    pub caller_revision: Option<String>,
    pub dataset_reference: Option<String>,
    pub endpoint_bounds: EndpointBounds,
    pub ordered_symbols: Vec<String>,
    pub decision_latency_ms: u64,
    pub portfolio: Option<serde_json::Value>,
    pub backtest: serde_json::Value,
    pub future: serde_json::Value,
    pub evaluation: serde_json::Value,
    pub retention: serde_json::Value,
    pub research_retention: serde_json::Value,
    pub profiles: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UnavailableCoverage {
    FirstReady,
    PerSourceValidity,
    MissingBuckets,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunCoverage {
    pub first_input_at: Option<NaiveDateTime>,
    pub last_input_at: Option<NaiveDateTime>,
    pub processed_primary_events: u64,
    pub permitted_from: NaiveDateTime,
    pub permitted_to: NaiveDateTime,
    pub first_ready_at: Option<NaiveDateTime>,
    pub unavailable: Vec<UnavailableCoverage>,
    pub forced_closes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunRecipe {
    pub ordinal: u64,
    pub candidate_ordinal: u64,
    pub window: String,
    pub symbol: String,
    pub data_mode: String,
    pub run_tags: BTreeMap<String, String>,
    pub from: NaiveDateTime,
    pub to: NaiveDateTime,
    pub coverage: Option<RunCoverage>,
}
