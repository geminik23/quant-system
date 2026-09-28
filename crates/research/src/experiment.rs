use crate::{ResearchError, ResearchRow, RunRecipe, SplitAccessRecord};
use chrono::NaiveDateTime;
use qs_backtest::evaluation::PositionOutcome;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FeatureCacheKey {
    pub dataset_reference: String,
    pub symbol: String,
    pub from: NaiveDateTime,
    pub to: NaiveDateTime,
    pub source: String,
    pub price_basis: String,
    pub alignment_offset_seconds: i64,
    pub output: String,
    pub parameters: BTreeMap<String, String>,
    pub seed_policy: String,
    pub missing_policy: String,
    pub clock: String,
    pub availability_policy: String,
}
const MAX_CACHE_KEY_TEXT_BYTES: usize = 256;
const MAX_CACHE_PARAMETERS: usize = 64;
const CACHE_CONTAINER_OVERHEAD_PER_ENTRY: usize = 128;

#[derive(Debug, Clone)]
struct FeatureCacheEntry {
    values: Vec<Option<f64>>,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct FeatureCache {
    maximum_bytes: usize,
    used_bytes: usize,
    entries: BTreeMap<FeatureCacheKey, FeatureCacheEntry>,
    order: VecDeque<FeatureCacheKey>,
}
impl FeatureCache {
    pub fn new(maximum_bytes: usize) -> Result<Self, ResearchError> {
        if maximum_bytes == 0 {
            return Err(ResearchError::InvalidPlan(
                "cache bytes must be positive".into(),
            ));
        }
        Ok(Self {
            maximum_bytes,
            used_bytes: 0,
            entries: BTreeMap::new(),
            order: VecDeque::new(),
        })
    }
    pub fn entry_bytes_upper_bound(
        key: &FeatureCacheKey,
        value_len: usize,
    ) -> Result<usize, ResearchError> {
        validate_key(key)?;
        let value_bytes = value_len
            .checked_mul(std::mem::size_of::<Option<f64>>())
            .ok_or_else(|| {
                ResearchError::InvalidPlan("cache entry byte count overflowed".into())
            })?;
        key_owned_bytes_upper_bound(key)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<FeatureCacheEntry>()))
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .and_then(|bytes| bytes.checked_add(CACHE_CONTAINER_OVERHEAD_PER_ENTRY))
            .ok_or_else(|| ResearchError::InvalidPlan("cache entry byte count overflowed".into()))
    }

    pub fn insert(
        &mut self,
        mut key: FeatureCacheKey,
        mut value: Vec<Option<f64>>,
    ) -> Result<(), ResearchError> {
        compact_key(&mut key);
        value.shrink_to_fit();
        let bytes = Self::entry_bytes_upper_bound(&key, value.len())?;
        if bytes > self.maximum_bytes {
            return Err(ResearchError::InvalidPlan(
                "one cache entry exceeds cache budget".into(),
            ));
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.used_bytes = self
                .used_bytes
                .checked_sub(previous.retained_bytes)
                .ok_or_else(|| {
                    ResearchError::InvalidPlan("cache byte accounting underflowed".into())
                })?;
            self.order.retain(|existing| existing != &key)
        }
        while self
            .used_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.maximum_bytes)
        {
            let oldest = self.order.pop_front().ok_or_else(|| {
                ResearchError::InvalidPlan("cache eviction state is inconsistent".into())
            })?;
            let removed = self.entries.remove(&oldest).ok_or_else(|| {
                ResearchError::InvalidPlan("cache eviction entry is missing".into())
            })?;
            self.used_bytes = self
                .used_bytes
                .checked_sub(removed.retained_bytes)
                .ok_or_else(|| {
                    ResearchError::InvalidPlan("cache byte accounting underflowed".into())
                })?;
        }
        self.used_bytes = self
            .used_bytes
            .checked_add(bytes)
            .ok_or_else(|| ResearchError::InvalidPlan("cache byte count overflowed".into()))?;
        self.order.push_back(key.clone());
        self.entries.insert(
            key,
            FeatureCacheEntry {
                values: value,
                retained_bytes: bytes,
            },
        );
        Ok(())
    }
    pub fn get(&self, key: &FeatureCacheKey) -> Option<&[Option<f64>]> {
        self.entries.get(key).map(|entry| entry.values.as_slice())
    }
    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
fn validate_key(key: &FeatureCacheKey) -> Result<(), ResearchError> {
    let text = [
        key.dataset_reference.as_str(),
        key.symbol.as_str(),
        key.source.as_str(),
        key.price_basis.as_str(),
        key.output.as_str(),
        key.seed_policy.as_str(),
        key.missing_policy.as_str(),
        key.clock.as_str(),
        key.availability_policy.as_str(),
    ];
    if text.iter().any(|value| {
        value.is_empty()
            || value.len() > MAX_CACHE_KEY_TEXT_BYTES
            || value.chars().any(char::is_control)
    }) || key.parameters.len() > MAX_CACHE_PARAMETERS
        || key.parameters.iter().any(|(name, value)| {
            name.is_empty()
                || name.len() > MAX_CACHE_KEY_TEXT_BYTES
                || value.is_empty()
                || value.len() > MAX_CACHE_KEY_TEXT_BYTES
                || name.chars().any(char::is_control)
                || value.chars().any(char::is_control)
        })
        || key.to <= key.from
    {
        Err(ResearchError::InvalidPlan(
            "incomplete or oversized feature cache equality key".into(),
        ))
    } else {
        Ok(())
    }
}

fn compact_key(key: &mut FeatureCacheKey) {
    key.dataset_reference.shrink_to_fit();
    key.symbol.shrink_to_fit();
    key.source.shrink_to_fit();
    key.price_basis.shrink_to_fit();
    key.output.shrink_to_fit();
    key.seed_policy.shrink_to_fit();
    key.missing_policy.shrink_to_fit();
    key.clock.shrink_to_fit();
    key.availability_policy.shrink_to_fit();
    key.parameters = std::mem::take(&mut key.parameters)
        .into_iter()
        .map(|(mut name, mut value)| {
            name.shrink_to_fit();
            value.shrink_to_fit();
            (name, value)
        })
        .collect();
}

/// Produce one immutable market-only midpoint series and reuse it only under the full cache key.
pub fn cached_market_midpoints(
    cache: &mut FeatureCache,
    key: FeatureCacheKey,
    events: &[qs_backtest::data_feed::FeedEvent],
) -> Result<Vec<Option<f64>>, ResearchError> {
    validate_key(&key)?;
    if key.output != "midpoint" {
        return Err(ResearchError::InvalidPlan(
            "market midpoint producer requires output='midpoint'".into(),
        ));
    }
    if let Some(values) = cache.get(&key) {
        return Ok(values.to_vec());
    }
    let mut values = Vec::with_capacity(events.len());
    for event in events {
        if event.event.symbol() != key.symbol
            || event.available_at() < key.from
            || event.available_at() >= key.to
        {
            return Err(ResearchError::InvalidPlan(
                "market feature events differ from the cache dataset/range/symbol key".into(),
            ));
        }
        let quote = event.event.to_quote();
        values.push(
            (quote.bid.is_finite()
                && quote.ask.is_finite()
                && quote.bid > 0.0
                && quote.ask >= quote.bid)
                .then_some(quote.bid / 2.0 + quote.ask / 2.0),
        );
    }
    cache.insert(key.clone(), values.clone())?;
    Ok(values)
}

fn key_owned_bytes_upper_bound(key: &FeatureCacheKey) -> Option<usize> {
    let string_bytes = [
        key.dataset_reference.capacity(),
        key.symbol.capacity(),
        key.source.capacity(),
        key.price_basis.capacity(),
        key.output.capacity(),
        key.seed_policy.capacity(),
        key.missing_policy.capacity(),
        key.clock.capacity(),
        key.availability_policy.capacity(),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)?;
    let parameter_bytes = key
        .parameters
        .iter()
        .try_fold(0usize, |total, (name, value)| {
            total
                .checked_add(std::mem::size_of::<(String, String)>())
                .and_then(|total| total.checked_add(3 * std::mem::size_of::<usize>()))
                .and_then(|total| total.checked_add(name.capacity()))
                .and_then(|total| total.checked_add(value.capacity()))
        })?;
    std::mem::size_of::<FeatureCacheKey>()
        .checked_add(string_bytes)
        .and_then(|bytes| bytes.checked_add(parameter_bytes))
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceRecord {
    pub feature: String,
    pub value: Option<f64>,
    pub valid: bool,
    pub source: String,
    pub sample_at: NaiveDateTime,
    pub available_at: NaiveDateTime,
    pub predicate: Option<String>,
    pub event: Option<String>,
    pub capture: Option<String>,
    pub decision_id: Option<String>,
    pub command_id: Option<String>,
    pub position_id: Option<String>,
    pub entry_regime: Option<String>,
    pub fill_regime: Option<String>,
    pub hindsight_regime: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}
impl TraceLimits {
    pub fn new(max_records: usize, max_bytes: usize) -> Result<Self, ResearchError> {
        if max_records == 0 || max_bytes == 0 {
            Err(ResearchError::InvalidPlan(
                "trace limits must be positive".into(),
            ))
        } else {
            Ok(Self {
                max_records,
                max_bytes,
            })
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BoundedTrace {
    records: Vec<TraceRecord>,
    omitted_records: u64,
    retained_bytes: usize,
}
impl BoundedTrace {
    pub fn push(&mut self, record: TraceRecord, limits: TraceLimits) -> Result<(), ResearchError> {
        if record.feature.is_empty()
            || record.source.is_empty()
            || record.available_at < record.sample_at
        {
            return Err(ResearchError::InvalidPlan("invalid trace record".into()));
        }
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?
            .len();
        if self.records.len() == limits.max_records
            || self
                .retained_bytes
                .checked_add(bytes)
                .is_none_or(|v| v > limits.max_bytes)
        {
            self.omitted_records = self.omitted_records.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("trace omission count overflowed".into())
            })?;
            return Ok(());
        }
        self.retained_bytes += bytes;
        self.records.push(record);
        Ok(())
    }
    pub fn records(&self) -> &[TraceRecord] {
        &self.records
    }
    pub fn omitted_records(&self) -> u64 {
        self.omitted_records
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointDependency {
    pub experiment_id: Option<String>,
    pub caller_revision: String,
    pub dataset_reference: String,
    pub factory_revision: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletedRunCheckpoint {
    pub recipe: RunRecipe,
    pub row: ResearchRow,
    pub positions: Vec<PositionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchCheckpoint {
    pub dependency: CheckpointDependency,
    #[serde(default)]
    pub experiment_recipe: Option<crate::ExperimentRecipe>,
    #[serde(default)]
    pub candidate_recipes: BTreeMap<u64, crate::CandidateRecipe>,
    #[serde(default)]
    pub frozen_selection: Option<crate::FrozenSelection>,
    /// First candidate ordinal that had not been generated when the checkpoint was written.
    pub frontier: u64,
    #[serde(default)]
    pub completed_candidates: BTreeSet<u64>,
    pub completed_runs: BTreeSet<u64>,
    #[serde(default)]
    pub committed_runs: BTreeMap<u64, CompletedRunCheckpoint>,
    pub failures: BTreeMap<u64, String>,
    pub generated: u64,
    pub executed: u64,
    #[serde(default)]
    pub generation_exhaustive: bool,
    pub split_access: Vec<SplitAccessRecord>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointLimits {
    pub max_bytes: usize,
    pub max_records: usize,
}
impl CheckpointLimits {
    pub fn new(max_bytes: usize, max_records: usize) -> Result<Self, ResearchError> {
        if max_bytes == 0 || max_records == 0 {
            Err(ResearchError::InvalidPlan(
                "checkpoint limits must be positive".into(),
            ))
        } else {
            Ok(Self {
                max_bytes,
                max_records,
            })
        }
    }
}
impl SearchCheckpoint {
    pub fn validate(&self, limits: CheckpointLimits) -> Result<(), ResearchError> {
        if self.dependency.caller_revision.is_empty()
            || self.dependency.dataset_reference.is_empty()
        {
            return Err(ResearchError::InvalidPlan(
                "checkpoint dependencies are incomplete".into(),
            ));
        }
        let records = self
            .candidate_recipes
            .len()
            .checked_add(self.completed_candidates.len())
            .and_then(|value| value.checked_add(self.completed_runs.len()))
            .and_then(|value| value.checked_add(self.committed_runs.len()))
            .and_then(|value| value.checked_add(self.failures.len()))
            .and_then(|value| value.checked_add(self.split_access.len()))
            .ok_or_else(|| {
                ResearchError::InvalidPlan("checkpoint record count overflowed".into())
            })?;
        if records > limits.max_records {
            return Err(ResearchError::InvalidPlan(
                "checkpoint record count exceeds limit".into(),
            ));
        }
        if self
            .candidate_recipes
            .iter()
            .any(|(ordinal, candidate)| *ordinal != candidate.ordinal || *ordinal >= self.frontier)
        {
            return Err(ResearchError::InvalidPlan(
                "checkpoint candidate recipe ordinal is inconsistent".into(),
            ));
        }

        if self
            .completed_runs
            .iter()
            .any(|run| self.failures.contains_key(run))
        {
            return Err(ResearchError::InvalidPlan(
                "checkpoint run cannot be completed and failed".into(),
            ));
        }
        if self
            .committed_runs
            .keys()
            .any(|run| !self.completed_runs.contains(run))
        {
            return Err(ResearchError::InvalidPlan(
                "checkpoint outcome must belong to a completed run".into(),
            ));
        }
        if self
            .completed_candidates
            .iter()
            .any(|candidate| *candidate >= self.frontier)
        {
            return Err(ResearchError::InvalidPlan(
                "completed candidate must precede the generation frontier".into(),
            ));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?
            .len();
        if bytes > limits.max_bytes {
            return Err(ResearchError::InvalidPlan(
                "checkpoint bytes exceed limit".into(),
            ));
        }
        Ok(())
    }
    pub fn encode(&self, limits: CheckpointLimits) -> Result<Vec<u8>, ResearchError> {
        self.validate(limits)?;
        serde_json::to_vec(self).map_err(|error| ResearchError::InvalidPlan(error.to_string()))
    }
    pub fn decode(
        bytes: &[u8],
        limits: CheckpointLimits,
        expected: &CheckpointDependency,
    ) -> Result<Self, ResearchError> {
        if bytes.len() > limits.max_bytes {
            return Err(ResearchError::InvalidPlan(
                "checkpoint bytes exceed limit".into(),
            ));
        }
        let value: Self = serde_json::from_slice(bytes)
            .map_err(|error| ResearchError::InvalidPlan(format!("invalid checkpoint: {error}")))?;
        value.validate(limits)?;
        if &value.dependency != expected {
            return Err(ResearchError::InvalidPlan(
                "checkpoint dependencies changed".into(),
            ));
        }
        Ok(value)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcentrationBucket {
    pub key: String,
    pub positions: usize,
    pub outcome: f64,
    pub share_of_absolute_outcome: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectedCandidateEvidence {
    pub by_symbol: Vec<ConcentrationBucket>,
    pub by_period: Vec<ConcentrationBucket>,
    pub by_regime: Vec<ConcentrationBucket>,
    pub uncertainty: UncertaintyAssessment,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum UncertaintyAssessment {
    Completed {
        method: String,
        assumptions: String,
        limitations: String,
    },
    Incomplete {
        reason: String,
    },
}
pub fn selected_candidate_evidence(
    positions: &[PositionOutcome],
    regime_tag: &str,
    uncertainty: UncertaintyAssessment,
) -> Result<SelectedCandidateEvidence, ResearchError> {
    if regime_tag.is_empty() {
        return Err(ResearchError::InvalidPlan(
            "regime tag must not be empty".into(),
        ));
    }
    let mut symbol = BTreeMap::new();
    let mut period = BTreeMap::new();
    let mut regime = BTreeMap::new();
    for position in positions {
        if !position.outcome.is_finite() {
            return Err(ResearchError::InvalidPlan(
                "position outcome is non-finite".into(),
            ));
        }
        accumulate(
            &mut symbol,
            position.dimensions.symbol.clone(),
            position.outcome,
        );
        let period_key = chrono::DateTime::from_timestamp_millis(position.ordinal)
            .map(|value| value.format("%Y-%m").to_string())
            .unwrap_or_else(|| "unknown".into());
        accumulate(&mut period, period_key, position.outcome);
        let regime_key = position
            .dimensions
            .tags
            .get(regime_tag)
            .cloned()
            .unwrap_or_else(|| "unavailable".into());
        accumulate(&mut regime, regime_key, position.outcome);
    }
    let total = positions
        .iter()
        .map(|position| position.outcome.abs())
        .sum::<f64>();
    Ok(SelectedCandidateEvidence {
        by_symbol: buckets(symbol, total),
        by_period: buckets(period, total),
        by_regime: buckets(regime, total),
        uncertainty,
    })
}
fn accumulate(map: &mut BTreeMap<String, (usize, f64)>, key: String, outcome: f64) {
    let value = map.entry(key).or_default();
    value.0 += 1;
    value.1 += outcome
}
fn buckets(map: BTreeMap<String, (usize, f64)>, total: f64) -> Vec<ConcentrationBucket> {
    map.into_iter()
        .map(|(key, (positions, outcome))| ConcentrationBucket {
            key,
            positions,
            outcome,
            share_of_absolute_outcome: (total > 0.0).then_some(outcome.abs() / total),
        })
        .collect()
}
