//! Validated domain values for historical strategy descriptions and decisions.

use std::collections::HashSet;
use std::fmt;

use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Deserializer, Serialize};

use crate::profile::RawSignal;
use crate::report::BacktestResult;

use super::config::{PriceBasis, SeriesId, StrategyRetentionLimits, Timeframe, WarmupRequirement};

pub const MAX_STRATEGY_ID_BYTES: usize = 64;
pub const MAX_STRATEGY_REVISION_BYTES: usize = 64;
pub const MAX_STRATEGY_TITLE_BYTES: usize = 256;
pub const MAX_INSTRUMENT_BYTES: usize = 64;
pub const MAX_TRADE_ID_BYTES: usize = 128;
pub const MAX_DECISION_LATENCY_MS: u64 = 86_400_000;

/// Errors returned while validating historical strategy domain values.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StrategyDomainError {
    #[error("strategy ID must contain 1 to {MAX_STRATEGY_ID_BYTES} ASCII identifier bytes")]
    InvalidStrategyId,
    #[error(
        "strategy revision must contain 1 to {MAX_STRATEGY_REVISION_BYTES} ASCII identifier bytes"
    )]
    InvalidRevision,
    #[error(
        "strategy title must contain 1 to {MAX_STRATEGY_TITLE_BYTES} trimmed non-control bytes"
    )]
    InvalidTitle,
    #[error("strategy requirements must declare at least one instrument")]
    MissingInstruments,
    #[error("strategy requirements must declare at least one series")]
    MissingSeries,
    #[error("invalid instrument symbol '{symbol}'")]
    InvalidInstrument { symbol: String },
    #[error("instrument symbol '{symbol}' is declared more than once")]
    DuplicateInstrument { symbol: String },
    #[error("series ID '{series_id}' is declared more than once")]
    DuplicateSeriesId { series_id: SeriesId },
    #[error("series '{series_id}' references undeclared instrument '{symbol}'")]
    UndeclaredSeriesInstrument { series_id: SeriesId, symbol: String },
    #[error(
        "series '{series_id}' duplicates another symbol, timeframe, and price-basis requirement"
    )]
    DuplicateSeriesDefinition { series_id: SeriesId },
    #[error("decision latency {value} ms exceeds the maximum {MAX_DECISION_LATENCY_MS} ms")]
    DecisionLatencyTooLarge { value: u64 },
    #[error(
        "decision timestamp {timestamp} plus {latency_ms} ms is outside the supported time range"
    )]
    DecisionTimestampOverflow {
        timestamp: NaiveDateTime,
        latency_ms: u64,
    },
    #[error("decision reason must contain 1 to {maximum} trimmed non-control bytes")]
    InvalidDecisionReason { maximum: usize },
    #[error("related trade ID must contain 1 to {MAX_TRADE_ID_BYTES} trimmed non-control bytes")]
    InvalidTradeId,
    #[error("decision returned {actual} signals, exceeding the callback limit {maximum}")]
    TooManySignals { actual: usize, maximum: usize },
    #[error("decision sequence {current} must be greater than the prior sequence {previous}")]
    NonMonotonicDecisionSequence { previous: u64, current: u64 },
    #[error("decision omitted counter overflowed")]
    OmittedCounterOverflow,
}

/// Stable caller-supplied strategy identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct StrategyId(String);

impl StrategyId {
    pub fn new(value: impl Into<String>) -> Result<Self, StrategyDomainError> {
        let value = value.into();
        if valid_identifier(&value, MAX_STRATEGY_ID_BYTES) {
            Ok(Self(value))
        } else {
            Err(StrategyDomainError::InvalidStrategyId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StrategyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for StrategyId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Human-readable identity and explicit revision for one strategy variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StrategyDescriptor {
    id: StrategyId,
    revision: String,
    title: String,
}

impl StrategyDescriptor {
    pub fn new(
        id: StrategyId,
        revision: impl Into<String>,
        title: impl Into<String>,
    ) -> Result<Self, StrategyDomainError> {
        let revision = revision.into();
        let title = title.into();
        if !valid_identifier(&revision, MAX_STRATEGY_REVISION_BYTES) {
            return Err(StrategyDomainError::InvalidRevision);
        }
        if !valid_text(&title, MAX_STRATEGY_TITLE_BYTES) {
            return Err(StrategyDomainError::InvalidTitle);
        }
        Ok(Self {
            id,
            revision,
            title,
        })
    }

    pub fn id(&self) -> &StrategyId {
        &self.id
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn title(&self) -> &str {
        &self.title
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyDescriptorDef {
    id: StrategyId,
    revision: String,
    title: String,
}

impl<'de> Deserialize<'de> for StrategyDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyDescriptorDef::deserialize(deserializer)?;
        Self::new(value.id, value.revision, value.title).map_err(serde::de::Error::custom)
    }
}

/// One required closed-bar series and its authoritative warmup count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SeriesRequirement {
    id: SeriesId,
    symbol: String,
    timeframe: Timeframe,
    price_basis: PriceBasis,
    warmup: WarmupRequirement,
}

impl SeriesRequirement {
    pub fn new(
        id: SeriesId,
        symbol: impl Into<String>,
        timeframe: Timeframe,
        price_basis: PriceBasis,
        warmup: WarmupRequirement,
    ) -> Result<Self, StrategyDomainError> {
        let symbol = symbol.into();
        validate_instrument(&symbol)?;
        Ok(Self {
            id,
            symbol,
            timeframe,
            price_basis,
            warmup,
        })
    }

    pub fn id(&self) -> &SeriesId {
        &self.id
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    pub fn price_basis(&self) -> PriceBasis {
        self.price_basis
    }

    pub fn warmup(&self) -> WarmupRequirement {
        self.warmup
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesRequirementDef {
    id: SeriesId,
    symbol: String,
    timeframe: Timeframe,
    price_basis: PriceBasis,
    warmup: WarmupRequirement,
}

impl<'de> Deserialize<'de> for SeriesRequirement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SeriesRequirementDef::deserialize(deserializer)?;
        Self::new(
            value.id,
            value.symbol,
            value.timeframe,
            value.price_basis,
            value.warmup,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Explicit historical inputs and runtime capabilities required by a strategy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StrategyRequirements {
    instruments: Vec<String>,
    series: Vec<SeriesRequirement>,
    decision_latency_ms: u64,
    needs_tick_execution: bool,
    needs_execution_feedback: bool,
}

impl StrategyRequirements {
    pub fn new(
        instruments: Vec<String>,
        series: Vec<SeriesRequirement>,
        decision_latency_ms: u64,
        needs_tick_execution: bool,
        needs_execution_feedback: bool,
    ) -> Result<Self, StrategyDomainError> {
        validate_requirements(&instruments, &series, decision_latency_ms)?;
        Ok(Self {
            instruments,
            series,
            decision_latency_ms,
            needs_tick_execution,
            needs_execution_feedback,
        })
    }

    pub fn instruments(&self) -> &[String] {
        &self.instruments
    }

    pub fn series(&self) -> &[SeriesRequirement] {
        &self.series
    }

    pub fn decision_latency_ms(&self) -> u64 {
        self.decision_latency_ms
    }

    pub fn needs_tick_execution(&self) -> bool {
        self.needs_tick_execution
    }

    pub fn needs_execution_feedback(&self) -> bool {
        self.needs_execution_feedback
    }

    /// Overall warmup is complete only when every declared series is ready.
    pub fn warmup_complete<F>(&self, mut available_bars: F) -> bool
    where
        F: FnMut(&SeriesId) -> usize,
    {
        self.series.iter().all(|requirement| {
            available_bars(requirement.id()) >= requirement.warmup().required_bars()
        })
    }

    pub fn effective_timestamp(
        &self,
        decision_timestamp: NaiveDateTime,
    ) -> Result<NaiveDateTime, StrategyDomainError> {
        let latency = i64::try_from(self.decision_latency_ms)
            .expect("validated strategy latency always fits i64");
        decision_timestamp
            .checked_add_signed(Duration::milliseconds(latency))
            .ok_or(StrategyDomainError::DecisionTimestampOverflow {
                timestamp: decision_timestamp,
                latency_ms: self.decision_latency_ms,
            })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrategyRequirementsDef {
    instruments: Vec<String>,
    series: Vec<SeriesRequirement>,
    decision_latency_ms: u64,
    needs_tick_execution: bool,
    needs_execution_feedback: bool,
}

impl<'de> Deserialize<'de> for StrategyRequirements {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = StrategyRequirementsDef::deserialize(deserializer)?;
        Self::new(
            value.instruments,
            value.series,
            value.decision_latency_ms,
            value.needs_tick_execution,
            value.needs_execution_feedback,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Classification for one strategy decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDecisionKind {
    NoAction,
    Entry,
    Management,
    Exit,
    Annotation,
    Rejected,
}

/// Bounded explanation and exact strict signals emitted at one decision boundary.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyDecisionRecord {
    sequence: u64,
    observed_through: NaiveDateTime,
    kind: StrategyDecisionKind,
    reason: String,
    related_trade_id: Option<String>,
    emitted_signals: Vec<RawSignal>,
}

impl StrategyDecisionRecord {
    pub fn new(
        sequence: u64,
        observed_through: NaiveDateTime,
        kind: StrategyDecisionKind,
        reason: impl Into<String>,
        related_trade_id: Option<String>,
        emitted_signals: Vec<RawSignal>,
        limits: StrategyRetentionLimits,
    ) -> Result<Self, StrategyDomainError> {
        let reason = reason.into();
        validate_decision_fields(
            &reason,
            related_trade_id.as_deref(),
            emitted_signals.len(),
            limits,
        )?;
        Ok(Self {
            sequence,
            observed_through,
            kind,
            reason,
            related_trade_id,
            emitted_signals,
        })
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn kind(&self) -> StrategyDecisionKind {
        self.kind
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn related_trade_id(&self) -> Option<&str> {
        self.related_trade_id.as_deref()
    }

    pub fn emitted_signals(&self) -> &[RawSignal] {
        &self.emitted_signals
    }
}

/// Exact output-retention accounting for strategy decisions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyDecisionRetention {
    pub retained: usize,
    pub omitted: usize,
}

/// Retained decisions plus exact omitted-count metadata.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyDecisionOutput {
    pub records: Vec<StrategyDecisionRecord>,
    pub retention: StrategyDecisionRetention,
}

/// Bounded recorder that returns executable signals even when a decision record is omitted.
pub struct StrategyDecisionRecorder {
    limits: StrategyRetentionLimits,
    records: Vec<StrategyDecisionRecord>,
    omitted: usize,
    last_sequence: Option<u64>,
}

impl StrategyDecisionRecorder {
    pub fn new(limits: StrategyRetentionLimits) -> Self {
        Self {
            limits,
            records: Vec::new(),
            omitted: 0,
            last_sequence: None,
        }
    }

    pub fn push(
        &mut self,
        record: StrategyDecisionRecord,
    ) -> Result<Vec<RawSignal>, StrategyDomainError> {
        if let Some(previous) = self.last_sequence
            && record.sequence() <= previous
        {
            return Err(StrategyDomainError::NonMonotonicDecisionSequence {
                previous,
                current: record.sequence(),
            });
        }
        let executable_signals = record.emitted_signals.clone();
        self.last_sequence = Some(record.sequence());
        if self.records.len() < self.limits.max_decisions() {
            self.records.push(record);
        } else {
            self.omitted = self
                .omitted
                .checked_add(1)
                .ok_or(StrategyDomainError::OmittedCounterOverflow)?;
        }
        Ok(executable_signals)
    }

    pub fn finish(self) -> StrategyDecisionOutput {
        StrategyDecisionOutput {
            retention: StrategyDecisionRetention {
                retained: self.records.len(),
                omitted: self.omitted,
            },
            records: self.records,
        }
    }
}

/// Strategy-specific wrapper around an existing economic replay result.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyBacktestResult {
    pub replay: BacktestResult,
    pub descriptor: StrategyDescriptor,
    pub decisions: StrategyDecisionOutput,
}

pub(crate) fn validate_decision_fields(
    reason: &str,
    related_trade_id: Option<&str>,
    signal_count: usize,
    limits: StrategyRetentionLimits,
) -> Result<(), StrategyDomainError> {
    if !valid_text(reason, limits.max_reason_bytes()) {
        return Err(StrategyDomainError::InvalidDecisionReason {
            maximum: limits.max_reason_bytes(),
        });
    }
    if related_trade_id.is_some_and(|trade_id| !valid_text(trade_id, MAX_TRADE_ID_BYTES)) {
        return Err(StrategyDomainError::InvalidTradeId);
    }
    if signal_count > limits.max_signals_per_callback() {
        return Err(StrategyDomainError::TooManySignals {
            actual: signal_count,
            maximum: limits.max_signals_per_callback(),
        });
    }
    Ok(())
}

fn validate_requirements(
    instruments: &[String],
    series: &[SeriesRequirement],
    decision_latency_ms: u64,
) -> Result<(), StrategyDomainError> {
    if instruments.is_empty() {
        return Err(StrategyDomainError::MissingInstruments);
    }
    if series.is_empty() {
        return Err(StrategyDomainError::MissingSeries);
    }
    if decision_latency_ms > MAX_DECISION_LATENCY_MS {
        return Err(StrategyDomainError::DecisionLatencyTooLarge {
            value: decision_latency_ms,
        });
    }

    let mut instrument_set = HashSet::with_capacity(instruments.len());
    for symbol in instruments {
        validate_instrument(symbol)?;
        if !instrument_set.insert(symbol.as_str()) {
            return Err(StrategyDomainError::DuplicateInstrument {
                symbol: symbol.clone(),
            });
        }
    }

    let mut series_ids = HashSet::with_capacity(series.len());
    let mut definitions = HashSet::with_capacity(series.len());
    for requirement in series {
        if !series_ids.insert(requirement.id()) {
            return Err(StrategyDomainError::DuplicateSeriesId {
                series_id: requirement.id().clone(),
            });
        }
        if !instrument_set.contains(requirement.symbol()) {
            return Err(StrategyDomainError::UndeclaredSeriesInstrument {
                series_id: requirement.id().clone(),
                symbol: requirement.symbol().to_string(),
            });
        }
        let definition = (
            requirement.symbol(),
            requirement.timeframe(),
            requirement.price_basis(),
        );
        if !definitions.insert(definition) {
            return Err(StrategyDomainError::DuplicateSeriesDefinition {
                series_id: requirement.id().clone(),
            });
        }
    }
    Ok(())
}

fn validate_instrument(symbol: &str) -> Result<(), StrategyDomainError> {
    if symbol.is_empty()
        || symbol.len() > MAX_INSTRUMENT_BYTES
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
    {
        return Err(StrategyDomainError::InvalidInstrument {
            symbol: symbol.to_string(),
        });
    }
    Ok(())
}

fn valid_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
