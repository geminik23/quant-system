//! Causal historical observations and complete-boundary analysis.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;

use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer, Serialize};

use super::annotation::{AnnotationId, AnnotationLimits, AnnotationTimeline, StrategyAnnotation};
use super::{ClosedBar, HistoricalSeriesView, SeriesId, SeriesViewError};

pub const MAX_ANALYZERS: usize = 256;
pub const MAX_RETAINED_OBSERVATIONS: usize = 1_000_000;
pub const MAX_OBSERVATIONS_PER_BOUNDARY: usize = 4096;
pub const MAX_OBSERVATION_SOURCE_SERIES: usize = 64;
pub const MAX_ZONE_ID_BYTES: usize = 64;
pub const MAX_PIVOT_SIDE_BARS: usize = 1_000_000;

/// Stable caller-supplied identity for one price zone.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ZoneId(String);

impl ZoneId {
    pub fn new(value: impl Into<String>) -> Result<Self, AnalysisError> {
        let value = value.into();
        if valid_identifier(&value, MAX_ZONE_ID_BYTES) {
            Ok(Self(value))
        } else {
            Err(AnalysisError::InvalidZoneId)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ZoneId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ZoneId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Provenance category for a descriptive price zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneSource {
    CausalAnnotation,
    DeterministicAnalyzer,
}

/// Descriptive side of a price zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneSide {
    Support,
    Resistance,
}

/// Descriptive lifecycle state of an immutable zone snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZoneState {
    Active,
    Broken,
    RetestPending,
    Retested,
    Choppy,
    Degraded,
    Invalid,
}

/// One validated immutable price-zone snapshot.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PriceZone {
    zone_id: ZoneId,
    side: ZoneSide,
    lower: f64,
    upper: f64,
    created_at: NaiveDateTime,
    touch_count: u32,
    state: ZoneState,
    source: ZoneSource,
}

impl PriceZone {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        zone_id: ZoneId,
        side: ZoneSide,
        lower: f64,
        upper: f64,
        created_at: NaiveDateTime,
        touch_count: u32,
        state: ZoneState,
        source: ZoneSource,
    ) -> Result<Self, AnalysisError> {
        validate_price(lower, "zone lower")?;
        validate_price(upper, "zone upper")?;
        if lower >= upper {
            return Err(AnalysisError::InvalidZoneGeometry { lower, upper });
        }
        Ok(Self {
            zone_id,
            side,
            lower,
            upper,
            created_at,
            touch_count,
            state,
            source,
        })
    }

    pub fn zone_id(&self) -> &ZoneId {
        &self.zone_id
    }

    pub fn side(&self) -> ZoneSide {
        self.side
    }

    pub fn lower(&self) -> f64 {
        self.lower
    }

    pub fn upper(&self) -> f64 {
        self.upper
    }

    pub fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    pub fn touch_count(&self) -> u32 {
        self.touch_count
    }

    pub fn state(&self) -> ZoneState {
        self.state
    }

    pub fn source(&self) -> ZoneSource {
        self.source
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceZoneDef {
    zone_id: ZoneId,
    side: ZoneSide,
    lower: f64,
    upper: f64,
    created_at: NaiveDateTime,
    touch_count: u32,
    state: ZoneState,
    source: ZoneSource,
}

impl<'de> Deserialize<'de> for PriceZone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = PriceZoneDef::deserialize(deserializer)?;
        Self::new(
            value.zone_id,
            value.side,
            value.lower,
            value.upper,
            value.created_at,
            value.touch_count,
            value.state,
            value.source,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// High or low classification for a confirmed swing point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwingKind {
    High,
    Low,
}

/// One immutable swing point with explicit anchor and confirmation times.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SwingPoint {
    kind: SwingKind,
    price: f64,
    anchor_open_time: NaiveDateTime,
    anchor_close_time: NaiveDateTime,
    confirmed_at: NaiveDateTime,
}

impl SwingPoint {
    pub fn new(
        kind: SwingKind,
        price: f64,
        anchor_open_time: NaiveDateTime,
        anchor_close_time: NaiveDateTime,
        confirmed_at: NaiveDateTime,
    ) -> Result<Self, AnalysisError> {
        validate_price(price, "swing price")?;
        if anchor_open_time >= anchor_close_time {
            return Err(AnalysisError::InvalidAnchorRange {
                open: anchor_open_time,
                close: anchor_close_time,
            });
        }
        if confirmed_at < anchor_close_time {
            return Err(AnalysisError::ConfirmationBeforeAnchorClose {
                confirmed_at,
                anchor_close: anchor_close_time,
            });
        }
        Ok(Self {
            kind,
            price,
            anchor_open_time,
            anchor_close_time,
            confirmed_at,
        })
    }

    pub fn kind(&self) -> SwingKind {
        self.kind
    }

    pub fn price(&self) -> f64 {
        self.price
    }

    pub fn anchor_open_time(&self) -> NaiveDateTime {
        self.anchor_open_time
    }

    pub fn anchor_close_time(&self) -> NaiveDateTime {
        self.anchor_close_time
    }

    pub fn confirmed_at(&self) -> NaiveDateTime {
        self.confirmed_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SwingPointDef {
    kind: SwingKind,
    price: f64,
    anchor_open_time: NaiveDateTime,
    anchor_close_time: NaiveDateTime,
    confirmed_at: NaiveDateTime,
}

impl<'de> Deserialize<'de> for SwingPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SwingPointDef::deserialize(deserializer)?;
        Self::new(
            value.kind,
            value.price,
            value.anchor_open_time,
            value.anchor_close_time,
            value.confirmed_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Common descriptive rejection patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionPattern {
    LongWick,
    Engulfing,
    DoubleTouch,
    SnapBackInside,
}

/// Common descriptive momentum states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MomentumState {
    Advancing,
    Stalling,
    Sideways,
    Reversing,
}

/// Small common observation vocabulary shared by historical strategies.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StrategyObservationValue {
    Zone(PriceZone),
    Swing(SwingPoint),
    Rejection {
        pattern: RejectionPattern,
        anchor_open_time: NaiveDateTime,
        anchor_close_time: NaiveDateTime,
    },
    Momentum(MomentumState),
}

impl StrategyObservationValue {
    pub fn rejection(
        pattern: RejectionPattern,
        anchor_open_time: NaiveDateTime,
        anchor_close_time: NaiveDateTime,
    ) -> Result<Self, AnalysisError> {
        if anchor_open_time >= anchor_close_time {
            return Err(AnalysisError::InvalidAnchorRange {
                open: anchor_open_time,
                close: anchor_close_time,
            });
        }
        Ok(Self::Rejection {
            pattern,
            anchor_open_time,
            anchor_close_time,
        })
    }

    pub(crate) fn validate_at(&self, observed_through: NaiveDateTime) -> Result<(), AnalysisError> {
        match self {
            Self::Zone(zone) if zone.created_at() > observed_through => {
                Err(AnalysisError::ValueAfterObservation {
                    value_time: zone.created_at(),
                    observed_through,
                })
            }
            Self::Swing(swing) if swing.confirmed_at() > observed_through => {
                Err(AnalysisError::ValueAfterObservation {
                    value_time: swing.confirmed_at(),
                    observed_through,
                })
            }
            Self::Rejection {
                anchor_close_time, ..
            } if *anchor_close_time > observed_through => {
                Err(AnalysisError::ValueAfterObservation {
                    value_time: *anchor_close_time,
                    observed_through,
                })
            }
            _ => Ok(()),
        }
    }

    pub fn zone(&self) -> Option<&PriceZone> {
        match self {
            Self::Zone(zone) => Some(zone),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum StrategyObservationValueDef {
    Zone(PriceZone),
    Swing(SwingPoint),
    Rejection {
        pattern: RejectionPattern,
        anchor_open_time: NaiveDateTime,
        anchor_close_time: NaiveDateTime,
    },
    Momentum(MomentumState),
}

impl<'de> Deserialize<'de> for StrategyObservationValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match StrategyObservationValueDef::deserialize(deserializer)? {
            StrategyObservationValueDef::Zone(value) => Ok(Self::Zone(value)),
            StrategyObservationValueDef::Swing(value) => Ok(Self::Swing(value)),
            StrategyObservationValueDef::Rejection {
                pattern,
                anchor_open_time,
                anchor_close_time,
            } => Self::rejection(pattern, anchor_open_time, anchor_close_time)
                .map_err(serde::de::Error::custom),
            StrategyObservationValueDef::Momentum(value) => Ok(Self::Momentum(value)),
        }
    }
}

/// Authoritative source category for a committed observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationOrigin {
    Analyzer,
    CausalAnnotation { annotation_id: AnnotationId },
}

/// One immutable committed causal observation.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyObservation {
    sequence: u64,
    observed_through: NaiveDateTime,
    valid_from: NaiveDateTime,
    symbol: String,
    source_series: Vec<SeriesId>,
    origin: ObservationOrigin,
    value: StrategyObservationValue,
}

impl StrategyObservation {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn valid_from(&self) -> NaiveDateTime {
        self.valid_from
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn source_series(&self) -> &[SeriesId] {
        &self.source_series
    }

    pub fn origin(&self) -> &ObservationOrigin {
        &self.origin
    }

    pub fn value(&self) -> &StrategyObservationValue {
        &self.value
    }

    pub(crate) fn from_annotation(
        sequence: u64,
        observed_through: NaiveDateTime,
        annotation: &StrategyAnnotation,
    ) -> Result<Self, AnalysisError> {
        let valid_from = annotation
            .valid_from()
            .expect("only causal annotations become observations");
        Self::validated(
            sequence,
            observed_through,
            valid_from,
            annotation.symbol().to_string(),
            annotation.source_series().to_vec(),
            ObservationOrigin::CausalAnnotation {
                annotation_id: annotation.annotation_id().clone(),
            },
            annotation.value().clone(),
        )
    }

    fn from_draft(
        sequence: u64,
        observed_through: NaiveDateTime,
        draft: StrategyObservationDraft,
    ) -> Result<Self, AnalysisError> {
        Self::validated(
            sequence,
            observed_through,
            observed_through,
            draft.symbol,
            draft.source_series,
            ObservationOrigin::Analyzer,
            draft.value,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn validated(
        sequence: u64,
        observed_through: NaiveDateTime,
        valid_from: NaiveDateTime,
        symbol: String,
        source_series: Vec<SeriesId>,
        origin: ObservationOrigin,
        value: StrategyObservationValue,
    ) -> Result<Self, AnalysisError> {
        validate_symbol(&symbol)?;
        validate_source_series(&source_series, MAX_OBSERVATION_SOURCE_SERIES)?;
        if valid_from > observed_through {
            return Err(AnalysisError::ObservationNotYetValid {
                valid_from,
                observed_through,
            });
        }
        value.validate_at(observed_through)?;
        Ok(Self {
            sequence,
            observed_through,
            valid_from,
            symbol,
            source_series,
            origin,
            value,
        })
    }
}

/// Metadata-free analyzer output validated and stamped by the pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct StrategyObservationDraft {
    symbol: String,
    source_series: Vec<SeriesId>,
    value: StrategyObservationValue,
}

impl StrategyObservationDraft {
    pub fn new(
        symbol: impl Into<String>,
        source_series: Vec<SeriesId>,
        value: StrategyObservationValue,
    ) -> Result<Self, AnalysisError> {
        let symbol = symbol.into();
        validate_symbol(&symbol)?;
        validate_source_series(&source_series, MAX_OBSERVATION_SOURCE_SERIES)?;
        Ok(Self {
            symbol,
            source_series,
            value,
        })
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn source_series(&self) -> &[SeriesId] {
        &self.source_series
    }

    pub fn value(&self) -> &StrategyObservationValue {
        &self.value
    }
}

/// Caller-visible bounds for strategy-visible observation history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationStoreLimits {
    max_retained: usize,
    max_per_boundary: usize,
}

impl ObservationStoreLimits {
    pub fn new(max_retained: usize, max_per_boundary: usize) -> Result<Self, AnalysisError> {
        validate_nonzero_limit("max_retained", max_retained, MAX_RETAINED_OBSERVATIONS)?;
        validate_nonzero_limit(
            "max_per_boundary",
            max_per_boundary,
            MAX_OBSERVATIONS_PER_BOUNDARY,
        )?;
        Ok(Self {
            max_retained,
            max_per_boundary,
        })
    }

    pub fn max_retained(self) -> usize {
        self.max_retained
    }

    pub fn max_per_boundary(self) -> usize {
        self.max_per_boundary
    }
}

impl Default for ObservationStoreLimits {
    fn default() -> Self {
        Self {
            max_retained: 10_000,
            max_per_boundary: 256,
        }
    }
}

/// Allocation-free suffix view over a possibly wrapped observation deque.
#[derive(Debug, Clone, Copy)]
pub struct ObservationWindow<'a> {
    older: &'a [StrategyObservation],
    newer: &'a [StrategyObservation],
}

impl<'a> ObservationWindow<'a> {
    pub fn len(&self) -> usize {
        self.older.len() + self.newer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.older.is_empty() && self.newer.is_empty()
    }

    pub fn latest(&self) -> Option<&'a StrategyObservation> {
        self.newer.last().or_else(|| self.older.last())
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &'a StrategyObservation> {
        self.older.iter().chain(self.newer.iter())
    }
}

/// Borrowed deterministic symbol-filtered selection.
#[derive(Debug, Clone, Copy)]
pub struct ObservationSelection<'a> {
    window: ObservationWindow<'a>,
    symbol: &'a str,
    count: usize,
}

impl<'a> ObservationSelection<'a> {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &'a StrategyObservation> {
        self.window
            .iter()
            .rev()
            .filter(move |observation| observation.symbol() == self.symbol)
            .take(self.count)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
    }
}

/// Object-safe read-only view over committed causal observations.
pub trait HistoricalObservationView {
    fn observations(&self, count: usize) -> ObservationWindow<'_>;
    fn for_symbol<'a>(&'a self, symbol: &'a str, count: usize) -> ObservationSelection<'a>;
    fn latest_zone(&self, zone_id: &ZoneId) -> Option<&StrategyObservation>;
    fn omitted(&self) -> u64;
}

/// Bounded strategy-visible working history.
#[derive(Debug, Clone)]
pub struct ObservationStore {
    retained: VecDeque<StrategyObservation>,
    omitted: u64,
    limits: ObservationStoreLimits,
}

impl ObservationStore {
    pub fn new(limits: ObservationStoreLimits) -> Self {
        Self {
            retained: VecDeque::with_capacity(limits.max_retained),
            omitted: 0,
            limits,
        }
    }

    pub fn limits(&self) -> ObservationStoreLimits {
        self.limits
    }

    pub fn len(&self) -> usize {
        self.retained.len()
    }

    pub fn is_empty(&self) -> bool {
        self.retained.is_empty()
    }

    pub fn omitted(&self) -> u64 {
        self.omitted
    }

    pub fn observations(&self, count: usize) -> ObservationWindow<'_> {
        HistoricalObservationView::observations(self, count)
    }

    pub fn for_symbol<'a>(&'a self, symbol: &'a str, count: usize) -> ObservationSelection<'a> {
        HistoricalObservationView::for_symbol(self, symbol, count)
    }

    pub fn latest_zone(&self, zone_id: &ZoneId) -> Option<&StrategyObservation> {
        HistoricalObservationView::latest_zone(self, zone_id)
    }

    fn push(&mut self, observation: StrategyObservation) -> Result<(), AnalysisError> {
        if self.retained.len() == self.limits.max_retained {
            self.omitted = self
                .omitted
                .checked_add(1)
                .ok_or(AnalysisError::OmittedCountOverflow)?;
            self.retained.pop_front();
        }
        self.retained.push_back(observation);
        Ok(())
    }
}

impl HistoricalObservationView for ObservationStore {
    fn observations(&self, count: usize) -> ObservationWindow<'_> {
        let (older, newer) = self.retained.as_slices();
        let available = count.min(self.retained.len());
        let skip = self.retained.len() - available;
        if skip < older.len() {
            ObservationWindow {
                older: &older[skip..],
                newer,
            }
        } else {
            ObservationWindow {
                older: &older[older.len()..],
                newer: &newer[skip - older.len()..],
            }
        }
    }

    fn for_symbol<'a>(&'a self, symbol: &'a str, count: usize) -> ObservationSelection<'a> {
        ObservationSelection {
            window: self.observations(usize::MAX),
            symbol,
            count,
        }
    }

    fn latest_zone(&self, zone_id: &ZoneId) -> Option<&StrategyObservation> {
        self.retained.iter().rev().find(|observation| {
            observation
                .value()
                .zone()
                .is_some_and(|zone| zone.zone_id() == zone_id)
        })
    }

    fn omitted(&self) -> u64 {
        self.omitted
    }
}

/// Complete historical boundary supplied after a closed-bar series commits one timestamp batch.
pub struct AnalysisBoundary<'a> {
    observed_through: NaiveDateTime,
    closed_bars: &'a [ClosedBar],
    series: &'a dyn HistoricalSeriesView,
}

impl<'a> AnalysisBoundary<'a> {
    pub fn new(
        observed_through: NaiveDateTime,
        closed_bars: &'a [ClosedBar],
        series: &'a dyn HistoricalSeriesView,
    ) -> Self {
        Self {
            observed_through,
            closed_bars,
            series,
        }
    }

    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn closed_bars(&self) -> &'a [ClosedBar] {
        self.closed_bars
    }

    pub fn series(&self) -> &'a dyn HistoricalSeriesView {
        self.series
    }
}

/// Read-only causal state visible during one analyzer callback.
#[derive(Clone, Copy)]
pub struct AnalysisContext<'a> {
    observed_through: NaiveDateTime,
    series: &'a dyn HistoricalSeriesView,
    observations: &'a dyn HistoricalObservationView,
}

impl<'a> AnalysisContext<'a> {
    pub fn observed_through(self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn series(self) -> &'a dyn HistoricalSeriesView {
        self.series
    }

    pub fn observations(self) -> &'a dyn HistoricalObservationView {
        self.observations
    }
}

/// Synchronous extension point for causal bar analysis.
pub trait HistoricalAnalyzer {
    fn on_bar(
        &mut self,
        bar: &ClosedBar,
        context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError>;
}

/// Observations atomically committed by one complete boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalysisBoundaryOutput {
    observed_through: NaiveDateTime,
    observations: Vec<StrategyObservation>,
}

impl AnalysisBoundaryOutput {
    pub fn observed_through(&self) -> NaiveDateTime {
        self.observed_through
    }

    pub fn observations(&self) -> &[StrategyObservation] {
        &self.observations
    }
}

/// Complete-boundary analyzer pipeline with bounded committed history.
pub struct AnalysisPipeline {
    analyzers: Vec<Box<dyn HistoricalAnalyzer>>,
    observations: ObservationStore,
    annotations: AnnotationTimeline,
    next_sequence: u64,
    last_boundary: Option<NaiveDateTime>,
    failed: bool,
}

impl AnalysisPipeline {
    pub fn new(
        analyzers: Vec<Box<dyn HistoricalAnalyzer>>,
        observation_limits: ObservationStoreLimits,
        annotation_limits: AnnotationLimits,
    ) -> Result<Self, AnalysisError> {
        if analyzers.len() > MAX_ANALYZERS {
            return Err(AnalysisError::TooManyAnalyzers {
                actual: analyzers.len(),
                maximum: MAX_ANALYZERS,
            });
        }
        Ok(Self {
            analyzers,
            observations: ObservationStore::new(observation_limits),
            annotations: AnnotationTimeline::new(annotation_limits),
            next_sequence: 0,
            last_boundary: None,
            failed: false,
        })
    }

    pub fn add_annotation(&mut self, annotation: StrategyAnnotation) -> Result<(), AnalysisError> {
        if self.failed {
            return Err(AnalysisError::PipelineFailed);
        }
        self.annotations
            .add(annotation, self.last_boundary)
            .map_err(AnalysisError::Annotation)
    }

    pub fn observations(&self) -> &ObservationStore {
        &self.observations
    }

    pub fn annotations(&self) -> &AnnotationTimeline {
        &self.annotations
    }

    pub(crate) fn into_research_annotations(self) -> Vec<StrategyAnnotation> {
        self.annotations.into_research_only()
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn on_boundary(
        &mut self,
        boundary: AnalysisBoundary<'_>,
    ) -> Result<AnalysisBoundaryOutput, AnalysisError> {
        if self.failed {
            return Err(AnalysisError::PipelineFailed);
        }
        self.validate_boundary(&boundary)?;

        let activates_annotation = self
            .annotations
            .pending_causal()
            .first()
            .and_then(|annotation| annotation.valid_from())
            .is_some_and(|valid_from| valid_from <= boundary.observed_through);
        if boundary.closed_bars.is_empty() && !activates_annotation {
            self.last_boundary = Some(boundary.observed_through);
            return Ok(AnalysisBoundaryOutput {
                observed_through: boundary.observed_through,
                observations: Vec::new(),
            });
        }

        let mut staged_store = self.observations.clone();
        let mut staged_annotations = self.annotations.clone();
        let mut staged_sequence = self.next_sequence;
        let mut committed = Vec::new();
        let eligible = staged_annotations
            .activate(boundary.observed_through)
            .map_err(AnalysisError::Annotation)?;
        self.ensure_boundary_capacity(eligible.len())?;
        for annotation in eligible {
            let sequence = take_sequence(&mut staged_sequence)?;
            let observation = StrategyObservation::from_annotation(
                sequence,
                boundary.observed_through,
                &annotation,
            )?;
            staged_store.push(observation.clone())?;
            committed.push(observation);
        }

        for bar in boundary.closed_bars {
            for analyzer_index in 0..self.analyzers.len() {
                let context = AnalysisContext {
                    observed_through: boundary.observed_through,
                    series: boundary.series,
                    observations: &staged_store,
                };
                let drafts = match self.analyzers[analyzer_index].on_bar(bar, context) {
                    Ok(drafts) => drafts,
                    Err(source) => {
                        self.failed = true;
                        return Err(AnalysisError::AnalyzerFailure {
                            analyzer_index,
                            source: Box::new(source),
                        });
                    }
                };
                let next_count = match committed.len().checked_add(drafts.len()) {
                    Some(count) => count,
                    None => {
                        self.failed = true;
                        return Err(AnalysisError::BoundaryOutputOverflow);
                    }
                };
                if let Err(error) = self.ensure_boundary_capacity(next_count) {
                    self.failed = true;
                    return Err(error);
                }
                for draft in drafts {
                    let sequence = match take_sequence(&mut staged_sequence) {
                        Ok(sequence) => sequence,
                        Err(error) => {
                            self.failed = true;
                            return Err(error);
                        }
                    };
                    let observation = match StrategyObservation::from_draft(
                        sequence,
                        boundary.observed_through,
                        draft,
                    ) {
                        Ok(observation) => observation,
                        Err(error) => {
                            self.failed = true;
                            return Err(error);
                        }
                    };
                    if let Err(error) = staged_store.push(observation.clone()) {
                        self.failed = true;
                        return Err(error);
                    }
                    committed.push(observation);
                }
            }
        }

        self.observations = staged_store;
        self.annotations = staged_annotations;
        self.next_sequence = staged_sequence;
        self.last_boundary = Some(boundary.observed_through);
        Ok(AnalysisBoundaryOutput {
            observed_through: boundary.observed_through,
            observations: committed,
        })
    }

    fn validate_boundary(&self, boundary: &AnalysisBoundary<'_>) -> Result<(), AnalysisError> {
        if let Some(previous) = self.last_boundary
            && boundary.observed_through < previous
        {
            return Err(AnalysisError::BoundaryRegression {
                previous,
                current: boundary.observed_through,
            });
        }
        for bar in boundary.closed_bars {
            if bar.close_time() > boundary.observed_through {
                return Err(AnalysisError::BarAfterBoundary {
                    series_id: bar.series_id().clone(),
                    close_time: bar.close_time(),
                    observed_through: boundary.observed_through,
                });
            }
        }
        Ok(())
    }

    fn ensure_boundary_capacity(&self, actual: usize) -> Result<(), AnalysisError> {
        let maximum = self.observations.limits.max_per_boundary;
        if actual > maximum {
            Err(AnalysisError::TooManyBoundaryObservations { actual, maximum })
        } else {
            Ok(())
        }
    }
}

/// Validated configuration for the reference confirmed-pivot analyzer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PivotConfig {
    series_id: SeriesId,
    left_bars: usize,
    right_bars: usize,
    required_bars: usize,
}

impl PivotConfig {
    pub fn new(
        series_id: SeriesId,
        left_bars: usize,
        right_bars: usize,
    ) -> Result<Self, AnalysisError> {
        validate_nonzero_limit("left_bars", left_bars, MAX_PIVOT_SIDE_BARS)?;
        validate_nonzero_limit("right_bars", right_bars, MAX_PIVOT_SIDE_BARS)?;
        let required_bars = left_bars
            .checked_add(1)
            .and_then(|value| value.checked_add(right_bars))
            .ok_or(AnalysisError::PivotHistoryOverflow)?;
        Ok(Self {
            series_id,
            left_bars,
            right_bars,
            required_bars,
        })
    }

    pub fn series_id(&self) -> &SeriesId {
        &self.series_id
    }

    pub fn left_bars(&self) -> usize {
        self.left_bars
    }

    pub fn right_bars(&self) -> usize {
        self.right_bars
    }

    pub fn required_bars(&self) -> usize {
        self.required_bars
    }
}

/// Exact delayed-confirmation high and low pivot analyzer.
#[derive(Debug, Clone)]
pub struct ConfirmedPivotAnalyzer {
    config: PivotConfig,
}

impl ConfirmedPivotAnalyzer {
    pub fn new(config: PivotConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &PivotConfig {
        &self.config
    }
}

impl HistoricalAnalyzer for ConfirmedPivotAnalyzer {
    fn on_bar(
        &mut self,
        bar: &ClosedBar,
        context: AnalysisContext<'_>,
    ) -> Result<Vec<StrategyObservationDraft>, AnalysisError> {
        context
            .series()
            .latest_bar(self.config.series_id())
            .map_err(AnalysisError::SeriesView)?;
        if bar.series_id() != self.config.series_id() {
            return Ok(Vec::new());
        }
        let history = context
            .series()
            .bars(self.config.series_id(), self.config.required_bars())
            .map_err(AnalysisError::SeriesView)?;
        if history.len() < self.config.required_bars() {
            return Ok(Vec::new());
        }
        let bars = history.iter().collect::<Vec<_>>();
        let candidate = bars[self.config.left_bars];
        let left = &bars[..self.config.left_bars];
        let right = &bars[self.config.left_bars + 1..];
        let is_high = left
            .iter()
            .chain(right.iter())
            .all(|neighbor| candidate.high() > neighbor.high());
        let is_low = left
            .iter()
            .chain(right.iter())
            .all(|neighbor| candidate.low() < neighbor.low());
        let mut drafts = Vec::with_capacity(usize::from(is_high) + usize::from(is_low));
        if is_high {
            drafts.push(pivot_draft(
                candidate,
                SwingKind::High,
                context.observed_through(),
            )?);
        }
        if is_low {
            drafts.push(pivot_draft(
                candidate,
                SwingKind::Low,
                context.observed_through(),
            )?);
        }
        Ok(drafts)
    }
}

fn pivot_draft(
    candidate: &ClosedBar,
    kind: SwingKind,
    confirmed_at: NaiveDateTime,
) -> Result<StrategyObservationDraft, AnalysisError> {
    let price = match kind {
        SwingKind::High => candidate.high(),
        SwingKind::Low => candidate.low(),
    };
    StrategyObservationDraft::new(
        candidate.symbol(),
        vec![candidate.series_id().clone()],
        StrategyObservationValue::Swing(SwingPoint::new(
            kind,
            price,
            candidate.open_time(),
            candidate.close_time(),
            confirmed_at,
        )?),
    )
}

/// Typed construction, causality, ordering, lookup, and pipeline failures.
#[derive(Debug, thiserror::Error)]
pub enum AnalysisError {
    #[error("zone ID must contain 1 to {MAX_ZONE_ID_BYTES} ASCII identifier bytes")]
    InvalidZoneId,
    #[error("{field} must be finite and positive")]
    InvalidPrice { field: &'static str },
    #[error("zone lower {lower} must be strictly below upper {upper}")]
    InvalidZoneGeometry { lower: f64, upper: f64 },
    #[error("anchor open {open} must be before anchor close {close}")]
    InvalidAnchorRange {
        open: NaiveDateTime,
        close: NaiveDateTime,
    },
    #[error("confirmation {confirmed_at} cannot precede anchor close {anchor_close}")]
    ConfirmationBeforeAnchorClose {
        confirmed_at: NaiveDateTime,
        anchor_close: NaiveDateTime,
    },
    #[error("invalid observation symbol '{symbol}'")]
    InvalidSymbol { symbol: String },
    #[error("observation source series must not contain duplicates")]
    DuplicateSourceSeries,
    #[error("observation source-series count {actual} exceeds maximum {maximum}")]
    TooManySourceSeries { actual: usize, maximum: usize },
    #[error("observation valid time {valid_from} is after boundary {observed_through}")]
    ObservationNotYetValid {
        valid_from: NaiveDateTime,
        observed_through: NaiveDateTime,
    },
    #[error("observation value time {value_time} is after boundary {observed_through}")]
    ValueAfterObservation {
        value_time: NaiveDateTime,
        observed_through: NaiveDateTime,
    },
    #[error("{field} must be greater than zero")]
    ZeroLimit { field: &'static str },
    #[error("{field} {actual} exceeds maximum {maximum}")]
    LimitTooLarge {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("analyzer count {actual} exceeds maximum {maximum}")]
    TooManyAnalyzers { actual: usize, maximum: usize },
    #[error("analysis boundary moved backwards from {previous} to {current}")]
    BoundaryRegression {
        previous: NaiveDateTime,
        current: NaiveDateTime,
    },
    #[error("bar for '{series_id}' closes at {close_time} after boundary {observed_through}")]
    BarAfterBoundary {
        series_id: SeriesId,
        close_time: NaiveDateTime,
        observed_through: NaiveDateTime,
    },
    #[error("boundary emitted {actual} observations, exceeding maximum {maximum}")]
    TooManyBoundaryObservations { actual: usize, maximum: usize },
    #[error("boundary observation count overflowed")]
    BoundaryOutputOverflow,
    #[error("observation sequence overflowed")]
    SequenceOverflow,
    #[error("observation omitted counter overflowed")]
    OmittedCountOverflow,
    #[error("pivot history requirement overflowed")]
    PivotHistoryOverflow,
    #[error(transparent)]
    SeriesView(#[from] SeriesViewError),
    #[error(transparent)]
    Annotation(#[from] super::annotation::AnnotationError),
    #[error("analyzer {analyzer_index} failed: {source}")]
    AnalyzerFailure {
        analyzer_index: usize,
        source: Box<AnalysisError>,
    },
    #[error("analysis pipeline is terminally failed")]
    PipelineFailed,
    #[error("analyzer failed: {message}")]
    Analyzer { message: String },
}

pub(crate) fn validate_symbol(symbol: &str) -> Result<(), AnalysisError> {
    if symbol.is_empty()
        || symbol.len() > super::MAX_INSTRUMENT_BYTES
        || !symbol.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
        })
    {
        return Err(AnalysisError::InvalidSymbol {
            symbol: symbol.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_source_series(
    source_series: &[SeriesId],
    maximum: usize,
) -> Result<(), AnalysisError> {
    if source_series.len() > maximum {
        return Err(AnalysisError::TooManySourceSeries {
            actual: source_series.len(),
            maximum,
        });
    }
    let mut unique = BTreeSet::new();
    if source_series.iter().any(|series| !unique.insert(series)) {
        return Err(AnalysisError::DuplicateSourceSeries);
    }
    Ok(())
}

fn validate_price(value: f64, field: &'static str) -> Result<(), AnalysisError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(AnalysisError::InvalidPrice { field })
    }
}

fn validate_nonzero_limit(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), AnalysisError> {
    if actual == 0 {
        return Err(AnalysisError::ZeroLimit { field });
    }
    if actual > maximum {
        return Err(AnalysisError::LimitTooLarge {
            field,
            actual,
            maximum,
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

fn take_sequence(next: &mut u64) -> Result<u64, AnalysisError> {
    let sequence = *next;
    *next = next.checked_add(1).ok_or(AnalysisError::SequenceOverflow)?;
    Ok(sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp() -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    fn observation(sequence: u64) -> StrategyObservation {
        StrategyObservation::validated(
            sequence,
            timestamp(),
            timestamp(),
            "EURUSD".to_string(),
            Vec::new(),
            ObservationOrigin::Analyzer,
            StrategyObservationValue::Momentum(MomentumState::Advancing),
        )
        .unwrap()
    }

    #[test]
    fn sequence_overflow_does_not_advance_sequence() {
        let mut next = u64::MAX;
        assert!(matches!(
            take_sequence(&mut next),
            Err(AnalysisError::SequenceOverflow)
        ));
        assert_eq!(next, u64::MAX);
    }

    #[test]
    fn omitted_overflow_does_not_mutate_retained_history() {
        let mut store = ObservationStore::new(ObservationStoreLimits::new(1, 1).unwrap());
        store.push(observation(0)).unwrap();
        store.omitted = u64::MAX;
        let snapshot = store
            .observations(usize::MAX)
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        assert!(matches!(
            store.push(observation(1)),
            Err(AnalysisError::OmittedCountOverflow)
        ));
        assert_eq!(
            store
                .observations(usize::MAX)
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            snapshot
        );
        assert_eq!(store.omitted(), u64::MAX);
    }
}
