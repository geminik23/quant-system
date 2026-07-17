use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Describes whether a metric can be interpreted by a consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricStatus {
    Available,
    InsufficientData,
    NotApplicable,
    InvalidInput,
}

/// A metric accompanied by an explicit availability status.
///
/// Consumers should branch on `status`, rather than assigning a meaning to a
/// missing value. Available metrics always contain a value; other statuses do
/// not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricValue<T> {
    pub status: MetricStatus,
    pub value: Option<T>,
    pub reason: Option<String>,
}

impl<T> Default for MetricValue<T> {
    fn default() -> Self {
        Self::insufficient_data("metric was not present in serialized input")
    }
}

impl<T> MetricValue<T> {
    pub fn available(value: T) -> Self {
        Self {
            status: MetricStatus::Available,
            value: Some(value),
            reason: None,
        }
    }

    pub fn insufficient_data(reason: impl Into<String>) -> Self {
        Self::unavailable(MetricStatus::InsufficientData, reason)
    }

    pub fn not_applicable(reason: impl Into<String>) -> Self {
        Self::unavailable(MetricStatus::NotApplicable, reason)
    }

    pub fn invalid_input(reason: impl Into<String>) -> Self {
        Self::unavailable(MetricStatus::InvalidInput, reason)
    }

    fn unavailable(status: MetricStatus, reason: impl Into<String>) -> Self {
        Self {
            status,
            value: None,
            reason: Some(reason.into()),
        }
    }
}

/// Normalized position direction, independent of the execution engine's side
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}

/// Dimensions used for filtering and deterministic breakdowns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionDimensions {
    pub symbol: String,
    pub side: PositionSide,
    #[serde(default)]
    pub group: Option<String>,
    /// A position may have multiple close reasons after partial closes.
    #[serde(default)]
    pub close_reasons: Vec<String>,
    /// Provider-specific categorical dimensions (setup, session, regime, etc.).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

/// R-normalized maximum favorable and adverse excursion values.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ExcursionInput {
    #[serde(default)]
    pub favorable_r: Option<f64>,
    #[serde(default)]
    pub adverse_r: Option<f64>,
}

/// Optional per-position execution observations.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ExecutionDiagnosticsInput {
    /// Positive values conventionally mean adverse slippage.
    #[serde(default)]
    pub slippage_bps: Option<f64>,
    #[serde(default)]
    pub latency_ms: Option<f64>,
    /// Filled quantity divided by requested quantity, normally in `[0, 1]`.
    #[serde(default)]
    pub fill_ratio: Option<f64>,
}

/// Provider-supplied classification of a completed-position outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeClassification {
    Win,
    Loss,
    Breakeven,
}

/// Generic completed-position input for provider evaluation.
///
/// `outcome` is deliberately unit-agnostic: it can be account currency, points,
/// or another consistently applied additive result. `ordinal` defines lifecycle
/// order for rolling metrics (for example, a close timestamp in milliseconds).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionOutcome {
    pub id: String,
    /// Optional provider or venue identifier associated with this position.
    #[serde(default)]
    pub trade_id: Option<String>,
    pub ordinal: i64,
    pub dimensions: PositionDimensions,
    pub outcome: f64,
    /// Provider classification, allowing the same configured breakeven tolerance
    /// used during accounting to be preserved. Missing values use exact-zero
    /// classification for backward compatibility.
    #[serde(default)]
    pub outcome_classification: Option<OutcomeClassification>,
    #[serde(default)]
    pub r_multiple: Option<f64>,
    #[serde(default)]
    pub excursions: Option<ExcursionInput>,
    #[serde(default)]
    pub execution: Option<ExecutionDiagnosticsInput>,
}

impl PositionOutcome {
    pub fn classification(&self) -> OutcomeClassification {
        self.outcome_classification.unwrap_or({
            if self.outcome > 0.0 {
                OutcomeClassification::Win
            } else if self.outcome < 0.0 {
                OutcomeClassification::Loss
            } else {
                OutcomeClassification::Breakeven
            }
        })
    }
}

/// Aggregate lifecycle counters supplied by a provider integration.
///
/// These counters are intentionally independent of completed-position rows, so
/// rejected, expired, or still-open candidates can be represented.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LifecycleCounts {
    pub candidates: u64,
    pub accepted: u64,
    pub opened: u64,
    pub completed: u64,
    pub rejected: u64,
    /// Accepted pending entries that reached the typed `Filled` terminal state.
    pub filled: u64,
    /// Accepted pending entries that reached the typed `Cancelled` terminal state.
    pub cancelled: u64,
    /// Accepted pending entries still unfilled when replay ended.
    pub unfilled_at_end: u64,
    pub open_at_end: u64,
}

/// Parser/source coverage supplied by an integration that can observe raw input.
///
/// The status counts partition `raw_messages`. Every parsed message emits at
/// least one signal, and entry signals are a subset of all emitted signals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceCoverageCounts {
    pub raw_messages: u64,
    pub parsed_messages: u64,
    pub skipped_messages: u64,
    pub failed_messages: u64,
    pub emitted_signals: u64,
    pub emitted_entry_signals: u64,
}

impl SourceCoverageCounts {
    pub fn validation_error(self) -> Option<String> {
        let Some(classified) = self
            .parsed_messages
            .checked_add(self.skipped_messages)
            .and_then(|count| count.checked_add(self.failed_messages))
        else {
            return Some("parsed/skipped/failed message counts overflow u64".into());
        };
        if classified != self.raw_messages {
            return Some(format!(
                "raw_messages ({}) must equal parsed_messages + skipped_messages + failed_messages ({classified})",
                self.raw_messages
            ));
        }
        if self.emitted_signals < self.parsed_messages {
            return Some(format!(
                "emitted_signals ({}) cannot be less than parsed_messages ({})",
                self.emitted_signals, self.parsed_messages
            ));
        }
        if self.emitted_entry_signals > self.emitted_signals {
            return Some(format!(
                "emitted_entry_signals ({}) cannot exceed emitted_signals ({})",
                self.emitted_entry_signals, self.emitted_signals
            ));
        }
        None
    }
}

/// Selects grouped positions, including positions that have no group.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupFilter {
    Named(String),
    Ungrouped,
}

/// Typed position filter.
///
/// Values within one field are ORed. Populated fields (and individual tag keys)
/// are ANDed with each other. Empty fields impose no constraint.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionFilter {
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub sides: Vec<PositionSide>,
    #[serde(default)]
    pub groups: Vec<GroupFilter>,
    #[serde(default)]
    pub close_reasons: Vec<String>,
    /// Each key is a separate dimension. Values for that key are ORed.
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<String>>,
}

impl PositionFilter {
    pub fn matches(&self, position: &PositionOutcome) -> bool {
        let dimensions = &position.dimensions;

        let symbol_matches = self.symbols.is_empty()
            || self
                .symbols
                .iter()
                .any(|symbol| symbol == &dimensions.symbol);
        let side_matches = self.sides.is_empty() || self.sides.contains(&dimensions.side);
        let group_matches = self.groups.is_empty()
            || self.groups.iter().any(|group| match group {
                GroupFilter::Named(name) => dimensions.group.as_ref() == Some(name),
                GroupFilter::Ungrouped => dimensions.group.is_none(),
            });
        let close_reason_matches = self.close_reasons.is_empty()
            || self.close_reasons.iter().any(|expected| {
                dimensions
                    .close_reasons
                    .iter()
                    .any(|actual| actual == expected)
            });
        let tags_match = self.tags.iter().all(|(key, accepted_values)| {
            accepted_values.is_empty()
                || dimensions
                    .tags
                    .get(key)
                    .is_some_and(|actual| accepted_values.iter().any(|value| value == actual))
        });

        symbol_matches && side_matches && group_matches && close_reason_matches && tags_match
    }
}

/// A requested categorical breakdown. Duplicate requests are evaluated once.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakdownDimension {
    Symbol,
    Side,
    Group,
    CloseReason,
    Tag(String),
}

/// Typed and sortable breakdown key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakdownValue {
    Text(String),
    Side(PositionSide),
    Missing,
}

/// Configuration for deterministic bootstrap confidence intervals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct BootstrapConfig {
    pub samples: usize,
    pub confidence_level: f64,
    pub seed: u64,
    pub minimum_sample_size: usize,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            samples: 2_000,
            confidence_level: 0.95,
            seed: 0xA076_1D64_78BD_642F,
            minimum_sample_size: 5,
        }
    }
}

/// Provider and source identifiers attached to an evaluation without changing
/// the normalized position rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EvaluationContext {
    pub provider_id: Option<String>,
    pub source_id: Option<String>,
}

impl EvaluationContext {
    pub fn is_empty(&self) -> bool {
        self.provider_id.is_none() && self.source_id.is_none()
    }
}

/// Independently selectable provider-report sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationSection {
    Coverage,
    PositionPerformance,
    RMetrics,
    Excursions,
    Execution,
    Robustness,
    Breakdowns,
}

impl EvaluationSection {
    pub const ALL: [Self; 7] = [
        Self::Coverage,
        Self::PositionPerformance,
        Self::RMetrics,
        Self::Excursions,
        Self::Execution,
        Self::Robustness,
        Self::Breakdowns,
    ];

    pub fn all() -> BTreeSet<Self> {
        Self::ALL.into_iter().collect()
    }
}

fn default_evaluation_sections() -> BTreeSet<EvaluationSection> {
    EvaluationSection::all()
}

const fn default_rolling_window() -> usize {
    20
}

const fn default_minimum_breakdown_bucket_count() -> usize {
    1
}

/// Typed report configuration, deliberately separate from normalized position
/// and lifecycle inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EvaluationOptions {
    pub context: EvaluationContext,
    /// Optional parser/source funnel counts. Absence means source coverage was unavailable.
    pub source_coverage: Option<SourceCoverageCounts>,
    /// Missing selectors request all sections; an explicit empty set requests none.
    #[serde(default = "default_evaluation_sections")]
    pub sections: BTreeSet<EvaluationSection>,
    pub filter: PositionFilter,
    pub breakdowns: Vec<BreakdownDimension>,
    pub bootstrap: BootstrapConfig,
    /// Number of chronologically ordered completed positions per rolling window.
    pub rolling_window: usize,
    /// Buckets with fewer selected positions are omitted before row limiting.
    pub minimum_breakdown_bucket_count: usize,
    /// Global deterministic cap across all requested breakdown bucket rows.
    pub maximum_breakdown_rows: Option<usize>,
    /// Include normalized rows selected by the same evaluation filter.
    pub include_position_rows: bool,
    /// Deterministic cap for included normalized position rows.
    pub maximum_position_rows: Option<usize>,
}

impl Default for EvaluationOptions {
    fn default() -> Self {
        Self {
            context: EvaluationContext::default(),
            source_coverage: None,
            sections: EvaluationSection::all(),
            filter: PositionFilter::default(),
            breakdowns: Vec::new(),
            bootstrap: BootstrapConfig::default(),
            rolling_window: default_rolling_window(),
            minimum_breakdown_bucket_count: default_minimum_breakdown_bucket_count(),
            maximum_breakdown_rows: None,
            include_position_rows: false,
            maximum_position_rows: None,
        }
    }
}

/// Complete input to [`super::evaluate`]. `options` is flattened so payloads
/// produced before `EvaluationOptions` was introduced retain the same serde shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EvaluationRequest {
    pub positions: Vec<PositionOutcome>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleCounts>,
    #[serde(flatten, default)]
    pub options: EvaluationOptions,
}

impl std::ops::Deref for EvaluationRequest {
    type Target = EvaluationOptions;

    fn deref(&self) -> &Self::Target {
        &self.options
    }
}

impl std::ops::DerefMut for EvaluationRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.options
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ConfidenceInterval {
    pub estimate: f64,
    pub lower: f64,
    pub upper: f64,
    pub confidence_level: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoverageSection {
    pub provided_positions: usize,
    pub selected_positions: usize,
    pub filtered_out_positions: usize,
    pub valid_outcomes: usize,
    pub invalid_outcomes: usize,
    /// `None` explicitly means raw parser/source outcomes were unavailable.
    pub source: Option<SourceCoverageCounts>,
    pub lifecycle: Option<LifecycleCounts>,
    pub acceptance_rate: MetricValue<f64>,
    pub open_rate: MetricValue<f64>,
    pub completion_rate: MetricValue<f64>,
    pub r_coverage: MetricValue<f64>,
    pub excursion_coverage: MetricValue<f64>,
    pub execution_coverage: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionPerformanceSection {
    pub position_count: usize,
    pub wins: usize,
    pub losses: usize,
    pub breakeven: usize,
    pub total_outcome: MetricValue<f64>,
    pub mean_outcome: MetricValue<f64>,
    pub median_outcome: MetricValue<f64>,
    pub win_rate: MetricValue<f64>,
    pub win_rate_confidence: MetricValue<ConfidenceInterval>,
    pub gross_positive: MetricValue<f64>,
    pub gross_negative: MetricValue<f64>,
    pub profit_factor: MetricValue<f64>,
    pub payoff_ratio: MetricValue<f64>,
    pub best_outcome: MetricValue<f64>,
    pub worst_outcome: MetricValue<f64>,
    pub mean_outcome_confidence: MetricValue<ConfidenceInterval>,
}

/// Deterministic type-7 quantiles of finite realized-R observations.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RQuantiles {
    pub p05: f64,
    pub p10: f64,
    pub p25: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
}

/// One chronologically ordered point on the cumulative realized-R curve.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CumulativeRPoint {
    pub position_id: String,
    pub ordinal: i64,
    pub realized_r: f64,
    pub cumulative_r: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RMetricsSection {
    pub observed_count: usize,
    pub missing_or_invalid_count: usize,
    pub total_r: MetricValue<f64>,
    pub mean_r: MetricValue<f64>,
    pub median_r: MetricValue<f64>,
    pub standard_deviation_r: MetricValue<f64>,
    pub positive_r_rate: MetricValue<f64>,
    pub positive_r_rate_confidence: MetricValue<ConfidenceInterval>,
    pub mean_r_confidence: MetricValue<ConfidenceInterval>,
    /// Sum of positive R divided by the absolute sum of negative R.
    #[serde(default)]
    pub profit_factor: MetricValue<f64>,
    #[serde(default)]
    pub average_winner_r: MetricValue<f64>,
    /// Arithmetic mean of negative R observations (retains its negative sign).
    #[serde(default)]
    pub average_loser_r: MetricValue<f64>,
    #[serde(default)]
    pub best_r: MetricValue<f64>,
    #[serde(default)]
    pub worst_r: MetricValue<f64>,
    #[serde(default)]
    pub quantiles: MetricValue<RQuantiles>,
    /// Ordered by `(ordinal, position_id, realized_r)` for deterministic output.
    #[serde(default)]
    pub cumulative_r_curve: MetricValue<Vec<CumulativeRPoint>>,
    /// Largest peak-to-trough decline on the cumulative realized-R curve.
    #[serde(default)]
    pub max_realized_r_drawdown: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExcursionMetricsSection {
    pub favorable_observed_count: usize,
    pub adverse_observed_count: usize,
    pub mean_favorable_r: MetricValue<f64>,
    pub median_favorable_r: MetricValue<f64>,
    pub mean_adverse_r: MetricValue<f64>,
    pub median_adverse_r: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionDiagnosticsSection {
    pub positions_with_diagnostics: usize,
    pub slippage_observed_count: usize,
    pub latency_observed_count: usize,
    pub fill_ratio_observed_count: usize,
    pub mean_slippage_bps: MetricValue<f64>,
    pub median_slippage_bps: MetricValue<f64>,
    pub adverse_slippage_rate: MetricValue<f64>,
    pub mean_latency_ms: MetricValue<f64>,
    pub median_latency_ms: MetricValue<f64>,
    pub mean_fill_ratio: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemovalImpact {
    pub removed_count: usize,
    pub original_total: f64,
    pub removed_total: f64,
    pub remaining_total: f64,
    pub remaining_mean: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollingOutcome {
    pub start_ordinal: i64,
    pub end_ordinal: i64,
    pub position_count: usize,
    pub total_outcome: f64,
    pub mean_outcome: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollingOutcomes {
    pub window_size: usize,
    pub windows: Vec<RollingOutcome>,
    pub worst_window_mean: MetricValue<f64>,
    pub best_window_mean: MetricValue<f64>,
    pub positive_window_rate: MetricValue<f64>,
}

/// Shares of gross positive completed-position P&L contributed by the top N winners.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PnlConcentrationSection {
    pub top_1: MetricValue<f64>,
    pub top_3: MetricValue<f64>,
    pub top_5: MetricValue<f64>,
    pub top_10: MetricValue<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntrinsicRobustnessSection {
    pub best_one_removed: MetricValue<RemovalImpact>,
    pub best_five_percent_removed: MetricValue<RemovalImpact>,
    /// Share of gross positive outcome contributed by the best position.
    pub best_one_positive_concentration: MetricValue<f64>,
    /// Share of gross positive outcome contributed by the best 5% of positions.
    pub best_five_percent_positive_concentration: MetricValue<f64>,
    /// Fixed-count concentration complements the sample-size-relative 5% metric.
    #[serde(default)]
    pub pnl_concentration: PnlConcentrationSection,
    pub rolling_outcomes: RollingOutcomes,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BreakdownBucket {
    pub value: BreakdownValue,
    pub performance: PositionPerformanceSection,
    pub r_metrics: RMetricsSection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationBreakdown {
    pub dimension: BreakdownDimension,
    /// Sorted by `BreakdownValue`; this ordering does not depend on hash seeds or
    /// source position order.
    pub buckets: Vec<BreakdownBucket>,
}

/// Visibility into minimum-count filtering and global breakdown row truncation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BreakdownRowSummary {
    pub available_rows: usize,
    pub included_rows: usize,
    pub truncated: bool,
}

impl BreakdownRowSummary {
    pub fn is_empty(&self) -> bool {
        self.available_rows == 0 && self.included_rows == 0 && !self.truncated
    }
}

/// Filtered normalized position rows included for metric reconciliation.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EvaluationPositionRows {
    pub available_rows: usize,
    pub included_rows: usize,
    pub truncated: bool,
    pub rows: Vec<PositionOutcome>,
}

fn requested_sections_default() -> BTreeSet<EvaluationSection> {
    EvaluationSection::all()
}

fn requested_all_sections(sections: &BTreeSet<EvaluationSection>) -> bool {
    *sections == EvaluationSection::all()
}

/// Provider-evaluation result. It intentionally has no aggregate score, rank, or
/// rating; consumers decide which individual sections matter for their use case.
///
/// Requested sections serialize exactly as they did before section selection was
/// introduced. Unrequested sections are omitted and remain `None` when decoded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvaluationReport {
    #[serde(default, skip_serializing_if = "EvaluationContext::is_empty")]
    pub context: EvaluationContext,
    #[serde(
        default = "requested_sections_default",
        skip_serializing_if = "requested_all_sections"
    )]
    pub requested_sections: BTreeSet<EvaluationSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage: Option<CoverageSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_performance: Option<PositionPerformanceSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r_metrics: Option<RMetricsSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excursions: Option<ExcursionMetricsSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionDiagnosticsSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub robustness: Option<IntrinsicRobustnessSection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakdowns: Option<Vec<EvaluationBreakdown>>,
    #[serde(default, skip_serializing_if = "BreakdownRowSummary::is_empty")]
    pub breakdown_rows: BreakdownRowSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_rows: Option<EvaluationPositionRows>,
}
