use std::collections::BTreeMap;
use std::io::Read;

use chrono::{DateTime, Utc};
use qs_backtest_api::BacktestResultMsg;
use serde::{Deserialize, Serialize};

use thiserror::Error;

use crate::{BacktestRequestSummary, ResultInputMetadata};

pub const RESULT_DOCUMENT_TYPE: &str = "quant-system-backtest-result";
pub const RESULT_DOCUMENT_FORMAT_VERSION: u32 = 1;
pub const ANALYSIS_DATASET_FORMAT_VERSION: u32 = 1;
pub const EXECUTION_DATASET_FORMAT_VERSION: u32 = 1;
pub const SUPPORTED_FUTURE_RESULT_FORMAT_VERSION: u32 = 1;

pub const DEFAULT_MAXIMUM_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAXIMUM_DECODED_PAYLOAD_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAXIMUM_RESULT_DOCUMENT_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_MAXIMUM_ARTIFACT_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultIoLimits {
    pub maximum_artifact_bytes: u64,
    pub maximum_decoded_payload_bytes: u64,
    pub maximum_result_document_bytes: u64,
    pub maximum_artifact_chunk_bytes: u64,
}

impl Default for ResultIoLimits {
    fn default() -> Self {
        Self {
            maximum_artifact_bytes: DEFAULT_MAXIMUM_ARTIFACT_BYTES,
            maximum_decoded_payload_bytes: DEFAULT_MAXIMUM_DECODED_PAYLOAD_BYTES,
            maximum_result_document_bytes: DEFAULT_MAXIMUM_RESULT_DOCUMENT_BYTES,
            maximum_artifact_chunk_bytes: DEFAULT_MAXIMUM_ARTIFACT_CHUNK_BYTES,
        }
    }
}

impl ResultIoLimits {
    pub fn validate(self) -> Result<Self, ResultDocumentError> {
        for (resource, value) in [
            ("artifact", self.maximum_artifact_bytes),
            ("decoded payload", self.maximum_decoded_payload_bytes),
            ("result document", self.maximum_result_document_bytes),
            ("artifact chunk", self.maximum_artifact_chunk_bytes),
        ] {
            if value == 0 {
                return Err(ResultDocumentError::InvalidLimit { resource });
            }
        }
        if self.maximum_decoded_payload_bytes > self.maximum_result_document_bytes {
            return Err(ResultDocumentError::InvalidLimitRelationship);
        }
        if self.maximum_artifact_chunk_bytes > self.maximum_artifact_bytes {
            return Err(ResultDocumentError::InvalidLimitRelationship);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisUnavailableReason {
    LegacyResult,
    PositionRowsOmitted,
    PositionRowsTruncated,
    AnalysisFeatureDisabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum AnalysisDatasetState {
    Complete(Box<PersistedAnalysisDataset>),
    Unavailable { reason: AnalysisUnavailableReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionDatasetUnavailableReason {
    LegacyOrOmitted,
    AnalysisFeatureDisabled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum PersistedExecutionDatasetState {
    Complete(Box<PersistedExecutionDataset>),
    Unavailable {
        reason: ExecutionDatasetUnavailableReason,
    },
}

impl Default for PersistedExecutionDatasetState {
    fn default() -> Self {
        Self::Unavailable {
            reason: ExecutionDatasetUnavailableReason::LegacyOrOmitted,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedAnalysisDataset {
    pub format_version: u32,
    pub positions: Vec<PersistedPositionOutcome>,
    pub lifecycle: Option<PersistedLifecycleCounts>,
    pub source_coverage: Option<PersistedSourceCoverageCounts>,
    pub default_options: PersistedEvaluationOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedPopulationUnit {
    CompletedPosition,
    CloseEvent,
    SourceMessage,
    MtmObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedMetricPopulation {
    pub unit: PersistedPopulationUnit,
    pub filter: PersistedPositionFilter,
    pub provided_count: u64,
    pub eligible_count: u64,
    pub observed_count: u64,
    pub excluded_count: u64,
    pub invalid_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedPositionOutcome {
    pub id: String,
    pub trade_id: Option<String>,
    pub ordinal: i64,
    pub symbol: String,
    pub side: PersistedPositionSide,
    pub group: Option<String>,
    pub close_reasons: Vec<String>,
    pub tags: BTreeMap<String, String>,
    pub outcome: f64,
    pub outcome_classification: Option<PersistedOutcomeClassification>,
    pub r_multiple: Option<f64>,
    pub favorable_r: Option<f64>,
    pub adverse_r: Option<f64>,
    pub slippage_bps: Option<f64>,
    pub latency_ms: Option<f64>,
    pub fill_ratio: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedPositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedOutcomeClassification {
    Win,
    Loss,
    Breakeven,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedLifecycleCounts {
    pub candidates: u64,
    pub accepted: u64,
    pub opened: u64,
    pub completed: u64,
    pub rejected: u64,
    pub filled: u64,
    pub cancelled: u64,
    pub unfilled_at_end: u64,
    pub open_at_end: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedSourceCoverageCounts {
    pub raw_messages: u64,
    pub parsed_messages: u64,
    pub skipped_messages: u64,
    pub failed_messages: u64,
    pub emitted_signals: u64,
    pub emitted_entry_signals: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedEvaluationOptions {
    pub provider_id: Option<String>,
    pub source_id: Option<String>,
    pub sections: Vec<String>,
    pub filter: PersistedPositionFilter,
    pub breakdowns: Vec<String>,
    pub bootstrap_samples: usize,
    pub bootstrap_confidence_level: f64,
    pub bootstrap_seed: u64,
    pub bootstrap_minimum_sample_size: usize,
    pub rolling_window: usize,
    pub minimum_breakdown_bucket_count: usize,
    pub maximum_breakdown_rows: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedPositionFilter {
    pub symbols: Vec<String>,
    pub sides: Vec<PersistedPositionSide>,
    pub groups: Vec<PersistedGroupFilter>,
    pub close_reasons: Vec<String>,
    pub tags: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedGroupFilter {
    Named(String),
    Ungrouped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedExecutionDataset {
    pub format_version: u32,
    pub fills: Vec<PersistedFill>,
    pub action_dispositions: Vec<PersistedActionDisposition>,
    pub close_events: Vec<PersistedCloseEvent>,
    pub completed_positions: Vec<PersistedCompletedPosition>,
    pub open_positions: Vec<PersistedOpenPosition>,
    pub pending_orders: Vec<PersistedPendingOrder>,
    pub pending_lifecycle: Vec<PersistedPendingLifecycleEvent>,
    pub risk_tranches: Vec<PersistedRiskTranche>,
    pub conversion_audits: Vec<PersistedConversionAudit>,
    pub completeness: PersistedExecutionCompleteness,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedCollectionCompleteness {
    pub available: bool,
    pub source_count: usize,
    pub included_count: usize,
    pub truncated: bool,
}

impl PersistedCollectionCompleteness {
    pub fn complete(count: usize) -> Self {
        Self {
            available: true,
            source_count: count,
            included_count: count,
            truncated: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedExecutionCompleteness {
    pub fills: PersistedCollectionCompleteness,
    pub action_dispositions: PersistedCollectionCompleteness,
    pub close_events: PersistedCollectionCompleteness,
    pub completed_positions: PersistedCollectionCompleteness,
    pub open_positions: PersistedCollectionCompleteness,
    pub pending_orders: PersistedCollectionCompleteness,
    pub pending_lifecycle: PersistedCollectionCompleteness,
    pub risk_tranches: PersistedCollectionCompleteness,
    pub conversion_audits: PersistedCollectionCompleteness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedTradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedOrderType {
    Market,
    Limit,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedFillPurpose {
    MarketEntry,
    MarketExit,
    LimitEntry,
    StopEntry,
    StopLoss,
    TakeProfit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedDispositionStatus {
    Applied,
    Skipped,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedCloseReason {
    Stoploss,
    Target,
    TrailingStop,
    TimeExit,
    BreakevenStop,
    Manual,
    EndOfData,
    GroupRule,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedNetPnlOutcome {
    Win,
    Loss,
    Breakeven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedRiskBasisStatus {
    Available,
    Partial,
    MissingStop,
    InvalidInput,
    NonProtectiveStop,
    ZeroRisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedPendingLifecycleState {
    Placed,
    Filled,
    Cancelled,
    UnfilledAtEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedFxPairDirection {
    Direct,
    Inverse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedConversionPriceSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedFxPair {
    pub symbol: String,
    pub base_currency: String,
    pub quote_currency: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedConversionRouteLeg {
    pub pair: PersistedFxPair,
    pub direction: PersistedFxPairDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PersistedConversionRoute {
    Identity {
        currency: String,
    },
    Direct {
        pair: PersistedFxPair,
    },
    Inverse {
        pair: PersistedFxPair,
    },
    TwoLeg {
        pivot_currency: String,
        first: PersistedConversionRouteLeg,
        second: PersistedConversionRouteLeg,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedFill {
    pub id: String,
    pub action_id: Option<String>,
    pub position_id: String,
    pub symbol: String,
    pub signal_ts: Option<String>,
    pub effective_ts: String,
    pub execution_ts: Option<String>,
    pub quote_ts: String,
    pub quote_age_millis: Option<i64>,
    pub size: f64,
    pub bid: f64,
    pub ask: f64,
    pub purpose: PersistedFillPurpose,
    pub side: PersistedTradeSide,
    pub price: f64,
    pub quote_price: f64,
    pub requested_price: Option<f64>,
    pub slippage_pips: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedActionDisposition {
    pub action_id: String,
    pub action_kind: Option<String>,
    pub signal_ts: Option<String>,
    pub effective_ts: Option<String>,
    pub status: PersistedDispositionStatus,
    pub reason: Option<String>,
    pub position_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedCloseEvent {
    pub id: String,
    pub action_id: Option<String>,
    pub fill_id: Option<String>,
    pub position_id: String,
    pub symbol: String,
    pub side: PersistedTradeSide,
    pub ts: String,
    pub size: f64,
    pub price: f64,
    pub entry_price: Option<f64>,
    pub pnl: f64,
    pub native_pnl: Option<f64>,
    pub native_currency: Option<String>,
    pub reason: PersistedCloseReason,
    pub remaining_size: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedCompletedPosition {
    pub position_id: String,
    pub symbol: String,
    pub side: PersistedTradeSide,
    pub group: Option<String>,
    pub trade_id: Option<String>,
    pub open_ts: String,
    pub close_ts: String,
    pub entry_size: f64,
    pub average_entry_price: f64,
    pub net_pnl: f64,
    pub native_net_pnl: Option<f64>,
    pub native_currency: Option<String>,
    pub outcome: PersistedNetPnlOutcome,
    pub initial_stop: Option<f64>,
    pub effective_stop: Option<PersistedEffectiveStop>,
    pub risk_basis_status: PersistedRiskBasisStatus,
    pub realized_r: Option<f64>,
    pub mae: Option<f64>,
    pub mfe: Option<f64>,
    pub close_reasons: Vec<PersistedCloseReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedOpenPosition {
    pub position_id: String,
    pub symbol: String,
    pub side: PersistedTradeSide,
    pub group: Option<String>,
    pub trade_id: Option<String>,
    pub open_ts: Option<String>,
    pub average_entry_price: f64,
    pub remaining_size: f64,
    pub initial_stop: Option<f64>,
    pub effective_stop: Option<PersistedEffectiveStop>,
    pub realized_pnl: f64,
    pub unrealized_pnl: Option<f64>,
    pub gross_exposure: Option<f64>,
    pub open_risk: Option<f64>,
    pub campaign_mae: Option<f64>,
    pub campaign_mfe: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PersistedEffectiveStop {
    pub price: f64,
    pub origin: PersistedStopOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedStopOrigin {
    Initial,
    Modified,
    Breakeven,
    Trailing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedPendingOrder {
    pub position_id: String,
    pub action_id: Option<String>,
    pub symbol: String,
    pub side: PersistedTradeSide,
    pub order_type: PersistedOrderType,
    pub requested_price: Option<f64>,
    pub size: f64,
    pub signal_ts: Option<String>,
    pub effective_ts: Option<String>,
    pub initial_stop: Option<f64>,
    pub group: Option<String>,
    pub trade_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedPendingLifecycleEvent {
    pub id: String,
    pub sequence: u64,
    pub position_id: String,
    pub placement_action_id: Option<String>,
    pub terminal_action_id: Option<String>,
    pub state: PersistedPendingLifecycleState,
    pub symbol: String,
    pub side: PersistedTradeSide,
    pub order_type: PersistedOrderType,
    pub requested_size: f64,
    pub filled_size: Option<f64>,
    pub requested_price: Option<f64>,
    pub fill_price: Option<f64>,
    pub signal_ts: Option<String>,
    pub placed_ts: Option<String>,
    pub effective_ts: Option<String>,
    pub terminal_ts: Option<String>,
    pub wait_latency_ms: Option<i64>,
    pub fill_ratio: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedRiskTranche {
    pub position_id: String,
    pub fill_id: Option<String>,
    pub size: f64,
    pub entry_price: f64,
    pub initial_stop: Option<f64>,
    pub contract_size: f64,
    pub risk_per_unit: Option<f64>,
    pub risk_amount: Option<f64>,
    pub native_risk_amount: Option<f64>,
    pub native_currency: Option<String>,
    pub status: PersistedRiskBasisStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedConversionAudit {
    pub context: String,
    pub position_id: String,
    pub from_currency: String,
    pub to_currency: String,
    pub input_amount: f64,
    pub output_amount: f64,
    pub operation_ts: String,
    pub route: PersistedConversionRoute,
    pub legs: Vec<PersistedConversionLeg>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistedConversionLeg {
    pub sequence: usize,
    pub symbol: String,
    pub direction: PersistedFxPairDirection,
    pub from_currency: String,
    pub to_currency: String,
    pub input_amount: f64,
    pub output_amount: f64,
    pub quote_ts: String,
    pub quote_age_millis: i64,
    pub bid: f64,
    pub ask: f64,
    pub price_side: PersistedConversionPriceSide,
    pub executable_price: f64,
    pub conversion_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResultDocument {
    document_type: String,
    format_version: u32,
    pub created_at: DateTime<Utc>,
    pub job_id: Option<String>,
    pub input: ResultInputMetadata,
    pub request: BacktestRequestSummary,
    pub result: BacktestResultMsg,
    pub analysis: AnalysisDatasetState,
    #[serde(default)]
    pub execution: PersistedExecutionDatasetState,
}

impl BacktestResultDocument {
    pub fn new(
        created_at: DateTime<Utc>,
        job_id: Option<String>,
        input: ResultInputMetadata,
        request: BacktestRequestSummary,
        result: BacktestResultMsg,
        analysis: AnalysisDatasetState,
        execution: PersistedExecutionDatasetState,
    ) -> Result<Self, ResultDocumentError> {
        validate_backtest_result(&result)?;
        validate_analysis_state(&analysis)?;
        validate_execution_state(&execution)?;
        Ok(Self {
            document_type: RESULT_DOCUMENT_TYPE.into(),
            format_version: RESULT_DOCUMENT_FORMAT_VERSION,
            created_at,
            job_id,
            input,
            request,
            result,
            analysis,
            execution,
        })
    }

    pub fn document_type(&self) -> &str {
        &self.document_type
    }

    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    pub fn validate(&self) -> Result<(), ResultDocumentError> {
        if self.document_type != RESULT_DOCUMENT_TYPE {
            return Err(ResultDocumentError::WrongDocumentType {
                actual: self.document_type.clone(),
            });
        }
        if self.format_version != RESULT_DOCUMENT_FORMAT_VERSION {
            return Err(ResultDocumentError::UnsupportedDocumentVersion {
                actual: self.format_version,
            });
        }
        validate_backtest_result(&self.result)?;
        validate_analysis_state(&self.analysis)?;
        validate_execution_state(&self.execution)
    }
}

#[derive(Debug, Clone)]
pub enum OpenedResultFile {
    Document(Box<BacktestResultDocument>),
    Legacy(Box<BacktestResultMsg>),
}

impl OpenedResultFile {
    pub fn result(&self) -> &BacktestResultMsg {
        match self {
            Self::Document(document) => &document.result,
            Self::Legacy(result) => result,
        }
    }
}

#[derive(Deserialize)]
struct DocumentDiscriminator {
    #[serde(default)]
    document_type: Option<serde_json::Value>,
}

pub fn decode_result_bytes(bytes: &[u8]) -> Result<OpenedResultFile, ResultDocumentError> {
    let discriminator: DocumentDiscriminator = decode_one(bytes)?;
    match discriminator.document_type {
        Some(serde_json::Value::String(document_type)) if document_type == RESULT_DOCUMENT_TYPE => {
            let document: BacktestResultDocument = decode_one(bytes)?;
            document.validate()?;
            Ok(OpenedResultFile::Document(Box::new(document)))
        }
        Some(actual) => Err(ResultDocumentError::WrongDocumentType {
            actual: actual.to_string(),
        }),
        None => {
            let result: BacktestResultMsg = decode_one(bytes)?;
            validate_backtest_result(&result)?;
            Ok(OpenedResultFile::Legacy(Box::new(result)))
        }
    }
}

pub fn decode_result_reader<R: Read>(
    reader: R,
    maximum_bytes: u64,
) -> Result<OpenedResultFile, ResultDocumentError> {
    let mut bytes = Vec::new();
    let mut bounded = reader.take(maximum_bytes.saturating_add(1));
    bounded
        .read_to_end(&mut bytes)
        .map_err(|error| ResultDocumentError::Io(error.to_string()))?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(ResultDocumentError::DocumentTooLarge {
            maximum: maximum_bytes,
        });
    }
    decode_result_bytes(&bytes)
}

fn decode_one<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ResultDocumentError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| ResultDocumentError::InvalidJson(error.to_string()))?;
    deserializer
        .end()
        .map_err(|error| ResultDocumentError::InvalidJson(error.to_string()))?;
    Ok(value)
}

pub fn validate_backtest_result(result: &BacktestResultMsg) -> Result<(), ResultDocumentError> {
    if let Some(future) = result.future.as_ref()
        && future.format_version != SUPPORTED_FUTURE_RESULT_FORMAT_VERSION
    {
        return Err(ResultDocumentError::UnsupportedEmbeddedResultVersion {
            actual: future.format_version,
        });
    }
    Ok(())
}

fn validate_analysis_state(state: &AnalysisDatasetState) -> Result<(), ResultDocumentError> {
    if let AnalysisDatasetState::Complete(dataset) = state {
        if dataset.format_version != ANALYSIS_DATASET_FORMAT_VERSION {
            return Err(ResultDocumentError::UnsupportedAnalysisVersion {
                actual: dataset.format_version,
            });
        }

        if dataset.positions.iter().any(|position| {
            !position.outcome.is_finite()
                || position.r_multiple.is_some_and(|value| !value.is_finite())
                || position.favorable_r.is_some_and(|value| !value.is_finite())
                || position.adverse_r.is_some_and(|value| !value.is_finite())
        }) {
            return Err(ResultDocumentError::InvalidDataset(
                "analysis positions contain non-finite values".into(),
            ));
        }
    }
    Ok(())
}

fn validate_execution_state(
    state: &PersistedExecutionDatasetState,
) -> Result<(), ResultDocumentError> {
    if let PersistedExecutionDatasetState::Complete(dataset) = state {
        if dataset.format_version != EXECUTION_DATASET_FORMAT_VERSION {
            return Err(ResultDocumentError::UnsupportedExecutionVersion {
                actual: dataset.format_version,
            });
        }
        for (name, completeness, actual) in [
            ("fills", dataset.completeness.fills, dataset.fills.len()),
            (
                "action dispositions",
                dataset.completeness.action_dispositions,
                dataset.action_dispositions.len(),
            ),
            (
                "close events",
                dataset.completeness.close_events,
                dataset.close_events.len(),
            ),
            (
                "completed positions",
                dataset.completeness.completed_positions,
                dataset.completed_positions.len(),
            ),
            (
                "open positions",
                dataset.completeness.open_positions,
                dataset.open_positions.len(),
            ),
            (
                "pending orders",
                dataset.completeness.pending_orders,
                dataset.pending_orders.len(),
            ),
            (
                "pending lifecycle",
                dataset.completeness.pending_lifecycle,
                dataset.pending_lifecycle.len(),
            ),
            (
                "risk tranches",
                dataset.completeness.risk_tranches,
                dataset.risk_tranches.len(),
            ),
            (
                "conversion audits",
                dataset.completeness.conversion_audits,
                dataset.conversion_audits.len(),
            ),
        ] {
            validate_collection(name, completeness, actual)?;
        }
    }
    Ok(())
}

fn validate_collection(
    name: &str,
    completeness: PersistedCollectionCompleteness,
    actual: usize,
) -> Result<(), ResultDocumentError> {
    if !completeness.available
        || completeness.included_count != actual
        || completeness.source_count < completeness.included_count
        || (!completeness.truncated && completeness.source_count != completeness.included_count)
    {
        return Err(ResultDocumentError::InvalidDataset(format!(
            "invalid {name} completeness"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResultDocumentError {
    #[error("{resource} byte limit must be positive")]
    InvalidLimit { resource: &'static str },
    #[error("result I/O limits have an invalid relationship")]
    InvalidLimitRelationship,
    #[error("result document exceeds the {maximum}-byte limit")]
    DocumentTooLarge { maximum: u64 },
    #[error("invalid result JSON: {0}")]
    InvalidJson(String),
    #[error("unexpected result document type '{actual}'")]
    WrongDocumentType { actual: String },
    #[error("unsupported result document version {actual}")]
    UnsupportedDocumentVersion { actual: u32 },
    #[error("unsupported embedded FutureQuote result version {actual}")]
    UnsupportedEmbeddedResultVersion { actual: u32 },
    #[error("unsupported analysis dataset version {actual}")]
    UnsupportedAnalysisVersion { actual: u32 },
    #[error("unsupported execution dataset version {actual}")]
    UnsupportedExecutionVersion { actual: u32 },
    #[error("invalid persisted dataset: {0}")]
    InvalidDataset(String),
    #[error("result I/O error: {0}")]
    Io(String),
}
