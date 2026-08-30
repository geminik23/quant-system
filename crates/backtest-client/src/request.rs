use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, Write};

use qs_backtest_api::{
    BacktestConfigMsg, BacktestRunSpec, FutureQuoteConfigMsg, ManagementProfileMsg,
    MtmOutputPolicyMsg, ProviderEvaluationOptionsMsg, ResultDeliveryMsg, RunBacktestRequest,
    SizingPolicyMsg, SubmitBacktestRequest, canonical_backtest_timestamp, parse_backtest_timestamp,
};
use serde::{Deserialize, Serialize};

use crate::{
    CanonicalDateFilter, InspectedSignalInput, PreparationCancellation, ResultDeliverySummary,
    ResultInputMetadata, SignalFileSummary, WorkflowError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolScope {
    Single(String),
    Multiple(Vec<String>),
    AllFromEntries,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum SymbolScopeSummary {
    Single { symbol: String },
    Multiple { symbols: Vec<String> },
    AllFromEntries,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalDataType {
    Tick,
    Bar,
}

impl HistoricalDataType {
    fn as_wire(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Bar => "bar",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ProfileSelection {
    None,
    Named(String),
    Inline(ManagementProfileMsg),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileSelectionSummary {
    None,
    Named(String),
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FillModel {
    BidAsk,
    AskOnly,
    MidPrice,
}

impl FillModel {
    fn as_wire(self) -> &'static str {
        match self {
            Self::BidAsk => "BidAsk",
            Self::AskOnly => "AskOnly",
            Self::MidPrice => "MidPrice",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BacktestRunOptions {
    pub symbol_scope: SymbolScope,
    pub exchange: String,
    pub data_type: HistoricalDataType,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub profile: ProfileSelection,
    pub account_currency: Option<String>,
    pub initial_balance: f64,
    pub close_on_finish: bool,
    pub fill_model: FillModel,
    pub sizing: Option<SizingPolicyMsg>,
    pub future: FutureQuoteConfigMsg,
}

pub struct PrepareBacktestInput {
    pub inspected: InspectedSignalInput,
    pub run: BacktestRunOptions,
    pub evaluation: ProviderEvaluationOptionsMsg,
    pub result_delivery: ResultDeliveryMsg,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BacktestRequestSummary {
    pub symbol_scope: SymbolScopeSummary,
    pub exchange: String,
    pub data_type: HistoricalDataType,
    pub timeframe: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub profile: ProfileSelectionSummary,
    pub account_currency: String,
    pub initial_balance: f64,
    pub close_on_finish: bool,
    pub fill_model: FillModel,
    pub signal_count: u64,
    pub signal_latency_ms: i64,
    pub slippage_pips: f64,
    pub stale_quote_after_ms: Option<i64>,
    pub conversion_stale_after_ms: i64,
    pub result_delivery: ResultDeliverySummary,
}

pub struct PreparedBacktest {
    request: SubmitBacktestRequest,
    input_summary: SignalFileSummary,
    input_metadata: ResultInputMetadata,
    request_summary: BacktestRequestSummary,
    serialized_request_bytes: usize,
}

impl fmt::Debug for PreparedBacktest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedBacktest")
            .field("input_summary", &self.input_summary)
            .field("input_metadata", &self.input_metadata)
            .field("request_summary", &self.request_summary)
            .field("serialized_request_bytes", &self.serialized_request_bytes)
            .finish_non_exhaustive()
    }
}

impl PreparedBacktest {
    pub fn request(&self) -> &SubmitBacktestRequest {
        &self.request
    }

    pub fn input_summary(&self) -> &SignalFileSummary {
        &self.input_summary
    }

    pub fn input_metadata(&self) -> &ResultInputMetadata {
        &self.input_metadata
    }

    pub fn request_summary(&self) -> &BacktestRequestSummary {
        &self.request_summary
    }

    pub fn serialized_request_bytes(&self) -> usize {
        self.serialized_request_bytes
    }

    pub fn into_request(self) -> SubmitBacktestRequest {
        self.request
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BacktestPreparer;

impl BacktestPreparer {
    pub async fn prepare(
        &self,
        input: PrepareBacktestInput,
        cancellation: PreparationCancellation,
    ) -> Result<PreparedBacktest, WorkflowError> {
        cancellation.check()?;
        tokio::task::spawn_blocking(move || prepare_blocking(input, cancellation))
            .await
            .map_err(|error| WorkflowError::PreparationTask {
                detail: error.to_string(),
            })?
    }
}

fn prepare_blocking(
    input: PrepareBacktestInput,
    cancellation: PreparationCancellation,
) -> Result<PreparedBacktest, WorkflowError> {
    cancellation.check()?;
    let (raw_signals, source_coverage, filter, input_summary, limits) =
        input.inspected.into_parts();
    let canonical_run_filter = canonical_run_filter(&input.run)?;
    if canonical_run_filter != filter {
        return Err(WorkflowError::InvalidConfiguration {
            field: "date range",
            detail: "run date range does not match the inspected input snapshot".into(),
        });
    }

    let (symbol, symbols, all_symbols, symbol_scope_summary) =
        resolve_symbol_scope(input.run.symbol_scope)?;
    let exchange = input.run.exchange.trim().to_owned();
    if exchange.is_empty() {
        return Err(invalid("exchange", "must not be empty"));
    }
    let timeframe = validate_timeframe(input.run.data_type, input.run.timeframe)?;
    if !input.run.initial_balance.is_finite() || input.run.initial_balance <= 0.0 {
        return Err(invalid("initial balance", "must be finite and positive"));
    }
    validate_sizing(input.run.sizing.as_ref(), input_summary.entry_count > 0)?;
    let account_currency = normalize_currency(input.run.account_currency.as_deref())?;
    let mut future = input.run.future;
    future.account_currency.clone_from(&account_currency);
    validate_future(&future, input.result_delivery)?;

    let (profile, profile_def, profile_summary) = match input.run.profile {
        ProfileSelection::None => (None, None, ProfileSelectionSummary::None),
        ProfileSelection::Named(name) => {
            let name = name.trim().to_owned();
            if name.is_empty() {
                return Err(invalid("profile", "named profile must not be empty"));
            }
            (
                Some(name.clone()),
                None,
                ProfileSelectionSummary::Named(name),
            )
        }
        ProfileSelection::Inline(profile) => (None, Some(profile), ProfileSelectionSummary::Inline),
    };

    let mut evaluation = input.evaluation;
    evaluation.source_coverage = source_coverage;
    let result_delivery_summary = match input.result_delivery {
        ResultDeliveryMsg::Auto => ResultDeliverySummary::Auto,
        ResultDeliveryMsg::Inline => ResultDeliverySummary::Inline,
        ResultDeliveryMsg::Artifact => ResultDeliverySummary::Artifact,
    };
    let request_summary = BacktestRequestSummary {
        symbol_scope: symbol_scope_summary,
        exchange: exchange.clone(),
        data_type: input.run.data_type,
        timeframe: timeframe.clone(),
        from: filter.from.clone(),
        to: filter.to.clone(),
        profile: profile_summary,
        account_currency: account_currency.clone(),
        initial_balance: input.run.initial_balance,
        close_on_finish: input.run.close_on_finish,
        fill_model: input.run.fill_model,
        signal_count: raw_signals.len() as u64,
        signal_latency_ms: future.signal_latency_ms,
        slippage_pips: future.slippage_pips,
        stale_quote_after_ms: future.stale_quote_after_ms,
        conversion_stale_after_ms: future.conversion_stale_after_ms,
        result_delivery: result_delivery_summary,
    };
    let request = SubmitBacktestRequest {
        request: RunBacktestRequest {
            request: BacktestRunSpec {
                symbol,
                symbols,
                all_symbols,
                exchange,
                data_type: input.run.data_type.as_wire().into(),
                timeframe,
                from: filter.from,
                to: filter.to,
                raw_signals,
                profile,
                profile_def,
                config: BacktestConfigMsg {
                    initial_balance: Some(input.run.initial_balance),
                    close_on_finish: Some(input.run.close_on_finish),
                    fill_model: Some(input.run.fill_model.as_wire().into()),
                    sizing: input.run.sizing,
                },
            },
            future,
            evaluation,
            result_delivery: input.result_delivery,
        },
    };

    cancellation.check()?;
    let serialized_request_bytes = measure_serialized_request(
        &request,
        limits.maximum_serialized_request_bytes,
        &cancellation,
    )?;
    cancellation.check()?;
    let input_metadata = ResultInputMetadata::from(&input_summary);
    Ok(PreparedBacktest {
        request,
        input_summary,
        input_metadata,
        request_summary,
        serialized_request_bytes,
    })
}

struct CappedCountingWriter<'a> {
    count: usize,
    limit: usize,
    exceeded_at: Option<usize>,
    cancelled: bool,
    cancellation: &'a PreparationCancellation,
}

impl Write for CappedCountingWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            self.cancelled = true;
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "request serialization cancelled",
            ));
        }
        let next = self.count.saturating_add(buffer.len());
        if next > self.limit {
            self.exceeded_at = Some(next);
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "serialized request limit exceeded",
            ));
        }
        self.count = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn measure_serialized_request(
    request: &SubmitBacktestRequest,
    limit: usize,
    cancellation: &PreparationCancellation,
) -> Result<usize, WorkflowError> {
    let mut writer = CappedCountingWriter {
        count: 0,
        limit,
        exceeded_at: None,
        cancelled: false,
        cancellation,
    };
    match serde_json::to_writer(&mut writer, request) {
        Ok(()) => Ok(writer.count),
        Err(_) if writer.cancelled => Err(WorkflowError::PreparationCancelled),
        Err(_) if writer.exceeded_at.is_some() => Err(WorkflowError::RequestTooLarge {
            actual: writer.exceeded_at.unwrap_or(limit.saturating_add(1)),
            limit,
        }),
        Err(error) => Err(WorkflowError::RequestSerialization {
            detail: error.to_string(),
        }),
    }
}

fn canonical_run_filter(run: &BacktestRunOptions) -> Result<CanonicalDateFilter, WorkflowError> {
    let from = run
        .from
        .as_deref()
        .map(canonical_backtest_timestamp)
        .transpose()
        .map_err(|error| invalid("from", &error.to_string()))?;
    let to = run
        .to
        .as_deref()
        .map(canonical_backtest_timestamp)
        .transpose()
        .map_err(|error| invalid("to", &error.to_string()))?;
    let parsed_from = from
        .as_deref()
        .map(parse_backtest_timestamp)
        .transpose()
        .map_err(|error| invalid("from", &error.to_string()))?;
    let parsed_to = to
        .as_deref()
        .map(parse_backtest_timestamp)
        .transpose()
        .map_err(|error| invalid("to", &error.to_string()))?;
    if parsed_from
        .zip(parsed_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(invalid(
            "date range",
            "from must not be later than to after UTC normalization",
        ));
    }
    Ok(CanonicalDateFilter { from, to })
}

fn resolve_symbol_scope(
    scope: SymbolScope,
) -> Result<(String, Vec<String>, bool, SymbolScopeSummary), WorkflowError> {
    match scope {
        SymbolScope::Single(symbol) => {
            let symbol = symbol.trim().to_owned();
            if symbol.is_empty() {
                return Err(invalid("symbol scope", "single symbol must not be empty"));
            }
            Ok((
                symbol.clone(),
                Vec::new(),
                false,
                SymbolScopeSummary::Single { symbol },
            ))
        }
        SymbolScope::Multiple(symbols) => {
            let mut seen = BTreeSet::new();
            let mut normalized = Vec::with_capacity(symbols.len());
            for symbol in symbols {
                let symbol = symbol.trim().to_owned();
                if symbol.is_empty() {
                    return Err(invalid(
                        "symbol scope",
                        "multiple symbols must not contain an empty value",
                    ));
                }
                if !seen.insert(symbol.clone()) {
                    return Err(invalid(
                        "symbol scope",
                        "multiple symbols must not contain duplicates",
                    ));
                }
                normalized.push(symbol);
            }
            if normalized.is_empty() {
                return Err(invalid(
                    "symbol scope",
                    "multiple symbols must contain at least one value",
                ));
            }
            Ok((
                String::new(),
                normalized.clone(),
                false,
                SymbolScopeSummary::Multiple {
                    symbols: normalized,
                },
            ))
        }
        SymbolScope::AllFromEntries => Ok((
            String::new(),
            Vec::new(),
            true,
            SymbolScopeSummary::AllFromEntries,
        )),
    }
}

fn validate_timeframe(
    data_type: HistoricalDataType,
    timeframe: Option<String>,
) -> Result<Option<String>, WorkflowError> {
    match (data_type, timeframe) {
        (HistoricalDataType::Tick, None) => Ok(None),
        (HistoricalDataType::Tick, Some(_)) => Err(invalid(
            "timeframe",
            "tick data must not specify a timeframe",
        )),
        (HistoricalDataType::Bar, Some(timeframe)) => {
            let timeframe = timeframe.trim().to_owned();
            if timeframe.is_empty() {
                Err(invalid("timeframe", "bar data requires a timeframe"))
            } else {
                Ok(Some(timeframe))
            }
        }
        (HistoricalDataType::Bar, None) => {
            Err(invalid("timeframe", "bar data requires a timeframe"))
        }
    }
}

fn validate_sizing(sizing: Option<&SizingPolicyMsg>, has_entry: bool) -> Result<(), WorkflowError> {
    if has_entry && sizing.is_none() {
        return Err(invalid("sizing", "Entry signals require a sizing policy"));
    }
    let value = match sizing {
        Some(SizingPolicyMsg::FixedLot { lots }) => Some((*lots, "fixed lots")),
        Some(SizingPolicyMsg::FixedRiskAmount { amount }) => Some((*amount, "fixed risk amount")),
        Some(SizingPolicyMsg::BalanceRiskPercent { percent }) => {
            Some((*percent, "balance risk percent"))
        }
        None => None,
    };
    if let Some((value, label)) = value
        && (!value.is_finite() || value <= 0.0)
    {
        return Err(invalid(
            "sizing",
            &format!("{label} must be finite and positive"),
        ));
    }
    Ok(())
}

fn normalize_currency(value: Option<&str>) -> Result<String, WorkflowError> {
    let value = value.ok_or_else(|| {
        invalid(
            "account currency",
            "FutureQuote requests require an account currency",
        )
    })?;
    let value = value.trim();
    if value.len() != 3 || !value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return Err(invalid(
            "account currency",
            "must contain exactly three ASCII letters",
        ));
    }
    Ok(value.to_ascii_uppercase())
}

fn validate_future(
    future: &FutureQuoteConfigMsg,
    result_delivery: ResultDeliveryMsg,
) -> Result<(), WorkflowError> {
    if future.signal_latency_ms < 0 {
        return Err(invalid("signal latency", "must be non-negative"));
    }
    if !future.slippage_pips.is_finite() {
        return Err(invalid("slippage", "must be finite"));
    }
    if future.stale_quote_after_ms.is_some_and(|value| value < 0) {
        return Err(invalid("stale quote age", "must be non-negative"));
    }
    if !future.pnl_epsilon.is_finite() || future.pnl_epsilon < 0.0 {
        return Err(invalid("PnL epsilon", "must be finite and non-negative"));
    }
    if future.conversion_stale_after_ms < 0 {
        return Err(invalid("conversion stale age", "must be non-negative"));
    }
    if let MtmOutputPolicyMsg::Bounded { max_points } = future.mtm_output
        && !(8..=16_384).contains(&max_points)
    {
        return Err(invalid(
            "MTM output",
            "bounded max_points must be between 8 and 16384",
        ));
    }
    if matches!(future.mtm_output, MtmOutputPolicyMsg::Full)
        && result_delivery == ResultDeliveryMsg::Inline
    {
        return Err(invalid(
            "result delivery",
            "full MTM output cannot force inline delivery",
        ));
    }
    Ok(())
}

fn invalid(field: &'static str, detail: &str) -> WorkflowError {
    WorkflowError::InvalidConfiguration {
        field,
        detail: detail.into(),
    }
}
