use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::NaiveDateTime;
use qs_backtest_api::{
    PositionRefMsg, RawSignalMsg, SourceCoverageCountsMsg, canonical_backtest_timestamp,
    decode_raw_signal_json_strict, parse_backtest_timestamp,
};

use crate::{ResultInputMetadata, WorkflowError};

const MIB: usize = 1024 * 1024;
pub const DEFAULT_MAX_FILE_BYTES: u64 = (8 * MIB) as u64;
pub const DEFAULT_MAX_LINE_BYTES: usize = 256 * 1024;
pub const DEFAULT_MAX_SIGNAL_COUNT: usize = 50_000;
pub const DEFAULT_MAX_SERIALIZED_REQUEST_BYTES: usize = 15 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalInputLimits {
    pub maximum_file_bytes: u64,
    pub maximum_line_bytes: usize,
    pub maximum_signal_count: usize,
    pub maximum_serialized_request_bytes: usize,
}

impl Default for SignalInputLimits {
    fn default() -> Self {
        Self {
            maximum_file_bytes: DEFAULT_MAX_FILE_BYTES,
            maximum_line_bytes: DEFAULT_MAX_LINE_BYTES,
            maximum_signal_count: DEFAULT_MAX_SIGNAL_COUNT,
            maximum_serialized_request_bytes: DEFAULT_MAX_SERIALIZED_REQUEST_BYTES,
        }
    }
}

impl SignalInputLimits {
    fn validate(self) -> Result<Self, WorkflowError> {
        for (field, value) in [
            ("maximum_line_bytes", self.maximum_line_bytes),
            ("maximum_signal_count", self.maximum_signal_count),
            (
                "maximum_serialized_request_bytes",
                self.maximum_serialized_request_bytes,
            ),
        ] {
            if value == 0 {
                return Err(WorkflowError::InvalidConfiguration {
                    field,
                    detail: "must be positive".into(),
                });
            }
        }
        if self.maximum_file_bytes == 0 {
            return Err(WorkflowError::InvalidConfiguration {
                field: "maximum_file_bytes",
                detail: "must be positive".into(),
            });
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalDecodingPolicy {
    Strict,
    Compatibility,
}

pub enum SignalInputSource {
    Path(PathBuf),
    Reader {
        display_name: String,
        reader: Box<dyn Read + Send>,
    },
}

pub struct InspectSignalInput {
    pub signals: SignalInputSource,
    pub source_coverage: Option<SourceCoverageCountsMsg>,
    pub decoding: SignalDecodingPolicy,
    pub limits: SignalInputLimits,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputWarning {
    NoSignals,
    NoSignalsAfterFilter,
    NoEntrySignals,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalFileSummary {
    pub display_name: String,
    pub byte_len: u64,

    pub physical_lines: u64,
    pub non_empty_lines: u64,
    pub signal_count: u64,
    pub retained_signal_count: u64,
    pub entry_count: u64,
    pub symbols: BTreeSet<String>,
    pub minimum_timestamp: Option<String>,
    pub maximum_timestamp: Option<String>,
    pub action_counts: BTreeMap<String, u64>,
    pub warnings: Vec<InputWarning>,
}

impl From<&SignalFileSummary> for ResultInputMetadata {
    fn from(summary: &SignalFileSummary) -> Self {
        Self {
            display_name: summary.display_name.clone(),
            byte_len: summary.byte_len,

            signal_count: summary.signal_count,
            retained_signal_count: summary.retained_signal_count,
            entry_count: summary.entry_count,
            symbols: summary.symbols.iter().cloned().collect(),
            minimum_timestamp: summary.minimum_timestamp.clone(),
            maximum_timestamp: summary.maximum_timestamp.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PreparationCancellation {
    cancelled: Arc<AtomicBool>,
}

impl PreparationCancellation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn check(&self) -> Result<(), WorkflowError> {
        if self.is_cancelled() {
            Err(WorkflowError::PreparationCancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDateFilter {
    pub from: Option<String>,
    pub to: Option<String>,
}

pub struct InspectedSignalInput {
    signals: Vec<RawSignalMsg>,
    source_coverage: Option<SourceCoverageCountsMsg>,
    filter: CanonicalDateFilter,
    summary: SignalFileSummary,
    limits: SignalInputLimits,
}

impl fmt::Debug for InspectedSignalInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InspectedSignalInput")
            .field("source_coverage", &self.source_coverage)
            .field("filter", &self.filter)
            .field("summary", &self.summary)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl InspectedSignalInput {
    pub fn signals(&self) -> &[RawSignalMsg] {
        &self.signals
    }

    pub fn source_coverage(&self) -> Option<SourceCoverageCountsMsg> {
        self.source_coverage
    }

    pub fn filter(&self) -> &CanonicalDateFilter {
        &self.filter
    }

    pub fn summary(&self) -> &SignalFileSummary {
        &self.summary
    }

    pub fn limits(&self) -> SignalInputLimits {
        self.limits
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<RawSignalMsg>,
        Option<SourceCoverageCountsMsg>,
        CanonicalDateFilter,
        SignalFileSummary,
        SignalInputLimits,
    ) {
        (
            self.signals,
            self.source_coverage,
            self.filter,
            self.summary,
            self.limits,
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BacktestInputInspector;

impl BacktestInputInspector {
    pub async fn inspect(
        &self,
        input: InspectSignalInput,
        cancellation: PreparationCancellation,
    ) -> Result<InspectedSignalInput, WorkflowError> {
        cancellation.check()?;
        tokio::task::spawn_blocking(move || inspect_blocking(input, cancellation))
            .await
            .map_err(|error| WorkflowError::PreparationTask {
                detail: error.to_string(),
            })?
    }
}

fn inspect_blocking(
    input: InspectSignalInput,
    cancellation: PreparationCancellation,
) -> Result<InspectedSignalInput, WorkflowError> {
    cancellation.check()?;
    let limits = input.limits.validate()?;
    let (filter, parsed_from, parsed_to) = canonical_filter(input.from, input.to)?;
    let (display_name, reader): (String, Box<dyn Read + Send>) = match input.signals {
        SignalInputSource::Path(path) => {
            cancellation.check()?;
            let display_name = path_display_name(&path);
            if let Ok(metadata) = std::fs::metadata(&path)
                && metadata.len() > limits.maximum_file_bytes
            {
                return Err(WorkflowError::InputByteLimit {
                    display_name,
                    resource: "file byte",
                    limit: limits.maximum_file_bytes,
                });
            }
            let file = File::open(&path).map_err(|error| WorkflowError::InputOpen {
                display_name: display_name.clone(),
                detail: error.to_string(),
            })?;
            (display_name, Box::new(file))
        }
        SignalInputSource::Reader {
            display_name,
            reader,
        } => {
            let display_name = sanitize_display_name(display_name)?;
            (display_name, reader)
        }
    };

    scan_reader(
        reader,
        display_name,
        input.source_coverage,
        input.decoding,
        limits,
        filter,
        parsed_from,
        parsed_to,
        cancellation,
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_reader(
    reader: Box<dyn Read + Send>,
    display_name: String,
    source_coverage: Option<SourceCoverageCountsMsg>,
    decoding: SignalDecodingPolicy,
    limits: SignalInputLimits,
    filter: CanonicalDateFilter,
    parsed_from: Option<NaiveDateTime>,
    parsed_to: Option<NaiveDateTime>,
    cancellation: PreparationCancellation,
) -> Result<InspectedSignalInput, WorkflowError> {
    let mut reader = BufReader::new(reader);

    let mut total_bytes = 0_u64;
    let mut line = Vec::new();
    let mut physical_lines = 0_u64;
    let mut non_empty_lines = 0_u64;
    let mut decoded_count = 0_u64;
    let mut retained = Vec::new();
    let mut entry_count = 0_u64;
    let mut symbols = BTreeSet::new();
    let mut minimum_timestamp: Option<String> = None;
    let mut maximum_timestamp: Option<String> = None;
    let mut action_counts = BTreeMap::new();

    loop {
        cancellation.check()?;
        let available = reader
            .fill_buf()
            .map_err(|error| WorkflowError::InputRead {
                display_name: display_name.clone(),
                line: physical_lines.saturating_add(1),
                detail: error.to_string(),
            })?;
        if available.is_empty() {
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let chunk = available[..consumed].to_vec();
        let next_total = total_bytes.checked_add(consumed as u64).ok_or_else(|| {
            WorkflowError::InputByteLimit {
                display_name: display_name.clone(),
                resource: "file byte",
                limit: limits.maximum_file_bytes,
            }
        })?;
        if next_total > limits.maximum_file_bytes {
            return Err(WorkflowError::InputByteLimit {
                display_name,
                resource: "file byte",
                limit: limits.maximum_file_bytes,
            });
        }

        total_bytes = next_total;

        let payload_len = chunk.len() - usize::from(newline.is_some());
        if line.len().saturating_add(payload_len) > limits.maximum_line_bytes {
            return Err(WorkflowError::InputLineLimit {
                display_name,
                line: physical_lines.saturating_add(1),
                limit: limits.maximum_line_bytes,
            });
        }
        line.extend_from_slice(&chunk[..payload_len]);
        reader.consume(consumed);

        if newline.is_some() {
            physical_lines = physical_lines.saturating_add(1);
            process_line(
                &line,
                physical_lines,
                &display_name,
                decoding,
                limits.maximum_signal_count,
                parsed_from,
                parsed_to,
                &mut non_empty_lines,
                &mut decoded_count,
                &mut retained,
                &mut entry_count,
                &mut symbols,
                &mut minimum_timestamp,
                &mut maximum_timestamp,
                &mut action_counts,
            )?;
            line.clear();
        }
    }

    if !line.is_empty() {
        cancellation.check()?;
        physical_lines = physical_lines.saturating_add(1);
        process_line(
            &line,
            physical_lines,
            &display_name,
            decoding,
            limits.maximum_signal_count,
            parsed_from,
            parsed_to,
            &mut non_empty_lines,
            &mut decoded_count,
            &mut retained,
            &mut entry_count,
            &mut symbols,
            &mut minimum_timestamp,
            &mut maximum_timestamp,
            &mut action_counts,
        )?;
    }

    cancellation.check()?;
    let retained_signal_count = retained.len() as u64;
    let mut warnings = Vec::new();
    if decoded_count == 0 {
        warnings.push(InputWarning::NoSignals);
    } else if retained_signal_count == 0 {
        warnings.push(InputWarning::NoSignalsAfterFilter);
    } else if entry_count == 0 {
        warnings.push(InputWarning::NoEntrySignals);
    }
    let summary = SignalFileSummary {
        display_name,
        byte_len: total_bytes,

        physical_lines,
        non_empty_lines,
        signal_count: decoded_count,
        retained_signal_count,
        entry_count,
        symbols,
        minimum_timestamp,
        maximum_timestamp,
        action_counts,
        warnings,
    };
    Ok(InspectedSignalInput {
        signals: retained,
        source_coverage,
        filter,
        summary,
        limits,
    })
}

#[allow(clippy::too_many_arguments)]
fn process_line(
    raw_line: &[u8],
    line_number: u64,
    display_name: &str,
    decoding: SignalDecodingPolicy,
    maximum_signal_count: usize,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
    non_empty_lines: &mut u64,
    decoded_count: &mut u64,
    retained: &mut Vec<RawSignalMsg>,
    entry_count: &mut u64,
    symbols: &mut BTreeSet<String>,
    minimum_timestamp: &mut Option<String>,
    maximum_timestamp: &mut Option<String>,
    action_counts: &mut BTreeMap<String, u64>,
) -> Result<(), WorkflowError> {
    let text = std::str::from_utf8(raw_line).map_err(|_| WorkflowError::InvalidUtf8 {
        display_name: display_name.to_owned(),
        line: line_number,
    })?;
    let text = text.trim();
    if text.is_empty() {
        return Ok(());
    }
    *non_empty_lines = non_empty_lines.saturating_add(1);
    if *decoded_count >= maximum_signal_count as u64 {
        return Err(WorkflowError::SignalCountLimit {
            display_name: display_name.to_owned(),
            limit: maximum_signal_count,
        });
    }
    let mut signal = match decoding {
        SignalDecodingPolicy::Strict => decode_raw_signal_json_strict(text),
        SignalDecodingPolicy::Compatibility => serde_json::from_str(text).map_err(Into::into),
    }
    .map_err(
        |error: qs_backtest_api::RawSignalDecodeError| WorkflowError::SignalDecode {
            display_name: display_name.to_owned(),
            line: line_number,
            detail: error.to_string(),
        },
    )?;
    if let RawSignalMsg::Entry { risk, .. } = &signal
        && (!risk.is_finite() || *risk <= 0.0)
    {
        return Err(WorkflowError::SignalDecode {
            display_name: display_name.to_owned(),
            line: line_number,
            detail: "Entry risk must be finite and positive".into(),
        });
    }
    *decoded_count = decoded_count.saturating_add(1);

    let parsed_timestamp =
        parse_backtest_timestamp(signal.ts()).map_err(|error| WorkflowError::SignalTimestamp {
            display_name: display_name.to_owned(),
            line: line_number,
            detail: error.to_string(),
        })?;
    let canonical_timestamp = canonical_backtest_timestamp(signal.ts()).map_err(|error| {
        WorkflowError::SignalTimestamp {
            display_name: display_name.to_owned(),
            line: line_number,
            detail: error.to_string(),
        }
    })?;
    set_signal_timestamp(&mut signal, canonical_timestamp.clone());

    if from.is_some_and(|bound| parsed_timestamp < bound)
        || to.is_some_and(|bound| parsed_timestamp > bound)
    {
        return Ok(());
    }

    if matches!(signal, RawSignalMsg::Entry { .. }) {
        *entry_count = entry_count.saturating_add(1);
    }
    collect_symbols(&signal, symbols);
    *action_counts
        .entry(action_name(&signal).into())
        .or_default() += 1;
    update_timestamp_range(minimum_timestamp, maximum_timestamp, &canonical_timestamp);
    retained.push(signal);
    Ok(())
}

fn canonical_filter(
    from: Option<String>,
    to: Option<String>,
) -> Result<
    (
        CanonicalDateFilter,
        Option<NaiveDateTime>,
        Option<NaiveDateTime>,
    ),
    WorkflowError,
> {
    let canonical_from = from
        .as_deref()
        .map(canonical_backtest_timestamp)
        .transpose()
        .map_err(|error| WorkflowError::InvalidConfiguration {
            field: "from",
            detail: error.to_string(),
        })?;
    let canonical_to = to
        .as_deref()
        .map(canonical_backtest_timestamp)
        .transpose()
        .map_err(|error| WorkflowError::InvalidConfiguration {
            field: "to",
            detail: error.to_string(),
        })?;
    let parsed_from = canonical_from
        .as_deref()
        .map(parse_backtest_timestamp)
        .transpose()
        .map_err(|error| WorkflowError::InvalidConfiguration {
            field: "from",
            detail: error.to_string(),
        })?;
    let parsed_to = canonical_to
        .as_deref()
        .map(parse_backtest_timestamp)
        .transpose()
        .map_err(|error| WorkflowError::InvalidConfiguration {
            field: "to",
            detail: error.to_string(),
        })?;
    if parsed_from
        .zip(parsed_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(WorkflowError::InvalidConfiguration {
            field: "date range",
            detail: "from must not be later than to after UTC normalization".into(),
        });
    }
    Ok((
        CanonicalDateFilter {
            from: canonical_from,
            to: canonical_to,
        },
        parsed_from,
        parsed_to,
    ))
}

fn sanitize_display_name(display_name: String) -> Result<String, WorkflowError> {
    let display_name = display_name.trim();
    if display_name.is_empty() || display_name.chars().any(char::is_control) {
        return Err(WorkflowError::InvalidConfiguration {
            field: "input display name",
            detail: "must be non-empty and contain no control characters".into(),
        });
    }
    let redacted = display_name
        .rsplit(['/', '\\'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("<input>");
    Ok(redacted.to_owned())
}

fn path_display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("<input>")
        .to_owned()
}

fn update_timestamp_range(
    minimum: &mut Option<String>,
    maximum: &mut Option<String>,
    timestamp: &str,
) {
    if minimum.as_deref().is_none_or(|value| timestamp < value) {
        *minimum = Some(timestamp.to_owned());
    }
    if maximum.as_deref().is_none_or(|value| timestamp > value) {
        *maximum = Some(timestamp.to_owned());
    }
}

fn collect_symbols(signal: &RawSignalMsg, symbols: &mut BTreeSet<String>) {
    match signal {
        RawSignalMsg::Entry { symbol, .. }
        | RawSignalMsg::CloseAllOf { symbol, .. }
        | RawSignalMsg::ModifyAllStoploss { symbol, .. } => {
            if !symbol.trim().is_empty() {
                symbols.insert(symbol.trim().to_owned());
            }
        }
        RawSignalMsg::Close { position, .. }
        | RawSignalMsg::ClosePartial { position, .. }
        | RawSignalMsg::ModifyStoploss { position, .. }
        | RawSignalMsg::MoveStoplossToEntry { position, .. }
        | RawSignalMsg::AddTarget { position, .. }
        | RawSignalMsg::RemoveTarget { position, .. }
        | RawSignalMsg::ModifyTarget { position, .. }
        | RawSignalMsg::AddRule { position, .. }
        | RawSignalMsg::RemoveRule { position, .. }
        | RawSignalMsg::ScaleIn { position, .. }
        | RawSignalMsg::CancelPending { position, .. } => {
            if let PositionRefMsg::AllOnSymbol { symbol } = position
                && !symbol.trim().is_empty()
            {
                symbols.insert(symbol.trim().to_owned());
            }
        }
        RawSignalMsg::CloseAll { .. }
        | RawSignalMsg::CancelAllPending { .. }
        | RawSignalMsg::CloseAllInGroup { .. }
        | RawSignalMsg::ModifyAllStoplossInGroup { .. } => {}
    }
}

fn action_name(signal: &RawSignalMsg) -> &'static str {
    match signal {
        RawSignalMsg::Entry { .. } => "Entry",
        RawSignalMsg::Close { .. } => "Close",
        RawSignalMsg::ClosePartial { .. } => "ClosePartial",
        RawSignalMsg::ModifyStoploss { .. } => "ModifyStoploss",
        RawSignalMsg::MoveStoplossToEntry { .. } => "MoveStoplossToEntry",
        RawSignalMsg::AddTarget { .. } => "AddTarget",
        RawSignalMsg::RemoveTarget { .. } => "RemoveTarget",
        RawSignalMsg::ModifyTarget { .. } => "ModifyTarget",
        RawSignalMsg::AddRule { .. } => "AddRule",
        RawSignalMsg::RemoveRule { .. } => "RemoveRule",
        RawSignalMsg::ScaleIn { .. } => "ScaleIn",
        RawSignalMsg::CancelPending { .. } => "CancelPending",
        RawSignalMsg::CloseAllOf { .. } => "CloseAllOf",
        RawSignalMsg::CloseAll { .. } => "CloseAll",
        RawSignalMsg::CancelAllPending { .. } => "CancelAllPending",
        RawSignalMsg::ModifyAllStoploss { .. } => "ModifyAllStoploss",
        RawSignalMsg::CloseAllInGroup { .. } => "CloseAllInGroup",
        RawSignalMsg::ModifyAllStoplossInGroup { .. } => "ModifyAllStoplossInGroup",
    }
}

fn set_signal_timestamp(signal: &mut RawSignalMsg, timestamp: String) {
    match signal {
        RawSignalMsg::Entry { ts, .. }
        | RawSignalMsg::Close { ts, .. }
        | RawSignalMsg::ClosePartial { ts, .. }
        | RawSignalMsg::ModifyStoploss { ts, .. }
        | RawSignalMsg::MoveStoplossToEntry { ts, .. }
        | RawSignalMsg::AddTarget { ts, .. }
        | RawSignalMsg::RemoveTarget { ts, .. }
        | RawSignalMsg::ModifyTarget { ts, .. }
        | RawSignalMsg::AddRule { ts, .. }
        | RawSignalMsg::RemoveRule { ts, .. }
        | RawSignalMsg::ScaleIn { ts, .. }
        | RawSignalMsg::CancelPending { ts, .. }
        | RawSignalMsg::CloseAllOf { ts, .. }
        | RawSignalMsg::CloseAll { ts }
        | RawSignalMsg::CancelAllPending { ts }
        | RawSignalMsg::ModifyAllStoploss { ts, .. }
        | RawSignalMsg::CloseAllInGroup { ts, .. }
        | RawSignalMsg::ModifyAllStoplossInGroup { ts, .. } => *ts = timestamp,
    }
}
