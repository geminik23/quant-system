//! Static presentation fixtures for the desktop shell preview.
//!
//! Every fixture and preview type lives in this module so the static shell
//! never duplicates production service DTOs. Nothing here performs IO, RPC,
//! parsing, or persistence; all values are presentation-only.

use std::sync::Arc;

/// Fixture scenario that drives the whole static shell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixtureScenario {
    PersistedResult,
    SummaryOnly,
    Disconnected,
    Warning,
    Unavailable,
}

/// A key-value presentation row.
pub struct Row {
    pub label: String,
    pub value: String,
}

impl Row {
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
        }
    }
}

/// A titled group of presentation rows.
pub struct Card {
    pub title: String,
    pub rows: Vec<Row>,
}

impl Card {
    pub fn new(title: impl Into<String>, rows: Vec<Row>) -> Self {
        Self {
            title: title.into(),
            rows,
        }
    }
}

/// Stage state for the run route fixture.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StageState {
    Done,
    Current,
    Pending,
}

/// One precomputed point for a chart presentation series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SeriesPoint {
    pub x: f32,
    pub value: f32,
}

/// Presentation state for one monthly return cell.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MonthlyReturnStatus {
    Observed(f32),
    Inactive,
    Missing,
}

/// One month in the static return grid.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MonthlyReturnCell {
    pub label: &'static str,
    pub status: MonthlyReturnStatus,
}

/// One calendar year of monthly return presentation state.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MonthlyReturnYear {
    pub year: i32,
    pub months: [MonthlyReturnCell; 12],
}

impl FixtureScenario {
    pub const ALL: [FixtureScenario; 5] = [
        FixtureScenario::PersistedResult,
        FixtureScenario::SummaryOnly,
        FixtureScenario::Disconnected,
        FixtureScenario::Warning,
        FixtureScenario::Unavailable,
    ];

    pub fn label(self) -> &'static str {
        match self {
            FixtureScenario::PersistedResult => "Persisted result",
            FixtureScenario::SummaryOnly => "Summary-only",
            FixtureScenario::Disconnected => "Disconnected",
            FixtureScenario::Warning => "Input warning",
            FixtureScenario::Unavailable => "Analysis unavailable",
        }
    }
}

/// Connection display for the header and status bar.
pub struct ConnectionDisplay {
    pub state_label: String,
    pub connected: bool,
}

pub fn connection_display(scenario: FixtureScenario) -> ConnectionDisplay {
    match scenario {
        FixtureScenario::Disconnected => ConnectionDisplay {
            state_label: "Disconnected".into(),
            connected: false,
        },
        _ => ConnectionDisplay {
            state_label: "Connected".into(),
            connected: true,
        },
    }
}

pub const DEFAULT_INPUT_NAME: &str = "eurusd_ema_atr_lifecycle.jsonl";

pub fn input_summary(input_name: &str) -> Card {
    Card::new(
        "Parsed signal input (fixture)",
        vec![
            Row::new("Display name", input_name),
            Row::new("Byte length", "482113"),
            Row::new("SHA-256", "9f2c41ab..."),
            Row::new("Physical lines", "4102"),
            Row::new("Non-empty lines", "4096"),
            Row::new("Decoded signals", "4096"),
            Row::new("Retained signals", "4094"),
            Row::new("Entry count", "1288"),
            Row::new("Symbol set", "EURUSD"),
            Row::new("Minimum timestamp", "2024-01-02 00:00:00"),
            Row::new("Maximum timestamp", "2024-12-30 23:59:59"),
            Row::new("Action count", "4096"),
            Row::new("Bounded limit state", "Within configured bounds"),
        ],
    )
}

pub fn input_policy() -> Card {
    Card::new(
        "Decoding policy (fixture)",
        vec![
            Row::new("Mode", "Strict standalone RawSignalMsg"),
            Row::new("Compatibility fallback", "Not used"),
            Row::new("Line diagnostics", "Physical line numbers"),
            Row::new("Raw signal bodies", "Not stored, logged, or displayed"),
        ],
    )
}

pub fn input_coverage(scenario: FixtureScenario) -> Card {
    match scenario {
        FixtureScenario::Warning => Card::new(
            "Source coverage (fixture)",
            vec![
                Row::new("Status", "Warning"),
                Row::new("Skipped lines", "3 physical lines skipped"),
                Row::new("Reason", "Unknown position field on line 1712"),
                Row::new("Outcome adapter", "Not connected"),
            ],
        ),
        FixtureScenario::Disconnected => Card::new(
            "Source coverage (fixture)",
            vec![
                Row::new("Status", "Unavailable"),
                Row::new("Reason", "Not connected to the backtest service"),
            ],
        ),
        _ => Card::new(
            "Source coverage (fixture)",
            vec![
                Row::new("Status", "Not provided"),
                Row::new("Outcome adapter", "Direct raw-signal replay"),
            ],
        ),
    }
}

pub fn input_date_filter() -> Card {
    Card::new(
        "Inclusive date filter (fixture)",
        vec![
            Row::new("From", "2024-01-02 00:00:00"),
            Row::new("To", "2024-12-30 23:59:59"),
            Row::new("Normalization", "Canonical UTC naive"),
            Row::new("Reversed ranges", "Rejected before input loading"),
        ],
    )
}

const CONFIGURE_SECTIONS: [&str; 8] = [
    "Connection",
    "Market Data",
    "Signal Scope",
    "Profile",
    "Account & Sizing",
    "Execution",
    "Evaluation",
    "Result Delivery",
];

pub fn configure_section(section_index: usize, scenario: FixtureScenario) -> Card {
    let connection = CONFIGURE_SECTIONS
        .get(section_index)
        .copied()
        .unwrap_or("Connection");
    match (connection, scenario) {
        ("Connection", FixtureScenario::Disconnected) => Card::new(
            "Connection",
            vec![
                Row::new("Endpoint", "tcp://127.0.0.1:41001"),
                Row::new("Server state", "Disconnected"),
                Row::new("Catalog generation", "Unavailable"),
                Row::new("Retry", "Status-first reconnect keeps the same job ID"),
            ],
        ),
        ("Connection", _) => Card::new(
            "Connection",
            vec![
                Row::new("Endpoint", "tcp://127.0.0.1:41001"),
                Row::new("Server state", "Connected (fixture)"),
                Row::new("Catalog generation", "17"),
                Row::new("Catalog loaded", "2026-08-29 12:00:00Z"),
            ],
        ),
        ("Market Data", _) => Card::new(
            "Market Data",
            vec![
                Row::new("Exchange", "Fixture exchange"),
                Row::new("Data type", "Tick"),
                Row::new("Timeframe", "M1"),
                Row::new("Date range", "2024-01-02 to 2024-12-30"),
                Row::new("Availability range", "2023-11-15 to 2025-01-10"),
                Row::new("Available symbols", "EURUSD, XAUUSD"),
            ],
        ),
        ("Signal Scope", _) => Card::new(
            "Signal Scope",
            vec![
                Row::new("Entry symbols", "All entry symbols"),
                Row::new("Explicit selection", "None"),
                Row::new("Input symbols", "EURUSD"),
                Row::new("Availability match", "Matched"),
            ],
        ),
        ("Profile", _) => Card::new(
            "Profile",
            vec![
                Row::new("Selection", "No management profile"),
                Row::new("Summary", "Raw signals replay without management profiles"),
            ],
        ),
        ("Account & Sizing", _) => Card::new(
            "Account & Sizing",
            vec![
                Row::new("Initial balance", "10000.00"),
                Row::new("Account currency", "USD"),
                Row::new("Sizing basis", "Risk fraction"),
                Row::new("Sizing value", "0.01"),
            ],
        ),
        ("Execution", _) => Card::new(
            "Execution",
            vec![
                Row::new("Close on finish", "Enabled"),
                Row::new("Fill model", "FutureQuoteV1 (close-only, zero spread)"),
                Row::new("Latency", "Next-quote"),
                Row::new("Slippage", "None"),
                Row::new("Conversion staleness", "Causal same-day conversion"),
                Row::new("MTM retention", "End of day"),
            ],
        ),
        ("Evaluation", _) => Card::new(
            "Evaluation",
            vec![
                Row::new("Requested sections", "Positions, economics, drawdown"),
                Row::new("Complete position rows", "Required"),
                Row::new("Local analysis availability", "Expected after delivery"),
            ],
        ),
        ("Result Delivery", FixtureScenario::SummaryOnly) => Card::new(
            "Result Delivery",
            vec![
                Row::new("Service delivery", "Inline compact summary"),
                Row::new("Local output intent", "SummaryOnly"),
                Row::new("Offline reopen", "Not produced by a summary-only run"),
                Row::new("Local analysis", "Not produced by a summary-only run"),
                Row::new("Server artifact retention", "Limited; not local evidence"),
            ],
        ),
        ("Result Delivery", _) => Card::new(
            "Result Delivery",
            vec![
                Row::new("Service delivery", "Artifact"),
                Row::new("Local output intent", "Persist"),
                Row::new("Document format", "Result Document V1"),
                Row::new("Output commit", "No-clobber"),
                Row::new("Result byte limit", "128 MiB guidance"),
            ],
        ),
        _ => Card::new("Connection", vec![Row::new("Section", "Unknown")]),
    }
}

pub fn run_stages(scenario: FixtureScenario) -> Vec<(String, StageState)> {
    let stage = |name: &str, state: StageState| (name.to_string(), state);
    match scenario {
        FixtureScenario::PersistedResult => vec![
            stage("Connecting", StageState::Done),
            stage("Submitting", StageState::Done),
            stage("Queued", StageState::Done),
            stage("Loading primary data", StageState::Done),
            stage("Loading conversion data", StageState::Done),
            stage("Replaying", StageState::Done),
            stage("Fetching result", StageState::Done),
            stage("Downloading artifact", StageState::Done),
            stage("Verifying result", StageState::Done),
            stage("Encoding document", StageState::Done),
            stage("Verifying document", StageState::Done),
            stage("Committing output", StageState::Done),
            stage("Releasing artifact", StageState::Done),
            stage("Completed persisted", StageState::Current),
        ],
        FixtureScenario::SummaryOnly => vec![
            stage("Connecting", StageState::Done),
            stage("Submitting", StageState::Done),
            stage("Queued", StageState::Done),
            stage("Loading primary data", StageState::Done),
            stage("Loading conversion data", StageState::Done),
            stage("Replaying", StageState::Done),
            stage("Fetching result", StageState::Done),
            stage("Validating inline summary", StageState::Done),
            stage("Completed summary only", StageState::Current),
        ],
        FixtureScenario::Disconnected => vec![
            stage("Connecting", StageState::Done),
            stage("Submitting", StageState::Done),
            stage("Queued", StageState::Done),
            stage("Loading primary data", StageState::Done),
            stage("Loading conversion data", StageState::Done),
            stage("Replaying", StageState::Current),
            stage("Fetching result", StageState::Pending),
        ],
        _ => vec![
            stage("Connecting", StageState::Done),
            stage("Submitting", StageState::Done),
            stage("Queued", StageState::Done),
            stage("Loading primary data", StageState::Done),
            stage("Loading conversion data", StageState::Done),
            stage("Replaying", StageState::Current),
            stage("Fetching result", StageState::Pending),
        ],
    }
}

pub fn run_progress(scenario: FixtureScenario) -> Vec<Row> {
    match scenario {
        FixtureScenario::Disconnected => vec![
            Row::new("Stage", "Reconnecting"),
            Row::new("Same job ID", "Retained"),
            Row::new("Next retry", "Within 2 seconds (fixture)"),
            Row::new("Progress", "Processed 1204551 events; total unknown"),
        ],
        _ => vec![
            Row::new("Stage", "Replaying"),
            Row::new("Progress", "Processed 1204551 events; total unknown"),
            Row::new("Heartbeat", "Liveness only; does not change percent"),
        ],
    }
}

pub fn run_inspector(saved_path: Option<&str>) -> Vec<Row> {
    let mut rows = vec![
        Row::new("Local run ID", "42"),
        Row::new("Job ID", "b7e1c2d9"),
        Row::new("Endpoint", "tcp://127.0.0.1:41001"),
        Row::new("Submit timestamp", "2026-08-29 12:00:01Z"),
        Row::new("Last snapshot sequence", "117"),
        Row::new("Dropped events", "0"),
        Row::new("Reconnect attempts", "0"),
        Row::new("Output intent", "Persist"),
        Row::new("Output target", "results/eurusd_lifecycle.json"),
    ];
    if let Some(path) = saved_path {
        rows.push(Row::new("Save-as target", path));
    }
    rows
}

pub fn evidence_bar() -> Vec<Row> {
    vec![
        Row::new("Subject", "Parsed signal replay"),
        Row::new("Mode", "Exploratory - Not validated"),
        Row::new("Evidence", "SingleRunEvidence"),
        Row::new("Scope", "EURUSD - All sides - All groups"),
        Row::new(
            "Population",
            "CompletedPosition: 84 observed / 112 provided",
        ),
        Row::new(
            "Completeness",
            "Positions complete - Execution partial - Source unavailable",
        ),
        Row::new("Policy", "Not selected"),
        Row::new("Document", "9f2c41ab - Result Document V1"),
    ]
}

pub fn equity_series() -> Arc<[SeriesPoint]> {
    Arc::from([
        SeriesPoint {
            x: 0.0,
            value: 10_000.0,
        },
        SeriesPoint {
            x: 1.0,
            value: 10_120.0,
        },
        SeriesPoint {
            x: 2.0,
            value: 9_940.0,
        },
        SeriesPoint {
            x: 3.0,
            value: 10_280.0,
        },
        SeriesPoint {
            x: 4.0,
            value: 10_460.0,
        },
        SeriesPoint {
            x: 5.0,
            value: 10_310.0,
        },
        SeriesPoint {
            x: 6.0,
            value: 10_720.0,
        },
        SeriesPoint {
            x: 7.0,
            value: 10_610.0,
        },
        SeriesPoint {
            x: 8.0,
            value: 10_940.0,
        },
        SeriesPoint {
            x: 9.0,
            value: 10_820.0,
        },
        SeriesPoint {
            x: 10.0,
            value: 11_070.0,
        },
        SeriesPoint {
            x: 11.0,
            value: 10_980.0,
        },
        SeriesPoint {
            x: 12.0,
            value: 11_220.0,
        },
        SeriesPoint {
            x: 13.0,
            value: 11_060.0,
        },
        SeriesPoint {
            x: 14.0,
            value: 11_310.0,
        },
        SeriesPoint {
            x: 15.0,
            value: 11_180.0,
        },
        SeriesPoint {
            x: 16.0,
            value: 11_460.0,
        },
        SeriesPoint {
            x: 17.0,
            value: 11_240.0,
        },
        SeriesPoint {
            x: 18.0,
            value: 11_580.0,
        },
        SeriesPoint {
            x: 19.0,
            value: 11_410.0,
        },
        SeriesPoint {
            x: 20.0,
            value: 11_690.0,
        },
        SeriesPoint {
            x: 21.0,
            value: 11_520.0,
        },
        SeriesPoint {
            x: 22.0,
            value: 11_760.0,
        },
        SeriesPoint {
            x: 23.0,
            value: 11_420.0,
        },
    ])
}

pub fn drawdown_series() -> Arc<[SeriesPoint]> {
    Arc::from([
        SeriesPoint { x: 0.0, value: 0.0 },
        SeriesPoint { x: 1.0, value: 0.0 },
        SeriesPoint {
            x: 2.0,
            value: -1.8,
        },
        SeriesPoint { x: 3.0, value: 0.0 },
        SeriesPoint { x: 4.0, value: 0.0 },
        SeriesPoint {
            x: 5.0,
            value: -1.4,
        },
        SeriesPoint { x: 6.0, value: 0.0 },
        SeriesPoint {
            x: 7.0,
            value: -1.0,
        },
        SeriesPoint { x: 8.0, value: 0.0 },
        SeriesPoint {
            x: 9.0,
            value: -1.1,
        },
        SeriesPoint {
            x: 10.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 11.0,
            value: -0.8,
        },
        SeriesPoint {
            x: 12.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 13.0,
            value: -1.4,
        },
        SeriesPoint {
            x: 14.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 15.0,
            value: -1.1,
        },
        SeriesPoint {
            x: 16.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 17.0,
            value: -1.9,
        },
        SeriesPoint {
            x: 18.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 19.0,
            value: -1.5,
        },
        SeriesPoint {
            x: 20.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 21.0,
            value: -1.5,
        },
        SeriesPoint {
            x: 22.0,
            value: 0.0,
        },
        SeriesPoint {
            x: 23.0,
            value: -2.9,
        },
    ])
}

pub fn monthly_returns() -> Vec<MonthlyReturnYear> {
    use MonthlyReturnStatus::{Inactive, Missing, Observed};
    vec![MonthlyReturnYear {
        year: 2024,
        months: [
            MonthlyReturnCell {
                label: "Jan",
                status: Observed(2.1),
            },
            MonthlyReturnCell {
                label: "Feb",
                status: Observed(-1.4),
            },
            MonthlyReturnCell {
                label: "Mar",
                status: Observed(3.2),
            },
            MonthlyReturnCell {
                label: "Apr",
                status: Observed(0.6),
            },
            MonthlyReturnCell {
                label: "May",
                status: Observed(-2.0),
            },
            MonthlyReturnCell {
                label: "Jun",
                status: Observed(1.8),
            },
            MonthlyReturnCell {
                label: "Jul",
                status: Observed(2.4),
            },
            MonthlyReturnCell {
                label: "Aug",
                status: Inactive,
            },
            MonthlyReturnCell {
                label: "Sep",
                status: Observed(1.1),
            },
            MonthlyReturnCell {
                label: "Oct",
                status: Observed(-0.8),
            },
            MonthlyReturnCell {
                label: "Nov",
                status: Observed(2.0),
            },
            MonthlyReturnCell {
                label: "Dec",
                status: Missing,
            },
        ],
    }]
}

pub fn result_summary() -> Card {
    Card::new(
        "Backtest complete",
        vec![
            Row::new("Final balance", "USD 11,420"),
            Row::new("Realized profit", "+USD 1,420"),
            Row::new("Completed trades", "84"),
            Row::new("Win rate", "53.6%"),
            Row::new("Maximum drawdown", "-8.2%"),
        ],
    )
}

pub fn result_highlights() -> Card {
    Card::new(
        "What stands out",
        vec![
            Row::new("Returns", "Positive, but confidence is still limited"),
            Row::new("Downside risk", "Mixed"),
            Row::new("Stability over time", "More evidence needed"),
            Row::new(
                "Analysis coverage",
                "84 observed of 112 provided · execution details partial",
            ),
            Row::new("Out-of-sample evidence", "Not available"),
        ],
    )
}

/// Metric evidence cell fixture following the common evidence cell shape.
pub struct MetricCell {
    pub metric: String,
    pub observed: String,
    pub interval: String,
    pub population: String,
    pub provided_eligible: String,
    pub observed_count: String,
    pub excluded_invalid: String,
    pub completeness: String,
}

pub fn metric_cells(section_index: usize) -> Vec<MetricCell> {
    let cell = MetricCell {
        metric: "Mean R".into(),
        observed: "+0.18 R".into(),
        interval: "[-0.04, +0.39]".into(),
        population: "CompletedPosition".into(),
        provided_eligible: "112 / 96".into(),
        observed_count: "84".into(),
        excluded_invalid: "12 / 0".into(),
        completeness: "R observation partial".into(),
    };
    let missing = MetricCell {
        metric: "Tail ratio".into(),
        observed: "Not estimated".into(),
        interval: "Not estimated".into(),
        population: "CompletedPosition".into(),
        provided_eligible: "112 / 96".into(),
        observed_count: "84".into(),
        excluded_invalid: "12 / 0".into(),
        completeness: "Confidence interval not estimated".into(),
    };
    match section_index {
        1 | 3 => vec![cell],
        _ => vec![cell, missing],
    }
}

pub const POSITIONS_HEADERS: [&str; 7] = [
    "ID",
    "Symbol",
    "Side",
    "Opened",
    "Closed",
    "Quantity",
    "Realized R",
];

pub fn positions_rows() -> Vec<[String; 7]> {
    (0..6)
        .map(|i| {
            [
                format!("run-42-{i:04}"),
                "EURUSD".into(),
                if i % 2 == 0 { "Long" } else { "Short" }.into(),
                "2024-03-11 08:15:00".into(),
                "2024-03-11 16:42:00".into(),
                "0.35".into(),
                format!("{:+.2}", 0.9 - (i as f32) * 0.31),
            ]
        })
        .collect()
}

pub fn metadata_card(document_kind: &str) -> Card {
    Card::new(
        "Document metadata (fixture)",
        vec![
            Row::new("Document kind", document_kind),
            Row::new("Document version", "V1"),
            Row::new("Digest prefix", "9f2c41ab"),
            Row::new("Saved at", "2026-08-29 12:05:44Z"),
            Row::new("Source input", DEFAULT_INPUT_NAME),
            Row::new("Management profile", "None"),
            Row::new("Official verdict", "Not produced by a single run"),
        ],
    )
}

pub const EXPERIMENT_NAV: [&str; 5] = ["Identity", "Protocol", "Child runs", "Evidence", "Verdict"];

pub fn experiment_section(section_index: usize) -> Card {
    match EXPERIMENT_NAV
        .get(section_index)
        .copied()
        .unwrap_or("Identity")
    {
        "Identity" => Card::new(
            "Experiment identity (fixture)",
            vec![
                Row::new("Experiment ID", "exp-2026-08-29-a"),
                Row::new("Strategy identity", "Not registered in preview"),
                Row::new("Policy", "Research Policy v1 (fixture)"),
            ],
        ),
        "Protocol" => Card::new(
            "Protocol (fixture)",
            vec![
                Row::new("Type", "Holdout out-of-sample"),
                Row::new("Policy freeze", "Frozen at creation"),
                Row::new("Child execution", "Sequential"),
            ],
        ),
        "Child runs" => Card::new(
            "Child runs (fixture)",
            vec![
                Row::new("Children", "Not executed in preview"),
                Row::new("Resubmission on resume", "Not permitted"),
            ],
        ),
        "Evidence" => Card::new(
            "Evidence (fixture)",
            vec![Row::new(
                "Criterion evidence",
                "Arrives with the validation workspace phase",
            )],
        ),
        _ => Card::new(
            "Verdict (fixture)",
            vec![
                Row::new("Status", "Not available"),
                Row::new(
                    "Reason",
                    "An official verdict requires a frozen experiment and independent child replays",
                ),
            ],
        ),
    }
}

pub fn summary_only_card() -> Card {
    Card::new(
        "Backtest summary",
        vec![
            Row::new("Result file", "Not saved"),
            Row::new("Open again later", "Not available until you save a result"),
            Row::new(
                "Detailed local analysis",
                "Not included in this compact summary",
            ),
            Row::new("Next step", "Save the summary under a new name"),
        ],
    )
}

pub fn unavailable_card() -> Card {
    Card::new(
        "Detailed analysis is not available",
        vec![
            Row::new(
                "Why",
                "This result file does not include the detailed trade dataset",
            ),
            Row::new("What you can still view", "Run summary and file details"),
            Row::new("Missing values", "Shown as unavailable, never as zero"),
        ],
    )
}

/// Rows for one metric evidence cell, following the common evidence shape.
pub fn evidence_cell_rows(cell: &MetricCell) -> Vec<Row> {
    vec![
        Row::new("Metric", cell.metric.clone()),
        Row::new("Observed value", cell.observed.clone()),
        Row::new("Confidence interval", cell.interval.clone()),
        Row::new("Population unit", cell.population.clone()),
        Row::new("Provided / eligible", cell.provided_eligible.clone()),
        Row::new("Observed", cell.observed_count.clone()),
        Row::new("Excluded / invalid", cell.excluded_invalid.clone()),
        Row::new("Scope", "Unfiltered baseline"),
        Row::new("Evidence class", "SingleRunEvidence"),
        Row::new("Completeness", cell.completeness.clone()),
    ]
}
