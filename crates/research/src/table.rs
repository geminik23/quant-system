use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::error::RunFailure;

/// Whether one run produced a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Completed,
    Failed { kind: String, message: String },
}

impl RunStatus {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Completed => "completed",
            Self::Failed { .. } => "failed",
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

impl From<RunFailure> for RunStatus {
    fn from(failure: RunFailure) -> Self {
        Self::Failed {
            kind: failure.kind().to_owned(),
            message: failure.message().to_owned(),
        }
    }
}

/// One evaluated configuration over one window.
///
/// Every figure here is net of trading costs. The profit-and-loss and drawdown columns come from the run result, whose trade log settles each position's commission and swap; the R and excursion columns come from provider evaluation, which reads completed positions directly. Both sit on the same basis, so a row never mixes a gross figure with a net one.
///
/// There is deliberately no score, rank, or overall rating column. The table reports what each configuration did and leaves the judgement to the reader.
#[derive(Debug, Clone, PartialEq)]
pub struct ResearchRow {
    pub family_id: String,
    pub symbol: String,
    /// The parameter labels this run was tagged with.
    pub params: BTreeMap<String, String>,
    pub window: String,
    pub status: RunStatus,
    /// How the run read its market data, recorded because it changes what the numbers mean.
    pub data_mode: String,

    pub positions: usize,
    pub win_rate: Option<f64>,
    pub net_pnl: f64,
    pub gross_pnl: Option<f64>,
    pub commission: f64,
    pub swap: f64,

    pub expectancy_r: Option<f64>,
    pub r_p05: Option<f64>,
    pub r_p50: Option<f64>,
    pub r_p95: Option<f64>,
    pub avg_favorable_r: Option<f64>,
    pub avg_adverse_r: Option<f64>,

    pub max_drawdown_pct: f64,
    /// Positions still open when the window ended and closed by the run rather than by the strategy.
    pub forced_closes: usize,
    /// Positions opened before the window began, which warmup should make impossible and which is reported so that it cannot pass unnoticed.
    pub entries_before_window: usize,
    /// How many configurations the search visited in total, so a reader can weigh the multiple-comparison risk of the row they are looking at.
    pub points_total: usize,
}

impl ResearchRow {
    /// Key used for deterministic ordering, independent of the order runs finished in.
    fn sort_key(&self) -> (&str, &str, Vec<(&str, &str)>, &str) {
        (
            self.family_id.as_str(),
            self.symbol.as_str(),
            self.params
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect(),
            self.window.as_str(),
        )
    }
}

/// Every row of one batch, in a deterministic order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResearchTable {
    rows: Vec<ResearchRow>,
}

impl ResearchTable {
    /// Build a table, ordering rows by family, symbol, parameters, and window.
    ///
    /// Sorting is by key and never by a metric, so the top of the table is not a recommendation.
    pub fn new(mut rows: Vec<ResearchRow>) -> Self {
        rows.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        Self { rows }
    }

    pub fn rows(&self) -> &[ResearchRow] {
        &self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Every parameter key any row carries, in a stable order.
    fn parameter_columns(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = self
            .rows
            .iter()
            .flat_map(|row| row.params.keys().map(String::as_str))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Pair each in-sample row with the out-of-sample row for the same configuration.
    ///
    /// A configuration whose windows did not both produce a row is left out, because a half-pair invites reading the in-sample number on its own.
    pub fn paired(&self, in_sample: &str, out_of_sample: &str) -> Vec<PairedRow<'_>> {
        let mut pairs = Vec::new();
        for row in self.rows.iter().filter(|row| row.window == in_sample) {
            let partner = self.rows.iter().find(|candidate| {
                candidate.window == out_of_sample
                    && candidate.family_id == row.family_id
                    && candidate.symbol == row.symbol
                    && candidate.params == row.params
            });
            if let Some(out) = partner {
                pairs.push(PairedRow {
                    in_sample: row,
                    out_of_sample: out,
                });
            }
        }
        pairs
    }

    /// Render the table as CSV with one column per parameter key.
    pub fn to_csv(&self) -> String {
        let parameters = self.parameter_columns();
        let mut out = String::new();

        let mut header: Vec<String> = vec!["family_id".into(), "symbol".into()];
        header.extend(parameters.iter().map(|key| format!("param_{key}")));
        header.extend(
            [
                "window",
                "status",
                "failure",
                "data_mode",
                "positions",
                "win_rate",
                "net_pnl",
                "gross_pnl",
                "commission",
                "swap",
                "expectancy_r",
                "r_p05",
                "r_p50",
                "r_p95",
                "avg_favorable_r",
                "avg_adverse_r",
                "max_drawdown_pct",
                "forced_closes",
                "entries_before_window",
                "points_total",
            ]
            .iter()
            .map(|name| (*name).to_owned()),
        );
        writeln!(out, "{}", header.join(",")).expect("writing to a string cannot fail");

        for row in &self.rows {
            let mut fields: Vec<String> = vec![escape(&row.family_id), escape(&row.symbol)];
            for key in &parameters {
                fields.push(escape(row.params.get(*key).map_or("", String::as_str)));
            }
            fields.push(escape(&row.window));
            fields.push(escape(row.status.as_str()));
            fields.push(match &row.status {
                RunStatus::Completed => String::new(),
                RunStatus::Failed { kind, message } => escape(&format!("{kind}: {message}")),
            });
            fields.push(escape(&row.data_mode));
            fields.push(row.positions.to_string());
            fields.push(number(row.win_rate));
            fields.push(format_f64(row.net_pnl));
            fields.push(number(row.gross_pnl));
            fields.push(format_f64(row.commission));
            fields.push(format_f64(row.swap));
            fields.push(number(row.expectancy_r));
            fields.push(number(row.r_p05));
            fields.push(number(row.r_p50));
            fields.push(number(row.r_p95));
            fields.push(number(row.avg_favorable_r));
            fields.push(number(row.avg_adverse_r));
            fields.push(format_f64(row.max_drawdown_pct));
            fields.push(row.forced_closes.to_string());
            fields.push(row.entries_before_window.to_string());
            fields.push(row.points_total.to_string());
            writeln!(out, "{}", fields.join(",")).expect("writing to a string cannot fail");
        }
        out
    }
}

/// One configuration's in-sample and out-of-sample rows, side by side.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PairedRow<'a> {
    pub in_sample: &'a ResearchRow,
    pub out_of_sample: &'a ResearchRow,
}

fn number(value: Option<f64>) -> String {
    value.map(format_f64).unwrap_or_default()
}

fn format_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value}")
    } else {
        // A non-finite metric is left blank rather than written as `inf`, which a spreadsheet would read as text.
        String::new()
    }
}

fn escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}
