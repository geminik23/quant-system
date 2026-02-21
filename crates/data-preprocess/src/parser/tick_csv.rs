use std::path::Path;

use chrono::FixedOffset;

use crate::error::Result;
use crate::models::Tick;
use crate::parser::parse_datetime_to_utc;

/// Parse optional float field; empty or whitespace-only → None.
fn parse_optional_f64(field: Option<&str>) -> Option<f64> {
    field
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<f64>().ok())
}

/// Parse optional integer field; empty or whitespace-only → None.
fn parse_optional_i32(field: Option<&str>) -> Option<i32> {
    field
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i32>().ok())
}

/// Parse a tab-delimited tick CSV into `Vec<Tick>`.
///
/// Malformed rows produce warnings instead of hard errors so partial
/// files can still be imported.
pub fn parse_tick_csv(
    path: &Path,
    exchange: &str,
    symbol: &str,
    source_offset: &FixedOffset,
) -> Result<(Vec<Tick>, Vec<String>)> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let mut ticks = Vec::new();
    let mut warnings = Vec::new();

    for (line_idx, record) in reader.records().enumerate() {
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                warnings.push(format!("line {}: {}", line_idx + 2, e));
                continue;
            }
        };

        let date_str = record.get(0).unwrap_or("").trim();
        let time_str = record.get(1).unwrap_or("").trim();
        let bid = parse_optional_f64(record.get(2));
        let ask = parse_optional_f64(record.get(3));
        let last = parse_optional_f64(record.get(4));
        let vol = parse_optional_f64(record.get(5));
        let flags = parse_optional_i32(record.get(6));

        let ts = match parse_datetime_to_utc(date_str, time_str, source_offset) {
            Ok(t) => t,
            Err(e) => {
                warnings.push(format!("line {}: {}", line_idx + 2, e));
                continue;
            }
        };

        ticks.push(Tick {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            ts,
            bid,
            ask,
            last,
            volume: vol,
            flags,
        });
    }

    Ok((ticks, warnings))
}
