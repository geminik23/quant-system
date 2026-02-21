use std::path::Path;

use chrono::FixedOffset;

use crate::error::Result;
use crate::models::{Bar, Timeframe};

use super::{parse_datetime_to_utc, parse_required_f64, parse_required_i32, parse_required_i64};

/// Parse a bar CSV file into a Vec<Bar>.
/// Tab-delimited with header row. Timestamps converted from source_offset to UTC.
pub fn parse_bar_csv(
    path: &Path,
    exchange: &str,
    symbol: &str,
    timeframe: Timeframe,
    source_offset: &FixedOffset,
) -> Result<(Vec<Bar>, Vec<String>)> {
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;

    let mut bars = Vec::new();
    let mut warnings = Vec::new();

    for (line_idx, record) in reader.records().enumerate() {
        let line_num = line_idx + 2; // +1 for header, +1 for 1-based
        let record = match record {
            Ok(r) => r,
            Err(e) => {
                warnings.push(format!("line {}: {}", line_num, e));
                continue;
            }
        };

        let date_str = record.get(0).unwrap_or("").trim();
        let time_str = record.get(1).unwrap_or("").trim();

        let ts = match parse_datetime_to_utc(date_str, time_str, source_offset) {
            Ok(t) => t,
            Err(e) => {
                warnings.push(format!("line {}: {}", line_num, e));
                continue;
            }
        };

        // Fields: DATE, TIME, OPEN, HIGH, LOW, CLOSE, TICKVOL, VOL, SPREAD
        let open = match parse_required_f64(record.get(2), "open", line_num) {
            Ok(v) => v,
            Err(w) => {
                warnings.push(w);
                continue;
            }
        };
        let high = match parse_required_f64(record.get(3), "high", line_num) {
            Ok(v) => v,
            Err(w) => {
                warnings.push(w);
                continue;
            }
        };
        let low = match parse_required_f64(record.get(4), "low", line_num) {
            Ok(v) => v,
            Err(w) => {
                warnings.push(w);
                continue;
            }
        };
        let close = match parse_required_f64(record.get(5), "close", line_num) {
            Ok(v) => v,
            Err(w) => {
                warnings.push(w);
                continue;
            }
        };

        let tick_vol = parse_required_i64(record.get(6)).unwrap_or(0);
        let volume = parse_required_i64(record.get(7)).unwrap_or(0);
        let spread = parse_required_i32(record.get(8)).unwrap_or(0);

        bars.push(Bar {
            exchange: exchange.to_string(),
            symbol: symbol.to_string(),
            timeframe,
            ts,
            open,
            high,
            low,
            close,
            tick_vol,
            volume,
            spread,
        });
    }

    Ok((bars, warnings))
}
