pub mod bar_csv;
pub mod tick_csv;

use crate::error::{DataError, Result};
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use std::path::Path;

/// Extract the symbol from a filename (first segment before '_'), uppercased.
pub fn extract_symbol_from_filename(path: &Path) -> Result<String> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| DataError::SymbolExtraction(path.display().to_string()))?;
    let symbol = stem
        .split('_')
        .next()
        .ok_or_else(|| DataError::SymbolExtraction(stem.to_string()))?;
    if symbol.is_empty() {
        return Err(DataError::SymbolExtraction(stem.to_string()));
    }
    Ok(symbol.to_uppercase())
}

/// Normalize exchange name: lowercase, trimmed.
pub fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_lowercase()
}

/// Parse a timezone offset string like "+02:00" or "-05:00" into FixedOffset.
pub fn parse_tz_offset(s: &str) -> Result<FixedOffset> {
    let s = s.trim();
    if s.len() < 5 {
        return Err(DataError::InvalidTimestamp(format!(
            "invalid tz offset: {s}"
        )));
    }
    let sign = match s.as_bytes()[0] {
        b'+' => 1i32,
        b'-' => -1i32,
        _ => {
            return Err(DataError::InvalidTimestamp(format!(
                "tz offset must start with +/-: {s}"
            )))
        }
    };
    let rest = &s[1..];
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 2 {
        return Err(DataError::InvalidTimestamp(format!(
            "invalid tz offset format: {s}"
        )));
    }
    let hours: i32 = parts[0]
        .parse()
        .map_err(|_| DataError::InvalidTimestamp(format!("bad hours in tz: {s}")))?;
    let minutes: i32 = parts[1]
        .parse()
        .map_err(|_| DataError::InvalidTimestamp(format!("bad minutes in tz: {s}")))?;
    let total_secs = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(total_secs)
        .ok_or_else(|| DataError::InvalidTimestamp(format!("out of range tz offset: {s}")))
}

/// Parse date+time CSV columns into NaiveDateTime (UTC).
/// Interprets the input as the given source_offset, converts to UTC.
pub fn parse_datetime_to_utc(
    date_str: &str,
    time_str: &str,
    source_offset: &FixedOffset,
) -> Result<NaiveDateTime> {
    let date = NaiveDate::parse_from_str(date_str, "%Y.%m.%d")
        .map_err(|e| DataError::InvalidTimestamp(format!("{date_str}: {e}")))?;
    // Handles both "HH:MM:SS" and "HH:MM:SS.mmm"
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
        .map_err(|e| DataError::InvalidTimestamp(format!("{time_str}: {e}")))?;
    let ndt = NaiveDateTime::new(date, time);
    let local = source_offset
        .from_local_datetime(&ndt)
        .single()
        .ok_or_else(|| DataError::InvalidTimestamp(format!("ambiguous datetime: {ndt}")))?;
    Ok(local.naive_utc())
}

/// Parse CLI datetime argument: "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS".
pub fn parse_datetime_arg(s: &str) -> Result<NaiveDateTime> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(DataError::InvalidTimestamp(format!(
        "expected YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS, got: {s}"
    )))
}

/// Parse an optional f64 from a CSV field (empty or whitespace → None).
pub fn parse_optional_f64(field: Option<&str>) -> Option<f64> {
    field
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

/// Parse an optional i32 from a CSV field (empty or whitespace → None).
pub fn parse_optional_i32(field: Option<&str>) -> Option<i32> {
    field
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

/// Parse a required f64 field; returns Err(warning string) if missing or invalid.
pub fn parse_required_f64(
    field: Option<&str>,
    name: &str,
    line: usize,
) -> std::result::Result<f64, String> {
    field
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| format!("line {}: missing or invalid {}", line, name))
}

/// Parse a required i64 field; returns None if missing or invalid.
pub fn parse_required_i64(field: Option<&str>) -> Option<i64> {
    field
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}

/// Parse a required i32 field; returns None if missing or invalid.
pub fn parse_required_i32(field: Option<&str>) -> Option<i32> {
    field
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse().ok())
}
