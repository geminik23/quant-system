use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike};
use thiserror::Error;

/// Error returned when a backtest wire timestamp is not canonicalizable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BacktestTimestampError {
    #[error("backtest timestamp is empty")]
    Empty,
    #[error("backtest timestamp must not contain surrounding whitespace")]
    SurroundingWhitespace,
    #[error("RFC 3339 -00:00 offsets are not supported")]
    UnknownLocalOffset,
    #[error("leap-second timestamps are not supported")]
    LeapSecond,
    #[error("invalid backtest timestamp format")]
    InvalidFormat,
}

/// Parse a backtest wire timestamp as UTC without consulting the OS timezone.
pub fn parse_backtest_timestamp(input: &str) -> Result<NaiveDateTime, BacktestTimestampError> {
    if input.is_empty() {
        return Err(BacktestTimestampError::Empty);
    }
    if input.trim() != input {
        return Err(BacktestTimestampError::SurroundingWhitespace);
    }
    if input.ends_with("-00:00") {
        return Err(BacktestTimestampError::UnknownLocalOffset);
    }

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(input) {
        let timestamp = timestamp.naive_utc();
        reject_leap_second(timestamp)?;
        return Ok(timestamp);
    }

    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(input, format) {
            reject_leap_second(timestamp)?;
            return Ok(timestamp);
        }
    }

    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .ok_or(BacktestTimestampError::InvalidFormat);
    }

    Err(BacktestTimestampError::InvalidFormat)
}

/// Normalize a backtest wire timestamp to a UTC naive ISO representation.
pub fn canonical_backtest_timestamp(input: &str) -> Result<String, BacktestTimestampError> {
    let timestamp = parse_backtest_timestamp(input)?;
    let base = timestamp.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanoseconds = timestamp.nanosecond();
    if nanoseconds == 0 {
        return Ok(base);
    }

    let fraction = format!("{nanoseconds:09}");
    Ok(format!("{base}.{}", fraction.trim_end_matches('0')))
}

fn reject_leap_second(timestamp: NaiveDateTime) -> Result<(), BacktestTimestampError> {
    if timestamp.nanosecond() >= 1_000_000_000 {
        return Err(BacktestTimestampError::LeapSecond);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_supported_timestamp_forms() {
        assert_eq!(
            canonical_backtest_timestamp("2026-01-15"),
            Ok("2026-01-15T00:00:00".into())
        );
        assert_eq!(
            canonical_backtest_timestamp("2026-01-15 10:30:00.120000"),
            Ok("2026-01-15T10:30:00.12".into())
        );
        assert_eq!(
            canonical_backtest_timestamp("2026-01-15T00:30:00+02:00"),
            Ok("2026-01-14T22:30:00".into())
        );
    }

    #[test]
    fn rejects_ambiguous_or_noncanonical_timestamp_forms() {
        assert_eq!(
            parse_backtest_timestamp(" 2026-01-15T10:30:00"),
            Err(BacktestTimestampError::SurroundingWhitespace)
        );
        assert_eq!(
            parse_backtest_timestamp("2026-01-15T10:30:00-00:00"),
            Err(BacktestTimestampError::UnknownLocalOffset)
        );
        assert_eq!(
            parse_backtest_timestamp("2026-01-15T10:30:60Z"),
            Err(BacktestTimestampError::LeapSecond)
        );
        assert_eq!(
            parse_backtest_timestamp("2026-01-15T24:00:00"),
            Err(BacktestTimestampError::InvalidFormat)
        );
        assert_eq!(
            parse_backtest_timestamp("2026-01-15T10:30:00 UTC"),
            Err(BacktestTimestampError::InvalidFormat)
        );
    }
}
