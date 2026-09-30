//! Neutral fixed-UTC session membership as a configured named input.

use chrono::{NaiveTime, Timelike};
use qs_strategy::{ScalarType, Value, ValueType};

use super::configured::{
    HistoricalNamedInputProjector, NamedInputProjectionContext, NamedInputProjectionError,
    ProjectedNamedInput,
};

const SECONDS_PER_DAY: u32 = 86_400;

/// Maximum number of windows one session projector accepts.
pub const MAX_SESSION_WINDOWS: usize = 16;

/// Invalid fixed-UTC session configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FixedUtcSessionError {
    #[error("a session needs at least one window")]
    NoWindows,
    #[error("a session accepts at most {maximum} windows, got {actual}")]
    TooManyWindows { actual: usize, maximum: usize },
    #[error("session window {index} starts and ends at the same time")]
    EmptyWindow { index: usize },
}

/// Reports whether a boundary falls inside any fixed UTC window, as a required boolean named input.
///
/// Each window is half-open, from its start up to but excluding its end, measured in seconds since UTC midnight; a window whose end is earlier than its start wraps past midnight. Sub-second parts of the boundary time are ignored. Sessions that follow a venue calendar or daylight-saving rule remain caller-owned projectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedUtcSessionProjector {
    windows: Vec<(u32, u32)>,
}

impl FixedUtcSessionProjector {
    pub fn new(
        windows: impl IntoIterator<Item = (NaiveTime, NaiveTime)>,
    ) -> Result<Self, FixedUtcSessionError> {
        let windows = windows
            .into_iter()
            .map(|(start, end)| {
                (
                    start.num_seconds_from_midnight(),
                    end.num_seconds_from_midnight(),
                )
            })
            .collect::<Vec<_>>();
        if windows.is_empty() {
            return Err(FixedUtcSessionError::NoWindows);
        }
        if windows.len() > MAX_SESSION_WINDOWS {
            return Err(FixedUtcSessionError::TooManyWindows {
                actual: windows.len(),
                maximum: MAX_SESSION_WINDOWS,
            });
        }
        if let Some(index) = windows.iter().position(|(start, end)| start == end) {
            return Err(FixedUtcSessionError::EmptyWindow { index });
        }
        Ok(Self { windows })
    }

    /// Whether the given seconds since UTC midnight fall inside any window.
    pub fn contains(&self, seconds_of_day: u32) -> bool {
        let second = seconds_of_day % SECONDS_PER_DAY;
        self.windows.iter().any(|&(start, end)| {
            if start < end {
                (start..end).contains(&second)
            } else {
                second >= start || second < end
            }
        })
    }
}

impl HistoricalNamedInputProjector for FixedUtcSessionProjector {
    fn output_type(&self) -> ValueType {
        ValueType::required(ScalarType::Bool)
    }

    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        Ok(ProjectedNamedInput {
            value: Value::Bool(self.contains(context.observed_through.num_seconds_from_midnight())),
            updated: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    #[test]
    fn windows_are_half_open_and_may_wrap_past_midnight() {
        let london = FixedUtcSessionProjector::new([(at(7, 0), at(16, 0))]).unwrap();
        assert!(!london.contains(at(6, 59).num_seconds_from_midnight() + 59));
        assert!(london.contains(at(7, 0).num_seconds_from_midnight()));
        assert!(london.contains(at(15, 59).num_seconds_from_midnight() + 59));
        assert!(!london.contains(at(16, 0).num_seconds_from_midnight()));

        let overnight = FixedUtcSessionProjector::new([(at(22, 0), at(2, 0))]).unwrap();
        assert!(overnight.contains(at(23, 30).num_seconds_from_midnight()));
        assert!(overnight.contains(at(0, 0).num_seconds_from_midnight()));
        assert!(overnight.contains(at(1, 59).num_seconds_from_midnight()));
        assert!(!overnight.contains(at(2, 0).num_seconds_from_midnight()));
        assert!(!overnight.contains(at(12, 0).num_seconds_from_midnight()));
    }

    #[test]
    fn empty_missing_and_excess_windows_are_rejected() {
        assert_eq!(
            FixedUtcSessionProjector::new([]).unwrap_err(),
            FixedUtcSessionError::NoWindows
        );
        assert_eq!(
            FixedUtcSessionProjector::new([(at(1, 0), at(2, 0)), (at(3, 0), at(3, 0))])
                .unwrap_err(),
            FixedUtcSessionError::EmptyWindow { index: 1 }
        );
        let many = (0..=MAX_SESSION_WINDOWS as u32).map(|hour| (at(hour, 0), at(hour, 30)));
        assert!(matches!(
            FixedUtcSessionProjector::new(many).unwrap_err(),
            FixedUtcSessionError::TooManyWindows { .. }
        ));
    }
}
