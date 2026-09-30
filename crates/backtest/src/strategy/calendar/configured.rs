use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use qs_strategy::{ScalarType, Value, ValueType};
use serde::{Deserialize, Serialize};

use super::{CalendarBar, CalendarError, CalendarFeatureKind, OpeningRange};
use crate::strategy::{
    ConfiguredNamedInputBinding, HistoricalNamedInputProjector, NamedInputProjectionContext,
    NamedInputProjectionError, ProjectedNamedInput, SeriesId,
};

pub const DEFAULT_CALENDAR_SESSION_ID: &str = "full_day";
pub const MAX_CALENDAR_ID_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarTimeBasis {
    #[default]
    SourceOpen,
    DecisionTime,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionScheduleSpec {
    #[default]
    FullDay,
    Custom {
        items: Vec<NamedSessionSpec>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionSpanSpec {
    FullDay,
    Timed {
        start: NaiveTime,
        end: NaiveTime,
        end_day_offset: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedSessionSpec {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    pub span: SessionSpanSpec,
    #[serde(default)]
    pub weekdays: BTreeSet<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum MarketScheduleSpec {
    #[default]
    Unspecified,
    Continuous,
    Weekly {
        intervals: Vec<WeeklyMarketIntervalSpec>,
        #[serde(default)]
        exceptions: BTreeMap<NaiveDate, Vec<LocalMarketIntervalSpec>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeeklyMarketIntervalSpec {
    pub weekday: u8,
    pub start: NaiveTime,
    pub end: NaiveTime,
    pub end_day_offset: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalMarketIntervalSpec {
    pub start: NaiveTime,
    pub end: NaiveTime,
    pub end_day_offset: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradingCalendarSpec {
    pub id: String,
    pub timezone: String,
    #[serde(default = "midnight")]
    pub day_boundary: NaiveTime,
    #[serde(default)]
    pub sessions: SessionScheduleSpec,
    #[serde(default)]
    pub market: MarketScheduleSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarAdmissionLimits {
    pub max_sessions: usize,
    pub max_market_intervals: usize,
    pub max_exceptions: usize,
    pub max_history_occurrences: usize,
    pub max_resolved_children: usize,
    pub max_owned_bytes: usize,
}

impl CalendarAdmissionLimits {
    pub fn new(
        max_sessions: usize,
        max_market_intervals: usize,
        max_exceptions: usize,
        max_history_occurrences: usize,
        max_resolved_children: usize,
        max_owned_bytes: usize,
    ) -> Result<Self, CalendarError> {
        if [
            max_sessions,
            max_market_intervals,
            max_exceptions,
            max_history_occurrences,
            max_resolved_children,
            max_owned_bytes,
        ]
        .contains(&0)
        {
            return Err(CalendarError::InvalidConfiguration(
                "calendar admission limits must be positive".into(),
            ));
        }
        Ok(Self {
            max_sessions,
            max_market_intervals,
            max_exceptions,
            max_history_occurrences,
            max_resolved_children,
            max_owned_bytes,
        })
    }
}

impl Default for CalendarAdmissionLimits {
    fn default() -> Self {
        Self {
            max_sessions: 32,
            max_market_intervals: 64,
            max_exceptions: 366,
            max_history_occurrences: 512,
            max_resolved_children: 1_000_000,
            max_owned_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOccurrenceId {
    pub calendar_id: String,
    pub session_id: String,
    pub start_utc: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTradingDay {
    pub label: NaiveDate,
    pub week_monday: NaiveDate,
    pub start_utc: NaiveDateTime,
    pub end_utc: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSessionOccurrence {
    pub id: SessionOccurrenceId,
    pub trading_day: NaiveDate,
    pub start_utc: NaiveDateTime,
    pub end_utc: NaiveDateTime,
}

#[derive(Debug, Clone)]
struct EffectiveSession {
    id: String,
    timezone: Tz,
    span: SessionSpanSpec,
    weekdays: BTreeSet<u8>,
}

#[derive(Debug, Clone)]
pub struct ConfiguredTradingCalendar {
    id: String,
    timezone: Tz,
    day_boundary: NaiveTime,
    sessions: Vec<EffectiveSession>,
    market: MarketScheduleSpec,
    limits: CalendarAdmissionLimits,
}

impl ConfiguredTradingCalendar {
    pub fn new(
        spec: TradingCalendarSpec,
        limits: CalendarAdmissionLimits,
    ) -> Result<Self, CalendarError> {
        validate_id("calendar", &spec.id)?;
        let timezone = spec
            .timezone
            .parse::<Tz>()
            .map_err(|_| CalendarError::InvalidTimezone(spec.timezone.clone()))?;
        let raw_sessions = match spec.sessions {
            SessionScheduleSpec::FullDay => vec![NamedSessionSpec {
                id: DEFAULT_CALENDAR_SESSION_ID.into(),
                timezone: None,
                span: SessionSpanSpec::FullDay,
                weekdays: BTreeSet::new(),
            }],
            SessionScheduleSpec::Custom { items } if items.is_empty() => {
                return Err(CalendarError::InvalidConfiguration(
                    "custom calendar sessions cannot be empty".into(),
                ));
            }
            SessionScheduleSpec::Custom { items } => items,
        };
        if raw_sessions.len() > limits.max_sessions {
            return Err(CalendarError::ResourceLimit(
                "calendar session count exceeds admission".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut sessions = Vec::with_capacity(raw_sessions.len());
        for session in raw_sessions {
            validate_id("session", &session.id)?;
            if !ids.insert(session.id.clone()) {
                return Err(CalendarError::InvalidConfiguration(format!(
                    "duplicate calendar session '{}'",
                    session.id
                )));
            }
            if session.weekdays.iter().any(|weekday| *weekday > 6) {
                return Err(CalendarError::InvalidConfiguration(format!(
                    "session '{}' has an invalid weekday",
                    session.id
                )));
            }
            validate_span(&session.id, &session.span)?;
            let session_timezone = match session.timezone {
                Some(value) => value
                    .parse::<Tz>()
                    .map_err(|_| CalendarError::InvalidTimezone(value))?,
                None => timezone,
            };
            sessions.push(EffectiveSession {
                id: session.id,
                timezone: session_timezone,
                span: session.span,
                weekdays: session.weekdays,
            });
        }
        validate_market(&spec.market, limits)?;
        let owned_bytes = spec
            .id
            .len()
            .checked_add(spec.timezone.len())
            .and_then(|value| {
                sessions
                    .iter()
                    .try_fold(value, |total, session| total.checked_add(session.id.len()))
            })
            .ok_or_else(|| CalendarError::ResourceLimit("calendar byte count overflowed".into()))?;
        if owned_bytes > limits.max_owned_bytes {
            return Err(CalendarError::ResourceLimit(
                "calendar configuration exceeds owned-byte admission".into(),
            ));
        }
        Ok(Self {
            id: spec.id,
            timezone,
            day_boundary: spec.day_boundary,
            sessions,
            market: spec.market,
            limits,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn session_ids(&self) -> impl Iterator<Item = &str> {
        self.sessions.iter().map(|session| session.id.as_str())
    }

    pub fn resolve_trading_day(
        &self,
        label: NaiveDate,
    ) -> Result<ResolvedTradingDay, CalendarError> {
        let next = label.succ_opt().ok_or(CalendarError::TimestampOverflow)?;
        let start_utc = resolve_boundary(self.timezone, label.and_time(self.day_boundary))?;
        let end_utc = resolve_boundary(self.timezone, next.and_time(self.day_boundary))?;
        if end_utc <= start_utc {
            return Err(CalendarError::NonPositiveSession);
        }
        let week_monday = label
            .checked_sub_days(chrono::Days::new(
                label.weekday().num_days_from_monday() as u64
            ))
            .ok_or(CalendarError::TimestampOverflow)?;
        Ok(ResolvedTradingDay {
            label,
            week_monday,
            start_utc,
            end_utc,
        })
    }

    pub fn trading_day_containing(
        &self,
        utc: NaiveDateTime,
    ) -> Result<ResolvedTradingDay, CalendarError> {
        let local_date = Utc
            .from_utc_datetime(&utc)
            .with_timezone(&self.timezone)
            .date_naive();
        for label in [
            local_date.pred_opt(),
            Some(local_date),
            local_date.succ_opt(),
        ]
        .into_iter()
        .flatten()
        {
            let day = self.resolve_trading_day(label)?;
            if utc >= day.start_utc && utc < day.end_utc {
                return Ok(day);
            }
        }
        Err(CalendarError::InvalidConfiguration(
            "instant is not contained by adjacent trading-day boundaries".into(),
        ))
    }

    pub fn session_occurrences(
        &self,
        trading_day: &ResolvedTradingDay,
    ) -> Result<Vec<ResolvedSessionOccurrence>, CalendarError> {
        let mut occurrences = Vec::new();
        for session in &self.sessions {
            match session.span {
                SessionSpanSpec::FullDay => {
                    let weekday = trading_day.label.weekday().num_days_from_monday() as u8;
                    if !session.weekdays.is_empty() && !session.weekdays.contains(&weekday) {
                        continue;
                    }
                    occurrences.push(ResolvedSessionOccurrence {
                        id: SessionOccurrenceId {
                            calendar_id: self.id.clone(),
                            session_id: session.id.clone(),
                            start_utc: trading_day.start_utc,
                        },
                        trading_day: trading_day.label,
                        start_utc: trading_day.start_utc,
                        end_utc: trading_day.end_utc,
                    });
                }
                SessionSpanSpec::Timed {
                    start,
                    end,
                    end_day_offset,
                } => {
                    let local_at_day_start = Utc
                        .from_utc_datetime(&trading_day.start_utc)
                        .with_timezone(&session.timezone)
                        .date_naive();
                    for date in [
                        local_at_day_start.pred_opt(),
                        Some(local_at_day_start),
                        local_at_day_start.succ_opt(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        let weekday = date.weekday().num_days_from_monday() as u8;
                        if !session.weekdays.is_empty() && !session.weekdays.contains(&weekday) {
                            continue;
                        }
                        let end_date = date
                            .checked_add_days(chrono::Days::new(u64::from(end_day_offset)))
                            .ok_or(CalendarError::TimestampOverflow)?;
                        let start_utc =
                            resolve_session_local(session.timezone, date.and_time(start), true)?;
                        let end_utc =
                            resolve_session_local(session.timezone, end_date.and_time(end), false)?;
                        if end_utc <= start_utc {
                            return Err(CalendarError::NonPositiveSession);
                        }
                        if start_utc >= trading_day.start_utc && start_utc < trading_day.end_utc {
                            occurrences.push(ResolvedSessionOccurrence {
                                id: SessionOccurrenceId {
                                    calendar_id: self.id.clone(),
                                    session_id: session.id.clone(),
                                    start_utc,
                                },
                                trading_day: trading_day.label,
                                start_utc,
                                end_utc,
                            });
                        }
                    }
                }
            }
        }
        occurrences.sort_by(|left, right| {
            left.start_utc
                .cmp(&right.start_utc)
                .then_with(|| left.id.session_id.cmp(&right.id.session_id))
        });
        let mut last_end = BTreeMap::new();
        for occurrence in &occurrences {
            if last_end
                .get(&occurrence.id.session_id)
                .is_some_and(|end| occurrence.start_utc < *end)
            {
                return Err(CalendarError::InvalidConfiguration(format!(
                    "session '{}' has overlapping occurrences",
                    occurrence.id.session_id
                )));
            }
            last_end.insert(occurrence.id.session_id.clone(), occurrence.end_utc);
        }
        Ok(occurrences)
    }

    pub fn selected_occurrence_containing(
        &self,
        session_id: &str,
        utc: NaiveDateTime,
    ) -> Result<Option<ResolvedSessionOccurrence>, CalendarError> {
        let day = self.trading_day_containing(utc)?;
        for label in [day.label.pred_opt(), Some(day.label)]
            .into_iter()
            .flatten()
        {
            let candidate_day = self.resolve_trading_day(label)?;
            for occurrence in self.session_occurrences(&candidate_day)? {
                if occurrence.id.session_id == session_id
                    && utc >= occurrence.start_utc
                    && utc < occurrence.end_utc
                {
                    return Ok(Some(occurrence));
                }
            }
        }
        Ok(None)
    }

    pub fn latest_occurrence(
        &self,
        session_id: &str,
        at: NaiveDateTime,
    ) -> Result<Option<ResolvedSessionOccurrence>, CalendarError> {
        let current_day = self.trading_day_containing(at)?;
        let mut best = None;
        for days_back in 0..=2 {
            let Some(label) = current_day
                .label
                .checked_sub_days(chrono::Days::new(days_back))
            else {
                break;
            };
            let day = self.resolve_trading_day(label)?;
            for occurrence in self.session_occurrences(&day)? {
                if occurrence.id.session_id == session_id
                    && occurrence.start_utc <= at
                    && best
                        .as_ref()
                        .is_none_or(|value: &ResolvedSessionOccurrence| {
                            occurrence.start_utc > value.start_utc
                        })
                {
                    best = Some(occurrence);
                }
            }
        }
        Ok(best)
    }

    pub fn opening_range_end(
        &self,
        occurrence: &ResolvedSessionOccurrence,
        minutes: u32,
    ) -> Result<NaiveDateTime, CalendarError> {
        let session = self
            .sessions
            .iter()
            .find(|session| session.id == occurrence.id.session_id)
            .ok_or_else(|| {
                CalendarError::InvalidConfiguration(format!(
                    "unknown calendar session '{}'",
                    occurrence.id.session_id
                ))
            })?;
        let start_local = Utc
            .from_utc_datetime(&occurrence.start_utc)
            .with_timezone(&session.timezone)
            .naive_local();
        let end_local = start_local
            .checked_add_signed(Duration::minutes(i64::from(minutes)))
            .ok_or(CalendarError::TimestampOverflow)?;
        resolve_session_local(session.timezone, end_local, false)
            .map(|end| end.min(occurrence.end_utc))
    }

    pub fn previous_occurrence(
        &self,
        session_id: &str,
        before: NaiveDateTime,
    ) -> Result<Option<ResolvedSessionOccurrence>, CalendarError> {
        let current_day = self.trading_day_containing(before)?;
        let scan = self.limits.max_history_occurrences.min(4096);
        let mut best = None;
        for days_back in 0..=scan {
            let Some(label) = current_day
                .label
                .checked_sub_days(chrono::Days::new(days_back as u64))
            else {
                break;
            };
            let day = self.resolve_trading_day(label)?;
            for occurrence in self.session_occurrences(&day)? {
                if occurrence.id.session_id == session_id
                    && occurrence.end_utc <= before
                    && best
                        .as_ref()
                        .is_none_or(|value: &ResolvedSessionOccurrence| {
                            occurrence.end_utc > value.end_utc
                        })
                {
                    best = Some(occurrence);
                }
            }
            if best.is_some() {
                break;
            }
        }
        Ok(best)
    }

    pub fn previous_trading_day(
        &self,
        current: NaiveDate,
        child_seconds: i64,
        alignment_offset_seconds: i64,
    ) -> Result<Option<ResolvedTradingDay>, CalendarError> {
        let scan = self.limits.max_history_occurrences.min(4096);
        for days_back in 1..=scan {
            let Some(label) = current.checked_sub_days(chrono::Days::new(days_back as u64)) else {
                break;
            };
            let day = self.resolve_trading_day(label)?;
            if self.day_is_eligible(&day, child_seconds, alignment_offset_seconds)? {
                return Ok(Some(day));
            }
        }
        Ok(None)
    }

    pub fn expected_child_opens(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        child_seconds: i64,
        alignment_offset_seconds: i64,
    ) -> Result<Option<Vec<NaiveDateTime>>, CalendarError> {
        if child_seconds <= 0 || end <= start {
            return Err(CalendarError::InvalidChildGeometry);
        }
        let intervals = match &self.market {
            MarketScheduleSpec::Unspecified => return Ok(None),
            MarketScheduleSpec::Continuous => vec![(start, end)],
            MarketScheduleSpec::Weekly { .. } => self.market_intervals(start, end)?,
        };
        let mut opens = Vec::new();
        for (interval_start, interval_end) in intervals {
            if (interval_start.and_utc().timestamp() - alignment_offset_seconds)
                .rem_euclid(child_seconds)
                != 0
                || (interval_end - interval_start).num_seconds() % child_seconds != 0
            {
                return Err(CalendarError::InvalidChildGeometry);
            }
            let mut open = interval_start;
            while open < interval_end {
                if opens.len() >= self.limits.max_resolved_children {
                    return Err(CalendarError::ResourceLimit(
                        "calendar child count exceeds admission".into(),
                    ));
                }
                opens.push(open);
                open = open
                    .checked_add_signed(Duration::seconds(child_seconds))
                    .ok_or(CalendarError::TimestampOverflow)?;
            }
        }
        opens.sort_unstable();
        opens.dedup();
        Ok(Some(opens))
    }

    fn day_is_eligible(
        &self,
        day: &ResolvedTradingDay,
        child_seconds: i64,
        alignment_offset_seconds: i64,
    ) -> Result<bool, CalendarError> {
        match self.expected_child_opens(
            day.start_utc,
            day.end_utc,
            child_seconds,
            alignment_offset_seconds,
        )? {
            Some(opens) => Ok(!opens.is_empty()),
            None => Ok(true),
        }
    }

    fn market_intervals(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
    ) -> Result<Vec<(NaiveDateTime, NaiveDateTime)>, CalendarError> {
        let MarketScheduleSpec::Weekly {
            intervals,
            exceptions,
        } = &self.market
        else {
            return Ok(vec![(start, end)]);
        };
        let start_date = Utc
            .from_utc_datetime(&start)
            .with_timezone(&self.timezone)
            .date_naive()
            .pred_opt()
            .ok_or(CalendarError::TimestampOverflow)?;
        let end_date = Utc
            .from_utc_datetime(&end)
            .with_timezone(&self.timezone)
            .date_naive()
            .succ_opt()
            .ok_or(CalendarError::TimestampOverflow)?;
        let mut result = Vec::new();
        let mut date = start_date;
        while date <= end_date {
            let local_intervals = if let Some(replacement) = exceptions.get(&date) {
                replacement.clone()
            } else {
                let weekday = date.weekday().num_days_from_monday() as u8;
                intervals
                    .iter()
                    .filter(|item| item.weekday == weekday)
                    .map(|item| LocalMarketIntervalSpec {
                        start: item.start,
                        end: item.end,
                        end_day_offset: item.end_day_offset,
                    })
                    .collect()
            };
            for interval in local_intervals {
                let interval_end_date = date
                    .checked_add_days(chrono::Days::new(u64::from(interval.end_day_offset)))
                    .ok_or(CalendarError::TimestampOverflow)?;
                let resolved_start =
                    resolve_session_local(self.timezone, date.and_time(interval.start), true)?;
                let resolved_end = resolve_session_local(
                    self.timezone,
                    interval_end_date.and_time(interval.end),
                    false,
                )?;
                let clipped_start = resolved_start.max(start);
                let clipped_end = resolved_end.min(end);
                if clipped_end > clipped_start {
                    result.push((clipped_start, clipped_end));
                }
            }
            date = date.succ_opt().ok_or(CalendarError::TimestampOverflow)?;
        }
        result.sort_unstable();
        let mut merged: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
        for interval in result {
            if let Some(last) = merged.last_mut()
                && interval.0 <= last.1
            {
                last.1 = last.1.max(interval.1);
            } else {
                merged.push(interval);
            }
        }
        Ok(merged)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredCalendarInput {
    pub name: String,
    pub source: String,
    pub calendar: TradingCalendarSpec,
    pub input: CalendarInputSpec,
    #[serde(default)]
    pub limits: CalendarAdmissionLimits,
}

impl ConfiguredCalendarInput {
    pub fn history_start(
        &self,
        evaluation_start: NaiveDateTime,
    ) -> Result<NaiveDateTime, CalendarError> {
        let calendar = ConfiguredTradingCalendar::new(self.calendar.clone(), self.limits)?;
        let current = calendar.trading_day_containing(evaluation_start)?;
        let child_seconds = i64::try_from(self.input.child_seconds)
            .map_err(|_| CalendarError::InvalidChildGeometry)?;
        match self.input.kind {
            CalendarFeatureKind::PreviousSessionHigh
            | CalendarFeatureKind::PreviousSessionLow
            | CalendarFeatureKind::PastSameSlotRangeRatio
            | CalendarFeatureKind::PastSameSlotCount => {
                let session_id = self.input.session_id.as_deref().unwrap_or(
                    calendar.session_ids().next().ok_or_else(|| {
                        CalendarError::InvalidConfiguration(
                            "calendar has no effective session".into(),
                        )
                    })?,
                );
                let mut before = evaluation_start;
                let mut earliest = current.start_utc;
                for _ in 0..self.input.maximum_history {
                    let occurrence = calendar
                        .previous_occurrence(session_id, before)?
                        .ok_or_else(|| {
                            CalendarError::InvalidConfiguration(format!(
                                "calendar history cannot resolve {} prior '{}' occurrences",
                                self.input.maximum_history, session_id
                            ))
                        })?;
                    earliest = earliest.min(occurrence.start_utc);
                    before = occurrence.start_utc;
                }
                Ok(earliest)
            }
            CalendarFeatureKind::PreviousDayHigh | CalendarFeatureKind::PreviousDayLow => {
                let mut label = current.label;
                let mut earliest = current.start_utc;
                for _ in 0..self.input.maximum_history {
                    let day = calendar
                        .previous_trading_day(
                            label,
                            child_seconds,
                            self.input.alignment_offset_seconds,
                        )?
                        .ok_or_else(|| {
                            CalendarError::InvalidConfiguration(
                                "calendar history cannot resolve the required prior trading days"
                                    .into(),
                            )
                        })?;
                    earliest = earliest.min(day.start_utc);
                    label = day.label;
                }
                Ok(earliest)
            }
            CalendarFeatureKind::PreviousWeekHigh | CalendarFeatureKind::PreviousWeekLow => {
                let weeks = self
                    .input
                    .maximum_history
                    .checked_add(1)
                    .and_then(|value| value.checked_mul(7))
                    .ok_or_else(|| {
                        CalendarError::ResourceLimit("calendar week history overflowed".into())
                    })?;
                let label = current
                    .week_monday
                    .checked_sub_days(chrono::Days::new(weeks as u64))
                    .ok_or(CalendarError::TimestampOverflow)?;
                calendar.resolve_trading_day(label).map(|day| day.start_utc)
            }
            _ => Ok(current.start_utc),
        }
    }

    pub fn estimated_owned_bytes(&self) -> Result<usize, CalendarError> {
        let calendar = ConfiguredTradingCalendar::new(self.calendar.clone(), self.limits)?;
        let child_seconds = i64::try_from(self.input.child_seconds)
            .map_err(|_| CalendarError::InvalidChildGeometry)?;
        estimated_projector_bytes(&calendar, &self.input, child_seconds)
    }

    pub fn binding(&self) -> Result<ConfiguredNamedInputBinding, CalendarError> {
        validate_id("historical input", &self.name)?;
        let series_id = SeriesId::new(&self.source)
            .map_err(|error| CalendarError::InvalidConfiguration(error.to_string()))?;
        let calendar = ConfiguredTradingCalendar::new(self.calendar.clone(), self.limits)?;
        let projector =
            ConfiguredCalendarFeatureProjector::new(series_id, calendar, self.input.clone())?;
        Ok(ConfiguredNamedInputBinding::new(
            self.name.clone(),
            Box::new(projector),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarInputSpec {
    pub calendar_id: String,
    pub kind: CalendarFeatureKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default)]
    pub time_basis: CalendarTimeBasis,
    #[serde(default = "default_opening_range_minutes")]
    pub opening_range_minutes: u32,
    pub child_seconds: u64,
    pub alignment_offset_seconds: i64,
    pub maximum_history: usize,
}

#[derive(Debug, Clone, Default)]
struct PeriodAccumulator {
    stats: super::CalendarPeriodStats,
    observed_opens: BTreeSet<NaiveDateTime>,
}

impl PeriodAccumulator {
    fn observe(&mut self, bar: &crate::strategy::series::ClosedBar) {
        self.stats.observe(bar.high(), bar.low());
        self.observed_opens.insert(bar.open_time());
    }
}

#[derive(Debug, Default)]
struct ConfiguredCalendarState {
    last_open: Option<NaiveDateTime>,
    retained: Option<Value>,
    days: BTreeMap<NaiveDate, PeriodAccumulator>,
    weeks: BTreeMap<NaiveDate, PeriodAccumulator>,
    sessions: BTreeMap<SessionOccurrenceId, PeriodAccumulator>,
    opening_bars: BTreeMap<SessionOccurrenceId, Vec<CalendarBar>>,
    slot_ranges: BTreeMap<(String, u32), VecDeque<f64>>,
}

pub struct ConfiguredCalendarFeatureProjector {
    series_id: SeriesId,
    calendar: ConfiguredTradingCalendar,
    input: CalendarInputSpec,
    child_seconds: i64,
    selected_session_id: Option<String>,
    state: RefCell<ConfiguredCalendarState>,
}

impl ConfiguredCalendarFeatureProjector {
    pub fn new(
        series_id: SeriesId,
        calendar: ConfiguredTradingCalendar,
        mut input: CalendarInputSpec,
    ) -> Result<Self, CalendarError> {
        if input.calendar_id != calendar.id {
            return Err(CalendarError::InvalidConfiguration(format!(
                "calendar input references '{}', but received '{}'",
                input.calendar_id, calendar.id
            )));
        }
        if input.opening_range_minutes == 0
            || input.child_seconds < 60
            || input.maximum_history == 0
            || input.maximum_history > calendar.limits.max_history_occurrences
        {
            return Err(CalendarError::InvalidChildGeometry);
        }
        let child_seconds =
            i64::try_from(input.child_seconds).map_err(|_| CalendarError::InvalidChildGeometry)?;
        let estimated_bytes = estimated_projector_bytes(&calendar, &input, child_seconds)?;
        if estimated_bytes > calendar.limits.max_owned_bytes {
            return Err(CalendarError::ResourceLimit(format!(
                "calendar projector needs an estimated {estimated_bytes} bytes, above {}",
                calendar.limits.max_owned_bytes
            )));
        }
        let session_required = matches!(
            input.kind,
            CalendarFeatureKind::SessionElapsedSeconds
                | CalendarFeatureKind::SessionMembership
                | CalendarFeatureKind::PreviousSessionHigh
                | CalendarFeatureKind::PreviousSessionLow
                | CalendarFeatureKind::OpeningRangeHighSoFar
                | CalendarFeatureKind::OpeningRangeLowSoFar
                | CalendarFeatureKind::OpeningRangeHighFinal
                | CalendarFeatureKind::OpeningRangeLowFinal
                | CalendarFeatureKind::PastSameSlotRangeRatio
                | CalendarFeatureKind::PastSameSlotCount
        );
        let session_ids = calendar.session_ids().collect::<Vec<_>>();
        let selected_session_id = match (session_required, input.session_id.take()) {
            (true, Some(value)) => {
                if !session_ids.contains(&value.as_str()) {
                    return Err(CalendarError::InvalidConfiguration(format!(
                        "unknown calendar session '{value}'"
                    )));
                }
                Some(value)
            }
            (true, None) if session_ids == [DEFAULT_CALENDAR_SESSION_ID] => {
                Some(DEFAULT_CALENDAR_SESSION_ID.into())
            }
            (true, None) => {
                return Err(CalendarError::InvalidConfiguration(
                    "a custom-session calendar input requires session_id".into(),
                ));
            }
            (false, Some(_)) => {
                return Err(CalendarError::InvalidConfiguration(
                    "day/week calendar inputs cannot select a session".into(),
                ));
            }
            (false, None) => None,
        };
        input.session_id = selected_session_id.clone();
        Ok(Self {
            series_id,
            calendar,
            input,
            child_seconds,
            selected_session_id,
            state: RefCell::new(ConfiguredCalendarState::default()),
        })
    }

    pub fn effective_input(&self) -> &CalendarInputSpec {
        &self.input
    }

    fn missing(&self, updated: bool) -> ProjectedNamedInput {
        ProjectedNamedInput {
            value: Value::Missing(self.input.kind.scalar_type()),
            updated,
        }
    }

    fn period_complete(
        &self,
        period: &PeriodAccumulator,
        start: NaiveDateTime,
        end: NaiveDateTime,
        observed_through: NaiveDateTime,
    ) -> Result<bool, CalendarError> {
        if observed_through < end {
            return Ok(false);
        }
        let Some(expected) = self.calendar.expected_child_opens(
            start,
            end,
            self.child_seconds,
            self.input.alignment_offset_seconds,
        )?
        else {
            return Ok(false);
        };
        Ok(!expected.is_empty()
            && expected.len() == period.observed_opens.len()
            && expected
                .iter()
                .all(|open| period.observed_opens.contains(open)))
    }

    fn previous_day_stats(
        &self,
        state: &ConfiguredCalendarState,
        current: &ResolvedTradingDay,
        observed_through: NaiveDateTime,
    ) -> Result<Option<super::CalendarPeriodStats>, CalendarError> {
        let Some(previous) = self.calendar.previous_trading_day(
            current.label,
            self.child_seconds,
            self.input.alignment_offset_seconds,
        )?
        else {
            return Ok(None);
        };
        let Some(period) = state.days.get(&previous.label) else {
            return Ok(None);
        };
        self.period_complete(
            period,
            previous.start_utc,
            previous.end_utc,
            observed_through,
        )
        .map(|complete| complete.then_some(period.stats))
    }

    fn previous_week_stats(
        &self,
        state: &ConfiguredCalendarState,
        current: &ResolvedTradingDay,
        observed_through: NaiveDateTime,
    ) -> Result<Option<super::CalendarPeriodStats>, CalendarError> {
        let previous_monday = current
            .week_monday
            .checked_sub_days(chrono::Days::new(7))
            .ok_or(CalendarError::TimestampOverflow)?;
        let Some(period) = state.weeks.get(&previous_monday) else {
            return Ok(None);
        };
        let start = self
            .calendar
            .resolve_trading_day(previous_monday)?
            .start_utc;
        let end_label = previous_monday
            .checked_add_days(chrono::Days::new(7))
            .ok_or(CalendarError::TimestampOverflow)?;
        let end = self.calendar.resolve_trading_day(end_label)?.start_utc;
        self.period_complete(period, start, end, observed_through)
            .map(|complete| complete.then_some(period.stats))
    }

    fn previous_session_stats(
        &self,
        state: &ConfiguredCalendarState,
        session_id: &str,
        before: NaiveDateTime,
        observed_through: NaiveDateTime,
    ) -> Result<Option<super::CalendarPeriodStats>, CalendarError> {
        let Some(previous) = self.calendar.previous_occurrence(session_id, before)? else {
            return Ok(None);
        };
        let Some(period) = state.sessions.get(&previous.id) else {
            return Ok(None);
        };
        self.period_complete(
            period,
            previous.start_utc,
            previous.end_utc,
            observed_through,
        )
        .map(|complete| complete.then_some(period.stats))
    }

    fn retain_bounds(&self, state: &mut ConfiguredCalendarState) {
        let keep = self.input.maximum_history.saturating_add(2);
        while state.days.len() > keep {
            if let Some(oldest) = state.days.keys().next().copied() {
                state.days.remove(&oldest);
            }
        }
        while state.weeks.len() > keep {
            if let Some(oldest) = state.weeks.keys().next().copied() {
                state.weeks.remove(&oldest);
            }
        }
        while state.sessions.len() > keep {
            if let Some(oldest) = state.sessions.keys().next().cloned() {
                state.sessions.remove(&oldest);
                state.opening_bars.remove(&oldest);
            }
        }
    }
}

impl HistoricalNamedInputProjector for ConfiguredCalendarFeatureProjector {
    fn output_type(&self) -> ValueType {
        ValueType::optional(self.input.kind.scalar_type())
    }

    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError> {
        let Some(bar) = context
            .closed_bars
            .iter()
            .find(|bar| bar.series_id() == &self.series_id)
        else {
            return Ok(self.missing(false));
        };
        let mut state = self.state.borrow_mut();
        if state.last_open == Some(bar.open_time()) {
            return Ok(ProjectedNamedInput {
                value: state
                    .retained
                    .clone()
                    .unwrap_or_else(|| Value::Missing(self.input.kind.scalar_type())),
                updated: false,
            });
        }
        let day = self
            .calendar
            .trading_day_containing(bar.open_time())
            .map_err(project_error)?;
        let selected_occurrence = match self.selected_session_id.as_deref() {
            Some(session_id) => self
                .calendar
                .selected_occurrence_containing(session_id, bar.open_time())
                .map_err(project_error)?,
            None => None,
        };
        match self.input.kind {
            CalendarFeatureKind::PreviousDayHigh | CalendarFeatureKind::PreviousDayLow => {
                state.days.entry(day.label).or_default().observe(bar);
            }
            CalendarFeatureKind::PreviousWeekHigh | CalendarFeatureKind::PreviousWeekLow => {
                state.weeks.entry(day.week_monday).or_default().observe(bar);
            }
            CalendarFeatureKind::PreviousSessionHigh | CalendarFeatureKind::PreviousSessionLow => {
                if let Some(occurrence) = selected_occurrence.as_ref() {
                    state
                        .sessions
                        .entry(occurrence.id.clone())
                        .or_default()
                        .observe(bar);
                }
            }
            CalendarFeatureKind::OpeningRangeHighSoFar
            | CalendarFeatureKind::OpeningRangeLowSoFar
            | CalendarFeatureKind::OpeningRangeHighFinal
            | CalendarFeatureKind::OpeningRangeLowFinal => {
                if let Some(occurrence) = selected_occurrence.as_ref() {
                    let opening_end = self
                        .calendar
                        .opening_range_end(occurrence, self.input.opening_range_minutes)
                        .map_err(project_error)?;
                    if bar.open_time() >= occurrence.start_utc && bar.open_time() < opening_end {
                        state
                            .opening_bars
                            .entry(occurrence.id.clone())
                            .or_default()
                            .push(CalendarBar {
                                open_utc: bar.open_time(),
                                close_utc: bar.close_time(),
                                available_at: context.observed_through,
                                high: bar.high(),
                                low: bar.low(),
                            });
                    }
                }
            }
            _ => {}
        }
        self.retain_bounds(&mut state);

        let value = match self.input.kind {
            CalendarFeatureKind::LocalSecondOfDay => {
                let local = Utc
                    .from_utc_datetime(&bar.open_time())
                    .with_timezone(&self.calendar.timezone);
                Value::Integer(i64::from(local.time().num_seconds_from_midnight()))
            }
            CalendarFeatureKind::SessionMembership => {
                let instant = match self.input.time_basis {
                    CalendarTimeBasis::SourceOpen => bar.open_time(),
                    CalendarTimeBasis::DecisionTime => context.observed_through,
                };
                let session_id = self
                    .selected_session_id
                    .as_deref()
                    .expect("session admitted");
                Value::Bool(
                    self.calendar
                        .selected_occurrence_containing(session_id, instant)
                        .map_err(project_error)?
                        .is_some(),
                )
            }
            CalendarFeatureKind::SessionElapsedSeconds => selected_occurrence
                .as_ref()
                .map(|occurrence| {
                    Value::Integer((bar.open_time() - occurrence.start_utc).num_seconds())
                })
                .unwrap_or(Value::Missing(ScalarType::Integer)),
            CalendarFeatureKind::PreviousSessionHigh | CalendarFeatureKind::PreviousSessionLow => {
                let stats = self
                    .previous_session_stats(
                        &state,
                        self.selected_session_id
                            .as_deref()
                            .expect("session admitted"),
                        context.observed_through,
                        context.observed_through,
                    )
                    .map_err(project_error)?;
                stats
                    .map(|stats| {
                        if self.input.kind == CalendarFeatureKind::PreviousSessionHigh {
                            Value::Price(stats.high)
                        } else {
                            Value::Price(stats.low)
                        }
                    })
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            CalendarFeatureKind::PreviousDayHigh | CalendarFeatureKind::PreviousDayLow => {
                let stats = self
                    .previous_day_stats(&state, &day, context.observed_through)
                    .map_err(project_error)?;
                stats
                    .map(|stats| {
                        if self.input.kind == CalendarFeatureKind::PreviousDayHigh {
                            Value::Price(stats.high)
                        } else {
                            Value::Price(stats.low)
                        }
                    })
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            CalendarFeatureKind::PreviousWeekHigh | CalendarFeatureKind::PreviousWeekLow => {
                let stats = self
                    .previous_week_stats(&state, &day, context.observed_through)
                    .map_err(project_error)?;
                stats
                    .map(|stats| {
                        if self.input.kind == CalendarFeatureKind::PreviousWeekHigh {
                            Value::Price(stats.high)
                        } else {
                            Value::Price(stats.low)
                        }
                    })
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            CalendarFeatureKind::OpeningRangeHighSoFar
            | CalendarFeatureKind::OpeningRangeLowSoFar
            | CalendarFeatureKind::OpeningRangeHighFinal
            | CalendarFeatureKind::OpeningRangeLowFinal => {
                let opening_occurrence = self
                    .calendar
                    .latest_occurrence(
                        self.selected_session_id
                            .as_deref()
                            .expect("session admitted"),
                        context.observed_through,
                    )
                    .map_err(project_error)?;
                let opening = match opening_occurrence.as_ref() {
                    Some(occurrence) => configured_opening_range(
                        &self.calendar,
                        occurrence,
                        self.input.opening_range_minutes,
                        self.child_seconds,
                        self.input.alignment_offset_seconds,
                        state
                            .opening_bars
                            .get(&occurrence.id)
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                        context.observed_through,
                    )
                    .map_err(project_error)?,
                    None => None,
                };
                match self.input.kind {
                    CalendarFeatureKind::OpeningRangeHighSoFar => opening
                        .map(|range| Value::Price(range.high))
                        .unwrap_or(Value::Missing(ScalarType::Price)),
                    CalendarFeatureKind::OpeningRangeLowSoFar => opening
                        .map(|range| Value::Price(range.low))
                        .unwrap_or(Value::Missing(ScalarType::Price)),
                    CalendarFeatureKind::OpeningRangeHighFinal => opening
                        .filter(|range| range.final_value)
                        .map(|range| Value::Price(range.high))
                        .unwrap_or(Value::Missing(ScalarType::Price)),
                    CalendarFeatureKind::OpeningRangeLowFinal => opening
                        .filter(|range| range.final_value)
                        .map(|range| Value::Price(range.low))
                        .unwrap_or(Value::Missing(ScalarType::Price)),
                    _ => unreachable!(),
                }
            }
            CalendarFeatureKind::PastSameSlotRangeRatio
            | CalendarFeatureKind::PastSameSlotCount => {
                let Some(occurrence) = selected_occurrence.as_ref() else {
                    state.last_open = Some(bar.open_time());
                    state.retained = Some(Value::Missing(self.input.kind.scalar_type()));
                    return Ok(self.missing(true));
                };
                let elapsed = (bar.open_time() - occurrence.start_utc).num_seconds();
                let slot = u32::try_from(elapsed / self.child_seconds)
                    .map_err(|_| NamedInputProjectionError::new("calendar slot exceeds u32"))?;
                let key = (occurrence.id.session_id.clone(), slot);
                let prior = state.slot_ranges.get(&key).cloned().unwrap_or_default();
                let value = if self.input.kind == CalendarFeatureKind::PastSameSlotCount {
                    Value::Integer(i64::try_from(prior.len()).map_err(|_| {
                        NamedInputProjectionError::new("same-slot history exceeds i64")
                    })?)
                } else if prior.len() < self.input.maximum_history {
                    Value::Missing(ScalarType::Ratio)
                } else {
                    let mean = prior.iter().sum::<f64>() / prior.len() as f64;
                    if mean > 0.0 {
                        Value::Ratio((bar.high() - bar.low()) / mean)
                    } else {
                        Value::Missing(ScalarType::Ratio)
                    }
                };
                let history = state.slot_ranges.entry(key).or_default();
                history.push_back(bar.high() - bar.low());
                while history.len() > self.input.maximum_history {
                    history.pop_front();
                }
                value
            }
        };
        state.last_open = Some(bar.open_time());
        state.retained = Some(value.clone());
        Ok(ProjectedNamedInput {
            value,
            updated: true,
        })
    }
}

fn configured_opening_range(
    calendar: &ConfiguredTradingCalendar,
    occurrence: &ResolvedSessionOccurrence,
    opening_range_minutes: u32,
    child_seconds: i64,
    alignment_offset_seconds: i64,
    bars: &[CalendarBar],
    observed_through: NaiveDateTime,
) -> Result<Option<OpeningRange>, CalendarError> {
    let end = calendar.opening_range_end(occurrence, opening_range_minutes)?;
    let Some(expected) = calendar.expected_child_opens(
        occurrence.start_utc,
        end,
        child_seconds,
        alignment_offset_seconds,
    )?
    else {
        return Ok(None);
    };
    let mut revealed = bars
        .iter()
        .filter(|bar| bar.available_at <= observed_through)
        .collect::<Vec<_>>();
    revealed.sort_by_key(|bar| bar.open_utc);
    if revealed.is_empty() {
        return Ok(None);
    }
    let high = revealed
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let low = revealed
        .iter()
        .map(|bar| bar.low)
        .fold(f64::INFINITY, f64::min);
    let observed = revealed
        .iter()
        .map(|bar| bar.open_utc)
        .collect::<BTreeSet<_>>();
    let final_value = !expected.is_empty()
        && observed_through >= end
        && expected.len() == observed.len()
        && expected.iter().all(|open| observed.contains(open));
    Ok(Some(OpeningRange {
        high,
        low,
        complete_children: observed.len(),
        required_children: expected.len(),
        final_value,
    }))
}

fn estimated_projector_bytes(
    calendar: &ConfiguredTradingCalendar,
    input: &CalendarInputSpec,
    child_seconds: i64,
) -> Result<usize, CalendarError> {
    let maximum_slots = 176_400_i64
        .checked_add(child_seconds - 1)
        .and_then(|value| value.checked_div(child_seconds))
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| CalendarError::ResourceLimit("calendar slot bound overflowed".into()))?;
    let retained_periods = input
        .maximum_history
        .checked_add(2)
        .ok_or_else(|| CalendarError::ResourceLimit("calendar history bound overflowed".into()))?;
    let observed_bytes = retained_periods
        .checked_mul(maximum_slots)
        .and_then(|value| value.checked_mul(std::mem::size_of::<NaiveDateTime>() + 32))
        .and_then(|value| value.checked_mul(3));
    let slot_bytes = maximum_slots
        .checked_mul(input.maximum_history)
        .and_then(|value| value.checked_mul(std::mem::size_of::<f64>() + 8));
    let opening_bytes = retained_periods
        .checked_mul(maximum_slots)
        .and_then(|value| value.checked_mul(std::mem::size_of::<CalendarBar>() + 16));
    let temporary_bytes = maximum_slots
        .checked_mul(std::mem::size_of::<NaiveDateTime>() + 16)
        .and_then(|value| value.checked_mul(2));
    let identity_element = std::mem::size_of::<SessionOccurrenceId>()
        .checked_add(calendar.id.len() + MAX_CALENDAR_ID_BYTES + 32)
        .ok_or_else(|| {
            CalendarError::ResourceLimit("calendar identity byte bound overflowed".into())
        })?;
    let identity_bytes = retained_periods.checked_mul(identity_element);
    observed_bytes
        .and_then(|value| slot_bytes.and_then(|slots| value.checked_add(slots)))
        .and_then(|value| opening_bytes.and_then(|opening| value.checked_add(opening)))
        .and_then(|value| temporary_bytes.and_then(|temporary| value.checked_add(temporary)))
        .and_then(|value| identity_bytes.and_then(|identity| value.checked_add(identity)))
        .ok_or_else(|| CalendarError::ResourceLimit("calendar state bound overflowed".into()))
}

fn validate_id(kind: &str, value: &str) -> Result<(), CalendarError> {
    if value.is_empty()
        || value.len() > MAX_CALENDAR_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(CalendarError::InvalidConfiguration(format!(
            "{kind} ID must be a bounded ASCII identifier"
        )));
    }
    Ok(())
}

fn validate_span(id: &str, span: &SessionSpanSpec) -> Result<(), CalendarError> {
    if let SessionSpanSpec::Timed {
        start,
        end,
        end_day_offset,
    } = span
        && (*end_day_offset > 1 || (*end_day_offset == 0 && end <= start))
    {
        return Err(CalendarError::InvalidConfiguration(format!(
            "session '{id}' has a non-positive or unsupported timed span"
        )));
    }
    Ok(())
}

fn validate_market(
    market: &MarketScheduleSpec,
    limits: CalendarAdmissionLimits,
) -> Result<(), CalendarError> {
    if let MarketScheduleSpec::Weekly {
        intervals,
        exceptions,
    } = market
    {
        if intervals.len() > limits.max_market_intervals
            || exceptions.len() > limits.max_exceptions
            || exceptions
                .values()
                .any(|items| items.len() > limits.max_market_intervals)
        {
            return Err(CalendarError::ResourceLimit(
                "market schedule exceeds admission".into(),
            ));
        }
        for interval in intervals {
            if interval.weekday > 6
                || interval.end_day_offset > 1
                || (interval.end_day_offset == 0 && interval.end <= interval.start)
            {
                return Err(CalendarError::InvalidConfiguration(
                    "market interval is invalid".into(),
                ));
            }
        }
        for interval in exceptions.values().flatten() {
            if interval.end_day_offset > 1
                || (interval.end_day_offset == 0 && interval.end <= interval.start)
            {
                return Err(CalendarError::InvalidConfiguration(
                    "market exception interval is invalid".into(),
                ));
            }
        }
    }
    Ok(())
}

fn resolve_session_local(
    timezone: Tz,
    local: NaiveDateTime,
    open: bool,
) -> Result<NaiveDateTime, CalendarError> {
    let mut probe = local;
    for _ in 0..=10_800 {
        match timezone.from_local_datetime(&probe) {
            chrono::LocalResult::Single(value) => {
                return Ok(value.with_timezone(&Utc).naive_utc());
            }
            chrono::LocalResult::Ambiguous(first, second) => {
                let first = first.with_timezone(&Utc).naive_utc();
                let second = second.with_timezone(&Utc).naive_utc();
                return Ok(if open {
                    first.min(second)
                } else {
                    first.max(second)
                });
            }
            chrono::LocalResult::None => {
                probe = probe
                    .checked_add_signed(Duration::seconds(1))
                    .ok_or(CalendarError::TimestampOverflow)?;
            }
        }
    }
    Err(CalendarError::UnresolvedLocalTime)
}

fn resolve_boundary(timezone: Tz, local: NaiveDateTime) -> Result<NaiveDateTime, CalendarError> {
    let mut probe = local;
    for _ in 0..=10_800 {
        match timezone.from_local_datetime(&probe) {
            chrono::LocalResult::Single(value) => {
                return Ok(value.with_timezone(&Utc).naive_utc());
            }
            chrono::LocalResult::Ambiguous(first, second) => {
                return Ok(first
                    .with_timezone(&Utc)
                    .naive_utc()
                    .min(second.with_timezone(&Utc).naive_utc()));
            }
            chrono::LocalResult::None => {
                probe = probe
                    .checked_add_signed(Duration::seconds(1))
                    .ok_or(CalendarError::TimestampOverflow)?;
            }
        }
    }
    Err(CalendarError::UnresolvedLocalTime)
}

fn project_error(error: CalendarError) -> NamedInputProjectionError {
    NamedInputProjectionError::new(error.to_string())
}

fn midnight() -> NaiveTime {
    NaiveTime::MIN
}

fn default_opening_range_minutes() -> u32 {
    5
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    fn full_day(zone: &str) -> ConfiguredTradingCalendar {
        ConfiguredTradingCalendar::new(
            TradingCalendarSpec {
                id: "main".into(),
                timezone: zone.into(),
                day_boundary: NaiveTime::MIN,
                sessions: SessionScheduleSpec::FullDay,
                market: MarketScheduleSpec::Continuous,
            },
            CalendarAdmissionLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn omitted_full_day_is_one_session_and_tracks_dst_day_length() {
        let calendar = full_day("America/New_York");
        assert_eq!(calendar.session_ids().collect::<Vec<_>>(), ["full_day"]);
        let spring = calendar
            .resolve_trading_day(NaiveDate::from_ymd_opt(2026, 3, 8).unwrap())
            .unwrap();
        let fall = calendar
            .resolve_trading_day(NaiveDate::from_ymd_opt(2026, 11, 1).unwrap())
            .unwrap();
        assert_eq!(spring.end_utc - spring.start_utc, Duration::hours(23));
        assert_eq!(fall.end_utc - fall.start_utc, Duration::hours(25));
    }

    #[test]
    fn custom_sessions_replace_default_and_can_overlap_by_id() {
        let calendar = ConfiguredTradingCalendar::new(
            TradingCalendarSpec {
                id: "main".into(),
                timezone: "UTC".into(),
                day_boundary: NaiveTime::MIN,
                sessions: SessionScheduleSpec::Custom {
                    items: vec![
                        NamedSessionSpec {
                            id: "morning".into(),
                            timezone: None,
                            span: SessionSpanSpec::Timed {
                                start: time(9, 0),
                                end: time(12, 0),
                                end_day_offset: 0,
                            },
                            weekdays: BTreeSet::new(),
                        },
                        NamedSessionSpec {
                            id: "focus".into(),
                            timezone: None,
                            span: SessionSpanSpec::Timed {
                                start: time(11, 0),
                                end: time(14, 0),
                                end_day_offset: 0,
                            },
                            weekdays: BTreeSet::new(),
                        },
                    ],
                },
                market: MarketScheduleSpec::Continuous,
            },
            CalendarAdmissionLimits::default(),
        )
        .unwrap();
        assert_eq!(
            calendar.session_ids().collect::<Vec<_>>(),
            ["morning", "focus"]
        );
        let day = calendar
            .resolve_trading_day(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .unwrap();
        let occurrences = calendar.session_occurrences(&day).unwrap();
        assert_eq!(occurrences.len(), 2);
        assert!(occurrences[1].start_utc < occurrences[0].end_utc);
    }

    #[test]
    fn explicit_empty_custom_sessions_reject() {
        let error = ConfiguredTradingCalendar::new(
            TradingCalendarSpec {
                id: "main".into(),
                timezone: "UTC".into(),
                day_boundary: NaiveTime::MIN,
                sessions: SessionScheduleSpec::Custom { items: vec![] },
                market: MarketScheduleSpec::Unspecified,
            },
            CalendarAdmissionLimits::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot be empty"));
    }

    struct UnusedSeries;

    impl crate::strategy::HistoricalSeriesView for UnusedSeries {
        fn latest_bar(
            &self,
            _: &SeriesId,
        ) -> Result<Option<&crate::strategy::ClosedBar>, crate::strategy::SeriesViewError> {
            unreachable!()
        }

        fn bars(
            &self,
            _: &SeriesId,
            _: usize,
        ) -> Result<crate::strategy::BarWindow<'_>, crate::strategy::SeriesViewError> {
            unreachable!()
        }

        fn warmup(
            &self,
            _: &SeriesId,
        ) -> Result<crate::strategy::SeriesWarmupState, crate::strategy::SeriesViewError> {
            unreachable!()
        }
    }

    struct UnusedObservations;

    impl crate::strategy::HistoricalObservationView for UnusedObservations {
        fn observations(&self, _: usize) -> crate::strategy::ObservationWindow<'_> {
            unreachable!()
        }

        fn for_symbol<'a>(
            &'a self,
            _: &'a str,
            _: usize,
        ) -> crate::strategy::ObservationSelection<'a> {
            unreachable!()
        }

        fn latest_zone(
            &self,
            _: &crate::strategy::ZoneId,
        ) -> Option<&crate::strategy::StrategyObservation> {
            unreachable!()
        }

        fn omitted(&self) -> u64 {
            0
        }
    }

    fn project_bar(
        projector: &ConfiguredCalendarFeatureProjector,
        bar: &crate::strategy::ClosedBar,
    ) -> Value {
        projector
            .project(NamedInputProjectionContext {
                observed_through: bar.close_time(),
                closed_bars: std::slice::from_ref(bar),
                observations: &[],
                series: &UnusedSeries,
                observation_history: &UnusedObservations,
            })
            .unwrap()
            .value
    }

    #[test]
    fn previous_day_and_named_session_have_independent_aggregates() {
        let calendar = TradingCalendarSpec {
            id: "main".into(),
            timezone: "UTC".into(),
            day_boundary: NaiveTime::MIN,
            sessions: SessionScheduleSpec::Custom {
                items: vec![
                    NamedSessionSpec {
                        id: "morning".into(),
                        timezone: None,
                        span: SessionSpanSpec::Timed {
                            start: time(0, 0),
                            end: time(12, 0),
                            end_day_offset: 0,
                        },
                        weekdays: BTreeSet::new(),
                    },
                    NamedSessionSpec {
                        id: "afternoon".into(),
                        timezone: None,
                        span: SessionSpanSpec::Timed {
                            start: time(12, 0),
                            end: time(0, 0),
                            end_day_offset: 1,
                        },
                        weekdays: BTreeSet::new(),
                    },
                ],
            },
            market: MarketScheduleSpec::Continuous,
        };
        let make_projector = |kind, session_id| {
            ConfiguredCalendarFeatureProjector::new(
                SeriesId::new("primary").unwrap(),
                ConfiguredTradingCalendar::new(
                    calendar.clone(),
                    CalendarAdmissionLimits::default(),
                )
                .unwrap(),
                CalendarInputSpec {
                    calendar_id: "main".into(),
                    kind,
                    session_id,
                    time_basis: CalendarTimeBasis::SourceOpen,
                    opening_range_minutes: 5,
                    child_seconds: 3600,
                    alignment_offset_seconds: 0,
                    maximum_history: 2,
                },
            )
            .unwrap()
        };
        let day_projector = make_projector(CalendarFeatureKind::PreviousDayHigh, None);
        let session_projector = make_projector(
            CalendarFeatureKind::PreviousSessionHigh,
            Some("morning".into()),
        );
        let start = NaiveDate::from_ymd_opt(2026, 6, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let series = SeriesId::new("primary").unwrap();
        for hour in 0..37 {
            let open = start + Duration::hours(hour);
            let high = if hour < 12 {
                10.0
            } else if hour < 24 {
                100.0
            } else {
                20.0
            };
            let bar = crate::strategy::ClosedBar::for_test(
                series.clone(),
                "EURUSD",
                open,
                open + Duration::hours(1),
                high,
                1.0,
            );
            let day_value = project_bar(&day_projector, &bar);
            let session_value = project_bar(&session_projector, &bar);
            if hour == 36 {
                assert_eq!(day_value, Value::Price(100.0));
                assert_eq!(session_value, Value::Price(20.0));
            }
        }
    }

    #[test]
    fn boundary_gap_uses_first_existing_second() {
        let calendar = ConfiguredTradingCalendar::new(
            TradingCalendarSpec {
                id: "main".into(),
                timezone: "America/New_York".into(),
                day_boundary: NaiveTime::from_hms_opt(2, 30, 15).unwrap(),
                sessions: SessionScheduleSpec::FullDay,
                market: MarketScheduleSpec::Continuous,
            },
            CalendarAdmissionLimits::default(),
        )
        .unwrap();
        let day = calendar
            .resolve_trading_day(NaiveDate::from_ymd_opt(2026, 3, 8).unwrap())
            .unwrap();
        assert_eq!(
            day.start_utc.time(),
            NaiveTime::from_hms_opt(7, 0, 0).unwrap()
        );
    }
}
