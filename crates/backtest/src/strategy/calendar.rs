use chrono::{
    Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
    Weekday,
};
use chrono_tz::Tz;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use qs_strategy::{ScalarType, Value, ValueType};

use super::{
    HistoricalNamedInputProjector, NamedInputProjectionContext, NamedInputProjectionError,
    ProjectedNamedInput, SeriesId,
};

mod configured;

pub use configured::{
    CalendarAdmissionLimits, CalendarInputSpec, CalendarTimeBasis,
    ConfiguredCalendarFeatureProjector, ConfiguredCalendarInput, ConfiguredTradingCalendar,
    DEFAULT_CALENDAR_SESSION_ID, LocalMarketIntervalSpec, MarketScheduleSpec, NamedSessionSpec,
    ResolvedSessionOccurrence, ResolvedTradingDay, SessionOccurrenceId, SessionScheduleSpec,
    SessionSpanSpec, TradingCalendarSpec, WeeklyMarketIntervalSpec,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSession {
    pub trading_day: NaiveDate,
    pub trading_week_monday: NaiveDate,
    pub open_utc: NaiveDateTime,
    pub close_utc: NaiveDateTime,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CalendarBar {
    pub open_utc: NaiveDateTime,
    pub close_utc: NaiveDateTime,
    pub available_at: NaiveDateTime,
    pub high: f64,
    pub low: f64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct OpeningRange {
    pub high: f64,
    pub low: f64,
    pub complete_children: usize,
    pub required_children: usize,
    pub final_value: bool,
}
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum CalendarError {
    #[error("invalid IANA timezone '{0}'")]
    InvalidTimezone(String),
    #[error("session interval is not positive")]
    NonPositiveSession,
    #[error("local time could not be resolved within three hours")]
    UnresolvedLocalTime,
    #[error("child duration and alignment are invalid for opening range")]
    InvalidChildGeometry,
    #[error("opening-range timestamp overflowed")]
    TimestampOverflow,
    #[error("opening-range bars overlap, mismatch, or exceed bounds")]
    InvalidChildren,
    #[error("invalid calendar configuration: {0}")]
    InvalidConfiguration(String),
    #[error("calendar resource limit exceeded: {0}")]
    ResourceLimit(String),
}
#[derive(Debug, Clone)]
pub struct IanaTradingCalendar {
    timezone: Tz,
    open: NaiveTime,
    close: NaiveTime,
    holidays: BTreeSet<NaiveDate>,
    early_closes: BTreeMap<NaiveDate, NaiveTime>,
}
impl IanaTradingCalendar {
    pub fn new(
        timezone: &str,
        open: NaiveTime,
        close: NaiveTime,
        holidays: BTreeSet<NaiveDate>,
        early_closes: BTreeMap<NaiveDate, NaiveTime>,
    ) -> Result<Self, CalendarError> {
        let timezone = timezone
            .parse()
            .map_err(|_| CalendarError::InvalidTimezone(timezone.into()))?;
        Ok(Self {
            timezone,
            open,
            close,
            holidays,
            early_closes,
        })
    }
    pub fn timezone(&self) -> Tz {
        self.timezone
    }
    pub fn resolve_session(
        &self,
        trading_day: NaiveDate,
    ) -> Result<Option<ResolvedSession>, CalendarError> {
        if self.holidays.contains(&trading_day) {
            return Ok(None);
        }
        let open_local = trading_day.and_time(self.open);
        let close_time = self
            .early_closes
            .get(&trading_day)
            .copied()
            .unwrap_or(self.close);
        let close_date = if close_time <= self.open {
            trading_day
                .succ_opt()
                .ok_or(CalendarError::TimestampOverflow)?
        } else {
            trading_day
        };
        let close_local = close_date.and_time(close_time);
        let open_utc = resolve_local(self.timezone, open_local, true)?;
        let close_utc = resolve_local(self.timezone, close_local, false)?;
        if close_utc <= open_utc {
            return Err(CalendarError::NonPositiveSession);
        }
        let monday = trading_day
            .checked_sub_days(chrono::Days::new(
                trading_day.weekday().num_days_from_monday() as u64,
            ))
            .ok_or(CalendarError::TimestampOverflow)?;
        Ok(Some(ResolvedSession {
            trading_day,
            trading_week_monday: monday,
            open_utc,
            close_utc,
        }))
    }
    pub fn session_containing(
        &self,
        utc: NaiveDateTime,
    ) -> Result<Option<ResolvedSession>, CalendarError> {
        let local_date = Utc
            .from_utc_datetime(&utc)
            .with_timezone(&self.timezone)
            .date_naive();
        for trading_day in [local_date.pred_opt(), Some(local_date)]
            .into_iter()
            .flatten()
        {
            if let Some(session) = self.resolve_session(trading_day)?
                && utc >= session.open_utc
                && utc < session.close_utc
            {
                return Ok(Some(session));
            }
        }
        Ok(None)
    }

    pub fn local_fields(&self, utc: NaiveDateTime) -> (NaiveDate, Weekday, u32) {
        let local = Utc.from_utc_datetime(&utc).with_timezone(&self.timezone);
        (
            local.date_naive(),
            local.weekday(),
            local.time().num_seconds_from_midnight(),
        )
    }
    pub fn opening_range_interval(
        &self,
        trading_day: NaiveDate,
        minutes: u32,
    ) -> Result<Option<(NaiveDateTime, NaiveDateTime)>, CalendarError> {
        let Some(session) = self.resolve_session(trading_day)? else {
            return Ok(None);
        };
        let local_start = trading_day.and_time(self.open);
        let local_end = local_start
            .checked_add_signed(Duration::minutes(i64::from(minutes)))
            .ok_or(CalendarError::TimestampOverflow)?;
        let end = resolve_local(self.timezone, local_end, false)?.min(session.close_utc);
        if end <= session.open_utc {
            return Err(CalendarError::NonPositiveSession);
        }
        Ok(Some((session.open_utc, end)))
    }
    pub fn opening_range(
        &self,
        trading_day: NaiveDate,
        minutes: u32,
        child_seconds: u64,
        alignment_offset_seconds: i64,
        bars: &[CalendarBar],
        observed_through: NaiveDateTime,
    ) -> Result<Option<OpeningRange>, CalendarError> {
        let Some((start, end)) = self.opening_range_interval(trading_day, minutes)? else {
            return Ok(None);
        };
        let child = i64::try_from(child_seconds)
            .ok()
            .filter(|v| *v > 0)
            .ok_or(CalendarError::InvalidChildGeometry)?;
        if (start.and_utc().timestamp() - alignment_offset_seconds).rem_euclid(child) != 0
            || (end - start).num_seconds() % child != 0
        {
            return Err(CalendarError::InvalidChildGeometry);
        }
        let required = usize::try_from((end - start).num_seconds() / child)
            .map_err(|_| CalendarError::InvalidChildGeometry)?;
        let mut selected = bars
            .iter()
            .filter(|bar| bar.open_utc >= start && bar.open_utc < end)
            .collect::<Vec<_>>();
        selected.sort_by_key(|bar| bar.open_utc);
        for (index, bar) in selected.iter().enumerate() {
            let expected = start
                .checked_add_signed(Duration::seconds(child * i64::try_from(index).unwrap()))
                .ok_or(CalendarError::TimestampOverflow)?;
            if bar.open_utc != expected
                || bar.close_utc != expected + Duration::seconds(child)
                || bar.available_at < bar.close_utc
                || !bar.high.is_finite()
                || !bar.low.is_finite()
                || bar.high < bar.low
            {
                return Err(CalendarError::InvalidChildren);
            }
        }
        let revealed = selected
            .iter()
            .filter(|bar| bar.available_at <= observed_through)
            .copied()
            .collect::<Vec<_>>();
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
        let final_value = selected.len() == required && revealed.len() == required;
        Ok(Some(OpeningRange {
            high,
            low,
            complete_children: revealed.len(),
            required_children: required,
            final_value,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalendarFeatureKind {
    LocalSecondOfDay,
    SessionElapsedSeconds,
    PreviousSessionHigh,
    PreviousSessionLow,
    PreviousDayHigh,
    PreviousDayLow,
    PreviousWeekHigh,
    PreviousWeekLow,
    OpeningRangeHighSoFar,
    OpeningRangeLowSoFar,
    OpeningRangeHighFinal,
    OpeningRangeLowFinal,
    PastSameSlotRangeRatio,
    PastSameSlotCount,
    SessionMembership,
}

impl CalendarFeatureKind {
    fn scalar_type(self) -> ScalarType {
        match self {
            Self::LocalSecondOfDay | Self::SessionElapsedSeconds | Self::PastSameSlotCount => {
                ScalarType::Integer
            }
            Self::PastSameSlotRangeRatio => ScalarType::Ratio,
            Self::SessionMembership => ScalarType::Bool,
            _ => ScalarType::Price,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct CalendarPeriodStats {
    high: f64,
    low: f64,
    initialized: bool,
}

impl CalendarPeriodStats {
    fn observe(&mut self, high: f64, low: f64) {
        if self.initialized {
            self.high = self.high.max(high);
            self.low = self.low.min(low);
        } else {
            self.high = high;
            self.low = low;
            self.initialized = true;
        }
    }
}

#[derive(Default)]
struct CalendarProjectorState {
    last_open: Option<NaiveDateTime>,
    retained: Option<Value>,
    sessions: BTreeMap<NaiveDate, CalendarPeriodStats>,
    weeks: BTreeMap<NaiveDate, CalendarPeriodStats>,
    opening_bars: BTreeMap<NaiveDate, Vec<CalendarBar>>,
    slot_ranges: BTreeMap<u32, Vec<f64>>,
}

/// Projects caller-calendar features from completed bars at their actual reveal boundary.
pub struct CalendarFeatureProjector {
    series_id: SeriesId,
    calendar: IanaTradingCalendar,
    kind: CalendarFeatureKind,
    opening_range_minutes: u32,
    child_seconds: u64,
    alignment_offset_seconds: i64,
    maximum_slot_history: usize,
    state: RefCell<CalendarProjectorState>,
}

impl CalendarFeatureProjector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        series_id: SeriesId,
        calendar: IanaTradingCalendar,
        kind: CalendarFeatureKind,
        opening_range_minutes: u32,
        child_seconds: u64,
        alignment_offset_seconds: i64,
        maximum_slot_history: usize,
    ) -> Result<Self, CalendarError> {
        if opening_range_minutes == 0
            || child_seconds < 60
            || i64::try_from(child_seconds).is_err()
            || maximum_slot_history == 0
        {
            return Err(CalendarError::InvalidChildGeometry);
        }
        Ok(Self {
            series_id,
            calendar,
            kind,
            opening_range_minutes,
            child_seconds,
            alignment_offset_seconds,
            maximum_slot_history,
            state: RefCell::new(CalendarProjectorState::default()),
        })
    }

    fn missing(&self, updated: bool) -> ProjectedNamedInput {
        ProjectedNamedInput {
            value: Value::Missing(self.kind.scalar_type()),
            updated,
        }
    }
}

impl HistoricalNamedInputProjector for CalendarFeatureProjector {
    fn output_type(&self) -> ValueType {
        ValueType::optional(self.kind.scalar_type())
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
                    .unwrap_or(Value::Missing(self.kind.scalar_type())),
                updated: false,
            });
        }
        let Some(session) = self
            .calendar
            .session_containing(bar.open_time())
            .map_err(|error| NamedInputProjectionError::new(error.to_string()))?
        else {
            state.last_open = Some(bar.open_time());
            state.retained = Some(Value::Missing(self.kind.scalar_type()));
            return Ok(self.missing(true));
        };
        let previous_session = state
            .sessions
            .range(..session.trading_day)
            .next_back()
            .map(|(_, stats)| *stats);
        let previous_week = state
            .weeks
            .range(..session.trading_week_monday)
            .next_back()
            .map(|(_, stats)| *stats);
        let elapsed = (bar.open_time() - session.open_utc).num_seconds();
        if elapsed < 0 {
            return Err(NamedInputProjectionError::new(
                "calendar bar precedes its resolved session",
            ));
        }
        let child_seconds = i64::try_from(self.child_seconds)
            .map_err(|_| NamedInputProjectionError::new("calendar child duration exceeds i64"))?;
        let slot = u32::try_from(elapsed / child_seconds)
            .map_err(|_| NamedInputProjectionError::new("calendar slot exceeds u32"))?;
        let prior_slot = state.slot_ranges.get(&slot).cloned().unwrap_or_default();
        let current_range = bar.high() - bar.low();

        state
            .sessions
            .entry(session.trading_day)
            .or_default()
            .observe(bar.high(), bar.low());
        state
            .weeks
            .entry(session.trading_week_monday)
            .or_default()
            .observe(bar.high(), bar.low());
        while state.sessions.len() > self.maximum_slot_history {
            if let Some(oldest) = state.sessions.keys().next().copied() {
                state.sessions.remove(&oldest);
                state.opening_bars.remove(&oldest);
            }
        }
        while state.weeks.len() > self.maximum_slot_history {
            if let Some(oldest) = state.weeks.keys().next().copied() {
                state.weeks.remove(&oldest);
            }
        }
        if let Some((opening_start, opening_end)) = self
            .calendar
            .opening_range_interval(session.trading_day, self.opening_range_minutes)
            .map_err(|error| NamedInputProjectionError::new(error.to_string()))?
            && bar.open_time() >= opening_start
            && bar.open_time() < opening_end
        {
            state
                .opening_bars
                .entry(session.trading_day)
                .or_default()
                .push(CalendarBar {
                    open_utc: bar.open_time(),
                    close_utc: bar.close_time(),
                    available_at: context.observed_through,
                    high: bar.high(),
                    low: bar.low(),
                });
        }
        let opening_bars = state
            .opening_bars
            .get(&session.trading_day)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let opening = self
            .calendar
            .opening_range(
                session.trading_day,
                self.opening_range_minutes,
                self.child_seconds,
                self.alignment_offset_seconds,
                opening_bars,
                context.observed_through,
            )
            .map_err(|error| NamedInputProjectionError::new(error.to_string()))?;
        let value = match self.kind {
            CalendarFeatureKind::LocalSecondOfDay => {
                Value::Integer(i64::from(self.calendar.local_fields(bar.open_time()).2))
            }
            CalendarFeatureKind::SessionElapsedSeconds => Value::Integer(elapsed),
            CalendarFeatureKind::PreviousSessionHigh | CalendarFeatureKind::PreviousDayHigh => {
                previous_session
                    .map(|stats| Value::Price(stats.high))
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            CalendarFeatureKind::PreviousSessionLow | CalendarFeatureKind::PreviousDayLow => {
                previous_session
                    .map(|stats| Value::Price(stats.low))
                    .unwrap_or(Value::Missing(ScalarType::Price))
            }
            CalendarFeatureKind::PreviousWeekHigh => previous_week
                .map(|stats| Value::Price(stats.high))
                .unwrap_or(Value::Missing(ScalarType::Price)),
            CalendarFeatureKind::PreviousWeekLow => previous_week
                .map(|stats| Value::Price(stats.low))
                .unwrap_or(Value::Missing(ScalarType::Price)),
            CalendarFeatureKind::OpeningRangeHighSoFar => opening
                .as_ref()
                .map(|range| Value::Price(range.high))
                .unwrap_or(Value::Missing(ScalarType::Price)),
            CalendarFeatureKind::OpeningRangeLowSoFar => opening
                .as_ref()
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
            CalendarFeatureKind::PastSameSlotRangeRatio => {
                if prior_slot.is_empty() {
                    Value::Missing(ScalarType::Ratio)
                } else {
                    let mean = prior_slot.iter().sum::<f64>() / prior_slot.len() as f64;
                    if mean > 0.0 {
                        Value::Ratio(current_range / mean)
                    } else {
                        Value::Missing(ScalarType::Ratio)
                    }
                }
            }
            CalendarFeatureKind::PastSameSlotCount => Value::Integer(
                i64::try_from(prior_slot.len())
                    .map_err(|_| NamedInputProjectionError::new("same-slot history exceeds i64"))?,
            ),
            CalendarFeatureKind::SessionMembership => Value::Bool(true),
        };
        let history = state.slot_ranges.entry(slot).or_default();
        history.push(current_range);
        if history.len() > self.maximum_slot_history {
            history.remove(0);
        }
        state.last_open = Some(bar.open_time());
        state.retained = Some(value.clone());
        Ok(ProjectedNamedInput {
            value,
            updated: true,
        })
    }
}

fn resolve_local(
    timezone: Tz,
    mut local: NaiveDateTime,
    open: bool,
) -> Result<NaiveDateTime, CalendarError> {
    for _ in 0..=180 {
        match timezone.from_local_datetime(&local) {
            LocalResult::Single(value) => return Ok(value.with_timezone(&Utc).naive_utc()),
            LocalResult::Ambiguous(first, second) => {
                let (a, b) = (
                    first.with_timezone(&Utc).naive_utc(),
                    second.with_timezone(&Utc).naive_utc(),
                );
                return Ok(if open { a.min(b) } else { a.max(b) });
            }
            LocalResult::None => {
                local = local
                    .checked_add_signed(Duration::minutes(1))
                    .ok_or(CalendarError::TimestampOverflow)?
            }
        }
    }
    Err(CalendarError::UnresolvedLocalTime)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn time(h: u32, m: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(h, m, 0).unwrap()
    }
    #[test]
    fn dst_gap_shifts_forward_and_fold_uses_earliest_open_latest_close() {
        let gap = IanaTradingCalendar::new(
            "America/New_York",
            time(2, 30),
            time(4, 0),
            BTreeSet::new(),
            BTreeMap::new(),
        )
        .unwrap()
        .resolve_session(NaiveDate::from_ymd_opt(2026, 3, 8).unwrap())
        .unwrap()
        .unwrap();
        assert_eq!(
            gap.open_utc,
            NaiveDate::from_ymd_opt(2026, 3, 8)
                .unwrap()
                .and_hms_opt(7, 0, 0)
                .unwrap()
        );
        let fold = IanaTradingCalendar::new(
            "America/New_York",
            time(1, 30),
            time(1, 45),
            BTreeSet::new(),
            BTreeMap::new(),
        )
        .unwrap()
        .resolve_session(NaiveDate::from_ymd_opt(2026, 11, 1).unwrap())
        .unwrap()
        .unwrap();
        assert_eq!(fold.close_utc - fold.open_utc, Duration::minutes(75));
    }
    #[test]
    fn opening_range_final_waits_for_every_actual_child_reveal() {
        let calendar = IanaTradingCalendar::new(
            "America/New_York",
            time(9, 30),
            time(16, 0),
            BTreeSet::new(),
            BTreeMap::new(),
        )
        .unwrap();
        let day = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let (start, end) = calendar.opening_range_interval(day, 10).unwrap().unwrap();
        let bars = [
            CalendarBar {
                open_utc: start,
                close_utc: start + Duration::minutes(5),
                available_at: start + Duration::minutes(5),
                high: 2.0,
                low: 1.0,
            },
            CalendarBar {
                open_utc: start + Duration::minutes(5),
                close_utc: end,
                available_at: end + Duration::minutes(7),
                high: 3.0,
                low: 0.5,
            },
        ];
        let early = calendar
            .opening_range(day, 10, 300, 0, &bars, end)
            .unwrap()
            .unwrap();
        assert!(!early.final_value);
        assert_eq!(early.high, 2.0);
        let final_value = calendar
            .opening_range(day, 10, 300, 0, &bars, end + Duration::minutes(7))
            .unwrap()
            .unwrap();
        assert!(final_value.final_value);
        assert_eq!((final_value.high, final_value.low), (3.0, 0.5));
    }
}
