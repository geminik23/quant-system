use std::collections::BTreeMap;

use chrono::{Duration, NaiveDateTime};

use crate::{DataError, Result, StoredTick};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PriorQuoteCarry {
    pub observed_at: NaiveDateTime,
    pub bid: f64,
    pub ask: f64,
    pub stale_limit: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PriceBins {
    pub lower: f64,
    pub width: f64,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuoteStatistics {
    pub accepted: u64,
    pub rejected: u64,
    pub duplicate_provider_rows: u64,
    pub coverage_millis: i64,
    pub spread_last: Option<f64>,
    pub spread_mean: Option<f64>,
    pub spread_max: Option<f64>,
    pub spread_p90: Option<f64>,
    pub spread_time_mean: Option<f64>,
    pub spread_bps_mean: Option<f64>,
    pub quote_activity: u64,
    pub quote_rate_per_second: Option<f64>,
    pub mid_change_count: u64,
    pub interarrival_mean_millis: Option<f64>,
    pub interarrival_max_millis: Option<i64>,
    pub interarrival_cv: Option<f64>,
    pub direction_changes: u64,
    pub longest_direction_streak: u64,
    pub path_length: f64,
    pub path_efficiency: Option<f64>,
    pub mid_change_variance: Option<f64>,
    pub first_high_at: Option<NaiveDateTime>,
    pub last_high_at: Option<NaiveDateTime>,
    pub first_low_at: Option<NaiveDateTime>,
    pub last_low_at: Option<NaiveDateTime>,
    pub twap: Option<f64>,
    pub crossings: u64,
    pub cumulative_above_millis: i64,
    pub continuous_above_millis: i64,
    pub elapsed_since_breakout_millis: Option<i64>,
    pub dominant_bin: Option<usize>,
    pub dominant_bin_center: Option<f64>,
    pub distance_from_dominant_center: Option<f64>,
}

#[derive(Clone)]
struct Quote {
    ts: NaiveDateTime,
    mid: f64,
    spread: f64,
}

type ProviderPayload = (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<i32>,
);
type QuoteInterval = (NaiveDateTime, NaiveDateTime, f64, f64);

#[allow(clippy::too_many_arguments)]
pub fn aggregate_quote_statistics(
    ticks: &[StoredTick],
    from: NaiveDateTime,
    to: NaiveDateTime,
    current_price: f64,
    captured_level: Option<f64>,
    bins: Option<PriceBins>,
    carry: Option<PriorQuoteCarry>,
) -> Result<QuoteStatistics> {
    if to <= from || !current_price.is_finite() {
        return Err(DataError::Other(
            "invalid quote-stat interval or current price".into(),
        ));
    }
    if let Some(bins) = bins
        && (!bins.lower.is_finite()
            || !bins.width.is_finite()
            || bins.width <= 0.0
            || bins.count == 0
            || bins.count > 4096)
    {
        return Err(DataError::Other("invalid price bins".into()));
    }

    let mut rows = ticks.to_vec();
    rows.sort_by_key(|row| (row.tick.ts, row.source_ordinal));
    let mut quotes = Vec::new();
    let mut rejected = 0_u64;
    let mut duplicate = 0_u64;
    let mut identities = BTreeMap::<(String, u64), ProviderPayload>::new();
    for row in rows {
        row.validate()?;
        if row.tick.ts < from || row.tick.ts >= to {
            continue;
        }
        if let (Some(source), Some(sequence)) = (row.source_identity.clone(), row.provider_sequence)
        {
            let payload = (
                row.tick.bid,
                row.tick.ask,
                row.tick.last,
                row.tick.volume,
                row.tick.flags,
            );
            if let Some(previous) = identities.get(&(source.clone(), sequence)) {
                if previous == &payload {
                    duplicate += 1;
                    continue;
                }
                return Err(DataError::Other(
                    "conflicting provider-sequence quote payload".into(),
                ));
            }
            identities.insert((source, sequence), payload);
        }
        let (Some(bid), Some(ask)) = (row.tick.bid, row.tick.ask) else {
            rejected += 1;
            continue;
        };
        if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 || ask < bid {
            rejected += 1;
            continue;
        }
        quotes.push(Quote {
            ts: row.tick.ts,
            mid: (bid + ask) / 2.0,
            spread: ask - bid,
        });
    }

    let intervals = quote_intervals(&quotes, from, to, carry)?;
    let duration_ms = (to - from).num_milliseconds();
    let coverage = intervals
        .iter()
        .map(|(start, end, _, _)| (*end - *start).num_milliseconds())
        .sum::<i64>();
    let weighted_mid = intervals
        .iter()
        .map(|(start, end, mid, _)| (*end - *start).num_milliseconds() as f64 * mid)
        .sum::<f64>();
    let weighted_spread = intervals
        .iter()
        .map(|(start, end, _, spread)| (*end - *start).num_milliseconds() as f64 * spread)
        .sum::<f64>();
    let spreads = quotes.iter().map(|quote| quote.spread).collect::<Vec<_>>();
    let mut sorted_spreads = spreads.clone();
    sorted_spreads.sort_by(f64::total_cmp);
    let spread_p90 = if sorted_spreads.is_empty() {
        None
    } else {
        let index = ((0.9 * sorted_spreads.len() as f64).ceil() as usize).saturating_sub(1);
        Some(sorted_spreads[index])
    };
    let mids = quotes.iter().map(|quote| quote.mid).collect::<Vec<_>>();
    let changes = mids
        .windows(2)
        .map(|window| window[1] - window[0])
        .collect::<Vec<_>>();
    let interarrival = quotes
        .windows(2)
        .map(|window| (window[1].ts - window[0].ts).num_milliseconds())
        .collect::<Vec<_>>();
    let interarrival_f64 = interarrival
        .iter()
        .map(|value| *value as f64)
        .collect::<Vec<_>>();
    let interarrival_mean = mean(&interarrival_f64);
    let interarrival_cv = interarrival_mean.and_then(|average| {
        if average == 0.0 {
            None
        } else {
            variance(&interarrival_f64, average).map(|value| value.sqrt() / average)
        }
    });
    let (direction_changes, longest_direction_streak) = direction_statistics(&changes);
    let path_length = changes.iter().map(|value| value.abs()).sum::<f64>();
    let path_efficiency = if path_length == 0.0 {
        Some(0.0)
    } else {
        mids.first()
            .zip(mids.last())
            .map(|(first, last)| (last - first).abs() / path_length)
    };
    let (first_high_at, last_high_at, first_low_at, last_low_at) = extrema_times(&quotes);
    let level = level_statistics(&quotes, &intervals, captured_level, to)?;
    let bin = bin_statistics(&intervals, bins, current_price);

    Ok(QuoteStatistics {
        accepted: u64::try_from(quotes.len()).unwrap_or(u64::MAX),
        rejected,
        duplicate_provider_rows: duplicate,
        coverage_millis: coverage,
        spread_last: quotes.last().map(|quote| quote.spread),
        spread_mean: mean(&spreads),
        spread_max: spreads.iter().copied().reduce(f64::max),
        spread_p90,
        spread_time_mean: (coverage > 0).then_some(weighted_spread / coverage as f64),
        spread_bps_mean: mean(
            &quotes
                .iter()
                .map(|quote| quote.spread / quote.mid * 10_000.0)
                .collect::<Vec<_>>(),
        ),
        quote_activity: u64::try_from(quotes.len()).unwrap_or(u64::MAX),
        quote_rate_per_second: (duration_ms > 0)
            .then_some(quotes.len() as f64 / (duration_ms as f64 / 1000.0)),
        mid_change_count: changes.iter().filter(|value| **value != 0.0).count() as u64,
        interarrival_mean_millis: interarrival_mean,
        interarrival_max_millis: interarrival.iter().copied().max(),
        interarrival_cv,
        direction_changes,
        longest_direction_streak,
        path_length,
        path_efficiency,
        mid_change_variance: mean(&changes).and_then(|average| variance(&changes, average)),
        first_high_at,
        last_high_at,
        first_low_at,
        last_low_at,
        twap: (coverage > 0).then_some(weighted_mid / coverage as f64),
        crossings: level.crossings,
        cumulative_above_millis: level.cumulative,
        continuous_above_millis: level.continuous,
        elapsed_since_breakout_millis: level.elapsed,
        dominant_bin: bin.index,
        dominant_bin_center: bin.center,
        distance_from_dominant_center: bin.distance,
    })
}

fn quote_intervals(
    quotes: &[Quote],
    from: NaiveDateTime,
    to: NaiveDateTime,
    carry: Option<PriorQuoteCarry>,
) -> Result<Vec<QuoteInterval>> {
    let mut intervals = Vec::new();
    if let Some(carry) = carry {
        if carry.stale_limit <= Duration::zero()
            || !carry.bid.is_finite()
            || !carry.ask.is_finite()
            || carry.bid <= 0.0
            || carry.ask < carry.bid
        {
            return Err(DataError::Other("invalid prior quote carry".into()));
        }
        let stale_end = carry
            .observed_at
            .checked_add_signed(carry.stale_limit)
            .ok_or_else(|| DataError::Other("carry stale timestamp overflow".into()))?;
        let end = quotes
            .first()
            .map(|quote| quote.ts)
            .unwrap_or(to)
            .min(to)
            .min(stale_end);
        if end > from && carry.observed_at <= from {
            intervals.push((
                from,
                end,
                (carry.bid + carry.ask) / 2.0,
                carry.ask - carry.bid,
            ));
        }
    }
    for (index, quote) in quotes.iter().enumerate() {
        let end = quotes
            .get(index + 1)
            .map(|next| next.ts)
            .unwrap_or(to)
            .min(to);
        if end > quote.ts {
            intervals.push((quote.ts.max(from), end, quote.mid, quote.spread));
        }
    }
    Ok(intervals)
}

#[derive(Default)]
struct LevelStatistics {
    crossings: u64,
    cumulative: i64,
    continuous: i64,
    elapsed: Option<i64>,
}

fn level_statistics(
    quotes: &[Quote],
    intervals: &[QuoteInterval],
    level: Option<f64>,
    to: NaiveDateTime,
) -> Result<LevelStatistics> {
    let Some(level) = level else {
        return Ok(LevelStatistics::default());
    };
    if !level.is_finite() {
        return Err(DataError::Other("captured level must be finite".into()));
    }
    let crossings = quotes
        .windows(2)
        .filter(|pair| {
            (pair[0].mid <= level && pair[1].mid > level)
                || (pair[0].mid >= level && pair[1].mid < level)
        })
        .count() as u64;
    let mut cumulative = 0_i64;
    let mut continuous = 0_i64;
    let mut breakout = None;
    let mut last_above_end = None;
    for (start, end, mid, _) in intervals {
        if *mid > level {
            let duration = (*end - *start).num_milliseconds();
            cumulative += duration;
            breakout.get_or_insert(*start);
            continuous = if last_above_end == Some(*start) {
                continuous + duration
            } else {
                duration
            };
            last_above_end = Some(*end);
        } else {
            continuous = 0;
            last_above_end = None;
        }
    }
    Ok(LevelStatistics {
        crossings,
        cumulative,
        continuous,
        elapsed: breakout.map(|timestamp| (to - timestamp).num_milliseconds()),
    })
}

#[derive(Default)]
struct BinStatistics {
    index: Option<usize>,
    center: Option<f64>,
    distance: Option<f64>,
}

fn bin_statistics(
    intervals: &[QuoteInterval],
    bins: Option<PriceBins>,
    current_price: f64,
) -> BinStatistics {
    let Some(bins) = bins else {
        return BinStatistics::default();
    };
    let mut dwell = vec![0_i64; bins.count];
    for (start, end, mid, _) in intervals {
        let relative = (*mid - bins.lower) / bins.width;
        if relative >= 0.0 {
            let index = relative.floor() as usize;
            if index < bins.count {
                dwell[index] += (*end - *start).num_milliseconds();
            }
        }
    }
    let maximum = dwell.iter().copied().max().unwrap_or(0);
    if maximum == 0 {
        return BinStatistics::default();
    }
    let index = dwell.iter().position(|value| *value == maximum).unwrap();
    let center = bins.lower + (index as f64 + 0.5) * bins.width;
    BinStatistics {
        index: Some(index),
        center: Some(center),
        distance: Some(current_price - center),
    }
}

fn direction_statistics(changes: &[f64]) -> (u64, u64) {
    let signs = changes
        .iter()
        .map(|value| value.total_cmp(&0.0))
        .filter(|sign| !sign.is_eq())
        .collect::<Vec<_>>();
    let changes = signs.windows(2).filter(|pair| pair[0] != pair[1]).count() as u64;
    let mut streak = 0_u64;
    let mut longest = 0_u64;
    let mut previous = None;
    for sign in signs {
        if previous == Some(sign) {
            streak += 1;
        } else {
            streak = 1;
            previous = Some(sign);
        }
        longest = longest.max(streak);
    }
    (changes, longest)
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn variance(values: &[f64], mean: f64) -> Option<f64> {
    (!values.is_empty()).then(|| {
        values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / values.len() as f64
    })
}

fn extrema_times(
    quotes: &[Quote],
) -> (
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
    Option<NaiveDateTime>,
) {
    let high = quotes.iter().map(|quote| quote.mid).reduce(f64::max);
    let low = quotes.iter().map(|quote| quote.mid).reduce(f64::min);
    let first_high =
        high.and_then(|value| quotes.iter().find(|quote| quote.mid == value).map(|q| q.ts));
    let last_high = high.and_then(|value| {
        quotes
            .iter()
            .rev()
            .find(|quote| quote.mid == value)
            .map(|q| q.ts)
    });
    let first_low =
        low.and_then(|value| quotes.iter().find(|quote| quote.mid == value).map(|q| q.ts));
    let last_low = low.and_then(|value| {
        quotes
            .iter()
            .rev()
            .find(|quote| quote.mid == value)
            .map(|q| q.ts)
    });
    (first_high, last_high, first_low, last_low)
}
