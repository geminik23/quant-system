//! Deterministic mark-to-market output collection.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use thiserror::Error;

use crate::portfolio::EquityPoint;

pub const DEFAULT_MTM_MAX_POINTS: usize = 4_096;
pub const MIN_MTM_MAX_POINTS: usize = 8;
pub const MAX_MTM_MAX_POINTS: usize = 16_384;

/// Controls how many exact mark-to-market observations are included in output artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MtmOutputPolicy {
    None,
    Bounded { max_points: usize },
    Full,
}

impl Default for MtmOutputPolicy {
    fn default() -> Self {
        Self::Bounded {
            max_points: DEFAULT_MTM_MAX_POINTS,
        }
    }
}

impl MtmOutputPolicy {
    pub fn validate(&self) -> Result<(), MtmOutputPolicyError> {
        if let Self::Bounded { max_points } = *self
            && !(MIN_MTM_MAX_POINTS..=MAX_MTM_MAX_POINTS).contains(&max_points)
        {
            return Err(MtmOutputPolicyError::InvalidMaxPoints { max_points });
        }
        Ok(())
    }

    pub fn max_points(self) -> Option<usize> {
        match self {
            Self::None => Some(0),
            Self::Bounded { max_points } => Some(max_points),
            Self::Full => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MtmOutputPolicyRepr {
    None,
    Bounded { max_points: usize },
    Full,
}

impl From<MtmOutputPolicyRepr> for MtmOutputPolicy {
    fn from(value: MtmOutputPolicyRepr) -> Self {
        match value {
            MtmOutputPolicyRepr::None => Self::None,
            MtmOutputPolicyRepr::Bounded { max_points } => Self::Bounded { max_points },
            MtmOutputPolicyRepr::Full => Self::Full,
        }
    }
}

impl<'de> Deserialize<'de> for MtmOutputPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let policy = Self::from(MtmOutputPolicyRepr::deserialize(deserializer)?);
        policy.validate().map_err(D::Error::custom)?;
        Ok(policy)
    }
}

/// Validation failure for bounded mark-to-market output configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MtmOutputPolicyError {
    #[error(
        "MTM max_points must be between {MIN_MTM_MAX_POINTS} and {MAX_MTM_MAX_POINTS}, got {max_points}"
    )]
    InvalidMaxPoints { max_points: usize },
}

/// Counts describing the relationship between exact observations and emitted points.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MtmOutputSummary {
    pub policy: MtmOutputPolicy,
    pub observed_points: u64,
    pub retained_points: u64,
    pub omitted_points: u64,
}

/// Streaming collector that bounds output while retaining significant observations.
#[derive(Debug, Clone)]
pub struct MtmCurveCollector {
    policy: MtmOutputPolicy,
    observed_points: u64,
    points: BTreeMap<u64, EquityPoint>,
    eviction_index: BTreeSet<(u64, u64)>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    min_equity: Option<(f64, u64)>,
    max_equity: Option<(f64, u64)>,
    max_drawdown: Option<(f64, u64)>,
}

impl Default for MtmCurveCollector {
    fn default() -> Self {
        Self::new(MtmOutputPolicy::default()).expect("default MTM output policy is valid")
    }
}

impl MtmCurveCollector {
    pub fn new(policy: MtmOutputPolicy) -> Result<Self, MtmOutputPolicyError> {
        policy.validate()?;
        Ok(Self {
            policy,
            observed_points: 0,
            points: BTreeMap::new(),
            eviction_index: BTreeSet::new(),
            first_sequence: None,
            last_sequence: None,
            min_equity: None,
            max_equity: None,
            max_drawdown: None,
        })
    }

    pub fn policy(&self) -> MtmOutputPolicy {
        self.policy
    }

    pub fn observe(&mut self, point: EquityPoint) -> u64 {
        self.push(point)
    }

    pub fn push(&mut self, mut point: EquityPoint) -> u64 {
        let sequence = self.observed_points;
        self.observed_points = self.observed_points.saturating_add(1);
        if point.observation_sequence.is_none() {
            point.observation_sequence = Some(sequence);
        }

        self.update_pins(sequence, &point);
        if !matches!(self.policy, MtmOutputPolicy::None) {
            self.points.insert(sequence, point);
            if matches!(self.policy, MtmOutputPolicy::Bounded { .. }) {
                self.eviction_index
                    .insert((sample_priority(sequence), sequence));
            }
            self.enforce_bound();
        }
        sequence
    }

    pub fn extend(&mut self, points: impl IntoIterator<Item = EquityPoint>) {
        for point in points {
            self.push(point);
        }
    }

    pub fn summary(&self) -> MtmOutputSummary {
        let retained_points = self.points.len() as u64;
        MtmOutputSummary {
            policy: self.policy,
            observed_points: self.observed_points,
            retained_points,
            omitted_points: self.observed_points.saturating_sub(retained_points),
        }
    }

    pub fn retained_points(&self) -> Vec<EquityPoint> {
        self.points.values().cloned().collect()
    }

    pub fn into_curve(self) -> Vec<EquityPoint> {
        self.points.into_values().collect()
    }

    pub fn into_parts(self) -> (Vec<EquityPoint>, MtmOutputSummary) {
        let summary = self.summary();
        (self.into_curve(), summary)
    }

    fn update_pins(&mut self, sequence: u64, point: &EquityPoint) {
        self.first_sequence.get_or_insert(sequence);
        self.last_sequence = Some(sequence);

        if let Some(equity) = point.equity.filter(|value| value.is_finite()) {
            if self.min_equity.is_none_or(|(minimum, _)| equity < minimum) {
                self.min_equity = Some((equity, sequence));
            }
            if self.max_equity.is_none_or(|(maximum, _)| equity > maximum) {
                self.max_equity = Some((equity, sequence));
            }
        }

        let drawdown = point
            .drawdown
            .filter(|value| value.is_finite())
            .or_else(|| point.max_drawdown.filter(|value| value.is_finite()));
        if let Some(drawdown) = drawdown
            && self
                .max_drawdown
                .is_none_or(|(maximum, _)| drawdown > maximum)
        {
            self.max_drawdown = Some((drawdown, sequence));
        }
    }

    fn enforce_bound(&mut self) {
        let MtmOutputPolicy::Bounded { max_points } = self.policy else {
            return;
        };
        while self.points.len() > max_points {
            let (_, evicted) = self
                .eviction_index
                .iter()
                .rev()
                .copied()
                .find(|(_, sequence)| !self.is_pinned(*sequence))
                .expect("a valid bounded policy always leaves an unpinned point");
            self.points.remove(&evicted);
            self.eviction_index
                .remove(&(sample_priority(evicted), evicted));
        }
    }

    fn is_pinned(&self, sequence: u64) -> bool {
        [
            self.first_sequence,
            self.last_sequence,
            self.min_equity.map(|(_, sequence)| sequence),
            self.max_equity.map(|(_, sequence)| sequence),
            self.max_drawdown.map(|(_, sequence)| sequence),
        ]
        .into_iter()
        .flatten()
        .any(|pinned| pinned == sequence)
    }
}

fn sample_priority(sequence: u64) -> u64 {
    let mut value = sequence.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};

    fn point(sequence: u64, equity: f64, drawdown: f64) -> EquityPoint {
        EquityPoint {
            ts: NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                + Duration::seconds(sequence as i64),
            equity: Some(equity),
            drawdown: Some(drawdown),
            ..EquityPoint::default()
        }
    }

    #[test]
    fn policy_defaults_and_validates_bounded_limits() {
        assert_eq!(
            MtmOutputPolicy::default(),
            MtmOutputPolicy::Bounded {
                max_points: DEFAULT_MTM_MAX_POINTS
            }
        );
        assert!(
            MtmOutputPolicy::Bounded {
                max_points: MIN_MTM_MAX_POINTS
            }
            .validate()
            .is_ok()
        );
        assert!(
            MtmOutputPolicy::Bounded {
                max_points: MAX_MTM_MAX_POINTS
            }
            .validate()
            .is_ok()
        );
        assert!(matches!(
            MtmOutputPolicy::Bounded { max_points: 7 }.validate(),
            Err(MtmOutputPolicyError::InvalidMaxPoints { max_points: 7 })
        ));
        assert!(
            serde_json::from_str::<MtmOutputPolicy>(r#"{"bounded":{"max_points":16385}}"#).is_err()
        );
    }

    #[test]
    fn none_and_full_policies_report_exact_counts() {
        let mut none = MtmCurveCollector::new(MtmOutputPolicy::None).unwrap();
        let mut full = MtmCurveCollector::new(MtmOutputPolicy::Full).unwrap();
        for sequence in 0..3 {
            let point = point(sequence, 100.0 + sequence as f64, 0.0);
            none.push(point.clone());
            full.push(point);
        }

        let (none_curve, none_summary) = none.into_parts();
        assert!(none_curve.is_empty());
        assert_eq!(none_summary.observed_points, 3);
        assert_eq!(none_summary.retained_points, 0);
        assert_eq!(none_summary.omitted_points, 3);

        let (full_curve, full_summary) = full.into_parts();
        assert_eq!(full_curve.len(), 3);
        assert_eq!(full_summary.retained_points, 3);
        assert_eq!(full_summary.omitted_points, 0);
        assert_eq!(full_curve[2].observation_sequence, Some(2));
    }

    #[test]
    fn bounded_output_is_deterministic_and_pins_significant_points() {
        let policy = MtmOutputPolicy::Bounded { max_points: 8 };
        let mut first = MtmCurveCollector::new(policy).unwrap();
        let mut second = MtmCurveCollector::new(policy).unwrap();

        for sequence in 0..100 {
            let mut equity = 100.0 + (sequence % 7) as f64;
            let mut drawdown = 0.0;
            if sequence == 10 {
                equity = 1_000.0;
            } else if sequence == 20 {
                equity = -1_000.0;
            } else if sequence == 30 {
                drawdown = 500.0;
            }
            let point = point(sequence, equity, drawdown);
            first.push(point.clone());
            second.push(point);
        }

        assert_eq!(first.eviction_index.len(), 8);
        assert_eq!(second.eviction_index.len(), 8);
        let (first_curve, summary) = first.into_parts();
        let second_curve = second.into_curve();
        assert_eq!(first_curve, second_curve);
        assert_eq!(first_curve.len(), 8);
        assert_eq!(summary.observed_points, 100);
        assert_eq!(summary.retained_points, 8);
        assert_eq!(summary.omitted_points, 92);

        let sequences: Vec<_> = first_curve
            .iter()
            .filter_map(|point| point.observation_sequence)
            .collect();
        assert_eq!(sequences, vec![0, 10, 20, 21, 30, 48, 68, 99]);
        for pinned in [0, 10, 20, 30, 99] {
            assert!(sequences.contains(&pinned), "missing pinned point {pinned}");
        }
        assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
