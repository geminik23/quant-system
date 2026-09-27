use chrono::{Duration, NaiveDateTime};

use crate::error::ResearchError;

/// Compatibility bound used by callers that do not provide an explicit research admission policy.
pub const DEFAULT_MAX_WINDOW_PAIRS: usize = 1_000_000;

/// One labelled evaluation period.
///
/// The period is half-open: a position may open at or after `from`, and events at `to` belong to the following window. The label is what appears in a result row, so it should be readable rather than derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataWindow {
    label: String,
    from: NaiveDateTime,
    to: NaiveDateTime,
}

impl DataWindow {
    pub fn new(
        label: impl Into<String>,
        from: NaiveDateTime,
        to: NaiveDateTime,
    ) -> Result<Self, ResearchError> {
        let label = label.into();
        if label.trim().is_empty() {
            return Err(ResearchError::InvalidWindow(
                "window label must not be empty".into(),
            ));
        }
        if to <= from {
            return Err(ResearchError::InvalidWindow(format!(
                "window '{label}' must end after it starts, got {from} to {to}"
            )));
        }
        Ok(Self { label, from, to })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn from(&self) -> NaiveDateTime {
        self.from
    }

    pub fn to(&self) -> NaiveDateTime {
        self.to
    }
}

/// An in-sample window paired with the out-of-sample window that follows it.
///
/// The pair is the unit of validation: a configuration is fitted or inspected on the first and judged on the second. Keeping them together means a table can never show one without the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowPair {
    pub in_sample: DataWindow,
    pub out_of_sample: DataWindow,
}

/// How evaluation periods are laid out over the available data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowPlan {
    /// One fixed in-sample and out-of-sample split.
    Fixed {
        in_sample: DataWindow,
        out_of_sample: DataWindow,
    },
    /// Repeated train-then-test splits rolled forward by a fixed step.
    RollingWalkForward {
        start: NaiveDateTime,
        end: NaiveDateTime,
        train: Duration,
        test: Duration,
        step: Duration,
    },
}

impl WindowPlan {
    /// Expand the plan into the ordered pairs a batch will run.
    pub fn pairs(&self) -> Result<Vec<WindowPair>, ResearchError> {
        self.pairs_with_limit(DEFAULT_MAX_WINDOW_PAIRS)
    }

    /// Expand this plan without allocating more than `max_pairs` evaluation pairs.
    pub fn pairs_with_limit(&self, max_pairs: usize) -> Result<Vec<WindowPair>, ResearchError> {
        if max_pairs == 0 {
            return Err(ResearchError::InvalidWindow(
                "maximum window pairs must be positive".into(),
            ));
        }
        match self {
            Self::Fixed {
                in_sample,
                out_of_sample,
            } => {
                if in_sample.label() == out_of_sample.label() {
                    return Err(ResearchError::InvalidWindow(
                        "in-sample and out-of-sample windows must have distinct labels".into(),
                    ));
                }
                if out_of_sample.from() < in_sample.to() {
                    return Err(ResearchError::InvalidWindow(format!(
                        "out-of-sample window '{}' starts before in-sample window '{}' ends",
                        out_of_sample.label(),
                        in_sample.label()
                    )));
                }
                Ok(vec![WindowPair {
                    in_sample: in_sample.clone(),
                    out_of_sample: out_of_sample.clone(),
                }])
            }
            Self::RollingWalkForward {
                start,
                end,
                train,
                test,
                step,
            } => Self::rolling_pairs(*start, *end, *train, *test, *step, max_pairs),
        }
    }

    fn rolling_pairs(
        start: NaiveDateTime,
        end: NaiveDateTime,
        train: Duration,
        test: Duration,
        step: Duration,
        max_pairs: usize,
    ) -> Result<Vec<WindowPair>, ResearchError> {
        for (name, value) in [("train", train), ("test", test), ("step", step)] {
            if value <= Duration::zero() {
                return Err(ResearchError::InvalidWindow(format!(
                    "walk-forward {name} span must be positive"
                )));
            }
        }
        if end <= start {
            return Err(ResearchError::InvalidWindow(
                "walk-forward range must end after it starts".into(),
            ));
        }
        let first_train_end = start.checked_add_signed(train).ok_or_else(|| {
            ResearchError::InvalidWindow("walk-forward train end overflowed".into())
        })?;
        let first_test_end = first_train_end.checked_add_signed(test).ok_or_else(|| {
            ResearchError::InvalidWindow("walk-forward test end overflowed".into())
        })?;
        if first_test_end > end {
            return Err(ResearchError::InvalidWindow(
                "walk-forward range is shorter than one train and test span".into(),
            ));
        }

        let step_nanos = step.num_nanoseconds().ok_or_else(|| {
            ResearchError::InvalidWindow("walk-forward step is too large to count".into())
        })?;
        let remaining_nanos = (end - first_test_end).num_nanoseconds().ok_or_else(|| {
            ResearchError::InvalidWindow("walk-forward span is too large to count".into())
        })?;
        let additional = remaining_nanos / step_nanos;
        let pair_count = usize::try_from(additional)
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                ResearchError::InvalidWindow("walk-forward pair count overflowed".into())
            })?;
        if pair_count > max_pairs {
            return Err(ResearchError::InvalidWindow(format!(
                "walk-forward plan produces {pair_count} pairs, above the limit of {max_pairs}"
            )));
        }

        let mut pairs = Vec::with_capacity(pair_count);
        let mut train_start = start;
        for index in 0..pair_count {
            let train_end = train_start.checked_add_signed(train).ok_or_else(|| {
                ResearchError::InvalidWindow("walk-forward train end overflowed".into())
            })?;
            let test_end = train_end.checked_add_signed(test).ok_or_else(|| {
                ResearchError::InvalidWindow("walk-forward test end overflowed".into())
            })?;
            pairs.push(WindowPair {
                in_sample: DataWindow::new(format!("is{index}"), train_start, train_end)?,
                out_of_sample: DataWindow::new(format!("oos{index}"), train_end, test_end)?,
            });
            if index + 1 < pair_count {
                train_start = train_start.checked_add_signed(step).ok_or_else(|| {
                    ResearchError::InvalidWindow("walk-forward step overflowed".into())
                })?;
            }
        }
        Ok(pairs)
    }
}
