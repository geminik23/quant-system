use chrono::{Duration, NaiveDateTime};

use crate::error::ResearchError;

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
            } => Self::rolling_pairs(*start, *end, *train, *test, *step),
        }
    }

    fn rolling_pairs(
        start: NaiveDateTime,
        end: NaiveDateTime,
        train: Duration,
        test: Duration,
        step: Duration,
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
        if start + train + test > end {
            return Err(ResearchError::InvalidWindow(
                "walk-forward range is shorter than one train and test span".into(),
            ));
        }

        let mut pairs = Vec::new();
        let mut train_start = start;
        // A split is emitted only when its whole test span fits, so every pair in the result was evaluated over the same amount of out-of-sample data.
        while train_start + train + test <= end {
            let train_end = train_start + train;
            let test_end = train_end + test;
            let index = pairs.len();
            pairs.push(WindowPair {
                in_sample: DataWindow::new(format!("is{index}"), train_start, train_end)?,
                out_of_sample: DataWindow::new(format!("oos{index}"), train_end, test_end)?,
            });
            train_start += step;
        }
        Ok(pairs)
    }
}
