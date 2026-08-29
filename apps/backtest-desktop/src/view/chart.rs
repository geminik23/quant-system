//! Viewport-bounded chart primitives for result presentation.

use std::sync::Arc;

use gpui::prelude::*;
use gpui::{PathBuilder, Rgba, canvas, point, px};

use crate::preview::SeriesPoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesProjectionError {
    TooFewPoints,
    NonFinite,
    NonMonotonicX,
}

/// Normalize a finite monotonic series into [0, 1] viewport coordinates.
pub fn normalize_series(points: &[SeriesPoint]) -> Result<Vec<(f32, f32)>, SeriesProjectionError> {
    if points.len() < 2 {
        return Err(SeriesProjectionError::TooFewPoints);
    }
    if points
        .iter()
        .any(|point| !point.x.is_finite() || !point.value.is_finite())
    {
        return Err(SeriesProjectionError::NonFinite);
    }
    if points.windows(2).any(|pair| pair[0].x >= pair[1].x) {
        return Err(SeriesProjectionError::NonMonotonicX);
    }

    let x_min = points.first().unwrap().x;
    let x_span = points.last().unwrap().x - x_min;
    let y_min = points
        .iter()
        .map(|point| point.value)
        .fold(f32::INFINITY, f32::min);
    let y_max = points
        .iter()
        .map(|point| point.value)
        .fold(f32::NEG_INFINITY, f32::max);
    let y_span = y_max - y_min;

    Ok(points
        .iter()
        .map(|point| {
            let x = (point.x - x_min) / x_span;
            let y = if y_span == 0.0 {
                0.5
            } else {
                1.0 - (point.value - y_min) / y_span
            };
            (x, y)
        })
        .collect())
}

/// Render a bounded line chart with an optional area fill.
pub fn line_chart(
    points: Arc<[SeriesPoint]>,
    stroke: Rgba,
    fill: Option<Rgba>,
    height: f32,
) -> impl IntoElement {
    let normalized = normalize_series(&points).unwrap_or_default();
    canvas(
        |_, _, _| (),
        move |bounds, _, window, _| {
            if normalized.len() < 2 {
                return;
            }

            for fraction in [0.25_f32, 0.5, 0.75] {
                let y = bounds.top() + bounds.size.height * fraction;
                let mut grid = PathBuilder::stroke(px(1.));
                grid.move_to(point(bounds.left(), y));
                grid.line_to(point(bounds.right(), y));
                if let Ok(path) = grid.build() {
                    window.paint_path(path, gpui::rgba(0xD5DAE055));
                }
            }

            if let Some(fill) = fill {
                let mut area = PathBuilder::fill();
                let (first_x, first_y) = normalized[0];
                area.move_to(point(
                    bounds.left() + bounds.size.width * first_x,
                    bounds.top() + bounds.size.height * first_y,
                ));
                for (x, y) in normalized.iter().copied().skip(1) {
                    area.line_to(point(
                        bounds.left() + bounds.size.width * x,
                        bounds.top() + bounds.size.height * y,
                    ));
                }
                let last_x = normalized.last().unwrap().0;
                area.line_to(point(
                    bounds.left() + bounds.size.width * last_x,
                    bounds.bottom(),
                ));
                area.line_to(point(
                    bounds.left() + bounds.size.width * first_x,
                    bounds.bottom(),
                ));
                area.close();
                if let Ok(path) = area.build() {
                    window.paint_path(path, fill);
                }
            }

            let mut line = PathBuilder::stroke(px(2.));
            for (index, (x, y)) in normalized.iter().copied().enumerate() {
                let projected = point(
                    bounds.left() + bounds.size.width * x,
                    bounds.top() + bounds.size.height * y,
                );
                if index == 0 {
                    line.move_to(projected);
                } else {
                    line.line_to(projected);
                }
            }
            if let Ok(path) = line.build() {
                window.paint_path(path, stroke);
            }
        },
    )
    .w_full()
    .h(px(height))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_monotonic_finite_series() {
        let projected = normalize_series(&[
            SeriesPoint {
                x: 1.0,
                value: 10.0,
            },
            SeriesPoint {
                x: 2.0,
                value: 20.0,
            },
            SeriesPoint {
                x: 3.0,
                value: 15.0,
            },
        ])
        .unwrap();
        assert_eq!(projected[0].0, 0.0);
        assert_eq!(projected[2].0, 1.0);
        assert_eq!(projected[1].1, 0.0);
        assert_eq!(projected[0].1, 1.0);
    }

    #[test]
    fn rejects_non_finite_or_non_monotonic_series() {
        assert_eq!(
            normalize_series(&[
                SeriesPoint { x: 1.0, value: 1.0 },
                SeriesPoint { x: 1.0, value: 2.0 },
            ]),
            Err(SeriesProjectionError::NonMonotonicX)
        );
        assert_eq!(
            normalize_series(&[
                SeriesPoint { x: 1.0, value: 1.0 },
                SeriesPoint {
                    x: 2.0,
                    value: f32::NAN
                },
            ]),
            Err(SeriesProjectionError::NonFinite)
        );
    }

    #[test]
    fn monthly_fixture_distinguishes_observed_inactive_and_missing() {
        let months = crate::preview::monthly_returns();
        assert!(matches!(
            months[0].status,
            crate::preview::MonthlyReturnStatus::Observed(value) if value > 0.0
        ));
        assert_eq!(
            months[7].status,
            crate::preview::MonthlyReturnStatus::Inactive
        );
        assert_eq!(
            months[11].status,
            crate::preview::MonthlyReturnStatus::Missing
        );
    }

    #[test]
    fn constant_series_uses_vertical_midpoint() {
        let projected = normalize_series(&[
            SeriesPoint { x: 1.0, value: 5.0 },
            SeriesPoint { x: 2.0, value: 5.0 },
        ])
        .unwrap();
        assert_eq!(projected, vec![(0.0, 0.5), (1.0, 0.5)]);
    }
}
