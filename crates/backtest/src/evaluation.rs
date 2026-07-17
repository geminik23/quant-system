//! Additive, report-independent provider evaluation.
//!
//! The module accepts normalized [`PositionOutcome`] rows instead of depending on
//! [`crate::report::TradeResult`] or [`crate::report::PositionSummary`]. A current
//! integration can map one `PositionSummary` to one outcome (`net_pnl`, symbol,
//! side, group, close reasons, and final-close ordering); future report versions
//! can additionally supply R, excursions, lifecycle counters, and execution
//! diagnostics without changing this evaluator.
//!
//! Filtering is exact and case-sensitive. Values within a filter dimension are
//! ORed, while different dimensions (including each tag key) are ANDed.
//! Breakdowns and rolling order are deterministic. No overall score, rank, or
//! rating is produced.

mod evaluator;
mod model;
mod stats;

pub use evaluator::evaluate;
pub use model::*;
pub use stats::{bootstrap_mean_confidence, wilson_interval};

#[cfg(test)]
mod tests;
