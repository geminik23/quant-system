//! Neutral strategy families shipped with the crate.
//!
//! These exist so the search loop has a public, self-contained example to run and test against. They are ordinary implementations of [`StrategyFamily`](crate::StrategyFamily) with no privileged access, and a caller's own family is written the same way.

mod ema_cross;

pub use ema_cross::{EmaCrossFamily, EmaCrossPoint, EmaEntryCondition};
