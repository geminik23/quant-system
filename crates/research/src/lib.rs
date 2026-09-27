//! Parameter-family search over historical replay.
//!
//! A declared space binds typed strategy parameters into complete documents. A batch runs every point over paired evaluation windows, retains normalized outcomes and bound documents, and exposes a deterministic table plus pooled provider evaluation.
//!
//! The crate provides the loop and the table. It does not choose a winner: there is no score, no rank, and no overall rating anywhere in its output, because a search over many configurations produces good-looking numbers by chance and a framework that ranks them invites reading that chance as skill. Every table reports how many configurations were searched so a reader can weigh what they are looking at, and pairs each in-sample result with the out-of-sample result for the same configuration.

mod error;
mod family;
mod geometry;
mod loader;
mod plan;
mod recipe;
mod runner;
mod space;
mod table;
mod window;

pub mod families;

pub use error::{ResearchError, RunFailure};
pub use family::StrategyFamily;
pub use geometry::SeriesGeometry;
pub use loader::{load_symbol_bars, load_symbol_ticks};
pub use plan::{PortfolioPlan, ResearchAdmissionLimits, ResearchPlan};
pub use recipe::{
    CandidateRecipe, EndpointBounds, ExperimentId, ExperimentOptions, ExperimentRecipe,
    RunCoverage, RunRecipe, SeriesBindingSnapshot, UnavailableCoverage, snapshot_bindings,
};
pub use runner::{
    BatchProgress, ResearchBatch, SymbolEvents, batch_data_range, batch_data_range_with_limits,
    run_batch, run_batch_controlled, run_batch_controlled_with_experiment,
    run_batch_controlled_with_limits, run_batch_with_experiment, validate_bar_window_alignment,
    validate_bar_window_alignment_with_limits, validate_batch, validate_batch_with_limits,
};
pub use space::{DeclaredSpace, DeclaredSpaceLimits};

pub use table::{PairedRow, ResearchRow, ResearchTable, RunStatus};
pub use window::{DataWindow, WindowPair, WindowPlan};
