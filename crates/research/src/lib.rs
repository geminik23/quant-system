//! Parameter-family search over historical replay.
//!
//! A declared space binds typed strategy parameters into complete documents. A batch runs every point over paired evaluation windows, retains normalized outcomes and bound documents, and exposes a deterministic table plus pooled provider evaluation.
//!
//! The crate provides the loop and the table. It does not choose a winner: there is no score, no rank, and no overall rating anywhere in its output, because a search over many configurations produces good-looking numbers by chance and a framework that ranks them invites reading that chance as skill. Every table reports how many configurations were searched so a reader can weigh what they are looking at, and pairs each in-sample result with the out-of-sample result for the same configuration.

mod error;
mod experiment;
mod factory;
mod family;
mod geometry;
mod loader;
mod plan;
mod projectors;
mod recipe;
mod runner;
mod search;
mod space;
mod table;
mod variants;
mod window;

pub mod families;

pub use error::{ResearchError, RunFailure};
pub use experiment::{
    AdmittedMarketView, BoundedTrace, CachedFeatureSample, CachedFeatureValues,
    CheckpointDependency, CheckpointLimits, CompletedRunCheckpoint, ConcentrationBucket,
    FeatureCache, FeatureCacheKey, SearchCheckpoint, SelectedCandidateEvidence, TraceLimits,
    TraceRecord, UncertaintyAssessment, cached_market_midpoints, cached_market_midpoints_for_view,
    project_cached_midpoints_to_bars, selected_candidate_evidence,
};
pub use factory::{
    DirectFactoryPoint, DirectResearchFactory, DirectRunCandidate, DirectStrategyError,
    run_direct_factory_batch, run_direct_factory_batch_controlled,
    run_direct_factory_batch_controlled_resume,
};
pub use family::StrategyFamily;
pub use geometry::SeriesGeometry;
pub use loader::{
    load_symbol_bars, load_symbol_ordered_ticks, load_symbol_ordered_ticks_controlled,
    load_symbol_price_bars, load_symbol_price_bars_controlled, load_symbol_ticks,
};
pub use plan::{PortfolioPlan, ResearchAdmissionLimits, ResearchPlan};
pub use projectors::{
    CachedMidpointProjectorSelection, CalendarInputDocument, CalendarProjectorSelection,
    HistoricalInputsDocument, NamedProjectorSelection, ProjectedStrategyFamily,
    QuoteProjectorSelection,
};
pub use qs_market_loader::{CountCapability, MarketLoadLimits, SeriesDescriptor, StoredPriceBasis};
pub use recipe::{
    CandidateRecipe, EndpointBounds, ExperimentId, ExperimentOptions, ExperimentRecipe,
    InputProjectorSnapshot, RegisteredFactorySelection, RunCoverage, RunRecipe,
    SeriesBindingSnapshot, UnavailableCoverage, snapshot_bindings,
};
pub use runner::{
    BatchProgress, ResearchBatch, SymbolEvents, batch_data_range, batch_data_range_with_limits,
    rerun_selected_candidate, rerun_selected_candidate_protected, run_batch, run_batch_controlled,
    run_batch_controlled_with_experiment, run_batch_controlled_with_experiment_resume,
    run_batch_controlled_with_limits, run_batch_with_experiment, selected_candidate_data_range,
    validate_bar_window_alignment, validate_bar_window_alignment_with_limits, validate_batch,
    validate_batch_with_limits,
};
pub use search::{
    CaptureCandidate, EvaluationRole, FrozenSelection, GeneratedStructuralFamily,
    GenerationCompletion, GenerationDispositions, PredicateAtom, ProtectedExperiment,
    ResumedStructuralGeneration, SplitAccessKind, SplitAccessRecord, StructuralCandidate,
    StructuralComparison, StructuralGeneration, StructuralOperators, StructuralResourceLimits,
    StructuralSearchSpec, resume_structural_generation, strict_embargo,
};
pub use space::{DeclaredSpace, DeclaredSpaceLimits};

pub use table::{PairedRow, ResearchRow, ResearchTable, RunStatus};
pub use variants::{
    ExecutionVariant, ExecutionVariantLimits, HeterogeneousDirectInstanceSpec,
    HeterogeneousInstanceSpec, HeterogeneousPortfolioCandidate,
    MixedHeterogeneousPortfolioCandidate, VariantResearchBatch, run_execution_variants,
    run_execution_variants_controlled, run_heterogeneous_portfolios,
    run_heterogeneous_portfolios_controlled, run_mixed_heterogeneous_portfolios,
};
pub use window::{DataWindow, WindowPair, WindowPlan};
