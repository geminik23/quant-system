//! Provider-neutral backtest client workflow and connection utilities.
//!
//! The default graph contains no transport provider. Enable `xrpc` only in a
//! process composition root that selects the current provider.

#[cfg(feature = "analysis")]
mod analysis;
mod artifact;
mod connector;
mod error;
mod input;
mod output;
mod request;
mod result_document;
mod resume;
pub mod scripted;
mod workflow;
mod workflow_actor;
mod workflow_error;

#[cfg(feature = "xrpc")]
pub mod provider;

#[cfg(feature = "analysis")]
pub use analysis::{
    AdditionalAnalytics, AnalysisCancellation, AnalysisCoordinator, AnalysisError,
    AnalysisRecomputeRequest, AnalysisSnapshot, CalendarOutcomeBucket, LocalMetric,
    LocalMetricStatus, OutcomeTransitions, evaluate_persisted_dataset, project_result_datasets,
    recompute_analysis,
};
pub use artifact::{ArtifactDownload, ArtifactError, VerifiedArtifactPayload};
pub use connector::{
    BacktestCatalogConnector, BacktestConnector, ManagedBacktestClient, ServiceCatalogSnapshot,
    parse_desktop_endpoint, probe_service_catalog,
};
pub use error::{CatalogProbeError, CatalogProbeStage, DesktopEndpointError};
pub use input::{
    BacktestInputInspector, CanonicalDateFilter, InputWarning, InspectSignalInput,
    InspectedSignalInput, PreparationCancellation, SignalDecodingPolicy, SignalFileSummary,
    SignalInputLimits, SignalInputSource,
};
pub use output::{
    CommittedOutput, OutputCommit, OutputError, ResultOutput, StagedOutput, open_result_path,
    stage_output,
};
pub use request::{
    BacktestPreparer, BacktestRequestSummary, BacktestRunOptions, FillModel, HistoricalDataType,
    PrepareBacktestInput, PreparedBacktest, ProfileSelection, ProfileSelectionSummary, SymbolScope,
    SymbolScopeSummary,
};
pub use result_document::*;
pub use resume::{
    ClientJobStatus, LocalCommitState, MemoryRunTransitionStore, OutputConflictPolicy,
    OutputIntent, OutputIntentSummary, OutputTarget, RESUME_RECORD_FORMAT_VERSION,
    ResultDeliverySummary, ResultFileFormat, ResultInputMetadata, ResumeRecord, RunStoreError,
    RunTransitionStore,
};
pub use workflow::{
    BacktestRunSnapshot, BacktestWorkflowEvent, BacktestWorkflowEventKind, ChannelConfigError,
    CompletedBacktest, DEFAULT_COMMAND_CAPACITY, DEFAULT_EVENT_CAPACITY, MAX_COMMAND_CAPACITY,
    MAX_EVENT_CAPACITY, ReconnectBackoffAction, ReconnectObservation, ReconnectPolicy,
    TokioWorkflowSleeper, WorkflowChannelConfig, WorkflowChannelError, WorkflowCommand,
    WorkflowCompletion, WorkflowSleeper, WorkflowState,
};
pub use workflow_actor::{BacktestRunHandle, BacktestWorkflow, BacktestWorkflowEventStream};
pub use workflow_error::{WorkflowError, WorkflowErrorCategory};
