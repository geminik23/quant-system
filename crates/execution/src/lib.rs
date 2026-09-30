//! Broker-neutral execution preparation, approval, and report projection.
//!
//! This crate converts existing [`qs_core::RawSignal`] intent into validated,
//! quantity-bearing provider requests. It does not own a broker connection,
//! price feed, account store, strategy scheduler, fill engine, or P&L model.
//!
//! A successful [`Submission`] means only that a provider accepted a request
//! for processing. Strategy lifecycle feedback is produced only from validated
//! [`ExecutionReport`] observations through [`FeedbackProjector`].

mod approval;
mod error;
mod feedback;
mod port;
mod prepare;
mod types;

pub use approval::{
    ApprovalAdmission, ApprovalDecision, ApprovalGate, ApprovalMode, ApprovalRefusal,
};
pub use error::{
    ApprovalError, FeedbackError, IntentError, PreparationError, ReportStreamError,
    RequestValidationError, SubmissionError,
};
pub use feedback::{FeedbackProjector, ProjectedReport};
pub use port::{ExecutionPort, PortFuture};
pub use prepare::{PreparationContext, default_risk_sizing, prepare};
pub use types::{
    CompletedIntent, CompletionKind, ConcreteQuantity, EntryRequest, ExecutionCapabilities,
    ExecutionIntent, ExecutionOperation, ExecutionReport, ExecutionRequest, ExposureRequest,
    ManagementOwner, PositionLookup, PositionSnapshot, PositionSnapshotStatus, PreparationAudit,
    PreparationOutcome, PreparationSizingBasis, PreparedExecution, ProviderOutcome, ReportOutcome,
    RequestIdentity, RequestKind, StateResolver, Submission, TargetInstruction,
};
