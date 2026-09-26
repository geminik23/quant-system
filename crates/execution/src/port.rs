use std::future::Future;
use std::pin::Pin;

use crate::{
    ExecutionCapabilities, ExecutionReport, ExecutionRequest, ReportStreamError, Submission,
    SubmissionError,
};

/// Runtime-neutral boxed future returned by an execution provider.
pub type PortFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Broker-neutral request and report boundary.
///
/// Implementations own their connection lifecycle. The contract performs no
/// automatic retries, and a closed report stream is not completion evidence.
pub trait ExecutionPort: Send {
    fn capabilities(&self) -> ExecutionCapabilities;

    fn submit<'a>(
        &'a mut self,
        request: ExecutionRequest,
    ) -> PortFuture<'a, Result<Submission, SubmissionError>>;

    fn next_report<'a>(
        &'a mut self,
    ) -> PortFuture<'a, Result<Option<ExecutionReport>, ReportStreamError>>;
}
