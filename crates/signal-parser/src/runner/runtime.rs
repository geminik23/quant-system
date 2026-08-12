//! Bounded standard-library admission runtime for logical ingestion services.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::ingestion::SourceEventRef;
use crate::runner::service::{
    AdmissionIdentity, IngestionService, LogicalIngestionServiceError, OutcomeReference,
    SourceSubmission, SourceSubmissionDisposition, SourceSubmissionOutcome,
    SourceSubmissionResponse,
};

/// Snapshot of bounded admission capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionCapacity {
    pub maximum_pending_submissions: usize,
    pub pending_submissions: usize,
    pub available_submissions: usize,
}

/// Lifecycle state for admission and draining.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainState {
    Open,
    Draining,
    Drained,
}

/// Stable report emitted when the runtime has drained all admitted work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    pub state: DrainState,
    pub admitted_submissions: usize,
    pub completed_submissions: usize,
    pub overload_rejections: usize,
    pub unavailable_rejections: usize,
    pub deadline_expired_submissions: usize,
    pub cancelled_before_execution: usize,
    pub unknown_completion_observations: usize,
}

/// Bounded count-only metrics for one admission runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMetrics {
    pub admitted_submissions: usize,
    pub completed_submissions: usize,
    pub overload_rejections: usize,
    pub unavailable_rejections: usize,
    pub deadline_expired_submissions: usize,
    pub cancelled_before_execution: usize,
    pub unknown_completion_observations: usize,
}

/// Bounded runtime artifact that retains metrics rather than submissions or payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeRunArtifact {
    pub schema_version: u32,
    pub drain_state: DrainState,
    pub capacity: AdmissionCapacity,
    pub metrics: RuntimeMetrics,
}

/// A cooperative cancellation signal shared with one controlled submission.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Absolute processing deadline measured by the local runtime clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeDeadline(Instant);

impl RuntimeDeadline {
    pub fn from_now(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.0
    }

    fn remaining(&self) -> Option<Duration> {
        self.0.checked_duration_since(Instant::now())
    }
}

/// Whether the runtime can prove that controlled work did not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionState {
    NotStarted,
    Unknown,
}

/// Provider-neutral result of one controlled submission.
#[derive(Debug)]
pub enum ControlledSubmissionResult {
    Completed(Result<SourceSubmissionResponse, LogicalIngestionServiceError>),
    DeadlineExceeded { completion: CompletionState },
    Cancelled { completion: CompletionState },
}

struct RuntimeState {
    drain_state: DrainState,
    pending_submissions: usize,
    admitted_submissions: usize,
    completed_submissions: usize,
    overload_rejections: usize,
    unavailable_rejections: usize,
    deadline_expired_submissions: usize,
    cancelled_before_execution: usize,
    unknown_completion_observations: usize,
}

enum JobResult {
    Completed(Result<SourceSubmissionResponse, LogicalIngestionServiceError>),
    DeadlineExceeded,
    Cancelled,
}

struct Job {
    submission: SourceSubmission,
    response: mpsc::Sender<JobResult>,
    deadline: Option<RuntimeDeadline>,
    cancellation: Option<CancellationToken>,
}

struct RuntimeInner {
    maximum_pending_submissions: usize,
    service: Arc<dyn IngestionService>,
    sender: Mutex<Option<SyncSender<Job>>>,
    state: Mutex<RuntimeState>,
    drained: Condvar,
    workers: Mutex<Vec<JoinHandle<()>>>,
    admissions: Mutex<BTreeMap<AdmissionIdentity, SourceEventRef>>,
    completed_admissions: Mutex<BTreeMap<AdmissionIdentity, OutcomeReference>>,
}

/// A single-worker bounded admission runtime backed only by standard-library synchronization.
pub struct AdmissionRuntime {
    inner: Arc<RuntimeInner>,
}

struct AdmissionWait {
    response_receiver: Receiver<JobResult>,
}

enum AdmissionRejection {
    RetryRequired,
    Unavailable,
}

impl From<AdmissionRejection> for LogicalIngestionServiceError {
    fn from(value: AdmissionRejection) -> Self {
        match value {
            AdmissionRejection::RetryRequired => Self::RetryRequired,
            AdmissionRejection::Unavailable => Self::Unavailable,
        }
    }
}

impl AdmissionRuntime {
    pub fn new(
        service: Arc<dyn IngestionService>,
        maximum_pending_submissions: NonZeroUsize,
    ) -> Self {
        let maximum_pending_submissions = maximum_pending_submissions.get();
        let (sender, receiver) = mpsc::sync_channel(maximum_pending_submissions);
        let inner = Arc::new(RuntimeInner {
            maximum_pending_submissions,
            service: Arc::clone(&service),
            sender: Mutex::new(Some(sender)),
            state: Mutex::new(RuntimeState {
                drain_state: DrainState::Open,
                pending_submissions: 0,
                admitted_submissions: 0,
                completed_submissions: 0,
                overload_rejections: 0,
                unavailable_rejections: 0,
                deadline_expired_submissions: 0,
                cancelled_before_execution: 0,
                unknown_completion_observations: 0,
            }),
            drained: Condvar::new(),
            workers: Mutex::new(Vec::new()),
            admissions: Mutex::new(BTreeMap::new()),
            completed_admissions: Mutex::new(BTreeMap::new()),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::spawn(move || run_worker(service, receiver, worker_inner));
        inner
            .workers
            .lock()
            .expect("runtime workers lock poisoned")
            .push(worker);
        Self { inner }
    }

    pub fn capacity(&self) -> AdmissionCapacity {
        let state = self
            .inner
            .state
            .lock()
            .expect("runtime state lock poisoned");
        AdmissionCapacity {
            maximum_pending_submissions: self.inner.maximum_pending_submissions,
            pending_submissions: state.pending_submissions,
            available_submissions: self
                .inner
                .maximum_pending_submissions
                .saturating_sub(state.pending_submissions),
        }
    }

    pub fn drain_state(&self) -> DrainState {
        self.inner
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .drain_state
    }

    /// Returns bounded runtime metrics without retaining submission data.
    pub fn run_artifact(&self) -> RuntimeRunArtifact {
        let state = self
            .inner
            .state
            .lock()
            .expect("runtime state lock poisoned");
        RuntimeRunArtifact {
            schema_version: 1,
            drain_state: state.drain_state,
            capacity: AdmissionCapacity {
                maximum_pending_submissions: self.inner.maximum_pending_submissions,
                pending_submissions: state.pending_submissions,
                available_submissions: self
                    .inner
                    .maximum_pending_submissions
                    .saturating_sub(state.pending_submissions),
            },
            metrics: runtime_metrics(&state),
        }
    }

    /// Drains the runtime and returns bounded metrics for the completed run.
    pub fn drain(&self) -> DrainReport {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("runtime state lock poisoned");
            if state.drain_state == DrainState::Open {
                state.drain_state = DrainState::Draining;
                self.inner
                    .sender
                    .lock()
                    .expect("runtime sender lock poisoned")
                    .take();
            }
            while state.pending_submissions != 0 {
                state = self
                    .inner
                    .drained
                    .wait(state)
                    .expect("runtime state lock poisoned");
            }
            state.drain_state = DrainState::Drained;
        }
        for worker in self
            .inner
            .workers
            .lock()
            .expect("runtime workers lock poisoned")
            .drain(..)
        {
            worker.join().expect("runtime worker panicked");
        }
        self.report()
    }

    fn report(&self) -> DrainReport {
        let state = self
            .inner
            .state
            .lock()
            .expect("runtime state lock poisoned");
        DrainReport {
            state: state.drain_state,
            admitted_submissions: state.admitted_submissions,
            completed_submissions: state.completed_submissions,
            overload_rejections: state.overload_rejections,
            unavailable_rejections: state.unavailable_rejections,
            deadline_expired_submissions: state.deadline_expired_submissions,
            cancelled_before_execution: state.cancelled_before_execution,
            unknown_completion_observations: state.unknown_completion_observations,
        }
    }

    /// Submits once and observes a local deadline and cooperative cancellation signal.
    ///
    /// A post-admission deadline or cancellation reports `Unknown` because work may have started; callers must reconcile by admission identity rather than resubmitting.
    pub fn submit_controlled(
        &self,
        submission: SourceSubmission,
        deadline: RuntimeDeadline,
        cancellation: CancellationToken,
    ) -> ControlledSubmissionResult {
        if cancellation.is_cancelled() {
            self.record_cancelled_before_execution();
            return ControlledSubmissionResult::Cancelled {
                completion: CompletionState::NotStarted,
            };
        }
        if deadline.is_expired() {
            self.record_deadline_expired();
            return ControlledSubmissionResult::DeadlineExceeded {
                completion: CompletionState::NotStarted,
            };
        }

        let admission = match self.admit_job(submission, Some(deadline), Some(cancellation.clone()))
        {
            Ok(admission) => admission,
            Err(rejection) => return ControlledSubmissionResult::Completed(Err(rejection.into())),
        };
        let receiver = admission.response_receiver;
        loop {
            if cancellation.is_cancelled() {
                self.record_unknown_completion();
                return ControlledSubmissionResult::Cancelled {
                    completion: CompletionState::Unknown,
                };
            }
            let Some(remaining) = deadline.remaining() else {
                self.record_unknown_completion();
                return ControlledSubmissionResult::DeadlineExceeded {
                    completion: CompletionState::Unknown,
                };
            };
            match receiver.recv_timeout(remaining.min(Duration::from_millis(1))) {
                Ok(JobResult::Completed(result)) => {
                    return ControlledSubmissionResult::Completed(result);
                }
                Ok(JobResult::DeadlineExceeded) => {
                    return ControlledSubmissionResult::DeadlineExceeded {
                        completion: CompletionState::NotStarted,
                    };
                }
                Ok(JobResult::Cancelled) => {
                    return ControlledSubmissionResult::Cancelled {
                        completion: CompletionState::NotStarted,
                    };
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return ControlledSubmissionResult::Completed(Err(
                        LogicalIngestionServiceError::Unavailable,
                    ));
                }
            }
        }
    }

    fn admit(&self, submission: SourceSubmission) -> AdmissionFuture {
        AdmissionFuture::ready(self.admit_immediately(submission))
    }

    fn admit_immediately(
        &self,
        submission: SourceSubmission,
    ) -> Result<SourceSubmissionResponse, LogicalIngestionServiceError> {
        let source = SourceEventRef::from(&submission.event);
        let admission_identity = submission.admission_identity.clone();
        {
            let admissions = self
                .inner
                .admissions
                .lock()
                .expect("runtime admissions lock poisoned");
            if admissions.contains_key(&admission_identity) {
                return Ok(SourceSubmissionResponse {
                    admission_identity: admission_identity.clone(),
                    source,
                    disposition: SourceSubmissionDisposition::Accepted,
                    outcome_reference: OutcomeReference::from_admission_identity(
                        &admission_identity,
                    ),
                });
            }
        }
        self.admit_job(submission, None, None)
            .map_err(LogicalIngestionServiceError::from)?;
        self.inner
            .admissions
            .lock()
            .expect("runtime admissions lock poisoned")
            .insert(admission_identity.clone(), source.clone());
        Ok(SourceSubmissionResponse {
            admission_identity: admission_identity.clone(),
            source,
            disposition: SourceSubmissionDisposition::Accepted,
            outcome_reference: OutcomeReference::from_admission_identity(&admission_identity),
        })
    }

    fn admit_job(
        &self,
        submission: SourceSubmission,
        deadline: Option<RuntimeDeadline>,
        cancellation: Option<CancellationToken>,
    ) -> Result<AdmissionWait, AdmissionRejection> {
        let (response_sender, response_receiver) = mpsc::channel();
        let job = Job {
            submission,
            response: response_sender,
            deadline,
            cancellation,
        };
        let mut state = self
            .inner
            .state
            .lock()
            .expect("runtime state lock poisoned");
        if state.drain_state != DrainState::Open {
            state.unavailable_rejections += 1;
            return Err(AdmissionRejection::Unavailable);
        }
        if state.pending_submissions == self.inner.maximum_pending_submissions {
            state.overload_rejections += 1;
            return Err(AdmissionRejection::RetryRequired);
        }
        let sender = self
            .inner
            .sender
            .lock()
            .expect("runtime sender lock poisoned");
        match sender
            .as_ref()
            .expect("open runtime has a sender")
            .try_send(job)
        {
            Ok(()) => {
                state.pending_submissions += 1;
                state.admitted_submissions += 1;
                Ok(AdmissionWait { response_receiver })
            }
            Err(TrySendError::Full(_)) => {
                state.overload_rejections += 1;
                Err(AdmissionRejection::RetryRequired)
            }
            Err(TrySendError::Disconnected(_)) => {
                state.unavailable_rejections += 1;
                Err(AdmissionRejection::Unavailable)
            }
        }
    }

    fn record_deadline_expired(&self) {
        self.inner
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .deadline_expired_submissions += 1;
    }

    fn record_cancelled_before_execution(&self) {
        self.inner
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .cancelled_before_execution += 1;
    }

    fn record_unknown_completion(&self) {
        self.inner
            .state
            .lock()
            .expect("runtime state lock poisoned")
            .unknown_completion_observations += 1;
    }
}

impl Drop for AdmissionRuntime {
    fn drop(&mut self) {
        let _ = self.drain();
    }
}

impl IngestionService for AdmissionRuntime {
    fn submit<'a>(
        &'a self,
        submission: SourceSubmission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SourceSubmissionResponse, LogicalIngestionServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(self.admit(submission))
    }

    fn outcome<'a>(
        &'a self,
        reference: OutcomeReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<SourceSubmissionOutcome>, LogicalIngestionServiceError>,
                > + Send
                + 'a,
        >,
    > {
        if let Some(identity) = reference.admission_identity() {
            let source = self
                .inner
                .admissions
                .lock()
                .expect("runtime admissions lock poisoned")
                .get(&identity)
                .cloned();
            let completed = self
                .inner
                .completed_admissions
                .lock()
                .expect("runtime admissions lock poisoned")
                .get(&identity)
                .cloned();
            return Box::pin(async move {
                match (source, completed) {
                    (Some(_source), Some(committed_reference)) => {
                        self.inner.service.outcome(committed_reference).await
                    }
                    (Some(source), None) => {
                        Ok(Some(SourceSubmissionOutcome::Pending { reference, source }))
                    }
                    (None, _) => Ok(None),
                }
            });
        }
        self.inner.service.outcome(reference)
    }
}

struct AdmissionFuture {
    ready: Option<Result<SourceSubmissionResponse, LogicalIngestionServiceError>>,
}

impl AdmissionFuture {
    fn ready(result: Result<SourceSubmissionResponse, LogicalIngestionServiceError>) -> Self {
        Self {
            ready: Some(result),
        }
    }
}

impl Future for AdmissionFuture {
    type Output = Result<SourceSubmissionResponse, LogicalIngestionServiceError>;

    fn poll(
        mut self: Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(
            self.ready
                .take()
                .expect("admission future must be polled only once"),
        )
    }
}

fn run_worker(
    service: Arc<dyn IngestionService>,
    receiver: Receiver<Job>,
    inner: Arc<RuntimeInner>,
) {
    while let Ok(job) = receiver.recv() {
        let admission_identity = job.submission.admission_identity.clone();
        let result = if job
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            let mut state = inner.state.lock().expect("runtime state lock poisoned");
            state.cancelled_before_execution += 1;
            JobResult::Cancelled
        } else if job
            .deadline
            .as_ref()
            .is_some_and(RuntimeDeadline::is_expired)
        {
            let mut state = inner.state.lock().expect("runtime state lock poisoned");
            state.deadline_expired_submissions += 1;
            JobResult::DeadlineExceeded
        } else {
            JobResult::Completed(futures::executor::block_on(service.submit(job.submission)))
        };
        if let JobResult::Completed(Ok(response)) = &result {
            inner
                .completed_admissions
                .lock()
                .expect("runtime completed admissions lock poisoned")
                .insert(admission_identity, response.outcome_reference.clone());
        }
        let _ = job.response.send(result);
        let mut state = inner.state.lock().expect("runtime state lock poisoned");
        state.pending_submissions -= 1;
        state.completed_submissions += 1;
        if state.pending_submissions == 0 {
            inner.drained.notify_all();
        }
    }
}

fn runtime_metrics(state: &RuntimeState) -> RuntimeMetrics {
    RuntimeMetrics {
        admitted_submissions: state.admitted_submissions,
        completed_submissions: state.completed_submissions,
        overload_rejections: state.overload_rejections,
        unavailable_rejections: state.unavailable_rejections,
        deadline_expired_submissions: state.deadline_expired_submissions,
        cancelled_before_execution: state.cancelled_before_execution,
        unknown_completion_observations: state.unknown_completion_observations,
    }
}
