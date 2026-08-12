use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::raw_signals_v1_schema;
use signal_parser::runner::{
    runtime::{
        AdmissionRuntime, CancellationToken, CompletionState, ControlledSubmissionResult,
        RuntimeDeadline,
    },
    service::{
        AdmissionIdentity, IngestionService, LogicalIngestionServiceError, OutcomeReference,
        SourceSubmission, SourceSubmissionOutcome, SourceSubmissionResponse,
    },
};

struct BlockingService {
    released: (Mutex<bool>, Condvar),
}

impl BlockingService {
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}

impl IngestionService for BlockingService {
    fn submit<'a>(
        &'a self,
        _submission: SourceSubmission,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<SourceSubmissionResponse, LogicalIngestionServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
            Err(LogicalIngestionServiceError::RetryRequired)
        })
    }

    fn outcome<'a>(
        &'a self,
        _reference: OutcomeReference,
    ) -> std::pin::Pin<
        Box<
            dyn Future<
                    Output = Result<Option<SourceSubmissionOutcome>, LogicalIngestionServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async { Err(LogicalIngestionServiceError::Unavailable) })
    }
}

fn submission(identity: &str) -> SourceSubmission {
    let timestamp = DateTimeUtc::parse("2026-08-10T00:00:00Z").unwrap();
    SourceSubmission {
        admission_identity: AdmissionIdentity::try_new(identity).unwrap(),
        delivery_identity: signal_parser::state::DurableDeliveryIdentity::Stable(format!(
            "runtime-delivery:{identity}"
        )),
        authenticated_context: signal_parser::runner::service::AuthenticatedSourceContext::try_new(
            SourceId::new("runtime:completion-contract-test").unwrap(),
            "runtime-test",
            None,
        )
        .unwrap(),
        event: SourceEvent::new(
            SourceEventKey::new(
                SourceId::new("runtime:completion-contract-test").unwrap(),
                ExternalEventId::new(identity).unwrap(),
            ),
            SourceOperation::Create,
            SourceRevision::Monotonic(1),
            SourceTimestamp::new(timestamp, SourceTimestampQuality::SourceProvided),
            timestamp,
            SourcePayload::Structured(StructuredPayload::new(
                raw_signals_v1_schema(),
                PayloadEncoding::Json,
                BoundedBytes::new(
                    serde_json::to_vec(&serde_json::json!({
                        "schema_version": 1,
                        "signals": [{"action": "CloseAll", "ts": "2026-08-10T00:00:00Z"}]
                    }))
                    .unwrap(),
                )
                .unwrap(),
            )),
        ),
    }
}

#[test]
fn controlled_runtime_preserves_timeout_and_cancellation_completion_knowledge() {
    let service = Arc::new(BlockingService {
        released: (Mutex::new(false), Condvar::new()),
    });
    let runtime = AdmissionRuntime::new(
        Arc::clone(&service) as Arc<dyn IngestionService>,
        NonZeroUsize::new(2).unwrap(),
    );

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        runtime.submit_controlled(
            submission("cancelled-before-admission"),
            RuntimeDeadline::from_now(Duration::from_secs(1)),
            cancelled,
        ),
        ControlledSubmissionResult::Cancelled {
            completion: CompletionState::NotStarted,
        }
    ));
    assert!(matches!(
        runtime.submit_controlled(
            submission("expired-before-admission"),
            RuntimeDeadline::from_now(Duration::ZERO),
            CancellationToken::new(),
        ),
        ControlledSubmissionResult::DeadlineExceeded {
            completion: CompletionState::NotStarted,
        }
    ));

    assert!(matches!(
        runtime.submit_controlled(
            submission("deadline-while-running"),
            RuntimeDeadline::from_now(Duration::from_millis(100)),
            CancellationToken::new(),
        ),
        ControlledSubmissionResult::DeadlineExceeded {
            completion: CompletionState::Unknown,
        }
    ));

    let cancelled_after_admission = CancellationToken::new();
    let cancellation_request = cancelled_after_admission.clone();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5));
        cancellation_request.cancel();
    });
    assert!(matches!(
        runtime.submit_controlled(
            submission("cancelled-after-admission"),
            RuntimeDeadline::from_now(Duration::from_secs(1)),
            cancelled_after_admission,
        ),
        ControlledSubmissionResult::Cancelled {
            completion: CompletionState::Unknown,
        }
    ));
    canceller.join().unwrap();

    service.release();
    let report = runtime.drain();
    assert_eq!(report.admitted_submissions, 2);
    assert_eq!(report.completed_submissions, 2);
    assert_eq!(report.deadline_expired_submissions, 1);
    assert_eq!(report.cancelled_before_execution, 2);
    assert_eq!(report.unknown_completion_observations, 2);

    let artifact = runtime.run_artifact();
    assert_eq!(artifact.schema_version, 1);
    assert_eq!(artifact.metrics.unknown_completion_observations, 2);
    assert_eq!(artifact.capacity.pending_submissions, 0);
}
