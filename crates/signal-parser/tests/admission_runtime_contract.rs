use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

use signal_parser::ingestion::{
    BoundedBytes, DateTimeUtc, ExternalEventId, PayloadEncoding, SourceEvent, SourceEventKey,
    SourceId, SourceOperation, SourcePayload, SourceRevision, SourceTimestamp,
    SourceTimestampQuality, StructuredPayload,
};
use signal_parser::normalization::raw_signals_v1_schema;
use signal_parser::runner::{
    runtime::{AdmissionRuntime, DrainState},
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
            SourceId::new("runtime:contract-test").unwrap(),
            "runtime-test",
            None,
        )
        .unwrap(),
        event: SourceEvent::new(
            SourceEventKey::new(
                SourceId::new("runtime:contract-test").unwrap(),
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

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::sleep(Duration::from_millis(1)),
        }
    }
}

#[test]
fn runtime_bounds_admission_reports_capacity_and_drains() {
    let inner = Arc::new(BlockingService {
        released: (Mutex::new(false), Condvar::new()),
    });
    let runtime = AdmissionRuntime::new(
        Arc::clone(&inner) as Arc<dyn IngestionService>,
        NonZeroUsize::new(1).unwrap(),
    );

    let first = runtime.submit(submission("first"));
    assert_eq!(runtime.capacity().pending_submissions, 1);
    assert_eq!(runtime.capacity().available_submissions, 0);
    assert!(matches!(
        block_on(runtime.submit(submission("overloaded"))),
        Err(LogicalIngestionServiceError::RetryRequired)
    ));

    inner.release();
    assert!(matches!(
        block_on(first),
        Ok(response) if response.disposition == signal_parser::runner::service::SourceSubmissionDisposition::Accepted
    ));

    let report = runtime.drain();
    assert_eq!(report.state, DrainState::Drained);
    assert_eq!(report.admitted_submissions, 1);
    assert_eq!(report.completed_submissions, 1);
    assert_eq!(report.overload_rejections, 1);
    assert!(matches!(
        block_on(runtime.submit(submission("after-drain"))),
        Err(LogicalIngestionServiceError::Unavailable)
    ));
}
