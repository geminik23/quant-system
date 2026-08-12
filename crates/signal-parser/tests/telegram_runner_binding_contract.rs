use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use signal_parser::adapters::telegram::{
    TelegramAdapterConfig, TelegramRelayInput, TelegramRelayOperation, TelegramRelaySourceAdapter,
};
use signal_parser::ingestion::{DateTimeUtc, SourceEventRef, SourceId, SourceOperation};
use signal_parser::runner::service::{
    IngestionService, LogicalIngestionServiceError, OutcomeReference, SourceSubmission,
    SourceSubmissionDisposition, SourceSubmissionOutcome, SourceSubmissionResponse,
};
use signal_parser::runner::telegram::{TelegramIngestionOutcome, TelegramRelayIngestionBinding};
use signal_parser::state::CommittedBatchId;

#[derive(Default)]
struct RecordingService {
    submissions: Mutex<Vec<SourceSubmission>>,
}

impl IngestionService for RecordingService {
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
        let source = SourceEventRef::from(&submission.event);
        let response = SourceSubmissionResponse {
            admission_identity: submission.admission_identity.clone(),
            source,
            disposition: SourceSubmissionDisposition::Committed,
            outcome_reference: OutcomeReference::from_batch_id(CommittedBatchId::from_bytes(
                [7; 32],
            )),
        };
        self.submissions.lock().unwrap().push(submission);
        Box::pin(async move { Ok(response) })
    }

    fn outcome<'a>(
        &'a self,
        _reference: OutcomeReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<SourceSubmissionOutcome>, LogicalIngestionServiceError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(None) })
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("recording logical service must complete synchronously"),
    }
}

#[test]
fn relay_binding_submits_the_adapted_event_to_the_logical_service() {
    let service = Arc::new(RecordingService::default());
    let adapter = TelegramRelaySourceAdapter::try_new(TelegramAdapterConfig::new(
        SourceId::new("telegram:runner-binding").unwrap(),
        false,
        false,
    ))
    .unwrap();
    let binding = TelegramRelayIngestionBinding::new(adapter, service.clone());

    let outcomes = block_on(
        binding.submit(
            TelegramRelayInput::try_new_message(
                TelegramRelayOperation::New,
                -100_123,
                Some(42),
                Some("synthetic message".to_string()),
                Some(1_786_200_000.25),
                None,
                "delivery-1",
            )
            .unwrap(),
            DateTimeUtc::parse("2026-08-09T12:00:00Z").unwrap(),
        ),
    )
    .unwrap();

    assert!(matches!(
        outcomes.as_slice(),
        [TelegramIngestionOutcome::Submitted { response, .. }]
            if response.disposition == SourceSubmissionDisposition::Committed
    ));
    let submissions = service.submissions.lock().unwrap();
    assert_eq!(submissions.len(), 1);
    assert_eq!(
        submissions[0].event.key().external_id().as_str(),
        "tgmsg:v1:-100123:42"
    );
    assert_eq!(submissions[0].event.operation(), SourceOperation::Create);

    assert!(matches!(
        submissions[0].delivery_identity,
        signal_parser::state::DurableDeliveryIdentity::Stable(ref identity)
            if identity.starts_with("telegram-relay-v1:")
    ));
}
