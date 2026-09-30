mod support;

use futures::executor::block_on;
use qs_core::OrderType;
use qs_execution::{
    ExecutionIntent, ExecutionOperation, ExecutionPort, IntentError, SubmissionError,
};

use support::{RecordingPort, capabilities, entry_signal, ts};

#[test]
fn intent_requires_bounded_identity_and_matching_time() {
    let signal = entry_signal(OrderType::Market, None);
    let intent = ExecutionIntent::new("command-1", "instance-1", "account-1", ts(0), signal);
    assert!(intent.is_ok());

    let mismatch = ExecutionIntent::new(
        "command-2",
        "instance-1",
        "account-1",
        ts(1),
        entry_signal(OrderType::Market, None),
    )
    .unwrap_err();
    assert!(matches!(mismatch, IntentError::TimestampMismatch { .. }));

    let invalid = ExecutionIntent::new(
        "",
        "instance-1",
        "account-1",
        ts(0),
        entry_signal(OrderType::Market, None),
    )
    .unwrap_err();
    assert!(matches!(invalid, IntentError::InvalidIdentifier { .. }));
}

#[test]
fn submission_acceptance_is_not_a_fill() {
    let port = RecordingPort::new(capabilities());
    let mut port: Box<dyn ExecutionPort> = Box::new(port);
    let request = qs_execution::ExecutionRequest {
        identity: qs_execution::RequestIdentity {
            request_id: "command-1:0".into(),
            parent_command_id: "command-1".into(),
            instance_id: "instance-1".into(),
            account_id: "account-1".into(),
            decision_time: ts(0),
        },
        operation: ExecutionOperation::ModifyStop {
            instrument: support::instrument_spec().instrument,
            position_id: "position-1".into(),
            stoploss: 1.1,
            price_grid: support::instrument_spec().price.grid,
        },
    };
    let submission = block_on(port.submit(request)).unwrap();
    assert_eq!(submission.request_id, "command-1:0");
    assert!(block_on(port.next_report()).unwrap().is_none());
}

#[test]
fn malformed_requests_are_rejected_before_feedback_capacity_changes() {
    let mut request = qs_execution::ExecutionRequest {
        identity: qs_execution::RequestIdentity {
            request_id: "".into(),
            parent_command_id: "command-1".into(),
            instance_id: "instance-1".into(),
            account_id: "account-1".into(),
            decision_time: ts(0),
        },
        operation: ExecutionOperation::ModifyStop {
            instrument: support::instrument_spec().instrument,
            position_id: "position-1".into(),
            stoploss: 1.1,
            price_grid: support::instrument_spec().price.grid,
        },
    };
    let mut projector = qs_execution::FeedbackProjector::new(1, 2).unwrap();
    assert!(matches!(
        projector.register(&request),
        Err(qs_execution::FeedbackError::InvalidRequest(_))
    ));
    assert_eq!(projector.active_requests(), 0);

    request.identity.request_id = "command-1:0".into();
    if let ExecutionOperation::ModifyStop { stoploss, .. } = &mut request.operation {
        *stoploss = f64::NAN;
    }
    assert!(request.validate().is_err());
    assert_eq!(projector.active_requests(), 0);
}

#[test]
fn submission_failures_distinguish_safe_release_from_unknown_acceptance() {
    let definite = SubmissionError::DefinitelyNotSubmitted {
        reason: "validation".into(),
    };
    let unknown = SubmissionError::AcceptanceUnknown {
        reason: "connection lost after write".into(),
    };
    assert!(definite.definitely_not_submitted());
    assert!(!unknown.definitely_not_submitted());
}
