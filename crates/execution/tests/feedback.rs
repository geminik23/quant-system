mod support;

use qs_core::{OrderType, Side};
use qs_execution::{
    ConcreteQuantity, EntryRequest, ExecutionOperation, ExecutionReport, ExecutionRequest,
    FeedbackError, FeedbackProjector, PreparedExecution, ProviderOutcome, ReportOutcome,
    RequestIdentity,
};
use qs_instruments::QuantityUnit;
use qs_strategy::{CommandFact, CommandFeedback, CommandTerminalStatus, ConfiguredActionKind};

use support::{instrument_spec, ts};

fn request(command_id: &str, operation: ExecutionOperation) -> ExecutionRequest {
    ExecutionRequest {
        identity: RequestIdentity {
            request_id: format!("{command_id}:0"),
            parent_command_id: command_id.into(),
            instance_id: "instance-1".into(),
            account_id: "account-1".into(),
            decision_time: ts(0),
        },
        operation,
    }
}

fn entry_request(command_id: &str, steps: u64) -> ExecutionRequest {
    let spec = instrument_spec();
    request(
        command_id,
        ExecutionOperation::Entry(EntryRequest {
            instrument: spec.instrument,
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            level: None,
            price_grid: spec.price.grid,
            quantity: ConcreteQuantity {
                steps,
                amount: steps as f64 * 0.01,
                step_amount: 0.01,
                unit: QuantityUnit::StandardLot,
            },
            initial_stop: Some(1.095),
            targets: Vec::new(),
            provider_rules: Vec::new(),
            group: None,
            trade_id: Some("trade-1".into()),
        }),
    )
}

fn close_request(command_id: &str, steps: u64, full: bool) -> ExecutionRequest {
    request(
        command_id,
        ExecutionOperation::Close {
            instrument: instrument_spec().instrument,
            position_id: "position-1".into(),
            quantity: ConcreteQuantity {
                steps,
                amount: steps as f64 * 0.01,
                step_amount: 0.01,
                unit: QuantityUnit::StandardLot,
            },
            full,
        },
    )
}

fn report(command_id: &str, observation_id: &str, outcome: ReportOutcome) -> ExecutionReport {
    ExecutionReport {
        observation_id: observation_id.into(),
        request_id: format!("{command_id}:0"),
        parent_command_id: command_id.into(),
        observed_at: ts(1),
        outcome,
    }
}

#[test]
fn accepted_and_resting_reports_do_not_fabricate_fills() {
    let request = entry_request("entry-1", 100);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    for (id, outcome) in [
        ("accepted-1", ReportOutcome::Accepted),
        ("resting-1", ReportOutcome::Resting),
    ] {
        let projected = projector.process(report("entry-1", id, outcome)).unwrap();
        assert!(projected.feedback.is_empty());
        assert!(projected.completion.is_none());
    }
    assert_eq!(projector.active_requests(), 1);
}

#[test]
fn partial_entry_then_cancel_preserves_exposure_and_completes_once() {
    let request = entry_request("entry-2", 100);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    let partial_report = report(
        "entry-2",
        "fill-1",
        ReportOutcome::Fill {
            incremental_steps: 40,
            cumulative_steps: 40,
            price: 1.1002,
        },
    );
    let partial = projector.process(partial_report.clone()).unwrap();
    assert_eq!(
        partial.feedback,
        vec![CommandFeedback::Fact {
            command_id: "entry-2".into(),
            fact: CommandFact::EntryFilled,
        }]
    );
    assert!(partial.completion.is_none());

    let duplicate = projector.process(partial_report).unwrap();
    assert!(duplicate.duplicate);
    assert!(duplicate.feedback.is_empty());

    let cancelled = projector
        .process(report(
            "entry-2",
            "cancelled-1",
            ReportOutcome::Cancelled {
                filled_steps: 40,
                reason: "remainder cancelled".into(),
            },
        ))
        .unwrap();
    assert_eq!(
        cancelled.feedback,
        vec![CommandFeedback::Terminal {
            command_id: "entry-2".into(),
            status: CommandTerminalStatus::Applied,
            reason: None,
        }]
    );
    assert_eq!(
        cancelled.completion,
        Some(ProviderOutcome::PartiallyFilledThenCancelled {
            filled_steps: 40,
            cancelled_steps: 60,
        })
    );
    assert_eq!(projector.active_requests(), 0);

    let duplicate_terminal = projector
        .process(report(
            "entry-2",
            "cancelled-1",
            ReportOutcome::Cancelled {
                filled_steps: 40,
                reason: "remainder cancelled".into(),
            },
        ))
        .unwrap();
    assert!(duplicate_terminal.duplicate);
}

#[test]
fn full_close_fact_waits_for_the_complete_quantity() {
    let request = close_request("close-1", 100, true);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    let partial = projector
        .process(report(
            "close-1",
            "close-fill-1",
            ReportOutcome::Fill {
                incremental_steps: 40,
                cumulative_steps: 40,
                price: 1.101,
            },
        ))
        .unwrap();
    assert!(partial.feedback.is_empty());

    let complete = projector
        .process(report(
            "close-1",
            "close-fill-2",
            ReportOutcome::Fill {
                incremental_steps: 60,
                cumulative_steps: 100,
                price: 1.101,
            },
        ))
        .unwrap();
    assert_eq!(
        complete.feedback,
        vec![
            CommandFeedback::Fact {
                command_id: "close-1".into(),
                fact: CommandFact::PositionClosed,
            },
            CommandFeedback::Terminal {
                command_id: "close-1".into(),
                status: CommandTerminalStatus::Applied,
                reason: None,
            },
        ]
    );
}

#[test]
fn partial_full_close_failure_is_explicit_and_never_fabricates_terminal_feedback() {
    let request = close_request("close-2", 100, true);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    projector
        .process(report(
            "close-2",
            "close-partial",
            ReportOutcome::Fill {
                incremental_steps: 25,
                cumulative_steps: 25,
                price: 1.101,
            },
        ))
        .unwrap();
    let error = projector
        .process(report(
            "close-2",
            "close-cancelled",
            ReportOutcome::Cancelled {
                filled_steps: 25,
                reason: "provider stopped".into(),
            },
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        FeedbackError::PartialOutcomeUnresolved { .. }
    ));
    assert_eq!(projector.active_requests(), 1);
}

#[test]
fn cancel_and_modify_project_committed_facts() {
    let spec = instrument_spec();
    let cancel = request(
        "cancel-1",
        ExecutionOperation::CancelPending {
            instrument: spec.instrument.clone(),
            position_id: "position-1".into(),
            order_id: "order-1".into(),
        },
    );
    let modify = request(
        "modify-1",
        ExecutionOperation::ModifyStop {
            instrument: spec.instrument.clone(),
            position_id: "position-1".into(),
            stoploss: 1.1,
            price_grid: spec.price.grid,
        },
    );
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&cancel).unwrap();
    projector.register(&modify).unwrap();

    let cancelled = projector
        .process(report(
            "cancel-1",
            "cancel-report",
            ReportOutcome::Cancelled {
                filled_steps: 0,
                reason: "cancelled".into(),
            },
        ))
        .unwrap();
    assert!(matches!(
        cancelled.feedback.as_slice(),
        [
            CommandFeedback::Fact {
                fact: CommandFact::PendingCancelled,
                ..
            },
            CommandFeedback::Terminal {
                status: CommandTerminalStatus::Applied,
                ..
            }
        ]
    ));

    let modified = projector
        .process(report(
            "modify-1",
            "modify-report",
            ReportOutcome::Modified { stoploss: 1.1 },
        ))
        .unwrap();
    assert!(matches!(
        modified.feedback.as_slice(),
        [
            CommandFeedback::Fact {
                fact: CommandFact::StoplossModified,
                ..
            },
            CommandFeedback::Terminal {
                status: CommandTerminalStatus::Applied,
                ..
            }
        ]
    ));
}

#[test]
fn rejected_reports_require_reasons_and_identical_observations_are_noops() {
    let request = entry_request("entry-3", 10);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    let rejection = report(
        "entry-3",
        "rejected-1",
        ReportOutcome::Rejected {
            reason: "market closed".into(),
        },
    );
    let projected = projector.process(rejection.clone()).unwrap();
    assert!(matches!(
        projected.feedback.as_slice(),
        [CommandFeedback::Terminal {
            status: CommandTerminalStatus::Rejected,
            reason: Some(reason),
            ..
        }] if reason == "market closed"
    ));
    assert!(projector.process(rejection.clone()).unwrap().duplicate);

    let mut conflicting = rejection;
    conflicting.outcome = ReportOutcome::Rejected {
        reason: "different".into(),
    };
    assert!(matches!(
        projector.process(conflicting),
        Err(FeedbackError::ConflictingDuplicate { .. })
    ));
}

#[test]
fn modified_stop_must_match_the_requested_grid_price() {
    let spec = instrument_spec();
    let modify = request(
        "modify-exact",
        ExecutionOperation::ModifyStop {
            instrument: spec.instrument,
            position_id: "position-1".into(),
            stoploss: 1.1,
            price_grid: spec.price.grid,
        },
    );
    let mut projector = FeedbackProjector::new(2, 8).unwrap();
    projector.register(&modify).unwrap();
    for (observation, stoploss) in [("wrong-stop", 1.10001), ("off-grid-stop", 1.100001)] {
        assert!(matches!(
            projector.process(report(
                "modify-exact",
                observation,
                ReportOutcome::Modified { stoploss }
            )),
            Err(FeedbackError::InvalidReport(_))
        ));
    }
    let projected = projector
        .process(report(
            "modify-exact",
            "exact-stop",
            ReportOutcome::Modified { stoploss: 1.1 },
        ))
        .unwrap();
    assert!(matches!(
        projected.feedback.as_slice(),
        [
            CommandFeedback::Fact {
                fact: CommandFact::StoplossModified,
                ..
            },
            CommandFeedback::Terminal {
                status: CommandTerminalStatus::Applied,
                ..
            }
        ]
    ));
}

#[test]
fn parent_terminal_waits_for_every_registered_child() {
    let first = entry_request("multi-entry", 10);
    let mut second = first.clone();
    second.identity.request_id = "multi-entry:1".into();
    let prepared = PreparedExecution {
        command_id: "multi-entry".into(),
        action: ConfiguredActionKind::Entry,
        requests: vec![first.clone(), second.clone()],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    };
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register_execution(&prepared).unwrap();

    let first_result = projector
        .process(report(
            "multi-entry",
            "multi-fill-1",
            ReportOutcome::Fill {
                incremental_steps: 10,
                cumulative_steps: 10,
                price: 1.1,
            },
        ))
        .unwrap();
    assert!(matches!(
        first_result.feedback.as_slice(),
        [CommandFeedback::Fact {
            fact: CommandFact::EntryFilled,
            ..
        }]
    ));

    let mut second_report = report(
        "multi-entry",
        "multi-fill-2",
        ReportOutcome::Fill {
            incremental_steps: 10,
            cumulative_steps: 10,
            price: 1.1,
        },
    );
    second_report.request_id = second.identity.request_id;
    let second_result = projector.process(second_report).unwrap();
    assert!(matches!(
        second_result.feedback.as_slice(),
        [CommandFeedback::Terminal {
            status: CommandTerminalStatus::Applied,
            ..
        }]
    ));
}

#[test]
fn partial_fills_across_children_emit_one_parent_fact() {
    let first = entry_request("partial-children", 10);
    let mut second = first.clone();
    second.identity.request_id = "partial-children:1".into();
    let prepared = PreparedExecution {
        command_id: "partial-children".into(),
        action: ConfiguredActionKind::Entry,
        requests: vec![first, second.clone()],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    };
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register_execution(&prepared).unwrap();

    let first_partial = projector
        .process(report(
            "partial-children",
            "child-1-partial",
            ReportOutcome::Fill {
                incremental_steps: 4,
                cumulative_steps: 4,
                price: 1.1,
            },
        ))
        .unwrap();
    assert!(matches!(
        first_partial.feedback.as_slice(),
        [CommandFeedback::Fact {
            fact: CommandFact::EntryFilled,
            ..
        }]
    ));
    let first_complete = projector
        .process(report(
            "partial-children",
            "child-1-complete",
            ReportOutcome::Fill {
                incremental_steps: 6,
                cumulative_steps: 10,
                price: 1.1,
            },
        ))
        .unwrap();
    assert!(first_complete.feedback.is_empty());

    let mut second_partial = report(
        "partial-children",
        "child-2-partial",
        ReportOutcome::Fill {
            incremental_steps: 5,
            cumulative_steps: 5,
            price: 1.1,
        },
    );
    second_partial.request_id = second.identity.request_id.clone();
    assert!(
        projector
            .process(second_partial)
            .unwrap()
            .feedback
            .is_empty()
    );
    let mut second_complete = report(
        "partial-children",
        "child-2-complete",
        ReportOutcome::Fill {
            incremental_steps: 5,
            cumulative_steps: 10,
            price: 1.1,
        },
    );
    second_complete.request_id = second.identity.request_id;
    assert!(matches!(
        projector
            .process(second_complete)
            .unwrap()
            .feedback
            .as_slice(),
        [CommandFeedback::Terminal {
            status: CommandTerminalStatus::Applied,
            ..
        }]
    ));
}

#[test]
fn active_parent_cannot_be_abandoned() {
    let request = entry_request("active-parent", 10);
    let mut projector = FeedbackProjector::new(2, 8).unwrap();
    projector.register(&request).unwrap();
    assert!(matches!(
        projector.abandon_parent("active-parent"),
        Err(FeedbackError::ActiveParentChildren { .. })
    ));
    assert_eq!(projector.active_requests(), 1);
    assert!(
        projector
            .process(report(
                "active-parent",
                "active-fill",
                ReportOutcome::Fill {
                    incremental_steps: 10,
                    cumulative_steps: 10,
                    price: 1.1,
                },
            ))
            .is_ok()
    );
}

#[test]
fn mixed_child_success_and_failure_never_fabricates_parent_terminal() {
    let first = entry_request("mixed-entry", 10);
    let mut second = first.clone();
    second.identity.request_id = "mixed-entry:1".into();
    let prepared = PreparedExecution {
        command_id: "mixed-entry".into(),
        action: ConfiguredActionKind::Entry,
        requests: vec![first, second.clone()],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    };
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register_execution(&prepared).unwrap();
    projector
        .process(report(
            "mixed-entry",
            "mixed-fill",
            ReportOutcome::Fill {
                incremental_steps: 10,
                cumulative_steps: 10,
                price: 1.1,
            },
        ))
        .unwrap();
    let mut rejected = report(
        "mixed-entry",
        "mixed-rejected",
        ReportOutcome::Rejected {
            reason: "second child rejected".into(),
        },
    );
    rejected.request_id = second.identity.request_id;
    assert!(matches!(
        projector.process(rejected),
        Err(FeedbackError::PartialOutcomeUnresolved { .. })
    ));
    assert!(projector.abandon_parent("mixed-entry").unwrap());
}

#[test]
fn provider_reasons_are_bounded_before_duplicate_retention() {
    let request = entry_request("reason-bound", 10);
    let mut projector = FeedbackProjector::new(2, 8).unwrap();
    projector.register(&request).unwrap();
    let error = projector
        .process(report(
            "reason-bound",
            "oversized-reason",
            ReportOutcome::Rejected {
                reason: "x".repeat(513),
            },
        ))
        .unwrap_err();
    assert!(matches!(error, FeedbackError::InvalidReport(_)));
    assert_eq!(projector.active_requests(), 1);
}

#[test]
fn fill_progression_and_overfill_are_rejected() {
    let request = entry_request("entry-4", 10);
    let mut projector = FeedbackProjector::new(4, 16).unwrap();
    projector.register(&request).unwrap();
    let error = projector
        .process(report(
            "entry-4",
            "overfill-1",
            ReportOutcome::Fill {
                incremental_steps: 11,
                cumulative_steps: 11,
                price: 1.1,
            },
        ))
        .unwrap_err();
    assert!(matches!(error, FeedbackError::InvalidReport(_)));
}
