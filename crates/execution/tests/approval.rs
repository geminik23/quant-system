mod support;

use chrono::Duration;
use qs_core::{OrderType, RawSignal};
use qs_execution::{
    ApprovalAdmission, ApprovalDecision, ApprovalError, ApprovalGate, ApprovalMode,
    ExecutionIntent, PositionLookup, PreparationContext, PreparationError, PreparationOutcome,
    PreparationSizingBasis, default_risk_sizing, prepare,
};
use qs_risk::Verdict;
use qs_strategy::{CommandFeedback, CommandTerminalStatus};

use support::{StaticState, capabilities, entry_signal, instrument_spec, position_ref, quote, ts};

fn intent(command_id: &str, signal: RawSignal) -> ExecutionIntent {
    ExecutionIntent::new(command_id, "instance-1", "account-1", signal.ts(), signal).unwrap()
}

#[test]
fn mode_and_timeout_are_explicit() {
    assert!(matches!(
        ApprovalGate::new(None, 1, 1),
        Err(ApprovalError::MissingMode)
    ));
    assert!(matches!(
        ApprovalGate::new(
            Some(ApprovalMode::RequireEntryApproval {
                timeout: Duration::zero()
            }),
            1,
            1
        ),
        Err(ApprovalError::InvalidTimeout)
    ));
}

#[test]
fn automatic_and_management_intents_are_ready_without_pending_approval() {
    let mut automatic = ApprovalGate::new(Some(ApprovalMode::Auto), 2, 4).unwrap();
    assert!(matches!(
        automatic
            .admit(
                intent("auto-entry", entry_signal(OrderType::Market, None)),
                ts(0)
            )
            .unwrap(),
        ApprovalAdmission::Ready(_)
    ));
    assert!(matches!(
        automatic
            .admit(
                intent("auto-entry", entry_signal(OrderType::Market, None)),
                ts(0)
            )
            .unwrap(),
        ApprovalAdmission::AlreadyConsumed
    ));

    let mut approval = ApprovalGate::new(
        Some(ApprovalMode::RequireEntryApproval {
            timeout: Duration::seconds(30),
        }),
        2,
        4,
    )
    .unwrap();
    assert!(matches!(
        approval
            .admit(
                intent(
                    "close-now",
                    RawSignal::Close {
                        ts: ts(0),
                        position: position_ref()
                    }
                ),
                ts(0)
            )
            .unwrap(),
        ApprovalAdmission::Ready(_)
    ));
    assert_eq!(approval.pending_len(), 0);
}

#[test]
fn approval_expires_at_the_exact_boundary_and_never_dispatches_twice() {
    let mut gate = ApprovalGate::new(
        Some(ApprovalMode::RequireEntryApproval {
            timeout: Duration::seconds(30),
        }),
        2,
        4,
    )
    .unwrap();
    let admission = gate
        .admit(
            intent("approval-1", entry_signal(OrderType::Market, None)),
            ts(0),
        )
        .unwrap();
    assert!(matches!(
        admission,
        ApprovalAdmission::Awaiting { expires_at, .. } if expires_at == ts(0) + Duration::seconds(30)
    ));
    assert!(matches!(
        gate.admit(
            intent("approval-1", entry_signal(OrderType::Market, None)),
            ts(0)
        )
        .unwrap(),
        ApprovalAdmission::AlreadyAwaiting
    ));

    let decision = gate
        .approve("approval-1", ts(0) + Duration::seconds(30))
        .unwrap();
    let ApprovalDecision::Refused(refusal) = decision else {
        panic!("exact expiry must refuse")
    };
    assert!(matches!(
        refusal.completion.feedback.as_slice(),
        [CommandFeedback::Terminal {
            status: CommandTerminalStatus::Rejected,
            reason: Some(_),
            ..
        }]
    ));
    assert!(matches!(
        gate.approve("approval-1", ts(0)).unwrap(),
        ApprovalDecision::AlreadyConsumed
    ));
    assert!(gate.complete("approval-1"));
    assert!(matches!(
        gate.approve("approval-1", ts(0)).unwrap(),
        ApprovalDecision::Unknown
    ));
}

#[test]
fn approval_releases_original_intent_for_fresh_preparation_and_risk_review() {
    let mut gate = ApprovalGate::new(
        Some(ApprovalMode::RequireEntryApproval {
            timeout: Duration::seconds(30),
        }),
        2,
        4,
    )
    .unwrap();
    gate.admit(
        intent("approval-2", entry_signal(OrderType::Market, None)),
        ts(0),
    )
    .unwrap();
    let ApprovalDecision::ReadyForValidation(intent) = gate.approve("approval-2", ts(1)).unwrap()
    else {
        panic!("approval should release the intent")
    };

    let spec = instrument_spec();
    let current_quote = qs_core::PriceQuote {
        ask: 1.10120,
        bid: 1.10100,
        ts: ts(1),
        ..quote()
    };
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let outcome = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(1),
            quote: Some(&current_quote),
            instrument: Some(&spec),
            sizing_policy: default_risk_sizing(),
            balance_before: 9_000.0,
            native_to_account_rate: Some(1.0),
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap();
    let PreparationOutcome::Prepared(prepared) = outcome else {
        panic!("approval should require fresh preparation")
    };
    assert_eq!(prepared.audit.as_ref().unwrap().preparation_price, 1.10120);
    let error = (*prepared)
        .apply_risk_verdict(Verdict::Reject {
            policy: "daily_loss_halt".into(),
            reason: "halt began while approval was pending".into(),
        })
        .unwrap_err();
    assert!(matches!(error, PreparationError::RiskRejected { .. }));
}

#[test]
fn explicit_rejection_and_batch_expiry_produce_no_ready_intent() {
    let mut gate = ApprovalGate::new(
        Some(ApprovalMode::RequireEntryApproval {
            timeout: Duration::seconds(10),
        }),
        3,
        6,
    )
    .unwrap();
    gate.admit(
        intent("reject-1", entry_signal(OrderType::Market, None)),
        ts(0),
    )
    .unwrap();
    assert!(matches!(
        gate.reject("reject-1", "operator rejected").unwrap(),
        ApprovalDecision::Refused(_)
    ));

    gate.admit(
        intent("expire-1", entry_signal(OrderType::Market, None)),
        ts(0),
    )
    .unwrap();
    gate.admit(
        intent("expire-2", entry_signal(OrderType::Market, None)),
        ts(0),
    )
    .unwrap();
    let expired = gate.expire(ts(0) + Duration::seconds(10)).unwrap();
    assert_eq!(expired.len(), 2);
    assert_eq!(gate.pending_len(), 0);
}
