mod support;

use qs_core::{OrderType, RawSignal, SizingPolicy};
use qs_execution::{
    ExecutionCapabilities, ExecutionIntent, ExecutionOperation, ManagementOwner, PositionLookup,
    PreparationContext, PreparationError, PreparationOutcome, PreparationSizingBasis,
    default_risk_sizing, prepare,
};
use qs_risk::Verdict;
use qs_strategy::{CommandFeedback, CommandTerminalStatus};

use support::{
    StaticState, capabilities, entry_signal, instrument_spec, open_snapshot, pending_snapshot,
    position_ref, quote, trailing_profile, ts,
};

fn intent(command_id: &str, signal: RawSignal) -> ExecutionIntent {
    ExecutionIntent::new(command_id, "instance-1", "account-1", signal.ts(), signal).unwrap()
}

fn prepared(outcome: PreparationOutcome) -> qs_execution::PreparedExecution {
    match outcome {
        PreparationOutcome::Prepared(prepared) => *prepared,
        PreparationOutcome::Completed(_) => panic!("expected prepared request"),
    }
}

#[test]
fn market_entry_reuses_profile_and_one_percent_sizing() {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let intent = intent("entry-1", entry_signal(OrderType::Market, None));
    let outcome = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: default_risk_sizing(),
            balance_before: 10_000.0,
            native_to_account_rate: Some(1.0),
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap();
    let prepared = prepared(outcome);
    assert_eq!(prepared.requests.len(), 1);
    let audit = prepared.audit.as_ref().unwrap();
    assert_eq!(audit.original_signal_price, None);
    assert_eq!(audit.preparation_price, quote.ask);
    assert_eq!(audit.sizing_reference_price, quote.ask);
    assert_eq!(audit.sizing.requested_account_risk, Some(100.0));
    assert_eq!(
        prepared.exposure.as_ref().unwrap().requested_risk,
        Some(100.0)
    );
    match &prepared.requests[0].operation {
        ExecutionOperation::Entry(entry) => {
            assert_eq!(entry.level, None);
            assert_eq!(entry.quantity.steps, audit.sizing.final_lot_steps);
            assert_eq!(entry.quantity.amount, audit.sizing.final_lot);
            assert_eq!(entry.targets.len(), 2);
            assert_eq!(
                entry
                    .targets
                    .iter()
                    .map(|target| target.quantity.steps)
                    .sum::<u64>(),
                entry.quantity.steps
            );
            assert!(
                entry
                    .targets
                    .iter()
                    .all(|target| target.quantity.step_amount == entry.quantity.step_amount)
            );
            assert_eq!(entry.initial_stop, Some(1.095));
        }
        other => panic!("unexpected operation: {other:?}"),
    }
}

#[test]
fn limit_and_stop_entries_keep_their_concrete_levels() {
    for (command, order_type, level) in [
        ("limit-1", OrderType::Limit, 1.099),
        ("stop-1", OrderType::Stop, 1.102),
    ] {
        let spec = instrument_spec();
        let quote = quote();
        let state = StaticState(PositionLookup::Unknown);
        let capabilities = capabilities();
        let intent = intent(command, entry_signal(order_type, Some(level)));
        let prepared = prepared(
            prepare(
                &intent,
                &PreparationContext {
                    as_of: ts(0),
                    quote: Some(&quote),
                    instrument: Some(&spec),
                    sizing_policy: SizingPolicy::FixedLot { lots: 0.2 },
                    balance_before: 10_000.0,
                    native_to_account_rate: None,
                    profile: None,
                    sizing_basis: PreparationSizingBasis::CurrentQuote,
                    state: &state,
                    capabilities: &capabilities,
                },
            )
            .unwrap(),
        );
        match &prepared.requests[0].operation {
            ExecutionOperation::Entry(entry) => assert_eq!(entry.level, Some(level)),
            other => panic!("unexpected operation: {other:?}"),
        }
        assert_eq!(
            prepared.audit.as_ref().unwrap().sizing_reference_price,
            level
        );
    }
}

#[test]
fn ongoing_profile_rules_require_an_explicit_owner() {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let profile = trailing_profile();
    let intent = intent("entry-rules", entry_signal(OrderType::Market, None));
    let mut unsupported = capabilities();
    unsupported.ongoing_management = ManagementOwner::Unsupported;
    let error = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: Some(&profile),
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &unsupported,
        },
    )
    .unwrap_err();
    assert_eq!(error, PreparationError::MissingManagementOwner);

    let external = capabilities();
    let prepared = prepared(
        prepare(
            &intent,
            &PreparationContext {
                as_of: ts(0),
                quote: Some(&quote),
                instrument: Some(&spec),
                sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
                balance_before: 10_000.0,
                native_to_account_rate: None,
                profile: Some(&profile),
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &state,
                capabilities: &external,
            },
        )
        .unwrap(),
    );
    assert_eq!(prepared.external_management.len(), 1);
    match &prepared.requests[0].operation {
        ExecutionOperation::Entry(entry) => assert!(entry.provider_rules.is_empty()),
        other => panic!("unexpected operation: {other:?}"),
    }
}

#[test]
fn ratio_close_uses_total_entered_steps_and_caps_at_the_remainder() {
    let capabilities = capabilities();
    let signal = RawSignal::ClosePartial {
        ts: ts(0),
        position: position_ref(),
        ratio: 0.5,
    };
    for (command, remaining_steps) in [("partial-1", 100), ("partial-2", 50)] {
        let state = StaticState(PositionLookup::Found(Box::new(open_snapshot(
            100,
            remaining_steps,
        ))));
        let intent = intent(command, signal.clone());
        let prepared = prepared(
            prepare(
                &intent,
                &PreparationContext {
                    as_of: ts(0),
                    quote: None,
                    instrument: None,
                    sizing_policy: default_risk_sizing(),
                    balance_before: 10_000.0,
                    native_to_account_rate: None,
                    profile: None,
                    sizing_basis: PreparationSizingBasis::CurrentQuote,
                    state: &state,
                    capabilities: &capabilities,
                },
            )
            .unwrap(),
        );
        match &prepared.requests[0].operation {
            ExecutionOperation::Close { quantity, full, .. } => {
                assert_eq!(quantity.steps, 50);
                assert_eq!(quantity.amount, 0.5);
                assert!(!full, "ClosePartial remains a partial-close command");
            }
            other => panic!("unexpected operation: {other:?}"),
        }
    }
}

#[test]
fn known_absent_close_and_cancel_complete_as_skipped() {
    let state = StaticState(PositionLookup::KnownAbsent);
    let capabilities = capabilities();
    for (command, signal) in [
        (
            "close-absent",
            RawSignal::Close {
                ts: ts(0),
                position: position_ref(),
            },
        ),
        (
            "cancel-absent",
            RawSignal::CancelPending {
                ts: ts(0),
                position: position_ref(),
            },
        ),
    ] {
        let outcome = prepare(
            &intent(command, signal),
            &PreparationContext {
                as_of: ts(0),
                quote: None,
                instrument: None,
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: None,
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &state,
                capabilities: &capabilities,
            },
        )
        .unwrap();
        let PreparationOutcome::Completed(completed) = outcome else {
            panic!("expected completed outcome")
        };
        assert!(matches!(
            completed.feedback.as_slice(),
            [CommandFeedback::Terminal {
                status: CommandTerminalStatus::Skipped,
                reason: Some(_),
                ..
            }]
        ));
    }
}

#[test]
fn cancel_and_stop_operations_use_concrete_provider_references() {
    let capabilities = capabilities();
    let cancel_state = StaticState(PositionLookup::Found(Box::new(pending_snapshot())));
    let cancel = prepared(
        prepare(
            &intent(
                "cancel-1",
                RawSignal::CancelPending {
                    ts: ts(0),
                    position: position_ref(),
                },
            ),
            &PreparationContext {
                as_of: ts(0),
                quote: None,
                instrument: None,
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: None,
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &cancel_state,
                capabilities: &capabilities,
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        &cancel.requests[0].operation,
        ExecutionOperation::CancelPending { order_id, .. } if order_id == "provider-order-1"
    ));

    let open_state = StaticState(PositionLookup::Found(Box::new(open_snapshot(100, 100))));
    let moved = prepared(
        prepare(
            &intent(
                "move-stop",
                RawSignal::MoveStoplossToEntry {
                    ts: ts(0),
                    position: position_ref(),
                },
            ),
            &PreparationContext {
                as_of: ts(0),
                quote: None,
                instrument: None,
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: None,
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &open_state,
                capabilities: &capabilities,
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        &moved.requests[0].operation,
        ExecutionOperation::ModifyStop { stoploss, .. } if (*stoploss - 1.1002).abs() < f64::EPSILON
    ));
}

#[test]
fn risk_rejection_is_typed_and_does_not_change_the_request() {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let intent = intent("risk-1", entry_signal(OrderType::Market, None));
    let prepared = prepared(
        prepare(
            &intent,
            &PreparationContext {
                as_of: ts(0),
                quote: Some(&quote),
                instrument: Some(&spec),
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: Some(1.0),
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &state,
                capabilities: &capabilities,
            },
        )
        .unwrap(),
    );
    let error = prepared
        .apply_risk_verdict(Verdict::Reject {
            policy: "halt".into(),
            reason: "daily loss limit".into(),
        })
        .unwrap_err();
    assert!(matches!(error, PreparationError::RiskRejected { .. }));
}

#[test]
fn future_quotes_and_non_trading_instruments_are_rejected() {
    let mut future_quote = quote();
    future_quote.ts = ts(1);
    let spec = instrument_spec();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let intent = intent("future-quote", entry_signal(OrderType::Market, None));
    let error = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&future_quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap_err();
    assert!(matches!(error, PreparationError::FutureQuote { .. }));

    let current_quote = quote();
    let mut halted = instrument_spec();
    halted.status = qs_instruments::ListingStatus::Halted;
    let error = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&current_quote),
            instrument: Some(&halted),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PreparationError::InstrumentUnavailable { .. }
    ));
}

#[test]
fn unsupported_signals_fail_explicitly() {
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let error = prepare(
        &intent(
            "target-1",
            RawSignal::AddTarget {
                ts: ts(0),
                position: position_ref(),
                price: 1.11,
                close_ratio: 0.5,
            },
        ),
        &PreparationContext {
            as_of: ts(0),
            quote: None,
            instrument: None,
            sizing_policy: default_risk_sizing(),
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap_err();
    assert_eq!(
        error,
        PreparationError::UnsupportedOperation {
            operation: "add_target"
        }
    );
    assert!(matches!(
        error.terminal_feedback("target-1"),
        CommandFeedback::Terminal {
            status: CommandTerminalStatus::Rejected,
            reason: Some(_),
            ..
        }
    ));
}

#[test]
fn missing_or_ambiguous_state_never_becomes_known_absence() {
    for lookup in [
        PositionLookup::Unknown,
        PositionLookup::Ambiguous { count: 2 },
    ] {
        let state = StaticState(lookup);
        let capabilities = capabilities();
        let error = prepare(
            &intent(
                "close-unknown",
                RawSignal::Close {
                    ts: ts(0),
                    position: position_ref(),
                },
            ),
            &PreparationContext {
                as_of: ts(0),
                quote: None,
                instrument: None,
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: None,
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &state,
                capabilities: &capabilities,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            PreparationError::UnknownReference | PreparationError::AmbiguousReference { .. }
        ));
    }
}

#[test]
fn public_entry_request_validation_preserves_symbol_grid_and_target_quantity() {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let intent = intent("validate-entry", entry_signal(OrderType::Market, None));
    let prepared = prepared(
        prepare(
            &intent,
            &PreparationContext {
                as_of: ts(0),
                quote: Some(&quote),
                instrument: Some(&spec),
                sizing_policy: default_risk_sizing(),
                balance_before: 10_000.0,
                native_to_account_rate: Some(1.0),
                profile: None,
                sizing_basis: PreparationSizingBasis::CurrentQuote,
                state: &state,
                capabilities: &capabilities,
            },
        )
        .unwrap(),
    );
    let mut wrong_symbol = prepared.requests[0].clone();
    let ExecutionOperation::Entry(entry) = &mut wrong_symbol.operation else {
        panic!("entry request")
    };
    entry.symbol = "GBPUSD".into();
    assert!(wrong_symbol.validate().is_err());

    let mut off_grid = prepared.requests[0].clone();
    let ExecutionOperation::Entry(entry) = &mut off_grid.operation else {
        panic!("entry request")
    };
    entry.initial_stop = Some(1.095001);
    assert!(off_grid.validate().is_err());

    let mut target_unit = prepared.requests[0].clone();
    let ExecutionOperation::Entry(entry) = &mut target_unit.operation else {
        panic!("entry request")
    };
    entry.targets[0].quantity.unit = qs_instruments::QuantityUnit::Contract;
    assert!(target_unit.validate().is_err());
}

#[test]
fn disabled_capability_refuses_before_dispatch() {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = ExecutionCapabilities::default();
    let error = prepare(
        &intent("entry-disabled", entry_signal(OrderType::Market, None)),
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap_err();
    assert_eq!(
        error,
        PreparationError::UnsupportedCapability {
            operation: "market_entry"
        }
    );
}
