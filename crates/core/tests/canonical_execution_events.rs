use std::num::NonZeroU32;

use chrono::NaiveDateTime;
use qs_core::canonical::{
    DateTimeUtc, ExecutionCommandId, FillId, TradeIntentId, VenueOrderRef, VenuePositionRef,
};
use qs_core::execution_events::*;
use qs_core::types::{
    CloseReason, Effect, ExecutionFill, Fill, FillPurpose, FutureEffect, FutureFill, Side,
};
use qs_instruments::{AssetId, Decimal, ExecutionVenueId, InstrumentId, Price, Quantity};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestCommand {
    operation: String,
    quantity: Quantity,
}

fn timestamp(value: &str) -> DateTimeUtc {
    DateTimeUtc::parse(value).unwrap()
}

fn decimal(value: &str) -> Decimal {
    value.parse().unwrap()
}

fn quantity(value: &str) -> Quantity {
    Quantity::new(decimal(value)).unwrap()
}

fn price(value: &str) -> Price {
    Price::new(decimal(value)).unwrap()
}

fn venue() -> ExecutionVenueId {
    ExecutionVenueId::new("paper").unwrap()
}

fn instrument() -> InstrumentId {
    "demo/fx_cfd/EURUSD".parse().unwrap()
}

fn intent_id() -> TradeIntentId {
    TradeIntentId::new("intent:test:1").unwrap()
}

fn command_id() -> ExecutionCommandId {
    ExecutionCommandId::new("command:intent:test:1:0").unwrap()
}

fn order_ref() -> VenueOrderRef {
    VenueOrderRef::new("order-1").unwrap()
}

fn position_ref() -> VenuePositionRef {
    VenuePositionRef::new("position-1").unwrap()
}

fn order_snapshot(
    order_quantity: &str,
    cumulative_quantity: &str,
    remaining_quantity: &str,
) -> CanonicalOrderSnapshot {
    CanonicalOrderSnapshot::new(
        order_ref(),
        Side::Buy,
        quantity(order_quantity),
        quantity(cumulative_quantity),
        quantity(remaining_quantity),
        Some(price("1.25")),
    )
    .unwrap()
}

fn fill(
    fill_id: &str,
    incremental_quantity: &str,
    cumulative_quantity: &str,
    remaining_quantity: &str,
) -> CanonicalFill {
    CanonicalFill::new(
        Some(FillId::new(fill_id).unwrap()),
        order_ref(),
        Side::Buy,
        price("1.25"),
        quantity(incremental_quantity),
        quantity(cumulative_quantity),
        quantity(remaining_quantity),
        LiquidityRole::Taker,
        Vec::new(),
    )
    .unwrap()
}

fn report(event: ExecutionEvent, sequence: Option<&str>) -> ExecutionReport {
    ExecutionReport::new(
        Some(intent_id()),
        Some(command_id()),
        venue(),
        instrument(),
        timestamp("2026-08-14T10:00:00Z"),
        timestamp("2026-08-14T10:00:01Z"),
        sequence.map(|value| VenueSequence::new(value).unwrap()),
        event,
        None,
    )
    .unwrap()
}

fn bridge_context(counter: u64) -> FutureEffectReportContext {
    FutureEffectReportContext {
        report_namespace: ReportNamespace::new("replay-run-1").unwrap(),
        report_counter: counter,
        execution_venue: venue(),
        instrument_id: instrument(),
        event_time: timestamp("2026-08-14T10:00:00Z"),
        received_at: timestamp("2026-08-14T10:00:01Z"),
        command_id: Some(command_id()),
        intent_id: Some(intent_id()),
    }
}

fn future_fill(side: Side, value: f64, size: f64, purpose: FillPurpose) -> FutureFill {
    FutureFill {
        execution: ExecutionFill {
            purpose,
            side,
            price: value,
            quote_price: value,
            requested_price: None,
            slippage_pips: 0.0,
        },
        size,
        ts: NaiveDateTime::parse_from_str("2026-08-14 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap(),
        source_quote_ts: None,
    }
}

fn reports(disposition: FutureEffectReportDisposition) -> Vec<ExecutionReport> {
    match disposition {
        FutureEffectReportDisposition::Reports(reports) => reports,
        other => panic!("expected reports, got {other:?}"),
    }
}

#[test]
fn command_and_attempt_serde_are_strict_and_keep_command_identity() {
    let created_at = timestamp("2026-08-14T10:00:00Z");
    let payload = TestCommand {
        operation: "submit".to_owned(),
        quantity: quantity("2"),
    };
    let command = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        7,
        created_at,
        payload.clone(),
    );
    let duplicate = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        7,
        created_at,
        payload.clone(),
    );
    let _: u32 = EXECUTION_SCHEMA_VERSION;
    let _: u32 = command.schema_version;
    assert_eq!(command.command_id.as_str(), "command:intent:test:1:7");
    assert_eq!(command.command_id, duplicate.command_id);
    assert_eq!(command.compare(&duplicate), CommandComparison::Duplicate);

    let changed_operation = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        7,
        created_at,
        TestCommand {
            operation: "cancel".to_owned(),
            quantity: quantity("2"),
        },
    );
    let changed_payload = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        7,
        created_at,
        TestCommand {
            operation: "submit".to_owned(),
            quantity: quantity("3"),
        },
    );
    let changed_ordinal = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        8,
        created_at,
        payload.clone(),
    );
    assert_eq!(command.command_id, changed_operation.command_id);
    assert_eq!(command.command_id, changed_payload.command_id);
    assert_ne!(command.command_id, changed_ordinal.command_id);
    assert_eq!(
        command.compare(&changed_operation),
        CommandComparison::Conflict
    );
    assert_eq!(
        command.compare(&changed_payload),
        CommandComparison::Conflict
    );

    let conflicting = ExecutionCommandEnvelope::new(
        command.command_id.clone(),
        command.intent_id.clone(),
        command.created_at,
        TestCommand {
            operation: "submit".to_owned(),
            quantity: quantity("9"),
        },
    );
    assert_eq!(command.compare(&conflicting), CommandComparison::Conflict);

    let encoded = serde_json::to_string(&command).unwrap();
    assert_eq!(
        serde_json::from_str::<ExecutionCommandEnvelope<TestCommand>>(&encoded).unwrap(),
        command
    );
    assert!(!encoded.contains("attempt"));

    let first = CommandDispatchAttempt::new(
        command.clone(),
        NonZeroU32::new(1).unwrap(),
        timestamp("2026-08-14T10:00:01Z"),
    );
    let second = CommandDispatchAttempt::new(
        command,
        NonZeroU32::new(2).unwrap(),
        timestamp("2026-08-14T10:00:02Z"),
    );
    assert_eq!(first.command.command_id, second.command.command_id);
    assert_eq!(first.command, second.command);

    let mut invalid_version = serde_json::to_value(&first.command).unwrap();
    invalid_version["schema_version"] = json!(2);
    assert!(
        serde_json::from_value::<ExecutionCommandEnvelope<TestCommand>>(invalid_version).is_err()
    );

    let mut unknown = serde_json::to_value(&first.command).unwrap();
    unknown["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ExecutionCommandEnvelope<TestCommand>>(unknown).is_err());

    let mut zero_attempt = serde_json::to_value(&first).unwrap();
    zero_attempt["attempt"] = json!(0);
    assert!(serde_json::from_value::<CommandDispatchAttempt<TestCommand>>(zero_attempt).is_err());
}

#[test]
fn command_identity_depends_only_on_intent_and_ordinal() {
    let first = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        0,
        timestamp("2026-08-14T10:00:00Z"),
        TestCommand {
            operation: "submit".to_owned(),
            quantity: quantity("2"),
        },
    );
    let changed = ExecutionCommandEnvelope::with_deterministic_id(
        intent_id(),
        0,
        timestamp("2026-08-14T10:01:00Z"),
        TestCommand {
            operation: "cancel".to_owned(),
            quantity: quantity("3"),
        },
    );

    assert_eq!(first.command_id, changed.command_id);
    assert_eq!(first.compare(&changed), CommandComparison::Conflict);
}

#[test]
fn dispatch_acknowledgement_is_not_venue_acceptance() {
    let acknowledged = CommandDispatchEvent::TransportAcknowledged {
        gateway_reference: Some(BoundedText::new("gateway-42").unwrap()),
    };
    match &acknowledged {
        CommandDispatchEvent::TransportAcknowledged {
            gateway_reference: Some(reference),
        } => assert_eq!(reference.as_str(), "gateway-42"),
        other => panic!("unexpected dispatch event: {other:?}"),
    }
    let dispatch_json = serde_json::to_string(&acknowledged).unwrap();
    assert!(dispatch_json.contains("transport_acknowledged"));
    assert!(serde_json::from_str::<ExecutionEvent>(&dispatch_json).is_err());

    let accepted = ExecutionEvent::VenueAccepted {
        order: order_snapshot("10", "0", "10"),
    };
    let accepted_json = serde_json::to_string(&accepted).unwrap();
    assert!(accepted_json.contains("venue_accepted"));
    assert!(serde_json::from_str::<CommandDispatchEvent>(&accepted_json).is_err());
}

#[test]
fn cancellation_reason_round_trips() {
    let event = ExecutionEvent::OrderCancelled {
        venue_order_ref: order_ref(),
        order: Some(order_snapshot("10", "4", "6")),
        reason: Some(BoundedText::new("cancelled by venue").unwrap()),
    };
    assert!(event.validate().is_ok());
    let decoded: ExecutionEvent =
        serde_json::from_str(&serde_json::to_string(&event).unwrap()).unwrap();
    assert_eq!(decoded, event);
    match decoded {
        ExecutionEvent::OrderCancelled { reason, .. } => {
            assert_eq!(reason.unwrap().as_str(), "cancelled by venue");
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let mismatched_order = CanonicalOrderSnapshot::new(
        VenueOrderRef::new("order-2").unwrap(),
        Side::Buy,
        quantity("10"),
        quantity("4"),
        quantity("6"),
        Some(price("1.25")),
    )
    .unwrap();
    for event in [
        ExecutionEvent::OrderCancelled {
            venue_order_ref: order_ref(),
            order: Some(mismatched_order.clone()),
            reason: None,
        },
        ExecutionEvent::OrderExpired {
            venue_order_ref: order_ref(),
            order: Some(mismatched_order),
        },
    ] {
        assert_eq!(
            event.validate().unwrap_err(),
            ExecutionEventError::OrderReferenceMismatch
        );
        assert!(
            serde_json::from_str::<ExecutionEvent>(&serde_json::to_string(&event).unwrap())
                .is_err()
        );
    }
}

#[test]
fn dispatch_reports_use_typed_identity_and_detect_conflicts() {
    let first = CommandDispatchReport::new(
        intent_id(),
        command_id(),
        venue(),
        timestamp("2026-08-14T10:00:00Z"),
        CommandDispatchEvent::TransportAcknowledged {
            gateway_reference: None,
        },
    );
    let duplicate = first.clone();
    let _: u32 = first.schema_version;
    assert_eq!(first.schema_version, EXECUTION_SCHEMA_VERSION);
    assert_eq!(first.compare(&duplicate), ReportComparison::Duplicate);

    let conflict = CommandDispatchReport::new(
        intent_id(),
        command_id(),
        venue(),
        timestamp("2026-08-14T10:00:00Z"),
        CommandDispatchEvent::TransportFailed {
            category: DispatchFailureCategory::Timeout,
            message: BoundedText::new("timed out").unwrap(),
        },
    );
    assert_eq!(first.compare(&conflict), ReportComparison::Conflict);

    let mut changed_version = first.clone();
    changed_version.schema_version = 2;
    assert_eq!(first.compare(&changed_version), ReportComparison::Conflict);

    let distinct = CommandDispatchReport::new(
        intent_id(),
        command_id(),
        venue(),
        timestamp("2026-08-14T10:00:01Z"),
        first.event.clone(),
    );
    assert_eq!(first.compare(&distinct), ReportComparison::Distinct);

    let mut invalid_version = serde_json::to_value(&first).unwrap();
    invalid_version["schema_version"] = json!(2);
    assert!(serde_json::from_value::<CommandDispatchReport>(invalid_version).is_err());

    let mut missing_version = serde_json::to_value(&first).unwrap();
    missing_version
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    assert!(serde_json::from_value::<CommandDispatchReport>(missing_version).is_err());

    let mut unknown = serde_json::to_value(&first).unwrap();
    assert!(unknown.get("report_id").is_none());
    unknown["report_id"] = json!("removed");
    assert!(serde_json::from_value::<CommandDispatchReport>(unknown).is_err());
}

#[test]
fn partial_and_final_events_carry_incremental_fills() {
    let partial = ExecutionEvent::OrderPartiallyFilled {
        fill: fill("fill-1", "4", "4", "6"),
        order: order_snapshot("10", "4", "6"),
    };
    let final_event = ExecutionEvent::OrderFilled {
        fill: fill("fill-2", "6", "10", "0"),
        order: order_snapshot("10", "10", "0"),
    };

    let partial_round_trip: ExecutionEvent =
        serde_json::from_str(&serde_json::to_string(&partial).unwrap()).unwrap();
    let final_round_trip: ExecutionEvent =
        serde_json::from_str(&serde_json::to_string(&final_event).unwrap()).unwrap();
    assert_eq!(partial_round_trip, partial);
    assert_eq!(final_round_trip, final_event);
    assert!(partial.validate().is_ok());
    assert!(final_event.validate().is_ok());

    match final_round_trip {
        ExecutionEvent::OrderFilled { fill, .. } => {
            assert_eq!(fill.quantity, quantity("6"));
            assert_eq!(fill.cumulative_quantity, quantity("10"));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let invalid_final = ExecutionEvent::OrderFilled {
        fill: fill("fill-3", "4", "4", "6"),
        order: order_snapshot("10", "4", "6"),
    };
    assert!(invalid_final.validate().is_err());
    assert!(
        serde_json::from_str::<ExecutionEvent>(&serde_json::to_string(&invalid_final).unwrap())
            .is_err()
    );
    assert!(
        ExecutionReport::new(
            Some(intent_id()),
            Some(command_id()),
            venue(),
            instrument(),
            timestamp("2026-08-14T10:00:00Z"),
            timestamp("2026-08-14T10:00:01Z"),
            None,
            invalid_final,
            None,
        )
        .is_err()
    );
}

#[test]
fn strict_values_reject_invalid_quantities_and_bounds() {
    let valid_fill = fill("fill-1", "1", "1", "0");
    let mut zero_fill = serde_json::to_value(&valid_fill).unwrap();
    zero_fill["quantity"] = json!("0");
    assert!(serde_json::from_value::<CanonicalFill>(zero_fill).is_err());

    assert!(BoundedText::new("x".repeat(MAX_BOUNDED_TEXT_BYTES + 1)).is_err());
    assert!(VenueSequence::new("sequence with spaces").is_err());
    assert!(
        FeeAmount::new(
            AssetId::new("USD").unwrap(),
            Decimal::ZERO,
            FeeType::Commission,
        )
        .is_err()
    );

    let invalid_fee = FeeAmount {
        asset: AssetId::new("USD").unwrap(),
        amount: Decimal::ZERO,
        fee_type: FeeType::Commission,
    };
    assert!(
        CanonicalFill::new(
            None,
            order_ref(),
            Side::Buy,
            price("1.25"),
            quantity("1"),
            quantity("1"),
            quantity("0"),
            LiquidityRole::Maker,
            vec![invalid_fee.clone()],
        )
        .is_err()
    );
    assert!(
        CanonicalClose::new(
            position_ref(),
            Side::Buy,
            quantity("1"),
            price("1.25"),
            CanonicalCloseReason::Manual,
            vec![invalid_fee],
        )
        .is_err()
    );

    let fee = FeeAmount::new(
        AssetId::new("USD").unwrap(),
        decimal("-0.25"),
        FeeType::Rebate,
    )
    .unwrap();
    let too_many_fill_fees = vec![fee.clone(); MAX_FILL_FEES + 1];
    assert!(
        CanonicalFill::new(
            None,
            order_ref(),
            Side::Buy,
            price("1.25"),
            quantity("1"),
            quantity("1"),
            quantity("0"),
            LiquidityRole::Maker,
            too_many_fill_fees.clone(),
        )
        .is_err()
    );
    let mut oversized_fill = valid_fill.clone();
    oversized_fill.fees = too_many_fill_fees;
    assert!(
        serde_json::from_str::<CanonicalFill>(&serde_json::to_string(&oversized_fill).unwrap())
            .is_err()
    );

    let target = CanonicalTarget::new(None, price("1.5"), quantity("1")).unwrap();
    let invalid_target = CanonicalTarget {
        target_ref: None,
        price: price("1.5"),
        quantity: quantity("0"),
    };
    assert!(
        CanonicalPositionSnapshot::new(
            position_ref(),
            Side::Buy,
            quantity("1"),
            quantity("1"),
            Some(price("1.25")),
            CanonicalProtection::default(),
            vec![invalid_target],
        )
        .is_err()
    );
    let oversized_position = CanonicalPositionSnapshot {
        venue_position_ref: position_ref(),
        side: Side::Buy,
        quantity_before: quantity("1"),
        quantity_after: quantity("1"),
        average_open_price: Some(price("1.25")),
        protection: CanonicalProtection::default(),
        targets: vec![target.clone(); MAX_POSITION_TARGETS + 1],
    };
    assert!(
        serde_json::from_str::<CanonicalPositionSnapshot>(
            &serde_json::to_string(&oversized_position).unwrap()
        )
        .is_err()
    );
    assert!(
        ExecutionEvent::PositionChanged {
            position: oversized_position,
            fill: None,
        }
        .validate()
        .is_err()
    );

    let valid_position = CanonicalPositionSnapshot::new(
        position_ref(),
        Side::Buy,
        quantity("1"),
        quantity("1"),
        Some(price("1.25")),
        CanonicalProtection::default(),
        Vec::new(),
    )
    .unwrap();
    let mut invalid_nested_fill = valid_fill;
    invalid_nested_fill.quantity = quantity("0");
    assert!(
        ExecutionEvent::PositionChanged {
            position: valid_position,
            fill: Some(invalid_nested_fill),
        }
        .validate()
        .is_err()
    );

    let mut oversized_close = CanonicalClose::new(
        position_ref(),
        Side::Buy,
        quantity("1"),
        price("1.25"),
        CanonicalCloseReason::Manual,
        Vec::new(),
    )
    .unwrap();
    oversized_close.fees = vec![fee; MAX_CLOSE_FEES + 1];
    assert!(
        serde_json::from_str::<CanonicalClose>(&serde_json::to_string(&oversized_close).unwrap())
            .is_err()
    );
    assert!(
        ExecutionEvent::PositionClosed {
            close: oversized_close,
        }
        .validate()
        .is_err()
    );

    let oversized_targets = ExecutionEvent::TargetsChanged {
        venue_position_ref: position_ref(),
        targets: vec![target; MAX_POSITION_TARGETS + 1],
    };
    assert!(oversized_targets.validate().is_err());
    assert!(
        serde_json::from_str::<ExecutionEvent>(&serde_json::to_string(&oversized_targets).unwrap())
            .is_err()
    );
}

#[test]
fn reports_use_venue_identity_and_direct_fact_comparison() {
    let event = ExecutionEvent::VenueAccepted {
        order: order_snapshot("10", "0", "10"),
    };
    let first = report(event.clone(), Some("venue-sequence-1"));
    let second = report(event, Some("venue-sequence-1"));
    assert_eq!(first.compare(&second), ReportComparison::Duplicate);

    let mut received_again = second.clone();
    received_again.received_at = timestamp("2026-08-14T10:01:00Z");
    assert_eq!(first, received_again);
    assert_eq!(first.compare(&received_again), ReportComparison::Duplicate);

    let conflict = report(
        ExecutionEvent::VenueRejected {
            category: RejectionCategory::RiskLimit,
            message: BoundedText::new("risk limit").unwrap(),
            venue_order_ref: Some(order_ref()),
        },
        Some("venue-sequence-1"),
    );
    assert_eq!(first.compare(&conflict), ReportComparison::Conflict);

    let distinct = report(
        ExecutionEvent::VenueAccepted {
            order: order_snapshot("10", "0", "10"),
        },
        Some("venue-sequence-2"),
    );
    assert_eq!(first.compare(&distinct), ReportComparison::Distinct);
}

#[test]
fn venue_fallback_keys_ignore_correlation_and_receipt_metadata() {
    let event = ExecutionEvent::VenueAccepted {
        order: order_snapshot("10", "0", "10"),
    };
    let first = report(event.clone(), Some("venue-sequence-1"));
    let differently_correlated = ExecutionReport::new(
        Some(TradeIntentId::new("intent-2").unwrap()),
        Some(ExecutionCommandId::new("command-2").unwrap()),
        venue(),
        instrument(),
        timestamp("2026-08-14T10:00:00Z"),
        timestamp("2026-08-14T10:00:02Z"),
        Some(VenueSequence::new("venue-sequence-1").unwrap()),
        event.clone(),
        None,
    )
    .unwrap();
    assert_eq!(first, differently_correlated);
    assert_eq!(
        first.venue_fallback_key(),
        differently_correlated.venue_fallback_key()
    );
    assert_eq!(
        first.compare(&differently_correlated),
        ReportComparison::Duplicate
    );

    let mut received_again = differently_correlated.clone();
    received_again.received_at = timestamp("2026-08-14T10:05:00Z");
    assert_eq!(
        differently_correlated.venue_fallback_key(),
        received_again.venue_fallback_key()
    );

    assert!(matches!(
        first.venue_fallback_key(),
        VenueEventDedupKey::Sequenced { .. }
    ));

    let changed_sequence = report(event.clone(), Some("venue-sequence-2"));
    assert_ne!(
        first.venue_fallback_key(),
        changed_sequence.venue_fallback_key()
    );

    let event_fallback = report(event, None);
    assert!(matches!(
        event_fallback.venue_fallback_key(),
        VenueEventDedupKey::Unsequenced { .. }
    ));
    let changed_event_fallback = report(
        ExecutionEvent::VenueRejected {
            category: RejectionCategory::RiskLimit,
            message: BoundedText::new("risk limit").unwrap(),
            venue_order_ref: Some(order_ref()),
        },
        None,
    );
    assert_ne!(
        event_fallback.venue_fallback_key(),
        changed_event_fallback.venue_fallback_key()
    );
}

#[test]
fn reports_without_correlation_and_reconciliation_are_valid() {
    let position = CanonicalPositionSnapshot::new(
        position_ref(),
        Side::Sell,
        quantity("2"),
        quantity("2"),
        Some(price("1.25")),
        CanonicalProtection {
            stop_loss: Some(price("1.3")),
        },
        Vec::new(),
    )
    .unwrap();
    let report = ExecutionReport::new(
        None,
        None,
        venue(),
        instrument(),
        timestamp("2026-08-14T10:00:00Z"),
        timestamp("2026-08-14T10:00:01Z"),
        None,
        ExecutionEvent::Reconciled {
            source: ReconciliationSource::VenueSnapshot,
            order: None,
            position: Some(position),
            note: Some(BoundedText::new("startup snapshot").unwrap()),
        },
        None,
    )
    .unwrap();
    let encoded = serde_json::to_string(&report).unwrap();
    assert!(!encoded.contains("report_id"));
    let decoded: ExecutionReport = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, report);
    assert!(decoded.intent_id.is_none());
    assert!(decoded.command_id.is_none());

    let empty = ExecutionEvent::Reconciled {
        source: ReconciliationSource::Operator,
        order: None,
        position: None,
        note: None,
    };
    assert!(empty.validate().is_err());
    assert!(
        serde_json::from_str::<ExecutionEvent>(&serde_json::to_string(&empty).unwrap()).is_err()
    );
    assert!(
        ExecutionReport::new(
            None,
            None,
            venue(),
            instrument(),
            timestamp("2026-08-14T10:00:00Z"),
            timestamp("2026-08-14T10:00:01Z"),
            None,
            empty,
            None,
        )
        .is_err()
    );
}

#[test]
fn report_serde_rejects_versions_unknown_fields_and_noncanonical_side() {
    let original = report(
        ExecutionEvent::VenueAccepted {
            order: order_snapshot("10", "0", "10"),
        },
        None,
    );
    let _: u32 = original.schema_version;
    let encoded = serde_json::to_string(&original).unwrap();
    assert!(encoded.contains("\"side\":\"buy\""));
    assert_eq!(
        serde_json::from_str::<ExecutionReport>(&encoded).unwrap(),
        original
    );

    let mut invalid_version = serde_json::to_value(&original).unwrap();
    invalid_version["schema_version"] = json!(0);
    assert!(serde_json::from_value::<ExecutionReport>(invalid_version).is_err());

    let mut unknown = serde_json::to_value(&original).unwrap();
    unknown["unexpected"] = json!(true);
    assert!(serde_json::from_value::<ExecutionReport>(unknown).is_err());

    let mut invalid_side = serde_json::to_value(&original).unwrap();
    invalid_side["event"]["data"]["order"]["side"] = json!("Buy");
    assert!(serde_json::from_value::<ExecutionReport>(invalid_side).is_err());
}

#[test]
fn unknown_outcome_exists_only_in_dispatch_reports() {
    let dispatch = CommandDispatchReport::new(
        intent_id(),
        command_id(),
        venue(),
        timestamp("2026-08-14T10:00:00Z"),
        CommandDispatchEvent::UnknownOutcome {
            category: DispatchFailureCategory::Connection,
            message: BoundedText::new("connection lost after send").unwrap(),
        },
    );
    let encoded = serde_json::to_string(&dispatch).unwrap();
    assert!(encoded.contains("unknown_outcome"));
    assert!(
        serde_json::from_value::<CommandDispatchReport>(serde_json::to_value(&dispatch).unwrap())
            .is_ok()
    );
    assert!(
        serde_json::from_str::<ExecutionEvent>(
            r#"{"type":"unknown_outcome","data":{"category":"connection","message":"lost"}}"#
        )
        .is_err()
    );
}

#[test]
fn bridge_maps_authoritative_open_and_scale_in_fills() {
    let open = FutureEffect::filled(
        Effect::PositionOpened {
            id: "position-1".to_owned(),
        },
        future_fill(Side::Buy, 1.25, 2.0, FillPurpose::MarketEntry),
        None,
    );
    let mapped = reports(execution_reports_from_future_effect(&open, &bridge_context(10)).unwrap());
    assert_eq!(mapped.len(), 2);
    assert!(matches!(
        mapped[0].event,
        ExecutionEvent::OrderFilled { .. }
    ));
    assert!(matches!(
        mapped[1].event,
        ExecutionEvent::PositionChanged { .. }
    ));
    assert_eq!(
        mapped[0].venue_sequence.as_ref().unwrap().as_str(),
        "replay-run-1:10"
    );
    assert_eq!(
        mapped[1].venue_sequence.as_ref().unwrap().as_str(),
        "replay-run-1:11"
    );

    let scale_in = FutureEffect::filled(
        Effect::ScaledIn {
            id: "position-1".to_owned(),
            fill: Fill {
                price: 1.3,
                size: 1.0,
                ts: NaiveDateTime::parse_from_str("2026-08-14 10:00:00", "%Y-%m-%d %H:%M:%S")
                    .unwrap(),
            },
        },
        future_fill(Side::Buy, 1.3, 1.0, FillPurpose::MarketEntry),
        None,
    );
    let mapped =
        reports(execution_reports_from_future_effect(&scale_in, &bridge_context(12)).unwrap());
    assert_eq!(mapped.len(), 1);
    assert!(matches!(
        mapped[0].event,
        ExecutionEvent::OrderFilled { .. }
    ));
}

#[test]
fn bridge_maps_close_partial_stop_and_cancel_facts() {
    let close = FutureEffect::filled(
        Effect::PositionClosed {
            id: "position-1".to_owned(),
            reason: CloseReason::Stoploss,
        },
        future_fill(Side::Buy, 1.2, 2.0, FillPurpose::StopLoss),
        None,
    );
    let close_reports =
        reports(execution_reports_from_future_effect(&close, &bridge_context(20)).unwrap());
    assert!(matches!(
        close_reports[0].event,
        ExecutionEvent::PositionClosed { .. }
    ));

    let partial = FutureEffect::filled(
        Effect::PartialClose {
            id: "position-1".to_owned(),
            ratio: 0.25,
            reason: CloseReason::Target,
        },
        future_fill(Side::Buy, 1.4, 0.5, FillPurpose::TakeProfit),
        None,
    );
    let partial_reports =
        reports(execution_reports_from_future_effect(&partial, &bridge_context(21)).unwrap());
    match &partial_reports[0].event {
        ExecutionEvent::PositionChanged { position, fill } => {
            assert_eq!(position.quantity_before, quantity("2"));
            assert_eq!(position.quantity_after, quantity("1.5"));
            assert!(fill.is_some());
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let stop = FutureEffect::plain_with_metadata(
        Effect::StoplossModified {
            id: "position-1".to_owned(),
            old_price: 1.1,
            new_price: 1.2,
        },
        Some(1.2),
        None,
    );
    let stop_reports =
        reports(execution_reports_from_future_effect(&stop, &bridge_context(22)).unwrap());
    assert!(matches!(
        stop_reports[0].event,
        ExecutionEvent::ProtectionChanged { .. }
    ));

    let cancel = FutureEffect::plain(Effect::OrderCancelled {
        id: "order-1".to_owned(),
    });
    let cancel_reports =
        reports(execution_reports_from_future_effect(&cancel, &bridge_context(23)).unwrap());
    match &cancel_reports[0].event {
        ExecutionEvent::OrderCancelled { reason, .. } => assert!(reason.is_none()),
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn bridge_does_not_invent_facts_for_informational_or_unsupported_effects() {
    let informational = FutureEffect::plain(Effect::RuleTriggered {
        id: "position-1".to_owned(),
        rule_name: "trailing".to_owned(),
    });
    assert_eq!(
        execution_reports_from_future_effect(&informational, &bridge_context(30)).unwrap(),
        FutureEffectReportDisposition::NoReport(
            FutureEffectNoReportReason::InformationalRuleEffect
        )
    );

    let placement = FutureEffect::plain(Effect::OrderPlaced {
        id: "order-1".to_owned(),
    });
    assert_eq!(
        execution_reports_from_future_effect(&placement, &bridge_context(31)).unwrap(),
        FutureEffectReportDisposition::Unsupported(
            UnsupportedFutureEffectReason::NonAuthoritativeOrderPlacement
        )
    );

    let missing_fill = FutureEffect::plain(Effect::PositionOpened {
        id: "position-1".to_owned(),
    });
    assert_eq!(
        execution_reports_from_future_effect(&missing_fill, &bridge_context(32)).unwrap(),
        FutureEffectReportDisposition::Unsupported(
            UnsupportedFutureEffectReason::MissingAuthoritativeFill
        )
    );
}
