use chrono::NaiveDateTime;
use qs_core::intent::{
    CompatibilityOnlyReason, IntentAction, IntentConstraints, IntentProducerKind, IntentProvenance,
    ManagementTargetDesiredState, OrderPreference, PriceReference, RawSignalActionClassification,
    RawSignalAdaptationContext, RawSignalAdaptationError, RawSignalAdaptationOutcome,
    RawSignalTimestampPolicy, ReductionRequest, ResolvedRawSignalTarget, RiskRequest,
    SelectorScope, TargetHint, TradeIntentRawSignalProjectionContext, adapt_raw_signal,
    project_trade_intent_to_raw_signal,
};
use qs_core::{
    DateTimeUtc, IntentCampaignRef, IntentIdentityNamespace, IntentPositionRef, IntentProducerId,
    PositionRef, RawSignal,
};
use qs_instruments::{CatalogSnapshotId, Price, ResolvedInstrumentRef, SpecRevision};
use serde_json::{Value, json};

fn instrument(listing: &str) -> ResolvedInstrumentRef {
    ResolvedInstrumentRef {
        instrument: format!("demo/fx_cfd/{listing}").parse().unwrap(),
        catalog: CatalogSnapshotId {
            version: "catalog-2026-08".to_owned(),
        },
        spec_revision: SpecRevision::new("1.0.0").unwrap(),
    }
}

fn price(value: &str) -> Price {
    value.parse().unwrap()
}

fn target(position: &str, listing: &str, pending_entry: bool) -> ResolvedRawSignalTarget {
    ResolvedRawSignalTarget {
        position: IntentPositionRef::new(position).unwrap(),
        campaign: Some(IntentCampaignRef::new(format!("campaign-{position}")).unwrap()),
        instrument: instrument(listing),
        desired_state: Some(ManagementTargetDesiredState {
            protection: None,
            targets: vec![TargetHint {
                price: PriceReference::Absolute(price("1.1")),
                close_fraction: Some(
                    qs_core::PositiveFraction::new("0.5".parse().unwrap()).unwrap(),
                ),
            }],
        }),
        pending_entry,
    }
}

fn context() -> RawSignalAdaptationContext {
    RawSignalAdaptationContext {
        timestamp_policy: RawSignalTimestampPolicy::AssumeUtc,
        base_provenance: IntentProvenance {
            producer: IntentProducerId::new("raw-signal-adapter-test").unwrap(),
            producer_kind: IntentProducerKind::LegacyUnknown,
            producer_revision: "1.0.0".to_owned(),
            correlation: None,
            source_refs: Vec::new(),
        },
        identity_namespace: IntentIdentityNamespace::new("raw-signal-test").unwrap(),
        constraints: IntentConstraints::default(),
        resolved_entry_instrument: Some(instrument("EURUSD")),
        management_targets: vec![target("position-1", "EURUSD", true)],
    }
}

fn management_projection_context() -> TradeIntentRawSignalProjectionContext {
    TradeIntentRawSignalProjectionContext {
        position: Some(PositionRef::ByTradeId {
            trade_id: "trade-1".to_owned(),
        }),
        current_target_state: context().management_targets[0].desired_state.clone(),
        ..Default::default()
    }
}

fn signal(value: Value) -> RawSignal {
    serde_json::from_value(value).unwrap()
}

fn position() -> Value {
    json!({"type":"ByTradeId", "trade_id":"trade-1"})
}

fn entry() -> RawSignal {
    signal(json!({
        "action":"Entry",
        "ts":"2026-08-14T10:00:00",
        "symbol":"EURUSD",
        "side":"Buy",
        "order_type":"Market",
        "price":null,
        "risk":1.5,
        "stoploss":1.08,
        "targets":[1.1, 1.12],
        "group":"Group with spaces",
        "trade_id":"trade-1"
    }))
}

fn intents(outcome: RawSignalAdaptationOutcome) -> Vec<qs_core::intent::TradeIntent> {
    match outcome {
        RawSignalAdaptationOutcome::Intents(intents) => intents,
        RawSignalAdaptationOutcome::CompatibilityOnly { .. } => {
            panic!("expected canonical intents")
        }
    }
}

#[test]
fn every_raw_signal_variant_has_an_explicit_disposition() {
    let signals = vec![
        entry(),
        signal(json!({"action":"Close","ts":"2026-08-14T10:01:00","position":position()})),
        signal(
            json!({"action":"ClosePartial","ts":"2026-08-14T10:02:00","position":position(),"ratio":0.5}),
        ),
        signal(
            json!({"action":"ModifyStoploss","ts":"2026-08-14T10:03:00","position":position(),"price":1.08}),
        ),
        signal(
            json!({"action":"MoveStoplossToEntry","ts":"2026-08-14T10:04:00","position":position()}),
        ),
        signal(
            json!({"action":"AddTarget","ts":"2026-08-14T10:05:00","position":position(),"price":1.12,"close_ratio":0.5}),
        ),
        signal(
            json!({"action":"RemoveTarget","ts":"2026-08-14T10:06:00","position":position(),"price":1.1}),
        ),
        signal(
            json!({"action":"ModifyTarget","ts":"2026-08-14T10:07:00","position":position(),"old_price":1.1,"new_price":1.11}),
        ),
        signal(
            json!({"action":"AddRule","ts":"2026-08-14T10:08:00","position":position(),"rule":{"type":"TimeExit","max_seconds":3600}}),
        ),
        signal(
            json!({"action":"RemoveRule","ts":"2026-08-14T10:09:00","position":position(),"rule_name":"time_exit"}),
        ),
        signal(
            json!({"action":"ScaleIn","ts":"2026-08-14T10:10:00","position":position(),"price":null,"size":0.25}),
        ),
        signal(json!({"action":"CancelPending","ts":"2026-08-14T10:11:00","position":position()})),
        signal(json!({"action":"CloseAllOf","ts":"2026-08-14T10:12:00","symbol":"EURUSD"})),
        signal(json!({"action":"CloseAll","ts":"2026-08-14T10:13:00"})),
        signal(json!({"action":"CancelAllPending","ts":"2026-08-14T10:14:00"})),
        signal(
            json!({"action":"ModifyAllStoploss","ts":"2026-08-14T10:15:00","symbol":"EURUSD","price":1.08}),
        ),
        signal(json!({"action":"CloseAllInGroup","ts":"2026-08-14T10:16:00","group_id":"group-1"})),
        signal(
            json!({"action":"ModifyAllStoplossInGroup","ts":"2026-08-14T10:17:00","group_id":"group-1","price":1.08}),
        ),
    ];

    for raw in signals {
        let classification = RawSignalActionClassification::of(&raw);
        let outcome = adapt_raw_signal(&raw, &context()).unwrap();
        match classification {
            RawSignalActionClassification::AddRule | RawSignalActionClassification::RemoveRule => {
                assert_eq!(
                    outcome,
                    RawSignalAdaptationOutcome::CompatibilityOnly {
                        action: classification,
                        reason: CompatibilityOnlyReason::RuleMutationHasNoCanonicalIntent,
                    }
                )
            }
            _ => assert!(matches!(outcome, RawSignalAdaptationOutcome::Intents(_))),
        }
    }
}

#[test]
fn entry_preserves_risk_trade_correlation_and_opaque_group() {
    let adapted = intents(adapt_raw_signal(&entry(), &context()).unwrap());
    assert_eq!(adapted.len(), 1);
    let intent = &adapted[0];
    assert_eq!(
        intent.provenance.correlation.as_ref().unwrap().as_str(),
        "trade-1"
    );
    match &intent.action {
        IntentAction::Enter(entry) => {
            assert_eq!(
                entry.risk_request,
                RiskRequest::UnitMultiplier("1.5".parse().unwrap())
            );
            assert_eq!(entry.target_hints.len(), 2);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    let projected = project_trade_intent_to_raw_signal(
        intent,
        &TradeIntentRawSignalProjectionContext {
            symbol: Some("EURUSD".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    match projected {
        RawSignal::Entry {
            risk_multiplier,
            trade_id,
            group,
            ..
        } => {
            assert_eq!(risk_multiplier, 1.5);
            assert_eq!(trade_id.as_deref(), Some("trade-1"));
            assert_eq!(group.as_deref(), Some("Group with spaces"));
        }
        action => panic!("unexpected projection: {action:?}"),
    }
}

#[test]
fn market_entry_price_round_trips_as_an_entry_reference() {
    let raw = signal(json!({
        "action":"Entry",
        "ts":"2026-08-14T10:00:00",
        "symbol":"EURUSD",
        "side":"Buy",
        "order_type":"Market",
        "price":1.085,
        "risk":1.0,
        "stoploss":null,
        "targets":[],
        "group":null,
        "trade_id":null
    }));
    let adapted = intents(adapt_raw_signal(&raw, &context()).unwrap());
    match &adapted[0].action {
        IntentAction::Enter(entry) => {
            assert_eq!(entry.order, OrderPreference::Market);
            assert_eq!(
                entry.entry_reference,
                Some(PriceReference::Absolute(price("1.085")))
            );
        }
        action => panic!("unexpected action: {action:?}"),
    }

    let projected = project_trade_intent_to_raw_signal(
        &adapted[0],
        &TradeIntentRawSignalProjectionContext {
            symbol: Some("EURUSD".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    match projected {
        RawSignal::Entry {
            order_type, price, ..
        } => {
            assert_eq!(order_type, qs_core::OrderType::Market);
            assert_eq!(price, Some(1.085));
        }
        action => panic!("unexpected projection: {action:?}"),
    }
}

#[test]
fn supplied_correlation_takes_precedence_over_entry_trade_id() {
    let mut context = context();
    context.base_provenance.correlation =
        Some(qs_core::IntentCorrelationId::new("supplied").unwrap());
    let adapted = intents(adapt_raw_signal(&entry(), &context).unwrap());
    assert_eq!(
        adapted[0].provenance.correlation.as_ref().unwrap().as_str(),
        "supplied"
    );
}

#[test]
fn target_deltas_replace_current_desired_state() {
    let add = signal(json!({
        "action":"AddTarget","ts":"2026-08-14T10:00:00",
        "position":position(),"price":1.2,"close_ratio":0.25
    }));
    let replacement = intents(adapt_raw_signal(&add, &context()).unwrap());
    match &replacement[0].action {
        IntentAction::ReplaceTargets(replace) => {
            assert_eq!(replace.targets.len(), 2);
            assert_eq!(
                replace.targets[1].price,
                PriceReference::Absolute(price("1.2"))
            );
        }
        action => panic!("unexpected action: {action:?}"),
    }

    let modify = signal(json!({
        "action":"ModifyTarget","ts":"2026-08-14T10:00:00",
        "position":position(),"old_price":1.1,"new_price":1.15
    }));
    let replacement = intents(adapt_raw_signal(&modify, &context()).unwrap());
    match &replacement[0].action {
        IntentAction::ReplaceTargets(replace) => {
            assert_eq!(replace.targets.len(), 1);
            assert_eq!(
                replace.targets[0].price,
                PriceReference::Absolute(price("1.15"))
            );
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[test]
fn target_deltas_reject_missing_state_missing_target_and_duplicate_prices() {
    let remove = signal(json!({
        "action":"RemoveTarget","ts":"2026-08-14T10:00:00",
        "position":position(),"price":1.1
    }));

    let mut missing_state = context();
    missing_state.management_targets[0].desired_state = None;
    assert_eq!(
        adapt_raw_signal(&remove, &missing_state).unwrap_err(),
        RawSignalAdaptationError::MissingTargetState
    );

    let absent = signal(json!({
        "action":"RemoveTarget","ts":"2026-08-14T10:00:00",
        "position":position(),"price":1.2
    }));
    assert_eq!(
        adapt_raw_signal(&absent, &context()).unwrap_err(),
        RawSignalAdaptationError::MissingTarget
    );

    let mut ambiguous = context();
    let state = ambiguous.management_targets[0]
        .desired_state
        .as_mut()
        .unwrap();
    state.targets.push(state.targets[0].clone());
    assert_eq!(
        adapt_raw_signal(&remove, &ambiguous).unwrap_err(),
        RawSignalAdaptationError::InvalidIntent(
            qs_core::intent::IntentValidationError::DuplicateTargetPriceReference
        )
    );
}

#[test]
fn scale_in_preserves_explicit_quantity_and_optional_price() {
    let scale = signal(json!({
        "action":"ScaleIn","ts":"2026-08-14T10:00:00",
        "position":position(),"price":1.0875,"size":0.25
    }));
    let adapted = intents(adapt_raw_signal(&scale, &context()).unwrap());
    match &adapted[0].action {
        IntentAction::AddTranche(tranche) => {
            assert_eq!(tranche.order, OrderPreference::Market);
            assert_eq!(
                tranche.entry_reference,
                Some(PriceReference::Absolute(price("1.0875")))
            );
            assert_eq!(tranche.quantity.get().to_string(), "0.25");
            tranche.quantity.require_positive().unwrap();
        }
        action => panic!("unexpected action: {action:?}"),
    }

    let projected = project_trade_intent_to_raw_signal(
        &adapted[0],
        &TradeIntentRawSignalProjectionContext {
            position: Some(PositionRef::ByTradeId {
                trade_id: "trade-1".to_owned(),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    match projected {
        RawSignal::ScaleIn { price, size, .. } => {
            assert_eq!(price, Some(1.0875));
            assert_eq!(size, 0.25);
        }
        action => panic!("unexpected projection: {action:?}"),
    }

    let mut unsupported = adapted[0].clone();
    if let IntentAction::AddTranche(tranche) = &mut unsupported.action {
        tranche.order = OrderPreference::Limit {
            limit: PriceReference::Absolute(price("1.08")),
        };
    }
    let error = project_trade_intent_to_raw_signal(
        &unsupported,
        &TradeIntentRawSignalProjectionContext {
            position: Some(PositionRef::ByTradeId {
                trade_id: "trade-1".to_owned(),
            }),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("order preference"));

    let zero = signal(json!({
        "action":"ScaleIn","ts":"2026-08-14T10:00:00",
        "position":position(),"price":null,"size":0.0
    }));
    assert!(matches!(
        adapt_raw_signal(&zero, &context()),
        Err(RawSignalAdaptationError::InvalidPositiveNumber { .. })
    ));
}

#[test]
fn bulk_dispositions_expand_exact_caller_resolved_scopes() {
    let adapt = |raw: RawSignal, targets: Vec<ResolvedRawSignalTarget>| {
        let mut context = context();
        context.management_targets = targets;
        intents(adapt_raw_signal(&raw, &context).unwrap())
    };

    let symbol_close = adapt(
        signal(json!({"action":"CloseAllOf","ts":"2026-08-14T10:00:00","symbol":"EURUSD"})),
        vec![
            target("position-b", "EURUSD", false),
            target("position-a", "EURUSD", false),
        ],
    );
    assert_eq!(symbol_close.len(), 2);
    assert!(symbol_close.iter().all(|intent| matches!(
        &intent.action,
        IntentAction::FlattenScope(flatten)
            if flatten.position.scope == SelectorScope::ExactPosition
    )));

    let group_close = adapt(
        signal(json!({"action":"CloseAllInGroup","ts":"2026-08-14T10:00:00","group_id":"group-1"})),
        vec![
            target("position-b", "GBPUSD", false),
            target("position-a", "EURUSD", false),
        ],
    );
    assert_eq!(group_close.len(), 2);
    assert!(group_close.iter().all(|intent| matches!(
        &intent.action,
        IntentAction::FlattenScope(flatten)
            if flatten.position.scope == SelectorScope::ExactPosition
    )));

    let pending_cancel = adapt(
        signal(json!({"action":"CancelAllPending","ts":"2026-08-14T10:00:00"})),
        vec![
            target("pending-b", "GBPUSD", true),
            target("pending-a", "EURUSD", true),
        ],
    );
    assert_eq!(pending_cancel.len(), 2);
    assert!(pending_cancel.iter().all(|intent| matches!(
        &intent.action,
        IntentAction::CancelEntry(cancel)
            if cancel.position.scope == SelectorScope::ExactPosition
    )));

    for protection in [
        adapt(
            signal(
                json!({"action":"ModifyAllStoploss","ts":"2026-08-14T10:00:00","symbol":"EURUSD","price":1.08}),
            ),
            vec![
                target("position-b", "EURUSD", false),
                target("position-a", "EURUSD", false),
            ],
        ),
        adapt(
            signal(
                json!({"action":"ModifyAllStoplossInGroup","ts":"2026-08-14T10:00:00","group_id":"group-1","price":1.08}),
            ),
            vec![
                target("position-b", "GBPUSD", false),
                target("position-a", "EURUSD", false),
            ],
        ),
    ] {
        assert_eq!(protection.len(), 2);
        assert!(protection.iter().all(|intent| matches!(
            &intent.action,
            IntentAction::ReplaceProtection(replacement)
                if replacement.position.scope == SelectorScope::ExactPosition
                    && matches!(
                        &replacement.protection,
                        qs_core::intent::ProtectionRequest::StopLoss {
                            stop: PriceReference::Absolute(value)
                        } if value == &price("1.08")
                    )
        )));
    }
}

#[test]
fn bulk_expansion_is_sorted_and_retry_stable() {
    let mut context = context();
    context.management_targets = vec![
        target("position-b", "GBPUSD", false),
        target("position-a", "EURUSD", false),
        target("position-a", "AUDUSD", false),
    ];
    let close_all = signal(json!({"action":"CloseAll","ts":"2026-08-14T10:00:00"}));
    let first = intents(adapt_raw_signal(&close_all, &context).unwrap());
    let retry = intents(adapt_raw_signal(&close_all, &context).unwrap());
    assert_eq!(first, retry);

    let positions = first
        .iter()
        .map(|intent| match &intent.action {
            IntentAction::FlattenScope(flatten) => (
                flatten
                    .position
                    .position
                    .as_ref()
                    .unwrap()
                    .as_str()
                    .to_owned(),
                intent.instrument.instrument.to_string(),
            ),
            action => panic!("unexpected action: {action:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        positions,
        vec![
            ("position-a".to_owned(), "demo/fx_cfd/AUDUSD".to_owned()),
            ("position-a".to_owned(), "demo/fx_cfd/EURUSD".to_owned()),
            ("position-b".to_owned(), "demo/fx_cfd/GBPUSD".to_owned()),
        ]
    );
    assert_eq!(
        first
            .iter()
            .map(|intent| intent.intent_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "intent:raw-signal-test:0",
            "intent:raw-signal-test:1",
            "intent:raw-signal-test:2",
        ]
    );
    for (left, right) in first.iter().zip(&retry) {
        assert_eq!(left.intent_id, right.intent_id);
        assert_eq!(left, right);
    }
}

#[test]
fn expanded_bulk_flatten_projects_to_per_position_close() {
    let mut context = context();
    context.management_targets = vec![
        target("position-b", "GBPUSD", false),
        target("position-a", "EURUSD", false),
    ];
    let close_all = signal(json!({"action":"CloseAll","ts":"2026-08-14T10:00:00"}));
    let expanded = intents(adapt_raw_signal(&close_all, &context).unwrap());
    assert_eq!(expanded.len(), 2);

    for intent in &expanded {
        let position = match &intent.action {
            IntentAction::FlattenScope(flatten) => flatten
                .position
                .position
                .as_ref()
                .unwrap()
                .as_str()
                .to_owned(),
            action => panic!("unexpected action: {action:?}"),
        };
        let projected = project_trade_intent_to_raw_signal(
            intent,
            &TradeIntentRawSignalProjectionContext {
                position: Some(PositionRef::ByTradeId {
                    trade_id: position.clone(),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        match projected {
            RawSignal::Close {
                position: PositionRef::ByTradeId { trade_id },
                ..
            } => assert_eq!(trade_id, position),
            action => panic!("unexpected projection: {action:?}"),
        }
    }

    for scope in [SelectorScope::Campaign, SelectorScope::Instrument] {
        let mut wider = expanded[0].clone();
        if let IntentAction::FlattenScope(flatten) = &mut wider.action {
            flatten.position.scope = scope;
            flatten.position.position = None;
            if scope == SelectorScope::Instrument {
                flatten.position.campaign = None;
            }
        }
        let error = project_trade_intent_to_raw_signal(&wider, &management_projection_context())
            .unwrap_err();
        assert!(error.to_string().contains("requires caller expansion"));
    }
}

#[test]
fn unresolved_direct_operations_error_while_empty_bulk_is_valid() {
    let mut empty = context();
    empty.management_targets.clear();
    let close = signal(json!({
        "action":"Close","ts":"2026-08-14T10:00:00","position":position()
    }));
    assert_eq!(
        adapt_raw_signal(&close, &empty).unwrap_err(),
        RawSignalAdaptationError::UnresolvedManagementTarget
    );

    let close_all = signal(json!({"action":"CloseAll","ts":"2026-08-14T10:00:00"}));
    assert!(matches!(
        adapt_raw_signal(&close_all, &empty).unwrap(),
        RawSignalAdaptationOutcome::Intents(intents) if intents.is_empty()
    ));
}

#[test]
fn cancel_pending_requires_pending_targets() {
    let cancel = signal(json!({
        "action":"CancelPending","ts":"2026-08-14T10:00:00","position":position()
    }));
    let mut open = context();
    open.management_targets[0].pending_entry = false;
    assert_eq!(
        adapt_raw_signal(&cancel, &open).unwrap_err(),
        RawSignalAdaptationError::TargetIsNotPending
    );

    let cancel_all = signal(json!({
        "action":"CancelAllPending","ts":"2026-08-14T10:00:00"
    }));
    assert!(matches!(
        adapt_raw_signal(&cancel_all, &open).unwrap(),
        RawSignalAdaptationOutcome::Intents(intents) if intents.is_empty()
    ));
}

#[test]
fn fixed_offset_timestamp_policy_is_explicit() {
    let mut context = context();
    context.timestamp_policy = RawSignalTimestampPolicy::FixedOffsetSeconds { seconds: 3600 };
    let adapted = intents(adapt_raw_signal(&entry(), &context).unwrap());
    assert_eq!(
        adapted[0].effective_at,
        DateTimeUtc::parse("2026-08-14T09:00:00Z").unwrap()
    );
}

#[test]
fn accepted_raw_signal_domain_round_trips_exactly() {
    let cases = vec![
        (
            signal(json!({
                "action":"Entry","ts":"2026-08-14T10:00:00","symbol":"EURUSD",
                "side":"Buy","order_type":"Market","price":1.085,"risk":1.0,
                "stoploss":null,"targets":[],"group":null,"trade_id":null
            })),
            TradeIntentRawSignalProjectionContext {
                symbol: Some("EURUSD".to_owned()),
                ..Default::default()
            },
        ),
        (
            signal(json!({"action":"Close","ts":"2026-08-14T10:00:00","position":position()})),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"ClosePartial","ts":"2026-08-14T10:00:00","position":position(),"ratio":0.5}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"ModifyStoploss","ts":"2026-08-14T10:00:00","position":position(),"price":1.08}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"MoveStoplossToEntry","ts":"2026-08-14T10:00:00","position":position()}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"AddTarget","ts":"2026-08-14T10:00:00","position":position(),"price":1.2,"close_ratio":0.25}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"RemoveTarget","ts":"2026-08-14T10:00:00","position":position(),"price":1.1}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"ModifyTarget","ts":"2026-08-14T10:00:00","position":position(),"old_price":1.1,"new_price":1.15}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"ScaleIn","ts":"2026-08-14T10:00:00","position":position(),"price":1.0875,"size":0.25}),
            ),
            management_projection_context(),
        ),
        (
            signal(
                json!({"action":"CancelPending","ts":"2026-08-14T10:00:00","position":position()}),
            ),
            management_projection_context(),
        ),
    ];

    for (raw, projection_context) in cases {
        let adapted = intents(adapt_raw_signal(&raw, &context()).unwrap());
        assert_eq!(adapted.len(), 1);
        let projected =
            project_trade_intent_to_raw_signal(&adapted[0], &projection_context).unwrap();
        assert_eq!(
            serde_json::to_value(projected).unwrap(),
            serde_json::to_value(raw).unwrap()
        );
    }
}

#[test]
fn target_replacement_reverse_projection_rejects_multi_delta() {
    let add = signal(json!({
        "action":"AddTarget","ts":"2026-08-14T10:00:00",
        "position":position(),"price":1.2,"close_ratio":0.25
    }));
    let mut adapted = intents(adapt_raw_signal(&add, &context()).unwrap()).remove(0);
    if let IntentAction::ReplaceTargets(replacement) = &mut adapted.action {
        replacement.targets.push(TargetHint {
            price: PriceReference::Absolute(price("1.3")),
            close_fraction: Some(qs_core::PositiveFraction::new("0.25".parse().unwrap()).unwrap()),
        });
    }
    let error = project_trade_intent_to_raw_signal(
        &adapted,
        &TradeIntentRawSignalProjectionContext {
            position: Some(PositionRef::ByTradeId {
                trade_id: "trade-1".to_owned(),
            }),
            current_target_state: context().management_targets[0].desired_state.clone(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not one add, remove, or price modification")
    );
}

#[test]
fn current_raw_signal_serde_contract_is_unchanged() {
    let raw = entry();
    let value = serde_json::to_value(&raw).unwrap();
    assert_eq!(value["action"], "Entry");
    assert_eq!(value["side"], "Buy");
    assert_eq!(value["risk"], 1.5);

    let mut unknown = value;
    unknown["future_field"] = json!(true);
    assert!(serde_json::from_value::<RawSignal>(unknown).is_err());
}

#[test]
fn fraction_and_quantity_projection_remain_exact_for_compatibility_values() {
    let partial = signal(json!({
        "action":"ClosePartial","ts":"2026-08-14T10:00:00",
        "position":position(),"ratio":0.25
    }));
    let intent = intents(adapt_raw_signal(&partial, &context()).unwrap()).remove(0);
    match &intent.action {
        IntentAction::Reduce(reduce) => match reduce.reduction {
            ReductionRequest::Fraction(fraction) => {
                assert_eq!(fraction.get().get().to_string(), "0.25")
            }
            ref reduction => panic!("unexpected reduction: {reduction:?}"),
        },
        action => panic!("unexpected action: {action:?}"),
    }
}

#[test]
fn timestamp_policy_does_not_depend_on_process_timezone() {
    let naive = NaiveDateTime::parse_from_str("2026-08-14 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
    assert_eq!(
        RawSignalTimestampPolicy::AssumeUtc.resolve(naive).unwrap(),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap()
    );
}
