use qs_core::intent::{
    AddTrancheIntent, CancelEntryIntent, EntryIntent, ExitIntent, ExpectedStateRevision,
    FlattenScopeIntent, IntentAction, IntentConstraints, IntentIdentityDisposition,
    IntentProducerKind, IntentProvenance, IntentValidationError, MAX_PROVENANCE_REFS,
    MAX_TARGET_HINTS, OrderPreference, PositionSelector, PriceReference, ProtectionRequest,
    ReduceIntent, ReductionRequest, ReplaceProtectionIntent, ReplaceTargetsIntent, RiskRequest,
    SelectorScope, TargetHint, TradeIntent, classify_intent_identity,
};
use qs_core::{
    DateTimeUtc, IntentIdentityNamespace, IntentPositionRef, IntentProducerId, OpaqueProvenanceRef,
    TradeIntentId,
};
use qs_instruments::{
    AssetId, CatalogSnapshotId, Decimal, Money, PositiveDecimal, Price, Quantity,
    ResolvedInstrumentRef, SpecRevision,
};
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

fn provenance() -> IntentProvenance {
    IntentProvenance {
        producer: IntentProducerId::new("test-producer").unwrap(),
        producer_kind: IntentProducerKind::ExternalSignal,
        producer_revision: "1.0.0".to_owned(),
        correlation: None,
        source_refs: Vec::new(),
    }
}

fn price(value: &str) -> Price {
    value.parse().unwrap()
}

fn exact_selector() -> PositionSelector {
    PositionSelector::exact(
        IntentPositionRef::new("position-1").unwrap(),
        Some(qs_core::IntentCampaignRef::new("campaign-1").unwrap()),
        instrument("EURUSD"),
    )
}

fn entry_action() -> IntentAction {
    IntentAction::Enter(EntryIntent {
        side: qs_core::Side::Buy,
        order: OrderPreference::Market,
        entry_reference: None,
        invalidation: None,
        target_hints: vec![TargetHint {
            price: qs_core::intent::PriceReference::Absolute(price("1.2")),
            close_fraction: None,
        }],
        risk_request: RiskRequest::UnitMultiplier("1.5".parse().unwrap()),
    })
}

fn intent_with_times(created_at: &str, effective_at: &str) -> TradeIntent {
    TradeIntent::with_deterministic_id(
        &IntentIdentityNamespace::new("test-namespace").unwrap(),
        0,
        instrument("EURUSD"),
        DateTimeUtc::parse(created_at).unwrap(),
        DateTimeUtc::parse(effective_at).unwrap(),
        None,
        ExpectedStateRevision::default(),
        provenance(),
        entry_action(),
        IntentConstraints::default(),
    )
    .unwrap()
}

fn intent() -> TradeIntent {
    intent_with_times("2026-08-14T10:00:00Z", "2026-08-14T10:00:00Z")
}

#[test]
fn deterministic_intent_derives_its_final_id_before_validation() {
    let namespace = IntentIdentityNamespace::new("test-namespace").unwrap();
    let existing = intent();
    let error = TradeIntent::with_deterministic_id(
        &namespace,
        0,
        instrument("EURUSD"),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        None,
        ExpectedStateRevision::default(),
        provenance(),
        entry_action(),
        IntentConstraints {
            supersedes: vec![existing.intent_id],
            ..Default::default()
        },
    )
    .unwrap_err();

    assert_eq!(error, IntentValidationError::SelfSupersession);
}

#[test]
fn intent_serde_is_strict_and_uses_snake_case_side() {
    let value = serde_json::to_value(intent()).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["action"]["type"], "enter");
    assert_eq!(value["action"]["side"], "buy");

    let mut unknown_envelope = value.clone();
    unknown_envelope["unexpected"] = json!(true);
    assert!(serde_json::from_value::<TradeIntent>(unknown_envelope).is_err());

    let mut unknown_action = value.clone();
    unknown_action["action"]["unexpected"] = json!(true);
    assert!(serde_json::from_value::<TradeIntent>(unknown_action).is_err());

    let mut legacy_side = value;
    legacy_side["action"]["side"] = json!("Buy");
    assert!(serde_json::from_value::<TradeIntent>(legacy_side).is_err());
}

#[test]
fn every_intent_action_strictly_round_trips_and_validates() {
    let half = qs_core::PositiveFraction::new("0.5".parse().unwrap()).unwrap();
    let actions = vec![
        entry_action(),
        IntentAction::Reduce(ReduceIntent {
            position: exact_selector(),
            reduction: ReductionRequest::Fraction(half),
        }),
        IntentAction::Exit(ExitIntent {
            position: exact_selector(),
        }),
        IntentAction::ReplaceProtection(ReplaceProtectionIntent {
            position: exact_selector(),
            protection: ProtectionRequest::StopLoss {
                stop: PriceReference::Absolute(price("1.08")),
            },
        }),
        IntentAction::ReplaceTargets(ReplaceTargetsIntent {
            position: exact_selector(),
            targets: vec![TargetHint {
                price: PriceReference::Absolute(price("1.2")),
                close_fraction: Some(half),
            }],
        }),
        IntentAction::AddTranche(AddTrancheIntent {
            position: exact_selector(),
            order: OrderPreference::Market,
            entry_reference: Some(PriceReference::Absolute(price("1.09"))),
            quantity: Quantity::new("0.25".parse().unwrap()).unwrap(),
        }),
        IntentAction::CancelEntry(CancelEntryIntent {
            position: exact_selector(),
        }),
        IntentAction::FlattenScope(FlattenScopeIntent {
            position: exact_selector(),
        }),
    ];
    assert_eq!(actions.len(), 8);

    for (ordinal, action) in actions.into_iter().enumerate() {
        let intent = TradeIntent::new(
            TradeIntentId::new(format!("action-{ordinal}")).unwrap(),
            instrument("EURUSD"),
            DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
            DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
            None,
            ExpectedStateRevision::default(),
            provenance(),
            action,
            IntentConstraints::default(),
        )
        .unwrap();
        intent.validate().unwrap();

        let encoded = serde_json::to_vec(&intent).unwrap();
        let decoded: TradeIntent = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, intent);
        decoded.validate().unwrap();

        let mut unknown = serde_json::to_value(&intent).unwrap();
        unknown["action"]["unexpected"] = json!(true);
        assert!(serde_json::from_value::<TradeIntent>(unknown).is_err());
    }
}

#[test]
fn intent_deserialization_rejects_version_ids_and_invalid_expiration() {
    let value = serde_json::to_value(intent()).unwrap();

    let mut unsupported = value.clone();
    unsupported["schema_version"] = json!(2);
    assert!(serde_json::from_value::<TradeIntent>(unsupported).is_err());

    let mut invalid_id = value.clone();
    invalid_id["intent_id"] = json!("intent with spaces");
    assert!(serde_json::from_value::<TradeIntent>(invalid_id).is_err());

    let mut invalid_expiration = value;
    invalid_expiration["expires_at"] = invalid_expiration["effective_at"].clone();
    assert!(serde_json::from_value::<TradeIntent>(invalid_expiration).is_err());
}

#[test]
fn created_time_may_follow_effective_time() {
    let imported = intent_with_times("2026-08-14T11:00:00Z", "2026-08-14T10:00:00Z");
    imported.validate().unwrap();
}

#[test]
fn exact_quantity_and_account_amount_must_be_positive() {
    let zero_quantity = Quantity::new(Decimal::ZERO).unwrap();
    let error = RiskRequest::ExplicitQuantity(zero_quantity)
        .validate()
        .unwrap_err();
    assert_eq!(error, IntentValidationError::NonPositiveQuantity);

    let zero_money = Money {
        asset: AssetId::new("USD").unwrap(),
        amount: Decimal::ZERO,
    };
    let error = RiskRequest::AccountAmount(zero_money)
        .validate()
        .unwrap_err();
    assert_eq!(error, IntentValidationError::NonPositiveAccountAmount);

    let selector = PositionSelector {
        position: Some(IntentPositionRef::new("position-1").unwrap()),
        campaign: None,
        instrument: Some(instrument("EURUSD")),
        scope: SelectorScope::ExactPosition,
    };
    let result = TradeIntent::new(
        TradeIntentId::new("quantity-zero").unwrap(),
        instrument("EURUSD"),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        None,
        ExpectedStateRevision::default(),
        provenance(),
        IntentAction::AddTranche(AddTrancheIntent {
            position: selector,
            order: OrderPreference::Market,
            entry_reference: None,
            quantity: zero_quantity,
        }),
        IntentConstraints::default(),
    );
    assert_eq!(
        result.unwrap_err(),
        IntentValidationError::NonPositiveQuantity
    );
}

#[test]
fn target_hint_validation_rejects_duplicate_prices_and_fraction_overflow() {
    let duplicate_targets = vec![
        TargetHint {
            price: qs_core::intent::PriceReference::Absolute(price("1.2")),
            close_fraction: Some(qs_core::PositiveFraction::new("0.4".parse().unwrap()).unwrap()),
        },
        TargetHint {
            price: qs_core::intent::PriceReference::Absolute(price("1.2")),
            close_fraction: Some(qs_core::PositiveFraction::new("0.5".parse().unwrap()).unwrap()),
        },
    ];
    let duplicate_action = IntentAction::Enter(EntryIntent {
        side: qs_core::Side::Buy,
        order: OrderPreference::Market,
        entry_reference: None,
        invalidation: None,
        target_hints: duplicate_targets,
        risk_request: RiskRequest::UnitMultiplier("1".parse().unwrap()),
    });
    let duplicate = TradeIntent::new(
        TradeIntentId::new("duplicate-target-price").unwrap(),
        instrument("EURUSD"),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        None,
        ExpectedStateRevision::default(),
        provenance(),
        duplicate_action,
        IntentConstraints::default(),
    );
    assert_eq!(
        duplicate.unwrap_err(),
        IntentValidationError::DuplicateTargetPriceReference
    );

    let excessive_action = IntentAction::Enter(EntryIntent {
        side: qs_core::Side::Buy,
        order: OrderPreference::Market,
        entry_reference: None,
        invalidation: None,
        target_hints: vec![
            TargetHint {
                price: qs_core::intent::PriceReference::Absolute(price("1.2")),
                close_fraction: Some(
                    qs_core::PositiveFraction::new("0.6".parse().unwrap()).unwrap(),
                ),
            },
            TargetHint {
                price: qs_core::intent::PriceReference::Absolute(price("1.3")),
                close_fraction: Some(
                    qs_core::PositiveFraction::new("0.5".parse().unwrap()).unwrap(),
                ),
            },
            TargetHint {
                price: qs_core::intent::PriceReference::Absolute(price("1.4")),
                close_fraction: None,
            },
        ],
        risk_request: RiskRequest::UnitMultiplier("1".parse().unwrap()),
    });
    let excessive = TradeIntent::new(
        TradeIntentId::new("excessive-target-fractions").unwrap(),
        instrument("EURUSD"),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        DateTimeUtc::parse("2026-08-14T10:00:00Z").unwrap(),
        None,
        ExpectedStateRevision::default(),
        provenance(),
        excessive_action,
        IntentConstraints::default(),
    );
    assert_eq!(
        excessive.unwrap_err(),
        IntentValidationError::TargetCloseFractionsExceedOne
    );

    let mut duplicate_json = serde_json::to_value(intent()).unwrap();
    duplicate_json["action"]["target_hints"] = json!([
        {"price":{"type":"absolute","value":"1.2"},"close_fraction":"0.4"},
        {"price":{"type":"absolute","value":"1.2"},"close_fraction":"0.5"}
    ]);
    assert!(serde_json::from_value::<TradeIntent>(duplicate_json).is_err());

    let mut excessive_json = serde_json::to_value(intent()).unwrap();
    excessive_json["action"]["target_hints"] = json!([
        {"price":{"type":"absolute","value":"1.2"},"close_fraction":"0.6"},
        {"price":{"type":"absolute","value":"1.3"},"close_fraction":"0.5"},
        {"price":{"type":"absolute","value":"1.4"},"close_fraction":null}
    ]);
    assert!(serde_json::from_value::<TradeIntent>(excessive_json).is_err());
}

#[test]
fn provenance_rejects_duplicate_source_references() {
    let source = OpaqueProvenanceRef::new("source:duplicate").unwrap();
    let mut duplicate = provenance();
    duplicate.source_refs = vec![source.clone(), source];
    assert_eq!(
        duplicate.validate().unwrap_err(),
        IntentValidationError::DuplicateCollectionValue {
            collection: "provenance source references"
        }
    );

    let mut value = serde_json::to_value(intent()).unwrap();
    value["provenance"]["source_refs"] = json!(["source:duplicate", "source:duplicate"]);
    assert!(serde_json::from_value::<TradeIntent>(value).is_err());
}

#[test]
fn all_wire_collections_reject_overflow_without_truncation() {
    let value = serde_json::to_value(intent()).unwrap();

    let mut provenance_overflow = value.clone();
    provenance_overflow["provenance"]["source_refs"] = Value::Array(
        (0..=MAX_PROVENANCE_REFS)
            .map(|index| json!(format!("source:{index}")))
            .collect(),
    );
    assert!(serde_json::from_value::<TradeIntent>(provenance_overflow).is_err());

    let mut targets_overflow = value;
    targets_overflow["action"]["target_hints"] = Value::Array(
        (0..=MAX_TARGET_HINTS)
            .map(|index| {
                json!({
                    "price": {"type":"absolute", "value": format!("1.{index:02}")},
                    "close_fraction": null
                })
            })
            .collect(),
    );
    assert!(serde_json::from_value::<TradeIntent>(targets_overflow).is_err());
}

#[test]
fn deterministic_identity_depends_only_on_namespace_and_ordinal() {
    let first = intent();
    let retry = intent();
    assert_eq!(first.intent_id.as_str(), "intent:test-namespace:0");
    assert_eq!(first.intent_id, retry.intent_id);
    assert_eq!(
        classify_intent_identity(&first, &retry),
        IntentIdentityDisposition::Duplicate
    );

    let next = TradeIntent::with_deterministic_id(
        &IntentIdentityNamespace::new("test-namespace").unwrap(),
        1,
        instrument("EURUSD"),
        first.created_at,
        first.effective_at,
        None,
        ExpectedStateRevision::default(),
        provenance(),
        entry_action(),
        IntentConstraints::default(),
    )
    .unwrap();
    assert_eq!(next.intent_id.as_str(), "intent:test-namespace:1");
    assert_ne!(first.intent_id, next.intent_id);

    let mut different_id = first.clone();
    different_id.intent_id = TradeIntentId::new("another-valid-id").unwrap();
    assert_eq!(
        classify_intent_identity(&first, &different_id),
        IntentIdentityDisposition::Distinct
    );
}

#[test]
fn same_deterministic_id_with_changed_content_is_a_conflict() {
    let original = intent();
    let changed = TradeIntent::with_deterministic_id(
        &IntentIdentityNamespace::new("test-namespace").unwrap(),
        0,
        instrument("EURUSD"),
        original.created_at,
        original.effective_at,
        None,
        ExpectedStateRevision::default(),
        provenance(),
        IntentAction::Enter(EntryIntent {
            side: qs_core::Side::Sell,
            ..match entry_action() {
                IntentAction::Enter(entry) => entry,
                _ => unreachable!(),
            }
        }),
        IntentConstraints::default(),
    )
    .unwrap();

    assert_eq!(original.intent_id, changed.intent_id);
    assert_ne!(original, changed);
    assert_eq!(
        classify_intent_identity(&original, &changed),
        IntentIdentityDisposition::Conflict
    );
}

#[test]
fn expected_state_revision_is_neutral_and_complete() {
    let incomplete = ExpectedStateRevision {
        state: Some(qs_core::IntentStateRef::new("account-state").unwrap()),
        revision: None,
    };
    assert_eq!(
        incomplete.validate().unwrap_err(),
        IntentValidationError::IncompleteExpectedState
    );

    let complete = ExpectedStateRevision {
        state: Some(qs_core::IntentStateRef::new("account-state").unwrap()),
        revision: Some(7),
    };
    complete.validate().unwrap();
}

#[test]
fn positive_decimal_contract_remains_exact() {
    let multiplier: PositiveDecimal = "1.2500".parse().unwrap();
    assert_eq!(multiplier.get().to_string(), "1.25");
}
