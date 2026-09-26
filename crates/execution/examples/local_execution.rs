use std::collections::{BTreeSet, VecDeque};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use futures::executor::block_on;
use qs_core::{OrderType, PositionRef, PriceQuote, RawSignal, Side, SizingPolicy};
use qs_execution::{
    ApprovalAdmission, ApprovalDecision, ApprovalGate, ApprovalMode, ConcreteQuantity,
    ExecutionCapabilities, ExecutionIntent, ExecutionOperation, ExecutionPort, ExecutionReport,
    FeedbackProjector, PortFuture, PositionLookup, PositionSnapshot, PositionSnapshotStatus,
    PreparationContext, PreparationOutcome, PreparationSizingBasis, ReportOutcome,
    ReportStreamError, StateResolver, Submission, SubmissionError, prepare,
};
use qs_instruments::{
    AssetId, Decimal, DecimalGrid, EconomicsModelId, EffectiveInterval, InstrumentAssets,
    InstrumentEconomics, InstrumentId, InstrumentSpec, ListingStatus, MarketKind, PositiveDecimal,
    PriceRules, QuantityRules, QuantityUnit,
};
use qs_risk::Verdict;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec = instrument_spec();
    let quote = PriceQuote {
        symbol: "EURUSD".into(),
        ts: ts(0),
        bid: 1.10000,
        ask: 1.10020,
    };
    let absent = FixedState(PositionLookup::Unknown);
    let capabilities = ExecutionCapabilities::all_with_external_management();
    let mut port = ScriptedPort::new(capabilities);
    let mut feedback = FeedbackProjector::new(8, 32)?;
    let mut reservations = BTreeSet::<String>::new();

    let entry = entry_intent("auto-entry")?;
    let mut automatic = ApprovalGate::new(Some(ApprovalMode::Auto), 4, 8)?;
    let ApprovalAdmission::Ready(entry) = automatic.admit(entry, ts(0))? else {
        unreachable!("automatic entry is ready")
    };
    let prepared_entry = prepared(prepare(
        &entry,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::BalanceRiskPercent { percent: 1.0 },
            balance_before: 10_000.0,
            native_to_account_rate: Some(1.0),
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &absent,
            capabilities: &capabilities,
        },
    )?);
    let prepared_entry = prepared_entry.apply_risk_verdict(Verdict::Approve)?;
    let entry_request = prepared_entry.requests[0].clone();
    feedback.register_execution(&prepared_entry)?;
    reservations.insert(entry_request.identity.request_id.clone());
    let submission = block_on(port.submit(entry_request.clone()))?;
    println!(
        "submitted {} without treating it as a fill",
        submission.request_id
    );
    let entry_steps = entry_request.expected_steps().expect("entry quantity");
    port.reports.push_back(ExecutionReport {
        observation_id: "entry-fill".into(),
        request_id: entry_request.identity.request_id.clone(),
        parent_command_id: entry_request.identity.parent_command_id.clone(),
        observed_at: ts(1),
        outcome: ReportOutcome::Fill {
            incremental_steps: entry_steps,
            cumulative_steps: entry_steps,
            price: 1.10030,
        },
    });
    let projected = feedback.process(block_on(port.next_report())?.unwrap())?;
    if projected.completion.is_some() {
        reservations.remove(&entry_request.identity.request_id);
    }
    println!("entry feedback: {:?}", projected.feedback);

    let approval_entry = entry_intent("approved-entry")?;
    let mut approval = ApprovalGate::new(
        Some(ApprovalMode::RequireEntryApproval {
            timeout: Duration::seconds(30),
        }),
        4,
        8,
    )?;
    let ApprovalAdmission::Awaiting { expires_at, .. } = approval.admit(approval_entry, ts(0))?
    else {
        unreachable!("approval mode holds entry")
    };
    println!("entry awaits approval until {expires_at}");
    let ApprovalDecision::ReadyForValidation(approved) =
        approval.approve("approved-entry", ts(1))?
    else {
        unreachable!("approved entry is released for fresh preparation")
    };
    let approved = prepared(prepare(
        &approved,
        &PreparationContext {
            as_of: ts(1),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &absent,
            capabilities: &capabilities,
        },
    )?)
    .apply_risk_verdict(Verdict::Approve)?;
    let approved_request = approved.requests[0].clone();
    feedback.register_execution(&approved)?;
    reservations.insert(approved_request.identity.request_id.clone());
    block_on(port.submit(approved_request.clone()))?;
    port.reports.push_back(ExecutionReport {
        observation_id: "approved-rejection".into(),
        request_id: approved_request.identity.request_id.clone(),
        parent_command_id: approved_request.identity.parent_command_id.clone(),
        observed_at: ts(2),
        outcome: ReportOutcome::Rejected {
            reason: "scripted provider refusal".into(),
        },
    });
    let approved_result = feedback.process(block_on(port.next_report())?.unwrap())?;
    if approved_result.completion.is_some() {
        reservations.remove(&approved_request.identity.request_id);
    }
    println!(
        "approved entry provider outcome: {:?}",
        approved_result.completion
    );

    let partial_state = FixedState(PositionLookup::Found(Box::new(position(&spec, 100, 100))));
    let partial = intent(
        "partial-close",
        RawSignal::ClosePartial {
            ts: ts(2),
            position: position_ref(),
            ratio: 0.5,
        },
    )?;
    let partial = prepared(prepare(
        &partial,
        &management_context(&partial_state, &capabilities),
    )?);
    let ExecutionOperation::Close {
        quantity: ConcreteQuantity { steps, .. },
        full,
        ..
    } = &partial.requests[0].operation
    else {
        unreachable!("partial close request")
    };
    println!("partial close requests {steps} steps; full={full}");
    let partial_request = partial.requests[0].clone();
    feedback.register_execution(&partial)?;
    block_on(port.submit(partial_request.clone()))?;
    port.reports.push_back(ExecutionReport {
        observation_id: "partial-fill".into(),
        request_id: partial_request.identity.request_id.clone(),
        parent_command_id: partial_request.identity.parent_command_id.clone(),
        observed_at: ts(3),
        outcome: ReportOutcome::Fill {
            incremental_steps: *steps,
            cumulative_steps: *steps,
            price: 1.10040,
        },
    });
    println!(
        "partial close feedback: {:?}",
        feedback
            .process(block_on(port.next_report())?.unwrap())?
            .feedback
    );

    let remainder_state = FixedState(PositionLookup::Found(Box::new(position(&spec, 100, 50))));
    let close = intent(
        "full-close",
        RawSignal::Close {
            ts: ts(3),
            position: position_ref(),
        },
    )?;
    let close = prepared(prepare(
        &close,
        &management_context(&remainder_state, &capabilities),
    )?);
    let ExecutionOperation::Close { quantity, full, .. } = &close.requests[0].operation else {
        unreachable!("full close request")
    };
    println!(
        "full close requests remaining {} steps; full={full}",
        quantity.steps
    );
    let close_request = close.requests[0].clone();
    feedback.register_execution(&close)?;
    block_on(port.submit(close_request.clone()))?;
    port.reports.push_back(ExecutionReport {
        observation_id: "full-close-fill".into(),
        request_id: close_request.identity.request_id.clone(),
        parent_command_id: close_request.identity.parent_command_id.clone(),
        observed_at: ts(4),
        outcome: ReportOutcome::Fill {
            incremental_steps: quantity.steps,
            cumulative_steps: quantity.steps,
            price: 1.10050,
        },
    });
    println!(
        "full close feedback: {:?}",
        feedback
            .process(block_on(port.next_report())?.unwrap())?
            .feedback
    );

    let mut refused_capabilities = capabilities;
    refused_capabilities.modify_stop = false;
    let stop = intent(
        "refused-stop",
        RawSignal::ModifyStoploss {
            ts: ts(4),
            position: position_ref(),
            price: 1.09900,
        },
    )?;
    let refusal = prepare(
        &stop,
        &management_context(&remainder_state, &refused_capabilities),
    )
    .unwrap_err();
    println!("explicit refusal: {refusal}");

    let unknown_prepared = prepare_fixed_entry(
        &entry_intent("unknown-entry")?,
        ts(4),
        &quote,
        &spec,
        &absent,
        &capabilities,
    )?;
    let unknown_request = unknown_prepared.requests[0].clone();
    reservations.insert(unknown_request.identity.request_id.clone());
    port.submit_errors
        .push_back(SubmissionError::AcceptanceUnknown {
            reason: "connection lost after write".into(),
        });
    let unknown = block_on(port.submit(unknown_request.clone())).unwrap_err();
    if unknown.definitely_not_submitted() {
        reservations.remove(&unknown_request.identity.request_id);
    }
    assert!(reservations.contains(&unknown_request.identity.request_id));

    let definite_prepared = prepare_fixed_entry(
        &entry_intent("definite-entry")?,
        ts(4),
        &quote,
        &spec,
        &absent,
        &capabilities,
    )?;
    let definite_request = definite_prepared.requests[0].clone();
    reservations.insert(definite_request.identity.request_id.clone());
    port.submit_errors
        .push_back(SubmissionError::DefinitelyNotSubmitted {
            reason: "local validation".into(),
        });
    let definite = block_on(port.submit(definite_request.clone())).unwrap_err();
    if definite.definitely_not_submitted() {
        reservations.remove(&definite_request.identity.request_id);
    }
    assert!(!reservations.contains(&definite_request.identity.request_id));
    println!("acceptance-unknown keeps its reservation; definite failure releases it");

    Ok(())
}

fn prepare_fixed_entry(
    intent: &ExecutionIntent,
    as_of: NaiveDateTime,
    quote: &PriceQuote,
    spec: &InstrumentSpec,
    state: &dyn StateResolver,
    capabilities: &ExecutionCapabilities,
) -> Result<qs_execution::PreparedExecution, qs_execution::PreparationError> {
    let outcome = prepare(
        intent,
        &PreparationContext {
            as_of,
            quote: Some(quote),
            instrument: Some(spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 0.1 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state,
            capabilities,
        },
    )?;
    prepared(outcome).apply_risk_verdict(Verdict::Approve)
}

fn management_context<'a>(
    state: &'a dyn StateResolver,
    capabilities: &'a ExecutionCapabilities,
) -> PreparationContext<'a> {
    PreparationContext {
        as_of: ts(4),
        quote: None,
        instrument: None,
        sizing_policy: SizingPolicy::BalanceRiskPercent { percent: 1.0 },
        balance_before: 10_000.0,
        native_to_account_rate: None,
        profile: None,
        sizing_basis: PreparationSizingBasis::CurrentQuote,
        state,
        capabilities,
    }
}

fn prepared(outcome: PreparationOutcome) -> qs_execution::PreparedExecution {
    match outcome {
        PreparationOutcome::Prepared(prepared) => *prepared,
        PreparationOutcome::Completed(_) => unreachable!("example expects active state"),
    }
}

fn intent(
    command_id: &str,
    signal: RawSignal,
) -> Result<ExecutionIntent, qs_execution::IntentError> {
    ExecutionIntent::new(
        command_id,
        "example-instance",
        "example-account",
        signal.ts(),
        signal,
    )
}

fn entry_intent(command_id: &str) -> Result<ExecutionIntent, qs_execution::IntentError> {
    intent(
        command_id,
        RawSignal::Entry {
            ts: ts(0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: Some(1.09500),
            targets: vec![1.10500],
            group: None,
            trade_id: Some(command_id.into()),
            entry_class: None,
        },
    )
}

fn position_ref() -> PositionRef {
    PositionRef::ByTradeId {
        trade_id: "auto-entry".into(),
    }
}

fn position(spec: &InstrumentSpec, total: u64, remaining: u64) -> PositionSnapshot {
    PositionSnapshot {
        position_id: "provider-position".into(),
        order_id: None,
        instrument: spec.instrument.clone(),
        symbol: "EURUSD".into(),
        side: Side::Buy,
        status: PositionSnapshotStatus::Open,
        total_entered_steps: total,
        remaining_steps: remaining,
        quantity_step: 0.01,
        quantity_unit: QuantityUnit::StandardLot,
        average_entry_price: Some(1.10030),
        price_grid: spec.price.grid,
        trade_id: Some("auto-entry".into()),
        group_id: None,
    }
}

struct FixedState(PositionLookup);

impl StateResolver for FixedState {
    fn resolve(&self, _: &PositionRef) -> PositionLookup {
        self.0.clone()
    }
}

struct ScriptedPort {
    capabilities: ExecutionCapabilities,
    requests: Vec<qs_execution::ExecutionRequest>,
    reports: VecDeque<ExecutionReport>,
    submit_errors: VecDeque<SubmissionError>,
}

impl ScriptedPort {
    fn new(capabilities: ExecutionCapabilities) -> Self {
        Self {
            capabilities,
            requests: Vec::new(),
            reports: VecDeque::new(),
            submit_errors: VecDeque::new(),
        }
    }
}

impl ExecutionPort for ScriptedPort {
    fn capabilities(&self) -> ExecutionCapabilities {
        self.capabilities
    }

    fn submit<'a>(
        &'a mut self,
        request: qs_execution::ExecutionRequest,
    ) -> PortFuture<'a, Result<Submission, SubmissionError>> {
        Box::pin(async move {
            request
                .validate()
                .map_err(|error| SubmissionError::DefinitelyNotSubmitted {
                    reason: error.to_string(),
                })?;
            if let Some(error) = self.submit_errors.pop_front() {
                return Err(error);
            }
            let request_id = request.identity.request_id.clone();
            self.requests.push(request);
            Ok(Submission {
                request_id,
                provider_reference: Some("scripted".into()),
            })
        })
    }

    fn next_report<'a>(
        &'a mut self,
    ) -> PortFuture<'a, Result<Option<ExecutionReport>, ReportStreamError>> {
        Box::pin(async move { Ok(self.reports.pop_front()) })
    }
}

fn ts(seconds: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(10, 0, seconds)
        .unwrap()
}

fn positive(value: &str) -> PositiveDecimal {
    value.parse().unwrap()
}

fn instrument_spec() -> InstrumentSpec {
    let usd = AssetId::new("USD").unwrap();
    InstrumentSpec {
        revision: "1.0.0".parse().unwrap(),
        instrument: InstrumentId::new(
            "broker-a".parse().unwrap(),
            MarketKind::new(MarketKind::FX_CFD).unwrap(),
            "EURUSD".parse().unwrap(),
        ),
        effective: EffectiveInterval::new("2026-01-01T00:00:00Z".parse().unwrap(), None).unwrap(),
        status: ListingStatus::Trading,
        assets: InstrumentAssets {
            base: Some("EUR".parse().unwrap()),
            quote: Some(usd.clone()),
            settlement: usd.clone(),
            fee_assets: BTreeSet::new(),
        },
        price: PriceRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.00001")),
            display_scale: 5,
        },
        quantity: QuantityRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.01")),
            minimum: positive("0.01"),
            maximum: Some(positive("100")),
            storage_scale: 2,
        },
        notional: None,
        economics: InstrumentEconomics {
            pnl_model: EconomicsModelId::new(EconomicsModelId::FX_QUOTE_LINEAR_V1).unwrap(),
            quantity_unit: QuantityUnit::StandardLot,
            contract_multiplier: positive("100000"),
            settlement_asset: usd,
            fee_model: None,
            funding_model: None,
            margin_model: None,
        },
        aliases: BTreeSet::from(["EURUSD".parse().unwrap()]),
    }
}
