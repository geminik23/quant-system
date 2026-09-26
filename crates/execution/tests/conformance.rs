mod support;

use qs_backtest::{
    BacktestRunner, FutureQuoteConfig, MarketEntrySizingBasis, VecFeed, data_feed::MarketEvent,
    ledger::ActionDispositionStatus, runner::BacktestConfig,
};
use qs_core::{
    FillPurpose, OrderType, PositionRef, RawSignal, Side, SizingPolicy,
    compute_instrument_size_for_spec_with_prices,
};
use qs_execution::{
    ExecutionIntent, ExecutionOperation, ExecutionReport, FeedbackProjector, PositionLookup,
    PreparationContext, PreparationOutcome, PreparationSizingBasis, ProviderOutcome, ReportOutcome,
    prepare,
};

use qs_strategy::{CommandFact, CommandFeedback, CommandTerminalStatus};
use qs_symbols::SymbolSpec;
use support::{
    StaticState, capabilities, entry_signal, instrument_spec, open_snapshot, position_ref, quote,
    ts,
};

fn prepare_with_basis(
    basis: PreparationSizingBasis,
) -> (qs_execution::PreparedExecution, qs_core::SizingResult) {
    let spec = instrument_spec();
    let quote = quote();
    let state = StaticState(PositionLookup::Unknown);
    let capabilities = capabilities();
    let signal = entry_signal(OrderType::Market, Some(1.101));
    let intent = ExecutionIntent::new(
        "conformance-entry",
        "instance-1",
        "account-1",
        ts(0),
        signal,
    )
    .unwrap();
    let outcome = prepare(
        &intent,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedRiskAmount { amount: 100.0 },
            balance_before: 10_000.0,
            native_to_account_rate: Some(1.0),
            profile: None,
            sizing_basis: basis,
            state: &state,
            capabilities: &capabilities,
        },
    )
    .unwrap();
    let PreparationOutcome::Prepared(prepared) = outcome else {
        panic!("expected prepared request")
    };
    let sizing_reference = match basis {
        PreparationSizingBasis::CurrentQuote => quote.ask,
        PreparationSizingBasis::SignalEntryPriceWithQuoteFallback => 1.101,
    };
    let expected = compute_instrument_size_for_spec_with_prices(
        &SizingPolicy::FixedRiskAmount { amount: 100.0 },
        1.0,
        10_000.0,
        qs_core::Side::Buy,
        sizing_reference,
        quote.ask,
        Some(1.095),
        &spec,
        Some(1.0),
    )
    .unwrap();
    (*prepared, expected)
}

#[test]
fn preparation_matches_core_sizing_for_each_explicit_basis() {
    for (historical_basis, execution_basis) in [
        (
            MarketEntrySizingBasis::FillPrice,
            PreparationSizingBasis::CurrentQuote,
        ),
        (
            MarketEntrySizingBasis::SignalEntryPrice,
            PreparationSizingBasis::SignalEntryPriceWithQuoteFallback,
        ),
    ] {
        let (prepared, expected) = prepare_with_basis(execution_basis);
        let audit = prepared.audit.as_ref().unwrap();
        assert_eq!(audit.sizing.final_lot_steps, expected.final_lot_steps);
        assert_eq!(audit.sizing.final_lot, expected.final_lot);
        match (&prepared.requests[0].operation, historical_basis) {
            (ExecutionOperation::Entry(entry), MarketEntrySizingBasis::FillPrice) => {
                assert_eq!(entry.quantity.steps, expected.final_lot_steps)
            }
            (ExecutionOperation::Entry(entry), MarketEntrySizingBasis::SignalEntryPrice) => {
                assert_eq!(entry.quantity.steps, expected.final_lot_steps)
            }
            _ => panic!("unexpected request"),
        }
    }
}

#[test]
fn historical_replay_and_preparation_agree_on_fixed_entry_and_ratio_quantities() {
    let symbol_specs = [(
        "EURUSD".to_owned(),
        SymbolSpec {
            canonical: "eurusd".into(),
            pip_position: 4,
            digits: 5,
            category: "forex".into(),
            lot_base_units: 100_000,
            lot_step_units: 1_000,
            lot_min_steps: 1,
            lot_max_steps: 0,
        },
    )]
    .into_iter()
    .collect();
    let config = BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs,
        ..BacktestConfig::default()
    };
    let trade_id = "historical-trade".to_owned();
    let signals = vec![
        RawSignal::Entry {
            ts: ts(0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some(trade_id.clone()),
            entry_class: None,
        },
        RawSignal::ClosePartial {
            ts: ts(2),
            position: PositionRef::ByTradeId {
                trade_id: trade_id.clone(),
            },
            ratio: 0.5,
        },
        RawSignal::ClosePartial {
            ts: ts(4),
            position: PositionRef::ByTradeId {
                trade_id: trade_id.clone(),
            },
            ratio: 0.5,
        },
    ];
    let events = (0..7)
        .map(|second| MarketEvent::Tick {
            symbol: "EURUSD".into(),
            ts: ts(second),
            bid: 1.10000 + second as f64 * 0.00001,
            ask: 1.10020 + second as f64 * 0.00001,
        })
        .collect();
    let historical = BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .run_raw_signals_future(&mut VecFeed::new(events), signals, None);
    let historical_sizes = historical
        .recorded_fills
        .iter()
        .filter_map(|fill| match fill.fill.purpose {
            FillPurpose::MarketEntry | FillPurpose::MarketExit => Some(fill.size),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(historical_sizes, vec![1.0, 0.5, 0.5]);
    let partial_dispositions = historical
        .action_dispositions
        .iter()
        .filter(|item| item.action_kind.as_deref() == Some("close_partial"))
        .collect::<Vec<_>>();
    assert_eq!(partial_dispositions.len(), 2);
    assert!(
        partial_dispositions
            .iter()
            .all(|item| item.status == ActionDispositionStatus::Applied)
    );

    let spec = instrument_spec();
    let current_quote = quote();
    let capabilities = capabilities();
    let unknown = StaticState(PositionLookup::Unknown);
    let entry = ExecutionIntent::new(
        "historical-entry",
        "instance-1",
        "account-1",
        ts(0),
        RawSignal::Entry {
            ts: ts(0),
            symbol: "EURUSD".into(),
            side: Side::Buy,
            order_type: OrderType::Market,
            price: None,
            risk_multiplier: 1.0,
            stoploss: None,
            targets: vec![],
            group: None,
            trade_id: Some(trade_id),
            entry_class: None,
        },
    )
    .unwrap();
    let PreparationOutcome::Prepared(entry) = prepare(
        &entry,
        &PreparationContext {
            as_of: ts(0),
            quote: Some(&current_quote),
            instrument: Some(&spec),
            sizing_policy: SizingPolicy::FixedLot { lots: 1.0 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &unknown,
            capabilities: &capabilities,
        },
    )
    .unwrap() else {
        panic!("entry should prepare")
    };
    assert_eq!(entry.requests[0].expected_steps(), Some(100));

    let open = StaticState(PositionLookup::Found(Box::new(open_snapshot(100, 100))));
    let partial = ExecutionIntent::new(
        "historical-partial",
        "instance-1",
        "account-1",
        ts(2),
        RawSignal::ClosePartial {
            ts: ts(2),
            position: position_ref(),
            ratio: 0.5,
        },
    )
    .unwrap();
    let PreparationOutcome::Prepared(partial) = prepare(
        &partial,
        &PreparationContext {
            as_of: ts(2),
            quote: None,
            instrument: None,
            sizing_policy: SizingPolicy::FixedLot { lots: 1.0 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &open,
            capabilities: &capabilities,
        },
    )
    .unwrap() else {
        panic!("partial close should prepare")
    };
    assert_eq!(partial.requests[0].expected_steps(), Some(50));

    let remainder = StaticState(PositionLookup::Found(Box::new(open_snapshot(100, 50))));
    let second_partial = ExecutionIntent::new(
        "historical-partial-2",
        "instance-1",
        "account-1",
        ts(4),
        RawSignal::ClosePartial {
            ts: ts(4),
            position: position_ref(),
            ratio: 0.5,
        },
    )
    .unwrap();
    let PreparationOutcome::Prepared(second_partial) = prepare(
        &second_partial,
        &PreparationContext {
            as_of: ts(4),
            quote: None,
            instrument: None,
            sizing_policy: SizingPolicy::FixedLot { lots: 1.0 },
            balance_before: 10_000.0,
            native_to_account_rate: None,
            profile: None,
            sizing_basis: PreparationSizingBasis::CurrentQuote,
            state: &remainder,
            capabilities: &capabilities,
        },
    )
    .unwrap() else {
        panic!("second partial close should prepare")
    };
    assert_eq!(second_partial.requests[0].expected_steps(), Some(50));

    let request = second_partial.requests[0].clone();
    let mut projector = FeedbackProjector::new(2, 8).unwrap();
    projector.register_execution(&second_partial).unwrap();
    let projected = projector
        .process(ExecutionReport {
            observation_id: "historical-partial-fill".into(),
            request_id: request.identity.request_id.clone(),
            parent_command_id: request.identity.parent_command_id.clone(),
            observed_at: ts(5),
            outcome: ReportOutcome::Fill {
                incremental_steps: 50,
                cumulative_steps: 50,
                price: 1.10005,
            },
        })
        .unwrap();
    assert_eq!(
        projected.feedback,
        vec![
            CommandFeedback::Fact {
                command_id: "historical-partial-2".into(),
                fact: CommandFact::PositionReduced,
            },
            CommandFeedback::Terminal {
                command_id: "historical-partial-2".into(),
                status: CommandTerminalStatus::Applied,
                reason: None,
            },
        ]
    );
}

#[test]
fn later_fill_price_never_resizes_an_already_submitted_request() {
    let (prepared, _) = prepare_with_basis(PreparationSizingBasis::CurrentQuote);
    let request = prepared.requests[0].clone();
    let original_steps = request.expected_steps().unwrap();
    let mut projector = FeedbackProjector::new(2, 8).unwrap();
    projector.register(&request).unwrap();
    let projected = projector
        .process(ExecutionReport {
            observation_id: "fill-at-different-price".into(),
            request_id: request.identity.request_id.clone(),
            parent_command_id: request.identity.parent_command_id.clone(),
            observed_at: ts(1),
            outcome: ReportOutcome::Fill {
                incremental_steps: original_steps,
                cumulative_steps: original_steps,
                price: 1.103,
            },
        })
        .unwrap();
    assert_eq!(
        projected.completion,
        Some(ProviderOutcome::Filled {
            filled_steps: original_steps
        })
    );
    assert_eq!(request.expected_steps(), Some(original_steps));
    assert_ne!(prepared.audit.as_ref().unwrap().preparation_price, 1.103);
}
