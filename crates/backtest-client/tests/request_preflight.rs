use std::io::Cursor;

use qs_backtest_api::{
    FutureQuoteConfigMsg, MtmOutputPolicyMsg, ProviderEvaluationOptionsMsg, ResultDeliveryMsg,
    SizingPolicyMsg, SourceCoverageCountsMsg,
};
use qs_backtest_client::{
    BacktestInputInspector, BacktestPreparer, BacktestRunOptions, FillModel, HistoricalDataType,
    InspectSignalInput, PreparationCancellation, PrepareBacktestInput, ProfileSelection,
    SignalDecodingPolicy, SignalInputLimits, SignalInputSource, SymbolScope, WorkflowError,
};

#[tokio::test]
async fn preparation_builds_a_canonical_private_request_and_summary() {
    let coverage = SourceCoverageCountsMsg {
        raw_messages: 1,
        parsed_messages: 1,
        skipped_messages: 0,
        failed_messages: 0,
        emitted_signals: 1,
        emitted_entry_signals: 1,
    };
    let inspected = inspect(
        SignalInputLimits::default(),
        Some(coverage),
        Some("2026-01-15T00:00:00+02:00"),
        Some("2026-01-15T02:00:00+02:00"),
    )
    .await;
    let prepared = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: options(Some("2026-01-14T22:00:00Z"), Some("2026-01-15T00:00:00Z")),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();

    let request = &prepared.request().request;
    assert_eq!(request.request.from.as_deref(), Some("2026-01-14T22:00:00"));
    assert_eq!(request.request.to.as_deref(), Some("2026-01-15T00:00:00"));
    assert_eq!(request.request.raw_signals[0].ts(), "2026-01-14T23:00:00");
    assert_eq!(request.future.account_currency, "USD");
    assert_eq!(request.evaluation.source_coverage, Some(coverage));
    assert_eq!(prepared.request_summary().account_currency, "USD");
    assert_eq!(prepared.input_metadata().display_name, "signals.jsonl");
    assert!(prepared.serialized_request_bytes() > 0);
}

#[tokio::test]
async fn preparation_rejects_mismatched_snapshot_range_and_oversized_request() {
    let inspected = inspect(SignalInputLimits::default(), None, None, None).await;
    let mismatch = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: options(Some("2026-01-15"), None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        mismatch,
        WorkflowError::InvalidConfiguration {
            field: "date range",
            ..
        }
    ));

    let inspected = inspect(
        SignalInputLimits {
            maximum_serialized_request_bytes: 1,
            ..SignalInputLimits::default()
        },
        None,
        None,
        None,
    )
    .await;
    let oversized = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: options(None, None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        oversized,
        WorkflowError::RequestTooLarge { limit: 1, .. }
    ));

    let inspected = inspect(
        SignalInputLimits {
            maximum_serialized_request_bytes: 1_024,
            ..SignalInputLimits::default()
        },
        None,
        None,
        None,
    )
    .await;
    let mut evaluation = ProviderEvaluationOptionsMsg::default();
    evaluation.context.provider_id = Some("x".repeat(1024 * 1024));
    let large_non_signal = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: options(None, None),
                evaluation,
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        large_non_signal,
        WorkflowError::RequestTooLarge {
            actual,
            limit: 1_024,
        } if actual > 1_024
    ));
}

#[tokio::test]
async fn serialized_request_limit_is_inclusive() {
    let prepared = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected: inspect(SignalInputLimits::default(), None, None, None).await,
                run: options(None, None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    let exact = prepared.serialized_request_bytes();

    let accepted = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected: inspect(
                    SignalInputLimits {
                        maximum_serialized_request_bytes: exact,
                        ..SignalInputLimits::default()
                    },
                    None,
                    None,
                    None,
                )
                .await,
                run: options(None, None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.serialized_request_bytes(), exact);

    let rejected = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected: inspect(
                    SignalInputLimits {
                        maximum_serialized_request_bytes: exact - 1,
                        ..SignalInputLimits::default()
                    },
                    None,
                    None,
                    None,
                )
                .await,
                run: options(None, None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(rejected, WorkflowError::RequestTooLarge { .. }));
}

#[tokio::test]
async fn preparation_validates_current_futurequote_requirements() {
    let inspected = inspect(SignalInputLimits::default(), None, None, None).await;
    let mut invalid = options(None, None);
    invalid.account_currency = None;
    let currency = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: invalid,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        currency,
        WorkflowError::InvalidConfiguration {
            field: "account currency",
            ..
        }
    ));

    let inspected = inspect(SignalInputLimits::default(), None, None, None).await;
    let mut invalid = options(None, None);
    invalid.data_type = HistoricalDataType::Tick;
    invalid.timeframe = Some("1m".into());
    let timeframe = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: invalid,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        timeframe,
        WorkflowError::InvalidConfiguration {
            field: "timeframe",
            ..
        }
    ));

    let inspected = inspect(SignalInputLimits::default(), None, None, None).await;
    let mut invalid = options(None, None);
    invalid.future.mtm_output = MtmOutputPolicyMsg::Full;
    let delivery = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: invalid,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Inline,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        delivery,
        WorkflowError::InvalidConfiguration {
            field: "result delivery",
            ..
        }
    ));
}

#[tokio::test]
async fn account_currency_is_required_without_entry_signals() {
    let inspected = BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "close-only.jsonl".into(),
                    reader: Box::new(Cursor::new(
                        br#"{"action":"CloseAll","ts":"2026-01-15T01:00:00Z"}"#.to_vec(),
                    )),
                },
                source_coverage: None,
                decoding: SignalDecodingPolicy::Strict,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    let mut run = options(None, None);
    run.account_currency = None;
    run.sizing = None;
    let error = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        WorkflowError::InvalidConfiguration {
            field: "account currency",
            ..
        }
    ));
}

#[tokio::test]
async fn local_request_validation_rejects_invalid_scope_balance_sizing_and_future_values() {
    let mut cases = Vec::new();

    let mut run = options(None, None);
    run.symbol_scope = SymbolScope::Single(" ".into());
    cases.push((run, "symbol scope"));

    let mut run = options(None, None);
    run.exchange = " ".into();
    cases.push((run, "exchange"));

    let mut run = options(None, None);
    run.data_type = HistoricalDataType::Bar;
    run.timeframe = None;
    cases.push((run, "timeframe"));

    let mut run = options(None, None);
    run.initial_balance = 0.0;
    cases.push((run, "initial balance"));

    let mut run = options(None, None);
    run.sizing = Some(SizingPolicyMsg::FixedLot { lots: 0.0 });
    cases.push((run, "sizing"));

    let mut run = options(None, None);
    run.future.signal_latency_ms = -1;
    cases.push((run, "signal latency"));

    let mut run = options(None, None);
    run.future.stale_quote_after_ms = Some(-1);
    cases.push((run, "stale quote age"));

    let mut run = options(None, None);
    run.future.conversion_stale_after_ms = -1;
    cases.push((run, "conversion stale age"));

    let mut run = options(None, None);
    run.future.mtm_output = MtmOutputPolicyMsg::Bounded { max_points: 7 };
    cases.push((run, "MTM output"));

    for (run, expected_field) in cases {
        let error = BacktestPreparer
            .prepare(
                PrepareBacktestInput {
                    inspected: inspect(SignalInputLimits::default(), None, None, None).await,
                    run,
                    evaluation: ProviderEvaluationOptionsMsg::default(),
                    result_delivery: ResultDeliveryMsg::Auto,
                },
                PreparationCancellation::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            WorkflowError::InvalidConfiguration { field, .. } if field == expected_field
        ));
    }
}

#[tokio::test]
async fn multiple_and_inferred_symbol_scopes_map_without_server_effects() {
    let mut multiple = options(None, None);
    multiple.symbol_scope = SymbolScope::Multiple(vec!["EURUSD".into(), "GBPUSD".into()]);
    let prepared = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected: inspect(SignalInputLimits::default(), None, None, None).await,
                run: multiple,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    assert!(prepared.request().request.request.symbol.is_empty());
    assert_eq!(
        prepared.request().request.request.symbols,
        vec!["EURUSD", "GBPUSD"]
    );
    assert!(!prepared.request().request.request.all_symbols);

    let mut inferred = options(None, None);
    inferred.symbol_scope = SymbolScope::AllFromEntries;
    let prepared = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected: inspect(SignalInputLimits::default(), None, None, None).await,
                run: inferred,
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    assert!(prepared.request().request.request.symbol.is_empty());
    assert!(prepared.request().request.request.symbols.is_empty());
    assert!(prepared.request().request.request.all_symbols);
}

#[tokio::test]
async fn cancelled_preparation_returns_before_serialization() {
    let inspected = inspect(SignalInputLimits::default(), None, None, None).await;
    let cancellation = PreparationCancellation::default();
    cancellation.cancel();
    let error = BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: options(None, None),
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            cancellation,
        )
        .await
        .unwrap_err();
    assert_eq!(error, WorkflowError::PreparationCancelled);
}

async fn inspect(
    limits: SignalInputLimits,
    coverage: Option<SourceCoverageCountsMsg>,
    from: Option<&str>,
    to: Option<&str>,
) -> qs_backtest_client::InspectedSignalInput {
    let row = format!("{}\n", entry("2026-01-15T01:00:00+02:00"));
    BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "signals.jsonl".into(),
                    reader: Box::new(Cursor::new(row.into_bytes())),
                },
                source_coverage: coverage,
                decoding: SignalDecodingPolicy::Strict,
                limits,
                from: from.map(str::to_owned),
                to: to.map(str::to_owned),
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap()
}

fn options(from: Option<&str>, to: Option<&str>) -> BacktestRunOptions {
    BacktestRunOptions {
        symbol_scope: SymbolScope::Single("EURUSD".into()),
        exchange: "fixture".into(),
        data_type: HistoricalDataType::Tick,
        timeframe: None,
        from: from.map(str::to_owned),
        to: to.map(str::to_owned),
        profile: ProfileSelection::None,
        account_currency: Some("usd".into()),
        initial_balance: 10_000.0,
        close_on_finish: true,
        fill_model: FillModel::BidAsk,
        sizing: Some(SizingPolicyMsg::FixedLot { lots: 0.02 }),
        future: FutureQuoteConfigMsg::default(),
    }
}

fn entry(ts: &str) -> String {
    format!(
        r#"{{"action":"Entry","ts":"{ts}","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":null,"targets":[],"group":null,"trade_id":"t1"}}"#
    )
}
