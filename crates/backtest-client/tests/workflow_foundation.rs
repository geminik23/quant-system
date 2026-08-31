use std::time::Duration;

use futures::StreamExt;
use qs_backtest_api::{
    BacktestClient, BacktestProgress, BacktestStatusResponse, CancelBacktestResponse,
    DeleteResultArtifactRequest, DeleteResultArtifactResponse, FutureQuoteConfigMsg,
    GetBacktestResultResponse, GetResultArtifactChunkRequest, GetResultArtifactChunkResponse,
    ProviderEvaluationOptionsMsg, ResultDeliveryMsg, RunBacktestRequest, SubmitBacktestRequest,
    SubmitBacktestResponse,
};
use qs_backtest_client::scripted::{
    ScriptedBacktestConnector, ScriptedConnectionScript, ScriptedConnectionStep, ScriptedRunCall,
    ScriptedWorkflowSleeper,
};
use qs_backtest_client::{
    BacktestConnector, BacktestRequestSummary, ClientJobStatus, FillModel, HistoricalDataType,
    LocalCommitState, MAX_COMMAND_CAPACITY, MAX_EVENT_CAPACITY, ManagedBacktestClient,
    MemoryRunTransitionStore, OutputIntentSummary, ProfileSelectionSummary,
    RESUME_RECORD_FORMAT_VERSION, ReconnectBackoffAction, ReconnectObservation, ReconnectPolicy,
    ResultDeliverySummary, ResultInputMetadata, ResumeRecord, RunStoreError, RunTransitionStore,
    SymbolScopeSummary, WorkflowChannelConfig, WorkflowSleeper,
};

#[test]
fn channel_defaults_and_ranges_are_fixed() {
    let defaults = WorkflowChannelConfig::default();
    assert_eq!(defaults.command_capacity(), 8);
    assert_eq!(defaults.event_capacity(), 64);
    assert!(WorkflowChannelConfig::new(1, 1).is_ok());
    assert!(WorkflowChannelConfig::new(MAX_COMMAND_CAPACITY, MAX_EVENT_CAPACITY).is_ok());
    assert!(WorkflowChannelConfig::new(0, 1).is_err());
    assert!(WorkflowChannelConfig::new(MAX_COMMAND_CAPACITY + 1, 1).is_err());
    assert!(WorkflowChannelConfig::new(1, 0).is_err());
    assert!(WorkflowChannelConfig::new(1, MAX_EVENT_CAPACITY + 1).is_err());
}

#[test]
fn reconnect_policy_uses_the_fixed_capped_schedule() {
    let policy = ReconnectPolicy;
    let delays = (1..=7)
        .map(|attempt| policy.delay_for_attempt(attempt))
        .collect::<Vec<_>>();
    assert_eq!(
        delays,
        vec![
            Duration::from_millis(500),
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(10),
        ]
    );
    assert!(!policy.has_jitter());
    assert_eq!(policy.maximum_attempts(), None);
    assert_eq!(policy.maximum_elapsed(), None);
    assert!(!policy.retries_initial_connect());
    assert_eq!(
        policy.action_after(ReconnectObservation::ConnectorConnected),
        ReconnectBackoffAction::Keep
    );
    assert_eq!(
        policy.action_after(ReconnectObservation::ActiveStatus),
        ReconnectBackoffAction::Keep
    );
    assert_eq!(
        policy.action_after(ReconnectObservation::ValidWatchSnapshot),
        ReconnectBackoffAction::Reset
    );
    assert_eq!(
        policy.action_after(ReconnectObservation::ReadSucceeded),
        ReconnectBackoffAction::Reset
    );
    for observation in [
        ReconnectObservation::Detach,
        ReconnectObservation::Cancel,
        ReconnectObservation::Shutdown,
        ReconnectObservation::TerminalStatus,
        ReconnectObservation::NotFound,
    ] {
        assert_eq!(
            policy.action_after(observation),
            ReconnectBackoffAction::Stop
        );
    }
}

#[tokio::test]
async fn scripted_sleeper_records_delays_without_wall_clock_waits() {
    let sleeper = ScriptedWorkflowSleeper::default();
    sleeper.sleep(Duration::from_millis(500)).await;
    sleeper.sleep(Duration::from_secs(1)).await;
    assert_eq!(
        sleeper.requested_delays(),
        vec![Duration::from_millis(500), Duration::from_secs(1)]
    );
}

#[tokio::test]
async fn memory_transition_store_enforces_monotonic_compare_and_swap() {
    let store = MemoryRunTransitionStore::default();
    let first = resume_record(1);
    store
        .compare_and_swap("local-1", 0, first.clone())
        .await
        .unwrap();
    assert_eq!(store.load("local-1").await.unwrap(), Some(first));

    let conflict = store
        .compare_and_swap("local-1", 0, resume_record(1))
        .await
        .unwrap_err();
    assert_eq!(
        conflict,
        RunStoreError::Conflict {
            expected: 0,
            actual: 1,
        }
    );
    let invalid = store
        .compare_and_swap("local-1", 1, resume_record(3))
        .await
        .unwrap_err();
    assert_eq!(
        invalid,
        RunStoreError::InvalidNextSequence {
            expected: 2,
            actual: 3,
        }
    );
    store
        .compare_and_swap("local-1", 1, resume_record(2))
        .await
        .unwrap();
    assert_eq!(
        store.load("local-1").await.unwrap().unwrap().state_sequence,
        2
    );
}

#[tokio::test]
async fn retained_script_records_identities_and_consumes_responses_in_order() {
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_status(Ok(status("job-1", "Running")));
    script.push_watch(Ok(vec![Ok(qs_backtest_api::BacktestEvent::Snapshot {
        status: status("job-1", "Completed"),
    })]));
    script.push_result(Ok(GetBacktestResultResponse {
        success: false,
        job_id: "job-1".into(),
        result: None,
        error: Some("fixture result".into()),
        artifact: None,
        inline_complete: true,
        artifact_consumed: false,
    }));
    script.push_artifact_chunk(Ok(GetResultArtifactChunkResponse {
        success: true,
        artifact_id: "artifact-1".into(),
        offset: 12,
        data_base64: String::new(),
        eof: true,
        error: None,
    }));
    script.push_delete_artifact(Ok(DeleteResultArtifactResponse {
        success: true,
        artifact_id: "artifact-1".into(),
        error: None,
    }));
    script.push_cancel(Ok(CancelBacktestResponse {
        success: true,
        job_id: "job-1".into(),
        error: None,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let client = connector.connect().await.unwrap();
    let submit = client.submit(submit_request()).await.unwrap();
    assert_eq!(submit.job_id.as_deref(), Some("job-1"));
    assert_eq!(client.status("job-1").await.unwrap().status, "Running");
    let mut watch = client.watch("job-1").await.unwrap();
    assert!(watch.next().await.unwrap().is_ok());
    assert!(client.result("job-1").await.is_ok());
    assert!(
        client
            .get_result_artifact_chunk(GetResultArtifactChunkRequest {
                artifact_id: "artifact-1".into(),
                offset: 12,
            })
            .await
            .is_ok()
    );
    assert!(
        client
            .delete_result_artifact(DeleteResultArtifactRequest {
                artifact_id: "artifact-1".into(),
            })
            .await
            .is_ok()
    );
    assert!(client.cancel("job-1").await.is_ok());
    client.close().await.unwrap();

    let calls = connector.calls();
    assert!(matches!(
        calls[0],
        ScriptedRunCall::Connect {
            connection_id: Some(ref id)
        } if id == "connection-1"
    ));
    assert!(matches!(
        calls[1],
        ScriptedRunCall::Submit {
            ref connection_id,
            signal_count: 0,
            serialized_request_bytes,
        } if connection_id == "connection-1" && serialized_request_bytes > 0
    ));
    assert!(matches!(
        calls[2],
        ScriptedRunCall::Status { ref job_id, .. } if job_id == "job-1"
    ));
    assert!(matches!(
        calls[4],
        ScriptedRunCall::Result { ref job_id, .. } if job_id == "job-1"
    ));
    assert!(matches!(
        calls[5],
        ScriptedRunCall::ArtifactChunk {
            ref artifact_id,
            offset: 12,
            ..
        } if artifact_id == "artifact-1"
    ));
    assert!(matches!(calls.last(), Some(ScriptedRunCall::Close { .. })));
}

fn status(job_id: &str, state: &str) -> BacktestStatusResponse {
    BacktestStatusResponse {
        success: true,
        job_id: job_id.into(),
        status: state.into(),
        error: None,
        elapsed_ms: Some(1),
        progress: BacktestProgress::default(),
    }
}

fn submit_request() -> SubmitBacktestRequest {
    SubmitBacktestRequest {
        request: RunBacktestRequest {
            request: qs_backtest_api::BacktestRunSpec {
                symbol: "EURUSD".into(),
                symbols: vec![],
                all_symbols: false,
                exchange: "fixture".into(),
                data_type: "tick".into(),
                timeframe: None,
                from: None,
                to: None,
                raw_signals: vec![],
                profile: None,
                profile_def: None,
                config: qs_backtest_api::BacktestConfigMsg {
                    initial_balance: Some(10_000.0),
                    close_on_finish: Some(true),
                    fill_model: Some("BidAsk".into()),
                    sizing: None,
                },
            },
            future: FutureQuoteConfigMsg {
                account_currency: "USD".into(),
                ..FutureQuoteConfigMsg::default()
            },
            evaluation: ProviderEvaluationOptionsMsg::default(),
            result_delivery: ResultDeliveryMsg::Auto,
        },
    }
}

fn resume_record(sequence: u64) -> ResumeRecord {
    ResumeRecord {
        format_version: RESUME_RECORD_FORMAT_VERSION,
        state_sequence: sequence,
        endpoint_display: "tcp://127.0.0.1:41001".into(),
        job_id: "job-1".into(),
        output: OutputIntentSummary::SummaryOnly,
        input: ResultInputMetadata {
            display_name: "signals.jsonl".into(),
            byte_len: 10,

            signal_count: 0,
            retained_signal_count: 0,
            entry_count: 0,
            symbols: vec![],
            minimum_timestamp: None,
            maximum_timestamp: None,
        },
        request: BacktestRequestSummary {
            symbol_scope: SymbolScopeSummary::Single {
                symbol: "EURUSD".into(),
            },
            exchange: "fixture".into(),
            data_type: HistoricalDataType::Tick,
            timeframe: None,
            from: None,
            to: None,
            profile: ProfileSelectionSummary::None,
            account_currency: "USD".into(),
            initial_balance: 10_000.0,
            close_on_finish: true,
            fill_model: FillModel::BidAsk,
            signal_count: 0,
            signal_latency_ms: 0,
            slippage_pips: 0.0,
            stale_quote_after_ms: None,
            conversion_stale_after_ms: 300_000,
            result_delivery: ResultDeliverySummary::Auto,
            evaluation: ProviderEvaluationOptionsMsg::default(),
        },
        result_delivery: ResultDeliverySummary::Auto,
        submitted_at: chrono::Utc::now(),
        last_known_state: ClientJobStatus::Queued,
        local_commit: LocalCommitState::NotStarted,
        commit_intent_id: None,

        pending_artifact_id: None,
        artifact_released: false,
    }
}
