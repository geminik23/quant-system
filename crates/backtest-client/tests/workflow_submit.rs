use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use qs_backtest_api::{
    BacktestEvent, BacktestProgress, BacktestResultMsg, BacktestStatusResponse,
    DeleteResultArtifactResponse, GetBacktestResultResponse, GetResultArtifactChunkResponse,
    ProviderEvaluationOptionsMsg, RESULT_FORMAT_VERSION, ResultArtifactRefMsg, ResultDeliveryMsg,
    SubmitBacktestResponse,
};
use qs_backtest_client::scripted::{
    ScriptedBacktestConnector, ScriptedConnectionScript, ScriptedConnectionStep, ScriptedRunCall,
    ScriptedSubmitGate, ScriptedWorkflowSleeper,
};
use qs_backtest_client::{
    BacktestInputInspector, BacktestPreparer, BacktestRunOptions, BacktestWorkflow,
    CompletedBacktest, FillModel, HistoricalDataType, InspectSignalInput, MemoryRunTransitionStore,
    OpenedResultFile, OutputConflictPolicy, OutputIntent, OutputTarget, PreparationCancellation,
    PrepareBacktestInput, ProfileSelection, ResultFileFormat, ResultIoLimits, ResumeRecord,
    RunStoreError, RunTransitionStore, SignalDecodingPolicy, SignalInputLimits, SignalInputSource,
    SymbolScope, WorkflowCompletion, WorkflowSleeper, WorkflowState, open_result_path,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn submit_once_completes_summary_only_and_persists_resume_record() {
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let store = MemoryRunTransitionStore::default();
    let inspect_store = store.clone();
    let workflow = BacktestWorkflow::new(connector, store);
    let handle = workflow.start("local-1", prepared().await, OutputIntent::SummaryOnly);
    let completion = handle.join().await.unwrap();
    let WorkflowCompletion::CompletedSummaryOnly(CompletedBacktest { job_id, .. }) = completion
    else {
        panic!("expected summary-only completion");
    };
    assert_eq!(job_id, "job-1");
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(|call| matches!(call, ScriptedRunCall::Submit { .. }))
            .count(),
        1
    );
    let record = inspect_store.load("local-1").await.unwrap().unwrap();
    assert_eq!(record.job_id, "job-1");
}

#[tokio::test]
async fn reconnect_is_status_first_and_keeps_the_same_job_id() {
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![]));
    let mut second = ScriptedConnectionScript::new("connection-2");
    second.push_status(Ok(completed_status("job-1")));
    second.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(first),
            ScriptedConnectionStep::client(second),
        ],
    );
    let calls = connector.clone();
    let sleeper = ScriptedWorkflowSleeper::default();
    let observed_sleeper = sleeper.clone();
    let workflow =
        BacktestWorkflow::with_sleeper(connector, MemoryRunTransitionStore::default(), sleeper);
    let completion = workflow
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    assert!(matches!(
        completion,
        WorkflowCompletion::CompletedSummaryOnly(_)
    ));
    assert_eq!(
        observed_sleeper.requested_delays(),
        vec![std::time::Duration::from_millis(500)]
    );
    let calls = calls.calls();
    let second_connect = calls
        .iter()
        .position(|call| matches!(call, ScriptedRunCall::Connect { connection_id: Some(id) } if id == "connection-2"))
        .unwrap();
    assert!(matches!(
        calls.get(second_connect + 1),
        Some(ScriptedRunCall::Status { job_id, .. }) if job_id == "job-1"
    ));
    assert!(matches!(
        calls.get(second_connect + 2),
        Some(ScriptedRunCall::Result { job_id, .. }) if job_id == "job-1"
    ));
}

#[tokio::test]
async fn resume_retries_transient_connect_and_reconciles_status_first() {
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    first.push_result(Ok(inline_result("job-1")));
    let transient =
        qs_backtest_api::BacktestClientError::Transport(qs_service::TransportFailure::new(
            qs_service::TransportFailureKind::Unavailable,
            qs_service::RetryDisposition::SafeBeforeInvocation,
            None,
            "offline",
        ));
    let mut third = ScriptedConnectionScript::new("connection-3");
    third.push_status(Ok(completed_status("job-1")));
    third.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(first),
            ScriptedConnectionStep::connect_error(transient),
            ScriptedConnectionStep::client(third),
        ],
    );
    let store = MemoryRunTransitionStore::default();
    let inspect_store = store.clone();
    let workflow =
        BacktestWorkflow::with_sleeper(connector, store, ScriptedWorkflowSleeper::default());
    assert!(matches!(
        workflow
            .start("local-1", prepared().await, OutputIntent::SummaryOnly)
            .join()
            .await
            .unwrap(),
        WorkflowCompletion::CompletedSummaryOnly(_)
    ));
    let record = inspect_store.load("local-1").await.unwrap().unwrap();
    assert!(matches!(
        workflow.resume("local-1", record).join().await.unwrap(),
        WorkflowCompletion::CompletedSummaryOnly(_)
    ));
}

#[tokio::test]
async fn active_status_after_result_retry_returns_to_watch() {
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    first.push_result(Err(qs_backtest_api::BacktestClientError::Transport(
        qs_service::TransportFailure::new(
            qs_service::TransportFailureKind::ConnectionClosed,
            qs_service::RetryDisposition::RequiresApplicationReconciliation,
            None,
            "lost",
        ),
    )));
    let mut second = ScriptedConnectionScript::new("connection-2");
    second.push_status(Ok(running_status("job-1")));
    second.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    second.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(first),
            ScriptedConnectionStep::client(second),
        ],
    );
    let calls = connector.clone();
    let workflow = BacktestWorkflow::with_sleeper(
        connector,
        MemoryRunTransitionStore::default(),
        ScriptedWorkflowSleeper::default(),
    );
    assert!(matches!(
        workflow
            .start("local-1", prepared().await, OutputIntent::SummaryOnly)
            .join()
            .await
            .unwrap(),
        WorkflowCompletion::CompletedSummaryOnly(_)
    ));
    let calls = calls.calls();
    let status = calls
        .iter()
        .position(|call| matches!(call, ScriptedRunCall::Status { connection_id, .. } if connection_id == "connection-2"))
        .unwrap();
    assert!(matches!(
        calls.get(status + 1),
        Some(ScriptedRunCall::Watch { connection_id, .. }) if connection_id == "connection-2"
    ));
}

#[tokio::test]
async fn persist_commits_before_artifact_release_and_reopens_offline() {
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = artifact_reference(&payload);
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: Some(BacktestResultMsg::default()),
        error: None,
        artifact: Some(reference.clone()),
        inline_complete: false,
        artifact_consumed: false,
    }));
    script.push_artifact_chunk(Ok(GetResultArtifactChunkResponse {
        success: true,
        artifact_id: reference.artifact_id.clone(),
        offset: 0,
        data_base64: STANDARD.encode(&payload),
        eof: true,
        error: None,
    }));
    script.push_delete_artifact(Ok(DeleteResultArtifactResponse {
        success: false,
        artifact_id: reference.artifact_id.clone(),
        error: Some("release deferred".into()),
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let store = MemoryRunTransitionStore::default();
    let inspect_store = store.clone();
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("result.json");
    let completion = BacktestWorkflow::new(connector, store)
        .start(
            "local-1",
            prepared().await,
            OutputIntent::Persist(OutputTarget {
                path: output_path.clone(),
                format: ResultFileFormat::DocumentV1,
                conflict: OutputConflictPolicy::FailIfExists,
            }),
        )
        .join()
        .await
        .unwrap();
    let WorkflowCompletion::CompletedPersisted(completed) = completion else {
        panic!("expected persisted completion");
    };
    assert_eq!(completed.warning.as_deref(), Some("release deferred"));
    assert!(matches!(
        open_result_path(&output_path, ResultIoLimits::default()).unwrap(),
        OpenedResultFile::Document(_)
    ));
    let record = inspect_store.load("local-1").await.unwrap().unwrap();
    assert_eq!(
        record.local_commit,
        qs_backtest_client::LocalCommitState::Committed
    );
    let calls = calls.calls();
    let chunk = calls
        .iter()
        .position(|call| matches!(call, ScriptedRunCall::ArtifactChunk { .. }))
        .unwrap();
    let release = calls
        .iter()
        .position(|call| matches!(call, ScriptedRunCall::DeleteArtifact { .. }))
        .unwrap();
    assert!(chunk < release);
}

#[tokio::test]
async fn committed_store_failure_leaves_artifact_and_resume_adopts_the_file() {
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = artifact_reference(&payload);
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: None,
        error: None,
        artifact: Some(reference.clone()),
        inline_complete: false,
        artifact_consumed: false,
    }));
    script.push_artifact_chunk(Ok(GetResultArtifactChunkResponse {
        success: true,
        artifact_id: reference.artifact_id.clone(),
        offset: 0,
        data_base64: STANDARD.encode(&payload),
        eof: true,
        error: None,
    }));
    let mut release = ScriptedConnectionScript::new("connection-2");
    release.push_delete_artifact(Ok(DeleteResultArtifactResponse {
        success: true,
        artifact_id: reference.artifact_id,
        error: None,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(script),
            ScriptedConnectionStep::client(release),
        ],
    );
    let calls = connector.clone();
    let store = FailCommittedStore {
        inner: MemoryRunTransitionStore::default(),
        fail_once: Arc::new(AtomicBool::new(true)),
    };
    let inspect_store = store.clone();
    let workflow = BacktestWorkflow::new(connector, store);
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("result.json");
    assert!(matches!(
        workflow
            .start(
                "local-1",
                prepared().await,
                OutputIntent::Persist(OutputTarget {
                    path: output_path.clone(),
                    format: ResultFileFormat::DocumentV1,
                    conflict: OutputConflictPolicy::FailIfExists,
                }),
            )
            .join()
            .await
            .unwrap(),
        WorkflowCompletion::Failed(_)
    ));
    assert!(output_path.is_file());
    assert!(
        !calls
            .calls()
            .iter()
            .any(|call| matches!(call, ScriptedRunCall::DeleteArtifact { .. }))
    );
    let record = inspect_store.load("local-1").await.unwrap().unwrap();
    assert_eq!(
        record.local_commit,
        qs_backtest_client::LocalCommitState::CommitPrepared
    );
    assert!(matches!(
        workflow.resume("local-1", record).join().await.unwrap(),
        WorkflowCompletion::CompletedPersisted(_)
    ));
    assert!(
        calls
            .calls()
            .iter()
            .any(|call| matches!(call, ScriptedRunCall::DeleteArtifact { .. }))
    );
}

#[tokio::test]
async fn committed_resume_rejects_tampered_local_result() {
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let store = MemoryRunTransitionStore::default();
    let inspect_store = store.clone();
    let workflow = BacktestWorkflow::new(connector, store);
    let directory = tempfile::tempdir().unwrap();
    let output_path = directory.path().join("result.json");
    assert!(matches!(
        workflow
            .start(
                "local-1",
                prepared().await,
                OutputIntent::Persist(OutputTarget {
                    path: output_path.clone(),
                    format: ResultFileFormat::DocumentV1,
                    conflict: OutputConflictPolicy::FailIfExists,
                }),
            )
            .join()
            .await
            .unwrap(),
        WorkflowCompletion::CompletedPersisted(_)
    ));
    let record = inspect_store.load("local-1").await.unwrap().unwrap();
    std::fs::write(
        &output_path,
        serde_json::to_vec(&BacktestResultMsg::default()).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        workflow.resume("local-1", record).join().await.unwrap(),
        WorkflowCompletion::Failed(_)
    ));
}

#[tokio::test]
async fn summary_only_artifact_does_not_download_or_release() {
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = artifact_reference(&payload);
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: Some(BacktestResultMsg::default()),
        error: None,
        artifact: Some(reference),
        inline_complete: false,
        artifact_consumed: false,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let completion = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default())
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    assert!(matches!(
        completion,
        WorkflowCompletion::CompletedSummaryOnly(_)
    ));
    assert!(!calls.calls().iter().any(|call| matches!(
        call,
        ScriptedRunCall::ArtifactChunk { .. } | ScriptedRunCall::DeleteArtifact { .. }
    )));
}

#[tokio::test]
async fn artifact_without_summary_finishes_output_required_without_release() {
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = artifact_reference(&payload);
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: None,
        error: None,
        artifact: Some(reference),
        inline_complete: false,
        artifact_consumed: false,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let completion = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default())
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    assert!(matches!(completion, WorkflowCompletion::OutputRequired(_)));
    assert!(!calls.calls().iter().any(|call| matches!(
        call,
        ScriptedRunCall::ArtifactChunk { .. } | ScriptedRunCall::DeleteArtifact { .. }
    )));
}

#[tokio::test]
async fn output_required_can_recover_the_same_job_with_persist() {
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = artifact_reference(&payload);
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    first.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: None,
        error: None,
        artifact: Some(reference.clone()),
        inline_complete: false,
        artifact_consumed: false,
    }));
    let mut second = ScriptedConnectionScript::new("connection-2");
    second.push_status(Ok(completed_status("job-1")));
    second.push_result(Ok(GetBacktestResultResponse {
        success: true,
        job_id: "job-1".into(),
        result: None,
        error: None,
        artifact: Some(reference.clone()),
        inline_complete: false,
        artifact_consumed: false,
    }));
    second.push_artifact_chunk(Ok(GetResultArtifactChunkResponse {
        success: true,
        artifact_id: reference.artifact_id.clone(),
        offset: 0,
        data_base64: STANDARD.encode(&payload),
        eof: true,
        error: None,
    }));
    second.push_delete_artifact(Ok(DeleteResultArtifactResponse {
        success: true,
        artifact_id: reference.artifact_id,
        error: None,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(first),
            ScriptedConnectionStep::client(second),
        ],
    );
    let calls = connector.clone();
    let workflow = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default());
    let completion = workflow
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    let WorkflowCompletion::OutputRequired(record) = completion else {
        panic!("expected output-required completion");
    };
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("recovered.json");
    let recovered = workflow
        .recover_output(
            "local-1",
            record,
            OutputTarget {
                path: output.clone(),
                format: ResultFileFormat::DocumentV1,
                conflict: OutputConflictPolicy::FailIfExists,
            },
        )
        .join()
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        WorkflowCompletion::CompletedPersisted(_)
    ));
    assert!(output.is_file());
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(|call| matches!(call, ScriptedRunCall::Submit { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn output_conflict_waits_for_save_as_without_rerunning() {
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    script.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: completed_status("job-1"),
    })]));
    script.push_result(Ok(inline_result("job-1")));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let directory = tempfile::tempdir().unwrap();
    let occupied = directory.path().join("occupied.json");
    let recovered = directory.path().join("recovered.json");
    std::fs::write(&occupied, b"owned").unwrap();
    let workflow = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default());
    let handle = workflow.start(
        "local-1",
        prepared().await,
        OutputIntent::Persist(OutputTarget {
            path: occupied.clone(),
            format: ResultFileFormat::DocumentV1,
            conflict: OutputConflictPolicy::FailIfExists,
        }),
    );
    for _ in 0..100 {
        if handle.snapshot().state == WorkflowState::CompletedAwaitingOutput {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle.snapshot().state,
        WorkflowState::CompletedAwaitingOutput
    );
    handle
        .save_as(OutputTarget {
            path: recovered.clone(),
            format: ResultFileFormat::DocumentV1,
            conflict: OutputConflictPolicy::FailIfExists,
        })
        .await
        .unwrap();
    assert!(matches!(
        handle.join().await.unwrap(),
        WorkflowCompletion::CompletedPersisted(_)
    ));
    assert_eq!(std::fs::read(occupied).unwrap(), b"owned");
    assert!(recovered.is_file());
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(|call| matches!(call, ScriptedRunCall::Submit { .. }))
            .count(),
        1
    );
}

#[derive(Clone)]
struct FailCommittedStore {
    inner: MemoryRunTransitionStore,
    fail_once: Arc<AtomicBool>,
}

#[async_trait]
impl RunTransitionStore for FailCommittedStore {
    async fn load(&self, local_run_id: &str) -> Result<Option<ResumeRecord>, RunStoreError> {
        self.inner.load(local_run_id).await
    }

    async fn compare_and_swap(
        &self,
        local_run_id: &str,
        expected_sequence: u64,
        next: ResumeRecord,
    ) -> Result<(), RunStoreError> {
        if next.local_commit == qs_backtest_client::LocalCommitState::Committed
            && self.fail_once.swap(false, Ordering::AcqRel)
        {
            return Err(RunStoreError::Poisoned);
        }
        self.inner
            .compare_and_swap(local_run_id, expected_sequence, next)
            .await
    }
}

#[derive(Clone, Default)]
struct ControlledSleeper {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl WorkflowSleeper for ControlledSleeper {
    async fn sleep(&self, _duration: std::time::Duration) {
        self.started.notify_one();
        self.release.notified().await;
    }
}

#[tokio::test]
async fn dropping_handle_stops_pending_reconnect() {
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![]));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(first)],
    );
    let calls = connector.clone();
    let sleeper = ControlledSleeper::default();
    let started = Arc::clone(&sleeper.started);
    let handle =
        BacktestWorkflow::with_sleeper(connector, MemoryRunTransitionStore::default(), sleeper)
            .start("local-1", prepared().await, OutputIntent::SummaryOnly);
    started.notified().await;
    drop(handle);
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(|call| matches!(call, ScriptedRunCall::Connect { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn cancel_interrupts_reconnect_sleep_and_is_sent_once() {
    let mut first = ScriptedConnectionScript::new("connection-1");
    first.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    first.push_watch(Ok(vec![]));
    let mut second = ScriptedConnectionScript::new("connection-2");
    second.push_status(Ok(running_status("job-1")));
    second.push_cancel(Ok(qs_backtest_api::CancelBacktestResponse {
        success: true,
        job_id: "job-1".into(),
        error: None,
    }));
    second.push_watch(Ok(vec![Ok(BacktestEvent::Snapshot {
        status: cancelled_status("job-1"),
    })]));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [
            ScriptedConnectionStep::client(first),
            ScriptedConnectionStep::client(second),
        ],
    );
    let calls = connector.clone();
    let sleeper = ControlledSleeper::default();
    let started = Arc::clone(&sleeper.started);
    let handle =
        BacktestWorkflow::with_sleeper(connector, MemoryRunTransitionStore::default(), sleeper)
            .start("local-1", prepared().await, OutputIntent::SummaryOnly);
    started.notified().await;
    handle.cancel().await.unwrap();
    assert!(matches!(
        handle.join().await.unwrap(),
        WorkflowCompletion::Cancelled
    ));
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(
                |call| matches!(call, ScriptedRunCall::Cancel { job_id, .. } if job_id == "job-1")
            )
            .count(),
        1
    );
}

#[tokio::test]
async fn cancel_during_inflight_submit_returns_submission_uncertain() {
    let gate = ScriptedSubmitGate::default();
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.set_submit_gate(gate.clone());
    script.push_submit(Ok(SubmitBacktestResponse {
        success: true,
        job_id: Some("job-1".into()),
        error: None,
    }));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let handle = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default()).start(
        "local-1",
        prepared().await,
        OutputIntent::SummaryOnly,
    );
    gate.wait_started().await;
    handle.cancel().await.unwrap();
    assert!(matches!(
        handle.join().await.unwrap(),
        WorkflowCompletion::SubmissionUncertain
    ));
}

#[tokio::test]
async fn uncertain_submission_is_terminal_and_never_retried() {
    let mut script = ScriptedConnectionScript::new("connection-1");
    script.push_submit(Err(qs_backtest_api::BacktestClientError::Transport(
        qs_service::TransportFailure::new(
            qs_service::TransportFailureKind::ConnectionClosed,
            qs_service::RetryDisposition::SafeBeforeInvocation,
            None,
            "response lost",
        ),
    )));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let completion = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default())
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    assert!(matches!(
        completion,
        WorkflowCompletion::SubmissionUncertain
    ));
    assert_eq!(
        calls
            .calls()
            .iter()
            .filter(|call| matches!(call, ScriptedRunCall::Submit { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn cancel_before_submit_is_local_cancel_without_server_call() {
    let script = ScriptedConnectionScript::new("connection-1");
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let handle = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default()).start(
        "local-1",
        prepared().await,
        OutputIntent::SummaryOnly,
    );
    handle.cancel().await.unwrap();
    assert!(matches!(
        handle.join().await.unwrap(),
        WorkflowCompletion::Cancelled
    ));
    assert!(!calls.calls().iter().any(|call| matches!(
        call,
        ScriptedRunCall::Submit { .. } | ScriptedRunCall::Cancel { .. }
    )));
}

#[tokio::test]
async fn detach_before_submit_never_calls_server_cancel() {
    let script = ScriptedConnectionScript::new("connection-1");
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::client(script)],
    );
    let calls = connector.clone();
    let handle = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default()).start(
        "local-1",
        prepared().await,
        OutputIntent::SummaryOnly,
    );
    handle.detach().await.unwrap();
    assert!(matches!(
        handle.join().await.unwrap(),
        WorkflowCompletion::Detached(None)
    ));
    assert!(!calls.calls().iter().any(|call| matches!(
        call,
        ScriptedRunCall::Submit { .. } | ScriptedRunCall::Cancel { .. }
    )));
}

#[tokio::test]
async fn initial_connect_failure_never_submits() {
    let error = qs_backtest_api::BacktestClientError::Transport(qs_service::TransportFailure::new(
        qs_service::TransportFailureKind::Unavailable,
        qs_service::RetryDisposition::SafeBeforeInvocation,
        None,
        "offline",
    ));
    let connector = ScriptedBacktestConnector::new(
        "tcp://127.0.0.1:41001",
        [ScriptedConnectionStep::connect_error(error)],
    );
    let calls = connector.clone();
    let workflow = BacktestWorkflow::new(connector, MemoryRunTransitionStore::default());
    let completion = workflow
        .start("local-1", prepared().await, OutputIntent::SummaryOnly)
        .join()
        .await
        .unwrap();
    assert!(matches!(completion, WorkflowCompletion::Failed(_)));
    assert!(
        !calls
            .calls()
            .iter()
            .any(|call| matches!(call, ScriptedRunCall::Submit { .. }))
    );
}

async fn prepared() -> qs_backtest_client::PreparedBacktest {
    let inspected = BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "signals.jsonl".into(),
                    reader: Box::new(Cursor::new(Vec::<u8>::new())),
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
    BacktestPreparer
        .prepare(
            PrepareBacktestInput {
                inspected,
                run: BacktestRunOptions {
                    symbol_scope: SymbolScope::Single("EURUSD".into()),
                    exchange: "fixture".into(),
                    data_type: HistoricalDataType::Tick,
                    timeframe: None,
                    from: None,
                    to: None,
                    profile: ProfileSelection::None,
                    account_currency: Some("USD".into()),
                    initial_balance: 10_000.0,
                    close_on_finish: true,
                    fill_model: FillModel::BidAsk,
                    sizing: None,
                    future: qs_backtest_api::FutureQuoteConfigMsg::default(),
                },
                evaluation: ProviderEvaluationOptionsMsg::default(),
                result_delivery: ResultDeliveryMsg::Auto,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap()
}

fn running_status(job_id: &str) -> BacktestStatusResponse {
    BacktestStatusResponse {
        success: true,
        job_id: job_id.into(),
        status: "Running".into(),
        error: None,
        elapsed_ms: Some(5),
        progress: BacktestProgress::default(),
    }
}

fn cancelled_status(job_id: &str) -> BacktestStatusResponse {
    BacktestStatusResponse {
        success: true,
        job_id: job_id.into(),
        status: "Cancelled".into(),
        error: None,
        elapsed_ms: Some(6),
        progress: BacktestProgress::default(),
    }
}

fn completed_status(job_id: &str) -> BacktestStatusResponse {
    BacktestStatusResponse {
        success: true,
        job_id: job_id.into(),
        status: "Completed".into(),
        error: None,
        elapsed_ms: Some(10),
        progress: BacktestProgress {
            stage: "completed".into(),
            ..BacktestProgress::default()
        },
    }
}

fn artifact_reference(payload: &[u8]) -> ResultArtifactRefMsg {
    ResultArtifactRefMsg {
        format_version: RESULT_FORMAT_VERSION,
        artifact_id: "artifact-1".into(),
        byte_len: payload.len() as u64,
        sha256: format!("{:x}", Sha256::digest(payload)),
        chunk_size: payload.len() as u64,
    }
}

fn inline_result(job_id: &str) -> GetBacktestResultResponse {
    GetBacktestResultResponse {
        success: true,
        job_id: job_id.into(),
        result: Some(BacktestResultMsg::default()),
        error: None,
        artifact: None,
        inline_complete: true,
        artifact_consumed: false,
    }
}
