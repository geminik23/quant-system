//! Backend bridge for the static desktop shell.
//!
//! This module owns the gpui_tokio ping actor that verifies task result
//! Entity updates, cancellation, and bounded shutdown, plus the native file
//! dialog prompts. It deliberately performs no production backtest work.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use gpui::prelude::*;
use gpui::{Context, Entity, PathPromptOptions, Task};
use gpui_tokio::Tokio;

use crate::model::{
    BacktestAppModel, ConnectionCatalogView, ConnectionFailureView, DialogOutcome, HeartbeatPayload,
};
use crate::view::BacktestWindow;

/// Heartbeat ping period. The displayed value changes at most twice per
/// second so the UI does not repaint for unchanged values.
const PING_PERIOD: Duration = Duration::from_millis(500);

/// Bridge that owns the ping task handle.
pub struct BackendBridge {
    heartbeat_task: Option<Task<()>>,
}

impl BackendBridge {
    pub fn new() -> Self {
        Self {
            heartbeat_task: None,
        }
    }

    /// Start a ping session. The loop spawns one bounded Tokio task per
    /// heartbeat, awaits its result on the main thread, and applies it to the
    /// model entity under a generation guard.
    pub fn start_heartbeat(
        &mut self,
        model: &Entity<BacktestAppModel>,
        cx: &mut Context<BacktestWindow>,
    ) {
        self.stop_heartbeat(model, cx);
        let generation = model.update(cx, |model, _| model.begin_ping_session());
        let model = model.downgrade();
        let period = PING_PERIOD;
        self.heartbeat_task = Some(cx.spawn(async move |_this, cx| {
            loop {
                let started = Instant::now();
                let ping = Tokio::spawn(cx, async move {
                    tokio::time::sleep(period).await;
                    HeartbeatPayload {
                        round_trip: started.elapsed(),
                    }
                });
                match ping.await {
                    Ok(payload) => {
                        let Some(model) = model.upgrade() else {
                            break;
                        };
                        let accepted = cx.update_entity(&model, |model, cx| {
                            if model.apply_heartbeat(generation, payload) {
                                cx.notify();
                                true
                            } else {
                                false
                            }
                        });
                        if !accepted {
                            break;
                        }
                    }
                    Err(_join_error) => break,
                }
            }
        }));
    }

    /// Stop the ping session. Dropping the held task aborts the in-flight
    /// Tokio task, which is the bounded shutdown contract being verified.
    pub fn stop_heartbeat(
        &mut self,
        model: &Entity<BacktestAppModel>,
        cx: &mut Context<BacktestWindow>,
    ) {
        self.heartbeat_task.take();
        model.update(cx, |model, cx| {
            model.stop_ping();
            cx.notify();
        });
    }

    pub fn toggle_heartbeat(
        &mut self,
        model: &Entity<BacktestAppModel>,
        cx: &mut Context<BacktestWindow>,
    ) {
        if self.heartbeat_task.is_some() {
            self.stop_heartbeat(model, cx);
        } else {
            self.start_heartbeat(model, cx);
        }
    }
}

/// Test a validated loopback endpoint through the typed backtest client.
pub fn test_service_connection(
    endpoint_value: String,
    model: &Entity<BacktestAppModel>,
    cx: &mut Context<BacktestWindow>,
) -> Task<()> {
    let generation = model.update(cx, |model, cx| {
        let generation = model.begin_connection_test(endpoint_value.clone());
        cx.notify();
        generation
    });
    let model = model.downgrade();
    cx.spawn(async move |_this, cx| {
        let probe =
            Tokio::spawn(cx, async move {
                let endpoint = qs_backtest_client::parse_desktop_endpoint(&endpoint_value)
                    .map_err(|error| ConnectionFailureView {
                        endpoint: "Invalid endpoint".into(),
                        message: error.to_string(),
                        technical_detail: "Only loopback tcp:// endpoints are accepted.".into(),
                    })?;
                let endpoint_display = endpoint.redacted();
                let connector = qs_backtest_client::provider::xrpc::XrpcBacktestConnector::new(
                    endpoint,
                    "qs-backtest-desktop",
                );
                qs_backtest_client::probe_service_catalog(&connector)
                    .await
                    .map(|snapshot| ConnectionCatalogView {
                        endpoint: snapshot.endpoint_display,
                        status: snapshot.ping.status,
                        uptime_secs: snapshot.ping.uptime_secs,
                        profile_count: snapshot.profiles.profiles.len(),
                        symbol_count: snapshot.symbols.symbols.len(),
                        loaded_at: snapshot
                            .loaded_at
                            .format("%Y-%m-%d %H:%M:%S UTC")
                            .to_string(),
                    })
                    .map_err(|error| ConnectionFailureView {
                        endpoint: endpoint_display,
                        message: error.user_message(),
                        technical_detail: error.to_string(),
                    })
            });
        let result = match probe.await {
            Ok(result) => result,
            Err(error) => Err(ConnectionFailureView {
                endpoint: "Connection task".into(),
                message: "The connection test stopped unexpectedly.".into(),
                technical_detail: error.to_string(),
            }),
        };
        if let Some(model) = model.upgrade() {
            cx.update_entity(&model, |model, cx| {
                if model.apply_connection_result(generation, result) {
                    cx.notify();
                }
            });
        }
    })
}

/// Quit the application after the requested delay. Used by the smoke harness
/// to verify window creation, rendering, and bounded shutdown end to end.
pub fn start_smoke_quit(secs: u64, cx: &mut Context<BacktestWindow>) -> Task<()> {
    cx.spawn(async move |_this, cx| {
        let timer = Tokio::spawn(cx, async move {
            tokio::time::sleep(Duration::from_secs(secs)).await;
        });
        let _ = timer.await;
        cx.update(|cx| cx.quit());
    })
}

/// Open the native file picker for a signal JSONL or a result document.
/// Receiver failure, platform error, user cancellation, and a selected path
/// are distinct outcomes; cancellation raises no notification.
pub fn prompt_open(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Task<()> {
    let prompt = cx.prompt_for_paths(PathPromptOptions {
        files: true,
        directories: false,
        multiple: false,
        prompt: Some("Select a signal JSONL or a result document".into()),
    });
    let model = model.downgrade();
    cx.spawn(async move |this, cx| {
        let outcome = match prompt.await {
            Ok(Ok(Some(paths))) => match paths.into_iter().next() {
                Some(path) => DialogOutcome::Selected(path),
                None => DialogOutcome::Cancelled,
            },
            Ok(Ok(None)) => DialogOutcome::Cancelled,
            Ok(Err(error)) => DialogOutcome::Failed(error.to_string()),
            Err(_receiver) => DialogOutcome::Failed("prompt receiver dropped".into()),
        };
        if let Some(model) = model.upgrade() {
            cx.update_entity(&model, |model, cx| {
                model.record_open_dialog(outcome);
                cx.notify();
            });
        }
        if let Some(view) = this.upgrade() {
            cx.update_entity(&view, |view, _| view.finish_dialog());
        }
    })
}

/// Open the native save picker for the summary-only save-as action.
pub fn prompt_save_as(
    model: &Entity<BacktestAppModel>,
    cx: &mut Context<BacktestWindow>,
) -> Task<()> {
    let directory = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let prompt = cx.prompt_for_new_path(&directory, Some("backtest-summary.json"));
    let model = model.downgrade();
    cx.spawn(async move |this, cx| {
        let outcome = match prompt.await {
            Ok(Ok(Some(path))) => DialogOutcome::Selected(path),
            Ok(Ok(None)) => DialogOutcome::Cancelled,
            Ok(Err(error)) => DialogOutcome::Failed(error.to_string()),
            Err(_receiver) => DialogOutcome::Failed("prompt receiver dropped".into()),
        };
        if let Some(model) = model.upgrade() {
            cx.update_entity(&model, |model, cx| {
                model.record_save_dialog(outcome);
                cx.notify();
            });
        }
        if let Some(view) = this.upgrade() {
            cx.update_entity(&view, |view, _| view.finish_dialog());
        }
    })
}
