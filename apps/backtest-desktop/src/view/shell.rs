//! Static application shell: header, phase bar, notification banner, and
//! bottom status bar.

use gpui::prelude::*;
use gpui::{Div, Entity, Stateful, div, px};

use crate::model::{
    AppRoute, BacktestAppModel, OpenDocumentKind, PhaseChipState, RunStep, ServiceConnectionState,
};
use crate::view::BacktestWindow;
use crate::view::theme;

/// Render the window header with the current task, a simple connection state,
/// and the two primary document actions.
pub fn header(
    model: &Entity<BacktestAppModel>,
    developer_preview: bool,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let (identity, chip, fixture, connection) = {
        let model = model.read(cx);
        (
            model.identity_label(),
            model.context_chip_label(),
            model.fixture(),
            model.connection().clone(),
        )
    };
    let fixture_connection = crate::preview::connection_display(fixture);
    let (service_label, service_ready, service_detail) = match connection {
        ServiceConnectionState::Idle if developer_preview => (
            fixture_connection.state_label,
            fixture_connection.connected,
            "Developer fixture connection state".to_string(),
        ),
        ServiceConnectionState::Idle => (
            "Not connected".to_string(),
            false,
            "Open Advanced settings to test the server".to_string(),
        ),
        ServiceConnectionState::Connecting { .. } => (
            "Connecting".to_string(),
            false,
            "Testing the backtest service".to_string(),
        ),
        ServiceConnectionState::Connected(catalog) => (
            "Connected".to_string(),
            true,
            format!(
                "{} profiles · {} market data entries",
                catalog.profile_count, catalog.symbol_count
            ),
        ),
        ServiceConnectionState::Failed(_) => (
            "Connection failed".to_string(),
            false,
            "Open Advanced settings for details".to_string(),
        ),
    };
    div()
        .flex()
        .flex_row()
        .items_center()
        .justify_between()
        .h(px(theme::HEADER_HEIGHT))
        .px_3()
        .gap_3()
        .border_b_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap_2()
                .min_w_0()
                .child(
                    div()
                        .text_size(px(theme::TITLE_SIZE))
                        .child("Quant System Backtest"),
                )
                .child(label_chip(chip, theme::accent()))
                .child(label_chip("UI preview", theme::warn_amber()))
                .child(
                    div()
                        .text_size(px(theme::SMALL_SIZE))
                        .text_color(theme::dim_text())
                        .truncate()
                        .child(identity),
                ),
        )
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap_2()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(
                    div()
                        .flex()
                        .flex_row()
                        .items_center()
                        .gap_1()
                        .child(connection_dot(service_ready))
                        .child(service_label),
                )
                .child(div().child(service_detail)),
        )
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap_2()
                .child(
                    action_chip("header-new", "New backtest").on_click(cx.listener(
                        move |this, _, _, cx| {
                            this.model.update(cx, |model, cx| {
                                model.new_backtest();
                                cx.notify();
                            });
                        },
                    )),
                )
                .child(
                    action_chip("header-open", "Open result").on_click(cx.listener(
                        move |this, _, _, cx| {
                            this.open_document_prompt(cx);
                        },
                    )),
                ),
        )
}

/// Render the phase bar. In wizard mode it navigates the run steps; when an
/// offline document is open only Results is active.
pub fn phase_bar(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    let (chips, context) = {
        let model = model.read(cx);
        (
            model.phase_chips(),
            model.phase_context().map(str::to_string),
        )
    };
    let mut bar = div()
        .flex()
        .flex_row()
        .items_center()
        .gap_2()
        .h(px(theme::PHASE_BAR_HEIGHT))
        .px_3()
        .border_b_1()
        .border_color(theme::border())
        .bg(theme::panel_bg());
    for (index, chip) in chips.iter().enumerate() {
        if index > 0 {
            bar = bar.child(
                div()
                    .text_color(theme::dim_text())
                    .text_size(px(theme::SMALL_SIZE))
                    .child(">"),
            );
        }
        let (text_color, bg, prefix) = match chip.state {
            PhaseChipState::Done => (theme::ok_green(), theme::chip_bg(), "Done · "),
            PhaseChipState::Current => (theme::accent(), theme::hover_bg(), ""),
            PhaseChipState::Pending => (theme::dim_text(), theme::chip_bg(), ""),
        };
        let label = format!("{prefix}{}", chip.label);
        bar = bar.child(
            div()
                .id(("phase-chip", index))
                .px_2()
                .py_1()
                .rounded_md()
                .bg(bg)
                .text_color(text_color)
                .text_size(px(theme::SMALL_SIZE))
                .cursor_pointer()
                .hover(|style| style.bg(theme::hover_bg()))
                .child(label)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        if index < 4 {
                            let step = RunStep::from_index(index).unwrap_or(RunStep::Input);
                            model.goto_step(step);
                        } else {
                            model.open_document(OpenDocumentKind::Result);
                        }
                        cx.notify();
                    });
                })),
        );
    }
    if let Some(context) = context {
        bar = bar.child(
            div()
                .ml_2()
                .px_2()
                .py_1()
                .rounded_md()
                .bg(theme::chip_bg())
                .text_color(theme::offline_purple())
                .text_size(px(theme::SMALL_SIZE))
                .child(context),
        );
    }
    bar
}

/// Render the dismissable notification banner. Renders an empty strip when
/// no notification is present so the shell layout stays stable.
pub fn notification(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    let notification = model.read(cx).notification().cloned();
    let mut container = div().h(px(0.));
    if let Some(notification) = notification {
        let color = if notification.warning {
            theme::warn_amber()
        } else {
            theme::accent()
        };
        let prefix = if notification.warning {
            "[warning]"
        } else {
            "[info]"
        };
        container = div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .gap_2()
            .px_3()
            .py_1()
            .bg(theme::chip_bg())
            .border_b_1()
            .border_color(theme::border())
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_2()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(color)
                    .child(prefix)
                    .child(notification.message),
            )
            .child(
                div()
                    .id("dismiss-notification")
                    .px_2()
                    .rounded_md()
                    .cursor_pointer()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(theme::dim_text())
                    .hover(|style| style.bg(theme::hover_bg()))
                    .child("Dismiss (esc)")
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.model.update(cx, |model, cx| {
                            model.dismiss_notification();
                            cx.notify();
                        });
                    })),
            );
    }
    container
}

/// Render a quiet bottom status bar. Technical heartbeat controls are only
/// present in developer preview mode.
pub fn status_bar(
    model: &Entity<BacktestAppModel>,
    developer_preview: bool,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let (connection_state, connected, ping_running) = {
        let model = model.read(cx);
        let display = crate::preview::connection_display(model.fixture());
        let (label, connected) = match model.connection() {
            ServiceConnectionState::Idle if developer_preview => {
                (display.state_label, display.connected)
            }
            ServiceConnectionState::Idle => ("Not connected".to_string(), false),
            ServiceConnectionState::Connecting { .. } => ("Connecting".to_string(), false),
            ServiceConnectionState::Connected(_) => ("Connected".to_string(), true),
            ServiceConnectionState::Failed(_) => ("Connection failed".to_string(), false),
        };
        (label, connected, model.ping_running())
    };
    let (activity, heartbeat_cell, route_label, inspector_collapsed) = {
        let model = model.read(cx);
        let activity = if model.active_execution().is_active() {
            "Backtest running".to_string()
        } else {
            "Ready".to_string()
        };
        let heartbeat_cell = match model.heartbeat() {
            Some(beat) => format!(
                "Heartbeat {age}s ago · {rtt} ms",
                age = beat.received_at.elapsed().as_secs(),
                rtt = beat.round_trip.as_millis()
            ),
            None => "Heartbeat unavailable".to_string(),
        };
        let route_label = match model.route() {
            AppRoute::NewRun { step } => step.label().to_string(),
            AppRoute::Results { .. } => "Results".to_string(),
        };
        (
            activity,
            heartbeat_cell,
            route_label,
            model.inspector_collapsed(),
        )
    };

    let mut bar = div()
        .flex()
        .flex_row()
        .items_center()
        .gap_3()
        .h(px(theme::STATUS_BAR_HEIGHT))
        .px_3()
        .border_t_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .text_size(px(theme::SMALL_SIZE))
        .text_color(theme::dim_text())
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap_1()
                .child(connection_dot(connected))
                .child(connection_state),
        )
        .child(div().child(activity))
        .child(div().flex_1());
    if developer_preview {
        bar = bar.child(div().truncate().child(heartbeat_cell)).child(
            div()
                .id("toggle-ping")
                .px_2()
                .rounded_md()
                .cursor_pointer()
                .hover(|style| style.bg(theme::hover_bg()))
                .child(if ping_running {
                    "Stop ping"
                } else {
                    "Start ping"
                })
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.toggle_ping(cx);
                })),
        );
    }
    bar.child(div().truncate().child(route_label)).child(
        div()
            .id("toggle-details")
            .px_2()
            .rounded_md()
            .cursor_pointer()
            .hover(|style| style.bg(theme::hover_bg()))
            .child(if inspector_collapsed {
                "Show details"
            } else {
                "Hide details"
            })
            .on_click(cx.listener(move |this, _, _, cx| {
                this.model.update(cx, |model, cx| {
                    model.toggle_inspector();
                    cx.notify();
                });
            })),
    )
}

/// Status marker whose meaning does not depend on color alone.
fn connection_dot(connected: bool) -> Div {
    let (color, label) = if connected {
        (theme::ok_green(), "[ok]")
    } else {
        (theme::error_red(), "[off]")
    };
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap_1()
        .text_color(color)
        .child(div().child(label))
}

/// A compact informational chip.
pub fn label_chip(label: &str, color: gpui::Rgba) -> Div {
    div()
        .px_2()
        .py_0p5()
        .rounded_md()
        .bg(theme::chip_bg())
        .text_color(color)
        .text_size(px(theme::SMALL_SIZE))
        .child(label.to_string())
}

/// A styled clickable header action chip. The caller attaches on_click with
/// a listener so the event type stays inferred.
pub fn action_chip(id: &'static str, label: &str) -> Stateful<Div> {
    div()
        .id(id)
        .px_2()
        .py_1()
        .rounded_md()
        .border_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .cursor_pointer()
        .text_size(px(theme::SMALL_SIZE))
        .hover(|style| style.bg(theme::hover_bg()))
        .child(label.to_string())
}
