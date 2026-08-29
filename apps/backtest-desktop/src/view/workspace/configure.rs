//! Configure route: common settings first, advanced controls on demand.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::{BacktestAppModel, ServiceConnectionState};
use crate::preview::{Card, Row};
use crate::view::BacktestWindow;
use crate::view::text_input::PreviewTextInput;
use crate::view::theme;
use crate::view::workspace::{render_card, render_row, route_title, section_frame};

pub fn configure_workspace(
    model: &Entity<BacktestAppModel>,
    endpoint_input: &Entity<PreviewTextInput>,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let (nav_index, fixture, connection) = {
        let model = model.read(cx);
        (
            model.nav_index(),
            model.fixture(),
            model.connection().clone(),
        )
    };
    let mut frame = div()
        .flex()
        .flex_col()
        .flex_1()
        .min_w_0()
        .overflow_hidden()
        .p_4()
        .gap_3()
        .child(route_title("Backtest settings"));

    if nav_index == 0 {
        frame = frame
            .child(render_card(&Card::new(
                "Market data",
                vec![
                    Row::new("Market", "EURUSD"),
                    Row::new("Data", "Tick data"),
                    Row::new("Period", "2024-01-02 to 2024-12-30"),
                ],
            )))
            .child(render_card(&Card::new(
                "Capital and risk",
                vec![
                    Row::new("Starting balance", "USD 10,000"),
                    Row::new("Risk per entry", "1%"),
                    Row::new("Close open positions at the end", "Yes"),
                ],
            )))
            .child(render_card(&Card::new(
                "Result",
                vec![
                    Row::new("Save result document", "Yes"),
                    Row::new("File", "results/eurusd_lifecycle.json"),
                    Row::new("Existing files", "Never overwrite"),
                ],
            )));
    } else {
        frame = frame
            .child(
                div()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(theme::dim_text())
                    .child("These settings are usually safe to leave unchanged."),
            )
            .child(connection_panel(&connection, endpoint_input, cx))
            .child(render_card(&crate::preview::configure_section(3, fixture)))
            .child(render_card(&crate::preview::configure_section(5, fixture)))
            .child(render_card(&crate::preview::configure_section(6, fixture)));
    }

    frame.child(div().flex_1()).child(
        div()
            .flex()
            .flex_row()
            .justify_between()
            .child(
                action("configure-back", "Back").on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        model.go_prev_step();
                        cx.notify();
                    });
                })),
            )
            .child(
                primary_action("configure-continue", "Review settings").on_click(cx.listener(
                    move |this, _, _, cx| {
                        this.model.update(cx, |model, cx| {
                            model.go_next_step();
                            cx.notify();
                        });
                    },
                )),
            ),
    )
}

fn connection_panel(
    connection: &ServiceConnectionState,
    endpoint_input: &Entity<PreviewTextInput>,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let mut status = div().flex().flex_col().gap_1();
    match connection {
        ServiceConnectionState::Idle => {
            status = status.child(render_row(&Row::new("Status", "Not tested")));
        }
        ServiceConnectionState::Connecting { endpoint } => {
            status = status
                .child(render_row(&Row::new("Status", "Connecting...")))
                .child(render_row(&Row::new("Endpoint", endpoint.clone())));
        }
        ServiceConnectionState::Connected(catalog) => {
            status = status
                .child(render_row(&Row::new("Status", "Connected")))
                .child(render_row(&Row::new("Endpoint", catalog.endpoint.clone())))
                .child(render_row(&Row::new(
                    "Server",
                    format!("{} · uptime {}s", catalog.status, catalog.uptime_secs),
                )))
                .child(render_row(&Row::new(
                    "Catalog",
                    format!(
                        "{} profiles · {} market data entries",
                        catalog.profile_count, catalog.symbol_count
                    ),
                )))
                .child(render_row(&Row::new("Loaded", catalog.loaded_at.clone())));
        }
        ServiceConnectionState::Failed(failure) => {
            status = status
                .child(render_row(&Row::new("Status", "Connection failed")))
                .child(render_row(&Row::new("Endpoint", failure.endpoint.clone())))
                .child(render_row(&Row::new(
                    "What happened",
                    failure.message.clone(),
                )))
                .child(render_row(&Row::new(
                    "Technical details",
                    failure.technical_detail.clone(),
                )));
        }
    }

    section_frame("Backtest server")
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(
                    "Use a loopback TCP endpoint. HTTP URLs and remote addresses are not accepted.",
                ),
        )
        .child(endpoint_input.clone())
        .child(
            primary_action("test-connection", "Test connection").on_click(cx.listener(
                move |this, _, _, cx| {
                    this.test_service_connection(cx);
                },
            )),
        )
        .child(status)
}

fn action(id: &'static str, label: &str) -> gpui::Stateful<Div> {
    div()
        .id(id)
        .px_3()
        .py_2()
        .rounded_md()
        .border_1()
        .border_color(theme::border())
        .text_size(px(theme::TEXT_SIZE))
        .cursor_pointer()
        .hover(|style| style.bg(theme::hover_bg()))
        .child(label.to_string())
}

fn primary_action(id: &'static str, label: &str) -> gpui::Stateful<Div> {
    action(id, label)
        .border_color(theme::accent())
        .bg(theme::hover_bg())
        .text_color(theme::accent())
}
