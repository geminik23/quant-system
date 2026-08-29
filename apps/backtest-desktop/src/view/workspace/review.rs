//! Review route: a concise confirmation before starting the run.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::BacktestAppModel;
use crate::preview::{Card, Row};
use crate::view::BacktestWindow;
use crate::view::theme;
use crate::view::workspace::{render_card, route_title};

pub fn review_workspace(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    let (fixture, active_execution, connection_ready, input_name) = {
        let model = model.read(cx);
        (
            model.fixture(),
            model.active_execution().is_active(),
            model.connection_ready(),
            model.input_display_name(),
        )
    };
    let blocked = fixture == crate::preview::FixtureScenario::Warning;
    let mut frame = div()
        .flex()
        .flex_col()
        .flex_1()
        .min_w_0()
        .overflow_hidden()
        .p_4()
        .gap_3()
        .child(route_title("Review and start"))
        .child(render_card(&Card::new(
            "Input",
            vec![
                Row::new("File", input_name),
                Row::new("Signals", "4,094"),
                Row::new("Market and period", "EURUSD · 2024 full year"),
            ],
        )))
        .child(render_card(&Card::new(
            "Backtest",
            vec![
                Row::new("Starting balance", "USD 10,000"),
                Row::new("Risk per entry", "1%"),
                Row::new("Execution", "Future quotes · close positions at the end"),
            ],
        )))
        .child(render_card(&Card::new(
            "Result",
            vec![
                Row::new("Save to", "results/eurusd_lifecycle.json"),
                Row::new("File safety", "Existing files are never overwritten"),
            ],
        )))
        .child(
            div()
                .px_3()
                .py_2()
                .rounded_md()
                .border_1()
                .border_color(theme::warn_amber())
                .bg(theme::chip_bg())
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::warn_amber())
                .child(
                    "Historical simulation only. Results do not guarantee future performance. A single run is not an official strategy validation.",
                ),
        );

    let start_label = if active_execution {
        "A backtest is already running"
    } else if blocked {
        "Resolve input warnings before starting"
    } else if !connection_ready {
        "Connect the backtest server first"
    } else {
        "Preview run screen"
    };
    frame = frame.child(div().flex_1()).child(
        div()
            .flex()
            .flex_row()
            .justify_between()
            .child(
                action("review-back", "Back").on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        model.go_prev_step();
                        cx.notify();
                    });
                })),
            )
            .child(
                primary_action("review-start", start_label).on_click(cx.listener(
                    move |this, _, _, cx| {
                        this.start_fixture_run(cx);
                    },
                )),
            ),
    );
    frame
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
