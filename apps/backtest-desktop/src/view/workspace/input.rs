//! Input route: choose a signal file first, then inspect optional details.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::BacktestAppModel;
use crate::preview::{Card, Row};
use crate::view::BacktestWindow;
use crate::view::theme;
use crate::view::workspace::{render_card, route_title, section_frame};

pub fn input_workspace(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    let (nav_index, fixture, input_name, selected_input) = {
        let model = model.read(cx);
        (
            model.nav_index(),
            model.fixture(),
            model.input_display_name(),
            model.selected_input_display_name().map(str::to_string),
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
        .child(route_title("Start a new backtest"));

    if nav_index == 0 {
        let has_selected_input = selected_input.is_some();
        let selected_file = match selected_input {
            Some(selected) => render_card(&Card::new(
                "Selected file",
                vec![
                    Row::new("File", selected),
                    Row::new("Market", "EURUSD"),
                    Row::new("Period", "2024-01-02 to 2024-12-30"),
                    Row::new("Signals", "4,094 ready for replay"),
                    Row::new(
                        "Warnings",
                        if fixture == crate::preview::FixtureScenario::Warning {
                            "3 lines need attention"
                        } else {
                            "None"
                        },
                    ),
                ],
            )),
            None => section_frame("No file selected").child(
                div()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(theme::dim_text())
                    .child("Choose a JSONL file to see its market, period, signal count, and warnings."),
            ),
        };
        frame = frame
            .child(
                section_frame("Choose a parsed signal file")
                    .child(
                        div()
                            .text_size(px(theme::TEXT_SIZE))
                            .text_color(theme::dim_text())
                            .child(
                                "Select the JSONL file that contains the signals you want to test.",
                            ),
                    )
                    .child(
                        primary_action("select-jsonl", "Choose JSONL file...").on_click(
                            cx.listener(move |this, _, _, cx| {
                                this.open_document_prompt(cx);
                            }),
                        ),
                    ),
            )
            .child(selected_file)
            .child(
                div().flex().flex_row().justify_end().child(
                    primary_action(
                        "input-continue",
                        if has_selected_input {
                            "Continue to settings"
                        } else {
                            "Choose a file to continue"
                        },
                    )
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.model.update(cx, |model, cx| {
                            if has_selected_input {
                                model.go_next_step();
                            } else {
                                model.notify_info("Choose a JSONL file before continuing.");
                            }
                            cx.notify();
                        });
                    })),
                ),
            );
    } else {
        frame = frame
            .child(render_card(&crate::preview::input_summary(&input_name)))
            .child(render_card(&crate::preview::input_policy()))
            .child(render_card(&crate::preview::input_coverage(fixture)))
            .child(render_card(&crate::preview::input_date_filter()));
    }
    frame
}

fn primary_action(id: &'static str, label: &str) -> gpui::Stateful<Div> {
    div()
        .id(id)
        .px_3()
        .py_2()
        .rounded_md()
        .border_1()
        .border_color(theme::accent())
        .bg(theme::hover_bg())
        .text_color(theme::accent())
        .text_size(px(theme::TEXT_SIZE))
        .cursor_pointer()
        .hover(|style| style.bg(theme::chip_bg()))
        .child(label.to_string())
}
