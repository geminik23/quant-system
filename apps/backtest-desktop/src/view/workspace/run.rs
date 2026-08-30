//! Run route: progress and safe actions first, technical stages on demand.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::BacktestAppModel;
use crate::view::BacktestWindow;
use crate::view::theme;
use crate::view::workspace::{render_row, render_stage_list, route_title, section_frame};

pub fn run_workspace(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    let (fixture, active_execution, nav_index, saved_path) = {
        let model = model.read(cx);
        (
            model.fixture(),
            model.active_execution().is_active(),
            model.nav_index(),
            model.saved_path().map(str::to_string),
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
        .child(route_title(if active_execution {
            "Run progress preview"
        } else {
            "Run preview status"
        }))
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
                    "Interface preview only. No backtest submit/watch workflow is connected yet.",
                ),
        );

    if nav_index == 0 {
        let (headline, detail) = match fixture {
            crate::preview::FixtureScenario::Disconnected => (
                "Restoring the connection",
                "The app is checking the existing job. It will not submit the backtest again.",
            ),
            crate::preview::FixtureScenario::PersistedResult if !active_execution => (
                "Backtest completed",
                "The result document is ready to open.",
            ),
            crate::preview::FixtureScenario::SummaryOnly => (
                "Backtest completed",
                "A compact summary is available. No offline analysis document was created.",
            ),
            _ => (
                "Replaying historical market data",
                "1,204,551 events processed · elapsed 2m 14s",
            ),
        };
        frame = frame
            .child(
                section_frame(headline)
                    .child(
                        div()
                            .text_size(px(theme::TEXT_SIZE))
                            .text_color(theme::dim_text())
                            .child(detail),
                    )
                    .child(
                        div()
                            .h(px(8.))
                            .w_full()
                            .rounded_md()
                            .bg(theme::selection_track())
                            .child(
                                div()
                                    .h(px(8.))
                                    .w(px(320.))
                                    .rounded_md()
                                    .bg(theme::accent()),
                            ),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap_2()
                    .child(
                        action("run-background", "Continue in background").on_click(cx.listener(
                            move |this, _, _, cx| {
                                this.model.update(cx, |model, cx| {
                                    if model.active_execution().is_active() {
                                        model.end_execution();
                                        model.notify_info(
                                            "The backtest is continuing in the background and can be resumed with the same job.",
                                        );
                                    } else {
                                        model.notify_info("No active backtest to move to the background.");
                                    }
                                    cx.notify();
                                });
                            },
                        )),
                    )
                    .child(
                        action("run-cancel", "Cancel backtest").on_click(cx.listener(
                            move |this, _, _, cx| {
                                this.model.update(cx, |model, cx| {
                                    if model.active_execution().is_active() {
                                        model.end_execution();
                                        model.notify_info("Cancellation requested.");
                                    } else {
                                        model.notify_info("No active backtest to cancel.");
                                    }
                                    cx.notify();
                                });
                            },
                        )),
                    ),
            );

        match fixture {
            crate::preview::FixtureScenario::PersistedResult => {
                frame = frame.child(primary_action("run-open-results", "View results").on_click(
                    cx.listener(move |this, _, _, cx| {
                        this.model.update(cx, |model, cx| {
                            model.open_document(crate::model::OpenDocumentKind::Result);
                            cx.notify();
                        });
                    }),
                ));
            }
            crate::preview::FixtureScenario::SummaryOnly => {
                frame = frame.child(
                    primary_action("run-save-as", "Preview save summary as...").on_click(
                        cx.listener(move |this, _, _, cx| {
                            this.save_summary_as(cx);
                        }),
                    ),
                );
                if let Some(path) = saved_path {
                    frame = frame.child(
                        section_frame("Saved")
                            .child(render_row(&crate::preview::Row::new("File", path))),
                    );
                }
            }
            _ => {}
        }
    } else {
        let stages = crate::preview::run_stages(fixture);
        let progress = crate::preview::run_progress(fixture);
        frame = frame
            .child(section_frame("Execution stages").child(render_stage_list(&stages)))
            .child(
                section_frame("Technical progress").child(
                    div()
                        .flex()
                        .flex_col()
                        .children(progress.iter().map(render_row)),
                ),
            );
    }
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
