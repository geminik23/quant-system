//! Results route: Result Explorer presentation for persisted documents.

use gpui::prelude::*;
use gpui::{Div, Entity, Stateful, div, px};

use crate::model::{BacktestAppModel, OpenDocumentKind, ResultsPresentation};
use crate::preview::MonthlyReturnStatus;
use crate::view::BacktestWindow;
use crate::view::theme;
use crate::view::workspace::{
    render_card, render_metric_cell, render_positions_table, render_row, route_title, section_frame,
};

pub fn results_workspace(
    model: &Entity<BacktestAppModel>,
    document: OpenDocumentKind,
    cx: &mut Context<BacktestWindow>,
) -> Stateful<Div> {
    let (presentation, nav_index, from_document, section_label) = {
        let model = model.read(cx);
        (
            model.results_presentation(),
            model.nav_index(),
            model.results_from_document(),
            match model.route() {
                crate::model::AppRoute::Results { section, .. } => section.label().to_string(),
                crate::model::AppRoute::NewRun { .. } => String::new(),
            },
        )
    };

    let mut frame = div()
        .id("results-workspace")
        .flex()
        .flex_col()
        .flex_1()
        .min_w_0()
        .overflow_y_scroll()
        .overflow_x_hidden()
        .p_4()
        .gap_3()
        .child(route_title(&section_label));

    if !from_document {
        frame = frame.child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child("In-session completion; no offline document was opened."),
        );
    }

    match document {
        OpenDocumentKind::Experiment => {
            let card = crate::preview::experiment_section(nav_index);
            frame = frame.child(render_card(&card));
            if nav_index == 4 {
                frame = frame.child(
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
                            "[not available] An official verdict requires a frozen experiment \
                             with independent child replays. Partial experiments never show an \
                             official pass.",
                        ),
                );
            }
        }
        OpenDocumentKind::Result => match presentation {
            ResultsPresentation::NotPersisted => {
                frame = frame
                    .child(render_card(&crate::preview::summary_only_card()))
                    .child(
                        primary_action("results-save-as", "Save summary as...").on_click(
                            cx.listener(move |this, _, _, cx| {
                                this.save_summary_as(cx);
                            }),
                        ),
                    );
            }
            ResultsPresentation::AnalysisUnavailable { reason } => {
                frame = frame
                    .child(render_card(&crate::preview::unavailable_card()))
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
                            .child(format!("[analysis unavailable] {reason}")),
                    )
                    .child(render_card(&crate::preview::metadata_card("Result")));
            }
            ResultsPresentation::Evidence => {
                frame = match nav_index {
                    0 => frame
                        .child(render_card(&crate::preview::result_summary()))
                        .child(result_charts())
                        .child(monthly_return_grid())
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
                                    "Exploratory result only. This single historical replay is not an official strategy validation.",
                                ),
                        )
                        .child(render_card(&crate::preview::result_highlights())),
                    7 => frame.child(render_positions_table()),
                    8 => {
                        let evidence = crate::preview::evidence_bar();
                        let mut evidence_rows = div().flex().flex_col();
                        for row in &evidence {
                            evidence_rows = evidence_rows.child(render_row(row));
                        }
                        frame
                            .child(section_frame("Analysis data details").child(evidence_rows))
                            .child(render_card(&crate::preview::metadata_card("Result")))
                    }
                    section_index => {
                        let cells = crate::preview::metric_cells(section_index);
                        for cell in &cells {
                            frame = frame.child(render_metric_cell(cell));
                        }
                        frame
                    }
                };
            }
        },
    }
    frame.child(div().flex_1())
}

fn result_charts() -> Div {
    section_frame("Portfolio path")
        .child(
            div()
                .flex()
                .flex_row()
                .justify_between()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child("Portfolio value")
                .child("USD 10,000 to USD 11,420"),
        )
        .child(crate::view::chart::line_chart(
            crate::preview::equity_series(),
            theme::accent(),
            Some(gpui::rgba(0x2F6FED1F)),
            150.0,
        ))
        .child(
            div()
                .flex()
                .flex_row()
                .justify_between()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child("Drawdown")
                .child("Worst: -8.2%"),
        )
        .child(crate::view::chart::line_chart(
            crate::preview::drawdown_series(),
            theme::error_red(),
            Some(gpui::rgba(0xCF222E1F)),
            90.0,
        ))
}

fn monthly_return_grid() -> Div {
    let months = crate::preview::monthly_returns();
    let mut rows = div().flex().flex_col().gap_2();
    for group in months.chunks(6) {
        let mut row = div().flex().flex_row().gap_2();
        for month in group {
            let (value, color, background) = match month.status {
                MonthlyReturnStatus::Observed(value) if value >= 0.0 => (
                    format!("+{value:.1}% ▲"),
                    theme::ok_green(),
                    gpui::rgba(0x1A7F3717),
                ),
                MonthlyReturnStatus::Observed(value) => (
                    format!("{value:.1}% ▼"),
                    theme::error_red(),
                    gpui::rgba(0xCF222E17),
                ),
                MonthlyReturnStatus::Inactive => {
                    ("Inactive".to_string(), theme::dim_text(), theme::chip_bg())
                }
                MonthlyReturnStatus::Missing => {
                    ("Missing".to_string(), theme::warn_amber(), theme::chip_bg())
                }
            };
            row = row.child(
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .gap_1()
                    .p_2()
                    .rounded_md()
                    .border_1()
                    .border_color(theme::border())
                    .bg(background)
                    .child(
                        div()
                            .text_size(px(theme::SMALL_SIZE))
                            .text_color(theme::dim_text())
                            .child(month.label),
                    )
                    .child(
                        div()
                            .text_size(px(theme::TEXT_SIZE))
                            .text_color(color)
                            .child(value),
                    ),
            );
        }
        rows = rows.child(row);
    }
    section_frame("Monthly returns").child(rows)
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
        .cursor_pointer()
        .text_size(px(theme::TEXT_SIZE))
        .hover(|style| style.bg(theme::chip_bg()))
        .child(label.to_string())
}
