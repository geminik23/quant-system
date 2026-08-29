//! Left navigation for the current user task.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::BacktestAppModel;
use crate::preview::FixtureScenario;
use crate::view::BacktestWindow;
use crate::view::text_input::PreviewTextInput;
use crate::view::theme;

/// Render task navigation. Fixture and IME controls are only visible when the
/// application is launched with `--preview`.
pub fn context_nav(
    model: &Entity<BacktestAppModel>,
    text_input: &Entity<PreviewTextInput>,
    developer_preview: bool,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let (sections, selected, title, fixture, ping_running, document_kind) = {
        let model = model.read(cx);
        (
            model.nav_sections(),
            model.nav_index(),
            match model.route() {
                crate::model::AppRoute::NewRun { step } => step.label().to_string(),
                crate::model::AppRoute::Results { .. } => "Explore results".to_string(),
            },
            model.fixture(),
            model.ping_running(),
            match model.route() {
                crate::model::AppRoute::Results { document, .. } => Some(*document),
                crate::model::AppRoute::NewRun { .. } => None,
            },
        )
    };

    let mut list = div().flex().flex_col().gap_1().px_2();
    for (index, section) in sections.iter().enumerate() {
        let is_selected = index == selected;
        let (text_color, bg, marker) = if is_selected {
            (theme::accent(), theme::hover_bg(), ">")
        } else {
            (theme::text(), theme::panel_bg(), " ")
        };
        list = list.child(
            div()
                .id(("nav-section", index))
                .flex()
                .flex_row()
                .items_center()
                .gap_1()
                .px_2()
                .py_2()
                .rounded_md()
                .bg(bg)
                .text_color(text_color)
                .text_size(px(theme::TEXT_SIZE))
                .cursor_pointer()
                .hover(|style| style.bg(theme::hover_bg()))
                .child(div().child(marker))
                .child(div().truncate().child(*section))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        model.select_nav(index);
                        cx.notify();
                    });
                })),
        );
    }

    let mut nav = div()
        .flex()
        .flex_col()
        .w(px(theme::CONTEXT_NAV_WIDTH))
        .min_w(px(theme::CONTEXT_NAV_WIDTH))
        .border_r_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .overflow_hidden()
        .child(
            div()
                .px_4()
                .pt_4()
                .pb_2()
                .text_size(px(theme::TITLE_SIZE))
                .text_color(theme::text())
                .child(title),
        )
        .child(list)
        .child(div().flex_1())
        .child(
            div()
                .p_3()
                .border_t_1()
                .border_color(theme::border())
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(
                    "Follow the steps above. Technical details stay hidden unless you open them.",
                ),
        );

    if developer_preview {
        nav = nav.child(developer_tools(
            fixture,
            ping_running,
            document_kind,
            text_input,
            cx,
        ));
    }
    nav
}

fn developer_tools(
    fixture: FixtureScenario,
    ping_running: bool,
    document_kind: Option<crate::model::OpenDocumentKind>,
    text_input: &Entity<PreviewTextInput>,
    cx: &mut Context<BacktestWindow>,
) -> Div {
    let mut scenarios = div().flex().flex_col().gap_1();
    for scenario in FixtureScenario::ALL {
        let is_current = scenario == fixture;
        scenarios = scenarios.child(
            div()
                .id(("fixture-scenario", scenario as usize))
                .px_2()
                .py_1()
                .rounded_md()
                .border_1()
                .border_color(theme::border())
                .bg(if is_current {
                    theme::hover_bg()
                } else {
                    theme::panel_bg()
                })
                .text_color(if is_current {
                    theme::accent()
                } else {
                    theme::text()
                })
                .text_size(px(theme::SMALL_SIZE))
                .cursor_pointer()
                .hover(|style| style.bg(theme::hover_bg()))
                .child(format!(
                    "[{}] {}",
                    if is_current { "x" } else { " " },
                    scenario.label()
                ))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        model.set_fixture(scenario);
                        cx.notify();
                    });
                })),
        );
    }
    let document_label = match document_kind {
        Some(crate::model::OpenDocumentKind::Experiment) => "Document: Experiment",
        _ => "Document: Result",
    };

    div()
        .flex()
        .flex_col()
        .gap_2()
        .p_3()
        .border_t_1()
        .border_color(theme::border())
        .bg(theme::chip_bg())
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::warn_amber())
                .child("Developer preview"),
        )
        .child(scenarios)
        .child(
            div()
                .id("fixture-document-toggle")
                .px_2()
                .py_1()
                .rounded_md()
                .border_1()
                .border_color(theme::border())
                .cursor_pointer()
                .text_size(px(theme::SMALL_SIZE))
                .hover(|style| style.bg(theme::hover_bg()))
                .child(document_label)
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.model.update(cx, |model, cx| {
                        model.toggle_document_kind();
                        cx.notify();
                    });
                })),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .gap_1()
                .child(
                    div()
                        .text_size(px(theme::SMALL_SIZE))
                        .text_color(theme::dim_text())
                        .child("IME input check"),
                )
                .child(text_input.clone()),
        )
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(if ping_running {
                    "Heartbeat actor running"
                } else {
                    "Heartbeat actor stopped"
                }),
        )
}
