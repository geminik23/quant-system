//! Main workspace rendering for each route.

pub mod configure;
pub mod input;
pub mod results;
pub mod review;
pub mod run;

use gpui::prelude::*;
use gpui::{AnyElement, Div, Entity, div, px};

use crate::model::{AppRoute, BacktestAppModel};
use crate::preview::{Card, Row, StageState};
use crate::view::BacktestWindow;
use crate::view::text_input::PreviewTextInput;
use crate::view::theme;

/// Render the workspace column for the current route.
pub fn workspace(
    model: &Entity<BacktestAppModel>,
    endpoint_input: &Entity<PreviewTextInput>,
    cx: &mut Context<BacktestWindow>,
) -> AnyElement {
    let route = model.read(cx).route().clone();
    match route {
        AppRoute::NewRun {
            step: crate::model::RunStep::Input,
        } => input::input_workspace(model, cx).into_any_element(),
        AppRoute::NewRun {
            step: crate::model::RunStep::Configure,
        } => configure::configure_workspace(model, endpoint_input, cx).into_any_element(),
        AppRoute::NewRun {
            step: crate::model::RunStep::Review,
        } => review::review_workspace(model, cx).into_any_element(),
        AppRoute::NewRun {
            step: crate::model::RunStep::Run,
        } => run::run_workspace(model, cx).into_any_element(),
        AppRoute::Results { document, .. } => {
            results::results_workspace(model, document, cx).into_any_element()
        }
    }
}

/// A titled workspace section.
pub fn section_frame(title: &str) -> Div {
    div()
        .flex()
        .flex_col()
        .gap_2()
        .p_3()
        .border_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .rounded_md()
        .child(
            div()
                .text_size(px(theme::TEXT_SIZE))
                .text_color(theme::text())
                .child(title.to_string()),
        )
}

/// Render a card of key-value rows.
pub fn render_card(card: &Card) -> Div {
    let mut rows = div().flex().flex_col();
    for row in &card.rows {
        rows = rows.child(render_row(row));
    }
    section_frame(&card.title).child(rows)
}

/// Render a single key-value row.
pub fn render_row(row: &Row) -> Div {
    div()
        .flex()
        .flex_row()
        .justify_between()
        .gap_3()
        .py_0p5()
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(row.label.clone()),
        )
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::text())
                .truncate()
                .child(row.value.clone()),
        )
}

/// Render a stage list for the run route.
pub fn render_stage_list(stages: &[(String, StageState)]) -> Div {
    let mut list = div().flex().flex_col();
    for (stage, state) in stages {
        let (color, marker) = match state {
            StageState::Done => (theme::dim_text(), "[done]"),
            StageState::Current => (theme::accent(), "[current]"),
            StageState::Pending => (theme::dim_text(), "[todo]"),
        };
        let is_current = matches!(state, StageState::Current);
        let bg = if is_current {
            theme::hover_bg()
        } else {
            theme::panel_bg()
        };
        list = list.child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap_2()
                .px_2()
                .py_0p5()
                .bg(bg)
                .rounded_md()
                .text_color(color)
                .text_size(px(theme::SMALL_SIZE))
                .child(div().child(marker))
                .child(div().child(stage.clone())),
        );
    }
    list
}

/// Render the small static positions table.
pub fn render_positions_table() -> Div {
    let headers = crate::preview::POSITIONS_HEADERS;
    let rows = crate::preview::positions_rows();
    let mut header_row = div().flex().flex_row();
    for header in headers {
        header_row = header_row.child(
            div()
                .flex_1()
                .px_2()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(header),
        );
    }
    let mut body = div().flex().flex_col();
    for row in rows {
        let mut cells = div().flex().flex_row().py_0p5();
        for cell in row {
            cells = cells.child(
                div()
                    .flex_1()
                    .px_2()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(theme::text())
                    .child(cell),
            );
        }
        body = body.child(cells);
    }
    section_frame("Positions (static fixture, 6 of 112 provided)")
        .child(
            div()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child("Virtualized 10,000-row rendering arrives with the result explorer phase."),
        )
        .child(header_row)
        .child(body)
}

/// Render a metric evidence cell following the common evidence shape.
pub fn render_metric_cell(cell: &crate::preview::MetricCell) -> Div {
    let rows = crate::preview::evidence_cell_rows(cell);
    let mut list = div().flex().flex_col();
    for row in &rows {
        list = list.child(render_row(row));
    }
    section_frame(&format!("Metric evidence - {}", cell.metric)).child(list)
}

/// Workspace title row helper.
pub fn route_title(label: &str) -> Div {
    div()
        .text_size(px(theme::TITLE_SIZE))
        .text_color(theme::text())
        .child(label.to_string())
}
