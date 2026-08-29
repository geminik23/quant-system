//! Right inspector panel. Shows exactly one selection context at a time,
//! derived from the current route, fixture scenario, and live heartbeat.

use gpui::prelude::*;
use gpui::{Div, Entity, div, px};

use crate::model::{AppRoute, BacktestAppModel};
use crate::preview::Row;
use crate::view::BacktestWindow;
use crate::view::theme;
use crate::view::workspace::render_row;

pub fn inspector(model: &Entity<BacktestAppModel>, cx: &mut Context<BacktestWindow>) -> Div {
    if model.read(cx).inspector_collapsed() {
        return div()
            .w(px(24.))
            .min_w(px(24.))
            .border_l_1()
            .border_color(theme::border())
            .bg(theme::panel_bg())
            .flex()
            .flex_col()
            .items_center()
            .py_2()
            .child(
                div()
                    .text_size(px(theme::SMALL_SIZE))
                    .text_color(theme::dim_text())
                    .child("i"),
            );
    }

    let (title, rows) = {
        let model = model.read(cx);
        let route = model.route().clone();
        let fixture = model.fixture();
        let saved_path = model.saved_path().map(str::to_string);
        let heartbeat_row = model.heartbeat().map(|beat| {
            Row::new(
                "Heartbeat",
                format!(
                    "counter {} - generation {gen} - {rtt} ms",
                    beat.counter,
                    gen = beat.generation,
                    rtt = beat.round_trip.as_millis()
                ),
            )
        });
        match route {
            AppRoute::NewRun {
                step: crate::model::RunStep::Input,
            } => (
                "Input inspector",
                match fixture {
                    crate::preview::FixtureScenario::Warning => vec![
                        Row::new("Line error", "Physical line 1712: unknown position field"),
                        Row::new("Skipped lines", "3"),
                        Row::new("Raw signal bodies", "Not displayed"),
                    ],
                    _ => vec![
                        Row::new("Warnings", "None"),
                        Row::new("Line errors", "None"),
                        Row::new("Raw signal bodies", "Not displayed"),
                    ],
                },
            ),
            AppRoute::NewRun {
                step: crate::model::RunStep::Configure,
            } => (
                "Effective request inspector",
                vec![
                    Row::new("Input digest prefix", "9f2c41ab"),
                    Row::new("Input revision", "3"),
                    Row::new("Form revision", "7"),
                    Row::new(
                        "Catalog generation",
                        if fixture == crate::preview::FixtureScenario::Disconnected {
                            "Unavailable".to_string()
                        } else {
                            "17".to_string()
                        },
                    ),
                    Row::new("Local blockers", "None"),
                    Row::new("Server validation", "Final authority"),
                ],
            ),
            AppRoute::NewRun {
                step: crate::model::RunStep::Review,
            } => (
                "Preparation inspector",
                vec![
                    Row::new("Preparation ID", "12"),
                    Row::new("Input digest prefix", "9f2c41ab"),
                    Row::new("Serialized request bytes", "8192"),
                    Row::new("Output intent", "Persist"),
                    Row::new("Target preflight", "Free"),
                ],
            ),
            AppRoute::NewRun {
                step: crate::model::RunStep::Run,
            } => {
                let mut rows = crate::preview::run_inspector(saved_path.as_deref());
                if let Some(beat_row) = heartbeat_row {
                    rows.push(beat_row);
                }
                ("Run inspector", rows)
            }
            AppRoute::Results { .. } => (
                "Document inspector",
                vec![
                    Row::new("Document digest prefix", "9f2c41ab"),
                    Row::new(
                        "Population",
                        "CompletedPosition: 84 observed / 112 provided",
                    ),
                    Row::new("Completeness", "Positions complete - Execution partial"),
                    Row::new("Policy", "Not selected"),
                    Row::new("Mode", "Exploratory - not validated"),
                ],
            ),
        }
    };

    let mut list = div().flex().flex_col();
    for row in &rows {
        list = list.child(render_row(row));
    }
    div()
        .flex()
        .flex_col()
        .w(px(theme::INSPECTOR_WIDTH))
        .min_w(px(theme::INSPECTOR_WIDTH))
        .border_l_1()
        .border_color(theme::border())
        .bg(theme::panel_bg())
        .overflow_hidden()
        .child(
            div()
                .px_3()
                .py_2()
                .text_size(px(theme::SMALL_SIZE))
                .text_color(theme::dim_text())
                .child(format!("{title} (fixture)")),
        )
        .child(list)
}
