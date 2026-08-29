//! Quant System backtest desktop client entry point.
//!
//! Current scope: a static application shell on a pinned GPUI revision with
//! no backend client, RPC, filesystem workflow, or result decoding.

mod controller;
mod model;
mod preview;
mod view;

use gpui::prelude::*;
use gpui::{App, Bounds, TitlebarOptions, WindowBounds, WindowOptions, px, size};
use gpui_platform::application;
use view::BacktestWindow;

struct AppOptions {
    smoke_secs: Option<u64>,
    developer_preview: bool,
}

/// Parse lifecycle smoke and developer-preview options. Developer preview
/// keeps fixture, IME, and heartbeat controls out of the normal user flow.
fn app_options() -> AppOptions {
    let mut smoke_secs = None;
    let mut developer_preview = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--preview" => developer_preview = true,
            "--smoke-secs" => {
                smoke_secs = args.next().and_then(|value| value.parse::<u64>().ok());
            }
            _ => {}
        }
    }
    if smoke_secs.is_none() {
        smoke_secs = std::env::var("QS_DESKTOP_SMOKE_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok());
    }
    AppOptions {
        smoke_secs,
        developer_preview,
    }
}

fn main() {
    let options = app_options();
    application().run(move |cx: &mut App| {
        gpui_tokio::init(cx);
        view::register_keybindings(cx);
        let bounds = Bounds::centered(
            None,
            size(
                px(view::theme::PHASE_DEFAULT_WIDTH),
                px(view::theme::PHASE_DEFAULT_HEIGHT),
            ),
            cx,
        );
        let window = cx
            .open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                    titlebar: Some(TitlebarOptions {
                        title: Some("Quant System Backtest".into()),
                        ..Default::default()
                    }),
                    window_min_size: Some(size(
                        px(view::theme::PHASE_MIN_WIDTH),
                        px(view::theme::PHASE_MIN_HEIGHT),
                    )),
                    focus: true,
                    ..Default::default()
                },
                move |window, cx| {
                    cx.new(|cx| {
                        BacktestWindow::new(
                            options.smoke_secs,
                            options.developer_preview,
                            window,
                            cx,
                        )
                    })
                },
            )
            .expect("window creation succeeds");
        window
            .update(cx, |_, _, cx| {
                cx.activate(true);
            })
            .expect("window activation succeeds");
        cx.on_window_closed(|cx, _window_id| {
            cx.quit();
        })
        .detach();
    });
}
