//! Root window view for the static desktop shell.

pub mod chart;
pub mod inspector;
pub mod nav;
pub mod shell;
pub mod text_input;
pub mod theme;
pub mod workspace;

use gpui::prelude::*;
use gpui::{
    App, Context, Entity, FocusHandle, KeyBinding, Render, Subscription, Task, Window, actions,
    div, px,
};

use crate::controller::{self, BackendBridge};
use crate::model::BacktestAppModel;
use crate::preview::FixtureScenario;
use crate::view::text_input::PreviewTextInput;

actions!(
    qs_backtest_desktop,
    [
        NextPhase,
        PreviousPhase,
        NextSection,
        PreviousSection,
        ToggleInspector,
        NewBacktest,
        OpenDocumentPrompt,
        SaveAsPrompt,
        NextFixture,
        PreviousFixture,
        TogglePing,
        DismissNotification,
        StartFixtureRun,
        Quit
    ]
);

/// Single application window. Owns the model entity, the backend bridge, and
/// the long-lived subscriptions and tasks so nothing is detached implicitly.
pub struct BacktestWindow {
    pub model: Entity<BacktestAppModel>,
    pub text_input: Entity<PreviewTextInput>,
    pub endpoint_input: Entity<PreviewTextInput>,
    backend: BackendBridge,
    focus_handle: FocusHandle,
    _model_subscription: Subscription,
    held_tasks: Vec<Task<()>>,
    dialog_open: bool,
    developer_preview: bool,
}

impl BacktestWindow {
    pub fn new(
        smoke_secs: Option<u64>,
        developer_preview: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let model = cx.new(|_| {
            let mut model = BacktestAppModel::new(FixtureScenario::PersistedResult);
            if !developer_preview {
                model.new_backtest();
            }
            model
        });
        let text_input = cx.new(|cx| PreviewTextInput::new(cx, "Type here (IME check)"));
        let endpoint_input = cx.new(|cx| {
            PreviewTextInput::new_with_content(cx, "tcp://127.0.0.1:41001", "tcp://127.0.0.1:41001")
        });
        let model_subscription = cx.observe(&model, |_, _, cx| cx.notify());
        let mut view = Self {
            model: model.clone(),
            text_input,
            endpoint_input,
            backend: BackendBridge::new(),
            focus_handle: cx.focus_handle(),
            _model_subscription: model_subscription,
            held_tasks: Vec::new(),
            dialog_open: false,
            developer_preview,
        };
        view.backend.start_heartbeat(&model, cx);
        if let Some(secs) = smoke_secs {
            let smoke = controller::start_smoke_quit(secs, cx);
            view.held_tasks.push(smoke);
        }
        window.focus(&view.focus_handle, cx);
        view
    }

    fn open_document_prompt(&mut self, cx: &mut Context<Self>) {
        if self.dialog_open {
            self.model.update(cx, |model, cx| {
                model.notify_info("A file dialog is already open.");
                cx.notify();
            });
            return;
        }
        self.dialog_open = true;
        let task = controller::prompt_open(&self.model, cx);
        self.held_tasks.push(task);
    }

    fn save_summary_as(&mut self, cx: &mut Context<Self>) {
        if self.dialog_open {
            self.model.update(cx, |model, cx| {
                model.notify_info("A file dialog is already open.");
                cx.notify();
            });
            return;
        }
        self.dialog_open = true;
        let task = controller::prompt_save_as(&self.model, cx);
        self.held_tasks.push(task);
    }

    /// Clear the single-dialog guard once a prompt task has finished.
    pub fn finish_dialog(&mut self) {
        self.dialog_open = false;
    }

    fn toggle_ping(&mut self, cx: &mut Context<Self>) {
        self.backend.toggle_heartbeat(&self.model, cx);
    }

    fn test_service_connection(&mut self, cx: &mut Context<Self>) {
        let endpoint = self.endpoint_input.read(cx).content().trim().to_string();
        let task = controller::test_service_connection(endpoint, &self.model, cx);
        self.held_tasks.push(task);
    }

    fn start_fixture_run(&mut self, cx: &mut Context<Self>) {
        let allow_static_preview = self.developer_preview;
        self.model.update(cx, |model, cx| {
            if !allow_static_preview && !model.connection_ready() {
                model.notify_warning(
                    "Test the backtest server connection in Advanced settings before starting.",
                );
                cx.notify();
                return;
            }
            if model.fixture() == FixtureScenario::Warning {
                model.notify_warning("Resolve the input warnings before starting the backtest.");
                cx.notify();
                return;
            }
            match model.begin_single_run() {
                Ok(local_run_id) => {
                    model.goto_step(crate::model::RunStep::Run);
                    model.notify_info(format!(
                        "Run UI preview started (local preview {local_run_id}); no backtest was submitted."
                    ));
                }
                Err(_) => {
                    model.notify_warning(
                        "A backtest is already running. Wait for it to finish, cancel it, or leave it running in the background.",
                    );
                }
            }
            cx.notify();
        });
    }
}

impl Render for BacktestWindow {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let model = self.model.clone();
        let text_input = self.text_input.clone();
        let endpoint_input = self.endpoint_input.clone();
        div()
            .id("backtest-desktop-root")
            .key_context("BacktestDesktop")
            .track_focus(&self.focus_handle)
            .flex()
            .flex_col()
            .size_full()
            .bg(theme::window_bg())
            .text_color(theme::text())
            .text_size(px(theme::TEXT_SIZE))
            .on_action(cx.listener(Self::handle_next_phase))
            .on_action(cx.listener(Self::handle_previous_phase))
            .on_action(cx.listener(Self::handle_next_section))
            .on_action(cx.listener(Self::handle_previous_section))
            .on_action(cx.listener(Self::handle_toggle_inspector))
            .on_action(cx.listener(Self::handle_new_backtest))
            .on_action(cx.listener(Self::handle_open_document_prompt))
            .on_action(cx.listener(Self::handle_save_as_prompt))
            .on_action(cx.listener(Self::handle_next_fixture))
            .on_action(cx.listener(Self::handle_previous_fixture))
            .on_action(cx.listener(Self::handle_toggle_ping))
            .on_action(cx.listener(Self::handle_dismiss_notification))
            .on_action(cx.listener(Self::handle_start_fixture_run))
            .child(shell::header(&model, self.developer_preview, cx))
            .child(shell::phase_bar(&model, cx))
            .child(shell::notification(&model, cx))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .child(nav::context_nav(
                        &model,
                        &text_input,
                        self.developer_preview,
                        cx,
                    ))
                    .child(workspace::workspace(&model, &endpoint_input, cx))
                    .child(inspector::inspector(&model, cx)),
            )
            .child(shell::status_bar(&model, self.developer_preview, cx))
    }
}

impl BacktestWindow {
    fn handle_next_phase(&mut self, _: &NextPhase, _: &mut Window, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            model.go_next_step();
            cx.notify();
        });
    }

    fn handle_previous_phase(&mut self, _: &PreviousPhase, _: &mut Window, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            model.go_prev_step();
            cx.notify();
        });
    }

    fn handle_next_section(&mut self, _: &NextSection, _: &mut Window, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            model.move_nav(1);
            cx.notify();
        });
    }

    fn handle_previous_section(
        &mut self,
        _: &PreviousSection,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.model.update(cx, |model, cx| {
            model.move_nav(-1);
            cx.notify();
        });
    }

    fn handle_toggle_inspector(
        &mut self,
        _: &ToggleInspector,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.model.update(cx, |model, cx| {
            model.toggle_inspector();
            cx.notify();
        });
    }

    fn handle_new_backtest(&mut self, _: &NewBacktest, _: &mut Window, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            model.new_backtest();
            cx.notify();
        });
    }

    fn handle_open_document_prompt(
        &mut self,
        _: &OpenDocumentPrompt,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.open_document_prompt(cx);
    }

    fn handle_save_as_prompt(&mut self, _: &SaveAsPrompt, _: &mut Window, cx: &mut Context<Self>) {
        self.save_summary_as(cx);
    }

    fn handle_next_fixture(&mut self, _: &NextFixture, _: &mut Window, cx: &mut Context<Self>) {
        self.model.update(cx, |model, cx| {
            model.cycle_fixture(1);
            cx.notify();
        });
    }

    fn handle_previous_fixture(
        &mut self,
        _: &PreviousFixture,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.model.update(cx, |model, cx| {
            model.cycle_fixture(-1);
            cx.notify();
        });
    }

    fn handle_toggle_ping(&mut self, _: &TogglePing, _: &mut Window, cx: &mut Context<Self>) {
        self.toggle_ping(cx);
    }

    fn handle_dismiss_notification(
        &mut self,
        _: &DismissNotification,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.model.update(cx, |model, cx| {
            model.dismiss_notification();
            cx.notify();
        });
    }

    fn handle_start_fixture_run(
        &mut self,
        _: &StartFixtureRun,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.start_fixture_run(cx);
    }
}

/// Register the shell and input keybindings plus the global quit action.
pub fn register_keybindings(cx: &mut App) {
    cx.bind_keys([
        KeyBinding::new("alt-right", NextPhase, None),
        KeyBinding::new("alt-left", PreviousPhase, None),
        KeyBinding::new("ctrl-down", NextSection, None),
        KeyBinding::new("ctrl-up", PreviousSection, None),
        KeyBinding::new("ctrl-i", ToggleInspector, None),
        KeyBinding::new("ctrl-n", NewBacktest, None),
        KeyBinding::new("ctrl-o", OpenDocumentPrompt, None),
        KeyBinding::new("ctrl-s", SaveAsPrompt, None),
        KeyBinding::new("ctrl-]", NextFixture, None),
        KeyBinding::new("ctrl-[", PreviousFixture, None),
        KeyBinding::new("ctrl-p", TogglePing, None),
        KeyBinding::new("escape", DismissNotification, None),
        KeyBinding::new("enter", StartFixtureRun, None),
        KeyBinding::new("ctrl-q", Quit, None),
    ]);
    cx.on_action(|_: &Quit, cx| cx.quit());
    text_input::register_keybindings(cx);
}
