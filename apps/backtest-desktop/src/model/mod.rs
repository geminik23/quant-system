//! Application model and reducer for the desktop shell.
//!
//! The model is intentionally free of GPUI types so reducer behavior can be
//! unit tested without a window or platform. Views render model state and
//! emit intents; this module owns the state transitions.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::preview::{self, FixtureScenario};

/// The five wizard phases rendered in the phase bar.
pub const PHASE_LABELS: [&str; 5] = ["Choose input", "Configure", "Review", "Run", "Results"];

/// One step of the new-run wizard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStep {
    Input,
    Configure,
    Review,
    Run,
}

impl RunStep {
    pub const ALL: [RunStep; 4] = [
        RunStep::Input,
        RunStep::Configure,
        RunStep::Review,
        RunStep::Run,
    ];

    pub fn label(self) -> &'static str {
        PHASE_LABELS[self.index()]
    }

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn from_index(index: usize) -> Option<RunStep> {
        RunStep::ALL.get(index).copied()
    }

    pub fn next(self) -> Option<RunStep> {
        RunStep::from_index(self.index() + 1)
    }

    pub fn prev(self) -> Option<RunStep> {
        self.index().checked_sub(1).and_then(RunStep::from_index)
    }
}

/// Result Explorer section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultSection {
    Overview,
    Edge,
    Risk,
    Robustness,
    Time,
    Execution,
    Coverage,
    Positions,
    Metadata,
}

impl ResultSection {
    pub const ALL: [ResultSection; 9] = [
        ResultSection::Overview,
        ResultSection::Edge,
        ResultSection::Risk,
        ResultSection::Robustness,
        ResultSection::Time,
        ResultSection::Execution,
        ResultSection::Coverage,
        ResultSection::Positions,
        ResultSection::Metadata,
    ];

    pub fn label(self) -> &'static str {
        RESULT_SECTION_LABELS[self as usize]
    }

    pub fn from_index(index: usize) -> ResultSection {
        ResultSection::ALL
            .get(index)
            .copied()
            .unwrap_or(ResultSection::Overview)
    }
}

pub const RESULT_SECTION_LABELS: [&str; 9] = [
    "Summary",
    "Returns",
    "Risk",
    "Robustness",
    "Time",
    "Execution",
    "Coverage",
    "Trades",
    "Details",
];

/// Experiment document section for the preview experiment fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExperimentSection {
    Identity,
    Protocol,
    ChildRuns,
    Evidence,
    Verdict,
}

impl ExperimentSection {
    pub const ALL: [ExperimentSection; 5] = [
        ExperimentSection::Identity,
        ExperimentSection::Protocol,
        ExperimentSection::ChildRuns,
        ExperimentSection::Evidence,
        ExperimentSection::Verdict,
    ];

    pub fn label(self) -> &'static str {
        EXPERIMENT_SECTION_LABELS[self as usize]
    }

    pub fn from_index(index: usize) -> ExperimentSection {
        ExperimentSection::ALL
            .get(index)
            .copied()
            .unwrap_or(ExperimentSection::Identity)
    }
}

pub const EXPERIMENT_SECTION_LABELS: [&str; 5] =
    ["Identity", "Protocol", "Child runs", "Evidence", "Verdict"];

/// Section selection for an open document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentSection {
    Result(ResultSection),
    Experiment(ExperimentSection),
}

impl DocumentSection {
    pub fn label(self) -> &'static str {
        match self {
            DocumentSection::Result(section) => section.label(),
            DocumentSection::Experiment(section) => section.label(),
        }
    }
}

/// Which offline document the Results route currently shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenDocumentKind {
    Result,
    Experiment,
}

/// Top-level route of the application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppRoute {
    NewRun {
        step: RunStep,
    },
    Results {
        document: OpenDocumentKind,
        section: DocumentSection,
    },
}

/// The single app-wide active execution, covering runs and experiment children.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActiveExecution {
    None,
    SingleRun {
        local_run_id: u64,
    },
    // Experiment child execution arrives with the experiment phase. The
    // reducer guard and display already cover this variant.
    #[allow(dead_code)]
    ExperimentChild {
        experiment_id: String,
        child_id: String,
        local_run_id: u64,
    },
}

impl ActiveExecution {
    pub fn is_active(&self) -> bool {
        !matches!(self, ActiveExecution::None)
    }
}

/// Error for the global single-execution guard.
#[derive(Debug, PartialEq, Eq)]
pub enum ExecutionStartError {
    AlreadyActive,
}

/// Phase bar chip presentation state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhaseChipState {
    Done,
    Current,
    Pending,
}

/// A rendered phase bar chip.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhaseChip {
    pub label: &'static str,
    pub state: PhaseChipState,
}

/// Result availability for the Results route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultsPresentation {
    Evidence,
    NotPersisted,
    AnalysisUnavailable { reason: String },
}

/// Heartbeat payload returned from a ping task.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatPayload {
    pub round_trip: Duration,
}

/// Latest accepted heartbeat snapshot.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatSnapshot {
    pub generation: u64,
    pub counter: u64,
    pub round_trip: Duration,
    pub received_at: Instant,
}

/// Dismissable user notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserNotification {
    pub warning: bool,
    pub message: String,
}

/// Outcome of a native file dialog prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DialogOutcome {
    Selected(PathBuf),
    Cancelled,
    Failed(String),
}

/// Successful service catalog presentation state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionCatalogView {
    pub endpoint: String,
    pub status: String,
    pub uptime_secs: u64,
    pub profile_count: usize,
    pub symbol_count: usize,
    pub loaded_at: String,
}

/// Safe service connection failure presentation state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionFailureView {
    pub endpoint: String,
    pub message: String,
    pub technical_detail: String,
}

/// User-observable state of an explicit service connection test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceConnectionState {
    Idle,
    Connecting { endpoint: String },
    Connected(ConnectionCatalogView),
    Failed(ConnectionFailureView),
}

/// Root application model.
pub struct BacktestAppModel {
    route: AppRoute,
    visited: [bool; 4],
    nav_index: usize,
    fixture: FixtureScenario,
    results_from_document: bool,
    active_execution: ActiveExecution,
    next_local_run_id: u64,
    inspector_collapsed: bool,
    notification: Option<UserNotification>,
    heartbeat: Option<HeartbeatSnapshot>,
    ping_generation: u64,
    ping_running: bool,
    connection_generation: u64,
    connection: ServiceConnectionState,
    input_path_override: Option<String>,
    saved_path: Option<String>,
    document_display_name: String,
}

impl BacktestAppModel {
    pub fn new(fixture: FixtureScenario) -> Self {
        let mut model = Self {
            route: AppRoute::NewRun {
                step: RunStep::Input,
            },
            visited: [true, false, false, false],
            nav_index: 0,
            fixture,
            results_from_document: false,
            active_execution: ActiveExecution::None,
            next_local_run_id: 1,
            inspector_collapsed: true,
            notification: None,
            heartbeat: None,
            ping_generation: 0,
            ping_running: false,
            connection_generation: 0,
            connection: ServiceConnectionState::Idle,
            input_path_override: None,
            saved_path: None,
            document_display_name: "eurusd_lifecycle_result.json".into(),
        };
        model.apply_fixture_route();
        model
    }

    pub fn route(&self) -> &AppRoute {
        &self.route
    }

    pub fn fixture(&self) -> FixtureScenario {
        self.fixture
    }

    pub fn nav_index(&self) -> usize {
        self.nav_index
    }

    pub fn connection(&self) -> &ServiceConnectionState {
        &self.connection
    }

    pub fn connection_ready(&self) -> bool {
        matches!(self.connection, ServiceConnectionState::Connected(_))
    }

    pub fn begin_connection_test(&mut self, endpoint: String) -> u64 {
        self.connection_generation += 1;
        self.connection = ServiceConnectionState::Connecting { endpoint };
        self.connection_generation
    }

    pub fn apply_connection_result(
        &mut self,
        generation: u64,
        result: Result<ConnectionCatalogView, ConnectionFailureView>,
    ) -> bool {
        if generation != self.connection_generation {
            return false;
        }
        self.connection = match result {
            Ok(catalog) => ServiceConnectionState::Connected(catalog),
            Err(failure) => ServiceConnectionState::Failed(failure),
        };
        true
    }

    /// Section list for the context navigation of the current route.
    pub fn nav_sections(&self) -> Vec<&'static str> {
        match &self.route {
            AppRoute::NewRun { step } => match step {
                RunStep::Input => vec!["Choose file", "File details"],
                RunStep::Configure => vec!["Basic settings", "Advanced settings"],
                RunStep::Review => vec!["Review and start"],
                RunStep::Run => vec!["Progress", "Technical details"],
            },
            AppRoute::Results { document, .. } => match document {
                OpenDocumentKind::Result => {
                    ResultSection::ALL.map(|section| section.label()).to_vec()
                }
                OpenDocumentKind::Experiment => ExperimentSection::ALL
                    .map(|section| section.label())
                    .to_vec(),
            },
        }
    }

    pub fn select_nav(&mut self, index: usize) {
        let len = self.nav_sections().len();
        self.nav_index = index.min(len.saturating_sub(1));
        self.sync_section_from_nav();
    }

    pub fn move_nav(&mut self, delta: i32) -> bool {
        let len = self.nav_sections().len() as i32;
        let candidate = self.nav_index as i32 + delta;
        if candidate < 0 || candidate >= len {
            return false;
        }
        self.nav_index = candidate as usize;
        self.sync_section_from_nav();
        true
    }

    fn sync_section_from_nav(&mut self) {
        if let AppRoute::Results { document, section } = &mut self.route {
            match document {
                OpenDocumentKind::Result => {
                    *section = DocumentSection::Result(ResultSection::from_index(self.nav_index));
                }
                OpenDocumentKind::Experiment => {
                    *section =
                        DocumentSection::Experiment(ExperimentSection::from_index(self.nav_index));
                }
            }
        }
    }

    pub fn goto_step(&mut self, step: RunStep) {
        self.visited[step.index()] = true;
        self.nav_index = 0;
        self.route = AppRoute::NewRun { step };
    }

    /// Advance the wizard. From the Run step it moves to the Results route as
    /// an in-session completion, not an offline document open.
    pub fn go_next_step(&mut self) -> bool {
        match self.route.clone() {
            AppRoute::NewRun { step } => match step.next() {
                Some(next) => {
                    self.goto_step(next);
                    true
                }
                None => {
                    self.visited = [true, true, true, true];
                    self.nav_index = 0;
                    self.results_from_document = false;
                    self.route = AppRoute::Results {
                        document: OpenDocumentKind::Result,
                        section: DocumentSection::Result(ResultSection::Overview),
                    };
                    true
                }
            },
            AppRoute::Results { .. } => false,
        }
    }

    pub fn go_prev_step(&mut self) -> bool {
        match self.route.clone() {
            AppRoute::NewRun { step } => match step.prev() {
                Some(prev) => {
                    self.goto_step(prev);
                    true
                }
                None => false,
            },
            AppRoute::Results { .. } => {
                self.route = AppRoute::NewRun { step: RunStep::Run };
                self.nav_index = 0;
                true
            }
        }
    }

    pub fn new_backtest(&mut self) {
        self.route = AppRoute::NewRun {
            step: RunStep::Input,
        };
        self.visited = [true, false, false, false];
        self.nav_index = 0;
        self.results_from_document = false;
        if self.active_execution.is_active() {
            self.notify_info("Active execution continues; a second execution is not permitted.");
        }
    }

    /// Open an offline document fixture. Phase bar switches to document mode
    /// and never shows the wizard steps as completed.
    pub fn open_document(&mut self, document: OpenDocumentKind) {
        let section = match document {
            OpenDocumentKind::Result => DocumentSection::Result(ResultSection::Overview),
            OpenDocumentKind::Experiment => {
                DocumentSection::Experiment(ExperimentSection::Identity)
            }
        };
        self.route = AppRoute::Results { document, section };
        self.results_from_document = true;
        self.nav_index = 0;
    }

    pub fn toggle_document_kind(&mut self) {
        let current = match &self.route {
            AppRoute::Results { document, .. } => *document,
            AppRoute::NewRun { .. } => OpenDocumentKind::Result,
        };
        let next = match current {
            OpenDocumentKind::Result => OpenDocumentKind::Experiment,
            OpenDocumentKind::Experiment => OpenDocumentKind::Result,
        };
        self.open_document(next);
    }

    pub fn set_fixture(&mut self, fixture: FixtureScenario) {
        self.fixture = fixture;
        self.apply_fixture_route();
    }

    pub fn cycle_fixture(&mut self, delta: i32) {
        let len = FixtureScenario::ALL.len() as i32;
        let index = FixtureScenario::ALL
            .iter()
            .position(|scenario| *scenario == self.fixture)
            .unwrap_or(0) as i32;
        let next = (index + delta).rem_euclid(len) as usize;
        self.set_fixture(FixtureScenario::ALL[next]);
    }

    /// Fixture scenarios drive a representative route so each state is visible.
    fn apply_fixture_route(&mut self) {
        match self.fixture {
            FixtureScenario::PersistedResult | FixtureScenario::Unavailable => {
                self.open_document(OpenDocumentKind::Result);
            }
            FixtureScenario::SummaryOnly => {
                self.route = AppRoute::NewRun { step: RunStep::Run };
                self.nav_index = 0;
                self.results_from_document = false;
            }
            FixtureScenario::Disconnected => {
                self.route = AppRoute::NewRun {
                    step: RunStep::Configure,
                };
                self.nav_index = 0;
                self.results_from_document = false;
            }
            FixtureScenario::Warning => {
                self.route = AppRoute::NewRun {
                    step: RunStep::Input,
                };
                self.nav_index = 0;
                self.results_from_document = false;
            }
        }
    }

    pub fn phase_chips(&self) -> [PhaseChip; 5] {
        let mut chips = [PhaseChip {
            label: PHASE_LABELS[0],
            state: PhaseChipState::Pending,
        }; 5];
        for (index, chip) in chips.iter_mut().enumerate() {
            chip.label = PHASE_LABELS[index];
        }
        match &self.route {
            AppRoute::NewRun { step } => {
                let current = step.index();
                for (index, chip) in chips.iter_mut().enumerate().take(4) {
                    chip.state = if index == current {
                        PhaseChipState::Current
                    } else if self.visited[index] && index < current {
                        PhaseChipState::Done
                    } else {
                        PhaseChipState::Pending
                    };
                }
                chips[4].state = PhaseChipState::Pending;
            }
            AppRoute::Results { .. } => {
                if self.results_from_document {
                    for chip in chips.iter_mut().take(4) {
                        chip.state = PhaseChipState::Pending;
                    }
                } else {
                    for chip in chips.iter_mut().take(4) {
                        chip.state = PhaseChipState::Done;
                    }
                }
                chips[4].state = PhaseChipState::Current;
            }
        }
        chips
    }

    pub fn phase_context(&self) -> Option<&'static str> {
        match &self.route {
            AppRoute::Results { .. } if self.results_from_document => {
                Some("Opened offline - Local document")
            }
            AppRoute::Results { .. } => Some("Run completed - Persisted results"),
            AppRoute::NewRun { .. } => None,
        }
    }

    pub fn inspector_collapsed(&self) -> bool {
        self.inspector_collapsed
    }

    pub fn toggle_inspector(&mut self) {
        self.inspector_collapsed = !self.inspector_collapsed;
    }

    /// Start the single app-wide execution. A second start is rejected by the
    /// reducer itself, not only by disabled view buttons.
    pub fn begin_execution(
        &mut self,
        execution: ActiveExecution,
    ) -> Result<(), ExecutionStartError> {
        if self.active_execution.is_active() {
            return Err(ExecutionStartError::AlreadyActive);
        }
        if let ActiveExecution::SingleRun { local_run_id }
        | ActiveExecution::ExperimentChild { local_run_id, .. } = &execution
        {
            self.next_local_run_id = self.next_local_run_id.max(local_run_id + 1);
        }
        self.active_execution = execution;
        Ok(())
    }

    pub fn begin_single_run(&mut self) -> Result<u64, ExecutionStartError> {
        let local_run_id = self.next_local_run_id;
        self.begin_execution(ActiveExecution::SingleRun { local_run_id })?;
        Ok(local_run_id)
    }

    pub fn end_execution(&mut self) {
        self.active_execution = ActiveExecution::None;
    }

    pub fn active_execution(&self) -> &ActiveExecution {
        &self.active_execution
    }

    pub fn begin_ping_session(&mut self) -> u64 {
        self.ping_generation += 1;
        self.ping_running = true;
        self.heartbeat = None;
        self.ping_generation
    }

    pub fn stop_ping(&mut self) {
        self.ping_running = false;
    }

    pub fn ping_running(&self) -> bool {
        self.ping_running
    }

    /// Accept a heartbeat result for the current ping session. Stale results
    /// from an older generation are rejected so they never overwrite newer
    /// model state.
    pub fn apply_heartbeat(&mut self, generation: u64, payload: HeartbeatPayload) -> bool {
        if !self.ping_running || generation != self.ping_generation {
            return false;
        }
        let counter = self.heartbeat.as_ref().map_or(1, |beat| beat.counter + 1);
        self.heartbeat = Some(HeartbeatSnapshot {
            generation,
            counter,
            round_trip: payload.round_trip,
            received_at: Instant::now(),
        });
        true
    }

    pub fn heartbeat(&self) -> Option<&HeartbeatSnapshot> {
        self.heartbeat.as_ref()
    }

    pub fn notify_info(&mut self, message: impl Into<String>) {
        self.notification = Some(UserNotification {
            warning: false,
            message: message.into(),
        });
    }

    pub fn notify_warning(&mut self, message: impl Into<String>) {
        self.notification = Some(UserNotification {
            warning: true,
            message: message.into(),
        });
    }

    pub fn notification(&self) -> Option<&UserNotification> {
        self.notification.as_ref()
    }

    pub fn dismiss_notification(&mut self) {
        self.notification = None;
    }

    /// Record the outcome of the native open dialog. User cancellation is a
    /// normal outcome and never raises a notification.
    pub fn record_open_dialog(&mut self, outcome: DialogOutcome) {
        match outcome {
            DialogOutcome::Selected(path) => {
                let display = path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.display().to_string());
                if path
                    .extension()
                    .is_some_and(|extension| extension == "jsonl")
                {
                    self.input_path_override = Some(display.clone());
                    self.notify_info(format!("Input selected (preview): {display}"));
                } else if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    self.document_display_name = display.clone();
                    self.open_document(OpenDocumentKind::Result);
                    self.notify_info(format!("Document selected (preview): {display}"));
                } else {
                    self.notify_warning(format!(
                        "Unsupported selection (preview validation): {display}"
                    ));
                }
            }
            DialogOutcome::Cancelled => {}
            DialogOutcome::Failed(message) => {
                self.notify_warning(format!("Open dialog failed: {message}"));
            }
        }
    }

    /// Record the outcome of the native save dialog.
    pub fn record_save_dialog(&mut self, outcome: DialogOutcome) {
        match outcome {
            DialogOutcome::Selected(path) => {
                self.saved_path = Some(path.display().to_string());
                self.notify_info(format!(
                    "Save target recorded (preview): {}",
                    path.display()
                ));
            }
            DialogOutcome::Cancelled => {}
            DialogOutcome::Failed(message) => {
                self.notify_warning(format!("Save dialog failed: {message}"));
            }
        }
    }

    pub fn selected_input_display_name(&self) -> Option<&str> {
        self.input_path_override.as_deref()
    }

    pub fn input_display_name(&self) -> String {
        self.input_path_override
            .clone()
            .unwrap_or_else(|| preview::DEFAULT_INPUT_NAME.to_string())
    }

    pub fn saved_path(&self) -> Option<&str> {
        self.saved_path.as_deref()
    }

    /// Which presentation the Results route should render.
    pub fn results_presentation(&self) -> ResultsPresentation {
        match self.fixture {
            FixtureScenario::SummaryOnly => ResultsPresentation::NotPersisted,
            FixtureScenario::Unavailable => ResultsPresentation::AnalysisUnavailable {
                reason: "Optional normalized dataset is absent in this document".into(),
            },
            _ => ResultsPresentation::Evidence,
        }
    }

    pub fn identity_label(&self) -> String {
        match &self.route {
            AppRoute::NewRun { .. } => "New backtest".into(),
            AppRoute::Results { document, .. } => match document {
                OpenDocumentKind::Result => self.document_display_name.clone(),
                OpenDocumentKind::Experiment => "exp-2026-08-29-a (fixture)".into(),
            },
        }
    }

    pub fn context_chip_label(&self) -> &'static str {
        match &self.route {
            AppRoute::NewRun { .. } => "New run",
            AppRoute::Results { document, .. } => match document {
                OpenDocumentKind::Result => "Result - Offline",
                OpenDocumentKind::Experiment => "Experiment - Offline",
            },
        }
    }

    pub fn results_from_document(&self) -> bool {
        self.results_from_document
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(fixture: FixtureScenario) -> BacktestAppModel {
        BacktestAppModel::new(fixture)
    }

    #[test]
    fn wizard_advances_through_all_steps_into_results() {
        let mut model = model(FixtureScenario::Warning);
        model.route = AppRoute::NewRun {
            step: RunStep::Input,
        };
        assert!(model.go_next_step());
        assert_eq!(
            model.route,
            AppRoute::NewRun {
                step: RunStep::Configure
            }
        );
        assert!(model.go_next_step());
        assert_eq!(
            model.route,
            AppRoute::NewRun {
                step: RunStep::Review
            }
        );
        assert!(model.go_next_step());
        assert_eq!(model.route, AppRoute::NewRun { step: RunStep::Run });
        assert!(model.go_next_step());
        assert!(matches!(model.route, AppRoute::Results { .. }));

        let chips = model.phase_chips();
        assert_eq!(chips[4].state, PhaseChipState::Current);
        for chip in chips.iter().take(4) {
            assert_eq!(chip.state, PhaseChipState::Done);
        }
        assert!(model.go_prev_step());
        assert_eq!(model.route, AppRoute::NewRun { step: RunStep::Run });
    }

    #[test]
    fn offline_document_does_not_mark_wizard_steps_done() {
        let mut model = model(FixtureScenario::PersistedResult);
        model.open_document(OpenDocumentKind::Result);
        let chips = model.phase_chips();
        for chip in chips.iter().take(4) {
            assert_eq!(chip.state, PhaseChipState::Pending);
        }
        assert_eq!(chips[4].state, PhaseChipState::Current);
        assert_eq!(
            model.phase_context(),
            Some("Opened offline - Local document")
        );
    }

    #[test]
    fn open_document_is_mutually_exclusive() {
        let mut model = model(FixtureScenario::PersistedResult);
        model.open_document(OpenDocumentKind::Result);
        assert_eq!(
            model.route,
            AppRoute::Results {
                document: OpenDocumentKind::Result,
                section: DocumentSection::Result(ResultSection::Overview)
            }
        );
        model.open_document(OpenDocumentKind::Experiment);
        assert_eq!(
            model.route,
            AppRoute::Results {
                document: OpenDocumentKind::Experiment,
                section: DocumentSection::Experiment(ExperimentSection::Identity)
            }
        );
    }

    #[test]
    fn active_execution_is_globally_single_instance() {
        let mut model = model(FixtureScenario::Warning);
        model.active_execution = ActiveExecution::None;
        let run_id = model.begin_single_run().expect("first execution starts");
        assert!(run_id >= 1);
        assert!(model.begin_single_run().is_err());
        let child = ActiveExecution::ExperimentChild {
            experiment_id: "exp-1".into(),
            child_id: "child-1".into(),
            local_run_id: 99,
        };
        let error = model.begin_execution(child);
        assert_eq!(error.unwrap_err(), ExecutionStartError::AlreadyActive);
        model.end_execution();
        assert!(model.begin_single_run().is_ok());
    }

    #[test]
    fn heartbeat_rejects_stale_generation_and_increments_counter() {
        let mut model = model(FixtureScenario::Warning);
        let generation = model.begin_ping_session();
        let payload = HeartbeatPayload {
            round_trip: Duration::from_millis(12),
        };
        assert!(model.apply_heartbeat(generation, payload));
        assert_eq!(model.heartbeat().unwrap().counter, 1);
        assert!(model.apply_heartbeat(generation, payload));
        assert_eq!(model.heartbeat().unwrap().counter, 2);

        let newer = model.begin_ping_session();
        assert_ne!(generation, newer);
        assert!(model.heartbeat().is_none());
        assert!(!model.apply_heartbeat(generation, payload));
        assert!(model.heartbeat().is_none());
        assert!(model.apply_heartbeat(newer, payload));
        assert_eq!(model.heartbeat().unwrap().counter, 1);

        model.stop_ping();
        assert!(!model.apply_heartbeat(newer, payload));
    }

    #[test]
    fn nav_selection_moves_within_bounds_and_syncs_sections() {
        let mut model = model(FixtureScenario::PersistedResult);
        model.open_document(OpenDocumentKind::Result);
        assert!(!model.move_nav(-1));
        for _ in 0..20 {
            model.move_nav(1);
        }
        let chips = model.nav_sections();
        assert_eq!(model.nav_index(), chips.len() - 1);
        assert_eq!(
            model.route,
            AppRoute::Results {
                document: OpenDocumentKind::Result,
                section: DocumentSection::Result(ResultSection::Metadata)
            }
        );
    }

    #[test]
    fn fixture_scenarios_drive_connection_and_results_presentation() {
        let disconnected = model(FixtureScenario::Disconnected);
        assert!(matches!(
            disconnected.route,
            AppRoute::NewRun {
                step: RunStep::Configure
            }
        ));

        let summary_only = model(FixtureScenario::SummaryOnly);
        assert!(matches!(
            summary_only.results_presentation(),
            ResultsPresentation::NotPersisted
        ));

        let unavailable = model(FixtureScenario::Unavailable);
        assert!(matches!(
            unavailable.results_presentation(),
            ResultsPresentation::AnalysisUnavailable { .. }
        ));

        let persisted = model(FixtureScenario::PersistedResult);
        assert!(matches!(
            persisted.results_presentation(),
            ResultsPresentation::Evidence
        ));
    }

    #[test]
    fn connection_test_rejects_stale_generation() {
        let mut model = model(FixtureScenario::PersistedResult);
        let first = model.begin_connection_test("tcp://127.0.0.1:41001".into());
        let second = model.begin_connection_test("tcp://127.0.0.1:41002".into());
        assert!(!model.apply_connection_result(
            first,
            Ok(ConnectionCatalogView {
                endpoint: "tcp://127.0.0.1:41001".into(),
                status: "ok".into(),
                uptime_secs: 1,
                profile_count: 1,
                symbol_count: 1,
                loaded_at: "now".into(),
            }),
        ));
        assert!(matches!(
            model.connection(),
            ServiceConnectionState::Connecting { endpoint }
                if endpoint == "tcp://127.0.0.1:41002"
        ));
        assert!(model.apply_connection_result(
            second,
            Err(ConnectionFailureView {
                endpoint: "tcp://127.0.0.1:41002".into(),
                message: "failed".into(),
                technical_detail: "detail".into(),
            }),
        ));
        assert!(matches!(
            model.connection(),
            ServiceConnectionState::Failed(_)
        ));
    }

    #[test]
    fn dialog_cancellation_is_silent() {
        let mut model = model(FixtureScenario::Warning);
        model.record_open_dialog(DialogOutcome::Cancelled);
        model.record_save_dialog(DialogOutcome::Cancelled);
        assert!(model.notification().is_none());
    }

    #[test]
    fn dialog_selections_route_by_extension() {
        let mut model = model(FixtureScenario::Warning);
        model.route = AppRoute::NewRun {
            step: RunStep::Input,
        };
        model.record_open_dialog(DialogOutcome::Selected("signals.jsonl".into()));
        assert_eq!(model.input_display_name(), "signals.jsonl");
        assert!(matches!(
            model.notification(),
            Some(UserNotification { warning: false, .. })
        ));

        model.record_open_dialog(DialogOutcome::Selected("result.json".into()));
        assert!(matches!(model.route, AppRoute::Results { .. }));
        assert_eq!(model.identity_label(), "result.json");

        model.notify_warning("unsupported");
        model.record_open_dialog(DialogOutcome::Selected("archive.zip".into()));
        assert!(matches!(
            model.notification(),
            Some(UserNotification { warning: true, .. })
        ));
    }

    #[test]
    fn dialog_failures_raise_warnings() {
        let mut model = model(FixtureScenario::Warning);
        model.record_open_dialog(DialogOutcome::Failed("platform".into()));
        assert!(matches!(
            model.notification(),
            Some(UserNotification { warning: true, .. })
        ));
        model.record_save_dialog(DialogOutcome::Failed("receiver".into()));
        assert!(matches!(
            model.notification(),
            Some(UserNotification { warning: true, .. })
        ));
    }

    #[test]
    fn new_backtest_resets_the_wizard() {
        let mut model = model(FixtureScenario::PersistedResult);
        model.open_document(OpenDocumentKind::Result);
        model.new_backtest();
        assert_eq!(
            model.route,
            AppRoute::NewRun {
                step: RunStep::Input
            }
        );
        let chips = model.phase_chips();
        assert_eq!(chips[0].state, PhaseChipState::Current);
        assert_eq!(chips[1].state, PhaseChipState::Pending);
    }

    #[test]
    fn default_user_flow_starts_empty_and_uses_task_navigation() {
        let mut model = model(FixtureScenario::PersistedResult);
        model.new_backtest();
        assert!(model.selected_input_display_name().is_none());
        assert_eq!(model.nav_sections(), vec!["Choose file", "File details"]);
        assert!(model.inspector_collapsed());

        model.goto_step(RunStep::Configure);
        assert_eq!(
            model.nav_sections(),
            vec!["Basic settings", "Advanced settings"]
        );
        model.goto_step(RunStep::Run);
        assert_eq!(model.nav_sections(), vec!["Progress", "Technical details"]);
    }

    #[test]
    fn inspector_toggle_and_notification_dismiss() {
        let mut model = model(FixtureScenario::Warning);
        assert!(model.inspector_collapsed());
        model.toggle_inspector();
        assert!(!model.inspector_collapsed());
        model.notify_info("note");
        assert!(model.notification().is_some());
        model.dismiss_notification();
        assert!(model.notification().is_none());
    }

    #[test]
    fn fixture_cycling_wraps_around() {
        let mut model = model(FixtureScenario::Unavailable);
        model.cycle_fixture(1);
        assert_eq!(model.fixture(), FixtureScenario::PersistedResult);
        model.cycle_fixture(-1);
        assert_eq!(model.fixture(), FixtureScenario::Unavailable);
    }
}
