use std::collections::{BTreeMap, BTreeSet, VecDeque};

use qs_instruments::{Decimal, DecimalGrid};
use qs_strategy::{CommandFact, CommandFeedback, CommandTerminalStatus, ConfiguredActionKind};

use crate::types::{MAX_REASON_BYTES, bounded_reason, validate_id};
use crate::{
    ExecutionOperation, ExecutionReport, ExecutionRequest, FeedbackError, PreparedExecution,
    ProviderOutcome, ReportOutcome, RequestKind,
};

/// Feedback and retained provider outcome produced from one observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedReport {
    pub duplicate: bool,
    pub feedback: Vec<CommandFeedback>,
    pub completion: Option<ProviderOutcome>,
}

impl ProjectedReport {
    fn pending() -> Self {
        Self {
            duplicate: false,
            feedback: Vec::new(),
            completion: None,
        }
    }
}

#[derive(Debug, Clone)]
struct RequestState {
    parent_command_id: String,
    kind: RequestKind,
    expected_steps: Option<u64>,
    expected_stop: Option<(f64, DecimalGrid)>,
    cumulative_steps: u64,
    fact_emitted: bool,
}

#[derive(Debug, Clone)]
struct ParentState {
    expected_children: BTreeSet<String>,
    completed: BTreeMap<String, ProviderOutcome>,
    expected_fact: CommandFact,
    fact_emitted: bool,
}

/// Validates provider reports and projects committed configured-strategy feedback.
pub struct FeedbackProjector {
    maximum_requests: usize,
    maximum_observations: usize,
    requests: BTreeMap<String, RequestState>,
    parents: BTreeMap<String, ParentState>,
    observations: BTreeMap<String, ExecutionReport>,
    request_observations: BTreeMap<String, Vec<String>>,
    terminal_observations: VecDeque<String>,
}

impl FeedbackProjector {
    pub fn new(
        maximum_requests: usize,
        maximum_observations: usize,
    ) -> Result<Self, FeedbackError> {
        if maximum_requests == 0 || maximum_observations == 0 {
            return Err(FeedbackError::InvalidCapacity);
        }
        Ok(Self {
            maximum_requests,
            maximum_observations,
            requests: BTreeMap::new(),
            parents: BTreeMap::new(),
            observations: BTreeMap::new(),
            request_observations: BTreeMap::new(),
            terminal_observations: VecDeque::new(),
        })
    }

    pub fn active_requests(&self) -> usize {
        self.requests.len()
    }

    pub fn register(&mut self, request: &ExecutionRequest) -> Result<(), FeedbackError> {
        self.register_requests(
            &request.identity.parent_command_id,
            expected_fact(request.kind()),
            std::slice::from_ref(request),
        )
    }

    /// Atomically register every child request for one prepared command.
    pub fn register_execution(
        &mut self,
        prepared: &PreparedExecution,
    ) -> Result<(), FeedbackError> {
        if prepared.requests.is_empty() {
            return Err(FeedbackError::InvalidRequest(
                "prepared execution contains no requests".into(),
            ));
        }
        let expected_kind = configured_request_kind(prepared.action);
        if prepared
            .requests
            .iter()
            .any(|request| request.kind() != expected_kind)
        {
            return Err(FeedbackError::InvalidRequest(
                "child request kind does not match the parent configured action".into(),
            ));
        }
        self.register_requests(
            &prepared.command_id,
            configured_fact(prepared.action),
            &prepared.requests,
        )
    }

    /// Remove a fully reconciled parent that intentionally received no strategy terminal.
    pub fn abandon_parent(&mut self, command_id: &str) -> Result<bool, FeedbackError> {
        let Some(parent) = self.parents.get(command_id) else {
            return Ok(false);
        };
        let has_active_child = parent
            .expected_children
            .iter()
            .any(|request_id| self.requests.contains_key(request_id));
        if has_active_child || parent.completed.len() != parent.expected_children.len() {
            return Err(FeedbackError::ActiveParentChildren {
                command_id: command_id.to_owned(),
            });
        }
        self.parents.remove(command_id);
        Ok(true)
    }

    fn register_requests(
        &mut self,
        command_id: &str,
        expected_fact: CommandFact,
        requests: &[ExecutionRequest],
    ) -> Result<(), FeedbackError> {
        validate_id("parent command", command_id)
            .map_err(|error| FeedbackError::InvalidRequest(error.to_string()))?;
        if self.parents.contains_key(command_id) {
            return Err(FeedbackError::DuplicateParent {
                command_id: command_id.to_owned(),
            });
        }
        if requests.len() > self.maximum_requests.saturating_sub(self.requests.len()) {
            return Err(FeedbackError::RequestCapacityExceeded {
                maximum: self.maximum_requests,
            });
        }
        let mut request_ids = BTreeSet::new();
        let mut staged = Vec::with_capacity(requests.len());
        for request in requests {
            request
                .validate()
                .map_err(|error| FeedbackError::InvalidRequest(error.to_string()))?;
            if request.identity.parent_command_id != command_id {
                return Err(FeedbackError::InvalidRequest(
                    "child parent_command_id does not match the prepared command".into(),
                ));
            }
            let request_id = request.identity.request_id.clone();
            if self.requests.contains_key(&request_id) || !request_ids.insert(request_id.clone()) {
                return Err(FeedbackError::DuplicateRequest { request_id });
            }
            let expected_stop = match &request.operation {
                ExecutionOperation::ModifyStop {
                    stoploss,
                    price_grid,
                    ..
                } => Some((*stoploss, *price_grid)),
                _ => None,
            };
            staged.push((
                request_id,
                RequestState {
                    parent_command_id: command_id.to_owned(),
                    kind: request.kind(),
                    expected_steps: request.expected_steps(),
                    expected_stop,
                    cumulative_steps: 0,
                    fact_emitted: false,
                },
            ));
        }
        self.parents.insert(
            command_id.to_owned(),
            ParentState {
                expected_children: request_ids,
                completed: BTreeMap::new(),
                expected_fact,
                fact_emitted: false,
            },
        );
        self.requests.extend(staged);
        Ok(())
    }

    pub fn process(&mut self, report: ExecutionReport) -> Result<ProjectedReport, FeedbackError> {
        validate_report_identity(&report)?;
        if let Some(existing) = self.observations.get(&report.observation_id) {
            if existing == &report {
                return Ok(ProjectedReport {
                    duplicate: true,
                    feedback: Vec::new(),
                    completion: None,
                });
            }
            return Err(FeedbackError::ConflictingDuplicate {
                observation_id: report.observation_id,
            });
        }
        let Some(current) = self.requests.get(&report.request_id) else {
            return Err(FeedbackError::UnknownRequest {
                request_id: report.request_id,
            });
        };
        if current.parent_command_id != report.parent_command_id {
            return Err(FeedbackError::ParentCommandMismatch {
                expected: current.parent_command_id.clone(),
                actual: report.parent_command_id,
            });
        }

        let mut staged_request = current.clone();
        let mut staged_parent = self
            .parents
            .get(&report.parent_command_id)
            .cloned()
            .ok_or_else(|| {
                FeedbackError::InvalidReport(format!(
                    "parent command '{}' is not registered",
                    report.parent_command_id
                ))
            })?;
        let result = apply_report(&mut staged_request, &report);
        let unresolved = matches!(result, Err(FeedbackError::PartialOutcomeUnresolved { .. }));
        match result {
            Ok(mut projected) => {
                filter_parent_facts(&mut staged_parent, &mut projected.feedback)?;
                let terminal = projected.completion.is_some();
                if terminal {
                    let completion = projected
                        .completion
                        .clone()
                        .expect("terminal child has a provider completion");
                    let parent_result = complete_parent_child(
                        &mut staged_parent,
                        &report.parent_command_id,
                        &report.request_id,
                        completion,
                    );
                    self.clear_request_observations(&report.request_id);
                    self.ensure_observation_capacity()?;
                    self.requests.remove(&report.request_id);
                    self.record_terminal_observation(report.clone());
                    match parent_result {
                        Ok(Some(terminal)) => {
                            self.parents.remove(&report.parent_command_id);
                            projected.feedback.push(terminal);
                        }
                        Ok(None) => {
                            self.parents
                                .insert(report.parent_command_id.clone(), staged_parent);
                        }
                        Err(error) => {
                            self.parents
                                .insert(report.parent_command_id.clone(), staged_parent);
                            return Err(error);
                        }
                    }
                } else {
                    self.ensure_observation_capacity()?;
                    self.requests
                        .insert(report.request_id.clone(), staged_request);
                    self.parents
                        .insert(report.parent_command_id.clone(), staged_parent);
                    self.record_active_observation(report);
                }
                Ok(projected)
            }
            Err(error) if unresolved => {
                self.ensure_observation_capacity()?;
                self.requests
                    .insert(report.request_id.clone(), staged_request);
                self.record_active_observation(report);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    fn ensure_observation_capacity(&mut self) -> Result<(), FeedbackError> {
        while self.observations.len() >= self.maximum_observations {
            let Some(observation_id) = self.terminal_observations.pop_front() else {
                return Err(FeedbackError::ObservationCapacityExceeded {
                    maximum: self.maximum_observations,
                });
            };
            self.observations.remove(&observation_id);
        }
        Ok(())
    }

    fn record_active_observation(&mut self, report: ExecutionReport) {
        self.request_observations
            .entry(report.request_id.clone())
            .or_default()
            .push(report.observation_id.clone());
        self.observations
            .insert(report.observation_id.clone(), report);
    }

    fn record_terminal_observation(&mut self, report: ExecutionReport) {
        self.terminal_observations
            .push_back(report.observation_id.clone());
        self.observations
            .insert(report.observation_id.clone(), report);
    }

    fn clear_request_observations(&mut self, request_id: &str) {
        if let Some(observation_ids) = self.request_observations.remove(request_id) {
            for observation_id in observation_ids {
                self.observations.remove(&observation_id);
            }
        }
    }
}

fn filter_parent_facts(
    parent: &mut ParentState,
    feedback: &mut Vec<CommandFeedback>,
) -> Result<(), FeedbackError> {
    if feedback.iter().any(
        |item| matches!(item, CommandFeedback::Fact { fact, .. } if *fact != parent.expected_fact),
    ) {
        return Err(FeedbackError::InvalidReport(
            "child fact does not match the parent configured action".into(),
        ));
    }
    feedback.retain(|item| match item {
        CommandFeedback::Fact { .. } if parent.fact_emitted => false,
        CommandFeedback::Fact { .. } => {
            parent.fact_emitted = true;
            true
        }
        CommandFeedback::Terminal { .. } => false,
    });
    Ok(())
}

fn complete_parent_child(
    parent: &mut ParentState,
    command_id: &str,
    request_id: &str,
    completion: ProviderOutcome,
) -> Result<Option<CommandFeedback>, FeedbackError> {
    if !parent.expected_children.contains(request_id) {
        return Err(FeedbackError::InvalidReport(format!(
            "request '{request_id}' is not a child of '{command_id}'"
        )));
    }
    parent.completed.insert(request_id.to_owned(), completion);
    if parent.completed.len() != parent.expected_children.len() {
        return Ok(None);
    }
    let mut successes = 0usize;
    let mut failures = Vec::new();
    let mut rejected_only = true;
    for outcome in parent.completed.values() {
        match outcome {
            ProviderOutcome::Filled { .. }
            | ProviderOutcome::PartiallyFilledThenCancelled { .. }
            | ProviderOutcome::Cancelled
            | ProviderOutcome::Modified => successes += 1,
            ProviderOutcome::Rejected { reason } => failures.push(reason.clone()),
            ProviderOutcome::Failed { reason } => {
                rejected_only = false;
                failures.push(reason.clone());
            }
        }
    }
    if successes > 0 && !failures.is_empty() {
        return Err(FeedbackError::PartialOutcomeUnresolved {
            request_id: command_id.to_owned(),
            reason: bounded_reason(format!(
                "parent has {successes} successful child outcome(s) and failures: {}",
                failures.join("; ")
            )),
        });
    }
    if failures.is_empty() {
        return Ok(Some(CommandFeedback::Terminal {
            command_id: command_id.to_owned(),
            status: CommandTerminalStatus::Applied,
            reason: None,
        }));
    }
    let status = if rejected_only {
        CommandTerminalStatus::Rejected
    } else {
        CommandTerminalStatus::Failed
    };
    Ok(Some(CommandFeedback::Terminal {
        command_id: command_id.to_owned(),
        status,
        reason: Some(bounded_reason(failures.join("; "))),
    }))
}

fn validate_report_identity(report: &ExecutionReport) -> Result<(), FeedbackError> {
    for (field, value) in [
        ("observation_id", report.observation_id.as_str()),
        ("request_id", report.request_id.as_str()),
        ("parent_command_id", report.parent_command_id.as_str()),
    ] {
        validate_id(field, value)
            .map_err(|error| FeedbackError::InvalidReport(error.to_string()))?;
    }
    let reason = match &report.outcome {
        ReportOutcome::Cancelled { reason, .. }
        | ReportOutcome::Rejected { reason }
        | ReportOutcome::Failed { reason } => Some(reason),
        _ => None,
    };
    if reason.is_some_and(|reason| reason.len() > MAX_REASON_BYTES) {
        return Err(FeedbackError::InvalidReport(format!(
            "provider reason exceeds {MAX_REASON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn apply_report(
    state: &mut RequestState,
    report: &ExecutionReport,
) -> Result<ProjectedReport, FeedbackError> {
    match &report.outcome {
        ReportOutcome::Accepted | ReportOutcome::Resting => Ok(ProjectedReport::pending()),
        ReportOutcome::Fill {
            incremental_steps,
            cumulative_steps,
            price,
        } => apply_fill(state, report, *incremental_steps, *cumulative_steps, *price),
        ReportOutcome::Cancelled {
            filled_steps,
            reason,
        } => apply_cancelled(state, report, *filled_steps, reason),
        ReportOutcome::Modified { stoploss } => apply_modified(state, report, *stoploss),
        ReportOutcome::Rejected { reason } => {
            apply_failure(state, report, CommandTerminalStatus::Rejected, reason)
        }
        ReportOutcome::Failed { reason } => {
            apply_failure(state, report, CommandTerminalStatus::Failed, reason)
        }
    }
}

fn apply_fill(
    state: &mut RequestState,
    report: &ExecutionReport,
    incremental_steps: u64,
    cumulative_steps: u64,
    price: f64,
) -> Result<ProjectedReport, FeedbackError> {
    if !matches!(
        state.kind,
        RequestKind::Entry | RequestKind::FullClose | RequestKind::PartialClose
    ) {
        return Err(outcome_mismatch(state, report.outcome.name()));
    }
    if incremental_steps == 0 || !price.is_finite() || price <= 0.0 {
        return Err(FeedbackError::InvalidReport(
            "fill needs positive steps and a finite positive price".into(),
        ));
    }
    let expected = state
        .expected_steps
        .expect("fill-bearing requests have steps");
    let next = state
        .cumulative_steps
        .checked_add(incremental_steps)
        .ok_or_else(|| FeedbackError::InvalidReport("cumulative fill overflow".into()))?;
    if next != cumulative_steps || cumulative_steps > expected {
        return Err(FeedbackError::InvalidReport(format!(
            "fill progression expected cumulative {next} of at most {expected}, got {cumulative_steps}"
        )));
    }
    state.cumulative_steps = cumulative_steps;
    let complete = cumulative_steps == expected;
    let mut feedback = Vec::new();
    if should_emit_fact(state.kind, complete, state.fact_emitted) {
        feedback.push(CommandFeedback::Fact {
            command_id: report.parent_command_id.clone(),
            fact: fact_for(state.kind),
        });
        state.fact_emitted = true;
    }
    let completion = complete.then(|| {
        feedback.push(applied(&report.parent_command_id));
        ProviderOutcome::Filled {
            filled_steps: cumulative_steps,
        }
    });
    Ok(ProjectedReport {
        duplicate: false,
        feedback,
        completion,
    })
}

fn apply_cancelled(
    state: &mut RequestState,
    report: &ExecutionReport,
    filled_steps: u64,
    reason: &str,
) -> Result<ProjectedReport, FeedbackError> {
    if filled_steps != state.cumulative_steps {
        return Err(FeedbackError::InvalidReport(format!(
            "cancelled outcome reports {filled_steps} filled steps, but {} were observed",
            state.cumulative_steps
        )));
    }
    match state.kind {
        RequestKind::CancelPending => {
            if filled_steps != 0 {
                return Err(FeedbackError::InvalidReport(
                    "cancel-pending report cannot carry filled steps".into(),
                ));
            }
            Ok(ProjectedReport {
                duplicate: false,
                feedback: vec![
                    CommandFeedback::Fact {
                        command_id: report.parent_command_id.clone(),
                        fact: CommandFact::PendingCancelled,
                    },
                    applied(&report.parent_command_id),
                ],
                completion: Some(ProviderOutcome::Cancelled),
            })
        }
        RequestKind::Entry | RequestKind::PartialClose if filled_steps > 0 => {
            let expected = state.expected_steps.expect("fill-bearing request");
            let mut feedback = Vec::new();
            if !state.fact_emitted {
                feedback.push(CommandFeedback::Fact {
                    command_id: report.parent_command_id.clone(),
                    fact: fact_for(state.kind),
                });
                state.fact_emitted = true;
            }
            feedback.push(applied(&report.parent_command_id));
            Ok(ProjectedReport {
                duplicate: false,
                feedback,
                completion: Some(ProviderOutcome::PartiallyFilledThenCancelled {
                    filled_steps,
                    cancelled_steps: expected.saturating_sub(filled_steps),
                }),
            })
        }
        RequestKind::Entry | RequestKind::FullClose | RequestKind::PartialClose
            if filled_steps == 0 =>
        {
            let reason = if reason.is_empty() {
                "request cancelled before any fill"
            } else {
                reason
            };
            Ok(ProjectedReport {
                duplicate: false,
                feedback: vec![terminal(
                    &report.parent_command_id,
                    CommandTerminalStatus::Rejected,
                    reason,
                )],
                completion: Some(ProviderOutcome::Rejected {
                    reason: bounded_reason(reason),
                }),
            })
        }
        RequestKind::FullClose => Err(FeedbackError::PartialOutcomeUnresolved {
            request_id: report.request_id.clone(),
            reason: bounded_reason(reason),
        }),
        RequestKind::ModifyStop => Err(outcome_mismatch(state, report.outcome.name())),
        _ => unreachable!("all request kinds are covered"),
    }
}

fn apply_modified(
    state: &mut RequestState,
    report: &ExecutionReport,
    stoploss: f64,
) -> Result<ProjectedReport, FeedbackError> {
    if state.kind != RequestKind::ModifyStop {
        return Err(outcome_mismatch(state, report.outcome.name()));
    }
    if !stoploss.is_finite() || stoploss <= 0.0 {
        return Err(FeedbackError::InvalidReport(
            "modified stop must be finite and positive".into(),
        ));
    }
    let (expected, grid) = state.expected_stop.ok_or_else(|| {
        FeedbackError::InvalidReport("modify-stop request did not retain its expected price".into())
    })?;
    let reported = Decimal::checked_from_f64(stoploss)
        .map_err(|error| FeedbackError::InvalidReport(error.to_string()))?;
    if !grid
        .contains(reported)
        .map_err(|error| FeedbackError::InvalidReport(error.to_string()))?
    {
        return Err(FeedbackError::InvalidReport(
            "reported stop is outside the request price grid".into(),
        ));
    }
    let expected = Decimal::checked_from_f64(expected)
        .map_err(|error| FeedbackError::InvalidReport(error.to_string()))?;
    if reported != expected {
        return Err(FeedbackError::InvalidReport(format!(
            "reported stop {reported} does not match requested stop {expected}"
        )));
    }
    Ok(ProjectedReport {
        duplicate: false,
        feedback: vec![
            CommandFeedback::Fact {
                command_id: report.parent_command_id.clone(),
                fact: CommandFact::StoplossModified,
            },
            applied(&report.parent_command_id),
        ],
        completion: Some(ProviderOutcome::Modified),
    })
}

fn apply_failure(
    state: &mut RequestState,
    report: &ExecutionReport,
    status: CommandTerminalStatus,
    reason: &str,
) -> Result<ProjectedReport, FeedbackError> {
    if state.cumulative_steps > 0 {
        return Err(FeedbackError::PartialOutcomeUnresolved {
            request_id: report.request_id.clone(),
            reason: bounded_reason(reason),
        });
    }
    let reason = if reason.is_empty() {
        "provider supplied no failure reason"
    } else {
        reason
    };
    let completion = match status {
        CommandTerminalStatus::Rejected => ProviderOutcome::Rejected {
            reason: bounded_reason(reason),
        },
        CommandTerminalStatus::Failed => ProviderOutcome::Failed {
            reason: bounded_reason(reason),
        },
        _ => unreachable!("failure projection uses rejected or failed"),
    };
    Ok(ProjectedReport {
        duplicate: false,
        feedback: vec![terminal(&report.parent_command_id, status, reason)],
        completion: Some(completion),
    })
}

fn should_emit_fact(kind: RequestKind, complete: bool, fact_emitted: bool) -> bool {
    if fact_emitted {
        return false;
    }
    match kind {
        RequestKind::Entry | RequestKind::PartialClose => true,
        RequestKind::FullClose => complete,
        RequestKind::CancelPending | RequestKind::ModifyStop => false,
    }
}

fn configured_request_kind(action: ConfiguredActionKind) -> RequestKind {
    match action {
        ConfiguredActionKind::Entry => RequestKind::Entry,
        ConfiguredActionKind::Close => RequestKind::FullClose,
        ConfiguredActionKind::ClosePartial => RequestKind::PartialClose,
        ConfiguredActionKind::MoveStoplossToEntry | ConfiguredActionKind::ModifyStoploss => {
            RequestKind::ModifyStop
        }
        ConfiguredActionKind::CancelPending => RequestKind::CancelPending,
    }
}

fn configured_fact(action: ConfiguredActionKind) -> CommandFact {
    expected_fact(configured_request_kind(action))
}

fn expected_fact(kind: RequestKind) -> CommandFact {
    fact_for(kind)
}

fn fact_for(kind: RequestKind) -> CommandFact {
    match kind {
        RequestKind::Entry => CommandFact::EntryFilled,
        RequestKind::FullClose => CommandFact::PositionClosed,
        RequestKind::PartialClose => CommandFact::PositionReduced,
        RequestKind::CancelPending => CommandFact::PendingCancelled,
        RequestKind::ModifyStop => CommandFact::StoplossModified,
    }
}

fn applied(command_id: &str) -> CommandFeedback {
    CommandFeedback::Terminal {
        command_id: command_id.to_owned(),
        status: CommandTerminalStatus::Applied,
        reason: None,
    }
}

fn terminal(command_id: &str, status: CommandTerminalStatus, reason: &str) -> CommandFeedback {
    CommandFeedback::Terminal {
        command_id: command_id.to_owned(),
        status,
        reason: Some(bounded_reason(reason)),
    }
}

fn outcome_mismatch(state: &RequestState, outcome: &'static str) -> FeedbackError {
    FeedbackError::OutcomeMismatch {
        outcome,
        request_kind: state.kind.as_str(),
    }
}
