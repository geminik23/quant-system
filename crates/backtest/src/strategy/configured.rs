//! Historical binding for reusable configured strategies.

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDateTime;
use qs_core::TradeEngine;
use qs_core::types::{Effect, PositionStatus};
use qs_strategy::{
    CommandFact, CommandFeedback, CommandTerminalStatus, ConfiguredActionKind, ConfiguredCommand,
    ConfiguredStrategy, ConfiguredStrategyRequirements, DecisionKind, MAX_GENERATED_ID_BYTES,
    MAX_ID_BYTES, MAX_NAMED_VALUES, MAX_OUTPUT_COMMANDS, MAX_OUTPUT_NOTES, MAX_TEXT_BYTES,
    NamedValue, NoteKind, OutputScalar, SourceId, StrategyInput, TradeSlotFacts, TradeSlotState,
    Value, ValueType,
};

use crate::ledger::ActionDispositionStatus;

use super::{
    BarSeriesSpec, ClosedBar, HistoricalObservationView, HistoricalSeriesView, JournalKind,
    SeriesId, StrategyDecisionDraft, StrategyDecisionKind, StrategyDescriptor, StrategyDomainError,
    StrategyFeedbackEvent, StrategyJournalDraft, StrategyJournalError, StrategyObservation,
    StrategyRequirements, StrategyResearchLimits, StrategyRetentionLimits,
};

const MAX_EXACT_F64_INTEGER: u64 = 1_u64 << 53;

/// Historical volume projection used for configured completed bars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalVolumeProjection {
    TickCountExact,
}

/// Complete historical binding for one logical configured source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredSourceBinding {
    source: SourceId,
    series: BarSeriesSpec,
}

impl ConfiguredSourceBinding {
    pub fn new(source: SourceId, series: BarSeriesSpec) -> Self {
        Self { source, series }
    }

    pub fn source(&self) -> &SourceId {
        &self.source
    }

    pub fn series(&self) -> &BarSeriesSpec {
        &self.series
    }

    pub fn series_id(&self) -> &SeriesId {
        self.series.requirement().id()
    }
}

/// Immutable causal values available to one named-input projector.
#[derive(Clone, Copy)]
pub struct NamedInputProjectionContext<'a> {
    pub observed_through: NaiveDateTime,
    pub closed_bars: &'a [ClosedBar],
    pub observations: &'a [StrategyObservation],
    pub series: &'a dyn HistoricalSeriesView,
    pub observation_history: &'a dyn HistoricalObservationView,
}

/// One typed named-input value and its boundary update provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedNamedInput {
    pub value: Value,
    pub updated: bool,
}

/// Pure historical projection for one configured named input.
pub trait HistoricalNamedInputProjector {
    fn output_type(&self) -> ValueType;

    fn project(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<ProjectedNamedInput, NamedInputProjectionError>;
}

/// Binding from a configured input name to a historical projector.
pub struct ConfiguredNamedInputBinding {
    name: String,
    projector: Box<dyn HistoricalNamedInputProjector>,
}

impl ConfiguredNamedInputBinding {
    pub fn new(name: impl Into<String>, projector: Box<dyn HistoricalNamedInputProjector>) -> Self {
        Self {
            name: name.into(),
            projector,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn output_type(&self) -> ValueType {
        self.projector.output_type()
    }
}

/// Complete caller-owned historical input binding.
pub struct ConfiguredHistoricalBindings {
    sources: Vec<ConfiguredSourceBinding>,
    named_inputs: Vec<ConfiguredNamedInputBinding>,
    volume: HistoricalVolumeProjection,
}

impl ConfiguredHistoricalBindings {
    pub fn new(
        sources: Vec<ConfiguredSourceBinding>,
        named_inputs: Vec<ConfiguredNamedInputBinding>,
        volume: HistoricalVolumeProjection,
    ) -> Self {
        Self {
            sources,
            named_inputs,
            volume,
        }
    }

    pub fn sources(&self) -> &[ConfiguredSourceBinding] {
        &self.sources
    }

    pub fn named_inputs(&self) -> &[ConfiguredNamedInputBinding] {
        &self.named_inputs
    }

    pub fn volume(&self) -> HistoricalVolumeProjection {
        self.volume
    }
}

/// Named-input projector failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct NamedInputProjectionError {
    message: String,
}

impl NamedInputProjectionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Binding failures detected before replay starts.
#[derive(Debug, thiserror::Error)]
pub enum ConfiguredStrategyAdapterBuildError {
    #[error("configured source '{source_id}' has no historical binding")]
    MissingSourceBinding { source_id: SourceId },
    #[error("configured source '{source_id}' is bound more than once")]
    DuplicateSourceBinding { source_id: SourceId },
    #[error(
        "historical series ID '{series_id}' cannot be bound to more than one configured source"
    )]
    DuplicateSeriesBinding { series_id: SeriesId },
    #[error("source '{source_id}' is not declared by the configured strategy")]
    UndeclaredSourceBinding { source_id: SourceId },
    #[error(
        "source '{source_id}' is bound to symbol '{series_symbol}', but the configured strategy primary symbol is '{primary_symbol}'"
    )]
    SourceSymbolMismatch {
        source_id: SourceId,
        primary_symbol: String,
        series_symbol: String,
    },
    #[error(
        "source '{source_id}' requires lookback {required}, but retained history is {retained}"
    )]
    RetentionBelowLookback {
        source_id: SourceId,
        required: usize,
        retained: usize,
    },
    #[error("source '{source_id}' requires lookback {required}, but historical warmup is {warmup}")]
    WarmupBelowLookback {
        source_id: SourceId,
        required: usize,
        warmup: usize,
    },
    #[error("configured named input '{name}' has no projector")]
    MissingNamedInputProjector { name: String },
    #[error("configured named input '{name}' has more than one projector")]
    DuplicateNamedInputProjector { name: String },
    #[error("named input '{name}' expects {expected:?}, but its projector returns {actual:?}")]
    NamedInputTypeMismatch {
        name: String,
        expected: ValueType,
        actual: ValueType,
    },
    #[error("named input projector '{name}' is not required by the configured strategy")]
    UndeclaredNamedInputProjector { name: String },
    #[error(transparent)]
    HistoricalRequirements(#[from] StrategyDomainError),
}

/// Static output compatibility failure detected before feed polling.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfiguredStrategyAdapterPreflightError {
    #[error("decision reason capacity {actual} is below configured output capacity {required}")]
    DecisionReasonCapacity { actual: usize, required: usize },
    #[error("signal capacity {actual} is below configured output capacity {required}")]
    SignalCapacity { actual: usize, required: usize },
    #[error("journal callback capacity {actual} is below configured output capacity {required}")]
    JournalCallbackCapacity { actual: usize, required: usize },
    #[error("journal reason capacity {actual} is below configured output capacity {required}")]
    JournalReasonCapacity { actual: usize, required: usize },
    #[error("journal value capacity {actual} is below configured output capacity {required}")]
    JournalValueCapacity { actual: usize, required: usize },
    #[error("journal key capacity {actual} is below configured output capacity {required}")]
    JournalKeyCapacity { actual: usize, required: usize },
    #[error(
        "historical trade identity capacity {actual} is below configured identity capacity {required}"
    )]
    TradeIdentityCapacity { actual: usize, required: usize },
}

/// Runtime historical projection or configured evaluation failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfiguredStrategyAdapterError {
    #[error("source '{source_id}' produced more than one completed bar at {timestamp}")]
    DuplicateSourceUpdate {
        source_id: SourceId,
        timestamp: NaiveDateTime,
    },
    #[error("tick count {tick_count} cannot be represented exactly as f64")]
    TickCountNotExactlyRepresentable { tick_count: u64 },
    #[error("named input '{name}' projection failed: {source}")]
    NamedInput {
        name: String,
        source: NamedInputProjectionError,
    },
    #[error("named input '{name}' returned a value incompatible with {expected:?}")]
    NamedInputValueType { name: String, expected: ValueType },
    #[error("trade slot '{slot}' has inconsistent engine state: {reason}")]
    TradeSlot { slot: String, reason: String },
    #[error("configured command '{command_id}' received an incompatible committed effect")]
    IncompatibleCommandEffect { command_id: String },
    #[error("configured strategy evaluation failed: {0}")]
    Evaluation(#[from] qs_strategy::EvaluationError),
    #[error("configured decision mapping failed: {0}")]
    Decision(#[from] StrategyDomainError),
    #[error("configured note mapping failed: {0}")]
    Journal(#[from] StrategyJournalError),
    #[error("configured output integer cannot be represented exactly as f64")]
    IntegerOutputPrecision,
}

#[derive(Debug, Clone)]
struct CommandRoute {
    action: ConfiguredActionKind,
    slot: String,
    fact_seen: bool,
    terminal: Option<CommandTerminalStatus>,
}

pub(crate) struct ConfiguredBoundaryOutput {
    pub decision: Option<StrategyDecisionDraft>,
    pub journal: Vec<StrategyJournalDraft>,
    pub commands: Vec<ConfiguredCommand>,
}

/// Historical runtime adapter for one reusable configured strategy instance.
pub struct BacktestConfiguredStrategyAdapter {
    strategy: ConfiguredStrategy,
    descriptor: StrategyDescriptor,
    requirements: StrategyRequirements,
    bindings: ConfiguredHistoricalBindings,
    command_routes: BTreeMap<String, CommandRoute>,
}

impl BacktestConfiguredStrategyAdapter {
    pub fn new(
        strategy: ConfiguredStrategy,
        descriptor: StrategyDescriptor,
        bindings: ConfiguredHistoricalBindings,
        decision_latency_ms: u64,
    ) -> Result<Self, ConfiguredStrategyAdapterBuildError> {
        validate_bindings(&strategy, &bindings)?;
        let series = bindings
            .sources
            .iter()
            .map(|binding| binding.series.requirement().clone())
            .collect::<Vec<_>>();
        let mut instruments = Vec::new();
        for requirement in &series {
            if !instruments
                .iter()
                .any(|symbol| symbol == requirement.symbol())
            {
                instruments.push(requirement.symbol().to_owned());
            }
        }
        let needs_feedback = strategy.input_requirements().needs_command_feedback;
        let requirements = StrategyRequirements::new(
            instruments,
            series,
            decision_latency_ms,
            true,
            needs_feedback,
        )?;
        Ok(Self {
            strategy,
            descriptor,
            requirements,
            bindings,
            command_routes: BTreeMap::new(),
        })
    }

    pub fn descriptor(&self) -> &StrategyDescriptor {
        &self.descriptor
    }

    pub fn requirements(&self) -> &StrategyRequirements {
        &self.requirements
    }

    pub fn configured_requirements(&self) -> &ConfiguredStrategyRequirements {
        self.strategy.input_requirements()
    }

    pub fn source_bindings(&self) -> &[ConfiguredSourceBinding] {
        &self.bindings.sources
    }

    pub fn series_specs(&self) -> impl ExactSizeIterator<Item = &BarSeriesSpec> {
        self.bindings.sources.iter().map(|binding| &binding.series)
    }

    pub fn configured_strategy(&self) -> &ConfiguredStrategy {
        &self.strategy
    }

    pub fn into_configured_strategy(self) -> ConfiguredStrategy {
        self.strategy
    }

    pub fn preflight(
        &self,
        retention: StrategyRetentionLimits,
        research: StrategyResearchLimits,
    ) -> Result<(), ConfiguredStrategyAdapterPreflightError> {
        if retention.max_reason_bytes() < MAX_TEXT_BYTES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::DecisionReasonCapacity {
                    actual: retention.max_reason_bytes(),
                    required: MAX_TEXT_BYTES,
                },
            );
        }
        if retention.max_signals_per_callback() < MAX_OUTPUT_COMMANDS {
            return Err(ConfiguredStrategyAdapterPreflightError::SignalCapacity {
                actual: retention.max_signals_per_callback(),
                required: MAX_OUTPUT_COMMANDS,
            });
        }
        if research.max_journal_per_callback() < MAX_OUTPUT_NOTES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::JournalCallbackCapacity {
                    actual: research.max_journal_per_callback(),
                    required: MAX_OUTPUT_NOTES,
                },
            );
        }
        if research.max_reason_bytes() < MAX_TEXT_BYTES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::JournalReasonCapacity {
                    actual: research.max_reason_bytes(),
                    required: MAX_TEXT_BYTES,
                },
            );
        }
        if research.max_values_per_record() < MAX_NAMED_VALUES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::JournalValueCapacity {
                    actual: research.max_values_per_record(),
                    required: MAX_NAMED_VALUES,
                },
            );
        }
        if research.max_value_key_bytes() < MAX_ID_BYTES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::JournalKeyCapacity {
                    actual: research.max_value_key_bytes(),
                    required: MAX_ID_BYTES,
                },
            );
        }
        if super::MAX_TRADE_ID_BYTES < MAX_GENERATED_ID_BYTES {
            return Err(
                ConfiguredStrategyAdapterPreflightError::TradeIdentityCapacity {
                    actual: super::MAX_TRADE_ID_BYTES,
                    required: MAX_GENERATED_ID_BYTES,
                },
            );
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn evaluate_boundary(
        &mut self,
        observed_through: NaiveDateTime,
        ready: bool,
        closed_bars: &[ClosedBar],
        observations: &[StrategyObservation],
        series: &dyn HistoricalSeriesView,
        observation_history: &dyn HistoricalObservationView,
        engine: &TradeEngine,
        feedback_events: &[StrategyFeedbackEvent],
        retention: StrategyRetentionLimits,
        research: StrategyResearchLimits,
    ) -> Result<ConfiguredBoundaryOutput, ConfiguredStrategyAdapterError> {
        let feedback = self.project_feedback(feedback_events)?;
        let input = StrategyInput {
            time: observed_through,
            ready,
            completed_bars: self.project_bars(observed_through, closed_bars)?,
            values: self.project_named_inputs(NamedInputProjectionContext {
                observed_through,
                closed_bars,
                observations,
                series,
                observation_history,
            })?,
            trade_slots: self.project_trade_slots(engine)?,
            feedback,
        };
        let output = self.strategy.evaluate(&input)?;
        for command in &output.commands {
            self.command_routes.insert(
                command.command_id.clone(),
                CommandRoute {
                    action: command.action_kind,
                    slot: command.trade_slot.clone(),
                    fact_seen: false,
                    terminal: None,
                },
            );
        }
        let emitted_signals = output
            .commands
            .iter()
            .map(|command| command.signal.clone())
            .collect::<Vec<_>>();
        let decision = output
            .decision
            .map(|decision| map_decision(decision, emitted_signals, retention))
            .transpose()?;
        let journal = output
            .notes
            .into_iter()
            .map(|note| map_note(note, self.strategy.primary_symbol(), research))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConfiguredBoundaryOutput {
            decision,
            journal,
            commands: output.commands,
        })
    }

    fn project_bars(
        &self,
        observed_through: NaiveDateTime,
        closed_bars: &[ClosedBar],
    ) -> Result<Vec<qs_strategy::CompletedBarUpdate>, ConfiguredStrategyAdapterError> {
        self.strategy
            .input_requirements()
            .completed_bars
            .iter()
            .filter_map(|requirement| {
                let binding = self
                    .bindings
                    .sources
                    .iter()
                    .find(|binding| binding.source == requirement.source)
                    .expect("bindings were validated at construction");
                let mut matching = closed_bars
                    .iter()
                    .filter(|bar| bar.series_id() == binding.series_id());
                let bar = matching.next()?;
                Some(if matching.next().is_some() {
                    Err(ConfiguredStrategyAdapterError::DuplicateSourceUpdate {
                        source_id: requirement.source.clone(),
                        timestamp: observed_through,
                    })
                } else {
                    Ok(qs_strategy::CompletedBarUpdate {
                        source: requirement.source.clone(),
                        bar: qs_strategy::CompletedBar {
                            open: bar.open(),
                            high: bar.high(),
                            low: bar.low(),
                            close: bar.close(),
                            volume: match self.bindings.volume {
                                HistoricalVolumeProjection::TickCountExact => {
                                    if bar.tick_count() > MAX_EXACT_F64_INTEGER {
                                        return Some(Err(
                                            ConfiguredStrategyAdapterError::TickCountNotExactlyRepresentable {
                                                tick_count: bar.tick_count(),
                                            },
                                        ));
                                    }
                                    bar.tick_count() as f64
                                }
                            },
                        },
                    })
                })
            })
            .collect()
    }

    fn project_named_inputs(
        &self,
        context: NamedInputProjectionContext<'_>,
    ) -> Result<Vec<NamedValue>, ConfiguredStrategyAdapterError> {
        self.strategy
            .input_requirements()
            .named_inputs
            .iter()
            .map(|requirement| {
                let binding = self
                    .bindings
                    .named_inputs
                    .iter()
                    .find(|binding| binding.name == requirement.name)
                    .expect("named input bindings were validated at construction");
                let projected = binding.projector.project(context).map_err(|source| {
                    ConfiguredStrategyAdapterError::NamedInput {
                        name: requirement.name.clone(),
                        source,
                    }
                })?;
                if !value_matches_type(&projected.value, requirement.value_type) {
                    return Err(ConfiguredStrategyAdapterError::NamedInputValueType {
                        name: requirement.name.clone(),
                        expected: requirement.value_type,
                    });
                }
                Ok(NamedValue {
                    name: requirement.name.clone(),
                    value: projected.value,
                    updated: projected.updated,
                })
            })
            .collect()
    }

    fn project_trade_slots(
        &self,
        engine: &TradeEngine,
    ) -> Result<Vec<TradeSlotFacts>, ConfiguredStrategyAdapterError> {
        self.strategy
            .input_requirements()
            .trade_slots
            .iter()
            .map(|slot| {
                let state = self
                    .strategy
                    .trade_id_for_slot(slot)
                    .and_then(|trade_id| engine.manager.id_by_trade_id(trade_id))
                    .and_then(|position_id| engine.get_position(&position_id))
                    .map(|position| match position.data.status {
                        PositionStatus::Pending => TradeSlotState::Pending {
                            side: position.data.side,
                            requested_price: position.data.pending_price,
                            stoploss: position.current_stoploss(),
                        },
                        PositionStatus::Open => TradeSlotState::Open {
                            side: position.data.side,
                            entry_price: position.data.average_entry(),
                            remaining_size: position.data.remaining_size(),
                            stoploss: position.current_stoploss(),
                        },
                        PositionStatus::Closed | PositionStatus::Cancelled => {
                            TradeSlotState::Vacant
                        }
                    })
                    .unwrap_or(TradeSlotState::Vacant);
                Ok(TradeSlotFacts {
                    slot: slot.clone(),
                    state,
                })
            })
            .collect()
    }

    fn project_feedback(
        &mut self,
        events: &[StrategyFeedbackEvent],
    ) -> Result<Vec<CommandFeedback>, ConfiguredStrategyAdapterError> {
        project_command_feedback(&mut self.command_routes, events)
    }

    pub(crate) fn finalize_feedback(
        &mut self,
        events: &[StrategyFeedbackEvent],
    ) -> Result<(), ConfiguredStrategyAdapterError> {
        let feedback = self.project_feedback(events)?;
        self.strategy.finalize_command_feedback(&feedback)?;
        self.command_routes.clear();
        Ok(())
    }
}

fn project_command_feedback(
    routes: &mut BTreeMap<String, CommandRoute>,
    events: &[StrategyFeedbackEvent],
) -> Result<Vec<CommandFeedback>, ConfiguredStrategyAdapterError> {
    let mut projected = Vec::new();
    for event in events {
        let Some(command_id) = event.action_id() else {
            continue;
        };
        let Some(route) = routes.get_mut(command_id) else {
            continue;
        };
        match event {
            StrategyFeedbackEvent::Effect { effect, .. } => {
                if let Some(fact) = map_effect(route.action, effect.effect()).map_err(|()| {
                    ConfiguredStrategyAdapterError::IncompatibleCommandEffect {
                        command_id: command_id.to_owned(),
                    }
                })? {
                    route.fact_seen = true;
                    projected.push(CommandFeedback::Fact {
                        command_id: command_id.to_owned(),
                        fact,
                    });
                }
            }
            StrategyFeedbackEvent::Disposition(disposition) => {
                let status = match disposition.status {
                    ActionDispositionStatus::Applied => CommandTerminalStatus::Applied,
                    ActionDispositionStatus::Skipped => CommandTerminalStatus::Skipped,
                    ActionDispositionStatus::Rejected => CommandTerminalStatus::Rejected,
                    ActionDispositionStatus::Failed => CommandTerminalStatus::Failed,
                };
                route.terminal = Some(status);
                projected.push(CommandFeedback::Terminal {
                    command_id: command_id.to_owned(),
                    status,
                    reason: disposition.reason.clone(),
                });
            }
        }
        let completed = route
            .terminal
            .is_some_and(|status| status != CommandTerminalStatus::Applied)
            || (route.terminal == Some(CommandTerminalStatus::Applied) && route.fact_seen);
        if completed {
            let completed_route = routes
                .remove(command_id)
                .expect("completed route remains registered");
            if completed_route.action == ConfiguredActionKind::CancelPending
                && completed_route.terminal == Some(CommandTerminalStatus::Applied)
            {
                routes.retain(|_, route| {
                    !(route.action == ConfiguredActionKind::Entry
                        && route.slot == completed_route.slot)
                });
            }
        }
    }
    Ok(projected)
}

fn validate_bindings(
    strategy: &ConfiguredStrategy,
    bindings: &ConfiguredHistoricalBindings,
) -> Result<(), ConfiguredStrategyAdapterBuildError> {
    let declared = strategy.declared_sources().iter().collect::<BTreeSet<_>>();
    let mut sources = BTreeSet::new();
    let mut series = BTreeSet::new();
    for binding in &bindings.sources {
        if !declared.contains(&binding.source) {
            return Err(
                ConfiguredStrategyAdapterBuildError::UndeclaredSourceBinding {
                    source_id: binding.source.clone(),
                },
            );
        }
        if !sources.insert(binding.source.clone()) {
            return Err(
                ConfiguredStrategyAdapterBuildError::DuplicateSourceBinding {
                    source_id: binding.source.clone(),
                },
            );
        }
        if !series.insert(binding.series_id().clone()) {
            return Err(
                ConfiguredStrategyAdapterBuildError::DuplicateSeriesBinding {
                    series_id: binding.series_id().clone(),
                },
            );
        }
        let series_symbol = binding.series.requirement().symbol();
        if series_symbol != strategy.primary_symbol() {
            return Err(ConfiguredStrategyAdapterBuildError::SourceSymbolMismatch {
                source_id: binding.source.clone(),
                primary_symbol: strategy.primary_symbol().to_owned(),
                series_symbol: series_symbol.to_owned(),
            });
        }
    }
    for source in strategy.declared_sources() {
        if !sources.contains(source) {
            return Err(ConfiguredStrategyAdapterBuildError::MissingSourceBinding {
                source_id: source.clone(),
            });
        }
    }
    for requirement in &strategy.input_requirements().completed_bars {
        let binding = bindings
            .sources
            .iter()
            .find(|binding| binding.source == requirement.source)
            .expect("every declared source was checked above");
        if binding.series.retained_bars() < requirement.required_lookback {
            return Err(
                ConfiguredStrategyAdapterBuildError::RetentionBelowLookback {
                    source_id: requirement.source.clone(),
                    required: requirement.required_lookback,
                    retained: binding.series.retained_bars(),
                },
            );
        }
        let warmup = binding.series.requirement().warmup().required_bars();
        if warmup < requirement.required_lookback {
            return Err(ConfiguredStrategyAdapterBuildError::WarmupBelowLookback {
                source_id: requirement.source.clone(),
                required: requirement.required_lookback,
                warmup,
            });
        }
    }
    let mut names = BTreeSet::new();
    for binding in &bindings.named_inputs {
        if !names.insert(binding.name.clone()) {
            return Err(
                ConfiguredStrategyAdapterBuildError::DuplicateNamedInputProjector {
                    name: binding.name.clone(),
                },
            );
        }
        let Some(requirement) = strategy
            .input_requirements()
            .named_inputs
            .iter()
            .find(|requirement| requirement.name == binding.name)
        else {
            return Err(
                ConfiguredStrategyAdapterBuildError::UndeclaredNamedInputProjector {
                    name: binding.name.clone(),
                },
            );
        };
        let actual = binding.output_type();
        if actual != requirement.value_type {
            return Err(
                ConfiguredStrategyAdapterBuildError::NamedInputTypeMismatch {
                    name: binding.name.clone(),
                    expected: requirement.value_type,
                    actual,
                },
            );
        }
    }
    for requirement in &strategy.input_requirements().named_inputs {
        if !names.contains(&requirement.name) {
            return Err(
                ConfiguredStrategyAdapterBuildError::MissingNamedInputProjector {
                    name: requirement.name.clone(),
                },
            );
        }
    }
    Ok(())
}

fn value_matches_type(value: &Value, expected: ValueType) -> bool {
    if value.is_missing() {
        return expected.optional && value.scalar_type() == expected.scalar;
    }
    if value.scalar_type() != expected.scalar {
        return false;
    }
    match value {
        Value::Number(value) | Value::Price(value) => value.is_finite(),
        Value::Text(value) => !value.is_empty() && value.len() <= MAX_TEXT_BYTES,
        _ => true,
    }
}

fn map_effect(action: ConfiguredActionKind, effect: &Effect) -> Result<Option<CommandFact>, ()> {
    let mapped = match effect {
        Effect::PositionOpened { .. } => {
            Some((ConfiguredActionKind::Entry, CommandFact::EntryFilled))
        }
        Effect::PositionClosed { .. } => {
            Some((ConfiguredActionKind::Close, CommandFact::PositionClosed))
        }
        Effect::PartialClose { .. } => Some((
            ConfiguredActionKind::ClosePartial,
            CommandFact::PositionReduced,
        )),
        Effect::StoplossModified { .. } => match action {
            ConfiguredActionKind::MoveStoplossToEntry | ConfiguredActionKind::ModifyStoploss => {
                return Ok(Some(CommandFact::StoplossModified));
            }
            _ => return Err(()),
        },
        Effect::OrderCancelled { .. } => Some((
            ConfiguredActionKind::CancelPending,
            CommandFact::PendingCancelled,
        )),
        Effect::OrderPlaced { .. }
        | Effect::StoplossRemoved { .. }
        | Effect::ScaledIn { .. }
        | Effect::RuleTriggered { .. } => None,
    };
    match mapped {
        Some((expected, fact)) if expected == action => Ok(Some(fact)),
        Some(_) => Err(()),
        None => Ok(None),
    }
}

fn map_decision(
    decision: qs_strategy::Decision,
    emitted_signals: Vec<qs_core::RawSignal>,
    limits: StrategyRetentionLimits,
) -> Result<StrategyDecisionDraft, StrategyDomainError> {
    let kind = match decision.kind {
        DecisionKind::Entry => StrategyDecisionKind::Entry,
        DecisionKind::Management => StrategyDecisionKind::Management,
        DecisionKind::Exit => StrategyDecisionKind::Exit,
        DecisionKind::Observation => StrategyDecisionKind::Annotation,
    };
    StrategyDecisionDraft::new(
        kind,
        decision.reason,
        decision.related_trade.map(|trade| trade.trade_id),
        emitted_signals,
        limits,
    )
}

fn map_note(
    note: qs_strategy::Note,
    symbol: &str,
    limits: StrategyResearchLimits,
) -> Result<StrategyJournalDraft, ConfiguredStrategyAdapterError> {
    let kind = match note.kind {
        NoteKind::Observation | NoteKind::Risk => JournalKind::DecisionContext,
        NoteKind::Execution | NoteKind::Lifecycle => JournalKind::OutcomeReview,
    };
    let mut values = BTreeMap::new();
    for output in note.values {
        let value = match output.value {
            OutputScalar::Integer(value) => {
                if value.unsigned_abs() > MAX_EXACT_F64_INTEGER {
                    return Err(ConfiguredStrategyAdapterError::IntegerOutputPrecision);
                }
                value as f64
            }
            OutputScalar::Number(value) | OutputScalar::Price(value) => value,
        };
        values.insert(output.name, value);
    }
    Ok(StrategyJournalDraft::new(
        kind,
        symbol,
        note.related_trade.map(|trade| trade.trade_id),
        note.reason,
        None,
        values,
        limits,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::ActionDisposition;
    use qs_core::types::FutureEffect;

    #[test]
    fn applied_terminal_before_effect_completes_command_correlation() {
        let command_id = "opaque-command".to_owned();
        let mut routes = BTreeMap::from([(
            command_id.clone(),
            CommandRoute {
                action: ConfiguredActionKind::Entry,
                slot: "primary".into(),
                fact_seen: false,
                terminal: None,
            },
        )]);
        let terminal = project_command_feedback(
            &mut routes,
            &[StrategyFeedbackEvent::Disposition(
                ActionDisposition::applied(command_id.clone()),
            )],
        )
        .unwrap();

        assert_eq!(
            terminal,
            vec![CommandFeedback::Terminal {
                command_id: command_id.clone(),
                status: CommandTerminalStatus::Applied,
                reason: None,
            }]
        );
        assert!(routes.contains_key(&command_id));

        let fact = project_command_feedback(
            &mut routes,
            &[StrategyFeedbackEvent::Effect {
                action_id: Some(command_id.clone()),
                effect: FutureEffect::plain(Effect::PositionOpened {
                    id: "position-1".into(),
                }),
            }],
        )
        .unwrap();

        assert_eq!(
            fact,
            vec![CommandFeedback::Fact {
                command_id,
                fact: CommandFact::EntryFilled,
            }]
        );
        assert!(routes.is_empty());
    }
}
