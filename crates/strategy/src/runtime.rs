use std::collections::{BTreeMap, BTreeSet, VecDeque};

use qs_core::{PositionRef, RawSignal, validate_raw_signal};

use crate::expression::{
    CompiledExpr, CompiledInputProvenance, EvalScope, ExprScope, collect_material_refs,
    compile_expr,
};
use crate::material::FeedbackObservation;
use crate::{
    ActionTemplate, AssignmentConfig, CommandFact, CommandFeedback, CommandTerminalStatus,
    CompileError, CompletedBarRequirement, ConfiguredActionKind, ConfiguredStrategyRequirements,
    DecisionKind, DecisionTemplate, EvaluationError, Expr, FeedbackField, MaterialConfig,
    MaterialEvalContext, MaterialEvaluator, MaterialLibrary, MaterialLookback, MaterialParams,
    MaterialUpdateTrigger, NamedExpr, NamedInputRequirement, NoteKind, NoteTemplate, ScalarType,
    SourceId, StrategyConfig, StrategyInput, TradeSlotState, TransitionConfig, Value, ValueType,
};

type TypedIndexMap = BTreeMap<String, (usize, ValueType)>;
type LookbackMap = BTreeMap<SourceId, usize>;

#[derive(Debug, Clone, PartialEq)]
pub enum OutputScalar {
    Integer(i64),
    Number(f64),
    Price(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamedOutput {
    pub name: String,
    pub value: OutputScalar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelatedTrade {
    pub slot: String,
    pub trade_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    pub kind: DecisionKind,
    pub reason: String,
    pub related_trade: Option<RelatedTrade>,
    pub values: Vec<NamedOutput>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    pub kind: NoteKind,
    pub reason: String,
    pub related_trade: Option<RelatedTrade>,
    pub values: Vec<NamedOutput>,
}

#[derive(Debug, Clone)]
pub struct ConfiguredCommand {
    pub command_id: String,
    pub action_kind: ConfiguredActionKind,
    pub trade_slot: String,
    pub signal: RawSignal,
}

#[derive(Debug, Clone)]
pub struct StrategyOutput {
    pub decision: Option<Decision>,
    pub commands: Vec<ConfiguredCommand>,
    pub notes: Vec<Note>,
}

impl StrategyOutput {
    fn empty() -> Self {
        Self {
            decision: None,
            commands: Vec::new(),
            notes: Vec::new(),
        }
    }
}

struct CompiledMaterial {
    id: String,
    inputs: Vec<CompiledExpr>,
    input_provenance: Vec<CompiledInputProvenance>,
    evaluator: Box<dyn MaterialEvaluator>,
    output_type: ValueType,
    lookbacks: LookbackMap,
    update_trigger: MaterialUpdateTrigger,
    clear_pulse_when_idle: bool,
}

#[derive(Clone)]
struct CompiledNamedExpr {
    name: String,
    value: CompiledExpr,
}

#[derive(Clone)]
struct CompiledDecision {
    kind: DecisionKind,
    reason: String,
    trade_slot: Option<String>,
    values: Vec<CompiledNamedExpr>,
}

#[derive(Clone)]
struct CompiledNote {
    kind: NoteKind,
    reason: String,
    trade_slot: Option<String>,
    values: Vec<CompiledNamedExpr>,
}

#[derive(Clone)]
struct CompiledAssignment {
    variable: usize,
    value_type: ValueType,
    value: CompiledExpr,
}

#[derive(Clone)]
enum CompiledAction {
    Entry {
        slot: String,
        side: CompiledExpr,
        order_type: qs_core::OrderType,
        price: CompiledExpr,
        risk: CompiledExpr,
        stoploss: CompiledExpr,
        targets: Vec<CompiledExpr>,
    },
    Close {
        slot: String,
    },
    ClosePartial {
        slot: String,
        ratio: CompiledExpr,
    },
    MoveStoplossToEntry {
        slot: String,
    },
    ModifyStoploss {
        slot: String,
        price: CompiledExpr,
    },
    CancelPending {
        slot: String,
    },
}

impl CompiledAction {
    fn kind(&self) -> ConfiguredActionKind {
        match self {
            Self::Entry { .. } => ConfiguredActionKind::Entry,
            Self::Close { .. } => ConfiguredActionKind::Close,
            Self::ClosePartial { .. } => ConfiguredActionKind::ClosePartial,
            Self::MoveStoplossToEntry { .. } => ConfiguredActionKind::MoveStoplossToEntry,
            Self::ModifyStoploss { .. } => ConfiguredActionKind::ModifyStoploss,
            Self::CancelPending { .. } => ConfiguredActionKind::CancelPending,
        }
    }
}

#[derive(Clone)]
struct CompiledTransition {
    priority: i32,
    target: usize,
    when: CompiledExpr,
    assignments: Vec<CompiledAssignment>,
    decision: Option<CompiledDecision>,
    actions: Vec<CompiledAction>,
    notes: Vec<CompiledNote>,
}

struct CompiledState {
    id: String,
    transitions: Vec<CompiledTransition>,
}

#[derive(Debug, Clone)]
struct SlotBinding {
    slot: String,
    trade_id: String,
}

#[derive(Debug, Clone)]
struct CommandBinding {
    command_id: String,
    slot: String,
    action: ConfiguredActionKind,
    terminal: Option<CommandTerminalStatus>,
    facts: BTreeSet<CommandFact>,
}

#[derive(Debug, Clone)]
struct IdentityState {
    campaign_counter: u64,
    leg_counter: u64,
    command_counter: u64,
    campaign_id: Option<String>,
    slots: Vec<SlotBinding>,
    commands: Vec<CommandBinding>,
}

struct ActionIdentityContext<'a> {
    strategy_id: &'a str,
    instance_id: &'a str,
    symbol: &'a str,
}

struct RequirementCollector {
    named_order: Vec<String>,
    named: BTreeMap<String, ValueType>,
    direct_lookbacks: LookbackMap,
    needs_feedback: bool,
}

impl RequirementCollector {
    fn new() -> Self {
        Self {
            named_order: Vec::new(),
            named: BTreeMap::new(),
            direct_lookbacks: BTreeMap::new(),
            needs_feedback: false,
        }
    }

    fn add_named(
        &mut self,
        name: &str,
        value_type: ValueType,
        path: &str,
    ) -> Result<(), CompileError> {
        crate::validate_id(name).map_err(|reason| CompileError::InvalidIdentifier {
            path: path.into(),
            reason,
        })?;
        if let Some(existing) = self.named.get(name) {
            if *existing != value_type {
                return Err(CompileError::TypeMismatch {
                    path: path.into(),
                    expected: *existing,
                    actual: value_type,
                });
            }
        } else {
            self.named_order.push(name.into());
            self.named.insert(name.into(), value_type);
        }
        Ok(())
    }

    fn add_source(&mut self, source: &SourceId, lookback: usize) {
        self.direct_lookbacks
            .entry(source.clone())
            .and_modify(|current| *current = (*current).max(lookback))
            .or_insert(lookback);
    }
}

pub struct ConfiguredStrategy {
    strategy_id: String,
    instance_id: String,
    primary_symbol: String,
    source_set: BTreeSet<SourceId>,
    materials: Vec<CompiledMaterial>,
    material_values: Vec<Value>,
    variables: Vec<Value>,
    states: Vec<CompiledState>,
    current_state: usize,
    identity: IdentityState,
    pending_feedback: Vec<FeedbackObservation>,
    requirements: ConfiguredStrategyRequirements,
    terminal: bool,
}

impl ConfiguredStrategy {
    pub fn compile(
        config: StrategyConfig,
        library: &MaterialLibrary,
        instance_id: impl Into<String>,
        primary_symbol: impl Into<String>,
    ) -> Result<Self, CompileError> {
        validate_config_bounds(&config)?;
        validate_id_at(&config.strategy_id, "strategy_id")?;
        crate::validate_text(&config.title, crate::MAX_TEXT_BYTES).map_err(|reason| {
            CompileError::InvalidConfig {
                path: "title".into(),
                reason,
            }
        })?;
        let instance_id = instance_id.into();
        let primary_symbol = primary_symbol.into();
        validate_id_at(&instance_id, "instance_id")?;
        validate_id_at(&primary_symbol, "primary_symbol")?;
        if generated_id(&config.strategy_id, &instance_id, "campaign", u64::MAX).is_err() {
            return Err(CompileError::InvalidIdCapacity);
        }

        let source_set = compile_sources(&config)?;
        let trade_slots = compile_trade_slots(&config)?;
        let mut collected = collect_requirements(&config, &source_set)?;
        let (variable_map, variables) = compile_variables(&config)?;
        let (materials, material_map, material_lookbacks) = compile_materials(
            &config.materials,
            library,
            &variable_map,
            &trade_slots,
            &source_set,
        )?;
        if materials
            .iter()
            .any(|material| material.update_trigger == MaterialUpdateTrigger::FeedbackPulse)
        {
            collected.needs_feedback = true;
        }
        merge_lookbacks(&mut collected.direct_lookbacks, &material_lookbacks);
        let states = compile_states(
            &config,
            &variable_map,
            &material_map,
            &trade_slots,
            &source_set,
        )?;
        let current_state = states
            .iter()
            .position(|state| state.id == config.initial_state)
            .ok_or_else(|| CompileError::InvalidStateTarget {
                path: "initial_state".into(),
                target: config.initial_state.clone(),
            })?;
        validate_reachable(&states, current_state)?;
        let material_values = materials
            .iter()
            .map(|material| Value::Missing(material.output_type.scalar))
            .collect();
        let requirements = ConfiguredStrategyRequirements {
            completed_bars: config
                .sources
                .iter()
                .filter_map(|source| {
                    collected
                        .direct_lookbacks
                        .get(source)
                        .map(|lookback| CompletedBarRequirement {
                            source: source.clone(),
                            required_lookback: *lookback,
                        })
                })
                .collect(),
            named_inputs: collected
                .named_order
                .iter()
                .map(|name| NamedInputRequirement {
                    name: name.clone(),
                    value_type: collected.named[name],
                })
                .collect(),
            trade_slots: config.trade_slots.clone(),
            needs_command_feedback: collected.needs_feedback,
        };
        Ok(Self {
            strategy_id: config.strategy_id,
            instance_id,
            primary_symbol,
            source_set,
            materials,
            material_values,
            variables,
            states,
            current_state,
            identity: IdentityState {
                campaign_counter: 0,
                leg_counter: 0,
                command_counter: 0,
                campaign_id: None,
                slots: Vec::new(),
                commands: Vec::new(),
            },
            pending_feedback: Vec::new(),
            requirements,
            terminal: false,
        })
    }

    pub fn state_id(&self) -> &str {
        &self.states[self.current_state].id
    }

    pub fn input_requirements(&self) -> &ConfiguredStrategyRequirements {
        &self.requirements
    }

    pub fn trade_id_for_slot(&self, slot: &str) -> Option<&str> {
        self.identity
            .slots
            .iter()
            .find(|item| item.slot == slot)
            .map(|item| item.trade_id.as_str())
    }

    pub fn evaluate(&mut self, input: &StrategyInput) -> Result<StrategyOutput, EvaluationError> {
        if self.terminal {
            return Err(EvaluationError::Terminal);
        }
        let result = self.evaluate_staged(input);
        if result.is_err() {
            self.terminal = true;
        }
        result
    }

    fn evaluate_staged(
        &mut self,
        input: &StrategyInput,
    ) -> Result<StrategyOutput, EvaluationError> {
        validate_input(input, &self.requirements, &self.source_set)?;
        let mut pending = self.pending_feedback.clone();
        let retained_observations = pending.clone();
        let had_pending = !pending.is_empty();
        let mut identity = self.identity.clone();
        let current_observations = process_feedback(&mut identity, &input.feedback)?;
        let mut observations = pending.clone();
        observations.extend(current_observations.iter().cloned());
        if input.ready {
            pending.clear();
        } else {
            append_pending_feedback(&mut pending, &current_observations)?;
        }
        let retaining_feedback = had_pending || !pending.is_empty();
        let mut evaluators: Vec<_> = self
            .materials
            .iter()
            .map(|material| material.evaluator.clone())
            .collect();
        let mut material_values = self.material_values.clone();
        let mut material_updates = vec![false; self.materials.len()];
        let updated_sources = input
            .completed_bars
            .iter()
            .map(|item| item.source.clone())
            .collect::<BTreeSet<_>>();
        for index in 0..self.materials.len() {
            let input_updates = self.materials[index]
                .input_provenance
                .iter()
                .map(|provenance| {
                    provenance
                        .material_indexes
                        .iter()
                        .all(|dependency| material_updates[*dependency])
                        && provenance
                            .sources
                            .iter()
                            .all(|source| updated_sources.contains(source))
                        && provenance.named_inputs.iter().all(|name| {
                            input
                                .values
                                .iter()
                                .find(|item| item.name == *name)
                                .is_some_and(|item| item.updated)
                        })
                        && !provenance.dynamic
                })
                .collect::<Vec<_>>();
            let triggered = match &self.materials[index].update_trigger {
                MaterialUpdateTrigger::EveryInput => true,
                MaterialUpdateTrigger::Source(source) => updated_sources.contains(source),
                MaterialUpdateTrigger::FeedbackPulse => {
                    !input.feedback.is_empty() || !retaining_feedback
                }
                MaterialUpdateTrigger::AllInputs => {
                    !input_updates.is_empty() && input_updates.iter().all(|updated| *updated)
                }
            };
            if !triggered {
                if self.materials[index].clear_pulse_when_idle {
                    material_values[index] = Value::Bool(false);
                }
                continue;
            }
            let scope = EvalScope {
                variables: &self.variables,
                materials: &material_values,
                input,
                feedback: &observations,
            };
            let mut values = Vec::with_capacity(self.materials[index].inputs.len());
            for expression in &self.materials[index].inputs {
                values.push(
                    expression.eval(&scope, &format!("materials[{}]", self.materials[index].id))?,
                );
            }
            let context = MaterialEvalContext {
                input,
                input_updates: &input_updates,
                feedback: &current_observations,
                retained_feedback: &retained_observations,
            };
            let value = evaluators[index]
                .evaluate(&values, &context)
                .map_err(|reason| {
                    crate::material::material_error(&self.materials[index].id, reason)
                })?;
            ensure_runtime_type(
                &value,
                self.materials[index].output_type,
                &format!("materials[{}]", self.materials[index].id),
            )?;
            material_values[index] = value;
            material_updates[index] = true;
        }

        if !input.ready {
            self.commit_all(evaluators, material_values, identity, pending);
            return Ok(StrategyOutput::empty());
        }

        let scope = EvalScope {
            variables: &self.variables,
            materials: &material_values,
            input,
            feedback: &observations,
        };
        let mut selected = None;
        for transition in &self.states[self.current_state].transitions {
            match transition.when.eval(&scope, "transition.when")? {
                Value::Bool(true) => {
                    selected = Some(transition.clone());
                    break;
                }
                Value::Bool(false) => {}
                value => {
                    return Err(EvaluationError::TypeMismatch {
                        path: "transition.when".into(),
                        expected: ScalarType::Bool,
                        actual: Some(value.scalar_type()),
                    });
                }
            }
        }
        let Some(transition) = selected else {
            self.commit_all(evaluators, material_values, identity, pending);
            return Ok(StrategyOutput::empty());
        };
        let mut variables = self.variables.clone();
        let assignment_scope = EvalScope {
            variables: &self.variables,
            materials: &material_values,
            input,
            feedback: &observations,
        };
        for assignment in &transition.assignments {
            let value = assignment
                .value
                .eval(&assignment_scope, "transition.assignment")?;
            ensure_runtime_type(&value, assignment.value_type, "transition.assignment")?;
            variables[assignment.variable] = value;
        }
        let output_scope = EvalScope {
            variables: &variables,
            materials: &material_values,
            input,
            feedback: &observations,
        };
        let action_ids = ActionIdentityContext {
            strategy_id: &self.strategy_id,
            instance_id: &self.instance_id,
            symbol: &self.primary_symbol,
        };
        let mut commands = Vec::with_capacity(transition.actions.len());
        for (index, action) in transition.actions.iter().enumerate() {
            commands.push(lower_action(
                action,
                &output_scope,
                input,
                &action_ids,
                &mut identity,
                index,
            )?);
        }
        let decision = transition
            .decision
            .as_ref()
            .map(|template| evaluate_decision(template, &output_scope, &identity))
            .transpose()?;
        let notes = transition
            .notes
            .iter()
            .map(|template| evaluate_note(template, &output_scope, &identity))
            .collect::<Result<Vec<_>, _>>()?;
        for (material, evaluator) in self.materials.iter_mut().zip(evaluators) {
            material.evaluator = evaluator;
        }
        self.material_values = material_values;
        self.variables = variables;
        self.current_state = transition.target;
        self.identity = identity;
        self.pending_feedback = pending;
        Ok(StrategyOutput {
            decision,
            commands,
            notes,
        })
    }

    fn commit_materials(
        &mut self,
        evaluators: Vec<Box<dyn MaterialEvaluator>>,
        values: Vec<Value>,
        pending: Vec<FeedbackObservation>,
    ) {
        for (material, evaluator) in self.materials.iter_mut().zip(evaluators) {
            material.evaluator = evaluator;
        }
        self.material_values = values;
        self.pending_feedback = pending;
    }

    fn commit_all(
        &mut self,
        evaluators: Vec<Box<dyn MaterialEvaluator>>,
        values: Vec<Value>,
        identity: IdentityState,
        pending: Vec<FeedbackObservation>,
    ) {
        self.commit_materials(evaluators, values, pending);
        self.identity = identity;
    }
}

fn compile_sources(config: &StrategyConfig) -> Result<BTreeSet<SourceId>, CompileError> {
    let mut sources = BTreeSet::new();
    for source in &config.sources {
        if !sources.insert(source.clone()) {
            return Err(CompileError::DuplicateIdentifier {
                path: "sources".into(),
                id: source.to_string(),
            });
        }
    }
    Ok(sources)
}

fn compile_trade_slots(config: &StrategyConfig) -> Result<BTreeSet<String>, CompileError> {
    let mut slots = BTreeSet::new();
    for (index, slot) in config.trade_slots.iter().enumerate() {
        validate_id_at(slot, &format!("trade_slots[{index}]"))?;
        if !slots.insert(slot.clone()) {
            return Err(CompileError::DuplicateIdentifier {
                path: "trade_slots".into(),
                id: slot.clone(),
            });
        }
    }
    Ok(slots)
}

fn collect_requirements(
    config: &StrategyConfig,
    sources: &BTreeSet<SourceId>,
) -> Result<RequirementCollector, CompileError> {
    let mut collector = RequirementCollector::new();
    for (index, material) in config.materials.iter().enumerate() {
        for (input_index, input) in material.inputs.iter().enumerate() {
            collect_expr_requirements(
                input,
                &format!("materials[{index}].inputs[{input_index}]"),
                sources,
                &mut collector,
            )?;
        }
        match &material.params {
            MaterialParams::BarField { source, .. } => {
                require_source(
                    sources,
                    source,
                    &format!("materials[{index}].params.source"),
                )?;
                collector.add_source(source, 1);
            }
            MaterialParams::Atr { source, period } => {
                require_source(
                    sources,
                    source,
                    &format!("materials[{index}].params.source"),
                )?;
                collector.add_source(source, usize::from(*period) + 1);
            }
            MaterialParams::Feedback { .. } => collector.needs_feedback = true,
            _ => {}
        }
    }
    for (state_index, state) in config.states.iter().enumerate() {
        for (transition_index, transition) in state.transitions.iter().enumerate() {
            let base = format!("states[{state_index}].transitions[{transition_index}]");
            collect_expr_requirements(
                &transition.when,
                &format!("{base}.when"),
                sources,
                &mut collector,
            )?;
            for (index, assignment) in transition.assignments.iter().enumerate() {
                collect_expr_requirements(
                    &assignment.value,
                    &format!("{base}.assignments[{index}]"),
                    sources,
                    &mut collector,
                )?;
            }
            if let Some(decision) = &transition.decision {
                for (index, value) in decision.values.iter().enumerate() {
                    collect_expr_requirements(
                        &value.value,
                        &format!("{base}.decision.values[{index}]"),
                        sources,
                        &mut collector,
                    )?;
                }
            }
            for (index, note) in transition.notes.iter().enumerate() {
                for (value_index, value) in note.values.iter().enumerate() {
                    collect_expr_requirements(
                        &value.value,
                        &format!("{base}.notes[{index}].values[{value_index}]"),
                        sources,
                        &mut collector,
                    )?;
                }
            }
            for (index, action) in transition.actions.iter().enumerate() {
                for expression in action_expressions(action) {
                    collect_expr_requirements(
                        expression,
                        &format!("{base}.actions[{index}]"),
                        sources,
                        &mut collector,
                    )?;
                }
            }
        }
    }
    Ok(collector)
}

fn collect_expr_requirements(
    expression: &Expr,
    path: &str,
    sources: &BTreeSet<SourceId>,
    collector: &mut RequirementCollector,
) -> Result<(), CompileError> {
    match expression {
        Expr::Input { field, value_type } => collector.add_named(field, *value_type, path)?,
        Expr::Bar { source, .. } => {
            require_source(sources, source, path)?;
            collector.add_source(source, 1);
        }
        Expr::Feedback { .. } => collector.needs_feedback = true,
        Expr::Eq { left, right }
        | Expr::Ne { left, right }
        | Expr::Lt { left, right }
        | Expr::Le { left, right }
        | Expr::Gt { left, right }
        | Expr::Ge { left, right }
        | Expr::Add { left, right }
        | Expr::Sub { left, right }
        | Expr::Mul { left, right }
        | Expr::Div { left, right }
        | Expr::Min { left, right }
        | Expr::Max { left, right } => {
            collect_expr_requirements(left, path, sources, collector)?;
            collect_expr_requirements(right, path, sources, collector)?;
        }
        Expr::All { items } | Expr::Any { items } => {
            for item in items {
                collect_expr_requirements(item, path, sources, collector)?;
            }
        }
        Expr::Not { value }
        | Expr::Abs { value }
        | Expr::IsPresent { value }
        | Expr::IsMissing { value } => {
            collect_expr_requirements(value, path, sources, collector)?;
        }
        _ => {}
    }
    Ok(())
}

fn action_expressions(action: &ActionTemplate) -> Vec<&Expr> {
    match action {
        ActionTemplate::Entry {
            side,
            price,
            risk,
            stoploss,
            targets,
            ..
        } => {
            let mut values = vec![side, price, risk, stoploss];
            values.extend(targets);
            values
        }
        ActionTemplate::ClosePartial { ratio, .. } => vec![ratio],
        ActionTemplate::ModifyStoploss { price, .. } => vec![price],
        _ => Vec::new(),
    }
}

fn compile_variables(config: &StrategyConfig) -> Result<(TypedIndexMap, Vec<Value>), CompileError> {
    let mut map = BTreeMap::new();
    let mut values = Vec::new();
    for (index, variable) in config.variables.iter().enumerate() {
        validate_id_at(&variable.id, &format!("variables[{index}].id"))?;
        if map.contains_key(&variable.id) {
            return Err(CompileError::DuplicateIdentifier {
                path: "variables".into(),
                id: variable.id.clone(),
            });
        }
        require_type(
            variable.initial.value_type(),
            variable.value_type,
            &format!("variables[{index}].initial"),
        )?;
        values.push(variable.initial.to_value()?);
        map.insert(variable.id.clone(), (index, variable.value_type));
    }
    Ok((map, values))
}

fn compile_materials(
    configs: &[MaterialConfig],
    library: &MaterialLibrary,
    variables: &TypedIndexMap,
    trade_slots: &BTreeSet<String>,
    sources: &BTreeSet<SourceId>,
) -> Result<(Vec<CompiledMaterial>, TypedIndexMap, LookbackMap), CompileError> {
    let mut ids = BTreeMap::new();
    for (index, material) in configs.iter().enumerate() {
        validate_id_at(&material.id, &format!("materials[{index}].id"))?;
        validate_id_at(&material.key, &format!("materials[{index}].key"))?;
        if ids.insert(material.id.clone(), index).is_some() {
            return Err(CompileError::DuplicateIdentifier {
                path: "materials".into(),
                id: material.id.clone(),
            });
        }
        if library.factory(&material.key).is_none() {
            return Err(CompileError::UnknownMaterialKey {
                path: format!("materials[{index}].key"),
                key: material.key.clone(),
            });
        }
    }
    let mut dependents = vec![Vec::new(); configs.len()];
    let mut indegree = vec![0usize; configs.len()];
    for (index, material) in configs.iter().enumerate() {
        let mut references = Vec::new();
        for (input_index, input) in material.inputs.iter().enumerate() {
            collect_material_refs(
                input,
                &mut references,
                &format!("materials[{index}].inputs[{input_index}]"),
            )?;
        }
        let mut unique = BTreeSet::new();
        for reference in references {
            let dependency =
                *ids.get(&reference)
                    .ok_or_else(|| CompileError::UnknownReference {
                        path: format!("materials[{index}].inputs"),
                        reference,
                    })?;
            if unique.insert(dependency) {
                dependents[dependency].push(index);
                indegree[index] += 1;
            }
        }
    }
    let mut ready = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect::<BTreeSet<_>>();
    let mut order = Vec::new();
    while let Some(index) = ready.pop_first() {
        order.push(index);
        for dependent in &dependents[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready.insert(*dependent);
            }
        }
    }
    if order.len() != configs.len() {
        return Err(CompileError::DependencyCycle {
            materials: configs
                .iter()
                .enumerate()
                .filter(|(index, _)| indegree[*index] > 0)
                .map(|(_, material)| material.id.clone())
                .collect(),
        });
    }

    let mut compiled: Vec<CompiledMaterial> = Vec::new();
    let mut map = BTreeMap::new();
    let mut aggregate = BTreeMap::new();
    for original in order {
        let material = &configs[original];
        validate_material_params(&material.params, trade_slots, sources, original)?;
        let scope = ExprScope {
            variables,
            materials: &map,
            trade_slots,
            sources,
        };
        let mut inputs = Vec::new();
        let mut input_types = Vec::new();
        for (index, input) in material.inputs.iter().enumerate() {
            let (compiled_input, value_type) = compile_expr(
                input,
                &scope,
                &format!("materials[{original}].inputs[{index}]"),
            )?;
            inputs.push(compiled_input);
            input_types.push(value_type);
        }
        let provenance = inputs
            .iter()
            .map(CompiledExpr::provenance)
            .collect::<Vec<_>>();
        if matches!(
            material.key.as_str(),
            crate::MATERIAL_CROSS_ABOVE | crate::MATERIAL_CROSS_BELOW
        ) && inputs
            .iter()
            .any(|input| input.direct_material_index().is_none())
        {
            return Err(CompileError::InvalidConfig {
                path: format!("materials[{original}].inputs"),
                reason: "crossing inputs must be direct material references".into(),
            });
        }
        if library.is_custom(&material.key) && !matches!(&material.params, MaterialParams::None) {
            return Err(CompileError::InvalidConfig {
                path: format!("materials[{original}].params"),
                reason: "custom material factories are parameterless".into(),
            });
        }
        let factory = library.factory(&material.key).unwrap();
        let trigger = factory
            .update_trigger(&material.params, &input_types)
            .map_err(|reason| CompileError::MaterialFactory {
                path: format!("materials[{original}].update_trigger"),
                reason,
            })?;
        validate_trigger(&trigger, &provenance, original)?;
        let build = factory
            .build(&material.params, &input_types)
            .map_err(|reason| CompileError::MaterialFactory {
                path: format!("materials[{original}]"),
                reason,
            })?;
        check_bound(
            &format!("materials[{original}].max_state_bytes"),
            build.max_state_bytes,
            crate::MAX_MATERIAL_STATE_BYTES,
        )?;
        let mut upstream = LookbackMap::new();
        for item in &provenance {
            for source in &item.sources {
                merge_lookback(&mut upstream, source.clone(), 1);
            }
            for dependency in &item.material_indexes {
                merge_lookbacks(&mut upstream, &compiled[*dependency].lookbacks);
            }
        }
        let lookbacks = apply_lookback_contract(build.lookback, upstream, original, sources)?;
        merge_lookbacks(&mut aggregate, &lookbacks);
        let index = compiled.len();
        map.insert(material.id.clone(), (index, build.output_type));
        compiled.push(CompiledMaterial {
            id: material.id.clone(),
            inputs,
            input_provenance: provenance,
            evaluator: build.evaluator,
            output_type: build.output_type,
            lookbacks,
            update_trigger: trigger,
            clear_pulse_when_idle: matches!(
                material.key.as_str(),
                crate::MATERIAL_CROSS_ABOVE | crate::MATERIAL_CROSS_BELOW
            ),
        });
    }
    Ok((compiled, map, aggregate))
}

fn validate_trigger(
    trigger: &MaterialUpdateTrigger,
    provenance: &[CompiledInputProvenance],
    index: usize,
) -> Result<(), CompileError> {
    if matches!(trigger, MaterialUpdateTrigger::AllInputs) {
        if provenance.is_empty() {
            return Err(CompileError::InvalidConfig {
                path: format!("materials[{index}].update_trigger"),
                reason: "AllInputs requires at least one input".into(),
            });
        }
        if provenance.iter().any(|item| item.dynamic) {
            return Err(CompileError::InvalidConfig {
                path: format!("materials[{index}].update_trigger"),
                reason: "AllInputs cannot use dynamic position, feedback, time, readiness, or variable dependencies".into(),
            });
        }
    }
    Ok(())
}

fn apply_lookback_contract(
    contract: MaterialLookback,
    mut upstream: LookbackMap,
    index: usize,
    sources: &BTreeSet<SourceId>,
) -> Result<LookbackMap, CompileError> {
    match contract {
        MaterialLookback::None => {
            if !upstream.is_empty() {
                return Err(CompileError::InvalidConfig {
                    path: format!("materials[{index}].lookback"),
                    reason: "material with source dependencies must declare inherited or explicit lookback".into(),
                });
            }
        }
        MaterialLookback::Sources(requirements) => {
            for requirement in requirements {
                require_source(
                    sources,
                    &requirement.source,
                    &format!("materials[{index}].lookback"),
                )?;
                check_bound(
                    &format!("materials[{index}].lookback"),
                    requirement.required_lookback,
                    crate::MAX_MATERIAL_LOOKBACK,
                )?;
                merge_lookback(
                    &mut upstream,
                    requirement.source,
                    requirement.required_lookback,
                );
            }
        }
        MaterialLookback::InheritInputs { minimum } => {
            check_bound(
                &format!("materials[{index}].lookback"),
                minimum,
                crate::MAX_MATERIAL_LOOKBACK,
            )?;
            if upstream.is_empty() && minimum > 0 {
                return Err(CompileError::InvalidConfig {
                    path: format!("materials[{index}].lookback"),
                    reason:
                        "positive inherited lookback requires a completed-bar source dependency"
                            .into(),
                });
            }
            for value in upstream.values_mut() {
                *value = (*value).max(minimum);
            }
        }
    }
    Ok(upstream)
}

fn validate_material_params(
    params: &MaterialParams,
    trade_slots: &BTreeSet<String>,
    sources: &BTreeSet<SourceId>,
    index: usize,
) -> Result<(), CompileError> {
    match params {
        MaterialParams::BarField { source, .. } | MaterialParams::Atr { source, .. } => {
            require_source(
                sources,
                source,
                &format!("materials[{index}].params.source"),
            )?;
        }
        MaterialParams::Position { slot } | MaterialParams::Feedback { slot, .. } => {
            require_trade_slot(
                trade_slots,
                slot,
                &format!("materials[{index}].params.slot"),
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn compile_states(
    config: &StrategyConfig,
    variables: &TypedIndexMap,
    materials: &TypedIndexMap,
    trade_slots: &BTreeSet<String>,
    sources: &BTreeSet<SourceId>,
) -> Result<Vec<CompiledState>, CompileError> {
    let mut state_ids = BTreeMap::new();
    for (index, state) in config.states.iter().enumerate() {
        validate_id_at(&state.id, &format!("states[{index}].id"))?;
        if state_ids.insert(state.id.clone(), index).is_some() {
            return Err(CompileError::DuplicateIdentifier {
                path: "states".into(),
                id: state.id.clone(),
            });
        }
    }
    let scope = ExprScope {
        variables,
        materials,
        trade_slots,
        sources,
    };
    let mut states = Vec::new();
    for (state_index, state) in config.states.iter().enumerate() {
        let mut priorities = BTreeSet::new();
        let mut transitions = Vec::new();
        for (transition_index, transition) in state.transitions.iter().enumerate() {
            if !priorities.insert(transition.priority) {
                return Err(CompileError::PriorityConflict {
                    state: state.id.clone(),
                    priority: transition.priority,
                });
            }
            let target = *state_ids.get(&transition.target).ok_or_else(|| {
                CompileError::InvalidStateTarget {
                    path: format!("states[{state_index}].transitions[{transition_index}].target"),
                    target: transition.target.clone(),
                }
            })?;
            if target == state_index {
                return Err(CompileError::InvalidStateTarget {
                    path: format!("states[{state_index}].transitions[{transition_index}].target"),
                    target: transition.target.clone(),
                });
            }
            transitions.push(compile_transition(
                transition,
                target,
                &scope,
                variables,
                &format!("states[{state_index}].transitions[{transition_index}]"),
            )?);
        }
        transitions.sort_by_key(|item| std::cmp::Reverse(item.priority));
        states.push(CompiledState {
            id: state.id.clone(),
            transitions,
        });
    }
    Ok(states)
}

fn compile_transition(
    config: &TransitionConfig,
    target: usize,
    scope: &ExprScope<'_>,
    variables: &TypedIndexMap,
    path: &str,
) -> Result<CompiledTransition, CompileError> {
    if !config.actions.is_empty() && config.decision.is_none() {
        return Err(CompileError::InvalidConfig {
            path: format!("{path}.decision"),
            reason: "a transition with actions requires a decision".into(),
        });
    }
    let (when, when_type) = compile_expr(&config.when, scope, &format!("{path}.when"))?;
    require_type(
        when_type,
        ValueType::required(ScalarType::Bool),
        &format!("{path}.when"),
    )?;
    let assignments = config
        .assignments
        .iter()
        .enumerate()
        .map(|(index, item)| {
            compile_assignment(
                item,
                scope,
                variables,
                &format!("{path}.assignments[{index}]"),
            )
        })
        .collect::<Result<_, _>>()?;
    let decision = config
        .decision
        .as_ref()
        .map(|item| compile_decision(item, scope, &format!("{path}.decision")))
        .transpose()?;
    let actions = config
        .actions
        .iter()
        .enumerate()
        .map(|(index, item)| compile_action(item, scope, &format!("{path}.actions[{index}]")))
        .collect::<Result<_, _>>()?;
    let notes = config
        .notes
        .iter()
        .enumerate()
        .map(|(index, item)| compile_note(item, scope, &format!("{path}.notes[{index}]")))
        .collect::<Result<_, _>>()?;
    Ok(CompiledTransition {
        priority: config.priority,
        target,
        when,
        assignments,
        decision,
        actions,
        notes,
    })
}

fn compile_assignment(
    config: &AssignmentConfig,
    scope: &ExprScope<'_>,
    variables: &TypedIndexMap,
    path: &str,
) -> Result<CompiledAssignment, CompileError> {
    let (index, expected) =
        variables
            .get(&config.variable)
            .ok_or_else(|| CompileError::UnknownReference {
                path: path.into(),
                reference: config.variable.clone(),
            })?;
    let (value, actual) = compile_expr(&config.value, scope, path)?;
    require_type(actual, *expected, path)?;
    Ok(CompiledAssignment {
        variable: *index,
        value_type: *expected,
        value,
    })
}

fn compile_named(
    values: &[NamedExpr],
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<Vec<CompiledNamedExpr>, CompileError> {
    check_bound(path, values.len(), crate::MAX_NAMED_VALUES)?;
    let mut names = BTreeSet::new();
    values
        .iter()
        .enumerate()
        .map(|(index, item)| {
            validate_id_at(&item.name, &format!("{path}[{index}].name"))?;
            if !names.insert(item.name.clone()) {
                return Err(CompileError::DuplicateIdentifier {
                    path: path.into(),
                    id: item.name.clone(),
                });
            }
            let (value, value_type) =
                compile_expr(&item.value, scope, &format!("{path}[{index}].value"))?;
            if value_type.optional
                || !matches!(
                    value_type.scalar,
                    ScalarType::Integer | ScalarType::Number | ScalarType::Price
                )
            {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}[{index}].value"),
                    reason: "output values must be required Integer, Number, or Price".into(),
                });
            }
            Ok(CompiledNamedExpr {
                name: item.name.clone(),
                value,
            })
        })
        .collect()
}

fn compile_decision(
    template: &DecisionTemplate,
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<CompiledDecision, CompileError> {
    validate_related_slot(template.trade_slot.as_deref(), scope, path)?;
    validate_reason(&template.reason, path)?;
    Ok(CompiledDecision {
        kind: template.kind,
        reason: template.reason.clone(),
        trade_slot: template.trade_slot.clone(),
        values: compile_named(&template.values, scope, &format!("{path}.values"))?,
    })
}

fn compile_note(
    template: &NoteTemplate,
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<CompiledNote, CompileError> {
    validate_related_slot(template.trade_slot.as_deref(), scope, path)?;
    validate_reason(&template.reason, path)?;
    Ok(CompiledNote {
        kind: template.kind,
        reason: template.reason.clone(),
        trade_slot: template.trade_slot.clone(),
        values: compile_named(&template.values, scope, &format!("{path}.values"))?,
    })
}

fn validate_related_slot(
    slot: Option<&str>,
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<(), CompileError> {
    if let Some(slot) = slot {
        require_trade_slot(scope.trade_slots, slot, &format!("{path}.trade_slot"))?;
    }
    Ok(())
}

fn validate_reason(reason: &str, path: &str) -> Result<(), CompileError> {
    crate::validate_text(reason, crate::MAX_TEXT_BYTES).map_err(|reason| {
        CompileError::InvalidConfig {
            path: format!("{path}.reason"),
            reason,
        }
    })
}

fn compile_action(
    action: &ActionTemplate,
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<CompiledAction, CompileError> {
    let required = |expression: &Expr, scalar, suffix: &str| {
        let (value, actual) = compile_expr(expression, scope, &format!("{path}.{suffix}"))?;
        require_type(
            actual,
            ValueType::required(scalar),
            &format!("{path}.{suffix}"),
        )?;
        Ok(value)
    };
    let optional = |expression: &Expr, scalar, suffix: &str| {
        let (value, actual) = compile_expr(expression, scope, &format!("{path}.{suffix}"))?;
        if actual.scalar != scalar {
            return Err(CompileError::TypeMismatch {
                path: format!("{path}.{suffix}"),
                expected: ValueType::optional(scalar),
                actual,
            });
        }
        Ok(value)
    };
    Ok(match action {
        ActionTemplate::Entry {
            slot,
            side,
            order_type,
            price,
            risk,
            stoploss,
            targets,
        } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            let targets = targets
                .iter()
                .enumerate()
                .map(|(index, target)| {
                    required(target, ScalarType::Price, &format!("targets[{index}]"))
                })
                .collect::<Result<_, _>>()?;
            CompiledAction::Entry {
                slot: slot.clone(),
                side: required(side, ScalarType::Side, "side")?,
                order_type: *order_type,
                price: optional(price, ScalarType::Price, "price")?,
                risk: required(risk, ScalarType::Number, "risk")?,
                stoploss: optional(stoploss, ScalarType::Price, "stoploss")?,
                targets,
            }
        }
        ActionTemplate::Close { slot } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            CompiledAction::Close { slot: slot.clone() }
        }
        ActionTemplate::ClosePartial { slot, ratio } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            CompiledAction::ClosePartial {
                slot: slot.clone(),
                ratio: required(ratio, ScalarType::Number, "ratio")?,
            }
        }
        ActionTemplate::MoveStoplossToEntry { slot } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            CompiledAction::MoveStoplossToEntry { slot: slot.clone() }
        }
        ActionTemplate::ModifyStoploss { slot, price } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            CompiledAction::ModifyStoploss {
                slot: slot.clone(),
                price: required(price, ScalarType::Price, "price")?,
            }
        }
        ActionTemplate::CancelPending { slot } => {
            require_trade_slot(scope.trade_slots, slot, &format!("{path}.slot"))?;
            CompiledAction::CancelPending { slot: slot.clone() }
        }
    })
}

fn lower_action(
    action: &CompiledAction,
    scope: &EvalScope<'_>,
    input: &StrategyInput,
    ids: &ActionIdentityContext<'_>,
    identity: &mut IdentityState,
    index: usize,
) -> Result<ConfiguredCommand, EvaluationError> {
    let kind = action.kind();
    let (signal, slot) = match action {
        CompiledAction::Entry {
            slot,
            side,
            order_type,
            price,
            risk,
            stoploss,
            targets,
        } => {
            if identity.slots.iter().any(|item| item.slot == *slot) {
                return Err(EvaluationError::InvalidAction {
                    path: format!("actions[{index}].slot"),
                    reason: "trade slot is already reserved".into(),
                });
            }
            if identity.campaign_id.is_none() {
                identity.campaign_counter = checked_next(identity.campaign_counter, "campaign")?;
                identity.leg_counter = 0;
                identity.campaign_id = Some(generated_id(
                    ids.strategy_id,
                    ids.instance_id,
                    "campaign",
                    identity.campaign_counter,
                )?);
            }
            identity.leg_counter = checked_next(identity.leg_counter, "leg")?;
            let trade_id = generated_scoped_id(
                ids.strategy_id,
                ids.instance_id,
                identity.campaign_counter,
                "trade",
                identity.leg_counter,
            )?;
            identity.slots.push(SlotBinding {
                slot: slot.clone(),
                trade_id: trade_id.clone(),
            });
            let side = required_side(side.eval(scope, "action.entry.side")?, "action.entry.side")?;
            let price = optional_price(
                price.eval(scope, "action.entry.price")?,
                "action.entry.price",
            )?;
            let risk =
                required_number(risk.eval(scope, "action.entry.risk")?, "action.entry.risk")?;
            let stoploss = optional_price(
                stoploss.eval(scope, "action.entry.stoploss")?,
                "action.entry.stoploss",
            )?;
            let targets = targets
                .iter()
                .map(|target| {
                    required_price(
                        target.eval(scope, "action.entry.targets")?,
                        "action.entry.targets",
                    )
                })
                .collect::<Result<_, _>>()?;
            (
                RawSignal::Entry {
                    ts: input.time,
                    symbol: ids.symbol.into(),
                    side,
                    order_type: *order_type,
                    price,
                    risk_multiplier: risk,
                    stoploss,
                    targets,
                    group: identity.campaign_id.clone(),
                    trade_id: Some(trade_id),
                },
                slot.clone(),
            )
        }
        CompiledAction::Close { slot } => (
            RawSignal::Close {
                ts: input.time,
                position: position_ref(identity, slot)?,
            },
            slot.clone(),
        ),
        CompiledAction::ClosePartial { slot, ratio } => (
            RawSignal::ClosePartial {
                ts: input.time,
                position: position_ref(identity, slot)?,
                ratio: required_number(
                    ratio.eval(scope, "action.close_partial.ratio")?,
                    "action.close_partial.ratio",
                )?,
            },
            slot.clone(),
        ),
        CompiledAction::MoveStoplossToEntry { slot } => (
            RawSignal::MoveStoplossToEntry {
                ts: input.time,
                position: position_ref(identity, slot)?,
            },
            slot.clone(),
        ),
        CompiledAction::ModifyStoploss { slot, price } => (
            RawSignal::ModifyStoploss {
                ts: input.time,
                position: position_ref(identity, slot)?,
                price: required_price(
                    price.eval(scope, "action.modify_stoploss.price")?,
                    "action.modify_stoploss.price",
                )?,
            },
            slot.clone(),
        ),
        CompiledAction::CancelPending { slot } => (
            RawSignal::CancelPending {
                ts: input.time,
                position: position_ref(identity, slot)?,
            },
            slot.clone(),
        ),
    };
    validate_raw_signal(&signal).map_err(|error| EvaluationError::InvalidAction {
        path: format!("actions[{index}]"),
        reason: error.to_string(),
    })?;
    if identity.commands.len() >= crate::MAX_COMMAND_CORRELATIONS {
        return Err(EvaluationError::OutputBound {
            kind: "command correlations",
        });
    }
    identity.command_counter = checked_next(identity.command_counter, "command")?;
    let command_id = generated_id(
        ids.strategy_id,
        ids.instance_id,
        "command",
        identity.command_counter,
    )?;
    identity.commands.push(CommandBinding {
        command_id: command_id.clone(),
        slot: slot.clone(),
        action: kind,
        terminal: None,
        facts: BTreeSet::new(),
    });
    Ok(ConfiguredCommand {
        command_id,
        action_kind: kind,
        trade_slot: slot,
        signal,
    })
}

fn process_feedback(
    identity: &mut IdentityState,
    feedback: &[CommandFeedback],
) -> Result<Vec<FeedbackObservation>, EvaluationError> {
    let mut observations = Vec::new();
    for event in feedback {
        let command_id = event.command_id();
        let index = identity
            .commands
            .iter()
            .position(|item| item.command_id == command_id)
            .ok_or_else(|| EvaluationError::InvalidAction {
                path: "feedback.command_id".into(),
                reason: format!("unknown or replayed command ID {command_id}"),
            })?;
        let binding = identity.commands[index].clone();
        match event {
            CommandFeedback::Fact { fact, .. } => {
                validate_fact(binding.action, *fact)?;
                if !identity.commands[index].facts.insert(*fact) {
                    return Err(EvaluationError::InvalidAction {
                        path: "feedback.fact".into(),
                        reason: "duplicate command fact".into(),
                    });
                }
                let field = match fact {
                    CommandFact::EntryFilled => Some(FeedbackField::EntryFilled),
                    CommandFact::PositionClosed => Some(FeedbackField::PositionClosed),
                    CommandFact::PendingCancelled => Some(FeedbackField::CancellationApplied),
                    CommandFact::PositionReduced | CommandFact::StoplossModified => None,
                };
                if let Some(field) = field {
                    observations.push(FeedbackObservation {
                        slot: binding.slot.clone(),
                        action: binding.action,
                        field,
                    });
                }
            }
            CommandFeedback::Terminal { status, reason, .. } => {
                validate_terminal_reason(*status, reason.as_deref())?;
                if identity.commands[index].terminal.replace(*status).is_some() {
                    return Err(EvaluationError::InvalidAction {
                        path: "feedback.terminal".into(),
                        reason: "duplicate command terminal".into(),
                    });
                }
                if *status != CommandTerminalStatus::Applied {
                    if binding.action == ConfiguredActionKind::Entry {
                        observations.push(FeedbackObservation {
                            slot: binding.slot.clone(),
                            action: binding.action,
                            field: FeedbackField::EntryRejected,
                        });
                    } else if binding.action == ConfiguredActionKind::CancelPending {
                        observations.push(FeedbackObservation {
                            slot: binding.slot.clone(),
                            action: binding.action,
                            field: FeedbackField::CancellationRejected,
                        });
                    }
                }
            }
        }

        let completed = command_completed(&identity.commands[index]);
        if completed {
            let successful =
                identity.commands[index].terminal == Some(CommandTerminalStatus::Applied);
            let release_slot = if successful {
                matches!(
                    binding.action,
                    ConfiguredActionKind::Close | ConfiguredActionKind::CancelPending
                )
            } else {
                binding.action == ConfiguredActionKind::Entry
            };
            if release_slot {
                release_trade_slot(identity, &binding.slot);
            }
            if successful && binding.action == ConfiguredActionKind::CancelPending {
                identity.commands.retain(|item| {
                    !(item.slot == binding.slot && item.action == ConfiguredActionKind::Entry)
                });
            }
            if let Some(position) = identity
                .commands
                .iter()
                .position(|item| item.command_id == command_id)
            {
                identity.commands.remove(position);
            }
        }
    }
    Ok(observations)
}

fn validate_terminal_reason(
    status: CommandTerminalStatus,
    reason: Option<&str>,
) -> Result<(), EvaluationError> {
    match (status, reason) {
        (CommandTerminalStatus::Applied, None) => Ok(()),
        (CommandTerminalStatus::Applied, Some(_)) => Err(EvaluationError::InvalidAction {
            path: "feedback.reason".into(),
            reason: "applied command terminal must not include a reason".into(),
        }),
        (_, Some(reason)) => {
            crate::validate_text(reason, crate::MAX_TEXT_BYTES).map_err(|reason| {
                EvaluationError::InvalidAction {
                    path: "feedback.reason".into(),
                    reason,
                }
            })
        }
        (_, None) => Err(EvaluationError::MissingRequired {
            path: "feedback.reason".into(),
        }),
    }
}

fn command_completed(binding: &CommandBinding) -> bool {
    match binding.terminal {
        Some(CommandTerminalStatus::Applied) => {
            binding.facts.contains(&required_fact(binding.action))
        }
        Some(_) => true,
        None => false,
    }
}

fn required_fact(action: ConfiguredActionKind) -> CommandFact {
    match action {
        ConfiguredActionKind::Entry => CommandFact::EntryFilled,
        ConfiguredActionKind::Close => CommandFact::PositionClosed,
        ConfiguredActionKind::ClosePartial => CommandFact::PositionReduced,
        ConfiguredActionKind::MoveStoplossToEntry | ConfiguredActionKind::ModifyStoploss => {
            CommandFact::StoplossModified
        }
        ConfiguredActionKind::CancelPending => CommandFact::PendingCancelled,
    }
}

fn validate_fact(action: ConfiguredActionKind, fact: CommandFact) -> Result<(), EvaluationError> {
    let compatible = matches!(
        (action, fact),
        (ConfiguredActionKind::Entry, CommandFact::EntryFilled)
            | (ConfiguredActionKind::Close, CommandFact::PositionClosed)
            | (
                ConfiguredActionKind::ClosePartial,
                CommandFact::PositionReduced
            )
            | (
                ConfiguredActionKind::MoveStoplossToEntry,
                CommandFact::StoplossModified
            )
            | (
                ConfiguredActionKind::ModifyStoploss,
                CommandFact::StoplossModified
            )
            | (
                ConfiguredActionKind::CancelPending,
                CommandFact::PendingCancelled
            )
    );
    if compatible {
        Ok(())
    } else {
        Err(EvaluationError::InvalidAction {
            path: "feedback.fact".into(),
            reason: "command fact is incompatible with the original action".into(),
        })
    }
}

fn release_trade_slot(identity: &mut IdentityState, slot: &str) {
    identity.slots.retain(|item| item.slot != slot);
    if identity.slots.is_empty() {
        identity.campaign_id = None;
    }
}

fn evaluate_decision(
    template: &CompiledDecision,
    scope: &EvalScope<'_>,
    identity: &IdentityState,
) -> Result<Decision, EvaluationError> {
    Ok(Decision {
        kind: template.kind,
        reason: template.reason.clone(),
        related_trade: resolve_related_trade(template.trade_slot.as_deref(), identity)?,
        values: evaluate_outputs(&template.values, scope)?,
    })
}

fn evaluate_note(
    template: &CompiledNote,
    scope: &EvalScope<'_>,
    identity: &IdentityState,
) -> Result<Note, EvaluationError> {
    Ok(Note {
        kind: template.kind,
        reason: template.reason.clone(),
        related_trade: resolve_related_trade(template.trade_slot.as_deref(), identity)?,
        values: evaluate_outputs(&template.values, scope)?,
    })
}

fn resolve_related_trade(
    slot: Option<&str>,
    identity: &IdentityState,
) -> Result<Option<RelatedTrade>, EvaluationError> {
    slot.map(|slot| {
        identity
            .slots
            .iter()
            .find(|item| item.slot == slot)
            .map(|item| RelatedTrade {
                slot: slot.into(),
                trade_id: item.trade_id.clone(),
            })
            .ok_or_else(|| EvaluationError::MissingRequired {
                path: format!("related trade slot {slot}"),
            })
    })
    .transpose()
}

fn evaluate_outputs(
    values: &[CompiledNamedExpr],
    scope: &EvalScope<'_>,
) -> Result<Vec<NamedOutput>, EvaluationError> {
    values
        .iter()
        .map(|item| {
            let value = item.value.eval(scope, "output.value")?;
            let value = match value {
                Value::Integer(value) if value.unsigned_abs() <= (1_u64 << 53) => {
                    OutputScalar::Integer(value)
                }
                Value::Number(value) if value.is_finite() => OutputScalar::Number(value),
                Value::Price(value) if value.is_finite() => OutputScalar::Price(value),
                Value::Integer(_) => {
                    return Err(EvaluationError::InvalidAction {
                        path: "output.value".into(),
                        reason: "integer is not exactly representable as f64".into(),
                    });
                }
                Value::Missing(_) => {
                    return Err(EvaluationError::MissingRequired {
                        path: "output.value".into(),
                    });
                }
                value => {
                    return Err(EvaluationError::TypeMismatch {
                        path: "output.value".into(),
                        expected: ScalarType::Number,
                        actual: Some(value.scalar_type()),
                    });
                }
            };
            Ok(NamedOutput {
                name: item.name.clone(),
                value,
            })
        })
        .collect()
}

fn validate_input(
    input: &StrategyInput,
    requirements: &ConfiguredStrategyRequirements,
    declared_sources: &BTreeSet<SourceId>,
) -> Result<(), EvaluationError> {
    check_runtime_bound(
        "completed bars",
        input.completed_bars.len(),
        crate::MAX_COMPLETED_BARS,
    )?;
    check_runtime_bound("input values", input.values.len(), crate::MAX_NAMED_VALUES)?;
    check_runtime_bound("trade slots", input.trade_slots.len(), crate::MAX_LEGS)?;
    check_runtime_bound(
        "feedback",
        input.feedback.len(),
        crate::MAX_PENDING_FEEDBACK,
    )?;

    let required_sources = requirements
        .completed_bars
        .iter()
        .map(|item| &item.source)
        .collect::<BTreeSet<_>>();
    let mut seen_sources = BTreeSet::new();
    for update in &input.completed_bars {
        if !declared_sources.contains(&update.source) || !required_sources.contains(&update.source)
        {
            return Err(EvaluationError::Material {
                material: "completed_bars".into(),
                reason: format!("undeclared or unrequired source {}", update.source),
            });
        }
        if !seen_sources.insert(&update.source) {
            return Err(EvaluationError::Material {
                material: "completed_bars".into(),
                reason: format!("duplicate source update {}", update.source),
            });
        }
        validate_bar(&update.bar)?;
    }

    let named_requirements = requirements
        .named_inputs
        .iter()
        .map(|item| (item.name.as_str(), item.value_type))
        .collect::<BTreeMap<_, _>>();
    let mut seen_names = BTreeSet::new();
    for value in &input.values {
        let expected = named_requirements.get(value.name.as_str()).ok_or_else(|| {
            EvaluationError::Material {
                material: "input".into(),
                reason: format!("unknown named input {}", value.name),
            }
        })?;
        if !seen_names.insert(value.name.as_str()) {
            return Err(EvaluationError::Material {
                material: "input".into(),
                reason: format!("duplicate named input {}", value.name),
            });
        }
        ensure_runtime_type(&value.value, *expected, &format!("input.{}", value.name))?;
    }
    for requirement in &requirements.named_inputs {
        if !requirement.value_type.optional
            && !input
                .values
                .iter()
                .any(|item| item.name == requirement.name)
        {
            return Err(EvaluationError::MissingRequired {
                path: format!("input.{}", requirement.name),
            });
        }
    }

    let mut seen_slots = BTreeSet::new();
    for facts in &input.trade_slots {
        if !requirements.trade_slots.contains(&facts.slot) {
            return Err(EvaluationError::Material {
                material: "trade_slots".into(),
                reason: format!("undeclared trade slot {}", facts.slot),
            });
        }
        if !seen_slots.insert(facts.slot.as_str()) {
            return Err(EvaluationError::Material {
                material: "trade_slots".into(),
                reason: format!("duplicate trade slot {}", facts.slot),
            });
        }
        validate_trade_slot_state(&facts.state)?;
    }
    for slot in &requirements.trade_slots {
        if !input.trade_slots.iter().any(|item| item.slot == *slot) {
            return Err(EvaluationError::MissingRequired {
                path: format!("trade_slots.{slot}"),
            });
        }
    }
    Ok(())
}

fn validate_bar(bar: &crate::CompletedBar) -> Result<(), EvaluationError> {
    if ![bar.open, bar.high, bar.low, bar.close, bar.volume]
        .into_iter()
        .all(f64::is_finite)
        || bar.open <= 0.0
        || bar.high <= 0.0
        || bar.low <= 0.0
        || bar.close <= 0.0
        || bar.volume < 0.0
        || bar.high < bar.low
        || bar.high < bar.open.max(bar.close)
        || bar.low > bar.open.min(bar.close)
    {
        return Err(EvaluationError::Material {
            material: "completed_bars".into(),
            reason: "invalid completed bar".into(),
        });
    }
    Ok(())
}

fn validate_trade_slot_state(state: &TradeSlotState) -> Result<(), EvaluationError> {
    let positive = |value: f64| value.is_finite() && value > 0.0;
    match state {
        TradeSlotState::Vacant => Ok(()),
        TradeSlotState::Pending {
            side,
            requested_price,
            stoploss,
        } => {
            if requested_price.is_some_and(|value| !positive(value))
                || stoploss.is_some_and(|value| !positive(value))
                || requested_price
                    .zip(*stoploss)
                    .is_some_and(|(entry, stop)| match side {
                        qs_core::Side::Buy => stop >= entry,
                        qs_core::Side::Sell => stop <= entry,
                    })
            {
                Err(EvaluationError::Material {
                    material: "trade_slots".into(),
                    reason: "invalid pending trade slot geometry".into(),
                })
            } else {
                Ok(())
            }
        }
        TradeSlotState::Open {
            entry_price,
            remaining_size,
            stoploss,
            ..
        } => {
            if !positive(*entry_price)
                || !positive(*remaining_size)
                || stoploss.is_some_and(|value| !positive(value))
            {
                Err(EvaluationError::Material {
                    material: "trade_slots".into(),
                    reason: "invalid open trade slot facts".into(),
                })
            } else {
                Ok(())
            }
        }
    }
}

fn append_pending_feedback(
    pending: &mut Vec<FeedbackObservation>,
    incoming: &[FeedbackObservation],
) -> Result<(), EvaluationError> {
    if pending.len().saturating_add(incoming.len()) > crate::MAX_PENDING_FEEDBACK {
        return Err(EvaluationError::OutputBound {
            kind: "pending feedback",
        });
    }
    pending.extend_from_slice(incoming);
    Ok(())
}

fn validate_reachable(states: &[CompiledState], initial: usize) -> Result<(), CompileError> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from([initial]);
    while let Some(state) = queue.pop_front() {
        if reached.insert(state) {
            for transition in &states[state].transitions {
                queue.push_back(transition.target);
            }
        }
    }
    for (index, state) in states.iter().enumerate() {
        if !reached.contains(&index) {
            return Err(CompileError::UnreachableState {
                state: state.id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_config_bounds(config: &StrategyConfig) -> Result<(), CompileError> {
    check_bound("sources", config.sources.len(), crate::MAX_SOURCES)?;
    check_bound("trade_slots", config.trade_slots.len(), crate::MAX_LEGS)?;
    check_bound("materials", config.materials.len(), crate::MAX_MATERIALS)?;
    check_bound("variables", config.variables.len(), crate::MAX_VARIABLES)?;
    check_bound("states", config.states.len(), crate::MAX_STATES)?;
    for (index, material) in config.materials.iter().enumerate() {
        check_bound(
            &format!("materials[{index}].inputs"),
            material.inputs.len(),
            crate::MAX_MATERIAL_INPUTS,
        )?;
    }
    for (state_index, state) in config.states.iter().enumerate() {
        check_bound(
            &format!("states[{state_index}].transitions"),
            state.transitions.len(),
            crate::MAX_TRANSITIONS,
        )?;
        for (transition_index, transition) in state.transitions.iter().enumerate() {
            let path = format!("states[{state_index}].transitions[{transition_index}]");
            check_bound(
                &format!("{path}.assignments"),
                transition.assignments.len(),
                crate::MAX_ASSIGNMENTS,
            )?;
            check_bound(
                &format!("{path}.actions"),
                transition.actions.len(),
                crate::MAX_ACTIONS,
            )?;
            check_bound(
                &format!("{path}.notes"),
                transition.notes.len(),
                crate::MAX_NOTES,
            )?;
            for (action_index, action) in transition.actions.iter().enumerate() {
                if let ActionTemplate::Entry { targets, .. } = action {
                    check_bound(
                        &format!("{path}.actions[{action_index}].targets"),
                        targets.len(),
                        crate::MAX_ENTRY_TARGETS,
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn merge_lookbacks(target: &mut LookbackMap, source: &LookbackMap) {
    for (source, lookback) in source {
        merge_lookback(target, source.clone(), *lookback);
    }
}

fn merge_lookback(target: &mut LookbackMap, source: SourceId, lookback: usize) {
    target
        .entry(source)
        .and_modify(|value| *value = (*value).max(lookback))
        .or_insert(lookback);
}

fn require_source(
    sources: &BTreeSet<SourceId>,
    source: &SourceId,
    path: &str,
) -> Result<(), CompileError> {
    if sources.contains(source) {
        Ok(())
    } else {
        Err(CompileError::UnknownReference {
            path: path.into(),
            reference: source.to_string(),
        })
    }
}

fn require_trade_slot(
    trade_slots: &BTreeSet<String>,
    slot: &str,
    path: &str,
) -> Result<(), CompileError> {
    validate_id_at(slot, path)?;
    if trade_slots.contains(slot) {
        Ok(())
    } else {
        Err(CompileError::UnknownReference {
            path: path.into(),
            reference: slot.into(),
        })
    }
}

fn validate_id_at(value: &str, path: &str) -> Result<(), CompileError> {
    crate::validate_id(value).map_err(|reason| CompileError::InvalidIdentifier {
        path: path.into(),
        reason,
    })
}

fn check_bound(path: &str, actual: usize, limit: usize) -> Result<(), CompileError> {
    if actual > limit {
        Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn check_runtime_bound(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), EvaluationError> {
    if actual > limit {
        Err(EvaluationError::OutputBound { kind })
    } else {
        Ok(())
    }
}

fn require_type(actual: ValueType, expected: ValueType, path: &str) -> Result<(), CompileError> {
    if actual.scalar != expected.scalar {
        Err(CompileError::TypeMismatch {
            path: path.into(),
            expected,
            actual,
        })
    } else if !expected.optional && actual.optional {
        Err(CompileError::OptionalToRequired { path: path.into() })
    } else {
        Ok(())
    }
}

fn ensure_runtime_type(
    value: &Value,
    expected: ValueType,
    path: &str,
) -> Result<(), EvaluationError> {
    if value.scalar_type() != expected.scalar || (!expected.optional && value.is_missing()) {
        return Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: expected.scalar,
            actual: if value.is_missing() {
                None
            } else {
                Some(value.scalar_type())
            },
        });
    }
    match value {
        Value::Number(value) | Value::Price(value) if !value.is_finite() => {
            Err(EvaluationError::NonFinite { path: path.into() })
        }
        Value::Text(value) => {
            crate::validate_text(value, crate::MAX_TEXT_BYTES).map_err(|reason| {
                EvaluationError::Material {
                    material: path.into(),
                    reason,
                }
            })
        }
        _ => Ok(()),
    }
}

fn position_ref(identity: &IdentityState, slot: &str) -> Result<PositionRef, EvaluationError> {
    identity
        .slots
        .iter()
        .find(|item| item.slot == slot)
        .map(|item| PositionRef::ByTradeId {
            trade_id: item.trade_id.clone(),
        })
        .ok_or_else(|| EvaluationError::InvalidAction {
            path: "action.slot".into(),
            reason: format!("trade slot {slot} is not reserved"),
        })
}

fn checked_next(value: u64, kind: &'static str) -> Result<u64, EvaluationError> {
    value
        .checked_add(1)
        .ok_or(EvaluationError::CounterExhausted { kind })
}

fn generated_id(
    strategy: &str,
    instance: &str,
    kind: &str,
    counter: u64,
) -> Result<String, EvaluationError> {
    let value = format!(
        "{}:{strategy}|{}:{instance}|{kind}:{counter}",
        strategy.len(),
        instance.len()
    );
    if value.len() > crate::MAX_GENERATED_ID_BYTES {
        Err(EvaluationError::IdCapacity)
    } else {
        Ok(value)
    }
}

fn generated_scoped_id(
    strategy: &str,
    instance: &str,
    campaign: u64,
    kind: &str,
    counter: u64,
) -> Result<String, EvaluationError> {
    let value = format!(
        "{}:{strategy}|{}:{instance}|campaign:{campaign}|{kind}:{counter}",
        strategy.len(),
        instance.len()
    );
    if value.len() > crate::MAX_GENERATED_ID_BYTES {
        Err(EvaluationError::IdCapacity)
    } else {
        Ok(value)
    }
}

fn required_number(value: Value, path: &str) -> Result<f64, EvaluationError> {
    match value {
        Value::Number(value) if value.is_finite() => Ok(value),
        Value::Missing(_) => Err(EvaluationError::MissingRequired { path: path.into() }),
        value => Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: ScalarType::Number,
            actual: Some(value.scalar_type()),
        }),
    }
}

fn required_price(value: Value, path: &str) -> Result<f64, EvaluationError> {
    match value {
        Value::Price(value) if value.is_finite() => Ok(value),
        Value::Missing(_) => Err(EvaluationError::MissingRequired { path: path.into() }),
        value => Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: ScalarType::Price,
            actual: Some(value.scalar_type()),
        }),
    }
}

fn optional_price(value: Value, path: &str) -> Result<Option<f64>, EvaluationError> {
    match value {
        Value::Price(value) if value.is_finite() => Ok(Some(value)),
        Value::Missing(ScalarType::Price) => Ok(None),
        value => Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: ScalarType::Price,
            actual: Some(value.scalar_type()),
        }),
    }
}

fn required_side(value: Value, path: &str) -> Result<qs_core::Side, EvaluationError> {
    match value {
        Value::Side(value) => Ok(value),
        Value::Missing(_) => Err(EvaluationError::MissingRequired { path: path.into() }),
        value => Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: ScalarType::Side,
            actual: Some(value.scalar_type()),
        }),
    }
}
