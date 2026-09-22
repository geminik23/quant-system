use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::expression::{EvalScope, ExprScope, collect_material_refs, compile_expr};
use crate::{
    ActionTemplate, BarField, CompileError, ConfiguredActionKind, ConfiguredStrategy, Expr,
    MaterialArg, MaterialArgs, MaterialLibrary, ParamKind, ParameterBinding, ParameterConfig,
    ParameterKind, ParameterValue, ScalarType, StrategyConfig, StrategyInput, Value,
};

/// A configured strategy document that may still contain declared parameter references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StrategyTemplate {
    document: StrategyConfig,
}

impl StrategyTemplate {
    pub fn new(document: StrategyConfig) -> Self {
        Self { document }
    }

    pub fn document(&self) -> &StrategyConfig {
        &self.document
    }

    pub fn into_document(self) -> StrategyConfig {
        self.document
    }

    /// Validate template shape and compile a representative binding for every choice option.
    pub fn validate(&self, library: &MaterialLibrary) -> Result<(), CompileError> {
        let declarations = validate_parameter_declarations(&self.document.parameters)?;
        validate_template_references(&self.document, &declarations, library)?;

        let baseline = representative_binding(&self.document.parameters)?;
        self.bind_internal(&baseline, library, true)?;
        for parameter in &self.document.parameters {
            if let ParameterKind::Choice { options } = &parameter.kind {
                for option in options {
                    let mut binding = baseline.clone();
                    binding
                        .0
                        .insert(parameter.id.clone(), ParameterValue::Choice(option.clone()));
                    self.bind_internal(&binding, library, true)?;
                }
            }
        }
        Ok(())
    }

    /// Substitute one complete parameter binding and return an ordinary compilable document.
    pub fn bind(
        &self,
        binding: &ParameterBinding,
        library: &MaterialLibrary,
    ) -> Result<StrategyConfig, CompileError> {
        let declarations = validate_parameter_declarations(&self.document.parameters)?;
        validate_template_references(&self.document, &declarations, library)?;
        validate_binding(&self.document.parameters, binding)?;
        self.bind_internal(binding, library, false)
    }

    fn bind_internal(
        &self,
        binding: &ParameterBinding,
        library: &MaterialLibrary,
        compile: bool,
    ) -> Result<StrategyConfig, CompileError> {
        validate_binding(&self.document.parameters, binding)?;
        let declarations: BTreeMap<_, _> = self
            .document
            .parameters
            .iter()
            .map(|parameter| (parameter.id.as_str(), parameter))
            .collect();
        let mut document = self.document.clone();
        document.parameters.clear();
        for (index, material) in document.materials.iter_mut().enumerate() {
            material.inputs = material
                .inputs
                .iter()
                .map(|expr| bind_expr(expr, binding, &format!("materials[{index}].inputs")))
                .collect::<Result<_, _>>()?;
            let schema = library.parameter_schema(&material.key).ok_or_else(|| {
                CompileError::UnknownMaterialKey {
                    path: format!("materials[{index}].key"),
                    key: material.key.clone(),
                }
            })?;
            material.params = bind_material_args(
                &material.params,
                schema,
                &declarations,
                binding,
                &format!("materials[{index}].params"),
            )?;
        }
        for (state_index, state) in document.states.iter_mut().enumerate() {
            for (transition_index, transition) in state.transitions.iter_mut().enumerate() {
                let path = format!("states[{state_index}].transitions[{transition_index}]");
                transition.when = bind_expr(&transition.when, binding, &format!("{path}.when"))?;
                for (index, assignment) in transition.assignments.iter_mut().enumerate() {
                    assignment.value = bind_expr(
                        &assignment.value,
                        binding,
                        &format!("{path}.assignments[{index}].value"),
                    )?;
                }
                if let Some(decision) = transition.decision.as_mut() {
                    for (index, value) in decision.values.iter_mut().enumerate() {
                        value.value = bind_expr(
                            &value.value,
                            binding,
                            &format!("{path}.decision.values[{index}]"),
                        )?;
                    }
                }
                for (index, action) in transition.actions.iter_mut().enumerate() {
                    bind_action(action, binding, &format!("{path}.actions[{index}]"))?;
                }
                for (note_index, note) in transition.notes.iter_mut().enumerate() {
                    for (value_index, value) in note.values.iter_mut().enumerate() {
                        value.value = bind_expr(
                            &value.value,
                            binding,
                            &format!("{path}.notes[{note_index}].values[{value_index}]"),
                        )?;
                    }
                }
            }
        }
        prune_materials(&mut document)?;
        if compile {
            ConfiguredStrategy::compile(document.clone(), library, "template", "TEMPLATE")?;
        }
        Ok(document)
    }
}

impl From<StrategyConfig> for StrategyTemplate {
    fn from(value: StrategyConfig) -> Self {
        Self::new(value)
    }
}

fn validate_parameter_declarations(
    parameters: &[ParameterConfig],
) -> Result<BTreeMap<&str, &ParameterConfig>, CompileError> {
    if parameters.len() > crate::MAX_PARAMETERS {
        return Err(CompileError::ExcessiveBound {
            path: "parameters".into(),
            actual: parameters.len(),
            limit: crate::MAX_PARAMETERS,
        });
    }
    let mut declarations = BTreeMap::new();
    for (index, parameter) in parameters.iter().enumerate() {
        crate::validate_id(&parameter.id).map_err(|reason| CompileError::InvalidIdentifier {
            path: format!("parameters[{index}].id"),
            reason,
        })?;
        if declarations
            .insert(parameter.id.as_str(), parameter)
            .is_some()
        {
            return Err(CompileError::DuplicateIdentifier {
                path: "parameters".into(),
                id: parameter.id.clone(),
            });
        }
        if let ParameterKind::Choice { options } = &parameter.kind {
            if options.is_empty() || options.len() > crate::MAX_PARAMETER_OPTIONS {
                return Err(CompileError::InvalidConfig {
                    path: format!("parameters[{index}].options"),
                    reason: "choice options must be non-empty and bounded".into(),
                });
            }
            let mut seen = BTreeSet::new();
            for option in options {
                crate::validate_id(option).map_err(|reason| CompileError::InvalidIdentifier {
                    path: format!("parameters[{index}].options"),
                    reason,
                })?;
                if !seen.insert(option) {
                    return Err(CompileError::DuplicateIdentifier {
                        path: format!("parameters[{index}].options"),
                        id: option.clone(),
                    });
                }
            }
        }
    }
    Ok(declarations)
}

fn validate_template_references(
    document: &StrategyConfig,
    declarations: &BTreeMap<&str, &ParameterConfig>,
    library: &MaterialLibrary,
) -> Result<(), CompileError> {
    for (index, material) in document.materials.iter().enumerate() {
        let schema = library.parameter_schema(&material.key).ok_or_else(|| {
            CompileError::UnknownMaterialKey {
                path: format!("materials[{index}].key"),
                key: material.key.clone(),
            }
        })?;
        validate_material_template_args(
            &material.params,
            schema,
            declarations,
            &format!("materials[{index}].params"),
        )?;
        for (input_index, input) in material.inputs.iter().enumerate() {
            validate_expr_template(
                input,
                declarations,
                &format!("materials[{index}].inputs[{input_index}]"),
            )?;
        }
    }
    for (state_index, state) in document.states.iter().enumerate() {
        for (transition_index, transition) in state.transitions.iter().enumerate() {
            let path = format!("states[{state_index}].transitions[{transition_index}]");
            validate_expr_template(&transition.when, declarations, &format!("{path}.when"))?;
            for (index, assignment) in transition.assignments.iter().enumerate() {
                validate_expr_template(
                    &assignment.value,
                    declarations,
                    &format!("{path}.assignments[{index}].value"),
                )?;
            }
            if let Some(decision) = &transition.decision {
                for (index, value) in decision.values.iter().enumerate() {
                    validate_expr_template(
                        &value.value,
                        declarations,
                        &format!("{path}.decision.values[{index}]"),
                    )?;
                }
            }
            for (index, action) in transition.actions.iter().enumerate() {
                visit_action_exprs(action, |expr, suffix| {
                    validate_expr_template(
                        expr,
                        declarations,
                        &format!("{path}.actions[{index}].{suffix}"),
                    )
                })?;
            }
            for (note_index, note) in transition.notes.iter().enumerate() {
                for (value_index, value) in note.values.iter().enumerate() {
                    validate_expr_template(
                        &value.value,
                        declarations,
                        &format!("{path}.notes[{note_index}].values[{value_index}]"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_expr_template(
    expr: &Expr,
    declarations: &BTreeMap<&str, &ParameterConfig>,
    path: &str,
) -> Result<(), CompileError> {
    match expr {
        Expr::Param { id } => {
            if !declarations.contains_key(id.as_str()) {
                return Err(CompileError::UnknownReference {
                    path: path.into(),
                    reference: id.clone(),
                });
            }
        }
        Expr::Select { param, cases } => {
            let declaration =
                declarations
                    .get(param.as_str())
                    .ok_or_else(|| CompileError::UnknownReference {
                        path: format!("{path}.param"),
                        reference: param.clone(),
                    })?;
            let ParameterKind::Choice { options } = &declaration.kind else {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}.param"),
                    reason: "select requires a choice parameter".into(),
                });
            };
            let expected: BTreeSet<_> = options.iter().map(String::as_str).collect();
            let actual: BTreeSet<_> = cases.keys().map(String::as_str).collect();
            if expected != actual {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}.cases"),
                    reason: "select cases must exactly match the declared choice options".into(),
                });
            }
            let mut known_types = cases
                .values()
                .filter_map(|case| template_type_hint(case, declarations));
            let first_type = known_types.next();
            if first_type.is_some_and(|first| known_types.any(|value| value != first)) {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}.cases"),
                    reason: "select cases must have the same expression type".into(),
                });
            }
            for (name, case) in cases {
                validate_expr_template(case, declarations, &format!("{path}.cases.{name}"))?;
            }
        }
        Expr::Not { value }
        | Expr::Abs { value }
        | Expr::IsPresent { value }
        | Expr::IsMissing { value } => validate_expr_template(value, declarations, path)?,
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
            validate_expr_template(left, declarations, path)?;
            validate_expr_template(right, declarations, path)?;
        }
        Expr::All { items } | Expr::Any { items } => {
            for item in items {
                validate_expr_template(item, declarations, path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn template_type_hint(
    expr: &Expr,
    declarations: &BTreeMap<&str, &ParameterConfig>,
) -> Option<crate::ValueType> {
    use crate::{ScalarType, ValueType};
    match expr {
        Expr::Literal { value } => Some(value.value_type()),
        Expr::Param { id } => declarations
            .get(id.as_str())
            .map(|parameter| ValueType::required(parameter_scalar_type(&parameter.kind))),
        Expr::Input { value_type, .. } => Some(*value_type),
        Expr::Bar { field, .. } => Some(crate::bar_field_type(*field)),
        Expr::Position { field, .. } => Some(crate::material::position_field_type(*field)),
        Expr::Feedback { .. } | Expr::Readiness => Some(ValueType::required(ScalarType::Bool)),
        Expr::InputTime => Some(ValueType::required(ScalarType::Timestamp)),
        Expr::Eq { .. }
        | Expr::Ne { .. }
        | Expr::Lt { .. }
        | Expr::Le { .. }
        | Expr::Gt { .. }
        | Expr::Ge { .. }
        | Expr::All { .. }
        | Expr::Any { .. }
        | Expr::Not { .. }
        | Expr::IsPresent { .. }
        | Expr::IsMissing { .. } => Some(ValueType::required(ScalarType::Bool)),
        Expr::Abs { value } => template_type_hint(value, declarations),
        Expr::Min { left, right } | Expr::Max { left, right } => {
            let left = template_type_hint(left, declarations)?;
            let right = template_type_hint(right, declarations)?;
            (left.scalar == right.scalar).then_some(ValueType {
                scalar: left.scalar,
                optional: left.optional || right.optional,
            })
        }
        Expr::Add { left, right }
        | Expr::Sub { left, right }
        | Expr::Mul { left, right }
        | Expr::Div { left, right } => {
            let left = template_type_hint(left, declarations)?;
            let right = template_type_hint(right, declarations)?;
            let scalar = template_arithmetic_type(expr, left.scalar, right.scalar)?;
            Some(ValueType {
                scalar,
                optional: left.optional || right.optional,
            })
        }
        Expr::Select { cases, .. } => {
            let mut values = cases
                .values()
                .filter_map(|case| template_type_hint(case, declarations));
            let first = values.next()?;
            values.all(|value| value == first).then_some(first)
        }
        Expr::Material { .. } | Expr::Variable { .. } => None,
    }
}

fn template_arithmetic_type(
    expr: &Expr,
    left: ScalarType,
    right: ScalarType,
) -> Option<ScalarType> {
    use ScalarType::*;
    let add = matches!(expr, Expr::Add { .. });
    let sub = matches!(expr, Expr::Sub { .. });
    let mul = matches!(expr, Expr::Mul { .. });
    let div = matches!(expr, Expr::Div { .. });
    match (left, right) {
        (Integer, Integer) if add || sub || mul || div => Some(Integer),
        (Number, Number) if add || sub || mul || div => Some(Number),
        (Price, Price) if add || sub => Some(Price),
        (Price, Number) if mul || div => Some(Price),
        (Number, Price) if mul => Some(Price),
        (Price, Price) if div => Some(Number),
        (Duration, Duration) if add || sub => Some(Duration),
        (Timestamp, Duration) if add || sub => Some(Timestamp),
        (Duration, Timestamp) if add => Some(Timestamp),
        (Timestamp, Timestamp) if sub => Some(Duration),
        _ => None,
    }
}

fn validate_material_template_args(
    args: &MaterialArgs,
    schema: &[crate::ParamSpec],
    declarations: &BTreeMap<&str, &ParameterConfig>,
    path: &str,
) -> Result<(), CompileError> {
    if args.len() > crate::MAX_MATERIAL_ARGS {
        return Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual: args.len(),
            limit: crate::MAX_MATERIAL_ARGS,
        });
    }
    for (name, arg) in args.iter() {
        let spec = schema
            .iter()
            .find(|spec| spec.name == name)
            .ok_or_else(|| CompileError::InvalidConfig {
                path: format!("{path}.{name}"),
                reason: "material argument is not declared by the factory".into(),
            })?;
        if let MaterialArg::Param(id) = arg {
            let declaration =
                declarations
                    .get(id.as_str())
                    .ok_or_else(|| CompileError::UnknownReference {
                        path: format!("{path}.{name}"),
                        reference: id.clone(),
                    })?;
            if !parameter_matches_material_kind(&declaration.kind, spec.kind) {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}.{name}"),
                    reason: "parameter kind does not match the material factory schema".into(),
                });
            }
        } else if !literal_matches_material_kind(arg, spec.kind) {
            return Err(CompileError::InvalidConfig {
                path: format!("{path}.{name}"),
                reason: "material argument does not match the material factory schema".into(),
            });
        }
    }
    for spec in schema {
        if spec.required && args.get(spec.name).is_none() {
            return Err(CompileError::InvalidConfig {
                path: format!("{path}.{}", spec.name),
                reason: "required material argument is missing".into(),
            });
        }
    }
    Ok(())
}

fn parameter_matches_material_kind(parameter: &ParameterKind, material: ParamKind) -> bool {
    matches!(
        (parameter, material),
        (ParameterKind::Integer, ParamKind::Integer { .. })
            | (ParameterKind::Number, ParamKind::Number { .. })
            | (
                ParameterKind::Choice { .. },
                ParamKind::Source | ParamKind::Slot | ParamKind::BarField | ParamKind::ActionKind
            )
    )
}

fn literal_matches_material_kind(arg: &MaterialArg, kind: ParamKind) -> bool {
    match (arg, kind) {
        (MaterialArg::Integer(value), ParamKind::Integer { min, max }) => {
            *value >= min && *value <= max
        }
        (MaterialArg::Number(value), ParamKind::Number { min, max }) => {
            value.is_finite() && *value >= min && *value <= max
        }
        (MaterialArg::Source(_), ParamKind::Source)
        | (MaterialArg::Slot(_), ParamKind::Slot)
        | (MaterialArg::BarField(_), ParamKind::BarField)
        | (MaterialArg::ActionKind(_), ParamKind::ActionKind) => true,
        _ => false,
    }
}

fn representative_binding(
    parameters: &[ParameterConfig],
) -> Result<ParameterBinding, CompileError> {
    let mut values = BTreeMap::new();
    for parameter in parameters {
        let value = match &parameter.kind {
            ParameterKind::Integer => ParameterValue::Integer(1),
            ParameterKind::Number => ParameterValue::Number(1.0),
            ParameterKind::Choice { options } => ParameterValue::Choice(
                options
                    .first()
                    .ok_or_else(|| CompileError::InvalidConfig {
                        path: format!("parameters.{}", parameter.id),
                        reason: "choice parameter has no options".into(),
                    })?
                    .clone(),
            ),
        };
        values.insert(parameter.id.clone(), value);
    }
    Ok(ParameterBinding(values))
}

fn validate_binding(
    parameters: &[ParameterConfig],
    binding: &ParameterBinding,
) -> Result<(), CompileError> {
    if parameters.len() != binding.0.len() {
        return Err(CompileError::InvalidConfig {
            path: "binding".into(),
            reason: "every declared parameter must be bound exactly once".into(),
        });
    }
    for parameter in parameters {
        let value = binding
            .get(&parameter.id)
            .ok_or_else(|| CompileError::InvalidConfig {
                path: format!("binding.{}", parameter.id),
                reason: "declared parameter is not bound".into(),
            })?;
        let valid = match (&parameter.kind, value) {
            (ParameterKind::Integer, ParameterValue::Integer(_)) => true,
            (ParameterKind::Number, ParameterValue::Number(value)) => value.is_finite(),
            (ParameterKind::Choice { options }, ParameterValue::Choice(value)) => {
                options.contains(value)
            }
            _ => false,
        };
        if !valid {
            return Err(CompileError::InvalidConfig {
                path: format!("binding.{}", parameter.id),
                reason: "bound value does not match the parameter declaration".into(),
            });
        }
    }
    for name in binding.0.keys() {
        if !parameters.iter().any(|parameter| parameter.id == *name) {
            return Err(CompileError::UnknownReference {
                path: "binding".into(),
                reference: name.clone(),
            });
        }
    }
    Ok(())
}

fn bind_material_args(
    args: &MaterialArgs,
    schema: &[crate::ParamSpec],
    declarations: &BTreeMap<&str, &ParameterConfig>,
    binding: &ParameterBinding,
    path: &str,
) -> Result<MaterialArgs, CompileError> {
    let mut bound = BTreeMap::new();
    for (name, arg) in args.iter() {
        let value = match arg {
            MaterialArg::Param(id) => {
                let declaration = declarations[id.as_str()];
                let spec = schema
                    .iter()
                    .find(|spec| spec.name == name)
                    .ok_or_else(|| CompileError::InvalidConfig {
                        path: format!("{path}.{name}"),
                        reason: "material argument is not declared by the factory".into(),
                    })?;
                bind_material_parameter(
                    binding
                        .get(id)
                        .ok_or_else(|| CompileError::UnknownReference {
                            path: format!("{path}.{name}"),
                            reference: id.clone(),
                        })?,
                    &declaration.kind,
                    spec.kind,
                    &format!("{path}.{name}"),
                )?
            }
            value => value.clone(),
        };
        bound.insert(name.clone(), value);
    }
    Ok(MaterialArgs(bound))
}

fn bind_material_parameter(
    value: &ParameterValue,
    declaration: &ParameterKind,
    kind: ParamKind,
    path: &str,
) -> Result<MaterialArg, CompileError> {
    let invalid = || CompileError::InvalidConfig {
        path: path.into(),
        reason: "bound parameter cannot be converted to the material argument kind".into(),
    };
    match (declaration, value, kind) {
        (ParameterKind::Integer, ParameterValue::Integer(value), ParamKind::Integer { .. }) => {
            Ok(MaterialArg::Integer(*value))
        }
        (ParameterKind::Number, ParameterValue::Number(value), ParamKind::Number { .. }) => {
            Ok(MaterialArg::Number(*value))
        }
        (ParameterKind::Choice { .. }, ParameterValue::Choice(value), ParamKind::Source) => {
            SourceIdFromChoice::source(value)
                .map(MaterialArg::Source)
                .map_err(|_| invalid())
        }
        (ParameterKind::Choice { .. }, ParameterValue::Choice(value), ParamKind::Slot) => {
            crate::validate_id(value).map_err(|_| invalid())?;
            Ok(MaterialArg::Slot(value.clone()))
        }
        (ParameterKind::Choice { .. }, ParameterValue::Choice(value), ParamKind::BarField) => {
            parse_bar_field(value)
                .map(MaterialArg::BarField)
                .ok_or_else(invalid)
        }
        (ParameterKind::Choice { .. }, ParameterValue::Choice(value), ParamKind::ActionKind) => {
            parse_action_kind(value)
                .map(MaterialArg::ActionKind)
                .ok_or_else(invalid)
        }
        _ => Err(invalid()),
    }
}

struct SourceIdFromChoice;
impl SourceIdFromChoice {
    fn source(value: &str) -> Result<crate::SourceId, String> {
        crate::SourceId::new(value)
    }
}

fn parse_bar_field(value: &str) -> Option<BarField> {
    match value {
        "open" => Some(BarField::Open),
        "high" => Some(BarField::High),
        "low" => Some(BarField::Low),
        "close" => Some(BarField::Close),
        "volume" => Some(BarField::Volume),
        _ => None,
    }
}

fn parse_action_kind(value: &str) -> Option<ConfiguredActionKind> {
    match value {
        "entry" => Some(ConfiguredActionKind::Entry),
        "close" => Some(ConfiguredActionKind::Close),
        "close_partial" => Some(ConfiguredActionKind::ClosePartial),
        "move_stoploss_to_entry" => Some(ConfiguredActionKind::MoveStoplossToEntry),
        "modify_stoploss" => Some(ConfiguredActionKind::ModifyStoploss),
        "cancel_pending" => Some(ConfiguredActionKind::CancelPending),
        _ => None,
    }
}

pub fn bind_expr(
    expr: &Expr,
    binding: &ParameterBinding,
    path: &str,
) -> Result<Expr, CompileError> {
    let child = |value: &Expr, suffix: &str| bind_expr(value, binding, &format!("{path}.{suffix}"));
    Ok(match expr {
        Expr::Param { id } => Expr::Literal {
            value: parameter_literal(binding.get(id).ok_or_else(|| {
                CompileError::UnknownReference {
                    path: path.into(),
                    reference: id.clone(),
                }
            })?),
        },
        Expr::Select { param, cases } => {
            let ParameterValue::Choice(selected) =
                binding
                    .get(param)
                    .ok_or_else(|| CompileError::UnknownReference {
                        path: format!("{path}.param"),
                        reference: param.clone(),
                    })?
            else {
                return Err(CompileError::InvalidConfig {
                    path: format!("{path}.param"),
                    reason: "select requires a bound choice parameter".into(),
                });
            };
            let case = cases
                .get(selected)
                .ok_or_else(|| CompileError::InvalidConfig {
                    path: format!("{path}.cases"),
                    reason: format!("select has no case for '{selected}'"),
                })?;
            bind_expr(case, binding, &format!("{path}.cases.{selected}"))?
        }
        Expr::Not { value } => Expr::Not {
            value: Box::new(child(value, "value")?),
        },
        Expr::Abs { value } => Expr::Abs {
            value: Box::new(child(value, "value")?),
        },
        Expr::IsPresent { value } => Expr::IsPresent {
            value: Box::new(child(value, "value")?),
        },
        Expr::IsMissing { value } => Expr::IsMissing {
            value: Box::new(child(value, "value")?),
        },
        Expr::Eq { left, right } => {
            binary(|left, right| Expr::Eq { left, right }, left, right, &child)?
        }
        Expr::Ne { left, right } => {
            binary(|left, right| Expr::Ne { left, right }, left, right, &child)?
        }
        Expr::Lt { left, right } => {
            binary(|left, right| Expr::Lt { left, right }, left, right, &child)?
        }
        Expr::Le { left, right } => {
            binary(|left, right| Expr::Le { left, right }, left, right, &child)?
        }
        Expr::Gt { left, right } => {
            binary(|left, right| Expr::Gt { left, right }, left, right, &child)?
        }
        Expr::Ge { left, right } => {
            binary(|left, right| Expr::Ge { left, right }, left, right, &child)?
        }
        Expr::Add { left, right } => {
            binary(|left, right| Expr::Add { left, right }, left, right, &child)?
        }
        Expr::Sub { left, right } => {
            binary(|left, right| Expr::Sub { left, right }, left, right, &child)?
        }
        Expr::Mul { left, right } => {
            binary(|left, right| Expr::Mul { left, right }, left, right, &child)?
        }
        Expr::Div { left, right } => {
            binary(|left, right| Expr::Div { left, right }, left, right, &child)?
        }
        Expr::Min { left, right } => {
            binary(|left, right| Expr::Min { left, right }, left, right, &child)?
        }
        Expr::Max { left, right } => {
            binary(|left, right| Expr::Max { left, right }, left, right, &child)?
        }
        Expr::All { items } => Expr::All {
            items: items
                .iter()
                .enumerate()
                .map(|(index, item)| child(item, &format!("items[{index}]")))
                .collect::<Result<_, _>>()?,
        },
        Expr::Any { items } => Expr::Any {
            items: items
                .iter()
                .enumerate()
                .map(|(index, item)| child(item, &format!("items[{index}]")))
                .collect::<Result<_, _>>()?,
        },
        value => value.clone(),
    })
}

fn binary<F>(
    constructor: F,
    left: &Expr,
    right: &Expr,
    child: &impl Fn(&Expr, &str) -> Result<Expr, CompileError>,
) -> Result<Expr, CompileError>
where
    F: FnOnce(Box<Expr>, Box<Expr>) -> Expr,
{
    Ok(constructor(
        Box::new(child(left, "left")?),
        Box::new(child(right, "right")?),
    ))
}

fn parameter_literal(value: &ParameterValue) -> crate::Literal {
    match value {
        ParameterValue::Integer(value) => crate::Literal::Integer(*value),
        ParameterValue::Number(value) => crate::Literal::Number(*value),
        ParameterValue::Choice(value) => crate::Literal::Text(value.clone()),
    }
}

fn bind_action(
    action: &mut ActionTemplate,
    binding: &ParameterBinding,
    path: &str,
) -> Result<(), CompileError> {
    visit_action_exprs_mut(action, |expr, suffix| {
        *expr = bind_expr(expr, binding, &format!("{path}.{suffix}"))?;
        Ok(())
    })
}

fn visit_action_exprs(
    action: &ActionTemplate,
    mut visitor: impl FnMut(&Expr, &str) -> Result<(), CompileError>,
) -> Result<(), CompileError> {
    match action {
        ActionTemplate::Entry {
            side,
            price,
            risk,
            stoploss,
            targets,
            ..
        } => {
            visitor(side, "side")?;
            visitor(price, "price")?;
            visitor(risk, "risk")?;
            visitor(stoploss, "stoploss")?;
            for (index, target) in targets.iter().enumerate() {
                visitor(target, &format!("targets[{index}]"))?;
            }
        }
        ActionTemplate::ClosePartial { ratio, .. } => visitor(ratio, "ratio")?,
        ActionTemplate::ModifyStoploss { price, .. } => visitor(price, "price")?,
        _ => {}
    }
    Ok(())
}

fn visit_action_exprs_mut(
    action: &mut ActionTemplate,
    mut visitor: impl FnMut(&mut Expr, &str) -> Result<(), CompileError>,
) -> Result<(), CompileError> {
    match action {
        ActionTemplate::Entry {
            side,
            price,
            risk,
            stoploss,
            targets,
            ..
        } => {
            visitor(side, "side")?;
            visitor(price, "price")?;
            visitor(risk, "risk")?;
            visitor(stoploss, "stoploss")?;
            for (index, target) in targets.iter_mut().enumerate() {
                visitor(target, &format!("targets[{index}]"))?;
            }
        }
        ActionTemplate::ClosePartial { ratio, .. } => visitor(ratio, "ratio")?,
        ActionTemplate::ModifyStoploss { price, .. } => visitor(price, "price")?,
        _ => {}
    }
    Ok(())
}

fn prune_materials(document: &mut StrategyConfig) -> Result<(), CompileError> {
    let state_by_id: BTreeMap<_, _> = document
        .states
        .iter()
        .enumerate()
        .map(|(index, state)| (state.id.as_str(), index))
        .collect();
    let initial = state_by_id
        .get(document.initial_state.as_str())
        .copied()
        .ok_or_else(|| CompileError::InvalidStateTarget {
            path: "initial_state".into(),
            target: document.initial_state.clone(),
        })?;
    let mut reachable_states = BTreeSet::from([initial]);
    let mut pending = vec![initial];
    while let Some(index) = pending.pop() {
        for transition in &document.states[index].transitions {
            let target = state_by_id
                .get(transition.target.as_str())
                .copied()
                .ok_or_else(|| CompileError::InvalidStateTarget {
                    path: format!("states[{index}].transitions.target"),
                    target: transition.target.clone(),
                })?;
            if reachable_states.insert(target) {
                pending.push(target);
            }
        }
    }

    let mut roots = Vec::new();
    for state_index in reachable_states {
        let state = &document.states[state_index];
        for (transition_index, transition) in state.transitions.iter().enumerate() {
            let path = format!("states[{state_index}].transitions[{transition_index}]");
            collect_material_refs(&transition.when, &mut roots, &format!("{path}.when"))?;
            for assignment in &transition.assignments {
                collect_material_refs(&assignment.value, &mut roots, &path)?;
            }
            if let Some(decision) = &transition.decision {
                for value in &decision.values {
                    collect_material_refs(&value.value, &mut roots, &path)?;
                }
            }
            for action in &transition.actions {
                visit_action_exprs(action, |expr, suffix| {
                    collect_material_refs(expr, &mut roots, &format!("{path}.{suffix}"))
                })?;
            }
            for note in &transition.notes {
                for value in &note.values {
                    collect_material_refs(&value.value, &mut roots, &path)?;
                }
            }
        }
    }

    let materials: BTreeMap<_, _> = document
        .materials
        .iter()
        .map(|material| (material.id.as_str(), material))
        .collect();
    let mut reachable = BTreeSet::new();
    let mut pending = roots;
    while let Some(id) = pending.pop() {
        if !reachable.insert(id.clone()) {
            continue;
        }
        let material =
            materials
                .get(id.as_str())
                .ok_or_else(|| CompileError::UnknownReference {
                    path: "materials".into(),
                    reference: id,
                })?;
        for input in &material.inputs {
            collect_material_refs(input, &mut pending, "materials.inputs")?;
        }
    }
    document
        .materials
        .retain(|material| reachable.contains(&material.id));
    Ok(())
}

/// Evaluate a parameter-and-literal-only expression after substituting a binding.
pub fn evaluate_parameter_expression(
    expr: &Expr,
    binding: &ParameterBinding,
) -> Result<Value, CompileError> {
    let bound = bind_expr(expr, binding, "constraint")?;
    let variables = BTreeMap::new();
    let materials = BTreeMap::new();
    let trade_slots = BTreeSet::new();
    let sources = BTreeSet::new();
    let scope = ExprScope {
        variables: &variables,
        materials: &materials,
        trade_slots: &trade_slots,
        sources: &sources,
    };
    let (compiled, _) = compile_expr(&bound, &scope, "constraint")?;
    let input = StrategyInput {
        time: NaiveDateTime::default(),
        ready: true,
        completed_bars: Vec::new(),
        values: Vec::new(),
        trade_slots: Vec::new(),
        feedback: Vec::new(),
    };
    compiled
        .eval(
            &EvalScope {
                variables: &[],
                materials: &[],
                input: &input,
                feedback: &[],
            },
            "constraint",
        )
        .map_err(|error| CompileError::InvalidConfig {
            path: "constraint".into(),
            reason: error.to_string(),
        })
}

pub fn parameter_value_label(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Number(value) => value.to_string(),
        ParameterValue::Choice(value) => value.clone(),
    }
}

pub fn require_boolean(value: Value, path: &str) -> Result<bool, CompileError> {
    match value {
        Value::Bool(value) => Ok(value),
        value => Err(CompileError::InvalidConfig {
            path: path.into(),
            reason: format!("expected boolean result, got {:?}", value.scalar_type()),
        }),
    }
}

pub fn parameter_scalar_type(kind: &ParameterKind) -> ScalarType {
    match kind {
        ParameterKind::Integer => ScalarType::Integer,
        ParameterKind::Number => ScalarType::Number,
        ParameterKind::Choice { .. } => ScalarType::Text,
    }
}
