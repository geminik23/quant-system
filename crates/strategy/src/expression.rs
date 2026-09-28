use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BarField, CompileError, ConfiguredActionKind, EvaluationError, Expr, FeedbackField,
    PositionField, ScalarType, SourceId, Value, ValueType, bar_field_type,
};

#[derive(Debug, Clone)]
pub(crate) enum CompiledExpr {
    Literal(Value),
    Variable(usize),
    Material(usize),
    Input(String, ValueType),
    Bar(SourceId, BarField),
    Position(String, PositionField),
    Feedback(String, ConfiguredActionKind, FeedbackField),
    InputTime,
    Readiness,
    Binary(BinaryOp, Box<Self>, Box<Self>, ValueType),
    List(BoolListOp, Vec<Self>),
    Not(Box<Self>),
    Strict(Box<Self>),
    Abs(Box<Self>, ValueType),
    Presence(Box<Self>, bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Min,
    Max,
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum BoolListOp {
    All,
    Any,
}

pub(crate) struct ExprScope<'a> {
    pub variables: &'a BTreeMap<String, (usize, ValueType)>,
    pub materials: &'a BTreeMap<String, (usize, ValueType)>,
    pub trade_slots: &'a BTreeSet<String>,
    pub sources: &'a BTreeSet<SourceId>,
}

pub(crate) struct EvalScope<'a> {
    pub variables: &'a [Value],
    pub materials: &'a [Value],
    pub input: &'a crate::StrategyInput,
    pub feedback: &'a [crate::material::FeedbackObservation],
}

pub(crate) fn compile_expr(
    expr: &Expr,
    scope: &ExprScope<'_>,
    path: &str,
) -> Result<(CompiledExpr, ValueType), CompileError> {
    let mut nodes = 0;
    compile_inner(expr, scope, path, 1, &mut nodes, false)
}

fn compile_inner(
    expr: &Expr,
    scope: &ExprScope<'_>,
    path: &str,
    depth: usize,
    nodes: &mut usize,
    strict: bool,
) -> Result<(CompiledExpr, ValueType), CompileError> {
    *nodes += 1;
    if *nodes > crate::MAX_EXPR_NODES {
        return Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual: *nodes,
            limit: crate::MAX_EXPR_NODES,
        });
    }
    if depth > crate::MAX_EXPR_DEPTH {
        return Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual: depth,
            limit: crate::MAX_EXPR_DEPTH,
        });
    }
    let child = |value: &Expr, suffix: &str, nodes: &mut usize| {
        compile_inner(
            value,
            scope,
            &format!("{path}.{suffix}"),
            depth + 1,
            nodes,
            strict,
        )
    };
    match expr {
        Expr::Literal { value } => {
            Ok((CompiledExpr::Literal(value.to_value()?), value.value_type()))
        }
        Expr::Variable { id } => scope
            .variables
            .get(id)
            .map(|(index, ty)| (CompiledExpr::Variable(*index), *ty))
            .ok_or_else(|| CompileError::UnknownReference {
                path: path.into(),
                reference: id.clone(),
            }),
        Expr::Material { id } => scope
            .materials
            .get(id)
            .map(|(index, ty)| (CompiledExpr::Material(*index), *ty))
            .ok_or_else(|| CompileError::UnknownReference {
                path: path.into(),
                reference: id.clone(),
            }),
        Expr::Param { .. } | Expr::Select { .. } => Err(CompileError::InvalidConfig {
            path: path.into(),
            reason: "strategy parameter expressions must be bound before compilation".into(),
        }),
        Expr::Input { field, value_type } => {
            crate::validate_id(field).map_err(|reason| CompileError::InvalidIdentifier {
                path: format!("{path}.field"),
                reason,
            })?;
            Ok((CompiledExpr::Input(field.clone(), *value_type), *value_type))
        }
        Expr::Bar { source, field } => {
            if !scope.sources.contains(source) {
                return Err(CompileError::UnknownReference {
                    path: format!("{path}.source"),
                    reference: source.to_string(),
                });
            }
            Ok((
                CompiledExpr::Bar(source.clone(), *field),
                bar_field_type(*field),
            ))
        }
        Expr::Position { slot, field } => {
            if !scope.trade_slots.contains(slot) {
                return Err(CompileError::UnknownReference {
                    path: format!("{path}.slot"),
                    reference: slot.clone(),
                });
            }
            let ty = crate::material::position_field_type(*field);
            Ok((CompiledExpr::Position(slot.clone(), *field), ty))
        }
        Expr::Feedback {
            slot,
            action,
            field,
        } => {
            if !scope.trade_slots.contains(slot) {
                return Err(CompileError::UnknownReference {
                    path: format!("{path}.slot"),
                    reference: slot.clone(),
                });
            }
            Ok((
                CompiledExpr::Feedback(slot.clone(), *action, *field),
                ValueType::required(ScalarType::Bool),
            ))
        }
        Expr::InputTime => Ok((
            CompiledExpr::InputTime,
            ValueType::required(ScalarType::Timestamp),
        )),
        Expr::Readiness => Ok((
            CompiledExpr::Readiness,
            ValueType::required(ScalarType::Bool),
        )),
        Expr::Eq { left, right } => {
            binary_compare(BinaryOp::Eq, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Ne { left, right } => {
            binary_compare(BinaryOp::Ne, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Lt { left, right } => {
            binary_compare(BinaryOp::Lt, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Le { left, right } => {
            binary_compare(BinaryOp::Le, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Gt { left, right } => {
            binary_compare(BinaryOp::Gt, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Ge { left, right } => {
            binary_compare(BinaryOp::Ge, left, right, scope, path, depth, nodes, strict)
        }
        Expr::Add { left, right } => binary_arithmetic(
            BinaryOp::Add,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::Sub { left, right } => binary_arithmetic(
            BinaryOp::Sub,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::Mul { left, right } => binary_arithmetic(
            BinaryOp::Mul,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::Div { left, right } => binary_arithmetic(
            BinaryOp::Div,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::Min { left, right } => binary_same_numeric(
            BinaryOp::Min,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::Max { left, right } => binary_same_numeric(
            BinaryOp::Max,
            left,
            right,
            scope,
            path,
            depth,
            nodes,
            strict,
        ),
        Expr::All { items } | Expr::Any { items } => {
            if items.is_empty() {
                return Err(CompileError::InvalidConfig {
                    path: path.into(),
                    reason: "boolean list must not be empty".into(),
                });
            }
            let mut compiled = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let (value, ty) = compile_inner(
                    item,
                    scope,
                    &format!("{path}.items[{index}]"),
                    depth + 1,
                    nodes,
                    strict,
                )?;
                expect(
                    ty,
                    ValueType {
                        scalar: ScalarType::Bool,
                        optional: strict,
                    },
                    path,
                )?;
                compiled.push(value);
            }
            let op = if matches!(expr, Expr::All { .. }) {
                BoolListOp::All
            } else {
                BoolListOp::Any
            };
            Ok((
                CompiledExpr::List(op, compiled),
                ValueType {
                    scalar: ScalarType::Bool,
                    optional: strict,
                },
            ))
        }
        Expr::Not { value } => {
            let (value, ty) = child(value, "value", nodes)?;
            expect(
                ty,
                ValueType {
                    scalar: ScalarType::Bool,
                    optional: strict,
                },
                path,
            )?;
            Ok((
                CompiledExpr::Not(Box::new(value)),
                ValueType {
                    scalar: ScalarType::Bool,
                    optional: strict,
                },
            ))
        }
        Expr::Strict { value } => {
            let (value, ty) = compile_inner(
                value,
                scope,
                &format!("{path}.value"),
                depth + 1,
                nodes,
                true,
            )?;
            if ty.scalar != ScalarType::Bool {
                return mismatch(path, ValueType::optional(ScalarType::Bool), ty);
            }
            Ok((
                CompiledExpr::Strict(Box::new(value)),
                ValueType::optional(ScalarType::Bool),
            ))
        }
        Expr::Abs { value } => {
            let (value, ty) = child(value, "value", nodes)?;
            if !matches!(
                ty.scalar,
                ScalarType::Integer
                    | ScalarType::Number
                    | ScalarType::Price
                    | ScalarType::Ratio
                    | ScalarType::Percent
                    | ScalarType::PricePerObservation
                    | ScalarType::PricePerObservationSquared
                    | ScalarType::RatioPerObservation
                    | ScalarType::RatioPerObservationSquared
                    | ScalarType::LogReturn
                    | ScalarType::LogReturnVariance
                    | ScalarType::Duration
            ) {
                return mismatch(path, ValueType::required(ScalarType::Number), ty);
            }
            Ok((CompiledExpr::Abs(Box::new(value), ty), ty))
        }
        Expr::IsPresent { value } | Expr::IsMissing { value } => {
            let (value, _) = child(value, "value", nodes)?;
            Ok((
                CompiledExpr::Presence(Box::new(value), matches!(expr, Expr::IsPresent { .. })),
                ValueType::required(ScalarType::Bool),
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn binary_compare(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &ExprScope<'_>,
    path: &str,
    depth: usize,
    nodes: &mut usize,
    strict: bool,
) -> Result<(CompiledExpr, ValueType), CompileError> {
    let (left, lt) = compile_inner(
        left,
        scope,
        &format!("{path}.left"),
        depth + 1,
        nodes,
        strict,
    )?;
    let (right, rt) = compile_inner(
        right,
        scope,
        &format!("{path}.right"),
        depth + 1,
        nodes,
        strict,
    )?;
    if lt.scalar != rt.scalar {
        return mismatch(path, lt, rt);
    }
    if !strict && (left.contains_strict() || right.contains_strict()) {
        return Err(CompileError::InvalidConfig {
            path: path.into(),
            reason: "a strict predicate cannot be compared by a legacy comparison; put the entire predicate under strict".into(),
        });
    }
    if matches!(
        op,
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) && matches!(lt.scalar, ScalarType::Bool | ScalarType::Side)
    {
        return Err(CompileError::InvalidConfig {
            path: path.into(),
            reason: "type is not ordered".into(),
        });
    }
    let ty = ValueType {
        scalar: ScalarType::Bool,
        optional: strict,
    };
    Ok((
        CompiledExpr::Binary(op, Box::new(left), Box::new(right), ty),
        ty,
    ))
}

#[allow(clippy::too_many_arguments)]
fn binary_same_numeric(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &ExprScope<'_>,
    path: &str,
    depth: usize,
    nodes: &mut usize,
    strict: bool,
) -> Result<(CompiledExpr, ValueType), CompileError> {
    let (left, lt) = compile_inner(
        left,
        scope,
        &format!("{path}.left"),
        depth + 1,
        nodes,
        strict,
    )?;
    let (right, rt) = compile_inner(
        right,
        scope,
        &format!("{path}.right"),
        depth + 1,
        nodes,
        strict,
    )?;
    if lt.scalar != rt.scalar
        || !matches!(
            lt.scalar,
            ScalarType::Integer
                | ScalarType::Number
                | ScalarType::Price
                | ScalarType::Ratio
                | ScalarType::Percent
                | ScalarType::PricePerObservation
                | ScalarType::PricePerObservationSquared
                | ScalarType::LogReturn
                | ScalarType::LogReturnVariance
                | ScalarType::Duration
        )
    {
        return mismatch(path, lt, rt);
    }
    let ty = ValueType {
        scalar: lt.scalar,
        optional: lt.optional || rt.optional,
    };
    Ok((
        CompiledExpr::Binary(op, Box::new(left), Box::new(right), ty),
        ty,
    ))
}

#[allow(clippy::too_many_arguments)]
fn binary_arithmetic(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    scope: &ExprScope<'_>,
    path: &str,
    depth: usize,
    nodes: &mut usize,
    strict: bool,
) -> Result<(CompiledExpr, ValueType), CompileError> {
    let (left, lt) = compile_inner(
        left,
        scope,
        &format!("{path}.left"),
        depth + 1,
        nodes,
        strict,
    )?;
    let (right, rt) = compile_inner(
        right,
        scope,
        &format!("{path}.right"),
        depth + 1,
        nodes,
        strict,
    )?;
    let scalar =
        arithmetic_type(op, lt.scalar, rt.scalar).ok_or_else(|| CompileError::InvalidConfig {
            path: path.into(),
            reason: format!(
                "invalid arithmetic operands {:?} and {:?}",
                lt.scalar, rt.scalar
            ),
        })?;
    let ty = ValueType {
        scalar,
        optional: lt.optional || rt.optional,
    };
    Ok((
        CompiledExpr::Binary(op, Box::new(left), Box::new(right), ty),
        ty,
    ))
}

fn arithmetic_type(op: BinaryOp, left: ScalarType, right: ScalarType) -> Option<ScalarType> {
    use ScalarType::*;
    match (op, left, right) {
        (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div, Integer, Integer) => {
            Some(Integer)
        }
        (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div, Number, Number) => {
            Some(Number)
        }
        (BinaryOp::Add | BinaryOp::Sub, Price, Price) => Some(Price),
        (BinaryOp::Add | BinaryOp::Sub, Ratio, Ratio) => Some(Ratio),
        (BinaryOp::Add | BinaryOp::Sub, Percent, Percent) => Some(Percent),
        (BinaryOp::Add | BinaryOp::Sub, PricePerObservation, PricePerObservation) => {
            Some(PricePerObservation)
        }
        (BinaryOp::Add | BinaryOp::Sub, PricePerObservationSquared, PricePerObservationSquared) => {
            Some(PricePerObservationSquared)
        }
        (BinaryOp::Add | BinaryOp::Sub, RatioPerObservation, RatioPerObservation) => {
            Some(RatioPerObservation)
        }
        (BinaryOp::Add | BinaryOp::Sub, RatioPerObservationSquared, RatioPerObservationSquared) => {
            Some(RatioPerObservationSquared)
        }
        (BinaryOp::Add | BinaryOp::Sub, LogReturn, LogReturn) => Some(LogReturn),
        (BinaryOp::Add | BinaryOp::Sub, LogReturnVariance, LogReturnVariance) => {
            Some(LogReturnVariance)
        }
        (BinaryOp::Mul, Price, Number)
        | (BinaryOp::Mul, Number, Price)
        | (BinaryOp::Div, Price, Number) => Some(Price),
        (BinaryOp::Mul, Ratio, Number)
        | (BinaryOp::Mul, Number, Ratio)
        | (BinaryOp::Div, Ratio, Number) => Some(Ratio),
        (BinaryOp::Mul, Percent, Number)
        | (BinaryOp::Mul, Number, Percent)
        | (BinaryOp::Div, Percent, Number) => Some(Percent),
        (BinaryOp::Mul, PricePerObservation, Number)
        | (BinaryOp::Mul, Number, PricePerObservation)
        | (BinaryOp::Div, PricePerObservation, Number) => Some(PricePerObservation),
        (BinaryOp::Mul, PricePerObservationSquared, Number)
        | (BinaryOp::Mul, Number, PricePerObservationSquared)
        | (BinaryOp::Div, PricePerObservationSquared, Number) => Some(PricePerObservationSquared),
        (BinaryOp::Mul, RatioPerObservation, Number)
        | (BinaryOp::Mul, Number, RatioPerObservation)
        | (BinaryOp::Div, RatioPerObservation, Number) => Some(RatioPerObservation),
        (BinaryOp::Mul, RatioPerObservationSquared, Number)
        | (BinaryOp::Mul, Number, RatioPerObservationSquared)
        | (BinaryOp::Div, RatioPerObservationSquared, Number) => Some(RatioPerObservationSquared),
        (BinaryOp::Mul, LogReturn, Number)
        | (BinaryOp::Mul, Number, LogReturn)
        | (BinaryOp::Div, LogReturn, Number) => Some(LogReturn),
        (BinaryOp::Mul, LogReturnVariance, Number)
        | (BinaryOp::Mul, Number, LogReturnVariance)
        | (BinaryOp::Div, LogReturnVariance, Number) => Some(LogReturnVariance),
        (BinaryOp::Div, Price, Price) => Some(Number),
        (BinaryOp::Add | BinaryOp::Sub, Duration, Duration) => Some(Duration),
        (BinaryOp::Add, Timestamp, Duration)
        | (BinaryOp::Add, Duration, Timestamp)
        | (BinaryOp::Sub, Timestamp, Duration) => Some(Timestamp),
        (BinaryOp::Sub, Timestamp, Timestamp) => Some(Duration),
        _ => None,
    }
}

fn expect(actual: ValueType, expected: ValueType, path: &str) -> Result<(), CompileError> {
    if actual.scalar != expected.scalar {
        return Err(CompileError::TypeMismatch {
            path: path.into(),
            expected,
            actual,
        });
    }
    if !expected.optional && actual.optional {
        return Err(CompileError::OptionalToRequired { path: path.into() });
    }
    Ok(())
}
fn mismatch<T>(path: &str, expected: ValueType, actual: ValueType) -> Result<T, CompileError> {
    Err(CompileError::TypeMismatch {
        path: path.into(),
        expected,
        actual,
    })
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompiledInputProvenance {
    pub material_indexes: BTreeSet<usize>,
    pub named_inputs: BTreeSet<String>,
    pub sources: BTreeSet<SourceId>,
    pub dynamic: bool,
}

impl CompiledExpr {
    fn contains_strict(&self) -> bool {
        match self {
            Self::Strict(_) => true,
            Self::Binary(_, left, right, _) => left.contains_strict() || right.contains_strict(),
            Self::List(_, items) => items.iter().any(Self::contains_strict),
            Self::Not(value) | Self::Abs(value, _) => value.contains_strict(),
            // Explicit presence diagnostics consume optionality instead of coercing it.
            Self::Presence(_, _) => false,
            _ => false,
        }
    }

    pub(crate) fn provenance(&self) -> CompiledInputProvenance {
        let mut provenance = CompiledInputProvenance::default();
        self.collect_provenance(&mut provenance);
        provenance
    }

    fn collect_provenance(&self, provenance: &mut CompiledInputProvenance) {
        match self {
            Self::Material(index) => {
                provenance.material_indexes.insert(*index);
            }
            Self::Input(name, _) => {
                provenance.named_inputs.insert(name.clone());
            }
            Self::Bar(source, _) => {
                provenance.sources.insert(source.clone());
            }
            Self::Not(value)
            | Self::Strict(value)
            | Self::Abs(value, _)
            | Self::Presence(value, _) => {
                value.collect_provenance(provenance);
            }
            Self::Binary(_, left, right, _) => {
                left.collect_provenance(provenance);
                right.collect_provenance(provenance);
            }
            Self::List(_, items) => {
                for item in items {
                    item.collect_provenance(provenance);
                }
            }
            Self::Variable(_)
            | Self::Position(_, _)
            | Self::Feedback(_, _, _)
            | Self::InputTime
            | Self::Readiness => provenance.dynamic = true,
            Self::Literal(_) => {}
        }
    }

    pub(crate) fn eval(&self, scope: &EvalScope<'_>, path: &str) -> Result<Value, EvaluationError> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Variable(index) => Ok(scope.variables[*index].clone()),
            Self::Material(index) => Ok(scope.materials[*index].clone()),
            Self::Input(field, ty) => {
                let value = scope
                    .input
                    .values
                    .iter()
                    .find(|value| value.name == *field)
                    .map(|value| value.value.clone())
                    .unwrap_or(Value::Missing(ty.scalar));
                validate_input_value(value, *ty, path)
            }
            Self::Bar(source, field) => Ok(scope
                .input
                .completed_bars
                .iter()
                .find(|item| item.source == *source)
                .map(|item| crate::material::bar_value(&item.bar, *field))
                .unwrap_or(Value::Missing(bar_field_type(*field).scalar))),
            Self::Position(slot, field) => eval_position(scope.input, slot, *field),
            Self::Feedback(slot, action, field) => {
                Ok(Value::Bool(scope.feedback.iter().any(|item| {
                    item.slot == *slot && item.action == *action && item.field == *field
                })))
            }
            Self::InputTime => Ok(Value::Timestamp(scope.input.time)),
            Self::Readiness => Ok(Value::Bool(scope.input.ready)),
            Self::Binary(op, left, right, ty) => eval_binary(
                *op,
                left.eval(scope, path)?,
                right.eval(scope, path)?,
                *ty,
                path,
            ),
            Self::List(op, items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(bool_value(item.eval(scope, path)?, path)?);
                }
                Ok(Value::Bool(match op {
                    BoolListOp::All => values.into_iter().all(|value| value),
                    BoolListOp::Any => values.into_iter().any(|value| value),
                }))
            }
            Self::Not(value) => Ok(Value::Bool(!bool_value(value.eval(scope, path)?, path)?)),
            Self::Strict(value) => value.eval_strict(scope, path),
            Self::Abs(value, ty) => {
                let value = value.eval(scope, path)?;
                if value.is_missing() {
                    return Ok(Value::Missing(ty.scalar));
                }
                match value {
                    Value::Integer(value) => value
                        .checked_abs()
                        .map(Value::Integer)
                        .ok_or_else(|| EvaluationError::ArithmeticOverflow { path: path.into() }),
                    Value::Number(value) => Value::Number(value.abs()).finite(path),
                    Value::Price(value) => Value::Price(value.abs()).finite(path),
                    Value::Ratio(value) => Value::Ratio(value.abs()).finite(path),
                    Value::Percent(value) => Value::Percent(value.abs()).finite(path),
                    Value::PricePerObservation(value) => {
                        Value::PricePerObservation(value.abs()).finite(path)
                    }
                    Value::PricePerObservationSquared(value) => {
                        Value::PricePerObservationSquared(value.abs()).finite(path)
                    }
                    Value::RatioPerObservation(value) => {
                        Value::RatioPerObservation(value.abs()).finite(path)
                    }
                    Value::RatioPerObservationSquared(value) => {
                        Value::RatioPerObservationSquared(value.abs()).finite(path)
                    }
                    Value::LogReturn(value) => Value::LogReturn(value.abs()).finite(path),
                    Value::LogReturnVariance(value) => {
                        Value::LogReturnVariance(value.abs()).finite(path)
                    }
                    Value::Duration(value) => value
                        .num_milliseconds()
                        .checked_abs()
                        .map(|value| Value::Duration(chrono::Duration::milliseconds(value)))
                        .ok_or_else(|| EvaluationError::ArithmeticOverflow { path: path.into() }),
                    _ => Err(EvaluationError::TypeMismatch {
                        path: path.into(),
                        expected: ty.scalar,
                        actual: Some(value.scalar_type()),
                    }),
                }
            }
            Self::Presence(value, present) => Ok(Value::Bool(
                value.eval(scope, path)?.is_missing() != *present,
            )),
        }
    }

    fn eval_strict(&self, scope: &EvalScope<'_>, path: &str) -> Result<Value, EvaluationError> {
        match self {
            Self::Strict(value) => value.eval_strict(scope, path),
            Self::Presence(value, present) => Ok(Value::Bool(
                value.eval_strict(scope, path)?.is_missing() != *present,
            )),
            Self::Binary(op, left, right, _)
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::Ne
                        | BinaryOp::Lt
                        | BinaryOp::Le
                        | BinaryOp::Gt
                        | BinaryOp::Ge
                ) =>
            {
                let left = left.eval_strict(scope, path)?;
                let right = right.eval_strict(scope, path)?;
                if left.is_missing() || right.is_missing() {
                    Ok(Value::Missing(ScalarType::Bool))
                } else {
                    compare(*op, left, right, path)
                }
            }
            Self::Not(value) => match value.eval_strict(scope, path)? {
                Value::Bool(value) => Ok(Value::Bool(!value)),
                Value::Missing(ScalarType::Bool) => Ok(Value::Missing(ScalarType::Bool)),
                value => bool_value(value, path).map(|value| Value::Bool(!value)),
            },
            Self::List(op, items) => {
                let mut invalid = false;
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    match item.eval_strict(scope, path)? {
                        Value::Bool(value) => values.push(value),
                        Value::Missing(ScalarType::Bool) => invalid = true,
                        other => {
                            bool_value(other, path)?;
                            unreachable!("a boolean value was validated")
                        }
                    }
                }
                if invalid {
                    Ok(Value::Missing(ScalarType::Bool))
                } else {
                    Ok(Value::Bool(match op {
                        BoolListOp::All => values.into_iter().all(|value| value),
                        BoolListOp::Any => values.into_iter().any(|value| value),
                    }))
                }
            }
            _ => self.eval(scope, path),
        }
    }
}

fn validate_input_value(
    value: Value,
    expected: ValueType,
    path: &str,
) -> Result<Value, EvaluationError> {
    if value.scalar_type() != expected.scalar {
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
    if value.is_missing() && !expected.optional {
        return Err(EvaluationError::MissingRequired { path: path.into() });
    }
    match &value {
        Value::Number(number)
        | Value::Price(number)
        | Value::Ratio(number)
        | Value::Percent(number)
        | Value::PricePerObservation(number)
        | Value::PricePerObservationSquared(number)
        | Value::RatioPerObservation(number)
        | Value::RatioPerObservationSquared(number)
        | Value::LogReturn(number)
        | Value::LogReturnVariance(number)
            if !number.is_finite() =>
        {
            Err(EvaluationError::NonFinite { path: path.into() })
        }
        Value::Text(text) => {
            crate::validate_text(text, crate::MAX_TEXT_BYTES).map_err(|reason| {
                EvaluationError::Material {
                    material: "input".into(),
                    reason,
                }
            })?;
            Ok(value)
        }
        _ => Ok(value),
    }
}

fn bool_value(value: Value, path: &str) -> Result<bool, EvaluationError> {
    if let Value::Bool(value) = value {
        Ok(value)
    } else {
        Err(EvaluationError::TypeMismatch {
            path: path.into(),
            expected: ScalarType::Bool,
            actual: if value.is_missing() {
                None
            } else {
                Some(value.scalar_type())
            },
        })
    }
}
fn eval_position(
    input: &crate::StrategyInput,
    slot: &str,
    field: PositionField,
) -> Result<Value, EvaluationError> {
    let facts = input
        .trade_slots
        .iter()
        .find(|facts| facts.slot == slot)
        .ok_or_else(|| EvaluationError::MissingRequired {
            path: format!("trade_slots.{slot}"),
        })?;
    Ok(crate::material::trade_slot_value(&facts.state, field))
}

fn eval_binary(
    op: BinaryOp,
    left: Value,
    right: Value,
    ty: ValueType,
    path: &str,
) -> Result<Value, EvaluationError> {
    if matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) {
        if left.is_missing() || right.is_missing() {
            return Ok(Value::Bool(false));
        }
        return compare(op, left, right, path);
    }
    if left.is_missing() || right.is_missing() {
        return Ok(Value::Missing(ty.scalar));
    }
    use BinaryOp::*;
    if is_semantic_float(ty.scalar) {
        if left.scalar_type() == ty.scalar
            && right.scalar_type() == ty.scalar
            && matches!(op, Add | Sub)
        {
            let a = float_value(&left).unwrap();
            let b = float_value(&right).unwrap();
            return typed_float(ty.scalar, if op == Add { a + b } else { a - b }).finite(path);
        }
        let (value, factor) = if left.scalar_type() == ty.scalar
            && matches!(&right, Value::Number(_))
        {
            (float_value(&left).unwrap(), float_value(&right).unwrap())
        } else if op == Mul && right.scalar_type() == ty.scalar && matches!(&left, Value::Number(_))
        {
            (float_value(&right).unwrap(), float_value(&left).unwrap())
        } else {
            (0.0, f64::NAN)
        };
        if factor.is_finite() && matches!(op, Mul | Div) {
            if op == Div && factor == 0.0 {
                return Err(EvaluationError::DivisionByZero { path: path.into() });
            }
            return typed_float(
                ty.scalar,
                if op == Mul {
                    value * factor
                } else {
                    value / factor
                },
            )
            .finite(path);
        }
    }
    let result = match (op, left, right) {
        (Add, Value::Integer(a), Value::Integer(b)) => a.checked_add(b).map(Value::Integer),
        (Sub, Value::Integer(a), Value::Integer(b)) => a.checked_sub(b).map(Value::Integer),
        (Mul, Value::Integer(a), Value::Integer(b)) => a.checked_mul(b).map(Value::Integer),
        (Div, Value::Integer(_), Value::Integer(0)) => {
            return Err(EvaluationError::DivisionByZero { path: path.into() });
        }
        (Div, Value::Integer(a), Value::Integer(b)) => a.checked_div(b).map(Value::Integer),
        (Add, Value::Number(a), Value::Number(b)) => Some(Value::Number(a + b)),
        (Sub, Value::Number(a), Value::Number(b)) => Some(Value::Number(a - b)),
        (Mul, Value::Number(a), Value::Number(b)) => Some(Value::Number(a * b)),
        (Div, Value::Number(_), Value::Number(0.0)) => {
            return Err(EvaluationError::DivisionByZero { path: path.into() });
        }
        (Div, Value::Number(a), Value::Number(b)) => Some(Value::Number(a / b)),
        (Add, Value::Price(a), Value::Price(b)) => Some(Value::Price(a + b)),
        (Sub, Value::Price(a), Value::Price(b)) => Some(Value::Price(a - b)),
        (Mul, Value::Price(a), Value::Number(b)) | (Mul, Value::Number(b), Value::Price(a)) => {
            Some(Value::Price(a * b))
        }
        (Div, Value::Price(_), Value::Number(0.0)) | (Div, Value::Price(_), Value::Price(0.0)) => {
            return Err(EvaluationError::DivisionByZero { path: path.into() });
        }
        (Div, Value::Price(a), Value::Number(b)) => Some(Value::Price(a / b)),
        (Div, Value::Price(a), Value::Price(b)) => Some(Value::Number(a / b)),
        (Add, Value::Duration(a), Value::Duration(b)) => a.checked_add(&b).map(Value::Duration),
        (Sub, Value::Duration(a), Value::Duration(b)) => a.checked_sub(&b).map(Value::Duration),
        (Add, Value::Timestamp(a), Value::Duration(b))
        | (Add, Value::Duration(b), Value::Timestamp(a)) => {
            a.checked_add_signed(b).map(Value::Timestamp)
        }
        (Sub, Value::Timestamp(a), Value::Duration(b)) => {
            a.checked_sub_signed(b).map(Value::Timestamp)
        }
        (Sub, Value::Timestamp(a), Value::Timestamp(b)) => Some(Value::Duration(a - b)),
        (Min, a, b) | (Max, a, b) => return min_max(op, a, b, path),
        _ => None,
    }
    .ok_or_else(|| EvaluationError::ArithmeticOverflow { path: path.into() })?;
    result.finite(path)
}

fn compare(op: BinaryOp, left: Value, right: Value, path: &str) -> Result<Value, EvaluationError> {
    let ordering = match (&left, &right) {
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Number(a), Value::Number(b))
        | (Value::Price(a), Value::Price(b))
        | (Value::Ratio(a), Value::Ratio(b))
        | (Value::Percent(a), Value::Percent(b))
        | (Value::PricePerObservation(a), Value::PricePerObservation(b))
        | (Value::PricePerObservationSquared(a), Value::PricePerObservationSquared(b))
        | (Value::RatioPerObservation(a), Value::RatioPerObservation(b))
        | (Value::RatioPerObservationSquared(a), Value::RatioPerObservationSquared(b))
        | (Value::LogReturn(a), Value::LogReturn(b))
        | (Value::LogReturnVariance(a), Value::LogReturnVariance(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Duration(a), Value::Duration(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
        (Value::Side(a), Value::Side(b)) => Some((*a as u8).cmp(&(*b as u8))),
        _ => {
            return Err(EvaluationError::TypeMismatch {
                path: path.into(),
                expected: left.scalar_type(),
                actual: Some(right.scalar_type()),
            });
        }
    };
    let equal = left == right;
    Ok(Value::Bool(match op {
        BinaryOp::Eq => equal,
        BinaryOp::Ne => !equal,
        BinaryOp::Lt => ordering.is_some_and(|o| o.is_lt()),
        BinaryOp::Le => ordering.is_some_and(|o| o.is_le()),
        BinaryOp::Gt => ordering.is_some_and(|o| o.is_gt()),
        BinaryOp::Ge => ordering.is_some_and(|o| o.is_ge()),
        _ => false,
    }))
}
fn is_semantic_float(scalar: ScalarType) -> bool {
    matches!(
        scalar,
        ScalarType::Ratio
            | ScalarType::Percent
            | ScalarType::PricePerObservation
            | ScalarType::PricePerObservationSquared
            | ScalarType::RatioPerObservation
            | ScalarType::RatioPerObservationSquared
            | ScalarType::LogReturn
            | ScalarType::LogReturnVariance
    )
}

fn float_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value)
        | Value::Price(value)
        | Value::Ratio(value)
        | Value::Percent(value)
        | Value::PricePerObservation(value)
        | Value::PricePerObservationSquared(value)
        | Value::RatioPerObservation(value)
        | Value::RatioPerObservationSquared(value)
        | Value::LogReturn(value)
        | Value::LogReturnVariance(value) => Some(*value),
        _ => None,
    }
}

fn typed_float(scalar: ScalarType, value: f64) -> Value {
    match scalar {
        ScalarType::Ratio => Value::Ratio(value),
        ScalarType::Percent => Value::Percent(value),
        ScalarType::PricePerObservation => Value::PricePerObservation(value),
        ScalarType::PricePerObservationSquared => Value::PricePerObservationSquared(value),
        ScalarType::RatioPerObservation => Value::RatioPerObservation(value),
        ScalarType::RatioPerObservationSquared => Value::RatioPerObservationSquared(value),
        ScalarType::LogReturn => Value::LogReturn(value),
        ScalarType::LogReturnVariance => Value::LogReturnVariance(value),
        _ => unreachable!("not a semantic floating-point scalar"),
    }
}

fn min_max(op: BinaryOp, left: Value, right: Value, path: &str) -> Result<Value, EvaluationError> {
    let less = match compare(BinaryOp::Lt, left.clone(), right.clone(), path)? {
        Value::Bool(value) => value,
        _ => false,
    };
    Ok(if matches!(op, BinaryOp::Min) == less {
        left
    } else {
        right
    })
}

pub(crate) fn collect_material_refs(
    expr: &Expr,
    refs: &mut Vec<String>,
    path: &str,
) -> Result<(), CompileError> {
    let mut nodes = 0;
    collect_material_refs_inner(expr, refs, path, 1, &mut nodes)
}

fn collect_material_refs_inner(
    expr: &Expr,
    refs: &mut Vec<String>,
    path: &str,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), CompileError> {
    *nodes += 1;
    if *nodes > crate::MAX_EXPR_NODES {
        return Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual: *nodes,
            limit: crate::MAX_EXPR_NODES,
        });
    }
    if depth > crate::MAX_EXPR_DEPTH {
        return Err(CompileError::ExcessiveBound {
            path: path.into(),
            actual: depth,
            limit: crate::MAX_EXPR_DEPTH,
        });
    }
    let mut visit = |child: &Expr, suffix: &str| {
        collect_material_refs_inner(child, refs, &format!("{path}.{suffix}"), depth + 1, nodes)
    };
    match expr {
        Expr::Material { id } => refs.push(id.clone()),
        Expr::Select { cases, .. } => {
            for (name, case) in cases {
                visit(case, &format!("cases.{name}"))?;
            }
        }
        Expr::Not { value }
        | Expr::Strict { value }
        | Expr::Abs { value }
        | Expr::IsPresent { value }
        | Expr::IsMissing { value } => visit(value, "value")?,
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
            visit(left, "left")?;
            visit(right, "right")?;
        }
        Expr::All { items } | Expr::Any { items } => {
            for (index, item) in items.iter().enumerate() {
                visit(item, &format!("items[{index}]"))?;
            }
        }
        _ => {}
    }
    Ok(())
}
