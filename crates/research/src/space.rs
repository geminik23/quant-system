use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use qs_backtest::{PriceBasis, Timeframe};
use qs_strategy::{
    Expr, MaterialLibrary, ParameterBinding, ParameterKind, ParameterValue, SourceId,
    StrategyConfig, StrategyTemplate, evaluate_parameter_expression, require_boolean,
};
use serde::Deserialize;

use crate::error::ResearchError;
use crate::family::StrategyFamily;
use crate::geometry::SeriesGeometry;

#[derive(Debug, Clone)]
struct BoundPoint {
    binding: ParameterBinding,
    document: StrategyConfig,
}

/// A strict declared parameter space backed by one strategy document and one space document.
#[derive(Clone)]
pub struct DeclaredSpace {
    family_id: String,
    points: Vec<BoundPoint>,
    series: Vec<SeriesGeometryConfig>,
    library: MaterialLibrary,
}

impl DeclaredSpace {
    pub fn load(
        strategy_path: impl AsRef<Path>,
        space_path: impl AsRef<Path>,
    ) -> Result<Self, ResearchError> {
        Self::load_with_library(strategy_path, space_path, MaterialLibrary::builtins())
    }

    pub fn load_with_library(
        strategy_path: impl AsRef<Path>,
        space_path: impl AsRef<Path>,
        library: MaterialLibrary,
    ) -> Result<Self, ResearchError> {
        let strategy = fs::read_to_string(strategy_path).map_err(ResearchError::Io)?;
        let space = fs::read_to_string(space_path).map_err(ResearchError::Io)?;
        Self::from_toml_with_library(&strategy, &space, library)
    }

    pub fn from_toml(strategy: &str, space: &str) -> Result<Self, ResearchError> {
        Self::from_toml_with_library(strategy, space, MaterialLibrary::builtins())
    }

    pub fn from_toml_with_library(
        strategy: &str,
        space: &str,
        library: MaterialLibrary,
    ) -> Result<Self, ResearchError> {
        let document: StrategyConfig = toml::from_str(strategy).map_err(ResearchError::Toml)?;
        let space: SpaceDocument = toml::from_str(space).map_err(ResearchError::Toml)?;
        Self::build(StrategyTemplate::new(document), space, library)
    }

    fn build(
        template: StrategyTemplate,
        space: SpaceDocument,
        library: MaterialLibrary,
    ) -> Result<Self, ResearchError> {
        template
            .validate(&library)
            .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
        validate_constraints(&space.constraints)?;
        validate_series(&space.series, template.document())?;
        let bindings = enumerate_bindings(template.document(), &space)?;
        let mut points = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let accepted = space
                .constraints
                .iter()
                .map(|constraint| {
                    let value = evaluate_parameter_expression(constraint, &binding)
                        .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
                    require_boolean(value, "constraints")
                        .map_err(|error| ResearchError::InvalidDocument(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .all(|value| value);
            if accepted {
                let document = template
                    .bind(&binding, &library)
                    .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
                points.push(BoundPoint { binding, document });
            }
        }
        if points.is_empty() {
            return Err(ResearchError::InvalidPlan(
                "declared search space produced no parameter points".into(),
            ));
        }
        for point in &points {
            for series in &space.series {
                series
                    .resolve("SYMBOL", &point.binding)
                    .map_err(ResearchError::InvalidPlan)?;
            }
        }
        Ok(Self {
            family_id: space.family_id,
            points,
            series: space.series,
            library,
        })
    }
}

impl StrategyFamily for DeclaredSpace {
    type Params = usize;

    fn family_id(&self) -> &str {
        &self.family_id
    }

    fn points(&self) -> Vec<Self::Params> {
        (0..self.points.len()).collect()
    }

    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding {
        self.points[*point].binding.clone()
    }

    fn config(&self, point: &Self::Params) -> StrategyConfig {
        self.points[*point].document.clone()
    }

    fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<SeriesGeometry> {
        let binding = &self.points[*point].binding;
        self.series
            .iter()
            .map(|series| series.resolve(symbol, binding))
            .collect::<Result<_, _>>()
            .expect("declared geometry was validated for every enumerated point")
    }

    fn library(&self) -> MaterialLibrary {
        self.library.clone()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpaceDocument {
    family_id: String,
    parameters: BTreeMap<String, SpaceBinding>,
    #[serde(default)]
    constraints: Vec<Expr>,
    series: Vec<SeriesGeometryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpaceBinding {
    values: Option<Vec<toml::Value>>,
    range: Option<NumericRange>,
    all: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct NumericRange {
    from: toml::Value,
    to: toml::Value,
    step: toml::Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesGeometryConfig {
    source: SourceId,
    symbol: GeometryString,
    timeframe_seconds: GeometryInteger,
    price_basis: GeometryString,
    alignment_offset_seconds: GeometryInteger,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum GeometryString {
    Literal(String),
    Parameter { param: String },
    PlanSymbol { plan_symbol: bool },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum GeometryInteger {
    Literal(i64),
    Parameter { param: String },
}

impl SeriesGeometryConfig {
    fn resolve(
        &self,
        plan_symbol: &str,
        binding: &ParameterBinding,
    ) -> Result<SeriesGeometry, String> {
        let symbol = self.symbol.resolve(plan_symbol, binding)?;
        let seconds = self.timeframe_seconds.resolve(binding)?;
        let seconds = u32::try_from(seconds).map_err(|_| "timeframe must fit u32".to_string())?;
        let timeframe = Timeframe::seconds(seconds).map_err(|error| error.to_string())?;
        let price_basis = match self.price_basis.resolve(plan_symbol, binding)?.as_str() {
            "bid" => PriceBasis::Bid,
            "ask" => PriceBasis::Ask,
            "mid" => PriceBasis::Mid,
            value => return Err(format!("unknown price basis '{value}'")),
        };
        let alignment_offset_seconds =
            i32::try_from(self.alignment_offset_seconds.resolve(binding)?)
                .map_err(|_| "alignment offset must fit i32".to_string())?;
        Ok(SeriesGeometry::new(
            self.source.clone(),
            symbol,
            timeframe,
            price_basis,
            alignment_offset_seconds,
        ))
    }
}

impl GeometryString {
    fn resolve(&self, plan_symbol: &str, binding: &ParameterBinding) -> Result<String, String> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Parameter { param } => match binding.get(param) {
                Some(ParameterValue::Choice(value)) => Ok(value.clone()),
                _ => Err(format!("geometry parameter '{param}' must be a choice")),
            },
            Self::PlanSymbol { plan_symbol: true } => Ok(plan_symbol.to_owned()),
            Self::PlanSymbol { plan_symbol: false } => {
                Err("plan_symbol must be true when present".into())
            }
        }
    }
}

impl GeometryInteger {
    fn resolve(&self, binding: &ParameterBinding) -> Result<i64, String> {
        match self {
            Self::Literal(value) => Ok(*value),
            Self::Parameter { param } => match binding.get(param) {
                Some(ParameterValue::Integer(value)) => Ok(*value),
                _ => Err(format!("geometry parameter '{param}' must be an integer")),
            },
        }
    }
}

fn enumerate_bindings(
    strategy: &StrategyConfig,
    space: &SpaceDocument,
) -> Result<Vec<ParameterBinding>, ResearchError> {
    if strategy.parameters.len() != space.parameters.len() {
        return Err(ResearchError::InvalidPlan(
            "every strategy parameter must have exactly one space binding".into(),
        ));
    }
    let mut dimensions = Vec::new();
    for parameter in &strategy.parameters {
        let binding = space.parameters.get(&parameter.id).ok_or_else(|| {
            ResearchError::InvalidPlan(format!(
                "strategy parameter '{}' has no space binding",
                parameter.id
            ))
        })?;
        dimensions.push((
            parameter.id.clone(),
            binding_values(&parameter.kind, binding, &parameter.id)?,
        ));
    }
    for name in space.parameters.keys() {
        if !strategy
            .parameters
            .iter()
            .any(|parameter| parameter.id == *name)
        {
            return Err(ResearchError::InvalidPlan(format!(
                "space binds unknown parameter '{name}'"
            )));
        }
    }

    let mut points = vec![ParameterBinding::default()];
    for (name, values) in dimensions {
        let mut next = Vec::with_capacity(points.len().saturating_mul(values.len()));
        for point in points {
            for value in &values {
                let mut point = point.clone();
                point.0.insert(name.clone(), value.clone());
                next.push(point);
            }
        }
        points = next;
    }
    Ok(points)
}

fn binding_values(
    kind: &ParameterKind,
    binding: &SpaceBinding,
    name: &str,
) -> Result<Vec<ParameterValue>, ResearchError> {
    let forms = usize::from(binding.values.is_some())
        + usize::from(binding.range.is_some())
        + usize::from(binding.all.is_some());
    if forms != 1 {
        return Err(ResearchError::InvalidPlan(format!(
            "space binding '{name}' must use exactly one of values, range, or all"
        )));
    }
    let values = if let Some(values) = &binding.values {
        if values.is_empty() {
            return Err(ResearchError::InvalidPlan(format!(
                "space binding '{name}' must not be empty"
            )));
        }
        values
            .iter()
            .map(|value| toml_parameter_value(kind, value, name))
            .collect::<Result<Vec<_>, _>>()?
    } else if let Some(range) = &binding.range {
        match kind {
            ParameterKind::Integer => {
                let (Some(from), Some(to), Some(step)) = (
                    range.from.as_integer(),
                    range.to.as_integer(),
                    range.step.as_integer(),
                ) else {
                    return Err(ResearchError::InvalidPlan(format!(
                        "integer range for '{name}' requires integer bounds and step"
                    )));
                };
                if step <= 0 || from > to {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range for '{name}' requires a positive step and non-empty bounds"
                    )));
                }
                let mut values = Vec::new();
                let mut current = from;
                while current <= to {
                    values.push(ParameterValue::Integer(current));
                    current = current.checked_add(step).ok_or_else(|| {
                        ResearchError::InvalidPlan(format!("range for '{name}' overflowed"))
                    })?;
                }
                values
            }
            ParameterKind::Number => {
                let from = toml_number(&range.from, name)?;
                let to = toml_number(&range.to, name)?;
                let step = toml_number(&range.step, name)?;
                if step <= 0.0 || from > to {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range for '{name}' requires a positive step and non-empty bounds"
                    )));
                }
                let count = ((to - from) / step).floor() as usize + 1;
                if count > 1_000_000 {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range for '{name}' exceeds one million values"
                    )));
                }
                (0..count)
                    .map(|index| ParameterValue::Number(from + step * index as f64))
                    .collect()
            }
            ParameterKind::Choice { .. } => {
                return Err(ResearchError::InvalidPlan(format!(
                    "choice parameter '{name}' cannot use a numeric range"
                )));
            }
        }
    } else {
        let ParameterKind::Choice { options } = kind else {
            return Err(ResearchError::InvalidPlan(format!(
                "only choice parameter '{name}' may use all"
            )));
        };
        if binding.all != Some(true) {
            return Err(ResearchError::InvalidPlan(format!(
                "all for '{name}' must be true"
            )));
        }
        options
            .iter()
            .cloned()
            .map(ParameterValue::Choice)
            .collect()
    };
    let mut unique = BTreeSet::new();
    for value in &values {
        let label = match value {
            ParameterValue::Integer(value) => format!("i:{value}"),
            ParameterValue::Number(value) => format!("n:{:016x}", value.to_bits()),
            ParameterValue::Choice(value) => format!("c:{value}"),
        };
        if !unique.insert(label) {
            return Err(ResearchError::InvalidPlan(format!(
                "space binding '{name}' contains a duplicate value"
            )));
        }
    }
    Ok(values)
}

fn toml_number(value: &toml::Value, name: &str) -> Result<f64, ResearchError> {
    let value = match value {
        toml::Value::Float(value) => *value,
        toml::Value::Integer(value) => *value as f64,
        _ => {
            return Err(ResearchError::InvalidPlan(format!(
                "numeric range for '{name}' requires numeric bounds and step"
            )));
        }
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ResearchError::InvalidPlan(format!(
            "numeric range for '{name}' must be finite"
        )))
    }
}

fn toml_parameter_value(
    kind: &ParameterKind,
    value: &toml::Value,
    name: &str,
) -> Result<ParameterValue, ResearchError> {
    match (kind, value) {
        (ParameterKind::Integer, toml::Value::Integer(value)) => {
            Ok(ParameterValue::Integer(*value))
        }
        (ParameterKind::Number, toml::Value::Float(value)) => {
            if value.is_finite() {
                Ok(ParameterValue::Number(*value))
            } else {
                Err(ResearchError::InvalidPlan(format!(
                    "number for '{name}' must be finite"
                )))
            }
        }
        (ParameterKind::Number, toml::Value::Integer(value)) => {
            Ok(ParameterValue::Number(*value as f64))
        }
        (ParameterKind::Choice { options }, toml::Value::String(value))
            if options.contains(value) =>
        {
            Ok(ParameterValue::Choice(value.clone()))
        }
        _ => Err(ResearchError::InvalidPlan(format!(
            "space value for '{name}' does not match its parameter type"
        ))),
    }
}

fn validate_constraints(constraints: &[Expr]) -> Result<(), ResearchError> {
    fn visit(expr: &Expr) -> bool {
        match expr {
            Expr::Literal { .. } | Expr::Param { .. } => true,
            Expr::Not { value }
            | Expr::Abs { value }
            | Expr::IsPresent { value }
            | Expr::IsMissing { value } => visit(value),
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
            | Expr::Max { left, right } => visit(left) && visit(right),
            Expr::All { items } | Expr::Any { items } => items.iter().all(visit),
            _ => false,
        }
    }
    if constraints.iter().all(visit) {
        Ok(())
    } else {
        Err(ResearchError::InvalidPlan(
            "space constraints may reference only parameters and literals".into(),
        ))
    }
}

fn validate_series(
    series: &[SeriesGeometryConfig],
    strategy: &StrategyConfig,
) -> Result<(), ResearchError> {
    if series.len() != strategy.sources.len() {
        return Err(ResearchError::InvalidPlan(
            "series geometry must bind every declared strategy source exactly once".into(),
        ));
    }
    let declared: BTreeSet<_> = strategy.sources.iter().cloned().collect();
    let actual: BTreeSet<_> = series.iter().map(|item| item.source.clone()).collect();
    if declared != actual || actual.len() != series.len() {
        return Err(ResearchError::InvalidPlan(
            "series geometry contains a missing, duplicate, or undeclared source".into(),
        ));
    }
    Ok(())
}
