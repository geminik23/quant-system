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
use crate::projectors::HistoricalInputsDocument;

#[derive(Debug, Clone)]
struct BoundPoint {
    binding: ParameterBinding,
    document: StrategyConfig,
}

/// Bounds applied before a declared parameter space allocates axes or candidate points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredSpaceLimits {
    pub max_axis_values: usize,
    pub max_pre_constraint_points: usize,
    pub max_points: usize,
}

impl DeclaredSpaceLimits {
    pub fn new(
        max_axis_values: usize,
        max_pre_constraint_points: usize,
        max_points: usize,
    ) -> Result<Self, ResearchError> {
        if [max_axis_values, max_pre_constraint_points, max_points].contains(&0) {
            return Err(ResearchError::InvalidPlan(
                "declared-space limits must be positive".into(),
            ));
        }
        Ok(Self {
            max_axis_values,
            max_pre_constraint_points,
            max_points,
        })
    }
}

impl Default for DeclaredSpaceLimits {
    fn default() -> Self {
        Self {
            max_axis_values: 1_000_000,
            max_pre_constraint_points: 1_000_000,
            max_points: 1_000_000,
        }
    }
}

/// A strict declared parameter space backed by one strategy document and one space document.
#[derive(Clone)]
pub struct DeclaredSpace {
    family_id: String,
    points: Vec<BoundPoint>,
    series: Vec<SeriesGeometryConfig>,
    historical_inputs: HistoricalInputsDocument,
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
        Self::from_toml_with_limits(strategy, space, DeclaredSpaceLimits::default())
    }

    pub fn from_toml_with_limits(
        strategy: &str,
        space: &str,
        limits: DeclaredSpaceLimits,
    ) -> Result<Self, ResearchError> {
        Self::from_toml_with_library_and_limits(
            strategy,
            space,
            MaterialLibrary::builtins(),
            limits,
        )
    }

    pub fn from_toml_with_library(
        strategy: &str,
        space: &str,
        library: MaterialLibrary,
    ) -> Result<Self, ResearchError> {
        Self::from_toml_with_library_and_limits(
            strategy,
            space,
            library,
            DeclaredSpaceLimits::default(),
        )
    }

    pub fn from_toml_with_library_and_limits(
        strategy: &str,
        space: &str,
        library: MaterialLibrary,
        limits: DeclaredSpaceLimits,
    ) -> Result<Self, ResearchError> {
        let document: StrategyConfig = toml::from_str(strategy).map_err(ResearchError::Toml)?;
        let space: SpaceDocument = toml::from_str(space).map_err(ResearchError::Toml)?;
        Self::build(StrategyTemplate::new(document), space, library, limits)
    }

    /// Build from an already decoded strategy template and a space document read from any serde source, such as a JSON value a service received, with the built-in material library.
    pub fn from_documents<'de, S>(template: StrategyConfig, space: S) -> Result<Self, ResearchError>
    where
        S: serde::Deserializer<'de>,
    {
        Self::from_documents_with_limits(template, space, DeclaredSpaceLimits::default())
    }

    pub fn from_documents_with_limits<'de, S>(
        template: StrategyConfig,
        space: S,
        limits: DeclaredSpaceLimits,
    ) -> Result<Self, ResearchError>
    where
        S: serde::Deserializer<'de>,
    {
        let space = SpaceDocument::deserialize(space)
            .map_err(|error| ResearchError::InvalidDocument(format!("space document: {error}")))?;
        Self::build(
            StrategyTemplate::new(template),
            space,
            MaterialLibrary::builtins(),
            limits,
        )
    }

    pub fn projector_selections(&self) -> Result<Vec<crate::NamedProjectorSelection>, String> {
        self.historical_inputs.calendar_selections()
    }

    pub fn validate_calendar_limits(
        &self,
        limits: qs_backtest::CalendarAdmissionLimits,
    ) -> Result<(), String> {
        let requested = self.historical_inputs.limits;
        if requested.max_sessions > limits.max_sessions
            || requested.max_market_intervals > limits.max_market_intervals
            || requested.max_exceptions > limits.max_exceptions
            || requested.max_history_occurrences > limits.max_history_occurrences
            || requested.max_resolved_children > limits.max_resolved_children
            || requested.max_owned_bytes > limits.max_owned_bytes
        {
            return Err("declared calendar limits exceed the server limits".into());
        }
        Ok(())
    }

    fn build(
        template: StrategyTemplate,
        space: SpaceDocument,
        library: MaterialLibrary,
        limits: DeclaredSpaceLimits,
    ) -> Result<Self, ResearchError> {
        template
            .validate(&library)
            .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
        validate_constraints(&space.constraints)?;
        validate_series(&space.series, template.document())?;
        for selection in space
            .historical_inputs
            .calendar_selections()
            .map_err(ResearchError::InvalidPlan)?
        {
            selection.binding().map_err(ResearchError::InvalidPlan)?;
        }
        let dimensions = binding_dimensions(template.document(), &space, limits)?;
        let mut points = Vec::new();
        for_each_binding(&dimensions, |binding| {
            let accepted = space
                .constraints
                .iter()
                .try_fold(true, |accepted, constraint| {
                    if !accepted {
                        return Ok(false);
                    }
                    let value = evaluate_parameter_expression(constraint, &binding)
                        .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
                    require_boolean(value, "constraints")
                        .map_err(|error| ResearchError::InvalidDocument(error.to_string()))
                })?;
            if accepted {
                if points.len() >= limits.max_points {
                    return Err(ResearchError::InvalidPlan(format!(
                        "declared search space exceeds the admitted point limit of {}",
                        limits.max_points
                    )));
                }
                let document = template
                    .bind(&binding, &library)
                    .map_err(|error| ResearchError::InvalidDocument(error.to_string()))?;
                points.push(BoundPoint { binding, document });
            }
            Ok(())
        })?;
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
            historical_inputs: space.historical_inputs,
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

    fn bindings(
        &self,
        symbol: &str,
        point: &Self::Params,
        requirements: &qs_strategy::ConfiguredStrategyRequirements,
    ) -> Result<qs_backtest::ConfiguredHistoricalBindings, String> {
        let base = qs_backtest::ConfiguredHistoricalBindings::from_geometry(
            self.geometry(symbol, point),
            requirements,
        )
        .map_err(|error| error.to_string())?;
        let (sources, mut named, volume) = base.into_parts();
        let required = requirements
            .named_inputs
            .iter()
            .map(|requirement| requirement.name.as_str())
            .collect::<BTreeSet<_>>();
        for selection in self.historical_inputs.calendar_selections()? {
            let name = selection.snapshot(symbol, "configured").name;
            if !required.contains(name.as_str()) {
                continue;
            }
            if named.iter().any(|binding| binding.name() == name) {
                return Err(format!("named input '{name}' has more than one projector"));
            }
            named.push(selection.binding()?);
        }
        Ok(qs_backtest::ConfiguredHistoricalBindings::new(
            sources, named, volume,
        ))
    }

    fn history_start(
        &self,
        _symbol: &str,
        _point: &Self::Params,
        evaluation_start: chrono::NaiveDateTime,
    ) -> Result<chrono::NaiveDateTime, String> {
        self.historical_inputs
            .calendar_selections()?
            .iter()
            .try_fold(evaluation_start, |start, selection| {
                selection
                    .history_start(evaluation_start)
                    .map(|candidate| start.min(candidate))
            })
    }

    fn history_start_for_requirements(
        &self,
        _symbol: &str,
        _point: &Self::Params,
        evaluation_start: chrono::NaiveDateTime,
        requirements: &qs_strategy::ConfiguredStrategyRequirements,
    ) -> Result<chrono::NaiveDateTime, String> {
        let required = requirements
            .named_inputs
            .iter()
            .map(|requirement| requirement.name.as_str())
            .collect::<BTreeSet<_>>();
        self.historical_inputs
            .calendar_selections()?
            .iter()
            .filter(|selection| required.contains(selection.name()))
            .try_fold(evaluation_start, |start, selection| {
                selection
                    .history_start(evaluation_start)
                    .map(|candidate| start.min(candidate))
            })
    }

    fn library(&self) -> MaterialLibrary {
        self.library.clone()
    }

    fn input_projector_recipe(
        &self,
        symbol: &str,
        _point: &Self::Params,
    ) -> Vec<crate::InputProjectorSnapshot> {
        self.historical_inputs
            .calendar_selections()
            .expect("historical inputs were validated")
            .iter()
            .map(|selection| selection.snapshot(symbol, "configured"))
            .collect()
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
    #[serde(default)]
    historical_inputs: HistoricalInputsDocument,
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

fn binding_dimensions(
    strategy: &StrategyConfig,
    space: &SpaceDocument,
    limits: DeclaredSpaceLimits,
) -> Result<Vec<(String, Vec<ParameterValue>)>, ResearchError> {
    let mut dimensions = Vec::with_capacity(strategy.parameters.len());
    let mut product = 1usize;
    for parameter in &strategy.parameters {
        let binding = space.parameters.get(&parameter.id).ok_or_else(|| {
            ResearchError::InvalidPlan(format!(
                "strategy parameter '{}' has no space binding",
                parameter.id
            ))
        })?;
        let values = binding_values(
            &parameter.kind,
            binding,
            &parameter.id,
            limits.max_axis_values,
        )?;
        product = product.checked_mul(values.len()).ok_or_else(|| {
            ResearchError::InvalidPlan("declared search-space product overflowed".into())
        })?;
        if product > limits.max_pre_constraint_points {
            return Err(ResearchError::InvalidPlan(format!(
                "declared search-space product {product} exceeds the pre-constraint limit of {}",
                limits.max_pre_constraint_points
            )));
        }
        dimensions.push((parameter.id.clone(), values));
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
    Ok(dimensions)
}

fn for_each_binding(
    dimensions: &[(String, Vec<ParameterValue>)],
    mut visitor: impl FnMut(ParameterBinding) -> Result<(), ResearchError>,
) -> Result<(), ResearchError> {
    fn visit(
        index: usize,
        dimensions: &[(String, Vec<ParameterValue>)],
        binding: &mut ParameterBinding,
        visitor: &mut dyn FnMut(ParameterBinding) -> Result<(), ResearchError>,
    ) -> Result<(), ResearchError> {
        let Some((name, values)) = dimensions.get(index) else {
            return visitor(binding.clone());
        };
        for value in values {
            binding.0.insert(name.clone(), value.clone());
            visit(index + 1, dimensions, binding, visitor)?;
        }
        binding.0.remove(name);
        Ok(())
    }

    visit(
        0,
        dimensions,
        &mut ParameterBinding::default(),
        &mut visitor,
    )
}

fn binding_values(
    kind: &ParameterKind,
    binding: &SpaceBinding,
    name: &str,
    max_axis_values: usize,
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
        if values.len() > max_axis_values {
            return Err(ResearchError::InvalidPlan(format!(
                "space binding '{name}' has {} values, above the axis limit of {max_axis_values}",
                values.len()
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
                let span = i128::from(to) - i128::from(from);
                let count = span / i128::from(step) + 1;
                let count = usize::try_from(count).map_err(|_| {
                    ResearchError::InvalidPlan(format!("range count for '{name}' overflowed"))
                })?;
                if count > max_axis_values {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range for '{name}' has {count} values, above the axis limit of {max_axis_values}"
                    )));
                }
                let mut values = Vec::with_capacity(count);
                for index in 0..count {
                    let value = i128::from(from) + i128::from(step) * index as i128;
                    let value = i64::try_from(value).map_err(|_| {
                        ResearchError::InvalidPlan(format!("range for '{name}' overflowed"))
                    })?;
                    values.push(ParameterValue::Integer(value));
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
                let quotient = (to - from) / step;
                if !quotient.is_finite() || quotient < 0.0 {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range count for '{name}' is not finite"
                    )));
                }
                let floored = quotient.floor();
                if floored > (usize::MAX - 1) as f64 {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range count for '{name}' overflowed"
                    )));
                }
                let count = (floored as usize).checked_add(1).ok_or_else(|| {
                    ResearchError::InvalidPlan(format!("range count for '{name}' overflowed"))
                })?;
                if count > max_axis_values {
                    return Err(ResearchError::InvalidPlan(format!(
                        "range for '{name}' has {count} values, above the axis limit of {max_axis_values}"
                    )));
                }
                let mut values = Vec::with_capacity(count);
                for index in 0..count {
                    let value = from + step * index as f64;
                    if !value.is_finite() {
                        return Err(ResearchError::InvalidPlan(format!(
                            "range value for '{name}' is not finite"
                        )));
                    }
                    values.push(ParameterValue::Number(value));
                }
                values
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
        if options.len() > max_axis_values {
            return Err(ResearchError::InvalidPlan(format!(
                "choice axis '{name}' has {} values, above the axis limit of {max_axis_values}",
                options.len()
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
