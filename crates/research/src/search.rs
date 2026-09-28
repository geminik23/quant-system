use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, NaiveDateTime};
use qs_strategy::{
    Expr, Literal, MaterialArg, MaterialArgs, MaterialConfig, MaterialLibrary, ParameterBinding,
    ParameterValue, SourceId, StrategyConfig,
};
use serde::{Deserialize, Serialize};

use crate::{
    CandidateRecipe, DataWindow, ExperimentRecipe, ResearchError, SeriesGeometry, StrategyFamily,
};
use qs_backtest::{
    ConfiguredHistoricalBindings, ConfiguredNamedInputBinding, SourceBarFactKind,
    SourceBarFactProjector,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralResourceLimits {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_candidates: usize,
    pub max_queue_bytes: usize,
    pub max_runs: usize,
    pub max_workers: usize,
    pub max_resident_bytes: usize,
    pub max_feed_bytes: usize,
    pub max_cache_bytes: usize,
    pub max_trace_bytes: usize,
    pub max_trace_records: usize,
    pub max_checkpoint_bytes: usize,
    pub max_checkpoint_records: usize,
    pub max_retained_records: usize,
}

impl StructuralResourceLimits {
    pub fn small_test() -> Self {
        Self {
            max_depth: 4,
            max_nodes: 32,
            max_candidates: 128,
            max_queue_bytes: 1 << 20,
            max_runs: 1024,
            max_workers: 2,
            max_resident_bytes: 8 << 20,
            max_feed_bytes: 8 << 20,
            max_cache_bytes: 4 << 20,
            max_trace_bytes: 1 << 20,
            max_trace_records: 4096,
            max_checkpoint_bytes: 1 << 20,
            max_checkpoint_records: 4096,
            max_retained_records: 4096,
        }
    }

    pub fn validate(self) -> Result<Self, ResearchError> {
        let values = [
            self.max_depth,
            self.max_nodes,
            self.max_candidates,
            self.max_queue_bytes,
            self.max_runs,
            self.max_workers,
            self.max_resident_bytes,
            self.max_feed_bytes,
            self.max_cache_bytes,
            self.max_trace_bytes,
            self.max_trace_records,
            self.max_checkpoint_bytes,
            self.max_checkpoint_records,
            self.max_retained_records,
        ];
        if values.contains(&0) {
            Err(ResearchError::InvalidPlan(
                "structural resource limits must all be positive".into(),
            ))
        } else {
            Ok(self)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuralComparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PredicateAtom {
    pub id: String,
    pub material: MaterialConfig,
    pub comparison: StructuralComparison,
    pub threshold: Literal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralOperators {
    pub not: bool,
    pub and: bool,
    pub or: bool,
    pub sequence: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaptureCandidate {
    pub id: String,
    pub material_key: String,
    pub source: SourceId,
    pub expiry: usize,
    pub capacity: Option<usize>,
    pub reset: Expr,
    pub gap: Expr,
    pub breakout: Expr,
    pub retest: Expr,
    pub level: Expr,
    pub normalization: Expr,
    pub close: Expr,
    pub tolerance: Expr,
    pub ordinal: Expr,
}

#[derive(Debug, Clone)]
pub struct StructuralSearchSpec {
    pub family_id: String,
    pub base_document: StrategyConfig,
    pub state_id: String,
    pub transition_priority: i32,
    pub atoms: Vec<PredicateAtom>,
    pub operators: StructuralOperators,
    pub sequence_source: SourceId,
    pub sequence_max_gap: usize,
    pub captures: Vec<CaptureCandidate>,
    pub geometry_by_symbol: BTreeMap<String, Vec<SeriesGeometry>>,
    pub limits: StructuralResourceLimits,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructuralCandidate {
    pub ordinal: u64,
    pub label: String,
    pub document: StrategyConfig,
    pub node_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationCompletion {
    Exhaustive,
    IncompleteBudget,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationDispositions {
    pub generated: u64,
    pub canonicalized: u64,
    pub duplicate: u64,
    pub invalid: u64,
    pub capability_excluded: u64,
    pub sound_pruned: u64,
    pub heuristic_pruned: u64,
    pub executed: u64,
    pub failed: u64,
    pub unvisited: u64,
}

#[derive(Debug, Clone)]
pub struct StructuralGeneration {
    pub candidates: Vec<StructuralCandidate>,
    pub completion: GenerationCompletion,
    pub dispositions: GenerationDispositions,
}

#[derive(Debug, Clone)]
pub struct ResumedStructuralGeneration {
    pub remaining: Vec<StructuralCandidate>,
    pub completed_runs: BTreeSet<u64>,
    pub committed_runs: BTreeMap<u64, crate::CompletedRunCheckpoint>,
    pub failures: BTreeMap<u64, String>,
}

pub fn resume_structural_generation(
    spec: &StructuralSearchSpec,
    checkpoint: &crate::SearchCheckpoint,
    dependency: &crate::CheckpointDependency,
    limits: crate::CheckpointLimits,
) -> Result<ResumedStructuralGeneration, ResearchError> {
    checkpoint.validate(limits)?;
    if &checkpoint.dependency != dependency {
        return Err(ResearchError::InvalidPlan(
            "resume dependencies changed".into(),
        ));
    }
    let generation = spec.generate()?;
    if checkpoint.generation_exhaustive
        && (generation.completion != GenerationCompletion::Exhaustive
            || checkpoint.frontier
                != u64::try_from(generation.candidates.len()).map_err(|_| {
                    ResearchError::InvalidPlan("generated candidate count exceeds u64".into())
                })?)
    {
        return Err(ResearchError::InvalidPlan(
            "checkpoint generation frontier is incompatible".into(),
        ));
    }
    let remaining = generation
        .candidates
        .into_iter()
        .filter(|candidate| !checkpoint.completed_candidates.contains(&candidate.ordinal))
        .collect();
    Ok(ResumedStructuralGeneration {
        remaining,
        completed_runs: checkpoint.completed_runs.clone(),
        committed_runs: checkpoint.committed_runs.clone(),
        failures: checkpoint.failures.clone(),
    })
}

#[derive(Debug, Clone)]
enum Node {
    Atom(usize),
    Not(Box<Node>),
    All(Box<Node>, Box<Node>),
    Any(Box<Node>, Box<Node>),
    Sequence(Box<Node>, Box<Node>),
    Capture(usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StructuralBinary {
    All,
    Any,
}

impl StructuralBinary {
    fn build(self, left: Node, right: Node) -> Node {
        match self {
            Self::All => Node::All(Box::new(left), Box::new(right)),
            Self::Any => Node::Any(Box::new(left), Box::new(right)),
        }
    }
}

impl Node {
    fn count(&self) -> usize {
        match self {
            Self::Atom(_) | Self::Capture(_) => 1,
            Self::Not(value) => 1 + value.count(),
            Self::All(left, right) | Self::Any(left, right) | Self::Sequence(left, right) => {
                1 + left.count() + right.count()
            }
        }
    }

    fn depth(&self) -> usize {
        match self {
            Self::Atom(_) | Self::Capture(_) => 1,
            Self::Not(value) => 1 + value.depth(),
            Self::All(left, right) | Self::Any(left, right) | Self::Sequence(left, right) => {
                1 + left.depth().max(right.depth())
            }
        }
    }
}

impl StructuralSearchSpec {
    pub fn generate(&self) -> Result<StructuralGeneration, ResearchError> {
        self.limits.validate()?;
        validate_id(&self.family_id, "family ID")?;
        if self.atoms.is_empty() {
            return Err(ResearchError::InvalidPlan(
                "structural universe needs at least one atom".into(),
            ));
        }
        if self.operators.sequence && self.sequence_max_gap == 0 {
            return Err(ResearchError::InvalidPlan(
                "sequence max gap must be positive".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        for atom in &self.atoms {
            validate_id(&atom.id, "atom ID")?;
            if !ids.insert(atom.id.clone()) {
                return Err(ResearchError::InvalidPlan("duplicate atom ID".into()));
            }
        }

        let mut nodes = Vec::<Node>::new();
        let mut retained_node_bytes = 0usize;
        let mut candidates = Vec::new();
        let mut canonical = BTreeSet::new();
        let mut dispositions = GenerationDispositions::default();
        let mut completion = GenerationCompletion::Exhaustive;

        for index in 0..self.atoms.len() {
            if self.admit_node(
                1,
                1,
                |_| Node::Atom(index),
                &mut nodes,
                &mut retained_node_bytes,
                &mut candidates,
                &mut canonical,
                &mut dispositions,
                &mut completion,
            )? {
                return Ok(StructuralGeneration {
                    candidates,
                    completion,
                    dispositions,
                });
            }
        }
        for index in 0..self.captures.len() {
            if self.admit_node(
                1,
                1,
                |_| Node::Capture(index),
                &mut nodes,
                &mut retained_node_bytes,
                &mut candidates,
                &mut canonical,
                &mut dispositions,
                &mut completion,
            )? {
                return Ok(StructuralGeneration {
                    candidates,
                    completion,
                    dispositions,
                });
            }
        }

        for depth in 2..=self.limits.max_depth {
            let prior_len = nodes.len();
            if self.operators.not {
                for index in 0..prior_len {
                    if nodes[index].depth() + 1 != depth {
                        continue;
                    }
                    let count = nodes[index].count().checked_add(1).ok_or_else(|| {
                        ResearchError::InvalidPlan("structural node count overflowed".into())
                    })?;
                    if self.admit_node(
                        depth,
                        count,
                        |nodes| Node::Not(Box::new(nodes[index].clone())),
                        &mut nodes,
                        &mut retained_node_bytes,
                        &mut candidates,
                        &mut canonical,
                        &mut dispositions,
                        &mut completion,
                    )? {
                        return Ok(StructuralGeneration {
                            candidates,
                            completion,
                            dispositions,
                        });
                    }
                }
            }
            for left in 0..prior_len {
                for right in left + 1..prior_len {
                    if 1 + nodes[left].depth().max(nodes[right].depth()) != depth {
                        continue;
                    }
                    let count = nodes[left]
                        .count()
                        .checked_add(nodes[right].count())
                        .and_then(|value| value.checked_add(1))
                        .ok_or_else(|| {
                            ResearchError::InvalidPlan("structural node count overflowed".into())
                        })?;
                    for operation in [StructuralBinary::All, StructuralBinary::Any] {
                        if (operation == StructuralBinary::All && !self.operators.and)
                            || (operation == StructuralBinary::Any && !self.operators.or)
                        {
                            continue;
                        }
                        if self.admit_node(
                            depth,
                            count,
                            |nodes| operation.build(nodes[left].clone(), nodes[right].clone()),
                            &mut nodes,
                            &mut retained_node_bytes,
                            &mut candidates,
                            &mut canonical,
                            &mut dispositions,
                            &mut completion,
                        )? {
                            return Ok(StructuralGeneration {
                                candidates,
                                completion,
                                dispositions,
                            });
                        }
                    }
                }
            }
            if self.operators.sequence {
                for first in 0..prior_len {
                    for second in 0..prior_len {
                        if first == second
                            || 1 + nodes[first].depth().max(nodes[second].depth()) != depth
                        {
                            continue;
                        }
                        let count = nodes[first]
                            .count()
                            .checked_add(nodes[second].count())
                            .and_then(|value| value.checked_add(1))
                            .ok_or_else(|| {
                                ResearchError::InvalidPlan(
                                    "structural node count overflowed".into(),
                                )
                            })?;
                        if self.admit_node(
                            depth,
                            count,
                            |nodes| {
                                Node::Sequence(
                                    Box::new(nodes[first].clone()),
                                    Box::new(nodes[second].clone()),
                                )
                            },
                            &mut nodes,
                            &mut retained_node_bytes,
                            &mut candidates,
                            &mut canonical,
                            &mut dispositions,
                            &mut completion,
                        )? {
                            return Ok(StructuralGeneration {
                                candidates,
                                completion,
                                dispositions,
                            });
                        }
                    }
                }
            }
            if nodes.len() == prior_len {
                break;
            }
        }
        Ok(StructuralGeneration {
            candidates,
            completion,
            dispositions,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn admit_node<F>(
        &self,
        depth: usize,
        count: usize,
        build: F,
        nodes: &mut Vec<Node>,
        retained_node_bytes: &mut usize,
        candidates: &mut Vec<StructuralCandidate>,
        canonical: &mut BTreeSet<String>,
        dispositions: &mut GenerationDispositions,
        completion: &mut GenerationCompletion,
    ) -> Result<bool, ResearchError>
    where
        F: FnOnce(&[Node]) -> Node,
    {
        dispositions.generated = dispositions
            .generated
            .checked_add(1)
            .ok_or_else(|| ResearchError::InvalidPlan("generated disposition overflowed".into()))?;
        if depth > self.limits.max_depth || count > self.limits.max_nodes {
            dispositions.invalid = dispositions.invalid.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("invalid disposition overflowed".into())
            })?;
            return Ok(false);
        }
        if candidates.len() == self.limits.max_candidates {
            *completion = GenerationCompletion::IncompleteBudget;
            dispositions.unvisited = dispositions.unvisited.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("unvisited disposition overflowed".into())
            })?;
            return Ok(true);
        }
        let bytes = count
            .checked_mul(std::mem::size_of::<Node>())
            .ok_or_else(|| ResearchError::InvalidPlan("structural node bytes overflowed".into()))?;
        if retained_node_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.limits.max_queue_bytes)
        {
            *completion = GenerationCompletion::IncompleteBudget;
            dispositions.unvisited = dispositions.unvisited.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("unvisited disposition overflowed".into())
            })?;
            return Ok(true);
        }
        let node = build(nodes);
        let (document, label) = match self.lower(&node) {
            Ok(value) => value,
            Err(_) => {
                dispositions.invalid = dispositions.invalid.checked_add(1).ok_or_else(|| {
                    ResearchError::InvalidPlan("invalid disposition overflowed".into())
                })?;
                return Ok(false);
            }
        };
        let encoded = serde_json::to_string(&document)
            .map_err(|error| ResearchError::InvalidPlan(error.to_string()))?;
        if !canonical.insert(encoded) {
            dispositions.duplicate = dispositions.duplicate.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("duplicate disposition overflowed".into())
            })?;
            return Ok(false);
        }
        dispositions.canonicalized =
            dispositions.canonicalized.checked_add(1).ok_or_else(|| {
                ResearchError::InvalidPlan("canonicalized disposition overflowed".into())
            })?;
        let ordinal = u64::try_from(candidates.len())
            .map_err(|_| ResearchError::InvalidPlan("candidate ordinal overflowed".into()))?;
        candidates.push(StructuralCandidate {
            ordinal,
            label,
            document,
            node_count: count,
        });
        *retained_node_bytes += bytes;
        nodes.push(node);
        Ok(false)
    }

    fn lower(&self, node: &Node) -> Result<(StrategyConfig, String), ResearchError> {
        let mut document = self.base_document.clone();
        let mut extra = Vec::new();
        let (expression, label) = self.lower_node(node, &mut extra)?;
        document.materials.extend(extra);
        let state = document
            .states
            .iter_mut()
            .find(|state| state.id == self.state_id)
            .ok_or_else(|| {
                ResearchError::InvalidPlan("structural target state is absent".into())
            })?;
        let transition = state
            .transitions
            .iter_mut()
            .find(|transition| transition.priority == self.transition_priority)
            .ok_or_else(|| {
                ResearchError::InvalidPlan("structural target transition is absent".into())
            })?;
        transition.when = Expr::Strict {
            value: Box::new(expression),
        };
        qs_strategy::ConfiguredStrategy::compile(
            document.clone(),
            &MaterialLibrary::builtins(),
            "structural_admission",
            "STRUCTURAL",
        )
        .map_err(|error| {
            ResearchError::InvalidPlan(format!("generated candidate does not compile: {error}"))
        })?;
        Ok((document, label))
    }

    fn lower_node(
        &self,
        node: &Node,
        materials: &mut Vec<MaterialConfig>,
    ) -> Result<(Expr, String), ResearchError> {
        Ok(match node {
            Node::Atom(index) => {
                let atom = &self.atoms[*index];
                let material = canonical_material(&atom.material);
                let material_id = material.id.clone();
                if !materials.iter().any(|existing| existing.id == material_id) {
                    materials.push(material);
                }
                (
                    compare(
                        atom.comparison,
                        Expr::Material { id: material_id },
                        Expr::Literal {
                            value: atom.threshold.clone(),
                        },
                    ),
                    atom.id.clone(),
                )
            }
            Node::Not(value) => {
                let (expression, label) = self.lower_node(value, materials)?;
                (
                    Expr::Not {
                        value: Box::new(expression),
                    },
                    format!("not({label})"),
                )
            }
            Node::All(left, right) | Node::Any(left, right) => {
                let is_all = matches!(node, Node::All(_, _));
                if let Some(index) = self.implied_atom(left, right, is_all) {
                    return self.lower_node(&Node::Atom(index), materials);
                }
                let (left, left_label) = self.lower_node(left, materials)?;
                let (right, right_label) = self.lower_node(right, materials)?;
                (
                    if is_all {
                        Expr::All {
                            items: vec![left, right],
                        }
                    } else {
                        Expr::Any {
                            items: vec![left, right],
                        }
                    },
                    format!(
                        "{}({left_label},{right_label})",
                        if is_all { "and" } else { "or" }
                    ),
                )
            }
            Node::Sequence(first, second) => {
                let (first, first_label) = self.lower_node(first, materials)?;
                let (second, second_label) = self.lower_node(second, materials)?;
                let id = format!("generated_sequence_{}", materials.len());
                materials.push(MaterialConfig {
                    id: id.clone(),
                    key: qs_strategy::MATERIAL_SEQUENCE_AB.into(),
                    inputs: vec![
                        Expr::Strict {
                            value: Box::new(first),
                        },
                        Expr::Strict {
                            value: Box::new(second),
                        },
                    ],
                    params: MaterialArgs::new([
                        ("source", MaterialArg::Source(self.sequence_source.clone())),
                        (
                            "max_gap",
                            MaterialArg::Integer(i64::try_from(self.sequence_max_gap).map_err(
                                |_| ResearchError::InvalidPlan("sequence gap exceeds i64".into()),
                            )?),
                        ),
                    ]),
                });
                (
                    Expr::Material { id },
                    format!("sequence({first_label},{second_label})"),
                )
            }
            Node::Capture(index) => {
                let capture = &self.captures[*index];
                let id = format!("generated_capture_{index}");
                let mut params = vec![
                    ("source", MaterialArg::Source(capture.source.clone())),
                    (
                        "expiry",
                        MaterialArg::Integer(i64::try_from(capture.expiry).map_err(|_| {
                            ResearchError::InvalidPlan("capture expiry exceeds i64".into())
                        })?),
                    ),
                ];
                if let Some(capacity) = capture.capacity {
                    params.push((
                        "capacity",
                        MaterialArg::Integer(i64::try_from(capacity).map_err(|_| {
                            ResearchError::InvalidPlan("capture capacity exceeds i64".into())
                        })?),
                    ));
                }
                materials.push(MaterialConfig {
                    id: id.clone(),
                    key: capture.material_key.clone(),
                    inputs: vec![
                        capture.reset.clone(),
                        capture.gap.clone(),
                        capture.breakout.clone(),
                        capture.retest.clone(),
                        capture.level.clone(),
                        capture.normalization.clone(),
                        capture.close.clone(),
                        capture.tolerance.clone(),
                        capture.ordinal.clone(),
                    ],
                    params: MaterialArgs::new(params),
                });
                (Expr::Material { id }, capture.id.clone())
            }
        })
    }

    fn implied_atom(&self, left: &Node, right: &Node, all: bool) -> Option<usize> {
        let (Node::Atom(left), Node::Atom(right)) = (left, right) else {
            return None;
        };
        let left_atom = &self.atoms[*left];
        let right_atom = &self.atoms[*right];
        if canonical_material(&left_atom.material) != canonical_material(&right_atom.material)
            || left_atom.comparison != right_atom.comparison
        {
            return None;
        }
        let ordering = literal_partial_cmp(&left_atom.threshold, &right_atom.threshold)?;
        let choose_left = match left_atom.comparison {
            StructuralComparison::Gt | StructuralComparison::Ge => {
                if all {
                    ordering.is_ge()
                } else {
                    ordering.is_le()
                }
            }
            StructuralComparison::Lt | StructuralComparison::Le => {
                if all {
                    ordering.is_le()
                } else {
                    ordering.is_ge()
                }
            }
            StructuralComparison::Eq | StructuralComparison::Ne => return None,
        };
        Some(if choose_left { *left } else { *right })
    }
}

fn canonical_material(material: &MaterialConfig) -> MaterialConfig {
    let mut material = material.clone();
    let close_alias = material.key == qs_strategy::MATERIAL_CLOSE_PRICE
        || (material.key == qs_strategy::MATERIAL_BAR_FIELD
            && material.params.get("field")
                == Some(&MaterialArg::BarField(qs_strategy::BarField::Close)));
    if close_alias {
        let source = match material.params.get("source") {
            Some(MaterialArg::Source(source)) => source.as_str(),
            _ => "source",
        };
        material.id = format!("generated_alias_close_{source}");
        material.key = qs_strategy::MATERIAL_BAR_FIELD.into();
        material.params.0.insert(
            "field".into(),
            MaterialArg::BarField(qs_strategy::BarField::Close),
        );
    }
    material
}

fn literal_partial_cmp(left: &Literal, right: &Literal) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Literal::Number(left), Literal::Number(right))
        | (Literal::Price(left), Literal::Price(right))
        | (Literal::Ratio(left), Literal::Ratio(right))
        | (Literal::Percent(left), Literal::Percent(right))
        | (Literal::PricePerObservation(left), Literal::PricePerObservation(right))
        | (Literal::PricePerObservationSquared(left), Literal::PricePerObservationSquared(right))
        | (Literal::RatioPerObservation(left), Literal::RatioPerObservation(right))
        | (Literal::RatioPerObservationSquared(left), Literal::RatioPerObservationSquared(right))
        | (Literal::LogReturn(left), Literal::LogReturn(right))
        | (Literal::LogReturnVariance(left), Literal::LogReturnVariance(right)) => {
            left.partial_cmp(right)
        }
        (Literal::Integer(left), Literal::Integer(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn compare(operation: StructuralComparison, left: Expr, right: Expr) -> Expr {
    match operation {
        StructuralComparison::Eq => Expr::Eq {
            left: Box::new(left),
            right: Box::new(right),
        },
        StructuralComparison::Ne => Expr::Ne {
            left: Box::new(left),
            right: Box::new(right),
        },
        StructuralComparison::Lt => Expr::Lt {
            left: Box::new(left),
            right: Box::new(right),
        },
        StructuralComparison::Le => Expr::Le {
            left: Box::new(left),
            right: Box::new(right),
        },
        StructuralComparison::Gt => Expr::Gt {
            left: Box::new(left),
            right: Box::new(right),
        },
        StructuralComparison::Ge => Expr::Ge {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

fn validate_id(value: &str, name: &str) -> Result<(), ResearchError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        Err(ResearchError::InvalidPlan(format!("invalid {name}")))
    } else {
        Ok(())
    }
}

#[derive(Clone)]
pub struct GeneratedStructuralFamily {
    family_id: String,
    candidates: Vec<StructuralCandidate>,
    geometry_by_symbol: BTreeMap<String, Vec<SeriesGeometry>>,
    sequence_source: SourceId,
}

impl GeneratedStructuralFamily {
    pub fn new(spec: &StructuralSearchSpec) -> Result<(Self, StructuralGeneration), ResearchError> {
        let generation = spec.generate()?;
        Ok((
            Self {
                family_id: spec.family_id.clone(),
                candidates: generation.candidates.clone(),
                geometry_by_symbol: spec.geometry_by_symbol.clone(),
                sequence_source: spec.sequence_source.clone(),
            },
            generation,
        ))
    }
}

impl StrategyFamily for GeneratedStructuralFamily {
    type Params = StructuralCandidate;

    fn family_id(&self) -> &str {
        &self.family_id
    }

    fn points(&self) -> Vec<Self::Params> {
        self.candidates.clone()
    }

    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding {
        ParameterBinding::new([("structure", ParameterValue::Choice(point.label.clone()))])
    }

    fn config(&self, point: &Self::Params) -> StrategyConfig {
        point.document.clone()
    }

    fn geometry(&self, symbol: &str, _: &Self::Params) -> Vec<SeriesGeometry> {
        self.geometry_by_symbol
            .get(symbol)
            .cloned()
            .unwrap_or_default()
    }

    fn bindings(
        &self,
        symbol: &str,
        point: &Self::Params,
        requirements: &qs_strategy::ConfiguredStrategyRequirements,
    ) -> Result<ConfiguredHistoricalBindings, String> {
        let base =
            ConfiguredHistoricalBindings::from_geometry(self.geometry(symbol, point), requirements)
                .map_err(|error| error.to_string())?;
        if requirements.named_inputs.is_empty() {
            return Ok(base);
        }
        let series_id = base
            .sources()
            .iter()
            .find(|binding| binding.source() == &self.sequence_source)
            .map(|binding| binding.series().requirement().id().clone())
            .ok_or_else(|| {
                "structural named inputs require the sequence source geometry".to_string()
            })?;
        let named = requirements
            .named_inputs
            .iter()
            .map(|requirement| {
                let kind = match requirement.name.as_str() {
                    "source_ordinal" => SourceBarFactKind::Ordinal,
                    "source_open_time" => SourceBarFactKind::OpenTime,
                    "source_close_time" => SourceBarFactKind::CloseTime,
                    "source_available_at" => SourceBarFactKind::AvailableAt,
                    "source_gap_before" => SourceBarFactKind::GapBefore,
                    name => {
                        return Err(format!(
                            "structural generated family has no trusted projector for '{name}'"
                        ));
                    }
                };
                Ok(ConfiguredNamedInputBinding::new(
                    requirement.name.clone(),
                    Box::new(SourceBarFactProjector::new(series_id.clone(), kind)),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(ConfiguredHistoricalBindings::new(
            base.sources().to_vec(),
            named,
            base.volume(),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationRole {
    Search,
    Validation,
    Final,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenSelection {
    pub candidate: CandidateRecipe,
    pub experiment: ExperimentRecipe,
    pub caller_revision: String,
    pub future_horizon_millis: Option<u64>,
    pub embargo_millis: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitAccessKind {
    Release,
    Access,
}

fn access_kind() -> SplitAccessKind {
    SplitAccessKind::Access
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitAccessRecord {
    #[serde(default = "access_kind")]
    pub kind: SplitAccessKind,
    pub role: EvaluationRole,
    pub from: NaiveDateTime,
    pub to: NaiveDateTime,
    pub caller_revision: String,
    pub rerun: bool,
    pub post_test_tuning: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ProtectedExperiment {
    frozen: Option<FrozenSelection>,
    final_released: bool,
    records: Vec<SplitAccessRecord>,
}

impl ProtectedExperiment {
    pub fn restore(
        selection: FrozenSelection,
        records: Vec<SplitAccessRecord>,
    ) -> Result<Self, ResearchError> {
        let mut protected = Self::default();
        protected.freeze(selection)?;
        for record in &records {
            if record.to <= record.from || record.caller_revision.is_empty() {
                return Err(ResearchError::InvalidPlan(
                    "invalid persisted split access record".into(),
                ));
            }
            if record.kind == SplitAccessKind::Release && record.role != EvaluationRole::Final {
                return Err(ResearchError::InvalidPlan(
                    "only final input can have a release record".into(),
                ));
            }
        }
        protected.final_released = records
            .iter()
            .any(|record| record.kind == SplitAccessKind::Release);
        protected.records = records;
        Ok(protected)
    }

    pub fn freeze(&mut self, selection: FrozenSelection) -> Result<(), ResearchError> {
        if self.final_released {
            return Err(ResearchError::InvalidPlan(
                "cannot replace a selection after final release".into(),
            ));
        }
        if let (Some(horizon), Some(embargo)) =
            (selection.future_horizon_millis, selection.embargo_millis)
            && embargo < horizon
        {
            return Err(ResearchError::InvalidPlan(
                "strict embargo is shorter than future horizon".into(),
            ));
        }
        self.frozen = Some(selection);
        Ok(())
    }

    pub fn release_final(&mut self) -> Result<(), ResearchError> {
        if self.frozen.is_none() {
            return Err(ResearchError::InvalidPlan(
                "final release requires frozen selection".into(),
            ));
        }
        self.final_released = true;
        Ok(())
    }

    pub fn release_final_for(
        &mut self,
        window: &DataWindow,
        caller_revision: &str,
    ) -> Result<(), ResearchError> {
        let frozen = self.frozen.as_ref().ok_or_else(|| {
            ResearchError::InvalidPlan("final release requires frozen selection".into())
        })?;
        if frozen.future_horizon_millis.is_none() {
            return Err(ResearchError::InvalidPlan(
                "strict final release requires a declared future horizon".into(),
            ));
        }
        if self.final_released {
            return Err(ResearchError::InvalidPlan(
                "final input is already released".into(),
            ));
        }
        self.final_released = true;
        self.records.push(SplitAccessRecord {
            kind: SplitAccessKind::Release,
            role: EvaluationRole::Final,
            from: window.from(),
            to: window.to(),
            caller_revision: caller_revision.into(),
            rerun: false,
            post_test_tuning: caller_revision != frozen.caller_revision,
        });
        Ok(())
    }

    pub fn access(
        &mut self,
        role: EvaluationRole,
        window: &DataWindow,
        caller_revision: &str,
        rerun: bool,
    ) -> Result<(), ResearchError> {
        let frozen = self.frozen.as_ref();
        if role == EvaluationRole::Final && frozen.is_none() {
            return Err(ResearchError::InvalidPlan(
                "final access requires frozen selection".into(),
            ));
        }
        if role == EvaluationRole::Final && !self.final_released {
            return Err(ResearchError::InvalidPlan(
                "final input is not released".into(),
            ));
        }
        if role == EvaluationRole::Final
            && frozen.is_some_and(|selection| selection.future_horizon_millis.is_none())
        {
            return Err(ResearchError::InvalidPlan(
                "strict final access requires a declared future horizon".into(),
            ));
        }
        self.records.push(SplitAccessRecord {
            kind: SplitAccessKind::Access,
            role,
            from: window.from(),
            to: window.to(),
            caller_revision: caller_revision.into(),
            rerun,
            post_test_tuning: frozen
                .is_some_and(|selection| caller_revision != selection.caller_revision),
        });
        Ok(())
    }

    pub fn records(&self) -> &[SplitAccessRecord] {
        &self.records
    }

    pub fn frozen(&self) -> Option<&FrozenSelection> {
        self.frozen.as_ref()
    }

    pub fn is_final_released(&self) -> bool {
        self.final_released
    }
}

pub fn strict_embargo(window: &DataWindow, horizon: Duration) -> Result<DataWindow, ResearchError> {
    if horizon < Duration::zero() {
        return Err(ResearchError::InvalidPlan(
            "future horizon must be nonnegative".into(),
        ));
    }
    let to = window
        .to()
        .checked_sub_signed(horizon)
        .ok_or_else(|| ResearchError::InvalidWindow("embargo underflowed".into()))?;
    DataWindow::new(window.label().to_owned(), window.from(), to)
}
