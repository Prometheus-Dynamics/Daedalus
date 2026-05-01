use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{TypeExpr, Value};

/// Trait implemented by converters between value types.
///
pub trait Converter: Send + Sync {
    fn id(&self) -> ConverterId;
    fn input(&self) -> &TypeExpr;
    fn output(&self) -> &TypeExpr;
    fn cost(&self) -> u64;
    fn feature_flags(&self) -> &[String] {
        &[]
    }
    fn requires_gpu(&self) -> bool {
        false
    }
    fn convert(&self, value: Value) -> DataResult<Value>;
}

/// Identifier for a converter edge in the graph.
///
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ConverterId(pub String);

/// Provenance for a conversion path.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConversionProvenance {
    pub steps: Vec<ConverterId>,
    pub total_cost: u64,
    pub skipped_cycles: Vec<ConverterId>,
    pub skipped_gpu: Vec<ConverterId>,
    pub skipped_features: Vec<ConverterId>,
}

/// Result of resolving a conversion path.
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConversionResolution {
    pub provenance: ConversionProvenance,
}

impl ConversionResolution {
    /// Render human-readable notes about skipped edges during resolution.
    pub fn notes(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.provenance.skipped_cycles.is_empty() {
            out.push(format!(
                "skipped cycles: {:?}",
                self.provenance.skipped_cycles
            ));
        }
        if !self.provenance.skipped_gpu.is_empty() {
            out.push(format!(
                "skipped GPU-only converters: {:?}",
                self.provenance.skipped_gpu
            ));
        }
        if !self.provenance.skipped_features.is_empty() {
            out.push(format!(
                "skipped converters missing features: {:?}",
                self.provenance.skipped_features
            ));
        }
        out
    }
}

#[derive(Clone, Debug)]
struct Edge {
    to: TypeExpr,
    id: ConverterId,
    cost: u64,
    feature_flags: Vec<String>,
    requires_gpu: bool,
}

/// Converter graph with deterministic resolver.
///
#[derive(Default)]
pub struct ConverterGraph {
    converters: BTreeMap<ConverterId, Box<dyn Converter>>,
    adjacency: BTreeMap<TypeExpr, BTreeSet<Edge>>,
}

/// Thread-safe wrapper type for concurrent registration/resolution.
///
pub type SharedConverterGraph = std::sync::Arc<std::sync::RwLock<ConverterGraph>>;

impl ConverterGraph {
    /// Create an empty converter graph.
    pub fn new() -> Self {
        Self {
            converters: BTreeMap::new(),
            adjacency: BTreeMap::new(),
        }
    }

    /// Register a converter into the graph.
    pub fn register(&mut self, converter: Box<dyn Converter>) {
        let id = converter.id();
        let input = converter.input().clone().normalize();
        let mut flags = converter.feature_flags().to_vec();
        flags.sort();
        let edge = Edge {
            to: converter.output().clone().normalize(),
            id: id.clone(),
            cost: converter.cost(),
            feature_flags: flags,
            requires_gpu: converter.requires_gpu(),
        };
        self.adjacency.entry(input).or_default().insert(edge);
        self.converters.insert(id, converter);
    }

    /// Resolve a conversion path using default context.
    pub fn resolve(&self, from: &TypeExpr, to: &TypeExpr) -> DataResult<ConversionResolution> {
        self.resolve_with_context(from, to, &[], true)
    }

    /// Resolve a conversion path with feature/GPU constraints.
    pub fn resolve_with_context(
        &self,
        from: &TypeExpr,
        to: &TypeExpr,
        active_features: &[String],
        allow_gpu: bool,
    ) -> DataResult<ConversionResolution> {
        let from = from.clone().normalize();
        let to = to.clone().normalize();
        if from == to {
            return Ok(ConversionResolution {
                provenance: ConversionProvenance {
                    steps: Vec::new(),
                    total_cost: 0,
                    skipped_cycles: Vec::new(),
                    skipped_gpu: Vec::new(),
                    skipped_features: Vec::new(),
                },
            });
        }

        // Dijkstra with deterministic tie-breaking (BTree and Reverse heap). Keep only
        // predecessor links in the heap state so resolving large graphs does not clone a full
        // path and visited set for every candidate edge.
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
        struct HeapEntry {
            cost: Reverse<u64>,
            node: TypeExpr,
        }
        let active_features: BTreeSet<&str> = active_features.iter().map(String::as_str).collect();
        let mut dist: BTreeMap<TypeExpr, u64> = BTreeMap::new();
        let mut prev: BTreeMap<TypeExpr, (TypeExpr, ConverterId)> = BTreeMap::new();
        let mut settled: BTreeSet<TypeExpr> = BTreeSet::new();
        let mut skipped_cycles = BTreeSet::new();
        let mut skipped_gpu = BTreeSet::new();
        let mut skipped_features = BTreeSet::new();
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        dist.insert(from.clone(), 0);
        heap.push(HeapEntry {
            cost: Reverse(0),
            node: from.clone(),
        });

        while let Some(HeapEntry {
            cost: Reverse(cost),
            node,
        }) = heap.pop()
        {
            if let Some(known) = dist.get(&node)
                && *known < cost
            {
                continue;
            }
            if !settled.insert(node.clone()) {
                continue;
            }
            if node == to {
                let mut steps = Vec::new();
                let mut cursor = to.clone();
                while cursor != from {
                    let Some((parent, converter)) = prev.get(&cursor) else {
                        break;
                    };
                    steps.push(converter.clone());
                    cursor = parent.clone();
                }
                steps.reverse();
                return Ok(ConversionResolution {
                    provenance: ConversionProvenance {
                        total_cost: cost,
                        steps,
                        skipped_cycles: skipped_cycles.into_iter().collect(),
                        skipped_gpu: skipped_gpu.into_iter().collect(),
                        skipped_features: skipped_features.into_iter().collect(),
                    },
                });
            }

            if let Some(edges) = self.adjacency.get(&node) {
                for edge in edges {
                    if edge.requires_gpu && !allow_gpu {
                        skipped_gpu.insert(edge.id.clone());
                        continue;
                    }
                    if !edge
                        .feature_flags
                        .iter()
                        .all(|f| active_features.contains(f.as_str()))
                    {
                        skipped_features.insert(edge.id.clone());
                        continue;
                    }
                    if settled.contains(&edge.to) {
                        skipped_cycles.insert(edge.id.clone());
                        continue; // skip cycles, keep searching other paths
                    }
                    let next_cost = cost.saturating_add(edge.cost);
                    let entry = dist.get(&edge.to);
                    let should_update = entry.map(|c| next_cost < *c).unwrap_or(true);
                    if should_update {
                        dist.insert(edge.to.clone(), next_cost);
                        prev.insert(edge.to.clone(), (node.clone(), edge.id.clone()));
                        heap.push(HeapEntry {
                            cost: Reverse(next_cost),
                            node: edge.to.clone(),
                        });
                    }
                }
            }
        }

        let mut notes = Vec::new();
        if !skipped_gpu.is_empty() {
            notes.push(format!(
                "skipped GPU-only converters: {:?}",
                skipped_gpu.into_iter().collect::<Vec<_>>()
            ));
        }
        if !skipped_features.is_empty() {
            notes.push(format!(
                "skipped converters missing features: {:?}",
                skipped_features.into_iter().collect::<Vec<_>>()
            ));
        }
        if !skipped_cycles.is_empty() {
            notes.push(format!(
                "skipped cycles: {:?}",
                skipped_cycles.into_iter().collect::<Vec<_>>()
            ));
        }
        let message = if notes.is_empty() {
            "no conversion path found".to_string()
        } else {
            format!("no conversion path found ({})", notes.join("; "))
        };
        Err(DataError::new(DataErrorCode::UnknownConverter, message))
    }

    /// Async wrapper around `resolve_with_context`, feature-gated.
    #[cfg(feature = "async")]
    pub async fn resolve_with_context_async(
        &self,
        from: &TypeExpr,
        to: &TypeExpr,
        active_features: &[String],
        allow_gpu: bool,
    ) -> DataResult<ConversionResolution> {
        self.resolve_with_context(from, to, active_features, allow_gpu)
    }

    /// Async wrapper around `resolve`, feature-gated.
    #[cfg(feature = "async")]
    pub async fn resolve_async(
        &self,
        from: &TypeExpr,
        to: &TypeExpr,
    ) -> DataResult<ConversionResolution> {
        self.resolve(from, to)
    }
}

/// Builder that turns a closure into a `Converter`.
///
pub struct ConverterBuilder<F>
where
    F: Fn(Value) -> DataResult<Value> + Send + Sync + 'static,
{
    id: ConverterId,
    input: TypeExpr,
    output: TypeExpr,
    cost: u64,
    feature_flags: Vec<String>,
    requires_gpu: bool,
    func: F,
}

impl<F> ConverterBuilder<F>
where
    F: Fn(Value) -> DataResult<Value> + Send + Sync + 'static,
{
    pub fn new(id: impl Into<String>, input: TypeExpr, output: TypeExpr, func: F) -> Self {
        Self {
            id: ConverterId(id.into()),
            input: input.normalize(),
            output: output.normalize(),
            cost: 1,
            feature_flags: Vec::new(),
            requires_gpu: false,
            func,
        }
    }

    pub fn cost(mut self, cost: u64) -> Self {
        self.cost = cost;
        self
    }

    pub fn feature_flag(mut self, flag: impl Into<String>) -> Self {
        self.feature_flags.push(flag.into());
        self
    }

    pub fn requires_gpu(mut self, requires: bool) -> Self {
        self.requires_gpu = requires;
        self
    }

    pub fn build(self) -> FnConverter<F> {
        let mut flags = self.feature_flags;
        flags.sort();
        FnConverter {
            id: self.id,
            input: self.input,
            output: self.output,
            cost: self.cost,
            feature_flags: flags,
            requires_gpu: self.requires_gpu,
            func: self.func,
        }
    }

    pub fn build_boxed(self) -> Box<dyn Converter> {
        Box::new(self.build())
    }
}

/// Converter implementation backed by a closure.
///
pub struct FnConverter<F>
where
    F: Fn(Value) -> DataResult<Value> + Send + Sync + 'static,
{
    id: ConverterId,
    input: TypeExpr,
    output: TypeExpr,
    cost: u64,
    feature_flags: Vec<String>,
    requires_gpu: bool,
    func: F,
}

impl<F> Converter for FnConverter<F>
where
    F: Fn(Value) -> DataResult<Value> + Send + Sync + 'static,
{
    fn id(&self) -> ConverterId {
        self.id.clone()
    }

    fn input(&self) -> &TypeExpr {
        &self.input
    }

    fn output(&self) -> &TypeExpr {
        &self.output
    }

    fn cost(&self) -> u64 {
        self.cost
    }

    fn feature_flags(&self) -> &[String] {
        &self.feature_flags
    }

    fn requires_gpu(&self) -> bool {
        self.requires_gpu
    }

    fn convert(&self, value: Value) -> DataResult<Value> {
        (self.func)(value)
    }
}

impl Ord for Edge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.cost, &self.id).cmp(&(other.cost, &other.id))
    }
}

impl PartialOrd for Edge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Edge {}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
