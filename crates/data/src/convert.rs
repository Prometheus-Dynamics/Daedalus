use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};

use crate::errors::{DataError, DataErrorCode, DataResult};
use crate::model::{TypeExpr, Value};

/// Trait implemented by converters between value types.
///
/// ```
/// use daedalus_data::convert::{Converter, ConverterId};
/// use daedalus_data::errors::DataResult;
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
///
/// struct Noop;
/// impl Converter for Noop {
///     fn id(&self) -> ConverterId { ConverterId("noop".into()) }
///     fn input(&self) -> &TypeExpr { &TypeExpr::Scalar(ValueType::Int) }
///     fn output(&self) -> &TypeExpr { &TypeExpr::Scalar(ValueType::Int) }
///     fn cost(&self) -> u64 { 0 }
///     fn convert(&self, value: Value) -> DataResult<Value> { Ok(value) }
/// }
/// ```
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
/// ```
/// use daedalus_data::convert::ConverterId;
/// let id = ConverterId("rgb_to_rgba".into());
/// assert_eq!(id.0, "rgb_to_rgba");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ConverterId(pub String);

/// Provenance for a conversion path.
///
/// ```
/// use daedalus_data::convert::{ConversionProvenance, ConverterId};
/// let prov = ConversionProvenance {
///     steps: vec![ConverterId("a".into())],
///     total_cost: 1,
///     skipped_cycles: vec![],
///     skipped_gpu: vec![],
///     skipped_features: vec![],
/// };
/// assert_eq!(prov.total_cost, 1);
/// ```
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
/// ```
/// use daedalus_data::convert::{ConversionProvenance, ConversionResolution, ConverterId};
/// let res = ConversionResolution {
///     provenance: ConversionProvenance {
///         steps: vec![ConverterId("noop".into())],
///         total_cost: 0,
///         skipped_cycles: vec![],
///         skipped_gpu: vec![],
///         skipped_features: vec![],
///     },
/// };
/// assert!(res.notes().is_empty());
/// ```
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
/// ```
/// use daedalus_data::convert::{ConverterBuilder, ConverterGraph};
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
///
/// let mut graph = ConverterGraph::new();
/// graph.register(
///     ConverterBuilder::new(
///         "bool-id",
///         TypeExpr::Scalar(ValueType::Bool),
///         TypeExpr::Scalar(ValueType::Bool),
///         |v: Value| Ok(v),
///     )
///     .build_boxed(),
/// );
/// let res = graph.resolve(&TypeExpr::Scalar(ValueType::Bool), &TypeExpr::Scalar(ValueType::Bool));
/// assert!(res.is_ok());
/// ```
#[derive(Default)]
pub struct ConverterGraph {
    converters: BTreeMap<ConverterId, Box<dyn Converter>>,
    adjacency: BTreeMap<TypeExpr, BTreeSet<Edge>>,
}

/// Thread-safe wrapper type for concurrent registration/resolution.
///
/// ```
/// use daedalus_data::convert::{ConverterBuilder, ConverterGraph, SharedConverterGraph};
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
/// use std::sync::{Arc, RwLock};
///
/// let graph: SharedConverterGraph = Arc::new(RwLock::new(ConverterGraph::new()));
/// {
///     let mut g = graph.write().unwrap();
///     g.register(
///         ConverterBuilder::new(
///             "id",
///             TypeExpr::Scalar(ValueType::Bool),
///             TypeExpr::Scalar(ValueType::Bool),
///             |v| Ok(v),
///         )
///         .build_boxed(),
///     );
/// }
/// {
///     let g = graph.read().unwrap();
///     let res = g.resolve(&TypeExpr::Scalar(ValueType::Bool), &TypeExpr::Scalar(ValueType::Bool)).unwrap();
///     assert_eq!(res.provenance.total_cost, 0);
/// }
/// ```
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

        // Dijkstra with deterministic tie-breaking (BTree and Reverse heap).
        #[allow(clippy::type_complexity)]
        type HeapEntry = (
            Reverse<u64>,
            TypeExpr,
            Vec<ConverterId>,
            BTreeSet<TypeExpr>,
            ConversionProvenance,
        );
        let mut dist: BTreeMap<TypeExpr, (u64, Vec<ConverterId>, ConversionProvenance)> =
            BTreeMap::new();
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        dist.insert(
            from.clone(),
            (
                0,
                Vec::new(),
                ConversionProvenance {
                    steps: Vec::new(),
                    total_cost: 0,
                    skipped_cycles: Vec::new(),
                    skipped_gpu: Vec::new(),
                    skipped_features: Vec::new(),
                },
            ),
        );
        heap.push((
            Reverse(0),
            from.clone(),
            Vec::new(),
            {
                let mut set = BTreeSet::new();
                set.insert(from.clone());
                set
            },
            ConversionProvenance {
                steps: Vec::new(),
                total_cost: 0,
                skipped_cycles: Vec::new(),
                skipped_gpu: Vec::new(),
                skipped_features: Vec::new(),
            },
        ));

        while let Some((Reverse(cost), node, path, visited, provenance)) = heap.pop() {
            if let Some((known, _, _)) = dist.get(&node)
                && *known < cost
            {
                continue;
            }
            if node == to {
                return Ok(ConversionResolution {
                    provenance: ConversionProvenance {
                        total_cost: cost,
                        steps: path,
                        skipped_cycles: provenance.skipped_cycles,
                        skipped_gpu: provenance.skipped_gpu,
                        skipped_features: provenance.skipped_features,
                    },
                });
            }

            if let Some(edges) = self.adjacency.get(&node) {
                for edge in edges {
                    if edge.requires_gpu && !allow_gpu {
                        let mut prov = provenance.clone();
                        prov.skipped_gpu.push(edge.id.clone());
                        continue;
                    }
                    if !edge
                        .feature_flags
                        .iter()
                        .all(|f| active_features.contains(f))
                    {
                        let mut prov = provenance.clone();
                        prov.skipped_features.push(edge.id.clone());
                        continue;
                    }
                    if visited.contains(&edge.to) {
                        let mut prov = provenance.clone();
                        prov.skipped_cycles.push(edge.id.clone());
                        continue; // skip cycles, keep searching other paths
                    }
                    let next_cost = cost.saturating_add(edge.cost);
                    let mut next_path = path.clone();
                    next_path.push(edge.id.clone());
                    let mut next_prov = provenance.clone();
                    next_prov.steps = next_path.clone();
                    next_prov.total_cost = next_cost;
                    let entry = dist.get(&edge.to);
                    let should_update = entry.map(|(c, _, _)| next_cost < *c).unwrap_or(true);
                    if should_update {
                        dist.insert(
                            edge.to.clone(),
                            (next_cost, next_path.clone(), next_prov.clone()),
                        );
                        let mut next_visited = visited.clone();
                        next_visited.insert(edge.to.clone());
                        heap.push((
                            Reverse(next_cost),
                            edge.to.clone(),
                            next_path,
                            next_visited,
                            next_prov,
                        ));
                    }
                }
            }
        }

        Err(DataError::new(
            DataErrorCode::UnknownConverter,
            "no conversion path found",
        ))
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
/// ```
/// use daedalus_data::convert::{ConverterBuilder, ConverterGraph};
/// use daedalus_data::errors::{DataError, DataErrorCode};
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
///
/// let conv = ConverterBuilder::new(
///     "int_to_string",
///     TypeExpr::Scalar(ValueType::Int),
///     TypeExpr::Scalar(ValueType::String),
///     |v| match v {
///         Value::Int(i) => Ok(Value::String(i.to_string().into())),
///         _ => Err(DataError::new(DataErrorCode::InvalidType, "expected int")),
///     },
/// )
/// .cost(2u64)
/// .feature_flag("fmt")
/// .build_boxed();
///
/// let mut graph = ConverterGraph::new();
/// graph.register(conv);
/// let res = graph
///     .resolve_with_context(
///         &TypeExpr::Scalar(ValueType::Int),
///         &TypeExpr::Scalar(ValueType::String),
///         &["fmt".into()],
///         true,
///     )
///     .expect("resolution");
/// assert_eq!(res.provenance.steps.len(), 1);
/// ```
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
/// ```
/// use daedalus_data::convert::{Converter, ConverterBuilder};
/// use daedalus_data::model::{TypeExpr, Value, ValueType};
/// let conv = ConverterBuilder::new(
///     "noop",
///     TypeExpr::Scalar(ValueType::Int),
///     TypeExpr::Scalar(ValueType::Int),
///     |v: Value| Ok(v),
/// )
/// .build();
/// assert_eq!(conv.cost(), 1);
/// ```
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
mod tests {
    use super::*;
    use crate::model::{TypeExpr, Value, ValueType};
    use once_cell::sync::Lazy;
    use proptest::prelude::*;
    #[cfg(feature = "async")]
    use std::future::Future;
    #[cfg(feature = "async")]
    use std::pin::Pin;
    #[cfg(feature = "async")]
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    struct Identity {
        id: ConverterId,
        ty: TypeExpr,
    }

    impl Converter for Identity {
        fn id(&self) -> ConverterId {
            self.id.clone()
        }
        fn input(&self) -> &TypeExpr {
            &self.ty
        }
        fn output(&self) -> &TypeExpr {
            &self.ty
        }
        fn cost(&self) -> u64 {
            0
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            Ok(v)
        }
    }

    struct BoolToInt;
    impl Converter for BoolToInt {
        fn id(&self) -> ConverterId {
            ConverterId("bool_to_int".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            match v {
                Value::Bool(b) => Ok(Value::Int(if b { 1 } else { 0 })),
                _ => Err(DataError::new(DataErrorCode::InvalidType, "expected bool")),
            }
        }
    }

    struct IntToString;
    impl Converter for IntToString {
        fn id(&self) -> ConverterId {
            ConverterId("int_to_string".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::String));
            &TY
        }
        fn cost(&self) -> u64 {
            2
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            match v {
                Value::Int(i) => Ok(Value::String(i.to_string().into())),
                _ => Err(DataError::new(DataErrorCode::InvalidType, "expected int")),
            }
        }
    }

    struct GpuOnly;
    impl Converter for GpuOnly {
        fn id(&self) -> ConverterId {
            ConverterId("gpu_only".into())
        }
        fn input(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
            &TY
        }
        fn output(&self) -> &TypeExpr {
            static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Float));
            &TY
        }
        fn cost(&self) -> u64 {
            1
        }
        fn requires_gpu(&self) -> bool {
            true
        }
        fn convert(&self, v: Value) -> DataResult<Value> {
            match v {
                Value::Int(i) => Ok(Value::Float(i as f64)),
                _ => Err(DataError::new(DataErrorCode::InvalidType, "expected int")),
            }
        }
    }

    #[test]
    fn resolves_trivial_path() {
        let mut graph = ConverterGraph::new();
        let ty = TypeExpr::Scalar(ValueType::Int);
        graph.register(Box::new(Identity {
            id: ConverterId("id".into()),
            ty: ty.clone(),
        }));
        let res = graph.resolve(&ty, &ty).expect("resolve");
        assert_eq!(res.provenance.total_cost, 0u64);
        assert!(res.provenance.steps.is_empty());
    }

    #[test]
    fn resolves_multi_step_path() {
        let mut graph = ConverterGraph::new();
        graph.register(Box::new(BoolToInt));
        graph.register(Box::new(IntToString));
        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::String);
        let res = graph.resolve(&from, &to).expect("resolve");
        assert_eq!(res.provenance.total_cost, 3u64);
        assert_eq!(
            res.provenance.steps,
            vec![
                ConverterId("bool_to_int".into()),
                ConverterId("int_to_string".into())
            ]
        );
    }

    #[test]
    fn respects_gpu_flag() {
        let mut graph = ConverterGraph::new();
        graph.register(Box::new(GpuOnly));
        let from = TypeExpr::Scalar(ValueType::Int);
        let to = TypeExpr::Scalar(ValueType::Float);
        let err = graph
            .resolve_with_context(&from, &to, &[], false)
            .unwrap_err();
        assert_eq!(err.code(), DataErrorCode::UnknownConverter);
        let res = graph
            .resolve_with_context(&from, &to, &[], true)
            .expect("resolve");
        assert_eq!(res.provenance.steps, vec![ConverterId("gpu_only".into())]);
    }

    #[test]
    fn cycles_do_not_hang() {
        struct AtoB;
        impl Converter for AtoB {
            fn id(&self) -> ConverterId {
                ConverterId("a_to_b".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn cost(&self) -> u64 {
                1
            }
            fn convert(&self, v: Value) -> DataResult<Value> {
                Ok(v)
            }
        }

        struct BtoA;
        impl Converter for BtoA {
            fn id(&self) -> ConverterId {
                ConverterId("b_to_a".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
                &TY
            }
            fn cost(&self) -> u64 {
                1
            }
            fn convert(&self, v: Value) -> DataResult<Value> {
                Ok(v)
            }
        }

        let mut graph = ConverterGraph::new();
        graph.register(Box::new(AtoB));
        graph.register(Box::new(BtoA));
        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::String);
        let err = graph.resolve(&from, &to).unwrap_err();
        assert!(matches!(
            err.code(),
            DataErrorCode::UnknownConverter | DataErrorCode::CycleDetected
        ));
    }

    #[test]
    fn skips_cycles_and_finds_alternate_path() {
        struct AtoB;
        impl Converter for AtoB {
            fn id(&self) -> ConverterId {
                ConverterId("a_to_b".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn cost(&self) -> u64 {
                1
            }
            fn convert(&self, v: Value) -> DataResult<Value> {
                Ok(v)
            }
        }

        struct BtoA;
        impl Converter for BtoA {
            fn id(&self) -> ConverterId {
                ConverterId("b_to_a".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Bool));
                &TY
            }
            fn cost(&self) -> u64 {
                1
            }
            fn convert(&self, v: Value) -> DataResult<Value> {
                Ok(v)
            }
        }

        struct BtoString;
        impl Converter for BtoString {
            fn id(&self) -> ConverterId {
                ConverterId("b_to_string".into())
            }
            fn input(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::Int));
                &TY
            }
            fn output(&self) -> &TypeExpr {
                static TY: Lazy<TypeExpr> = Lazy::new(|| TypeExpr::Scalar(ValueType::String));
                &TY
            }
            fn cost(&self) -> u64 {
                5
            }
            fn convert(&self, v: Value) -> DataResult<Value> {
                Ok(v)
            }
        }

        let mut graph = ConverterGraph::new();
        graph.register(Box::new(AtoB));
        graph.register(Box::new(BtoA));
        graph.register(Box::new(BtoString));
        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::String);
        let res = graph.resolve(&from, &to).expect("resolve");
        assert_eq!(
            res.provenance.steps,
            vec![
                ConverterId("a_to_b".into()),
                ConverterId("b_to_string".into())
            ]
        );
    }

    #[test]
    fn errors_when_no_path() {
        let graph = ConverterGraph::new();
        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::String);
        let err = graph.resolve(&from, &to).unwrap_err();
        assert_eq!(err.code(), DataErrorCode::UnknownConverter);
    }

    #[test]
    fn builder_sorts_feature_flags() {
        let conv = ConverterBuilder::new(
            "id",
            TypeExpr::Scalar(ValueType::Int),
            TypeExpr::Scalar(ValueType::String),
            Ok,
        )
        .feature_flag("b")
        .feature_flag("a")
        .build();
        assert_eq!(conv.feature_flags, vec!["a", "b"]);
    }

    proptest! {
        #[test]
        fn chain_costs_are_additive(len in 1usize..6) {
            // Build a chain of unique type expressions using tuple arity to differentiate them.
            let mut graph = ConverterGraph::new();
            let types: Vec<TypeExpr> = (0..=len).map(|i| {
                let v = vec![TypeExpr::Scalar(ValueType::Int); i];
                TypeExpr::Tuple(v)
            }).collect();
            for i in 0..len {
                let input = types[i].clone();
                let output = types[i + 1].clone();
                graph.register(ConverterBuilder::new(
                    format!("c{i}"),
                    input.clone(),
                    output.clone(),
                    Ok,
                ).cost(1).build_boxed());
            }
            let res = graph.resolve(&types[0], &types[len]).expect("resolve chain");
            prop_assert_eq!(res.provenance.steps.len(), len);
            prop_assert_eq!(res.provenance.total_cost, len as u64);
        }

        #[test]
        fn feature_flag_filtering(allows in proptest::bool::ANY) {
            let mut graph = ConverterGraph::new();
            graph.register(
                ConverterBuilder::new(
                    "flagged",
                    TypeExpr::Scalar(ValueType::Int),
                    TypeExpr::Scalar(ValueType::Float),
                    Ok,
                )
                .feature_flag("feat")
                .build_boxed(),
            );
            let from = TypeExpr::Scalar(ValueType::Int);
            let to = TypeExpr::Scalar(ValueType::Float);
            let features = if allows { vec!["feat".to_string()] } else { vec![] };
            let res = graph.resolve_with_context(&from, &to, &features, true);
            prop_assert_eq!(res.is_ok(), allows);
        }
    }

    #[cfg(feature = "async")]
    fn dummy_raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            dummy_raw_waker()
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    #[cfg(feature = "async")]
    fn block_on<F: Future>(mut fut: F) -> F::Output {
        let waker: Waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(val) => return val,
                Poll::Pending => continue,
            }
        }
    }

    #[cfg(feature = "async")]
    #[test]
    fn async_resolve_matches_sync() {
        let mut graph = ConverterGraph::new();
        graph.register(
            ConverterBuilder::new(
                "id",
                TypeExpr::Scalar(ValueType::Int),
                TypeExpr::Scalar(ValueType::Int),
                Ok,
            )
            .build_boxed(),
        );
        let from = TypeExpr::Scalar(ValueType::Int);
        let to = TypeExpr::Scalar(ValueType::Int);
        let sync = graph.resolve(&from, &to).unwrap();
        let async_res = block_on(graph.resolve_async(&from, &to)).unwrap();
        assert_eq!(sync.provenance, async_res.provenance);
    }

    #[test]
    fn golden_resolution_is_stable() {
        let mut graph = ConverterGraph::new();
        graph.register(Box::new(BoolToInt));
        graph.register(Box::new(IntToString));
        let from = TypeExpr::Scalar(ValueType::Bool);
        let to = TypeExpr::Scalar(ValueType::String);
        let res = graph.resolve(&from, &to).expect("resolve");
        let json = serde_json::to_string(&res).expect("serialize");
        assert_eq!(
            json,
            r#"{"provenance":{"steps":["bool_to_int","int_to_string"],"total_cost":3,"skipped_cycles":[],"skipped_gpu":[],"skipped_features":[]}}"#
        );
    }
}
