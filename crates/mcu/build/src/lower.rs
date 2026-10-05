//! Planner output -> device plan: schedule-ordered nodes, edges with queue capacities, typed
//! constants and parameters, and host ports, with every MCU restriction checked here (on the
//! host). Compiled code ([`McuPlan::to_rust`]) and loaded blobs ([`McuPlan::to_blob`]) are both
//! rendered from this plan.

use std::collections::BTreeMap;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_mcu::{Overflow, Scalar, ScalarKind};
use daedalus_planner::{
    DiagnosticCode, EdgeResolutionExplanation, Graph, PlannerConfig, PlannerInput, build_plan,
    edge_explanations, is_host_bridge_metadata,
};
use daedalus_registry::{transport_key_typeexpr, typeexpr_transport_key};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{NodeFire, RuntimePlan, SchedulerConfig, build_runtime};
use daedalus_transport::{FreshnessPolicy, OverflowPolicy, PressurePolicy, TypeKey};

use crate::{CompileError, CompileOptions, NodeSpec};

/// Node metadata marking constants as tunable parameters: a map from input name to its range
/// (`[min, max]`, or unit for the type's full range), or a list of input names.
pub const PARAMS_META_KEY: &str = "daedalus.mcu.params";

/// The device plan: rendered as Rust ([`McuPlan::to_rust`]) or as a blob
/// ([`McuPlan::to_blob`]).
#[derive(Clone, Debug, PartialEq)]
pub struct McuPlan {
    /// Stable hash of the planner's execution plan.
    pub hash: u64,
    /// Nodes in schedule order (the host bridge excluded).
    pub nodes: Vec<PlanNode>,
    /// Edge queues, in graph edge order (host edges included).
    pub edges: Vec<PlanEdge>,
    /// Ports the application pushes into (host bridge outputs).
    pub host_inputs: Vec<HostPort>,
    /// Ports the application pops from (host bridge inputs).
    pub host_outputs: Vec<HostPort>,
    /// Tunable constants, by id.
    pub params: Vec<PlanParam>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanNode {
    /// Graph label, else registry id (`NODE_IDS` in the generated code).
    pub name: String,
    /// Index of the node's declaration in the node list (the loaded-mode library entry).
    pub entry: usize,
    /// Module path of the node's generated glue (`NodeDesc::path`).
    pub path: String,
    /// Fire mode `all` with at least one connected required input.
    pub wait_all: bool,
    /// One source per declared input, in declaration order.
    pub inputs: Vec<InputSource>,
    /// Outgoing edge indices per declared output.
    pub outputs: Vec<Vec<usize>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InputSource {
    Edge {
        edge: usize,
        required: bool,
    },
    /// A graph constant: its scalar value, if it is one, and a typed Rust expression.
    Const {
        value: Option<Scalar>,
        expr: String,
        required: bool,
    },
    /// A graph constant marked tunable: parameter `param`.
    Param {
        param: usize,
        required: bool,
    },
    /// An unconnected optional input (`None`).
    Absent,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanEdge {
    /// `from.port -> to.port`, for diagnostics.
    pub label: String,
    pub from: Endpoint,
    /// Rust type of the queued values (the consumer's port type).
    pub ty: String,
    pub capacity: usize,
    pub overflow: Overflow,
    /// The producer's values go through the builtin widening `From` conversion.
    pub widen: bool,
}

/// Where an edge's values come from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Host { port: usize },
    Node { node: usize, output: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub struct HostPort {
    pub name: String,
    /// Rust type the application pushes or pops.
    pub ty: String,
    /// Transport key of that type.
    pub key: String,
    /// Edges fed by (host input) or feeding (host output: exactly one) this port.
    pub edges: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanParam {
    /// `<node>.<input>`.
    pub name: String,
    /// Plan node and input index.
    pub node: usize,
    pub input: usize,
    /// The graph constant, the initial value.
    pub value: Scalar,
    pub min: Scalar,
    pub max: Scalar,
}

pub(crate) fn lower(
    graph: Graph,
    specs: &[NodeSpec],
    registry: &PluginRegistry,
    options: &CompileOptions,
) -> Result<McuPlan, CompileError> {
    let config = registry
        .planner_config_with_transport(PlannerConfig::default())
        .map_err(|err| CompileError::Planner(err.to_string()))?;
    let output = build_plan(PlannerInput { graph }, config);
    let errors: Vec<String> = output
        .diagnostics
        .iter()
        .filter(|d| d.code != DiagnosticCode::LintWarning)
        .map(|d| format!("  [{:?}] {}", d.code, d.message))
        .collect();
    if !errors.is_empty() {
        return Err(CompileError::Planner(errors.join("\n")));
    }
    let plan = output.plan;
    RuntimePlan::try_from_execution(&plan).map_err(|err| CompileError::Planner(err.to_string()))?;
    let runtime = build_runtime(&plan, &SchedulerConfig::default());
    let graph = &plan.graph;
    let explanations = edge_explanations(&graph.metadata);

    let types = TypeNames::new(specs);
    let spec_of = |index: usize| -> Result<Option<(usize, &NodeSpec)>, CompileError> {
        let node = &graph.nodes[index];
        if is_host_bridge_metadata(&node.metadata) {
            return Ok(None);
        }
        specs
            .iter()
            .enumerate()
            .find(|(_, spec)| spec.id == node.id.0)
            .map(Some)
            .ok_or_else(|| unsupported(format!("node `{}` is not a device node", node.id.0)))
    };
    let name_of = |index: usize| {
        let node = &graph.nodes[index];
        node.label.clone().unwrap_or_else(|| node.id.0.clone())
    };
    let hosts: Vec<usize> = (0..graph.nodes.len())
        .filter(|&i| is_host_bridge_metadata(&graph.nodes[i].metadata))
        .collect();
    if hosts.len() > 1 {
        return Err(unsupported("more than one host bridge node".into()));
    }
    let order: Vec<usize> = runtime.schedule_order.iter().map(|node| node.0).collect();
    let rank = |index: usize| order.iter().position(|&o| o == index);

    let mut edges = Vec::with_capacity(graph.edges.len());
    // Resolved (producer, consumer) types per edge.
    let mut edge_types = Vec::with_capacity(graph.edges.len());
    for (index, (edge, runtime_edge)) in graph.edges.iter().zip(&runtime.edges).enumerate() {
        let label = format!(
            "{}.{} -> {}.{}",
            name_of(edge.from.node.0),
            edge.from.port,
            name_of(edge.to.node.0),
            edge.to.port
        );
        let explanation = explanations
            .iter()
            .find(|e| {
                e.from_node == graph.nodes[edge.from.node.0].id.0
                    && e.from_port == edge.from.port
                    && e.to_node == graph.nodes[edge.to.node.0].id.0
                    && e.to_port == edge.to.port
            })
            .ok_or_else(|| unsupported(format!("edge {index} ({label}) has no resolved types")))?;
        let (capacity, overflow) = queue_shape(runtime_edge.policy(), options)
            .map_err(|why| unsupported(format!("edge {label}: {why}")))?;
        let from = edge.from.node.0;
        let from = match spec_of(from)? {
            None => Endpoint::Host {
                port: ports_used(graph, from)
                    .iter()
                    .filter(|(_, out)| *out)
                    .position(|(port, _)| *port == edge.from.port)
                    .unwrap_or_default(),
            },
            Some((_, spec)) => Endpoint::Node {
                // Renumbered to plan order below.
                node: from,
                output: spec
                    .outputs
                    .iter()
                    .position(|p| p.name.eq_ignore_ascii_case(&edge.from.port))
                    .ok_or_else(|| unsupported(format!("edge {label}: unknown output")))?,
            },
        };
        edge_types.push((&explanation.from_type, &explanation.to_type));
        edges.push(PlanEdge {
            ty: types.rust(&explanation.to_type)?,
            widen: widens(explanation)
                .map_err(|why| unsupported(format!("edge {label}: {why}")))?,
            from,
            label,
            capacity,
            overflow,
        });
    }

    let incoming = |node: usize, port: &str| -> Result<Option<usize>, CompileError> {
        let mut found = graph
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.to.node.0 == node && e.to.port.eq_ignore_ascii_case(port));
        let first = found.next().map(|(i, _)| i);
        if found.next().is_some() {
            return Err(unsupported(format!(
                "several edges into `{}.{port}` (fan-in)",
                name_of(node)
            )));
        }
        Ok(first)
    };
    let outgoing = |node: usize, port: &str| -> Vec<usize> {
        graph
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.from.node.0 == node && e.from.port.eq_ignore_ascii_case(port))
            .map(|(i, _)| i)
            .collect()
    };

    let mut nodes = Vec::new();
    let mut params = Vec::new();
    let (mut host_inputs, mut host_outputs) = (Vec::new(), Vec::new());
    for (index, node) in graph.nodes.iter().enumerate() {
        let Some((entry, spec)) = spec_of(index)? else {
            for (port, from) in ports_used(graph, index) {
                if from {
                    // `ports_used` only lists ports with edges.
                    let edges_out = outgoing(index, &port);
                    let producer = edge_types[edges_out[0]].0;
                    host_inputs.push(HostPort {
                        name: port,
                        ty: types.rust(producer)?,
                        key: typeexpr_transport_key(producer).to_string(),
                        edges: edges_out,
                    });
                } else {
                    let edge = incoming(index, &port)?.unwrap_or_default();
                    host_outputs.push(HostPort {
                        name: port,
                        ty: edges[edge].ty.clone(),
                        key: typeexpr_transport_key(edge_types[edge].1).to_string(),
                        edges: vec![edge],
                    });
                }
            }
            continue;
        };
        if !matches!(node.compute, daedalus_planner::ComputeAffinity::CpuOnly) {
            return Err(unsupported(format!(
                "node `{}` asks for a GPU",
                name_of(index)
            )));
        }
        let ranges = if options.freeze_params {
            BTreeMap::new()
        } else {
            param_ranges(node.metadata.get(PARAMS_META_KEY))
                .map_err(|why| unsupported(format!("`{}`: {why}", name_of(index))))?
        };
        if let Some(port) = ranges
            .keys()
            .find(|port| !spec.inputs.iter().any(|p| &p.name == *port))
        {
            return Err(unsupported(format!(
                "parameter `{}.{port}`: no such input",
                name_of(index)
            )));
        }
        let rank = rank(index).unwrap_or(usize::MAX);
        let mut inputs = Vec::with_capacity(spec.inputs.len());
        for (k, port) in spec.inputs.iter().enumerate() {
            let required = !port.optional;
            let name = format!("{}.{}", name_of(index), port.name);
            let constant = node.const_inputs.iter().find(|(n, _)| n == &port.name);
            inputs.push(match (incoming(index, &port.name)?, constant) {
                (Some(_), Some(_)) => {
                    return Err(unsupported(format!(
                        "`{name}` has both an edge and a constant"
                    )));
                }
                (Some(_), None) if ranges.contains_key(&port.name) => {
                    return Err(unsupported(format!(
                        "parameter `{name}` is connected to an edge, not a constant"
                    )));
                }
                (Some(edge), None) => InputSource::Edge { edge, required },
                (None, Some((_, value))) => {
                    let (value, expr) = constant_value(&port.key, value)
                        .map_err(|why| unsupported(format!("constant `{name}`: {why}")))?;
                    match ranges.get(&port.name) {
                        Some(range) => {
                            let value = value.ok_or_else(|| {
                                unsupported(format!("parameter `{name}`: not a scalar"))
                            })?;
                            let (min, max) = param_range(value, range)
                                .map_err(|why| unsupported(format!("parameter `{name}`: {why}")))?;
                            params.push((
                                rank,
                                k,
                                PlanParam {
                                    name,
                                    node: index,
                                    input: k,
                                    value,
                                    min,
                                    max,
                                },
                            ));
                            InputSource::Param { param: 0, required }
                        }
                        None => InputSource::Const {
                            value,
                            expr,
                            required,
                        },
                    }
                }
                (None, None) if ranges.contains_key(&port.name) => {
                    return Err(unsupported(format!("parameter `{name}` has no constant")));
                }
                (None, None) if port.optional => InputSource::Absent,
                (None, None) => {
                    return Err(CompileError::Planner(format!(
                        "required input `{name}` is not connected and has no constant"
                    )));
                }
            });
        }
        let has_required_edge = inputs
            .iter()
            .any(|input| matches!(input, InputSource::Edge { required: true, .. }));
        nodes.push((
            index,
            PlanNode {
                name: name_of(index),
                entry,
                path: spec.path.clone(),
                wait_all: has_required_edge
                    && NodeFire::from_metadata(&node.metadata) == NodeFire::All,
                inputs,
                outputs: spec
                    .outputs
                    .iter()
                    .map(|port| outgoing(index, &port.name))
                    .collect(),
            },
        ));
    }

    // A node runs at most once per tick, and a consumer in fire mode `any` drains its edges
    // every tick, so an edge between two nodes into such a consumer never holds more than one
    // value: one slot, never full, whatever its policy.
    for (_, node) in nodes.iter().filter(|(_, node)| !node.wait_all) {
        for input in &node.inputs {
            if let InputSource::Edge { edge, .. } = *input
                && matches!(edges[edge].from, Endpoint::Node { .. })
            {
                edges[edge].capacity = 1;
                edges[edge].overflow = Overflow::DropOldest;
            }
        }
    }

    // Schedule order; node indices and parameter ids follow it.
    nodes.sort_by_key(|(index, _)| rank(*index));
    let mut plan_of = vec![0; graph.nodes.len()];
    for (plan_index, (graph_index, _)) in nodes.iter().enumerate() {
        plan_of[*graph_index] = plan_index;
    }
    let plan_index = |graph_index: usize| plan_of.get(graph_index).copied();
    for edge in &mut edges {
        if let Endpoint::Node { node, .. } = &mut edge.from {
            *node = plan_index(*node).unwrap_or_default();
        }
    }
    params.sort_by_key(|(rank, k, _)| (*rank, *k));
    let params: Vec<PlanParam> = params
        .into_iter()
        .enumerate()
        .map(|(id, (_, k, mut param))| {
            param.node = plan_index(param.node).unwrap_or_default();
            if let Some((_, node)) = nodes.get_mut(param.node)
                && let InputSource::Param { param: slot, .. } = &mut node.inputs[k]
            {
                *slot = id;
            }
            param
        })
        .collect();
    Ok(McuPlan {
        hash: plan.hash.0,
        nodes: nodes.into_iter().map(|(_, node)| node).collect(),
        edges,
        host_inputs,
        host_outputs,
        params,
    })
}

fn unsupported(message: String) -> CompileError {
    CompileError::Unsupported(message)
}

/// Host bridge ports in use: `(port, true)` for edges leaving the bridge (host inputs),
/// `(port, false)` for edges entering it (host outputs), deduplicated in edge order.
fn ports_used(graph: &Graph, host: usize) -> Vec<(String, bool)> {
    let mut ports: Vec<(String, bool)> = Vec::new();
    for edge in &graph.edges {
        for (end, from) in [(&edge.from, true), (&edge.to, false)] {
            if end.node.0 == host && !ports.iter().any(|(p, f)| p == &end.port && *f == from) {
                ports.push((end.port.clone(), from));
            }
        }
    }
    ports
}

/// Queue capacity and overflow for an edge policy.
fn queue_shape(
    policy: &daedalus_runtime::RuntimeEdgePolicy,
    options: &CompileOptions,
) -> Result<(usize, Overflow), String> {
    if matches!(
        policy.freshness,
        FreshnessPolicy::LatestByTimestamp
            | FreshnessPolicy::MaxAge(_)
            | FreshnessPolicy::MaxLag { .. }
    ) {
        return Err(format!("freshness policy {:?}", policy.freshness));
    }
    let fifo = options.fifo_capacity.max(1);
    Ok(match &policy.pressure {
        PressurePolicy::LatestOnly => (1, Overflow::DropOldest),
        PressurePolicy::Bounded { capacity, overflow } => (
            (*capacity).max(1),
            match overflow {
                OverflowPolicy::DropOldest => Overflow::DropOldest,
                OverflowPolicy::DropIncoming => Overflow::DropNewest,
                // A serial device executor cannot block a producer.
                OverflowPolicy::Backpressure | OverflowPolicy::Error => Overflow::Error,
            },
        ),
        PressurePolicy::BufferAll | PressurePolicy::ErrorOnFull => (fifo, Overflow::Error),
        PressurePolicy::DropOldest => (fifo, Overflow::DropOldest),
        PressurePolicy::DropNewest => (fifo, Overflow::DropNewest),
        PressurePolicy::Coalesce { .. } => return Err("coalescing pressure policy".into()),
    })
}

/// Whether an edge converts through the builtin widening adapter; other adapters need the
/// full runtime.
fn widens(explanation: &EdgeResolutionExplanation) -> Result<bool, String> {
    match explanation.adapter_path.as_slice() {
        [] => Ok(false),
        [step] if step.adapter.as_str().starts_with("daedalus.builtin.widen.") => Ok(true),
        steps => Err(format!(
            "adapter path {:?} (only builtin numeric widening runs on the device)",
            steps.iter().map(|s| s.adapter.as_str()).collect::<Vec<_>>()
        )),
    }
}

/// Rust type names per transport key: builtin scalars by name, other keys through the
/// `In<k>`/`Out<k>` alias of a device port declaring them.
pub(crate) struct TypeNames(BTreeMap<String, String>);

impl TypeNames {
    pub(crate) fn new(specs: &[NodeSpec]) -> Self {
        let mut names = BTreeMap::new();
        for spec in specs {
            for (prefix, ports) in [("In", &spec.inputs), ("Out", &spec.outputs)] {
                for (k, port) in ports.iter().enumerate() {
                    names
                        .entry(port.key.clone())
                        .or_insert_with(|| format!("::{}::{prefix}{k}", spec.path));
                }
            }
        }
        Self(names)
    }

    pub(crate) fn rust(&self, ty: &TypeExpr) -> Result<String, CompileError> {
        if let TypeExpr::Scalar(scalar) = ty {
            return Ok(match scalar {
                ValueType::String => "::alloc::string::String".into(),
                ValueType::Bytes => "::alloc::vec::Vec<u8>".into(),
                other => other.rust_name().into(),
            });
        }
        let key = typeexpr_transport_key(ty);
        self.0
            .get(key.as_str())
            .cloned()
            .ok_or_else(|| unsupported(format!("no device port declares type `{key}`")))
    }

    /// The Rust type of a port with transport key `key`.
    pub(crate) fn of_key(&self, key: &str) -> Result<String, CompileError> {
        self.rust(&transport_key_typeexpr(&TypeKey::new(key)))
    }
}

/// The scalar kind of a transport key, if it is one.
pub(crate) fn scalar_kind(key: &str) -> Option<ScalarKind> {
    let TypeExpr::Scalar(scalar) = transport_key_typeexpr(&TypeKey::new(key)) else {
        return None;
    };
    Some(match scalar {
        ValueType::Bool => ScalarKind::Bool,
        ValueType::I8 => ScalarKind::I8,
        ValueType::I16 => ScalarKind::I16,
        ValueType::I32 => ScalarKind::I32,
        ValueType::Int => ScalarKind::I64,
        ValueType::U8 => ScalarKind::U8,
        ValueType::U16 => ScalarKind::U16,
        ValueType::U32 => ScalarKind::U32,
        ValueType::U64 => ScalarKind::U64,
        ValueType::F32 => ScalarKind::F32,
        ValueType::Float => ScalarKind::F64,
        _ => return None,
    })
}

/// A graph value as a scalar of `kind`, with the planner's constant rules.
pub fn scalar_value(kind: ScalarKind, value: &Value) -> Result<Scalar, String> {
    let scalar = match value {
        Value::Bool(b) => Scalar::Bool(*b),
        Value::Int(i) => Scalar::I64(*i),
        Value::Float(f) => Scalar::F64(*f),
        other => return Err(format!("{other:?} for a `{}` port", kind.rust_name())),
    };
    scalar
        .coerce(kind)
        .ok_or_else(|| format!("{value:?} does not fit `{}`", kind.rust_name()))
}

/// A Rust literal of `value`.
pub(crate) fn literal(value: Scalar) -> String {
    let float = |v: f64, debug: String, ty: &str| match v {
        _ if v.is_nan() => format!("{ty}::NAN"),
        f64::INFINITY => format!("{ty}::INFINITY"),
        f64::NEG_INFINITY => format!("{ty}::NEG_INFINITY"),
        _ => format!("{debug}_{ty}"),
    };
    match value {
        Scalar::Bool(b) => b.to_string(),
        Scalar::F32(v) => float(v.into(), format!("{v:?}"), "f32"),
        Scalar::F64(v) => float(v, format!("{v:?}"), "f64"),
        Scalar::I8(v) => format!("{v}_i8"),
        Scalar::I16(v) => format!("{v}_i16"),
        Scalar::I32(v) => format!("{v}_i32"),
        Scalar::I64(v) => format!("{v}_i64"),
        Scalar::U8(v) => format!("{v}_u8"),
        Scalar::U16(v) => format!("{v}_u16"),
        Scalar::U32(v) => format!("{v}_u32"),
        Scalar::U64(v) => format!("{v}_u64"),
    }
}

/// A constant on a port of type `key`: its scalar (builtin scalars) and a typed Rust literal
/// (also `()`, `isize`, `usize`; the planner has already checked the value fits).
fn constant_value(key: &str, value: &Value) -> Result<(Option<Scalar>, String), String> {
    if let Some(kind) = scalar_kind(key) {
        let scalar = scalar_value(kind, value)?;
        return Ok((Some(scalar), literal(scalar)));
    }
    let TypeExpr::Scalar(scalar) = transport_key_typeexpr(&TypeKey::new(key)) else {
        return Err(format!("constants of type `{key}`"));
    };
    let name = scalar.rust_name();
    Ok(match (scalar, value) {
        (ValueType::Unit, Value::Unit) => (None, "()".into()),
        (ValueType::ISize | ValueType::USize, Value::Int(i)) => (None, format!("{i}_{name}")),
        _ => return Err(format!("{value:?} for a `{name}` port")),
    })
}

/// The ranges of `daedalus.mcu.params`: input name -> `[min, max]` (or unit).
fn param_ranges(meta: Option<&Value>) -> Result<BTreeMap<String, Value>, String> {
    let bad = || {
        format!(
            "`{PARAMS_META_KEY}` is a map from input names to [min, max] or a list of input names"
        )
    };
    let Some(meta) = meta else {
        return Ok(BTreeMap::new());
    };
    let pairs: Vec<(&Value, Value)> = match meta {
        Value::Map(entries) => entries.iter().map(|(k, v)| (k, v.clone())).collect(),
        Value::List(names) => names.iter().map(|name| (name, Value::Unit)).collect(),
        _ => return Err(bad()),
    };
    pairs
        .into_iter()
        .map(|(name, range)| Ok((name.as_str().ok_or_else(bad)?.to_string(), range)))
        .collect()
}

/// A parameter's range: `[min, max]` converted to the value's kind, or the kind's bounds.
fn param_range(value: Scalar, range: &Value) -> Result<(Scalar, Scalar), String> {
    let kind = value.kind();
    let (min, max) = match range {
        Value::Unit => kind.bounds(),
        Value::List(bounds) | Value::Tuple(bounds) if bounds.len() == 2 => (
            scalar_value(kind, &bounds[0])?,
            scalar_value(kind, &bounds[1])?,
        ),
        other => return Err(format!("range {other:?} is not [min, max]")),
    };
    let spec = daedalus_mcu::ParamSpec { kind, min, max };
    spec.check(value)
        .map_err(|_| format!("constant {value:?} outside its range {min:?}..={max:?}"))?;
    Ok((min, max))
}
