//! Planner output -> device plan: schedule-ordered nodes, edges with queue capacities, typed
//! constants and host ports, with every MCU restriction checked here (on the host).

use std::collections::BTreeMap;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_mcu::{NodeDesc, Overflow};
use daedalus_planner::{
    DiagnosticCode, EdgeResolutionExplanation, Graph, PlannerConfig, PlannerInput, build_plan,
    edge_explanations, is_host_bridge_metadata,
};
use daedalus_registry::{transport_key_typeexpr, typeexpr_transport_key};
use daedalus_runtime::plugins::PluginRegistry;
use daedalus_runtime::{NodeFire, RuntimePlan, SchedulerConfig, build_runtime};
use daedalus_transport::{FreshnessPolicy, OverflowPolicy, PressurePolicy};

use crate::{CompileError, CompileOptions};

/// The compact device plan rendered by [`McuPlan::to_rust`].
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
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanNode {
    /// Graph label, else registry id (`NODE_IDS` in the generated code).
    pub name: String,
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
    /// A graph constant, as a typed Rust expression.
    Const {
        expr: String,
        required: bool,
    },
    /// An unconnected optional input (`None`).
    Absent,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlanEdge {
    /// `from.port -> to.port`, for diagnostics.
    pub label: String,
    /// Rust type of the queued values (the consumer's port type).
    pub ty: String,
    pub capacity: usize,
    pub overflow: Overflow,
    /// The producer's values go through the builtin widening `From` conversion.
    pub widen: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HostPort {
    pub name: String,
    /// Rust type the application pushes or pops.
    pub ty: String,
    /// Edges fed by (host input) or feeding (host output: exactly one) this port.
    pub edges: Vec<usize>,
}

pub(crate) fn lower(
    graph: Graph,
    descs: &[NodeDesc],
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

    let types = TypeNames::new(descs);
    let desc_of = |index: usize| -> Result<Option<&NodeDesc>, CompileError> {
        let node = &graph.nodes[index];
        if is_host_bridge_metadata(&node.metadata) {
            return Ok(None);
        }
        descs
            .iter()
            .find(|desc| desc.id == node.id.0)
            .map(Some)
            .ok_or_else(|| unsupported(format!("node `{}` is not a device node", node.id.0)))
    };
    let name_of = |index: usize| {
        let node = &graph.nodes[index];
        node.label.clone().unwrap_or_else(|| node.id.0.clone())
    };
    if graph
        .nodes
        .iter()
        .filter(|n| is_host_bridge_metadata(&n.metadata))
        .count()
        > 1
    {
        return Err(unsupported("more than one host bridge node".into()));
    }

    let mut edges = Vec::with_capacity(graph.edges.len());
    let mut producer_types = Vec::with_capacity(graph.edges.len());
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
        producer_types.push(&explanation.from_type);
        edges.push(PlanEdge {
            ty: types.rust(&explanation.to_type)?,
            widen: widens(explanation)
                .map_err(|why| unsupported(format!("edge {label}: {why}")))?,
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
    let (mut host_inputs, mut host_outputs) = (Vec::new(), Vec::new());
    for (index, node) in graph.nodes.iter().enumerate() {
        let Some(desc) = desc_of(index)? else {
            for (port, from) in ports_used(graph, index) {
                if from {
                    // `ports_used` only lists ports with edges.
                    let edges_out = outgoing(index, &port);
                    let ty = types.rust(producer_types[edges_out[0]])?;
                    host_inputs.push(HostPort {
                        name: port,
                        ty,
                        edges: edges_out,
                    });
                } else {
                    let edge = incoming(index, &port)?.unwrap_or_default();
                    let ty = edges[edge].ty.clone();
                    host_outputs.push(HostPort {
                        name: port,
                        ty,
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
        let mut inputs = Vec::with_capacity(desc.inputs.len());
        for port in desc.inputs {
            let required = !port.optional;
            let constant = node.const_inputs.iter().find(|(name, _)| name == port.name);
            inputs.push(match (incoming(index, port.name)?, constant) {
                (Some(_), Some(_)) => {
                    return Err(unsupported(format!(
                        "`{}.{}` has both an edge and a constant",
                        name_of(index),
                        port.name
                    )));
                }
                (Some(edge), None) => InputSource::Edge { edge, required },
                (None, Some((_, value))) => InputSource::Const {
                    expr: const_expr(port.key, value).map_err(|why| {
                        unsupported(format!(
                            "constant `{}.{}`: {why}",
                            name_of(index),
                            port.name
                        ))
                    })?,
                    required,
                },
                (None, None) if port.optional => InputSource::Absent,
                (None, None) => {
                    return Err(CompileError::Planner(format!(
                        "required input `{}.{}` is not connected and has no constant",
                        name_of(index),
                        port.name
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
                path: desc.path.into(),
                wait_all: has_required_edge
                    && NodeFire::from_metadata(&node.metadata) == NodeFire::All,
                inputs,
                outputs: desc
                    .outputs
                    .iter()
                    .map(|port| outgoing(index, port.name))
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
                && !is_host_bridge_metadata(&graph.nodes[graph.edges[edge].from.node.0].metadata)
            {
                edges[edge].capacity = 1;
                edges[edge].overflow = Overflow::DropOldest;
            }
        }
    }

    let order: Vec<usize> = runtime.schedule_order.iter().map(|node| node.0).collect();
    nodes.sort_by_key(|(index, _)| order.iter().position(|o| o == index));
    Ok(McuPlan {
        hash: plan.hash.0,
        nodes: nodes.into_iter().map(|(_, node)| node).collect(),
        edges,
        host_inputs,
        host_outputs,
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
struct TypeNames(BTreeMap<String, String>);

impl TypeNames {
    fn new(descs: &[NodeDesc]) -> Self {
        let mut names = BTreeMap::new();
        for desc in descs {
            let ports = [("In", desc.inputs), ("Out", desc.outputs)];
            for (prefix, ports) in ports {
                for (k, port) in ports.iter().enumerate() {
                    names
                        .entry(port.key.to_string())
                        .or_insert_with(|| format!("::{}::{prefix}{k}", desc.path));
                }
            }
        }
        Self(names)
    }

    fn rust(&self, ty: &TypeExpr) -> Result<String, CompileError> {
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
}

/// A typed literal for constant `value` on a port of type `key` (builtin scalars only; the
/// planner has already checked the value fits).
fn const_expr(key: &str, value: &Value) -> Result<String, String> {
    let TypeExpr::Scalar(scalar) = transport_key_typeexpr(&daedalus_transport::TypeKey::new(key))
    else {
        return Err(format!("constants of type `{key}`"));
    };
    let name = scalar.rust_name();
    let float = matches!(scalar, ValueType::F32 | ValueType::Float);
    Ok(match (scalar, value) {
        (ValueType::Unit, Value::Unit) => "()".into(),
        (ValueType::Bool, Value::Bool(b)) => b.to_string(),
        (_, Value::Int(i)) if scalar.is_numeric() => format!("{i}_{name}"),
        (_, Value::Float(f)) if float && f.is_finite() => format!("{f:?}_{name}"),
        _ => return Err(format!("{value:?} for a `{name}` port")),
    })
}
