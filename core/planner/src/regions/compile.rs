//! Compiles one region and checks what came back.

use std::collections::BTreeSet;

use daedalus_backend::{
    Backend, CompileCx, Compiled, ConstOf, HostBinding, Incoming, InnerEdge, NodeBody, PortRef,
    Refusal, Region, UnitInfo,
};
use daedalus_graph::{EdgePolicyDoc, Lit, NodeDoc};
use daedalus_registry::Registry;
use daedalus_types::{NodeIndex, PortIndex};

use super::Formed;
use super::RegionError;
use crate::assign::{Assignment, Placed};
use crate::solve::type_table::TypeTable;
use crate::solve::{Graph, Port};

pub(super) struct Compiling<'a> {
    pub graph: &'a Graph,
    pub registry: &'a Registry,
    pub types: &'a TypeTable,
    pub assignment: &'a Assignment,
    pub merge_allowed: bool,
    pub fifo_capacity: u32,
}

/// Runs the backend's compile for one region.
pub(super) fn compile_region(
    cx: &Compiling<'_>,
    formed: &Formed,
    backend: &dyn Backend,
    incoming: &[Incoming<'_>],
) -> Result<Compiled, Refusal> {
    let nodes: Vec<NodeIndex> = formed.nodes.iter().map(|&node| NodeIndex(node)).collect();
    let mut consts = Vec::new();
    let mut bodies = Vec::new();
    for &node in &formed.nodes {
        let solved = &cx.graph.nodes[node as usize];
        for (port, value) in &solved.consts {
            consts.push(ConstOf {
                node: NodeIndex(node),
                port,
                value,
            });
        }
        let entry = cx.registry.entry(&solved.doc.id);
        let body = cx.assignment.bodies[node as usize];
        if let (Some(entry), Some(body)) = (entry, body)
            && let Some(record) = entry.bodies.get(body.0 as usize)
        {
            bodies.push(NodeBody {
                node: NodeIndex(node),
                id: &solved.doc.id,
                body,
                inputs: solved.inputs.len() as u16,
                outputs: solved.outputs.len() as u16,
                merges: !opted_out(&solved.doc),
                fire_all: matches!(solved.fire, daedalus_registry::Fire::All),
                code: record.code,
                text: record.text,
            });
        }
    }
    let edges = inner_edges(cx.graph, &formed.nodes);
    let (host_inputs, host_outputs) = host_bindings(cx.graph, cx.assignment, &formed.nodes);
    let spaces = backend.memory();
    let region = Region {
        backend: formed.backend,
        nodes: &nodes,
        inputs: &formed.inputs,
        outputs: &formed.outputs,
        incoming,
        rate: formed.rate,
    };
    backend.compile(
        &region,
        &CompileCx {
            types: cx.types.exprs(),
            consts: &consts,
            spaces: &spaces,
            bodies: &bodies,
            edges: &edges,
            host_inputs: &host_inputs,
            host_outputs: &host_outputs,
            fifo_capacity: cx.fifo_capacity,
            merge_allowed: cx.merge_allowed,
        },
    )
}

/// Node metadata that forbids merging the node with its neighbours.
const NO_MERGE_KEY: &str = "daedalus.node.fusion";

fn opted_out(doc: &NodeDoc) -> bool {
    match doc.metadata.get(NO_MERGE_KEY) {
        Some(Lit::Bool(allowed)) => !allowed,
        Some(Lit::Str(value)) => matches!(value.as_str(), "never" | "false" | "off"),
        _ => false,
    }
}

fn port_at(ports: &[Port], name: &str) -> Option<PortIndex> {
    let at = ports.iter().position(|port| port.name == name)?;
    Some(PortIndex(at as u16))
}

/// The region's ports that face the host bridge: those it feeds, then those it reads.
fn host_bindings<'g>(
    graph: &'g Graph,
    assignment: &Assignment,
    nodes: &[u32],
) -> (Vec<HostBinding<'g>>, Vec<HostBinding<'g>>) {
    let is_host = |node: u32| matches!(assignment.placed[node as usize], Placed::Host);
    let port = |node: u32, at: PortIndex| PortRef {
        node: NodeIndex(node),
        port: at,
    };
    let (mut inputs, mut outputs) = (Vec::new(), Vec::new());
    for edge in &graph.edges {
        let (from, to) = (edge.from.node, edge.to.node);
        if is_host(from)
            && nodes.contains(&to)
            && let Some(at) = port_at(&graph.nodes[to as usize].inputs, &edge.to.port)
        {
            inputs.push(HostBinding {
                port: port(to, at),
                name: &edge.from.port,
                policy: &edge.policy,
            });
        }
        if is_host(to)
            && nodes.contains(&from)
            && let Some(at) = port_at(&graph.nodes[from as usize].outputs, &edge.from.port)
        {
            outputs.push(HostBinding {
                port: port(from, at),
                name: &edge.to.port,
                policy: &edge.policy,
            });
        }
    }
    (inputs, outputs)
}

fn inner_edges<'g>(graph: &'g Graph, nodes: &[u32]) -> Vec<InnerEdge<'g>> {
    let mut edges = Vec::new();
    for edge in &graph.edges {
        let (from, to) = (edge.from.node, edge.to.node);
        if !nodes.contains(&from) || !nodes.contains(&to) {
            continue;
        }
        let (Some(out), Some(input)) = (
            port_at(&graph.nodes[from as usize].outputs, &edge.from.port),
            port_at(&graph.nodes[to as usize].inputs, &edge.to.port),
        ) else {
            continue;
        };
        edges.push(InnerEdge {
            from: PortRef {
                node: NodeIndex(from),
                port: out,
            },
            to: PortRef {
                node: NodeIndex(to),
                port: input,
            },
            queued: matches!(edge.policy, EdgePolicyDoc::Bounded { .. }),
            policy: &edge.policy,
        });
    }
    edges
}

pub(super) fn units(compiled: &Compiled) -> Vec<&UnitInfo> {
    match compiled {
        Compiled::Steps(steps) => steps.iter().map(|step| &step.info).collect(),
        Compiled::Blob { units, .. } | Compiled::Source { units, .. } => units.iter().collect(),
    }
}

fn set_of(ports: &[PortRef]) -> BTreeSet<PortRef> {
    ports.iter().copied().collect()
}

/// R4 and R5 on what a backend returned.
pub(super) fn check_contract(
    formed: &Formed,
    region: u32,
    backend: &str,
    compiled: &Compiled,
) -> Result<(), RegionError> {
    let units = units(compiled);
    let fail = |rule: &'static str, detail: String| RegionError::BackendContract {
        backend: backend.to_string(),
        region,
        rule,
        detail,
    };
    let wanted: BTreeSet<u32> = formed.nodes.iter().copied().collect();
    let covered: BTreeSet<u32> = units
        .iter()
        .flat_map(|unit| unit.covers.iter().map(|node| node.0))
        .collect();
    if covered != wanted {
        return Err(fail(
            "R5",
            format!("units cover {covered:?}, the region is {wanted:?}"),
        ));
    }
    let reads: BTreeSet<PortRef> = units.iter().flat_map(|u| u.reads.iter().copied()).collect();
    if !extends(&reads, &formed.inputs, &wanted) {
        return Err(fail(
            "R4",
            format!("reads {reads:?}, boundary inputs {:?}", formed.inputs),
        ));
    }
    let writes: BTreeSet<PortRef> = units
        .iter()
        .flat_map(|u| u.writes.iter().copied())
        .collect();
    if !extends(&writes, &formed.outputs, &wanted) {
        return Err(fail(
            "R4",
            format!("writes {writes:?}, boundary outputs {:?}", formed.outputs),
        ));
    }
    Ok(())
}

/// A unit must name every boundary port. It may also name ports inside its region, which is how
/// it asks for a slot on an internal edge or a place for a port with no edge.
fn extends(named: &BTreeSet<PortRef>, boundary: &[PortRef], nodes: &BTreeSet<u32>) -> bool {
    let boundary = set_of(boundary);
    boundary.is_subset(named) && named.iter().all(|port| nodes.contains(&port.node.0))
}
