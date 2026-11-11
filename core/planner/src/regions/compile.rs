//! Compiles one region and checks what came back.

use std::collections::BTreeSet;

use daedalus_backend::{
    Backend, CompileCx, Compiled, ConstOf, HostBinding, Incoming, InnerEdge, NodeBody,
    OutputSharing, PortRef, Refusal, Region, Sharing, UnitInfo,
};
use daedalus_graph::{EdgePolicyDoc, Lit, NodeDoc};
use daedalus_registry::Registry;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_value::Access;

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
    pub freeze_params: bool,
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
                label: solved.doc.label.as_deref(),
                metadata: &solved.doc.metadata,
                code: record.code,
                text: record.text,
            });
        }
    }
    let edges = inner_edges(cx.graph, &formed.nodes);
    let sharing = output_sharing(cx, formed);
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
            freeze_params: cx.freeze_params,
            sharing: &sharing,
        },
    )
}

/// How each output of the region's nodes is consumed. An output with one consumer that takes it by
/// value, on this backend, is `Taker`. One that every consumer reads, on this backend, is `Readers`.
/// Anything else, such as a consumer on another backend, stays `Fresh`.
fn output_sharing(cx: &Compiling<'_>, formed: &Formed) -> Vec<OutputSharing> {
    let mut out = Vec::new();
    for &node in &formed.nodes {
        let solved = &cx.graph.nodes[node as usize];
        for (at, output) in solved.outputs.iter().enumerate() {
            let consumers: Vec<(&str, u32)> = cx
                .graph
                .edges
                .iter()
                .filter(|edge| edge.from.node == node && edge.from.port == output.name)
                .map(|edge| (edge.to.port.as_str(), edge.to.node))
                .collect();
            let sharing = sharing_of(cx, formed.backend, &consumers);
            if sharing != Sharing::Fresh {
                out.push(OutputSharing {
                    port: PortRef {
                        node: NodeIndex(node),
                        port: PortIndex(at as u16),
                    },
                    sharing,
                });
            }
        }
    }
    out
}

fn sharing_of(cx: &Compiling<'_>, backend: BackendIndex, consumers: &[(&str, u32)]) -> Sharing {
    // The host's only reader takes the value out of the port, so it is the one taker, and the
    // value moves to it. With a reader on a node as well, the host shares the value as a reader.
    if let [(_, node)] = consumers
        && matches!(cx.assignment.placed[*node as usize], Placed::Host)
    {
        return Sharing::Taker;
    }
    let mut by_value = Vec::with_capacity(consumers.len());
    for &(port, node) in consumers {
        match cx.assignment.placed[node as usize] {
            // The host reads alongside a node. Its typed take copies a value a node still holds.
            Placed::Host => by_value.push(false),
            // Another backend may take the value in its own way, so the value is not kept.
            Placed::Backend(at) if at != backend => return Sharing::Fresh,
            Placed::Backend(_) => {
                let access = cx.graph.nodes[node as usize]
                    .inputs
                    .iter()
                    .find(|input| input.name == port)
                    .map(|input| input.access);
                by_value.push(matches!(access, Some(Access::Move | Access::Modify)));
            }
        }
    }
    match by_value.as_slice() {
        [true] => Sharing::Taker,
        [] => Sharing::Fresh,
        values if values.iter().all(|taken| !taken) => Sharing::Readers,
        _ => Sharing::Fresh,
    }
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
