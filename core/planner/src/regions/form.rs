//! Groups nodes into regions, greedily, in one fixed pass over the edges.

use daedalus_backend::{PortRef, RateId};
use daedalus_graph::NodeKindDoc;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

use super::Formed;
use super::rules::{Adjacency, convex};
use crate::assign::{Assignment, Placed};
use crate::solve::{Graph, Port, Rate};

fn port_index(ports: &[Port], name: &str) -> Option<PortIndex> {
    ports
        .iter()
        .position(|port| port.name == name)
        .map(|at| PortIndex(at as u16))
}

/// The boundary ports of a set of nodes. Bridges are always outside.
fn boundary(graph: &Graph, inside: &[bool], nodes: &[u32]) -> (Vec<PortRef>, Vec<PortRef>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let is_in = |node: u32| inside[node as usize];
    for edge in &graph.edges {
        if is_in(edge.to.node)
            && !is_in(edge.from.node)
            && let Some(port) =
                port_index(&graph.nodes[edge.to.node as usize].inputs, &edge.to.port)
        {
            inputs.push(PortRef {
                node: NodeIndex(edge.to.node),
                port,
            });
        }
        if is_in(edge.from.node)
            && !is_in(edge.to.node)
            && let Some(port) = port_index(
                &graph.nodes[edge.from.node as usize].outputs,
                &edge.from.port,
            )
        {
            outputs.push(PortRef {
                node: NodeIndex(edge.from.node),
                port,
            });
        }
    }
    debug_assert!(!nodes.is_empty());
    inputs.sort();
    inputs.dedup();
    outputs.sort();
    outputs.dedup();
    (inputs, outputs)
}

/// The edges in formation order: producer's position, port name, consumer.
fn visit_order(graph: &Graph, adjacency: &Adjacency) -> Vec<(u32, u32)> {
    // A control node is always a region of its own.
    let alone = |node: u32| graph.nodes[node as usize].doc.kind == NodeKindDoc::Control;
    let mut edges: Vec<(u32, &str, u32, u32)> = graph
        .edges
        .iter()
        .filter(|edge| !alone(edge.from.node) && !alone(edge.to.node))
        .map(|edge| {
            let position = adjacency.position[edge.from.node as usize];
            (
                position,
                edge.from.port.as_str(),
                edge.to.node,
                edge.from.node,
            )
        })
        .collect();
    edges.sort_unstable();
    edges
        .into_iter()
        .map(|(_, _, to, from)| (from, to))
        .collect()
}

fn rate_id(rates: &mut Vec<(Rate, Rate)>, pair: (Rate, Rate)) -> RateId {
    let at = rates
        .iter()
        .position(|known| *known == pair)
        .unwrap_or_else(|| {
            rates.push(pair);
            rates.len() - 1
        });
    RateId(at as u32)
}

pub(super) fn form(graph: &Graph, assignment: &Assignment) -> (Vec<Formed>, Vec<(Rate, Rate)>) {
    let count = graph.nodes.len();
    let adjacency = Adjacency::new(graph);
    let mut owner: Vec<u32> = (0..count as u32).collect();
    let mut members: Vec<Vec<u32>> = (0..count as u32).map(|node| vec![node]).collect();
    let mut inside = vec![false; count];
    let mut seen = vec![0u32; count];
    let mut stamp = 0;
    let backend = |node: u32| match assignment.placed[node as usize] {
        Placed::Backend(index) => Some(index),
        Placed::Host => None,
    };
    let rate = |node: u32| {
        let node = &graph.nodes[node as usize];
        (node.rate_in, node.rate_out)
    };
    for (from, to) in visit_order(graph, &adjacency) {
        let (a, b) = (owner[from as usize], owner[to as usize]);
        if a == b
            || backend(from).is_none()
            || backend(from) != backend(to)
            || rate(from) != rate(to)
        {
            continue;
        }
        let merged: Vec<u32> = members[a as usize]
            .iter()
            .chain(&members[b as usize])
            .copied()
            .collect();
        for &node in &merged {
            inside[node as usize] = true;
        }
        stamp += 1;
        let ok = convex(&adjacency, &merged, &inside, &mut seen, stamp);
        for &node in &merged {
            inside[node as usize] = false;
        }
        if !ok {
            continue;
        }
        let keep = a.min(b);
        for &node in &merged {
            owner[node as usize] = keep;
        }
        members[a.max(b) as usize] = Vec::new();
        members[keep as usize] = merged;
    }
    let mut rates = Vec::new();
    let mut formed = Vec::new();
    // A region's nodes are listed producers first, because that's the order its steps run in.
    let mut position = vec![0; graph.nodes.len()];
    for (at, &node) in graph.order.iter().enumerate() {
        position[node as usize] = at;
    }
    for mut nodes in members.into_iter().filter(|nodes| !nodes.is_empty()) {
        nodes.sort_unstable_by_key(|&node| position[node as usize]);
        let Some(index) = backend(nodes[0]) else {
            continue;
        };
        for &node in &nodes {
            inside[node as usize] = true;
        }
        let (inputs, outputs) = boundary(graph, &inside, &nodes);
        for &node in &nodes {
            inside[node as usize] = false;
        }
        let rate = rate_id(&mut rates, rate(nodes[0]));
        formed.push(Formed {
            backend: index,
            nodes,
            inputs,
            outputs,
            rate,
        });
    }
    formed.sort_by_key(|region: &Formed| region.nodes[0]);
    (formed, rates)
}

/// The backend index as a slice position.
pub(super) fn rank(index: BackendIndex) -> usize {
    usize::from(index.0)
}
