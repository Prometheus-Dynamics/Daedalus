//! What must be true of every region.

use std::fmt;

use daedalus_graph::NodeKindDoc;

use crate::solve::Graph;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// The nodes are connected.
    R1Connected,
    /// No path leaves the region and comes back.
    R2Convex,
    /// Every node has the same rate pair.
    R3OneRate,
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Rule::R1Connected => "R1 (connected)",
            Rule::R2Convex => "R2 (convex)",
            Rule::R3OneRate => "R3 (one rate pair)",
        })
    }
}

/// Edges between non-bridge nodes, ready for walking.
pub(super) struct Adjacency {
    pub leaving: Vec<Vec<u32>>,
    pub entering: Vec<Vec<u32>>,
    /// Each node's place in the topological order.
    pub position: Vec<u32>,
}

impl Adjacency {
    pub fn new(graph: &Graph) -> Adjacency {
        let count = graph.nodes.len();
        let mut leaving = vec![Vec::new(); count];
        let mut entering = vec![Vec::new(); count];
        let bridge = |node: u32| graph.nodes[node as usize].doc.kind == NodeKindDoc::HostBridge;
        for edge in &graph.edges {
            if !bridge(edge.from.node) && !bridge(edge.to.node) {
                leaving[edge.from.node as usize].push(edge.to.node);
                entering[edge.to.node as usize].push(edge.from.node);
            }
        }
        let mut position = vec![0; count];
        for (at, &node) in graph.order.iter().enumerate() {
            position[node as usize] = at as u32;
        }
        Adjacency {
            leaving,
            entering,
            position,
        }
    }
}

fn members(count: usize, nodes: &[u32]) -> Vec<bool> {
    let mut inside = vec![false; count];
    for &node in nodes {
        inside[node as usize] = true;
    }
    inside
}

/// Whether adding no further nodes, `inside` is convex. `seen` is scratch stamped with `stamp`.
pub(super) fn convex(
    adjacency: &Adjacency,
    nodes: &[u32],
    inside: &[bool],
    seen: &mut [u32],
    stamp: u32,
) -> bool {
    let limit = nodes
        .iter()
        .map(|&n| adjacency.position[n as usize])
        .max()
        .unwrap_or(0);
    let mut stack: Vec<u32> = nodes.to_vec();
    while let Some(node) = stack.pop() {
        for &next in &adjacency.leaving[node as usize] {
            if inside[next as usize] {
                if !inside[node as usize] {
                    return false;
                }
                continue;
            }
            if adjacency.position[next as usize] < limit && seen[next as usize] != stamp {
                seen[next as usize] = stamp;
                stack.push(next);
            }
        }
    }
    true
}

fn connected(adjacency: &Adjacency, nodes: &[u32], inside: &[bool]) -> bool {
    let Some(&first) = nodes.first() else {
        return true;
    };
    let mut seen = vec![false; inside.len()];
    seen[first as usize] = true;
    let mut stack = vec![first];
    let mut reached = 1;
    while let Some(node) = stack.pop() {
        let around = adjacency.leaving[node as usize]
            .iter()
            .chain(&adjacency.entering[node as usize]);
        for &next in around {
            if inside[next as usize] && !seen[next as usize] {
                seen[next as usize] = true;
                reached += 1;
                stack.push(next);
            }
        }
    }
    reached == nodes.len()
}

/// Checks R1, R2 and R3 on a set of nodes.
pub fn check(graph: &Graph, nodes: &[u32]) -> Result<(), Rule> {
    let adjacency = Adjacency::new(graph);
    let inside = members(graph.nodes.len(), nodes);
    if !connected(&adjacency, nodes, &inside) {
        return Err(Rule::R1Connected);
    }
    let mut seen = vec![0; graph.nodes.len()];
    if !convex(&adjacency, nodes, &inside, &mut seen, 1) {
        return Err(Rule::R2Convex);
    }
    let rate = |node: u32| {
        let node = &graph.nodes[node as usize];
        (node.rate_in, node.rate_out)
    };
    match nodes.split_first() {
        Some((&first, rest)) if rest.iter().any(|&node| rate(node) != rate(first)) => {
            Err(Rule::R3OneRate)
        }
        _ => Ok(()),
    }
}
