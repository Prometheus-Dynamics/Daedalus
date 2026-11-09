//! Who reads what, and which free nodes have to be decided together.

use std::collections::BTreeMap;

use crate::solve::Graph;

/// The graph as the search sees it: output ports numbered, consumers by port.
pub(super) struct Wiring {
    pub producer: Vec<u32>,
    /// Ports a node writes.
    pub produces: Vec<Vec<u32>>,
    /// Ports a node reads, once per edge.
    pub consumes: Vec<Vec<u32>>,
    pub consumers: Vec<Vec<u32>>,
}

pub(super) fn wiring(graph: &Graph) -> Wiring {
    let count = graph.nodes.len();
    let mut numbers: BTreeMap<(u32, &str), u32> = BTreeMap::new();
    let mut wiring = Wiring {
        producer: Vec::new(),
        produces: vec![Vec::new(); count],
        consumes: vec![Vec::new(); count],
        consumers: Vec::new(),
    };
    for edge in &graph.edges {
        let next = numbers.len() as u32;
        let port = *numbers
            .entry((edge.from.node, edge.from.port.as_str()))
            .or_insert(next);
        if port == next {
            wiring.producer.push(edge.from.node);
            wiring.consumers.push(Vec::new());
            wiring.produces[edge.from.node as usize].push(port);
        }
        wiring.consumes[edge.to.node as usize].push(port);
        wiring.consumers[port as usize].push(edge.to.node);
    }
    wiring
}

fn find(parent: &mut [u32], mut at: u32) -> u32 {
    while parent[at as usize] != at {
        parent[at as usize] = parent[parent[at as usize] as usize];
        at = parent[at as usize];
    }
    at
}

/// The coupled groups of free nodes, each sorted, ordered by lowest node.
pub(super) fn components(wiring: &Wiring, free: &[bool]) -> Vec<Vec<u32>> {
    let mut parent: Vec<u32> = (0..free.len() as u32).collect();
    let mut join = |a: u32, b: u32| {
        let (a, b) = (find(&mut parent, a), find(&mut parent, b));
        parent[a.max(b) as usize] = a.min(b);
    };
    for (port, readers) in wiring.consumers.iter().enumerate() {
        let producer = wiring.producer[port];
        let mut previous = None;
        for &reader in readers.iter().filter(|&&reader| free[reader as usize]) {
            if free[producer as usize] {
                join(producer, reader);
            }
            if let Some(previous) = previous {
                join(previous, reader);
            }
            previous = Some(reader);
        }
    }
    let mut groups: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for node in (0..free.len() as u32).filter(|&node| free[node as usize]) {
        groups
            .entry(find(&mut parent, node))
            .or_default()
            .push(node);
    }
    groups.into_values().collect()
}
