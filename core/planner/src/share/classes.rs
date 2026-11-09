//! Which nodes are equal, across graphs.

use std::collections::{BTreeMap, BTreeSet};

use daedalus_graph::{EdgeDoc, Lit, NodeKindDoc};

use crate::solve::convert::key_of;
use crate::solve::{Graph, Node};

pub(super) struct Classes {
    /// By graph, then node. `None` for a node that can't be shared.
    pub of: Vec<Vec<Option<u32>>>,
    /// By class: whether two or more graphs have it.
    pub shared: Vec<bool>,
}

fn finite(lit: &Lit) -> bool {
    match lit {
        Lit::Float(value) => value.is_finite(),
        Lit::List(items) => items.iter().all(finite),
        Lit::Struct(fields) => fields.iter().all(|field| finite(&field.value)),
        Lit::Enum { payload, .. } => payload.as_deref().is_none_or(finite),
        _ => true,
    }
}

/// Everything about a node that makes it the same as another, apart from where its inputs come
/// from. `None` when it can't be shared: not flagged, or holding a value or a type with no exact
/// written form. A shared output reaches a remainder through a host input, which names its type.
fn node_key(node: &Node) -> Option<String> {
    let doc = &node.doc;
    let eligible = node.shareable
        && doc.kind == NodeKindDoc::Node
        && node
            .consts
            .values()
            .chain(doc.metadata.values())
            .all(finite)
        && node.outputs.iter().all(|port| key_of(&port.ty).is_some());
    if !eligible {
        return None;
    }
    let mut doc = doc.clone();
    doc.label = None;
    doc.ui_id = None;
    doc.consts.clear();
    doc.fire = None;
    doc.reusable = None;
    doc.shareable = false;
    doc.inputs.clear();
    doc.outputs.clear();
    let ports: Vec<String> = node
        .inputs
        .iter()
        .chain(&node.outputs)
        .map(|port| format!("{port:?}"))
        .collect();
    let solved = format!("{:?} {:?} {}", node.fire, node.required, node.reusable);
    serde_json::to_string(&(&doc, ports, solved, &node.consts)).ok()
}

pub(super) fn alias(node: &Node) -> &str {
    node.doc.label.as_deref().unwrap_or(&node.doc.id)
}

/// Where one input comes from, by class for a node. `None` when the source isn't shareable.
fn source_key(graph: &Graph, edge: &EdgeDoc, of: &[Option<u32>]) -> Option<String> {
    let from = &graph.nodes[edge.from.node as usize];
    let port = &edge.from.port;
    let source = if from.doc.kind == NodeKindDoc::HostBridge {
        let ty = from.doc.port_types.get(port);
        let policy = from.doc.host_inputs.get(port).copied().unwrap_or_default();
        format!("host {:?}", (alias(from), port, ty, policy))
    } else {
        format!("node {:?}", (of[edge.from.node as usize]?, port))
    };
    serde_json::to_string(&(&edge.to.port, source, &edge.policy)).ok()
}

fn class_key(
    graph: &Graph,
    node: u32,
    incoming: &[&EdgeDoc],
    of: &[Option<u32>],
) -> Option<String> {
    let own = node_key(&graph.nodes[node as usize])?;
    let mut sources = incoming
        .iter()
        .map(|edge| source_key(graph, edge, of))
        .collect::<Option<Vec<_>>>()?;
    sources.sort_unstable();
    serde_json::to_string(&(own, sources)).ok()
}

pub(super) fn classify(graphs: &[&Graph]) -> Classes {
    let mut interned = BTreeMap::new();
    let mut seen_in: Vec<BTreeSet<usize>> = Vec::new();
    let mut result = Vec::with_capacity(graphs.len());
    for (number, graph) in graphs.iter().enumerate() {
        let mut incoming: Vec<Vec<&EdgeDoc>> = vec![Vec::new(); graph.nodes.len()];
        for edge in &graph.edges {
            incoming[edge.to.node as usize].push(edge);
        }
        let mut of = vec![None; graph.nodes.len()];
        for &node in &graph.order {
            let Some(key) = class_key(graph, node, &incoming[node as usize], &of) else {
                continue;
            };
            let next = interned.len() as u32;
            let class = *interned.entry(key).or_insert(next);
            if class == next {
                seen_in.push(BTreeSet::new());
            }
            seen_in[class as usize].insert(number);
            of[node as usize] = Some(class);
        }
        result.push(of);
    }
    Classes {
        of: result,
        shared: seen_in.iter().map(|graphs| graphs.len() >= 2).collect(),
    }
}
