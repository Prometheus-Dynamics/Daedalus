//! The rules.

use daedalus_graph::NodeKindDoc;
use daedalus_registry::{Fire, Registry};

use super::{
    EXCLUSIVE_ON_FANOUT, FIRE_ALL_MAY_NOT_PRODUCE, UNCONNECTED_REQUIRED_INPUT, UNUSED_OUTPUT,
};
use crate::diagnostic::Diagnostic;
use crate::solve::{Graph, Inserted};

fn is_bridge(graph: &Graph, node: usize) -> bool {
    graph.nodes[node].doc.kind == NodeKindDoc::HostBridge
}

pub(super) fn unconnected_required_inputs(graph: &Graph, out: &mut Vec<Diagnostic>) {
    for (index, node) in graph.nodes.iter().enumerate() {
        for name in &node.required {
            let wired = graph
                .edges
                .iter()
                .any(|e| e.to.node == index as u32 && e.to.port == *name);
            let defaulted = node
                .inputs
                .iter()
                .any(|port| port.name == *name && port.default.is_some());
            if !wired && !defaulted && !node.consts.contains_key(name) {
                let id = &node.doc.id;
                let message = format!("required input `{id}`.{name} isn't connected");
                out.push(Diagnostic::lint(
                    UNCONNECTED_REQUIRED_INPUT,
                    id,
                    Some(name),
                    message,
                ));
            }
        }
    }
}

pub(super) fn unused_outputs(graph: &Graph, out: &mut Vec<Diagnostic>) {
    for (index, node) in graph.nodes.iter().enumerate() {
        for port in &node.outputs {
            let read = graph
                .edges
                .iter()
                .any(|e| e.from.node == index as u32 && e.from.port == port.name);
            if !read {
                let (id, name) = (&node.doc.id, &port.name);
                let message = format!("nothing reads output `{id}`.{name}");
                out.push(Diagnostic::lint(UNUSED_OUTPUT, id, Some(name), message));
            }
        }
    }
}

/// An exclusive reader of a value someone else also reads costs a copy, which the planner added.
pub(super) fn exclusive_on_fanout(graph: &Graph, out: &mut Vec<Diagnostic>) {
    for (index, node) in graph.nodes.iter().enumerate() {
        if node.inserted != Inserted::Copy {
            continue;
        }
        for edge in graph.edges.iter().filter(|e| e.from.node == index as u32) {
            let id = &graph.nodes[edge.to.node as usize].doc.id;
            let port = &edge.to.port;
            let message = format!(
                "`{id}`.{port} takes its value exclusively from a shared source, which needs a copy"
            );
            out.push(Diagnostic::lint(
                EXCLUSIVE_ON_FANOUT,
                id,
                Some(port),
                message,
            ));
        }
    }
}

/// A node that fires when every input has a value never fires on a tick where one doesn't come.
pub(super) fn fire_all_may_not_produce(
    graph: &Graph,
    registry: &Registry,
    out: &mut Vec<Diagnostic>,
) {
    let conditional = |node: usize, port: &str| {
        registry
            .entry(&graph.nodes[node].doc.id)
            .is_some_and(|entry| {
                entry
                    .decl
                    .outputs
                    .iter()
                    .any(|p| p.name == port && p.conditional)
            })
    };
    let required = |node: usize, port: &str| {
        graph.nodes[node]
            .inputs
            .iter()
            .any(|p| p.name == port && !p.optional)
    };
    let mut may_skip = vec![false; graph.nodes.len()];
    let mut changed = true;
    while changed {
        changed = false;
        for edge in &graph.edges {
            let (from, to) = (edge.from.node as usize, edge.to.node as usize);
            let empty =
                !is_bridge(graph, from) && (may_skip[from] || conditional(from, &edge.from.port));
            if empty
                && !may_skip[to]
                && !is_bridge(graph, to)
                && graph.nodes[to].fire == Fire::All
                && required(to, &edge.to.port)
            {
                may_skip[to] = true;
                changed = true;
            }
        }
    }
    for (index, node) in graph.nodes.iter().enumerate() {
        if may_skip[index] {
            let id = &node.doc.id;
            let message = format!("`{id}` waits for every input, and one of them may not produce");
            out.push(Diagnostic::lint(
                FIRE_ALL_MAY_NOT_PRODUCE,
                id,
                None,
                message,
            ));
        }
    }
}
