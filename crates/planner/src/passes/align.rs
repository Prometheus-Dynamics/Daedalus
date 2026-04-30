use daedalus_data::model::Value;
use std::borrow::Cow;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::Graph;
use crate::metadata::PLAN_TOPO_ORDER_KEY;

use super::is_host_bridge;

pub(super) fn align(graph: &mut Graph, diags: &mut Vec<Diagnostic>) {
    // Kahn topo sort to detect cycles and emit ordering metadata.
    let n = graph.nodes.len();
    let mut indegree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for edge in &graph.edges {
        if edge.from.node.0 < n
            && edge.to.node.0 < n
            && !is_host_bridge(&graph.nodes[edge.from.node.0])
            && !is_host_bridge(&graph.nodes[edge.to.node.0])
        {
            adj[edge.from.node.0].push(edge.to.node.0);
            indegree[edge.to.node.0] += 1;
        }
    }
    let mut queue: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter(|(_, d)| **d == 0)
        .map(|(i, _)| i)
        .collect();
    let mut order = Vec::new();
    while let Some(v) = queue.pop() {
        order.push(v);
        for &nxt in &adj[v] {
            indegree[nxt] -= 1;
            if indegree[nxt] == 0 {
                queue.push(nxt);
            }
        }
    }
    if order.len() != n {
        // Cycle: collect nodes with indegree > 0 for deterministic message.
        let mut cyc_nodes: Vec<String> = indegree
            .iter()
            .enumerate()
            .filter(|(_, d)| **d > 0)
            .map(|(i, _)| graph.nodes[i].id.0.clone())
            .collect();
        cyc_nodes.sort();
        diags.push(
            Diagnostic::new(
                DiagnosticCode::ScheduleConflict,
                format!(
                    "graph contains a cycle involving nodes: {}",
                    cyc_nodes.join(",")
                ),
            )
            .in_pass("align"),
        );
    } else {
        graph.metadata.insert(
            PLAN_TOPO_ORDER_KEY.into(),
            Value::List(
                order
                    .iter()
                    .map(|&idx| Value::String(Cow::Owned(graph.nodes[idx].id.0.clone())))
                    .collect(),
            ),
        );
    }
}
