//! What a patch reaches.

use std::collections::BTreeSet;

use daedalus_graph::{GraphBody, GraphPatch, PatchOp};
use serde::{Deserialize, Serialize};

use super::patch::select;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchImpact {
    /// The selected nodes and everything downstream of them, sorted.
    pub downstream_nodes: Vec<u32>,
    /// Those that feed nothing.
    pub sink_nodes: Vec<u32>,
}

/// Works out what a patch touches, without applying it.
pub fn analyze(graph: &GraphBody, patch: &GraphPatch) -> PatchImpact {
    let mut reached = BTreeSet::new();
    let mut stack = Vec::new();
    for op in &patch.ops {
        let (PatchOp::SetConst { node, .. }
        | PatchOp::ReplaceNodeId { node, .. }
        | PatchOp::DeleteNode { node }) = op;
        if let Some(index) = select(graph, node)
            && reached.insert(index)
        {
            stack.push(index);
        }
    }
    while let Some(node) = stack.pop() {
        for edge in graph.edges.iter().filter(|edge| edge.from.node == node) {
            if reached.insert(edge.to.node) {
                stack.push(edge.to.node);
            }
        }
    }
    let sink_nodes = reached
        .iter()
        .copied()
        .filter(|&n| !graph.edges.iter().any(|e| e.from.node == n))
        .collect();
    PatchImpact {
        downstream_nodes: reached.into_iter().collect(),
        sink_nodes,
    }
}
