//! A versioned list of edits to a graph.

use std::collections::BTreeSet;
use std::fmt;

use daedalus_graph::{GraphBody, GraphPatch, NodeSelector, PatchOp};
use serde::{Deserialize, Serialize};

const PATCH_VERSION: u32 = 1;

/// One op that matched, and the node it changed, by its index before the patch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedOp {
    pub op: u32,
    pub node: u32,
}

/// A selector that matched nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnknownSelector {
    pub op: u32,
    pub selector: NodeSelector,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchReport {
    pub applied: Vec<AppliedOp>,
    pub not_found: Vec<UnknownSelector>,
    pub removed_nodes: u32,
    pub removed_edges: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PatchError {
    UnsupportedVersion { version: u32 },
}

impl fmt::Display for PatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatchError::UnsupportedVersion { version } => {
                write!(
                    f,
                    "patch version {version} isn't supported (this build reads {PATCH_VERSION})"
                )
            }
        }
    }
}

impl std::error::Error for PatchError {}

pub(super) fn select(graph: &GraphBody, selector: &NodeSelector) -> Option<u32> {
    let by_id = |id: &str| {
        graph
            .nodes
            .iter()
            .position(|node| node.ui_id.as_deref() == Some(id))
            .map(|at| at as u32)
    };
    match (selector.index, selector.id.as_deref()) {
        (Some(index), None) => ((index as usize) < graph.nodes.len()).then_some(index),
        (None, Some(id)) => by_id(id),
        (Some(index), Some(id)) => by_id(id).filter(|&found| found == index),
        (None, None) => None,
    }
}

fn delete(graph: &mut GraphBody, doomed: &BTreeSet<u32>, report: &mut PatchReport) {
    let mut remap = vec![None; graph.nodes.len()];
    let mut next = 0;
    for (at, slot) in remap.iter_mut().enumerate() {
        if !doomed.contains(&(at as u32)) {
            *slot = Some(next);
            next += 1;
        }
    }
    let before = graph.edges.len();
    graph.edges.retain_mut(|edge| {
        match (remap[edge.from.node as usize], remap[edge.to.node as usize]) {
            (Some(from), Some(to)) => {
                edge.from.node = from;
                edge.to.node = to;
                true
            }
            _ => false,
        }
    });
    report.removed_edges = (before - graph.edges.len()) as u32;
    report.removed_nodes = doomed.len() as u32;
    let mut at = 0;
    graph.nodes.retain(|_| {
        at += 1;
        remap[at - 1].is_some()
    });
}

/// Applies a patch. Selectors are resolved against the graph as it was, so deleting one node
/// doesn't change what a later op's index means. A selector that matches nothing is reported.
pub fn apply_patch(graph: &mut GraphBody, patch: &GraphPatch) -> Result<PatchReport, PatchError> {
    if patch.version != PATCH_VERSION {
        return Err(PatchError::UnsupportedVersion {
            version: patch.version,
        });
    }
    let mut report = PatchReport::default();
    let mut doomed = BTreeSet::new();
    let selector = |op: &PatchOp| match op {
        PatchOp::SetConst { node, .. }
        | PatchOp::ReplaceNodeId { node, .. }
        | PatchOp::DeleteNode { node } => node.clone(),
    };
    for (number, op) in patch.ops.iter().enumerate() {
        let number = number as u32;
        let wanted = selector(op);
        let Some(index) = select(graph, &wanted) else {
            report.not_found.push(UnknownSelector {
                op: number,
                selector: wanted,
            });
            continue;
        };
        let node = &mut graph.nodes[index as usize];
        match op {
            PatchOp::SetConst { port, value, .. } => match value {
                Some(value) => {
                    node.consts.insert(port.clone(), value.clone());
                }
                None => {
                    node.consts.remove(port);
                }
            },
            PatchOp::ReplaceNodeId { new_id, .. } => node.id = new_id.clone(),
            PatchOp::DeleteNode { .. } => {
                doomed.insert(index);
            }
        }
        report.applied.push(AppliedOp {
            op: number,
            node: index,
        });
    }
    delete(graph, &doomed, &mut report);
    Ok(report)
}
