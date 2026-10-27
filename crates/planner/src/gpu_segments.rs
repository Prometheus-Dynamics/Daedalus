//! GPU chains of a planned graph: the union-find that groups GPU-to-GPU edges, shared by the
//! schedule pass and the GPU buffer pass.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use crate::graph::{ComputeAffinity, Graph};

/// Whether `compute` runs on the GPU (preferred or required).
pub(crate) fn is_gpu_node(compute: ComputeAffinity) -> bool {
    matches!(
        compute,
        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
    )
}

/// Disjoint sets over node indices.
#[derive(Clone, Debug)]
pub(crate) struct Dsu {
    parent: Vec<usize>,
}

impl Dsu {
    pub(crate) fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    pub(crate) fn find(&mut self, idx: usize) -> usize {
        if self.parent[idx] != idx {
            self.parent[idx] = self.find(self.parent[idx]);
        }
        self.parent[idx]
    }

    pub(crate) fn union(&mut self, a: usize, b: usize) {
        let root_a = self.find(a);
        let root_b = self.find(b);
        if root_a != root_b {
            self.parent[root_b] = root_a;
        }
    }
}

/// Node index groups (each sorted) of the GPU nodes joined by edges whose both ends are GPU
/// nodes, one group per connected chain.
pub(crate) fn gpu_dependency_segments(graph: &Graph) -> Vec<Vec<usize>> {
    let mut dsu = Dsu::new(graph.nodes.len());
    for edge in &graph.edges {
        let from = edge.from.node.0;
        let to = edge.to.node.0;
        let Some(from_node) = graph.nodes.get(from) else {
            continue;
        };
        let Some(to_node) = graph.nodes.get(to) else {
            continue;
        };
        if is_gpu_node(from_node.compute) && is_gpu_node(to_node.compute) {
            dsu.union(from, to);
        }
    }

    let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for idx in 0..graph.nodes.len() {
        if is_gpu_node(graph.nodes[idx].compute) {
            let root = dsu.find(idx);
            by_root.entry(root).or_default().push(idx);
        }
    }

    by_root
        .into_values()
        .map(|mut indices| {
            indices.sort_unstable();
            indices
        })
        .collect()
}
