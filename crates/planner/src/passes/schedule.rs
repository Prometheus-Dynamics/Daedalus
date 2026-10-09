use alloc::borrow::Cow;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use daedalus_data::model::Value;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::{ComputeAffinity, Graph};
use crate::metadata::{
    PLAN_GPU_SEGMENTS_KEY, PLAN_GPU_WHY_KEY, PLAN_SCHEDULE_ORDER_KEY, PLAN_SCHEDULE_PRIORITY_KEY,
    PLAN_TOPO_ORDER_KEY, metadata_node_indices, node_index_list,
};

use super::{PlannerConfig, diagnostic_node_id};

fn string_value(value: impl Into<String>) -> Value {
    Value::String(Cow::Owned(value.into()))
}

fn string_list(values: impl IntoIterator<Item = String>) -> Value {
    Value::List(values.into_iter().map(string_value).collect())
}

fn node_index_matrix(segments: impl IntoIterator<Item = Vec<usize>>) -> Value {
    Value::List(segments.into_iter().map(node_index_list).collect())
}

fn priority_value(node: usize, id: String, priority: u8) -> Value {
    Value::Map(vec![
        (string_value("node"), Value::Int(node as i64)),
        (string_value("id"), string_value(id)),
        (string_value("priority"), Value::Int(i64::from(priority))),
    ])
}

pub(super) fn gpu(graph: &mut Graph, config: &PlannerConfig, diags: &mut Vec<Diagnostic>) {
    let mut gpu_reasons: Vec<String> = Vec::new();
    // If GPU is disabled, flag required nodes.
    if !config.enable_gpu {
        gpu_reasons.push("gpu-disabled".into());
        let mut gpu_nodes: Vec<usize> = Vec::new();
        for (idx, node) in graph.nodes.iter().enumerate() {
            if matches!(node.compute, ComputeAffinity::GpuRequired) {
                gpu_nodes.push(idx);
                diags.push(
                    Diagnostic::new(
                        DiagnosticCode::GpuUnsupported,
                        format!("node {} requires GPU but GPU is disabled", node.id.0),
                    )
                    .in_pass("gpu")
                    .at_node(diagnostic_node_id(node)),
                );
            }
        }
        if !gpu_nodes.is_empty() {
            graph
                .metadata
                .insert(PLAN_GPU_SEGMENTS_KEY.into(), node_index_matrix([gpu_nodes]));
            graph
                .metadata
                .insert(PLAN_GPU_WHY_KEY.into(), string_list(gpu_reasons));
        }
        return;
    }

    // If caps are provided, validate support.
    #[cfg(feature = "gpu")]
    if let Some(caps) = &config.gpu_caps {
        let require_format = daedalus_gpu::GpuFormat::Rgba8Unorm;
        let mut ok = true;
        let has_format = caps
            .format_features
            .iter()
            .find(|f| f.format == require_format && f.sampleable);
        if caps.queue_count == 0 || !caps.has_transfer_queue {
            ok = false;
        }
        if has_format.is_none() {
            ok = false;
        }
        if !ok {
            gpu_reasons.push(format!(
                "insufficient-caps:queues={} transfer={} format_sampleable={}",
                caps.queue_count,
                caps.has_transfer_queue,
                has_format.is_some()
            ));
            for node in &graph.nodes {
                if matches!(
                    node.compute,
                    ComputeAffinity::GpuRequired | ComputeAffinity::GpuPreferred
                ) {
                    diags.push(
                        Diagnostic::new(
                            DiagnosticCode::GpuUnsupported,
                            format!(
                                "node {} cannot run on GPU: insufficient caps (queues={}, transfer={}, format={:?} sampleable={})",
                                node.id.0,
                                caps.queue_count,
                                caps.has_transfer_queue,
                                require_format,
                                has_format.is_some()
                            ),
                        )
                        .in_pass("gpu")
                        .at_node(diagnostic_node_id(node)),
                    );
                }
            }
        }
    }

    let segments = gpu_dependency_segments(graph);
    if !segments.is_empty() {
        graph
            .metadata
            .insert(PLAN_GPU_SEGMENTS_KEY.into(), node_index_matrix(segments));
    }
    if !gpu_reasons.is_empty() {
        gpu_reasons.sort();
        gpu_reasons.dedup();
        graph
            .metadata
            .insert(PLAN_GPU_WHY_KEY.into(), string_list(gpu_reasons));
    }
}

fn is_gpu_node(compute: ComputeAffinity) -> bool {
    matches!(
        compute,
        ComputeAffinity::GpuPreferred | ComputeAffinity::GpuRequired
    )
}

#[derive(Clone, Debug)]
struct Dsu {
    parent: Vec<usize>,
}

impl Dsu {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    fn find(&mut self, idx: usize) -> usize {
        if self.parent[idx] != idx {
            self.parent[idx] = self.find(self.parent[idx]);
        }
        self.parent[idx]
    }

    fn union(&mut self, a: usize, b: usize) {
        let root_a = self.find(a);
        let root_b = self.find(b);
        if root_a != root_b {
            self.parent[root_b] = root_a;
        }
    }
}

fn gpu_dependency_segments(graph: &Graph) -> Vec<Vec<usize>> {
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

pub(super) fn schedule(graph: &mut Graph, _diags: &mut Vec<Diagnostic>) {
    // If topo_order exists, use it; else declared order. Both are node indices: several
    // instances of one node id are told apart only by index. Attach basic priority info.
    let order = graph
        .metadata
        .get(PLAN_TOPO_ORDER_KEY)
        .and_then(metadata_node_indices)
        .unwrap_or_else(|| (0..graph.nodes.len()).collect());
    graph
        .metadata
        .insert(PLAN_SCHEDULE_ORDER_KEY.into(), node_index_list(order));

    // Prefer GPU-required nodes first within same topo layer (simple heuristic).
    let mut priorities: Vec<(usize, u8)> = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, n)| {
            let p = match n.compute {
                ComputeAffinity::GpuPreferred => 1,
                ComputeAffinity::GpuRequired | ComputeAffinity::CpuOnly => 2,
            };
            (idx, p)
        })
        .collect();
    priorities.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then_with(|| graph.nodes[a.0].id.0.cmp(&graph.nodes[b.0].id.0))
            .then_with(|| a.0.cmp(&b.0))
    });
    graph.metadata.insert(
        PLAN_SCHEDULE_PRIORITY_KEY.into(),
        Value::List(
            priorities
                .into_iter()
                .map(|(idx, priority)| priority_value(idx, graph.nodes[idx].id.0.clone(), priority))
                .collect(),
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::NodeInstance;

    fn node(id: &str, compute: ComputeAffinity) -> NodeInstance {
        NodeInstance::new(id).with_compute(compute)
    }

    #[test]
    fn schedule_metadata_is_structured() {
        let mut graph = Graph::default();
        graph
            .nodes
            .push(node("a,with,comma", ComputeAffinity::CpuOnly));
        graph
            .nodes
            .push(node("gpu:node", ComputeAffinity::GpuRequired));
        graph
            .metadata
            .insert(PLAN_TOPO_ORDER_KEY.into(), node_index_list([1, 0]));

        schedule(&mut graph, &mut Vec::new());

        assert_eq!(
            graph.metadata.get(PLAN_SCHEDULE_ORDER_KEY),
            Some(&node_index_list([1, 0]))
        );
        assert!(matches!(
            graph.metadata.get(PLAN_SCHEDULE_PRIORITY_KEY),
            Some(Value::List(items)) if items.iter().all(|item| matches!(item, Value::Map(_)))
        ));
    }

    #[test]
    fn gpu_metadata_is_structured() {
        let mut graph = Graph::default();
        graph.nodes.push(node("cpu", ComputeAffinity::CpuOnly));
        graph
            .nodes
            .push(node("gpu-a", ComputeAffinity::GpuPreferred));
        graph
            .nodes
            .push(node("gpu-b", ComputeAffinity::GpuRequired));

        gpu(
            &mut graph,
            &PlannerConfig {
                enable_gpu: true,
                ..Default::default()
            },
            &mut Vec::new(),
        );

        assert_eq!(
            graph.metadata.get(PLAN_GPU_SEGMENTS_KEY),
            Some(&node_index_matrix([vec![1], vec![2]]))
        );
    }

    #[test]
    fn gpu_segments_follow_gpu_dependencies_not_declaration_adjacency() {
        let mut graph = Graph::default();
        graph.nodes.push(node("cpu-root", ComputeAffinity::CpuOnly));
        graph
            .nodes
            .push(node("gpu-a", ComputeAffinity::GpuRequired));
        graph
            .nodes
            .push(node("gpu-b", ComputeAffinity::GpuPreferred));
        graph
            .nodes
            .push(node("gpu-c", ComputeAffinity::GpuPreferred));
        graph
            .nodes
            .push(node("gpu-d", ComputeAffinity::GpuPreferred));
        graph.edges.push(crate::graph::Edge {
            from: crate::graph::PortRef {
                node: crate::graph::NodeRef(0),
                port: "out".into(),
            },
            to: crate::graph::PortRef {
                node: crate::graph::NodeRef(1),
                port: "in".into(),
            },
            metadata: Default::default(),
        });
        graph.edges.push(crate::graph::Edge {
            from: crate::graph::PortRef {
                node: crate::graph::NodeRef(0),
                port: "out".into(),
            },
            to: crate::graph::PortRef {
                node: crate::graph::NodeRef(2),
                port: "in".into(),
            },
            metadata: Default::default(),
        });
        graph.edges.push(crate::graph::Edge {
            from: crate::graph::PortRef {
                node: crate::graph::NodeRef(3),
                port: "out".into(),
            },
            to: crate::graph::PortRef {
                node: crate::graph::NodeRef(4),
                port: "in".into(),
            },
            metadata: Default::default(),
        });

        gpu(
            &mut graph,
            &PlannerConfig {
                enable_gpu: true,
                ..Default::default()
            },
            &mut Vec::new(),
        );

        assert_eq!(
            graph.metadata.get(PLAN_GPU_SEGMENTS_KEY),
            Some(&node_index_matrix([vec![1], vec![2], vec![3, 4]]))
        );
    }
}
