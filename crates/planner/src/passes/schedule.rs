use daedalus_data::model::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::diagnostics::{Diagnostic, DiagnosticCode};
use crate::graph::{ComputeAffinity, Graph};
use crate::metadata::{
    PLAN_GPU_SEGMENTS_KEY, PLAN_GPU_WHY_KEY, PLAN_SCHEDULE_ORDER_KEY, PLAN_SCHEDULE_PRIORITY_KEY,
    PLAN_TOPO_ORDER_KEY,
};

use super::{PlannerConfig, diagnostic_node_id};

fn string_value(value: impl Into<String>) -> Value {
    Value::String(Cow::Owned(value.into()))
}

fn string_list(values: impl IntoIterator<Item = String>) -> Value {
    Value::List(values.into_iter().map(string_value).collect())
}

fn string_matrix(values: impl IntoIterator<Item = Vec<String>>) -> Value {
    Value::List(
        values
            .into_iter()
            .map(|items| Value::List(items.into_iter().map(string_value).collect()))
            .collect(),
    )
}

fn priority_value(id: String, priority: u8) -> Value {
    Value::Map(vec![
        (string_value("id"), string_value(id)),
        (string_value("priority"), Value::Int(i64::from(priority))),
    ])
}

fn string_list_from_metadata(value: Option<&Value>) -> Option<Vec<String>> {
    let Value::List(items) = value? else {
        return None;
    };
    Some(
        items
            .iter()
            .filter_map(|item| match item {
                Value::String(value) => Some(value.to_string()),
                _ => None,
            })
            .collect(),
    )
}

pub(super) fn gpu(graph: &mut Graph, config: &PlannerConfig, diags: &mut Vec<Diagnostic>) {
    let mut gpu_reasons: Vec<String> = Vec::new();
    // If GPU is disabled, flag required nodes.
    if !config.enable_gpu {
        gpu_reasons.push("gpu-disabled".into());
        let mut gpu_nodes: Vec<String> = Vec::new();
        for node in &graph.nodes {
            if matches!(node.compute, ComputeAffinity::GpuRequired) {
                gpu_nodes.push(node.id.0.clone());
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
                .insert(PLAN_GPU_SEGMENTS_KEY.into(), string_matrix(vec![gpu_nodes]));
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
            .insert(PLAN_GPU_SEGMENTS_KEY.into(), string_matrix(segments));
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

fn gpu_dependency_segments(graph: &Graph) -> Vec<Vec<String>> {
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
                .into_iter()
                .map(|idx| graph.nodes[idx].id.0.clone())
                .collect()
        })
        .collect()
}

pub(super) fn schedule(graph: &mut Graph, _diags: &mut Vec<Diagnostic>) {
    // If topo_order exists, use it; else declared order. Attach basic priority info.
    let order =
        string_list_from_metadata(graph.metadata.get(PLAN_TOPO_ORDER_KEY)).unwrap_or_else(|| {
            graph
                .nodes
                .iter()
                .map(|n| n.id.0.clone())
                .collect::<Vec<_>>()
        });
    graph
        .metadata
        .insert(PLAN_SCHEDULE_ORDER_KEY.into(), string_list(order));

    // Prefer GPU-required nodes first within same topo layer (simple heuristic).
    let mut priorities: Vec<(String, u8)> = graph
        .nodes
        .iter()
        .map(|n| {
            let p = match n.compute {
                ComputeAffinity::GpuPreferred => 1,
                ComputeAffinity::GpuRequired | ComputeAffinity::CpuOnly => 2,
            };
            (n.id.0.clone(), p)
        })
        .collect();
    priorities.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    graph.metadata.insert(
        PLAN_SCHEDULE_PRIORITY_KEY.into(),
        Value::List(
            priorities
                .into_iter()
                .map(|(id, priority)| priority_value(id, priority))
                .collect(),
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::NodeInstance;
    use daedalus_registry::ids::NodeId;

    fn node(id: &str, compute: ComputeAffinity) -> NodeInstance {
        NodeInstance {
            id: NodeId::new(id),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        }
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
        graph.metadata.insert(
            PLAN_TOPO_ORDER_KEY.into(),
            Value::List(vec![string_value("gpu:node"), string_value("a,with,comma")]),
        );

        schedule(&mut graph, &mut Vec::new());

        assert_eq!(
            graph.metadata.get(PLAN_SCHEDULE_ORDER_KEY),
            Some(&Value::List(vec![
                string_value("gpu:node"),
                string_value("a,with,comma"),
            ]))
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
            Some(&Value::List(vec![
                Value::List(vec![string_value("gpu-a")]),
                Value::List(vec![string_value("gpu-b")]),
            ]))
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
            Some(&Value::List(vec![
                Value::List(vec![string_value("gpu-a")]),
                Value::List(vec![string_value("gpu-b")]),
                Value::List(vec![string_value("gpu-c"), string_value("gpu-d")]),
            ]))
        );
    }
}
