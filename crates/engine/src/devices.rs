//! The device each node runs on: CPU-fallback swaps and the resolved device the plan reports.

use crate::prelude::*;
use daedalus_planner::Graph;

/// Without a GPU backend, a node declared with `fallback = "<cpu node>"` runs that CPU node in its
/// place (same ports, same edges): the GPU node's handler takes device-resident inputs a CPU
/// payload does not carry. Instances keep their labels; the swap is by node id.
pub(crate) fn use_cpu_fallbacks(graph: &mut Graph) {
    for node in &mut graph.nodes {
        let Some(daedalus_data::model::Value::String(fallback)) = node.metadata.get("fallback")
        else {
            continue;
        };
        // A bare fallback id names a node of the same plugin (`plugin:node`), as the macro's
        // `fallback = "..."` is written within the plugin.
        let node_id = node.id.to_string();
        let id = match node_id.split_once(':') {
            Some((plugin, _)) if !fallback.contains(':') => format!("{plugin}:{fallback}"),
            _ => fallback.to_string(),
        };
        node.metadata.insert(
            daedalus_runtime::NODE_FALLBACK_FROM_META_KEY.to_string(),
            daedalus_data::model::Value::String(node_id.into()),
        );
        node.id = daedalus_registry::ids::NodeId::new(id);
    }
}

/// Record each node's resolved device and why, from its `compute` affinity, whether the planner
/// targets the GPU, and any CPU-fallback swap.
pub(crate) fn record_devices(graph: &mut Graph, gpu_enabled: bool) {
    use daedalus_core::compute::ComputeAffinity;
    use daedalus_data::model::Value;
    use daedalus_runtime::{
        NODE_DEVICE_META_KEY, NODE_DEVICE_REASON_META_KEY, NODE_FALLBACK_FROM_META_KEY,
    };
    for node in &mut graph.nodes {
        let fallback_from = match node.metadata.get(NODE_FALLBACK_FROM_META_KEY) {
            Some(Value::String(from)) => Some(from.to_string()),
            _ => None,
        };
        let (device, reason) = match (node.compute, gpu_enabled, fallback_from) {
            (_, _, Some(from)) => ("cpu", format!("cpu fallback for {from}: no GPU backend")),
            (ComputeAffinity::CpuOnly, _, None) => ("cpu", "compute = cpu_only".to_string()),
            (affinity, true, None) => ("gpu", format!("compute = {}", affinity_name(affinity))),
            (ComputeAffinity::GpuPreferred, false, None) => (
                "cpu",
                "compute = gpu_preferred, but the planner's GPU is off: runs on the CPU"
                    .to_string(),
            ),
            (ComputeAffinity::GpuRequired, false, None) => (
                "gpu",
                "compute = gpu_required, but the planner's GPU is off: planning fails".to_string(),
            ),
        };
        node.metadata.insert(
            NODE_DEVICE_META_KEY.to_string(),
            Value::String(device.into()),
        );
        node.metadata.insert(
            NODE_DEVICE_REASON_META_KEY.to_string(),
            Value::String(reason.into()),
        );
    }
}

fn affinity_name(affinity: daedalus_core::compute::ComputeAffinity) -> &'static str {
    use daedalus_core::compute::ComputeAffinity;
    match affinity {
        ComputeAffinity::CpuOnly => "cpu_only",
        ComputeAffinity::GpuPreferred => "gpu_preferred",
        ComputeAffinity::GpuRequired => "gpu_required",
    }
}

#[cfg(test)]
mod tests {
    use daedalus_core::compute::ComputeAffinity;
    use daedalus_data::model::Value;
    use daedalus_planner::{Graph, NodeInstance};
    use daedalus_runtime::{NODE_DEVICE_META_KEY, NODE_DEVICE_REASON_META_KEY};

    use super::{record_devices, use_cpu_fallbacks};

    fn resolved(graph: &Graph, index: usize) -> (String, String) {
        let text = |key: &str| match graph.nodes[index].metadata.get(key) {
            Some(Value::String(v)) => v.to_string(),
            other => panic!("{key}: {other:?}"),
        };
        (
            text(NODE_DEVICE_META_KEY),
            text(NODE_DEVICE_REASON_META_KEY),
        )
    }

    fn graph() -> Graph {
        Graph {
            nodes: vec![
                NodeInstance::new("p:cpu"),
                NodeInstance::new("p:gpu").with_compute(ComputeAffinity::GpuPreferred),
                NodeInstance::new("p:must").with_compute(ComputeAffinity::GpuRequired),
                NodeInstance::new("p:gpu_fb")
                    .with_compute(ComputeAffinity::GpuPreferred)
                    .with_metadata("fallback", Value::String("cpu".into())),
            ],
            ..Graph::default()
        }
    }

    #[test]
    fn gpu_backend_resolves_gpu_nodes_to_the_gpu() {
        let mut graph = graph();
        record_devices(&mut graph, true);
        assert_eq!(
            resolved(&graph, 0),
            ("cpu".into(), "compute = cpu_only".into())
        );
        assert_eq!(
            resolved(&graph, 1),
            ("gpu".into(), "compute = gpu_preferred".into())
        );
        assert_eq!(
            resolved(&graph, 2),
            ("gpu".into(), "compute = gpu_required".into())
        );
        assert_eq!(resolved(&graph, 3).0, "gpu");
    }

    #[test]
    fn cpu_backend_swaps_fallbacks_and_says_why() {
        let mut graph = graph();
        use_cpu_fallbacks(&mut graph);
        record_devices(&mut graph, false);
        assert_eq!(graph.nodes[3].id.0, "p:cpu");
        let (device, reason) = resolved(&graph, 3);
        assert_eq!(device, "cpu");
        assert!(reason.contains("cpu fallback for p:gpu_fb"), "{reason}");
        let (device, reason) = resolved(&graph, 1);
        assert_eq!(device, "cpu");
        assert!(reason.contains("gpu_preferred"), "{reason}");
        assert!(resolved(&graph, 2).1.contains("planning fails"));
    }
}
