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
pub(crate) fn record_devices(graph: &mut Graph, gpu_enabled: bool, gpu_backend: bool) {
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
        let configured = match node
            .metadata
            .get(daedalus_runtime::NODE_DEVICE_FROM_META_KEY)
        {
            Some(Value::String(path)) if fallback_from.is_none() => {
                Some(configured_device(node, path, gpu_backend))
            }
            _ => None,
        };
        let (device, reason) = match (node.compute, gpu_enabled, fallback_from) {
            _ if configured.is_some() => configured.expect("checked"),
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

/// The device a `device_from` node reports: the const config value at `path`
/// (`"<port>.<field>..."`), `"gpu"` meaning the GPU when the engine has a GPU backend.
fn configured_device(
    node: &daedalus_planner::NodeInstance,
    path: &str,
    gpu_backend: bool,
) -> (&'static str, String) {
    use daedalus_data::model::Value;
    let mut segments = path.split('.');
    let port = segments.next().unwrap_or_default();
    let field = path.rsplit('.').next().unwrap_or(port).to_string();
    let value = node
        .const_inputs
        .iter()
        .find(|(name, _)| name == port)
        .map(|(_, value)| value)
        .and_then(|mut value| {
            for segment in segments {
                value = match value {
                    Value::Struct(_) => value.field(segment)?,
                    Value::Map(entries) => entries
                        .iter()
                        .find(|(key, _)| key.as_str() == Some(segment))
                        .map(|(_, v)| v)?,
                    _ => return None,
                };
            }
            Some(value)
        });
    let Some(value) = value else {
        return ("cpu", format!("config {field} not set: default (cpu)"));
    };
    let chosen = match value {
        Value::String(text) => text.to_ascii_lowercase(),
        Value::Enum(variant) => variant.name.to_ascii_lowercase(),
        other => format!("{other:?}").to_ascii_lowercase(),
    };
    match (chosen.as_str(), gpu_backend) {
        ("gpu", true) => ("gpu", format!("config {field} = gpu")),
        ("gpu", false) => (
            "cpu",
            format!("config {field} = gpu, no GPU backend: CPU fallback"),
        ),
        (other, _) => ("cpu", format!("config {field} = {other}")),
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
        record_devices(&mut graph, true, true);
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
        record_devices(&mut graph, false, false);
        assert_eq!(graph.nodes[3].id.0, "p:cpu");
        let (device, reason) = resolved(&graph, 3);
        assert_eq!(device, "cpu");
        assert!(reason.contains("cpu fallback for p:gpu_fb"), "{reason}");
        let (device, reason) = resolved(&graph, 1);
        assert_eq!(device, "cpu");
        assert!(reason.contains("gpu_preferred"), "{reason}");
        assert!(resolved(&graph, 2).1.contains("planning fails"));
    }

    /// A `device_from` node reports its config's backend; `compute` stays CPU.
    #[test]
    fn device_from_reads_the_instance_config() {
        use daedalus_data::model::{EnumValue, StructFieldValue};
        let config = |backend: Value| {
            Value::Struct(vec![
                StructFieldValue {
                    name: "levels".into(),
                    value: Value::Int(3),
                },
                StructFieldValue {
                    name: "backend".into(),
                    value: backend,
                },
            ])
        };
        let node = |value: Option<Value>| {
            let mut node = NodeInstance::new("eidos:dense.map").with_metadata(
                daedalus_runtime::NODE_DEVICE_FROM_META_KEY,
                Value::String("config.backend".into()),
            );
            if let Some(value) = value {
                node = node.with_const_input("config", value);
            }
            node
        };
        let mut graph = Graph {
            nodes: vec![
                node(Some(config(Value::String("gpu".into())))),
                node(Some(config(Value::String("cpu".into())))),
                node(None),
                node(Some(config(Value::Enum(EnumValue {
                    name: "Gpu".into(),
                    value: None,
                })))),
            ],
            ..Graph::default()
        };
        let mut no_gpu = graph.clone();
        record_devices(&mut graph, false, true);
        assert_eq!(
            resolved(&graph, 0),
            ("gpu".into(), "config backend = gpu".into())
        );
        assert_eq!(
            resolved(&graph, 1),
            ("cpu".into(), "config backend = cpu".into())
        );
        assert_eq!(resolved(&graph, 2).0, "cpu");
        assert_eq!(resolved(&graph, 3).0, "gpu", "enum variants by name");
        assert!(
            graph
                .nodes
                .iter()
                .all(|n| n.compute == ComputeAffinity::CpuOnly)
        );
        record_devices(&mut no_gpu, false, false);
        assert_eq!(
            resolved(&no_gpu, 0),
            (
                "cpu".into(),
                "config backend = gpu, no GPU backend: CPU fallback".into()
            )
        );
    }
}
