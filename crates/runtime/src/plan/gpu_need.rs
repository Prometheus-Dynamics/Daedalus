//! Whether a plan needs a GPU context: the nodes that prefer or require one, and the edges that
//! move a value to or through a device. Read by the executors, which create the context only
//! when a plan needs it.

use daedalus_planner::ComputeAffinity;

use super::{NODE_DEVICE_META_KEY, RuntimeEdgeTransport, RuntimeNode, RuntimePlan};

impl RuntimeEdgeTransport {
    /// Whether this edge moves its value to or through a GPU: a GPU or CPU-and-GPU target
    /// residency, or an adapter step that runs on the device or requires one.
    pub fn needs_gpu(&self) -> bool {
        matches!(
            self.target_residency,
            Some(daedalus_transport::Residency::Gpu | daedalus_transport::Residency::CpuAndGpu)
        ) || self.adapter_path.iter().any(|step| {
            step.requires_gpu
                || matches!(
                    step.kind,
                    daedalus_transport::AdaptKind::DeviceUpload
                        | daedalus_transport::AdaptKind::DeviceDownload
                        | daedalus_transport::AdaptKind::DeviceTransfer
                )
        })
    }
}

impl RuntimePlan {
    /// Whether running this plan needs a GPU context: a node that prefers or requires one, or an
    /// edge that needs one (see [`RuntimeEdgeTransport::needs_gpu`]). A plan without either runs
    /// on the CPU alone and creates no device.
    pub fn needs_gpu(&self) -> bool {
        gpu_needed(&self.nodes, &self.edge_transports)
    }
}

/// [`RuntimePlan::needs_gpu`] over a plan's node and edge tables (the executors keep these).
pub(crate) fn gpu_needed(
    nodes: &[RuntimeNode],
    transports: &[Option<RuntimeEdgeTransport>],
) -> bool {
    nodes.iter().any(node_needs_gpu)
        || transports
            .iter()
            .flatten()
            .any(RuntimeEdgeTransport::needs_gpu)
}

/// A node needs the GPU when it prefers or requires one, or when the engine resolved its device
/// to the GPU: a `device_from` node whose config selects the GPU backend keeps its `CpuOnly`
/// compute, and reports `daedalus.device = "gpu"` (the engine's resolution, `devices.rs`).
fn node_needs_gpu(node: &RuntimeNode) -> bool {
    !matches!(node.compute, ComputeAffinity::CpuOnly)
        || matches!(
            node.metadata.get(NODE_DEVICE_META_KEY),
            Some(daedalus_data::model::Value::String(device)) if &**device == "gpu"
        )
}
