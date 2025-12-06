use crate::{ComputeAffinity, NodeInstance};

/// Convenience helper to build a `NodeInstance` for tests/examples.
///
/// ```
/// use daedalus_planner::helpers::node;
/// use daedalus_planner::ComputeAffinity;
/// let instance = node("demo", ComputeAffinity::CpuOnly, ["in"], ["out"]);
/// assert_eq!(instance.outputs.len(), 1);
/// ```
pub fn node(
    id: impl Into<String>,
    compute: ComputeAffinity,
    inputs: impl IntoIterator<Item = impl Into<String>>,
    outputs: impl IntoIterator<Item = impl Into<String>>,
) -> NodeInstance {
    NodeInstance {
        id: daedalus_registry::ids::NodeId::new(id.into()),
        bundle: None,
        label: None,
        inputs: inputs.into_iter().map(Into::into).collect(),
        outputs: outputs.into_iter().map(Into::into).collect(),
        compute,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: Default::default(),
    }
}
