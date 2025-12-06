//! Helpers to turn node descriptors into planner `NodeInstance` values with compute hints threaded through.
//! Feature-gated to avoid pulling planner dependencies into minimal builds.

use crate::NodeDescriptor;

/// Build a planner `NodeInstance` from a descriptor, providing ports and version.
pub fn node_instance<I, O>(
    desc: &NodeDescriptor,
    inputs: I,
    outputs: O,
) -> daedalus_planner::NodeInstance
where
    I: IntoIterator,
    I::Item: Into<String>,
    O: IntoIterator,
    O::Item: Into<String>,
{
    daedalus_planner::NodeInstance {
        id: desc.id.clone(),
        bundle: None,
        label: desc.label.clone(),
        inputs: inputs.into_iter().map(Into::into).collect(),
        outputs: outputs.into_iter().map(Into::into).collect(),
        compute: desc.default_compute,
        const_inputs: Vec::new(),
        sync_groups: desc.sync_groups.clone(),
        metadata: desc.metadata.clone(),
    }
}
