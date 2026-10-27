//! Node fixtures for this crate's unit tests.

use daedalus_planner::ComputeAffinity;

use crate::plan::RuntimeNode;

/// A CPU node named `id` with `stable_id` and no const inputs, sync groups or metadata.
pub(crate) fn test_node(id: &str, stable_id: u128) -> RuntimeNode {
    RuntimeNode {
        id: id.to_string(),
        stable_id,
        bundle: None,
        label: None,
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: Default::default(),
    }
}
