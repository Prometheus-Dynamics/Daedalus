use daedalus_core::compute::ComputeAffinity;
use daedalus_runtime::NodeError;

use crate::NodeDescriptor;
use crate::node;

#[node(id = "starter.print", bundle = "starter")]
fn starter_print() -> Result<(), NodeError> {
    Ok(())
}

#[node(
    id = "starter.gpu_copy",
    bundle = "starter",
    compute(ComputeAffinity::GpuPreferred)
)]
fn starter_gpu_copy() -> Result<(), NodeError> {
    Ok(())
}

/// Starter bundle: minimal set of nodes used in examples/tests.
pub fn nodes() -> Vec<NodeDescriptor> {
    vec![starter_print::descriptor(), starter_gpu_copy::descriptor()]
}
