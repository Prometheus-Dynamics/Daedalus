use daedalus_runtime::NodeError;

use crate::NodeDecl;
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
pub fn nodes() -> Vec<NodeDecl> {
    vec![
        StarterPrintNode::node_decl().expect("starter.print node declaration"),
        StarterGpuCopyNode::node_decl().expect("starter.gpu_copy node declaration"),
    ]
}
