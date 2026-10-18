use daedalus_runtime::NodeError;

use crate::NodeDecl;
use crate::node;

/// Logs `value`: through `tracing` with the `tracing` feature, else on stdout.
#[node(id = "starter.print", inputs("value"))]
#[cfg_attr(not(feature = "tracing"), allow(clippy::print_stdout))]
fn starter_print(value: i32) -> Result<(), NodeError> {
    #[cfg(feature = "tracing")]
    tracing::info!(target: "daedalus_nodes::starter", value, "starter.print");
    #[cfg(not(feature = "tracing"))]
    println!("{value}");
    Ok(())
}

/// Starter bundle: minimal set of nodes used in examples/tests.
pub fn nodes() -> Vec<NodeDecl> {
    vec![StarterPrintNode::node_decl().expect("starter.print node declaration")]
}
