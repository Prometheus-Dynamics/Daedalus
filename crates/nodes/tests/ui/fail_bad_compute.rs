use daedalus_nodes::node;
use daedalus_runtime::NodeError;

#[node(
    id = "starter.bad",
    compute = "wat"
)]
pub fn bad_compute() -> Result<(), NodeError> {
    Ok(())
}

fn main() {}
