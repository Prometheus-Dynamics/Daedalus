use daedalus_nodes::node;
use daedalus_runtime::NodeError;

#[node(
    id = "starter.ok",
    bundle = "starter",
    outputs("out")
)]
fn ok_node() -> Result<i32, NodeError> {
    Ok(1)
}

fn main() {
    let d = ok_node::descriptor();
    assert_eq!(d.id.0, "starter.ok");
    assert!(matches!(
        d.default_compute,
        daedalus_core::compute::ComputeAffinity::CpuOnly
    ));
}
