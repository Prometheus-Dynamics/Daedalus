use daedalus_macros::node;

#[node(
    id = "vision.double",
    inputs(x),
    outputs(y),
    backend = acme_npu_author(model = "scale2", x: u32, y: u32)
)]
const DOUBLE: &str = "scale2";

fn main() {}
