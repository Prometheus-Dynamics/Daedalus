use daedalus_macros::node;

#[node(
    id = "vision.double",
    inputs(x),
    outputs(y),
    backend = daedalus_fake_npu_author(model = "scale2", x: u32, y: u32)
)]
const DOUBLE: &str = "scale2";

fn main() {
    assert_eq!(DOUBLE_NODE.body.backend, "fake-npu");
    assert_eq!(DOUBLE_NODE.body.text, "scale2");
}
