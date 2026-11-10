use daedalus_macros::node;

#[node(id = "vision.double", inputs(x), outputs(y), backend = daedalus_fake_npu_author)]
fn double(x: u32) -> u32 {
    x * 2
}

fn main() {}
