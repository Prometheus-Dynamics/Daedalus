use daedalus_macros::node;

#[node(id = "ui.port", outputs(out), backend = daedalus_wgpu_author(out: u32))]
const PORT: &str = "
    @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(1) fn main() { dst[0] = 1u; }
";

#[node(id = "ui.function", backend = daedalus_wgpu_author)]
fn function(x: u32) -> u32 {
    x
}

fn main() {}
