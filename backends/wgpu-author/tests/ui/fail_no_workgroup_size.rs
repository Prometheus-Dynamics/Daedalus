use daedalus_macros::node;

#[node(id = "ui.unsized", outputs(dst), backend = daedalus_wgpu_author(dst: u32))]
const UNSIZED: &str = "
    @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
    @compute fn main() { dst[0] = 1u; }
";

fn main() {}
