use daedalus_macros::node;

#[node(id = "ui.nine", outputs(b0), backend = daedalus_wgpu_author(b0: u32))]
const NINE: &str = "
    @group(0) @binding(0) var<storage, read_write> b0: array<u32>;
    @group(0) @binding(1) var<storage, read_write> b1: array<u32>;
    @group(0) @binding(2) var<storage, read_write> b2: array<u32>;
    @group(0) @binding(3) var<storage, read_write> b3: array<u32>;
    @group(0) @binding(4) var<storage, read_write> b4: array<u32>;
    @group(0) @binding(5) var<storage, read_write> b5: array<u32>;
    @group(0) @binding(6) var<storage, read_write> b6: array<u32>;
    @group(0) @binding(7) var<storage, read_write> b7: array<u32>;
    @group(0) @binding(8) var<storage, read_write> b8: array<u32>;
    @compute @workgroup_size(1) fn main() {}
";

fn main() {}
