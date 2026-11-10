//! A binding that is only text in a comment isn't one, and a workgroup size built from a const
//! is read.

use daedalus_macros::node;
use daedalus_wgpu::shader::ShaderSpec;

#[node(id = "ui.double", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: u32, dst: u32))]
const DOUBLE: &str = "
    // @group(0) @binding(3) var<storage, read> ghost: array<u32>;
    const SIDE: u32 = 8;
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(SIDE * SIDE)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * 2u; }
    }
";

fn main() {
    let spec = (DOUBLE_NODE.body.code as &dyn std::any::Any)
        .downcast_ref::<ShaderSpec>()
        .expect("a shader spec");
    assert_eq!(spec.workgroup, [64, 1, 1]);
    assert!(spec.bindings.iter().all(|binding| binding.index != 3));
    assert_eq!(spec.bindings.len(), 2);
}
