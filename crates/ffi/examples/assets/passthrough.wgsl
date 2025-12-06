@group(0) @binding(0)
var<uniform> params: vec4<u32>;

@compute @workgroup_size(1)
fn main() {
  // no-op placeholder; this file exists so the macro can include_str! it.
  let _ = params.x;
}

