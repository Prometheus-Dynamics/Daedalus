@group(0) @binding(0) var input_tex: texture_2d<f32>;
@group(0) @binding(1) var output_tex: texture_storage_2d<rgba8unorm, write>;

@compute @workgroup_size(8, 8, 1)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
  let xy = vec2<i32>(i32(gid.x), i32(gid.y));
  let c = textureLoad(input_tex, xy, 0);
  let inv = vec4<f32>(1.0 - c.r, 1.0 - c.g, 1.0 - c.b, c.a);
  textureStore(output_tex, xy, inv);
}

