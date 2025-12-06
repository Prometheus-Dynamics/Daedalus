@group(0) @binding(0) var<storage, read_write> out_buf: array<u32>;

@compute @workgroup_size(1, 1, 1)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
  let _ = gid;
  out_buf[0] = 1u;
}

