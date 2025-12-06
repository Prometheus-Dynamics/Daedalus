struct Pixels { data: array<u32>, };
@group(0) @binding(0) var<storage, read> input: Pixels;
@group(0) @binding(1) var<storage, read_write> output: Pixels;
@group(0) @binding(2) var<uniform> meta_count: u32;

fn brighten(channel: u32) -> u32 {
    let v = i32(channel) + 30;
    return u32(clamp(v, 0, 255));
}

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let idx = gid.x;
    if idx >= meta_count { return; }
    let pix = input.data[idx];
    let r = (pix & 0x000000ffu);
    let g = (pix & 0x0000ff00u) >> 8u;
    let b = (pix & 0x00ff0000u) >> 16u;
    let a = (pix & 0xff000000u);
    let nr = brighten(r);
    let ng = brighten(g);
    let nb = brighten(b);
    output.data[idx] = (a) | (nb << 16u) | (ng << 8u) | nr;
}
