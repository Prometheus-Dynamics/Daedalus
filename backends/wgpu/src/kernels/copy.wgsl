struct Params {
    n: u32,
    threads: u32,
    out_len: u32,
    unused: u32,
}

@group(0) @binding(0) var<storage, read> src: array<vec4<u32>>;
@group(0) @binding(1) var<storage, read_write> dst: array<vec4<u32>>;
@group(0) @binding(2) var<storage, read> params: Params;

fn keep(lane: u32, rest: u32) -> u32 {
    let first = lane * 4u;
    if (rest >= first + 4u) {
        return 0xffffffffu;
    }
    if (rest <= first) {
        return 0u;
    }
    return (1u << ((rest - first) * 8u)) - 1u;
}

@compute @workgroup_size(WG)
fn copy_quads(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * WG;
    if (t >= params.threads) {
        return;
    }
    let rest = params.out_len - t * 16u;
    let v = src[t];
    dst[t] = vec4<u32>(v.x & keep(0u, rest), v.y & keep(1u, rest), v.z & keep(2u, rest), v.w & keep(3u, rest));
}
