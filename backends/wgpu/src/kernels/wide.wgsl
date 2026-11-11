// The kernel ops on vectors: one invocation reads and writes whole vectors of four words, with
// the same bytes and the same order as the word text. Counts that are whole vectors use these;
// the rest use the word text. `common.wgsl` comes before this text.

struct Params {
    // Input elements, by entry point.
    n: u32,
    // Invocations that write.
    threads: u32,
    // Output bytes.
    out_len: u32,
    unused: u32,
}

@group(0) @binding(0) var<storage, read> src: array<vec4<u32>>;
@group(0) @binding(1) var<storage, read_write> dst: array<vec4<u32>>;
@group(0) @binding(2) var<storage, read> params: Params;

// Byte `k` of a vector: bytes are numbered from the first word, in memory order.
fn vec_byte(v: vec4<u32>, k: u32) -> u32 {
    return (v[k / 4u] >> ((k % 4u) * 8u)) & 0xffu;
}

// Invocation t takes eight halves, from one input vector, and writes two output vectors.
@compute @workgroup_size(WG)
fn f16_to_f32_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * WG;
    if (t >= params.threads) { return; }
    let v = src[t];
    var out: array<u32, 8>;
    for (var k = 0u; k < 8u; k++) {
        out[k] = half_to_f32_bits((v[k / 2u] >> ((k % 2u) * 16u)) & 0xffffu);
    }
    dst[2u * t] = vec4<u32>(out[0], out[1], out[2], out[3]);
    dst[2u * t + 1u] = vec4<u32>(out[4], out[5], out[6], out[7]);
}

// Invocation t takes sixteen floats, from four input vectors, and writes one output vector of
// sixteen bytes: the four output words the word text would write for them.
@compute @workgroup_size(WG)
fn f32_to_unorm8_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * WG;
    if (t >= params.threads) { return; }
    var words: vec4<u32>;
    for (var j = 0u; j < 4u; j++) {
        var word = 0u;
        for (var k = 0u; k < 4u; k++) {
            let i = 4u * j + k;
            word |= unorm8(src[4u * t + i / 4u][i % 4u]) << (8u * k);
        }
        words[j] = word;
    }
    dst[t] = words;
}

// The two hex digits of each byte, packed low first: a lookup, in place of two digit formulas.
@group(0) @binding(3) var<storage, read> hex_table: array<u32>;

// Four hex digits of two bytes: the byte table's two entries, the first in the low half.
fn hex_pair(a: u32, b: u32) -> u32 {
    return hex_table[a] | (hex_table[b] << 16u);
}

// Invocation t takes one input vector, sixteen bytes, and writes two output vectors: the eight
// output words, four hex digits each, that the word text writes for the same bytes.
@compute @workgroup_size(WG)
fn hex_lower_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * WG;
    if (t >= params.threads) { return; }
    let v = src[t];
    let w0 = hex_pair(vec_byte(v, 0u), vec_byte(v, 1u));
    let w1 = hex_pair(vec_byte(v, 2u), vec_byte(v, 3u));
    let w2 = hex_pair(vec_byte(v, 4u), vec_byte(v, 5u));
    let w3 = hex_pair(vec_byte(v, 6u), vec_byte(v, 7u));
    let w4 = hex_pair(vec_byte(v, 8u), vec_byte(v, 9u));
    let w5 = hex_pair(vec_byte(v, 10u), vec_byte(v, 11u));
    let w6 = hex_pair(vec_byte(v, 12u), vec_byte(v, 13u));
    let w7 = hex_pair(vec_byte(v, 14u), vec_byte(v, 15u));
    dst[2u * t] = vec4<u32>(w0, w1, w2, w3);
    dst[2u * t + 1u] = vec4<u32>(w4, w5, w6, w7);
}

