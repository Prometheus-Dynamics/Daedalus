// The base64 kernels on vectors. Written by scripts/gen-base64-wide.py: edit the generator,
// not this file. `common.wgsl` comes before this text.

struct Params {
    // Input symbols (decode) or bytes (encode).
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
// decode: [offset of the first invalid symbol, or 0xffffffff; decoded length].
@group(0) @binding(3) var<storage, read_write> status: array<atomic<u32>>;
// decode: the six bits of each byte, or 0xff for a byte that is no symbol.
@group(0) @binding(4) var<storage, read> dec_table: array<u32>;

// Four output vectors for forty-eight input bytes: three input vectors, sixteen groups of
// three bytes, four symbols each. Counts that are multiples of 48 have no padded group.
@compute @workgroup_size(64)
fn base64_encode_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * 64u;
    if (t >= params.threads) { return; }
    let v0 = src[3u * t];
    let v1 = src[3u * t + 1u];
    let v2 = src[3u * t + 2u];
    let b0 = ((v0.x >> 0u) & 0xffu);
    let b1 = ((v0.x >> 8u) & 0xffu);
    let b2 = ((v0.x >> 16u) & 0xffu);
    let b3 = ((v0.x >> 24u) & 0xffu);
    let b4 = ((v0.y >> 0u) & 0xffu);
    let b5 = ((v0.y >> 8u) & 0xffu);
    let b6 = ((v0.y >> 16u) & 0xffu);
    let b7 = ((v0.y >> 24u) & 0xffu);
    let b8 = ((v0.z >> 0u) & 0xffu);
    let b9 = ((v0.z >> 8u) & 0xffu);
    let b10 = ((v0.z >> 16u) & 0xffu);
    let b11 = ((v0.z >> 24u) & 0xffu);
    let b12 = ((v0.w >> 0u) & 0xffu);
    let b13 = ((v0.w >> 8u) & 0xffu);
    let b14 = ((v0.w >> 16u) & 0xffu);
    let b15 = ((v0.w >> 24u) & 0xffu);
    let b16 = ((v1.x >> 0u) & 0xffu);
    let b17 = ((v1.x >> 8u) & 0xffu);
    let b18 = ((v1.x >> 16u) & 0xffu);
    let b19 = ((v1.x >> 24u) & 0xffu);
    let b20 = ((v1.y >> 0u) & 0xffu);
    let b21 = ((v1.y >> 8u) & 0xffu);
    let b22 = ((v1.y >> 16u) & 0xffu);
    let b23 = ((v1.y >> 24u) & 0xffu);
    let b24 = ((v1.z >> 0u) & 0xffu);
    let b25 = ((v1.z >> 8u) & 0xffu);
    let b26 = ((v1.z >> 16u) & 0xffu);
    let b27 = ((v1.z >> 24u) & 0xffu);
    let b28 = ((v1.w >> 0u) & 0xffu);
    let b29 = ((v1.w >> 8u) & 0xffu);
    let b30 = ((v1.w >> 16u) & 0xffu);
    let b31 = ((v1.w >> 24u) & 0xffu);
    let b32 = ((v2.x >> 0u) & 0xffu);
    let b33 = ((v2.x >> 8u) & 0xffu);
    let b34 = ((v2.x >> 16u) & 0xffu);
    let b35 = ((v2.x >> 24u) & 0xffu);
    let b36 = ((v2.y >> 0u) & 0xffu);
    let b37 = ((v2.y >> 8u) & 0xffu);
    let b38 = ((v2.y >> 16u) & 0xffu);
    let b39 = ((v2.y >> 24u) & 0xffu);
    let b40 = ((v2.z >> 0u) & 0xffu);
    let b41 = ((v2.z >> 8u) & 0xffu);
    let b42 = ((v2.z >> 16u) & 0xffu);
    let b43 = ((v2.z >> 24u) & 0xffu);
    let b44 = ((v2.w >> 0u) & 0xffu);
    let b45 = ((v2.w >> 8u) & 0xffu);
    let b46 = ((v2.w >> 16u) & 0xffu);
    let b47 = ((v2.w >> 24u) & 0xffu);
    let x0 = (b0 << 16u) | (b1 << 8u) | b2;
    let w0 = b64_symbol((x0 >> 18u) & 63u) << 0u | b64_symbol((x0 >> 12u) & 63u) << 8u | b64_symbol((x0 >> 6u) & 63u) << 16u | b64_symbol((x0 >> 0u) & 63u) << 24u;
    let x1 = (b3 << 16u) | (b4 << 8u) | b5;
    let w1 = b64_symbol((x1 >> 18u) & 63u) << 0u | b64_symbol((x1 >> 12u) & 63u) << 8u | b64_symbol((x1 >> 6u) & 63u) << 16u | b64_symbol((x1 >> 0u) & 63u) << 24u;
    let x2 = (b6 << 16u) | (b7 << 8u) | b8;
    let w2 = b64_symbol((x2 >> 18u) & 63u) << 0u | b64_symbol((x2 >> 12u) & 63u) << 8u | b64_symbol((x2 >> 6u) & 63u) << 16u | b64_symbol((x2 >> 0u) & 63u) << 24u;
    let x3 = (b9 << 16u) | (b10 << 8u) | b11;
    let w3 = b64_symbol((x3 >> 18u) & 63u) << 0u | b64_symbol((x3 >> 12u) & 63u) << 8u | b64_symbol((x3 >> 6u) & 63u) << 16u | b64_symbol((x3 >> 0u) & 63u) << 24u;
    let x4 = (b12 << 16u) | (b13 << 8u) | b14;
    let w4 = b64_symbol((x4 >> 18u) & 63u) << 0u | b64_symbol((x4 >> 12u) & 63u) << 8u | b64_symbol((x4 >> 6u) & 63u) << 16u | b64_symbol((x4 >> 0u) & 63u) << 24u;
    let x5 = (b15 << 16u) | (b16 << 8u) | b17;
    let w5 = b64_symbol((x5 >> 18u) & 63u) << 0u | b64_symbol((x5 >> 12u) & 63u) << 8u | b64_symbol((x5 >> 6u) & 63u) << 16u | b64_symbol((x5 >> 0u) & 63u) << 24u;
    let x6 = (b18 << 16u) | (b19 << 8u) | b20;
    let w6 = b64_symbol((x6 >> 18u) & 63u) << 0u | b64_symbol((x6 >> 12u) & 63u) << 8u | b64_symbol((x6 >> 6u) & 63u) << 16u | b64_symbol((x6 >> 0u) & 63u) << 24u;
    let x7 = (b21 << 16u) | (b22 << 8u) | b23;
    let w7 = b64_symbol((x7 >> 18u) & 63u) << 0u | b64_symbol((x7 >> 12u) & 63u) << 8u | b64_symbol((x7 >> 6u) & 63u) << 16u | b64_symbol((x7 >> 0u) & 63u) << 24u;
    let x8 = (b24 << 16u) | (b25 << 8u) | b26;
    let w8 = b64_symbol((x8 >> 18u) & 63u) << 0u | b64_symbol((x8 >> 12u) & 63u) << 8u | b64_symbol((x8 >> 6u) & 63u) << 16u | b64_symbol((x8 >> 0u) & 63u) << 24u;
    let x9 = (b27 << 16u) | (b28 << 8u) | b29;
    let w9 = b64_symbol((x9 >> 18u) & 63u) << 0u | b64_symbol((x9 >> 12u) & 63u) << 8u | b64_symbol((x9 >> 6u) & 63u) << 16u | b64_symbol((x9 >> 0u) & 63u) << 24u;
    let x10 = (b30 << 16u) | (b31 << 8u) | b32;
    let w10 = b64_symbol((x10 >> 18u) & 63u) << 0u | b64_symbol((x10 >> 12u) & 63u) << 8u | b64_symbol((x10 >> 6u) & 63u) << 16u | b64_symbol((x10 >> 0u) & 63u) << 24u;
    let x11 = (b33 << 16u) | (b34 << 8u) | b35;
    let w11 = b64_symbol((x11 >> 18u) & 63u) << 0u | b64_symbol((x11 >> 12u) & 63u) << 8u | b64_symbol((x11 >> 6u) & 63u) << 16u | b64_symbol((x11 >> 0u) & 63u) << 24u;
    let x12 = (b36 << 16u) | (b37 << 8u) | b38;
    let w12 = b64_symbol((x12 >> 18u) & 63u) << 0u | b64_symbol((x12 >> 12u) & 63u) << 8u | b64_symbol((x12 >> 6u) & 63u) << 16u | b64_symbol((x12 >> 0u) & 63u) << 24u;
    let x13 = (b39 << 16u) | (b40 << 8u) | b41;
    let w13 = b64_symbol((x13 >> 18u) & 63u) << 0u | b64_symbol((x13 >> 12u) & 63u) << 8u | b64_symbol((x13 >> 6u) & 63u) << 16u | b64_symbol((x13 >> 0u) & 63u) << 24u;
    let x14 = (b42 << 16u) | (b43 << 8u) | b44;
    let w14 = b64_symbol((x14 >> 18u) & 63u) << 0u | b64_symbol((x14 >> 12u) & 63u) << 8u | b64_symbol((x14 >> 6u) & 63u) << 16u | b64_symbol((x14 >> 0u) & 63u) << 24u;
    let x15 = (b45 << 16u) | (b46 << 8u) | b47;
    let w15 = b64_symbol((x15 >> 18u) & 63u) << 0u | b64_symbol((x15 >> 12u) & 63u) << 8u | b64_symbol((x15 >> 6u) & 63u) << 16u | b64_symbol((x15 >> 0u) & 63u) << 24u;
    dst[4u * t + 0u] = vec4<u32>(w0, w1, w2, w3);
    dst[4u * t + 1u] = vec4<u32>(w4, w5, w6, w7);
    dst[4u * t + 2u] = vec4<u32>(w8, w9, w10, w11);
    dst[4u * t + 3u] = vec4<u32>(w12, w13, w14, w15);
}

// Three output vectors for sixty-four input symbols: sixteen groups of four symbols, three
// bytes each. A group that holds an invalid symbol, or a padded last group with bits no byte
// takes, lowers the status to the index of its first such symbol; the status is written
// once an invocation, and only when it found one. Output bytes past the length are zero.
fn byte_w(i: u32) -> u32 {
    return (src[i / 16u][(i % 16u) / 4u] >> ((i % 4u) * 8u)) & 0xffu;
}

@compute @workgroup_size(64)
fn base64_decode_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = gid.x + gid.y * groups.x * 64u;
    if (t >= params.threads) { return; }
    let n = params.n;
    let last = n / 4u - 1u;
    let pad = select(0u, select(1u, 2u, byte_w(n - 2u) == 61u), byte_w(n - 1u) == 61u);
    let out_len = (last + 1u) * 3u - pad;
    if (t == 0u) {
        atomicStore(&status[1], out_len);
    }
    let v0 = src[4u * t];
    let v1 = src[4u * t + 1u];
    let v2 = src[4u * t + 2u];
    let v3 = src[4u * t + 3u];
    var first = 0xffffffffu;
    var o0 = 0u;
    var o1 = 0u;
    var o2 = 0u;
    var o3 = 0u;
    var o4 = 0u;
    var o5 = 0u;
    var o6 = 0u;
    var o7 = 0u;
    var o8 = 0u;
    var o9 = 0u;
    var o10 = 0u;
    var o11 = 0u;
    {
        let s = v0.x;
        let g = 16u * t + 0u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o0 = o0 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 0u);
        o0 = o0 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 8u);
        o0 = o0 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 16u);
    }
    {
        let s = v0.y;
        let g = 16u * t + 1u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o0 = o0 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 24u);
        o1 = o1 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 0u);
        o1 = o1 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 8u);
    }
    {
        let s = v0.z;
        let g = 16u * t + 2u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o1 = o1 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 16u);
        o1 = o1 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 24u);
        o2 = o2 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 0u);
    }
    {
        let s = v0.w;
        let g = 16u * t + 3u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o2 = o2 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 8u);
        o2 = o2 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 16u);
        o2 = o2 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 24u);
    }
    {
        let s = v1.x;
        let g = 16u * t + 4u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o3 = o3 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 0u);
        o3 = o3 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 8u);
        o3 = o3 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 16u);
    }
    {
        let s = v1.y;
        let g = 16u * t + 5u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o3 = o3 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 24u);
        o4 = o4 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 0u);
        o4 = o4 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 8u);
    }
    {
        let s = v1.z;
        let g = 16u * t + 6u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o4 = o4 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 16u);
        o4 = o4 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 24u);
        o5 = o5 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 0u);
    }
    {
        let s = v1.w;
        let g = 16u * t + 7u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o5 = o5 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 8u);
        o5 = o5 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 16u);
        o5 = o5 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 24u);
    }
    {
        let s = v2.x;
        let g = 16u * t + 8u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o6 = o6 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 0u);
        o6 = o6 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 8u);
        o6 = o6 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 16u);
    }
    {
        let s = v2.y;
        let g = 16u * t + 9u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o6 = o6 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 24u);
        o7 = o7 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 0u);
        o7 = o7 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 8u);
    }
    {
        let s = v2.z;
        let g = 16u * t + 10u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o7 = o7 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 16u);
        o7 = o7 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 24u);
        o8 = o8 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 0u);
    }
    {
        let s = v2.w;
        let g = 16u * t + 11u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o8 = o8 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 8u);
        o8 = o8 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 16u);
        o8 = o8 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 24u);
    }
    {
        let s = v3.x;
        let g = 16u * t + 12u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o9 = o9 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 0u);
        o9 = o9 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 8u);
        o9 = o9 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 16u);
    }
    {
        let s = v3.y;
        let g = 16u * t + 13u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o9 = o9 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 24u);
        o10 = o10 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 0u);
        o10 = o10 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 8u);
    }
    {
        let s = v3.z;
        let g = 16u * t + 14u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o10 = o10 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 16u);
        o10 = o10 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 24u);
        o11 = o11 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 0u);
    }
    {
        let s = v3.w;
        let g = 16u * t + 15u;
        let used = select(4u, 4u - pad, g == last);
        let e0 = dec_table[(s >> 0u) & 0xffu];
        let q0 = select(0u, e0, 0u < used);
        let e1 = dec_table[(s >> 8u) & 0xffu];
        let q1 = select(0u, e1, 1u < used);
        let e2 = dec_table[(s >> 16u) & 0xffu];
        let q2 = select(0u, e2, 2u < used);
        let e3 = dec_table[(s >> 24u) & 0xffu];
        let q3 = select(0u, e3, 3u < used);
        let bad = (q0 | q1 | q2 | q3) & 0x100u;
        if (bad != 0u) {
            // The failing path only: the lowest invalid symbol of the group.
            var at = 0xffffffffu;
            if ((q3 & 0x100u) != 0u) { at = 4u * g + 3u; }
            if ((q2 & 0x100u) != 0u) { at = 4u * g + 2u; }
            if ((q1 & 0x100u) != 0u) { at = 4u * g + 1u; }
            if ((q0 & 0x100u) != 0u) { at = 4u * g + 0u; }
            first = min(first, at);
        }
        let x = ((q0 & 63u) << 18u) | ((q1 & 63u) << 12u) | ((q2 & 63u) << 6u) | ((q3 & 63u) << 0u);
        if (g == last && pad > 0u) {
            let stray = x & select(0xffu, 0xffffu, pad == 2u);
            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));
        }
        o11 = o11 | (select(0u, (x >> 16u) & 0xffu, 3u * g + 0u < out_len) << 8u);
        o11 = o11 | (select(0u, (x >> 8u) & 0xffu, 3u * g + 1u < out_len) << 16u);
        o11 = o11 | (select(0u, (x >> 0u) & 0xffu, 3u * g + 2u < out_len) << 24u);
    }
    dst[3u * t + 0u] = vec4<u32>(o0, o1, o2, o3);
    dst[3u * t + 1u] = vec4<u32>(o4, o5, o6, o7);
    dst[3u * t + 2u] = vec4<u32>(o8, o9, o10, o11);
    if (first != 0xffffffffu) {
        atomicMin(&status[0], first);
    }
}
