// Data-parallel kernels on GPU-resident data (`GpuKernels`): the GPU column of the Daedalus ops
// in docs/backend-matrix.md. Buffers are `array<u32>` (WGSL has no byte stores): every thread
// writes whole output words, little-endian bytes, and the results are bit-exact with the CPU
// references (integer-only conversions, no float flushing of the values that matter).

struct Params {
    // Input elements: pixels, floats, halves or bytes, per entry point.
    n: u32,
    // Threads that write (output words, or output elements).
    threads: u32,
    // Output bytes (to zero the tail of the last word).
    out_len: u32,
    unused: u32,
}

@group(0) @binding(0) var<storage, read> src: array<u32>;
@group(0) @binding(1) var<storage, read_write> dst: array<u32>;
@group(0) @binding(2) var<storage, read> params: Params;
// base64 decode: [first invalid symbol offset (0xffffffff when none), decoded length].
@group(0) @binding(3) var<storage, read_write> status: array<atomic<u32>>;

const WG: u32 = 64u;

fn thread_index(gid: vec3<u32>, groups: vec3<u32>) -> u32 {
    return gid.x + gid.y * groups.x * WG;
}

fn byte_at(i: u32) -> u32 {
    return (src[i >> 2u] >> ((i & 3u) * 8u)) & 0xffu;
}

/// Keep the bytes of word `t` that lie before `out_len`.
fn tail_mask(t: u32, word: u32) -> u32 {
    let first = t * 4u;
    if (first + 4u <= params.out_len) {
        return word;
    }
    let keep = params.out_len - first;
    return word & ((1u << (keep * 8u)) - 1u);
}

@compute @workgroup_size(64)
fn rgb_to_rgba(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let b = t * 3u;
    dst[t] = byte_at(b) | (byte_at(b + 1u) << 8u) | (byte_at(b + 2u) << 16u) | 0xff000000u;
}

@compute @workgroup_size(64)
fn rgba_to_rgb(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let j = t * 4u + k;
        if (j < params.out_len) {
            word |= byte_at((j / 3u) * 4u + j % 3u) << (8u * k);
        }
    }
    dst[t] = word;
}

@compute @workgroup_size(64)
fn gray_to_rgba(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let g = byte_at(t);
    dst[t] = g | (g << 8u) | (g << 16u) | 0xff000000u;
}

@compute @workgroup_size(64)
fn rgba_red(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let p = t * 4u + k;
        if (p < params.out_len) {
            word |= (src[p] & 0xffu) << (8u * k);
        }
    }
    dst[t] = word;
}

@compute @workgroup_size(64)
fn f32_from_le(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // GPU words are little-endian: the bits are the float.
    dst[t] = src[t];
}

fn half_to_f32_bits(h: u32) -> u32 {
    let sign = (h & 0x8000u) << 16u;
    let e = (h >> 10u) & 0x1fu;
    let m = h & 0x3ffu;
    if (e == 0x1fu) {
        // Inf, or NaN quieted (as F16C and the CPU kernels do).
        return sign | 0x7f800000u | (m << 13u) | select(0u, 0x400000u, m != 0u);
    }
    if (e != 0u) {
        return sign | ((e + 112u) << 23u) | (m << 13u);
    }
    if (m == 0u) {
        return sign;
    }
    // Subnormal m * 2^-24, normalized with integers (a float multiply could be flushed).
    let p = firstLeadingBit(m);
    return sign | ((p + 103u) << 23u) | ((m << (23u - p)) & 0x7fffffu);
}

@compute @workgroup_size(64)
fn f16_to_f32(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = half_to_f32_bits((src[t >> 1u] >> ((t & 1u) * 16u)) & 0xffffu);
}

fn unorm8(bits: u32) -> u32 {
    // NaN by its bits (a float compare may be folded away).
    if ((bits & 0x7fffffffu) > 0x7f800000u) { return 0u; }
    let s = clamp(bitcast<f32>(bits), 0.0, 1.0) * 255.0;
    let whole = u32(s);
    // Half away from zero, like the CPU kernels (`round` is half-to-even in WGSL).
    return min(whole + select(0u, 1u, s - f32(whole) >= 0.5), 255u);
}

@compute @workgroup_size(64)
fn f32_to_unorm8(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let i = t * 4u + k;
        if (i < params.n) {
            word |= unorm8(src[i]) << (8u * k);
        }
    }
    dst[t] = word;
}

@compute @workgroup_size(64)
fn copy_words(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = tail_mask(t, src[t]);
}

fn hex_digit(n: u32) -> u32 {
    return n + 48u + select(0u, 39u, n > 9u);
}

@compute @workgroup_size(64)
fn hex_lower(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // Output word t: the 4 digits of input bytes 2t and 2t + 1.
    let a = byte_at(2u * t);
    let b = select(0u, byte_at(2u * t + 1u), 2u * t + 1u < params.n);
    let word = hex_digit(a >> 4u) | (hex_digit(a & 15u) << 8u) | (hex_digit(b >> 4u) << 16u)
        | (hex_digit(b & 15u) << 24u);
    dst[t] = tail_mask(t, word);
}

fn b64_symbol(v: u32) -> u32 {
    var c = v + 65u;
    c += select(0u, 6u, v >= 26u);
    c -= select(0u, 75u, v >= 52u);
    c -= select(0u, 15u, v == 62u);
    c -= select(0u, 12u, v == 63u);
    return c;
}

@compute @workgroup_size(64)
fn base64_encode(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // Output word t: the 4 symbols of input bytes 3t..3t + 2, padded with '=' at the end.
    let b = t * 3u;
    let have = min(3u, params.n - b);
    let x = (byte_at(b) << 16u) | (select(0u, byte_at(b + 1u), have > 1u) << 8u)
        | select(0u, byte_at(b + 2u), have > 2u);
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let c = select(61u, b64_symbol((x >> (18u - 6u * k)) & 63u), k <= have);
        word |= c << (8u * k);
    }
    dst[t] = word;
}

fn b64_value(c: u32) -> u32 {
    var v = 0xffu;
    v = select(v, c - 65u, c >= 65u && c <= 90u);
    v = select(v, c - 71u, c >= 97u && c <= 122u);
    v = select(v, c + 4u, c >= 48u && c <= 57u);
    v = select(v, 62u, c == 43u);
    return select(v, 63u, c == 47u);
}

/// The 3 bytes of symbol group `g` as a 24-bit value; reports bad symbols, and with padding
/// on the last group, stray bits in its last symbol.
fn b64_group(g: u32, pad: u32, last: u32) -> u32 {
    var x = 0u;
    let used = select(4u, 4u - pad, g == last);
    for (var k = 0u; k < 4u; k++) {
        let at = g * 4u + k;
        if (k < used) {
            let v = b64_value(byte_at(at));
            if (v == 0xffu) {
                atomicMin(&status[0], at);
            }
            x |= (v & 63u) << (18u - 6u * k);
        }
    }
    if (g == last && pad > 0u) {
        let stray = x & select(0xffu, 0xffffu, pad == 2u);
        if (stray != 0u) {
            atomicMin(&status[0], g * 4u + used - 1u);
        }
    }
    return x;
}

@compute @workgroup_size(64)
fn base64_decode(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let n = params.n;
    let last = n / 4u - 1u;
    // '=' padding counts only at the very end: "xx==" is 2, "xxx=" is 1, "xx=x" is 0 (and its
    // '=' an invalid symbol).
    let pad = select(0u, select(1u, 2u, byte_at(n - 2u) == 61u), byte_at(n - 1u) == 61u);
    let out_len = (last + 1u) * 3u - pad;
    if (t == 0u) {
        atomicStore(&status[1], out_len);
    }
    // Output word t: decoded bytes 4t..4t + 3, from the groups that hold them.
    var word = 0u;
    var cached_group = 0xffffffffu;
    var bits = 0u;
    for (var k = 0u; k < 4u; k++) {
        let j = t * 4u + k;
        if (j < out_len) {
            let g = j / 3u;
            if (g != cached_group) {
                bits = b64_group(g, pad, last);
                cached_group = g;
            }
            word |= ((bits >> (16u - 8u * (j % 3u))) & 0xffu) << (8u * k);
        }
    }
    // Every group has at least one output byte, so every group is checked by some thread.
    dst[t] = word;
}
