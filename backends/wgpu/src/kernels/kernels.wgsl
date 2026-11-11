// The kernel ops on buffers: one invocation writes one whole word of the output, since WGSL
// stores no bytes. Words are little-endian, as the device's memory is. `common.wgsl` comes
// before this text.

struct Params {
    // Input elements: floats, halves or bytes, by entry point.
    n: u32,
    // Invocations that write.
    threads: u32,
    // Output bytes, to zero what is past them in the last word.
    out_len: u32,
    unused: u32,
}

@group(0) @binding(0) var<storage, read> src: array<u32>;
@group(0) @binding(1) var<storage, read_write> dst: array<u32>;
// A small buffer of its own for each distinct set of counts, bound as it is: nothing is
// copied before a pass.
@group(0) @binding(2) var<storage, read> params: Params;
// base64 decode: [offset of the first invalid symbol, or 0xffffffff; decoded length].
@group(0) @binding(3) var<storage, read_write> status: array<atomic<u32>>;
// The CRC-32 table.
@group(0) @binding(4) var<storage, read> table: array<u32>;

const WG: u32 = 64u;

fn thread_index(gid: vec3<u32>, groups: vec3<u32>) -> u32 {
    return gid.x + gid.y * groups.x * WG;
}

fn byte_at(i: u32) -> u32 {
    return (src[i >> 2u] >> ((i & 3u) * 8u)) & 0xffu;
}

// The bytes of output word `t` that are before `out_len`.
fn tail_mask(t: u32, word: u32) -> u32 {
    let first = t * 4u;
    if (first + 4u <= params.out_len) {
        return word;
    }
    let keep = params.out_len - first;
    return word & ((1u << (keep * 8u)) - 1u);
}

@compute @workgroup_size(64)
fn f32_from_le(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // The device's words are little-endian: the bits are the float.
    dst[t] = src[t];
}

@compute @workgroup_size(64)
fn f16_to_f32(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = half_to_f32_bits((src[t >> 1u] >> ((t & 1u) * 16u)) & 0xffffu);
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

// Eight words an invocation, not one: a word-a-thread copy of 4 MiB is a million invocations, and
// the device spends its time starting them. `threads` counts the invocations, and the last of
// them may have fewer words to write.
@compute @workgroup_size(64)
fn copy_words(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let words = (params.out_len + 3u) / 4u;
    for (var k = 0u; k < 8u; k++) {
        let w = t * 8u + k;
        if (w < words) {
            dst[w] = tail_mask(w, src[w]);
        }
    }
}

@compute @workgroup_size(64)
fn hex_lower(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // Output word t: the four digits of input bytes 2t and 2t + 1.
    let a = byte_at(2u * t);
    let b = select(0u, byte_at(2u * t + 1u), 2u * t + 1u < params.n);
    let word = hex_digit(a >> 4u) | (hex_digit(a & 15u) << 8u) | (hex_digit(b >> 4u) << 16u)
        | (hex_digit(b & 15u) << 24u);
    dst[t] = tail_mask(t, word);
}

@compute @workgroup_size(64)
fn base64_encode(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    // Output word t: the four symbols of input bytes 3t to 3t + 2, padded with '=' at the end.
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

// The three bytes of symbol group `g` as 24 bits. Reports a byte that is no symbol, and, on a
// padded last group, bits in its last symbol that no byte takes.
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
    // '=' pads only at the very end: "xx==" is 2, "xxx=" is 1, and in "xx=x" it is an invalid
    // symbol.
    let pad = select(0u, select(1u, 2u, byte_at(n - 2u) == 61u), byte_at(n - 1u) == 61u);
    let out_len = (last + 1u) * 3u - pad;
    if (t == 0u) {
        atomicStore(&status[1], out_len);
    }
    // Output word t: decoded bytes 4t to 4t + 3, from the groups that hold them. Every group
    // has a byte in some word, so every group is checked by some invocation.
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
    dst[t] = word;
}

const CRC_CHUNK: u32 = 4096u;

// The CRC-32 of each 4 KiB chunk: invocation t takes the bytes from 4096t. The host combines
// them.
@compute @workgroup_size(64)
fn crc32_chunks(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let start = t * CRC_CHUNK;
    let end = min(start + CRC_CHUNK, params.n);
    var crc = 0xffffffffu;
    // Whole words first: a chunk starts on a word, so four bytes come from one load, in the
    // order the bytes are in memory. The tail, if the count ends inside a word, is byte by byte.
    var w = start / 4u;
    for (; w * 4u + 4u <= end; w++) {
        let v = src[w];
        crc = table[(crc ^ v) & 0xffu] ^ (crc >> 8u);
        crc = table[(crc ^ (v >> 8u)) & 0xffu] ^ (crc >> 8u);
        crc = table[(crc ^ (v >> 16u)) & 0xffu] ^ (crc >> 8u);
        crc = table[(crc ^ (v >> 24u)) & 0xffu] ^ (crc >> 8u);
    }
    for (var i = w * 4u; i < end; i++) {
        crc = table[(crc ^ byte_at(i)) & 0xffu] ^ (crc >> 8u);
    }
    dst[t] = ~crc;
}
