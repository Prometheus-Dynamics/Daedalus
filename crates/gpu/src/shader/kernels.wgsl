// The compute form of the data-parallel kernels (`GpuKernels`): one thread per output word.
// The word functions are in `kernels_common.wgsl`, prepended at load.

@group(0) @binding(0) var<storage, read> src: array<u32>;
@group(0) @binding(1) var<storage, read_write> dst: array<u32>;
@group(0) @binding(2) var<storage, read> params: Params;
// base64 decode: [first invalid symbol offset (0xffffffff when none), decoded length].
@group(0) @binding(3) var<storage, read_write> status: array<atomic<u32>>;

// Auxiliary read-only data: the CRC-32 table for `crc32_chunks` (another buffer otherwise).
@group(0) @binding(4) var<storage, read> aux: array<u32>;

const WG: u32 = 64u;

fn thread_index(gid: vec3<u32>, groups: vec3<u32>) -> u32 {
    return gid.x + gid.y * groups.x * WG;
}

@compute @workgroup_size(64)
fn rgb_to_rgba(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_rgb_to_rgba(t);
}

@compute @workgroup_size(64)
fn rgba_to_rgb(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_rgba_to_rgb(t);
}

@compute @workgroup_size(64)
fn gray_to_rgba(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_gray_to_rgba(t);
}

@compute @workgroup_size(64)
fn rgba_red(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_rgba_red(t);
}

@compute @workgroup_size(64)
fn f32_from_le(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_f32_from_le(t);
}

@compute @workgroup_size(64)
fn f16_to_f32(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_f16_to_f32(t);
}

@compute @workgroup_size(64)
fn f32_to_unorm8(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_f32_to_unorm8(t);
}

@compute @workgroup_size(64)
fn copy_words(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_copy_words(t);
}

@compute @workgroup_size(64)
fn hex_lower(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_hex_lower(t);
}

@compute @workgroup_size(64)
fn base64_encode(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    dst[t] = word_base64_encode(t);
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

const CRC_CHUNK: u32 = 4096u;

// The CRC-32 of each 4 KiB chunk (thread t: bytes 4096t..), table-driven; the host combines them
// (`crc32_combine`).
@compute @workgroup_size(64)
fn crc32_chunks(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {
    let t = thread_index(gid, groups);
    if (t >= params.threads) { return; }
    let start = t * CRC_CHUNK;
    let end = min(start + CRC_CHUNK, params.n);
    var crc = 0xffffffffu;
    for (var i = start; i < end; i++) {
        crc = aux[(crc ^ byte_at(i)) & 0xffu] ^ (crc >> 8u);
    }
    dst[t] = ~crc;
}
