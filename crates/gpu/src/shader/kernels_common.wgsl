// Shared by the compute (`kernels.wgsl`) and fragment forms of the data-parallel kernels on GPU-resident data (`GpuKernels`): the GPU column of the Daedalus ops
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

// `src` and `params` are declared by the module that includes this (compute or fragment).

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

fn word_rgb_to_rgba(t: u32) -> u32 {
    let b = t * 3u;
    return byte_at(b) | (byte_at(b + 1u) << 8u) | (byte_at(b + 2u) << 16u) | 0xff000000u;
}

fn word_rgba_to_rgb(t: u32) -> u32 {
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let j = t * 4u + k;
        if (j < params.out_len) {
            word |= byte_at((j / 3u) * 4u + j % 3u) << (8u * k);
        }
    }
    return word;
}

fn word_gray_to_rgba(t: u32) -> u32 {
    let g = byte_at(t);
    return g | (g << 8u) | (g << 16u) | 0xff000000u;
}

fn word_rgba_red(t: u32) -> u32 {
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let p = t * 4u + k;
        if (p < params.out_len) {
            word |= (src[p] & 0xffu) << (8u * k);
        }
    }
    return word;
}

fn word_f32_from_le(t: u32) -> u32 {
    // GPU words are little-endian: the bits are the float.
    return src[t];
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

fn word_f16_to_f32(t: u32) -> u32 {
    return half_to_f32_bits((src[t >> 1u] >> ((t & 1u) * 16u)) & 0xffffu);
}

fn unorm8(bits: u32) -> u32 {
    // NaN by its bits (a float compare may be folded away).
    if ((bits & 0x7fffffffu) > 0x7f800000u) { return 0u; }
    let s = clamp(bitcast<f32>(bits), 0.0, 1.0) * 255.0;
    let whole = u32(s);
    // Half away from zero, like the CPU kernels (`round` is half-to-even in WGSL).
    return min(whole + select(0u, 1u, s - f32(whole) >= 0.5), 255u);
}

fn word_f32_to_unorm8(t: u32) -> u32 {
    var word = 0u;
    for (var k = 0u; k < 4u; k++) {
        let i = t * 4u + k;
        if (i < params.n) {
            word |= unorm8(src[i]) << (8u * k);
        }
    }
    return word;
}

fn word_copy_words(t: u32) -> u32 {
    return tail_mask(t, src[t]);
}

fn hex_digit(n: u32) -> u32 {
    return n + 48u + select(0u, 39u, n > 9u);
}

fn word_hex_lower(t: u32) -> u32 {
    // Output word t: the 4 digits of input bytes 2t and 2t + 1.
    let a = byte_at(2u * t);
    let b = select(0u, byte_at(2u * t + 1u), 2u * t + 1u < params.n);
    let word = hex_digit(a >> 4u) | (hex_digit(a & 15u) << 8u) | (hex_digit(b >> 4u) << 16u)
        | (hex_digit(b & 15u) << 24u);
    return tail_mask(t, word);
}

fn b64_symbol(v: u32) -> u32 {
    var c = v + 65u;
    c += select(0u, 6u, v >= 26u);
    c -= select(0u, 75u, v >= 52u);
    c -= select(0u, 15u, v == 62u);
    c -= select(0u, 12u, v == 63u);
    return c;
}

fn word_base64_encode(t: u32) -> u32 {
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
    return word;
}
