// Functions of one value, with no buffers: the per-value part of the kernel ops. They are the
// start of `kernels.wgsl`, and they are the whole of the pixel bodies in `kernels/mod.rs`, so a
// value converted in a merged chain and one converted by a kernel op go through the same text.
// Everything is integer arithmetic on bits wherever a float operation could be flushed or folded,
// so the results are bit for bit the CPU reference's.

// The bits of the f32 a binary16 value converts to.
fn half_to_f32_bits(h: u32) -> u32 {
    let sign = (h & 0x8000u) << 16u;
    let e = (h >> 10u) & 0x1fu;
    let m = h & 0x3ffu;
    if (e == 0x1fu) {
        // Infinity, or a NaN made quiet, as F16C and the CPU kernels do.
        return sign | 0x7f800000u | (m << 13u) | select(0u, 0x400000u, m != 0u);
    }
    if (e != 0u) {
        return sign | ((e + 112u) << 23u) | (m << 13u);
    }
    if (m == 0u) {
        return sign;
    }
    // A subnormal, m * 2^-24, normalized with integers: a float multiply could be flushed.
    let p = firstLeadingBit(m);
    return sign | ((p + 103u) << 23u) | ((m << (23u - p)) & 0x7fffffu);
}

// The unorm8 of the f32 with these bits: clamped, scaled, rounded half away from zero. NaN is 0.
fn unorm8(bits: u32) -> u32 {
    // NaN by its bits: a float compare may be folded away.
    if ((bits & 0x7fffffffu) > 0x7f800000u) { return 0u; }
    let s = clamp(bitcast<f32>(bits), 0.0, 1.0) * 255.0;
    let whole = u32(s);
    // WGSL's `round` is half to even.
    return min(whole + select(0u, 1u, s - f32(whole) >= 0.5), 255u);
}

fn hex_digit(n: u32) -> u32 {
    return n + 48u + select(0u, 39u, n > 9u);
}

fn b64_symbol(v: u32) -> u32 {
    var c = v + 65u;
    c += select(0u, 6u, v >= 26u);
    c -= select(0u, 75u, v >= 52u);
    c -= select(0u, 15u, v == 62u);
    c -= select(0u, 12u, v == 63u);
    return c;
}

// The six bits a base64 symbol stands for, or 0xff for a byte that is no symbol.
fn b64_value(c: u32) -> u32 {
    var v = 0xffu;
    v = select(v, c - 65u, c >= 65u && c <= 90u);
    v = select(v, c - 71u, c >= 97u && c <= 122u);
    v = select(v, c + 4u, c >= 48u && c <= 57u);
    v = select(v, 62u, c == 43u);
    return select(v, 63u, c == 47u);
}
