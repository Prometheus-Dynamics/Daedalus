//! The kernels in portable SIMD (`core::simd`), the inventory's `portable_simd` column. Nightly
//! only: built with `--cfg daedalus_portable_simd` on the pinned nightly toolchain
//! (`scripts/accel.sh`). The product builds on stable without it. Each function matches its
//! ISA kernel bit for bit; the tails run the scalar loops.

use core::simd::prelude::*;
use core::simd::simd_swizzle;

/// Lane `i` of 16 RGBA pixels takes RGB byte `(i / 4) * 3 + i % 4` (alpha lanes take 0).
const RGB_TO_RGBA: [usize; 64] = {
    let mut a = [0usize; 64];
    let mut i = 0;
    while i < 64 {
        a[i] = if i % 4 == 3 { 0 } else { (i / 4) * 3 + i % 4 };
        i += 1;
    }
    a
};

/// The first 48 lanes take RGBA byte `(i / 3) * 4 + i % 3`.
const RGBA_TO_RGB: [usize; 64] = {
    let mut a = [0usize; 64];
    let mut i = 0;
    while i < 48 {
        a[i] = (i / 3) * 4 + i % 3;
        i += 1;
    }
    a
};

const GRAY_TO_RGBA: [usize; 64] = {
    let mut a = [0usize; 64];
    let mut i = 0;
    while i < 64 {
        a[i] = i / 4;
        i += 1;
    }
    a
};

const RGBA_RED: [usize; 16] = {
    let mut a = [0usize; 16];
    let mut i = 0;
    while i < 16 {
        a[i] = 4 * i;
        i += 1;
    }
    a
};

/// The alpha lanes of 16 RGBA pixels.
fn alpha_lanes() -> Mask<i8, 64> {
    Mask::from_array(core::array::from_fn(|i| i % 4 == 3))
}

#[inline(never)]
pub fn rgb_to_rgba(src: &[u8], dst: &mut [u8]) {
    let px = (src.len() / 3).min(dst.len() / 4);
    let alpha = alpha_lanes();
    let mut i = 0;
    // 16 pixels per step: 48 of 64 loaded bytes.
    while i + 16 <= px && 3 * i + 64 <= src.len() {
        let v = u8x64::from_slice(&src[3 * i..3 * i + 64]);
        let out = alpha.select(u8x64::splat(255), simd_swizzle!(v, RGB_TO_RGBA));
        out.copy_to_slice(&mut dst[4 * i..4 * i + 64]);
        i += 16;
    }
    crate::pixel::rgb_to_rgba::v(&src[3 * i..3 * px], &mut dst[4 * i..4 * px]);
}

#[inline(never)]
pub fn rgba_to_rgb(src: &[u8], dst: &mut [u8]) {
    let px = (src.len() / 4).min(dst.len() / 3);
    let mut i = 0;
    while i + 16 <= px {
        let v = u8x64::from_slice(&src[4 * i..4 * i + 64]);
        let out = simd_swizzle!(v, RGBA_TO_RGB);
        dst[3 * i..3 * i + 48].copy_from_slice(&out.as_array()[..48]);
        i += 16;
    }
    crate::pixel::rgba_to_rgb::v(&src[4 * i..4 * px], &mut dst[3 * i..3 * px]);
}

#[inline(never)]
pub fn gray_to_rgba(src: &[u8], dst: &mut [u8]) {
    let px = src.len().min(dst.len() / 4);
    let alpha = alpha_lanes();
    let mut i = 0;
    while i + 16 <= px {
        let v = u8x16::from_slice(&src[i..i + 16]);
        let out = alpha.select(u8x64::splat(255), simd_swizzle!(v, GRAY_TO_RGBA));
        out.copy_to_slice(&mut dst[4 * i..4 * i + 64]);
        i += 16;
    }
    crate::pixel::gray_to_rgba::v(&src[i..px], &mut dst[4 * i..4 * px]);
}

#[inline(never)]
pub fn rgba_red(src: &[u8], dst: &mut [u8]) {
    let px = (src.len() / 4).min(dst.len());
    let mut i = 0;
    while i + 16 <= px {
        let v = u8x64::from_slice(&src[4 * i..4 * i + 64]);
        simd_swizzle!(v, RGBA_RED).copy_to_slice(&mut dst[i..i + 16]);
        i += 16;
    }
    for (s, d) in src[4 * i..4 * px].chunks_exact(4).zip(&mut dst[i..px]) {
        *d = s[0];
    }
}

#[inline(never)]
pub fn copy_bytes(src: &[u8], dst: &mut [u8]) {
    let n = src.len().min(dst.len());
    let mut i = 0;
    while i + 64 <= n {
        // `black_box` keeps the loads and stores as vector code: without it LLVM turns the loop
        // back into a `memcpy` call (as for the ISA kernels, `copy.rs`).
        core::hint::black_box(u8x64::from_slice(&src[i..i + 64]))
            .copy_to_slice(&mut dst[i..i + 64]);
        i += 64;
    }
    dst[i..n].copy_from_slice(&src[i..n]);
}

/// binary16 to f32, eight per step, with the masks of the ISA kernel.
#[inline(never)]
pub fn f16_to_f32(src: &[u8], dst: &mut [f32]) {
    let n = (src.len() / 2).min(dst.len());
    let mut i = 0;
    while i + 8 <= n {
        let h: u32x8 = u32x8::from_array(core::array::from_fn(|k| {
            u32::from(u16::from_le_bytes([src[2 * (i + k)], src[2 * (i + k) + 1]]))
        }));
        let sign = (h & u32x8::splat(0x8000)) << 16;
        let mant = h & u32x8::splat(0x3ff);
        let shifted = (h & u32x8::splat(0x7fff)) << 13;
        let exp = shifted & u32x8::splat(0x0f80_0000);
        let normal = shifted + u32x8::splat(112 << 23);
        let quiet = mant
            .simd_ne(u32x8::splat(0))
            .select(u32x8::splat(0x0040_0000), u32x8::splat(0));
        let special = (normal + u32x8::splat(112 << 23)) | quiet;
        let sub = (f32x8::from_bits(normal + u32x8::splat(1 << 23))
            - f32x8::splat(f32::from_bits(113 << 23)))
        .to_bits();
        let is_sub = exp.simd_eq(u32x8::splat(0));
        let is_special = exp.simd_eq(u32x8::splat(0x0f80_0000));
        let magnitude = is_sub.select(sub, is_special.select(special, normal));
        f32x8::from_bits(sign | magnitude).copy_to_slice(&mut dst[i..i + 8]);
        i += 8;
    }
    crate::float::f16_to_f32_base::v(&src[2 * i..2 * n], &mut dst[i..n]);
}

/// f32 to unorm8, eight per step.
#[inline(never)]
pub fn f32_to_unorm8(src: &[f32], dst: &mut [u8]) {
    let n = src.len().min(dst.len());
    let mut i = 0;
    while i + 8 <= n {
        let v = f32x8::from_slice(&src[i..i + 8]);
        // `simd_max` returns the non-NaN operand, so NaN becomes 0, as in the ISA kernel.
        let scaled =
            v.simd_max(f32x8::splat(0.0)).simd_min(f32x8::splat(1.0)) * f32x8::splat(255.0);
        let whole: u32x8 = scaled.cast();
        let up = (scaled - whole.cast::<f32>())
            .simd_ge(f32x8::splat(0.5))
            .select(u32x8::splat(1), u32x8::splat(0));
        let out: u8x8 = (whole + up).cast();
        out.copy_to_slice(&mut dst[i..i + 8]);
        i += 8;
    }
    crate::float::f32_to_unorm8::v(&src[i..n], &mut dst[i..n]);
}

/// `0-9a-f` for each nibble lane.
fn hex_digits(n: u8x16) -> u8x16 {
    let nine = n.simd_gt(u8x16::splat(9));
    n + u8x16::splat(b'0') + nine.select(u8x16::splat(b'a' - b'0' - 10), u8x16::splat(0))
}

#[inline(never)]
pub fn hex_lower(src: &[u8], dst: &mut [u8]) {
    let n = src.len().min(dst.len() / 2);
    let mut i = 0;
    while i + 16 <= n {
        let v = u8x16::from_slice(&src[i..i + 16]);
        let (a, b) = hex_digits(v >> 4).interleave(hex_digits(v & u8x16::splat(15)));
        a.copy_to_slice(&mut dst[2 * i..2 * i + 16]);
        b.copy_to_slice(&mut dst[2 * i + 16..2 * i + 32]);
        i += 16;
    }
    crate::hex::hex_lower::v(&src[i..n], &mut dst[2 * i..2 * n]);
}

/// Base64 symbols for 6-bit lanes.
fn b64_symbols(v: u8x16) -> u8x16 {
    let mut c = v + u8x16::splat(b'A');
    c += v
        .simd_ge(u8x16::splat(26))
        .select(u8x16::splat(6), u8x16::splat(0));
    c -= v
        .simd_ge(u8x16::splat(52))
        .select(u8x16::splat(75), u8x16::splat(0));
    c -= v
        .simd_eq(u8x16::splat(62))
        .select(u8x16::splat(15), u8x16::splat(0));
    c - v
        .simd_eq(u8x16::splat(63))
        .select(u8x16::splat(12), u8x16::splat(0))
}

/// Byte `j` of lane `k` (little-endian u32): input bytes `3k + 2, 3k + 1, 3k`, so the lane is the
/// group's 24-bit big-endian value.
const B64_GATHER: [usize; 16] = [2, 1, 0, 0, 5, 4, 3, 3, 8, 7, 6, 6, 11, 10, 9, 9];

/// Whole 3-byte groups to 4 symbols each (like `encode_groups`), four groups per step.
#[inline(never)]
pub fn base64_encode_groups(src: &[u8], dst: &mut [u8]) {
    let groups = (src.len() / 3).min(dst.len() / 4);
    let mut g = 0;
    while g + 4 <= groups && 3 * g + 16 <= src.len() {
        let v = u8x16::from_slice(&src[3 * g..3 * g + 16]);
        let w: u32x4 = u32x4::from_ne_bytes(simd_swizzle!(v, B64_GATHER).to_ne_bytes());
        let six = u32x4::splat(63);
        let idx = ((w >> 18) & six)
            | (((w >> 12) & six) << 8)
            | (((w >> 6) & six) << 16)
            | ((w & six) << 24);
        b64_symbols(u8x16::from_ne_bytes(idx.to_ne_bytes()))
            .copy_to_slice(&mut dst[4 * g..4 * g + 16]);
        g += 4;
    }
    crate::base64::encode_groups::v(&src[3 * g..3 * groups], &mut dst[4 * g..4 * groups]);
}

/// The 6-bit value of each symbol lane, and 0xff for anything outside the alphabet.
fn b64_values(c: u8x16) -> u8x16 {
    let upper = (c - u8x16::splat(b'A')).simd_lt(u8x16::splat(26));
    let lower = (c - u8x16::splat(b'a')).simd_lt(u8x16::splat(26));
    let digit = (c - u8x16::splat(b'0')).simd_lt(u8x16::splat(10));
    let mut v = u8x16::splat(0xff);
    v = upper.select(c - u8x16::splat(b'A'), v);
    v = lower.select(c - u8x16::splat(b'a' - 26), v);
    v = digit.select(c + u8x16::splat(52 - b'0'), v);
    v = c.simd_eq(u8x16::splat(b'+')).select(u8x16::splat(62), v);
    c.simd_eq(u8x16::splat(b'/')).select(u8x16::splat(63), v)
}

/// Lanes 0..12 take decoded bytes 0, 1, 2 of each u32 lane.
const B64_COMPACT: [usize; 16] = [2, 1, 0, 6, 5, 4, 10, 9, 8, 14, 13, 12, 0, 0, 0, 0];

/// Whole 4-symbol groups to 3 bytes each (like `decode_groups`); `false` on an invalid symbol.
#[inline(never)]
pub fn base64_decode_groups(src: &[u8], dst: &mut [u8]) -> bool {
    let groups = (src.len() / 4).min(dst.len() / 3);
    let mut g = 0;
    let mut bad = false;
    while g + 4 <= groups {
        let v = b64_values(u8x16::from_slice(&src[4 * g..4 * g + 16]));
        bad |= v.simd_eq(u8x16::splat(0xff)).any();
        // Lane value from symbols [a, b, c, d] (little-endian bytes): a<<18 | b<<12 | c<<6 | d.
        let w = u32x4::from_ne_bytes(v.to_ne_bytes());
        let m = u32x4::splat(0xff);
        let n = ((w & m) << 18) | (((w >> 8) & m) << 12) | (((w >> 16) & m) << 6) | ((w >> 24) & m);
        let bytes = simd_swizzle!(u8x16::from_ne_bytes(n.to_ne_bytes()), B64_COMPACT);
        dst[3 * g..3 * g + 12].copy_from_slice(&bytes.as_array()[..12]);
        g += 4;
    }
    let rest =
        crate::base64::decode_groups::v(&src[4 * g..4 * groups], &mut dst[3 * g..3 * groups]);
    !bad && rest
}
