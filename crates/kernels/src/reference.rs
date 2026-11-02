//! Scalar references: deliberately simple, per-element, written independently of the
//! vectorized kernels. Conformance tests compare every backend against these.

extern crate alloc;

use alloc::vec::Vec;

pub fn rgb_to_rgba(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..src.len() / 3 {
        out.extend_from_slice(&[src[3 * i], src[3 * i + 1], src[3 * i + 2], 255]);
    }
    out
}

pub fn rgba_to_rgb(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..src.len() / 4 {
        out.extend_from_slice(&src[4 * i..4 * i + 3]);
    }
    out
}

pub fn gray_to_rgba(src: &[u8]) -> Vec<u8> {
    src.iter().flat_map(|&g| [g, g, g, 255]).collect()
}

pub fn rgba_red(src: &[u8]) -> Vec<u8> {
    (0..src.len() / 4).map(|i| src[4 * i]).collect()
}

pub fn f32_from_le(src: &[u8]) -> Vec<f32> {
    (0..src.len() / 4)
        .map(|i| {
            f32::from_bits(u32::from_le_bytes([
                src[4 * i],
                src[4 * i + 1],
                src[4 * i + 2],
                src[4 * i + 3],
            ]))
        })
        .collect()
}

/// IEEE 754 binary16 to binary32 by cases (NaN quieted).
pub fn f16_to_f32(src: &[u8]) -> Vec<f32> {
    (0..src.len() / 2)
        .map(|i| {
            let h = u16::from_le_bytes([src[2 * i], src[2 * i + 1]]);
            let sign = if h & 0x8000 != 0 { -1.0f32 } else { 1.0 };
            let exp = i32::from((h >> 10) & 0x1f);
            let mant = u32::from(h & 0x3ff);
            match exp {
                0 => sign * (mant as f32) * pow2(-24),
                0x1f if mant == 0 => sign * f32::INFINITY,
                0x1f => f32::from_bits((u32::from(h & 0x8000) << 16) | 0x7fc0_0000 | (mant << 13)),
                _ => sign * (1.0 + mant as f32 / 1024.0) * pow2(exp - 15),
            }
        })
        .collect()
}

/// `2^e` by repeated doubling or halving (`powi` is not in `core`).
fn pow2(e: i32) -> f32 {
    let mut x = 1.0f32;
    for _ in 0..e.unsigned_abs() {
        x = if e < 0 { x * 0.5 } else { x * 2.0 };
    }
    x
}

/// `clamp(0, 1) * 255`, rounded half away from zero, NaN to 0 (the GPU readback rule).
pub fn f32_to_unorm8(src: &[f32]) -> Vec<u8> {
    src.iter()
        .map(|&v| {
            if v.is_nan() {
                return 0;
            }
            let x = f64::from(v.clamp(0.0, 1.0) * 255.0);
            let floor = x as u32;
            (if x - f64::from(floor) >= 0.5 {
                floor + 1
            } else {
                floor
            }) as u8
        })
        .collect()
}

pub fn hex_lower(src: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    src.iter()
        .flat_map(|&b| [DIGITS[usize::from(b >> 4)], DIGITS[usize::from(b & 15)]])
        .collect()
}

/// CRC-32 (IEEE), one bit at a time.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding, bit by bit.
pub fn base64_encode(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for group in src.chunks(3) {
        let mut bits = 0u32;
        for (i, &b) in group.iter().enumerate() {
            bits |= u32::from(b) << (16 - 8 * i);
        }
        let symbols = group.len() + 1;
        for i in 0..4 {
            if i < symbols {
                out.push(ALPHABET[((bits >> (18 - 6 * i)) & 63) as usize]);
            } else {
                out.push(b'=');
            }
        }
    }
    out
}

/// Standard base64 decode; `None` for anything the canonical engine rejects.
pub fn base64_decode(src: &[u8]) -> Option<Vec<u8>> {
    if !src.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::new();
    let groups = src.len() / 4;
    for (g, group) in src.chunks(4).enumerate() {
        let pad = group.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && g + 1 != groups) {
            return None;
        }
        let mut bits = 0u32;
        for (i, &c) in group[..4 - pad].iter().enumerate() {
            let v = ALPHABET.iter().position(|&a| a == c)? as u32;
            bits |= v << (18 - 6 * i);
        }
        let bytes = 3 - pad;
        let unused_mask = (1u32 << (8 * (3 - bytes))) - 1;
        if bits & unused_mask != 0 {
            return None;
        }
        for i in 0..bytes {
            out.push((bits >> (16 - 8 * i)) as u8);
        }
    }
    Some(out)
}

/// Area downscale by an integer `factor` of tightly packed `channels`-byte pixels. The output is
/// `max(1, width / factor)` by `max(1, height / factor)`: blocks start at the origin and a
/// trailing partial block is dropped, unless a side is shorter than `factor`, where its one
/// block is what there is. Each output byte is the block's rounded mean, `(sum + n / 2) / n`
/// (for 2x, `(a + b + c + d + 2) >> 2`). Returns the bytes and the output size.
pub fn resize_area(
    src: &[u8],
    width: usize,
    height: usize,
    channels: usize,
    factor: usize,
) -> (Vec<u8>, usize, usize) {
    let (ow, oh) = ((width / factor).max(1), (height / factor).max(1));
    let mut out = Vec::new();
    for oy in 0..oh {
        for ox in 0..ow {
            for c in 0..channels {
                let (mut sum, mut n) = (0usize, 0usize);
                for y in oy * factor..((oy + 1) * factor).min(height) {
                    for x in ox * factor..((ox + 1) * factor).min(width) {
                        sum += src[(y * width + x) * channels + c] as usize;
                        n += 1;
                    }
                }
                out.push(((sum + n / 2) / n) as u8);
            }
        }
    }
    (out, ow, oh)
}

/// The source index and 11-bit weight of the next sample for output `d` of `dst` along an
/// axis of `src`: half-pixel centres, `(d + 0.5) * src / dst - 0.5`, clamped at the edges.
fn linear_tap(d: usize, src: usize, dst: usize) -> (usize, u32) {
    // In units of 1 / (2 * dst) source pixels.
    let pos = ((2 * d + 1) * src) as i64 - dst as i64;
    let unit = 2 * dst as i64;
    if pos < 0 {
        return (0, 0);
    }
    let (s, rem) = ((pos / unit) as usize, pos % unit);
    if s >= src - 1 {
        return (src - 1, 0);
    }
    (s, ((rem * 2048 + unit / 2) / unit) as u32)
}

/// Bilinear resize of tightly packed `channels`-byte pixels, as OpenCV's `INTER_LINEAR` on
/// 8-bit data: half-pixel centres, clamped edges, 11-bit weights (rounded half up) and
/// `(v + 2^21) >> 22` after both passes.
pub fn resize_bilinear(
    src: &[u8],
    width: usize,
    height: usize,
    channels: usize,
    out_width: usize,
    out_height: usize,
) -> Vec<u8> {
    let at = |x: usize, y: usize, c: usize| src[(y * width + x) * channels + c] as u32;
    let mut out = Vec::new();
    for oy in 0..out_height {
        let (y0, wy) = linear_tap(oy, height, out_height);
        let y1 = (y0 + 1).min(height - 1);
        for ox in 0..out_width {
            let (x0, wx) = linear_tap(ox, width, out_width);
            let x1 = (x0 + 1).min(width - 1);
            for c in 0..channels {
                let row = |y| at(x0, y, c) * (2048 - wx) + at(x1, y, c) * wx;
                let v = row(y0) * (2048 - wy) + row(y1) * wy;
                out.push(((v + (1 << 21)) >> 22) as u8);
            }
        }
    }
    out
}

/// 8-bit indices to RGBA8 through a 256-entry palette.
pub fn palette(indices: &[u8], lut: &[[u8; 4]; 256]) -> Vec<u8> {
    indices.iter().flat_map(|&i| lut[i as usize]).collect()
}

/// A colour-key mask of `channels`-byte pixels (R, G, B first): 255 where each of R, G and B
/// lies in `min..=max`, else 0.
pub fn color_key_mask(src: &[u8], channels: usize, min: [u8; 3], max: [u8; 3]) -> Vec<u8> {
    src.chunks_exact(channels)
        .map(|p| {
            let inside = (0..3).all(|c| min[c] <= p[c] && p[c] <= max[c]);
            if inside { 255 } else { 0 }
        })
        .collect()
}
