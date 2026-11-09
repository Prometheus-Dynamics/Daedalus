//! Float conversions for GPU readbacks.

use crate::multiversion;

/// Little-endian f32 bytes to f32 (`min(src.len() / 4, dst.len())` values). On little-endian
/// targets this is a byte copy ([`crate::copy_bytes`]); big-endian targets swap.
pub fn f32_from_le(src: &[u8], dst: &mut [f32]) {
    let n = (src.len() / 4).min(dst.len());
    if cfg!(target_endian = "little") {
        // SAFETY: `n` f32s are `4 * n` initialized bytes, and every bit pattern is an f32.
        let out = unsafe { core::slice::from_raw_parts_mut(dst.as_mut_ptr().cast::<u8>(), 4 * n) };
        crate::copy_bytes(&src[..4 * n], out);
    } else {
        for (s, d) in src.as_chunks::<4>().0.iter().zip(dst.iter_mut()) {
            *d = f32::from_le_bytes(*s);
        }
    }
}

multiversion! {
    /// Little-endian IEEE half-precision bytes to f32, bit-exact (subnormals, infinities and
    /// NaN payloads included). Branch-free, four lanes per step, so it vectorizes everywhere
    /// (ARMv7's loop vectorizer declines the one-at-a-time form).
    pub fn f16_to_f32_base(src: &[u8], dst: &mut [f32]) -> () {
        let (quads, _) = src.as_chunks::<8>();
        let (out, _) = dst.as_chunks_mut::<4>();
        let steps = quads.len().min(out.len());
        for (s, d) in quads.iter().zip(out.iter_mut()) {
            let h = [
                u16::from_le_bytes([s[0], s[1]]),
                u16::from_le_bytes([s[2], s[3]]),
                u16::from_le_bytes([s[4], s[5]]),
                u16::from_le_bytes([s[6], s[7]]),
            ];
            *d = h.map(|h| f32::from_bits(half_bits_to_f32_bits(h)));
        }
        let done = steps * 4;
        for (s, d) in src[2 * done..].as_chunks::<2>().0.iter().zip(dst[done..].iter_mut()) {
            *d = f32::from_bits(half_bits_to_f32_bits(u16::from_le_bytes(*s)));
        }
    }
}

/// One half to f32 bits with masks instead of branches (F. Giesen's `half_to_float`): every
/// case is computed and the right one is kept, so the loop vectorizes on every ISA.
#[inline(always)]
fn half_bits_to_f32_bits(h: u16) -> u32 {
    let h = u32::from(h);
    let sign = (h & 0x8000) << 16;
    let mant = h & 0x3ff;
    // Exponent and mantissa moved into place, exponent rebiased (15 to 127).
    let shifted = (h & 0x7fff) << 13;
    let exp = shifted & 0x0f80_0000;
    let normal = shifted.wrapping_add(112 << 23);
    // Inf/NaN: all-ones exponent; NaN quieted (as F16C's `vcvtph2ps` and `half` do).
    let quiet = mask(mant != 0) & 0x0040_0000;
    let special = normal.wrapping_add(112 << 23) | quiet;
    // Subnormal: (2^-14 + m * 2^-24) - 2^-14, exact in f32.
    let subnormal =
        (f32::from_bits(normal.wrapping_add(1 << 23)) - f32::from_bits(113 << 23)).to_bits();
    let is_sub = mask(exp == 0);
    let is_special = mask(exp == 0x0f80_0000);
    let magnitude =
        (subnormal & is_sub) | (special & is_special) | (normal & !(is_sub | is_special));
    sign | magnitude
}

/// All ones when `b`, else zero.
#[inline(always)]
fn mask(b: bool) -> u32 {
    (b as u32).wrapping_neg()
}

multiversion! {
    /// f32 to unorm8: clamp to `[0, 1]`, scale by 255, round half away from zero; NaN gives 0.
    pub fn f32_to_unorm8(src: &[f32], dst: &mut [u8]) -> () {
        for (&v, d) in src.iter().zip(dst.iter_mut()) {
            // `max` maps NaN to 0 (as `clamp` then `as u8` does); `clamp` itself would keep
            // the NaN.
            #[allow(clippy::manual_clamp)]
            let scaled = v.max(0.0).min(1.0) * 255.0;
            // Round half away from zero without `round` (not in `core`): truncate, then add
            // one when the exact fraction is at least a half (`x + 0.5` would round ties up).
            let whole = scaled as u32;
            let up = (scaled - whole as f32 >= 0.5) as u32;
            *d = (whole + up) as u8;
        }
    }
}

/// binary16 to f32 with F16C (`vcvtph2ps`, eight per instruction), the `x86_avx2+f16c` column.
/// Bit-exact with [`f16_to_f32_base`]: F16C quiets NaNs and converts subnormals exactly.
///
/// # Safety
/// The CPU must support AVX2 and F16C.
#[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
#[target_feature(enable = "avx2,f16c")]
#[inline(never)]
// SAFETY: the caller checks AVX2 and F16C first (see the `# Safety` section above).
pub unsafe fn f16_to_f32_f16c(src: &[u8], dst: &mut [f32]) {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{_mm_loadu_si128, _mm256_cvtph_ps, _mm256_storeu_ps};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{_mm_loadu_si128, _mm256_cvtph_ps, _mm256_storeu_ps};
    let n = (src.len() / 2).min(dst.len());
    let blocks = n / 8;
    for i in 0..blocks {
        // SAFETY: 16 source bytes and 8 destination floats are in bounds (`i < n / 8`).
        unsafe {
            let h = _mm_loadu_si128(src.as_ptr().add(16 * i).cast());
            _mm256_storeu_ps(dst.as_mut_ptr().add(8 * i), _mm256_cvtph_ps(h));
        }
    }
    let done = blocks * 8;
    f16_to_f32_base::v(&src[2 * done..2 * n], &mut dst[done..n]);
}

/// The F16C kernel (`f16_to_f32_f16c`) when the CPU has AVX2 and F16C (`std`); `None` otherwise.
pub fn f16_to_f32_with_f16c(src: &[u8], dst: &mut [f32]) -> Option<()> {
    #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
    if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("f16c") {
        // SAFETY: the CPU reports AVX2 and F16C.
        unsafe { f16_to_f32_f16c(src, dst) };
        return Some(());
    }
    let _ = (src, dst);
    None
}

/// Little-endian binary16 bytes to f32 (`min(src.len() / 2, dst.len())` values), bit-exact:
/// F16C on x86 when the CPU has it, else the vectorized [`f16_to_f32_base`].
pub fn f16_to_f32(src: &[u8], dst: &mut [f32]) {
    if f16_to_f32_with_f16c(src, dst).is_none() {
        f16_to_f32_base(src, dst);
    }
}
