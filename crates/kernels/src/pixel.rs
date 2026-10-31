//! Pixel layout conversions. Each converts `min(src pixels, dst pixels)` pixels.

use crate::multiversion;

multiversion! {
    sse4 = ssse3::rgb_to_rgba;
    /// RGB8 to RGBA8 with alpha 255.
    pub fn rgb_to_rgba(src: &[u8], dst: &mut [u8]) -> () {
        for (s, d) in src.as_chunks::<3>().0.iter().zip(dst.as_chunks_mut::<4>().0) {
            *d = [s[0], s[1], s[2], 255];
        }
    }
}

multiversion! {
    sse4 = ssse3::rgba_to_rgb;
    /// RGBA8 to RGB8, dropping alpha.
    pub fn rgba_to_rgb(src: &[u8], dst: &mut [u8]) -> () {
        for (s, d) in src.as_chunks::<4>().0.iter().zip(dst.as_chunks_mut::<3>().0) {
            *d = [s[0], s[1], s[2]];
        }
    }
}

multiversion! {
    /// Gray8 to RGBA8 (`[g, g, g, 255]`).
    pub fn gray_to_rgba(src: &[u8], dst: &mut [u8]) -> () {
        for (&g, d) in src.iter().zip(dst.as_chunks_mut::<4>().0) {
            *d = [g, g, g, 255];
        }
    }
}

multiversion! {
    /// The red channel of RGBA8 (a gray image stored as RGBA).
    pub fn rgba_red(src: &[u8], dst: &mut [u8]) -> () {
        for (s, d) in src.as_chunks::<4>().0.iter().zip(dst.iter_mut()) {
            *d = s[0];
        }
    }
}

/// SSSE3 `pshufb` kernels for the 3-byte layouts, which LLVM vectorizes only with AVX2.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod ssse3 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::*;

    /// Four RGB pixels (12 of 16 loaded bytes) to four RGBA pixels per step.
    ///
    /// # Safety
    /// The CPU must support SSSE3.
    #[target_feature(enable = "ssse3,sse4.1")]
    #[inline]
    pub unsafe fn rgb_to_rgba(src: &[u8], dst: &mut [u8]) {
        let pixels = (src.len() / 3).min(dst.len() / 4);
        // Every step loads 16 bytes, so the last step must start 16 bytes before the end.
        let steps = if src.len() >= 16 {
            ((src.len() - 16) / 12 + 1).min(pixels / 4)
        } else {
            0
        };
        let shuffle = _mm_setr_epi8(0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, 10, 11, -1);
        let alpha = _mm_set1_epi32(0xff00_0000u32 as i32);
        for i in 0..steps {
            // SAFETY: `12 * i + 16 <= src.len()` and `16 * i + 16 <= dst.len()` by `steps`;
            // unaligned loads and stores.
            unsafe {
                let v = _mm_loadu_si128(src.as_ptr().add(12 * i).cast());
                let v = _mm_or_si128(_mm_shuffle_epi8(v, shuffle), alpha);
                _mm_storeu_si128(dst.as_mut_ptr().add(16 * i).cast(), v);
            }
        }
        let done = steps * 4;
        for (s, d) in src[3 * done..]
            .as_chunks::<3>()
            .0
            .iter()
            .zip(dst[4 * done..].as_chunks_mut::<4>().0)
        {
            *d = [s[0], s[1], s[2], 255];
        }
    }

    /// Four RGBA pixels to four RGB pixels (12 bytes) per step.
    ///
    /// # Safety
    /// The CPU must support SSSE3.
    #[target_feature(enable = "ssse3,sse4.1")]
    #[inline]
    pub unsafe fn rgba_to_rgb(src: &[u8], dst: &mut [u8]) {
        let pixels = (src.len() / 4).min(dst.len() / 3);
        // Every step stores 16 bytes (12 used), so the last step must end 16 bytes before the
        // end of `dst`.
        let steps = if dst.len() >= 16 {
            ((dst.len() - 16) / 12 + 1).min(pixels / 4)
        } else {
            0
        };
        let shuffle = _mm_setr_epi8(0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14, -1, -1, -1, -1);
        for i in 0..steps {
            // SAFETY: `16 * i + 16 <= src.len()` and `12 * i + 16 <= dst.len()` by `steps`.
            unsafe {
                let v = _mm_loadu_si128(src.as_ptr().add(16 * i).cast());
                _mm_storeu_si128(
                    dst.as_mut_ptr().add(12 * i).cast(),
                    _mm_shuffle_epi8(v, shuffle),
                );
            }
        }
        let done = steps * 4;
        for (s, d) in src[4 * done..]
            .as_chunks::<4>()
            .0
            .iter()
            .zip(dst[3 * done..].as_chunks_mut::<3>().0)
        {
            *d = [s[0], s[1], s[2]];
        }
    }
}
