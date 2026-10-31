//! Byte and row copies with explicit vector loads and stores. A plain copy loop is turned back
//! into a `memcpy` call, and musl's `memcpy` on ARMv7 and RISC-V is scalar, so each ISA gets
//! its own loop: SSE2 / AVX2 and NEON intrinsics, NEON32 and RVV inline asm.

use daedalus_accel::Backend;

/// Copy `min(src.len(), dst.len())` bytes.
#[inline]
pub fn copy_bytes(src: &[u8], dst: &mut [u8]) {
    #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
    if crate::has_avx2() {
        // SAFETY: the CPU reports AVX2.
        return unsafe { copy_bytes::avx2(src, dst) };
    }
    copy_bytes::v(src, dst)
}

/// Copy `rows` rows of `row_bytes` bytes between strided buffers (re-pitching). Rows that do
/// not fit in either buffer are not copied.
pub fn copy_rows(
    src: &[u8],
    src_stride: usize,
    dst: &mut [u8],
    dst_stride: usize,
    row_bytes: usize,
    rows: usize,
) {
    if src_stride == row_bytes && dst_stride == row_bytes {
        let n = row_bytes.saturating_mul(rows);
        let n = n.min(src.len()).min(dst.len());
        return copy_bytes(&src[..n], &mut dst[..n]);
    }
    for row in 0..rows {
        let (s, d) = (row * src_stride, row * dst_stride);
        let (Some(s), Some(d)) = (src.get(s..s + row_bytes), dst.get_mut(d..d + row_bytes)) else {
            return;
        };
        copy_bytes(s, d);
    }
}

#[doc(hidden)]
pub mod copy_bytes {
    use super::Backend;

    /// The baseline build: SSE2 on x86_64, NEON on AArch64, NEON32 / RVV asm when compiled in,
    /// else `copy_from_slice`.
    #[inline(never)]
    pub fn v(src: &[u8], dst: &mut [u8]) {
        let n = src.len().min(dst.len());
        let (src, dst) = (&src[..n], &mut dst[..n]);
        #[cfg(target_arch = "x86_64")]
        {
            use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_storeu_si128};
            let (blocks, _) = src.as_chunks::<64>();
            let (out, _) = dst.as_chunks_mut::<64>();
            for (s, d) in blocks.iter().zip(out.iter_mut()) {
                let (s, d) = (
                    s.as_ptr().cast::<__m128i>(),
                    d.as_mut_ptr().cast::<__m128i>(),
                );
                // SAFETY: SSE2 is part of x86_64; each block is 64 valid bytes and the loads and
                // stores are unaligned.
                unsafe {
                    let v = [
                        _mm_loadu_si128(s),
                        _mm_loadu_si128(s.add(1)),
                        _mm_loadu_si128(s.add(2)),
                        _mm_loadu_si128(s.add(3)),
                    ];
                    for (i, v) in v.into_iter().enumerate() {
                        _mm_storeu_si128(d.add(i), v);
                    }
                }
            }
            let done = blocks.len() * 64;
            dst[done..].copy_from_slice(&src[done..]);
        }
        #[cfg(target_arch = "aarch64")]
        {
            use core::arch::aarch64::{vld1q_u8_x4, vst1q_u8_x4};
            let (blocks, _) = src.as_chunks::<64>();
            let (out, _) = dst.as_chunks_mut::<64>();
            for (s, d) in blocks.iter().zip(out.iter_mut()) {
                // SAFETY: NEON is part of AArch64; each block is 64 valid bytes.
                unsafe { vst1q_u8_x4(d.as_mut_ptr(), vld1q_u8_x4(s.as_ptr())) };
            }
            let done = blocks.len() * 64;
            dst[done..].copy_from_slice(&src[done..]);
        }
        #[cfg(daedalus_neon32)]
        {
            let blocks = n / 32;
            if blocks > 0 {
                // SAFETY: `blocks * 32` bytes are valid in both slices; NEON is compiled in.
                unsafe {
                    core::arch::asm!(
                        "2:",
                        "vld1.8 {{d0, d1, d2, d3}}, [{s}]!",
                        "vst1.8 {{d0, d1, d2, d3}}, [{d}]!",
                        "subs {n}, {n}, #1",
                        "bne 2b",
                        s = inout(reg) src.as_ptr() => _,
                        d = inout(reg) dst.as_mut_ptr() => _,
                        n = inout(reg) blocks => _,
                        out("d0") _, out("d1") _, out("d2") _, out("d3") _,
                        options(nostack),
                    );
                }
            }
            let done = blocks * 32;
            dst[done..].copy_from_slice(&src[done..]);
        }
        #[cfg(daedalus_rvv)]
        if n > 0 {
            // SAFETY: the loop reads and writes exactly `n` bytes; V is compiled in. `vsetvli`
            // with e8/m8 moves up to 8 vector registers of bytes per pass, for any VLEN.
            unsafe {
                core::arch::asm!(
                    "2:",
                    "vsetvli {vl}, {n}, e8, m8, ta, ma",
                    "vle8.v v0, ({s})",
                    "vse8.v v0, ({d})",
                    "sub {n}, {n}, {vl}",
                    "add {s}, {s}, {vl}",
                    "add {d}, {d}, {vl}",
                    "bnez {n}, 2b",
                    vl = out(reg) _,
                    s = inout(reg) src.as_ptr() => _,
                    d = inout(reg) dst.as_mut_ptr() => _,
                    n = inout(reg) n => _,
                    out("v0") _, out("v1") _, out("v2") _, out("v3") _,
                    out("v4") _, out("v5") _, out("v6") _, out("v7") _,
                    options(nostack),
                );
            }
        }
        #[cfg(not(any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            daedalus_neon32,
            daedalus_rvv,
        )))]
        dst.copy_from_slice(src);
    }

    /// AVX2: 128 bytes per pass in four 32-byte registers.
    ///
    /// # Safety
    /// The CPU must support AVX2.
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    #[inline(never)]
    pub unsafe fn avx2(src: &[u8], dst: &mut [u8]) {
        #[cfg(target_arch = "x86")]
        use core::arch::x86::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256};
        #[cfg(target_arch = "x86_64")]
        use core::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256};
        let n = src.len().min(dst.len());
        let (src, dst) = (&src[..n], &mut dst[..n]);
        let (blocks, _) = src.as_chunks::<128>();
        let (out, _) = dst.as_chunks_mut::<128>();
        for (s, d) in blocks.iter().zip(out.iter_mut()) {
            let (s, d) = (
                s.as_ptr().cast::<__m256i>(),
                d.as_mut_ptr().cast::<__m256i>(),
            );
            // SAFETY: AVX2 is enabled; each block is 128 valid bytes; unaligned loads/stores.
            unsafe {
                let v = [
                    _mm256_loadu_si256(s),
                    _mm256_loadu_si256(s.add(1)),
                    _mm256_loadu_si256(s.add(2)),
                    _mm256_loadu_si256(s.add(3)),
                ];
                for (i, v) in v.into_iter().enumerate() {
                    _mm256_storeu_si256(d.add(i), v);
                }
            }
        }
        let done = blocks.len() * 128;
        dst[done..].copy_from_slice(&src[done..]);
    }

    /// Run on `backend`; `None` when this process cannot run it.
    pub fn run(backend: Backend, src: &[u8], dst: &mut [u8]) -> Option<()> {
        #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
        if backend == Backend::X86Avx2 {
            if !crate::has_avx2() {
                return None;
            }
            // SAFETY: the CPU reports AVX2.
            unsafe { avx2(src, dst) };
            return Some(());
        }
        (backend == crate::compiled_backend()).then(|| v(src, dst))
    }
}
