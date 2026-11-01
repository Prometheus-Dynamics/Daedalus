//! CRC-32 (IEEE 802.3, the zlib checksum: reflected polynomial `0xEDB88320`, initial and final
//! `!0`).
//!
//! A CRC is one dependency chain, so it does not vectorize as such. It speeds up with
//! carry-less multiplication (x86 PCLMULQDQ folding) or with CRC instructions (AArch64 `crc`).
//! Without either, it uses slice-by-8 tables.

use daedalus_accel::Backend;

/// CRC-32 of `bytes`: AArch64 CRC instructions or x86 PCLMULQDQ folding when the CPU has them,
/// else slice-by-8.
pub fn crc32(bytes: &[u8]) -> u32 {
    #[cfg(all(target_arch = "riscv64", target_os = "linux", daedalus_rvv))]
    if crate::riscv::has_zvbc() {
        // SAFETY: V is compiled in and the kernel reports Zvbc.
        return unsafe { crc32::rvv_zvbc(bytes) };
    }
    #[cfg(all(feature = "std", target_arch = "aarch64"))]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: the CPU reports the CRC instructions.
        return unsafe { crc32::arm_crc(bytes) };
    }
    #[cfg(all(feature = "std", target_arch = "x86_64"))]
    if std::arch::is_x86_feature_detected!("pclmulqdq")
        && std::arch::is_x86_feature_detected!("sse4.1")
    {
        // SAFETY: the CPU reports PCLMULQDQ and SSE4.1.
        return unsafe { crc32::pclmul(bytes) };
    }
    crc32::v(bytes)
}

/// The CRC-32 of `A ++ B` from `crc32(A)`, `crc32(B)` and `B`'s length (zlib's
/// `crc32_combine`): `crc_a` times `x^(8 len_b)` modulo the polynomial, plus `crc_b`. Joins CRCs
/// computed per chunk in parallel (the GPU form).
pub fn crc32_combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    mul_mod_p(x_pow_2n_8(len_b), crc_a) ^ crc_b
}

/// `a * b` modulo the reflected polynomial (bit 31 is `x^0`).
fn mul_mod_p(a: u32, mut b: u32) -> u32 {
    let mut m = 1u32 << 31;
    let mut p = 0u32;
    loop {
        if a & m != 0 {
            p ^= b;
            if a & (m - 1) == 0 {
                return p;
            }
        }
        m >>= 1;
        b = if b & 1 != 0 {
            (b >> 1) ^ 0xEDB8_8320
        } else {
            b >> 1
        };
    }
}

/// `x^(8 n)` modulo the polynomial, by squaring.
fn x_pow_2n_8(mut n: u64) -> u32 {
    // x^(2^k) for k = 3 onward: x^8, x^16, ...
    let mut power = 1u32 << (31 - 8); // x^8
    let mut p = 1u32 << 31; // 1
    while n != 0 {
        if n & 1 != 0 {
            p = mul_mod_p(power, p);
        }
        power = mul_mod_p(power, power);
        n >>= 1;
    }
    p
}

/// `TABLES[k][b]`: the CRC of byte `b` followed by `k` zero bytes.
const TABLES: [[u32; 256]; 8] = {
    let mut t = [[0u32; 256]; 8];
    let mut b = 0;
    while b < 256 {
        let mut crc = b as u32;
        let mut i = 0;
        while i < 8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
            i += 1;
        }
        t[0][b] = crc;
        b += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut b = 0;
        while b < 256 {
            let prev = t[k - 1][b];
            t[k][b] = (prev >> 8) ^ t[0][(prev & 0xff) as usize];
            b += 1;
        }
        k += 1;
    }
    t
};

/// Continue a raw (not inverted) CRC state over `bytes` with slice-by-8.
fn slice8(mut crc: u32, bytes: &[u8]) -> u32 {
    let (blocks, rest) = bytes.as_chunks::<8>();
    for b in blocks {
        let lo = crc ^ u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        crc = TABLES[7][(lo & 0xff) as usize]
            ^ TABLES[6][((lo >> 8) & 0xff) as usize]
            ^ TABLES[5][((lo >> 16) & 0xff) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][usize::from(b[4])]
            ^ TABLES[2][usize::from(b[5])]
            ^ TABLES[1][usize::from(b[6])]
            ^ TABLES[0][usize::from(b[7])];
    }
    for &b in rest {
        crc = (crc >> 8) ^ TABLES[0][((crc ^ u32::from(b)) & 0xff) as usize];
    }
    crc
}

/// 64x64 carry-less multiply in software: the 128-bit product as (low, high).
#[cfg(test)]
pub(crate) fn clmul_soft(a: u64, b: u64) -> (u64, u64) {
    let (mut lo, mut hi) = (0u64, 0u64);
    for i in 0..64 {
        if (b >> i) & 1 == 1 {
            lo ^= a << i;
            if i > 0 {
                hi ^= a >> (64 - i);
            }
        }
    }
    (lo, hi)
}

/// The PCLMULQDQ folding algorithm over any 64x64 carry-less multiply `clmul` (128-bit lanes as
/// (low, high) pairs): the RVV Zvbc kernel plugs in `vclmul`/`vclmulh`, the tests a software
/// multiply.
#[cfg(any(test, all(target_arch = "riscv64", target_os = "linux", daedalus_rvv)))]
pub(crate) fn crc32_fold(bytes: &[u8], clmul: &mut dyn FnMut(u64, u64) -> (u64, u64)) -> u32 {
    const K1: u64 = 0x1_5444_2bd4;
    const K2: u64 = 0x1_c6e4_1596;
    const K3: u64 = 0x1_7519_97d0;
    const K4: u64 = 0x0_ccaa_009e;
    const K5: u64 = 0x1_63cd_6124;
    const P_X: u64 = 0x1_DB71_0641;
    const U_PRIME: u64 = 0x1_F701_1641;
    if bytes.len() < 128 {
        return !slice8(!0, bytes);
    }
    let (blocks, rest) = bytes.as_chunks::<16>();
    let load = |i: usize| -> (u64, u64) {
        let b = &blocks[i];
        (
            u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
            u64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]),
        )
    };
    // fold(a, b) = a.lo * k_lo ^ a.hi * k_hi ^ b
    let mut fold = |a: (u64, u64), b: (u64, u64), k_lo: u64, k_hi: u64| {
        let (l1, h1) = clmul(a.0, k_lo);
        let (l2, h2) = clmul(a.1, k_hi);
        (b.0 ^ l1 ^ l2, b.1 ^ h1 ^ h2)
    };
    let mut x3 = load(0);
    x3.0 ^= 0xffff_ffff;
    let (mut x2, mut x1, mut x0) = (load(1), load(2), load(3));
    let mut i = 4;
    while i + 4 <= blocks.len() {
        x3 = fold(x3, load(i), K1, K2);
        x2 = fold(x2, load(i + 1), K1, K2);
        x1 = fold(x1, load(i + 2), K1, K2);
        x0 = fold(x0, load(i + 3), K1, K2);
        i += 4;
    }
    let mut x = fold(x3, x2, K3, K4);
    x = fold(x, x1, K3, K4);
    x = fold(x, x0, K3, K4);
    while i < blocks.len() {
        x = fold(x, load(i), K3, K4);
        i += 1;
    }
    // 128 to 64 bits: x.lo * K4 ^ (x >> 64).
    let (l, h) = clmul(x.0, K4);
    let x = (l ^ x.1, h);
    // 64 to 32: (x & 0xffffffff) * K5 ^ (x >> 32).
    let (l, h) = clmul(x.0 & 0xffff_ffff, K5);
    let x = (l ^ ((x.0 >> 32) | (x.1 << 32)), h ^ (x.1 >> 32));
    // Barrett reduction.
    let (t1, _) = clmul(x.0 & 0xffff_ffff, U_PRIME);
    let (t2, _) = clmul(t1 & 0xffff_ffff, P_X);
    let crc = ((x.0 ^ t2) >> 32) as u32;
    !slice8(crc, rest)
}

#[doc(hidden)]
pub mod crc32 {
    use super::{Backend, slice8};

    /// Slice-by-8 (every target).
    #[inline(never)]
    pub fn v(bytes: &[u8]) -> u32 {
        !slice8(!0, bytes)
    }

    /// AArch64 CRC instructions, 8 bytes per `crc32x`.
    ///
    /// # Safety
    /// The CPU must have the `crc` feature.
    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "crc")]
    #[inline(never)]
    pub unsafe fn arm_crc(bytes: &[u8]) -> u32 {
        use core::arch::aarch64::{__crc32b, __crc32d};
        let mut crc = !0u32;
        let (blocks, rest) = bytes.as_chunks::<8>();
        for b in blocks {
            crc = __crc32d(crc, u64::from_le_bytes(*b));
        }
        for &b in rest {
            crc = __crc32b(crc, b);
        }
        !crc
    }

    /// x86 PCLMULQDQ folding (Intel, "Fast CRC Computation for Generic Polynomials Using
    /// PCLMULQDQ"): four 128-bit lanes fold 64 bytes per step, then a Barrett reduction.
    ///
    /// # Safety
    /// The CPU must have PCLMULQDQ and SSE4.1.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "pclmulqdq,sse4.1")]
    #[inline(never)]
    pub unsafe fn pclmul(bytes: &[u8]) -> u32 {
        use core::arch::x86_64::*;
        // Folding constants for the reflected polynomial (x^(k) mod P, bit-reflected).
        const K1: i64 = 0x1_5444_2bd4;
        const K2: i64 = 0x1_c6e4_1596;
        const K3: i64 = 0x1_7519_97d0;
        const K4: i64 = 0x0_ccaa_009e;
        const K5: i64 = 0x1_63cd_6124;
        const P_X: i64 = 0x1_DB71_0641;
        const U_PRIME: i64 = 0x1_F701_1641;
        if bytes.len() < 128 {
            return !slice8(!0, bytes);
        }
        let fold = |a: __m128i, b: __m128i, keys: __m128i| {
            _mm_xor_si128(
                _mm_xor_si128(b, _mm_clmulepi64_si128(a, keys, 0x00)),
                _mm_clmulepi64_si128(a, keys, 0x11),
            )
        };
        let (blocks, rest) = bytes.as_chunks::<16>();
        // SAFETY: each block is 16 readable bytes; unaligned loads.
        let load = |i: usize| unsafe { _mm_loadu_si128(blocks[i].as_ptr().cast()) };
        let mut x3 = _mm_xor_si128(load(0), _mm_cvtsi32_si128(!0));
        let (mut x2, mut x1, mut x0) = (load(1), load(2), load(3));
        let k1k2 = _mm_set_epi64x(K2, K1);
        let mut i = 4;
        while i + 4 <= blocks.len() {
            x3 = fold(x3, load(i), k1k2);
            x2 = fold(x2, load(i + 1), k1k2);
            x1 = fold(x1, load(i + 2), k1k2);
            x0 = fold(x0, load(i + 3), k1k2);
            i += 4;
        }
        let k3k4 = _mm_set_epi64x(K4, K3);
        let mut x = fold(x3, x2, k3k4);
        x = fold(x, x1, k3k4);
        x = fold(x, x0, k3k4);
        while i < blocks.len() {
            x = fold(x, load(i), k3k4);
            i += 1;
        }
        // 128 to 64 bits, then 64 to 32 with the Barrett reduction.
        let low32 = _mm_set_epi32(0, 0, 0, !0);
        let x = _mm_xor_si128(_mm_clmulepi64_si128(x, k3k4, 0x10), _mm_srli_si128(x, 8));
        let x = _mm_xor_si128(
            _mm_clmulepi64_si128(_mm_and_si128(x, low32), _mm_set_epi64x(0, K5), 0x00),
            _mm_srli_si128(x, 4),
        );
        let pu = _mm_set_epi64x(U_PRIME, P_X);
        let t1 = _mm_clmulepi64_si128(_mm_and_si128(x, low32), pu, 0x10);
        let t2 = _mm_clmulepi64_si128(_mm_and_si128(t1, low32), pu, 0x00);
        let crc = _mm_extract_epi32(_mm_xor_si128(x, t2), 1) as u32;
        // `crc` is the raw state after the blocks; finish the tail.
        !slice8(crc, rest)
    }

    /// RVV with Zvbc: the folding of [`super::crc32_fold`] with each 64x64 carry-less
    /// multiply done by `vclmul`/`vclmulh` (SEW 64). Chosen at runtime (`riscv_hwprobe`), so a
    /// `+v` build runs it on cores with Zvbc and slice-by-8 elsewhere.
    ///
    /// # Safety
    /// The CPU must have V (compiled in) and Zvbc.
    #[cfg(all(target_arch = "riscv64", target_os = "linux", daedalus_rvv))]
    #[inline(never)]
    pub unsafe fn rvv_zvbc(bytes: &[u8]) -> u32 {
        super::crc32_fold(bytes, &mut |a, b| {
            let (lo, hi): (u64, u64);
            // SAFETY: V and Zvbc are compiled in; one element, registers v1-v3 clobbered.
            unsafe {
                core::arch::asm!(
                    ".option push",
                    ".option arch, +zvbc",
                    "vsetivli zero, 1, e64, m1, ta, ma",
                    "vmv.s.x v1, {a}",
                    "vclmul.vx v2, v1, {b}",
                    "vclmulh.vx v3, v1, {b}",
                    "vmv.x.s {lo}, v2",
                    "vmv.x.s {hi}, v3",
                    ".option pop",
                    a = in(reg) a,
                    b = in(reg) b,
                    lo = out(reg) lo,
                    hi = out(reg) hi,
                    out("v1") _, out("v2") _, out("v3") _,
                    options(nostack, nomem),
                );
            }
            (lo, hi)
        })
    }

    /// Run with `backend` and extension: `Scalar` is slice-by-8; `X86Sse2` runs PCLMULQDQ,
    /// `Neon64` the CRC instructions and `Rvv` the Zvbc folding, when the CPU (or build) has
    /// them. `None` otherwise.
    pub fn run(backend: Backend, bytes: &[u8]) -> Option<u32> {
        match backend {
            Backend::Scalar => Some(v(bytes)),
            #[cfg(all(feature = "std", target_arch = "x86_64"))]
            Backend::X86Sse2
                if std::arch::is_x86_feature_detected!("pclmulqdq")
                    && std::arch::is_x86_feature_detected!("sse4.1") =>
            {
                // SAFETY: the CPU reports PCLMULQDQ and SSE4.1.
                Some(unsafe { pclmul(bytes) })
            }
            #[cfg(all(feature = "std", target_arch = "aarch64"))]
            Backend::Neon64 if std::arch::is_aarch64_feature_detected!("crc") => {
                // SAFETY: the CPU reports the CRC instructions.
                Some(unsafe { arm_crc(bytes) })
            }
            #[cfg(all(target_arch = "riscv64", target_os = "linux", daedalus_rvv))]
            // SAFETY: V is compiled in and the kernel reports Zvbc.
            Backend::Rvv if crate::riscv::has_zvbc() => Some(unsafe { rvv_zvbc(bytes) }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate alloc;

    /// The folding algorithm with a software carry-less multiply matches the bitwise CRC (the
    /// same algorithm the RVV Zvbc kernel runs with `vclmul`).
    #[test]
    fn folding_with_a_software_clmul_matches() {
        let data: alloc::vec::Vec<u8> = (0..70_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        for len in [128usize, 129, 191, 192, 255, 256, 4097, 70_000] {
            let want = crate::reference::crc32(&data[..len]);
            assert_eq!(
                super::crc32_fold(&data[..len], &mut super::clmul_soft),
                want,
                "{len}"
            );
        }
        assert_eq!(super::clmul_soft(0b11, 0b11), (0b101, 0));
        assert_eq!(super::clmul_soft(1 << 63, 2), (0, 1));
    }
}
