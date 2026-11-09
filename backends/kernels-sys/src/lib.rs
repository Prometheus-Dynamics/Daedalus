//! Daedalus's data-parallel CPU kernels, one implementation per ISA:
//!
//! - **x86_64:** the SSE2 baseline, and SSE4.1 and AVX2 chosen at runtime (`std`).
//! - **AArch64:** NEON (baseline).
//! - **ARMv7:** NEON when built with `-C target-feature=+neon` (stable Rust cannot detect it at
//!   runtime, so the build decides).
//! - **RISC-V:** RVV 1.0 when built with `-C target-feature=+v` (likewise).
//!
//! The kernels are plain loops the compiler vectorizes for each target. The conformance tests
//! check every ISA against the independent scalar references in [`reference`](mod@reference).
//! This crate does not depend on the acceleration inventory; that lives in `testing/crates`.
#![no_std]
#![cfg_attr(daedalus_portable_simd, feature(portable_simd))]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod base64;
mod copy;
#[cfg(feature = "crc")]
mod crc;
mod float;
mod hex;
#[cfg(daedalus_portable_simd)]
pub mod portable;
pub mod reference;
#[cfg(all(feature = "crc", target_arch = "riscv64", target_os = "linux"))]
mod riscv;

pub use base64::{
    Base64Error, base64_decode, base64_decode_into, base64_decoded_len, base64_encode,
    base64_encode_into, base64_encoded_len, decode_groups, encode_groups,
};
pub use copy::{copy_bytes, copy_rows};
#[cfg(feature = "crc")]
pub use crc::{crc32, crc32_combine};
pub use float::{f16_to_f32, f16_to_f32_base, f16_to_f32_with_f16c, f32_from_le, f32_to_unorm8};
pub use hex::hex_lower;

/// The instruction set a kernel entry runs on. Each kernel's `run` takes one to force that
/// entry, and `None` comes back when this process cannot run it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isa {
    /// The portable loops, when no vector build applies.
    Scalar,
    /// x86_64 SSE2 (SSSE3 and SSE4.1 where a kernel has a shuffle path).
    X86Sse2,
    /// x86 AVX2.
    X86Avx2,
    /// AArch64 NEON.
    Neon64,
    /// ARMv7 NEON, built with `+neon`.
    Neon32,
    /// RISC-V vector (RVV 1.0), built with `+v`.
    Rvv,
    /// The `core::simd` kernels (nightly, `daedalus_portable_simd`).
    PortableSimd,
}

/// The ISA the kernels use on this machine: AVX2 when the CPU has it (`std`, x86), otherwise
/// what the target was compiled for.
pub fn active_backend() -> Isa {
    #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
    if has_avx2() {
        return Isa::X86Avx2;
    }
    compiled_backend()
}

/// The ISA the baseline (`v`) kernels are compiled for.
pub const fn compiled_backend() -> Isa {
    if cfg!(any(
        target_arch = "x86_64",
        all(target_arch = "x86", target_feature = "sse2")
    )) {
        Isa::X86Sse2
    } else if cfg!(target_arch = "aarch64") {
        Isa::Neon64
    } else if cfg!(daedalus_neon32) {
        Isa::Neon32
    } else if cfg!(daedalus_rvv) {
        Isa::Rvv
    } else {
        Isa::Scalar
    }
}

#[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
fn x86_level() -> u8 {
    use core::sync::atomic::{AtomicU8, Ordering};
    // 0 unknown, 1 SSE2, 2 SSSE3 + SSE4.1, 3 AVX2.
    static LEVEL: AtomicU8 = AtomicU8::new(0);
    match LEVEL.load(Ordering::Relaxed) {
        0 => {
            let level = if std::arch::is_x86_feature_detected!("avx2") {
                3
            } else if std::arch::is_x86_feature_detected!("ssse3")
                && std::arch::is_x86_feature_detected!("sse4.1")
            {
                2
            } else {
                1
            };
            LEVEL.store(level, Ordering::Relaxed);
            level
        }
        level => level,
    }
}

#[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
fn has_avx2() -> bool {
    x86_level() >= 3
}

#[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
fn has_sse4() -> bool {
    x86_level() >= 2
}

/// Defines a kernel `name` with a module `name` holding its per-ISA entries:
///
/// - `name::v`: the baseline build (SSE2, NEON, RVV, or scalar), `#[inline(never)]` so `accel-
///   matrix asm` finds it;
/// - `name::sse4` and `name::avx2` on x86: the same body compiled with SSSE3 + SSE4.1, and
///   with AVX2 (the SSE and AVX2 columns, chosen at runtime);
/// - `name::run(backend, ...)`: one backend forced, `None` when this process cannot run it
///   (for the conformance tests).
///
/// `name(...)` calls the best entry for this machine. A leading `sse4 = path;` replaces the
/// SSE build with a hand-written `unsafe fn` (SSSE3 + SSE4.1 enabled) of the same signature.
macro_rules! multiversion {
    (sse4 = $sse4:path; $($rest:tt)*) => {
        crate::multiversion!(@with [$sse4] $($rest)*);
    };
    (
        @with [$sse4:path]
        $(#[$meta:meta])*
        pub fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty $body:block
    ) => {
        $(#[$meta])*
        #[inline]
        pub fn $name($($arg: $ty),*) -> $ret {
            #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
            if crate::has_avx2() {
                // SAFETY: the CPU reports AVX2.
                return unsafe { $name::avx2($($arg),*) };
            }
            #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
            if crate::has_sse4() {
                // SAFETY: the CPU reports SSSE3 and SSE4.1.
                return unsafe { $name::sse4($($arg),*) };
            }
            $name::v($($arg),*)
        }

        #[doc(hidden)]
        pub mod $name {
            #[allow(unused_imports)]
            use super::*;

            #[inline(always)]
            fn body($($arg: $ty),*) -> $ret $body

            /// The baseline build of the kernel.
            #[inline(never)]
            pub fn v($($arg: $ty),*) -> $ret {
                body($($arg),*)
            }

            /// The kernel compiled with SSSE3 and SSE4.1 (byte shuffles for the interleaved
            /// layouts), the x86 SSE column.
            ///
            /// # Safety
            /// The CPU must support SSSE3 and SSE4.1.
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            #[target_feature(enable = "ssse3,sse4.1")]
            #[inline(never)]
            #[allow(unused_unsafe)]
            // SAFETY: the caller checks SSSE3 and SSE4.1 first (see the `# Safety` section above).
            pub unsafe fn sse4($($arg: $ty),*) -> $ret {
                // `body`, or a hand-written SSSE3 kernel where the compiler does not
                // vectorize it.
                // SAFETY: this function is only called once SSSE3 and SSE4.1 are checked.
                unsafe { $sse4($($arg),*) }
            }

            /// The kernel compiled with AVX2.
            ///
            /// # Safety
            /// The CPU must support AVX2.
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            #[target_feature(enable = "avx2")]
            #[inline(never)]
            // SAFETY: the caller checks AVX2 first (see the `# Safety` section above).
            pub unsafe fn avx2($($arg: $ty),*) -> $ret {
                body($($arg),*)
            }

            /// Run on `backend`; `None` when this process cannot run it.
            pub fn run(backend: crate::Isa, $($arg: $ty),*) -> Option<$ret> {
                #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
                if backend == crate::Isa::X86Avx2 {
                    if !crate::has_avx2() {
                        return None;
                    }
                    // SAFETY: the CPU reports AVX2.
                    return Some(unsafe { avx2($($arg),*) });
                }
                #[cfg(all(feature = "std", any(target_arch = "x86", target_arch = "x86_64")))]
                if backend == crate::Isa::X86Sse2 && crate::has_sse4() {
                    // SAFETY: the CPU reports SSSE3 and SSE4.1.
                    return Some(unsafe { sse4($($arg),*) });
                }
                (backend == crate::compiled_backend()).then(|| v($($arg),*))
            }
        }
    };
    ($($rest:tt)*) => {
        crate::multiversion!(@with [body] $($rest)*);
    };
}
pub(crate) use multiversion;
