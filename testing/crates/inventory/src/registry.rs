//! The CPU rows of the acceleration inventory: the kernels in `backends/kernels-sys` that
//! carry over to `next`, one row per (op, backend). Each row names its file and the conformance
//! test that checks it against the scalar reference.

use daedalus_accel::{Backend, Exempt, Ext, Kernel, Op, Registry, Status};

/// The GPU-free CPU ops, with their scalar references.
pub static OPS: &[Op] = &[
    Op::new(
        "daedalus.float.f32_from_le",
        "little-endian f32 bytes to f32 (GPU readbacks)",
        "daedalus_kernels_sys::reference::f32_from_le",
    ),
    Op::new(
        "daedalus.float.f16_to_f32",
        "binary16 to f32 (half-float readbacks)",
        "daedalus_kernels_sys::reference::f16_to_f32",
    )
    .could_use(&[(Backend::X86Avx2, &[Ext::F16c])]),
    Op::new(
        "daedalus.float.f32_to_unorm8",
        "f32 to unorm8, clamped, rounded half away from zero",
        "daedalus_kernels_sys::reference::f32_to_unorm8",
    ),
    Op::new(
        "daedalus.copy.bytes",
        "byte copy; row re-pitch runs it per row",
        "<[u8]>::copy_from_slice",
    ),
    Op::new(
        "daedalus.codec.hex_lower",
        "lowercase hex",
        "daedalus_kernels_sys::reference::hex_lower",
    ),
    Op::new(
        "daedalus.codec.base64_encode",
        "standard base64 encode",
        "daedalus_kernels_sys::reference::base64_encode",
    ),
    Op::new(
        "daedalus.codec.base64_decode",
        "standard base64 decode",
        "daedalus_kernels_sys::reference::base64_decode",
    ),
    Op::new(
        "daedalus.codec.crc32",
        "CRC-32 (IEEE, reflected); a serial chain per buffer",
        "daedalus_kernels_sys::reference::crc32",
    )
    .not_per_element()
    .could_use(&[(Backend::Rvv, &[Ext::Zvbc])]),
];

/// Every CPU row. The scalar column is the reference itself; each vector build is checked by
/// the conformance test named in its row.
pub static KERNEL_ROWS: &[Kernel] = &[
    // float
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::scalar_references_match_known_answers",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::X86Sse2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::X86Avx2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::Neon64,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::Neon32,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::Rvv,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_from_le",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::scalar_references_match_known_answers",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::X86Sse2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::X86Avx2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::X86Avx2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::f16_to_f32_with_f16c",
    )
    .with_extensions(&[Ext::F16c]),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::Neon64,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::Neon32,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::Rvv,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f16_to_f32",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::scalar_references_match_known_answers",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::X86Sse2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::X86Avx2,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::Neon64,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::Neon32,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::Rvv,
        "backends/kernels-sys/src/float.rs",
        "backends/kernels-sys/tests/conformance.rs::float_conversions",
    ),
    Kernel::implemented(
        "daedalus.float.f32_to_unorm8",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    // copy
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::X86Sse2,
        "backends/kernels-sys/src/copy.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::X86Avx2,
        "backends/kernels-sys/src/copy.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::Neon64,
        "backends/kernels-sys/src/copy.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::Neon32,
        "backends/kernels-sys/src/copy.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::Rvv,
        "backends/kernels-sys/src/copy.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.copy.bytes",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    // hex
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::X86Sse2,
        "backends/kernels-sys/src/hex.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::X86Avx2,
        "backends/kernels-sys/src/hex.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::Neon64,
        "backends/kernels-sys/src/hex.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::Neon32,
        "backends/kernels-sys/src/hex.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::Rvv,
        "backends/kernels-sys/src/hex.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.hex_lower",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    // base64
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::X86Sse2,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::X86Avx2,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::Neon64,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::Neon32,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::Rvv,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_encode",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::Scalar,
        "backends/kernels-sys/src/reference.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::X86Sse2,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::X86Avx2,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::Neon64,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::Neon32,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::Rvv,
        "backends/kernels-sys/src/base64.rs",
        "backends/kernels-sys/tests/conformance.rs::copies_and_codecs",
    ),
    Kernel::implemented(
        "daedalus.codec.base64_decode",
        Backend::PortableSimd,
        "backends/kernels-sys/src/portable.rs",
        "backends/kernels-sys/tests/conformance.rs::portable_simd_kernels",
    ),
    // crc32: the bitwise reference is the scalar column; slice-by-8 is the base build.
    Kernel::implemented(
        "daedalus.codec.crc32",
        Backend::Scalar,
        "backends/kernels-sys/src/crc.rs",
        "backends/kernels-sys/tests/crc.rs::every_runnable_path_agrees_with_the_reference",
    ),
    Kernel::implemented(
        "daedalus.codec.crc32",
        Backend::X86Sse2,
        "backends/kernels-sys/src/crc.rs",
        "backends/kernels-sys/tests/crc.rs::every_runnable_path_agrees_with_the_reference",
    )
    .with_extensions(&[Ext::Pclmulqdq]),
    Kernel::missing(
        "daedalus.codec.crc32",
        Backend::X86Avx2,
        Status::Gap("no AVX2 CRC path; the PCLMULQDQ path serves AVX2 hosts"),
    ),
    Kernel::implemented(
        "daedalus.codec.crc32",
        Backend::Neon64,
        "backends/kernels-sys/src/crc.rs",
        "backends/kernels-sys/tests/crc.rs::every_runnable_path_agrees_with_the_reference",
    )
    .with_extensions(&[Ext::Crc32]),
    Kernel::implemented(
        "daedalus.codec.crc32",
        Backend::Neon32,
        "backends/kernels-sys/src/crc.rs",
        "backends/kernels-sys/tests/crc.rs::every_runnable_path_agrees_with_the_reference",
    ),
    Kernel::implemented(
        "daedalus.codec.crc32",
        Backend::Rvv,
        "backends/kernels-sys/src/crc.rs",
        "backends/kernels-sys/tests/crc.rs::every_runnable_path_agrees_with_the_reference",
    )
    .with_extensions(&[Ext::Zvbc]),
    Kernel::missing(
        "daedalus.codec.crc32",
        Backend::PortableSimd,
        Status::Gap("no core::simd CRC kernel yet"),
    ),
];

/// Source files with SIMD markers that hold no kernel row of their own.
pub static EXEMPT: &[Exempt] = &[
    Exempt {
        path: "backends/kernels-sys/src/lib.rs",
        reason: "the multiversion dispatch macro and the runtime feature checks; each kernel's row names its own file",
    },
    Exempt {
        path: "backends/kernels-sys/src/riscv.rs",
        reason: "riscv_hwprobe detection; it feeds the Zvbc extension column of the crc32 rows",
    },
];

/// The CPU rows as one registry table.
pub static CPU: Registry = Registry {
    repo: "daedalus",
    name: "kernels-sys",
    ops: OPS,
    kernels: KERNEL_ROWS,
    exempt: EXEMPT,
    children: &[],
};
