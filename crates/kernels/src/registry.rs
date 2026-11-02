//! Daedalus's entries in the acceleration inventory (`docs/backend-matrix.md`).

use daedalus_accel::{Backend, CallSite, Exempt, Ext, Kernel, Op, Registry, Status};

mod fixed;
use fixed::{IMAGE_TABLE, RGA_TABLE};

/// Unused once every op has a `core::simd` kernel; kept for ops added without one.
#[allow(dead_code)]
const PORTABLE: Status = Status::Gap("no core::simd kernel yet (portable.rs)");
const NO_FIXED: Status =
    Status::Impossible("no fixed-function unit on the supported boards works on host memory");

/// The CPU rows every vectorized kernel has: scalar reference, the x86 SSE build (`sse`), the
/// baseline build for NEON / NEON32 / RVV (`v`) and AVX2, with the asm symbols the matrix
/// checks.
macro_rules! cpu_kernels {
    ($op:expr, $file:expr, $test:expr, $sse:expr, $v:expr, $avx2:expr) => {
        cpu_kernels!($op, $file, $test, $sse, $v, $avx2; Kernel::missing($op, Backend::FixedFunction, NO_FIXED))
    };
    ($op:expr, $file:expr, $test:expr, $sse:expr, $v:expr, $avx2:expr; $fixed:expr) => {
        [
            Kernel::implemented(
                $op,
                Backend::Scalar,
                "crates/kernels/src/reference.rs",
                $test,
            ),
            Kernel::implemented($op, Backend::X86Sse2, $file, $test).with_symbol($sse),
            Kernel::implemented($op, Backend::X86Avx2, $file, $test).with_symbol($avx2),
            Kernel::implemented($op, Backend::Neon64, $file, $test).with_symbol($v),
            Kernel::implemented($op, Backend::Neon32, $file, $test).with_symbol($v),
            Kernel::implemented($op, Backend::Rvv, $file, $test).with_symbol($v),
            Kernel::missing($op, Backend::PortableSimd, PORTABLE),
            $fixed,
        ]
    };
    ($op:expr, $file:expr, $test:expr, $v:expr, $avx2:expr) => {
        cpu_kernels!($op, $file, $test, $v, $v, $avx2)
    };
}

/// The GPU compute kernel of an op on GPU-resident data (`GpuKernels`, `kernels.wgsl`),
/// bit-exact with the CPU kernels; its evidence carries the measured time against them.
const fn gpu_compute(op: &'static str) -> Kernel {
    Kernel::implemented(
        op,
        Backend::Gpu,
        "crates/gpu/src/shader/kernels.wgsl",
        "crates/gpu/src/kernels_tests.rs",
    )
    .with_variant("compute")
}

const PIXEL: &str = "crates/kernels/src/pixel.rs";
const FLOAT: &str = "crates/kernels/src/float.rs";
const COPY: &str = "crates/kernels/src/copy.rs";
const CODEC_HEX: &str = "crates/kernels/src/hex.rs";
const CODEC_B64: &str = "crates/kernels/src/base64.rs";

const fn op(id: &'static str, summary: &'static str, scalar: &'static str) -> Op {
    Op::new(id, summary, scalar)
}

static SITES_PIXEL_RGB_TO_RGBA: [CallSite; 1] =
    [
        CallSite::new("crates/gpu/src/convert.rs", "daedalus_kernels::rgb_to_rgba")
            .forbid(&["ImageBuffer::from_fn"]),
    ];

static SITES_PIXEL_RGBA_TO_RGB: [CallSite; 1] =
    [
        CallSite::new("crates/gpu/src/convert.rs", "daedalus_kernels::rgba_to_rgb")
            .forbid(&["image::Rgb([p[0], p[1], p[2]])"]),
    ];

static SITES_PIXEL_GRAY_TO_RGBA: [CallSite; 1] = [CallSite::new(
    "crates/gpu/src/convert.rs",
    "daedalus_kernels::gray_to_rgba",
)
.forbid(&["extend_from_slice(&[v, v, v, 255])"])];

static SITES_PIXEL_RGBA_RED: [CallSite; 1] =
    [
        CallSite::new("crates/gpu/src/convert.rs", "daedalus_kernels::rgba_red")
            .forbid(&[".map(|rgba| rgba[0])"]),
    ];

static SITES_FLOAT_F32_FROM_LE: [CallSite; 1] = [CallSite::new(
    "crates/gpu/src/shader/run_output.rs",
    "daedalus_kernels::f32_from_le",
)
.forbid(&["f32::from_le_bytes"])];

static SITES_FLOAT_F16_TO_F32: [CallSite; 1] = [CallSite::new(
    "crates/gpu/src/shader/run_output.rs",
    "daedalus_kernels::f16_to_f32",
)
.forbid(&["half::f16"])];

static SITES_FLOAT_F32_TO_UNORM8: [CallSite; 1] = [CallSite::new(
    "crates/gpu/src/shader/run_output.rs",
    "daedalus_kernels::f32_to_unorm8",
)
.forbid(&["(clamped * 255.0).round()"])];

static SITES_COPY_BYTES: [CallSite; 6] = [
    CallSite::new(
        "crates/gpu/src/wgpu_backend.rs",
        "daedalus_kernels::copy_rows",
    ),
    CallSite::new(
        "crates/gpu/src/wgpu_backend/texture_transfer.rs",
        "daedalus_kernels::copy_rows",
    )
    .forbid(&["out.extend_from_slice(&src[start"]),
    CallSite::new(
        "crates/gpu/src/shader/prepare/prepare_texture.rs",
        "daedalus_kernels::copy_rows",
    )
    .forbid(&[".copy_from_slice(&bytes[src_start"]),
    CallSite::new(
        "crates/gpu/src/shader/readback.rs",
        "daedalus_kernels::copy_rows",
    )
    .forbid(&["trimmed.extend_from_slice"]),
    CallSite::new(
        "crates/gpu/src/shader/readback_async.rs",
        "daedalus_kernels::copy_rows",
    )
    .forbid(&["trimmed.extend_from_slice"]),
    CallSite::new(
        "crates/gpu/src/shader/gpu_storage.rs",
        "daedalus_kernels::copy_rows",
    )
    .forbid(&["dst.copy_from_slice(&src[..unpadded])"]),
];

static SITES_CODEC_HEX_LOWER: [CallSite; 1] = [CallSite::new(
    "crates/ffi/core/src/schema_package/package_support.rs",
    "daedalus_kernels::hex_lower",
)
.forbid(&["b\"0123456789abcdef\""])];

static SITES_CODEC_BASE64_ENCODE: [CallSite; 2] = [
    CallSite::new("crates/data/src/json.rs", "base64_encode(").forbid(&["B64.encode"]),
    CallSite::new(
        "crates/ffi/core/src/wire_protocol/wire_value.rs",
        "daedalus_kernels::base64_encode",
    )
    .forbid(&["STANDARD.encode"]),
];

static SITES_CODEC_BASE64_DECODE: [CallSite; 2] = [
    CallSite::new("crates/data/src/json.rs", "base64_decode(").forbid(&["B64.decode"]),
    CallSite::new(
        "crates/ffi/core/src/wire_protocol/wire_value.rs",
        "daedalus_kernels::base64_decode",
    )
    .forbid(&["general_purpose::STANDARD"]),
];

static SITES_GPU_STORAGE_TO_TEXTURE: [CallSite; 1] = [CallSite::new(
    "crates/gpu/src/shader/gpu_texture.rs",
    "fn copy_folded",
)];

static OPS: [Op; 12] = [
    op(
        "daedalus.pixel.rgb_to_rgba",
        "RGB8 to RGBA8 (GPU upload of RgbImage)",
        "daedalus_kernels::reference::rgb_to_rgba",
    )
    .wired_at(&SITES_PIXEL_RGB_TO_RGBA),
    op(
        "daedalus.pixel.rgba_to_rgb",
        "RGBA8 to RGB8 (GPU download to RgbImage)",
        "daedalus_kernels::reference::rgba_to_rgb",
    )
    .wired_at(&SITES_PIXEL_RGBA_TO_RGB),
    op(
        "daedalus.pixel.gray_to_rgba",
        "Gray8 to RGBA8 (GrayImage upload without R8)",
        "daedalus_kernels::reference::gray_to_rgba",
    )
    .wired_at(&SITES_PIXEL_GRAY_TO_RGBA),
    op(
        "daedalus.pixel.rgba_red",
        "RGBA8 red channel (GrayImage download from RGBA)",
        "daedalus_kernels::reference::rgba_red",
    )
    .wired_at(&SITES_PIXEL_RGBA_RED),
    op(
        "daedalus.float.f32_from_le",
        "LE bytes to f32 (r32float readback)",
        "daedalus_kernels::reference::f32_from_le",
    )
    .wired_at(&SITES_FLOAT_F32_FROM_LE),
    op(
        "daedalus.float.f16_to_f32",
        "binary16 to f32 (rgba16float readback)",
        "daedalus_kernels::reference::f16_to_f32",
    )
    .could_use(&[(Backend::X86Avx2, &[Ext::F16c])])
    .wired_at(&SITES_FLOAT_F16_TO_F32),
    op(
        "daedalus.float.f32_to_unorm8",
        "f32 to unorm8 (r32float to gray8 readback)",
        "daedalus_kernels::reference::f32_to_unorm8",
    )
    .wired_at(&SITES_FLOAT_F32_TO_UNORM8),
    op(
        "daedalus.copy.bytes",
        "byte copy; row re-pitch on upload and readback runs it per row",
        "<[u8]>::copy_from_slice",
    )
    .wired_at(&SITES_COPY_BYTES),
    op(
        "daedalus.codec.hex_lower",
        "lowercase hex (package digests)",
        "daedalus_kernels::reference::hex_lower",
    )
    .wired_at(&SITES_CODEC_HEX_LOWER),
    op(
        "daedalus.codec.base64_encode",
        "standard base64 encode (JSON and FFI wire bytes)",
        "daedalus_kernels::reference::base64_encode",
    )
    .wired_at(&SITES_CODEC_BASE64_ENCODE),
    op(
        "daedalus.gpu.storage_to_texture",
        "a frame in a storage buffer (any stride) into a sampled texture, on the device",
        "crates/gpu/src/fragment_rect_tests.rs (rows de-strided on the CPU)",
    )
    .wired_at(&SITES_GPU_STORAGE_TO_TEXTURE),
    op(
        "daedalus.codec.base64_decode",
        "standard base64 decode (JSON and FFI wire bytes)",
        "daedalus_kernels::reference::base64_decode",
    )
    .wired_at(&SITES_CODEC_BASE64_DECODE),
];

const T: &str = "crates/kernels/tests/conformance.rs";

static PIXEL_KERNELS: [[Kernel; 9]; 4] = [
    with_gpu(
        cpu_kernels!(
            "daedalus.pixel.rgb_to_rgba",
            PIXEL,
            T,
            "rgb_to_rgba::sse4",
            "rgb_to_rgba::v",
            "rgb_to_rgba::avx2"
        ),
        gpu_compute("daedalus.pixel.rgb_to_rgba"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.pixel.rgba_to_rgb",
            PIXEL,
            T,
            "rgba_to_rgb::sse4",
            "rgba_to_rgb::v",
            "rgba_to_rgb::avx2"
        ),
        gpu_compute("daedalus.pixel.rgba_to_rgb"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.pixel.gray_to_rgba",
            PIXEL,
            T,
            "gray_to_rgba::sse4",
            "gray_to_rgba::v",
            "gray_to_rgba::avx2"
        ),
        gpu_compute("daedalus.pixel.gray_to_rgba"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.pixel.rgba_red",
            PIXEL,
            T,
            "rgba_red::sse4",
            "rgba_red::v",
            "rgba_red::avx2"
        ),
        gpu_compute("daedalus.pixel.rgba_red"),
    ),
];

static FLOAT_KERNELS: [[Kernel; 9]; 3] = [
    with_gpu(
        cpu_kernels!(
            "daedalus.float.f32_from_le",
            FLOAT,
            T,
            "copy_bytes::v",
            "copy_bytes::v",
            "copy_bytes::avx2"
        ),
        gpu_compute("daedalus.float.f32_from_le"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.float.f16_to_f32",
            FLOAT,
            T,
            "f16_to_f32_base::sse4",
            "f16_to_f32_base::v",
            "f16_to_f32_base::avx2"
        ),
        gpu_compute("daedalus.float.f16_to_f32"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.float.f32_to_unorm8",
            FLOAT,
            T,
            "f32_to_unorm8::sse4",
            "f32_to_unorm8::v",
            "f32_to_unorm8::avx2"
        ),
        gpu_compute("daedalus.float.f32_to_unorm8"),
    ),
];

static OTHER_KERNELS: [[Kernel; 9]; 4] = [
    with_gpu(
        cpu_kernels!(
            "daedalus.copy.bytes",
            COPY,
            T,
            "copy_bytes::v",
            "copy_bytes::v",
            "copy_bytes::avx2";
            // GPU-resident bytes: `copy_buffer_to_buffer` on the copy engine.
            Kernel::implemented(
                "daedalus.copy.bytes",
                Backend::FixedFunction,
                "crates/gpu/src/shader/kernels.rs",
                "crates/gpu/src/kernels_tests.rs",
            )
            .with_variant("copy_engine")
        ),
        gpu_compute("daedalus.copy.bytes"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.codec.hex_lower",
            CODEC_HEX,
            T,
            "hex_lower::sse4",
            "hex_lower::v",
            "hex_lower::avx2"
        ),
        gpu_compute("daedalus.codec.hex_lower"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.codec.base64_encode",
            CODEC_B64,
            T,
            "encode_groups::sse4",
            "encode_groups::v",
            "encode_groups::avx2"
        ),
        gpu_compute("daedalus.codec.base64_encode"),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.codec.base64_decode",
            CODEC_B64,
            T,
            "decode_groups::sse4",
            "decode_groups::v",
            "decode_groups::avx2"
        ),
        gpu_compute("daedalus.codec.base64_decode"),
    ),
];

/// The `core::simd` kernel of each op (`portable.rs`, nightly).
const PORTABLE_SYMBOLS: [(&str, &str); 11] = [
    ("daedalus.pixel.rgb_to_rgba", "portable::rgb_to_rgba"),
    ("daedalus.pixel.rgba_to_rgb", "portable::rgba_to_rgb"),
    ("daedalus.pixel.gray_to_rgba", "portable::gray_to_rgba"),
    ("daedalus.pixel.rgba_red", "portable::rgba_red"),
    ("daedalus.float.f32_from_le", "portable::copy_bytes"),
    ("daedalus.float.f16_to_f32", "portable::f16_to_f32"),
    ("daedalus.float.f32_to_unorm8", "portable::f32_to_unorm8"),
    ("daedalus.copy.bytes", "portable::copy_bytes"),
    ("daedalus.codec.hex_lower", "portable::hex_lower"),
    (
        "daedalus.codec.base64_encode",
        "portable::base64_encode_groups",
    ),
    (
        "daedalus.codec.base64_decode",
        "portable::base64_decode_groups",
    ),
];

const fn str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// The op's portable-SIMD entry: implemented when it has a `core::simd` kernel.
const fn portable_entry(k: Kernel) -> Kernel {
    let mut i = 0;
    while i < PORTABLE_SYMBOLS.len() {
        if str_eq(PORTABLE_SYMBOLS[i].0, k.op) {
            return Kernel::implemented(
                k.op,
                Backend::PortableSimd,
                "crates/kernels/src/portable.rs",
                T,
            )
            .with_symbol(PORTABLE_SYMBOLS[i].1);
        }
        i += 1;
    }
    k
}

const fn with_gpu(cpu: [Kernel; 8], gpu: Kernel) -> [Kernel; 9] {
    [
        cpu[0],
        cpu[1],
        cpu[2],
        cpu[3],
        cpu[4],
        cpu[5],
        portable_entry(cpu[6]),
        cpu[7],
        gpu,
    ]
}

const fn flatten<const N: usize, const M: usize>(rows: &[[Kernel; 9]; N]) -> [Kernel; M] {
    let mut out = [rows[0][0]; M];
    let mut i = 0;
    while i < N * 9 {
        out[i] = rows[i / 9][i % 9];
        i += 1;
    }
    out
}

static KERNELS: [Kernel; 99] = {
    let pixel: [Kernel; 36] = flatten(&PIXEL_KERNELS);
    let float: [Kernel; 27] = flatten(&FLOAT_KERNELS);
    let other: [Kernel; 36] = flatten(&OTHER_KERNELS);
    let mut out = [pixel[0]; 99];
    let mut i = 0;
    while i < 99 {
        out[i] = if i < 36 {
            pixel[i]
        } else if i < 63 {
            float[i - 36]
        } else {
            other[i - 63]
        };
        i += 1;
    }
    out
};

const GPU_TEST: &str = "crates/gpu/src/fragment_rect_tests.rs";
const ON_DEVICE: Status = Status::Impossible(
    "the data is GPU-resident (a storage buffer to a texture on the device); the CPU path is the upload, daedalus.copy.bytes",
);

/// The storage-to-texture copy: the copy engine (the TFU on V3D) for 256-aligned strides and
/// per-row copies, the fold-and-unfold fragment pass for other strides.
static GPU_KERNELS: [Kernel; 9] = [
    Kernel::implemented(
        "daedalus.gpu.storage_to_texture",
        Backend::Scalar,
        GPU_TEST,
        GPU_TEST,
    ),
    Kernel::implemented(
        "daedalus.gpu.storage_to_texture",
        Backend::Gpu,
        "crates/gpu/src/shader/gpu_texture.rs",
        GPU_TEST,
    )
    .with_variant("fold_unfold"),
    Kernel::implemented(
        "daedalus.gpu.storage_to_texture",
        Backend::FixedFunction,
        "crates/gpu/src/shader/gpu_texture.rs",
        GPU_TEST,
    )
    .with_variant("direct"),
    Kernel::implemented(
        "daedalus.gpu.storage_to_texture",
        Backend::FixedFunction,
        "crates/gpu/src/shader/gpu_texture.rs",
        GPU_TEST,
    )
    .with_variant("rows"),
    Kernel::missing(
        "daedalus.gpu.storage_to_texture",
        Backend::X86Sse2,
        ON_DEVICE,
    ),
    Kernel::missing(
        "daedalus.gpu.storage_to_texture",
        Backend::X86Avx2,
        ON_DEVICE,
    ),
    Kernel::missing(
        "daedalus.gpu.storage_to_texture",
        Backend::Neon64,
        ON_DEVICE,
    ),
    Kernel::missing(
        "daedalus.gpu.storage_to_texture",
        Backend::Neon32,
        ON_DEVICE,
    ),
    Kernel::missing("daedalus.gpu.storage_to_texture", Backend::Rvv, ON_DEVICE),
];

/// The fragment forms (`GpuKernels::encode_fragment`): every GPU kernel but base64 decode.
static FRAGMENT_KERNELS: [Kernel; 10] = {
    const OPS: [&str; 10] = [
        "daedalus.pixel.rgb_to_rgba",
        "daedalus.pixel.rgba_to_rgb",
        "daedalus.pixel.gray_to_rgba",
        "daedalus.pixel.rgba_red",
        "daedalus.float.f32_from_le",
        "daedalus.float.f16_to_f32",
        "daedalus.float.f32_to_unorm8",
        "daedalus.copy.bytes",
        "daedalus.codec.hex_lower",
        "daedalus.codec.base64_encode",
    ];
    let mut out = [gpu_fragment(OPS[0]); 10];
    let mut i = 0;
    while i < 10 {
        out[i] = gpu_fragment(OPS[i]);
        i += 1;
    }
    out
};

const fn gpu_fragment(op: &'static str) -> Kernel {
    Kernel::implemented(
        op,
        Backend::Gpu,
        "crates/gpu/src/shader/kernels_common.wgsl",
        "crates/gpu/src/kernels_tests.rs",
    )
    .with_variant("fragment")
}

static FRAGMENT_TABLE: Registry = Registry {
    name: "daedalus-gpu-fragment",
    kernels: &FRAGMENT_KERNELS,
    ..Registry::EMPTY
};

static GPU_OPS: Registry = Registry {
    name: "daedalus-gpu",
    kernels: &GPU_KERNELS,
    exempt: &[
        Exempt {
            path: "crates/gpu/src/shader/fragment.rs",
            reason: "FragmentPass itself: the pipeline type; kernels are the shaders callers pass",
        },
        Exempt {
            path: "crates/gpu/src/shader/pipeline.rs",
            reason: "shader dispatch: builds pipelines for callers' WGSL",
        },
        Exempt {
            path: "crates/gpu/src/shader/fallback.rs",
            reason: "test shaders for the spec cache (in its test module)",
        },
    ],
    children: &[&PORTABLE_GPU, &FRAGMENT_TABLE],
    ..Registry::EMPTY
};

/// PortableSimd for the GPU op: not a CPU op.
static PORTABLE_GPU: Registry = Registry {
    name: "daedalus-gpu-portable",
    kernels: &[Kernel::missing(
        "daedalus.gpu.storage_to_texture",
        Backend::PortableSimd,
        ON_DEVICE,
    )],
    ..Registry::EMPTY
};

const CRC: &str = "crates/kernels/src/crc.rs";
const CRC_OP: &str = "daedalus.codec.crc32";
const CHAIN: Status = Status::NotDataParallel(
    "a CRC is one dependency chain; it speeds up with carry-less multiply or CRC instructions, the extension columns",
);

static CRC_SITES: [CallSite; 1] = [CallSite::new(
    "crates/mcu/build/src/blob.rs",
    "use daedalus_kernels::crc32",
)];

static CRC_OPS: [Op; 1] = [Op::new(
    CRC_OP,
    "CRC-32 (IEEE) of MCU plan blobs",
    "daedalus_kernels::reference::crc32",
)
.could_use(&[(Backend::Rvv, &[Ext::Zvbc])])
.wired_at(&CRC_SITES)];

static CRC_KERNELS: [Kernel; 12] = [
    Kernel::implemented(CRC_OP, Backend::Scalar, CRC, T),
    Kernel::implemented(CRC_OP, Backend::X86Sse2, CRC, T)
        .with_extensions(&[Ext::Pclmulqdq])
        .with_symbol("crc32::pclmul"),
    Kernel::implemented(CRC_OP, Backend::Neon64, CRC, T)
        .with_extensions(&[Ext::Crc32])
        .with_symbol("crc32::arm_crc"),
    Kernel::implemented(CRC_OP, Backend::Rvv, CRC, T)
        .with_extensions(&[Ext::Zvbc])
        .with_symbol("crc32::rvv_zvbc"),
    Kernel::missing(CRC_OP, Backend::X86Sse2, CHAIN),
    Kernel::missing(CRC_OP, Backend::X86Avx2, CHAIN),
    Kernel::missing(CRC_OP, Backend::Neon64, CHAIN),
    Kernel::implemented(CRC_OP, Backend::Neon32, CRC, T).with_symbol("crc32::neon_vmull_p8"),
    Kernel::missing(CRC_OP, Backend::Rvv, CHAIN),
    Kernel::implemented(
        CRC_OP,
        Backend::Gpu,
        "crates/gpu/src/shader/kernels.wgsl",
        "crates/gpu/src/kernels_tests.rs",
    )
    .with_variant("chunks+combine"),
    Kernel::missing(CRC_OP, Backend::PortableSimd, CHAIN),
    Kernel::missing(CRC_OP, Backend::FixedFunction, NO_FIXED),
];

static F16C_KERNELS: [Kernel; 1] = [Kernel::implemented(
    "daedalus.float.f16_to_f32",
    Backend::X86Avx2,
    "crates/kernels/src/float.rs",
    T,
)
.with_extensions(&[Ext::F16c])
.with_symbol("f16_to_f32_f16c")];

static F16C_TABLE: Registry = Registry {
    name: "daedalus-kernels-f16c",
    kernels: &F16C_KERNELS,
    ..Registry::EMPTY
};

static CRC_TABLE: Registry = Registry {
    name: "daedalus-kernels-crc",
    ops: &CRC_OPS,
    kernels: &CRC_KERNELS,
    ..Registry::EMPTY
};

/// Daedalus's kernels, and the source files with SIMD markers that hold none.
pub static ACCEL: Registry = Registry {
    repo: "daedalus",
    name: "daedalus-kernels",
    ops: &OPS,
    kernels: &KERNELS,
    exempt: &[
        Exempt {
            path: "crates/kernels/src/lib.rs",
            reason: "the multiversion! macro and AVX2 detection; kernels are in pixel/float/copy/hex/base64",
        },
        Exempt {
            path: "crates/accel/src/conform.rs",
            reason: "backend detection and the vlenb read for evidence",
        },
        Exempt {
            path: "crates/accel/src/matrix.rs",
            reason: "the generator's marker list",
        },
        Exempt {
            path: "crates/kernels/src/registry.rs",
            reason: "the inventory table",
        },
        Exempt {
            path: "crates/kernels/src/riscv.rs",
            reason: "RISC-V extension detection (riscv_hwprobe syscall)",
        },
    ],
    children: &[&GPU_OPS, &CRC_TABLE, &F16C_TABLE, &RGA_TABLE, &IMAGE_TABLE],
};
