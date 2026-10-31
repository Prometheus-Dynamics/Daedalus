//! Daedalus's entries in the acceleration inventory (`docs/backend-matrix.md`).

use daedalus_accel::{Backend, Exempt, Kernel, Op, Registry, Status};

const PORTABLE: Status = Status::Impossible(
    "portable SIMD (core::simd) is nightly-only on Rust 1.99; each ISA has its own kernel",
);
const NO_FIXED: Status =
    Status::Impossible("no fixed-function unit on the supported boards works on host memory");
const HOST_SIDE: Status = Status::Impossible(
    "the host side of a GPU transfer: the bytes are already in host memory, and a GPU pass would add a round trip",
);
const WIRE: Status = Status::Impossible(
    "host-side serialization of payload bytes for JSON / the FFI wire; a GPU round trip costs more than the encoding",
);

/// The CPU rows every vectorized kernel has: scalar reference, the x86 SSE build (`sse`), the
/// baseline build for NEON / NEON32 / RVV (`v`) and AVX2, with the asm symbols the matrix
/// checks.
macro_rules! cpu_kernels {
    ($op:expr, $file:expr, $test:expr, $sse:expr, $v:expr, $avx2:expr) => {
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
            Kernel::missing($op, Backend::FixedFunction, NO_FIXED),
        ]
    };
    ($op:expr, $file:expr, $test:expr, $v:expr, $avx2:expr) => {
        cpu_kernels!($op, $file, $test, $v, $v, $avx2)
    };
}

const PIXEL: &str = "crates/kernels/src/pixel.rs";
const FLOAT: &str = "crates/kernels/src/float.rs";
const COPY: &str = "crates/kernels/src/copy.rs";
const CODEC_HEX: &str = "crates/kernels/src/hex.rs";
const CODEC_B64: &str = "crates/kernels/src/base64.rs";

const fn op(id: &'static str, summary: &'static str, scalar: &'static str) -> Op {
    Op::new(id, summary, scalar)
}

static OPS: [Op; 12] = [
    op(
        "daedalus.pixel.rgb_to_rgba",
        "RGB8 to RGBA8 (GPU upload of RgbImage)",
        "daedalus_kernels::reference::rgb_to_rgba",
    ),
    op(
        "daedalus.pixel.rgba_to_rgb",
        "RGBA8 to RGB8 (GPU download to RgbImage)",
        "daedalus_kernels::reference::rgba_to_rgb",
    ),
    op(
        "daedalus.pixel.gray_to_rgba",
        "Gray8 to RGBA8 (GrayImage upload without R8)",
        "daedalus_kernels::reference::gray_to_rgba",
    ),
    op(
        "daedalus.pixel.rgba_red",
        "RGBA8 red channel (GrayImage download from RGBA)",
        "daedalus_kernels::reference::rgba_red",
    ),
    op(
        "daedalus.float.f32_from_le",
        "LE bytes to f32 (r32float readback)",
        "daedalus_kernels::reference::f32_from_le",
    ),
    op(
        "daedalus.float.f16_to_f32",
        "binary16 to f32 (rgba16float readback)",
        "daedalus_kernels::reference::f16_to_f32",
    ),
    op(
        "daedalus.float.f32_to_unorm8",
        "f32 to unorm8 (r32float to gray8 readback)",
        "daedalus_kernels::reference::f32_to_unorm8",
    ),
    op(
        "daedalus.copy.bytes",
        "byte copy; row re-pitch on upload and readback runs it per row",
        "<[u8]>::copy_from_slice",
    ),
    op(
        "daedalus.codec.hex_lower",
        "lowercase hex (package digests)",
        "daedalus_kernels::reference::hex_lower",
    ),
    op(
        "daedalus.codec.base64_encode",
        "standard base64 encode (JSON and FFI wire bytes)",
        "daedalus_kernels::reference::base64_encode",
    ),
    op(
        "daedalus.gpu.storage_to_texture",
        "a frame in a storage buffer (any stride) into a sampled texture, on the device",
        "crates/gpu/src/fragment_rect_tests.rs (rows de-strided on the CPU)",
    ),
    op(
        "daedalus.codec.base64_decode",
        "standard base64 decode (JSON and FFI wire bytes)",
        "daedalus_kernels::reference::base64_decode",
    ),
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
        Kernel::missing(
            "daedalus.pixel.rgb_to_rgba",
            Backend::Gpu,
            Status::Gap(
                "planned: upload packed RGB to a storage buffer and expand it in a fragment pass (25% fewer bytes)",
            ),
        ),
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
        Kernel::missing(
            "daedalus.pixel.rgba_to_rgb",
            Backend::Gpu,
            Status::Gap("planned: pack to RGB in a GPU pass before the readback (25% fewer bytes)"),
        ),
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
        Kernel::missing(
            "daedalus.pixel.gray_to_rgba",
            Backend::Gpu,
            Status::Gap(
                "planned: upload R8 and expand on the GPU when the format lacks R8 sampling",
            ),
        ),
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
        Kernel::missing(
            "daedalus.pixel.rgba_red",
            Backend::Gpu,
            Status::Gap(
                "planned: render the red channel to an R8 target before the readback (4x fewer bytes)",
            ),
        ),
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
        Kernel::missing("daedalus.float.f32_from_le", Backend::Gpu, HOST_SIDE),
    ),
    with_gpu(
        cpu_kernels!(
            "daedalus.float.f16_to_f32",
            FLOAT,
            T,
            "f16_to_f32::sse4",
            "f16_to_f32::v",
            "f16_to_f32::avx2"
        ),
        Kernel::missing(
            "daedalus.float.f16_to_f32",
            Backend::Gpu,
            Status::Impossible(
                "converting before the readback doubles the bytes read back; the conversion is the host side of the transfer",
            ),
        ),
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
        Kernel::missing(
            "daedalus.float.f32_to_unorm8",
            Backend::Gpu,
            Status::Gap("planned: convert in a GPU pass before the readback (4x fewer bytes)"),
        ),
    ),
];

static OTHER_KERNELS: [[Kernel; 9]; 4] = [
    with_gpu(
        cpu_kernels!(
            "daedalus.copy.bytes",
            COPY,
            T,
            "copy_bytes::v",
            "copy_bytes::avx2"
        ),
        Kernel::missing("daedalus.copy.bytes", Backend::Gpu, HOST_SIDE),
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
        Kernel::missing("daedalus.codec.hex_lower", Backend::Gpu, WIRE),
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
        Kernel::missing("daedalus.codec.base64_encode", Backend::Gpu, WIRE),
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
        Kernel::missing("daedalus.codec.base64_decode", Backend::Gpu, WIRE),
    ),
];

const fn with_gpu(cpu: [Kernel; 8], gpu: Kernel) -> [Kernel; 9] {
    [
        cpu[0], cpu[1], cpu[2], cpu[3], cpu[4], cpu[5], cpu[6], cpu[7], gpu,
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
    children: &[&PORTABLE_GPU],
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
    ],
    children: &[&GPU_OPS],
};
