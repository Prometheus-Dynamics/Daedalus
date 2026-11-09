//! The GPU rows of the image ops: the compute form of each op, on frames that are already on the
//! device (`GpuImages`, `crates/gpu/src/shader/image.wgsl`). Each is bit-exact with its scalar
//! reference (`crates/gpu/src/shader/image_tests.rs`). Line is closed form: each pixel decides
//! whether it is on the segment.

use daedalus_accel::{Backend, Exempt, Kernel, Registry};

const SOURCE: &str = "crates/gpu/src/shader/image.wgsl";
const TEST: &str = "crates/gpu/src/shader/image_tests.rs";

const fn gpu(op: &'static str) -> Kernel {
    Kernel::implemented(op, Backend::Gpu, SOURCE, TEST).with_variant("compute")
}

static GPU_IMAGE_KERNELS: [Kernel; 11] = [
    gpu("daedalus.image.resize_area"),
    gpu("daedalus.image.resize_bilinear"),
    gpu("daedalus.image.crop"),
    gpu("daedalus.image.flip"),
    gpu("daedalus.image.rotate"),
    gpu("daedalus.image.fill"),
    gpu("daedalus.image.blend"),
    gpu("daedalus.image.palette"),
    gpu("daedalus.image.color_key_mask"),
    gpu("daedalus.image.mask_rop"),
    gpu("daedalus.image.line"),
];

pub(super) static GPU_IMAGE_TABLE: Registry = Registry {
    name: "daedalus-gpu-image",
    kernels: &GPU_IMAGE_KERNELS,
    exempt: &[Exempt {
        path: "crates/gpu/src/shader/image.rs",
        reason: "GpuImages: the pipeline and dispatch of image.wgsl (the kernel rows name the shader)",
    }],
    ..Registry::EMPTY
};
