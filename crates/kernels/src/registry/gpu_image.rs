//! The GPU rows of the image ops: the compute form of each op but line, on frames that are
//! already on the device (`GpuImages`, `crates/gpu/src/shader/image.wgsl`). Each is bit-exact
//! with its scalar reference (`crates/gpu/src/shader/image_tests.rs`). Line is one serial walk
//! with at most `width + height` steps, so it has no data-parallel GPU form.

use daedalus_accel::{Backend, Kernel, Registry, Status};

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
    Kernel::missing(
        "daedalus.image.line",
        Backend::Gpu,
        Status::NotDataParallel(
            "one line is a serial walk of at most width + height Bresenham steps; no data-parallel form",
        ),
    ),
];

pub(super) static GPU_IMAGE_TABLE: Registry = Registry {
    name: "daedalus-gpu-image",
    kernels: &GPU_IMAGE_KERNELS,
    ..Registry::EMPTY
};
