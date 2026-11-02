//! The fixed-function entries: Rockchip's RGA on the pixel ops, and the image nodes other repos
//! lower onto.

use daedalus_accel::{Backend, Kernel, Op, Registry};

use super::{T, op};

/// Rockchip's RGA (`daedalus-rga`): the `rockchip-rga` V4L2 mem2mem driver and the vendor's
/// `librga`. Written, untested on board.
const RGA_V4L2: (&str, &str) = ("crates/rga/src/lib.rs", "crates/rga/tests/v4l2.rs");
const RGA_LIB: (&str, &str) = ("crates/rga/src/librga.rs", "crates/rga/tests/librga.rs");

/// Fixed-function image nodes other repos lower onto (Eidos's pyramids): only the scalar
/// reference and the fixed-function rows are Daedalus's.
const NODE_ONLY: &[Backend] = &[Backend::Scalar, Backend::FixedFunction];
const NODE_REASON: &str = "a fixed-function node: the CPU kernels and the callers are Eidos's";

static IMAGE_OPS: [Op; 7] = [
    op(
        "daedalus.image.resize_area",
        "Area downscale by an integer factor, rounded block means (pyramid levels)",
        "daedalus_kernels::reference::resize_area",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.resize_bilinear",
        "Bilinear resize, OpenCV INTER_LINEAR on 8-bit data",
        "daedalus_kernels::reference::resize_bilinear",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.palette",
        "8-bit indices to RGBA8 through a 256-entry palette (heat and label maps)",
        "daedalus_kernels::reference::palette",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.color_key_mask",
        "Mask of the pixels whose R, G and B lie in a range (colour threshold)",
        "daedalus_kernels::reference::color_key_mask",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.crop",
        "A rectangle of a frame, unscaled (a full-frame rectangle is a copy)",
        "daedalus_kernels::reference::crop",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.flip",
        "Mirror left to right or top to bottom",
        "daedalus_kernels::reference::flip",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.rotate",
        "Quarter turns clockwise",
        "daedalus_kernels::reference::rotate",
    )
    .only_on(NODE_ONLY, NODE_REASON),
];

static IMAGE_KERNELS: [Kernel; 19] = [
    Kernel::implemented(
        "daedalus.image.resize_area",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.resize_bilinear",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.palette",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.color_key_mask",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.crop",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.crop",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.image.crop",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    // The mainline driver has neither: librga only.
    Kernel::implemented(
        "daedalus.image.palette",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.image.color_key_mask",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.image.resize_area",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.image.resize_bilinear",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.image.resize_area",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.image.resize_bilinear",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
];

pub(super) static IMAGE_TABLE: Registry = Registry {
    name: "daedalus-kernels-image",
    ops: &IMAGE_OPS,
    kernels: &IMAGE_KERNELS,
    ..Registry::EMPTY
};

static RGA_KERNELS: [Kernel; 6] = [
    Kernel::implemented(
        "daedalus.pixel.rgb_to_rgba",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.pixel.rgba_to_rgb",
        Backend::FixedFunction,
        RGA_V4L2.0,
        RGA_V4L2.1,
    )
    .with_variant("rga-v4l2"),
    Kernel::implemented(
        "daedalus.pixel.rgb_to_rgba",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.pixel.rgba_to_rgb",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.pixel.gray_to_rgba",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
    Kernel::implemented(
        "daedalus.pixel.rgba_red",
        Backend::FixedFunction,
        RGA_LIB.0,
        RGA_LIB.1,
    )
    .with_variant("librga"),
];

pub(super) static RGA_TABLE: Registry = Registry {
    name: "daedalus-kernels-rga",
    kernels: &RGA_KERNELS,
    ..Registry::EMPTY
};
