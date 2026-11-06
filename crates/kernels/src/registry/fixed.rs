//! The fixed-function entries: Rockchip's RGA on the pixel ops, the K230's VGLite, and the image
//! nodes other repos lower onto.
//!
//! The RGA and VGLite routes are Lemnos's (`lemnos-drivers-rga`, `lemnos-drivers-vglite`) at the
//! revision pinned in the workspace `Cargo.toml`. Their sources and conformance tests live in that
//! repo, so rows name them as `<repo>@<rev>:<path>` (see `daedalus_accel::Kernel::source`) and
//! their cells stay untested here. The test at the end ties every row to Lemnos's capability table.

use daedalus_accel::{Backend, Kernel, Op, Registry, Status};

use super::{T, op};

// The source and test paths are literal `lemnos@<rev>:<path>` strings: the matrix reads them as
// data. The rev is the one pinned in the workspace `Cargo.toml`; bump both together.

/// The mainline `rockchip-rga` V4L2 mem2mem route (`v4l2`).
const V4L2: (&str, &str) = (
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-rga/src/v4l2.rs",
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-rga/tests/v4l2.rs",
);
/// The vendor kernel's multi-RGA node, `/dev/rga` (`multirga`): fill, mask ROP, blend, palette,
/// colour key, flips and turns, gray input.
const MULTIRGA: (&str, &str) = (
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-rga/src/multirga/device.rs",
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-rga/tests/multirga.rs",
);
/// The K230's VGLite GPU through `/dev/vg_lite` (`vglite`).
const VGLITE: (&str, &str) = (
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-vglite/src/gpu.rs",
    "lemnos@952e1c7dd37c698daa477f5caf82fad20d182ca8:crates/lemnos-drivers-vglite/tests/vglite.rs",
);

/// Fixed-function image nodes other repos lower onto (Eidos's pyramids): only the scalar
/// reference and the fixed-function rows are Daedalus's.
const NODE_ONLY: &[Backend] = &[Backend::Scalar, Backend::FixedFunction];
const NODE_REASON: &str = "a fixed-function node: the CPU kernels and the callers are Eidos's";

static IMAGE_OPS: [Op; 11] = [
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
    op(
        "daedalus.image.fill",
        "Set a rectangle to one colour (mask fill, border clear, boxes)",
        "daedalus_kernels::reference::fill",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.mask_rop",
        "AND, OR or XOR of two 8-bit masks",
        "daedalus_kernels::reference::mask_rop",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.blend",
        "Porter-Duff SRC, SRC_OVER or DST_OVER of RGBA8, straight or premultiplied",
        "daedalus_kernels::reference::blend",
    )
    .only_on(NODE_ONLY, NODE_REASON),
    op(
        "daedalus.image.line",
        "A one-pixel line between two points, both ends included (Bresenham, Eidos's plain line)",
        "daedalus_kernels::reference::line",
    )
    .only_on(NODE_ONLY, NODE_REASON),
];

/// The image rows. Lemnos's `caps` decides which route serves each, and the test below checks
/// it. Routes: `v4l2` (the mainline driver: crop, flip, turn, the area and bilinear resizes),
/// `multirga` (fill, mask ROP, blend, palette, colour key, flip, turn; no resize or crop) and
/// `vglite` (the K230 GPU). The mainline driver has no fill, ROP, blend, palette or colour key.
static IMAGE_KERNELS: [Kernel; 32] = [
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
        "daedalus.image.fill",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.mask_rop",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.blend",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    Kernel::implemented(
        "daedalus.image.line",
        Backend::Scalar,
        "crates/kernels/src/reference.rs",
        T,
    ),
    // The mainline `rockchip-rga` driver.
    Kernel::implemented(
        "daedalus.image.crop",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.image.resize_area",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.image.resize_bilinear",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    // The vendor kernel's multi-RGA node (`/dev/rga`). Lemnos's caps serve no resize or crop here.
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.fill",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.mask_rop",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.blend",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.palette",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    Kernel::implemented(
        "daedalus.image.color_key_mask",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
    // The K230's VGLite. No colour key and no ROP exist on this GPU. Its resizes are the
    // bilinear scale and the 2x area downscale only.
    Kernel::implemented(
        "daedalus.image.resize_area",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.resize_bilinear",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.crop",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.flip",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.rotate",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.fill",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.blend",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    Kernel::implemented(
        "daedalus.image.palette",
        Backend::FixedFunction,
        VGLITE.0,
        VGLITE.1,
    )
    .with_variant("vglite"),
    // Lemnos's VGLite route does not serve lines: its strokes are not shown to match Bresenham on
    // a K230, which is what this op is. Lines run on the CPU until the K230 slope-sweep test
    // (`lemnos-drivers-vglite/tests/lines.rs`) passes and Lemnos adds the row back.
    Kernel::missing(
        "daedalus.image.line",
        Backend::FixedFunction,
        Status::Deferred(
            "Lemnos's VGLite route does not serve lines until a K230 slope-sweep test matches \
             Bresenham; this op is exact Bresenham, so lines run on the CPU",
        ),
    )
    .with_variant("vglite"),
];

pub(super) static IMAGE_TABLE: Registry = Registry {
    name: "daedalus-kernels-image",
    ops: &IMAGE_OPS,
    kernels: &IMAGE_KERNELS,
    ..Registry::EMPTY
};

/// The pixel conversions on RGA. Lemnos's `caps` serves a conversion through the mainline driver
/// when neither side is gray, and through multi-RGA only from gray to RGBA-class. The Y-only
/// output of `rgba_red` has no route, so its row is a gap in `registry.rs`.
static RGA_KERNELS: [Kernel; 3] = [
    Kernel::implemented(
        "daedalus.pixel.rgb_to_rgba",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.pixel.rgba_to_rgb",
        Backend::FixedFunction,
        V4L2.0,
        V4L2.1,
    )
    .with_variant("v4l2"),
    Kernel::implemented(
        "daedalus.pixel.gray_to_rgba",
        Backend::FixedFunction,
        MULTIRGA.0,
        MULTIRGA.1,
    )
    .with_variant("multirga"),
];

pub(super) static RGA_TABLE: Registry = Registry {
    name: "daedalus-kernels-rga",
    kernels: &RGA_KERNELS,
    ..Registry::EMPTY
};

#[cfg(all(test, target_os = "linux"))]
mod lemnos_routes {
    //! Ties the rows above to Lemnos's capability table (`lemnos_drivers_rga::caps::route_for`
    //! at the pinned revision). Mapping from our op ids to Lemnos's `caps::Op`:
    //! - `image.crop`, `flip`, `rotate`, `fill`, `mask_rop`, `blend`, `palette`, `color_key_mask`,
    //!   `line` are `Op::Fixed` of the same-named `FixedOp` (1:1).
    //! - `image.resize_area` and `resize_bilinear` are `Op::Resize(Filter::Area | Bilinear)`.
    //! - `pixel.rgb_to_rgba`, `rgba_to_rgb`, `gray_to_rgba`, `rgba_red` are `Op::Convert`; the
    //!   formats (both sides, gray included) decide the route, so they are not separate ops.
    //!   `rgba_red` writes one byte a pixel, Y-only output, which no route serves.
    //! - Lemnos has no op for `Line` on VGLite: its row is `FixedOp::Line`, unserved by design.
    //!
    //! Our RGBA8 is Lemnos's `Abgr32` (both are the four-byte RGBA class; the layout here only
    //! feeds the route check).
    use super::*;
    use lemnos_drivers_rga::caps::{self, FixedOp, Op as RgaOp, Route, Soc};
    use lemnos_drivers_rga::{Filter, Layout, PixelFormat};

    fn frame(format: PixelFormat, width: u32, height: u32) -> Layout {
        Layout::packed(format, width, height)
    }

    /// The Lemnos request each op makes, with frames its route rules accept. A resize uses a
    /// 2x step the filter serves (area down, bilinear up: the mainline driver's bilinear is
    /// upscale only).
    fn request(op: &str) -> Option<(RgaOp, Layout, Layout)> {
        let rgba = |w, h| frame(PixelFormat::Abgr32, w, h);
        let same = (640, 360);
        let fixed = |f: FixedOp| (RgaOp::Fixed(f), rgba(same.0, same.1), rgba(same.0, same.1));
        Some(match op {
            "daedalus.image.resize_area" => {
                (RgaOp::Resize(Filter::Area), rgba(1280, 720), rgba(640, 360))
            }
            "daedalus.image.resize_bilinear" => (
                RgaOp::Resize(Filter::Bilinear),
                rgba(640, 360),
                rgba(1280, 720),
            ),
            "daedalus.image.crop" => fixed(FixedOp::Crop),
            "daedalus.image.flip" => fixed(FixedOp::Flip),
            "daedalus.image.rotate" => fixed(FixedOp::Rotate),
            "daedalus.image.fill" => fixed(FixedOp::Fill),
            "daedalus.image.mask_rop" => fixed(FixedOp::MaskRop),
            "daedalus.image.blend" => fixed(FixedOp::Blend),
            "daedalus.image.palette" => fixed(FixedOp::Palette),
            "daedalus.image.color_key_mask" => fixed(FixedOp::ColorKey),
            "daedalus.image.line" => fixed(FixedOp::Line),
            "daedalus.pixel.rgb_to_rgba" => (
                RgaOp::Convert,
                frame(PixelFormat::Rgb24, same.0, same.1),
                rgba(same.0, same.1),
            ),
            "daedalus.pixel.rgba_to_rgb" => (
                RgaOp::Convert,
                rgba(same.0, same.1),
                frame(PixelFormat::Rgb24, same.0, same.1),
            ),
            "daedalus.pixel.gray_to_rgba" => (
                RgaOp::Convert,
                frame(PixelFormat::Grey, same.0, same.1),
                rgba(same.0, same.1),
            ),
            "daedalus.pixel.rgba_red" => (
                RgaOp::Convert,
                rgba(same.0, same.1),
                frame(PixelFormat::Grey, same.0, same.1),
            ),
            _ => return None,
        })
    }

    /// The SoC each route is checked on. RK3566 has the mainline node (RGA2) and the multi-RGA
    /// node; RK3576 has no mainline node, so multi-RGA is the route there; the K230D is VGLite.
    fn soc_of(variant: &str) -> Soc {
        match variant {
            "v4l2" => Soc::Rk3566,
            "multirga" => Soc::Rk3576,
            "vglite" => Soc::K230d,
            other => panic!("unknown fixed-function variant {other}"),
        }
    }

    fn route_of(variant: &str) -> Route {
        match variant {
            "v4l2" => Route::V4l2,
            "multirga" => Route::MultiRga,
            "vglite" => Route::Vglite,
            other => panic!("unknown fixed-function variant {other}"),
        }
    }

    #[test]
    fn every_implemented_fixed_row_is_served_by_its_route() {
        let rows = IMAGE_KERNELS.iter().chain(RGA_KERNELS.iter());
        let mut checked = 0;
        for k in rows.filter(|k| k.backend == Backend::FixedFunction) {
            let (op, src, dst) =
                request(k.op).unwrap_or_else(|| panic!("{} has no Lemnos request", k.op));
            let variant = k.variant.expect("a fixed-function row names its route");
            match k.status {
                Status::Implemented => {
                    assert_eq!(
                        caps::route_for(soc_of(variant), op, src, dst),
                        Some(route_of(variant)),
                        "{} on {variant}",
                        k.op
                    );
                    checked += 1;
                }
                // A row that is not implemented must have no route on any SoC.
                _ => {
                    for soc in Soc::ALL {
                        assert_eq!(
                            caps::route_for(soc, op, src, dst),
                            None,
                            "{} is {:?} but {soc:?} serves it",
                            k.op,
                            k.status
                        );
                    }
                }
            }
        }
        assert!(checked >= 20, "only {checked} served rows checked");
    }

    #[test]
    fn line_is_not_served_on_vglite_so_it_runs_on_the_cpu() {
        let (op, src, dst) = request("daedalus.image.line").unwrap();
        assert_eq!(caps::route_for(Soc::K230d, op, src, dst), None);
    }

    #[test]
    fn y_only_output_has_no_route() {
        let (op, src, dst) = request("daedalus.pixel.rgba_red").unwrap();
        for soc in Soc::ALL {
            assert_eq!(caps::route_for(soc, op, src, dst), None, "{soc:?}");
        }
    }
}
