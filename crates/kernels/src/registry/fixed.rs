//! The fixed-function entries: Rockchip's RGA on the pixel conversions.
//!
//! The RGA routes are Lemnos's (`lemnos-drivers-rga`) at the revision pinned in the workspace
//! `Cargo.toml`. Their sources and conformance tests live in that repo, so rows name them as
//! `<repo>@<rev>:<path>` (see `daedalus_accel::Kernel::source`) and their cells stay untested
//! here. The test at the end ties every row to Lemnos's capability table.

use daedalus_accel::{Backend, Kernel, Registry};

// The source and test paths are literal `lemnos@<rev>:<path>` strings: the matrix reads them as
// data. The rev is the one pinned in the workspace `Cargo.toml`; bump both together.

/// The mainline `rockchip-rga` V4L2 mem2mem route (`v4l2`).
const V4L2: (&str, &str) = (
    "lemnos@cf3a4ac1b6e0043fde5bc80175a162a982b1239f:crates/lemnos-drivers-rga/src/v4l2.rs",
    "lemnos@cf3a4ac1b6e0043fde5bc80175a162a982b1239f:crates/lemnos-drivers-rga/tests/v4l2.rs",
);
/// The vendor kernel's multi-RGA node, `/dev/rga` (`multirga`): gray input to RGBA.
const MULTIRGA: (&str, &str) = (
    "lemnos@cf3a4ac1b6e0043fde5bc80175a162a982b1239f:crates/lemnos-drivers-rga/src/multirga/device.rs",
    "lemnos@cf3a4ac1b6e0043fde5bc80175a162a982b1239f:crates/lemnos-drivers-rga/tests/multirga.rs",
);

/// The pixel conversions on RGA. Lemnos's `caps` serves a conversion through the mainline driver
/// when neither side is gray, and through multi-RGA only from gray to RGBA-class. `rgba_red`'s
/// Y-only output is Lemnos's `MultiRga::channel_to_gray`, deferred in `registry.rs` until its
/// board exactness test passes.
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
    //! - `pixel.rgb_to_rgba`, `rgba_to_rgb`, `gray_to_rgba`, `rgba_red` are `Op::Convert`; the
    //!   formats (both sides, gray included) decide the route, so they are not separate ops.
    //!   `rgba_red` writes one byte a pixel: Lemnos's `FixedOp::Channel` (02c8396), which no
    //!   route serves until its board exactness test passes.
    //!
    //! Our RGBA8 is Lemnos's `Abgr32` (both are the four-byte RGBA class; the layout here only
    //! feeds the route check).
    use super::*;
    use daedalus_accel::Status;
    use lemnos_drivers_rga::caps::{self, Op as RgaOp, Route, Soc};
    use lemnos_drivers_rga::{Layout, PixelFormat};

    fn frame(format: PixelFormat, width: u32, height: u32) -> Layout {
        Layout::packed(format, width, height)
    }

    /// The Lemnos request each op makes, with frames its route rules accept.
    fn request(op: &str) -> Option<(RgaOp, Layout, Layout)> {
        let rgba = |w, h| frame(PixelFormat::Abgr32, w, h);
        let same = (640, 360);
        Some(match op {
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

    /// The SoC each route is checked on: RK3566 has the mainline node (RGA2); RK3576 has no
    /// mainline node, so multi-RGA is the route there.
    fn soc_of(variant: &str) -> Soc {
        match variant {
            "v4l2" => Soc::Rk3566,
            "multirga" => Soc::Rk3576,
            other => panic!("unknown fixed-function variant {other}"),
        }
    }

    fn route_of(variant: &str) -> Route {
        match variant {
            "v4l2" => Route::V4l2,
            "multirga" => Route::MultiRga,
            other => panic!("unknown fixed-function variant {other}"),
        }
    }

    #[test]
    fn every_implemented_fixed_row_is_served_by_its_route() {
        let mut checked = 0;
        for k in RGA_KERNELS
            .iter()
            .filter(|k| k.backend == Backend::FixedFunction)
        {
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
        assert_eq!(checked, RGA_KERNELS.len(), "every RGA row is served");
    }

    #[test]
    fn y_only_output_has_no_route() {
        let (op, src, dst) = request("daedalus.pixel.rgba_red").unwrap();
        for soc in Soc::ALL {
            assert_eq!(caps::route_for(soc, op, src, dst), None, "{soc:?}");
        }
    }
}
