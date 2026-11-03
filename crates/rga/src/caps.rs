//! Which fixed-function 2D unit each target SoC has, by which route Daedalus reaches it, and
//! what it does with each resize filter. Plain data with no OS calls, readable on any target.
//!
//! Sources: the mainline `rockchip-rga` driver in Linux 7.2 (`rga-hw.c`, `rga3-hw.c`, the
//! RK356x and RK3588 device trees) and librga's `im2d_type.h` (airockchip/librga 2b32edc).
//! The K230D's VGLite: `vg_lite.h`, `vg_lite.c` and `vg_lite_options.h` of the K230 Linux SDK
//! v0.6.1 (`daedalus-vglite`).
//! Nothing here has been measured on a board: every exactness is
//! [`Exactness::Unmeasured`] until a board run records it.

use crate::resize::Filter;

/// A target SoC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Soc {
    Rk3566,
    Rk3568,
    Rk3576,
    Rk3588,
    Rk3562,
    /// Kendryte K230D: VGLite 2.5D GPU (`daedalus-vglite`) and an Image 2D engine with no
    /// Linux driver.
    K230d,
}

impl Soc {
    pub const ALL: [Soc; 6] = [
        Soc::Rk3566,
        Soc::Rk3568,
        Soc::Rk3576,
        Soc::Rk3588,
        Soc::Rk3562,
        Soc::K230d,
    ];
}

/// A fixed-function 2D unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Unit {
    /// Rockchip RGA2 (the mainline driver's `rk3288-rga` compatible).
    Rga2,
    /// Rockchip RGA3 (RK3588 has two cores).
    Rga3,
    /// K230D Image 2D engine.
    K230Image2d,
    /// K230D VGLite 2.5D GPU.
    K230Vglite,
}

/// How Daedalus reaches a unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Route {
    /// The mainline V4L2 mem2mem driver ([`crate::Rga`]).
    V4l2,
    /// The vendor's `librga` on a vendor kernel ([`crate::Librga`]).
    Librga,
    /// The vendor's `libvg_lite` (`daedalus-vglite`).
    Vglite,
    /// Not written: the interfaces need a design note first.
    Design,
}

/// How close a unit's result is to the scalar reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Exactness {
    Exact,
    /// Measured: no byte differs by more than this.
    Tolerance(u8),
    /// Not measured on a board yet.
    Unmeasured,
}

/// What a route does with one filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilterCaps {
    pub filter: Filter,
    /// Downscale, upscale.
    pub down: bool,
    pub up: bool,
    pub exactness: Exactness,
    /// How the unit computes it, and what is known to differ from the reference.
    pub note: &'static str,
}

/// A fixed-function operation other than a resize.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FixedOp {
    /// 8-bit indices to RGBA8 through 256 entries (`daedalus.image.palette`).
    Palette,
    /// Per-channel range test giving a mask (`daedalus.image.color_key_mask`).
    ColorKey,
    /// A rectangle, unscaled; the full frame is a copy (`daedalus.image.crop`).
    Crop,
    /// Mirror (`daedalus.image.flip`).
    Flip,
    /// Quarter turns (`daedalus.image.rotate`).
    Rotate,
    /// A solid rectangle (`daedalus.image.fill`); boxes are four.
    Fill,
    /// AND, OR, XOR of masks (`daedalus.image.mask_rop`).
    MaskRop,
    /// Porter-Duff SRC, SRC_OVER, DST_OVER (`daedalus.image.blend`).
    Blend,
    /// A one-pixel line between two points (`daedalus.image.line`).
    Line,
}

/// When a route copies frames through a staging buffer, beyond the unit's own work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Staging {
    /// Never: the unit reads and writes the caller's memory (librga wraps virtual addresses and
    /// dmabufs alike).
    Never,
    /// Byte buffers go through the driver's buffers, one copy in and one out; dmabufs are
    /// queued as they are.
    BytesOnly,
    /// Byte buffers, and dmabufs that are not physically contiguous or whose rows are not
    /// aligned as the unit needs, go through a contiguous staging buffer; contiguous dmabufs
    /// are mapped in place. Each call reports the bytes it copied.
    UnlessContiguous,
}

/// What a route does with one [`FixedOp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpCaps {
    pub op: FixedOp,
    pub exactness: Exactness,
    pub note: &'static str,
}

/// One unit on one SoC through one route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnitCaps {
    pub soc: Soc,
    pub unit: Unit,
    pub route: Route,
    /// The smallest frame the driver takes, when known.
    pub min_size: Option<(u32, u32)>,
    /// The largest scale factor either way, when known.
    pub max_scale: Option<u32>,
    /// Bytes the unit moves for an 8-bit luma resize, per byte of luma, in thousandths: 1000
    /// when it takes gray directly, 1500 when luma goes through NV12 with dummy chroma.
    pub luma_cost_permille: u16,
    pub staging: Staging,
    pub filters: &'static [FilterCaps],
    pub ops: &'static [OpCaps],
}

const AREA_RGA2_V4L2: FilterCaps = FilterCaps {
    filter: Filter::Area,
    down: true,
    up: false,
    exactness: Exactness::Unmeasured,
    note: "the driver's down mode, factor (dst << 16) / src + 1; phase and rounding unmeasured",
};
const AREA_RGA3_V4L2: FilterCaps = FilterCaps {
    filter: Filter::Area,
    down: true,
    up: false,
    exactness: Exactness::Unmeasured,
    note: "the driver's down mode, factor from TRM 5.4.7; phase and rounding unmeasured",
};
const BILINEAR_RGA2_V4L2: FilterCaps = FilterCaps {
    filter: Filter::Bilinear,
    down: false,
    up: true,
    exactness: Exactness::Unmeasured,
    note: "upscale only; factor (src - 1) / (dst - 1) aligns corners, not OpenCV's half-pixel \
           centres, so edges and phase are expected to differ",
};
const BILINEAR_RGA3_V4L2: FilterCaps = FilterCaps {
    filter: Filter::Bilinear,
    down: false,
    up: true,
    exactness: Exactness::Unmeasured,
    note: "upscale only; factor src / dst, the driver notes up to half a pixel of extra scale",
};
const AREA_LIBRGA: FilterCaps = FilterCaps {
    filter: Filter::Area,
    down: true,
    up: false,
    exactness: Exactness::Unmeasured,
    note: "imresize with IM_INTERP_AVERAGE; which units honour it is unmeasured",
};
const BILINEAR_LIBRGA: FilterCaps = FilterCaps {
    filter: Filter::Bilinear,
    down: true,
    up: true,
    exactness: Exactness::Unmeasured,
    note: "imresize with IM_INTERP_LINEAR",
};

const CROP_V4L2: OpCaps = OpCaps {
    op: FixedOp::Crop,
    exactness: Exactness::Unmeasured,
    note: "the output queue's crop selection, unscaled",
};
const CROP_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::Crop,
    exactness: Exactness::Unmeasured,
    note: "imcrop",
};
const FLIP_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::Flip,
    exactness: Exactness::Unmeasured,
    note: "imflip with IM_HAL_TRANSFORM_FLIP_H or FLIP_V",
};
const ROTATE_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::Rotate,
    exactness: Exactness::Unmeasured,
    note: "imrotate with IM_HAL_TRANSFORM_ROT_90/180/270; clockwise is assumed, unmeasured",
};

const FLIP_V4L2: OpCaps = OpCaps {
    op: FixedOp::Flip,
    exactness: Exactness::Unmeasured,
    note: "V4L2_CID_HFLIP or VFLIP on the context",
};
const ROTATE_V4L2: OpCaps = OpCaps {
    op: FixedOp::Rotate,
    exactness: Exactness::Unmeasured,
    note: "V4L2_CID_ROTATE 90, 180 or 270 on the context; clockwise is assumed, unmeasured",
};

const FILL_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::Fill,
    exactness: Exactness::Unmeasured,
    note: "imfill; a mask is RGBA8888 at a quarter width, so its rectangles align to 4 bytes",
};
const ROP_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::MaskRop,
    exactness: Exactness::Unmeasured,
    note: "imrop on masks as RGBA8888 at a quarter width; IM_ROP_XOR is 0xf6, which reads as \
           pattern OR (src XOR dst) in ROP3 terms, so plain XOR is unmeasured",
};
const BLEND_LIBRGA: OpCaps = OpCaps {
    op: FixedOp::Blend,
    exactness: Exactness::Unmeasured,
    note: "imblend; the hardware's rounding is undocumented",
};

/// Mainline (Linux 7.2): no fill (it stores `V4L2_CID_BG_COLOR` and never programs it), no
/// ROP, no blend.
const V4L2_OPS: &[OpCaps] = &[CROP_V4L2, FLIP_V4L2, ROTATE_V4L2];
const RGA3_LIBRGA_OPS: &[OpCaps] = &[CROP_LIBRGA, FLIP_LIBRGA, ROTATE_LIBRGA, BLEND_LIBRGA];

const RGA2_LIBRGA_OPS: &[OpCaps] = &[
    CROP_LIBRGA,
    FLIP_LIBRGA,
    ROTATE_LIBRGA,
    FILL_LIBRGA,
    ROP_LIBRGA,
    BLEND_LIBRGA,
    OpCaps {
        op: FixedOp::Palette,
        exactness: Exactness::Unmeasured,
        note: "impalette with a BPP8 source and a 256x1 RGBA8888 table",
    },
    OpCaps {
        op: FixedOp::ColorKey,
        exactness: Exactness::Unmeasured,
        note: "imcolorkey INVERTED from RGBX8888 onto a cleared RGBA8888 destination, whose \
               alpha is the mask; INVERTED keeping the in-range pixels is from its name only",
    },
];

const RGA2_V4L2: &[FilterCaps] = &[AREA_RGA2_V4L2, BILINEAR_RGA2_V4L2];
const RGA3_V4L2: &[FilterCaps] = &[AREA_RGA3_V4L2, BILINEAR_RGA3_V4L2];
const LIBRGA: &[FilterCaps] = &[AREA_LIBRGA, BILINEAR_LIBRGA];

const fn v4l2(soc: Soc, unit: Unit) -> UnitCaps {
    let rga3 = matches!(unit, Unit::Rga3);
    UnitCaps {
        soc,
        unit,
        route: Route::V4l2,
        min_size: Some(if rga3 { (128, 128) } else { (34, 34) }),
        max_scale: Some(if rga3 { 8 } else { 16 }),
        luma_cost_permille: 1500,
        staging: Staging::BytesOnly,
        filters: if rga3 { RGA3_V4L2 } else { RGA2_V4L2 },
        // The mainline driver has no palette or colour key.
        ops: V4L2_OPS,
    }
}

const fn librga(soc: Soc, unit: Unit) -> UnitCaps {
    UnitCaps {
        soc,
        unit,
        route: Route::Librga,
        min_size: None,
        max_scale: None,
        luma_cost_permille: 1000,
        staging: Staging::Never,
        filters: LIBRGA,
        // Palette and colour key are RGA2 features; librga sends them there.
        ops: if matches!(unit, Unit::Rga2) {
            RGA2_LIBRGA_OPS
        } else {
            RGA3_LIBRGA_OPS
        },
    }
}

const fn design(soc: Soc, unit: Unit) -> UnitCaps {
    UnitCaps {
        soc,
        unit,
        route: Route::Design,
        min_size: None,
        max_scale: None,
        luma_cost_permille: 1000,
        staging: Staging::Never,
        filters: &[],
        ops: &[],
    }
}

const VGLITE_FILTERS: &[FilterCaps] = &[
    FilterCaps {
        filter: Filter::Area,
        down: true,
        up: false,
        exactness: Exactness::Unmeasured,
        note: "2x only: a bilinear blit at half scale, whose half-pixel phase puts each output \
               centre on the corner of four source pixels (weights of a quarter each); VGLite \
               has no averaging filter, and its rounding is unmeasured",
    },
    FilterCaps {
        filter: Filter::Bilinear,
        down: true,
        up: true,
        exactness: Exactness::Unmeasured,
        note: "vg_lite_blit with a scale matrix and VG_LITE_FILTER_BI_LINEAR, sampled at the \
               half-pixel phase as OpenCV; weights, rounding and edges unmeasured",
    },
];

const VGLITE_OPS: &[OpCaps] = &[
    OpCaps {
        op: FixedOp::Crop,
        exactness: Exactness::Unmeasured,
        note: "a translated, point-sampled blit",
    },
    OpCaps {
        op: FixedOp::Flip,
        exactness: Exactness::Unmeasured,
        note: "a mirror matrix, point sampled (gcFEATURE_VG_MIRROR is 0)",
    },
    OpCaps {
        op: FixedOp::Rotate,
        exactness: Exactness::Unmeasured,
        note: "a quarter-turn matrix, point sampled",
    },
    OpCaps {
        op: FixedOp::Fill,
        exactness: Exactness::Unmeasured,
        note: "vg_lite_clear; masks as A8 (an L8 target converts the colour with a truncating \
               float sum, so it is refused)",
    },
    OpCaps {
        op: FixedOp::Blend,
        exactness: Exactness::Unmeasured,
        note: "vg_lite_blit with BLEND_NONE, SRC_OVER or DST_OVER, straight alpha: the K230 \
               build premultiplies every RGBA source itself, so premultiplied input is refused",
    },
    OpCaps {
        op: FixedOp::Palette,
        exactness: Exactness::Unmeasured,
        note: "an INDEX_8 source through vg_lite_set_CLUT's 256 entries",
    },
    OpCaps {
        op: FixedOp::Line,
        exactness: Exactness::Unmeasured,
        note: "vg_lite_draw of a 1-wide stroke with square caps through the pixel centres, no \
               anti-aliasing; Bresenham's pixels on axes and diagonals in theory, other slopes \
               differ where a centre is near the edge",
    },
];

const fn vglite(soc: Soc) -> UnitCaps {
    UnitCaps {
        soc,
        unit: Unit::K230Vglite,
        route: Route::Vglite,
        min_size: None,
        max_scale: None,
        luma_cost_permille: 1000,
        staging: Staging::UnlessContiguous,
        filters: VGLITE_FILTERS,
        ops: VGLITE_OPS,
    }
}

static TABLE: [UnitCaps; 12] = [
    v4l2(Soc::Rk3566, Unit::Rga2),
    librga(Soc::Rk3566, Unit::Rga2),
    v4l2(Soc::Rk3568, Unit::Rga2),
    librga(Soc::Rk3568, Unit::Rga2),
    // No RGA node in the mainline RK3576 and RK3562 device trees (Linux 7.2).
    librga(Soc::Rk3576, Unit::Rga2),
    librga(Soc::Rk3562, Unit::Rga2),
    v4l2(Soc::Rk3588, Unit::Rga3),
    v4l2(Soc::Rk3588, Unit::Rga2),
    librga(Soc::Rk3588, Unit::Rga3),
    librga(Soc::Rk3588, Unit::Rga2),
    vglite(Soc::K230d),
    // No Linux driver in the K230 SDK; the RT-Smart side may own it.
    design(Soc::K230d, Unit::K230Image2d),
];

/// Every unit and route on `soc`, preferred first (mainline before librga, RGA3 before RGA2,
/// VGLite before the K230D's undriven Image 2D engine).
pub fn caps(soc: Soc) -> impl Iterator<Item = &'static UnitCaps> {
    TABLE.iter().filter(move |c| c.soc == soc)
}

impl UnitCaps {
    /// What this route does with `op`, if anything.
    pub fn op(&self, op: FixedOp) -> Option<&OpCaps> {
        self.ops.iter().find(|o| o.op == op)
    }

    /// What this route does with `filter`, if anything.
    pub fn filter(&self, filter: Filter) -> Option<&FilterCaps> {
        self.filters.iter().find(|f| f.filter == filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_soc_is_described_and_nothing_is_measured_yet() {
        for soc in Soc::ALL {
            assert!(caps(soc).next().is_some(), "{soc:?}");
        }
        for c in caps(Soc::Rk3588) {
            for f in c.filters {
                assert_eq!(f.exactness, Exactness::Unmeasured);
                assert!(!f.note.is_empty());
            }
        }
        let first = caps(Soc::Rk3588).next().unwrap();
        assert_eq!((first.unit, first.route), (Unit::Rga3, Route::V4l2));
        let area = first.filter(Filter::Area).unwrap();
        assert!(area.down && !area.up);
        let k230: Vec<_> = caps(Soc::K230d).collect();
        assert_eq!(
            (k230[0].unit, k230[0].route),
            (Unit::K230Vglite, Route::Vglite)
        );
        assert_eq!(k230[0].staging, Staging::UnlessContiguous);
        assert!(k230[0].op(FixedOp::Line).is_some() && k230[0].op(FixedOp::ColorKey).is_none());
        assert!(k230[1].route == Route::Design && k230[1].filters.is_empty());
        assert!(caps(Soc::Rk3576).all(|c| c.route == Route::Librga));
        // Palette and colour key: librga on RGA2 only (palette on VGLite too).
        for c in Soc::ALL
            .into_iter()
            .flat_map(caps)
            .filter(|c| c.route != Route::Vglite)
        {
            let has = c.op(FixedOp::Palette).is_some();
            assert_eq!(
                has,
                c.route == Route::Librga && c.unit == Unit::Rga2,
                "{c:?}"
            );
            let crop = c.op(FixedOp::Crop).is_some();
            assert_eq!(crop, c.route != Route::Design, "{c:?}");
            let flip = c.op(FixedOp::Flip).is_some();
            assert_eq!(flip, c.route != Route::Design, "{c:?}");
            let fill = c.op(FixedOp::Fill).is_some();
            assert_eq!(fill, has, "{c:?}");
        }
    }
}
