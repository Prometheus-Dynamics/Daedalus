//! Which fixed-function 2D unit each target SoC has, by which route Daedalus reaches it, and
//! what it does with each resize filter. Plain data with no OS calls, readable on any target.
//!
//! Sources: the mainline `rockchip-rga` driver in Linux 7.2 (`rga-hw.c`, `rga3-hw.c`, the
//! RK356x and RK3588 device trees) and librga's `im2d_type.h` (airockchip/librga 2b32edc).
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
    /// Kendryte K230D: Image 2D engine and VGLite 2.5D GPU. No code yet.
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

const RGA2_LIBRGA_OPS: &[OpCaps] = &[
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
        filters: if rga3 { RGA3_V4L2 } else { RGA2_V4L2 },
        // The mainline driver has no palette or colour key.
        ops: &[],
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
        filters: LIBRGA,
        // Palette and colour key are RGA2 features; librga sends them there.
        ops: if matches!(unit, Unit::Rga2) {
            RGA2_LIBRGA_OPS
        } else {
            &[]
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
        filters: &[],
        ops: &[],
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
    design(Soc::K230d, Unit::K230Image2d),
    design(Soc::K230d, Unit::K230Vglite),
];

/// Every unit and route on `soc`, preferred first (mainline before librga, RGA3 before RGA2).
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
        assert!(caps(Soc::K230d).all(|c| c.route == Route::Design && c.filters.is_empty()));
        assert!(caps(Soc::Rk3576).all(|c| c.route == Route::Librga));
        // Palette and colour key: librga on RGA2 only.
        for c in Soc::ALL.into_iter().flat_map(caps) {
            let has = c.op(FixedOp::Palette).is_some();
            assert_eq!(
                has,
                c.route == Route::Librga && c.unit == Unit::Rga2,
                "{c:?}"
            );
        }
    }
}
