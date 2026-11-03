//! What each operation asks of VGLite, worked out before any library call: the blit matrix,
//! the filter, the blend mode, a line's path. Anything the unit cannot do as asked is
//! [`io::ErrorKind::Unsupported`], never a substitute.
//!
//! VGLite maps a source point to the target by the matrix and samples the source at the
//! inverse image of each target pixel's centre (`vg_lite_blit`'s step setup); the bilinear
//! filter shifts that by half a pixel, which is OpenCV's half-pixel phase.

use std::io;

use daedalus_rga::{Filter, Transform};

use crate::ffi::{self, BLEND_DST_OVER, BLEND_NONE, BLEND_SRC_OVER, Matrix};

/// A pixel format the K230's VGLite reads and writes (`vg_lite_options.h`: no 24-bit RGB, no
/// planar YUV input).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Format {
    /// R, G, B, A bytes.
    Rgba8888,
    /// B, G, R, A bytes.
    Bgra8888,
    /// R, G, B, X bytes: the fourth byte is ignored and the pixel is opaque.
    Rgbx8888,
    /// One luma byte.
    L8,
    /// One alpha byte.
    A8,
    /// 8-bit indices through the colour table: a source of [`crate::Vglite::palette_bytes`].
    Index8,
}

impl Format {
    pub fn bytes_per_pixel(self) -> u32 {
        match self {
            Self::Rgba8888 | Self::Bgra8888 | Self::Rgbx8888 => 4,
            Self::L8 | Self::A8 | Self::Index8 => 1,
        }
    }

    pub(crate) fn code(self) -> i32 {
        match self {
            Self::Rgba8888 => ffi::RGBA8888,
            Self::Bgra8888 => ffi::BGRA8888,
            Self::Rgbx8888 => ffi::RGBX8888,
            Self::L8 => ffi::L8,
            Self::A8 => ffi::A8,
            Self::Index8 => ffi::INDEX_8,
        }
    }

    /// The row alignment VGLite asks of a source, in bytes (`_check_source_aligned`).
    pub(crate) fn stride_align(self) -> u32 {
        match self.bytes_per_pixel() {
            4 => 64,
            _ => 16,
        }
    }
}

/// An image's size and layout. The stride is in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Image {
    pub format: Format,
    pub width: u32,
    pub height: u32,
    pub stride: u32,
}

impl Image {
    /// Tightly packed rows.
    pub fn packed(format: Format, width: u32, height: u32) -> Self {
        Self {
            format,
            width,
            height,
            stride: width * format.bytes_per_pixel(),
        }
    }

    /// The bytes the image spans: full rows but the last.
    pub fn frame_len(&self) -> usize {
        if self.height == 0 {
            return 0;
        }
        (self.height as usize - 1) * self.stride as usize + self.row_len()
    }

    pub(crate) fn row_len(&self) -> usize {
        (self.width * self.format.bytes_per_pixel()) as usize
    }

    /// VGLite can address the caller's rows directly: the stride meets the source alignment.
    pub(crate) fn direct(&self) -> bool {
        self.stride.is_multiple_of(self.format.stride_align())
    }

    pub(crate) fn check(&self, len: usize) -> io::Result<()> {
        if self.width == 0 || self.height == 0 {
            return invalid(format!("empty image {self:?}"));
        }
        if (self.stride as usize) < self.row_len() {
            return invalid(format!("stride shorter than a row: {self:?}"));
        }
        if len < self.frame_len() {
            return invalid(format!(
                "{len} bytes for an image of {}: {self:?}",
                self.frame_len()
            ));
        }
        Ok(())
    }
}

/// A Porter-Duff mode of [`crate::Vglite::blend_bytes`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlendMode {
    /// The foreground replaces the destination.
    Src,
    SrcOver,
    DstOver,
}

pub(crate) fn invalid<T>(why: String) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidInput, why))
}

pub(crate) fn unsupported<T>(why: String) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::Unsupported, why))
}

/// `rect` (left, top, width, height) is non-empty and inside a `width` by `height` frame.
pub(crate) fn check_rect(rect: (u32, u32, u32, u32), width: u32, height: u32) -> io::Result<()> {
    let (left, top, w, h) = rect;
    let inside = w > 0
        && h > 0
        && left.checked_add(w).is_some_and(|r| r <= width)
        && top.checked_add(h).is_some_and(|b| b <= height);
    if inside {
        Ok(())
    } else {
        invalid(format!("{rect:?} is not inside a {width}x{height} frame"))
    }
}

/// The output is `want` in size and `format`, or the call would be a resize or a conversion.
pub(crate) fn check_output(dst: Image, want: (u32, u32), format: Format) -> io::Result<()> {
    if (dst.width, dst.height) != want {
        return invalid(format!(
            "a {}x{} output for a {}x{} result; that is a resize",
            dst.width, dst.height, want.0, want.1
        ));
    }
    if dst.format != format {
        return unsupported(format!(
            "{:?} output from {format:?}: one format at a time",
            dst.format
        ));
    }
    Ok(())
}

pub(crate) const IDENTITY: Matrix = Matrix {
    m: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
};

fn affine(a: f32, b: f32, c: f32, d: f32, e: f32, f: f32) -> Matrix {
    Matrix {
        m: [[a, b, c], [d, e, f], [0.0, 0.0, 1.0]],
    }
}

/// The matrix and filter of a resize of `src` to `dst`.
///
/// Bilinear: a scale by `dst / src` per axis, either way. Area: only a 2x downscale, as a
/// bilinear blit at half scale. At the half-pixel phase each output centre falls on the corner
/// of four source pixels, so the weights are a quarter each: the 2x2 mean, if VGLite's
/// rounding agrees (unmeasured). VGLite has no averaging filter; larger factors would need
/// passes that each round, so they are refused. A trailing odd row or column is cropped away,
/// as the reference does.
pub(crate) fn resize(src: Image, dst: Image, filter: Filter) -> io::Result<(Matrix, i32)> {
    if src.format != dst.format {
        return unsupported(format!(
            "resize from {:?} to {:?}: one format at a time",
            src.format, dst.format
        ));
    }
    let (sw, sh, dw, dh) = (src.width, src.height, dst.width, dst.height);
    match filter {
        Filter::Area => {
            if (sw / 2).max(1) != dw || (sh / 2).max(1) != dh || sw < 2 || sh < 2 {
                return unsupported(format!(
                    "area {sw}x{sh} to {dw}x{dh}: VGLite has no averaging filter; only a 2x \
                     downscale (a half-phase bilinear blit) is offered"
                ));
            }
            Ok((affine(0.5, 0.0, 0.0, 0.0, 0.5, 0.0), ffi::FILTER_BI_LINEAR))
        }
        Filter::Bilinear => {
            if (sw, sh) == (dw, dh) {
                return unsupported("bilinear resize to the same size".into());
            }
            Ok((
                affine(
                    dw as f32 / sw as f32,
                    0.0,
                    0.0,
                    0.0,
                    dh as f32 / sh as f32,
                    0.0,
                ),
                ffi::FILTER_BI_LINEAR,
            ))
        }
    }
}

/// The matrix of an unscaled crop of `rect` (left, top, width, height): a translation, point
/// sampled; what falls outside the target is clipped.
pub(crate) fn crop(rect: (u32, u32, u32, u32)) -> Matrix {
    affine(1.0, 0.0, -(rect.0 as f32), 0.0, 1.0, -(rect.1 as f32))
}

/// The matrix of a flip or a clockwise quarter turn of a `w` by `h` source, point sampled.
/// `(x, y)` on the source's pixel grid maps to the target's: a turn of 90 sends the top-left
/// corner to the top-right.
pub(crate) fn transform(t: Transform, w: u32, h: u32) -> Matrix {
    let (w, h) = (w as f32, h as f32);
    match t {
        Transform::FlipHorizontal => affine(-1.0, 0.0, w, 0.0, 1.0, 0.0),
        Transform::FlipVertical => affine(1.0, 0.0, 0.0, 0.0, -1.0, h),
        Transform::Rotate90 => affine(0.0, -1.0, h, 1.0, 0.0, 0.0),
        Transform::Rotate180 => affine(-1.0, 0.0, w, 0.0, -1.0, h),
        Transform::Rotate270 => affine(0.0, 1.0, 0.0, -1.0, 0.0, w),
    }
}

/// VGLite's blend for a mode. The K230's build sets the blit's `in_premult` bit for every
/// RGBA source (`gcFEATURE_VG_SRC_PREMULTIPLIED` 1 skips the code that clears it), and both
/// SRC_OVER modes program the same hardware blend, so by the library's source the unit takes
/// straight alpha and premultiplies it itself. A premultiplied source would be multiplied
/// twice: refused, except for SRC, which copies.
pub(crate) fn blend(mode: BlendMode, premultiplied: bool) -> io::Result<i32> {
    Ok(match (mode, premultiplied) {
        (BlendMode::Src, _) => BLEND_NONE,
        (_, true) => {
            return unsupported(format!(
                "premultiplied {mode:?}: VGLite premultiplies the source itself"
            ));
        }
        (BlendMode::SrcOver, false) => BLEND_SRC_OVER,
        (BlendMode::DstOver, false) => BLEND_DST_OVER,
    })
}

/// `vg_lite_color_t` of RGBA bytes: R in the low byte (`0xAABBGGRR`), as the library's
/// `rgb_to_l` reads it.
pub(crate) fn color(rgba: [u8; 4]) -> u32 {
    u32::from_le_bytes(rgba)
}

/// The path of a one-pixel line from `from` to `to`, both ends included: a 1-wide stroke with
/// square caps between the pixel centres, so each end pixel is covered whole. On axis-aligned
/// and 45-degree lines that covers Bresenham's pixels exactly in theory; other slopes differ
/// where a pixel centre is near the stroke's edge (unmeasured). FP32 coordinates: each opcode
/// in its own 32-bit slot, as `_flatten_path` reads them.
pub(crate) fn line_path(from: (i32, i32), to: (i32, i32)) -> Vec<u32> {
    let centre = |v: i32| (v as f32 + 0.5).to_bits();
    vec![
        ffi::OP_MOVE,
        centre(from.0),
        centre(from.1),
        ffi::OP_LINE,
        centre(to.0),
        centre(to.1),
        ffi::OP_END,
    ]
}

/// A line's bounding box, with the stroke's half width and caps.
pub(crate) fn line_bounds(from: (i32, i32), to: (i32, i32)) -> [f32; 4] {
    let lo = |a: i32, b: i32| a.min(b) as f32 - 0.5;
    let hi = |a: i32, b: i32| a.max(b) as f32 + 1.5;
    [
        lo(from.0, to.0),
        lo(from.1, to.1),
        hi(from.0, to.0),
        hi(from.1, to.1),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(m: &Matrix, x: f32, y: f32) -> (f32, f32) {
        (
            m.m[0][0] * x + m.m[0][1] * y + m.m[0][2],
            m.m[1][0] * x + m.m[1][1] * y + m.m[1][2],
        )
    }

    #[test]
    fn turns_send_the_corners_where_the_reference_does() {
        // The source's top-left pixel (its centre at 0.5, 0.5) lands on the reference's pixel.
        let (w, h) = (4.0, 2.0);
        let cases = [
            (Transform::FlipHorizontal, (w - 0.5, 0.5)),
            (Transform::FlipVertical, (0.5, h - 0.5)),
            (Transform::Rotate90, (h - 0.5, 0.5)),
            (Transform::Rotate180, (w - 0.5, h - 0.5)),
            (Transform::Rotate270, (0.5, w - 0.5)),
        ];
        for (t, want) in cases {
            assert_eq!(apply(&transform(t, 4, 2), 0.5, 0.5), want, "{t:?}");
        }
        assert_eq!(apply(&crop((3, 1, 2, 2)), 3.5, 1.5), (0.5, 0.5));
    }

    #[test]
    fn area_is_a_half_phase_2x_bilinear_and_nothing_else() {
        let img = |w, h| Image::packed(Format::L8, w, h);
        let (m, filter) = resize(img(64, 32), img(32, 16), Filter::Area).unwrap();
        assert_eq!(filter, ffi::FILTER_BI_LINEAR);
        // Output centre (i + 0.5) samples the source at 2i + 1, minus the half-pixel shift:
        // between source pixels 2i and 2i + 1.
        assert_eq!(m.m[0][0], 0.5);
        assert!(resize(img(65, 33), img(32, 16), Filter::Area).is_ok());
        for bad in [(16, 8), (64, 32), (33, 16)] {
            let err = resize(img(64, 32), img(bad.0, bad.1), Filter::Area).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{bad:?}");
        }
        assert!(resize(img(64, 32), img(100, 50), Filter::Bilinear).is_ok());
        assert!(resize(img(64, 32), img(64, 32), Filter::Bilinear).is_err());
        let rgba = Image::packed(Format::Rgba8888, 32, 16);
        assert!(resize(img(64, 32), rgba, Filter::Bilinear).is_err());
    }

    #[test]
    fn layouts_blends_and_paths() {
        let img = Image::packed(Format::Rgba8888, 16, 2);
        assert!(img.direct() && img.check(128).is_ok() && img.check(127).is_err());
        assert!(!Image::packed(Format::Rgba8888, 5, 2).direct());
        assert_eq!(Image { stride: 80, ..img }.frame_len(), 80 + 64);
        assert_eq!(blend(BlendMode::Src, true).unwrap(), BLEND_NONE);
        assert!(blend(BlendMode::DstOver, true).is_err());
        assert_eq!(color([1, 2, 3, 4]), 0x0403_0201);
        let path = line_path((1, 2), (3, 2));
        assert_eq!(path.len(), 7);
        assert_eq!(f32::from_bits(path[1]), 1.5);
        assert_eq!(line_bounds((3, 2), (1, 0)), [0.5, -0.5, 4.5, 3.5]);
    }
}
