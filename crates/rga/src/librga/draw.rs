//! Fill, raster operations and alpha blending through librga (`imfill`, `imrop`, `imblend`).
//! The mainline driver has none of them: Linux 7.2 stores `V4L2_CID_BG_COLOR` but never
//! programs it. librga's C entry points draw no lines; axis-aligned boxes are four fills.
//!
//! An 8-bit mask is handed to the engine as RGBA8888 at a quarter of its width: a solid fill
//! and the bitwise operations work per byte, so the relabelling is exact, but rows, the
//! rectangle's left edge and its width must be multiples of four bytes.

use std::io;
use std::os::fd::BorrowedFd;
use std::os::raw::c_int;

use super::{ImRect, Image, Librga, RgaBuffer, RgaFormat, check, missing, status};
use crate::transform::check_crop;

/// `IM_ROP_*`: `dst = src op dst`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rop {
    And,
    Or,
    Xor,
}

impl Rop {
    fn code(self) -> c_int {
        match self {
            Rop::And => 0x88,
            Rop::Or => 0xee,
            Rop::Xor => 0xf6,
        }
    }
}

/// `IM_ALPHA_BLEND_*` (Porter-Duff), foreground onto the destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlendMode {
    /// The foreground over the destination.
    SrcOver,
    /// The foreground replaces the destination.
    Src,
    /// The destination over the foreground.
    DstOver,
}

impl BlendMode {
    fn code(self, premultiplied: bool) -> c_int {
        let mode = match self {
            BlendMode::SrcOver => 1 << 6,
            BlendMode::Src => 1 << 7,
            BlendMode::DstOver => 1 << 13,
        };
        if premultiplied { mode | 1 << 25 } else { mode }
    }
}

/// An all-zero `rga_buffer_t`: librga's "no second source".
fn none() -> RgaBuffer {
    // SAFETY: every field is an integer or a raw pointer, for which zero is valid.
    unsafe { std::mem::zeroed() }
}

/// An 8-bit `width` by `height` mask as RGBA8888 at a quarter of the width.
fn mask_as_rgba(width: u32, height: u32) -> io::Result<Image> {
    if !width.is_multiple_of(4) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("a mask {width} bytes wide: rows must be multiples of four bytes"),
        ));
    }
    Ok(Image::packed(RgaFormat::Rgba8888, width / 4, height))
}

impl Librga {
    /// Fill `rect` (left, top, width, height) of an RGBA8888 image with `rgba`. Byte buffer.
    pub fn fill_bytes(
        &self,
        image: Image,
        buf: &mut [u8],
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
    ) -> io::Result<()> {
        check(image, buf.len(), image, buf.len())?;
        self.fill(image, self.wrap_ptr(image, buf.as_mut_ptr()), rect, rgba)
    }

    /// [`Librga::fill_bytes`] on a dmabuf.
    pub fn fill_dmabuf(
        &self,
        image: Image,
        fd: BorrowedFd<'_>,
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
    ) -> io::Result<()> {
        self.fill(image, self.wrap_fd(image, fd), rect, rgba)
    }

    /// Set `rect` of an 8-bit mask to `value`. The mask's width, `rect`'s left edge and its
    /// width must be multiples of four (see the module docs); otherwise `Unsupported`.
    pub fn fill_mask_bytes(
        &self,
        width: u32,
        height: u32,
        mask: &mut [u8],
        rect: (u32, u32, u32, u32),
        value: u8,
    ) -> io::Result<()> {
        let image = mask_as_rgba(width, height)?;
        check_crop(rect, width, height)?;
        if !rect.0.is_multiple_of(4) || !rect.2.is_multiple_of(4) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("mask rectangle {rect:?}: left and width must be multiples of four"),
            ));
        }
        let quarter = (rect.0 / 4, rect.1, rect.2 / 4, rect.3);
        self.fill_bytes(image, mask, quarter, [value; 4])
    }

    /// The outline of `rect`, `thickness` pixels inside it, as up to four fills.
    pub fn rectangle_bytes(
        &self,
        image: Image,
        buf: &mut [u8],
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
        thickness: u32,
    ) -> io::Result<()> {
        let (left, top, w, h) = rect;
        let t = thickness.max(1);
        if t * 2 >= w || t * 2 >= h {
            return self.fill_bytes(image, buf, rect, rgba);
        }
        for side in [
            (left, top, w, t),
            (left, top + h - t, w, t),
            (left, top + t, t, h - 2 * t),
            (left + w - t, top + t, t, h - 2 * t),
        ] {
            self.fill_bytes(image, buf, side, rgba)?;
        }
        Ok(())
    }

    fn fill(
        &self,
        image: Image,
        buf: RgaBuffer,
        rect: (u32, u32, u32, u32),
        [r, g, b, a]: [u8; 4],
    ) -> io::Result<()> {
        check_crop(rect, image.width, image.height)?;
        let call = self.api.fill.ok_or_else(|| missing("imfill_t"))?;
        let rect = ImRect {
            x: rect.0 as c_int,
            y: rect.1 as c_int,
            width: rect.2 as c_int,
            height: rect.3 as c_int,
        };
        // Packed A, B, G, R, as librga's fill sample gives it.
        let color = i32::from_le_bytes([r, g, b, a]);
        // SAFETY: the buffer describes live memory for the (synchronous) call.
        status(unsafe { call(buf, rect, color, 1) })
    }

    /// `dst = src op dst` on two 8-bit masks of the same size (width a multiple of four).
    pub fn rop_mask_bytes(
        &self,
        width: u32,
        height: u32,
        src: &[u8],
        dst: &mut [u8],
        op: Rop,
    ) -> io::Result<()> {
        let image = mask_as_rgba(width, height)?;
        check(image, src.len(), image, dst.len())?;
        self.rop(
            self.wrap_ptr(image, src.as_ptr()),
            self.wrap_ptr(image, dst.as_mut_ptr()),
            op,
        )
    }

    /// [`Librga::rop_mask_bytes`] between dmabufs.
    pub fn rop_mask_dmabuf(
        &self,
        width: u32,
        height: u32,
        src: BorrowedFd<'_>,
        dst: BorrowedFd<'_>,
        op: Rop,
    ) -> io::Result<()> {
        let image = mask_as_rgba(width, height)?;
        self.rop(self.wrap_fd(image, src), self.wrap_fd(image, dst), op)
    }

    fn rop(&self, src: RgaBuffer, dst: RgaBuffer, op: Rop) -> io::Result<()> {
        let call = self.api.rop.ok_or_else(|| missing("imrop_t"))?;
        // SAFETY: both buffers describe live memory for the (synchronous) call.
        status(unsafe { call(src, dst, op.code(), 1) })
    }

    /// Blend RGBA8888 `fg` onto `dst` in place with `mode`; `premultiplied` says the colours
    /// are already multiplied by alpha (`IM_ALPHA_BLEND_PRE_MUL`). To blend through a mask, the
    /// mask is `fg`'s alpha. Byte buffers of the same size.
    pub fn blend_bytes(
        &self,
        image: Image,
        fg: &[u8],
        dst: &mut [u8],
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<()> {
        check(image, fg.len(), image, dst.len())?;
        self.blend(
            self.wrap_ptr(image, fg.as_ptr()),
            self.wrap_ptr(image, dst.as_mut_ptr()),
            mode,
            premultiplied,
        )
    }

    /// [`Librga::blend_bytes`] between dmabufs.
    pub fn blend_dmabuf(
        &self,
        image: Image,
        fg: BorrowedFd<'_>,
        dst: BorrowedFd<'_>,
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<()> {
        self.blend(
            self.wrap_fd(image, fg),
            self.wrap_fd(image, dst),
            mode,
            premultiplied,
        )
    }

    fn blend(
        &self,
        fg: RgaBuffer,
        dst: RgaBuffer,
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<()> {
        let call = self.api.blend.ok_or_else(|| missing("imblend_t"))?;
        // SAFETY: the buffers describe live memory for the (synchronous) call; the second
        // source is librga's empty buffer, as its `imblend` macro passes it.
        status(unsafe { call(fg, none(), dst, mode.code(premultiplied), 1) })
    }
}
