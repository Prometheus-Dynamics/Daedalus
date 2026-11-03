//! [`Vglite`]: the loaded library and one call per operation.

use std::io;
use std::sync::{Mutex, OnceLock};

use crate::ffi::{
    self, Api, BLEND_NONE, CAP_SQUARE, DRAW_STROKE_PATH, FILL_NON_ZERO, FILTER_POINT, JOIN_MITER,
    PATH_FP32, Path, QUALITY_LOW, Rectangle, status,
};
use crate::plan::{
    self, BlendMode, Format, IDENTITY, Image, check_output, check_rect, invalid, unsupported,
};
use crate::surface::{Frame, Input, Output, Surface, Transfer};
use crate::{Filter, Transform};

/// The tessellation window `vg_lite_init` allocates for paths; larger targets are drawn in
/// windows of this size.
const TESS: i32 = 256;

/// The library keeps one context per process (`s_context`): one call at a time.
static LOCK: Mutex<()> = Mutex::new(());
static LOADED: OnceLock<Result<Api, String>> = OnceLock::new();

/// `libvg_lite.so`, loaded and initialised once per process. See the crate docs.
#[derive(Clone, Copy)]
pub struct Vglite {
    api: Api,
}

impl Vglite {
    /// Load `libvg_lite.so` and open the GPU (`vg_lite_init`).
    pub fn load() -> io::Result<Self> {
        let loaded = LOADED.get_or_init(|| {
            let api = ffi::load().map_err(|e| e.to_string())?;
            // SAFETY: the library's own initialisation, once per process.
            status("vg_lite_init", unsafe { (api.init)(TESS, TESS) }).map_err(|e| e.to_string())?;
            Ok(api)
        });
        match loaded {
            Ok(api) => Ok(Self { api: *api }),
            Err(why) => Err(io::Error::new(io::ErrorKind::NotFound, why.clone())),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_api(api: Api) -> Self {
        Self { api }
    }

    /// One operation: the surfaces, `draw` on them, `vg_lite_finish`, the result copied out.
    fn run(
        &self,
        src: Option<(Image, Input<'_>)>,
        dst: Image,
        mut out: Output<'_>,
        keep: bool,
        draw: impl FnOnce(&Api, Option<&mut Surface>, &mut Surface) -> io::Result<()>,
    ) -> io::Result<Transfer> {
        let _one = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut transfer = Transfer::default();
        let mut source = match &src {
            Some((image, input)) => Some(Surface::input(self.api, *image, input, &mut transfer)?),
            None => None,
        };
        let mut target = Surface::output(self.api, dst, &mut out, keep, &mut transfer)?;
        draw(&self.api, source.as_mut(), &mut target)?;
        // SAFETY: submits the queued commands and waits for them.
        status("vg_lite_finish", unsafe { (self.api.finish)() })?;
        target.download(&mut out, &mut transfer)?;
        Ok(transfer)
    }

    /// A blit of the whole source through `matrix`.
    fn blit(
        &self,
        src: (Image, Input<'_>),
        dst: Image,
        out: Output<'_>,
        keep: bool,
        (matrix, blend, filter): (ffi::Matrix, i32, i32),
    ) -> io::Result<Transfer> {
        self.run(Some(src), dst, out, keep, |api, source, target| {
            let source = source.expect("a blit has a source");
            let mut matrix = matrix;
            // SAFETY: both buffers are allocated or mapped for the call.
            status("vg_lite_blit", unsafe {
                (api.blit)(
                    &mut target.buffer,
                    &mut source.buffer,
                    &mut matrix,
                    blend,
                    0,
                    filter,
                )
            })
        })
    }

    fn resize(
        &self,
        src: Image,
        input: Input<'_>,
        dst: Image,
        out: Output<'_>,
        filter: Filter,
    ) -> io::Result<Transfer> {
        let plan = plan::resize(src, dst, filter)?;
        // An area downscale reads whole 2x2 blocks: a trailing odd row or column is cropped.
        let src = match filter {
            Filter::Area => Image {
                width: dst.width * 2,
                height: dst.height * 2,
                ..src
            },
            Filter::Bilinear => src,
        };
        self.blit((src, input), dst, out, false, (plan.0, BLEND_NONE, plan.1))
    }

    /// Resize between byte buffers of one format, to `dst`'s size: [`Filter::Bilinear`] either
    /// way, [`Filter::Area`] by exactly 2 (a half-phase bilinear blit; anything else is
    /// `Unsupported`).
    pub fn resize_bytes(
        &self,
        src: Image,
        from: &[u8],
        dst: Image,
        to: &mut [u8],
        filter: Filter,
    ) -> io::Result<Transfer> {
        src.check(from.len())?;
        dst.check(to.len())?;
        self.resize(src, Input::Bytes(from), dst, Output::Bytes(to), filter)
    }

    /// [`Vglite::resize_bytes`] between dmabufs.
    pub fn resize_dmabuf(
        &self,
        src: Frame<'_>,
        dst: Frame<'_>,
        filter: Filter,
    ) -> io::Result<Transfer> {
        self.resize(
            src.image,
            Input::Frame(src),
            dst.image,
            Output::Frame(dst),
            filter,
        )
    }

    fn crop(
        &self,
        src: Image,
        input: Input<'_>,
        rect: (u32, u32, u32, u32),
        dst: Image,
        out: Output<'_>,
    ) -> io::Result<Transfer> {
        check_rect(rect, src.width, src.height)?;
        check_output(dst, (rect.2, rect.3), src.format)?;
        let plan = (plan::crop(rect), BLEND_NONE, FILTER_POINT);
        self.blit((src, input), dst, out, false, plan)
    }

    /// `rect` (left, top, width, height) of `src`, unscaled, into `dst` of the rectangle's size:
    /// a translated, point-sampled blit. A full-frame rectangle is a copy.
    pub fn crop_bytes(
        &self,
        src: Image,
        from: &[u8],
        rect: (u32, u32, u32, u32),
        dst: Image,
        to: &mut [u8],
    ) -> io::Result<Transfer> {
        src.check(from.len())?;
        dst.check(to.len())?;
        self.crop(src, Input::Bytes(from), rect, dst, Output::Bytes(to))
    }

    /// [`Vglite::crop_bytes`] between dmabufs.
    pub fn crop_dmabuf(
        &self,
        src: Frame<'_>,
        rect: (u32, u32, u32, u32),
        dst: Frame<'_>,
    ) -> io::Result<Transfer> {
        self.crop(
            src.image,
            Input::Frame(src),
            rect,
            dst.image,
            Output::Frame(dst),
        )
    }

    fn transform(
        &self,
        src: Image,
        input: Input<'_>,
        dst: Image,
        out: Output<'_>,
        t: Transform,
    ) -> io::Result<Transfer> {
        check_output(dst, t.output_size(src.width, src.height), src.format)?;
        let plan = (
            plan::transform(t, src.width, src.height),
            BLEND_NONE,
            FILTER_POINT,
        );
        self.blit((src, input), dst, out, false, plan)
    }

    /// A flip or a clockwise quarter turn into `dst` of the result's size: a mirror or rotation
    /// matrix, point sampled (the K230's VGLite has no mirror shortcut).
    pub fn transform_bytes(
        &self,
        src: Image,
        from: &[u8],
        dst: Image,
        to: &mut [u8],
        t: Transform,
    ) -> io::Result<Transfer> {
        src.check(from.len())?;
        dst.check(to.len())?;
        self.transform(src, Input::Bytes(from), dst, Output::Bytes(to), t)
    }

    /// [`Vglite::transform_bytes`] between dmabufs.
    pub fn transform_dmabuf(
        &self,
        src: Frame<'_>,
        dst: Frame<'_>,
        t: Transform,
    ) -> io::Result<Transfer> {
        self.transform(
            src.image,
            Input::Frame(src),
            dst.image,
            Output::Frame(dst),
            t,
        )
    }

    fn fill(
        &self,
        image: Image,
        out: Output<'_>,
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        check_rect(rect, image.width, image.height)?;
        if matches!(image.format, Format::L8 | Format::Index8) {
            return unsupported(format!(
                "fill of {:?}: VGLite converts the colour to luma with a truncating float sum; \
                 fill a mask as A8",
                image.format
            ));
        }
        let mut rect = Rectangle {
            x: rect.0 as i32,
            y: rect.1 as i32,
            width: rect.2 as i32,
            height: rect.3 as i32,
        };
        self.run(None, image, out, true, |api, _, target| {
            // SAFETY: the target is allocated or mapped for the call.
            status("vg_lite_clear", unsafe {
                (api.clear)(&mut target.buffer, &mut rect, plan::color(rgba))
            })
        })
    }

    /// Set `rect` (left, top, width, height) to `rgba` (`vg_lite_clear`). An A8 mask takes the
    /// alpha byte. L8 is `Unsupported`.
    pub fn fill_bytes(
        &self,
        image: Image,
        buf: &mut [u8],
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        image.check(buf.len())?;
        self.fill(image, Output::Bytes(buf), rect, rgba)
    }

    /// [`Vglite::fill_bytes`] on a dmabuf.
    pub fn fill_dmabuf(
        &self,
        frame: Frame<'_>,
        rect: (u32, u32, u32, u32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        self.fill(frame.image, Output::Frame(frame), rect, rgba)
    }

    fn blend(
        &self,
        fg: Image,
        input: Input<'_>,
        dst: Image,
        out: Output<'_>,
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<Transfer> {
        check_output(dst, (fg.width, fg.height), fg.format)?;
        if !matches!(fg.format, Format::Rgba8888 | Format::Bgra8888) {
            return unsupported(format!("blend of {:?}: RGBA or BGRA only", fg.format));
        }
        let plan = (IDENTITY, plan::blend(mode, premultiplied)?, FILTER_POINT);
        self.blit((fg, input), dst, out, true, plan)
    }

    /// `fg` over (or under) `dst` of the same size and format, in place: SRC, SRC_OVER or
    /// DST_OVER with straight alpha. Premultiplied SRC_OVER and DST_OVER are `Unsupported`.
    pub fn blend_bytes(
        &self,
        fg: Image,
        fg_bytes: &[u8],
        dst: Image,
        dst_bytes: &mut [u8],
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<Transfer> {
        fg.check(fg_bytes.len())?;
        dst.check(dst_bytes.len())?;
        self.blend(
            fg,
            Input::Bytes(fg_bytes),
            dst,
            Output::Bytes(dst_bytes),
            mode,
            premultiplied,
        )
    }

    /// [`Vglite::blend_bytes`] between dmabufs.
    pub fn blend_dmabuf(
        &self,
        fg: Frame<'_>,
        dst: Frame<'_>,
        mode: BlendMode,
        premultiplied: bool,
    ) -> io::Result<Transfer> {
        self.blend(
            fg.image,
            Input::Frame(fg),
            dst.image,
            Output::Frame(dst),
            mode,
            premultiplied,
        )
    }

    fn palette(
        &self,
        indices: Image,
        input: Input<'_>,
        lut: &[[u8; 4]; 256],
        dst: Image,
        out: Output<'_>,
    ) -> io::Result<Transfer> {
        if indices.format != Format::Index8 {
            return invalid(format!(
                "palette indices as {:?}, not Index8",
                indices.format
            ));
        }
        check_output(dst, (indices.width, indices.height), Format::Rgba8888)?;
        let mut table: Vec<u32> = lut.iter().map(|&c| plan::color(c)).collect();
        self.run(
            Some((indices, input)),
            dst,
            out,
            false,
            |api, source, target| {
                let source = source.expect("a palette has indices");
                let mut matrix = IDENTITY;
                // SAFETY: 256 entries; both buffers are allocated or mapped for the call.
                unsafe {
                    status("vg_lite_set_CLUT", (api.set_clut)(256, table.as_mut_ptr()))?;
                    status(
                        "vg_lite_blit",
                        (api.blit)(
                            &mut target.buffer,
                            &mut source.buffer,
                            &mut matrix,
                            BLEND_NONE,
                            0,
                            FILTER_POINT,
                        ),
                    )
                }
            },
        )
    }

    /// 8-bit indices to RGBA8 through 256 entries (an `INDEX_8` source and the colour table),
    /// tightly packed byte buffers.
    pub fn palette_bytes(
        &self,
        width: u32,
        height: u32,
        indices: &[u8],
        lut: &[[u8; 4]; 256],
        dst: &mut [u8],
    ) -> io::Result<Transfer> {
        let (s, d) = (
            Image::packed(Format::Index8, width, height),
            Image::packed(Format::Rgba8888, width, height),
        );
        s.check(indices.len())?;
        d.check(dst.len())?;
        self.palette(s, Input::Bytes(indices), lut, d, Output::Bytes(dst))
    }

    /// [`Vglite::palette_bytes`] between dmabufs (`indices` as [`Format::Index8`]).
    pub fn palette_dmabuf(
        &self,
        indices: Frame<'_>,
        lut: &[[u8; 4]; 256],
        dst: Frame<'_>,
    ) -> io::Result<Transfer> {
        self.palette(
            indices.image,
            Input::Frame(indices),
            lut,
            dst.image,
            Output::Frame(dst),
        )
    }

    fn line(
        &self,
        image: Image,
        out: Output<'_>,
        from: (i32, i32),
        to: (i32, i32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        let Some(paths) = self.api.paths else {
            return unsupported("this libvg_lite has no path entry points".into());
        };
        if image.format == Format::Index8 {
            return unsupported("a line on Index8".into());
        }
        let mut data = plan::line_path(from, to);
        let bounds = plan::line_bounds(from, to);
        let color = plan::color(rgba);
        self.run(None, image, out, true, |api, _, target| {
            let mut path = Path::zeroed();
            let mut matrix = IDENTITY;
            // SAFETY: the path data outlives the path, which is drawn and finished before
            // `vg_lite_clear_path` frees what the library allocated for it.
            unsafe {
                status(
                    "vg_lite_init_path",
                    (paths.init_path)(
                        &mut path,
                        PATH_FP32,
                        QUALITY_LOW,
                        (data.len() * 4) as u32,
                        data.as_mut_ptr().cast(),
                        bounds[0],
                        bounds[1],
                        bounds[2],
                        bounds[3],
                    ),
                )?;
                let drawn = (|| {
                    status(
                        "vg_lite_set_stroke",
                        (paths.set_stroke)(
                            &mut path,
                            CAP_SQUARE,
                            JOIN_MITER,
                            1.0,
                            4.0,
                            std::ptr::null_mut(),
                            0,
                            0.0,
                            color,
                        ),
                    )?;
                    status(
                        "vg_lite_set_path_type",
                        (paths.set_path_type)(&mut path, DRAW_STROKE_PATH),
                    )?;
                    status("vg_lite_update_stroke", (paths.update_stroke)(&mut path))?;
                    status(
                        "vg_lite_draw",
                        (paths.draw)(
                            &mut target.buffer,
                            &mut path,
                            FILL_NON_ZERO,
                            &mut matrix,
                            BLEND_NONE,
                            color,
                        ),
                    )?;
                    status("vg_lite_finish", (api.finish)())
                })();
                let cleared = status("vg_lite_clear_path", (paths.clear_path)(&mut path));
                drawn.and(cleared)
            }
        })
    }

    /// A one-pixel line from `from` to `to`, both ends included, in `rgba`: a 1-wide stroke with
    /// square caps through the pixel centres (`vg_lite_draw`, no anti-aliasing). Pixels outside
    /// the frame are clipped. Which pixels a slope other than 0, 45 or 90 degrees covers against
    /// Bresenham's is unmeasured.
    pub fn line_bytes(
        &self,
        image: Image,
        buf: &mut [u8],
        from: (i32, i32),
        to: (i32, i32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        image.check(buf.len())?;
        self.line(image, Output::Bytes(buf), from, to, rgba)
    }

    /// [`Vglite::line_bytes`] on a dmabuf.
    pub fn line_dmabuf(
        &self,
        frame: Frame<'_>,
        from: (i32, i32),
        to: (i32, i32),
        rgba: [u8; 4],
    ) -> io::Result<Transfer> {
        self.line(frame.image, Output::Frame(frame), from, to, rgba)
    }
}
