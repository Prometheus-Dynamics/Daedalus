//! Rockchip's RGA 2D engine (scale, crop, pixel-format conversion) between dmabufs, through the
//! kernel's V4L2 mem2mem interface: the mainline `rockchip-rga` driver on boards, or any
//! mem2mem device (the kernel's `vim2m` in the tests). The binding Daedalus and Styx share.
//!
//! ```no_run
//! # #[cfg(target_os = "linux")] {
//! use daedalus_rga::{Layout, PixelFormat, Rga};
//! let mut rga = Rga::find().expect("rockchip-rga");
//! let src = Layout::packed(PixelFormat::Nv12, 1280, 720);
//! let dst = Layout::packed(PixelFormat::Abgr32, 640, 360);
//! # let (src_fd, dst_fd): (std::os::fd::OwnedFd, std::os::fd::OwnedFd) = todo!();
//! use std::os::fd::AsFd;
//! rga.blit(src, src_fd.as_fd(), src.frame_len(), None, dst, dst_fd.as_fd(), dst.frame_len())?;
//! # }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Status: written and tested against `vim2m` (conversion, scaling, dmabuf in and out); on a
//! Rockchip board untested. [`Librga`] reaches the engine through the vendor's library instead,
//! for RGA3 on vendor kernels, gray input and Y-only output (written, untested on board).
//!
//! Resizes ([`Filter`]: area and bilinear) refuse what a route cannot do as asked; [`caps`]
//! describes each SoC's units, routes and what is known of their exactness.

pub mod caps;
#[cfg(target_os = "linux")]
mod librga;
mod resize;
mod transform;
#[cfg(target_os = "linux")]
mod v4l2;

#[cfg(target_os = "linux")]
pub use librga::{Image, Librga, RgaFormat};
pub use resize::Filter;
pub use transform::Transform;

#[cfg(target_os = "linux")]
pub use rga::Rga;
#[cfg(target_os = "linux")]
pub use v4l2::{Layout, PixelFormat, Rect, find_m2m_device};

#[cfg(target_os = "linux")]
mod rga {
    use std::collections::HashMap;
    use std::io;
    use std::os::fd::BorrowedFd;
    use std::path::{Path, PathBuf};

    use crate::caps::Route;
    use crate::resize::{Filter, source_size};
    use crate::transform::{check_crop, check_size};
    use crate::v4l2::{Layout, Memory, PixelFormat, Rect, Session, find_m2m_device};

    type Key = (Layout, Layout, Option<Rect>, Memory);

    /// A 2D engine on a mem2mem device. Each distinct (source layout, destination layout, crop,
    /// memory) gets its own session, configured once and reused.
    pub struct Rga {
        path: PathBuf,
        sessions: HashMap<Key, Session>,
    }

    impl Rga {
        /// The RGA, when the kernel exposes `rockchip-rga`.
        pub fn find() -> Option<Self> {
            Some(Self::on_device(&find_m2m_device("rockchip-rga")?))
        }

        /// The engine on any V4L2 mem2mem device (`vim2m` for tests).
        pub fn on_device(path: &Path) -> Self {
            Self {
                path: path.to_path_buf(),
                sessions: HashMap::new(),
            }
        }

        fn session(
            &mut self,
            src: Layout,
            dst: Layout,
            crop: Option<Rect>,
            memory: Memory,
        ) -> io::Result<&mut Session> {
            let key = (src, dst, crop, memory);
            if !self.sessions.contains_key(&key) {
                let s = Session::open(&self.path, src, dst, crop, memory)?;
                self.sessions.insert(key, s);
            }
            Ok(self.sessions.get_mut(&key).expect("inserted above"))
        }

        /// Convert, scale (to `dst`'s size) and crop (`crop` of the source) from dmabuf
        /// `src_fd` into dmabuf `dst_fd`, without a CPU copy. Returns the bytes the engine wrote.
        #[allow(clippy::too_many_arguments)]
        pub fn blit(
            &mut self,
            src: Layout,
            src_fd: BorrowedFd<'_>,
            src_len: usize,
            crop: Option<Rect>,
            dst: Layout,
            dst_fd: BorrowedFd<'_>,
            dst_len: usize,
        ) -> io::Result<usize> {
            self.session(src, dst, crop, Memory::Dmabuf)?
                .convert_dmabuf(src_fd, src_len, dst_fd, dst_len)
        }

        /// [`Rga::blit`] between byte buffers (the engine's own buffers, with a copy in and
        /// out). Returns the bytes written.
        pub fn convert(
            &mut self,
            src: Layout,
            src_bytes: &[u8],
            crop: Option<Rect>,
            dst: Layout,
            dst_bytes: &mut [u8],
        ) -> io::Result<usize> {
            self.session(src, dst, crop, Memory::Mmap)?
                .convert_bytes(src_bytes, dst_bytes)
        }

        /// The frame sizes the device expects for a conversion.
        pub fn frame_lens(&mut self, src: Layout, dst: Layout) -> io::Result<(usize, usize)> {
            Ok(self.session(src, dst, None, Memory::Mmap)?.frame_lens())
        }

        /// The source crop for a resize: the whole frame, or the top-left part an area
        /// downscale reads when the size is not a multiple of the factor.
        fn resize_crop(src: Layout, dst: Layout, filter: Filter) -> io::Result<Option<Rect>> {
            let (width, height) = source_size(
                (src.width, src.height),
                (dst.width, dst.height),
                filter,
                Route::V4l2,
            )?;
            Ok(
                ((width, height) != (src.width, src.height)).then_some(Rect {
                    left: 0,
                    top: 0,
                    width,
                    height,
                }),
            )
        }

        /// Resize dmabuf `src_fd` into dmabuf `dst_fd` (to `dst`'s size) with `filter`. The
        /// mainline driver has no filter control: it averages when it downscales and
        /// interpolates when it upscales, so [`Filter::Area`] must be an integer downscale and
        /// [`Filter::Bilinear`] an upscale; anything else is `Unsupported`. See
        /// [`crate::caps`] for what is known of each unit's exactness. Returns the bytes written.
        #[allow(clippy::too_many_arguments)]
        pub fn resize(
            &mut self,
            src: Layout,
            src_fd: BorrowedFd<'_>,
            src_len: usize,
            dst: Layout,
            dst_fd: BorrowedFd<'_>,
            dst_len: usize,
            filter: Filter,
        ) -> io::Result<usize> {
            let crop = Self::resize_crop(src, dst, filter)?;
            self.blit(src, src_fd, src_len, crop, dst, dst_fd, dst_len)
        }

        /// [`Rga::resize`] between byte buffers. Returns the bytes written.
        pub fn resize_bytes(
            &mut self,
            src: Layout,
            src_bytes: &[u8],
            dst: Layout,
            dst_bytes: &mut [u8],
            filter: Filter,
        ) -> io::Result<usize> {
            let crop = Self::resize_crop(src, dst, filter)?;
            self.convert(src, src_bytes, crop, dst, dst_bytes)
        }

        /// Resize 8-bit luma, tightly packed, with `filter`. The mainline driver has no gray
        /// format, so the luma goes through NV12 with neutral chroma, and only Y comes back:
        /// the engine moves 1.5 times the bytes. A source that is already NV12 should go
        /// through [`Rga::resize`] instead, without the copies.
        #[allow(clippy::too_many_arguments)]
        pub fn resize_luma(
            &mut self,
            width: u32,
            height: u32,
            src: &[u8],
            out_width: u32,
            out_height: u32,
            dst: &mut [u8],
            filter: Filter,
        ) -> io::Result<()> {
            let (luma, out_luma) = (
                width as usize * height as usize,
                out_width as usize * out_height as usize,
            );
            if src.len() < luma || dst.len() < out_luma {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "buffers of {} and {} bytes for {width}x{height} and \
                         {out_width}x{out_height} luma",
                        src.len(),
                        dst.len()
                    ),
                ));
            }
            let s = Layout::packed(PixelFormat::Nv12, width, height);
            let d = Layout::packed(PixelFormat::Nv12, out_width, out_height);
            let mut staged = vec![128u8; s.frame_len()];
            staged[..luma].copy_from_slice(&src[..luma]);
            let mut out = vec![0u8; d.frame_len()];
            self.resize_bytes(s, &staged, d, &mut out, filter)?;
            dst[..out_luma].copy_from_slice(&out[..out_luma]);
            Ok(())
        }

        /// Copy the `rect` of dmabuf `src_fd` into dmabuf `dst_fd`, whose layout must have the
        /// rectangle's size (a different size is a resize: [`Rga::resize`]). A full-frame
        /// rectangle is a copy. Returns the bytes written.
        #[allow(clippy::too_many_arguments)]
        pub fn crop(
            &mut self,
            src: Layout,
            src_fd: BorrowedFd<'_>,
            src_len: usize,
            rect: Rect,
            dst: Layout,
            dst_fd: BorrowedFd<'_>,
            dst_len: usize,
        ) -> io::Result<usize> {
            let crop = Self::crop_rect(src, rect, dst)?;
            self.blit(src, src_fd, src_len, crop, dst, dst_fd, dst_len)
        }

        /// [`Rga::crop`] between byte buffers. Returns the bytes written.
        pub fn crop_bytes(
            &mut self,
            src: Layout,
            src_bytes: &[u8],
            rect: Rect,
            dst: Layout,
            dst_bytes: &mut [u8],
        ) -> io::Result<usize> {
            let crop = Self::crop_rect(src, rect, dst)?;
            self.convert(src, src_bytes, crop, dst, dst_bytes)
        }

        /// The device crop for `rect`: none for the full frame.
        fn crop_rect(src: Layout, rect: Rect, dst: Layout) -> io::Result<Option<Rect>> {
            let (Ok(left), Ok(top)) = (u32::try_from(rect.left), u32::try_from(rect.top)) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("crop {rect:?} starts outside the frame"),
                ));
            };
            check_crop((left, top, rect.width, rect.height), src.width, src.height)?;
            check_size((dst.width, dst.height), (rect.width, rect.height))?;
            let full = (left, top, rect.width, rect.height) == (0, 0, src.width, src.height);
            Ok((!full).then_some(rect))
        }

        /// RGB8 to RGBA8, tightly packed. The mainline driver has no R,G,B,A order, so the
        /// frames are relabelled: RGB bytes read as `BGR24` and written as `ABGR32` (B,G,R,A)
        /// come out R,G,B,A.
        pub fn rgb_to_rgba(
            &mut self,
            width: u32,
            height: u32,
            src: &[u8],
            dst: &mut [u8],
        ) -> io::Result<()> {
            let s = Layout::packed(PixelFormat::Bgr24, width, height);
            let d = Layout::packed(PixelFormat::Abgr32, width, height);
            self.convert(s, src, None, d, dst).map(drop)
        }

        /// RGBA8 to RGB8 (the reverse relabelling).
        pub fn rgba_to_rgb(
            &mut self,
            width: u32,
            height: u32,
            src: &[u8],
            dst: &mut [u8],
        ) -> io::Result<()> {
            let s = Layout::packed(PixelFormat::Abgr32, width, height);
            let d = Layout::packed(PixelFormat::Bgr24, width, height);
            self.convert(s, src, None, d, dst).map(drop)
        }
    }
}
