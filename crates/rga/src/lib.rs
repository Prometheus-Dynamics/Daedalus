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
//! Rockchip board untested. A librga backend (RGA3 on vendor kernels) is planned behind the same
//! API.

#[cfg(target_os = "linux")]
mod v4l2;

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
