//! The buffers VGLite reads and writes: the caller's dmabuf mapped directly, or a staging buffer
//! the library allocates (`vg_lite_allocate`: `dma_alloc_coherent`, physically contiguous) with
//! the caller's pixels copied in and out.
//!
//! The kernel driver maps no user memory, and its dmabuf map keeps only the last scatter-list
//! entry's address (`vg_lite_hal_map`), so a dmabuf maps correctly only when it is one
//! physically contiguous block. Which it is cannot be read from user space: the caller says
//! ([`Backing`]).

use std::io;
use std::os::fd::BorrowedFd;

use lemnos_linux::dma::{Access, DmaBufRef, Mapping};

use crate::ffi::{Api, Buffer, MAP_DMABUF, status};
use crate::plan::Image;

/// What backs a dmabuf, as far as VGLite's kernel driver is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Backing {
    /// One physically contiguous block: a CMA heap (`linux,cma`) or the K230's MMZ. VGLite reads
    /// and writes it in place when its stride meets the alignment.
    Contiguous,
    /// Pages (the `system` heap, most V4L2 and DRM buffers): copied through a contiguous
    /// staging buffer. Mapping it directly would read and write the wrong memory.
    Scattered,
}

/// A dmabuf holding one image.
#[derive(Clone, Copy, Debug)]
pub struct Frame<'a> {
    pub image: Image,
    pub fd: BorrowedFd<'a>,
    pub backing: Backing,
}

impl Frame<'_> {
    /// VGLite can take it in place.
    pub(crate) fn direct(&self) -> bool {
        self.backing == Backing::Contiguous && self.image.direct()
    }
}

/// What an operation cost beyond the unit's own work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Transfer {
    /// Bytes the CPU copied into and out of staging buffers; zero when every buffer was a
    /// directly mapped contiguous dmabuf.
    pub staged_bytes: usize,
}

/// Where an operation's pixels come from.
pub(crate) enum Input<'a> {
    Bytes(&'a [u8]),
    Frame(Frame<'a>),
}

/// Where an operation's pixels go.
pub(crate) enum Output<'a> {
    Bytes(&'a mut [u8]),
    Frame(Frame<'a>),
}

/// A buffer described to VGLite, released when dropped.
pub(crate) struct Surface {
    api: Api,
    pub(crate) buffer: Buffer,
    image: Image,
    /// The caller's dmabuf mapped for the library's cache maintenance, when not staged.
    _mapping: Option<Mapping>,
    staged: bool,
}

impl Surface {
    /// A staging buffer for `image`; the library picks the stride.
    pub(crate) fn staging(api: Api, image: Image) -> io::Result<Self> {
        let mut buffer = Buffer::new(image.width, image.height, 0, image.format.code());
        // SAFETY: a valid buffer description; the library fills its memory fields.
        status("vg_lite_allocate", unsafe { (api.allocate)(&mut buffer) })?;
        Ok(Self {
            api,
            buffer,
            image,
            _mapping: None,
            staged: true,
        })
    }

    /// The caller's contiguous dmabuf, in place. `vg_lite_finish` cleans and invalidates the
    /// target's `memory`, so it points at a real mapping of the buffer.
    pub(crate) fn mapped(api: Api, frame: Frame<'_>) -> io::Result<Self> {
        let dmabuf = DmaBufRef::from_fd(frame.fd)?;
        frame.image.check(dmabuf.len())?;
        let mut mapping = dmabuf.map()?;
        let image = frame.image;
        let mut buffer = Buffer::new(image.width, image.height, image.stride, image.format.code());
        buffer.memory = mapping.as_mut_slice().as_mut_ptr().cast();
        let fd = std::os::fd::AsRawFd::as_raw_fd(&frame.fd);
        // SAFETY: a valid description of a dmabuf that outlives the surface (borrowed by the
        // caller for the call).
        status("vg_lite_map", unsafe {
            (api.map)(&mut buffer, MAP_DMABUF, fd)
        })?;
        Ok(Self {
            api,
            buffer,
            image,
            _mapping: Some(mapping),
            staged: false,
        })
    }

    /// The surface for reading `input`, its pixels copied in when staged.
    pub(crate) fn input(
        api: Api,
        image: Image,
        input: &Input<'_>,
        transfer: &mut Transfer,
    ) -> io::Result<Self> {
        match input {
            Input::Frame(frame) if frame.direct() => Self::mapped(api, *frame),
            _ => {
                let mut surface = Self::staging(api, image)?;
                surface.upload(input, transfer)?;
                Ok(surface)
            }
        }
    }

    /// The surface for writing `output`. With `keep`, its current pixels matter (a blend or a
    /// drawing onto it) and are copied in when staged.
    pub(crate) fn output(
        api: Api,
        image: Image,
        output: &mut Output<'_>,
        keep: bool,
        transfer: &mut Transfer,
    ) -> io::Result<Self> {
        match output {
            Output::Frame(frame) if frame.direct() => Self::mapped(api, *frame),
            _ => {
                let mut surface = Self::staging(api, image)?;
                if keep {
                    let input = match output {
                        Output::Bytes(bytes) => Input::Bytes(bytes),
                        Output::Frame(frame) => Input::Frame(*frame),
                    };
                    surface.upload(&input, transfer)?;
                }
                Ok(surface)
            }
        }
    }

    fn pixels(&mut self) -> &mut [u8] {
        let len = self.buffer.stride as usize * self.buffer.height as usize;
        // SAFETY: the library allocated `stride * height` bytes at `memory`, mapped into this
        // process, and nothing else touches them while the surface is borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.buffer.memory.cast::<u8>(), len) }
    }

    fn upload(&mut self, input: &Input<'_>, transfer: &mut Transfer) -> io::Result<()> {
        let image = self.image;
        let stride = self.buffer.stride as usize;
        match input {
            Input::Bytes(bytes) => {
                copy_rows(bytes, image.stride as usize, self.pixels(), stride, image)
            }
            Input::Frame(frame) => {
                let dmabuf = DmaBufRef::from_fd(frame.fd)?;
                frame.image.check(dmabuf.len())?;
                let mapping = dmabuf.map()?;
                dmabuf.begin_cpu_access(Access::Read)?;
                copy_rows(
                    mapping.as_slice(),
                    image.stride as usize,
                    self.pixels(),
                    stride,
                    image,
                );
                dmabuf.end_cpu_access(Access::Read)?;
            }
        }
        transfer.staged_bytes += image.row_len() * image.height as usize;
        let len = stride * image.height as usize;
        clean_dcache(self.buffer.memory.cast(), len);
        Ok(())
    }

    /// Copy a staged result out to `output`; nothing to do for a mapped dmabuf. Call after
    /// `vg_lite_finish`, which cleans and invalidates the target's cache lines.
    pub(crate) fn download(
        &mut self,
        output: &mut Output<'_>,
        transfer: &mut Transfer,
    ) -> io::Result<()> {
        if !self.staged {
            return Ok(());
        }
        let image = self.image;
        let stride = self.buffer.stride as usize;
        match output {
            Output::Bytes(bytes) => {
                copy_rows(self.pixels(), stride, bytes, image.stride as usize, image)
            }
            Output::Frame(frame) => {
                let dmabuf = DmaBufRef::from_fd(frame.fd)?;
                let mut mapping = dmabuf.map()?;
                dmabuf.begin_cpu_access(Access::Write)?;
                copy_rows(
                    self.pixels(),
                    stride,
                    mapping.as_mut_slice(),
                    image.stride as usize,
                    image,
                );
                dmabuf.end_cpu_access(Access::Write)?;
            }
        }
        transfer.staged_bytes += image.row_len() * image.height as usize;
        Ok(())
    }
}

impl Drop for Surface {
    fn drop(&mut self) {
        let release = if self.staged {
            self.api.free
        } else {
            self.api.unmap
        };
        // SAFETY: the buffer was allocated or mapped by this library and is released once.
        let _ = unsafe { release(&mut self.buffer) };
    }
}

fn copy_rows(from: &[u8], from_stride: usize, to: &mut [u8], to_stride: usize, image: Image) {
    let row = image.row_len();
    for y in 0..image.height as usize {
        to[y * to_stride..][..row].copy_from_slice(&from[y * from_stride..][..row]);
    }
}

/// Write the CPU's stores to `len` bytes at `ptr` back to memory before the GPU reads them, as
/// the library's `thead_csi_dcache_clean_invalid_range` does: the C910's `th.dcache.civa`
/// (XTheadCmo), one 64-byte line at a time. Nothing to do on other targets (the tests' hosts).
fn clean_dcache(ptr: *const u8, len: usize) {
    #[cfg(target_arch = "riscv64")]
    {
        let start = ptr as usize & !63;
        let end = ptr as usize + len;
        // SAFETY: cache maintenance on lines of a live mapping; no memory is written.
        unsafe {
            core::arch::asm!("fence iorw, iorw");
            let mut line = start;
            while line < end {
                // th.dcache.civa a0: 0000001 00111 rs1 000 00000 0001011.
                core::arch::asm!(".long 0x0275000b", in("a0") line);
                line += 64;
            }
            core::arch::asm!("fence iorw, iorw");
        }
    }
    #[cfg(not(target_arch = "riscv64"))]
    let _ = (ptr, len);
}
