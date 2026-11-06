//! Dma-buf frames over the surface: a [`Frame`] to push into an input port, and the
//! [`FrameLease`] taken from an output port.
//!
//! A pushed frame is borrowed for the call: the surface duplicates its descriptors and keeps those,
//! so the caller's dma-bufs are shared, never copied. A lease owns the descriptors the surface
//! hands out (duplicated for it) and keeps the frame alive until it drops.

use std::marker::PhantomData;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

use lemnos_abi::{BufferDesc, Handle, MAX_PLANES, PlaneDesc};

use crate::error::HostError;
use crate::surface::Surface;

/// A frame to push: an image or a block of bytes, one plane per dma-buf range. The descriptors
/// are borrowed for `'a`; a push only needs them for the call.
#[derive(Clone, Copy)]
pub struct Frame<'a> {
    desc: BufferDesc,
    fds: PhantomData<BorrowedFd<'a>>,
}

impl<'a> Frame<'a> {
    /// An image of `width` x `height` pixels in `fourcc` (a DRM format) with `modifier` (0 is
    /// linear). Its planes are added with [`Self::plane`].
    pub fn image(width: u32, height: u32, fourcc: u32, modifier: u64) -> Self {
        Self {
            desc: BufferDesc::image(width, height, fourcc, modifier),
            fds: PhantomData,
        }
    }

    /// A block of bytes: `len` bytes at the start of the dma-buf `fd`.
    pub fn bytes(fd: BorrowedFd<'a>, len: u64) -> Self {
        Self {
            desc: BufferDesc::bytes(fd.as_raw_fd(), len),
            fds: PhantomData,
        }
    }

    /// Adds the next plane: `size` bytes at `offset` in the dma-buf `fd`, `stride` bytes per row
    /// (0 for a plane that is not an image). Planes may share a dma-buf.
    pub fn plane(
        mut self,
        fd: BorrowedFd<'a>,
        offset: u64,
        stride: u64,
        size: u64,
    ) -> Result<Self, HostError> {
        let plane = PlaneDesc {
            fd: fd.as_raw_fd(),
            reserved: 0,
            offset,
            stride,
            size,
        };
        self.desc = self
            .desc
            .with_plane(plane)
            .map_err(|_| HostError::InvalidFrame("more planes than a frame holds"))?;
        Ok(self)
    }

    /// The descriptor the surface reads.
    pub(crate) fn desc(&self) -> &BufferDesc {
        &self.desc
    }
}

/// A frame taken from an output port. It owns one descriptor per plane (duplicated from the
/// surface's frame) and keeps the frame alive; dropping it closes the descriptors and releases the
/// frame.
pub struct FrameLease<'s> {
    surface: &'s Surface,
    handle: Handle,
    desc: BufferDesc,
    fds: [Option<OwnedFd>; MAX_PLANES],
}

impl<'s> FrameLease<'s> {
    /// Takes over a lease the surface made with the descriptors in `desc`: one owned descriptor
    /// per plane in use.
    ///
    /// # Safety
    /// `desc` must be the descriptor a successful take wrote (`BufferDesc::OWNED`), and its
    /// descriptors must not be owned by anything else.
    pub(crate) unsafe fn new(surface: &'s Surface, handle: Handle, desc: BufferDesc) -> Self {
        let mut fds: [Option<OwnedFd>; MAX_PLANES] = [const { None }; MAX_PLANES];
        for (slot, plane) in fds.iter_mut().zip(desc.planes()) {
            // Safety: the caller hands over each plane's descriptor (see the function's notes).
            *slot = Some(unsafe { OwnedFd::from_raw_fd(plane.fd) });
        }
        Self {
            surface,
            handle,
            desc,
            fds,
        }
    }

    /// The width in pixels (0 for a block of bytes).
    pub fn width(&self) -> u32 {
        self.desc.width
    }

    /// The height in pixels.
    pub fn height(&self) -> u32 {
        self.desc.height
    }

    /// The DRM fourcc (0 for a block of bytes).
    pub fn fourcc(&self) -> u32 {
        self.desc.fourcc
    }

    /// The DRM format modifier.
    pub fn modifier(&self) -> u64 {
        self.desc.modifier
    }

    /// The planes: each one's offset, stride and size, and the dma-buf it is in (`fd`, the
    /// lease's own descriptor; use [`Self::plane_fd`] to borrow it as an fd).
    pub fn planes(&self) -> &[PlaneDesc] {
        self.desc.planes()
    }

    /// The dma-buf of plane `index`, borrowed from this lease.
    pub fn plane_fd(&self, index: usize) -> Option<BorrowedFd<'_>> {
        self.fds.get(index)?.as_ref().map(AsFd::as_fd)
    }
}

impl Drop for FrameLease<'_> {
    fn drop(&mut self) {
        // Safety: the handle is this lease's. A failure has nothing to undo: the surface does not
        // hold a lease it cannot find.
        let _ = unsafe { (self.surface.api.frame_release)(self.handle) };
        // The descriptors close as `fds` drops.
    }
}
