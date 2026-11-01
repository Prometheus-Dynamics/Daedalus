//! Camera frames (`SharedFrame`, the payload of an External-residency port: a Styx `FrameLease`)
//! as storage buffers, imported once per dmabuf and reused across frames.
//!
//! A camera hands out the same few pooled dmabufs over and over. [`GpuFrameImports`] keys each
//! import by the dmabuf's inode, which the kernel numbers uniquely for the dmabuf's whole life,
//! so a buffer seen again maps to its existing import: no new `vkAllocateMemory`, no copy.

use std::collections::HashMap;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use daedalus_transport::SharedFrame;
use parking_lot::Mutex;

use crate::external::{DmabufAccess, export_dmabuf_fence};
use crate::{ExternalImportError, GpuContextHandle};

use super::GpuStorage;

/// A frame imported as a storage buffer for one use: the buffer (shared with every other frame
/// in the same dmabuf), each plane's byte offset and stride in it, and the frame itself, held so
/// the producer cannot reuse the dmabuf while this lives. Keep it until the GPU work reading the
/// frame has completed.
pub struct GpuFrameInput {
    pub storage: Arc<GpuStorage>,
    /// `(offset, stride)` per plane, inline (no allocation per frame).
    planes: [(u64, u64); MAX_PLANES],
    plane_count: usize,
    _frame: SharedFrame,
}

/// Planes a frame may have (`daedalus:frame` allows four).
const MAX_PLANES: usize = 4;

impl GpuFrameInput {
    /// `(offset, stride)` of each plane in [`GpuFrameInput::storage`].
    pub fn planes(&self) -> &[(u64, u64)] {
        &self.planes[..self.plane_count]
    }
}

/// What the import cache did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GpuFrameImportStats {
    /// Frames served by an existing import.
    pub hits: u64,
    /// dmabufs imported.
    pub imports: u64,
    /// Imports dropped to stay within the capacity.
    pub evictions: u64,
}

/// Imports camera frames as storage buffers, once per dmabuf (see the module docs). Holds up to
/// `capacity` imports, dropping the least recently used.
pub struct GpuFrameImports {
    gpu: GpuContextHandle,
    capacity: usize,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// (st_dev, st_ino) to (import, last use).
    entries: HashMap<(u64, u64), (Arc<GpuStorage>, u64)>,
    clock: u64,
    stats: GpuFrameImportStats,
}

impl GpuFrameImports {
    /// A cache for `gpu`'s device keeping up to `capacity` dmabuf imports (a camera's pool size
    /// is enough).
    pub fn new(gpu: &GpuContextHandle, capacity: usize) -> Self {
        Self {
            gpu: gpu.clone(),
            capacity: capacity.max(1),
            state: Mutex::new(State::default()),
        }
    }

    /// The frame as a storage buffer. Every plane must be in the same dmabuf (one fd). Before
    /// returning, waits on the CPU (up to `timeout`) for the dmabuf's pending writes, through
    /// its implicit fence (`DMA_BUF_IOCTL_EXPORT_SYNC_FILE`); a producer that wrote the frame
    /// with the CPU must have ended its access (`DMA_BUF_IOCTL_SYNC` with `SYNC_END`) first.
    ///
    /// The import covers the dmabuf from offset 0 to the end of the last plane (rounded up to 4);
    /// the dmabuf needs the driver's buffer padding beyond that (256 B on v3dv; Styx pads), a
    /// Vulkan rule rather than a choice: [`GpuFrameImports::required_len`] gives the size.
    pub fn import(
        &self,
        frame: &SharedFrame,
        timeout: Duration,
    ) -> Result<GpuFrameInput, ExternalImportError> {
        let view = frame.as_view();
        // Planes may carry different fds (dups) of one dmabuf, as Styx's NV12 does: one buffer
        // means one inode, whatever the fd numbers.
        let mut first: Option<(i32, (u64, u64))> = None;
        let mut planes = [(0u64, 0u64); MAX_PLANES];
        let mut plane_count = 0;
        let mut end = 0u64;
        for (i, plane) in view.planes().enumerate() {
            let plane_fd = plane.dmabuf_fd.ok_or_else(|| {
                ExternalImportError::invalid(format!("plane {i} has no dma-buf fd"))
            })?;
            // SAFETY: the frame keeps its fds open while `frame` is borrowed.
            let id = inode(unsafe { BorrowedFd::borrow_raw(plane_fd) })?;
            match first {
                None => first = Some((plane_fd, id)),
                Some((_, first_id)) if first_id != id => {
                    return Err(ExternalImportError::invalid(
                        "the planes are in different dma-bufs; import each plane's dma-buf separately",
                    ));
                }
                Some(_) => {}
            }
            if plane_count == MAX_PLANES {
                return Err(ExternalImportError::invalid(format!(
                    "more than {MAX_PLANES} planes"
                )));
            }
            planes[plane_count] = (plane.offset, plane.stride);
            plane_count += 1;
            end = end.max(plane.offset + plane.len);
        }
        let (fd, key) =
            first.ok_or_else(|| ExternalImportError::invalid("the frame has no planes"))?;
        // SAFETY: as above.
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        wait_writes(fd, timeout)?;
        let len = end.next_multiple_of(4);
        let storage = {
            let mut guard = self.state.lock();
            let state = &mut *guard;
            state.clock += 1;
            let now = state.clock;
            match state.entries.get_mut(&key) {
                Some((storage, last)) if storage.size() >= len => {
                    *last = now;
                    state.stats.hits += 1;
                    Some(storage.clone())
                }
                _ => None,
            }
        };
        let storage = match storage {
            Some(storage) => storage,
            None => {
                let owned: OwnedFd =
                    fd.try_clone_to_owned()
                        .map_err(|e| ExternalImportError::ImportFailed {
                            reason: format!("dup of the frame's dma-buf: {e}"),
                        })?;
                let storage = Arc::new(GpuStorage::import_dmabuf(
                    &self.gpu,
                    owned,
                    0,
                    len,
                    None,
                    timeout,
                    Some("daedalus-frame"),
                )?);
                let mut state = self.state.lock();
                if state.entries.len() >= self.capacity
                    && let Some(oldest) = state
                        .entries
                        .iter()
                        .min_by_key(|(_, (_, last))| *last)
                        .map(|(k, _)| *k)
                {
                    state.entries.remove(&oldest);
                    state.stats.evictions += 1;
                }
                let now = state.clock;
                state.entries.insert(key, (storage.clone(), now));
                state.stats.imports += 1;
                storage
            }
        };
        Ok(GpuFrameInput {
            storage,
            planes,
            plane_count,
            _frame: frame.clone(),
        })
    }

    /// The dmabuf bytes a frame whose last plane ends at byte `end` needs to import (see
    /// [`GpuStorage::dmabuf_import_len`]).
    pub fn required_len(&self, end: u64) -> Result<u64, ExternalImportError> {
        GpuStorage::dmabuf_import_len(&self.gpu, end.next_multiple_of(4))
    }

    pub fn stats(&self) -> GpuFrameImportStats {
        self.state.lock().stats
    }

    /// Drop every import (the dmabufs are released once no frame input still holds them).
    pub fn clear(&self) {
        self.state.lock().entries.clear();
    }
}

/// The dmabuf's identity: its inode is unique for its whole life.
fn inode(fd: BorrowedFd<'_>) -> Result<(u64, u64), ExternalImportError> {
    // SAFETY: an all-zero `stat` is valid; fstat fills it.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: valid fd and out pointer.
    if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
        return Err(ExternalImportError::invalid(format!(
            "fstat of the dma-buf: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok((st.st_dev as u64, st.st_ino as u64))
}

/// Wait for the dmabuf's pending writes (its implicit fence). A kernel without
/// `DMA_BUF_IOCTL_EXPORT_SYNC_FILE` (before 6.0) has nothing to wait on here.
fn wait_writes(fd: BorrowedFd<'_>, timeout: Duration) -> Result<(), ExternalImportError> {
    match export_dmabuf_fence(fd, DmabufAccess::Read) {
        Ok(fence) => crate::external::wait_sync_file(std::os::fd::AsFd::as_fd(&fence), timeout),
        Err(_) => Ok(()),
    }
}
