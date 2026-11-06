//! Dma-buf frames on Linux: the surface's [`FrameObject`] (a `FrameSource` over the caller's
//! dma-bufs), and the push and take that move one across the surface with no copy.
//!
//! A pushed frame holds one dma-buf per plane, each its own descriptor, so the pixels stay where
//! the caller put them. Its CPU bytes are mapped on first use and bracketed by dma-buf sync, as
//! the `daedalus:frame` contract requires. A taken frame is a lease: the surface keeps one
//! reference until the caller releases it, and hands out duplicated descriptors the caller owns.
//! Warm frames allocate no frame object: the port's last object is reused once the graph has let
//! go of it.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use daedalus::transport::{
    FrameFormatKind, FramePlane, FrameResidency, FrameSource, PlaneMapping, SharedFrame,
};
use lemnos_abi::{BufferDesc, FenceDesc, Handle, MAX_PLANES, PlaneDesc, Status};
use lemnos_linux::dma::{Access, DmaBuf, Mapping, SyncFence};

use crate::ffi;
use crate::io::feed;
use crate::state::{Kind, Tables, lock};

/// A frame the surface holds: its metadata, one dma-buf per plane, and each plane's lazily mapped
/// bytes. Immutable once built; a push that reuses the object overwrites it whole.
pub struct FrameObject {
    width: u32,
    height: u32,
    fourcc: u32,
    modifier: u64,
    /// Planes in use.
    count: usize,
    /// Plane `i`'s offset, stride and size (its `fd` is not used: `bufs[i]` holds it).
    planes: [PlaneDesc; MAX_PLANES],
    /// Plane `i`'s dma-buf, a descriptor the object owns.
    bufs: [Option<DmaBuf>; MAX_PLANES],
    /// Plane `i`'s mapping of its dma-buf, made on the first CPU access and kept until drop.
    maps: [OnceLock<Option<Mapping>>; MAX_PLANES],
    /// Plane `i`'s open CPU accesses: the first syncs its dma-buf for the CPU, the last ends that.
    readers: Mutex<[u32; MAX_PLANES]>,
}

impl FrameObject {
    /// A frame over `desc`'s planes, with `bufs[i]` the dma-buf of plane `i`. `desc` has been
    /// validated, so its plane count is at most [`MAX_PLANES`].
    fn new(desc: &BufferDesc, bufs: [Option<DmaBuf>; MAX_PLANES]) -> Self {
        Self {
            width: desc.width,
            height: desc.height,
            fourcc: desc.fourcc,
            modifier: desc.modifier,
            count: (desc.plane_count as usize).min(MAX_PLANES),
            planes: desc.planes,
            bufs,
            maps: [const { OnceLock::new() }; MAX_PLANES],
            readers: Mutex::new([0; MAX_PLANES]),
        }
    }

    /// The index of plane `index` when it is in use.
    fn plane_index(&self, index: u32) -> Option<usize> {
        usize::try_from(index).ok().filter(|&i| i < self.count)
    }
}

impl FrameSource for FrameObject {
    fn width(&self) -> u32 {
        self.width
    }

    fn height(&self) -> u32 {
        self.height
    }

    fn format(&self) -> u32 {
        self.fourcc
    }

    fn format_kind(&self) -> FrameFormatKind {
        if self.fourcc == 0 {
            FrameFormatKind::Unknown
        } else {
            FrameFormatKind::Pixel
        }
    }

    fn modifier(&self) -> u64 {
        self.modifier
    }

    fn residency(&self) -> FrameResidency {
        FrameResidency::External
    }

    fn plane_count(&self) -> u32 {
        self.count as u32
    }

    /// Metadata only: the plane's dma-buf and layout, no mapping and no sync.
    fn plane(&self, index: u32) -> Option<FramePlane> {
        let i = self.plane_index(index)?;
        let fd = self.bufs[i].as_ref()?.as_fd().as_raw_fd();
        let plane = self.planes[i];
        Some(
            FramePlane::dmabuf(fd, plane.offset, plane.stride, plane.size)
                .with_mapping(PlaneMapping::Cached),
        )
    }

    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let i = self.plane_index(index)?;
        let buf = self.bufs[i].as_ref()?;
        let plane = self.planes[i];
        let start = usize::try_from(plane.offset).ok()?;
        let end = start.checked_add(usize::try_from(plane.size).ok()?)?;
        let map = self.maps[i]
            .get_or_init(|| buf.map_read_only().ok())
            .as_ref()?;
        let bytes = map.as_slice().get(start..end)?;
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        if readers[i] == 0 && buf.begin_cpu_access(Access::Read).is_err() {
            return None;
        }
        readers[i] = readers[i].saturating_add(1);
        Some(bytes)
    }

    fn end_cpu_access(&self, index: u32) {
        let Some(i) = self.plane_index(index) else {
            return;
        };
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        if readers[i] == 0 {
            return;
        }
        readers[i] -= 1;
        if readers[i] == 0
            && let Some(buf) = &self.bufs[i]
        {
            // The access is over whether or not the kernel's sync call succeeds.
            let _ = buf.end_cpu_access(Access::Read);
        }
    }
}

/// A test hook: makes the next push or take on this thread panic once its work has started, so
/// the cleanup a panic must do is exercised. Only in test builds.
#[cfg(test)]
pub(crate) fn arm_panic() {
    PANIC_NEXT.with(|flag| flag.set(true));
}

#[cfg(test)]
thread_local! {
    static PANIC_NEXT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The point where the armed test panic fires (a no-op outside tests).
#[cfg(test)]
fn fault_point() {
    if PANIC_NEXT.with(|flag| flag.replace(false)) {
        panic!("injected frame panic");
    }
}

#[cfg(not(test))]
#[inline(always)]
fn fault_point() {}

/// A descriptor for `raw`: taken over when `owned` (the caller gave it up), otherwise a dup of
/// the caller's, which stays theirs.
fn fd_of(raw: i32, owned: bool) -> Result<OwnedFd, Status> {
    if owned {
        // Safety: under `BufferDesc::OWNED` the caller hands this descriptor to the surface.
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    } else {
        // Safety: the caller lends the descriptor for the call.
        unsafe { BorrowedFd::borrow_raw(raw) }
            .try_clone_to_owned()
            .map_err(|error| Status::from_io_error(&error))
    }
}

/// One owned descriptor per plane of `desc` (`None` for an unused plane). Planes that share an fd
/// share the caller's one: the first plane takes it (or dups it), and each later one dups that.
/// Under `OWNED`, every descriptor the caller gave up is taken before anything can fail, so an
/// error drops them all, closing them.
fn plane_fds(desc: &BufferDesc, owned: bool) -> Result<[Option<OwnedFd>; MAX_PLANES], Status> {
    let count = (desc.plane_count as usize).min(MAX_PLANES);
    let raw = |i: usize| desc.planes[i].fd;
    let mut fds: [Option<OwnedFd>; MAX_PLANES] = [const { None }; MAX_PLANES];
    for (i, slot) in fds.iter_mut().enumerate().take(count) {
        if raw(i) < 0 || (0..i).any(|j| raw(j) == raw(i)) {
            continue;
        }
        *slot = Some(fd_of(raw(i), owned)?);
    }
    for i in 0..count {
        if raw(i) < 0 || fds[i].is_some() {
            continue;
        }
        let first = (0..i)
            .find(|&j| raw(j) == raw(i))
            .ok_or(Status::INVALID_ARGUMENT)?;
        let original = fds[first].as_ref().ok_or(Status::INVALID_ARGUMENT)?;
        fds[i] = Some(
            original
                .try_clone()
                .map_err(|error| Status::from_io_error(&error))?,
        );
    }
    Ok(fds)
}

/// The fence's descriptor when it has one (taken or dup'ed as the planes are).
fn fence_fd(fence: Option<FenceDesc>, owned: bool) -> Result<Option<OwnedFd>, Status> {
    match fence {
        Some(fence) if fence.fd >= 0 => fd_of(fence.fd, owned).map(Some),
        _ => Ok(None),
    }
}

/// Pushes a dma-buf frame into input `port`. See [`crate::frames::daedalus_surface_push_frame`].
///
/// # Safety
/// As the entry point's contract.
pub(super) unsafe fn push(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    desc: *const BufferDesc,
    fence: *const FenceDesc,
) -> Result<(), Status> {
    // Safety: a valid descriptor, per the caller's contract. Read first, so an OWNED descriptor
    // is taken over before any check can fail.
    let desc = unsafe { ffi::value(desc) }?;
    let fence = if fence.is_null() {
        None
    } else {
        // Safety: a non-null fence is valid for a read, per the caller's contract.
        Some(unsafe { ffi::value(fence) }?)
    };
    let owned = desc.flags & BufferDesc::OWNED != 0;
    let fence_fd = fence_fd(fence, owned)?;
    let mut fds = plane_fds(&desc, owned)?;

    // Safety: the port name, valid for `port_len` bytes per the caller's contract.
    let name = unsafe { ffi::text(port, port_len) }?;
    let mut guard = lock();
    let graph = guard.graphs.get_mut(graph)?;
    let index = graph.input(name, Kind::Frame)?;
    validated(desc.validate())?;
    if let Some(fence) = fence {
        validated(fence.validate())?;
        if fence.kind == FenceDesc::SYNCOBJ {
            // A syncobj point cannot be attached to a dma-buf through Lemnos: refused, not ignored.
            return Err(Status::NOT_SUPPORTED);
        }
    }

    let count = (desc.plane_count as usize).min(MAX_PLANES);
    let mut bufs: [Option<DmaBuf>; MAX_PLANES] = [const { None }; MAX_PLANES];
    for (i, slot) in bufs.iter_mut().enumerate().take(count) {
        let fd = fds[i].take().ok_or(Status::INVALID_ARGUMENT)?;
        let buf = DmaBuf::from_fd(fd).map_err(|error| Status::from_io_error(&error))?;
        let plane = desc.planes[i];
        let fits = plane
            .offset
            .checked_add(plane.size)
            .is_some_and(|end| end <= buf.len() as u64);
        if !fits {
            return Err(Status::INVALID_ARGUMENT);
        }
        *slot = Some(buf);
    }

    // The acquire fence: a write fence on each dma-buf, which readers wait for. The sync-file is
    // the caller's to keep (or ours, under OWNED) and is closed after the import.
    if let (Some(fence), Some(fd)) = (fence, fence_fd)
        && fence.kind == FenceDesc::SYNC_FILE
    {
        let sync = SyncFence::from_fd(fd);
        for buf in bufs.iter().flatten() {
            buf.import_fence(Access::Write, &sync)
                .map_err(|error| match error.kind() {
                    io::ErrorKind::Unsupported => Status::NOT_SUPPORTED,
                    _ => Status::from_io_error(&error),
                })?;
        }
    }

    fault_point();

    // Reuse the port's last object when the graph has let go of it; otherwise a new one.
    let object = match graph.frame_spare[index].take() {
        Some(mut spare) => match Arc::get_mut(&mut spare) {
            Some(slot) => {
                *slot = FrameObject::new(&desc, bufs);
                spare
            }
            None => Arc::new(FrameObject::new(&desc, bufs)),
        },
        None => Arc::new(FrameObject::new(&desc, bufs)),
    };
    let frame = SharedFrame::new(Arc::clone(&object));
    graph.frame_spare[index] = Some(object);
    let id = graph.inputs[index].id.clone();
    feed(graph.graph.push_payload(id, frame.into_payload()))
}

/// Takes the next frame from output `port` as a lease, with its descriptors in `desc`. See
/// [`crate::frames::daedalus_surface_take_frame`].
///
/// # Safety
/// As the entry point's contract.
pub(super) unsafe fn take(
    graph: Handle,
    port: *const u8,
    port_len: usize,
    desc: *mut BufferDesc,
    fence: *mut FenceDesc,
    frame: *mut Handle,
) -> Result<(), Status> {
    // Safety: the port name, valid for `port_len` bytes per the caller's contract.
    let name = unsafe { ffi::text(port, port_len) }?;
    if desc.is_null() || fence.is_null() || frame.is_null() {
        return Err(Status::INVALID_ARGUMENT);
    }
    let mut guard = lock();
    let Tables { graphs, frames, .. } = &mut *guard;
    let graph = graphs.get_mut(graph)?;
    let index = graph.output(name, Kind::Frame)?;
    let shared = match graph.frame_stash.iter().position(|(i, _)| *i == index) {
        Some(at) => graph.frame_stash.swap_remove(at).1,
        None => {
            let payload = graph.graph.take_payload(name).ok_or(Status::AGAIN)?;
            match SharedFrame::from_payload(&payload) {
                Some(Ok(shared)) => shared,
                Some(Err(_)) => return Err(Status::NOT_SUPPORTED),
                None => return Err(Status::INVALID_ARGUMENT),
            }
        }
    };
    let (fds, out) = match export(&shared) {
        Ok(exported) => exported,
        Err(status) => {
            // Not this call's to lose: the frame comes back first on the next take.
            graph.frame_stash.push((index, shared));
            return Err(status);
        }
    };

    fault_point();

    let lease = frames.insert(shared)?;
    // Every fallible step is past: the caller owns the descriptors now.
    for fd in fds.into_iter().flatten() {
        let _ = fd.into_raw_fd();
    }
    // Safety: the three pointers are non-null (checked above) and valid for writes.
    unsafe {
        desc.write_unaligned(out);
        fence.write_unaligned(FenceDesc::none());
        frame.write_unaligned(lease);
    }
    Ok(())
}

/// The descriptors of `frame`'s planes, each a dup the caller will own, and the descriptor that
/// describes them. The dups are returned owned, so an error drops (closes) them.
fn export(frame: &SharedFrame) -> Result<([Option<OwnedFd>; MAX_PLANES], BufferDesc), Status> {
    let view = frame.as_view();
    let count = view.plane_count() as usize;
    if count == 0 || count > MAX_PLANES {
        return Err(Status::NOT_SUPPORTED);
    }
    let mut out = BufferDesc::image(view.width(), view.height(), view.format(), view.modifier());
    let mut fds: [Option<OwnedFd>; MAX_PLANES] = [const { None }; MAX_PLANES];
    for (i, slot) in fds.iter_mut().enumerate().take(count) {
        let plane = view.plane(i as u32).ok_or(Status::NOT_SUPPORTED)?;
        // A plane with no dma-buf (CPU memory) has no descriptor to hand out.
        let raw = plane.dmabuf_fd.ok_or(Status::NOT_SUPPORTED)?;
        let fd = fd_of(raw, false)?;
        out = out
            .with_plane(PlaneDesc {
                fd: fd.as_raw_fd(),
                reserved: 0,
                offset: plane.offset,
                stride: plane.stride,
                size: plane.len,
            })
            .map_err(|_| Status::NOT_SUPPORTED)?;
        *slot = Some(fd);
    }
    out.flags = BufferDesc::OWNED;
    Ok((fds, out))
}

/// A `lemnos-abi` validation status as a result.
fn validated(status: Status) -> Result<(), Status> {
    if status.is_ok() { Ok(()) } else { Err(status) }
}
