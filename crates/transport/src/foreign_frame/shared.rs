//! [`SharedFrame`]: an owned, reference-counted frame seen through `daedalus:frame`.

use core::fmt;

use super::{
    FrameColorimetry, FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource,
    FrameVTable, FrameView,
};
use crate::portable::Arc;
use crate::{
    ForeignHandle, ForeignInterface, ForeignInterfaceMismatch, ForeignRef, ForeignView, Payload,
    Residency,
};

/// An owned frame: a [`ForeignHandle`] checked against `daedalus:frame` v3.
///
/// Use it where a [`FrameView`] (borrowed for one call) is not enough: a node keeps it in its
/// state, returns a frame it received (`-> Result<SharedFrame, _>`), or returns one it made
/// ([`Self::new`]). It works as a `#[node]` input and output on the Rust-ABI and the stable
/// plugin paths alike; read it through [`Self::as_view`].
///
/// # Ownership
///
/// - Cloning retains the frame and dropping releases it, through the owner's own
///   retain/release functions (for a host frame, its `Arc`): the frame's memory stays valid and
///   its metadata unchanged while any clone lives, on either side of a plugin boundary, and the
///   owner's value drops exactly once, when the last clone drops. Nothing is copied.
/// - Frames are immutable: there is no way to write a shared frame's planes. A node that
///   changes pixels builds a new frame ([`Self::new`] over its own [`FrameSource`]); one that
///   only changes metadata wraps the input: a [`FrameSource`] holding the `SharedFrame` that
///   forwards to its `FrameSource` methods and overrides the rest, which keeps the pixels zero
///   copy.
/// - Plane fds ([`FramePlane::dmabuf_fd`](super::FramePlane::dmabuf_fd)) are borrowed from the
///   frame: valid while any clone lives, never closed by the consumer. `dup` one to keep it past
///   that (`ExternalFrameDescriptor::from_frame_view` in `daedalus-gpu` does).
/// - A frame made by a plugin runs the plugin's code on every access and release, so a host
///   that receives one from a separately built plugin keeps that plugin's library loaded for
///   good (as for any foreign handle the plugin created).
#[derive(Clone)]
pub struct SharedFrame {
    handle: ForeignHandle,
    /// The handle's vtable, checked against v3 once.
    vtable: *const FrameVTable,
    /// Whether this build created the handle (its owner code is ours).
    local: bool,
}

// Safety: the handle is `Send + Sync`, and the vtable is an immutable static of the owner.
unsafe impl Send for SharedFrame {}
// Safety: see above.
unsafe impl Sync for SharedFrame {}

impl SharedFrame {
    /// Share `frame` (one `Arc` move, no copy).
    pub fn new<T: FrameSource>(frame: Arc<T>) -> Self {
        Self::checked(ForeignHandle::from_arc::<T, FrameInterface>(frame), true)
            .expect("a FrameSource provides this build's daedalus:frame")
    }

    /// Own a frame received as a handle (the handle's reference moves in).
    pub fn from_handle(handle: ForeignHandle) -> Result<Self, ForeignInterfaceMismatch> {
        Self::checked(handle, false)
    }

    fn checked(handle: ForeignHandle, local: bool) -> Result<Self, ForeignInterfaceMismatch> {
        let vtable: *const FrameVTable = handle.view::<FrameInterface>()?.vtable();
        Ok(Self {
            handle,
            vtable,
            local,
        })
    }

    /// The frame `payload` carries (a retain of its handle) or lends through its provider (a
    /// handle over the owner's `Arc`). `None` when the payload is not foreign-readable, `Err`
    /// when it is another interface or frame version.
    pub fn from_payload(payload: &Payload) -> Option<Result<Self, ForeignInterfaceMismatch>> {
        let (handle, local) = match payload.foreign_handle() {
            Some(handle) => (handle.clone(), payload.foreign_handle_is_local()),
            None => (payload.to_foreign_handle()?, true),
        };
        Some(Self::checked(handle, local))
    }

    /// The frame as a [`FrameView`] (no reference count change).
    pub fn as_view(&self) -> FrameView<'_> {
        // Safety: the vtable was checked against v3 in `checked` and, like the data, stays
        // valid while `self` holds the handle.
        unsafe { ForeignRef::from_checked(self.handle.data(), &*self.vtable) }
    }

    /// The underlying handle.
    pub fn handle(&self) -> &ForeignHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ForeignHandle {
        self.handle
    }

    /// Whether this build created the frame ([`Self::new`], or a host frame lent through its
    /// provider), rather than receiving it from another build.
    pub fn is_local(&self) -> bool {
        self.local
    }

    /// Whether both are the same frame (same owner value), not just equal metadata.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        core::ptr::eq(self.handle.data(), other.handle.data())
    }

    /// The payload residency matching the frame's.
    pub fn payload_residency(&self) -> Residency {
        match self.as_view().residency() {
            FrameResidency::Cpu => Residency::Cpu,
            FrameResidency::Gpu => Residency::Gpu,
            FrameResidency::External => Residency::External,
        }
    }

    /// A payload carrying the frame under `daedalus:frame` (what a node output of this type
    /// pushes): one allocation for the payload storage, no copy.
    pub fn into_payload(self) -> Payload {
        let residency = self.payload_residency();
        let key = <FrameInterface as ForeignInterface>::KEY;
        if self.local {
            Payload::foreign_local(key, self.handle, residency)
        } else {
            Payload::foreign(key, self.handle, residency)
        }
    }
}

/// Forwards to the frame's own accessors, so a source wrapping a `SharedFrame` delegates what it
/// does not change. The caller's `plane_data` / `end_cpu_access` pairing passes through.
impl FrameSource for SharedFrame {
    fn width(&self) -> u32 {
        self.as_view().width()
    }
    fn height(&self) -> u32 {
        self.as_view().height()
    }
    fn format(&self) -> u32 {
        self.as_view().format()
    }
    fn format_kind(&self) -> FrameFormatKind {
        self.as_view().format_kind()
    }
    fn modifier(&self) -> u64 {
        self.as_view().modifier()
    }
    fn timestamp_ns(&self) -> u64 {
        self.as_view().timestamp_ns()
    }
    fn sequence(&self) -> u64 {
        self.as_view().sequence()
    }
    fn residency(&self) -> FrameResidency {
        self.as_view().residency()
    }
    fn plane_count(&self) -> u32 {
        self.as_view().plane_count()
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        self.as_view().plane(index)
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let view = self.as_view();
        if index >= view.plane_count() {
            return None;
        }
        let mut len = 0u64;
        // Safety: a checked v3 vtable with its data pointer, `index` in range.
        let ptr = unsafe { (view.vtable().plane_data)(view.data(), index, &mut len) };
        // Safety: a non-null result maps `len` bytes, valid and unchanged until the matching
        // `end_cpu_access`, which the `FrameSource` contract obliges our caller to make (and
        // which we forward).
        (!ptr.is_null()).then(|| unsafe { core::slice::from_raw_parts(ptr, len as usize) })
    }
    fn end_cpu_access(&self, index: u32) {
        let view = self.as_view();
        // Safety: ends an access `plane_data` began (the caller's contract).
        unsafe { (view.vtable().plane_end_cpu_access)(view.data(), index) }
    }
    fn colorimetry(&self) -> FrameColorimetry {
        self.as_view().colorimetry()
    }
    fn metadata_u64(&self, key: u32) -> Option<u64> {
        self.as_view().metadata_u64(key)
    }
    fn metadata_bytes(&self, key: u32) -> Option<&[u8]> {
        self.as_view().metadata_bytes(key)
    }
}

impl<'a> ForeignView<'a> for SharedFrame {
    type Interface = FrameInterface;

    fn from_payload(payload: &'a Payload) -> Option<Result<Self, ForeignInterfaceMismatch>> {
        SharedFrame::from_payload(payload)
    }
}

impl From<SharedFrame> for Payload {
    fn from(frame: SharedFrame) -> Self {
        frame.into_payload()
    }
}

impl fmt::Debug for SharedFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let view = self.as_view();
        f.debug_struct("SharedFrame")
            .field("width", &view.width())
            .field("height", &view.height())
            .field("format", &view.format())
            .field("sequence", &view.sequence())
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}
