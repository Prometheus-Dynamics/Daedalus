//! [`SharedFrame`]: an owned, reference-counted frame seen through `daedalus:frame`.

use core::fmt;

use super::weak::{FrameId, WeakFrame};
use super::{
    FrameColorimetry, FrameFormatKind, FrameInterface, FramePlane, FrameResidency, FrameSource,
    FrameVTable, FrameView,
};
use crate::arc_owner::ArcOps;
use crate::payload::{recycled_incarnation, recycled_ref};
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
///
/// # Identity
///
/// [`Self::id`] names the frame for as long as it lives: an `Arc`-owned frame by its value's
/// address, a recycled one by its node's address and the node's incarnation. [`Self::downgrade`]
/// gives a [`WeakFrame`] that tells whether the frame is still alive, and [`WeakFrame::upgrade`]
/// makes a new `SharedFrame` of it while it is.
///
/// # Same-build access
///
/// [`Self::downcast_ref`] reads the owner as its own type when this build made it (an `Arc` or a
/// recycled node of that type). A frame from another build, or of another type, gives `None`.
#[derive(Clone)]
pub struct SharedFrame {
    pub(super) handle: ForeignHandle,
    /// The handle's vtable, checked against v3 once.
    pub(super) vtable: *const FrameVTable,
    /// Whether this build created the handle (its owner code is ours).
    pub(super) local: bool,
    /// The owner's `Arc` operations, when the owner is an `Arc` of a known type (see
    /// [`Self::arc_ops`]).
    pub(super) arc: Option<&'static ArcOps>,
}

// Safety: the handle is `Send + Sync`, and the vtable is an immutable static of the owner.
unsafe impl Send for SharedFrame {}
// Safety: see above.
unsafe impl Sync for SharedFrame {}

impl SharedFrame {
    /// Share `frame` (one `Arc` move, no copy).
    pub fn new<T: FrameSource>(frame: Arc<T>) -> Self {
        Self::checked(
            ForeignHandle::from_arc::<T, FrameInterface>(frame),
            true,
            Some(ArcOps::of::<T>()),
        )
        .expect("a FrameSource provides this build's daedalus:frame")
    }

    /// Own a frame received as a handle (the handle's reference moves in).
    pub fn from_handle(handle: ForeignHandle) -> Result<Self, ForeignInterfaceMismatch> {
        Self::checked(handle, false, None)
    }

    /// Own a frame this build made as a handle (the handle's reference moves in): a
    /// [`ForeignHandle::from_raw_parts`] over its own [`FrameSource`] with an owner of its own,
    /// such as a recycled object that clears itself when its last holder releases it. It is
    /// [`Self::is_local`], as [`Self::new`]'s is.
    ///
    /// # Safety
    /// The handle's vtable and its owner's retain and release must be this build's code.
    pub unsafe fn from_local_handle(
        handle: ForeignHandle,
    ) -> Result<Self, ForeignInterfaceMismatch> {
        Self::checked(handle, true, None)
    }

    fn checked(
        handle: ForeignHandle,
        local: bool,
        arc: Option<&'static ArcOps>,
    ) -> Result<Self, ForeignInterfaceMismatch> {
        let vtable: *const FrameVTable = handle.view::<FrameInterface>()?.vtable();
        Ok(Self {
            handle,
            vtable,
            local,
            arc,
        })
    }

    /// The frame `payload` carries (a retain of its handle) or lends through its provider (a
    /// handle over the owner's `Arc`, or a recycled node's handle). `None` when the payload is not
    /// foreign-readable, `Err` when it is another interface or frame version.
    pub fn from_payload(payload: &Payload) -> Option<Result<Self, ForeignInterfaceMismatch>> {
        let (handle, local, arc) = match payload.foreign_handle() {
            Some(handle) => (handle.clone(), payload.foreign_handle_is_local(), None),
            None => (payload.to_foreign_handle()?, true, payload.arc_owner_ops()),
        };
        Some(Self::checked(handle, local, arc))
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

    /// The frame's `Arc` operations. Only this build's constructors set them ([`Self::new`] and a
    /// provided payload's [`Self::from_payload`]), each paired with the `Arc<T>` owner it
    /// describes, so the table is trusted; the owner's pointer must still be the value's.
    /// (Comparing the owner's release function by address was not reliable across generic
    /// instances under Miri, so it is not used here; a recycled node is recognised by its
    /// non-generic release, see `recycle`.)
    pub(super) fn arc_ops(&self) -> Option<&'static ArcOps> {
        let ops = self.arc?;
        core::ptr::eq(self.handle.owner().ptr, self.handle.data()).then_some(ops)
    }

    /// The owner as a `T`, when this build made the frame's owner of type `T` (an `Arc<T>` or a
    /// recycled node of `T`, see [`RecyclePool`](crate::RecyclePool)). `None` for a frame from
    /// another build, or of another type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        if let Some(value) = recycled_ref::<T>(&self.handle) {
            return Some(value);
        }
        let ops = self.arc_ops()?;
        if !ops.is::<T>() {
            return None;
        }
        // Safety: the owner is the `Arc` that `ops` describes (checked by `arc_ops`), whose type
        // is `T` (checked above), so its value is a live `T` for the borrow of `self`.
        Some(unsafe { &*self.handle.data().cast::<T>() })
    }

    /// The frame's identity, stable for its life and never reused while a frame or a
    /// [`WeakFrame`] of it lives (see [`FrameId`]).
    pub fn id(&self) -> FrameId {
        match recycled_incarnation(&self.handle) {
            Some((node, generation)) => FrameId::new(node, generation),
            None => FrameId::new(self.handle.data() as usize, 0),
        }
    }

    /// A weak reference to this frame: `None` when the frame's owner is not one this build can
    /// weaken (a frame from another build, or an owner that is not an `Arc` or a recycled node
    /// of a type this build knows).
    pub fn downgrade(&self) -> Option<WeakFrame> {
        WeakFrame::of(self)
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
    /// pushes): the payload holds the handle in place, so nothing is allocated or copied.
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
