//! [`WeakFrame`] and [`FrameId`]: a frame's identity, and a reference to it that does not keep it
//! alive (see [`SharedFrame::downgrade`]).

use core::ffi::c_void;
use core::fmt;

use super::shared::SharedFrame;
use super::{FrameInterface, FrameVTable};
use crate::arc_owner::ArcOps;
use crate::payload::NodeWeak;
use crate::{ForeignHandle, ForeignOwner};

/// A frame's identity: equal for two frames exactly when they are the same frame, and stable for
/// the frame's life. It is never reused while a [`SharedFrame`] or a [`WeakFrame`] of the frame
/// lives: an `Arc`-owned frame is named by its value's address (its allocation stays while any
/// reference to it does), and a recycled one by its node's address and the node's incarnation (a
/// node is taken again only once no reference to the pool's state is left, and each taking makes
/// a new incarnation).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId {
    addr: usize,
    /// `0` for an `Arc`-owned frame, the node's incarnation (at least `1`) for a recycled one.
    generation: u64,
}

impl FrameId {
    pub(super) fn new(addr: usize, generation: u64) -> Self {
        Self { addr, generation }
    }
}

impl fmt::Debug for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameId")
            .field("addr", &(self.addr as *const ()))
            .field("generation", &self.generation)
            .finish()
    }
}

/// A weak reference to a [`SharedFrame`]: it does not keep the frame alive, but it keeps the
/// frame's identity ([`Self::id`]) and tells whether the frame is still [`Self::is_alive`].
/// [`Self::upgrade`] makes a new `SharedFrame` of the frame while it is alive.
pub struct WeakFrame {
    kind: WeakKind,
    vtable: *const FrameVTable,
    local: bool,
    id: FrameId,
}

enum WeakKind {
    /// An `Arc`-owned frame: a `Weak` of the value (its table's operations, by data pointer), and
    /// the owner's functions, copied from the frame's handle, which upgrade hands on.
    Arc {
        weak: *const c_void,
        ops: &'static ArcOps,
        owner: ForeignOwner,
    },
    /// A recycled node's incarnation (see [`NodeWeak`]).
    Recycled(NodeWeak),
}

// Safety: the weak reference is thread-safe as an `Arc`'s `Weak` is (its operations are the
// table's, over `Weak<T>` with `T: Send + Sync`), the owner's functions are thread-safe reference
// counts, and the vtable is an immutable static (as for `SharedFrame`).
unsafe impl Send for WeakFrame {}
// Safety: see `Send`.
unsafe impl Sync for WeakFrame {}

impl WeakFrame {
    /// The weak reference to `frame`, when its owner can be weakened here (see
    /// [`SharedFrame::downgrade`]).
    pub(super) fn of(frame: &SharedFrame) -> Option<Self> {
        let vtable = frame.vtable;
        let local = frame.local;
        if let Some(node) = NodeWeak::of(&frame.handle) {
            let (addr, generation) = node.incarnation();
            return Some(Self {
                kind: WeakKind::Recycled(node),
                vtable,
                local,
                id: FrameId::new(addr, generation),
            });
        }
        let ops = frame.arc_ops()?;
        let owner = frame.handle.owner();
        // Safety: the owner is the `Arc` of `ops` (checked by `arc_ops`), alive while `frame` is.
        let weak = unsafe { ops.downgrade(owner.ptr) };
        Some(Self {
            kind: WeakKind::Arc { weak, ops, owner },
            vtable,
            local,
            id: FrameId::new(frame.handle.data() as usize, 0),
        })
    }

    /// The frame's identity (the same as [`SharedFrame::id`] of the frame it came from).
    pub fn id(&self) -> FrameId {
        self.id
    }

    /// Whether the frame is still alive: some [`SharedFrame`] of it exists (for a recycled
    /// frame, its node still holds this incarnation).
    pub fn is_alive(&self) -> bool {
        match &self.kind {
            // Safety: the weak reference is alive while `self` is.
            WeakKind::Arc { weak, ops, .. } => (unsafe { ops.strong_count(*weak) }) > 0,
            WeakKind::Recycled(node) => node.is_alive(),
        }
    }

    /// A new [`SharedFrame`] of the frame, when it is still alive (`None` once its last frame is
    /// gone). The new frame is a holder of the frame like any clone of it.
    pub fn upgrade(&self) -> Option<SharedFrame> {
        match &self.kind {
            WeakKind::Arc { weak, ops, owner } => {
                // Safety: the weak reference is alive while `self` is.
                let strong = unsafe { ops.upgrade(*weak) }?;
                let owner = ForeignOwner {
                    ptr: strong,
                    retain: owner.retain,
                    release: owner.release,
                };
                // Safety: `strong` is the data pointer of the `Arc` whose strong count the new
                // handle now owns (the owner's release gives it back), and `vtable` is the vtable
                // of this frame's own handle, whose owner is that same `Arc`.
                let handle = unsafe {
                    ForeignHandle::from_raw_parts::<FrameInterface>(strong, &*self.vtable, owner)
                };
                Some(SharedFrame {
                    handle,
                    vtable: self.vtable,
                    local: self.local,
                    arc: Some(ops),
                })
            }
            WeakKind::Recycled(node) => {
                let (data, owner) = node.upgrade()?;
                // Safety: `data` is the node's value, which the new holder keeps alive, and
                // `vtable` is the vtable of this incarnation's frame (the node's type is fixed).
                let handle = unsafe {
                    ForeignHandle::from_raw_parts::<FrameInterface>(data, &*self.vtable, owner)
                };
                Some(SharedFrame {
                    handle,
                    vtable: self.vtable,
                    local: self.local,
                    arc: None,
                })
            }
        }
    }
}

impl Clone for WeakFrame {
    fn clone(&self) -> Self {
        let kind = match &self.kind {
            // Safety: the weak reference is alive while `self` is.
            WeakKind::Arc { weak, ops, owner } => WeakKind::Arc {
                weak: unsafe { ops.clone_weak(*weak) },
                ops,
                owner: *owner,
            },
            WeakKind::Recycled(node) => WeakKind::Recycled(node.clone()),
        };
        Self {
            kind,
            vtable: self.vtable,
            local: self.local,
            id: self.id,
        }
    }
}

impl Drop for WeakFrame {
    fn drop(&mut self) {
        if let WeakKind::Arc { weak, ops, .. } = &self.kind {
            // Safety: this is the one drop of the weak reference `self` holds.
            unsafe { ops.drop_weak(*weak) };
        }
    }
}

impl fmt::Debug for WeakFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WeakFrame")
            .field("id", &self.id)
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}
