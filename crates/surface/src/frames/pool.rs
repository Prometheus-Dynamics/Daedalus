//! A port's recycled frame objects: a pushed frame reuses an object the graph has let go of, so a
//! warm push allocates nothing, and an object lets go of its dma-bufs as soon as the last holder
//! of its frame does, so the surface never holds a caller's buffer past the graph.
//!
//! Each object lives in a [`Shell`]: the [`FrameObject`] and a count of the frame handles alive.
//! A frame's handle is built over the shell with the surface's own retain and release
//! ([`ForeignOwner`]): a retain counts one more holder (and one more `Arc` reference), a release
//! one fewer, and the release that drops the count to zero clears the object (its dma-bufs close
//! and its mappings go) before giving up its `Arc` reference. The pool keeps one `Arc` per shell;
//! a push reuses a shell only when that is the last one (`Arc::get_mut`), so it never touches an
//! object a frame can still reach.

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::transport::{
    ForeignHandle, ForeignOwner, FrameInterface, ProvideForeign, SharedFrame,
};

use super::dma::FrameObject;

/// Objects a port keeps for reuse: frames a caller can hold at once (taken and not released, or
/// queued in the graph) before a push allocates a new object. Past it, an object is still made
/// for the push but not kept.
const KEPT: usize = 4;

/// One recycled frame object and the number of frame handles over it.
struct Shell {
    /// Handles alive over [`Self::frame`]: zero while it sits in the pool, cleared.
    holders: AtomicUsize,
    /// Written only through `Arc::get_mut` (no handle exists) or by the release that took
    /// `holders` to zero (no handle is left, and its `Arc` reference keeps the pool out).
    frame: UnsafeCell<FrameObject>,
}

// Safety: the frame is read through shared references only while a handle is alive, and written
// only when none is (see `frame`); `FrameObject` itself is `Send + Sync`.
unsafe impl Send for Shell {}
// Safety: see above.
unsafe impl Sync for Shell {}

/// One more holder of the shell at `ptr`.
unsafe extern "C" fn retain(ptr: *const c_void) {
    // Safety: `ptr` is a shell `Arc` a live handle holds a reference of.
    unsafe {
        (*ptr.cast::<Shell>())
            .holders
            .fetch_add(1, Ordering::Relaxed);
        Arc::increment_strong_count(ptr.cast::<Shell>());
    }
}

/// One fewer: the last holder clears the frame, then every holder gives up its reference.
unsafe extern "C" fn release(ptr: *const c_void) {
    let shell = ptr.cast::<Shell>();
    // Safety: the releasing handle holds a reference, so the shell is alive. The holder that
    // takes the count to zero is the last one: no reference into the frame is left, and its own
    // `Arc` reference keeps a push from reusing the shell until the frame is cleared.
    unsafe {
        if (*shell).holders.fetch_sub(1, Ordering::AcqRel) == 1 {
            (*(*shell).frame.get()).clear();
        }
        Arc::decrement_strong_count(shell);
    }
}

/// A port's frame objects (see the module docs).
#[derive(Default)]
pub struct FramePool {
    shells: Vec<Arc<Shell>>,
}

impl FramePool {
    /// A frame over `frame`, in a free kept object when there is one, else in a new object (kept
    /// while fewer than [`KEPT`] are).
    pub(super) fn share(&mut self, frame: FrameObject) -> SharedFrame {
        let free = self
            .shells
            .iter_mut()
            .position(|shell| Arc::get_mut(shell).is_some());
        let shell = match free {
            Some(at) => {
                // The pool's is the only reference: no handle reaches the (cleared) frame.
                let slot = Arc::get_mut(&mut self.shells[at]).expect("found free above");
                *slot.frame.get_mut() = frame;
                Arc::clone(&self.shells[at])
            }
            None => {
                let shell = Arc::new(Shell {
                    holders: AtomicUsize::new(0),
                    frame: UnsafeCell::new(frame),
                });
                if self.shells.len() < KEPT {
                    self.shells.reserve_exact(KEPT);
                    self.shells.push(Arc::clone(&shell));
                }
                shell
            }
        };
        handle(shell)
    }

    /// Objects kept for reuse (tests).
    #[cfg(test)]
    pub(crate) fn kept(&self) -> usize {
        self.shells.len()
    }

    /// Whether `data` is one of this pool's frame objects (tests: a frame's identity).
    #[cfg(test)]
    pub(crate) fn holds(&self, data: *const c_void) -> bool {
        self.shells
            .iter()
            .any(|shell| std::ptr::eq(shell.frame.get().cast_const().cast(), data))
    }
}

/// The frame of `shell`, as its first holder.
fn handle(shell: Arc<Shell>) -> SharedFrame {
    shell.holders.store(1, Ordering::Relaxed);
    let data: *const c_void = shell.frame.get().cast_const().cast();
    let owner = ForeignOwner {
        ptr: Arc::into_raw(shell).cast(),
        retain,
        release,
    };
    // Safety: `data` is the shell's frame, which stays valid while `owner` holds the shell, and
    // the vtable is `FrameObject`'s own provider of `daedalus:frame`.
    let handle = unsafe {
        ForeignHandle::from_raw_parts::<FrameInterface>(
            data,
            <FrameObject as ProvideForeign<FrameInterface>>::vtable(),
            owner,
        )
    };
    // Safety: the vtable and the owner's functions are this build's code.
    unsafe { SharedFrame::from_local_handle(handle) }
        .expect("FrameObject provides this build's daedalus:frame")
}
