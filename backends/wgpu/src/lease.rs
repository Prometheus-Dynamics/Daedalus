//! An optional lease on the device, for a GPU that several processes share.
//!
//! A host that has to take turns on the GPU gives the backend a [`GpuLease`]. The backend asks
//! for the lease before it gives the device work, and gives it back when the last of that work
//! has finished. It knows nothing about who else wants the GPU or how the turn is decided: that
//! is the host's. With no lease set, a submit pays one load of a flag.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// A turn on the GPU, asked for by the backend and decided by the host.
///
/// The calls are paired: one `acquire`, then one `release`, however many submissions ran in
/// between. Both are made on whichever thread submits or sees the work finish, so they must be
/// quick and must not call back into the backend. An `acquire` that waits for another process
/// holds up the submit that asked.
pub trait GpuLease: Send + Sync {
    /// The backend is about to give the device work, and had none in flight.
    fn acquire(&self);
    /// The last of the work given since `acquire` has finished.
    fn release(&self);
}

#[derive(Default)]
struct Inner {
    hook: Option<Arc<dyn GpuLease>>,
    /// Submissions that have been given to the device under the hook and haven't finished.
    users: usize,
}

/// One device's lease: the host's hook, and how much work is out under it.
#[derive(Default)]
pub(crate) struct LeaseState {
    /// Whether a hook is set. Read before the lock, so a device with no lease never takes it.
    on: AtomicBool,
    inner: Mutex<Inner>,
}

impl LeaseState {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets the hook, or takes it away. Work in flight under the old hook is released there and
    /// acquired under the new one, so each hook's calls stay paired.
    pub(crate) fn set(&self, hook: Option<Arc<dyn GpuLease>>) {
        let mut inner = self.inner();
        if inner.users > 0 {
            if let Some(old) = &inner.hook {
                old.release();
            }
            if let Some(new) = &hook {
                new.acquire();
            }
        }
        self.on.store(hook.is_some(), Ordering::Release);
        inner.hook = hook;
    }

    /// Counts one submission in, asking for the lease if it is the first. `None` with no hook.
    pub(crate) fn begin(self: &Arc<Self>) -> Option<LeaseUse> {
        if !self.on.load(Ordering::Acquire) {
            return None;
        }
        let mut inner = self.inner();
        let hook = inner.hook.as_ref()?;
        if inner.users == 0 {
            hook.acquire();
        }
        inner.users += 1;
        Some(LeaseUse(Arc::clone(self)))
    }

    fn end(&self) {
        let mut inner = self.inner();
        if inner.users == 0 {
            return;
        }
        inner.users -= 1;
        if inner.users == 0
            && let Some(hook) = &inner.hook
        {
            hook.release();
        }
    }
}

/// One submission's part of the lease. Dropped when the device has finished the submission, or
/// when the submission is abandoned.
pub(crate) struct LeaseUse(Arc<LeaseState>);

impl Drop for LeaseUse {
    fn drop(&mut self) {
        self.0.end();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{GpuLease, LeaseState};

    #[derive(Default)]
    struct Counted {
        acquired: AtomicUsize,
        released: AtomicUsize,
    }

    impl Counted {
        fn calls(&self) -> (usize, usize) {
            (
                self.acquired.load(Ordering::Acquire),
                self.released.load(Ordering::Acquire),
            )
        }
    }

    impl GpuLease for Counted {
        fn acquire(&self) {
            self.acquired.fetch_add(1, Ordering::AcqRel);
        }
        fn release(&self) {
            self.released.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn with_no_hook_nothing_is_counted() {
        let state = Arc::new(LeaseState::default());
        assert!(state.begin().is_none());
    }

    #[test]
    fn overlapping_work_is_one_acquire_and_one_release() {
        let state = Arc::new(LeaseState::default());
        let hook = Arc::new(Counted::default());
        state.set(Some(hook.clone()));
        let first = state.begin().expect("leased");
        assert_eq!(hook.calls(), (1, 0));
        let second = state.begin().expect("leased");
        assert_eq!(hook.calls(), (1, 0), "work was already out");
        drop(first);
        assert_eq!(hook.calls(), (1, 0), "work is still out");
        drop(second);
        assert_eq!(hook.calls(), (1, 1));
        // The next work asks again.
        drop(state.begin());
        assert_eq!(hook.calls(), (2, 2));
    }

    #[test]
    fn changing_the_hook_with_work_out_keeps_each_hooks_calls_paired() {
        let state = Arc::new(LeaseState::default());
        let (old, new) = (Arc::new(Counted::default()), Arc::new(Counted::default()));
        state.set(Some(old.clone()));
        let out = state.begin().expect("leased");
        state.set(Some(new.clone()));
        assert_eq!(old.calls(), (1, 1), "the old hook got its release");
        assert_eq!(
            new.calls(),
            (1, 0),
            "the new hook holds the work that is out"
        );
        drop(out);
        assert_eq!(new.calls(), (1, 1));
        // Taken away with work out: released there, and nothing is asked afterwards.
        let out = state.begin().expect("leased");
        state.set(None);
        assert_eq!(new.calls(), (2, 2));
        drop(out);
        assert!(state.begin().is_none());
        assert_eq!(new.calls(), (2, 2));
    }
}
