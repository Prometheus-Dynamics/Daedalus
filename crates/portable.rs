//! What the `no_std`-capable crates take from the platform, chosen per build (see "Portability"
//! in docs/development.md):
//!
//! - Locks and lazy globals: the standard primitives with `std`, `spin` without.
//! - Atomics and `Arc`: `core`/`alloc` where the target has pointer-sized compare-and-swap,
//!   otherwise `portable-atomic` (`critical-section` based) and `portable-atomic-util::Arc`.
//!   `AtomicU64` falls back to `portable-atomic` on targets without 64-bit atomics.
//!
//! Shared module: each crate's `src/portable.rs` is a symlink to this file (like
//! `build_features.rs`), and each crate uses a subset of it.
#![allow(dead_code, unused_imports, unused_macros)]

#[cfg(feature = "std")]
pub(crate) use std::sync::{Mutex, MutexGuard, OnceLock};

#[cfg(not(feature = "std"))]
pub(crate) use spin::{Mutex, MutexGuard};

#[cfg(target_has_atomic = "ptr")]
pub(crate) use alloc::sync::{Arc, Weak};
#[cfg(target_has_atomic = "ptr")]
pub(crate) use core::sync::atomic::{AtomicBool, AtomicUsize};
#[cfg(not(target_has_atomic = "ptr"))]
pub(crate) use portable_atomic::{AtomicBool, AtomicUsize};
#[cfg(not(target_has_atomic = "ptr"))]
pub(crate) use portable_atomic_util::{Arc, Weak};

#[cfg(target_has_atomic = "64")]
pub(crate) use core::sync::atomic::AtomicU64;
#[cfg(not(target_has_atomic = "64"))]
pub(crate) use portable_atomic::AtomicU64;

/// `Arc::new(value)` as an `Arc` of the unsized type the context expects (`Arc<dyn Trait>`).
///
/// `portable-atomic-util::Arc` cannot unsize-coerce on stable Rust, so on targets without
/// compare-and-swap the value goes through a `Box` (one extra allocation and move).
macro_rules! arc_dyn {
    ($value:expr) => {{
        #[cfg(target_has_atomic = "ptr")]
        let arc = $crate::portable::Arc::new($value);
        #[cfg(not(target_has_atomic = "ptr"))]
        let arc =
            $crate::portable::Arc::from(alloc::boxed::Box::new($value) as alloc::boxed::Box<_>);
        arc
    }};
}
pub(crate) use arc_dyn;

/// Locks `mutex`, recovering the guard if a panicking holder poisoned it.
///
/// Every critical section guarded this way leaves its state structurally valid, so a poisoned
/// lock carries no information worth propagating.
pub(crate) fn lock_recover<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    #[cfg(feature = "std")]
    return mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    #[cfg(not(feature = "std"))]
    mutex.lock()
}

/// The `std::sync::OnceLock` subset these crates use, over `spin::Once`.
#[cfg(not(feature = "std"))]
pub(crate) struct OnceLock<T>(spin::Once<T>);

#[cfg(not(feature = "std"))]
impl<T> OnceLock<T> {
    pub(crate) const fn new() -> Self {
        Self(spin::Once::new())
    }

    pub(crate) fn get_or_init(&self, init: impl FnOnce() -> T) -> &T {
        self.0.call_once(init)
    }
}
