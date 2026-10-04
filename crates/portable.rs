//! `std` / `no_std` switch for the locks and lazy globals of the `no_std`-capable crates: `std`
//! keeps the standard primitives, `no_std` + `alloc` swaps in `spin` (see "Portability" in
//! docs/development.md).
//!
//! Shared module: each crate's `src/portable.rs` is a symlink to this file (like
//! `build_features.rs`), and each crate uses a subset of it.
#![allow(dead_code, unused_imports)]

#[cfg(feature = "std")]
pub(crate) use std::sync::{Mutex, MutexGuard, OnceLock};

#[cfg(not(feature = "std"))]
pub(crate) use spin::{Mutex, MutexGuard};

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

#[cfg(target_has_atomic = "64")]
pub(crate) use core::sync::atomic::AtomicU64;

/// The `AtomicU64` subset these crates use, behind a spin lock on targets without 64-bit
/// atomics (e.g. `thumbv7em`).
#[cfg(not(target_has_atomic = "64"))]
#[derive(Debug, Default)]
pub(crate) struct AtomicU64(spin::Mutex<u64>);

#[cfg(not(target_has_atomic = "64"))]
impl AtomicU64 {
    pub(crate) const fn new(value: u64) -> Self {
        Self(spin::Mutex::new(value))
    }

    pub(crate) fn load(&self, _order: core::sync::atomic::Ordering) -> u64 {
        *self.0.lock()
    }

    pub(crate) fn fetch_add(&self, value: u64, _order: core::sync::atomic::Ordering) -> u64 {
        let mut current = self.0.lock();
        let previous = *current;
        *current = previous.wrapping_add(value);
        previous
    }
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
