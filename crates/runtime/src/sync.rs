//! Lock types of the runtime and engine: `lock_api` locks over a raw lock chosen per build.
//!
//! - `std` (implied by the default `threads`): `parking_lot`, which parks contended threads.
//! - without `std`: `spin` locks, for targets without threads (`wasm32-unknown-unknown`) and,
//!   later, `no_std` (see "Portability" in docs/development.md). On targets without
//!   compare-and-swap (`thumbv6m`) `spin` runs on `portable-atomic`, whose `critical-section`
//!   implementation the final binary provides, like the tier-1 crates.
//!
//! Both are the same `lock_api` types over different raw locks, so the API (`lock()` returns the
//! guard, no poisoning, `MutexGuard::unlocked`, ...) does not change with the backend.
//! [`Condvar`] blocks a thread, so it exists only with `std`.

#[cfg(feature = "std")]
pub use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(feature = "std"))]
pub use spin::lock_api::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "std")]
pub use parking_lot::Condvar;
