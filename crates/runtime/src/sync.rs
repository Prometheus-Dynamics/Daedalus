//! Lock types of the runtime and engine: `lock_api` locks over a raw lock chosen per build.
//!
//! - `std` (default): `parking_lot`, which parks contended threads.
//! - without `std`: `spin` locks, for single-threaded targets (`wasm32-unknown-unknown`) and, later,
//!   `no_std` (see "Portability" in docs/development.md).
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
