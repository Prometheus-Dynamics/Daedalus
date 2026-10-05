//! Process-wide serialization of graphics driver setup.
//!
//! The Vulkan loader's pre-instance calls (`vkEnumerateInstanceExtensionProperties`, which wgpu
//! makes when it creates an instance) scan and negotiate with every installed ICD. Some ICDs do
//! not survive that concurrently: with the NVIDIA ICD installed next to Mesa (even without an
//! NVIDIA GPU), two threads creating instances at once crash in the loader's terminator calling a
//! null ICD entry point while the other thread is inside `vk_icdNegotiateLoaderICDInterfaceVersion`
//! of `libGLX_nvidia.so`. Every place in this crate that creates a wgpu instance, enumerates or
//! requests adapters, or opens a device does so under [`driver_lock`]; nothing else is serialized.
//!
//! The guard is `Send` (it holds no borrowed lock), so async constructors may keep it across the
//! `.await`s of wgpu's adapter and device futures, which resolve immediately on native backends.

use parking_lot::{Condvar, Mutex};

static BUSY: Mutex<bool> = Mutex::new(false);
static FREED: Condvar = Condvar::new();

/// Held while a wgpu instance, adapter or device is being created; see the module docs.
#[must_use = "the driver lock is released when the guard is dropped"]
pub(crate) struct DriverGuard(());

/// Wait until no other thread sets up a graphics driver, then hold the lock (not reentrant).
pub(crate) fn driver_lock() -> DriverGuard {
    let mut busy = BUSY.lock();
    while *busy {
        FREED.wait(&mut busy);
    }
    *busy = true;
    DriverGuard(())
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        *BUSY.lock() = false;
        FREED.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::driver_lock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn driver_lock_is_exclusive_and_send() {
        fn assert_send<T: Send>(_: &T) {}
        static INSIDE: AtomicUsize = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        let guard = driver_lock();
                        assert_send(&guard);
                        assert_eq!(INSIDE.fetch_add(1, Ordering::SeqCst), 0);
                        std::thread::yield_now();
                        INSIDE.fetch_sub(1, Ordering::SeqCst);
                    }
                });
            }
        });
    }
}
