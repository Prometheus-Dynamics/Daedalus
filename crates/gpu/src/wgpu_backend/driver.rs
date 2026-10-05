//! Process-wide guards around the Vulkan loader, which is not safe against everything wgpu does
//! from several threads at once.
//!
//! - **Driver setup** ([`driver_lock`]). The loader's pre-instance calls
//!   (`vkEnumerateInstanceExtensionProperties`, made when wgpu creates an instance) scan and
//!   negotiate with every installed ICD. With the NVIDIA ICD installed next to Mesa (even without
//!   an NVIDIA GPU), two threads doing that at once crash in the loader's terminator calling a null
//!   ICD entry point while the other thread is inside `vk_icdNegotiateLoaderICDInterfaceVersion`
//!   of `libGLX_nvidia.so`. Every place in this crate that creates a wgpu instance, enumerates or
//!   requests adapters, or opens a device does so under the lock.
//! - **Debug labels** ([`instance_descriptor`]). The loader implements the `VK_EXT_debug_utils`
//!   device commands (`vkSetDebugUtilsObjectNameEXT`, labels) with terminators that look the device
//!   up by walking every instance's ICD and device lists, which another thread creating or
//!   destroying an instance or device changes under a different lock; the walk then dereferences
//!   freed handles. wgpu names an object on every command encoder (so on every submission) when its
//!   `DEBUG` instance flag is set, which `InstanceFlags::from_build_config` does in debug builds.
//!   Instances created here leave `DEBUG` off unless `WGPU_DEBUG=1` asks for labels (e.g. for a
//!   RenderDoc capture); the other flags (validation) keep wgpu's defaults and environment.
//!
//! The lock guard is `Send` (it holds no borrowed lock), so async constructors may keep it across
//! the `.await`s of wgpu's adapter and device futures, which resolve immediately on native
//! backends. Tearing instances and devices down needs no lock: the loader serializes creation and
//! destruction itself.

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

/// The descriptor for wgpu instances on `backends`: wgpu's defaults without debug labels unless
/// `WGPU_DEBUG=1` (see the module docs).
pub(crate) fn instance_descriptor(backends: wgpu::Backends) -> wgpu::InstanceDescriptor {
    let mut desc = wgpu::InstanceDescriptor::new_without_display_handle();
    desc.backends = backends;
    desc.flags = (desc.flags - wgpu::InstanceFlags::DEBUG).with_env();
    desc
}

#[cfg(test)]
mod tests {
    use super::{driver_lock, instance_descriptor};
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

    #[test]
    fn instances_have_no_debug_labels_by_default() {
        let desc = instance_descriptor(wgpu::Backends::VULKAN);
        assert_eq!(desc.backends, wgpu::Backends::VULKAN);
        if std::env::var_os("WGPU_DEBUG").is_none() {
            assert!(!desc.flags.contains(wgpu::InstanceFlags::DEBUG));
        }
    }
}
