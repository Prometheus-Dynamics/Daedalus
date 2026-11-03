//! Process-wide guards around the Vulkan loader, which is not safe against everything wgpu does
//! from several threads at once.
//!
//! - **Driver setup** ([`driver_lock`]). The loader's pre-instance calls
//!   (`vkEnumerateInstanceExtensionProperties`, made when wgpu creates an instance) scan and
//!   negotiate with every installed ICD. With the NVIDIA ICD installed next to Mesa (even without
//!   an NVIDIA GPU), two threads doing that at once crash in the loader's terminator calling a null
//!   ICD entry point while the other thread is inside `vk_icdNegotiateLoaderICDInterfaceVersion`
//!   of `libGLX_nvidia.so`. Loader 1.4.341's `vkEnumeratePhysicalDevices` also rescans the ICDs
//!   and `dlclose`s the previous scan (`loader_unload_scanned_icd`): a thread opening a device at
//!   the same time (`vkGetDeviceProcAddr` in `loader_get_icd_and_device`) faults. In this crate an
//!   instance comes only from [`LockedInstance::new`], which borrows a held [`DriverGuard`] and
//!   is the one way to enumerate or request adapters, and a device only from [`open_device`] (or
//!   the dmabuf `request_device`), which takes the guard too: none can run unlocked.
//!   `tests/driver_stress.rs` opens and closes 200 contexts on 4 threads; dropping the lock
//!   around the enumeration makes it crash.
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
//! destruction itself, and a destroyed instance's ICDs stay loaded while another instance holds
//! them.

use parking_lot::{Condvar, Mutex};

static BUSY: Mutex<bool> = Mutex::new(false);
static FREED: Condvar = Condvar::new();

/// Held while a graphics driver is being set up; see the module docs. Dropping it releases the
/// lock. It is `Send`, and it is not reentrant: taking it again on the same thread deadlocks.
#[must_use = "the driver lock is released when the guard is dropped"]
pub struct DriverGuard(());

/// Wait until no other thread sets up a graphics driver, then hold the lock.
///
/// Take it around any wgpu instance, adapter or device creation a host makes on its own, so that
/// it serializes with the creations Daedalus does. Do not call a Daedalus GPU constructor while
/// holding it: that takes the same lock and deadlocks. Use [`with_driver_lock`] for a closure.
pub fn driver_lock() -> DriverGuard {
    let mut busy = BUSY.lock();
    while *busy {
        FREED.wait(&mut busy);
    }
    *busy = true;
    DriverGuard(())
}

/// Run `f` while holding the driver lock (see [`driver_lock`]), and return its result.
pub fn with_driver_lock<R>(f: impl FnOnce() -> R) -> R {
    let _guard = driver_lock();
    f()
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        *BUSY.lock() = false;
        FREED.notify_one();
    }
}

/// A wgpu instance whose adapter calls run under the driver lock: it borrows the guard, so it
/// cannot outlive it, and the wgpu instance is not handed out.
pub(crate) struct LockedInstance<'g> {
    instance: wgpu::Instance,
    _held: &'g DriverGuard,
}

impl<'g> LockedInstance<'g> {
    /// A wgpu instance on `backends`, without debug labels unless `WGPU_DEBUG=1`.
    pub(crate) fn new(held: &'g DriverGuard, backends: wgpu::Backends) -> Self {
        Self {
            instance: wgpu::Instance::new(instance_descriptor(backends)),
            _held: held,
        }
    }

    pub(crate) async fn enumerate_adapters(&self, backends: wgpu::Backends) -> Vec<wgpu::Adapter> {
        self.instance.enumerate_adapters(backends).await
    }

    pub(crate) async fn request_adapter(
        &self,
        options: &wgpu::RequestAdapterOptions<'_, '_>,
    ) -> Result<wgpu::Adapter, wgpu::RequestAdapterError> {
        self.instance.request_adapter(options).await
    }
}

/// Open a device on `adapter`, under the driver lock.
pub(crate) async fn open_device(
    _held: &DriverGuard,
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    adapter.request_device(desc).await
}

/// The descriptor for wgpu instances on `backends`: wgpu's defaults without debug labels unless
/// `WGPU_DEBUG=1` (see the module docs).
fn instance_descriptor(backends: wgpu::Backends) -> wgpu::InstanceDescriptor {
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
    fn with_driver_lock_returns_the_value_and_releases_the_lock() {
        assert_eq!(super::with_driver_lock(|| 7), 7);
        // Would deadlock if the closure form left the lock held.
        drop(driver_lock());
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
