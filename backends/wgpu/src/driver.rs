//! The one place a wgpu instance or device is made, and the lock they're made under.
//!
//! The Vulkan loader's calls before an instance exists scan and negotiate with every installed
//! driver. With the NVIDIA driver installed beside Mesa, two threads doing that at once crash
//! inside the loader. So an instance exists only while a [`DriverGuard`] is borrowed, and a device
//! comes only from [`open_device`], which takes the guard too.
//!
//! Debug labels are off unless `WGPU_DEBUG=1`: with them, every submit calls the loader's
//! object-naming path, which walks lists that another thread's setup is changing.
//!
//! Dropping instances and devices needs no lock. The loader serializes that itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};

static BUSY: Mutex<bool> = Mutex::new(false);
static FREED: Condvar = Condvar::new();
static DEVICES: AtomicU64 = AtomicU64::new(0);

/// Held while a graphics driver is being set up. Dropping it releases the lock. It is `Send`, and
/// taking it twice on one thread deadlocks.
#[must_use = "the driver lock is released when the guard is dropped"]
pub struct DriverGuard(());

/// Waits until no other thread is setting up a graphics driver, then holds the lock.
///
/// A program that makes its own wgpu or Vulkan instances and devices takes this around them, so
/// they don't race the ones made here. Don't open a backend while holding it.
pub fn driver_lock() -> DriverGuard {
    let mut busy = BUSY.lock().unwrap_or_else(PoisonError::into_inner);
    while *busy {
        busy = FREED.wait(busy).unwrap_or_else(PoisonError::into_inner);
    }
    *busy = true;
    DriverGuard(())
}

/// Runs `f` under the driver lock.
pub fn with_driver_lock<R>(f: impl FnOnce() -> R) -> R {
    let _guard = driver_lock();
    f()
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        *BUSY.lock().unwrap_or_else(PoisonError::into_inner) = false;
        FREED.notify_one();
    }
}

impl daedalus_vk_sys::DriverHeld for DriverGuard {}

/// How many devices this process has opened here. Adopted devices aren't counted.
pub fn devices_created() -> u64 {
    DEVICES.load(Ordering::Relaxed)
}

/// A wgpu instance that can't outlive the guard it was made under.
pub(crate) struct LockedInstance<'g> {
    instance: wgpu::Instance,
    _held: &'g DriverGuard,
}

impl<'g> LockedInstance<'g> {
    pub(crate) fn new(held: &'g DriverGuard, backends: wgpu::Backends) -> Self {
        Self {
            instance: wgpu::Instance::new(instance_descriptor(backends)),
            _held: held,
        }
    }

    pub(crate) fn adapters(&self, backends: wgpu::Backends) -> Vec<wgpu::Adapter> {
        pollster::block_on(self.instance.enumerate_adapters(backends))
    }
}

/// Opens a device on `adapter`. The only device constructor. On Vulkan the device also gets
/// dma-buf import and 8-bit storage where the adapter has them.
pub(crate) fn open_device(
    held: &DriverGuard,
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    #[cfg(target_os = "linux")]
    let extended = daedalus_vk_sys::device::open_extended(held, adapter, desc);
    #[cfg(not(target_os = "linux"))]
    let extended = None;
    let _ = held;
    let opened = match extended {
        Some(opened) => opened,
        None => pollster::block_on(adapter.request_device(desc))?,
    };
    DEVICES.fetch_add(1, Ordering::Relaxed);
    Ok(opened)
}

fn instance_descriptor(backends: wgpu::Backends) -> wgpu::InstanceDescriptor {
    let mut desc = wgpu::InstanceDescriptor::new_without_display_handle();
    desc.backends = backends;
    desc.flags = (desc.flags - wgpu::InstanceFlags::DEBUG).with_env();
    desc
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{driver_lock, instance_descriptor, with_driver_lock};

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
    fn the_closure_form_returns_the_value_and_releases_the_lock() {
        assert_eq!(with_driver_lock(|| 7), 7);
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
