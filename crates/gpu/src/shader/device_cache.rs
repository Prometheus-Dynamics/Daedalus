use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
};

static NEXT_DEVICE_KEY: AtomicUsize = AtomicUsize::new(1);
static DEVICE_KEYS: OnceLock<Mutex<HashMap<u64, usize>>> = OnceLock::new();

/// The device's identity: the same for every clone of a `wgpu::Device`, and unique among live
/// devices, across wgpu instances too. That is the address of the backend's device object, which
/// wgpu keeps in place for the device's lifetime. The address of the handle passed in differs per
/// clone, and wgpu's own `Hash` is unique only within one instance (its ids restart per
/// instance), so neither is used where the backend object is reachable.
fn device_id(device: &wgpu::Device) -> u64 {
    // SAFETY: only the address of the backend device is read; nothing is called on it.
    #[cfg(any(target_os = "linux", target_os = "android", windows))]
    if let Some(raw) = unsafe { device.as_hal::<wgpu::hal::api::Vulkan>() } {
        return std::ptr::from_ref(&*raw) as usize as u64;
    }
    // SAFETY: as above.
    #[cfg(windows)]
    if let Some(raw) = unsafe { device.as_hal::<wgpu::hal::api::Dx12>() } {
        return std::ptr::from_ref(&*raw) as usize as u64;
    }
    // SAFETY: as above.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    if let Some(raw) = unsafe { device.as_hal::<wgpu::hal::api::Metal>() } {
        return std::ptr::from_ref(&*raw) as usize as u64;
    }
    // Another backend (WebGPU): wgpu's identity, unique within its instance.
    let mut hasher = std::hash::DefaultHasher::new();
    device.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn register_device(device: &wgpu::Device) -> usize {
    let id = device_id(device);
    let key = NEXT_DEVICE_KEY.fetch_add(1, Ordering::Relaxed);
    DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .insert(id, key);
    key
}

/// The key of the per-device caches (pipelines, staging pools, the installed pipeline cache):
/// the same for every clone of a device.
pub(crate) fn device_key(device: &wgpu::Device) -> usize {
    let id = device_id(device);
    let mut keys = DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock();
    *keys
        .entry(id)
        .or_insert_with(|| NEXT_DEVICE_KEY.fetch_add(1, Ordering::Relaxed))
}

pub(crate) fn unregister_device(device: &wgpu::Device, key: usize) {
    let id = device_id(device);
    let mut keys = DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock();
    if keys.get(&id).copied() == Some(key) {
        keys.remove(&id);
    }
}
