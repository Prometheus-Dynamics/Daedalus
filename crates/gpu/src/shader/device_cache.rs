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

#[cfg(all(test, feature = "gpu-wgpu"))]
#[allow(clippy::print_stderr)] // the measurement
mod timing {
    use std::time::Instant;

    /// Per-call cost of [`super::device_key`], which every dispatch made several times before the
    /// keys were passed down. The minimum of five batches, as host load is noisy. Run with
    /// `cargo test -p daedalus-gpu --features gpu-wgpu -- --ignored --nocapture device_key_cost`.
    #[test]
    #[ignore = "timing probe: needs a GPU, run with --ignored --nocapture"]
    fn device_key_cost() {
        let Ok(gpu) = crate::selection::select_backend(&crate::GpuOptions::default()) else {
            return eprintln!("skipping: no adapter");
        };
        let Some((device, _queue)) = gpu.wgpu_locked() else {
            return eprintln!("skipping: not a wgpu device");
        };
        const N: u32 = 200_000;
        let mut acc = 0usize;
        let mut best = f64::MAX;
        for _ in 0..5 {
            let start = Instant::now();
            for _ in 0..N {
                acc ^= super::device_key(std::hint::black_box(device));
            }
            best = best.min(start.elapsed().as_nanos() as f64 / f64::from(N));
        }
        std::hint::black_box(acc);
        eprintln!("device_key: {best:.1} ns per call (best of 5 x {N})");
    }
}
