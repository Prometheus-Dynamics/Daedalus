use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
};

static NEXT_DEVICE_KEY: AtomicUsize = AtomicUsize::new(1);
static DEVICE_KEYS: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();

fn device_ptr(device: &wgpu::Device) -> usize {
    device as *const _ as usize
}

pub(crate) fn register_device(device: &wgpu::Device) -> usize {
    let ptr = device_ptr(device);
    let key = NEXT_DEVICE_KEY.fetch_add(1, Ordering::Relaxed);
    DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .insert(ptr, key);
    key
}

pub(crate) fn device_key(device: &wgpu::Device) -> usize {
    let ptr = device_ptr(device);
    let mut keys = DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock();
    if let Some(key) = keys.get(&ptr).copied() {
        return key;
    }
    let key = NEXT_DEVICE_KEY.fetch_add(1, Ordering::Relaxed);
    keys.insert(ptr, key);
    key
}

pub(crate) fn unregister_device(device: &wgpu::Device, key: usize) {
    let ptr = device_ptr(device);
    let mut keys = DEVICE_KEYS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock();
    if keys.get(&ptr).copied() == Some(key) {
        keys.remove(&ptr);
    }
}
