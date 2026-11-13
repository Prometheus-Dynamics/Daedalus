//! The moves between the host and the device, as entries the planner can pick. Each has a fixed
//! cost class, so planning never measures a device.
//!
//! A value on the device is a [`Storage`](crate::Storage), in whichever of the backend's memory
//! spaces it was made. A node body never copies across a space: the planner puts one of these
//! between two nodes when their memories differ.

use std::sync::atomic::{AtomicU64, Ordering};

/// The CPU backend's memory space.
pub const HOST_SPACE: &str = "cpu.host";
/// Host bytes into a device buffer.
pub const UPLOAD: &str = "wgpu.upload";
/// Host bytes into a host-mapped buffer, written in place.
pub const UPLOAD_MAPPED: &str = "wgpu.upload.mapped";
/// A device buffer into a host-mapped one, which the host then reads in place.
pub const READBACK: &str = "wgpu.readback";
/// A device buffer into dma-heap memory, which the host reads through its cache. Offered only on
/// a device that imports dma-bufs, and only where the system dma-heap opens.
pub const READBACK_HEAP: &str = "wgpu.readback.heap";
/// A host-mapped buffer handed to the host as it is.
pub const MAP: &str = "wgpu.map";
/// The same for a host-cached buffer.
pub const MAP_CACHED: &str = "wgpu.map.cached";
/// The same for a dma-heap buffer.
pub const MAP_HEAP: &str = "wgpu.map.dmaheap";

/// How much has moved between the host and one device.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferStats {
    pub uploads: u64,
    pub upload_bytes: u64,
    /// Copies from device memory to memory the host reads.
    pub downloads: u64,
    pub download_bytes: u64,
    /// Values whose memory was imported in place, with nothing copied.
    pub imports: u64,
}

#[derive(Default)]
pub(crate) struct Counters {
    uploads: AtomicU64,
    upload_bytes: AtomicU64,
    downloads: AtomicU64,
    download_bytes: AtomicU64,
    imports: AtomicU64,
}

impl Counters {
    pub(crate) fn upload(&self, bytes: u64) {
        self.uploads.fetch_add(1, Ordering::Relaxed);
        self.upload_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn download(&self, bytes: u64) {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        self.download_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn import(&self) {
        self.imports.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> TransferStats {
        TransferStats {
            uploads: self.uploads.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            downloads: self.downloads.load(Ordering::Relaxed),
            download_bytes: self.download_bytes.load(Ordering::Relaxed),
            imports: self.imports.load(Ordering::Relaxed),
        }
    }
}

/// A string kept for the life of the process, once for each distinct text.
pub(crate) fn intern(text: String) -> &'static str {
    static KEPT: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());
    let mut kept = KEPT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(found) = kept.iter().find(|have| **have == text) {
        return found;
    }
    let leaked: &'static str = text.leak();
    kept.push(leaked);
    leaked
}

/// Buffers a transfer step keeps to reuse. A consumer that holds more than this at once makes
/// the step allocate.
pub(crate) const KEPT: usize = 4;

/// Picks a buffer of `bytes` nobody else holds from `kept`, or makes one with `make`.
pub(crate) fn pooled(
    kept: &mut Vec<std::sync::Arc<crate::Storage>>,
    bytes: u64,
    make: impl FnOnce() -> Result<crate::Storage, crate::StorageError>,
) -> Result<std::sync::Arc<crate::Storage>, crate::StorageError> {
    use std::sync::Arc;
    // The step's is the only reference, so the last consumer is done with it.
    let free = kept
        .iter()
        .position(|storage| storage.size == bytes && Arc::strong_count(storage) == 1);
    if let Some(at) = free {
        return Ok(Arc::clone(&kept[at]));
    }
    let made = Arc::new(make()?);
    let stale = kept.iter().position(|storage| {
        Arc::strong_count(storage) == 1 && (storage.size != bytes || kept.len() >= KEPT)
    });
    if let Some(stale) = stale {
        kept.swap_remove(stale);
    }
    kept.push(Arc::clone(&made));
    Ok(made)
}
