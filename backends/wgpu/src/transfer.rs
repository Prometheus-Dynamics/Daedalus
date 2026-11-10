//! The moves between the host and the device, as entries the planner can pick. Each has a fixed
//! cost class, so planning never measures a device.
//!
//! A value on the device is a [`Storage`](crate::Storage), in whichever of the backend's memory
//! spaces it was made. A node body never copies across a space: the planner puts one of these
//! between two nodes when their memories differ.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_abi::Step;
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};

use crate::memory::{SPACE_DEVICE, SPACE_DMA_HEAP, SPACE_HOST_CACHED, SPACE_HOST_MAPPED};

/// The CPU backend's memory space.
pub const HOST_SPACE: &str = "cpu.host";
/// Host bytes into a device buffer.
pub const UPLOAD: &str = "wgpu.upload";
/// Host bytes into a host-mapped buffer, written in place.
pub const UPLOAD_MAPPED: &str = "wgpu.upload.mapped";
/// A device buffer into a host-mapped one, which the host then reads in place.
pub const READBACK: &str = "wgpu.readback";
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
}

#[derive(Default)]
pub(crate) struct Counters {
    uploads: AtomicU64,
    upload_bytes: AtomicU64,
    downloads: AtomicU64,
    download_bytes: AtomicU64,
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

    pub(crate) fn stats(&self) -> TransferStats {
        TransferStats {
            uploads: self.uploads.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            downloads: self.downloads.load(Ordering::Relaxed),
            download_bytes: self.download_bytes.load(Ordering::Relaxed),
        }
    }
}

fn entry(
    id: &'static str,
    from: &'static str,
    to: &'static str,
    cost: TransferCost,
    make: fn(&CompileCx<'_>) -> Result<Step, Refusal>,
) -> Transfer {
    Transfer {
        id,
        key: None,
        from: Cow::Borrowed(from),
        to: Cow::Borrowed(to),
        cost,
        make,
    }
}

/// Every move this backend offers. All are for any type.
pub(crate) fn table() -> Vec<Transfer> {
    use TransferCost::{Copy, Import};
    vec![
        entry(UPLOAD, HOST_SPACE, SPACE_DEVICE, Copy, crate::upload::make),
        entry(
            UPLOAD_MAPPED,
            HOST_SPACE,
            SPACE_HOST_MAPPED,
            Copy,
            crate::upload::make_mapped,
        ),
        entry(
            READBACK,
            SPACE_DEVICE,
            SPACE_HOST_MAPPED,
            Copy,
            crate::readback::make,
        ),
        entry(
            MAP,
            SPACE_HOST_MAPPED,
            HOST_SPACE,
            Import,
            crate::readback::make_map,
        ),
        entry(
            MAP_CACHED,
            SPACE_HOST_CACHED,
            HOST_SPACE,
            Import,
            crate::readback::make_map,
        ),
        entry(
            MAP_HEAP,
            SPACE_DMA_HEAP,
            HOST_SPACE,
            Import,
            crate::readback::make_map,
        ),
    ]
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
