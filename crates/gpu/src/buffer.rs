use serde::{Deserialize, Serialize};
use std::sync::Mutex;

use crate::{GpuError, GpuMemoryLocation, GpuUsage, handles::GpuBufferHandle};

/// Simple buffer pool trait; implementations provide recycling.
pub trait BufferPool: Send + Sync {
    fn alloc(
        &self,
        size_bytes: u64,
        usage: GpuUsage,
        location: GpuMemoryLocation,
    ) -> Result<GpuBufferHandle, GpuError>;
    fn free(&self, handle: GpuBufferHandle);
}

/// Naive buffer pool that reuses freed handles by size and usage.
pub struct SimpleBufferPool {
    buckets: Mutex<Vec<Vec<GpuBufferHandle>>>,
}

impl SimpleBufferPool {
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(vec![Vec::new(); 16]), // buckets by power-of-two sizes
        }
    }
}

impl Default for SimpleBufferPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool for SimpleBufferPool {
    fn alloc(
        &self,
        size_bytes: u64,
        usage: GpuUsage,
        location: GpuMemoryLocation,
    ) -> Result<GpuBufferHandle, GpuError> {
        if usage.is_empty() {
            return Err(GpuError::Unsupported);
        }
        let bucket_idx = bucket_for(size_bytes);
        let mut buckets = self.buckets.lock().expect("buffer pool lock poisoned");
        if let Some(bucket) = buckets.get_mut(bucket_idx)
            && let Some((idx, _)) = bucket
                .iter()
                .enumerate()
                .find(|(_, h)| h.usage == usage && h.location == location)
        {
            let handle = bucket.swap_remove(idx);
            return Ok(handle);
        }
        Ok(GpuBufferHandle::new(size_bytes, location, usage))
    }

    fn free(&self, handle: GpuBufferHandle) {
        let idx = bucket_for(handle.size_bytes);
        let mut buckets = self.buckets.lock().expect("buffer pool lock poisoned");
        if let Some(bucket) = buckets.get_mut(idx) {
            bucket.push(handle);
        }
    }
}

fn bucket_for(size: u64) -> usize {
    let mut pow = 0;
    let mut s = 1u64;
    while s < size && pow < 63 {
        s <<= 1;
        pow += 1;
    }
    usize::min(pow as usize, 15)
}

/// Upload telemetry for tracking bytes transferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransferStats {
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
}

impl TransferStats {
    pub fn record_upload(&mut self, bytes: u64) {
        self.bytes_uploaded = self.bytes_uploaded.saturating_add(bytes);
    }

    pub fn record_download(&mut self, bytes: u64) {
        self.bytes_downloaded = self.bytes_downloaded.saturating_add(bytes);
    }

    pub fn take(&mut self) -> TransferStats {
        std::mem::take(self)
    }
}
