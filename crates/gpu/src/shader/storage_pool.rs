//! A pool of [`GpuStorage`] buffers in power-of-two size classes, for per-frame scratch.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{GpuContextHandle, GpuError};

use super::GpuStorage;

/// Smallest size class, bytes.
const MIN_CLASS: u64 = 256;

/// Counters of a [`GpuStoragePool`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GpuStoragePoolStats {
    /// Buffers created because no pooled one fit.
    pub created: u64,
    /// Takes served from the pool.
    pub reused: u64,
    /// Returned buffers dropped because keeping them would exceed the cap.
    pub dropped: u64,
    /// Bytes held by the pool (idle buffers).
    pub pooled_bytes: u64,
}

impl GpuStoragePoolStats {
    /// Share of takes served from the pool.
    pub fn reuse_rate(&self) -> f64 {
        let takes = self.created + self.reused;
        if takes == 0 {
            0.0
        } else {
            self.reused as f64 / takes as f64
        }
    }
}

/// Scratch storage buffers a node reuses frame after frame instead of allocating: take one of at
/// least the size needed, and give it back when the GPU work using it is done (after its
/// [`super::GpuTicket`] completes). Buffers are kept in power-of-two size classes (from 256 B),
/// up to `cap_bytes` idle; one returned beyond the cap is dropped.
///
/// A taken buffer is not cleared: it holds what its last user wrote. Its
/// [`GpuStorage::size`] is the size asked for; the buffer behind it may be larger (its class),
/// which `arrayLength` in a shader would see when bound with [`GpuStorage::buffer`].
pub struct GpuStoragePool {
    gpu: GpuContextHandle,
    cap_bytes: u64,
    state: Arc<Mutex<PoolState>>,
}

#[derive(Default)]
struct PoolState {
    idle: BTreeMap<u64, Vec<GpuStorage>>,
    stats: GpuStoragePoolStats,
}

impl std::fmt::Debug for GpuStoragePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GpuStoragePool")
            .field("cap_bytes", &self.cap_bytes)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// The class a request of `size` bytes comes from.
fn class_of(size: u64) -> u64 {
    size.max(MIN_CLASS).next_power_of_two()
}

impl GpuStoragePool {
    /// A pool on `gpu`'s device keeping at most `cap_bytes` of idle buffers. `Unsupported` on a
    /// non-wgpu backend.
    pub fn new(gpu: &GpuContextHandle, cap_bytes: u64) -> Result<Self, GpuError> {
        gpu.wgpu().ok_or(GpuError::Unsupported)?;
        Ok(Self {
            gpu: gpu.clone(),
            cap_bytes,
            state: Arc::default(),
        })
    }

    /// A buffer of at least `size` bytes (a multiple of 4): a pooled one of its class, else a
    /// new one.
    pub fn take(&self, size: u64) -> Result<GpuStorage, GpuError> {
        if size == 0 || !size.is_multiple_of(4) {
            return Err(GpuError::InvalidInput(format!(
                "pooled buffer size {size} must be a non-zero multiple of 4"
            )));
        }
        let class = class_of(size);
        let pooled = {
            let mut state = self.state.lock();
            let found = state.idle.get_mut(&class).and_then(Vec::pop);
            if found.is_some() {
                state.stats.reused += 1;
                state.stats.pooled_bytes -= class;
            }
            found
        };
        let mut storage = match pooled {
            Some(storage) => storage,
            None => {
                let storage = GpuStorage::new(&self.gpu, class, Some("daedalus-pool"))?;
                self.state.lock().stats.created += 1;
                storage
            }
        };
        storage.size = size;
        Ok(storage)
    }

    /// Return a buffer taken from this pool. The GPU work using it must be complete (or ordered
    /// before any later use, on the same queue).
    pub fn give(&self, mut storage: GpuStorage) {
        let class = storage.buffer.size();
        storage.size = class;
        let mut state = self.state.lock();
        if state.stats.pooled_bytes + class > self.cap_bytes {
            state.stats.dropped += 1;
            return;
        }
        state.stats.pooled_bytes += class;
        state.idle.entry(class).or_default().push(storage);
    }

    pub fn stats(&self) -> GpuStoragePoolStats {
        self.state.lock().stats
    }

    /// Drop every idle buffer.
    pub fn clear(&self) {
        let mut state = self.state.lock();
        state.idle.clear();
        state.stats.pooled_bytes = 0;
    }
}
