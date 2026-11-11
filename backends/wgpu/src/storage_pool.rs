//! A pool of scratch storage buffers in power-of-two sizes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::WgpuBackend;
use crate::buffer::{Storage, StorageError};
use crate::context::Context;

/// The smallest size class, in bytes.
const MIN_CLASS: u64 = 256;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoragePoolStats {
    /// Buffers made by [`StoragePool::fill`].
    pub created: u64,
    /// Takes served.
    pub reused: u64,
    /// Takes that found the class empty.
    pub missed: u64,
    /// Buffers given back and dropped, because keeping them would pass the cap.
    pub dropped: u64,
    /// Bytes of idle buffers.
    pub pooled_bytes: u64,
}

/// Scratch buffers a node takes and gives back run after run.
///
/// The pool is filled at load with [`StoragePool::fill`]. A take never makes a buffer: when its
/// class is empty the take says so, and the caller faults. A buffer given back beyond the cap is
/// dropped and counted. A taken buffer isn't cleared, and holds what its last user wrote.
pub struct StoragePool {
    context: Arc<Context>,
    cap_bytes: u64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    idle: BTreeMap<u64, Vec<Storage>>,
    stats: StoragePoolStats,
}

/// The class a request for `size` bytes comes from.
fn class_of(size: u64) -> u64 {
    size.max(MIN_CLASS).next_power_of_two()
}

impl StoragePool {
    /// A pool on the backend's device that keeps at most `cap_bytes` of idle buffers.
    pub fn new(backend: &WgpuBackend, cap_bytes: u64) -> Result<StoragePool, StorageError> {
        Ok(StoragePool {
            context: backend.context().map_err(StorageError::Open)?,
            cap_bytes,
            state: Mutex::new(State::default()),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Makes `count` buffers that serve requests for up to `size` bytes, and returns how many the
    /// cap had room for. Call this at load.
    pub fn fill(&self, size: u64, count: usize) -> Result<usize, StorageError> {
        let class = class_of(size);
        let mut made = 0;
        for _ in 0..count {
            if self.state().stats.pooled_bytes + class > self.cap_bytes {
                break;
            }
            let storage = Storage::zeroed(Arc::clone(&self.context), class)?;
            let mut state = self.state();
            state.stats.created += 1;
            state.stats.pooled_bytes += class;
            let list = state.idle.entry(class).or_default();
            // Room to take every buffer and give it back without growing.
            list.reserve(count);
            list.push(storage);
            made += 1;
        }
        Ok(made)
    }

    /// A buffer of `size` bytes, a non-zero multiple of 4. `None` when its class is empty.
    pub fn take(&self, size: u64) -> Option<Storage> {
        if size == 0 || !size.is_multiple_of(4) {
            return None;
        }
        let class = class_of(size);
        let mut state = self.state();
        let Some(mut storage) = state.idle.get_mut(&class).and_then(Vec::pop) else {
            state.stats.missed += 1;
            return None;
        };
        state.stats.reused += 1;
        state.stats.pooled_bytes -= class;
        storage.size = size;
        Some(storage)
    }

    /// Gives a buffer back. The work that used it must have finished, or be ordered before any
    /// later use on the same queue. A buffer from another device, or beyond the cap, is dropped.
    pub fn give(&self, mut storage: Storage) {
        let class = storage.capacity();
        let mut state = self.state();
        let foreign = storage.context.key != self.context.key;
        if foreign || state.stats.pooled_bytes + class > self.cap_bytes {
            state.stats.dropped += 1;
            return;
        }
        storage.size = class;
        state.stats.pooled_bytes += class;
        state.idle.entry(class).or_default().push(storage);
    }

    pub fn stats(&self) -> StoragePoolStats {
        self.state().stats
    }

    /// Drops every idle buffer.
    pub fn clear(&self) {
        let mut state = self.state();
        state.idle.clear();
        state.stats.pooled_bytes = 0;
    }

    /// Which device the pool's buffers are on.
    pub fn device_key(&self) -> u64 {
        self.context.key
    }
}

#[cfg(test)]
mod tests {
    use super::class_of;

    #[test]
    fn sizes_round_up_to_a_power_of_two_class() {
        assert_eq!(class_of(4), 256);
        assert_eq!(class_of(256), 256);
        assert_eq!(class_of(260), 512);
        assert_eq!(class_of(1 << 20), 1 << 20);
    }
}
