//! The host's hold on output values a worker wrote into lent memory.
//!
//! A worker that agreed values by reference writes an output value into one of the pool's value
//! buffers, the one the run's note named. The host does not copy it out: the output cell is a
//! hand-over whose bytes are that range of the host's own mapping of the pool
//! ([`daedalus_abi::HeldRegions`]). While any cell holds a range of a buffer, the buffer is held,
//! and the host names another one in the next run's note, or none (the worker then sends its
//! outputs in the packet). When the last cell lets go, the buffer is free again. A cell's value
//! keeps the mapping alive, so it outlives a worker that died, and a pool replaced by a larger one.
//!
//! The host also writes input values into the value buffers, a lease at a time: a node on the
//! host writes its output straight into room the host leases it ([`Rooms`]), and the worker reads
//! it there as an input. Leases are taken in a buffer the worker is never told to write in, and
//! a buffer the worker is told to write in is claimed for its run, so neither writes over the
//! other.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use daedalus_abi::{HeldRegions, Lease, LeaseCounts, Lend, Regions, SharedBytes, WritableRegions};
use shm_ring_buffers::{BufferIndex, PoolView};

/// The value buffers of one pool, as the host maps them: a view kept for the cells, so the
/// buffers stay mapped while it lives.
pub(crate) struct Values {
    view: PoolView,
    /// The index of the first value buffer in the pool.
    first: u32,
}

impl Values {
    fn buffer(&self, index: usize) -> Option<BufferIndex> {
        Some(BufferIndex(
            self.first.checked_add(u32::try_from(index).ok()?)?,
        ))
    }
}

impl Regions for Values {
    fn region(&self, index: usize) -> Option<&[u8]> {
        self.view.read_bytes(self.buffer(index)?).ok()
    }
}

impl WritableRegions for Values {
    fn region_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        let buffer = self.buffer(index)?;
        self.view.write_bytes(buffer).ok()
    }
}

/// The value buffers `first .. first + count` of a pool, and who holds them.
pub(crate) struct Held {
    regions: Arc<HeldRegions<Values>>,
    first: u32,
}

impl Held {
    /// `view` is writable, so the host can lease room in the buffers.
    pub(crate) fn new(view: PoolView, first: u32, count: u32) -> Held {
        Held {
            regions: HeldRegions::writable(Values { view, first }, count as usize),
            first,
        }
    }

    /// Claims a value buffer no cell or lease holds, for a run's outputs, until
    /// [`Held::unclaim`].
    pub(crate) fn claim(&self) -> Option<u32> {
        self.regions.claim().map(|at| self.first + at as u32)
    }

    pub(crate) fn unclaim(&self, buffer: u32) {
        if let Some(at) = buffer.checked_sub(self.first) {
            self.regions.unclaim(at as usize);
        }
    }

    /// Where a hand-over of these buffers is: its buffer and offset. None for any other bytes.
    pub(crate) fn locate(&self, bytes: &SharedBytes) -> Option<(u32, u64)> {
        let (at, offset) = self.regions.locate(bytes)?;
        Some((self.first + at as u32, offset as u64))
    }

    /// The pool indexes of the value buffers.
    pub(crate) fn buffers(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.regions.count() as u32).map(|at| self.first + at)
    }

    /// A value buffer no cell holds, if there is one.
    #[cfg(test)]
    pub(crate) fn free_buffer(&self) -> Option<u32> {
        self.regions.free_region().map(|at| self.first + at as u32)
    }

    /// The bytes `offset .. offset + len` of value buffer `buffer`, as a hand-over a cell holds.
    /// None when `buffer` is not a value buffer or the range is not inside it.
    pub(crate) fn adopt(&self, buffer: u32, offset: u64, len: u64) -> Option<SharedBytes> {
        let at = buffer.checked_sub(self.first)? as usize;
        let offset = usize::try_from(offset).ok()?;
        let len = usize::try_from(len).ok()?;
        self.regions.adopt(at, offset, len)
    }

    pub(crate) fn lease_counts(&self) -> LeaseCounts {
        self.regions.lease_counts()
    }
}

/// Room in a worker's value buffers that a node on the host writes an output value into, for the
/// worker to read where it is. It follows the worker's pool: a pool replaced by a larger one, or
/// lent to a worker started again, is pointed at anew, and while the worker has none it lends
/// nothing, so the node writes its value as it would without it.
#[derive(Default)]
pub struct Rooms {
    held: Mutex<Option<Arc<HeldRegions<Values>>>>,
    threshold: AtomicUsize,
}

impl Rooms {
    pub fn new() -> Arc<Rooms> {
        Arc::new(Rooms {
            held: Mutex::new(None),
            threshold: AtomicUsize::new(usize::MAX),
        })
    }

    /// Lends nothing until pointed at a pool again.
    pub fn clear(&self) {
        *self.held.lock().unwrap_or_else(PoisonError::into_inner) = None;
        self.threshold.store(usize::MAX, Ordering::Relaxed);
    }

    pub(crate) fn point(&self, held: &Held, threshold: usize) {
        *self.held.lock().unwrap_or_else(PoisonError::into_inner) = Some(held.regions.clone());
        self.threshold.store(threshold, Ordering::Relaxed);
    }

    /// The leases taken in the pool it points at now.
    pub fn lease_counts(&self) -> LeaseCounts {
        self.current()
            .map(|held| held.lease_counts())
            .unwrap_or_default()
    }

    fn current(&self) -> Option<Arc<HeldRegions<Values>>> {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Lend for Rooms {
    fn lease(&self, len: usize, items_at: usize) -> Option<Lease> {
        if len < self.threshold() {
            return None;
        }
        self.current()?.lease(len, items_at)
    }

    fn threshold(&self) -> usize {
        self.threshold.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shm_ring_buffers::Pool;

    fn held() -> (Pool, Held) {
        let pool = Pool::in_memory(1, 4, 64 * 1024).expect("a pool");
        let view = pool.writable_view().expect("a view");
        (pool, Held::new(view, 2, 2))
    }

    #[test]
    fn a_buffer_is_held_while_a_value_in_it_is_and_free_after() {
        let (_pool, held) = held();
        assert_eq!(held.buffers().collect::<Vec<_>>(), [2, 3]);
        assert_eq!(held.free_buffer(), Some(2));
        let value = held.adopt(2, 0, 16).expect("in range");
        assert_eq!(held.free_buffer(), Some(3), "buffer 2 is held");
        drop(value);
        assert_eq!(held.free_buffer(), Some(2), "let go, it is free");
    }

    #[test]
    fn rooms_lend_in_the_pool_they_point_at_and_nothing_below_the_threshold() {
        let (_pool, held) = held();
        let rooms = Rooms::new();
        assert!(rooms.lease(4096, 0).is_none(), "pointed at nothing");
        rooms.point(&held, 1024);
        assert!(rooms.lease(100, 0).is_none(), "below the threshold");
        let mut lease = rooms.lease(4096, 0).expect("room");
        lease.bytes_mut()[..2].copy_from_slice(&[5, 6]);
        let value = lease.seal(2).expect("sealed");
        let (buffer, offset) = held.locate(&value).expect("in a value buffer");
        assert_eq!(held.claim().filter(|claimed| *claimed == buffer), None);
        assert_eq!(
            held.adopt(buffer, offset, 2)
                .map(|again| again.bytes().to_vec()),
            Some(vec![5, 6]),
            "the host's own mapping sees what the lease wrote"
        );
        assert_eq!(rooms.lease_counts().sealed, 1);
        rooms.clear();
        assert!(rooms.lease(4096, 0).is_none(), "cleared");
    }

    #[test]
    fn a_range_outside_a_value_buffer_is_refused() {
        let (_pool, held) = held();
        assert!(held.adopt(0, 0, 16).is_none(), "not a value buffer");
        assert!(held.adopt(4, 0, 16).is_none(), "past the last");
        assert!(
            held.adopt(2, 64 * 1024 - 8, 16).is_none(),
            "runs off the end"
        );
    }

    #[test]
    fn a_value_outlives_the_pool_it_came_from() {
        let (pool, held) = held();
        let value = held.adopt(3, 8, 8).expect("in range");
        drop(held);
        drop(pool);
        assert_eq!(value.bytes().len(), 8, "the mapping is still there");
    }
}
