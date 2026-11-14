//! The host's hold on output values a worker wrote into lent memory.
//!
//! A worker that agreed values by reference writes an output value into one of the pool's value
//! buffers, the one the run's note named. The host does not copy it out: the output cell is a
//! hand-over whose bytes are that range of the host's own mapping of the pool
//! ([`daedalus_abi::HeldRegions`]). While any cell holds a range of a buffer, the buffer is held,
//! and the host names another one in the next run's note, or none (the worker then sends its
//! outputs in the packet). When the last cell lets go, the buffer is free again. A cell's value
//! keeps the mapping alive, so it outlives a worker that died, and a pool replaced by a larger one.

use std::sync::Arc;

use daedalus_abi::{HeldRegions, Regions, SharedBytes};
use shm_ring_buffers::{BufferIndex, PoolView};

/// The value buffers of one pool, as the host maps them: a view kept for the cells, so the
/// buffers stay mapped while it lives.
pub(crate) struct Values {
    view: PoolView,
    /// The index of the first value buffer in the pool.
    first: u32,
}

impl Regions for Values {
    fn region(&self, index: usize) -> Option<&[u8]> {
        let buffer = self.first.checked_add(u32::try_from(index).ok()?)?;
        self.view.read_bytes(BufferIndex(buffer)).ok()
    }
}

/// The value buffers `first .. first + count` of a pool, and who holds them.
pub(crate) struct Held {
    regions: Arc<HeldRegions<Values>>,
    first: u32,
}

impl Held {
    pub(crate) fn new(view: PoolView, first: u32, count: u32) -> Held {
        Held {
            regions: HeldRegions::new(Values { view, first }, count as usize),
            first,
        }
    }

    /// The pool indexes of the value buffers.
    pub(crate) fn buffers(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.regions.count() as u32).map(|at| self.first + at)
    }

    /// A value buffer no cell holds, if there is one.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use shm_ring_buffers::Pool;

    fn held() -> (Pool, Held) {
        let pool = Pool::in_memory(1, 4, 64 * 1024).expect("a pool");
        let view = pool.view().expect("a view");
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
