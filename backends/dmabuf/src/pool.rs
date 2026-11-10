//! Blocks, and a pool that reuses them.

use std::os::fd::BorrowedFd;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use daedalus_dma_sys::{Access, DmaBuf, Error, Identity, Mapping};

/// Idle blocks kept per size.
pub const KEPT: usize = 4;

/// Where a pool's blocks come from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    /// Anonymous memory. Needs no device.
    Memfd,
    /// The dma-heap of this name under `/dev/dma_heap`, such as `system`.
    Heap(String),
}

/// A block of memory with an fd and a length, and nothing else. It is mapped once, when made.
pub struct Block {
    buf: DmaBuf,
    map: Mapping,
    len: u64,
    /// A dma-heap block is told when the host starts and stops touching it.
    synced: bool,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl Block {
    fn new(source: &Source, len: u64) -> Result<Block, Error> {
        let (buf, synced) = match source {
            Source::Memfd => (crate::memfd::allocate(len)?, false),
            Source::Heap(name) => (crate::dma_heap::allocate(name, len)?, true),
        };
        let map = buf.as_ref().map()?;
        Ok(Block {
            buf,
            map,
            len,
            synced,
        })
    }

    /// The bytes asked for. The memory behind them may be larger, rounded up to pages.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The fd, to hand to a device or another process.
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.buf.as_fd()
    }

    /// Which memory this is, however many fds name it.
    pub fn identity(&self) -> Result<Identity, Error> {
        self.buf.as_ref().identity()
    }

    /// Starts host access of kind `access`. The guard reads and writes, and its drop ends the
    /// access.
    pub fn access(&self, access: Access) -> Result<Guard<'_>, Error> {
        if self.synced {
            self.buf.as_ref().begin_cpu_access(access)?;
        }
        Ok(Guard {
            block: self,
            access,
        })
    }
}

/// Host access to a block's bytes, until dropped.
pub struct Guard<'a> {
    block: &'a Block,
    access: Access,
}

impl Guard<'_> {
    fn range(&self, offset: u64, len: usize) -> Result<(), Error> {
        match offset.checked_add(len as u64) {
            Some(end) if end <= self.block.len => Ok(()),
            _ => Err(Error::Range),
        }
    }

    /// Copies the block from `offset` into `out`.
    pub fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), Error> {
        self.range(offset, out.len())?;
        self.block.map.read(offset, out)
    }

    /// Copies `bytes` into the block at `offset`. Needs write access.
    pub fn write(&self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        if self.access == Access::Read {
            return Err(Error::Range);
        }
        self.range(offset, bytes.len())?;
        self.block.map.write(offset, bytes)
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.block.synced {
            // Nothing useful to do with a failure here: the access is over either way.
            let _ = self.block.buf.as_ref().end_cpu_access(self.access);
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Blocks made because none of the size was idle.
    pub allocated: u64,
    /// Takes served by a block used before.
    pub reused: u64,
    /// Blocks let go because their size already had [`KEPT`] idle.
    pub dropped: u64,
    /// Blocks idle now.
    pub idle: usize,
    /// Blocks out with a holder now.
    pub lent: usize,
}

/// A pool of blocks. A block is idle again once its last holder has dropped it, and the pool
/// finds that out the next time it is asked for anything. Dropping the pool lets go of every
/// idle block, and a block still held lives until its holder drops it.
pub struct Pool {
    source: Source,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Every block the pool has handed out and not yet seen come back.
    lent: Vec<Arc<Block>>,
    idle: Vec<Arc<Block>>,
    stats: PoolStats,
}

impl State {
    /// Moves blocks nobody else holds from `lent` to `idle`, up to [`KEPT`] per size.
    fn reclaim(&mut self) {
        let mut at = 0;
        while at < self.lent.len() {
            // The pool's is the only reference, so nobody can make another: the block is back.
            if Arc::strong_count(&self.lent[at]) != 1 {
                at += 1;
                continue;
            }
            let block = self.lent.swap_remove(at);
            let same = self
                .idle
                .iter()
                .filter(|idle| idle.len == block.len)
                .count();
            if same >= KEPT {
                self.stats.dropped += 1;
            } else {
                self.idle.push(block);
            }
        }
    }
}

impl Pool {
    pub fn new(source: Source) -> Pool {
        Pool {
            source,
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A block of `len` bytes: one used before when the pool has one idle, and a new one
    /// otherwise. A reused block holds what its last user wrote.
    pub fn take(&self, len: u64) -> Result<Arc<Block>, Error> {
        let mut state = self.state();
        state.reclaim();
        let block = match state.idle.iter().position(|idle| idle.len == len) {
            Some(at) => {
                state.stats.reused += 1;
                state.idle.swap_remove(at)
            }
            None => {
                let block = Arc::new(Block::new(&self.source, len)?);
                state.stats.allocated += 1;
                block
            }
        };
        state.lent.push(Arc::clone(&block));
        Ok(block)
    }

    pub fn stats(&self) -> PoolStats {
        let mut state = self.state();
        state.reclaim();
        PoolStats {
            idle: state.idle.len(),
            lent: state.lent.len(),
            ..state.stats
        }
    }

    pub fn source(&self) -> &Source {
        &self.source
    }
}
