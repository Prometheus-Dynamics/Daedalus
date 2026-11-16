//! The arena a run's values live in. A node's `dd_alloc` memory, and the values the loop writes for
//! a node's inputs and state, are kept here until the run ends, then freed together. A pointer the
//! arena hands out stays valid because each block is a heap allocation the arena never moves or
//! reallocates: only new blocks are added.

use std::ptr;

use crate::ffi::DdValue;

/// The size of a block the arena allocates when the current one is full.
const BLOCK_BYTES: usize = 64 * 1024;

/// The bytes of one allocation unit. A `u128` is 16-byte aligned on every target the SDK builds on.
const UNIT: usize = 16;

#[derive(Default)]
pub struct RunArena {
    /// Raw memory for `alloc`, in blocks of units.
    blocks: Vec<Vec<u128>>,
    /// The bytes of the last block already handed out.
    used: usize,
    /// Values and bytes the arena keeps for pointers the loop wrote into C values.
    values: Vec<Vec<DdValue>>,
    bytes: Vec<Vec<u8>>,
}

impl RunArena {
    /// Memory for `size` bytes, zeroed and 16-byte aligned, valid until the arena is dropped.
    pub fn alloc(&mut self, size: usize) -> *mut u8 {
        let size = size.max(1).div_ceil(UNIT) * UNIT;
        let room = self
            .blocks
            .last()
            .map_or(0, |block| block.len() * UNIT - self.used);
        if room < size {
            self.blocks.push(vec![0; size.max(BLOCK_BYTES) / UNIT]);
            self.used = 0;
        }
        let Some(block) = self.blocks.last_mut() else {
            return ptr::null_mut();
        };
        let out = block.as_mut_ptr().cast::<u8>().wrapping_add(self.used);
        self.used += size;
        out
    }

    /// Keeps `values` for as long as the arena lives, and returns the pointer to their first.
    pub fn keep_values(&mut self, mut values: Vec<DdValue>) -> *mut DdValue {
        let first = values.as_mut_ptr();
        self.values.push(values);
        first
    }

    /// Keeps `bytes` for as long as the arena lives, and returns the pointer to their first.
    pub fn keep_bytes(&mut self, mut bytes: Vec<u8>) -> *mut u8 {
        let first = bytes.as_mut_ptr();
        self.bytes.push(bytes);
        first
    }
}
