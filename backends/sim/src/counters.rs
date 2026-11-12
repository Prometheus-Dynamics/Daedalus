//! Which steps ran, with no heap.

use std::sync::atomic::{AtomicU32, Ordering};

const SLOTS: usize = 64;
static NODES: [AtomicU32; SLOTS] = [const { AtomicU32::new(0) }; SLOTS];
static TRANSFERS: [AtomicU32; 4] = [const { AtomicU32::new(0) }; 4];

pub(crate) fn bump_node(node: u32) {
    NODES[node as usize % SLOTS].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn bump_transfer(index: usize) {
    TRANSFERS[index].fetch_add(1, Ordering::Relaxed);
}

/// How many times the node has run in this process. Nodes share a slot modulo 64.
pub fn ran(node: u32) -> u32 {
    NODES[node as usize % SLOTS].load(Ordering::Relaxed)
}
