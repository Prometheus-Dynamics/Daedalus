//! Blocks from a dma-heap, which devices can import.

use daedalus_dma_sys::{DmaBuf, DmaHeap, Error};

pub(crate) fn allocate(heap: &str, len: u64) -> Result<DmaBuf, Error> {
    DmaHeap::open(heap)?.allocate(len)
}
