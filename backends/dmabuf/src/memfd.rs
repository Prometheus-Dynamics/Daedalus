//! Blocks from anonymous memory. No device is involved, so this works anywhere Linux does.

use daedalus_dma_sys::{DmaBuf, Error};

pub(crate) fn allocate(len: u64) -> Result<DmaBuf, Error> {
    DmaBuf::memfd("daedalus-block", len)
}
