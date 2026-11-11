//! Plain data: a type whose bytes are its value, tagged with its key and layout hash. A receiver
//! whose type has another layout refuses the block, and the error names both hashes.

use std::fmt;
use std::mem::size_of;
use std::ptr;
use std::sync::Arc;

use daedalus_abi::PlainData;

/// A plain-data block: its type key, its layout hash, and a copy of its bytes.
#[derive(Clone, Debug)]
pub struct PodBlock {
    key: String,
    layout: u64,
    bytes: Arc<[u8]>,
}

/// Why a block was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodError {
    /// The block is of another type key.
    Key { expected: String, found: String },
    /// The block has the same key under another layout. Both hashes are named.
    Layout {
        key: String,
        expected: u64,
        found: u64,
    },
    /// The block's bytes are not the size of the type it is read as.
    Size {
        key: String,
        expected: usize,
        found: usize,
    },
}

impl fmt::Display for PodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PodError::Key { expected, found } => {
                write!(f, "a block of type {found} is not {expected}")
            }
            PodError::Layout {
                key,
                expected,
                found,
            } => write!(
                f,
                "type {key} has layout {found:#018x} here, the receiver expects {expected:#018x}"
            ),
            PodError::Size {
                key,
                expected,
                found,
            } => write!(
                f,
                "a block of type {key} has {found} bytes, the receiver expects {expected}"
            ),
        }
    }
}

impl std::error::Error for PodError {}

impl PodBlock {
    /// A block of type `key` and `layout`, holding a copy of `bytes`.
    pub fn new(key: &str, layout: u64, bytes: &[u8]) -> PodBlock {
        PodBlock {
            key: key.to_owned(),
            layout,
            bytes: Arc::from(bytes),
        }
    }

    /// A block holding a copy of `value`, named by its type's key and layout.
    pub fn of<T: PlainData>(value: &T) -> PodBlock {
        // SAFETY: `T` is plain data, so each of its `size_of::<T>()` bytes is part of the value.
        // `value` is a valid reference for that many bytes, and the slice does not outlive it.
        let bytes =
            unsafe { std::slice::from_raw_parts((value as *const T).cast::<u8>(), size_of::<T>()) };
        PodBlock::new(T::KEY, T::LAYOUT, bytes)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// The block's bytes, if its key and layout are the receiver's.
    pub fn accept(&self, key: &str, layout: u64) -> Result<&[u8], PodError> {
        if self.key != key {
            return Err(PodError::Key {
                expected: key.to_owned(),
                found: self.key.clone(),
            });
        }
        if self.layout != layout {
            return Err(PodError::Layout {
                key: key.to_owned(),
                expected: layout,
                found: self.layout,
            });
        }
        Ok(&self.bytes)
    }

    /// The block as a value of `T`, if its key and layout are `T`'s and its size is `T`'s.
    pub fn read<T: PlainData>(&self) -> Result<T, PodError> {
        let bytes = self.accept(T::KEY, T::LAYOUT)?;
        if bytes.len() != size_of::<T>() {
            return Err(PodError::Size {
                key: T::KEY.to_owned(),
                expected: size_of::<T>(),
                found: bytes.len(),
            });
        }
        // SAFETY: the length is `T`'s size, and `T` is plain data, so any bytes of that length are
        // a valid `T`. The read is unaligned, so the bytes need no alignment.
        Ok(unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<T>()) })
    }
}
