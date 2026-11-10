//! Plain data: a repr(C) type that crosses as its bytes, tagged with its type key and layout hash.
//! A receiver whose type has another layout refuses it, and the error names both hashes.

use std::fmt;

use crate::owned::SharedBytes;

/// A plain-data block: its type key, its layout hash, and its bytes.
#[derive(Clone, Debug)]
pub struct PodBlock {
    key: String,
    layout: u64,
    bytes: SharedBytes,
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
        }
    }
}

impl std::error::Error for PodError {}

impl PodBlock {
    pub fn new(key: &str, layout: u64, bytes: SharedBytes) -> PodBlock {
        PodBlock {
            key: key.to_owned(),
            layout,
            bytes,
        }
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
        Ok(self.bytes.bytes())
    }
}
