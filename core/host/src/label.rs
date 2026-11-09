//! A name copied into a fixed buffer, so an error can say what went wrong without allocating.

use core::fmt;

/// Longest name an error can carry, in bytes. Longer names are cut on a character boundary.
pub const LABEL_BYTES: usize = 48;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Label {
    len: u8,
    bytes: [u8; LABEL_BYTES],
}

impl Label {
    pub fn new(text: &str) -> Label {
        let mut end = text.len().min(LABEL_BYTES);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let mut bytes = [0; LABEL_BYTES];
        bytes[..end].copy_from_slice(&text.as_bytes()[..end]);
        Label {
            len: end as u8,
            bytes,
        }
    }

    pub fn as_str(&self) -> &str {
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}
