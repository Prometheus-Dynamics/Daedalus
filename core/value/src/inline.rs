//! Small values stored in place. Scalars go in through `to_le_bytes`.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InlineBytes<const N: usize> {
    len: u8,
    bytes: [u8; N],
}

impl<const N: usize> InlineBytes<N> {
    const FITS: () = assert!(N <= 16, "an inline value holds at most 16 bytes");

    pub const fn new() -> Self {
        let () = Self::FITS;
        InlineBytes {
            len: 0,
            bytes: [0; N],
        }
    }

    /// `None` when `value` doesn't fit.
    pub fn from_slice(value: &[u8]) -> Option<Self> {
        if value.len() > N {
            return None;
        }
        let mut out = Self::new();
        out.bytes[..value.len()].copy_from_slice(value);
        out.len = value.len() as u8;
        Some(out)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl<const N: usize> Default for InlineBytes<N> {
    fn default() -> Self {
        Self::new()
    }
}
