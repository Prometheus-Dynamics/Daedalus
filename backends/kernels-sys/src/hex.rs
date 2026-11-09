//! Lowercase hex encoding.

use crate::multiversion;

multiversion! {
    /// Lowercase hex of `src` into `dst` (`2 * src.len()` bytes; extra source bytes are skipped
    /// when `dst` is shorter).
    pub fn hex_lower(src: &[u8], dst: &mut [u8]) -> () {
        for (&b, d) in src.iter().zip(dst.as_chunks_mut::<2>().0) {
            *d = [nibble(b >> 4), nibble(b & 0xf)];
        }
    }
}

/// `0-9a-f` for a nibble, branch-free.
#[inline(always)]
fn nibble(n: u8) -> u8 {
    n + b'0' + if n > 9 { b'a' - b'0' - 10 } else { 0 }
}
