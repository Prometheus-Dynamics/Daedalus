//! Standard base64 (RFC 4648 alphabet, canonical `=` padding), as the `base64` crate's
//! `STANDARD` engine: no whitespace, padding required, no stray bits in the last symbol.
//! The bulk of both directions is branch-free per 3-byte / 4-symbol group so it vectorizes.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use crate::multiversion;

/// A malformed base64 input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Base64Error {
    /// The length is not a multiple of 4.
    InvalidLength,
    /// A byte outside the alphabet (or `=` before the end).
    InvalidByte { offset: usize, byte: u8 },
    /// The last symbol has bits set that the padding says are unused.
    InvalidLastSymbol { offset: usize, byte: u8 },
}

impl core::fmt::Display for Base64Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Base64Error::InvalidLength => f.write_str("invalid base64 length"),
            Base64Error::InvalidByte { offset, byte } => {
                write!(f, "invalid base64 byte {byte:#04x} at offset {offset}")
            }
            Base64Error::InvalidLastSymbol { offset, byte } => {
                write!(
                    f,
                    "invalid base64 last symbol {byte:#04x} at offset {offset}"
                )
            }
        }
    }
}

impl core::error::Error for Base64Error {}

/// Encoded length of `n` bytes, padding included.
pub const fn base64_encoded_len(n: usize) -> usize {
    n.div_ceil(3) * 4
}

/// Encode `src` into `dst`, which must hold [`base64_encoded_len`] bytes. Returns that length.
///
/// # Panics
/// When `dst` is too short.
pub fn base64_encode_into(src: &[u8], dst: &mut [u8]) -> usize {
    let len = base64_encoded_len(src.len());
    assert!(
        dst.len() >= len,
        "base64_encode_into: dst holds {} of {len} bytes",
        dst.len()
    );
    let full = src.len() / 3;
    encode_groups(&src[..full * 3], &mut dst[..full * 4]);
    let rest = &src[full * 3..];
    let tail = &mut dst[full * 4..len];
    match *rest {
        [a] => tail.copy_from_slice(&[sym(a >> 2), sym((a & 3) << 4), b'=', b'=']),
        [a, b] => tail.copy_from_slice(&[
            sym(a >> 2),
            sym(((a & 3) << 4) | (b >> 4)),
            sym((b & 0xf) << 2),
            b'=',
        ]),
        _ => {}
    }
    len
}

/// Encode `src` as a `String`.
pub fn base64_encode(src: &[u8]) -> String {
    let mut out = alloc::vec![0u8; base64_encoded_len(src.len())];
    base64_encode_into(src, &mut out);
    // The alphabet is ASCII.
    String::from_utf8(out).unwrap_or_default()
}

multiversion! {
    /// Whole 3-byte groups to 4 symbols each.
    pub fn encode_groups(src: &[u8], dst: &mut [u8]) -> () {
        for (s, d) in src.as_chunks::<3>().0.iter().zip(dst.as_chunks_mut::<4>().0) {
            let n = (u32::from(s[0]) << 16) | (u32::from(s[1]) << 8) | u32::from(s[2]);
            *d = [
                sym((n >> 18) as u8),
                sym(((n >> 12) & 63) as u8),
                sym(((n >> 6) & 63) as u8),
                sym((n & 63) as u8),
            ];
        }
    }
}

/// The symbol for a 6-bit value, branch-free.
#[inline(always)]
fn sym(v: u8) -> u8 {
    let mut c = v.wrapping_add(b'A');
    c = c.wrapping_add(if v >= 26 { 6 } else { 0 }); // a-z
    c = c.wrapping_sub(if v >= 52 { 75 } else { 0 }); // 0-9
    c = c.wrapping_sub(if v == 62 { 15 } else { 0 }); // '+'
    c.wrapping_sub(if v == 63 { 12 } else { 0 }) // '/'
}

/// The 6-bit value of a symbol, and 0xff for anything outside the alphabet (`=` included).
#[inline(always)]
fn value(c: u8) -> u8 {
    let upper = c.wrapping_sub(b'A') < 26;
    let lower = c.wrapping_sub(b'a') < 26;
    let digit = c.wrapping_sub(b'0') < 10;
    let mut v = 0xffu8;
    v = if upper { c.wrapping_sub(b'A') } else { v };
    v = if lower { c.wrapping_sub(b'a' - 26) } else { v };
    v = if digit { c.wrapping_add(52 - b'0') } else { v };
    v = if c == b'+' { 62 } else { v };
    if c == b'/' { 63 } else { v }
}

/// Decoded length of `src`, after checking its length and padding.
pub fn base64_decoded_len(src: &[u8]) -> Result<usize, Base64Error> {
    if !src.len().is_multiple_of(4) {
        return Err(Base64Error::InvalidLength);
    }
    let pad = match src {
        [.., b'=', b'='] => 2,
        [.., b'='] => 1,
        _ => 0,
    };
    Ok(src.len() / 4 * 3 - pad)
}

/// Decode `src` into `dst`, which must hold [`base64_decoded_len`] bytes. Returns that length.
///
/// # Panics
/// When `dst` is too short.
pub fn base64_decode_into(src: &[u8], dst: &mut [u8]) -> Result<usize, Base64Error> {
    let len = base64_decoded_len(src)?;
    assert!(
        dst.len() >= len,
        "base64_decode_into: dst holds {} of {len} bytes",
        dst.len()
    );
    if src.is_empty() {
        return Ok(0);
    }
    let body = src.len() / 4 - 1;
    if !decode_groups(&src[..body * 4], &mut dst[..body * 3]) {
        return Err(first_invalid(&src[..body * 4], 0));
    }
    let last = &src[body * 4..];
    let at = body * 4;
    let pad = len - body * 3;
    let pad = 3 - pad; // '=' count: 0, 1 or 2
    let mut v = [0u8; 4];
    for (i, &c) in last[..4 - pad].iter().enumerate() {
        v[i] = value(c);
        if v[i] == 0xff {
            return Err(Base64Error::InvalidByte {
                offset: at + i,
                byte: c,
            });
        }
    }
    let n = (u32::from(v[0]) << 18)
        | (u32::from(v[1]) << 12)
        | (u32::from(v[2]) << 6)
        | u32::from(v[3]);
    let bytes = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
    let unused = match pad {
        2 => n & 0xffff,
        1 => n & 0xff,
        _ => 0,
    };
    if unused != 0 {
        let i = 3 - pad;
        return Err(Base64Error::InvalidLastSymbol {
            offset: at + i,
            byte: last[i],
        });
    }
    dst[body * 3..len].copy_from_slice(&bytes[..3 - pad]);
    Ok(len)
}

/// Decode `src` into a new `Vec`.
pub fn base64_decode(src: &[u8]) -> Result<Vec<u8>, Base64Error> {
    let mut out = alloc::vec![0u8; base64_decoded_len(src)?];
    base64_decode_into(src, &mut out)?;
    Ok(out)
}

multiversion! {
    /// Whole 4-symbol groups to 3 bytes each; `false` when a symbol is outside the alphabet.
    pub fn decode_groups(src: &[u8], dst: &mut [u8]) -> bool {
        let mut bad = 0u8;
        for (s, d) in src.as_chunks::<4>().0.iter().zip(dst.as_chunks_mut::<3>().0) {
            let v = [value(s[0]), value(s[1]), value(s[2]), value(s[3])];
            bad |= v[0] | v[1] | v[2] | v[3];
            let n = (u32::from(v[0] & 63) << 18)
                | (u32::from(v[1] & 63) << 12)
                | (u32::from(v[2] & 63) << 6)
                | u32::from(v[3] & 63);
            *d = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        }
        // Valid values are below 64; 0xff marks an invalid symbol.
        bad < 64
    }
}

fn first_invalid(src: &[u8], base: usize) -> Base64Error {
    let (offset, &byte) = src
        .iter()
        .enumerate()
        .find(|&(_, &c)| value(c) == 0xff)
        .unwrap_or((0, &0));
    Base64Error::InvalidByte {
        offset: base + offset,
        byte,
    }
}
