//! Bytes in graph JSON. Speed doesn't matter while planning.

#![cfg(feature = "alloc")]

use alloc::string::String;
use alloc::vec::Vec;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serializer};

const DIGITS: &[u8; 16] = b"0123456789abcdef";

pub fn serialize<S: Serializer>(bytes: &[u8], out: S) -> Result<S::Ok, S::Error> {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out.serialize_str(&text)
}

fn digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

pub fn deserialize<'de, D: Deserializer<'de>>(input: D) -> Result<Vec<u8>, D::Error> {
    let text = String::deserialize(input)?;
    if text.len() % 2 != 0 {
        return Err(D::Error::custom("hex bytes must have an even length"));
    }
    let (pairs, _) = text.as_bytes().as_chunks::<2>();
    pairs
        .iter()
        .map(|&[high, low]| match (digit(high), digit(low)) {
            (Some(high), Some(low)) => Ok(high << 4 | low),
            _ => Err(D::Error::custom("invalid hex digit")),
        })
        .collect()
}
