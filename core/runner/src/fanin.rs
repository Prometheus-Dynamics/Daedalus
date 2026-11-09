//! Fan-in ports: `ins0`, `ins1`, ... The order comes from the numeric suffix and is worked out once
//! at load, so the tick never sorts.

#![cfg(feature = "alloc")]

use alloc::vec::Vec;

/// The numeric suffix of `port` after `prefix`, if it is a port of that fan-in.
fn suffix(prefix: &str, port: &str) -> Option<u32> {
    let digits = port.strip_prefix(prefix)?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The indexes of the `ports` that belong to the fan-in `prefix`, ordered by numeric suffix.
pub fn fanin_order(prefix: &str, ports: &[&str]) -> Vec<u32> {
    let mut found: Vec<(u32, u32)> = ports
        .iter()
        .enumerate()
        .filter_map(|(at, port)| Some((suffix(prefix, port)?, at as u32)))
        .collect();
    found.sort_unstable();
    found.into_iter().map(|(_, at)| at).collect()
}
