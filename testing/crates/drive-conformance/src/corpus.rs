//! The corpus files: `core/wire/tests/corpus/service/{client,server}.bin`. Each body is a `u32`
//! little-endian length, then the body.

use std::path::PathBuf;

/// The corpus file for `kind` (`client` or `server`).
pub fn path(kind: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../core/wire/tests/corpus/service")
        .join(format!("{kind}.bin"))
}

/// The bodies in a corpus file's bytes.
pub fn bodies(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut out = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let Some((head, tail)) = rest.split_first_chunk::<4>() else {
            return Err("a body length is cut short".to_owned());
        };
        let len = u32::from_le_bytes(*head) as usize;
        if tail.len() < len {
            return Err("a body is cut short".to_owned());
        }
        out.push(tail[..len].to_vec());
        rest = &tail[len..];
    }
    Ok(out)
}
