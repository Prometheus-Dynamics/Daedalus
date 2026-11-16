//! SHA-256 digests, and their text form: 64 lowercase hex digits.

use std::fmt;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Seek, SeekFrom};
use std::str::FromStr;

use sha2::{Digest as _, Sha256};

/// A SHA-256 digest: 32 bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// The digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Digest {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Digest(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Digest({self})")
    }
}

/// The text is not 64 lowercase hex digits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDigestError;

impl fmt::Display for ParseDigestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a sha256 digest is 64 lowercase hex digits")
    }
}

impl std::error::Error for ParseDigestError {}

impl FromStr for Digest {
    type Err = ParseDigestError;

    fn from_str(text: &str) -> Result<Digest, ParseDigestError> {
        let bytes = text.as_bytes();
        let hex = |c: u8| matches!(c, b'0'..=b'9' | b'a'..=b'f');
        if bytes.len() != 64 || !bytes.iter().all(|&c| hex(c)) {
            return Err(ParseDigestError);
        }
        let mut out = [0u8; 32];
        let (pairs, _) = bytes.as_chunks::<2>();
        for (slot, pair) in out.iter_mut().zip(pairs) {
            *slot = (nibble(pair[0]) << 4) | nibble(pair[1]);
        }
        Ok(Digest(out))
    }
}

fn nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        _ => c - b'a' + 10,
    }
}

/// Hashes everything in `file`, from its start, and returns the digest with the first two bytes
/// of the file. Those two bytes say whether the file starts with `#!`.
pub fn hash_file(file: &File) -> io::Result<(Digest, [u8; 2])> {
    let mut reader: &File = file;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut head = [0u8; 2];
    let mut head_len = 0;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if head_len < head.len() {
            let take = (head.len() - head_len).min(read);
            head[head_len..head_len + take].copy_from_slice(&buffer[..take]);
            head_len += take;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((Digest(hasher.finalize().into()), head))
}
