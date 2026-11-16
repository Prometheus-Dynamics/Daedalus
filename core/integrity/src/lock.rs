//! The lock file: one line per approved file, in the form `sha256sum` writes.
//!
//! ```text
//! # daedalus.lock 1
//! 9f86d081...(64 hex)  libfoo.so
//! 2c26b46b...(64 hex)  /opt/daedalus/bin/python3
//! ```
//!
//! The header is required. Each entry is 64 lowercase hex digits, two spaces, then the key. Lines
//! end in LF, are sorted by key, and no key repeats. A key with a `/` is an absolute path and
//! matches exactly; any other key is a file name.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::io;
use std::path::Path;

use crate::digest::Digest;
use crate::error::IntegrityError;

pub const LOCK_HEADER: &str = "# daedalus.lock 1";

/// The lock's entries, by key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Lock {
    entries: BTreeMap<String, Digest>,
}

/// The lock is malformed. `line` is 1-based.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LockError {
    pub line: usize,
    pub why: String,
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.why)
    }
}

impl std::error::Error for LockError {}

impl Lock {
    pub fn new() -> Lock {
        Lock::default()
    }

    /// Reads a lock from its text. Any violation of the format is an error with its line.
    pub fn parse(text: &str) -> Result<Lock, LockError> {
        let fail = |line: usize, why: &str| LockError {
            line,
            why: why.to_string(),
        };
        let body = match text.strip_suffix('\n') {
            Some(body) => body,
            None if text.is_empty() => return Err(fail(1, "the header is missing")),
            None => return Err(fail(text.lines().count().max(1), "the last line has no LF")),
        };
        let mut lines = body
            .split('\n')
            .enumerate()
            .map(|(index, line)| (index + 1, line));
        match lines.next() {
            Some((_, LOCK_HEADER)) => {}
            Some((line, _)) => {
                return Err(fail(line, "the first line must be `# daedalus.lock 1`"));
            }
            None => return Err(fail(1, "the header is missing")),
        }
        let mut entries = BTreeMap::new();
        let mut previous: Option<String> = None;
        for (number, line) in lines {
            if line.contains('\r') {
                return Err(fail(number, "lines end in LF, not CRLF"));
            }
            if line.starts_with('#') || line.is_empty() {
                return Err(fail(number, "only the header may be a comment or be empty"));
            }
            let Some((hex, rest)) = line.split_at_checked(64) else {
                return Err(fail(number, "expected 64 hex digits, two spaces and a key"));
            };
            let Some(key) = rest.strip_prefix("  ") else {
                return Err(fail(number, "expected two spaces after the digest"));
            };
            let digest: Digest = hex
                .parse()
                .map_err(|_| fail(number, "the digest is not 64 lowercase hex digits"))?;
            if key.is_empty() || key.chars().any(|c| c.is_control() || c == '\\') {
                return Err(fail(
                    number,
                    "the key is empty or has a control character or `\\`",
                ));
            }
            if let Some(last) = &previous {
                if key == last {
                    return Err(fail(number, "this key is repeated"));
                }
                if key < last.as_str() {
                    return Err(fail(number, "entries must be sorted by key"));
                }
            }
            previous = Some(key.to_string());
            entries.insert(key.to_string(), digest);
        }
        Ok(Lock { entries })
    }

    /// The lock's text. Parsing it gives back the same lock.
    pub fn render(&self) -> String {
        let mut out = format!("{LOCK_HEADER}\n");
        for (key, digest) in &self.entries {
            let _ = writeln!(out, "{digest}  {key}");
        }
        out
    }

    /// The entries, in key order.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &Digest)> {
        self.entries
            .iter()
            .map(|(key, digest)| (key.as_str(), digest))
    }

    pub fn get(&self, key: &str) -> Option<&Digest> {
        self.entries.get(key)
    }

    /// Sets the entry for `key`. The caller has checked the key is valid.
    pub fn set(&mut self, key: String, digest: Digest) {
        self.entries.insert(key, digest);
    }

    /// The entry that approves the file at `path`, and its key. An absolute key matching `path`
    /// exactly wins; otherwise the key is the file name of `path`.
    pub fn expected_for(&self, path: &Path) -> Option<(&str, Digest)> {
        if let Some(full) = path.to_str()
            && let Some((key, digest)) = self.entries.get_key_value(full)
            && key.contains('/')
        {
            return Some((key.as_str(), *digest));
        }
        let name = path.file_name()?.to_str()?;
        let (key, digest) = self.entries.get_key_value(name)?;
        Some((key.as_str(), *digest))
    }
}

/// Reads and parses the lock at `path`.
pub fn read_lock(path: &Path) -> Result<Lock, IntegrityError> {
    let text =
        std::fs::read_to_string(path).map_err(|error: io::Error| IntegrityError::LockRead {
            lock: path.to_path_buf(),
            message: error.to_string(),
        })?;
    Lock::parse(&text).map_err(|error| IntegrityError::Lock {
        lock: path.to_path_buf(),
        line: error.line,
        why: error.why,
    })
}
