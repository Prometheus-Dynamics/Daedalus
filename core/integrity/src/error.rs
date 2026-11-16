//! Why a file was refused, or a lock could not be read or written. Each message names the file.

use std::fmt;
use std::path::PathBuf;

use crate::digest::Digest;

#[derive(Debug)]
#[non_exhaustive]
pub enum IntegrityError {
    /// The host cannot check files here: Locked and Record need Linux and `/proc/self/fd`.
    Unsupported { what: &'static str },
    /// The file could not be opened.
    Open { path: PathBuf, message: String },
    /// The file could not be read to hash it.
    Read { path: PathBuf, message: String },
    /// The path is not a regular file.
    NotRegular { path: PathBuf },
    /// The file name is not UTF-8, so it cannot be a key in a lock.
    BadName { path: PathBuf },
    /// The lock lists the file under another digest.
    Mismatch {
        path: PathBuf,
        lock: PathBuf,
        key: String,
        expected: Digest,
        found: Digest,
    },
    /// The lock has no entry for the file.
    NotListed {
        path: PathBuf,
        lock: PathBuf,
        key: String,
        found: Digest,
    },
    /// The program starts with `#!`, so the kernel would run its interpreter, not the file.
    Script { path: PathBuf, interpreter: String },
    /// The library mapped at load is not the file that was hashed.
    NotLoaded { path: PathBuf },
    /// The lock is malformed. The line number is 1-based.
    Lock {
        lock: PathBuf,
        line: usize,
        why: String,
    },
    /// The lock could not be read.
    LockRead { lock: PathBuf, message: String },
    /// The lock could not be written, or was changed by someone else while it was written.
    Write { lock: PathBuf, message: String },
    /// Recording would change an entry that is already in the lock. Use `update` to approve it.
    Conflict {
        lock: PathBuf,
        key: String,
        recorded: Digest,
        found: Digest,
    },
}

impl fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntegrityError::Unsupported { what } => write!(
                f,
                "{what} needs Linux, where /proc/self/fd is; the integrity mode is not supported here"
            ),
            IntegrityError::Open { path, message } => {
                write!(f, "{}: cannot open: {message}", path.display())
            }
            IntegrityError::Read { path, message } => {
                write!(f, "{}: cannot read: {message}", path.display())
            }
            IntegrityError::NotRegular { path } => {
                write!(f, "{} is not a regular file", path.display())
            }
            IntegrityError::BadName { path } => {
                write!(f, "{}: the file name is not UTF-8", path.display())
            }
            IntegrityError::Mismatch {
                path,
                lock,
                key,
                expected,
                found,
            } => write!(
                f,
                "{} is not the file the lock approves: {} expects sha256 {expected} for {key}, \
                 this file is {found}. Install the approved file, or run `daedalus-lock update` \
                 if this one is meant.",
                path.display(),
                lock.display()
            ),
            IntegrityError::NotListed {
                path,
                lock,
                key,
                found,
            } => write!(
                f,
                "{} is not in the lock {} (no entry {key}); its sha256 is {found}. Add it with \
                 `daedalus-lock record`.",
                path.display(),
                lock.display()
            ),
            IntegrityError::Script { path, interpreter } => write!(
                f,
                "{} is a script: the kernel would run {interpreter}, which is not what was \
                 hashed. Name the interpreter as the program and add the script with `also_check`",
                path.display()
            ),
            IntegrityError::NotLoaded { path } => write!(
                f,
                "{}: the library mapped at load is not the file that was checked; it changed \
                 between the check and the load",
                path.display()
            ),
            IntegrityError::Lock { lock, line, why } => {
                write!(f, "{}: line {line}: {why}", lock.display())
            }
            IntegrityError::LockRead { lock, message } => {
                write!(f, "{}: cannot read the lock: {message}", lock.display())
            }
            IntegrityError::Write { lock, message } => {
                write!(f, "{}: cannot write the lock: {message}", lock.display())
            }
            IntegrityError::Conflict {
                lock,
                key,
                recorded,
                found,
            } => write!(
                f,
                "{} already has {key} as sha256 {recorded}, this file is {found}: run \
                 `daedalus-lock update` to approve it",
                lock.display()
            ),
        }
    }
}

impl std::error::Error for IntegrityError {}
