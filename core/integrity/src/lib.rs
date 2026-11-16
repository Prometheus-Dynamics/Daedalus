//! Plugin integrity: which library files and worker programs a host approves.
//!
//! A `daedalus.lock` file lists the SHA-256 digest of each approved file. A host in Locked mode
//! opens a file once, hashes the open file, and loads or runs that same open file, so the bytes
//! checked are the bytes used. Record mode writes the digests of what loads, for a user to read.
//! Off, the default, checks nothing.
//!
//! The check covers only the named file. See `docs/plugins.md` for what it does not cover.

#![forbid(unsafe_code)]
// The errors carry the paths and digests they name, so they are large. They are returned at
// load time, not on a hot path, and the messages are worth the size.
#![allow(clippy::result_large_err)]

mod checked;
mod digest;
mod error;
mod lock;
mod mode;
mod program;
mod write;

pub use checked::{Checked, dlopen_name};
pub use digest::{Digest, ParseDigestError, hash_file};
pub use error::IntegrityError;
pub use lock::{LOCK_HEADER, Lock, LockError, read_lock};
pub use mode::{Integrity, Mode};
pub use program::find_program;
pub use write::{merge, update};
