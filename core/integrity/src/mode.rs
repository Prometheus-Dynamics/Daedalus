//! The host's integrity mode, and how it opens files.

use std::path::{Path, PathBuf};

use crate::checked::Checked;
use crate::error::IntegrityError;

/// What a host does with the files it loads or runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mode {
    /// No check. The default.
    #[default]
    Off,
    /// A file is loaded only if the lock approves its digest.
    Locked,
    /// Files load as they do under Off, and each one that loads is recorded in the lock.
    Record,
}

/// The mode, and the lock it reads or writes.
#[derive(Clone, Debug)]
pub struct Integrity {
    mode: Mode,
    lock: PathBuf,
}

impl Default for Integrity {
    fn default() -> Integrity {
        Integrity::off()
    }
}

impl Integrity {
    /// No check.
    pub fn off() -> Integrity {
        Integrity {
            mode: Mode::Off,
            lock: PathBuf::new(),
        }
    }

    /// Loads only what `lock` approves. Needs Linux.
    pub fn locked(lock: impl Into<PathBuf>) -> Result<Integrity, IntegrityError> {
        supported()?;
        Ok(Integrity {
            mode: Mode::Locked,
            lock: lock.into(),
        })
    }

    /// Records what loads into `lock`. Needs Linux.
    pub fn record(lock: impl Into<PathBuf>) -> Result<Integrity, IntegrityError> {
        supported()?;
        Ok(Integrity {
            mode: Mode::Record,
            lock: lock.into(),
        })
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// The lock file, or `None` when the mode is Off.
    pub fn lock_path(&self) -> Option<&Path> {
        (self.mode != Mode::Off).then_some(self.lock.as_path())
    }

    /// Opens `path` for loading or running. Off gives a `Checked` that holds nothing and names
    /// `path` as it is. Locked reads the lock, hashes the open file, and refuses it unless the
    /// lock approves that digest. Record hashes the file and leaves the lock to
    /// [`Checked::record`].
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Checked, IntegrityError> {
        Checked::open(self.mode, &self.lock, path.as_ref())
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn supported() -> Result<(), IntegrityError> {
    if Path::new("/proc/self/fd").is_dir() {
        Ok(())
    } else {
        Err(IntegrityError::Unsupported {
            what: "plugin integrity",
        })
    }
}

#[cfg(not(all(unix, target_os = "linux")))]
fn supported() -> Result<(), IntegrityError> {
    Err(IntegrityError::Unsupported {
        what: "plugin integrity",
    })
}
