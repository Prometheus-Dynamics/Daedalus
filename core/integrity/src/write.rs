//! Writing entries into a lock, safely when several writers share it.

use std::path::Path;

use crate::digest::Digest;
use crate::error::IntegrityError;
#[cfg(unix)]
use crate::lock::Lock;

/// Adds `entries` to the lock at `lock_path`, creating the lock if it is missing. An entry the
/// lock already has under the same digest is kept. An entry with another digest is a conflict,
/// and then nothing is written: approving a changed file is [`update`].
#[cfg(unix)]
pub fn merge(lock_path: &Path, entries: &[(String, Digest)]) -> Result<(), IntegrityError> {
    write_entries(lock_path, entries, false)
}

/// Sets each of `entries` in the lock at `lock_path`, replacing an entry that has another digest.
/// This is how a changed file is approved.
#[cfg(unix)]
pub fn update(lock_path: &Path, entries: &[(String, Digest)]) -> Result<(), IntegrityError> {
    write_entries(lock_path, entries, true)
}

/// The write takes an exclusive lock on the lock's directory, so a host, a worker and the CLI
/// cannot lose each other's entries. It writes a temporary file and renames it over the lock, so
/// a reader sees the old lock or the new one, never a part of one.
#[cfg(unix)]
fn write_entries(
    lock_path: &Path,
    entries: &[(String, Digest)],
    overwrite: bool,
) -> Result<(), IntegrityError> {
    use std::fs::{self, File, OpenOptions};
    use std::io::Write as _;

    let write_error = |message: String| IntegrityError::Write {
        lock: lock_path.to_path_buf(),
        message,
    };
    let dir = match lock_path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let dir_handle = File::open(dir).map_err(|error| write_error(error.to_string()))?;
    dir_handle
        .lock()
        .map_err(|error| write_error(error.to_string()))?;

    let mut lock = match fs::read_to_string(lock_path) {
        Ok(text) => crate::lock::Lock::parse(&text).map_err(|error| IntegrityError::Lock {
            lock: lock_path.to_path_buf(),
            line: error.line,
            why: error.why,
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Lock::new(),
        Err(error) => {
            return Err(IntegrityError::LockRead {
                lock: lock_path.to_path_buf(),
                message: error.to_string(),
            });
        }
    };
    for (key, found) in entries {
        if let Some(recorded) = lock
            .get(key)
            .filter(|recorded| *recorded != found && !overwrite)
        {
            return Err(IntegrityError::Conflict {
                lock: lock_path.to_path_buf(),
                key: key.clone(),
                recorded: *recorded,
                found: *found,
            });
        }
        lock.set(key.clone(), *found);
    }

    let tmp = dir.join(format!(".daedalus.lock.{}.tmp", std::process::id()));
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(lock.render().as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, lock_path)
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&tmp);
        return Err(write_error(error.to_string()));
    }
    Ok(())
}

/// Writing a lock needs Unix, where the directory is locked while it is written.
#[cfg(not(unix))]
pub fn merge(lock_path: &Path, _entries: &[(String, Digest)]) -> Result<(), IntegrityError> {
    let _ = lock_path;
    Err(IntegrityError::Unsupported {
        what: "writing a lock",
    })
}

/// Writing a lock needs Unix, where the directory is locked while it is written.
#[cfg(not(unix))]
pub fn update(lock_path: &Path, _entries: &[(String, Digest)]) -> Result<(), IntegrityError> {
    let _ = lock_path;
    Err(IntegrityError::Unsupported {
        what: "writing a lock",
    })
}
