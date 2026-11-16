//! A file opened for loading or running, with its digest, and the names it can be given.
//!
//! The file is kept open, so its number is not reused, and the name `/proc/self/fd/N` always
//! means this file. Each name made by [`Checked::dlopen_path`] is unique for the process, because
//! the dynamic loader matches a name against the libraries it has loaded before it opens
//! anything: a reused name would hand back the old library.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::digest::{Digest, hash_file};
use crate::error::IntegrityError;
use crate::lock::read_lock;
use crate::mode::Mode;
use crate::write::merge;

/// Gives each `Checked` a serial of its own, so no two dlopen names in this process are alike.
static NEXT_SERIAL: AtomicU64 = AtomicU64::new(0);

/// A file opened for loading or running. Off mode holds no file and no digest.
#[derive(Debug)]
pub struct Checked {
    path: PathBuf,
    file: Option<File>,
    digest: Option<Digest>,
    serial: u64,
    /// The lock a Record writes this file into, once it has loaded.
    record_to: Option<PathBuf>,
}

impl Checked {
    /// The file as it was named, which is also the name a worker's `argv[0]` takes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The file's digest, when the mode checks files.
    pub fn digest(&self) -> Option<&Digest> {
        self.digest.as_ref()
    }

    /// The key a lock uses for this file: its file name.
    pub fn key(&self) -> Result<String, IntegrityError> {
        file_key(&self.path)
    }

    /// The interpreter a `#!` file names, or `None` when the file does not start with `#!`.
    pub fn interpreter(&self) -> Option<String> {
        self.file.as_ref()?;
        self.interpreter_text()
    }

    /// The name to load the library by. Off gives the path as it was named. Otherwise it is the
    /// open file's number, under a name unique to this `Checked`.
    pub fn dlopen_path(&self) -> PathBuf {
        match self.fd() {
            Some(fd) => dlopen_name(self.serial, fd),
            None => self.path.clone(),
        }
    }

    /// The name to execute: the open file, by its number. The number stays close-on-exec, so it
    /// is gone from a program the file runs; this only works for ELF files, not `#!` scripts.
    pub fn exec_path(&self) -> PathBuf {
        match self.fd() {
            Some(fd) => PathBuf::from(format!("/proc/self/fd/{fd}")),
            None => self.path.clone(),
        }
    }

    /// Takes the open file out, to be kept for as long as a library loaded from it lives.
    pub fn take_file(&mut self) -> Option<File> {
        self.file.take()
    }

    /// Checks that the library mapped at `addr` is the file that was checked. Call it after the
    /// library is loaded, with an address inside it.
    pub fn confirm_mapped(&self, addr: usize) -> Result<(), IntegrityError> {
        match &self.file {
            None => Ok(()),
            Some(file) => confirm_mapped(file, addr, &self.path),
        }
    }

    /// Writes this file's entry into the lock, when the file was opened in Record mode. A
    /// file that failed to load is not recorded: call this after it loaded.
    pub fn record(&self) -> Result<(), IntegrityError> {
        let (Some(lock), Some(digest)) = (&self.record_to, self.digest) else {
            return Ok(());
        };
        merge(lock, &[(self.key()?, digest)])
    }

    fn fd(&self) -> Option<i32> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            self.file.as_ref().map(|file| file.as_raw_fd())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    fn interpreter_text(&self) -> Option<String> {
        let mut reader: &File = self.file.as_ref()?;
        reader.seek(SeekFrom::Start(0)).ok()?;
        let mut head = [0u8; 256];
        let read = reader.read(&mut head).ok()?;
        let text = std::str::from_utf8(&head[..read]).ok()?;
        let line = text.lines().next()?;
        let interpreter = line.strip_prefix("#!")?.trim();
        Some(interpreter.to_string())
    }

    /// Opens `path` under `mode`.
    pub(crate) fn open(mode: Mode, lock: &Path, path: &Path) -> Result<Checked, IntegrityError> {
        let serial = NEXT_SERIAL.fetch_add(1, Ordering::Relaxed);
        if mode == Mode::Off {
            return Ok(Checked {
                path: path.to_path_buf(),
                file: None,
                digest: None,
                serial,
                record_to: None,
            });
        }
        let file = File::open(path).map_err(|error| IntegrityError::Open {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        let meta = file.metadata().map_err(|error| IntegrityError::Read {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        if !meta.is_file() {
            return Err(IntegrityError::NotRegular {
                path: path.to_path_buf(),
            });
        }
        let (digest, _head) = hash_file(&file).map_err(|error| IntegrityError::Read {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
        let checked = Checked {
            path: path.to_path_buf(),
            file: Some(file),
            digest: Some(digest),
            serial,
            record_to: (mode == Mode::Record).then(|| lock.to_path_buf()),
        };
        if mode == Mode::Locked {
            let key = checked.key()?;
            let lock_file = read_lock(lock)?;
            match lock_file.expected_for(path) {
                Some((_, expected)) if expected == digest => {}
                Some((entry, expected)) => {
                    return Err(IntegrityError::Mismatch {
                        path: path.to_path_buf(),
                        lock: lock.to_path_buf(),
                        key: entry.to_string(),
                        expected,
                        found: digest,
                    });
                }
                None => {
                    return Err(IntegrityError::NotListed {
                        path: path.to_path_buf(),
                        lock: lock.to_path_buf(),
                        key,
                        found: digest,
                    });
                }
            }
        }
        Ok(checked)
    }
}

/// The key of `path` in a lock: its file name. A name that is not UTF-8 has no key.
pub(crate) fn file_key(path: &Path) -> Result<String, IntegrityError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| IntegrityError::BadName {
            path: path.to_path_buf(),
        })
}

/// The name a library is loaded by, for `fd` of this process and `serial` of its `Checked`.
///
/// The serial is written after `/proc/self/fd/` as a run of no-op components: a `1` bit after the
/// leading one is `./`, a `0` bit is `/`. Each serial gives a different string, and every string
/// names `fd` in the kernel.
pub fn dlopen_name(serial: u64, fd: i32) -> PathBuf {
    let number = serial + 1;
    let bits = 64 - number.leading_zeros() - 1;
    let mut name = String::from("/proc/self/fd/");
    for bit in (0..bits).rev() {
        name.push_str(if (number >> bit) & 1 == 1 { "./" } else { "/" });
    }
    name.push_str(&fd.to_string());
    PathBuf::from(name)
}

#[cfg(all(unix, target_os = "linux"))]
fn confirm_mapped(file: &File, addr: usize, path: &Path) -> Result<(), IntegrityError> {
    use std::os::unix::fs::MetadataExt;

    let not_loaded = || IntegrityError::NotLoaded {
        path: path.to_path_buf(),
    };
    let ino = file.metadata().map_err(|_| not_loaded())?.ino();
    let maps = std::fs::read_to_string("/proc/self/maps").map_err(|_| not_loaded())?;
    for line in maps.lines() {
        // Each line: range, permissions, offset, device, inode, then the path if there is one.
        let mut fields = line.split_whitespace();
        let (Some(range), Some(_), Some(_), Some(_), Some(inode)) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            continue;
        };
        let Some((start, end)) = range.split_once('-') else {
            continue;
        };
        let (Ok(start), Ok(end)) = (
            usize::from_str_radix(start, 16),
            usize::from_str_radix(end, 16),
        ) else {
            continue;
        };
        if addr >= start && addr < end {
            // The inode is the file's identity. The device is not compared: on btrfs the kernel
            // shows the filesystem's device here, and `st_dev` is per subvolume, so the two differ
            // for the same file.
            return if inode.parse::<u64>() == Ok(ino) {
                Ok(())
            } else {
                Err(not_loaded())
            };
        }
    }
    Err(not_loaded())
}

#[cfg(not(all(unix, target_os = "linux")))]
fn confirm_mapped(_file: &File, _addr: usize, path: &Path) -> Result<(), IntegrityError> {
    Err(IntegrityError::NotLoaded {
        path: path.to_path_buf(),
    })
}
