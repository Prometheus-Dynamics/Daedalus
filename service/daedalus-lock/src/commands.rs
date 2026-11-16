//! The three commands: record a file's digest, check files against the lock, update an entry.

use std::fs::File;
use std::path::{Path, PathBuf};

use daedalus_integrity::{Digest, IntegrityError, Lock, hash_file, merge, read_lock, update};

/// Why a command did not run, or failed. Each maps to an exit code.
#[derive(Debug)]
pub enum Fail {
    /// The arguments are not a command the tool has. Exit 2.
    Usage(String),
    /// A file does not match the lock, has no entry, or would change an approved entry. Exit 1.
    Mismatch(String),
    /// The lock or a file named on the command line cannot be read or is malformed. Exit 3.
    Unreadable(String),
}

/// What a command found. Each line goes to stdout, each warning to stderr. `failed` is set when a
/// file was checked and did not pass.
#[derive(Debug, Default)]
pub struct Report {
    pub lines: Vec<String>,
    pub warnings: Vec<String>,
    pub failed: bool,
}

fn unreadable(error: impl std::fmt::Display) -> Fail {
    Fail::Unreadable(error.to_string())
}

/// The digest of the file at `path`, and its first two bytes.
fn digest_of(path: &Path) -> Result<(Digest, [u8; 2]), Fail> {
    let file =
        File::open(path).map_err(|error| unreadable(format!("{}: {error}", path.display())))?;
    hash_file(&file).map_err(|error| unreadable(format!("{}: {error}", path.display())))
}

/// The key a file is recorded under: its file name, or with `absolute` its absolute path as
/// given (not resolving symbolic links).
fn key_of(path: &Path, absolute: bool) -> Result<String, Fail> {
    let text = if absolute {
        std::path::absolute(path)
            .map_err(|error| unreadable(format!("{}: {error}", path.display())))?
            .to_str()
            .map(str::to_string)
    } else {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
    };
    text.ok_or_else(|| unreadable(format!("{}: the name is not UTF-8", path.display())))
}

/// The lock's directory: where its relative keys are.
fn lock_dir(lock: &Path) -> PathBuf {
    match lock.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// The file a lock key names: an absolute key as it is, any other under `root`.
fn file_of(root: &Path, key: &str) -> PathBuf {
    let path = Path::new(key);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(key)
    }
}

fn load(lock: &Path) -> Result<Lock, Fail> {
    read_lock(lock).map_err(unreadable)
}

/// Records each file's digest into the lock, creating the lock if it is missing. A file already
/// recorded under another digest is a mismatch, and the lock is not changed.
pub fn record(lock: &Path, paths: &[PathBuf], absolute: bool) -> Result<Report, Fail> {
    if paths.is_empty() {
        return Err(Fail::Usage("record needs at least one PATH".into()));
    }
    let mut report = Report::default();
    let mut entries = Vec::new();
    for path in paths {
        let (digest, head) = digest_of(path)?;
        if &head == b"#!" {
            report.warnings.push(format!(
                "{} starts with `#!`: the kernel runs its interpreter, not this file. Record the \
                 interpreter as the program, and add this file with also_check.",
                path.display()
            ));
        }
        let key = key_of(path, absolute)?;
        report
            .lines
            .push(format!("{key}: recorded sha256 {digest}"));
        entries.push((key, digest));
    }
    merge(lock, &entries).map_err(|error| match error {
        IntegrityError::Conflict { .. } => Fail::Mismatch(error.to_string()),
        other => unreadable(other),
    })?;
    Ok(report)
}

/// Checks files against the lock. With PATHs, each is checked against its entry. Without, each
/// entry's file is found under `root` (default: the lock's directory) and checked.
pub fn check(lock: &Path, root: Option<&Path>, paths: &[PathBuf]) -> Result<Report, Fail> {
    let lock_file = load(lock)?;
    let mut report = Report::default();
    if paths.is_empty() {
        let root = root.map_or_else(|| lock_dir(lock), Path::to_path_buf);
        for (key, expected) in lock_file.entries() {
            let path = file_of(&root, key);
            match digest_of(&path) {
                Ok((found, _)) if found == *expected => {
                    report.lines.push(format!("{key}: OK"));
                }
                Ok((found, _)) => {
                    report.failed = true;
                    report.lines.push(format!(
                        "{key}: FAILED (expected sha256 {expected}, found {found})"
                    ));
                }
                Err(Fail::Unreadable(message)) if !path.exists() => {
                    report.failed = true;
                    report
                        .lines
                        .push(format!("{key}: FAILED (missing: {message})"));
                }
                Err(other) => return Err(other),
            }
        }
    } else {
        for path in paths {
            let (found, _) = digest_of(path)?;
            match lock_file.expected_for(path) {
                None => {
                    report.failed = true;
                    report.lines.push(format!(
                        "{}: FAILED (no entry in the lock; sha256 {found})",
                        path.display()
                    ));
                }
                Some((_, expected)) if expected == found => {
                    report.lines.push(format!("{}: OK", path.display()));
                }
                Some((key, expected)) => {
                    report.failed = true;
                    report.lines.push(format!(
                        "{}: FAILED ({key} expects sha256 {expected}, found {found})",
                        path.display()
                    ));
                }
            }
        }
    }
    Ok(report)
}

/// Approves the current file for each entry: each entry's digest is re-hashed and written, and
/// the change shown as old and new. With PATHs, only those files' entries are updated.
pub fn update_entries(lock: &Path, root: Option<&Path>, paths: &[PathBuf]) -> Result<Report, Fail> {
    let lock_file = load(lock)?;
    let mut report = Report::default();
    let mut entries = Vec::new();
    if paths.is_empty() {
        let root = root.map_or_else(|| lock_dir(lock), Path::to_path_buf);
        for (key, old) in lock_file.entries() {
            let (new, _) = digest_of(&file_of(&root, key))?;
            report.lines.push(show(key, old, &new));
            entries.push((key.to_string(), new));
        }
    } else {
        for path in paths {
            let Some((key, old)) = lock_file.expected_for(path) else {
                return Err(Fail::Mismatch(format!(
                    "{} has no entry in the lock to update: record it first",
                    path.display()
                )));
            };
            let (new, _) = digest_of(path)?;
            report.lines.push(show(key, &old, &new));
            entries.push((key.to_string(), new));
        }
    }
    update(lock, &entries).map_err(unreadable)?;
    Ok(report)
}

fn show(key: &str, old: &Digest, new: &Digest) -> String {
    if old == new {
        format!("{key}: unchanged sha256 {new}")
    } else {
        format!("{key}: {old} -> {new}")
    }
}
