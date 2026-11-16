//! Finding the program a worker names.

use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// The file a program name runs. A name with a separator is used as given. A bare name is
/// searched for in the directories of `path_var`, in order. `None` when a bare name is not found.
pub fn find_program(name: &OsStr, path_var: Option<&OsStr>) -> Option<PathBuf> {
    let given = Path::new(name);
    if given.components().count() > 1 || given.is_absolute() {
        return Some(given.to_path_buf());
    }
    let dirs = env::split_paths(path_var?);
    dirs.map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}
