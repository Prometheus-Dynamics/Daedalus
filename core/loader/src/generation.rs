//! One loaded library. Steps hold an `Arc` to it, so it stays open until the last of them is
//! released: that is the retirement of a generation.

use std::fs::File;
use std::path::PathBuf;

use daedalus_integrity::Digest;
use libloading::Library;

/// A library a plugin was loaded from, under its number for that plugin name.
pub struct Generation {
    name: String,
    number: u64,
    path: PathBuf,
    /// Held so the library stays open until the generation is dropped.
    _library: Library,
    /// The open file the library was loaded from, when the file was checked. It is kept open so
    /// its number is not reused while the library lives. It is declared after `_library`, so it
    /// is dropped after it.
    _file: Option<File>,
    digest: Option<Digest>,
}

impl Generation {
    pub(crate) fn new(
        name: String,
        number: u64,
        path: PathBuf,
        library: Library,
        file: Option<File>,
        digest: Option<Digest>,
    ) -> Generation {
        Generation {
            name,
            number,
            path,
            _library: library,
            _file: file,
            digest,
        }
    }

    /// The digest of the file, when it was checked.
    pub fn digest(&self) -> Option<&Digest> {
        self.digest.as_ref()
    }

    /// The plugin's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Which load of this plugin name this is: the first is 1.
    pub fn number(&self) -> u64 {
        self.number
    }

    /// The file it was loaded from.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl std::fmt::Debug for Generation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Generation")
            .field("name", &self.name)
            .field("number", &self.number)
            .field("path", &self.path)
            .finish()
    }
}
