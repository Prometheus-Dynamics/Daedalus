//! One loaded library. Steps hold an `Arc` to it, so it stays open until the last of them is
//! released: that is the retirement of a generation.

use std::path::PathBuf;

use libloading::Library;

/// A library a plugin was loaded from, under its number for that plugin name.
pub struct Generation {
    name: String,
    number: u64,
    path: PathBuf,
    /// Held so the library stays open until the generation is dropped.
    _library: Library,
}

impl Generation {
    pub(crate) fn new(name: String, number: u64, path: PathBuf, library: Library) -> Generation {
        Generation {
            name,
            number,
            path,
            _library: library,
        }
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
