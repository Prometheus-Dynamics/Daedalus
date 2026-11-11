//! Opening a library and reading its entry, with the layout check first.

use std::fmt;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use daedalus_abi::{ENTRY_SYMBOL, EntryFn, PluginEntry, Status};
use libloading::Library;

/// Why a plugin could not be loaded. Each variant names the file.
#[derive(Debug)]
pub enum LoadError {
    /// The file could not be opened as a library.
    Open { path: PathBuf, message: String },
    /// The library has no entry point.
    Symbol { path: PathBuf, message: String },
    /// The plugin was built against another layout. Both hashes are named.
    Layout {
        path: PathBuf,
        expected: u64,
        found: u64,
    },
    /// The entry point refused for another reason.
    Refused { path: PathBuf, status: Status },
    /// A string in the entry is not UTF-8, or a port type key is not valid.
    BadText { path: PathBuf, what: &'static str },
    /// A node could not join the registry.
    Register {
        path: PathBuf,
        error: daedalus_registry::RegError,
    },
    /// A retry was stopped by hand before the plugin loaded.
    Stopped { path: PathBuf },
    /// A backend call returned a status the host does not expect.
    Compile { backend: String, status: Status },
    /// A codec conversion refused its input, or the plugin returned a status the host does not expect.
    Codec { key: String, status: Status },
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Open { path, message } => write!(f, "{}: {message}", path.display()),
            LoadError::Symbol { path, message } => {
                write!(f, "{}: no entry point: {message}", path.display())
            }
            LoadError::Layout {
                path,
                expected,
                found,
            } => write!(
                f,
                "{}: built for layout {found:#018x}, this host has {expected:#018x}",
                path.display()
            ),
            LoadError::Refused { path, status } => {
                write!(
                    f,
                    "{}: entry point refused with status {}",
                    path.display(),
                    status.0
                )
            }
            LoadError::BadText { path, what } => {
                write!(f, "{}: {what} is not valid text", path.display())
            }
            LoadError::Register { path, error } => write!(f, "{}: {error}", path.display()),
            LoadError::Stopped { path } => write!(f, "{}: loading was stopped", path.display()),
            LoadError::Compile { backend, status } => {
                write!(f, "backend {backend} refused with status {}", status.0)
            }
            LoadError::Codec { key, status } => {
                write!(f, "codec for {key} refused with status {}", status.0)
            }
        }
    }
}

impl std::error::Error for LoadError {}

/// A library opened and its entry read. The entry's text is still valid: the library is held.
pub(crate) struct Opened {
    pub library: Library,
    pub entry: PluginEntry,
}

/// Opens `path`, calls its entry with this host's layout hash, and checks the status.
///
/// # Safety
/// Loading runs the library's initialisers, and the entry is trusted to have the signature
/// [`EntryFn`]. The caller must name a trusted Daedalus plugin.
pub(crate) unsafe fn open_library(path: &Path) -> Result<Opened, LoadError> {
    // SAFETY: the caller names a trusted plugin, as this function's contract says.
    let library = unsafe { Library::new(path) }.map_err(|error| LoadError::Open {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let mut out = MaybeUninit::<PluginEntry>::uninit();
    let status = {
        // SAFETY: the symbol is looked up with the signature the entry point is written to, and
        // `library` stays loaded for as long as the returned symbol is used here.
        let entry: libloading::Symbol<'_, EntryFn> =
            unsafe { library.get(ENTRY_SYMBOL) }.map_err(|error| LoadError::Symbol {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        // SAFETY: `out` is a writable `PluginEntry`. A plugin with another layout writes only its
        // own hash into the first field, so the rest of `out` stays unread.
        unsafe { (*entry)(daedalus_abi::LAYOUT_HASH, out.as_mut_ptr()) }
    };
    if status == Status::VERSION_MISMATCH {
        // SAFETY: the plugin wrote its layout hash into the first field of `out`, which is the
        // field at offset zero in both builds.
        let found = unsafe { core::ptr::addr_of!((*out.as_ptr()).layout).read() };
        return Err(LoadError::Layout {
            path: path.to_path_buf(),
            expected: daedalus_abi::LAYOUT_HASH,
            found,
        });
    }
    if status != Status::OK {
        return Err(LoadError::Refused {
            path: path.to_path_buf(),
            status,
        });
    }
    // SAFETY: the entry returned OK, which means it wrote every field of `out`.
    let entry = unsafe { out.assume_init() };
    Ok(Opened { library, entry })
}
