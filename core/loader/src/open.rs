//! Opening a library and reading its entry, with the layout check first.

use std::fmt;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};

use daedalus_abi::{
    AbiDesc, ENTRY_SYMBOL, EntryFn, Grown, Mismatch, OLD_ENTRY_SYMBOL, PluginEntry, SHAPES,
    SHAPES_SYMBOL, ShapeDesc, ShapesFn, Status,
};
use libloading::Library;

/// Why a plugin could not be loaded. Each variant names the file.
#[derive(Debug)]
pub enum LoadError {
    /// The file could not be opened as a library.
    Open { path: PathBuf, message: String },
    /// The library has no entry point.
    Symbol { path: PathBuf, message: String },
    /// The plugin's shapes don't fit this host's: it was built against a newer Daedalus, or
    /// one where a shape was laid out differently. The shape is named.
    Shapes { path: PathBuf, why: Mismatch },
    /// The plugin was built before plugins described their shapes. It has to be built again.
    Older { path: PathBuf },
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
    /// A plugin could not make the bytes asked for, or does not make any.
    Produce { plugin: String, status: Status },
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Open { path, message } => write!(f, "{}: {message}", path.display()),
            LoadError::Symbol { path, message } => {
                write!(f, "{}: no entry point: {message}", path.display())
            }
            LoadError::Shapes { path, why } => {
                write!(f, "{}: this plugin {why}", path.display())
            }
            LoadError::Older { path } => write!(
                f,
                "{}: this plugin was built for an older Daedalus, before a plugin described \
                 its layout to its host: build it again against this one",
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
            LoadError::Produce { plugin, status } => {
                write!(f, "plugin {plugin} made no bytes: status {}", status.0)
            }
        }
    }
}

impl std::error::Error for LoadError {}

/// A library opened and its entry read. The entry's text is still valid: the library is held.
pub(crate) struct Opened {
    pub library: Library,
    pub entry: PluginEntry,
    /// The plugin's shapes, which say how its tables are laid out.
    pub shapes: Vec<ShapeDesc>,
}

/// The most shapes a plugin may describe. This build has a few dozen.
const MAX_SHAPES: u32 = 4096;

/// Opens `path`, reads the plugin's shapes and checks they fit this host's, and only then calls
/// its entry.
///
/// # Safety
/// Loading runs the library's initialisers, and its symbols are trusted to have the signatures
/// [`ShapesFn`] and [`EntryFn`]. The caller must name a trusted Daedalus plugin.
pub(crate) unsafe fn open_library(path: &Path) -> Result<Opened, LoadError> {
    // SAFETY: the caller names a trusted plugin, as this function's contract says.
    let library = unsafe { Library::new(path) }.map_err(|error| LoadError::Open {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let shapes = {
        // SAFETY: the symbol is looked up with the signature it is written to, and `library`
        // stays loaded for as long as the returned symbol is used here.
        let found: Result<libloading::Symbol<'_, ShapesFn>, _> =
            unsafe { library.get(SHAPES_SYMBOL) };
        let describe = match found {
            Ok(describe) => describe,
            Err(error) => {
                // SAFETY: only whether the symbol is there is asked. It is never called.
                let old: Result<libloading::Symbol<'_, unsafe extern "C" fn()>, _> =
                    unsafe { library.get(OLD_ENTRY_SYMBOL) };
                return Err(match old {
                    Ok(_) => LoadError::Older {
                        path: path.to_path_buf(),
                    },
                    Err(_) => LoadError::Symbol {
                        path: path.to_path_buf(),
                        message: error.to_string(),
                    },
                });
            }
        };
        let mut desc = AbiDesc {
            shapes: core::ptr::null(),
            count: 0,
            reserved: 0,
        };
        // SAFETY: `desc` is a writable `AbiDesc`, a shape that never changes.
        let status = unsafe { (*describe)(&mut desc) };
        if status != Status::OK || desc.shapes.is_null() || desc.count > MAX_SHAPES {
            return Err(LoadError::Refused {
                path: path.to_path_buf(),
                status,
            });
        }
        // SAFETY: the plugin says `count` descriptors are at `shapes`, in memory of the library
        // that is held. A descriptor never changes shape, so they are read at this build's size.
        unsafe { std::slice::from_raw_parts(desc.shapes, desc.count as usize) }.to_vec()
    };
    daedalus_abi::fits(SHAPES, &shapes).map_err(|why| LoadError::Shapes {
        path: path.to_path_buf(),
        why,
    })?;
    // Zeroed, so the fields a plugin built before them doesn't write read as absent.
    let mut out = MaybeUninit::<PluginEntry>::zeroed();
    let status = {
        // SAFETY: as for the shapes symbol above.
        let entry: libloading::Symbol<'_, EntryFn> =
            unsafe { library.get(ENTRY_SYMBOL) }.map_err(|error| LoadError::Symbol {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        // SAFETY: `out` is a writable `PluginEntry` of this host's size, and the plugin's own is
        // no larger: its shapes fit.
        unsafe { (*entry)(out.as_mut_ptr(), size_of::<PluginEntry>()) }
    };
    if status != Status::OK {
        return Err(LoadError::Refused {
            path: path.to_path_buf(),
            status,
        });
    }
    // SAFETY: the entry returned OK, so it wrote every field it has, which are at least the ones
    // every build has. The rest is zero, which is absent for each field added since.
    let entry = unsafe { out.assume_init() };
    Ok(Opened {
        library,
        entry,
        shapes,
    })
}

/// A plugin's table of `count` `T`s at `table`, each read at the plugin's own stride.
///
/// # Safety
/// `table` must be a table the plugin's entry declared, of `count` elements, in a library that is
/// still loaded, and `shapes` the descriptors that plugin gave, which fit this host's.
pub(crate) unsafe fn read_table<T: Grown>(
    table: *const T,
    count: usize,
    shapes: &[ShapeDesc],
    path: &Path,
    what: &'static str,
) -> Result<Vec<T>, LoadError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let theirs = daedalus_abi::find(shapes, T::ID).filter(|_| !table.is_null());
    let Some(theirs) = theirs else {
        return Err(LoadError::BadText {
            path: path.to_path_buf(),
            what,
        });
    };
    Ok((0..count)
        // SAFETY: `index` is within the table, and `theirs` fits, as the caller says.
        .map(|index| unsafe { daedalus_abi::read_grown(table, index, theirs) })
        .collect())
}
