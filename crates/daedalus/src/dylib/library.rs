//! An opened plugin library, closed when the last reference drops unless something pinned it,
//! and the private copies reloads open.

use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use libloading::Library;

/// A loaded library. Everything that may point into its code or data holds an `Arc` of it
/// (stable install generations, buffers the plugin produced); what cannot be tracked pins it.
pub struct LibraryHandle {
    library: ManuallyDrop<Library>,
    pinned: AtomicBool,
    /// A private copy that was opened (removed once closed, or right after opening where the
    /// OS keeps the mapping).
    shadow: Option<PathBuf>,
}

impl LibraryHandle {
    /// Open `path`, or a private copy of it when `shadow` (so rebuilding the file at `path`,
    /// even in place, never touches the mapped code, and reopening a rebuilt file the process
    /// already loaded yields the new build instead of the cached old one).
    ///
    /// # Safety
    /// As [`Library::new`]: loading runs the library's initialisers.
    pub unsafe fn open(path: &Path, shadow: bool) -> Result<Self, OpenError> {
        let copy = if shadow {
            Some(shadow_copy(path).map_err(OpenError::Copy)?)
        } else {
            None
        };
        // Safety: forwarded from the caller.
        let library = unsafe { Library::new(copy.as_deref().unwrap_or(path)) };
        let library = match library {
            Ok(library) => library,
            Err(err) => {
                if let Some(copy) = &copy {
                    let _ = std::fs::remove_file(copy);
                }
                return Err(OpenError::Load(err));
            }
        };
        // Unix keeps the mapping of an unlinked file; elsewhere the copy goes once closed.
        let shadow = match copy {
            Some(copy) if cfg!(unix) => {
                let _ = std::fs::remove_file(&copy);
                None
            }
            copy => copy,
        };
        Ok(Self {
            library: ManuallyDrop::new(library),
            pinned: AtomicBool::new(false),
            shadow,
        })
    }

    pub fn library(&self) -> &Library {
        &self.library
    }

    /// Never close the library (for handlers registered through the Rust ABI, or handles whose
    /// code the host keeps without tracking).
    pub fn pin(&self) {
        self.pinned.store(true, Ordering::Release);
    }

    pub fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::Acquire)
    }
}

impl Drop for LibraryHandle {
    fn drop(&mut self) {
        if self.is_pinned() {
            return;
        }
        // Safety: the last reference: nothing the host keeps points into the library. (The OS
        // may keep it mapped until threads holding its thread-local destructors exit.)
        unsafe { ManuallyDrop::drop(&mut self.library) };
        if let Some(shadow) = &self.shadow {
            let _ = std::fs::remove_file(shadow);
        }
    }
}

/// Why [`LibraryHandle::open`] failed.
#[derive(Debug)]
pub enum OpenError {
    Copy(std::io::Error),
    Load(libloading::Error),
}

/// Copy `path` to a fresh file under the temp dir.
fn shadow_copy(path: &Path) -> std::io::Result<PathBuf> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join("daedalus-plugins");
    std::fs::create_dir_all(&dir)?;
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "plugin".to_string());
    let copy = dir.join(format!(
        "{}-{}-{name}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::copy(path, &copy)?;
    Ok(copy)
}
