//! A dma-buf as a storage buffer, with no copy: the bytes of an image where a shader or a copy
//! can read them.

use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;

use daedalus_dma_sys::{DmaBufRef, Identity};
use daedalus_vk_sys::Error;
use daedalus_vk_sys::memory::import_dma_buf;

use super::ImageKind;
use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::dmabuf::{Described, Fourcc, ImportError, ImportStats, Importer};

/// The bytes of an image in a dma-buf, described for import as a buffer.
#[derive(Debug)]
pub struct ExternalBuffer {
    pub fd: OwnedFd,
    pub width: u32,
    pub height: u32,
    pub fourcc: Fourcc,
    /// Where each plane starts, and how far apart its rows are.
    pub planes: Vec<(u64, u64)>,
}

/// An imported buffer, and where each plane of the image is in it.
#[derive(Clone, Debug)]
pub struct ImportedStorage {
    pub storage: Arc<Storage>,
    /// Each plane's offset and stride, as it was described.
    pub planes: Vec<(u64, u64)>,
}

/// Buffers a device keeps imported. A producer cycles through a few.
const KEPT: usize = 16;

#[derive(Default)]
pub(crate) struct StorageImports {
    kept: Vec<(Identity, Arc<Storage>)>,
    stats: ImportStats,
}

impl StorageImports {
    pub(crate) fn stats(&self) -> ImportStats {
        self.stats
    }
}

impl WgpuBackend {
    /// Imports the dma-buf behind `image` as a storage buffer, or finds it if it has been
    /// imported. The buffer is the dma-buf: nothing is copied. A Bayer mosaic or a compressed
    /// stream is refused, and so is a buffer shorter than this device needs for the image.
    pub fn import_storage(&self, image: ExternalBuffer) -> Result<ImportedStorage, ImportError> {
        let context = self
            .context()
            .map_err(|error| ImportError::Unsupported(error.to_string()))?;
        let kind = ImageKind::of(image.fourcc);
        if let Some(why) = kind.refusal() {
            return Err(ImportError::UnsupportedFormat {
                fourcc: image.fourcc,
                modifier: None,
                reason: why.into(),
            });
        }
        // The layout is checked where the fourcc is one the device has a format for. Anything
        // else is bytes, and only has to be there.
        let described = Described {
            width: image.width,
            height: image.height,
            fourcc: image.fourcc,
            modifier: None,
            planes: &image.planes,
        };
        let buffer = DmaBufRef::from_fd(image.fd.as_fd());
        let len = buffer
            .len()
            .map_err(|error| ImportError::invalid(format!("the dma-buf's size: {error}")))?;
        if image.fourcc.format().is_some() {
            let layout = described.validate()?;
            let profile = &context.record.profile;
            let importer = Importer {
                family: profile.family,
                vendor: profile.vendor,
            };
            // Every plane is in this one buffer, so it has to reach the furthest of them.
            let needed = importer.required_len(image.fourcc, &layout);
            let furthest = needed.iter().copied().max().unwrap_or(0);
            if len < furthest {
                return Err(ImportError::invalid(format!(
                    "the dma-buf is {len} bytes, and this device needs {furthest} for the image"
                )));
            }
        }
        let id = buffer
            .identity()
            .map_err(|error| ImportError::invalid(format!("the dma-buf: {error}")))?;
        let mut imports = context.storage_imports();
        if let Some(at) = imports.kept.iter().position(|(have, _)| *have == id) {
            imports.stats.hits += 1;
            return Ok(ImportedStorage {
                storage: Arc::clone(&imports.kept[at].1),
                planes: image.planes,
            });
        }
        // A buffer is a whole number of words.
        let size = len / 4 * 4;
        let imported =
            import_dma_buf(&context.device, image.fd, size, None).map_err(|error| match error {
                Error::NoDmaBufImport | Error::NotVulkan => {
                    ImportError::Unsupported("this device can't import a dma-buf".into())
                }
                other => ImportError::Refused(other.to_string()),
            })?;
        let storage = Arc::new(Storage::imported(Arc::clone(&context), imported, size));
        imports.stats.imports += 1;
        if imports.kept.len() >= KEPT {
            imports.kept.remove(0);
            imports.stats.evictions += 1;
        }
        imports.kept.push((id, Arc::clone(&storage)));
        Ok(ImportedStorage {
            storage,
            planes: image.planes,
        })
    }

    /// How often this device's cache of imported buffers was used. `None` while no device is
    /// open.
    pub fn storage_import_stats(&self) -> Option<ImportStats> {
        self.context()
            .ok()
            .map(|context| context.storage_imports().stats())
    }
}
