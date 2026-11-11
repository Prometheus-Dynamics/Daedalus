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
    /// stream is refused, and so is a buffer shorter than this device needs for the image, or
    /// one the device can't take (`Refused`, with the driver's `TooSmall` text where the buffer is
    /// short for the device's own memory).
    pub fn import_storage(&self, image: ExternalBuffer) -> Result<ImportedStorage, ImportError> {
        let context = self
            .context()
            .map_err(|error| ImportError::Unsupported(error.to_string()))?;
        Self::import_on(&context, image)
    }

    /// [`WgpuBackend::import_storage`] on a device's context, for a step of the backend that has
    /// the context and not the backend.
    pub(crate) fn import_on(
        context: &Arc<crate::context::Context>,
        image: ExternalBuffer,
    ) -> Result<ImportedStorage, ImportError> {
        Self::import_sized(context, image, None)
    }

    /// [`WgpuBackend::import_on`] for a buffer of `want` bytes, when the value is shorter than its
    /// dma-buf, as a dma-heap allocation is: the buffer is the value's length, and the dma-buf
    /// must be able to back it. Such a buffer is not cached, since the cache is keyed by the
    /// dma-buf and not by its length.
    pub(crate) fn import_sized(
        context: &Arc<crate::context::Context>,
        image: ExternalBuffer,
        want: Option<u64>,
    ) -> Result<ImportedStorage, ImportError> {
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
        let id = buffer
            .identity()
            .map_err(|error| ImportError::invalid(format!("the dma-buf: {error}")))?;
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
        let mut imports = context.storage_imports();
        if let Some(at) = imports
            .kept
            .iter()
            .position(|(have, _)| *have == id)
            .filter(|_| want.is_none())
        {
            imports.stats.hits += 1;
            return Ok(ImportedStorage {
                storage: Arc::clone(&imports.kept[at].1),
                planes: image.planes,
            });
        }
        let refused = |error: Error| match error {
            Error::NoDmaBufImport | Error::NotVulkan => {
                ImportError::Unsupported("this device can't import a dma-buf".into())
            }
            other => ImportError::Refused(other.to_string()),
        };
        let storage = match want {
            Some(want) => {
                // The device must be able to back the value's length with this dma-buf.
                let needed = daedalus_vk_sys::memory::dma_buf_bytes_needed(&context.device, want)
                    .map_err(refused)?;
                if needed > len {
                    return Err(ImportError::Refused(
                        daedalus_vk_sys::Error::TooSmall {
                            have: len,
                            need: needed,
                        }
                        .to_string(),
                    ));
                }
                let imported =
                    import_dma_buf(&context.device, image.fd, want, len, None).map_err(refused)?;
                imports.stats.imports += 1;
                return Ok(ImportedStorage {
                    storage: Arc::new(Storage::imported(Arc::clone(context), imported, want)),
                    planes: image.planes,
                });
            }
            None => {
                // A buffer is a whole number of words, and as much of the dma-buf as the device
                // can back with it: one that pads a buffer's memory gets a buffer that much shorter.
                let size = daedalus_vk_sys::memory::dma_buf_size_that_fits(&context.device, len)
                    .map_err(refused)?;
                let imported =
                    import_dma_buf(&context.device, image.fd, size, len, None).map_err(refused)?;
                Arc::new(Storage::imported(Arc::clone(context), imported, size))
            }
        };
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
