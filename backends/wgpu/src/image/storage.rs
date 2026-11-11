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

/// `ENOTTY` on Linux: the kind of fd the dma-buf sync ioctl is not for.
const ENOTTY: i32 = 25;

/// Bytes a device reads past the end of a buffer's last plane, so a buffer that is copied to has
/// this much slack after its data.
const DEVICE_SLACK: u64 = 256;

/// Whether the dma-buf's exporter is a dma-heap, by the name the kernel gives it in the fd's
/// `fdinfo`: the heap's own name. Anything else, or anything unreadable, is not a heap.
#[cfg(target_os = "linux")]
fn exporter_is_dma_heap(fd: std::os::fd::BorrowedFd<'_>) -> bool {
    use std::os::fd::AsRawFd;
    let Ok(info) = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", fd.as_raw_fd())) else {
        return false;
    };
    let Some(exporter) = info
        .lines()
        .find_map(|line| line.strip_prefix("exp_name:"))
        .map(str::trim)
    else {
        return false;
    };
    std::fs::read_dir("/dev/dma_heap")
        .into_iter()
        .flatten()
        .flatten()
        .any(|heap| heap.file_name().to_str() == Some(exporter))
}

#[cfg(not(target_os = "linux"))]
fn exporter_is_dma_heap(_: std::os::fd::BorrowedFd<'_>) -> bool {
    false
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
        let id = buffer
            .identity()
            .map_err(|error| ImportError::invalid(format!("the dma-buf: {error}")))?;
        // Only dma-heap buffers are imported in place: the heap keeps them coherent with the CPU,
        // and a cached import is one of those. Any other dma-buf is copied through the CPU.
        let cached = context
            .storage_imports()
            .kept
            .iter()
            .any(|(have, _)| *have == id);
        let in_place = cached || exporter_is_dma_heap(image.fd.as_fd());
        if image.fourcc.format().is_some() {
            let layout = described.validate()?;
            if in_place {
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
        }
        if !in_place {
            return self.copy_import(&context, image, len);
        }
        let mut imports = context.storage_imports();
        if let Some(at) = imports.kept.iter().position(|(have, _)| *have == id) {
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
        // A buffer is a whole number of words, and as much of the dma-buf as the device can
        // back with it: one that pads a buffer's memory gets a buffer that much shorter.
        let size = daedalus_vk_sys::memory::dma_buf_size_that_fits(&context.device, len)
            .map_err(refused)?;
        let imported =
            import_dma_buf(&context.device, image.fd, size, len, None).map_err(refused)?;
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

    /// Copies a dma-buf that is not a dma-heap buffer into a buffer of the device's own. The CPU
    /// reads the bytes through its mapping, which holds what the exporter last wrote, and the
    /// copy is what the device reads. The copy is counted in the import stats. Nothing is cached:
    /// the producer writes the next frame into the same memory.
    fn copy_import(
        &self,
        context: &crate::context::Context,
        image: ExternalBuffer,
        len: u64,
    ) -> Result<ImportedStorage, ImportError> {
        use daedalus_dma_sys::{Access, DmaBuf};
        let ExternalBuffer { fd, planes, .. } = image;
        let buf = DmaBuf::from_fd(fd);
        let fail = |what: &str, error: daedalus_dma_sys::Error| {
            ImportError::invalid(format!("copying the dma-buf ({what}): {error}"))
        };
        // An fd without the dma-buf sync (ENOTTY) has no exporter cache to maintain, so the CPU
        // read needs no sync for it. Any other refusal is an error.
        let sync = |result: Result<(), daedalus_dma_sys::Error>, what: &str| match result {
            Err(daedalus_dma_sys::Error::Os(errno)) if errno == ENOTTY => Ok(()),
            other => other.map_err(|error| fail(what, error)),
        };
        let mut bytes = vec![0u8; len as usize];
        sync(buf.as_ref().begin_cpu_access(Access::Read), "begin")?;
        let read = buf
            .as_ref()
            .map_read_only()
            .map_err(|error| fail("map", error))
            .and_then(|map| map.read(0, &mut bytes).map_err(|error| fail("read", error)));
        sync(buf.as_ref().end_cpu_access(Access::Read), "end")?;
        read?;
        // The device's buffer keeps the slack a V3D buffer needs past its last byte.
        let size = (len + DEVICE_SLACK).next_multiple_of(4);
        let storage =
            Storage::new(self, size).map_err(|error| ImportError::invalid(format!("{error}")))?;
        storage
            .write(0, &bytes)
            .map_err(|error| ImportError::invalid(format!("{error}")))?;
        let mut imports = context.storage_imports();
        imports.stats.copies += 1;
        imports.stats.copied_bytes += len;
        Ok(ImportedStorage {
            storage: Arc::new(storage),
            planes,
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
