//! The import itself, on Linux and Vulkan.

use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};
use daedalus_dma_sys::{DmaBufRef, Identity, SyncFence};
use daedalus_runner::{UNSTAMPED, put_typed, take_typed};
use daedalus_types::Typed;
use daedalus_vk_sys::device::Extras;
use daedalus_vk_sys::image::{ImageError, ImageImport, ImageMemory, ImagePlane, import_image};
use daedalus_vk_sys::semaphore::Semaphore;
use daedalus_vk_sys::submit::Submitter;

use super::fence::{Acquired, DEFAULT_ACQUIRE_TIMEOUT, FenceWait, acquire};
use super::layout::{Described, Fourcc, ImageFormat, ImportError};
use super::modifiers::{Importer, MOD_LINEAR};
use super::refused::Refusable;
use crate::WgpuBackend;
use crate::context::Context;
use crate::memory::SPACE_TEXTURE;
use crate::texture::Texture;

/// One memory plane of an external image.
#[derive(Debug)]
pub struct ExternalPlane {
    /// The dma-buf the plane is in. Planes may share one.
    pub fd: OwnedFd,
    /// Where the plane's first row starts in it.
    pub offset: u64,
    /// Bytes from the start of one row to the start of the next.
    pub stride: u64,
}

/// An image in memory someone else owns, described for import.
#[derive(Debug)]
pub struct ExternalImage {
    pub width: u32,
    pub height: u32,
    pub fourcc: Fourcc,
    /// The DRM format modifier. `None` is the implicit layout, which is linear.
    pub modifier: Option<u64>,
    /// One for each memory plane of the modifier, in order.
    pub planes: Vec<ExternalPlane>,
    /// A sync_file that signals when the producer has finished writing.
    pub acquire_fence: Option<OwnedFd>,
    /// The bound on a host wait for that fence.
    pub acquire_timeout: Duration,
}

impl ExternalImage {
    pub fn new(width: u32, height: u32, fourcc: Fourcc, planes: Vec<ExternalPlane>) -> Self {
        ExternalImage {
            width,
            height,
            fourcc,
            modifier: None,
            planes,
            acquire_fence: None,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }

    fn described<'a>(&self, planes: &'a [(u64, u64)]) -> Described<'a> {
        Described {
            width: self.width,
            height: self.height,
            fourcc: self.fourcc,
            modifier: self.modifier,
            planes,
        }
    }
}

/// What an import is cached under: the memory, and how it was described.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Key {
    memory: Vec<Identity>,
    planes: Vec<(u64, u64)>,
    size: (u32, u32),
    fourcc: Fourcc,
    modifier: u64,
}

/// How often the import cache was used.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ImportStats {
    /// Imports that found the buffer already imported.
    pub hits: u64,
    /// Imports that reached the driver.
    pub imports: u64,
    /// Cached imports let go to make room.
    pub evictions: u64,
}

/// Imports a device keeps. A producer cycles through a few buffers, so a few are enough.
const KEPT: usize = 16;

/// A device's imports, by the memory they are of, and the ring their acquires go through.
#[derive(Default)]
pub(crate) struct Imports {
    kept: Vec<(Key, Arc<Texture>)>,
    stats: ImportStats,
    acquirer: Option<Submitter>,
}

impl Imports {
    pub(crate) fn stats(&self) -> ImportStats {
        self.stats
    }
}

fn wgpu_format(format: ImageFormat) -> wgpu::TextureFormat {
    match format {
        ImageFormat::R8 => wgpu::TextureFormat::R8Unorm,
        ImageFormat::Rg8 => wgpu::TextureFormat::Rg8Unorm,
        ImageFormat::Rgba8 => wgpu::TextureFormat::Rgba8Unorm,
        ImageFormat::Bgra8 => wgpu::TextureFormat::Bgra8Unorm,
        ImageFormat::Nv12 => wgpu::TextureFormat::NV12,
    }
}

/// Waits for the producer's fence the way this device does, and gives the semaphore the acquire
/// has to wait on when the device takes it.
fn settle_fence(
    context: &Context,
    fence: Option<OwnedFd>,
    timeout: Duration,
    on_device: bool,
) -> Result<Option<Semaphore>, ImportError> {
    let Some(fence) = fence else {
        return Ok(None);
    };
    // Only a real sync_file may reach a driver as a fence: anything else never signals.
    let fence = SyncFence::from_fd(fence)
        .map_err(|_| ImportError::invalid("the acquire fence isn't a sync_file"))?;
    let signaled = fence.is_signaled().unwrap_or(false);
    let how = FenceWait::resolve(on_device);
    let wait = |timeout| fence.wait(timeout).unwrap_or(false);
    match acquire(Some(signaled), how, timeout, wait)? {
        Acquired::Ready | Acquired::Waited => Ok(None),
        Acquired::OnDevice => match fence.as_fd().try_clone_to_owned() {
            // The device waits. If it turns the fence down after all, the host does.
            Ok(copy) => match Semaphore::import_sync_fd(&context.device, Some(copy)) {
                Ok(semaphore) => Ok(Some(semaphore)),
                Err(_) => match fence.wait(timeout) {
                    Ok(true) => Ok(None),
                    _ => Err(ImportError::FenceTimeout(timeout)),
                },
            },
            Err(_) => Err(ImportError::invalid(
                "the acquire fence couldn't be duplicated",
            )),
        },
    }
}

/// Imports `image` on `context`'s device, or finds the import of the same memory.
pub(crate) fn import_on(
    context: &Arc<Context>,
    image: ExternalImage,
) -> Result<Arc<Texture>, ImportError> {
    let planes: Vec<(u64, u64)> = image
        .planes
        .iter()
        .map(|plane| (plane.offset, plane.stride))
        .collect();
    let described = image.described(&planes);
    let layout = described.validate()?;
    let format_error = |reason: String| ImportError::UnsupportedFormat {
        fourcc: image.fourcc,
        modifier: image.modifier,
        reason,
    };
    let profile = &context.record.profile;
    let importer = Importer {
        family: profile.family,
        vendor: profile.vendor,
    };
    if let Some(why) = importer.refuses(&described) {
        return Err(format_error(why.into()));
    }
    let max = context.limits.max_texture_dimension_2d;
    if image.width > max || image.height > max {
        return Err(ImportError::invalid(format!(
            "an image of {}x{}, where this device's textures are at most {max} a side",
            image.width, image.height
        )));
    }
    let extras = Extras::of(&context.device)
        .ok_or_else(|| ImportError::Unsupported("the device isn't on Vulkan".into()))?;
    if !extras.dma_buf_import {
        return Err(ImportError::Unsupported(
            "this device can't import a dma-buf".into(),
        ));
    }
    let planar = layout.format.is_planar();
    if planar
        && !context
            .device
            .features()
            .contains(wgpu::Features::TEXTURE_FORMAT_NV12)
    {
        return Err(format_error(
            "this device can't sample NV12 as one image: import the Y plane as R8 and the UV plane as GR88".into(),
        ));
    }

    // Which dma-buf each plane is in. Planes in one buffer are one piece of memory.
    let mut ids = Vec::with_capacity(image.planes.len());
    for (index, plane) in image.planes.iter().enumerate() {
        let id = DmaBufRef::from_fd(plane.fd.as_fd())
            .identity()
            .map_err(|error| ImportError::invalid(format!("plane {index}'s fd: {error}")))?;
        ids.push(id);
    }
    let shared = ids.windows(2).all(|pair| pair[0] == pair[1]);
    let needed = importer.required_len(image.fourcc, &layout);
    let key = Key {
        memory: ids,
        planes: planes.clone(),
        size: (image.width, image.height),
        fourcc: image.fourcc,
        modifier: image.modifier.unwrap_or(MOD_LINEAR),
    };

    let mut imports = context.imports();
    if let Some(at) = imports.kept.iter().position(|(have, _)| *have == key) {
        // The same memory, described the same way: only its fence is new. The image is already
        // this device's, so the host waits for the producer.
        let found = Arc::clone(&imports.kept[at].1);
        imports.stats.hits += 1;
        drop(imports);
        settle_fence(context, image.acquire_fence, image.acquire_timeout, false)?;
        return Ok(found);
    }

    let mut memory = Vec::with_capacity(image.planes.len());
    let fds = image.planes.into_iter().map(|plane| plane.fd);
    let groups: Vec<(OwnedFd, u64)> = if shared {
        let need = needed.iter().copied().max().unwrap_or(0);
        fds.take(1).map(|fd| (fd, need)).collect()
    } else {
        fds.zip(needed.iter().copied()).collect()
    };
    for (index, (fd, need)) in groups.into_iter().enumerate() {
        let len = DmaBufRef::from_fd(fd.as_fd())
            .len()
            .map_err(|error| ImportError::invalid(format!("dma-buf {index}'s size: {error}")))?;
        if len < need {
            return Err(ImportError::invalid(format!(
                "dma-buf {index} is {len} bytes, and this device needs {need} for the image"
            )));
        }
        memory.push(ImageMemory { fd, len });
    }
    let fence = settle_fence(
        context,
        image.acquire_fence,
        image.acquire_timeout,
        extras.sync_fd_import,
    )?;

    if imports.acquirer.is_none() {
        let ring = Submitter::new(&context.device, &context.queue, &context.submit, 2)
            .map_err(|error| ImportError::Unsupported(error.to_string()))?;
        imports.acquirer = Some(ring);
    }
    let Some(acquirer) = imports.acquirer.as_ref() else {
        return Err(ImportError::Unsupported("no ring to acquire with".into()));
    };
    let mut usage = wgpu::TextureUsages::TEXTURE_BINDING;
    if !planar {
        usage |= wgpu::TextureUsages::COPY_SRC;
    }
    let vk_planes: Vec<ImagePlane> = planes
        .iter()
        .map(|&(offset, stride)| ImagePlane { offset, stride })
        .collect();
    let request = ImageImport {
        width: image.width,
        height: image.height,
        format: wgpu_format(layout.format),
        modifier: key.modifier,
        planes: &vk_planes,
        memory,
        usage,
        fence,
        keepalive: None,
    };
    let texture =
        import_image(&context.device, acquirer, request).map_err(|error| match error {
            ImageError::NoImport(why) => ImportError::Unsupported(why.into()),
            ImageError::Format(why) => format_error(why),
            ImageError::Short { needed, len } => ImportError::invalid(format!(
                "the driver needs {needed} bytes for this image, and the dma-buf has {len}"
            )),
            ImageError::Refused {
                reason,
                fd,
                driver_closed,
            } => {
                // Closed by exactly one side, whichever that is on this driver.
                Refusable::new(fd).refused(driver_closed);
                ImportError::Refused(reason)
            }
            ImageError::Vulkan(result) => ImportError::Refused(format!("Vulkan: {result:?}")),
        })?;
    context.count_submit();
    let texture = Arc::new(Texture::imported(
        Arc::clone(context),
        texture,
        layout.format,
        image.width,
        image.height,
    ));
    imports.stats.imports += 1;
    if imports.kept.len() >= KEPT {
        imports.kept.remove(0);
        imports.stats.evictions += 1;
    }
    imports.kept.push((key, Arc::clone(&texture)));
    Ok(texture)
}

impl WgpuBackend {
    /// Imports `image` as a texture on this backend's device, with no copy. The same memory
    /// described the same way is imported once: later calls find it.
    pub fn import_external(&self, image: ExternalImage) -> Result<Arc<Texture>, ImportError> {
        let context = self
            .context()
            .map_err(|error| ImportError::Unsupported(error.to_string()))?;
        import_on(&context, image)
    }

    /// How often this device's import cache was used. `None` while no device is open.
    pub fn import_stats(&self) -> Option<ImportStats> {
        self.context().ok().map(|context| context.imports().stats())
    }
}

/// A type that is an image in external memory. Its owner writes only `describe`: the transfer
/// that moves it onto the device is [`import_transfer`].
pub trait ImportExternal: Send + Sync + 'static {
    /// The image, with fds of its own: duplicate the ones this value keeps.
    fn describe(&self) -> Result<ExternalImage, ImportError>;
}

/// Imports each value of `T` it is given. What it passes on is a texture that holds that
/// value, so the producer's buffer is kept until the last reader lets go, and a region that
/// reads it keeps it until its work on the device is done.
struct ImportStep<T> {
    context: Option<Arc<Context>>,
    of: std::marker::PhantomData<fn(T)>,
}

impl<T: ImportExternal> OwnedStep for ImportStep<T> {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let mut fault = |reason: &str| {
            cx.fault(None, reason);
            StepEnd::Fault
        };
        let Some(value) = slots.cell(0).and_then(take_typed::<T>) else {
            return fault("an import was given a value of another type");
        };
        let context = match &self.context {
            Some(context) if !context.is_lost() => Arc::clone(context),
            _ => {
                let found = Context::latest().filter(|latest| !latest.is_lost());
                let Some(context) = found else {
                    return fault("an import has no device to go to");
                };
                self.context = Some(Arc::clone(&context));
                context
            }
        };
        let texture = match value
            .describe()
            .and_then(|image| import_on(&context, image))
        {
            Ok(texture) => texture,
            Err(error) => return fault(&error.to_string()),
        };
        let texture = Arc::new(texture.holding(value));
        let Some(out) = slots.cell(1) else {
            return fault("an import has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, texture);
        StepEnd::Done
    }
}

fn make_import<T: ImportExternal>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(ImportStep::<T> {
        context: None,
        of: std::marker::PhantomData,
    }))
}

/// The transfer that imports values of `T` from the memory space `from` as textures. Add it to
/// what a backend of yours offers, and a `T` that reaches a GPU node is imported on the way.
pub fn import_transfer<T: ImportExternal + Typed>(
    id: &'static str,
    from: &'static str,
) -> Transfer {
    Transfer {
        id,
        key: Some(T::KEY),
        from: std::borrow::Cow::Borrowed(from),
        to: std::borrow::Cow::Borrowed(SPACE_TEXTURE),
        cost: TransferCost::Import,
        make: make_import::<T>,
    }
}
