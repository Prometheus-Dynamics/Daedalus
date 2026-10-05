//! Zero-copy dmabuf import through wgpu-hal's Vulkan backend (`gpu-dmabuf`, Linux only).
//!
//! # How it works
//!
//! 1. Device creation: wgpu-hal 30 already enables `VK_KHR_external_memory_fd` and
//!    `VK_EXT_external_memory_dma_buf` whenever the adapter supports them, but not
//!    `VK_EXT_image_drm_format_modifier`. [`request_device`] opens the hal device with
//!    `open_with_callback` to add that extension (plus `VK_KHR_external_semaphore_fd` and
//!    `VK_EXT_queue_family_foreign` when available) and wraps it with
//!    `Adapter::create_device_from_hal`. It also requests `TEXTURE_FORMAT_NV12` when the adapter
//!    has it. Any failure falls back to plain `request_device`.
//! 2. Import: a pending acquire fence is imported as a semaphore (or waited for on the CPU, see
//!    below), then a `VkImage` is created with
//!    `VK_IMAGE_TILING_DRM_FORMAT_MODIFIER_EXT` and an explicit per-plane layout (offset/stride
//!    from the descriptor). Each dmabuf is imported with `VkImportMemoryFdInfoKHR`: one dedicated
//!    allocation when all planes share a dmabuf, or one allocation per memory plane
//!    (`VK_IMAGE_CREATE_DISJOINT_BIT`) when they do not. The image is handed to wgpu via
//!    `vulkan::Device::texture_from_raw` + `Device::create_texture_from_hal`, and a queue family
//!    acquire is submitted for it (`dmabuf/acquire.rs`).
//! 3. Lifetime: the hal texture gets a drop callback that destroys the image, frees the imported
//!    memory (which drops the kernel's dmabuf references taken by the import) and only then drops
//!    the caller's keepalive. wgpu-core runs it once the texture is dropped and no in-flight
//!    submission uses it.
//!
//! # NV12
//!
//! NV12 imports as one `wgpu::TextureFormat::NV12` texture (`G8_B8R8_2PLANE_420_UNORM`, created
//! `MUTABLE_FORMAT | EXTENDED_USAGE` like wgpu's own NV12 textures). Shaders read it through
//! per-plane views ([`texture_plane_views`](crate::texture_plane_views): Y as `R8Unorm`, UV as
//! `Rg8Unorm`); YCbCr-to-RGB conversion is up to the shader, no `VkSamplerYcbcrConversion` is
//! used. wgpu 30 allows only sampling for NV12 (no copies, storage or render targets), so such
//! images cannot be read back with `read_texture`. Devices without `TEXTURE_FORMAT_NV12` get
//! `UnsupportedFormat`; import the planes separately (`R8` + `GR88`) there.
//!
//! # Modifiers
//!
//! The descriptor lists one plane per *memory* plane of the modifier: the format planes, then any
//! aux planes (e.g. AMD DCC metadata), each with the offset/pitch the producer reported. They
//! become the `VkImageDrmFormatModifierExplicitCreateInfoEXT` plane layouts as given; the plane
//! count must match the modifier's `drmFormatModifierPlaneCount`. All memory planes in one dmabuf
//! bind one dedicated allocation; planes in different dmabufs make a `DISJOINT` image bound per
//! `MEMORY_PLANE_i` aspect (when the modifier has the `DISJOINT` feature).
//!
//! # Synchronization
//!
//! - **Acquire fence**: an already signaled fence is skipped. A pending one becomes a GPU-side wait
//!   where possible, see `acquire.rs` and `watcher.rs`:
//!   [`Timeline`](crate::AcquireFenceWait::Timeline) (timeline semaphore host-signaled by a
//!   watcher thread on fence or timeout) on devices with timeline semaphores, else
//!   [`SyncFd`](crate::AcquireFenceWait::SyncFd) (binary `SYNC_FD` semaphore, no timeout) with
//!   `VK_KHR_external_semaphore_fd`, else [`Cpu`](crate::AcquireFenceWait::Cpu): the import polls
//!   the fence, bounded by the descriptor's `acquire_timeout`, before any Vulkan object exists. An
//!   unbounded `acquire_timeout` (`Duration::MAX`) prefers `SyncFd` over `Timeline`.
//! - **Queue family transfers.** Every import submits a `FOREIGN` (or `EXTERNAL`) -> wgpu family
//!   ownership acquire, `GENERAL -> SHADER_READ_ONLY_OPTIMAL`, and registers the texture with wgpu
//!   in that state (`TextureUses::RESOURCE`, wgpu 30's `create_texture_from_hal` initial state), so
//!   wgpu never transitions it from `UNDEFINED` and every later barrier chains to the fence wait.
//!   Dropping the last handle submits the matching release back to the foreign family.
//! - **Layout on arrival.** The image is created `UNDEFINED` (the only option besides
//!   `PREINITIALIZED`), but its memory already holds the producer's frame, so the acquire names
//!   the layout the producer left it in. Dmabuf producers outside Vulkan have no layout; the
//!   convention of Mesa's WSI and of compositors (wlroots, Mutter) is `GENERAL` in the foreign
//!   family, which Mesa drivers keep compatible with the modifier (including its compression
//!   metadata). `UNDEFINED` would allow the driver to discard the contents and to reinitialize
//!   compression metadata; RADV happens to preserve DCC images either way on a foreign acquire
//!   (checked with the modifier round-trip test), other drivers need not. The release mirrors the
//!   acquire (`-> GENERAL`, to the foreign family).

use std::os::fd::{AsFd, AsRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use ash::{ext, khr, vk};
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use super::acquire::{Acquire, OPTIONAL_EXTENSIONS};
use super::image::{ImportRequest, create_imported_texture};
use crate::{
    DRM_FORMAT_MOD_LINEAR, ExternalFrameDescriptor, ExternalImportError, ExternalImportSupport,
    ExternalPlane, GpuFormat, GpuImageHandle, GpuUsage, WgpuBackend, external::sync_file_signaled,
};

/// Device extensions an import needs. The first two are enabled by wgpu-hal itself when present.
const REQUIRED_EXTENSIONS: [&std::ffi::CStr; 3] = [
    khr::external_memory_fd::NAME,
    ext::external_memory_dma_buf::NAME,
    ext::image_drm_format_modifier::NAME,
];

/// Request the wgpu device, adding `VK_EXT_image_drm_format_modifier` on Vulkan adapters that
/// support the full dmabuf import extension set, and `TEXTURE_FORMAT_NV12` when available.
pub(in crate::wgpu_backend) async fn request_device(
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    let mut desc = desc.clone();
    desc.required_features |= adapter.features() & wgpu::Features::TEXTURE_FORMAT_NV12;
    if let Some(open) = open_with_modifier_extension(adapter, &desc) {
        // SAFETY: `open` was created from this adapter's hal adapter with `desc`'s features and
        // limits; the callback only appended an extension the adapter reports as supported.
        match unsafe { adapter.create_device_from_hal::<Vulkan>(open, &desc) } {
            Ok(pair) => return Ok(pair),
            Err(err) => tracing::warn!(
                target: "daedalus_gpu::dmabuf",
                error = %err,
                "device with dmabuf extensions rejected; falling back to default device"
            ),
        }
    }
    adapter.request_device(&desc).await
}

fn open_with_modifier_extension(
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Option<wgpu::hal::OpenDevice<Vulkan>> {
    // SAFETY: the hal adapter is only used to open a device; nothing is destroyed through it.
    let hal_adapter = unsafe { adapter.as_hal::<Vulkan>() }?;
    let caps = hal_adapter.physical_device_capabilities();
    if caps.properties().api_version < vk::API_VERSION_1_1
        || hal_adapter.shared_instance().instance_api_version() < vk::API_VERSION_1_1
    {
        return None;
    }
    if let Some(missing) = REQUIRED_EXTENSIONS
        .iter()
        .find(|ext| !caps.supports_extension(ext))
    {
        tracing::debug!(
            target: "daedalus_gpu::dmabuf",
            extension = ?missing,
            "adapter lacks a dmabuf import extension; using default device"
        );
        return None;
    }
    let extensions: Vec<&'static std::ffi::CStr> = REQUIRED_EXTENSIONS
        .into_iter()
        .chain(OPTIONAL_EXTENSIONS)
        .filter(|ext| caps.supports_extension(ext))
        .collect();
    let callback = Box::new(move |args: hal_vk::CreateDeviceCallbackArgs<'_, '_, '_>| {
        for ext in &extensions {
            if !args.extensions.contains(ext) {
                args.extensions.push(ext);
            }
        }
    });
    // SAFETY: the callback only adds extensions that `supports_extension` confirmed; it removes
    // nothing and does not touch features.
    let opened = unsafe {
        hal_adapter.open_with_callback(
            desc.required_features,
            &desc.required_limits,
            &desc.memory_hints,
            Some(callback),
        )
    };
    match opened {
        Ok(open) => Some(open),
        Err(err) => {
            tracing::warn!(
                target: "daedalus_gpu::dmabuf",
                error = %err,
                "opening Vulkan device with dmabuf extensions failed"
            );
            None
        }
    }
}

/// Per-device import state; `None` when import is unsupported.
pub(in crate::wgpu_backend) type ImportState = Option<Acquire>;

/// Decide whether the created device can import dmabufs, and how acquire fences are waited for.
pub(in crate::wgpu_backend) fn probe(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
) -> (ExternalImportSupport, ImportState) {
    // SAFETY: only queries are made through the hal device (and an ash device handle cloned).
    let Some(hal_dev) = (unsafe { device.as_hal::<Vulkan>() }) else {
        let reason = "the wgpu device is not using the Vulkan backend (dmabuf import needs Vulkan)";
        return (ExternalImportSupport::unsupported(reason), None);
    };
    match probe_support(&hal_dev) {
        Ok(()) => {
            let acquire = Acquire::new(device, queue, &hal_dev);
            let support = ExternalImportSupport::Supported {
                acquire_fence: acquire.fence_wait(),
            };
            (support, Some(acquire))
        }
        Err(reason) => (ExternalImportSupport::unsupported(reason), None),
    }
}

fn probe_support(hal_dev: &hal_vk::Device) -> Result<(), String> {
    let enabled = hal_dev.enabled_device_extensions();
    let missing: Vec<String> = REQUIRED_EXTENSIONS
        .iter()
        .filter(|ext| !enabled.contains(ext))
        .map(|ext| ext.to_string_lossy().into_owned())
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "Vulkan device lacks required extensions: {}",
            missing.join(", ")
        ));
    }
    if hal_dev.shared_instance().instance_api_version() < vk::API_VERSION_1_1 {
        return Err("Vulkan instance older than 1.1".into());
    }
    Ok(())
}

pub(in crate::wgpu_backend) fn import(
    backend: &WgpuBackend,
    mut desc: ExternalFrameDescriptor,
) -> Result<GpuImageHandle, ExternalImportError> {
    let acquire = backend
        .dmabuf
        .as_ref()
        .ok_or_else(|| ExternalImportError::Unsupported {
            reason: "dmabuf import is unavailable on this device".into(),
        })?;
    let layout = desc.validate()?;
    let max = backend.caps.max_texture_dimension;
    if desc.width > max || desc.height > max {
        return Err(ExternalImportError::invalid(format!(
            "extent {}x{} exceeds max texture dimension {max}",
            desc.width, desc.height
        )));
    }
    let format_error = |reason: String| ExternalImportError::UnsupportedFormat {
        fourcc: desc.fourcc,
        modifier: desc.modifier,
        reason,
    };
    let planar = layout.is_multi_planar();
    if planar
        && !backend
            .device
            .features()
            .contains(wgpu::Features::TEXTURE_FORMAT_NV12)
    {
        return Err(format_error(
            "the device lacks wgpu TEXTURE_FORMAT_NV12 (sampleable G8_B8R8_2PLANE_420_UNORM); \
             import the Y plane as R8 and the UV plane as GR88 instead"
                .into(),
        ));
    }
    let features = backend
        .caps
        .format_features
        .iter()
        .find(|f| f.format == layout.format)
        .copied();
    let wants = |u: GpuUsage| desc.usage.contains(u);
    let format_ok =
        |check: fn(&crate::GpuFormatFeatures) -> bool| features.is_some_and(|f| check(&f));
    if (wants(GpuUsage::STORAGE) && !format_ok(|f| f.storage))
        || (wants(GpuUsage::RENDER_TARGET) && !format_ok(|f| f.renderable))
    {
        return Err(format_error(format!(
            "{:?} does not allow the requested usage {:?}",
            layout.format, desc.usage
        )));
    }
    let usage = UsageSet::new(desc.usage, planar);
    let wgpu_format = crate::wgpu_backend::map_format(layout.format);
    let vk_format = map_vk_format(layout.format)
        .ok_or_else(|| format_error(format!("no Vulkan format for {:?}", layout.format)))?;
    // A fence that already signaled needs no wait at all; a pending one becomes a GPU wait where
    // possible, else the import blocks here (before any Vulkan object exists).
    let gpu_fence = match desc.acquire_fence.take() {
        Some(fence) if !sync_file_signaled(fence.as_fd()) => {
            match acquire.gpu_fence(fence, desc.acquire_timeout) {
                Ok(gpu) => Some(gpu),
                Err(fence) => {
                    desc.acquire_fence = Some(fence);
                    desc.wait_acquire_fence()?;
                    None
                }
            }
        }
        _ => None,
    };

    let ExternalFrameDescriptor {
        width,
        height,
        fourcc,
        modifier,
        planes,
        label,
        keepalive,
        ..
    } = desc;
    let plane_layouts = planes
        .iter()
        .map(|plane| vk::SubresourceLayout {
            offset: plane.offset,
            size: 0,
            row_pitch: plane.stride,
            array_pitch: 0,
            depth_pitch: 0,
        })
        .collect();
    let (fds, disjoint) = plane_memory(planes, &layout.min_len)?;

    let hal_texture = {
        // SAFETY: the hal device outlives this block; resources created through it are handed to
        // wgpu below, which owns them from then on.
        let hal_dev = unsafe { backend.device.as_hal::<Vulkan>() }.ok_or_else(|| {
            ExternalImportError::Unsupported {
                reason: "the wgpu device is not using the Vulkan backend".into(),
            }
        })?;
        let request = ImportRequest {
            width,
            height,
            fourcc,
            modifier: modifier.unwrap_or(DRM_FORMAT_MOD_LINEAR),
            plane_layouts,
            disjoint,
            wgpu_format,
            vk_format,
            view_formats: view_formats(wgpu_format, vk_format),
            vk_usage: usage.vk,
            format_features: usage.format_features,
            hal_usage: usage.hal,
            label: label.clone(),
        };
        // SAFETY: `hal_dev` is the device the wgpu device wraps; see `create_imported_texture`.
        unsafe { create_imported_texture(&hal_dev, &request, fds, keepalive)? }
    };

    let wgpu_desc = wgpu::TextureDescriptor {
        label: label.as_deref(),
        size: wgpu::Extent3d {
            width,
            height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu_format,
        usage: usage.wgpu,
        view_formats: &[],
    };
    // SAFETY: the hal texture was created on this device with exactly `wgpu_desc`'s extent,
    // format, and usages, and its memory is bound to initialized (imported) contents. Its state
    // is `RESOURCE` (`SHADER_READ_ONLY_OPTIMAL`) as of the acquire submitted right below, before
    // anyone else can use the texture.
    let texture = unsafe {
        backend.device.create_texture_from_hal::<Vulkan>(
            hal_texture,
            &wgpu_desc,
            wgpu::wgt::TextureUses::RESOURCE,
        )
    };
    let texture = Arc::new(texture);
    let pending = gpu_fence.is_some();
    let (mut handle, token) = backend.register_gpu_texture_with(
        texture.clone(),
        layout.format,
        width,
        height,
        usage.wgpu,
        |resource| acquire.token(resource, pending),
    );
    acquire.submit(&texture, &token, gpu_fence)?;
    handle.label = label;
    Ok(handle)
}

#[cfg(test)]
impl WgpuBackend {
    /// Switch an import-capable backend to another acquire-fence wait mode; `false` when the
    /// device does not support it.
    pub(in crate::wgpu_backend) fn set_fence_wait(
        &mut self,
        mode: crate::AcquireFenceWait,
    ) -> bool {
        let Some(acquire) = &mut self.dmabuf else {
            return false;
        };
        let available = acquire.set_fence_wait(mode);
        self.dmabuf_support = ExternalImportSupport::Supported {
            acquire_fence: acquire.fence_wait(),
        };
        available
    }
}

/// The dmabufs backing the planes with their sizes: one when every plane lives in the same dmabuf
/// (same or `dup`ed fd), otherwise one per plane (`disjoint`).
fn plane_memory(
    planes: Vec<ExternalPlane>,
    min_len: &[u64],
) -> Result<(Vec<(OwnedFd, u64)>, bool), ExternalImportError> {
    let identity = |plane: &ExternalPlane| {
        std::fs::File::from(plane.fd.try_clone()?)
            .metadata()
            .map(|m| (m.dev(), m.ino()))
    };
    let ids = planes
        .iter()
        .map(identity)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ExternalImportError::invalid(format!("cannot stat plane fd: {err}")))?;
    let shared = ids.windows(2).all(|w| w[0] == w[1]);
    let groups: Vec<(OwnedFd, u64)> = if shared {
        let need = min_len.iter().copied().max().unwrap_or(0);
        let fd = planes.into_iter().next().map(|p| p.fd);
        fd.map(|fd| (fd, need)).into_iter().collect()
    } else {
        planes
            .into_iter()
            .map(|p| p.fd)
            .zip(min_len.iter().copied())
            .collect()
    };
    let fds = groups
        .into_iter()
        .enumerate()
        .map(|(index, (fd, need))| {
            let len = dmabuf_len(&fd)?;
            if len < need {
                return Err(ExternalImportError::invalid(format!(
                    "dmabuf {index} is {len} bytes but the plane layout needs {need}"
                )));
            }
            Ok((fd, len))
        })
        .collect::<Result<_, _>>()?;
    Ok((fds, !shared))
}

/// Size of a dmabuf (`lseek(SEEK_END)`, which dmabufs support); the offset is reset to 0.
fn dmabuf_len(fd: &OwnedFd) -> Result<u64, ExternalImportError> {
    // SAFETY: plain lseek calls on a valid descriptor.
    let len = unsafe { libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_END) };
    // SAFETY: as above.
    if len < 0 || unsafe { libc::lseek(fd.as_raw_fd(), 0, libc::SEEK_SET) } < 0 {
        return Err(ExternalImportError::invalid(format!(
            "cannot determine dmabuf size (lseek): {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(len as u64)
}

/// Usage flags expressed for every layer that needs them.
struct UsageSet {
    wgpu: wgpu::TextureUsages,
    hal: wgpu::wgt::TextureUses,
    vk: vk::ImageUsageFlags,
    format_features: vk::FormatFeatureFlags,
}

impl UsageSet {
    /// Imported frames are always sampleable; single-plane ones are also copy sources (wgpu 30
    /// only samples multi-planar textures) and take the requested upload/storage/render usages.
    fn new(usage: GpuUsage, planar: bool) -> Self {
        let mut wgpu = wgpu::TextureUsages::TEXTURE_BINDING;
        if !planar {
            wgpu |= wgpu::TextureUsages::COPY_SRC
                | crate::wgpu_backend::mapping::texture_usage_flags(
                    usage & (GpuUsage::UPLOAD | GpuUsage::STORAGE | GpuUsage::RENDER_TARGET),
                );
        }
        Self::from_wgpu(wgpu)
    }

    /// The hal/Vulkan usages and Vulkan format features matching `wgpu` usages.
    fn from_wgpu(wgpu: wgpu::TextureUsages) -> Self {
        use vk::FormatFeatureFlags as F;
        use vk::ImageUsageFlags as V;
        use wgpu::TextureUsages as W;
        use wgpu::wgt::TextureUses as H;
        let layers = [
            (
                W::TEXTURE_BINDING,
                H::RESOURCE,
                V::SAMPLED,
                F::SAMPLED_IMAGE,
            ),
            (W::COPY_SRC, H::COPY_SRC, V::TRANSFER_SRC, F::TRANSFER_SRC),
            (W::COPY_DST, H::COPY_DST, V::TRANSFER_DST, F::TRANSFER_DST),
            (
                W::STORAGE_BINDING,
                H::STORAGE_READ_ONLY | H::STORAGE_WRITE_ONLY | H::STORAGE_READ_WRITE,
                V::STORAGE,
                F::STORAGE_IMAGE,
            ),
            (
                W::RENDER_ATTACHMENT,
                H::COLOR_TARGET,
                V::COLOR_ATTACHMENT,
                F::COLOR_ATTACHMENT,
            ),
        ];
        layers.into_iter().filter(|(w, ..)| wgpu.contains(*w)).fold(
            Self {
                wgpu,
                hal: H::empty(),
                vk: V::empty(),
                format_features: F::empty(),
            },
            |mut set, (_, hal, vk, features)| {
                set.hal |= hal;
                set.vk |= vk;
                set.format_features |= features;
                set
            },
        )
    }
}

/// Formats the image is viewed with: for a multi-planar format the format itself and its plane
/// formats (the per-plane views), declared up front because `MUTABLE_FORMAT` images with a DRM
/// format modifier need a `VkImageFormatListCreateInfo`; empty otherwise.
fn view_formats(format: wgpu::TextureFormat, vk_format: vk::Format) -> Vec<vk::Format> {
    let Some(planes) = format.planes() else {
        return Vec::new();
    };
    let plane_formats = (0..planes)
        .filter_map(wgpu::TextureAspect::from_plane)
        .filter_map(|aspect| format.aspect_specific_format(aspect))
        .filter_map(crate::wgpu_backend::gpu_format_from_wgpu)
        .filter_map(map_vk_format);
    std::iter::once(vk_format).chain(plane_formats).collect()
}

/// Vulkan format of the importable formats (those a `DrmFourcc` maps to).
fn map_vk_format(format: GpuFormat) -> Option<vk::Format> {
    Some(match format {
        GpuFormat::R8Unorm => vk::Format::R8_UNORM,
        GpuFormat::Rg8Unorm => vk::Format::R8G8_UNORM,
        GpuFormat::Rgba8Unorm => vk::Format::R8G8B8A8_UNORM,
        GpuFormat::Bgra8Unorm => vk::Format::B8G8R8A8_UNORM,
        GpuFormat::Nv12 => vk::Format::G8_B8R8_2PLANE_420_UNORM,
        GpuFormat::Rgba16Float | GpuFormat::Depth24Stencil8 => return None,
    })
}
