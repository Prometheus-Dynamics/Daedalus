//! Zero-copy dmabuf import through wgpu-hal's Vulkan backend (`gpu-dmabuf`, Linux only).
//!
//! # How it works
//!
//! 1. Device creation: wgpu-hal 29 already enables `VK_KHR_external_memory_fd` and
//!    `VK_EXT_external_memory_dma_buf` whenever the adapter supports them, but not
//!    `VK_EXT_image_drm_format_modifier`. [`request_device`] opens the hal device with
//!    `open_with_callback` to add that extension and wraps it with
//!    `Adapter::create_device_from_hal`. Any failure falls back to plain `request_device`.
//! 2. Import: a `VkImage` is created with `VK_IMAGE_TILING_DRM_FORMAT_MODIFIER_EXT` and an
//!    explicit plane layout (offset/stride from the descriptor), the dmabuf fd is imported with
//!    `VkImportMemoryFdInfoKHR` into a dedicated allocation, bound, and handed to wgpu via
//!    `vulkan::Device::texture_from_raw` + `Device::create_texture_from_hal`.
//! 3. Lifetime: the hal texture gets a drop callback that destroys the image, frees the imported
//!    memory (which drops the kernel's dmabuf reference taken by the import) and only then drops
//!    the caller's keepalive. wgpu-core runs it once the texture is dropped and no in-flight
//!    submission uses it.
//!
//! # Known gaps
//!
//! - No queue-family-foreign ownership transfer or explicit dmabuf fence sync: wgpu tracks the
//!   imported image as a fresh texture and its first barrier transitions from
//!   `VK_IMAGE_LAYOUT_UNDEFINED`. That is content-preserving on drivers without compression
//!   metadata for the imported modifier (v3dv, RADV/ANV with `LINEAR`), and callers must hand
//!   over frames whose producer writes have completed (V4L2/libcamera dequeued buffers are).
//! - Single memory plane only; multi-planar YUV is imported plane by plane.

use std::io::{Seek, SeekFrom};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::sync::Arc;

use ash::{ext, khr, vk};
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use crate::{
    DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalImportError,
    ExternalImportSupport, ExternalKeepalive, GpuImageHandle, GpuUsage, WgpuBackend,
};

/// Device extensions an import needs. The first two are enabled by wgpu-hal itself when present.
const REQUIRED_EXTENSIONS: [&std::ffi::CStr; 3] = [
    khr::external_memory_fd::NAME,
    ext::external_memory_dma_buf::NAME,
    ext::image_drm_format_modifier::NAME,
];

const DMA_BUF: vk::ExternalMemoryHandleTypeFlags = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;

/// Request the wgpu device, adding `VK_EXT_image_drm_format_modifier` on Vulkan adapters that
/// support the full dmabuf import extension set.
pub(in crate::wgpu_backend) async fn request_device(
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    if let Some(open) = open_with_modifier_extension(adapter, desc) {
        // SAFETY: `open` was created from this adapter's hal adapter with `desc`'s features and
        // limits; the callback only appended an extension the adapter reports as supported.
        match unsafe { adapter.create_device_from_hal::<Vulkan>(open, desc) } {
            Ok(pair) => return Ok(pair),
            Err(err) => tracing::warn!(
                target: "daedalus_gpu::dmabuf",
                error = %err,
                "device with dmabuf extensions rejected; falling back to default device"
            ),
        }
    }
    adapter.request_device(desc).await
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
    let callback = Box::new(|args: hal_vk::CreateDeviceCallbackArgs<'_, '_, '_>| {
        for ext in REQUIRED_EXTENSIONS {
            if !args.extensions.contains(&ext) {
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

/// Decide whether the created device can import dmabufs.
pub(in crate::wgpu_backend) fn probe_support(device: &wgpu::Device) -> ExternalImportSupport {
    // SAFETY: only read-only queries are made through the hal device.
    let Some(hal_dev) = (unsafe { device.as_hal::<Vulkan>() }) else {
        return ExternalImportSupport::unsupported(
            "the wgpu device is not using the Vulkan backend (dmabuf import needs Vulkan)",
        );
    };
    let enabled = hal_dev.enabled_device_extensions();
    let missing: Vec<String> = REQUIRED_EXTENSIONS
        .iter()
        .filter(|ext| !enabled.contains(ext))
        .map(|ext| ext.to_string_lossy().into_owned())
        .collect();
    if !missing.is_empty() {
        return ExternalImportSupport::unsupported(format!(
            "Vulkan device lacks required extensions: {}",
            missing.join(", ")
        ));
    }
    if hal_dev.shared_instance().instance_api_version() < vk::API_VERSION_1_1 {
        return ExternalImportSupport::unsupported("Vulkan instance older than 1.1");
    }
    ExternalImportSupport::Supported
}

/// Everything the Vulkan side needs, resolved from the descriptor.
struct ImportRequest {
    width: u32,
    height: u32,
    fourcc: DrmFourcc,
    modifier: u64,
    offset: u64,
    stride: u64,
    dmabuf_len: u64,
    wgpu_format: wgpu::TextureFormat,
    vk_format: vk::Format,
    vk_usage: vk::ImageUsageFlags,
    format_features: vk::FormatFeatureFlags,
    hal_usage: wgpu::wgt::TextureUses,
    label: Option<String>,
}

impl ImportRequest {
    fn format_error(&self, reason: impl Into<String>) -> ExternalImportError {
        ExternalImportError::UnsupportedFormat {
            fourcc: self.fourcc,
            modifier: Some(self.modifier),
            reason: reason.into(),
        }
    }
}

pub(in crate::wgpu_backend) fn import(
    backend: &WgpuBackend,
    desc: ExternalFrameDescriptor,
) -> Result<GpuImageHandle, ExternalImportError> {
    let layout = desc.validate()?;
    let max = backend.caps.max_texture_dimension;
    if desc.width > max || desc.height > max {
        return Err(ExternalImportError::invalid(format!(
            "extent {}x{} exceeds max texture dimension {max}",
            desc.width, desc.height
        )));
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
        return Err(ExternalImportError::UnsupportedFormat {
            fourcc: desc.fourcc,
            modifier: desc.modifier,
            reason: format!(
                "{:?} does not allow the requested usage {:?}",
                layout.format, desc.usage
            ),
        });
    }
    let usage = UsageSet::from_gpu(desc.usage);
    let wgpu_format = crate::wgpu_backend::map_format(layout.format);

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
    let plane = planes
        .into_iter()
        .next()
        .ok_or_else(|| ExternalImportError::invalid("no planes"))?;
    let (fd, dmabuf_len) = dmabuf_len(plane.fd)?;
    if dmabuf_len < layout.min_len {
        return Err(ExternalImportError::invalid(format!(
            "dmabuf is {dmabuf_len} bytes but the plane layout needs {}",
            layout.min_len
        )));
    }

    let vk_format =
        map_vk_format(wgpu_format).ok_or_else(|| ExternalImportError::UnsupportedFormat {
            fourcc,
            modifier,
            reason: format!("no Vulkan format for {wgpu_format:?}"),
        })?;
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
            offset: plane.offset,
            stride: plane.stride,
            dmabuf_len,
            wgpu_format,
            vk_format,
            vk_usage: usage.vk,
            format_features: usage.format_features,
            hal_usage: usage.hal,
            label: label.clone(),
        };
        // SAFETY: `hal_dev` is the device the wgpu device wraps; see `create_imported_texture`.
        unsafe { create_imported_texture(&hal_dev, &request, fd, keepalive)? }
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
    // format, and usages, and its memory is bound to initialized (imported) contents.
    let texture = unsafe {
        backend
            .device
            .create_texture_from_hal::<Vulkan>(hal_texture, &wgpu_desc)
    };
    let mut handle =
        backend.register_texture(Arc::new(texture), wgpu_format, width, height, usage.wgpu);
    handle.label = label;
    Ok(handle)
}

/// Usage flags expressed for every layer that needs them.
struct UsageSet {
    wgpu: wgpu::TextureUsages,
    hal: wgpu::wgt::TextureUses,
    vk: vk::ImageUsageFlags,
    format_features: vk::FormatFeatureFlags,
}

impl UsageSet {
    fn from_gpu(usage: GpuUsage) -> Self {
        use vk::FormatFeatureFlags as F;
        use vk::ImageUsageFlags as V;
        use wgpu::TextureUsages as W;
        use wgpu::wgt::TextureUses as H;
        // Imported frames are always sampleable and readable back.
        let mut set = Self {
            wgpu: W::TEXTURE_BINDING | W::COPY_SRC,
            hal: H::RESOURCE | H::COPY_SRC,
            vk: V::SAMPLED | V::TRANSFER_SRC,
            format_features: F::SAMPLED_IMAGE | F::TRANSFER_SRC,
        };
        if usage.contains(GpuUsage::UPLOAD) {
            set.wgpu |= W::COPY_DST;
            set.hal |= H::COPY_DST;
            set.vk |= V::TRANSFER_DST;
            set.format_features |= F::TRANSFER_DST;
        }
        if usage.contains(GpuUsage::STORAGE) {
            set.wgpu |= W::STORAGE_BINDING;
            set.hal |= H::STORAGE_READ_ONLY | H::STORAGE_WRITE_ONLY | H::STORAGE_READ_WRITE;
            set.vk |= V::STORAGE;
            set.format_features |= F::STORAGE_IMAGE;
        }
        if usage.contains(GpuUsage::RENDER_TARGET) {
            set.wgpu |= W::RENDER_ATTACHMENT;
            set.hal |= H::COLOR_TARGET;
            set.vk |= V::COLOR_ATTACHMENT;
            set.format_features |= F::COLOR_ATTACHMENT;
        }
        set
    }
}

fn map_vk_format(format: wgpu::TextureFormat) -> Option<vk::Format> {
    Some(match format {
        wgpu::TextureFormat::R8Unorm => vk::Format::R8_UNORM,
        wgpu::TextureFormat::Rg8Unorm => vk::Format::R8G8_UNORM,
        wgpu::TextureFormat::Rgba8Unorm => vk::Format::R8G8B8A8_UNORM,
        wgpu::TextureFormat::Bgra8Unorm => vk::Format::B8G8R8A8_UNORM,
        _ => return None,
    })
}

/// Size of the dmabuf (`lseek(SEEK_END)`, which dmabufs support), returning the fd unchanged.
fn dmabuf_len(fd: OwnedFd) -> Result<(OwnedFd, u64), ExternalImportError> {
    let mut file = std::fs::File::from(fd);
    let len = file
        .seek(SeekFrom::End(0))
        .and_then(|len| file.seek(SeekFrom::Start(0)).map(|_| len))
        .map_err(|err| {
            ExternalImportError::invalid(format!("cannot determine dmabuf size (lseek): {err}"))
        })?;
    Ok((OwnedFd::from(file), len))
}

/// Create a `VkImage` over the dmabuf and wrap it as a wgpu-hal texture.
///
/// # Safety
///
/// `hal_dev` must be the hal device behind the wgpu device the texture will be registered with.
/// On success, ownership of `fd` has moved into the Vulkan allocation and `keepalive` into the
/// texture's drop callback.
unsafe fn create_imported_texture(
    hal_dev: &hal_vk::Device,
    req: &ImportRequest,
    fd: OwnedFd,
    keepalive: Option<ExternalKeepalive>,
) -> Result<hal_vk::Texture, ExternalImportError> {
    let device = hal_dev.raw_device();
    let instance = hal_dev.shared_instance().raw_instance();
    let phys = hal_dev.raw_physical_device();

    // SAFETY: `phys` belongs to `instance`; queries only.
    unsafe {
        check_modifier(instance, phys, req)?;
        check_image_format(instance, phys, req)?;
    }

    let fd_api = khr::external_memory_fd::Device::new(instance, device);
    let mut fd_props = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: `fd` is a valid open descriptor for the duration of the call.
    unsafe { fd_api.get_memory_fd_properties(DMA_BUF, fd.as_raw_fd(), &mut fd_props) }.map_err(
        |err| ExternalImportError::ImportFailed {
            reason: format!("fd is not an importable dmabuf (vkGetMemoryFdPropertiesKHR: {err})"),
        },
    )?;

    let plane_layouts = [vk::SubresourceLayout {
        offset: req.offset,
        size: 0,
        row_pitch: req.stride,
        array_pitch: 0,
        depth_pitch: 0,
    }];
    let mut explicit = vk::ImageDrmFormatModifierExplicitCreateInfoEXT::default()
        .drm_format_modifier(req.modifier)
        .plane_layouts(&plane_layouts);
    let mut external = vk::ExternalMemoryImageCreateInfo::default().handle_types(DMA_BUF);
    let image_info = vk::ImageCreateInfo::default()
        .image_type(vk::ImageType::TYPE_2D)
        .format(req.vk_format)
        .extent(vk::Extent3D {
            width: req.width,
            height: req.height,
            depth: 1,
        })
        .mip_levels(1)
        .array_layers(1)
        .samples(vk::SampleCountFlags::TYPE_1)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(req.vk_usage)
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .initial_layout(vk::ImageLayout::UNDEFINED)
        .push_next(&mut external)
        .push_next(&mut explicit);
    // SAFETY: valid create info; the image is destroyed on every error path below.
    let image = unsafe { device.create_image(&image_info, None) }.map_err(|err| {
        req.format_error(format!("vkCreateImage rejected the plane layout: {err}"))
    })?;
    let destroy_image = || {
        // SAFETY: `image` was created above and is not referenced by anything else yet.
        unsafe { device.destroy_image(image, None) }
    };

    // SAFETY: `image` is a valid image of `device`.
    let reqs = unsafe { device.get_image_memory_requirements(image) };
    let type_bits = reqs.memory_type_bits & fd_props.memory_type_bits;
    if type_bits == 0 {
        destroy_image();
        return Err(ExternalImportError::ImportFailed {
            reason: format!(
                "no memory type compatible with both the image (0x{:x}) and the dmabuf (0x{:x})",
                reqs.memory_type_bits, fd_props.memory_type_bits
            ),
        });
    }
    if reqs.size > req.dmabuf_len {
        destroy_image();
        return Err(ExternalImportError::invalid(format!(
            "driver needs {} bytes for this image but the dmabuf has {}",
            reqs.size, req.dmabuf_len
        )));
    }

    let raw_fd = fd.into_raw_fd();
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(DMA_BUF)
        .fd(raw_fd);
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let alloc_info = vk::MemoryAllocateInfo::default()
        .allocation_size(reqs.size)
        .memory_type_index(type_bits.trailing_zeros())
        .push_next(&mut import)
        .push_next(&mut dedicated);
    // SAFETY: on success Vulkan owns `raw_fd`; on failure it does not, so we close it.
    let memory = match unsafe { device.allocate_memory(&alloc_info, None) } {
        Ok(memory) => memory,
        Err(err) => {
            // SAFETY: Vulkan did not take ownership of `raw_fd` on failure.
            drop(unsafe { OwnedFd::from_raw_fd(raw_fd) });
            destroy_image();
            return Err(ExternalImportError::ImportFailed {
                reason: format!("vkAllocateMemory (dmabuf import) failed: {err}"),
            });
        }
    };
    // SAFETY: dedicated allocation for `image`, offset 0 (the plane offset is in the layout).
    if let Err(err) = unsafe { device.bind_image_memory(image, memory, 0) } {
        // SAFETY: neither handle is referenced elsewhere yet.
        unsafe { device.free_memory(memory, None) };
        destroy_image();
        return Err(ExternalImportError::ImportFailed {
            reason: format!("vkBindImageMemory failed: {err}"),
        });
    }

    let owner = device.clone();
    let drop_callback: wgpu::hal::DropCallback = Box::new(move || {
        // SAFETY: wgpu-hal calls this exactly once, from `destroy_texture`, after wgpu-core has
        // retired every submission using the texture. Nothing else references these handles.
        unsafe {
            owner.destroy_image(image, None);
            owner.free_memory(memory, None);
        }
        // The producer may recycle its buffer only now.
        drop(keepalive);
    });
    let hal_desc = wgpu::hal::TextureDescriptor {
        label: req.label.as_deref(),
        size: wgpu::Extent3d {
            width: req.width,
            height: req.height,
            depth_or_array_layers: 1,
        },
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: req.wgpu_format,
        usage: req.hal_usage,
        memory_flags: wgpu::hal::MemoryFlags::empty(),
        view_formats: Vec::new(),
    };
    // SAFETY: `image` was created to match `hal_desc`; with a drop callback wgpu-hal does not
    // destroy the image, and `TextureMemory::External` stops it from freeing `memory`; both are
    // released by the callback.
    Ok(unsafe {
        hal_dev.texture_from_raw(
            image,
            &hal_desc,
            Some(drop_callback),
            hal_vk::TextureMemory::External,
        )
    })
}

/// Check that the device advertises the modifier for this format with the needed features.
///
/// # Safety
///
/// `phys` must belong to `instance`.
unsafe fn check_modifier(
    instance: &ash::Instance,
    phys: vk::PhysicalDevice,
    req: &ImportRequest,
) -> Result<(), ExternalImportError> {
    let mut count_list = vk::DrmFormatModifierPropertiesListEXT::default();
    let mut props = vk::FormatProperties2::default().push_next(&mut count_list);
    // SAFETY: valid physical device and out-structure chain.
    unsafe { instance.get_physical_device_format_properties2(phys, req.vk_format, &mut props) };
    let count = count_list.drm_format_modifier_count as usize;
    let mut modifiers = vec![vk::DrmFormatModifierPropertiesEXT::default(); count];
    let mut list = vk::DrmFormatModifierPropertiesListEXT::default()
        .drm_format_modifier_properties(&mut modifiers);
    let mut props = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: as above; `modifiers` has room for `count` entries.
    unsafe { instance.get_physical_device_format_properties2(phys, req.vk_format, &mut props) };
    let written = list.drm_format_modifier_count as usize;
    modifiers.truncate(written);

    let Some(entry) = modifiers
        .iter()
        .find(|m| m.drm_format_modifier == req.modifier)
    else {
        let known: Vec<String> = modifiers
            .iter()
            .map(|m| format!("0x{:x}", m.drm_format_modifier))
            .collect();
        return Err(req.format_error(format!(
            "modifier not advertised for {:?}; device supports [{}]",
            req.vk_format,
            known.join(", ")
        )));
    };
    if entry.drm_format_modifier_plane_count != 1 {
        return Err(req.format_error(format!(
            "modifier needs {} memory planes; only single-plane imports are implemented",
            entry.drm_format_modifier_plane_count
        )));
    }
    if !entry
        .drm_format_modifier_tiling_features
        .contains(req.format_features)
    {
        return Err(req.format_error(format!(
            "modifier supports {:?}, import needs {:?}",
            entry.drm_format_modifier_tiling_features, req.format_features
        )));
    }
    Ok(())
}

/// Check that an image with this format/modifier/usage can be imported from a dmabuf.
///
/// # Safety
///
/// `phys` must belong to `instance`.
unsafe fn check_image_format(
    instance: &ash::Instance,
    phys: vk::PhysicalDevice,
    req: &ImportRequest,
) -> Result<(), ExternalImportError> {
    let mut external_info =
        vk::PhysicalDeviceExternalImageFormatInfo::default().handle_type(DMA_BUF);
    let mut modifier_info = vk::PhysicalDeviceImageDrmFormatModifierInfoEXT::default()
        .drm_format_modifier(req.modifier)
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    let info = vk::PhysicalDeviceImageFormatInfo2::default()
        .format(req.vk_format)
        .ty(vk::ImageType::TYPE_2D)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(req.vk_usage)
        .push_next(&mut external_info)
        .push_next(&mut modifier_info);
    let mut external_props = vk::ExternalImageFormatProperties::default();
    let mut props = vk::ImageFormatProperties2::default().push_next(&mut external_props);
    // SAFETY: valid physical device and structure chains.
    unsafe { instance.get_physical_device_image_format_properties2(phys, &info, &mut props) }
        .map_err(|err| {
            req.format_error(format!(
                "vkGetPhysicalDeviceImageFormatProperties2 rejected the import: {err}"
            ))
        })?;
    let max_extent = props.image_format_properties.max_extent;
    let memory_features = external_props
        .external_memory_properties
        .external_memory_features;
    if req.width > max_extent.width || req.height > max_extent.height {
        return Err(req.format_error(format!(
            "extent {}x{} exceeds {}x{} for this modifier",
            req.width, req.height, max_extent.width, max_extent.height
        )));
    }
    if !memory_features.contains(vk::ExternalMemoryFeatureFlags::IMPORTABLE) {
        return Err(req.format_error("dmabuf memory is not importable for this image"));
    }
    Ok(())
}
