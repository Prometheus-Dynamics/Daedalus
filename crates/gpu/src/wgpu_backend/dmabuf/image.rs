//! Creating a `VkImage` over dmabuf memory and wrapping it as a wgpu-hal texture.

use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};

use ash::{khr, vk};
use wgpu::hal::vulkan as hal_vk;

use crate::{DrmFourcc, ExternalImportError, ExternalKeepalive};

const DMA_BUF: vk::ExternalMemoryHandleTypeFlags = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
const MEMORY_PLANES: [vk::ImageAspectFlags; crate::MAX_MEMORY_PLANES] = [
    vk::ImageAspectFlags::MEMORY_PLANE_0_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_1_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_2_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_3_EXT,
];

/// Everything the Vulkan side needs, resolved from the descriptor.
pub(super) struct ImportRequest {
    pub width: u32,
    pub height: u32,
    pub fourcc: DrmFourcc,
    pub modifier: u64,
    /// One layout per memory plane of the modifier (format planes, then aux planes).
    pub plane_layouts: Vec<vk::SubresourceLayout>,
    /// Bind each memory plane to its own dmabuf (`VK_IMAGE_CREATE_DISJOINT_BIT`).
    pub disjoint: bool,
    pub wgpu_format: wgpu::TextureFormat,
    pub vk_format: vk::Format,
    pub vk_usage: vk::ImageUsageFlags,
    pub format_features: vk::FormatFeatureFlags,
    pub hal_usage: wgpu::wgt::TextureUses,
    pub label: Option<String>,
}

impl ImportRequest {
    fn format_error(&self, reason: impl Into<String>) -> ExternalImportError {
        ExternalImportError::UnsupportedFormat {
            fourcc: self.fourcc,
            modifier: Some(self.modifier),
            reason: reason.into(),
        }
    }

    fn create_flags(&self) -> vk::ImageCreateFlags {
        let mut flags = vk::ImageCreateFlags::empty();
        if self.wgpu_format.is_multi_planar_format() {
            // wgpu views each plane with a plane-compatible format (R8/RG8), like its own NV12
            // textures.
            flags |= vk::ImageCreateFlags::MUTABLE_FORMAT | vk::ImageCreateFlags::EXTENDED_USAGE;
        }
        if self.disjoint {
            flags |= vk::ImageCreateFlags::DISJOINT;
        }
        flags
    }
}

/// An image with its imported memory; destroyed (and the dmabuf references dropped) on drop.
struct ImportedImage {
    device: ash::Device,
    image: vk::Image,
    memories: Vec<vk::DeviceMemory>,
}

impl Drop for ImportedImage {
    fn drop(&mut self) {
        // SAFETY: the handles were created on `device` and are referenced by nothing else once
        // this owner goes away (wgpu-hal runs the texture drop callback after the GPU is done).
        unsafe {
            self.device.destroy_image(self.image, None);
            for memory in self.memories.drain(..) {
                self.device.free_memory(memory, None);
            }
        }
    }
}

/// Create a `VkImage` over the dmabuf(s) and wrap it as a wgpu-hal texture.
///
/// `fds` holds one dmabuf (with its size) for all planes, or one per plane when `req.disjoint`.
///
/// # Safety
///
/// `hal_dev` must be the hal device behind the wgpu device the texture will be registered with.
/// On success, ownership of `fds` has moved into the Vulkan allocations and `keepalive` into the
/// texture's drop callback.
pub(super) unsafe fn create_imported_texture(
    hal_dev: &hal_vk::Device,
    req: &ImportRequest,
    fds: Vec<(OwnedFd, u64)>,
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

    let mut explicit = vk::ImageDrmFormatModifierExplicitCreateInfoEXT::default()
        .drm_format_modifier(req.modifier)
        .plane_layouts(&req.plane_layouts);
    let mut external = vk::ExternalMemoryImageCreateInfo::default().handle_types(DMA_BUF);
    let image_info = vk::ImageCreateInfo::default()
        .flags(req.create_flags())
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
    // SAFETY: valid create info; the owner below destroys the image on every path.
    let image = unsafe { device.create_image(&image_info, None) }.map_err(|err| {
        req.format_error(format!("vkCreateImage rejected the plane layout: {err}"))
    })?;
    let mut owned = ImportedImage {
        device: device.clone(),
        image,
        memories: Vec::with_capacity(fds.len()),
    };

    let fd_api = khr::external_memory_fd::Device::new(instance, device);
    for (plane, (fd, len)) in fds.into_iter().enumerate() {
        let aspect = req.disjoint.then(|| MEMORY_PLANES[plane]);
        // SAFETY: `image` is valid; the memory is recorded in `owned` right away.
        let memory = unsafe { import_memory(device, &fd_api, image, aspect, fd, len)? };
        owned.memories.push(memory);
    }
    let bind = |plane: usize| {
        vk::BindImageMemoryInfo::default()
            .image(image)
            .memory(owned.memories[plane])
    };
    let mut plane_infos: Vec<_> = MEMORY_PLANES[..owned.memories.len()]
        .iter()
        .map(|&aspect| vk::BindImagePlaneMemoryInfo::default().plane_aspect(aspect))
        .collect();
    let binds: Vec<_> = if req.disjoint {
        plane_infos
            .iter_mut()
            .enumerate()
            .map(|(plane, info)| bind(plane).push_next(info))
            .collect()
    } else {
        vec![bind(0)]
    };
    // SAFETY: each memory was allocated for this image (plane); offset 0, the plane offsets are
    // part of the explicit layout.
    unsafe { device.bind_image_memory2(&binds) }.map_err(|err| {
        ExternalImportError::ImportFailed {
            reason: format!("vkBindImageMemory2 failed: {err}"),
        }
    })?;

    let drop_callback: wgpu::hal::DropCallback = Box::new(move || {
        // wgpu-hal calls this exactly once, from `destroy_texture`, after wgpu-core has retired
        // every submission using the texture.
        drop(owned);
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
    // destroy the image, and `TextureMemory::External` stops it from freeing the memory; both are
    // released by the callback. Multi-planar images carry `MUTABLE_FORMAT`, so empty
    // `view_formats` are allowed.
    Ok(unsafe {
        hal_dev.texture_from_raw(
            image,
            &hal_desc,
            Some(drop_callback),
            hal_vk::TextureMemory::External,
        )
    })
}

/// Import one dmabuf as memory for `image` (or for one memory plane of a disjoint image).
///
/// # Safety
///
/// `image` must be a valid image of `device` that was created for dmabuf import.
unsafe fn import_memory(
    device: &ash::Device,
    fd_api: &khr::external_memory_fd::Device,
    image: vk::Image,
    plane: Option<vk::ImageAspectFlags>,
    fd: OwnedFd,
    dmabuf_len: u64,
) -> Result<vk::DeviceMemory, ExternalImportError> {
    let mut fd_props = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: `fd` is a valid open descriptor for the duration of the call.
    unsafe { fd_api.get_memory_fd_properties(DMA_BUF, fd.as_raw_fd(), &mut fd_props) }.map_err(
        |err| ExternalImportError::ImportFailed {
            reason: format!("fd is not an importable dmabuf (vkGetMemoryFdPropertiesKHR: {err})"),
        },
    )?;

    let mut plane_info =
        plane.map(|aspect| vk::ImagePlaneMemoryRequirementsInfo::default().plane_aspect(aspect));
    let mut reqs_info = vk::ImageMemoryRequirementsInfo2::default().image(image);
    if let Some(info) = plane_info.as_mut() {
        reqs_info = reqs_info.push_next(info);
    }
    let mut reqs = vk::MemoryRequirements2::default();
    // SAFETY: valid image and structure chains.
    unsafe { device.get_image_memory_requirements2(&reqs_info, &mut reqs) };
    let reqs = reqs.memory_requirements;
    let type_bits = reqs.memory_type_bits & fd_props.memory_type_bits;
    if type_bits == 0 {
        return Err(ExternalImportError::ImportFailed {
            reason: format!(
                "no memory type compatible with both the image (0x{:x}) and the dmabuf (0x{:x})",
                reqs.memory_type_bits, fd_props.memory_type_bits
            ),
        });
    }
    if reqs.size > dmabuf_len {
        return Err(ExternalImportError::invalid(format!(
            "driver needs {} bytes for this image but the dmabuf has {dmabuf_len}",
            reqs.size
        )));
    }

    let raw_fd = fd.into_raw_fd();
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(DMA_BUF)
        .fd(raw_fd);
    // Dedicated allocations are not allowed for disjoint images.
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let mut alloc_info = vk::MemoryAllocateInfo::default()
        .allocation_size(reqs.size)
        .memory_type_index(type_bits.trailing_zeros())
        .push_next(&mut import);
    if plane.is_none() {
        alloc_info = alloc_info.push_next(&mut dedicated);
    }
    // SAFETY: on success Vulkan owns `raw_fd`; on failure it does not, so we close it.
    unsafe { device.allocate_memory(&alloc_info, None) }.map_err(|err| {
        // SAFETY: Vulkan did not take ownership of `raw_fd` on failure.
        drop(unsafe { OwnedFd::from_raw_fd(raw_fd) });
        ExternalImportError::ImportFailed {
            reason: format!("vkAllocateMemory (dmabuf import) failed: {err}"),
        }
    })
}

/// Check that the device advertises the modifier for this format with the needed features and
/// memory plane count.
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
    let planes = entry.drm_format_modifier_plane_count as usize;
    if planes != req.plane_layouts.len() {
        return Err(req.format_error(format!(
            "modifier needs {planes} memory planes but the descriptor provides {}",
            req.plane_layouts.len()
        )));
    }
    let mut needed = req.format_features;
    if req.disjoint {
        needed |= vk::FormatFeatureFlags::DISJOINT;
    }
    if !entry.drm_format_modifier_tiling_features.contains(needed) {
        return Err(req.format_error(format!(
            "modifier supports {:?}, import needs {needed:?}",
            entry.drm_format_modifier_tiling_features
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
        .flags(req.create_flags())
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
