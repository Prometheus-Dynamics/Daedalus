//! An image whose memory is a dma-buf someone else filled, imported with no copy.
//!
//! The image is created with the producer's exact layout, each plane's offset and pitch as
//! given, under the DRM format modifier that names how the memory is arranged. Its memory is
//! the dma-buf itself. wgpu is handed the image with a callback that destroys it, frees the
//! imported memory and only then lets go of whatever kept the producer's buffer alive, and wgpu
//! runs that once nothing in flight still uses the texture.
//!
//! An import submits one command of its own: the image arrives owned by nobody Vulkan knows,
//! in the layout producers outside Vulkan leave it in, and is acquired for this device's queue
//! as a read-only image. wgpu is told that is the state it is in, so it never clears it. When
//! the producer gave a fence, that submit waits on it.

use std::any::Any;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};

use ash::{ext, khr, vk};
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use crate::device::Raw;
use crate::semaphore::Semaphore;
use crate::submit::Submitter;

const DMA_BUF: vk::ExternalMemoryHandleTypeFlags = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
/// The most memory planes a modifier can have.
pub const MAX_PLANES: usize = 4;
const MEMORY_PLANES: [vk::ImageAspectFlags; MAX_PLANES] = [
    vk::ImageAspectFlags::MEMORY_PLANE_0_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_1_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_2_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_3_EXT,
];

/// Where one memory plane starts in its dma-buf, and the bytes between its rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImagePlane {
    pub offset: u64,
    pub stride: u64,
}

/// One dma-buf of an image, and how long it is.
#[derive(Debug)]
pub struct ImageMemory {
    pub fd: OwnedFd,
    pub len: u64,
}

/// What to import.
pub struct ImageImport<'a> {
    pub width: u32,
    pub height: u32,
    /// One of R8Unorm, Rg8Unorm, Rgba8Unorm, Bgra8Unorm and NV12.
    pub format: wgpu::TextureFormat,
    pub modifier: u64,
    /// Every memory plane of the modifier, in order.
    pub planes: &'a [ImagePlane],
    /// The dma-bufs: one when every plane is in the same one, or one for each plane.
    pub memory: Vec<ImageMemory>,
    pub usage: wgpu::TextureUsages,
    /// The producer's fence, which the acquire waits on.
    pub fence: Option<Semaphore>,
    /// Kept until the image is destroyed and nothing in flight uses it.
    pub keepalive: Option<Box<dyn Any + Send + Sync>>,
}

/// Why an image wasn't imported.
#[derive(Debug)]
pub enum ImageError {
    /// The device isn't on Vulkan, or was opened without dma-buf image import.
    NoImport(&'static str),
    /// The format, the modifier or the plane layout isn't one this device takes.
    Format(String),
    /// A dma-buf is shorter than the driver needs for the image.
    Short {
        needed: u64,
        len: u64,
    },
    /// The driver refused the memory. `fd` is the one it refused, which may or may not still be
    /// open: `driver_closed` says the driver closed it, against what Vulkan promises, and then
    /// it must not be closed again.
    Refused {
        reason: String,
        fd: OwnedFd,
        driver_closed: bool,
    },
    Vulkan(vk::Result),
}

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageError::NoImport(why) => f.write_str(why),
            ImageError::Format(why) => f.write_str(why),
            ImageError::Short { needed, len } => {
                write!(
                    f,
                    "the driver needs {needed} bytes for this image, and the dma-buf has {len}"
                )
            }
            ImageError::Refused { reason, .. } => f.write_str(reason),
            ImageError::Vulkan(result) => write!(f, "Vulkan: {result:?}"),
        }
    }
}

impl From<vk::Result> for ImageError {
    fn from(result: vk::Result) -> ImageError {
        ImageError::Vulkan(result)
    }
}

fn vk_format(format: wgpu::TextureFormat) -> Option<vk::Format> {
    Some(match format {
        wgpu::TextureFormat::R8Unorm => vk::Format::R8_UNORM,
        wgpu::TextureFormat::Rg8Unorm => vk::Format::R8G8_UNORM,
        wgpu::TextureFormat::Rgba8Unorm => vk::Format::R8G8B8A8_UNORM,
        wgpu::TextureFormat::Bgra8Unorm => vk::Format::B8G8R8A8_UNORM,
        wgpu::TextureFormat::NV12 => vk::Format::G8_B8R8_2PLANE_420_UNORM,
        _ => return None,
    })
}

/// The usages an import can have, as wgpu, wgpu-hal and Vulkan each name them.
fn usages(
    wgpu: wgpu::TextureUsages,
) -> (
    wgpu::wgt::TextureUses,
    vk::ImageUsageFlags,
    vk::FormatFeatureFlags,
) {
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
    ];
    let mut out = (H::empty(), V::empty(), F::empty());
    for (want, hal, vulkan, features) in layers {
        if wgpu.contains(want) {
            out.0 |= hal;
            out.1 |= vulkan;
            out.2 |= features;
        }
    }
    out
}

/// The image and its imported memory, destroyed together when wgpu is done with the texture.
struct Imported {
    device: ash::Device,
    image: vk::Image,
    memories: Vec<vk::DeviceMemory>,
    /// Waited on by the acquire, so it has to outlive it.
    _fence: Option<Semaphore>,
    _keepalive: Option<Box<dyn Any + Send + Sync>>,
}

// SAFETY: the handles are plain numbers of a device that is used from any thread, and the
// keepalive is `Send + Sync` by its type. Nothing here is touched again until `Drop`.
unsafe impl Send for Imported {}
// SAFETY: as above. There is no shared access to speak of: the only use is `Drop`.
unsafe impl Sync for Imported {}

impl Drop for Imported {
    fn drop(&mut self) {
        // SAFETY: wgpu runs the texture's drop callback, which owns this, once nothing in flight
        // uses the image. The image and memories were made on `device` and belong to nothing
        // else. Freeing imported memory drops Vulkan's hold on the dma-buf.
        unsafe {
            self.device.destroy_image(self.image, None);
            for memory in self.memories.drain(..) {
                self.device.free_memory(memory, None);
            }
        }
    }
}

/// The modifiers the device lists for a format.
fn modifiers(raw: &Raw, format: vk::Format) -> Vec<vk::DrmFormatModifierPropertiesEXT> {
    let mut list = vk::DrmFormatModifierPropertiesListEXT::default();
    let mut properties = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: a query on the device's own physical device. This call only counts.
    unsafe {
        raw.instance
            .get_physical_device_format_properties2(raw.physical, format, &mut properties);
    }
    let count = list.drm_format_modifier_count as usize;
    let mut found = vec![vk::DrmFormatModifierPropertiesEXT::default(); count];
    let mut list = vk::DrmFormatModifierPropertiesListEXT::default()
        .drm_format_modifier_properties(&mut found);
    let mut properties = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: as above, now with room for `count` entries.
    unsafe {
        raw.instance
            .get_physical_device_format_properties2(raw.physical, format, &mut properties);
    }
    let filled = list.drm_format_modifier_count as usize;
    found.truncate(filled);
    found
}

/// Whether this device's driver closes the fd of an import it refuses, which Vulkan says it
/// doesn't. NVIDIA's does.
pub fn driver_closes_refused_fds(device: &wgpu::Device) -> bool {
    let Some(raw) = Raw::of(device) else {
        return false;
    };
    let mut driver = vk::PhysicalDeviceDriverProperties::default();
    let mut properties = vk::PhysicalDeviceProperties2::default().push_next(&mut driver);
    // SAFETY: a query on the device's own physical device.
    unsafe {
        raw.instance
            .get_physical_device_properties2(raw.physical, &mut properties);
    }
    driver.driver_id == vk::DriverId::NVIDIA_PROPRIETARY
}

/// Imports the image `request` describes as a wgpu texture on `device`, and submits its
/// acquire through `submitter`, which is the device's.
pub fn import_image(
    device: &wgpu::Device,
    submitter: &Submitter,
    request: ImageImport<'_>,
) -> Result<wgpu::Texture, ImageError> {
    let raw = Raw::of(device).ok_or(ImageError::NoImport("the device isn't on Vulkan"))?;
    let wanted = [
        khr::external_memory_fd::NAME,
        ext::external_memory_dma_buf::NAME,
        ext::image_drm_format_modifier::NAME,
    ];
    if !wanted.iter().all(|name| raw.has(name)) {
        return Err(ImageError::NoImport(
            "the device can't import a dma-buf as an image",
        ));
    }
    let format = vk_format(request.format).ok_or_else(|| {
        ImageError::Format(format!("{:?} isn't an importable format", request.format))
    })?;
    let planar = request.format == wgpu::TextureFormat::NV12;
    let (hal_usage, vk_usage, features) = usages(request.usage);
    let count = request.planes.len();
    let disjoint = request.memory.len() > 1;
    if count == 0 || count > MAX_PLANES || (disjoint && request.memory.len() != count) {
        return Err(ImageError::Format(
            "an image has one to four planes, in one dma-buf or one each".into(),
        ));
    }

    // The device has to list the modifier for the format, with as many planes as were given.
    let listed = modifiers(&raw, format);
    let Some(entry) = listed
        .iter()
        .find(|entry| entry.drm_format_modifier == request.modifier)
    else {
        let known: Vec<String> = listed
            .iter()
            .map(|entry| format!("0x{:x}", entry.drm_format_modifier))
            .collect();
        return Err(ImageError::Format(format!(
            "the device doesn't list modifier 0x{:x} for {format:?}: it lists [{}]",
            request.modifier,
            known.join(", ")
        )));
    };
    if entry.drm_format_modifier_plane_count as usize != count {
        return Err(ImageError::Format(format!(
            "the modifier has {} memory planes, and {count} were given",
            entry.drm_format_modifier_plane_count
        )));
    }
    let mut needed = features;
    if disjoint {
        needed |= vk::FormatFeatureFlags::DISJOINT;
    }
    if !entry.drm_format_modifier_tiling_features.contains(needed) {
        return Err(ImageError::Format(format!(
            "the modifier allows {:?}, and the import needs {needed:?}",
            entry.drm_format_modifier_tiling_features
        )));
    }

    let layouts: Vec<vk::SubresourceLayout> = request
        .planes
        .iter()
        .map(|plane| vk::SubresourceLayout {
            offset: plane.offset,
            size: 0,
            row_pitch: plane.stride,
            array_pitch: 0,
            depth_pitch: 0,
        })
        .collect();
    // wgpu views each plane of a planar image with that plane's own format, so they are listed.
    let views = [format, vk::Format::R8_UNORM, vk::Format::R8G8_UNORM];
    let mut flags = vk::ImageCreateFlags::empty();
    if planar {
        flags |= vk::ImageCreateFlags::MUTABLE_FORMAT | vk::ImageCreateFlags::EXTENDED_USAGE;
    }
    if disjoint {
        flags |= vk::ImageCreateFlags::DISJOINT;
    }
    let mut explicit = vk::ImageDrmFormatModifierExplicitCreateInfoEXT::default()
        .drm_format_modifier(request.modifier)
        .plane_layouts(&layouts);
    let mut external = vk::ExternalMemoryImageCreateInfo::default().handle_types(DMA_BUF);
    let mut format_list = vk::ImageFormatListCreateInfo::default().view_formats(&views);
    let mut info = vk::ImageCreateInfo::default()
        .flags(flags)
        .image_type(vk::ImageType::TYPE_2D)
        .format(format)
        .extent(vk::Extent3D {
            width: request.width,
            height: request.height,
            depth: 1,
        })
        .mip_levels(1)
        .array_layers(1)
        .samples(vk::SampleCountFlags::TYPE_1)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(vk_usage)
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .initial_layout(vk::ImageLayout::UNDEFINED)
        .push_next(&mut external)
        .push_next(&mut explicit);
    if planar {
        info = info.push_next(&mut format_list);
    }
    let vk_device = &raw.device;
    // SAFETY: a valid create info on a live device. `owned` destroys the image on every path.
    let image = unsafe { vk_device.create_image(&info, None) }.map_err(|error| {
        ImageError::Format(format!("the driver refused the plane layout: {error}"))
    })?;
    let mut owned = Imported {
        device: vk_device.clone(),
        image,
        memories: Vec::with_capacity(request.memory.len()),
        _fence: None,
        _keepalive: None,
    };

    let table = khr::external_memory_fd::Device::new(&raw.instance, vk_device);
    for (plane, memory) in request.memory.into_iter().enumerate() {
        let aspect = disjoint.then(|| MEMORY_PLANES[plane]);
        let imported = import_memory(&raw, &table, image, aspect, memory)?;
        owned.memories.push(imported);
    }
    let mut plane_infos: Vec<vk::BindImagePlaneMemoryInfo<'_>> = MEMORY_PLANES
        .iter()
        .take(owned.memories.len())
        .map(|&aspect| vk::BindImagePlaneMemoryInfo::default().plane_aspect(aspect))
        .collect();
    let mut binds = Vec::with_capacity(owned.memories.len());
    for (memory, plane) in owned.memories.iter().zip(plane_infos.iter_mut()) {
        let bind = vk::BindImageMemoryInfo::default()
            .image(image)
            .memory(*memory);
        binds.push(if disjoint {
            bind.push_next(plane)
        } else {
            bind
        });
    }
    // SAFETY: each memory was imported for this image, or this plane of it, and is bound once.
    // The planes' offsets are part of the image's layout, so every bind is at offset 0.
    unsafe { vk_device.bind_image_memory2(&binds) }?;
    owned._fence = request.fence;
    owned._keepalive = request.keepalive;
    let fence = owned._fence.as_ref().map(|fence| fence.raw);

    // SAFETY: reads the hal device, which the wgpu device keeps alive.
    let hal_device = unsafe { device.as_hal::<Vulkan>() }
        .ok_or(ImageError::NoImport("the device isn't on Vulkan"))?;
    let size = wgpu::Extent3d {
        width: request.width,
        height: request.height,
        depth_or_array_layers: 1,
    };
    let hal_desc = wgpu::hal::TextureDescriptor {
        label: None,
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: request.format,
        usage: hal_usage,
        memory_flags: wgpu::hal::MemoryFlags::empty(),
        view_formats: Vec::new(),
    };
    let on_drop: wgpu::hal::DropCallback = Box::new(move || drop(owned));
    // SAFETY: the image matches `hal_desc`. With a drop callback wgpu-hal neither destroys the
    // image nor frees its memory, which is marked external: the callback does both. A planar
    // image has `MUTABLE_FORMAT`, so having no view formats here is allowed.
    let hal_texture = unsafe {
        hal_device.texture_from_raw(
            image,
            &hal_desc,
            Some(on_drop),
            hal_vk::TextureMemory::External,
        )
    };
    drop(hal_device);
    let desc = wgpu::TextureDescriptor {
        label: None,
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: request.format,
        usage: request.usage,
        view_formats: &[],
    };
    // SAFETY: the hal texture was made on this device to exactly `desc`, and its memory holds
    // the producer's image. It is in the read-only state as of the acquire submitted below,
    // before anything else can use the texture, so wgpu never treats it as uninitialized.
    let texture = unsafe {
        device.create_texture_from_hal::<Vulkan>(
            hal_texture,
            &desc,
            wgpu::wgt::TextureUses::RESOURCE,
        )
    };

    let foreign = if raw.has(ext::queue_family_foreign::NAME) {
        vk::QUEUE_FAMILY_FOREIGN_EXT
    } else {
        vk::QUEUE_FAMILY_EXTERNAL
    };
    let aspect = if disjoint {
        vk::ImageAspectFlags::PLANE_0 | vk::ImageAspectFlags::PLANE_1
    } else {
        vk::ImageAspectFlags::COLOR
    };
    // A producer outside Vulkan has no layout. The convention is that the image is in the
    // general layout in a foreign queue family, which keeps what the memory holds.
    let barrier = vk::ImageMemoryBarrier::default()
        .src_access_mask(vk::AccessFlags::empty())
        .dst_access_mask(vk::AccessFlags::SHADER_READ)
        .old_layout(vk::ImageLayout::GENERAL)
        .new_layout(vk::ImageLayout::SHADER_READ_ONLY_OPTIMAL)
        .src_queue_family_index(foreign)
        .dst_queue_family_index(raw.family)
        .image(image)
        .subresource_range(vk::ImageSubresourceRange {
            aspect_mask: aspect,
            base_mip_level: 0,
            level_count: 1,
            base_array_layer: 0,
            layer_count: 1,
        });
    let record = |device: &ash::Device, commands: vk::CommandBuffer| {
        // SAFETY: `commands` is recording on the image's device, and the image lives until the
        // texture's drop callback, which wgpu holds back until this submit has finished.
        unsafe {
            device.cmd_pipeline_barrier(
                commands,
                vk::PipelineStageFlags::TOP_OF_PIPE,
                vk::PipelineStageFlags::ALL_COMMANDS,
                vk::DependencyFlags::empty(),
                &[],
                &[],
                &[barrier],
            );
        }
    };
    // SAFETY: the recording is one barrier on an image that outlives it, and the fence's
    // semaphore is owned by the texture's drop callback, so it outlives the submit too.
    unsafe { submitter.submit_raw(record, fence) }.map_err(|error| {
        ImageError::Format(format!("the acquire couldn't be submitted: {error}"))
    })?;
    Ok(texture)
}

/// Imports one dma-buf as the memory of `image`, or of one plane of it.
fn import_memory(
    raw: &Raw,
    table: &khr::external_memory_fd::Device,
    image: vk::Image,
    plane: Option<vk::ImageAspectFlags>,
    memory: ImageMemory,
) -> Result<vk::DeviceMemory, ImageError> {
    let device = &raw.device;
    let mut types = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: the extension is enabled, and the fd is open for the length of the call.
    unsafe { table.get_memory_fd_properties(DMA_BUF, memory.fd.as_raw_fd(), &mut types) }.map_err(
        |error| {
            ImageError::Format(format!(
                "the fd isn't a dma-buf this device can import: {error}"
            ))
        },
    )?;
    let mut plane_info =
        plane.map(|aspect| vk::ImagePlaneMemoryRequirementsInfo::default().plane_aspect(aspect));
    let mut asked = vk::ImageMemoryRequirementsInfo2::default().image(image);
    if let Some(info) = plane_info.as_mut() {
        asked = asked.push_next(info);
    }
    let mut needs = vk::MemoryRequirements2::default();
    // SAFETY: a query on an image of this device.
    unsafe { device.get_image_memory_requirements2(&asked, &mut needs) };
    let needs = needs.memory_requirements;
    let both = needs.memory_type_bits & types.memory_type_bits;
    if both == 0 {
        return Err(ImageError::Format(format!(
            "no memory type fits both the image (0x{:x}) and the dma-buf (0x{:x})",
            needs.memory_type_bits, types.memory_type_bits
        )));
    }
    if needs.size > memory.len {
        return Err(ImageError::Short {
            needed: needs.size,
            len: memory.len,
        });
    }
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(DMA_BUF)
        .fd(memory.fd.as_raw_fd());
    // A plane of a disjoint image can't have a dedicated allocation.
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let mut allocate = vk::MemoryAllocateInfo::default()
        .allocation_size(needs.size)
        .memory_type_index(both.trailing_zeros())
        .push_next(&mut import);
    if plane.is_none() {
        allocate = allocate.push_next(&mut dedicated);
    }
    // SAFETY: an import of an fd this call owns. On success Vulkan owns the fd.
    match unsafe { device.allocate_memory(&allocate, None) } {
        Ok(imported) => {
            let _ = memory.fd.into_raw_fd();
            Ok(imported)
        }
        Err(error) => {
            let mut driver = vk::PhysicalDeviceDriverProperties::default();
            let mut properties = vk::PhysicalDeviceProperties2::default().push_next(&mut driver);
            // SAFETY: a query on the device's own physical device.
            unsafe {
                raw.instance
                    .get_physical_device_properties2(raw.physical, &mut properties);
            }
            Err(ImageError::Refused {
                reason: format!("the driver refused the dma-buf's memory: {error}"),
                fd: memory.fd,
                driver_closed: driver.driver_id == vk::DriverId::NVIDIA_PROPRIETARY,
            })
        }
    }
}
