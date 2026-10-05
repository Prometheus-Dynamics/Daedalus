//! Every modifier the device can render to, export and import, round-tripped between devices: a
//! raw Vulkan producer allocates a `DRM_FORMAT_MODIFIER` image and exports it as a dmabuf with its
//! memory plane layouts, imports it through Daedalus as a render target, renders a pattern into
//! the left half and fast-clears the right half (so compressed modifiers carry real compression
//! metadata), and drops it (release to the foreign family). A second device then imports it from
//! the dmabuf and reads it back.

use std::os::fd::{AsFd, FromRawFd, OwnedFd};

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;

use super::support::{
    consumer_backend, device_poll, exclusive, import_backend, note, skip, validated,
};
use crate::{
    DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalImportError, ExternalPlane,
    GpuBackend, GpuUsage, MAX_MEMORY_PLANES, WgpuBackend,
};

const DMA_BUF: vk::ExternalMemoryHandleTypeFlags = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
const MEMORY_PLANES: [vk::ImageAspectFlags; MAX_MEMORY_PLANES] = [
    vk::ImageAspectFlags::MEMORY_PLANE_0_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_1_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_2_EXT,
    vk::ImageAspectFlags::MEMORY_PLANE_3_EXT,
];
const WIDTH: u32 = 200;
const HEIGHT: u32 = 120;

/// A renderable format under test.
struct Case {
    fourcc: DrmFourcc,
    format: vk::Format,
    /// WGSL expression of the fragment color from `x` and `y` (`u32`).
    color: &'static str,
    /// Bytes of a pattern texel in memory order.
    texel: fn(u32, u32) -> Vec<u8>,
    clear: wgpu::Color,
    clear_texel: &'static [u8],
}

const CASES: [Case; 2] = [
    Case {
        fourcc: DrmFourcc::XRGB8888,
        format: vk::Format::B8G8R8A8_UNORM,
        color: "vec4<f32>(f32(x ^ y), f32(y), f32(x), 255.0) / 255.0",
        texel: |x, y| vec![x as u8, y as u8, (x ^ y) as u8, 0xff],
        clear: wgpu::Color {
            r: 0x60 as f64 / 255.0,
            g: 0x40 as f64 / 255.0,
            b: 0x20 as f64 / 255.0,
            a: 1.0,
        },
        clear_texel: &[0x20, 0x40, 0x60, 0xff],
    },
    Case {
        fourcc: DrmFourcc::R8,
        format: vk::Format::R8_UNORM,
        color: "vec4<f32>(f32((x * 3u + y * 5u) & 255u) / 255.0, 0.0, 0.0, 1.0)",
        texel: |x, y| vec![(x * 3 + y * 5) as u8],
        clear: wgpu::Color {
            r: 51.0 / 255.0,
            g: 0.0,
            b: 0.0,
            a: 1.0,
        },
        clear_texel: &[51],
    },
];

#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import"]
fn dmabuf_import_tiled_and_compressed_modifiers() {
    let _gpu = exclusive();
    let Some(producer) = import_backend() else {
        return;
    };
    let (lavapipe, independent) = consumer_backend();
    let consumers = [
        (
            "second device",
            WgpuBackend::new().expect("consumer backend"),
        ),
        ("lavapipe", lavapipe),
    ];
    let consumers = if independent {
        &consumers[..]
    } else {
        &consumers[..1]
    };
    let mut tested = Vec::new();
    for case in &CASES {
        for (modifier, planes) in renderable_modifiers(&producer, case.format) {
            let usage = vk::ImageUsageFlags::SAMPLED
                | vk::ImageUsageFlags::TRANSFER_SRC
                | vk::ImageUsageFlags::COLOR_ATTACHMENT;
            let exported = match export_image(&producer, case.format, modifier, planes, usage) {
                Ok(exported) => exported,
                Err(reason) => {
                    skip(format!(
                        "{:?} 0x{modifier:x}: export: {reason}",
                        case.fourcc
                    ));
                    continue;
                }
            };
            let desc = |exported: &Exported| {
                let planes = exported
                    .planes
                    .iter()
                    .map(|&(offset, pitch)| {
                        ExternalPlane::from_borrowed(exported.fd.as_fd(), offset, pitch).unwrap()
                    })
                    .collect();
                ExternalFrameDescriptor::new(WIDTH, HEIGHT, case.fourcc, planes)
                    .with_modifier(modifier)
            };
            let target = validated(&producer, || {
                producer.import_dmabuf(desc(&exported).with_usage(GpuUsage::RENDER_TARGET))
            })
            .unwrap_or_else(|err| panic!("producer import of 0x{modifier:x}: {err}"));
            validated(&producer, || render(&producer, &target, case));
            drop(target); // releases the image to the foreign family
            device_poll(&producer);

            for (name, consumer) in consumers {
                let handle = match consumer.import_dmabuf(desc(&exported)) {
                    Ok(handle) => handle,
                    // lavapipe only imports memory it can map (not the producer's VRAM).
                    Err(err @ ExternalImportError::ImportFailed { .. }) if *name == "lavapipe" => {
                        skip(format!("{name}: {:?} 0x{modifier:x}: {err}", case.fourcc));
                        continue;
                    }
                    Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
                        skip(format!(
                            "{name}: {:?} 0x{modifier:x}: {reason}",
                            case.fourcc
                        ));
                        continue;
                    }
                    Err(err) => panic!("{name}: import of 0x{modifier:x} failed: {err}"),
                };
                let read = validated(consumer, || consumer.read_texture(&handle)).unwrap();
                check_pattern(&read, case, &format!("{name}, modifier 0x{modifier:x}"));
                tested.push(format!(
                    "{:?} 0x{modifier:x} ({planes} memory plane(s)) on {name}",
                    case.fourcc
                ));
            }
        }
    }
    note(format!(
        "modifiers round-tripped:\n  {}",
        tested.join("\n  ")
    ));
    assert!(!tested.is_empty(), "no modifier could be tested");
}

/// Tiled NV12 modifiers have no renderable or copyable layout on most drivers, so only the
/// two-memory-plane import itself and sampling it through the plane views are checked (contents
/// are undefined).
#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import"]
fn dmabuf_import_tiled_nv12_layouts() {
    let _gpu = exclusive();
    let Some(producer) = import_backend() else {
        return;
    };
    if !producer
        .device_queue()
        .0
        .features()
        .contains(wgpu::Features::TEXTURE_FORMAT_NV12)
    {
        return skip("no TEXTURE_FORMAT_NV12".into());
    }
    let consumer = WgpuBackend::new().expect("consumer backend");
    let format = vk::Format::G8_B8R8_2PLANE_420_UNORM;
    let mut tested = Vec::new();
    for (modifier, planes) in
        format_modifiers(&producer, format, vk::FormatFeatureFlags::SAMPLED_IMAGE)
    {
        let usage = vk::ImageUsageFlags::SAMPLED;
        let exported = match export_image(&producer, format, modifier, planes, usage) {
            Ok(exported) => exported,
            Err(reason) => {
                skip(format!("NV12 0x{modifier:x}: export: {reason}"));
                continue;
            }
        };
        let planes = exported
            .planes
            .iter()
            .map(|&(offset, pitch)| {
                ExternalPlane::from_borrowed(exported.fd.as_fd(), offset, pitch).unwrap()
            })
            .collect();
        let desc = ExternalFrameDescriptor::new(WIDTH, HEIGHT, DrmFourcc::NV12, planes)
            .with_modifier(modifier);
        let handle = match validated(&consumer, || consumer.import_dmabuf(desc)) {
            Ok(handle) => handle,
            Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
                skip(format!("NV12 0x{modifier:x}: {reason}"));
                continue;
            }
            Err(err) => panic!("NV12 import of 0x{modifier:x} failed: {err}"),
        };
        let words = validated(&consumer, || super::nv12::sample_nv12(&consumer, &handle));
        assert_eq!(words.len(), (WIDTH * HEIGHT) as usize);
        tested.push(format!(
            "0x{modifier:x} ({} memory planes)",
            exported.planes.len()
        ));
    }
    note(format!(
        "NV12 modifiers imported and sampled: {}",
        tested.join(", ")
    ));
    assert!(!tested.is_empty(), "no NV12 modifier could be imported");
}

fn check_pattern(read: &[u8], case: &Case, context: &str) {
    let bpp = case.clear_texel.len();
    assert_eq!(read.len(), (WIDTH * HEIGHT) as usize * bpp);
    for (index, texel) in read.chunks_exact(bpp).enumerate() {
        let (x, y) = (index as u32 % WIDTH, index as u32 / WIDTH);
        let expected = if x < WIDTH / 2 {
            (case.texel)(x, y)
        } else {
            case.clear_texel.to_vec()
        };
        assert_eq!(texel, expected, "texel ({x},{y}), {context}");
    }
}

/// Modifiers the device can render to, sample and copy from.
fn renderable_modifiers(backend: &WgpuBackend, format: vk::Format) -> Vec<(u64, u32)> {
    let needed = vk::FormatFeatureFlags::COLOR_ATTACHMENT
        | vk::FormatFeatureFlags::SAMPLED_IMAGE
        | vk::FormatFeatureFlags::TRANSFER_SRC;
    format_modifiers(backend, format, needed)
}

/// Modifiers (with their memory plane count) of `format` that have the `needed` features,
/// `LINEAR` first.
fn format_modifiers(
    backend: &WgpuBackend,
    format: vk::Format,
    needed: vk::FormatFeatureFlags,
) -> Vec<(u64, u32)> {
    // SAFETY: queries only.
    let hal = unsafe { backend.device_queue().0.as_hal::<Vulkan>() }.expect("Vulkan device");
    let instance = hal.shared_instance().raw_instance();
    let phys = hal.raw_physical_device();
    let mut count = vk::DrmFormatModifierPropertiesListEXT::default();
    let mut props = vk::FormatProperties2::default().push_next(&mut count);
    // SAFETY: valid physical device and out-structure chain.
    unsafe { instance.get_physical_device_format_properties2(phys, format, &mut props) };
    let mut modifiers = vec![
        vk::DrmFormatModifierPropertiesEXT::default();
        count.drm_format_modifier_count as usize
    ];
    let mut list = vk::DrmFormatModifierPropertiesListEXT::default()
        .drm_format_modifier_properties(&mut modifiers);
    let mut props = vk::FormatProperties2::default().push_next(&mut list);
    // SAFETY: as above; `modifiers` has room for the advertised count.
    unsafe { instance.get_physical_device_format_properties2(phys, format, &mut props) };
    let written = list.drm_format_modifier_count as usize;
    modifiers.truncate(written);
    let mut out: Vec<(u64, u32)> = modifiers
        .iter()
        .filter(|m| m.drm_format_modifier_tiling_features.contains(needed))
        .map(|m| (m.drm_format_modifier, m.drm_format_modifier_plane_count))
        .collect();
    out.sort_by_key(|&(modifier, _)| modifier != DRM_FORMAT_MOD_LINEAR);
    out
}

/// A dmabuf exported from a `DRM_FORMAT_MODIFIER` image, with its memory plane layouts
/// (offset, row pitch).
struct Exported {
    fd: OwnedFd,
    planes: Vec<(u64, u64)>,
}

/// Allocate a `WIDTH`x`HEIGHT` image with `modifier` on `backend`'s device and export its memory.
fn export_image(
    backend: &WgpuBackend,
    format: vk::Format,
    modifier: u64,
    planes: u32,
    usage: vk::ImageUsageFlags,
) -> Result<Exported, String> {
    // SAFETY: the raw objects below are created and destroyed here; only the dmabuf fd escapes.
    let hal = unsafe { backend.device_queue().0.as_hal::<Vulkan>() }.ok_or("not Vulkan")?;
    let device = hal.raw_device();
    let instance = hal.shared_instance().raw_instance();
    let phys = hal.raw_physical_device();

    let mut external_info =
        vk::PhysicalDeviceExternalImageFormatInfo::default().handle_type(DMA_BUF);
    let mut modifier_info = vk::PhysicalDeviceImageDrmFormatModifierInfoEXT::default()
        .drm_format_modifier(modifier)
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    let info = vk::PhysicalDeviceImageFormatInfo2::default()
        .format(format)
        .ty(vk::ImageType::TYPE_2D)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(usage)
        .push_next(&mut external_info)
        .push_next(&mut modifier_info);
    let mut external_props = vk::ExternalImageFormatProperties::default();
    let mut props = vk::ImageFormatProperties2::default().push_next(&mut external_props);
    // SAFETY: valid physical device and structure chains.
    unsafe { instance.get_physical_device_image_format_properties2(phys, &info, &mut props) }
        .map_err(|err| format!("image format query: {err}"))?;
    if !external_props
        .external_memory_properties
        .external_memory_features
        .contains(vk::ExternalMemoryFeatureFlags::EXPORTABLE)
    {
        return Err("dmabuf memory not exportable".into());
    }

    let modifiers = [modifier];
    let mut list =
        vk::ImageDrmFormatModifierListCreateInfoEXT::default().drm_format_modifiers(&modifiers);
    let mut external = vk::ExternalMemoryImageCreateInfo::default().handle_types(DMA_BUF);
    let image_info = vk::ImageCreateInfo::default()
        .image_type(vk::ImageType::TYPE_2D)
        .format(format)
        .extent(vk::Extent3D {
            width: WIDTH,
            height: HEIGHT,
            depth: 1,
        })
        .mip_levels(1)
        .array_layers(1)
        .samples(vk::SampleCountFlags::TYPE_1)
        .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
        .usage(usage)
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .initial_layout(vk::ImageLayout::UNDEFINED)
        .push_next(&mut external)
        .push_next(&mut list);
    // SAFETY: valid create info; destroyed below.
    let image = unsafe { device.create_image(&image_info, None) }
        .map_err(|err| format!("vkCreateImage: {err}"))?;
    // SAFETY: valid image.
    let reqs = unsafe { device.get_image_memory_requirements(image) };
    let mut export = vk::ExportMemoryAllocateInfo::default().handle_types(DMA_BUF);
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().image(image);
    let alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(reqs.size)
        .memory_type_index(reqs.memory_type_bits.trailing_zeros())
        .push_next(&mut export)
        .push_next(&mut dedicated);
    let fd_api = khr::external_memory_fd::Device::new(instance, device);
    // SAFETY: the memory is bound to `image` and both are destroyed before returning; the dmabuf
    // keeps the buffer alive for the importers.
    let result = unsafe {
        device.allocate_memory(&alloc, None).map(|memory| {
            let fd = device.bind_image_memory(image, memory, 0).and_then(|()| {
                let info = vk::MemoryGetFdInfoKHR::default()
                    .memory(memory)
                    .handle_type(DMA_BUF);
                fd_api.get_memory_fd(&info)
            });
            let layouts = MEMORY_PLANES[..planes as usize]
                .iter()
                .map(|&aspect_mask| {
                    let layout = device.get_image_subresource_layout(
                        image,
                        vk::ImageSubresource {
                            aspect_mask,
                            mip_level: 0,
                            array_layer: 0,
                        },
                    );
                    (layout.offset, layout.row_pitch)
                })
                .collect();
            device.free_memory(memory, None);
            fd.map(|fd| Exported {
                fd: OwnedFd::from_raw_fd(fd),
                planes: layouts,
            })
        })
    };
    // SAFETY: nothing uses the image.
    unsafe { device.destroy_image(image, None) };
    result
        .and_then(|exported| exported)
        .map_err(|err| format!("allocate/export: {err}"))
}

/// Draw the pattern into the left half of `target` and clear the rest.
fn render(backend: &WgpuBackend, target: &crate::GpuImageHandle, case: &Case) {
    let (device, queue) = backend.device_queue();
    let texture = backend.get_texture(target).expect("registered texture");
    let view = texture.create_view(&Default::default());
    let source = format!(
        r#"
@vertex
fn vs(@builtin(vertex_index) i: u32) -> @builtin(position) vec4<f32> {{
    let p = vec2<f32>(f32((i << 1u) & 2u), f32(i & 2u));
    return vec4<f32>(p * 2.0 - 1.0, 0.0, 1.0);
}}
@fragment
fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<f32> {{
    let x = u32(pos.x);
    let y = u32(pos.y);
    return {};
}}
"#,
        case.color
    );
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("modifier-pattern"),
        source: wgpu::ShaderSource::Wgsl(source.into()),
    });
    let pipeline = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: Some("modifier-pattern"),
        layout: None,
        vertex: wgpu::VertexState {
            module: &shader,
            entry_point: Some("vs"),
            compilation_options: Default::default(),
            buffers: &[],
        },
        primitive: Default::default(),
        depth_stencil: None,
        multisample: Default::default(),
        fragment: Some(wgpu::FragmentState {
            module: &shader,
            entry_point: Some("fs"),
            compilation_options: Default::default(),
            targets: &[Some(texture.format().into())],
        }),
        multiview_mask: None,
        cache: None,
    });
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("modifier-pattern"),
            color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                view: &view,
                depth_slice: None,
                resolve_target: None,
                ops: wgpu::Operations {
                    load: wgpu::LoadOp::Clear(case.clear),
                    store: wgpu::StoreOp::Store,
                },
            })],
            ..Default::default()
        });
        pass.set_pipeline(&pipeline);
        pass.set_scissor_rect(0, 0, WIDTH / 2, HEIGHT);
        pass.draw(0..3, 0..1);
    }
    queue.submit(Some(encoder.finish()));
}
