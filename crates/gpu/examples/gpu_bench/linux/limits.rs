//! The compute limits and the Vulkan features wgpu does not surface.

use ash::vk;
use daedalus_gpu::wgpu::{self, hal::api::Vulkan};

use super::{kv, yes_no};

/// The wgpu limits (the device requests the adapter's) and the Vulkan properties wgpu does not
/// surface: 64-bit and 16-bit types, atomics, timestamps, storage image formats.
pub(super) fn limits(device: &wgpu::Device) {
    let l = device.limits();
    kv(
        "max_compute_workgroup_storage_size",
        l.max_compute_workgroup_storage_size,
    );
    kv(
        "max_compute_invocations_per_workgroup",
        l.max_compute_invocations_per_workgroup,
    );
    kv(
        "max_compute_workgroup_size",
        format!(
            "{}x{}x{}",
            l.max_compute_workgroup_size_x,
            l.max_compute_workgroup_size_y,
            l.max_compute_workgroup_size_z
        ),
    );
    kv(
        "max_compute_workgroups_per_dimension",
        l.max_compute_workgroups_per_dimension,
    );
    kv(
        "max_storage_buffer_binding_size",
        l.max_storage_buffer_binding_size,
    );
    kv(
        "max_storage_buffers_per_shader_stage",
        l.max_storage_buffers_per_shader_stage,
    );
    kv(
        "max_storage_textures_per_shader_stage",
        l.max_storage_textures_per_shader_stage,
    );
    kv("max_buffer_size", l.max_buffer_size);
    kv("wgpu_enabled_features", format!("{:?}", device.features()));
    // SAFETY: only read-only queries are made through the hal device.
    let Some(hal) = (unsafe { device.as_hal::<Vulkan>() }) else {
        return kv("vulkan", "n/a (not a Vulkan device)");
    };
    let instance = hal.shared_instance().raw_instance();
    let phys = hal.raw_physical_device();
    let mut v11 = vk::PhysicalDeviceVulkan11Features::default();
    let mut v12 = vk::PhysicalDeviceVulkan12Features::default();
    let mut features = vk::PhysicalDeviceFeatures2::default()
        .push_next(&mut v11)
        .push_next(&mut v12);
    // SAFETY: `phys` belongs to `instance`; queries only.
    unsafe { instance.get_physical_device_features2(phys, &mut features) };
    let f = features.features;
    let mut subgroup = vk::PhysicalDeviceSubgroupProperties::default();
    let mut props = vk::PhysicalDeviceProperties2::default().push_next(&mut subgroup);
    // SAFETY: as above.
    unsafe { instance.get_physical_device_properties2(phys, &mut props) };
    let p = props.properties;
    let api = p.api_version;
    kv(
        "vulkan_api",
        format!(
            "{}.{}.{}",
            vk::api_version_major(api),
            vk::api_version_minor(api),
            vk::api_version_patch(api)
        ),
    );
    let lim = p.limits;
    kv(
        "vk_max_compute_shared_memory_size",
        lim.max_compute_shared_memory_size,
    );
    kv(
        "vk_max_compute_work_group_invocations",
        lim.max_compute_work_group_invocations,
    );
    kv("vk_max_storage_buffer_range", lim.max_storage_buffer_range);
    kv(
        "vk_min_storage_buffer_offset_alignment",
        lim.min_storage_buffer_offset_alignment,
    );
    kv("shader_int64", yes_no(f.shader_int64 == vk::TRUE));
    kv("shader_int16", yes_no(f.shader_int16 == vk::TRUE));
    kv("shader_float64", yes_no(f.shader_float64 == vk::TRUE));
    kv("shader_float16", yes_no(v12.shader_float16 == vk::TRUE));
    kv(
        "storage_buffer_16bit_access",
        yes_no(v11.storage_buffer16_bit_access == vk::TRUE),
    );
    kv(
        "shader_buffer_int64_atomics",
        yes_no(v12.shader_buffer_int64_atomics == vk::TRUE),
    );
    kv(
        "shader_shared_int64_atomics",
        yes_no(v12.shader_shared_int64_atomics == vk::TRUE),
    );
    kv(
        "shader_storage_image_extended_formats",
        yes_no(f.shader_storage_image_extended_formats == vk::TRUE),
    );
    kv(
        "subgroup_size",
        format!(
            "{} (ops {:?})",
            subgroup.subgroup_size, subgroup.supported_operations
        ),
    );
    kv(
        "timestamp_compute_and_graphics",
        yes_no(lim.timestamp_compute_and_graphics == vk::TRUE),
    );
    kv("timestamp_period_ns", lim.timestamp_period);
    // SAFETY: as above.
    let families = unsafe { instance.get_physical_device_queue_family_properties(phys) };
    let bits: Vec<String> = families
        .iter()
        .map(|q| format!("{:?}={}", q.queue_flags, q.timestamp_valid_bits))
        .collect();
    kv("queue_timestamp_valid_bits", bits.join(", "));
    let storage: Vec<String> = [
        ("r32uint", vk::Format::R32_UINT),
        ("r32sint", vk::Format::R32_SINT),
        ("r32float", vk::Format::R32_SFLOAT),
        ("rg32float", vk::Format::R32G32_SFLOAT),
        ("rgba8unorm", vk::Format::R8G8B8A8_UNORM),
        ("rgba8uint", vk::Format::R8G8B8A8_UINT),
        ("r8unorm", vk::Format::R8_UNORM),
        ("r16float", vk::Format::R16_SFLOAT),
        ("rgba16float", vk::Format::R16G16B16A16_SFLOAT),
        ("rgba32float", vk::Format::R32G32B32A32_SFLOAT),
    ]
    .iter()
    .map(|(name, format)| {
        // SAFETY: as above.
        let fp = unsafe { instance.get_physical_device_format_properties(phys, *format) };
        let t = fp.optimal_tiling_features;
        let mut tags = Vec::new();
        if t.contains(vk::FormatFeatureFlags::STORAGE_IMAGE) {
            tags.push("storage");
        }
        if t.contains(vk::FormatFeatureFlags::STORAGE_IMAGE_ATOMIC) {
            tags.push("atomic");
        }
        format!(
            "{name}({})",
            if tags.is_empty() {
                "-".into()
            } else {
                tags.join("+")
            }
        )
    })
    .collect();
    kv("storage_image_formats", storage.join(" "));
}
