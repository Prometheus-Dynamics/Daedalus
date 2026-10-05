//! Paste-friendly report of the GPU, dmabuf import (fence paths, NV12, DRM format modifiers), and
//! kernel dma-buf support of the host.
//!
//! ```text
//! cargo run -p daedalus-gpu --features gpu-dmabuf --example gpu_probe   # or ./scripts/ci.sh pi
//! DAEDALUS_DMA_HEAP=/dev/dma_heap/linux,cma ...                         # heap for the sync_file probe
//! ```
//!
//! Every probe reports its error and the report continues, so missing hardware never panics.
//! See "Validating on a Raspberry Pi 5" in docs/testing.md for what each line means.
#![allow(clippy::print_stdout)] // the report is the program's output

#[cfg(not(target_os = "linux"))]
fn main() {
    println!("gpu_probe: dmabuf probing needs Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::{CStr, c_char};
    use std::fmt::Display;
    use std::fs::File;
    use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

    use ash::{ext, khr, vk};
    use daedalus_gpu::wgpu::{self, hal::api::Vulkan, hal::vulkan as hal_vk};
    use daedalus_gpu::{
        DRM_FORMAT_MOD_LINEAR, DmabufAccess, ExternalImportSupport, GpuBackend, WgpuBackend,
        export_dmabuf_fence,
    };

    const HEAP_DIR: &str = "/dev/dma_heap";
    /// `_IOWR('H', 0, struct dma_heap_allocation_data)`.
    const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800;
    const NV12: vk::Format = vk::Format::G8_B8R8_2PLANE_420_UNORM;

    #[repr(C)]
    struct DmaHeapAllocationData {
        len: u64,
        fd: u32,
        fd_flags: u32,
        heap_flags: u64,
    }

    fn heading(title: &str) {
        println!("\n[{title}]");
    }

    fn kv(key: &str, value: impl Display) {
        println!("{key}: {value}");
    }

    fn yes_no(value: bool) -> &'static str {
        if value { "yes" } else { "no" }
    }

    pub fn run() {
        println!("daedalus-gpu gpu_probe {}", env!("CARGO_PKG_VERSION"));
        heading("system");
        system();
        heading("gpu");
        match WgpuBackend::new() {
            Ok(backend) => gpu(&backend),
            Err(err) => kv("adapter", format!("unavailable: {err}")),
        }
        heading("dma-heap");
        let heaps = dma_heaps();
        heading("sync-file");
        sync_file(&heaps);
    }

    fn system() {
        // SAFETY: `utsname` is plain C data, valid when zeroed.
        let mut uts: libc::utsname = unsafe { std::mem::zeroed() };
        // SAFETY: `uts` is a correctly sized out-struct.
        if unsafe { libc::uname(&mut uts) } == 0 {
            // SAFETY: on success every `utsname` field is NUL-terminated.
            let field = |f: &[c_char]| unsafe { CStr::from_ptr(f.as_ptr()) }.to_string_lossy();
            kv(
                "kernel",
                format!("{} {}", field(&uts.sysname), field(&uts.release)),
            );
            kv("kernel_build", field(&uts.version));
            kv("machine", field(&uts.machine));
        } else {
            kv(
                "kernel",
                format!("uname failed: {}", std::io::Error::last_os_error()),
            );
        }
        match std::fs::read_to_string("/proc/device-tree/model") {
            Ok(model) => kv("model", model.trim_end_matches(['\0', '\n'])),
            Err(err) => kv("model", format!("unknown ({err})")),
        }
    }

    fn gpu(backend: &WgpuBackend) {
        let Some((device, _)) = backend.wgpu_device_queue() else {
            return kv("adapter", "unavailable: backend exposes no wgpu device");
        };
        let info = device.adapter_info();
        kv("name", &info.name);
        kv("backend", format!("{:?}", info.backend));
        kv("device_type", format!("{:?}", info.device_type));
        kv("vendor_id", format!("0x{:04x}", info.vendor));
        kv("device_id", format!("0x{:04x}", info.device));
        kv("driver", &info.driver);
        kv("driver_info", &info.driver_info);
        match backend.dmabuf_import_support() {
            ExternalImportSupport::Supported { acquire_fence } => {
                kv("dmabuf_import", "supported");
                kv("dmabuf_acquire_fence_wait", acquire_fence.as_str());
            }
            ExternalImportSupport::Unsupported { reason } => {
                kv("dmabuf_import", format!("unsupported: {reason}"))
            }
        }
        let nv12 = device
            .features()
            .contains(wgpu::Features::TEXTURE_FORMAT_NV12);
        kv("texture_format_nv12", yes_no(nv12));
        // SAFETY: only read-only queries are made through the hal device.
        let Some(hal) = (unsafe { device.as_hal::<Vulkan>() }) else {
            return kv("nv12_linear", "n/a (not a Vulkan device)");
        };
        let enabled = hal.enabled_device_extensions();
        for ext in [
            khr::external_semaphore_fd::NAME,
            ext::queue_family_foreign::NAME,
        ] {
            kv(&ext.to_string_lossy(), yes_no(enabled.contains(&ext)));
        }
        let (timeline, sync_fd) = fence_paths(&hal);
        kv("timeline_semaphore", yes_no(timeline));
        kv("sync_fd_semaphore_import", yes_no(sync_fd));
        heading("nv12-linear");
        nv12_linear(&hal);
        heading("modifiers");
        modifiers(&hal);
    }

    /// Whether the device has timeline semaphores (the `timeline` fence wait) and imports
    /// `sync_file`s into semaphores (the `sync_fd` wait).
    fn fence_paths(hal: &hal_vk::Device) -> (bool, bool) {
        let instance = hal.shared_instance().raw_instance();
        let phys = hal.raw_physical_device();
        let mut v12 = vk::PhysicalDeviceVulkan12Features::default();
        let mut features = vk::PhysicalDeviceFeatures2::default().push_next(&mut v12);
        // SAFETY: `phys` belongs to `instance`; queries only.
        unsafe { instance.get_physical_device_features2(phys, &mut features) };
        let info = vk::PhysicalDeviceExternalSemaphoreInfo::default()
            .handle_type(vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD);
        let mut props = vk::ExternalSemaphoreProperties::default();
        // SAFETY: as above.
        unsafe {
            instance.get_physical_device_external_semaphore_properties(phys, &info, &mut props)
        };
        let sync_fd = hal
            .enabled_device_extensions()
            .contains(&khr::external_semaphore_fd::NAME)
            && props
                .external_semaphore_features
                .contains(vk::ExternalSemaphoreFeatureFlags::IMPORTABLE);
        (v12.timeline_semaphore == vk::TRUE, sync_fd)
    }

    /// The DRM format modifiers advertised per importable format: memory plane count, whether it
    /// carries aux (compression) planes, the usable features, and whether the hardware tests
    /// round-trip it (`dmabuf_import_tiled_and_compressed_modifiers`: renderable R8/XRGB8888).
    fn modifiers(hal: &hal_vk::Device) {
        if !hal
            .enabled_device_extensions()
            .contains(&ext::image_drm_format_modifier::NAME)
        {
            return kv(
                "modifiers",
                "n/a (VK_EXT_image_drm_format_modifier not enabled)",
            );
        }
        let instance = hal.shared_instance().raw_instance();
        let phys = hal.raw_physical_device();
        let tested_features = vk::FormatFeatureFlags::COLOR_ATTACHMENT
            | vk::FormatFeatureFlags::SAMPLED_IMAGE
            | vk::FormatFeatureFlags::TRANSFER_SRC;
        for (name, format, format_planes, tested) in [
            ("r8", vk::Format::R8_UNORM, 1, true),
            ("xrgb8888", vk::Format::B8G8R8A8_UNORM, 1, true),
            ("xbgr8888", vk::Format::R8G8B8A8_UNORM, 1, false),
            ("nv12", NV12, 2, false),
        ] {
            let list: Vec<String> = format_modifiers(instance, phys, format)
                .iter()
                .map(|m| {
                    let features = m.drm_format_modifier_tiling_features;
                    let planes = m.drm_format_modifier_plane_count;
                    let mut tags = vec![format!("{planes} plane(s)")];
                    if planes > format_planes {
                        tags.push("aux".into());
                    }
                    for (flag, tag) in [
                        (vk::FormatFeatureFlags::SAMPLED_IMAGE, "sample"),
                        (vk::FormatFeatureFlags::COLOR_ATTACHMENT, "render"),
                        (vk::FormatFeatureFlags::STORAGE_IMAGE, "storage"),
                        (vk::FormatFeatureFlags::DISJOINT, "disjoint"),
                    ] {
                        if features.contains(flag) {
                            tags.push(tag.into());
                        }
                    }
                    if tested && features.contains(tested_features) {
                        tags.push("tested".into());
                    }
                    format!("0x{:x} ({})", m.drm_format_modifier, tags.join(", "))
                })
                .collect();
            kv(
                &format!("{name}_modifiers"),
                format!("[{}]", list.join("; ")),
            );
        }
    }

    /// What the driver advertises for NV12 with the `LINEAR` DRM modifier, and whether a dmabuf
    /// import of it is accepted with both planes in one dmabuf and with one dmabuf per plane.
    fn nv12_linear(hal: &hal_vk::Device) {
        let instance = hal.shared_instance().raw_instance();
        let phys = hal.raw_physical_device();
        // SAFETY: `phys` belongs to `instance`.
        let api = unsafe { instance.get_physical_device_properties(phys) }.api_version;
        kv(
            "vulkan_api",
            format!(
                "{}.{}.{}",
                vk::api_version_major(api),
                vk::api_version_minor(api),
                vk::api_version_patch(api)
            ),
        );
        if !hal
            .enabled_device_extensions()
            .contains(&ext::image_drm_format_modifier::NAME)
        {
            return kv(
                "nv12_linear",
                "n/a (VK_EXT_image_drm_format_modifier not enabled)",
            );
        }
        let modifiers = format_modifiers(instance, phys, NV12);
        let list: Vec<String> = modifiers
            .iter()
            .map(|m| format!("0x{:x}", m.drm_format_modifier))
            .collect();
        kv("nv12_modifiers", format!("[{}]", list.join(", ")));
        let Some(linear) = modifiers
            .iter()
            .find(|m| m.drm_format_modifier == DRM_FORMAT_MOD_LINEAR)
        else {
            return kv("nv12_linear", "not advertised");
        };
        let features = linear.drm_format_modifier_tiling_features;
        kv("nv12_linear", "advertised");
        kv(
            "nv12_linear_memory_planes",
            linear.drm_format_modifier_plane_count,
        );
        kv("nv12_linear_tiling_features", format!("{features:?}"));
        let disjoint_feature = features.contains(vk::FormatFeatureFlags::DISJOINT);
        kv("nv12_linear_disjoint_feature", yes_no(disjoint_feature));
        let single = import_check(instance, phys, vk::ImageCreateFlags::empty());
        let disjoint = if disjoint_feature {
            import_check(instance, phys, vk::ImageCreateFlags::DISJOINT)
        } else {
            Err("modifier lacks the DISJOINT format feature".into())
        };
        let verdict = |r: &Result<(), String>| match r {
            Ok(()) => "ok".to_string(),
            Err(err) => format!("rejected: {err}"),
        };
        kv("nv12_linear_import_one_dmabuf", verdict(&single));
        kv("nv12_linear_import_disjoint", verdict(&disjoint));
        kv(
            "nv12_linear_needs_disjoint",
            yes_no(single.is_err() && disjoint.is_ok()),
        );
    }

    fn format_modifiers(
        instance: &ash::Instance,
        phys: vk::PhysicalDevice,
        format: vk::Format,
    ) -> Vec<vk::DrmFormatModifierPropertiesEXT> {
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
        modifiers
    }

    /// The image-format query the import makes for a sampled NV12 `LINEAR` dmabuf image.
    fn import_check(
        instance: &ash::Instance,
        phys: vk::PhysicalDevice,
        extra: vk::ImageCreateFlags,
    ) -> Result<(), String> {
        let mut external = vk::PhysicalDeviceExternalImageFormatInfo::default()
            .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
        let mut modifier = vk::PhysicalDeviceImageDrmFormatModifierInfoEXT::default()
            .drm_format_modifier(DRM_FORMAT_MOD_LINEAR)
            .sharing_mode(vk::SharingMode::EXCLUSIVE);
        // The import lists its view formats (the format and its plane formats), as required for a
        // `MUTABLE_FORMAT` image with a DRM format modifier.
        let view_formats = [NV12, vk::Format::R8_UNORM, vk::Format::R8G8_UNORM];
        let mut format_list = vk::ImageFormatListCreateInfo::default().view_formats(&view_formats);
        let info = vk::PhysicalDeviceImageFormatInfo2::default()
            .format(NV12)
            .ty(vk::ImageType::TYPE_2D)
            .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
            .usage(vk::ImageUsageFlags::SAMPLED)
            .flags(
                vk::ImageCreateFlags::MUTABLE_FORMAT | vk::ImageCreateFlags::EXTENDED_USAGE | extra,
            )
            .push_next(&mut external)
            .push_next(&mut modifier)
            .push_next(&mut format_list);
        let mut external_props = vk::ExternalImageFormatProperties::default();
        let mut props = vk::ImageFormatProperties2::default().push_next(&mut external_props);
        // SAFETY: valid physical device and structure chains.
        unsafe { instance.get_physical_device_image_format_properties2(phys, &info, &mut props) }
            .map_err(|err| err.to_string())?;
        let importable = external_props
            .external_memory_properties
            .external_memory_features
            .contains(vk::ExternalMemoryFeatureFlags::IMPORTABLE);
        importable
            .then_some(())
            .ok_or_else(|| "dmabuf memory not importable".into())
    }

    /// List the dma-heaps and report whether each opens; returns the ones that do.
    fn dma_heaps() -> Vec<String> {
        let mut paths: Vec<String> = match std::fs::read_dir(HEAP_DIR) {
            Ok(dir) => dir
                .flatten()
                .map(|e| e.path().to_string_lossy().into_owned())
                .collect(),
            Err(err) => {
                kv(HEAP_DIR, format!("unavailable: {err}"));
                return Vec::new();
            }
        };
        paths.sort();
        if paths.is_empty() {
            kv(HEAP_DIR, "no heaps");
        }
        paths
            .into_iter()
            .filter(
                |path| match File::options().read(true).write(true).open(path) {
                    Ok(_) => {
                        kv(path, "opens");
                        true
                    }
                    Err(err) => {
                        kv(path, format!("open failed: {err}"));
                        false
                    }
                },
            )
            .collect()
    }

    /// Allocate a page from a dma-heap and snapshot its implicit fences as a `sync_file`.
    fn sync_file(heaps: &[String]) {
        let heap = std::env::var("DAEDALUS_DMA_HEAP").ok().or_else(|| {
            let system = heaps.iter().find(|h| h.ends_with("/system"));
            system.or(heaps.first()).cloned()
        });
        let Some(heap) = heap else {
            return kv("export_sync_file", "untested (no openable dma-heap)");
        };
        kv("heap", &heap);
        let dmabuf = match alloc(&heap, 4096) {
            Ok(fd) => fd,
            Err(err) => {
                return kv(
                    "export_sync_file",
                    format!("untested (alloc failed: {err})"),
                );
            }
        };
        match export_dmabuf_fence(dmabuf.as_fd(), DmabufAccess::Read) {
            Ok(fence) => {
                let mut pfd = libc::pollfd {
                    fd: fence.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: one valid pollfd, zero timeout.
                let signaled = unsafe { libc::poll(&mut pfd, 1, 0) } > 0;
                kv("export_sync_file", "works");
                kv("fence_signaled", yes_no(signaled));
            }
            Err(err) if err.raw_os_error() == Some(libc::ENOTTY) => kv(
                "export_sync_file",
                format!("unsupported ({err}; needs Linux 6.0+)"),
            ),
            Err(err) => kv("export_sync_file", format!("error: {err}")),
        }
    }

    fn alloc(heap: &str, len: u64) -> std::io::Result<OwnedFd> {
        let heap = File::options().read(true).write(true).open(heap)?;
        let mut data = DmaHeapAllocationData {
            len,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // SAFETY: valid heap fd and a correctly laid out argument struct.
        let rc = unsafe { libc::ioctl(heap.as_raw_fd(), DMA_HEAP_IOCTL_ALLOC as _, &mut data) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the kernel returned a fresh fd the caller now owns.
        Ok(unsafe { OwnedFd::from_raw_fd(data.fd as i32) })
    }
}
