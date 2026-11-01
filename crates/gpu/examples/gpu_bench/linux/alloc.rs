//! What a GPU buffer costs, stage by stage: `vkCreateBuffer` and `vkAllocateMemory` alone, wgpu's
//! `create_buffer` under each memory-hint mode (the first allocation creates a memory block),
//! the first use (wgpu's lazy zero-initialization clear) against a buffer without it, and the
//! host-mapped buffer's creation and in-place write.

use std::time::{Duration, Instant};

use ash::vk;
use daedalus_gpu::wgpu::hal::api::Vulkan;
use daedalus_gpu::{
    GpuBackendKind, GpuContextHandle, GpuKernel, GpuKernels, GpuMemoryHints, GpuOptions,
    GpuStorage, select_backend,
};

use super::{kv, median};

const SIZES: [u64; 3] = [4 << 10, 1 << 20, 16 << 20];

fn ms(d: Duration) -> String {
    format!("{:.3} ms", d.as_secs_f64() * 1e3)
}

fn ctx(hints: GpuMemoryHints) -> Option<GpuContextHandle> {
    select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        memory_hints: hints,
        ..Default::default()
    })
    .ok()
    .filter(|c| c.wgpu().is_some())
}

/// Submit-to-done of a 4-byte copy reading `buffer` (its first use triggers the clear).
fn first_use(kernels: &GpuKernels, buffer: &GpuStorage, sink: &GpuStorage) -> Duration {
    let start = Instant::now();
    kernels
        .run(GpuKernel::Copy, buffer, sink, 4)
        .map(|t| t.wait())
        .ok();
    start.elapsed()
}

pub(super) fn allocation() {
    for hints in [GpuMemoryHints::Performance, GpuMemoryHints::MemoryUsage] {
        let Some(ctx) = ctx(hints) else {
            return kv("allocation", "no wgpu context");
        };
        let name = format!("{hints:?}").to_lowercase();
        let kernels = match GpuKernels::new(&ctx) {
            Ok(k) => k,
            Err(err) => return kv("allocation", format!("kernels: {err}")),
        };
        let sink = GpuStorage::new(&ctx, 4, None).unwrap();
        let _ = first_use(&kernels, &sink, &sink);
        for size in SIZES {
            let label = format!("{name}_{}k", size >> 10);
            // The first allocation of a size on a fresh device, then 10 more.
            let start = Instant::now();
            let first = GpuStorage::new(&ctx, size, None);
            let first_create = start.elapsed();
            let mut more = Vec::new();
            let mut keep = Vec::new();
            for _ in 0..10 {
                let start = Instant::now();
                if let Ok(b) = GpuStorage::new(&ctx, size, None) {
                    more.push(start.elapsed());
                    keep.push(b);
                }
            }
            let Ok(first) = first else {
                kv(&label, "allocation failed");
                continue;
            };
            let clear = first_use(&kernels, &first, &sink);
            let reuse = first_use(&kernels, &first, &sink);
            kv(
                &format!("{label}_create_buffer"),
                format!(
                    "first {}, then median {}",
                    ms(first_create),
                    ms(median(&mut more))
                ),
            );
            kv(
                &format!("{label}_first_use"),
                format!("{} (lazy zero-init clear), reused {}", ms(clear), ms(reuse)),
            );
            match GpuStorage::new_uninit(&ctx, size, None) {
                Ok(b) => {
                    let use_ = first_use(&kernels, &b, &sink);
                    kv(&format!("{label}_uninit_first_use"), ms(use_));
                }
                Err(err) => kv(&format!("{label}_uninit"), err),
            }
            let start = Instant::now();
            match GpuStorage::new_host_mapped(&ctx, size, None) {
                Ok(b) => {
                    let create = start.elapsed();
                    let data = vec![0x5a; size as usize];
                    let start = Instant::now();
                    let _ = b.write_mapped(0, &data);
                    kv(
                        &format!("{label}_host_mapped"),
                        format!(
                            "create {}, write in place {}",
                            ms(create),
                            ms(start.elapsed())
                        ),
                    );
                }
                Err(err) => kv(&format!("{label}_host_mapped"), err),
            }
            for (kind, made) in [
                ("host_mapped", GpuStorage::new_host_mapped(&ctx, size, None)),
                ("host_cached", GpuStorage::new_host_cached(&ctx, size, None)),
            ] {
                if let Ok(b) = made {
                    let mut out = vec![0u8; size as usize];
                    let start = Instant::now();
                    let _ = b.read_mapped(0, &mut out);
                    kv(&format!("{label}_{kind}_read"), ms(start.elapsed()));
                }
            }
        }
    }
    raw_vulkan();
}

/// `vkCreateBuffer` and a dedicated `vkAllocateMemory` (device-local), each alone.
fn raw_vulkan() {
    let Some(ctx) = ctx(GpuMemoryHints::MemoryUsage) else {
        return;
    };
    let (device, _) = ctx.wgpu().unwrap();
    // SAFETY: raw calls on wgpu's own Vulkan device; everything created is destroyed here.
    unsafe {
        let Some(hal) = device.as_hal::<Vulkan>() else {
            return kv("vk_raw", "not Vulkan");
        };
        let raw = hal.raw_device();
        let instance = hal.shared_instance().raw_instance();
        let props = instance.get_physical_device_memory_properties(hal.raw_physical_device());
        for size in SIZES {
            let info = vk::BufferCreateInfo::default()
                .size(size)
                .usage(vk::BufferUsageFlags::STORAGE_BUFFER | vk::BufferUsageFlags::TRANSFER_DST);
            let (mut create, mut alloc) = (Vec::new(), Vec::new());
            for _ in 0..10 {
                let start = Instant::now();
                let Ok(buffer) = raw.create_buffer(&info, None) else {
                    break;
                };
                create.push(start.elapsed());
                let req = raw.get_buffer_memory_requirements(buffer);
                let index = props.memory_types[..props.memory_type_count as usize]
                    .iter()
                    .enumerate()
                    .position(|(i, t)| {
                        req.memory_type_bits & (1 << i) != 0
                            && t.property_flags
                                .contains(vk::MemoryPropertyFlags::DEVICE_LOCAL)
                    })
                    .unwrap_or(0);
                let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
                let a = vk::MemoryAllocateInfo::default()
                    .allocation_size(req.size)
                    .memory_type_index(index as u32)
                    .push_next(&mut dedicated);
                let start = Instant::now();
                if let Ok(memory) = raw.allocate_memory(&a, None) {
                    alloc.push(start.elapsed());
                    raw.free_memory(memory, None);
                }
                raw.destroy_buffer(buffer, None);
            }
            if create.is_empty() || alloc.is_empty() {
                continue;
            }
            kv(
                &format!("vk_raw_{}k", size >> 10),
                format!(
                    "vkCreateBuffer median {}, vkAllocateMemory (dedicated) median {}",
                    ms(median(&mut create)),
                    ms(median(&mut alloc))
                ),
            );
        }
    }
}
