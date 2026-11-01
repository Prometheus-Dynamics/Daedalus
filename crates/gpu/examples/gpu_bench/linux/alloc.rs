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

use super::{kv, median, stats, wait};

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
    // The CPU's own copy speed, for the mapped reads above.
    for size in SIZES {
        let (src, mut dst) = (vec![0x5a_u8; size as usize], vec![0_u8; size as usize]);
        let mut times = Vec::new();
        for _ in 0..10 {
            let start = Instant::now();
            dst.copy_from_slice(std::hint::black_box(&src));
            times.push(start.elapsed());
        }
        std::hint::black_box(&dst);
        kv(
            &format!("cpu_memcpy_{}k", size >> 10),
            ms(median(&mut times)),
        );
    }
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
        for (i, t) in props.memory_types[..props.memory_type_count as usize]
            .iter()
            .enumerate()
        {
            kv(
                &format!("vk_memory_type_{i}"),
                format!("heap {} {:?}", t.heap_index, t.property_flags),
            );
        }
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

/// Eight small kernels as eight submissions against one encoder and one submission: wgpu's
/// per-encoder and per-submit cost (and allocations) paid once per frame instead of per kernel.
pub(super) fn batching() {
    let Some(ctx) = ctx(GpuMemoryHints::Auto) else {
        return kv("batching", "no wgpu context");
    };
    let (device, queue) = ctx.wgpu().unwrap();
    let Ok(kernels) = GpuKernels::new(&ctx) else {
        return;
    };
    let n = 64 << 10;
    let bufs: Vec<(GpuStorage, GpuStorage)> = (0..8)
        .map(|_| {
            (
                GpuStorage::new(&ctx, n, None).unwrap(),
                GpuStorage::new(&ctx, n, None).unwrap(),
            )
        })
        .collect();
    let wait = || {
        let _ = device.poll(daedalus_gpu::wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
    };
    let mut separate = Vec::new();
    let mut batched = Vec::new();
    for round in 0..40 {
        let start = Instant::now();
        for (src, dst) in &bufs {
            let _ = kernels.run(GpuKernel::Copy, src, dst, n);
        }
        wait();
        let t = start.elapsed();
        let start = Instant::now();
        let mut encoder = device.create_command_encoder(&Default::default());
        for (src, dst) in &bufs {
            let _ = kernels.encode(&mut encoder, GpuKernel::Copy, src, dst, n, None);
        }
        queue.submit([encoder.finish()]);
        wait();
        if round >= 5 {
            separate.push(t);
            batched.push(start.elapsed());
        }
    }
    kv(
        "batching_8_kernels",
        format!(
            "8 submits {} per frame, 1 encoder + 1 submit {} per frame (64 KiB copies)",
            ms(median(&mut separate)),
            ms(median(&mut batched))
        ),
    );
}

/// What one small dispatch costs through `GpuKernels` against the same dispatch written directly
/// in wgpu (one pipeline and bind group made up front, five storage bindings like the kernels):
/// CPU time to record and submit, and submit to done.
pub(super) fn dispatch_floor() {
    let Some(ctx) = ctx(GpuMemoryHints::Auto) else {
        return kv("dispatch_floor", "no wgpu context");
    };
    let (device, queue) = ctx.wgpu().unwrap();
    let Ok(kernels) = GpuKernels::new(&ctx) else {
        return;
    };
    let n: u64 = 64 << 10;
    let src = GpuStorage::new(&ctx, n, None).unwrap();
    let dst = GpuStorage::new(&ctx, n, None).unwrap();
    let params = GpuStorage::with_data(&ctx, &((n / 4) as u32).to_le_bytes(), None).unwrap();
    let status = GpuStorage::new(&ctx, 8, None).unwrap();
    let aux = GpuStorage::new(&ctx, 1024, None).unwrap();
    let pipeline = super::pipeline(
        device,
        "@group(0) @binding(0) var<storage, read> s: array<u32>;
         @group(0) @binding(1) var<storage, read_write> d: array<u32>;
         @group(0) @binding(2) var<storage, read> p: array<u32>;
         @group(0) @binding(3) var<storage, read_write> st: array<u32>;
         @group(0) @binding(4) var<storage, read> a: array<u32>;
         @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) g: vec3<u32>) {
             if (g.x < p[0] + st[0] * 0u + a[0] * 0u) { d[g.x] = s[g.x]; }
         }",
    );
    let entries: Vec<_> = [&src, &dst, &params, &status, &aux]
        .iter()
        .enumerate()
        .map(|(i, b)| daedalus_gpu::wgpu::BindGroupEntry {
            binding: i as u32,
            resource: b.buffer().as_entire_binding(),
        })
        .collect();
    let bind = device.create_bind_group(&daedalus_gpu::wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &entries,
    });
    let raw = || {
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups((n / 4).div_ceil(64) as u32, 1, 1);
        }
        queue.submit([encoder.finish()]);
    };
    let encode = || {
        let mut encoder = device.create_command_encoder(&Default::default());
        let _ = kernels.encode(&mut encoder, GpuKernel::Copy, &src, &dst, n, None);
        queue.submit([encoder.finish()]);
    };
    let run = || {
        let _ = kernels.run(GpuKernel::Copy, &src, &dst, n);
    };
    for (name, f) in [
        ("raw_wgpu", &raw as &dyn Fn()),
        ("gpukernels_encode", &encode),
        ("gpukernels_run", &run),
    ] {
        for _ in 0..20 {
            f();
            wait(device);
        }
        let (mut cpu, mut done) = (Vec::new(), Vec::new());
        for _ in 0..200 {
            let start = Instant::now();
            f();
            cpu.push(start.elapsed());
            wait(device);
            done.push(start.elapsed());
        }
        kv(&format!("dispatch_{name}_record_submit"), stats(&mut cpu));
        kv(&format!("dispatch_{name}_submit_to_done"), stats(&mut done));
    }
}
