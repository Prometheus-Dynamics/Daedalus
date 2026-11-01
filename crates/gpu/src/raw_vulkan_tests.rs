//! A SPIR-V kernel made outside Daedalus (a raw `ash` compute pipeline) run on Daedalus's device
//! through `submit_raw`, reading an imported camera frame buffer in place (skipped without a
//! Vulkan device; the frame needs `/dev/dma_heap/system` and dmabuf-importable storage, else the
//! kernel reads a plain `GpuStorage`).
#![allow(clippy::print_stderr)]

use ash::vk;

use super::RawVulkan;
use crate::{GpuBackendKind, GpuContextHandle, GpuOptions, GpuStorage, select_backend};

/// Inverts every word of `src` into `dst`.
const INVERT: &str = "
@group(0) @binding(0) var<storage, read> src: array<u32>;
@group(0) @binding(1) var<storage, read_write> dst: array<u32>;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) g: vec3<u32>) {
    if (g.x < arrayLength(&dst)) {
        dst[g.x] = ~src[g.x];
    }
}
";

fn spirv() -> Vec<u32> {
    let module = naga::front::wgsl::parse_str(INVERT).unwrap();
    let info = naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)
    .unwrap();
    let pipeline = naga::back::spv::PipelineOptions {
        shader_stage: naga::ShaderStage::Compute,
        entry_point: "main".into(),
    };
    naga::back::spv::write_vec(
        &module,
        &info,
        &naga::back::spv::Options::default(),
        Some(&pipeline),
    )
    .unwrap()
}

fn vulkan_ctx() -> Option<(GpuContextHandle, RawVulkan)> {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .ok()?;
    // SAFETY: the handles are used within the test, under the sync contract.
    let raw = unsafe { ctx.raw_vulkan() }?;
    Some((ctx, raw))
}

/// The pipeline and the descriptor set binding `src` and `dst`, destroyed on drop.
struct Invert<'a> {
    device: &'a ash::Device,
    module: vk::ShaderModule,
    set_layout: vk::DescriptorSetLayout,
    layout: vk::PipelineLayout,
    pipeline: vk::Pipeline,
    pool: vk::DescriptorPool,
    set: vk::DescriptorSet,
}

impl<'a> Invert<'a> {
    /// # Safety
    /// `src` and `dst` must be buffers of `device` and outlive the returned value's use.
    unsafe fn new(device: &'a ash::Device, src: &GpuStorage, dst: &GpuStorage) -> Self {
        // SAFETY: forwarded.
        unsafe { Self::with_code(device, &spirv(), src, dst) }
    }

    /// # Safety
    /// As [`Invert::new`]; `code` must be a valid compute shader with `main` reading binding 0
    /// and writing binding 1.
    unsafe fn with_code(
        device: &'a ash::Device,
        code: &[u32],
        src: &GpuStorage,
        dst: &GpuStorage,
    ) -> Self {
        // SAFETY (throughout): valid create infos on a live device.
        unsafe {
            let module = device
                .create_shader_module(&vk::ShaderModuleCreateInfo::default().code(code), None)
                .unwrap();
            let bindings = [0, 1].map(|binding| {
                vk::DescriptorSetLayoutBinding::default()
                    .binding(binding)
                    .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                    .descriptor_count(1)
                    .stage_flags(vk::ShaderStageFlags::COMPUTE)
            });
            let set_layout = device
                .create_descriptor_set_layout(
                    &vk::DescriptorSetLayoutCreateInfo::default().bindings(&bindings),
                    None,
                )
                .unwrap();
            let layouts = [set_layout];
            let layout = device
                .create_pipeline_layout(
                    &vk::PipelineLayoutCreateInfo::default().set_layouts(&layouts),
                    None,
                )
                .unwrap();
            let stage = vk::PipelineShaderStageCreateInfo::default()
                .stage(vk::ShaderStageFlags::COMPUTE)
                .module(module)
                .name(c"main");
            let pipeline = device
                .create_compute_pipelines(
                    vk::PipelineCache::null(),
                    &[vk::ComputePipelineCreateInfo::default()
                        .stage(stage)
                        .layout(layout)],
                    None,
                )
                .unwrap()[0];
            let sizes = [vk::DescriptorPoolSize::default()
                .ty(vk::DescriptorType::STORAGE_BUFFER)
                .descriptor_count(2)];
            let pool = device
                .create_descriptor_pool(
                    &vk::DescriptorPoolCreateInfo::default()
                        .max_sets(1)
                        .pool_sizes(&sizes),
                    None,
                )
                .unwrap();
            let set = device
                .allocate_descriptor_sets(
                    &vk::DescriptorSetAllocateInfo::default()
                        .descriptor_pool(pool)
                        .set_layouts(&layouts),
                )
                .unwrap()[0];
            let infos = [src, dst].map(|storage| {
                let raw = storage.raw_vk_buffer().unwrap();
                [vk::DescriptorBufferInfo::default()
                    .buffer(raw.buffer)
                    .offset(raw.offset)
                    .range(raw.size)]
            });
            let writes = [0, 1].map(|binding| {
                vk::WriteDescriptorSet::default()
                    .dst_set(set)
                    .dst_binding(binding)
                    .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                    .buffer_info(&infos[binding as usize])
            });
            device.update_descriptor_sets(&writes, &[]);
            Self {
                device,
                module,
                set_layout,
                layout,
                pipeline,
                pool,
                set,
            }
        }
    }

    /// # Safety
    /// `cmd` must be recording on the pipeline's device.
    unsafe fn record(&self, cmd: vk::CommandBuffer, words: u32) {
        // SAFETY: as above.
        unsafe {
            let d = self.device;
            d.cmd_bind_pipeline(cmd, vk::PipelineBindPoint::COMPUTE, self.pipeline);
            d.cmd_bind_descriptor_sets(
                cmd,
                vk::PipelineBindPoint::COMPUTE,
                self.layout,
                0,
                &[self.set],
                &[],
            );
            d.cmd_dispatch(cmd, words.div_ceil(64), 1, 1);
        }
    }
}

impl Drop for Invert<'_> {
    fn drop(&mut self) {
        // SAFETY: created in `new`, unused once the submission completed.
        unsafe {
            let d = self.device;
            d.destroy_descriptor_pool(self.pool, None);
            d.destroy_pipeline(self.pipeline, None);
            d.destroy_pipeline_layout(self.layout, None);
            d.destroy_descriptor_set_layout(self.set_layout, None);
            d.destroy_shader_module(self.module, None);
        }
    }
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

/// Run the raw kernel over `src` and check the result against `input`.
fn run_and_check(ctx: &GpuContextHandle, raw: &RawVulkan, src: &GpuStorage, input: &[u8]) {
    let dst = GpuStorage::new_host_cached(ctx, input.len() as u64, Some("raw-dst")).unwrap();
    // SAFETY: both storages outlive the submission, which the ticket waits for.
    let invert = unsafe { Invert::new(&raw.device, src, &dst) };
    let words = (input.len() / 4) as u32;
    // SAFETY: valid commands; everything they use outlives the wait below.
    let ticket = unsafe { ctx.submit_raw(|_, cmd| invert.record(cmd, words)) }.unwrap();
    ticket.wait();
    let mut out = vec![0u8; input.len()];
    dst.read_mapped(0, &mut out).unwrap();
    let expected: Vec<u8> = input.iter().map(|b| !b).collect();
    assert_eq!(out, expected);
    drop(invert);
}

#[test]
fn a_raw_spirv_dispatch_runs_on_daedalus_storage() {
    let Some((ctx, raw)) = vulkan_ctx() else {
        return eprintln!("skipping: no Vulkan device");
    };
    let input = pattern(64 << 10);
    let src = GpuStorage::with_data(&ctx, &input, Some("raw-src")).unwrap();
    let range = unsafe { src.raw_vk_buffer() }.unwrap();
    assert_eq!((range.offset, range.size), (0, input.len() as u64));
    assert_ne!(range.buffer, vk::Buffer::null());
    run_and_check(&ctx, &raw, &src, &input);
    // Daedalus's own work after the raw submission sees its result (queue order).
    let again = GpuStorage::new(&ctx, input.len() as u64, None).unwrap();
    crate::GpuKernels::new(&ctx)
        .unwrap()
        .run(crate::GpuKernel::Copy, &src, &again, input.len() as u64)
        .unwrap()
        .wait();
    let mut copied = vec![0u8; input.len()];
    again.read_into(0, &mut copied).unwrap();
    assert_eq!(copied, input);
}

#[cfg(feature = "gpu-dmabuf")]
#[test]
fn a_raw_spirv_dispatch_reads_an_imported_frame_in_place() {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::Duration;

    let Some((ctx, raw)) = vulkan_ctx() else {
        return eprintln!("skipping: no Vulkan device");
    };
    // A 640x480 GREY frame in a dma-heap buffer, sized as the import requires.
    let frame_len = 640 * 480;
    let needed = match GpuStorage::dmabuf_import_len(&ctx, frame_len) {
        Ok(needed) => needed,
        Err(err) => return eprintln!("skipping: {err}"),
    };
    eprintln!("a {frame_len}-byte import needs {needed} bytes of dmabuf");
    let Ok(heap) = std::fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/dma_heap/system")
    else {
        return eprintln!("skipping: no /dev/dma_heap/system access");
    };
    #[repr(C)]
    struct Alloc {
        len: u64,
        fd: u32,
        fd_flags: u32,
        heap_flags: u64,
    }
    let mut req = Alloc {
        len: needed,
        fd: 0,
        fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
        heap_flags: 0,
    };
    // SAFETY: `req` is the DMA_HEAP_IOCTL_ALLOC argument.
    if unsafe { libc::ioctl(heap.as_raw_fd(), 0xC018_4800u64 as _, &mut req) } != 0 {
        return eprintln!("skipping: dma-heap allocation failed");
    }
    // SAFETY: a new dmabuf fd we own.
    let fd = unsafe { OwnedFd::from_raw_fd(req.fd as i32) };
    let pixels = pattern(frame_len as usize);
    // SAFETY: map, fill and unmap the frame's bytes.
    unsafe {
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            frame_len as usize,
            libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd.as_raw_fd(),
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        std::ptr::copy_nonoverlapping(pixels.as_ptr(), ptr.cast(), pixels.len());
        libc::munmap(ptr, frame_len as usize);
    }
    let frame = match GpuStorage::import_dmabuf(
        &ctx,
        fd,
        0,
        frame_len,
        None,
        Duration::from_secs(1),
        Some("raw-frame"),
    ) {
        Ok(frame) => frame,
        Err(err) => return eprintln!("skipping: cannot import dmabuf storage ({err})"),
    };
    run_and_check(&ctx, &raw, &frame, &pixels);
}

/// 8-bit samples from a storage buffer (`storageBuffer8BitAccess`, which Daedalus enables where
/// the device has it), as gpuisp reads raw Bayer data: a precompiled GLSL kernel
/// (`raw_vulkan/widen_u8.comp`) widens each byte to a word.
#[test]
fn a_raw_kernel_reads_8_bit_storage() {
    let Some((ctx, raw)) = vulkan_ctx() else {
        return eprintln!("skipping: no Vulkan device");
    };
    eprintln!(
        "storage_buffer_8bit {}, uniform_and_storage_buffer_8bit {}, timestamp bits {}, period {} ns",
        raw.storage_buffer_8bit,
        raw.uniform_and_storage_buffer_8bit,
        raw.timestamp_valid_bits,
        raw.timestamp_period_ns
    );
    if !raw.storage_buffer_8bit {
        return eprintln!("skipping: the device has no 8-bit storage-buffer access");
    }
    let bytes = include_bytes!("raw_vulkan/widen_u8.spv");
    let code: Vec<u32> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|w| u32::from_le_bytes(*w))
        .collect();
    let input = pattern(4096);
    let src = GpuStorage::with_data(&ctx, &input, None).unwrap();
    let dst = GpuStorage::new_host_cached(&ctx, 4 * input.len() as u64, None).unwrap();
    // SAFETY: both storages outlive the submission, which the ticket waits for.
    let widen = unsafe { Invert::with_code(&raw.device, &code, &src, &dst) };
    // SAFETY: valid commands; everything they use outlives the wait below.
    let ticket = unsafe { ctx.submit_raw(|_, cmd| widen.record(cmd, input.len() as u32)) }.unwrap();
    ticket.wait();
    let mut out = vec![0u8; 4 * input.len()];
    dst.read_mapped(0, &mut out).unwrap();
    let words: Vec<u32> = out
        .as_chunks::<4>()
        .0
        .iter()
        .map(|w| u32::from_le_bytes(*w))
        .collect();
    let want: Vec<u32> = input.iter().map(|&b| u32::from(b) * 3).collect();
    assert_eq!(words, want);
}
