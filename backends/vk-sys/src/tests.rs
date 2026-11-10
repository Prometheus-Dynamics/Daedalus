//! Run against a real device: the machine's best adapter, and its software one when it has one.
//! With `DAEDALUS_GPU_REQUIRED=1`, as in the GPU lane of CI, having no adapter is a failure.

use std::os::fd::OwnedFd;
use std::time::Duration;

use ash::vk;

use crate::device::{Extras, Raw, test_device};
use crate::memory::{self, Placement};
use crate::semaphore::Semaphore;
use crate::submit::Submitter;
use crate::{Error, SubmitLock};

const WORDS: u64 = 256;
const BYTES: u64 = WORDS * 4;

const FILL: &str = "
struct Out { words: array<u32> }
@group(0) @binding(0) var<storage, read_write> out: Out;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    if id.x < arrayLength(&out.words) {
        out.words[id.x] = id.x * 3u + 1u;
    }
}
";

/// Every device these tests should run on.
fn devices() -> Vec<(wgpu::Device, wgpu::Queue)> {
    let mut found: Vec<(wgpu::Device, wgpu::Queue)> = Vec::new();
    for software in [false, true] {
        if let Some(pair) = test_device(software) {
            found.push(pair);
        }
    }
    assert!(
        !found.is_empty() || std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
        "this lane needs an adapter"
    );
    found
}

fn spirv() -> Vec<u32> {
    let module = naga::front::wgsl::parse_str(FILL).expect("the shader parses");
    let info = naga::valid::Validator::new(
        naga::valid::ValidationFlags::all(),
        naga::valid::Capabilities::empty(),
    )
    .validate(&module)
    .expect("the shader is valid");
    naga::back::spv::write_vec(&module, &info, &naga::back::spv::Options::default(), None)
        .expect("the shader compiles")
}

/// A compute pipeline with one storage buffer, made with raw calls and destroyed on drop.
struct Pipeline {
    device: ash::Device,
    module: vk::ShaderModule,
    set_layout: vk::DescriptorSetLayout,
    layout: vk::PipelineLayout,
    pipeline: vk::Pipeline,
    pool: vk::DescriptorPool,
    set: vk::DescriptorSet,
}

impl Pipeline {
    fn new(device: &ash::Device, buffer: vk::Buffer) -> Pipeline {
        let code = spirv();
        let bindings = [vk::DescriptorSetLayoutBinding::default()
            .binding(0)
            .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
            .descriptor_count(1)
            .stage_flags(vk::ShaderStageFlags::COMPUTE)];
        let sizes = [vk::DescriptorPoolSize::default()
            .ty(vk::DescriptorType::STORAGE_BUFFER)
            .descriptor_count(1)];
        // SAFETY: valid create infos on a live device. Each object is destroyed in `Drop`.
        unsafe {
            let module = device
                .create_shader_module(&vk::ShaderModuleCreateInfo::default().code(&code), None)
                .expect("a shader module");
            let set_layout = device
                .create_descriptor_set_layout(
                    &vk::DescriptorSetLayoutCreateInfo::default().bindings(&bindings),
                    None,
                )
                .expect("a set layout");
            let set_layouts = [set_layout];
            let layout = device
                .create_pipeline_layout(
                    &vk::PipelineLayoutCreateInfo::default().set_layouts(&set_layouts),
                    None,
                )
                .expect("a pipeline layout");
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
                .expect("a pipeline")[0];
            let pool = device
                .create_descriptor_pool(
                    &vk::DescriptorPoolCreateInfo::default()
                        .max_sets(1)
                        .pool_sizes(&sizes),
                    None,
                )
                .expect("a descriptor pool");
            let set = device
                .allocate_descriptor_sets(
                    &vk::DescriptorSetAllocateInfo::default()
                        .descriptor_pool(pool)
                        .set_layouts(&set_layouts),
                )
                .expect("a descriptor set")[0];
            let buffers = [vk::DescriptorBufferInfo::default()
                .buffer(buffer)
                .range(vk::WHOLE_SIZE)];
            device.update_descriptor_sets(
                &[vk::WriteDescriptorSet::default()
                    .dst_set(set)
                    .dst_binding(0)
                    .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                    .buffer_info(&buffers)],
                &[],
            );
            Pipeline {
                device: device.clone(),
                module,
                set_layout,
                layout,
                pipeline,
                pool,
                set,
            }
        }
    }

    /// Records one dispatch that fills the buffer.
    fn record(&self, device: &ash::Device, commands: vk::CommandBuffer) {
        // SAFETY: `commands` is recording on this device, and the pipeline, layout and set are
        // alive until the test has waited for the submit.
        unsafe {
            device.cmd_bind_pipeline(commands, vk::PipelineBindPoint::COMPUTE, self.pipeline);
            device.cmd_bind_descriptor_sets(
                commands,
                vk::PipelineBindPoint::COMPUTE,
                self.layout,
                0,
                &[self.set],
                &[],
            );
            device.cmd_dispatch(commands, (WORDS as u32).div_ceil(64), 1, 1);
        }
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        // SAFETY: the test waited for every submit that used these.
        unsafe {
            self.device.destroy_descriptor_pool(self.pool, None);
            self.device.destroy_pipeline(self.pipeline, None);
            self.device.destroy_pipeline_layout(self.layout, None);
            self.device
                .destroy_descriptor_set_layout(self.set_layout, None);
            self.device.destroy_shader_module(self.module, None);
        }
    }
}

/// Reads a buffer the way wgpu does: a copy to a staging buffer, then a map.
fn read_through_wgpu(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    lock: &SubmitLock,
    buffer: &wgpu::Buffer,
) -> Vec<u8> {
    let staging = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: BYTES,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
    encoder.copy_buffer_to_buffer(buffer, 0, &staging, 0, BYTES);
    {
        let _held = lock.lock();
        queue.submit([encoder.finish()]);
    }
    {
        let _held = lock.lock();
        staging
            .slice(..)
            .map_async(wgpu::MapMode::Read, |result| result.expect("the map"));
    }
    device
        .poll(wgpu::PollType::wait_indefinitely())
        .expect("the poll");
    let view = staging
        .slice(..)
        .get_mapped_range()
        .expect("a mapped range");
    view.to_vec()
}

fn expected() -> Vec<u8> {
    (0..WORDS as u32)
        .flat_map(|word| (word * 3 + 1).to_le_bytes())
        .collect()
}

#[test]
fn a_raw_spirv_dispatch_writes_a_buffer_that_wgpu_reads_back() {
    for (device, queue) in devices() {
        let lock = SubmitLock::default();
        for placement in [
            Placement::Device,
            Placement::HostMapped,
            Placement::HostCached,
        ] {
            let (buffer, mapping) =
                memory::storage_buffer(&device, BYTES, placement, None).expect("a buffer");
            assert_eq!(mapping.is_some(), placement != Placement::Device);
            let raw = Raw::of(&device).expect("a Vulkan device");
            let pipeline = Pipeline::new(&raw.device, memory::handle(&buffer).expect("a handle"));
            let submitter = Submitter::new(&device, &queue, &lock, 2).expect("a submitter");
            assert_eq!(submitter.slots(), 2);
            // More submits than slots: each reuse waits for the slot's last one.
            for _ in 0..5 {
                // SAFETY: the recording binds a pipeline and a set that outlive the wait below.
                let ticket = unsafe {
                    submitter.submit(|device, commands| pipeline.record(device, commands), &[])
                }
                .expect("a submit");
                assert!(ticket.wait(Duration::from_secs(30)), "{placement:?}");
                assert!(ticket.is_done());
            }
            assert_eq!(
                read_through_wgpu(&device, &queue, &lock, &buffer),
                expected(),
                "{placement:?}"
            );
            if let Some(mapping) = mapping {
                let mut seen = vec![0; BYTES as usize];
                mapping.read(0, &mut seen).expect("in range");
                assert_eq!(seen, expected(), "{placement:?} through the mapping");
            }
        }
    }
}

#[test]
fn host_mapped_memory_round_trips_and_checks_its_range() {
    for (device, queue) in devices() {
        let (buffer, mapping) =
            memory::storage_buffer(&device, BYTES, Placement::HostMapped, Some("mapped"))
                .expect("a buffer");
        let mapping = mapping.expect("a mapping");
        assert_eq!(mapping.len(), BYTES as usize);
        assert!(!mapping.is_empty());
        mapping.write(0, &expected()).expect("in range");
        mapping.write(8, &[9, 9, 9, 9]).expect("in range");
        let mut seen = [0; 8];
        mapping.read(4, &mut seen).expect("in range");
        assert_eq!(seen, [4, 0, 0, 0, 9, 9, 9, 9]);
        // The device sees what the host wrote, with no upload.
        let through = read_through_wgpu(&device, &queue, &SubmitLock::default(), &buffer);
        assert_eq!(through[8..12], [9, 9, 9, 9]);
        assert_eq!(through[..8], expected()[..8]);

        assert_eq!(mapping.write(BYTES - 2, &[0; 4]), Err(Error::Range));
        assert_eq!(mapping.read(u64::MAX, &mut seen), Err(Error::Range));
        for bad in [0, 6] {
            let refused = memory::storage_buffer(&device, bad, Placement::Device, None);
            assert_eq!(refused.err(), Some(Error::Range));
        }
    }
}

#[test]
fn an_fd_that_is_no_dma_buf_is_refused_with_a_typed_error() {
    for (device, _queue) in devices() {
        let extras = Extras::of(&device).expect("a Vulkan device");
        let not_a_dma_buf = || OwnedFd::from(std::fs::File::open("/dev/zero").expect("/dev/zero"));
        let imported = memory::import_dma_buf(&device, not_a_dma_buf(), BYTES, None);
        if extras.dma_buf_import {
            // The device can import, and this fd isn't one it can.
            assert!(imported.is_err(), "/dev/zero imported as a dma-buf");
        } else {
            // lavapipe is here.
            assert_eq!(imported.err(), Some(Error::NoDmaBufImport));
        }
        let refused = memory::import_dma_buf(&device, not_a_dma_buf(), 6, None);
        assert_eq!(refused.err(), Some(Error::Range));
    }
}

#[test]
fn a_signaled_fence_imports_where_the_device_can_and_a_submit_waits_on_it() {
    for (device, queue) in devices() {
        let extras = Extras::of(&device).expect("a Vulkan device");
        let imported = Semaphore::import_sync_fd(&device, None);
        if !extras.sync_fd_import {
            assert_eq!(imported.err(), Some(Error::NoSyncFdImport));
            continue;
        }
        let semaphore = imported.expect("an already signaled fence");
        let submitter =
            Submitter::new(&device, &queue, &SubmitLock::default(), 1).expect("a submitter");
        // SAFETY: nothing is recorded, and the semaphore outlives the wait below.
        let ticket = unsafe { submitter.submit(|_, _| {}, &[&semaphore]) }.expect("a submit");
        assert!(ticket.wait(Duration::from_secs(30)));
        let too_many = [&semaphore; crate::submit::MAX_WAITS + 1];
        // SAFETY: refused before anything is recorded.
        let refused = unsafe { submitter.submit(|_, _| {}, &too_many) };
        assert_eq!(refused.err(), Some(Error::Range));
    }
}

#[test]
fn a_device_reports_what_it_was_opened_with() {
    for (device, _queue) in devices() {
        let extras = Extras::of(&device).expect("a Vulkan device");
        assert!(extras.timestamp_period_ns >= 0.0);
        assert!(extras.timestamp_valid_bits <= 64);
        if extras.eight_bit.uniform_and_storage_buffer {
            assert!(extras.eight_bit.storage_buffer);
        }
    }
}
