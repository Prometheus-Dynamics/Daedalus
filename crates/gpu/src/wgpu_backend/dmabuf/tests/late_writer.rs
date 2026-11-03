//! A producer on another device whose GPU job finishes late, handing out a `sync_file` of its
//! completion: the pending acquire fence of the fence tests.

use std::os::fd::{FromRawFd, OwnedFd};
use std::time::Duration;

use ash::{khr, vk};
use wgpu::hal::api::Vulkan;

use super::support::device_poll;
use crate::{GpuImageHandle, WgpuBackend};

const SYNC_FD: vk::ExternalSemaphoreHandleTypeFlags = vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD;

/// Writes `pixel(x, y)` into a storage texture after a GPU spin of about the requested duration,
/// handing out a `sync_file` that signals once the write is done.
pub(super) struct LateWriter<'a> {
    backend: &'a WgpuBackend,
    pipeline: wgpu::ComputePipeline,
    bind_group: wgpu::BindGroup,
    spins: wgpu::Buffer,
    pub(super) iterations: std::cell::Cell<u32>,
}

impl<'a> LateWriter<'a> {
    /// Writes into `target`, a storage-capable image registered with `backend`.
    pub(super) fn new(
        backend: &'a WgpuBackend,
        target: &GpuImageHandle,
        duration: Duration,
    ) -> Self {
        let texture = backend.get_texture(target).expect("registered texture");
        Self::with_view(backend, &texture.create_view(&Default::default()), duration)
    }

    /// Writes into a 64x32 texture of its own: for tests that need only the late `sync_file`.
    pub(super) fn on_own_texture(backend: &'a WgpuBackend, duration: Duration) -> Self {
        let texture = backend
            .device_queue()
            .0
            .create_texture(&wgpu::TextureDescriptor {
                label: Some("late-writer-target"),
                size: wgpu::Extent3d {
                    width: 64,
                    height: 32,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Rgba8Unorm,
                usage: wgpu::TextureUsages::STORAGE_BINDING,
                view_formats: &[],
            });
        Self::with_view(backend, &texture.create_view(&Default::default()), duration)
    }

    fn with_view(backend: &'a WgpuBackend, view: &wgpu::TextureView, duration: Duration) -> Self {
        let device = backend.device_queue().0;
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("late-writer"),
            source: wgpu::ShaderSource::Wgsl(
                r#"
@group(0) @binding(0) var dst: texture_storage_2d<rgba8unorm, write>;
@group(0) @binding(1) var<uniform> spins: u32;
@compute @workgroup_size(1)
fn main() {
var acc = 1u;
for (var i = 0u; i < spins; i++) { acc = acc * 1664525u + 1013904223u; }
let dims = textureDimensions(dst);
for (var y = 0u; y < dims.y; y++) {
    for (var x = 0u; x < dims.x; x++) {
        var c = vec4<f32>(vec3<f32>(f32(x), f32(y), f32(x ^ y)) / 255.0, 1.0);
        if (acc == 7u) { c = vec4<f32>(0.0); }
        textureStore(dst, vec2<i32>(i32(x), i32(y)), c);
    }
}
}
"#
                .into(),
            ),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("late-writer"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        let spins = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("late-writer-spins"),
            size: 16,
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("late-writer"),
            layout: &pipeline.get_bind_group_layout(0),
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: spins.as_entire_binding(),
                },
            ],
        });
        let writer = Self {
            backend,
            pipeline,
            bind_group,
            spins,
            iterations: std::cell::Cell::new(1 << 20),
        };
        // Calibrate the spin to about `duration` on this GPU: the fastest of three runs, since
        // other work on the GPU only makes a run slower (and the job then too short).
        let fastest = (0..3)
            .map(|_| {
                let start = std::time::Instant::now();
                writer.dispatch();
                device_poll(backend);
                start.elapsed()
            })
            .min()
            .unwrap();
        let per_spin = fastest.as_secs_f64() / f64::from(writer.iterations.get());
        writer
            .iterations
            .set((duration.as_secs_f64() / per_spin).clamp(1e6, 2e9) as u32);
        writer
    }

    /// Double the job, for a calibration that came out short.
    pub(super) fn lengthen(&self) {
        let longer = self.iterations.get().saturating_mul(2).min(2_000_000_000);
        self.iterations.set(longer);
    }

    fn dispatch(&self) {
        let (device, queue) = self.backend.device_queue();
        queue
            .raw()
            .write_buffer(&self.spins, 0, &self.iterations.get().to_le_bytes());
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &self.bind_group, &[]);
            pass.dispatch_workgroups(1, 1, 1);
        }
        queue.submit(Some(encoder.finish()));
    }

    /// Submit the late write and export a `sync_file` of its completion.
    pub(super) fn write(&self) -> OwnedFd {
        let (device, queue) = self.backend.device_queue();
        // SAFETY: the semaphore is created, signaled by the next submission, exported, and
        // destroyed once that submission completed.
        unsafe {
            let hal_dev = device.as_hal::<Vulkan>().expect("Vulkan device");
            let raw = hal_dev.raw_device().clone();
            let mut export = vk::ExportSemaphoreCreateInfo::default().handle_types(SYNC_FD);
            let semaphore = raw
                .create_semaphore(
                    &vk::SemaphoreCreateInfo::default().push_next(&mut export),
                    None,
                )
                .expect("exportable semaphore");
            queue
                .raw()
                .as_hal::<Vulkan>()
                .expect("Vulkan queue")
                .add_signal_semaphore(semaphore, None);
            self.dispatch();
            let fd_api = khr::external_semaphore_fd::Device::new(
                hal_dev.shared_instance().raw_instance(),
                &raw,
            );
            let info = vk::SemaphoreGetFdInfoKHR::default()
                .semaphore(semaphore)
                .handle_type(SYNC_FD);
            let fd = fd_api.get_semaphore_fd(&info).expect("export sync_file");
            queue.on_submitted_work_done(move || raw.destroy_semaphore(semaphore, None));
            OwnedFd::from_raw_fd(fd)
        }
    }
}
