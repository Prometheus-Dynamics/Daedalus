//! `GpuContextHandle` against a real wgpu device (skipped without one).

use crate::{GpuBackendKind, GpuOptions, select_backend};

/// A node-owned pipeline built and dispatched on the engine's own device through
/// `GpuContextHandle::wgpu()`; `None` on a backend that is not wgpu.
#[test]
fn wgpu_accessor_dispatches_on_the_engine_device() {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    let Some((device, queue)) = ctx.wgpu() else {
        assert_ne!(ctx.backend_kind(), GpuBackendKind::Wgpu);
        return;
    };
    assert_eq!(ctx.backend_kind(), GpuBackendKind::Wgpu);
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: None,
        source: wgpu::ShaderSource::Wgsl(
            "@group(0) @binding(0) var<storage, read_write> out: array<u32>;
             @compute @workgroup_size(1) fn main() { out[0] = 42u; }"
                .into(),
        ),
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: None,
        layout: None,
        module: &module,
        entry_point: Some("main"),
        compilation_options: Default::default(),
        cache: None,
    });
    let buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: 4,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });
    let readback = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: 4,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[wgpu::BindGroupEntry {
            binding: 0,
            resource: buffer.as_entire_binding(),
        }],
    });
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_compute_pass(&Default::default());
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        pass.dispatch_workgroups(1, 1, 1);
    }
    encoder.copy_buffer_to_buffer(&buffer, 0, &readback, 0, 4);
    queue.submit([encoder.finish()]);
    let slice = readback.slice(..);
    slice.map_async(wgpu::MapMode::Read, |_| {});
    let _ = device.poll(wgpu::PollType::Wait {
        submission_index: None,
        timeout: None,
    });
    let value = u32::from_le_bytes(
        slice.get_mapped_range().expect("mapped")[..4]
            .try_into()
            .unwrap(),
    );
    assert_eq!(value, 42);
}
