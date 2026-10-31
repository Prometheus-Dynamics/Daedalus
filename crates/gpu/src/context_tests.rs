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

/// The wgpu backend reports the adapter's compute limits and the optional features it enabled.
#[test]
fn wgpu_reports_compute_limits_and_enabled_features() {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    let Some((device, _)) = ctx.wgpu() else {
        return;
    };
    let compute = ctx.capabilities().compute;
    let limits = device.limits();
    assert_eq!(
        compute.max_invocations_per_workgroup,
        limits.max_compute_invocations_per_workgroup
    );
    assert_eq!(
        compute.max_workgroup_storage_size,
        limits.max_compute_workgroup_storage_size
    );
    // Every conforming device meets the WebGPU defaults.
    let defaults = crate::GpuComputeCaps::WEBGPU_DEFAULTS;
    assert!(compute.max_invocations_per_workgroup >= defaults.max_invocations_per_workgroup);
    assert!(compute.fits_workgroup([8, 8, 1]));
    let enabled = device.features();
    assert_eq!(
        compute.features.shader_f16,
        enabled.contains(wgpu::Features::SHADER_F16)
    );
    assert_eq!(
        compute.features.timestamps_inside_passes,
        enabled.contains(wgpu::Features::TIMESTAMP_QUERY_INSIDE_PASSES)
    );
}

/// `GpuStorage` checks sizes and write ranges, grows on demand, and needs a wgpu backend.
#[test]
fn gpu_storage_validates_and_grows() {
    let noop = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Noop),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        crate::GpuStorage::new(&noop, 16, None).unwrap_err(),
        crate::GpuError::Unsupported
    );
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    if ctx.wgpu().is_none() {
        return;
    }
    for bad in [0, 6] {
        assert!(matches!(
            crate::GpuStorage::new(&ctx, bad, None),
            Err(crate::GpuError::InvalidInput(_))
        ));
    }
    let mut storage = crate::GpuStorage::with_data(&ctx, &[1, 2, 3, 4, 5], Some("s")).unwrap();
    assert_eq!(storage.size(), 8);
    assert!(storage.write(4, &[0; 4]).is_ok());
    assert!(storage.write(6, &[0; 4]).is_err(), "unaligned offset");
    assert!(storage.write(8, &[0; 4]).is_err(), "past the end");
    assert!(!storage.ensure_size(8).unwrap());
    assert!(storage.ensure_size(30).unwrap());
    assert_eq!(storage.size(), 32);
    assert_eq!(
        storage.binding(0, crate::shader::Access::ReadWrite).binding,
        0
    );
}

/// A ticket completes through non-blocking polls alone; `GpuInFlight` caps and orders tickets.
#[test]
fn tickets_complete_without_waiting_and_in_flight_is_bounded() {
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .unwrap();
    let Some((device, _)) = ctx.wgpu() else {
        assert!(matches!(
            crate::GpuTicket::submit(&ctx, []),
            Err(crate::GpuError::Unsupported)
        ));
        return;
    };
    let encode = || device.create_command_encoder(&Default::default()).finish();
    let poll_until = |ticket: &crate::GpuTicket| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while ticket.try_complete().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "ticket never completed"
            );
            std::thread::yield_now();
        }
    };
    let ticket = crate::GpuTicket::submit(&ctx, [encode()]).unwrap();
    poll_until(&ticket);
    assert!(ticket.is_complete());

    let mut in_flight = crate::GpuInFlight::new(2);
    for value in [1, 2] {
        let ticket = crate::GpuTicket::submit(&ctx, [encode()]).unwrap();
        in_flight.push(ticket, value).unwrap();
    }
    assert!(in_flight.is_full());
    let extra = crate::GpuTicket::submit(&ctx, [encode()]).unwrap();
    let (extra, value) = in_flight.push(extra, 3).unwrap_err();
    assert_eq!(value, 3);
    extra.wait();
    let mut drained = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while drained.len() < 2 {
        assert!(std::time::Instant::now() < deadline);
        if let Some(value) = in_flight.pop_completed() {
            drained.push(value);
        }
    }
    assert_eq!(
        drained,
        [1, 2],
        "completed values come out in submission order"
    );
    assert!(in_flight.is_empty());
}
