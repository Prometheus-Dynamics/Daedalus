//! The `gpu_bench` report: shared helpers and the section order.

mod compute;
mod frame_import;
mod limits;

use std::fmt::Display;
use std::time::{Duration, Instant};

use daedalus_gpu::wgpu;
use daedalus_gpu::{GpuBackend, WgpuBackend};

fn heading(title: &str) {
    println!("\n[{title}]");
}

fn kv(key: &str, value: impl Display) {
    println!("{key}: {value}");
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn iters() -> usize {
    std::env::var("GPU_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(100)
}

/// p50/p90/p99/min of `samples`, in microseconds.
fn stats(samples: &mut [Duration]) -> String {
    if samples.is_empty() {
        return "no samples".into();
    }
    samples.sort();
    let at = |q: f64| {
        let i = ((samples.len() - 1) as f64 * q).round() as usize;
        samples[i].as_secs_f64() * 1e6
    };
    format!(
        "p50 {:.1} us, p90 {:.1} us, p99 {:.1} us, min {:.1} us (n={})",
        at(0.5),
        at(0.9),
        at(0.99),
        samples[0].as_secs_f64() * 1e6,
        samples.len()
    )
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn wait(device: &wgpu::Device) {
    let _ = device.poll(wgpu::PollType::Wait {
        submission_index: None,
        timeout: None,
    });
}

pub fn run() {
    println!("daedalus-gpu gpu_bench {}", env!("CARGO_PKG_VERSION"));
    let backend = match WgpuBackend::new() {
        Ok(backend) => backend,
        Err(err) => return kv("adapter", format!("unavailable: {err}")),
    };
    let Some((device, queue)) = backend.wgpu_device_queue() else {
        return kv("adapter", "unavailable: backend exposes no wgpu device");
    };
    let info = device.adapter_info();
    kv("adapter", format!("{} ({:?})", info.name, info.backend));
    kv("driver", format!("{} {}", info.driver, info.driver_info));
    heading("limits");
    limits::limits(device);
    heading("dispatch-latency");
    compute::dispatch_latency(device, queue);
    heading("throughput");
    compute::throughput(device, queue);
    heading("frame-import");
    frame_import::frame_import(&backend, device, queue);
}

fn pipeline(device: &wgpu::Device, wgsl: &str) -> wgpu::ComputePipeline {
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("gpu_bench"),
        source: wgpu::ShaderSource::Wgsl(wgsl.into()),
    });
    device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("gpu_bench"),
        layout: None,
        module: &module,
        entry_point: Some("main"),
        compilation_options: wgpu::PipelineCompilationOptions::default(),
        cache: None,
    })
}

fn bind(
    device: &wgpu::Device,
    pipeline: &wgpu::ComputePipeline,
    buffers: &[&wgpu::Buffer],
) -> wgpu::BindGroup {
    let entries: Vec<wgpu::BindGroupEntry<'_>> = buffers
        .iter()
        .enumerate()
        .map(|(i, b)| wgpu::BindGroupEntry {
            binding: i as u32,
            resource: b.as_entire_binding(),
        })
        .collect();
    device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &entries,
    })
}

fn storage(device: &wgpu::Device, bytes: u64) -> wgpu::Buffer {
    device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: bytes,
        usage: wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_SRC
            | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    })
}

/// Record one dispatch of `groups` workgroups, submit it and wait for the fence.
fn run_once(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    pipeline: &wgpu::ComputePipeline,
    bind_group: &wgpu::BindGroup,
    groups: (u32, u32),
) -> Duration {
    let start = Instant::now();
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_compute_pass(&Default::default());
        pass.set_pipeline(pipeline);
        pass.set_bind_group(0, bind_group, &[]);
        pass.dispatch_workgroups(groups.0, groups.1, 1);
    }
    queue.submit([encoder.finish()]);
    wait(device);
    start.elapsed()
}
