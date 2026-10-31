//! Dispatch latency and kernel throughput, against the CPU.

use std::time::Instant;

use daedalus_gpu::wgpu::{self, util::DeviceExt};

use super::{bind, iters, kv, median, pipeline, run_once, stats, storage, wait};
/// Record, submit and wait for a one-workgroup dispatch that writes one word: the floor of
/// every GPU node's round trip.
pub(super) fn dispatch_latency(device: &wgpu::Device, queue: &wgpu::Queue) {
    let pipeline = pipeline(
        device,
        "@group(0) @binding(0) var<storage, read_write> out: array<u32>;
         @compute @workgroup_size(1) fn main() { out[0] = out[0] + 1u; }",
    );
    let buffer = storage(device, 256);
    let bind_group = bind(device, &pipeline, &[&buffer]);
    for _ in 0..10 {
        run_once(device, queue, &pipeline, &bind_group, (1, 1));
    }
    let mut samples: Vec<_> = (0..iters())
        .map(|_| run_once(device, queue, &pipeline, &bind_group, (1, 1)))
        .collect();
    kv("empty_dispatch_submit_to_fence", stats(&mut samples));
    // Submit only (no wait): what a tick pays when it never waits.
    let mut submit = Vec::new();
    for _ in 0..iters() {
        let start = Instant::now();
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(1, 1, 1);
        }
        queue.submit([encoder.finish()]);
        submit.push(start.elapsed());
        wait(device);
    }
    kv("empty_dispatch_record_and_submit", stats(&mut submit));
    // Non-blocking completion check after the work is done: the cost of a poll per tick.
    let mut polls = Vec::new();
    for _ in 0..iters() {
        let start = Instant::now();
        let _ = device.poll(wgpu::PollType::Poll);
        polls.push(start.elapsed());
    }
    kv("idle_poll", stats(&mut polls));
}

const SAXPY: &str = "
    @group(0) @binding(0) var<storage, read> x: array<f32>;
    @group(0) @binding(1) var<storage, read_write> y: array<f32>;
    @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) id: vec3<u32>,
                                         @builtin(num_workgroups) n: vec3<u32>) {
        let i = id.x + id.y * n.x * 64u;
        if i < arrayLength(&y) { y[i] = 2.5 * x[i] + y[i]; }
    }";

/// 256 dependent FMAs per element (512 flops), 4 independent chains for ILP.
const FMA: &str = "
    @group(0) @binding(0) var<storage, read> x: array<f32>;
    @group(0) @binding(1) var<storage, read_write> y: array<f32>;
    @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) id: vec3<u32>,
                                         @builtin(num_workgroups) n: vec3<u32>) {
        let i = id.x + id.y * n.x * 64u;
        if i >= arrayLength(&y) { return; }
        var a = x[i]; var b = a + 1.0; var c = a + 2.0; var d = a + 3.0;
        for (var k = 0u; k < 64u; k++) {
            a = a * 0.999 + 0.001; b = b * 0.999 + 0.001;
            c = c * 0.999 + 0.001; d = d * 0.999 + 0.001;
        }
        y[i] = a + b + c + d;
    }";

/// Workgroup counts covering `elements` with 64-wide groups, split into 2D when the count
/// exceeds the per-dimension limit.
fn groups_for(device: &wgpu::Device, elements: u32) -> (u32, u32) {
    let groups = elements.div_ceil(64);
    let max = device.limits().max_compute_workgroups_per_dimension;
    if groups <= max {
        (groups, 1)
    } else {
        (max, groups.div_ceil(max))
    }
}

pub(super) fn throughput(device: &wgpu::Device, queue: &wgpu::Queue) {
    let elements: u32 = 4 << 20; // 16 MiB per f32 buffer
    let bytes = u64::from(elements) * 4;
    let input: Vec<f32> = (0..elements).map(|i| (i % 1024) as f32).collect();
    let x = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("x"),
        contents: bytemuck::cast_slice(&input),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let y = storage(device, bytes);
    let groups = groups_for(device, elements);
    let runs = iters().min(30);
    for (name, wgsl, flops_per, bytes_per) in
        [("saxpy", SAXPY, 2.0, 12.0), ("fma256", FMA, 512.0, 8.0)]
    {
        let pipeline = pipeline(device, wgsl);
        let bind_group = bind(device, &pipeline, &[&x, &y]);
        run_once(device, queue, &pipeline, &bind_group, groups);
        let mut samples: Vec<_> = (0..runs)
            .map(|_| run_once(device, queue, &pipeline, &bind_group, groups))
            .collect();
        let t = median(&mut samples).as_secs_f64();
        let n = f64::from(elements);
        kv(
            &format!("gpu_{name}"),
            format!(
                "{:.2} ms for {}M elements: {:.2} GB/s, {:.2} GFLOP/s",
                t * 1e3,
                elements >> 20,
                n * bytes_per / t / 1e9,
                n * flops_per / t / 1e9
            ),
        );
    }
    cpu_baseline(&input);
}

fn cpu_saxpy(x: &[f32], y: &mut [f32]) {
    for (y, x) in y.iter_mut().zip(x) {
        *y += 2.5 * *x;
    }
}

fn cpu_fma(x: &[f32], y: &mut [f32]) {
    for (y, x) in y.iter_mut().zip(x) {
        let (mut a, mut b, mut c, mut d) = (*x, *x + 1.0, *x + 2.0, *x + 3.0);
        for _ in 0..64 {
            a = a * 0.999 + 0.001;
            b = b * 0.999 + 0.001;
            c = c * 0.999 + 0.001;
            d = d * 0.999 + 0.001;
        }
        *y = a + b + c + d;
    }
}

/// The same kernels on the CPU, on 1 thread and on every core, as the fallback's budget.
fn cpu_baseline(input: &[f32]) {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut y = vec![0.0f32; input.len()];
    let runs = 5;
    for (name, kernel, flops_per, bytes_per) in [
        ("saxpy", cpu_saxpy as fn(&[f32], &mut [f32]), 2.0, 12.0),
        ("fma256", cpu_fma, 512.0, 8.0),
    ] {
        for t in [1, threads] {
            let mut samples = Vec::new();
            for _ in 0..runs {
                let start = Instant::now();
                let chunk = input.len().div_ceil(t);
                std::thread::scope(|s| {
                    for (x, y) in input.chunks(chunk).zip(y.chunks_mut(chunk)) {
                        s.spawn(move || kernel(x, y));
                    }
                });
                samples.push(start.elapsed());
            }
            let secs = median(&mut samples).as_secs_f64();
            let n = input.len() as f64;
            kv(
                &format!("cpu_{name}_{t}t"),
                format!(
                    "{:.2} ms: {:.2} GB/s, {:.2} GFLOP/s",
                    secs * 1e3,
                    n * bytes_per / secs / 1e9,
                    n * flops_per / secs / 1e9
                ),
            );
        }
    }
    std::hint::black_box(&y);
}
