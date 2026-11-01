//! What a repeated GPU kernel dispatch allocates, against the same dispatch written directly in
//! wgpu: after warm-up, `GpuKernels::encode` adds nothing (its bind groups and parameter buffers
//! are cached), and `run` adds only its `GpuTicket` (the completion flag and wgpu's callback box).
//! wgpu itself allocates per encoder and submission; that floor is the baseline here.
#![allow(clippy::print_stderr)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use daedalus_gpu::{
    GpuBackendKind, GpuKernel, GpuKernels, GpuOptions, GpuStorage, select_backend, wgpu,
};

struct Counting;
static ARMED: AtomicBool = AtomicBool::new(false);
static COUNT: AtomicUsize = AtomicUsize::new(0);
/// `GPU_ALLOC_TRACE=1`: print a backtrace per allocation of one `encode` call.
static TRACING: AtomicBool = AtomicBool::new(false);
thread_local! {
    static IN_TRACE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// SAFETY: forwards to `System`, counting allocations while armed.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            COUNT.fetch_add(1, Ordering::Relaxed);
            if TRACING.load(Ordering::Relaxed) && !IN_TRACE.with(|t| t.replace(true)) {
                let trace = std::backtrace::Backtrace::force_capture().to_string();
                let frames: Vec<&str> = trace
                    .lines()
                    .filter(|l| l.contains("daedalus") || l.contains("wgpu"))
                    .take(3)
                    .collect();
                eprintln!("alloc {}:\n{}", layout.size(), frames.join("\n"));
                IN_TRACE.with(|t| t.set(false));
            }
        }
        // SAFETY: as `System::alloc`.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: as `System::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Allocations per call of `f`, the median over a few rounds after a warm-up.
fn per_call(mut f: impl FnMut()) -> usize {
    for _ in 0..20 {
        f();
    }
    let mut counts: Vec<usize> = (0..15)
        .map(|_| {
            COUNT.store(0, Ordering::SeqCst);
            ARMED.store(true, Ordering::SeqCst);
            f();
            ARMED.store(false, Ordering::SeqCst);
            COUNT.load(Ordering::SeqCst)
        })
        .collect();
    counts.sort_unstable();
    counts[counts.len() / 2]
}

#[test]
fn a_kernel_dispatch_allocates_no_more_than_raw_wgpu() {
    let Ok(ctx) = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) else {
        return eprintln!("skipping: no GPU");
    };
    let Some((device, queue)) = ctx.wgpu() else {
        return eprintln!("skipping: no wgpu device");
    };
    let n = 1 << 16;
    let src = GpuStorage::with_data(&ctx, &vec![7u8; n as usize], None).unwrap();
    let dst = GpuStorage::new(&ctx, n, None).unwrap();
    // The kernels bind five buffers (source, destination, parameters, status, aux); so does the
    // baseline.
    let params = GpuStorage::with_data(&ctx, &((n / 4) as u32).to_le_bytes(), None).unwrap();
    let status = GpuStorage::new(&ctx, 8, None).unwrap();
    let aux = GpuStorage::new(&ctx, 1024, None).unwrap();
    let kernels = GpuKernels::new(&ctx).unwrap();

    // The same copy written directly: one pipeline and bind group, made once.
    let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: None,
        source: wgpu::ShaderSource::Wgsl(
            "@group(0) @binding(0) var<storage, read> s: array<u32>;
             @group(0) @binding(1) var<storage, read_write> d: array<u32>;
             @group(0) @binding(2) var<storage, read> p: array<u32>;
             @group(0) @binding(3) var<storage, read_write> st: array<u32>;
             @group(0) @binding(4) var<storage, read> a: array<u32>;
             @compute @workgroup_size(64) fn main(@builtin(global_invocation_id) g: vec3<u32>) {
                 if (g.x < p[0] + st[0] * 0u + a[0] * 0u) { d[g.x] = s[g.x]; }
             }"
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
    let bind = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: src.buffer().as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: dst.buffer().as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: params.buffer().as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: status.buffer().as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 4,
                resource: aux.buffer().as_entire_binding(),
            },
        ],
    });
    let wait = || {
        let _ = device.poll(wgpu::PollType::Wait {
            submission_index: None,
            timeout: None,
        });
    };
    let raw = per_call(|| {
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups((n / 4).div_ceil(64) as u32, 1, 1);
        }
        queue.submit([encoder.finish()]);
        wait();
    });
    let encoded = per_call(|| {
        let mut encoder = device.create_command_encoder(&Default::default());
        kernels
            .encode(&mut encoder, GpuKernel::Copy, &src, &dst, n, None)
            .unwrap();
        queue.submit([encoder.finish()]);
        wait();
    });
    if std::env::var_os("GPU_ALLOC_TRACE").is_some() {
        TRACING.store(true, Ordering::SeqCst);
        ARMED.store(true, Ordering::SeqCst);
        let mut encoder = device.create_command_encoder(&Default::default());
        kernels
            .encode(&mut encoder, GpuKernel::Copy, &src, &dst, n, None)
            .unwrap();
        queue.submit([encoder.finish()]);
        wait();
        ARMED.store(false, Ordering::SeqCst);
        eprintln!("---- raw");
        ARMED.store(true, Ordering::SeqCst);
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups((n / 4).div_ceil(64) as u32, 1, 1);
        }
        queue.submit([encoder.finish()]);
        wait();
        ARMED.store(false, Ordering::SeqCst);
        TRACING.store(false, Ordering::SeqCst);
    }
    let run = per_call(|| kernels.run(GpuKernel::Copy, &src, &dst, n).unwrap().wait());
    // wgpu's own bookkeeping varies a little between runs: measure the raw path again last and
    // take the larger.
    let raw_again = per_call(|| {
        let mut encoder = device.create_command_encoder(&Default::default());
        {
            let mut pass = encoder.begin_compute_pass(&Default::default());
            pass.set_pipeline(&pipeline);
            pass.set_bind_group(0, &bind, &[]);
            pass.dispatch_workgroups((n / 4).div_ceil(64) as u32, 1, 1);
        }
        queue.submit([encoder.finish()]);
        wait();
    });
    eprintln!(
        "allocations per dispatch: raw wgpu {raw} then {raw_again}, GpuKernels::encode {encoded}, run {run}"
    );
    let raw = raw.max(raw_again);
    assert!(encoded <= raw, "encode adds allocations: {encoded} > {raw}");
    assert!(
        run <= raw + 2,
        "run adds more than its ticket: {run} > {raw} + 2"
    );
}
