use std::io::{self, Write};
use std::time::{Duration, Instant};

use daedalus_gpu::shader::{
    Access, BindingData, BindingKind, BindingSpec, BufferInit, ShaderBinding, ShaderSpec,
    dispatch_shader_with_bindings, dispatch_shader_with_bindings_async,
    set_async_poll_overflow_thread_limit, set_async_poll_worker_limit,
};
use daedalus_gpu::{GpuBackendKind, GpuError, GpuOptions, select_backend};

const WARMUP_ITERS: usize = 10;
const SYNC_ITERS: usize = 1_000;
const ASYNC_ITERS: usize = 200;

static BINDINGS: &[BindingSpec] = &[
    BindingSpec {
        binding: 0,
        kind: BindingKind::Storage,
        access: Access::ReadOnly,
        invocation_stride: Some(4),
        texture_format: None,
        sample_type: None,
        view_dimension: None,
        sampler_kind: None,
    },
    BindingSpec {
        binding: 1,
        kind: BindingKind::Storage,
        access: Access::WriteOnly,
        invocation_stride: Some(4),
        texture_format: None,
        sample_type: None,
        view_dimension: None,
        sampler_kind: None,
    },
];

static SPEC: ShaderSpec = ShaderSpec {
    name: "bench-dispatch-readback",
    src: r#"
        @group(0) @binding(0)
        var<storage, read> input: array<u32>;

        @group(0) @binding(1)
        var<storage, read_write> output: array<u32>;

        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            output[id.x] = input[id.x] + 1u;
        }
    "#,
    entry: "main",
    workgroup_size: Some([64, 1, 1]),
    bindings: BINDINGS,
};

fn dispatch_readback_once(
    gpu: &daedalus_gpu::GpuContextHandle,
    input: &[u32],
) -> Result<u32, GpuError> {
    let output_len = std::mem::size_of_val(input) as u64;
    let bindings = [
        ShaderBinding {
            binding: 0,
            kind: BindingKind::Storage,
            access: Access::ReadOnly,
            data: BindingData::Buffer(BufferInit::Bytes(bytemuck::cast_slice(input))),
            readback: false,
        },
        ShaderBinding {
            binding: 1,
            kind: BindingKind::Storage,
            access: Access::WriteOnly,
            data: BindingData::Buffer(BufferInit::Empty(output_len)),
            readback: true,
        },
    ];
    let output = dispatch_shader_with_bindings(
        &SPEC,
        SPEC.src,
        &bindings,
        Some(gpu),
        None,
        Some([input.len() as u32, 1, 1]),
    )?;
    let bytes = output
        .buffers
        .get(&1)
        .ok_or_else(|| GpuError::Internal("missing output binding 1".into()))?;
    let values: &[u32] = bytemuck::cast_slice(bytes);
    values
        .last()
        .copied()
        .ok_or_else(|| GpuError::Internal("empty readback".into()))
}

async fn dispatch_readback_async_once(
    gpu: &daedalus_gpu::GpuContextHandle,
    input: &[u32],
) -> Result<u32, GpuError> {
    let output_len = std::mem::size_of_val(input) as u64;
    let bindings = [
        ShaderBinding {
            binding: 0,
            kind: BindingKind::Storage,
            access: Access::ReadOnly,
            data: BindingData::Buffer(BufferInit::Bytes(bytemuck::cast_slice(input))),
            readback: false,
        },
        ShaderBinding {
            binding: 1,
            kind: BindingKind::Storage,
            access: Access::WriteOnly,
            data: BindingData::Buffer(BufferInit::Empty(output_len)),
            readback: true,
        },
    ];
    let output = dispatch_shader_with_bindings_async(
        &SPEC,
        SPEC.src,
        &bindings,
        Some(gpu),
        None,
        Some([input.len() as u32, 1, 1]),
    )
    .await?;
    let bytes = output
        .buffers
        .get(&1)
        .ok_or_else(|| GpuError::Internal("missing output binding 1".into()))?;
    let values: &[u32] = bytemuck::cast_slice(bytes);
    values
        .last()
        .copied()
        .ok_or_else(|| GpuError::Internal("empty readback".into()))
}

fn measure(
    name: &str,
    iterations: usize,
    mut run: impl FnMut() -> Result<u32, GpuError>,
) -> Result<(), GpuError> {
    for _ in 0..WARMUP_ITERS {
        std::hint::black_box(run()?);
    }

    let started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(run()?);
    }
    let elapsed = started.elapsed();
    print_measurement(name, iterations, elapsed);
    Ok(())
}

fn print_measurement(name: &str, iterations: usize, elapsed: Duration) {
    let nanos = elapsed.as_nanos() / iterations as u128;
    let micros = nanos as f64 / 1_000.0;
    let mut stdout = io::stdout().lock();
    writeln!(
        stdout,
        "{name}: {iterations} iterations in {elapsed:?}; mean {micros:.3} us/iter"
    )
    .expect("write gpu benchmark measurement");
}

fn gpu_dispatch_readback() -> Result<(), GpuError> {
    let Ok(gpu) = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "gpu_dispatch_readback: no wgpu backend available; skipping benchmark"
        )
        .expect("write gpu benchmark skip reason");
        return Ok(());
    };

    for len in [256usize, 4096] {
        let input = (0..len as u32).collect::<Vec<_>>();
        measure(
            &format!("gpu_dispatch_readback/storage_buffer_u32_plus_one/{len}"),
            SYNC_ITERS,
            || dispatch_readback_once(&gpu, std::hint::black_box(&input)),
        )?;
    }

    set_async_poll_worker_limit(1);
    set_async_poll_overflow_thread_limit(1);
    for (len, concurrent) in [(256usize, 4usize), (4096, 8)] {
        let input = (0..len as u32).collect::<Vec<_>>();
        measure(
            &format!("gpu_dispatch_readback/async_concurrent_storage_buffer_u32_plus_one/{len}"),
            ASYNC_ITERS,
            || {
                std::thread::scope(|scope| {
                    let handles = (0..concurrent)
                        .map(|_| {
                            let gpu = gpu.clone();
                            let input = input.clone();
                            scope.spawn(move || {
                                pollster::block_on(dispatch_readback_async_once(&gpu, &input))
                            })
                        })
                        .collect::<Vec<_>>();
                    let mut last = 0;
                    for handle in handles {
                        last = handle.join().map_err(|_| {
                            GpuError::Internal(
                                "async readback worker panicked during benchmark".into(),
                            )
                        })??;
                    }
                    Ok(last)
                })
            },
        )?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = gpu_dispatch_readback() {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "gpu_dispatch_readback failed: {error}")
            .expect("write gpu benchmark error");
        std::process::exit(1);
    }
    std::process::exit(0);
}
