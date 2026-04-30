use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use daedalus_gpu::shader::{
    Access, BindingData, BindingKind, BindingSpec, BufferInit, ShaderBinding, ShaderSpec,
    dispatch_shader_with_bindings,
};
use daedalus_gpu::{GpuBackendKind, GpuError, GpuOptions, select_backend};

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

fn gpu_dispatch_readback(c: &mut Criterion) {
    let Ok(gpu) = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) else {
        return;
    };

    let mut group = c.benchmark_group("gpu_dispatch_readback");
    for len in [256usize, 4096] {
        let input = (0..len as u32).collect::<Vec<_>>();
        group.bench_function(BenchmarkId::new("storage_buffer_u32_plus_one", len), |b| {
            b.iter(|| {
                let last = dispatch_readback_once(&gpu, std::hint::black_box(&input))
                    .expect("gpu dispatch readback");
                std::hint::black_box(last);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, gpu_dispatch_readback);
criterion_main!(benches);
