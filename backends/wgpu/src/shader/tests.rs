//! The cost of a run against the same pass written straight on wgpu.

use daedalus_abi::counting::counted;

use super::{BindingKind, BindingSpec, Bound, Dispatch, Shader, ShaderSpec};
use crate::{Options, Storage, WgpuBackend};

static DOUBLE: ShaderSpec = ShaderSpec {
    name: "double",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read> src: array<u32>;
        @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * 2u; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[
        BindingSpec {
            index: 0,
            name: "src",
            kind: BindingKind::Storage {
                read_only: true,
                stride: 4,
            },
        },
        BindingSpec {
            index: 1,
            name: "dst",
            kind: BindingKind::Storage {
                read_only: false,
                stride: 4,
            },
        },
    ],
    dispatch: Dispatch::PerElement {
        binding: 1,
        stride: 4,
    },
};

#[test]
fn a_kernel_dispatch_allocates_no_more_than_raw_wgpu() {
    let backend = WgpuBackend::open(Options::default());
    let Ok(context) = backend.context() else {
        assert!(std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none());
        return;
    };
    let shader = Shader::load(&backend, &DOUBLE).expect("loads");
    let src = Storage::new(&backend, 4096).expect("a buffer");
    let dst = Storage::new(&backend, 4096).expect("a buffer");
    let bound = [Bound::Storage(&src), Bound::Storage(&dst)];

    // The same pass with nothing of ours around it.
    let group = context
        .device
        .create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &shader.pipeline.layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: src.buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: dst.buffer.as_entire_binding(),
                },
            ],
        });
    let raw = || {
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor::default());
            pass.set_pipeline(&shader.pipeline.pipeline);
            pass.set_bind_group(0, &group, &[]);
            pass.dispatch_workgroups(16, 1, 1);
        }
        let _held = context.submit.lock();
        context.queue.submit([encoder.finish()]);
    };

    // Warm both paths, then count. wgpu's own allocations vary a little from run to run as its
    // tables grow, so compare the cheapest run of each.
    for _ in 0..8 {
        shader.run(&bound).expect("runs");
        raw();
    }
    let cheapest = |run: &dyn Fn()| (0..16).map(|_| counted(run).1).min().unwrap_or(0);
    let ours = cheapest(&|| shader.run(&bound).expect("runs"));
    let theirs = cheapest(&raw);
    assert!(
        ours <= theirs,
        "a run allocates {ours} times, and raw wgpu {theirs}"
    );

    let mut seen = [0; 16];
    src.write(0, &[21, 0, 0, 0]).expect("in range");
    shader.run(&bound).expect("runs");
    dst.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen[..4], [42, 0, 0, 0]);
}
