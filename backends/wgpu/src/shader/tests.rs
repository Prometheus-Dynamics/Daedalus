//! The cost of a run against the same pass written straight on wgpu.

use daedalus_abi::counting::counted;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
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
    inputs: &[],
    outputs: &[],
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

    let group = context
        .device
        .create_bind_group(&wgpu::BindGroupDescriptor {
            label: None,
            layout: &shader.pipeline.layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: src.buffer().as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: dst.buffer().as_entire_binding(),
                },
            ],
        });
    // The same pass with nothing of ours around it, and the same notice when it's done.
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
        let done = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&done);
        let _held = context.submit.lock();
        context.queue.submit([encoder.finish()]);
        context
            .queue
            .on_submitted_work_done(move || flag.store(true, Ordering::Release));
        done
    };
    let ours = || match shader.run(&bound).expect("runs") {
        Run::Started(ticket) => ticket,
        Run::Busy(_) => panic!("the device was idle"),
    };

    // Warm both paths, then count the submit alone, with the device idle before each. wgpu's
    // own allocations vary a little from run to run as its tables grow, so compare the cheapest
    // run of each.
    let idle = || context.wait("the test's work").expect("idle");
    for _ in 0..8 {
        ours().wait().expect("finishes");
        drop(raw());
        idle();
    }
    let cheapest_of = |run: &dyn Fn() -> usize| (0..16).map(|_| run()).min().unwrap_or(0);
    let ours_cost = cheapest_of(&|| {
        let (ticket, cost) = counted(ours);
        ticket.wait().expect("finishes");
        cost
    });
    let raw_cost = cheapest_of(&|| {
        let (done, cost) = counted(raw);
        idle();
        assert!(done.load(Ordering::Acquire));
        cost
    });
    assert!(
        ours_cost <= raw_cost,
        "a run allocates {ours_cost} times, and raw wgpu {raw_cost}"
    );

    let mut seen = [0; 16];
    src.write(0, &[21, 0, 0, 0]).expect("in range");
    ours().wait().expect("finishes");
    dst.read_into(0, &mut seen).expect("a readback");
    assert_eq!(seen[..4], [42, 0, 0, 0]);
}
