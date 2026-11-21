//! A held queue: work submitted behind `raw_hold` is done only once the gate opens.

use crate::common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

const SET: ShaderSpec = ShaderSpec {
    name: "test.raw_gate.set",
    entry: "main",
    source: "
        @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
        @compute @workgroup_size(64)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&dst) { dst[id.x] = id.x + 7u; }
        }
    ",
    workgroup: [64, 1, 1],
    bindings: &[BindingSpec {
        index: 0,
        name: "dst",
        kind: BindingKind::Storage {
            read_only: false,
            stride: 4,
        },
    }],
    dispatch: Dispatch::PerElement {
        binding: 0,
        stride: 4,
    },
    inputs: &[],
    outputs: &[],
};

/// One run of `SET` over `dst`, on its own thread: behind a hold the submit may block until the
/// gate opens (lavapipe's does). The flag is set once the run has finished.
fn run_on_a_thread<'scope>(
    scope: &'scope std::thread::Scope<'scope, '_>,
    shader: &'scope Shader,
    dst: &'scope Storage,
    done: &'scope AtomicBool,
) -> std::thread::ScopedJoinHandle<'scope, ()> {
    scope.spawn(move || {
        let Run::Started(ticket) = shader.run(&[Bound::Storage(dst)]).expect("runs") else {
            panic!("nothing else is in flight");
        };
        ticket.wait().expect("finishes once open");
        done.store(true, Ordering::Release);
    })
}

#[test]
fn work_behind_a_held_queue_runs_only_once_the_gate_opens() {
    let Some(record) = common::adapter() else {
        return;
    };
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    });
    let shader = Shader::load(&backend, &SET).expect("loads");
    let dst = Storage::new(&backend, 4096).expect("a buffer");
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let gate = backend.raw_hold().expect("the queue is held");
        let run = run_on_a_thread(scope, &shader, &dst, &done);
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !done.load(Ordering::Acquire),
            "the work ran while the gate was closed"
        );
        assert!(!gate.passed(Duration::ZERO), "the held wait passed closed");
        gate.open().expect("opens");
        gate.open().expect("opening again does nothing");
        run.join().expect("the run's thread");
        assert!(gate.passed(Duration::from_secs(10)), "the held wait passed");
    });
    assert!(done.load(Ordering::Acquire), "the held work finished");
    let mut got = vec![0; 4096];
    dst.read_into(0, &mut got).expect("a readback");
    let words = got.as_chunks::<4>().0;
    assert!(
        (0..1024).all(|index| u32::from_le_bytes(words[index]) == index as u32 + 7),
        "the held work ran in full"
    );
    // A gate dropped closed opens, so nothing stays held.
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let gate = backend.raw_hold().expect("held again");
        let run = run_on_a_thread(scope, &shader, &dst, &done);
        drop(gate);
        run.join().expect("the run's thread");
    });
    assert!(
        done.load(Ordering::Acquire),
        "the work finished after the drop"
    );
}
