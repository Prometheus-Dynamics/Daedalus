//! The shared-memory transport on Linux: a large value travels in memory the host lends the worker,
//! a small one stays in its packet, a worker that doesn't ask stays plain and is counted, the
//! lent memory grows when a value outgrows it, and a worker that dies or hangs is replaced by one
//! that is lent memory again. The worker asks for shared memory alone here, so whole packets go
//! in it; values by reference are `tests/by_reference.rs`.

#![cfg(target_os = "linux")]

mod support;

use std::time::Duration;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::ProcessBackend;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_worker_conformance::runner::registry;
use support::spec;

/// The fixture, asking for shared memory but not values by reference.
fn packets(args: &[&str]) -> daedalus_backend_process::WorkerSpec {
    spec(args).env("DAEDALUS_FIXTURE_NO_REFERENCE", "1")
}

/// The `fixture.raw_io` node, which hands its bytes back reversed, compiled as a one-node region.
fn raw_io_step(backend: &ProcessBackend) -> Step {
    let ports = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &ports,
        outputs: &ports,
        incoming: &[],
        placed: &[],
        read_in: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.raw_io",
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.raw_io",
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
        sharing: &[],
    };
    match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile: {other:?}"),
    }
}

/// Runs the step once with `len` bytes, and checks a whole run comes back reversed.
fn round_trip(step: &mut Step, len: usize) -> (StepEnd, String) {
    let mut payload: Vec<u8> = (0..len).map(|at| (at * 31 + 7) as u8).collect();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: payload.as_mut_ptr(),
            len,
            cap: len,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    if matches!(end, StepEnd::Done) {
        let back = cells[1].bytes().expect("bytes come back");
        assert_eq!(back.len(), len, "the whole value comes back");
        assert!(
            back.iter().rev().eq(payload.iter()),
            "the bytes come back reversed"
        );
    }
    daedalus_runner::drop_owned(core::mem::replace(&mut cells[1], Slot::EMPTY));
    (end, cx.reason().to_owned())
}

const LARGE: usize = 256 << 10;

#[test]
fn a_large_value_travels_in_lent_memory_both_ways() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(packets(&[]), &registry())
        .expect("registers");
    let mut step = raw_io_step(&backend);
    for _ in 0..3 {
        let (end, reason) = round_trip(&mut step, LARGE);
        assert!(matches!(end, StepEnd::Done), "{reason}");
    }
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.grants, 1, "{telemetry:?}");
    assert_eq!(telemetry.plain_workers, 0, "{telemetry:?}");
    assert_eq!(telemetry.shared_in, 3, "each run went in lent memory");
    assert_eq!(telemetry.shared_out, 3, "each answer came back in it");
    release(step);
}

#[test]
fn a_small_value_stays_in_its_packet() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(packets(&[]), &registry())
        .expect("registers");
    let mut step = raw_io_step(&backend);
    let (end, reason) = round_trip(&mut step, 1024);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.grants, 1, "the worker is lent memory");
    assert_eq!(telemetry.shared_in, 0, "{telemetry:?}");
    assert_eq!(telemetry.shared_out, 0, "{telemetry:?}");
    release(step);
}

#[test]
fn a_worker_that_does_not_ask_stays_plain_and_is_counted() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(packets(&[]).env("DAEDALUS_FIXTURE_PLAIN", "1"), &registry())
        .expect("registers");
    let mut step = raw_io_step(&backend);
    let (end, reason) = round_trip(&mut step, LARGE);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.grants, 0, "{telemetry:?}");
    assert_eq!(telemetry.plain_workers, 1, "{telemetry:?}");
    assert_eq!(telemetry.shared_in, 0, "{telemetry:?}");
    assert_eq!(telemetry.shared_out, 0, "{telemetry:?}");
    release(step);
}

#[test]
fn lent_memory_grows_when_a_value_outgrows_it() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(packets(&[]), &registry())
        .expect("registers");
    let mut step = raw_io_step(&backend);
    // Larger than the memory first lent, so the first run goes plain and the memory grows.
    let big = 3 << 20;
    for _ in 0..3 {
        let (end, reason) = round_trip(&mut step, big);
        assert!(matches!(end, StepEnd::Done), "{reason}");
    }
    let telemetry = backend.telemetry();
    assert!(
        telemetry.grants >= 2,
        "the memory was lent again, larger: {telemetry:?}"
    );
    assert!(telemetry.shared_in >= 1, "{telemetry:?}");
    assert!(telemetry.shared_out >= 1, "{telemetry:?}");
    // Once grown, it stays grown: the last run went both ways in lent memory.
    let grants = telemetry.grants;
    let (shared_in, shared_out) = (telemetry.shared_in, telemetry.shared_out);
    let (end, reason) = round_trip(&mut step, big);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.grants, grants, "{telemetry:?}");
    assert_eq!(telemetry.shared_in, shared_in + 1, "{telemetry:?}");
    assert_eq!(telemetry.shared_out, shared_out + 1, "{telemetry:?}");
    release(step);
}

#[test]
fn a_worker_that_dies_is_replaced_by_one_lent_memory_again() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("shared-memory");
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let marker = dir.join("exit-once");
    let _ = std::fs::remove_file(&marker);
    let mut backend = ProcessBackend::new();
    let arg = format!("exit-once:{}", marker.display());
    backend
        .add_worker(packets(&["--misbehave", &arg]), &registry())
        .expect("registers");
    let mut step = raw_io_step(&backend);
    let (end, reason) = round_trip(&mut step, LARGE);
    assert!(matches!(end, StepEnd::Fault), "{reason}");
    std::thread::sleep(Duration::from_millis(400));
    let (end, reason) = round_trip(&mut step, LARGE);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    let telemetry = backend.telemetry();
    assert_eq!(
        telemetry.grants, 2,
        "each worker was lent memory: {telemetry:?}"
    );
    assert_eq!(telemetry.shared_out, 1, "the new worker answered in it");
    release(step);
}

#[test]
fn a_hung_worker_is_killed_and_its_replacement_lent_memory_again() {
    let mut backend = ProcessBackend::new();
    let hang = packets(&["--misbehave", "hang"])
        .deadline(Duration::from_millis(300))
        .grace(Duration::from_millis(200));
    backend.add_worker(hang, &registry()).expect("registers");
    let mut step = raw_io_step(&backend);
    for _ in 0..2 {
        let (end, reason) = round_trip(&mut step, LARGE);
        assert!(matches!(end, StepEnd::Fault), "{reason}");
        assert!(reason.contains("killed"), "{reason}");
    }
    let telemetry = backend.telemetry();
    assert_eq!(telemetry.grants, 2, "{telemetry:?}");
    assert_eq!(telemetry.shared_in, 2, "both runs went in lent memory");
    assert_eq!(backend.killed_pids("fixture.raw_io").len(), 2);
    release(step);
}
