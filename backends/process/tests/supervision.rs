//! Supervision: exits, hangs, oversize packets, stderr floods, and what the registry refuses.

mod support;

use std::time::Duration;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, ProcessError};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};
use daedalus_worker_conformance::runner::registry;
use support::spec;

/// Compiles the scalar_add fixture as a one-node region, and returns its step.
fn scalar_add_step(backend: &ProcessBackend) -> daedalus_abi::Step {
    let inputs = [
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(0),
        },
        PortRef {
            node: NodeIndex(0),
            port: PortIndex(1),
        },
    ];
    let outputs = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
        incoming: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: "fixture.scalar_add",
        body: BodyIndex(0),
        inputs: 2,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: "fixture.scalar_add",
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

/// Runs a scalar_add step once with 2 + 40, returning how it ended and its fault text.
fn run_add(step: &mut daedalus_abi::Step) -> (StepEnd, String, Option<i64>) {
    let mut a = 2i64.to_le_bytes();
    let mut b = 40i64.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: a.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: b.as_mut_ptr(),
            len: 8,
            cap: 8,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    let value = cells[2]
        .bytes()
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map(i64::from_le_bytes);
    let reason = cx.reason().to_owned();
    (end, reason, value)
}

fn unique(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("supervision");
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn a_worker_that_exits_mid_run_faults_then_respawns_after_the_backoff() {
    let marker = unique("exit-once");
    let mut backend = ProcessBackend::new();
    let arg = format!("exit-once:{}", marker.display());
    backend
        .add_worker(spec(&["--misbehave", &arg]), &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);

    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault), "the first run faults");
    assert!(reason.contains("exited mid-run"), "{reason}");

    // Straight after the fault the link is down: the run faults at once, saying it is backing off.
    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("backing off"), "{reason}");

    std::thread::sleep(Duration::from_millis(400));
    let (end, _, value) = run_add(&mut step);
    assert!(
        matches!(end, StepEnd::Done),
        "the run after the backoff succeeds"
    );
    assert_eq!(value, Some(42));
    let telemetry = backend.telemetry();
    assert!(telemetry.restarts >= 1, "{telemetry:?}");
    assert!(telemetry.spawns >= 2, "{telemetry:?}");
    release(step);
}

#[test]
fn a_worker_that_never_answers_is_cancelled_then_killed_with_its_own_pid() {
    let mut backend = ProcessBackend::new();
    let hang = spec(&["--misbehave", "hang"])
        .deadline(Duration::from_millis(300))
        .grace(Duration::from_millis(200));
    backend.add_worker(hang, &registry()).expect("registers");
    let spawned = backend
        .worker_pid("fixture.scalar_add")
        .expect("a running worker");
    let mut step = scalar_add_step(&backend);

    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("killed"), "{reason}");
    assert_eq!(
        backend.killed_pids("fixture.scalar_add"),
        vec![spawned],
        "the killed pid is the spawned one"
    );
    release(step);
}

#[test]
fn an_oversize_packet_is_a_fault_and_not_a_large_allocation() {
    let mut backend = ProcessBackend::new();
    let mut oversize = spec(&["--misbehave", "oversize"]);
    oversize.packet_cap = 1 << 20;
    backend
        .add_worker(oversize, &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);
    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("over the cap"), "{reason}");
    release(step);
}

#[test]
fn stderr_is_drained_and_kept_within_64_kib() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&["--misbehave", "stderr-flood"]), &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);
    let (end, _, value) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(value, Some(42));
    // The drain thread counts stderr as it reads it, so give it a moment to catch up.
    let started = std::time::Instant::now();
    while backend.telemetry().stderr_bytes < 1 << 20 && started.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(backend.telemetry().stderr_bytes >= 1 << 20);
    assert!(backend.stderr_held("fixture.scalar_add") <= 64 * 1024);
    release(step);
}

#[test]
fn a_run_keeps_trying_after_three_consecutive_faults() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&["--misbehave", "exit"]), &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);
    let mut spawns_before = backend.telemetry().spawns;
    for round in 0..4 {
        let (end, _, _) = run_add(&mut step);
        assert!(matches!(end, StepEnd::Fault), "round {round} faults");
        // A respawn happens on the first run after the backoff, so the next run starts one.
        std::thread::sleep(Duration::from_millis(900));
        let (end, _, _) = run_add(&mut step);
        assert!(matches!(end, StepEnd::Fault), "round {round} still faults");
        let spawns = backend.telemetry().spawns;
        assert!(
            spawns > spawns_before,
            "round {round} started a new worker after the backoff"
        );
        spawns_before = spawns;
    }
    let telemetry = backend.telemetry();
    assert!(telemetry.faults >= 4, "{telemetry:?}");
    release(step);
}

#[test]
fn a_worker_whose_node_the_registry_lacks_is_refused_by_name() {
    let mut backend = ProcessBackend::new();
    let empty = daedalus_registry::Registry::new();
    match backend.add_worker(spec(&[]), &empty) {
        Err(error @ ProcessError::Declaration { .. }) => {
            let text = error.to_string();
            assert!(text.contains("the registry has no such node"), "{text}");
        }
        other => panic!("expected a declaration error, got {other:?}"),
    }
}
