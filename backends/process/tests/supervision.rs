//! Supervision: exits, hangs, oversize packets, stderr floods, and what the registry refuses.

mod support;

use std::time::Duration;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
#[cfg(unix)]
use daedalus_backend_process::WorkerSpec;
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
        placed: &[],
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
    run_add_with(step, &mut StepCx::new())
}

/// [`run_add`] with the caller's context.
fn run_add_with(step: &mut daedalus_abi::Step, cx: &mut StepCx) -> (StepEnd, String, Option<i64>) {
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
    let end = run_once(step, &mut Slots::of(&mut cells), cx);
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

    // Straight after the fault the link is down. The run is skipped at once, saying the worker is
    // backing off: the failure was the fault above, and this is not another.
    let faults = backend.telemetry().faults;
    let (end, reason, _) = run_add(&mut step);
    assert!(
        matches!(end, StepEnd::Skipped),
        "skipped, not a second fault"
    );
    assert!(reason.contains("backing off"), "{reason}");
    assert_eq!(
        backend.telemetry().faults,
        faults,
        "the wait counts no fault"
    );

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

    // The host stopped that worker itself, so the next run doesn't wait: a new worker is
    // started at once. This one hangs too, and is killed in its turn.
    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault));
    assert!(!reason.contains("backing off"), "{reason}");
    assert!(reason.contains("killed"), "{reason}");
    let killed = backend.killed_pids("fixture.scalar_add");
    assert_eq!(
        killed.len(),
        2,
        "a second worker ran and was killed: {killed:?}"
    );
    assert_ne!(killed[0], killed[1]);
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
        let (end, reason, _) = run_add(&mut step);
        if round == 0 {
            assert!(matches!(end, StepEnd::Fault), "the first run faults");
        } else {
            // The round before left the worker backing off: this run is skipped, not faulted.
            assert!(matches!(end, StepEnd::Skipped), "round {round}: {reason}");
            assert!(reason.contains("backing off"), "{reason}");
        }
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

/// What a worker started from `spec` finds in its environment. The program is a shell that writes
/// its environment to a file and exits, so it never says hello and is refused: the file is what
/// this is for.
#[cfg(unix)]
fn environment_of(name: &str, shape: impl FnOnce(WorkerSpec) -> WorkerSpec) -> String {
    let out = unique(name);
    let spec = WorkerSpec::new("/bin/sh")
        .arg("-c")
        .arg(format!("env > '{}'", out.display()));
    let mut backend = ProcessBackend::new();
    assert!(
        backend.add_worker(shape(spec), &registry()).is_err(),
        "a shell is no worker"
    );
    // The shell may still be writing when it is refused.
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(&out)
            && !text.is_empty()
        {
            return text;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("the shell wrote no environment to {}", out.display());
}

#[cfg(unix)]
fn has(environment: &str, key: &str) -> bool {
    environment
        .lines()
        .any(|line| line.starts_with(&format!("{key}=")))
}

#[cfg(unix)]
#[test]
fn a_worker_inherits_the_hosts_environment_unless_the_spec_takes_it_away() {
    // The test's own environment has these, or the comparison below says nothing.
    let inherited: Vec<&str> = ["PATH", "HOME"]
        .into_iter()
        .filter(|key| std::env::var_os(key).is_some())
        .collect();
    assert!(!inherited.is_empty(), "the test needs PATH or HOME set");

    let plain = environment_of("env-plain", |spec| spec.env("DAEDALUS_TEST_SET", "1"));
    assert!(has(&plain, "DAEDALUS_TEST_SET"));
    for key in &inherited {
        assert!(has(&plain, key), "{key} is inherited by default");
    }

    let cleared = environment_of("env-clear", |spec| {
        spec.env("DAEDALUS_TEST_SET", "1").env_clear()
    });
    assert!(
        has(&cleared, "DAEDALUS_TEST_SET"),
        "what the spec sets is set"
    );
    for key in &inherited {
        assert!(!has(&cleared, key), "{key} was cleared: {cleared}");
    }

    let first = inherited[0];
    let removed = environment_of("env-remove", |spec| {
        spec.env_remove(first)
            .env_remove("DAEDALUS_TEST_BOTH")
            .env("DAEDALUS_TEST_BOTH", "set")
    });
    assert!(!has(&removed, first), "{first} was removed");
    assert!(has(&removed, "DAEDALUS_TEST_BOTH"), "set wins over removed");
    for key in &inherited[1..] {
        assert!(has(&removed, key), "{key} is still inherited");
    }
}

#[test]
fn a_worker_registers_the_nodes_it_declares_and_then_serves_them() {
    use daedalus_registry::Registry;
    let mut backend = ProcessBackend::new();
    let mut found = Registry::new();
    let added = backend
        .add_worker_declaring(spec(&[]), &mut found)
        .expect("the worker says what it serves");
    // Everything the worker declared is registered, exactly as a registry written by hand has it.
    let by_hand = registry();
    let expected: Vec<String> = daedalus_worker_conformance::fixtures::declarations()
        .into_iter()
        .map(|decl| decl.id)
        .collect();
    assert_eq!(added, expected);
    for id in &added {
        let (got, want) = (
            found.entry(id).expect("registered"),
            by_hand.entry(id).expect("known"),
        );
        assert_eq!(got.decl.inputs, want.decl.inputs, "{id}");
        assert_eq!(got.decl.outputs, want.decl.outputs, "{id}");
        assert_eq!(got.bodies[0].backend, "process", "{id}");
    }
    // And the worker runs them.
    let mut step = scalar_add_step(&backend);
    let (end, reason, value) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!(value, Some(42));
    release(step);

    // The same spec again shares the worker and registers nothing.
    let again = backend
        .add_worker_declaring(spec(&[]), &mut found)
        .expect("shared");
    assert!(again.is_empty());
}

#[test]
fn a_worker_that_disagrees_with_a_node_already_registered_is_still_refused() {
    use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
    use daedalus_types::{Scalar, TypeExpr};
    // The registry has the worker's `fixture.scalar_add` with one input where the worker has two.
    let mut registry = Registry::new();
    registry
        .register_owned(OwnedDecl {
            id: "fixture.scalar_add".into(),
            source: "by hand".into(),
            inputs: vec![OwnedPort {
                name: "a".into(),
                ty: TypeRef::Expr(TypeExpr::Scalar(Scalar::Int)),
                optional: false,
            }],
            outputs: Vec::new(),
            body: OwnedBody::text("process", "fixture.scalar_add"),
            conversion: None,
        })
        .expect("registers");
    let mut backend = ProcessBackend::new();
    match backend.add_worker_declaring(spec(&[]), &mut registry) {
        Err(error @ ProcessError::Declaration { .. }) => {
            let text = error.to_string();
            assert!(text.contains("fixture.scalar_add"), "{text}");
            assert!(text.contains("the worker declares"), "{text}");
        }
        other => panic!("expected a declaration error, got {other:?}"),
    }
}

#[test]
fn a_workers_log_lines_reach_the_hosts_handler_and_are_counted() {
    use std::sync::{Arc, Mutex};
    let mut backend = ProcessBackend::new();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&heard);
    backend.on_log(move |line| {
        into.lock()
            .expect("the list")
            .push((line.level, line.text.to_owned(), line.pid));
    });
    backend
        .add_worker(spec(&["--misbehave", "log"]), &registry())
        .expect("registers");
    let pid = backend.worker_pid("fixture.scalar_add").expect("a worker");
    let mut step = scalar_add_step(&backend);
    for _ in 0..2 {
        let (end, _, value) = run_add(&mut step);
        assert!(matches!(end, StepEnd::Done));
        assert_eq!(value, Some(42));
    }
    use daedalus_backend_process::LogLevel;
    let lines = heard.lock().expect("the list").clone();
    let round = [
        (LogLevel::Info, "starting the sum".to_owned(), pid),
        (LogLevel::Warn, "the sum is large".to_owned(), pid),
    ];
    assert_eq!(lines, [round.clone(), round].concat());
    assert_eq!(backend.telemetry().logs, 4);

    // With the handler gone the lines are still counted.
    backend.clear_on_log();
    let (end, _, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Done));
    assert_eq!(heard.lock().expect("the list").len(), 4);
    assert_eq!(backend.telemetry().logs, 6);
    release(step);
}

#[test]
fn a_fault_report_ends_with_what_the_worker_logged_before_it_died() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&["--misbehave", "log-exit"]), &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);
    let (end, reason, _) = run_add(&mut step);
    assert!(matches!(end, StepEnd::Fault));
    assert!(
        reason.contains("[error] the device went away"),
        "the report has the worker's last line: {reason}"
    );
    assert_eq!(backend.telemetry().logs, 1);
    release(step);
}

#[test]
fn a_workers_reason_that_doesnt_fit_goes_whole_to_the_hosts_detail() {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(spec(&["--misbehave", "long-fault"]), &registry())
        .expect("registers");
    let mut step = scalar_add_step(&backend);
    let mut cx = StepCx::new();
    let kept = daedalus_abi::testing::keep_detail(&mut cx);
    let (end, reason, _) = run_add_with(&mut step, &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert!(cx.reason_cut(), "eight lines don't fit a reason: {reason}");
    let detail = kept.text();
    assert!(detail.starts_with(&reason), "the reason is its start");
    for line in 0..8 {
        assert!(
            detail.contains(&format!("the check found line {line} out of place")),
            "line {line} is in the detail: {detail}"
        );
    }
    release(step);
}
