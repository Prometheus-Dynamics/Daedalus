//! A dylib plugin's nodes, run through the plugin worker, give what an in-process load gives, and a
//! node that aborts is a fault the host survives.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use daedalus_abi::testing::release;
use daedalus_abi::{Slot, SlotState, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_loader::{Loader, RegionIn, run_step};
use daedalus_registry::Registry;
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

/// The dylib fixture, built by cargo into the test target directory.
fn fixture_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
            let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib/Cargo.toml");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(&manifest)
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the dylib fixture builds");
            target.join("debug").join(format!(
                "{}daedalus_dylib_fixture{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        })
        .clone()
}

/// Compiles one plugin node as a one-node region of the process backend.
fn process_step(backend: &ProcessBackend, node: &'static str) -> Step {
    let inputs = [PortRef {
        node: NodeIndex(0),
        port: PortIndex(0),
    }];
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
        read_in: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: node,
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: false,
        fire_all: false,
        metrics: 0,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: node,
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
        lent: &[],
        offered: &[],
    };
    match backend.compile(&region, &cx) {
        Ok(Compiled::Steps(mut units)) => units.pop().expect("a unit").step,
        other => panic!("compile {node}: {other:?}"),
    }
}

/// Runs a one-input, one-output u32 node: the input is `value`, and the output bytes come back.
fn run_u32(step: &mut Step, value: u32) -> (StepEnd, String, Option<Vec<u8>>) {
    let mut input = value.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            data: input.as_mut_ptr(),
            len: 4,
            cap: 4,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let end = run_step(step, &mut Slots::of(&mut cells), &mut cx);
    let out = cells[1].bytes().map(<[u8]>::to_vec);
    (end, cx.reason().to_owned(), out)
}

#[test]
fn the_plugin_through_its_worker_gives_the_in_process_outputs() {
    let library = fixture_library();

    // In process: the loader runs the plugin's own step.
    let mut loader = Loader::new();
    let plugin = loader.open(&library).expect("opens");
    let backend = plugin.backend("fixture.cpu").expect("backend");
    let nodes = ["fixture.pass"];
    let port = daedalus_abi::BoundaryPort {
        node: 0,
        port: 0,
        reserved: 0,
        type_index: 0,
        slot: 0,
    };
    let region = RegionIn {
        nodes: &nodes,
        inputs: &[port],
        outputs: &[port],
    };
    let mut in_process = backend
        .compile(&region)
        .expect("compiles")
        .pop()
        .expect("unit")
        .step;
    let (end, _, in_process_out) = run_u32(&mut in_process, 4242);
    assert!(matches!(end, StepEnd::Done));
    release(in_process);

    // Through the worker: the same node, from the same library.
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    let mut process = ProcessBackend::new();
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-plugin-worker"))
        .arg(library.display().to_string());
    process.add_worker(spec, &registry).expect("registers");
    let mut step = process_step(&process, "fixture.pass");
    let (end, reason, through_worker) = run_u32(&mut step, 4242);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!(through_worker, in_process_out);
    assert_eq!(through_worker, Some(4242u32.to_le_bytes().to_vec()));
    release(step);
}

#[test]
fn an_aborting_node_faults_its_region_and_the_host_runs_on_after_the_backoff() {
    let library = fixture_library();
    let mut loader = Loader::new();
    let plugin = loader.open(&library).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    let mut process = ProcessBackend::new();
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-plugin-worker"))
        .arg(library.display().to_string());
    process.add_worker(spec, &registry).expect("registers");

    let mut abort = process_step(&process, "fixture.abort");
    let (end, reason, _) = run_u32(&mut abort, 1);
    assert!(matches!(end, StepEnd::Fault));
    assert!(reason.contains("exited mid-run"), "{reason}");

    std::thread::sleep(Duration::from_millis(300));
    let mut pass = process_step(&process, "fixture.pass");
    let (end, reason, out) = run_u32(&mut pass, 7);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!(out, Some(7u32.to_le_bytes().to_vec()));
    assert!(process.telemetry().restarts >= 1);
}

#[test]
fn what_a_plugins_node_gives_as_detail_reaches_the_host_through_its_worker() {
    let library = fixture_library();
    let mut loader = Loader::new();
    let plugin = loader.open(&library).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    let mut process = ProcessBackend::new();
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-plugin-worker"))
        .arg(library.display().to_string());
    process.add_worker(spec, &registry).expect("registers");

    let mut boom = process_step(&process, "fixture.boom");
    let mut input = 1u32.to_le_bytes();
    let mut cells = [
        Slot {
            state: SlotState::Full as u32,
            data: input.as_mut_ptr(),
            len: 4,
            cap: 4,
            ..Slot::EMPTY
        },
        Slot::EMPTY,
    ];
    let mut cx = StepCx::new();
    let kept = daedalus_abi::testing::keep_detail(&mut cx);
    let end = run_step(&mut boom, &mut Slots::of(&mut cells), &mut cx);
    assert!(matches!(end, StepEnd::Fault));
    assert!(cx.reason().contains("panicked"), "{}", cx.reason());
    assert_eq!(kept.text(), "boom: what led up to it, in 2 pieces");
    assert_eq!(
        (cx.code, cx.code_is_own()),
        (daedalus_abi::own::PANIC, true),
        "a panic in a worker is named as one, as it is in the host's process"
    );
    release(boom);
}

/// Runs the plugin worker binary with `args`, its stdin closed, and gives its exit code and stderr.
#[cfg(target_os = "linux")]
fn run_worker_binary(args: &[&std::ffi::OsStr]) -> (Option<i32>, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_daedalus-plugin-worker"))
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run the plugin worker");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[cfg(target_os = "linux")]
#[test]
fn the_lock_refuses_a_changed_library_and_loads_the_approved_one() {
    use daedalus_integrity::{Digest, Lock};
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("plugin-worker-lock");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let library = dir.join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(fixture_library(), &library).expect("copy the fixture");
    let lock_path = dir.join("daedalus.lock");
    let approved = Digest::of(&std::fs::read(&library).unwrap());
    let mut lock = Lock::new();
    lock.set(
        library.file_name().unwrap().to_str().unwrap().to_string(),
        approved,
    );
    std::fs::write(&lock_path, lock.render()).unwrap();

    let (code, stderr) = run_worker_binary(&[
        "--lock".as_ref(),
        lock_path.as_os_str(),
        library.as_os_str(),
    ]);
    assert!(
        !stderr.contains("is not the file the lock approves"),
        "{stderr}"
    );
    assert_ne!(
        code,
        Some(2),
        "the approved library is not refused: {stderr}"
    );

    let mut bytes = std::fs::read(&library).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    std::fs::write(&library, &bytes).unwrap();
    let (code, stderr) = run_worker_binary(&[
        "--lock".as_ref(),
        lock_path.as_os_str(),
        library.as_os_str(),
    ]);
    assert_eq!(code, Some(2), "{stderr}");
    assert!(
        stderr.contains("is not the file the lock approves"),
        "{stderr}"
    );
    assert!(stderr.contains(&approved.to_string()), "{stderr}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(target_os = "linux")]
#[test]
fn record_writes_the_library_and_a_lock_flag_needs_its_file() {
    use daedalus_integrity::{Digest, read_lock};
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("plugin-worker-record");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let library = dir.join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(fixture_library(), &library).expect("copy the fixture");
    let lock_path = dir.join("daedalus.lock");

    let (code, stderr) = run_worker_binary(&[
        "--lock".as_ref(),
        lock_path.as_os_str(),
        "--record".as_ref(),
        library.as_os_str(),
    ]);
    assert_ne!(code, Some(2), "{stderr}");
    let name = library.file_name().unwrap().to_str().unwrap().to_string();
    let recorded = read_lock(&lock_path).expect("the lock was written");
    assert_eq!(
        recorded.get(&name),
        Some(&Digest::of(&std::fs::read(&library).unwrap()))
    );

    let (code, stderr) = run_worker_binary(&["--record".as_ref(), library.as_os_str()]);
    assert_eq!(code, Some(2), "--record alone is a usage error: {stderr}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_plugin_node_that_prints_does_not_corrupt_the_worker_stream() {
    let library = fixture_library();
    let mut loader = Loader::new();
    let plugin = loader.open(&library).expect("opens");
    let mut registry = Registry::new();
    plugin.join(&mut registry).expect("joins");
    let mut process = ProcessBackend::new();
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-plugin-worker"))
        .arg(library.display().to_string());
    process.add_worker(spec, &registry).expect("registers");

    // The node prints a line to stdout, then passes its input on: the run's answer is intact.
    let mut step = process_step(&process, "fixture.chatty");
    let (end, reason, out) = run_u32(&mut step, 4243);
    assert!(matches!(end, StepEnd::Done), "{reason}");
    assert_eq!(out, Some(4243u32.to_le_bytes().to_vec()));
    release(step);
    assert_eq!(process.telemetry().faults, 0);
}
