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
