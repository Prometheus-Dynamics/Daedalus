//! Builds the worker fixture binary once per test binary, and gives specs for it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use daedalus_abi::Step;
use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_types::{BackendIndex, NodeIndex, PortIndex};

/// The worker fixture binary, built by cargo into the test target directory, or the one
/// `DAEDALUS_WORKER_FIXTURE` names: a machine with no cargo, such as a board, runs a test binary
/// built elsewhere beside a fixture built with it.
pub fn worker_binary() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            if let Some(path) = std::env::var_os("DAEDALUS_WORKER_FIXTURE") {
                return PathBuf::from(path);
            }
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("worker-fixture");
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .current_dir(&root)
                .args([
                    "build",
                    "-q",
                    "-p",
                    "daedalus-worker-fixture",
                    "--target-dir",
                ])
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the worker fixture builds");
            target.join("debug").join(format!(
                "daedalus-worker-fixture{}",
                std::env::consts::EXE_SUFFIX
            ))
        })
        .clone()
}

/// A spec that starts the fixture worker, with extra arguments.
pub fn spec(args: &[&str]) -> WorkerSpec {
    let mut spec = WorkerSpec::new(worker_binary());
    for arg in args {
        spec = spec.arg(*arg);
    }
    spec
}

#[allow(dead_code)] // not every test binary compiles a step
/// One node as a region of its own, with one input and one output.
pub fn step_of(backend: &ProcessBackend, id: &'static str) -> Step {
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
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id,
        body: BodyIndex(0),
        inputs: 1,
        outputs: 1,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: id,
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
