//! Builds the worker fixture binary once per test binary, and gives specs for it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use daedalus_backend_process::WorkerSpec;

/// The worker fixture binary, built by cargo into the test target directory.
pub fn worker_binary() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
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
