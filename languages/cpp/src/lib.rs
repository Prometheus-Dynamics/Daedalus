//! The C and C++ worker SDK, as a worker spec. The SDK is in `sdk/` (a C codec and loop, with C and
//! C++ headers), and the canonical fixture worker is built from `fixtures/nodes.cpp` by CMake.

#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_backend_process::WorkerSpec;

/// Configures and builds the fixture worker in `build_dir` with CMake. Returns its path.
pub fn build(build_dir: &Path) -> io::Result<PathBuf> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let configure = Command::new("cmake")
        .arg("-S")
        .arg(&source)
        .arg("-B")
        .arg(build_dir)
        .status()?;
    if !configure.success() {
        return Err(io::Error::other("cmake configure failed"));
    }
    let build = Command::new("cmake")
        .arg("--build")
        .arg(build_dir)
        .arg("--target")
        .arg("daedalus_cpp_worker")
        .status()?;
    if !build.success() {
        return Err(io::Error::other("cmake build failed"));
    }
    Ok(build_dir.join("bin").join(format!(
        "daedalus_cpp_worker{}",
        std::env::consts::EXE_SUFFIX
    )))
}

/// A spec that starts the worker at `worker`.
pub fn spec(worker: &Path) -> WorkerSpec {
    WorkerSpec::new(worker)
}
