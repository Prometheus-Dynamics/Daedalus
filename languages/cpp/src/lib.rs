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
    // Every target, the worker and the native tests, so one configure serves both.
    let build = Command::new("cmake")
        .arg("--build")
        .arg(build_dir)
        .status()?;
    if !build.success() {
        return Err(io::Error::other("cmake build failed"));
    }
    let name = format!("daedalus_cpp_worker{}", std::env::consts::EXE_SUFFIX);
    let bin = build_dir.join("bin");
    // A generator with several configurations, as Visual Studio's is, puts the binary in a
    // folder named for the configuration.
    ["", "Debug", "Release", "RelWithDebInfo", "MinSizeRel"]
        .iter()
        .map(|configuration| bin.join(configuration).join(&name))
        .find(|worker| worker.is_file())
        .ok_or_else(|| io::Error::other("cmake built no worker binary"))
}

/// A spec that starts the worker at `worker`.
pub fn spec(worker: &Path) -> WorkerSpec {
    WorkerSpec::new(worker)
}

/// Runs the native tests (the value codec and the worker loop's state messages) with ctest, in a
/// build made by [`build`].
pub fn run_native_tests(build_dir: &Path) -> io::Result<()> {
    let mut ctest = Command::new("ctest");
    ctest.arg("--output-on-failure").current_dir(build_dir);
    // A generator with several configurations needs the one to test, as Visual Studio's does.
    if build_dir.join("bin").join("Debug").is_dir() {
        ctest.args(["-C", "Debug"]);
    }
    if ctest.status()?.success() {
        Ok(())
    } else {
        Err(io::Error::other("a native C or C++ test failed"))
    }
}
