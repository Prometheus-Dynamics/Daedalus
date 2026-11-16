//! The C and C++ worker SDK, as a worker spec. The SDK is in `sdk/` (the C header and a CMake build),
//! and its C API is implemented in Rust by `daedalus-lang-cpp-sdk`, whose staticlib CMake links. The
//! canonical fixture worker is built from `fixtures/nodes.cpp` by CMake.

#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_backend_process::WorkerSpec;

/// The CMake build directory for the SDK under `target_tmp`, named `name` and keyed by the SDK's
/// source path. A CMake cache belongs to one source directory, so two clones that share a target
/// directory must not share a build directory.
pub fn build_dir(target_tmp: &Path, name: &str) -> PathBuf {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in env!("CARGO_MANIFEST_DIR").as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    target_tmp.join(format!("{name}-{hash:016x}"))
}

/// The staticlib of the Rust C API, which cargo builds beside the test binaries (the SDK crate is a
/// dev-dependency). Its file name carries a metadata hash, so the newest match in the deps folder is
/// the one this build of the tests links.
pub fn sdk_library() -> io::Result<PathBuf> {
    let deps = std::env::current_exe()?
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| io::Error::other("the test binary has no deps folder"))?;
    let mut found = Vec::new();
    for entry in std::fs::read_dir(&deps)? {
        let path = entry?.path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        // `libNAME-HASH.a`, or `NAME-HASH.lib` from the MSVC toolchain.
        let library = name.ends_with(".a") || name.ends_with(".lib");
        if library && name.contains("daedalus_lang_cpp_sdk") {
            let modified = std::fs::metadata(&path)?.modified()?;
            found.push((modified, path));
        }
    }
    found.sort();
    found
        .pop()
        .map(|(_, path)| path)
        .ok_or_else(|| io::Error::other("cargo built no daedalus-lang-cpp-sdk staticlib"))
}

/// Configures and builds the fixture worker in `build_dir` with CMake, linked against the Rust C API.
/// Returns the worker's path.
pub fn build(build_dir: &Path) -> io::Result<PathBuf> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let library = sdk_library()?;
    let configure = Command::new("cmake")
        .arg("-S")
        .arg(&source)
        .arg("-B")
        .arg(build_dir)
        .arg(format!("-DDAEDALUS_RUST_LIB={}", library.display()))
        .status()?;
    if !configure.success() {
        return Err(io::Error::other("cmake configure failed"));
    }
    // Every target, the worker and the native test, so one configure serves both.
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

/// Runs the native test (the header links, and a value round-trips) with ctest, in a build made by
/// [`build`].
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
