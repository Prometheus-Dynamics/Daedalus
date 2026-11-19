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

/// The shared library a crate builds beside the test binaries, by its file stem (`daedalus_capi`).
/// A cdylib is not hashed, so the file is the one of that name in the deps folder.
pub fn shared_library(stem: &str) -> io::Result<PathBuf> {
    let deps = std::env::current_exe()?
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| io::Error::other("the test binary has no deps folder"))?;
    let name = format!(
        "{}{stem}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let found = deps.join(name);
    if found.is_file() {
        Ok(found)
    } else {
        Err(io::Error::other(format!(
            "{} is not there",
            found.display()
        )))
    }
}

/// Configures and builds the fixture worker in `build_dir` with CMake, linked against the Rust C API,
/// and the host and client libraries the SDK's targets name. Returns the worker's path.
pub fn build(build_dir: &Path) -> io::Result<PathBuf> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let library = sdk_library()?;
    let host = shared_library("daedalus_capi")?;
    let client = shared_library("daedalus_client_capi")?;
    let configure = Command::new("cmake")
        .arg("-S")
        .arg(&source)
        .arg("-B")
        .arg(build_dir)
        .arg(format!("-DDAEDALUS_RUST_LIB={}", library.display()))
        .arg(format!("-DDAEDALUS_HOST_LIB={}", host.display()))
        .arg(format!("-DDAEDALUS_CLIENT_LIB={}", client.display()))
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

/// The configuration a multi-configuration generator put the build in (Visual Studio's), or `None`
/// for a single-configuration build.
fn configuration(build_dir: &Path) -> Option<&'static str> {
    build_dir
        .join("bin")
        .join("Debug")
        .is_dir()
        .then_some("Debug")
}

/// Runs a CMake command, and names `what` when it fails.
fn run_cmake(mut command: Command, what: &str) -> io::Result<()> {
    if command.status()?.success() {
        Ok(())
    } else {
        Err(io::Error::other(what.to_owned()))
    }
}

/// Installs the SDK that [`build`] made in `build_dir` into `prefix`, then configures and builds the
/// consumer in `sdk/tests/consumer` in `consumer_build`. The consumer finds the installed package
/// with `find_package`, and links the Rust library through it. Returns the consumer's path.
pub fn install_and_build_consumer(
    build_dir: &Path,
    prefix: &Path,
    consumer_build: &Path,
) -> io::Result<PathBuf> {
    let configuration = configuration(build_dir);
    // A stale prefix from an earlier run would hide a file the install no longer writes.
    std::fs::remove_dir_all(prefix).or_else(|error| match error.kind() {
        io::ErrorKind::NotFound => Ok(()),
        _ => Err(error),
    })?;
    let mut install = Command::new("cmake");
    install
        .arg("--install")
        .arg(build_dir)
        .arg("--prefix")
        .arg(prefix);
    if let Some(configuration) = configuration {
        install.args(["--config", configuration]);
    }
    run_cmake(install, "cmake --install failed")?;

    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("sdk")
        .join("tests")
        .join("consumer");
    let mut configure = Command::new("cmake");
    configure
        .arg("-S")
        .arg(&source)
        .arg("-B")
        .arg(consumer_build)
        .arg(format!("-DCMAKE_PREFIX_PATH={}", prefix.display()));
    run_cmake(configure, "cmake configure of the consumer failed")?;

    let mut build = Command::new("cmake");
    build.arg("--build").arg(consumer_build);
    if let Some(configuration) = configuration {
        build.args(["--config", configuration]);
    }
    run_cmake(build, "cmake build of the consumer failed")?;

    let name = format!("consumer{}", std::env::consts::EXE_SUFFIX);
    ["", "Debug", "Release", "RelWithDebInfo", "MinSizeRel"]
        .iter()
        .map(|configuration| consumer_build.join(configuration).join(&name))
        .find(|consumer| consumer.is_file())
        .ok_or_else(|| io::Error::other("cmake built no consumer binary"))
}
