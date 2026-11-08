//! Building the surface and the stable-ABI plugin as `cdylib`s for this test (the same approach as
//! `crates/daedalus/tests/support/cdylib.rs`).

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The workspace root: this crate lives in `<workspace>/crates/host-wrapper`.
pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("host-wrapper lives under <workspace>/crates")
        .to_path_buf()
}

/// Builds `package`'s library with `features` in its own cargo invocation and returns the
/// `cdylib`'s path. `DAEDALUS_CDYLIB_<LIB>` (the library name upper-cased) names a prebuilt one.
pub fn build_cdylib(package: &str, lib_name: &str, features: &str) -> PathBuf {
    if let Some(path) = std::env::var_os(format!("DAEDALUS_CDYLIB_{}", lib_name.to_uppercase())) {
        return PathBuf::from(path);
    }
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .args(["build", "-p", package, "--lib"])
        .arg("--message-format=json")
        .current_dir(workspace_root())
        .stderr(Stdio::inherit());
    if !features.is_empty() {
        command.args(["--features", features]);
        // A feature set changes the cdylib's exports but not its file name, so builds with
        // different features get their own target directory: test binaries that run in parallel
        // would otherwise overwrite each other's library (an `alloc-count` build with another's).
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| workspace_root().join("target"));
        command.env(
            "CARGO_TARGET_DIR",
            target.join(format!("cdylib-{}", features.replace(',', "-"))),
        );
    }
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let output = command
        .output()
        .expect("failed to spawn cargo build for a cdylib");
    assert!(output.status.success(), "cdylib build of {package} failed");
    let stdout = String::from_utf8(output.stdout).expect("cargo output is UTF-8");
    stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == lib_name)
        .flat_map(|msg| {
            msg["filenames"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(PathBuf::from))
        })
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(std::env::consts::DLL_SUFFIX))
        })
        .unwrap_or_else(|| panic!("cargo did not report a cdylib artifact for {package}"))
}
