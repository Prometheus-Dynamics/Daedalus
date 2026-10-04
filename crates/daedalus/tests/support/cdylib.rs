//! Building example plugins as `cdylib`s for the dynamic plugin tests.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("facade crate should live under <workspace>/crates")
        .to_path_buf()
}

/// `--features` for a plugin build: its `dylib` export plus this host's enabled boundary
/// facade features (e.g. GPU), which the build fingerprint requires the plugin to share, so a
/// host built with e.g. `--all-features` gets a plugin built the same way.
#[allow(dead_code)] // Not every test binary builds host-matching plugins.
pub fn host_matching_features() -> String {
    let (_, facade) = daedalus::dylib::boundary_features()
        .into_iter()
        .find(|(krate, _)| *krate == "daedalus")
        .expect("facade features are fingerprinted");
    std::iter::once("dylib".to_string())
        .chain(
            facade
                .into_iter()
                .map(|feature| format!("daedalus/{feature}")),
        )
        .collect::<Vec<_>>()
        .join(",")
}

/// Build `package`'s library with `features` in its own cargo invocation (with the cargo, and
/// therefore rustc, running this test) and return the `cdylib`'s path.
pub fn build_cdylib(package: &str, lib_name: &str, features: &str) -> PathBuf {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .args(["build", "-p", package, "--lib", "--features", features])
        .arg("--message-format=json")
        .current_dir(workspace_root())
        .stderr(Stdio::inherit());
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let output = command
        .output()
        .expect("failed to spawn cargo build for dynamic plugin");
    assert!(output.status.success(), "dynamic plugin build failed");
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
