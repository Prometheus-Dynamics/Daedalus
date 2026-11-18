//! The C++ driver (`tests/drive/driver.cpp`, over `daedalus_client.hpp`) through the drive
//! conformance suite: every scenario, over each transport the platform has, zero differences from
//! the golden transcripts. The driver talks to the Rust client through the client C ABI, so the
//! codec check does not apply to it: that check is for a client that encodes its own messages.
//!
//! Unix only: the driver is built with the C++ compiler and loads the client library as a shared
//! object. The suite itself runs over TCP on every platform.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_drive_conformance::{Failure, build_fixture, run_driver};

/// The client C ABI as a shared library, which cargo builds beside the tests (a dev-dependency).
fn client_library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let deps = exe.parent().expect("the deps directory");
    let name = format!(
        "{}daedalus_client_capi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let found = deps.join(name);
    assert!(
        found.exists(),
        "{} isn't there: the crate builds as a cdylib",
        found.display()
    );
    found
}

/// The driver, compiled with the C++ compiler against the client header and the SDK's wrappers.
/// Returns its path. A test without a C++ compiler fails: the suite needs one.
fn build_driver(library: &Path) -> PathBuf {
    let cxx = ["c++", "g++", "clang++"]
        .into_iter()
        .find(|cxx| {
            Command::new(cxx)
                .arg("--version")
                .output()
                .is_ok_and(|out| out.status.success())
        })
        .expect("a C++ compiler: this test needs one");
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = daedalus_lang_cpp::build_dir(Path::new(env!("CARGO_TARGET_TMPDIR")), "cpp-driver");
    std::fs::create_dir_all(&dir).expect("a directory");
    let driver = dir.join("driver");
    let built = Command::new(cxx)
        .args([
            "-std=c++17",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pedantic",
            "-I",
        ])
        .arg(manifest.join("../client-capi/include"))
        .arg("-I")
        .arg(manifest.join("sdk/include"))
        .arg(manifest.join("tests/drive/driver.cpp"))
        .arg(library)
        .arg("-o")
        .arg(&driver)
        .output()
        .expect("the compiler runs");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    driver
}

fn report(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(|failure| failure.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_cpp_driver_gives_the_golden_transcripts_for_every_scenario() {
    let library = client_library();
    let driver = build_driver(&library);
    let fixture = build_fixture(&Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture"))
        .expect("the dylib fixture builds");
    let mut command = Command::new(&driver);
    command.env("LD_LIBRARY_PATH", library.parent().expect("a directory"));
    command.env("DYLD_LIBRARY_PATH", library.parent().expect("a directory"));
    let failures = run_driver(command, &fixture);
    assert!(failures.is_empty(), "{}", report(&failures));
}
