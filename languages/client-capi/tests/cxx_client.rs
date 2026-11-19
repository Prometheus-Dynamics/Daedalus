//! A C++ program against the built library, through the wrapper `daedalus_client.hpp`: compiled
//! with the C header and the SDK's wrapper, linked to the shared library, and run against an
//! in-process daemon with the plugin fixture. It checks what the wrapper adds: the retry for a
//! result larger than its first buffer, the status and code a `ClientError` carries, and the handle
//! closed with its client.

#![cfg(unix)]

use crate::support;

use std::path::Path;
use std::process::Command;

#[test]
fn a_cpp_program_reads_large_results_and_typed_errors_through_the_wrapper() {
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
    let library = support::surface_library();
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("cxx-client");
    std::fs::create_dir_all(&dir).expect("a directory");
    let program = dir.join("client");
    let built = Command::new(cxx)
        .args([
            "-std=c++17",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pedantic",
            "-I",
        ])
        .arg(manifest.join("include"))
        .arg("-I")
        .arg(manifest.join("../cpp/sdk/include"))
        .arg(manifest.join("tests/cxx/client.cpp"))
        .arg(&library)
        .arg("-o")
        .arg(&program)
        .output()
        .expect("the compiler runs");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );

    // The daemon is this process's: the program connects to its loopback port.
    let daemon = support::daemon();
    let ran = Command::new(&program)
        .arg(support::tcp(daemon.port))
        .arg(support::fixture_library())
        .env("LD_LIBRARY_PATH", library.parent().expect("a directory"))
        .env("DYLD_LIBRARY_PATH", library.parent().expect("a directory"))
        .output()
        .expect("the program runs");
    assert!(
        ran.status.success(),
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "c++ client: ok\n");
    drop(daemon);
}
