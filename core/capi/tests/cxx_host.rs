//! A C++ program against the built library, through the header `daedalus_host.hpp`: compiled with
//! the C API's header and the SDK's wrapper, linked to the shared library, and run on the plugin
//! fixture. It is the same check as `c_host.rs`, written with RAII and exceptions.

#![cfg(unix)]

use crate::support;

use std::path::Path;
use std::process::Command;

#[test]
fn a_cpp_program_drives_the_host_through_the_wrapper() {
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
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("cxx-host");
    std::fs::create_dir_all(&dir).expect("a directory");
    let program = dir.join("host");
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
        .arg(manifest.join("../../languages/cpp/sdk/include"))
        .arg(manifest.join("tests/cxx/host.cpp"))
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

    let ran = Command::new(&program)
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
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "c++ host: ok\n");
}
