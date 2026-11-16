//! A real C program against the built library: compiled with the header, linked to the shared
//! library, and run against an in-process daemon with the plugin fixture loaded.

#![cfg(unix)]

mod support;

use std::path::Path;
use std::process::Command;

#[test]
fn a_c_program_connects_loads_stores_starts_feeds_steps_takes_and_closes() {
    let cc = ["cc", "gcc", "clang"]
        .into_iter()
        .find(|cc| {
            Command::new(cc)
                .arg("--version")
                .output()
                .is_ok_and(|out| out.status.success())
        })
        .expect("a C compiler: this test needs one");
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let library = support::surface_library();
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("c-client");
    std::fs::create_dir_all(&dir).expect("a directory");
    let program = dir.join("caller");
    let built = Command::new(cc)
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic", "-I"])
        .arg(manifest.join("include"))
        .arg(manifest.join("tests/c/caller.c"))
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
    let endpoint = support::tcp(daemon.port);
    let ran = Command::new(&program)
        .arg(&endpoint)
        .arg(support::fixture_library())
        .arg("hello")
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
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "c client: ok hello\n");
    drop(daemon);
}
