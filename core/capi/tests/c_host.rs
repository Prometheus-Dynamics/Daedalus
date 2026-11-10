//! A real C program against the built library: compiled with the header, linked to the shared
//! library, and run on the plugin fixture.

#![cfg(unix)]

mod support;

use std::path::Path;
use std::process::Command;

#[test]
fn a_c_program_compiles_a_graph_pushes_ticks_and_takes() {
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
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("c-host");
    std::fs::create_dir_all(&dir).expect("a directory");
    let program = dir.join("host");
    let built = Command::new(cc)
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic", "-I"])
        .arg(manifest.join("include"))
        .arg(manifest.join("tests/c/host.c"))
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
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "c host: ok\n");
}
