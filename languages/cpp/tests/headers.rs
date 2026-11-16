//! The host header (`core/capi`) and the worker header (`languages/cpp/sdk`) compile in one C
//! program. They used to share a file name and a guard, so one program could include only one.

#![cfg(unix)]

use std::path::Path;
use std::process::Command;

#[test]
fn both_c_headers_compile_and_link_in_one_program() {
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
    let host = manifest.join("../../core/capi/include");
    let worker = manifest.join("sdk/include");
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("c-headers");
    std::fs::create_dir_all(&dir).expect("a directory");
    let program = dir.join("both");
    let built = Command::new(cc)
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pedantic", "-I"])
        .arg(&host)
        .arg("-I")
        .arg(&worker)
        .arg(manifest.join("tests/c/both_headers.c"))
        .arg("-o")
        .arg(&program)
        .output()
        .expect("the compiler runs");
    assert!(
        built.status.success(),
        "{}",
        String::from_utf8_lossy(&built.stderr)
    );
    let ran = Command::new(&program).status().expect("the program runs");
    assert!(ran.success(), "the program exits 0: {ran}");
}
