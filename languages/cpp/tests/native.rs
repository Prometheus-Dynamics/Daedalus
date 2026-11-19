//! The SDK's CMake build: it builds, it installs into a prefix, and a consumer built against the
//! installed package with `find_package` links each library and runs. The worker header's own
//! behaviour is the fixture worker's, run by the conformance suites; the codec is tested in Rust.

use std::path::Path;
use std::process::Command;

use daedalus_lang_cpp::{build, install_and_build_consumer};

#[test]
fn the_sdk_builds_installs_and_a_consumer_links_every_library() {
    let target = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = daedalus_lang_cpp::build_dir(target, "cpp-native");
    build(&dir).expect("cmake builds the SDK and its fixture worker");

    // Keyed by this checkout like the SDK's build: a CMake cache made from another checkout's
    // sources is refused, and an install holds only this one's libraries.
    let prefix = daedalus_lang_cpp::build_dir(target, "cpp-install");
    let consumer_build = daedalus_lang_cpp::build_dir(target, "cpp-consumer");
    let consumer = install_and_build_consumer(&dir, &prefix, &consumer_build)
        .expect("the installed SDK is found by find_package and the consumer builds");
    // The consumer loads the host and client libraries from the install's library folder. Windows
    // searches the program's folder and PATH for a DLL, so the folder goes on PATH; the other
    // platforms are given it as their library path too.
    let libdir = prefix.join("lib");
    let mut run = Command::new(&consumer);
    if cfg!(windows) {
        let path = std::env::var_os("PATH").unwrap_or_default();
        let joined = std::env::join_paths(
            std::iter::once(libdir.clone()).chain(std::env::split_paths(&path)),
        )
        .expect("the library folder is a path");
        run.env("PATH", joined);
    } else {
        run.env("LD_LIBRARY_PATH", &libdir)
            .env("DYLD_LIBRARY_PATH", &libdir);
    }
    let ran = run.status().expect("the consumer runs");
    assert!(ran.success(), "the consumer exits 0: {ran}");
}
