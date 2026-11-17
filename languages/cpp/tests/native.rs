//! The native C++ test: the header links against the Rust library, and a value round-trips. ctest
//! runs it in the CMake build the SDK makes. The same build is then installed into a prefix, and a
//! consumer built against the installed package runs.

use std::path::Path;
use std::process::Command;

use daedalus_lang_cpp::{build, install_and_build_consumer, run_native_tests};

#[test]
fn the_native_c_and_cpp_tests_pass() {
    let target = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let dir = daedalus_lang_cpp::build_dir(target, "cpp-native");
    build(&dir).expect("cmake builds the SDK and its tests");
    run_native_tests(&dir).expect("the native tests pass");

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
