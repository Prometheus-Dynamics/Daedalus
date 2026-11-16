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

    let prefix = target.join("cpp-install");
    let consumer_build = target.join("cpp-consumer");
    let consumer = install_and_build_consumer(&dir, &prefix, &consumer_build)
        .expect("the installed SDK is found by find_package and the consumer builds");
    let ran = Command::new(&consumer).status().expect("the consumer runs");
    assert!(ran.success(), "the consumer exits 0: {ran}");
}
