//! The native C and C++ tests: the value codec and the worker loop's state messages, run by ctest in
//! the CMake build the SDK makes.

use std::path::Path;

use daedalus_lang_cpp::{build, run_native_tests};

#[test]
fn the_native_c_and_cpp_tests_pass() {
    let dir = daedalus_lang_cpp::build_dir(Path::new(env!("CARGO_TARGET_TMPDIR")), "cpp-native");
    build(&dir).expect("cmake builds the SDK and its tests");
    run_native_tests(&dir).expect("the native tests pass");
}
