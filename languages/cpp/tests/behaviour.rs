//! The C++ worker through the worker behaviour suite, and the transport its large values took: by
//! reference on Linux, where shared memory is lent, and plain elsewhere.

use std::path::Path;
use std::sync::OnceLock;

use daedalus_lang_cpp::{build, spec};
use daedalus_worker_conformance::{Transport, behaviours, large_transport};

fn built() -> std::path::PathBuf {
    static BUILT: OnceLock<std::path::PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let dir = daedalus_lang_cpp::build_dir(
                Path::new(env!("CARGO_TARGET_TMPDIR")),
                "cpp-behaviour",
            );
            build(&dir).expect("cmake builds the fixture worker")
        })
        .clone()
}

#[test]
fn the_cpp_worker_passes_every_behaviour() {
    let failures = behaviours(spec(&built()), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_cpp_worker_carries_large_values_by_reference_on_linux() {
    let transport = large_transport(&spec(&built())).expect("the large value round-trips");
    let want = if cfg!(target_os = "linux") {
        Transport::ByReference
    } else {
        Transport::Plain
    };
    assert_eq!(transport, want);
}
