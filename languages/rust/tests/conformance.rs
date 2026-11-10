//! The fixtures this SDK can serve, through the Rust worker, in two rounds each.

use daedalus_backend_process::WorkerSpec;
use daedalus_worker_conformance::run_fixtures;

#[test]
fn the_rust_worker_passes_the_fixtures_it_serves() {
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-rust-fixture-worker"));
    let failures = run_fixtures(spec, &["scalar_add", "multi_output", "raw_io"])
        .expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}
