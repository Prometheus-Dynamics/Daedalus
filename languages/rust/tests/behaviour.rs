//! The Rust reference worker through the worker behaviour suite.

use daedalus_backend_process::WorkerSpec;
use daedalus_worker_conformance::{Transport, behaviours, large_transport};

const WORKER: &str = env!("CARGO_BIN_EXE_daedalus-rust-fixture-worker");

#[test]
fn the_rust_worker_passes_every_behaviour() {
    let failures = behaviours(WorkerSpec::new(WORKER), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_rust_worker_carries_large_values_by_reference_on_linux() {
    let transport = large_transport(&WorkerSpec::new(WORKER)).expect("the large value round-trips");
    let want = if cfg!(target_os = "linux") {
        Transport::ByReference
    } else {
        Transport::Plain
    };
    assert_eq!(transport, want);
}
