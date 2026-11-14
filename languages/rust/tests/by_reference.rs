//! A Rust worker takes values by reference on Linux: a large `bytes` input reaches a `&[u8]`
//! parameter where the host put it, and a large output is held by the host's cell in the value
//! buffer the worker wrote it into.

#![cfg(target_os = "linux")]

use daedalus_backend_process::WorkerSpec;
use daedalus_worker_conformance::large_values;

#[test]
fn large_values_travel_by_reference_through_the_rust_worker() {
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-rust-fixture-worker"));
    let large = large_values(spec, 1 << 20, 4).expect("each run gives the value reversed");
    let telemetry = large.telemetry;
    assert_eq!(telemetry.agreed, 1, "{telemetry:?}");
    assert_eq!(telemetry.placed_in, 4, "{telemetry:?}");
    assert_eq!(telemetry.placed_out, 4, "{telemetry:?}");
    assert_eq!(large.held, 4, "{telemetry:?}");
    assert!(telemetry.bytes_in < 4096, "{telemetry:?}");
    assert!(telemetry.bytes_out < 4096, "{telemetry:?}");
}
