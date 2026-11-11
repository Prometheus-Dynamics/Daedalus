//! The thirteen canonical fixtures, through the Rust worker, in two rounds each.

use daedalus_backend_process::WorkerSpec;
use daedalus_worker_conformance::run_fixtures;

#[test]
fn all_thirteen_fixtures_pass_through_the_rust_worker() {
    let spec = WorkerSpec::new(env!("CARGO_BIN_EXE_daedalus-rust-fixture-worker"));
    let names = [
        "scalar_add",
        "multi_output",
        "raw_io",
        "bytes_echo",
        "struct_record",
        "enum_choice",
        "optional_value",
        "list_reverse",
        "tuple_pair",
        "map_pairs",
        "custom_type_key",
        "failure_diagnostic",
        "stateful_counter",
    ];
    let failures = run_fixtures(spec, &names).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}
