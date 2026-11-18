//! The Python worker through the worker behaviour suite. The large value goes as a plain packet:
//! this SDK has no shared memory, by ruling (see the SDK's README), so the transport is `Plain`.

use daedalus_backend_process::ProcessBackend;
use daedalus_lang_python::{fixture_worker, spec, spec_with};
use daedalus_worker_conformance::runner::registry;
use daedalus_worker_conformance::{Transport, behaviours, large_transport};

#[test]
fn the_python_worker_passes_every_behaviour() {
    let failures = behaviours(spec(&fixture_worker()), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_python_worker_carries_large_values_as_plain_packets() {
    let transport = large_transport(&spec(&fixture_worker())).expect("the large value round-trips");
    assert_eq!(transport, Transport::Plain);
}

#[test]
fn an_interpreter_that_is_not_there_is_refused_at_start_naming_it() {
    let missing = "daedalus-no-such-python";
    let error = ProcessBackend::new()
        .add_worker(spec_with(missing, &fixture_worker()), &registry())
        .expect_err("a worker whose interpreter is missing does not start");
    assert!(error.to_string().contains(missing), "{error}");
}
