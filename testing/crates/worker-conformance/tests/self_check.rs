//! The behaviour suite's self-check: the honest worker passes every behaviour, and each sabotaged
//! worker fails the behaviour it breaks. A suite that passed everything would show nothing.

use daedalus_backend_process::WorkerSpec;
use daedalus_worker_conformance::{BEHAVIOURS, behaviours};

const SABOTAGE: &str = env!("CARGO_BIN_EXE_daedalus-worker-sabotage");

fn sabotaged(name: &str) -> WorkerSpec {
    WorkerSpec::new(SABOTAGE).env("DAEDALUS_SABOTAGE", name)
}

#[test]
fn the_honest_worker_passes_every_behaviour() {
    let failures = behaviours(WorkerSpec::new(SABOTAGE), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn each_sabotaged_worker_fails_the_behaviour_it_breaks() {
    for name in BEHAVIOURS {
        let failures = behaviours(sabotaged(name), &[name]).expect("the worker registers");
        assert_eq!(failures.len(), 1, "{name}: {failures:#?}");
        assert_eq!(failures[0].fixture, name, "{failures:#?}");
    }
}

#[test]
fn an_unknown_behaviour_name_is_a_failure() {
    let failures = behaviours(WorkerSpec::new(SABOTAGE), &["no_such_case"]).expect("registers");
    assert_eq!(failures.len(), 1, "{failures:#?}");
}
