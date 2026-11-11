//! The example end to end: a host loads the graph, the Rust worker serves its node, the host feeds,
//! steps, reads the worker's progress, and cancels a run the worker is waiting in.

use std::path::Path;
use std::process::Command;

const GOLDEN: &str = "\
host: 2 went to the worker and came back as Some(6), its progress was Some((1000, 1000))
host: a run the host cancelled ended with the fault \"the run was cancelled\"
";

fn worker() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_daedalus-example-rust-worker-serve"))
}

#[test]
fn the_host_runs_the_node_in_the_worker_and_cancels_a_run() {
    assert_eq!(daedalus_example_rust_worker::run(worker()), GOLDEN);
}

#[test]
fn the_example_binary_prints_the_same_lines() {
    let output = Command::new(env!("CARGO_BIN_EXE_daedalus-example-rust-worker"))
        .output()
        .expect("the example runs");
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), GOLDEN);
}
