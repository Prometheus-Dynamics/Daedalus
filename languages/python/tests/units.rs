#![allow(clippy::print_stderr)] // the skip notice: no interpreter on this machine
//! The Python SDK's own unit tests (`sdk/tests`, run with `unittest`), with the two C libraries
//! and the fixture plugin in the environment so the host and client tests can reach them.

mod support;

use std::path::Path;
use std::process::Command;

#[test]
fn the_python_sdk_unit_tests_pass() {
    let Some(python) = support::python() else {
        eprintln!(
            "skipping: {} doesn't run here, so the Python unit tests did not run",
            daedalus_lang_python::DEFAULT_INTERPRETER
        );
        return;
    };
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    let graph = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testing/crates/drive-conformance/scenarios/graphs/pass.json");
    let output = Command::new(python)
        .args(["-m", "unittest", "discover", "-s", "tests"])
        .current_dir(&sdk)
        .env(
            "DAEDALUS_CLIENT_LIB",
            support::library("daedalus_client_capi"),
        )
        .env("DAEDALUS_HOST_LIB", support::library("daedalus_capi"))
        .env("DAEDALUS_TEST_PLUGIN", support::plugin())
        .env("DAEDALUS_TEST_GRAPH", graph)
        .output()
        .expect("the interpreter runs");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{text}");
}
