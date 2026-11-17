#![allow(clippy::print_stderr)] // the skip notice: no interpreter on this machine
//! The Python client, under the drive conformance suite: `drive/driver.py` is run over every
//! scenario and every transport, and its answers must equal the golden transcripts.
//!
//! The codec mode of the suite does not apply here. It checks a pure-language client's own codec
//! against the wire's corpus; this client's codec is the Rust one, so the corpus is checked by the
//! Rust client's tests instead.

mod support;

use std::path::Path;
use std::process::Command;

use daedalus_drive_conformance::run_driver;

#[test]
fn the_python_driver_gives_the_golden_transcripts_on_every_transport() {
    let Some(python) = support::python() else {
        eprintln!("skipping: no python3 on PATH, so the Python driver did not run");
        return;
    };
    let driver = Path::new(env!("CARGO_MANIFEST_DIR")).join("drive/driver.py");
    let mut command = Command::new(python);
    command.arg(driver).env(
        "DAEDALUS_CLIENT_LIB",
        support::library("daedalus_client_capi"),
    );
    let failures = run_driver(command, support::plugin());
    let report: Vec<String> = failures.iter().map(|failure| failure.to_string()).collect();
    assert!(failures.is_empty(), "{}", report.join("\n"));
}
