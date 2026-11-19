//! The drive conformance suite (`testing/crates/drive-conformance`) on a driver over this crate's C
//! ABI. The driver is the reference driver's commands, parsing and answer lines, with every request
//! made through the C calls a C caller makes (`capi.rs`) and every result read back from the
//! structs, JSON and wire bytes the calls give (`json.rs`). Every scenario, over each transport,
//! must give the golden transcripts byte for byte.
//!
//! This file is its own harness, since the suite runs its driver as a program. Run as a test (no
//! `DAEDALUS_DRIVE_CAPI` in the environment), it runs the suite with itself as the driver. Run by
//! the suite, it is the driver: the endpoint is its argument, the commands are on standard input,
//! and the answers go to standard output.

mod capi;
mod json;

use std::io::Write;
use std::path::Path;
use std::process::{Command, ExitCode};

use daedalus_drive_conformance::reference::{self, Mutation};
use daedalus_drive_conformance::{build_fixture, coverage, run_driver};

/// Set on the driver's command: this process is the driver, not the suite.
const DRIVER: &str = "DAEDALUS_DRIVE_CAPI";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let outcome = if std::env::var_os(DRIVER).is_some() {
        drive(&args)
    } else if args.iter().any(|arg| arg == "--list") {
        // A listing of the tests: this harness has one, and lists none by name.
        Ok(())
    } else {
        suite()
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(std::io::stderr(), "drive-capi: {error}");
            ExitCode::FAILURE
        }
    }
}

fn drive(args: &[String]) -> Result<(), String> {
    let [endpoint] = args else {
        return Err("usage: ENDPOINT".to_owned());
    };
    let root = capi::Session::connect(endpoint)?;
    reference::drive_service(
        root,
        Mutation::None,
        &mut std::io::stdin().lock(),
        &mut std::io::stdout().lock(),
    )
}

fn suite() -> Result<(), String> {
    let fixture = build_fixture(&Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture"))?;
    let mut driver = Command::new(std::env::current_exe().map_err(|error| error.to_string())?);
    driver.env(DRIVER, "1");
    let failures = run_driver(driver, &fixture);
    if !failures.is_empty() {
        let lines: Vec<String> = failures.iter().map(ToString::to_string).collect();
        return Err(format!(
            "{} answers differ from the golden transcripts:\n{}",
            failures.len(),
            lines.join("\n")
        ));
    }
    let (transcripts, lines) = coverage()?;
    writeln!(
        std::io::stdout(),
        "test result: ok. the C ABI driver gives the golden transcripts: {transcripts} \
         transcripts, {lines} answer lines"
    )
    .map_err(|error| error.to_string())
}
