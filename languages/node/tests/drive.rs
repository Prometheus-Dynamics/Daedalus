//! The drive conformance suite, through the Node driver (`drive/driver.js`) over the pure
//! JavaScript client: every scenario over every transport, and the codec check on the corpus.
//! A missing `node` is a skip, said on stderr, as the Python and Java drivers' tests do.

use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_drive_conformance::{Failure, build_fixture, run_codec_check, run_driver};

fn driver() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("drive")
        .join("driver.js")
}

/// The Node command, or `None` (with a line on stderr) when `node` is not on the path or is older
/// than the SDK's engines floor, 22.
// The skip line goes to stderr, where a run without a Node 22 shows why nothing ran.
#[allow(clippy::print_stderr)]
fn node() -> Option<Command> {
    let output = Command::new("node").arg("--version").output().ok();
    let version = output
        .as_ref()
        .and_then(|out| String::from_utf8(out.stdout.clone()).ok());
    let major = version.as_deref().and_then(|text| {
        text.trim()
            .trim_start_matches('v')
            .split('.')
            .next()?
            .parse::<u32>()
            .ok()
    });
    match major {
        Some(major) if major >= 22 => {
            let mut command = Command::new("node");
            command.arg(driver());
            Some(command)
        }
        _ => {
            eprintln!("skipped: no Node 22 on the path, so the Node driver did not run");
            None
        }
    }
}

fn fixture() -> PathBuf {
    let target = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("drive-fixture");
    build_fixture(&target).expect("the dylib fixture builds")
}

fn report(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(|failure| failure.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn every_scenario_over_every_transport_matches_the_golden_transcripts() {
    let Some(command) = node() else { return };
    let failures = run_driver(command, &fixture());
    assert!(failures.is_empty(), "{}", report(&failures));
}

#[test]
fn the_codec_check_matches_the_corpus_goldens() {
    let Some(command) = node() else { return };
    // The codec mode is `DRIVER --recode KIND FILE`: the suite appends those arguments.
    let failures = run_codec_check(&command);
    assert!(failures.is_empty(), "{}", report(&failures));
}
