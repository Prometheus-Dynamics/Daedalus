//! The drive conformance suite, run on the Rust reference driver, and on drivers that are wrong on
//! purpose (so the suite is not vacuous). `BLESS=1` rewrites the golden transcripts from the
//! reference driver first.

use std::path::PathBuf;
use std::process::Command;

use daedalus_drive_conformance::{
    Failure, bless, build_fixture, golden_dir, load_scenarios, run_codec_check, run_driver,
};

fn fixture() -> PathBuf {
    let target = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("drive-fixture");
    build_fixture(&target).expect("the dylib fixture builds")
}

fn reference() -> Command {
    Command::new(env!("CARGO_BIN_EXE_drive-reference"))
}

fn faulty(kind: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_drive-faulty"));
    command.args(["--mutate", kind]);
    command
}

/// Under `BLESS=1` the reference-driver test writes every golden, and the tests that read them
/// wait for the next run, so they never race with the writer.
fn blessing() -> bool {
    std::env::var_os("BLESS").is_some()
}

fn report(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(|failure| failure.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_reference_driver_gives_the_golden_transcripts() {
    let fixture = fixture();
    if blessing() {
        let failures = bless(&reference(), &fixture);
        assert!(failures.is_empty(), "{}", report(&failures));
    }
    let failures = run_driver(reference(), &fixture);
    assert!(failures.is_empty(), "{}", report(&failures));
}

#[test]
fn the_codec_goldens_have_one_line_per_corpus_body() {
    if blessing() {
        return;
    }
    for kind in ["client", "server"] {
        let bytes =
            std::fs::read(daedalus_drive_conformance::corpus_path(kind)).expect("the corpus");
        let bodies = daedalus_drive_conformance::corpus_bodies(&bytes).expect("a corpus file");
        let golden = std::fs::read_to_string(daedalus_drive_conformance::codec_golden_path(kind))
            .expect("the codec golden");
        assert_eq!(golden.lines().count(), bodies.len(), "{kind}");
    }
}

#[test]
fn the_reference_driver_passes_the_codec_check() {
    if blessing() {
        return;
    }
    let failures = run_codec_check(&reference());
    assert!(failures.is_empty(), "{}", report(&failures));
}

#[test]
fn every_scenario_has_a_golden_transcript_and_no_golden_is_left_over() {
    if blessing() {
        return;
    }
    let scenarios = load_scenarios().expect("the scenarios load");
    let names: Vec<String> = scenarios.iter().map(|s| s.name.clone()).collect();
    for scenario in &scenarios {
        let golden = golden_dir().join(format!("{}.out", scenario.name));
        let text = std::fs::read_to_string(&golden)
            .unwrap_or_else(|error| panic!("{}: {error}", golden.display()));
        assert_eq!(
            text.lines().count(),
            scenario.commands.len(),
            "{}: one golden line per command",
            scenario.name
        );
    }
    for entry in std::fs::read_dir(golden_dir()).expect("the golden directory") {
        let path = entry.expect("an entry").path();
        let name = path
            .file_stem()
            .expect("a name")
            .to_string_lossy()
            .into_owned();
        if name.starts_with("codec-") {
            continue;
        }
        assert!(names.contains(&name), "{} has no scenario", path.display());
    }
}

#[test]
fn a_driver_that_cannot_run_is_a_failure() {
    let failures = run_driver(Command::new("/no/such/drive-driver"), &fixture());
    assert!(!failures.is_empty(), "a missing driver passed the suite");
}

#[test]
fn a_driver_with_a_wrong_value_fails_the_suite() {
    let failures = run_driver(faulty("value"), &fixture());
    assert!(
        failures.iter().any(|failure| failure.scenario == "02_run"),
        "{}",
        report(&failures)
    );
}

#[test]
fn a_driver_that_drops_an_event_fails_the_suite() {
    let failures = run_driver(faulty("event"), &fixture());
    assert!(
        failures
            .iter()
            .any(|failure| failure.scenario == "03_faults"),
        "{}",
        report(&failures)
    );
}

#[test]
fn a_driver_that_swallows_an_error_code_fails_the_suite() {
    let failures = run_driver(faulty("code"), &fixture());
    assert!(
        failures
            .iter()
            .any(|failure| failure.scenario == "13_errors"),
        "{}",
        report(&failures)
    );
}

#[test]
fn a_driver_that_recodes_wrongly_fails_the_codec_check() {
    let failures = run_codec_check(&faulty("recode"));
    assert!(
        !failures.is_empty(),
        "a wrong recode passed the codec check"
    );
}
