//! The Java driver, through the drive conformance suite: every scenario over every transport the
//! suite runs (TCP, and a unix socket where the platform has one), and the codec check on the corpus.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use daedalus_drive_conformance::{Failure, build_fixture, run_codec_check, run_driver};
use daedalus_lang_java::{driver, driver_class_path, or_fail, tools};

fn target() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("java-drive")
}

/// The jar and the driver, built once for this binary.
fn driver_classes() -> &'static OsString {
    static CLASSES: OnceLock<OsString> = OnceLock::new();
    CLASSES.get_or_init(|| or_fail(driver_class_path(&target())))
}

/// The dylib fixture the scenarios load, built once.
fn fixture() -> &'static PathBuf {
    static FIXTURE: OnceLock<PathBuf> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        build_fixture(&PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("drive-fixture"))
            .expect("the dylib fixture builds")
    })
}

fn report(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(|failure| failure.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_java_driver_gives_the_golden_transcripts_over_every_transport() {
    let failures = run_driver(or_fail(driver(driver_classes())), fixture());
    assert!(failures.is_empty(), "{}", report(&failures));
}

#[test]
fn the_java_codec_re_encodes_the_corpus_to_the_goldens() {
    let failures = run_codec_check(&or_fail(driver(driver_classes())));
    assert!(failures.is_empty(), "{}", report(&failures));
}

#[test]
fn a_java_driver_that_cannot_run_is_a_failure() {
    let mut missing = Command::new(&or_fail(tools()).java);
    missing.args(["-cp", "/no/such/classes", "dev.daedalus.drive.Drive"]);
    let failures = run_driver(missing, fixture());
    assert!(
        !failures.is_empty(),
        "a driver that cannot start passed the suite"
    );
}
