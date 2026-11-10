//! The canonical fixtures, through the Java worker, in two rounds each. The SDK's own checks run
//! too, as plain Java programs.

use std::path::Path;
use std::process::Command;

use daedalus_lang_java::{compile, spec};
use daedalus_worker_conformance::run_all;

#[test]
fn all_thirteen_fixtures_pass_through_the_java_worker() {
    let classes = Path::new(env!("CARGO_TARGET_TMPDIR")).join("java-classes");
    compile(&classes, &[]).expect("javac compiles the SDK and the fixtures");
    let failures = run_all(spec(&classes)).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_sdk_wire_checks_pass() {
    let classes = Path::new(env!("CARGO_TARGET_TMPDIR")).join("java-tests");
    let test_source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/src/test/java/dev/daedalus/worker");
    let sources: Vec<_> = std::fs::read_dir(&test_source)
        .expect("the test sources")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    compile(&classes, &sources).expect("javac compiles the checks");
    let status = Command::new("java")
        .arg("-cp")
        .arg(&classes)
        .arg("dev.daedalus.worker.WireTest")
        .status()
        .expect("java runs");
    assert!(status.success(), "WireTest failed");
}

#[test]
fn a_print_in_a_body_does_not_reach_the_packet_stream() {
    use daedalus_wire::codec::Writer;
    use daedalus_wire::{DEFAULT_CAP, kind, read_packet, write_packet};
    use std::process::Stdio;

    let classes = Path::new(env!("CARGO_TARGET_TMPDIR")).join("java-chatty");
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("sdk/src/test/java/dev/daedalus/worker/ChattyWorker.java");
    compile(&classes, &[source]).expect("javac compiles the chatty worker");
    let mut child = Command::new("java")
        .args(["-Xlog:disable", "-Xlog:all=warning:stderr", "-cp"])
        .arg(&classes)
        .arg("dev.daedalus.worker.ChattyWorker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("java runs");
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut buf = Vec::new();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).expect("the hello arrives");
    assert_eq!(buf[0], kind::HELLO);
    // A run of demo.chatty with x = 5. The body prints, and that print must not corrupt the reply.
    let mut run = Writer::new();
    run.u8(kind::RUN);
    run.u64(1);
    run.u32(0);
    run.u32(1);
    run.str("demo.chatty").expect("a short string");
    run.u32(1);
    run.str("demo.chatty").expect("a short string");
    run.str("x").expect("a short string");
    run.i64(5);
    write_packet(&mut stdin, &run.buf, DEFAULT_CAP).expect("the run is written");
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).expect("the reply arrives");
    assert_eq!(buf[0], kind::OUTPUTS, "the reply is outputs, not the print");
    write_packet(&mut stdin, &[kind::SHUTDOWN], DEFAULT_CAP).expect("shutdown is written");
    drop(stdin);
    let output = child.wait_with_output().expect("the worker exits");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("hello from a body"));
}
