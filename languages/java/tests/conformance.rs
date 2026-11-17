//! The Java worker, through the worker conformance suites: the thirteen value fixtures, the six
//! behaviours and the packet-level check that a print in a body stays off the stream. The SDK's own
//! checks (the value codec, the service codec and the layout hashes) run as plain Java programs, on
//! the jar this test builds.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use daedalus_lang_java::{
    class_path, compile, fixture_class_path, or_fail, sdk_test_sources, spec, tools,
};
use daedalus_wire::codec::Writer;
use daedalus_wire::{DEFAULT_CAP, kind, read_packet, write_packet};
use daedalus_worker_conformance::{Transport, behaviours, large_transport, run_all};

/// Where this binary builds: its own directory, so it never shares a jar with the driver's test.
fn target() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("java-worker")
}

/// The jar and the fixture worker, built once for this binary.
fn fixtures() -> &'static OsString {
    static CLASSES: OnceLock<OsString> = OnceLock::new();
    CLASSES.get_or_init(|| or_fail(fixture_class_path(&target())))
}

/// The jar, which the fixtures build: every path to it goes through the build, so none runs first.
fn jar() -> PathBuf {
    let _built = fixtures();
    target().join("daedalus.jar")
}

/// The SDK's checks, compiled against the jar, once for this binary.
fn sdk_checks() -> &'static PathBuf {
    static CLASSES: OnceLock<PathBuf> = OnceLock::new();
    CLASSES.get_or_init(|| {
        let out = target().join("sdk-tests");
        or_fail(compile(&out, &[jar()], &sdk_test_sources()));
        out
    })
}

/// Runs one of the SDK's checks: `java -cp <jar> <classes> <main>`, with its arguments.
fn run_check(main: &str, args: &[String]) {
    let cp = class_path(&[jar(), sdk_checks().clone()]).expect("a class path");
    let output = Command::new(&or_fail(tools()).java)
        .arg("-cp")
        .arg(cp)
        .arg(main)
        .args(args)
        .output()
        .expect("java runs");
    assert!(
        output.status.success(),
        "{main} failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn all_thirteen_fixtures_pass_through_the_java_worker() {
    let failures = run_all(or_fail(spec(fixtures()))).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn the_six_behaviours_pass_through_the_java_worker() {
    let failures = behaviours(or_fail(spec(fixtures())), &[]).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn large_values_travel_plain_because_the_java_worker_has_no_shared_memory() {
    let transport =
        large_transport(&or_fail(spec(fixtures()))).expect("the large values round trip");
    assert!(
        matches!(transport, Transport::Plain),
        "the Java worker takes plain packets only"
    );
}

#[test]
fn a_print_in_a_body_does_not_reach_the_packet_stream() {
    let cp = class_path(&[jar(), sdk_checks().clone()]).expect("a class path");
    let mut child = Command::new(&or_fail(tools()).java)
        .args(["-Xlog:disable", "-Xlog:all=warning:stderr", "-cp"])
        .arg(&cp)
        .arg("dev.daedalus.worker.ChattyWorker")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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

#[test]
fn the_value_codec_checks_pass() {
    run_check("dev.daedalus.wire.WireTest", &[]);
}

#[test]
fn the_service_codec_checks_pass() {
    run_check(
        "dev.daedalus.service.ServiceTest",
        &[daedalus_wire::service::SERVICE_LAYOUT_HASH.to_string()],
    );
}

#[test]
fn a_watch_keeps_at_most_watch_queue_events() {
    assert_eq!(
        daedalus_wire::service::WATCH_QUEUE,
        64,
        "the Java bound is the daemon's"
    );
    run_check("dev.daedalus.client.WatchTest", &[]);
}

#[test]
fn the_worker_layout_hash_is_the_rust_constant() {
    run_check(
        "dev.daedalus.worker.LayoutTest",
        &[daedalus_wire::LAYOUT_HASH.to_string()],
    );
}
