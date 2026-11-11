//! The canonical fixtures through the C++ worker, in two rounds each; and two supervision checks
//! that a fault and a malformed packet each leave the worker serving the next run.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use daedalus_lang_cpp::{build, spec};
use daedalus_wire::codec::Writer;
use daedalus_wire::{DEFAULT_CAP, kind, read_packet, write_packet};
use daedalus_worker_conformance::run_all;

/// Builds the worker once per test binary. Tests run in parallel, and two CMake configures into one
/// build directory race, so every test gets the same built path.
fn built() -> std::path::PathBuf {
    static BUILT: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let dir =
                daedalus_lang_cpp::build_dir(Path::new(env!("CARGO_TARGET_TMPDIR")), "cpp-build");
            build(&dir).expect("cmake builds the fixture worker")
        })
        .clone()
}

#[test]
fn all_thirteen_fixtures_pass_through_the_cpp_worker() {
    let failures = run_all(spec(&built())).expect("the worker registers");
    assert!(failures.is_empty(), "{failures:#?}");
}

fn run_message(run: u64, node: &str, port: &str, value: i64) -> Vec<u8> {
    let mut out = Writer::new();
    out.u8(kind::RUN);
    out.u64(run);
    out.u32(0);
    out.u32(1);
    out.str(node).expect("a short string");
    out.u32(1);
    out.str(node).expect("a short string");
    out.str(port).expect("a short string");
    out.i64(value);
    out.buf
}

#[test]
fn a_fault_and_a_malformed_packet_each_leave_the_worker_serving() {
    let worker = built();
    let mut child = Command::new(&worker)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the worker starts");
    let mut stdout = child.stdout.take().expect("stdout");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut buf = Vec::new();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).expect("the hello arrives");

    // A node that faults: a fault packet, then outputs with status fault.
    write_packet(
        &mut stdin,
        &run_message(1, "fixture.failure_diagnostic", "value", 7),
        DEFAULT_CAP,
    )
    .unwrap();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).unwrap();
    assert_eq!(buf[0], kind::FAULT, "the fault comes first");
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).unwrap();
    assert_eq!(buf[0], kind::OUTPUTS);
    assert_eq!(buf[9], 2, "the run's status is fault");

    // A packet that is not a message: a malformed-packet fault, and the next run still works.
    write_packet(&mut stdin, &[0x7f], DEFAULT_CAP).unwrap();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).unwrap();
    assert_eq!(buf[0], kind::FAULT);
    let mut both = Writer::new();
    both.u8(kind::RUN);
    both.u64(2);
    both.u32(0);
    both.u32(1);
    both.str("fixture.scalar_add").expect("a short string");
    both.u32(2);
    for (port, value) in [("a", 2i64), ("b", 40)] {
        both.str("fixture.scalar_add").expect("a short string");
        both.str(port).expect("a short string");
        both.i64(value);
    }
    write_packet(&mut stdin, &both.buf, DEFAULT_CAP).unwrap();
    read_packet(&mut stdout, DEFAULT_CAP, &mut buf).unwrap();
    assert_eq!(
        buf[0],
        kind::OUTPUTS,
        "the worker still serves after a malformed packet"
    );
    write_packet(&mut stdin, &[kind::SHUTDOWN], DEFAULT_CAP).unwrap();
    stdin.flush().unwrap();
    drop(stdin);
    let status = child.wait().expect("the worker exits");
    assert!(status.success());
}
